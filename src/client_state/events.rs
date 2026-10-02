//! Best-effort lifecycle hints collected outside authoritative fences.
use std::{
    cell::RefCell,
    marker::PhantomData,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::controller::events::{EventBatch, EventSink, NewEvent, PublishAttempt, QueueHint};

const MAX_HINTS: usize = 32;
// Stable, title-free diagnostic: events are best effort and repair handles drops.
static CONTROLLER_EVENT_HINTS_DROPPED: AtomicU64 = AtomicU64::new(0);

pub fn dropped_hint_count() -> u64 {
    CONTROLLER_EVENT_HINTS_DROPPED.load(Ordering::Relaxed)
}

#[derive(Default)]
struct PendingHints {
    scopes: usize,
    hints: Vec<(Arc<dyn EventSink>, NewEvent)>,
}

thread_local! { static PENDING: RefCell<PendingHints> = RefCell::default(); }

/// A thread-bound scope. Nested scopes defer release until the outer guard drops.
pub struct DeferredHints {
    sink: Option<Arc<dyn EventSink>>,
    _thread_bound: PhantomData<Rc<()>>,
}

impl DeferredHints {
    pub fn begin(sink: Arc<dyn EventSink>) -> Self {
        Self::enter(Some(sink))
    }

    fn enter(sink: Option<Arc<dyn EventSink>>) -> Self {
        PENDING.with(|pending| pending.borrow_mut().scopes += 1);
        Self {
            sink,
            _thread_bound: PhantomData,
        }
    }

    /// Encloses a log or drain fence even before a nested writer has a sink.
    /// The guard must be declared before the fence, or stored after its FD.
    pub(crate) fn fence() -> Self {
        Self::enter(None)
    }

    pub fn capture(&self, event: NewEvent) {
        if let Some(sink) = &self.sink {
            Self::capture_for(sink, event);
        }
    }

    pub(crate) fn capture_for(sink: &Arc<dyn EventSink>, event: NewEvent) {
        PENDING.with(|pending| {
            let mut pending = pending.borrow_mut();
            if pending.scopes == 0 {
                CONTROLLER_EVENT_HINTS_DROPPED.fetch_add(1, Ordering::Relaxed);
                return;
            }
            if matches!(&event, NewEvent::QueueChanged(_))
                && pending.hints.iter().any(|(binding, event)| {
                    Arc::ptr_eq(binding, sink)
                        && matches!(event, NewEvent::QueueChanged(hint) if hint.turn_id.is_none())
                })
            {
                return;
            }
            if let NewEvent::RunChanged { run_id, .. } = &event
                && pending.hints.iter().any(|(binding, event)| {
                    Arc::ptr_eq(binding, sink)
                        && matches!(event, NewEvent::RunChanged { run_id: saved, task_id: None } if saved == run_id)
                })
            {
                return;
            }
            let event = if pending.hints.len() == MAX_HINTS
                && matches!(&event, NewEvent::QueueChanged(_))
            {
                pending.hints.retain(|(binding, event)| {
                    !(Arc::ptr_eq(binding, sink) && matches!(event, NewEvent::QueueChanged(_)))
                });
                NewEvent::QueueChanged(QueueHint {
                    turn_id: None,
                    state: None,
                    kind: None,
                    code: None,
                })
            } else if pending.hints.len() == MAX_HINTS
                && let NewEvent::RunChanged { run_id, .. } = event
            {
                pending.hints.retain(|(binding, event)| {
                    !(Arc::ptr_eq(binding, sink)
                        && matches!(event, NewEvent::RunChanged { run_id: saved, .. } if *saved == run_id))
                });
                NewEvent::RunChanged { run_id, task_id: None }
            } else {
                event
            };
            if pending.hints.len() == MAX_HINTS {
                CONTROLLER_EVENT_HINTS_DROPPED.fetch_add(1, Ordering::Relaxed);
            } else {
                pending.hints.push((sink.clone(), event));
            }
        });
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn finish(self) {}
}

impl Drop for DeferredHints {
    fn drop(&mut self) {
        let hints = PENDING.with(|pending| {
            let mut pending = pending.borrow_mut();
            pending.scopes -= 1;
            if pending.scopes == 0 {
                std::mem::take(&mut pending.hints)
            } else {
                Vec::new()
            }
        });
        let mut batches: Vec<(Arc<dyn EventSink>, Vec<NewEvent>)> = Vec::new();
        for (sink, event) in hints {
            if let Some((_, batch)) = batches
                .iter_mut()
                .find(|(binding, _)| Arc::ptr_eq(binding, &sink))
            {
                batch.push(event);
            } else {
                batches.push((sink, vec![event]));
            }
        }
        for (sink, events) in batches {
            let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                EventBatch::try_new(events).map(|batch| sink.try_publish(batch))
            }));
            if !matches!(attempt, Ok(Ok(PublishAttempt::Queued))) {
                CONTROLLER_EVENT_HINTS_DROPPED.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

use crate::{
    controller::events::{SafeCode, SafeOutcome, TaskHint, TurnHint},
    task::{LocalTaskRecord, TaskOutcome, TaskState},
};

fn safe_code(code: &str) -> SafeCode {
    SafeCode::from_public_code(code)
}

fn outcome_hint(outcome: &TaskOutcome) -> (SafeOutcome, Option<SafeCode>) {
    match outcome {
        TaskOutcome::Done => (SafeOutcome::Done, None),
        TaskOutcome::NeedsInput => (SafeOutcome::NeedsInput, None),
        TaskOutcome::Blocked => (SafeOutcome::Blocked, None),
        TaskOutcome::Unknown => (SafeOutcome::Unknown, None),
        TaskOutcome::Failed { reason } => (SafeOutcome::Failed, Some(safe_code(reason))),
        TaskOutcome::Cancelled => (SafeOutcome::Cancelled, None),
        TaskOutcome::TimedOut => (SafeOutcome::TimedOut, None),
        TaskOutcome::Lost => (SafeOutcome::Lost, None),
    }
}

pub(crate) fn task_hint(record: &LocalTaskRecord) -> TaskHint {
    TaskHint {
        task_id: record.meta().task_id(),
        run_id: record.meta().run_id(),
        turn_id: record.status().turns().last().map(|turn| turn.turn_id()),
        state: match record.status().state() {
            TaskState::Queued => "queued",
            TaskState::Active => "active",
            TaskState::Open => "open",
            TaskState::Closed => "closed",
            TaskState::Abandoned => "abandoned",
            TaskState::Lost => "lost",
        }
        .into(),
        code: record.abandon_code().map(safe_code).or_else(|| {
            record
                .status()
                .last_outcome()
                .and_then(|outcome| outcome_hint(outcome).1)
        }),
    }
}

fn result_imported(record: &LocalTaskRecord) -> bool {
    record
        .status()
        .head_oid()
        .is_some_and(|head| record.fetched_head() == Some(head))
}

pub(crate) fn capture_task_diff(
    old: &LocalTaskRecord,
    next: &LocalTaskRecord,
    mut capture: impl FnMut(NewEvent),
) {
    let before = task_hint(old);
    let after = task_hint(next);
    let terminal_changed = next.status().turns().iter().any(|turn| {
        turn.terminal().is_some()
            && turn.outcome().map(outcome_hint)
                != old
                    .status()
                    .turns()
                    .iter()
                    .find(|previous| previous.turn_id() == turn.turn_id())
                    .and_then(|previous| previous.outcome())
                    .map(outcome_hint)
    });
    let continuation = next.auto_continue_intent().map(|intent| intent.turn_id());
    if before != after
        || terminal_changed
        || old.status().last_outcome().map(outcome_hint)
            != next.status().last_outcome().map(outcome_hint)
        || old.runner().is_some() != next.runner().is_some()
        || result_imported(old) != result_imported(next)
        || old.close_intent().is_some() != next.close_intent().is_some()
        || old.auto_continue_intent().map(|intent| intent.turn_id()) != continuation
        || old.submission_intent_turn_id() != next.submission_intent_turn_id()
        || old.submission_rollback_turn_id() != next.submission_rollback_turn_id()
    {
        capture(NewEvent::TaskChanged(after.clone()));
    }
    if old.status().state() != next.status().state() {
        match next.status().state() {
            TaskState::Closed => capture(NewEvent::TaskClosed(after)),
            TaskState::Abandoned => capture(NewEvent::TaskAbandoned(after)),
            _ => {}
        }
    }
    for turn in next.status().turns() {
        let Some((outcome, code)) = turn
            .terminal()
            .and_then(|_| turn.outcome())
            .map(outcome_hint)
        else {
            continue;
        };
        let previous = old
            .status()
            .turns()
            .iter()
            .find(|previous| previous.turn_id() == turn.turn_id())
            .and_then(|previous| previous.terminal().and_then(|_| previous.outcome()))
            .map(outcome_hint);
        if previous.as_ref() == Some(&(outcome, code.clone())) {
            continue;
        }
        let hint = TurnHint {
            task_id: next.meta().task_id(),
            turn_id: turn.turn_id(),
            run_id: next.meta().run_id(),
            outcome,
            code,
        };
        capture(if previous.is_some() {
            NewEvent::TurnOutcomeChanged(hint)
        } else {
            NewEvent::TurnFinished(hint)
        });
    }
    if let Some(next_turn) = continuation
        && old.auto_continue_intent().map(|intent| intent.turn_id()) != Some(next_turn)
        && let Some(previous) = next.status().turns().last()
    {
        capture(NewEvent::AutoContinueScheduled {
            task_id: next.meta().task_id(),
            run_id: next.meta().run_id(),
            previous: previous.turn_id(),
            next: next_turn,
        });
    }
}

pub(crate) fn generic_queue_hint() -> NewEvent {
    NewEvent::QueueChanged(QueueHint {
        turn_id: None,
        state: None,
        kind: None,
        code: None,
    })
}

pub(crate) fn capture_queue_diff(
    old: &crate::job::QueueSnapshot,
    next: &crate::job::QueueSnapshot,
    mut capture: impl FnMut(NewEvent),
) {
    let mut changed = Vec::new();
    for entry in next.entries() {
        if old
            .entries()
            .iter()
            .find(|old| old.job_id() == entry.job_id())
            != Some(entry)
        {
            changed.push(NewEvent::QueueChanged(QueueHint {
                turn_id: Some(entry.job_id()),
                state: Some(
                    match entry.state() {
                        crate::job::QueueState::Waiting { .. } => "waiting",
                        crate::job::QueueState::Dispatching { .. } => "dispatching",
                        crate::job::QueueState::Parked => "parked",
                    }
                    .into(),
                ),
                kind: Some(
                    match entry.kind() {
                        crate::job::QueueEntryKind::Batch => "batch",
                        crate::job::QueueEntryKind::TaskTurn => "task_turn",
                    }
                    .into(),
                ),
                code: None,
            }));
            if changed.len() > MAX_HINTS {
                capture(generic_queue_hint());
                return;
            }
        }
    }
    for entry in old.entries() {
        if !next
            .entries()
            .iter()
            .any(|next| next.job_id() == entry.job_id())
        {
            changed.push(NewEvent::QueueChanged(QueueHint {
                turn_id: Some(entry.job_id()),
                state: None,
                kind: None,
                code: None,
            }));
            if changed.len() > MAX_HINTS {
                capture(generic_queue_hint());
                return;
            }
        }
    }
    for event in changed {
        capture(event);
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{cell::RefCell, collections::BTreeSet};

    use super::*;
    use crate::controller::events::{EventBatch, PublishAttempt, QueueHint};

    #[derive(Default)]
    pub(crate) struct CheckingSink {
        recorder: crate::controller::events::testing::RecordingSink,
        release_check: Option<Box<dyn Fn() -> bool + Send + Sync>>,
        pub(crate) unsafe_releases: std::sync::atomic::AtomicUsize,
    }

    impl CheckingSink {
        pub(crate) fn checking(check: impl Fn() -> bool + Send + Sync + 'static) -> Self {
            Self {
                release_check: Some(Box::new(check)),
                ..Self::default()
            }
        }
        pub(crate) fn events(&self) -> Vec<NewEvent> {
            self.recorder
                .batches()
                .into_iter()
                .flat_map(EventBatch::into_events)
                .collect()
        }
        pub(crate) fn batches(&self) -> Vec<Vec<NewEvent>> {
            self.recorder
                .batches()
                .into_iter()
                .map(EventBatch::into_events)
                .collect()
        }
        pub(crate) fn clear(&self) {
            self.recorder.take_batches();
        }
    }

    thread_local! { static FENCES: RefCell<BTreeSet<&'static str>> = RefCell::default(); }

    struct TestFence(&'static str);
    impl TestFence {
        fn enter(name: &'static str) -> Self {
            FENCES.with(|fences| {
                assert!(fences.borrow_mut().insert(name));
            });
            Self(name)
        }
    }
    impl Drop for TestFence {
        fn drop(&mut self) {
            FENCES.with(|fences| {
                fences.borrow_mut().remove(self.0);
            });
        }
    }

    impl EventSink for CheckingSink {
        fn try_publish(&self, batch: EventBatch) -> PublishAttempt {
            FENCES.with(|fences| assert!(fences.borrow().is_empty(), "released under a fence"));
            if self.release_check.as_ref().is_some_and(|check| !check()) {
                self.unsafe_releases.fetch_add(1, Ordering::Relaxed);
            }
            self.recorder.try_publish(batch)
        }
    }

    #[test]
    fn durable_hints_are_released_after_all_outer_fences() {
        let sink = Arc::new(CheckingSink::default());
        let scope = DeferredHints::begin(sink.clone());
        {
            let log = TestFence::enter("runner_log");
            let drain = TestFence::enter("drain");
            {
                let state = TestFence::enter("state");
                let nested = DeferredHints::begin(sink.clone());
                nested.capture(NewEvent::ControllerDrainChanged { drained: true });
                assert!(sink.batches().is_empty());
                drop(state);
            }
            assert!(sink.batches().is_empty());
            drop(drain);
            drop(log);
        }
        scope.finish();
        assert_eq!(sink.batches().len(), 1);
        assert_eq!(
            sink.events(),
            vec![NewEvent::ControllerDrainChanged { drained: true }]
        );
        sink.clear();
    }

    #[test]
    fn durable_hints_survive_error_unwinding() {
        fn fail(sink: Arc<CheckingSink>) -> Result<(), ()> {
            let scope = DeferredHints::begin(sink);
            let _state = TestFence::enter("state");
            scope.capture(NewEvent::ControllerDrainChanged { drained: true });
            Err(())
        }
        let sink = Arc::new(CheckingSink::default());
        let result = fail(sink.clone());
        assert!(result.is_err());
        assert_eq!(sink.events().len(), 1);
    }

    #[test]
    fn queue_overflow_becomes_one_generic_hint() {
        let sink = Arc::new(CheckingSink::default());
        let scope = DeferredHints::begin(sink.clone());
        for _ in 0..33 {
            scope.capture(NewEvent::QueueChanged(QueueHint {
                turn_id: Some(crate::task::TurnId::generate()),
                state: Some("parked".into()),
                kind: Some("task_turn".into()),
                code: None,
            }));
        }
        scope.finish();
        assert_eq!(
            sink.events(),
            vec![NewEvent::QueueChanged(QueueHint {
                turn_id: None,
                state: None,
                kind: None,
                code: None,
            })]
        );
    }
    #[test]
    fn run_overflow_becomes_one_generic_hint() {
        let sink = Arc::new(CheckingSink::default());
        let scope = DeferredHints::begin(sink.clone());
        let run_id = crate::task::RunId::generate();
        for _ in 0..40 {
            scope.capture(NewEvent::RunChanged {
                run_id,
                task_id: Some(crate::task::TaskId::generate()),
            });
        }
        scope.finish();
        assert_eq!(
            sink.events(),
            vec![NewEvent::RunChanged {
                run_id,
                task_id: None
            }]
        );
    }

    #[test]
    fn nested_bindings_release_to_their_own_sinks() {
        let first = Arc::new(CheckingSink::default());
        let second = Arc::new(CheckingSink::default());
        let outer = DeferredHints::begin(first.clone());
        let inner = DeferredHints::begin(second.clone());
        outer.capture(NewEvent::ControllerDrainChanged { drained: true });
        inner.capture(NewEvent::ControllerDrainChanged { drained: false });
        outer.finish();
        assert!(first.events().is_empty());
        assert!(second.events().is_empty());
        inner.finish();
        assert_eq!(
            first.events(),
            vec![NewEvent::ControllerDrainChanged { drained: true }]
        );
        assert_eq!(
            second.events(),
            vec![NewEvent::ControllerDrainChanged { drained: false }]
        );
    }

    #[test]
    fn non_coalescible_overflow_is_bounded() {
        let sink = Arc::new(CheckingSink::default());
        let scope = DeferredHints::begin(sink.clone());
        for _ in 0..64 {
            scope.capture(NewEvent::ControllerDrainChanged { drained: true });
        }
        scope.finish();
        assert_eq!(sink.events().len(), 32);
        assert_eq!(sink.batches().len(), 1);
    }

    use crate::{
        agent::{AgentKind, PermissionPolicy},
        client_state::ClientStateStore,
        task::{
            ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskId, TaskLimits, TaskMeta,
            TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnId, TurnSummary,
            TurnTerminal,
        },
    };

    pub(crate) fn task_record() -> LocalTaskRecord {
        let meta = TaskMeta::new(TaskMetaInput {
            session_import: None,
            task_id: TaskId::generate(),
            run_id: None,
            project_id: "a".repeat(64),
            worktree_id: "b".repeat(64),
            agent: AgentKind::Codex,
            model: None,
            effort: None,
            policy: PermissionPolicy::Workspace,
            source: TaskSource::Local {
                wip: false,
                push_target: None,
            },
            publish: vec![PublishMode::Fetch],
            publish_branch: None,
            base_oid: "0123456789abcdef0123456789abcdef01234567".parse().unwrap(),
            limits: TaskLimits::default(),
            close_policy: ClosePolicy::Never,
            env_profile: None,
            git_identity: GitIdentity::new("PRIVATE_TITLE", "private@example.test").unwrap(),
            title: None,
            prompt: "PRIVATE_PROMPT /private/project/path".into(),
            created_at_millis: 100,
        })
        .unwrap();
        let status = TaskStatus::new(
            TaskState::Queued,
            None,
            None,
            false,
            None,
            None,
            vec![],
            vec![],
            None,
            vec![TurnSummary::new(
                1,
                TurnId::generate(),
                None,
                None,
                None,
                false,
                None,
                None,
            )],
            100,
        )
        .unwrap();
        LocalTaskRecord::new(
            meta,
            status,
            None,
            None,
            None,
            "c".repeat(64),
            Some("mini-1".into()),
            true,
            None,
        )
        .unwrap()
    }

    pub(crate) fn terminal_record(
        record: &LocalTaskRecord,
        outcome: TaskOutcome,
    ) -> LocalTaskRecord {
        let turn = record.status().turns().last().unwrap();
        let status = TaskStatus::new(
            TaskState::Open,
            Some(outcome.clone()),
            Some("mini-1".into()),
            false,
            None,
            Some("PRIVATE_SUMMARY".into()),
            vec![],
            vec![],
            None,
            vec![TurnSummary::new(
                1,
                turn.turn_id(),
                Some(TurnTerminal::Succeeded),
                Some(outcome),
                None,
                false,
                Some(110),
                Some(120),
            )],
            120,
        )
        .unwrap();
        record.with_status(status).unwrap()
    }

    pub(crate) fn store_with_sink() -> (tempfile::TempDir, ClientStateStore, Arc<CheckingSink>) {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(CheckingSink::default());
        let store = ClientStateStore::open(&dir.path().canonicalize().unwrap().join("state"))
            .unwrap()
            .with_event_sink(sink.clone());
        (dir, store, sink)
    }

    pub(crate) fn locks_are_free(paths: &[std::path::PathBuf]) -> bool {
        use std::os::fd::AsRawFd;
        paths.iter().all(|path| {
            let lock = std::fs::File::open(path).unwrap();
            unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) == 0 }
        })
    }

    fn owner() -> crate::job::ProcessIdentity {
        crate::supervisor::SystemProcessInspector
            .identity_for_pid(std::process::id())
            .unwrap()
    }

    fn queue_row(store: &ClientStateStore, turn: TurnId) -> crate::job::QueueEntry {
        crate::job::QueueEntry::new(
            turn,
            store.client_id(),
            "a".repeat(64),
            "b".repeat(64),
            crate::job::CommandSummary::argv(1).unwrap(),
            vec![],
            crate::scheduler::WorkerPreference::Automatic,
            crate::job::QueueEntryKind::TaskTurn,
            None,
            owner(),
            100,
        )
        .unwrap()
    }

    #[test]
    fn every_queue_publication_path_is_covered() {
        let (_dir, store, sink) = store_with_sink();
        let record = task_record();
        let turn = record.status().turns()[0].turn_id();
        store.create_task(record.clone()).unwrap();
        store
            .write_turn_prompt(record.meta().task_id(), turn, "PRIVATE_PROMPT")
            .unwrap();
        sink.clear();
        store.enqueue(queue_row(&store, turn)).unwrap();
        assert!(
            matches!(&sink.events()[0], NewEvent::QueueChanged(hint) if hint.turn_id == Some(turn))
        );
        sink.clear();
        let crate::client_state::RunnerSlotDecision::Acquired { token } =
            store.reserve_runner_slot(turn, owner(), 8, false).unwrap()
        else {
            panic!("reservation missing");
        };
        sink.clear();
        store
            .complete_runner_spawn(record.meta().task_id(), turn, token, owner())
            .unwrap();
        assert!(sink.events().iter().any(
            |event| matches!(event, NewEvent::QueueChanged(hint) if hint.turn_id == Some(turn))
        ));
        sink.clear();
        store.park_row(turn).unwrap();
        assert!(
            matches!(&sink.events()[0], NewEvent::QueueChanged(hint) if hint.state.as_deref() == Some("parked"))
        );
        sink.clear();
        store.update_queue(|_| Ok(((), true))).unwrap();
        assert!(sink.events().is_empty());
    }

    #[test]
    fn queue_publication_reuses_pre_mutation_snapshot() {
        let (_dir, store, sink) = store_with_sink();
        let turn = TurnId::generate();
        store.enqueue(queue_row(&store, turn)).unwrap();
        sink.clear();
        let queue = store.inner.state_root.join("queue/state.json");
        use std::os::unix::fs::MetadataExt;
        let original = std::fs::metadata(&queue).unwrap();
        store
            .update_queue(|snapshot| {
                snapshot.entries[0].park()?;
                // A same-inode read witness: the caller has already read the
                // authoritative snapshot. Publication must not parse it again.
                std::fs::write(&queue, b"not a queue snapshot\n")?;
                let witness = std::fs::metadata(&queue)?;
                assert_eq!(
                    (witness.dev(), witness.ino()),
                    (original.dev(), original.ino())
                );
                Ok(((), true))
            })
            .unwrap();
        assert!(matches!(
            store.queue_entry(turn).unwrap().unwrap().state(),
            crate::job::QueueState::Parked
        ));
        assert!(
            matches!(sink.events().as_slice(), [NewEvent::QueueChanged(hint)]
            if hint.turn_id == Some(turn) && hint.state.as_deref() == Some("parked")),
            "queue hints must use the caller's pre-mutation snapshot, without rereading the file"
        );
    }

    #[test]
    fn queue_publication_without_previous_keeps_generic_hint() {
        use std::os::fd::AsRawFd;
        let (_dir, store, sink) = store_with_sink();
        let turn = TurnId::generate();
        store.enqueue(queue_row(&store, turn)).unwrap();
        sink.clear();
        let lock = store.acquire_queue_lock().unwrap();
        let (mut snapshot, identity) =
            crate::client_state::read_queue_snapshot(store.inner.queue.as_raw_fd()).unwrap();
        snapshot.entries[0].park().unwrap();
        crate::client_state::publish_queue_snapshot(&store, &snapshot, None, identity).unwrap();
        assert!(
            sink.events().is_empty(),
            "the queue fence still defers release"
        );
        drop(lock);
        assert!(
            matches!(sink.events().as_slice(), [NewEvent::QueueChanged(hint)] if hint.turn_id.is_none())
        );
        assert!(matches!(
            store.queue_entry(turn).unwrap().unwrap().state(),
            crate::job::QueueState::Parked
        ));
    }

    fn observation(time: u64) -> crate::job::AdmissionObservation {
        crate::job::AdmissionObservation::new(
            "mini-1".into(),
            true,
            crate::scheduler::CandidateSlot::Idle,
            vec![],
            None,
            1,
            time,
        )
        .unwrap()
    }

    #[test]
    fn removed_task_hint_survives_rooted_cleanup_error() {
        let (_dir, bare, _) = store_with_sink();
        let locked = vec![
            bare.inner.state_root.clone(),
            bare.inner.state_root.join("jobs.lock"),
        ];
        let sink = Arc::new(CheckingSink::checking(move || locks_are_free(&locked)));
        let store = bare.with_event_sink(sink.clone());
        let record = task_record();
        let task = record.meta().task_id();
        store.create_task(record).unwrap();
        sink.clear();
        let fault = crate::rooted_fs::fail_next_cleanup_before_final_root_removal(libc::EIO);
        let error = store.remove_task_submission_record(task).unwrap_err();
        assert!(
            matches!(error, crate::error::WorkerError::Io(ref error) if error.raw_os_error() == Some(libc::EIO))
        );
        assert!(
            store.load_task_optional(task).unwrap().is_none(),
            "deletion committed before cleanup failed"
        );
        assert!(
            sink.events()
                .iter()
                .any(|event| matches!(event, NewEvent::TaskRemoved(hint) if hint.task_id == task)),
            "committed tombstone survives rooted cleanup failure"
        );
        assert_eq!(sink.unsafe_releases.load(Ordering::Relaxed), 0);
        drop(fault);
        sink.clear();
        store.remove_task_submission_record(task).unwrap();
        assert!(sink.events().is_empty(), "an absent retry is silent");
    }

    #[test]
    fn attached_waiter_direct_queue_publication_is_captured() {
        let (_dir, bare, _) = store_with_sink();
        let locked = vec![
            bare.inner.state_root.clone(),
            bare.inner.state_root.join("jobs.lock"),
            bare.inner.state_root.join("queue/lock"),
        ];
        let sink = Arc::new(CheckingSink::checking(move || locks_are_free(&locked)));
        let store = bare.with_event_sink(sink.clone());
        let turn = TurnId::generate();
        store.enqueue(queue_row(&store, turn)).unwrap();
        sink.clear();
        store.try_park_attached_waiter(turn, owner()).unwrap();
        assert!(sink.events().iter().any(|event| matches!(event, NewEvent::QueueChanged(hint) if hint.turn_id == Some(turn) && hint.state.as_deref() == Some("parked"))));
        assert_eq!(sink.unsafe_releases.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn observation_cas_loser_and_ttl_are_silent() {
        let (_dir, store, sink) = store_with_sink();
        let first = observation(100);
        store.publish_admission_observation(first.clone()).unwrap();
        assert!(matches!(
            &sink.events()[0],
            NewEvent::WorkerChanged {
                ready: Some(true),
                observed_at_millis: 100,
                ..
            }
        ));
        sink.clear();
        assert_eq!(
            store
                .commit_admission_observation("mini-1", None, observation(110))
                .unwrap(),
            first
        );
        assert!(
            store
                .admission_observation("mini-1", 5000, || Err(crate::error::WorkerError::Protocol(
                    "offline fixture".into()
                )))
                .is_err()
        );
        assert!(sink.events().is_empty());
        store
            .commit_admission_observation("mini-1", Some(&first), observation(110))
            .unwrap();
        assert_eq!(sink.events().len(), 1);
        sink.clear();
        store
            .publish_admission_observation(observation(110))
            .unwrap();
        assert!(sink.events().is_empty());
        store.invalidate_admission_observation("mini-1").unwrap();
        assert!(matches!(
            &sink.events()[0],
            NewEvent::WorkerChanged { ready: None, .. }
        ));
        sink.clear();
        store.invalidate_admission_observation("mini-1").unwrap();
        assert!(sink.events().is_empty());
    }

    #[test]
    fn real_log_fence_delays_started_and_terminal_hints() {
        use crate::controller::events::{AcceptedHint, WorkerName};
        let (_dir, bare, _) = store_with_sink();
        let record = task_record();
        bare.create_task(record.clone()).unwrap();
        let task = record.meta().task_id();
        let turn = record.status().turns()[0].turn_id();
        let log = crate::runner_log::RunnerLog::open(&bare.inner.state_root, task, turn).unwrap();
        let locked = vec![
            bare.inner.state_root.clone(),
            bare.inner.state_root.join("jobs.lock"),
            bare.inner
                .state_root
                .join(format!("runners/{task}/{turn}.log")),
        ];
        let sink = Arc::new(CheckingSink::checking(move || locks_are_free(&locked)));
        let store = bare.with_event_sink(sink.clone());
        store.capture_accepted_turn(AcceptedHint {
            task_id: task,
            turn_id: turn,
            run_id: None,
            worker: WorkerName::parse("mini-1").unwrap(),
        });
        let terminal = terminal_record(&record, TaskOutcome::Done);
        store.update_task_if_current(&record, terminal).unwrap();
        assert!(
            sink.events().is_empty(),
            "existing log fence may span the entire turn"
        );
        drop(log);
        assert!(
            sink.events()
                .iter()
                .any(|event| matches!(event, NewEvent::TurnStarted(_)))
        );
        assert!(
            sink.events()
                .iter()
                .any(|event| matches!(event, NewEvent::TurnFinished(_)))
        );
        assert_eq!(sink.unsafe_releases.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn real_launch_permit_defers_nested_store_hints() {
        let (_dir, store, sink) = store_with_sink();
        let root = store.inner.state_root.parent().unwrap().join("controller");
        crate::controller::drain::set_drained(&root, false).unwrap();
        let permit = crate::controller::drain::launch_permit(&root, store.wait_deadline())
            .unwrap()
            .unwrap();
        store.create_task(task_record()).unwrap();
        assert!(sink.events().is_empty());
        drop(permit);
        assert_eq!(sink.events().len(), 1);
    }

    #[test]
    fn sink_survives_clone_deadline_handle_and_bound_reopen() {
        let (_dir, store, sink) = store_with_sink();
        let deadline = store.with_wait_deadline(crate::client_state::WaitDeadline::default());
        deadline.clone().create_task(task_record()).unwrap();
        assert_eq!(sink.events().len(), 1);
        sink.clear();
        let reopened = store.reopen_until(None).unwrap();
        reopened.create_task(task_record()).unwrap();
        assert_eq!(sink.events().len(), 1);
    }

    #[test]
    fn continuation_precedes_retirement() {
        let (_dir, store, sink) = store_with_sink();
        let record = terminal_record(&task_record(), TaskOutcome::NeedsInput)
            .with_questions_policy(crate::task::QuestionsPolicy::Decide)
            .with_runner(Some(crate::task::RunnerIdentity::new(owner())))
            .unwrap();
        let task = record.meta().task_id();
        let turn = record.status().turns()[0].turn_id();
        store.create_task(record).unwrap();
        sink.clear();
        crate::task_client::stage_auto_continue_before_retirement(&store, task, turn).unwrap();
        assert!(
            store
                .load_task(task)
                .unwrap()
                .auto_continue_intent()
                .is_some()
        );
        assert!(store.load_task(task).unwrap().runner().is_some());
        assert_eq!(
            sink.events()
                .iter()
                .filter(|event| matches!(event, NewEvent::AutoContinueScheduled { .. }))
                .count(),
            1
        );
        sink.clear();
        crate::task_client::stage_auto_continue_before_retirement(&store, task, turn).unwrap();
        assert!(sink.events().is_empty());
        store.record_runner(task, None).unwrap();
        assert!(
            !sink
                .events()
                .iter()
                .any(|event| matches!(event, NewEvent::AutoContinueScheduled { .. }))
        );
        sink.clear();
        store
            .mutate_task(task, Some(turn), |current| {
                current.with_auto_continue_intent(None)
            })
            .unwrap();
        assert!(
            sink.events()
                .iter()
                .any(|event| matches!(event, NewEvent::TaskChanged(_)))
        );
    }

    #[test]
    fn central_queue_capture_covers_dispatch_bypass() {
        let (dir, store, sink) = store_with_sink();
        let donor = task_record();
        let recipient = task_record();
        store.create_task(donor.clone()).unwrap();
        store.create_task(recipient.clone()).unwrap();
        store.write_task_project_path(&donor, dir.path()).unwrap();
        let from = donor.status().turns()[0].turn_id();
        let to = recipient.status().turns()[0].turn_id();
        store
            .write_turn_prompt(donor.meta().task_id(), from, "PRIVATE_PROMPT")
            .unwrap();
        store
            .write_turn_prompt(recipient.meta().task_id(), to, "PRIVATE_PROMPT")
            .unwrap();
        let mut donor_row = queue_row(&store, from);
        donor_row.preference = crate::scheduler::WorkerPreference::Pinned {
            worker: "mini-2".into(),
        };
        store.enqueue(donor_row).unwrap();
        store.enqueue(queue_row(&store, to)).unwrap();
        store.park_row(to).unwrap();
        sink.clear();
        let observation = crate::scheduler::CandidateObservation::new(
            "mini-1".into(),
            true,
            crate::scheduler::CandidateSlot::Idle,
            vec![],
            None,
            1,
        )
        .unwrap();
        let selected = store
            .claim_parked_for_waiting_runner(
                donor.meta().task_id(),
                from,
                owner(),
                &[observation],
                120,
            )
            .unwrap();
        assert_eq!(selected.unwrap().0, recipient.meta().task_id());
        assert!(
            sink.events()
                .iter()
                .any(|event| matches!(event, NewEvent::QueueChanged(hint)
            if hint.turn_id == Some(to) && hint.state.as_deref() == Some("dispatching")))
        );
        assert!(
            sink.events()
                .iter()
                .any(|event| matches!(event, NewEvent::QueueChanged(hint)
            if hint.turn_id == Some(from) && hint.state.as_deref() == Some("parked")))
        );
    }
}
