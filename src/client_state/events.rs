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
    SafeCode::from_public_code(match code {
        "PUBLISH_FAILED"
        | "RESULT_FETCH_FAILED"
        | "RESULT_UNPARSEABLE"
        | "LOG_DRAIN_UNAVAILABLE"
        | "RUNNER_HANDOFF_FAILED"
        | "CAPACITY_BUSY"
        | "TASK_CANCELLED"
        | "TASK_LOST"
        | "SUBMISSION_ROLLBACK_INCOMPLETE"
        | "AUTO_CONTINUE_FAILED"
        | "TURN_FAILED" => code,
        _ => "TURN_FAILED",
    })
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
        if previous.as_ref() == Some(&(outcome.clone(), code.clone())) {
            continue;
        }
        let hint = TurnHint {
            task_id: after_task_id(next),
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

fn after_task_id(record: &LocalTaskRecord) -> crate::task::TaskId {
    record.meta().task_id()
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
    use std::{cell::RefCell, collections::BTreeSet, sync::Mutex};

    use super::*;
    use crate::controller::events::{EventBatch, PublishAttempt, QueueHint};

    #[derive(Default)]
    pub(crate) struct RecordingSink {
        events: Mutex<Vec<Vec<NewEvent>>>,
        release_check: Option<Box<dyn Fn() -> bool + Send + Sync>>,
        pub(crate) unsafe_releases: std::sync::atomic::AtomicUsize,
    }

    impl RecordingSink {
        pub(crate) fn checking(check: impl Fn() -> bool + Send + Sync + 'static) -> Self {
            Self {
                release_check: Some(Box::new(check)),
                ..Self::default()
            }
        }
        pub(crate) fn events(&self) -> Vec<NewEvent> {
            self.events
                .lock()
                .unwrap()
                .iter()
                .flatten()
                .cloned()
                .collect()
        }
        pub(crate) fn batches(&self) -> Vec<Vec<NewEvent>> {
            self.events.lock().unwrap().clone()
        }
        pub(crate) fn clear(&self) {
            self.events.lock().unwrap().clear();
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

    impl EventSink for RecordingSink {
        fn try_publish(&self, batch: EventBatch) -> PublishAttempt {
            FENCES.with(|fences| assert!(fences.borrow().is_empty(), "released under a fence"));
            if self.release_check.as_ref().is_some_and(|check| !check()) {
                self.unsafe_releases.fetch_add(1, Ordering::Relaxed);
            }
            self.events.lock().unwrap().push(batch.0);
            PublishAttempt::Queued
        }
    }

    #[test]
    fn durable_hints_are_released_after_all_outer_fences() {
        let sink = Arc::new(RecordingSink::default());
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
        let sink = Arc::new(RecordingSink::default());
        let result: Result<(), ()> = (|| {
            let scope = DeferredHints::begin(sink.clone());
            let _state = TestFence::enter("state");
            scope.capture(NewEvent::ControllerDrainChanged { drained: true });
            Err(())
        })();
        assert!(result.is_err());
        assert_eq!(sink.events().len(), 1);
    }

    #[test]
    fn queue_overflow_becomes_one_generic_hint() {
        let sink = Arc::new(RecordingSink::default());
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
    use crate::{
        agent::{AgentKind, PermissionPolicy},
        client_state::{ClientStateStore, ClientStateWritePoint},
        task::{
            ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskId, TaskLimits, TaskMeta,
            TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnId, TurnSummary,
            TurnTerminal,
        },
    };

    pub(crate) fn task_record() -> LocalTaskRecord {
        let meta = TaskMeta::new(TaskMetaInput {
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

    pub(crate) fn store_with_sink() -> (tempfile::TempDir, ClientStateStore, Arc<RecordingSink>) {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(RecordingSink::default());
        let store = ClientStateStore::open(&dir.path().canonicalize().unwrap().join("state"))
            .unwrap()
            .with_event_sink(sink.clone());
        (dir, store, sink)
    }

    #[test]
    fn task_creation_is_captured_after_publication() {
        let (_dir, store, sink) = store_with_sink();
        let record = task_record();
        store.create_task(record.clone()).unwrap();
        assert_eq!(sink.events().len(), 1);
        assert!(matches!(&sink.events()[0], NewEvent::TaskCreated(hint)
            if hint.task_id == record.meta().task_id() && hint.state == "queued"));
        let printed = format!("{:?}", sink.events());
        for private in [
            "PRIVATE_TITLE",
            "PRIVATE_PROMPT",
            "/private/project/path",
            "PRIVATE_SUMMARY",
        ] {
            assert!(!printed.contains(private));
        }
    }

    #[test]
    fn losing_cas_and_noop_are_silent() {
        let (_dir, store, sink) = store_with_sink();
        let record = task_record();
        store.create_task(record.clone()).unwrap();
        sink.clear();
        store.create_task(record.clone()).unwrap();
        assert!(
            store
                .update_task_if_current(&record, record.clone())
                .unwrap()
        );
        assert!(sink.events().is_empty());
        let terminal = terminal_record(&record, TaskOutcome::Done);
        assert!(
            store
                .update_task_if_current(&record, terminal.clone())
                .unwrap()
        );
        assert!(
            sink.events()
                .iter()
                .any(|event| matches!(event, NewEvent::TurnFinished(_)))
        );
        sink.clear();
        assert!(
            !store
                .update_task_if_current(&record, record.clone())
                .unwrap()
        );
        store
            .mutate_task(
                record.meta().task_id(),
                Some(record.status().turns()[0].turn_id()),
                |current| current.with_status(current.status().clone()),
            )
            .unwrap();
        assert!(sink.events().is_empty());
    }

    #[test]
    fn no_hint_before_durability() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(RecordingSink::default());
        let store = ClientStateStore::open_with_write_fault(
            &dir.path().canonicalize().unwrap().join("state"),
            ClientStateWritePoint::AfterActiveTaskIndexBeforeTaskPublish,
        )
        .unwrap()
        .with_event_sink(sink.clone());
        assert!(store.create_task(task_record()).is_err());
        assert!(sink.events().is_empty());
    }

    #[test]
    fn terminal_correction_emits_outcome_changed() {
        let (_dir, store, sink) = store_with_sink();
        let record = task_record();
        store.create_task(record.clone()).unwrap();
        let terminal = terminal_record(&record, TaskOutcome::Done);
        assert!(
            store
                .update_task_if_current(&record, terminal.clone())
                .unwrap()
        );
        sink.clear();
        let corrected = terminal_record(&terminal, TaskOutcome::failed("PUBLISH_FAILED"));
        assert!(store.update_task_if_current(&terminal, corrected).unwrap());
        assert!(
            sink.events()
                .iter()
                .any(|event| matches!(event, NewEvent::TurnOutcomeChanged(_)))
        );
        assert!(
            !sink
                .events()
                .iter()
                .any(|event| matches!(event, NewEvent::TurnFinished(_)))
        );
    }

    #[test]
    fn bare_local_store_is_silent() {
        let (_dir, store, sink) = store_with_sink();
        let bare = ClientStateStore::open(&store.inner.state_root).unwrap();
        bare.create_task(task_record()).unwrap();
        assert!(sink.events().is_empty());
    }

    pub(crate) fn locks_are_free(paths: &[std::path::PathBuf]) -> bool {
        use std::os::fd::AsRawFd;
        paths.iter().all(|path| {
            let lock = std::fs::File::open(path).unwrap();
            unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) == 0 }
        })
    }

    #[test]
    fn durable_task_hint_survives_index_cleanup_error() {
        let (_dir, store, sink) = store_with_sink();
        let record = task_record();
        store.create_task(record.clone()).unwrap();
        sink.clear();
        let terminal = terminal_record(&record, TaskOutcome::Done);
        let status = terminal.status();
        let closed = terminal
            .with_status(
                TaskStatus::new(
                    TaskState::Closed,
                    status.last_outcome().cloned(),
                    status.worker().map(str::to_owned),
                    false,
                    None,
                    None,
                    vec![],
                    vec![],
                    None,
                    status.turns().to_vec(),
                    130,
                )
                .unwrap(),
            )
            .unwrap();
        store.inject_write_failure_once(ClientStateWritePoint::AfterQuiescentTaskBeforeIndexRetire);
        assert!(
            store
                .update_task_if_current(&record, closed.clone())
                .is_err()
        );
        assert_eq!(store.load_task(record.meta().task_id()).unwrap(), closed);
        assert!(
            sink.events()
                .iter()
                .any(|event| matches!(event, NewEvent::TaskClosed(_)))
        );
    }

    #[test]
    fn task_hint_after_real_state_lock_release() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap().join("state");
        let bare = ClientStateStore::open(&root).unwrap();
        let locked = vec![root.clone(), root.join("jobs.lock")];
        let sink = Arc::new(RecordingSink::checking(move || locks_are_free(&locked)));
        let store = bare.with_event_sink(sink.clone());
        store.create_task(task_record()).unwrap();
        assert_eq!(sink.events().len(), 1);
        assert_eq!(sink.unsafe_releases.load(Ordering::Relaxed), 0);
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
        assert!(
            sink.events()
                .iter()
                .filter(|event| matches!(event, NewEvent::QueueChanged(_)))
                .count()
                >= 1
        );
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
    fn queue_hint_after_real_queue_and_state_locks_release() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap().join("state");
        let bare = ClientStateStore::open(&root).unwrap();
        let locked = vec![
            root.clone(),
            root.join("jobs.lock"),
            root.join("queue/lock"),
        ];
        let sink = Arc::new(RecordingSink::checking(move || locks_are_free(&locked)));
        let store = bare.with_event_sink(sink.clone());
        store
            .enqueue(queue_row(&store, TurnId::generate()))
            .unwrap();
        assert_eq!(sink.events().len(), 1);
        assert_eq!(sink.unsafe_releases.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn queue_prepublication_failure_is_silent() {
        let (_dir, store, sink) = store_with_sink();
        store.inject_write_failure_once(ClientStateWritePoint::BeforePublish);
        assert!(
            store
                .enqueue(queue_row(&store, TurnId::generate()))
                .is_err()
        );
        assert!(sink.events().is_empty());
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
    fn affinity_changes_emit_only_generic_queue_invalidation() {
        let (_dir, store, sink) = store_with_sink();
        store
            .record_affinity(
                "a".repeat(64).as_str(),
                "b".repeat(64).as_str(),
                "mini-1",
                100,
            )
            .unwrap();
        assert_eq!(sink.events().len(), 1);
        assert!(
            matches!(&sink.events()[0], NewEvent::QueueChanged(hint) if hint.turn_id.is_none())
        );
        sink.clear();
        store
            .remove_affinity_if_matches(&"a".repeat(64), &"b".repeat(64), "mini-1")
            .unwrap();
        assert_eq!(sink.events().len(), 1);
        let encoded = format!("{:?}", sink.events());
        assert!(!encoded.contains(&"a".repeat(64)));
        assert!(!encoded.contains(&"b".repeat(64)));
    }

    fn run_and_dag(record: &LocalTaskRecord) -> (crate::task::RunRecord, crate::dag::DagRecord) {
        use crate::dag::{DagBase, DagFrozenSpec, DagNode, DagNodeState, DagRecord, dag_pin_ref};
        let id = crate::task::RunId::generate();
        let node = DagNode {
            batch_id: "private-node".into(),
            task_id: record.meta().task_id(),
            turn_id: record.status().turns()[0].turn_id(),
            depends_on: vec![],
            base: DagBase::Frozen {
                oid: record.meta().base_oid().clone(),
                pin_ref: dag_pin_ref(id, "private-node"),
                wip: false,
            },
            frozen: DagFrozenSpec {
                questions: None,
                prompt: "PRIVATE_PROMPT".into(),
                title: Some("PRIVATE_TITLE".into()),
                agent: "codex".into(),
                model: None,
                effort: None,
                source: "local".into(),
                origin_url: None,
                publish: vec!["fetch".into()],
                publish_branch: None,
                close_on: ClosePolicy::Never,
                env_profile: None,
                worker: None,
                wip: false,
                project_path: "/private/project/path".into(),
                project_id: "a".repeat(64),
                worktree_id: "b".repeat(64),
                timeout_millis: 1000,
                max_turns: None,
                max_budget_usd_cents: None,
                max_followups: 10,
                permissions: "workspace".into(),
                requires: vec![],
                include_untracked: vec![],
                include_empty_dirs: vec![],
                allow_sensitive: vec![],
                cli_includes: vec![],
                branch: None,
            },
            state: DagNodeState::Waiting,
            bound_oid: None,
            bound_turn_id: None,
            pin_ref: None,
            blocked_by: None,
            claimed_by: None,
            claimed_at_millis: None,
        };
        (
            crate::task::RunRecord::new(id, None, vec![], 1, 100).unwrap(),
            DagRecord::new(
                id,
                std::collections::BTreeMap::from([("private-node".into(), node)]),
                1,
                None,
                100,
            )
            .unwrap(),
        )
    }

    #[test]
    fn run_creation_and_reservations_invalidate_without_settlement() {
        let (_dir, store, sink) = store_with_sink();
        let (run, _) = run_and_dag(&task_record());
        store.create_run(run.clone()).unwrap();
        assert_eq!(sink.events().len(), 1);
        assert!(
            matches!(&sink.events()[0], NewEvent::RunChanged { run_id, .. } if *run_id == run.run_id())
        );
        sink.clear();
        store.create_run(run.clone()).unwrap();
        assert!(sink.events().is_empty());
        let branch = "agent/private-branch".parse().unwrap();
        store
            .reserve_run_publish_branch(run.run_id(), branch)
            .unwrap();
        assert_eq!(sink.events().len(), 1);
        assert!(!format!("{:?}", sink.events()).contains("private-branch"));
    }

    #[test]
    fn partial_dag_creation_does_not_admit_child() {
        let (_dir, store, sink) = store_with_sink();
        let (run, dag) = run_and_dag(&task_record());
        store.inject_write_failure_once(ClientStateWritePoint::AfterDagPublishBeforeRun);
        assert!(store.create_run_with_dag(run.clone(), dag).is_err());
        assert!(store.load_run_dag(run.run_id()).unwrap().is_some());
        assert!(
            sink.events()
                .iter()
                .any(|event| matches!(event, NewEvent::RunChanged { .. }))
        );
        assert!(
            !sink
                .events()
                .iter()
                .any(|event| matches!(event, NewEvent::DagChildAdmitted { .. }))
        );
        sink.clear();
        store
            .claim_next_eligible_dag_node(run.run_id(), owner(), 110)
            .unwrap();
        assert!(store.load_run(run.run_id()).is_ok());
        assert!(
            sink.events()
                .iter()
                .any(|event| matches!(event, NewEvent::RunChanged { .. }))
        );
    }

    #[test]
    fn dag_recovery_admission_requires_two_records() {
        let (_dir, store, sink) = store_with_sink();
        let record = task_record();
        let (run, dag) = run_and_dag(&record);
        store.create_run_with_dag(run.clone(), dag).unwrap();
        store
            .claim_next_eligible_dag_node(run.run_id(), owner(), 110)
            .unwrap();
        store.create_task(record.clone()).unwrap();
        store
            .write_turn_prompt(
                record.meta().task_id(),
                record.status().turns()[0].turn_id(),
                "PRIVATE_PROMPT",
            )
            .unwrap();
        sink.clear();
        store.inject_write_failure_once(ClientStateWritePoint::AfterDagRunMembership);
        assert!(
            store
                .mark_dag_node_submitted(run.run_id(), "private-node", record.meta().task_id())
                .is_err()
        );
        assert!(
            !sink
                .events()
                .iter()
                .any(|event| matches!(event, NewEvent::DagChildAdmitted { .. }))
        );
        sink.clear();
        store
            .claim_next_eligible_dag_node(run.run_id(), owner(), 120)
            .unwrap();
        assert!(sink.events().iter().any(|event| matches!(event,
            NewEvent::DagChildAdmitted { run_id, task_id, turn_id } if *run_id == run.run_id()
                && *task_id == record.meta().task_id() && *turn_id == record.status().turns()[0].turn_id())));
        assert!(!format!("{:?}", sink.events()).contains("private-node"));
        sink.clear();
        store
            .claim_next_eligible_dag_node(run.run_id(), owner(), 130)
            .unwrap();
        assert!(sink.events().is_empty());
    }
}
