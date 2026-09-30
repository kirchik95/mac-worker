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
}
