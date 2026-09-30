use mac_worker::controller::events::{
    EventBatch, EventSink, NewEvent, PublishAttempt, SafeOutcome, testing::RecordingSink,
};

#[test]
fn producer_sink_drops_a_whole_batch() {
    let sink = RecordingSink::new();
    sink.set_drop_mode(true);
    let batch =
        EventBatch::try_new(vec![NewEvent::ControllerDrainChanged { drained: true }; 2]).unwrap();
    assert_eq!(sink.try_publish(batch.clone()), PublishAttempt::Dropped);
    assert!(sink.batches().is_empty());
    sink.set_drop_mode(false);
    assert_eq!(sink.try_publish(batch), PublishAttempt::Queued);
    assert_eq!(sink.batches()[0].len(), 2);
}

use mac_worker::{
    agent::{AgentKind, PermissionPolicy},
    client_state::{ClientStateStore, ClientStateWritePoint},
    task::{
        ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskId, TaskLimits, TaskMeta,
        TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnId, TurnSummary,
        TurnTerminal,
    },
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct CheckingSink {
    recorder: RecordingSink,
    release_check: Option<Box<dyn Fn() -> bool + Send + Sync>>,
    unsafe_releases: AtomicUsize,
}
impl CheckingSink {
    fn checking(check: impl Fn() -> bool + Send + Sync + 'static) -> Self {
        Self {
            release_check: Some(Box::new(check)),
            ..Self::default()
        }
    }
    fn events(&self) -> Vec<NewEvent> {
        self.recorder
            .batches()
            .into_iter()
            .flat_map(EventBatch::into_events)
            .collect()
    }
    fn clear(&self) {
        self.recorder.take_batches();
    }
}
impl EventSink for CheckingSink {
    fn try_publish(&self, batch: EventBatch) -> PublishAttempt {
        if self.release_check.as_ref().is_some_and(|check| !check()) {
            self.unsafe_releases.fetch_add(1, Ordering::Relaxed);
        }
        self.recorder.try_publish(batch)
    }
}

fn task_record() -> LocalTaskRecord {
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
        title: Some("PRIVATE_TITLE".into()),
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

fn terminal_record(record: &LocalTaskRecord, outcome: TaskOutcome) -> LocalTaskRecord {
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

fn store_with_sink() -> (tempfile::TempDir, ClientStateStore, Arc<CheckingSink>) {
    let dir = tempfile::tempdir().unwrap();
    let sink = Arc::new(CheckingSink::default());
    let store = ClientStateStore::open(&dir.path().canonicalize().unwrap().join("state"))
        .unwrap()
        .with_event_sink(sink.clone());
    (dir, store, sink)
}

fn locks_are_free(paths: &[std::path::PathBuf]) -> bool {
    use std::os::fd::AsRawFd;
    paths.iter().all(|path| {
        let lock = std::fs::File::open(path).unwrap();
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) == 0 }
    })
}

fn owner() -> mac_worker::job::ProcessIdentity {
    mac_worker::supervisor::SystemProcessInspector
        .identity_for_pid(std::process::id())
        .unwrap()
}

fn queue_row(store: &ClientStateStore, turn: TurnId) -> mac_worker::job::QueueEntry {
    mac_worker::job::QueueEntry::new(
        turn,
        store.client_id(),
        "a".repeat(64),
        "b".repeat(64),
        mac_worker::job::CommandSummary::argv(1).unwrap(),
        vec![],
        mac_worker::scheduler::WorkerPreference::Automatic,
        mac_worker::job::QueueEntryKind::TaskTurn,
        None,
        owner(),
        100,
    )
    .unwrap()
}

fn run_and_dag(
    record: &LocalTaskRecord,
) -> (mac_worker::task::RunRecord, mac_worker::dag::DagRecord) {
    use mac_worker::dag::{DagBase, DagFrozenSpec, DagNode, DagNodeState, DagRecord, dag_pin_ref};
    let id = mac_worker::task::RunId::generate();
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
        mac_worker::task::RunRecord::new(id, None, vec![], 1, 100).unwrap(),
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
fn task_creation_is_captured_after_publication() {
    let (_dir, store, sink) = store_with_sink();
    let record = task_record();
    store.create_task(record.clone()).unwrap();
    assert_eq!(sink.events().len(), 1);
    assert!(matches!(&sink.events()[0], NewEvent::TaskCreated(hint)
        if hint.task_id == record.meta().task_id() && hint.state == "queued"));
    let printed = serialize_hints(&sink.events());
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
    let sink = Arc::new(CheckingSink::default());
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
    let (dir, _store, sink) = store_with_sink();
    let bare = ClientStateStore::open(&dir.path().canonicalize().unwrap().join("state")).unwrap();
    bare.create_task(task_record()).unwrap();
    assert!(sink.events().is_empty());
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
    let sink = Arc::new(CheckingSink::checking(move || locks_are_free(&locked)));
    let store = bare.with_event_sink(sink.clone());
    store.create_task(task_record()).unwrap();
    assert_eq!(sink.events().len(), 1);
    assert_eq!(sink.unsafe_releases.load(Ordering::Relaxed), 0);
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
    let sink = Arc::new(CheckingSink::checking(move || locks_are_free(&locked)));
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
    assert!(matches!(&sink.events()[0], NewEvent::QueueChanged(hint) if hint.turn_id.is_none()));
    sink.clear();
    store
        .remove_affinity_if_matches(&"a".repeat(64), &"b".repeat(64), "mini-1")
        .unwrap();
    assert_eq!(sink.events().len(), 1);
    let encoded = serialize_hints(&sink.events());
    assert!(!encoded.contains(&"a".repeat(64)));
    assert!(!encoded.contains(&"b".repeat(64)));
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
    assert!(!serialize_hints(&sink.events()).contains("private-branch"));
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
    assert!(!serialize_hints(&sink.events()).contains("private-node"));
    sink.clear();
    store
        .claim_next_eligible_dag_node(run.run_id(), owner(), 130)
        .unwrap();
    assert!(sink.events().is_empty());
}

#[test]
fn accepted_status_proof_required() {
    use mac_worker::controller::events::{AcceptedHint, WorkerName};
    let (_dir, store, sink) = store_with_sink();
    let record = task_record();
    store.create_task(record.clone()).unwrap();
    let turn = record.status().turns()[0].turn_id();
    let prepared = TaskStatus::new(
        TaskState::Active,
        None,
        Some("mini-1".into()),
        false,
        None,
        None,
        vec![],
        vec![],
        None,
        record.status().turns().to_vec(),
        110,
    )
    .unwrap();
    store
        .mutate_task(record.meta().task_id(), Some(turn), |current| {
            current.with_status(prepared)
        })
        .unwrap();
    assert!(
        !sink
            .events()
            .iter()
            .any(|event| matches!(event, NewEvent::TurnStarted(_)))
    );
    sink.clear();
    store.capture_accepted_turn(AcceptedHint {
        task_id: record.meta().task_id(),
        turn_id: turn,
        run_id: record.meta().run_id(),
        worker: WorkerName::parse("mini-1").unwrap(),
    });
    assert_eq!(sink.events().len(), 1);
    assert!(matches!(&sink.events()[0], NewEvent::TurnStarted(hint) if hint.turn_id == turn));
}

#[test]
fn drain_hint_after_lock_release() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap().join("controller");
    mac_worker::controller::drain::set_drained(&root, false).unwrap();
    let locked = vec![root.join("drain.lock")];
    let sink = Arc::new(CheckingSink::checking(move || locks_are_free(&locked)));
    mac_worker::controller::drain::set_drained_with_event_sink(&root, true, Some(sink.clone()))
        .unwrap();
    assert_eq!(
        sink.events(),
        vec![NewEvent::ControllerDrainChanged { drained: true }]
    );
    assert_eq!(sink.unsafe_releases.load(Ordering::Relaxed), 0);
    sink.clear();
    mac_worker::controller::drain::set_drained_with_event_sink(&root, true, Some(sink.clone()))
        .unwrap();
    assert!(sink.events().is_empty());
    mac_worker::controller::drain::set_drained_with_event_sink(&root, false, Some(sink.clone()))
        .unwrap();
    assert_eq!(
        sink.events(),
        vec![NewEvent::ControllerDrainChanged { drained: false }]
    );
}

#[test]
fn reopen_refuses_replaced_store_binding() {
    let (dir, store, _sink) = store_with_sink();
    let root = dir.path().canonicalize().unwrap().join("state");
    std::fs::rename(&root, root.with_extension("retired")).unwrap();
    ClientStateStore::open(&root).unwrap();
    assert!(store.reopen_until(None).is_err());
}

#[test]
fn removed_task_hint_survives_later_rollback_error() {
    let (_dir, store, sink) = store_with_sink();
    let record = task_record();
    let task = record.meta().task_id();
    store.create_task(record).unwrap();
    sink.clear();
    store.inject_write_failure_once(ClientStateWritePoint::AfterTaskSubmissionRecordRemoval);
    assert!(store.remove_task_submission_record(task).is_err());
    assert!(store.load_task_optional(task).unwrap().is_none());
    assert!(matches!(&sink.events()[0], NewEvent::TaskRemoved(hint) if hint.task_id == task));
    sink.clear();
    store.remove_task_submission_record(task).unwrap();
    assert!(sink.events().is_empty());
}

#[test]
fn all_terminal_outcomes_are_safe_and_prose_free() {
    for (outcome, expected) in [
        (TaskOutcome::Done, SafeOutcome::Done),
        (TaskOutcome::NeedsInput, SafeOutcome::NeedsInput),
        (TaskOutcome::Blocked, SafeOutcome::Blocked),
        (TaskOutcome::Unknown, SafeOutcome::Unknown),
        (
            TaskOutcome::failed("PRIVATE_SECRET /private/project/path"),
            SafeOutcome::Failed,
        ),
        (TaskOutcome::Cancelled, SafeOutcome::Cancelled),
        (TaskOutcome::TimedOut, SafeOutcome::TimedOut),
        (TaskOutcome::Lost, SafeOutcome::Lost),
    ] {
        let (_dir, store, sink) = store_with_sink();
        let record = task_record();
        store.create_task(record.clone()).unwrap();
        sink.clear();
        store
            .update_task_if_current(&record, terminal_record(&record, outcome))
            .unwrap();
        assert!(sink.events().iter().any(
            |event| matches!(event, NewEvent::TurnFinished(hint) if hint.outcome == expected)
        ));
        let encoded = serialize_hints(&sink.events());
        assert!(!encoded.contains("PRIVATE_SECRET"));
        assert!(!encoded.contains("/private/project/path"));
    }
}

#[test]
fn metadata_rewrites_and_runner_log_sidecars_are_silent() {
    use mac_worker::task::{HerdrTurnReport, HerdrTurnState};
    let (_dir, store, sink) = store_with_sink();
    let record = terminal_record(&task_record(), TaskOutcome::Done);
    let task = record.meta().task_id();
    let turn = record.status().turns()[0].turn_id();
    store.create_task(record.clone()).unwrap();
    sink.clear();
    let status = record.status();
    let rewritten = TaskStatus::new(
        status.state(),
        status.last_outcome().cloned(),
        status.worker().map(str::to_owned),
        status.session_present(),
        status.head_oid().cloned(),
        status.summary().map(str::to_owned),
        status.questions().to_vec(),
        status.files_changed().to_vec(),
        status.diff_stat().map(str::to_owned),
        vec![status.turns()[0].clone().with_herdr(Some(HerdrTurnReport {
            state: HerdrTurnState::Attached,
            pane_id: Some("PRIVATE_PANE".into()),
        }))],
        status.updated_at_millis(),
    )
    .unwrap();
    store
        .update_task_if_current(&record, record.with_status(rewritten).unwrap())
        .unwrap();
    store
        .finish_local_runner_log(task, turn, TaskOutcome::Done)
        .unwrap();
    assert!(sink.events().is_empty());
}

#[test]
fn legacy_job_running_and_health_bookkeeping_are_silent() {
    use mac_worker::job::{
        JobMeta, JobStatus, LeaseToken, LocalJobRecord, RemoteUncertainty,
        RequestFingerprintMaterial,
    };
    let (dir, store, sink) = store_with_sink();
    let token = LeaseToken::generate();
    let material = RequestFingerprintMaterial::new(
        TurnId::generate(),
        store.client_id(),
        token,
        100,
        "mini-1".into(),
        "a".repeat(64),
        "b".repeat(64),
        "c".repeat(64),
        "packages/app".into(),
        1000,
        "heavy".into(),
        mac_worker::job::CommandSpec::argv(vec!["PRIVATE_COMMAND".into()]).unwrap(),
    )
    .unwrap();
    let meta = JobMeta::new(&material, material.fingerprint()).unwrap();
    let record = LocalJobRecord::new(meta.clone(), token, None, RemoteUncertainty::None).unwrap();
    store.create_job(record).unwrap();
    let status = JobStatus::running(
        110,
        owner().pid(),
        owner().start_time_micros(),
        owner().pid(),
        owner().start_time_micros(),
    )
    .unwrap();
    store
        .update_job(
            LocalJobRecord::new(meta, token, Some(status), RemoteUncertainty::None).unwrap(),
        )
        .unwrap();
    let health = mac_worker::controller::health::HealthStore::open(
        &dir.path().canonicalize().unwrap().join("controller"),
    )
    .unwrap();
    health
        .write(&mac_worker::controller::health::ControllerHealth::new(
            owner(),
            100,
        ))
        .unwrap();
    assert!(sink.events().is_empty());
}

#[test]
fn unavailable_sink_does_not_change_authoritative_success() {
    struct Unavailable;
    impl EventSink for Unavailable {
        fn try_publish(&self, _: EventBatch) -> PublishAttempt {
            PublishAttempt::Dropped
        }
    }
    let (_dir, store, _sink) = store_with_sink();
    let store = store.with_event_sink(Arc::new(Unavailable));
    let record = task_record();
    store.create_task(record.clone()).unwrap();
    assert_eq!(store.load_task(record.meta().task_id()).unwrap(), record);
}

#[test]
fn task_tree_removal_invalidation_survives_cleanup_error() {
    let (_dir, store, sink) = store_with_sink();
    let record = task_record();
    let task = record.meta().task_id();
    store.create_task(record.clone()).unwrap();
    store
        .write_turn_prompt(task, record.status().turns()[0].turn_id(), "PRIVATE_PROMPT")
        .unwrap();
    sink.clear();
    store.inject_submission_rollback_cleanup_failure_once(
        ClientStateWritePoint::AfterTaskSubmissionTurnsRetirement,
    );
    assert!(store.remove_task_submission_turns(task).is_err());
    assert!(
        sink.events()
            .iter()
            .any(|event| matches!(event, NewEvent::TaskChanged(_)))
    );
}

#[test]
fn changed_last_outcome_is_a_task_invalidation() {
    let (_dir, store, sink) = store_with_sink();
    let record = terminal_record(&task_record(), TaskOutcome::Done);
    store.create_task(record.clone()).unwrap();
    sink.clear();
    let before = record.status();
    let next = TaskStatus::new(
        before.state(),
        Some(TaskOutcome::NeedsInput),
        before.worker().map(str::to_owned),
        before.session_present(),
        before.head_oid().cloned(),
        None,
        vec![],
        vec![],
        None,
        before.turns().to_vec(),
        130,
    )
    .unwrap();
    store
        .update_task_if_current(&record, record.with_status(next).unwrap())
        .unwrap();
    assert!(
        sink.events()
            .iter()
            .any(|event| matches!(event, NewEvent::TaskChanged(_)))
    );
}

// Exercise the frozen wire encoding, including its per-event newline budget.
fn serialize_hints(events: &[NewEvent]) -> String {
    events
        .iter()
        .enumerate()
        .map(|(index, event)| {
            let wire = event
                .to_wire(
                    uuid::Uuid::from_u128(1),
                    mac_worker::controller::events::Seq::new(index as u64 + 1),
                    120,
                )
                .unwrap();
            let encoded = serde_json::to_string(&wire).unwrap();
            assert!(
                encoded.len() + 1 <= mac_worker::controller::events::contracts::MAX_EVENT_BYTES
            );
            encoded
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn finalizer_changes_terminal_outcome() {
    use mac_worker::{
        config::Config,
        controller::registry::ProjectRegistry,
        error::WorkerError,
        job::{CommandSummary, ProcessIdentity, QueueEntry, QueueEntryKind},
        paths::PathLayout,
        process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
        scheduler::WorkerPreference,
        supervisor::{
            ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
        },
        task::RunnerIdentity,
        task_client::TaskClient,
        turn_runner::InlineRunnerExecutor,
    };
    struct LocalGitOnly;
    impl ProcessRunner for LocalGitOnly {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            assert_eq!(request.program, std::ffi::OsStr::new("/usr/bin/git"));
            SystemProcessRunner.run(request)
        }
    }
    struct ReusedOwner(ProcessIdentity);
    impl ProcessInspector for ReusedOwner {
        fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
            mac_worker::supervisor::SystemProcessInspector.identity_for_pid(pid)
        }
        fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
            if expected == self.0 {
                ProcessObservation::Reused
            } else {
                ProcessObservation::Matching {
                    process_group: expected.pid(),
                }
            }
        }
        fn observe_group(&self, _: u32) -> ProcessGroupObservation {
            ProcessGroupObservation::Ambiguous
        }
        fn observe_group_members(&self, _: u32) -> ProcessGroupMembership {
            ProcessGroupMembership::Ambiguous
        }
    }
    let (dir, plain, _) = store_with_sink();
    let root = dir.path().canonicalize().unwrap();
    let paths = PathLayout {
        state: root.join("state"),
        config: root.join("config.toml"),
        cache: root.join("cache"),
        data: root.join("data"),
    };
    let repo = super::support::GitRepo::init();
    repo.write("base.txt", b"fixture\n");
    repo.commit_all("base");
    let dead = ProcessIdentity::new(super::support::fixture_pid(424_244), 4_242_447).unwrap();
    let record = terminal_record(&task_record(), TaskOutcome::Done)
        .with_runner(Some(RunnerIdentity::new(dead)))
        .unwrap();
    let task = record.meta().task_id();
    let turn = record.status().turns()[0].turn_id();
    ProjectRegistry::open(&paths.controller_state_root())
        .unwrap()
        .register(
            record.meta().project_id(),
            record.meta().worktree_id(),
            repo.root(),
        )
        .unwrap();
    plain.create_task(record.clone()).unwrap();
    plain.write_task_project_path(&record, repo.root()).unwrap();
    plain
        .write_turn_prompt(task, turn, "PRIVATE_PROMPT")
        .unwrap();
    plain
        .enqueue(
            QueueEntry::new(
                turn,
                plain.client_id(),
                record.meta().project_id().into(),
                record.meta().worktree_id().into(),
                CommandSummary::argv(1).unwrap(),
                vec![],
                WorkerPreference::Pinned {
                    worker: "mini-1".into(),
                },
                QueueEntryKind::TaskTurn,
                None,
                dead,
                100,
            )
            .unwrap(),
        )
        .unwrap();
    plain
        .claim_next(dead, &["mini-1".into()], 110)
        .unwrap()
        .unwrap();
    drop(plain.open_runner_log(task, turn).unwrap());
    // Plant a crash checkpoint in this isolated root, as existing recovery fixtures do.
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    let checkpoint = paths
        .state
        .join(format!("runners/{task}/{turn}.checkpoint.json"));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(checkpoint)
        .unwrap();
    file.write_all(&serde_json::to_vec(&serde_json::json!({
        "version": 1, "task_id": task, "turn_id": turn,
        "committed": { "offsets": [0, 0], "len": 0, "accepted": true,
            "completion": { "outcome": { "kind": "failed", "reason": "LOG_DRAIN_UNAVAILABLE" }, "drained": false } },
        "pending": null
    })).unwrap()).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let locked = vec![
        paths.state.clone(),
        paths.state.join("jobs.lock"),
        paths.state.join("queue/lock"),
        paths.state.join(format!("runners/{task}/{turn}.log")),
    ];
    let sink = Arc::new(CheckingSink::checking(move || locks_are_free(&locked)));
    let store = ClientStateStore::open_with_owner_inspector(&paths.state, ReusedOwner(dead))
        .unwrap()
        .with_event_sink(sink.clone());
    let config = Config::parse("version = 1\n").unwrap();
    let report = TaskClient::new(
        &LocalGitOnly,
        &config,
        &paths,
        &store,
        &InlineRunnerExecutor,
    )
    .reconcile_selected(&[task])
    .unwrap();
    assert_eq!(report.repaired_rows(), 1);
    assert_eq!(
        store.load_task(task).unwrap().status().last_outcome(),
        Some(&TaskOutcome::failed("LOG_DRAIN_UNAVAILABLE"))
    );
    assert!(sink.events().iter().any(|event| matches!(event, NewEvent::TurnOutcomeChanged(hint) if hint.turn_id == turn && hint.outcome == SafeOutcome::Failed && hint.code.as_ref().map(|code| code.as_str()) == Some("LOG_DRAIN_UNAVAILABLE"))));
    assert!(
        !sink
            .events()
            .iter()
            .any(|event| matches!(event, NewEvent::TurnFinished(_)))
    );
    assert_eq!(sink.unsafe_releases.load(Ordering::Relaxed), 0);
}
