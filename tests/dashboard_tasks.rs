use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy, TurnLimits},
    client_state::{ClientStateConcurrencyHook, ClientStateConcurrencyPoint, ClientStateStore},
    config::{Config, WorkerEntry},
    dashboard::{
        model::{DashboardError, DashboardQueueEntryKind, DashboardSnapshot},
        service::{
            Clock, DashboardDataSource, DashboardService, MonotonicClock, WorkerObservationResult,
        },
        source::{DashboardRemoteReader, DashboardWorkerReader, MacWorkerDashboardSource},
        task::{DashboardTaskSource, MAX_TASK_LOG_LIMIT, MacWorkerTaskSource},
    },
    error::WorkerError,
    job::{
        AdmissionObservation, CommandSummary, JobId, LogChunk, LogStream, QueueEntry,
        QueueEntryKind, QueueRunReference, RunId as QueueRunId, StatusResponse,
    },
    lease::SlotState,
    project_config::ProjectSettings,
    protocol::{
        HealthStatus, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION,
        WorkerHealth as ProbeWorkerHealth, WorkersReport,
    },
    scheduler::{CandidateSlot, WorkerPreference},
    task::{
        ClosePolicy, DeliveryState, GitIdentity, LocalTaskRecord, OriginDelivery, RunId, RunRecord,
        RunnerIdentity, RunnerState, TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome,
        TaskSource, TaskState, TaskStatus, TurnId, TurnSummary,
    },
    task_store::{TaskStatusRequest, TaskStatusResponse},
    task_view::{ReviewState, TaskListJson},
};
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const REPO_ID: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const BASE_OID: &str = "0123456789abcdef0123456789abcdef01234567";
const REMOTE_SSH: &str = "operator@mini-1.internal";
const SECRET: &str = "prompt-secret-value";

#[test]
fn active_remote_task_status_overrides_local_status_without_a_write() {
    let harness = DashboardTaskHarness::active_local_task()
        .with_remote_status(TaskState::Open, Some(TaskOutcome::NeedsInput))
        .with_remote_runner(RunnerState::Live);
    let before = harness.local_state_fingerprint();
    let snapshot = harness.snapshot().unwrap();
    let row = snapshot
        .task_view
        .tasks
        .iter()
        .find(|row| row.task_id == harness.task_id())
        .unwrap();

    assert_eq!(row.state, TaskState::Open);
    assert_eq!(row.last_outcome, Some(TaskOutcome::NeedsInput));
    assert_eq!(row.freshness, mac_worker::task_view::TaskFreshness::Current);
    assert_eq!(before, harness.local_state_fingerprint());
    assert_eq!(harness.mutation_calls(), 0);
}

#[test]
fn close_intent_skips_remote_refresh_on_snapshot_and_detail() {
    let harness = DashboardTaskHarness::open_with_close_intent()
        .with_remote_status(TaskState::Closed, Some(TaskOutcome::Done));
    let before = harness.local_state_fingerprint();
    let snapshot = harness.snapshot().unwrap();
    let row = snapshot
        .task_view
        .tasks
        .iter()
        .find(|row| row.task_id == harness.task_id())
        .unwrap();

    assert_eq!(row.state, TaskState::Open);
    assert_eq!(row.review_state, ReviewState::ClosePending);
    assert_ne!(row.review_state, ReviewState::Accepted);
    assert_eq!(harness.task_status_calls(), 0);
    assert_eq!(before, harness.local_state_fingerprint());

    let detail = harness
        .task_source()
        .task_detail(harness.task_id())
        .unwrap();
    assert_eq!(detail.task.state, TaskState::Open);
    assert_eq!(detail.review_state, ReviewState::ClosePending);
    assert_ne!(detail.review_state, ReviewState::Accepted);
    assert_eq!(harness.task_status_calls(), 0);
}

#[test]
fn log_drain_unavailable_skips_remote_refresh_on_snapshot_and_detail() {
    let harness = DashboardTaskHarness::open_with_log_drain_unavailable()
        .with_remote_status(TaskState::Closed, Some(TaskOutcome::Done));
    let before = harness.local_state_fingerprint();
    let snapshot = harness.snapshot().unwrap();
    let row = snapshot
        .task_view
        .tasks
        .iter()
        .find(|row| row.task_id == harness.task_id())
        .unwrap();

    assert_eq!(row.state, TaskState::Open);
    assert_eq!(harness.task_status_calls(), 0);
    assert_eq!(before, harness.local_state_fingerprint());

    let detail = harness
        .task_source()
        .task_detail(harness.task_id())
        .unwrap();
    assert_eq!(detail.task.state, TaskState::Open);
    assert_eq!(harness.task_status_calls(), 0);
}

#[test]
fn remote_status_failure_keeps_a_stale_row_and_dead_runner() {
    let harness = DashboardTaskHarness::active_local_task()
        .with_runner_liveness(Some(RunnerState::Dead))
        .with_remote_failure("SSH_UNAVAILABLE");
    let runner = mac_worker::job::ProcessIdentity::new(2_000_000_000, 1).unwrap();
    assert_eq!(
        harness.state.runner_liveness(harness.task_id()).unwrap(),
        Some(RunnerState::Live),
        "a single unconfirmed Absent is not death"
    );
    harness.state.note_confirmed_runner_absence(runner);
    let snapshot = harness.snapshot().unwrap();
    let row = snapshot
        .task_view
        .tasks
        .iter()
        .find(|row| row.task_id == harness.task_id())
        .unwrap();

    assert_eq!(row.freshness, mac_worker::task_view::TaskFreshness::Stale);
    assert_eq!(row.runner, Some(RunnerState::Dead));
    assert!(
        snapshot
            .collection
            .errors
            .iter()
            .any(|error| error.code == "TASK_STATUS_STALE")
    );
    assert_eq!(harness.mutation_calls(), 0);
}

#[test]
fn queue_projection_keeps_kind_pin_run_cap_and_phase_four_reason() {
    let snapshot = QueueHarness::new().snapshot().unwrap();

    assert_eq!(
        snapshot.queue[0].entry_kind,
        DashboardQueueEntryKind::TaskTurn
    );
    assert!(snapshot.queue[0].task_id.is_some());
    assert_eq!(snapshot.queue[0].run_max_parallel, Some(2));
    assert_eq!(snapshot.queue[0].pinned_worker.as_deref(), Some("mini-2"));
    assert_eq!(snapshot.queue[0].blocking_code, "RUN_MAX_PARALLEL");
    assert_eq!(snapshot.queue[1].entry_kind, DashboardQueueEntryKind::Batch);
    assert!(snapshot.queue[1].task_id.is_none());

    assert_safe_snapshot(&snapshot);
}

#[test]
fn active_worker_card_maps_turn_identity_to_task_title_and_agent() {
    let snapshot = DashboardTaskHarness::active_local_task()
        .snapshot()
        .unwrap();
    let worker = snapshot
        .workers
        .iter()
        .find(|worker| worker.name == "mini-1")
        .unwrap();
    let active = worker.active_task.as_ref().unwrap();

    assert_eq!(active.task_id, snapshot.task_view.tasks[0].task_id);
    assert_eq!(active.title, "Repair login");
    assert_eq!(active.agent, "codex");
    assert_eq!(active.model.as_deref(), Some("gpt-5"));
    assert_eq!(active.effort, None);
    assert_eq!(active.turn_number, 1);
}

#[test]
fn task_projection_exposes_only_bounded_recorded_execution_settings() {
    let snapshot = DashboardTaskHarness::active_local_task()
        .snapshot()
        .unwrap();
    let row = &snapshot.task_view.tasks[0];

    assert_eq!(row.model.as_deref(), Some("gpt-5"));
    assert_eq!(row.effort, None);
    assert_eq!(row.permissions.as_deref(), Some("workspace"));
    assert_eq!(row.env_profile.as_deref(), Some("team-ci"));

    let encoded = serde_json::to_string(&snapshot).unwrap();
    assert!(!encoded.contains("ada@example.test"));
    assert!(!encoded.contains(SECRET));
}

#[test]
fn dashboard_snapshot_and_cli_task_list_keep_the_same_projection_fields() {
    let snapshot = DashboardTaskHarness::active_local_task()
        .snapshot()
        .unwrap();
    let cli = serde_json::to_value(TaskListJson::new(
        PROTOCOL_VERSION,
        snapshot.task_view.clone(),
    ))
    .unwrap();
    let snapshot_json = serde_json::to_value(snapshot).unwrap();

    assert_eq!(cli["tasks"], snapshot_json["tasks"]);
    assert_eq!(cli["runs"], snapshot_json["runs"]);
    assert_eq!(cli["progress"], snapshot_json["progress"]);
    assert_eq!(cli["protocol_version"], PROTOCOL_VERSION);
}

#[test]
fn missing_project_file_projects_the_real_launch_defaults() {
    let harness = DashboardTaskHarness::active_local_task();
    let launch_directory = tempfile::tempdir().unwrap();
    let mut settings = ProjectSettings::load(launch_directory.path(), &[]).unwrap();
    settings
        .task
        .permissions
        .insert("/Users/alice/private-agent".into(), "workspace".into());
    let source = MacWorkerDashboardSource::new(
        Arc::clone(&harness.config),
        Arc::clone(&harness.workers) as Arc<dyn DashboardWorkerReader>,
        Arc::clone(&harness.state),
        Arc::clone(&harness.remote) as Arc<dyn DashboardRemoteReader>,
    )
    .with_project_settings(settings);

    let snapshot = DashboardService::new(source, FixedClock, FixedClock)
        .snapshot(Default::default())
        .unwrap();
    let defaults = snapshot.project_defaults.unwrap();

    assert_eq!(defaults.default_agent, "codex");
    assert_eq!(defaults.timeout_seconds, 45 * 60);
    assert_eq!(defaults.max_followups, 10);
    assert_eq!(defaults.source, "local");
    assert_eq!(defaults.publish, vec!["fetch"]);
    assert_eq!(defaults.env_profile, None);
    assert_eq!(
        defaults.permissions.get("codex").map(String::as_str),
        Some("workspace")
    );
    assert_eq!(
        defaults.permissions.get("claude").map(String::as_str),
        Some("unattended")
    );
    assert!(
        !serde_json::to_string(&defaults)
            .unwrap()
            .contains("/Users/alice")
    );
}

#[test]
fn task_source_uses_typed_ids_and_stale_detail_without_mutation() {
    let harness = DashboardTaskHarness::active_local_task().with_remote_failure("SSH_UNAVAILABLE");
    let source = MacWorkerTaskSource::new(
        Arc::clone(&harness.config),
        Arc::clone(&harness.state),
        Arc::clone(&harness.remote) as Arc<dyn DashboardRemoteReader>,
    );
    let detail = source.task_detail(harness.task_id()).unwrap();
    assert_eq!(
        detail.task.freshness,
        mac_worker::task_view::TaskFreshness::Stale
    );

    let turn_id = harness
        .state
        .load_task(harness.task_id())
        .unwrap()
        .status()
        .turns()[0]
        .turn_id();
    harness
        .state
        .write_turn_prompt(harness.task_id(), turn_id, SECRET)
        .unwrap();
    let chunk = source
        .read_task_log(harness.task_id(), turn_id, LogStream::Stdout, 3, 4)
        .unwrap();
    assert_eq!(chunk.offset(), 3);
    assert_eq!(chunk.next_offset(), 7);
    assert_eq!(
        harness.remote.log_requests(),
        vec![(turn_id, LogStream::Stdout, 3, 4)]
    );
    assert_eq!(harness.mutation_calls(), 0);
}

#[test]
fn task_detail_and_log_map_a_missing_record_to_task_not_found() {
    let harness = DashboardTaskHarness::active_local_task();
    let source = harness.task_source();
    let missing = task_id(99);

    let detail = source.task_detail(missing).unwrap_err();
    assert_eq!(detail.code, "TASK_NOT_FOUND");
    assert_eq!(detail.message, "task is not present in local state");

    let log = source
        .read_task_log(missing, turn_id(1), LogStream::Stdout, 0, 4)
        .unwrap_err();
    assert_eq!(log.code, "TASK_NOT_FOUND");
    assert_eq!(log.message, "task is not present in local state");
}

#[test]
fn addressed_task_reads_ignore_an_unrelated_corrupt_sibling() {
    let harness = DashboardTaskHarness::active_local_task();
    for number in 2..=17 {
        harness.create_extra_task(number, Some("mini-1"));
    }
    let sibling = harness.create_extra_task(18, Some("mini-1"));
    std::fs::write(harness.task_path(sibling), b"{not-a-task-record").unwrap();

    assert!(harness.state.list_tasks().is_err());
    let snapshot = harness.snapshot().unwrap();
    assert!(
        snapshot
            .collection
            .errors
            .iter()
            .any(|error| error.code == "IO"),
        "list/collection still scans every task record"
    );
    assert!(snapshot.task_view.tasks.is_empty());

    let source = harness.task_source();
    let detail = source.task_detail(harness.task_id()).unwrap();
    assert_eq!(detail.task.task_id, harness.task_id());
    assert_eq!(detail.task.title, "Repair login");

    let turn_id = harness
        .state
        .load_task(harness.task_id())
        .unwrap()
        .status()
        .turns()[0]
        .turn_id();
    harness
        .state
        .write_turn_prompt(harness.task_id(), turn_id, SECRET)
        .unwrap();
    let chunk = source
        .read_task_log(harness.task_id(), turn_id, LogStream::Stdout, 3, 4)
        .unwrap();
    assert_eq!(chunk.offset(), 3);
    assert_eq!(chunk.next_offset(), 7);
    assert_eq!(
        harness.remote.log_requests(),
        vec![(turn_id, LogStream::Stdout, 3, 4)]
    );
}

#[test]
fn task_detail_fails_when_the_target_record_is_corrupt() {
    let harness = DashboardTaskHarness::active_local_task();
    std::fs::write(harness.task_path(harness.task_id()), b"{broken").unwrap();

    let error = harness
        .task_source()
        .task_detail(harness.task_id())
        .unwrap_err();
    assert_eq!(error.code, "IO");
    assert_eq!(error.message, "local dashboard task state is unavailable");
    assert_ne!(error.code, "TASK_NOT_FOUND");
}

#[test]
fn task_detail_fails_when_the_target_filename_and_embedded_id_differ() {
    let harness = DashboardTaskHarness::active_local_task();
    let sibling = harness.create_extra_task(2, Some("mini-1"));
    let foreign = std::fs::read(harness.task_path(sibling)).unwrap();
    std::fs::write(harness.task_path(harness.task_id()), foreign).unwrap();

    let error = harness
        .task_source()
        .task_detail(harness.task_id())
        .unwrap_err();
    assert_eq!(error.code, "IO");
    assert_eq!(error.message, "local dashboard task state is unavailable");
}

#[test]
fn read_task_log_rejects_a_turn_owned_by_a_different_task() {
    let harness = DashboardTaskHarness::active_local_task();
    let other = harness.create_extra_task(2, Some("mini-1"));
    let foreign_turn = turn_id(2);
    harness
        .state
        .write_turn_prompt(other, foreign_turn, SECRET)
        .unwrap();
    harness
        .state
        .write_turn_prompt(harness.task_id(), turn_id(1), SECRET)
        .unwrap();

    let error = harness
        .task_source()
        .read_task_log(harness.task_id(), foreign_turn, LogStream::Stdout, 0, 4)
        .unwrap_err();
    assert_eq!(error.code, "TURN_NOT_FOUND");
    assert_eq!(error.message, "turn is not present in the requested task");
}

#[test]
fn read_task_log_rejects_out_of_range_limits() {
    let harness = DashboardTaskHarness::active_local_task();
    let source = harness.task_source();
    let turn = turn_id(1);

    let zero = source
        .read_task_log(harness.task_id(), turn, LogStream::Stdout, 0, 0)
        .unwrap_err();
    assert_eq!(zero.code, "INVALID_LOG_LIMIT");
    assert_eq!(zero.message, "log limit must be between 1 and 65536 bytes");

    let oversized = source
        .read_task_log(
            harness.task_id(),
            turn,
            LogStream::Stdout,
            0,
            MAX_TASK_LOG_LIMIT + 1,
        )
        .unwrap_err();
    assert_eq!(oversized.code, "INVALID_LOG_LIMIT");
}

#[test]
fn closed_task_detail_uses_local_status_without_a_remote_call() {
    let harness = DashboardTaskHarness::closed_local_task();
    let detail = harness
        .task_source()
        .task_detail(harness.task_id())
        .unwrap();

    assert_eq!(detail.task.state, TaskState::Closed);
    assert_eq!(
        detail.task.freshness,
        mac_worker::task_view::TaskFreshness::Current
    );
    assert_eq!(harness.remote.task_status_calls(), 0);
    assert_eq!(harness.mutation_calls(), 0);
}

#[test]
fn closed_task_with_pending_delivery_shows_remote_delivered_without_reopening() {
    let harness = DashboardTaskHarness::closed_local_task();
    let record = harness.state.load_task(harness.task_id()).unwrap();
    let head = record.status().head_oid().cloned().expect("fixture head");
    let turn = record.status().turns()[0].turn_id();
    let pending = OriginDelivery::new(
        turn,
        DeliveryState::Pending,
        head.clone(),
        "https://example.test/repo.git".into(),
        "refs/heads/release-candidate".into(),
        1,
        1,
        None,
        None,
        1,
        1,
    )
    .unwrap();
    harness
        .state
        .update_task(record.with_delivery(Some(pending.clone())).unwrap())
        .unwrap();
    let delivered = OriginDelivery::new(
        turn,
        DeliveryState::Delivered,
        head.clone(),
        pending.origin().to_owned(),
        pending.target().to_owned(),
        1,
        2,
        None,
        None,
        1,
        3,
    )
    .unwrap();
    let regressing = TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Done),
        Some("mini-1".into()),
        true,
        Some(head.clone()),
        None,
        Vec::new(),
        Vec::new(),
        None,
        harness
            .state
            .load_task(harness.task_id())
            .unwrap()
            .status()
            .turns()
            .to_vec(),
        2_000,
    )
    .unwrap();
    harness.remote.set_task_status(Ok(
        TaskStatusResponse::new(regressing).with_deliveries(vec![delivered.clone()])
    ));

    let before = harness.local_state_fingerprint();
    let detail = harness
        .task_source()
        .task_detail(harness.task_id())
        .unwrap();
    assert_eq!(detail.task.state, TaskState::Closed);
    assert_eq!(detail.head_oid.as_ref(), Some(&head));
    assert_eq!(
        detail.delivery.as_ref().map(OriginDelivery::state),
        Some(DeliveryState::Delivered)
    );
    assert_eq!(detail.deliveries[0].state(), DeliveryState::Delivered);
    assert!(harness.remote.task_status_calls() >= 1);
    assert_eq!(
        before,
        harness.local_state_fingerprint(),
        "dashboard reads must not mutate local task state"
    );
    assert_eq!(harness.mutation_calls(), 0);
    let persisted = harness.state.load_task(harness.task_id()).unwrap();
    assert_eq!(persisted.status().state(), TaskState::Closed);
    assert_eq!(persisted.status().head_oid(), Some(&head));
    assert_eq!(
        persisted.delivery().map(OriginDelivery::state),
        Some(DeliveryState::Pending)
    );
}

#[test]
fn read_task_log_rejects_an_unknown_configured_worker() {
    let harness = DashboardTaskHarness::active_local_task();
    let ghost = harness.create_extra_task(2, Some("ghost"));
    let turn = turn_id(2);
    harness
        .state
        .write_turn_prompt(ghost, turn, SECRET)
        .unwrap();

    let error = harness
        .task_source()
        .read_task_log(ghost, turn, LogStream::Stdout, 0, 4)
        .unwrap_err();
    assert_eq!(error.code, "TASK_SOURCE_FAILED");
    assert_eq!(
        error.message,
        "task references an unknown configured worker"
    );
}

#[test]
fn missing_tasks_directory_is_not_mapped_to_task_not_found() {
    let harness = DashboardTaskHarness::active_local_task();
    std::fs::remove_dir_all(harness.tasks_dir()).unwrap();

    let error = harness
        .task_source()
        .task_detail(harness.task_id())
        .unwrap_err();
    assert_eq!(error.code, "IO");
    assert_eq!(error.message, "local dashboard task state is unavailable");
}

#[test]
fn task_detail_waits_for_a_pre_exchange_replacement_writer() {
    let (writer_entered_tx, writer_entered_rx) = mpsc::channel();
    let (writer_release_tx, writer_release_rx) = mpsc::channel();
    let harness =
        DashboardTaskHarness::active_local_task_with_hook(Arc::new(PreExchangeReplacementGate {
            entered: writer_entered_tx,
            release: Mutex::new(writer_release_rx),
            used: AtomicBool::new(false),
        }));
    let original = harness.state.load_task(harness.task_id()).unwrap();
    let replacement = original.with_runner(None).unwrap();
    let expected = replacement.clone();

    thread::scope(|scope| {
        let writer_state = Arc::clone(&harness.state);
        let writer = scope.spawn(move || writer_state.update_task(replacement));
        writer_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();

        let source = harness.task_source();
        let task_id = harness.task_id();
        let contention = harness.state.observe_next_lock_contention();
        let (detail_tx, detail_rx) = mpsc::channel();
        scope.spawn(move || {
            detail_tx.send(source.task_detail(task_id)).unwrap();
        });

        let reader_contended = contention.confirmed_within(Duration::from_secs(2));
        writer_release_tx.send(()).unwrap();
        writer.join().unwrap().unwrap();
        assert!(
            reader_contended,
            "task detail must wait for the pre-exchange writer"
        );
        let detail = detail_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        assert_eq!(detail.task.task_id, task_id);
        assert_eq!(harness.state.load_task(task_id).unwrap(), expected);
    });
}

struct PreExchangeReplacementGate {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    used: AtomicBool,
}

impl ClientStateConcurrencyHook for PreExchangeReplacementGate {
    fn reach(&self, point: ClientStateConcurrencyPoint) {
        if point == ClientStateConcurrencyPoint::TaskReplacementPreExchange
            && !self.used.swap(true, Ordering::SeqCst)
        {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
        }
    }
}

#[derive(Clone)]
struct DashboardTaskHarness {
    _temp: Arc<tempfile::TempDir>,
    state: Arc<ClientStateStore>,
    config: Arc<Config>,
    workers: Arc<FakeWorkers>,
    remote: Arc<FakeRemote>,
    task_id: TaskId,
}

impl DashboardTaskHarness {
    fn active_local_task() -> Self {
        Self::local_task(
            TaskState::Active,
            Some("mini-1"),
            Some(turn_id(1)),
            true,
            None,
        )
    }

    fn closed_local_task() -> Self {
        Self::local_task(
            TaskState::Closed,
            Some("mini-1"),
            Some(turn_id(1)),
            false,
            None,
        )
    }

    fn open_with_close_intent() -> Self {
        let harness = Self::local_task(
            TaskState::Open,
            Some("mini-1"),
            Some(turn_id(1)),
            false,
            None,
        );
        let record = harness.state.load_task(harness.task_id).unwrap();
        let intent = mac_worker::task::TaskCloseIntent::from_record(&record, false).unwrap();
        harness
            .state
            .update_task(record.with_close_intent(intent).unwrap())
            .unwrap();
        harness
    }

    fn open_with_log_drain_unavailable() -> Self {
        let harness = Self::local_task(
            TaskState::Open,
            Some("mini-1"),
            Some(turn_id(1)),
            false,
            None,
        );
        let record = harness.state.load_task(harness.task_id).unwrap();
        harness
            .state
            .update_task(
                record
                    .with_abandon_code(Some("LOG_DRAIN_UNAVAILABLE".into()))
                    .unwrap(),
            )
            .unwrap();
        harness
    }

    fn active_local_task_with_hook(hook: Arc<dyn ClientStateConcurrencyHook>) -> Self {
        Self::local_task(
            TaskState::Active,
            Some("mini-1"),
            Some(turn_id(1)),
            true,
            Some(hook),
        )
    }

    fn local_task(
        state: TaskState,
        worker: Option<&str>,
        turn: Option<TurnId>,
        with_runner: bool,
        hook: Option<Arc<dyn ClientStateConcurrencyHook>>,
    ) -> Self {
        let temp = Arc::new(tempfile::tempdir().unwrap());
        let state_root = temp.path().canonicalize().unwrap().join("state");
        let store = Arc::new(match hook {
            Some(hook) => ClientStateStore::open_with_concurrency_hook(&state_root, hook).unwrap(),
            None => ClientStateStore::open(&state_root).unwrap(),
        });
        let task_id = task_id(1);
        let turn_id = turn.unwrap_or_else(|| turn_id(1));
        let run_id = run_id();
        store
            .create_task(task_record(
                task_id,
                run_id,
                state,
                worker,
                turn,
                with_runner,
            ))
            .unwrap();
        store
            .create_run(
                RunRecord::new(
                    run_id,
                    Some("dashboard run".into()),
                    vec![task_id],
                    2,
                    1_000,
                )
                .unwrap(),
            )
            .unwrap();

        let config = Arc::new(config_with_workers(&["mini-1"]));
        let workers = Arc::new(FakeWorkers::with_active_turn(turn_id));
        let remote = Arc::new(FakeRemote::default());
        Self {
            _temp: temp,
            state: store,
            config,
            workers,
            remote,
            task_id,
        }
    }

    fn with_remote_status(self, state: TaskState, outcome: Option<TaskOutcome>) -> Self {
        let record = self.state.load_task(self.task_id).unwrap();
        let status = TaskStatus::new(
            state,
            outcome,
            Some("mini-1".into()),
            true,
            Some(BASE_OID.parse().unwrap()),
            Some("remote status summary".into()),
            vec!["remote question".into()],
            vec!["src/login.rs".into()],
            Some("1 file changed".into()),
            record.status().turns().to_vec(),
            2_000,
        )
        .unwrap();
        self.remote
            .set_task_status(Ok(TaskStatusResponse::new(status)));
        self
    }

    fn with_remote_failure(self, code: &str) -> Self {
        self.remote.set_task_status(Err(code.into()));
        self
    }

    fn with_runner_liveness(self, _runner: Option<RunnerState>) -> Self {
        self
    }

    fn with_remote_runner(self, _runner: RunnerState) -> Self {
        self
    }

    fn task_id(&self) -> TaskId {
        self.task_id
    }

    fn task_source(&self) -> MacWorkerTaskSource {
        MacWorkerTaskSource::new(
            Arc::clone(&self.config),
            Arc::clone(&self.state),
            Arc::clone(&self.remote) as Arc<dyn DashboardRemoteReader>,
        )
    }

    fn task_path(&self, task_id: TaskId) -> std::path::PathBuf {
        self.tasks_dir().join(format!("{task_id}.json"))
    }

    fn tasks_dir(&self) -> std::path::PathBuf {
        self._temp
            .path()
            .canonicalize()
            .unwrap()
            .join("state")
            .join("tasks")
    }

    fn create_extra_task(&self, number: u128, worker: Option<&str>) -> TaskId {
        let id = task_id(number);
        self.state
            .create_task(task_record(
                id,
                run_id(),
                TaskState::Closed,
                worker,
                None,
                false,
            ))
            .unwrap();
        id
    }

    fn local_state_fingerprint(&self) -> Vec<u8> {
        self.state
            .load_task(self.task_id)
            .unwrap()
            .canonical_bytes()
            .unwrap()
    }

    fn mutation_calls(&self) -> usize {
        self.remote.mutation_calls()
    }

    fn task_status_calls(&self) -> usize {
        self.remote.task_status_calls()
    }

    fn snapshot(&self) -> Result<DashboardSnapshot, DashboardError> {
        let source = MacWorkerDashboardSource::new(
            Arc::clone(&self.config),
            Arc::clone(&self.workers) as Arc<dyn DashboardWorkerReader>,
            Arc::clone(&self.state),
            Arc::clone(&self.remote) as Arc<dyn DashboardRemoteReader>,
        );
        DashboardService::new(source, FixedClock, FixedClock).snapshot(Default::default())
    }
}

struct QueueHarness {
    _temp: Arc<tempfile::TempDir>,
    state: Arc<ClientStateStore>,
    config: Arc<Config>,
    workers: Arc<FakeWorkers>,
    remote: Arc<FakeRemote>,
}

impl QueueHarness {
    fn new() -> Self {
        let temp = Arc::new(tempfile::tempdir().unwrap());
        let state_root = temp.path().canonicalize().unwrap().join("state");
        let state = Arc::new(ClientStateStore::open(&state_root).unwrap());
        let run_id = run_id();
        let active_one = task_id(10);
        let active_two = task_id(11);
        let queued = task_id(12);
        let task_turn_id = turn_id(12);

        for (task, status, turn) in [
            (active_one, TaskState::Active, Some(turn_id(10))),
            (active_two, TaskState::Active, Some(turn_id(11))),
            (queued, TaskState::Queued, None),
        ] {
            state
                .create_task(task_record(
                    task,
                    run_id,
                    status,
                    (status == TaskState::Active).then_some("mini-1"),
                    turn,
                    false,
                ))
                .unwrap();
        }
        state
            .create_run(
                RunRecord::new(
                    run_id,
                    Some("capacity run".into()),
                    vec![active_one, active_two, queued],
                    2,
                    1_000,
                )
                .unwrap(),
            )
            .unwrap();
        state
            .write_turn_prompt(queued, task_turn_id, SECRET)
            .unwrap();

        let owner = mac_worker::job::ProcessIdentity::new(42, 1_000).unwrap();
        state
            .enqueue(
                QueueEntry::new(
                    task_turn_id,
                    state.client_id(),
                    PROJECT_ID.into(),
                    WORKTREE_ID.into(),
                    CommandSummary::argv(1).unwrap(),
                    vec!["swift".into()],
                    WorkerPreference::Pinned {
                        worker: "mini-2".into(),
                    },
                    QueueEntryKind::TaskTurn,
                    Some(
                        QueueRunReference::new(QueueRunId::new(run_id.to_string()).unwrap(), 2)
                            .unwrap(),
                    ),
                    owner,
                    1_001,
                )
                .unwrap(),
            )
            .unwrap();
        state
            .enqueue(
                QueueEntry::new(
                    job_id(900),
                    state.client_id(),
                    PROJECT_ID.into(),
                    WORKTREE_ID.into(),
                    CommandSummary::shell(),
                    Vec::new(),
                    WorkerPreference::Automatic,
                    QueueEntryKind::Batch,
                    None,
                    owner,
                    1_002,
                )
                .unwrap(),
            )
            .unwrap();

        state
            .admission_observation("mini-2", 1_003, || {
                AdmissionObservation::new(
                    "mini-2".into(),
                    true,
                    CandidateSlot::Idle,
                    vec!["swift".into()],
                    Some(1024),
                    2048,
                    1_003,
                )
            })
            .unwrap();

        Self {
            _temp: temp,
            state,
            config: Arc::new(config_with_workers(&["mini-1", "mini-2"])),
            workers: Arc::new(FakeWorkers::idle_workers()),
            remote: Arc::new(FakeRemote::default()),
        }
    }

    fn snapshot(&self) -> Result<DashboardSnapshot, DashboardError> {
        let source = MacWorkerDashboardSource::new(
            Arc::clone(&self.config),
            Arc::clone(&self.workers) as Arc<dyn DashboardWorkerReader>,
            Arc::clone(&self.state),
            Arc::clone(&self.remote) as Arc<dyn DashboardRemoteReader>,
        );
        DashboardService::new(source, FixedClock, FixedClock).snapshot(Default::default())
    }
}

#[derive(Default)]
struct FakeRemote {
    task_status: Mutex<Option<Result<TaskStatusResponse, String>>>,
    task_status_calls: AtomicUsize,
    log_requests: Mutex<Vec<(JobId, LogStream, u64, u32)>>,
    mutation_calls: AtomicUsize,
}

impl FakeRemote {
    fn set_task_status(&self, response: Result<TaskStatusResponse, String>) {
        *self.task_status.lock().unwrap() = Some(response);
    }

    fn mutation_calls(&self) -> usize {
        self.mutation_calls.load(Ordering::SeqCst)
    }

    fn task_status_calls(&self) -> usize {
        self.task_status_calls.load(Ordering::SeqCst)
    }

    fn task_status_response(&self) -> Result<TaskStatusResponse, WorkerError> {
        self.task_status_calls.fetch_add(1, Ordering::SeqCst);
        match self.task_status.lock().unwrap().clone() {
            Some(Ok(response)) => Ok(response),
            Some(Err(code)) => Err(WorkerError::Protocol(code)),
            None => Err(WorkerError::Protocol("TASK_NOT_FOUND".into())),
        }
    }

    fn log_requests(&self) -> Vec<(JobId, LogStream, u64, u32)> {
        self.log_requests.lock().unwrap().clone()
    }
}

impl DashboardRemoteReader for FakeRemote {
    fn status(&self, _worker: &WorkerEntry, _job_id: JobId) -> Result<StatusResponse, WorkerError> {
        Err(WorkerError::Protocol("JOB_NOT_FOUND".into()))
    }

    fn log_chunk(
        &self,
        _worker: &WorkerEntry,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    ) -> Result<LogChunk, WorkerError> {
        self.log_requests
            .lock()
            .unwrap()
            .push((job_id, stream, offset, limit));
        LogChunk::new(stream, offset, b"data".to_vec())
    }

    fn task_status(
        &self,
        _worker: &WorkerEntry,
        _request: &TaskStatusRequest,
    ) -> Result<TaskStatusResponse, WorkerError> {
        self.task_status_response()
    }

    fn task_status_with_deadline(
        &self,
        worker: &WorkerEntry,
        request: &TaskStatusRequest,
        _deadline: Duration,
    ) -> Result<TaskStatusResponse, WorkerError> {
        self.task_status(worker, request)
    }
}

struct FakeWorkers {
    report: WorkersReport,
}

impl FakeWorkers {
    fn with_active_turn(turn_id: TurnId) -> Self {
        Self {
            report: WorkersReport {
                protocol_version: PROTOCOL_VERSION,
                workers: vec![ProbeWorkerHealth {
                    name: "mini-1".into(),
                    ssh: REMOTE_SSH.into(),
                    status: HealthStatus::Ready,
                    probe: Some(probe(SlotState::Busy, Some(turn_id))),
                    missing_capabilities: Vec::new(),
                    error_code: None,
                    error_message: None,
                }],
            },
        }
    }

    fn idle_workers() -> Self {
        Self {
            report: WorkersReport {
                protocol_version: PROTOCOL_VERSION,
                workers: ["mini-1", "mini-2"]
                    .into_iter()
                    .map(|name| ProbeWorkerHealth {
                        name: name.into(),
                        ssh: format!("operator@{name}.internal"),
                        status: HealthStatus::Ready,
                        probe: Some(probe(SlotState::Idle, None)),
                        missing_capabilities: Vec::new(),
                        error_code: None,
                        error_message: None,
                    })
                    .collect(),
            },
        }
    }
}

impl DashboardWorkerReader for FakeWorkers {
    fn inspect(&self, _config: &Config, _deadline: Duration) -> WorkersReport {
        self.report.clone()
    }
}

fn config_with_workers(names: &[&str]) -> Config {
    let config = Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        controller: Default::default(),
        workers: names
            .iter()
            .map(|name| WorkerEntry {
                name: (*name).into(),
                ssh: format!("operator@{name}.internal"),
                slots: 1,
                capabilities: vec!["swift".into()],
                remote_binary: "~/.local/bin/worker".into(),
                herdr: false,
            })
            .collect(),
    };
    config.validate().unwrap();
    config
}

fn probe(slot_state: SlotState, active_turn: Option<TurnId>) -> ProbeResponse {
    ProbeResponse {
        protocol_version: PROTOCOL_VERSION,
        supervision_version: SUPERVISION_VERSION,
        hostname: "mini.local".into(),
        arch: "arm64".into(),
        os_version: "26.2".into(),
        free_disk_bytes: 100,
        total_disk_bytes: 200,
        memory_pressure: MemoryPressure::Normal,
        swap_used_bytes: None,
        available_memory_bytes: Some(50),
        cpu_counters: None,
        slot_state,
        active_lease: active_turn.map(|turn_id| mac_worker::lease::LeaseSummary {
            job_id: turn_id,
            project_id: PROJECT_ID.into(),
            worktree_id: WORKTREE_ID.into(),
            created_at_millis: 1_000,
        }),
        capabilities: vec!["swift".into()],
        agent_facts: None,
        facts_age_millis: None,
        configured_slots: 0,
        busy_slots: 0,
    }
}

fn task_record(
    task_id: TaskId,
    run_id: RunId,
    state: TaskState,
    worker: Option<&str>,
    turn_id: Option<TurnId>,
    with_runner: bool,
) -> LocalTaskRecord {
    let turns = turn_id
        .map(|turn_id| {
            vec![TurnSummary::new(
                1,
                turn_id,
                None,
                None,
                None,
                false,
                Some(1_500),
                None,
            )]
        })
        .unwrap_or_default();
    let status = TaskStatus::new(
        state,
        None,
        worker.map(str::to_owned),
        worker.is_some(),
        Some(BASE_OID.parse().unwrap()),
        Some(format!("summary {SECRET}")),
        vec![format!("question {SECRET}").into()],
        vec!["src/login.rs".into(), "/Users/alice/private.rs".into()],
        Some(format!("diff {SECRET}")),
        turns,
        1_600,
    )
    .unwrap();
    LocalTaskRecord::new(
        TaskMeta::new(TaskMetaInput {
            task_id,
            run_id: Some(run_id),
            project_id: PROJECT_ID.into(),
            worktree_id: WORKTREE_ID.into(),
            agent: AgentKind::Codex,
            model: Some("gpt-5".into()),
            effort: None,
            policy: PermissionPolicy::Workspace,
            source: TaskSource::Local {
                wip: false,
                push_target: None,
            },
            publish: vec![mac_worker::task::PublishMode::Fetch],
            publish_branch: None,
            base_oid: BASE_OID.parse().unwrap(),
            limits: TaskLimits::new(TurnLimits::new(60_000, None, None).unwrap(), 2).unwrap(),
            close_policy: ClosePolicy::Done,
            env_profile: Some("team-ci".into()),
            git_identity: GitIdentity::new("Ada", "ada@example.test").unwrap(),
            title: Some("Repair login".into()),
            prompt: format!("private prompt {SECRET}"),
            created_at_millis: 1_000,
        })
        .unwrap(),
        status,
        None,
        with_runner.then(|| {
            RunnerIdentity::new(mac_worker::job::ProcessIdentity::new(2_000_000_000, 1).unwrap())
        }),
        None,
        REPO_ID.into(),
        worker.map(str::to_owned),
        false,
        None,
    )
    .unwrap()
}

fn task_id(value: u128) -> TaskId {
    TaskId::new(Uuid::from_u128(value))
}

fn turn_id(value: u128) -> TurnId {
    JobId::new(Uuid::from_u128(100 + value))
}

fn job_id(value: u128) -> JobId {
    JobId::new(Uuid::from_u128(value))
}

fn run_id() -> RunId {
    RunId::new(Uuid::from_u128(500))
}

fn assert_safe_snapshot(snapshot: &DashboardSnapshot) {
    let value = serde_json::to_value(snapshot).unwrap();
    let encoded = serde_json::to_string(&value).unwrap();
    for forbidden in [
        SECRET,
        "prompt",
        "session_ref",
        "operator@",
        "/Users/",
        "~/.local/bin/worker",
        "raw command",
    ] {
        assert!(!encoded.contains(forbidden), "leaked {forbidden}");
    }
    assert_no_private_keys(&value);
    assert!(encoded.chars().all(|character| !character.is_control()));
}

fn assert_no_private_keys(value: &serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map {
                assert!(
                    ![
                        "prompt",
                        "env",
                        "environment",
                        "session_ref",
                        "ssh",
                        "command",
                        "argv",
                        "shell",
                        "path",
                    ]
                    .contains(&key.as_str()),
                    "leaked key {key}"
                );
                assert_no_private_keys(child);
            }
        }
        serde_json::Value::Array(values) => values.iter().for_each(assert_no_private_keys),
        _ => {}
    }
}

#[derive(Clone, Copy)]
struct FixedClock;

impl Clock for FixedClock {
    fn now_millis(&self) -> u64 {
        10_000
    }
}

impl MonotonicClock for FixedClock {
    fn now_millis(&self) -> u64 {
        0
    }
}

#[allow(dead_code)]
struct EmptyTaskSource;

impl DashboardDataSource for EmptyTaskSource {
    fn configured_workers(&self) -> Result<Vec<String>, DashboardError> {
        Ok(Vec::new())
    }

    fn collect_workers(&self, _deadline: Duration) -> Vec<WorkerObservationResult> {
        Vec::new()
    }

    fn local_jobs(
        &self,
    ) -> Result<Vec<mac_worker::dashboard::model::DashboardJob>, DashboardError> {
        Ok(Vec::new())
    }

    fn authoritative_active_jobs(
        &self,
        _deadline: Duration,
    ) -> Vec<Result<mac_worker::dashboard::model::DashboardJob, DashboardError>> {
        Vec::new()
    }

    fn queue_entries(
        &self,
    ) -> Result<Vec<mac_worker::dashboard::model::DashboardQueueEntry>, DashboardError> {
        Ok(Vec::new())
    }
}
