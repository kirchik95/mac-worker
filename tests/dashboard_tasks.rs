use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy, TurnLimits},
    client_state::ClientStateStore,
    config::{Config, WorkerEntry},
    dashboard::{
        model::{DashboardError, DashboardQueueEntryKind, DashboardSnapshot},
        service::{
            Clock, DashboardDataSource, DashboardService, MonotonicClock, WorkerObservationResult,
        },
        source::{DashboardRemoteReader, DashboardWorkerReader, MacWorkerDashboardSource},
    },
    error::WorkerError,
    job::{
        AdmissionObservation, CommandSummary, JobId, LogChunk, LogStream, QueueEntry,
        QueueEntryKind, QueueRunReference, RunId as QueueRunId, StatusResponse,
    },
    lease::SlotState,
    protocol::{
        HealthStatus, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION,
        WorkerHealth as ProbeWorkerHealth, WorkersReport,
    },
    scheduler::{CandidateSlot, WorkerPreference},
    task::{
        ClosePolicy, GitIdentity, LocalTaskRecord, RunId, RunRecord, RunnerIdentity, RunnerState,
        TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState,
        TaskStatus, TurnId, TurnSummary,
    },
    task_store::{TaskStatusRequest, TaskStatusResponse},
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
fn remote_status_failure_keeps_a_stale_row_and_dead_runner() {
    let harness = DashboardTaskHarness::active_local_task()
        .with_runner_liveness(Some(RunnerState::Dead))
        .with_remote_failure("SSH_UNAVAILABLE");
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
    assert_eq!(active.turn_number, 1);
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
        let temp = Arc::new(tempfile::tempdir().unwrap());
        let state_root = temp.path().canonicalize().unwrap().join("state");
        let state = Arc::new(ClientStateStore::open(&state_root).unwrap());
        let task_id = task_id(1);
        let turn_id = turn_id(1);
        let run_id = run_id();
        state
            .create_task(task_record(
                task_id,
                run_id,
                TaskState::Active,
                Some("mini-1"),
                Some(turn_id),
                true,
            ))
            .unwrap();
        state
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
            state,
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
    mutation_calls: AtomicUsize,
}

impl FakeRemote {
    fn set_task_status(&self, response: Result<TaskStatusResponse, String>) {
        *self.task_status.lock().unwrap() = Some(response);
    }

    fn mutation_calls(&self) -> usize {
        self.mutation_calls.load(Ordering::SeqCst)
    }

    fn task_status_response(&self) -> Result<TaskStatusResponse, WorkerError> {
        self.task_status_calls.fetch_add(1, Ordering::SeqCst);
        match self.task_status.lock().unwrap().clone() {
            Some(Ok(response)) => Ok(response),
            Some(Err(code)) => Err(WorkerError::Protocol(code)),
            None => Err(WorkerError::Protocol("TASK_NOT_FOUND".into())),
        }
    }
}

impl DashboardRemoteReader for FakeRemote {
    fn status(&self, _worker: &WorkerEntry, _job_id: JobId) -> Result<StatusResponse, WorkerError> {
        Err(WorkerError::Protocol("JOB_NOT_FOUND".into()))
    }

    fn log_chunk(
        &self,
        _worker: &WorkerEntry,
        _job_id: JobId,
        _stream: LogStream,
        _offset: u64,
        _limit: u32,
    ) -> Result<LogChunk, WorkerError> {
        Err(WorkerError::Protocol("JOB_NOT_FOUND".into()))
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
        workers: names
            .iter()
            .map(|name| WorkerEntry {
                name: (*name).into(),
                ssh: format!("operator@{name}.internal"),
                slots: 1,
                capabilities: vec!["swift".into()],
                remote_binary: "~/.local/bin/worker".into(),
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
        vec![format!("question {SECRET}")],
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
            policy: PermissionPolicy::Workspace,
            source: TaskSource::Local { wip: false },
            publish: vec![mac_worker::task::PublishMode::Fetch],
            publish_branch: None,
            base_oid: BASE_OID.parse().unwrap(),
            limits: TaskLimits::new(TurnLimits::new(60_000, None, None).unwrap(), 2).unwrap(),
            close_policy: ClosePolicy::Done,
            env_profile: Some("secret-profile".into()),
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
        "secret-profile",
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
