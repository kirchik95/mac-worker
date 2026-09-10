#[allow(dead_code)]
mod support;

use std::{
    ffi::OsStr,
    io::{Read, Write},
    net::TcpStream,
    os::unix::process::ExitStatusExt,
    process::ExitStatus,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, AtomicUsize, Ordering},
    },
    time::Duration,
};

use mac_worker::{
    client_state::ClientStateStore,
    config::Config,
    dashboard::{
        model::{ApiError, DashboardError, DashboardJob, DashboardLogChunk},
        service::{
            Clock, DashboardDataSource, DashboardService, MonotonicClock, WorkerObservationResult,
        },
        task::{
            DashboardTaskMutationSource, DashboardTaskSource, MacWorkerTaskMutationSource,
            TaskMutationRequest,
        },
        web::{DashboardHttpServer, DashboardHttpState, DashboardLogSource},
    },
    error::WorkerError,
    job::{JobId, LogStream, ProcessIdentity},
    paths::PathLayout,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    project_state::ProjectState,
    task::{
        LocalTaskRecord, RunnerIdentity, TaskId, TaskOutcome, TaskState, TaskStatus, TurnId,
        TurnSummary, TurnTerminal,
    },
    task_store::{TaskCloseRequest, TaskCloseResponse, TaskStatusResponse},
    task_view::{ReviewState, TaskDetailProjection},
    transfer::HostOperation,
    turn_runner::RunnerExecutor,
};
use uuid::Uuid;

use crate::support::task_harness;

struct TaskRemoteRunner {
    status: Mutex<TaskStatus>,
}

impl TaskRemoteRunner {
    fn new(status: TaskStatus) -> Self {
        Self {
            status: Mutex::new(status),
        }
    }
}

impl ProcessRunner for TaskRemoteRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/git") {
            return SystemProcessRunner.run(request);
        }
        if request.program != OsStr::new("/usr/bin/ssh") {
            return Err(WorkerError::Protocol("unexpected fixture process".into()));
        }
        let operation = request
            .args
            .last()
            .and_then(|argument| argument.to_str())
            .unwrap_or_default();
        match operation {
            value if value == HostOperation::TaskStatus.command() => {
                let status = self.status.lock().unwrap().clone();
                canonical_process(&TaskStatusResponse::new(status))
            }
            value if value == HostOperation::TaskClose.command() => {
                apply_close(&self.status, request)
            }
            other => Err(WorkerError::Protocol(format!(
                "unexpected fixture worker operation: {other}"
            ))),
        }
    }
}

struct FailingCloseRunner {
    inner: TaskRemoteRunner,
    fail_remaining: AtomicU32,
}

impl ProcessRunner for FailingCloseRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/git") {
            return SystemProcessRunner.run(request);
        }
        let operation = request
            .args
            .last()
            .and_then(|argument| argument.to_str())
            .unwrap_or_default();
        if operation == HostOperation::TaskClose.command()
            && self.fail_remaining.load(Ordering::SeqCst) > 0
        {
            self.fail_remaining.fetch_sub(1, Ordering::SeqCst);
            return Ok(failed_host_request());
        }
        self.inner.run(request)
    }
}

fn apply_close(
    status: &Mutex<TaskStatus>,
    request: &ProcessRequest,
) -> Result<ProcessResult, WorkerError> {
    let close: TaskCloseRequest = decode_request(request)?;
    let current = status.lock().unwrap().clone();
    let next = TaskStatus::new(
        if close.discard() {
            TaskState::Abandoned
        } else {
            TaskState::Closed
        },
        current.last_outcome().cloned(),
        current.worker().map(str::to_owned),
        current.session_present(),
        current.head_oid().cloned(),
        current.summary().map(str::to_owned),
        current.questions().to_vec(),
        current.files_changed().to_vec(),
        current.diff_stat().map(str::to_owned),
        current.turns().to_vec(),
        current.updated_at_millis() + 1,
    )?
    .copying_reported_checks(&current)?;
    *status.lock().unwrap() = next.clone();
    canonical_process(&TaskCloseResponse::new(next))
}

fn decode_request<T: serde::de::DeserializeOwned>(
    request: &ProcessRequest,
) -> Result<T, WorkerError> {
    serde_json::from_slice(
        request
            .stdin
            .as_deref()
            .ok_or_else(|| WorkerError::Protocol("fixture worker request had no stdin".into()))?,
    )
    .map_err(|error| WorkerError::Protocol(error.to_string()))
}

fn canonical_process<T: serde::Serialize>(value: &T) -> Result<ProcessResult, WorkerError> {
    let mut stdout =
        serde_json::to_vec(value).map_err(|error| WorkerError::Protocol(error.to_string()))?;
    stdout.push(b'\n');
    Ok(ProcessResult {
        status: ExitStatus::from_raw(0),
        stdout,
        stderr: Vec::new(),
    })
}

fn failed_host_request() -> ProcessResult {
    ProcessResult {
        status: ExitStatus::from_raw(23 << 8),
        stdout: Vec::new(),
        stderr: Vec::new(),
    }
}

struct RecordingExecutor {
    starts: AtomicUsize,
    last: Mutex<Option<(TaskId, TurnId)>>,
}

impl RecordingExecutor {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            starts: AtomicUsize::new(0),
            last: Mutex::new(None),
        })
    }
}

impl RunnerExecutor for RecordingExecutor {
    fn start(
        &self,
        _paths: &PathLayout,
        task_id: TaskId,
        turn_id: TurnId,
    ) -> Result<RunnerIdentity, WorkerError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        *self.last.lock().unwrap() = Some((task_id, turn_id));
        Ok(RunnerIdentity::new(ProcessIdentity::new(2_000_000_001, 1)?))
    }
}

struct MutationHarness {
    _repo: support::GitRepo,
    _state_root: tempfile::TempDir,
    store: Arc<ClientStateStore>,
    paths: PathLayout,
    config: Config,
    task_id: TaskId,
    record: LocalTaskRecord,
}

fn task_config() -> Config {
    Config::parse("version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n")
        .unwrap()
}

fn open_mutation_task() -> MutationHarness {
    let repo = support::GitRepo::init();
    let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let state_root = tempfile::tempdir().unwrap();
    let paths = task_harness::paths(state_root.path().canonicalize().unwrap());
    let store = Arc::new(ClientStateStore::open(&paths.state).unwrap());
    let task_id = TaskId::new(Uuid::from_u128(0x31));
    let turn_id = JobId::new(Uuid::from_u128(0x32));
    let record = open_done_record_for_project(
        task_id,
        turn_id,
        project.context.project_id.clone(),
        project.context.worktree_id.clone(),
    );
    store.create_task(record.clone()).unwrap();
    store.write_task_project_path(&record, repo.root()).unwrap();
    MutationHarness {
        _repo: repo,
        _state_root: state_root,
        store,
        paths,
        config: task_config(),
        task_id,
        record,
    }
}

fn open_done_record_for_project(
    task_id: TaskId,
    turn_id: JobId,
    project_id: String,
    worktree_id: String,
) -> LocalTaskRecord {
    // Keep this fixture aligned with tests/task_review.rs so dashboard
    // mutations exercise the same Open+done review record.
    let base_oid: mac_worker::task::BaseOid = "a".repeat(40).parse().unwrap();
    let meta = mac_worker::task::TaskMeta::new(mac_worker::task::TaskMetaInput {
        task_id,
        run_id: None,
        project_id,
        worktree_id,
        agent: mac_worker::agent::AgentKind::Codex,
        model: None,
        effort: None,
        policy: mac_worker::agent::PermissionPolicy::Workspace,
        source: mac_worker::task::TaskSource::Local {
            wip: false,
            push_target: None,
        },
        publish: vec![mac_worker::task::PublishMode::Fetch],
        publish_branch: None,
        base_oid: base_oid.clone(),
        limits: mac_worker::task::TaskLimits::default(),
        close_policy: mac_worker::task::ClosePolicy::Never,
        env_profile: None,
        git_identity: mac_worker::task::GitIdentity::new("mac-worker", "mac-worker@example.test")
            .unwrap(),
        title: None,
        prompt: "fixture task".into(),
        created_at_millis: 1,
    })
    .unwrap();
    let turn = TurnSummary::new(
        1,
        turn_id,
        Some(TurnTerminal::Succeeded),
        Some(TaskOutcome::Done),
        Some(true),
        false,
        Some(1),
        Some(2),
    );
    let status = TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Done),
        Some("mini-1".into()),
        true,
        Some(base_oid),
        Some("ready for review".into()),
        Vec::new(),
        Vec::new(),
        None,
        vec![turn],
        2,
    )
    .unwrap();
    LocalTaskRecord::new(
        meta,
        status,
        None,
        None,
        None,
        "c".repeat(64),
        None,
        true,
        None,
    )
    .unwrap()
}

fn mutation_request(record: &LocalTaskRecord, message: Option<&str>) -> TaskMutationRequest {
    let status = record.status();
    TaskMutationRequest {
        message: message.map(str::to_owned),
        expected_task_id: record.meta().task_id(),
        expected_turn_id: status.turns().last().map(TurnSummary::turn_id),
        expected_turn_count: u32::try_from(status.turns().len()).unwrap(),
        expected_head_oid: status.head_oid().cloned(),
        expected_updated_at_millis: status.updated_at_millis(),
        expected_state: status.state(),
    }
}

fn complete_extra_turn(store: &ClientStateStore, task_id: TaskId) {
    let current = store.load_task(task_id).unwrap();
    let extra_id = JobId::new(Uuid::from_u128(0x99));
    let mut turns = current.status().turns().to_vec();
    let next_number = u32::try_from(turns.len()).unwrap() + 1;
    turns.push(TurnSummary::new(
        next_number,
        extra_id,
        Some(TurnTerminal::Succeeded),
        Some(TaskOutcome::Done),
        Some(true),
        false,
        Some(3),
        Some(4),
    ));
    let status = TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Done),
        current.status().worker().map(str::to_owned),
        current.status().session_present(),
        current.status().head_oid().cloned(),
        current.status().summary().map(str::to_owned),
        current.status().questions().to_vec(),
        current.status().files_changed().to_vec(),
        current.status().diff_stat().map(str::to_owned),
        turns,
        current.status().updated_at_millis() + 10,
    )
    .unwrap()
    .copying_reported_checks(current.status())
    .unwrap();
    store
        .update_task(current.with_status(status).unwrap())
        .unwrap();
}

fn queue_len(store: &ClientStateStore) -> usize {
    store.queue_snapshot().unwrap().entries().len()
}

#[test]
fn stale_reply_after_expected_check_does_not_enqueue_or_start() {
    let harness = open_mutation_task();
    let executor = RecordingExecutor::new();
    let store = Arc::clone(&harness.store);
    let task_id = harness.task_id;
    let expected = mutation_request(&harness.record, Some("please add tests"));
    let source = MacWorkerTaskMutationSource::new(
        Arc::new(harness.config.clone()),
        Arc::clone(&harness.store),
        harness.paths.clone(),
    )
    .with_process_runner(Arc::new(TaskRemoteRunner::new(
        harness.record.status().clone(),
    )))
    .with_executor(Arc::clone(&executor) as Arc<dyn RunnerExecutor>)
    .with_after_expected_check(Arc::new(move || complete_extra_turn(&store, task_id)));

    let error = source.reply(harness.task_id, &expected).unwrap_err();
    assert_eq!(error.code, "TASK_REVISION_CONFLICT");
    assert_eq!(executor.starts.load(Ordering::SeqCst), 0);
    assert_eq!(queue_len(&harness.store), 0);
    let current = harness.store.load_task(harness.task_id).unwrap();
    assert_eq!(current.status().turns().len(), 2);
    assert_eq!(current.status().state(), TaskState::Open);
    assert!(current.runner().is_none());
}

#[test]
fn stale_accept_after_expected_check_does_not_fence_or_close() {
    let harness = open_mutation_task();
    let executor = RecordingExecutor::new();
    let store = Arc::clone(&harness.store);
    let task_id = harness.task_id;
    let expected = mutation_request(&harness.record, None);
    let source = MacWorkerTaskMutationSource::new(
        Arc::new(harness.config.clone()),
        Arc::clone(&harness.store),
        harness.paths.clone(),
    )
    .with_process_runner(Arc::new(TaskRemoteRunner::new(
        harness.record.status().clone(),
    )))
    .with_executor(Arc::clone(&executor) as Arc<dyn RunnerExecutor>)
    .with_after_expected_check(Arc::new(move || complete_extra_turn(&store, task_id)));

    let error = source.accept(harness.task_id, &expected).unwrap_err();
    assert_eq!(error.code, "TASK_REVISION_CONFLICT");
    let current = harness.store.load_task(harness.task_id).unwrap();
    assert!(current.close_intent().is_none());
    assert_eq!(current.status().state(), TaskState::Open);
    assert_eq!(current.status().turns().len(), 2);
}

#[test]
fn reply_starts_exactly_one_detached_follow_up_without_chdir() {
    let harness = open_mutation_task();
    let cwd_before = std::env::current_dir().unwrap();
    let executor = RecordingExecutor::new();
    let source = MacWorkerTaskMutationSource::new(
        Arc::new(harness.config.clone()),
        Arc::clone(&harness.store),
        harness.paths.clone(),
    )
    .with_process_runner(Arc::new(TaskRemoteRunner::new(
        harness.record.status().clone(),
    )))
    .with_executor(Arc::clone(&executor) as Arc<dyn RunnerExecutor>);

    let detail = source
        .reply(
            harness.task_id,
            &mutation_request(&harness.record, Some("please add tests")),
        )
        .unwrap();
    assert_eq!(std::env::current_dir().unwrap(), cwd_before);
    assert_eq!(executor.starts.load(Ordering::SeqCst), 1);
    assert_eq!(queue_len(&harness.store), 1);
    let current = harness.store.load_task(harness.task_id).unwrap();
    assert_eq!(current.status().state(), TaskState::Active);
    assert_eq!(
        current.runner().unwrap().process_identity().pid(),
        2_000_000_001
    );
    assert_eq!(detail.task.turn_count, 2);
}

#[test]
fn accept_retry_after_lost_close_clears_intent_and_is_accepted() {
    let harness = open_mutation_task();
    let remote = Arc::new(FailingCloseRunner {
        inner: TaskRemoteRunner::new(harness.record.status().clone()),
        fail_remaining: AtomicU32::new(1),
    });
    let executor = RecordingExecutor::new();
    let source = MacWorkerTaskMutationSource::new(
        Arc::new(harness.config.clone()),
        Arc::clone(&harness.store),
        harness.paths.clone(),
    )
    .with_process_runner(Arc::clone(&remote) as Arc<dyn ProcessRunner>)
    .with_executor(Arc::clone(&executor) as Arc<dyn RunnerExecutor>);

    let error = source
        .accept(harness.task_id, &mutation_request(&harness.record, None))
        .unwrap_err();
    assert_eq!(error.code, "HOST_REQUEST_FAILED");
    let fenced = harness.store.load_task(harness.task_id).unwrap();
    assert_eq!(fenced.status().state(), TaskState::Open);
    assert!(fenced.close_intent().is_some());

    let detail = source
        .accept(harness.task_id, &mutation_request(&fenced, None))
        .unwrap();
    assert_eq!(detail.review_state, ReviewState::Accepted);
    let accepted = harness.store.load_task(harness.task_id).unwrap();
    assert_eq!(accepted.status().state(), TaskState::Closed);
    assert!(accepted.close_intent().is_none());
}

struct EmptyDashboardSource;

impl DashboardDataSource for EmptyDashboardSource {
    fn configured_workers(&self) -> Result<Vec<String>, DashboardError> {
        Ok(Vec::new())
    }

    fn collect_workers(&self, _deadline: Duration) -> Vec<WorkerObservationResult> {
        Vec::new()
    }

    fn local_jobs(&self) -> Result<Vec<DashboardJob>, DashboardError> {
        Ok(Vec::new())
    }

    fn authoritative_active_jobs(
        &self,
        _deadline: Duration,
    ) -> Vec<Result<DashboardJob, DashboardError>> {
        Vec::new()
    }

    fn queue_entries(
        &self,
    ) -> Result<Vec<mac_worker::dashboard::model::DashboardQueueEntry>, DashboardError> {
        Ok(Vec::new())
    }
}

struct UnusedLogs;

impl DashboardLogSource for UnusedLogs {
    fn job_detail(&self, _job_id: JobId) -> Result<DashboardJob, ApiError> {
        Err(ApiError::new("JOB_NOT_FOUND", "unused"))
    }

    fn read_log(
        &self,
        _job_id: JobId,
        _stream: LogStream,
        _offset: u64,
        _limit: u32,
    ) -> Result<DashboardLogChunk, ApiError> {
        Err(ApiError::new("JOB_NOT_FOUND", "unused"))
    }
}

struct UnusedTasks;

impl DashboardTaskSource for UnusedTasks {
    fn task_detail(&self, _task_id: TaskId) -> Result<TaskDetailProjection, ApiError> {
        Err(ApiError::new("TASK_NOT_FOUND", "unused"))
    }

    fn read_task_log(
        &self,
        _task_id: TaskId,
        _turn_id: TurnId,
        _stream: LogStream,
        _offset: u64,
        _limit: u32,
    ) -> Result<DashboardLogChunk, ApiError> {
        Err(ApiError::new("TURN_NOT_FOUND", "unused"))
    }
}

struct FixedClock;

impl Clock for FixedClock {
    fn now_millis(&self) -> u64 {
        1_000
    }
}

struct FixedMonotonic;

impl MonotonicClock for FixedMonotonic {
    fn now_millis(&self) -> u64 {
        0
    }
}

struct HttpResponse {
    status: u16,
    body: Vec<u8>,
}

impl HttpResponse {
    fn parse(raw: Vec<u8>) -> Self {
        let split = raw
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .unwrap();
        let (head, body) = raw.split_at(split + 4);
        let status = std::str::from_utf8(head)
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        Self {
            status,
            body: body.to_vec(),
        }
    }
}

fn post_task(address: &str, path: &str, origin: &str, body: &[u8]) -> HttpResponse {
    let mut stream = TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let header = format!(
        "POST {path} HTTP/1.1\r\nHost: {address}\r\nOrigin: {origin}\r\nContent-Type: application/json\r\nX-Mac-Worker-Task: 1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut wire = header.into_bytes();
    wire.extend_from_slice(body);
    stream.write_all(&wire).unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    HttpResponse::parse(raw)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_stale_reply_returns_409_with_zero_runner_starts() {
    let harness = open_mutation_task();
    let executor = RecordingExecutor::new();
    let store = Arc::clone(&harness.store);
    let task_id = harness.task_id;
    let source = MacWorkerTaskMutationSource::new(
        Arc::new(harness.config.clone()),
        Arc::clone(&harness.store),
        harness.paths.clone(),
    )
    .with_process_runner(Arc::new(TaskRemoteRunner::new(
        harness.record.status().clone(),
    )))
    .with_executor(Arc::clone(&executor) as Arc<dyn RunnerExecutor>)
    .with_after_expected_check(Arc::new(move || complete_extra_turn(&store, task_id)));
    let state = Arc::new(DashboardHttpState {
        service: Arc::new(DashboardService::new(
            EmptyDashboardSource,
            FixedClock,
            FixedMonotonic,
        )),
        log_source: Arc::new(UnusedLogs),
        task_source: Arc::new(UnusedTasks),
        settings_source: None,
        mutation_source: Some(Arc::new(source)),
    });
    let server = DashboardHttpServer::bind(None, state).await.unwrap();
    let address = server
        .local_url()
        .strip_prefix("http://")
        .unwrap()
        .to_owned();
    let origin = format!("http://{address}");
    let path = format!("/api/v1/tasks/{}/reply", harness.task_id);
    let body = serde_json::to_vec(&serde_json::json!({
        "message": "please add tests",
        "expected_task_id": harness.record.meta().task_id(),
        "expected_turn_id": harness.record.status().turns().last().map(TurnSummary::turn_id),
        "expected_turn_count": harness.record.status().turns().len(),
        "expected_head_oid": harness.record.status().head_oid(),
        "expected_updated_at_millis": harness.record.status().updated_at_millis(),
        "expected_state": "open"
    }))
    .unwrap();

    let response = post_task(&address, &path, &origin, &body);
    assert_eq!(response.status, 409);
    let payload: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(payload["error"]["code"], "TASK_REVISION_CONFLICT");
    assert_eq!(executor.starts.load(Ordering::SeqCst), 0);
    assert_eq!(queue_len(&harness.store), 0);
    server.shutdown().await.unwrap();
}
