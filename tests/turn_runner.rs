#[allow(dead_code)]
mod support;

use std::{
    ffi::OsStr,
    fs,
    io::{self, Write},
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    path::{Path, PathBuf},
    process::{self, ExitStatus},
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use mac_worker::herdr::HerdrSocket;
use mac_worker::{
    agent::{AgentKind, PermissionPolicy},
    agent_facts::{AgentAuth, AgentFacts, AgentProbe, FACTS_TTL, ProfileProbe},
    client_state::{
        ClientStateConcurrencyHook, ClientStateConcurrencyPoint, ClientStateStore,
        ClientStateWritePoint,
    },
    config::{Config, WorkerEntry},
    dashboard::{
        service::{DashboardService, SystemClock, SystemMonotonicClock},
        source::{DashboardRemoteReader, DashboardWorkerReader, MacWorkerDashboardSource},
        task::{DashboardTaskSource, MacWorkerTaskSource},
    },
    error::WorkerError,
    job::{
        AdmissionObservation, CommandSummary, HostControlError, JobId, JobMeta, JobState,
        JobStatus, LeaseAcquireRequest, LeaseAcquireResponse, LeaseRecord, LogChunk,
        LogChunkRequest, LogStream, ProcessIdentity, QueueEntry, QueueEntryKind, StatusLogsRequest,
        StatusLogsResponse, StatusRequest, StatusResponse, SubmitResponse,
    },
    lease::SlotState,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    project_state::ProjectState,
    protocol::{
        CpuCounters, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION,
        WorkersReport,
    },
    scheduler::WorkerPreference,
    supervisor::{
        ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
    },
    task::{
        BaseOid, ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, RunId, RunRecord,
        RunnerState, TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome, TaskSource,
        TaskState, TaskStatus, TurnId, TurnSummary, TurnTerminal,
    },
    task_client::{TaskClient, TaskSubmitRequest, WaitSelector},
    task_store::{
        TaskCloseRequest, TaskCloseResponse, TaskPrepareRequest, TaskPrepareResponse,
        TaskStatusRequest, TaskStatusResponse,
    },
    transfer::HostOperation,
    transfer_repo::TransferRepo,
    turn::{TaskTurnRequest, TaskTurnResponse},
    turn_runner::{
        DetachedRunnerExecutor, InlineRunnerExecutor, RunnerExecutor, RunnerStart, TurnRunner,
        start_runner_with_reservation,
    },
};

static CURRENT_DIR_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn inline_executor_returns_the_current_process_identity() {
    let executor = InlineRunnerExecutor;
    let identity = executor
        .start(
            &support::task_harness::paths(tempfile::tempdir().unwrap().path()),
            TaskId::generate(),
            TurnId::generate(),
        )
        .unwrap()
        .process_identity();

    assert_eq!(identity.pid(), process::id());
    assert!(identity.start_time_micros() > 0);
}

#[test]
fn detached_executor_creates_private_runner_log_and_directory() {
    let temp = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(temp.path().canonicalize().unwrap());
    let task_id = TaskId::generate();
    let turn_id = TurnId::generate();

    let identity = DetachedRunnerExecutor
        .start(&paths, task_id, turn_id)
        .unwrap()
        .process_identity();

    assert!(identity.pid() > 0);
    let task_dir = paths.state.join("runners").join(task_id.to_string());
    let log =
        support::task_harness::runner_log(temp.path(), &task_id.to_string(), &turn_id.to_string());
    assert_eq!(
        fs::metadata(&task_dir).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(log).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[test]
fn detached_runner_does_not_mirror_child_stdio_into_owner_log() {
    let temp = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(temp.path().canonicalize().unwrap());
    let task_id = TaskId::generate();
    let turn_id = TurnId::generate();

    DetachedRunnerExecutor
        .start(&paths, task_id, turn_id)
        .unwrap();
    let log =
        support::task_harness::runner_log(temp.path(), &task_id.to_string(), &turn_id.to_string());
    let child_output = (0..50).find_map(|_| {
        let bytes = fs::read(&log).unwrap();
        if bytes.is_empty() {
            std::thread::sleep(Duration::from_millis(10));
            None
        } else {
            Some(bytes)
        }
    });

    assert!(
        child_output.is_none(),
        "detached child stdout/stderr must not be mirrored into the owner log"
    );
}

#[test]
fn process_identity_rejects_zero_components() {
    assert!(ProcessIdentity::new(0, 1).is_err());
    assert!(ProcessIdentity::new(1, 0).is_err());
}

struct CurrentDirGuard {
    previous: PathBuf,
}

impl CurrentDirGuard {
    fn enter(path: &Path) -> Self {
        let previous = std::env::current_dir().unwrap();
        std::env::set_current_dir(path).unwrap();
        Self { previous }
    }
}

impl Drop for CurrentDirGuard {
    fn drop(&mut self) {
        std::env::set_current_dir(&self.previous).unwrap();
    }
}

struct FetchFailingRunner {
    requests: Mutex<Vec<ProcessRequest>>,
    task: Mutex<Option<(TaskMeta, TurnId)>>,
}

struct AcceptedThenTerminalRunner {
    requests: Mutex<Vec<ProcessRequest>>,
    task: Mutex<Option<(TaskMeta, TurnId)>>,
    turn_submitted: Mutex<bool>,
    stdout_log_sent: Mutex<bool>,
    probe_count: Mutex<u32>,
    facts_fresh: Mutex<bool>,
    first_probe_fresh: bool,
    base_release_failures: Mutex<u8>,
    worker: Mutex<String>,
}

/// A remote with immutable, seekable logs. Unlike a once-only byte supplier,
/// this exposes duplicate reads after a runner restart and a final tail larger
/// than one transport chunk.
struct ReplayableLogsRunner<'a> {
    inner: &'a AcceptedThenTerminalRunner,
    logs: [Vec<u8>; 2],
    meta: Mutex<Option<JobMeta>>,
    reads: Mutex<Vec<(LogStream, u64)>>,
    status_logs: Mutex<Vec<(u64, u64)>>,
    status_calls: Mutex<usize>,
    fail_stderr_once: AtomicBool,
    fail_after_first_status_logs: AtomicBool,
    terminal_on_submit: bool,
    combined: bool,
}

impl<'a> ReplayableLogsRunner<'a> {
    fn new(inner: &'a AcceptedThenTerminalRunner) -> Self {
        Self {
            inner,
            logs: [vec![0xf1; 150_011], vec![0xfe; 91_017]],
            meta: Mutex::new(None),
            reads: Mutex::new(Vec::new()),
            status_logs: Mutex::new(Vec::new()),
            status_calls: Mutex::new(0),
            fail_stderr_once: AtomicBool::new(false),
            fail_after_first_status_logs: AtomicBool::new(false),
            terminal_on_submit: false,
            combined: false,
        }
    }

    fn combined(inner: &'a AcceptedThenTerminalRunner) -> Self {
        let mut runner = Self::new(inner);
        runner.combined = true;
        runner
    }

    fn terminal_job(&self, meta: &JobMeta) -> Result<StatusResponse, WorkerError> {
        StatusResponse::new(
            meta.clone(),
            JobStatus::new(
                JobState::Succeeded,
                meta.created_at_millis() + 2,
                None,
                None,
                None,
                None,
                Some(0),
                None,
                Some(self.logs[0].len() as u64),
                Some(self.logs[1].len() as u64),
                None,
                None,
            )?,
        )
    }

    fn log_slice(&self, stream: LogStream, offset: u64, limit: u32) -> Vec<u8> {
        let bytes = match stream {
            LogStream::Stdout => &self.logs[0],
            LogStream::Stderr => &self.logs[1],
        };
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        if start >= bytes.len() {
            return Vec::new();
        }
        let end = bytes
            .len()
            .min(start.saturating_add(usize::try_from(limit).unwrap_or(0)));
        bytes[start..end].to_vec()
    }
}

impl ProcessRunner for ReplayableLogsRunner<'_> {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let operation = request.args.last().and_then(|arg| arg.to_str());
        if operation == Some(HostOperation::TaskTurn.command()) {
            let turn: TaskTurnRequest = decode_request(request)?;
            let material = turn.submit().material();
            *self.meta.lock().unwrap() = Some(JobMeta::new(material, material.fingerprint())?);
            let response = self.inner.run(request)?;
            return if self.terminal_on_submit {
                let accepted: TaskTurnResponse = serde_json::from_slice(&response.stdout).unwrap();
                canonical_process(&TaskTurnResponse::new(
                    accepted.submit().clone(),
                    self.inner.task_status(turn.turn().task_id(), true)?,
                ))
            } else {
                Ok(response)
            };
        }
        if operation == Some(HostOperation::Status.command()) {
            let query: StatusRequest = decode_request(request)?;
            let meta = self.meta.lock().unwrap().clone().unwrap();
            assert_eq!(query.job_id(), meta.job_id());
            *self.status_calls.lock().unwrap() += 1;
            return canonical_process(&self.terminal_job(&meta)?);
        }
        if operation == Some(HostOperation::StatusLogs.command()) {
            if !self.combined {
                return self.inner.run(request);
            }
            let query: StatusLogsRequest = decode_request(request)?;
            let meta = self.meta.lock().unwrap().clone().unwrap();
            assert_eq!(query.job_id(), meta.job_id());
            self.status_logs
                .lock()
                .unwrap()
                .push((query.stdout_offset(), query.stderr_offset()));
            let polls = self.status_logs.lock().unwrap().len();
            if polls > 1
                && self
                    .fail_after_first_status_logs
                    .swap(false, Ordering::SeqCst)
            {
                return Err(WorkerError::Transport {
                    code: "SSH_FAILED",
                    message: "fixture connection interrupted after combined append".into(),
                });
            }
            let stdout = LogChunk::new(
                LogStream::Stdout,
                query.stdout_offset(),
                self.log_slice(
                    LogStream::Stdout,
                    query.stdout_offset(),
                    query.stdout_limit(),
                ),
            )?;
            let stderr = LogChunk::new(
                LogStream::Stderr,
                query.stderr_offset(),
                self.log_slice(
                    LogStream::Stderr,
                    query.stderr_offset(),
                    query.stderr_limit(),
                ),
            )?;
            return canonical_process(&StatusLogsResponse::new(
                self.terminal_job(&meta)?,
                stdout,
                stderr,
            )?);
        }
        if operation == Some(HostOperation::LogChunk.command()) {
            let query: LogChunkRequest = decode_request(request)?;
            self.reads
                .lock()
                .unwrap()
                .push((query.stream(), query.offset()));
            if query.stream() == LogStream::Stderr
                && self.fail_stderr_once.swap(false, Ordering::SeqCst)
            {
                return Err(WorkerError::Transport {
                    code: "SSH_FAILED",
                    message: "fixture connection interrupted after stdout append".into(),
                });
            }
            let index = if query.stream() == LogStream::Stdout {
                0
            } else {
                1
            };
            let bytes = &self.logs[index];
            let offset = query.offset() as usize;
            let end = bytes.len().min(offset + query.limit() as usize);
            return canonical_process(&mac_worker::job::LogChunkResponse::new(LogChunk::new(
                query.stream(),
                query.offset(),
                bytes[offset..end].to_vec(),
            )?)?);
        }
        self.inner.run(request)
    }
}

struct MissingJobLogsRunner<'a> {
    inner: ReplayableLogsRunner<'a>,
}

impl ProcessRunner for MissingJobLogsRunner<'_> {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let operation = request.args.last().and_then(|arg| arg.to_str());
        if operation == Some(HostOperation::Status.command())
            || operation == Some(HostOperation::LogChunk.command())
        {
            let mut result = canonical_process(&HostControlError::new(
                "JOB_NOT_FOUND",
                "job ID is not indexed",
            )?)?;
            result.status = ExitStatus::from_raw(23 << 8);
            return Ok(result);
        }
        self.inner.run(request)
    }
}

struct RejectResultFetch<'a, R> {
    inner: &'a R,
    result_fetches: AtomicUsize,
}

impl<'a, R> RejectResultFetch<'a, R> {
    fn new(inner: &'a R) -> Self {
        Self {
            inner,
            result_fetches: AtomicUsize::new(0),
        }
    }

    fn result_fetch_count(&self) -> usize {
        self.result_fetches.load(Ordering::SeqCst)
    }
}

impl<R: ProcessRunner> ProcessRunner for RejectResultFetch<'_, R> {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/git")
            && request.args.iter().any(|arg| arg == "fetch")
            && request.args.iter().any(|arg| {
                arg.to_string_lossy().contains("refs/mac-worker/results/")
                    || arg.to_string_lossy().starts_with("--upload-pack=")
            })
        {
            self.result_fetches.fetch_add(1, Ordering::SeqCst);
            return Err(WorkerError::Protocol("RESULT_FETCH_FAILED".into()));
        }
        self.inner.run(request)
    }
}

#[derive(Default)]
struct BoundedFollowWriter {
    bytes: Vec<u8>,
    polls: usize,
}

impl Write for BoundedFollowWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.polls += 1;
        if self.polls > 5 {
            panic!("task logs -f kept polling after undrainable completion");
        }
        Ok(())
    }
}

struct HostReportsSuccessReader {
    status: TaskStatus,
}

impl DashboardRemoteReader for HostReportsSuccessReader {
    fn status(&self, _worker: &WorkerEntry, _job_id: JobId) -> Result<StatusResponse, WorkerError> {
        Err(WorkerError::Protocol("JOB_NOT_FOUND".into()))
    }

    fn log_chunk(
        &self,
        _worker: &WorkerEntry,
        _job_id: JobId,
        stream: LogStream,
        offset: u64,
        _limit: u32,
    ) -> Result<LogChunk, WorkerError> {
        LogChunk::new(stream, offset, Vec::new())
    }

    fn task_status(
        &self,
        _worker: &WorkerEntry,
        _request: &TaskStatusRequest,
    ) -> Result<TaskStatusResponse, WorkerError> {
        Ok(TaskStatusResponse::new(self.status.clone()))
    }
}

struct IdleDashboardWorkers;

impl DashboardWorkerReader for IdleDashboardWorkers {
    fn inspect(&self, _config: &Config, _deadline: Duration) -> WorkersReport {
        WorkersReport {
            protocol_version: PROTOCOL_VERSION,
            workers: Vec::new(),
        }
    }
}

fn persist_independent_terminal_status(fixture: &AcceptedThenTerminalFixture) {
    let terminal = fixture.runner.task_status(fixture.task_id, true).unwrap();
    let record = fixture.state.load_task(fixture.task_id).unwrap();
    fixture
        .state
        .update_task(
            record
                .with_status(terminal)
                .unwrap()
                .with_runner(None)
                .unwrap(),
        )
        .unwrap();
}

/// TaskPrepare never ran for a planted journal, so the fake host has no
/// prepared task. Synthesize Open/Done locally so reconcile will not
/// recreate a row after retiring a completed dispatch.
fn persist_local_open_success(fixture: &AcceptedThenTerminalFixture) {
    let record = fixture.state.load_task(fixture.task_id).unwrap();
    let now = record.status().updated_at_millis().saturating_add(1);
    let turn = record.status().turns().last().map_or_else(
        || {
            TurnSummary::new(
                1,
                fixture.turn_id,
                Some(TurnTerminal::Succeeded),
                Some(TaskOutcome::Done),
                Some(true),
                false,
                Some(1),
                Some(2),
            )
        },
        |last| {
            TurnSummary::new(
                last.turn_number(),
                last.turn_id(),
                Some(TurnTerminal::Succeeded),
                Some(TaskOutcome::Done),
                last.agent_committed().or(Some(true)),
                last.log_truncated(),
                last.started_at_millis(),
                Some(last.ended_at_millis().unwrap_or(now)),
            )
        },
    );
    let turns = if record.status().turns().is_empty() {
        vec![turn]
    } else {
        let mut turns = record.status().turns().to_vec();
        *turns.last_mut().unwrap() = turn;
        turns
    };
    let status = TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Done),
        record.status().worker().map(str::to_owned),
        record.status().session_present(),
        record.status().head_oid().cloned(),
        Some("finished".into()),
        record.status().questions().to_vec(),
        record.status().files_changed().to_vec(),
        record.status().diff_stat().map(str::to_owned),
        turns,
        now,
    )
    .unwrap();
    fixture
        .state
        .update_task(record.with_status(status).unwrap())
        .unwrap();
}

fn plant_undrainable_journal_ahead_of_success_record(fixture: &AcceptedThenTerminalFixture) {
    persist_local_open_success(fixture);
    fixture
        .state
        .open_runner_log(fixture.task_id, fixture.turn_id)
        .unwrap();
    write_fixture_checkpoint(
        fixture,
        &serde_json::json!({
            "version": 1,
            "task_id": fixture.task_id,
            "turn_id": fixture.turn_id,
            "committed": {
                "offsets": [0, 0],
                "len": 0,
                "accepted": true,
                "completion": {
                    "outcome": {"kind": "failed", "reason": "LOG_DRAIN_UNAVAILABLE"},
                    "drained": false
                }
            },
            "pending": null
        }),
    );
    fixture.state.record_runner(fixture.task_id, None).unwrap();
}

/// Prior-turn `fetched_head` is kept by `with_status`/`say`. The current
/// journal/queue row still belongs to `fixture.turn_id`, now as turn 2.
fn plant_undrainable_follow_up_with_prior_fetched_head(
    fixture: &AcceptedThenTerminalFixture,
) -> BaseOid {
    persist_local_open_success(fixture);
    let earlier = BaseOid::from_str("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    let first_turn_id = TurnId::generate();
    let record = fixture.state.load_task(fixture.task_id).unwrap();
    let now = record.status().updated_at_millis().saturating_add(1);
    let first = TurnSummary::new(
        1,
        first_turn_id,
        Some(TurnTerminal::Succeeded),
        Some(TaskOutcome::Done),
        Some(true),
        false,
        Some(1),
        Some(2),
    );
    let second = TurnSummary::new(
        2,
        fixture.turn_id,
        Some(TurnTerminal::Succeeded),
        Some(TaskOutcome::Done),
        Some(true),
        false,
        Some(3),
        Some(4),
    );
    let status = TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Done),
        Some("mini-1".into()),
        record.status().session_present(),
        record.status().head_oid().cloned(),
        Some("finished".into()),
        record.status().questions().to_vec(),
        record.status().files_changed().to_vec(),
        record.status().diff_stat().map(str::to_owned),
        vec![first, second],
        now,
    )
    .unwrap();
    fixture
        .state
        .update_task(
            record
                .with_status(status)
                .unwrap()
                .with_fetched_head(Some(earlier.clone()))
                .unwrap(),
        )
        .unwrap();
    fixture
        .state
        .open_runner_log(fixture.task_id, fixture.turn_id)
        .unwrap();
    write_fixture_checkpoint(
        fixture,
        &serde_json::json!({
            "version": 1,
            "task_id": fixture.task_id,
            "turn_id": fixture.turn_id,
            "committed": {
                "offsets": [0, 0],
                "len": 0,
                "accepted": true,
                "completion": {
                    "outcome": {"kind": "failed", "reason": "LOG_DRAIN_UNAVAILABLE"},
                    "drained": false
                }
            },
            "pending": null
        }),
    );
    fixture.state.record_runner(fixture.task_id, None).unwrap();
    earlier
}

fn claim_waiting_turn(fixture: &AcceptedThenTerminalFixture) {
    let live_owner = fixture
        .state
        .queue_entry(fixture.turn_id)
        .unwrap()
        .unwrap()
        .owner_opt()
        .copied()
        .expect("submitted turn has an owner");
    fixture
        .state
        .claim_next(live_owner, &["mini-1".into()], u64::MAX / 4)
        .unwrap()
        .expect("waiting turn must become dispatching");
}

fn assert_undrainable_publication_kept_the_base_pin(
    store: &ClientStateStore,
    fixture: &AcceptedThenTerminalFixture,
) {
    let record = store.load_task(fixture.task_id).unwrap();
    assert_eq!(
        record.status().last_outcome(),
        Some(&TaskOutcome::failed("LOG_DRAIN_UNAVAILABLE"))
    );
    assert_eq!(record.status().state(), TaskState::Open);
    assert_eq!(record.abandon_code(), Some("LOG_DRAIN_UNAVAILABLE"));
    assert!(record.fetched_head().is_none());
    assert!(
        store
            .queue_entry_for_task_turn(fixture.task_id)
            .unwrap()
            .is_none()
    );
    assert_eq!(store.runner_liveness(fixture.task_id).unwrap(), None);
    assert!(transfer_for_fixture(fixture).has_ref(&base_pin_name(fixture)));
}

fn assert_wait_and_dashboard_keep_undrainable_failure<R: ProcessRunner>(
    remote: &R,
    store: ClientStateStore,
    fixture: &AcceptedThenTerminalFixture,
    host_success: TaskStatus,
) {
    let client = TaskClient::new(
        remote,
        &fixture.config,
        &fixture.paths,
        &store,
        &fixture.executor,
    );
    let waited = client
        .wait(
            WaitSelector::Task(fixture.task_id),
            Some(Duration::from_secs(2)),
        )
        .unwrap();
    assert_eq!(waited.exit_code(), 1);
    let mut follow = BoundedFollowWriter::default();
    client
        .logs(
            fixture.task_id,
            None,
            true,
            true,
            &mut follow,
            &mut Vec::new(),
        )
        .unwrap();
    assert!(follow.polls <= 5);
    drop(client);

    let store = std::sync::Arc::new(store);
    let config = std::sync::Arc::new(fixture.config.clone());
    let dashboard_remote = std::sync::Arc::new(HostReportsSuccessReader {
        status: host_success,
    });
    let tasks = MacWorkerTaskSource::new(
        std::sync::Arc::clone(&config),
        std::sync::Arc::clone(&store),
        std::sync::Arc::clone(&dashboard_remote)
            as std::sync::Arc<dyn mac_worker::dashboard::source::DashboardRemoteReader>,
    );
    let detail = tasks.task_detail(fixture.task_id).unwrap();
    assert_eq!(detail.task.state, TaskState::Open);
    assert_eq!(
        detail.task.last_outcome,
        Some(TaskOutcome::failed("LOG_DRAIN_UNAVAILABLE"))
    );
    let log_error = tasks
        .read_task_log(fixture.task_id, fixture.turn_id, LogStream::Stdout, 0, 64)
        .unwrap_err();
    assert_eq!(log_error.code, "LOG_DRAIN_UNAVAILABLE");

    let snapshot = DashboardService::new(
        MacWorkerDashboardSource::new(
            std::sync::Arc::clone(&config),
            std::sync::Arc::new(IdleDashboardWorkers),
            std::sync::Arc::clone(&store),
            dashboard_remote
                as std::sync::Arc<dyn mac_worker::dashboard::source::DashboardRemoteReader>,
        ),
        SystemClock,
        SystemMonotonicClock::new(),
    )
    .snapshot(Default::default())
    .unwrap();
    let row = snapshot
        .task_view
        .tasks
        .iter()
        .find(|row| row.task_id == fixture.task_id)
        .expect("undrainable task is present in the dashboard snapshot");
    assert_eq!(row.state, TaskState::Open);
    assert_eq!(
        row.last_outcome,
        Some(TaskOutcome::failed("LOG_DRAIN_UNAVAILABLE"))
    );
}

fn fixture_checkpoint(fixture: &AcceptedThenTerminalFixture) -> serde_json::Value {
    let path = fixture
        .paths
        .state
        .join("runners")
        .join(fixture.task_id.to_string())
        .join(format!("{}.checkpoint.json", fixture.turn_id));
    serde_json::from_slice(&fs::read(&path).unwrap()).unwrap()
}

fn write_fixture_checkpoint(fixture: &AcceptedThenTerminalFixture, checkpoint: &serde_json::Value) {
    let path = fixture
        .paths
        .state
        .join("runners")
        .join(fixture.task_id.to_string())
        .join(format!("{}.checkpoint.json", fixture.turn_id));
    fs::write(&path, serde_json::to_vec(checkpoint).unwrap()).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
}

fn adopt_dead_owner_and_reconcile<R: ProcessRunner>(
    fixture: &AcceptedThenTerminalFixture,
    remote: &R,
    dead_owner: ProcessIdentity,
) -> (ClientStateStore, mac_worker::task_client::ReconcileReport) {
    fixture
        .state
        .adopt_row(fixture.turn_id, dead_owner)
        .unwrap();
    let store = ClientStateStore::open_with_owner_inspector(
        &fixture.paths.state,
        DeadOwnerInspector { dead_owner },
    )
    .unwrap();
    let report = TaskClient::new(
        remote,
        &fixture.config,
        &fixture.paths,
        &store,
        &fixture.executor,
    )
    .reconcile_runners()
    .unwrap();
    (store, report)
}

fn transfer_for_fixture(fixture: &AcceptedThenTerminalFixture) -> TransferRepo {
    let project =
        ProjectState::load(&SystemProcessRunner, &std::env::current_dir().unwrap(), &[]).unwrap();
    TransferRepo::open_or_create(&fixture.paths.cache, &project.context.common_dir).unwrap()
}

fn base_pin_name(fixture: &AcceptedThenTerminalFixture) -> String {
    format!("refs/mac-worker/bases/{}", fixture.task_id)
}

fn result_fetch_count(runner: &AcceptedThenTerminalRunner) -> usize {
    runner
        .requests()
        .iter()
        .filter(|request| {
            request.program == OsStr::new("/usr/bin/git")
                && request.args.iter().any(|arg| arg == "fetch")
                && request.args.iter().any(|arg| {
                    arg.to_string_lossy().contains("refs/mac-worker/results/")
                        || arg.to_string_lossy().starts_with("--upload-pack=")
                })
        })
        .count()
}

#[derive(Clone)]
struct ChildAdoptsThenFails {
    state: ClientStateStore,
}

struct ParkedSubmissionGate {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    used: AtomicBool,
}

struct SubmissionIntentRaceGate {
    submit_entered: mpsc::Sender<()>,
    submit_release: Mutex<mpsc::Receiver<()>>,
    reconciliation_entered: mpsc::Sender<()>,
    reconciliation_release: Mutex<mpsc::Receiver<()>>,
}

struct SubmissionIntentClearFailureGate {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

#[derive(Clone, Copy)]
struct DeadOwnerInspector {
    dead_owner: ProcessIdentity,
}

impl ProcessInspector for DeadOwnerInspector {
    fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
        ProcessIdentity::new(pid, u64::from(pid) * 10_000 + 7)
    }

    fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
        if expected == self.dead_owner {
            ProcessObservation::Absent
        } else {
            ProcessObservation::Matching {
                process_group: expected.pid(),
            }
        }
    }

    fn observe_group(&self, _process_group: u32) -> ProcessGroupObservation {
        ProcessGroupObservation::Ambiguous
    }

    fn observe_group_members(&self, _leader: u32) -> ProcessGroupMembership {
        ProcessGroupMembership::Ambiguous
    }
}

impl ClientStateConcurrencyHook for ParkedSubmissionGate {
    fn reach(&self, point: ClientStateConcurrencyPoint) {
        if point == ClientStateConcurrencyPoint::ParkedTaskTurnPublication
            && !self.used.swap(true, Ordering::SeqCst)
        {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
        }
    }
}

impl ClientStateConcurrencyHook for SubmissionIntentRaceGate {
    fn reach(&self, point: ClientStateConcurrencyPoint) {
        match point {
            ClientStateConcurrencyPoint::SubmissionIntentClear => {
                self.submit_entered.send(()).unwrap();
                self.submit_release.lock().unwrap().recv().unwrap();
            }
            ClientStateConcurrencyPoint::SubmissionIntentReconciliationBeforeTransferLock => {
                self.reconciliation_entered.send(()).unwrap();
                self.reconciliation_release.lock().unwrap().recv().unwrap();
            }
            _ => {}
        }
    }
}

impl ClientStateConcurrencyHook for SubmissionIntentClearFailureGate {
    fn reach(&self, point: ClientStateConcurrencyPoint) {
        if point == ClientStateConcurrencyPoint::SubmissionIntentClearResultUncertain {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
        }
    }
}

impl RunnerExecutor for ChildAdoptsThenFails {
    fn start(
        &self,
        paths: &mac_worker::paths::PathLayout,
        task_id: TaskId,
        turn_id: TurnId,
    ) -> Result<mac_worker::task::RunnerIdentity, WorkerError> {
        let identity = InlineRunnerExecutor.start(paths, task_id, turn_id)?;
        self.state.adopt_row(turn_id, identity.process_identity())?;
        Err(WorkerError::Protocol(
            "simulated parent handoff failure after detached child adoption".into(),
        ))
    }
}

impl AcceptedThenTerminalRunner {
    fn new() -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            task: Mutex::new(None),
            turn_submitted: Mutex::new(false),
            stdout_log_sent: Mutex::new(false),
            probe_count: Mutex::new(0),
            facts_fresh: Mutex::new(false),
            first_probe_fresh: true,
            base_release_failures: Mutex::new(0),
            worker: Mutex::new("mini-1".into()),
        }
    }

    /// A worker whose facts cache has already aged out when submit probes it.
    fn with_stale_facts() -> Self {
        Self {
            first_probe_fresh: false,
            ..Self::new()
        }
    }

    fn with_base_release_failure() -> Self {
        let runner = Self::new();
        *runner.base_release_failures.lock().unwrap() = 1;
        runner
    }

    fn task_status(
        &self,
        task_id: TaskId,
        terminal: bool,
    ) -> Result<TaskStatus, mac_worker::error::WorkerError> {
        let task = self.task.lock().unwrap();
        let (meta, turn_id) = task.as_ref().ok_or_else(|| {
            mac_worker::error::WorkerError::Protocol(
                "task status requested before task preparation".into(),
            )
        })?;
        assert_eq!(meta.task_id(), task_id);
        let outcome = terminal.then_some(TaskOutcome::Done);
        let turn = TurnSummary::new(
            1,
            *turn_id,
            terminal.then_some(TurnTerminal::Succeeded),
            outcome.clone(),
            terminal.then_some(true),
            false,
            Some(meta.created_at_millis()),
            terminal.then_some(meta.created_at_millis() + 1),
        );
        TaskStatus::new(
            if terminal {
                TaskState::Closed
            } else {
                TaskState::Active
            },
            outcome,
            Some(self.worker.lock().unwrap().clone()),
            true,
            Some(meta.base_oid().clone()),
            terminal.then_some("finished".into()),
            Vec::new(),
            Vec::new(),
            None,
            vec![turn],
            meta.created_at_millis() + u64::from(terminal),
        )
    }

    fn requests(&self) -> Vec<ProcessRequest> {
        self.requests.lock().unwrap().clone()
    }

    fn facts_fresh(&self) -> bool {
        *self.facts_fresh.lock().unwrap()
    }

    fn facts_are_fresh_for_probe(&self) -> bool {
        let mut count = self.probe_count.lock().unwrap();
        *count += 1;
        self.facts_fresh() || (self.first_probe_fresh && *count == 1)
    }

    fn submitted_turn_origin(&self) -> Option<String> {
        self.requests()
            .into_iter()
            .find(|request| {
                request
                    .args
                    .iter()
                    .any(|arg| arg == HostOperation::TaskTurn.command())
            })
            .map(|request| decode_request::<TaskTurnRequest>(&request).unwrap())
            .and_then(|request| request.origin_url().map(str::to_owned))
    }

    fn origin_read_count(&self) -> usize {
        self.requests()
            .iter()
            .filter(|request| {
                request.program == OsStr::new("/usr/bin/git")
                    && request.args.iter().any(|arg| arg == "config")
                    && request.args.iter().any(|arg| arg == "--get")
                    && request.args.iter().any(|arg| arg == "remote.origin.url")
            })
            .count()
    }
}

impl ProcessRunner for AcceptedThenTerminalRunner {
    fn run(
        &self,
        request: &ProcessRequest,
    ) -> Result<ProcessResult, mac_worker::error::WorkerError> {
        self.requests.lock().unwrap().push(request.clone());

        if request.program == OsStr::new("/usr/bin/git") {
            if request.args.iter().any(|arg| arg == "update-ref")
                && request.args.iter().any(|arg| arg == "-d")
                && request
                    .args
                    .iter()
                    .any(|arg| arg.to_string_lossy().contains("refs/mac-worker/bases/"))
            {
                let mut failures = self.base_release_failures.lock().unwrap();
                if *failures > 0 {
                    *failures -= 1;
                    return Ok(ProcessResult {
                        status: ExitStatus::from_raw(1 << 8),
                        stdout: Vec::new(),
                        stderr: b"simulated base-release failure".to_vec(),
                    });
                }
            }
            if request.args.iter().any(|arg| arg == "ls-remote") {
                let head = SystemProcessRunner.run(&ProcessRequest {
                    program: "/usr/bin/git".into(),
                    args: vec!["rev-parse".into(), "HEAD".into()],
                    environment: Vec::new(),
                    environment_remove: Vec::new(),
                    stdin: None,
                    policy: mac_worker::process::ProcessPolicy {
                        stdout_limit: 64 * 1024,
                        stderr_limit: 64 * 1024,
                        deadline: Duration::from_secs(5),
                    },
                    isolate_parent_environment: false,
                })?;
                return Ok(ProcessResult {
                    status: ExitStatus::from_raw(0),
                    stdout: format!(
                        "{}\trefs/heads/main\n",
                        String::from_utf8_lossy(&head.stdout).trim()
                    )
                    .into_bytes(),
                    stderr: Vec::new(),
                });
            }
            let is_result_fetch = request.args.iter().any(|arg| arg == "fetch")
                && request.args.iter().any(|arg| {
                    arg.to_string_lossy().contains("refs/mac-worker/results/")
                        || arg.to_string_lossy().starts_with("--upload-pack=")
                });
            if is_result_fetch {
                return Ok(ProcessResult {
                    status: ExitStatus::from_raw(0),
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                });
            }
            let is_result_ref_read = request.args.iter().any(|arg| {
                arg.to_string_lossy().contains("refs/mac-worker/results/")
                    || arg.to_string_lossy().contains("refs/remotes/mac-worker/")
            });
            if request.args.iter().any(|arg| arg == "rev-parse") && is_result_ref_read {
                let base_oid = self
                    .task
                    .lock()
                    .unwrap()
                    .as_ref()
                    .expect("result ref read after task preparation")
                    .0
                    .base_oid()
                    .to_string();
                return Ok(ProcessResult {
                    status: ExitStatus::from_raw(0),
                    stdout: format!("{base_oid}\n").into_bytes(),
                    stderr: Vec::new(),
                });
            }
            if request.args.iter().any(|arg| arg == "push")
                && request
                    .args
                    .iter()
                    .any(|arg| arg.to_string_lossy().starts_with("--receive-pack="))
            {
                return Ok(ProcessResult {
                    status: ExitStatus::from_raw(0),
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                });
            }
            return SystemProcessRunner.run(request);
        }

        assert_eq!(request.program, OsStr::new("/usr/bin/ssh"));
        let operation = request
            .args
            .last()
            .and_then(|argument| argument.to_str())
            .unwrap_or_default();
        match operation {
            value if value == HostOperation::RefreshFacts.command() => {
                *self.facts_fresh.lock().unwrap() = true;
                Ok(ProcessResult {
                    status: ExitStatus::from_raw(0),
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                })
            }
            "~/.local/bin/worker host probe" => {
                // The submit path gets one fresh observation to pass initial
                // admission. The runner then observes a stale cache and must
                // refresh facts before claiming the turn.
                let fresh = self.facts_are_fresh_for_probe();
                canonical_process(&ProbeResponse {
                    protocol_version: PROTOCOL_VERSION,
                    supervision_version: SUPERVISION_VERSION,
                    hostname: "mini-1.local".into(),
                    arch: "arm64".into(),
                    os_version: "26.2".into(),
                    free_disk_bytes: 100 * 1024 * 1024 * 1024,
                    total_disk_bytes: 250 * 1024 * 1024 * 1024,
                    memory_pressure: MemoryPressure::Normal,
                    swap_used_bytes: Some(0),
                    available_memory_bytes: Some(12 * 1024 * 1024 * 1024),
                    cpu_counters: Some(CpuCounters {
                        user_ticks: 10,
                        system_ticks: 20,
                        idle_ticks: 30,
                        nice_ticks: 40,
                    }),
                    slot_state: SlotState::Idle,
                    active_lease: None,
                    capabilities: vec!["darwin-arm64".into()],
                    agent_facts: Some(AgentFacts {
                        agents: vec![AgentProbe {
                            name: "codex".into(),
                            version: Some("0.1.0".into()),
                            auth: AgentAuth::Authenticated,
                            auth_by_profile: vec![(
                                "secure".into(),
                                if self.facts_fresh() {
                                    AgentAuth::Authenticated
                                } else {
                                    AgentAuth::Unauthenticated
                                },
                            )],
                        }],
                        env_profiles: vec![ProfileProbe {
                            name: "secure".into(),
                            secure: true,
                        }],
                        git_identity: true,
                        collected_at_millis: if fresh { u64::MAX / 2 } else { 1 },
                        herdr: None,
                    }),
                    facts_age_millis: Some(if fresh { 0 } else { FACTS_TTL + 1 }),
                })
            }
            value if value == HostOperation::LeaseAcquire.command() => {
                let acquire: LeaseAcquireRequest = decode_request(request)?;
                let material = acquire.material();
                let lease = LeaseRecord::new(
                    material,
                    acquire.request_fingerprint().clone(),
                    material.created_at_millis(),
                    material.created_at_millis() + material.timeout_millis(),
                )?;
                canonical_process(&LeaseAcquireResponse::Acquired { lease })
            }
            value if value == HostOperation::TaskPrepare.command() => {
                *self.worker.lock().unwrap() = if request.args.iter().any(|arg| arg == "mac2") {
                    "mini-2"
                } else {
                    "mini-1"
                }
                .into();
                let prepare: TaskPrepareRequest = decode_request(request)?;
                *self.task.lock().unwrap() = Some((prepare.meta().clone(), prepare.job_id()));
                canonical_process(&TaskPrepareResponse::new(
                    prepare.meta().base_oid().clone(),
                    false,
                ))
            }
            value if value == HostOperation::TaskStatus.command() => {
                let status_request: TaskStatusRequest = decode_request(request)?;
                let terminal = *self.turn_submitted.lock().unwrap();
                canonical_process(&TaskStatusResponse::new(
                    self.task_status(status_request.task_id(), terminal)?,
                ))
            }
            value if value == HostOperation::TaskTurn.command() => {
                let turn: TaskTurnRequest = decode_request(request)?;
                let material = turn.submit().material();
                let active = self.task_status(turn.turn().task_id(), false)?;
                *self.turn_submitted.lock().unwrap() = true;
                let job_meta = JobMeta::new(material, material.fingerprint())?;
                let submit = SubmitResponse::Accepted {
                    meta: Box::new(job_meta),
                    status: JobStatus::accepted(material.created_at_millis() + 1)?,
                };
                canonical_process(&TaskTurnResponse::new(submit, active))
            }
            value if value == HostOperation::Status.command() => {
                fixture_terminal_job(&self.requests(), request, 13, 0)
            }
            value if value == HostOperation::LogChunk.command() => {
                let chunk_request: LogChunkRequest = decode_request(request)?;
                let bytes = if chunk_request.stream() == LogStream::Stdout
                    && chunk_request.offset() == 0
                    && !*self.stdout_log_sent.lock().unwrap()
                {
                    *self.stdout_log_sent.lock().unwrap() = true;
                    b"remote event\n".to_vec()
                } else {
                    Vec::new()
                };
                let response = mac_worker::job::LogChunkResponse::new(LogChunk::new(
                    chunk_request.stream(),
                    chunk_request.offset(),
                    bytes,
                )?)?;
                canonical_process(&response)
            }
            value if value == HostOperation::StatusLogs.command() => {
                Ok(clap_unrecognized_subcommand("status-logs"))
            }
            other => panic!("unexpected worker operation: {other}"),
        }
    }
}

#[derive(Default)]
struct FailAfterFirstEvent {
    bytes: Vec<u8>,
    events: usize,
}

impl Write for FailAfterFirstEvent {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.events > 0 {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe));
        }
        self.bytes.extend_from_slice(bytes);
        self.events += bytes.iter().filter(|byte| **byte == b'\n').count();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct FailOnFlush {
    bytes: Vec<u8>,
}

impl Write for FailOnFlush {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::from(io::ErrorKind::BrokenPipe))
    }
}

struct AcceptedThenTerminalFixture {
    _repo: support::GitRepo,
    _current_dir: CurrentDirGuard,
    state_root: tempfile::TempDir,
    paths: mac_worker::paths::PathLayout,
    state: ClientStateStore,
    config: Config,
    runner: AcceptedThenTerminalRunner,
    executor: InlineRunnerExecutor,
    task_id: TaskId,
    turn_id: TurnId,
    reported_runner: Option<RunnerState>,
}

fn submit_request(
    project: &Path,
    origin: Option<&str>,
    preference: WorkerPreference,
    wait_for_capacity: bool,
) -> TaskSubmitRequest {
    TaskSubmitRequest {
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        prompt: "make the change".into(),
        project: project.to_path_buf(),
        base: "main".into(),
        wip: origin.is_none(),
        source: None,
        publish: origin.map(|_| vec!["fetch".into(), "push".into()]),
        publish_branch: None,
        cli_includes: Vec::new(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        preference,
        wait_for_capacity,
        attached: false,
        run_id: None,
    }
}

impl AcceptedThenTerminalFixture {
    fn new() -> Self {
        Self::new_with_push_origin(None)
    }

    fn new_with_push_origin(origin: Option<&str>) -> Self {
        Self::new_with_options(
            origin,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            true,
        )
    }

    fn new_with_options(
        origin: Option<&str>,
        preference: WorkerPreference,
        wait_for_capacity: bool,
    ) -> Self {
        Self::build(
            AcceptedThenTerminalRunner::new(),
            origin,
            preference,
            wait_for_capacity,
            false,
        )
    }

    fn new_origin_source(origin: &str) -> Self {
        Self::build(
            AcceptedThenTerminalRunner::new(),
            Some(origin),
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            true,
            true,
        )
    }

    fn new_with_runner(runner: AcceptedThenTerminalRunner, preference: WorkerPreference) -> Self {
        Self::build(runner, None, preference, false, false)
    }

    fn build(
        runner: AcceptedThenTerminalRunner,
        origin: Option<&str>,
        preference: WorkerPreference,
        wait_for_capacity: bool,
        origin_source: bool,
    ) -> Self {
        let repo = support::GitRepo::init();
        repo.write("base.txt", b"base\n");
        repo.commit_all("base");
        if let Some(origin) = origin {
            repo.git(&["remote", "add", "origin", origin]);
        }
        let state_root = tempfile::tempdir().unwrap();
        let state_root_path = state_root.path().canonicalize().unwrap();
        let paths = support::task_harness::paths(&state_root_path);
        let state = ClientStateStore::open(&paths.state).unwrap();
        let config = Config::parse(
            "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\", \"origin:example.test\"]\n",
        )
        .unwrap();
        let executor = InlineRunnerExecutor;
        let current_dir = CurrentDirGuard::enter(repo.root());
        let mut request = submit_request(repo.root(), origin, preference, wait_for_capacity);
        if origin_source {
            request.source = Some("origin".into());
            request.wip = false;
            request.publish = Some(vec!["fetch".into()]);
        }
        let report = TaskClient::new(&runner, &config, &paths, &state, &executor)
            .submit(request, &mut Vec::new(), &mut Vec::new())
            .unwrap();
        let task_id = report.task_id();
        let reported_runner = report.runner();
        let turn_id = state
            .queue_entry_for_task_turn(task_id)
            .unwrap()
            .unwrap()
            .job_id();
        Self {
            _repo: repo,
            _current_dir: current_dir,
            state_root,
            paths,
            state,
            config,
            runner,
            executor,
            task_id,
            turn_id,
            reported_runner,
        }
    }

    fn run(
        &self,
        follower: &mut dyn Write,
    ) -> Result<mac_worker::turn_runner::TurnOutcomeReport, mac_worker::error::WorkerError> {
        TurnRunner::new(
            &self.runner,
            &self.config,
            &self.paths,
            &self.state,
            &self.executor,
        )
        .run(self.task_id, self.turn_id, Some(follower))
    }

    fn runner_log(&self) -> Vec<u8> {
        fs::read(support::task_harness::runner_log(
            self.state_root.path(),
            &self.task_id.to_string(),
            &self.turn_id.to_string(),
        ))
        .unwrap()
    }
}

#[test]
fn successful_submission_reports_the_handed_off_runner() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();

    assert_eq!(
        fixture.reported_runner,
        fixture.state.runner_liveness(fixture.task_id).unwrap()
    );
    assert_eq!(fixture.reported_runner, Some(RunnerState::Live));
}

#[test]
fn submit_completes_handoff_after_bind_records_the_child() {
    // Break caught: start_runner_with_reservation compared the pre-bind queue
    // row to the bound row, so TaskClient submit always got TASK_BUSY after a
    // successful executor.start and never recorded task.runner. Primitive
    // reserve tests plant a status turn and miss both that comparison and the
    // queued-empty-turns holder after adoption.
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let entry = fixture.state.queue_entry(fixture.turn_id).unwrap().unwrap();
    assert!(
        entry.slot_reservation().is_none(),
        "complete must clear the token after the bound child is recorded"
    );
    let record = fixture.state.load_task(fixture.task_id).unwrap();
    assert_eq!(
        record
            .runner()
            .map(|runner| runner.process_identity().pid()),
        Some(process::id())
    );
    assert_eq!(fixture.reported_runner, Some(RunnerState::Live));
    let second = start_runner_with_reservation(
        &fixture.state,
        &fixture.executor,
        &fixture.paths,
        fixture.task_id,
        fixture.turn_id,
        8,
        false,
    )
    .unwrap();
    assert!(
        matches!(second, RunnerStart::Pending),
        "same-turn live owner must not spawn again after adoption: {second:?}"
    );
}

#[test]
fn runner_drains_both_final_streams_beyond_one_chunk() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let remote = ReplayableLogsRunner::new(&fixture.runner);
    TurnRunner::new(
        &remote,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, None)
    .unwrap();
    let log = fixture.runner_log();
    assert_eq!(log.iter().filter(|byte| **byte == 0xf1).count(), 150_011);
    assert_eq!(log.iter().filter(|byte| **byte == 0xfe).count(), 91_017);
    let reads = remote.reads.lock().unwrap();
    assert!(
        reads.contains(&(LogStream::Stdout, 150_011)),
        "stdout EOF was not confirmed"
    );
    assert!(
        reads.contains(&(LogStream::Stderr, 91_017)),
        "stderr EOF was not confirmed"
    );
}

#[test]
fn runner_reads_logs_when_submit_already_reports_a_terminal_task() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let mut remote = ReplayableLogsRunner::new(&fixture.runner);
    remote.terminal_on_submit = true;
    TurnRunner::new(
        &remote,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, None)
    .unwrap();
    let log = fixture.runner_log();
    assert_eq!(log.iter().filter(|byte| **byte == 0xf1).count(), 150_011);
    assert_eq!(log.iter().filter(|byte| **byte == 0xfe).count(), 91_017);
}

#[test]
fn restarted_runner_resumes_both_logs_without_repeating_a_committed_prefix() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let remote = ReplayableLogsRunner::new(&fixture.runner);
    remote.fail_stderr_once.store(true, Ordering::SeqCst);
    let error = TurnRunner::new(
        &remote,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, None)
    .unwrap_err();
    assert_eq!(error.public_code(), "SSH_LAUNCH_FAILED");
    let prefix = fixture.runner_log();
    assert_eq!(prefix.iter().filter(|byte| **byte == 0xf1).count(), 65_536);
    let reopened = ClientStateStore::open(&fixture.paths.state).unwrap();
    remote.reads.lock().unwrap().clear();
    TurnRunner::new(
        &remote,
        &fixture.config,
        &fixture.paths,
        &reopened,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, None)
    .unwrap();
    let log = fixture.runner_log();
    assert!(
        log.starts_with(&prefix),
        "recovery rewrote already visible bytes"
    );
    assert_eq!(log.iter().filter(|byte| **byte == 0xf1).count(), 150_011);
    assert_eq!(log.iter().filter(|byte| **byte == 0xfe).count(), 91_017);
    let reads = remote.reads.lock().unwrap();
    assert_eq!(
        reads
            .iter()
            .find(|(stream, _)| *stream == LogStream::Stdout),
        Some(&(LogStream::Stdout, 65_536))
    );
    assert_eq!(
        reads
            .iter()
            .find(|(stream, _)| *stream == LogStream::Stderr),
        Some(&(LogStream::Stderr, 0))
    );
}

fn count_host_ops(requests: &[ProcessRequest], command: &str) -> usize {
    requests
        .iter()
        .filter(|request| request.args.last().and_then(|arg| arg.to_str()) == Some(command))
        .count()
}

#[test]
fn combined_follower_drains_a_terminal_tail_without_log_chunk_polls() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let remote = ReplayableLogsRunner::combined(&fixture.runner);
    TurnRunner::new(
        &remote,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, None)
    .unwrap();
    let log = fixture.runner_log();
    assert_eq!(log.iter().filter(|byte| **byte == 0xf1).count(), 150_011);
    assert_eq!(log.iter().filter(|byte| **byte == 0xfe).count(), 91_017);
    let polls = remote.status_logs.lock().unwrap().clone();
    assert_eq!(polls.first(), Some(&(0, 0)));
    assert!(
        polls.contains(&(65_536, 65_536)),
        "combined follower never requested the second chunk pair: {polls:?}"
    );
    assert!(
        polls.contains(&(150_011, 91_017)),
        "combined follower never confirmed both EOFs: {polls:?}"
    );
    assert!(
        remote.reads.lock().unwrap().is_empty(),
        "combined follower must not fall back to log-chunk"
    );
    assert_eq!(polls.len(), 4, "combined polls: {polls:?}");
    assert_eq!(
        *remote.status_calls.lock().unwrap(),
        1,
        "terminal revalidation should be one status RPC after combined drain"
    );
}

#[test]
fn combined_follower_restarts_from_committed_offsets_after_a_late_tail() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let remote = ReplayableLogsRunner::combined(&fixture.runner);
    remote
        .fail_after_first_status_logs
        .store(true, Ordering::SeqCst);
    let error = TurnRunner::new(
        &remote,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, None)
    .unwrap_err();
    assert_eq!(error.public_code(), "SSH_LAUNCH_FAILED");
    let prefix = fixture.runner_log();
    assert_eq!(prefix.iter().filter(|byte| **byte == 0xf1).count(), 65_536);
    assert_eq!(prefix.iter().filter(|byte| **byte == 0xfe).count(), 65_536);
    assert_eq!(
        remote.status_logs.lock().unwrap().as_slice(),
        &[(0, 0), (65_536, 65_536)]
    );
    let reopened = ClientStateStore::open(&fixture.paths.state).unwrap();
    remote.status_logs.lock().unwrap().clear();
    TurnRunner::new(
        &remote,
        &fixture.config,
        &fixture.paths,
        &reopened,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, None)
    .unwrap();
    let log = fixture.runner_log();
    assert!(
        log.starts_with(&prefix),
        "recovery rewrote already visible bytes"
    );
    assert_eq!(log.iter().filter(|byte| **byte == 0xf1).count(), 150_011);
    assert_eq!(log.iter().filter(|byte| **byte == 0xfe).count(), 91_017);
    let resume = remote.status_logs.lock().unwrap().clone();
    assert_eq!(
        resume.first(),
        Some(&(65_536, 65_536)),
        "restart must resume both combined offsets: {resume:?}"
    );
    assert!(
        resume.contains(&(150_011, 91_017)),
        "restart never confirmed both EOFs: {resume:?}"
    );
    assert!(remote.reads.lock().unwrap().is_empty());
    assert_eq!(
        count_host_ops(
            &fixture.runner.requests(),
            HostOperation::LogChunk.command()
        ),
        0
    );
}

#[test]
fn recovery_drains_the_log_tail_when_a_dead_owner_has_terminal_status_and_offsets_behind_available_streams()
 {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let remote = ReplayableLogsRunner::new(&fixture.runner);
    remote.fail_stderr_once.store(true, Ordering::SeqCst);
    let error = TurnRunner::new(
        &remote,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, None)
    .unwrap_err();
    assert_eq!(error.public_code(), "SSH_LAUNCH_FAILED");
    let prefix = fixture.runner_log();
    assert_eq!(prefix.iter().filter(|byte| **byte == 0xf1).count(), 65_536);
    persist_independent_terminal_status(&fixture);
    assert!(fixture_checkpoint(&fixture)["committed"]["completion"].is_null());

    let dead_owner = ProcessIdentity::new(424_242, 4_242_427).unwrap();
    remote.reads.lock().unwrap().clear();
    let (store, report) = adopt_dead_owner_and_reconcile(&fixture, &remote, dead_owner);
    assert_eq!(report.repaired_rows(), 0);
    assert!(store.queue_entry(fixture.turn_id).unwrap().is_some());
    assert!(fixture_checkpoint(&fixture)["committed"]["completion"].is_null());
    assert_eq!(
        fixture
            .runner_log()
            .iter()
            .filter(|byte| **byte == 0xf1)
            .count(),
        65_536
    );

    TurnRunner::new(
        &remote,
        &fixture.config,
        &fixture.paths,
        &store,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, None)
    .unwrap();

    let log = fixture.runner_log();
    assert!(
        log.starts_with(&prefix),
        "recovery rewrote already visible bytes"
    );
    assert_eq!(log.iter().filter(|byte| **byte == 0xf1).count(), 150_011);
    assert_eq!(log.iter().filter(|byte| **byte == 0xfe).count(), 91_017);
    let reads = remote.reads.lock().unwrap();
    assert_eq!(
        reads
            .iter()
            .find(|(stream, _)| *stream == LogStream::Stdout),
        Some(&(LogStream::Stdout, 65_536))
    );
    assert_eq!(
        reads
            .iter()
            .find(|(stream, _)| *stream == LogStream::Stderr),
        Some(&(LogStream::Stderr, 0))
    );
    let checkpoint = fixture_checkpoint(&fixture);
    assert_eq!(checkpoint["committed"]["completion"]["drained"], true);
    assert!(
        store
            .queue_entry_for_task_turn(fixture.task_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn recovery_imports_the_result_after_a_crash_between_terminal_persist_and_fetch() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let remote = ReplayableLogsRunner::new(&fixture.runner);
    remote.fail_stderr_once.store(true, Ordering::SeqCst);
    TurnRunner::new(
        &remote,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, None)
    .unwrap_err();
    assert_eq!(result_fetch_count(&fixture.runner), 0);
    persist_independent_terminal_status(&fixture);

    let prefix = fixture.runner_log();
    let stdout_have = prefix.iter().filter(|byte| **byte == 0xf1).count();
    assert_eq!(stdout_have, 65_536);
    let mut extra = vec![0xf1; 150_011 - stdout_have];
    extra.extend(vec![0xfe; 91_017]);
    let log_path = support::task_harness::runner_log(
        fixture.state_root.path(),
        &fixture.task_id.to_string(),
        &fixture.turn_id.to_string(),
    );
    fs::OpenOptions::new()
        .append(true)
        .open(&log_path)
        .unwrap()
        .write_all(&extra)
        .unwrap();
    let mut checkpoint = fixture_checkpoint(&fixture);
    checkpoint["committed"]["offsets"] = serde_json::json!([150_011, 91_017]);
    checkpoint["committed"]["len"] = serde_json::json!(prefix.len() + extra.len());
    checkpoint["committed"]["accepted"] = serde_json::json!(true);
    checkpoint["committed"]["completion"] = serde_json::Value::Null;
    write_fixture_checkpoint(&fixture, &checkpoint);

    let earlier = BaseOid::from_str("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    let imported = fixture
        .state
        .load_task(fixture.task_id)
        .unwrap()
        .meta()
        .base_oid()
        .clone();
    assert_ne!(earlier, imported);
    let record = fixture.state.load_task(fixture.task_id).unwrap();
    fixture
        .state
        .update_task(record.with_fetched_head(Some(earlier.clone())).unwrap())
        .unwrap();
    let transfer = transfer_for_fixture(&fixture);
    assert!(transfer.has_ref(&base_pin_name(&fixture)));
    assert_eq!(
        fixture
            .state
            .load_task(fixture.task_id)
            .unwrap()
            .fetched_head(),
        Some(&earlier)
    );

    let dead_owner = ProcessIdentity::new(424_243, 4_242_437).unwrap();
    let (store, report) = adopt_dead_owner_and_reconcile(&fixture, &remote, dead_owner);
    assert_eq!(report.repaired_rows(), 0);
    assert!(store.queue_entry(fixture.turn_id).unwrap().is_some());
    assert!(fixture_checkpoint(&fixture)["committed"]["completion"].is_null());
    assert!(transfer_for_fixture(&fixture).has_ref(&base_pin_name(&fixture)));

    TurnRunner::new(
        &remote,
        &fixture.config,
        &fixture.paths,
        &store,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, None)
    .unwrap();

    assert!(result_fetch_count(&fixture.runner) >= 1);
    let recovered = store.load_task(fixture.task_id).unwrap();
    assert_eq!(recovered.fetched_head(), Some(&imported));
    assert_ne!(recovered.fetched_head(), Some(&earlier));
    assert!(
        store
            .queue_entry_for_task_turn(fixture.task_id)
            .unwrap()
            .is_none()
    );
    assert!(!transfer_for_fixture(&fixture).has_ref(&base_pin_name(&fixture)));
    let checkpoint = fixture_checkpoint(&fixture);
    assert_eq!(checkpoint["committed"]["completion"]["drained"], true);
    assert_eq!(
        checkpoint["committed"]["completion"]["outcome"]["kind"],
        "done"
    );
}

#[test]
fn reconcile_retires_a_dead_dispatching_row_whose_journal_already_proves_completion() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    fixture
        .state
        .open_runner_log(fixture.task_id, fixture.turn_id)
        .unwrap();
    write_fixture_checkpoint(
        &fixture,
        &serde_json::json!({
            "version": 1,
            "task_id": fixture.task_id,
            "turn_id": fixture.turn_id,
            "committed": {
                "offsets": [0, 0],
                "len": 0,
                "accepted": true,
                "completion": {"outcome": {"kind": "done"}, "drained": true}
            },
            "pending": null
        }),
    );
    persist_local_open_success(&fixture);
    assert!(transfer_for_fixture(&fixture).has_ref(&base_pin_name(&fixture)));

    let dead_owner = ProcessIdentity::new(424_244, 4_242_447).unwrap();
    fixture.state.record_runner(fixture.task_id, None).unwrap();
    let live_owner = fixture
        .state
        .queue_entry(fixture.turn_id)
        .unwrap()
        .unwrap()
        .owner_opt()
        .copied()
        .expect("submitted turn has an owner");
    fixture
        .state
        .claim_next(live_owner, &["mini-1".into()], u64::MAX / 4)
        .unwrap()
        .expect("waiting turn must become dispatching before orphan repair");
    let (store, report) = adopt_dead_owner_and_reconcile(&fixture, &fixture.runner, dead_owner);
    assert_eq!(report.repaired_rows(), 1);
    assert_eq!(report.started_runners(), 0);
    assert!(
        store
            .queue_entry_for_task_turn(fixture.task_id)
            .unwrap()
            .is_none()
    );
    assert!(!transfer_for_fixture(&fixture).has_ref(&base_pin_name(&fixture)));
}

#[test]
fn recovery_records_log_drain_unavailable_when_the_worker_job_is_gone() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let inner = ReplayableLogsRunner::new(&fixture.runner);
    inner.fail_stderr_once.store(true, Ordering::SeqCst);
    TurnRunner::new(
        &inner,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, None)
    .unwrap_err();
    persist_independent_terminal_status(&fixture);
    assert_eq!(
        fixture
            .state
            .load_task(fixture.task_id)
            .unwrap()
            .status()
            .last_outcome(),
        Some(&TaskOutcome::Done)
    );

    let remote = MissingJobLogsRunner { inner };
    let dead_owner = ProcessIdentity::new(424_245, 4_242_457).unwrap();
    let (store, report) = adopt_dead_owner_and_reconcile(&fixture, &remote, dead_owner);
    assert_eq!(report.repaired_rows(), 0);
    assert!(fixture_checkpoint(&fixture)["committed"]["completion"].is_null());

    let error = TurnRunner::new(
        &remote,
        &fixture.config,
        &fixture.paths,
        &store,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, None)
    .unwrap_err();
    assert_eq!(error.public_code(), "LOG_DRAIN_UNAVAILABLE", "{error}");

    let checkpoint = fixture_checkpoint(&fixture);
    assert_eq!(checkpoint["committed"]["accepted"], true);
    assert_eq!(checkpoint["committed"]["completion"]["drained"], false);
    assert_eq!(
        checkpoint["committed"]["completion"]["outcome"]["kind"],
        "failed"
    );
    assert_eq!(
        checkpoint["committed"]["completion"]["outcome"]["reason"],
        "LOG_DRAIN_UNAVAILABLE"
    );
    let log_bytes = fixture.runner_log();
    let log = String::from_utf8_lossy(&log_bytes);
    assert!(
        !log.contains("\"type\":\"turn_terminal\""),
        "undrainable journal claimed turn_terminal"
    );
    assert!(
        log.contains("exited after acceptance: LOG_DRAIN_UNAVAILABLE"),
        "undrainable journal missing drain diagnostic"
    );
    let record = store.load_task(fixture.task_id).unwrap();
    assert_eq!(
        record.status().last_outcome(),
        Some(&TaskOutcome::failed("LOG_DRAIN_UNAVAILABLE"))
    );
    assert_eq!(record.status().state(), TaskState::Open);
    assert_eq!(record.abandon_code(), Some("LOG_DRAIN_UNAVAILABLE"));
    assert!(record.fetched_head().is_some());
    assert!(
        store
            .queue_entry_for_task_turn(fixture.task_id)
            .unwrap()
            .is_none()
    );
    assert_eq!(store.runner_liveness(fixture.task_id).unwrap(), None);
    assert!(!transfer_for_fixture(&fixture).has_ref(&base_pin_name(&fixture)));

    let client = TaskClient::new(
        &remote,
        &fixture.config,
        &fixture.paths,
        &store,
        &fixture.executor,
    );
    let waited = client
        .wait(
            WaitSelector::Task(fixture.task_id),
            Some(Duration::from_secs(2)),
        )
        .unwrap();
    assert_eq!(waited.exit_code(), 1);

    let mut follow = BoundedFollowWriter::default();
    client
        .logs(
            fixture.task_id,
            None,
            true,
            true,
            &mut follow,
            &mut Vec::new(),
        )
        .unwrap();
    assert!(follow.polls <= 5);
    let followed = String::from_utf8_lossy(&follow.bytes);
    assert!(
        followed.contains("exited after acceptance: LOG_DRAIN_UNAVAILABLE"),
        "followed log missing drain diagnostic"
    );
    assert!(
        !followed.contains("\"type\":\"turn_terminal\""),
        "followed log claimed turn_terminal"
    );

    let after = client.reconcile_runners().unwrap();
    assert_eq!(after.started_runners(), 0);
    let record = store.load_task(fixture.task_id).unwrap();
    assert_eq!(
        record.status().last_outcome(),
        Some(&TaskOutcome::failed("LOG_DRAIN_UNAVAILABLE"))
    );
    assert_eq!(record.status().state(), TaskState::Open);
    assert_eq!(record.abandon_code(), Some("LOG_DRAIN_UNAVAILABLE"));
    drop(client);

    let host_success = fixture.runner.task_status(fixture.task_id, true).unwrap();
    let store = std::sync::Arc::new(store);
    let config = std::sync::Arc::new(fixture.config.clone());
    let dashboard_remote = std::sync::Arc::new(HostReportsSuccessReader {
        status: host_success,
    });
    let tasks = MacWorkerTaskSource::new(
        std::sync::Arc::clone(&config),
        std::sync::Arc::clone(&store),
        std::sync::Arc::clone(&dashboard_remote)
            as std::sync::Arc<dyn mac_worker::dashboard::source::DashboardRemoteReader>,
    );
    let detail = tasks.task_detail(fixture.task_id).unwrap();
    assert_eq!(detail.task.state, TaskState::Open);
    assert_eq!(
        detail.task.last_outcome,
        Some(TaskOutcome::failed("LOG_DRAIN_UNAVAILABLE"))
    );
    let log_error = tasks
        .read_task_log(fixture.task_id, fixture.turn_id, LogStream::Stdout, 0, 64)
        .unwrap_err();
    assert_eq!(log_error.code, "LOG_DRAIN_UNAVAILABLE");

    let snapshot = DashboardService::new(
        MacWorkerDashboardSource::new(
            std::sync::Arc::clone(&config),
            std::sync::Arc::new(IdleDashboardWorkers),
            std::sync::Arc::clone(&store),
            dashboard_remote
                as std::sync::Arc<dyn mac_worker::dashboard::source::DashboardRemoteReader>,
        ),
        SystemClock,
        SystemMonotonicClock::new(),
    )
    .snapshot(Default::default())
    .unwrap();
    let row = snapshot
        .task_view
        .tasks
        .iter()
        .find(|row| row.task_id == fixture.task_id)
        .expect("undrainable task is present in the dashboard snapshot");
    assert_eq!(row.state, TaskState::Open);
    assert_eq!(
        row.last_outcome,
        Some(TaskOutcome::failed("LOG_DRAIN_UNAVAILABLE"))
    );
}

#[test]
fn reconcile_repairs_an_undrainable_journal_left_ahead_of_the_local_record() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    plant_undrainable_journal_ahead_of_success_record(&fixture);
    claim_waiting_turn(&fixture);
    assert_eq!(
        fixture
            .state
            .load_task(fixture.task_id)
            .unwrap()
            .status()
            .last_outcome(),
        Some(&TaskOutcome::Done)
    );
    assert!(transfer_for_fixture(&fixture).has_ref(&base_pin_name(&fixture)));

    let host_success = fixture
        .state
        .load_task(fixture.task_id)
        .unwrap()
        .status()
        .clone();
    let remote = RejectResultFetch::new(&fixture.runner);
    let dead_owner = ProcessIdentity::new(424_246, 4_242_467).unwrap();
    let (store, report) = adopt_dead_owner_and_reconcile(&fixture, &remote, dead_owner);
    assert_eq!(report.repaired_rows(), 1);
    assert_eq!(report.started_runners(), 0);
    assert_undrainable_publication_kept_the_base_pin(&store, &fixture);
    assert_wait_and_dashboard_keep_undrainable_failure(&remote, store, &fixture, host_success);
}

#[test]
fn runner_repairs_an_undrainable_journal_left_ahead_of_the_local_record() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    plant_undrainable_journal_ahead_of_success_record(&fixture);
    claim_waiting_turn(&fixture);
    let host_success = fixture
        .state
        .load_task(fixture.task_id)
        .unwrap()
        .status()
        .clone();
    let remote = RejectResultFetch::new(&fixture.runner);
    let report = TurnRunner::new(
        &remote,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, None)
    .unwrap();
    assert_eq!(
        report.status().last_outcome(),
        Some(&TaskOutcome::failed("LOG_DRAIN_UNAVAILABLE"))
    );
    assert_eq!(report.status().state(), TaskState::Open);
    assert_eq!(report.exit_code(), 1);
    assert_undrainable_publication_kept_the_base_pin(&fixture.state, &fixture);
    assert_wait_and_dashboard_keep_undrainable_failure(
        &remote,
        fixture.state.clone(),
        &fixture,
        host_success,
    );
}

#[test]
fn undrainable_record_write_failure_after_journal_finish_is_recovered() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let inner = ReplayableLogsRunner::new(&fixture.runner);
    inner.fail_stderr_once.store(true, Ordering::SeqCst);
    TurnRunner::new(
        &inner,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, None)
    .unwrap_err();
    persist_independent_terminal_status(&fixture);
    assert_eq!(
        fixture
            .state
            .load_task(fixture.task_id)
            .unwrap()
            .status()
            .last_outcome(),
        Some(&TaskOutcome::Done)
    );
    assert!(transfer_for_fixture(&fixture).has_ref(&base_pin_name(&fixture)));

    let missing = MissingJobLogsRunner { inner };
    let remote = RejectResultFetch::new(&missing);
    let host_success = fixture.runner.task_status(fixture.task_id, true).unwrap();
    fixture
        .state
        .inject_write_failure_once(ClientStateWritePoint::BeforeUndrainableRecordUpdate);
    let error = TurnRunner::new(
        &remote,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, None)
    .unwrap_err();
    assert_eq!(error.public_code(), "IO", "{error}");
    let checkpoint = fixture_checkpoint(&fixture);
    assert_eq!(checkpoint["committed"]["completion"]["drained"], false);
    assert_eq!(
        checkpoint["committed"]["completion"]["outcome"]["reason"],
        "LOG_DRAIN_UNAVAILABLE"
    );
    let record = fixture.state.load_task(fixture.task_id).unwrap();
    assert_eq!(record.status().last_outcome(), Some(&TaskOutcome::Done));
    assert_eq!(record.abandon_code(), None);
    assert!(record.fetched_head().is_none());
    assert!(transfer_for_fixture(&fixture).has_ref(&base_pin_name(&fixture)));

    let dead_owner = ProcessIdentity::new(424_247, 4_242_477).unwrap();
    let (store, report) = adopt_dead_owner_and_reconcile(&fixture, &remote, dead_owner);
    assert_eq!(report.repaired_rows(), 1);
    assert_undrainable_publication_kept_the_base_pin(&store, &fixture);
    assert_wait_and_dashboard_keep_undrainable_failure(&remote, store, &fixture, host_success);
}

#[test]
fn undrainable_follow_up_still_imports_when_prior_fetched_head_is_present() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let earlier = plant_undrainable_follow_up_with_prior_fetched_head(&fixture);
    claim_waiting_turn(&fixture);
    let planted = fixture.state.load_task(fixture.task_id).unwrap();
    assert_eq!(planted.fetched_head(), Some(&earlier));
    assert_eq!(planted.status().turns().len(), 2);
    assert_eq!(planted.status().last_outcome(), Some(&TaskOutcome::Done));
    assert!(transfer_for_fixture(&fixture).has_ref(&base_pin_name(&fixture)));
    let host_success = planted.status().clone();

    let remote = RejectResultFetch::new(&fixture.runner);
    let dead_owner = ProcessIdentity::new(424_248, 4_242_487).unwrap();
    let (store, report) = adopt_dead_owner_and_reconcile(&fixture, &remote, dead_owner);
    assert_eq!(report.repaired_rows(), 1);
    assert!(remote.result_fetch_count() >= 1);

    let record = store.load_task(fixture.task_id).unwrap();
    assert_eq!(record.fetched_head(), Some(&earlier));
    assert_eq!(record.status().state(), TaskState::Open);
    assert_eq!(
        record.status().last_outcome(),
        Some(&TaskOutcome::failed("LOG_DRAIN_UNAVAILABLE"))
    );
    assert_eq!(record.abandon_code(), Some("LOG_DRAIN_UNAVAILABLE"));
    let turns = record.status().turns();
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0].terminal(), Some(TurnTerminal::Succeeded));
    assert_eq!(turns[0].outcome(), Some(&TaskOutcome::Done));
    assert_eq!(turns[1].turn_id(), fixture.turn_id);
    assert_eq!(turns[1].terminal(), Some(TurnTerminal::Failed));
    assert_eq!(
        turns[1].outcome(),
        Some(&TaskOutcome::failed("LOG_DRAIN_UNAVAILABLE"))
    );
    assert!(
        store
            .queue_entry_for_task_turn(fixture.task_id)
            .unwrap()
            .is_none()
    );
    assert!(transfer_for_fixture(&fixture).has_ref(&base_pin_name(&fixture)));
    assert_wait_and_dashboard_keep_undrainable_failure(&remote, store, &fixture, host_success);
}

fn assert_remote_turn_completed(