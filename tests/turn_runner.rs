#[allow(dead_code)]
mod support;

use std::{
    ffi::OsStr,
    fs,
    io::{self, Write},
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    path::{Path, PathBuf},
    process::{self, ExitStatus},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
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
    config::Config,
    error::WorkerError,
    job::{
        AdmissionObservation, CommandSummary, JobMeta, JobState, JobStatus, LeaseAcquireRequest,
        LeaseAcquireResponse, LeaseRecord, LogChunk, LogChunkRequest, LogStream, ProcessIdentity,
        QueueEntry, QueueEntryKind, StatusRequest, StatusResponse, SubmitResponse,
    },
    lease::SlotState,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    protocol::{CpuCounters, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION},
    scheduler::WorkerPreference,
    supervisor::{
        ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
    },
    task::{
        ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, RunId, RunRecord, RunnerState,
        TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState,
        TaskStatus, TurnId, TurnSummary, TurnTerminal,
    },
    task_client::{TaskClient, TaskSubmitRequest, WaitSelector},
    task_store::{
        TaskCloseRequest, TaskCloseResponse, TaskPrepareRequest, TaskPrepareResponse,
        TaskStatusRequest, TaskStatusResponse,
    },
    transfer::HostOperation,
    transfer_repo::TransferRepo,
    turn::{TaskTurnRequest, TaskTurnResponse},
    turn_runner::{DetachedRunnerExecutor, InlineRunnerExecutor, RunnerExecutor, TurnRunner},
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
    fail_stderr_once: AtomicBool,
    terminal_on_submit: bool,
}

impl<'a> ReplayableLogsRunner<'a> {
    fn new(inner: &'a AcceptedThenTerminalRunner) -> Self {
        Self {
            inner,
            logs: [vec![0xf1; 150_011], vec![0xfe; 91_017]],
            meta: Mutex::new(None),
            reads: Mutex::new(Vec::new()),
            fail_stderr_once: AtomicBool::new(false),
            terminal_on_submit: false,
        }
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
            return canonical_process(&StatusResponse::new(
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
        )
    }

    fn new_with_runner(runner: AcceptedThenTerminalRunner, preference: WorkerPreference) -> Self {
        Self::build(runner, None, preference, false)
    }

    fn build(
        runner: AcceptedThenTerminalRunner,
        origin: Option<&str>,
        preference: WorkerPreference,
        wait_for_capacity: bool,
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
        let report = TaskClient::new(&runner, &config, &paths, &state, &executor)
            .submit(
                submit_request(repo.root(), origin, preference, wait_for_capacity),
                &mut Vec::new(),
                &mut Vec::new(),
            )
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

fn assert_remote_turn_completed(
    fixture: &AcceptedThenTerminalFixture,
    outcome: &mac_worker::turn_runner::TurnOutcomeReport,
) {
    assert_eq!(outcome.status().state(), TaskState::Closed);
    assert_eq!(outcome.status().last_outcome(), Some(&TaskOutcome::Done));
    assert_eq!(outcome.exit_code(), 0);
    assert!(outcome.status().turns().last().is_some_and(|turn| {
        turn.terminal() == Some(TurnTerminal::Succeeded)
            && turn.outcome() == Some(&TaskOutcome::Done)
    }));
    assert!(fixture.runner.requests().iter().any(|request| {
        request
            .args
            .iter()
            .any(|arg| arg == HostOperation::TaskTurn.command())
    }));
    assert!(fixture.runner.requests().iter().any(|request| {
        request
            .args
            .iter()
            .any(|arg| arg == HostOperation::LogChunk.command())
    }));
    assert!(
        fixture
            .state
            .queue_entry_for_task_turn(fixture.task_id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fixture.state.runner_liveness(fixture.task_id).unwrap(),
        None
    );

    let log = String::from_utf8(fixture.runner_log()).unwrap();
    assert!(
        log.contains("\"type\":\"turn_accepted\""),
        "runner log: {log}"
    );
    assert!(log.contains("remote event\n"), "runner log: {log}");
    assert!(
        log.contains("\"type\":\"turn_terminal\""),
        "runner log: {log}"
    );
    assert!(!log.contains("\"kind\":\"lost\""), "runner log: {log}");
}

#[test]
fn follower_write_error_detaches_runner_and_completes_remote_turn() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let mut follower = FailAfterFirstEvent::default();

    let outcome = fixture.run(&mut follower).unwrap();

    assert_eq!(follower.events, 1);
    assert_remote_turn_completed(&fixture, &outcome);
}

#[test]
fn runner_refreshes_stale_agent_facts_before_claiming() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    // Query time is captured before the turn. This test proves the runner
    // published refreshed capabilities, not that the whole Git turn finished
    // inside the 2s admission TTL. cached_admission clamps now to observed_at,
    // so a slightly early query still reads the published record at age 0.
    // Submit's first probe is TTL-fresh but still Unauthenticated on the
    // secure profile, so it cannot emit agent:codex@secure; only the
    // post-refresh probe (facts_fresh) publishes that capability.
    let query_now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let worker = &fixture.config.workers[0];
    fixture
        .state
        .publish_admission_observation(
            AdmissionObservation::new(
                worker.name.clone(),
                true,
                mac_worker::scheduler::CandidateSlot::Idle,
                vec!["darwin-arm64".into()],
                Some(12 * 1024 * 1024 * 1024),
                100 * 1024 * 1024 * 1024,
                query_now,
            )
            .unwrap()
            .with_local_binding(
                worker.ssh.clone(),
                worker.remote_binary.clone(),
                worker.capabilities.clone(),
                worker.slots,
                Some(FACTS_TTL + 1),
                query_now,
            ),
        )
        .unwrap();
    let outcome = fixture.run(&mut Vec::new()).unwrap();

    assert_eq!(outcome.status().state(), TaskState::Closed);
    assert!(fixture.runner.facts_fresh());
    assert!(fixture.runner.requests().iter().any(|request| {
        request
            .args
            .iter()
            .any(|arg| arg == HostOperation::RefreshFacts.command())
    }));
    let cached = fixture
        .state
        .admission_observation("mini-1", query_now, || {
            Err(mac_worker::error::WorkerError::Protocol(
                "refreshed facts were not cached".into(),
            ))
        })
        .unwrap();
    assert!(
        cached
            .observation()
            .capabilities()
            .iter()
            .any(|capability| capability == "agent:codex@secure")
    );
}

#[derive(Clone, Copy)]
enum AdmissionFailure {
    Probe,
    ProbeOnce,
    Refresh,
}

struct AdmissionFailureRunner<'a> {
    inner: &'a AcceptedThenTerminalRunner,
    ssh: &'static str,
    failure: AdmissionFailure,
    failed_worker_requests: Mutex<Vec<ProcessRequest>>,
}

impl ProcessRunner for AdmissionFailureRunner<'_> {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program != OsStr::new("/usr/bin/ssh")
            || !request.args.iter().any(|arg| arg == self.ssh)
        {
            return self.inner.run(request);
        }
        let retried = {
            let mut requests = self.failed_worker_requests.lock().unwrap();
            let retried = !requests.is_empty();
            requests.push(request.clone());
            retried
        };
        if matches!(self.failure, AdmissionFailure::ProbeOnce) && retried {
            return self.inner.run(request);
        }
        let operation = request.args.last().unwrap();
        if matches!(self.failure, AdmissionFailure::Refresh)
            && operation == "~/.local/bin/worker host probe"
        {
            let response = self.inner.run(request)?;
            let mut probe: ProbeResponse = serde_json::from_slice(&response.stdout).unwrap();
            probe.agent_facts.as_mut().unwrap().collected_at_millis = 1;
            probe.facts_age_millis = Some(FACTS_TTL + 1);
            return canonical_process(&probe);
        }
        Ok(ProcessResult {
            status: ExitStatus::from_raw(255 << 8),
            stdout: Vec::new(),
            stderr: b"simulated worker connection failure".to_vec(),
        })
    }
}

fn add_second_worker(fixture: &mut AcceptedThenTerminalFixture, first: bool) {
    let mut worker = fixture.config.workers[0].clone();
    worker.name = "mini-2".into();
    worker.ssh = "mac2".into();
    fixture.config.workers.insert(usize::from(!first), worker);
}

fn plant_bound_mini1_ready(state: &ClientStateStore, capabilities: Vec<String>) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    state
        .publish_admission_observation(
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                mac_worker::scheduler::CandidateSlot::Idle,
                capabilities,
                Some(8 * 1024 * 1024 * 1024),
                64 * 1024 * 1024 * 1024,
                now,
            )
            .unwrap()
            .with_local_binding(
                "mac1".into(),
                "~/.local/bin/worker".into(),
                vec!["darwin-arm64".into()],
                1,
                Some(0),
                now,
            ),
        )
        .unwrap();
}

#[test]
fn detached_runner_starts_on_its_ready_pin_without_probing_other_workers() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let mut fixture = AcceptedThenTerminalFixture::new();
    add_second_worker(&mut fixture, false);
    let runner = AdmissionFailureRunner {
        inner: &fixture.runner,
        ssh: "mac2",
        failure: AdmissionFailure::Probe,
        failed_worker_requests: Mutex::new(Vec::new()),
    };
    let outcome = TurnRunner::new(
        &runner,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .run_detached(fixture.task_id, fixture.turn_id)
    .unwrap();
    assert_remote_turn_completed(&fixture, &outcome);
    assert!(
        runner.failed_worker_requests.lock().unwrap().is_empty(),
        "a ready pinned turn must not wait on unrelated SSH probes"
    );
}

struct FixedTaskExecutor(ProcessIdentity);
impl RunnerExecutor for FixedTaskExecutor {
    fn start(
        &self,
        _: &mac_worker::paths::PathLayout,
        _: TaskId,
        _: TurnId,
    ) -> Result<mac_worker::task::RunnerIdentity, WorkerError> {
        Ok(mac_worker::task::RunnerIdentity::new(self.0))
    }
}

#[derive(Default)]
struct CountedInlineExecutor(Mutex<Vec<(TaskId, TurnId)>>);
impl RunnerExecutor for CountedInlineExecutor {
    fn start(
        &self,
        paths: &mac_worker::paths::PathLayout,
        task: TaskId,
        turn: TurnId,
    ) -> Result<mac_worker::task::RunnerIdentity, WorkerError> {
        self.0.lock().unwrap().push((task, turn));
        InlineRunnerExecutor.start(paths, task, turn)
    }
}

struct BusyPinnedRunner<'a> {
    fixture: &'a AcceptedThenTerminalFixture,
    donor: TurnId,
    probes: Mutex<u32>,
}

impl ProcessRunner for BusyPinnedRunner<'_> {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let mut result = self.fixture.runner.run(request)?;
        if request.program == OsStr::new("/usr/bin/ssh")
            && request.args.iter().any(|arg| arg == "mac1")
            && request
                .args
                .last()
                .is_some_and(|arg| arg == "~/.local/bin/worker host probe")
        {
            let mut count = self.probes.lock().unwrap();
            *count += 1;
            if *count > 1 {
                // Bound the regression: a runner that keeps waiting on its
                // pin is cancelled on its next polling pass, then exits.
                self.fixture
                    .state
                    .request_queue_cancel(self.donor, u64::MAX / 2)?;
            }
            let mut probe: ProbeResponse = serde_json::from_slice(&result.stdout).unwrap();
            probe.slot_state = SlotState::Busy;
            result = canonical_process(&probe)?;
        }
        Ok(result)
    }
}

fn submit_pool_task(
    fixture: &AcceptedThenTerminalFixture,
    project: &Path,
    worker: &str,
    executor: &dyn RunnerExecutor,
) -> (TaskId, TurnId) {
    let report = TaskClient::new(
        &fixture.runner,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        executor,
    )
    .submit(
        TaskSubmitRequest {
            agent: AgentKind::Codex,
            model: None,
            effort: None,
            prompt: "recipient private prompt".into(),
            project: project.to_path_buf(),
            base: "main".into(),
            wip: true,
            source: Some("local".into()),
            publish: Some(vec!["fetch".into()]),
            publish_branch: None,
            cli_includes: vec![],
            limits: TaskLimits::default(),
            close_policy: ClosePolicy::Never,
            env_profile: None,
            preference: WorkerPreference::Pinned {
                worker: worker.into(),
            },
            wait_for_capacity: true,
            attached: false,
            run_id: None,
        },
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .unwrap();
    (
        report.task_id(),
        fixture
            .state
            .queue_entry_for_task_turn(report.task_id())
            .unwrap()
            .unwrap()
            .job_id(),
    )
}

fn saturated_pool(
    fixture: &mut AcceptedThenTerminalFixture,
    recipient_project: &Path,
) -> ((TaskId, TurnId), (TaskId, TurnId)) {
    *fixture.runner.facts_fresh.lock().unwrap() = true;
    add_second_worker(fixture, false);
    let mut third = fixture.config.workers[0].clone();
    third.name = "mini-3".into();
    third.ssh = "mac3".into();
    fixture.config.workers.push(third);
    fixture.state = ClientStateStore::open_with_owner_inspector(
        &fixture.paths.state,
        DeadOwnerInspector {
            dead_owner: ProcessIdentity::new(999_999, 1).unwrap(),
        },
    )
    .unwrap();
    // An already reserved mini-1 turn and two separate pinned runners fill
    // the process budget. The queued mini-2 task must therefore be parked.
    let active_owner = ProcessIdentity::new(424_241, 1).unwrap();
    fixture
        .state
        .adopt_row(fixture.turn_id, active_owner)
        .unwrap();
    fixture
        .state
        .record_runner(
            fixture.task_id,
            Some(mac_worker::task::RunnerIdentity::new(active_owner)),
        )
        .unwrap();
    fixture
        .state
        .claim_next(active_owner, &["mini-1".into()], u64::MAX / 4)
        .unwrap()
        .unwrap();
    let donor = submit_pool_task(
        fixture,
        fixture._repo.root(),
        "mini-1",
        &InlineRunnerExecutor,
    );
    submit_pool_task(
        fixture,
        fixture._repo.root(),
        "mini-1",
        &FixedTaskExecutor(ProcessIdentity::new(424_242, 2).unwrap()),
    );
    let recipient = submit_pool_task(fixture, recipient_project, "mini-2", &InlineRunnerExecutor);
    assert!(matches!(
        fixture
            .state
            .queue_entry(recipient.1)
            .unwrap()
            .unwrap()
            .state(),
        mac_worker::job::QueueState::Parked
    ));
    *fixture.runner.facts_fresh.lock().unwrap() = true;
    (donor, recipient)
}

#[test]
fn detached_waiter_executes_parked_turn_in_its_saved_project_without_an_extra_process() {
    // Break caught: all process slots wait on mini-1 while mini-2 is idle;
    // task execution/result import also used the donor's inherited cwd.
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    for linked_worktree in [false, true] {
        let mut fixture = AcceptedThenTerminalFixture::new();
        let other_repo = support::GitRepo::init();
        other_repo.write("recipient.txt", b"recipient repository\n");
        other_repo.commit_all("recipient base");
        let recipient_project = if linked_worktree {
            let path = fixture.state_root.path().join("linked-recipient");
            assert!(
                fixture
                    ._repo
                    .git(&[
                        "worktree",
                        "add",
                        "-b",
                        "recipient",
                        path.to_str().unwrap(),
                        "main"
                    ])
                    .status
                    .success()
            );
            path
        } else {
            other_repo.root().to_path_buf()
        };
        let (donor, recipient) = saturated_pool(&mut fixture, &recipient_project);
        let donor_owner = fixture
            .state
            .load_task(donor.0)
            .unwrap()
            .runner()
            .unwrap()
            .process_identity();
        let original = fixture.state.queue_entry(donor.1).unwrap().unwrap();
        let original_prompt = fixture.state.read_turn_prompt(donor.0, donor.1).unwrap();
        let runner = BusyPinnedRunner {
            fixture: &fixture,
            donor: donor.1,
            probes: Mutex::new(0),
        };
        let executor = CountedInlineExecutor::default();
        let outcome = TurnRunner::new(
            &runner,
            &fixture.config,
            &fixture.paths,
            &fixture.state,
            &executor,
        )
        .run_detached(donor.0, donor.1)
        .unwrap();
        assert_eq!(outcome.status().state(), TaskState::Closed);
        assert_eq!(outcome.status().worker(), Some("mini-2"));
        assert_eq!(outcome.status().turns()[0].turn_id(), recipient.1);
        assert_eq!(
            fixture
                .state
                .load_task(recipient.0)
                .unwrap()
                .status()
                .state(),
            TaskState::Closed
        );
        assert_eq!(
            fixture.state.load_task(donor.0).unwrap().status().state(),
            TaskState::Queued
        );
        let resumed = fixture.state.queue_entry(donor.1).unwrap().unwrap();
        assert_eq!(resumed.queue_id(), original.queue_id());
        assert_eq!(resumed.preference(), original.preference());
        assert_eq!(
            fixture.state.read_turn_prompt(donor.0, donor.1).unwrap(),
            original_prompt
        );
        assert_eq!(resumed.owner_opt(), Some(&donor_owner));
        // The only spawn happens after terminal completion to resume the
        // displaced donor. Yielding itself never starts another process.
        assert_eq!(*executor.0.lock().unwrap(), vec![donor]);
        let requests = fixture.runner.requests();
        let submissions = requests
            .iter()
            .filter(|request| {
                request
                    .args
                    .last()
                    .is_some_and(|arg| arg == HostOperation::TaskTurn.command())
            })
            .collect::<Vec<_>>();
        assert_eq!(submissions.len(), 1);
        assert_eq!(
            decode_request::<TaskTurnRequest>(submissions[0])
                .unwrap()
                .turn()
                .task_id(),
            recipient.0
        );
        let common_dir = if linked_worktree {
            fixture._repo.root().join(".git")
        } else {
            other_repo.root().join(".git")
        }
        .canonicalize()
        .unwrap();
        assert!(
            requests
                .iter()
                .any(|request| request.args.iter().any(|arg| arg == "fetch")
                    && request
                        .args
                        .iter()
                        .any(|arg| arg.to_string_lossy().contains("refs/remotes/mac-worker/"))
                    && request.args.iter().any(|arg| arg == common_dir.as_os_str())),
            "result import must target the recipient's Git common directory"
        );
        assert_eq!(
            std::env::current_dir().unwrap(),
            fixture._repo.root().canonicalize().unwrap()
        );
    }
}

#[test]
fn attached_waiter_keeps_following_the_requested_turn() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let mut fixture = AcceptedThenTerminalFixture::new();
    let project = fixture._repo.root().to_path_buf();
    let (donor, recipient) = saturated_pool(&mut fixture, &project);
    let runner = BusyPinnedRunner {
        fixture: &fixture,
        donor: donor.1,
        probes: Mutex::new(0),
    };
    let error = TurnRunner::new(
        &runner,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .run(donor.0, donor.1, Some(&mut Vec::new()))
    .unwrap_err();
    assert_eq!(error.public_code(), "TASK_QUEUE_MISSING");
    assert!(matches!(
        fixture
            .state
            .queue_entry(recipient.1)
            .unwrap()
            .unwrap()
            .state(),
        mac_worker::job::QueueState::Parked
    ));
    assert!(!fixture.runner.requests().iter().any(|request| {
        request
            .args
            .last()
            .is_some_and(|arg| arg == HostOperation::TaskTurn.command())
    }));
}

#[test]
fn interrupted_legacy_reassignment_recovers_from_an_unrelated_working_directory() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let mut fixture = AcceptedThenTerminalFixture::new();
    let project = fixture._repo.root().to_path_buf();
    let (donor, recipient) = saturated_pool(&mut fixture, &project);
    // A pre-upgrade parked task has a prompt but no saved context.
    fs::remove_file(
        fixture
            .paths
            .state
            .join("turns")
            .join(recipient.0.to_string())
            .join("project.json"),
    )
    .unwrap();
    let crashed_owner = ProcessIdentity::new(424_243, 3).unwrap();
    fixture.state.adopt_row(donor.1, crashed_owner).unwrap();
    fixture
        .state
        .record_runner(
            donor.0,
            Some(mac_worker::task::RunnerIdentity::new(crashed_owner)),
        )
        .unwrap();
    let unrelated = tempfile::tempdir().unwrap();
    let _cwd = CurrentDirGuard::enter(unrelated.path());
    let observations = vec![
        mac_worker::scheduler::CandidateObservation::new(
            "mini-2".into(),
            true,
            mac_worker::scheduler::CandidateSlot::Idle,
            vec!["agent:codex".into(), "darwin-arm64".into()],
            Some(16 << 30),
            64 << 30,
        )
        .unwrap(),
    ];
    fixture
        .state
        .inject_write_failure_once(ClientStateWritePoint::AfterRunnerYieldQueuePublication);
    assert!(
        fixture
            .state
            .claim_parked_for_waiting_runner(
                donor.0,
                donor.1,
                crashed_owner,
                &observations,
                u64::MAX / 4
            )
            .is_err()
    );
    let resumed_state = ClientStateStore::open_with_owner_inspector(
        &fixture.paths.state,
        DeadOwnerInspector {
            dead_owner: crashed_owner,
        },
    )
    .unwrap();
    let recovery = TaskClient::new(
        &fixture.runner,
        &fixture.config,
        &fixture.paths,
        &resumed_state,
        &fixture.executor,
    )
    .reconcile_runners()
    .unwrap();
    assert_eq!(recovery.started_runners(), 1);
    let outcome = TurnRunner::new(
        &fixture.runner,
        &fixture.config,
        &fixture.paths,
        &resumed_state,
        &fixture.executor,
    )
    .run_detached(recipient.0, recipient.1)
    .unwrap();
    assert_eq!(outcome.status().state(), TaskState::Closed);
    assert_eq!(outcome.status().worker(), Some("mini-2"));
    assert_eq!(
        resumed_state
            .task_project_path(&resumed_state.load_task(recipient.0).unwrap())
            .unwrap(),
        Some(project.canonicalize().unwrap())
    );
    assert_eq!(
        resumed_state.load_task(donor.0).unwrap().status().state(),
        TaskState::Queued
    );
    assert_eq!(
        std::env::current_dir().unwrap(),
        unrelated.path().canonicalize().unwrap()
    );
}

fn assert_unavailable_observation(fixture: &AcceptedThenTerminalFixture, worker: &str) {
    let bytes = fs::read(
        fixture
            .paths
            .state
            .join("observations")
            .join(format!("{worker}.json")),
    )
    .unwrap();
    let observation: AdmissionObservation = serde_json::from_slice(&bytes).unwrap();
    assert!(!observation.ready());
    assert!(observation.capabilities().is_empty());
}

fn assert_automatic_admission_survives(failure: AdmissionFailure) {
    for failed_worker_first in [true, false] {
        let mut fixture =
            AcceptedThenTerminalFixture::new_with_options(None, WorkerPreference::Automatic, false);
        add_second_worker(&mut fixture, failed_worker_first);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        fixture
            .state
            .publish_admission_observation(
                AdmissionObservation::new(
                    "mini-2".into(),
                    true,
                    mac_worker::scheduler::CandidateSlot::Idle,
                    vec!["agent:codex".into(), "darwin-arm64".into()],
                    Some(12 * 1024 * 1024 * 1024),
                    100 * 1024 * 1024 * 1024,
                    now,
                )
                .unwrap(),
            )
            .unwrap();
        let runner = AdmissionFailureRunner {
            inner: &fixture.runner,
            ssh: "mac2",
            failure,
            failed_worker_requests: Mutex::new(Vec::new()),
        };

        let outcome = TurnRunner::new(
            &runner,
            &fixture.config,
            &fixture.paths,
            &fixture.state,
            &fixture.executor,
        )
        .run(fixture.task_id, fixture.turn_id, Some(&mut Vec::new()))
        .unwrap();

        assert_remote_turn_completed(&fixture, &outcome);
        assert_eq!(outcome.status().worker(), Some("mini-1"));
        assert_unavailable_observation(&fixture, "mini-2");
        let requests = runner.failed_worker_requests.lock().unwrap();
        assert!(requests.iter().all(|request| {
            request.args.last().is_some_and(|operation| {
                operation == "~/.local/bin/worker host probe"
                    || (matches!(failure, AdmissionFailure::Refresh)
                        && operation == HostOperation::RefreshFacts.command())
            })
        }));
    }
}

#[test]
fn automatic_admission_skips_unreachable_workers_in_either_inventory_order() {
    // Break caught: one failed SSH probe aborts the healthy worker's turn,
    // or leaves an earlier positive observation available to the scheduler.
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    assert_automatic_admission_survives(AdmissionFailure::Probe);
}

#[test]
fn automatic_admission_skips_failed_fact_refresh_in_either_inventory_order() {
    // Break caught: a failed refresh after a successful but stale probe
    // escapes the per-worker observation and aborts automatic selection.
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    assert_automatic_admission_survives(AdmissionFailure::Refresh);
}

#[test]
fn pinned_admission_does_not_fall_back_when_its_worker_is_unreachable() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let mut fixture = AcceptedThenTerminalFixture::new_with_options(
        None,
        WorkerPreference::Pinned {
            worker: "mini-1".into(),
        },
        false,
    );
    add_second_worker(&mut fixture, true);
    fs::remove_file(fixture.paths.state.join("observations/mini-1.json")).unwrap();
    let runner = AdmissionFailureRunner {
        inner: &fixture.runner,
        ssh: "mac1",
        failure: AdmissionFailure::Probe,
        failed_worker_requests: Mutex::new(Vec::new()),
    };

    let error = TurnRunner::new(
        &runner,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, Some(&mut Vec::new()))
    .unwrap_err();

    assert_eq!(error.public_code(), "CAPACITY_BUSY");
    assert_eq!(
        fixture
            .state
            .load_task(fixture.task_id)
            .unwrap()
            .status()
            .state(),
        TaskState::Abandoned
    );
    assert_unavailable_observation(&fixture, "mini-1");
    assert!(fixture.runner.requests().iter().all(|request| {
        request.program != OsStr::new("/usr/bin/ssh")
            || !request.args.iter().any(|arg| arg == "mac2")
    }));
}

#[test]
fn pinned_admission_waits_for_its_worker_to_recover() {
    // Break caught: a temporary probe failure terminates a waiting runner
    // or an unavailable observation prevents dispatch after recovery.
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    fs::remove_file(fixture.paths.state.join("observations/mini-1.json")).unwrap();
    let runner = AdmissionFailureRunner {
        inner: &fixture.runner,
        ssh: "mac1",
        failure: AdmissionFailure::ProbeOnce,
        failed_worker_requests: Mutex::new(Vec::new()),
    };

    let outcome = TurnRunner::new(
        &runner,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, Some(&mut Vec::new()))
    .unwrap();

    assert_remote_turn_completed(&fixture, &outcome);
    assert_eq!(outcome.status().worker(), Some("mini-1"));
}

#[test]
fn admission_preserves_local_state_errors_after_a_failed_peer() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let mut fixture =
        AcceptedThenTerminalFixture::new_with_options(None, WorkerPreference::Automatic, false);
    add_second_worker(&mut fixture, true);
    fs::write(
        fixture.paths.state.join("observations/mini-1.json"),
        b"corrupt local observation\n",
    )
    .unwrap();
    let runner = AdmissionFailureRunner {
        inner: &fixture.runner,
        ssh: "mac2",
        failure: AdmissionFailure::Refresh,
        failed_worker_requests: Mutex::new(Vec::new()),
    };

    let error = TurnRunner::new(
        &runner,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .run(fixture.task_id, fixture.turn_id, Some(&mut Vec::new()))
    .unwrap_err();

    assert!(
        matches!(&error, WorkerError::Io(inner) if inner.kind() == io::ErrorKind::InvalidData),
        "{error}"
    );
    assert!(
        fixture
            .state
            .queue_entry(fixture.turn_id)
            .unwrap()
            .is_some()
    );
}

#[test]
fn admission_preserves_invalid_transport_configuration_errors() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let mut fixture = AcceptedThenTerminalFixture::new();
    fixture.config.workers[0].remote_binary = "/invalid/worker".into();

    let error = fixture.run(&mut Vec::new()).unwrap_err();

    assert_eq!(error.public_code(), "INVALID_REQUEST");
    assert!(
        fixture
            .state
            .queue_entry(fixture.turn_id)
            .unwrap()
            .is_some()
    );
}

#[test]
fn runner_uses_the_submit_time_local_push_target_after_origin_changes() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture =
        AcceptedThenTerminalFixture::new_with_push_origin(Some("https://example.test/repo.git"));
    let queued = fixture
        .state
        .queue_entry_for_task_turn(fixture.task_id)
        .unwrap()
        .unwrap();
    assert!(
        queued
            .requirements()
            .iter()
            .any(|requirement| requirement == "origin:example.test"),
        "scheduler admission must retain the requirement derived at submission"
    );
    assert_eq!(fixture.runner.origin_read_count(), 1);
    fixture._repo.git(&[
        "remote",
        "set-url",
        "origin",
        "https://other.example.test/repo.git",
    ]);
    assert_eq!(
        String::from_utf8(
            fixture
                ._repo
                .git(&["config", "--get", "remote.origin.url"])
                .stdout,
        )
        .unwrap(),
        "https://other.example.test/repo.git\n"
    );

    fixture.run(&mut Vec::new()).unwrap();

    assert_eq!(
        fixture.runner.origin_read_count(),
        1,
        "post-submit execution must not read the mutable Git origin"
    );
    assert_eq!(
        fixture.runner.submitted_turn_origin().as_deref(),
        Some("https://example.test/repo.git"),
        "each turn must use the target pinned when the task was submitted"
    );
}

#[test]
fn codex_first_turn_does_not_prebind_a_session() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    fixture.run(&mut Vec::new()).unwrap();
    assert!(fixture.runner.requests().iter().all(|request| {
        request
            .args
            .iter()
            .all(|arg| arg != HostOperation::TaskPrebind.command())
    }));
}

#[test]
fn follower_flush_error_detaches_runner_and_completes_remote_turn() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let mut follower = FailOnFlush::default();

    let outcome = fixture.run(&mut follower).unwrap();

    assert!(follower.bytes.windows(4).any(|window| window == b"type"));
    assert_remote_turn_completed(&fixture, &outcome);
}

impl FetchFailingRunner {
    fn new() -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            task: Mutex::new(None),
        }
    }

    fn requests(&self) -> Vec<ProcessRequest> {
        self.requests.lock().unwrap().clone()
    }

    fn task_status(&self, task_id: TaskId) -> Result<TaskStatus, mac_worker::error::WorkerError> {
        let task = self.task.lock().unwrap();
        let (meta, turn_id) = task.as_ref().ok_or_else(|| {
            mac_worker::error::WorkerError::Protocol(
                "task status requested before task preparation".into(),
            )
        })?;
        assert_eq!(meta.task_id(), task_id);
        TaskStatus::new(
            TaskState::Open,
            Some(TaskOutcome::Done),
            Some("mini-1".into()),
            true,
            Some(meta.base_oid().clone()),
            Some("finished".into()),
            Vec::new(),
            Vec::new(),
            None,
            vec![TurnSummary::new(
                1,
                *turn_id,
                Some(TurnTerminal::Succeeded),
                Some(TaskOutcome::Done),
                Some(true),
                false,
                Some(meta.created_at_millis()),
                Some(meta.created_at_millis() + 1),
            )],
            meta.created_at_millis() + 1,
        )
    }
}

impl ProcessRunner for FetchFailingRunner {
    fn run(
        &self,
        request: &ProcessRequest,
    ) -> Result<ProcessResult, mac_worker::error::WorkerError> {
        self.requests.lock().unwrap().push(request.clone());

        if request.program == OsStr::new("/usr/bin/git") {
            let is_result_fetch = request.args.iter().any(|arg| arg == "fetch")
                && request
                    .args
                    .iter()
                    .any(|arg| arg.to_string_lossy().starts_with("--upload-pack="));
            if is_result_fetch {
                return Ok(ProcessResult {
                    status: ExitStatus::from_raw(1 << 8),
                    stdout: Vec::new(),
                    stderr: b"result is unavailable".to_vec(),
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
            "~/.local/bin/worker host probe" => canonical_process(&ProbeResponse {
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
                        auth_by_profile: Vec::new(),
                    }],
                    env_profiles: Vec::new(),
                    git_identity: true,
                    collected_at_millis: u64::MAX / 2,
                    herdr: None,
                }),
                facts_age_millis: Some(0),
            }),
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
                let prepare: TaskPrepareRequest = decode_request(request)?;
                *self.task.lock().unwrap() = Some((prepare.meta().clone(), prepare.job_id()));
                canonical_process(&TaskPrepareResponse::new(
                    prepare.meta().base_oid().clone(),
                    false,
                ))
            }
            value if value == HostOperation::TaskStatus.command() => {
                let status_request: TaskStatusRequest = decode_request(request)?;
                canonical_process(&TaskStatusResponse::new(
                    self.task_status(status_request.task_id())?,
                ))
            }
            value if value == HostOperation::TaskTurn.command() => {
                let turn: TaskTurnRequest = decode_request(request)?;
                let material = turn.submit().material();
                let job_meta = JobMeta::new(material, material.fingerprint())?;
                let submit = SubmitResponse::Accepted {
                    meta: Box::new(job_meta),
                    status: JobStatus::accepted(material.created_at_millis() + 1)?,
                };
                canonical_process(&TaskTurnResponse::new(
                    submit,
                    self.task_status(turn.turn().task_id())?,
                ))
            }
            value if value == HostOperation::Status.command() => {
                fixture_terminal_job(&self.requests(), request, 0, 0)
            }
            value if value == HostOperation::LogChunk.command() => {
                let query: LogChunkRequest = decode_request(request)?;
                canonical_process(&mac_worker::job::LogChunkResponse::new(LogChunk::new(
                    query.stream(),
                    query.offset(),
                    vec![],
                )?)?)
            }
            value if value == HostOperation::TaskClose.command() => {
                let close: TaskCloseRequest = decode_request(request)?;
                let status = self.task_status(close.task_id())?;
                let status = TaskStatus::new(
                    if close.discard() {
                        TaskState::Abandoned
                    } else {
                        TaskState::Closed
                    },
                    status.last_outcome().cloned(),
                    status.worker().map(str::to_owned),
                    status.session_present(),
                    status.head_oid().cloned(),
                    status.summary().map(str::to_owned),
                    status.questions().to_vec(),
                    status.files_changed().to_vec(),
                    status.diff_stat().map(str::to_owned),
                    status.turns().to_vec(),
                    status.updated_at_millis() + 1,
                )?;
                canonical_process(&TaskCloseResponse::new(status))
            }
            other => panic!("unexpected worker operation: {other}"),
        }
    }
}

fn decode_request<T: serde::de::DeserializeOwned>(
    request: &ProcessRequest,
) -> Result<T, mac_worker::error::WorkerError> {
    serde_json::from_slice(request.stdin.as_deref().ok_or_else(|| {
        mac_worker::error::WorkerError::Protocol("worker request had no stdin".into())
    })?)
    .map_err(|error| mac_worker::error::WorkerError::Protocol(error.to_string()))
}

fn canonical_process<T: serde::Serialize>(
    value: &T,
) -> Result<ProcessResult, mac_worker::error::WorkerError> {
    let mut stdout = serde_json::to_vec(value)
        .map_err(|error| mac_worker::error::WorkerError::Protocol(error.to_string()))?;
    stdout.push(b'\n');
    Ok(ProcessResult {
        status: ExitStatus::from_raw(0),
        stdout,
        stderr: Vec::new(),
    })
}

#[test]
fn result_fetch_failure_finishes_the_turn_and_leaves_the_task_closable() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    let _current_dir = CurrentDirGuard::enter(repo.root());

    let state_root = tempfile::tempdir().unwrap();
    let state_root_path = state_root.path().canonicalize().unwrap();
    let paths = support::task_harness::paths(&state_root_path);
    let state = ClientStateStore::open(&paths.state).unwrap();
    let config = Config::parse(
        "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap();
    let runner = FetchFailingRunner::new();
    let executor = InlineRunnerExecutor;
    let client = TaskClient::new(&runner, &config, &paths, &state, &executor);

    let report = client
        .submit(
            TaskSubmitRequest {
                agent: AgentKind::Codex,
                model: None,
                effort: None,
                prompt: "make the change".into(),
                project: repo.root().to_path_buf(),
                base: "main".into(),
                wip: true,
                source: None,
                publish: None,
                publish_branch: None,
                cli_includes: Vec::new(),
                limits: TaskLimits::default(),
                close_policy: ClosePolicy::Never,
                env_profile: None,
                preference: WorkerPreference::Pinned {
                    worker: "mini-1".into(),
                },
                wait_for_capacity: true,
                attached: false,
                run_id: None,
            },
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap();
    let task_id = report.task_id();
    let entry = state.queue_entry_for_task_turn(task_id).unwrap().unwrap();
    let turn_id = entry.job_id();

    let outcome = TurnRunner::new(&runner, &config, &paths, &state, &executor)
        .run(task_id, turn_id, None)
        .unwrap();

    assert_eq!(outcome.status().state(), TaskState::Open);
    assert_eq!(
        outcome.status().last_outcome(),
        Some(&TaskOutcome::failed("RESULT_FETCH_FAILED"))
    );
    assert_eq!(outcome.exit_code(), 70);
    assert_eq!(state.runner_liveness(task_id).unwrap(), None);
    assert!(state.queue_entry_for_task_turn(task_id).unwrap().is_none());
    assert_eq!(outcome.events().last().unwrap()["type"], "turn_terminal");
    assert_eq!(
        outcome.events().last().unwrap()["outcome"]["reason"],
        "RESULT_FETCH_FAILED"
    );
    assert!(runner.requests().iter().any(|request| {
        request.program == OsStr::new("/usr/bin/git")
            && request.args.iter().any(|arg| arg == "fetch")
            && request
                .args
                .iter()
                .any(|arg| arg.to_string_lossy().starts_with("--upload-pack="))
    }));

    let project = mac_worker::project_state::ProjectState::load(&runner, repo.root(), &[]).unwrap();
    {
        let transfer =
            TransferRepo::open_or_create(&paths.cache, &project.context.common_dir).unwrap();
        assert!(!transfer.has_ref(&format!("refs/mac-worker/bases/{task_id}")));
    }

    let closed = client.close(task_id, true).unwrap();
    assert_eq!(closed.status().state(), TaskState::Abandoned);
}

#[test]
fn close_succeeds_immediately_after_wait_returns() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    let _current_dir = CurrentDirGuard::enter(repo.root());

    let state_root = tempfile::tempdir().unwrap();
    let state_root_path = state_root.path().canonicalize().unwrap();
    let paths = support::task_harness::paths(&state_root_path);
    let state = ClientStateStore::open(&paths.state).unwrap();
    let config = Config::parse(
        "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap();
    let runner = FetchFailingRunner::new();
    let executor = InlineRunnerExecutor;
    let client = TaskClient::new(&runner, &config, &paths, &state, &executor);

    let report = client
        .submit(
            TaskSubmitRequest {
                agent: AgentKind::Codex,
                model: None,
                effort: None,
                prompt: "make the change".into(),
                project: repo.root().to_path_buf(),
                base: "main".into(),
                wip: true,
                source: None,
                publish: None,
                publish_branch: None,
                cli_includes: Vec::new(),
                limits: TaskLimits::default(),
                close_policy: ClosePolicy::Never,
                env_profile: None,
                preference: WorkerPreference::Pinned {
                    worker: "mini-1".into(),
                },
                wait_for_capacity: true,
                attached: false,
                run_id: None,
            },
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap();
    let task_id = report.task_id();
    let turn_id = state
        .queue_entry_for_task_turn(task_id)
        .unwrap()
        .unwrap()
        .job_id();

    TurnRunner::new(&runner, &config, &paths, &state, &executor)
        .run(task_id, turn_id, None)
        .unwrap();

    let waited = client
        .wait(WaitSelector::Task(task_id), Some(Duration::from_secs(5)))
        .unwrap();
    assert_eq!(waited.task_ids(), &[task_id]);
    assert!(state.queue_entry_for_task_turn(task_id).unwrap().is_none());
    assert_eq!(state.runner_liveness(task_id).unwrap(), None);

    let closed = client.close(task_id, true).unwrap();
    assert_eq!(closed.status().state(), TaskState::Abandoned);
}

fn assert_submit_rolls_back_post_create_state(
    fault: ClientStateWritePoint,
    expected_error: &str,
    expect_rollback: bool,
    fail_base_release: bool,
    default_publish_branch: bool,
) {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    assert!(
        repo.git(&[
            "remote",
            "add",
            "origin",
            "https://github.com/example/project.git"
        ])
        .status
        .success()
    );
    let _current_dir = CurrentDirGuard::enter(repo.root());

    let state_root = tempfile::tempdir().unwrap();
    let state_root_path = state_root.path().canonicalize().unwrap();
    let paths = support::task_harness::paths(&state_root_path);
    let state = ClientStateStore::open(&paths.state).unwrap();
    let run_id = RunId::generate();
    state
        .create_run(RunRecord::new(run_id, None, vec![TaskId::generate()], 1, 1).unwrap())
        .unwrap();
    let config = Config::parse(
        "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap();
    let runner = if fail_base_release {
        AcceptedThenTerminalRunner::with_base_release_failure()
    } else {
        AcceptedThenTerminalRunner::new()
    };
    let executor = InlineRunnerExecutor;
    let client = TaskClient::new(&runner, &config, &paths, &state, &executor);

    plant_bound_mini1_ready(
        &state,
        vec![
            "darwin-arm64".into(),
            "origin:github.com".into(),
            "agent:codex".into(),
        ],
    );
    if fault == ClientStateWritePoint::AfterTaskSubmissionTurnsRetirement {
        state.inject_write_failure_once(ClientStateWritePoint::BeforeSubmissionReport);
        state.inject_submission_rollback_cleanup_failure_once(fault);
    } else if fault == ClientStateWritePoint::AfterTaskReplacementExchangeBeforeFirstDirectorySync {
        state.inject_write_failure_once(ClientStateWritePoint::BeforeSubmissionReport);
        state.inject_task_replacement_after_exchange_failure_once();
    } else {
        state.inject_write_failure_once(fault);
    }

    let error = client
        .submit(
            TaskSubmitRequest {
                agent: AgentKind::Codex,
                model: None,
                effort: None,
                prompt: "make the change".into(),
                project: repo.root().to_path_buf(),
                base: "main".into(),
                wip: fail_base_release,
                source: Some("local".into()),
                publish: Some(if fail_base_release {
                    vec!["fetch".into()]
                } else {
                    vec!["fetch".into(), "push".into()]
                }),
                publish_branch: (!fail_base_release && !default_publish_branch)
                    .then_some("rollback-test".into()),
                cli_includes: Vec::new(),
                limits: TaskLimits::default(),
                close_policy: ClosePolicy::Never,
                env_profile: None,
                preference: WorkerPreference::Pinned {
                    worker: "mini-1".into(),
                },
                wait_for_capacity: true,
                attached: false,
                run_id: Some(run_id),
            },
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap_err();

    assert!(
        error.to_string().contains(expected_error),
        "submit returned an unexpected error: {error}"
    );
    if fault == ClientStateWritePoint::AfterTaskReplacementExchangeBeforeFirstDirectorySync {
        let reopened = ClientStateStore::open(&paths.state).unwrap();
        assert!(reopened.list_tasks().unwrap().is_empty());

        TaskClient::new(&runner, &config, &paths, &reopened, &executor)
            .reconcile_runners()
            .unwrap();
        assert!(reopened.list_tasks().unwrap().is_empty());
        assert!(reopened.queue_snapshot().unwrap().entries().is_empty());
        assert!(
            reopened
                .load_run(run_id)
                .unwrap()
                .publish_branches()
                .is_empty()
        );
        let turn_entries = std::fs::read_dir(paths.state.join("turns"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(turn_entries, vec![".mac-worker-rooted-fs"]);
        return;
    }
    if expect_rollback {
        assert!(state.list_tasks().unwrap().is_empty());
        assert!(state.queue_snapshot().unwrap().entries().is_empty());
        assert!(
            state
                .load_run(run_id)
                .unwrap()
                .publish_branches()
                .is_empty()
        );
        let turn_entries = std::fs::read_dir(paths.state.join("turns"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(turn_entries, vec![".mac-worker-rooted-fs"]);
        let project =
            mac_worker::project_state::ProjectState::load(&runner, repo.root(), &[]).unwrap();
        let transfer =
            TransferRepo::open_or_create(&paths.cache, &project.context.common_dir).unwrap();
        let base_refs = process::Command::new("/usr/bin/git")
            .args([
                "--git-dir",
                &transfer.path().to_string_lossy(),
                "for-each-ref",
                "refs/mac-worker/bases",
            ])
            .output()
            .unwrap();
        assert!(base_refs.status.success());
        assert!(base_refs.stdout.is_empty());
    } else {
        let task = state.list_tasks().unwrap().pop().unwrap();
        assert_eq!(task.status().state(), TaskState::Abandoned);
        assert_eq!(task.abandon_code(), Some("SUBMISSION_ROLLBACK_INCOMPLETE"));
        if fault == ClientStateWritePoint::AfterTaskSubmissionTurnsRetirement {
            let task_id = task.meta().task_id();
            assert!(state.queue_entry_for_task_turn(task_id).unwrap().is_none());
            assert_eq!(state.queue_snapshot().unwrap().entries().len(), 1);
            assert!(
                std::fs::metadata(paths.state.join("turns").join(task_id.to_string())).is_err()
            );
            assert!(
                state
                    .load_run(run_id)
                    .unwrap()
                    .publish_branches()
                    .is_empty()
            );
            let project =
                mac_worker::project_state::ProjectState::load(&runner, repo.root(), &[]).unwrap();
            {
                let transfer =
                    TransferRepo::open_or_create(&paths.cache, &project.context.common_dir)
                        .unwrap();
                assert!(!transfer.has_ref(&format!("refs/mac-worker/bases/{task_id}")));
            }

            let reopened = ClientStateStore::open(&paths.state).unwrap();
            let recovered = reopened.load_task(task_id).unwrap();
            assert_eq!(
                recovered.abandon_code(),
                Some("SUBMISSION_ROLLBACK_INCOMPLETE")
            );
            let recovered_client = TaskClient::new(&runner, &config, &paths, &reopened, &executor);
            recovered_client.reconcile_runners().unwrap();
            assert!(reopened.list_tasks().unwrap().is_empty());
            assert!(reopened.queue_snapshot().unwrap().entries().is_empty());
            assert!(
                reopened
                    .load_run(run_id)
                    .unwrap()
                    .publish_branches()
                    .is_empty()
            );
            let turn_entries = std::fs::read_dir(paths.state.join("turns"))
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<Vec<_>>();
            assert_eq!(turn_entries, vec![".mac-worker-rooted-fs"]);
            let transfer =
                TransferRepo::open_or_create(&paths.cache, &project.context.common_dir).unwrap();
            assert!(!transfer.has_ref(&format!("refs/mac-worker/bases/{task_id}")));
        } else if fault == ClientStateWritePoint::BeforeTaskSubmissionRecordRemoval {
            assert!(
                state
                    .queue_entry_for_task_turn(task.meta().task_id())
                    .unwrap()
                    .is_none()
            );
        } else {
            let entry = state
                .queue_entry_for_task_turn(task.meta().task_id())
                .unwrap()
                .unwrap();
            assert!(
                state
                    .read_turn_prompt(task.meta().task_id(), entry.job_id())
                    .is_ok()
            );
        }
        if fault != ClientStateWritePoint::AfterTaskSubmissionTurnsRetirement {
            client.reconcile_runners().unwrap();
            assert!(state.list_tasks().unwrap().is_empty());
            assert!(state.queue_snapshot().unwrap().entries().is_empty());
            assert!(
                state
                    .load_run(run_id)
                    .unwrap()
                    .publish_branches()
                    .is_empty()
            );
            let project =
                mac_worker::project_state::ProjectState::load(&runner, repo.root(), &[]).unwrap();
            let transfer =
                TransferRepo::open_or_create(&paths.cache, &project.context.common_dir).unwrap();
            assert!(!transfer.has_ref(&format!("refs/mac-worker/bases/{}", task.meta().task_id())));
        }
    }
}

#[test]
fn submit_rolls_back_post_create_state_when_prompt_write_fails() {
    assert_submit_rolls_back_post_create_state(
        ClientStateWritePoint::BeforeTurnPromptWrite,
        "before task turn prompt write",
        true,
        false,
        false,
    );
}

#[test]
fn submit_rolls_back_post_create_state_when_run_reference_load_fails() {
    assert_submit_rolls_back_post_create_state(
        ClientStateWritePoint::BeforeRunLoad,
        "before run load",
        true,
        false,
        false,
    );
}

#[test]
fn submit_rolls_back_post_create_state_when_queue_publication_fails() {
    assert_submit_rolls_back_post_create_state(
        ClientStateWritePoint::BeforePublish,
        "before publication",
        true,
        false,
        false,
    );
}

#[test]
fn submit_retains_complete_state_when_queue_rollback_fails() {
    assert_submit_rolls_back_post_create_state(
        ClientStateWritePoint::BeforeTaskReport,
        "before task report",
        false,
        false,
        false,
    );
}

#[test]
fn submit_retries_rollback_after_reservation_release_failure() {
    assert_submit_rolls_back_post_create_state(
        ClientStateWritePoint::BeforeRunPublishBranchRelease,
        "before run publish-branch release",
        false,
        false,
        false,
    );
}

#[test]
fn submit_retries_rollback_after_final_task_record_removal_failure() {
    assert_submit_rolls_back_post_create_state(
        ClientStateWritePoint::BeforeTaskSubmissionRecordRemoval,
        "before task submission record removal",
        false,
        false,
        false,
    );
}

#[test]
fn submit_restores_task_and_queue_after_prompt_tree_removal_failure() {
    assert_submit_rolls_back_post_create_state(
        ClientStateWritePoint::BeforeTaskSubmissionTurnsRemoval,
        "before task submission turns removal",
        false,
        false,
        false,
    );
}

#[test]
fn submit_retries_rollback_after_base_release_failure() {
    assert_submit_rolls_back_post_create_state(
        ClientStateWritePoint::BeforeTaskRollbackBaseRelease,
        "before task rollback base release",
        false,
        true,
        false,
    );
}

#[test]
fn submit_rolls_back_the_effective_default_run_publish_branch() {
    assert_submit_rolls_back_post_create_state(
        ClientStateWritePoint::BeforeSubmissionReport,
        "before submission report",
        true,
        false,
        true,
    );
}

#[test]
fn restart_recovers_after_turn_tree_retirement_fault_with_a_durable_rollback_marker() {
    assert_submit_rolls_back_post_create_state(
        ClientStateWritePoint::AfterTaskSubmissionTurnsRetirement,
        "before submission report",
        false,
        false,
        false,
    );
}

#[test]
fn reopen_recovers_first_replacement_residue_after_rollback_retry_removes_task() {
    // Break caught: the first marker replacement exchanged and left its
    // displaced replace-<uuid> record before fsync. The retry completed the
    // rollback and removed the live task, leaving reopen unable to validate
    // or discard the first residue.
    assert_submit_rolls_back_post_create_state(
        ClientStateWritePoint::AfterTaskReplacementExchangeBeforeFirstDirectorySync,
        "before submission report",
        false,
        false,
        false,
    );
}

#[test]
fn reconciliation_excludes_retired_pending_rollback_turn_before_dead_owner_adoption() {
    // Break caught: recovery used only the prompt-tree mapping, so a second
    // cleanup fault let reconciliation adopt a dead owner after the tree had
    // already been retired.
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    let _current_dir = CurrentDirGuard::enter(repo.root());
    let root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
    let state = ClientStateStore::open(&paths.state).unwrap();
    let config = Config::parse(
        "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap();
    let runner = AcceptedThenTerminalRunner::new();
    let executor = InlineRunnerExecutor;
    plant_bound_mini1_ready(&state, vec!["darwin-arm64".into(), "agent:codex".into()]);
    state.inject_write_failure_once(ClientStateWritePoint::BeforeSubmissionReport);
    state.inject_submission_rollback_cleanup_failure_once(
        ClientStateWritePoint::AfterTaskSubmissionTurnsRetirement,
    );

    let error = TaskClient::new(&runner, &config, &paths, &state, &executor)
        .submit(
            TaskSubmitRequest {
                agent: AgentKind::Codex,
                model: None,
                effort: None,
                prompt: "make the change".into(),
                project: repo.root().to_path_buf(),
                base: "main".into(),
                wip: true,
                source: Some("local".into()),
                publish: Some(vec!["fetch".into()]),
                publish_branch: None,
                cli_includes: Vec::new(),
                limits: TaskLimits::default(),
                close_policy: ClosePolicy::Never,
                env_profile: None,
                preference: WorkerPreference::Pinned {
                    worker: "mini-1".into(),
                },
                wait_for_capacity: true,
                attached: false,
                run_id: None,
            },
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap_err();
    assert!(error.to_string().contains("before submission report"));

    let pending = state.list_tasks().unwrap().pop().unwrap();
    let task_id = pending.meta().task_id();
    let turn_id = pending.submission_rollback_turn_id().unwrap();
    assert!(state.turn_ids_for_task(task_id).unwrap().is_empty());

    let dead_owner = ProcessIdentity::new(424_242, 4_242_427).unwrap();
    state
        .remove_task_turn_for_submission_rollback(turn_id)
        .unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    state
        .enqueue(
            QueueEntry::new(
                turn_id,
                state.client_id(),
                pending.meta().project_id().to_owned(),
                pending.meta().worktree_id().to_owned(),
                CommandSummary::argv(1).unwrap(),
                Vec::new(),
                WorkerPreference::Pinned {
                    worker: "mini-1".into(),
                },
                QueueEntryKind::TaskTurn,
                None,
                dead_owner,
                now,
            )
            .unwrap(),
        )
        .unwrap();

    let restarted = ClientStateStore::open_with_owner_inspector(
        &paths.state,
        DeadOwnerInspector { dead_owner },
    )
    .unwrap();
    restarted.inject_write_failure_once(ClientStateWritePoint::BeforeTaskReport);
    TaskClient::new(&runner, &config, &paths, &restarted, &executor)
        .reconcile_runners()
        .unwrap();
    let retained = restarted.queue_entry(turn_id).unwrap().unwrap();
    assert_eq!(retained.owner_opt(), Some(&dead_owner));
    assert!(restarted.load_task(task_id).is_ok());

    TaskClient::new(&runner, &config, &paths, &restarted, &executor)
        .reconcile_runners()
        .unwrap();
    assert!(restarted.load_task(task_id).is_err());
    assert!(restarted.queue_entry(turn_id).unwrap().is_none());
}

#[test]
fn rollback_retry_does_not_release_a_later_tasks_reacquired_run_publish_branch() {
    // Break caught: a rollback retried by task A released a branch-name-only
    // reservation that task B had acquired after A's earlier compensation.
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    assert!(
        repo.git(&[
            "remote",
            "add",
            "origin",
            "https://github.com/example/project.git",
        ])
        .status
        .success()
    );
    let _current_dir = CurrentDirGuard::enter(repo.root());
    let root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
    let state = ClientStateStore::open(&paths.state).unwrap();
    let run_id = RunId::generate();
    state
        .create_run(
            RunRecord::new(
                run_id,
                None,
                vec![TaskId::generate(), TaskId::generate()],
                2,
                1,
            )
            .unwrap(),
        )
        .unwrap();
    let config = Config::parse(
        "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap();
    let runner = AcceptedThenTerminalRunner::new();
    let executor = InlineRunnerExecutor;
    plant_bound_mini1_ready(
        &state,
        vec![
            "darwin-arm64".into(),
            "origin:github.com".into(),
            "agent:codex".into(),
        ],
    );
    let request = || TaskSubmitRequest {
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        prompt: "make the change".into(),
        project: repo.root().to_path_buf(),
        base: "main".into(),
        wip: false,
        source: Some("local".into()),
        publish: Some(vec!["fetch".into(), "push".into()]),
        publish_branch: Some("shared-branch".into()),
        cli_includes: Vec::new(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        preference: WorkerPreference::Pinned {
            worker: "mini-1".into(),
        },
        wait_for_capacity: true,
        attached: false,
        run_id: Some(run_id),
    };

    state.inject_write_failure_once(ClientStateWritePoint::BeforeRunPublishBranchRelease);
    let first_error = TaskClient::new(&runner, &config, &paths, &state, &executor)
        .submit(request(), &mut Vec::new(), &mut Vec::new())
        .unwrap_err();
    assert!(
        first_error
            .to_string()
            .contains("before run publish-branch release")
    );
    let task_a = state.list_tasks().unwrap().pop().unwrap().meta().task_id();

    // B's initial public submit reconciles A first. Let that first retry
    // release A's reservation but stop before queue retirement, leaving A's
    // durable rollback record for the later retry below.
    state.inject_write_failure_once(ClientStateWritePoint::BeforeTaskReport);
    let task_b = TaskClient::new(&runner, &config, &paths, &state, &executor)
        .submit(request(), &mut Vec::new(), &mut Vec::new())
        .unwrap()
        .task_id();
    let handed_off_b = state.load_task(task_b).unwrap();
    assert!(handed_off_b.submission_intent_turn_id().is_none());
    assert!(handed_off_b.runner().is_some());

    TaskClient::new(&runner, &config, &paths, &state, &executor)
        .reconcile_runners()
        .unwrap();
    assert!(state.load_task(task_a).is_err());
    assert!(state.load_task(task_b).is_ok());
    assert_eq!(
        state.load_run(run_id).unwrap().publish_branches(),
        &["shared-branch".parse().unwrap()]
    );
}

#[test]
fn submit_never_rolls_back_a_parked_row_after_another_runner_adopts_it() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    let _current_dir = CurrentDirGuard::enter(repo.root());
    let root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let state = Arc::new(
        ClientStateStore::open_with_concurrency_hook(
            &paths.state,
            Arc::new(ParkedSubmissionGate {
                entered: entered_tx,
                release: Mutex::new(release_rx),
                used: AtomicBool::new(false),
            }),
        )
        .unwrap(),
    );
    let config = Config::parse(
        "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap();
    let runner = AcceptedThenTerminalRunner::new();
    let executor = InlineRunnerExecutor;
    plant_bound_mini1_ready(&state, vec!["darwin-arm64".into(), "agent:codex".into()]);
    let request = || TaskSubmitRequest {
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        prompt: "make the change".into(),
        project: repo.root().to_path_buf(),
        base: "main".into(),
        wip: true,
        source: Some("local".into()),
        publish: Some(vec!["fetch".into()]),
        publish_branch: None,
        cli_includes: Vec::new(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        preference: WorkerPreference::Pinned {
            worker: "mini-1".into(),
        },
        wait_for_capacity: true,
        attached: false,
        run_id: None,
    };
    TaskClient::new(&runner, &config, &paths, &state, &executor)
        .submit(request(), &mut Vec::new(), &mut Vec::new())
        .unwrap();
    state.inject_write_failure_once(ClientStateWritePoint::AfterParkedTaskTurnPublication);

    thread::scope(|scope| {
        let submit = scope.spawn(|| {
            TaskClient::new(&runner, &config, &paths, &state, &executor).submit(
                request(),
                &mut Vec::new(),
                &mut Vec::new(),
            )
        });
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let owner = InlineRunnerExecutor
            .start(&paths, TaskId::generate(), TurnId::generate())
            .unwrap()
            .process_identity();
        let entry = state.unpark_oldest(owner).unwrap().unwrap();
        state.adopt_row(entry.job_id(), owner).unwrap();
        release_tx.send(()).unwrap();
        let error = submit.join().unwrap().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("after parked task-turn publication")
        );
        assert_eq!(state.list_tasks().unwrap().len(), 2);
        assert!(state.queue_entry(entry.job_id()).unwrap().is_some());
    });
}

#[test]
fn reconciliation_does_not_rollback_a_submission_that_cleared_its_intent_while_waiting_for_transfer_lock()
 {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    let _current_dir = CurrentDirGuard::enter(repo.root());
    let root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
    let (submit_entered_tx, submit_entered_rx) = mpsc::channel();
    let (submit_release_tx, submit_release_rx) = mpsc::channel();
    let (reconciliation_entered_tx, reconciliation_entered_rx) = mpsc::channel();
    let (reconciliation_release_tx, reconciliation_release_rx) = mpsc::channel();
    let state = Arc::new(
        ClientStateStore::open_with_concurrency_hook(
            &paths.state,
            Arc::new(SubmissionIntentRaceGate {
                submit_entered: submit_entered_tx,
                submit_release: Mutex::new(submit_release_rx),
                reconciliation_entered: reconciliation_entered_tx,
                reconciliation_release: Mutex::new(reconciliation_release_rx),
            }),
        )
        .unwrap(),
    );
    let config = Config::parse(
        "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap();
    let runner = AcceptedThenTerminalRunner::new();
    let executor = InlineRunnerExecutor;
    plant_bound_mini1_ready(&state, vec!["darwin-arm64".into(), "agent:codex".into()]);
    let request = || TaskSubmitRequest {
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        prompt: "make the change".into(),
        project: repo.root().to_path_buf(),
        base: "main".into(),
        wip: true,
        source: Some("local".into()),
        publish: Some(vec!["fetch".into()]),
        publish_branch: None,
        cli_includes: Vec::new(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        preference: WorkerPreference::Pinned {
            worker: "mini-1".into(),
        },
        wait_for_capacity: true,
        attached: false,
        run_id: None,
    };

    thread::scope(|scope| {
        let submit = scope.spawn(|| {
            TaskClient::new(&runner, &config, &paths, &state, &executor).submit(
                request(),
                &mut Vec::new(),
                &mut Vec::new(),
            )
        });
        submit_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        let reconcile = scope.spawn(|| {
            TaskClient::new(&runner, &config, &paths, &state, &executor).reconcile_runners()
        });
        reconciliation_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        submit_release_tx.send(()).unwrap();
        let report = submit.join().unwrap().unwrap();
        let task_id = report.task_id();
        let handed_off = state.load_task(task_id).unwrap();
        assert!(handed_off.submission_intent_turn_id().is_none());
        assert!(handed_off.runner().is_some());
        assert!(state.queue_entry_for_task_turn(task_id).unwrap().is_some());

        reconciliation_release_tx.send(()).unwrap();
        reconcile.join().unwrap().unwrap();

        let retained = state.load_task(task_id).unwrap();
        assert!(retained.submission_intent_turn_id().is_none());
        assert!(retained.runner().is_some());
        let entry = state.queue_entry_for_task_turn(task_id).unwrap().unwrap();
        assert!(state.read_turn_prompt(task_id, entry.job_id()).is_ok());
        let project =
            mac_worker::project_state::ProjectState::load(&runner, repo.root(), &[]).unwrap();
        let transfer =
            TransferRepo::open_or_create(&paths.cache, &project.context.common_dir).unwrap();
        assert!(transfer.has_ref(&format!("refs/mac-worker/bases/{task_id}")));
    });
}

#[test]
fn intent_clear_fsync_failure_does_not_restore_a_stale_submission_snapshot_after_adoption() {
    // Break caught: a clear that publishes before its final sync can return an
    // error after reconciliation has adopted the row. Submit must not then
    // write its pre-handoff rollback snapshot over the runner identity.
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    let _current_dir = CurrentDirGuard::enter(repo.root());
    let root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
    let (clear_entered_tx, clear_entered_rx) = mpsc::channel();
    let (clear_release_tx, clear_release_rx) = mpsc::channel();
    let state = Arc::new(
        ClientStateStore::open_with_concurrency_hook(
            &paths.state,
            Arc::new(SubmissionIntentClearFailureGate {
                entered: clear_entered_tx,
                release: Mutex::new(clear_release_rx),
            }),
        )
        .unwrap(),
    );
    let config = Config::parse(
        "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap();
    let runner = AcceptedThenTerminalRunner::new();
    let executor = InlineRunnerExecutor;
    plant_bound_mini1_ready(&state, vec!["darwin-arm64".into(), "agent:codex".into()]);
    state.inject_write_failure_once(
        ClientStateWritePoint::AfterSubmissionIntentClearPublicationBeforeFinalSync,
    );

    let request = || TaskSubmitRequest {
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        prompt: "make the change".into(),
        project: repo.root().to_path_buf(),
        base: "main".into(),
        wip: true,
        source: Some("local".into()),
        publish: Some(vec!["fetch".into()]),
        publish_branch: None,
        cli_includes: Vec::new(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        preference: WorkerPreference::Pinned {
            worker: "mini-1".into(),
        },
        wait_for_capacity: true,
        attached: false,
        run_id: None,
    };

    thread::scope(|scope| {
        let submit = scope.spawn(|| {
            TaskClient::new(&runner, &config, &paths, &state, &executor).submit(
                request(),
                &mut Vec::new(),
                &mut Vec::new(),
            )
        });
        clear_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();

        let published = state.list_tasks().unwrap().pop().unwrap();
        let task_id = published.meta().task_id();
        let turn_id = state
            .queue_entry_for_task_turn(task_id)
            .unwrap()
            .unwrap()
            .job_id();
        assert!(published.submission_intent_turn_id().is_none());

        let dead_owner = ProcessIdentity::new(424_243, 4_242_437).unwrap();
        state.adopt_row(turn_id, dead_owner).unwrap();
        state.inject_submission_rollback_cleanup_failure_once(
            ClientStateWritePoint::AfterTaskSubmissionTurnsRetirement,
        );
        let reconciler = ClientStateStore::open_with_owner_inspector(
            &paths.state,
            DeadOwnerInspector { dead_owner },
        )
        .unwrap();
        TaskClient::new(&runner, &config, &paths, &reconciler, &executor)
            .reconcile_runners()
            .unwrap();
        assert!(reconciler.load_task(task_id).unwrap().runner().is_some());

        clear_release_tx.send(()).unwrap();
        let error = submit.join().unwrap().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("after submission intent-clear publication before final sync")
        );

        let retained = reconciler.load_task(task_id).unwrap();
        assert!(retained.runner().is_some());
        assert!(reconciler.queue_entry(turn_id).unwrap().is_some());
        assert!(reconciler.read_turn_prompt(task_id, turn_id).is_ok());

        let outcome = TurnRunner::new(&runner, &config, &paths, &reconciler, &executor)
            .run(task_id, turn_id, None)
            .unwrap();
        assert_eq!(outcome.status().state(), TaskState::Closed);
        assert!(reconciler.queue_entry(turn_id).unwrap().is_none());
        assert!(reconciler.read_turn_prompt(task_id, turn_id).is_err());
    });
}

#[test]
fn reconciliation_keeps_pending_submission_intent_out_of_runner_startup_until_recovery_completes() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    let _current_dir = CurrentDirGuard::enter(repo.root());
    let root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
    let state = ClientStateStore::open(&paths.state).unwrap();
    let config = Config::parse(
        "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap();
    let runner = AcceptedThenTerminalRunner::with_base_release_failure();
    let executor = InlineRunnerExecutor;
    plant_bound_mini1_ready(&state, vec!["darwin-arm64".into(), "agent:codex".into()]);
    state.inject_write_failure_once(ClientStateWritePoint::BeforeSubmissionReport);
    state.inject_task_rollback_update_failures(2);

    let error = TaskClient::new(&runner, &config, &paths, &state, &executor)
        .submit(
            TaskSubmitRequest {
                agent: AgentKind::Codex,
                model: None,
                effort: None,
                prompt: "make the change".into(),
                project: repo.root().to_path_buf(),
                base: "main".into(),
                wip: true,
                source: Some("local".into()),
                publish: Some(vec!["fetch".into()]),
                publish_branch: None,
                cli_includes: Vec::new(),
                limits: TaskLimits::default(),
                close_policy: ClosePolicy::Never,
                env_profile: None,
                preference: WorkerPreference::Pinned {
                    worker: "mini-1".into(),
                },
                wait_for_capacity: true,
                attached: false,
                run_id: None,
            },
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap_err();
    assert!(error.to_string().contains("before submission report"));

    let pending = state.list_tasks().unwrap().pop().unwrap();
    let task_id = pending.meta().task_id();
    assert!(pending.submission_intent_turn_id().is_some());
    let first = TaskClient::new(&runner, &config, &paths, &state, &executor)
        .reconcile_runners()
        .unwrap();
    assert_eq!(first.started_runners(), 0);
    let retained = state.load_task(task_id).unwrap();
    assert!(retained.submission_intent_turn_id().is_some());
    assert!(retained.runner().is_none());
    let entry = state.queue_entry_for_task_turn(task_id).unwrap().unwrap();
    assert!(state.read_turn_prompt(task_id, entry.job_id()).is_ok());
    assert!(!runner.requests().iter().any(|request| {
        request
            .args
            .iter()
            .any(|argument| argument == HostOperation::TaskTurn.command())
    }));

    TaskClient::new(&runner, &config, &paths, &state, &executor)
        .reconcile_runners()
        .unwrap();
    assert!(state.list_tasks().unwrap().is_empty());
    assert!(state.queue_snapshot().unwrap().entries().is_empty());
}

#[test]
fn submit_retries_rollback_after_marker_update_failure() {
    assert_submit_rolls_back_post_create_state(
        ClientStateWritePoint::BeforeTaskRollbackUpdate,
        "before task rollback update",
        true,
        false,
        false,
    );
}

#[test]
fn restart_recovers_prompt_failure_when_every_rollback_marker_write_fails() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    assert!(
        repo.git(&[
            "remote",
            "add",
            "origin",
            "https://github.com/example/project.git",
        ])
        .status
        .success()
    );
    let _current_dir = CurrentDirGuard::enter(repo.root());
    let root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
    let state = ClientStateStore::open(&paths.state).unwrap();
    let run_id = RunId::generate();
    state
        .create_run(RunRecord::new(run_id, None, vec![TaskId::generate()], 1, 1).unwrap())
        .unwrap();
    let config = Config::parse(
        "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap();
    let runner = AcceptedThenTerminalRunner::new();
    let executor = InlineRunnerExecutor;
    plant_bound_mini1_ready(
        &state,
        vec![
            "darwin-arm64".into(),
            "origin:github.com".into(),
            "agent:codex".into(),
        ],
    );
    state.inject_write_failure_once(ClientStateWritePoint::BeforeTurnPromptWrite);
    state.inject_task_rollback_update_failures(2);

    let error = TaskClient::new(&runner, &config, &paths, &state, &executor)
        .submit(
            TaskSubmitRequest {
                agent: AgentKind::Codex,
                model: None,
                effort: None,
                prompt: "make the change".into(),
                project: repo.root().to_path_buf(),
                base: "main".into(),
                wip: false,
                source: Some("local".into()),
                publish: Some(vec!["fetch".into(), "push".into()]),
                publish_branch: Some("rollback-test".into()),
                cli_includes: Vec::new(),
                limits: TaskLimits::default(),
                close_policy: ClosePolicy::Never,
                env_profile: None,
                preference: WorkerPreference::Pinned {
                    worker: "mini-1".into(),
                },
                wait_for_capacity: true,
                attached: false,
                run_id: Some(run_id),
            },
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap_err();
    assert!(error.to_string().contains("before task turn prompt write"));

    let pending = state.list_tasks().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].status().state(), TaskState::Queued);
    let task_id = pending[0].meta().task_id();

    let reopened = ClientStateStore::open(&paths.state).unwrap();
    TaskClient::new(&runner, &config, &paths, &reopened, &executor)
        .reconcile_runners()
        .unwrap();
    assert!(reopened.list_tasks().unwrap().is_empty());
    assert!(reopened.queue_snapshot().unwrap().entries().is_empty());
    assert!(
        reopened
            .load_run(run_id)
            .unwrap()
            .publish_branches()
            .is_empty()
    );
    let turn_entries = std::fs::read_dir(paths.state.join("turns"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(turn_entries, vec![".mac-worker-rooted-fs"]);
    let project = mac_worker::project_state::ProjectState::load(&runner, repo.root(), &[]).unwrap();
    let transfer = TransferRepo::open_or_create(&paths.cache, &project.context.common_dir).unwrap();
    assert!(!transfer.has_ref(&format!("refs/mac-worker/bases/{task_id}")));
}

#[test]
fn submit_preserves_state_when_detached_child_adopts_before_handoff_failure() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    let _current_dir = CurrentDirGuard::enter(repo.root());
    let state_root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
    let state = ClientStateStore::open(&paths.state).unwrap();
    let config = Config::parse(
        "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap();
    let runner = AcceptedThenTerminalRunner::new();
    let executor = ChildAdoptsThenFails {
        state: state.clone(),
    };

    let error = TaskClient::new(&runner, &config, &paths, &state, &executor)
        .submit(
            TaskSubmitRequest {
                agent: AgentKind::Codex,
                model: None,
                effort: None,
                prompt: "make the change".into(),
                project: repo.root().to_path_buf(),
                base: "main".into(),
                wip: true,
                source: None,
                publish: None,
                publish_branch: None,
                cli_includes: Vec::new(),
                limits: TaskLimits::default(),
                close_policy: ClosePolicy::Never,
                env_profile: None,
                preference: WorkerPreference::Pinned {
                    worker: "mini-1".into(),
                },
                wait_for_capacity: true,
                attached: false,
                run_id: None,
            },
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("simulated parent handoff failure")
    );
    let tasks = state.list_tasks().unwrap();
    assert_eq!(tasks.len(), 1);
    let task_id = tasks[0].meta().task_id();
    let entry = state.queue_entry_for_task_turn(task_id).unwrap().unwrap();
    assert!(entry.owner_opt().is_some());
    assert!(state.read_turn_prompt(task_id, entry.job_id()).is_ok());
    let project =
        mac_worker::project_state::ProjectState::load(&SystemProcessRunner, repo.root(), &[])
            .unwrap();
    let transfer = TransferRepo::open_or_create(&paths.cache, &project.context.common_dir).unwrap();
    assert!(transfer.has_ref(&format!("refs/mac-worker/bases/{task_id}")));
}

struct ProbeFailingRunner;

impl ProcessRunner for ProbeFailingRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let _ = request;
        Err(WorkerError::Unavailable(
            "REFRESH_FACTS_FAILED: worker mini-1 is unreachable".into(),
        ))
    }
}

#[test]
fn runner_that_exits_early_writes_one_diagnostic_line_and_no_secret() {
    let state_root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
    let state = ClientStateStore::open(&paths.state).unwrap();
    let config = Config::parse(
        "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap();
    let task_id = TaskId::generate();
    let turn_id = TurnId::generate();
    let owner = InlineRunnerExecutor
        .start(&paths, task_id, turn_id)
        .unwrap()
        .process_identity();
    let secret = "deadbeefdeadbeefdeadbeefdeadbeef";
    let prompt = format!("do not leak {secret} or /Users/alice/.ssh/id_rsa");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let meta = TaskMeta::new(TaskMetaInput {
        task_id,
        run_id: None,
        project_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        worktree_id: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
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
        git_identity: GitIdentity::new("mac-worker", "mac-worker@example.test").unwrap(),
        title: None,
        prompt: prompt.clone(),
        created_at_millis: now,
    })
    .unwrap();
    let status = TaskStatus::new(
        TaskState::Queued,
        None,
        None,
        false,
        Some(meta.base_oid().clone()),
        None,
        Vec::new(),
        Vec::new(),
        None,
        Vec::new(),
        now,
    )
    .unwrap();
    state
        .create_task(
            LocalTaskRecord::new(
                meta,
                status,
                None,
                None,
                None,
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".into(),
                None,
                false,
                None,
            )
            .unwrap(),
        )
        .unwrap();
    state.write_turn_prompt(task_id, turn_id, &prompt).unwrap();
    plant_bound_mini1_ready(&state, vec!["darwin-arm64".into()]);
    state
        .enqueue(
            QueueEntry::new(
                turn_id,
                state.client_id(),
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
                CommandSummary::argv(1).unwrap(),
                vec!["agent:codex".into()],
                WorkerPreference::Automatic,
                QueueEntryKind::TaskTurn,
                None,
                owner,
                now,
            )
            .unwrap(),
        )
        .unwrap();

    let error = TurnRunner::new(
        &ProbeFailingRunner,
        &config,
        &paths,
        &state,
        &InlineRunnerExecutor,
    )
    .run(task_id, turn_id, Some(&mut Vec::new()))
    .unwrap_err();
    assert_eq!(error.public_code(), "CAPACITY_BUSY");

    let log = String::from_utf8(
        fs::read(support::task_harness::runner_log(
            state_root.path(),
            &task_id.to_string(),
            &turn_id.to_string(),
        ))
        .unwrap(),
    )
    .unwrap();
    let lines: Vec<_> = log.lines().filter(|line| !line.is_empty()).collect();
    assert_eq!(lines, ["exited: CAPACITY_BUSY workers=mini-1"]);
    assert!(!log.contains(secret));
    assert!(!log.contains("/Users/alice"));
    assert!(!log.contains("id_rsa"));
}

fn fixture_terminal_job(
    requests: &[ProcessRequest],
    query: &ProcessRequest,
    stdout: u64,
    stderr: u64,
) -> Result<ProcessResult, WorkerError> {
    let query: StatusRequest = decode_request(query)?;
    let material = if let Some(request) = requests.iter().rev().find(|r| {
        r.args
            .last()
            .is_some_and(|a| a == HostOperation::TaskTurn.command())
    }) {
        let turn: TaskTurnRequest = decode_request(request)?;
        turn.submit().material().clone()
    } else {
        // This fixture models an already-completed task returned by prepare.
        let request = requests
            .iter()
            .rev()
            .find(|r| {
                r.args
                    .last()
                    .is_some_and(|a| a == HostOperation::LeaseAcquire.command())
            })
            .unwrap();
        let acquire: LeaseAcquireRequest = decode_request(request)?;
        acquire.material().clone()
    };
    let meta = JobMeta::new(&material, material.fingerprint())?;
    assert_eq!(query.job_id(), meta.job_id());
    canonical_process(&StatusResponse::new(
        meta,
        JobStatus::new(
            JobState::Succeeded,
            material.created_at_millis() + 2,
            None,
            None,
            None,
            None,
            Some(0),
            None,
            Some(stdout),
            Some(stderr),
            None,
            None,
        )?,
    )?)
}

#[test]
fn completed_retry_only_retries_local_cleanup() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    *fixture.runner.base_release_failures.lock().unwrap() = 1;
    assert!(fixture.run(&mut Vec::new()).is_err());
    assert!(
        fixture
            .state
            .queue_entry(fixture.turn_id)
            .unwrap()
            .is_some()
    );
    let before = fixture.runner_log();
    let count = fixture.runner.requests().len();
    fixture.run(&mut Vec::new()).unwrap();
    assert_eq!(fixture.runner_log(), before);
    assert!(
        fixture
            .state
            .queue_entry(fixture.turn_id)
            .unwrap()
            .is_none()
    );
    assert!(
        fixture.runner.requests()[count..]
            .iter()
            .all(|r| r.program != OsStr::new("/usr/bin/ssh")),
        "completed retry must not contact remote"
    );
}

#[test]
fn detached_executor_rejects_a_substituted_log_without_changing_its_target() {
    use std::os::unix::fs::symlink;
    let root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
    let store = ClientStateStore::open(&paths.state).unwrap();
    let task = TaskId::generate();
    let turn = TurnId::generate();
    store.open_runner_log(task, turn).unwrap();
    let log = paths
        .state
        .join("runners")
        .join(task.to_string())
        .join(format!("{turn}.log"));
    let target = root.path().join("target");
    fs::write(&target, b"untouched").unwrap();
    fs::set_permissions(&target, fs::Permissions::from_mode(0o640)).unwrap();
    fs::remove_file(&log).unwrap();
    symlink(&target, &log).unwrap();
    assert!(DetachedRunnerExecutor.start(&paths, task, turn).is_err());
    assert_eq!(
        fs::metadata(&target).unwrap().permissions().mode() & 0o777,
        0o640
    );
    assert_eq!(fs::read(target).unwrap(), b"untouched");
}

#[test]
fn invalid_terminal_identity_or_changed_final_lengths_keep_recovery_ownership() {
    struct CorruptStatus<'a> {
        inner: ReplayableLogsRunner<'a>,
        calls: Mutex<u32>,
        change_length: bool,
    }
    impl ProcessRunner for CorruptStatus<'_> {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            let mut response = self.inner.run(request)?;
            if request
                .args
                .last()
                .is_some_and(|a| a == HostOperation::Status.command())
            {
                let mut calls = self.calls.lock().unwrap();
                *calls += 1;
                let mut value: serde_json::Value =
                    serde_json::from_slice(&response.stdout).unwrap();
                if self.change_length {
                    if *calls >= 4 {
                        value["status"]["final_stdout_bytes"] = 150_012.into();
                    }
                } else {
                    value["meta"]["worker_name"] = "other-worker".into();
                }
                response.stdout = serde_json::to_vec(&value).unwrap();
            }
            Ok(response)
        }
    }
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    for change_length in [false, true] {
        let fixture = AcceptedThenTerminalFixture::new();
        let remote = CorruptStatus {
            inner: ReplayableLogsRunner::new(&fixture.runner),
            calls: Mutex::new(0),
            change_length,
        };
        assert!(
            TurnRunner::new(
                &remote,
                &fixture.config,
                &fixture.paths,
                &fixture.state,
                &fixture.executor
            )
            .run(fixture.task_id, fixture.turn_id, None)
            .is_err()
        );
        assert!(
            fixture
                .state
                .queue_entry(fixture.turn_id)
                .unwrap()
                .is_some()
        );
        let path = fixture
            .paths
            .state
            .join("runners")
            .join(fixture.task_id.to_string())
            .join(format!("{}.checkpoint.json", fixture.turn_id));
        let checkpoint: serde_json::Value =
            serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        assert!(checkpoint["committed"]["completion"].is_null());
    }
}

struct FenceRelease(Option<mpsc::Sender<()>>);
impl FenceRelease {
    fn release(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}
impl Drop for FenceRelease {
    fn drop(&mut self) {
        self.release();
    }
}

struct ContentionHook {
    entered: Mutex<Option<mpsc::Sender<()>>>,
    resume: Mutex<mpsc::Receiver<()>>,
}
impl ClientStateConcurrencyHook for ContentionHook {
    fn reach(&self, point: ClientStateConcurrencyPoint) {
        if point == ClientStateConcurrencyPoint::RunnerLogContention
            && let Some(sender) = self.entered.lock().unwrap().take()
        {
            sender.send(()).unwrap();
            self.resume
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(20))
                .expect("release contending runner");
        }
    }
}

struct RefreshFenceRemote<'a> {
    inner: &'a AcceptedThenTerminalRunner,
    status: TaskStatus,
    entered: Mutex<Option<mpsc::Sender<()>>>,
    resume: Mutex<mpsc::Receiver<()>>,
}
impl ProcessRunner for RefreshFenceRemote<'_> {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if thread::current().name() == Some("fenced-refresh")
            && request
                .args
                .last()
                .is_some_and(|arg| arg == HostOperation::TaskStatus.command())
        {
            if let Some(sender) = self.entered.lock().unwrap().take() {
                sender.send(()).unwrap();
                self.resume
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(20))
                    .expect("release fenced refresh");
            }
            return canonical_process(&TaskStatusResponse::new(self.status.clone()));
        }
        self.inner.run(request)
    }
}

fn runner_refresh_contention(ownership_change: Option<bool>) {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let (refresh_tx, refresh_rx) = mpsc::channel();
    let (release_refresh_tx, release_refresh_rx) = mpsc::channel();
    let (contended_tx, contended_rx) = mpsc::channel();
    let (release_runner_tx, release_runner_rx) = mpsc::channel();
    let state = ClientStateStore::open_with_concurrency_hook(
        &fixture.paths.state,
        Arc::new(ContentionHook {
            entered: Mutex::new(Some(contended_tx)),
            resume: Mutex::new(release_runner_rx),
        }),
    )
    .unwrap();
    let record = state.load_task(fixture.task_id).unwrap();
    // A waiting task with a known worker is eligible for queued status refresh.
    let status = TaskStatus::new(
        TaskState::Queued,
        None,
        Some("mini-1".into()),
        false,
        record.status().head_oid().cloned(),
        None,
        vec![],
        vec![],
        None,
        vec![],
        record.status().updated_at_millis(),
    )
    .unwrap();
    state
        .update_task(record.with_status(status.clone()).unwrap())
        .unwrap();
    let remote = RefreshFenceRemote {
        inner: &fixture.runner,
        status,
        entered: Mutex::new(Some(refresh_tx)),
        resume: Mutex::new(release_refresh_rx),
    };
    thread::scope(|scope| {
        let mut release_refresh = FenceRelease(Some(release_refresh_tx));
        let mut release_runner = FenceRelease(Some(release_runner_tx));
        let refresh = thread::Builder::new()
            .name("fenced-refresh".into())
            .spawn_scoped(scope, || {
                TaskClient::new(
                    &remote,
                    &fixture.config,
                    &fixture.paths,
                    &state,
                    &fixture.executor,
                )
                .reconcile_runners()
            })
            .unwrap();
        refresh_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("refresh owns journal");
        let runner = scope.spawn(|| {
            TurnRunner::new(
                &remote,
                &fixture.config,
                &fixture.paths,
                &state,
                &fixture.executor,
            )
            .run(fixture.task_id, fixture.turn_id, None)
        });
        contended_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("runner claimed and encountered refresh fence");
        assert!(matches!(
            state.queue_entry(fixture.turn_id).unwrap().unwrap().state(),
            mac_worker::job::QueueState::Dispatching { .. }
        ));
        release_refresh.release();
        refresh.join().unwrap().unwrap();
        let replacement_owner = ProcessIdentity::new(999_997, 997).unwrap();
        if let Some(retire) = ownership_change {
            if retire {
                let entry = state.queue_entry(fixture.turn_id).unwrap().unwrap();
                state
                    .remove_task_turn_after_terminal(fixture.turn_id, *entry.owner_opt().unwrap())
                    .unwrap();
                let replacement_turn = TurnId::generate();
                state
                    .write_turn_prompt(fixture.task_id, replacement_turn, "new turn")
                    .unwrap();
                state
                    .enqueue(
                        QueueEntry::new(
                            replacement_turn,
                            state.client_id(),
                            entry.project_id().into(),
                            entry.worktree_id().into(),
                            CommandSummary::argv(1).unwrap(),
                            vec![],
                            WorkerPreference::Pinned {
                                worker: "mini-1".into(),
                            },
                            QueueEntryKind::TaskTurn,
                            None,
                            replacement_owner,
                            10,
                        )
                        .unwrap(),
                    )
                    .unwrap();
            } else {
                state.adopt_row(fixture.turn_id, replacement_owner).unwrap();
            }
            state
                .record_runner(
                    fixture.task_id,
                    Some(mac_worker::task::RunnerIdentity::new(replacement_owner)),
                )
                .unwrap();
        }
        release_runner.release();
        let result = runner.join().unwrap();
        if ownership_change.is_some() {
            assert_eq!(result.unwrap_err().public_code(), "TASK_BUSY");
            assert_eq!(
                state
                    .load_task(fixture.task_id)
                    .unwrap()
                    .runner()
                    .unwrap()
                    .process_identity(),
                replacement_owner
            );
            assert_eq!(
                state
                    .queue_entry_for_task_turn(fixture.task_id)
                    .unwrap()
                    .unwrap()
                    .owner_opt(),
                Some(&replacement_owner)
            );
        } else {
            result.expect("legitimate runner must survive transient fence contention");
        }
    });
    assert_eq!(
        fixture
            .runner
            .requests()
            .iter()
            .filter(|request| request
                .args
                .last()
                .is_some_and(|arg| arg == HostOperation::TaskTurn.command()))
            .count(),
        usize::from(ownership_change.is_none())
    );
    if ownership_change.is_none() {
        assert!(state.queue_entry(fixture.turn_id).unwrap().is_none());
    }
}

#[test]
fn runner_retries_a_journal_fence_held_by_queued_refresh() {
    runner_refresh_contention(None);
}
#[test]
fn contending_runner_stops_when_its_owner_changes() {
    runner_refresh_contention(Some(false));
}
#[test]
fn contending_runner_cannot_mutate_a_replacement_turn() {
    runner_refresh_contention(Some(true));
}

struct DelayedProjectionRemote<'a> {
    inner: &'a AcceptedThenTerminalRunner,
    accepted: Mutex<Option<mpsc::Sender<()>>>,
    drain: Mutex<mpsc::Receiver<()>>,
    response: Mutex<Option<mpsc::Sender<()>>>,
    release_response: Mutex<mpsc::Receiver<()>>,
    cancelled: AtomicBool,
    delay_refresh: bool,
}
impl DelayedProjectionRemote<'_> {
    fn terminal(&self) -> TaskStatus {
        let task = self.inner.task.lock().unwrap();
        let (meta, turn) = task.as_ref().unwrap();
        let cancelled = self.cancelled.load(Ordering::SeqCst);
        let outcome = if cancelled {
            TaskOutcome::Cancelled
        } else {
            TaskOutcome::Done
        };
        TaskStatus::new(
            TaskState::Open,
            Some(outcome.clone()),
            Some("mini-1".into()),
            true,
            Some(meta.base_oid().clone()),
            Some("published turn".into()),
            vec![],
            vec![],
            None,
            vec![TurnSummary::new(
                1,
                *turn,
                Some(if cancelled {
                    TurnTerminal::Cancelled
                } else {
                    TurnTerminal::Succeeded
                }),
                Some(outcome),
                Some(true),
                false,
                Some(meta.created_at_millis()),
                Some(meta.created_at_millis() + 1),
            )],
            meta.created_at_millis() + 1,
        )
        .unwrap()
    }
    fn delay_response(&self) {
        if let Some(sender) = self.response.lock().unwrap().take() {
            sender.send(()).unwrap();
            self.release_response
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(20))
                .expect("release delayed projection");
        }
    }
}
impl ProcessRunner for DelayedProjectionRemote<'_> {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let operation = request.args.last().and_then(|arg| arg.to_str());
        if operation == Some(HostOperation::TaskCancel.command()) {
            self.cancelled.store(true, Ordering::SeqCst);
            let response = mac_worker::task_store::TaskCancelResponse::new(self.terminal());
            self.delay_response();
            return canonical_process(&response);
        }
        if operation == Some(HostOperation::TaskStatus.command()) {
            if !*self.inner.turn_submitted.lock().unwrap() {
                return self.inner.run(request);
            }
            if thread::current().name() == Some("finishing-turn")
                && let Some(sender) = self.accepted.lock().unwrap().take()
            {
                sender.send(()).unwrap();
                self.drain
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(20))
                    .expect("release accepted turn drain");
            }
            let response = TaskStatusResponse::new(self.terminal());
            if self.delay_refresh && thread::current().name() == Some("delayed-projection") {
                self.delay_response();
            }
            return canonical_process(&response);
        }
        self.inner.run(request)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ProjectionCase {
    CancelSuccessor,
    CancelCompleted,
    IdleSuccessor,
    CancelResponsive,
    IdleWriteFailure,
    IdleCorruption,
}

fn delayed_task_projection_cannot_overwrite_publication(case: ProjectionCase) {
    let refresh = matches!(
        case,
        ProjectionCase::IdleSuccessor
            | ProjectionCase::IdleWriteFailure
            | ProjectionCase::IdleCorruption
    );
    let successor = matches!(
        case,
        ProjectionCase::CancelSuccessor | ProjectionCase::IdleSuccessor
    );
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let (accepted_tx, accepted_rx) = mpsc::channel();
    let (drain_tx, drain_rx) = mpsc::channel();
    let (response_tx, response_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let remote = DelayedProjectionRemote {
        inner: &fixture.runner,
        accepted: Mutex::new((!refresh).then_some(accepted_tx)),
        drain: Mutex::new(drain_rx),
        response: Mutex::new(Some(response_tx)),
        release_response: Mutex::new(release_rx),
        cancelled: AtomicBool::new(false),
        delay_refresh: refresh,
    };
    let client = TaskClient::new(
        &remote,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    );
    thread::scope(|scope| {
        let mut release_drain = FenceRelease(Some(drain_tx));
        let mut release_response = FenceRelease(Some(release_tx));
        let runner = thread::Builder::new()
            .name("finishing-turn".into())
            .spawn_scoped(scope, || {
                TurnRunner::new(
                    &remote,
                    &fixture.config,
                    &fixture.paths,
                    &fixture.state,
                    &fixture.executor,
                )
                .run(fixture.task_id, fixture.turn_id, None)
            })
            .unwrap();
        let delayed = if refresh {
            runner.join().unwrap().unwrap();
            thread::Builder::new()
                .name("delayed-projection".into())
                .spawn_scoped(scope, || client.reconcile_runners().map(|_| ()))
                .unwrap()
        } else {
            accepted_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("A accepted before cancellation");
            let cancel = thread::Builder::new()
                .name("delayed-projection".into())
                .spawn_scoped(scope, || client.cancel(fixture.task_id).map(|_| ()))
                .unwrap();
            response_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("remote cancellation applied; response delayed");
            if case == ProjectionCase::CancelResponsive {
                release_response.release();
                cancel.join().unwrap().unwrap();
                assert_eq!(
                    fixture
                        .state
                        .load_task(fixture.task_id)
                        .unwrap()
                        .status()
                        .last_outcome(),
                    Some(&TaskOutcome::Cancelled)
                );
                assert!(
                    fixture
                        .state
                        .queue_entry(fixture.turn_id)
                        .unwrap()
                        .is_some()
                );
                release_drain.release();
                runner.join().unwrap().unwrap();
                assert!(
                    fixture
                        .state
                        .load_task(fixture.task_id)
                        .unwrap()
                        .fetched_head()
                        .is_some()
                );
                return;
            }
            release_drain.release();
            runner.join().unwrap().unwrap();
            cancel
        };
        if refresh {
            response_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("idle refresh response delayed");
        }
        let completed = fixture.state.load_task(fixture.task_id).unwrap();
        assert!(
            completed.fetched_head().is_some(),
            "A published its fetched head"
        );
        assert!(completed.runner().is_none());
        assert!(
            fixture
                .state
                .queue_entry(fixture.turn_id)
                .unwrap()
                .is_none()
        );
        let old_log = fixture.runner_log();
        let checkpoint_path = fixture
            .paths
            .state
            .join("runners")
            .join(fixture.task_id.to_string())
            .join(format!("{}.checkpoint.json", fixture.turn_id));
        let checkpoint = fs::read(&checkpoint_path).unwrap();
        assert!(!serde_json::from_slice::<serde_json::Value>(&checkpoint).unwrap()["committed"]["completion"].is_null());
        if successor {
            client
                .say(
                    fixture.task_id,
                    "next turn".into(),
                    false,
                    &mut vec![],
                    &mut vec![],
                )
                .unwrap();
        }
        let expected = fixture.state.load_task(fixture.task_id).unwrap();
        let expected_row = fixture
            .state
            .queue_entry_for_task_turn(fixture.task_id)
            .unwrap();
        if successor {
            assert_eq!(expected.status().turns().len(), 2);
            assert!(expected.runner().is_some());
            assert_ne!(expected_row.as_ref().unwrap().job_id(), fixture.turn_id);
        }
        if case == ProjectionCase::IdleWriteFailure {
            fixture
                .state
                .inject_write_failure_once(ClientStateWritePoint::BeforeTaskRollbackUpdate);
        }
        let task_path = fixture
            .paths
            .state
            .join("tasks")
            .join(format!("{}.json", fixture.task_id));
        if case == ProjectionCase::IdleCorruption {
            fs::write(&task_path, b"corrupt").unwrap();
        }
        release_response.release();
        let result = delayed.join().unwrap();
        if case == ProjectionCase::IdleCorruption {
            assert!(
                matches!(result, Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::InvalidData)
            );
            assert_eq!(fs::read(task_path).unwrap(), b"corrupt");
            return;
        }
        if case == ProjectionCase::IdleWriteFailure {
            assert_eq!(result.unwrap_err().public_code(), "IO");
        } else {
            result.unwrap();
        }
        let actual = fixture.state.load_task(fixture.task_id).unwrap();
        assert_eq!(
            actual.status().turns(),
            expected.status().turns(),
            "late response overwrote turn history"
        );
        assert_eq!(
            actual.runner(),
            expected.runner(),
            "late response overwrote runner identity"
        );
        assert_eq!(
            actual.fetched_head(),
            expected.fetched_head(),
            "late response lost fetched head"
        );
        assert_eq!(
            fixture
                .state
                .queue_entry_for_task_turn(fixture.task_id)
                .unwrap(),
            expected_row,
            "late response changed queue ownership"
        );
        assert_eq!(fixture.runner_log(), old_log);
        assert_eq!(fs::read(checkpoint_path).unwrap(), checkpoint);
    });
}

#[test]
fn delayed_accepted_cancel_preserves_a_successor_turn() {
    delayed_task_projection_cannot_overwrite_publication(ProjectionCase::CancelSuccessor);
}
#[test]
fn delayed_accepted_cancel_preserves_completed_publication() {
    delayed_task_projection_cannot_overwrite_publication(ProjectionCase::CancelCompleted);
}
#[test]
fn delayed_idle_refresh_preserves_a_successor_turn() {
    delayed_task_projection_cannot_overwrite_publication(ProjectionCase::IdleSuccessor);
}

#[test]
fn accepted_cancel_projects_without_waiting_for_the_drain_fence() {
    delayed_task_projection_cannot_overwrite_publication(ProjectionCase::CancelResponsive);
}
#[test]
fn delayed_idle_refresh_propagates_local_write_failure() {
    delayed_task_projection_cannot_overwrite_publication(ProjectionCase::IdleWriteFailure);
}
#[test]
fn delayed_idle_refresh_propagates_current_record_corruption() {
    delayed_task_projection_cannot_overwrite_publication(ProjectionCase::IdleCorruption);
}

// --- laptop herdr notifications ------------------------------------------

fn runner_with_notifier<'a>(
    fixture: &'a AcceptedThenTerminalFixture,
    config: &'a mac_worker::config::Config,
    socket: Option<HerdrSocket>,
) -> TurnRunner<'a> {
    TurnRunner::new(
        &fixture.runner,
        config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .with_notifier(socket)
}

#[test]
fn a_finished_turn_notifies_the_configured_herdr_session_once() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let home = tempfile::tempdir().unwrap();
    let server = support::fake_herdr::FakeHerdr::start_in_home(home.path());

    let outcome = runner_with_notifier(
        &fixture,
        &fixture.config,
        Some(HerdrSocket::at(server.path())),
    )
    .run(fixture.task_id, fixture.turn_id, Some(&mut Vec::new()))
    .unwrap();

    assert_eq!(outcome.status().state(), TaskState::Closed);
    let requests = server.requests();
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert_eq!(requests[0]["method"], "notification.show");
    let params = &requests[0]["params"];
    let short = &fixture.task_id.to_string()[..12];
    assert_eq!(params["title"], format!("task {short}: done"));
    assert_eq!(params["sound"], "done");
    let body = params["body"].as_str().unwrap_or_default();
    assert!(body.starts_with("make the change"), "{body}");
    for forbidden in ["/Users/", "/private/", "/var/", "/tmp/"] {
        assert!(!body.contains(forbidden), "{body}");
    }
}

#[test]
fn notifications_stay_silent_when_turned_off_in_the_configuration() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let home = tempfile::tempdir().unwrap();
    let server = support::fake_herdr::FakeHerdr::start_in_home(home.path());
    let mut config = fixture.config.clone();
    config.notifications.herdr = false;

    let outcome = runner_with_notifier(&fixture, &config, Some(HerdrSocket::at(server.path())))
        .run(fixture.task_id, fixture.turn_id, Some(&mut Vec::new()))
        .unwrap();

    assert_eq!(outcome.status().state(), TaskState::Closed);
    assert!(server.requests().is_empty(), "{:?}", server.requests());
}

#[test]
fn a_missing_herdr_session_changes_nothing_about_the_turn() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let empty_home = tempfile::tempdir().unwrap();

    let started = std::time::Instant::now();
    let outcome = runner_with_notifier(
        &fixture,
        &fixture.config,
        Some(HerdrSocket::default_for_home(empty_home.path())),
    )
    .run(fixture.task_id, fixture.turn_id, Some(&mut Vec::new()))
    .unwrap();

    assert_eq!(outcome.status().state(), TaskState::Closed);
    assert_eq!(outcome.status().last_outcome(), Some(&TaskOutcome::Done));
    assert!(started.elapsed() < std::time::Duration::from_secs(20));
}

#[test]
fn a_runner_without_an_injected_session_never_opens_a_socket() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let outcome = fixture.run(&mut Vec::new()).unwrap();
    assert_eq!(outcome.status().state(), TaskState::Closed);
}

#[test]
fn the_early_exit_line_names_the_error_that_ended_the_runner() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    // A task recorded without its project context makes the runner resolve
    // the project from its working directory.  From another repository it
    // must refuse with PROJECT_MISMATCH, and the log must name that error
    // instead of hiding it behind the queue's blocking code.
    let project_context = fixture
        .state_root
        .path()
        .join("state")
        .join("turns")
        .join(fixture.task_id.to_string())
        .join("project.json");
    fs::remove_file(&project_context)
        .unwrap_or_else(|error| panic!("{}: {error}", project_context.display()));
    let elsewhere = support::GitRepo::init();
    elsewhere.write("other.txt", b"other\n");
    elsewhere.commit_all("other");
    let _elsewhere = CurrentDirGuard::enter(elsewhere.root());

    let error = fixture.run(&mut Vec::new()).unwrap_err();

    assert_eq!(error.public_code(), "PROJECT_MISMATCH", "{error}");
    let log = String::from_utf8(fixture.runner_log()).unwrap();
    let line = log.lines().next().unwrap_or_default();
    assert!(line.starts_with("exited: "), "{log}");
    assert!(line.contains("PROJECT_MISMATCH"), "{log}");
    assert!(line.ends_with("workers=mini-1"), "{log}");
}

#[test]
fn a_pinned_submit_refreshes_stale_facts_instead_of_reporting_missing_agents() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    // Facts older than their TTL carry no agent capabilities.  Before the
    // shared admission observer, a pinned submit read that as the worker
    // missing agent:codex and failed with CAPABILITY_MISSING; the operator
    // had to run `worker workers --refresh` by hand.  Now the submit itself
    // refreshes and probes again before deciding.
    let fixture = AcceptedThenTerminalFixture::new_with_runner(
        AcceptedThenTerminalRunner::with_stale_facts(),
        WorkerPreference::Pinned {
            worker: "mini-1".into(),
        },
    );

    assert!(fixture.runner.facts_fresh());
    let requests = fixture.runner.requests();
    let refresh = requests
        .iter()
        .position(|request| {
            request
                .args
                .iter()
                .any(|arg| arg == HostOperation::RefreshFacts.command())
        })
        .expect("the submit refreshed the stale facts");
    let probes = requests
        .iter()
        .enumerate()
        .filter(|(_, request)| {
            request
                .args
                .iter()
                .any(|arg| arg == "~/.local/bin/worker host probe")
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    assert!(
        probes.iter().any(|&index| index < refresh) && probes.iter().any(|&index| index > refresh),
        "probe, refresh, probe again: probes {probes:?}, refresh {refresh}"
    );

    let outcome = fixture.run(&mut Vec::new()).unwrap();
    assert_eq!(outcome.status().state(), TaskState::Closed);
}

#[test]
fn a_pinned_submit_whose_refresh_fails_reports_the_worker_not_its_capabilities() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    // Without a fresh cached observation the submit probes; the probe
    // reports stale facts and the refresh fails.
    fs::remove_file(fixture.paths.state.join("observations/mini-1.json")).unwrap();
    let runner = AdmissionFailureRunner {
        inner: &fixture.runner,
        ssh: "mac1",
        failure: AdmissionFailure::Refresh,
        failed_worker_requests: Mutex::new(Vec::new()),
    };

    let error = TaskClient::new(
        &runner,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .submit(
        submit_request(
            fixture._repo.root(),
            None,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            false,
        ),
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .unwrap_err();

    assert_eq!(error.public_code(), "CAPACITY_BUSY", "{error}");
    assert_unavailable_observation(&fixture, "mini-1");
}

#[test]
fn a_runner_for_a_row_held_by_another_identity_stops_after_the_adoption_wait() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    // A runner started by hand for a waiting row that belongs to another
    // process is not the detached child its parent is about to adopt; it
    // must stop with the owner mismatch instead of sleeping forever, and
    // leave the row exactly as it found it.
    let foreign = ProcessIdentity::new(424_243, 1).unwrap();
    fixture.state.adopt_row(fixture.turn_id, foreign).unwrap();

    let started = std::time::Instant::now();
    let error = TurnRunner::new(
        &fixture.runner,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .with_adoption_wait(std::time::Duration::from_millis(300))
    .run(fixture.task_id, fixture.turn_id, Some(&mut Vec::new()))
    .unwrap_err();

    assert_eq!(error.public_code(), "QUEUE_OWNER_MISMATCH", "{error}");
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    let entry = fixture.state.queue_entry(fixture.turn_id).unwrap().unwrap();
    assert_eq!(entry.owner_opt(), Some(&foreign));
    assert!(matches!(
        entry.state(),
        mac_worker::job::QueueState::Waiting { .. }
    ));
    let log = String::from_utf8(fixture.runner_log()).unwrap();
    let line = log.lines().next().unwrap_or_default();
    assert!(line.contains("QUEUE_OWNER_MISMATCH"), "{log}");
}

fn is_host_probe(request: &ProcessRequest) -> bool {
    request.program == OsStr::new("/usr/bin/ssh")
        && request
            .args
            .last()
            .is_some_and(|argument| argument == "~/.local/bin/worker host probe")
}

fn ssh_destination(request: &ProcessRequest) -> String {
    request
        .args
        .windows(2)
        .find(|window| window[0] == "--")
        .map(|window| window[1].to_string_lossy().into_owned())
        .unwrap_or_default()
}

const ADMISSION_GATE_TIMEOUT: Duration = Duration::from_secs(2);

struct ProbeStartGate {
    state: Arc<(Mutex<ProbeGateState>, std::sync::Condvar)>,
}

struct ProbeGateState {
    started: Vec<String>,
    released: std::collections::HashSet<String>,
    release_all: bool,
}

impl ProbeStartGate {
    fn new() -> Self {
        Self {
            state: Arc::new((
                Mutex::new(ProbeGateState {
                    started: Vec::new(),
                    released: std::collections::HashSet::new(),
                    release_all: false,
                }),
                std::sync::Condvar::new(),
            )),
        }
    }

    fn enter(&self, destination: &str) {
        let (state, changed) = &*self.state;
        let mut state = state.lock().unwrap();
        state.started.push(destination.to_owned());
        changed.notify_all();
        let deadline = Instant::now() + ADMISSION_GATE_TIMEOUT;
        while !state.release_all && !state.released.contains(destination) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                state.release_all = true;
                changed.notify_all();
                return;
            }
            let (next, timeout) = changed.wait_timeout(state, remaining).unwrap();
            state = next;
            if timeout.timed_out() {
                state.release_all = true;
                changed.notify_all();
                return;
            }
        }
    }

    fn wait_for_started(&self, count: usize) {
        let (state, changed) = &*self.state;
        let mut state = state.lock().unwrap();
        let deadline = Instant::now() + ADMISSION_GATE_TIMEOUT;
        while state.started.len() < count {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let started = state.started.clone();
                state.release_all = true;
                changed.notify_all();
                drop(state);
                panic!(
                    "timed out after {ADMISSION_GATE_TIMEOUT:?} waiting for {count} admission probes to start; started {started:?}"
                );
            }
            let (next, timeout) = changed.wait_timeout(state, remaining).unwrap();
            state = next;
            if timeout.timed_out() && state.started.len() < count {
                let started = state.started.clone();
                state.release_all = true;
                changed.notify_all();
                drop(state);
                panic!(
                    "timed out after {ADMISSION_GATE_TIMEOUT:?} waiting for {count} admission probes to start; started {started:?}"
                );
            }
        }
    }

    fn release_all(&self) {
        let (state, changed) = &*self.state;
        state.lock().unwrap().release_all = true;
        changed.notify_all();
    }

    fn started(&self) -> Vec<String> {
        self.state.0.lock().unwrap().started.clone()
    }
}

struct GatedAdmissionRunner<'a> {
    inner: &'a AcceptedThenTerminalRunner,
    gate: ProbeStartGate,
}

impl ProcessRunner for GatedAdmissionRunner<'_> {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if is_host_probe(request) {
            self.gate.enter(&ssh_destination(request));
        }
        self.inner.run(request)
    }
}

/// Break caught: TurnRunner probes every worker before consulting the 2s cache,
/// so a just-written submit observation still causes a second host probe.
#[test]
fn runner_does_not_host_probe_when_submit_cache_is_fresh() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    let probes_after_submit = fixture
        .runner
        .requests()
        .iter()
        .filter(|request| is_host_probe(request))
        .count();
    *fixture.runner.facts_fresh.lock().unwrap() = true;
    fixture.run(&mut Vec::new()).unwrap();
    let probes_after_run = fixture
        .runner
        .requests()
        .iter()
        .filter(|request| is_host_probe(request))
        .count();
    assert_eq!(
        probes_after_run, probes_after_submit,
        "runner added host probes despite a TTL-fresh submit observation; after={probes_after_run} before={probes_after_submit}"
    );
    assert!(
        fixture.runner.requests().iter().any(|request| {
            request
                .args
                .iter()
                .any(|argument| argument == HostOperation::LeaseAcquire.command())
        }),
        "a ready cache hit must still run remote lease/prebind"
    );
}

/// Break caught: TaskClient keys the cache by worker name only, so an SSH
/// destination change under the same name is served from the previous host.
#[test]
fn submit_probes_new_ssh_destination_after_same_name_identity_change() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let mut fixture = AcceptedThenTerminalFixture::new();
    *fixture.runner.facts_fresh.lock().unwrap() = true;
    fixture.config.workers[0].ssh = "mac1-renamed".into();
    let error = TaskClient::new(
        &fixture.runner,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .submit(
        submit_request(
            fixture._repo.root(),
            None,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            false,
        ),
        &mut Vec::new(),
        &mut Vec::new(),
    );
    let _ = error;
    let probed_renamed = fixture
        .runner
        .requests()
        .iter()
        .any(|request| is_host_probe(request) && ssh_destination(request) == "mac1-renamed");
    assert!(
        probed_renamed,
        "same-name SSH identity change must miss the cache and probe the new destination"
    );
}

/// Config::validate requires exactly one slot; invalid local config is a
/// caller error and must not SSH, even with a fresh bound cache.
#[test]
fn submit_rejects_invalid_slot_count_without_probing() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let mut fixture = AcceptedThenTerminalFixture::new();
    *fixture.runner.facts_fresh.lock().unwrap() = true;
    let probes_before = fixture
        .runner
        .requests()
        .iter()
        .filter(|request| is_host_probe(request))
        .count();
    fixture.config.workers[0].slots = 2;
    let error = TaskClient::new(
        &fixture.runner,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .submit(
        submit_request(
            fixture._repo.root(),
            None,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            false,
        ),
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .unwrap_err();
    assert!(
        matches!(error, WorkerError::Config(_)),
        "slots != 1 must fail locally, got {error:?}"
    );
    let probes_after = fixture
        .runner
        .requests()
        .iter()
        .filter(|request| is_host_probe(request))
        .count();
    assert_eq!(
        probes_after, probes_before,
        "invalid local config must not start a host probe; before={probes_before} after={probes_after}"
    );
}

#[test]
fn submit_probes_again_when_declared_capabilities_change() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let mut fixture = AcceptedThenTerminalFixture::new();
    *fixture.runner.facts_fresh.lock().unwrap() = true;
    let probes_before = fixture
        .runner
        .requests()
        .iter()
        .filter(|request| is_host_probe(request))
        .count();
    fixture.config.workers[0].capabilities.push("gpu".into());
    let _ = TaskClient::new(
        &fixture.runner,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .submit(
        submit_request(
            fixture._repo.root(),
            None,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            false,
        ),
        &mut Vec::new(),
        &mut Vec::new(),
    );
    let probes_after = fixture
        .runner
        .requests()
        .iter()
        .filter(|request| is_host_probe(request))
        .count();
    assert!(
        probes_after > probes_before,
        "declared capability change must miss the bound cache; before={probes_before} after={probes_after}"
    );
}

#[test]
fn submit_does_not_reuse_a_future_bound_probe_timestamp() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    *fixture.runner.facts_fresh.lock().unwrap() = true;
    let worker = &fixture.config.workers[0];
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    fixture
        .state
        .publish_admission_observation(
            AdmissionObservation::new(
                worker.name.clone(),
                true,
                mac_worker::scheduler::CandidateSlot::Idle,
                vec!["darwin-arm64".into(), "agent:codex".into()],
                Some(10),
                20,
                now + 60_000,
            )
            .unwrap()
            .with_local_binding(
                worker.ssh.clone(),
                worker.remote_binary.clone(),
                worker.capabilities.clone(),
                worker.slots,
                Some(0),
                now + 60_000,
            ),
        )
        .unwrap();
    let probes_before = fixture
        .runner
        .requests()
        .iter()
        .filter(|request| is_host_probe(request))
        .count();
    let _ = TaskClient::new(
        &fixture.runner,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .submit(
        submit_request(
            fixture._repo.root(),
            None,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            false,
        ),
        &mut Vec::new(),
        &mut Vec::new(),
    );
    let probes_after = fixture
        .runner
        .requests()
        .iter()
        .filter(|request| is_host_probe(request))
        .count();
    assert!(
        probes_after > probes_before,
        "a future bound probe timestamp must not skip SSH; before={probes_before} after={probes_after}"
    );
}

#[test]
fn submit_reuses_a_bound_negative_without_facts() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let fixture = AcceptedThenTerminalFixture::new();
    *fixture.runner.facts_fresh.lock().unwrap() = true;
    let worker = &fixture.config.workers[0];
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    fixture
        .state
        .publish_admission_observation(
            AdmissionObservation::new(
                worker.name.clone(),
                false,
                mac_worker::scheduler::CandidateSlot::Busy,
                Vec::new(),
                None,
                0,
                now,
            )
            .unwrap()
            .with_local_binding(
                worker.ssh.clone(),
                worker.remote_binary.clone(),
                worker.capabilities.clone(),
                worker.slots,
                None,
                now,
            ),
        )
        .unwrap();
    let probes_before = fixture
        .runner
        .requests()
        .iter()
        .filter(|request| is_host_probe(request))
        .count();
    let error = TaskClient::new(
        &fixture.runner,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .submit(
        submit_request(
            fixture._repo.root(),
            None,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            false,
        ),
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .unwrap_err();
    assert_eq!(error.public_code(), "CAPACITY_BUSY");
    let probes_after = fixture
        .runner
        .requests()
        .iter()
        .filter(|request| is_host_probe(request))
        .count();
    assert_eq!(
        probes_after, probes_before,
        "a bound negative within TTL must suppress SSH; before={probes_before} after={probes_after}"
    );
}

#[test]
fn submit_rejects_invalid_transport_on_a_cache_hit_without_probing() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let mut fixture = AcceptedThenTerminalFixture::new();
    *fixture.runner.facts_fresh.lock().unwrap() = true;
    let probes_before = fixture
        .runner
        .requests()
        .iter()
        .filter(|request| is_host_probe(request))
        .count();
    fixture.config.workers[0].remote_binary = "/tmp/worker".into();
    let error = TaskClient::new(
        &fixture.runner,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .submit(
        submit_request(
            fixture._repo.root(),
            None,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            false,
        ),
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .unwrap_err();
    assert_eq!(error.public_code(), "INVALID_REQUEST");
    let probes_after = fixture
        .runner
        .requests()
        .iter()
        .filter(|request| is_host_probe(request))
        .count();
    assert_eq!(
        probes_after, probes_before,
        "invalid local transport must fail before cache or SSH; before={probes_before} after={probes_after}"
    );
}

struct DelayedPeerRunner<'a> {
    inner: &'a AcceptedThenTerminalRunner,
    ssh: &'static str,
    delay: Duration,
}

impl ProcessRunner for DelayedPeerRunner<'_> {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if is_host_probe(request) && ssh_destination(request) == self.ssh {
            thread::sleep(self.delay);
            return Ok(ProcessResult {
                status: ExitStatus::from_raw(255 << 8),
                stdout: Vec::new(),
                stderr: b"offline peer".to_vec(),
            });
        }
        self.inner.run(request)
    }
}

#[test]
fn slow_offline_peer_does_not_erase_healthy_warm_eligibility() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let mut fixture =
        AcceptedThenTerminalFixture::new_with_options(None, WorkerPreference::Automatic, false);
    add_second_worker(&mut fixture, false);
    *fixture.runner.facts_fresh.lock().unwrap() = true;
    let _ = fs::remove_file(fixture.paths.state.join("observations/mini-2.json"));
    let worker = fixture.config.workers[0].clone();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    fixture
        .state
        .publish_admission_observation(
            AdmissionObservation::new(
                worker.name.clone(),
                true,
                mac_worker::scheduler::CandidateSlot::Idle,
                vec![
                    "darwin-arm64".into(),
                    "origin:example.test".into(),
                    "agent:codex".into(),
                ],
                Some(10),
                20,
                now.saturating_sub(1_900),
            )
            .unwrap()
            .with_local_binding(
                worker.ssh.clone(),
                worker.remote_binary.clone(),
                worker.capabilities.clone(),
                worker.slots,
                Some(0),
                now.saturating_sub(1_900),
            ),
        )
        .unwrap();
    let runner = DelayedPeerRunner {
        inner: &fixture.runner,
        ssh: "mac2",
        delay: Duration::from_millis(250),
    };
    TaskClient::new(
        &runner,
        &fixture.config,
        &fixture.paths,
        &fixture.state,
        &fixture.executor,
    )
    .submit(
        submit_request(
            fixture._repo.root(),
            None,
            WorkerPreference::Automatic,
            false,
        ),
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .expect("healthy warm worker must stay eligible while an offline peer is slow");
}

/// Break caught: admission still probes workers sequentially, so four cold
/// workers never have three host probes in flight.
#[test]
fn automatic_cold_admission_runs_three_host_probes_before_the_fourth() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let mut fixture =
        AcceptedThenTerminalFixture::new_with_options(None, WorkerPreference::Automatic, false);
    add_second_worker(&mut fixture, false);
    let mut mini3 = fixture.config.workers[0].clone();
    mini3.name = "mini-3".into();
    mini3.ssh = "mac3".into();
    fixture.config.workers.push(mini3);
    let mut mini4 = fixture.config.workers[0].clone();
    mini4.name = "mini-4".into();
    mini4.ssh = "mac4".into();
    fixture.config.workers.push(mini4);
    *fixture.runner.facts_fresh.lock().unwrap() = true;
    let _ = fs::remove_file(fixture.paths.state.join("observations/mini-1.json"));
    let gate = ProbeStartGate::new();
    let runner = GatedAdmissionRunner {
        inner: &fixture.runner,
        gate: ProbeStartGate {
            state: Arc::clone(&gate.state),
        },
    };
    let submit = thread::scope(|scope| {
        let handle = scope.spawn(|| {
            TaskClient::new(
                &runner,
                &fixture.config,
                &fixture.paths,
                &fixture.state,
                &fixture.executor,
            )
            .submit(
                submit_request(
                    fixture._repo.root(),
                    None,
                    WorkerPreference::Automatic,
                    false,
                ),
                &mut Vec::new(),
                &mut Vec::new(),
            )
        });
        gate.wait_for_started(3);
        let started = gate.started();
        gate.release_all();
        let _ = handle.join().unwrap();
        started
    });
    assert!(
        submit.len() >= 3,
        "expected at least three in-flight admission probes, started {submit:?}"
    );
}
