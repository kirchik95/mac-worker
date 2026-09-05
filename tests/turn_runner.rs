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
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use mac_worker::{
    agent::AgentKind,
    agent_facts::{AgentAuth, AgentFacts, AgentProbe, FACTS_TTL, ProfileProbe},
    client_state::{
        ClientStateConcurrencyHook, ClientStateConcurrencyPoint, ClientStateStore,
        ClientStateWritePoint,
    },
    config::Config,
    error::WorkerError,
    job::{
        AdmissionObservation, CommandSummary, JobMeta, JobStatus, LeaseAcquireRequest,
        LeaseAcquireResponse, LeaseRecord, LogChunk, LogChunkRequest, LogStream, ProcessIdentity,
        QueueEntry, QueueEntryKind, SubmitResponse,
    },
    lease::SlotState,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    protocol::{CpuCounters, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION},
    scheduler::WorkerPreference,
    supervisor::{
        ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
    },
    task::{
        ClosePolicy, RunId, RunRecord, TaskId, TaskLimits, TaskMeta, TaskOutcome, TaskState,
        TaskStatus, TurnId, TurnSummary, TurnTerminal,
    },
    task_client::{TaskClient, TaskSubmitRequest},
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
    let paths = support::task_harness::paths(temp.path());
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
    let paths = support::task_harness::paths(temp.path());
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
    base_release_failures: Mutex<u8>,
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
            base_release_failures: Mutex::new(0),
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
            Some("mini-1".into()),
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
        self.facts_fresh() || *count == 1
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
}

impl AcceptedThenTerminalFixture {
    fn new() -> Self {
        Self::new_with_push_origin(None)
    }

    fn new_with_push_origin(origin: Option<&str>) -> Self {
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
        let runner = AcceptedThenTerminalRunner::new();
        let executor = InlineRunnerExecutor;
        let current_dir = CurrentDirGuard::enter(repo.root());
        let report = TaskClient::new(&runner, &config, &paths, &state, &executor)
            .submit(
                TaskSubmitRequest {
                    agent: AgentKind::Codex,
                    model: None,
                    prompt: "make the change".into(),
                    project: repo.root().to_path_buf(),
                    base: "main".into(),
                    wip: origin.is_none(),
                    source: None,
                    publish: origin.map(|_| vec!["fetch".into(), "push".into()]),
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
    let outcome = fixture.run(&mut Vec::new()).unwrap();

    assert_eq!(outcome.status().state(), TaskState::Closed);
    assert!(fixture.runner.facts_fresh());
    assert!(fixture.runner.requests().iter().any(|request| {
        request
            .args
            .iter()
            .any(|arg| arg == HostOperation::RefreshFacts.command())
    }));
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let cached = fixture
        .state
        .admission_observation("mini-1", now, || {
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

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    state
        .admission_observation("mini-1", now, || {
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                mac_worker::scheduler::CandidateSlot::Idle,
                vec![
                    "darwin-arm64".into(),
                    "origin:github.com".into(),
                    "agent:codex".into(),
                ],
                Some(8 * 1024 * 1024 * 1024),
                64 * 1024 * 1024 * 1024,
                now,
            )
        })
        .unwrap();
    if fault == ClientStateWritePoint::AfterTaskSubmissionTurnsRetirement {
        state.inject_write_failure_once(ClientStateWritePoint::BeforeSubmissionReport);
        state.inject_submission_rollback_cleanup_failure_once(fault);
    } else {
        state.inject_write_failure_once(fault);
    }

    let error = client
        .submit(
            TaskSubmitRequest {
                agent: AgentKind::Codex,
                model: None,
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
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    state
        .admission_observation("mini-1", now, || {
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                mac_worker::scheduler::CandidateSlot::Idle,
                vec!["darwin-arm64".into(), "agent:codex".into()],
                Some(8 * 1024 * 1024 * 1024),
                64 * 1024 * 1024 * 1024,
                now,
            )
        })
        .unwrap();
    state.inject_write_failure_once(ClientStateWritePoint::BeforeSubmissionReport);
    state.inject_submission_rollback_cleanup_failure_once(
        ClientStateWritePoint::AfterTaskSubmissionTurnsRetirement,
    );

    let error = TaskClient::new(&runner, &config, &paths, &state, &executor)
        .submit(
            TaskSubmitRequest {
                agent: AgentKind::Codex,
                model: None,
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
        "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 2\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap();
    let runner = AcceptedThenTerminalRunner::new();
    let executor = InlineRunnerExecutor;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    state
        .admission_observation("mini-1", now, || {
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                mac_worker::scheduler::CandidateSlot::Idle,
                vec![
                    "darwin-arm64".into(),
                    "origin:github.com".into(),
                    "agent:codex".into(),
                ],
                Some(8 * 1024 * 1024 * 1024),
                64 * 1024 * 1024 * 1024,
                now,
            )
        })
        .unwrap();
    let request = || TaskSubmitRequest {
        agent: AgentKind::Codex,
        model: None,
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
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    state
        .admission_observation("mini-1", now, || {
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                mac_worker::scheduler::CandidateSlot::Idle,
                vec!["darwin-arm64".into(), "agent:codex".into()],
                Some(8 * 1024 * 1024 * 1024),
                64 * 1024 * 1024 * 1024,
                now,
            )
        })
        .unwrap();
    let request = || TaskSubmitRequest {
        agent: AgentKind::Codex,
        model: None,
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
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    state
        .admission_observation("mini-1", now, || {
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                mac_worker::scheduler::CandidateSlot::Idle,
                vec!["darwin-arm64".into(), "agent:codex".into()],
                Some(8 * 1024 * 1024 * 1024),
                64 * 1024 * 1024 * 1024,
                now,
            )
        })
        .unwrap();
    let request = || TaskSubmitRequest {
        agent: AgentKind::Codex,
        model: None,
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
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    state
        .admission_observation("mini-1", now, || {
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                mac_worker::scheduler::CandidateSlot::Idle,
                vec!["darwin-arm64".into(), "agent:codex".into()],
                Some(8 * 1024 * 1024 * 1024),
                64 * 1024 * 1024 * 1024,
                now,
            )
        })
        .unwrap();
    state.inject_write_failure_once(ClientStateWritePoint::BeforeSubmissionReport);
    state.inject_task_rollback_update_failures(2);

    let error = TaskClient::new(&runner, &config, &paths, &state, &executor)
        .submit(
            TaskSubmitRequest {
                agent: AgentKind::Codex,
                model: None,
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
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    state
        .admission_observation("mini-1", now, || {
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                mac_worker::scheduler::CandidateSlot::Idle,
                vec![
                    "darwin-arm64".into(),
                    "origin:github.com".into(),
                    "agent:codex".into(),
                ],
                Some(8 * 1024 * 1024 * 1024),
                64 * 1024 * 1024 * 1024,
                now,
            )
        })
        .unwrap();
    state.inject_write_failure_once(ClientStateWritePoint::BeforeTurnPromptWrite);
    state.inject_task_rollback_update_failures(2);

    let error = TaskClient::new(&runner, &config, &paths, &state, &executor)
        .submit(
            TaskSubmitRequest {
                agent: AgentKind::Codex,
                model: None,
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
