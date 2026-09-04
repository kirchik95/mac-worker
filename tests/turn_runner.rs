#[allow(dead_code)]
mod support;

use std::{
    ffi::OsStr,
    fs,
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    path::{Path, PathBuf},
    process::{self, ExitStatus},
    sync::Mutex,
};

use mac_worker::{
    agent::AgentKind,
    agent_facts::{AgentAuth, AgentFacts, AgentProbe},
    client_state::ClientStateStore,
    config::Config,
    job::{
        JobMeta, JobStatus, LeaseAcquireRequest, LeaseAcquireResponse, LeaseRecord,
        ProcessIdentity, SubmitResponse,
    },
    lease::SlotState,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    protocol::{CpuCounters, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION},
    scheduler::WorkerPreference,
    task::{
        ClosePolicy, TaskId, TaskLimits, TaskMeta, TaskOutcome, TaskState, TaskStatus, TurnId,
        TurnSummary, TurnTerminal,
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
                    collected_at_millis: 1,
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
    let transfer = TransferRepo::open_or_create(&paths.cache, &project.context.common_dir).unwrap();
    assert!(!transfer.has_ref(&format!("refs/mac-worker/bases/{task_id}")));

    let closed = client.close(task_id, true).unwrap();
    assert_eq!(closed.status().state(), TaskState::Abandoned);
}
