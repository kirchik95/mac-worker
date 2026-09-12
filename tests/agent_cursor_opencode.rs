#[allow(dead_code)]
mod support;

use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fs,
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::Mutex,
};

use mac_worker::{
    agent::{
        AgentKind, PermissionPolicy, adapter_for, parse_prebind_session_ref, prebind_login_request,
        render_prebind_shell,
    },
    agent_facts::{AgentAuth, AgentFacts, AgentProbe, ProfileProbe},
    client_state::ClientStateStore,
    config::Config,
    error::WorkerError,
    host_store::HostStore,
    job::{
        CommandSpec, HostControlError, JobMeta, JobStatus, LeaseAcquireRequest,
        LeaseAcquireResponse, LeaseRecord, LogChunk, LogChunkRequest, SubmitResponse,
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
        SessionBinding, TaskPrebindRequest, TaskPrepareRequest, TaskPrepareResponse,
        TaskSessionRequest, TaskSessionResponse, TaskStatusRequest, TaskStatusResponse,
    },
    transfer::HostOperation,
    turn::{TaskTurnRequest, TaskTurnResponse, prebind_session},
    turn_runner::InlineRunnerExecutor,
};

static CURRENT_DIR_LOCK: Mutex<()> = Mutex::new(());

const FIRST_PROMPT: &str = "Fix the planted secret";

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

struct HostState {
    calls: Vec<String>,
    session: Option<String>,
    last_shell: String,
    last_prompt: String,
    child_env: BTreeMap<String, String>,
    task: Option<(TaskMeta, TurnId)>,
    turn_submitted: bool,
    native_job: Option<JobMeta>,
    fail_first_turn: bool,
    stream_session: Option<String>,
    profile_home: Option<PathBuf>,
    error_text: String,
}

struct RecordingHost {
    state: Mutex<HostState>,
    agent: AgentKind,
}

impl RecordingHost {
    fn new(agent: AgentKind) -> Self {
        Self {
            state: Mutex::new(HostState {
                calls: Vec::new(),
                session: None,
                last_shell: String::new(),
                last_prompt: String::new(),
                child_env: BTreeMap::new(),
                task: None,
                turn_submitted: false,
                native_job: None,
                fail_first_turn: false,
                stream_session: None,
                profile_home: None,
                error_text: String::new(),
            }),
            agent,
        }
    }

    fn host_calls(&self) -> Vec<String> {
        self.state.lock().unwrap().calls.clone()
    }

    fn persisted_session(&self) -> Option<String> {
        self.state.lock().unwrap().session.clone()
    }

    fn last_shell(&self) -> String {
        self.state.lock().unwrap().last_shell.clone()
    }

    fn last_prompt(&self) -> String {
        self.state.lock().unwrap().last_prompt.clone()
    }

    fn child_env(&self, name: &str) -> Option<String> {
        self.state.lock().unwrap().child_env.get(name).cloned()
    }

    fn error_text(&self) -> String {
        self.state.lock().unwrap().error_text.clone()
    }

    fn set_stream(&self, session: &str) {
        self.state.lock().unwrap().stream_session = Some(session.to_owned());
    }

    fn set_fail_first_turn(&self) {
        self.state.lock().unwrap().fail_first_turn = true;
    }

    fn set_profile_home(&self, home: PathBuf) {
        self.state.lock().unwrap().profile_home = Some(home);
    }

    fn record_error(&self, error: &WorkerError) {
        self.state.lock().unwrap().error_text = error.to_string();
    }

    fn task_status(&self, terminal: bool) -> Result<TaskStatus, WorkerError> {
        let state = self.state.lock().unwrap();
        let (meta, turn_id) = state.task.as_ref().ok_or_else(|| {
            WorkerError::Protocol("task status requested before task preparation".into())
        })?;
        let failed = state.fail_first_turn && state.turn_submitted;
        let outcome = if !terminal {
            None
        } else if failed {
            Some(TaskOutcome::failed("agent exited 1"))
        } else {
            Some(TaskOutcome::Done)
        };
        let turn = TurnSummary::new(
            1,
            *turn_id,
            terminal.then_some(if failed {
                TurnTerminal::Failed
            } else {
                TurnTerminal::Succeeded
            }),
            outcome.clone(),
            terminal.then_some(!failed),
            false,
            Some(meta.created_at_millis()),
            terminal.then_some(meta.created_at_millis() + 1),
        );
        TaskStatus::new(
            if terminal {
                TaskState::Open
            } else {
                TaskState::Active
            },
            outcome,
            Some("mini-1".into()),
            state.session.is_some(),
            Some(meta.base_oid().clone()),
            terminal.then_some("finished".into()),
            Vec::new(),
            Vec::new(),
            None,
            vec![turn],
            meta.created_at_millis() + u64::from(terminal),
        )
    }
}

impl ProcessRunner for RecordingHost {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/git") {
            let is_result_fetch = request.args.iter().any(|arg| arg == "fetch")
                && request.args.iter().any(|arg| {
                    arg.to_string_lossy().contains("refs/mac-worker/results/")
                        || arg.to_string_lossy().starts_with("--upload-pack=")
                });
            if is_result_fetch {
                return Ok(success(Vec::new()));
            }
            let is_result_ref_read = request.args.iter().any(|arg| {
                arg.to_string_lossy().contains("refs/mac-worker/results/")
                    || arg.to_string_lossy().contains("refs/remotes/mac-worker/")
            });
            if request.args.iter().any(|arg| arg == "rev-parse") && is_result_ref_read {
                let base_oid = self
                    .state
                    .lock()
                    .unwrap()
                    .task
                    .as_ref()
                    .expect("result ref read after task preparation")
                    .0
                    .base_oid()
                    .to_string();
                return Ok(success(format!("{base_oid}\n").into_bytes()));
            }
            if request.args.iter().any(|arg| arg == "push")
                && request
                    .args
                    .iter()
                    .any(|arg| arg.to_string_lossy().starts_with("--receive-pack="))
            {
                return Ok(success(Vec::new()));
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
            "~/.local/bin/worker host probe" => canonical_process(&probe_response(self.agent)),
            value if value == HostOperation::RefreshFacts.command() => Ok(success(Vec::new())),
            value if value == HostOperation::LeaseAcquire.command() => {
                let acquire: LeaseAcquireRequest = decode_request(request)?;
                if let CommandSpec::Shell { shell } = acquire.material().command() {
                    self.state.lock().unwrap().last_shell = shell.clone();
                }
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
                self.state.lock().unwrap().task = Some((prepare.meta().clone(), prepare.job_id()));
                canonical_process(&TaskPrepareResponse::new(
                    prepare.meta().base_oid().clone(),
                    false,
                ))
            }
            value if value == HostOperation::TaskPrebind.command() => {
                let prebind: TaskPrebindRequest = decode_request(request)?;
                let mut state = self.state.lock().unwrap();
                if let Some(existing) = state.session.clone() {
                    drop(state);
                    return canonical_process(&TaskSessionResponse::new(SessionBinding::new(
                        self.agent, existing, 1,
                    )?));
                }
                if let Some(session_ref) = prebind.session_ref() {
                    state.session = Some(session_ref.to_owned());
                    let binding = SessionBinding::new(self.agent, session_ref, 1)?;
                    drop(state);
                    return canonical_process(&TaskSessionResponse::new(binding));
                }
                state.calls.push("cursor-agent create-chat".into());
                if let Some(home) = state.profile_home.clone()
                    && let Some(profile) = prebind.env_profile()
                {
                    let path = home
                        .join(".config")
                        .join("mac-worker")
                        .join("env")
                        .join(format!("{profile}.env"));
                    let argv = adapter_for(self.agent)
                        .prebind_session()
                        .expect("cursor prebind argv");
                    let mut extra = Vec::new();
                    let text = fs::read_to_string(&path).unwrap();
                    for line in text.lines() {
                        if let Some((name, value)) = line.split_once('=') {
                            extra.push((OsString::from(name), OsString::from(value)));
                        }
                    }
                    let process = prebind_login_request(&argv, &home, &extra)?;
                    for (name, value) in process.environment {
                        if let (Some(name), Some(value)) = (name.to_str(), value.to_str()) {
                            state.child_env.insert(name.to_owned(), value.to_owned());
                        }
                    }
                }
                state.session = Some("chat0001".into());
                drop(state);
                canonical_process(&TaskSessionResponse::new(SessionBinding::new(
                    self.agent, "chat0001", 1,
                )?))
            }
            value if value == HostOperation::TaskSession.command() => {
                let _request: TaskSessionRequest = decode_request(request)?;
                let session = self.state.lock().unwrap().session.clone();
                match session {
                    Some(session) => canonical_process(&TaskSessionResponse::new(
                        SessionBinding::new(self.agent, session, 1)?,
                    )),
                    None => {
                        let mut result = canonical_process(&HostControlError::new(
                            "SESSION_UNBOUND",
                            "task has no bound agent session",
                        )?)?;
                        result.status = ExitStatus::from_raw(23 << 8);
                        Ok(result)
                    }
                }
            }
            value if value == HostOperation::TaskStatus.command() => {
                let _status: TaskStatusRequest = decode_request(request)?;
                let terminal = self.state.lock().unwrap().turn_submitted;
                canonical_process(&TaskStatusResponse::new(self.task_status(terminal)?))
            }
            value if value == HostOperation::TaskTurn.command() => {
                let turn: TaskTurnRequest = decode_request(request)?;
                let prompt = turn.prompt().to_owned();
                let mut state = self.state.lock().unwrap();
                state.calls.push("task-turn".into());
                state.last_prompt = prompt;
                state.turn_submitted = true;
                if let Some(session) = state.stream_session.clone() {
                    state.session = Some(session);
                }
                drop(state);
                let active = self.task_status(false)?;
                let material = turn.submit().material();
                let job_meta = JobMeta::new(material, material.fingerprint())?;
                self.state.lock().unwrap().native_job = Some(job_meta.clone());
                let submit = SubmitResponse::Accepted {
                    meta: Box::new(job_meta),
                    status: JobStatus::accepted(material.created_at_millis() + 1)?,
                };
                canonical_process(&TaskTurnResponse::new(submit, active))
            }
            value if value == HostOperation::Status.command() => {
                let query: mac_worker::job::StatusRequest = decode_request(request)?;
                let state = self.state.lock().unwrap();
                let meta = state
                    .native_job
                    .clone()
                    .expect("native status follows accepted turn");
                assert_eq!(query.job_id(), meta.job_id());
                let status = if state.fail_first_turn {
                    JobStatus::failed(meta.created_at_millis() + 2, 1, 0, 0)?
                } else {
                    JobStatus::succeeded(meta.created_at_millis() + 2, 0, 0)?
                };
                canonical_process(&mac_worker::job::StatusResponse::new(meta, status)?)
            }
            value if value == HostOperation::LogChunk.command() => {
                let chunk_request: LogChunkRequest = decode_request(request)?;
                let response = mac_worker::job::LogChunkResponse::new(LogChunk::new(
                    chunk_request.stream(),
                    chunk_request.offset(),
                    Vec::new(),
                )?)?;
                canonical_process(&response)
            }
            value if value == HostOperation::StatusLogs.command() => {
                Ok(clap_unrecognized_subcommand("status-logs"))
            }
            value if value == HostOperation::ResolveOrAbandon.command() => {
                Ok(success(b"{\"protocol_version\":1}\n".to_vec()))
            }
            other => panic!("unexpected worker operation: {other}"),
        }
    }
}

struct TaskHarness {
    _repo: support::GitRepo,
    _current_dir: CurrentDirGuard,
    _state_root: tempfile::TempDir,
    _profile_root: Option<tempfile::TempDir>,
    paths: mac_worker::paths::PathLayout,
    state: ClientStateStore,
    config: Config,
    runner: RecordingHost,
    executor: InlineRunnerExecutor,
    task_id: Mutex<Option<TaskId>>,
}

impl TaskHarness {
    fn cursor() -> Self {
        Self::new(AgentKind::Cursor, None)
    }

    fn opencode() -> Self {
        Self::new(AgentKind::Opencode, None)
    }

    fn with_stream(self, session: &str) -> Self {
        self.runner.set_stream(session);
        self
    }

    fn with_profile(mut self, name: &str, key: &str, value: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let env_dir = root.path().join(".config/mac-worker/env");
        fs::create_dir_all(&env_dir).unwrap();
        let path = env_dir.join(format!("{name}.env"));
        fs::write(&path, format!("{key}={value}\n")).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        self.runner.set_profile_home(root.path().to_path_buf());
        self._profile_root = Some(root);
        self.config = Config::parse(
            "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
        )
        .unwrap();
        let _ = name;
        self
    }

    fn fail_first_turn(self) -> Self {
        self.runner.set_fail_first_turn();
        self
    }

    fn new(agent: AgentKind, env_profile: Option<&str>) -> Self {
        let _ = env_profile;
        let repo = support::GitRepo::init();
        repo.write("base.txt", b"base\n");
        repo.commit_all("base");
        let state_root = tempfile::tempdir().unwrap();
        let state_root_path = state_root.path().canonicalize().unwrap();
        let paths = support::task_harness::paths(&state_root_path);
        let state = ClientStateStore::open(&paths.state).unwrap();
        let config = Config::parse(
            "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
        )
        .unwrap();
        let runner = RecordingHost::new(agent);
        let current_dir = CurrentDirGuard::enter(repo.root());
        Self {
            _repo: repo,
            _current_dir: current_dir,
            _state_root: state_root,
            _profile_root: None,
            paths,
            state,
            config,
            runner,
            executor: InlineRunnerExecutor,
            task_id: Mutex::new(None),
        }
    }

    fn submit_first_turn(&self) -> Result<(), WorkerError> {
        self.submit_prompt(FIRST_PROMPT, self.profile_name())
    }

    fn profile_name(&self) -> Option<String> {
        self.runner
            .state
            .lock()
            .unwrap()
            .profile_home
            .as_ref()
            .map(|_| "agents".to_owned())
    }

    fn submit_prompt(&self, prompt: &str, env_profile: Option<String>) -> Result<(), WorkerError> {
        let report = match TaskClient::new(
            &self.runner,
            &self.config,
            &self.paths,
            &self.state,
            &self.executor,
        )
        .submit(
            TaskSubmitRequest {
                agent: self.runner.agent,
                model: None,
                effort: None,
                prompt: prompt.into(),
                project: std::env::current_dir().unwrap(),
                base: "main".into(),
                wip: true,
                source: None,
                publish: None,
                publish_branch: None,
                cli_includes: Vec::new(),
                limits: TaskLimits::default(),
                close_policy: ClosePolicy::Never,
                env_profile,
                preference: WorkerPreference::Pinned {
                    worker: "mini-1".into(),
                },
                wait_for_capacity: true,
                attached: true,
                run_id: None,
            },
            &mut Vec::new(),
            &mut Vec::new(),
        ) {
            Ok(report) => report,
            Err(error) => {
                self.runner.record_error(&error);
                return Err(error);
            }
        };
        *self.task_id.lock().unwrap() = Some(report.task_id());
        Ok(())
    }

    fn say(&self, message: &str) -> Result<(), WorkerError> {
        let task_id = self.task_id.lock().unwrap().expect("submitted task");
        TaskClient::new(
            &self.runner,
            &self.config,
            &self.paths,
            &self.state,
            &self.executor,
        )
        .say(
            task_id,
            message.into(),
            true,
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .map(|_| ())
        .inspect_err(|error| {
            self.runner.record_error(error);
        })
    }

    fn cancel(&self) -> Result<(), WorkerError> {
        let task_id = self.task_id.lock().unwrap().expect("submitted task");
        TaskClient::new(
            &self.runner,
            &self.config,
            &self.paths,
            &self.state,
            &self.executor,
        )
        .cancel(task_id)
        .map(|_| ())
    }

    fn host_calls(&self) -> Vec<String> {
        self.runner.host_calls()
    }

    fn persisted_session(&self) -> Option<String> {
        self.runner.persisted_session()
    }

    fn last_shell(&self) -> String {
        self.runner.last_shell()
    }

    fn last_prompt(&self) -> String {
        self.runner.last_prompt()
    }

    fn child_env(&self, name: &str) -> Option<String> {
        self.runner.child_env(name)
    }

    fn task_json(&self) -> String {
        let task_id = self.task_id.lock().unwrap().expect("submitted task");
        serde_json::to_string(&self.state.load_task(task_id).unwrap()).unwrap()
    }

    fn error_text(&self) -> String {
        self.runner.error_text()
    }
}

fn probe_response(agent: AgentKind) -> ProbeResponse {
    let name = match agent {
        AgentKind::Cursor => "cursor",
        AgentKind::Opencode => "opencode",
        AgentKind::Codex => "codex",
        AgentKind::Claude => "claude",
    };
    ProbeResponse {
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
                name: name.into(),
                version: Some("0.1.0".into()),
                auth: AgentAuth::Authenticated,
                auth_by_profile: vec![
                    ("secure".into(), AgentAuth::Authenticated),
                    ("agents".into(), AgentAuth::Authenticated),
                ],
            }],
            env_profiles: vec![
                ProfileProbe {
                    name: "secure".into(),
                    secure: true,
                },
                ProfileProbe {
                    name: "agents".into(),
                    secure: true,
                },
            ],
            git_identity: true,
            collected_at_millis: u64::MAX / 2,
            herdr: None,
            origin_https_helpers: Default::default(),
        }),
        facts_age_millis: Some(0),
        configured_slots: 0,
        busy_slots: 0,
    }
}

fn decode_request<T: serde::de::DeserializeOwned>(
    request: &ProcessRequest,
) -> Result<T, WorkerError> {
    serde_json::from_slice(
        request
            .stdin
            .as_deref()
            .ok_or_else(|| WorkerError::Protocol("worker request had no stdin".into()))?,
    )
    .map_err(|error| WorkerError::Protocol(error.to_string()))
}

fn canonical_process<T: serde::Serialize>(value: &T) -> Result<ProcessResult, WorkerError> {
    let mut stdout =
        serde_json::to_vec(value).map_err(|error| WorkerError::Protocol(error.to_string()))?;
    stdout.push(b'\n');
    Ok(success(stdout))
}

fn success(stdout: Vec<u8>) -> ProcessResult {
    ProcessResult {
        status: ExitStatus::from_raw(0),
        stdout,
        stderr: Vec::new(),
    }
}

fn clap_unrecognized_subcommand(subcommand: &str) -> ProcessResult {
    ProcessResult {
        status: ExitStatus::from_raw(2 << 8),
        stdout: Vec::new(),
        stderr: format!("error: unrecognized subcommand '{subcommand}'\n").into_bytes(),
    }
}

fn write_profile(home: &Path, name: &str, body: &str) {
    let dir = home.join(".config/mac-worker/env");
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{name}.env"));
    fs::write(&path, body).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
}

#[test]
fn cursor_prebind_happens_before_first_turn_and_the_chat_id_is_persisted() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let harness = TaskHarness::cursor();
    harness.submit_first_turn().unwrap();
    assert_eq!(
        harness.host_calls(),
        vec!["cursor-agent create-chat", "task-turn"]
    );
    assert_eq!(harness.persisted_session(), Some("chat0001".into()));
    assert!(harness.last_shell().contains("'--resume' 'chat0001'"));
    assert!(harness.last_shell().contains("'--force'"));
    assert!(
        harness
            .last_shell()
            .contains("\"Read the task from $MAC_WORKER_TURN_DIR/prompt.md and follow it.\"")
    );
    assert!(!harness.last_shell().contains(FIRST_PROMPT));
}

#[test]
fn opencode_binds_session_from_json_and_resumes_with_auto() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let harness = TaskHarness::opencode().with_stream("ses0001");
    harness.submit_first_turn().unwrap();
    assert_eq!(harness.persisted_session(), Some("ses0001".into()));
    harness.say("continue the migration").unwrap();
    assert!(harness.last_shell().contains("'--session' 'ses0001'"));
    assert!(harness.last_shell().contains("'--auto'"));
    assert!(
        harness
            .last_shell()
            .contains("\"Read the task from $MAC_WORKER_TURN_DIR/prompt.md and follow it.\"")
    );
}

#[test]
fn cursor_and_opencode_use_profile_values_only_in_the_child_environment() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let harness = TaskHarness::cursor().with_profile("agents", "CURSOR_API_KEY", "secret-value");
    harness.submit_first_turn().unwrap();
    assert_eq!(
        harness.child_env("CURSOR_API_KEY"),
        Some("secret-value".into())
    );
    assert!(!harness.task_json().contains("secret-value"));
    assert!(!harness.error_text().contains("secret-value"));
}

#[test]
fn opencode_first_turn_includes_auto_and_the_quoted_prompt_pointer() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let harness = TaskHarness::opencode();
    harness.submit_first_turn().unwrap();
    assert!(harness.last_shell().contains("'--auto'"));
    assert!(
        harness
            .last_shell()
            .contains("\"Read the task from $MAC_WORKER_TURN_DIR/prompt.md and follow it.\"")
    );
    assert!(!harness.last_shell().contains(FIRST_PROMPT));
    assert!(!harness.last_shell().contains("--workspace"));
    assert!(!harness.last_shell().contains("/Users/"));
}

#[test]
fn opencode_prompt_requires_an_exact_json_final_message() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let harness = TaskHarness::opencode();
    harness.submit_first_turn().unwrap();
    let prompt = harness.last_prompt();
    assert!(prompt.contains("OpenCode"));
    assert!(
        prompt.contains("end your final message with exactly the JSON object and nothing after it")
    );
    assert!(prompt.contains("Do not wrap it in a Markdown code fence or add prose"));
    assert!(prompt.contains("\"files_changed\""));
}

#[test]
fn cursor_workspace_policy_records_permission_fallback() {
    let launch = adapter_for(AgentKind::Cursor)
        .resume_turn(
            &mac_worker::agent::TurnParams {
                kind: AgentKind::Cursor,
                model: None,
                effort: None,
                policy: PermissionPolicy::Workspace,
                limits: mac_worker::agent::TurnLimits::new(45 * 60 * 1000, None, None).unwrap(),
                session_seed: uuid::Uuid::from_u128(1),
            },
            "chat0001",
        )
        .unwrap();
    assert!(launch.permission_fallback());
    assert!(launch.args().contains(&"--force".into()));
}

#[test]
fn resume_without_a_binding_is_session_unbound() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let harness = TaskHarness::opencode();
    harness.submit_first_turn().unwrap();
    assert_eq!(harness.persisted_session(), None);
    let error = harness.say("continue").unwrap_err();
    assert_eq!(error.public_code(), "SESSION_UNBOUND");
}

#[test]
fn malformed_and_truncated_trailer_results_remain_unknown() {
    let adapter = adapter_for(AgentKind::Cursor);
    let truncated = adapter
        .extract_result("```mac-worker-result\n{\"status\":\"done\"", None)
        .unwrap();
    assert_eq!(truncated.status(), mac_worker::agent::ResultStatus::Unknown);
    let malformed = adapter_for(AgentKind::Opencode)
        .extract_result("```mac-worker-result\nnot-json\n```", None)
        .unwrap();
    assert_eq!(malformed.status(), mac_worker::agent::ResultStatus::Unknown);
}

#[test]
fn cancel_then_resume_reuses_the_bound_session() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let harness = TaskHarness::cursor();
    harness.submit_first_turn().unwrap();
    harness.cancel().unwrap();
    harness.say("continue after cancel").unwrap();
    assert_eq!(harness.persisted_session(), Some("chat0001".into()));
    assert!(harness.last_shell().contains("'--resume' 'chat0001'"));
}

#[test]
fn failed_first_cursor_turn_retains_the_binding_for_say() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let harness = TaskHarness::cursor().fail_first_turn();
    harness.submit_first_turn().unwrap();
    assert_eq!(harness.persisted_session(), Some("chat0001".into()));
    harness.say("retry after failure").unwrap();
    assert!(harness.last_shell().contains("'--resume' 'chat0001'"));
}

#[test]
fn parse_prebind_accepts_json_ids_and_plain_lines() {
    assert_eq!(
        parse_prebind_session_ref(br#"{"id":"chat0001"}"#).unwrap(),
        "chat0001"
    );
    assert_eq!(
        parse_prebind_session_ref(br#"{"chatId":"chat-json"}"#).unwrap(),
        "chat-json"
    );
    assert_eq!(
        parse_prebind_session_ref(b"chat-plain\n").unwrap(),
        "chat-plain"
    );
    assert!(parse_prebind_session_ref(b"warning: retrying\nchat-plain\n").is_err());
    assert!(parse_prebind_session_ref(br#"created {"id":"chat-json"}"#).is_err());
    assert!(parse_prebind_session_ref(br#"{"status":"ok"}"#).is_err());
    assert!(parse_prebind_session_ref(b"").is_err());
    assert!(parse_prebind_session_ref(b"has\x01control").is_err());
}

#[test]
fn host_prebind_runs_create_chat_in_the_login_shell_with_profile_values() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    write_profile(&home, "agents", "CURSOR_API_KEY=secret-value\n");
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let runner = support::recording_runner::RecordingRunner::returning(ProcessResult {
        status: ExitStatus::from_raw(0),
        stdout: b"chat0001\n".to_vec(),
        stderr: Vec::new(),
    });
    let response = prebind_session(
        &store,
        &runner,
        &TaskPrebindRequest::discover(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            TaskId::generate(),
            AgentKind::Cursor,
            Some("agents".into()),
        ),
        &home,
    )
    .unwrap();
    assert_eq!(response.binding().session_ref(), "chat0001");
    let request = runner.single_request();
    assert_eq!(request.program, OsStr::new("/bin/zsh"));
    assert!(request.stdin.is_none());
    let shell = request.args.last().and_then(|arg| arg.to_str()).unwrap();
    assert_eq!(
        shell,
        &render_prebind_shell(&["cursor-agent".into(), "create-chat".into()]).unwrap()
    );
    assert!(
        request
            .environment
            .iter()
            .any(|(name, value)| name == "CURSOR_API_KEY" && value == "secret-value")
    );
    let debug = format!("{request:?}");
    assert!(!debug.contains("secret-value"));
}

#[test]
fn adapters_expose_only_documented_native_delete_commands() {
    assert_eq!(
        adapter_for(AgentKind::Codex).delete_session("00000000-0000-0000-0000-000000000001"),
        Some(vec![
            "codex".into(),
            "delete".into(),
            "--force".into(),
            "00000000-0000-0000-0000-000000000001".into(),
        ])
    );
    assert_eq!(
        adapter_for(AgentKind::Opencode).delete_session("ses0001"),
        Some(vec![
            "opencode".into(),
            "session".into(),
            "delete".into(),
            "ses0001".into(),
        ])
    );
    assert_eq!(
        adapter_for(AgentKind::Cursor).delete_session("chat0001"),
        None
    );
    assert_eq!(
        adapter_for(AgentKind::Claude).delete_session("session-1"),
        None
    );
}

#[test]
fn native_delete_rejects_unbound_or_control_session_references() {
    for kind in [AgentKind::Codex, AgentKind::Opencode] {
        assert_eq!(adapter_for(kind).delete_session(""), None);
        assert_eq!(adapter_for(kind).delete_session("session\n1"), None);
    }
}

#[test]
fn prebind_login_request_runs_the_command_through_a_login_shell() {
    let home = tempfile::tempdir().unwrap();
    let request = prebind_login_request(
        &["/bin/echo".to_string(), "prebind-ok".to_string()],
        home.path(),
        &[],
    )
    .unwrap();
    assert_eq!(request.program, "/bin/zsh");
    assert_eq!(
        request.args,
        vec![
            std::ffi::OsString::from("-lc"),
            std::ffi::OsString::from("exec '/bin/echo' 'prebind-ok'"),
        ]
    );
    let result = mac_worker::process::SystemProcessRunner
        .run(&request)
        .unwrap();
    assert!(
        result.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&result.stdout).trim(), "prebind-ok");
}

fn cursor_fixture(name: &str) -> String {
    fs::read_to_string(format!("tests/fixtures/cursor/{name}"))
        .unwrap_or_else(|error| panic!("Cursor fixture {name} must be readable: {error}"))
}

#[test]
fn cursor_live_markdown_final_message_stays_unknown() {
    // Captured from a live Cursor turn on a worker: the agent rendered the
    // contract as Markdown instead of a JSON object.
    let result = adapter_for(AgentKind::Cursor)
        .extract_result(&cursor_fixture("stream-json.jsonl"), None)
        .unwrap();
    assert_eq!(result.status(), mac_worker::agent::ResultStatus::Unknown);
}

#[test]
fn cursor_extracts_the_json_object_ending_the_final_message() {
    let result = adapter_for(AgentKind::Cursor)
        .extract_result(&cursor_fixture("final-json.jsonl"), None)
        .unwrap();
    assert_eq!(result.status(), mac_worker::agent::ResultStatus::Done);
    assert_eq!(result.summary(), "smoke file created");
    assert_eq!(result.files_changed(), &["scratch/cursor-smoke.txt"]);
}

#[test]
fn cursor_extracts_a_fenced_json_object_from_the_final_message() {
    let result = adapter_for(AgentKind::Cursor)
        .extract_result(&cursor_fixture("final-fenced.jsonl"), None)
        .unwrap();
    assert_eq!(result.status(), mac_worker::agent::ResultStatus::Done);
    assert_eq!(result.summary(), "smoke file created");
}

#[test]
fn cursor_extracts_needs_input_from_the_final_message() {
    let result = adapter_for(AgentKind::Cursor)
        .extract_result(&cursor_fixture("final-needs-input.jsonl"), None)
        .unwrap();
    assert_eq!(result.status(), mac_worker::agent::ResultStatus::NeedsInput);
    assert_eq!(
        result.questions(),
        &[mac_worker::agent::Question::open("alpha or beta?")]
    );
}

#[test]
fn cursor_live_fixture_captures_the_session_id() {
    let adapter = adapter_for(AgentKind::Cursor);
    let events: Vec<mac_worker::agent::AgentEvent> = cursor_fixture("stream-json.jsonl")
        .lines()
        .filter_map(|line| adapter.parse_event(line))
        .collect();
    assert!(matches!(
        events.first(),
        Some(mac_worker::agent::AgentEvent::SessionStarted { session_ref }) if session_ref == "2df4613a-3015-46d8-9f06-b595b06988f4"
    ));
}

#[test]
fn cursor_prompt_requires_an_exact_json_final_message() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let harness = TaskHarness::cursor();
    harness.submit_first_turn().unwrap();
    let prompt = harness.last_prompt();
    assert!(prompt.contains("Cursor:"));
    assert!(
        prompt.contains("end your final message with exactly the JSON object and nothing after it")
    );
    assert!(prompt.contains("do not render it as Markdown"));
    assert!(prompt.contains("\"files_changed\""));
}
