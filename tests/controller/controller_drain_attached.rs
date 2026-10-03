//! Attached drain regression: real queue, admission and completion, fake worker transport.

use crate::support;

use std::{
    ffi::{OsStr, OsString},
    os::{
        fd::AsRawFd,
        unix::{
            fs::{OpenOptionsExt, PermissionsExt},
            process::ExitStatusExt,
        },
    },
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use mac_worker::test_support::{
    agents::{
        agent::{AgentKind, PermissionPolicy},
        agent_facts::{AgentAuth, AgentFacts, AgentProbe, ProfileProbe},
    },
    client_state::ClientStateStore,
    core::{
        config::Config,
        error::WorkerError,
        paths::PathLayout,
        protocol::{
            CpuCounters, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION,
        },
    },
    host::{
        job::{
            JobMeta, JobState, JobStatus, LeaseAcquireRequest, LeaseAcquireResponse, LeaseRecord,
            LogChunk, LogChunkRequest, LogStream, StatusRequest, StatusResponse, SubmitResponse,
        },
        lease::SlotState,
        process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    },
    task::{
        client::TaskClient,
        model::{
            BaseOid, ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, RunnerIdentity,
            TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState,
            TaskStatus, TurnId, TurnSummary, TurnTerminal,
        },
        prepared_followup::PreparedFollowup,
        project_state::ProjectState,
        store::{
            SessionBinding, TaskPrepareRequest, TaskPrepareResponse, TaskSessionRequest,
            TaskSessionResponse, TaskStatusRequest, TaskStatusResponse,
        },
        turn::{TaskTurnRequest, TaskTurnResponse},
        turn_runner::{InlineRunnerExecutor, RunnerExecutor},
    },
    transfer::{HostOperation, repo::repo_id_for},
};
use uuid::Uuid;

static CURRENT_DIR_LOCK: Mutex<()> = Mutex::new(());

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
        let _ = std::env::set_current_dir(&self.previous);
    }
}

struct CountingInlineExecutor {
    starts: AtomicUsize,
}

impl CountingInlineExecutor {
    fn new() -> Self {
        Self {
            starts: AtomicUsize::new(0),
        }
    }

    fn starts(&self) -> usize {
        self.starts.load(Ordering::SeqCst)
    }
}

impl RunnerExecutor for CountingInlineExecutor {
    fn start(
        &self,
        paths: &PathLayout,
        task_id: TaskId,
        turn_id: TurnId,
    ) -> Result<RunnerIdentity, WorkerError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        InlineRunnerExecutor.start(paths, task_id, turn_id)
    }
}

/// SSH/host + result-ref fake. User git otherwise goes to the real binary.
struct CompletingHost {
    user_repo: PathBuf,
    result_oid: BaseOid,
    prior_turns: Mutex<Vec<TurnSummary>>,
    prior_head: Mutex<BaseOid>,
    worker: Mutex<String>,
    turn_submitted: Mutex<bool>,
    stdout_log_sent: Mutex<bool>,
    task: Mutex<Option<(TaskMeta, TurnId)>>,
    accepted_meta: Mutex<Option<JobMeta>>,
    live_turns: Mutex<Vec<TurnSummary>>,
    submissions: AtomicUsize,
    drain_root: PathBuf,
}

impl CompletingHost {
    fn new(
        user_repo: PathBuf,
        result_oid: BaseOid,
        prior: &LocalTaskRecord,
        drain_root: PathBuf,
    ) -> Self {
        Self {
            user_repo,
            drain_root,
            result_oid,
            prior_turns: Mutex::new(prior.status().turns().to_vec()),
            prior_head: Mutex::new(
                prior
                    .status()
                    .head_oid()
                    .cloned()
                    .unwrap_or_else(|| prior.meta().base_oid().clone()),
            ),
            worker: Mutex::new("mini-1".into()),
            turn_submitted: Mutex::new(false),
            stdout_log_sent: Mutex::new(false),
            task: Mutex::new(None),
            accepted_meta: Mutex::new(None),
            live_turns: Mutex::new(prior.status().turns().to_vec()),
            submissions: AtomicUsize::new(0),
        }
    }

    fn submissions(&self) -> usize {
        self.submissions.load(Ordering::SeqCst)
    }

    fn task_status(&self, terminal: bool) -> Result<TaskStatus, WorkerError> {
        let turns = self.live_turns.lock().unwrap().clone();
        let outcome = terminal.then_some(TaskOutcome::Done);
        let head = if terminal {
            self.result_oid.clone()
        } else {
            self.prior_head.lock().unwrap().clone()
        };
        TaskStatus::new(
            if !terminal && self.prior_turns.lock().unwrap().is_empty() {
                TaskState::Active
            } else {
                TaskState::Open
            },
            outcome,
            Some(self.worker.lock().unwrap().clone()),
            true,
            Some(head),
            terminal.then_some("follow-up finished".into()),
            Vec::new(),
            if terminal {
                vec!["agent.txt".into()]
            } else {
                Vec::new()
            },
            None,
            turns,
            20,
        )
    }

    fn plant_result_ref(&self, transfer: &Path, dest_ref: &str) -> ProcessResult {
        let fetch = ProcessRequest {
            program: "/usr/bin/git".into(),
            args: vec![
                OsString::from("-C"),
                transfer.as_os_str().to_os_string(),
                OsString::from("fetch"),
                OsString::from("--no-write-fetch-head"),
                self.user_repo.as_os_str().to_os_string(),
                OsString::from(format!("{}:{dest_ref}", self.result_oid.as_str())),
            ],
            environment: vec![
                ("GIT_CONFIG_GLOBAL".into(), "/dev/null".into()),
                ("GIT_CONFIG_NOSYSTEM".into(), "1".into()),
                ("GIT_TERMINAL_PROMPT".into(), "0".into()),
            ],
            environment_remove: vec![
                "GIT_DIR".into(),
                "GIT_WORK_TREE".into(),
                "GIT_INDEX_FILE".into(),
                "GIT_COMMON_DIR".into(),
            ],
            stdin: None,
            policy: mac_worker::test_support::host::process::ProcessPolicy {
                stdout_limit: 64 * 1024,
                stderr_limit: 64 * 1024,
                deadline: std::time::Duration::from_secs(15),
            },
            isolate_parent_environment: false,
        };
        SystemProcessRunner
            .run(&fetch)
            .expect("plant result objects")
    }
}

impl ProcessRunner for CompletingHost {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/git") {
            let is_worker_result_fetch = request.args.iter().any(|arg| arg == "fetch")
                && request.args.iter().any(|arg| {
                    arg.to_string_lossy().starts_with("--upload-pack=")
                        && arg.to_string_lossy().contains("host upload-pack")
                });
            if is_worker_result_fetch {
                let transfer = request
                    .args
                    .windows(2)
                    .find(|pair| pair[0] == "-C")
                    .map(|pair| PathBuf::from(&pair[1]))
                    .expect("result fetch names the transfer repo");
                let dest = request
                    .args
                    .iter()
                    .rev()
                    .find_map(|arg| {
                        arg.to_str()?
                            .split_once(':')
                            .map(|(_, dest)| dest.to_owned())
                    })
                    .expect("result fetch names the destination ref");
                return Ok(self.plant_result_ref(&transfer, &dest));
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
            value if value == HostOperation::RefreshFacts.command() => Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout: Vec::new(),
                stderr: Vec::new(),
            }),
            "~/.local/bin/worker host probe" => canonical_process(&ProbeResponse {
                features: None,
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
                        autoupdate: None,
                        name: "codex".into(),
                        version: Some("0.1.0".into()),
                        auth: AgentAuth::Authenticated,
                        auth_by_profile: vec![("secure".into(), AgentAuth::Authenticated)],
                    }],
                    env_profiles: vec![ProfileProbe {
                        name: "secure".into(),
                        secure: true,
                    }],
                    git_identity: true,
                    collected_at_millis: u64::MAX / 2,
                    herdr: None,
                    origin_https_helpers: Default::default(),
                }),
                facts_age_millis: Some(0),
                configured_slots: 0,
                busy_slots: 0,
                build_id: None,
                binary_sha256: None,
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
            value if value == HostOperation::TaskSession.command() => {
                let session: TaskSessionRequest = decode_request(request)?;
                let _ = session;
                canonical_process(&TaskSessionResponse::new(SessionBinding::new(
                    AgentKind::Codex,
                    "thread-followup-1",
                    1,
                )?))
            }
            value if value == HostOperation::TaskPrepare.command() => {
                let prepare: TaskPrepareRequest = decode_request(request)?;
                *self.worker.lock().unwrap() = prepare.worker().to_owned();
                *self.task.lock().unwrap() = Some((prepare.meta().clone(), prepare.job_id()));
                canonical_process(&TaskPrepareResponse::new(
                    prepare.meta().base_oid().clone(),
                    false,
                ))
            }
            value if value == HostOperation::TaskStatus.command() => {
                let _status: TaskStatusRequest = decode_request(request)?;
                canonical_process(&TaskStatusResponse::new(
                    self.task_status(*self.turn_submitted.lock().unwrap())?,
                ))
            }
            value if value == HostOperation::TaskTurn.command() => {
                let turn: TaskTurnRequest = decode_request(request)?;
                assert!(
                    !mac_worker::test_support::controller::drain::is_drained(&self.drain_root)
                        .unwrap(),
                    "attached command dispatched a worker task-turn while drained"
                );
                self.submissions.fetch_add(1, Ordering::SeqCst);
                let material = turn.submit().material();
                let follow = TurnSummary::new(
                    turn.turn().turn_number(),
                    material.job_id(),
                    None,
                    None,
                    None,
                    false,
                    Some(material.created_at_millis()),
                    None,
                );
                {
                    let mut live = self.live_turns.lock().unwrap();
                    *live = self.prior_turns.lock().unwrap().clone();
                    live.push(follow);
                }
                let active = self.task_status(false)?;
                *self.turn_submitted.lock().unwrap() = true;
                {
                    let mut live = self.live_turns.lock().unwrap();
                    let last = live.last_mut().expect("follow-up turn was recorded");
                    *last = TurnSummary::new(
                        last.turn_number(),
                        last.turn_id(),
                        Some(TurnTerminal::Succeeded),
                        Some(TaskOutcome::Done),
                        Some(true),
                        false,
                        last.started_at_millis(),
                        Some(material.created_at_millis() + 1),
                    );
                }
                let job_meta = JobMeta::new(material, material.fingerprint())?;
                *self.accepted_meta.lock().unwrap() = Some(job_meta.clone());
                let submit = SubmitResponse::Accepted {
                    meta: Box::new(job_meta),
                    status: JobStatus::accepted(material.created_at_millis() + 1)?,
                };
                canonical_process(&TaskTurnResponse::new(submit, active))
            }
            value if value == HostOperation::Status.command() => {
                let query: StatusRequest = decode_request(request)?;
                let meta = self
                    .accepted_meta
                    .lock()
                    .unwrap()
                    .clone()
                    .expect("job status after the accepted turn");
                assert_eq!(query.job_id(), meta.job_id());
                let created = meta.created_at_millis();
                canonical_process(&StatusResponse::new(
                    meta,
                    JobStatus::new(
                        JobState::Succeeded,
                        created + 2,
                        None,
                        None,
                        None,
                        None,
                        Some(0),
                        None,
                        Some(13),
                        Some(0),
                        None,
                        None,
                    )?,
                )?)
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
                canonical_process(&mac_worker::test_support::host::job::LogChunkResponse::new(
                    LogChunk::new(chunk_request.stream(), chunk_request.offset(), bytes)?,
                )?)
            }
            value if value == HostOperation::StatusLogs.command() => Ok(ProcessResult {
                status: ExitStatus::from_raw(2 << 8),
                stdout: Vec::new(),
                stderr: b"error: unrecognized subcommand 'status-logs'\n".to_vec(),
            }),
            other => panic!("unexpected worker operation: {other}"),
        }
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
    Ok(ProcessResult {
        status: ExitStatus::from_raw(0),
        stdout,
        stderr: Vec::new(),
    })
}

fn task_config() -> Config {
    Config::parse(
        "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap()
}

fn git_text(repo: &support::GitRepo, args: &[&str]) -> String {
    let output = repo.git(args);
    assert!(
        output.status.success(),
        "{args:?} {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn plant_open_first_turn(
    store: &ClientStateStore,
    project: &ProjectState,
    base_oid: BaseOid,
    task_number: u128,
    first_turn: u128,
) -> LocalTaskRecord {
    let task_id = TaskId::new(Uuid::from_u128(task_number));
    let turn_id = TurnId::new(Uuid::from_u128(first_turn));
    let meta = TaskMeta::new(TaskMetaInput {
        session_import: None,
        task_id,
        run_id: None,
        project_id: project.context.project_id.clone(),
        worktree_id: project.context.worktree_id.clone(),
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
        base_oid: base_oid.clone(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("mac-worker", "mac-worker@example.test").unwrap(),
        title: None,
        prompt: "first turn".into(),
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
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![turn],
        2,
    )
    .unwrap();
    let repo_id = repo_id_for(&project.context.common_dir).unwrap();
    let record =
        LocalTaskRecord::new(meta, status, None, None, None, repo_id, None, true, None).unwrap();
    store.create_task(record.clone()).unwrap();
    store
        .write_task_project_path(&record, &project.context.root)
        .unwrap();
    record
}

#[derive(Clone, Copy)]
enum AttachedPath {
    Submit,
    Say,
    Resume,
    ResumeParked,
    ResumeContended,
}

fn attached_waits_for_drain(path: AttachedPath, json: bool, expire: bool) {
    let _lock = CURRENT_DIR_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let repo = support::GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    let base_oid: BaseOid = git_text(&repo, &["rev-parse", "HEAD"]).parse().unwrap();
    let _cwd = CurrentDirGuard::enter(repo.root());
    let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let state_root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
    let journal_gate = std::sync::Arc::new(ReleaseJournalOnContention(Mutex::new(None)));
    let store =
        ClientStateStore::open_with_concurrency_hook(&paths.state, journal_gate.clone()).unwrap();
    let prior_store =
        ClientStateStore::open(&paths.state.parent().unwrap().join("prior-fixture")).unwrap();
    let expected = plant_open_first_turn(
        if matches!(path, AttachedPath::Submit) {
            &prior_store
        } else {
            &store
        },
        &project,
        base_oid.clone(),
        11,
        12,
    );
    let prepared = PreparedFollowup::prepare(
        &expected,
        "follow up".to_owned(),
        TurnId::new(Uuid::from_u128(13)),
        4_000,
    )
    .unwrap();
    let host = CompletingHost::new(
        repo.root().to_path_buf(),
        base_oid,
        &expected,
        paths.controller_state_root(),
    );
    if matches!(path, AttachedPath::Submit) {
        host.prior_turns.lock().unwrap().clear();
        host.live_turns.lock().unwrap().clear();
    }
    let executor = CountingInlineExecutor::new();
    let config = task_config();
    assert!(!config.controller.enabled);
    mac_worker::test_support::controller::drain::set_drained(&paths.controller_state_root(), true)
        .unwrap();
    let polls = AtomicUsize::new(0);
    let poll = |delay: std::time::Duration| {
        assert!(
            delay > std::time::Duration::ZERO && delay <= std::time::Duration::from_millis(100)
        );
        assert_eq!(
            host.submissions(),
            0,
            "no worker task-turn before admission"
        );
        assert_eq!(executor.starts(), 0);
        let rows = store.queue_snapshot().unwrap();
        assert_eq!(rows.entries().len(), 1);
        let row = &rows.entries()[0];
        assert_eq!(
            *row.owner_opt().unwrap(),
            mac_worker::test_support::host::supervisor::SystemProcessInspector
                .identity_for_pid(std::process::id())
                .unwrap()
        );
        assert!(row.slot_reservation().is_none());
        probe_recovery(&paths);
        if polls.fetch_add(1, Ordering::SeqCst) == 1 {
            if expire {
                return Err(WorkerError::task(
                    "WAIT_TIMEOUT",
                    "injected deadline expiry",
                ));
            }
            mac_worker::test_support::controller::drain::set_drained(
                &paths.controller_state_root(),
                false,
            )
            .unwrap();
            probe_recovery(&paths);
            assert_eq!(store.queue_entry(row.job_id()).unwrap().as_ref(), Some(row));
        }
        Ok(())
    };
    let client = TaskClient::new(&host, &config, &paths, &store, &executor)
        .with_drain_wait(&poll)
        .with_json_events(json);
    if matches!(
        path,
        AttachedPath::Resume | AttachedPath::ResumeParked | AttachedPath::ResumeContended
    ) {
        client
            .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
            .unwrap();
        assert_eq!(host.submissions(), 0);
        assert_eq!(executor.starts(), 0);
        // A replay can name an exited incarnation of this PID. Re-own it
        // before waiting; recovery in another process must see the live waiter.
        store
            .adopt_row(
                prepared.turn_id(),
                mac_worker::test_support::host::job::ProcessIdentity::new(std::process::id(), 1)
                    .unwrap(),
            )
            .unwrap();
        if matches!(path, AttachedPath::ResumeParked) {
            store.park_row(prepared.turn_id()).unwrap();
        }
        if matches!(path, AttachedPath::ResumeContended) {
            let dir = paths
                .state
                .join("runners")
                .join(prepared.task_id().to_string());
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
            let log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .mode(0o600)
                .open(dir.join(format!("{}.log", prepared.turn_id())))
                .unwrap();
            assert_eq!(
                unsafe { libc::flock(log.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
                0
            );
            *journal_gate.0.lock().unwrap() = Some(log);
        }
    }
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let report = match path {
        AttachedPath::Submit => client.submit(
            mac_worker::test_support::task::client::TaskSubmitRequest {
                session_import: None,
                questions: None,
                agent: AgentKind::Codex,
                model: None,
                effort: None,
                prompt: "submit during drain".into(),
                project: repo.root().to_path_buf(),
                base: "HEAD".into(),
                wip: false,
                source: None,
                publish: None,
                publish_branch: None,
                cli_includes: vec![],
                limits: TaskLimits::default(),
                close_policy: ClosePolicy::Never,
                env_profile: None,
                preference:
                    mac_worker::test_support::client_state::scheduler::WorkerPreference::Automatic,
                wait_for_capacity: true,
                attached: true,
                run_id: None,
            },
            &mut stdout,
            &mut stderr,
        ),
        AttachedPath::Say => client.say(
            expected.meta().task_id(),
            "follow up".into(),
            true,
            &mut stdout,
            &mut stderr,
        ),
        AttachedPath::Resume | AttachedPath::ResumeParked | AttachedPath::ResumeContended => {
            client.say_prepared(&prepared, true, &mut stdout, &mut stderr)
        }
    };
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    if json {
        assert!(stderr.is_empty());
        let notices = String::from_utf8(stdout)
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|event| event["type"] == "controller_draining")
            .collect::<Vec<_>>();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0]["protocol_version"], PROTOCOL_VERSION);
        assert!(
            notices[0]["message"]
                .as_str()
                .unwrap()
                .contains("worker controller drain --off")
        );
    } else {
        assert_eq!(
            String::from_utf8(stderr)
                .unwrap()
                .lines()
                .collect::<Vec<_>>(),
            vec![
                "controller is draining; the turn is queued and will start after `worker controller drain --off`"
            ]
        );
    }
    if expire {
        let error = report.unwrap_err();
        assert_eq!(error.public_code(), "WAIT_TIMEOUT");
        assert!(error.to_string().contains("queued"));
        assert_eq!(host.submissions(), 0);
        assert_eq!(executor.starts(), 0);
        let row = store.queue_snapshot().unwrap().entries()[0].clone();
        assert!(matches!(
            row.state(),
            mac_worker::test_support::host::job::QueueState::Parked
        ));
        let task_id = store.task_id_for_turn(row.job_id()).unwrap().unwrap();
        assert!(
            !store
                .read_turn_prompt(task_id, row.job_id())
                .unwrap()
                .is_empty()
        );
        assert!(store.load_task(task_id).unwrap().runner().is_none());
        mac_worker::test_support::controller::drain::set_drained(
            &paths.controller_state_root(),
            false,
        )
        .unwrap();
        assert_eq!(
            client
                .reconcile_selected(&[task_id])
                .unwrap()
                .started_runners(),
            1
        );
        assert_eq!(
            client
                .reconcile_selected(&[task_id])
                .unwrap()
                .started_runners(),
            0
        );
        let outcome = mac_worker::test_support::task::turn_runner::TurnRunner::new(
            &host, &config, &paths, &store, &executor,
        )
        .run(task_id, row.job_id(), None)
        .unwrap();
        assert_eq!(outcome.exit_code(), 0);
        assert_eq!(host.submissions(), 1);
        assert_eq!(executor.starts(), 1);
        assert!(store.queue_entry(row.job_id()).unwrap().is_none());
        return;
    }
    let report = report.unwrap();
    assert_eq!(host.submissions(), 1);
    assert_eq!(executor.starts(), 1);
    assert_eq!(report.exit_code(), Some(0));
    assert_eq!(report.status().state(), TaskState::Open);
    assert_eq!(report.status().last_outcome(), Some(&TaskOutcome::Done));
    assert!(
        store
            .queue_entry_for_task_turn(report.task_id())
            .unwrap()
            .is_none()
    );
}

#[test]
fn attached_submit_waits_for_drain() {
    attached_waits_for_drain(AttachedPath::Submit, false, false);
}

#[test]
fn attached_say_waits_for_drain() {
    attached_waits_for_drain(AttachedPath::Say, false, false);
}

#[test]
fn attached_resumed_followup_waits_for_drain() {
    attached_waits_for_drain(AttachedPath::Resume, false, false);
}

fn probe_recovery(paths: &PathLayout) {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &crate::support::libtest_name(module_path!(), "drain_recovery_probe"),
            "--nocapture",
        ])
        .env("DRAIN_RECOVERY_ROOT", paths.state.parent().unwrap())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "recovery probe: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn drain_recovery_probe() {
    let Some(root) = std::env::var_os("DRAIN_RECOVERY_ROOT") else {
        return;
    };
    struct NoProcesses;
    impl ProcessRunner for NoProcesses {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            assert_eq!(request.program, OsStr::new("/usr/bin/ssh"));
            assert_eq!(
                request.args.last().and_then(|arg| arg.to_str()),
                Some(HostOperation::TaskStatus.command())
            );
            // A read-only refresh is allowed while admission is drained.
            Err(WorkerError::Protocol("fixture status unavailable".into()))
        }
    }
    let paths = support::task_harness::paths(PathBuf::from(root));
    let store = ClientStateStore::open(&paths.state).unwrap();
    let task_ids = store
        .list_tasks()
        .unwrap()
        .iter()
        .map(|record| record.meta().task_id())
        .collect::<Vec<_>>();
    let executor = CountingInlineExecutor::new();
    let config = task_config();
    let report = TaskClient::new(&NoProcesses, &config, &paths, &store, &executor)
        .reconcile_selected(&task_ids)
        .unwrap();
    assert_eq!(report.started_runners(), 0);
    assert_eq!(executor.starts(), 0);
}

#[test]
fn attached_json_drain_notice_is_one_event() {
    attached_waits_for_drain(AttachedPath::Submit, true, false);
}

#[test]
fn attached_deadline_leaves_each_path_queued_for_recovery() {
    for path in [
        AttachedPath::Submit,
        AttachedPath::Say,
        AttachedPath::Resume,
    ] {
        attached_waits_for_drain(path, false, true);
    }
}

#[test]
fn attached_waiter_exit_is_recovered_once_after_drain_off() {
    let _lock = CURRENT_DIR_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let repo = support::GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    let base_oid: BaseOid = git_text(&repo, &["rev-parse", "HEAD"]).parse().unwrap();
    let _cwd = CurrentDirGuard::enter(repo.root());
    let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
    let store = ClientStateStore::open(&paths.state).unwrap();
    let expected = plant_open_first_turn(&store, &project, base_oid.clone(), 11, 12);
    let task_id = expected.meta().task_id();
    mac_worker::test_support::controller::drain::set_drained(&paths.controller_state_root(), true)
        .unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &crate::support::libtest_name(module_path!(), "drain_exiting_waiter"),
            "--nocapture",
        ])
        .env("DRAIN_EXIT_ROOT", paths.state.parent().unwrap())
        .env("DRAIN_EXIT_REPO", repo.root())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "waiter: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let row = store.queue_entry_for_task_turn(task_id).unwrap().unwrap();
    assert_ne!(row.owner_opt().unwrap().pid(), std::process::id());
    assert!(
        !store
            .read_turn_prompt(task_id, row.job_id())
            .unwrap()
            .is_empty()
    );
    let host = CompletingHost::new(
        repo.root().to_path_buf(),
        base_oid,
        &expected,
        paths.controller_state_root(),
    );
    let config = task_config();
    let executor = CountingInlineExecutor::new();
    let client = TaskClient::new(&host, &config, &paths, &store, &executor);
    // First absence is unknown. Advance only the injected liveness clock to
    // prove the second absence, without a wall-clock sleep.
    assert_eq!(
        client
            .reconcile_selected(&[task_id])
            .unwrap()
            .started_runners(),
        0
    );
    assert_eq!(
        store
            .queue_entry(row.job_id())
            .unwrap()
            .unwrap()
            .owner_opt(),
        row.owner_opt()
    );
    store.advance_liveness_clock(
        mac_worker::test_support::client_state::RUNNER_ABSENCE_CONFIRMATION,
    );
    assert_eq!(
        client
            .reconcile_selected(&[task_id])
            .unwrap()
            .started_runners(),
        0
    );
    assert_eq!(host.submissions(), 0);
    assert_eq!(executor.starts(), 0);
    mac_worker::test_support::controller::drain::set_drained(&paths.controller_state_root(), false)
        .unwrap();
    assert_eq!(
        client
            .reconcile_selected(&[task_id])
            .unwrap()
            .started_runners(),
        1
    );
    assert_eq!(
        client
            .reconcile_selected(&[task_id])
            .unwrap()
            .started_runners(),
        0
    );
    let outcome = mac_worker::test_support::task::turn_runner::TurnRunner::new(
        &host, &config, &paths, &store, &executor,
    )
    .run(task_id, row.job_id(), None)
    .unwrap();
    assert_eq!(outcome.exit_code(), 0);
    assert_eq!(host.submissions(), 1);
    assert_eq!(executor.starts(), 1);
    assert!(store.queue_entry(row.job_id()).unwrap().is_none());
}

#[test]
fn drain_exiting_waiter() {
    let Some(root) = std::env::var_os("DRAIN_EXIT_ROOT") else {
        return;
    };
    let repo = PathBuf::from(std::env::var_os("DRAIN_EXIT_REPO").unwrap());
    let _cwd = CurrentDirGuard::enter(&repo);
    let paths = support::task_harness::paths(PathBuf::from(root));
    let store = ClientStateStore::open(&paths.state).unwrap();
    let expected = store.list_tasks().unwrap().remove(0);
    let prepared = PreparedFollowup::prepare(
        &expected,
        "exit during drain".into(),
        TurnId::generate(),
        4_000,
    )
    .unwrap();
    let host = CompletingHost::new(
        repo,
        expected.meta().base_oid().clone(),
        &expected,
        paths.controller_state_root(),
    );
    let executor = CountingInlineExecutor::new();
    let config = task_config();
    let exit_between_polls = |_: std::time::Duration| -> Result<(), WorkerError> {
        assert_eq!(host.submissions(), 0);
        assert_eq!(executor.starts(), 0);
        let row = store.queue_entry(prepared.turn_id()).unwrap().unwrap();
        assert_eq!(row.owner_opt().unwrap().pid(), std::process::id());
        assert!(row.slot_reservation().is_none());
        std::process::exit(0);
    };
    let _ = TaskClient::new(&host, &config, &paths, &store, &executor)
        .with_drain_wait(&exit_between_polls)
        .say_prepared(&prepared, true, &mut Vec::new(), &mut Vec::new());
    panic!("the child must exit in the drain wait before admission");
}

struct ReleaseJournalOnContention(Mutex<Option<std::fs::File>>);

impl mac_worker::test_support::client_state::ClientStateConcurrencyHook
    for ReleaseJournalOnContention
{
    fn reach(&self, point: mac_worker::test_support::client_state::ClientStateConcurrencyPoint) {
        if point == mac_worker::test_support::client_state::ClientStateConcurrencyPoint::RunnerLogContention {
            self.0.lock().unwrap().take();
        }
    }
}

#[test]
fn attached_parked_replay_retains_ownership() {
    attached_waits_for_drain(AttachedPath::ResumeParked, false, false);
}

#[test]
fn attached_contended_replay_retains_ownership() {
    attached_waits_for_drain(AttachedPath::ResumeContended, false, false);
}
