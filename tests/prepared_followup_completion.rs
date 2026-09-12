//! Real TurnRunner completion proof for prepared follow-up replay.
//!
//! Fake boundary: SSH/host transport and agent only. `TaskClient::say_prepared`
//! still uses the real queue, prompt tree, `TurnRunner` drain/import/finalize,
//! and normal prompt/queue retirement. The follow-up is not seeded as a
//! completed `LocalTaskRecord`.
//!
//! Immutable 623 is a known red at exact replay after HEAD moves. A focused
//! failure there is the baseline; do not treat a later production correction
//! as this tests-only checkpoint.

#[allow(dead_code)]
mod support;

use std::{
    ffi::{OsStr, OsString},
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy},
    agent_facts::{AgentAuth, AgentFacts, AgentProbe, ProfileProbe},
    client_state::ClientStateStore,
    config::Config,
    error::WorkerError,
    job::{
        JobMeta, JobState, JobStatus, LeaseAcquireRequest, LeaseAcquireResponse, LeaseRecord,
        LogChunk, LogChunkRequest, LogStream, StatusRequest, StatusResponse, SubmitResponse,
    },
    lease::SlotState,
    paths::PathLayout,
    prepared_followup::PreparedFollowup,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    project_state::ProjectState,
    protocol::{CpuCounters, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION},
    task::{
        BaseOid, ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, RunnerIdentity, TaskId,
        TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus,
        TurnId, TurnSummary, TurnTerminal,
    },
    task_client::TaskClient,
    task_store::{
        SessionBinding, TaskPrepareRequest, TaskPrepareResponse, TaskSessionRequest,
        TaskSessionResponse, TaskStatusRequest, TaskStatusResponse,
    },
    transfer::HostOperation,
    transfer_repo::repo_id_for,
    turn::{TaskTurnRequest, TaskTurnResponse},
    turn_runner::{InlineRunnerExecutor, RunnerExecutor},
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
}

impl CompletingHost {
    fn new(user_repo: PathBuf, result_oid: BaseOid, prior: &LocalTaskRecord) -> Self {
        Self {
            user_repo,
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
            TaskState::Open,
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
            policy: mac_worker::process::ProcessPolicy {
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
                canonical_process(&mac_worker::job::LogChunkResponse::new(LogChunk::new(
                    chunk_request.stream(),
                    chunk_request.offset(),
                    bytes,
                )?)?)
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

fn commit_result_without_moving_head(repo: &support::GitRepo) -> BaseOid {
    let base = git_text(repo, &["rev-parse", "HEAD"]);
    repo.write("agent.txt", b"follow-up change\n");
    repo.commit_all("follow-up result");
    let result = git_text(repo, &["rev-parse", "HEAD"]);
    assert!(
        repo.git(&["reset", "--hard", &base]).status.success(),
        "restore frozen base HEAD"
    );
    assert_ne!(
        result, base,
        "result commit must differ from the frozen base"
    );
    result.parse().unwrap()
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

fn public_code(error: &WorkerError) -> String {
    error.public_code()
}

/// Completes follow-up N through real TurnRunner import + retirement.
///
/// This must stay green on 623; it is the fixture, not the known-red replay.
#[test]
fn real_followup_completion_imports_changed_head_and_retires_prompt_queue() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    let base_oid: BaseOid = git_text(&repo, &["rev-parse", "HEAD"]).parse().unwrap();
    let result_oid = commit_result_without_moving_head(&repo);
    let _cwd = CurrentDirGuard::enter(repo.root());
    let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let state_root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
    let store = ClientStateStore::open(&paths.state).unwrap();
    let expected = plant_open_first_turn(&store, &project, base_oid.clone(), 11, 12);
    let prepared = PreparedFollowup::prepare(
        &expected,
        "follow up".to_owned(),
        TurnId::new(Uuid::from_u128(13)),
        4_000,
    )
    .unwrap();
    assert_eq!(prepared.base_oid(), &base_oid);

    let host = CompletingHost::new(repo.root().to_path_buf(), result_oid.clone(), &expected);
    let executor = CountingInlineExecutor::new();
    let config = task_config();
    let client = TaskClient::new(&host, &config, &paths, &store, &executor);
    let report = client
        .say_prepared(&prepared, true, &mut Vec::new(), &mut Vec::new())
        .expect("attached say_prepared must run the real TurnRunner completion path");

    let last = report.status().turns().last().unwrap();
    assert_eq!(last.turn_id(), prepared.turn_id());
    assert_eq!(last.terminal(), Some(TurnTerminal::Succeeded));
    assert_eq!(last.outcome(), Some(&TaskOutcome::Done));
    assert_eq!(report.status().state(), TaskState::Open);
    assert_eq!(report.status().head_oid(), Some(&result_oid));
    assert_ne!(
        report.status().head_oid(),
        Some(prepared.base_oid()),
        "completion must advance HEAD off the frozen follow-up base"
    );
    assert!(
        store
            .queue_entry_for_task_turn(prepared.task_id())
            .unwrap()
            .is_none(),
        "normal completion must retire the queue row"
    );
    assert!(
        store
            .read_turn_prompt(prepared.task_id(), prepared.turn_id())
            .is_err(),
        "normal completion must retire the prompt"
    );
    assert_eq!(host.submissions(), 1);
    assert_eq!(executor.starts(), 1);
}

/// Exact original PreparedFollowup after real completion: same task/turn,
/// terminal outcome, exact result head, no second execution, no prompt/queue.
/// Different self-consistent same-N preparation must conflict.
///
/// 623 red: `resume_prepared_followup` compares live `head_oid` to
/// `prepared.base_oid()` before the terminal early-return.
#[test]
fn exact_prepared_replay_after_real_completion_and_reject_different_same_n() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    let base_oid: BaseOid = git_text(&repo, &["rev-parse", "HEAD"]).parse().unwrap();
    let result_oid = commit_result_without_moving_head(&repo);
    let _cwd = CurrentDirGuard::enter(repo.root());
    let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let state_root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
    let store = ClientStateStore::open(&paths.state).unwrap();
    let expected = plant_open_first_turn(&store, &project, base_oid, 21, 22);
    let prepared = PreparedFollowup::prepare(
        &expected,
        "follow up".to_owned(),
        TurnId::new(Uuid::from_u128(23)),
        5_000,
    )
    .unwrap();
    let host = CompletingHost::new(repo.root().to_path_buf(), result_oid.clone(), &expected);
    let executor = CountingInlineExecutor::new();
    let config = task_config();
    let client = TaskClient::new(&host, &config, &paths, &store, &executor);
    client
        .say_prepared(&prepared, true, &mut Vec::new(), &mut Vec::new())
        .expect("real completion must succeed before the replay proof");
    assert_eq!(host.submissions(), 1);
    assert_eq!(executor.starts(), 1);

    let persisted = serde_json::to_vec(&prepared).unwrap();
    let store = ClientStateStore::open(&paths.state).unwrap();
    let executor = CountingInlineExecutor::new();
    let client = TaskClient::new(&host, &config, &paths, &store, &executor);
    let replayed: PreparedFollowup = serde_json::from_slice(&persisted).unwrap();
    assert_eq!(replayed, prepared);

    let replay = client.say_prepared(&replayed, true, &mut Vec::new(), &mut Vec::new());
    let report = replay.unwrap_or_else(|error| {
        panic!(
            "exact prepared replay after real completion must report the durable terminal turn without re-executing; 623 fails here on mutable HEAD vs frozen base ({})",
            public_code(&error)
        );
    });
    assert_eq!(
        report.status().turns().last().unwrap().turn_id(),
        prepared.turn_id()
    );
    assert_eq!(
        report.status().turns().last().unwrap().terminal(),
        Some(TurnTerminal::Succeeded)
    );
    assert_eq!(report.status().head_oid(), Some(&result_oid));
    assert_eq!(host.submissions(), 1);
    assert_eq!(
        executor.starts(),
        0,
        "terminal replay must not start another runner"
    );
    assert!(
        store
            .queue_entry_for_task_turn(prepared.task_id())
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .read_turn_prompt(prepared.task_id(), prepared.turn_id())
            .is_err()
    );

    let other = PreparedFollowup::prepare(
        prepared.expected(),
        "different follow up".to_owned(),
        prepared.turn_id(),
        prepared.created_at_millis(),
    )
    .unwrap();
    assert_ne!(other, prepared);
    let error = client
        .say_prepared(&other, false, &mut Vec::new(), &mut Vec::new())
        .expect_err("a different self-consistent same-N preparation must conflict");
    assert_eq!(public_code(&error), "TASK_REVISION_CONFLICT");
    assert_eq!(host.submissions(), 1);
}
