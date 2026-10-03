use crate::{support, task_state_fixture::TaskStateFixture};

use std::{
    ffi::OsStr,
    fs,
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    process::ExitStatus,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use mac_worker::test_support::{
    agents::{
        agent::{AgentKind, PermissionPolicy, PromptDelivery, TurnLaunch, TurnLimits},
        agent_facts::{AgentAuth, AgentFacts, AgentProbe},
    },
    client_state::{ClientStateStore, scheduler::WorkerPreference},
    core::{
        config::Config,
        error::WorkerError,
        paths::PathLayout,
        protocol::{MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION},
    },
    host::{
        job::{
            ClientId, CommandSpec, ExecutionScope, HostControlError, JobId, LeaseAcquireRequest,
            LeaseAcquireResponse, LeaseRecord, LeaseToken, RequestFingerprintMaterial,
            ResolveOrAbandonResponse, SubmitRequest,
        },
        job_service::{JobService, LaunchCandidate, SupervisorLauncher},
        lease::{AdmissionFacts, LeaseService, SlotState},
        process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
        store::{HostStore, HostStoreWritePoint, SupervisorGuard},
    },
    session::{HOST_FEATURE_SESSION_IMPORT, SessionAgent, SessionImportMeta, imported_session_id},
    task::{
        client::{TaskClient, TaskSubmitRequest},
        model::{
            BaseOid, ClosePolicy, GitIdentity, PublishMode, TaskId, TaskLimits, TaskMeta,
            TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnId, TurnSummary,
            TurnTerminal,
        },
        store::{
            SessionBinding, TaskPrepareRequest, TaskPrepareResponse, TaskSessionResponse,
            TaskStatusResponse, TaskStore,
        },
        turn::{TaskTurnRequest, TurnMaterial},
        turn_runner::{InlineRunnerExecutor, TurnRunner},
    },
    transfer::HostOperation,
};
use uuid::Uuid;

const PROJECT: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const WORKTREE: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const PROMPT: &str = "continue the synthetic conversation";

fn import(agent: AgentKind) -> SessionImportMeta {
    SessionImportMeta::new(
        SessionAgent::from_agent_kind(agent).unwrap(),
        "a".repeat(40),
        "1.2.3",
    )
    .unwrap()
}

fn decode<T: serde::de::DeserializeOwned>(request: &ProcessRequest) -> T {
    serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap()
}

fn response<T: serde::Serialize>(value: &T) -> Result<ProcessResult, WorkerError> {
    let mut stdout = serde_json::to_vec(value).unwrap();
    stdout.push(b'\n');
    Ok(ProcessResult {
        status: ExitStatus::from_raw(0),
        stdout,
        stderr: vec![],
    })
}

fn boundary() -> WorkerError {
    WorkerError::Protocol("fixture boundary reached".into())
}

fn host_failure(code: &'static str) -> Result<ProcessResult, WorkerError> {
    let mut stdout =
        serde_json::to_vec(&HostControlError::new(code, "fixture boundary reached").unwrap())
            .unwrap();
    stdout.push(b'\n');
    Ok(ProcessResult {
        status: ExitStatus::from_raw(1 << 8),
        stdout,
        stderr: vec![],
    })
}

// The fake host stops at lease acquisition (argv tests) or submission
// (ordering tests), so no native agent, SSH or supervisor process is run.
struct RunnerHost {
    requests: Mutex<Vec<ProcessRequest>>,
    stop_at_lease: bool,
    wrong_binding: bool,
    session_override: Mutex<Option<String>>,
    prepared: Mutex<Option<(TaskMeta, TurnId)>>,
}

impl RunnerHost {
    fn new(stop_at_lease: bool, wrong_binding: bool) -> Self {
        Self {
            requests: Mutex::new(vec![]),
            stop_at_lease,
            wrong_binding,
            session_override: Mutex::new(None),
            prepared: Mutex::new(None),
        }
    }

    fn calls(&self) -> Vec<ProcessRequest> {
        self.requests.lock().unwrap().clone()
    }

    fn leases(&self) -> Vec<LeaseAcquireRequest> {
        self.calls()
            .iter()
            .filter(|call| is_op(call, HostOperation::LeaseAcquire))
            .map(decode)
            .collect()
    }
}

fn is_op(request: &ProcessRequest, op: HostOperation) -> bool {
    request.args.iter().any(|arg| arg == op.command())
}

impl ProcessRunner for RunnerHost {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.requests.lock().unwrap().push(request.clone());
        if request.program == OsStr::new("/usr/bin/git") {
            if request.args.iter().any(|arg| arg == "push")
                && request
                    .args
                    .iter()
                    .any(|arg| arg.to_string_lossy().starts_with("--receive-pack="))
            {
                return response(&serde_json::json!({}));
            }
            return SystemProcessRunner.run(request);
        }
        assert_eq!(request.program, OsStr::new("/usr/bin/ssh"));
        let op = request.args.last().unwrap().to_str().unwrap();
        if op == "~/.local/bin/worker host probe" {
            return response(&ProbeResponse {
                features: Some(vec![HOST_FEATURE_SESSION_IMPORT.into()]),
                protocol_version: PROTOCOL_VERSION,
                supervision_version: SUPERVISION_VERSION,
                hostname: "mini-1.local".into(),
                arch: "arm64".into(),
                os_version: "26.2".into(),
                free_disk_bytes: 100 << 30,
                total_disk_bytes: 250 << 30,
                memory_pressure: MemoryPressure::Normal,
                swap_used_bytes: Some(0),
                available_memory_bytes: Some(12 << 30),
                cpu_counters: None,
                slot_state: SlotState::Idle,
                active_lease: None,
                capabilities: vec![],
                agent_facts: Some(AgentFacts {
                    agents: ["claude", "codex"]
                        .into_iter()
                        .map(|name| AgentProbe {
                            autoupdate: None,
                            name: name.into(),
                            version: Some("1.2.3".into()),
                            auth: AgentAuth::Authenticated,
                            auth_by_profile: vec![],
                        })
                        .collect(),
                    env_profiles: vec![],
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
            });
        }
        if op == HostOperation::LeaseAcquire.command() {
            if self.stop_at_lease {
                return Err(boundary());
            }
            let acquire: LeaseAcquireRequest = decode(request);
            let material = acquire.material();
            return response(&LeaseAcquireResponse::Acquired {
                lease: LeaseRecord::new(
                    material,
                    acquire.request_fingerprint().clone(),
                    material.created_at_millis(),
                    material.created_at_millis() + material.timeout_millis(),
                )?,
            });
        }
        if op == HostOperation::TaskPrepare.command() {
            let prepare: TaskPrepareRequest = decode(request);
            *self.prepared.lock().unwrap() = Some((prepare.meta().clone(), prepare.job_id()));
            return response(&TaskPrepareResponse::new(
                prepare.meta().base_oid().clone(),
                false,
            ));
        }
        if op == HostOperation::TaskSession.command() {
            let prepared = self.prepared.lock().unwrap();
            let (meta, _) = prepared
                .as_ref()
                .expect("session lookup must follow prepare");
            let id = self
                .session_override
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| {
                    if self.wrong_binding {
                        Uuid::from_u128(999).to_string()
                    } else {
                        imported_session_id(&meta.task_id())
                    }
                });
            return response(&TaskSessionResponse::new(SessionBinding::new(
                meta.agent(),
                id,
                1,
            )?));
        }
        if op == HostOperation::TaskStatus.command() {
            let prepared = self.prepared.lock().unwrap();
            let Some((meta, turn_id)) = prepared.as_ref() else {
                return host_failure("TASK_NOT_FOUND");
            };
            return response(&TaskStatusResponse::new(TaskStatus::new(
                TaskState::Active,
                None,
                Some("mini-1".into()),
                meta.session_import().is_some(),
                Some(meta.base_oid().clone()),
                None,
                vec![],
                vec![],
                None,
                vec![TurnSummary::new(
                    1,
                    *turn_id,
                    None,
                    None,
                    None,
                    false,
                    Some(meta.created_at_millis()),
                    None,
                )],
                meta.created_at_millis(),
            )?));
        }
        if op == HostOperation::TaskTurn.command() {
            return host_failure("TASK_BUSY");
        }
        if op == HostOperation::Status.command() {
            return host_failure("JOB_ABANDONED");
        }
        if op == HostOperation::ResolveOrAbandon.command() {
            return response(&ResolveOrAbandonResponse::abandoned());
        }
        panic!("unexpected fake host operation: {op}");
    }
}

struct RunnerFixture {
    _repo: support::GitRepo,
    _root: tempfile::TempDir,
    paths: PathLayout,
    state: ClientStateStore,
    config: Config,
    host: RunnerHost,
    task: TaskId,
    turn: TurnId,
}

impl RunnerFixture {
    fn new(agent: AgentKind, imported: bool, stop_at_lease: bool, wrong_binding: bool) -> Self {
        let repo = support::GitRepo::init();
        repo.write("base.txt", b"base\n");
        repo.commit_all("base");
        let root = tempfile::tempdir().unwrap();
        let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
        let state = ClientStateStore::open(&paths.state).unwrap();
        let config = Config::parse(
            "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
        )
        .unwrap();
        let host = RunnerHost::new(stop_at_lease, wrong_binding);
        let result = TaskClient::new(&host, &config, &paths, &state, &InlineRunnerExecutor)
            .submit(
                TaskSubmitRequest {
                    session_import: imported.then(|| import(agent)),
                    questions: None,
                    agent,
                    model: None,
                    effort: None,
                    prompt: PROMPT.into(),
                    project: repo.root().to_path_buf(),
                    base: "main".into(),
                    wip: true,
                    source: None,
                    publish: None,
                    publish_branch: None,
                    cli_includes: vec![],
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
                &mut vec![],
                &mut vec![],
            )
            .unwrap();
        let task = result.task_id();
        let turn = state
            .queue_entry_for_task_turn(task)
            .unwrap()
            .unwrap()
            .job_id();
        Self {
            _repo: repo,
            _root: root,
            paths,
            state,
            config,
            host,
            task,
            turn,
        }
    }

    fn run(&self) -> WorkerError {
        TurnRunner::new(
            &self.host,
            &self.config,
            &self.paths,
            &self.state,
            &InlineRunnerExecutor,
        )
        .run(self.task, self.turn, None)
        .unwrap_err()
    }

    fn assert_imported_argv(&self, agent: AgentKind) {
        let leases = self.host.leases();
        let lease = leases.last().expect("runner must reach lease acquisition");
        let shell = match lease.material().command() {
            CommandSpec::Shell { shell } => shell,
            other => panic!("unexpected command: {other:?}"),
        };
        let id = imported_session_id(&self.task);
        match agent {
            AgentKind::Claude => {
                assert!(shell.contains("--resume"), "{shell}");
                assert!(shell.contains("--verbose"), "{shell}");
                assert!(!shell.contains("--session-id"), "{shell}");
            }
            AgentKind::Codex => {
                assert!(shell.contains("'exec' 'resume'"), "{shell}");
            }
            _ => unreachable!(),
        }
        assert!(shell.contains(&id), "{shell}");
        let calls = self.host.calls();
        let lease_index = calls
            .iter()
            .position(|call| is_op(call, HostOperation::LeaseAcquire))
            .unwrap();
        assert!(
            calls[..lease_index]
                .iter()
                .all(|call| !is_op(call, HostOperation::TaskSession))
        );
        assert!(
            calls
                .iter()
                .all(|call| !is_op(call, HostOperation::TaskPrebind))
        );
    }
}

#[test]
fn imported_claude_argv_is_built_locally_before_the_lease() {
    let fixture = RunnerFixture::new(AgentKind::Claude, true, true, false);
    fixture.run();
    fixture.assert_imported_argv(AgentKind::Claude);
}

#[test]
fn imported_codex_argv_is_built_locally_before_the_lease() {
    let fixture = RunnerFixture::new(AgentKind::Codex, true, true, false);
    fixture.run();
    fixture.assert_imported_argv(AgentKind::Codex);
}

#[test]
fn crashed_imported_first_turn_replays_with_the_same_lease_fingerprint() {
    let fixture = RunnerFixture::new(AgentKind::Claude, true, true, false);
    fixture.run();
    let first = fixture.host.leases().last().unwrap().clone();
    // A crash can leave a turn-1 history entry and a mutable session-present
    // projection. Neither is a reason to reclassify the import as FollowUp.
    let record = fixture.state.load_task(fixture.task).unwrap();
    let status = TaskStatus::new(
        TaskState::Active,
        None,
        Some("mini-1".into()),
        true,
        Some(record.meta().base_oid().clone()),
        None,
        vec![],
        vec![],
        None,
        vec![TurnSummary::new(
            1,
            fixture.turn,
            None,
            None,
            None,
            false,
            Some(record.meta().created_at_millis()),
            None,
        )],
        record.meta().created_at_millis(),
    )
    .unwrap();
    fixture
        .state
        .replace_task_fixture(record.with_status(status).unwrap())
        .unwrap();
    fixture.run();
    fixture.assert_imported_argv(AgentKind::Claude);
    assert_eq!(fixture.host.leases().len(), 2);
    assert_eq!(fixture.host.leases().last().unwrap(), &first);
}

#[test]
#[ignore = "wave-2 baseline rejects SessionRefPush; run after W6 integration"]
fn imported_first_turn_pushes_prepares_verifies_then_submits() {
    let fixture = RunnerFixture::new(AgentKind::Codex, true, false, false);
    let error = fixture.run();
    assert_eq!(error.public_code(), "TASK_BUSY", "{error}");
    fixture.assert_imported_argv(AgentKind::Codex);
    let calls = fixture.host.calls();
    let index = |op| calls.iter().position(|call| is_op(call, op)).unwrap();
    let push = calls
        .iter()
        .position(|call| {
            call.program == OsStr::new("/usr/bin/git") && call.args.iter().any(|arg| arg == "push")
        })
        .unwrap();
    assert!(index(HostOperation::LeaseAcquire) < push);
    assert!(push < index(HostOperation::TaskPrepare));
    assert!(index(HostOperation::TaskPrepare) < index(HostOperation::TaskSession));
    assert!(index(HostOperation::TaskSession) < index(HostOperation::TaskTurn));
    assert!(calls[push].args.iter().any(|arg| arg == "--atomic"));
    assert!(calls[push].args.iter().any(|arg| arg.to_string_lossy()
        == format!(
            "{}:refs/mac-worker/sessions/{}",
            "a".repeat(40),
            fixture.task
        )));
    let submitted: TaskTurnRequest = decode(&calls[index(HostOperation::TaskTurn)]);
    assert!(submitted.turn().resume());
    assert_eq!(submitted.turn().turn_number(), 1);
}

#[test]
#[ignore = "wave-2 baseline rejects SessionRefPush; run after W6 integration"]
fn imported_prepare_binding_mismatch_fails_before_submission() {
    let fixture = RunnerFixture::new(AgentKind::Claude, true, false, true);
    assert_eq!(fixture.run().public_code(), "SESSION_PLACEMENT_FAILED");
    let calls = fixture.host.calls();
    assert!(
        calls
            .iter()
            .any(|call| is_op(call, HostOperation::TaskPrepare))
    );
    assert!(
        calls
            .iter()
            .any(|call| is_op(call, HostOperation::TaskSession))
    );
    assert!(
        calls
            .iter()
            .all(|call| !is_op(call, HostOperation::TaskTurn))
    );
}

#[test]
fn fresh_codex_still_prepares_without_session_lookup_or_resume() {
    let fixture = RunnerFixture::new(AgentKind::Codex, false, false, false);
    assert_eq!(fixture.run().public_code(), "TASK_BUSY");
    let calls = fixture.host.calls();
    assert!(
        calls
            .iter()
            .any(|call| is_op(call, HostOperation::TaskPrepare))
    );
    assert!(
        calls
            .iter()
            .all(|call| !is_op(call, HostOperation::TaskSession)
                && !is_op(call, HostOperation::TaskPrebind))
    );
    let submitted: TaskTurnRequest = decode(
        calls
            .iter()
            .find(|call| is_op(call, HostOperation::TaskTurn))
            .unwrap(),
    );
    assert!(!submitted.turn().resume());
}

#[test]
fn imported_follow_up_uses_the_bound_ref_and_does_not_prepare_or_push() {
    let fixture = RunnerFixture::new(AgentKind::Codex, true, false, false);
    let record = fixture.state.load_task(fixture.task).unwrap();
    let status = TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::NeedsInput),
        Some("mini-1".into()),
        true,
        Some(record.meta().base_oid().clone()),
        Some("need a synthetic detail".into()),
        vec![],
        vec![],
        None,
        vec![TurnSummary::new(
            1,
            TurnId::new(Uuid::from_u128(999)),
            Some(TurnTerminal::Succeeded),
            Some(TaskOutcome::NeedsInput),
            Some(true),
            false,
            Some(record.meta().created_at_millis()),
            Some(record.meta().created_at_millis() + 1),
        )],
        record.meta().created_at_millis() + 1,
    )
    .unwrap();
    *fixture.host.prepared.lock().unwrap() = Some((record.meta().clone(), fixture.turn));
    *fixture.host.session_override.lock().unwrap() = Some("native-follow-up-session".into());
    fixture
        .state
        .replace_task_fixture(record.with_status(status).unwrap())
        .unwrap();
    assert_eq!(fixture.run().public_code(), "TASK_BUSY");
    let calls = fixture.host.calls();
    let session = calls
        .iter()
        .position(|call| is_op(call, HostOperation::TaskSession))
        .unwrap();
    let lease = calls
        .iter()
        .position(|call| is_op(call, HostOperation::LeaseAcquire))
        .unwrap();
    assert!(session < lease);
    assert!(
        calls
            .iter()
            .all(|call| !is_op(call, HostOperation::TaskPrepare)
                && !is_op(call, HostOperation::TaskPrebind))
    );
    assert!(
        calls
            .iter()
            .all(|call| !call.args.iter().any(|arg| arg == "push"))
    );
    let submitted: TaskTurnRequest = decode(
        calls
            .iter()
            .find(|call| is_op(call, HostOperation::TaskTurn))
            .unwrap(),
    );
    assert!(submitted.turn().resume());
    assert_eq!(submitted.turn().turn_number(), 2);
    let leases = fixture.host.leases();
    let CommandSpec::Shell { shell } = leases.last().unwrap().material().command() else {
        panic!("expected shell");
    };
    assert!(shell.contains("native-follow-up-session"), "{shell}");
    assert!(
        !shell.contains(&imported_session_id(&fixture.task)),
        "{shell}"
    );
}

struct BoundaryLauncher(AtomicUsize);
impl SupervisorLauncher for BoundaryLauncher {
    fn launch(&self, _job: JobId, _guard: SupervisorGuard) -> Result<LaunchCandidate, WorkerError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(boundary())
    }
}

struct HostFixture {
    root: tempfile::TempDir,
    store: HostStore,
    request: TaskTurnRequest,
    task: TaskId,
}

impl HostFixture {
    fn new(
        agent: AgentKind,
        imported: bool,
        resume: bool,
        binding: Option<(AgentKind, String)>,
    ) -> Self {
        let root = tempfile::tempdir().unwrap();
        let store = HostStore::open(&root.path().join("host")).unwrap();
        let repo = support::GitRepo::init();
        repo.write("base.txt", b"base\n");
        repo.commit_all("base");
        let base: BaseOid = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let task = TaskId::new(Uuid::from_u128(1));
        let job = JobId::new(Uuid::from_u128(3));
        let mirror = store.mirror(PROJECT).unwrap();
        assert!(
            repo.git(&[
                "push",
                mirror.path().to_str().unwrap(),
                &format!("HEAD:refs/mac-worker/bases/{task}")
            ])
            .status
            .success()
        );
        let limits = TurnLimits::new(30_000, None, None).unwrap();
        let turn = TurnMaterial::from_prompt(
            task,
            1,
            agent,
            None,
            None,
            PermissionPolicy::Workspace,
            limits.clone(),
            base.clone(),
            PROMPT,
            None,
            Uuid::from_u128(2),
            resume,
        )
        .unwrap();
        let launch = TurnLaunch::new(
            "/bin/sh",
            vec!["-c".into(), "true".into()],
            PromptDelivery::Stdin,
            vec![],
            false,
        );
        let seed = RequestFingerprintMaterial::new(
            job,
            ClientId::new(Uuid::from_u128(4)),
            LeaseToken::new(Uuid::from_u128(5)),
            100,
            "mini-1".into(),
            PROJECT.into(),
            WORKTREE.into(),
            turn.digest(),
            String::new(),
            30_000,
            "heavy".into(),
            CommandSpec::shell("true".into()).unwrap(),
        )
        .unwrap();
        let lease = LeaseRecord::new(&seed, seed.fingerprint(), 100, 30_100).unwrap();
        let projected = turn.v1_material(&lease, &launch).unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        LeaseService::new(&store)
            .acquire(
                &LeaseAcquireRequest::new(projected.clone())
                    .with_execution_scope(ExecutionScope::task(task)),
                &AdmissionFacts {
                    free_disk_bytes: 100 << 30,
                    total_disk_bytes: 200 << 30,
                    memory_pressure: MemoryPressure::Normal,
                    swap_used_bytes: Some(0),
                },
                now,
            )
            .unwrap();
        let meta = TaskMeta::new(TaskMetaInput {
            session_import: None,
            task_id: task,
            run_id: None,
            project_id: PROJECT.into(),
            worktree_id: WORKTREE.into(),
            agent,
            model: None,
            effort: None,
            policy: PermissionPolicy::Workspace,
            source: TaskSource::Local {
                wip: false,
                push_target: None,
            },
            publish: vec![PublishMode::Fetch],
            publish_branch: None,
            base_oid: base,
            limits: TaskLimits::new(limits, 3).unwrap(),
            close_policy: ClosePolicy::Never,
            env_profile: None,
            git_identity: GitIdentity::new("Fixture", "fixture@example.test").unwrap(),
            title: None,
            prompt: PROMPT.into(),
            created_at_millis: 100,
        })
        .unwrap();
        let task_store = TaskStore::new(&store, &SystemProcessRunner);
        let admission = store.admission_lock(job).unwrap();
        let transfer = store.transfer_lock_after(&admission, job).unwrap();
        task_store
            .prepare(
                &TaskPrepareRequest::new(meta.clone(), job, "mini-1"),
                &transfer,
            )
            .unwrap();
        drop(transfer);
        drop(admission);
        // Model the post-placement task directly: this isolates host acceptance
        // from the parallel W5 placement implementation and native stores.
        if imported {
            let mut value = serde_json::to_value(&meta).unwrap();
            value["session_import"] = serde_json::to_value(import(agent)).unwrap();
            let meta: TaskMeta = serde_json::from_value(value).unwrap();
            fs::write(
                store.task_dir(PROJECT, task).unwrap().join("meta.json"),
                serde_json::to_vec(&meta).unwrap(),
            )
            .unwrap();
        }
        if let Some((agent, id)) = binding {
            task_store
                .bind_session(PROJECT, task, SessionBinding::new(agent, id, 100).unwrap())
                .unwrap();
        }
        let request = TaskTurnRequest::new(
            SubmitRequest::new(projected).with_execution_scope(ExecutionScope::task(task)),
            turn,
            PROMPT,
        );
        Self {
            root,
            store,
            request,
            task,
        }
    }

    fn set_binding(&self, binding: Option<(AgentKind, String)>) {
        let path = self
            .store
            .task_dir(PROJECT, self.task)
            .unwrap()
            .join("session.json");
        match binding {
            None => fs::remove_file(path).unwrap(),
            Some((agent, id)) => {
                fs::write(
                    &path,
                    serde_json::to_vec(&SessionBinding::new(agent, id, 100).unwrap()).unwrap(),
                )
                .unwrap();
                fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
            }
        }
    }
}

fn correct_binding(agent: AgentKind) -> Option<(AgentKind, String)> {
    Some((agent, imported_session_id(&TaskId::new(Uuid::from_u128(1)))))
}

#[test]
fn host_rejects_imported_first_turn_with_resume_false() {
    for agent in [AgentKind::Claude, AgentKind::Codex] {
        let fixture = HostFixture::new(agent, true, false, correct_binding(agent));
        let launcher = BoundaryLauncher(AtomicUsize::new(0));
        assert_eq!(
            JobService::new(&fixture.store, &launcher)
                .submit_turn(fixture.request)
                .unwrap_err()
                .public_code(),
            "TASK_SESSION_CONFLICT"
        );
        assert_eq!(launcher.0.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn host_rejects_imported_first_turn_with_missing_binding() {
    let fixture = HostFixture::new(AgentKind::Codex, true, true, None);
    let launcher = BoundaryLauncher(AtomicUsize::new(0));
    assert_eq!(
        JobService::new(&fixture.store, &launcher)
            .submit_turn(fixture.request)
            .unwrap_err()
            .public_code(),
        "SESSION_UNBOUND"
    );
    assert_eq!(launcher.0.load(Ordering::SeqCst), 0);
}

#[test]
fn host_rejects_imported_first_turn_with_wrong_binding_agent_or_ref() {
    for binding in [
        (
            AgentKind::Claude,
            imported_session_id(&TaskId::new(Uuid::from_u128(1))),
        ),
        (AgentKind::Codex, Uuid::from_u128(999).to_string()),
    ] {
        let fixture = HostFixture::new(AgentKind::Codex, true, true, Some(binding));
        let launcher = BoundaryLauncher(AtomicUsize::new(0));
        assert_eq!(
            JobService::new(&fixture.store, &launcher)
                .submit_turn(fixture.request)
                .unwrap_err()
                .public_code(),
            "TASK_SESSION_CONFLICT"
        );
        assert_eq!(launcher.0.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn host_accepts_imported_resume_without_claude_seed_binding() {
    for agent in [AgentKind::Claude, AgentKind::Codex] {
        let fixture = HostFixture::new(agent, true, true, correct_binding(agent));
        let launcher = BoundaryLauncher(AtomicUsize::new(0));
        let error = JobService::new(&fixture.store, &launcher)
            .submit_turn(fixture.request)
            .unwrap_err();
        assert!(
            error.to_string().contains("fixture boundary reached"),
            "{error}"
        );
        assert_eq!(launcher.0.load(Ordering::SeqCst), 1);
        assert_eq!(
            TaskStore::new(&fixture.store, &SystemProcessRunner)
                .session(PROJECT, fixture.task)
                .unwrap()
                .unwrap()
                .session_ref(),
            imported_session_id(&fixture.task)
        );
    }
}

#[test]
fn host_repair_rechecks_imported_binding_before_recording_acceptance() {
    for point in [
        HostStoreWritePoint::AfterJobRename,
        HostStoreWritePoint::AfterJobPublish,
    ] {
        let fixture = HostFixture::new(
            AgentKind::Codex,
            true,
            true,
            correct_binding(AgentKind::Codex),
        );
        let launcher = BoundaryLauncher(AtomicUsize::new(0));
        let faulted =
            HostStore::open_with_write_fault(&fixture.root.path().join("host"), point).unwrap();
        JobService::new(&faulted, &launcher)
            .submit_turn(fixture.request.clone())
            .unwrap_err();
        assert_eq!(launcher.0.load(Ordering::SeqCst), 0);
        fixture.set_binding(Some((AgentKind::Codex, Uuid::from_u128(999).to_string())));
        assert_eq!(
            JobService::new(&fixture.store, &launcher)
                .submit_turn(fixture.request.clone())
                .unwrap_err()
                .public_code(),
            "TASK_SESSION_CONFLICT"
        );
        assert_eq!(launcher.0.load(Ordering::SeqCst), 0);
        fixture.set_binding(None);
        assert_eq!(
            JobService::new(&fixture.store, &launcher)
                .submit_turn(fixture.request.clone())
                .unwrap_err()
                .public_code(),
            "SESSION_UNBOUND"
        );
        assert_eq!(launcher.0.load(Ordering::SeqCst), 0);
        fixture.set_binding(correct_binding(AgentKind::Codex));
        let error = JobService::new(&fixture.store, &launcher)
            .submit_turn(fixture.request)
            .unwrap_err();
        assert!(
            error.to_string().contains("fixture boundary reached"),
            "{error}"
        );
        assert_eq!(launcher.0.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn host_accepted_replay_rechecks_imported_binding() {
    let fixture = HostFixture::new(
        AgentKind::Claude,
        true,
        true,
        correct_binding(AgentKind::Claude),
    );
    let launcher = BoundaryLauncher(AtomicUsize::new(0));
    JobService::new(&fixture.store, &launcher)
        .submit_turn(fixture.request.clone())
        .unwrap_err();
    assert_eq!(launcher.0.load(Ordering::SeqCst), 1);
    fixture.set_binding(Some((AgentKind::Claude, Uuid::from_u128(999).to_string())));
    assert_eq!(
        JobService::new(&fixture.store, &launcher)
            .submit_turn(fixture.request.clone())
            .unwrap_err()
            .public_code(),
        "TASK_SESSION_CONFLICT"
    );
    assert_eq!(launcher.0.load(Ordering::SeqCst), 1);
    fixture.set_binding(None);
    assert_eq!(
        JobService::new(&fixture.store, &launcher)
            .submit_turn(fixture.request)
            .unwrap_err()
            .public_code(),
        "SESSION_UNBOUND"
    );
}

#[test]
fn non_imported_prebound_first_turn_remains_accepted() {
    let fixture = HostFixture::new(
        AgentKind::Cursor,
        false,
        false,
        Some((AgentKind::Cursor, "cursor-prebound-session".into())),
    );
    let launcher = BoundaryLauncher(AtomicUsize::new(0));
    let error = JobService::new(&fixture.store, &launcher)
        .submit_turn(fixture.request)
        .unwrap_err();
    assert!(
        error.to_string().contains("fixture boundary reached"),
        "{error}"
    );
    assert_eq!(launcher.0.load(Ordering::SeqCst), 1);
}
