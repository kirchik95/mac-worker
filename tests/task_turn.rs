#[allow(dead_code)]
mod support;

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    process::{self, Command},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use mac_worker::{
    agent::{
        AgentKind, PermissionPolicy, PromptDelivery, Question, TurnLaunch, TurnLimits, TurnParams,
        adapter_for,
    },
    error::WorkerError,
    host_store::HostStore,
    job::{
        ClientId, CommandSpec, JobId, JobState, JobStatus, LeaseAcquireRequest, LeaseRecord,
        LeaseToken, RequestFingerprintMaterial,
    },
    job_service::{JobService, LaunchCandidate, SupervisorLauncher},
    lease::{AdmissionFacts, LeaseService},
    protocol::{MemoryPressure, PROTOCOL_VERSION, SUPERVISION_VERSION},
    supervisor::{
        LaunchPlan, ProcessGroupMembership, ProcessGroupObservation, ProcessInspector,
        ProcessObservation, ReconciliationRuntime, StdinSource, StdoutSink, Supervisor,
        SupervisorFaultPoint, SystemProcessInspector,
    },
    task::{
        BaseOid, BranchName, ClosePolicy, GitIdentity, PublishMode, PushTarget, TaskId, TaskLimits,
        TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState,
    },
    task_store::{TaskCancelRequest, TaskCloseRequest, TaskPrepareRequest, TaskStore},
    turn::{EnvProfile, TaskTurnRequest, TurnMaterial, TurnSection},
};
use support::GitRepo;
use tempfile::tempdir;
use uuid::Uuid;

const PROJECT_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const WORKTREE_ID: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

fn task_id() -> TaskId {
    TaskId::new(Uuid::from_u128(1))
}

fn base_oid() -> BaseOid {
    "a".repeat(40).parse().unwrap()
}

fn lease(command: CommandSpec, manifest_digest: String) -> LeaseRecord {
    let material = RequestFingerprintMaterial::new(
        JobId::new(Uuid::from_u128(3)),
        ClientId::new(Uuid::from_u128(4)),
        LeaseToken::new(Uuid::from_u128(5)),
        100,
        "mini-1".into(),
        "b".repeat(64),
        "c".repeat(64),
        manifest_digest,
        String::new(),
        30_000,
        "heavy".into(),
        command,
    )
    .unwrap();
    LeaseRecord::new(&material, material.fingerprint(), 100, 30_100).unwrap()
}

fn material(prompt: &str) -> TurnMaterial {
    TurnMaterial::from_prompt(
        task_id(),
        1,
        AgentKind::Codex,
        None,
        None,
        PermissionPolicy::Workspace,
        TurnLimits::new(30_000, None, None).unwrap(),
        base_oid(),
        prompt,
        None,
        Uuid::from_u128(2),
        false,
    )
    .unwrap()
}

struct InlineTurnLauncher {
    store: HostStore,
    fault: Option<SupervisorFaultPoint>,
}

impl SupervisorLauncher for InlineTurnLauncher {
    fn launch(
        &self,
        job_id: JobId,
        guard: mac_worker::host_store::SupervisorGuard,
    ) -> Result<LaunchCandidate, WorkerError> {
        let inspector = SystemProcessInspector;
        let identity = inspector.identity_for_pid(process::id())?;
        match self.fault {
            Some(point) => {
                Supervisor::new_with_fault(&self.store, &inspector, point)
                    .run_with_guard(job_id, guard)?;
            }
            None => Supervisor::new(&self.store, &inspector).run_with_guard(job_id, guard)?,
        }
        Ok(LaunchCandidate::new(identity))
    }
}

struct CappedTurnLauncher {
    store: HostStore,
    stdout_log_cap: Option<u64>,
    stderr_log_cap: Option<u64>,
}

impl SupervisorLauncher for CappedTurnLauncher {
    fn launch(
        &self,
        job_id: JobId,
        guard: mac_worker::host_store::SupervisorGuard,
    ) -> Result<LaunchCandidate, WorkerError> {
        let inspector = SystemProcessInspector;
        let identity = inspector.identity_for_pid(process::id())?;
        Supervisor::new(&self.store, &inspector)
            .with_log_caps(self.stdout_log_cap, self.stderr_log_cap)
            .run_with_guard(job_id, guard)?;
        Ok(LaunchCandidate::new(identity))
    }
}

struct CountingFailingLauncher {
    launches: Arc<AtomicUsize>,
}

impl SupervisorLauncher for CountingFailingLauncher {
    fn launch(
        &self,
        _job_id: JobId,
        _guard: mac_worker::host_store::SupervisorGuard,
    ) -> Result<LaunchCandidate, WorkerError> {
        self.launches.fetch_add(1, Ordering::SeqCst);
        Err(WorkerError::Protocol("test launcher was invoked".into()))
    }
}

struct FastReconciliationRuntime {
    clock: Mutex<Duration>,
}

impl ProcessInspector for FastReconciliationRuntime {
    fn identity_for_pid(&self, pid: u32) -> Result<mac_worker::job::ProcessIdentity, WorkerError> {
        SystemProcessInspector.identity_for_pid(pid)
    }

    fn observe(&self, expected: mac_worker::job::ProcessIdentity) -> ProcessObservation {
        SystemProcessInspector.observe(expected)
    }

    fn observe_group(&self, process_group: u32) -> ProcessGroupObservation {
        SystemProcessInspector.observe_group(process_group)
    }

    fn observe_group_members(&self, leader: u32) -> ProcessGroupMembership {
        SystemProcessInspector.observe_group_members(leader)
    }
}

impl ReconciliationRuntime for FastReconciliationRuntime {
    fn signal_process_group(&self, process_group: u32, signal: i32) -> Result<(), WorkerError> {
        if !matches!(signal, libc::SIGTERM | libc::SIGKILL) {
            return Err(WorkerError::Protocol(
                "test reconciliation signal is not permitted".into(),
            ));
        }
        let process_group = i32::try_from(process_group)
            .map_err(|_| WorkerError::Protocol("test process group is invalid".into()))?;
        if unsafe { libc::kill(-process_group, signal) } != 0 {
            return Err(WorkerError::Io(std::io::Error::last_os_error()));
        }
        Ok(())
    }

    fn monotonic_now(&self) -> Duration {
        *self.clock.lock().unwrap()
    }

    fn sleep(&self, duration: Duration) {
        thread::sleep(duration.min(Duration::from_millis(20)));
        let mut clock = self.clock.lock().unwrap();
        *clock = clock.saturating_add(duration);
    }
}

fn prepared_task_turn(
    script: &str,
) -> (
    tempfile::TempDir,
    HostStore,
    mac_worker::turn::TaskTurnRequest,
    TaskCancelRequest,
) {
    prepared_task_turn_with_options(
        script,
        TaskSource::Local {
            wip: false,
            push_target: None,
        },
        vec![PublishMode::Fetch],
        None,
        None,
    )
}

fn prepared_push_task_turn(
    script: &str,
    origin: &str,
) -> (
    tempfile::TempDir,
    HostStore,
    mac_worker::turn::TaskTurnRequest,
    TaskCancelRequest,
) {
    prepared_task_turn_with_options(
        script,
        TaskSource::Local {
            wip: false,
            push_target: Some(PushTarget::new(origin.to_owned()).unwrap()),
        },
        vec![PublishMode::Fetch, PublishMode::Push],
        Some("release-candidate".parse().unwrap()),
        Some(origin.to_owned()),
    )
}

fn request_with_mismatched_origin(
    request: &mac_worker::turn::TaskTurnRequest,
) -> mac_worker::turn::TaskTurnRequest {
    mac_worker::turn::TaskTurnRequest::new_with_origin(
        request.submit().clone(),
        request.turn().clone(),
        request.prompt(),
        Some("https://other.example.test/repo.git".into()),
    )
    .unwrap()
}

fn prepared_task_turn_with_options(
    script: &str,
    task_source: TaskSource,
    publish: Vec<PublishMode>,
    publish_branch: Option<BranchName>,
    origin_url: Option<String>,
) -> (
    tempfile::TempDir,
    HostStore,
    mac_worker::turn::TaskTurnRequest,
    TaskCancelRequest,
) {
    let temp = tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"base\n");
    source.commit_all("base");
    let base_oid: BaseOid = String::from_utf8(source.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let mirror = store.mirror(PROJECT_ID).unwrap();
    let mirror_path = mirror.path().to_string_lossy().into_owned();
    let base_ref = format!("HEAD:refs/mac-worker/bases/{}", task_id());
    assert!(
        source
            .git(&["push", &mirror_path, &base_ref])
            .status
            .success()
    );

    let prompt = "turn prompt";
    let turn_limits = TurnLimits::new(30_000, None, None).unwrap();
    let turn = TurnMaterial::from_prompt(
        task_id(),
        1,
        AgentKind::Codex,
        None,
        None,
        PermissionPolicy::Workspace,
        turn_limits.clone(),
        base_oid.clone(),
        prompt,
        None,
        Uuid::from_u128(2),
        false,
    )
    .unwrap();
    let launch = TurnLaunch::new(
        "/bin/sh",
        vec!["-c".into(), script.into()],
        PromptDelivery::Stdin,
        Vec::new(),
        false,
    );
    let job_id = JobId::new(Uuid::from_u128(3));
    let client_id = ClientId::new(Uuid::from_u128(4));
    let lease_token = LeaseToken::new(Uuid::from_u128(5));
    let seed_material = RequestFingerprintMaterial::new(
        job_id,
        client_id,
        lease_token,
        100,
        "test-worker".into(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        turn.digest(),
        String::new(),
        30_000,
        "heavy".into(),
        CommandSpec::shell("true".into()).unwrap(),
    )
    .unwrap();
    let seed_lease =
        LeaseRecord::new(&seed_material, seed_material.fingerprint(), 100, 30_100).unwrap();
    let projected = turn.v1_material(&seed_lease, &launch).unwrap();
    LeaseService::new(&store)
        .acquire(
            &LeaseAcquireRequest::new(projected.clone()),
            &AdmissionFacts {
                free_disk_bytes: 100 * 1024 * 1024 * 1024,
                total_disk_bytes: 200 * 1024 * 1024 * 1024,
                memory_pressure: MemoryPressure::Normal,
                swap_used_bytes: Some(0),
            },
            100,
        )
        .unwrap();

    let meta = TaskMeta::new(TaskMetaInput {
        task_id: task_id(),
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        policy: PermissionPolicy::Workspace,
        source: task_source,
        publish,
        publish_branch,
        base_oid,
        limits: TaskLimits::new(turn_limits, 3).unwrap(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: prompt.into(),
        created_at_millis: 100,
    })
    .unwrap();
    let admission = store.admission_lock(job_id).unwrap();
    let transfer = store.transfer_lock_after(&admission, job_id).unwrap();
    TaskStore::new(&store, &mac_worker::process::SystemProcessRunner)
        .prepare(
            &TaskPrepareRequest::new(meta, job_id, "test-worker"),
            &transfer,
        )
        .unwrap();
    drop(transfer);
    drop(admission);

    let request = mac_worker::turn::TaskTurnRequest::new_with_origin(
        mac_worker::job::SubmitRequest::new(projected),
        turn,
        prompt,
        origin_url,
    )
    .unwrap();
    let cancel = TaskCancelRequest::new(PROJECT_ID, task_id(), job_id);
    (temp, store, request, cancel)
}

#[test]
fn turn_material_commits_prompt_through_the_digest_slot_without_v1_fields() {
    let first = material("prompt-a");
    let second = material("prompt-b");
    assert_ne!(first.digest(), second.digest());
    let json = serde_json::to_value(&first).unwrap();
    assert!(json["prompt_sha256"].is_string());
    assert!(json["base_oid"].is_string());
    assert!(json.get("prompt").is_none());
    assert!(json.get("session_ref").is_none());
    assert_eq!(PROTOCOL_VERSION, 6);
    assert_eq!(SUPERVISION_VERSION, 3);
}

#[test]
fn env_profile_requires_owner_only_regular_file_and_does_not_debug_values() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("token.env");
    fs::write(&path, "CLAUDE_CODE_OAUTH_TOKEN=secret-value\n").unwrap();
    assert_eq!(
        EnvProfile::load(&path).unwrap_err().public_code(),
        "ENV_PROFILE_PERMISSIONS"
    );
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let profile = EnvProfile::load(&path).unwrap();
    assert_eq!(profile.names(), &["CLAUDE_CODE_OAUTH_TOKEN"]);
    assert!(!format!("{profile:?}").contains("secret-value"));
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(
        EnvProfile::load(&path).unwrap_err().public_code(),
        "ENV_PROFILE_PERMISSIONS"
    );
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&path, "MAC_WORKER_TURN_DIR=/tmp/escape\n").unwrap();
    assert_eq!(
        EnvProfile::load(&path).unwrap_err().public_code(),
        "ENV_PROFILE_INVALID"
    );

    let hardlinked = directory.path().join("hardlinked.env");
    let alias = directory.path().join("hardlinked-alias.env");
    fs::write(&hardlinked, "CLAUDE_CODE_OAUTH_TOKEN=secret-value\n").unwrap();
    fs::set_permissions(&hardlinked, fs::Permissions::from_mode(0o600)).unwrap();
    fs::hard_link(&hardlinked, &alias).unwrap();
    assert_eq!(
        EnvProfile::load(&hardlinked).unwrap_err().public_code(),
        "ENV_PROFILE_PERMISSIONS"
    );
}

#[test]
fn keychain_profile_values_are_consumed_and_never_exported() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("agents.env");
    fs::write(
        &path,
        "CURSOR_API_KEY=cursor-key\nMAC_WORKER_KEYCHAIN_PASSWORD=profile-password\nMAC_WORKER_KEYCHAIN_PATH=/tmp/custom.keychain-db\n",
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();

    let profile = EnvProfile::load(&path).unwrap();
    assert_eq!(profile.names(), &["CURSOR_API_KEY"]);
    let debug = format!("{profile:?}");
    assert!(!debug.contains("profile-password"));
    assert!(!debug.contains("/tmp/custom.keychain-db"));

    let turn = material("prompt");
    let section = TurnSection::new(
        turn.clone(),
        "b".repeat(64),
        GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
    )
    .unwrap();
    let command = CommandSpec::shell("exec 'codex' '-'".into()).unwrap();
    let plan = LaunchPlan::turn(
        &command,
        &lease(command.clone(), turn.digest()),
        &section,
        std::path::Path::new("/Users/worker"),
        &profile,
        section.git_identity(),
    )
    .unwrap();
    assert!(
        plan.env()
            .iter()
            .any(|(name, value)| { name == "CURSOR_API_KEY" && value == "cursor-key" })
    );
    assert!(
        plan.env()
            .iter()
            .all(|(name, _)| name != "MAC_WORKER_KEYCHAIN_PASSWORD")
    );
    assert!(
        plan.env()
            .iter()
            .all(|(name, _)| name != "MAC_WORKER_KEYCHAIN_PATH")
    );
}

#[test]
fn keychain_unlock_failure_is_a_durable_turn_error_code() {
    let status = JobStatus::accepted(1)
        .unwrap()
        .into_infrastructure_terminal(JobState::Lost, 2, 0, 0, "KEYCHAIN_UNLOCK_FAILED".into())
        .unwrap();
    assert_eq!(status.state(), JobState::Lost);
    assert_eq!(status.error_code(), Some("KEYCHAIN_UNLOCK_FAILED"));
}

#[test]
fn authentication_failure_in_a_turn_records_an_incident_and_names_the_outcome() {
    let script = concat!(
        "printf '%s\\n' ",
        "'ERROR codex_login::auth::manager: Failed to refresh token: ",
        "Your access token could not be refreshed because your refresh token was already used. ",
        "Please log out and sign in again.'; ",
        "exit 1",
    );
    let (temp, store, request, _cancel) = prepared_task_turn(script);
    let launcher = InlineTurnLauncher {
        store: store.clone(),
        fault: None,
    };
    let response = JobService::new(&store, &launcher)
        .submit_turn(request)
        .unwrap();
    assert_eq!(
        response.task().last_outcome(),
        Some(&TaskOutcome::failed("agent authentication failed"))
    );
    let incidents =
        fs::read_to_string(temp.path().join("host").join("auth-incidents.json")).unwrap();
    assert!(incidents.contains(r#""agent":"codex""#), "{incidents}");
    assert!(
        incidents.contains(r#""reason":"auth failed in a turn""#),
        "{incidents}"
    );
    assert!(!incidents.contains("refresh token"), "{incidents}");
    assert!(!incidents.contains("Please log out"), "{incidents}");
}

#[test]
fn a_successful_turn_still_publishes_when_the_incident_store_is_corrupt() {
    let script = concat!(
        "printf '%s\\n' ",
        "'{\"type\":\"thread.started\",\"thread_id\":\"session-ok\"}' ",
        "'{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"",
        "{\\\"status\\\":\\\"done\\\",\\\"summary\\\":\\\"ok\\\",\\\"questions\\\":[],\\\"files_changed\\\":[]}",
        "\"}}'",
    );
    let (temp, store, request, cancel) = prepared_task_turn(script);
    let incidents = temp.path().join("host").join("auth-incidents.json");
    fs::write(&incidents, b"{not-canonical").unwrap();
    fs::set_permissions(&incidents, fs::Permissions::from_mode(0o600)).unwrap();
    let launcher = InlineTurnLauncher {
        store: store.clone(),
        fault: None,
    };
    let response = JobService::new(&store, &launcher)
        .submit_turn(request)
        .unwrap();
    assert_eq!(response.task().last_outcome(), Some(&TaskOutcome::Done));
    assert_eq!(fs::read(&incidents).unwrap(), b"{not-canonical");
    let job_path = store
        .job(PROJECT_ID, WORKTREE_ID, cancel.turn_id())
        .unwrap();
    let supervisor = fs::read_to_string(job_path.join("supervisor.log")).unwrap();
    assert!(
        supervisor.contains("auth success not recorded: PROTOCOL"),
        "{supervisor}"
    );
}

#[test]
fn a_non_auth_agent_exit_does_not_record_an_incident() {
    let (temp, store, request, _cancel) = prepared_task_turn("printf 'boom\\n'; exit 1");
    let launcher = InlineTurnLauncher {
        store: store.clone(),
        fault: None,
    };
    let response = JobService::new(&store, &launcher)
        .submit_turn(request)
        .unwrap();
    assert_eq!(
        response.task().last_outcome(),
        Some(&TaskOutcome::failed("agent exited 1"))
    );
    assert!(
        !temp
            .path()
            .join("host")
            .join("auth-incidents.json")
            .exists(),
        "non-auth failures must not write auth-incidents.json"
    );
}

#[test]
fn turn_material_round_trips_canonically() {
    let original = material("prompt");
    let bytes = serde_json::to_vec(&original).unwrap();
    let decoded: TurnMaterial = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded, original);
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
}

#[test]
fn task_turn_publication_origin_survives_the_canonical_turn_payload() {
    let (_temp, _store, request, _cancel) = prepared_task_turn("exit 0");
    let request = TaskTurnRequest::new_with_origin(
        request.submit().clone(),
        request.turn().clone(),
        request.prompt(),
        Some("https://example.test/repo.git".into()),
    )
    .unwrap();
    let bytes = serde_json::to_vec(&request).unwrap();
    let parsed: TaskTurnRequest = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        parsed.origin_url(),
        Some("https://example.test/repo.git"),
        "the worker push target must survive the durable request boundary"
    );
    assert!(
        TaskTurnRequest::new_with_origin(
            request.submit().clone(),
            request.turn().clone(),
            request.prompt(),
            Some("https://USER:secret@EXAMPLE.test/repo.git?token=secret".into()),
        )
        .is_err()
    );
}

#[test]
fn local_push_turn_rejects_a_target_other_than_the_pinned_origin() {
    let (_temp, store, request, _cancel) =
        prepared_push_task_turn("exit 0", "https://example.test/repo.git");
    let mismatched = TaskTurnRequest::new_with_origin(
        request.submit().clone(),
        request.turn().clone(),
        request.prompt(),
        Some("https://other.example.test/repo.git".into()),
    )
    .unwrap();

    let error = JobService::new(
        &store,
        &InlineTurnLauncher {
            store: store.clone(),
            fault: None,
        },
    )
    .submit_turn(mismatched)
    .unwrap_err();
    assert_eq!(error.public_code(), "REQUEST_CONFLICT");
}

#[test]
fn accepted_origin_mismatch_replay_returns_conflict_before_launch() {
    let (_temp, store, request, _cancel) =
        prepared_push_task_turn("exit 0", "ssh://127.0.0.1:1/repo.git");
    let setup_launches = Arc::new(AtomicUsize::new(0));
    assert!(
        JobService::new(
            &store,
            &CountingFailingLauncher {
                launches: setup_launches.clone(),
            },
        )
        .submit_turn(request.clone())
        .is_err()
    );
    assert_eq!(setup_launches.load(Ordering::SeqCst), 1);

    let job_path = store
        .job(PROJECT_ID, WORKTREE_ID, JobId::new(Uuid::from_u128(3)))
        .unwrap();
    let status_before = fs::read(job_path.join("status.json")).unwrap();
    assert_eq!(
        serde_json::from_slice::<JobStatus>(&status_before)
            .unwrap()
            .state(),
        JobState::Accepted
    );
    let mismatched = request_with_mismatched_origin(&request);
    assert_eq!(
        mismatched.submit().request_fingerprint(),
        request.submit().request_fingerprint()
    );
    let replay_launches = Arc::new(AtomicUsize::new(0));
    let error = JobService::new(
        &store,
        &CountingFailingLauncher {
            launches: replay_launches.clone(),
        },
    )
    .submit_turn(mismatched)
    .unwrap_err();

    assert_eq!(error.public_code(), "REQUEST_CONFLICT");
    assert_eq!(replay_launches.load(Ordering::SeqCst), 0);
    assert_eq!(
        fs::read(job_path.join("status.json")).unwrap(),
        status_before
    );
}

#[test]
fn running_origin_mismatch_replay_returns_conflict_before_launch() {
    let script = "sleep 3; printf '%s\\n' '{\"type\":\"thread.started\",\"thread_id\":\"session-origin-running\"}' '{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"{\\\"status\\\":\\\"done\\\",\\\"summary\\\":\\\"finished\\\",\\\"questions\\\":[],\\\"files_changed\\\":[]}\"}}'";
    let (_temp, store, request, _cancel) =
        prepared_push_task_turn(script, "ssh://127.0.0.1:1/repo.git");
    let submit_store = store.clone();
    let submit_request = request.clone();
    let submit = thread::spawn(move || {
        JobService::new(
            &submit_store,
            &InlineTurnLauncher {
                store: submit_store.clone(),
                fault: None,
            },
        )
        .submit_turn(submit_request)
    });
    let job_path = store
        .job(PROJECT_ID, WORKTREE_ID, JobId::new(Uuid::from_u128(3)))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let running = fs::read(job_path.join("status.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<JobStatus>(&bytes).ok())
            .is_some_and(|status| status.state() == JobState::Running);
        if running {
            break;
        }
        if Instant::now() >= deadline {
            let _ = submit.join();
            panic!("inline supervisor did not publish Running before the deadline");
        }
        thread::sleep(Duration::from_millis(10));
    }

    let status_before = fs::read(job_path.join("status.json")).unwrap();
    let replay_launches = Arc::new(AtomicUsize::new(0));
    let error = JobService::new(
        &store,
        &CountingFailingLauncher {
            launches: replay_launches.clone(),
        },
    )
    .submit_turn(request_with_mismatched_origin(&request))
    .unwrap_err();

    assert_eq!(error.public_code(), "REQUEST_CONFLICT");
    assert_eq!(replay_launches.load(Ordering::SeqCst), 0);
    assert_eq!(
        fs::read(job_path.join("status.json")).unwrap(),
        status_before
    );
    let _ = submit.join().expect("running submit thread panicked");
}

#[test]
fn terminal_origin_mismatch_replay_returns_conflict_before_recovery() {
    let script = r#"printf '%s\n' '{"type":"thread.started","thread_id":"session-origin-terminal"}' '{"type":"item.completed","item":{"type":"agent_message","text":"{\"status\":\"done\",\"summary\":\"finished\",\"questions\":[],\"files_changed\":[]}"}}'"#;
    let (_temp, store, request, _cancel) =
        prepared_push_task_turn(script, "ssh://127.0.0.1:1/repo.git");
    assert!(
        JobService::new(
            &store,
            &InlineTurnLauncher {
                store: store.clone(),
                fault: None,
            },
        )
        .submit_turn(request.clone())
        .is_err()
    );

    let job_path = store
        .job(PROJECT_ID, WORKTREE_ID, JobId::new(Uuid::from_u128(3)))
        .unwrap();
    let status_before = fs::read(job_path.join("status.json")).unwrap();
    assert!(
        serde_json::from_slice::<JobStatus>(&status_before)
            .unwrap()
            .state()
            .is_terminal()
    );
    let replay_launches = Arc::new(AtomicUsize::new(0));
    let error = JobService::new(
        &store,
        &CountingFailingLauncher {
            launches: replay_launches.clone(),
        },
    )
    .submit_turn(request_with_mismatched_origin(&request))
    .unwrap_err();

    assert_eq!(error.public_code(), "REQUEST_CONFLICT");
    assert_eq!(replay_launches.load(Ordering::SeqCst), 0);
    assert_eq!(
        fs::read(job_path.join("status.json")).unwrap(),
        status_before
    );
}

#[test]
fn failed_origin_push_keeps_task_open_and_retains_the_mirror_branch() {
    let origin = "ssh://127.0.0.1:1/repo.git";
    let script = r#"printf changed > agent.txt; printf '%s\n' '{"type":"thread.started","thread_id":"session-push"}' '{"type":"item.completed","item":{"type":"agent_message","text":"{\"status\":\"done\",\"summary\":\"push attempt\",\"questions\":[],\"files_changed\":[]}"}}'"#;
    let (_temp, store, request, _cancel) = prepared_push_task_turn(script, origin);
    let launcher = InlineTurnLauncher {
        store: store.clone(),
        fault: None,
    };

    let error = JobService::new(&store, &launcher)
        .submit_turn(request)
        .unwrap_err();
    assert_eq!(error.public_code(), "PUBLISH_FAILED");
    let status = store.task_status(PROJECT_ID, task_id()).unwrap();
    assert_eq!(status.state(), TaskState::Open);
    assert!(matches!(
        status.last_outcome(),
        Some(TaskOutcome::Failed { reason }) if reason == "PUBLISH_FAILED"
    ));
    assert!(
        store
            .task_workspace_if_present(PROJECT_ID, task_id())
            .unwrap()
            .is_some()
    );
    let mirror = store.mirror(PROJECT_ID).unwrap();
    let branch = format!("refs/heads/task/{}", task_id());
    let branch_exists = Command::new("/usr/bin/git")
        .args([
            "--git-dir",
            &mirror.path().to_string_lossy(),
            "show-ref",
            "--verify",
            &branch,
        ])
        .output()
        .unwrap()
        .status
        .success();
    assert!(
        branch_exists,
        "publisher must retain the mirror result branch"
    );
}

#[test]
fn launch_plans_keep_batch_golden_and_turns_use_the_account_environment() {
    let batch_command = CommandSpec::argv(vec!["/usr/bin/true".into(), "literal".into()]).unwrap();
    let batch_lease = lease(batch_command.clone(), "d".repeat(64));
    let batch = LaunchPlan::batch(
        &batch_command,
        &batch_lease,
        std::path::Path::new("/job/home"),
        std::path::Path::new("/job/tmp"),
    )
    .unwrap();
    assert_eq!(batch.program(), "/usr/bin/true");
    assert_eq!(
        batch.args(),
        &["/usr/bin/true".to_string(), "literal".to_string()]
    );
    assert_eq!(
        batch.env(),
        &[
            ("LC_ALL".into(), "C".into()),
            ("LANG".into(), "C".into()),
            (
                "PATH".into(),
                "/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin".into(),
            ),
            ("HOME".into(), "/job/home".into()),
            ("TMPDIR".into(), "/job/tmp".into()),
            (
                "MAC_WORKER_JOB_ID".into(),
                "00000000000000000000000000000003".into()
            ),
            (
                "MAC_WORKER_CLIENT_ID".into(),
                "00000000000000000000000000000004".into(),
            ),
            ("MAC_WORKER_PROJECT_ID".into(), "b".repeat(64).into()),
            ("MAC_WORKER_WORKTREE_ID".into(), "c".repeat(64).into()),
        ]
    );
    assert!(batch.cwd().as_os_str().is_empty());
    assert_eq!(batch.stdin(), &StdinSource::Null);
    assert_eq!(batch.stdout(), StdoutSink::Direct);

    let turn = material("prompt");
    let section = TurnSection::new(
        turn.clone(),
        "b".repeat(64),
        GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
    )
    .unwrap();
    let turn_command = CommandSpec::shell("exec 'codex' '-'".into()).unwrap();
    let turn_lease = lease(turn_command.clone(), turn.digest());
    let profile_dir = tempdir().unwrap();
    let profile_path = profile_dir.path().join("agents.env");
    fs::write(&profile_path, "CLAUDE_CODE_OAUTH_TOKEN=secret\n").unwrap();
    fs::set_permissions(&profile_path, fs::Permissions::from_mode(0o600)).unwrap();
    let profile = EnvProfile::load(&profile_path).unwrap();
    let plan = LaunchPlan::turn(
        &turn_command,
        &turn_lease,
        &section,
        std::path::Path::new("/Users/worker"),
        &profile,
        section.git_identity(),
    )
    .unwrap();
    assert_eq!(plan.program(), "/bin/zsh");
    assert_eq!(plan.args()[1], "-lc");
    assert_eq!(
        plan.cwd(),
        std::path::Path::new(
            "tasks/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/00000000000000000000000000000001/workspace"
        )
    );
    assert_eq!(plan.stdin(), &StdinSource::File("prompt.md".into()));
    assert_eq!(plan.stdout(), StdoutSink::Pipe);
    assert!(
        plan.env()
            .iter()
            .any(|(name, value)| { name == "HOME" && value == "/Users/worker" })
    );
    for name in [
        "USER",
        "LOGNAME",
        "SHELL",
        "TMPDIR",
        "MAC_WORKER_TURN_DIR",
        "MAC_WORKER_TASK_ID",
        "MAC_WORKER_TURN",
        "GIT_AUTHOR_NAME",
        "GIT_COMMITTER_EMAIL",
        "CLAUDE_CODE_OAUTH_TOKEN",
    ] {
        assert!(plan.env().iter().any(|(key, _)| key == name), "{name}");
    }
    for name in ["USER", "LOGNAME", "SHELL"] {
        if let Some(expected) = std::env::var_os(name) {
            let actual = plan
                .env()
                .iter()
                .find_map(|(key, value)| (key == name).then_some(value.clone()));
            assert_eq!(
                actual,
                Some(expected),
                "{name} must preserve the account value"
            );
        }
    }
    assert!(plan.env().iter().all(|(name, _)| name != "PATH"));
    assert!(!plan.args().iter().any(|arg| arg.contains("/Users/worker")));
    assert!(!format!("{plan:?}").contains("secret"));
}

#[test]
fn turn_material_v1_projection_uses_the_unchanged_digest_slot() {
    let turn = material("prompt");
    let launch = TurnLaunch::new(
        "codex",
        vec!["exec".into(), "-".into()],
        PromptDelivery::Stdin,
        Vec::new(),
        false,
    );
    let lease = lease(
        CommandSpec::shell("exec 'codex' '-'".into()).unwrap(),
        turn.digest(),
    );
    let projected = turn.v1_material(&lease, &launch).unwrap();
    assert_eq!(projected.manifest_digest(), turn.digest());
    assert_eq!(projected.relative_working_dir(), "");
    assert_eq!(projected.resource_class(), "heavy");
    assert!(
        serde_json::to_value(&projected)
            .unwrap()
            .get("turn")
            .is_none()
    );
}

#[test]
fn submit_turn_runs_and_publishes_through_the_durable_supervisor() {
    let temp = tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"base\n");
    source.commit_all("base");
    let base_oid: BaseOid = String::from_utf8(source.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let mirror = store.mirror(PROJECT_ID).unwrap();
    let mirror_path = mirror.path().to_string_lossy().into_owned();
    let base_ref = format!("HEAD:refs/mac-worker/bases/{}", task_id());
    assert!(
        source
            .git(&["push", &mirror_path, &base_ref])
            .status
            .success()
    );

    let prompt = "turn prompt";
    let turn_limits = TurnLimits::new(30_000, None, None).unwrap();
    let turn = TurnMaterial::from_prompt(
        task_id(),
        1,
        AgentKind::Codex,
        None,
        None,
        PermissionPolicy::Workspace,
        turn_limits.clone(),
        base_oid.clone(),
        prompt,
        None,
        Uuid::from_u128(2),
        false,
    )
    .unwrap();
    let launch = TurnLaunch::new(
        "/bin/sh",
        vec![
            "-c".into(),
            "printf changed > agent.txt; printf '%s\\n' '{\"type\":\"thread.started\",\"thread_id\":\"session-1\"}' '{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"{\\\"status\\\":\\\"done\\\",\\\"summary\\\":\\\"finished at /Users/worker/.git/index.lock\\\",\\\"questions\\\":[\\\"why is /Users/worker/.git/index.lock locked?\\\"],\\\"files_changed\\\":[]}\"}}'".into(),
        ],
        PromptDelivery::Stdin,
        Vec::new(),
        false,
    );
    let seed_material = RequestFingerprintMaterial::new(
        JobId::new(Uuid::from_u128(3)),
        ClientId::new(Uuid::from_u128(4)),
        LeaseToken::new(Uuid::from_u128(5)),
        100,
        "test-worker".into(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        turn.digest(),
        String::new(),
        30_000,
        "heavy".into(),
        CommandSpec::shell("true".into()).unwrap(),
    )
    .unwrap();
    let seed_lease =
        LeaseRecord::new(&seed_material, seed_material.fingerprint(), 100, 30_100).unwrap();
    let projected = turn.v1_material(&seed_lease, &launch).unwrap();
    LeaseService::new(&store)
        .acquire(
            &LeaseAcquireRequest::new(projected.clone()),
            &AdmissionFacts {
                free_disk_bytes: 100 * 1024 * 1024 * 1024,
                total_disk_bytes: 200 * 1024 * 1024 * 1024,
                memory_pressure: MemoryPressure::Normal,
                swap_used_bytes: Some(0),
            },
            100,
        )
        .unwrap();

    let meta = TaskMeta::new(TaskMetaInput {
        task_id: task_id(),
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
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
        base_oid,
        limits: TaskLimits::new(turn_limits, 3).unwrap(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: prompt.into(),
        created_at_millis: 100,
    })
    .unwrap();
    let admission = store
        .admission_lock(JobId::new(Uuid::from_u128(3)))
        .unwrap();
    let transfer = store
        .transfer_lock_after(&admission, JobId::new(Uuid::from_u128(3)))
        .unwrap();
    TaskStore::new(&store, &mac_worker::process::SystemProcessRunner)
        .prepare(
            &TaskPrepareRequest::new(meta, JobId::new(Uuid::from_u128(3)), "test-worker"),
            &transfer,
        )
        .unwrap();
    drop(transfer);
    drop(admission);

    let request =
        TaskTurnRequest::new(mac_worker::job::SubmitRequest::new(projected), turn, prompt);
    let faulting_launcher = InlineTurnLauncher {
        store: store.clone(),
        fault: Some(SupervisorFaultPoint::AfterTerminalStatus),
    };
    assert!(
        JobService::new(&store, &faulting_launcher)
            .submit_turn(request.clone())
            .is_err()
    );
    let job_path = store
        .job(PROJECT_ID, WORKTREE_ID, JobId::new(Uuid::from_u128(3)))
        .unwrap();
    let job_status: mac_worker::job::JobStatus =
        serde_json::from_slice(&fs::read(job_path.join("status.json")).unwrap()).unwrap();
    assert!(job_status.state().is_terminal());
    assert!(job_path.join("execution.json").is_file());

    let launcher = InlineTurnLauncher {
        store: store.clone(),
        fault: None,
    };
    let response = JobService::new(&store, &launcher)
        .submit_turn(request.clone())
        .unwrap_or_else(|error| {
            let path = store
                .job(PROJECT_ID, WORKTREE_ID, JobId::new(Uuid::from_u128(3)))
                .unwrap();
            let names = fs::read_dir(&path)
                .map(|entries| {
                    entries
                        .filter_map(Result::ok)
                        .map(|entry| entry.file_name().to_string_lossy().into_owned())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            panic!("submit_turn failed: {error:?}; job entries: {names:?}");
        });
    assert_eq!(response.task().state(), TaskState::Open);
    assert_eq!(response.task().last_outcome(), Some(&TaskOutcome::Done));
    assert_eq!(response.task().summary(), Some("finished at [path]"));
    assert_eq!(
        response.task().questions(),
        &[Question::open("why is [path] locked?")]
    );
    assert!(response.task().session_present());
    assert_eq!(response.task().turns()[0].agent_committed(), Some(false));
    assert_eq!(response.task().files_changed(), &["agent.txt"]);
    assert!(response.task().diff_stat().is_some());
    assert!(
        store
            .task_workspace_if_present(PROJECT_ID, task_id())
            .unwrap()
            .is_some()
    );
    let status_json = fs::read(
        store
            .task_dir(PROJECT_ID, task_id())
            .unwrap()
            .join("status.json"),
    )
    .unwrap();
    let status_json = String::from_utf8(status_json).unwrap();
    assert!(!status_json.contains("/Users/worker/.git/index.lock"));
    assert!(status_json.contains("finished at [path]"));
    assert!(status_json.contains("why is [path] locked?"));

    let retry = JobService::new(&store, &launcher)
        .submit_turn(request)
        .unwrap();
    assert_eq!(retry.task().state(), TaskState::Open);
    assert_eq!(retry.task().last_outcome(), Some(&TaskOutcome::Done));
}

#[test]
fn resumed_codex_turn_stays_alive_and_publishes_after_needs_input() {
    let first_script = r#"printf '%s\n' '{"type":"thread.started","thread_id":"session-resume"}' '{"type":"item.completed","item":{"type":"agent_message","text":"{\"status\":\"needs_input\",\"summary\":\"need details\",\"questions\":[\"Which detail?\"],\"files_changed\":[]}"}}'"#;
    let (temp, store, first_request, _) = prepared_task_turn(first_script);
    let launcher = InlineTurnLauncher {
        store: store.clone(),
        fault: None,
    };
    let first = JobService::new(&store, &launcher)
        .submit_turn(first_request.clone())
        .expect("first turn should publish needs_input");
    assert_eq!(first.task().state(), TaskState::Open);
    assert_eq!(first.task().last_outcome(), Some(&TaskOutcome::NeedsInput));
    assert!(first.task().session_present());
    let base_oid = first
        .task()
        .head_oid()
        .cloned()
        .expect("first turn should retain its workspace head");

    let fake_bin = temp.path().join("fake-bin");
    fs::create_dir(&fake_bin).unwrap();
    let fake_codex = fake_bin.join("codex");
    fs::write(
        &fake_codex,
        r#"#!/bin/sh
printf '%s\n' '{"type":"thread.started","thread_id":"session-resume"}'
/bin/sleep 0.1
printf '%s\n' '{"type":"item.completed","item":{"type":"agent_message","text":"{\"status\":\"done\",\"summary\":\"resumed\",\"questions\":[],\"files_changed\":[]}"}}'
"#,
    )
    .unwrap();
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).unwrap();

    let resume_prompt = "resume prompt";
    let resume_job_id = JobId::new(Uuid::from_u128(6));
    let resume_turn = TurnMaterial::from_prompt(
        task_id(),
        2,
        AgentKind::Codex,
        None,
        None,
        PermissionPolicy::Workspace,
        first_request.turn().limits().clone(),
        base_oid.clone(),
        resume_prompt,
        None,
        resume_job_id.as_uuid(),
        true,
    )
    .unwrap();
    let params = TurnParams {
        kind: resume_turn.agent(),
        model: resume_turn.model().map(str::to_owned),
        effort: None,
        policy: resume_turn.policy(),
        limits: resume_turn.limits().clone(),
        session_seed: resume_turn.session_seed(),
    };
    let launch = adapter_for(AgentKind::Codex)
        .resume_turn(&params, "session-resume")
        .unwrap();
    let launch = TurnLaunch::new(
        fake_codex.to_string_lossy(),
        launch.args().to_vec(),
        launch.prompt_delivery(),
        launch.env_names().to_vec(),
        launch.permission_fallback(),
    );
    let seed_material = RequestFingerprintMaterial::new(
        resume_job_id,
        ClientId::new(Uuid::from_u128(4)),
        LeaseToken::new(Uuid::from_u128(7)),
        200,
        "test-worker".into(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        resume_turn.digest(),
        String::new(),
        30_000,
        "heavy".into(),
        CommandSpec::shell("true".into()).unwrap(),
    )
    .unwrap();
    let seed_lease =
        LeaseRecord::new(&seed_material, seed_material.fingerprint(), 200, 30_200).unwrap();
    let projected = resume_turn.v1_material(&seed_lease, &launch).unwrap();
    let admission_facts = AdmissionFacts {
        free_disk_bytes: 100 * 1024 * 1024 * 1024,
        total_disk_bytes: 200 * 1024 * 1024 * 1024,
        memory_pressure: MemoryPressure::Normal,
        swap_used_bytes: Some(0),
    };
    LeaseService::new(&store)
        .acquire(
            &LeaseAcquireRequest::new(projected.clone()),
            &admission_facts,
            200,
        )
        .unwrap();

    let (meta, prepared) = TaskStore::new(&store, &mac_worker::process::SystemProcessRunner)
        .prepare_resume(
            PROJECT_ID,
            task_id(),
            resume_job_id,
            2,
            "test-worker",
            &base_oid,
        )
        .unwrap();
    assert_eq!(meta.agent(), AgentKind::Codex);
    assert_eq!(prepared.state(), TaskState::Active);

    let resume_request = TaskTurnRequest::new(
        mac_worker::job::SubmitRequest::new(projected),
        resume_turn,
        resume_prompt,
    );
    let resumed = JobService::new(&store, &launcher)
        .submit_turn(resume_request)
        .unwrap_or_else(|error| panic!("resumed turn should publish done: {error}"));
    assert_eq!(resumed.task().state(), TaskState::Open);
    assert_eq!(resumed.task().last_outcome(), Some(&TaskOutcome::Done));
    assert!(resumed.task().session_present());
    assert_eq!(resumed.task().turns().len(), 2);
}

#[test]
fn successful_codex_turn_without_a_bound_session_fails_publication() {
    let temp = tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"base\n");
    source.commit_all("base");
    let base_oid: BaseOid = String::from_utf8(source.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let mirror = store.mirror(PROJECT_ID).unwrap();
    let mirror_path = mirror.path().to_string_lossy().into_owned();
    let base_ref = format!("HEAD:refs/mac-worker/bases/{}", task_id());
    assert!(
        source
            .git(&["push", &mirror_path, &base_ref])
            .status
            .success()
    );

    let prompt = "turn prompt";
    let turn_limits = TurnLimits::new(30_000, None, None).unwrap();
    let turn = TurnMaterial::from_prompt(
        task_id(),
        1,
        AgentKind::Codex,
        None,
        None,
        PermissionPolicy::Workspace,
        turn_limits.clone(),
        base_oid.clone(),
        prompt,
        None,
        Uuid::from_u128(2),
        false,
    )
    .unwrap();
    let launch = TurnLaunch::new(
        "/bin/sh",
        vec![
            "-c".into(),
            "printf changed > agent.txt; printf '%s\\n' '{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"{\\\"status\\\":\\\"done\\\",\\\"summary\\\":\\\"ok\\\",\\\"questions\\\":[],\\\"files_changed\\\":[]}\"}}'".into(),
        ],
        PromptDelivery::Stdin,
        Vec::new(),
        false,
    );
    let seed_material = RequestFingerprintMaterial::new(
        JobId::new(Uuid::from_u128(3)),
        ClientId::new(Uuid::from_u128(4)),
        LeaseToken::new(Uuid::from_u128(5)),
        100,
        "test-worker".into(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        turn.digest(),
        String::new(),
        30_000,
        "heavy".into(),
        CommandSpec::shell("true".into()).unwrap(),
    )
    .unwrap();
    let seed_lease =
        LeaseRecord::new(&seed_material, seed_material.fingerprint(), 100, 30_100).unwrap();
    let projected = turn.v1_material(&seed_lease, &launch).unwrap();
    LeaseService::new(&store)
        .acquire(
            &LeaseAcquireRequest::new(projected.clone()),
            &AdmissionFacts {
                free_disk_bytes: 100 * 1024 * 1024 * 1024,
                total_disk_bytes: 200 * 1024 * 1024 * 1024,
                memory_pressure: MemoryPressure::Normal,
                swap_used_bytes: Some(0),
            },
            100,
        )
        .unwrap();

    let meta = TaskMeta::new(TaskMetaInput {
        task_id: task_id(),
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
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
        base_oid,
        limits: TaskLimits::new(turn_limits, 3).unwrap(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: prompt.into(),
        created_at_millis: 100,
    })
    .unwrap();
    let admission = store
        .admission_lock(JobId::new(Uuid::from_u128(3)))
        .unwrap();
    let transfer = store
        .transfer_lock_after(&admission, JobId::new(Uuid::from_u128(3)))
        .unwrap();
    TaskStore::new(&store, &mac_worker::process::SystemProcessRunner)
        .prepare(
            &TaskPrepareRequest::new(meta, JobId::new(Uuid::from_u128(3)), "test-worker"),
            &transfer,
        )
        .unwrap();
    drop(transfer);
    drop(admission);

    let request =
        TaskTurnRequest::new(mac_worker::job::SubmitRequest::new(projected), turn, prompt);
    let launcher = InlineTurnLauncher {
        store: store.clone(),
        fault: None,
    };

    let error = JobService::new(&store, &launcher)
        .submit_turn(request)
        .unwrap_err();
    assert_eq!(error.public_code(), "PUBLISH_FAILED");
    let status = store.task_status(PROJECT_ID, task_id()).unwrap();
    assert_eq!(status.state(), TaskState::Open);
    assert!(!status.session_present());
    assert!(matches!(
        status.last_outcome(),
        Some(TaskOutcome::Failed { .. })
    ));
}

#[test]
// Catches the publisher rejecting the agent-written last-message file:
// Codex writes `-o last.md` with the account umask (0644), and a reader that
// demands an owner-only file failed every successful live turn with
// PUBLISH_FAILED while failed turns (no last.md) published fine.
fn publication_tolerates_an_agent_written_last_message_with_default_mode() {
    let temp = tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"base\n");
    source.commit_all("base");
    let base_oid: BaseOid = String::from_utf8(source.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let mirror = store.mirror(PROJECT_ID).unwrap();
    let mirror_path = mirror.path().to_string_lossy().into_owned();
    let base_ref = format!("HEAD:refs/mac-worker/bases/{}", task_id());
    assert!(
        source
            .git(&["push", &mirror_path, &base_ref])
            .status
            .success()
    );

    let prompt = "turn prompt";
    let turn_limits = TurnLimits::new(30_000, None, None).unwrap();
    let turn = TurnMaterial::from_prompt(
        task_id(),
        1,
        AgentKind::Codex,
        None,
        None,
        PermissionPolicy::Workspace,
        turn_limits.clone(),
        base_oid.clone(),
        prompt,
        None,
        Uuid::from_u128(2),
        false,
    )
    .unwrap();
    let launch = TurnLaunch::new(
        "/bin/sh",
        vec![
            "-c".into(),
            "umask 022; printf '%s' '{\"status\":\"done\",\"summary\":\"ok\",\"questions\":[],\"files_changed\":[]}' > \"$MAC_WORKER_TURN_DIR/last.md\"; printf changed > agent.txt; printf '%s\\n' '{\"type\":\"thread.started\",\"thread_id\":\"session-1\"}' '{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"{\\\"status\\\":\\\"done\\\",\\\"summary\\\":\\\"ok\\\",\\\"questions\\\":[],\\\"files_changed\\\":[]}\"}}'".into(),
        ],
        PromptDelivery::Stdin,
        Vec::new(),
        false,
    );
    let seed_material = RequestFingerprintMaterial::new(
        JobId::new(Uuid::from_u128(3)),
        ClientId::new(Uuid::from_u128(4)),
        LeaseToken::new(Uuid::from_u128(5)),
        100,
        "test-worker".into(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        turn.digest(),
        String::new(),
        30_000,
        "heavy".into(),
        CommandSpec::shell("true".into()).unwrap(),
    )
    .unwrap();
    let seed_lease =
        LeaseRecord::new(&seed_material, seed_material.fingerprint(), 100, 30_100).unwrap();
    let projected = turn.v1_material(&seed_lease, &launch).unwrap();
    LeaseService::new(&store)
        .acquire(
            &LeaseAcquireRequest::new(projected.clone()),
            &AdmissionFacts {
                free_disk_bytes: 100 * 1024 * 1024 * 1024,
                total_disk_bytes: 200 * 1024 * 1024 * 1024,
                memory_pressure: MemoryPressure::Normal,
                swap_used_bytes: Some(0),
            },
            100,
        )
        .unwrap();

    let meta = TaskMeta::new(TaskMetaInput {
        task_id: task_id(),
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
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
        base_oid,
        limits: TaskLimits::new(turn_limits, 3).unwrap(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: prompt.into(),
        created_at_millis: 100,
    })
    .unwrap();
    let admission = store
        .admission_lock(JobId::new(Uuid::from_u128(3)))
        .unwrap();
    let transfer = store
        .transfer_lock_after(&admission, JobId::new(Uuid::from_u128(3)))
        .unwrap();
    TaskStore::new(&store, &mac_worker::process::SystemProcessRunner)
        .prepare(
            &TaskPrepareRequest::new(meta, JobId::new(Uuid::from_u128(3)), "test-worker"),
            &transfer,
        )
        .unwrap();
    drop(transfer);
    drop(admission);

    let request =
        TaskTurnRequest::new(mac_worker::job::SubmitRequest::new(projected), turn, prompt);
    let faulting_launcher = InlineTurnLauncher {
        store: store.clone(),
        fault: Some(SupervisorFaultPoint::AfterTerminalStatus),
    };
    assert!(
        JobService::new(&store, &faulting_launcher)
            .submit_turn(request.clone())
            .is_err()
    );
    let job_path = store
        .job(PROJECT_ID, WORKTREE_ID, JobId::new(Uuid::from_u128(3)))
        .unwrap();
    let job_status: mac_worker::job::JobStatus =
        serde_json::from_slice(&fs::read(job_path.join("status.json")).unwrap()).unwrap();
    assert!(job_status.state().is_terminal());
    assert!(job_path.join("execution.json").is_file());

    let launcher = InlineTurnLauncher {
        store: store.clone(),
        fault: None,
    };
    let response = JobService::new(&store, &launcher)
        .submit_turn(request.clone())
        .unwrap_or_else(|error| {
            let path = store
                .job(PROJECT_ID, WORKTREE_ID, JobId::new(Uuid::from_u128(3)))
                .unwrap();
            let names = fs::read_dir(&path)
                .map(|entries| {
                    entries
                        .filter_map(Result::ok)
                        .map(|entry| entry.file_name().to_string_lossy().into_owned())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            panic!("submit_turn failed: {error:?}; job entries: {names:?}");
        });
    assert_eq!(response.task().state(), TaskState::Open);
    assert_eq!(response.task().last_outcome(), Some(&TaskOutcome::Done));
    assert!(response.task().session_present());
    assert_eq!(response.task().turns()[0].agent_committed(), Some(false));
    assert_eq!(response.task().files_changed(), &["agent.txt"]);
    assert!(response.task().diff_stat().is_some());
    assert!(
        store
            .task_workspace_if_present(PROJECT_ID, task_id())
            .unwrap()
            .is_some()
    );

    let retry = JobService::new(&store, &launcher)
        .submit_turn(request)
        .unwrap();
    assert_eq!(retry.task().state(), TaskState::Open);
    assert_eq!(retry.task().last_outcome(), Some(&TaskOutcome::Done));
}

#[test]
fn cancelling_a_running_turn_hands_off_before_deadline_and_publishes_cancelled() {
    // Break caught: the turn supervisor retained its guard while waiting for
    // the agent, so a concurrent host cancellation expired with
    // SUPERVISOR_LOCK_PENDING instead of publishing a cancelled turn.
    let script = "printf '%s\\n' '{\"type\":\"thread.started\",\"thread_id\":\"session-cancel\"}'; sleep 8; printf '%s\\n' '{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"{\\\"status\\\":\\\"done\\\",\\\"summary\\\":\\\"natural\\\",\\\"questions\\\":[],\\\"files_changed\\\":[]}\"}}'";
    let (_temp, store, request, cancel_request) = prepared_task_turn(script);
    let submit_store = store.clone();
    let submit_request = request.clone();
    let submit = thread::spawn(move || {
        let launcher = InlineTurnLauncher {
            store: submit_store.clone(),
            fault: None,
        };
        JobService::new(&submit_store, &launcher).submit_turn(submit_request)
    });

    let running_deadline = Instant::now() + Duration::from_secs(5);
    let job_path = store
        .job(PROJECT_ID, WORKTREE_ID, cancel_request.turn_id())
        .unwrap();
    loop {
        let running = fs::read(job_path.join("status.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<JobStatus>(&bytes).ok())
            .is_some_and(|status| {
                status.state() == JobState::Running && status.child_identity().is_some()
            });
        if running {
            break;
        }
        if Instant::now() >= running_deadline {
            let _ = submit.join();
            panic!("inline supervisor did not publish Running before the deadline");
        }
        thread::sleep(Duration::from_millis(10));
    }

    let cancel_started = Instant::now();
    let cancel_store = store.clone();
    let cancel = thread::spawn(move || {
        let launcher = InlineTurnLauncher {
            store: cancel_store.clone(),
            fault: None,
        };
        JobService::new_with_reconciliation(
            &cancel_store,
            &launcher,
            Arc::new(FastReconciliationRuntime {
                clock: Mutex::new(Duration::ZERO),
            }),
        )
        .cancel_task(cancel_request)
    });
    let cancel_result = cancel.join().expect("cancellation thread panicked");
    let cancel_elapsed = cancel_started.elapsed();
    let submit_result = submit.join().expect("submit thread panicked");
    let cancelled =
        cancel_result.unwrap_or_else(|error| panic!("turn cancellation failed: {error}"));
    let submitted = submit_result.unwrap_or_else(|error| panic!("turn runner failed: {error}"));

    assert!(
        cancel_elapsed < Duration::from_secs(5),
        "cancellation exceeded the supervisor handoff deadline: {cancel_elapsed:?}"
    );
    assert_eq!(cancelled.status().state(), TaskState::Open);
    assert_eq!(
        cancelled.status().last_outcome(),
        Some(&TaskOutcome::Cancelled)
    );
    assert_eq!(submitted.task().state(), TaskState::Open);
    assert_eq!(
        submitted.task().last_outcome(),
        Some(&TaskOutcome::Cancelled)
    );

    let job_status: JobStatus = serde_json::from_slice(
        &fs::read(job_path.join("status.json")).expect("cancelled job status is retained"),
    )
    .unwrap();
    assert_eq!(job_status.state(), JobState::Cancelled);
}

#[test]
fn a_cancelled_turn_does_not_record_an_auth_incident_from_the_stderr_tail() {
    let script = concat!(
        "printf '%s\\n' ",
        "'ERROR codex_login::auth::manager: Failed to refresh token: ",
        "Your access token could not be refreshed because your refresh token was already used. ",
        "Please log out and sign in again.' >&2; ",
        "printf '%s\\n' '{\"type\":\"thread.started\",\"thread_id\":\"session-cancel\"}'; ",
        "sleep 8; ",
        "printf '%s\\n' '{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"{\\\"status\\\":\\\"done\\\",\\\"summary\\\":\\\"natural\\\",\\\"questions\\\":[],\\\"files_changed\\\":[]}\"}}'",
    );
    let (temp, store, request, cancel_request) = prepared_task_turn(script);
    let submit_store = store.clone();
    let submit_request = request.clone();
    let submit = thread::spawn(move || {
        let launcher = InlineTurnLauncher {
            store: submit_store.clone(),
            fault: None,
        };
        JobService::new(&submit_store, &launcher).submit_turn(submit_request)
    });

    let running_deadline = Instant::now() + Duration::from_secs(5);
    let job_path = store
        .job(PROJECT_ID, WORKTREE_ID, cancel_request.turn_id())
        .unwrap();
    loop {
        let running = fs::read(job_path.join("status.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<JobStatus>(&bytes).ok())
            .is_some_and(|status| {
                status.state() == JobState::Running && status.child_identity().is_some()
            });
        if running {
            break;
        }
        if Instant::now() >= running_deadline {
            let _ = submit.join();
            panic!("inline supervisor did not publish Running before the deadline");
        }
        thread::sleep(Duration::from_millis(10));
    }

    let cancel_store = store.clone();
    let cancel = thread::spawn(move || {
        let launcher = InlineTurnLauncher {
            store: cancel_store.clone(),
            fault: None,
        };
        JobService::new_with_reconciliation(
            &cancel_store,
            &launcher,
            Arc::new(FastReconciliationRuntime {
                clock: Mutex::new(Duration::ZERO),
            }),
        )
        .cancel_task(cancel_request)
    });
    let cancel_result = cancel.join().expect("cancellation thread panicked");
    let submit_result = submit.join().expect("submit thread panicked");
    let cancelled =
        cancel_result.unwrap_or_else(|error| panic!("turn cancellation failed: {error}"));
    let submitted = submit_result.unwrap_or_else(|error| panic!("turn runner failed: {error}"));

    assert_eq!(
        cancelled.status().last_outcome(),
        Some(&TaskOutcome::Cancelled)
    );
    assert_eq!(
        submitted.task().last_outcome(),
        Some(&TaskOutcome::Cancelled)
    );
    assert!(
        !temp
            .path()
            .join("host")
            .join("auth-incidents.json")
            .exists(),
        "a cancelled turn must not record an auth incident"
    );
}

#[test]
fn turn_section_carries_the_herdr_flag_only_when_set() {
    let identity = GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap();
    let plain = TurnSection::new(material("hello"), "a".repeat(64), identity.clone()).unwrap();
    let plain_json = serde_json::to_string(&plain).unwrap();
    assert!(!plain_json.contains("herdr_reporter"), "{plain_json}");
    assert!(!plain.herdr_reporter());

    let flagged = TurnSection::new(material("hello"), "a".repeat(64), identity)
        .unwrap()
        .with_herdr_reporter(true);
    let json = serde_json::to_string(&flagged).unwrap();
    assert!(json.ends_with(r#","herdr_reporter":true}"#), "{json}");
    let back: TurnSection = serde_json::from_str(&json).unwrap();
    assert!(back.herdr_reporter());
    assert_eq!(back, flagged);

    let old: TurnSection = serde_json::from_str(&plain_json).unwrap();
    assert!(!old.herdr_reporter());
    assert_eq!(old, plain);
}

#[test]
fn verbose_stderr_overflow_does_not_fail_complete_stdout_publication() {
    let script = r#"printf '%s\n' '{"type":"thread.started","thread_id":"session-stderr-cap"}' '{"type":"item.completed","item":{"type":"agent_message","text":"{\"status\":\"done\",\"summary\":\"ok\",\"questions\":[],\"files_changed\":[]}"}}'; /usr/bin/head -c 80 /dev/zero >&2; printf 'LATE-STDERR-FAIL' >&2"#;
    let (_temp, store, request, _cancel) = prepared_task_turn(script);
    let launcher = CappedTurnLauncher {
        store: store.clone(),
        stdout_log_cap: None,
        stderr_log_cap: Some(64),
    };
    let response = JobService::new(&store, &launcher)
        .submit_turn(request)
        .expect("complete stdout must publish despite stderr overflow");
    assert_eq!(response.task().state(), TaskState::Open);
    assert_eq!(response.task().last_outcome(), Some(&TaskOutcome::Done));
    assert!(
        response.task().turns()[0].log_truncated(),
        "stderr overflow must still set aggregate log_truncated"
    );
    let job_path = store
        .job(PROJECT_ID, WORKTREE_ID, JobId::new(Uuid::from_u128(3)))
        .unwrap();
    assert!(!job_path.join("last.md").exists());
    let stderr = fs::read(job_path.join("stderr.log")).unwrap();
    assert!(
        stderr
            .windows(b"LATE-STDERR-FAIL".len())
            .any(|window| window == b"LATE-STDERR-FAIL"),
        "late stderr marker missing: {}",
        String::from_utf8_lossy(&stderr)
    );
}

#[test]
fn truncated_stdout_without_last_md_fails_publication() {
    let script = r#"printf '%s\n' '{"type":"thread.started","thread_id":"session-trunc"}' '{"type":"item.completed","item":{"type":"agent_message","text":"{\"status\":\"done\",\"summary\":\"finished\",\"questions\":[],\"files_changed\":[]}"}}'"#;
    let (_temp, store, request, _cancel) = prepared_task_turn(script);
    let launcher = CappedTurnLauncher {
        store: store.clone(),
        stdout_log_cap: Some(80),
        stderr_log_cap: None,
    };
    let error = JobService::new(&store, &launcher)
        .submit_turn(request)
        .unwrap_err();
    assert_eq!(error.public_code(), "PUBLISH_FAILED");
    let status = store.task_status(PROJECT_ID, task_id()).unwrap();
    assert_eq!(status.state(), TaskState::Open);
    assert!(matches!(
        status.last_outcome(),
        Some(TaskOutcome::Failed { reason }) if reason == "PUBLISH_FAILED"
    ));
    let job_path = store
        .job(PROJECT_ID, WORKTREE_ID, JobId::new(Uuid::from_u128(3)))
        .unwrap();
    assert!(!job_path.join("last.md").exists());
}

// --- herdr reporter hooks -------------------------------------------------
//
// The reporter reads the worker account's HOME to find herdr's socket, so
// these run their bodies in a subprocess whose HOME is a temporary directory:
// either empty (no herdr) or holding a scripted fake herdr started by the
// wrapper.  A test process must never reach the developer's own herdr.

/// A fake Codex turn that binds a session and ends `done` with a summary.
const HERDR_TURN_SCRIPT: &str = "printf '%s\\n' '{\"type\":\"thread.started\",\"thread_id\":\"session-1\"}' '{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"{\\\"status\\\":\\\"done\\\",\\\"summary\\\":\\\"slept\\\",\\\"questions\\\":[],\\\"files_changed\\\":[]}\"}}'";

fn herdr_turn_job_id() -> JobId {
    JobId::new(Uuid::from_u128(3))
}

fn run_flagged_turn(store: &HostStore, request: TaskTurnRequest) -> mac_worker::task::TaskStatus {
    let launcher = InlineTurnLauncher {
        store: store.clone(),
        fault: None,
    };
    JobService::new(store, &launcher)
        .submit_turn(request.with_herdr_reporter(true))
        .unwrap()
        .task()
        .clone()
}

#[test]
fn herdr_reporter_marks_the_turn_unavailable_without_a_socket_wrapper() {
    let temp = tempdir().unwrap();
    let home = temp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    support::agent_launch_fixture::assert_subprocess_success(
        "herdr_reporter_marks_the_turn_unavailable_without_a_socket",
        &[("HOME", home.to_str().unwrap())],
        false,
    );
}

#[test]
fn herdr_reporter_marks_the_turn_unavailable_without_a_socket() {
    if support::agent_launch_fixture::skip_unless_subtest() {
        return;
    }
    let (_temp, store, request, _cancel) = prepared_task_turn(HERDR_TURN_SCRIPT);
    let status = run_flagged_turn(&store, request);

    assert_eq!(status.state(), TaskState::Open);
    let turn = &status.turns()[0];
    assert_eq!(
        turn.herdr().map(|report| report.state),
        Some(mac_worker::task::HerdrTurnState::Unavailable)
    );
    assert_eq!(turn.herdr().and_then(|report| report.pane_id.clone()), None);
    let log = fs::read_to_string(
        store
            .job(PROJECT_ID, WORKTREE_ID, herdr_turn_job_id())
            .unwrap()
            .join("supervisor.log"),
    )
    .unwrap();
    assert_eq!(
        log.matches("herdr reporter: unavailable (absent)").count(),
        1,
        "{log}"
    );
}

#[test]
fn a_turn_without_the_flag_never_touches_herdr_wrapper() {
    let temp = tempdir().unwrap();
    let home = temp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let server = support::fake_herdr::FakeHerdr::start_in_home(&home);
    support::agent_launch_fixture::assert_subprocess_success(
        "a_turn_without_the_flag_never_touches_herdr",
        &[("HOME", home.to_str().unwrap())],
        false,
    );
    assert!(server.requests().is_empty(), "{:?}", server.requests());
}

#[test]
fn a_turn_without_the_flag_never_touches_herdr() {
    if support::agent_launch_fixture::skip_unless_subtest() {
        return;
    }
    let (_temp, store, request, _cancel) = prepared_task_turn(HERDR_TURN_SCRIPT);
    let launcher = InlineTurnLauncher {
        store: store.clone(),
        fault: None,
    };
    let response = JobService::new(&store, &launcher)
        .submit_turn(request)
        .unwrap();
    assert_eq!(response.task().turns()[0].herdr(), None);
    TaskStore::new(&store, &mac_worker::process::SystemProcessRunner)
        .close(&TaskCloseRequest::new(PROJECT_ID, task_id(), false))
        .unwrap();
}

#[test]
fn herdr_reporter_attaches_the_turn_and_close_removes_its_tab_wrapper() {
    use support::fake_herdr::{FakeHerdr, Reply};

    let temp = tempdir().unwrap();
    let home = temp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let server = FakeHerdr::start_in_home(&home);
    let idle = || {
        Reply::Result(serde_json::json!({
            "type": "pane_process_info",
            "process_info": { "shell_pid": 500, "foreground_processes": [{ "name": "zsh", "pid": 500 }] }
        }))
    };
    // start: no workspace yet
    server.reply(
        "workspace.list",
        Reply::Result(serde_json::json!({ "type": "workspace_list", "workspaces": [] })),
    );
    server.reply(
        "workspace.create",
        Reply::Result(serde_json::json!({
            "type": "workspace_created",
            "workspace": { "workspace_id": "w9", "label": "mac-worker" },
            "tab": { "tab_id": "w9:t1", "workspace_id": "w9" },
            "root_pane": { "pane_id": "w9:p1" }
        })),
    );
    server.reply(
        "tab.list",
        Reply::Result(serde_json::json!({ "type": "tab_list", "tabs": [] })),
    );
    server.reply(
        "tab.create",
        Reply::Result(serde_json::json!({
            "type": "tab_created",
            "tab": { "tab_id": "w9:t2", "label": "x", "workspace_id": "w9" },
            "root_pane": { "pane_id": "w9:p2", "tab_id": "w9:t2", "workspace_id": "w9" }
        })),
    );
    server.reply("pane.process_info", idle());
    // terminal: the pane is still there
    server.reply("pane.process_info", idle());
    // close: the workspace and the task's tab exist now
    server.reply(
        "workspace.list",
        Reply::Result(serde_json::json!({
            "type": "workspace_list",
            "workspaces": [{ "workspace_id": "w9", "label": "mac-worker" }]
        })),
    );
    server.reply(
        "tab.list",
        Reply::Result(serde_json::json!({
            "type": "tab_list",
            "tabs": [
                { "tab_id": "w9:t1", "label": "1", "workspace_id": "w9" },
                { "tab_id": "w9:t2", "label": "task 000000000000 · turn 1", "workspace_id": "w9" }
            ]
        })),
    );
    // after the close only the workspace's own first tab is left
    server.reply(
        "tab.list",
        Reply::Result(serde_json::json!({
            "type": "tab_list",
            "tabs": [{ "tab_id": "w9:t1", "label": "1", "workspace_id": "w9" }]
        })),
    );

    support::agent_launch_fixture::assert_subprocess_success(
        "herdr_reporter_attaches_the_turn_and_close_removes_its_tab",
        &[("HOME", home.to_str().unwrap())],
        false,
    );

    let requests = server.requests();
    let methods: Vec<&str> = requests
        .iter()
        .map(|request| request["method"].as_str().unwrap_or(""))
        .collect();
    assert_eq!(
        methods,
        vec![
            "workspace.list",
            "workspace.create",
            "tab.list",
            "tab.create",
            "pane.process_info",
            "pane.send_input",
            "pane.report_agent",
            "pane.report_metadata",
            "pane.process_info",
            "pane.report_agent",
            "pane.report_metadata",
            "workspace.list",
            "tab.list",
            "tab.close",
            "tab.list",
            "workspace.close",
        ],
        "{requests:?}"
    );
    let by_method = |method: &str| -> Vec<&serde_json::Value> {
        requests
            .iter()
            .filter(|request| request["method"] == method)
            .map(|request| &request["params"])
            .collect()
    };
    assert_eq!(
        by_method("tab.create")[0]["label"],
        "task 000000000000 · turn 1"
    );
    assert_eq!(
        by_method("pane.send_input")[0]["text"],
        format!(
            "exec ~/.local/bin/worker host follow-turn {PROJECT_ID} {WORKTREE_ID} {}",
            herdr_turn_job_id()
        )
    );
    let agent = by_method("pane.report_agent");
    assert_eq!(agent[0]["state"], "working");
    assert_eq!(agent[0]["agent"], "codex");
    assert_eq!(
        agent[0]["message"], "turn prompt",
        "the working message is the task title, which the harness derives from the prompt's first line"
    );
    assert_eq!(
        agent[1]["state"], "idle",
        "a done turn shows as done until seen"
    );
    assert_eq!(agent[1]["message"], "slept");
    let metadata = by_method("pane.report_metadata");
    assert_eq!(metadata[0]["tokens"]["mw_outcome"], "running");
    assert_eq!(metadata[1]["tokens"]["mw_outcome"], "done");
    assert_eq!(metadata[1]["display_agent"], "mac-worker");
    assert_eq!(by_method("tab.close")[0]["tab_id"], "w9:t2");
    for request in &requests {
        let text = request.to_string();
        for forbidden in [
            "/Users/",
            "/private/",
            "/var/",
            "/tmp/",
            "prompt.md",
            "session-1",
        ] {
            assert!(!text.contains(forbidden), "{forbidden} in {text}");
        }
    }
}

#[test]
fn herdr_reporter_attaches_the_turn_and_close_removes_its_tab() {
    if support::agent_launch_fixture::skip_unless_subtest() {
        return;
    }
    let (_temp, store, request, _cancel) = prepared_task_turn(HERDR_TURN_SCRIPT);
    let status = run_flagged_turn(&store, request);

    let turn = &status.turns()[0];
    assert_eq!(
        turn.herdr().map(|report| report.state),
        Some(mac_worker::task::HerdrTurnState::Attached)
    );
    assert_eq!(
        turn.herdr().and_then(|report| report.pane_id.as_deref()),
        Some("w9:p2")
    );
    let log = fs::read_to_string(
        store
            .job(PROJECT_ID, WORKTREE_ID, herdr_turn_job_id())
            .unwrap()
            .join("supervisor.log"),
    )
    .unwrap_or_default();
    assert!(
        !log.contains("herdr reporter"),
        "an attached turn leaves no reporter diagnostic: {log}"
    );
    TaskStore::new(&store, &mac_worker::process::SystemProcessRunner)
        .close(&TaskCloseRequest::new(PROJECT_ID, task_id(), false))
        .unwrap();
}
