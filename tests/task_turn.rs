#[allow(dead_code)]
mod support;

use std::{fs, os::unix::fs::PermissionsExt, process};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy, PromptDelivery, TurnLaunch, TurnLimits},
    error::WorkerError,
    host_store::HostStore,
    job::{
        ClientId, CommandSpec, JobId, LeaseAcquireRequest, LeaseRecord, LeaseToken,
        RequestFingerprintMaterial,
    },
    job_service::{JobService, LaunchCandidate, SupervisorLauncher},
    lease::{AdmissionFacts, LeaseService},
    protocol::{MemoryPressure, PROTOCOL_VERSION, SUPERVISION_VERSION},
    supervisor::{
        LaunchPlan, StdinSource, StdoutSink, Supervisor, SupervisorFaultPoint,
        SystemProcessInspector,
    },
    task::{
        BaseOid, ClosePolicy, GitIdentity, PublishMode, TaskId, TaskLimits, TaskMeta,
        TaskMetaInput, TaskOutcome, TaskSource, TaskState,
    },
    task_store::{TaskPrepareRequest, TaskStore},
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
    assert_eq!(PROTOCOL_VERSION, 4);
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
            "printf changed > agent.txt; printf '%s\\n' '{\"type\":\"thread.started\",\"thread_id\":\"session-1\"}' '{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"{\\\"status\\\":\\\"done\\\",\\\"summary\\\":\\\"ok\\\",\\\"questions\\\":[],\\\"files_changed\\\":[]}\"}}'".into(),
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
        policy: PermissionPolicy::Workspace,
        source: TaskSource::Local { wip: false },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid,
        limits: TaskLimits::new(turn_limits, 3).unwrap(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
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
        policy: PermissionPolicy::Workspace,
        source: TaskSource::Local { wip: false },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid,
        limits: TaskLimits::new(turn_limits, 3).unwrap(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
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
