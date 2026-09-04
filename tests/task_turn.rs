#[allow(dead_code)]
mod support;

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    process,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use mac_worker::{
    agent::{
        AgentKind, PermissionPolicy, PromptDelivery, TurnLaunch, TurnLimits, TurnParams,
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
        BaseOid, ClosePolicy, GitIdentity, PublishMode, TaskId, TaskLimits, TaskMeta,
        TaskMetaInput, TaskOutcome, TaskSource, TaskState,
    },
    task_store::{TaskCancelRequest, TaskPrepareRequest, TaskStore},
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
        policy: PermissionPolicy::Workspace,
        source: TaskSource::Local { wip: false },
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

    let request = mac_worker::turn::TaskTurnRequest::new(
        mac_worker::job::SubmitRequest::new(projected),
        turn,
        prompt,
    );
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
        policy: PermissionPolicy::Workspace,
        source: TaskSource::Local { wip: false },
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
        &["why is [path] locked?".to_owned()]
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
        policy: PermissionPolicy::Workspace,
        source: TaskSource::Local { wip: false },
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
