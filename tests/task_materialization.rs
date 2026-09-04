#[allow(dead_code)]
mod support;

use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy},
    error::WorkerError,
    host_store::{AdmissionGuard, HostStore, TransferGuard},
    job::{
        ClientId, CommandSpec, JobId, LeaseAcquireRequest, LeaseToken, RequestFingerprintMaterial,
    },
    lease::{AdmissionFacts, LeaseService},
    process::SystemProcessRunner,
    protocol::MemoryPressure,
    task::{
        BaseOid, ClosePolicy, GitIdentity, PublishMode, TaskId, TaskLimits, TaskMeta,
        TaskMetaInput, TaskState,
    },
    task_store::{
        MAX_DIFF_BYTES, SessionBinding, TaskCloseRequest, TaskDiffRequest, TaskPrepareRequest,
        TaskStatusRequest, TaskStore,
    },
};
use support::GitRepo;
use tempfile::TempDir;
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn task_id() -> TaskId {
    TaskId::new(Uuid::from_u128(1))
}

fn job_id() -> JobId {
    JobId::new(Uuid::from_u128(10))
}

fn client_id() -> ClientId {
    ClientId::new(Uuid::from_u128(20))
}

fn lease_token() -> LeaseToken {
    LeaseToken::new(Uuid::from_u128(30))
}

fn task_meta(base_oid: BaseOid) -> TaskMeta {
    TaskMeta::new(TaskMetaInput {
        task_id: task_id(),
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
        agent: AgentKind::Codex,
        model: None,
        policy: PermissionPolicy::Workspace,
        source: mac_worker::task::TaskSource::Local { wip: false },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid,
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: "make the requested change".into(),
        created_at_millis: 100,
    })
    .unwrap()
}

fn git(path: &Path, args: &[&str]) -> String {
    let output = Command::new("/usr/bin/git")
        .current_dir(path)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .args(args)
        .output()
        .expect("run git fixture command");
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}

fn git_status(path: &Path, args: &[&str]) -> std::process::Output {
    Command::new("/usr/bin/git")
        .current_dir(path)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .args(args)
        .output()
        .expect("run git status fixture command")
}

fn git_ref_exists(mirror: &mac_worker::rooted_fs::RootedDir, reference: &str) -> bool {
    Command::new("/usr/bin/git")
        .args(["--git-dir"])
        .arg(mirror.path())
        .args(["show-ref", "--verify", "--quiet", reference])
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .status()
        .expect("check mirror ref")
        .success()
}

fn store_with_mirror() -> (TempDir, HostStore, BaseOid) {
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let source = GitRepo::init();
    source.write("a.txt", b"base\n");
    source.commit_all("base");
    let base_oid: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    let mirror = store.mirror(PROJECT_ID).unwrap();
    let mirror_path = mirror.path().to_string_lossy().into_owned();
    let reference = format!("HEAD:refs/mac-worker/bases/{}", task_id());
    let output = source.git(&["push", &mirror_path, &reference]);
    assert!(
        output.status.success(),
        "base push failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    (temp, store, base_oid)
}

fn transfer_guard(store: &HostStore) -> (AdmissionGuard, TransferGuard) {
    let admission = store.admission_lock(job_id()).unwrap();
    let transfer = store.transfer_lock_after(&admission, job_id()).unwrap();
    (admission, transfer)
}

fn prepare_request(base_oid: BaseOid) -> TaskPrepareRequest {
    TaskPrepareRequest::new(task_meta(base_oid), job_id(), "mini-1")
}

fn prepare_task(store: &HostStore, base_oid: BaseOid) {
    let (_admission, transfer) = transfer_guard(store);
    TaskStore::new(store, &SystemProcessRunner)
        .prepare(&prepare_request(base_oid), &transfer)
        .unwrap();
}

fn acquire_lease(store: &HostStore) {
    let request = LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            job_id(),
            client_id(),
            lease_token(),
            100,
            "mini-1".into(),
            PROJECT_ID.into(),
            WORKTREE_ID.into(),
            "c".repeat(64),
            String::new(),
            30_000,
            "heavy".into(),
            CommandSpec::shell("true".into()).unwrap(),
        )
        .unwrap(),
    );
    LeaseService::new(store)
        .acquire(
            &request,
            &AdmissionFacts {
                free_disk_bytes: 100 * 1024 * 1024 * 1024,
                total_disk_bytes: 200 * 1024 * 1024 * 1024,
                memory_pressure: MemoryPressure::Normal,
                swap_used_bytes: Some(0),
            },
            100,
        )
        .unwrap();
}

#[test]
fn prepare_creates_shared_clone_on_task_branch_at_verified_base_under_lease() {
    let (_temp, store, base_oid) = store_with_mirror();
    acquire_lease(&store);
    let (_admission, transfer) = transfer_guard(&store);
    let response = TaskStore::new(&store, &SystemProcessRunner)
        .prepare(&prepare_request(base_oid.clone()), &transfer)
        .unwrap();

    assert_eq!(response.head(), &base_oid);
    assert!(!response.reused());
    let workspace = store.task_workspace(PROJECT_ID, task_id()).unwrap();
    assert_eq!(
        git(&workspace, &["rev-parse", "--abbrev-ref", "HEAD"]),
        format!("task/{}", task_id())
    );
    assert!(workspace.join(".git").is_dir());
    assert!(
        fs::read_to_string(workspace.join(".git/objects/info/alternates"))
            .unwrap()
            .trim()
            .ends_with("objects")
    );
    let mirror = store.mirror(PROJECT_ID).unwrap();
    assert!(!git_ref_exists(
        &mirror,
        &format!("refs/heads/task/{}", task_id())
    ));
    assert_eq!(
        store.task_status(PROJECT_ID, task_id()).unwrap().state(),
        TaskState::Active
    );
}

#[test]
fn prepare_is_idempotent_on_retry_and_refuses_inconsistent_state() {
    let (_temp, store, base_oid) = store_with_mirror();
    acquire_lease(&store);
    prepare_task(&store, base_oid.clone());

    {
        let (_admission, transfer) = transfer_guard(&store);
        assert!(
            TaskStore::new(&store, &SystemProcessRunner)
                .prepare(&prepare_request(base_oid.clone()), &transfer)
                .unwrap()
                .reused()
        );
    }

    let workspace = store.task_workspace(PROJECT_ID, task_id()).unwrap();
    fs::remove_file(workspace.join(".git/HEAD")).unwrap();
    {
        let (_admission, transfer) = transfer_guard(&store);
        assert!(
            !TaskStore::new(&store, &SystemProcessRunner)
                .prepare(&prepare_request(base_oid.clone()), &transfer)
                .unwrap()
                .reused()
        );
    }

    let workspace = store.task_workspace(PROJECT_ID, task_id()).unwrap();
    let commit = git_status(
        &workspace,
        &[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.test",
            "commit",
            "--allow-empty",
            "-m",
            "change",
        ],
    );
    assert!(commit.status.success());
    let error = {
        let (_admission, transfer) = transfer_guard(&store);
        TaskStore::new(&store, &SystemProcessRunner)
            .prepare(&prepare_request(base_oid), &transfer)
            .unwrap_err()
    };
    assert_eq!(error.public_code(), "WORKTREE_INCONSISTENT");
}

#[test]
fn resume_reuses_the_published_workspace_and_appends_one_active_turn() {
    let (_temp, store, base_oid) = store_with_mirror();
    acquire_lease(&store);
    prepare_task(&store, base_oid.clone());
    TaskStore::new(&store, &SystemProcessRunner)
        .publish_branch_into_mirror(PROJECT_ID, task_id())
        .unwrap();
    TaskStore::new(&store, &SystemProcessRunner)
        .bind_session(
            PROJECT_ID,
            task_id(),
            SessionBinding::new(AgentKind::Codex, "session-1", 101).unwrap(),
        )
        .unwrap();

    let workspace = store.task_workspace(PROJECT_ID, task_id()).unwrap();
    let error = TaskStore::new(&store, &SystemProcessRunner)
        .prepare_resume(
            PROJECT_ID,
            task_id(),
            JobId::new(Uuid::from_u128(11)),
            2,
            "mini-2",
            &base_oid,
        )
        .unwrap_err();
    assert_eq!(error.public_code(), "TASK_TURN_CONFLICT");
    let resumed = TaskStore::new(&store, &SystemProcessRunner)
        .prepare_resume(
            PROJECT_ID,
            task_id(),
            JobId::new(Uuid::from_u128(11)),
            2,
            "mini-1",
            &base_oid,
        )
        .unwrap();

    assert_eq!(resumed.0.task_id(), task_id());
    assert_eq!(resumed.1.state(), TaskState::Active);
    assert!(resumed.1.session_present());
    assert_eq!(resumed.1.turns().len(), 2);
    assert_eq!(resumed.1.turns()[1].turn_number(), 2);
    assert_eq!(
        resumed.1.turns()[1].turn_id(),
        JobId::new(Uuid::from_u128(11))
    );
    assert_eq!(
        git(&workspace, &["rev-parse", "--abbrev-ref", "HEAD"]),
        format!("task/{}", task_id())
    );
    assert_eq!(git(&workspace, &["rev-parse", "HEAD"]), base_oid.as_str());
}

#[test]
fn diff_uses_private_index_and_bounds_escaped_output() {
    let (_temp, store, base_oid) = store_with_mirror();
    acquire_lease(&store);
    prepare_task(&store, base_oid.clone());
    let workspace = store.task_workspace(PROJECT_ID, task_id()).unwrap();
    let index = workspace.join(".git/index");
    let before = fs::read(&index).unwrap();
    fs::write(workspace.join("a.txt"), b"changed\n").unwrap();

    let diff = TaskStore::new(&store, &SystemProcessRunner)
        .diff(&TaskDiffRequest::new(PROJECT_ID, task_id(), false))
        .unwrap();
    assert!(diff.text().contains("+changed") && !diff.truncated());
    assert_eq!(fs::read(&index).unwrap(), before);
    assert!(!workspace.join(".git/index.lock").exists());

    fs::write(workspace.join("large.txt"), vec![b'x'; MAX_DIFF_BYTES]).unwrap();
    let large = TaskStore::new(&store, &SystemProcessRunner)
        .diff(&TaskDiffRequest::new(PROJECT_ID, task_id(), false))
        .unwrap();
    assert!(large.truncated());
    assert!(large.text().len() <= MAX_DIFF_BYTES);
}

#[test]
fn close_removes_only_workspace_and_discard_prunes_published_refs() {
    let (_temp, store, base_oid) = store_with_mirror();
    acquire_lease(&store);
    prepare_task(&store, base_oid);
    let workspace = store.task_workspace(PROJECT_ID, task_id()).unwrap();
    let commit = git_status(
        &workspace,
        &[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.test",
            "commit",
            "--allow-empty",
            "-m",
            "published",
        ],
    );
    assert!(commit.status.success());
    TaskStore::new(&store, &SystemProcessRunner)
        .publish_branch_into_mirror(PROJECT_ID, task_id())
        .unwrap();

    TaskStore::new(&store, &SystemProcessRunner)
        .close(&TaskCloseRequest::new(PROJECT_ID, task_id(), false))
        .unwrap();
    assert!(
        store
            .task_workspace_if_present(PROJECT_ID, task_id())
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .task_dir(PROJECT_ID, task_id())
            .unwrap()
            .join("meta.json")
            .exists()
    );
    assert_eq!(
        store.task_status(PROJECT_ID, task_id()).unwrap().state(),
        TaskState::Closed
    );
    let mirror = store.mirror(PROJECT_ID).unwrap();
    assert!(git_ref_exists(
        &mirror,
        &format!("refs/heads/task/{}", task_id())
    ));

    TaskStore::new(&store, &SystemProcessRunner)
        .close(&TaskCloseRequest::new(PROJECT_ID, task_id(), true))
        .unwrap();
    assert_eq!(
        store.task_status(PROJECT_ID, task_id()).unwrap().state(),
        TaskState::Abandoned
    );
    assert!(!git_ref_exists(
        &mirror,
        &format!("refs/heads/task/{}", task_id())
    ));
    assert!(!git_ref_exists(
        &mirror,
        &format!("refs/mac-worker/bases/{}", task_id())
    ));
}

#[test]
fn session_binding_is_owner_only_idempotent_and_strict() {
    let (_temp, store, base_oid) = store_with_mirror();
    acquire_lease(&store);
    prepare_task(&store, base_oid);
    let binding = SessionBinding::new(AgentKind::Codex, "session-1", 200).unwrap();
    let task_store = TaskStore::new(&store, &SystemProcessRunner);
    task_store
        .bind_session(PROJECT_ID, task_id(), binding.clone())
        .unwrap();
    assert_eq!(
        task_store.session(PROJECT_ID, task_id()).unwrap(),
        Some(binding)
    );
    task_store
        .bind_session(
            PROJECT_ID,
            task_id(),
            SessionBinding::new(AgentKind::Codex, "session-1", 200).unwrap(),
        )
        .unwrap();
    let error = task_store
        .bind_session(
            PROJECT_ID,
            task_id(),
            SessionBinding::new(AgentKind::Codex, "session-2", 201).unwrap(),
        )
        .unwrap_err();
    assert_eq!(error.public_code(), "TASK_SESSION_CONFLICT");
    let session = store
        .task_dir(PROJECT_ID, task_id())
        .unwrap()
        .join("session.json");
    assert_eq!(
        fs::metadata(session).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[test]
fn task_dtos_carry_protocol_version_and_reject_unknown_fields() {
    let request = TaskStatusRequest::new(PROJECT_ID, task_id());
    let value = serde_json::to_value(&request).unwrap();
    assert_eq!(
        value["protocol_version"],
        mac_worker::protocol::PROTOCOL_VERSION
    );
    let mut object = value.as_object().unwrap().clone();
    object.insert("extra".into(), serde_json::json!(true));
    assert!(serde_json::from_value::<TaskStatusRequest>(object.into()).is_err());
}

#[test]
fn unknown_task_status_is_not_created_as_a_side_effect() {
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let error = TaskStore::new(&store, &SystemProcessRunner)
        .status(&TaskStatusRequest::new(PROJECT_ID, task_id()))
        .unwrap_err();
    assert!(matches!(
        error,
        WorkerError::Task {
            code: "TASK_NOT_FOUND",
            ..
        }
    ));
    assert!(!temp.path().join("host/tasks").join(PROJECT_ID).exists());
}
