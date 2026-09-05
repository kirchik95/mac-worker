#[allow(dead_code)]
mod support;

use std::{fs, path::Path, process::Command};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy},
    host_store::{HostGc, HostStore},
    job::{
        ClientId, CommandSpec, JobId, LeaseAcquireRequest, LeaseToken, RequestFingerprintMaterial,
    },
    lease::{AdmissionFacts, LeaseService},
    process::SystemProcessRunner,
    protocol::MemoryPressure,
    task::{
        BaseOid, ClosePolicy, GitIdentity, PublishMode, TaskId, TaskLimits, TaskMeta,
        TaskMetaInput, TaskSource, TaskState,
    },
    task_store::{SessionBinding, TaskStore},
    transfer_repo::{TransferGc, TransferRepo},
};
use support::GitRepo;
use tempfile::TempDir;
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const OTHER_PROJECT_ID: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn task_id(value: u128) -> TaskId {
    TaskId::new(Uuid::from_u128(value))
}

fn job_id(value: u128) -> JobId {
    JobId::new(Uuid::from_u128(value))
}

fn client_id() -> ClientId {
    ClientId::new(Uuid::from_u128(20))
}

fn lease_token() -> LeaseToken {
    LeaseToken::new(Uuid::from_u128(30))
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

fn ref_exists(path: &Path, reference: &str) -> bool {
    Command::new("/usr/bin/git")
        .args(["--git-dir"])
        .arg(path)
        .args(["show-ref", "--verify", "--quiet", reference])
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .status()
        .expect("check git ref")
        .success()
}

fn set_ref(path: &Path, reference: &str, oid: &str) {
    let output = Command::new("/usr/bin/git")
        .args(["--git-dir"])
        .arg(path)
        .args(["update-ref", reference, oid])
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .expect("set git ref");
    assert!(
        output.status.success(),
        "set ref failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn task_meta(task_id: TaskId, base_oid: BaseOid, created_at_millis: u64) -> TaskMeta {
    TaskMeta::new(TaskMetaInput {
        task_id,
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
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: "gc fixture".into(),
        created_at_millis,
    })
    .unwrap()
}

fn acquire_lease(store: &HostStore, job: JobId) {
    let request = LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            job,
            client_id(),
            lease_token(),
            1,
            "mini-1".into(),
            PROJECT_ID.into(),
            PROJECT_ID.into(),
            WORKTREE_ID.into(),
            "c".repeat(64),
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
            1,
        )
        .unwrap();
}

fn fixture() -> (TempDir, HostStore, GitRepo, BaseOid) {
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"base\n");
    source.commit_all("base");
    let base_oid: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    let mirror = store.mirror(PROJECT_ID).unwrap();
    let output = source.git(&[
        "push",
        mirror.path().to_str().unwrap(),
        &format!("HEAD:refs/mac-worker/bases/{}", task_id(1)),
    ]);
    assert!(output.status.success());
    (temp, store, source, base_oid)
}

fn prepare_task(store: &HostStore, task: TaskId, job: JobId, base_oid: &BaseOid) {
    acquire_lease(store, job);
    let admission = store.admission_lock(job).unwrap();
    let transfer = store.transfer_lock_after(&admission, job).unwrap();
    TaskStore::new(store, &SystemProcessRunner)
        .prepare(
            &mac_worker::task_store::TaskPrepareRequest::new(
                task_meta(task, base_oid.clone(), 1),
                job,
                "mini-1",
            ),
            &transfer,
        )
        .unwrap();
    drop(transfer);
    drop(admission);
    TaskStore::new(store, &SystemProcessRunner)
        .publish_branch_into_mirror(PROJECT_ID, task)
        .unwrap();
}

fn rewrite_status(store: &HostStore, task: TaskId, state: TaskState, updated_at_millis: u64) {
    let status = store.task_status(PROJECT_ID, task).unwrap();
    let replacement = mac_worker::task::TaskStatus::new(
        state,
        status.last_outcome().cloned(),
        status.worker().map(str::to_owned),
        status.session_present(),
        status.head_oid().cloned(),
        status.summary().map(str::to_owned),
        status.questions().to_vec(),
        status.files_changed().to_vec(),
        status.diff_stat().map(str::to_owned),
        status.turns().to_vec(),
        updated_at_millis,
    )
    .unwrap();
    fs::write(
        store
            .task_dir(PROJECT_ID, task)
            .unwrap()
            .join("status.json"),
        serde_json::to_vec(&replacement).unwrap(),
    )
    .unwrap();
}

#[test]
fn gc_closes_idle_open_task_but_preserves_result_branch_and_metadata() {
    let (_temp, store, _source, base_oid) = fixture();
    let task = task_id(1);
    prepare_task(&store, task, job_id(1), &base_oid);
    rewrite_status(&store, task, TaskState::Open, 1);

    let now = 1 + mac_worker::host_store::TASK_RETENTION_MILLIS;
    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    let candidate = preview
        .candidates()
        .iter()
        .find(|candidate| candidate.kind() == "task" && candidate.reason() == "open task retention")
        .expect("idle open task candidate");
    assert_eq!(candidate.identifier(), format!("{PROJECT_ID}/{task}"));
    assert!(candidate.size_bytes() > 0);

    let applied = HostGc::new(&store, &SystemProcessRunner)
        .apply_at(now)
        .unwrap();
    assert!(
        applied
            .applied()
            .iter()
            .any(|item| item.identifier() == candidate.identifier())
    );
    assert_eq!(
        store.task_status(PROJECT_ID, task).unwrap().state(),
        TaskState::Closed
    );
    assert!(
        store
            .task_workspace_if_present(PROJECT_ID, task)
            .unwrap()
            .is_none()
    );
    let mirror = store.mirror(PROJECT_ID).unwrap();
    assert!(ref_exists(
        mirror.path(),
        &format!("refs/heads/task/{task}")
    ));
}

#[test]
fn gc_prunes_expired_task_branch_without_removing_foreign_mirror_refs() {
    let (_temp, store, source, base_oid) = fixture();
    let task = task_id(1);
    prepare_task(&store, task, job_id(1), &base_oid);
    rewrite_status(&store, task, TaskState::Closed, 1);
    let mirror = store.mirror(PROJECT_ID).unwrap();
    let keep = Command::new("/usr/bin/git")
        .args(["--git-dir", mirror.path().to_str().unwrap(), "update-ref"])
        .args([
            "refs/heads/keep",
            &git(source.root(), &["rev-parse", "HEAD"]),
        ])
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap();
    assert!(
        keep.status.success(),
        "keep ref push failed: {}",
        String::from_utf8_lossy(&keep.stderr)
    );

    let now = 1 + mac_worker::host_store::BRANCH_RETENTION_MILLIS;
    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    assert!(preview.candidates().iter().any(|candidate| {
        candidate.kind() == "branch" && candidate.reason() == "branch retention"
    }));
    let applied = HostGc::new(&store, &SystemProcessRunner)
        .apply_at(now)
        .unwrap();
    assert!(!applied.applied().is_empty());
    assert!(!ref_exists(
        mirror.path(),
        &format!("refs/heads/task/{task}")
    ));
    assert!(!ref_exists(
        mirror.path(),
        &format!("refs/mac-worker/bases/{task}")
    ));
    assert!(ref_exists(mirror.path(), "refs/heads/keep"));
}

#[test]
fn gc_expires_task_metadata_before_a_newer_result_branch() {
    let (_temp, store, _source, base_oid) = fixture();
    let task = task_id(1);
    prepare_task(&store, task, job_id(1), &base_oid);
    rewrite_status(&store, task, TaskState::Closed, 1);
    let mirror_path = store.mirror(PROJECT_ID).unwrap().path().to_path_buf();

    let now = 1 + mac_worker::host_store::JOB_RETENTION_MILLIS;
    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    assert!(preview.candidates().iter().any(|candidate| {
        candidate.kind() == "task" && candidate.reason() == "task metadata retention"
    }));
    assert!(
        !preview
            .candidates()
            .iter()
            .any(|candidate| candidate.kind() == "branch")
    );

    HostGc::new(&store, &SystemProcessRunner)
        .apply_at(now)
        .unwrap();
    assert!(!store.task_dir(PROJECT_ID, task).unwrap().exists());
    assert!(ref_exists(&mirror_path, &format!("refs/heads/task/{task}")));
    assert!(ref_exists(
        &mirror_path,
        &format!("refs/mac-worker/bases/{task}")
    ));
}

#[test]
fn gc_prunes_orphaned_task_refs_after_task_metadata_retention() {
    let (_temp, store, _source, base_oid) = fixture();
    let task = task_id(1);
    prepare_task(&store, task, job_id(1), &base_oid);
    rewrite_status(&store, task, TaskState::Closed, 1);
    let mirror_path = store.mirror(PROJECT_ID).unwrap().path().to_path_buf();

    HostGc::new(&store, &SystemProcessRunner)
        .apply_at(1 + mac_worker::host_store::JOB_RETENTION_MILLIS)
        .unwrap();
    let now = u64::MAX / 2;
    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    assert!(preview.candidates().iter().any(|candidate| {
        candidate.kind() == "branch" && candidate.identifier() == format!("{PROJECT_ID}/{task}")
    }));
    assert!(
        !preview
            .candidates()
            .iter()
            .any(|candidate| candidate.kind() == "mirror" && candidate.identifier() == PROJECT_ID)
    );

    HostGc::new(&store, &SystemProcessRunner)
        .apply_at(now)
        .unwrap();
    assert!(!ref_exists(
        &mirror_path,
        &format!("refs/heads/task/{task}")
    ));
    assert!(!ref_exists(
        &mirror_path,
        &format!("refs/mac-worker/bases/{task}")
    ));
    let mirror_preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    assert!(
        mirror_preview.candidates().iter().any(|candidate| {
            candidate.kind() == "mirror" && candidate.identifier() == PROJECT_ID
        })
    );
}

#[test]
fn gc_does_not_touch_active_tasks_or_their_base_refs() {
    let (_temp, store, _source, base_oid) = fixture();
    let task = task_id(1);
    prepare_task(&store, task, job_id(1), &base_oid);
    rewrite_status(&store, task, TaskState::Active, 1);

    let now = u64::MAX / 2;
    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    assert!(
        !preview
            .candidates()
            .iter()
            .any(|candidate| { candidate.identifier().ends_with(&format!("/{task}")) })
    );
    assert!(ref_exists(
        store.mirror(PROJECT_ID).unwrap().path(),
        &format!("refs/mac-worker/bases/{task}")
    ));
}

#[test]
fn gc_fails_closed_on_malformed_task_metadata() {
    let (_temp, store, _source, base_oid) = fixture();
    let task = task_id(1);
    prepare_task(&store, task, job_id(1), &base_oid);
    fs::write(
        store.task_dir(PROJECT_ID, task).unwrap().join("meta.json"),
        br#"{"#,
    )
    .unwrap();

    let error = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(u64::MAX / 2)
        .unwrap_err();
    assert_eq!(error.public_code(), "GC_METADATA_INVALID");
    assert!(store.task_dir(PROJECT_ID, task).unwrap().exists());
}

#[test]
fn gc_is_idempotent_and_marks_empty_mirrors_only_after_all_refs_are_gone() {
    let (_temp, store, _source, base_oid) = fixture();
    let task = task_id(1);
    prepare_task(&store, task, job_id(1), &base_oid);
    rewrite_status(&store, task, TaskState::Closed, 1);
    let empty_mirror = store.mirror(OTHER_PROJECT_ID).unwrap();
    let empty_path = empty_mirror.path().to_path_buf();

    let now = u64::MAX / 2;
    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    assert!(preview.candidates().iter().any(|candidate| {
        candidate.kind() == "mirror" && candidate.identifier() == OTHER_PROJECT_ID
    }));
    HostGc::new(&store, &SystemProcessRunner)
        .apply_at(now)
        .unwrap();
    assert!(!empty_path.exists());

    let second = HostGc::new(&store, &SystemProcessRunner)
        .apply_at(now)
        .unwrap();
    assert!(
        !second
            .candidates()
            .iter()
            .any(|candidate| { candidate.identifier().ends_with(&format!("/{task}")) })
    );
}

#[test]
fn gc_never_requests_native_agent_session_deletion() {
    let (_temp, store, _source, base_oid) = fixture();
    let task = task_id(1);
    prepare_task(&store, task, job_id(1), &base_oid);
    TaskStore::new(&store, &SystemProcessRunner)
        .bind_session(
            PROJECT_ID,
            task,
            SessionBinding::new(AgentKind::Codex, "session-1", 1).unwrap(),
        )
        .unwrap();
    rewrite_status(&store, task, TaskState::Open, 1);

    let report = HostGc::new(&store, &SystemProcessRunner)
        .apply_at(1 + mac_worker::host_store::TASK_RETENTION_MILLIS)
        .unwrap();
    assert!(
        report
            .applied()
            .iter()
            .any(|candidate| candidate.kind() == "task")
    );
    assert!(
        store
            .task_dir(PROJECT_ID, task)
            .unwrap()
            .join("session.json")
            .exists()
    );
}

#[test]
fn transfer_gc_previews_and_removes_only_an_unreferenced_empty_transfer_repo() {
    let temp = tempfile::tempdir().unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"base\n");
    source.commit_all("base");
    let transfer =
        TransferRepo::open_or_create(&temp.path().join("cache"), &source.root().join(".git"))
            .unwrap();
    let repo_path = transfer.path().to_path_buf();
    let now = u64::MAX / 2;
    let preview = TransferGc::new(&temp.path().join("cache"), &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    assert!(preview.candidates().iter().any(|candidate| {
        candidate.kind() == "transfer_repo" && candidate.identifier() == transfer.repo_id()
    }));
    TransferGc::new(&temp.path().join("cache"), &SystemProcessRunner)
        .apply_at(now)
        .unwrap();
    assert!(!repo_path.exists());
}

#[test]
fn transfer_gc_keeps_a_repo_with_a_live_base_ref() {
    let temp = tempfile::tempdir().unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"base\n");
    source.commit_all("base");
    let oid = git(source.root(), &["rev-parse", "HEAD"]);
    let transfer =
        TransferRepo::open_or_create(&temp.path().join("cache"), &source.root().join(".git"))
            .unwrap();
    let repo_path = transfer.path().to_path_buf();
    let task = task_id(1);
    set_ref(&repo_path, &format!("refs/mac-worker/bases/{task}"), &oid);

    let report = TransferGc::new(&temp.path().join("cache"), &SystemProcessRunner)
        .apply_at(u64::MAX / 2)
        .unwrap();
    assert!(report.candidates().is_empty());
    assert!(repo_path.exists());
}

#[test]
fn transfer_gc_can_collect_stale_result_refs_without_collecting_base_refs() {
    let temp = tempfile::tempdir().unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"base\n");
    source.commit_all("base");
    let oid = git(source.root(), &["rev-parse", "HEAD"]);
    let transfer =
        TransferRepo::open_or_create(&temp.path().join("cache"), &source.root().join(".git"))
            .unwrap();
    let repo_path = transfer.path().to_path_buf();
    let task = task_id(1);
    set_ref(&repo_path, &format!("refs/mac-worker/results/{task}"), &oid);

    let report = TransferGc::new(&temp.path().join("cache"), &SystemProcessRunner)
        .preview_at(u64::MAX / 2)
        .unwrap();
    assert!(report.candidates().iter().any(|candidate| {
        candidate.kind() == "transfer_repo" && candidate.identifier() == transfer.repo_id()
    }));
    assert!(repo_path.exists());
}
