#[allow(dead_code)]
mod support;

use std::os::unix::process::ExitStatusExt;
use std::{
    ffi::OsStr,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy},
    error::WorkerError,
    host_store::{AdmissionGuard, HostStore, TransferGuard},
    job::{
        ClientId, CommandSpec, ExecutionScope, JobId, LeaseAcquireRequest, LeaseAcquireResponse,
        LeaseRecord, LeaseToken, RequestFingerprintMaterial,
    },
    lease::{AdmissionFacts, LeaseService},
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    protocol::MemoryPressure,
    task::{
        BaseOid, ClosePolicy, GitIdentity, PublishMode, TaskId, TaskLimits, TaskMeta,
        TaskMetaInput, TaskState, TaskStatus,
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
    task_id_for(1)
}

fn task_id_for(value: u128) -> TaskId {
    TaskId::new(Uuid::from_u128(value))
}

fn job_id() -> JobId {
    job_id_for(10)
}

fn job_id_for(value: u128) -> JobId {
    JobId::new(Uuid::from_u128(value))
}

fn client_id() -> ClientId {
    ClientId::new(Uuid::from_u128(20))
}

fn lease_token() -> LeaseToken {
    LeaseToken::new(Uuid::from_u128(30))
}

fn task_meta(base_oid: BaseOid) -> TaskMeta {
    task_meta_for(task_id(), base_oid)
}

fn task_meta_for(task_id: TaskId, base_oid: BaseOid) -> TaskMeta {
    TaskMeta::new(TaskMetaInput {
        task_id,
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        policy: PermissionPolicy::Workspace,
        source: mac_worker::task::TaskSource::Local {
            wip: false,
            push_target: None,
        },
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
    transfer_guard_for(store, job_id())
}

fn transfer_guard_for(store: &HostStore, job: JobId) -> (AdmissionGuard, TransferGuard) {
    let admission = store.admission_lock(job).unwrap();
    let transfer = store.transfer_lock_after(&admission, job).unwrap();
    (admission, transfer)
}

fn prepare_request(base_oid: BaseOid) -> TaskPrepareRequest {
    TaskPrepareRequest::new(task_meta(base_oid), job_id(), "mini-1")
}

fn prepare_task(store: &HostStore, base_oid: BaseOid) {
    prepare_task_for(store, task_id(), job_id(), base_oid, &SystemProcessRunner);
}

fn prepare_task_for(
    store: &HostStore,
    task: TaskId,
    job: JobId,
    base_oid: BaseOid,
    runner: &dyn ProcessRunner,
) {
    let (_admission, transfer) = transfer_guard_for(store, job);
    TaskStore::new(store, runner)
        .prepare(
            &TaskPrepareRequest::new(task_meta_for(task, base_oid), job, "mini-1"),
            &transfer,
        )
        .unwrap();
}

fn rewrite_status(store: &HostStore, task: TaskId, state: TaskState, updated_at_millis: u64) {
    let status = store.task_status(PROJECT_ID, task).unwrap();
    let replacement = TaskStatus::new(
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

#[derive(Clone)]
struct NativeDeleteRunner {
    requests: Arc<Mutex<Vec<ProcessRequest>>>,
    workspace: PathBuf,
    delete_success: bool,
    workspace_removed_at_delete: Arc<Mutex<Option<bool>>>,
}

impl NativeDeleteRunner {
    fn new(workspace: PathBuf, delete_success: bool) -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            workspace,
            delete_success,
            workspace_removed_at_delete: Arc::new(Mutex::new(None)),
        }
    }

    fn delete_requests(&self) -> Vec<ProcessRequest> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| {
                request.program.as_os_str() == OsStr::new("/bin/zsh")
                    && request
                        .args
                        .last()
                        .and_then(|argument| argument.to_str())
                        .is_some_and(|shell| shell.contains("'codex' 'delete'"))
            })
            .cloned()
            .collect()
    }

    fn workspace_removed_at_delete(&self) -> bool {
        self.workspace_removed_at_delete
            .lock()
            .unwrap()
            .expect("native delete was invoked")
    }
}

impl ProcessRunner for NativeDeleteRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.requests.lock().unwrap().push(request.clone());
        let is_delete = request.program.as_os_str() == OsStr::new("/bin/zsh")
            && request
                .args
                .last()
                .and_then(|argument| argument.to_str())
                .is_some_and(|shell| shell.contains("'codex' 'delete'"));
        if is_delete {
            *self.workspace_removed_at_delete.lock().unwrap() = Some(!self.workspace.exists());
            return Ok(ProcessResult {
                status: ExitStatus::from_raw(if self.delete_success { 0 } else { 1 << 8 }),
                stdout: Vec::new(),
                stderr: b"delete failed at /private/worker/secret-session-store".to_vec(),
            });
        }
        SystemProcessRunner.run(request)
    }
}

#[derive(Clone)]
struct BlockingDeleteRunner {
    inner: NativeDeleteRunner,
    delete_started: mpsc::Sender<()>,
    release_delete: Arc<Mutex<mpsc::Receiver<()>>>,
}

impl BlockingDeleteRunner {
    fn new(inner: NativeDeleteRunner) -> (Self, mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (delete_started, started_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        (
            Self {
                inner,
                delete_started,
                release_delete: Arc::new(Mutex::new(release_receiver)),
            },
            started_receiver,
            release_sender,
        )
    }
}

impl ProcessRunner for BlockingDeleteRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let is_delete = request.program.as_os_str() == OsStr::new("/bin/zsh")
            && request
                .args
                .last()
                .and_then(|argument| argument.to_str())
                .is_some_and(|shell| shell.contains("'codex' 'delete'"));
        if is_delete {
            self.delete_started.send(()).unwrap();
            self.release_delete.lock().unwrap().recv().unwrap();
        }
        self.inner.run(request)
    }
}

#[test]
fn origin_prepare_fetches_the_exact_base_before_workspace_creation() {
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    store.mirror(PROJECT_ID).unwrap();
    acquire_lease(&store);
    let base_oid: BaseOid = "0123456789012345678901234567890123456789".parse().unwrap();
    let meta = TaskMeta::new(TaskMetaInput {
        task_id: task_id(),
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        policy: PermissionPolicy::Workspace,
        source: mac_worker::task::TaskSource::Origin {
            url: "https://example.test/repo.git".into(),
        },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid: base_oid.clone(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: "prepare from origin".into(),
        created_at_millis: 100,
    })
    .unwrap();
    let runner =
        support::recording_runner::RecordingRunner::returning(mac_worker::process::ProcessResult {
            status: std::process::ExitStatus::from_raw(1),
            stdout: Vec::new(),
            stderr: b"origin unavailable".to_vec(),
        });
    let request = TaskPrepareRequest::new(meta, job_id(), "mini-1");
    let error = {
        let (_admission, transfer) = transfer_guard(&store);
        TaskStore::new(&store, &runner)
            .prepare(&request, &transfer)
            .unwrap_err()
    };
    assert_eq!(error.public_code(), "BASE_UNAVAILABLE");
    assert!(
        store
            .task_workspace_if_present(PROJECT_ID, task_id())
            .unwrap()
            .is_none()
    );
    let request = runner.single_request();
    let args = request
        .args
        .iter()
        .map(|argument| argument.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(args.iter().any(|arg| arg == "fetch"));
    assert!(
        args.iter()
            .any(|arg| arg == "https://example.test/repo.git")
    );
    assert!(args.iter().any(|arg| arg == base_oid.as_str()));
}

#[test]
fn origin_prepare_pins_the_base_ref_and_rejects_a_conflicting_oid() {
    let (_temp, store, base_oid) = store_with_mirror();
    acquire_lease(&store);
    struct FetchStub;
    impl ProcessRunner for FetchStub {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            if request.args.iter().any(|arg| arg == "fetch")
                && request
                    .args
                    .iter()
                    .any(|arg| arg == "https://example.test/repo.git")
            {
                return Ok(ProcessResult {
                    status: ExitStatus::from_raw(0),
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                });
            }
            SystemProcessRunner.run(request)
        }
    }
    let meta = TaskMeta::new(TaskMetaInput {
        task_id: task_id(),
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        policy: PermissionPolicy::Workspace,
        source: mac_worker::task::TaskSource::Origin {
            url: "https://example.test/repo.git".into(),
        },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid: base_oid.clone(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: "prepare from origin".into(),
        created_at_millis: 100,
    })
    .unwrap();
    let request = TaskPrepareRequest::new(meta.clone(), job_id(), "mini-1");
    {
        let (_admission, transfer) = transfer_guard(&store);
        TaskStore::new(&store, &FetchStub)
            .prepare(&request, &transfer)
            .unwrap();
    }
    let mirror = store.mirror(PROJECT_ID).unwrap();
    let pin = format!("refs/mac-worker/bases/{}", task_id());
    assert!(git_ref_exists(&mirror, &pin));
    assert_eq!(
        git(mirror.path(), &["rev-parse", "--verify", "--quiet", &pin]),
        base_oid.as_str()
    );
    {
        let (_admission, transfer) = transfer_guard(&store);
        TaskStore::new(&store, &FetchStub)
            .prepare(&request, &transfer)
            .unwrap();
    }
    let other = TaskMeta::new(TaskMetaInput {
        task_id: task_id(),
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        policy: PermissionPolicy::Workspace,
        source: mac_worker::task::TaskSource::Origin {
            url: "https://example.test/repo.git".into(),
        },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".parse().unwrap(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: "prepare from origin".into(),
        created_at_millis: 100,
    })
    .unwrap();
    let conflict_request = TaskPrepareRequest::new(other, job_id(), "mini-1");
    let error = {
        let (_admission, transfer) = transfer_guard(&store);
        TaskStore::new(&store, &FetchStub)
            .prepare(&conflict_request, &transfer)
            .unwrap_err()
    };
    assert_eq!(error.public_code(), "BASE_REF_CONFLICT");
}

fn healthy_facts() -> AdmissionFacts {
    AdmissionFacts {
        free_disk_bytes: 100 * 1024 * 1024 * 1024,
        total_disk_bytes: 200 * 1024 * 1024 * 1024,
        memory_pressure: MemoryPressure::Normal,
        swap_used_bytes: Some(0),
    }
}

fn task_lease_request(job: JobId, token: LeaseToken, task: TaskId) -> LeaseAcquireRequest {
    LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            job,
            client_id(),
            token,
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
    )
    .with_execution_scope(ExecutionScope::task(task))
}

fn acquire_task_lease_request(store: &HostStore, request: &LeaseAcquireRequest) -> LeaseRecord {
    match LeaseService::new(store)
        .acquire(request, &healthy_facts(), 100)
        .unwrap()
    {
        LeaseAcquireResponse::Acquired { lease } => lease,
        other => panic!("expected acquired lease, got {other:?}"),
    }
}

fn acquire_task_lease(store: &HostStore) -> (LeaseAcquireRequest, LeaseRecord) {
    let request = task_lease_request(job_id(), lease_token(), task_id());
    let lease = acquire_task_lease_request(store, &request);
    (request, lease)
}

fn acquire_lease(store: &HostStore) {
    let _ = acquire_task_lease(store);
}

fn retire_lease(store: &HostStore, request: &LeaseAcquireRequest, lease: &LeaseRecord) {
    store.record_abandoned(request, 200).unwrap();
    let receipt = store.cleanup_job_owned(lease).unwrap();
    LeaseService::new(store)
        .release_after_cleanup(lease, &receipt)
        .unwrap();
}

fn idle_task_for_close(
    store: &HostStore,
    request: &LeaseAcquireRequest,
    lease: &LeaseRecord,
    task: TaskId,
) {
    rewrite_status(store, task, TaskState::Open, 1);
    retire_lease(store, request, lease);
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
    let (request, lease) = acquire_task_lease(&store);
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
    let missing_lease = TaskStore::new(&store, &SystemProcessRunner)
        .prepare_resume(
            PROJECT_ID,
            task_id(),
            JobId::new(Uuid::from_u128(11)),
            2,
            "mini-1",
            &base_oid,
        )
        .unwrap_err();
    assert_eq!(missing_lease.public_code(), "TASK_LEASE_MISMATCH");
    let wrong_worker = TaskStore::new(&store, &SystemProcessRunner)
        .prepare_resume(PROJECT_ID, task_id(), job_id(), 1, "mini-2", &base_oid)
        .unwrap_err();
    assert_eq!(wrong_worker.public_code(), "TASK_LEASE_MISMATCH");

    rewrite_status(&store, task_id(), TaskState::Open, 2);
    retire_lease(&store, &request, &lease);
    let next = task_lease_request(
        JobId::new(Uuid::from_u128(11)),
        LeaseToken::new(Uuid::from_u128(31)),
        task_id(),
    );
    acquire_task_lease_request(&store, &next);
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
    let (request, lease) = acquire_task_lease(&store);
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
    idle_task_for_close(&store, &request, &lease, task_id());

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
fn discard_keeps_task_intact_when_the_mirror_is_missing() {
    let (_temp, store, base_oid) = store_with_mirror();
    let (request, lease) = acquire_task_lease(&store);
    prepare_task(&store, base_oid);
    TaskStore::new(&store, &SystemProcessRunner)
        .publish_branch_into_mirror(PROJECT_ID, task_id())
        .unwrap();
    let workspace = store.task_workspace(PROJECT_ID, task_id()).unwrap();
    let mirror_path = store.mirror(PROJECT_ID).unwrap().path().to_path_buf();
    idle_task_for_close(&store, &request, &lease, task_id());
    fs::remove_dir_all(mirror_path).unwrap();

    let error = TaskStore::new(&store, &SystemProcessRunner)
        .close(&TaskCloseRequest::new(PROJECT_ID, task_id(), true))
        .unwrap_err();

    assert_eq!(error.public_code(), "BASE_UNAVAILABLE");
    assert!(workspace.exists());
    assert_eq!(
        store.task_status(PROJECT_ID, task_id()).unwrap().state(),
        TaskState::Open
    );
}

#[test]
fn explicit_close_refreshes_the_task_retention_timestamp() {
    let (_temp, store, base_oid) = store_with_mirror();
    let (request, lease) = acquire_task_lease(&store);
    prepare_task(&store, base_oid);
    TaskStore::new(&store, &SystemProcessRunner)
        .publish_branch_into_mirror(PROJECT_ID, task_id())
        .unwrap();
    rewrite_status(&store, task_id(), TaskState::Open, 1);
    retire_lease(&store, &request, &lease);

    TaskStore::new(&store, &SystemProcessRunner)
        .close(&TaskCloseRequest::new(PROJECT_ID, task_id(), false))
        .unwrap();

    let status = store.task_status(PROJECT_ID, task_id()).unwrap();
    assert_eq!(status.state(), TaskState::Closed);
    assert!(status.updated_at_millis() > 1);
}

#[test]
fn discard_deletes_codex_session_after_workspace_removal_with_bounded_argv() {
    let (_temp, store, base_oid) = store_with_mirror();
    let (request, lease) = acquire_task_lease(&store);
    prepare_task(&store, base_oid);
    let workspace = store.task_workspace(PROJECT_ID, task_id()).unwrap();
    TaskStore::new(&store, &SystemProcessRunner)
        .publish_branch_into_mirror(PROJECT_ID, task_id())
        .unwrap();
    TaskStore::new(&store, &SystemProcessRunner)
        .bind_session(
            PROJECT_ID,
            task_id(),
            SessionBinding::new(AgentKind::Codex, "session-1", 200).unwrap(),
        )
        .unwrap();
    idle_task_for_close(&store, &request, &lease, task_id());

    let runner = NativeDeleteRunner::new(workspace.clone(), true);
    let response = TaskStore::new(&store, &runner)
        .close(&TaskCloseRequest::new(PROJECT_ID, task_id(), true))
        .unwrap();

    assert_eq!(response.status().state(), TaskState::Abandoned);
    assert!(runner.workspace_removed_at_delete());
    let requests = runner.delete_requests();
    assert_eq!(requests.len(), 1);
    let shell = requests[0].args.last().unwrap().to_string_lossy();
    assert_eq!(shell, "exec 'codex' 'delete' '--force' 'session-1'");
    assert_eq!(requests[0].policy.deadline, Duration::from_secs(15));
    assert!(!shell.contains("prompt.md"));
    assert!(!shell.contains(workspace.to_string_lossy().as_ref()));
}

#[test]
fn discard_records_a_bounded_warning_when_native_session_deletion_fails() {
    let (_temp, store, base_oid) = store_with_mirror();
    let (request, lease) = acquire_task_lease(&store);
    prepare_task(&store, base_oid);
    TaskStore::new(&store, &SystemProcessRunner)
        .publish_branch_into_mirror(PROJECT_ID, task_id())
        .unwrap();
    TaskStore::new(&store, &SystemProcessRunner)
        .bind_session(
            PROJECT_ID,
            task_id(),
            SessionBinding::new(AgentKind::Codex, "session-1", 200).unwrap(),
        )
        .unwrap();
    idle_task_for_close(&store, &request, &lease, task_id());

    let workspace = store.task_workspace(PROJECT_ID, task_id()).unwrap();
    let runner = NativeDeleteRunner::new(workspace, false);
    let response = TaskStore::new(&store, &runner)
        .close(&TaskCloseRequest::new(PROJECT_ID, task_id(), true))
        .unwrap();

    assert_eq!(response.status().state(), TaskState::Abandoned);
    let encoded = serde_json::to_string(&response).unwrap();
    assert!(encoded.contains("\"warnings\""));
    assert!(!encoded.contains("session-1"));
    assert!(!encoded.contains("secret-session-store"));
}

#[test]
fn discard_skips_native_deletion_when_another_task_references_the_session() {
    let (_temp, store, base_oid) = store_with_mirror();
    let (request, lease) = acquire_task_lease(&store);
    prepare_task(&store, base_oid.clone());
    let task_two = task_id_for(2);
    let job_two = job_id_for(11);
    LeaseService::new(&store).set_slot_count(2).unwrap();
    let two = task_lease_request(job_two, LeaseToken::new(Uuid::from_u128(31)), task_two);
    acquire_task_lease_request(&store, &two);
    prepare_task_for(&store, task_two, job_two, base_oid, &SystemProcessRunner);

    TaskStore::new(&store, &SystemProcessRunner)
        .publish_branch_into_mirror(PROJECT_ID, task_id())
        .unwrap();
    let shared = SessionBinding::new(AgentKind::Codex, "shared-session", 200).unwrap();
    assert!(
        mac_worker::agent::adapter_for(AgentKind::Codex)
            .delete_session(shared.session_ref())
            .is_some()
    );
    TaskStore::new(&store, &SystemProcessRunner)
        .bind_session(PROJECT_ID, task_id(), shared.clone())
        .unwrap();
    TaskStore::new(&store, &SystemProcessRunner)
        .bind_session(PROJECT_ID, task_two, shared)
        .unwrap();
    idle_task_for_close(&store, &request, &lease, task_id());

    let workspace = store.task_workspace(PROJECT_ID, task_id()).unwrap();
    let runner = NativeDeleteRunner::new(workspace, true);
    TaskStore::new(&store, &runner)
        .close(&TaskCloseRequest::new(PROJECT_ID, task_id(), true))
        .unwrap();

    assert!(runner.delete_requests().is_empty());
}

#[test]
fn discard_serializes_native_session_delete_with_a_concurrent_session_binding() {
    let (_temp, store, base_oid) = store_with_mirror();
    let (request, lease) = acquire_task_lease(&store);
    prepare_task(&store, base_oid.clone());
    TaskStore::new(&store, &SystemProcessRunner)
        .publish_branch_into_mirror(PROJECT_ID, task_id())
        .unwrap();
    let shared = SessionBinding::new(AgentKind::Codex, "shared-session", 200).unwrap();
    TaskStore::new(&store, &SystemProcessRunner)
        .bind_session(PROJECT_ID, task_id(), shared.clone())
        .unwrap();

    let task_two = task_id_for(2);
    let job_two = job_id_for(11);
    LeaseService::new(&store).set_slot_count(2).unwrap();
    let two = task_lease_request(job_two, LeaseToken::new(Uuid::from_u128(31)), task_two);
    acquire_task_lease_request(&store, &two);
    prepare_task_for(&store, task_two, job_two, base_oid, &SystemProcessRunner);
    idle_task_for_close(&store, &request, &lease, task_id());

    let workspace = store.task_workspace(PROJECT_ID, task_id()).unwrap();
    let inner = NativeDeleteRunner::new(workspace, true);
    let (runner, delete_started, release_delete) = BlockingDeleteRunner::new(inner);
    let close_store = store.clone();
    let close_runner = runner.clone();
    let close = std::thread::spawn(move || {
        TaskStore::new(&close_store, &close_runner).close(&TaskCloseRequest::new(
            PROJECT_ID,
            task_id(),
            true,
        ))
    });
    delete_started
        .recv_timeout(Duration::from_secs(1))
        .expect("native deletion should start");

    let (bind_started, bind_started_receiver) = mpsc::channel();
    let (bind_result, bind_done) = mpsc::channel();
    let bind_store = store.clone();
    let bind = std::thread::spawn(move || {
        bind_started.send(()).unwrap();
        let result = TaskStore::new(&bind_store, &SystemProcessRunner)
            .bind_session(PROJECT_ID, task_two, shared);
        bind_result.send(result).unwrap();
    });
    bind_started_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("session binding should start while deletion is blocked");
    let bind_completed_during_delete = bind_done.recv_timeout(Duration::from_millis(250)).is_ok();

    release_delete.send(()).unwrap();
    close.join().unwrap().unwrap();
    bind.join().unwrap();
    bind_done
        .recv_timeout(Duration::from_secs(1))
        .expect("session binding should finish after discard")
        .unwrap();
    assert!(
        !bind_completed_during_delete,
        "session binding must wait while discard checks and deletes the session"
    );
}

#[test]
fn discard_rejects_a_late_session_binding_on_the_terminal_task() {
    let (_temp, store, base_oid) = store_with_mirror();
    let (request, lease) = acquire_task_lease(&store);
    prepare_task(&store, base_oid);
    TaskStore::new(&store, &SystemProcessRunner)
        .publish_branch_into_mirror(PROJECT_ID, task_id())
        .unwrap();
    idle_task_for_close(&store, &request, &lease, task_id());
    TaskStore::new(&store, &SystemProcessRunner)
        .close(&TaskCloseRequest::new(PROJECT_ID, task_id(), true))
        .unwrap();

    let error = TaskStore::new(&store, &SystemProcessRunner)
        .bind_session(
            PROJECT_ID,
            task_id(),
            SessionBinding::new(AgentKind::Codex, "late-session", 200).unwrap(),
        )
        .unwrap_err();

    assert_eq!(error.public_code(), "TASK_CLOSED");
    assert!(
        !store
            .task_dir(PROJECT_ID, task_id())
            .unwrap()
            .join("session.json")
            .exists()
    );
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

    let error = SessionBinding::new(AgentKind::Codex, "--help", 202).unwrap_err();
    assert_eq!(error.public_code(), "TASK_SESSION_INVALID");
    assert!(
        mac_worker::agent::adapter_for(AgentKind::Codex)
            .delete_session("--help")
            .is_none()
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

fn v6_host_request(value: serde_json::Value) -> serde_json::Value {
    let mut value = value;
    value["protocol_version"] = serde_json::json!(6);
    value
}

#[test]
fn v6_task_status_request_is_refused_before_store_writes() {
    let temp = tempfile::tempdir().unwrap();
    let host = temp.path().join("host");
    let store = HostStore::open(&host).unwrap();
    let marker = host
        .join("tasks")
        .join(PROJECT_ID)
        .join(task_id().to_string());
    fs::create_dir_all(&marker).unwrap();
    let sentinel = marker.join("do-not-touch");
    fs::write(&sentinel, b"persisted-task").unwrap();
    let request: TaskStatusRequest = serde_json::from_value(v6_host_request(
        serde_json::to_value(TaskStatusRequest::new(PROJECT_ID, task_id())).unwrap(),
    ))
    .unwrap();
    let error = TaskStore::new(&store, &SystemProcessRunner)
        .status(&request)
        .unwrap_err();
    match error {
        WorkerError::Protocol(message) => {
            assert!(
                message.contains("INCOMPATIBLE_PROTOCOL"),
                "unexpected protocol error: {message}"
            )
        }
        other => panic!("expected protocol mismatch, got {other:?}"),
    }
    assert_eq!(fs::read(&sentinel).unwrap(), b"persisted-task");
}

#[test]
fn v6_task_close_request_is_refused_before_store_writes() {
    let temp = tempfile::tempdir().unwrap();
    let host = temp.path().join("host");
    let store = HostStore::open(&host).unwrap();
    let marker = host
        .join("tasks")
        .join(PROJECT_ID)
        .join(task_id().to_string());
    fs::create_dir_all(&marker).unwrap();
    let sentinel = marker.join("do-not-touch");
    fs::write(&sentinel, b"persisted-task").unwrap();
    let request: TaskCloseRequest = serde_json::from_value(v6_host_request(
        serde_json::to_value(TaskCloseRequest::new(PROJECT_ID, task_id(), false)).unwrap(),
    ))
    .unwrap();
    let error = TaskStore::new(&store, &SystemProcessRunner)
        .close(&request)
        .unwrap_err();
    match error {
        WorkerError::Protocol(message) => {
            assert!(
                message.contains("INCOMPATIBLE_PROTOCOL"),
                "unexpected protocol error: {message}"
            )
        }
        other => panic!("expected protocol mismatch, got {other:?}"),
    }
    assert_eq!(fs::read(&sentinel).unwrap(), b"persisted-task");
}

#[test]
fn prepare_rejects_job_scope_and_wrong_project_before_creating_a_task() {
    let (_temp, store, base_oid) = store_with_mirror();
    let other_project = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    let job_scope = LeaseAcquireRequest::new(
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
    LeaseService::new(&store)
        .acquire(&job_scope, &healthy_facts(), 100)
        .unwrap();
    {
        let (admission, transfer) = transfer_guard(&store);
        let error = TaskStore::new(&store, &SystemProcessRunner)
            .prepare(&prepare_request(base_oid.clone()), &transfer)
            .unwrap_err();
        assert!(
            matches!(
                error,
                WorkerError::Task {
                    code: "EXECUTION_SCOPE_CONFLICT",
                    ..
                }
            ),
            "{error:?}"
        );
        assert!(!store.task_dir(PROJECT_ID, task_id()).unwrap().exists());
        drop(transfer);
        drop(admission);
    }

    let other_job = job_id_for(11);
    let other_token = LeaseToken::new(Uuid::from_u128(31));
    let task_scope = LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            other_job,
            client_id(),
            other_token,
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
    )
    .with_execution_scope(ExecutionScope::task(task_id()));
    LeaseService::new(&store).set_slot_count(2).unwrap();
    LeaseService::new(&store)
        .acquire(&task_scope, &healthy_facts(), 100)
        .unwrap();
    let (admission, transfer) = transfer_guard_for(&store, other_job);
    let wrong_meta = TaskMeta::new(TaskMetaInput {
        task_id: task_id(),
        run_id: None,
        project_id: other_project.into(),
        worktree_id: WORKTREE_ID.into(),
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        policy: PermissionPolicy::Workspace,
        source: mac_worker::task::TaskSource::Local {
            wip: false,
            push_target: None,
        },
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
    .unwrap();
    let error = TaskStore::new(&store, &SystemProcessRunner)
        .prepare(
            &TaskPrepareRequest::new(wrong_meta, other_job, "mini-1"),
            &transfer,
        )
        .unwrap_err();
    assert!(
        matches!(
            error,
            WorkerError::Task {
                code: "EXECUTION_SCOPE_CONFLICT",
                ..
            }
        ),
        "{error:?}"
    );
    assert!(!store.task_dir(other_project, task_id()).unwrap().exists());
    drop(transfer);
    drop(admission);
}

#[test]
fn close_refuses_open_task_while_a_task_scope_lease_is_live() {
    let (_temp, store, base_oid) = store_with_mirror();
    acquire_lease(&store);
    prepare_task(&store, base_oid);
    rewrite_status(&store, task_id(), TaskState::Open, 1);
    let error = TaskStore::new(&store, &SystemProcessRunner)
        .close(&TaskCloseRequest::new(PROJECT_ID, task_id(), false))
        .unwrap_err();
    assert_eq!(error.public_code(), "TASK_BUSY");
    assert!(
        store
            .task_workspace_if_present(PROJECT_ID, task_id())
            .unwrap()
            .is_some()
    );
    assert_eq!(
        store.task_status(PROJECT_ID, task_id()).unwrap().state(),
        TaskState::Open
    );
}
