//! Deleting an OpenCode session when a task is discarded.
//!
//! The delete runs on the worker, outside any turn, in the account's login
//! shell. OpenCode 2 sends `session delete` to its background service unless
//! the command carries `--standalone`, and OpenCode 1 rejects that flag, so
//! the worker asks the installed OpenCode for its version first.
//!
//! The OpenCode commands are scripted. No OpenCode runs.

#[allow(dead_code)]
use crate::support;

use std::{
    fs, os::unix::process::ExitStatusExt, path::Path, process::ExitStatus, sync::Mutex,
    time::Duration,
};

use mac_worker::test_support::{
    agents::agent::{AgentKind, PermissionPolicy},
    core::{error::WorkerError, protocol::MemoryPressure},
    host::{
        job::{
            ClientId, CommandSpec, ExecutionScope, JobId, LeaseAcquireRequest,
            LeaseAcquireResponse, LeaseRecord, LeaseToken, RequestFingerprintMaterial,
        },
        lease::{AdmissionFacts, LeaseService},
        process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
        store::HostStore,
    },
    task::{
        model::{
            BaseOid, ClosePolicy, GitIdentity, PublishMode, TaskId, TaskLimits, TaskMeta,
            TaskMetaInput, TaskSource, TaskState, TaskStatus,
        },
        store::{
            SessionBinding, TaskCloseRequest, TaskCloseResponse, TaskPrepareRequest, TaskStore,
        },
    },
};
use support::GitRepo;
use tempfile::TempDir;
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const SESSION: &str = "ses_f0ea2018effeLWl2cU5BkMIijI";

const VERSION: &str = "exec 'opencode' '--version'";
const DELETE_V1: &str = "exec 'opencode' 'session' 'delete' 'ses_f0ea2018effeLWl2cU5BkMIijI'";
const DELETE_V2: &str =
    "exec 'opencode' 'session' 'delete' 'ses_f0ea2018effeLWl2cU5BkMIijI' '--standalone'";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Generation {
    V1,
    V2,
}

/// The worker account's login shell with a scripted OpenCode. Git runs for
/// real; no other login-shell command is expected during a close.
struct OpencodeHost {
    generation: Generation,
    /// What `--version` prints, with its exit status.
    version: (&'static [u8], i32),
    shells: Mutex<Vec<ProcessRequest>>,
}

impl OpencodeHost {
    fn v1() -> Self {
        Self::new(Generation::V1, (b"1.18.32\n", 0))
    }

    fn v2() -> Self {
        Self::new(Generation::V2, (b"opencode v2.0.18\n", 0))
    }

    fn new(generation: Generation, version: (&'static [u8], i32)) -> Self {
        Self {
            generation,
            version,
            shells: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<ProcessRequest> {
        self.shells.lock().unwrap().clone()
    }

    /// Every login-shell command, in order.
    fn shells(&self) -> Vec<String> {
        self.requests()
            .iter()
            .map(|request| request.args[1].to_str().unwrap().to_owned())
            .collect()
    }
}

impl ProcessRunner for OpencodeHost {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program != "/bin/zsh" {
            return SystemProcessRunner.run(request);
        }
        assert_eq!(request.args[0], "-lc");
        self.shells.lock().unwrap().push(request.clone());
        let shell = request.args[1].to_str().unwrap();
        Ok(match (shell, self.generation) {
            (VERSION, _) => output(self.version.1, self.version.0, b""),
            (DELETE_V1, Generation::V1) | (DELETE_V2, Generation::V2) => {
                output(0, format!("Session {SESSION} deleted\n").as_bytes(), b"")
            }
            // v1 rejects the flag it does not know.
            (DELETE_V2, Generation::V1) => output(
                1,
                b"",
                b"opencode session delete <sessionID>\n\ndelete a session\n",
            ),
            // This form reaches the v2 background service. The tests assert
            // that it is never sent.
            (DELETE_V1, Generation::V2) => output(0, b"", b""),
            (other, _) => panic!("unexpected login shell command: {other}"),
        })
    }
}

fn output(status: i32, stdout: &[u8], stderr: &[u8]) -> ProcessResult {
    ProcessResult {
        status: ExitStatus::from_raw(status << 8),
        stdout: stdout.to_vec(),
        stderr: stderr.to_vec(),
    }
}

fn task_id(value: u128) -> TaskId {
    TaskId::new(Uuid::from_u128(value))
}

fn job_id(value: u128) -> JobId {
    JobId::new(Uuid::from_u128(value))
}

fn task_meta(task: TaskId, base_oid: BaseOid) -> TaskMeta {
    TaskMeta::new(TaskMetaInput {
        session_import: None,
        task_id: task,
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
        agent: AgentKind::Opencode,
        model: None,
        effort: None,
        policy: PermissionPolicy::Unattended,
        source: TaskSource::Local {
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
    let output = std::process::Command::new("/usr/bin/git")
        .current_dir(path)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .args(args)
        .output()
        .expect("run git fixture command");
    assert!(output.status.success(), "git {args:?} failed");
    String::from_utf8(output.stdout).unwrap().trim().into()
}

/// A host store whose mirror holds the base of every task in `tasks`.
fn store_with_mirror(tasks: &[TaskId]) -> (TempDir, HostStore, BaseOid) {
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let source = GitRepo::init();
    source.write("a.txt", b"base\n");
    source.commit_all("base");
    let base_oid: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    let mirror = store.mirror(PROJECT_ID).unwrap();
    let mirror_path = mirror.path().to_string_lossy().into_owned();
    for task in tasks {
        let reference = format!("HEAD:refs/mac-worker/bases/{task}");
        assert!(
            source
                .git(&["push", &mirror_path, &reference])
                .status
                .success()
        );
    }
    (temp, store, base_oid)
}

fn lease_request(job: JobId, token: u128, task: TaskId) -> LeaseAcquireRequest {
    LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            job,
            ClientId::new(Uuid::from_u128(20)),
            LeaseToken::new(Uuid::from_u128(token)),
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

fn acquire(store: &HostStore, request: &LeaseAcquireRequest) -> LeaseRecord {
    let facts = AdmissionFacts {
        free_disk_bytes: 100 * 1024 * 1024 * 1024,
        total_disk_bytes: 200 * 1024 * 1024 * 1024,
        memory_pressure: MemoryPressure::Normal,
        swap_used_bytes: Some(0),
    };
    match LeaseService::new(store)
        .acquire(request, &facts, 100)
        .unwrap()
    {
        LeaseAcquireResponse::Acquired { lease } => lease,
        other => panic!("expected acquired lease, got {other:?}"),
    }
}

fn prepare(store: &HostStore, task: TaskId, job: JobId, base_oid: BaseOid) {
    let admission = store.admission_lock(job).unwrap();
    let transfer = store.transfer_lock_after(&admission, job).unwrap();
    TaskStore::new(store, &SystemProcessRunner)
        .prepare(
            &TaskPrepareRequest::new(task_meta(task, base_oid), job, "mini-1"),
            &transfer,
        )
        .unwrap();
}

/// Leaves `task` open with no live lease, as it is between turns.
fn make_idle(store: &HostStore, task: TaskId, request: &LeaseAcquireRequest, lease: &LeaseRecord) {
    let status = store.task_status(PROJECT_ID, task).unwrap();
    let open = TaskStatus::new(
        TaskState::Open,
        status.last_outcome().cloned(),
        status.worker().map(str::to_owned),
        status.session_present(),
        status.head_oid().cloned(),
        status.summary().map(str::to_owned),
        status.questions().to_vec(),
        status.files_changed().to_vec(),
        status.diff_stat().map(str::to_owned),
        status.turns().to_vec(),
        1,
    )
    .unwrap();
    fs::write(
        store
            .task_dir(PROJECT_ID, task)
            .unwrap()
            .join("status.json"),
        serde_json::to_vec(&open).unwrap(),
    )
    .unwrap();
    store.record_abandoned(request, 200).unwrap();
    let receipt = store.cleanup_job_owned(lease).unwrap();
    LeaseService::new(store)
        .release_after_cleanup(lease, &receipt)
        .unwrap();
}

/// An idle task bound to `binding`, ready to be discarded.
fn task_with_session(binding: SessionBinding) -> (TempDir, HostStore) {
    let task = task_id(1);
    let (temp, store, base_oid) = store_with_mirror(&[task]);
    let request = lease_request(job_id(10), 30, task);
    let lease = acquire(&store, &request);
    prepare(&store, task, job_id(10), base_oid);
    let tasks = TaskStore::new(&store, &SystemProcessRunner);
    tasks.publish_branch_into_mirror(PROJECT_ID, task).unwrap();
    tasks.bind_session(PROJECT_ID, task, binding).unwrap();
    make_idle(&store, task, &request, &lease);
    (temp, store)
}

fn opencode_session() -> SessionBinding {
    SessionBinding::new(AgentKind::Opencode, SESSION, 200).unwrap()
}

fn discard(store: &HostStore, host: &OpencodeHost) -> TaskCloseResponse {
    let response = TaskStore::new(store, host)
        .close(&TaskCloseRequest::new(PROJECT_ID, task_id(1), true))
        .unwrap();
    assert_eq!(response.status().state(), TaskState::Abandoned);
    response
}

#[test]
fn discard_on_a_v2_host_deletes_the_session_standalone() {
    let (_temp, store) = task_with_session(opencode_session());
    let host = OpencodeHost::v2();
    let response = discard(&store, &host);
    assert_eq!(host.shells(), [VERSION, DELETE_V2]);
    assert!(response.warnings().is_empty(), "{:?}", response.warnings());
    // Both commands are bounded login-shell requests without stdin.
    for request in host.requests() {
        assert_eq!(request.program, "/bin/zsh");
        assert!(request.isolate_parent_environment);
        assert!(request.stdin.is_none());
        assert_eq!(request.policy.deadline, Duration::from_secs(15));
    }
}

#[test]
fn discard_on_a_v1_host_keeps_the_plain_delete() {
    let (_temp, store) = task_with_session(opencode_session());
    let host = OpencodeHost::v1();
    let response = discard(&store, &host);
    assert_eq!(host.shells(), [VERSION, DELETE_V1]);
    assert!(response.warnings().is_empty(), "{:?}", response.warnings());
}

/// A `--version` that answers without a version. The unit tests of
/// `installed_adapter` cover the other ways a version can be unreadable.
const UNREADABLE: (&[u8], i32) = (b"development build\n", 0);

#[test]
fn an_unreadable_version_never_sends_the_delete_without_standalone() {
    // The unknown generation may be v2, so the delete is standalone. On v2
    // it works and nothing reached the service.
    let (_temp, store) = task_with_session(opencode_session());
    let host = OpencodeHost::new(Generation::V2, UNREADABLE);
    let response = discard(&store, &host);
    assert_eq!(host.shells(), [VERSION, DELETE_V2]);
    assert!(response.warnings().is_empty(), "{:?}", response.warnings());
}

#[test]
fn an_unreadable_version_on_v1_fails_the_delete_but_not_the_discard() {
    // v1 rejects the flag: the task is still discarded and the response
    // carries the bounded warning, without the session id.
    let (_temp, store) = task_with_session(opencode_session());
    let host = OpencodeHost::new(Generation::V1, UNREADABLE);
    let response = discard(&store, &host);
    assert_eq!(host.shells(), [VERSION, DELETE_V2]);
    assert_eq!(
        response.warnings(),
        ["native agent session deletion failed"]
    );
    assert!(!serde_json::to_string(&response).unwrap().contains(SESSION));
}

#[test]
fn a_session_shared_with_another_task_is_kept_without_asking_for_the_version() {
    let (first, second) = (task_id(1), task_id(2));
    let (_temp, store, base_oid) = store_with_mirror(&[first, second]);
    LeaseService::new(&store).set_slot_count(2).unwrap();
    let request = lease_request(job_id(10), 30, first);
    let lease = acquire(&store, &request);
    prepare(&store, first, job_id(10), base_oid.clone());
    let other = lease_request(job_id(11), 31, second);
    acquire(&store, &other);
    prepare(&store, second, job_id(11), base_oid);
    let tasks = TaskStore::new(&store, &SystemProcessRunner);
    tasks.publish_branch_into_mirror(PROJECT_ID, first).unwrap();
    tasks
        .bind_session(PROJECT_ID, first, opencode_session())
        .unwrap();
    tasks
        .bind_session(PROJECT_ID, second, opencode_session())
        .unwrap();
    make_idle(&store, first, &request, &lease);

    let host = OpencodeHost::v2();
    let response = discard(&store, &host);
    assert_eq!(host.shells(), Vec::<String>::new());
    assert!(response.warnings().is_empty());
}

#[test]
fn an_agent_with_one_dialect_is_deleted_without_a_version_probe() {
    let (_temp, store) =
        task_with_session(SessionBinding::new(AgentKind::Codex, "session-1", 200).unwrap());
    struct CodexHost(Mutex<Vec<String>>);
    impl ProcessRunner for CodexHost {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            if request.program != "/bin/zsh" {
                return SystemProcessRunner.run(request);
            }
            self.0
                .lock()
                .unwrap()
                .push(request.args[1].to_str().unwrap().to_owned());
            Ok(output(0, b"", b""))
        }
    }
    let host = CodexHost(Mutex::new(Vec::new()));
    let response = TaskStore::new(&store, &host)
        .close(&TaskCloseRequest::new(PROJECT_ID, task_id(1), true))
        .unwrap();
    assert_eq!(response.status().state(), TaskState::Abandoned);
    assert_eq!(
        *host.0.lock().unwrap(),
        ["exec 'codex' 'delete' '--force' 'session-1'"]
    );
}
