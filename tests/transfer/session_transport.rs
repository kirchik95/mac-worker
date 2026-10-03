use crate::support::{GitRepo, recording_runner::RecordingRunner};
use mac_worker::test_support::{
    core::{config::WorkerEntry, error::WorkerError},
    host::{
        job::{ClientId, CommandSpec, JobId, LeaseToken, RequestFingerprintMaterial},
        process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
        store::HostStore,
    },
    session::{
        PackageFile, PackageSource, REQUEST_SESSION_REF_PREFIX, SESSION_REF_PREFIX, SessionAgent,
        SessionPackage,
    },
    task::model::{BaseOid, TaskId},
    transfer::{
        TransferIdentity,
        git::{GitTransport, SessionRefPush},
        repo::{TransferGc, TransferRepo},
    },
};
use std::{
    ffi::OsString,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::Mutex,
};
use tempfile::TempDir;
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const REQUEST_ID: &str = "00000000000000000000000000000028";
const TOKEN: &str = "00000000000000000000000000000029";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn task_id() -> TaskId {
    TaskId::new(Uuid::from_u128(1))
}

fn identity() -> TransferIdentity {
    let job = JobId::new(Uuid::from_u128(10));
    let client = ClientId::new(Uuid::from_u128(20));
    let lease = LeaseToken::new(Uuid::from_u128(30));
    let fingerprint = RequestFingerprintMaterial::new(
        job,
        client,
        lease,
        100,
        "mini-1".into(),
        PROJECT_ID.into(),
        "c".repeat(64),
        "d".repeat(64),
        String::new(),
        30_000,
        "heavy".into(),
        CommandSpec::shell("true".into()).unwrap(),
    )
    .unwrap()
    .fingerprint();
    TransferIdentity::new(job, client, lease, fingerprint)
}

fn worker() -> WorkerEntry {
    WorkerEntry {
        name: "mini-1".into(),
        ssh: "mac1".into(),
        slots: 1,
        capabilities: Vec::new(),
        remote_binary: "~/.local/bin/worker".into(),
        herdr: false,
    }
}

fn package() -> SessionPackage {
    SessionPackage::build(
        PackageSource {
            agent: SessionAgent::Claude,
            source_session_id: "00000000-0000-4000-8000-000000000001".into(),
            source_agent_version: "2.1.288".into(),
            source_cwd_relative: ".".into(),
            scrubbed: 1,
        },
        vec![
            PackageFile {
                path: "sidecar/subagents/agent-test.meta.json".into(),
                bytes: b"{}\n".to_vec(),
            },
            PackageFile {
                path: "main.jsonl".into(),
                bytes: b"{\"sessionId\":\"@@MW_SESSION@@\",\"cwd\":\"@@MW_WORKSPACE@@\"}\n"
                    .to_vec(),
            },
            PackageFile {
                path: "sidecar/name with\ttab\nand quote\".txt".into(),
                bytes: b"synthetic sidecar\0\xff".to_vec(),
            },
        ],
    )
    .unwrap()
}

struct Fixture {
    repo: GitRepo,
    cache: TempDir,
    transfer: TransferRepo,
    base: BaseOid,
}

impl Fixture {
    fn new() -> Self {
        let repo = GitRepo::init();
        repo.write("source.txt", b"synthetic snapshot\n");
        repo.commit_all("synthetic snapshot");
        let base = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let cache = tempfile::tempdir().unwrap();
        let transfer =
            TransferRepo::open_or_create(cache.path(), &repo.root().join(".git")).unwrap();
        Self {
            repo,
            cache,
            transfer,
            base,
        }
    }
}

fn git(path: &Path, args: &[&str]) -> std::process::Output {
    let mut command = Command::new("/usr/bin/git");
    command
        .arg("--git-dir")
        .arg(path)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1");
    for name in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_PARAMETERS",
    ] {
        command.env_remove(name);
    }
    command.output().unwrap()
}

fn git_ok(path: &Path, args: &[&str]) -> Vec<u8> {
    let result = git(path, args);
    assert!(
        result.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    result.stdout
}

fn has_ref(path: &Path, name: &str) -> bool {
    git(path, &["show-ref", "--verify", name]).status.success()
}

// Retain the real push request, but route its receive-pack locally: no SSH or worker commands.
struct LocalPushRunner {
    remote: PathBuf,
    requests: Mutex<Vec<ProcessRequest>>,
}

impl LocalPushRunner {
    fn new(remote: &Path) -> Self {
        Self {
            remote: remote.to_owned(),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn single_request(&self) -> ProcessRequest {
        let requests = self.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        requests[0].clone()
    }
}

impl ProcessRunner for LocalPushRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.requests.lock().unwrap().push(request.clone());
        let mut local = request.clone();
        assert!(local.args.iter().any(|arg| arg == "push"));
        local
            .args
            .retain(|arg| !arg.to_string_lossy().starts_with("--receive-pack="));
        let destination = local
            .args
            .iter_mut()
            .find(|arg| **arg == OsString::from(format!("mac1:{PROJECT_ID}")))
            .unwrap();
        *destination = self.remote.as_os_str().to_owned();
        local
            .environment
            .retain(|(key, _)| key != "GIT_SSH_COMMAND");
        SystemProcessRunner.run(&local)
    }
}

fn push_base(
    fixture: &Fixture,
    runner: &dyn ProcessRunner,
    session: Option<&str>,
) -> Result<(), WorkerError> {
    GitTransport::new(runner)
        .push_base(
            &worker(),
            &identity(),
            PROJECT_ID,
            task_id(),
            &fixture.base,
            fixture.transfer.path(),
            session.map(|package_oid| SessionRefPush { package_oid }),
        )
        .map(|_| ())
}

fn push_controller(
    fixture: &Fixture,
    runner: &dyn ProcessRunner,
    session: Option<&str>,
) -> Result<(), WorkerError> {
    GitTransport::new(runner)
        .push_controller_source(
            "/usr/bin/true",
            "mac1",
            "~/.local/bin/worker",
            TOKEN,
            REQUEST_ID,
            identity().request_fingerprint(),
            PROJECT_ID,
            WORKTREE_ID,
            &fixture.base,
            fixture.transfer.path(),
            session.map(|package_oid| SessionRefPush { package_oid }),
        )
        .map(|_| ())
}

#[test]
fn package_write_is_deterministic_parentless_and_has_exact_tree() {
    let fixture = Fixture::new();
    let package = package();
    let runner = RecordingRunner::passthrough();
    let oid = fixture
        .transfer
        .write_session_package(&runner, task_id(), &package)
        .unwrap();
    assert_eq!(
        fixture
            .transfer
            .write_session_package(&runner, task_id(), &package)
            .unwrap(),
        oid
    );
    let second_task = TaskId::new(Uuid::from_u128(2));
    assert_eq!(
        fixture
            .transfer
            .write_session_package(&runner, second_task, &package)
            .unwrap(),
        oid
    );
    let commit = git_ok(fixture.transfer.path(), &["cat-file", "commit", &oid]);
    let commit = String::from_utf8(commit).unwrap();
    let tree = String::from_utf8(git_ok(
        fixture.transfer.path(),
        &["rev-parse", &format!("{oid}^{{tree}}")],
    ))
    .unwrap();
    assert_eq!(
        commit,
        format!(
            "tree {}\nauthor mac-worker <session@mac-worker.invalid> 0 +0000\ncommitter mac-worker <session@mac-worker.invalid> 0 +0000\n\nmac-worker session package\n",
            tree.trim()
        )
    );
    let names = git_ok(
        fixture.transfer.path(),
        &["ls-tree", "-rz", "--name-only", &oid],
    );
    let mut expected = vec!["manifest.json".to_owned()];
    expected.extend(
        package
            .files()
            .iter()
            .map(|file| format!("session/{}", file.path)),
    );
    expected.sort();
    let actual: Vec<_> = names
        .split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
        .map(|name| String::from_utf8(name.to_vec()).unwrap())
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(
        git_ok(
            fixture.transfer.path(),
            &["show", &format!("{oid}:manifest.json")]
        ),
        package.manifest_json()
    );
    for file in package.files() {
        assert_eq!(
            git_ok(
                fixture.transfer.path(),
                &["show", &format!("{oid}:session/{}", file.path)]
            ),
            file.bytes
        );
    }
    assert_eq!(
        git_ok(
            fixture.transfer.path(),
            &["rev-parse", &format!("{SESSION_REF_PREFIX}{}", task_id())]
        ),
        format!("{oid}\n").as_bytes()
    );
    for request in runner.requests() {
        assert!(
            request
                .environment_remove
                .iter()
                .any(|key| key == "GIT_DIR")
        );
        assert!(
            request
                .environment_remove
                .iter()
                .any(|key| key == "GIT_OBJECT_DIRECTORY")
        );
    }
    // Owned pin survives losing the user alternate's entire graph.
    fs::remove_dir_all(fixture.repo.root().join(".git/objects")).unwrap();
    fs::create_dir_all(fixture.repo.root().join(".git/objects")).unwrap();
    git_ok(
        fixture.transfer.path(),
        &["fsck", "--full", "--no-reflogs", &oid],
    );
}

#[test]
fn owned_pin_validation_accepts_only_canonical_session_and_request_ids() {
    let fixture = Fixture::new();
    for prefix in [
        SESSION_REF_PREFIX,
        REQUEST_SESSION_REF_PREFIX,
        "refs/mac-worker/requests/",
    ] {
        let name = format!("{prefix}{REQUEST_ID}");
        fixture
            .transfer
            .pin_object(&SystemProcessRunner, &name, &fixture.base)
            .unwrap();
        assert!(has_ref(fixture.transfer.path(), &name));
        fixture
            .transfer
            .unpin_object(&SystemProcessRunner, &name)
            .unwrap();
        fixture
            .transfer
            .unpin_object(&SystemProcessRunner, &name)
            .unwrap();
        for invalid in [
            "",
            "abc",
            "A0000000000000000000000000000028",
            "00000000-0000-0000-0000-000000000028",
            "00000000000000000000000000000028/extra",
            "000000000000000000000000000000280",
        ] {
            let runner = RecordingRunner::returning_success();
            let error = fixture
                .transfer
                .pin_object(&runner, &format!("{prefix}{invalid}"), &fixture.base)
                .unwrap_err();
            assert_eq!(error.public_code(), "BASE_UNAVAILABLE");
            assert!(runner.requests().is_empty());
        }
    }
    for name in [
        "refs/heads/main",
        "refs/mac-worker/session/00000000000000000000000000000028",
        "refs/mac-worker/bases/00000000000000000000000000000028",
    ] {
        assert!(
            fixture
                .transfer
                .pin_object(&SystemProcessRunner, name, &fixture.base)
                .is_err()
        );
    }
}

#[test]
fn mirror_hook_accepts_sessions_and_bases_but_rejects_other_refs_and_deletions() {
    let fixture = Fixture::new();
    let host = tempfile::tempdir().unwrap();
    let store = HostStore::open(&host.path().join("host")).unwrap();
    let mirror = store.mirror(PROJECT_ID).unwrap();
    let session_ref = format!("{SESSION_REF_PREFIX}{}", task_id());
    let base_ref = format!("refs/mac-worker/bases/{}", task_id());
    for name in [&session_ref, &base_ref] {
        let result = fixture.repo.git(&[
            "push",
            mirror.path().to_str().unwrap(),
            &format!("{}:{name}", fixture.base),
        ]);
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(has_ref(mirror.path(), name));
        assert!(
            !fixture
                .repo
                .git(&["push", mirror.path().to_str().unwrap(), &format!(":{name}")])
                .status
                .success()
        );
        assert!(has_ref(mirror.path(), name));
    }
    for name in [
        "refs/heads/main",
        "refs/tags/test",
        "refs/mac-worker/request-sessions/test",
    ] {
        assert!(
            !fixture
                .repo
                .git(&[
                    "push",
                    mirror.path().to_str().unwrap(),
                    &format!("{}:{name}", fixture.base)
                ])
                .status
                .success()
        );
        assert!(!has_ref(mirror.path(), name));
    }
}

#[test]
fn base_push_with_session_atomically_publishes_both_refs() {
    let fixture = Fixture::new();
    let oid = fixture
        .transfer
        .write_session_package(&SystemProcessRunner, task_id(), &package())
        .unwrap();
    let host = tempfile::tempdir().unwrap();
    let store = HostStore::open(&host.path().join("host")).unwrap();
    let mirror = store.mirror(PROJECT_ID).unwrap();
    let runner = LocalPushRunner::new(mirror.path());
    push_base(&fixture, &runner, Some(&oid)).unwrap();
    let request = runner.single_request();
    assert!(request.args.iter().any(|arg| arg == "--atomic"));
    assert!(has_ref(
        mirror.path(),
        &format!("refs/mac-worker/bases/{}", task_id())
    ));
    assert_eq!(
        git_ok(
            mirror.path(),
            &["rev-parse", &format!("{SESSION_REF_PREFIX}{}", task_id())]
        ),
        format!("{oid}\n").as_bytes()
    );
}

#[test]
fn controller_push_with_session_atomically_publishes_request_scoped_refs() {
    let fixture = Fixture::new();
    let oid = fixture
        .transfer
        .write_session_package(&SystemProcessRunner, task_id(), &package())
        .unwrap();
    let remote = tempfile::tempdir().unwrap();
    git_ok(remote.path(), &["init", "--bare"]);
    let runner = LocalPushRunner::new(remote.path());
    push_controller(&fixture, &runner, Some(&oid)).unwrap();
    assert!(
        runner
            .single_request()
            .args
            .iter()
            .any(|arg| arg == "--atomic")
    );
    assert!(has_ref(
        remote.path(),
        &format!("refs/mac-worker/requests/{REQUEST_ID}")
    ));
    assert_eq!(
        git_ok(
            remote.path(),
            &[
                "rev-parse",
                &format!("{REQUEST_SESSION_REF_PREFIX}{REQUEST_ID}")
            ]
        ),
        format!("{oid}\n").as_bytes()
    );
}

#[test]
fn rejection_of_one_ref_in_atomic_push_leaves_neither_ref() {
    for controller in [false, true] {
        let fixture = Fixture::new();
        let oid = fixture
            .transfer
            .write_session_package(&SystemProcessRunner, task_id(), &package())
            .unwrap();
        let remote = tempfile::tempdir().unwrap();
        git_ok(remote.path(), &["init", "--bare"]);
        let hook = remote.path().join("hooks/update");
        fs::write(&hook, b"#!/bin/sh\ncase \"$1\" in refs/mac-worker/sessions/*|refs/mac-worker/request-sessions/*) exit 1;; esac\nexit 0\n").unwrap();
        fs::set_permissions(&hook, fs::Permissions::from_mode(0o700)).unwrap();
        let runner = LocalPushRunner::new(remote.path());
        let result = if controller {
            push_controller(&fixture, &runner, Some(&oid))
        } else {
            push_base(&fixture, &runner, Some(&oid))
        };
        assert_eq!(result.unwrap_err().public_code(), "BASE_PUSH_FAILED");
        assert!(
            runner
                .single_request()
                .args
                .iter()
                .any(|arg| arg == "--atomic")
        );
        assert!(git_ok(remote.path(), &["for-each-ref", "--format=%(refname)"]).is_empty());
    }
}

#[test]
fn none_push_paths_keep_exact_single_ref_operations() {
    let fixture = Fixture::new();
    for controller in [false, true] {
        let runner = RecordingRunner::returning_success();
        if controller {
            push_controller(&fixture, &runner, None).unwrap();
        } else {
            push_base(&fixture, &runner, None).unwrap();
        }
        let request = runner.single_request();
        let start = request.args.iter().position(|arg| arg == "push").unwrap();
        let identity = identity();
        let expected = if controller {
            vec![
                "push".into(),
                "--porcelain".into(),
                "--no-verify".into(),
                format!(
                    "--receive-pack=~/.local/bin/worker host controller-receive-pack {TOKEN} {REQUEST_ID} {} {PROJECT_ID} {WORKTREE_ID} {}",
                    identity.request_fingerprint(),
                    fixture.base
                ),
                format!("mac1:{PROJECT_ID}"),
                format!("{}:refs/mac-worker/requests/{REQUEST_ID}", fixture.base),
            ]
        } else {
            vec![
                "push".into(),
                "--no-verify".into(),
                format!(
                    "--receive-pack=~/.local/bin/worker host receive-pack {} {} {} {}",
                    identity.job_id(),
                    identity.client_id(),
                    LeaseToken::new(Uuid::from_u128(30)),
                    identity.request_fingerprint()
                ),
                format!("mac1:{PROJECT_ID}"),
                format!("{}:refs/mac-worker/bases/{}", fixture.base, task_id()),
            ]
        };
        assert_eq!(
            &request.args[start..],
            expected.iter().map(OsString::from).collect::<Vec<_>>()
        );
        assert!(!request.args.iter().any(|arg| arg == "--atomic"));
        assert!(
            request
                .environment
                .iter()
                .any(|(key, value)| key == "GIT_CONFIG_GLOBAL" && value == "/dev/null")
        );
    }
}

#[test]
fn paired_release_is_idempotent_with_and_without_a_base_pin() {
    for with_base in [false, true] {
        let fixture = Fixture::new();
        let oid = fixture
            .transfer
            .write_session_package(&SystemProcessRunner, task_id(), &package())
            .unwrap();
        let base_ref = format!("refs/mac-worker/bases/{}", task_id());
        let session_ref = format!("{SESSION_REF_PREFIX}{}", task_id());
        let other_task = TaskId::new(Uuid::from_u128(2));
        let other_ref = format!("{SESSION_REF_PREFIX}{other_task}");
        fixture
            .transfer
            .write_session_package(&SystemProcessRunner, other_task, &package())
            .unwrap();
        if with_base {
            git_ok(
                fixture.transfer.path(),
                &["update-ref", &base_ref, fixture.base.as_str()],
            );
        }
        // Delete packed pins too, not just loose ref files.
        git_ok(fixture.transfer.path(), &["pack-refs", "--all", "--prune"]);
        fixture
            .transfer
            .release_task_refs(&SystemProcessRunner, task_id())
            .unwrap();
        fixture
            .transfer
            .release_task_refs(&SystemProcessRunner, task_id())
            .unwrap();
        assert!(!has_ref(fixture.transfer.path(), &base_ref));
        assert!(!has_ref(fixture.transfer.path(), &session_ref));
        assert_eq!(
            git_ok(fixture.transfer.path(), &["rev-parse", &other_ref]),
            format!("{oid}\n").as_bytes()
        );
    }
}

#[test]
fn individual_releases_preserve_the_other_task_pin() {
    let fixture = Fixture::new();
    fixture
        .transfer
        .write_session_package(&SystemProcessRunner, task_id(), &package())
        .unwrap();
    let base_ref = format!("refs/mac-worker/bases/{}", task_id());
    let session_ref = format!("{SESSION_REF_PREFIX}{}", task_id());
    git_ok(
        fixture.transfer.path(),
        &["update-ref", &base_ref, fixture.base.as_str()],
    );
    fixture
        .transfer
        .release_base(&SystemProcessRunner, task_id())
        .unwrap();
    assert!(!has_ref(fixture.transfer.path(), &base_ref));
    assert!(has_ref(fixture.transfer.path(), &session_ref));
    git_ok(
        fixture.transfer.path(),
        &["update-ref", &base_ref, fixture.base.as_str()],
    );
    fixture
        .transfer
        .release_session(&SystemProcessRunner, task_id())
        .unwrap();
    fixture
        .transfer
        .release_session(&SystemProcessRunner, task_id())
        .unwrap();
    assert!(has_ref(fixture.transfer.path(), &base_ref));
    assert!(!has_ref(fixture.transfer.path(), &session_ref));
}

struct FailSessionDeletion;

impl ProcessRunner for FailSessionDeletion {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.args.iter().any(|arg| arg == "update-ref")
            && request.args.iter().any(|arg| arg == "-d")
            && request
                .args
                .iter()
                .any(|arg| arg.to_string_lossy().starts_with(SESSION_REF_PREFIX))
        {
            return Err(WorkerError::Git {
                code: "BASE_UNAVAILABLE",
                message: "synthetic deletion failure".into(),
            });
        }
        SystemProcessRunner.run(request)
    }
}

#[test]
fn paired_release_reports_failure_and_retry_finishes_partial_cleanup() {
    let fixture = Fixture::new();
    fixture
        .transfer
        .write_session_package(&SystemProcessRunner, task_id(), &package())
        .unwrap();
    let base_ref = format!("refs/mac-worker/bases/{}", task_id());
    let session_ref = format!("{SESSION_REF_PREFIX}{}", task_id());
    git_ok(
        fixture.transfer.path(),
        &["update-ref", &base_ref, fixture.base.as_str()],
    );
    assert_eq!(
        fixture
            .transfer
            .release_task_refs(&FailSessionDeletion, task_id())
            .unwrap_err()
            .public_code(),
        "BASE_UNAVAILABLE"
    );
    assert!(!has_ref(fixture.transfer.path(), &base_ref));
    assert!(has_ref(fixture.transfer.path(), &session_ref));
    fixture
        .transfer
        .release_task_refs(&SystemProcessRunner, task_id())
        .unwrap();
    assert!(!has_ref(fixture.transfer.path(), &base_ref));
    assert!(!has_ref(fixture.transfer.path(), &session_ref));
}

#[test]
fn live_session_refs_prevent_transfer_repo_collection() {
    for prefix in [SESSION_REF_PREFIX, REQUEST_SESSION_REF_PREFIX] {
        let fixture = Fixture::new();
        let path = fixture.transfer.path().to_owned();
        git_ok(
            &path,
            &[
                "update-ref",
                &format!("{prefix}{REQUEST_ID}"),
                fixture.base.as_str(),
            ],
        );
        drop(fixture.transfer);
        let gc = TransferGc::new(fixture.cache.path(), &SystemProcessRunner);
        let report = gc.apply_at(u64::MAX).unwrap();
        assert!(report.candidates().is_empty());
        assert!(report.applied().is_empty());
        assert!(path.exists());
        git_ok(
            &path,
            &["update-ref", "-d", &format!("{prefix}{REQUEST_ID}")],
        );
        assert_eq!(gc.apply_at(u64::MAX).unwrap().applied().len(), 1);
        assert!(!path.exists());
    }
}
