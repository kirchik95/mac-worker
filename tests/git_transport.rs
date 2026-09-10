#[allow(dead_code)]
mod support;

use std::{
    ffi::OsString,
    fs,
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    path::{Path, PathBuf},
    process::Command,
};

use clap::Parser;
use mac_worker::{
    RuntimeContext,
    cli::Cli,
    config::WorkerEntry,
    error::WorkerError,
    git_transport::{
        GitServerExecutor, GitTransport, HostGitService, ReceivePackComponents,
        UploadPackComponents,
    },
    host_store::{HOST_LAYOUT_VERSION, HostStore},
    job::{
        ClientId, CommandSpec, JobId, LeaseAcquireRequest, LeaseToken, RequestFingerprintMaterial,
    },
    lease::{AdmissionFacts, LeaseService},
    process::{ProcessRequest, ProcessResult},
    protocol::{HealthStatus, PROTOCOL_VERSION},
    run_with_io_in_context,
    task::{BaseOid, TaskId},
    transfer::TransferIdentity,
    transfer_repo::TransferRepo,
    transport::SshTransport,
};
use support::{GitRepo, recording_runner::RecordingRunner};
use tempfile::TempDir;
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

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

fn task_id() -> TaskId {
    TaskId::new(Uuid::from_u128(1))
}

fn job_id() -> JobId {
    JobId::new(Uuid::from_u128(10))
}

fn other_job_id() -> JobId {
    JobId::new(Uuid::from_u128(11))
}

fn client_id() -> ClientId {
    ClientId::new(Uuid::from_u128(20))
}

fn lease_token() -> LeaseToken {
    LeaseToken::new(Uuid::from_u128(30))
}

fn fingerprint() -> mac_worker::job::RequestFingerprint {
    RequestFingerprintMaterial::new(
        job_id(),
        client_id(),
        lease_token(),
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
    .fingerprint()
}

fn base_oid() -> BaseOid {
    "0123456789012345678901234567890123456789".parse().unwrap()
}

fn transfer_identity() -> TransferIdentity {
    TransferIdentity::new(job_id(), client_id(), lease_token(), fingerprint())
}

fn env(request: &ProcessRequest, name: &str) -> String {
    request
        .environment
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.to_string_lossy().into_owned())
        .unwrap_or_else(|| panic!("missing environment variable {name}"))
}

fn assert_request_has_arg(request: &ProcessRequest, expected: &str) {
    assert!(
        request.args.iter().any(|arg| arg == expected),
        "missing {expected:?} in {:?}",
        request.args
    );
}

fn git_in(path: &Path, args: &[&str]) -> std::process::Output {
    Command::new("/usr/bin/git")
        .args(["--git-dir"])
        .arg(path)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .args(args)
        .output()
        .expect("run git directory fixture command")
}

#[test]
fn push_base_runs_in_transfer_repo_with_pinned_ssh_and_hidden_receive_pack() {
    let runner = RecordingRunner::returning_success();
    let transfer = tempfile::tempdir().unwrap();
    let identity = transfer_identity();
    let receipt = GitTransport::new(&runner)
        .push_base(
            &worker(),
            &identity,
            PROJECT_ID,
            task_id(),
            &base_oid(),
            transfer.path(),
        )
        .unwrap();
    let request = runner.single_request();

    assert_eq!(request.program, "/usr/bin/git");
    assert!(
        request
            .args
            .windows(2)
            .any(|window| window[0] == "-C" && window[1] == transfer.path().as_os_str())
    );
    assert_request_has_arg(&request, "--no-verify");
    assert!(
        request
            .args
            .windows(2)
            .any(|window| window[0] == "-c" && window[1] == "gc.auto=0")
    );
    let receive = request
        .args
        .iter()
        .find(|argument| argument.to_string_lossy().starts_with("--receive-pack="))
        .expect("hidden receive-pack override");
    let receive = receive.to_string_lossy();
    assert!(receive.starts_with(&format!(
        "--receive-pack=~/.local/bin/worker host receive-pack {} ",
        identity.job_id()
    )));
    assert!(
        request
            .args
            .contains(&OsString::from("mac1:".to_owned() + PROJECT_ID))
    );
    assert!(
        request
            .args
            .last()
            .unwrap()
            .to_string_lossy()
            .ends_with(&format!(":refs/mac-worker/bases/{}", task_id()))
    );
    assert!(
        !env(&request, "GIT_SSH_COMMAND")
            .split(' ')
            .any(|part| part == "--")
    );
    assert_eq!(env(&request, "GIT_CONFIG_GLOBAL"), "/dev/null");
    assert_eq!(env(&request, "GIT_CONFIG_NOSYSTEM"), "1");
    assert_eq!(env(&request, "GIT_TERMINAL_PROMPT"), "0");
    assert_eq!(receipt.objects_written(), 0);
}

#[test]
fn fetch_result_uses_hidden_upload_pack_and_aliases_import_receipt() {
    let expected_head = "0123456789012345678901234567890123456789";
    let runner = RecordingRunner::returning_results(vec![
        Ok(ProcessResult {
            status: std::process::ExitStatus::from_raw(0),
            stdout: Vec::new(),
            stderr: Vec::new(),
        }),
        Ok(ProcessResult {
            status: std::process::ExitStatus::from_raw(0),
            stdout: format!("{expected_head}\n").into_bytes(),
            stderr: Vec::new(),
        }),
    ]);
    let transfer = tempfile::tempdir().unwrap();
    let receipt = GitTransport::new(&runner)
        .fetch_result(
            &worker(),
            client_id(),
            PROJECT_ID,
            task_id(),
            transfer.path(),
        )
        .unwrap();
    let requests = runner.requests();
    assert_eq!(requests.len(), 2, "fetch must verify the fetched local ref");
    let request = &requests[0];

    assert_eq!(request.program, "/usr/bin/git");
    assert!(
        request
            .args
            .windows(2)
            .any(|window| window[0] == "-C" && window[1] == transfer.path().as_os_str())
    );
    assert_request_has_arg(request, "--no-write-fetch-head");
    let upload = request
        .args
        .iter()
        .find(|argument| argument.to_string_lossy().starts_with("--upload-pack="))
        .expect("hidden upload-pack override");
    assert!(
        upload
            .to_string_lossy()
            .contains(&format!("host upload-pack {}", task_id()))
    );
    assert!(request.args.iter().any(|argument| {
        argument
            .to_string_lossy()
            .contains(&format!("mac1:{PROJECT_ID}"))
    }));
    assert!(request.args.iter().any(|argument| {
        argument.to_string_lossy().contains(&format!(
            "refs/heads/task/{}:refs/mac-worker/results/{}",
            task_id(),
            task_id()
        ))
    }));
    assert_eq!(
        env(request, "GIT_SSH_COMMAND"),
        "/usr/bin/ssh -o BatchMode=yes -o ConnectTimeout=5 -o ForwardAgent=no -o ClearAllForwardings=yes"
    );
    assert_eq!(
        receipt.local_ref(),
        format!("refs/mac-worker/results/{}", task_id())
    );
    assert_eq!(receipt.head().as_str(), expected_head);
    assert!(
        requests[1]
            .args
            .windows(2)
            .any(|window| window[0] == "rev-parse" && window[1] == "--verify")
    );
}

#[test]
fn fetch_result_rejects_a_missing_local_ref_instead_of_fabricating_an_oid() {
    let runner = RecordingRunner::returning_results(vec![
        Ok(ProcessResult {
            status: std::process::ExitStatus::from_raw(0),
            stdout: Vec::new(),
            stderr: Vec::new(),
        }),
        Ok(ProcessResult {
            status: std::process::ExitStatus::from_raw(1 << 8),
            stdout: Vec::new(),
            stderr: b"fatal: needed a single revision\n".to_vec(),
        }),
    ]);
    let transfer = tempfile::tempdir().unwrap();

    let error = GitTransport::new(&runner)
        .fetch_result(
            &worker(),
            client_id(),
            PROJECT_ID,
            task_id(),
            transfer.path(),
        )
        .unwrap_err();

    assert_eq!(error.public_code(), "RESULT_FETCH_FAILED");
    assert_eq!(runner.requests().len(), 2);
}

type GitInvocation = (String, PathBuf, Vec<(OsString, OsString)>);

struct RecordingExecutor {
    calls: std::sync::Mutex<Vec<GitInvocation>>,
}

impl RecordingExecutor {
    fn new() -> Self {
        Self {
            calls: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<GitInvocation> {
        self.calls.lock().unwrap().clone()
    }
}

impl GitServerExecutor for RecordingExecutor {
    fn exec(
        &self,
        program: &str,
        mirror: &mac_worker::rooted_fs::RootedDir,
        environment: &[(OsString, OsString)],
    ) -> Result<std::convert::Infallible, WorkerError> {
        self.calls.lock().unwrap().push((
            program.into(),
            mirror.path().to_path_buf(),
            environment.to_vec(),
        ));
        Err(WorkerError::Protocol(
            "TEST_EXECUTOR_INVOKED: sentinel".into(),
        ))
    }
}

fn store_with_lease() -> (TempDir, HostStore) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let store = HostStore::open(&root).unwrap();
    let material = RequestFingerprintMaterial::new(
        job_id(),
        client_id(),
        lease_token(),
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
    .unwrap();
    let request = LeaseAcquireRequest::new(material);
    let facts = AdmissionFacts {
        free_disk_bytes: 100 * 1024 * 1024 * 1024,
        total_disk_bytes: 200 * 1024 * 1024 * 1024,
        memory_pressure: mac_worker::protocol::MemoryPressure::Normal,
        swap_used_bytes: Some(0),
    };
    LeaseService::new(&store)
        .acquire(&request, &facts, 100)
        .unwrap();
    (temp, store)
}

#[test]
fn receive_pack_is_keyed_by_turn_job_id_and_validates_lease_like_rsync() {
    let (_temp, store) = store_with_lease();
    let executor = RecordingExecutor::new();
    let service = HostGitService::new(&store);
    let error = service
        .receive_pack(
            &ReceivePackComponents::new(job_id(), client_id(), lease_token(), fingerprint()),
            PROJECT_ID,
            &executor,
        )
        .unwrap_err();
    assert_eq!(error.public_code(), "TEST_EXECUTOR_INVOKED");
    assert_eq!(executor.calls().len(), 1);
    assert_eq!(executor.calls()[0].0, "git-receive-pack");
    assert!(
        executor.calls()[0]
            .1
            .ends_with(format!("repos/{PROJECT_ID}.git"))
    );

    let error = service
        .receive_pack(
            &ReceivePackComponents::new(other_job_id(), client_id(), lease_token(), fingerprint()),
            PROJECT_ID,
            &executor,
        )
        .unwrap_err();
    assert_eq!(error.public_code(), "LEASE_IDENTITY_MISMATCH");
    assert_eq!(executor.calls().len(), 1);

    for bad in ["../x", "ABC", "", &"a".repeat(65)] {
        let error = service
            .receive_pack(
                &ReceivePackComponents::new(job_id(), client_id(), lease_token(), fingerprint()),
                bad,
                &executor,
            )
            .unwrap_err();
        assert_eq!(error.public_code(), "INVALID_COMPONENT");
    }
    assert_eq!(executor.calls().len(), 1);
}

#[test]
fn upload_pack_requires_published_task_metadata_and_branch_without_creating_mirror() {
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let runner = RecordingRunner::passthrough();
    let service = HostGitService::new(&store);
    let executor = RecordingExecutor::new();
    let error = service
        .upload_pack(
            &UploadPackComponents::new(task_id(), client_id()),
            PROJECT_ID,
            &executor,
        )
        .unwrap_err();
    assert_eq!(error.public_code(), "TASK_NOT_FOUND");
    assert!(store.mirror_if_present(PROJECT_ID).unwrap().is_none());
    drop(runner);
}

#[test]
fn mirror_hook_wins_over_global_hooks_path_and_denies_heads_and_deletions() {
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let mirror = store.mirror(PROJECT_ID).unwrap();
    let mirror_path = mirror.path().to_path_buf();
    assert_eq!(
        fs::metadata(mirror_path.join("hooks/pre-receive"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        git_in(&mirror_path, &["config", "receive.denyDeletes"]).stdout,
        b"true\n"
    );
    assert_eq!(
        git_in(&mirror_path, &["config", "core.hooksPath"]).stdout,
        b"hooks\n"
    );

    let source = GitRepo::init();
    source.write("a.txt", b"base\n");
    source.commit_all("base");
    let head = String::from_utf8(source.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    let mut command = Command::new("/usr/bin/git");
    command
        .current_dir(source.root())
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_DIR", source.root().join(".git"))
        .args([
            "push",
            &mirror_path.to_string_lossy(),
            &format!("{head}:refs/heads/main"),
        ]);
    let push = command.output().unwrap();
    assert!(!push.status.success());
}

#[test]
fn mirror_repair_rejects_hardlinked_pre_receive_before_writing() {
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let mirror = store.mirror(PROJECT_ID).unwrap();
    let hook = mirror.path().join("hooks/pre-receive");
    let sentinel = temp.path().join("sentinel");
    fs::write(&sentinel, b"do not overwrite").unwrap();
    fs::set_permissions(&sentinel, fs::Permissions::from_mode(0o600)).unwrap();

    fs::remove_file(&hook).unwrap();
    fs::hard_link(&sentinel, &hook).unwrap();

    assert!(
        store.mirror(PROJECT_ID).is_err(),
        "a hardlinked hook must be rejected before repair"
    );
    assert_eq!(fs::read(&sentinel).unwrap(), b"do not overwrite");
}

#[test]
fn git_ssh_command_matches_json_transport_options_without_separator() {
    let command = SshTransport::new(RecordingRunner::default())
        .git_ssh_command(&worker())
        .unwrap();
    assert_eq!(
        command,
        "/usr/bin/ssh -o BatchMode=yes -o ConnectTimeout=5 -o ForwardAgent=no -o ClearAllForwardings=yes"
    );
    assert!(!command.split(' ').any(|part| part == "--"));
}

#[test]
fn outdated_layout_fails_closed_until_hidden_setup_migrates_it() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("data/mac-worker/host");
    fs::create_dir_all(root.parent().unwrap()).unwrap();
    HostStore::open(&root).unwrap();
    let layout = root.join("layout.json");
    let bytes = fs::read(&layout).unwrap();
    let current = format!("\"version\":{HOST_LAYOUT_VERSION}");
    let old = "\"version\":1".to_string();
    let bytes = String::from_utf8(bytes).unwrap().replace(&current, &old);
    fs::write(&layout, bytes).unwrap();

    let error = match HostStore::open(&root) {
        Ok(_) => panic!("outdated host layout unexpectedly opened"),
        Err(error) => error,
    };
    assert_eq!(
        error.public_code(),
        "HOST_LAYOUT_OUTDATED",
        "unexpected error: {error:?}"
    );
    assert_eq!(
        mac_worker::probe::ProbeCollector::collect_at(&root)
            .unwrap_err()
            .public_code(),
        "HOST_LAYOUT_OUTDATED"
    );

    let runtime = RuntimeContext::isolated(
        [
            ("XDG_CONFIG_HOME".into(), temp.path().join("config").into()),
            ("XDG_DATA_HOME".into(), temp.path().join("data").into()),
        ]
        .into_iter()
        .collect(),
        temp.path().join("home"),
        temp.path().to_path_buf(),
    );
    let cli = Cli::try_parse_from(["worker", "host", "migrate-layout"]).unwrap();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_io_in_context(
        cli,
        &RecordingRunner::default(),
        &runtime,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0, "stderr: {}", String::from_utf8_lossy(&stderr));
    assert!(root.join("repos").is_dir());
    assert!(root.join("tasks").is_dir());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&fs::read(&layout).unwrap()).unwrap()["version"],
        HOST_LAYOUT_VERSION
    );
    assert!(HostStore::open(&root).is_ok());
}

#[test]
fn protocol_three_probe_fixture_is_a_protocol_mismatch_and_new_fixtures_derive_from_the_current_protocol()
 {
    assert_eq!(PROTOCOL_VERSION, 7);
    let mut old = serde_json::json!({
        "protocol_version": 3,
        "supervision_version": 2,
        "hostname": "mini-1.local",
        "arch": "arm64",
        "os_version": "26.2",
        "free_disk_bytes": 1,
        "total_disk_bytes": 2,
        "memory_pressure": "normal",
        "swap_used_bytes": 0,
        "available_memory_bytes": null,
        "cpu_counters": null,
        "slot_state": "idle",
        "active_lease": null,
        "capabilities": []
    });
    let decoded: mac_worker::protocol::ProbeResponse = serde_json::from_value(old.clone()).unwrap();
    assert_eq!(decoded.protocol_version, 3);
    old["protocol_version"] = serde_json::json!(PROTOCOL_VERSION);
    let decoded: mac_worker::protocol::ProbeResponse = serde_json::from_value(old).unwrap();
    assert_eq!(decoded.protocol_version, PROTOCOL_VERSION);
    assert_eq!(HealthStatus::Unavailable, HealthStatus::Unavailable);
}

#[test]
fn shared_recording_runner_can_execute_real_transfer_commands_and_keep_request_history() {
    let repo = GitRepo::init();
    repo.write("a.txt", b"a\n");
    repo.commit_all("base");
    let cache = tempfile::tempdir().unwrap();
    let transfer = TransferRepo::open_or_create(cache.path(), &repo.root().join(".git")).unwrap();
    let runner = RecordingRunner::passthrough();
    transfer
        .resolve_base(
            &runner,
            &mac_worker::project::ProjectInspector::new(&runner)
                .inspect(repo.root())
                .unwrap(),
            "HEAD",
        )
        .unwrap();
    assert!(!runner.requests().is_empty());
    assert!(runner.write_args().is_empty());
}
