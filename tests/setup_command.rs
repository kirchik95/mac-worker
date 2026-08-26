use std::{
    collections::VecDeque,
    ffi::OsString,
    fs,
    io::{self, Write},
    os::unix::{
        fs::{PermissionsExt, symlink},
        process::ExitStatusExt,
    },
    path::{Path, PathBuf},
    process::{ExitStatus, Output, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use mac_worker::{
    cli::{Cli, Command, HostCommand},
    config::WorkerEntry,
    error::WorkerError,
    execute_with,
    install::Installer,
    output::CommandOutput,
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner},
    protocol::{
        PROTOCOL_VERSION, SetupFailureKind, SetupHostResult, SetupReport, SetupWarning,
        SetupWarningCode,
    },
    run_with_io,
};
use tempfile::tempdir;
use uuid::Uuid;

const INSTALLATION_ID: &str = "00112233445566778899aabbccddeeff";
const CANDIDATE_DIGEST: &str = "04802bb988238c79cc29d0a1c8ded9bbc728ad5e5bbec2aeaf548d913b0ba1c2";

#[derive(Clone)]
struct RecordingRunner {
    requests: Arc<Mutex<Vec<ProcessRequest>>>,
    results: Arc<Mutex<VecDeque<Result<ProcessResult, WorkerError>>>>,
}

struct CandidateMutatingRunner {
    inner: RecordingRunner,
    candidate: PathBuf,
}

struct BrokenWriter;

#[derive(Default)]
struct FlushBrokenWriter {
    bytes: Vec<u8>,
}

impl Write for BrokenWriter {
    fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "writer closed"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Write for FlushBrokenWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "flush failed"))
    }
}

impl RecordingRunner {
    fn returning(results: Vec<ProcessResult>) -> Self {
        Self::returning_results(results.into_iter().map(Ok).collect())
    }

    fn returning_results(results: Vec<Result<ProcessResult, WorkerError>>) -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            results: Arc::new(Mutex::new(VecDeque::from(results))),
        }
    }

    fn requests(&self) -> Vec<ProcessRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl ProcessRunner for RecordingRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.requests.lock().unwrap().push(request.clone());
        self.results
            .lock()
            .unwrap()
            .pop_front()
            .expect("a predetermined process result")
    }
}

impl ProcessRunner for CandidateMutatingRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let result = self.inner.run(request);
        if request.program == self.candidate.as_os_str()
            && request.args == [OsString::from("host"), OsString::from("probe")]
        {
            fs::write(&self.candidate, b"candidate changed after preflight read").unwrap();
        }
        result
    }
}

fn result(code: i32, stdout: &[u8], stderr: &[u8]) -> ProcessResult {
    ProcessResult {
        status: ExitStatus::from_raw(code << 8),
        stdout: stdout.to_vec(),
        stderr: stderr.to_vec(),
    }
}

fn valid_probe_json() -> &'static [u8] {
    br#"{"protocol_version":1,"hostname":"mini-1.local","arch":"arm64","os_version":"26.2","free_disk_bytes":536870912,"memory_pressure":"normal","swap_used_bytes":134217728,"capabilities":["darwin-arm64"]}"#
}

fn worker() -> WorkerEntry {
    WorkerEntry {
        name: "mini-1".into(),
        ssh: "mac1".into(),
        slots: 1,
        capabilities: vec!["darwin-arm64".into()],
        remote_binary: "~/.local/bin/worker".into(),
    }
}

fn executable_fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let directory = tempdir().unwrap();
    let current_exe = directory.path().join("worker");
    fs::write(&current_exe, b"test worker").unwrap();
    fs::set_permissions(&current_exe, fs::Permissions::from_mode(0o755)).unwrap();
    (directory, current_exe)
}

fn preflight_policy() -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: 1024 * 1024,
        stderr_limit: 1024 * 1024,
        deadline: Duration::from_secs(15),
    }
}

fn control_policy() -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: 64 * 1024,
        stderr_limit: 64 * 1024,
        deadline: Duration::from_secs(30),
    }
}

fn transfer_policy() -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: 64 * 1024,
        stderr_limit: 64 * 1024,
        deadline: Duration::from_secs(5 * 60),
    }
}

fn assert_ssh_request_argv_and_policy(request: &ProcessRequest, policy: ProcessPolicy) {
    // Regression: setup requests inherited forwarding settings from SSH
    // configuration because the fixed argv did not disable them explicitly.
    assert_eq!(request.program, OsString::from("/usr/bin/ssh"));
    assert_eq!(request.args.len(), 11);
    assert_eq!(
        &request.args[..10],
        [
            OsString::from("-o"),
            OsString::from("BatchMode=yes"),
            OsString::from("-o"),
            OsString::from("ConnectTimeout=5"),
            OsString::from("-o"),
            OsString::from("ForwardAgent=no"),
            OsString::from("-o"),
            OsString::from("ClearAllForwardings=yes"),
            OsString::from("--"),
            OsString::from("mac1"),
        ]
    );
    assert!(!request.args[10].is_empty());
    assert!(request.environment.is_empty());
    assert_eq!(request.policy, policy);
}

fn assert_ssh_request_shape(request: &ProcessRequest, policy: ProcessPolicy) {
    assert_ssh_request_argv_and_policy(request, policy);
    assert!(request.stdin.is_none());
}

fn assert_upload_request_shape(request: &ProcessRequest, expected_stdin: &[u8]) {
    assert_ssh_request_argv_and_policy(request, transfer_policy());
    assert_eq!(request.stdin.as_deref(), Some(expected_stdin));
}

fn success_results() -> Vec<Result<ProcessResult, WorkerError>> {
    vec![
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"match\n", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"promoted\n", b"")),
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(0, b"", b"")),
    ]
}

struct GeneratedSetupCommands {
    acquire: String,
    upload: ProcessRequest,
    digest: String,
    prepare: String,
    promotion: String,
    reconciliation: String,
    verification: String,
    success_cleanup: String,
    cleanup: String,
    rollback: String,
}

fn request_command(request: &ProcessRequest) -> String {
    request
        .args
        .last()
        .expect("an SSH request must contain a remote command")
        .to_string_lossy()
        .into_owned()
}

fn assert_remote_command(request: &ProcessRequest, expected: &str) {
    assert_eq!(request_command(request), expected);
}

fn contains_remote_command(requests: &[ProcessRequest], expected: &str) -> bool {
    requests
        .iter()
        .filter(|request| request.program == "/usr/bin/ssh")
        .any(|request| request_command(request) == expected)
}

fn generated_setup_commands() -> GeneratedSetupCommands {
    let (_directory, current_exe) = executable_fixture();
    let runner = RecordingRunner::returning_results(success_results());
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());
    let installed = installer.install(&current_exe, &worker());
    assert!(installed.installed);
    let requests = runner.requests();

    let mut rollback_results = success_results();
    rollback_results[7] = Ok(result(255, b"", b"candidate failed"));
    let rollback_runner = RecordingRunner::returning_results(rollback_results);
    let rollback_installer = Installer::with_installation_id(
        &rollback_runner,
        Uuid::parse_str(INSTALLATION_ID).unwrap(),
    );
    let failed = rollback_installer.install(&current_exe, &worker());
    assert_eq!(failed.error_code.as_deref(), Some("VERIFICATION_FAILED"));
    let rollback_requests = rollback_runner.requests();

    let cleanup_runner = RecordingRunner::returning(vec![
        result(0, valid_probe_json(), b""),
        result(0, b"", b""),
        result(1, b"", b"upload refused"),
        result(0, b"", b""),
    ]);
    let cleanup_installer =
        Installer::with_installation_id(&cleanup_runner, Uuid::parse_str(INSTALLATION_ID).unwrap());
    let failed = cleanup_installer.install(&current_exe, &worker());
    assert_eq!(failed.error_code.as_deref(), Some("TRANSFER_FAILED"));
    let cleanup_requests = cleanup_runner.requests();

    GeneratedSetupCommands {
        acquire: request_command(&requests[1]),
        upload: requests[2].clone(),
        digest: request_command(&requests[3]),
        prepare: request_command(&requests[4]),
        promotion: request_command(&requests[5]),
        reconciliation: request_command(&requests[6]),
        verification: request_command(&requests[7]),
        success_cleanup: request_command(&requests[8]),
        cleanup: request_command(&cleanup_requests[3]),
        rollback: request_command(&rollback_requests[8]),
    }
}

fn run_remote_command(command: &str, home: &Path) -> Output {
    std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(command)
        .env("HOME", home)
        .output()
        .unwrap()
}

fn run_remote_request(request: &ProcessRequest, home: &Path) -> Output {
    let mut child = std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(request_command(request))
        .env("HOME", home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(request.stdin.as_deref().unwrap_or_default())
        .unwrap();
    child.wait_with_output().unwrap()
}

fn file_digest(path: &Path) -> String {
    let output = std::process::Command::new("/usr/bin/shasum")
        .args(["-a", "256"])
        .arg(path)
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .into()
}

fn promoted_fixture(home: &Path, previous: Option<&[u8]>) -> (PathBuf, PathBuf, PathBuf) {
    let bin = home.join(".local/bin");
    let setup = home.join(".local/share/mac-worker/setup");
    let lock = setup.join(".install-lock");
    let transaction = setup.join(INSTALLATION_ID);
    fs::create_dir_all(&bin).unwrap();
    fs::create_dir_all(&lock).unwrap();
    fs::create_dir_all(&transaction).unwrap();
    let active = bin.join("worker");
    fs::write(&active, b"test worker").unwrap();
    fs::write(lock.join("owner"), format!("{INSTALLATION_ID}\n")).unwrap();
    fs::write(
        transaction.join("candidate.sha256"),
        format!("{CANDIDATE_DIGEST}\n"),
    )
    .unwrap();
    fs::write(transaction.join("state"), b"promoted\n").unwrap();
    if let Some(previous) = previous {
        let backup = transaction.join("worker.previous");
        fs::write(&backup, previous).unwrap();
        fs::write(
            transaction.join("previous.sha256"),
            format!("{}\n", file_digest(&backup)),
        )
        .unwrap();
    } else {
        fs::write(transaction.join("no-previous"), b"").unwrap();
    }
    (active, lock, transaction)
}

fn run_executable_setup(
    results: Vec<Result<ProcessResult, WorkerError>>,
) -> (u8, Vec<u8>, Vec<u8>, Vec<ProcessRequest>) {
    let directory = tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
    )
    .unwrap();
    let runner = RecordingRunner::returning_results(results);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_io(
        Cli {
            config: Some(config_path),
            json: true,
            command: Command::Setup { hosts: Vec::new() },
        },
        &runner,
        &mut stdout,
        &mut stderr,
    );
    (exit, stdout, stderr, runner.requests())
}

#[derive(Clone, Copy, Debug)]
enum UnexpectedEntryKind {
    File,
    Directory,
    Symlink,
}

fn plant_unexpected_entry(root: &Path, name: &str, kind: UnexpectedEntryKind) -> PathBuf {
    let path = root.join(name);
    match kind {
        UnexpectedEntryKind::File => fs::write(&path, b"unexpected").unwrap(),
        UnexpectedEntryKind::Directory => fs::create_dir(&path).unwrap(),
        UnexpectedEntryKind::Symlink => symlink("state", &path).unwrap(),
    }
    path
}

#[test]
fn generated_acquisition_rejects_a_symlinked_setup_before_touching_its_target() {
    // Catches following an existing setup symlink while creating the lock and
    // owner transaction. The generated production command itself is executed.
    let commands = generated_setup_commands();
    let directory = tempdir().unwrap();
    let home = directory.path().join("home");
    let outside = directory.path().join("outside");
    fs::create_dir_all(home.join(".local/bin")).unwrap();
    fs::create_dir_all(home.join(".local/share/mac-worker")).unwrap();
    fs::create_dir(&outside).unwrap();
    symlink(&outside, home.join(".local/share/mac-worker/setup")).unwrap();

    let output = run_remote_command(&commands.acquire, &home);

    assert!(
        !output.status.success(),
        "symlinked setup was accepted and mutated {}",
        outside.display()
    );
    assert!(!outside.join(".install-lock").exists());
    assert!(!outside.join(INSTALLATION_ID).exists());
}

#[test]
fn generated_upload_writes_exact_stdin_bytes_only_after_validating_the_acquired_transaction() {
    // Catches replacing the guarded upload with a separate path-based transfer
    // or consuming bytes without creating the exact owner-scoped regular file.
    let commands = generated_setup_commands();
    let directory = tempdir().unwrap();
    let home = directory.path().join("home");
    fs::create_dir(&home).unwrap();
    assert!(
        run_remote_command(&commands.acquire, &home)
            .status
            .success()
    );

    assert_upload_request_shape(&commands.upload, b"test worker");
    let output = run_remote_request(&commands.upload, &home);

    assert!(output.status.success(), "{:?}", output.stderr);
    let transaction = home
        .join(".local/share/mac-worker/setup")
        .join(INSTALLATION_ID);
    let staged = transaction.join("worker.new");
    assert_eq!(fs::read(&staged).unwrap(), b"test worker");
    assert_eq!(
        fs::metadata(&staged).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(fs::read(transaction.join("state")).unwrap(), b"acquired\n");
    assert!(!transaction.join("candidate.sha256").exists());
}

#[test]
fn generated_upload_rejects_a_swapped_transaction_symlink_without_touching_its_target() {
    // Catches opening worker.new through a transaction path swapped to a
    // symlink after acquisition but before the upload request executes.
    let commands = generated_setup_commands();
    let directory = tempdir().unwrap();
    let home = directory.path().join("home");
    let outside = directory.path().join("outside");
    fs::create_dir(&home).unwrap();
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("sentinel"), b"outside unchanged").unwrap();
    assert!(
        run_remote_command(&commands.acquire, &home)
            .status
            .success()
    );
    let transaction = home
        .join(".local/share/mac-worker/setup")
        .join(INSTALLATION_ID);
    fs::rename(&transaction, home.join("retained-transaction")).unwrap();
    symlink(&outside, &transaction).unwrap();

    assert_upload_request_shape(&commands.upload, b"test worker");
    let output = run_remote_request(&commands.upload, &home);

    assert!(!output.status.success());
    assert!(!outside.join("worker.new").exists());
    assert_eq!(
        fs::read(outside.join("sentinel")).unwrap(),
        b"outside unchanged"
    );
}

#[test]
fn candidate_path_change_after_preflight_cannot_split_upload_bytes_from_the_expected_digest() {
    // Catches hashing one local read but uploading bytes from a later path
    // read after the executable changes during protocol preflight.
    let (directory, current_exe) = executable_fixture();
    let inner = RecordingRunner::returning_results(success_results());
    let runner = CandidateMutatingRunner {
        inner: inner.clone(),
        candidate: current_exe.clone(),
    };
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(installed.installed);
    assert_eq!(
        fs::read(&current_exe).unwrap(),
        b"candidate changed after preflight read"
    );
    let requests = inner.requests();
    assert_upload_request_shape(&requests[2], b"test worker");
    let home = directory.path().join("home");
    fs::create_dir(&home).unwrap();
    assert!(
        run_remote_command(&request_command(&requests[1]), &home)
            .status
            .success()
    );
    assert!(run_remote_request(&requests[2], &home).status.success());
    let digest = run_remote_command(&request_command(&requests[3]), &home);
    assert!(digest.status.success());
    assert_eq!(digest.stdout, b"match\n");
}

#[test]
fn generated_locked_context_rejects_owner_with_terminal_nul_and_preserves_acquired_evidence() {
    // Catches whole-file command substitution dropping a terminal NUL from
    // the lock owner and allowing the digest phase to mutate the transaction.
    let commands = generated_setup_commands();
    let directory = tempdir().unwrap();
    let home = directory.path().join("home");
    fs::create_dir(&home).unwrap();
    assert!(
        run_remote_command(&commands.acquire, &home)
            .status
            .success()
    );
    assert!(run_remote_request(&commands.upload, &home).status.success());
    let setup = home.join(".local/share/mac-worker/setup");
    let lock = setup.join(".install-lock");
    let transaction = setup.join(INSTALLATION_ID);
    let active = home.join(".local/bin/worker");
    fs::write(&active, b"active helper unchanged").unwrap();
    let mut malformed_owner = INSTALLATION_ID.as_bytes().to_vec();
    malformed_owner.push(0);
    fs::write(lock.join("owner"), &malformed_owner).unwrap();

    let output = run_remote_command(&commands.digest, &home);

    assert!(!output.status.success());
    assert_eq!(fs::read(lock.join("owner")).unwrap(), malformed_owner);
    assert_eq!(fs::read(&active).unwrap(), b"active helper unchanged");
    assert_eq!(
        fs::read(transaction.join("worker.new")).unwrap(),
        b"test worker"
    );
    assert_eq!(fs::read(transaction.join("state")).unwrap(), b"acquired\n");
    assert!(!transaction.join("candidate.sha256").exists());
}

#[test]
fn generated_locked_context_rejects_non_lf_exact_state_bytes_before_mutation() {
    // Catches accepting a terminal NUL as the required LF on a fixed state;
    // companion CRLF and missing-LF cases protect the complete byte layout.
    let commands = generated_setup_commands();
    let cases: &[(&str, &[u8])] = &[
        ("terminal NUL", b"acquired\0"),
        ("CRLF", b"acquired\r\n"),
        ("missing LF", b"acquired"),
    ];

    for (label, malformed_state) in cases {
        let directory = tempdir().unwrap();
        let home = directory.path().join("home");
        fs::create_dir(&home).unwrap();
        assert!(
            run_remote_command(&commands.acquire, &home)
                .status
                .success()
        );
        assert!(run_remote_request(&commands.upload, &home).status.success());
        let transaction = home
            .join(".local/share/mac-worker/setup")
            .join(INSTALLATION_ID);
        fs::write(transaction.join("state"), malformed_state).unwrap();

        let output = run_remote_command(&commands.digest, &home);

        assert!(!output.status.success(), "accepted {label}");
        assert_eq!(
            fs::read(transaction.join("state")).unwrap(),
            *malformed_state,
            "mutated {label}"
        );
        assert_eq!(
            fs::read(transaction.join("worker.new")).unwrap(),
            b"test worker"
        );
        assert!(!transaction.join("candidate.sha256").exists());
    }
}

#[test]
fn generated_canonical_hex_reader_rejects_digest_with_terminal_nul_and_preserves_staged_evidence() {
    // Catches command substitution erasing the digest's terminal NUL and
    // allowing prepare to create backup evidence or advance state.
    let commands = generated_setup_commands();
    let directory = tempdir().unwrap();
    let home = directory.path().join("home");
    fs::create_dir(&home).unwrap();
    assert!(
        run_remote_command(&commands.acquire, &home)
            .status
            .success()
    );
    assert!(run_remote_request(&commands.upload, &home).status.success());
    assert!(run_remote_command(&commands.digest, &home).status.success());
    let setup = home.join(".local/share/mac-worker/setup");
    let lock = setup.join(".install-lock");
    let transaction = setup.join(INSTALLATION_ID);
    let active = home.join(".local/bin/worker");
    fs::write(&active, b"active helper unchanged").unwrap();
    let mut malformed_digest = CANDIDATE_DIGEST.as_bytes().to_vec();
    malformed_digest.push(0);
    fs::write(transaction.join("candidate.sha256"), &malformed_digest).unwrap();

    let output = run_remote_command(&commands.prepare, &home);

    assert!(!output.status.success());
    assert_eq!(
        fs::read(lock.join("owner")).unwrap(),
        format!("{INSTALLATION_ID}\n").as_bytes()
    );
    assert_eq!(fs::read(&active).unwrap(), b"active helper unchanged");
    assert_eq!(
        fs::read(transaction.join("candidate.sha256")).unwrap(),
        malformed_digest
    );
    assert_eq!(
        fs::read(transaction.join("worker.new")).unwrap(),
        b"test worker"
    );
    assert_eq!(fs::read(transaction.join("state")).unwrap(), b"staged\n");
    assert!(!transaction.join("worker.previous").exists());
    assert!(!transaction.join("previous.sha256").exists());
    assert!(!transaction.join("no-previous").exists());
}

#[test]
fn generated_setup_phases_complete_and_cleanup_only_owned_evidence() {
    // Catches a containment guard that rejects the normal replacement flow or
    // a cleanup command that removes the promoted helper.
    let commands = generated_setup_commands();
    let directory = tempdir().unwrap();
    let home = directory.path().join("home");
    fs::create_dir(&home).unwrap();

    assert!(
        run_remote_command(&commands.acquire, &home)
            .status
            .success()
    );
    let transaction = home
        .join(".local/share/mac-worker/setup")
        .join(INSTALLATION_ID);
    let active = home.join(".local/bin/worker");
    fs::write(&active, b"previous worker").unwrap();
    fs::write(transaction.join("worker.new"), b"test worker").unwrap();

    let digest = run_remote_command(&commands.digest, &home);
    assert!(digest.status.success());
    assert_eq!(digest.stdout, b"match\n");
    assert!(
        run_remote_command(&commands.prepare, &home)
            .status
            .success()
    );
    assert!(
        run_remote_command(&commands.promotion, &home)
            .status
            .success()
    );
    let reconciliation = run_remote_command(&commands.reconciliation, &home);
    assert!(reconciliation.status.success());
    assert_eq!(reconciliation.stdout, b"promoted\n");
    assert!(
        run_remote_command(&commands.success_cleanup, &home)
            .status
            .success()
    );

    assert_eq!(fs::read(&active).unwrap(), b"test worker");
    assert!(!transaction.exists());
    assert!(
        !home
            .join(".local/share/mac-worker/setup/.install-lock")
            .exists()
    );
}

#[test]
fn verified_success_cleanup_rejects_changed_active_bytes_and_preserves_evidence() {
    // Regression: post-verification cleanup reused the failure cleanup guard,
    // so a replaced active helper could not prevent deletion of the proof.
    let commands = generated_setup_commands();
    let cases: [(&str, Option<&[u8]>); 2] = [
        ("replacement install", Some(b"previous worker")),
        ("first install", None),
    ];

    for (label, previous) in cases {
        let directory = tempdir().unwrap();
        let home = directory.path().join("home");
        let (active, lock, transaction) = promoted_fixture(&home, previous);
        fs::write(&active, b"replacement bytes").unwrap();

        let output = run_remote_command(&commands.success_cleanup, &home);

        assert!(!output.status.success(), "cleanup accepted {label}");
        assert_eq!(fs::read(&active).unwrap(), b"replacement bytes");
        assert_eq!(
            fs::read(lock.join("owner")).unwrap(),
            format!("{INSTALLATION_ID}\n").as_bytes()
        );
        assert_eq!(
            fs::read(transaction.join("candidate.sha256")).unwrap(),
            format!("{CANDIDATE_DIGEST}\n").as_bytes()
        );
        assert_eq!(fs::read(transaction.join("state")).unwrap(), b"promoted\n");
        if previous.is_some() {
            assert_eq!(
                fs::read(transaction.join("worker.previous")).unwrap(),
                b"previous worker"
            );
            assert!(transaction.join("previous.sha256").is_file());
        } else {
            assert_eq!(fs::read(transaction.join("no-previous")).unwrap(), b"");
        }
    }
}

#[test]
fn verified_success_request_binds_cleanup_to_the_verification_digest() {
    // Regression: the success cleanup request carried no candidate digest,
    // unlike the immediately preceding verification request.
    let commands = generated_setup_commands();
    let expected_assignment = format!("expected_digest='{CANDIDATE_DIGEST}'");

    assert!(
        commands
            .verification
            .lines()
            .any(|line| line == expected_assignment)
    );
    assert!(
        commands
            .success_cleanup
            .lines()
            .any(|line| line == expected_assignment)
    );
}

#[test]
fn generated_rollback_restores_the_owned_backup_and_releases_evidence() {
    // Catches validation that cannot recognize the exact legitimate rollback
    // state produced by acquisition, digest, prepare, and promotion.
    let commands = generated_setup_commands();
    let directory = tempdir().unwrap();
    let home = directory.path().join("home");
    fs::create_dir(&home).unwrap();

    assert!(
        run_remote_command(&commands.acquire, &home)
            .status
            .success()
    );
    let transaction = home
        .join(".local/share/mac-worker/setup")
        .join(INSTALLATION_ID);
    let active = home.join(".local/bin/worker");
    fs::write(&active, b"previous worker").unwrap();
    fs::write(transaction.join("worker.new"), b"test worker").unwrap();
    assert!(run_remote_command(&commands.digest, &home).status.success());
    assert!(
        run_remote_command(&commands.prepare, &home)
            .status
            .success()
    );
    assert!(
        run_remote_command(&commands.promotion, &home)
            .status
            .success()
    );

    let rollback = run_remote_command(&commands.rollback, &home);

    assert!(rollback.status.success());
    assert_eq!(fs::read(&active).unwrap(), b"previous worker");
    assert!(!transaction.exists());
    assert!(
        !home
            .join(".local/share/mac-worker/setup/.install-lock")
            .exists()
    );
}

#[test]
fn generated_rollback_removes_a_failed_first_install_and_releases_evidence() {
    // Catches a rollback implementation that handles replacements but cannot
    // safely remove the owned candidate when no previous helper existed.
    let commands = generated_setup_commands();
    let directory = tempdir().unwrap();
    let home = directory.path().join("home");
    fs::create_dir(&home).unwrap();

    assert!(
        run_remote_command(&commands.acquire, &home)
            .status
            .success()
    );
    let transaction = home
        .join(".local/share/mac-worker/setup")
        .join(INSTALLATION_ID);
    let active = home.join(".local/bin/worker");
    fs::write(transaction.join("worker.new"), b"test worker").unwrap();
    assert!(run_remote_command(&commands.digest, &home).status.success());
    assert!(
        run_remote_command(&commands.prepare, &home)
            .status
            .success()
    );
    assert!(
        run_remote_command(&commands.promotion, &home)
            .status
            .success()
    );
    assert!(active.is_file());

    let rollback = run_remote_command(&commands.rollback, &home);

    assert!(rollback.status.success());
    assert!(!active.exists());
    assert!(!transaction.exists());
    assert!(
        !home
            .join(".local/share/mac-worker/setup/.install-lock")
            .exists()
    );
}

#[test]
fn generated_promotion_rejects_a_swapped_symlinked_bin_without_touching_its_target() {
    // Catches promotion following a bin-directory swap between SSH requests.
    let commands = generated_setup_commands();
    let directory = tempdir().unwrap();
    let home = directory.path().join("home");
    let outside = directory.path().join("outside");
    let setup = home.join(".local/share/mac-worker/setup");
    let lock = setup.join(".install-lock");
    let transaction = setup.join(INSTALLATION_ID);
    fs::create_dir_all(home.join(".local/share/mac-worker/setup/.install-lock")).unwrap();
    fs::create_dir(&transaction).unwrap();
    fs::create_dir(&outside).unwrap();
    symlink(&outside, home.join(".local/bin")).unwrap();
    fs::write(outside.join("worker"), b"outside worker").unwrap();
    fs::write(lock.join("owner"), format!("{INSTALLATION_ID}\n")).unwrap();
    fs::write(transaction.join("worker.new"), b"test worker").unwrap();
    fs::write(
        transaction.join("candidate.sha256"),
        format!("{CANDIDATE_DIGEST}\n"),
    )
    .unwrap();
    fs::write(transaction.join("state"), b"prepared\n").unwrap();
    fs::write(transaction.join("worker.previous"), b"outside worker").unwrap();
    fs::write(
        transaction.join("previous.sha256"),
        format!("{}\n", file_digest(&transaction.join("worker.previous"))),
    )
    .unwrap();

    let output = run_remote_command(&commands.promotion, &home);

    assert!(!output.status.success());
    assert_eq!(fs::read(outside.join("worker")).unwrap(), b"outside worker");
    assert_eq!(
        fs::read(transaction.join("worker.new")).unwrap(),
        b"test worker"
    );
    assert_eq!(fs::read(transaction.join("state")).unwrap(), b"prepared\n");
}

#[test]
fn generated_rollback_rejects_a_swapped_symlinked_bin_without_touching_its_target() {
    // Catches rollback following a bin-directory swap before restoring or
    // removing the active helper.
    let commands = generated_setup_commands();
    let directory = tempdir().unwrap();
    let home = directory.path().join("home");
    let outside = directory.path().join("outside");
    let (_active, lock, transaction) = promoted_fixture(&home, Some(b"previous worker"));
    fs::rename(home.join(".local/bin"), home.join("original-bin")).unwrap();
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("worker"), b"test worker").unwrap();
    symlink(&outside, home.join(".local/bin")).unwrap();

    let output = run_remote_command(&commands.rollback, &home);

    assert!(!output.status.success());
    assert_eq!(fs::read(outside.join("worker")).unwrap(), b"test worker");
    assert_eq!(
        fs::read(transaction.join("worker.previous")).unwrap(),
        b"previous worker"
    );
    assert!(lock.join("owner").is_file());
}

#[test]
fn generated_verification_rejects_a_swapped_symlinked_bin_without_running_its_target() {
    // Catches the post-reconciliation probe following a bin-directory swap
    // and executing a helper outside the validated installation root.
    let commands = generated_setup_commands();
    let directory = tempdir().unwrap();
    let home = directory.path().join("home");
    let outside = directory.path().join("outside");
    let marker = home.join("outside-probe-ran");
    let (_active, lock, transaction) = promoted_fixture(&home, None);
    fs::rename(home.join(".local/bin"), home.join("original-bin")).unwrap();
    fs::create_dir(&outside).unwrap();
    let outside_worker = outside.join("worker");
    fs::write(
        &outside_worker,
        b"#!/bin/sh\nprintf '%s\\n' ran > \"$HOME/outside-probe-ran\"\n",
    )
    .unwrap();
    fs::set_permissions(&outside_worker, fs::Permissions::from_mode(0o755)).unwrap();
    symlink(&outside, home.join(".local/bin")).unwrap();

    let output = run_remote_command(&commands.verification, &home);

    assert!(!output.status.success());
    assert!(!marker.exists());
    assert!(lock.join("owner").is_file());
    assert!(transaction.join("candidate.sha256").is_file());
    assert!(transaction.join("state").is_file());
}

#[test]
fn generated_cleanup_rejects_every_unexpected_direct_entry_before_mutation() {
    // Catches partial cleanup before direct-entry validation, including names
    // that cannot be represented safely as newline-delimited text.
    let commands = generated_setup_commands();
    let cases = [
        (false, "unexpected", UnexpectedEntryKind::File),
        (false, ".unexpected", UnexpectedEntryKind::File),
        (false, "unexpected-dir", UnexpectedEntryKind::Directory),
        (false, "unexpected-link", UnexpectedEntryKind::Symlink),
        (false, "embedded\nnewline", UnexpectedEntryKind::File),
        (false, "trailing-newline\n", UnexpectedEntryKind::File),
        (true, "unexpected", UnexpectedEntryKind::File),
        (true, ".unexpected", UnexpectedEntryKind::File),
        (true, "unexpected-dir", UnexpectedEntryKind::Directory),
        (true, "unexpected-link", UnexpectedEntryKind::Symlink),
        (true, "embedded\nnewline", UnexpectedEntryKind::File),
        (true, "trailing-newline\n", UnexpectedEntryKind::File),
    ];

    for (in_lock, name, kind) in cases {
        let directory = tempdir().unwrap();
        let home = directory.path().join("home");
        let (active, lock, transaction) = promoted_fixture(&home, None);
        let root = if in_lock { &lock } else { &transaction };
        let unexpected = plant_unexpected_entry(root, name, kind);
        let output = run_remote_command(&commands.cleanup, &home);

        assert!(
            !output.status.success(),
            "cleanup accepted {kind:?} entry {name:?} in {}",
            root.display()
        );
        assert_eq!(fs::read(&active).unwrap(), b"test worker");
        assert!(lock.join("owner").is_file());
        assert!(transaction.join("candidate.sha256").is_file());
        assert!(transaction.join("no-previous").is_file());
        assert!(transaction.join("state").is_file());
        assert!(fs::symlink_metadata(&unexpected).is_ok());
    }
}

#[test]
fn generated_rollback_rejects_every_unexpected_direct_entry_before_mutation() {
    // Catches restoring/removing the active helper before validating every
    // transaction and lock entry with pathname-safe argv semantics.
    let commands = generated_setup_commands();
    let cases = [
        (false, "unexpected", UnexpectedEntryKind::File),
        (false, ".unexpected", UnexpectedEntryKind::File),
        (false, "unexpected-dir", UnexpectedEntryKind::Directory),
        (false, "unexpected-link", UnexpectedEntryKind::Symlink),
        (false, "embedded\nnewline", UnexpectedEntryKind::File),
        (false, "trailing-newline\n", UnexpectedEntryKind::File),
        (true, "unexpected", UnexpectedEntryKind::File),
        (true, ".unexpected", UnexpectedEntryKind::File),
        (true, "unexpected-dir", UnexpectedEntryKind::Directory),
        (true, "unexpected-link", UnexpectedEntryKind::Symlink),
        (true, "embedded\nnewline", UnexpectedEntryKind::File),
        (true, "trailing-newline\n", UnexpectedEntryKind::File),
    ];

    for (in_lock, name, kind) in cases {
        let directory = tempdir().unwrap();
        let home = directory.path().join("home");
        let (active, lock, transaction) = promoted_fixture(&home, Some(b"previous worker"));
        let root = if in_lock { &lock } else { &transaction };
        let unexpected = plant_unexpected_entry(root, name, kind);
        let output = run_remote_command(&commands.rollback, &home);

        assert!(
            !output.status.success(),
            "rollback accepted {kind:?} entry {name:?} in {}",
            root.display()
        );
        assert_eq!(fs::read(&active).unwrap(), b"test worker");
        assert!(lock.join("owner").is_file());
        assert!(transaction.join("candidate.sha256").is_file());
        assert!(transaction.join("worker.previous").is_file());
        assert!(transaction.join("previous.sha256").is_file());
        assert!(transaction.join("state").is_file());
        assert!(fs::symlink_metadata(&unexpected).is_ok());
    }
}

#[test]
fn success_locks_hashes_promotes_reconciles_verifies_and_releases_with_safe_argv() {
    // Catches reintroducing a separate SCP mutation, skipping a transaction
    // boundary, or dropping the SSH option terminator, stdin, or policy.
    let (_directory, current_exe) = executable_fixture();
    let runner = RecordingRunner::returning_results(success_results());
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(installed.installed);
    assert_eq!(installed.protocol_version, Some(1));
    assert!(installed.warnings.is_empty());
    let requests = runner.requests();
    assert_eq!(requests.len(), 9);
    assert_eq!(
        requests[0],
        ProcessRequest {
            program: current_exe.clone().into_os_string(),
            args: vec!["host".into(), "probe".into()],
            environment: Vec::new(),
            environment_remove: Vec::new(),
            stdin: None,
            policy: preflight_policy(),
        }
    );
    assert_ssh_request_shape(&requests[1], control_policy());
    assert_upload_request_shape(&requests[2], b"test worker");
    for request in [&requests[3], &requests[4], &requests[5], &requests[6]] {
        assert_ssh_request_shape(request, control_policy());
    }
    assert_ssh_request_shape(&requests[7], preflight_policy());
    assert_ssh_request_shape(&requests[8], control_policy());
    assert!(
        requests
            .iter()
            .all(|request| request.program != "/usr/bin/scp")
    );
}

#[test]
fn competing_installation_is_rejected_before_transfer_or_target_access() {
    // Catches ignoring the atomic lock failure and touching the shared target.
    let (_directory, current_exe) = executable_fixture();
    let runner = RecordingRunner::returning(vec![
        result(0, valid_probe_json(), b""),
        result(75, b"", b"lock busy"),
    ]);
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(!installed.installed);
    assert_eq!(installed.error_code.as_deref(), Some("INSTALL_LOCKED"));
    let requests = runner.requests();
    assert_eq!(requests.len(), 2);
    assert_ssh_request_shape(&requests[1], control_policy());
}

#[test]
fn upload_failure_attempts_owned_cleanup_without_losing_primary_error() {
    // Catches returning from a partial guarded upload or replacing its primary
    // error with a later cleanup failure.
    let (_directory, current_exe) = executable_fixture();
    let runner = RecordingRunner::returning(vec![
        result(0, valid_probe_json(), b""),
        result(0, b"", b""),
        result(1, b"", b"transfer interrupted"),
        result(1, b"", b"cleanup refused"),
    ]);
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(!installed.installed);
    assert_eq!(installed.error_code.as_deref(), Some("TRANSFER_FAILED"));
    assert!(
        installed
            .error_message
            .as_deref()
            .unwrap()
            .contains("transfer interrupted")
    );
    assert_eq!(installed.warnings.len(), 1);
    assert_eq!(installed.warnings[0].code, SetupWarningCode::CleanupFailed);
    assert!(installed.warnings[0].message.contains("cleanup refused"));
    let requests = runner.requests();
    assert_eq!(requests.len(), 4);
    assert_ssh_request_shape(&requests[3], control_policy());
    assert_remote_command(&requests[3], &generated_setup_commands().cleanup);
}

#[test]
fn changed_candidate_digest_is_never_prepared_or_promoted() {
    // Catches promoting staged bytes that no longer match the local snapshot.
    let (_directory, current_exe) = executable_fixture();
    let runner = RecordingRunner::returning(vec![
        result(0, valid_probe_json(), b""),
        result(0, b"", b""),
        result(0, b"", b""),
        result(0, b"mismatch\n", b""),
        result(0, b"", b""),
    ]);
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(!installed.installed);
    assert_eq!(
        installed.error_code.as_deref(),
        Some("CANDIDATE_DIGEST_MISMATCH")
    );
    let requests = runner.requests();
    assert_eq!(requests.len(), 5);
    assert_ssh_request_shape(&requests[4], control_policy());
    assert_remote_command(&requests[4], &generated_setup_commands().cleanup);
}

#[test]
fn promotion_failure_before_move_reconciles_previous_target_then_cleans_up() {
    // Catches retrying promotion or treating failed acknowledgment as success.
    let (_directory, current_exe) = executable_fixture();
    let runner = RecordingRunner::returning(vec![
        result(0, valid_probe_json(), b""),
        result(0, b"", b""),
        result(0, b"", b""),
        result(0, b"match\n", b""),
        result(0, b"", b""),
        result(1, b"", b"chmod failed"),
        result(0, b"previous\n", b""),
        result(0, b"", b""),
    ]);
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(!installed.installed);
    assert_eq!(installed.error_code.as_deref(), Some("PROMOTION_FAILED"));
    let requests = runner.requests();
    assert_eq!(requests.len(), 8);
    assert_ssh_request_shape(&requests[6], control_policy());
    assert_ssh_request_shape(&requests[7], control_policy());
    let commands = generated_setup_commands();
    assert_remote_command(&requests[6], &commands.reconciliation);
    assert_remote_command(&requests[7], &commands.cleanup);
}

#[test]
fn disconnected_promotion_reconciled_as_candidate_continues_to_probe() {
    // Catches submitting promotion twice or rolling back an already-active
    // candidate merely because the SSH result was lost.
    let (_directory, current_exe) = executable_fixture();
    let mut results = success_results();
    results[5] = Err(WorkerError::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "promotion connection lost",
    )));
    let runner = RecordingRunner::returning_results(results);
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(installed.installed);
    let requests = runner.requests();
    assert_eq!(requests.len(), 9);
    assert_ssh_request_shape(&requests[5], control_policy());
    assert_ssh_request_shape(&requests[6], control_policy());
    let commands = generated_setup_commands();
    assert_remote_command(&requests[5], &commands.promotion);
    assert_remote_command(&requests[6], &commands.reconciliation);
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.program == "/usr/bin/ssh" && request_command(request) == commands.promotion
            })
            .count(),
        1
    );
}

#[test]
fn disconnected_promotion_reconciled_as_previous_retains_owned_state() {
    // Catches treating a lost SSH result plus one observation of the previous
    // target as proof that the remote promotion command has stopped.
    let (_directory, current_exe) = executable_fixture();
    let runner = RecordingRunner::returning_results(vec![
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"match\n", b"")),
        Ok(result(0, b"", b"")),
        Err(WorkerError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "promotion connection lost",
        ))),
        Ok(result(0, b"previous\n", b"")),
        Ok(result(0, b"", b"")),
    ]);
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(!installed.installed);
    assert_eq!(
        installed.error_code.as_deref(),
        Some("UNKNOWN_INSTALLATION_STATE")
    );
    let requests = runner.requests();
    assert_eq!(requests.len(), 7);
    assert_ssh_request_shape(&requests[5], control_policy());
    assert_ssh_request_shape(&requests[6], control_policy());
    let commands = generated_setup_commands();
    assert_remote_command(&requests[5], &commands.promotion);
    assert_remote_command(&requests[6], &commands.reconciliation);
    assert!(!contains_remote_command(&requests, &commands.cleanup));
    assert!(!contains_remote_command(&requests, &commands.rollback));
}

#[test]
fn ssh_transport_failure_reconciled_as_previous_retains_owned_state() {
    // Catches treating SSH's reserved transport-error status as a concrete
    // remote promotion failure eligible for cleanup.
    let (_directory, current_exe) = executable_fixture();
    let runner = RecordingRunner::returning(vec![
        result(0, valid_probe_json(), b""),
        result(0, b"", b""),
        result(0, b"", b""),
        result(0, b"match\n", b""),
        result(0, b"", b""),
        result(255, b"", b"connection lost"),
        result(0, b"previous\n", b""),
        result(0, b"", b""),
    ]);
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(!installed.installed);
    assert_eq!(
        installed.error_code.as_deref(),
        Some("UNKNOWN_INSTALLATION_STATE")
    );
    let requests = runner.requests();
    assert_eq!(requests.len(), 7);
    assert_ssh_request_shape(&requests[5], control_policy());
    assert_ssh_request_shape(&requests[6], control_policy());
    let commands = generated_setup_commands();
    assert_remote_command(&requests[5], &commands.promotion);
    assert_remote_command(&requests[6], &commands.reconciliation);
    assert!(!contains_remote_command(&requests, &commands.cleanup));
    assert!(!contains_remote_command(&requests, &commands.rollback));
}

#[test]
fn reconciliation_requires_candidate_digest_and_final_promoted_marker() {
    // Catches classifying candidate bytes as promoted while the owner
    // transaction still records an in-progress promotion.
    let (directory, current_exe) = executable_fixture();
    let runner = RecordingRunner::returning_results(vec![
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"match\n", b"")),
        Ok(result(0, b"", b"")),
        Err(WorkerError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "promotion connection lost",
        ))),
        Ok(result(0, b"unknown\n", b"")),
    ]);
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());
    let _ = installer.install(&current_exe, &worker());
    let command = runner.requests()[6]
        .args
        .last()
        .unwrap()
        .to_string_lossy()
        .into_owned();

    let home = directory.path().join("home");
    let (_active, _lock, transaction) = promoted_fixture(&home, None);
    fs::write(transaction.join("state"), b"promoting\n").unwrap();

    let output = run_remote_command(&command, &home);

    assert!(output.status.success());
    assert_eq!(output.stdout, b"unknown\n");
}

#[test]
fn unrecoverable_promotion_state_retains_lock_and_scoped_data() {
    // Catches guessing rollback/cleanup when neither byte state is provable.
    let (_directory, current_exe) = executable_fixture();
    let runner = RecordingRunner::returning_results(vec![
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"match\n", b"")),
        Ok(result(0, b"", b"")),
        Err(WorkerError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "promotion connection lost",
        ))),
        Ok(result(0, b"unknown\n", b"")),
    ]);
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(!installed.installed);
    assert_eq!(
        installed.error_code.as_deref(),
        Some("UNKNOWN_INSTALLATION_STATE")
    );
    let requests = runner.requests();
    assert_eq!(requests.len(), 7);
    assert_ssh_request_shape(&requests[6], control_policy());
    let commands = generated_setup_commands();
    assert_remote_command(&requests[6], &commands.reconciliation);
    assert!(!contains_remote_command(&requests, &commands.cleanup));
    assert!(!contains_remote_command(&requests, &commands.rollback));
}

#[test]
fn failed_verification_restores_owned_backup_and_releases_lock() {
    // Catches deleting the previous helper or restoring a shared stale backup.
    let (_directory, current_exe) = executable_fixture();
    let runner = RecordingRunner::returning(vec![
        result(0, valid_probe_json(), b""),
        result(0, b"", b""),
        result(0, b"", b""),
        result(0, b"match\n", b""),
        result(0, b"", b""),
        result(0, b"", b""),
        result(0, b"promoted\n", b""),
        result(255, b"", b"candidate failed"),
        result(0, b"", b""),
    ]);
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(!installed.installed);
    assert_eq!(installed.error_code.as_deref(), Some("VERIFICATION_FAILED"));
    let requests = runner.requests();
    assert_eq!(requests.len(), 9);
    assert_ssh_request_shape(&requests[8], control_policy());
    assert_remote_command(&requests[8], &generated_setup_commands().rollback);
}

#[test]
fn verified_install_with_cleanup_failure_stays_installed_with_typed_warning() {
    // Catches calling a byte-matched, verified install failed solely because
    // targeted cleanup did not finish.
    let (_directory, current_exe) = executable_fixture();
    let mut results = success_results();
    results[8] = Ok(result(1, b"", b"lock release failed"));
    let runner = RecordingRunner::returning_results(results);
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(installed.installed);
    assert_eq!(installed.protocol_version, Some(PROTOCOL_VERSION));
    assert_eq!(installed.warnings.len(), 1);
    assert_eq!(installed.warnings[0].code, SetupWarningCode::CleanupFailed);
    assert!(
        installed.warnings[0]
            .message
            .contains("lock release failed")
    );
}

#[test]
fn setup_dispatch_keeps_processing_inventory_names_after_a_host_failure() {
    let directory = tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"first\"\nssh = \"mac1\"\nslots = 1\n[[workers]]\nname = \"second\"\nssh = \"mac2\"\nslots = 1\n",
    )
    .unwrap();
    let mut results = vec![
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(75, b"", b"lock busy")),
    ];
    results.extend(success_results());
    let runner = RecordingRunner::returning_results(results);
    let cli = Cli {
        config: Some(config_path),
        json: true,
        command: Command::Setup {
            hosts: vec!["first".into(), "second".into()],
        },
    };

    let output = execute_with(cli, &runner).unwrap();

    let CommandOutput::Setup(report) = output else {
        panic!("setup must return a setup report")
    };
    assert_eq!(report.workers.len(), 2);
    assert!(!report.workers[0].installed);
    assert!(report.workers[1].installed);
}

#[test]
fn setup_selection_uses_inventory_names_and_empty_means_all() {
    let directory = tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"first\"\nssh = \"mac1\"\nslots = 1\n[[workers]]\nname = \"second\"\nssh = \"mac2\"\nslots = 1\n",
    )
    .unwrap();
    let runner = RecordingRunner::returning(vec![
        result(0, b"invalid local probe", b""),
        result(0, b"invalid local probe", b""),
    ]);
    let output = execute_with(
        Cli {
            config: Some(config_path.clone()),
            json: false,
            command: Command::Setup { hosts: Vec::new() },
        },
        &runner,
    )
    .unwrap();
    let CommandOutput::Setup(report) = output else {
        panic!("setup must return a setup report")
    };
    assert_eq!(
        report
            .workers
            .iter()
            .map(|worker| worker.name.as_str())
            .collect::<Vec<_>>(),
        vec!["first", "second"]
    );

    let empty_runner = RecordingRunner::returning(Vec::new());
    let error = execute_with(
        Cli {
            config: Some(config_path),
            json: false,
            command: Command::Setup {
                hosts: vec!["mac1".into()],
            },
        },
        &empty_runner,
    )
    .unwrap_err();
    assert!(matches!(error, WorkerError::Config(_)));
    assert!(empty_runner.requests().is_empty());
}

#[test]
fn hidden_host_probe_does_not_load_client_inventory() {
    let runner = RecordingRunner::returning(Vec::new());
    let output = execute_with(
        Cli {
            config: Some("/definitely/missing/mac-worker.toml".into()),
            json: false,
            command: Command::Host {
                command: HostCommand::Probe,
            },
        },
        &runner,
    )
    .unwrap();

    let CommandOutput::Probe(probe) = output else {
        panic!("host probe must return raw probe data")
    };
    assert_eq!(probe.protocol_version, 1);
    assert!(!probe.hostname.is_empty());
}

#[test]
fn setup_json_and_human_output_keep_per_host_results() {
    let report = SetupReport {
        protocol_version: PROTOCOL_VERSION,
        workers: vec![
            SetupHostResult {
                name: "mini-1".into(),
                ssh: "mac1".into(),
                installed: true,
                protocol_version: Some(PROTOCOL_VERSION),
                error_code: None,
                error_message: None,
                failure_kind: None,
                warnings: Vec::new(),
            },
            SetupHostResult {
                name: "mini-2".into(),
                ssh: "mac2".into(),
                installed: false,
                protocol_version: None,
                error_code: Some("INSTALL_FAILED".into()),
                error_message: Some("transfer failed".into()),
                failure_kind: Some(SetupFailureKind::Infrastructure),
                warnings: Vec::new(),
            },
        ],
    };
    let output = CommandOutput::Setup(report);

    assert_eq!(
        output.render_human(),
        "mini-1: installed (protocol 1)\nmini-2: failed [INSTALL_FAILED]: transfer failed"
    );
    assert_eq!(
        output.render_json().unwrap(),
        r#"{"kind":"setup","protocol_version":1,"workers":[{"name":"mini-1","ssh":"mac1","installed":true,"protocol_version":1,"error_code":null,"error_message":null,"warnings":[]},{"name":"mini-2","ssh":"mac2","installed":false,"protocol_version":null,"error_code":"INSTALL_FAILED","error_message":"transfer failed","warnings":[]}]}"#
    );
}

#[test]
fn setup_human_output_surfaces_cleanup_warning_after_verified_success() {
    // Catches showing only "installed" while hiding that the owned lock or
    // installation-scoped state could not be removed.
    let output = CommandOutput::Setup(SetupReport {
        protocol_version: PROTOCOL_VERSION,
        workers: vec![SetupHostResult {
            name: "mini-1".into(),
            ssh: "mac1".into(),
            installed: true,
            protocol_version: Some(PROTOCOL_VERSION),
            error_code: None,
            error_message: None,
            failure_kind: None,
            warnings: vec![SetupWarning {
                code: SetupWarningCode::CleanupFailed,
                message: "lock release failed".into(),
            }],
        }],
    });

    assert_eq!(
        output.render_human(),
        "mini-1: installed (protocol 1)\n  warning [CLEANUP_FAILED]: lock release failed"
    );
}

#[test]
fn executable_setup_all_success_renders_complete_report_and_exits_zero() {
    // Catches treating the typed setup report as an error before it is emitted
    // or returning a reserved failure for an entirely successful selection.
    let directory = tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap();
    let runner = RecordingRunner::returning_results(success_results());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_with_io(
        Cli {
            config: Some(config_path),
            json: true,
            command: Command::Setup { hosts: Vec::new() },
        },
        &runner,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, 0);
    assert!(stderr.is_empty());
    let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(value["workers"][0]["name"], "mini-1");
    assert_eq!(value["workers"][0]["installed"], true);
}

#[test]
fn executable_setup_all_failed_renders_every_host_and_exits_unavailable() {
    // Catches returning exit zero merely because setup produced a report, or
    // aborting the report after the first failed host.
    let directory = tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"first\"\nssh = \"mac1\"\nslots = 1\n[[workers]]\nname = \"second\"\nssh = \"mac2\"\nslots = 1\n",
    )
    .unwrap();
    let runner = RecordingRunner::returning(vec![
        result(0, valid_probe_json(), b""),
        result(75, b"", b"lock busy"),
        result(0, valid_probe_json(), b""),
        result(75, b"", b"lock busy"),
    ]);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_with_io(
        Cli {
            config: Some(config_path),
            json: true,
            command: Command::Setup { hosts: Vec::new() },
        },
        &runner,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, 69);
    assert!(stderr.is_empty());
    let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(value["workers"].as_array().unwrap().len(), 2);
    assert_eq!(value["workers"][0]["name"], "first");
    assert_eq!(value["workers"][0]["installed"], false);
    assert_eq!(value["workers"][1]["name"], "second");
    assert_eq!(value["workers"][1]["installed"], false);
}

#[test]
fn executable_setup_integrity_failure_renders_report_then_exits_infrastructure() {
    // Catches flattening a digest/integrity failure into retryable worker
    // unavailability or returning before the typed report is rendered.
    let directory = tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
    )
    .unwrap();
    let runner = RecordingRunner::returning(vec![
        result(0, valid_probe_json(), b""),
        result(0, b"", b""),
        result(0, b"", b""),
        result(0, b"mismatch\n", b""),
        result(0, b"", b""),
    ]);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_with_io(
        Cli {
            config: Some(config_path),
            json: true,
            command: Command::Setup { hosts: Vec::new() },
        },
        &runner,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, 70);
    assert!(stderr.is_empty());
    let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(
        value["workers"][0]["error_code"],
        "CANDIDATE_DIGEST_MISMATCH"
    );
}

#[test]
fn executable_setup_local_io_failure_renders_report_then_exits_io() {
    // Catches losing the local I/O category when preflight turns the failure
    // into the stable LOCAL_BINARY_INVALID report code.
    let directory = tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
    )
    .unwrap();
    let runner =
        RecordingRunner::returning_results(vec![Err(WorkerError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "cannot execute candidate",
        )))]);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_with_io(
        Cli {
            config: Some(config_path),
            json: true,
            command: Command::Setup { hosts: Vec::new() },
        },
        &runner,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, 74);
    assert!(stderr.is_empty());
    let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(value["workers"][0]["error_code"], "LOCAL_BINARY_INVALID");
}

#[test]
fn executable_setup_acquisition_io_retains_state_renders_report_and_exits_io() {
    // Catches flattening a local acquisition runner I/O cause while the
    // stable unknown-state report retains remote evidence.
    let (exit, stdout, stderr, requests) = run_executable_setup(vec![
        Ok(result(0, valid_probe_json(), b"")),
        Err(WorkerError::Io(std::io::Error::other(
            "acquisition result read failed",
        ))),
    ]);

    assert_eq!(exit, 74);
    assert!(stderr.is_empty());
    assert_eq!(
        stdout,
        b"{\"kind\":\"setup\",\"protocol_version\":1,\"workers\":[{\"name\":\"mini-1\",\"ssh\":\"mac1\",\"installed\":false,\"protocol_version\":null,\"error_code\":\"UNKNOWN_INSTALLATION_STATE\",\"error_message\":\"installation lock acquisition result was lost; scoped state was retained: I/O error: acquisition result read failed\",\"warnings\":[]}]}\n"
    );
    assert_eq!(requests.len(), 2);
}

#[test]
fn executable_setup_digest_io_cleans_up_renders_report_and_exits_io() {
    // Catches assigning infrastructure after a typed local digest runner I/O
    // failure while preserving the phase-specific public report.
    let (exit, stdout, stderr, requests) = run_executable_setup(vec![
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"", b"")),
        Err(WorkerError::Io(std::io::Error::other(
            "digest result read failed",
        ))),
        Ok(result(0, b"", b"")),
    ]);

    assert_eq!(exit, 74);
    assert!(stderr.is_empty());
    assert_eq!(
        stdout,
        b"{\"kind\":\"setup\",\"protocol_version\":1,\"workers\":[{\"name\":\"mini-1\",\"ssh\":\"mac1\",\"installed\":false,\"protocol_version\":null,\"error_code\":\"DIGEST_VERIFICATION_FAILED\",\"error_message\":\"failed to verify staged candidate digest: I/O error: digest result read failed\",\"warnings\":[]}]}\n"
    );
    assert_eq!(requests.len(), 5);
}

#[test]
fn executable_setup_prepare_io_cleans_up_renders_report_and_exits_io() {
    // Catches run-success string flattening at the prepare boundary.
    let (exit, stdout, stderr, requests) = run_executable_setup(vec![
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"match\n", b"")),
        Err(WorkerError::Io(std::io::Error::other(
            "prepare result read failed",
        ))),
        Ok(result(0, b"", b"")),
    ]);

    assert_eq!(exit, 74);
    assert!(stderr.is_empty());
    assert_eq!(
        stdout,
        b"{\"kind\":\"setup\",\"protocol_version\":1,\"workers\":[{\"name\":\"mini-1\",\"ssh\":\"mac1\",\"installed\":false,\"protocol_version\":null,\"error_code\":\"INSTALL_FAILED\",\"error_message\":\"failed to launch /usr/bin/ssh: I/O error: prepare result read failed\",\"warnings\":[]}]}\n"
    );
    assert_eq!(requests.len(), 6);
}

#[test]
fn executable_setup_promotion_io_retains_ambiguous_state_and_exits_io() {
    // Catches losing the typed promotion cause when reconciliation observes
    // the previous helper but cannot prove the remote command has stopped.
    let (exit, stdout, stderr, requests) = run_executable_setup(vec![
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"match\n", b"")),
        Ok(result(0, b"", b"")),
        Err(WorkerError::Io(std::io::Error::other(
            "promotion result read failed",
        ))),
        Ok(result(0, b"previous\n", b"")),
    ]);

    assert_eq!(exit, 74);
    assert!(stderr.is_empty());
    assert_eq!(
        stdout,
        b"{\"kind\":\"setup\",\"protocol_version\":1,\"workers\":[{\"name\":\"mini-1\",\"ssh\":\"mac1\",\"installed\":false,\"protocol_version\":null,\"error_code\":\"UNKNOWN_INSTALLATION_STATE\",\"error_message\":\"failed to launch /usr/bin/ssh: I/O error: promotion result read failed; the previous target is currently observable but promotion completion is unproven; installation lock and scoped state were retained\",\"warnings\":[]}]}\n"
    );
    assert_eq!(requests.len(), 7);
}

#[test]
fn executable_setup_reconciliation_io_retains_state_renders_report_and_exits_io() {
    // Catches converting the typed reconciliation runner cause to a string
    // before aggregate classification.
    let (exit, stdout, stderr, requests) = run_executable_setup(vec![
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"match\n", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"", b"")),
        Err(WorkerError::Io(std::io::Error::other(
            "reconciliation result read failed",
        ))),
    ]);

    assert_eq!(exit, 74);
    assert!(stderr.is_empty());
    assert_eq!(
        stdout,
        b"{\"kind\":\"setup\",\"protocol_version\":1,\"workers\":[{\"name\":\"mini-1\",\"ssh\":\"mac1\",\"installed\":false,\"protocol_version\":null,\"error_code\":\"UNKNOWN_INSTALLATION_STATE\",\"error_message\":\"promotion reported success but reconciliation could not prove completion: I/O error: reconciliation result read failed; installation lock and scoped state were retained\",\"warnings\":[]}]}\n"
    );
    assert_eq!(requests.len(), 7);
}

#[test]
fn executable_setup_verification_io_rolls_back_renders_report_and_exits_io() {
    // Catches SshTransport rendering away the verification runner cause before
    // Installer assigns the aggregate setup category.
    let (exit, stdout, stderr, requests) = run_executable_setup(vec![
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"match\n", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"promoted\n", b"")),
        Err(WorkerError::Io(std::io::Error::other(
            "verification result read failed",
        ))),
        Ok(result(0, b"", b"")),
    ]);

    assert_eq!(exit, 74);
    assert!(stderr.is_empty());
    assert_eq!(
        stdout,
        b"{\"kind\":\"setup\",\"protocol_version\":1,\"workers\":[{\"name\":\"mini-1\",\"ssh\":\"mac1\",\"installed\":false,\"protocol_version\":null,\"error_code\":\"VERIFICATION_FAILED\",\"error_message\":\"failed to launch SSH probe: I/O error: verification result read failed\",\"warnings\":[]}]}\n"
    );
    assert_eq!(requests.len(), 9);
}

#[test]
fn executable_setup_upload_io_renders_transfer_report_cleans_up_and_exits_io() {
    // Catches flattening typed SSH upload I/O into retryable unavailability,
    // omitting the complete report, or skipping owner-scoped cleanup.
    let directory = tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
    )
    .unwrap();
    let runner = RecordingRunner::returning_results(vec![
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(0, b"", b"")),
        Err(WorkerError::Io(std::io::Error::other(
            "upload result read failed",
        ))),
        Ok(result(0, b"", b"")),
    ]);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_with_io(
        Cli {
            config: Some(config_path),
            json: true,
            command: Command::Setup { hosts: Vec::new() },
        },
        &runner,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, 74);
    assert!(stderr.is_empty());
    assert_eq!(
        stdout,
        b"{\"kind\":\"setup\",\"protocol_version\":1,\"workers\":[{\"name\":\"mini-1\",\"ssh\":\"mac1\",\"installed\":false,\"protocol_version\":null,\"error_code\":\"TRANSFER_FAILED\",\"error_message\":\"failed to launch /usr/bin/ssh: I/O error: upload result read failed\",\"warnings\":[]}]}\n"
    );
    let requests = runner.requests();
    assert_eq!(requests.len(), 4);
    let executable_bytes = fs::read(std::env::current_exe().unwrap()).unwrap();
    assert_upload_request_shape(&requests[2], &executable_bytes);
    assert_ssh_request_shape(&requests[3], control_policy());
}

#[test]
fn executable_setup_upload_nonzero_renders_transfer_report_cleans_up_and_exits_unavailable() {
    // Catches classifying an acknowledged remote SSH upload failure as local
    // I/O, omitting the complete report, or skipping owner-scoped cleanup.
    let directory = tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
    )
    .unwrap();
    let runner = RecordingRunner::returning(vec![
        result(0, valid_probe_json(), b""),
        result(0, b"", b""),
        result(1, b"", b"transfer interrupted"),
        result(0, b"", b""),
    ]);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_with_io(
        Cli {
            config: Some(config_path),
            json: true,
            command: Command::Setup { hosts: Vec::new() },
        },
        &runner,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, 69);
    assert!(stderr.is_empty());
    assert_eq!(
        stdout,
        b"{\"kind\":\"setup\",\"protocol_version\":1,\"workers\":[{\"name\":\"mini-1\",\"ssh\":\"mac1\",\"installed\":false,\"protocol_version\":null,\"error_code\":\"TRANSFER_FAILED\",\"error_message\":\"/usr/bin/ssh failed with exit 1: transfer interrupted\",\"warnings\":[]}]}\n"
    );
    let requests = runner.requests();
    assert_eq!(requests.len(), 4);
    let executable_bytes = fs::read(std::env::current_exe().unwrap()).unwrap();
    assert_upload_request_shape(&requests[2], &executable_bytes);
    assert_ssh_request_shape(&requests[3], control_policy());
}

#[test]
fn executable_setup_mixed_failures_render_all_hosts_and_use_strongest_category() {
    // Catches first/last-result aggregation and proves I/O outranks
    // infrastructure, which in turn outranks retryable unavailability.
    let directory = tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"retryable\"\nssh = \"mac1\"\nslots = 1\n[[workers]]\nname = \"integrity\"\nssh = \"mac2\"\nslots = 1\n[[workers]]\nname = \"local-io\"\nssh = \"mac3\"\nslots = 1\n",
    )
    .unwrap();
    let runner = RecordingRunner::returning_results(vec![
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(75, b"", b"lock busy")),
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"mismatch\n", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, valid_probe_json(), b"")),
        Err(WorkerError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "cannot read acquisition result",
        ))),
    ]);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_with_io(
        Cli {
            config: Some(config_path),
            json: true,
            command: Command::Setup { hosts: Vec::new() },
        },
        &runner,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, 74);
    assert!(stderr.is_empty());
    let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(value["workers"].as_array().unwrap().len(), 3);
    assert_eq!(value["workers"][0]["error_code"], "INSTALL_LOCKED");
    assert_eq!(
        value["workers"][1]["error_code"],
        "CANDIDATE_DIGEST_MISMATCH"
    );
    assert_eq!(
        value["workers"][2]["error_code"],
        "UNKNOWN_INSTALLATION_STATE"
    );
}

#[test]
fn executable_setup_partial_failure_renders_success_and_failure_then_exits_unavailable() {
    // Catches reducing the aggregate exit to the last host result while still
    // requiring both per-host outcomes to remain visible.
    let directory = tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"first\"\nssh = \"mac1\"\nslots = 1\n[[workers]]\nname = \"second\"\nssh = \"mac2\"\nslots = 1\n",
    )
    .unwrap();
    let mut results = vec![
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(75, b"", b"lock busy")),
    ];
    results.extend(success_results());
    let runner = RecordingRunner::returning_results(results);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_with_io(
        Cli {
            config: Some(config_path),
            json: true,
            command: Command::Setup { hosts: Vec::new() },
        },
        &runner,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, 69);
    assert!(stderr.is_empty());
    let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(value["workers"][0]["installed"], false);
    assert_eq!(value["workers"][1]["installed"], true);
}

#[test]
fn successful_output_broken_pipe_is_typed_io_and_maps_to_exit_74() {
    // Catches println-style panic behavior or mapping a successful-output I/O
    // failure to the setup aggregate status instead of the reserved I/O code.
    let output = CommandOutput::Setup(SetupReport {
        protocol_version: PROTOCOL_VERSION,
        workers: Vec::new(),
    });
    let error = output
        .write_to(&mut BrokenWriter, true, false)
        .expect_err("a closed output writer must be reported");
    assert!(matches!(error, WorkerError::Io(_)));

    let directory = tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap();
    let runner = RecordingRunner::returning_results(success_results());
    let mut stderr = Vec::new();
    let exit = run_with_io(
        Cli {
            config: Some(config_path),
            json: true,
            command: Command::Setup { hosts: Vec::new() },
        },
        &runner,
        &mut BrokenWriter,
        &mut stderr,
    );
    assert_eq!(exit, 74);
    assert!(String::from_utf8(stderr).unwrap().contains("I/O error"));
}

#[test]
fn successful_output_flush_failure_is_typed_io() {
    // Catches reporting success when a buffered writer accepted the bytes but
    // could not deliver them during the final flush.
    let output = CommandOutput::Setup(SetupReport {
        protocol_version: PROTOCOL_VERSION,
        workers: Vec::new(),
    });
    let mut writer = FlushBrokenWriter::default();

    let error = output
        .write_to(&mut writer, true, false)
        .expect_err("flush failure must be reported");

    assert!(matches!(error, WorkerError::Io(_)));
    assert!(!writer.bytes.is_empty());
}

#[test]
fn error_report_fallback_ignores_a_broken_stderr_writer() {
    // Catches eprintln-style panic behavior when reporting the original error
    // is itself impossible.
    let runner = RecordingRunner::returning(Vec::new());
    let exit = run_with_io(
        Cli {
            config: Some("/definitely/missing/mac-worker.toml".into()),
            json: false,
            command: Command::Workers,
        },
        &runner,
        &mut Vec::new(),
        &mut BrokenWriter,
    );

    assert_eq!(exit, 64);
}
