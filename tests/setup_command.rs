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
    error::{ProcessError, WorkerError},
    execute_with,
    install::{Installer, prepare_candidate},
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
const CANDIDATE_BYTES: &[u8] = concat!(
    "#!/bin/sh\n",
    "if [ \"$1\" = host ] && [ \"$2\" = complete-protocol-upgrade ]; then\n",
    "  /bin/mv \"$0\" \"$3\"\n",
    "  exit $?\n",
    "fi\n",
    "if [ \"$1\" = host ] && [ \"$2\" = complete-unverified-rollback ]; then\n",
    "  if [ -n \"$4\" ]; then\n",
    "    /bin/mv \"$4\" \"$3\"\n",
    "    exit $?\n",
    "  fi\n",
    "  /bin/rm -f \"$3\"\n",
    "  exit $?\n",
    "fi\n",
    "exit 0\n"
)
.as_bytes();
const CANDIDATE_DIGEST: &str = "306135cb221abbb5a44247360fe2caef4db945fab12d58c29f134658ae318716";

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
        // Setup lists the laptop process table through this runner. An empty
        // table keeps byte-exact reports independent of a live dashboard.
        if request.program == "/bin/ps" {
            return Ok(result(0, b"", b""));
        }
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

fn result(code: i32, stdout: impl AsRef<[u8]>, stderr: impl AsRef<[u8]>) -> ProcessResult {
    ProcessResult {
        status: ExitStatus::from_raw(code << 8),
        stdout: stdout.as_ref().to_vec(),
        stderr: stderr.as_ref().to_vec(),
    }
}

fn valid_probe_json() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "protocol_version": PROTOCOL_VERSION,
        "supervision_version": mac_worker::protocol::SUPERVISION_VERSION,
        "hostname": "mini-1.local",
        "arch": "arm64",
        "os_version": "26.2",
        "free_disk_bytes": 536_870_912_u64,
        "total_disk_bytes": 1_073_741_824_u64,
        "memory_pressure": "normal",
        "swap_used_bytes": 134_217_728_u64,
        "available_memory_bytes": 12 * 1024 * 1024 * 1024_u64,
        "cpu_counters": {
            "user_ticks": 10,
            "system_ticks": 20,
            "idle_ticks": 30,
            "nice_ticks": 40,
        },
        "slot_state": "idle",
        "active_lease": null,
        "capabilities": ["darwin-arm64"],
    }))
    .unwrap()
}

#[test]
fn canonical_setup_probe_fixture_carries_all_v3_scheduler_and_dashboard_facts() {
    // Omitting either v3 fact here would leave setup verification untested against its full probe.
    let probe: serde_json::Value = serde_json::from_slice(&valid_probe_json()).unwrap();

    assert_eq!(probe["available_memory_bytes"], 12 * 1024 * 1024 * 1024_u64);
    assert_eq!(
        probe["cpu_counters"],
        serde_json::json!({
            "user_ticks": 10,
            "system_ticks": 20,
            "idle_ticks": 30,
            "nice_ticks": 40,
        })
    );
}

#[test]
fn setup_refreshes_agent_facts_after_warmup_and_before_the_verification_probe() {
    // Facts collection launches every agent and must not share the 15 s probe
    // deadline. migrate-layout stays in this locked step so an outdated
    // layout still fails the install; verification only probes.
    let commands = generated_setup_commands();
    let facts_refresh = &commands.facts_refresh;
    let verification = &commands.verification;

    assert!(
        facts_refresh.contains("\"$worker_path\" host migrate-layout"),
        "facts refresh must migrate layout: {facts_refresh}"
    );
    assert!(
        facts_refresh.contains("\"$worker_path\" host refresh-facts"),
        "facts refresh must collect agent facts: {facts_refresh}"
    );
    assert!(
        !facts_refresh.contains("host probe"),
        "facts refresh must not run the verification probe: {facts_refresh}"
    );
    assert!(
        !verification.contains("host refresh-facts"),
        "verification must not refresh facts: {verification}"
    );
    assert!(
        !verification.contains("host migrate-layout"),
        "verification must not migrate layout: {verification}"
    );
    assert!(
        verification.contains("exec \"$worker_path\" host probe"),
        "verification must finish with a probe: {verification}"
    );

    let (_directory, current_exe) = executable_fixture();
    let runner = RecordingRunner::returning_results(success_results());
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());
    assert!(installer.install(&current_exe, &worker()).installed);
    let requests = runner.requests();
    let ssh: Vec<&ProcessRequest> = requests
        .iter()
        .filter(|request| request.program == "/usr/bin/ssh")
        .collect();
    let warmup_at = ssh
        .iter()
        .position(|request| request_command(request) == commands.warmup)
        .expect("warm-up must be issued");
    let facts_refresh_at = ssh
        .iter()
        .position(|request| request_command(request) == *facts_refresh)
        .expect("facts refresh must be issued");
    let verification_at = ssh
        .iter()
        .position(|request| request_command(request) == *verification)
        .expect("verification must be issued");

    assert!(warmup_at < facts_refresh_at);
    assert!(facts_refresh_at < verification_at);
    assert_eq!(ssh[facts_refresh_at].policy, facts_refresh_policy());
}

#[test]
fn setup_promotes_through_complete_protocol_upgrade_not_a_post_drain_mv() {
    let commands = generated_setup_commands();
    assert!(
        commands.promotion.contains(
            "\"$transaction/worker.new\" host complete-protocol-upgrade \"$worker_path\""
        ),
        "promotion must invoke the candidate fence: {}",
        commands.promotion
    );
    assert!(
        !commands.promotion.contains("/bin/mv "),
        "promotion must not shell-mv the helper: {}",
        commands.promotion
    );
    assert!(
        commands
            .rollback
            .contains("\"$worker_path\" host complete-unverified-rollback \"$worker_path\""),
        "rollback must invoke the unverified-rollback fence: {}",
        commands.rollback
    );
    assert!(
        !commands.rollback.contains("/bin/mv "),
        "rollback must not shell-mv the previous helper: {}",
        commands.rollback
    );
}

#[test]
fn setup_warms_the_promoted_helper_with_version_before_the_verification_probe() {
    // Gatekeeper's first-launch assessment must not share the 15 s probe
    // deadline used later by `worker workers`. `--version` is the cheap
    // launch that does not migrate layout or refresh facts.
    let commands = generated_setup_commands();
    let warmup = &commands.warmup;
    let verification = &commands.verification;

    assert!(
        warmup.contains("\"$worker_path\" --version"),
        "warm-up must launch the promoted helper: {warmup}"
    );
    assert!(
        !warmup.contains("host migrate-layout"),
        "warm-up must not touch host layout: {warmup}"
    );
    assert!(
        !warmup.contains("host refresh-facts"),
        "warm-up must not refresh facts: {warmup}"
    );
    assert!(
        !warmup.contains("host probe"),
        "warm-up must not run the verification probe: {warmup}"
    );

    let (_directory, current_exe) = executable_fixture();
    let runner = RecordingRunner::returning_results(success_results());
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());
    assert!(installer.install(&current_exe, &worker()).installed);
    let requests = runner.requests();
    let ssh: Vec<&ProcessRequest> = requests
        .iter()
        .filter(|request| request.program == "/usr/bin/ssh")
        .collect();
    let warmup_at = ssh
        .iter()
        .position(|request| request_command(request) == *warmup)
        .expect("warm-up must be issued");
    let verification_at = ssh
        .iter()
        .position(|request| request_command(request) == *verification)
        .expect("verification must be issued");
    let promotion_at = ssh
        .iter()
        .position(|request| request_command(request) == commands.promotion)
        .expect("promotion must be issued");

    assert!(promotion_at < warmup_at);
    assert!(warmup_at < verification_at);
    assert_eq!(ssh[warmup_at].policy, warmup_policy());
}

fn expected_setup_json(expected: &[u8]) -> Vec<u8> {
    let digest = mac_worker::binary_identity::sha256_hex(
        &fs::read(std::env::current_exe().unwrap()).unwrap(),
    );
    String::from_utf8(expected.to_vec())
        .unwrap()
        .replacen(
            "\"protocol_version\":1",
            &format!("\"protocol_version\":{PROTOCOL_VERSION}"),
            1,
        )
        .replace(
            "\"warnings\":[]}",
            &format!("\"warnings\":[],\"binary_sha256\":\"{digest}\"}}"),
        )
        .into_bytes()
}

fn worker() -> WorkerEntry {
    WorkerEntry {
        name: "mini-1".into(),
        ssh: "mac1".into(),
        slots: 1,
        capabilities: vec!["darwin-arm64".into()],
        remote_binary: "~/.local/bin/worker".into(),
        herdr: false,
    }
}

fn executable_fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let directory = tempdir().unwrap();
    let current_exe = directory.path().join("worker");
    fs::write(&current_exe, CANDIDATE_BYTES).unwrap();
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

fn warmup_policy() -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: 64 * 1024,
        stderr_limit: 64 * 1024,
        deadline: Duration::from_secs(90),
    }
}

fn facts_refresh_policy() -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: 64 * 1024,
        stderr_limit: 64 * 1024,
        deadline: Duration::from_secs(120),
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
        Ok(result(0, b"worker 0.1.0\n", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"not_enabled\n", b"")),
    ]
}

struct GeneratedSetupCommands {
    acquire: String,
    upload: ProcessRequest,
    digest: String,
    prepare: String,
    promotion: String,
    reconciliation: String,
    warmup: String,
    facts_refresh: String,
    verification: String,
    success_cleanup: String,
    cleanup: String,
    rollback: String,
    outbox_wake: String,
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
    rollback_results[9] = Ok(result(255, b"", b"candidate failed"));
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
        warmup: request_command(&requests[7]),
        facts_refresh: request_command(&requests[8]),
        verification: request_command(&requests[9]),
        success_cleanup: request_command(&requests[10]),
        cleanup: request_command(&cleanup_requests[3]),
        rollback: request_command(&rollback_requests[10]),
        outbox_wake: request_command(&requests[11]),
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
    fs::write(&active, CANDIDATE_BYTES).unwrap();
    fs::set_permissions(&active, fs::Permissions::from_mode(0o755)).unwrap();
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
            command: Command::Setup {
                hosts: Vec::new(),
                allow_debug: false,
            },
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

    assert_upload_request_shape(&commands.upload, CANDIDATE_BYTES);
    let output = run_remote_request(&commands.upload, &home);

    assert!(output.status.success(), "{:?}", output.stderr);
    let transaction = home
        .join(".local/share/mac-worker/setup")
        .join(INSTALLATION_ID);
    let staged = transaction.join("worker.new");
    assert_eq!(fs::read(&staged).unwrap(), CANDIDATE_BYTES);
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

    assert_upload_request_shape(&commands.upload, CANDIDATE_BYTES);
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
    assert_upload_request_shape(&requests[2], CANDIDATE_BYTES);
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
        CANDIDATE_BYTES
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
            CANDIDATE_BYTES
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
        CANDIDATE_BYTES
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
    fs::write(transaction.join("worker.new"), CANDIDATE_BYTES).unwrap();

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

    assert_eq!(fs::read(&active).unwrap(), CANDIDATE_BYTES);
    assert!(!transaction.exists());
    assert!(
        !home
            .join(".local/share/mac-worker/setup/.install-lock")
            .exists()
    );
}

#[test]
fn successful_install_keeps_one_verified_previous_helper() {
    // The previous helper used to be deleted with the transaction, so a bad
    // promotion left nothing to copy back.
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
    let retained = home.join(".local/bin/worker.previous");
    fs::write(&active, b"previous worker").unwrap();
    fs::write(transaction.join("worker.new"), CANDIDATE_BYTES).unwrap();
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
    fs::write(&retained, b"stale older helper").unwrap();

    assert!(
        run_remote_command(&commands.success_cleanup, &home)
            .status
            .success()
    );

    assert_eq!(fs::read(&active).unwrap(), CANDIDATE_BYTES);
    assert_eq!(fs::read(&retained).unwrap(), b"previous worker");
    assert!(!transaction.exists());
    assert!(!home.join(".local/bin/worker.previous.new").exists());
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
    fs::write(transaction.join("worker.new"), CANDIDATE_BYTES).unwrap();
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
    fs::write(transaction.join("worker.new"), CANDIDATE_BYTES).unwrap();
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
    fs::write(transaction.join("worker.new"), CANDIDATE_BYTES).unwrap();
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
        CANDIDATE_BYTES
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
    fs::write(outside.join("worker"), CANDIDATE_BYTES).unwrap();
    symlink(&outside, home.join(".local/bin")).unwrap();

    let output = run_remote_command(&commands.rollback, &home);

    assert!(!output.status.success());
    assert_eq!(fs::read(outside.join("worker")).unwrap(), CANDIDATE_BYTES);
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
        assert_eq!(fs::read(&active).unwrap(), CANDIDATE_BYTES);
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
        assert_eq!(fs::read(&active).unwrap(), CANDIDATE_BYTES);
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
    assert_eq!(installed.protocol_version, Some(PROTOCOL_VERSION));
    assert!(installed.warnings.is_empty());
    let requests = runner.requests();
    assert_eq!(requests.len(), 12);
    assert_eq!(
        requests[0],
        ProcessRequest {
            program: current_exe.clone().into_os_string(),
            args: vec!["host".into(), "probe".into()],
            environment: Vec::new(),
            environment_remove: Vec::new(),
            stdin: None,
            policy: preflight_policy(),
            isolate_parent_environment: false,
        }
    );
    assert_ssh_request_shape(&requests[1], control_policy());
    assert_upload_request_shape(&requests[2], CANDIDATE_BYTES);
    for request in [&requests[3], &requests[4], &requests[5], &requests[6]] {
        assert_ssh_request_shape(request, control_policy());
    }
    assert_ssh_request_shape(&requests[7], warmup_policy());
    assert_ssh_request_shape(&requests[8], facts_refresh_policy());
    assert_ssh_request_shape(&requests[9], preflight_policy());
    assert_ssh_request_shape(&requests[10], control_policy());
    assert_ssh_request_shape(&requests[11], control_policy());
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
    assert_eq!(requests.len(), 12);
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
        result(0, b"worker 0.1.0\n", b""),
        result(0, b"", b""),
        result(255, b"", b"candidate failed"),
        result(0, b"", b""),
    ]);
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(!installed.installed);
    assert_eq!(installed.error_code.as_deref(), Some("VERIFICATION_FAILED"));
    assert!(installed.warnings.is_empty());
    let requests = runner.requests();
    assert_eq!(requests.len(), 11);
    let commands = generated_setup_commands();
    assert_remote_command(&requests[7], &commands.warmup);
    assert_remote_command(&requests[8], &commands.facts_refresh);
    assert_remote_command(&requests[9], &commands.verification);
    assert_ssh_request_shape(&requests[10], control_policy());
    assert_remote_command(&requests[10], &commands.rollback);
}

#[test]
fn a_failed_or_stalled_warmup_does_not_fail_setup_when_verification_passes() {
    // Warm-up exists only to absorb Gatekeeper's first launch; verification
    // still decides, and a diagnostic warning is the only leftover.
    let (_directory, current_exe) = executable_fixture();
    for (label, warmup) in [
        (
            "nonzero",
            Ok(result(1, b"", b"gatekeeper assessment failed")),
        ),
        (
            "deadline",
            Err(WorkerError::Process(ProcessError::DeadlineExceeded {
                deadline: Duration::from_secs(90),
            })),
        ),
    ] {
        let mut results = success_results();
        results[7] = warmup;
        let runner = RecordingRunner::returning_results(results);
        let installer =
            Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

        let installed = installer.install(&current_exe, &worker());

        assert!(installed.installed, "{label}");
        assert_eq!(
            installed.protocol_version,
            Some(PROTOCOL_VERSION),
            "{label}"
        );
        assert_eq!(installed.error_code, None, "{label}");
        assert_eq!(installed.warnings.len(), 1, "{label}");
        assert_eq!(
            installed.warnings[0].code,
            SetupWarningCode::WarmupFailed,
            "{label}"
        );
        let requests = runner.requests();
        let commands = generated_setup_commands();
        assert_remote_command(&requests[7], &commands.warmup);
        assert_remote_command(&requests[8], &commands.facts_refresh);
        assert_remote_command(&requests[9], &commands.verification);
        assert_remote_command(&requests[10], &commands.success_cleanup);
        assert_remote_command(&requests[11], &commands.outbox_wake);
        assert!(!contains_remote_command(&requests, &commands.rollback));
        assert_eq!(installed.outbox.as_deref(), Some("not_enabled"), "{label}");
    }
}

#[test]
fn failed_verification_after_warmup_failure_still_rolls_back_and_keeps_warmup_on_the_message() {
    // A warm-up miss must not change rollback, become a warning on the
    // failure path, or hide the verification cause.
    let (_directory, current_exe) = executable_fixture();
    let runner = RecordingRunner::returning_results(vec![
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"match\n", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"promoted\n", b"")),
        Err(WorkerError::Process(ProcessError::DeadlineExceeded {
            deadline: Duration::from_secs(90),
        })),
        Ok(result(0, b"", b"")),
        Ok(result(255, b"", b"candidate failed")),
        Ok(result(0, b"", b"")),
    ]);
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(!installed.installed);
    assert_eq!(installed.error_code.as_deref(), Some("VERIFICATION_FAILED"));
    let message = installed.error_message.as_deref().unwrap();
    assert!(message.contains("candidate failed"), "{message}");
    assert!(message.contains("first-launch warm-up:"), "{message}");
    assert!(
        message.contains("process exceeded its 90s execution deadline"),
        "{message}"
    );
    assert!(installed.warnings.is_empty());
    let requests = runner.requests();
    let commands = generated_setup_commands();
    assert_eq!(requests.len(), 11);
    assert_remote_command(&requests[7], &commands.warmup);
    assert_remote_command(&requests[8], &commands.facts_refresh);
    assert_remote_command(&requests[9], &commands.verification);
    assert_remote_command(&requests[10], &commands.rollback);
}

#[test]
fn a_failed_or_stalled_facts_refresh_does_not_fail_setup_when_verification_passes() {
    // Facts collection is allowed to miss its generous deadline; verification
    // still decides, and the operator is told to retry with --refresh.
    let (_directory, current_exe) = executable_fixture();
    for (label, facts_refresh) in [
        ("nonzero", Ok(result(1, b"", b"agent version probe failed"))),
        (
            "deadline",
            Err(WorkerError::Process(ProcessError::DeadlineExceeded {
                deadline: Duration::from_secs(120),
            })),
        ),
    ] {
        let mut results = success_results();
        results[8] = facts_refresh;
        let runner = RecordingRunner::returning_results(results);
        let installer =
            Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

        let installed = installer.install(&current_exe, &worker());

        assert!(installed.installed, "{label}");
        assert_eq!(
            installed.protocol_version,
            Some(PROTOCOL_VERSION),
            "{label}"
        );
        assert_eq!(installed.error_code, None, "{label}");
        assert_eq!(installed.warnings.len(), 1, "{label}");
        assert_eq!(
            installed.warnings[0].code,
            SetupWarningCode::FactsRefreshFailed,
            "{label}"
        );
        assert!(
            installed.warnings[0]
                .message
                .contains("worker workers --refresh"),
            "{label}: {}",
            installed.warnings[0].message
        );
        let requests = runner.requests();
        let commands = generated_setup_commands();
        assert_remote_command(&requests[7], &commands.warmup);
        assert_remote_command(&requests[8], &commands.facts_refresh);
        assert_remote_command(&requests[9], &commands.verification);
        assert_remote_command(&requests[10], &commands.success_cleanup);
        assert_remote_command(&requests[11], &commands.outbox_wake);
        assert!(!contains_remote_command(&requests, &commands.rollback));
        assert_eq!(installed.outbox.as_deref(), Some("not_enabled"), "{label}");
    }
}

#[test]
fn failed_layout_migration_during_facts_refresh_rolls_back_the_promoted_helper() {
    // Exit 77 from the facts-refresh script is migrate-layout or a locked
    // state invariant, the same class verification used to fail on.
    let (_directory, current_exe) = executable_fixture();
    let mut results = success_results();
    results[8] = Ok(result(77, b"", b"migrate-layout refused"));
    let runner = RecordingRunner::returning_results(results);
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(!installed.installed);
    assert_eq!(installed.error_code.as_deref(), Some("VERIFICATION_FAILED"));
    assert!(
        installed
            .error_message
            .as_deref()
            .unwrap()
            .contains("migrate-layout refused")
    );
    assert!(installed.warnings.is_empty());
    let requests = runner.requests();
    let commands = generated_setup_commands();
    assert_eq!(requests.len(), 10);
    assert_remote_command(&requests[7], &commands.warmup);
    assert_remote_command(&requests[8], &commands.facts_refresh);
    assert_remote_command(&requests[9], &commands.rollback);
    assert!(!contains_remote_command(&requests, &commands.verification));
}

#[test]
fn verified_install_with_cleanup_failure_stays_installed_with_typed_warning() {
    // Catches calling a byte-matched, verified install failed solely because
    // targeted cleanup did not finish.
    let (_directory, current_exe) = executable_fixture();
    let mut results = success_results();
    results[10] = Ok(result(1, b"", b"lock release failed"));
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
    assert_eq!(installed.outbox.as_deref(), Some("not_enabled"));
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
    let mut second_host = success_results();
    let _ = second_host.remove(0);
    let mut results = vec![
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(75, b"", b"lock busy")),
    ];
    results.extend(second_host);
    let runner = RecordingRunner::returning_results(results);
    let cli = Cli {
        config: Some(config_path),
        json: true,
        command: Command::Setup {
            hosts: vec!["first".into(), "second".into()],
            allow_debug: false,
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
            command: Command::Setup {
                hosts: Vec::new(),
                allow_debug: false,
            },
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
                allow_debug: false,
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
    assert_eq!(probe.protocol_version, PROTOCOL_VERSION);
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
                outbox: None,
                controller_service: None,
                build_id: None,
                binary_sha256: None,
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
                outbox: None,
                controller_service: None,
                build_id: None,
                binary_sha256: None,
            },
        ],
        warnings: Vec::new(),
    };
    let output = CommandOutput::Setup(report);

    assert_eq!(
        output.render_human(),
        format!(
            "mini-1: installed (protocol {PROTOCOL_VERSION})\nmini-2: failed [INSTALL_FAILED]: transfer failed"
        )
    );
    assert_eq!(
        output.render_json().unwrap(),
        format!(
            r#"{{"kind":"setup","protocol_version":{PROTOCOL_VERSION},"workers":[{{"name":"mini-1","ssh":"mac1","installed":true,"protocol_version":{PROTOCOL_VERSION},"error_code":null,"error_message":null,"warnings":[]}},{{"name":"mini-2","ssh":"mac2","installed":false,"protocol_version":null,"error_code":"INSTALL_FAILED","error_message":"transfer failed","warnings":[]}}]}}"#
        )
    );
}

#[test]
fn setup_wakes_the_outbox_after_a_successful_install_and_renders_the_outcome() {
    let commands = generated_setup_commands();
    assert!(
        commands.outbox_wake.contains("host outbox --wake"),
        "setup must invoke the installed helper: {}",
        commands.outbox_wake
    );
    assert!(
        commands.outbox_wake.contains("not_enabled"),
        "setup must report not_enabled when the worker never enabled the outbox: {}",
        commands.outbox_wake
    );
    assert!(
        !commands.outbox_wake.contains("launchctl"),
        "setup must not call launchctl: {}",
        commands.outbox_wake
    );

    let (_directory, current_exe) = executable_fixture();
    for (label, stdout, expected, warning) in [
        (
            "not_enabled",
            b"not_enabled\n".as_slice(),
            "not_enabled",
            false,
        ),
        ("woken", b"woken\n".as_slice(), "woken", false),
        ("restarted", b"restarted\n".as_slice(), "restarted", false),
        ("failed", b"".as_slice(), "failed OUTBOX_WAKE_FAILED", true),
    ] {
        let mut results = success_results();
        results[11] = if warning {
            Ok(result(
                70,
                stdout,
                b"outbox watcher did not acknowledge launch",
            ))
        } else {
            Ok(result(0, stdout, b""))
        };
        let runner = RecordingRunner::returning_results(results);
        let installer =
            Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());
        let installed = installer.install(&current_exe, &worker());
        assert!(installed.installed, "{label}");
        assert_eq!(installed.outbox.as_deref(), Some(expected), "{label}");
        if warning {
            assert!(
                installed
                    .warnings
                    .iter()
                    .any(|item| item.code == SetupWarningCode::OutboxWakeFailed),
                "{label}: {:?}",
                installed.warnings
            );
        } else {
            assert!(
                !installed
                    .warnings
                    .iter()
                    .any(|item| item.code == SetupWarningCode::OutboxWakeFailed),
                "{label}"
            );
        }
        let requests = runner.requests();
        assert_remote_command(&requests[11], &commands.outbox_wake);
    }

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
                code: SetupWarningCode::OutboxWakeFailed,
                message: "outbox wake failed with exit 70".into(),
            }],
            outbox: Some("failed OUTBOX_WAKE_FAILED".into()),
            controller_service: None,
            build_id: None,
            binary_sha256: None,
        }],
        warnings: Vec::new(),
    });
    assert_eq!(
        output.render_human(),
        format!(
            "mini-1: installed (protocol {PROTOCOL_VERSION})\n  outbox: failed OUTBOX_WAKE_FAILED\n  warning [OUTBOX_WAKE_FAILED]: outbox wake failed with exit 70"
        )
    );
    let json: serde_json::Value = serde_json::from_str(&output.render_json().unwrap()).unwrap();
    assert_eq!(json["workers"][0]["outbox"], "failed OUTBOX_WAKE_FAILED");
    assert_eq!(json["protocol_version"], PROTOCOL_VERSION);
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
            outbox: None,
            controller_service: None,
            build_id: None,
            binary_sha256: None,
        }],
        warnings: Vec::new(),
    });

    assert_eq!(
        output.render_human(),
        format!(
            "mini-1: installed (protocol {PROTOCOL_VERSION})\n  warning [CLEANUP_FAILED]: lock release failed"
        )
    );
}

#[test]
fn setup_human_output_surfaces_warmup_warning_after_verified_success() {
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
                code: SetupWarningCode::WarmupFailed,
                message: "process exceeded its 90s execution deadline".into(),
            }],
            outbox: None,
            controller_service: None,
            build_id: None,
            binary_sha256: None,
        }],
        warnings: Vec::new(),
    });

    assert_eq!(
        output.render_human(),
        format!(
            "mini-1: installed (protocol {PROTOCOL_VERSION})\n  warning [WARMUP_FAILED]: process exceeded its 90s execution deadline"
        )
    );
}

#[test]
fn setup_human_output_surfaces_facts_refresh_warning_after_verified_success() {
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
                code: SetupWarningCode::FactsRefreshFailed,
                message:
                    "agent facts were not refreshed during setup; run `worker workers --refresh`"
                        .into(),
            }],
            outbox: None,
            controller_service: None,
            build_id: None,
            binary_sha256: None,
        }],
        warnings: Vec::new(),
    });

    assert_eq!(
        output.render_human(),
        format!(
            "mini-1: installed (protocol {PROTOCOL_VERSION})\n  warning [FACTS_REFRESH_FAILED]: agent facts were not refreshed during setup; run `worker workers --refresh`"
        )
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
            command: Command::Setup {
                hosts: Vec::new(),
                allow_debug: false,
            },
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
        result(75, b"", b"lock busy"),
    ]);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_with_io(
        Cli {
            config: Some(config_path),
            json: true,
            command: Command::Setup {
                hosts: Vec::new(),
                allow_debug: false,
            },
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
            command: Command::Setup {
                hosts: Vec::new(),
                allow_debug: false,
            },
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
            command: Command::Setup {
                hosts: Vec::new(),
                allow_debug: false,
            },
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
        expected_setup_json(b"{\"kind\":\"setup\",\"protocol_version\":1,\"workers\":[{\"name\":\"mini-1\",\"ssh\":\"mac1\",\"installed\":false,\"protocol_version\":null,\"error_code\":\"UNKNOWN_INSTALLATION_STATE\",\"error_message\":\"installation lock acquisition result was lost; scoped state was retained: I/O error: acquisition result read failed\",\"warnings\":[]}]}\n")
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
        expected_setup_json(b"{\"kind\":\"setup\",\"protocol_version\":1,\"workers\":[{\"name\":\"mini-1\",\"ssh\":\"mac1\",\"installed\":false,\"protocol_version\":null,\"error_code\":\"DIGEST_VERIFICATION_FAILED\",\"error_message\":\"failed to verify staged candidate digest: I/O error: digest result read failed\",\"warnings\":[]}]}\n")
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
        expected_setup_json(b"{\"kind\":\"setup\",\"protocol_version\":1,\"workers\":[{\"name\":\"mini-1\",\"ssh\":\"mac1\",\"installed\":false,\"protocol_version\":null,\"error_code\":\"INSTALL_FAILED\",\"error_message\":\"failed to launch /usr/bin/ssh: I/O error: prepare result read failed\",\"warnings\":[]}]}\n")
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
        expected_setup_json(b"{\"kind\":\"setup\",\"protocol_version\":1,\"workers\":[{\"name\":\"mini-1\",\"ssh\":\"mac1\",\"installed\":false,\"protocol_version\":null,\"error_code\":\"UNKNOWN_INSTALLATION_STATE\",\"error_message\":\"failed to launch /usr/bin/ssh: I/O error: promotion result read failed; the previous target is currently observable but promotion completion is unproven; installation lock and scoped state were retained\",\"warnings\":[]}]}\n")
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
        expected_setup_json(b"{\"kind\":\"setup\",\"protocol_version\":1,\"workers\":[{\"name\":\"mini-1\",\"ssh\":\"mac1\",\"installed\":false,\"protocol_version\":null,\"error_code\":\"UNKNOWN_INSTALLATION_STATE\",\"error_message\":\"promotion reported success but reconciliation could not prove completion: I/O error: reconciliation result read failed; installation lock and scoped state were retained\",\"warnings\":[]}]}\n")
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
        Ok(result(0, b"worker 0.1.0\n", b"")),
        Ok(result(0, b"", b"")),
        Err(WorkerError::Io(std::io::Error::other(
            "verification result read failed",
        ))),
        Ok(result(0, b"", b"")),
    ]);

    assert_eq!(exit, 74);
    assert!(stderr.is_empty());
    assert_eq!(
        stdout,
        expected_setup_json(b"{\"kind\":\"setup\",\"protocol_version\":1,\"workers\":[{\"name\":\"mini-1\",\"ssh\":\"mac1\",\"installed\":false,\"protocol_version\":null,\"error_code\":\"VERIFICATION_FAILED\",\"error_message\":\"failed to launch SSH probe: I/O error: verification result read failed\",\"warnings\":[]}]}\n")
    );
    assert_eq!(requests.len(), 11);
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
            command: Command::Setup {
                hosts: Vec::new(),
                allow_debug: false,
            },
        },
        &runner,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, 74);
    assert!(stderr.is_empty());
    assert_eq!(
        stdout,
        expected_setup_json(b"{\"kind\":\"setup\",\"protocol_version\":1,\"workers\":[{\"name\":\"mini-1\",\"ssh\":\"mac1\",\"installed\":false,\"protocol_version\":null,\"error_code\":\"TRANSFER_FAILED\",\"error_message\":\"failed to launch /usr/bin/ssh: I/O error: upload result read failed\",\"warnings\":[]}]}\n")
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
            command: Command::Setup {
                hosts: Vec::new(),
                allow_debug: false,
            },
        },
        &runner,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, 69);
    assert!(stderr.is_empty());
    assert_eq!(
        stdout,
        expected_setup_json(b"{\"kind\":\"setup\",\"protocol_version\":1,\"workers\":[{\"name\":\"mini-1\",\"ssh\":\"mac1\",\"installed\":false,\"protocol_version\":null,\"error_code\":\"TRANSFER_FAILED\",\"error_message\":\"/usr/bin/ssh failed with exit 1: transfer interrupted\",\"warnings\":[]}]}\n")
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
        Ok(result(0, b"", b"")),
        Ok(result(0, b"", b"")),
        Ok(result(0, b"mismatch\n", b"")),
        Ok(result(0, b"", b"")),
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
            command: Command::Setup {
                hosts: Vec::new(),
                allow_debug: false,
            },
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
    let mut second_host = success_results();
    let _ = second_host.remove(0);
    let mut results = vec![
        Ok(result(0, valid_probe_json(), b"")),
        Ok(result(75, b"", b"lock busy")),
    ];
    results.extend(second_host);
    let runner = RecordingRunner::returning_results(results);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_with_io(
        Cli {
            config: Some(config_path),
            json: true,
            command: Command::Setup {
                hosts: Vec::new(),
                allow_debug: false,
            },
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
        warnings: Vec::new(),
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
            command: Command::Setup {
                hosts: Vec::new(),
                allow_debug: false,
            },
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
        warnings: Vec::new(),
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
            command: Command::Workers {
                refresh: false,
                clear_auth_incidents: false,
            },
        },
        &runner,
        &mut Vec::new(),
        &mut BrokenWriter,
    );

    assert_eq!(exit, 64);
}

fn probe_json_with_herdr(herdr: Option<serde_json::Value>, facts_age_millis: u64) -> Vec<u8> {
    let mut probe: serde_json::Value = serde_json::from_slice(&valid_probe_json()).unwrap();
    let mut facts = serde_json::json!({
        "agents": [],
        "env_profiles": [],
        "git_identity": true,
        "collected_at_millis": 10,
    });
    if let Some(herdr) = herdr {
        facts["herdr"] = herdr;
    }
    probe["agent_facts"] = facts;
    probe["facts_age_millis"] = serde_json::json!(facts_age_millis);
    serde_json::to_vec(&probe).unwrap()
}

fn herdr_worker() -> WorkerEntry {
    WorkerEntry {
        herdr: true,
        ..worker()
    }
}

/// A byte-matched, verified install whose final verification probe answers
/// with `probe`.
fn install_with_verification_probe(probe: Vec<u8>, worker: &WorkerEntry) -> SetupHostResult {
    let (_directory, current_exe) = executable_fixture();
    let mut results = success_results();
    results[9] = Ok(result(0, probe, b""));
    let runner = RecordingRunner::returning_results(results);
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());
    installer.install(&current_exe, worker)
}

fn herdr_unavailable_warning() -> SetupWarning {
    SetupWarning {
        code: SetupWarningCode::HerdrUnavailable,
        message: mac_worker::protocol::HERDR_UNAVAILABLE_MESSAGE.into(),
    }
}

#[test]
fn verified_install_with_herdr_requested_but_unavailable_stays_installed_with_typed_warning() {
    // Spec 5.3: `herdr = true` with any fact but `available` is a setup
    // warning; the host still reports installed with its protocol version.
    use mac_worker::agent_facts::FACTS_TTL;

    for (label, probe) in [
        (
            "not installed",
            probe_json_with_herdr(Some(serde_json::json!({ "state": "not_installed" })), 0),
        ),
        (
            "no socket",
            probe_json_with_herdr(
                Some(serde_json::json!({ "state": "no_socket", "version": "0.9.0" })),
                0,
            ),
        ),
        (
            "no response",
            probe_json_with_herdr(
                Some(serde_json::json!({ "state": "no_response", "version": "0.9.0" })),
                0,
            ),
        ),
        ("facts predating the fact", probe_json_with_herdr(None, 0)),
        (
            "stale facts",
            probe_json_with_herdr(
                Some(serde_json::json!({ "state": "available", "version": "0.9.0" })),
                FACTS_TTL + 1,
            ),
        ),
        ("no facts", valid_probe_json()),
    ] {
        let installed = install_with_verification_probe(probe, &herdr_worker());

        assert!(installed.installed, "{label}");
        assert_eq!(
            installed.protocol_version,
            Some(PROTOCOL_VERSION),
            "{label}"
        );
        assert_eq!(installed.error_code, None, "{label}");
        assert_eq!(installed.error_message, None, "{label}");
        assert_eq!(
            installed.warnings,
            vec![herdr_unavailable_warning()],
            "{label}"
        );
    }
}

#[test]
fn verified_install_carries_no_herdr_warning_when_available_or_not_requested() {
    let available = probe_json_with_herdr(
        Some(serde_json::json!({ "state": "available", "version": "0.9.0" })),
        0,
    );
    let installed = install_with_verification_probe(available, &herdr_worker());
    assert!(installed.installed);
    assert!(installed.warnings.is_empty(), "{:?}", installed.warnings);

    for probe in [
        probe_json_with_herdr(Some(serde_json::json!({ "state": "not_installed" })), 0),
        valid_probe_json(),
    ] {
        let installed = install_with_verification_probe(probe, &worker());
        assert!(installed.installed);
        assert!(installed.warnings.is_empty(), "{:?}", installed.warnings);
    }
}

#[test]
fn setup_output_surfaces_the_herdr_warning_after_installed_in_text_and_json() {
    let message = mac_worker::protocol::HERDR_UNAVAILABLE_MESSAGE;
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
            warnings: vec![herdr_unavailable_warning()],
            outbox: None,
            controller_service: None,
            build_id: None,
            binary_sha256: None,
        }],
        warnings: Vec::new(),
    });

    assert_eq!(
        output.render_human(),
        format!(
            "mini-1: installed (protocol {PROTOCOL_VERSION})\n  warning [HERDR_UNAVAILABLE]: {message}"
        )
    );
    let json: serde_json::Value = serde_json::from_str(&output.render_json().unwrap()).unwrap();
    assert_eq!(json["workers"][0]["installed"], true);
    assert_eq!(
        json["workers"][0]["warnings"],
        serde_json::json!([{ "code": "HERDR_UNAVAILABLE", "message": message }])
    );
}

#[test]
fn setup_human_and_json_output_surface_an_outdated_laptop_binary_warning() {
    let message = "worker dashboard (pid 7) was started before the installed binary; restart it";
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
            warnings: Vec::new(),
            outbox: None,
            controller_service: None,
            build_id: None,
            binary_sha256: None,
        }],
        warnings: vec![SetupWarning {
            code: SetupWarningCode::LaptopBinaryOutdated,
            message: message.into(),
        }],
    });

    assert_eq!(
        output.render_human(),
        format!(
            "mini-1: installed (protocol {PROTOCOL_VERSION})\nwarning [LAPTOP_BINARY_OUTDATED]: {message}"
        )
    );
    let json: serde_json::Value = serde_json::from_str(&output.render_json().unwrap()).unwrap();
    assert_eq!(json["warnings"][0]["code"], "LAPTOP_BINARY_OUTDATED");
    assert_eq!(json["warnings"][0]["message"], message);
}

#[test]
fn setup_reads_the_candidate_once_and_installs_identical_bytes_on_every_host() {
    let (directory, current_exe) = executable_fixture();
    let per_host = || {
        let mut results = success_results();
        let _ = results.remove(0);
        results
    };
    let mut results = vec![Ok(result(0, valid_probe_json(), b""))];
    results.extend(per_host());
    results.extend(per_host());
    let inner = RecordingRunner::returning_results(results);
    let runner = CandidateMutatingRunner {
        inner: inner.clone(),
        candidate: current_exe.clone(),
    };
    let candidate = prepare_candidate(&runner, &current_exe, false).unwrap();
    let hosts = [
        worker(),
        WorkerEntry {
            name: "mini-2".into(),
            ssh: "mac2".into(),
            ..worker()
        },
    ];
    let mut installed = Vec::new();
    for host in &hosts {
        let installer =
            Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());
        installed.push(installer.install_candidate(&candidate, host));
    }

    assert!(installed.iter().all(|host| host.installed), "{installed:?}");
    assert!(
        installed
            .iter()
            .all(|host| host.binary_sha256.as_deref() == Some(CANDIDATE_DIGEST)),
        "{installed:?}"
    );
    let requests = inner.requests();
    let probes = requests
        .iter()
        .filter(|request| {
            request.program == current_exe.as_os_str()
                && request.args == [OsString::from("host"), OsString::from("probe")]
        })
        .count();
    assert_eq!(probes, 1);
    let uploads = requests
        .iter()
        .filter(|request| request.stdin.as_deref() == Some(CANDIDATE_BYTES))
        .count();
    assert_eq!(uploads, 2);
    assert_eq!(
        fs::read(&current_exe).unwrap(),
        b"candidate changed after preflight read"
    );
    let _ = directory;
}

#[test]
fn setup_refuses_a_debug_candidate_unless_allow_debug_is_set() {
    let (_directory, current_exe) = executable_fixture();
    let debug_probe = debug_probe_json();
    let refused = RecordingRunner::returning_results(vec![Ok(result(0, debug_probe.clone(), b""))]);
    let error = prepare_candidate(&refused, &current_exe, false).unwrap_err();
    assert!(error.debug_refused);
    assert!(error.message.contains("--allow-debug"), "{}", error.message);
    assert!(
        refused
            .requests()
            .iter()
            .all(|request| request.program != "/usr/bin/ssh")
    );

    let mut allowed_results = success_results();
    allowed_results[0] = Ok(result(0, debug_probe, b""));
    let allowed = RecordingRunner::returning_results(allowed_results);
    let candidate = prepare_candidate(&allowed, &current_exe, true).unwrap();
    let installer =
        Installer::with_installation_id(&allowed, Uuid::parse_str(INSTALLATION_ID).unwrap());
    let installed = installer.install_candidate(&candidate, &worker());
    assert!(installed.installed, "{installed:?}");
    assert_eq!(
        installed.build_id.as_deref(),
        Some("0.1.0+0123456789ab-debug")
    );
    assert_eq!(installed.binary_sha256.as_deref(), Some(CANDIDATE_DIGEST));
}

fn debug_probe_json() -> Vec<u8> {
    let mut probe: serde_json::Value = serde_json::from_slice(&valid_probe_json()).unwrap();
    probe["build_id"] = serde_json::json!("0.1.0+0123456789ab-debug");
    probe["binary_sha256"] = serde_json::json!(CANDIDATE_DIGEST);
    serde_json::to_vec(&probe).unwrap()
}

fn setup_with_controller(
    enabled: bool,
    destination: &str,
    results: Vec<Result<ProcessResult, WorkerError>>,
) -> (CommandOutput, RecordingRunner) {
    let directory = tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    fs::write(
        &config_path,
        format!("version = 1\n[controller]\nenabled = {enabled}\nssh = '{destination}'\n[[workers]]\nname = 'mini-1'\nssh = 'mac1'\nslots = 1\n"),
    )
    .unwrap();
    let runner = RecordingRunner::returning_results(results);
    let output = execute_with(
        Cli {
            config: Some(config_path),
            json: true,
            command: Command::Setup {
                hosts: Vec::new(),
                allow_debug: false,
            },
        },
        &SetupControllerHealth(&runner),
    )
    .unwrap();
    (output, runner)
}

struct SetupControllerHealth<'a>(&'a RecordingRunner);
impl ProcessRunner for SetupControllerHealth<'_> {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request
            .args
            .last()
            .is_some_and(|arg| arg == "~/.local/bin/worker host controller-rpc")
        {
            self.0.requests.lock().unwrap().push(request.clone());
            let parsed = mac_worker::controller::parse_request(
                mac_worker::controller::decode_frame(request.stdin.as_ref().unwrap()).unwrap(),
            )
            .unwrap();
            let mut health =
                serde_json::to_value(mac_worker::controller::health::ControllerHealth::new(
                    mac_worker::job::ProcessIdentity::new(42, 1001000).unwrap(),
                    1001,
                ))
                .unwrap();
            health["supervised"] = serde_json::json!(true);
            health["binary_sha256"] =
                serde_json::json!(mac_worker::binary_identity::current_binary_sha256().unwrap());
            health["config_path"] =
                serde_json::json!("/Users/controller/.config/mac-worker/config.toml");
            health["paths"] = setup_service_paths();
            return Ok(result(0, mac_worker::controller::encode_json_frame(&serde_json::json!({"protocol_version":PROTOCOL_VERSION,"command":parsed.command(),"request_id":parsed.request_id(),"payload_sha256":parsed.payload_sha256(),"result":{"state":"healthy","reason":"tick_succeeded","leader_running":true,"health":health}})).unwrap(), b""));
        }
        self.0.run(request)
    }
}

fn setup_service_paths() -> serde_json::Value {
    serde_json::json!({"config":"/Users/controller/.config/mac-worker/config.toml","state":"/Users/controller/.local/state/mac-worker","cache":"/Users/controller/.cache/mac-worker","data":"/Users/controller/.local/share/mac-worker"})
}

fn restarted_controller_service() -> ProcessResult {
    let status: mac_worker::controller::service::ServiceStatus = serde_json::from_value(serde_json::json!({"label":"com.mac-worker.controller","domain":"gui/501","installed":true,"loaded":true,"pid":42,"running":true,"restart_started_at_millis":1000,"paths":setup_service_paths()})).unwrap();
    result(0, serde_json::to_vec(&status).unwrap(), b"")
}

fn resolved_setup_host(user: &str, hostname: &str, port: u16) -> ProcessResult {
    result(
        0,
        format!("user {user}\nhostname {hostname}\nport {port}\nproxyjump none\n"),
        b"",
    )
}

#[test]
fn setup_controller_restarts_the_service_after_verified_install_and_reports_it() {
    let mut results = success_results();
    results.push(Ok(restarted_controller_service()));
    let (output, runner) = setup_with_controller(true, "mac1", results);
    let json: serde_json::Value = serde_json::from_str(&output.render_json().unwrap()).unwrap();
    assert_eq!(json["workers"][0]["installed"], true);
    assert_eq!(json["workers"][0]["controller_service"], "restarted");
    assert_eq!(json["workers"][0]["warnings"], serde_json::json!([]));
    assert!(output.render_human().contains("\n  controller: restarted"));
    assert_eq!(output.aggregate_exit_kind(), None);
    let requests = runner.requests();
    assert_eq!(
        requests.len(),
        14,
        "exact SSH match needs restart plus verified leader health"
    );
    let restart = &requests[12];
    assert_eq!(
        request_command(restart),
        "~/.local/bin/worker host controller-service"
    );
    assert!(restart.args.iter().any(|arg| arg == "mac1"));
    let request: serde_json::Value =
        serde_json::from_slice(restart.stdin.as_ref().unwrap()).unwrap();
    assert_eq!(
        request,
        serde_json::json!({ "action": "restart", "include_details": true })
    );
}

#[test]
fn setup_controller_disabled_mode_leaves_the_service_stopped_without_extra_calls() {
    let (output, runner) = setup_with_controller(false, "mac1", success_results());
    let json: serde_json::Value = serde_json::from_str(&output.render_json().unwrap()).unwrap();
    assert_eq!(json["workers"][0]["installed"], true);
    assert!(json["workers"][0].get("controller_service").is_none());
    assert_eq!(runner.requests().len(), 12);
}

#[test]
fn setup_controller_does_not_restart_after_a_failed_install() {
    let (output, runner) = setup_with_controller(
        true,
        "mac1",
        vec![
            Ok(result(0, valid_probe_json(), b"")),
            Ok(result(75, b"", b"busy")),
        ],
    );
    let json: serde_json::Value = serde_json::from_str(&output.render_json().unwrap()).unwrap();
    assert_eq!(json["workers"][0]["installed"], false);
    assert!(json["workers"][0].get("controller_service").is_none());
    assert_eq!(runner.requests().len(), 2);
}

#[test]
fn setup_controller_matches_aliases_by_resolved_host_port_and_account() {
    let mut results = success_results();
    results.extend([
        Ok(resolved_setup_host("owner", "mini-1.local", 2222)),
        Ok(resolved_setup_host("owner", "MINI-1.local", 2222)),
        Ok(restarted_controller_service()),
    ]);
    let (output, runner) = setup_with_controller(true, "controller-alias", results);
    let json: serde_json::Value = serde_json::from_str(&output.render_json().unwrap()).unwrap();
    assert_eq!(json["workers"][0]["controller_service"], "restarted");
    let requests = runner.requests();
    assert_eq!(requests.len(), 16);
    assert!(
        requests[14]
            .args
            .iter()
            .any(|arg| arg == "controller-alias")
    );
    assert_eq!(
        requests[12].args,
        ["-G", "--", "controller-alias"].map(OsString::from)
    );
    assert_eq!(requests[13].args, ["-G", "--", "mac1"].map(OsString::from));
    assert_eq!(
        request_command(&requests[14]),
        "~/.local/bin/worker host controller-service"
    );
}

#[test]
fn setup_controller_ambiguous_proxy_jump_route_warns_without_restart() {
    let mut results = success_results();
    results.extend([
        Ok(result(
            0,
            b"user owner\nhostname 10.0.0.1\nport 22\nproxyjump gateway-a\n",
            b"",
        )),
        Ok(result(
            0,
            b"user owner\nhostname 10.0.0.1\nport 22\nproxyjump gateway-b\n",
            b"",
        )),
        // Let the broken implementation return success so this fails on the
        // public outcome, rather than an exhausted fake response queue.
        Ok(restarted_controller_service()),
    ]);
    let (output, runner) = setup_with_controller(true, "controller-alias", results);
    assert_controller_restart_warning(&output);
    assert!(
        output
            .render_human()
            .contains("could not determine whether this worker is the controller")
    );
    assert_eq!(runner.requests().len(), 14);
    assert!(
        !runner
            .requests()
            .iter()
            .any(|request| request_command(request).contains("controller-service"))
    );
}

#[test]
fn setup_controller_matches_aliases_with_the_same_proxy_jump_route() {
    let mut results = success_results();
    results.extend([
        Ok(result(0, b"user owner\nhostname mini.local\nport 2222\nproxyjump owner@gateway-a:2200,gateway-b\n", b"")),
        Ok(result(0, b"user owner\nhostname MINI.local\nport 2222\nproxyjump owner@gateway-a:2200,gateway-b\n", b"")),
        Ok(restarted_controller_service()),
    ]);
    let (output, runner) = setup_with_controller(true, "controller-alias", results);
    let json: serde_json::Value = serde_json::from_str(&output.render_json().unwrap()).unwrap();
    assert_eq!(json["workers"][0]["controller_service"], "restarted");
    assert_eq!(json["workers"][0]["warnings"], serde_json::json!([]));
    assert_eq!(runner.requests().len(), 15);
}

#[test]
fn setup_controller_does_not_restart_other_hosts_ports_or_accounts() {
    for (user, host, port) in [
        ("owner", "mini-2.local", 22),
        ("other", "mini-1.local", 22),
        ("owner", "mini-1.local", 2222),
    ] {
        let mut results = success_results();
        results.extend([
            Ok(resolved_setup_host("owner", "mini-1.local", 22)),
            Ok(resolved_setup_host(user, host, port)),
        ]);
        let (output, runner) = setup_with_controller(true, "controller-alias", results);
        let json: serde_json::Value = serde_json::from_str(&output.render_json().unwrap()).unwrap();
        assert_eq!(json["workers"][0]["installed"], true);
        assert!(json["workers"][0].get("controller_service").is_none());
        assert_eq!(json["workers"][0]["warnings"], serde_json::json!([]));
        assert_eq!(runner.requests().len(), 14);
    }
}

#[test]
fn setup_controller_alias_resolution_failure_is_a_redacted_install_warning() {
    let mut results = success_results();
    results.push(Ok(result(
        1,
        b"",
        b"token=PLANTED_CONTROLLER_SECRET /private/owner",
    )));
    let (output, runner) = setup_with_controller(true, "controller-alias", results);
    assert_controller_restart_warning(&output);
    assert_eq!(
        runner.requests().len(),
        13,
        "unknown alias must not restart any service"
    );
}

#[test]
fn setup_controller_restart_failures_keep_the_helper_installed_and_report_a_warning() {
    for failure in [
        Ok(result(70, b"", b"token=PLANTED_CONTROLLER_SECRET /private/owner")),
        Err(WorkerError::Io(io::Error::other("PLANTED_CONTROLLER_SECRET /private/owner"))),
        Ok(result(0, br#"{"label":"com.mac-worker.controller","domain":"gui/501","installed":true,"loaded":false}"#, b"")),
        Ok(result(0, br#"{"label":"com.example.other-service","domain":"gui/501","installed":true,"loaded":true}"#, b"")),
    ] {
        let mut results = success_results();
        results.push(failure);
        let (output, runner) = setup_with_controller(true, "mac1", results);
        assert_controller_restart_warning(&output);
        assert_eq!(runner.requests().len(), 13);
    }
}

fn assert_controller_restart_warning(output: &CommandOutput) {
    let encoded = output.render_json().unwrap();
    let json: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    assert_eq!(json["workers"][0]["installed"], true);
    assert_eq!(json["workers"][0]["error_code"], serde_json::Value::Null);
    assert_eq!(
        json["workers"][0]["controller_service"],
        "failed CONTROLLER_RESTART_FAILED"
    );
    assert_eq!(
        json["workers"][0]["warnings"][0]["code"],
        "CONTROLLER_RESTART_FAILED"
    );
    assert_eq!(output.aggregate_exit_kind(), None);
    let human = output.render_human();
    assert!(human.contains("controller: failed CONTROLLER_RESTART_FAILED"));
    assert!(human.contains("warning [CONTROLLER_RESTART_FAILED]"));
    for rendered in [encoded, human] {
        assert!(!rendered.contains("PLANTED_CONTROLLER_SECRET"));
        assert!(!rendered.contains("/private/owner"));
    }
}

#[test]
fn setup_controller_loaded_job_without_verified_leader_is_a_warning() {
    let mut results = success_results();
    results.push(Ok(result(0, br#"{"label":"com.mac-worker.controller","domain":"gui/501","installed":true,"loaded":true}"#, b"")));
    let (output, _) = setup_with_controller(true, "mac1", results);
    assert_controller_restart_warning(&output);
}
