use std::{
    collections::VecDeque,
    ffi::OsString,
    fs,
    io::{self, Write},
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    process::ExitStatus,
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
    protocol::{PROTOCOL_VERSION, SetupHostResult, SetupReport, SetupWarning, SetupWarningCode},
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

fn ssh_request(command: String, policy: ProcessPolicy) -> ProcessRequest {
    ProcessRequest {
        program: OsString::from("/usr/bin/ssh"),
        args: vec![
            "-o".into(),
            "BatchMode=yes".into(),
            "-o".into(),
            "ConnectTimeout=5".into(),
            "--".into(),
            "mac1".into(),
            command.into(),
        ],
        environment: Vec::new(),
        stdin: None,
        policy,
    }
}

fn acquire_command() -> String {
    format!(
        "umask 077 && mkdir -p ~/.local/bin ~/.local/share/mac-worker/setup && if mkdir ~/.local/share/mac-worker/setup/.install-lock; then printf '%s\\n' {INSTALLATION_ID} > ~/.local/share/mac-worker/setup/.install-lock/owner; else exit 75; fi && mkdir ~/.local/share/mac-worker/setup/{INSTALLATION_ID} && printf '%s\\n' acquired > ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/state"
    )
}

fn digest_command() -> String {
    format!(
        "candidate=$(/usr/bin/shasum -a 256 ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.new) && candidate=${{candidate%% *}} && if [ \"$candidate\" = {CANDIDATE_DIGEST} ]; then printf '%s\\n' {CANDIDATE_DIGEST} > ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/candidate.sha256 && printf '%s\\n' staged > ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/state && printf '%s\\n' match; else printf '%s\\n' mismatch; fi"
    )
}

fn prepare_command() -> String {
    format!(
        "if [ -f ~/.local/bin/worker ]; then cp -p ~/.local/bin/worker ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.previous && previous=$(/usr/bin/shasum -a 256 ~/.local/bin/worker) && printf '%s\\n' \"${{previous%% *}}\" > ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/previous.sha256; else : > ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/no-previous; fi && printf '%s\\n' prepared > ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/state"
    )
}

fn promotion_command() -> String {
    format!(
        "printf '%s\\n' promoting > ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/state && chmod 0755 ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.new && mv ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.new ~/.local/bin/worker && printf '%s\\n' promoted > ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/state"
    )
}

fn reconciliation_command() -> String {
    format!(
        "target=$(/usr/bin/shasum -a 256 ~/.local/bin/worker 2>/dev/null) && target=${{target%% *}}; if [ \"$target\" = {CANDIDATE_DIGEST} ]; then printf '%s\\n' promoted; elif [ -f ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/previous.sha256 ] && [ \"$target\" = \"$(/bin/cat ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/previous.sha256)\" ]; then printf '%s\\n' previous; elif [ -f ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/no-previous ] && [ ! -e ~/.local/bin/worker ]; then printf '%s\\n' previous; else printf '%s\\n' unknown; fi"
    )
}

fn cleanup_command() -> String {
    format!(
        "if [ \"$(/bin/cat ~/.local/share/mac-worker/setup/.install-lock/owner 2>/dev/null)\" = {INSTALLATION_ID} ]; then rm -f ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.new ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.previous ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/candidate.sha256 ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/previous.sha256 ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/no-previous ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/state && rmdir ~/.local/share/mac-worker/setup/{INSTALLATION_ID} && rm -f ~/.local/share/mac-worker/setup/.install-lock/owner && rmdir ~/.local/share/mac-worker/setup/.install-lock; else exit 76; fi"
    )
}

fn rollback_command() -> String {
    format!(
        "if [ \"$(/bin/cat ~/.local/share/mac-worker/setup/.install-lock/owner 2>/dev/null)\" = {INSTALLATION_ID} ]; then if [ -f ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.previous ]; then mv ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.previous ~/.local/bin/worker; elif [ -f ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/no-previous ]; then rm -f ~/.local/bin/worker; else exit 77; fi && printf '%s\\n' rolled_back > ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/state && rm -f ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.new ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/candidate.sha256 ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/previous.sha256 ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/no-previous ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/state && rmdir ~/.local/share/mac-worker/setup/{INSTALLATION_ID} && rm -f ~/.local/share/mac-worker/setup/.install-lock/owner && rmdir ~/.local/share/mac-worker/setup/.install-lock; else exit 76; fi"
    )
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

#[test]
fn success_locks_hashes_promotes_reconciles_verifies_and_releases_with_safe_argv() {
    // Catches skipping a transaction boundary, an option terminator/policy,
    // or the fixed-literal plus lowercase-hex remote-expression contract.
    let (_directory, current_exe) = executable_fixture();
    let runner = RecordingRunner::returning_results(success_results());
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(installed.installed);
    assert_eq!(installed.protocol_version, Some(1));
    assert!(installed.warnings.is_empty());
    assert_eq!(
        runner.requests(),
        vec![
            ProcessRequest {
                program: current_exe.clone().into_os_string(),
                args: vec!["host".into(), "probe".into()],
                environment: Vec::new(),
                stdin: None,
                policy: preflight_policy(),
            },
            ssh_request(acquire_command(), control_policy()),
            ProcessRequest {
                program: OsString::from("/usr/bin/scp"),
                args: vec![
                    "-q".into(),
                    "-o".into(),
                    "BatchMode=yes".into(),
                    "-o".into(),
                    "ConnectTimeout=5".into(),
                    "--".into(),
                    current_exe.as_os_str().into(),
                    format!("mac1:~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.new")
                        .into(),
                ],
                environment: Vec::new(),
                stdin: None,
                policy: transfer_policy(),
            },
            ssh_request(digest_command(), control_policy()),
            ssh_request(prepare_command(), control_policy()),
            ssh_request(promotion_command(), control_policy()),
            ssh_request(reconciliation_command(), control_policy()),
            ssh_request("~/.local/bin/worker host probe".into(), preflight_policy(),),
            ssh_request(cleanup_command(), control_policy()),
        ]
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
    assert_eq!(
        requests[1],
        ssh_request(acquire_command(), control_policy())
    );
}

#[test]
fn scp_failure_attempts_owned_cleanup_without_losing_primary_error() {
    // Catches returning from a partial transfer or replacing its primary error
    // with a later cleanup failure.
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
    assert_eq!(
        runner.requests().last(),
        Some(&ssh_request(cleanup_command(), control_policy()))
    );
}

#[test]
fn changed_candidate_digest_is_never_prepared_or_promoted() {
    // Catches promoting bytes that changed while SCP read the local candidate.
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
    assert_eq!(runner.requests().len(), 5);
    assert_eq!(
        runner.requests().last(),
        Some(&ssh_request(cleanup_command(), control_policy()))
    );
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
    assert_eq!(
        requests[6],
        ssh_request(reconciliation_command(), control_policy())
    );
    assert_eq!(
        requests[7],
        ssh_request(cleanup_command(), control_policy())
    );
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
    assert_eq!(
        requests[6],
        ssh_request(reconciliation_command(), control_policy())
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.args.last() == Some(&promotion_command().into()))
            .count(),
        1
    );
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
    assert_eq!(
        requests[6],
        ssh_request(reconciliation_command(), control_policy())
    );
    assert!(!requests.contains(&ssh_request(cleanup_command(), control_policy())));
    assert!(!requests.contains(&ssh_request(rollback_command(), control_policy())));
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
    assert_eq!(
        runner.requests().last(),
        Some(&ssh_request(rollback_command(), control_policy()))
    );
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
                warnings: Vec::new(),
            },
            SetupHostResult {
                name: "mini-2".into(),
                ssh: "mac2".into(),
                installed: false,
                protocol_version: None,
                error_code: Some("INSTALL_FAILED".into()),
                error_message: Some("transfer failed".into()),
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
