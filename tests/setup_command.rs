use std::{
    collections::VecDeque,
    ffi::OsString,
    fs,
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    process::ExitStatus,
    sync::{Arc, Mutex},
};

use mac_worker::{
    cli::{Cli, Command, HostCommand},
    config::WorkerEntry,
    error::WorkerError,
    execute_with,
    install::Installer,
    output::CommandOutput,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::{PROTOCOL_VERSION, SetupHostResult, SetupReport},
};
use tempfile::tempdir;
use uuid::Uuid;

const INSTALLATION_ID: &str = "00112233445566778899aabbccddeeff";

#[derive(Clone)]
struct RecordingRunner {
    requests: Arc<Mutex<Vec<ProcessRequest>>>,
    results: Arc<Mutex<VecDeque<Result<ProcessResult, WorkerError>>>>,
}

impl RecordingRunner {
    fn returning(results: Vec<ProcessResult>) -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            results: Arc::new(Mutex::new(VecDeque::from(
                results.into_iter().map(Ok).collect::<Vec<_>>(),
            ))),
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

#[test]
fn first_install_preflights_then_stages_promotes_verifies_and_cleans_up() {
    // This catches a reordered or shell-expanded transfer, a non-scoped backup,
    // or an installation that skips verification before deleting its backup.
    let (_directory, current_exe) = executable_fixture();
    let runner = RecordingRunner::returning(vec![
        result(0, valid_probe_json(), b""),
        result(0, b"", b""),
        result(0, b"", b""),
        result(0, b"", b""),
        result(0, valid_probe_json(), b""),
        result(0, b"", b""),
    ]);
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(installed.installed);
    assert_eq!(installed.protocol_version, Some(1));
    assert_eq!(
        runner.requests(),
        vec![
            ProcessRequest {
                program: current_exe.clone().into_os_string(),
                args: vec!["host".into(), "probe".into()],
                stdin: None,
            },
            ProcessRequest {
                program: OsString::from("/usr/bin/ssh"),
                args: vec![
                    "-o".into(),
                    "BatchMode=yes".into(),
                    "-o".into(),
                    "ConnectTimeout=5".into(),
                    "mac1".into(),
                    format!(
                        "umask 077 && mkdir -p ~/.local/bin ~/.local/share/mac-worker/setup/{INSTALLATION_ID}"
                    )
                    .into(),
                ],
                stdin: None,
            },
            ProcessRequest {
                program: OsString::from("/usr/bin/scp"),
                args: vec![
                    "-q".into(),
                    "-o".into(),
                    "BatchMode=yes".into(),
                    "-o".into(),
                    "ConnectTimeout=5".into(),
                    current_exe.as_os_str().into(),
                    format!(
                        "mac1:~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.new"
                    )
                    .into(),
                ],
                stdin: None,
            },
            ProcessRequest {
                program: OsString::from("/usr/bin/ssh"),
                args: vec![
                    "-o".into(),
                    "BatchMode=yes".into(),
                    "-o".into(),
                    "ConnectTimeout=5".into(),
                    "mac1".into(),
                    format!(
                        "if [ -f ~/.local/bin/worker ]; then cp -p ~/.local/bin/worker ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.previous; fi && chmod 0755 ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.new && mv ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.new ~/.local/bin/worker"
                    )
                    .into(),
                ],
                stdin: None,
            },
            ProcessRequest {
                program: OsString::from("/usr/bin/ssh"),
                args: vec![
                    "-o".into(),
                    "BatchMode=yes".into(),
                    "-o".into(),
                    "ConnectTimeout=5".into(),
                    "mac1".into(),
                    "~/.local/bin/worker host probe".into(),
                ],
                stdin: None,
            },
            ProcessRequest {
                program: OsString::from("/usr/bin/ssh"),
                args: vec![
                    "-o".into(),
                    "BatchMode=yes".into(),
                    "-o".into(),
                    "ConnectTimeout=5".into(),
                    "mac1".into(),
                    format!(
                        "rm -f ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.previous && rmdir ~/.local/share/mac-worker/setup/{INSTALLATION_ID}"
                    )
                    .into(),
                ],
                stdin: None,
            },
        ]
    );
}

#[test]
fn successful_replacement_uses_one_conditional_promotion_and_scoped_cleanup() {
    // This catches adding a racy existence-probe request or overwriting an
    // existing helper without retaining an installation-scoped backup.
    let (_directory, current_exe) = executable_fixture();
    let runner = RecordingRunner::returning(vec![
        result(0, valid_probe_json(), b""),
        result(0, b"", b""),
        result(0, b"", b""),
        result(0, b"replacement backed up", b""),
        result(0, valid_probe_json(), b""),
        result(0, b"", b""),
    ]);
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(installed.installed);
    let requests = runner.requests();
    assert_eq!(requests.len(), 6);
    assert_eq!(requests[3].program, OsString::from("/usr/bin/ssh"));
    assert_eq!(
        requests[3].args[5].to_string_lossy(),
        format!(
            "if [ -f ~/.local/bin/worker ]; then cp -p ~/.local/bin/worker ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.previous; fi && chmod 0755 ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.new && mv ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.new ~/.local/bin/worker"
        )
    );
    assert_eq!(
        requests[5].args[5].to_string_lossy(),
        format!(
            "rm -f ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.previous && rmdir ~/.local/share/mac-worker/setup/{INSTALLATION_ID}"
        )
    );
}

#[test]
fn failed_verification_restores_only_the_installation_scoped_backup() {
    // This catches deleting the previous working binary or restoring a shared,
    // potentially stale backup after the promoted candidate fails its probe.
    let (_directory, current_exe) = executable_fixture();
    let runner = RecordingRunner::returning(vec![
        result(0, valid_probe_json(), b""),
        result(0, b"", b""),
        result(0, b"", b""),
        result(0, b"", b""),
        result(255, b"", b"candidate failed"),
        result(0, b"", b""),
    ]);
    let installer =
        Installer::with_installation_id(&runner, Uuid::parse_str(INSTALLATION_ID).unwrap());

    let installed = installer.install(&current_exe, &worker());

    assert!(!installed.installed);
    assert_eq!(installed.error_code.as_deref(), Some("VERIFICATION_FAILED"));
    let requests = runner.requests();
    assert_eq!(requests.len(), 6);
    assert_eq!(requests[4].args[5], "~/.local/bin/worker host probe");
    assert_eq!(
        requests[5],
        ProcessRequest {
            program: OsString::from("/usr/bin/ssh"),
            args: vec![
                "-o".into(),
                "BatchMode=yes".into(),
                "-o".into(),
                "ConnectTimeout=5".into(),
                "mac1".into(),
                format!(
                    "if [ -f ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.previous ]; then mv ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.previous ~/.local/bin/worker; else rm -f ~/.local/bin/worker; fi && rm -f ~/.local/share/mac-worker/setup/{INSTALLATION_ID}/worker.new && rmdir ~/.local/share/mac-worker/setup/{INSTALLATION_ID}"
                )
                .into(),
            ],
            stdin: None,
        }
    );
}

#[test]
fn setup_dispatch_keeps_processing_inventory_names_after_a_host_failure() {
    // This catches treating user arguments as SSH destinations or aborting the
    // requested batch when an earlier inventory worker cannot be installed.
    let directory = tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"first\"\nssh = \"mac1\"\nslots = 1\n[[workers]]\nname = \"second\"\nssh = \"mac2\"\nslots = 1\n",
    )
    .unwrap();
    let runner = RecordingRunner::returning(vec![
        result(0, valid_probe_json(), b""),
        result(0, b"", b""),
        result(255, b"", b"offline"),
        result(0, valid_probe_json(), b""),
        result(0, b"", b""),
        result(0, b"", b""),
        result(0, b"", b""),
        result(0, valid_probe_json(), b""),
        result(0, b"", b""),
    ]);
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
    assert_eq!(report.workers[0].name, "first");
    assert!(!report.workers[0].installed);
    assert_eq!(report.workers[1].name, "second");
    assert!(report.workers[1].installed);
    let requests = runner.requests();
    assert_eq!(requests[2].program, OsString::from("/usr/bin/scp"));
    let scp_destination = requests[2].args[6].to_string_lossy();
    let generated_id = scp_destination
        .strip_prefix("mac1:~/.local/share/mac-worker/setup/")
        .and_then(|value| value.strip_suffix("/worker.new"))
        .expect("SCP destination must stay inside the owned setup directory");
    assert_eq!(generated_id.len(), 32);
    assert!(
        generated_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    );
    let remote_destinations = requests
        .into_iter()
        .filter(|request| request.program == "/usr/bin/ssh")
        .map(|request| request.args[4].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        remote_destinations,
        vec![
            OsString::from("mac1"),
            OsString::from("mac2"),
            OsString::from("mac2"),
            OsString::from("mac2"),
            OsString::from("mac2"),
        ]
    );
}

#[test]
fn workers_dispatch_loads_inventory_and_returns_the_typed_report() {
    // This catches routing the public workers command through setup or leaving
    // the parsed phase-one command disconnected from its service.
    let directory = tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap();
    let runner = RecordingRunner::returning(vec![result(0, valid_probe_json(), b"")]);
    let cli = Cli {
        config: Some(config_path),
        json: false,
        command: Command::Workers,
    };

    let output = execute_with(cli, &runner).unwrap();

    let CommandOutput::Workers(report) = output else {
        panic!("workers must return a workers report")
    };
    assert_eq!(report.protocol_version, 1);
    assert_eq!(report.workers.len(), 1);
    assert_eq!(report.workers[0].name, "mini-1");
}

#[test]
fn setup_rejects_an_ssh_destination_that_is_not_a_logical_inventory_name() {
    // This catches allowing a CLI token to bypass the validated inventory and
    // become an arbitrary SSH destination.
    let directory = tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
    )
    .unwrap();
    let runner = RecordingRunner::returning(Vec::new());
    let cli = Cli {
        config: Some(config_path),
        json: false,
        command: Command::Setup {
            hosts: vec!["mac1".into()],
        },
    };

    let error = execute_with(cli, &runner).unwrap_err();

    assert!(matches!(error, WorkerError::Config(_)));
    assert!(runner.requests().is_empty());
}

#[test]
fn setup_without_names_attempts_every_configured_worker() {
    // This catches interpreting an empty setup list as no work instead of the
    // complete validated inventory.
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
    let cli = Cli {
        config: Some(config_path),
        json: false,
        command: Command::Setup { hosts: Vec::new() },
    };

    let output = execute_with(cli, &runner).unwrap();

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
    assert!(report.workers.iter().all(|worker| !worker.installed));
    assert_eq!(runner.requests().len(), 2);
}

#[test]
fn hidden_host_probe_does_not_load_client_inventory() {
    // This catches accidentally coupling the remotely invoked helper to the
    // MacBook-only inventory path.
    let runner = RecordingRunner::returning(Vec::new());
    let cli = Cli {
        config: Some(std::path::PathBuf::from(
            "/definitely/missing/mac-worker.toml",
        )),
        json: false,
        command: Command::Host {
            command: HostCommand::Probe,
        },
    };

    let output = execute_with(cli, &runner).unwrap();

    let CommandOutput::Probe(probe) = output else {
        panic!("host probe must return raw probe data to the renderer")
    };
    assert_eq!(probe.protocol_version, 1);
    assert!(!probe.hostname.is_empty());
}

#[test]
fn command_output_renders_tagged_compact_json() {
    // This catches leaking an untagged report shape through the public JSON
    // boundary or emitting pretty/debug text where machine JSON is required.
    let output = CommandOutput::Setup(SetupReport {
        protocol_version: PROTOCOL_VERSION,
        workers: vec![SetupHostResult {
            name: "mini-1".into(),
            ssh: "mac1".into(),
            installed: true,
            protocol_version: Some(PROTOCOL_VERSION),
            error_code: None,
            error_message: None,
        }],
    });

    let rendered = output.render_json().unwrap();

    assert_eq!(
        rendered,
        r#"{"kind":"setup","protocol_version":1,"workers":[{"name":"mini-1","ssh":"mac1","installed":true,"protocol_version":1,"error_code":null,"error_message":null}]}"#
    );
}

#[test]
fn setup_human_output_distinguishes_installed_and_failed_hosts() {
    // This catches a human renderer that hides per-host failures behind a
    // successful top-level report.
    let output = CommandOutput::Setup(SetupReport {
        protocol_version: PROTOCOL_VERSION,
        workers: vec![
            SetupHostResult {
                name: "mini-1".into(),
                ssh: "mac1".into(),
                installed: true,
                protocol_version: Some(PROTOCOL_VERSION),
                error_code: None,
                error_message: None,
            },
            SetupHostResult {
                name: "mini-2".into(),
                ssh: "mac2".into(),
                installed: false,
                protocol_version: None,
                error_code: Some("INSTALL_FAILED".into()),
                error_message: Some("transfer failed".into()),
            },
        ],
    });

    assert_eq!(
        output.render_human(),
        "mini-1: installed (protocol 1)\nmini-2: failed [INSTALL_FAILED]: transfer failed"
    );
}
