use std::{
    collections::BTreeMap, fs, io::Cursor, os::unix::process::ExitStatusExt, path::PathBuf,
    process::ExitStatus, sync::Mutex,
};

use clap::Parser;
use mac_worker::test_support::{
    cli::Cli,
    controller::{init::ConfiguredHost, service::ServiceStatus},
    core::{config::WorkerEntry, error::WorkerError},
    host::process::{ProcessRequest, ProcessResult, ProcessRunner},
    runtime::{RuntimeContext, run_with_stdio_in_context},
    transfer::{HostOperation, controller_host_request},
};
use serde_json::json;

// Definitions from 0445205:src/controller/service.rs, provision.rs and protocol.rs.
// Only external crate paths are adjusted. The production canonical decoder is
// unchanged from that revision and is exercised via controller_host_request.
mod legacy {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum ServiceAction {
        Install,
        Restart,
        Uninstall,
        Status,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ServiceStatus {
        pub label: String,
        pub domain: String,
        pub installed: bool,
        pub loaded: bool,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ConfigWriteResult {
        pub changed: bool,
        pub conflict: bool,
        pub diff: Option<String>,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct ControllerServiceRequest {
        pub action: ServiceAction,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct ControllerConfigureRequest {
        pub config_toml: String,
        pub workers: Vec<mac_worker::test_support::controller::provision::PlannedWorker>,
        pub known_hosts: String,
        #[serde(default)]
        pub force: bool,
    }
}

#[derive(Default)]
struct LocalLaunchctl(Mutex<bool>);
impl ProcessRunner for LocalLaunchctl {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(request.program, "/bin/launchctl");
        let mut loaded = self.0.lock().unwrap();
        let code = match request.args[0].to_str().unwrap() {
            "print" => {
                if *loaded {
                    0
                } else {
                    113
                }
            }
            "bootstrap" | "kickstart" => {
                *loaded = true;
                0
            }
            "bootout" => {
                *loaded = false;
                0
            }
            other => panic!("unexpected launchctl action {other}"),
        };
        Ok(ProcessResult {
            status: ExitStatus::from_raw(code << 8),
            stdout: format!(
                "gui/{}/com.mac-worker.controller = {{\n state = running\n pid = 42\n}}\n",
                unsafe { libc::geteuid() }
            )
            .into_bytes(),
            stderr: vec![],
        })
    }
}

struct NewHost {
    home: PathBuf,
    launchctl: LocalLaunchctl,
}
impl ProcessRunner for NewHost {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(request.program, "/usr/bin/ssh");
        let command = match request.args.last().unwrap().to_str().unwrap() {
            "~/.local/bin/worker host controller-service" => "controller-service",
            "~/.local/bin/worker host controller-configure" => "controller-configure",
            other => panic!("unexpected fake SSH command {other}"),
        };
        let runtime =
            RuntimeContext::isolated(BTreeMap::new(), self.home.clone(), self.home.clone());
        let mut stdout = vec![];
        let mut stderr = vec![];
        let code = run_with_stdio_in_context(
            Cli::try_parse_from(["worker", "host", command]).unwrap(),
            &self.launchctl,
            &runtime,
            &mut Cursor::new(request.stdin.as_ref().unwrap()),
            &mut stdout,
            &mut stderr,
        );
        Ok(ProcessResult {
            status: ExitStatus::from_raw(i32::from(code) << 8),
            stdout,
            stderr,
        })
    }
}

fn worker() -> WorkerEntry {
    WorkerEntry {
        name: "controller".into(),
        ssh: "controller".into(),
        slots: 1,
        capabilities: vec![],
        remote_binary: "~/.local/bin/worker".into(),
        herdr: false,
    }
}

fn configure_request(host: &NewHost) -> legacy::ControllerConfigureRequest {
    use mac_worker::test_support::controller::provision::{PlannedWorker, ResolvedSsh};
    let config_toml =
        "version=1\n[[workers]]\nname='mini-1'\nssh='mac-worker-controller-mini-1'\nslots=1\n";
    let path = host.home.join(".config/mac-worker/config.toml");
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    // The conflict path produces a genuine configure reply without SSH-file writes.
    fs::write(path, config_toml.replace("slots=1", "slots=2")).unwrap();
    legacy::ControllerConfigureRequest {
        config_toml: config_toml.into(),
        workers: vec![PlannedWorker {
            name: "mini-1".into(),
            alias: "mac-worker-controller-mini-1".into(),
            target: ResolvedSsh::parse("user owner\nhostname 127.0.0.1\nport 22\n").unwrap(),
            trusted_keys: None,
        }],
        known_hosts: String::new(),
        force: false,
    }
}

#[test]
fn legacy_laptop_canonical_decoder_accepts_every_new_service_action() {
    let temp = tempfile::tempdir().unwrap();
    let host = NewHost {
        home: temp.path().to_owned(),
        launchctl: LocalLaunchctl::default(),
    };
    let worker = worker();
    for action in [
        legacy::ServiceAction::Install,
        legacy::ServiceAction::Status,
        legacy::ServiceAction::Restart,
        legacy::ServiceAction::Uninstall,
    ] {
        let result = controller_host_request::<_, legacy::ServiceStatus>(
            &host,
            &worker,
            HostOperation::ControllerService,
            &legacy::ControllerServiceRequest { action },
        )
        .unwrap_or_else(|error| panic!("{action:?}: {error}"));
        assert_eq!(result.label, "com.mac-worker.controller");
        assert_eq!(result.loaded, action != legacy::ServiceAction::Uninstall);
    }
}

#[test]
fn legacy_laptop_canonical_decoder_accepts_new_configure_reply() {
    let temp = tempfile::tempdir().unwrap();
    let host = NewHost {
        home: temp.path().to_owned(),
        launchctl: LocalLaunchctl::default(),
    };
    let result = controller_host_request::<_, legacy::ConfigWriteResult>(
        &host,
        &worker(),
        HostOperation::ControllerConfigure,
        &configure_request(&host),
    )
    .unwrap();
    assert!(result.conflict);
}

#[test]
fn opted_in_laptop_receives_service_identity_and_configure_path() {
    let temp = tempfile::tempdir().unwrap();
    let host = NewHost {
        home: temp.path().to_owned(),
        launchctl: LocalLaunchctl::default(),
    };
    let worker = worker();
    let result: ServiceStatus = controller_host_request(
        &host,
        &worker,
        HostOperation::ControllerService,
        &json!({"action":"install","include_details":true}),
    )
    .unwrap();
    assert_eq!(result.pid, Some(42));
    assert_eq!(result.running, Some(true));
    assert!(result.paths.is_some());
    let mut request = serde_json::to_value(configure_request(&host)).unwrap();
    request["include_details"] = json!(true);
    let result: ConfiguredHost =
        controller_host_request(&host, &worker, HostOperation::ControllerConfigure, &request)
            .unwrap();
    assert_eq!(
        result.config_path,
        Some(host.home.join(".config/mac-worker/config.toml"))
    );
}

#[derive(Default)]
struct OldHost {
    service_calls: Mutex<Vec<serde_json::Value>>,
}
impl ProcessRunner for OldHost {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(request.program, "/usr/bin/ssh");
        if request.args.last().unwrap() != "~/.local/bin/worker host controller-service" {
            return Err(WorkerError::Unavailable(
                "CONTROLLER_UNAVAILABLE: legacy health/drain unavailable".into(),
            ));
        }
        let bytes = request.stdin.as_ref().unwrap();
        self.service_calls
            .lock()
            .unwrap()
            .push(serde_json::from_slice(bytes).unwrap());
        let request: legacy::ControllerServiceRequest = match serde_json::from_slice(bytes) {
            Ok(request) => request,
            Err(_) => {
                let error = mac_worker::test_support::host::job::HostControlError::new(
                    "INVALID_REQUEST",
                    "invalid controller host request",
                )
                .unwrap();
                let mut stdout = serde_json::to_vec(&error).unwrap();
                stdout.push(b'\n');
                return Ok(ProcessResult {
                    status: ExitStatus::from_raw(64 << 8),
                    stdout,
                    stderr: vec![],
                });
            }
        };
        let loaded = request.action != legacy::ServiceAction::Uninstall;
        let response = legacy::ServiceStatus {
            label: "com.mac-worker.controller".into(),
            domain: "gui/501".into(),
            installed: loaded,
            loaded,
        };
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: serde_json::to_vec(&response).unwrap(),
            stderr: vec![],
        })
    }
}

fn old_host_cli(action: &str) -> (OldHost, serde_json::Value, String) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("config.toml");
    fs::write(&path, "version=1\n[controller]\nenabled=true\nssh='controller'\n[[workers]]\nname='mini-1'\nssh='worker-1'\nslots=1\n").unwrap();
    let runtime = RuntimeContext::isolated(BTreeMap::new(), temp.path().into(), temp.path().into());
    let host = OldHost::default();
    let mut out = vec![];
    let mut err = vec![];
    let exit = run_with_stdio_in_context(
        Cli::try_parse_from([
            "worker",
            "--config",
            path.to_str().unwrap(),
            "--json",
            "controller",
            action,
        ])
        .unwrap(),
        &host,
        &runtime,
        &mut Cursor::new([]),
        &mut out,
        &mut err,
    );
    assert_eq!(
        exit,
        0,
        "{} {}",
        String::from_utf8_lossy(&out),
        String::from_utf8_lossy(&err)
    );
    (
        host,
        serde_json::from_slice(&out).unwrap(),
        fs::read_to_string(path).unwrap(),
    )
}

#[test]
fn new_laptop_status_falls_back_when_legacy_helper_rejects_details() {
    let (host, status, _) = old_host_cli("status");
    assert_eq!(status["service"]["loaded"], true);
    assert_eq!(status["service"]["pid"], serde_json::Value::Null);
    let calls = host.service_calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0]["include_details"], true);
    assert!(calls[1].get("include_details").is_none());
}

#[test]
fn new_laptop_disable_accepts_legacy_helper_and_disables_local_mode() {
    let (host, _, config) = old_host_cli("disable");
    assert!(
        !mac_worker::test_support::core::config::Config::parse(&config)
            .unwrap()
            .controller
            .enabled
    );
    let calls = host.service_calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].get("include_details").is_none());
}
