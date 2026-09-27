#[allow(dead_code)]
mod support;

use clap::Parser;
use mac_worker::{
    cli::Cli,
    config::Config,
    controller::{ControllerFault, decode_frame, encode_json_frame, serve_rpc_with_runtime},
    doctor::{DoctorRequest, DoctorService},
    error::WorkerError,
    job::HostControlError,
    paths::PathLayout,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    protocol::PROTOCOL_VERSION,
};
use serde_json::{Value, json};
use std::{io::Cursor, os::unix::process::ExitStatusExt, process::ExitStatus};

fn paths(root: &std::path::Path) -> PathLayout {
    let root = root.canonicalize().unwrap();
    PathLayout {
        config: root.join("config.toml"),
        state: root.join("state"),
        cache: root.join("cache"),
        data: root.join("data"),
    }
}

struct OldController;
impl ProcessRunner for OldController {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == "/usr/bin/ssh" {
            let rpc = request
                .args
                .last()
                .unwrap()
                .to_string_lossy()
                .contains("controller-rpc");
            return Ok(ProcessResult {
                status: ExitStatus::from_raw(if rpc { 1 << 8 } else { 0 }),
                stdout: if rpc {
                    encode_json_frame(
                        &HostControlError::new("CONTROLLER_TRANSPORT", "protocol error").unwrap(),
                    )?
                } else {
                    serde_json::to_vec(&json!({
                        "protocol_version": PROTOCOL_VERSION, "hostname": "test.local", "arch": "arm64",
                        "os_version": "26.2", "free_disk_bytes": 536870912_u64,
                        "memory_pressure": "normal", "swap_used_bytes": 0,
                        "capabilities": [],
                    })).unwrap()
                },
                stderr: b"must not show /private/secret token=private-value".to_vec(),
            });
        }
        SystemProcessRunner.run(request)
    }
}

#[test]
fn controller_status_accepts_json() {
    assert!(Cli::try_parse_from(["worker", "controller", "status", "--json"]).is_ok());
}

#[test]
fn health_rpc_is_read_only_and_reports_missing_leader() {
    let temp = tempfile::tempdir().unwrap();
    let paths = paths(temp.path());
    let config =
        Config::parse("version=1\n[controller]\nenabled=true\nssh='controller'\n").unwrap();
    let frame = encode_json_frame(&json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": "018f0f4a6b5c7d8e9f00112233445560",
        "command": "controller.health", "body": {},
    }))
    .unwrap();
    let mut stdout = Vec::new();
    let result = serve_rpc_with_runtime(
        &paths,
        &config,
        &OldController,
        &mut Cursor::new(frame),
        &mut stdout,
        ControllerFault::None,
    );
    assert!(result.is_ok(), "health read failed: {result:?}");
    let reply: Value = serde_json::from_slice(decode_frame(&stdout).unwrap()).unwrap();
    assert_eq!(reply["command"], "controller.health");
    assert_eq!(reply["result"]["state"], "stale");
    assert_eq!(reply["result"]["reason"], "missing");
    assert!(
        !paths.state.exists(),
        "a health read must not create controller/task state"
    );
}

#[test]
fn doctor_handles_an_older_controller_without_a_protocol_error() {
    let repo = support::GitRepo::init();
    repo.write("README.md", b"fixture\n");
    repo.commit_all("fixture");
    let temp = tempfile::tempdir().unwrap();
    let paths = paths(temp.path());
    let config = Config::parse("version=1\n[controller]\nenabled=true\nssh='controller'\n[[workers]]\nname='test'\nssh='test'\nslots=1\n").unwrap();
    let report = DoctorService {
        runner: &OldController,
        config: &config,
        paths: &paths,
        laptop_processes: &mac_worker::laptop::EmptyLaptopProcessTable,
        installed_binary_mtime: None,
    }
    .inspect(DoctorRequest {
        project: repo.root().into(),
        cli_includes: Vec::new(),
    })
    .unwrap();
    let value = serde_json::to_value(&report).unwrap();
    assert_eq!(value["controller"]["state"], "unsupported", "{value}");
    let output = mac_worker::output::CommandOutput::Doctor(report).render_human();
    assert!(output.contains("upgrade"), "{output}");
    assert!(!output.contains("protocol error"));
    assert!(!output.contains("private-value"));
}

#[test]
fn old_health_is_stale_even_when_the_process_identity_matches() {
    use mac_worker::{
        controller::health::ControllerHealth,
        controller::health_read::{HealthReason, HealthState, assess_health},
        job::ProcessIdentity,
        supervisor::ProcessObservation,
    };
    let mut health = ControllerHealth::new(ProcessIdentity::new(42, 1).unwrap(), 1);
    health.last_tick_start_millis = Some(100);
    health.last_tick_end_millis = Some(200);
    health.last_success_millis = Some(200);
    let status = assess_health(
        Some(health),
        ProcessObservation::Matching { process_group: 42 },
        20_000,
    );
    assert_eq!(status.state, HealthState::Stale);
    assert_eq!(status.reason, HealthReason::TickOverdue);
}

#[test]
fn ambiguous_identity_never_becomes_a_dead_leader() {
    use mac_worker::{
        controller::health::ControllerHealth,
        controller::health_read::{HealthState, assess_health},
        job::ProcessIdentity,
        supervisor::ProcessObservation,
    };
    let health = ControllerHealth::new(ProcessIdentity::new(42, 1).unwrap(), 100);
    let unknown = assess_health(Some(health.clone()), ProcessObservation::Ambiguous, 200);
    assert_eq!(unknown.state, HealthState::Unknown);
    assert_eq!(unknown.leader_running, None);
    for observation in [ProcessObservation::Absent, ProcessObservation::Reused] {
        let stale = assess_health(Some(health.clone()), observation, 200);
        assert_eq!(stale.state, HealthState::Stale);
        assert_eq!(stale.leader_running, Some(false));
    }
}

#[test]
fn controller_status_cli_reports_an_old_record_as_stale() {
    use mac_worker::controller::{
        ControllerLeader,
        health::{ControllerHealth, HealthStore},
    };
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let state = root.join("mac-worker-controller");
    let leader = ControllerLeader::acquire(&state).unwrap();
    let mut health = ControllerHealth::new(leader.identity(), 1);
    health.last_tick_start_millis = Some(100);
    health.last_tick_end_millis = Some(200);
    HealthStore::open(&state).unwrap().write(&health).unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_worker"))
        .env_clear()
        .env("HOME", &root)
        .env("XDG_STATE_HOME", &root)
        .args(["controller", "status", "--json"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(status["state"], "stale");
    assert_eq!(status["reason"], "tick_overdue");
    assert_eq!(status["leader_running"], true);
    drop(leader);
}

#[test]
fn doctor_with_controller_only_inventory_reaches_the_health_operation() {
    use std::{collections::BTreeMap, ffi::OsString};
    let repo = support::GitRepo::init();
    repo.write("README.md", b"fixture\n");
    repo.commit_all("fixture");
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let paths = paths(&root);
    std::fs::write(
        &paths.config,
        "version=1\n[controller]\nenabled=true\nssh='controller'\n",
    )
    .unwrap();
    let environment = BTreeMap::from([
        (OsString::from("HOME"), root.as_os_str().to_owned()),
        (OsString::from("XDG_CACHE_HOME"), root.join("cache").into()),
        (OsString::from("XDG_STATE_HOME"), root.join("state").into()),
        (OsString::from("XDG_DATA_HOME"), root.join("data").into()),
    ]);
    let runtime = mac_worker::RuntimeContext::isolated(environment, root, repo.root().into());
    let cli = Cli::try_parse_from([
        "worker",
        "--config",
        paths.config.to_str().unwrap(),
        "--json",
        "doctor",
    ])
    .unwrap();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    mac_worker::run_with_io_in_context(cli, &OldController, &runtime, &mut stdout, &mut stderr);
    let report: Value = serde_json::from_slice(&stdout)
        .unwrap_or_else(|_| panic!("{}", String::from_utf8_lossy(&stderr)));
    assert_eq!(report["controller"]["state"], "unsupported");
    assert_eq!(report["workers"], json!([]));
    assert!(
        !report["issues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|issue| issue["code"] == "NO_ELIGIBLE_WORKER")
    );
}

#[test]
fn health_reply_identity_is_verified_before_doctor_uses_it() {
    struct WrongIdentity;
    impl ProcessRunner for WrongIdentity {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            let request =
                mac_worker::controller::decode_request(request.stdin.as_ref().unwrap()).unwrap();
            let result = mac_worker::controller::health_read::assess_health(
                None,
                mac_worker::supervisor::ProcessObservation::Absent,
                100,
            );
            Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stderr: Vec::new(),
                stdout: encode_json_frame(&json!({
                    "protocol_version": PROTOCOL_VERSION,
                    "command": "controller.health",
                    "request_id": "018f0f4a6b5c7d8e9f00112233445560",
                    "payload_sha256": request.payload_sha256(), "result": result,
                }))?,
            })
        }
    }
    let config =
        Config::parse("version=1\n[controller]\nenabled=true\nssh='controller'\n").unwrap();
    let status = mac_worker::controller::health_read::fetch_controller_health(
        &WrongIdentity,
        &config.controller,
    );
    assert_eq!(
        status.state,
        mac_worker::controller::health_read::HealthState::Unavailable
    );
    assert_eq!(status.error_code.as_deref(), Some("CONTROLLER_UNAVAILABLE"));
}
