use crate::support;

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
    let mut health = ControllerHealth::new(
        ProcessIdentity::new(support::fixture_pid(42), 1).unwrap(),
        1,
    );
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
    let health = ControllerHealth::new(
        ProcessIdentity::new(support::fixture_pid(42), 1).unwrap(),
        100,
    );
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
    struct MissingService;
    impl ProcessRunner for MissingService {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            assert_eq!(request.program, "/bin/launchctl");
            assert_eq!(request.args[0], "print");
            Ok(ProcessResult {
                status: ExitStatus::from_raw(113 << 8),
                stdout: Vec::new(),
                stderr: Vec::new(),
            })
        }
    }
    let runtime = mac_worker::RuntimeContext::isolated(
        std::collections::BTreeMap::from([
            ("HOME".into(), root.as_os_str().to_owned()),
            ("XDG_STATE_HOME".into(), root.as_os_str().to_owned()),
        ]),
        root.clone(),
        root.clone(),
    );
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = mac_worker::run_with_stdio_in_context(
        Cli::try_parse_from(["worker", "controller", "status", "--json"]).unwrap(),
        &MissingService,
        &runtime,
        &mut Cursor::new(Vec::new()),
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&stderr));
    let status: Value = serde_json::from_slice(&stdout).unwrap();
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

#[test]
fn health_check_against_legacy_dispatch_does_not_publish_orphan_requests() {
    use mac_worker::{
        client_state::ClientStateStore,
        controller::{ControllerStore, FakeControllerExecutor},
        task_client::TaskClient,
        turn_runner::DetachedRunnerExecutor,
    };
    struct LegacyDispatch {
        paths: PathLayout,
        config: Config,
    }
    impl ProcessRunner for LegacyDispatch {
        fn run(&self, process: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            let request = mac_worker::controller::decode_request(process.stdin.as_ref().unwrap())?;
            // The exact pre-health dispatch shape: only existing read commands
            // bypass the durable kernel. Unknown commands create a receipt
            // before their handler rejects them.
            let state = ClientStateStore::open(&self.paths.state)?;
            let client = TaskClient::new(
                &OldController,
                &self.config,
                &self.paths,
                &state,
                &DetachedRunnerExecutor,
            );
            let reply = if mac_worker::controller::is_read_command(request.command()) {
                mac_worker::controller::read::serve_read_command(&request, &client)
            } else {
                ControllerStore::open(&self.paths.controller_state_root())?
                    .handle_with(&request, &FakeControllerExecutor, ControllerFault::None)
                    .and_then(|ack| encode_json_frame(&ack))
            };
            let (status, stdout) = match reply {
                Ok(frame) => (0, frame),
                Err(error) => (
                    1 << 8,
                    encode_json_frame(
                        &HostControlError::new(error.public_code(), error.public_message())
                            .unwrap(),
                    )?,
                ),
            };
            Ok(ProcessResult {
                status: ExitStatus::from_raw(status),
                stdout,
                stderr: Vec::new(),
            })
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let paths = paths(temp.path());
    let config =
        Config::parse("version=1\n[controller]\nenabled=true\nssh='controller'\n").unwrap();
    let store = ControllerStore::open(&paths.controller_state_root()).unwrap();
    let legacy = LegacyDispatch { paths, config };
    for _ in 0..2 {
        let health = mac_worker::controller::health_read::fetch_controller_health(
            &legacy,
            &legacy.config.controller,
        );
        assert_eq!(
            health.state,
            mac_worker::controller::health_read::HealthState::Unsupported
        );
    }
    assert_eq!(
        store.pending_health(1_000).unwrap().active_count,
        0,
        "diagnostics against old controllers must not create orphan retry work"
    );
}

#[test]
fn additive_health_selector_round_trips_without_opening_a_task_store() {
    struct Loopback {
        paths: PathLayout,
        config: Config,
    }
    impl ProcessRunner for Loopback {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            let mut stdout = Vec::new();
            serve_rpc_with_runtime(
                &self.paths,
                &self.config,
                &OldController,
                &mut Cursor::new(request.stdin.as_ref().unwrap()),
                &mut stdout,
                ControllerFault::None,
            )?;
            Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout,
                stderr: Vec::new(),
            })
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let runner = Loopback {
        paths: paths(temp.path()),
        config: Config::parse("version=1\n[controller]\nenabled=true\nssh='controller'\n").unwrap(),
    };
    let status = mac_worker::controller::health_read::fetch_controller_health(
        &runner,
        &runner.config.controller,
    );
    assert_eq!(
        status.state,
        mac_worker::controller::health_read::HealthState::Stale
    );
    assert_eq!(
        status.reason,
        mac_worker::controller::health_read::HealthReason::Missing
    );
    assert!(!runner.paths.state.exists());
    assert!(!runner.paths.controller_state_root().exists());
}
