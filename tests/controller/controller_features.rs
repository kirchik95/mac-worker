//! Additive protocol discovery, using only isolated state and fake transports.
use std::{
    collections::BTreeMap, ffi::OsString, io::Cursor, os::unix::process::ExitStatusExt,
    process::ExitStatus, sync::Mutex,
};

use clap::Parser;
use mac_worker::{
    RuntimeContext,
    cli::Cli,
    config::Config,
    controller::{
        decode_frame, decode_request, encode_json_frame,
        health_read::{
            ControllerHealthStatus, HealthState, fetch_controller_health, serve_health_read,
        },
    },
    error::WorkerError,
    job::HostControlError,
    output::CommandOutput,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::{PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION, WorkersReport},
    transport::SshTransport,
};
use serde_json::{Value, json};

fn config() -> Config {
    Config::parse("version=1\n[controller]\nenabled=true\nssh='controller'\n[[workers]]\nname='test'\nssh='test'\nslots=1\n").unwrap()
}

fn probe_json() -> Value {
    json!({
        "protocol_version": PROTOCOL_VERSION, "supervision_version": SUPERVISION_VERSION,
        "hostname": "test.local", "arch": "arm64", "os_version": "26.2",
        "free_disk_bytes": 100, "total_disk_bytes": 200, "memory_pressure": "normal",
        "swap_used_bytes": 0, "slot_state": "idle", "active_lease": null,
        "capabilities": ["git"], "future_field": true,
    })
}

struct ProbeRunner(Value);
impl ProcessRunner for ProbeRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(
            request.args.last().unwrap(),
            "~/.local/bin/worker host probe"
        );
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: serde_json::to_vec(&self.0).unwrap(),
            stderr: Vec::new(),
        })
    }
}

#[test]
fn probe_features_preserve_old_unknown_and_new_authoritative_lists() {
    for features in [
        None,
        Some(json!([])),
        Some(json!(["host.future-feature", "future.feature"])),
    ] {
        let mut value = probe_json();
        if let Some(features) = &features {
            value["features"] = features.clone();
        }
        let runner = ProbeRunner(value);
        let health = SshTransport::new(&runner).probe(&config().workers[0]);
        let probe = health
            .probe
            .as_ref()
            .expect("valid old and new probes decode");
        let encoded = serde_json::to_value(probe).unwrap();
        assert_eq!(encoded.get("features"), features.as_ref());
        assert_eq!(probe.capabilities, ["git"]);
        let report = CommandOutput::Workers(WorkersReport {
            protocol_version: PROTOCOL_VERSION,
            workers: vec![health],
        });
        let json: Value = serde_json::from_str(&report.render_json().unwrap()).unwrap();
        assert_eq!(
            json["workers"][0]["features"],
            features.unwrap_or(Value::Null)
        );
    }
    let mut invalid = probe_json();
    invalid["features"] = json!("not a list");
    assert!(serde_json::from_value::<ProbeResponse>(invalid).is_err());
}

struct HealthRunner {
    root: tempfile::TempDir,
    // None: serving binary; Some: supplied old/new shape; Err: pre-health controller.
    reply: Result<Option<Value>, ()>,
    requests: Mutex<usize>,
}
impl HealthRunner {
    fn new(reply: Result<Option<Value>, ()>) -> Self {
        Self {
            root: tempfile::tempdir().unwrap(),
            reply,
            requests: Mutex::new(0),
        }
    }
}
impl ProcessRunner for HealthRunner {
    fn run(&self, process: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let request = decode_request(process.stdin.as_ref().unwrap())?;
        let health = request.command() == "task.list"
            && request.body() == &json!({"controller_health": true});
        let (code, stdout) = if health {
            *self.requests.lock().unwrap() += 1;
            match &self.reply {
                Ok(None) => (
                    0,
                    serve_health_read(&request, &self.root.path().join("absent"))?,
                ),
                Ok(Some(result)) => (
                    0,
                    encode_json_frame(&json!({
                        "protocol_version": PROTOCOL_VERSION, "command": request.command(),
                        "request_id": request.request_id(), "payload_sha256": request.payload_sha256(),
                        "result": result,
                    }))?,
                ),
                Err(()) => (
                    1,
                    encode_json_frame(
                        &HostControlError::new("INVALID_REQUEST", "old controller").unwrap(),
                    )?,
                ),
            }
        } else {
            (
                1,
                encode_json_frame(
                    &HostControlError::new("INVALID_REQUEST", "unsupported diagnostic").unwrap(),
                )?,
            )
        };
        Ok(ProcessResult {
            status: ExitStatus::from_raw(code << 8),
            stdout,
            stderr: Vec::new(),
        })
    }
}

fn old_health() -> Value {
    json!({"state": "stale", "reason": "missing", "future_field": true})
}

#[test]
fn health_features_describe_the_serving_binary_even_without_a_leader() {
    let runner = HealthRunner::new(Ok(None));
    let status = fetch_controller_health(&runner, &config().controller);
    assert_eq!(status.state, HealthState::Stale);
    assert_eq!(
        serde_json::to_value(status).unwrap()["features"],
        json!(["controller.events", "controller.task-logs-wait"])
    );
    assert_eq!(*runner.requests.lock().unwrap(), 1);
    assert!(!runner.root.path().join("absent").exists());
}

#[test]
fn health_features_are_unknown_for_old_shapes_and_unsupported_controllers() {
    let old: ControllerHealthStatus = serde_json::from_value(old_health()).unwrap();
    assert!(serde_json::to_value(old).unwrap().get("features").is_none());
    for reply in [Ok(Some(old_health())), Err(())] {
        let runner = HealthRunner::new(reply);
        let status = fetch_controller_health(&runner, &config().controller);
        assert!(
            serde_json::to_value(status)
                .unwrap()
                .get("features")
                .is_none()
        );
    }
}

#[test]
fn controller_status_prints_features_in_plain_and_json_output() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path().canonicalize().unwrap();
    let config_path = root.join("config.toml");
    std::fs::write(
        &config_path,
        "version=1\n[controller]\nenabled=true\nssh='controller'\n",
    )
    .unwrap();
    let runtime = RuntimeContext::isolated(
        BTreeMap::from([
            (OsString::from("XDG_STATE_HOME"), root.join("state").into()),
            (OsString::from("XDG_CACHE_HOME"), root.join("cache").into()),
            (OsString::from("XDG_DATA_HOME"), root.join("data").into()),
        ]),
        root.clone(),
        root.clone(),
    );
    for (reply, expected) in [
        (
            Ok(None),
            "features: controller.events, controller.task-logs-wait",
        ),
        (Ok(Some(old_health())), "features: unknown"),
        (Err(()), "features: unknown"),
    ] {
        for json_mode in [false, true] {
            let runner = HealthRunner::new(reply.clone());
            let mut args = vec![
                "worker",
                "--config",
                config_path.to_str().unwrap(),
                "controller",
                "status",
            ];
            if json_mode {
                args.push("--json");
            }
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            let exit = mac_worker::run_with_stdio_in_context(
                Cli::try_parse_from(args).unwrap(),
                &runner,
                &runtime,
                &mut Cursor::new(Vec::new()),
                &mut stdout,
                &mut stderr,
            );
            assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&stderr));
            if json_mode {
                let value: Value = serde_json::from_slice(&stdout).unwrap();
                assert_eq!(
                    value["features"],
                    if expected.ends_with("unknown") {
                        Value::Null
                    } else {
                        json!(["controller.events", "controller.task-logs-wait"])
                    }
                );
            } else {
                assert!(
                    String::from_utf8(stdout)
                        .unwrap()
                        .lines()
                        .any(|line| line == expected)
                );
            }
        }
    }
}

#[test]
fn health_frames_keep_the_existing_envelope_shape() {
    let request = mac_worker::controller::parse_request(
        &serde_json::to_vec(&json!({
            "protocol_version": PROTOCOL_VERSION, "request_id": "018f0f4a6b5c7d8e9f00112233445560",
            "command": "task.list", "body": {"controller_health": true},
        }))
        .unwrap(),
    )
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let frame = serve_health_read(&request, &root.path().join("absent")).unwrap();
    let reply: Value = serde_json::from_slice(decode_frame(&frame).unwrap()).unwrap();
    assert_eq!(reply.as_object().unwrap().len(), 5);
    assert_eq!(reply["protocol_version"], 7);
    assert_eq!(reply["payload_sha256"], request.payload_sha256());
}
