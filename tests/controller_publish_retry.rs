#[allow(dead_code)]
#[path = "support/controller_gap.rs"]
mod fixture;

use std::{ffi::OsString, os::unix::process::ExitStatusExt, process::ExitStatus};

use fixture::{ControllerBridge, IsolatedHost, NoProcesses};
use mac_worker::{
    client_state::ClientStateStore,
    controller::{decode_frame, encode_json_frame},
    error::WorkerError,
    outbox::OutboxRetryResponse,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::PROTOCOL_VERSION,
    task::{DeliveryState, LocalTaskRecord, OriginDelivery, TaskId, TaskState},
    transfer::HostOperation,
};
use serde_json::{Value, json};

struct OutboxWorker {
    task_id: TaskId,
    delivery: OriginDelivery,
}

impl OutboxWorker {
    fn retrying(record: &LocalTaskRecord) -> Self {
        Self {
            task_id: record.meta().task_id(),
            delivery: OriginDelivery::new(
                record.status().turns()[0].turn_id(),
                DeliveryState::Retrying,
                record.meta().base_oid().clone(),
                "https://example.test/repo".into(),
                "refs/heads/retry-fixture".into(),
                0,
                1,
                None,
                None,
                1,
                3,
            )
            .unwrap(),
        }
    }
}

impl ProcessRunner for OutboxWorker {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(request.program, OsString::from("/usr/bin/ssh"));
        assert!(request.args.iter().any(|arg| arg == "fixture-worker"));
        assert_eq!(
            request.args.last().unwrap().to_string_lossy(),
            format!("{} {}", HostOperation::OutboxRetry.command(), self.task_id)
        );
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: serde_json::to_vec(&OutboxRetryResponse::new(vec![self.delivery.clone()]))
                .unwrap(),
            stderr: Vec::new(),
        })
    }
}

#[test]
fn publish_retry_routes_through_controller_and_persists_deliveries_there() {
    let controller = IsolatedHost::new(true);
    let laptop = IsolatedHost::new(false);
    let record = controller.seed(true, false);
    let task_id = record.meta().task_id().to_string();
    let worker = OutboxWorker::retrying(&record);
    let bridge = ControllerBridge {
        controller: &controller,
        worker: &worker,
    };
    let (exit, stdout, stderr) = laptop.cli(
        &bridge,
        &["worker", "--json", "task", "publish-retry", &task_id],
    );
    assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&stderr));
    let report: Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(report["task_id"], task_id);
    assert_eq!(report["protocol_version"], PROTOCOL_VERSION);
    assert_eq!(report["deliveries"][0]["state"], "retrying");
    assert_eq!(report["deliveries"][0]["attempt"], 0);
    assert_eq!(report["deliveries"].as_array().unwrap().len(), 1);
    let saved = ClientStateStore::open(&controller.paths.state)
        .unwrap()
        .load_task(record.meta().task_id())
        .unwrap();
    assert_eq!(saved.deliveries(), std::slice::from_ref(&worker.delivery));
    assert_eq!(saved.status().state(), TaskState::Closed);
    assert!(
        !laptop.paths.state.exists(),
        "laptop must not open a local task store"
    );

    let (exit, stdout, stderr) =
        laptop.cli(&bridge, &["worker", "task", "publish-retry", &task_id]);
    assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&stderr));
    assert!(
        String::from_utf8(stdout)
            .unwrap()
            .contains("delivery: retrying turn=")
    );
}

#[test]
fn publish_retry_keeps_task_errors_without_contacting_worker() {
    let controller = IsolatedHost::new(true);
    let laptop = IsolatedHost::new(false);
    let record = controller.seed(false, false);
    let bridge = ControllerBridge {
        controller: &controller,
        worker: &NoProcesses,
    };
    for (task_id, expected) in [
        (record.meta().task_id(), "TASK_CONFIG_INVALID"),
        (TaskId::generate(), "TASK_NOT_FOUND"),
    ] {
        let (exit, stdout, stderr) = laptop.cli(
            &bridge,
            &["worker", "task", "publish-retry", &task_id.to_string()],
        );
        assert_ne!(exit, 0);
        assert!(stdout.is_empty());
        assert!(
            String::from_utf8_lossy(&stderr).contains(expected),
            "{}",
            String::from_utf8_lossy(&stderr)
        );
    }
    assert!(!laptop.paths.state.exists());
}

#[test]
fn publish_retry_rejects_extra_request_fields_before_contacting_worker() {
    let controller = IsolatedHost::new(true);
    let record = controller.seed(true, false);
    let response = controller.rpc(
        &NoProcesses,
        "task.publish-retry",
        json!({ "task_id": record.meta().task_id(), "worker": "untrusted" }),
    );
    assert!(!response.status.success());
    let reply: Value = serde_json::from_slice(decode_frame(&response.stdout).unwrap()).unwrap();
    assert_eq!(reply["error"]["code"], "INVALID_REQUEST");
    assert!(
        ClientStateStore::open(&controller.paths.state)
            .unwrap()
            .load_task(record.meta().task_id())
            .unwrap()
            .deliveries()
            .is_empty()
    );
}

struct ModifiedReply<'a> {
    bridge: ControllerBridge<'a>,
    field: &'static str,
    value: Value,
}

impl ProcessRunner for ModifiedReply<'_> {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let mut response = self.bridge.run(request)?;
        let mut reply: Value =
            serde_json::from_slice(decode_frame(&response.stdout).unwrap()).unwrap();
        if self.field == "task_id" || self.field == "warnings" {
            reply["result"][self.field] = self.value.clone();
        } else {
            reply[self.field] = self.value.clone();
        }
        response.stdout = encode_json_frame(&reply).unwrap();
        Ok(response)
    }
}

#[test]
fn publish_retry_rejects_controller_reply_with_wrong_identity() {
    let controller = IsolatedHost::new(true);
    let laptop = IsolatedHost::new(false);
    let record = controller.seed(true, false);
    let worker = OutboxWorker::retrying(&record);
    for (field, value) in [
        ("task_id", json!(TaskId::generate())),
        ("request_id", json!("018f0f4a6b5c7d8e9f00112233445599")),
        ("payload_sha256", json!("0".repeat(64))),
    ] {
        let runner = ModifiedReply {
            bridge: ControllerBridge {
                controller: &controller,
                worker: &worker,
            },
            field,
            value,
        };
        let (exit, stdout, stderr) = laptop.cli(
            &runner,
            &[
                "worker",
                "task",
                "publish-retry",
                &record.meta().task_id().to_string(),
            ],
        );
        assert_ne!(exit, 0);
        assert!(stdout.is_empty());
        assert!(
            String::from_utf8_lossy(&stderr).contains("CONTROLLER_UNAVAILABLE"),
            "{}",
            String::from_utf8_lossy(&stderr)
        );
    }
}

#[test]
fn publish_retry_prints_controller_persistence_warnings() {
    let controller = IsolatedHost::new(true);
    let laptop = IsolatedHost::new(false);
    let record = controller.seed(true, false);
    let worker = OutboxWorker::retrying(&record);
    let warning = "retry succeeded; delivery status could not be saved";
    let runner = ModifiedReply {
        bridge: ControllerBridge {
            controller: &controller,
            worker: &worker,
        },
        field: "warnings",
        value: json!([warning]),
    };
    for json_output in [false, true] {
        let task_id = record.meta().task_id().to_string();
        let mut args = vec!["worker", "task", "publish-retry", &task_id];
        if json_output {
            args.push("--json");
        }
        let (exit, stdout, stderr) = laptop.cli(&runner, &args);
        assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&stderr));
        if json_output {
            let report: Value = serde_json::from_slice(&stdout).unwrap();
            assert_eq!(report["warnings"], json!([warning]));
        } else {
            assert!(
                String::from_utf8(stdout)
                    .unwrap()
                    .contains(&format!("warning: {warning}"))
            );
        }
    }
}
