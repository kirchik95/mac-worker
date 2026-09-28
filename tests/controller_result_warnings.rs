#[allow(dead_code)]
#[path = "support/controller_gap.rs"]
mod fixture;

use fixture::{ControllerBridge, IsolatedHost, NoProcesses};
use mac_worker::{controller::decode_frame, protocol::PROTOCOL_VERSION};
use serde_json::{Value, json};

const WARNING: &str = "requested workspace permission for claude has no workspace sandbox; effective permission is unattended";

#[test]
fn result_carries_effective_permission_warning_from_controller_to_laptop() {
    let controller = IsolatedHost::new(true);
    let laptop = IsolatedHost::new(false);
    let record = controller.seed(false, true);
    let task_id = record.meta().task_id().to_string();
    let bridge = ControllerBridge {
        controller: &controller,
        worker: &NoProcesses,
    };

    let (exit, stdout, stderr) =
        laptop.cli(&bridge, &["worker", "--json", "task", "result", &task_id]);
    assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&stderr));
    let result: Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(result["warnings"], json!([WARNING]));
    assert_eq!(result["task_id"], task_id);
    assert_eq!(result["protocol_version"], PROTOCOL_VERSION);
    assert!(
        !laptop.paths.state.exists(),
        "laptop must not open a local task store"
    );

    let (exit, stdout, stderr) = laptop.cli(&bridge, &["worker", "task", "result", &task_id]);
    assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&stderr));
    assert!(
        String::from_utf8(stdout)
            .unwrap()
            .contains(&format!("warning: {WARNING}"))
    );
}

#[test]
fn result_legacy_laptop_decodes_new_reply_for_a_task_with_warnings() {
    let controller = IsolatedHost::new(true);
    let record = controller.seed(false, true);
    let response = controller.rpc(
        &NoProcesses,
        "task.result",
        json!({"task_id":record.meta().task_id()}),
    );
    assert!(response.status.success());
    assert_eq!(
        legacy::decode_result(decode_frame(&response.stdout).unwrap()).unwrap(),
        record.meta().task_id()
    );
    let status = controller.rpc(
        &NoProcesses,
        "task.status",
        json!({"task_id":record.meta().task_id()}),
    );
    assert_eq!(
        legacy::decode_status(decode_frame(&status.stdout).unwrap()),
        [WARNING]
    );
}

#[test]
fn result_new_laptop_reads_warnings_from_an_old_controller() {
    let controller = IsolatedHost::new(true);
    let record = controller.seed(false, true);
    let laptop = IsolatedHost::new(false);
    let runner = legacy::OldController {
        task_id: record.meta().task_id(),
        status: record.status().clone(),
        wrong_status_task: false,
    };
    let (exit, out, err) = laptop.cli(
        &runner,
        &[
            "worker",
            "--json",
            "task",
            "result",
            &record.meta().task_id().to_string(),
        ],
    );
    assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&err));
    let result: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(result["warnings"], json!([WARNING]));
    assert_eq!(result["task_id"], record.meta().task_id().to_string());
    assert!(!laptop.paths.state.exists());
}

#[test]
fn result_warning_status_read_checks_task_identity() {
    let controller = IsolatedHost::new(true);
    let record = controller.seed(false, true);
    let laptop = IsolatedHost::new(false);
    let runner = legacy::OldController {
        task_id: record.meta().task_id(),
        status: record.status().clone(),
        wrong_status_task: true,
    };
    let (exit, out, _) = laptop.cli(
        &runner,
        &[
            "worker",
            "--json",
            "task",
            "result",
            &record.meta().task_id().to_string(),
        ],
    );
    assert_ne!(exit, 0);
    let error: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(error["code"], "CONTROLLER_UNAVAILABLE");
    assert!(error.get("warnings").is_none());
    assert!(!laptop.paths.state.exists());
}

mod legacy {
    use mac_worker::{
        controller::{decode_frame, encode_json_frame, parse_request},
        error::WorkerError,
        process::{ProcessRequest, ProcessResult, ProcessRunner},
        protocol::PROTOCOL_VERSION,
        task::{OriginDelivery, RunId, RunnerState, TaskId, TaskStatus},
    };
    use serde::{Deserialize, Serialize};
    use serde_json::{Value, json};
    use std::{os::unix::process::ExitStatusExt, process::ExitStatus};
    // These four DTO declarations are copied verbatim from ed8137f:
    // src/controller/read.rs and src/controller/protocol.rs (WireRequest).
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct ControllerReadReply<T> {
        protocol_version: u32,
        command: String,
        request_id: String,
        payload_sha256: String,
        result: T,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct ControllerTaskResult {
        task_id: TaskId,
        status: TaskStatus,
        branch: String,
        fetch: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stage: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        residual: Option<Vec<String>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery: Option<OriginDelivery>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        deliveries: Vec<OriginDelivery>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct ControllerTaskStatusResult {
        task_id: TaskId,
        run_id: Option<RunId>,
        status: TaskStatus,
        #[serde(default)]
        warnings: Vec<String>,
        #[serde(default)]
        events: Vec<Value>,
        runner: Option<RunnerState>,
        exit_code: Option<u8>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery: Option<OriginDelivery>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        deliveries: Vec<OriginDelivery>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stage: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        residual: Option<Vec<String>>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct WireRequest {
        protocol_version: u32,
        request_id: String,
        #[serde(default, rename = "payload_sha256")]
        _ignored_digest: Option<String>,
        command: String,
        body: Value,
    }

    pub fn decode_result(bytes: &[u8]) -> Result<TaskId, serde_json::Error> {
        Ok(
            serde_json::from_slice::<ControllerReadReply<ControllerTaskResult>>(bytes)?
                .result
                .task_id,
        )
    }
    pub fn decode_status(bytes: &[u8]) -> Vec<String> {
        serde_json::from_slice::<ControllerReadReply<ControllerTaskStatusResult>>(bytes)
            .unwrap()
            .result
            .warnings
    }
    pub struct OldController {
        pub task_id: TaskId,
        pub status: TaskStatus,
        pub wrong_status_task: bool,
    }
    impl ProcessRunner for OldController {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            assert_eq!(request.program, "/usr/bin/ssh");
            assert_eq!(
                request.args.last().unwrap(),
                "~/.local/bin/worker host controller-rpc"
            );
            let bytes = decode_frame(request.stdin.as_ref().unwrap()).unwrap();
            let wire: WireRequest = serde_json::from_slice(bytes).unwrap();
            assert_eq!(wire.protocol_version, PROTOCOL_VERSION);
            assert_eq!(wire.body, json!({"task_id":self.task_id}));
            let digest = parse_request(bytes).unwrap().payload_sha256().to_owned();
            let payload = match wire.command.as_str() {
                "task.result" => serde_json::to_value(ControllerTaskResult {
                    task_id: self.task_id,
                    status: self.status.clone(),
                    branch: "worker/result".into(),
                    fetch: "worker task fetch".into(),
                    stage: None,
                    residual: None,
                    delivery: None,
                    deliveries: vec![],
                })
                .unwrap(),
                "task.status" => serde_json::to_value(ControllerTaskStatusResult {
                    task_id: if self.wrong_status_task {
                        TaskId::generate()
                    } else {
                        self.task_id
                    },
                    run_id: None,
                    status: self.status.clone(),
                    warnings: vec![super::WARNING.into()],
                    events: vec![],
                    runner: None,
                    exit_code: Some(0),
                    delivery: None,
                    deliveries: vec![],
                    stage: None,
                    residual: None,
                })
                .unwrap(),
                command => panic!("old controller does not support {command}"),
            };
            Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout: encode_json_frame(&ControllerReadReply {
                    protocol_version: PROTOCOL_VERSION,
                    command: wire.command,
                    request_id: wire.request_id,
                    payload_sha256: digest,
                    result: payload,
                })
                .unwrap(),
                stderr: vec![],
            })
        }
    }
}
