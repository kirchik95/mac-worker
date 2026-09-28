#[allow(dead_code)]
#[path = "support/controller_gap.rs"]
mod fixture;

use fixture::{ControllerBridge, IsolatedHost, NoProcesses};
use mac_worker::{
    controller::{ControllerTaskResult, decode_frame},
    protocol::PROTOCOL_VERSION,
};
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
fn result_accepts_older_controller_payload_without_warnings() {
    let controller = IsolatedHost::new(true);
    let record = controller.seed(false, false);
    let response = controller.rpc(
        &NoProcesses,
        "task.result",
        json!({ "task_id": record.meta().task_id() }),
    );
    assert!(response.status.success());
    let reply: Value = serde_json::from_slice(decode_frame(&response.stdout).unwrap()).unwrap();
    let mut legacy = reply["result"].clone();
    legacy.as_object_mut().unwrap().remove("warnings");
    let result: ControllerTaskResult = serde_json::from_value(legacy).unwrap();
    assert!(result.into_report().warnings().is_empty());
}
