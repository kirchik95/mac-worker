//! T1 gate only; actual leader/CLI/loop routing remains T7's responsibility.
use mac_worker::controller::channel::{server_eligible_read, testing::request_fixture};
use serde_json::json;

#[test]
fn gate_raw_operator_setters_are_ineligible() {
    for command in ["controller.drain", "task.reconcile", "task.publish-retry"] {
        assert!(!server_eligible_read(&request_fixture(command, json!({}))));
    }
}
