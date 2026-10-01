//! T1 nonignored gate; no observation or speed claim until T7d's real fixture.
use mac_worker::controller::channel::{server_eligible_read, testing::request_fixture};
use serde_json::json;

#[test]
fn gate_measurement_reads_exclude_mutation_traffic() {
    assert!(server_eligible_read(&request_fixture(
        "task.wait.poll",
        json!({"run": "fixture"})
    )));
    assert!(!server_eligible_read(&request_fixture(
        "task.submit",
        json!({"run": "fixture"})
    )));
}
