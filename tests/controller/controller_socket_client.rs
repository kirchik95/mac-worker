//! T1 dependency gate, not read-loop fallback policy coverage (owned by T6).
use mac_worker::controller::channel::{client::*, testing::request_fixture};
use serde_json::json;

#[test]
fn gate_logs_bytes_require_the_follow_loop_scope() {
    let request = request_fixture(
        "task.logs",
        json!({"task_id": "0123456789ab4def8123456789abcdef"}),
    );
    assert!(eligible_read(ReadLoopScope::LogsFollow, &request));
    assert!(!eligible_read(ReadLoopScope::Wait, &request));
    assert!(!eligible_read(ReadLoopScope::EventsFollow, &request));
    assert!(!eligible_read(ReadLoopScope::Notify, &request));
}
