//! Nonblocking controller read service and bounded native jobs.
pub use super::contracts::{ChannelExecutor, ChildRpcSpec, ServerContext};
pub use crate::process::{CleanupState, ProcessCompletion, TrackedProcessRunner};
pub mod child;
pub mod control;
pub use child::ChildRpcExecutor;
pub use control::NativeControl;

// Phase A bridge; Phase B replaces this with T1's frozen grammar predicate.
fn eligible_request(request: &crate::controller::ControllerRequest) -> bool {
    use serde_json::Value;
    let Some(body) = request.body().as_object() else {
        return false;
    };
    let task_id = |key: &str| {
        body.get(key)
            .and_then(Value::as_str)
            .is_some_and(|text| text.parse::<crate::task::TaskId>().is_ok())
    };
    match request.command() {
        "task.wait.poll" => {
            body.len() == 1
                && (task_id("task_id")
                    || body.get("run").and_then(Value::as_str).is_some_and(|text| {
                        !text.is_empty()
                            && text.len() <= 1024
                            && !text.chars().any(char::is_control)
                    }))
        }
        "task.logs" => {
            task_id("task_id")
                && body.iter().all(|(key, value)| match key.as_str() {
                    "task_id" => true,
                    "turn" => value.as_u64().is_some_and(|n| u32::try_from(n).is_ok()),
                    "turn_id" => value
                        .as_str()
                        .is_some_and(|s| s.parse::<crate::task::TurnId>().is_ok()),
                    "offset" | "limit" | "wait_ms" => value.as_u64().is_some(),
                    "raw" | "follow" => value.is_boolean(),
                    _ => false,
                })
        }
        "task.list" => {
            request.body() == &serde_json::json!({"controller_health": true})
                || crate::controller::events::rpc::EventSelector::from_request_body(request.body())
                    .is_ok()
        }
        _ => false,
    }
}
