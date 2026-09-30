//! Safe selector facade. Serving RPC and state-only readers are implemented by T4.
pub use super::contracts::{
    EventReadResult, EventSelector, JournalProvider, OpaqueCursor, ReadQuery, TaskAddressQuery,
    TaskFacts, TaskFactsBatch, TaskFactsWire, TaskProjectionProvider, TaskProjectionReader,
    TaskRepairPage, TaskRepairQuery, ensure_frame_bound,
};

#[path = "task_reads.rs"]
pub mod task_reads;
pub use task_reads::{ExistingTaskProjectionProvider, TaskEventReadStore};

use crate::{
    controller::{ControllerRequest, encode_json_frame, read::ControllerReadReply},
    error::WorkerError,
};
use serde::Serialize;
use std::time::Duration;

pub fn is_event_selector(request: &ControllerRequest) -> bool {
    request.command() == "task.list" && request.body().get("controller_events").is_some()
}

pub fn serve_selector_with(
    request: &ControllerRequest,
    journal: &dyn JournalProvider,
    tasks: &dyn TaskProjectionProvider,
    deadline: Duration,
) -> Result<Vec<u8>, WorkerError> {
    if !is_event_selector(request) {
        return Err(WorkerError::Protocol(
            "CONTROLLER_EVENTS_INVALID: invalid selector command".into(),
        ));
    }
    // Validate the complete grammar before opening either provider. The T8
    // dispatcher can route even malformed selectors here without mutation.
    match EventSelector::from_request_body(request.body())? {
        EventSelector::Read(query) => {
            let reader = journal.open_existing(deadline)?.ok_or_else(|| {
                WorkerError::Unavailable("CONTROLLER_EVENTS_UNAVAILABLE: journal absent".into())
            })?;
            let result = reader.read(query.normalized(), deadline)?;
            result.validate()?;
            encode_reply(request, result)
        }
        EventSelector::Tasks(query) => {
            let result = tasks
                .open_existing(deadline)?
                .addressed(query.clone(), deadline)?;
            super::client::validate_addressed(&query, &result)?;
            encode_reply(request, result)
        }
        EventSelector::Repair(query) => {
            // Baseline H is captured by the consumer before enumeration. A
            // state-only call never needs to open or lock an event journal.
            let query = query.normalized();
            let result = tasks
                .open_existing(deadline)?
                .repair(query.clone(), deadline)?;
            result.validate()?;
            if result.baseline_after != query.baseline_after {
                return Err(WorkerError::Protocol(
                    "CONTROLLER_EVENTS_INVALID: repair baseline changed".into(),
                ));
            }
            encode_reply(request, result)
        }
    }
}

fn encode_reply<T: Serialize>(
    request: &ControllerRequest,
    result: T,
) -> Result<Vec<u8>, WorkerError> {
    let reply = ControllerReadReply::from_request(request, result);
    ensure_frame_bound(&reply)?;
    encode_json_frame(&reply)
}
