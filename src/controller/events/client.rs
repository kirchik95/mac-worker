//! Laptop event/reconciliation facade. Transport and reconciliation are T4.
pub use super::contracts::{
    AttentionSummary, BaselineKind, ChangeCause, DerivedTaskChange, EventReconciler, EventSource,
    EventSupport, PreviousProjection, ReconcileInput, Reconciliation, RepairProgress,
    TaskEligibilitySignature,
};

use super::contracts::*;
use crate::{
    config::ControllerConfig,
    controller::{
        ControllerRequest, controller_rpc_ssh_request, decode_frame, encode_json_frame,
        parse_request,
        read::{ControllerReadIdentity, ControllerReadReply},
    },
    error::WorkerError,
    job::HostControlError,
    process::ProcessRunner,
};
use serde::de::DeserializeOwned;
use std::{collections::BTreeSet, sync::Arc, time::Duration};

const LEGACY_SELECTOR_REJECTION: &str = "task.list body contained unexpected key controller_events";

pub struct ControllerEventClient {
    runner: Arc<dyn ProcessRunner>,
    controller: ControllerConfig,
    runtime: Arc<dyn EventRuntime>,
}
impl ControllerEventClient {
    pub fn new(
        runner: Arc<dyn ProcessRunner>,
        controller: ControllerConfig,
        runtime: Arc<dyn EventRuntime>,
    ) -> Self {
        Self {
            runner,
            controller,
            runtime,
        }
    }
    fn budget(&self, deadline: Duration) -> Result<Duration, WorkerError> {
        check(self.runtime.as_ref(), deadline)?;
        Ok(deadline.min(self.runtime.now().saturating_add(RPC_BUDGET)))
    }
    fn exchange<T: DeserializeOwned>(
        &self,
        request: &ControllerRequest,
        deadline: Duration,
    ) -> Result<T, WorkerError> {
        let deadline = self.budget(deadline)?;
        let mut process = controller_rpc_ssh_request(&self.controller)?;
        process.policy.deadline = deadline.saturating_sub(self.runtime.now()).min(RPC_BUDGET);
        process.stdin = Some(encode_json_frame(&serde_json::json!({
            "protocol_version": crate::protocol::PROTOCOL_VERSION,
            "request_id": request.request_id(), "command": request.command(), "body": request.body()
        }))?);
        let result = self.runner.run_interruptible(&process, &|| {
            self.runtime.cancelled() || self.runtime.now() >= deadline
        });
        check(self.runtime.as_ref(), deadline)?;
        let result = result.map_err(|_| unavailable())?;
        let bytes = decode_frame(&result.stdout).map_err(|_| unavailable())?;
        if let Ok(error) = serde_json::from_slice::<HostControlError>(bytes) {
            if request.body().get("controller_events").is_some()
                && error.error().code() == "INVALID_REQUEST"
                && error.error().message() == LEGACY_SELECTOR_REJECTION
            {
                return Err(WorkerError::Unavailable(
                    "CONTROLLER_EVENTS_UNSUPPORTED: legacy selector rejection".into(),
                ));
            }
            return Err(unavailable());
        }
        if !result.status.success() {
            return Err(unavailable());
        }
        let reply: ControllerReadReply<T> =
            serde_json::from_slice(bytes).map_err(|_| unavailable())?;
        reply.verify_envelope(request).map_err(|_| unavailable())?;
        Ok(reply.into_result())
    }
}
fn selector_request(selector: &EventSelector) -> Result<ControllerRequest, WorkerError> {
    request_body(selector.request_body()?)
}
fn request_body(body: serde_json::Value) -> Result<ControllerRequest, WorkerError> {
    parse_request(
        &serde_json::to_vec(&serde_json::json!({
            "protocol_version": crate::protocol::PROTOCOL_VERSION,
            "request_id": format!("{:x}",uuid::Uuid::new_v4().simple()),
            "command":"task.list", "body": body,
        }))
        .map_err(|_| unavailable())?,
    )
}
fn unavailable() -> WorkerError {
    WorkerError::Unavailable("CONTROLLER_EVENTS_UNAVAILABLE: event read unavailable".into())
}
fn check(runtime: &dyn EventRuntime, deadline: Duration) -> Result<(), WorkerError> {
    if runtime.cancelled() {
        return Err(WorkerError::Unavailable(
            "CONTROLLER_EVENTS_CANCELLED: cancelled".into(),
        ));
    }
    if runtime.now() >= deadline {
        return Err(unavailable());
    }
    Ok(())
}

pub(crate) fn validate_addressed(
    query: &TaskAddressQuery,
    result: &TaskFactsBatch,
) -> Result<(), WorkerError> {
    query.validate()?;
    result.validate()?;
    let requested: BTreeSet<_> = query.task_ids.iter().copied().collect();
    let returned: BTreeSet<_> = result
        .rows
        .iter()
        .map(|row| row.task_id)
        .chain(result.missing.iter().copied())
        .collect();
    if requested != returned
        || (!query.include_titles && result.rows.iter().any(|row| row.title.is_some()))
    {
        return Err(unavailable());
    }
    Ok(())
}
fn validate_read(query: &ReadQuery, result: &EventReadResult) -> Result<(), WorkerError> {
    result.validate()?;
    if let EventReadResult::Batch(batch) = result {
        let after = query.after.ok_or_else(unavailable)?;
        if after.journal_id != batch.journal_id
            || batch.events.len() > query.limit
            || batch
                .events
                .first()
                .is_some_and(|event| after.seq.checked_increment() != Some(event.seq))
            || (batch.events.is_empty() && batch.next_after != after)
        {
            return Err(unavailable());
        }
    }
    Ok(())
}
impl EventSource for ControllerEventClient {
    fn discover(&self, deadline: Duration) -> Result<EventSupport, WorkerError> {
        let deadline = self.budget(deadline)?;
        let request = request_body(serde_json::json!({"controller_health":true}))?;
        let status: crate::controller::health_read::ControllerHealthStatus =
            self.exchange(&request, deadline)?;
        status.verify_payload(&request)?;
        Ok(
            if status.features.as_ref().is_some_and(|features| {
                features
                    .iter()
                    .any(|feature| feature == "controller.events")
            }) {
                EventSupport::Supported
            } else {
                EventSupport::Unsupported
            },
        )
    }
    fn read(&self, query: ReadQuery, deadline: Duration) -> Result<EventReadResult, WorkerError> {
        let query = query.normalized();
        let result = self.exchange(
            &selector_request(&EventSelector::Read(query.clone()))?,
            deadline,
        )?;
        validate_read(&query, &result)?;
        Ok(result)
    }
    fn tasks(
        &self,
        query: TaskAddressQuery,
        deadline: Duration,
    ) -> Result<TaskFactsBatch, WorkerError> {
        query.validate()?;
        let result = self.exchange(
            &selector_request(&EventSelector::Tasks(query.clone()))?,
            deadline,
        )?;
        validate_addressed(&query, &result)?;
        Ok(result)
    }
    fn repair(
        &self,
        query: TaskRepairQuery,
        deadline: Duration,
    ) -> Result<TaskRepairPage, WorkerError> {
        let query = query.normalized();
        let result: TaskRepairPage = self.exchange(
            &selector_request(&EventSelector::Repair(query.clone()))?,
            deadline,
        )?;
        result.validate()?;
        if result.rows.len() > query.limit || result.baseline_after != query.baseline_after {
            return Err(unavailable());
        }
        Ok(result)
    }
}
