//! Gated controller adapter seams. T4 owns parsing and read/mutation handling.
#![allow(dead_code)]
use super::batch::FrozenBatchBody;
use crate::{
    error::WorkerError,
    integration::{contracts::*, coordinator::IntegrationCoordinator},
    prepared_submit::FrozenSubmitBody,
    task::TaskId,
};
use std::collections::BTreeMap;
pub fn prepare_integrating_submit(
    _submit: FrozenSubmitBody,
    _integration: FrozenIntegrationPolicy,
) -> Result<FrozenIntegratingSubmit, WorkerError> {
    Err(integration_unavailable())
}
pub fn prepare_integrating_batch(
    _batch: FrozenBatchBody,
    _integrations: BTreeMap<TaskId, Option<FrozenIntegrationPolicy>>,
) -> Result<FrozenIntegratingBatch, WorkerError> {
    Err(integration_unavailable())
}
pub fn serve_integration_read(
    _state: &dyn IntegrationState,
    _task_ids: &[TaskId],
) -> Result<IntegrationReadResult, WorkerError> {
    Err(integration_unavailable())
}
pub fn prepare_integration_redrive(
    _request: &IntegrationRedriveRequest,
) -> Result<IntegrationRedriveRequest, WorkerError> {
    Err(integration_unavailable())
}
pub fn execute_integration_redrive(
    _coordinator: &IntegrationCoordinator<'_>,
    _request: &IntegrationRedriveRequest,
) -> Result<IntegrationSnapshot, WorkerError> {
    Err(integration_unavailable())
}
