//! Projection seam. T5 supplies the shared projection.
use super::contracts::*;
use crate::error::WorkerError;
// Fallible while unwired, as required by the T1 brief; no success projection.
pub fn project_integration(
    _snapshot: Option<&IntegrationSnapshot>,
    _facts: &IntegrationTaskFacts,
) -> Result<IntegrationView, WorkerError> {
    Err(integration_unavailable())
}
