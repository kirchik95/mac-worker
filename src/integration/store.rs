//! Rooted owner store seam. T3 owns its durable implementation.
use super::contracts::*;
use crate::{error::WorkerError, job::ProcessIdentity, paths::PathLayout, task::TaskId};
use std::sync::Arc;
pub struct RootedIntegrationState;
impl RootedIntegrationState {
    pub fn open(
        _paths: &PathLayout,
        _runtime: Arc<dyn IntegrationRuntime>,
    ) -> Result<Self, WorkerError> {
        Err(integration_unavailable())
    }
}
impl IntegrationState for RootedIntegrationState {
    fn load(&self, _task: TaskId) -> Result<Option<IntegrationRecord>, WorkerError> {
        Err(integration_unavailable())
    }
    fn load_policy(&self, _task: TaskId) -> Result<Option<FrozenIntegrationPolicy>, WorkerError> {
        Err(integration_unavailable())
    }
    fn publish_policy(
        &self,
        _task: TaskId,
        _policy: &FrozenIntegrationPolicy,
    ) -> Result<(), WorkerError> {
        Err(integration_unavailable())
    }
    fn replace(
        &self,
        _task: TaskId,
        _expected: IntegrationRevision,
        _next: &IntegrationRecord,
    ) -> Result<bool, WorkerError> {
        Err(integration_unavailable())
    }
    fn reserve(
        &self,
        _key: &TargetKey,
        _id: IntegrationId,
        _epoch: u32,
        _actor: ProcessIdentity,
    ) -> Result<Option<TargetReservation>, WorkerError> {
        Err(integration_unavailable())
    }
    fn release(&self, _reservation: &TargetReservation) -> Result<(), WorkerError> {
        Err(integration_unavailable())
    }
    fn due(&self, _now_millis: u64, _limit: usize) -> Result<Vec<TaskId>, WorkerError> {
        Err(integration_unavailable())
    }
}
