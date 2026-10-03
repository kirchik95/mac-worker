//! Owner coordinator interface. T3 supplies the lifecycle implementation.
use super::contracts::*;
use crate::{
    error::WorkerError,
    task::{TaskId, TurnId},
};
pub struct IntegrationCoordinator<'a> {
    state: &'a dyn IntegrationState,
    host: &'a dyn IntegrationHost,
    turns: &'a dyn IntegrationTurns,
    runtime: &'a dyn IntegrationRuntime,
    observer: &'a dyn IntegrationObserver,
}
impl<'a> IntegrationCoordinator<'a> {
    pub fn new(
        state: &'a dyn IntegrationState,
        host: &'a dyn IntegrationHost,
        turns: &'a dyn IntegrationTurns,
        runtime: &'a dyn IntegrationRuntime,
        observer: &'a dyn IntegrationObserver,
    ) -> Self {
        Self {
            state,
            host,
            turns,
            runtime,
            observer,
        }
    }
    pub fn on_terminal(&self, _task: TaskId, _source: TurnId) -> Result<(), WorkerError> {
        Err(integration_unavailable())
    }
    pub fn drive_once(&self, _task: TaskId) -> Result<IntegrationSnapshot, WorkerError> {
        Err(integration_unavailable())
    }
    pub fn redrive(
        &self,
        _task: TaskId,
        _expected: IntegrationRevision,
    ) -> Result<IntegrationSnapshot, WorkerError> {
        Err(integration_unavailable())
    }
    pub fn revoke(
        &self,
        _task: TaskId,
        _expected: IntegrationRevision,
    ) -> Result<IntegrationSnapshot, WorkerError> {
        Err(integration_unavailable())
    }
}
impl PreparedIntegrationTurn {
    pub fn prepare(
        _record: &crate::task::LocalTaskRecord,
        _integration: &IntegrationRecord,
        _purpose: IntegrationTurnPurpose,
        _attempt: u8,
        _ordinal: u8,
    ) -> Result<Self, WorkerError> {
        Err(integration_unavailable())
    }
}
