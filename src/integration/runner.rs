//! Detached driver seam. T3 owns execution and recovery.
use super::{contracts::*, coordinator::IntegrationCoordinator};
use crate::{error::WorkerError, task::TaskId};
pub struct IntegrationRunner<'a> {
    coordinator: IntegrationCoordinator<'a>,
}
impl<'a> IntegrationRunner<'a> {
    pub fn new(coordinator: IntegrationCoordinator<'a>) -> Self {
        Self { coordinator }
    }
    pub fn run(&self, _task: TaskId) -> Result<IntegrationSnapshot, WorkerError> {
        Err(integration_unavailable())
    }
}
