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
    pub fn run(&self, task: TaskId) -> Result<IntegrationSnapshot, WorkerError> {
        let mut snapshot = self
            .coordinator
            .snapshot(task)?
            .ok_or_else(integration_unavailable)?;
        // Yield to selected recovery rather than wait for clocks, queue slots or agents.
        // A detached child also bounds a non-progressing capable peer.
        for _ in 0..32 {
            let revision = snapshot.revision;
            snapshot = self.coordinator.drive_once(task)?;
            if snapshot.revision == revision || !self.coordinator.ready_to_drive(task)? {
                break;
            }
        }
        Ok(snapshot)
    }
}
