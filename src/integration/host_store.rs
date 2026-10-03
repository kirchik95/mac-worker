//! Host sidecar seam. T2 owns durable reads and writes.
use super::contracts::*;
use crate::{error::WorkerError, host_store::HostStore, task::TaskId};
pub struct HostIntegrationStore<'a> {
    store: &'a HostStore,
}
impl<'a> HostIntegrationStore<'a> {
    pub fn new(store: &'a HostStore) -> Self {
        Self { store }
    }
    pub fn load(
        &self,
        _project_id: &str,
        _task: TaskId,
    ) -> Result<Option<IntegrationRecord>, WorkerError> {
        Err(integration_unavailable())
    }
}
