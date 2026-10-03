//! Host service seam; T2 supplies all Git and storage behavior.
use super::contracts::*;
use crate::{error::WorkerError, host_store::HostStore, process::ProcessRunner};
pub struct HostIntegrationService<'a> {
    store: &'a HostStore,
    runner: &'a dyn ProcessRunner,
    runtime: &'a dyn IntegrationRuntime,
}
impl<'a> HostIntegrationService<'a> {
    pub fn new(
        store: &'a HostStore,
        runner: &'a dyn ProcessRunner,
        runtime: &'a dyn IntegrationRuntime,
    ) -> Self {
        Self {
            store,
            runner,
            runtime,
        }
    }
    pub fn execute(
        &self,
        request: &HostIntegrationRequest,
    ) -> Result<HostIntegrationResponse, WorkerError> {
        request.validate()?;
        Err(integration_unavailable())
    }
}
impl IntegrationHost for HostIntegrationService<'_> {
    fn execute(
        &self,
        request: &HostIntegrationRequest,
    ) -> Result<HostIntegrationResponse, WorkerError> {
        HostIntegrationService::execute(self, request)
    }
}
