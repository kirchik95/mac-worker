//! Existing transport adapter seam. No transport is invoked by T1.
use super::contracts::*;
use crate::{config::WorkerEntry, error::WorkerError, transfer::RemoteJobClient};
pub struct RemoteIntegrationHost<'a> {
    client: &'a RemoteJobClient<'a>,
    worker: &'a WorkerEntry,
}
impl<'a> RemoteIntegrationHost<'a> {
    pub fn new(client: &'a RemoteJobClient<'a>, worker: &'a WorkerEntry) -> Self {
        Self { client, worker }
    }
}
impl IntegrationHost for RemoteIntegrationHost<'_> {
    fn execute(
        &self,
        request: &HostIntegrationRequest,
    ) -> Result<HostIntegrationResponse, WorkerError> {
        request.validate()?;
        Err(integration_unavailable())
    }
}
