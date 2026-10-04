//! Strict integration adapters on the existing authenticated control transport.
use super::contracts::*;
use crate::{config::WorkerEntry, error::WorkerError, transfer::RemoteJobClient};
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IntegrationTurnRequest {
    pub prepared: PreparedIntegrationTurn,
    pub request: crate::turn::TaskTurnRequest,
}
impl ValidateIntegration for IntegrationTurnRequest {
    fn validate(&self) -> Result<(), WorkerError> {
        self.prepared.validate()?;
        self.request.validate()?;
        if self.request.turn().task_id() != self.prepared.followup.task_id()
            || self.request.submit().material().job_id() != self.prepared.followup.turn_id()
            || self.request.turn().limits() != &self.prepared.approved_turn_limits
        {
            return Err(super::host_store::invalid());
        }
        Ok(())
    }
}
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
        self.client.task_integration(self.worker, request)
    }
}
