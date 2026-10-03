//! Standard hardened Git seam. No Git is run by the T1 facade.
use super::contracts::*;
use crate::{error::WorkerError, host_store::HostStore, process::ProcessRunner};
pub struct IntegrationGit<'a> {
    store: &'a HostStore,
    runner: &'a dyn ProcessRunner,
    runtime: &'a dyn IntegrationRuntime,
}
impl<'a> IntegrationGit<'a> {
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
    pub fn push_candidate(
        &self,
        policy: &FrozenIntegrationPolicy,
        candidate: &IntegrationCandidate,
    ) -> Result<IntegrationReceipt, WorkerError> {
        policy.validate()?;
        candidate.validate()?;
        Err(integration_unavailable())
    }
}
#[cfg(any(test, feature = "test-support"))]
pub mod testing {
    // T2 owns the real origin, constructor and Git schedules.
    pub struct GitIntegrationFixture;
}
