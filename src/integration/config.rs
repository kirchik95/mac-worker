//! Pure policy/configuration seams. T4 owns resolution and preflight.
use super::contracts::*;
use crate::{
    error::WorkerError,
    process::ProcessRunner,
    rooted_fs::RootedDir,
    task::{BaseOid, BranchName, ClosePolicy, TaskId},
};
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrationPolicySettings {
    #[serde(default, skip_serializing_if = "IntegrationOverride::is_inherit")]
    pub integrate: IntegrationOverride,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_merge: Option<VerifyPolicy>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrationProjectPolicy {
    pub settings: IntegrationPolicySettings,
    pub project_id: String,
    pub base_oid: Option<BaseOid>,
    pub base_task: Option<TaskId>,
}
pub fn resolve_integration_policy(
    _project: &IntegrationProjectPolicy,
    _batch_default: Option<&IntegrationPolicySettings>,
    _task_override: &IntegrationOverride,
    _verify_override: Option<VerifyPolicy>,
    _requested_close: ClosePolicy,
    _origin: &str,
    _base_kind: IntegrationBaseKind,
) -> Result<Option<FrozenIntegrationPolicy>, WorkerError> {
    Err(integration_unavailable())
}
pub fn preflight_integration_base(
    _runner: &dyn ProcessRunner,
    _origin: &str,
    _branch: &BranchName,
    _base: Option<&BaseOid>,
    _local_repo: &RootedDir,
) -> Result<IntegrationBasePreflight, WorkerError> {
    Err(integration_unavailable())
}
