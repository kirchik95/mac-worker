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
    project: &IntegrationProjectPolicy,
    batch_default: Option<&IntegrationPolicySettings>,
    task_override: &IntegrationOverride,
    verify_override: Option<VerifyPolicy>,
    requested_close: ClosePolicy,
    origin: &str,
    base_kind: IntegrationBaseKind,
) -> Result<Option<FrozenIntegrationPolicy>, WorkerError> {
    let Some((target, verify)) = resolve_integration_settings(
        &project.settings,
        batch_default,
        task_override,
        verify_override,
    )?
    else {
        return Ok(None);
    };
    // Normalize configured origin, never discover a default remote or target.
    let key = TargetKey::new(origin, target.as_str())
        .map_err(|_| integration_error("TASK_CONFIG_INVALID"))?;
    let policy = FrozenIntegrationPolicy {
        schema_version: INTEGRATION_SCHEMA_VERSION,
        origin: key.origin,
        target,
        verify,
        requested_close,
        base_kind,
        base_oid: project.base_oid.clone(),
        base_task: project.base_task,
        base_preflight: IntegrationBasePreflight::Unknown,
        project_id: project.project_id.clone(),
    };
    policy
        .validate()
        .map_err(|_| integration_error("TASK_CONFIG_INVALID"))?;
    Ok(Some(policy))
}

/// Resolve inputs independently of base capture, so previews and refusal gates
/// can validate opt-in before any pin, snapshot, session capture or admission.
pub fn resolve_integration_settings(
    project: &IntegrationPolicySettings,
    batch: Option<&IntegrationPolicySettings>,
    task: &IntegrationOverride,
    verify: Option<VerifyPolicy>,
) -> Result<Option<(BranchName, VerifyPolicy)>, WorkerError> {
    let effective = [
        Some(task),
        batch.map(|value| &value.integrate),
        Some(&project.integrate),
    ]
    .into_iter()
    .flatten()
    .find(|value| !value.is_inherit());
    let effective_verify = verify
        .or(batch.and_then(|value| value.verify_merge))
        .or(project.verify_merge);
    match effective {
        Some(IntegrationOverride::Target(branch)) => Ok(Some((
            validate_integration_target(branch.as_str())?,
            effective_verify.unwrap_or_default(),
        ))),
        _ => {
            // Disabling a target also disables inherited verification. An
            // explicit verify override with no effective target is still invalid.
            if verify.is_some() || (effective.is_none() && effective_verify.is_some()) {
                return Err(integration_error("TASK_CONFIG_INVALID"));
            }
            Ok(None)
        }
    }
}

impl From<&crate::project_config::TaskSettings> for IntegrationPolicySettings {
    fn from(settings: &crate::project_config::TaskSettings) -> Self {
        Self {
            integrate: settings.integrate.clone(),
            verify_merge: settings.verify_merge,
        }
    }
}
impl From<&crate::task_client::BatchDefaults> for IntegrationPolicySettings {
    fn from(settings: &crate::task_client::BatchDefaults) -> Self {
        Self {
            integrate: settings.integrate.clone(),
            verify_merge: settings.verify_merge,
        }
    }
}

/// Temporary fail-closed boundary until T6 routes enabled inputs to wrappers.
/// Ordinary submission must never parse opt-in and then silently ignore it.
pub(crate) fn reject_unrouted_integration(
    settings: &crate::project_config::TaskSettings,
    request: &crate::task_client::TaskSubmitRequest,
) -> Result<(), WorkerError> {
    reject_unrouted_settings(
        &settings.into(),
        &request.integrate,
        request.verify_merge,
        request.wip,
    )
}

pub(crate) fn reject_unrouted_settings(
    settings: &IntegrationPolicySettings,
    integrate: &IntegrationOverride,
    verify: Option<VerifyPolicy>,
    wip: bool,
) -> Result<(), WorkerError> {
    if resolve_integration_settings(settings, None, integrate, verify)?.is_some() {
        if wip {
            return Err(IntegrationCode::IntegrationWipBase.error());
        }
        return Err(IntegrationCode::IntegrationUnavailable.error());
    }
    Ok(())
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
