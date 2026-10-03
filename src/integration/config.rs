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
    runner: &dyn ProcessRunner,
    origin: &str,
    branch: &BranchName,
    base: Option<&BaseOid>,
    local_repo: &RootedDir,
) -> Result<IntegrationBasePreflight, WorkerError> {
    use IntegrationBasePreflight::{Pass, Unknown};
    // Pure validation precedes even the advertisement. Use the existing
    // preflight environment and bounds, not a target fetch/transfer ref.
    let key = TargetKey::new(origin, branch.as_str())
        .map_err(|_| integration_error("TASK_CONFIG_INVALID"))?;
    let reference = format!("refs/heads/{}", key.branch.as_str());
    let request = crate::git_transport::origin_ref_request(key.origin, &reference)?;
    let Ok(result) = runner.run(&request) else {
        return Ok(Unknown);
    };
    if !result.status.success() {
        return Ok(Unknown);
    }
    if result.stdout.is_empty() {
        return Err(IntegrationCode::IntegrationTargetMissing.error());
    }
    let Ok(text) = std::str::from_utf8(&result.stdout) else {
        return Ok(Unknown);
    };
    let mut lines = text.lines();
    let Some((oid, advertised_ref)) = lines.next().and_then(|line| line.split_once('\t')) else {
        return Ok(Unknown);
    };
    if advertised_ref != reference || lines.next().is_some() {
        return Ok(Unknown);
    }
    let Ok(target) = oid.parse::<BaseOid>() else {
        return Ok(Unknown);
    };
    let Some(base) = base else {
        return Ok(Unknown);
    };

    let run_local = |operation: Vec<std::ffi::OsString>| {
        let mut request = crate::git_transport::git_request_with_config(
            local_repo.path(),
            None,
            &[
                ("core.hooksPath".into(), "/dev/null".into()),
                ("core.fsmonitor".into(), "false".into()),
            ],
            operation,
        );
        request.policy.deadline = std::time::Duration::from_secs(30);
        request.environment.extend([
            ("GIT_NO_LAZY_FETCH".into(), "1".into()),
            ("GIT_NO_REPLACE_OBJECTS".into(), "1".into()),
            ("GIT_GRAFT_FILE".into(), "/dev/null".into()),
        ]);
        runner.run(&request)
    };
    // A negative answer in shallow/incomplete history is not a proof. Traverse
    // both commit histories before testing ancestry, without fetching missing
    // promisor objects or accepting replace/graft identities.
    let Ok(shallow) = run_local(vec!["rev-parse".into(), "--is-shallow-repository".into()]) else {
        return Ok(Unknown);
    };
    if !shallow.status.success() || shallow.stdout != b"false\n" {
        return Ok(Unknown);
    }
    let Ok(history) = run_local(vec![
        "rev-list".into(),
        "--count".into(),
        target.as_str().into(),
        base.as_str().into(),
    ]) else {
        return Ok(Unknown);
    };
    if !history.status.success()
        || std::str::from_utf8(&history.stdout)
            .ok()
            .and_then(|count| count.trim().parse::<u64>().ok())
            .is_none()
    {
        return Ok(Unknown);
    }
    let Ok(ancestry) = run_local(vec![
        "merge-base".into(),
        "--is-ancestor".into(),
        base.as_str().into(),
        target.as_str().into(),
    ]) else {
        return Ok(Unknown);
    };
    match ancestry.status.code() {
        Some(0) => Ok(Pass),
        Some(1) => Err(IntegrationCode::IntegrationBaseNotOnTarget.error()),
        _ => Ok(Unknown),
    }
}
