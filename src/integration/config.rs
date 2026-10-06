//! Pure policy/configuration seams. T4 owns resolution and preflight.
use super::contracts::*;
use crate::{
    error::WorkerError,
    process::{ProcessRequest, ProcessRunner},
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

/// Resolve batch inputs without dropping their task/default/project provenance.
/// Disabled results carry no inherited verify override.
pub fn batch_integration_inputs(
    project: &crate::project_config::TaskSettings,
    defaults: &crate::task_client::BatchDefaults,
    task: &crate::task_client::BatchTask,
) -> Result<IntegrationPolicySettings, WorkerError> {
    let effective = resolve_integration_settings(
        &project.into(),
        Some(&defaults.into()),
        &task.integrate,
        task.verify_merge,
    )?;
    Ok(match effective {
        Some((branch, verify)) => IntegrationPolicySettings {
            integrate: IntegrationOverride::Target(branch),
            verify_merge: Some(verify),
        },
        None => IntegrationPolicySettings {
            integrate: IntegrationOverride::Disabled,
            verify_merge: None,
        },
    })
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct IntegrationPreview {
    pub target: String,
    pub verify: VerifyPolicy,
}

pub fn batch_integration_preview(
    project: &crate::project_config::TaskSettings,
    defaults: &crate::task_client::BatchDefaults,
    task: &crate::task_client::BatchTask,
) -> Result<Option<IntegrationPreview>, WorkerError> {
    let effective = batch_integration_inputs(project, defaults, task)?;
    Ok(match effective.integrate {
        IntegrationOverride::Target(branch) => Some(IntegrationPreview {
            target: public_target_display(
                branch.as_str(),
                &crate::redaction::RedactionBoundary::from_env(),
            ),
            verify: effective.verify_merge.unwrap_or_default(),
        }),
        _ => None,
    })
}

pub(crate) fn batch_is_integrating(
    settings: &crate::project_config::TaskSettings,
    batch: &crate::task_client::BatchFile,
) -> Result<bool, WorkerError> {
    let mut enabled = false;
    for task in &batch.tasks {
        enabled |= batch_integration_preview(settings, &batch.defaults, task)?.is_some();
    }
    Ok(enabled)
}

pub fn preflight_integration_base(
    runner: &dyn ProcessRunner,
    origin: &str,
    branch: &BranchName,
    base: Option<&BaseOid>,
    local_repo: &RootedDir,
) -> Result<IntegrationBasePreflight, WorkerError> {
    preflight_integration_base_with(
        runner,
        origin,
        branch,
        base,
        local_repo,
        crate::git_transport::origin_ref_request,
    )
}

fn preflight_integration_base_with(
    runner: &dyn ProcessRunner,
    origin: &str,
    branch: &BranchName,
    base: Option<&BaseOid>,
    local_repo: &RootedDir,
    origin_ref_request: impl FnOnce(String, &str) -> Result<ProcessRequest, WorkerError>,
) -> Result<IntegrationBasePreflight, WorkerError> {
    use IntegrationBasePreflight::{Pass, Unknown};
    // Pure validation precedes even the advertisement. Use the existing
    // preflight bounds and integration-only hardening, not a target fetch/ref.
    let key = TargetKey::new(origin, branch.as_str())
        .map_err(|_| integration_error("TASK_CONFIG_INVALID"))?;
    let reference = format!("refs/heads/{}", key.branch.as_str());
    let credentials =
        crate::git_transport::GitTransport::new(runner).origin_credential_config(&key.origin);
    let Ok(advertised) = origin_ref_request(key.origin, &reference) else {
        return Ok(Unknown);
    };
    let Ok(mut request) =
        super::git::hardened_read_request(runner, local_repo, advertised.args, &credentials)
    else {
        return Ok(Unknown);
    };
    request.policy = advertised.policy;
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
        let request = super::git::hardened_read_request(runner, local_repo, operation, &[])?;
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

/// Freeze the project's explicit policy against the already captured source.
/// From-task ancestry is bound by its parent's imported receipt on the owner.
pub(crate) fn freeze_source_policy(
    runner: &dyn ProcessRunner,
    project: &crate::project_state::ProjectState,
    integrate: &IntegrationOverride,
    verify: Option<VerifyPolicy>,
    requested_close: ClosePolicy,
    base_oid: Option<BaseOid>,
    base_task: Option<TaskId>,
) -> Result<Option<FrozenIntegrationPolicy>, WorkerError> {
    let policy = resolve_integration_policy(
        &IntegrationProjectPolicy {
            settings: (&project.settings.task).into(),
            project_id: project.context.project_id.clone(),
            base_oid,
            base_task,
        },
        None,
        integrate,
        verify,
        requested_close,
        project.origin.as_deref().unwrap_or(""),
        if base_task.is_some() {
            IntegrationBaseKind::FromTask
        } else {
            IntegrationBaseKind::Committed
        },
    )?;
    let Some(mut policy) = policy else {
        return Ok(None);
    };
    if base_task.is_none() {
        let repo = RootedDir::open_anchored_absolute(&project.context.root)?;
        policy.base_preflight = preflight_integration_base(
            runner,
            &policy.origin,
            &policy.target,
            policy.base_oid.as_ref(),
            &repo,
        )?;
    }
    Ok(Some(policy))
}

/// From-parent policy compatibility is known before any source pin or fetch.
pub(crate) fn validate_batch_policy_inputs(
    settings: &crate::project_config::TaskSettings,
    batch: &crate::task_client::BatchFile,
) -> Result<(), WorkerError> {
    let mut targets = std::collections::BTreeMap::new();
    for task in &batch.tasks {
        let inputs = batch_integration_inputs(settings, &batch.defaults, task)?;
        let resolved = resolve_integration_settings(
            &settings.into(),
            None,
            &inputs.integrate,
            inputs.verify_merge,
        )?;
        if let Some(id) = &task.id {
            targets.insert(id.as_str(), resolved.map(|(branch, _)| branch));
        }
    }
    for task in &batch.tasks {
        let Some(parent) =
            crate::dag::parse_from_base(task.base.as_deref().unwrap_or(&batch.defaults.base))
        else {
            continue;
        };
        let inputs = batch_integration_inputs(settings, &batch.defaults, task)?;
        if let Some((target, _)) = resolve_integration_settings(
            &settings.into(),
            None,
            &inputs.integrate,
            inputs.verify_merge,
        )? && targets.get(parent).and_then(Option::as_ref) != Some(&target)
        {
            return Err(integration_error("TASK_CONFIG_INVALID"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preflight_origin_ref_request_failure_is_unknown() {
        struct NoProcesses;
        impl ProcessRunner for NoProcesses {
            fn run(
                &self,
                request: &ProcessRequest,
            ) -> Result<crate::process::ProcessResult, WorkerError> {
                panic!("preflight ran {:?} without an advertisement", request.args);
            }
        }
        let root = tempfile::tempdir().unwrap();
        let repo = RootedDir::open(root.path()).unwrap();
        let base: BaseOid = "c".repeat(40).parse().unwrap();
        let preflight = preflight_integration_base_with(
            &NoProcesses,
            "git@github.com:fixture/repo.git",
            &validate_integration_target("main").unwrap(),
            Some(&base),
            &repo,
            // What a debug build returns for an invalid MAC_WORKER_TEST_SSH.
            |_, _| {
                Err(WorkerError::Protocol(
                    "CONTROLLER_UNAVAILABLE: MAC_WORKER_TEST_SSH must be an absolute executable path"
                        .into(),
                ))
            },
        )
        .unwrap();
        assert_eq!(preflight, IntegrationBasePreflight::Unknown);
    }

    #[test]
    fn batch_inputs_survive_resolution_including_disabled_override() {
        let root = tempfile::tempdir().unwrap();
        let settings = crate::project_config::ProjectSettings::load(root.path(), &[])
            .unwrap()
            .task;
        let batch: crate::task_client::BatchFile = toml::from_str("integrate = 'main'\nverify_merge = 'moved-target'\n[[tasks]]\nprompt = 'work'\n[[tasks]]\nprompt = 'disabled'\nintegrate = false").unwrap();
        for (index, task) in batch.tasks.iter().enumerate() {
            let request = crate::task_client::resolve_batch_task_without_local_workers(
                &batch.defaults,
                task,
                root.path(),
                root.path(),
                &settings,
            )
            .unwrap();
            if index == 0 {
                assert!(matches!(request.integrate, IntegrationOverride::Target(_)));
                assert_eq!(request.verify_merge, Some(VerifyPolicy::MovedTarget));
                let (target, verify) = resolve_integration_settings(
                    &(&settings).into(),
                    None,
                    &request.integrate,
                    request.verify_merge,
                )
                .unwrap()
                .unwrap();
                assert_eq!(target.as_str(), "main");
                assert_eq!(verify, VerifyPolicy::MovedTarget);
            } else {
                assert_eq!(request.integrate, IntegrationOverride::Disabled);
                assert!(
                    resolve_integration_settings(
                        &(&settings).into(),
                        None,
                        &request.integrate,
                        request.verify_merge
                    )
                    .unwrap()
                    .is_none()
                );
            }
        }
    }
}
