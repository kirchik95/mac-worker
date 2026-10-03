//! Opt-in controller adapters. T6 wires these before ordinary routing/effects.
#![allow(dead_code)]
use super::{batch::FrozenBatchBody, protocol::ControllerRequest, read::ControllerReadReply};
use crate::{
    dag::DagBase,
    error::WorkerError,
    integration::{contracts::*, coordinator::IntegrationCoordinator},
    prepared_submit::FrozenSubmitBody,
    task::{ClosePolicy, TaskId},
};
use std::collections::{BTreeMap, HashSet};

fn invalid() -> WorkerError {
    IntegrationCode::IntegrationStateInvalid.error()
}

fn add_requirements(
    requires: &mut Vec<String>,
    policy: &FrozenIntegrationPolicy,
) -> Result<(), WorkerError> {
    let origin = if crate::project::canonical_file_origin(&policy.origin)?.is_some() {
        // File origins are isolated Git fixtures, never an implicit remote.
        "file".to_owned()
    } else {
        crate::project::origin_host(&policy.origin)?
    };
    for requirement in [
        format!("origin:{origin}"),
        format!("feature:{}", crate::features::HOST_FEATURE_INTEGRATION),
    ] {
        if !requires.contains(&requirement) {
            requires.push(requirement);
        }
    }
    Ok(())
}

pub fn prepare_integrating_submit(
    mut submit: FrozenSubmitBody,
    integration: FrozenIntegrationPolicy,
) -> Result<FrozenIntegratingSubmit, WorkerError> {
    if submit.wip {
        return Err(IntegrationCode::IntegrationWipBase.error());
    }
    if submit.publish_branch.as_deref() == Some(integration.target.as_str()) {
        return Err(IntegrationCode::IntegrationPublishTargetCollision.error());
    }
    if submit.close_on != integration.requested_close && submit.close_on != ClosePolicy::Never {
        return Err(invalid());
    }
    submit.close_on = ClosePolicy::Never;
    let mut wrapper = FrozenIntegratingSubmit {
        submit,
        integration,
    };
    wrapper.validate()?;
    add_requirements(&mut wrapper.submit.requires, &wrapper.integration)?;
    validate_integrating_submit(&wrapper)?;
    Ok(wrapper)
}

/// Validate again on the owner before policy publication. Decoding the frozen
/// DTO alone does not validate ordinary prepared limits or single-submit base kind.
pub fn validate_integrating_submit(wrapper: &FrozenIntegratingSubmit) -> Result<(), WorkerError> {
    wrapper.validate()?;
    if wrapper.integration.base_kind != IntegrationBaseKind::Committed
        || wrapper.integration.base_oid.as_ref() != Some(&wrapper.submit.base_oid)
        || wrapper.submit.task_id.as_uuid().is_nil()
        || wrapper.submit.turn_id.as_uuid().is_nil()
        || wrapper
            .submit
            .session_import
            .as_ref()
            .is_some_and(|import| import.package_oid() == wrapper.submit.base_oid.as_str())
    {
        return Err(invalid());
    }
    wrapper.submit.prepared()?;
    validate_requirements(&wrapper.submit.requires, &wrapper.integration)
}

fn validate_requirements(
    requires: &[String],
    policy: &FrozenIntegrationPolicy,
) -> Result<(), WorkerError> {
    let mut expected = Vec::new();
    add_requirements(&mut expected, policy)?;
    if !expected
        .iter()
        .all(|requirement| requires.contains(requirement))
    {
        return Err(invalid());
    }
    Ok(())
}

pub fn prepare_integrating_batch(
    mut batch: FrozenBatchBody,
    integrations: BTreeMap<TaskId, Option<FrozenIntegrationPolicy>>,
) -> Result<FrozenIntegratingBatch, WorkerError> {
    // The complete map and parent compatibility are checked before changing
    // any node, publishing policy or entering the ordinary batch transaction.
    validate_batch_bindings(&batch, &integrations, false)?;
    for node in batch.nodes.values_mut() {
        if let Some(policy) = integrations[&node.task_id].as_ref() {
            node.frozen.close_on = ClosePolicy::Never;
            add_requirements(&mut node.frozen.requires, policy)?;
        }
    }
    let wrapper = FrozenIntegratingBatch {
        batch,
        integrations,
    };
    validate_integrating_batch(&wrapper)?;
    Ok(wrapper)
}

pub fn validate_integrating_batch(wrapper: &FrozenIntegratingBatch) -> Result<(), WorkerError> {
    wrapper.validate()?;
    validate_batch_bindings(&wrapper.batch, &wrapper.integrations, true)
}

fn validate_batch_bindings(
    batch: &FrozenBatchBody,
    integrations: &BTreeMap<TaskId, Option<FrozenIntegrationPolicy>>,
    effective: bool,
) -> Result<(), WorkerError> {
    super::batch::validate_wire_graph(batch)?;
    FrozenIntegratingBatch {
        batch: batch.clone(),
        integrations: integrations.clone(),
    }
    .validate()?;
    for node in batch.nodes.values() {
        let Some(policy) = integrations[&node.task_id].as_ref() else {
            continue;
        };
        policy.validate()?;
        if node.frozen.wip || matches!(node.base, DagBase::Frozen { wip: true, .. }) {
            return Err(IntegrationCode::IntegrationWipBase.error());
        }
        if node.frozen.publish_branch.as_deref() == Some(policy.target.as_str()) {
            return Err(IntegrationCode::IntegrationPublishTargetCollision.error());
        }
        if node.frozen.project_id != policy.project_id
            || node.frozen.origin_url.as_deref() != Some(policy.origin.as_str())
            || (effective && node.frozen.close_on != ClosePolicy::Never)
            || (!effective
                && node.frozen.close_on != ClosePolicy::Never
                && node.frozen.close_on != policy.requested_close)
        {
            return Err(invalid());
        }
        node.frozen.limits()?;
        node.frozen.permission_policy()?;
        if effective {
            validate_requirements(&node.frozen.requires, policy)?;
        }
        match &node.base {
            DagBase::Frozen { oid, .. } => {
                if policy.base_kind != IntegrationBaseKind::Committed
                    || policy.base_oid.as_ref() != Some(oid)
                    || policy.base_task.is_some()
                {
                    return Err(invalid());
                }
            }
            DagBase::From { parent } => {
                let parent_node = batch.nodes.get(parent).ok_or_else(invalid)?;
                if policy.base_kind != IntegrationBaseKind::FromTask
                    || policy.base_task != Some(parent_node.task_id)
                    || policy.base_oid.is_some()
                    || policy.base_preflight != IntegrationBasePreflight::Unknown
                {
                    return Err(invalid());
                }
                let parent_policy = integrations[&parent_node.task_id]
                    .as_ref()
                    .ok_or_else(|| integration_error("TASK_CONFIG_INVALID"))?;
                if policy.target_key()? != parent_policy.target_key()?
                    || policy.project_id != parent_policy.project_id
                {
                    return Err(integration_error("TASK_CONFIG_INVALID"));
                }
            }
        }
    }
    Ok(())
}

pub fn require_controller_integration(features: &[String]) -> Result<(), WorkerError> {
    if !features
        .iter()
        .any(|feature| feature == crate::features::CONTROLLER_FEATURE_INTEGRATION)
    {
        return Err(IntegrationCode::IntegrationUnavailable.error());
    }
    Ok(())
}

/// Gated decode of the entire body; the existing envelope hashes this wrapper,
/// never just its nested ordinary submit or batch.
pub fn parse_integrating_submit(
    request: &ControllerRequest,
    features: &[String],
) -> Result<FrozenIntegratingSubmit, WorkerError> {
    require_controller_integration(features)?;
    if request.command() != "task.submit-integrating" {
        return Err(invalid());
    }
    let wrapper: FrozenIntegratingSubmit =
        serde_json::from_value(request.body().clone()).map_err(|_| invalid())?;
    validate_integrating_submit(&wrapper)?;
    Ok(wrapper)
}
pub fn parse_integrating_batch(
    request: &ControllerRequest,
    features: &[String],
) -> Result<FrozenIntegratingBatch, WorkerError> {
    require_controller_integration(features)?;
    if request.command() != "task.batch-integrating" {
        return Err(invalid());
    }
    let wrapper: FrozenIntegratingBatch =
        serde_json::from_value(request.body().clone()).map_err(|_| invalid())?;
    validate_integrating_batch(&wrapper)?;
    Ok(wrapper)
}

/// Call before ordinary task creation/admission. Exact policy replay is handled
/// by IntegrationState; a partial batch failure creates no ordinary task effect.
pub fn publish_integrating_submit(
    state: &dyn IntegrationState,
    wrapper: &FrozenIntegratingSubmit,
) -> Result<(), WorkerError> {
    validate_integrating_submit(wrapper)?;
    state.publish_policy(wrapper.submit.task_id, &wrapper.integration)
}
pub fn publish_integrating_batch(
    state: &dyn IntegrationState,
    wrapper: &FrozenIntegratingBatch,
) -> Result<(), WorkerError> {
    validate_integrating_batch(wrapper)?;
    for (task, policy) in &wrapper.integrations {
        if let Some(policy) = policy {
            state.publish_policy(*task, policy)?;
        } else if state.load_policy(*task)?.is_some() {
            return Err(invalid());
        }
    }
    Ok(())
}

pub fn serve_integration_read(
    state: &dyn IntegrationState,
    task_ids: &[TaskId],
) -> Result<IntegrationReadResult, WorkerError> {
    validate_read_ids(task_ids)?;
    let mut integrations = BTreeMap::new();
    for task in task_ids {
        let snapshot = match state.load(*task)? {
            Some(record) => {
                record.validate()?;
                if record.task_id != *task {
                    return Err(invalid());
                }
                Some(record.snapshot)
            }
            None => {
                // An armed policy without a materialized cycle is not disabled.
                if state.load_policy(*task)?.is_some() {
                    return Err(IntegrationCode::IntegrationUnavailable.error());
                }
                None
            }
        };
        integrations.insert(*task, snapshot);
    }
    let result = IntegrationReadResult {
        schema_version: INTEGRATION_SCHEMA_VERSION,
        integrations,
    };
    result.validate()?;
    Ok(result)
}

fn validate_read_ids(task_ids: &[TaskId]) -> Result<(), WorkerError> {
    if task_ids.len() > MAX_READ_TASKS
        || task_ids.iter().collect::<HashSet<_>>().len() != task_ids.len()
        || task_ids.iter().any(|task| task.as_uuid().is_nil())
    {
        return Err(invalid());
    }
    Ok(())
}

pub fn is_integration_selector(request: &ControllerRequest) -> bool {
    request.command() == "task.list" && request.body().get("integration").is_some()
}

pub fn integration_selector_ids(request: &ControllerRequest) -> Result<Vec<TaskId>, WorkerError> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Selector {
        integration: Query,
    }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Query {
        task_ids: Vec<TaskId>,
    }
    if !is_integration_selector(request) {
        return Err(invalid());
    }
    let selector: Selector =
        serde_json::from_value(request.body().clone()).map_err(|_| invalid())?;
    validate_read_ids(&selector.integration.task_ids)?;
    Ok(selector.integration.task_ids)
}

/// Dispatch before ordinary task.list and before durable mutation fallback.
/// Only state reads occur, and the strict existing reply envelope is unchanged.
pub fn serve_integration_selector(
    request: &ControllerRequest,
    features: &[String],
    state: &dyn IntegrationState,
) -> Result<Vec<u8>, WorkerError> {
    let ids = integration_selector_ids(request)?;
    require_controller_integration(features)?;
    let result = serve_integration_read(state, &ids)?;
    super::protocol::encode_json_frame(&ControllerReadReply::from_request(request, result))
}

impl super::read::ControllerReadIdentity for IntegrationReadResult {
    fn verify_payload(&self, request: &ControllerRequest) -> Result<(), WorkerError> {
        self.validate()?;
        let expected: HashSet<_> = integration_selector_ids(request)?.into_iter().collect();
        if self.integrations.keys().copied().collect::<HashSet<_>>() != expected {
            return Err(invalid());
        }
        Ok(())
    }
}

/// Both source-finished checks and paired pin release must use this same
/// extractor on retries, never recapture a live session or use the package as base.
pub fn nested_integration_submit(
    request: &ControllerRequest,
) -> Result<Option<FrozenSubmitBody>, WorkerError> {
    if request.command() != "task.submit-integrating" {
        return Ok(None);
    }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Nested {
        submit: FrozenSubmitBody,
        integration: serde_json::Value,
    }
    // A rejected policy must not hide already-pinned base/session inputs from
    // the existing rollback lifecycle. Do not validate policy on this route.
    let nested: Nested = serde_json::from_value(request.body().clone()).map_err(|_| invalid())?;
    if !nested.integration.is_object() {
        return Err(invalid());
    }
    Ok(Some(nested.submit))
}

pub fn prepare_integration_redrive(
    request: &IntegrationRedriveRequest,
) -> Result<IntegrationRedriveRequest, WorkerError> {
    request.validate()?;
    if request.task_id.as_uuid().is_nil() {
        return Err(invalid());
    }
    Ok(request.clone())
}
pub fn execute_integration_redrive(
    coordinator: &IntegrationCoordinator<'_>,
    request: &IntegrationRedriveRequest,
) -> Result<IntegrationSnapshot, WorkerError> {
    let prepared = prepare_integration_redrive(request)?;
    // The coordinator owns revision CAS, terminal-task refusal and one epoch
    // increment. The durable controller request identity fences execution replay.
    coordinator.redrive(prepared.task_id, prepared.expected)
}
