//! Controller `task.batch` prepare/execute adapter.
//!
//! FLOW owns CLI, STORE, leader, source streaming, and the checkout registry.
//! This module validates the frozen graph, binds nested source receipts with
//! the outer envelope digest, resolves `max_parallel` from controller slots,
//! and resumes existing DAG/run progress without feeding a progressed graph
//! into exact-equality `create_run_with_dag`.

use std::{
    collections::{BTreeMap, HashSet},
    io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    client_state::ClientStateStore,
    config::Config,
    controller::{
        controller_transfer_cache_id,
        protocol::ControllerRequest,
        transfer::{ControllerTransfer, SourceSubmitBind},
    },
    dag::{
        DagBase, DagNode, DagNodeState, DagRecord, GraphNode, dag_pin_ref, validate_batch_graph,
    },
    error::WorkerError,
    job::RequestFingerprint,
    paths::PathLayout,
    process::ProcessRunner,
    task::{BaseOid, RunId, RunRecord, TaskId, TurnId},
    task_client::{RunReport, TaskClient, resolve_batch_max_parallel},
    transfer_repo::TransferRepo,
};

const COMMAND: &str = "task.batch";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchKind {
    Independent,
    Dag,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenBatchSource {
    pub request_id: String,
    pub project_id: String,
    pub worktree_id: String,
    pub expected_oid: BaseOid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenBatchBody {
    pub kind: BatchKind,
    pub run_id: RunId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_parallel: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub created_at_millis: u64,
    pub nodes: BTreeMap<String, DagNode>,
    pub sources: Vec<FrozenBatchSource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedBatchSource {
    request_id: String,
    project_id: String,
    worktree_id: String,
    expected_oid: BaseOid,
    fingerprint: String,
    cache_id: String,
    receipt_oid: BaseOid,
}

impl PreparedBatchSource {
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn project_id(&self) -> &str {
        &self.project_id
    }
    pub fn worktree_id(&self) -> &str {
        &self.worktree_id
    }
    pub fn expected_oid(&self) -> &BaseOid {
        &self.expected_oid
    }
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
    pub fn cache_id(&self) -> &str {
        &self.cache_id
    }
    pub fn receipt_oid(&self) -> &BaseOid {
        &self.receipt_oid
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedTaskBatch {
    kind: BatchKind,
    run_id: RunId,
    requested_max_parallel: Option<u32>,
    max_parallel: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    created_at_millis: u64,
    nodes: BTreeMap<String, DagNode>,
    sources: Vec<PreparedBatchSource>,
}

impl PreparedTaskBatch {
    pub fn command(&self) -> &'static str {
        COMMAND
    }
    pub fn task_id(&self) -> Option<TaskId> {
        None
    }
    pub fn turn_id(&self) -> Option<TurnId> {
        None
    }
    pub fn run_id(&self) -> RunId {
        self.run_id
    }
    pub fn created_at_millis(&self) -> u64 {
        self.created_at_millis
    }
    pub fn requested_max_parallel(&self) -> Option<u32> {
        self.requested_max_parallel
    }
    pub fn max_parallel(&self) -> u32 {
        self.max_parallel
    }
    pub fn kind(&self) -> BatchKind {
        self.kind
    }
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
    pub fn nodes(&self) -> &BTreeMap<String, DagNode> {
        &self.nodes
    }
    pub fn sources(&self) -> &[PreparedBatchSource] {
        &self.sources
    }
}

#[derive(Debug, Default, Clone)]
pub struct ControllerCheckoutMap {
    inner: BTreeMap<(String, String), PathBuf>,
}

impl ControllerCheckoutMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(
        &mut self,
        project_id: impl Into<String>,
        worktree_id: impl Into<String>,
        path: PathBuf,
    ) {
        self.inner
            .insert((project_id.into(), worktree_id.into()), path);
    }

    pub fn get(&self, project_id: &str, worktree_id: &str) -> Option<&Path> {
        self.inner
            .get(&(project_id.to_owned(), worktree_id.to_owned()))
            .map(PathBuf::as_path)
    }
}

pub struct BatchExecuteContext<'a> {
    pub client: &'a TaskClient<'a>,
    pub store: &'a ClientStateStore,
    pub paths: &'a PathLayout,
    pub runner: &'a dyn ProcessRunner,
    pub checkouts: &'a ControllerCheckoutMap,
}

pub fn prepare_task_batch(
    request: &ControllerRequest,
    transfer: &ControllerTransfer,
    cache_root: &Path,
    runner: &dyn ProcessRunner,
    config: &Config,
) -> Result<PreparedTaskBatch, WorkerError> {
    if request.command() != COMMAND {
        return Err(invalid("controller batch command must be task.batch"));
    }
    let body: FrozenBatchBody = serde_json::from_value(request.body().clone())
        .map_err(|_| invalid("task.batch body is invalid"))?;
    validate_wire_graph(&body)?;
    let max_parallel =
        resolve_batch_max_parallel(body.max_parallel, config.configured_runner_slots())?;
    let _ = DagRecord::new(
        body.run_id,
        body.nodes.clone(),
        max_parallel,
        body.name.clone(),
        body.created_at_millis,
    )?;
    let fingerprint = RequestFingerprint::new(request.payload_sha256().to_owned())?;
    let mut sources = Vec::with_capacity(body.sources.len());
    for source in &body.sources {
        let receipt = transfer.bind_source_for_submit(
            cache_root,
            runner,
            SourceSubmitBind {
                request_id: &source.request_id,
                fingerprint: &fingerprint,
                project_id: &source.project_id,
                worktree_id: &source.worktree_id,
                expected_oid: &source.expected_oid,
            },
        )?;
        if receipt.oid() != &source.expected_oid {
            return Err(WorkerError::Protocol(
                "CONTROLLER_REQUEST_CONFLICT: source receipt does not match the frozen base".into(),
            ));
        }
        let cache_id = controller_transfer_cache_id(&source.project_id, &source.worktree_id)?;
        sources.push(PreparedBatchSource {
            request_id: source.request_id.clone(),
            project_id: source.project_id.clone(),
            worktree_id: source.worktree_id.clone(),
            expected_oid: source.expected_oid.clone(),
            fingerprint: fingerprint.as_str().to_owned(),
            cache_id,
            receipt_oid: receipt.oid().clone(),
        });
    }
    Ok(PreparedTaskBatch {
        kind: body.kind,
        run_id: body.run_id,
        requested_max_parallel: body.max_parallel,
        max_parallel,
        name: body.name,
        created_at_millis: body.created_at_millis,
        nodes: body.nodes,
        sources,
    })
}

pub fn execute_task_batch(
    ctx: &BatchExecuteContext<'_>,
    prepared: &PreparedTaskBatch,
) -> Result<RunReport, WorkerError> {
    let mapped = mapped_nodes(prepared, ctx.checkouts)?;
    match prepared.kind {
        BatchKind::Dag => execute_dag(ctx, prepared, mapped),
        BatchKind::Independent => execute_independent(ctx, prepared, mapped),
    }
}

fn execute_dag(
    ctx: &BatchExecuteContext<'_>,
    prepared: &PreparedTaskBatch,
    mapped: BTreeMap<String, DagNode>,
) -> Result<RunReport, WorkerError> {
    pin_frozen_dag_sources(ctx, prepared, &mapped)?;
    ensure_dag_published(ctx.store, prepared, mapped)?;
    ctx.client.advance_pending_dags()?;
    let run = ctx.store.load_run(prepared.run_id)?;
    Ok(RunReport::from_parts(
        prepared.run_id,
        run.task_ids().to_vec(),
    ))
}

fn execute_independent(
    ctx: &BatchExecuteContext<'_>,
    prepared: &PreparedTaskBatch,
    mapped: BTreeMap<String, DagNode>,
) -> Result<RunReport, WorkerError> {
    let task_ids: Vec<TaskId> = mapped.values().map(|node| node.task_id).collect();
    let pristine = RunRecord::new(
        prepared.run_id,
        prepared.name.clone(),
        task_ids.clone(),
        prepared.max_parallel,
        prepared.created_at_millis,
    )?;
    match load_run_optional(ctx.store, prepared.run_id)? {
        None => ctx.store.create_run(pristine)?,
        Some(existing) => require_independent_run_identity(&existing, &pristine)?,
    }
    for (index, node) in mapped.values().enumerate() {
        ctx.client.submit_with_ids(
            crate::task_client::request_from_frozen_node(node, Some(prepared.run_id))?,
            Some(node.task_id),
            Some(node.turn_id),
            node.frozen.title.clone(),
            &mut io::sink(),
            &mut io::sink(),
            index > 0,
            Some(crate::task_client::FrozenSubmit::Dag(node)),
            Some(prepared.created_at_millis),
        )?;
    }
    Ok(RunReport::from_parts(prepared.run_id, task_ids))
}

fn ensure_dag_published(
    store: &ClientStateStore,
    prepared: &PreparedTaskBatch,
    mapped: BTreeMap<String, DagNode>,
) -> Result<(), WorkerError> {
    let pristine_dag = DagRecord::new(
        prepared.run_id,
        mapped,
        prepared.max_parallel,
        prepared.name.clone(),
        prepared.created_at_millis,
    )?;
    let pristine_run = RunRecord::new(
        prepared.run_id,
        prepared.name.clone(),
        Vec::new(),
        prepared.max_parallel,
        prepared.created_at_millis,
    )?;
    match store.load_run_dag(prepared.run_id)? {
        None => store.create_run_with_dag(pristine_run, pristine_dag),
        Some(existing) => {
            require_immutable_dag_identity(&existing, &pristine_dag)?;
            match load_run_optional(store, prepared.run_id)? {
                None => store.create_run(pristine_run),
                Some(run) => require_dag_run_identity(&run, &pristine_run, &existing),
            }
        }
    }
}

fn pin_frozen_dag_sources(
    ctx: &BatchExecuteContext<'_>,
    prepared: &PreparedTaskBatch,
    mapped: &BTreeMap<String, DagNode>,
) -> Result<(), WorkerError> {
    for node in mapped.values() {
        let DagBase::Frozen { oid, pin_ref, .. } = &node.base else {
            continue;
        };
        let expected = dag_pin_ref(prepared.run_id, &node.batch_id);
        if pin_ref != &expected {
            return Err(WorkerError::task(
                "TASK_CONFIG_INVALID",
                "frozen DAG pin_ref must match refs/mac-worker/dag/{run}/{batch}",
            ));
        }
        let transfer = TransferRepo::open_or_create_controller_cache(
            &ctx.paths.cache,
            &node.frozen.project_id,
            &node.frozen.worktree_id,
        )?;
        transfer.pin_object(ctx.runner, pin_ref, oid)?;
    }
    Ok(())
}

fn mapped_nodes(
    prepared: &PreparedTaskBatch,
    checkouts: &ControllerCheckoutMap,
) -> Result<BTreeMap<String, DagNode>, WorkerError> {
    let mut nodes = prepared.nodes.clone();
    for node in nodes.values_mut() {
        let path = checkouts
            .get(&node.frozen.project_id, &node.frozen.worktree_id)
            .ok_or_else(|| {
                WorkerError::task(
                    "TASK_CONFIG_INVALID",
                    "controller checkout mapping is missing",
                )
            })?;
        node.frozen.project_path = path.to_string_lossy().into_owned();
    }
    Ok(nodes)
}

fn validate_wire_graph(body: &FrozenBatchBody) -> Result<(), WorkerError> {
    if body.nodes.is_empty() {
        return Err(WorkerError::task(
            "TASK_CONFIG_INVALID",
            "batch has no tasks",
        ));
    }
    let computed = graph_kind(&body.nodes);
    if body.kind != computed {
        return Err(WorkerError::task(
            "TASK_CONFIG_INVALID",
            "batch kind does not match frozen graph edges",
        ));
    }
    let mut frozen_keys = HashSet::new();
    for node in body.nodes.values() {
        if node.state != DagNodeState::Waiting
            || node.bound_oid.is_some()
            || node.bound_turn_id.is_some()
            || node.pin_ref.is_some()
            || node.blocked_by.is_some()
            || node.claimed_by.is_some()
            || node.claimed_at_millis.is_some()
        {
            return Err(WorkerError::task(
                "TASK_CONFIG_INVALID",
                "frozen batch nodes must be waiting without claim or bind",
            ));
        }
        match &node.base {
            DagBase::Frozen { oid, pin_ref, .. } => {
                if pin_ref != &dag_pin_ref(body.run_id, &node.batch_id) {
                    return Err(WorkerError::task(
                        "TASK_CONFIG_INVALID",
                        "frozen DAG pin_ref must match refs/mac-worker/dag/{run}/{batch}",
                    ));
                }
                let key = (
                    node.frozen.project_id.clone(),
                    node.frozen.worktree_id.clone(),
                    oid.clone(),
                );
                frozen_keys.insert(key);
            }
            DagBase::From { .. } => {}
        }
    }
    let mut source_keys = HashSet::new();
    for source in &body.sources {
        if !source_keys.insert((
            source.project_id.clone(),
            source.worktree_id.clone(),
            source.expected_oid.clone(),
        )) {
            return Err(invalid("task.batch sources must be unique frozen roots"));
        }
    }
    if source_keys != frozen_keys {
        return Err(invalid(
            "task.batch sources must match frozen root project/worktree/OID tuples",
        ));
    }
    validate_depends_graph(&body.nodes)?;
    Ok(())
}

fn validate_depends_graph(nodes: &BTreeMap<String, DagNode>) -> Result<(), WorkerError> {
    let bases: Vec<String> = nodes
        .values()
        .map(|node| match &node.base {
            DagBase::From { parent } => format!("from:{parent}"),
            DagBase::Frozen { .. } => String::new(),
        })
        .collect();
    let graph: Vec<GraphNode<'_>> = nodes
        .values()
        .zip(bases.iter())
        .map(|(node, base)| GraphNode {
            id: Some(node.batch_id.as_str()),
            depends_on: &node.depends_on,
            base,
        })
        .collect();
    if let Some(issue) = validate_batch_graph(&graph).into_iter().next() {
        return Err(WorkerError::task("TASK_CONFIG_INVALID", issue.message));
    }
    Ok(())
}

fn graph_kind(nodes: &BTreeMap<String, DagNode>) -> BatchKind {
    if nodes
        .values()
        .any(|node| !node.depends_on.is_empty() || matches!(node.base, DagBase::From { .. }))
    {
        BatchKind::Dag
    } else {
        BatchKind::Independent
    }
}

fn require_immutable_dag_identity(
    existing: &DagRecord,
    pristine: &DagRecord,
) -> Result<(), WorkerError> {
    if existing.version != pristine.version
        || existing.run_id != pristine.run_id
        || existing.max_parallel != pristine.max_parallel
        || existing.name != pristine.name
        || existing.created_at_millis != pristine.created_at_millis
        || existing.nodes.len() != pristine.nodes.len()
    {
        return Err(run_conflict());
    }
    for (batch_id, expected) in &pristine.nodes {
        let Some(actual) = existing.nodes.get(batch_id) else {
            return Err(run_conflict());
        };
        if actual.batch_id != expected.batch_id
            || actual.task_id != expected.task_id
            || actual.turn_id != expected.turn_id
            || actual.depends_on != expected.depends_on
            || actual.base != expected.base
            || actual.frozen != expected.frozen
        {
            return Err(run_conflict());
        }
    }
    Ok(())
}

fn require_dag_run_identity(
    existing: &RunRecord,
    pristine: &RunRecord,
    dag: &DagRecord,
) -> Result<(), WorkerError> {
    if existing.run_id() != pristine.run_id()
        || existing.name() != pristine.name()
        || existing.max_parallel() != pristine.max_parallel()
        || existing.created_at_millis() != pristine.created_at_millis()
    {
        return Err(run_conflict());
    }
    let allowed: HashSet<TaskId> = dag.nodes.values().map(|node| node.task_id).collect();
    if existing
        .task_ids()
        .iter()
        .any(|task_id| !allowed.contains(task_id))
    {
        return Err(run_conflict());
    }
    Ok(())
}

fn require_independent_run_identity(
    existing: &RunRecord,
    pristine: &RunRecord,
) -> Result<(), WorkerError> {
    if existing.run_id() != pristine.run_id()
        || existing.name() != pristine.name()
        || existing.max_parallel() != pristine.max_parallel()
        || existing.created_at_millis() != pristine.created_at_millis()
        || existing.task_ids() != pristine.task_ids()
    {
        return Err(run_conflict());
    }
    Ok(())
}

fn load_run_optional(
    store: &ClientStateStore,
    run_id: RunId,
) -> Result<Option<RunRecord>, WorkerError> {
    match store.load_run(run_id) {
        Ok(run) => Ok(Some(run)),
        Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn run_conflict() -> WorkerError {
    WorkerError::task(
        "RUN_ID_CONFLICT",
        "run ID is already present with different metadata",
    )
}

fn invalid(message: &str) -> WorkerError {
    WorkerError::Protocol(format!("INVALID_REQUEST: {message}"))
}
