//! Laptop freeze of an existing batch file into an immutable `task.batch` body.
//!
//! FLOW persists `LaptopFrozenBatch::body` as the envelope **before** any
//! network effect and streams from the pinned laptop transfer paths. Replay
//! must reuse that saved body; this module must not be called again to mint a
//! new graph. No ClientStateStore, SSH, RPC, run, DAG file, or queue.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    io,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use uuid::Uuid;

use crate::{
    config::Config,
    controller::batch::{BatchKind, FrozenBatchBody, FrozenBatchSource},
    dag::{
        DagBase, DagNode, DagNodeState, DagRecord, GraphNode, dag_pin_ref, parse_from_base,
        validate_batch_graph,
    },
    error::WorkerError,
    paths::PathLayout,
    process::ProcessRunner,
    project_state::ProjectState,
    task::{GitIdentity, RunId, TaskId, TurnId},
    task_client::{
        BatchTask, batch_has_dag_edges, freeze_spec, load_batch_file,
        resolve_batch_task_without_local_workers,
    },
    transfer_repo::TransferRepo,
};

const GIT_NAME: &str = "mac-worker";
const GIT_EMAIL: &str = "mac-worker@localhost";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaptopBatchSourceStream {
    request_id: String,
    git_path: PathBuf,
    project_id: String,
    worktree_id: String,
    expected_oid: crate::task::BaseOid,
    pin_ref: String,
}

impl LaptopBatchSourceStream {
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn git_path(&self) -> &Path {
        &self.git_path
    }
    pub fn project_id(&self) -> &str {
        &self.project_id
    }
    pub fn worktree_id(&self) -> &str {
        &self.worktree_id
    }
    pub fn expected_oid(&self) -> &crate::task::BaseOid {
        &self.expected_oid
    }
    pub fn pin_ref(&self) -> &str {
        &self.pin_ref
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaptopFrozenBatch {
    body: FrozenBatchBody,
    sources: Vec<LaptopBatchSourceStream>,
}

impl LaptopFrozenBatch {
    pub fn body(&self) -> &FrozenBatchBody {
        &self.body
    }

    pub fn sources(&self) -> &[LaptopBatchSourceStream] {
        &self.sources
    }
}

/// Freeze one current checkout. `config` may have `workers=[]` and is not
/// used as slot or worker-name authority (controller prepare remains that).
pub fn freeze_laptop_batch(
    runner: &dyn ProcessRunner,
    config: &Config,
    paths: &PathLayout,
    project: &Path,
    batch_file: &Path,
    run_name: Option<String>,
    max_parallel: Option<u32>,
) -> Result<LaptopFrozenBatch, WorkerError> {
    let _ = config;
    if let Some(0) = max_parallel {
        return Err(WorkerError::task(
            "TASK_CONFIG_INVALID",
            "batch max_parallel must be positive",
        ));
    }
    let batch = load_batch_file(batch_file)?;
    let graph: Vec<GraphNode<'_>> = batch
        .tasks
        .iter()
        .map(|task| GraphNode {
            id: task.id.as_deref(),
            depends_on: &task.depends_on,
            base: task.base.as_deref().unwrap_or(&batch.defaults.base),
        })
        .collect();
    if let Some(issue) = validate_batch_graph(&graph).into_iter().next() {
        return Err(WorkerError::task("TASK_CONFIG_INVALID", issue.message));
    }
    let kind = if batch_has_dag_edges(&batch.defaults, &batch.tasks) {
        BatchKind::Dag
    } else {
        BatchKind::Independent
    };
    let batch_dir = batch_file.parent().unwrap_or_else(|| Path::new("."));
    let project_state = ProjectState::load(runner, project, &[])?;
    let identity = GitIdentity::new(GIT_NAME, GIT_EMAIL)?;
    let transfer = TransferRepo::open_or_create(&paths.cache, &project_state.context.common_dir)?;
    let run_id = RunId::generate();
    let created = freeze_time_millis()?;
    let mut nodes = BTreeMap::new();
    let mut dag_pins = Vec::new();
    let mut source_pins = Vec::new();
    let mut source_rows: BTreeMap<(String, String, String), LaptopBatchSourceStream> =
        BTreeMap::new();
    let freeze_result = (|| -> Result<(), WorkerError> {
        for task in &batch.tasks {
            let request = resolve_batch_task_without_local_workers(
                &batch.defaults,
                task,
                batch_dir,
                project,
                &project_state.settings.task,
            )?;
            let task_id = TaskId::generate();
            let turn_id = TurnId::generate();
            let batch_id = assigned_batch_id(task, task_id);
            let mut depends_on = task.depends_on.clone();
            if let Some(parent) = parse_from_base(&request.base)
                && !depends_on.iter().any(|dep| dep == parent)
            {
                depends_on.push(parent.to_owned());
            }
            let mut frozen = freeze_spec(&request, &project_state)?;
            frozen.title = task.title.clone();
            let base = if let Some(parent) = parse_from_base(&request.base) {
                DagBase::From {
                    parent: parent.to_owned(),
                }
            } else {
                let pin_ref = dag_pin_ref(run_id, &batch_id);
                let (oid, wip) = if request.wip {
                    let commit = transfer.build_wip_base(
                        runner,
                        &project_state.context,
                        task_id,
                        &project_state.settings,
                        &identity,
                    )?;
                    (commit.oid().clone(), true)
                } else {
                    (
                        TransferRepo::resolve_base_oid(
                            runner,
                            &project_state.context,
                            &request.base,
                        )?,
                        false,
                    )
                };
                transfer.pin_object(runner, &pin_ref, &oid)?;
                dag_pins.push(pin_ref.clone());
                let key = (
                    frozen.project_id.clone(),
                    frozen.worktree_id.clone(),
                    oid.to_string(),
                );
                match source_rows.entry(key) {
                    Entry::Occupied(_) => {}
                    Entry::Vacant(slot) => {
                        let request_id = nested_request_id();
                        let source_ref = transfer.pin_frozen_source(runner, &request_id, &oid)?;
                        source_pins.push(source_ref.clone());
                        slot.insert(LaptopBatchSourceStream {
                            request_id,
                            git_path: transfer.path().to_path_buf(),
                            project_id: frozen.project_id.clone(),
                            worktree_id: frozen.worktree_id.clone(),
                            expected_oid: oid.clone(),
                            pin_ref: source_ref,
                        });
                    }
                }
                DagBase::Frozen { oid, pin_ref, wip }
            };
            if nodes
                .insert(
                    batch_id.clone(),
                    DagNode {
                        batch_id,
                        task_id,
                        turn_id,
                        depends_on,
                        base,
                        frozen,
                        state: DagNodeState::Waiting,
                        bound_oid: None,
                        bound_turn_id: None,
                        pin_ref: None,
                        blocked_by: None,
                        claimed_by: None,
                        claimed_at_millis: None,
                    },
                )
                .is_some()
            {
                return Err(WorkerError::task(
                    "TASK_CONFIG_INVALID",
                    "duplicate batch task id",
                ));
            }
        }
        Ok(())
    })();
    if let Err(error) = freeze_result {
        unpin_all(&transfer, runner, &dag_pins, &source_pins);
        return Err(error);
    }
    let sources: Vec<LaptopBatchSourceStream> = source_rows.into_values().collect();
    let body = FrozenBatchBody {
        kind,
        run_id,
        max_parallel,
        name: run_name,
        created_at_millis: created,
        nodes,
        sources: sources
            .iter()
            .map(|source| FrozenBatchSource {
                request_id: source.request_id.clone(),
                project_id: source.project_id.clone(),
                worktree_id: source.worktree_id.clone(),
                expected_oid: source.expected_oid.clone(),
            })
            .collect(),
    };
    let structural_parallel = max_parallel.unwrap_or(1);
    if let Err(error) = DagRecord::new(
        body.run_id,
        body.nodes.clone(),
        structural_parallel,
        body.name.clone(),
        body.created_at_millis,
    ) {
        unpin_all(&transfer, runner, &dag_pins, &source_pins);
        return Err(error);
    }
    Ok(LaptopFrozenBatch { body, sources })
}

fn assigned_batch_id(task: &BatchTask, task_id: TaskId) -> String {
    task.id.clone().unwrap_or_else(|| task_id.to_string())
}

fn nested_request_id() -> String {
    format!("{:x}", Uuid::new_v4().simple())
}

fn freeze_time_millis() -> Result<u64, WorkerError> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| WorkerError::Io(io::Error::other("system clock predates Unix epoch")))?
        .as_millis();
    u64::try_from(value)
        .map_err(|_| WorkerError::Io(io::Error::other("system clock is out of range")))
}

fn unpin_all(
    transfer: &TransferRepo,
    runner: &dyn ProcessRunner,
    dag_pins: &[String],
    source_pins: &[String],
) {
    for pin in dag_pins.iter().chain(source_pins) {
        let _ = transfer.unpin_object(runner, pin);
    }
}
