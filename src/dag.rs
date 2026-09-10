use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    agent::{PermissionPolicy, TurnLimits},
    error::WorkerError,
    job::ProcessIdentity,
    supervisor::ProcessObservation,
    task::{
        BaseOid, BranchName, ClosePolicy, LocalTaskRecord, RunId, RunProgress, TaskId, TaskLimits,
        TaskOutcome, TaskState, TurnId,
    },
    task_view::{ReviewState, TaskFreshness, TaskListProjection, TaskListRow},
};

pub const DAG_PARENT_FAILED: &str = "DAG_PARENT_FAILED";
pub const DAG_WAITING: &str = "DAG_WAITING";
pub const DAG_CLAIMED: &str = "DAG_CLAIMED";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DagNodeState {
    Waiting,
    Claimed,
    Submitted,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParentGate {
    Ready,
    Waiting,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimedNodeAction {
    MarkSubmitted,
    Continue,
    Retake,
    SkipLive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DagBase {
    Frozen {
        oid: BaseOid,
        pin_ref: String,
        wip: bool,
    },
    From {
        parent: String,
    },
}

/// Resolved execution input frozen at initial batch submit.
/// Delayed submit must not reread `.worker.toml` or HEAD.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DagFrozenSpec {
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub agent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_url: Option<String>,
    pub publish: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_branch: Option<String>,
    pub close_on: ClosePolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,
    pub wip: bool,
    pub project_path: String,
    pub project_id: String,
    pub worktree_id: String,
    pub timeout_millis: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_turns: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_budget_usd_cents: Option<u64>,
    pub max_followups: u32,
    /// Resolved `PermissionPolicy` (`workspace` or `unattended`).
    pub permissions: String,
    pub requires: Vec<String>,
    pub include_untracked: Vec<String>,
    pub include_empty_dirs: Vec<String>,
    pub allow_sensitive: Vec<String>,
    pub cli_includes: Vec<String>,
    /// Branch frozen at initial batch submit for first-turn prompt composition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DagNode {
    pub batch_id: String,
    pub task_id: TaskId,
    pub turn_id: TurnId,
    pub depends_on: Vec<String>,
    pub base: DagBase,
    pub frozen: DagFrozenSpec,
    pub state: DagNodeState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bound_oid: Option<BaseOid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bound_turn_id: Option<TurnId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_by: Option<ProcessIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_at_millis: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DagRecord {
    pub version: u32,
    pub run_id: RunId,
    pub max_parallel: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub created_at_millis: u64,
    pub nodes: BTreeMap<String, DagNode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DagNodeProjection {
    pub batch_id: String,
    pub task_id: TaskId,
    pub turn_id: TurnId,
    pub state: DagNodeState,
    pub depends_on: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound_oid: Option<BaseOid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DagClaim {
    pub batch_id: String,
    pub node: DagNode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphIssue {
    pub kind: &'static str,
    pub message: String,
}

impl DagFrozenSpec {
    pub fn permission_policy(&self) -> Result<PermissionPolicy, WorkerError> {
        match self.permissions.as_str() {
            "workspace" => Ok(PermissionPolicy::Workspace),
            "unattended" => Ok(PermissionPolicy::Unattended),
            _ => Err(WorkerError::task(
                "TASK_CONFIG_INVALID",
                "frozen DAG permissions must be workspace or unattended",
            )),
        }
    }

    pub fn limits(&self) -> Result<TaskLimits, WorkerError> {
        TaskLimits::new(
            TurnLimits::new(
                self.timeout_millis,
                self.max_turns,
                self.max_budget_usd_cents,
            )
            .map_err(|error| WorkerError::task("TASK_CONFIG_INVALID", error.to_string()))?,
            self.max_followups,
        )
    }
}

impl DagRecord {
    pub fn new(
        run_id: RunId,
        nodes: BTreeMap<String, DagNode>,
        max_parallel: u32,
        name: Option<String>,
        created_at_millis: u64,
    ) -> Result<Self, WorkerError> {
        let record = Self {
            version: 1,
            run_id,
            max_parallel,
            name,
            created_at_millis,
            nodes,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.version != 1 {
            return Err(WorkerError::task(
                "TASK_CONFIG_INVALID",
                "DAG record version must be 1",
            ));
        }
        if self.max_parallel == 0 {
            return Err(WorkerError::task(
                "TASK_CONFIG_INVALID",
                "DAG max_parallel must be greater than zero",
            ));
        }
        let mut task_ids = Vec::new();
        let mut turn_ids = Vec::new();
        for (id, node) in &self.nodes {
            if id != &node.batch_id {
                return Err(WorkerError::task(
                    "TASK_CONFIG_INVALID",
                    "DAG node key must match batch_id",
                ));
            }
            if let Err(message) = validate_graph_id(&node.batch_id) {
                return Err(WorkerError::task("TASK_CONFIG_INVALID", message));
            }
            if task_ids.contains(&node.task_id) {
                return Err(WorkerError::task(
                    "TASK_CONFIG_INVALID",
                    "DAG task IDs must be unique",
                ));
            }
            task_ids.push(node.task_id);
            if turn_ids.contains(&node.turn_id) {
                return Err(WorkerError::task(
                    "TASK_CONFIG_INVALID",
                    "DAG turn IDs must be unique",
                ));
            }
            turn_ids.push(node.turn_id);
            node.frozen.permission_policy()?;
            node.frozen.limits()?;
            let canonical = dag_pin_ref(self.run_id, &node.batch_id);
            match &node.base {
                DagBase::Frozen { pin_ref, .. } => {
                    if pin_ref != &canonical {
                        return Err(WorkerError::task(
                            "TASK_CONFIG_INVALID",
                            "frozen DAG pin_ref must be the canonical TransferRepo pin",
                        ));
                    }
                }
                DagBase::From { parent } => {
                    if !node.depends_on.iter().any(|dep| dep == parent) {
                        return Err(WorkerError::task(
                            "TASK_CONFIG_INVALID",
                            format!("from:{parent} must be a depends_on edge"),
                        ));
                    }
                    if !self.nodes.contains_key(parent) {
                        return Err(WorkerError::task(
                            "TASK_CONFIG_INVALID",
                            format!("from:{parent} parent is missing"),
                        ));
                    }
                    match (&node.bound_oid, &node.bound_turn_id, &node.pin_ref) {
                        (None, None, None) => {}
                        (Some(_), Some(_), Some(pin)) if pin == &canonical => {}
                        _ => {
                            return Err(WorkerError::task(
                                "TASK_CONFIG_INVALID",
                                "from: binding must store oid, turn, and canonical pin together",
                            ));
                        }
                    }
                }
            }
            if let Some(pin) = &node.pin_ref
                && pin != &canonical
            {
                return Err(WorkerError::task(
                    "TASK_CONFIG_INVALID",
                    "DAG node pin_ref must be the canonical TransferRepo pin",
                ));
            }
            match node.state {
                DagNodeState::Blocked => {
                    if node.blocked_by.is_none() {
                        return Err(WorkerError::task(
                            "TASK_CONFIG_INVALID",
                            "blocked DAG node must name blocked_by",
                        ));
                    }
                    if node.claimed_by.is_some() || node.claimed_at_millis.is_some() {
                        return Err(WorkerError::task(
                            "TASK_CONFIG_INVALID",
                            "blocked DAG node must not retain a claim owner",
                        ));
                    }
                }
                DagNodeState::Claimed => {
                    if node.claimed_by.is_none() || node.claimed_at_millis.is_none() {
                        return Err(WorkerError::task(
                            "TASK_CONFIG_INVALID",
                            "claimed DAG node must record claimed_by and claimed_at_millis",
                        ));
                    }
                }
                DagNodeState::Waiting | DagNodeState::Submitted => {
                    if node.claimed_by.is_some() || node.claimed_at_millis.is_some() {
                        return Err(WorkerError::task(
                            "TASK_CONFIG_INVALID",
                            "waiting or submitted DAG node must not retain a claim owner",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn node(&self, batch_id: &str) -> Option<&DagNode> {
        self.nodes.get(batch_id)
    }

    pub fn pending(&self) -> bool {
        self.nodes
            .values()
            .any(|node| matches!(node.state, DagNodeState::Waiting | DagNodeState::Claimed))
    }

    /// Pins that retain objects. Always computed; never trust serialized strings for release.
    pub fn retention_pin_refs(&self) -> Vec<String> {
        self.nodes
            .values()
            .filter(|node| match &node.base {
                DagBase::Frozen { .. } => true,
                DagBase::From { .. } => node.bound_oid.is_some(),
            })
            .map(|node| dag_pin_ref(self.run_id, &node.batch_id))
            .collect()
    }
}

impl DagNode {
    pub fn execution_oid(&self) -> Option<&BaseOid> {
        match &self.base {
            DagBase::Frozen { oid, .. } => Some(oid),
            DagBase::From { .. } => self.bound_oid.as_ref(),
        }
    }

    pub fn from_parent(&self) -> Option<&str> {
        match &self.base {
            DagBase::From { parent } => Some(parent.as_str()),
            DagBase::Frozen { .. } => None,
        }
    }

    pub fn take_claim(&mut self, caller: ProcessIdentity, now_millis: u64) {
        self.state = DagNodeState::Claimed;
        self.claimed_by = Some(caller);
        self.claimed_at_millis = Some(now_millis);
        self.blocked_by = None;
    }

    pub fn mark_submitted(&mut self) {
        self.state = DagNodeState::Submitted;
        self.claimed_by = None;
        self.claimed_at_millis = None;
        self.blocked_by = None;
    }

    pub fn mark_blocked(&mut self, code: impl Into<String>) {
        self.state = DagNodeState::Blocked;
        self.blocked_by = Some(code.into());
        self.claimed_by = None;
        self.claimed_at_millis = None;
    }
}

pub fn dag_pin_ref(run_id: RunId, batch_id: &str) -> String {
    format!("refs/mac-worker/dag/{run_id}/{batch_id}")
}

pub fn parse_from_base(base: &str) -> Option<&str> {
    base.strip_prefix("from:")
        .filter(|parent| !parent.is_empty())
}

pub fn claim_owner_is_live(observation: ProcessObservation) -> bool {
    matches!(
        observation,
        ProcessObservation::Matching { .. } | ProcessObservation::Ambiguous
    )
}

/// Recoverable claim authority: live foreign owners skip; dead/absent owners
/// are retaken even when an incomplete task already exists; a still-live
/// caller continues the same IDs. Continue does not persist caller ownership, so
/// a dead claim must Retake. An existing task is Submitted only after the durable
/// submission contract has cleared intent *and* a turn directory exists.
pub fn claimed_node_action(
    claimed_by: Option<ProcessIdentity>,
    caller: ProcessIdentity,
    owner_live: bool,
    task_exists: bool,
    submission_complete: bool,
) -> ClaimedNodeAction {
    if task_exists && submission_complete {
        return ClaimedNodeAction::MarkSubmitted;
    }
    match claimed_by {
        Some(owner) if owner == caller => ClaimedNodeAction::Continue,
        Some(_) if owner_live => ClaimedNodeAction::SkipLive,
        Some(_) | None => ClaimedNodeAction::Retake,
    }
}

pub fn dag_submission_complete(record: &LocalTaskRecord) -> bool {
    record.submission_intent_turn_id().is_none()
        && record.submission_rollback_turn_id().is_none()
        && record.abandon_code() != Some("SUBMISSION_ROLLBACK_INCOMPLETE")
}

/// Closed+Done is the only successful parent gate, including close_on=never.
/// Open+NeedsInput and Open+Done wait. Open unsuccessful terminals block.
pub fn parent_gate(record: &LocalTaskRecord) -> ParentGate {
    match record.status().state() {
        TaskState::Abandoned | TaskState::Lost => ParentGate::Failed,
        TaskState::Closed => match record.status().last_outcome() {
            Some(TaskOutcome::Done) => ParentGate::Ready,
            _ => ParentGate::Failed,
        },
        TaskState::Queued | TaskState::Active => ParentGate::Waiting,
        TaskState::Open => match record.status().last_outcome() {
            Some(TaskOutcome::NeedsInput) | Some(TaskOutcome::Done) | None => ParentGate::Waiting,
            Some(
                TaskOutcome::Failed { .. }
                | TaskOutcome::Blocked
                | TaskOutcome::Cancelled
                | TaskOutcome::TimedOut
                | TaskOutcome::Lost
                | TaskOutcome::Unknown,
            ) => ParentGate::Failed,
        },
    }
}

pub fn parents_ready(
    _record: &DagRecord,
    node: &DagNode,
    parents: &BTreeMap<String, ParentGate>,
) -> bool {
    node.depends_on
        .iter()
        .all(|dep| parents.get(dep) == Some(&ParentGate::Ready))
        && match &node.base {
            DagBase::Frozen { .. } => true,
            DagBase::From { .. } => node.bound_oid.is_some(),
        }
}

pub fn parents_failed(node: &DagNode, parents: &BTreeMap<String, ParentGate>) -> bool {
    node.depends_on
        .iter()
        .any(|dep| parents.get(dep) == Some(&ParentGate::Failed))
}

/// Bind `from:` only to this accepted turn's imported object.
pub fn accepted_import_oid(
    record: &LocalTaskRecord,
    last_turn_id: TurnId,
    queue_busy: bool,
    journal_complete: bool,
    object_exists: bool,
) -> Option<BaseOid> {
    if parent_gate(record) != ParentGate::Ready || queue_busy || !journal_complete || !object_exists
    {
        return None;
    }
    let last = record.status().turns().last()?;
    if last.turn_id() != last_turn_id {
        return None;
    }
    if last.outcome() != Some(&TaskOutcome::Done) {
        return None;
    }
    let fetched = record.fetched_head()?;
    let head = record.status().head_oid()?;
    if fetched != head {
        return None;
    }
    Some(fetched.clone())
}

pub struct GraphNode<'a> {
    pub id: Option<&'a str>,
    pub depends_on: &'a [String],
    pub base: &'a str,
}

pub fn validate_batch_graph(tasks: &[GraphNode<'_>]) -> Vec<GraphIssue> {
    let mut issues = Vec::new();
    let mut ids = BTreeMap::new();
    for (index, task) in tasks.iter().enumerate() {
        if let Some(id) = task.id {
            if let Err(message) = validate_graph_id(id) {
                issues.push(GraphIssue {
                    kind: "invalid_id",
                    message,
                });
                continue;
            }
            if ids.insert(id, index).is_some() {
                issues.push(GraphIssue {
                    kind: "duplicate_id",
                    message: format!("duplicate batch task id {id}"),
                });
            }
        }
    }
    let mut adjacency: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for task in tasks {
        let Some(id) = task.id else {
            if !task.depends_on.is_empty() || parse_from_base(task.base).is_some() {
                issues.push(GraphIssue {
                    kind: "invalid_dependency",
                    message: "depends_on and from: require a task id".into(),
                });
            }
            continue;
        };
        let mut deps = task.depends_on.to_vec();
        if let Some(parent) = parse_from_base(task.base)
            && !deps.iter().any(|dep| dep == parent)
        {
            deps.push(parent.to_owned());
        }
        if let Some(parent) = parse_from_base(task.base)
            && !ids.contains_key(parent)
        {
            issues.push(GraphIssue {
                kind: "unknown_dependency",
                message: format!("task {id} uses from:{parent} but that id is missing"),
            });
        }
        for dep in &deps {
            if !ids.contains_key(dep.as_str()) {
                issues.push(GraphIssue {
                    kind: "unknown_dependency",
                    message: format!("task {id} depends on unknown id {dep}"),
                });
            }
        }
        adjacency.insert(id.to_owned(), deps);
    }
    if let Some(cycle) = detect_cycle(&adjacency) {
        issues.push(GraphIssue {
            kind: "cycle",
            message: format!("cyclic depends_on: {}", cycle.join(" -> ")),
        });
    }
    issues
}

pub fn validate_graph_id(id: &str) -> Result<(), String> {
    if id.is_empty()
        || id.len() > 64
        || !id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
    {
        return Err("batch task id must be a short lowercase identifier".into());
    }
    Ok(())
}

fn detect_cycle(graph: &BTreeMap<String, Vec<String>>) -> Option<Vec<String>> {
    #[derive(Clone, Copy)]
    enum Color {
        White,
        Gray,
        Black,
    }
    let mut color = BTreeMap::new();
    for node in graph.keys() {
        color.insert(node.clone(), Color::White);
    }
    fn visit(
        node: &str,
        graph: &BTreeMap<String, Vec<String>>,
        color: &mut BTreeMap<String, Color>,
        stack: &mut Vec<String>,
    ) -> Option<Vec<String>> {
        color.insert(node.to_owned(), Color::Gray);
        stack.push(node.to_owned());
        if let Some(edges) = graph.get(node) {
            for next in edges {
                match color.get(next).copied().unwrap_or(Color::White) {
                    Color::Gray => {
                        let start = stack.iter().position(|item| item == next).unwrap_or(0);
                        let mut cycle = stack[start..].to_vec();
                        cycle.push(next.clone());
                        return Some(cycle);
                    }
                    Color::White => {
                        if let Some(cycle) = visit(next, graph, color, stack) {
                            return Some(cycle);
                        }
                    }
                    Color::Black => {}
                }
            }
        }
        stack.pop();
        color.insert(node.to_owned(), Color::Black);
        None
    }
    for node in graph.keys() {
        if matches!(color.get(node), Some(Color::White)) {
            let mut stack = Vec::new();
            if let Some(cycle) = visit(node, graph, &mut color, &mut stack) {
                return Some(cycle);
            }
        }
    }
    None
}

pub fn node_list_state(node: &DagNode) -> TaskState {
    match node.state {
        DagNodeState::Waiting | DagNodeState::Claimed | DagNodeState::Submitted => {
            TaskState::Queued
        }
        DagNodeState::Blocked => TaskState::Abandoned,
    }
}

pub fn pending_list_row(run_id: RunId, node: &DagNode, created_at_millis: u64) -> TaskListRow {
    let blocking_code = match node.state {
        DagNodeState::Waiting => Some(DAG_WAITING.to_owned()),
        DagNodeState::Claimed => Some(DAG_CLAIMED.to_owned()),
        DagNodeState::Blocked => node.blocked_by.clone(),
        DagNodeState::Submitted => None,
    };
    TaskListRow {
        task_id: node.task_id,
        run_id: Some(run_id),
        run_position: None,
        title: node
            .frozen
            .title
            .clone()
            .unwrap_or_else(|| node.batch_id.clone()),
        agent: node.frozen.agent.clone(),
        model: node.frozen.model.clone(),
        effort: node.frozen.effort.clone(),
        permissions: Some(node.frozen.permissions.clone()),
        env_profile: node.frozen.env_profile.clone(),
        state: node_list_state(node),
        blocking_code,
        last_outcome: None,
        worker: node.frozen.worker.clone(),
        branch: BranchName::for_task(node.task_id),
        turn_count: 0,
        runner: None,
        freshness: TaskFreshness::Current,
        created_at_millis,
        updated_at_millis: created_at_millis,
        active_turn_id: None,
        close_policy: node.frozen.close_on,
        review_state: ReviewState::NotReviewable,
        delivery: None,
        deliveries: Vec::new(),
    }
}

pub fn merge_pending_into_projection(
    projection: &mut TaskListProjection,
    run_id: RunId,
    dag: &DagRecord,
    created_at_millis: u64,
) {
    let mut dag_nodes = Vec::with_capacity(dag.nodes.len());
    for node in dag.nodes.values() {
        dag_nodes.push(DagNodeProjection {
            batch_id: node.batch_id.clone(),
            task_id: node.task_id,
            turn_id: node.turn_id,
            state: node.state,
            depends_on: node.depends_on.clone(),
            blocked_by: node.blocked_by.clone(),
            bound_oid: node.bound_oid.clone(),
        });
        if node.state == DagNodeState::Submitted {
            continue;
        }
        if projection
            .tasks
            .iter()
            .any(|row| row.task_id == node.task_id)
        {
            continue;
        }
        projection
            .tasks
            .push(pending_list_row(run_id, node, created_at_millis));
    }
    projection.dag_nodes.extend(dag_nodes);
    if let Some(run) = projection.runs.iter_mut().find(|run| run.run_id == run_id) {
        let states = projection
            .tasks
            .iter()
            .filter(|task| task.run_id == Some(run_id))
            .map(|task| task.state);
        run.progress = RunProgress::from_states(states);
    }
    projection.progress = RunProgress::from_states(projection.tasks.iter().map(|task| task.state));
}

pub fn dag_run_is_quiescent(dag: &DagRecord) -> bool {
    dag.nodes
        .values()
        .all(|node| matches!(node.state, DagNodeState::Submitted | DagNodeState::Blocked))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        agent::{AgentKind, PermissionPolicy},
        job::JobId,
        task::{
            BaseOid, ClosePolicy, GitIdentity, PublishMode, TaskMeta, TaskMetaInput, TaskSource,
            TaskStatus, TurnSummary, TurnTerminal,
        },
    };
    use uuid::Uuid;

    fn oid() -> BaseOid {
        "0123456789abcdef0123456789abcdef01234567".parse().unwrap()
    }

    fn fixture_record(state: TaskState, outcome: Option<TaskOutcome>) -> LocalTaskRecord {
        fixture_record_with_turn(state, outcome, None, None)
    }

    fn fixture_record_with_turn(
        state: TaskState,
        outcome: Option<TaskOutcome>,
        fetched_head: Option<BaseOid>,
        turn_id: Option<TurnId>,
    ) -> LocalTaskRecord {
        let task_id = TaskId::new(Uuid::from_u128(1));
        let base_oid = oid();
        let turn = turn_id.unwrap_or_else(|| JobId::new(Uuid::from_u128(2)));
        let meta = TaskMeta::new(TaskMetaInput {
            task_id,
            run_id: None,
            project_id: "a".repeat(64),
            worktree_id: "b".repeat(64),
            agent: AgentKind::Codex,
            model: None,
            effort: None,
            policy: PermissionPolicy::Workspace,
            source: TaskSource::Local {
                wip: false,
                push_target: None,
            },
            publish: vec![PublishMode::Fetch],
            publish_branch: None,
            base_oid: base_oid.clone(),
            limits: TaskLimits::default(),
            close_policy: ClosePolicy::Never,
            env_profile: None,
            git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
            title: None,
            prompt: "gate".into(),
            created_at_millis: 1,
        })
        .unwrap();
        let terminal = match &outcome {
            Some(TaskOutcome::Done | TaskOutcome::NeedsInput | TaskOutcome::Unknown) => {
                Some(TurnTerminal::Succeeded)
            }
            Some(TaskOutcome::Failed { .. } | TaskOutcome::Blocked) => Some(TurnTerminal::Failed),
            Some(TaskOutcome::Cancelled) => Some(TurnTerminal::Cancelled),
            Some(TaskOutcome::TimedOut) => Some(TurnTerminal::TimedOut),
            Some(TaskOutcome::Lost) => Some(TurnTerminal::Lost),
            None => None,
        };
        let status = TaskStatus::new(
            state,
            outcome.clone(),
            Some("mini-1".into()),
            false,
            Some(fetched_head.clone().unwrap_or(base_oid)),
            None,
            Vec::new(),
            Vec::new(),
            None,
            vec![TurnSummary::new(
                1,
                turn,
                terminal,
                outcome,
                Some(true),
                false,
                Some(1),
                Some(2),
            )],
            2,
        )
        .unwrap();
        LocalTaskRecord::new(
            meta,
            status,
            None,
            None,
            fetched_head,
            "c".repeat(64),
            None,
            false,
            None,
        )
        .unwrap()
    }

    #[test]
    fn from_base_without_depends_on_is_a_graph_error_once_parent_is_missing() {
        let issues = validate_batch_graph(&[GraphNode {
            id: Some("child"),
            depends_on: &[],
            base: "from:missing",
        }]);
        assert!(
            issues
                .iter()
                .any(|issue| issue.kind == "unknown_dependency"),
            "{issues:?}"
        );
    }

    #[test]
    fn from_base_derives_a_dependency_edge_for_cycle_detection() {
        let issues = validate_batch_graph(&[
            GraphNode {
                id: Some("a"),
                depends_on: &[],
                base: "from:b",
            },
            GraphNode {
                id: Some("b"),
                depends_on: &["a".into()],
                base: "HEAD",
            },
        ]);
        assert!(
            issues.iter().any(|issue| issue.kind == "cycle"),
            "{issues:?}"
        );
    }

    #[test]
    fn closed_without_done_is_not_acceptance() {
        let closed_failed = fixture_record(
            TaskState::Closed,
            Some(TaskOutcome::failed("agent exited 1")),
        );
        let closed_needs_input = fixture_record(TaskState::Closed, Some(TaskOutcome::NeedsInput));
        let closed_done = fixture_record(TaskState::Closed, Some(TaskOutcome::Done));
        assert_eq!(parent_gate(&closed_failed), ParentGate::Failed);
        assert_eq!(parent_gate(&closed_needs_input), ParentGate::Failed);
        assert_eq!(parent_gate(&closed_done), ParentGate::Ready);
    }

    #[test]
    fn open_failed_blocks_descendants_and_needs_input_waits() {
        let open_failed =
            fixture_record(TaskState::Open, Some(TaskOutcome::failed("agent exited 1")));
        let open_blocked = fixture_record(TaskState::Open, Some(TaskOutcome::Blocked));
        let open_needs = fixture_record(TaskState::Open, Some(TaskOutcome::NeedsInput));
        let open_done = fixture_record(TaskState::Open, Some(TaskOutcome::Done));
        assert_eq!(parent_gate(&open_failed), ParentGate::Failed);
        assert_eq!(parent_gate(&open_blocked), ParentGate::Failed);
        assert_eq!(parent_gate(&open_needs), ParentGate::Waiting);
        assert_eq!(parent_gate(&open_done), ParentGate::Waiting);
        assert_eq!(
            parent_gate(&fixture_record(TaskState::Abandoned, None)),
            ParentGate::Failed
        );
        assert_eq!(
            parent_gate(&fixture_record(TaskState::Queued, None)),
            ParentGate::Waiting
        );
    }

    #[test]
    fn claimed_node_recovery_distinguishes_live_owner_from_dead_and_existing_task() {
        let caller = ProcessIdentity::new(7, 1).unwrap();
        let other = ProcessIdentity::new(8, 2).unwrap();
        assert_eq!(
            claimed_node_action(Some(other), caller, true, false, false),
            ClaimedNodeAction::SkipLive
        );
        assert_eq!(
            claimed_node_action(Some(other), caller, false, false, false),
            ClaimedNodeAction::Retake
        );
        assert_eq!(
            claimed_node_action(Some(caller), caller, true, false, false),
            ClaimedNodeAction::Continue
        );
        assert_eq!(
            claimed_node_action(Some(other), caller, true, true, true),
            ClaimedNodeAction::MarkSubmitted
        );
        assert_eq!(
            claimed_node_action(Some(other), caller, true, true, false),
            ClaimedNodeAction::SkipLive
        );
        assert_eq!(
            claimed_node_action(Some(other), caller, false, true, false),
            ClaimedNodeAction::Retake
        );
        assert_eq!(
            claimed_node_action(None, caller, false, true, false),
            ClaimedNodeAction::Retake
        );
        assert_eq!(
            claimed_node_action(None, caller, false, false, false),
            ClaimedNodeAction::Retake
        );
    }

    #[test]
    fn retention_pins_are_canonical_and_ignore_serialized_strings() {
        let run_id = RunId::generate();
        let frozen = sample_frozen();
        let node = DagNode {
            batch_id: "login".into(),
            task_id: TaskId::generate(),
            turn_id: TurnId::generate(),
            depends_on: Vec::new(),
            base: DagBase::Frozen {
                oid: oid(),
                pin_ref: dag_pin_ref(run_id, "login"),
                wip: false,
            },
            frozen,
            state: DagNodeState::Waiting,
            bound_oid: None,
            bound_turn_id: None,
            pin_ref: None,
            blocked_by: None,
            claimed_by: None,
            claimed_at_millis: None,
        };
        let record =
            DagRecord::new(run_id, BTreeMap::from([("login".into(), node)]), 1, None, 1).unwrap();
        assert_eq!(
            record.retention_pin_refs(),
            vec![dag_pin_ref(run_id, "login")]
        );
    }

    fn sample_frozen() -> DagFrozenSpec {
        DagFrozenSpec {
            prompt: "do work".into(),
            title: None,
            agent: "codex".into(),
            model: None,
            effort: None,
            source: "local".into(),
            origin_url: None,
            publish: vec!["fetch".into()],
            publish_branch: None,
            close_on: ClosePolicy::Done,
            env_profile: None,
            worker: None,
            wip: false,
            project_path: "/tmp/project".into(),
            project_id: "a".repeat(64),
            worktree_id: "b".repeat(64),
            timeout_millis: 45 * 60 * 1000,
            max_turns: None,
            max_budget_usd_cents: None,
            max_followups: 10,
            permissions: "workspace".into(),
            requires: vec!["agent:codex".into()],
            include_untracked: Vec::new(),
            include_empty_dirs: Vec::new(),
            allow_sensitive: Vec::new(),
            cli_includes: Vec::new(),
            branch: Some("main".into()),
        }
    }
}
