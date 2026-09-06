use std::{collections::HashMap, fmt};

use serde::Serialize;

use crate::{
    agent::{AgentKind, PermissionPolicy},
    redaction::RedactionBoundary,
    scheduler::QueueBlockingReason,
    task::{
        BaseOid, BranchName, LocalTaskRecord, RunId, RunProgress, RunRecord, RunnerState, TaskId,
        TaskOutcome, TaskState, TaskStatus, TurnId, TurnSummary, TurnTerminal,
    },
};

const MAX_TASK_VIEW_ERROR_MESSAGE_CHARS: usize = 256;
const MAX_WORKER_NAME_CHARS: usize = 128;
const TASK_VIEW_OVERFLOW_CODE: &str = "TASK_VIEW_OVERFLOW";
const TASK_VIEW_MISSING_TASK_CODE: &str = "TASK_VIEW_MISSING_TASK";
const PATH_PLACEHOLDER: &str = "[path]";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskFreshness {
    Current,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskListProjection {
    pub tasks: Vec<TaskListRow>,
    pub runs: Vec<TaskRunProjection>,
    pub progress: RunProgress,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskListRow {
    pub task_id: TaskId,
    pub run_id: Option<RunId>,
    pub run_position: Option<u32>,
    pub title: String,
    pub agent: String,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub permissions: Option<String>,
    pub env_profile: Option<String>,
    pub state: TaskState,
    pub blocking_code: Option<String>,
    pub last_outcome: Option<TaskOutcome>,
    pub worker: Option<String>,
    pub branch: BranchName,
    pub turn_count: u32,
    pub runner: Option<RunnerState>,
    pub freshness: TaskFreshness,
    pub created_at_millis: u64,
    pub updated_at_millis: u64,
    pub active_turn_id: Option<TurnId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskRunProjection {
    pub run_id: RunId,
    pub name: Option<String>,
    pub max_parallel: u32,
    pub created_at_millis: u64,
    pub progress: RunProgress,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskDetailProjection {
    pub task: TaskListRow,
    pub project_id: String,
    pub worktree_id: String,
    pub base_oid: Option<BaseOid>,
    pub head_oid: Option<BaseOid>,
    pub session_present: bool,
    pub summary: Option<String>,
    pub questions: Vec<String>,
    pub files_changed: Vec<String>,
    pub diff_stat: Option<String>,
    pub fetch_command: String,
    pub turns: Vec<TaskTurnProjection>,
    pub timeline: Vec<TaskTimelineEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskTurnProjection {
    pub turn_number: u32,
    pub turn_id: TurnId,
    pub terminal: Option<TurnTerminal>,
    pub outcome: Option<TaskOutcome>,
    pub agent_committed: Option<bool>,
    pub log_truncated: bool,
    pub started_at_millis: Option<u64>,
    pub ended_at_millis: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskTimelineEvent {
    pub turn_number: u32,
    pub turn_id: TurnId,
    pub outcome: Option<TaskOutcome>,
    pub started_at_millis: Option<u64>,
    pub ended_at_millis: Option<u64>,
    pub terminal: Option<TurnTerminal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskListJson {
    pub protocol_version: u32,
    #[serde(flatten)]
    pub projection: TaskListProjection,
}

impl TaskListJson {
    pub fn new(protocol_version: u32, projection: TaskListProjection) -> Self {
        Self {
            protocol_version,
            projection,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskViewError {
    code: &'static str,
    message: String,
}

impl TaskViewError {
    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    fn new(code: &'static str, message: impl AsRef<str>) -> Self {
        Self {
            code,
            message: RedactionBoundary::from_env()
                .text(message.as_ref(), MAX_TASK_VIEW_ERROR_MESSAGE_CHARS),
        }
    }

    fn overflow(field: &'static str) -> Self {
        Self::new(
            TASK_VIEW_OVERFLOW_CODE,
            format!("{field} exceeds the task view numeric bound"),
        )
    }

    fn missing_task() -> Self {
        Self::new(
            TASK_VIEW_MISSING_TASK_CODE,
            "run references a task that is not present in the local task records",
        )
    }
}

impl fmt::Display for TaskViewError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for TaskViewError {}

pub fn project_task_list(
    records: &[LocalTaskRecord],
    runs: &[RunRecord],
    runner_states: &HashMap<TaskId, Option<RunnerState>>,
    freshness: &HashMap<TaskId, TaskFreshness>,
) -> Result<TaskListProjection, TaskViewError> {
    project_task_list_with_blocking_codes(records, runs, runner_states, freshness, &HashMap::new())
}

pub fn project_task_list_with_blocking_codes(
    records: &[LocalTaskRecord],
    runs: &[RunRecord],
    runner_states: &HashMap<TaskId, Option<RunnerState>>,
    freshness: &HashMap<TaskId, TaskFreshness>,
    blocking_codes: &HashMap<TaskId, String>,
) -> Result<TaskListProjection, TaskViewError> {
    let mut states = HashMap::with_capacity(records.len());
    for record in records {
        states.insert(record.meta().task_id(), record.status().state());
    }

    let mut run_positions = HashMap::new();
    for run in runs {
        for (index, task_id) in run.task_ids().iter().copied().enumerate() {
            if !states.contains_key(&task_id) {
                return Err(TaskViewError::missing_task());
            }
            let position = checked_position(index)?;
            run_positions.insert((task_id, run.run_id()), position);
        }
    }

    let mut tasks = records
        .iter()
        .map(|record| {
            let task_id = record.meta().task_id();
            let run_position = record
                .meta()
                .run_id()
                .and_then(|run_id| run_positions.get(&(task_id, run_id)).copied());
            task_list_row(
                record,
                record.status(),
                runner_states.get(&task_id).copied().flatten(),
                freshness
                    .get(&task_id)
                    .copied()
                    .unwrap_or(TaskFreshness::Current),
                run_position,
                blocking_codes.get(&task_id).cloned(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    tasks.sort_by(|left, right| {
        left.updated_at_millis
            .cmp(&right.updated_at_millis)
            .then_with(|| left.task_id.to_string().cmp(&right.task_id.to_string()))
    });

    let progress = RunProgress::from_states(tasks.iter().map(|task| task.state));
    let boundary = RedactionBoundary::from_env();
    let runs = runs
        .iter()
        .map(|run| {
            let states = run
                .task_ids()
                .iter()
                .map(|task_id| {
                    states
                        .get(task_id)
                        .copied()
                        .ok_or_else(TaskViewError::missing_task)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(TaskRunProjection {
                run_id: run.run_id(),
                name: run.name().map(|name| boundary.title(name)),
                max_parallel: run.max_parallel(),
                created_at_millis: run.created_at_millis(),
                progress: RunProgress::from_states(states),
            })
        })
        .collect::<Result<Vec<_>, TaskViewError>>()?;

    Ok(TaskListProjection {
        tasks,
        runs,
        progress,
    })
}

pub fn project_task_detail(
    record: &LocalTaskRecord,
    status: &TaskStatus,
    runner: Option<RunnerState>,
    freshness: TaskFreshness,
) -> Result<TaskDetailProjection, TaskViewError> {
    let boundary = RedactionBoundary::from_env();
    let task = task_list_row(record, status, runner, freshness, None, None)?;
    let turns = status
        .turns()
        .iter()
        .map(|turn| project_turn(turn, &boundary))
        .collect::<Vec<_>>();
    let timeline = status
        .turns()
        .iter()
        .map(|turn| TaskTimelineEvent {
            turn_number: turn.turn_number(),
            turn_id: turn.turn_id(),
            outcome: turn
                .outcome()
                .map(|outcome| redact_outcome(outcome, &boundary)),
            started_at_millis: turn.started_at_millis(),
            ended_at_millis: turn.ended_at_millis(),
            terminal: turn.terminal(),
        })
        .collect::<Vec<_>>();

    Ok(TaskDetailProjection {
        task,
        project_id: record.meta().project_id().to_owned(),
        worktree_id: record.meta().worktree_id().to_owned(),
        base_oid: Some(record.meta().base_oid().clone()),
        head_oid: status.head_oid().cloned(),
        session_present: status.session_present(),
        summary: status.summary().map(|summary| boundary.summary(summary)),
        questions: boundary.questions(status.questions()),
        files_changed: status
            .files_changed()
            .iter()
            .map(|path| safe_changed_file(&boundary, path))
            .collect(),
        diff_stat: status
            .diff_stat()
            .map(|diff_stat| boundary.diff_stat(diff_stat)),
        fetch_command: format!("worker task fetch {}", record.meta().task_id()),
        turns,
        timeline,
    })
}

fn task_list_row(
    record: &LocalTaskRecord,
    status: &TaskStatus,
    runner: Option<RunnerState>,
    freshness: TaskFreshness,
    run_position: Option<u32>,
    blocking_code: Option<String>,
) -> Result<TaskListRow, TaskViewError> {
    let boundary = RedactionBoundary::from_env();
    let turn_count =
        u32::try_from(status.turns().len()).map_err(|_| TaskViewError::overflow("turn_count"))?;
    let active_turn_id = (status.state() == TaskState::Active)
        .then(|| status.turns().last())
        .flatten()
        .filter(|turn| turn.terminal().is_none())
        .map(TurnSummary::turn_id);
    let task_id = record.meta().task_id();

    Ok(TaskListRow {
        task_id,
        run_id: record.meta().run_id(),
        run_position,
        title: boundary.title(record.meta().title().as_str()),
        agent: agent_name(record.meta().agent()).to_owned(),
        model: record.meta().model().map(|model| boundary.text(model, 256)),
        effort: None,
        permissions: Some(permission_name(record.meta().policy()).to_owned()),
        env_profile: record
            .meta()
            .env_profile()
            .map(|profile| boundary.text(profile, 128)),
        state: status.state(),
        blocking_code,
        last_outcome: status
            .last_outcome()
            .map(|outcome| redact_outcome(outcome, &boundary)),
        worker: status
            .worker()
            .map(|worker| boundary.text(worker, MAX_WORKER_NAME_CHARS)),
        branch: BranchName::for_task(task_id),
        turn_count,
        runner,
        freshness,
        created_at_millis: record.meta().created_at_millis(),
        updated_at_millis: status.updated_at_millis(),
        active_turn_id,
    })
}

pub(crate) fn queue_blocking_code(reason: Option<&QueueBlockingReason>) -> &'static str {
    match reason {
        Some(QueueBlockingReason::PinnedWorkerBusy { .. }) => "PINNED_WORKER_BUSY",
        Some(QueueBlockingReason::CapabilityMissing { .. }) => "CAPABILITY_MISSING",
        Some(QueueBlockingReason::RunCap) => "RUN_MAX_PARALLEL",
        Some(QueueBlockingReason::NoEligibleWorker) => "NO_COMPATIBLE_IDLE_WORKER",
        None => "WAITING_FOR_DISPATCH",
    }
}

fn project_turn(turn: &TurnSummary, boundary: &RedactionBoundary) -> TaskTurnProjection {
    TaskTurnProjection {
        turn_number: turn.turn_number(),
        turn_id: turn.turn_id(),
        terminal: turn.terminal(),
        outcome: turn
            .outcome()
            .map(|outcome| redact_outcome(outcome, boundary)),
        agent_committed: turn.agent_committed(),
        log_truncated: turn.log_truncated(),
        started_at_millis: turn.started_at_millis(),
        ended_at_millis: turn.ended_at_millis(),
    }
}

fn redact_outcome(outcome: &TaskOutcome, boundary: &RedactionBoundary) -> TaskOutcome {
    match outcome {
        TaskOutcome::Failed { reason } => TaskOutcome::Failed {
            reason: boundary.failure_reason(reason),
        },
        other => other.clone(),
    }
}

fn safe_changed_file(boundary: &RedactionBoundary, path: &str) -> String {
    let redacted = boundary.changed_file(path);
    if is_unsafe_path(&redacted) {
        PATH_PLACEHOLDER.to_owned()
    } else {
        redacted
    }
}

fn is_unsafe_path(value: &str) -> bool {
    value.starts_with('/')
        || value.starts_with('\\')
        || value.starts_with("~/")
        || value.starts_with("~\\")
        || value.as_bytes().get(1) == Some(&b':')
        || value.split(['/', '\\']).any(|component| component == "..")
}

fn checked_position(index: usize) -> Result<u32, TaskViewError> {
    let index = u32::try_from(index).map_err(|_| TaskViewError::overflow("run_position"))?;
    index
        .checked_add(1)
        .ok_or_else(|| TaskViewError::overflow("run_position"))
}

fn agent_name(agent: AgentKind) -> &'static str {
    match agent {
        AgentKind::Codex => "codex",
        AgentKind::Claude => "claude",
        AgentKind::Cursor => "cursor",
        AgentKind::Opencode => "opencode",
    }
}

fn permission_name(policy: PermissionPolicy) -> &'static str {
    match policy {
        PermissionPolicy::Workspace => "workspace",
        PermissionPolicy::Unattended => "unattended",
    }
}
