use std::{collections::HashMap, fmt};

use serde::{Deserialize, Serialize};

use crate::{
    agent::{AgentKind, PermissionPolicy, Question, ReportedCheckStatus},
    job::QueueState,
    redaction::RedactionBoundary,
    scheduler::QueueBlockingReason,
    task::{
        BaseOid, BranchName, ClosePolicy, LocalTaskRecord, OriginDelivery, PublishMode, RunId,
        RunProgress, RunRecord, RunnerState, TaskId, TaskOutcome, TaskState, TaskStatus, TurnId,
        TurnSummary, TurnTerminal,
    },
};

const MAX_TASK_VIEW_ERROR_MESSAGE_CHARS: usize = 256;
const MAX_WORKER_NAME_CHARS: usize = 128;
const TASK_VIEW_OVERFLOW_CODE: &str = "TASK_VIEW_OVERFLOW";
const TASK_VIEW_MISSING_TASK_CODE: &str = "TASK_VIEW_MISSING_TASK";
const PATH_PLACEHOLDER: &str = "[path]";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewState {
    NotReviewable,
    WaitingOnYou,
    ReadyForReview,
    ReadyForFollowUp,
    ClosePending,
    Accepted,
    ClosedAfterDone,
    Closed,
}

/// Remote overlay is a read-only projection. Skip it while a local close
/// fence is unresolved, and while a local drain diagnostic must remain visible.
/// Extra local-only guards AND onto this predicate so CLI `status`/`list`
/// and the dashboard cannot drift.
pub fn remote_status_refresh_allowed(record: &LocalTaskRecord) -> bool {
    record.close_intent().is_none()
        && matches!(record.status().state(), TaskState::Active | TaskState::Open)
        && record.abandon_code() != Some("LOG_DRAIN_UNAVAILABLE")
}

/// Same guards as [`remote_status_refresh_allowed`], plus closed tasks whose
/// origin delivery is still pending or retrying. Status, list, result, and
/// the dashboard share this so a failed push stays visible after close.
pub fn remote_observation_allowed(record: &LocalTaskRecord) -> bool {
    record.close_intent().is_none()
        && record.abandon_code() != Some("LOG_DRAIN_UNAVAILABLE")
        && record.needs_remote_observation()
}

/// One CLI line per origin delivery. Short turn IDs match the dashboard chip.
pub fn format_delivery_line(delivery: &OriginDelivery) -> String {
    let mut line = format!(
        "delivery: {} turn={} branch={} attempt={}",
        delivery.state().as_str(),
        short_turn_id(delivery.turn_id()),
        delivery.branch(),
        delivery.attempt(),
    );
    if let Some(error) = delivery.last_error() {
        line.push_str(" error=");
        line.push_str(error);
    }
    line
}

/// `push: retrying` suffix for text `task list`, only when publish includes push.
pub fn format_list_push_suffix(row: &TaskListRow) -> Option<String> {
    if !row.publish_push {
        return None;
    }
    Some(format!(
        " push: {}",
        row.delivery.as_ref()?.state().as_str()
    ))
}

/// Delivery for the latest turn, else the newest recorded delivery.
pub fn last_turn_delivery<'a>(
    status: &TaskStatus,
    deliveries: &'a [OriginDelivery],
) -> Option<&'a OriginDelivery> {
    let last_turn = status.turns().last().map(TurnSummary::turn_id);
    last_turn
        .and_then(|turn_id| {
            deliveries
                .iter()
                .find(|delivery| delivery.turn_id() == turn_id)
        })
        .or_else(|| deliveries.first())
}

fn short_turn_id(turn_id: TurnId) -> String {
    let rendered = turn_id.to_string();
    rendered.chars().take(8).collect()
}

pub fn review_state(record: &LocalTaskRecord, status: &TaskStatus) -> ReviewState {
    // Unresolved local fence wins over any projected overlay, including a
    // remote Closed status observed after a lost close response.
    if record.close_intent().is_some() {
        return ReviewState::ClosePending;
    }
    match status.state() {
        TaskState::Queued | TaskState::Active => ReviewState::NotReviewable,
        TaskState::Open => match status.last_outcome() {
            Some(TaskOutcome::NeedsInput) => ReviewState::WaitingOnYou,
            Some(TaskOutcome::Done) => ReviewState::ReadyForReview,
            Some(_) => ReviewState::ReadyForFollowUp,
            None => ReviewState::ReadyForFollowUp,
        },
        TaskState::Closed => {
            if record.meta().close_policy() == ClosePolicy::Never
                && matches!(status.last_outcome(), Some(TaskOutcome::Done))
            {
                ReviewState::Accepted
            } else if record.meta().close_policy() == ClosePolicy::Done
                && matches!(status.last_outcome(), Some(TaskOutcome::Done))
            {
                ReviewState::ClosedAfterDone
            } else {
                ReviewState::Closed
            }
        }
        TaskState::Abandoned | TaskState::Lost => ReviewState::Closed,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskReportedCheckProjection {
    pub name: String,
    pub command: String,
    pub status: ReportedCheckStatus,
    pub detail: String,
    pub source: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskFreshness {
    Current,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskListProjection {
    pub tasks: Vec<TaskListRow>,
    pub runs: Vec<TaskRunProjection>,
    pub progress: RunProgress,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dag_nodes: Vec<crate::dag::DagNodeProjection>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub residual: Option<Vec<String>>,
    pub last_outcome: Option<TaskOutcome>,
    pub worker: Option<String>,
    pub branch: BranchName,
    pub turn_count: u32,
    pub runner: Option<RunnerState>,
    pub freshness: TaskFreshness,
    pub created_at_millis: u64,
    pub updated_at_millis: u64,
    pub active_turn_id: Option<TurnId>,
    pub close_policy: ClosePolicy,
    pub review_state: ReviewState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<OriginDelivery>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deliveries: Vec<OriginDelivery>,
    /// True when the task asked for an origin push. Text `task list` uses
    /// this so fetch-only rows never grow a `push:` suffix.
    #[serde(skip)]
    pub publish_push: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    pub questions: Vec<Question>,
    pub files_changed: Vec<String>,
    pub diff_stat: Option<String>,
    pub fetch_command: String,
    pub review_state: ReviewState,
    pub close_policy: ClosePolicy,
    pub reported_checks: Vec<TaskReportedCheckProjection>,
    pub fetched_head: Option<BaseOid>,
    pub fetched_ref: Option<String>,
    pub review_commands: Vec<String>,
    pub turns: Vec<TaskTurnProjection>,
    pub timeline: Vec<TaskTimelineEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery: Option<OriginDelivery>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub deliveries: Vec<OriginDelivery>,
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
        dag_nodes: Vec::new(),
    })
}

/// Narrows already-projected list rows by `--state` / `--outcome`.
///
/// Projection must see every task the run still names, otherwise
/// `run_position` and per-run progress would be rebuilt as if the survivors
/// were a new run. Filtering the rows afterwards keeps those positions and
/// blocking codes, and only the list-wide progress follows the visible set.
pub fn filter_task_list(
    mut projection: TaskListProjection,
    state: Option<TaskState>,
    outcome: Option<&str>,
) -> TaskListProjection {
    if state.is_none() && outcome.is_none() {
        return projection;
    }
    projection.tasks.retain(|task| {
        state.is_none_or(|expected| task.state == expected)
            && outcome.is_none_or(|kind| {
                task.last_outcome
                    .as_ref()
                    .is_some_and(|value| value.kind() == kind)
            })
    });
    projection.progress = RunProgress::from_states(projection.tasks.iter().map(|task| task.state));
    projection
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
        review_state: review_state(record, status),
        close_policy: record.meta().close_policy(),
        reported_checks: status
            .reported_checks()
            .iter()
            .map(|check| TaskReportedCheckProjection {
                name: boundary.text(check.name(), crate::agent::MAX_CHECK_NAME_BYTES),
                command: boundary.text(check.command(), crate::agent::MAX_CHECK_COMMAND_BYTES),
                status: check.status(),
                detail: boundary.text(check.detail(), crate::agent::MAX_CHECK_DETAIL_BYTES),
                source: check.source(),
            })
            .collect(),
        fetched_head: record.fetched_head().cloned(),
        fetched_ref: status.worker().map(|worker| {
            format!(
                "refs/remotes/mac-worker/{worker}/task/{}",
                record.meta().task_id()
            )
        }),
        review_commands: review_commands(record, status),
        turns,
        timeline,
        delivery: record.delivery().cloned(),
        deliveries: record.deliveries().to_vec(),
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
        effort: record
            .meta()
            .effort()
            .map(|effort| boundary.text(effort, 256)),
        permissions: Some(permission_name(record.meta().policy()).to_owned()),
        env_profile: record
            .meta()
            .env_profile()
            .map(|profile| boundary.text(profile, 128)),
        state: status.state(),
        blocking_code,
        stage: record
            .failure_receipt()
            .map(|receipt| receipt.stage().to_owned()),
        residual: record.failure_receipt().map(|receipt| {
            receipt
                .residual()
                .iter()
                .map(|item| (*item).to_owned())
                .collect()
        }),
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
        close_policy: record.meta().close_policy(),
        review_state: review_state(record, status),
        delivery: record.delivery().cloned(),
        deliveries: record.deliveries().to_vec(),
        publish_push: record.meta().publish().contains(&PublishMode::Push),
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

/// Blocking code a task row reports. Busy-but-capable workers stay
/// `WAITING_FOR_DISPATCH`; a requirement no configured worker offers is named.
pub fn task_row_blocking_code(reason: Option<&QueueBlockingReason>) -> String {
    match reason {
        Some(QueueBlockingReason::CapabilityMissing { missing }) => {
            format!("NO_WORKER_OFFERS:{}", missing.join(","))
        }
        Some(QueueBlockingReason::PinnedWorkerBusy { .. }) => "PINNED_WORKER_BUSY".to_owned(),
        Some(QueueBlockingReason::RunCap) => "RUN_MAX_PARALLEL".to_owned(),
        Some(QueueBlockingReason::NoEligibleWorker) | None => "WAITING_FOR_DISPATCH".to_owned(),
    }
}

/// Parked rows must name the stall: a missing requirement, full eligible
/// workers, or the existing waiting codes for pin/run-cap cases.
pub fn parked_task_row_blocking_code(reason: Option<&QueueBlockingReason>) -> String {
    match reason {
        Some(QueueBlockingReason::CapabilityMissing { missing }) => {
            format!("CAPABILITY_MISSING:{}", missing.join(","))
        }
        Some(QueueBlockingReason::PinnedWorkerBusy { .. }) => "PINNED_WORKER_BUSY".to_owned(),
        Some(QueueBlockingReason::RunCap) => "RUN_MAX_PARALLEL".to_owned(),
        Some(QueueBlockingReason::NoEligibleWorker) | None => "CAPACITY_BUSY".to_owned(),
    }
}

pub fn task_row_blocking_code_for_queue_state(
    state: &QueueState,
    reason: Option<&QueueBlockingReason>,
) -> String {
    if matches!(state, QueueState::Parked) {
        parked_task_row_blocking_code(reason)
    } else {
        task_row_blocking_code(reason)
    }
}

fn review_commands(record: &LocalTaskRecord, status: &TaskStatus) -> Vec<String> {
    let task_id = record.meta().task_id();
    let mut commands = vec![format!("worker task fetch {task_id}")];
    if let Some(worker) = status.worker() {
        let fetched_ref = format!("refs/remotes/mac-worker/{worker}/task/{task_id}");
        commands.push(format!("git rev-parse {fetched_ref}"));
        if record.fetched_head().is_some() {
            commands.push(format!("git log -1 --oneline {fetched_ref}"));
        }
    }
    commands
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

#[cfg(test)]
mod tests {
    use super::{
        TaskFreshness, TaskListRow, format_delivery_line, format_list_push_suffix,
        last_turn_delivery, project_task_detail,
    };
    use crate::{
        agent::{AgentKind, PermissionPolicy},
        job::JobId,
        protocol::PROTOCOL_VERSION,
        task::{
            BaseOid, BranchName, ClosePolicy, DeliveryState, GitIdentity, LocalTaskRecord,
            OriginDelivery, PublishMode, TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome,
            TaskSource, TaskState, TaskStatus, TurnSummary, TurnTerminal,
        },
        task_client::{ControllerTaskProjection, TaskReport, TaskResultReport},
        task_view::ReviewState,
    };
    use uuid::Uuid;

    fn turn_id() -> crate::task::TurnId {
        JobId::new(Uuid::from_u128(0x018f_0f4a_6b5c_7d8e_9f00_1122_3344_5566))
    }

    fn origin_delivery(state: DeliveryState, error: Option<&str>) -> OriginDelivery {
        OriginDelivery::new(
            turn_id(),
            state,
            "0123456789abcdef0123456789abcdef01234567"
                .parse::<BaseOid>()
                .unwrap(),
            "https://example.test/repo.git".into(),
            "refs/heads/release-candidate".into(),
            3,
            9,
            error.map(str::to_owned),
            None,
            1,
            2,
        )
        .unwrap()
    }

    fn closed_record(
        publish: Vec<PublishMode>,
        delivery: Option<OriginDelivery>,
    ) -> LocalTaskRecord {
        let task_id = TaskId::new(Uuid::from_u128(1));
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
                push_target: publish.contains(&PublishMode::Push).then(|| {
                    crate::task::PushTarget::new("https://example.test/repo.git".into()).unwrap()
                }),
            },
            publish,
            publish_branch: None,
            base_oid: "0123456789abcdef0123456789abcdef01234567".parse().unwrap(),
            limits: TaskLimits::default(),
            close_policy: ClosePolicy::Never,
            env_profile: None,
            git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
            title: None,
            prompt: "fixture".into(),
            created_at_millis: 1,
        })
        .unwrap();
        let status = TaskStatus::new(
            TaskState::Closed,
            Some(TaskOutcome::Done),
            Some("mini-1".into()),
            false,
            Some(meta.base_oid().clone()),
            None,
            Vec::new(),
            Vec::new(),
            None,
            vec![TurnSummary::new(
                1,
                turn_id(),
                Some(TurnTerminal::Succeeded),
                Some(TaskOutcome::Done),
                Some(true),
                false,
                Some(1),
                Some(2),
            )],
            2,
        )
        .unwrap();
        let record = LocalTaskRecord::new(
            meta,
            status,
            None,
            None,
            None,
            "c".repeat(64),
            None,
            false,
            None,
        )
        .unwrap();
        match delivery {
            Some(delivery) => record.with_delivery(Some(delivery)).unwrap(),
            None => record,
        }
    }

    fn list_row(delivery: Option<OriginDelivery>, publish_push: bool) -> TaskListRow {
        TaskListRow {
            task_id: TaskId::new(Uuid::from_u128(1)),
            run_id: None,
            run_position: None,
            title: "Repair login".into(),
            agent: "codex".into(),
            model: None,
            effort: None,
            permissions: None,
            env_profile: None,
            state: TaskState::Closed,
            blocking_code: None,
            stage: None,
            residual: None,
            last_outcome: Some(TaskOutcome::Done),
            worker: Some("mini-1".into()),
            branch: BranchName::for_task(TaskId::new(Uuid::from_u128(1))),
            turn_count: 1,
            runner: None,
            freshness: TaskFreshness::Current,
            created_at_millis: 1,
            updated_at_millis: 2,
            active_turn_id: None,
            close_policy: ClosePolicy::Never,
            review_state: ReviewState::Closed,
            delivery: delivery.clone(),
            deliveries: delivery.into_iter().collect(),
            publish_push,
        }
    }

    #[test]
    fn delivery_line_names_state_short_turn_branch_attempt_and_error() {
        let delivery = origin_delivery(DeliveryState::Retrying, Some("ORIGIN_AUTH_FAILED"));
        assert_eq!(
            format_delivery_line(&delivery),
            "delivery: retrying turn=018f0f4a branch=release-candidate attempt=3 error=ORIGIN_AUTH_FAILED"
        );
    }

    #[test]
    fn list_push_suffix_is_reserved_for_origin_push_tasks() {
        let delivered = origin_delivery(DeliveryState::Delivered, None);
        assert_eq!(
            format_list_push_suffix(&list_row(Some(delivered.clone()), true)).as_deref(),
            Some(" push: delivered")
        );
        assert_eq!(
            format_list_push_suffix(&list_row(Some(delivered), false)),
            None
        );
        assert_eq!(format_list_push_suffix(&list_row(None, true)), None);
    }

    #[test]
    fn last_turn_delivery_prefers_the_matching_turn() {
        let first = origin_delivery(DeliveryState::Delivered, None);
        let second = OriginDelivery::new(
            JobId::new(Uuid::from_u128(0x118f_0f4a_6b5c_7d8e_9f00_1122_3344_5566)),
            DeliveryState::Failed,
            first.oid().clone(),
            first.origin().to_owned(),
            format!("refs/heads/{}", first.branch()),
            1,
            0,
            Some("ORIGIN_AUTH_FAILED".into()),
            None,
            3,
            4,
        )
        .unwrap();
        let status = closed_record(vec![PublishMode::Fetch], Some(first.clone()))
            .status()
            .clone();
        assert_eq!(
            last_turn_delivery(&status, &[second.clone(), first.clone()])
                .map(OriginDelivery::turn_id),
            Some(first.turn_id())
        );
        assert_eq!(
            last_turn_delivery(&status, std::slice::from_ref(&second)).map(OriginDelivery::state),
            Some(DeliveryState::Failed)
        );
    }

    #[test]
    fn task_detail_and_list_rows_copy_origin_deliveries() {
        let delivery = origin_delivery(DeliveryState::Retrying, Some("ORIGIN_AUTH_FAILED"));
        let record = closed_record(
            vec![PublishMode::Fetch, PublishMode::Push],
            Some(delivery.clone()),
        );
        let detail =
            project_task_detail(&record, record.status(), None, TaskFreshness::Stale).unwrap();
        assert_eq!(
            detail.delivery.as_ref().map(OriginDelivery::state),
            Some(DeliveryState::Retrying)
        );
        assert_eq!(
            detail.deliveries[0].last_error(),
            Some("ORIGIN_AUTH_FAILED")
        );
        assert!(detail.task.publish_push);
        assert_eq!(
            detail.task.delivery.as_ref().map(OriginDelivery::state),
            Some(DeliveryState::Retrying)
        );
    }

    #[test]
    fn status_text_and_json_surface_deliveries() {
        let delivery = origin_delivery(DeliveryState::Failed, Some("ORIGIN_AUTH_FAILED"));
        let report = TaskReport::from_controller(ControllerTaskProjection {
            task_id: TaskId::new(Uuid::from_u128(1)),
            run_id: None,
            status: closed_record(vec![PublishMode::Fetch], Some(delivery.clone()))
                .status()
                .clone(),
            warnings: Vec::new(),
            events: Vec::new(),
            runner: None,
            exit_code: None,
            delivery: Some(delivery.clone()),
            deliveries: vec![delivery.clone()],
            failure_receipt: None,
        });
        let mut text = Vec::new();
        crate::write_task_report(&report, false, &mut text).unwrap();
        let rendered = String::from_utf8(text).unwrap();
        assert!(
            rendered.contains(
                "delivery: failed turn=018f0f4a branch=release-candidate attempt=3 error=ORIGIN_AUTH_FAILED"
            ),
            "{rendered}"
        );

        let mut json_bytes = Vec::new();
        crate::write_task_report(&report, true, &mut json_bytes).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&json_bytes).unwrap();
        assert_eq!(json["protocol_version"], PROTOCOL_VERSION);
        assert_eq!(json["deliveries"][0]["state"], "failed");
        assert_eq!(json["deliveries"][0]["last_error"], "ORIGIN_AUTH_FAILED");
    }

    #[test]
    fn result_text_and_json_surface_the_last_turn_delivery() {
        let delivery = origin_delivery(DeliveryState::Delivered, None);
        let report = TaskResultReport::from_controller(
            TaskId::new(Uuid::from_u128(1)),
            closed_record(vec![PublishMode::Fetch], Some(delivery.clone()))
                .status()
                .clone(),
            "task/1".into(),
            "worker task fetch 1".into(),
            None,
            vec![delivery],
        );
        let mut text = Vec::new();
        crate::write_task_result_report(&report, false, &mut text).unwrap();
        let rendered = String::from_utf8(text).unwrap();
        assert!(
            rendered
                .contains("delivery: delivered turn=018f0f4a branch=release-candidate attempt=3"),
            "{rendered}"
        );
        assert!(!rendered.contains("error="), "{rendered}");

        let mut json_bytes = Vec::new();
        crate::write_task_result_report(&report, true, &mut json_bytes).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&json_bytes).unwrap();
        assert_eq!(json["deliveries"][0]["state"], "delivered");
        assert_eq!(json["delivery"]["state"], "delivered");
    }

    #[test]
    fn list_text_adds_a_push_suffix_and_json_keeps_last_delivery() {
        let delivery = origin_delivery(DeliveryState::Retrying, Some("ORIGIN_AUTH_FAILED"));
        let row = list_row(Some(delivery.clone()), true);
        let report =
            crate::task_client::TaskListReport::from_projection(super::TaskListProjection {
                tasks: vec![row],
                runs: Vec::new(),
                progress: crate::task::RunProgress::from_states([TaskState::Closed]),
                dag_nodes: Vec::new(),
            });
        let mut text = Vec::new();
        crate::write_task_list_report(&report, false, &mut text).unwrap();
        let rendered = String::from_utf8(text).unwrap();
        assert!(rendered.contains("push: retrying"), "{rendered}");

        let mut json_bytes = Vec::new();
        crate::write_task_list_report(&report, true, &mut json_bytes).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&json_bytes).unwrap();
        assert_eq!(json["tasks"][0]["delivery"]["state"], "retrying");
        assert!(json["tasks"][0].get("publish_push").is_none());
    }
}
