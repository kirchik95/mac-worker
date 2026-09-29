//! Strict DTO definitions copied from f59e56a for mixed-version regressions.
#![allow(dead_code)]
use mac_worker::agent;
use mac_worker::agent::{Question, ReportedCheck};
use mac_worker::task::{
    BaseOid, ClosePolicy, HerdrTurnReport, OriginDelivery, RunId, RunnerState, TaskId, TaskOutcome,
    TaskState, TurnId, TurnTerminal,
};
use serde_json::Value;
fn default_true() -> bool {
    true
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenSubmitBody {
    pub task_id: TaskId,
    pub turn_id: TurnId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunId>,
    pub created_at_millis: u64,
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
    pub project_id: String,
    pub worktree_id: String,
    pub base_oid: BaseOid,
    pub timeout_millis: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_turns: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_budget_usd_cents: Option<u64>,
    pub max_followups: u32,
    pub permissions: String,
    pub requires: Vec<String>,
    pub include_untracked: Vec<String>,
    pub include_empty_dirs: Vec<String>,
    pub allow_sensitive: Vec<String>,
    pub cli_includes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// CLI `--no-wait` inverts this. Omitted old bodies default true, preserving
    /// capacity behavior.
    #[serde(default = "default_true")]
    pub wait_for_capacity: bool,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnSummary {
    turn_number: u32,
    turn_id: TurnId,
    terminal: Option<TurnTerminal>,
    outcome: Option<TaskOutcome>,
    agent_committed: Option<bool>,
    log_truncated: bool,
    started_at_millis: Option<u64>,
    ended_at_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    herdr: Option<HerdrTurnReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result_parse_reason: Option<agent::ResultParseReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_identity: Option<agent::AgentIdentity>,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskStatus {
    state: TaskState,
    last_outcome: Option<TaskOutcome>,
    worker: Option<String>,
    session_present: bool,
    head_oid: Option<BaseOid>,
    summary: Option<String>,
    questions: Vec<Question>,
    files_changed: Vec<String>,
    diff_stat: Option<String>,
    turns: Vec<TurnSummary>,
    updated_at_millis: u64,
    #[serde(default)]
    reported_checks: Vec<ReportedCheck>,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerTaskStatusResult {
    task_id: TaskId,
    run_id: Option<RunId>,
    status: TaskStatus,
    #[serde(default)]
    warnings: Vec<String>,
    #[serde(default)]
    events: Vec<Value>,
    runner: Option<RunnerState>,
    exit_code: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivery: Option<OriginDelivery>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    deliveries: Vec<OriginDelivery>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    residual: Option<Vec<String>>,
}
