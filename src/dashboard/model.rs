use std::{collections::BTreeMap, fmt};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{
    Serialize, Serializer,
    ser::{Error as _, SerializeStruct},
};

use crate::{
    agent_facts::{FACTS_TTL, HerdrFacts},
    job::{JobId, LogChunk, LogStream, MAX_LOG_CHUNK_BYTES},
    task::{RunId, RunProgress, RunnerState, TaskId, TurnId},
    task_view::TaskListProjection,
};

pub const DASHBOARD_API_VERSION: u32 = 1;
pub const MAX_ERROR_MESSAGE_CHARS: usize = 512;
pub const MAX_PROJECT_LABEL_CHARS: usize = 96;

const MAX_ERROR_CODE_BYTES: usize = 128;
const FALLBACK_ERROR_CODE: &str = "DASHBOARD_ERROR";
const INVALID_DASHBOARD_API_VERSION_MESSAGE: &str = "dashboard snapshot API version must be 1";
const INVALID_CPU_BUSY_PERCENT_MESSAGE: &str =
    "dashboard CPU busy percent must be finite and between 0 and 100";

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DashboardSnapshot {
    #[serde(serialize_with = "serialize_dashboard_api_version")]
    pub api_version: u32,
    pub revision: u64,
    pub generated_at_millis: u64,
    pub collection: CollectionSummary,
    pub project_defaults: Option<DashboardProjectDefaults>,
    #[serde(flatten)]
    pub task_view: TaskListProjection,
    pub workers: Vec<DashboardWorker>,
    pub queue: Vec<DashboardQueueEntry>,
    pub active_jobs: Vec<DashboardJob>,
    pub recent_jobs: Vec<DashboardJob>,
}

impl TaskListProjection {
    pub fn empty() -> Self {
        Self {
            tasks: Vec::new(),
            runs: Vec::new(),
            progress: RunProgress {
                total: 0,
                queued: 0,
                active: 0,
                open: 0,
                closed: 0,
                failed_like: 0,
            },
            dag_nodes: Vec::new(),
        }
    }
}

fn serialize_dashboard_api_version<S>(api_version: &u32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if *api_version != DASHBOARD_API_VERSION {
        return Err(S::Error::custom(INVALID_DASHBOARD_API_VERSION_MESSAGE));
    }
    api_version.serialize(serializer)
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Freshness {
    Current,
    Stale,
    Offline,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WorkerHealth {
    Ready,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DashboardProjectDefaults {
    pub default_agent: String,
    pub timeout_seconds: u64,
    pub max_followups: u32,
    pub source: String,
    pub publish: Vec<String>,
    pub env_profile: Option<String>,
    pub permissions: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentFactsFreshness {
    Current,
    Stale,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DashboardAgentFacts {
    pub collected_at_millis: u64,
    pub freshness: AgentFactsFreshness,
    pub agents: Vec<DashboardAgent>,
    /// The worker's herdr fact as `{ state, version }`, passed through
    /// unchanged; `null` for records that predate it.  Never a capability.
    pub herdr: Option<HerdrFacts>,
    #[serde(skip)]
    freshness_age_at_observation_millis: u64,
    #[serde(skip)]
    freshness_observed_at_millis: u64,
}

impl DashboardAgentFacts {
    pub(crate) fn from_observation(
        collected_at_millis: u64,
        facts_age_millis: u64,
        observed_at_millis: u64,
        agents: Vec<DashboardAgent>,
        herdr: Option<HerdrFacts>,
    ) -> Self {
        let mut facts = Self {
            collected_at_millis,
            freshness: AgentFactsFreshness::Current,
            agents,
            herdr,
            freshness_age_at_observation_millis: facts_age_millis,
            freshness_observed_at_millis: observed_at_millis,
        };
        facts.refresh_freshness(observed_at_millis);
        facts
    }

    pub(crate) fn refresh_freshness(&mut self, now_millis: u64) {
        let elapsed = now_millis.saturating_sub(self.freshness_observed_at_millis);
        let age = self
            .freshness_age_at_observation_millis
            .saturating_add(elapsed);
        self.freshness = if age > FACTS_TTL {
            AgentFactsFreshness::Stale
        } else {
            AgentFactsFreshness::Current
        };
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DashboardAgent {
    pub name: String,
    pub version: Option<String>,
    pub auth: String,
    pub auth_by_profile: Vec<DashboardProfileAuth>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DashboardProfileAuth {
    pub profile: String,
    pub auth: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DashboardHerdr {
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interactive_agents: Option<u32>,
    /// Whether [`age_millis`] has passed [`FACTS_TTL`]. Never a capability:
    /// scheduling still ignores a stale fact.
    pub stale: bool,
    pub age_millis: u64,
    #[serde(skip)]
    age_at_observation_millis: u64,
    #[serde(skip)]
    observed_at_millis: u64,
}

impl DashboardHerdr {
    pub(crate) fn from_observation(
        state: String,
        version: Option<String>,
        interactive_agents: Option<u32>,
        facts_age_millis: u64,
        observed_at_millis: u64,
    ) -> Self {
        let mut herdr = Self {
            state,
            version,
            interactive_agents,
            stale: false,
            age_millis: facts_age_millis,
            age_at_observation_millis: facts_age_millis,
            observed_at_millis,
        };
        herdr.refresh_age(observed_at_millis);
        herdr
    }

    pub(crate) fn refresh_age(&mut self, now_millis: u64) {
        let elapsed = now_millis.saturating_sub(self.observed_at_millis);
        self.age_millis = self.age_at_observation_millis.saturating_add(elapsed);
        self.stale = self.age_millis > FACTS_TTL;
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DashboardWorker {
    pub name: String,
    pub health: WorkerHealth,
    pub freshness: Freshness,
    pub observed_at_millis: Option<u64>,
    pub hostname: Option<String>,
    pub agent_facts: Option<DashboardAgentFacts>,
    /// Herdr fact for the worker card chip, including a stale fact and its
    /// age; `null` only when there is no fact at all. Never a capability.
    pub herdr: Option<DashboardHerdr>,
    pub slot: SlotSummary,
    pub capabilities: Vec<String>,
    pub missing_capabilities: Vec<String>,
    pub system: SystemSummary,
    pub error: Option<DashboardError>,
    pub active_task: Option<DashboardActiveTask>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DashboardActiveTask {
    pub task_id: TaskId,
    pub title: String,
    pub agent: String,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub turn_number: u32,
    pub started_at_millis: Option<u64>,
    pub runner: Option<RunnerState>,
}

impl Serialize for DashboardActiveTask {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut record = serializer.serialize_struct("DashboardActiveTask", 8)?;
        record.serialize_field("task_id", &self.task_id)?;
        record.serialize_field(
            "title",
            &sanitize_bounded(&self.title, MAX_PROJECT_LABEL_CHARS),
        )?;
        record.serialize_field(
            "agent",
            &sanitize_bounded(&self.agent, MAX_PROJECT_LABEL_CHARS),
        )?;
        record.serialize_field(
            "model",
            &self
                .model
                .as_deref()
                .map(|model| sanitize_bounded(model, 256)),
        )?;
        record.serialize_field(
            "effort",
            &self
                .effort
                .as_deref()
                .map(|effort| sanitize_bounded(effort, 128)),
        )?;
        record.serialize_field("turn_number", &self.turn_number)?;
        record.serialize_field("started_at_millis", &self.started_at_millis)?;
        record.serialize_field("runner", &self.runner)?;
        record.end()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DashboardJob {
    pub job_id: JobId,
    pub worker_name: String,
    pub project_id: String,
    pub worktree_id: String,
    pub project_label: Option<String>,
    pub manifest_digest: String,
    pub command_summary: DashboardCommandSummary,
    pub resource_class: String,
    pub created_at_millis: u64,
    pub updated_at_millis: u64,
    pub state: DashboardJobState,
    pub exit_code: Option<u8>,
    pub terminating_signal: Option<u32>,
    pub final_stdout_bytes: Option<u64>,
    pub final_stderr_bytes: Option<u64>,
    pub artifact_status: Option<ArtifactStatus>,
    pub remote_uncertainty: Option<String>,
}

impl Serialize for DashboardJob {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut record = serializer.serialize_struct("DashboardJob", 17)?;
        record.serialize_field("job_id", &self.job_id)?;
        record.serialize_field("worker_name", &self.worker_name)?;
        record.serialize_field("project_id", &self.project_id)?;
        record.serialize_field("worktree_id", &self.worktree_id)?;
        record.serialize_field(
            "project_label",
            &self
                .project_label
                .as_deref()
                .map(|label| sanitize_bounded(label, MAX_PROJECT_LABEL_CHARS)),
        )?;
        record.serialize_field("manifest_digest", &self.manifest_digest)?;
        record.serialize_field("command_summary", &self.command_summary)?;
        record.serialize_field("resource_class", &self.resource_class)?;
        record.serialize_field("created_at_millis", &self.created_at_millis)?;
        record.serialize_field("updated_at_millis", &self.updated_at_millis)?;
        record.serialize_field("state", &self.state)?;
        record.serialize_field("exit_code", &self.exit_code)?;
        record.serialize_field("terminating_signal", &self.terminating_signal)?;
        record.serialize_field("final_stdout_bytes", &self.final_stdout_bytes)?;
        record.serialize_field("final_stderr_bytes", &self.final_stderr_bytes)?;
        record.serialize_field("artifact_status", &self.artifact_status)?;
        record.serialize_field("remote_uncertainty", &self.remote_uncertainty)?;
        record.end()
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SlotSummary {
    pub state: DashboardSlotState,
    pub capacity: u8,
    pub active_job_id: Option<JobId>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SystemSummary {
    pub free_disk_bytes: Option<u64>,
    pub total_disk_bytes: Option<u64>,
    pub memory_pressure: Option<DashboardMemoryPressure>,
    pub swap_used_bytes: Option<u64>,
    #[serde(serialize_with = "serialize_optional_cpu_busy_percent")]
    pub cpu_busy_percent: Option<f64>,
}

fn serialize_optional_cpu_busy_percent<S>(
    cpu_busy_percent: &Option<f64>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if cpu_busy_percent.is_some_and(|value| !value.is_finite() || !(0.0..=100.0).contains(&value)) {
        return Err(S::Error::custom(INVALID_CPU_BUSY_PERCENT_MESSAGE));
    }
    cpu_busy_percent.serialize(serializer)
}

#[derive(Debug, Clone, PartialEq)]
pub struct DashboardQueueEntry {
    pub position: u32,
    pub job_id: JobId,
    pub entry_kind: DashboardQueueEntryKind,
    pub task_id: Option<TaskId>,
    pub turn_id: Option<TurnId>,
    pub run_id: Option<RunId>,
    pub run_max_parallel: Option<u32>,
    pub pinned_worker: Option<String>,
    pub project_id: String,
    pub worktree_id: String,
    pub project_label: Option<String>,
    pub command_summary: DashboardCommandSummary,
    pub created_at_millis: u64,
    pub requirements: Vec<String>,
    pub blocking_code: String,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DashboardQueueEntryKind {
    Batch,
    TaskTurn,
}

impl Serialize for DashboardQueueEntry {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut record = serializer.serialize_struct("DashboardQueueEntry", 15)?;
        record.serialize_field("position", &self.position)?;
        record.serialize_field("job_id", &self.job_id)?;
        record.serialize_field("entry_kind", &self.entry_kind)?;
        record.serialize_field("task_id", &self.task_id)?;
        record.serialize_field("turn_id", &self.turn_id)?;
        record.serialize_field("run_id", &self.run_id)?;
        record.serialize_field("run_max_parallel", &self.run_max_parallel)?;
        record.serialize_field(
            "pinned_worker",
            &self
                .pinned_worker
                .as_deref()
                .map(|worker| sanitize_bounded(worker, MAX_PROJECT_LABEL_CHARS)),
        )?;
        record.serialize_field("project_id", &self.project_id)?;
        record.serialize_field("worktree_id", &self.worktree_id)?;
        record.serialize_field(
            "project_label",
            &self
                .project_label
                .as_deref()
                .map(|label| sanitize_bounded(label, MAX_PROJECT_LABEL_CHARS)),
        )?;
        record.serialize_field("command_summary", &self.command_summary)?;
        record.serialize_field("created_at_millis", &self.created_at_millis)?;
        record.serialize_field(
            "requirements",
            &self
                .requirements
                .iter()
                .map(|requirement| sanitize_bounded(requirement, MAX_PROJECT_LABEL_CHARS))
                .collect::<Vec<_>>(),
        )?;
        record.serialize_field("blocking_code", &sanitize_error_code(&self.blocking_code))?;
        record.end()
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CollectionSummary {
    pub freshness: Freshness,
    pub errors: Vec<DashboardError>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DashboardJobState {
    Uploading,
    Verified,
    Accepted,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    Lost,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DashboardSlotState {
    Idle,
    Busy,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DashboardMemoryPressure {
    Normal,
    Warn,
    Critical,
    Unknown,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DashboardCommandMode {
    Argv,
    Shell,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DashboardCommandSummary {
    pub mode: DashboardCommandMode,
    pub arg_count: Option<u16>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactStatus {
    Pending,
    Available,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DashboardError {
    pub code: String,
    pub message: String,
}

impl DashboardError {
    pub fn new(code: impl Into<String>, message: impl AsRef<str>) -> Self {
        Self {
            code: sanitize_error_code(&code.into()),
            message: sanitize_bounded(message.as_ref(), MAX_ERROR_MESSAGE_CHARS),
        }
    }
}

impl fmt::Display for DashboardError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for DashboardError {}

impl Serialize for DashboardError {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut record = serializer.serialize_struct("DashboardError", 2)?;
        record.serialize_field("code", &self.code)?;
        record.serialize_field(
            "message",
            &sanitize_bounded(&self.message, MAX_ERROR_MESSAGE_CHARS),
        )?;
        record.end()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DashboardLogChunk {
    stream: LogStream,
    offset: u64,
    next_offset: u64,
    data: String,
}

impl DashboardLogChunk {
    pub fn from_log_chunk(chunk: &LogChunk) -> Result<Self, DashboardError> {
        chunk
            .validate()
            .map_err(|_| DashboardError::new("INVALID_LOG_CHUNK", "log chunk is invalid"))?;
        Ok(Self {
            stream: chunk.stream(),
            offset: chunk.offset(),
            next_offset: chunk.next_offset(),
            data: chunk.data().to_owned(),
        })
    }

    pub fn from_bytes(
        stream: LogStream,
        offset: u64,
        bytes: Vec<u8>,
    ) -> Result<Self, DashboardError> {
        if bytes.len() > MAX_LOG_CHUNK_BYTES {
            return Err(DashboardError::new(
                "LOG_CHUNK_TOO_LARGE",
                "log chunk exceeds its byte limit",
            ));
        }
        let byte_count = u64::try_from(bytes.len()).expect("usize fits in u64");
        let next_offset = offset.checked_add(byte_count).ok_or_else(|| {
            DashboardError::new("LOG_OFFSET_OVERFLOW", "log chunk offset exceeds u64")
        })?;
        Ok(Self {
            stream,
            offset,
            next_offset,
            data: STANDARD.encode(bytes),
        })
    }

    pub fn stream(&self) -> LogStream {
        self.stream
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn data(&self) -> &str {
        &self.data
    }
}

impl Serialize for DashboardLogChunk {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut record = serializer.serialize_struct("DashboardLogChunk", 4)?;
        record.serialize_field("stream", &self.stream)?;
        record.serialize_field("offset", &self.offset)?;
        record.serialize_field("next_offset", &self.next_offset)?;
        record.serialize_field("data", &self.data)?;
        record.end()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub code: String,
    pub message: String,
}

impl ApiError {
    pub fn new(code: impl Into<String>, message: impl AsRef<str>) -> Self {
        Self {
            code: sanitize_error_code(&code.into()),
            message: sanitize_bounded(message.as_ref(), MAX_ERROR_MESSAGE_CHARS),
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ApiError {}

impl Serialize for ApiError {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut record = serializer.serialize_struct("ApiError", 1)?;
        record.serialize_field(
            "error",
            &DashboardError::new(self.code.clone(), &self.message),
        )?;
        record.end()
    }
}

pub fn short_identifier(identifier: &str) -> String {
    identifier.chars().take(12).collect()
}

pub fn project_label_or_fallback(job: &DashboardJob) -> String {
    job.project_label
        .as_deref()
        .map(|label| sanitize_bounded(label, MAX_PROJECT_LABEL_CHARS))
        .unwrap_or_else(|| {
            format!(
                "project-{}/worktree-{}",
                short_identifier(&job.project_id),
                short_identifier(&job.worktree_id)
            )
        })
}

pub fn sanitize_bounded(value: &str, limit: usize) -> String {
    let mut tokens = Vec::new();
    let mut used = 0;
    let mut truncated = false;

    for character in value.chars() {
        let escaped = match character {
            '\n' => "\\n".to_owned(),
            '\r' => "\\r".to_owned(),
            '\t' => "\\t".to_owned(),
            character if character.is_control() => character.escape_unicode().to_string(),
            character => character.to_string(),
        };
        let escaped_length = escaped.chars().count();
        if used + escaped_length > limit {
            truncated = true;
            break;
        }
        tokens.push(escaped);
        used += escaped_length;
    }

    if truncated {
        if limit == 0 {
            return String::new();
        }
        while used + 1 > limit {
            let removed = tokens
                .pop()
                .expect("a full output limit always contains at least one token");
            used -= removed.chars().count();
        }
        tokens.push("…".to_owned());
    }
    tokens.concat()
}

fn sanitize_error_code(code: &str) -> String {
    if code.len() <= MAX_ERROR_CODE_BYTES
        && code.starts_with(|character: char| character.is_ascii_uppercase())
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        code.to_owned()
    } else {
        FALLBACK_ERROR_CODE.to_owned()
    }
}
