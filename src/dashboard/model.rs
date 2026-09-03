use std::fmt;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{
    Serialize, Serializer,
    ser::{Error as _, SerializeStruct},
};

use crate::job::{JobId, LogChunk, LogStream, MAX_LOG_CHUNK_BYTES};

pub const DASHBOARD_API_VERSION: u32 = 1;
pub const MAX_ERROR_MESSAGE_CHARS: usize = 512;
pub const MAX_PROJECT_LABEL_CHARS: usize = 96;

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
    pub workers: Vec<DashboardWorker>,
    pub queue: Vec<DashboardQueueEntry>,
    pub active_jobs: Vec<DashboardJob>,
    pub recent_jobs: Vec<DashboardJob>,
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

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DashboardWorker {
    pub name: String,
    pub health: WorkerHealth,
    pub freshness: Freshness,
    pub observed_at_millis: Option<u64>,
    pub hostname: Option<String>,
    pub slot: SlotSummary,
    pub capabilities: Vec<String>,
    pub missing_capabilities: Vec<String>,
    pub system: SystemSummary,
    pub error: Option<DashboardError>,
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
    pub project_id: String,
    pub worktree_id: String,
    pub project_label: Option<String>,
    pub command_summary: DashboardCommandSummary,
    pub created_at_millis: u64,
    pub requirements: Vec<String>,
    pub blocking_code: String,
}

impl Serialize for DashboardQueueEntry {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut record = serializer.serialize_struct("DashboardQueueEntry", 9)?;
        record.serialize_field("position", &self.position)?;
        record.serialize_field("job_id", &self.job_id)?;
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
        record.serialize_field("requirements", &self.requirements)?;
        record.serialize_field("blocking_code", &self.blocking_code)?;
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
            code: code.into(),
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
            code: code.into(),
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
