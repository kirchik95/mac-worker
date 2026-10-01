//! Wire and component contracts. No filesystem or transport side effects.

/// All first-wave bounds live here; the RPC frame cap is the existing cap.
pub const SCHEMA_VERSION: u32 = 1;
pub const MAX_EVENT_BYTES: usize = 1_024;
pub const MAX_BATCH_EVENTS: usize = 32;
pub const MAX_BATCH_BYTES: usize = 32 * 1_024;
pub const MAX_RETAINED_SEGMENTS: usize = 64;
pub const MAX_SEGMENT_BYTES: usize = 256 * 1_024;
#[cfg(any(test, feature = "test-support"))]
pub const MAX_RETAINED_BYTES: usize = MAX_RETAINED_SEGMENTS * MAX_SEGMENT_BYTES;
pub const MAX_METADATA_BYTES: usize = 64 * 1_024;
pub const MAX_RECOVERY_EVIDENCE_BYTES: usize = 512 * 1_024;
pub const MAX_RECOVERY_EVIDENCE_FILES: usize = 32;
pub const MAX_JOURNAL_BYTES: usize = 18 * 1_024 * 1_024;
pub const MAX_JOURNAL_FILES: usize = 128;
pub const PUBLISHER_CAPACITY: usize = 128;
pub const MAX_PUBLISHER_BYTES: usize = PUBLISHER_CAPACITY * MAX_BATCH_BYTES;
pub const JOURNAL_ADMISSION_BUDGET: Duration = Duration::from_millis(50);
/// Maximum exit wait for in-flight and queued appends; never a disk I/O deadline.
/// Raised after the live incident on 2026-10-01 to let slower F_FULLFSYNC appends finish.
pub const PUBLISHER_EXIT_GRACE: Duration = Duration::from_secs(3);
pub const JOURNAL_CHECK_INTERVAL: Duration = Duration::from_millis(200);
pub const MAX_STALE_RETRIES: usize = 3;
pub use crate::controller::protocol::MAX_FRAME_BYTES;
pub const RPC_BUDGET: Duration = Duration::from_secs(30);
pub const READ_DEFAULT_LIMIT: usize = 128;
pub const READ_MAX_LIMIT: usize = 256;
pub const READ_MAX_WAIT_MS: u64 = 20_000;
pub const READ_FOLLOW_WAIT_MS: u64 = 15_000;
pub const ADDRESSED_MAX_TASKS: usize = 16;
pub const MAX_TASK_FACT_BYTES: usize = 2 * 1_024;
pub const MAX_DISPLAY_TITLE_BYTES: usize = 512;
pub const MAX_DISPATCH_ASSOCIATIONS: usize = 32;
pub const MAX_STATE_RECORD_BYTES: usize = 1_024 * 1_024;
pub const REPAIR_DEFAULT_LIMIT: usize = 64;
pub const REPAIR_MAX_LIMIT: usize = 128;
pub const REPAIR_MAX_INPUT_BYTES: usize = 8 * 1_024 * 1_024;
pub const REPAIR_WORK_BUDGET: Duration = Duration::from_millis(50);
pub const MAX_OPAQUE_CURSOR_BYTES: usize = 2 * 1_024;
pub const REPAIR_MAX_DIRECTORY_ENTRIES: usize = 100_000;
pub const REPAIR_INTERVAL: Duration = Duration::from_secs(15);
pub const SSE_CAPACITY: usize = 256;
pub const SSE_MAX_STREAMS: usize = 8;
pub const SSE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
pub const REFRESH_DEBOUNCE: Duration = Duration::from_millis(100);
#[cfg(any(test, feature = "test-support"))]
pub const TUNNEL_TIMEOUT: Duration = Duration::from_secs(30);
pub const RECONNECT_BACKOFF: [Duration; 4] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(5),
];
pub const NOTIFY_DECISION_CAPACITY: usize = 4_096;
pub const NOTIFY_PENDING_CAPACITY: usize = 256;
pub const MAX_RECONCILIATION_ROWS: usize = 256;
pub const NOTIFY_COALESCE_AFTER: Duration = Duration::from_secs(60);
pub const NOTIFY_COALESCE_COUNT: usize = 5;
pub const NOTICE_CHANNEL_BUDGET: Duration = Duration::from_secs(2);
pub const MAX_NOTIFY_STATE_BYTES: usize = MAX_FRAME_BYTES;
pub const CONTROLLER_EVENTS_INVALID: &str = "CONTROLLER_EVENTS_INVALID";
pub const CONTROLLER_EVENTS_UNAVAILABLE: &str = "CONTROLLER_EVENTS_UNAVAILABLE";
pub const CONTROLLER_EVENTS_UNSUPPORTED: &str = "CONTROLLER_EVENTS_UNSUPPORTED";
pub const CONTROLLER_EVENTS_CANCELLED: &str = "CONTROLLER_EVENTS_CANCELLED";
pub const CONTROLLER_EVENTS_REPAIR_REGISTRY_TOO_LARGE: &str =
    "CONTROLLER_EVENTS_REPAIR_REGISTRY_TOO_LARGE";

pub(crate) fn invalid(message: &str) -> WorkerError {
    WorkerError::Protocol(format!("{CONTROLLER_EVENTS_INVALID}: {message}"))
}
pub(crate) fn unavailable(message: &str) -> WorkerError {
    WorkerError::Unavailable(format!("{CONTROLLER_EVENTS_UNAVAILABLE}: {message}"))
}

use crate::{
    error::WorkerError,
    task::{RunId, TaskId, TurnId},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::{collections::BTreeMap, fmt, str::FromStr, sync::Arc, time::Duration};

/// Canonical decimal u64 on the wire; never a JSON number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seq(u64);

impl Seq {
    pub const ZERO: Self = Self(0);
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
    pub const fn as_u64(self) -> u64 {
        self.0
    }
    pub fn checked_increment(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl fmt::Display for Seq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for Seq {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.len() > 20
            || !value.bytes().all(|b| b.is_ascii_digit())
            || (value.len() > 1 && value.starts_with('0'))
        {
            return Err("sequence must be canonical decimal u64 text".into());
        }
        value
            .parse()
            .map(Self)
            .map_err(|_| "invalid sequence".into())
    }
}

impl Serialize for Seq {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}
impl<'de> Deserialize<'de> for Seq {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d)?.parse().map_err(de::Error::custom)
    }
}

/// Journal UUID and publication position. Ordering across different UUIDs
/// is deterministic for containers only; it never establishes causality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EventCursor {
    #[serde(with = "uuid_wire")]
    pub journal_id: uuid::Uuid,
    pub seq: Seq,
}

/// Journal read arguments. Defaults: limit 128, wait 0; normalize to 1..256 / 20 s.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadQuery {
    #[serde(default, deserialize_with = "deserialize_request_cursor")]
    pub after: Option<EventCursor>,
    #[serde(default = "default_read_limit")]
    pub limit: usize,
    #[serde(default)]
    pub wait_ms: u64,
}

fn default_read_limit() -> usize {
    READ_DEFAULT_LIMIT
}

/// Exclusive task.list controller_events selector, discriminated by snake_case op.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum EventSelector {
    Read(ReadQuery),
    Tasks(TaskAddressQuery),
    Repair(TaskRepairQuery),
}

impl EventSelector {
    pub fn request_body(&self) -> Result<serde_json::Value, WorkerError> {
        if let Self::Tasks(query) = self {
            query.validate()?;
        }
        let selector =
            serde_json::to_value(self).map_err(|_| invalid("selector encoding failed"))?;
        Ok(serde_json::json!({"controller_events": selector}))
    }

    /// Strict request grammar; a selector cannot be combined with old filters.
    pub fn from_request_body(body: &serde_json::Value) -> Result<Self, WorkerError> {
        let object = body
            .as_object()
            .ok_or_else(|| invalid("selector body is not an object"))?;
        if object.len() != 1 {
            return Err(invalid("selector cannot be combined with filters"));
        }
        if let Some(selector) = object
            .get("controller_events")
            .and_then(serde_json::Value::as_object)
        {
            for key in ["after", "baseline_after"] {
                if let Some(cursor) = selector.get(key).and_then(serde_json::Value::as_object)
                    && (cursor.len() != 2
                        || !cursor.contains_key("journal_id")
                        || !cursor.contains_key("seq"))
                {
                    return Err(invalid("invalid request cursor keys"));
                }
            }
        }
        let selector: Self = serde_json::from_value(
            object
                .get("controller_events")
                .cloned()
                .ok_or_else(|| invalid("missing controller_events"))?,
        )
        .map_err(|_| invalid("invalid selector grammar"))?;
        if let Self::Tasks(query) = &selector {
            query.validate()?;
        }
        Ok(selector)
    }
}

impl ReadQuery {
    pub fn normalized(mut self) -> Self {
        self.limit = self.limit.clamp(1, READ_MAX_LIMIT);
        self.wait_ms = self.wait_ms.min(READ_MAX_WAIT_MS);
        self
    }
    #[cfg(test)]
    pub(crate) fn follow(after: EventCursor) -> Self {
        Self {
            after: Some(after),
            limit: READ_DEFAULT_LIMIT,
            wait_ms: READ_FOLLOW_WAIT_MS,
        }
    }
}
impl Default for ReadQuery {
    fn default() -> Self {
        Self {
            after: None,
            limit: READ_DEFAULT_LIMIT,
            wait_ms: 0,
        }
    }
}

/// Private base64url-without-padding token. Consumers never decode positions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct OpaqueCursor(String);
impl OpaqueCursor {
    pub fn parse(value: impl Into<String>) -> Result<Self, WorkerError> {
        use base64::Engine;
        let value = value.into();
        if value.is_empty() || value.len() > MAX_OPAQUE_CURSOR_BYTES {
            return Err(invalid("opaque cursor exceeds its encoding bound"));
        }
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&value)
            .map_err(|_| invalid("opaque cursor is not base64url"))?;
        Ok(Self(value))
    }
    #[cfg(any(test, feature = "test-support"))]
    pub fn encode<T: Serialize>(value: &T) -> Result<Self, WorkerError> {
        use base64::Engine;
        let bytes = serde_json::to_vec(value).map_err(|_| invalid("cursor encoding failed"))?;
        Self::parse(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for OpaqueCursor {
    type Error = WorkerError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}
impl From<OpaqueCursor> for String {
    fn from(value: OpaqueCursor) -> Self {
        value.0
    }
}

/// One to sixteen distinct task IDs; addressed proof continuation is independent of repair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAddressQuery {
    pub task_ids: Vec<TaskId>,
    #[serde(default)]
    pub include_titles: bool,
    #[serde(default)]
    pub proof_after: Option<OpaqueCursor>,
}
impl TaskAddressQuery {
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.task_ids.is_empty() || self.task_ids.len() > ADDRESSED_MAX_TASKS {
            return Err(invalid("addressed task count must be 1..16"));
        }
        for (index, id) in self.task_ids.iter().enumerate() {
            if self.task_ids[..index].contains(id) {
                return Err(invalid("duplicate task ID"));
            }
        }
        Ok(())
    }
    pub fn try_new(
        task_ids: Vec<TaskId>,
        include_titles: bool,
        proof_after: Option<OpaqueCursor>,
    ) -> Result<Self, WorkerError> {
        let query = Self {
            task_ids,
            include_titles,
            proof_after,
        };
        query.validate()?;
        Ok(query)
    }
}
/// Task-only key page. Preserve the pre-enumeration baseline H across every page; titles excluded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskRepairQuery {
    #[serde(default)]
    pub after: Option<OpaqueCursor>,
    #[serde(default = "default_repair_limit")]
    pub limit: usize,
    #[serde(default, deserialize_with = "deserialize_request_cursor")]
    pub baseline_after: Option<EventCursor>,
}
fn default_repair_limit() -> usize {
    REPAIR_DEFAULT_LIMIT
}
impl TaskRepairQuery {
    pub fn normalized(mut self) -> Self {
        self.limit = self.limit.clamp(1, REPAIR_MAX_LIMIT);
        self
    }
}
impl Default for TaskRepairQuery {
    fn default() -> Self {
        Self {
            after: None,
            limit: REPAIR_DEFAULT_LIMIT,
            baseline_after: None,
        }
    }
}

/// Committed manifest window. Empty journals have oldest=1 and head=0.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JournalWindow {
    #[serde(with = "uuid_wire")]
    pub journal_id: uuid::Uuid,
    pub oldest_seq: Seq,
    pub head_seq: Seq,
}
impl JournalWindow {
    pub fn cursor(&self) -> EventCursor {
        EventCursor {
            journal_id: self.journal_id,
            seq: self.head_seq,
        }
    }
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.journal_id.is_nil()
            || self.oldest_seq == Seq::ZERO
            || self.oldest_seq.as_u64().saturating_sub(1) > self.head_seq.as_u64()
        {
            return Err(invalid("invalid journal window"));
        }
        Ok(())
    }
}
/// Tolerant envelope. Unknown schema/kinds only invalidate globally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WireEvent {
    pub schema_version: u32,
    #[serde(with = "uuid_wire")]
    pub journal_id: uuid::Uuid,
    pub seq: Seq,
    pub time_millis: u64,
    pub kind: String,
    pub data: serde_json::Value,
}
impl WireEvent {
    pub fn encoded_len(&self) -> Result<usize, WorkerError> {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len() + 1)
            .map_err(|_| invalid("event encoding failed"))
    }
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.schema_version == 0
            || self.journal_id.is_nil()
            || self.seq == Seq::ZERO
            || self.kind.is_empty()
            || self.kind.len() > 64
            || !self
                .kind
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_'))
            || !self.data.is_object()
        {
            return Err(invalid("invalid event envelope"));
        }
        if self.encoded_len()? > MAX_EVENT_BYTES {
            return Err(invalid("event exceeds its byte bound"));
        }
        self.known_payload()?;
        Ok(())
    }
    /// Unknown kinds/versions return None even if raw data contains task_id.
    pub fn affected_task(&self) -> Option<TaskId> {
        self.validate().ok()?;
        self.known_payload()
            .ok()??
            .get("task_id")?
            .as_str()?
            .parse()
            .ok()
    }
    /// Safe debugging JSON: unknown data and additive private fields are
    /// excluded. Consumers must never print raw unknown event data.
    pub fn debug_value(&self) -> Result<serde_json::Value, WorkerError> {
        self.validate()?;
        let mut value = serde_json::json!({"schema_version":self.schema_version,"journal_id":self.journal_id.to_string(),"seq":self.seq,"time_millis":self.time_millis,"kind":self.kind});
        if let Some(data) = self.known_payload()? {
            value["data"] = data;
        }
        Ok(value)
    }
    fn known_payload(&self) -> Result<Option<serde_json::Value>, WorkerError> {
        if self.schema_version != SCHEMA_VERSION {
            return Ok(None);
        }
        let data = self.data.clone();
        let safe = match self.kind.as_str() {
            "task.created" | "task.changed" | "task.removed" | "task.closed" | "task.abandoned" => {
                let hint: TaskHint =
                    serde_json::from_value(data).map_err(|_| invalid("invalid task hint"))?;
                hint.validate()?;
                serde_json::to_value(hint)
            }
            "turn.started" => serde_json::to_value(
                serde_json::from_value::<AcceptedHint>(data)
                    .map_err(|_| invalid("invalid accepted hint"))?,
            ),
            "turn.finished" | "turn.outcome_changed" => serde_json::to_value(
                serde_json::from_value::<TurnHint>(data)
                    .map_err(|_| invalid("invalid terminal hint"))?,
            ),
            "queue.changed" => {
                let hint: QueueHint =
                    serde_json::from_value(data).map_err(|_| invalid("invalid queue hint"))?;
                hint.validate()?;
                serde_json::to_value(hint)
            }
            "task.auto_continue_scheduled" => serde_json::to_value(
                serde_json::from_value::<AutoContinueData>(data)
                    .map_err(|_| invalid("invalid continuation hint"))?,
            ),
            "run.changed" => serde_json::to_value(
                serde_json::from_value::<RunChangedData>(data)
                    .map_err(|_| invalid("invalid run hint"))?,
            ),
            "dag.child_admitted" => serde_json::to_value(
                serde_json::from_value::<DagChildData>(data)
                    .map_err(|_| invalid("invalid DAG hint"))?,
            ),
            "worker.changed" => serde_json::to_value(
                serde_json::from_value::<WorkerChangedData>(data)
                    .map_err(|_| invalid("invalid worker hint"))?,
            ),
            "controller.drained" | "controller.undrained" => {
                let hint: DrainData =
                    serde_json::from_value(data).map_err(|_| invalid("invalid drain hint"))?;
                if hint.drained != (self.kind == "controller.drained") {
                    return Err(invalid("drain hint kind contradicts state"));
                }
                serde_json::to_value(hint)
            }
            _ => return Ok(None),
        }
        .map_err(|_| invalid("safe hint encoding failed"))?;
        Ok(Some(safe))
    }
}

/// Measure the complete existing read envelope, not just its result payload.
pub fn ensure_frame_bound<T: Serialize>(reply: &T) -> Result<(), WorkerError> {
    let length = serde_json::to_vec(reply)
        .map_err(|_| invalid("reply encoding failed"))?
        .len();
    if length >= MAX_FRAME_BYTES {
        return Err(invalid("reply frame exceeds its byte bound"));
    }
    Ok(())
}
/// Committed records with last-delivered next_after; empty timeout preserves the input cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReadBatch {
    pub schema_version: u32,
    #[serde(with = "uuid_wire")]
    pub journal_id: uuid::Uuid,
    pub oldest_seq: Seq,
    pub head_seq: Seq,
    pub next_after: EventCursor,
    pub events: Vec<WireEvent>,
    pub has_more: bool,
}
/// Stable repair reason and current safe window; this control invents no journal records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SnapshotRequired {
    pub reason: String,
    pub window: JournalWindow,
}
/// Tolerant selector reply discriminated by type: batch or snapshot_required.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventReadResult {
    Batch(ReadBatch),
    SnapshotRequired(SnapshotRequired),
}

/// Safe outcome catalog; failure prose is never part of this enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SafeOutcome {
    Done,
    NeedsInput,
    Blocked,
    Unknown,
    Failed,
    Cancelled,
    TimedOut,
    Lost,
}
impl From<&crate::task::TaskOutcome> for SafeOutcome {
    fn from(value: &crate::task::TaskOutcome) -> Self {
        use crate::task::TaskOutcome;
        match value {
            TaskOutcome::Done => Self::Done,
            TaskOutcome::NeedsInput => Self::NeedsInput,
            TaskOutcome::Blocked => Self::Blocked,
            TaskOutcome::Unknown => Self::Unknown,
            TaskOutcome::Failed { .. } => Self::Failed,
            TaskOutcome::Cancelled => Self::Cancelled,
            TaskOutcome::TimedOut => Self::TimedOut,
            TaskOutcome::Lost => Self::Lost,
        }
    }
}
/// Closed public-code catalog; unknown values collapse to TURN_FAILED.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
pub struct SafeCode(String);
impl SafeCode {
    /// Adding a code requires a serial contract update. Shape-only uppercase
    /// validation would permit secrets masquerading as codes.
    pub fn from_public_code(value: &str) -> Self {
        let code = match value {
            "TURN_FAILED"
            | "PUBLISH_FAILED"
            | "RESULT_FETCH_FAILED"
            | "RESULT_UNPARSEABLE"
            | "LOG_DRAIN_UNAVAILABLE"
            | "LOG_CHECKPOINT_INVALID"
            | "BASE_PUSH_FAILED"
            | "BASE_UNAVAILABLE"
            | "CANCELLED"
            | "CANCELLED_PRELAUNCH"
            | "TIMED_OUT"
            | "LOST"
            | "CAPACITY_BUSY"
            | "WAITING_FOR_DISPATCH"
            | "PINNED_WORKER_BUSY"
            | "CAPABILITY_MISSING"
            | "RUN_MAX_PARALLEL"
            | "NO_COMPATIBLE_IDLE_WORKER"
            | "RUNNER_UNVERIFIABLE"
            | "AUTO_CONTINUE_FAILED"
            | "TASK_BUSY"
            | "TASK_NOT_FOUND"
            | "TASK_INCONSISTENT"
            | "HOST_UNAVAILABLE"
            | "HOST_IO"
            | "HOST_LAYOUT_OUTDATED"
            | "PROJECT_MISMATCH"
            | "LEASE_IDENTITY_MISMATCH"
            | "AGENT_EXITED"
            | "AGENT_UNSUPPORTED"
            | "AGENT_LIMIT_REACHED"
            | "QUEUE_WORKER_INVALID"
            | "ADMISSION_UNAVAILABLE"
            | "WORKER_UNAVAILABLE"
            | "WORKER_BUSY"
            | "PROTOCOL"
            | "IO" => value,
            _ => "TURN_FAILED",
        };
        Self(code.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl From<String> for SafeCode {
    fn from(value: String) -> Self {
        Self::from_public_code(&value)
    }
}
impl From<SafeCode> for String {
    fn from(value: SafeCode) -> Self {
        value.0
    }
}
/// Same grammar and 128-byte bound as existing queue worker names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct WorkerName(String);
impl WorkerName {
    pub fn parse(value: impl Into<String>) -> Result<Self, WorkerError> {
        let value = value.into();
        if value.len() > 128 || !crate::config::valid_identifier(&value) {
            return Err(invalid("invalid worker name"));
        }
        Ok(Self(value))
    }
    #[cfg(test)]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for WorkerName {
    type Error = WorkerError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}
impl From<WorkerName> for String {
    fn from(value: WorkerName) -> Self {
        value.0
    }
}
/// Saved task identifiers/state/code only. Constructor and decoder reject noncatalog states.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskHint {
    pub task_id: TaskId,
    pub run_id: Option<RunId>,
    pub turn_id: Option<TurnId>,
    pub state: String,
    pub code: Option<SafeCode>,
}
/// Saved terminal outcome hint; does not prove retirement, import completion or eligibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnHint {
    pub task_id: TaskId,
    pub turn_id: TurnId,
    pub run_id: Option<RunId>,
    pub outcome: SafeOutcome,
    pub code: Option<SafeCode>,
}
/// Host acceptance hint, captured only after accepted status is saved, never prepared Active.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptedHint {
    pub task_id: TaskId,
    pub turn_id: TurnId,
    pub run_id: Option<RunId>,
    pub worker: WorkerName,
}
/// Saved waiting/dispatching/parked hint; optional fields None mean global invalidation. No owners/affinities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueueHint {
    pub turn_id: Option<TurnId>,
    pub state: Option<String>,
    pub kind: Option<String>,
    pub code: Option<SafeCode>,
}
fn known_task_state(state: &str) -> bool {
    matches!(
        state,
        "queued" | "active" | "open" | "closed" | "abandoned" | "lost"
    )
}
impl TaskHint {
    pub fn validate(&self) -> Result<(), WorkerError> {
        if !known_task_state(&self.state) {
            return Err(invalid("invalid task hint state"));
        }
        Ok(())
    }
    #[cfg(any(test, feature = "test-support"))]
    pub fn try_new(
        task_id: TaskId,
        run_id: Option<RunId>,
        turn_id: Option<TurnId>,
        state: impl Into<String>,
        code: Option<SafeCode>,
    ) -> Result<Self, WorkerError> {
        let hint = Self {
            task_id,
            run_id,
            turn_id,
            state: state.into(),
            code,
        };
        hint.validate()?;
        Ok(hint)
    }
}
impl QueueHint {
    /// Only saved queue states/kinds. Removal/global invalidation uses None.
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self
            .state
            .as_deref()
            .is_some_and(|v| !matches!(v, "waiting" | "dispatching" | "parked"))
            || self
                .kind
                .as_deref()
                .is_some_and(|v| !matches!(v, "batch" | "task_turn"))
        {
            return Err(invalid("invalid queue hint state/kind"));
        }
        Ok(())
    }
    #[cfg(any(test, feature = "test-support"))]
    pub fn try_new(
        turn_id: Option<TurnId>,
        state: Option<String>,
        kind: Option<String>,
        code: Option<SafeCode>,
    ) -> Result<Self, WorkerError> {
        let hint = Self {
            turn_id,
            state,
            kind,
            code,
        };
        hint.validate()?;
        Ok(hint)
    }
}
#[derive(Serialize, Deserialize)]
struct AutoContinueData {
    task_id: TaskId,
    run_id: Option<RunId>,
    previous_turn_id: TurnId,
    next_turn_id: TurnId,
}
#[derive(Serialize, Deserialize)]
struct RunChangedData {
    run_id: RunId,
    task_id: Option<TaskId>,
}
#[derive(Serialize, Deserialize)]
struct DagChildData {
    run_id: RunId,
    task_id: TaskId,
    turn_id: TurnId,
}
#[derive(Serialize, Deserialize)]
struct WorkerChangedData {
    worker: WorkerName,
    ready: Option<bool>,
    observed_at_millis: u64,
    code: Option<SafeCode>,
}
#[derive(Serialize, Deserialize)]
struct DrainData {
    drained: bool,
}
/// The only producer input: typed identifiers and explicitly safe fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NewEvent {
    TaskCreated(TaskHint),
    TaskChanged(TaskHint),
    TaskRemoved(TaskHint),
    TurnStarted(AcceptedHint),
    TurnFinished(TurnHint),
    TurnOutcomeChanged(TurnHint),
    AutoContinueScheduled {
        task_id: TaskId,
        run_id: Option<RunId>,
        previous: TurnId,
        next: TurnId,
    },
    TaskClosed(TaskHint),
    TaskAbandoned(TaskHint),
    QueueChanged(QueueHint),
    RunChanged {
        run_id: RunId,
        task_id: Option<TaskId>,
    },
    DagChildAdmitted {
        run_id: RunId,
        task_id: TaskId,
        turn_id: TurnId,
    },
    WorkerChanged {
        worker: WorkerName,
        ready: Option<bool>,
        observed_at_millis: u64,
        code: Option<SafeCode>,
    },
    ControllerDrainChanged {
        drained: bool,
    },
}
impl NewEvent {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::TaskCreated(_) => "task.created",
            Self::TaskChanged(_) => "task.changed",
            Self::TaskRemoved(_) => "task.removed",
            Self::TurnStarted(_) => "turn.started",
            Self::TurnFinished(_) => "turn.finished",
            Self::TurnOutcomeChanged(_) => "turn.outcome_changed",
            Self::AutoContinueScheduled { .. } => "task.auto_continue_scheduled",
            Self::TaskClosed(_) => "task.closed",
            Self::TaskAbandoned(_) => "task.abandoned",
            Self::QueueChanged(_) => "queue.changed",
            Self::RunChanged { .. } => "run.changed",
            Self::DagChildAdmitted { .. } => "dag.child_admitted",
            Self::WorkerChanged { .. } => "worker.changed",
            Self::ControllerDrainChanged { drained: true } => "controller.drained",
            Self::ControllerDrainChanged { drained: false } => "controller.undrained",
        }
    }
    pub fn to_wire(
        &self,
        journal_id: uuid::Uuid,
        seq: Seq,
        millis: u64,
    ) -> Result<WireEvent, WorkerError> {
        let data = match self {
            Self::TaskCreated(hint)
            | Self::TaskChanged(hint)
            | Self::TaskRemoved(hint)
            | Self::TaskClosed(hint)
            | Self::TaskAbandoned(hint) => {
                hint.validate()?;
                serde_json::to_value(hint)
            }
            Self::TurnStarted(hint) => serde_json::to_value(hint),
            Self::TurnFinished(hint) | Self::TurnOutcomeChanged(hint) => serde_json::to_value(hint),
            Self::QueueChanged(hint) => {
                hint.validate()?;
                serde_json::to_value(hint)
            }
            Self::AutoContinueScheduled {
                task_id,
                run_id,
                previous,
                next,
            } => serde_json::to_value(AutoContinueData {
                task_id: *task_id,
                run_id: *run_id,
                previous_turn_id: *previous,
                next_turn_id: *next,
            }),
            Self::RunChanged { run_id, task_id } => serde_json::to_value(RunChangedData {
                run_id: *run_id,
                task_id: *task_id,
            }),
            Self::DagChildAdmitted {
                run_id,
                task_id,
                turn_id,
            } => serde_json::to_value(DagChildData {
                run_id: *run_id,
                task_id: *task_id,
                turn_id: *turn_id,
            }),
            Self::WorkerChanged {
                worker,
                ready,
                observed_at_millis,
                code,
            } => serde_json::to_value(WorkerChangedData {
                worker: worker.clone(),
                ready: *ready,
                observed_at_millis: *observed_at_millis,
                code: code.clone(),
            }),
            Self::ControllerDrainChanged { drained } => {
                serde_json::to_value(DrainData { drained: *drained })
            }
        }
        .map_err(|_| invalid("event encoding failed"))?;
        let event = WireEvent {
            schema_version: SCHEMA_VERSION,
            journal_id,
            seq,
            time_millis: millis,
            kind: self.kind().into(),
            data,
        };
        event.validate()?;
        Ok(event)
    }
}
/// All-or-nothing batch; validates at construction, never publishes a prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventBatch(Vec<NewEvent>);
impl EventBatch {
    pub fn try_new(events: Vec<NewEvent>) -> Result<Self, WorkerError> {
        if events.len() > MAX_BATCH_EVENTS {
            return Err(invalid("too many batch events"));
        }
        // Worst-case u64 widths: accepting now cannot fail on a wider clock/seq
        // later. All UUID encodings have fixed width. Empty batches are no-ops.
        let mut bytes = 0;
        for event in &events {
            bytes += event
                .to_wire(uuid::Uuid::from_u128(1), Seq::new(u64::MAX), u64::MAX)?
                .encoded_len()?;
        }
        if bytes > MAX_BATCH_BYTES {
            return Err(invalid("batch exceeds its byte bound"));
        }
        Ok(Self(events))
    }
    pub fn events(&self) -> &[NewEvent] {
        &self.0
    }
    #[cfg(any(test, feature = "test-support"))]
    pub fn into_events(self) -> Vec<NewEvent> {
        self.0
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
/// Optional best-effort enqueue result; a drop never changes authoritative mutation success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishAttempt {
    Queued,
    Dropped,
}
/// Try-only producer entry point. No I/O, flock, sleep, fsync or join here.
pub trait EventSink: Send + Sync {
    fn try_publish(&self, batch: EventBatch) -> PublishAttempt;
}
/// Monotonic time; deadlines are absolute `now()` values.
pub trait EventRuntime: Send + Sync {
    fn now(&self) -> Duration;
    fn sleep(&self, duration: Duration);
    fn cancelled(&self) -> bool;
}
/// Reads committed prefixes. Normalize ReadQuery. Missing/reset/expired/ahead
/// cursors return a control. Empty timeout retains the cursor; next_after never
/// jumps to an undelivered head. Sleep outside locks, checking cancellation and
/// the original absolute deadline. Journal reads never call state or RPC.
pub trait JournalReader: Send + Sync {
    fn window(&self, deadline: Duration) -> Result<JournalWindow, WorkerError>;
    fn read(&self, query: ReadQuery, deadline: Duration) -> Result<EventReadResult, WorkerError>;
}
/// Atomically publish a validated batch outside authoritative fences; speculative tails are invisible.
pub trait JournalWriter: JournalReader {
    fn append(&self, batch: EventBatch, deadline: Duration) -> Result<EventCursor, WorkerError>;
}
/// Optional existing host journal; must never initialize one.
pub trait JournalProvider: Send + Sync {
    fn open_existing(
        &self,
        deadline: Duration,
    ) -> Result<Option<Arc<dyn JournalReader>>, WorkerError>;
}

/// Validated state facts, separate from the title-free journal envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskFacts {
    pub task_id: TaskId,
    pub run_id: Option<RunId>,
    pub state: String,
    pub latest_turn_id: Option<TurnId>,
    pub outcome: Option<SafeOutcome>,
    pub code: Option<SafeCode>,
    pub runner_present: bool,
    pub close_intent: bool,
    pub auto_continue_intent: bool,
    pub queue_dispatching: Option<bool>,
    pub result_imported: bool,
    pub busy: Option<bool>,
    pub quiescent: Option<bool>,
    pub fact_digest: String,
    pub title: Option<String>,
}
/// Tolerant wire facts; all required identity/proof fields remain required.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskFactsWire {
    pub task_id: TaskId,
    pub run_id: Option<RunId>,
    pub state: String,
    pub latest_turn_id: Option<TurnId>,
    pub outcome: Option<String>,
    pub code: Option<String>,
    pub runner_present: bool,
    pub close_intent: bool,
    pub auto_continue_intent: bool,
    pub queue_dispatching: Option<bool>,
    pub result_imported: bool,
    pub busy: Option<bool>,
    pub quiescent: Option<bool>,
    pub fact_digest: String,
    pub title: Option<String>,
}
impl TaskFacts {
    pub fn try_new(wire: TaskFactsWire) -> Result<Self, WorkerError> {
        wire.try_into()
    }
    pub fn eligibility_signature(&self) -> TaskEligibilitySignature {
        let (busy, quiescent) = self.proof_flags();
        let current_attention = self.state == "open"
            && self.latest_turn_id.is_some()
            && quiescent == Some(true)
            && matches!(
                self.outcome,
                Some(SafeOutcome::NeedsInput | SafeOutcome::Blocked)
            );
        let abandoned_without_turn = self.state == "abandoned"
            && self.latest_turn_id.is_none()
            && self.outcome.is_none()
            && quiescent == Some(true);
        TaskEligibilitySignature {
            latest_turn_id: self.latest_turn_id,
            outcome: self.outcome,
            code: self.code.clone(),
            busy,
            quiescent,
            current_attention,
            abandoned_without_turn,
        }
    }
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.state.is_empty()
            || self.state.len() > 32
            || !self
                .state
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b == b'_')
            || self.fact_digest.len() != 64
            || !self
                .fact_digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || (self.outcome.is_some() && self.latest_turn_id.is_none())
            || self.title.as_ref().is_some_and(|v| {
                v.len() > MAX_DISPLAY_TITLE_BYTES || v.chars().any(char::is_control)
            })
        {
            return Err(invalid("invalid task facts"));
        }
        if serde_json::to_vec(self)
            .map_err(|_| invalid("fact encoding failed"))?
            .len()
            > MAX_TASK_FACT_BYTES
        {
            return Err(invalid("task facts exceed their byte bound"));
        }
        Ok(())
    }
    fn proof_flags(&self) -> (Option<bool>, Option<bool>) {
        if self.validate().is_err() {
            return (None, None);
        }
        if self.runner_present
            || self.close_intent
            || self.auto_continue_intent
            || self.state == "active"
            || self.queue_dispatching == Some(true)
            || self.busy == Some(true)
        {
            return (Some(true), Some(false));
        }
        if !known_task_state(&self.state)
            || self.queue_dispatching.is_none()
            || self.busy != Some(false)
        {
            return (None, None);
        }
        if matches!(
            self.state.as_str(),
            "open" | "closed" | "abandoned" | "lost"
        ) {
            return (Some(false), self.quiescent);
        }
        (Some(false), Some(false))
    }
}
/// At most sixteen distinct row/missing IDs; unknown proof carries an independent continuation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskFactsBatch {
    pub rows: Vec<TaskFacts>,
    pub missing: Vec<TaskId>,
    pub proof_after: Option<OpaqueCursor>,
    pub baseline_after: Option<EventCursor>,
}
/// At most 128 title-free rows. Complete can include unknown proofs; it is not a point-in-time snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskRepairPage {
    pub rows: Vec<TaskFacts>,
    pub next: Option<OpaqueCursor>,
    pub complete: bool,
    pub restart: bool,
    pub baseline_after: Option<EventCursor>,
}
/// Existing-state, read-only addressed/repair facts; no initialization, SSH, Git or task reconciliation.
pub trait TaskProjectionReader: Send + Sync {
    fn addressed(
        &self,
        query: TaskAddressQuery,
        deadline: Duration,
    ) -> Result<TaskFactsBatch, WorkerError>;
    fn repair(
        &self,
        query: TaskRepairQuery,
        deadline: Duration,
    ) -> Result<TaskRepairPage, WorkerError>;
}
/// Existing state opener, independent of journal availability and its locks.
pub trait TaskProjectionProvider: Send + Sync {
    fn open_existing(
        &self,
        deadline: Duration,
    ) -> Result<Arc<dyn TaskProjectionReader>, WorkerError>;
}
/// Transport support only; Unsupported never means confirmed absence of task attention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSupport {
    Supported,
    Unsupported,
}
/// Remote read abstraction. Discovery/read/state requests share the original absolute RPC deadline.
pub trait EventSource: Send + Sync {
    fn discover(&self, deadline: Duration) -> Result<EventSupport, WorkerError>;
    fn read(&self, query: ReadQuery, deadline: Duration) -> Result<EventReadResult, WorkerError>;
    fn tasks(
        &self,
        query: TaskAddressQuery,
        deadline: Duration,
    ) -> Result<TaskFactsBatch, WorkerError>;
    fn repair(
        &self,
        query: TaskRepairQuery,
        deadline: Duration,
    ) -> Result<TaskRepairPage, WorkerError>;
}
// Canonical UUID ordering preserves the exact BTreeMap<TaskId, ...> contract
// without changing saved-ID schemas or editing task.rs outside T1 ownership.
impl Ord for TaskId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_uuid().cmp(&other.as_uuid())
    }
}
impl PartialOrd for TaskId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
/// Absent means cold start even with a persisted event cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreviousProjection {
    Absent,
    Present(BTreeMap<TaskId, TaskFacts>),
}
/// Cold baseline suppresses historical repair completions; Warm has a previous complete projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BaselineKind {
    Cold,
    Warm,
}
/// Notification-relevant facts only: latest outcome/proof/attention/abandonment, excluding title/metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskEligibilitySignature {
    pub latest_turn_id: Option<TurnId>,
    pub outcome: Option<SafeOutcome>,
    pub code: Option<SafeCode>,
    pub busy: Option<bool>,
    pub quiescent: Option<bool>,
    pub current_attention: bool,
    pub abandoned_without_turn: bool,
}
/// Replay terminal/abandonment evidence stays separate from warm derived repair differences.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeCause {
    ReplayTerminal {
        turn_id: TurnId,
        outcome: SafeOutcome,
    },
    ReplayAbandoned,
    RepairDifference,
}
/// Previous/current confirmed facts without a sequence; never serialized as a fabricated WireEvent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedTaskChange {
    pub task_id: TaskId,
    pub previous: Option<TaskFacts>,
    pub current: Option<TaskFacts>,
    pub cause: ChangeCause,
}
/// Consumer-owned nonoverlapping sweep progress; budgeted pages continue rather than timer-restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairProgress {
    NotStarted,
    InProgress,
    Complete,
    Restarted,
}
/// Optional feed hint and repair/title preferences; neither is permission to display unconfirmed outcomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileInput {
    pub read: Option<EventReadResult>,
    pub repair_due: bool,
    pub include_titles: bool,
}
/// Count and SHA-256 of the sorted complete current attention set, independent of epoch/time/page boundaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttentionSummary {
    pub count: usize,
    pub fingerprint: String,
}
/// Derived changes have no sequence. Chunk changes/confirmed/pending at 256;
/// overflow requires repair. Cold history never creates repair differences.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reconciliation {
    pub consumed_after: Option<EventCursor>,
    pub baseline: BaselineKind,
    pub changes: Vec<DerivedTaskChange>,
    pub confirmed: Vec<TaskFacts>,
    pub pending_ids: Vec<TaskId>,
    pub repair: RepairProgress,
    pub attention: Option<AttentionSummary>,
    pub repair_needed: bool,
}
/// Confirms affected IDs before yielding bounded changes; cold history and incomplete absence do not become notices.
pub trait EventReconciler {
    fn reconcile(
        &mut self,
        source: &dyn EventSource,
        input: ReconcileInput,
        deadline: Duration,
    ) -> Result<Reconciliation, WorkerError>;
}
/// Serialization uses event/data fields matching the TypeScript contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewerMessage {
    ControllerEvent(WireEvent),
    SnapshotRequired(SnapshotRequired),
    Ready(JournalWindow),
    SnapshotReady { revision: u64 },
    Heartbeat,
    Unavailable { code: String },
}
impl ViewerMessage {
    pub fn event_name(&self) -> &'static str {
        match self {
            Self::ControllerEvent(_) => "controller.event",
            Self::SnapshotRequired(_) | Self::Unavailable { .. } => "snapshot_required",
            Self::Ready(_) => "ready",
            Self::SnapshotReady { .. } => "snapshot.ready",
            Self::Heartbeat => "heartbeat",
        }
    }
    pub fn cursor(&self) -> Option<EventCursor> {
        match self {
            Self::ControllerEvent(event) => Some(EventCursor {
                journal_id: event.journal_id,
                seq: event.seq,
            }),
            _ => None,
        }
    }
}
/// Bounded local subscriptions (256 messages/eight streams). Stop cancels streams before HTTP shutdown.
pub trait ViewerEventSource: Send + Sync {
    fn subscribe(
        &self,
        after: Option<EventCursor>,
    ) -> Result<tokio::sync::mpsc::Receiver<ViewerMessage>, WorkerError>;
    fn stop(&self);
}
impl Serialize for ViewerMessage {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let data = self.data_value().map_err(serde::ser::Error::custom)?;
        serde_json::json!({"event": self.event_name(), "data": data}).serialize(s)
    }
}
/// Request debounced local refresh; broadcast cache revisions only after the projection is visible.
pub trait LocalProjectionRefresh: Send + Sync {
    fn request_refresh(&self);
    fn subscribe_publications(&self) -> tokio::sync::broadcast::Receiver<u64>;
}
/// Laptop channel choice. Auto resolves the laptop Herdr socket or macOS; no controller-account forwarding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum NotifyChannel {
    #[default]
    Auto,
    Macos,
    Herdr,
    Both,
}
/// CLI-only options; default is follow=false, quiet=false, no_titles=false, channel=Auto.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NotifyOptions {
    pub follow: bool,
    pub quiet: bool,
    pub no_titles: bool,
    pub channel: NotifyChannel,
}
/// Fixed sound policy: Done for done, Request for needs_input/blocked, otherwise None.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeSound {
    None,
    Done,
    Request,
}
/// Laptop display-only safe title and fixed outcome copy; never journal/cache task prose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub fingerprint: String,
    pub title: String,
    pub body: String,
    pub sound: NoticeSound,
}
/// Bounded pending task/turn identity without display titles or persisted task projections.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingCandidate {
    pub task_id: TaskId,
    pub turn_id: Option<TurnId>,
}
/// Transport-only cache; complete projections, titles and prose are excluded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NotifyState {
    pub schema_version: u32,
    pub consumed_after: Option<EventCursor>,
    pub last_complete_repair_millis: Option<u64>,
    pub decisions: Vec<String>,
    pub pending: Vec<PendingCandidate>,
    pub attention_overflow: Option<String>,
    pub repair_needed: bool,
}
impl NotifyState {
    pub fn empty() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            consumed_after: None,
            last_complete_repair_millis: None,
            decisions: Vec::new(),
            pending: Vec::new(),
            attention_overflow: None,
            repair_needed: false,
        }
    }
}
/// Save next state before attempting these notices; quiet consumes decisions without channel calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotifyPlan {
    pub next: NotifyState,
    pub notices: Vec<Notice>,
}
/// One bounded delivery attempt after cache save; OS delivery is not transactional/exactly-once.
pub trait NoticeChannel: Send + Sync {
    fn deliver(&self, notice: &Notice, deadline: Duration) -> Result<(), WorkerError>;
}
impl<'de> Deserialize<'de> for TaskFacts {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        TaskFactsWire::deserialize(d)?
            .try_into()
            .map_err(de::Error::custom)
    }
}
impl TryFrom<TaskFactsWire> for TaskFacts {
    type Error = WorkerError;
    fn try_from(wire: TaskFactsWire) -> Result<Self, Self::Error> {
        if serde_json::to_vec(&wire)
            .map_err(|_| invalid("fact encoding failed"))?
            .len()
            > MAX_TASK_FACT_BYTES
        {
            return Err(invalid("wire facts exceed their byte bound"));
        }
        let outcome = wire.outcome.as_ref().and_then(|value| {
            serde_json::from_value::<SafeOutcome>(serde_json::Value::String(value.clone())).ok()
        });
        let unknown_outcome = wire.outcome.is_some() && outcome.is_none();
        let mut facts = Self {
            task_id: wire.task_id,
            run_id: wire.run_id,
            state: wire.state,
            latest_turn_id: wire.latest_turn_id,
            outcome,
            code: wire.code.as_deref().map(SafeCode::from_public_code),
            runner_present: wire.runner_present,
            close_intent: wire.close_intent,
            auto_continue_intent: wire.auto_continue_intent,
            queue_dispatching: wire.queue_dispatching,
            result_imported: wire.result_imported,
            busy: wire.busy,
            quiescent: wire.quiescent,
            fact_digest: wire.fact_digest,
            title: wire.title,
        };
        facts.validate()?;
        (facts.busy, facts.quiescent) = facts.proof_flags();
        if unknown_outcome {
            facts.quiescent = None;
        }
        Ok(facts)
    }
}

impl ReadBatch {
    pub fn validate(&self) -> Result<(), WorkerError> {
        JournalWindow {
            journal_id: self.journal_id,
            oldest_seq: self.oldest_seq,
            head_seq: self.head_seq,
        }
        .validate()?;
        if self.schema_version == 0
            || self.events.len() > READ_MAX_LIMIT
            || self.next_after.journal_id != self.journal_id
            || self.next_after.seq > self.head_seq
            || self.next_after.seq.as_u64() < self.oldest_seq.as_u64().saturating_sub(1)
        {
            return Err(invalid("invalid read batch cursor/bounds"));
        }
        let mut previous: Option<Seq> = None;
        for event in &self.events {
            event.validate()?;
            if event.journal_id != self.journal_id
                || event.seq < self.oldest_seq
                || event.seq > self.head_seq
                || previous.is_some_and(|seq| seq.checked_increment() != Some(event.seq))
            {
                return Err(invalid("invalid committed event order"));
            }
            previous = Some(event.seq);
        }
        if previous.is_some_and(|seq| seq != self.next_after.seq)
            || self.has_more != (self.next_after.seq < self.head_seq)
            || (self.events.is_empty() && self.has_more)
        {
            return Err(invalid("next_after is not last delivered"));
        }
        Ok(())
    }
}
fn stable_reason(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}
impl SnapshotRequired {
    pub fn validate(&self) -> Result<(), WorkerError> {
        if !stable_reason(&self.reason) {
            return Err(invalid("invalid repair reason"));
        }
        self.window.validate()
    }
}
impl EventReadResult {
    pub fn validate(&self) -> Result<(), WorkerError> {
        match self {
            Self::Batch(batch) => batch.validate(),
            Self::SnapshotRequired(control) => control.validate(),
        }
    }
}
fn distinct_ids(ids: impl IntoIterator<Item = TaskId>) -> bool {
    let mut seen = std::collections::BTreeSet::new();
    ids.into_iter().all(|id| seen.insert(id))
}
impl TaskFactsBatch {
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.rows.len() + self.missing.len() > ADDRESSED_MAX_TASKS
            || !distinct_ids(
                self.rows
                    .iter()
                    .map(|row| row.task_id)
                    .chain(self.missing.iter().copied()),
            )
        {
            return Err(invalid("invalid addressed reply IDs/count"));
        }
        for row in &self.rows {
            row.validate()?;
        }
        Ok(())
    }
}
impl TaskRepairPage {
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.rows.len() > REPAIR_MAX_LIMIT
            || !distinct_ids(self.rows.iter().map(|row| row.task_id))
            || (self.complete && (self.next.is_some() || self.restart))
            || (!self.complete && !self.restart && self.next.is_none())
        {
            return Err(invalid("invalid repair page bounds/continuation"));
        }
        for row in &self.rows {
            row.validate()?;
            if row.title.is_some() {
                return Err(invalid("repair contains display titles"));
            }
        }
        Ok(())
    }
}
fn digest_text(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
impl NotifyState {
    /// Decision/overflow fingerprints are lowercase SHA-256 hex, preventing
    /// free text in persisted dedup. Complete projections remain in memory.
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.schema_version != SCHEMA_VERSION
            || self.decisions.len() > NOTIFY_DECISION_CAPACITY
            || self.pending.len() > NOTIFY_PENDING_CAPACITY
            || self.decisions.iter().any(|v| !digest_text(v))
            || self
                .attention_overflow
                .as_deref()
                .is_some_and(|v| !digest_text(v))
            || !distinct_ids(self.pending.iter().map(|v| v.task_id))
        {
            return Err(invalid("invalid notifier cache bounds/fingerprints"));
        }
        if serde_json::to_vec(self)
            .map_err(|_| invalid("cache encoding failed"))?
            .len()
            > MAX_NOTIFY_STATE_BYTES
        {
            return Err(invalid("notifier cache exceeds byte bound"));
        }
        Ok(())
    }
}
impl Reconciliation {
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.changes.len() > MAX_RECONCILIATION_ROWS
            || self.confirmed.len() > MAX_RECONCILIATION_ROWS
            || self.pending_ids.len() > NOTIFY_PENDING_CAPACITY
            || !distinct_ids(self.pending_ids.iter().copied())
        {
            return Err(invalid("reconciliation chunk exceeds bound"));
        }
        for facts in &self.confirmed {
            facts.validate()?;
        }
        for change in &self.changes {
            if self.baseline == BaselineKind::Cold && change.cause == ChangeCause::RepairDifference
            {
                return Err(invalid("cold repair cannot invent historical changes"));
            }
            if change.previous.is_none() && change.current.is_none() {
                return Err(invalid("derived change has no facts"));
            }
            for facts in [&change.previous, &change.current].into_iter().flatten() {
                facts.validate()?;
                if facts.task_id != change.task_id {
                    return Err(invalid("derived change task identity mismatch"));
                }
            }
        }
        if self
            .attention
            .as_ref()
            .is_some_and(|v| !digest_text(&v.fingerprint))
        {
            return Err(invalid("invalid attention fingerprint"));
        }
        Ok(())
    }
}
impl ViewerMessage {
    /// SSE data only; Serialize additionally wraps the frozen event name.
    pub fn data_value(&self) -> Result<serde_json::Value, WorkerError> {
        match self {
            Self::ControllerEvent(event) => {
                event.validate()?;
                serde_json::to_value(event)
            }
            Self::SnapshotRequired(control) => {
                control.validate()?;
                serde_json::to_value(control)
            }
            Self::Ready(window) => {
                window.validate()?;
                serde_json::to_value(window)
            }
            Self::SnapshotReady { revision } => Ok(serde_json::json!({"revision": revision})),
            Self::Heartbeat => Ok(serde_json::json!({})),
            Self::Unavailable { code } => {
                let safe = match code.as_str() {
                    CONTROLLER_EVENTS_UNAVAILABLE
                    | CONTROLLER_EVENTS_CANCELLED
                    | CONTROLLER_EVENTS_REPAIR_REGISTRY_TOO_LARGE => code.as_str(),
                    _ => CONTROLLER_EVENTS_UNAVAILABLE,
                };
                Ok(serde_json::json!({"reason":"unavailable","code":safe,"window":null}))
            }
        }
        .map_err(|_| invalid("SSE data encoding failed"))
    }
}
// Reply structs tolerate additive fields while checking required identity and bounds.
macro_rules! validated_deserialize {
    ($name:ident, $raw:ident, { $($(#[$attr:meta])* $field:ident: $type:ty),* $(,)? }) => {
        #[derive(Deserialize)] struct $raw { $($(#[$attr])* $field: $type),* }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let raw = $raw::deserialize(d)?; let result = Self { $($field: raw.$field),* };
                result.validate().map_err(de::Error::custom)?; Ok(result)
            }
        }
    };
}
validated_deserialize!(JournalWindow, RawWindow, { #[serde(with = "uuid_wire")] journal_id: uuid::Uuid, oldest_seq: Seq, head_seq: Seq });
validated_deserialize!(WireEvent, RawEvent, { schema_version: u32, #[serde(with = "uuid_wire")] journal_id: uuid::Uuid, seq: Seq, time_millis: u64, kind: String, data: serde_json::Value });
validated_deserialize!(ReadBatch, RawBatch, { schema_version: u32, #[serde(with = "uuid_wire")] journal_id: uuid::Uuid, oldest_seq: Seq, head_seq: Seq, next_after: EventCursor, events: Vec<WireEvent>, has_more: bool });
validated_deserialize!(SnapshotRequired, RawSnapshot, { reason: String, window: JournalWindow });
validated_deserialize!(TaskFactsBatch, RawFactsBatch, { rows: Vec<TaskFacts>, missing: Vec<TaskId>, proof_after: Option<OpaqueCursor>, baseline_after: Option<EventCursor> });
validated_deserialize!(TaskRepairPage, RawRepairPage, { rows: Vec<TaskFacts>, next: Option<OpaqueCursor>, complete: bool, restart: bool, baseline_after: Option<EventCursor> });
validated_deserialize!(NotifyState, RawNotifyState, { schema_version: u32, consumed_after: Option<EventCursor>, last_complete_repair_millis: Option<u64>, decisions: Vec<String>, pending: Vec<PendingCandidate>, attention_overflow: Option<String>, repair_needed: bool });

/// Requests reject unknown cursor keys even when decoded directly; replies use
/// the tolerant EventCursor decoder so additive result fields remain compatible.
fn deserialize_request_cursor<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<EventCursor>, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RequestCursor {
        #[serde(with = "uuid_wire")]
        journal_id: uuid::Uuid,
        seq: Seq,
    }
    Ok(
        Option::<RequestCursor>::deserialize(d)?.map(|cursor| EventCursor {
            journal_id: cursor.journal_id,
            seq: cursor.seq,
        }),
    )
}
impl<'de> Deserialize<'de> for TaskAddressQuery {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {
            task_ids: Vec<TaskId>,
            #[serde(default)]
            include_titles: bool,
            #[serde(default)]
            proof_after: Option<OpaqueCursor>,
        }
        let raw = Request::deserialize(d)?;
        Self::try_new(raw.task_ids, raw.include_titles, raw.proof_after).map_err(de::Error::custom)
    }
}
validated_deserialize!(TaskHint, RawTaskHint, { task_id: TaskId, run_id: Option<RunId>, turn_id: Option<TurnId>, state: String, code: Option<SafeCode> });
validated_deserialize!(QueueHint, RawQueueHint, { turn_id: Option<TurnId>, state: Option<String>, kind: Option<String>, code: Option<SafeCode> });

mod uuid_wire {
    use super::*;
    pub fn serialize<S: Serializer>(id: &uuid::Uuid, s: S) -> Result<S::Ok, S::Error> {
        if id.is_nil() {
            return Err(serde::ser::Error::custom("nil journal UUID"));
        }
        s.serialize_str(&id.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<uuid::Uuid, D::Error> {
        let value = String::deserialize(d)?;
        let id = uuid::Uuid::parse_str(&value).map_err(de::Error::custom)?;
        if id.is_nil() || id.to_string() != value {
            return Err(de::Error::custom("invalid journal UUID"));
        }
        Ok(id)
    }
}
