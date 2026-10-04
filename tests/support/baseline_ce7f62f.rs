//! Frozen protocol-7 codecs from ce7f62f0ed46f162f76196254ac97902e4543477.
//!
//! Copied with `git show ce7f62f:<path>`, including transitive wire types,
//! constructors invoked by decoding, validation, redaction and unique-key
//! visitors. Paths below identify the original owners. Only local paths and
//! non-wire CLI derives changed; store/transport/launch methods are omitted.
//! Keep this module independent of mac_worker and the surrounding test module.
#![allow(dead_code)]

use redaction::RedactionBoundary;
use scrub::Scrubber;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, DeserializeOwned},
    ser::{self, SerializeStruct},
};
use serde_json::{Map, Value};
use std::{collections::BTreeMap, fmt, str::FromStr};
use uuid::Uuid;

const PROTOCOL_VERSION: u32 = 7;
const MAX_PROMPT_BYTES: usize = 256 * 1024;
const MAX_TITLE_BYTES: usize = 120;
const MAX_FOLLOWUPS: u32 = 100;
const DEFAULT_MAX_FOLLOWUPS: u32 = 10;
const DEFAULT_TURN_TIMEOUT_MILLIS: u64 = 45 * 60 * 1000;
const MAX_IDENTITY_BYTES: usize = 256;
const MAX_HEX_ID_BYTES: usize = 64;
const MAX_EFFORT_BYTES: usize = 32;
const MAX_TASK_FACT_BYTES: usize = 2 * 1024;
const MAX_DISPLAY_TITLE_BYTES: usize = 512;
const CONTROLLER_EVENTS_INVALID: &str = "CONTROLLER_EVENTS_INVALID";

// Local error carriers preserve baseline validation without importing the
// production error catalogue (errors themselves are never exchanged here).
#[derive(Debug)]
pub enum WorkerError {
    Task {
        code: &'static str,
        message: std::borrow::Cow<'static, str>,
    },
    Project {
        code: &'static str,
        message: String,
    },
    Protocol(String),
}
impl WorkerError {
    fn task(code: &'static str, message: impl Into<std::borrow::Cow<'static, str>>) -> Self {
        Self::Task {
            code,
            message: message.into(),
        }
    }
}
impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Task { code, message } => write!(f, "{code}: {message}"),
            Self::Project { code, message } => write!(f, "{code}: {message}"),
            Self::Protocol(message) => f.write_str(message),
        }
    }
}
#[derive(Debug)]
pub struct AdapterError(String);
impl AdapterError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}
impl fmt::Display for AdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
fn session_error(
    code: &'static str,
    message: impl Into<std::borrow::Cow<'static, str>>,
) -> WorkerError {
    WorkerError::task(code, message)
}

// ce7f62f:src/task.rs (TaskId/RunId); src/job.rs (identical JobId codec).

macro_rules! canonical_uuid_id {
    ($name:ident $(, $uuid_cfg:meta)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name(Uuid);

        impl $name {
            #[cfg(any(test, feature = "test-support"))]
            pub fn new(value: Uuid) -> Self {
                Self(value)
            }

            $(#[$uuid_cfg])?
            pub fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{:x}", self.0.simple())
            }
        }

        impl FromStr for $name {
            type Err = String;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                if !is_lower_hex(value, 32) {
                    return Err("identifier must be a lowercase simple UUID".into());
                }
                Uuid::parse_str(value)
                    .map(Self)
                    .map_err(|_| "identifier must be a lowercase simple UUID".into())
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.to_string())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let value = String::deserialize(deserializer)?;
                value.parse().map_err(de::Error::custom)
            }
        }
    };
}

canonical_uuid_id!(TaskId);
canonical_uuid_id!(RunId);
canonical_uuid_id!(JobId);
pub type TurnId = JobId;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BaseOid(String);

impl BaseOid {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BaseOid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for BaseOid {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if is_lower_hex(value, 40) {
            Ok(Self(value.to_owned()))
        } else {
            Err("base object ID must be 40 lowercase hexadecimal characters".into())
        }
    }
}

impl Serialize for BaseOid {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for BaseOid {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BranchName(String);

impl BranchName {
    pub fn for_task(task_id: TaskId) -> Self {
        Self(format!("task/{task_id}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BranchName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for BranchName {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if is_valid_branch_name(value) {
            Ok(Self(value.to_owned()))
        } else {
            Err("branch name is not a conservative Git ref subset".into())
        }
    }
}

impl Serialize for BranchName {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for BranchName {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskTitle(String);

impl TaskTitle {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<(), WorkerError> {
        if self.0.len() > MAX_TITLE_BYTES || self.0.chars().any(char::is_control) {
            return Err(task_config(format!(
                "task title exceeds {MAX_TITLE_BYTES} bytes or contains a control character"
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskSource {
    Local {
        wip: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        push_target: Option<PushTarget>,
    },
    Origin {
        url: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushTarget {
    url: String,
    requirement: String,
}

impl PushTarget {
    pub fn new(url: String) -> Result<Self, WorkerError> {
        if let Some(file) = project::canonical_file_origin(&url)? {
            return Ok(Self {
                url: file,
                requirement: "origin:file".into(),
            });
        }
        let normalized = project::normalize_origin(&url)
            .map_err(|_| task_config("push origin URL is invalid or not in normalized form"))?;
        if normalized != url {
            return Err(task_config(
                "push origin URL is invalid or not in normalized form",
            ));
        }
        let host = project::origin_host(&url)
            .map_err(|_| task_config("push origin URL is invalid or not in normalized form"))?;
        Ok(Self {
            url,
            requirement: format!("origin:{host}"),
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn requirement(&self) -> &str {
        &self.requirement
    }

    fn validate(&self) -> Result<(), WorkerError> {
        let expected = Self::new(self.url.clone())?;
        if self.requirement != expected.requirement {
            return Err(task_config(
                "push origin requirement does not match the normalized origin URL",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublishMode {
    Fetch,
    Push,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClosePolicy {
    Done,
    Never,
}

/// How the coordinator handles ambiguity. New submissions default to Decide;
/// missing policy on a pre-upgrade task record is resolved separately as Ask.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuestionsPolicy {
    #[default]
    Decide,
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Queued,
    Active,
    Open,
    Closed,
    Abandoned,
    Lost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnTerminal {
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    Lost,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskOutcome {
    Done,
    NeedsInput,
    Blocked,
    Unknown,
    Failed { reason: String },
    Cancelled,
    TimedOut,
    Lost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    Pending,
    Retrying,
    Delivered,
    Failed,
}

impl TaskOutcome {
    fn redact(self, boundary: &RedactionBoundary) -> Self {
        match self {
            Self::Failed { reason } => Self::Failed {
                reason: boundary.failure_reason(&reason),
            },
            other => other,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OriginDelivery {
    turn_id: TurnId,
    state: DeliveryState,
    oid: BaseOid,
    origin: String,
    target: String,
    attempt: u32,
    next_attempt_at_millis: u64,
    last_error: Option<String>,
    superseded_by: Option<BaseOid>,
    created_at_millis: u64,
    updated_at_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitIdentity {
    name: String,
    email: String,
}

impl GitIdentity {
    pub fn new(name: impl Into<String>, email: impl Into<String>) -> Result<Self, WorkerError> {
        let identity = Self {
            name: name.into(),
            email: email.into(),
        };
        identity.validate()?;
        Ok(identity)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn email(&self) -> &str {
        &self.email
    }

    fn validate(&self) -> Result<(), WorkerError> {
        validate_identity_component(&self.name, "Git identity name")?;
        validate_identity_component(&self.email, "Git identity email")?;
        Ok(())
    }
}

impl Serialize for GitIdentity {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("GitIdentity", 2)?;
        record.serialize_field("name", &self.name)?;
        record.serialize_field("email", &self.email)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for GitIdentity {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            name: String,
            email: String,
        }
        let wire: Wire = deserialize_unique_object(deserializer)?;
        Self::new(wire.name, wire.email).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskLimits {
    pub turn: TurnLimits,
    pub max_followups: u32,
}

impl TaskLimits {
    pub fn new(turn: TurnLimits, max_followups: u32) -> Result<Self, WorkerError> {
        let limits = Self {
            turn,
            max_followups,
        };
        limits.validate()?;
        Ok(limits)
    }

    fn validate(&self) -> Result<(), WorkerError> {
        self.turn
            .validate()
            .map_err(|error| task_config(error.to_string()))?;
        if self.max_followups > MAX_FOLLOWUPS {
            return Err(task_config(format!(
                "max_followups must be at most {MAX_FOLLOWUPS}"
            )));
        }
        Ok(())
    }
}

impl Default for TaskLimits {
    fn default() -> Self {
        Self::new(
            TurnLimits::new(DEFAULT_TURN_TIMEOUT_MILLIS, None, None)
                .expect("default turn limits are valid"),
            DEFAULT_MAX_FOLLOWUPS,
        )
        .expect("default task limits are valid")
    }
}

impl Serialize for TaskLimits {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("TaskLimits", 2)?;
        record.serialize_field("turn", &TurnLimitsWire::from(&self.turn))?;
        record.serialize_field("max_followups", &self.max_followups)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for TaskLimits {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            turn: TurnLimitsWire,
            max_followups: u32,
        }
        let wire: Wire = deserialize_unique_object(deserializer)?;
        let turn = wire.turn.into_limits().map_err(de::Error::custom)?;
        Self::new(turn, wire.max_followups).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskMeta {
    session_import: Option<SessionImportMeta>,
    task_id: TaskId,
    run_id: Option<RunId>,
    project_id: String,
    worktree_id: String,
    agent: AgentKind,
    model: Option<String>,
    effort: Option<String>,
    policy: PermissionPolicy,
    effective_policy: Option<PermissionPolicy>,
    source: TaskSource,
    publish: Vec<PublishMode>,
    publish_branch: Option<BranchName>,
    base_oid: BaseOid,
    limits: TaskLimits,
    close_policy: ClosePolicy,
    env_profile: Option<String>,
    git_identity: GitIdentity,
    title: TaskTitle,
    created_at_millis: u64,
}

impl TaskMeta {
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn with_effective_policy(
        mut self,
        effective: PermissionPolicy,
    ) -> Result<Self, WorkerError> {
        self.effective_policy = (effective != self.policy).then_some(effective);
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<(), WorkerError> {
        validate_hex_component(&self.project_id, "project ID")?;
        validate_hex_component(&self.worktree_id, "worktree ID")?;
        self.title.validate()?;
        if let Some(import) = &self.session_import {
            import.validate()?;
            if matches!(self.source, TaskSource::Origin { .. }) {
                return Err(session_error(
                    "SESSION_REQUIRES_SNAPSHOT",
                    "session import requires a laptop snapshot",
                ));
            }
            if self.agent != import.agent().agent_kind() {
                return Err(session_error(
                    "SESSION_AGENT_MISMATCH",
                    "session import agent does not match task agent",
                ));
            }
        }
        if let Some(model) = &self.model {
            validate_optional_text(model, MAX_IDENTITY_BYTES, "model")?;
        }
        if let Some(effort) = &self.effort {
            validate_effort(effort).map_err(|error| task_config(error.to_string()))?;
        }
        if let Some(profile) = &self.env_profile {
            validate_optional_text(profile, MAX_IDENTITY_BYTES, "env profile")?;
        }
        self.limits.validate()?;
        self.git_identity.validate()?;
        self.validate_core_scope()?;
        Ok(())
    }

    fn validate_core_scope(&self) -> Result<(), WorkerError> {
        match &self.source {
            TaskSource::Origin { url } => {
                let normalized = project::normalize_origin(url)
                    .map_err(|_| task_config("origin URL is invalid or not in normalized form"))?;
                if normalized != *url {
                    return Err(task_config(
                        "origin URL is invalid or not in normalized form",
                    ));
                }
            }
            TaskSource::Local { push_target, .. } => {
                if let Some(target) = push_target {
                    target.validate()?;
                }
                if self.publish.contains(&PublishMode::Push) && push_target.is_none() {
                    return Err(task_config(
                        "local push publication requires a pinned origin target",
                    ));
                }
                if !self.publish.contains(&PublishMode::Push) && push_target.is_some() {
                    return Err(task_config("local push target requires push publication"));
                }
            }
        }
        if !self.publish.contains(&PublishMode::Fetch) {
            return Err(task_config("publish fetch is required for every task"));
        }
        if matches!(self.source, TaskSource::Local { wip: true, .. })
            && self.publish.contains(&PublishMode::Push)
        {
            return Err(WorkerError::task(
                "PUBLISH_REQUIRES_COMMITTED_BASE",
                "publish push requires a committed base",
            ));
        }
        if self
            .publish
            .iter()
            .enumerate()
            .any(|(index, mode)| self.publish[..index].contains(mode))
        {
            return Err(task_config("publish modes must be unique"));
        }
        if self.publish_branch.is_some() && !self.publish.contains(&PublishMode::Push) {
            return Err(task_config("publish branch requires publish push"));
        }
        Ok(())
    }
}

impl Serialize for TaskMeta {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        // `effort` is omitted when unset so a record written before it existed
        // re-serializes byte for byte and keeps its canonical bytes.
        let mut record = serializer.serialize_struct(
            "TaskMeta",
            17 + usize::from(self.effort.is_some())
                + usize::from(self.effective_policy.is_some())
                + usize::from(self.session_import.is_some()),
        )?;
        if let Some(import) = &self.session_import {
            record.serialize_field("session_import", import)?;
        }
        record.serialize_field("task_id", &self.task_id)?;
        record.serialize_field("run_id", &self.run_id)?;
        record.serialize_field("project_id", &self.project_id)?;
        record.serialize_field("worktree_id", &self.worktree_id)?;
        record.serialize_field("agent", &AgentKindWire::from(self.agent))?;
        record.serialize_field("model", &self.model)?;
        if self.effort.is_some() {
            record.serialize_field("effort", &self.effort)?;
        }
        record.serialize_field("policy", &PermissionPolicyWire::from(self.policy))?;
        if let Some(effective) = self.effective_policy {
            record.serialize_field("effective_policy", &PermissionPolicyWire::from(effective))?;
        }
        record.serialize_field("source", &self.source)?;
        record.serialize_field("publish", &self.publish)?;
        record.serialize_field("publish_branch", &self.publish_branch)?;
        record.serialize_field("base_oid", &self.base_oid)?;
        record.serialize_field("limits", &self.limits)?;
        record.serialize_field("close_policy", &self.close_policy)?;
        record.serialize_field("env_profile", &self.env_profile)?;
        record.serialize_field("git_identity", &self.git_identity)?;
        record.serialize_field("title", &self.title)?;
        record.serialize_field("created_at_millis", &self.created_at_millis)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for TaskMeta {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            #[serde(default, skip_serializing_if = "Option::is_none")]
            session_import: Option<SessionImportMeta>,
            task_id: TaskId,
            run_id: Option<RunId>,
            project_id: String,
            worktree_id: String,
            agent: AgentKindWire,
            model: Option<String>,
            #[serde(default)]
            effort: Option<String>,
            policy: PermissionPolicyWire,
            #[serde(default)]
            effective_policy: Option<PermissionPolicyWire>,
            source: TaskSource,
            publish: Vec<PublishMode>,
            publish_branch: Option<BranchName>,
            base_oid: BaseOid,
            limits: TaskLimits,
            close_policy: ClosePolicy,
            env_profile: Option<String>,
            git_identity: GitIdentity,
            title: TaskTitle,
            created_at_millis: u64,
        }
        let wire: Wire = deserialize_unique_object(deserializer)?;
        let meta = TaskMeta {
            session_import: wire.session_import,
            task_id: wire.task_id,
            run_id: wire.run_id,
            project_id: wire.project_id,
            worktree_id: wire.worktree_id,
            agent: wire.agent.into(),
            model: wire.model,
            effort: wire.effort,
            policy: wire.policy.into(),
            effective_policy: None,
            source: wire.source,
            publish: wire.publish,
            publish_branch: wire.publish_branch,
            base_oid: wire.base_oid,
            limits: wire.limits,
            close_policy: wire.close_policy,
            env_profile: wire.env_profile,
            git_identity: wire.git_identity,
            title: wire.title,
            created_at_millis: wire.created_at_millis,
        };
        meta.validate().map_err(de::Error::custom)?;
        match wire.effective_policy {
            Some(effective) => meta
                .with_effective_policy(effective.into())
                .map_err(de::Error::custom),
            None => Ok(meta),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnSummary {
    turn_number: u32,
    turn_id: TurnId,
    #[serde(default, skip_serializing_if = "is_false")]
    auto_continue: bool,
    terminal: Option<TurnTerminal>,
    outcome: Option<TaskOutcome>,
    agent_committed: Option<bool>,
    log_truncated: bool,
    started_at_millis: Option<u64>,
    ended_at_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    herdr: Option<HerdrTurnReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result_parse_reason: Option<ResultParseReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_identity: Option<AgentIdentity>,
}

/// Whether, and where, a turn was shown in the worker's herdr.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HerdrTurnReport {
    pub state: HerdrTurnState,
    /// Herdr's opaque pane id on the worker; carries no path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pane_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HerdrTurnState {
    /// The worker's herdr shows this turn.
    Attached,
    /// The worker was asked to report but its herdr could not be reached.
    Unavailable,
    /// The worker is not configured to report.
    Disabled,
}

impl TurnSummary {
    fn redact(self, boundary: &RedactionBoundary) -> Self {
        Self {
            outcome: self.outcome.map(|outcome| outcome.redact(boundary)),
            agent_identity: self
                .agent_identity
                .map(|identity| identity.redacted(boundary)),
            ..self
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    reported_checks: Vec<ReportedCheck>,
}

pub(crate) fn is_false(value: &bool) -> bool {
    !value
}

impl TaskStatus {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
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
    ) -> Result<Self, WorkerError> {
        let boundary = RedactionBoundary::from_env();
        let status = Self {
            state,
            last_outcome: last_outcome.map(|outcome| outcome.redact(&boundary)),
            worker,
            session_present,
            head_oid,
            summary: summary.as_deref().map(|summary| boundary.summary(summary)),
            questions: boundary.questions(questions),
            files_changed: boundary.changed_files(files_changed),
            diff_stat: diff_stat.as_deref().map(|stat| boundary.diff_stat(stat)),
            turns: turns
                .into_iter()
                .map(|turn| turn.redact(&boundary))
                .collect(),
            updated_at_millis,
            reported_checks: Vec::new(),
        };
        status.validate()?;
        Ok(status)
    }

    pub fn state(&self) -> TaskState {
        self.state
    }

    pub fn with_reported_checks(mut self, checks: Vec<ReportedCheck>) -> Result<Self, WorkerError> {
        let boundary = RedactionBoundary::from_env();
        self.reported_checks = boundary.reported_checks(checks);
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<(), WorkerError> {
        if let Some(worker) = &self.worker {
            validate_pinned_worker(worker)?;
        }
        if let Some(summary) = &self.summary {
            validate_optional_text(summary, MAX_PROMPT_BYTES, "status summary")?;
        }
        if let Some(diff_stat) = &self.diff_stat {
            validate_optional_text(diff_stat, MAX_PROMPT_BYTES, "diff stat")?;
        }
        Ok(())
    }
}

impl Serialize for TaskStatus {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let field_count = if self.reported_checks.is_empty() {
            11
        } else {
            12
        };
        let mut record = serializer.serialize_struct("TaskStatus", field_count)?;
        record.serialize_field("state", &self.state)?;
        record.serialize_field("last_outcome", &self.last_outcome)?;
        record.serialize_field("worker", &self.worker)?;
        record.serialize_field("session_present", &self.session_present)?;
        record.serialize_field("head_oid", &self.head_oid)?;
        record.serialize_field("summary", &self.summary)?;
        record.serialize_field("questions", &self.questions)?;
        record.serialize_field("files_changed", &self.files_changed)?;
        record.serialize_field("diff_stat", &self.diff_stat)?;
        record.serialize_field("turns", &self.turns)?;
        record.serialize_field("updated_at_millis", &self.updated_at_millis)?;
        if !self.reported_checks.is_empty() {
            record.serialize_field("reported_checks", &self.reported_checks)?;
        }
        record.end()
    }
}

impl<'de> Deserialize<'de> for TaskStatus {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
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
        let wire: Wire = deserialize_unique_object(deserializer)?;
        Self::new(
            wire.state,
            wire.last_outcome,
            wire.worker,
            wire.session_present,
            wire.head_oid,
            wire.summary,
            wire.questions,
            wire.files_changed,
            wire.diff_stat,
            wire.turns,
            wire.updated_at_millis,
        )
        .and_then(|status| status.with_reported_checks(wire.reported_checks))
        .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunnerIdentity(ProcessIdentity);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunnerState {
    Live,
    Dead,
    Exited,
}

/// Durable close fence. Local state stays `Open` until the remote close is
/// acknowledged; omitted from JSON when unset so legacy records round-trip.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskCloseIntent {
    discard: bool,
    last_turn_id: Option<TurnId>,
    turn_count: u32,
    head_oid: Option<BaseOid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalTaskRecord {
    meta: TaskMeta,
    questions_policy: Option<QuestionsPolicy>,
    auto_continue_intent: Option<Box<PreparedFollowup>>,
    status: TaskStatus,
    status_observed_at_millis: Option<u64>,
    runner: Option<RunnerIdentity>,
    fetched_head: Option<BaseOid>,
    repo_id: String,
    // Task 7 converts pinned_worker into WorkerPreference.
    pinned_worker: Option<String>,
    wait_for_capacity: bool,
    abandon_code: Option<String>,
    submission_intent_turn_id: Option<TurnId>,
    submission_rollback_turn_id: Option<TurnId>,
    close_intent: Option<TaskCloseIntent>,
    delivery: Option<OriginDelivery>,
    deliveries: Vec<OriginDelivery>,
    failure_receipt: Option<failure_receipt::FailureReceipt>,
}

impl LocalTaskRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        meta: TaskMeta,
        status: TaskStatus,
        status_observed_at_millis: Option<u64>,
        runner: Option<RunnerIdentity>,
        fetched_head: Option<BaseOid>,
        repo_id: String,
        pinned_worker: Option<String>,
        wait_for_capacity: bool,
        abandon_code: Option<String>,
    ) -> Result<Self, WorkerError> {
        let record = Self {
            meta,
            questions_policy: None,
            auto_continue_intent: None,
            status,
            status_observed_at_millis,
            runner,
            fetched_head,
            repo_id,
            pinned_worker,
            wait_for_capacity,
            abandon_code,
            submission_intent_turn_id: None,
            submission_rollback_turn_id: None,
            close_intent: None,
            delivery: None,
            deliveries: Vec::new(),
            failure_receipt: None,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn meta(&self) -> &TaskMeta {
        &self.meta
    }

    pub(crate) fn auto_continue_intent(&self) -> Option<&PreparedFollowup> {
        self.auto_continue_intent.as_deref()
    }

    fn validate(&self) -> Result<(), WorkerError> {
        self.meta.validate()?;
        self.status.validate()?;
        if let Some(intent) = self.auto_continue_intent()
            && (!intent.auto_continue()
                || intent.task_id() != self.meta.task_id()
                || intent.expected().auto_continue_intent().is_some())
        {
            return Err(task_config("invalid automatic continuation intent"));
        }
        validate_hex_component(&self.repo_id, "repo ID")?;
        if let Some(worker) = &self.pinned_worker {
            validate_pinned_worker(worker)?;
        }
        if let Some(code) = &self.abandon_code {
            validate_optional_text(code, 128, "abandon code")?;
        }
        if self.submission_rollback_turn_id.is_some()
            && self.abandon_code.as_deref() != Some("SUBMISSION_ROLLBACK_INCOMPLETE")
        {
            return Err(task_config(
                "submission rollback turn requires an incomplete rollback marker",
            ));
        }
        if self.submission_intent_turn_id.is_some()
            && self.submission_rollback_turn_id.is_some()
            && self.submission_intent_turn_id != self.submission_rollback_turn_id
        {
            return Err(task_config(
                "submission intent and rollback marker turn identifiers differ",
            ));
        }
        if let Some(intent) = &self.close_intent {
            if self.status.state != TaskState::Open {
                return Err(task_config("close intent requires an open task"));
            }
            if self.submission_intent_turn_id.is_some()
                || self.submission_rollback_turn_id.is_some()
            {
                return Err(task_config(
                    "close intent cannot overlap submission recovery",
                ));
            }
            let _ = intent;
        }
        Ok(())
    }
}

impl Serialize for LocalTaskRecord {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut field_count = 9
            + usize::from(self.questions_policy.is_some())
            + usize::from(self.auto_continue_intent.is_some());
        if self.submission_intent_turn_id.is_some() {
            field_count += 1;
        }
        if self.submission_rollback_turn_id.is_some() {
            field_count += 1;
        }
        if self.close_intent.is_some() {
            field_count += 1;
        }
        if self.delivery.is_some() {
            field_count += 1;
        }
        if !self.deliveries.is_empty() {
            field_count += 1;
        }
        if self.failure_receipt.is_some() {
            field_count += 2;
        }
        let mut record = serializer.serialize_struct("LocalTaskRecord", field_count)?;
        record.serialize_field("meta", &self.meta)?;
        if let Some(policy) = self.questions_policy {
            record.serialize_field("questions_policy", &policy)?;
        }
        if let Some(intent) = &self.auto_continue_intent {
            record.serialize_field("auto_continue_intent", intent)?;
        }
        record.serialize_field("status", &self.status)?;
        record.serialize_field("status_observed_at_millis", &self.status_observed_at_millis)?;
        record.serialize_field("runner", &self.runner)?;
        record.serialize_field("fetched_head", &self.fetched_head)?;
        record.serialize_field("repo_id", &self.repo_id)?;
        record.serialize_field("pinned_worker", &self.pinned_worker)?;
        record.serialize_field("wait_for_capacity", &self.wait_for_capacity)?;
        record.serialize_field("abandon_code", &self.abandon_code)?;
        if let Some(turn_id) = self.submission_intent_turn_id {
            record.serialize_field("submission_intent_turn_id", &turn_id)?;
        }
        if let Some(turn_id) = self.submission_rollback_turn_id {
            record.serialize_field("submission_rollback_turn_id", &turn_id)?;
        }
        if let Some(intent) = &self.close_intent {
            record.serialize_field("close_intent", intent)?;
        }
        if let Some(delivery) = &self.delivery {
            record.serialize_field("delivery", delivery)?;
        }
        if !self.deliveries.is_empty() {
            record.serialize_field("deliveries", &self.deliveries)?;
        }
        if let Some(receipt) = &self.failure_receipt {
            record.serialize_field("failure_stage", receipt.stage())?;
            record.serialize_field("failure_residual", &receipt.residual())?;
        }
        record.end()
    }
}

impl<'de> Deserialize<'de> for LocalTaskRecord {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            meta: TaskMeta,
            #[serde(default)]
            questions_policy: Option<QuestionsPolicy>,
            #[serde(default)]
            auto_continue_intent: Option<Box<PreparedFollowup>>,
            status: TaskStatus,
            status_observed_at_millis: Option<u64>,
            runner: Option<RunnerIdentity>,
            fetched_head: Option<BaseOid>,
            repo_id: String,
            pinned_worker: Option<String>,
            wait_for_capacity: bool,
            abandon_code: Option<String>,
            #[serde(default)]
            submission_intent_turn_id: Option<TurnId>,
            #[serde(default)]
            submission_rollback_turn_id: Option<TurnId>,
            #[serde(default)]
            close_intent: Option<TaskCloseIntent>,
            #[serde(default)]
            delivery: Option<OriginDelivery>,
            #[serde(default)]
            deliveries: Vec<OriginDelivery>,
            #[serde(default)]
            failure_stage: Option<String>,
            #[serde(default)]
            failure_residual: Vec<String>,
        }
        let wire: Wire = deserialize_unique_object(deserializer)?;
        let mut record = Self::new(
            wire.meta,
            wire.status,
            wire.status_observed_at_millis,
            wire.runner,
            wire.fetched_head,
            wire.repo_id,
            wire.pinned_worker,
            wire.wait_for_capacity,
            wire.abandon_code,
        )
        .map_err(de::Error::custom)?;
        record.questions_policy = wire.questions_policy;
        record.auto_continue_intent = wire.auto_continue_intent;
        record.submission_intent_turn_id = wire.submission_intent_turn_id;
        record.submission_rollback_turn_id = wire.submission_rollback_turn_id;
        record.close_intent = wire.close_intent;
        record.deliveries = if wire.deliveries.is_empty() {
            wire.delivery.into_iter().collect()
        } else {
            wire.deliveries
        };
        record.delivery = record.deliveries.first().cloned();
        record.failure_receipt = match wire.failure_stage.as_deref() {
            Some(stage) => {
                let residual = wire
                    .failure_residual
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                failure_receipt::FailureReceipt::new(stage, &residual)
            }
            None => None,
        };
        record.validate().map_err(de::Error::custom)?;
        Ok(record)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AgentKindWire {
    Codex,
    Claude,
    Cursor,
    Opencode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PermissionPolicyWire {
    Workspace,
    Unattended,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TurnLimitsWire {
    timeout_millis: u64,
    max_turns: Option<u32>,
    max_budget_usd_cents: Option<u64>,
}

impl From<AgentKind> for AgentKindWire {
    fn from(kind: AgentKind) -> Self {
        match kind {
            AgentKind::Codex => Self::Codex,
            AgentKind::Claude => Self::Claude,
            AgentKind::Cursor => Self::Cursor,
            AgentKind::Opencode => Self::Opencode,
        }
    }
}

impl From<AgentKindWire> for AgentKind {
    fn from(kind: AgentKindWire) -> Self {
        match kind {
            AgentKindWire::Codex => Self::Codex,
            AgentKindWire::Claude => Self::Claude,
            AgentKindWire::Cursor => Self::Cursor,
            AgentKindWire::Opencode => Self::Opencode,
        }
    }
}

impl From<PermissionPolicy> for PermissionPolicyWire {
    fn from(policy: PermissionPolicy) -> Self {
        match policy {
            PermissionPolicy::Workspace => Self::Workspace,
            PermissionPolicy::Unattended => Self::Unattended,
        }
    }
}

impl From<PermissionPolicyWire> for PermissionPolicy {
    fn from(policy: PermissionPolicyWire) -> Self {
        match policy {
            PermissionPolicyWire::Workspace => Self::Workspace,
            PermissionPolicyWire::Unattended => Self::Unattended,
        }
    }
}

impl From<&TurnLimits> for TurnLimitsWire {
    fn from(limits: &TurnLimits) -> Self {
        Self {
            timeout_millis: limits.timeout_millis,
            max_turns: limits.max_turns,
            max_budget_usd_cents: limits.max_budget_usd_cents,
        }
    }
}

impl TurnLimitsWire {
    fn into_limits(self) -> Result<TurnLimits, WorkerError> {
        TurnLimits::new(
            self.timeout_millis,
            self.max_turns,
            self.max_budget_usd_cents,
        )
        .map_err(|error| task_config(error.to_string()))
    }
}

struct UniqueObject(Map<String, Value>);
struct UniqueValue(Value);

impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> de::Visitor<'de> for Visitor {
            type Value = UniqueValue;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON value with unique object keys")
            }

            fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Bool(value)))
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Number(value.into())))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Number(value.into())))
            }

            fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
                serde_json::Number::from_f64(value)
                    .map(Value::Number)
                    .map(UniqueValue)
                    .ok_or_else(|| E::custom("JSON number is not finite"))
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::String(value.to_owned())))
            }

            fn visit_borrowed_str<E: de::Error>(self, value: &'de str) -> Result<Self::Value, E> {
                self.visit_str(value)
            }

            fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::String(value)))
            }

            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }

            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }

            fn visit_seq<A: de::SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(UniqueValue(value)) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(UniqueValue(Value::Array(values)))
            }

            fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut object = Map::new();
                while let Some((key, UniqueValue(value))) = map.next_entry()? {
                    if object.contains_key(&key) {
                        return Err(de::Error::custom(format!("duplicate field `{key}`")));
                    }
                    object.insert(key, value);
                }
                Ok(UniqueValue(Value::Object(object)))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

impl<'de> Deserialize<'de> for UniqueObject {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> de::Visitor<'de> for Visitor {
            type Value = UniqueObject;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON object with unique keys")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut object = Map::new();
                while let Some((key, UniqueValue(value))) =
                    map.next_entry::<String, UniqueValue>()?
                {
                    if object.contains_key(&key) {
                        return Err(de::Error::custom(format!("duplicate field `{key}`")));
                    }
                    object.insert(key, value);
                }
                Ok(UniqueObject(object))
            }
        }

        deserializer.deserialize_map(Visitor)
    }
}

fn deserialize_unique_object<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: DeserializeOwned,
    D: Deserializer<'de>,
{
    let UniqueObject(object) = UniqueObject::deserialize(deserializer)?;
    T::deserialize(Value::Object(object)).map_err(de::Error::custom)
}

pub(crate) fn deserialize_unique_json<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Value, D::Error> {
    UniqueValue::deserialize(deserializer).map(|UniqueValue(value)| value)
}

fn is_valid_branch_name(value: &str) -> bool {
    if value.is_empty()
        || value.starts_with('-')
        || value.starts_with('/')
        || value.ends_with('/')
        || value.ends_with('.')
        || value.contains("..")
        || value.contains("//")
        || value.starts_with("refs/")
    {
        return false;
    }
    if value.chars().any(|character| {
        character.is_control()
            || character.is_whitespace()
            || matches!(
                character,
                '~' | '^' | ':' | '?' | '*' | '[' | '\\' | '@' | '{' | '}'
            )
    }) {
        return false;
    }
    value.split('/').all(|component| {
        !component.is_empty()
            && !component.ends_with(".lock")
            && !component.starts_with('.')
            && !component.ends_with('.')
    })
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_hex_component(value: &str, label: &str) -> Result<(), WorkerError> {
    if is_lower_hex(value, MAX_HEX_ID_BYTES) {
        Ok(())
    } else {
        Err(task_config(format!(
            "{label} must be {MAX_HEX_ID_BYTES} lowercase hexadecimal characters"
        )))
    }
}

fn validate_identity_component(value: &str, label: &str) -> Result<(), WorkerError> {
    if value.is_empty() || value.len() > MAX_IDENTITY_BYTES {
        return Err(task_config(format!(
            "{label} must be between 1 and {MAX_IDENTITY_BYTES} bytes"
        )));
    }
    if value
        .chars()
        .any(|character| character.is_control() || matches!(character, '<' | '>'))
    {
        return Err(task_config(format!(
            "{label} contains a forbidden character"
        )));
    }
    Ok(())
}

fn validate_optional_text(value: &str, max_bytes: usize, label: &str) -> Result<(), WorkerError> {
    if value.is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(task_config(format!(
            "{label} is empty, too long, or contains a control character"
        )));
    }
    Ok(())
}

fn validate_pinned_worker(name: &str) -> Result<(), WorkerError> {
    if name.is_empty() || name.chars().any(char::is_control) {
        return Err(task_config(
            "pinned worker must be a non-empty name without control characters",
        ));
    }
    Ok(())
}

fn task_config(message: impl Into<std::borrow::Cow<'static, str>>) -> WorkerError {
    WorkerError::task("TASK_CONFIG_INVALID", message)
}

// ce7f62f:src/agent/mod.rs.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentKind {
    Codex,
    Claude,
    Cursor,
    Opencode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionPolicy {
    Workspace,
    Unattended,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnLimits {
    pub timeout_millis: u64,
    pub max_turns: Option<u32>,
    pub max_budget_usd_cents: Option<u64>,
}

/// Fixed public codes only: neither parser errors nor agent-controlled keys
/// may become a diagnostic (both can contain credentials).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResultParseReason {
    #[serde(rename = "no_result_json")]
    NoResultJson,
    #[serde(rename = "empty_output")]
    EmptyOutput,
    #[serde(rename = "truncated")]
    Truncated,
    #[serde(rename = "schema_mismatch:status")]
    Status,
    #[serde(rename = "schema_mismatch:summary")]
    Summary,
    #[serde(rename = "schema_mismatch:questions")]
    Questions,
    #[serde(rename = "schema_mismatch:files_changed")]
    FilesChanged,
    #[serde(rename = "schema_mismatch:checks")]
    Checks,
    #[serde(rename = "schema_mismatch:unknown_field")]
    UnknownField,
}

/// One question an agent ends a turn with. `options` carries the machine
/// readable answers the agent will accept, so an orchestrator can pick a key
/// instead of parsing prose; an empty list means the question is open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Question {
    text: String,
    options: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportedCheckStatus {
    Pass,
    Fail,
    NotRun,
    Error,
}

/// A check the agent claimed it ran. mac-worker stores and displays it as
/// agent-reported only; a `pass` is never laptop-verified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportedCheck {
    name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    command: String,
    status: ReportedCheckStatus,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    detail: String,
}

impl AgentKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Cursor => "cursor",
            Self::Opencode => "opencode",
        }
    }

    pub fn from_name(value: &str) -> Option<Self> {
        match value {
            "codex" => Some(Self::Codex),
            "claude" => Some(Self::Claude),
            "cursor" => Some(Self::Cursor),
            "opencode" => Some(Self::Opencode),
            _ => None,
        }
    }
}

impl PermissionPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Workspace => "workspace",
            Self::Unattended => "unattended",
        }
    }
}

impl TurnLimits {
    pub const MAX_TIMEOUT_MILLIS: u64 = 24 * 60 * 60 * 1000;
    pub const MAX_BUDGET_USD_CENTS: u64 = 100_000;

    pub fn new(
        timeout_millis: u64,
        max_turns: Option<u32>,
        max_budget_usd_cents: Option<u64>,
    ) -> Result<Self, AdapterError> {
        let limits = Self {
            timeout_millis,
            max_turns,
            max_budget_usd_cents,
        };
        limits.validate()?;
        Ok(limits)
    }

    pub fn validate(&self) -> Result<(), AdapterError> {
        if self.timeout_millis == 0 {
            return Err(AdapterError::new("timeout must be greater than zero"));
        }
        if self.timeout_millis > Self::MAX_TIMEOUT_MILLIS {
            return Err(AdapterError::new("timeout must not exceed 24 hours"));
        }
        if self.max_turns == Some(0) {
            return Err(AdapterError::new("max turns must be greater than zero"));
        }
        if self
            .max_budget_usd_cents
            .is_some_and(|budget| budget > Self::MAX_BUDGET_USD_CENTS)
        {
            return Err(AdapterError::new("budget must not exceed 100000 cents"));
        }
        Ok(())
    }
}

pub(crate) fn validate_effort(effort: &str) -> Result<(), AdapterError> {
    if effort.is_empty() {
        return Err(AdapterError::new("effort must not be empty"));
    }
    if effort.len() > MAX_EFFORT_BYTES {
        return Err(AdapterError::new("effort must not exceed 32 bytes"));
    }
    if !effort
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(AdapterError::new(
            "effort must contain only ASCII letters, digits, '-', or '_'",
        ));
    }
    Ok(())
}

impl Question {
    pub fn new(text: impl Into<String>, options: Vec<String>) -> Self {
        Self {
            text: text.into(),
            options,
        }
    }

    pub fn open(text: impl Into<String>) -> Self {
        Self::new(text, Vec::new())
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn options(&self) -> &[String] {
        &self.options
    }
}

impl From<String> for Question {
    fn from(text: String) -> Self {
        Self::open(text)
    }
}

impl From<&str> for Question {
    fn from(text: &str) -> Self {
        Self::open(text)
    }
}

impl serde::Serialize for Question {
    /// A question without options stays a plain string on the wire, so records
    /// written before options existed round-trip unchanged.
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.options.is_empty() {
            return serializer.serialize_str(&self.text);
        }
        use serde::ser::SerializeStruct;
        let mut record = serializer.serialize_struct("Question", 2)?;
        record.serialize_field("text", &self.text)?;
        record.serialize_field("options", &self.options)?;
        record.end()
    }
}

impl<'de> serde::Deserialize<'de> for Question {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged, deny_unknown_fields)]
        enum Wire {
            Text(String),
            Structured {
                text: String,
                #[serde(default)]
                options: Vec<String>,
            },
        }
        Ok(match Wire::deserialize(deserializer)? {
            Wire::Text(text) => Self::open(text),
            Wire::Structured { text, options } => Self::new(text, options),
        })
    }
}

impl ReportedCheck {
    pub fn new(
        name: impl Into<String>,
        command: impl Into<String>,
        status: ReportedCheckStatus,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            command: command.into(),
            status,
            detail: detail.into(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn command(&self) -> &str {
        &self.command
    }

    pub fn status(&self) -> ReportedCheckStatus {
        self.status
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }

    pub fn source(&self) -> &'static str {
        AGENT_REPORTED_CHECK_SOURCE
    }
}

const MAX_REPORTED_CHECKS: usize = 32;
const MAX_CHECK_NAME_BYTES: usize = 128;
const MAX_CHECK_COMMAND_BYTES: usize = 256;
const MAX_CHECK_DETAIL_BYTES: usize = 1024;
const AGENT_REPORTED_CHECK_SOURCE: &str = "agent_reported";

// ce7f62f:src/agent/identity.rs.

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentIdentity {
    pub executable: String,
    pub version: Option<String>,
    pub version_observation: VersionObservation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VersionObservation {
    Observed,
    Unavailable,
    TimedOut,
    OutputLimit,
}

impl AgentIdentity {
    pub(crate) fn redacted(&self, boundary: &RedactionBoundary) -> Self {
        // Keep the useful path below the account home while hiding its owner.
        let relative = self
            .executable
            .strip_prefix("~/")
            .map(str::to_owned)
            .or_else(|| {
                std::env::var_os("HOME")
                    .and_then(|home| std::fs::canonicalize(home).ok())
                    .and_then(|home| {
                        std::path::Path::new(&self.executable)
                            .strip_prefix(home)
                            .ok()
                            .map(|p| p.to_string_lossy().into_owned())
                    })
            });
        // Redact the suffix before adding the public home marker; the generic
        // boundary deliberately removes entire tilde-prefixed paths.
        let executable = relative.map_or_else(
            || boundary.text(&self.executable, 4096),
            |relative| format!("~/{}", boundary.text(&relative, 4094)),
        );
        Self {
            executable,
            version: self.version.as_ref().map(|v| boundary.text(v, 128)),
            version_observation: self.version_observation,
        }
    }
}

// ce7f62f:src/job.rs.

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProcessIdentity {
    pid: u32,
    start_time_micros: u64,
}

// ce7f62f:src/session_transfer/contracts.rs.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionAgent {
    Claude,
    Codex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SessionFormat {
    ClaudeJsonlV1,
    CodexRolloutV1,
}

impl SessionAgent {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
        }
    }
    pub fn agent_kind(self) -> AgentKind {
        match self {
            Self::Claude => AgentKind::Claude,
            Self::Codex => AgentKind::Codex,
        }
    }
    pub fn from_agent_kind(kind: AgentKind) -> Option<Self> {
        match kind {
            AgentKind::Claude => Some(Self::Claude),
            AgentKind::Codex => Some(Self::Codex),
            _ => None,
        }
    }
    pub fn format(self) -> SessionFormat {
        match self {
            Self::Claude => SessionFormat::ClaudeJsonlV1,
            Self::Codex => SessionFormat::CodexRolloutV1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "SessionImportMetaWire")]
pub struct SessionImportMeta {
    agent: SessionAgent,
    format: SessionFormat,
    package_oid: String,
    source_agent_version: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionImportMetaWire {
    agent: SessionAgent,
    format: SessionFormat,
    package_oid: String,
    source_agent_version: String,
}

impl TryFrom<SessionImportMetaWire> for SessionImportMeta {
    type Error = WorkerError;
    fn try_from(wire: SessionImportMetaWire) -> Result<Self, Self::Error> {
        let meta = Self {
            agent: wire.agent,
            format: wire.format,
            package_oid: wire.package_oid,
            source_agent_version: wire.source_agent_version,
        };
        meta.validate()?;
        Ok(meta)
    }
}

pub(crate) fn supported_agent_version(version: &str, scrubber: &Scrubber) -> bool {
    if version.len() > 64 || version.contains('/') {
        return false;
    }
    let numeric = if let Some((numeric, suffix)) = version.split_once(['-', '+']) {
        if suffix.is_empty()
            || suffix.len() > 32
            || !suffix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.')
        {
            return false;
        }
        numeric
    } else {
        version
    };
    let mut components = 0;
    if !numeric.split('.').all(|part| {
        components += 1;
        !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit())
    }) || !(2..=4).contains(&components)
    {
        return false;
    }
    let json = serde_json::to_vec(version).expect("version string serializes");
    scrubber
        .scrub_line(&json)
        .is_ok_and(|line| line.replacements == 0)
}

impl SessionImportMeta {
    pub fn new(
        agent: SessionAgent,
        package_oid: impl Into<String>,
        source_agent_version: impl Into<String>,
    ) -> Result<Self, WorkerError> {
        let meta = Self {
            agent,
            format: agent.format(),
            package_oid: package_oid.into(),
            source_agent_version: source_agent_version.into(),
        };
        meta.validate()?;
        Ok(meta)
    }
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.format != self.agent.format()
            || ![40, 64].contains(&self.package_oid.len())
            || !self
                .package_oid
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || !supported_agent_version(&self.source_agent_version, &Scrubber::new(vec![]))
        {
            return Err(session_error(
                "TASK_CONFIG_INVALID",
                "invalid session import metadata",
            ));
        }
        Ok(())
    }
    pub fn agent(&self) -> SessionAgent {
        self.agent
    }
    pub fn format(&self) -> SessionFormat {
        self.format
    }
    pub fn package_oid(&self) -> &str {
        &self.package_oid
    }
    pub fn source_agent_version(&self) -> &str {
        &self.source_agent_version
    }
}

// ce7f62f:src/prepared_followup.rs.

/// One persisted follow-up intent, allocated once by the controller caller.
///
/// `attached` stays a call-time execution mode on
/// [`TaskClient::say_prepared`](TaskClient::say_prepared);
/// everything needed to reproduce the turn bit-for-bit is frozen here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedFollowup {
    #[serde(default, skip_serializing_if = "is_false")]
    auto_continue: bool,
    expected: LocalTaskRecord,
    turn_id: TurnId,
    turn_number: u32,
    created_at_millis: u64,
    message: String,
    composed_prompt: String,
    base_oid: BaseOid,
    agent: String,
    model: Option<String>,
    worker: String,
    max_followups: u32,
}

impl PreparedFollowup {
    pub fn auto_continue(&self) -> bool {
        self.auto_continue
    }

    pub fn expected(&self) -> &LocalTaskRecord {
        &self.expected
    }

    pub fn task_id(&self) -> TaskId {
        self.expected.meta().task_id()
    }
}

// ce7f62f:src/prepared_submit.rs.

/// Frozen payload for one submit. No MacBook paths. `run_id` is optional:
/// ordinary remote tasks omit it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenSubmitBody {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_import: Option<SessionImportMeta>,
    /// Explicit override only: older controllers reject unknown fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub questions: Option<QuestionsPolicy>,
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

fn default_true() -> bool {
    true
}

// ce7f62f:src/dag.rs.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DagNodeState {
    Waiting,
    Claimed,
    Submitted,
    Blocked,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub questions: Option<QuestionsPolicy>,
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

// ce7f62f:src/controller/batch.rs.

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

// ce7f62f:src/controller/read.rs.

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerReadReply<T> {
    protocol_version: u32,
    command: String,
    request_id: String,
    payload_sha256: String,
    result: T,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerTaskResult {
    task_id: TaskId,
    status: TaskStatus,
    branch: String,
    fetch: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    residual: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivery: Option<OriginDelivery>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    deliveries: Vec<OriginDelivery>,
    // Accept the short-lived wave-2 field on reads, but never emit it: the
    // legacy result decoder is strict. Status is the compatible warning source.
    #[serde(default, skip_serializing)]
    warnings: Vec<String>,
}

// ce7f62f:src/controller/envelope.rs.

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperationOutcome {
    Acknowledged,
    Rejected { code: String },
}

/// Laptop transport-cache retry handle. This is not a second task/queue store.
/// Retrying an existing envelope must not re-freeze a changed body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationEnvelope {
    request_id: String,
    payload_sha256: String,
    command: String,
    body: Value,
    created_at_millis: u64,
    // Explicit null fields distinguish new pending envelopes from legacy files.
    #[serde(default)]
    settled_at_millis: Option<u64>,
    #[serde(default)]
    outcome: Option<OperationOutcome>,
}

// ce7f62f:src/task_store.rs.

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskStatusResponse {
    protocol_version: u32,
    status: TaskStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivery: Option<OriginDelivery>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    deliveries: Vec<OriginDelivery>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskCloseRequest {
    protocol_version: u32,
    project_id: String,
    task_id: TaskId,
    discard: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskCloseResponse {
    protocol_version: u32,
    status: TaskStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivery: Option<OriginDelivery>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    deliveries: Vec<OriginDelivery>,
}

// ce7f62f:src/controller/protocol.rs (incoming request wire DTO).

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireRequest {
    protocol_version: u32,
    request_id: String,
    #[serde(default, rename = "payload_sha256")]
    _ignored_digest: Option<String>,
    command: String,
    body: Value,
}

// ce7f62f:src/controller/events/contracts.rs.

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

/// Closed public-code catalog; unknown values collapse to TURN_FAILED.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
pub struct SafeCode(String);

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

fn known_task_state(state: &str) -> bool {
    matches!(
        state,
        "queued" | "active" | "open" | "closed" | "abandoned" | "lost"
    )
}

pub(crate) fn invalid(message: &str) -> WorkerError {
    WorkerError::Protocol(format!("{CONTROLLER_EVENTS_INVALID}: {message}"))
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

impl TaskFacts {
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

// ce7f62f:src/redaction.rs.
mod redaction {
    use std::{fmt, path::PathBuf};

    use super::{
        MAX_CHECK_COMMAND_BYTES, MAX_CHECK_DETAIL_BYTES, MAX_CHECK_NAME_BYTES, MAX_REPORTED_CHECKS,
        Question, ReportedCheck,
    };

    pub const MAX_SUMMARY_BYTES: usize = 4 * 1024;
    pub const MAX_QUESTION_BYTES: usize = 1024;
    pub const MAX_QUESTION_COUNT: usize = 16;
    pub const MAX_QUESTION_OPTION_BYTES: usize = 256;
    pub const MAX_QUESTION_OPTION_COUNT: usize = 8;
    pub const MAX_CHANGED_FILE_BYTES: usize = 256;
    pub const MAX_CHANGED_FILE_COUNT: usize = 256;
    pub const MAX_FAILURE_REASON_BYTES: usize = 1024;
    pub const MAX_DIFF_STAT_BYTES: usize = 4 * 1024;
    pub const MAX_TITLE_BYTES: usize = 120;

    const PATH_PLACEHOLDER: &str = "[path]";
    const TOKEN_PLACEHOLDER: &str = "[token]";
    const UNSETTLED_PLACEHOLDER: &str = "[redacted]";
    const MAX_SETTLE_PASSES: usize = 4;
    const MIN_HEX_TOKEN: usize = 32;
    const MIN_BASE64_TOKEN: usize = 32;
    const MIN_SK_TOKEN: usize = 8;
    const COMMON_PATH_PREFIXES: &[&str] = &[
        "/Users/",
        "/home/",
        "/private/var/folders/",
        "/var/folders/",
        "/private/tmp/",
        "/tmp/",
    ];

    #[derive(Clone)]
    pub struct RedactionBoundary {
        homes: Vec<String>,
        secrets: Vec<String>,
    }

    impl fmt::Debug for RedactionBoundary {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("RedactionBoundary")
                .field("home_count", &self.homes.len())
                .field("secret_count", &self.secrets.len())
                .finish()
        }
    }

    impl RedactionBoundary {
        pub fn from_env() -> Self {
            Self::for_home(std::env::var_os("HOME").map(PathBuf::from))
        }

        pub fn new(home: impl Into<PathBuf>) -> Self {
            Self::for_home(Some(home.into()))
        }

        fn for_home(home: Option<PathBuf>) -> Self {
            let mut homes = COMMON_PATH_PREFIXES
                .iter()
                .map(|prefix| (*prefix).to_owned())
                .collect::<Vec<_>>();
            if let Some(home) = home {
                if let Some(raw) = home.to_str() {
                    push_unique(&mut homes, raw.to_owned());
                }
                if let Ok(canonical) = std::fs::canonicalize(&home)
                    && let Some(raw) = canonical.to_str()
                {
                    push_unique(&mut homes, raw.to_owned());
                }
            }
            homes.sort_by_key(|home| std::cmp::Reverse(home.len()));
            Self {
                homes,
                secrets: Vec::new(),
            }
        }

        pub fn with_secrets<I, S>(mut self, secrets: I) -> Self
        where
            I: IntoIterator<Item = S>,
            S: AsRef<str>,
        {
            for secret in secrets {
                push_unique(&mut self.secrets, escape_controls(secret.as_ref()));
            }
            self.secrets
                .sort_by_key(|secret| std::cmp::Reverse(secret.len()));
            self
        }

        pub fn summary(&self, input: &str) -> String {
            self.text(input, MAX_SUMMARY_BYTES)
        }

        pub fn question(&self, input: &str) -> String {
            self.text(input, MAX_QUESTION_BYTES)
        }

        pub fn changed_file(&self, input: &str) -> String {
            self.text(input, MAX_CHANGED_FILE_BYTES)
        }

        pub fn failure_reason(&self, input: &str) -> String {
            self.text(input, MAX_FAILURE_REASON_BYTES)
        }

        pub fn diff_stat(&self, input: &str) -> String {
            self.text(input, MAX_DIFF_STAT_BYTES)
        }

        pub fn title(&self, input: &str) -> String {
            self.text(input, MAX_TITLE_BYTES)
        }

        pub fn question_option(&self, input: &str) -> String {
            self.text(input, MAX_QUESTION_OPTION_BYTES)
        }

        pub fn questions<I, Q>(&self, items: I) -> Vec<Question>
        where
            I: IntoIterator<Item = Q>,
            Q: std::borrow::Borrow<Question>,
        {
            items
                .into_iter()
                .map(|item| {
                    let question = item.borrow();
                    Question::new(
                        self.question(question.text()),
                        question
                            .options()
                            .iter()
                            .map(|option| self.question_option(option))
                            .take(MAX_QUESTION_OPTION_COUNT)
                            .collect(),
                    )
                })
                .take(MAX_QUESTION_COUNT)
                .collect()
        }

        pub fn changed_files<I, S>(&self, items: I) -> Vec<String>
        where
            I: IntoIterator<Item = S>,
            S: AsRef<str>,
        {
            items
                .into_iter()
                .map(|item| self.changed_file(item.as_ref()))
                .take(MAX_CHANGED_FILE_COUNT)
                .collect()
        }

        pub fn reported_checks<I>(&self, items: I) -> Vec<ReportedCheck>
        where
            I: IntoIterator<Item = ReportedCheck>,
        {
            items
                .into_iter()
                .map(|check| {
                    ReportedCheck::new(
                        self.text(check.name(), MAX_CHECK_NAME_BYTES),
                        self.text(check.command(), MAX_CHECK_COMMAND_BYTES),
                        check.status(),
                        self.text(check.detail(), MAX_CHECK_DETAIL_BYTES),
                    )
                })
                .take(MAX_REPORTED_CHECKS)
                .collect()
        }

        /// Escape, redact and bound `input`. The result is a fixed point:
        /// `text(text(x)) == text(x)`. Task records are decoded through this
        /// boundary again and must re-encode byte for byte, so a field that
        /// changes on a second pass makes its record unreadable.
        pub fn text(&self, input: &str, max_bytes: usize) -> String {
            let escaped = escape_controls(input);
            let mut text = truncate_bytes(&self.redact(&escaped), max_bytes);
            // Truncation can leave a new token at the end: a long run whose
            // `=x` suffix kept it from matching, or a lone `~`. Settle again.
            for _ in 0..MAX_SETTLE_PASSES {
                let again = truncate_bytes(&self.redact(&text), max_bytes);
                if again == text {
                    return text;
                }
                text = again;
            }
            // Not reached by any known input; this marker is itself stable.
            truncate_bytes(UNSETTLED_PLACEHOLDER, max_bytes)
        }

        fn redact(&self, input: &str) -> String {
            let input = self.secrets.iter().fold(input.to_owned(), |input, secret| {
                input.replace(secret, TOKEN_PLACEHOLDER)
            });
            redact_tokens(&redact_home_paths(
                &self.redact_tilde_paths(&input),
                &self.homes,
            ))
        }

        fn redact_tilde_paths(&self, input: &str) -> String {
            let mut output = String::with_capacity(input.len());
            let mut rest = input;
            while !rest.is_empty() {
                if let Some(start) = find_tilde_path(rest) {
                    output.push_str(&rest[..start]);
                    output.push_str(PATH_PLACEHOLDER);
                    rest = skip_path_token(&rest[start..]);
                } else {
                    output.push_str(rest);
                    break;
                }
            }
            output
        }
    }

    fn push_unique(values: &mut Vec<String>, value: String) {
        if !value.is_empty() && !values.iter().any(|existing| existing == &value) {
            values.push(value);
        }
    }

    fn redact_home_paths(input: &str, homes: &[String]) -> String {
        if homes.is_empty() {
            return input.to_owned();
        }
        let mut output = String::with_capacity(input.len());
        let mut rest = input;
        while !rest.is_empty() {
            let Some((start, home)) = next_home_path(rest, homes) else {
                output.push_str(rest);
                break;
            };
            output.push_str(&rest[..start]);
            output.push_str(PATH_PLACEHOLDER);
            rest = skip_path_token(&rest[start + home.len()..]);
            if rest.starts_with(home) {
                // Avoid a tight loop if a home path is a prefix of itself after skip.
                rest = &rest[home.chars().next().map(char::len_utf8).unwrap_or(1)..];
            }
        }
        output
    }

    fn next_home_path<'a>(input: &'a str, homes: &[String]) -> Option<(usize, &'a str)> {
        let mut found: Option<(usize, &str)> = None;
        for home in homes {
            let mut search = input;
            let mut offset = 0;
            while let Some(local) = search.find(home.as_str()) {
                let start = offset + local;
                if is_path_token_start(input, start) {
                    let candidate = &input[start..start + home.len()];
                    if found.is_none_or(|(best, current)| {
                        start < best || (start == best && candidate.len() > current.len())
                    }) {
                        found = Some((start, candidate));
                    }
                }
                let advance = local + home.len();
                search = &search[advance..];
                offset += advance;
            }
        }
        found
    }

    fn find_tilde_path(input: &str) -> Option<usize> {
        input.char_indices().find_map(|(index, character)| {
            (character == '~'
                && is_path_token_start(input, index)
                && matches!(input[index + 1..].chars().next(), None | Some('/')))
            .then_some(index)
        })
    }

    fn is_path_token_start(input: &str, index: usize) -> bool {
        index == 0
            || input[..index].chars().next_back().is_some_and(|character| {
                character.is_whitespace()
                    || matches!(character, '"' | '\'' | '=' | ':' | ',' | '(' | '[')
            })
    }

    fn skip_path_token(input: &str) -> &str {
        let end = input
            .char_indices()
            .find(|(_, character)| {
                character.is_whitespace() || matches!(character, '"' | '\'' | ',' | ')' | ']' | ';')
            })
            .map(|(index, _)| index)
            .unwrap_or(input.len());
        &input[end..]
    }

    fn redact_tokens(input: &str) -> String {
        let mut output = String::with_capacity(input.len());
        let mut rest = input;
        while !rest.is_empty() {
            if let Some(stripped) = strip_prefix_ignore_ascii_case(rest, "Bearer")
                && stripped.starts_with(char::is_whitespace)
            {
                output.push_str("Bearer ");
                let value = stripped.trim_start_matches(char::is_whitespace);
                // A value that is already a placeholder must survive whole:
                // `skip_token_run` stops at `]`, so re-redacting `[token]` would
                // leave one more `]` per pass, and a stored task record would
                // never read back canonical.
                let value = [TOKEN_PLACEHOLDER, PATH_PLACEHOLDER]
                    .iter()
                    .find_map(|placeholder| value.strip_prefix(placeholder))
                    .unwrap_or(value);
                rest = skip_token_run(value);
                output.push_str(TOKEN_PLACEHOLDER);
                continue;
            }
            if rest.starts_with("sk-") && sk_token_len(&rest[3..]) >= MIN_SK_TOKEN {
                output.push_str(TOKEN_PLACEHOLDER);
                rest = skip_token_run(&rest[3..]);
                continue;
            }
            let ascii_run = ascii_run_len(rest);
            if ascii_run >= MIN_HEX_TOKEN
                && rest.as_bytes()[..ascii_run]
                    .iter()
                    .all(u8::is_ascii_hexdigit)
            {
                output.push_str(TOKEN_PLACEHOLDER);
                rest = &rest[ascii_run..];
                continue;
            }
            if let Some(len) = base64_token_len(rest) {
                output.push_str(TOKEN_PLACEHOLDER);
                rest = &rest[len..];
                continue;
            }
            let next = rest.chars().next().expect("non-empty remainder");
            output.push(next);
            rest = &rest[next.len_utf8()..];
        }
        output
    }

    fn strip_prefix_ignore_ascii_case<'a>(input: &'a str, prefix: &str) -> Option<&'a str> {
        if input.is_char_boundary(prefix.len())
            && input[..prefix.len()].eq_ignore_ascii_case(prefix)
        {
            Some(&input[prefix.len()..])
        } else {
            None
        }
    }

    fn ascii_run_len(input: &str) -> usize {
        input
            .as_bytes()
            .iter()
            .take_while(|byte| byte.is_ascii_alphanumeric())
            .count()
    }

    fn sk_token_len(input: &str) -> usize {
        input
            .as_bytes()
            .iter()
            .take_while(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'-'))
            .count()
    }

    fn skip_token_run(input: &str) -> &str {
        let len = input
            .as_bytes()
            .iter()
            .take_while(|byte| {
                byte.is_ascii_graphic() && !matches!(*byte, b'"' | b'\'' | b',' | b')' | b']')
            })
            .count();
        &input[len..]
    }

    fn base64_token_len(input: &str) -> Option<usize> {
        let bytes = input.as_bytes();
        let mut len = 0;
        while len < bytes.len() && is_base64_body(bytes[len]) {
            len += 1;
        }
        let mut padded = len;
        while padded < bytes.len() && bytes[padded] == b'=' && padded - len < 2 {
            padded += 1;
        }
        if len >= MIN_BASE64_TOKEN
            && (padded == bytes.len()
                || !bytes[padded].is_ascii_alphanumeric()
                    && bytes[padded] != b'+'
                    && bytes[padded] != b'/')
        {
            Some(padded)
        } else {
            None
        }
    }

    fn is_base64_body(byte: u8) -> bool {
        byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/')
    }

    fn escape_controls(input: &str) -> String {
        let mut escaped = String::with_capacity(input.len());
        for character in input.chars() {
            match character {
                '\n' => escaped.push_str("\\n"),
                '\r' => escaped.push_str("\\r"),
                '\t' => escaped.push_str("\\t"),
                other if other.is_control() => {
                    escaped.push_str(&format!("\\u{{{:04x}}}", u32::from(other)));
                }
                other => escaped.push(other),
            }
        }
        escaped
    }

    fn truncate_bytes(input: &str, max_bytes: usize) -> String {
        if input.len() <= max_bytes {
            return input.to_owned();
        }
        let mut end = max_bytes;
        while end > 0 && !input.is_char_boundary(end) {
            end -= 1;
        }
        input[..end].to_owned()
    }
}

// ce7f62f:src/session_transfer/scrub.rs.
mod scrub {
    use std::collections::{BTreeMap, HashMap, VecDeque};

    use super::WorkerError;

    /// Replacement text for secret spans.
    pub const SCRUBBED: &str = "[scrubbed]";

    /// Structure-aware session secret scrubber.
    pub struct Scrubber {
        exact: Vec<ExactNode>,
    }

    #[derive(Default)]
    struct ExactNode {
        edges: BTreeMap<u8, usize>,
        failure: usize,
        longest: usize,
    }

    /// A JSON line and the number of replaced secret spans.
    pub struct ScrubbedLine {
        /// Encoded JSON, preserving untouched bytes.
        pub bytes: Vec<u8>,
        /// Number of secret spans replaced.
        pub replacements: u32,
    }

    impl Scrubber {
        /// Build a scrubber, ignoring exact secrets shorter than eight bytes.
        pub fn new(exact_secrets: Vec<String>) -> Self {
            // A reversed failure-link trie finds the longest exact match at each
            // starting byte in one backwards pass, including overlapping secrets.
            let mut exact = vec![ExactNode::default()];
            for secret in exact_secrets.into_iter().filter(|secret| secret.len() >= 8) {
                let mut state = 0;
                for byte in secret.bytes().rev() {
                    state = if let Some(&next) = exact[state].edges.get(&byte) {
                        next
                    } else {
                        let next = exact.len();
                        exact.push(ExactNode::default());
                        exact[state].edges.insert(byte, next);
                        next
                    };
                }
                exact[state].longest = secret.len();
            }
            let mut queue: VecDeque<usize> = exact[0].edges.values().copied().collect();
            while let Some(state) = queue.pop_front() {
                let edges: Vec<_> = exact[state].edges.iter().map(|(&b, &s)| (b, s)).collect();
                for (byte, next) in edges {
                    let mut failure = exact[state].failure;
                    while failure != 0 && !exact[failure].edges.contains_key(&byte) {
                        failure = exact[failure].failure;
                    }
                    exact[next].failure = exact[failure].edges.get(&byte).copied().unwrap_or(0);
                    exact[next].longest =
                        exact[next].longest.max(exact[exact[next].failure].longest);
                    queue.push_back(next);
                }
            }
            Self { exact }
        }

        /// Scrub JSON string values without changing object keys.
        pub fn scrub_line(&self, line: &[u8]) -> Result<ScrubbedLine, WorkerError> {
            // Validate first; error messages must never echo transcript contents.
            serde_json::from_slice::<serde_json::Value>(line).map_err(|_| unreadable())?;
            let mut bytes = Vec::with_capacity(line.len());
            let mut replacements = 0;
            let mut copied = 0;
            let mut cursor = 0;
            while cursor < line.len() {
                if line[cursor] != b'"' {
                    cursor += 1;
                    continue;
                }
                let start = cursor;
                cursor += 1;
                while line[cursor] != b'"' {
                    if line[cursor] == b'\\' {
                        cursor += 1;
                    }
                    cursor += 1;
                }
                cursor += 1;
                let mut after = cursor;
                while after < line.len() && line[after].is_ascii_whitespace() {
                    after += 1;
                }
                if line.get(after) == Some(&b':') {
                    continue;
                }
                let value: String =
                    serde_json::from_slice(&line[start..cursor]).map_err(|_| unreadable())?;
                let (scrubbed, count) = self.scrub_text(&value);
                if count != 0 {
                    bytes.extend_from_slice(&line[copied..start]);
                    let scrubbed = std::str::from_utf8(&scrubbed).map_err(|_| unreadable())?;
                    bytes.extend_from_slice(
                        &serde_json::to_vec(scrubbed).map_err(|_| unreadable())?,
                    );
                    copied = cursor;
                    replacements += count;
                }
            }
            bytes.extend_from_slice(&line[copied..]);
            Ok(ScrubbedLine {
                bytes,
                replacements,
            })
        }

        fn scrub_text(&self, text: &str) -> (Vec<u8>, u32) {
            let input = text.as_bytes();
            let mut spans = vec![0; input.len()];
            if self.exact.len() > 1 {
                let mut state = 0;
                for index in (0..input.len()).rev() {
                    let byte = input[index];
                    while state != 0 && !self.exact[state].edges.contains_key(&byte) {
                        state = self.exact[state].failure;
                    }
                    state = self.exact[state].edges.get(&byte).copied().unwrap_or(0);
                    spans[index] = self.exact[state].longest;
                }
            }
            pem_spans(input, &mut spans);
            let mut output = Vec::new();
            let mut count = 0;
            let mut copied = 0;
            let mut index = 0;
            while index < input.len() {
                let (pattern_length, keep) = if index == 0 || !word(input[index - 1]) {
                    token_span(&input[index..])
                } else {
                    (0, 0)
                };
                let length = spans[index].max(pattern_length);
                if length == 0 {
                    index += 1;
                    continue;
                }
                output.extend_from_slice(&input[copied..index]);
                // Exact secrets win ties: an exact Bearer secret is removed whole.
                if pattern_length > spans[index] {
                    output.extend_from_slice(&input[index..index + keep]);
                }
                output.extend_from_slice(SCRUBBED.as_bytes());
                count += 1;
                index += length;
                copied = index;
            }
            if count != 0 {
                output.extend_from_slice(&input[copied..]);
            }
            (output, count)
        }
    }

    fn unreadable() -> WorkerError {
        WorkerError::task("SESSION_UNREADABLE", "session line is not valid JSON")
    }

    fn word(byte: u8) -> bool {
        byte.is_ascii_alphanumeric() || byte == b'_'
    }

    type TokenPattern<'a> = (&'a [u8], usize, fn(u8) -> bool, usize);

    fn token_span(input: &[u8]) -> (usize, usize) {
        let (prefix, minimum, allowed, keep): TokenPattern<'_> = match input[0] {
            b'B' if input.starts_with(b"Bearer ") => (
                b"Bearer ",
                16,
                |b| b.is_ascii_alphanumeric() || b"._~+/=-".contains(&b),
                7,
            ),
            b's' if input.starts_with(b"sk-ant-") => (b"sk-ant-", 16, |b| word(b) || b == b'-', 0),
            b's' if input.starts_with(b"sk-") => (b"sk-", 20, |b| word(b) || b == b'-', 0),
            b'g' if input.starts_with(b"github_pat_") => (b"github_pat_", 40, word, 0),
            b'g' if input.len() >= 4
                && &input[..2] == b"gh"
                && b"posur".contains(&input[2])
                && input[3] == b'_' =>
            {
                (&input[..4], 30, |b| b.is_ascii_alphanumeric(), 0)
            }
            b'x' if input.len() >= 5
                && &input[..3] == b"xox"
                && b"abprs".contains(&input[3])
                && input[4] == b'-' =>
            {
                (
                    &input[..5],
                    10,
                    |b| b.is_ascii_alphanumeric() || b == b'-',
                    0,
                )
            }
            b'A' if input.starts_with(b"AKIA") && input.len() >= 20 => {
                if input[4..20]
                    .iter()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
                    && !input.get(20).is_some_and(u8::is_ascii_alphanumeric)
                {
                    return (20, 0);
                }
                return (0, 0);
            }
            _ => return (0, 0),
        };
        let length = input[prefix.len()..]
            .iter()
            .take_while(|&&b| allowed(b))
            .count();
        if length >= minimum {
            (prefix.len() + length, keep)
        } else {
            (0, 0)
        }
    }

    fn pem_spans(input: &[u8], spans: &mut [usize]) {
        let mut pending: HashMap<&[u8], usize> = HashMap::new();
        let mut index = 0;
        while index < input.len() {
            let (prefix, begin) = if input[index..].starts_with(b"-----BEGIN ") {
                (11, true)
            } else if input[index..].starts_with(b"-----END ") {
                (9, false)
            } else {
                index += 1;
                continue;
            };
            let start = index;
            let label_start = index + prefix;
            index = label_start;
            // A delimiter label cannot contain a dash or a line break. Advancing
            // past the label prevents repeated scans of malformed long headers.
            while index < input.len() && !matches!(input[index], b'-' | b'\n' | b'\r') {
                index += 1;
            }
            let label = &input[label_start..index];
            if !(label == b"PRIVATE KEY" || label.ends_with(b" PRIVATE KEY"))
                || !input[index..].starts_with(b"-----")
            {
                continue;
            }
            index += 5;
            if begin {
                if start == 0 || !word(input[start - 1]) {
                    pending.entry(label).or_insert(start);
                }
            } else if let Some(start) = pending.remove(label) {
                spans[start] = spans[start].max(index - start);
            }
        }
    }
}

// ce7f62f:src/failure_receipt.rs.
mod failure_receipt {
    //! Fixed host-failure receipt vocabulary.
    //!
    //! Operators need to know which stage failed and which resources remain
    //! without reading a payload or a redacted supervisor line. The values are
    //! a closed set so the host message stays an opaque string that protocol-6
    //! readers already accept, and a laptop that does not know the grammar still
    //! sees `HOST_IO: host state operation failed …`.

    /// Stages a host I/O failure may name. Never free text.
    pub const STAGE_ADMISSION: &str = "admission";
    pub const STAGE_PREPARE: &str = "prepare";
    pub const STAGE_LAUNCH: &str = "launch";
    pub const STAGE_DRAIN: &str = "drain";
    pub const STAGE_PUBLISH: &str = "publish";
    pub const STAGE_CLEANUP: &str = "cleanup";
    pub const STAGE_LEASE_RELEASE: &str = "lease-release";
    pub const STAGE_CANCEL: &str = "cancel";
    pub const STAGE_FOLLOW: &str = "follow";

    pub const STAGES: &[&str] = &[
        STAGE_ADMISSION,
        STAGE_PREPARE,
        STAGE_LAUNCH,
        STAGE_DRAIN,
        STAGE_PUBLISH,
        STAGE_CLEANUP,
        STAGE_LEASE_RELEASE,
        STAGE_CANCEL,
        STAGE_FOLLOW,
    ];

    /// Resources a failed host operation may leave behind. Never free text.
    pub const RESIDUAL_LEASE: &str = "lease";
    pub const RESIDUAL_CLEANUP_TREE: &str = "cleanup-tree";
    pub const RESIDUAL_JOB_DIR: &str = "job-dir";
    pub const RESIDUAL_SUPERVISOR_LOCK: &str = "supervisor-lock";
    pub const RESIDUAL_TRANSFER_LOCK: &str = "transfer-lock";
    pub const RESIDUAL_SESSION: &str = "session";
    pub const RESIDUAL_WORKSPACE: &str = "workspace";

    pub const RESIDUALS: &[&str] = &[
        RESIDUAL_LEASE,
        RESIDUAL_CLEANUP_TREE,
        RESIDUAL_JOB_DIR,
        RESIDUAL_SUPERVISOR_LOCK,
        RESIDUAL_TRANSFER_LOCK,
        RESIDUAL_SESSION,
        RESIDUAL_WORKSPACE,
    ];

    const HOST_IO_MESSAGE_PREFIX: &str = "host state operation failed";
    const HOST_IO_CODE: &str = "HOST_IO";

    /// Which stage failed and which vocabulary resources are still present.
    ///
    /// Residuals are unique and stored in [`RESIDUALS`] order so the wire form
    /// is stable across callers that observe the same leftover set.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct FailureReceipt {
        stage: &'static str,
        residual: Vec<&'static str>,
    }

    impl FailureReceipt {
        /// Builds a receipt from vocabulary constants. Anything outside the set is
        /// `None` so a caller can never put free text on the wire.
        pub fn new(stage: &str, residual: &[&str]) -> Option<Self> {
            let stage = intern_stage(stage)?;
            let residual = intern_residuals(residual)?;
            Some(Self { stage, residual })
        }

        pub fn stage(&self) -> &'static str {
            self.stage
        }

        pub fn residual(&self) -> &[&'static str] {
            &self.residual
        }

        /// Wire message body. Existing `HostControlError` readers treat this as
        /// an opaque string; protocol 6 does not change.
        pub fn host_message(&self) -> String {
            format!(
                "{HOST_IO_MESSAGE_PREFIX} [stage={} residual={}]",
                self.stage,
                self.residual.join(",")
            )
        }

        /// Operator-facing parenthetical: `(stage=cleanup, residual=lease,cleanup-tree)`.
        #[cfg(test)]
        pub fn render_parenthetical(&self) -> String {
            format!(
                "(stage={}, residual={})",
                self.stage,
                self.residual.join(",")
            )
        }

        /// `HOST_IO (stage=cleanup, residual=lease,cleanup-tree)`.
        #[cfg(test)]
        pub fn render_with_code(&self, code: &str) -> String {
            format!("{code} {}", self.render_parenthetical())
        }

        /// Parses the host message body. Unknown vocabulary is no receipt, never
        /// an error, so an old worker's free-form `HOST_IO` text stays opaque.
        pub fn parse_host_message(message: &str) -> Option<Self> {
            let rest = message.strip_prefix(HOST_IO_MESSAGE_PREFIX)?;
            let rest = rest.strip_prefix(" [stage=")?;
            let (stage, rest) = rest.split_once(" residual=")?;
            let residual = rest.strip_suffix(']')?;
            if residual.contains(' ') || residual.contains('[') {
                return None;
            }
            Self::new(stage, &split_residuals(residual)?)
        }

        /// Parses `HOST_IO: host state operation failed [stage=… residual=…]`.
        pub fn parse_protocol_message(message: &str) -> Option<Self> {
            let (code, detail) = message.split_once(": ")?;
            (code == HOST_IO_CODE)
                .then(|| Self::parse_host_message(detail))
                .flatten()
        }
    }

    fn intern_stage(stage: &str) -> Option<&'static str> {
        STAGES.iter().copied().find(|candidate| *candidate == stage)
    }

    fn intern_residual(residual: &str) -> Option<&'static str> {
        RESIDUALS
            .iter()
            .copied()
            .find(|candidate| *candidate == residual)
    }

    fn intern_residuals(residual: &[&str]) -> Option<Vec<&'static str>> {
        let mut interned = Vec::with_capacity(residual.len());
        for item in residual {
            let interned_item = intern_residual(item)?;
            if interned.contains(&interned_item) {
                return None;
            }
            interned.push(interned_item);
        }
        interned.sort_by_key(|item| residual_rank(item));
        Some(interned)
    }

    fn residual_rank(residual: &str) -> usize {
        RESIDUALS
            .iter()
            .position(|candidate| *candidate == residual)
            .unwrap_or(usize::MAX)
    }

    fn split_residuals(residual: &str) -> Option<Vec<&str>> {
        if residual.is_empty() {
            return Some(Vec::new());
        }
        Some(residual.split(',').collect())
    }
}

// ce7f62f:src/project.rs (origin validation called by TaskMeta/PushTarget).
mod project {
    use super::WorkerError;
    use url::Url;

    pub(crate) fn normalize_origin(origin: &str) -> Result<String, WorkerError> {
        if let Ok(mut url) = Url::parse(origin)
            && matches!(url.scheme(), "http" | "https" | "ssh")
        {
            let scheme = url.scheme().to_ascii_lowercase();
            url.set_scheme(&scheme).map_err(|_| invalid_origin())?;
            if let Some(host) = url.host_str() {
                url.set_host(Some(&host.to_ascii_lowercase()))
                    .map_err(|_| invalid_origin())?;
            }
            url.set_username("").map_err(|_| invalid_origin())?;
            url.set_password(None).map_err(|_| invalid_origin())?;
            url.set_query(None);
            url.set_fragment(None);
            return Ok(url.to_string());
        }

        if let Some((host, path)) = origin.split_once(':')
            && !host.contains('/')
            && !host.is_empty()
            && !path.is_empty()
        {
            let host = host.rsplit_once('@').map_or(host, |(_, host)| host);
            if !host.is_empty() {
                return Ok(format!("{}:{path}", host.to_ascii_lowercase()));
            }
        }

        Err(invalid_origin())
    }

    pub(crate) fn canonical_file_origin(origin: &str) -> Result<Option<String>, WorkerError> {
        let Ok(url) = Url::parse(origin) else {
            return Ok(None);
        };
        if url.scheme() != "file" {
            return Ok(None);
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(invalid_origin());
        }
        let path = url.to_file_path().map_err(|_| invalid_origin())?;
        if !path.is_absolute() {
            return Err(invalid_origin());
        }
        let canonical = url.to_string();
        if canonical != origin {
            return Err(invalid_origin());
        }
        Ok(Some(canonical))
    }

    pub(crate) fn origin_host(origin: &str) -> Result<String, WorkerError> {
        let normalized = normalize_origin(origin)?;
        if let Ok(url) = Url::parse(&normalized) {
            return url
                .host_str()
                .map(str::to_owned)
                .filter(|host| !host.is_empty())
                .ok_or_else(invalid_origin);
        }
        normalized
            .split_once(':')
            .map(|(host, _)| host.to_owned())
            .filter(|host| !host.is_empty())
            .ok_or_else(invalid_origin)
    }

    fn invalid_origin() -> WorkerError {
        project_error(
            "INVALID_ORIGIN",
            "Git returned an unsupported origin URL".into(),
        )
    }

    fn project_error(code: &'static str, message: String) -> WorkerError {
        WorkerError::Project { code, message }
    }
}
