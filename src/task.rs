use std::{fmt, str::FromStr};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, DeserializeOwned},
    ser::{self, SerializeStruct},
};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::{
    agent::{AgentKind, AgentOutcome, PermissionPolicy, TurnLimits},
    error::WorkerError,
    job::{JobId, ProcessIdentity},
};

pub const MAX_PROMPT_BYTES: usize = 256 * 1024;
pub const MAX_TITLE_BYTES: usize = 120;
pub const MAX_FOLLOWUPS: u32 = 100;
pub const DEFAULT_MAX_FOLLOWUPS: u32 = 10;
const DEFAULT_TURN_TIMEOUT_MILLIS: u64 = 30 * 60 * 1000;
const MAX_IDENTITY_BYTES: usize = 256;
const MAX_HEX_ID_BYTES: usize = 64;

pub type TurnId = JobId;

macro_rules! canonical_uuid_id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name(Uuid);

        impl $name {
            pub fn new(value: Uuid) -> Self {
                Self(value)
            }

            pub fn generate() -> Self {
                Self(Uuid::new_v4())
            }

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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskSource {
    Local { wip: bool },
    Origin { url: String },
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

impl TaskState {
    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Queued, Self::Active | Self::Abandoned)
                | (Self::Active, Self::Open | Self::Closed | Self::Lost)
                | (
                    Self::Open,
                    Self::Active | Self::Closed | Self::Abandoned | Self::Lost
                )
        )
    }
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

impl TaskOutcome {
    pub fn from_turn(terminal: TurnTerminal, outcome: Option<AgentOutcome>) -> Self {
        match (terminal, outcome) {
            (TurnTerminal::Lost, _) => Self::Lost,
            (TurnTerminal::TimedOut, _) => Self::TimedOut,
            (TurnTerminal::Cancelled, _) => Self::Cancelled,
            (TurnTerminal::Succeeded, Some(AgentOutcome::Done)) => Self::Done,
            (TurnTerminal::Succeeded, Some(AgentOutcome::NeedsInput)) => Self::NeedsInput,
            (TurnTerminal::Succeeded, Some(AgentOutcome::Blocked)) => Self::Blocked,
            (TurnTerminal::Succeeded, Some(AgentOutcome::Unknown) | None) => Self::Unknown,
            (
                TurnTerminal::Succeeded | TurnTerminal::Failed,
                Some(AgentOutcome::Failed { exit_code }),
            ) => Self::Failed {
                reason: format!("agent exited {exit_code}"),
            },
            (TurnTerminal::Succeeded, Some(AgentOutcome::Signalled)) => Self::Cancelled,
            (TurnTerminal::Failed, _) => Self::Failed {
                reason: "turn failed".into(),
            },
        }
    }
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
pub struct TaskMetaInput {
    pub task_id: TaskId,
    pub run_id: Option<RunId>,
    pub project_id: String,
    pub worktree_id: String,
    pub agent: AgentKind,
    pub model: Option<String>,
    pub policy: PermissionPolicy,
    pub source: TaskSource,
    pub publish: Vec<PublishMode>,
    pub publish_branch: Option<BranchName>,
    pub base_oid: BaseOid,
    pub limits: TaskLimits,
    pub close_policy: ClosePolicy,
    pub env_profile: Option<String>,
    pub git_identity: GitIdentity,
    pub prompt: String,
    pub created_at_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskMeta {
    task_id: TaskId,
    run_id: Option<RunId>,
    project_id: String,
    worktree_id: String,
    agent: AgentKind,
    model: Option<String>,
    policy: PermissionPolicy,
    source: TaskSource,
    publish: Vec<PublishMode>,
    publish_branch: Option<BranchName>,
    base_oid: BaseOid,
    limits: TaskLimits,
    close_policy: ClosePolicy,
    env_profile: Option<String>,
    git_identity: GitIdentity,
    title: TaskTitle,
    prompt: String,
    created_at_millis: u64,
}

impl TaskMeta {
    pub fn new(input: TaskMetaInput) -> Result<Self, WorkerError> {
        let title = title_from_prompt(&input.prompt);
        let meta = Self {
            task_id: input.task_id,
            run_id: input.run_id,
            project_id: input.project_id,
            worktree_id: input.worktree_id,
            agent: input.agent,
            model: input.model,
            policy: input.policy,
            source: input.source,
            publish: input.publish,
            publish_branch: input.publish_branch,
            base_oid: input.base_oid,
            limits: input.limits,
            close_policy: input.close_policy,
            env_profile: input.env_profile,
            git_identity: input.git_identity,
            title,
            prompt: input.prompt,
            created_at_millis: input.created_at_millis,
        };
        meta.validate()?;
        Ok(meta)
    }

    pub fn title(&self) -> &TaskTitle {
        &self.title
    }

    pub fn prompt(&self) -> &str {
        &self.prompt
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn run_id(&self) -> Option<RunId> {
        self.run_id
    }

    pub fn agent(&self) -> AgentKind {
        self.agent
    }

    pub fn source(&self) -> &TaskSource {
        &self.source
    }

    pub fn publish(&self) -> &[PublishMode] {
        &self.publish
    }

    pub fn base_oid(&self) -> &BaseOid {
        &self.base_oid
    }

    pub fn git_identity(&self) -> &GitIdentity {
        &self.git_identity
    }

    pub fn summary(&self) -> TaskSummary {
        TaskSummary {
            task_id: self.task_id,
            run_id: self.run_id,
            agent: self.agent,
            title: self.title.clone(),
            state: TaskState::Queued,
            last_outcome: None,
            worker: None,
            turns: 0,
            runner: None,
            updated_at_millis: self.created_at_millis,
        }
    }

    fn validate(&self) -> Result<(), WorkerError> {
        validate_hex_component(&self.project_id, "project ID")?;
        validate_hex_component(&self.worktree_id, "worktree ID")?;
        if self.prompt.len() > MAX_PROMPT_BYTES {
            return Err(task_config(format!(
                "prompt exceeds {MAX_PROMPT_BYTES} bytes"
            )));
        }
        if let Some(model) = &self.model {
            validate_optional_text(model, MAX_IDENTITY_BYTES, "model")?;
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
        if matches!(self.source, TaskSource::Origin { .. }) {
            return Err(task_config("source origin is deferred to a later plan"));
        }
        if self.publish.contains(&PublishMode::Push) {
            return Err(task_config("publish push is deferred to a later plan"));
        }
        Ok(())
    }
}

impl Serialize for TaskMeta {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("TaskMeta", 18)?;
        record.serialize_field("task_id", &self.task_id)?;
        record.serialize_field("run_id", &self.run_id)?;
        record.serialize_field("project_id", &self.project_id)?;
        record.serialize_field("worktree_id", &self.worktree_id)?;
        record.serialize_field("agent", &AgentKindWire::from(self.agent))?;
        record.serialize_field("model", &self.model)?;
        record.serialize_field("policy", &PermissionPolicyWire::from(self.policy))?;
        record.serialize_field("source", &self.source)?;
        record.serialize_field("publish", &self.publish)?;
        record.serialize_field("publish_branch", &self.publish_branch)?;
        record.serialize_field("base_oid", &self.base_oid)?;
        record.serialize_field("limits", &self.limits)?;
        record.serialize_field("close_policy", &self.close_policy)?;
        record.serialize_field("env_profile", &self.env_profile)?;
        record.serialize_field("git_identity", &self.git_identity)?;
        record.serialize_field("title", &self.title)?;
        record.serialize_field("prompt", &self.prompt)?;
        record.serialize_field("created_at_millis", &self.created_at_millis)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for TaskMeta {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            task_id: TaskId,
            run_id: Option<RunId>,
            project_id: String,
            worktree_id: String,
            agent: AgentKindWire,
            model: Option<String>,
            policy: PermissionPolicyWire,
            source: TaskSource,
            publish: Vec<PublishMode>,
            publish_branch: Option<BranchName>,
            base_oid: BaseOid,
            limits: TaskLimits,
            close_policy: ClosePolicy,
            env_profile: Option<String>,
            git_identity: GitIdentity,
            title: TaskTitle,
            prompt: String,
            created_at_millis: u64,
        }
        let wire: Wire = deserialize_unique_object(deserializer)?;
        let meta = TaskMeta::new(TaskMetaInput {
            task_id: wire.task_id,
            run_id: wire.run_id,
            project_id: wire.project_id,
            worktree_id: wire.worktree_id,
            agent: wire.agent.into(),
            model: wire.model,
            policy: wire.policy.into(),
            source: wire.source,
            publish: wire.publish,
            publish_branch: wire.publish_branch,
            base_oid: wire.base_oid,
            limits: wire.limits,
            close_policy: wire.close_policy,
            env_profile: wire.env_profile,
            git_identity: wire.git_identity,
            prompt: wire.prompt,
            created_at_millis: wire.created_at_millis,
        })
        .map_err(de::Error::custom)?;
        if meta.title != wire.title {
            return Err(de::Error::custom("task title does not match the prompt"));
        }
        Ok(meta)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSummary {
    task_id: TaskId,
    run_id: Option<RunId>,
    agent: AgentKind,
    title: TaskTitle,
    state: TaskState,
    last_outcome: Option<TaskOutcome>,
    worker: Option<String>,
    turns: u32,
    runner: Option<RunnerState>,
    updated_at_millis: u64,
}

impl Serialize for TaskSummary {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut record = serializer.serialize_struct("TaskSummary", 10)?;
        record.serialize_field("task_id", &self.task_id)?;
        record.serialize_field("run_id", &self.run_id)?;
        record.serialize_field("agent", &AgentKindWire::from(self.agent))?;
        record.serialize_field("title", &self.title)?;
        record.serialize_field("state", &self.state)?;
        record.serialize_field("last_outcome", &self.last_outcome)?;
        record.serialize_field("worker", &self.worker)?;
        record.serialize_field("turns", &self.turns)?;
        record.serialize_field("runner", &self.runner)?;
        record.serialize_field("updated_at_millis", &self.updated_at_millis)?;
        record.end()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
}

impl TurnSummary {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        turn_number: u32,
        turn_id: TurnId,
        terminal: Option<TurnTerminal>,
        outcome: Option<TaskOutcome>,
        agent_committed: Option<bool>,
        log_truncated: bool,
        started_at_millis: Option<u64>,
        ended_at_millis: Option<u64>,
    ) -> Self {
        Self {
            turn_number,
            turn_id,
            terminal,
            outcome,
            agent_committed,
            log_truncated,
            started_at_millis,
            ended_at_millis,
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
    questions: Vec<String>,
    files_changed: Vec<String>,
    diff_stat: Option<String>,
    turns: Vec<TurnSummary>,
    updated_at_millis: u64,
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
        questions: Vec<String>,
        files_changed: Vec<String>,
        diff_stat: Option<String>,
        turns: Vec<TurnSummary>,
        updated_at_millis: u64,
    ) -> Result<Self, WorkerError> {
        let status = Self {
            state,
            last_outcome,
            worker,
            session_present,
            head_oid,
            summary,
            questions,
            files_changed,
            diff_stat,
            turns,
            updated_at_millis,
        };
        status.validate()?;
        Ok(status)
    }

    pub fn state(&self) -> TaskState {
        self.state
    }

    pub fn last_outcome(&self) -> Option<&TaskOutcome> {
        self.last_outcome.as_ref()
    }

    pub fn turns(&self) -> &[TurnSummary] {
        &self.turns
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
        let mut record = serializer.serialize_struct("TaskStatus", 11)?;
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
            questions: Vec<String>,
            files_changed: Vec<String>,
            diff_stat: Option<String>,
            turns: Vec<TurnSummary>,
            updated_at_millis: u64,
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
        .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunnerIdentity(ProcessIdentity);

impl RunnerIdentity {
    pub fn new(identity: ProcessIdentity) -> Self {
        Self(identity)
    }

    pub fn process_identity(&self) -> ProcessIdentity {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunnerState {
    Live,
    Dead,
    Exited,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalTaskRecord {
    meta: TaskMeta,
    status: TaskStatus,
    status_observed_at_millis: Option<u64>,
    runner: Option<RunnerIdentity>,
    fetched_head: Option<BaseOid>,
    repo_id: String,
    // Task 7 converts pinned_worker into WorkerPreference.
    pinned_worker: Option<String>,
    wait_for_capacity: bool,
    abandon_code: Option<String>,
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
            status,
            status_observed_at_millis,
            runner,
            fetched_head,
            repo_id,
            pinned_worker,
            wait_for_capacity,
            abandon_code,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn meta(&self) -> &TaskMeta {
        &self.meta
    }

    pub fn status(&self) -> &TaskStatus {
        &self.status
    }

    pub fn pinned_worker(&self) -> Option<&str> {
        self.pinned_worker.as_deref()
    }

    pub fn repo_id(&self) -> &str {
        &self.repo_id
    }

    pub fn summary(&self) -> TaskSummary {
        TaskSummary {
            task_id: self.meta.task_id,
            run_id: self.meta.run_id,
            agent: self.meta.agent,
            title: self.meta.title.clone(),
            state: self.status.state,
            last_outcome: self.status.last_outcome.clone(),
            worker: self.status.worker.clone(),
            turns: u32::try_from(self.status.turns.len()).unwrap_or(u32::MAX),
            runner: self.runner.as_ref().map(|_| RunnerState::Live),
            updated_at_millis: self.status.updated_at_millis,
        }
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    fn validate(&self) -> Result<(), WorkerError> {
        self.meta.validate()?;
        self.status.validate()?;
        validate_hex_component(&self.repo_id, "repo ID")?;
        if let Some(worker) = &self.pinned_worker {
            validate_pinned_worker(worker)?;
        }
        if let Some(code) = &self.abandon_code {
            validate_optional_text(code, 128, "abandon code")?;
        }
        Ok(())
    }
}

impl Serialize for LocalTaskRecord {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("LocalTaskRecord", 9)?;
        record.serialize_field("meta", &self.meta)?;
        record.serialize_field("status", &self.status)?;
        record.serialize_field("status_observed_at_millis", &self.status_observed_at_millis)?;
        record.serialize_field("runner", &self.runner)?;
        record.serialize_field("fetched_head", &self.fetched_head)?;
        record.serialize_field("repo_id", &self.repo_id)?;
        record.serialize_field("pinned_worker", &self.pinned_worker)?;
        record.serialize_field("wait_for_capacity", &self.wait_for_capacity)?;
        record.serialize_field("abandon_code", &self.abandon_code)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for LocalTaskRecord {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            meta: TaskMeta,
            status: TaskStatus,
            status_observed_at_millis: Option<u64>,
            runner: Option<RunnerIdentity>,
            fetched_head: Option<BaseOid>,
            repo_id: String,
            pinned_worker: Option<String>,
            wait_for_capacity: bool,
            abandon_code: Option<String>,
        }
        let wire: Wire = deserialize_unique_object(deserializer)?;
        Self::new(
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
        .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRecord {
    run_id: RunId,
    name: Option<String>,
    task_ids: Vec<TaskId>,
    max_parallel: u32,
    created_at_millis: u64,
}

impl RunRecord {
    pub fn new(
        run_id: RunId,
        name: Option<String>,
        task_ids: Vec<TaskId>,
        max_parallel: u32,
        created_at_millis: u64,
    ) -> Result<Self, WorkerError> {
        let record = Self {
            run_id,
            name,
            task_ids,
            max_parallel,
            created_at_millis,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    fn validate(&self) -> Result<(), WorkerError> {
        if let Some(name) = &self.name {
            validate_optional_text(name, MAX_TITLE_BYTES, "run name")?;
        }
        if self.max_parallel == 0 {
            return Err(task_config("max_parallel must be greater than zero"));
        }
        Ok(())
    }
}

impl Serialize for RunRecord {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("RunRecord", 5)?;
        record.serialize_field("run_id", &self.run_id)?;
        record.serialize_field("name", &self.name)?;
        record.serialize_field("task_ids", &self.task_ids)?;
        record.serialize_field("max_parallel", &self.max_parallel)?;
        record.serialize_field("created_at_millis", &self.created_at_millis)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for RunRecord {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            run_id: RunId,
            name: Option<String>,
            task_ids: Vec<TaskId>,
            max_parallel: u32,
            created_at_millis: u64,
        }
        let wire: Wire = deserialize_unique_object(deserializer)?;
        Self::new(
            wire.run_id,
            wire.name,
            wire.task_ids,
            wire.max_parallel,
            wire.created_at_millis,
        )
        .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunProgress {
    pub total: usize,
    pub queued: usize,
    pub active: usize,
    pub open: usize,
    pub closed: usize,
    pub failed_like: usize,
}

impl RunProgress {
    pub fn from_states(states: impl IntoIterator<Item = TaskState>) -> Self {
        let mut progress = Self {
            total: 0,
            queued: 0,
            active: 0,
            open: 0,
            closed: 0,
            failed_like: 0,
        };
        for state in states {
            progress.total += 1;
            match state {
                TaskState::Queued => progress.queued += 1,
                TaskState::Active => progress.active += 1,
                TaskState::Open => progress.open += 1,
                TaskState::Closed => progress.closed += 1,
                TaskState::Abandoned | TaskState::Lost => progress.failed_like += 1,
            }
        }
        progress
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PermissionPolicyWire {
    Workspace,
    Unattended,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TurnLimitsWire {
    timeout_millis: u64,
    max_turns: Option<u32>,
    max_budget_usd_cents: Option<u64>,
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
                while let Some((key, value)) = map.next_entry::<String, Value>()? {
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

fn title_from_prompt(prompt: &str) -> TaskTitle {
    let line = prompt
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("");
    TaskTitle(truncate_bytes(&escape_controls(line), MAX_TITLE_BYTES))
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
        return input.to_string();
    }
    let mut end = max_bytes;
    while end > 0 && !input.is_char_boundary(end) {
        end -= 1;
    }
    input[..end].to_string()
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

fn task_config(message: impl Into<String>) -> WorkerError {
    WorkerError::Task {
        code: "TASK_CONFIG_INVALID",
        message: message.into(),
    }
}
