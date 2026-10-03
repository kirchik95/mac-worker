//! Integration-only DTOs; existing task, turn and controller bodies stay strict.
use std::{collections::BTreeMap, fmt, str::FromStr, time::Duration};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::{
    agent::{ReportedCheck, TurnLimits},
    client_state::RunnerLivenessVerdict,
    controller::batch::FrozenBatchBody,
    error::WorkerError,
    job::ProcessIdentity,
    prepared_followup::PreparedFollowup,
    prepared_submit::FrozenSubmitBody,
    redaction::RedactionBoundary,
    task::{BaseOid, BranchName, ClosePolicy, GitIdentity, LocalTaskRecord, TaskId, TurnId},
    task_view::ReviewState,
};

pub const INTEGRATION_SCHEMA_VERSION: u32 = 1;
pub const MAX_TARGET_BYTES: usize = 255;
pub const MAX_TARGET_DISPLAY_BYTES: usize = 128;
pub const MAX_PUBLIC_SNAPSHOT_BYTES: usize = 4 * 1024;
pub const MAX_PRIVATE_RECORD_BYTES: usize = 64 * 1024;
pub const MAX_PREPARED_TURN_BYTES: usize = 256 * 1024;
pub const MAX_INTEGRATION_RPC_BYTES: usize = 1024 * 1024 - 1;
pub const MAX_FACTS_ANNOTATION_BYTES: usize = 512;
pub const MAX_COMPOUND_FACTS_BYTES: usize = 2048;
pub const MAX_CANDIDATES: usize = 3;
pub const MAX_AUXILIARY_INTENTS: usize = 5;
pub const MAX_ARCHIVED_RECEIPTS: usize = 8;
pub const MAX_RESOLVE_TURNS: u8 = 2;
pub const MAX_VERIFY_TURNS: u8 = 3;
pub const MAX_PROMPT_BYTES: usize = 16 * 1024;
pub const MAX_CONFLICT_PATHS: usize = 256;
pub const MAX_CONFLICT_PATH_BYTES: usize = 1024;
pub const MAX_MESSAGE_TITLE_BYTES: usize = 120;
pub const MAX_MESSAGE_SUMMARY_BYTES: usize = 1024;
pub const MAX_COMMIT_MESSAGE_BYTES: usize = 2048;
pub const MAX_JOURNAL_HINT_BYTES: usize = 1024;
pub const MAX_READ_TASKS: usize = 16;
pub const MAX_GIT_DRIVERS: usize = 4;
pub const AUXILIARY_ADMISSION_MILLIS: u64 = 600_000;
pub const AUXILIARY_EXECUTION_CAP: Duration = Duration::from_secs(600);
pub const GIT_DEADLINE: Duration = Duration::from_secs(60);
pub const HOST_DEADLINE: Duration = Duration::from_secs(90);
pub const HELPER_LOOKUP_DEADLINE: Duration = Duration::from_secs(5);
pub const GIT_OUTPUT_BYTES: usize = 64 * 1024;
pub const TRANSPORT_RETRY_DELAYS_MILLIS: &[u64] = &[2000, 10_000, 30_000];

pub fn integration_error(code: &'static str) -> WorkerError {
    WorkerError::task(code, "integration contract rejected")
}
pub fn integration_unavailable() -> WorkerError {
    WorkerError::Unavailable("INTEGRATION_UNAVAILABLE: integration is not wired".into())
}
fn invalid() -> WorkerError {
    integration_error("INTEGRATION_STATE_INVALID")
}
pub trait ValidateIntegration {
    fn validate(&self) -> Result<(), WorkerError>;
}

// The same field list defines the public struct and its strict validating wire.
// Validation also runs at encode boundaries, since callers can mutate fields.
macro_rules! contract {
    ($name:ident { $($(#[$attr:meta])* $field:ident: $ty:ty),* $(,)? }) => {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
        pub struct $name { $($(#[$attr])* pub $field: $ty),* }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Wire { $($(#[$attr])* $field: $ty),* }
                let wire = Wire::deserialize(d)?;
                let value = Self { $($field: wire.$field),* };
                value.validate().map_err(serde::de::Error::custom)?;
                Ok(value)
            }
        }
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IntegrationId(uuid::Uuid);
impl IntegrationId {
    pub fn as_uuid(self) -> uuid::Uuid {
        self.0
    }
    pub fn derive(
        task: TaskId,
        source: TurnId,
        head: &BaseOid,
        key: &TargetKey,
    ) -> Result<Self, WorkerError> {
        let key = key.canonical_bytes()?;
        let mut digest = Sha256::new();
        digest.update(b"mac-worker/integration/v1\0");
        digest.update(task.as_uuid().as_bytes());
        digest.update(source.as_uuid().as_bytes());
        digest.update(head.as_str().as_bytes());
        digest.update((key.len() as u64).to_be_bytes());
        digest.update(key);
        Ok(Self(uuid_v8(digest)))
    }
}
impl fmt::Display for IntegrationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.hyphenated().fmt(f)
    }
}
impl FromStr for IntegrationId {
    type Err = WorkerError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let uuid = uuid::Uuid::parse_str(value).map_err(|_| invalid())?;
        if uuid.is_nil()
            || uuid.hyphenated().to_string() != value
            || uuid.get_version_num() != 8
            || uuid.get_variant() != uuid::Variant::RFC4122
        {
            return Err(invalid());
        }
        Ok(Self(uuid))
    }
}
impl Serialize for IntegrationId {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}
impl<'de> Deserialize<'de> for IntegrationId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IntegrationRevision(pub u64);
impl IntegrationRevision {
    pub fn next(self) -> Result<Self, WorkerError> {
        self.0.checked_add(1).map(Self).ok_or_else(invalid)
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum IntegrationOverride {
    #[default]
    Inherit,
    Disabled,
    Target(BranchName),
}
impl IntegrationOverride {
    pub fn is_inherit(&self) -> bool {
        matches!(self, Self::Inherit)
    }
}
impl Serialize for IntegrationOverride {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Inherit => Err(serde::ser::Error::custom(
                "inherited integration must be omitted",
            )),
            Self::Disabled => s.serialize_bool(false),
            Self::Target(target) => s.serialize_str(target.as_str()),
        }
    }
}
impl<'de> Deserialize<'de> for IntegrationOverride {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            Branch(String),
            Disabled(bool),
        }
        match Wire::deserialize(d)? {
            Wire::Branch(branch) => validate_integration_target(&branch)
                .map(Self::Target)
                .map_err(serde::de::Error::custom),
            Wire::Disabled(false) => Ok(Self::Disabled),
            Wire::Disabled(true) => Err(serde::de::Error::custom(
                "integrate accepts a branch or false",
            )),
        }
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum VerifyPolicy {
    #[default]
    Never,
    MovedTarget,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IntegrationBaseKind {
    Committed,
    FromTask,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationBasePreflight {
    Pass,
    Unknown,
}

contract!(TargetKey {
    origin: String,
    branch: BranchName
});
impl TargetKey {
    pub fn new(origin: &str, branch: &str) -> Result<Self, WorkerError> {
        let origin = canonical_origin(origin)?;
        let key = Self {
            origin,
            branch: validate_integration_target(branch)?,
        };
        key.validate()?;
        Ok(key)
    }
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, WorkerError> {
        self.validate()?;
        serde_json::to_vec(&(self.origin.as_str(), self.branch.as_str())).map_err(|_| invalid())
    }
}
impl ValidateIntegration for TargetKey {
    fn validate(&self) -> Result<(), WorkerError> {
        if canonical_origin(&self.origin)? != self.origin {
            return Err(invalid());
        }
        validate_integration_target(self.branch.as_str())?;
        Ok(())
    }
}
fn canonical_origin(origin: &str) -> Result<String, WorkerError> {
    if origin.is_empty() || origin.len() > 8192 || origin.chars().any(char::is_control) {
        return Err(invalid());
    }
    crate::project::canonical_file_origin(origin)
        .and_then(|file| file.map_or_else(|| crate::project::normalize_origin(origin), Ok))
        .map_err(|_| invalid())
}
contract!(FrozenIntegrationPolicy {
    schema_version: u32,
    origin: String,
    target: BranchName,
    verify: VerifyPolicy,
    requested_close: ClosePolicy,
    base_kind: IntegrationBaseKind,
    base_oid: Option<BaseOid>,
    base_task: Option<TaskId>,
    base_preflight: IntegrationBasePreflight,
    project_id: String,
});
impl FrozenIntegrationPolicy {
    pub fn target_key(&self) -> Result<TargetKey, WorkerError> {
        TargetKey::new(&self.origin, self.target.as_str())
    }
}
impl ValidateIntegration for FrozenIntegrationPolicy {
    fn validate(&self) -> Result<(), WorkerError> {
        if self.schema_version != INTEGRATION_SCHEMA_VERSION
            || !valid_digest(&self.project_id)
            || (self.base_kind == IntegrationBaseKind::Committed
                && (self.base_oid.is_none() || self.base_task.is_some()))
            || (self.base_kind == IntegrationBaseKind::FromTask && self.base_task.is_none())
        {
            return Err(invalid());
        }
        canonical_origin(&self.origin)?;
        validate_integration_target(self.target.as_str())?;
        check_size(self, MAX_PRIVATE_RECORD_BYTES)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationStatus {
    Armed,
    Pending,
    Fetching,
    Resolving,
    Verifying,
    CommitReady,
    Pushing,
    Published,
    RetryWait,
    Parked,
    Integrated,
    Blocked,
    Revoked,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntegrationTransition {
    pub from: Option<IntegrationStatus>,
    pub next: &'static [IntegrationStatus],
    pub resume: bool,
    pub manual_redrive: bool,
    pub legacy_settlement: bool,
    pub new_cycle: bool,
}
pub const INTEGRATION_TRANSITIONS: &[IntegrationTransition] = &[
    IntegrationTransition {
        from: None,
        next: &[IntegrationStatus::Armed],
        resume: false,
        manual_redrive: false,
        legacy_settlement: false,
        new_cycle: false,
    },
    IntegrationTransition {
        from: Some(IntegrationStatus::Armed),
        next: &[IntegrationStatus::Pending, IntegrationStatus::Revoked],
        resume: false,
        manual_redrive: false,
        legacy_settlement: false,
        new_cycle: false,
    },
    IntegrationTransition {
        from: Some(IntegrationStatus::Pending),
        next: &[
            IntegrationStatus::Fetching,
            IntegrationStatus::Parked,
            IntegrationStatus::Blocked,
            IntegrationStatus::Revoked,
        ],
        resume: false,
        manual_redrive: false,
        legacy_settlement: true,
        new_cycle: false,
    },
    IntegrationTransition {
        from: Some(IntegrationStatus::Fetching),
        next: &[
            IntegrationStatus::Resolving,
            IntegrationStatus::Verifying,
            IntegrationStatus::CommitReady,
            IntegrationStatus::Integrated,
            IntegrationStatus::RetryWait,
            IntegrationStatus::Parked,
            IntegrationStatus::Blocked,
            IntegrationStatus::Revoked,
        ],
        resume: false,
        manual_redrive: false,
        legacy_settlement: true,
        new_cycle: false,
    },
    IntegrationTransition {
        from: Some(IntegrationStatus::Resolving),
        next: &[
            IntegrationStatus::Fetching,
            IntegrationStatus::RetryWait,
            IntegrationStatus::Parked,
            IntegrationStatus::Blocked,
            IntegrationStatus::Revoked,
        ],
        resume: false,
        manual_redrive: false,
        legacy_settlement: true,
        new_cycle: false,
    },
    IntegrationTransition {
        from: Some(IntegrationStatus::Verifying),
        next: &[
            IntegrationStatus::Fetching,
            IntegrationStatus::RetryWait,
            IntegrationStatus::Parked,
            IntegrationStatus::Blocked,
            IntegrationStatus::Revoked,
        ],
        resume: false,
        manual_redrive: false,
        legacy_settlement: true,
        new_cycle: false,
    },
    IntegrationTransition {
        from: Some(IntegrationStatus::CommitReady),
        next: &[
            IntegrationStatus::Pushing,
            IntegrationStatus::Fetching,
            IntegrationStatus::Parked,
            IntegrationStatus::Revoked,
        ],
        resume: false,
        manual_redrive: false,
        legacy_settlement: true,
        new_cycle: false,
    },
    IntegrationTransition {
        from: Some(IntegrationStatus::Pushing),
        next: &[
            IntegrationStatus::Published,
            IntegrationStatus::Fetching,
            IntegrationStatus::RetryWait,
            IntegrationStatus::Parked,
            IntegrationStatus::Blocked,
        ],
        resume: false,
        manual_redrive: false,
        legacy_settlement: true,
        new_cycle: false,
    },
    IntegrationTransition {
        from: Some(IntegrationStatus::Published),
        next: &[
            IntegrationStatus::Integrated,
            IntegrationStatus::RetryWait,
            IntegrationStatus::Parked,
        ],
        resume: false,
        manual_redrive: false,
        legacy_settlement: true,
        new_cycle: false,
    },
    IntegrationTransition {
        from: Some(IntegrationStatus::RetryWait),
        next: &[
            IntegrationStatus::Parked,
            IntegrationStatus::Blocked,
            IntegrationStatus::Revoked,
        ],
        resume: true,
        manual_redrive: false,
        legacy_settlement: true,
        new_cycle: false,
    },
    IntegrationTransition {
        from: Some(IntegrationStatus::Parked),
        next: &[IntegrationStatus::Revoked],
        resume: true,
        manual_redrive: false,
        legacy_settlement: true,
        new_cycle: false,
    },
    IntegrationTransition {
        from: Some(IntegrationStatus::Integrated),
        next: &[],
        resume: false,
        manual_redrive: false,
        legacy_settlement: false,
        new_cycle: true,
    },
    IntegrationTransition {
        from: Some(IntegrationStatus::Blocked),
        next: &[IntegrationStatus::Revoked],
        resume: false,
        manual_redrive: true,
        legacy_settlement: true,
        new_cycle: false,
    },
    IntegrationTransition {
        from: Some(IntegrationStatus::Revoked),
        next: &[],
        resume: false,
        manual_redrive: false,
        legacy_settlement: false,
        new_cycle: true,
    },
];
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationPauseReason {
    ControllerDrained,
    ControllerDisabled,
    HelperUnavailable,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrationPauseEvidence {
    pub reason: IntegrationPauseReason,
    pub effective_at_millis: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationDisposition {
    Merged,
    AlreadyIntegrated,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationVerification {
    SourceAgentReportOnly,
    ResolveAgentReport,
    VerifyAgentReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[allow(clippy::enum_variant_names)] // The prefix is the stable diagnostic wire catalog.
pub enum IntegrationCode {
    IntegrationAuthFailed,
    IntegrationNetwork,
    IntegrationPolicyRejected,
    IntegrationTargetMissing,
    IntegrationBaseNotOnTarget,
    IntegrationWipBase,
    IntegrationTargetMovedExhausted,
    IntegrationConflictBudgetExhausted,
    IntegrationResolutionIncomplete,
    IntegrationChecksFailed,
    IntegrationChecksNotRun,
    IntegrationResolveBlocked,
    IntegrationFollowupLimit,
    IntegrationVerifyChangedTree,
    IntegrationVerifyTreeMismatch,
    IntegrationTurnQueueTimeout,
    IntegrationWorkerOffline,
    IntegrationWorkspaceMissing,
    IntegrationUnavailable,
    IntegrationPublishTargetCollision,
    IntegrationConflictListTooLarge,
    IntegrationStateInvalid,
    IntegrationDependencyBlocked,
    IntegrationDependencyNotIntegrated,
    IntegrationStopUnconfirmed,
    IntegrationAlreadyCommitted,
}

impl IntegrationCode {
    pub const ALL: &'static [Self] = &[
        Self::IntegrationAuthFailed,
        Self::IntegrationNetwork,
        Self::IntegrationPolicyRejected,
        Self::IntegrationTargetMissing,
        Self::IntegrationBaseNotOnTarget,
        Self::IntegrationWipBase,
        Self::IntegrationTargetMovedExhausted,
        Self::IntegrationConflictBudgetExhausted,
        Self::IntegrationResolutionIncomplete,
        Self::IntegrationChecksFailed,
        Self::IntegrationChecksNotRun,
        Self::IntegrationResolveBlocked,
        Self::IntegrationFollowupLimit,
        Self::IntegrationVerifyChangedTree,
        Self::IntegrationVerifyTreeMismatch,
        Self::IntegrationTurnQueueTimeout,
        Self::IntegrationWorkerOffline,
        Self::IntegrationWorkspaceMissing,
        Self::IntegrationUnavailable,
        Self::IntegrationPublishTargetCollision,
        Self::IntegrationConflictListTooLarge,
        Self::IntegrationStateInvalid,
        Self::IntegrationDependencyBlocked,
        Self::IntegrationDependencyNotIntegrated,
        Self::IntegrationStopUnconfirmed,
        Self::IntegrationAlreadyCommitted,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::IntegrationAuthFailed => "INTEGRATION_AUTH_FAILED",
            Self::IntegrationNetwork => "INTEGRATION_NETWORK",
            Self::IntegrationPolicyRejected => "INTEGRATION_POLICY_REJECTED",
            Self::IntegrationTargetMissing => "INTEGRATION_TARGET_MISSING",
            Self::IntegrationBaseNotOnTarget => "INTEGRATION_BASE_NOT_ON_TARGET",
            Self::IntegrationWipBase => "INTEGRATION_WIP_BASE",
            Self::IntegrationTargetMovedExhausted => "INTEGRATION_TARGET_MOVED_EXHAUSTED",
            Self::IntegrationConflictBudgetExhausted => "INTEGRATION_CONFLICT_BUDGET_EXHAUSTED",
            Self::IntegrationResolutionIncomplete => "INTEGRATION_RESOLUTION_INCOMPLETE",
            Self::IntegrationChecksFailed => "INTEGRATION_CHECKS_FAILED",
            Self::IntegrationChecksNotRun => "INTEGRATION_CHECKS_NOT_RUN",
            Self::IntegrationResolveBlocked => "INTEGRATION_RESOLVE_BLOCKED",
            Self::IntegrationFollowupLimit => "INTEGRATION_FOLLOWUP_LIMIT",
            Self::IntegrationVerifyChangedTree => "INTEGRATION_VERIFY_CHANGED_TREE",
            Self::IntegrationVerifyTreeMismatch => "INTEGRATION_VERIFY_TREE_MISMATCH",
            Self::IntegrationTurnQueueTimeout => "INTEGRATION_TURN_QUEUE_TIMEOUT",
            Self::IntegrationWorkerOffline => "INTEGRATION_WORKER_OFFLINE",
            Self::IntegrationWorkspaceMissing => "INTEGRATION_WORKSPACE_MISSING",
            Self::IntegrationUnavailable => "INTEGRATION_UNAVAILABLE",
            Self::IntegrationPublishTargetCollision => "INTEGRATION_PUBLISH_TARGET_COLLISION",
            Self::IntegrationConflictListTooLarge => "INTEGRATION_CONFLICT_LIST_TOO_LARGE",
            Self::IntegrationStateInvalid => "INTEGRATION_STATE_INVALID",
            Self::IntegrationDependencyBlocked => "INTEGRATION_DEPENDENCY_BLOCKED",
            Self::IntegrationDependencyNotIntegrated => "INTEGRATION_DEPENDENCY_NOT_INTEGRATED",
            Self::IntegrationStopUnconfirmed => "INTEGRATION_STOP_UNCONFIRMED",
            Self::IntegrationAlreadyCommitted => "INTEGRATION_ALREADY_COMMITTED",
        }
    }
    pub fn error(self) -> WorkerError {
        integration_error(self.as_str())
    }
}
contract!(IntegrationSnapshot {
    schema_version: u32, integration_id: IntegrationId, epoch: u32,
    revision: IntegrationRevision, target: String, state: IntegrationStatus,
    resume_state: Option<IntegrationStatus>, pause_reason: Option<IntegrationPauseReason>,
    source_turn_id: TurnId, source_head: BaseOid, merge_oid: Option<BaseOid>,
    observed_target_oid: Option<BaseOid>, disposition: Option<IntegrationDisposition>,
    attempts: u8, resolve_turns: u8, verify_turns: u8, blocked_code: Option<IntegrationCode>,
    retry_exhausted: bool, retry_at_millis: Option<u64>, verification: IntegrationVerification,
    updated_at_millis: u64,
});
impl IntegrationSnapshot {
    pub fn result_oid(&self) -> Option<&BaseOid> {
        self.merge_oid
            .as_ref()
            .or(self.observed_target_oid.as_ref())
    }
    pub fn annotation(&self) -> Result<IntegrationFactsAnnotation, WorkerError> {
        self.validate()?;
        let annotation = IntegrationFactsAnnotation {
            integration_id: self.integration_id,
            epoch: self.epoch,
            revision: self.revision,
            state: self.state,
            code: self.blocked_code,
            result_oid: self.result_oid().cloned(),
        };
        annotation.validate()?;
        Ok(annotation)
    }
}
impl ValidateIntegration for IntegrationSnapshot {
    fn validate(&self) -> Result<(), WorkerError> {
        let parked = self.state == IntegrationStatus::Parked;
        let resumable = parked || self.state == IntegrationStatus::RetryWait;
        if self.schema_version != INTEGRATION_SCHEMA_VERSION
            || self.revision.0 == 0
            || self.target.is_empty()
            || self.target.len() > MAX_TARGET_DISPLAY_BYTES
            || self.target.chars().any(char::is_control)
            || self.attempts as usize > MAX_CANDIDATES
            || self.resolve_turns > MAX_RESOLVE_TURNS
            || self.verify_turns > MAX_VERIFY_TURNS
            || parked != self.pause_reason.is_some()
            || resumable != self.resume_state.is_some()
            || self.resume_state.is_some_and(|state| {
                matches!(
                    state,
                    IntegrationStatus::Armed
                        | IntegrationStatus::Parked
                        | IntegrationStatus::RetryWait
                        | IntegrationStatus::Integrated
                        | IntegrationStatus::Blocked
                        | IntegrationStatus::Revoked
                )
            })
            || (self.state == IntegrationStatus::Blocked && self.blocked_code.is_none())
            || (self.state == IntegrationStatus::Integrated
                && (self.disposition.is_none() || self.result_oid().is_none()))
            || (self.disposition == Some(IntegrationDisposition::AlreadyIntegrated)
                && (self.merge_oid.is_some() || self.observed_target_oid.is_none()))
            || (self.disposition == Some(IntegrationDisposition::Merged)
                && self.merge_oid.is_none())
        {
            return Err(invalid());
        }
        check_size(self, MAX_PUBLIC_SNAPSHOT_BYTES)
    }
}
contract!(IntegrationFactsAnnotation {
    integration_id: IntegrationId, epoch: u32, revision: IntegrationRevision,
    state: IntegrationStatus, code: Option<IntegrationCode>, result_oid: Option<BaseOid>,
});
impl IntegrationFactsAnnotation {
    pub fn confirms(&self, snapshot: &IntegrationSnapshot) -> bool {
        self.validate().is_ok()
            && snapshot.validate().is_ok()
            && snapshot
                .annotation()
                .as_ref()
                .is_ok_and(|actual| actual == self)
    }
}
impl ValidateIntegration for IntegrationFactsAnnotation {
    fn validate(&self) -> Result<(), WorkerError> {
        if self.revision.0 == 0 {
            return Err(invalid());
        }
        check_size(self, MAX_FACTS_ANNOTATION_BYTES)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowState {
    Queued,
    Running,
    Integrating,
    NeedsYou,
    Done,
}
contract!(IntegrationView {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    integration: Option<IntegrationSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    workflow_state: Option<WorkflowState>,
    review_state: ReviewState, attention: bool, requested_close: ClosePolicy,
});
impl ValidateIntegration for IntegrationView {
    fn validate(&self) -> Result<(), WorkerError> {
        if let Some(snapshot) = &self.integration {
            snapshot.validate()?;
        }
        check_size(self, MAX_PUBLIC_SNAPSHOT_BYTES + 512)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrationCandidateId {
    pub integration_id: IntegrationId,
    pub epoch: u32,
    pub attempt: u8,
}
contract!(CleanHManifest {
    branch: BranchName, head: BaseOid, untracked_files: Vec<String>,
});
impl ValidateIntegration for CleanHManifest {
    fn validate(&self) -> Result<(), WorkerError> {
        if !self.branch.as_str().starts_with("task/") {
            return Err(invalid());
        }
        for path in &self.untracked_files {
            validate_relative_path(path)?;
        }
        check_size(self, MAX_PRIVATE_RECORD_BYTES)
    }
}
contract!(IntegrationCandidate {
    id: IntegrationCandidateId, target_head: BaseOid, source_head: BaseOid,
    tree_oid: Option<BaseOid>, merge_oid: Option<BaseOid>, message: String,
    identity: GitIdentity, timestamp_millis: u64, attribute_source: BaseOid,
    ours: BaseOid, theirs: BaseOid, clean_h: CleanHManifest, conflict_paths: Vec<String>,
});
impl ValidateIntegration for IntegrationCandidate {
    fn validate(&self) -> Result<(), WorkerError> {
        if self.id.attempt == 0
            || self.id.attempt as usize > MAX_CANDIDATES
            || self.attribute_source != self.source_head
            || self.ours != self.source_head
            || self.theirs != self.target_head
            || self.source_head == self.target_head
            || self.clean_h.head != self.source_head
            || self.message.is_empty()
            || self.message.len() > MAX_COMMIT_MESSAGE_BYTES
            || (self.merge_oid.is_some() && self.tree_oid.is_none())
        {
            return Err(invalid());
        }
        self.clean_h.validate()?;
        validate_conflict_paths(&self.conflict_paths)?;
        check_size(self, MAX_PRIVATE_RECORD_BYTES)
    }
}
contract!(IntegrationWorkspaceBinding {
    task_id: TaskId, candidate: IntegrationCandidateId, branch: BranchName,
    head: BaseOid, merge_head: BaseOid, attribute_source: BaseOid, ours: BaseOid,
    theirs: BaseOid, pinned_tree: Option<BaseOid>, clean_h: CleanHManifest,
});
impl IntegrationWorkspaceBinding {
    pub fn validate_for(&self, candidate: &IntegrationCandidate) -> Result<(), WorkerError> {
        self.validate()?;
        candidate.validate()?;
        if self.candidate != candidate.id
            || self.head != candidate.source_head
            || self.merge_head != candidate.target_head
            || self.attribute_source != candidate.attribute_source
            || self.ours != candidate.ours
            || self.theirs != candidate.theirs
            || self.clean_h != candidate.clean_h
            || self
                .pinned_tree
                .as_ref()
                .is_some_and(|tree| Some(tree) != candidate.tree_oid.as_ref())
        {
            return Err(invalid());
        }
        Ok(())
    }
}
impl ValidateIntegration for IntegrationWorkspaceBinding {
    fn validate(&self) -> Result<(), WorkerError> {
        if self.candidate.attempt == 0
            || self.candidate.attempt as usize > MAX_CANDIDATES
            || self.branch != BranchName::for_task(self.task_id)
            || self.clean_h.branch != self.branch
            || self.clean_h.head != self.head
            || self.attribute_source != self.head
            || self.ours != self.head
            || self.theirs != self.merge_head
            || self.head == self.merge_head
        {
            return Err(invalid());
        }
        self.clean_h.validate()
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationTurnPurpose {
    Resolve,
    Verify,
}
contract!(PreparedIntegrationTurn {
    integration_id: IntegrationId,
    epoch: u32,
    attempt: u8,
    purpose: IntegrationTurnPurpose,
    ordinal: u8,
    followup: PreparedFollowup,
    workspace_binding: IntegrationWorkspaceBinding,
    #[serde(with = "approved_limits_wire")]
    approved_turn_limits: TurnLimits,
});
mod approved_limits_wire {
    use super::*;
    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Wire {
        timeout_millis: u64,
        max_turns: Option<u32>,
        max_budget_usd_cents: Option<u64>,
    }
    pub fn serialize<S: Serializer>(limits: &TurnLimits, s: S) -> Result<S::Ok, S::Error> {
        Wire {
            timeout_millis: limits.timeout_millis,
            max_turns: limits.max_turns,
            max_budget_usd_cents: limits.max_budget_usd_cents,
        }
        .serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<TurnLimits, D::Error> {
        let wire = Wire::deserialize(d)?;
        TurnLimits::new(
            wire.timeout_millis,
            wire.max_turns,
            wire.max_budget_usd_cents,
        )
        .map_err(serde::de::Error::custom)
    }
}
impl PreparedIntegrationTurn {
    pub fn validate_for(&self, record: &IntegrationRecord) -> Result<(), WorkerError> {
        self.validate()?;
        record.validate()?;
        if self.integration_id != record.snapshot.integration_id
            || self.epoch != record.snapshot.epoch
            || self.followup.task_id() != record.task_id
            || self.workspace_binding.head != record.snapshot.source_head
        {
            return Err(invalid());
        }
        let candidate = record
            .candidates
            .iter()
            .find(|candidate| candidate.id == self.workspace_binding.candidate)
            .ok_or_else(invalid)?;
        self.workspace_binding.validate_for(candidate)
    }
    pub fn binding(&self) -> Result<String, WorkerError> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| invalid())?;
        let mut digest = Sha256::new();
        digest.update(b"mac-worker/prepared-integration-turn/v1\0");
        digest.update(bytes);
        Ok(format!("{:x}", digest.finalize()))
    }
}
impl ValidateIntegration for PreparedIntegrationTurn {
    fn validate(&self) -> Result<(), WorkerError> {
        let binding = &self.workspace_binding;
        binding.validate()?;
        let ordinary = &self.followup.expected().meta().limits().turn;
        if self.attempt == 0
            || self.attempt as usize > MAX_CANDIDATES
            || self.ordinal == 0
            || self.ordinal
                > match self.purpose {
                    IntegrationTurnPurpose::Resolve => MAX_RESOLVE_TURNS,
                    IntegrationTurnPurpose::Verify => MAX_VERIFY_TURNS,
                }
            || binding.candidate
                != (IntegrationCandidateId {
                    integration_id: self.integration_id,
                    epoch: self.epoch,
                    attempt: self.attempt,
                })
            || self.followup.task_id() != binding.task_id
            || self.followup.turn_id()
                != auxiliary_turn_id(
                    self.integration_id,
                    self.epoch,
                    self.attempt,
                    self.purpose,
                    self.ordinal,
                )?
            || self.followup.base_oid() != &binding.head
            || (self.purpose == IntegrationTurnPurpose::Verify && binding.pinned_tree.is_none())
            || self.approved_turn_limits.timeout_millis == 0
            || self.approved_turn_limits.timeout_millis
                > ordinary.timeout_millis.min(AUXILIARY_ADMISSION_MILLIS)
            || self.approved_turn_limits.max_turns != ordinary.max_turns
            || self.approved_turn_limits.max_budget_usd_cents != ordinary.max_budget_usd_cents
        {
            return Err(invalid());
        }
        if self.followup.composed_prompt().len() > MAX_PROMPT_BYTES {
            return Err(invalid());
        }
        check_size(self, MAX_PREPARED_TURN_BYTES)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationStep {
    Fetch,
    Prepare,
    AcceptTurn,
    Build,
    Push,
    Repair,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationPhase {
    Drive,
    Fetch,
    Prepare,
    AcceptTurn,
    Build,
    Push,
    Repair,
    AuxiliaryAdmission,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrationPhaseKey {
    pub task: TaskId,
    pub intent: IntegrationId,
    pub epoch: u32,
    pub revision: IntegrationRevision,
    pub phase: IntegrationPhase,
}
/// Short ownership/handoff permit. Drop the guard before transport or waiting.
pub struct IntegrationPhasePermit {
    pub key: IntegrationPhaseKey,
    _guard: Option<Box<dyn Send>>,
}
impl IntegrationPhasePermit {
    pub fn new(key: IntegrationPhaseKey) -> Self {
        Self { key, _guard: None }
    }
    pub fn with_guard(key: IntegrationPhaseKey, guard: Box<dyn Send>) -> Self {
        Self {
            key,
            _guard: Some(guard),
        }
    }
}
pub enum IntegrationDriveAdmission {
    Permit(IntegrationPhasePermit),
    Park(IntegrationPauseEvidence),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IntegrationHook {
    AfterPolicy,
    AfterSourceImport,
    AfterRunnerRetirement,
    AfterIntent,
    AfterFetchBeforePin,
    AfterTargetPin,
    AfterWorkspaceManifest,
    DuringWorkspacePrepare,
    AfterAuxPrepared,
    AfterAuxPrompt,
    AfterAuxCas,
    AfterAuxEnqueue,
    AfterAuxAccepted,
    AfterAuxCompleted,
    AfterCommitBeforePin,
    AfterMergePin,
    AfterPushIntent,
    AfterPushBeforeReceipt,
    AfterReceipt,
    AfterOwnerImport,
    AfterClose,
    AfterRevoke,
    AfterRevokeAck,
    TargetReserved,
    BeforePhasePermit,
    AfterPhaseAdmission,
    BeforeAuxAdmission,
    AfterPark,
    BeforeAdvertisement,
    AfterAdvertisement,
    BeforePush,
    BeforeRevokeAck,
    AfterStateBeforeEvent,
}
contract!(TargetReservation {
    key: TargetKey,
    integration_id: IntegrationId,
    epoch: u32,
    actor: ProcessIdentity,
});
impl ValidateIntegration for TargetReservation {
    fn validate(&self) -> Result<(), WorkerError> {
        self.key.validate()?;
        self.actor.validate()
    }
}
contract!(IntegrationReceipt {
    integration_id: IntegrationId, epoch: u32, source_turn_id: TurnId, source_head: BaseOid,
    target_head: BaseOid, merge_oid: Option<BaseOid>, disposition: IntegrationDisposition,
    imported: bool, recorded_at_millis: u64,
});
impl ValidateIntegration for IntegrationReceipt {
    fn validate(&self) -> Result<(), WorkerError> {
        if (self.disposition == IntegrationDisposition::Merged) != self.merge_oid.is_some() {
            return Err(invalid());
        }
        check_size(self, MAX_PUBLIC_SNAPSHOT_BYTES)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrationPushIntent {
    pub candidate: IntegrationCandidateId,
    pub expected_target: BaseOid,
    pub merge_oid: BaseOid,
    pub started_at_millis: u64,
    pub uncertain: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrationTombstone {
    pub epoch: u32,
    pub revision: IntegrationRevision,
    pub requested_at_millis: u64,
    pub acknowledged: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrationPhaseRetry {
    pub phase: IntegrationPhase,
    pub retries: u8,
    pub code: IntegrationCode,
    pub due_at_millis: u64,
}
contract!(IntegrationRecord {
    schema_version: u32, task_id: TaskId, policy: FrozenIntegrationPolicy,
    snapshot: IntegrationSnapshot, target_key: TargetKey, cycle_base: BaseOid,
    source_revision: String, source_summary: String, source_checks: Vec<ReportedCheck>,
    git_identity: GitIdentity, actor: Option<ProcessIdentity>,
    candidates: Vec<IntegrationCandidate>, auxiliaries: Vec<IntegrationAuxiliaryIntent>,
    archived_receipts: Vec<IntegrationReceipt>, push_intent: Option<IntegrationPushIntent>,
    receipt: Option<IntegrationReceipt>, tombstone: Option<IntegrationTombstone>,
    pause: Option<IntegrationPauseEvidence>, remaining_admission_millis: Option<u64>,
    admission_deadline_millis: Option<u64>, remaining_backoff_millis: Option<u64>,
    phase_retries: Vec<IntegrationPhaseRetry>, ready_at_millis: u64, run_position: u64,
    followups_spent: u32,
});
impl ValidateIntegration for IntegrationRecord {
    fn validate(&self) -> Result<(), WorkerError> {
        self.policy.validate()?;
        self.snapshot.validate()?;
        self.target_key.validate()?;
        if self.schema_version != INTEGRATION_SCHEMA_VERSION
            || self.target_key != self.policy.target_key()?
            || self.snapshot.integration_id
                != IntegrationId::derive(
                    self.task_id,
                    self.snapshot.source_turn_id,
                    &self.snapshot.source_head,
                    &self.target_key,
                )?
            || !valid_digest(&self.source_revision)
            || self.source_summary.len() > MAX_MESSAGE_SUMMARY_BYTES
            || self.candidates.len() > MAX_CANDIDATES
            || self.auxiliaries.len() > MAX_AUXILIARY_INTENTS
            || self.archived_receipts.len() > MAX_ARCHIVED_RECEIPTS
            || self
                .remaining_admission_millis
                .is_some_and(|v| v > AUXILIARY_ADMISSION_MILLIS)
            || self.remaining_backoff_millis.is_some_and(|v| v > 30_000)
            || self.phase_retries.len() > 8
            || self.phase_retries.iter().any(|r| r.retries > 3)
            || (self.snapshot.state == IntegrationStatus::Parked) != self.pause.is_some()
            || self
                .pause
                .is_some_and(|p| Some(p.reason) != self.snapshot.pause_reason)
            || (self.pause.is_some() && self.admission_deadline_millis.is_some())
        {
            return Err(invalid());
        }
        if let Some(actor) = self.actor {
            actor.validate()?;
        }
        for candidate in &self.candidates {
            candidate.validate()?;
            if candidate.id.integration_id != self.snapshot.integration_id
                || candidate.id.epoch != self.snapshot.epoch
                || candidate.source_head != self.snapshot.source_head
                || candidate.clean_h.branch != BranchName::for_task(self.task_id)
            {
                return Err(invalid());
            }
        }
        let attempts: std::collections::HashSet<_> = self
            .candidates
            .iter()
            .map(|candidate| candidate.id.attempt)
            .collect();
        if attempts.len() != self.candidates.len() {
            return Err(invalid());
        }
        for auxiliary in &self.auxiliaries {
            auxiliary.validate()?;
            if auxiliary.integration_id != self.snapshot.integration_id
                || auxiliary.epoch != self.snapshot.epoch
            {
                return Err(invalid());
            }
        }
        let turns: std::collections::HashSet<_> = self
            .auxiliaries
            .iter()
            .map(|auxiliary| auxiliary.turn_id)
            .collect();
        if turns.len() != self.auxiliaries.len() {
            return Err(invalid());
        }
        for receipt in &self.archived_receipts {
            receipt.validate()?;
        }
        if let Some(receipt) = &self.receipt {
            receipt.validate()?;
        }
        check_size(self, MAX_PRIVATE_RECORD_BYTES)
    }
}
// Complete preparation lives in its own sidecar; this is its compact reference.
contract!(IntegrationAuxiliaryIntent {
    turn_id: TurnId, integration_id: IntegrationId, epoch: u32, attempt: u8,
    purpose: IntegrationTurnPurpose, ordinal: u8, prepared_binding: String,
    created_at_millis: u64, queue_position: Option<u64>, accepted: bool, completed: bool,
});
impl ValidateIntegration for IntegrationAuxiliaryIntent {
    fn validate(&self) -> Result<(), WorkerError> {
        if self.turn_id
            != auxiliary_turn_id(
                self.integration_id,
                self.epoch,
                self.attempt,
                self.purpose,
                self.ordinal,
            )?
            || !valid_digest(&self.prepared_binding)
        {
            return Err(invalid());
        }
        Ok(())
    }
}
impl PreparedIntegrationTurn {
    pub fn intent(&self) -> Result<IntegrationAuxiliaryIntent, WorkerError> {
        Ok(IntegrationAuxiliaryIntent {
            turn_id: self.followup.turn_id(),
            integration_id: self.integration_id,
            epoch: self.epoch,
            attempt: self.attempt,
            purpose: self.purpose,
            ordinal: self.ordinal,
            prepared_binding: self.binding()?,
            created_at_millis: self.followup.created_at_millis(),
            queue_position: None,
            accepted: false,
            completed: false,
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrationTaskFacts {
    pub ordinary: LocalTaskRecord,
    pub cycle_base: BaseOid,
    pub result_imported: bool,
    pub session_import_complete: bool,
    pub continuation_pending: bool,
    pub runner_present: bool,
    pub stop_requested: bool,
    pub close_pending: bool,
    pub submission_pending: bool,
    pub auxiliary_purpose: Option<IntegrationTurnPurpose>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum HostIntegrationAction {
    Arm {
        policy: FrozenIntegrationPolicy,
    },
    Step {
        step: IntegrationStep,
        record: Box<IntegrationRecord>,
    },
    Read,
    Revoke {
        tombstone: IntegrationTombstone,
    },
}
contract!(HostIntegrationRequest {
    protocol_version: u32, task_id: TaskId, integration_id: Option<IntegrationId>,
    epoch: u32, revision: IntegrationRevision, action: HostIntegrationAction,
});
contract!(IntegrationResponseIdentity {
    protocol_version: u32, task_id: TaskId, integration_id: Option<IntegrationId>,
    epoch: u32, revision: IntegrationRevision,
});
impl ValidateIntegration for IntegrationResponseIdentity {
    fn validate(&self) -> Result<(), WorkerError> {
        if self.protocol_version != crate::protocol::PROTOCOL_VERSION
            || (self.integration_id.is_none() && (self.revision.0 != 0 || self.epoch != 0))
            || (self.integration_id.is_some() && self.revision.0 == 0)
        {
            return Err(invalid());
        }
        Ok(())
    }
}
impl IntegrationResponseIdentity {
    pub fn for_request(request: &HostIntegrationRequest) -> Self {
        Self {
            protocol_version: request.protocol_version,
            task_id: request.task_id,
            integration_id: request.integration_id,
            epoch: request.epoch,
            revision: request.revision,
        }
    }
}
impl ValidateIntegration for HostIntegrationRequest {
    fn validate(&self) -> Result<(), WorkerError> {
        IntegrationResponseIdentity::for_request(self).validate()?;
        match &self.action {
            HostIntegrationAction::Arm { policy } => {
                if self.integration_id.is_some() {
                    return Err(invalid());
                }
                policy.validate()?;
            }
            HostIntegrationAction::Step { record, .. } => {
                record.validate()?;
                if record.task_id != self.task_id
                    || Some(record.snapshot.integration_id) != self.integration_id
                    || record.snapshot.epoch != self.epoch
                    || record.snapshot.revision != self.revision
                {
                    return Err(invalid());
                }
            }
            HostIntegrationAction::Revoke { tombstone } => {
                if self.integration_id.is_none()
                    || tombstone.epoch != self.epoch
                    || tombstone.revision != self.revision
                {
                    return Err(invalid());
                }
            }
            HostIntegrationAction::Read => {}
        }
        check_size(self, MAX_INTEGRATION_RPC_BYTES)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "response", rename_all = "snake_case", deny_unknown_fields)]
pub enum HostIntegrationResponse {
    Progress {
        identity: IntegrationResponseIdentity,
        snapshot: Option<IntegrationSnapshot>,
    },
    NeedTurn {
        identity: IntegrationResponseIdentity,
        candidate: Box<IntegrationCandidate>,
        purpose: IntegrationTurnPurpose,
    },
    TargetMoved {
        identity: IntegrationResponseIdentity,
        observed_target: BaseOid,
    },
    Integrated {
        identity: IntegrationResponseIdentity,
        receipt: IntegrationReceipt,
    },
    Blocked {
        identity: IntegrationResponseIdentity,
        code: IntegrationCode,
        retry_exhausted: bool,
    },
    Revoked {
        identity: IntegrationResponseIdentity,
    },
}
impl HostIntegrationResponse {
    pub fn identity(&self) -> &IntegrationResponseIdentity {
        match self {
            Self::Progress { identity, .. }
            | Self::NeedTurn { identity, .. }
            | Self::TargetMoved { identity, .. }
            | Self::Integrated { identity, .. }
            | Self::Blocked { identity, .. }
            | Self::Revoked { identity } => identity,
        }
    }
    pub fn validate_for(&self, request: &HostIntegrationRequest) -> Result<(), WorkerError> {
        request.validate()?;
        self.validate()?;
        if self.identity() != &IntegrationResponseIdentity::for_request(request) {
            return Err(invalid());
        }
        Ok(())
    }
}
impl ValidateIntegration for HostIntegrationResponse {
    fn validate(&self) -> Result<(), WorkerError> {
        self.identity().validate()?;
        match self {
            Self::Progress { snapshot, identity } => {
                if snapshot.is_some() != identity.integration_id.is_some() {
                    return Err(invalid());
                }
                if let Some(snapshot) = snapshot {
                    snapshot.validate()?;
                    if Some(snapshot.integration_id) != identity.integration_id
                        || snapshot.epoch != identity.epoch
                        || snapshot.revision != identity.revision
                    {
                        return Err(invalid());
                    }
                }
            }
            Self::NeedTurn {
                candidate,
                identity,
                ..
            } => {
                candidate.validate()?;
                if Some(candidate.id.integration_id) != identity.integration_id
                    || candidate.id.epoch != identity.epoch
                    || candidate.clean_h.branch != BranchName::for_task(identity.task_id)
                {
                    return Err(invalid());
                }
            }
            Self::Integrated { receipt, identity } => {
                receipt.validate()?;
                if Some(receipt.integration_id) != identity.integration_id
                    || receipt.epoch != identity.epoch
                {
                    return Err(invalid());
                }
            }
            _ => {}
        }
        check_size(self, MAX_INTEGRATION_RPC_BYTES)
    }
}
contract!(FrozenIntegratingSubmit {
    submit: FrozenSubmitBody,
    integration: FrozenIntegrationPolicy
});
impl ValidateIntegration for FrozenIntegratingSubmit {
    fn validate(&self) -> Result<(), WorkerError> {
        self.integration.validate()?;
        if self.submit.close_on != ClosePolicy::Never
            || self.submit.wip
            || self.submit.project_id != self.integration.project_id
            || self
                .integration
                .base_oid
                .as_ref()
                .is_some_and(|base| base != &self.submit.base_oid)
            || self.submit.origin_url.as_deref() != Some(self.integration.origin.as_str())
            || self.submit.publish_branch.as_deref() == Some(self.integration.target.as_str())
        {
            return Err(invalid());
        }
        check_size(self, MAX_INTEGRATION_RPC_BYTES)
    }
}
contract!(FrozenIntegratingBatch {
    batch: FrozenBatchBody, integrations: BTreeMap<TaskId, Option<FrozenIntegrationPolicy>>,
});
impl ValidateIntegration for FrozenIntegratingBatch {
    fn validate(&self) -> Result<(), WorkerError> {
        let ids: std::collections::HashSet<_> =
            self.batch.nodes.values().map(|node| node.task_id).collect();
        if ids.len() != self.batch.nodes.len()
            || ids.len() != self.integrations.len()
            || !self.integrations.keys().all(|id| ids.contains(id))
        {
            return Err(invalid());
        }
        for policy in self.integrations.values().flatten() {
            policy.validate()?;
        }
        check_size(self, MAX_INTEGRATION_RPC_BYTES)
    }
}
contract!(IntegrationReadResult {
    schema_version: u32, integrations: BTreeMap<TaskId, Option<IntegrationSnapshot>>,
});
impl ValidateIntegration for IntegrationReadResult {
    fn validate(&self) -> Result<(), WorkerError> {
        if self.schema_version != INTEGRATION_SCHEMA_VERSION
            || self.integrations.len() > MAX_READ_TASKS
        {
            return Err(invalid());
        }
        for snapshot in self.integrations.values().flatten() {
            snapshot.validate()?;
        }
        check_size(self, MAX_INTEGRATION_RPC_BYTES)
    }
}

pub trait IntegrationHost: Send + Sync {
    fn execute(
        &self,
        request: &HostIntegrationRequest,
    ) -> Result<HostIntegrationResponse, WorkerError>;
}
pub trait IntegrationState: Send + Sync {
    fn load_policy(&self, task: TaskId) -> Result<Option<FrozenIntegrationPolicy>, WorkerError>;
    fn load(&self, task: TaskId) -> Result<Option<IntegrationRecord>, WorkerError>;
    fn publish_policy(
        &self,
        task: TaskId,
        policy: &FrozenIntegrationPolicy,
    ) -> Result<(), WorkerError>;
    fn replace(
        &self,
        task: TaskId,
        expected: IntegrationRevision,
        next: &IntegrationRecord,
    ) -> Result<bool, WorkerError>;
    fn reserve(
        &self,
        key: &TargetKey,
        id: IntegrationId,
        epoch: u32,
        actor: ProcessIdentity,
    ) -> Result<Option<TargetReservation>, WorkerError>;
    fn release(&self, reservation: &TargetReservation) -> Result<(), WorkerError>;
    fn due(&self, now_millis: u64, limit: usize) -> Result<Vec<TaskId>, WorkerError>;
}
pub trait IntegrationTurns: Send + Sync {
    fn enqueue(&self, prepared: &PreparedIntegrationTurn) -> Result<TurnId, WorkerError>;
}
pub trait IntegrationRuntime: Send + Sync {
    fn now_millis(&self) -> u64;
    fn actor(&self) -> ProcessIdentity;
    fn actor_verdict(&self, actor: ProcessIdentity) -> RunnerLivenessVerdict;
    fn begin_phase(
        &self,
        key: &IntegrationPhaseKey,
    ) -> Result<IntegrationDriveAdmission, WorkerError>;
    fn reach(&self, point: IntegrationHook);
}
pub trait IntegrationObserver: Send + Sync {
    fn facts(&self, task: TaskId) -> Result<IntegrationTaskFacts, WorkerError>;
}
contract!(IntegrationRedriveRequest {
    task_id: TaskId,
    expected: IntegrationRevision,
    request_id: String,
});
impl ValidateIntegration for IntegrationRedriveRequest {
    fn validate(&self) -> Result<(), WorkerError> {
        let id = uuid::Uuid::parse_str(&self.request_id).map_err(|_| invalid())?;
        if id.is_nil() || id.simple().to_string() != self.request_id || self.expected.0 == 0 {
            return Err(invalid());
        }
        Ok(())
    }
}

pub fn validate_integration_target(target: &str) -> Result<BranchName, WorkerError> {
    if target.len() > MAX_TARGET_BYTES {
        return Err(integration_error("TASK_CONFIG_INVALID"));
    }
    target
        .parse()
        .map_err(|_| integration_error("TASK_CONFIG_INVALID"))
}
pub fn public_target_display(target: &str, boundary: &RedactionBoundary) -> String {
    let redacted = boundary.text(target, usize::MAX);
    if redacted.len() <= MAX_TARGET_DISPLAY_BYTES {
        return redacted;
    }
    let mut end = MAX_TARGET_DISPLAY_BYTES - '…'.len_utf8();
    while !redacted.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", boundary.text(&redacted[..end], end))
}
pub fn validate_relative_path(path: &str) -> Result<(), WorkerError> {
    if path.is_empty()
        || path.len() > MAX_CONFLICT_PATH_BYTES
        || path.contains(['\\', '\0', ':'])
        || path.chars().any(char::is_control)
        || path
            .split('/')
            .any(|p| p.is_empty() || p == "." || p == "..")
    {
        return Err(integration_error("INTEGRATION_CONFLICT_LIST_TOO_LARGE"));
    }
    Ok(())
}
pub fn validate_conflict_paths(paths: &[String]) -> Result<(), WorkerError> {
    if paths.len() > MAX_CONFLICT_PATHS {
        return Err(integration_error("INTEGRATION_CONFLICT_LIST_TOO_LARGE"));
    }
    for path in paths {
        validate_relative_path(path)?;
    }
    if serde_json::to_vec(paths).map_err(|_| invalid())?.len() > MAX_PROMPT_BYTES {
        return Err(integration_error("INTEGRATION_CONFLICT_LIST_TOO_LARGE"));
    }
    Ok(())
}
pub fn validate_prompt(prompt: &str) -> Result<(), WorkerError> {
    if prompt.len() > MAX_PROMPT_BYTES {
        return Err(integration_error("INTEGRATION_CONFLICT_LIST_TOO_LARGE"));
    }
    Ok(())
}
fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
pub fn check_size<T: Serialize>(value: &T, max: usize) -> Result<(), WorkerError> {
    if serde_json::to_vec(value).map_err(|_| invalid())?.len() > max {
        return Err(invalid());
    }
    Ok(())
}
pub fn encode_bounded<T: ValidateIntegration + Serialize>(
    value: &T,
    max: usize,
) -> Result<Vec<u8>, WorkerError> {
    value.validate()?;
    check_size(value, max)?;
    serde_json::to_vec(value).map_err(|_| invalid())
}
pub fn decode_bounded<T: ValidateIntegration + DeserializeOwned>(
    bytes: &[u8],
    max: usize,
) -> Result<T, WorkerError> {
    if bytes.len() > max {
        return Err(invalid());
    }
    let value: T = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    value.validate()?;
    Ok(value)
}
pub fn encode_snapshot(value: &IntegrationSnapshot) -> Result<Vec<u8>, WorkerError> {
    encode_bounded(value, MAX_PUBLIC_SNAPSHOT_BYTES)
}
pub fn decode_snapshot(bytes: &[u8]) -> Result<IntegrationSnapshot, WorkerError> {
    decode_bounded(bytes, MAX_PUBLIC_SNAPSHOT_BYTES)
}
pub fn encode_record(value: &IntegrationRecord) -> Result<Vec<u8>, WorkerError> {
    encode_bounded(value, MAX_PRIVATE_RECORD_BYTES)
}
pub fn decode_record(bytes: &[u8]) -> Result<IntegrationRecord, WorkerError> {
    decode_bounded(bytes, MAX_PRIVATE_RECORD_BYTES)
}
pub fn encode_prepared_turn(value: &PreparedIntegrationTurn) -> Result<Vec<u8>, WorkerError> {
    encode_bounded(value, MAX_PREPARED_TURN_BYTES)
}
pub fn decode_prepared_turn(bytes: &[u8]) -> Result<PreparedIntegrationTurn, WorkerError> {
    decode_bounded(bytes, MAX_PREPARED_TURN_BYTES)
}
pub fn encode_host_request(value: &HostIntegrationRequest) -> Result<Vec<u8>, WorkerError> {
    encode_bounded(value, MAX_INTEGRATION_RPC_BYTES)
}
pub fn decode_host_request(bytes: &[u8]) -> Result<HostIntegrationRequest, WorkerError> {
    decode_bounded(bytes, MAX_INTEGRATION_RPC_BYTES)
}
pub fn encode_host_response(value: &HostIntegrationResponse) -> Result<Vec<u8>, WorkerError> {
    encode_bounded(value, MAX_INTEGRATION_RPC_BYTES)
}
pub fn decode_host_response(bytes: &[u8]) -> Result<HostIntegrationResponse, WorkerError> {
    decode_bounded(bytes, MAX_INTEGRATION_RPC_BYTES)
}

pub fn auxiliary_turn_id(
    id: IntegrationId,
    epoch: u32,
    attempt: u8,
    purpose: IntegrationTurnPurpose,
    ordinal: u8,
) -> Result<TurnId, WorkerError> {
    let cap = match purpose {
        IntegrationTurnPurpose::Resolve => MAX_RESOLVE_TURNS,
        IntegrationTurnPurpose::Verify => MAX_VERIFY_TURNS,
    };
    if attempt == 0 || attempt as usize > MAX_CANDIDATES || ordinal == 0 || ordinal > cap {
        return Err(invalid());
    }
    let mut digest = Sha256::new();
    digest.update(b"mac-worker/integration-turn/v1\0");
    digest.update(id.as_uuid().as_bytes());
    digest.update(epoch.to_be_bytes());
    digest.update([
        attempt,
        match purpose {
            IntegrationTurnPurpose::Resolve => 0,
            IntegrationTurnPurpose::Verify => 1,
        },
        ordinal,
    ]);
    Ok(TurnId::new(uuid_v8(digest)))
}
fn uuid_v8(digest: Sha256) -> uuid::Uuid {
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.finalize()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn task() -> TaskId {
        TaskId::new(uuid::Uuid::from_u128(2))
    }
    fn source() -> TurnId {
        TurnId::new(uuid::Uuid::from_u128(3))
    }
    fn head() -> BaseOid {
        "a".repeat(40).parse().unwrap()
    }

    #[test]
    fn canonical_identity_binds_task_source_head_and_target() {
        let key = TargetKey::new(
            "https://user:password@EXAMPLE.test/repo.git?secret=x",
            "main",
        )
        .unwrap();
        assert_eq!(key.origin, "https://example.test/repo.git");
        let id = IntegrationId::derive(task(), source(), &head(), &key).unwrap();
        assert_eq!(id.to_string(), "df525afd-647b-82bb-884e-d5eb88429385");
        assert_eq!(
            id,
            IntegrationId::derive(
                task(),
                source(),
                &head(),
                &TargetKey::new("https://example.test/repo.git", "main").unwrap()
            )
            .unwrap()
        );
        for (task, source, head, key) in [
            (
                TaskId::new(uuid::Uuid::from_u128(4)),
                source(),
                head(),
                key.clone(),
            ),
            (
                task(),
                TurnId::new(uuid::Uuid::from_u128(4)),
                head(),
                key.clone(),
            ),
            (
                task(),
                source(),
                "b".repeat(40).parse().unwrap(),
                key.clone(),
            ),
            (
                task(),
                source(),
                head(),
                TargetKey::new("https://example.test/repo.git", "other").unwrap(),
            ),
            (
                task(),
                source(),
                head(),
                TargetKey::new("https://example.test/other.git", "main").unwrap(),
            ),
        ] {
            assert_ne!(
                id,
                IntegrationId::derive(task, source, &head, &key).unwrap()
            );
        }
    }

    #[test]
    fn auxiliary_identity_binds_epoch_attempt_purpose_and_ordinal() {
        let id: IntegrationId = "df525afd-647b-82bb-884e-d5eb88429385".parse().unwrap();
        let turn = auxiliary_turn_id(id, 0, 1, IntegrationTurnPurpose::Resolve, 1).unwrap();
        assert_eq!(turn.to_string(), "171ac900f4dc82d4987e2d7b0a1b7281");
        for (epoch, attempt, purpose, ordinal) in [
            (1, 1, IntegrationTurnPurpose::Resolve, 1),
            (0, 2, IntegrationTurnPurpose::Resolve, 1),
            (0, 1, IntegrationTurnPurpose::Verify, 1),
            (0, 1, IntegrationTurnPurpose::Resolve, 2),
        ] {
            assert_ne!(
                turn,
                auxiliary_turn_id(id, epoch, attempt, purpose, ordinal).unwrap()
            );
        }
        assert!(auxiliary_turn_id(id, 0, 4, IntegrationTurnPurpose::Resolve, 1).is_err());
        assert!(auxiliary_turn_id(id, 0, 1, IntegrationTurnPurpose::Resolve, 3).is_err());
    }

    #[test]
    fn from_task_policy_preserves_unresolved_base_provenance() {
        assert_eq!(
            serde_json::to_value(IntegrationBaseKind::FromTask).unwrap(),
            json!("from-task")
        );
        let policy = json!({
            "schema_version":1,"origin":"https://example.test/repo.git","target":"main",
            "verify":"never","requested_close":"never","base_kind":"from-task",
            "base_oid":null,"base_task":task(),"base_preflight":"unknown","project_id":"a".repeat(64),
        });
        assert!(serde_json::from_value::<FrozenIntegrationPolicy>(policy.clone()).is_ok());
        let mut invalid = policy;
        invalid["base_task"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<FrozenIntegrationPolicy>(invalid).is_err());
    }

    #[test]
    fn every_host_reply_retains_protocol_task_intent_epoch_and_revision() {
        let wire = json!({
            "response":"target_moved","identity":{
                "protocol_version":7,"task_id":task(),
                "integration_id":"df525afd-647b-82bb-884e-d5eb88429385",
                "epoch":2,"revision":9,
            },"observed_target":"b".repeat(40),
        });
        let response = decode_host_response(&serde_json::to_vec(&wire).unwrap()).unwrap();
        assert_eq!(serde_json::to_value(response).unwrap(), wire);
        let mut invalid = wire;
        invalid["identity"]["protocol_version"] = json!(6);
        assert!(decode_host_response(&serde_json::to_vec(&invalid).unwrap()).is_err());
    }
    #[test]
    fn arm_freezes_policy_before_a_final_source_identity_exists() {
        let request = json!({
            "protocol_version":7,"task_id":task(),"integration_id":null,"epoch":0,"revision":0,
            "action":{"action":"arm","policy":{
                "schema_version":1,"origin":"https://example.test/repo.git","target":"main",
                "verify":"never","requested_close":"done","base_kind":"committed",
                "base_oid":"b".repeat(40),"base_task":null,"base_preflight":"pass",
                "project_id":"a".repeat(64),
            }},
        });
        let request = decode_host_request(&serde_json::to_vec(&request).unwrap()).unwrap();
        let response = json!({"response":"progress","snapshot":null,"identity":{
            "protocol_version":7,"task_id":task(),"integration_id":null,"epoch":0,"revision":0,
        }});
        let response = decode_host_response(&serde_json::to_vec(&response).unwrap()).unwrap();
        response.validate_for(&request).unwrap();
    }

    #[test]
    fn transition_data_covers_every_state_and_keeps_push_stop_fenced() {
        assert_eq!(INTEGRATION_TRANSITIONS.len(), 14);
        let absent = INTEGRATION_TRANSITIONS
            .iter()
            .find(|row| row.from.is_none())
            .unwrap();
        assert_eq!(absent.next, [IntegrationStatus::Armed]);
        let pushing = INTEGRATION_TRANSITIONS
            .iter()
            .find(|row| row.from == Some(IntegrationStatus::Pushing))
            .unwrap();
        assert_eq!(
            pushing.next,
            [
                IntegrationStatus::Published,
                IntegrationStatus::Fetching,
                IntegrationStatus::RetryWait,
                IntegrationStatus::Parked,
                IntegrationStatus::Blocked,
            ]
        );
        assert!(!pushing.next.contains(&IntegrationStatus::Revoked));
        let parked = INTEGRATION_TRANSITIONS
            .iter()
            .find(|row| row.from == Some(IntegrationStatus::Parked))
            .unwrap();
        assert!(parked.resume && parked.legacy_settlement);
        let blocked = INTEGRATION_TRANSITIONS
            .iter()
            .find(|row| row.from == Some(IntegrationStatus::Blocked))
            .unwrap();
        assert!(blocked.manual_redrive && blocked.legacy_settlement);
        for state in [IntegrationStatus::Integrated, IntegrationStatus::Revoked] {
            let row = INTEGRATION_TRANSITIONS
                .iter()
                .find(|row| row.from == Some(state))
                .unwrap();
            assert!(row.new_cycle);
            assert!(row.next.is_empty());
        }
    }
}
