use std::{
    collections::{BTreeMap, VecDeque},
    ffi::{OsStr, OsString},
    fmt,
    fs::OpenOptions,
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

use serde::{de::Error as DeError, ser::SerializeStruct};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    agent::{
        AgentKind, PermissionPolicy, Question, TurnLaunch, TurnLimits, adapter_for,
        parse_prebind_session_ref, prebind_login_request, render_shell,
    },
    error::WorkerError,
    host_store::{HostStore, PublicationReceipt, StagedJob, StagingNonce},
    job::JobId,
    job::{
        CommandSpec, JobMeta, LeaseRecord, MAX_LOG_CHUNK_BYTES, RequestFingerprintMaterial,
        SubmitRequest, SubmitResponse,
    },
    outbox::{DeliveryCommit, OriginOutbox},
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
    redaction::RedactionBoundary,
    rooted_fs::RootedDir,
    task::{
        BaseOid, BranchName, ClosePolicy, GitIdentity, PublishMode, TaskId, TaskMeta, TaskOutcome,
        TaskStatus, TurnTerminal,
    },
    task_store::{
        SessionBinding, TaskCloseRequest, TaskPrebindRequest, TaskSessionResponse, TaskStore,
    },
};

pub const LOG_CAP_BYTES: u64 = 256 * 1024 * 1024;
pub const LOG_TAIL_BYTES: usize = 64 * 1024;
/// stderr.log is not a strict `LOG_CAP_BYTES` file: the pump writes a prefix of
/// at most `LOG_CAP_BYTES`, then may append up to `LOG_TAIL_BYTES` of the late
/// stream so a post-cap failure remains visible. Disk bound, not a cap.
pub const STDERR_LOG_BOUND_BYTES: u64 = LOG_CAP_BYTES + LOG_TAIL_BYTES as u64;
pub(crate) const MAX_NDJSON_RECORD_BYTES: usize = 1024 * 1024;
const MAX_ENV_PROFILE_BYTES: u64 = 64 * 1024;
const MAX_ENV_NAME_BYTES: usize = 128;
const MAX_ENV_VALUE_BYTES: usize = 64 * 1024;
const PARSE_REASON_FILE: &str = "result-parse-reason.json";

/// These immutable, private diagnostics survive terminal cleanup so publication
/// and postmortem inspection can be retried after a supervisor handoff.
pub(crate) fn retained_diagnostic_files(dir: &RootedDir) -> Result<Vec<String>, WorkerError> {
    let mut retained = Vec::new();
    for (name, limit) in [
        (crate::agent::identity::IDENTITY_FILE, 8192),
        (PARSE_REASON_FILE, 256),
        (crate::project_readiness::SETUP_RESULT_FILE, 4096),
    ] {
        if dir.entry_exists(name)? {
            dir.read_private_regular(name, limit)?;
            retained.push(name.into());
        }
    }
    Ok(retained)
}

fn preparation_failure_outcome(dir: &RootedDir) -> Result<Option<TaskOutcome>, WorkerError> {
    let Some(failure) = crate::project_readiness::load_setup_stage_result(dir)? else {
        return Ok(None);
    };
    let reason = match failure.code.as_str() {
        "SETUP_INPUTS_CHANGED" => {
            "SETUP_INPUTS_CHANGED: review setup inputs and submit a new task to approve them"
        }
        "AGENT_IDENTITY_PROBE_FAILED" => {
            // Preserve the public reason for legacy turns refused before
            // version observation became best effort.
            "AGENT_IDENTITY_PROBE_FAILED: agent --version exceeded its time or output bound"
        }
        "AGENT_EXECUTABLE_NOT_FOUND" => {
            "AGENT_EXECUTABLE_NOT_FOUND: check the agent PATH in the login shell and env profile"
        }
        crate::agent::OPENCODE_DIALECT_MISMATCH => {
            "OPENCODE_DIALECT_MISMATCH: the worker's OpenCode is not the generation this turn was built for, so it was not started; run `worker workers --refresh` and retry"
        }
        crate::agent::OPENCODE_VERSION_UNVERIFIED => {
            "OPENCODE_VERSION_UNVERIFIED: `opencode --version` gave no version on the worker, so the turn was not started without --standalone; check OpenCode in the worker's login shell"
        }
        _ => return Ok(None),
    };
    Ok(Some(TaskOutcome::failed(reason)))
}

fn persist_parse_reason(
    dir: &RootedDir,
    reason: Option<crate::agent::ResultParseReason>,
) -> Result<(), WorkerError> {
    if let Some(reason) = reason {
        let bytes = serde_json::to_vec(&reason)
            .map_err(|_| turn_error("PUBLISH_FAILED", "cannot encode parse reason"))?;
        if !dir.entry_exists(PARSE_REASON_FILE)? {
            dir.write_private_atomic_no_replace(PARSE_REASON_FILE, &bytes)?;
        }
    }
    Ok(())
}

/// Adopt diagnostics staged by the prepare-turn helper into the retained turn
/// directory. Callers run in the supervisor or recovery after the child is
/// gone, so no process that a cancel can kill writes these private records.
/// Best effort: a missing, partial or invalid staged file is skipped, which
/// only drops a diagnostic.
pub(crate) fn adopt_staged_turn_diagnostics(turn_dir: &RootedDir) {
    use crate::agent::identity::{IDENTITY_FILE, IDENTITY_MAX_BYTES, STAGED_IDENTITY_FILE};
    use crate::project_readiness::{
        SETUP_RESULT_FILE, SETUP_RESULT_MAX_BYTES, STAGED_SETUP_RESULT_FILE, SetupStageResult,
    };
    adopt_staged_record::<crate::agent::AgentIdentity>(
        turn_dir,
        STAGED_IDENTITY_FILE,
        IDENTITY_FILE,
        IDENTITY_MAX_BYTES,
    );
    adopt_staged_record::<SetupStageResult>(
        turn_dir,
        STAGED_SETUP_RESULT_FILE,
        SETUP_RESULT_FILE,
        SETUP_RESULT_MAX_BYTES,
    );
}

fn adopt_staged_record<T: serde::de::DeserializeOwned + serde::Serialize>(
    turn_dir: &RootedDir,
    staged: &str,
    retained: &str,
    maximum: u64,
) {
    let _ = (|| -> Result<(), WorkerError> {
        if turn_dir.entry_exists(retained)? {
            return Ok(());
        }
        let tmp = crate::inputs::RelativePath::parse(b"tmp")
            .map_err(|error| WorkerError::Protocol(error.to_string()))?;
        let tmp = match turn_dir.open_child_directory(&tmp, false) {
            Ok(tmp) => tmp,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let bytes = match tmp.read_private_regular(staged, maximum) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let record: T = serde_json::from_slice(&bytes)
            .map_err(|_| turn_error("PUBLISH_FAILED", "invalid staged turn diagnostic"))?;
        let bytes = serde_json::to_vec(&record)
            .map_err(|_| turn_error("PUBLISH_FAILED", "cannot encode turn diagnostic"))?;
        turn_dir.write_private_atomic_no_replace(retained, &bytes)?;
        Ok(())
    })();
}

pub(crate) fn attach_turn_diagnostics(
    store: &HostStore,
    project: &str,
    task: TaskId,
    turn: crate::task::TurnId,
    summary: crate::task::TurnSummary,
) -> Result<crate::task::TurnSummary, WorkerError> {
    let meta =
        TaskStore::new(store, &crate::process::SystemProcessRunner).load_meta(project, task)?;
    let dir = match store.open_directory(
        &format!("jobs/{project}/{}/{turn}", meta.worktree_id()),
        false,
    ) {
        Ok(dir) => dir,
        Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(summary);
        }
        Err(error) => return Err(error),
    };
    let reason = match dir.read_private_regular(PARSE_REASON_FILE, 256) {
        Ok(bytes) => Some(
            serde_json::from_slice(&bytes)
                .map_err(|_| turn_error("PUBLISH_FAILED", "invalid result parse reason"))?,
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let identity: Option<crate::agent::AgentIdentity> =
        match dir.read_private_regular(crate::agent::identity::IDENTITY_FILE, 8192) {
            Ok(bytes) => Some(
                serde_json::from_slice(&bytes)
                    .map_err(|_| turn_error("PUBLISH_FAILED", "invalid agent identity"))?,
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
    let boundary = publication_boundary(&dir)?;
    Ok(summary
        .with_parse_reason(reason)
        .with_agent_identity(identity.map(|identity| identity.redacted(&boundary))))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnMaterial {
    task_id: TaskId,
    turn_number: u32,
    agent: AgentKind,
    model: Option<String>,
    effort: Option<String>,
    policy: PermissionPolicy,
    effective_policy: Option<PermissionPolicy>,
    limits: TurnLimits,
    base_oid: BaseOid,
    prompt_sha256: String,
    env_profile: Option<String>,
    session_seed: Uuid,
    resume: bool,
    frozen_setup: Option<crate::project_readiness::FrozenSetup>,
}

impl TurnMaterial {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_id: TaskId,
        turn_number: u32,
        agent: AgentKind,
        model: Option<String>,
        effort: Option<String>,
        policy: PermissionPolicy,
        limits: TurnLimits,
        base_oid: BaseOid,
        prompt_sha256: String,
        env_profile: Option<String>,
        session_seed: Uuid,
        resume: bool,
    ) -> Result<Self, WorkerError> {
        let material = Self {
            task_id,
            turn_number,
            agent,
            model,
            effort,
            policy,
            effective_policy: None,
            limits,
            base_oid,
            prompt_sha256,
            env_profile,
            session_seed,
            resume,
            frozen_setup: None,
        };
        material.validate()?;
        Ok(material)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_prompt(
        task_id: TaskId,
        turn_number: u32,
        agent: AgentKind,
        model: Option<String>,
        effort: Option<String>,
        policy: PermissionPolicy,
        limits: TurnLimits,
        base_oid: BaseOid,
        prompt: impl AsRef<[u8]>,
        env_profile: Option<String>,
        session_seed: Uuid,
        resume: bool,
    ) -> Result<Self, WorkerError> {
        let prompt = prompt.as_ref();
        if prompt.len() > crate::task::MAX_PROMPT_BYTES {
            return Err(turn_error(
                "PROMPT_TOO_LARGE",
                "turn prompt exceeds the supported size",
            ));
        }
        Self::new(
            task_id,
            turn_number,
            agent,
            model,
            effort,
            policy,
            limits,
            base_oid,
            format!("{:x}", Sha256::digest(prompt)),
            env_profile,
            session_seed,
            resume,
        )
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, WorkerError> {
        serde_json::to_vec(self)
            .map_err(|error| turn_error("TURN_MATERIAL_INVALID", error.to_string()))
    }

    pub fn digest(&self) -> String {
        format!(
            "{:x}",
            Sha256::digest(self.canonical_bytes().expect("validated turn material"))
        )
    }

    pub fn v1_material(
        &self,
        lease: &LeaseRecord,
        launch: &TurnLaunch,
    ) -> Result<RequestFingerprintMaterial, WorkerError> {
        let shell = render_shell(launch)
            .map_err(|error| turn_error("TURN_COMMAND_INVALID", error.to_string()))?;
        RequestFingerprintMaterial::new(
            lease.job_id(),
            lease.client_id(),
            lease.lease_token(),
            lease.created_at_millis(),
            lease.worker_name().into(),
            lease.project_id().into(),
            lease.worktree_id().into(),
            self.digest(),
            String::new(),
            self.limits.timeout_millis,
            "heavy".into(),
            CommandSpec::shell(shell)?,
        )
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn turn_number(&self) -> u32 {
        self.turn_number
    }

    pub fn agent(&self) -> AgentKind {
        self.agent
    }

    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    pub fn effort(&self) -> Option<&str> {
        self.effort.as_deref()
    }

    pub fn policy(&self) -> PermissionPolicy {
        self.policy
    }

    pub fn effective_policy(&self) -> Option<PermissionPolicy> {
        self.effective_policy
    }

    pub fn with_effective_policy(
        mut self,
        effective: PermissionPolicy,
    ) -> Result<Self, WorkerError> {
        self.effective_policy = (effective != self.policy).then_some(effective);
        self.validate()?;
        Ok(self)
    }

    pub fn limits(&self) -> &TurnLimits {
        &self.limits
    }

    pub fn base_oid(&self) -> &BaseOid {
        &self.base_oid
    }

    pub fn prompt_sha256(&self) -> &str {
        &self.prompt_sha256
    }

    pub fn env_profile(&self) -> Option<&str> {
        self.env_profile.as_deref()
    }

    pub fn session_seed(&self) -> Uuid {
        self.session_seed
    }

    pub fn resume(&self) -> bool {
        self.resume
    }

    pub fn with_frozen_setup(
        mut self,
        setup: Option<crate::project_readiness::FrozenSetup>,
    ) -> Result<Self, WorkerError> {
        if let Some(setup) = &setup {
            setup.validate()?;
        }
        self.frozen_setup = setup;
        Ok(self)
    }

    pub fn frozen_setup(&self) -> Option<&crate::project_readiness::FrozenSetup> {
        self.frozen_setup.as_ref()
    }

    fn validate(&self) -> Result<(), WorkerError> {
        if self.turn_number == 0 {
            return Err(turn_error("TURN_INVALID", "turn number must be positive"));
        }
        self.limits
            .validate()
            .map_err(|error| turn_error("TURN_INVALID", error.to_string()))?;
        validate_hex(&self.prompt_sha256, "prompt digest")?;
        if let Some(model) = &self.model {
            validate_text(model, 256, "model")?;
        }
        if let Some(effort) = &self.effort {
            crate::agent::validate_effort(effort)
                .map_err(|error| turn_error("TURN_INVALID", error.to_string()))?;
        }
        if let Some(profile) = &self.env_profile {
            validate_text(profile, 128, "environment profile")?;
            if profile.contains('/') || profile.contains('\\') || profile == "." || profile == ".."
            {
                return Err(turn_error(
                    "TURN_INVALID",
                    "environment profile name is invalid",
                ));
            }
        }
        if self.session_seed.is_nil() {
            return Err(turn_error("TURN_INVALID", "session seed must be non-nil"));
        }
        Ok(())
    }
}

impl serde::Serialize for TurnMaterial {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(serde::ser::Error::custom)?;
        // Omitted when unset so a turn without an effort keeps the canonical
        // bytes, and therefore the digest, it had before the field existed.
        let mut record = serializer.serialize_struct(
            "TurnMaterial",
            11 + usize::from(self.effort.is_some())
                + usize::from(self.effective_policy.is_some())
                + usize::from(self.frozen_setup.is_some()),
        )?;
        record.serialize_field("task_id", &self.task_id)?;
        record.serialize_field("turn_number", &self.turn_number)?;
        record.serialize_field("agent", &agent_name(self.agent))?;
        record.serialize_field("model", &self.model)?;
        if self.effort.is_some() {
            record.serialize_field("effort", &self.effort)?;
        }
        record.serialize_field("policy", &policy_name(self.policy))?;
        if let Some(effective) = self.effective_policy {
            record.serialize_field("effective_policy", &policy_name(effective))?;
        }
        record.serialize_field("limits", &TurnLimitsWire::from(&self.limits))?;
        record.serialize_field("base_oid", &self.base_oid)?;
        record.serialize_field("prompt_sha256", &self.prompt_sha256)?;
        record.serialize_field("env_profile", &self.env_profile)?;
        record.serialize_field("session_seed", &self.session_seed.to_string())?;
        record.serialize_field("resume", &self.resume)?;
        if let Some(setup) = &self.frozen_setup {
            record.serialize_field("frozen_setup", setup)?;
        }
        record.end()
    }
}

impl<'de> serde::Deserialize<'de> for TurnMaterial {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            task_id: TaskId,
            turn_number: u32,
            agent: String,
            model: Option<String>,
            #[serde(default)]
            effort: Option<String>,
            policy: String,
            #[serde(default)]
            effective_policy: Option<String>,
            limits: TurnLimitsWire,
            base_oid: BaseOid,
            prompt_sha256: String,
            env_profile: Option<String>,
            session_seed: String,
            resume: bool,
            #[serde(default)]
            frozen_setup: Option<crate::project_readiness::FrozenSetup>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let session_seed = Uuid::parse_str(&wire.session_seed)
            .map_err(|_| D::Error::custom("session seed is invalid"))?;
        let material = Self::new(
            wire.task_id,
            wire.turn_number,
            parse_agent(&wire.agent).map_err(D::Error::custom)?,
            wire.model,
            wire.effort,
            parse_policy(&wire.policy).map_err(D::Error::custom)?,
            wire.limits.into_limits().map_err(D::Error::custom)?,
            wire.base_oid,
            wire.prompt_sha256,
            wire.env_profile,
            session_seed,
            wire.resume,
        )
        .and_then(|material| material.with_frozen_setup(wire.frozen_setup))
        .map_err(D::Error::custom)?;
        match wire.effective_policy {
            Some(effective) => material
                .with_effective_policy(parse_policy(&effective).map_err(D::Error::custom)?)
                .map_err(D::Error::custom),
            None => Ok(material),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnSection {
    turn: TurnMaterial,
    project_id: String,
    git_identity: GitIdentity,
    origin_url: Option<String>,
    herdr_reporter: bool,
}

#[derive(Clone, PartialEq, Eq)]
pub struct TurnReceipt {
    job_id: JobId,
    task_id: TaskId,
    prepared_head: BaseOid,
    staging_nonce: StagingNonce,
}

impl fmt::Debug for TurnReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TurnReceipt")
            .field("job_id", &self.job_id)
            .field("task_id", &self.task_id)
            .field("prepared_head", &self.prepared_head)
            .finish_non_exhaustive()
    }
}

impl TurnReceipt {
    pub(crate) fn new(
        job_id: JobId,
        task_id: TaskId,
        prepared_head: BaseOid,
        staging_nonce: StagingNonce,
    ) -> Self {
        Self {
            job_id,
            task_id,
            prepared_head,
            staging_nonce,
        }
    }

    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn prepared_head(&self) -> &BaseOid {
        &self.prepared_head
    }
}

impl PublicationReceipt for TurnReceipt {
    fn validate_for(&self, staged: &StagedJob) -> Result<(), WorkerError> {
        if self.job_id != staged.job_id() || self.staging_nonce != staged.receipt_nonce() {
            return Err(turn_error(
                "JOB_ID_CONFLICT",
                "turn receipt does not match staged job",
            ));
        }
        Ok(())
    }
}

impl TurnSection {
    pub fn new(
        turn: TurnMaterial,
        project_id: impl Into<String>,
        git_identity: GitIdentity,
    ) -> Result<Self, WorkerError> {
        Self::new_with_origin(turn, project_id, git_identity, None)
    }

    pub fn new_with_origin(
        turn: TurnMaterial,
        project_id: impl Into<String>,
        git_identity: GitIdentity,
        origin_url: Option<String>,
    ) -> Result<Self, WorkerError> {
        let section = Self {
            turn,
            project_id: project_id.into(),
            git_identity,
            origin_url,
            herdr_reporter: false,
        };
        section.turn.validate()?;
        validate_origin_url(section.origin_url.as_deref())?;
        if section.project_id.is_empty()
            || section.project_id.len() != 64
            || !section
                .project_id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(turn_error("TURN_INVALID", "turn project ID is invalid"));
        }
        Ok(section)
    }

    /// Whether the worker reports this turn to its herdr server.  A
    /// per-worker choice the laptop makes from its configuration, carried
    /// beside the other per-worker values and outside the turn digest.
    pub fn with_herdr_reporter(mut self, enabled: bool) -> Self {
        self.herdr_reporter = enabled;
        self
    }

    pub fn herdr_reporter(&self) -> bool {
        self.herdr_reporter
    }

    pub fn turn(&self) -> &TurnMaterial {
        &self.turn
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn git_identity(&self) -> &GitIdentity {
        &self.git_identity
    }

    pub fn origin_url(&self) -> Option<&str> {
        self.origin_url.as_deref()
    }
}

impl serde::Serialize for TurnSection {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // The flag is written only when set, so records of turns that
        // predate it keep their exact bytes.
        let fields = if self.herdr_reporter { 5 } else { 4 };
        let mut record = serializer.serialize_struct("TurnSection", fields)?;
        record.serialize_field("turn", &self.turn)?;
        record.serialize_field("project_id", &self.project_id)?;
        record.serialize_field("git_identity", &self.git_identity)?;
        record.serialize_field("origin_url", &self.origin_url)?;
        if self.herdr_reporter {
            record.serialize_field("herdr_reporter", &self.herdr_reporter)?;
        }
        record.end()
    }
}

impl<'de> serde::Deserialize<'de> for TurnSection {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            turn: TurnMaterial,
            project_id: String,
            git_identity: GitIdentity,
            origin_url: Option<String>,
            #[serde(default)]
            herdr_reporter: bool,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::new_with_origin(
            wire.turn,
            wire.project_id,
            wire.git_identity,
            wire.origin_url,
        )
        .map(|section| section.with_herdr_reporter(wire.herdr_reporter))
        .map_err(D::Error::custom)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct TaskTurnRequest {
    submit: SubmitRequest,
    turn: TurnMaterial,
    prompt: String,
    origin_url: Option<String>,
    herdr_reporter: bool,
}

impl fmt::Debug for TaskTurnRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskTurnRequest")
            .field("job_id", &self.submit.material().job_id())
            .field("task_id", &self.turn.task_id)
            .field("turn_number", &self.turn.turn_number)
            .finish_non_exhaustive()
    }
}

impl TaskTurnRequest {
    pub fn new(submit: SubmitRequest, turn: TurnMaterial, prompt: impl Into<String>) -> Self {
        Self {
            submit,
            turn,
            prompt: prompt.into(),
            origin_url: None,
            herdr_reporter: false,
        }
    }

    pub fn new_with_origin(
        submit: SubmitRequest,
        turn: TurnMaterial,
        prompt: impl Into<String>,
        origin_url: Option<String>,
    ) -> Result<Self, WorkerError> {
        let request = Self {
            submit,
            turn,
            prompt: prompt.into(),
            origin_url,
            herdr_reporter: false,
        };
        validate_origin_url(request.origin_url.as_deref())?;
        Ok(request)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        self.submit.validate()?;
        self.turn.validate()?;
        validate_origin_url(self.origin_url.as_deref())?;
        let prompt = self.prompt.as_bytes();
        if prompt.len() > crate::task::MAX_PROMPT_BYTES {
            return Err(turn_error(
                "PROMPT_TOO_LARGE",
                "turn prompt exceeds the supported size",
            ));
        }
        let digest = format!("{:x}", Sha256::digest(prompt));
        if digest != self.turn.prompt_sha256 {
            return Err(turn_error(
                "REQUEST_CONFLICT",
                "turn prompt digest does not match",
            ));
        }
        if self.submit.material().manifest_digest() != self.turn.digest() {
            return Err(turn_error(
                "REQUEST_CONFLICT",
                "turn material digest does not match",
            ));
        }
        Ok(())
    }

    pub fn submit(&self) -> &SubmitRequest {
        &self.submit
    }

    pub fn turn(&self) -> &TurnMaterial {
        &self.turn
    }

    pub fn prompt(&self) -> &str {
        &self.prompt
    }

    pub fn origin_url(&self) -> Option<&str> {
        self.origin_url.as_deref()
    }

    /// Ask the worker to report this turn to its herdr server.
    pub fn with_herdr_reporter(mut self, enabled: bool) -> Self {
        self.herdr_reporter = enabled;
        self
    }

    pub fn herdr_reporter(&self) -> bool {
        self.herdr_reporter
    }

    #[doc(hidden)]
    pub fn with_turn(&self, turn: TurnMaterial) -> Self {
        Self {
            submit: self.submit.clone(),
            turn,
            prompt: self.prompt.clone(),
            origin_url: self.origin_url.clone(),
            herdr_reporter: self.herdr_reporter,
        }
    }

    #[doc(hidden)]
    pub fn with_prompt(&self, prompt: impl Into<String>) -> Self {
        Self {
            submit: self.submit.clone(),
            turn: self.turn.clone(),
            prompt: prompt.into(),
            origin_url: self.origin_url.clone(),
            herdr_reporter: self.herdr_reporter,
        }
    }
}

impl serde::Serialize for TaskTurnRequest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(serde::ser::Error::custom)?;
        let fields = if self.herdr_reporter { 5 } else { 4 };
        let mut record = serializer.serialize_struct("TaskTurnRequest", fields)?;
        record.serialize_field("submit", &self.submit)?;
        record.serialize_field("turn", &self.turn)?;
        record.serialize_field("prompt", &self.prompt)?;
        record.serialize_field("origin_url", &self.origin_url)?;
        if self.herdr_reporter {
            record.serialize_field("herdr_reporter", &self.herdr_reporter)?;
        }
        record.end()
    }
}

impl<'de> serde::Deserialize<'de> for TaskTurnRequest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            submit: SubmitRequest,
            turn: TurnMaterial,
            prompt: String,
            origin_url: Option<String>,
            #[serde(default)]
            herdr_reporter: bool,
        }
        let wire = Wire::deserialize(deserializer)?;
        let request = Self::new_with_origin(wire.submit, wire.turn, wire.prompt, wire.origin_url)
            .map_err(D::Error::custom)?;
        let request = request.with_herdr_reporter(wire.herdr_reporter);
        request.validate().map_err(D::Error::custom)?;
        Ok(request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskTurnResponse {
    submit: SubmitResponse,
    task: TaskStatus,
}

impl TaskTurnResponse {
    pub fn new(submit: SubmitResponse, task: TaskStatus) -> Self {
        Self { submit, task }
    }

    pub fn submit(&self) -> &SubmitResponse {
        &self.submit
    }

    pub fn task(&self) -> &TaskStatus {
        &self.task
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalPath {
    ChildExit(i32),
    Timeout,
    PrelaunchFailure,
    AmbiguousChild,
    HostCancel,
    LostReconciliation,
}

/// True when a publication error must keep `execution.json` and the own lease:
/// the exact own turn is still nonterminal, or the task status cannot be read.
pub(crate) fn own_turn_publication_still_recoverable(
    store: &HostStore,
    project_id: &str,
    task_id: TaskId,
    job_id: JobId,
) -> bool {
    match TaskStore::new(store, &crate::process::SystemProcessRunner)
        .load_status(project_id, task_id)
    {
        Ok(status) => status
            .turns()
            .last()
            .is_some_and(|turn| turn.turn_id() == job_id && turn.terminal().is_none()),
        Err(_) => true,
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TurnTerminalHook;

impl TurnTerminalHook {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn invoke(
        &self,
        store: &HostStore,
        turn_dir: &RootedDir,
        job_meta: &JobMeta,
        section: &TurnSection,
        terminal: TurnTerminal,
        path: TerminalPath,
        exit_code: Option<i32>,
        protocol_truncated: bool,
        log_truncated: bool,
    ) -> Result<TurnResult, WorkerError> {
        let _ = path;
        adopt_staged_turn_diagnostics(turn_dir);
        let runner = crate::process::SystemProcessRunner;
        let task_store = TaskStore::new(store, &runner);
        let meta = task_store.load_meta(section.project_id(), section.turn().task_id())?;
        let status = task_store.load_status(meta.project_id(), meta.task_id())?;
        let task = PreparedTask::new(meta.clone(), status.clone());
        let published = match TurnPublisher::new(store, &runner).publish_with_terminal(
            &task,
            turn_dir,
            terminal,
            exit_code,
            protocol_truncated,
            log_truncated,
            section.origin_url(),
        ) {
            Ok(result) => {
                report_turn_to_herdr(
                    section,
                    job_meta,
                    &meta,
                    result.outcome(),
                    result.summary(),
                    result.questions(),
                    &task_store,
                );
                Ok((result, meta))
            }
            Err(error) => {
                // Auto-close TASK_BUSY runs after finish_turn already wrote
                // terminal Done. That write is idempotent, so a PUBLISH_FAILED
                // fallback is a no-op on disk; Herdr must not report the
                // fallback. A keyed current-turn intent, or an unreadable
                // intent record, is recoverable and must not terminalize
                // Failed/PUBLISH_FAILED. Pre-intent source/protocol/session
                // failures still persist Failed/PUBLISH_FAILED.
                let current = task_store
                    .load_status(meta.project_id(), meta.task_id())
                    .unwrap_or(status);
                if let Some(turn) = current.turns().last() {
                    if turn.terminal().is_none() {
                        let recoverable_intent = OriginOutbox::new(store, &runner)
                            .has_intent(meta.project_id(), meta.task_id(), turn.turn_id())
                            .unwrap_or(true);
                        if !recoverable_intent {
                            let fallback = preparation_failure_outcome(turn_dir)?.unwrap_or(
                                TaskOutcome::Failed {
                                    reason: "PUBLISH_FAILED".into(),
                                },
                            );
                            let _ = task_store.finish_turn(
                                meta.project_id(),
                                meta.task_id(),
                                turn.turn_id(),
                                terminal,
                                fallback.clone(),
                                false,
                                log_truncated,
                                current.head_oid().cloned(),
                                None,
                                Vec::new(),
                                Vec::new(),
                                None,
                                Vec::new(),
                                false,
                            );
                            report_turn_to_herdr(
                                section,
                                job_meta,
                                &meta,
                                &fallback,
                                None,
                                &[],
                                &task_store,
                            );
                        }
                    } else if let Some(outcome) = current.last_outcome() {
                        report_turn_to_herdr(
                            section,
                            job_meta,
                            &meta,
                            outcome,
                            current.summary(),
                            current.questions(),
                            &task_store,
                        );
                    }
                }
                Err(error)
            }
        };
        // The snapshot is the publication input. Drop it once this attempt
        // cannot be retried. A still-recoverable failure keeps the file so
        // the next attempt redacts with the same secrets.
        if !own_turn_publication_still_recoverable(
            store,
            section.project_id(),
            section.turn().task_id(),
            job_meta.job_id(),
        ) {
            discard_launched_redaction(turn_dir)?;
        }
        match published {
            Ok((result, meta)) => {
                if meta.close_policy() == ClosePolicy::Done
                    && result.outcome() == &TaskOutcome::Done
                {
                    TaskStore::new(store, &runner).close_after_own_terminal_turn(
                        &TaskCloseRequest::new(meta.project_id(), meta.task_id(), false),
                        job_meta.job_id(),
                    )?;
                }
                Ok(result)
            }
            Err(error) => Err(error),
        }
    }
}

/// Tells the worker's herdr how the turn ended, when the worker reports.
/// Best effort under the reporter's budget; the result is noted on the turn
/// record and nothing else changes.
#[allow(clippy::too_many_arguments)]
fn report_turn_to_herdr(
    section: &TurnSection,
    job_meta: &JobMeta,
    meta: &crate::task::TaskMeta,
    outcome: &TaskOutcome,
    summary: Option<&str>,
    questions: &[Question],
    task_store: &TaskStore,
) {
    if !section.herdr_reporter() {
        return;
    }
    let Some(home) = crate::task_store::herdr_account_home() else {
        return;
    };
    let identity = crate::herdr_reporter::TurnIdentity {
        project_id: section.project_id().to_owned(),
        worktree_id: job_meta.worktree_id().to_owned(),
        job_id: job_meta.job_id().to_string(),
        task_id: section.turn().task_id(),
        turn_number: section.turn().turn_number(),
        agent: section.turn().agent(),
        title: meta.title().as_str().to_owned(),
    };
    let pane_id = crate::herdr_reporter::take_start(&job_meta.job_id().to_string())
        .and_then(|report| report.pane_id);
    let reported = crate::herdr_reporter::HerdrReporter::for_home(&home).terminal(
        &identity,
        pane_id.as_deref(),
        outcome,
        summary,
        questions,
    );
    let _ = task_store.record_turn_herdr(
        section.project_id(),
        section.turn().task_id(),
        job_meta.job_id(),
        reported.report,
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnResult {
    outcome: TaskOutcome,
    agent_committed: bool,
    log_truncated: bool,
    head_oid: Option<BaseOid>,
    summary: Option<String>,
    questions: Vec<Question>,
    files_changed: Vec<String>,
    diff_stat: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PreparedTask {
    meta: TaskMeta,
    status: TaskStatus,
}

impl PreparedTask {
    pub fn new(meta: TaskMeta, status: TaskStatus) -> Self {
        Self { meta, status }
    }

    pub fn meta(&self) -> &TaskMeta {
        &self.meta
    }

    pub fn status(&self) -> &TaskStatus {
        &self.status
    }
}

pub struct TurnPublisher<'a> {
    store: &'a HostStore,
    runner: &'a dyn ProcessRunner,
}

type WorkspacePublication = (bool, Option<BaseOid>, Option<String>, Vec<String>);

impl<'a> TurnPublisher<'a> {
    pub fn new(store: &'a HostStore, runner: &'a dyn ProcessRunner) -> Self {
        Self { store, runner }
    }

    pub fn publish(
        &self,
        task: &PreparedTask,
        turn_dir: &RootedDir,
        exit_code: Option<i32>,
    ) -> Result<TurnResult, WorkerError> {
        let terminal = match exit_code {
            Some(0) => TurnTerminal::Succeeded,
            Some(_) => TurnTerminal::Failed,
            None => TurnTerminal::Lost,
        };
        let origin_url = task.meta().push_origin_url();
        self.publish_with_terminal(
            task, turn_dir, terminal, exit_code, false, false, origin_url,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn publish_with_terminal(
        &self,
        task: &PreparedTask,
        turn_dir: &RootedDir,
        terminal: TurnTerminal,
        exit_code: Option<i32>,
        protocol_truncated: bool,
        log_truncated: bool,
        origin_url: Option<&str>,
    ) -> Result<TurnResult, WorkerError> {
        let meta = task.meta();
        let turn_id = task
            .status()
            .turns()
            .last()
            .map(|turn| turn.turn_id())
            .ok_or_else(|| turn_error("PUBLISH_FAILED", "task has no turn history"))?;
        let tail = read_optional_text(turn_dir, "tail.log", LOG_TAIL_BYTES as u64)?;
        // The agent writes its last message itself (Codex `-o`), with the
        // account's umask rather than owner-only mode. The turn directory is
        // owner-only, so restore the file's mode before the private reader
        // sees it instead of failing publication of a successful turn.
        if turn_dir.entry_exists("last.md")? {
            turn_dir
                .set_private_regular_mode("last.md", 0o600)
                .map_err(|error| {
                    turn_error(
                        "PUBLISH_FAILED",
                        format!("cannot restore last.md mode: {error}"),
                    )
                })?;
        }
        let last_message =
            read_optional_text(turn_dir, "last.md", crate::task::MAX_PROMPT_BYTES as u64)?;
        let adapter = crate::agent::adapter_for(meta.agent());
        let last_parses = last_message.as_deref().is_some_and(|text| {
            adapter
                .extract_result("", Some(text))
                .ok()
                .is_some_and(|result| result.status() != crate::agent::ResultStatus::Unknown)
        });
        let existing_session =
            TaskStore::new(self.store, self.runner).session(meta.project_id(), meta.task_id())?;
        let scan = if existing_session.is_some() && last_parses {
            StdoutScan::default()
        } else {
            scan_stdout_protocol(turn_dir, adapter)?
        };
        // Refuse capped or incomplete stdout protocol. Aggregate log truncation
        // (stderr overflow) is recorded on the turn and must not fail a
        // complete session/result when last.md is absent.
        if (protocol_truncated || scan.truncated) && !last_parses {
            persist_parse_reason(turn_dir, Some(crate::agent::ResultParseReason::Truncated))?;
            return Err(turn_error(
                "PUBLISH_FAILED",
                "truncated protocol output cannot be published as a result",
            ));
        }
        let session_ref = scan.session_ref;
        let stream = scan.result_text;
        let session = ensure_session_binding(
            self.store,
            meta,
            existing_session,
            session_ref.as_deref(),
            adapter,
        )?;
        if session.is_none() && terminal == TurnTerminal::Succeeded {
            return Err(turn_error(
                "PUBLISH_FAILED",
                "successful turn did not bind an agent session",
            ));
        }
        // The display tail normally contains raw protocol envelopes, not a
        // decoded agent result. Preserve its legacy valid-result fallback,
        // but never let an envelope mask the decoded candidate's parse error.
        let fallback_tail = tail.as_deref().filter(|text| {
            adapter
                .extract_result("", Some(text))
                .ok()
                .is_some_and(|result| result.status() != crate::agent::ResultStatus::Unknown)
        });
        let structured = adapter
            .extract_result(&stream, last_message.as_deref().or(fallback_tail))
            .map_err(|error| turn_error("PUBLISH_FAILED", error.to_string()))?;
        let mut parse_reason = structured.parse_reason();
        if parse_reason == Some(crate::agent::ResultParseReason::EmptyOutput)
            && !read_auth_scan_tail(turn_dir, "stdout.log")?
                .trim()
                .is_empty()
        {
            parse_reason = Some(crate::agent::ResultParseReason::NoResultJson);
        }
        let preparation_failure = preparation_failure_outcome(turn_dir)?;
        if preparation_failure.is_none() {
            persist_parse_reason(turn_dir, parse_reason)?;
        }

        let (agent_committed, head_oid, diff_stat, files_changed) = if self
            .store
            .task_workspace_if_present(meta.project_id(), meta.task_id())?
            .is_some()
        {
            self.publish_workspace(meta, turn_id)?
        } else {
            (true, task.status().head_oid().cloned(), None, Vec::new())
        };

        let agent_outcome = adapter.classify(exit_code, structured.status());
        // Scan bounded raw stdout/stderr tails, not protocol result candidates
        // or the last.md shortcut. A complete last.md still leaves auth
        // phrases in the raw logs.
        let stdout_auth = read_auth_scan_tail(turn_dir, "stdout.log")?;
        let stderr_auth = read_auth_scan_tail(turn_dir, "stderr.log")?;
        // Only Succeeded/Failed would otherwise become "agent exited N". A
        // Cancelled/TimedOut/Lost turn keeps that terminal even when the
        // stderr tail matches an auth phrase.
        let auth_failed = matches!(terminal, TurnTerminal::Succeeded | TurnTerminal::Failed)
            && matches!(agent_outcome, crate::agent::AgentOutcome::Failed { .. })
            && adapter.output_shows_auth_failure(&stdout_auth, &stderr_auth);
        let outcome = if let Some(outcome) = preparation_failure {
            outcome
        } else if auth_failed {
            crate::auth_incidents::record_incident(
                self.store.host_state_root(),
                meta.agent(),
                meta.env_profile(),
                now_millis()?,
            )?;
            TaskOutcome::failed(crate::auth_incidents::AGENT_AUTHENTICATION_FAILED)
        } else {
            let outcome = TaskOutcome::from_turn(terminal, Some(agent_outcome));
            if terminal == TurnTerminal::Succeeded
                && let Err(error) = crate::auth_incidents::record_success(
                    self.store.host_state_root(),
                    meta.agent(),
                    meta.env_profile(),
                    now_millis()?,
                )
            {
                note_unrecorded_auth_success(turn_dir, &error);
            }
            outcome
        };
        if meta.publish().contains(&PublishMode::Push) {
            let expected_origin = meta.push_origin_url().ok_or_else(|| {
                turn_error("PUBLISH_FAILED", "push publication has no origin target")
            })?;
            if origin_url != Some(expected_origin) {
                return Err(turn_error(
                    "REQUEST_CONFLICT",
                    "turn origin target does not match the task",
                ));
            }
            let oid = head_oid
                .as_ref()
                .ok_or_else(|| turn_error("PUBLISH_FAILED", "push publication has no commit"))?;
            let branch = meta
                .publish_branch()
                .cloned()
                .unwrap_or_else(|| BranchName::for_task(meta.task_id()));
            let now = now_millis()?;
            let outbox = OriginOutbox::new(self.store, self.runner);
            let commit = DeliveryCommit {
                project_id: meta.project_id(),
                task_id: meta.task_id(),
                turn_id,
                oid,
                origin: expected_origin,
                branch: &branch,
                now_millis: now,
            };
            if outbox.commit_intent(commit.clone()).is_err() {
                outbox.commit_intent(commit)?;
            }
        }
        let boundary = publication_boundary(turn_dir)?;
        let summary = nonempty(&boundary.summary(structured.summary()));
        let questions = boundary.questions(structured.questions());
        let agent_files_changed = boundary.changed_files(structured.files_changed());
        let files_changed = boundary.changed_files(files_changed);
        let diff_stat = diff_stat.map(|stat| boundary.diff_stat(&stat));
        let reported_checks = boundary.reported_checks(structured.checks().to_vec());
        TaskStore::new(self.store, self.runner).finish_turn(
            meta.project_id(),
            meta.task_id(),
            turn_id,
            terminal,
            outcome.clone(),
            agent_committed,
            log_truncated,
            head_oid.clone(),
            summary.clone(),
            questions.clone(),
            if files_changed.is_empty() {
                agent_files_changed.clone()
            } else {
                files_changed.clone()
            },
            diff_stat.clone(),
            reported_checks,
            false,
        )?;
        let _ = session;
        Ok(TurnResult {
            outcome,
            agent_committed,
            log_truncated,
            head_oid,
            summary,
            questions,
            files_changed,
            diff_stat,
        })
    }

    fn publish_workspace(
        &self,
        meta: &TaskMeta,
        turn_id: JobId,
    ) -> Result<WorkspacePublication, WorkerError> {
        let workspace = self
            .store
            .task_workspace(meta.project_id(), meta.task_id())?;
        let status = self.git(
            &workspace,
            &["status", "--porcelain=v1", "--untracked-files=all"],
            None,
        )?;
        let dirty = !status.trim().is_empty();
        if dirty {
            self.git(&workspace, &["add", "--all"], None)?;
            let message = format!("mac-worker: uncommitted changes after turn {}", turn_id);
            self.git(
                &workspace,
                &["commit", "-m", &message],
                Some(meta.git_identity()),
            )?;
        }

        let mirror = self
            .store
            .mirror_if_present(meta.project_id())?
            .ok_or_else(|| turn_error("PUBLISH_FAILED", "project mirror is absent"))?;
        let branch = format!("refs/heads/task/{}", meta.task_id());
        self.run_git(
            vec![
                "-C".into(),
                mirror.path().as_os_str().to_os_string(),
                "fetch".into(),
                workspace.as_os_str().to_os_string(),
                format!("+{branch}:{branch}").into(),
            ],
            None,
        )?;
        let head = self
            .git(&workspace, &["rev-parse", "HEAD"], None)?
            .trim()
            .parse()
            .map_err(|_| turn_error("PUBLISH_FAILED", "published head is not a valid object ID"))?;
        let diff_stat = self.git(
            &workspace,
            &["diff", "--stat", meta.base_oid().as_str(), "HEAD"],
            None,
        )?;
        let files = self
            .git(
                &workspace,
                &["diff", "--name-only", meta.base_oid().as_str(), "HEAD"],
                None,
            )?
            .lines()
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let diff_stat = diff_stat.split_whitespace().collect::<Vec<_>>().join(" ");
        let diff_stat = bound_string(&diff_stat);
        Ok((!dirty, Some(head), nonempty(&diff_stat), files))
    }

    fn git(
        &self,
        workspace: &std::path::Path,
        args: &[&str],
        identity: Option<&GitIdentity>,
    ) -> Result<String, WorkerError> {
        let mut arguments = vec!["-C".into(), workspace.as_os_str().to_os_string()];
        arguments.extend(args.iter().map(|arg| OsString::from(*arg)));
        let result = self.run_git(arguments, identity)?;
        String::from_utf8(result)
            .map_err(|_| turn_error("PUBLISH_FAILED", "Git output is not UTF-8"))
    }

    fn run_git(
        &self,
        args: Vec<OsString>,
        identity: Option<&GitIdentity>,
    ) -> Result<Vec<u8>, WorkerError> {
        let mut environment = vec![
            (
                OsString::from("GIT_CONFIG_GLOBAL"),
                OsString::from("/dev/null"),
            ),
            (OsString::from("GIT_CONFIG_NOSYSTEM"), OsString::from("1")),
            (OsString::from("GIT_TERMINAL_PROMPT"), OsString::from("0")),
        ];
        if let Some(identity) = identity {
            environment.extend([
                (OsString::from("GIT_AUTHOR_NAME"), identity.name().into()),
                (OsString::from("GIT_AUTHOR_EMAIL"), identity.email().into()),
                (OsString::from("GIT_COMMITTER_NAME"), identity.name().into()),
                (
                    OsString::from("GIT_COMMITTER_EMAIL"),
                    identity.email().into(),
                ),
            ]);
        }
        let result = self
            .runner
            .run(&ProcessRequest {
                program: "/usr/bin/git".into(),
                args: {
                    let mut prefixed = vec![
                        OsString::from("-c"),
                        OsString::from("core.fsync=objects,derived-metadata,reference"),
                        OsString::from("-c"),
                        OsString::from("core.fsyncMethod=fsync"),
                    ];
                    prefixed.extend(args);
                    prefixed
                },
                environment,
                environment_remove: [
                    "GIT_DIR",
                    "GIT_WORK_TREE",
                    "GIT_INDEX_FILE",
                    "GIT_COMMON_DIR",
                    "GIT_OBJECT_DIRECTORY",
                    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
                    "GIT_CEILING_DIRECTORIES",
                    "GIT_DISCOVERY_ACROSS_FILESYSTEM",
                    "GIT_CONFIG_COUNT",
                    "GIT_CONFIG_PARAMETERS",
                ]
                .into_iter()
                .map(OsString::from)
                .collect(),
                stdin: None,
                policy: ProcessPolicy {
                    stdout_limit: crate::task_store::MAX_DIFF_BYTES,
                    stderr_limit: 128 * 1024,
                    deadline: std::time::Duration::from_secs(15 * 60),
                },
                isolate_parent_environment: false,
            })
            .map_err(|error| turn_error("PUBLISH_FAILED", error.to_string()))?;
        if !result.status.success() {
            return Err(turn_error(
                "PUBLISH_FAILED",
                String::from_utf8_lossy(&result.stderr).trim().to_owned(),
            ));
        }
        Ok(result.stdout)
    }
}

fn ensure_session_binding(
    store: &HostStore,
    meta: &TaskMeta,
    existing: Option<SessionBinding>,
    session_ref: Option<&str>,
    adapter: &'static dyn crate::agent::AgentAdapter,
) -> Result<Option<SessionBinding>, WorkerError> {
    let task_store = TaskStore::new(store, &crate::process::SystemProcessRunner);
    if let Some(binding) = existing {
        return Ok(Some(binding));
    }
    if let Some(binding) = task_store.session(meta.project_id(), meta.task_id())? {
        return Ok(Some(binding));
    }
    let Some(session_ref) = session_ref else {
        return Ok(None);
    };
    let binding = SessionBinding::new(adapter.kind(), session_ref, now_millis()?)?;
    task_store.bind_session(meta.project_id(), meta.task_id(), binding.clone())?;
    Ok(Some(binding))
}

#[derive(Default)]
struct StdoutScan {
    session_ref: Option<String>,
    result_text: String,
    truncated: bool,
}

struct ScanLimits {
    log_cap: u64,
    max_record: usize,
    max_result: usize,
}

fn scan_stdout_protocol(
    turn_dir: &RootedDir,
    adapter: &'static dyn crate::agent::AgentAdapter,
) -> Result<StdoutScan, WorkerError> {
    scan_stdout_protocol_with(
        turn_dir,
        adapter,
        ScanLimits {
            log_cap: LOG_CAP_BYTES,
            max_record: MAX_NDJSON_RECORD_BYTES,
            max_result: crate::task::MAX_PROMPT_BYTES,
        },
    )
}

fn scan_stdout_protocol_with(
    turn_dir: &RootedDir,
    adapter: &'static dyn crate::agent::AgentAdapter,
    limits: ScanLimits,
) -> Result<StdoutScan, WorkerError> {
    if !turn_dir.entry_exists("stdout.log")? {
        return Err(turn_error(
            "PUBLISH_FAILED",
            "cannot read stdout.log: missing",
        ));
    }
    let mut offset = 0_u64;
    let mut pending = Vec::new();
    let mut session_ref = None;
    let mut candidates = CandidateBuffers::default();
    let mut stored = 0_u64;
    let mut truncated = false;
    let mut discard_until_newline = false;
    loop {
        let chunk = turn_dir
            .read_private_regular_chunk("stdout.log", offset, MAX_LOG_CHUNK_BYTES)
            .map_err(|error| {
                turn_error("PUBLISH_FAILED", format!("cannot read stdout.log: {error}"))
            })?;
        if chunk.is_empty() {
            break;
        }
        let room = limits.log_cap.saturating_sub(stored);
        if room == 0 {
            truncated = true;
            break;
        }
        let take = chunk
            .len()
            .min(usize::try_from(room).unwrap_or(chunk.len()));
        if take < chunk.len() {
            truncated = true;
        }
        pending.extend_from_slice(&chunk[..take]);
        stored = stored.saturating_add(take as u64);
        offset = offset.saturating_add(take as u64);
        consume_pending_records(
            &mut pending,
            &mut discard_until_newline,
            adapter,
            &mut session_ref,
            &mut candidates,
            &limits,
        );
        if truncated && take < chunk.len() {
            break;
        }
    }
    if discard_until_newline {
        truncated = true;
    } else if !pending.iter().all(u8::is_ascii_whitespace) {
        if complete_json_object(&pending).is_some() {
            consider_protocol_record(
                adapter,
                &mut session_ref,
                &mut candidates,
                &pending,
                limits.max_result,
            );
        } else {
            truncated = true;
        }
    }
    Ok(StdoutScan {
        session_ref,
        result_text: candidates.join(),
        truncated,
    })
}

pub(crate) fn take_next_complete_record(
    pending: &mut Vec<u8>,
    discard_until_newline: &mut bool,
    max_record: usize,
) -> Option<Vec<u8>> {
    loop {
        if *discard_until_newline {
            if let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
                pending.drain(..=newline);
                *discard_until_newline = false;
            } else {
                pending.clear();
                return None;
            }
            continue;
        }
        let Some(newline) = pending.iter().position(|byte| *byte == b'\n') else {
            if pending.len() > max_record {
                *discard_until_newline = true;
                pending.clear();
            }
            return None;
        };
        if newline >= max_record {
            pending.drain(..=newline);
            continue;
        }
        return Some(pending.drain(..=newline).collect());
    }
}

fn consume_pending_records(
    pending: &mut Vec<u8>,
    discard_until_newline: &mut bool,
    adapter: &'static dyn crate::agent::AgentAdapter,
    session_ref: &mut Option<String>,
    candidates: &mut CandidateBuffers,
    limits: &ScanLimits,
) {
    while let Some(line) =
        take_next_complete_record(pending, discard_until_newline, limits.max_record)
    {
        consider_protocol_record(adapter, session_ref, candidates, &line, limits.max_result);
    }
}

#[derive(Default)]
struct CandidateBuffers {
    results: VecDeque<String>,
    result_bytes: usize,
    others: VecDeque<String>,
    other_bytes: usize,
}

impl CandidateBuffers {
    fn push_result(&mut self, text: String, max: usize) {
        push_bounded_line(&mut self.results, &mut self.result_bytes, text, max);
    }

    fn push_other(&mut self, text: String, max: usize) {
        push_bounded_line(&mut self.others, &mut self.other_bytes, text, max);
    }

    fn join(&self) -> String {
        let mut joined = String::new();
        for line in self.results.iter().chain(self.others.iter()) {
            if !joined.is_empty() {
                joined.push('\n');
            }
            joined.push_str(line);
        }
        joined
    }
}

fn push_bounded_line(lines: &mut VecDeque<String>, used: &mut usize, text: String, max: usize) {
    if text.len() > max {
        return;
    }
    while *used + text.len() > max {
        let Some(oldest) = lines.pop_front() else {
            return;
        };
        *used = used.saturating_sub(oldest.len());
    }
    *used = used.saturating_add(text.len());
    lines.push_back(text);
}

fn consider_protocol_record(
    adapter: &'static dyn crate::agent::AgentAdapter,
    session_ref: &mut Option<String>,
    candidates: &mut CandidateBuffers,
    record: &[u8],
    max_result: usize,
) {
    let text = String::from_utf8_lossy(record);
    let text = text.trim_end_matches(['\r', '\n']);
    if session_ref.is_none()
        && let Some(event) = adapter.parse_event(text)
    {
        *session_ref = adapter.session_ref(std::slice::from_ref(&event));
    }
    if text.len() > max_result {
        return;
    }
    match protocol_candidate_kind(text) {
        Some(CandidateKind::ExplicitResult) => candidates.push_result(text.to_string(), max_result),
        Some(CandidateKind::Other) => candidates.push_other(text.to_string(), max_result),
        None => {}
    }
}

enum CandidateKind {
    ExplicitResult,
    Other,
}

fn protocol_candidate_kind(text: &str) -> Option<CandidateKind> {
    let value = serde_json::from_str::<serde_json::Value>(text).ok()?;
    if !value.is_object() {
        return None;
    }
    match value.get("type").and_then(serde_json::Value::as_str) {
        Some("result") => Some(CandidateKind::ExplicitResult),
        Some("assistant" | "item.completed" | "text") => Some(CandidateKind::Other),
        _ => None,
    }
}

fn complete_json_object(bytes: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(bytes).ok()?.trim();
    if text.is_empty() {
        return None;
    }
    match serde_json::from_str::<serde_json::Value>(text) {
        Ok(value) if value.is_object() => Some(text),
        _ => None,
    }
}

pub fn prebind_session(
    store: &HostStore,
    runner: &dyn ProcessRunner,
    request: &TaskPrebindRequest,
    account_home: &Path,
) -> Result<TaskSessionResponse, WorkerError> {
    request.validate()?;
    let agent = request.agent()?;
    let task_store = TaskStore::new(store, runner);
    match task_store.session(request.project_id(), request.task_id()) {
        Ok(Some(binding)) if binding.agent() == agent => {
            return Ok(TaskSessionResponse::new(binding));
        }
        Ok(Some(_)) => {
            return Err(turn_error(
                "TASK_SESSION_CONFLICT",
                "task session binding belongs to a different agent",
            ));
        }
        Ok(None) => {}
        Err(error) if error.public_code() == "TASK_NOT_FOUND" => {}
        Err(error) => return Err(error),
    }
    if let Some(session_ref) = request.session_ref() {
        let binding = SessionBinding::new(agent, session_ref, now_millis()?)?;
        task_store.bind_session(request.project_id(), request.task_id(), binding.clone())?;
        return Ok(TaskSessionResponse::new(binding));
    }
    let argv = adapter_for(agent).prebind_session().ok_or_else(|| {
        WorkerError::task(
            "TASK_CONFIG_INVALID",
            "agent does not expose a session prebind command",
        )
    })?;
    let profile = match request.env_profile() {
        Some(name) => EnvProfile::load_for_home(
            &account_home
                .join(".config")
                .join("mac-worker")
                .join("env")
                .join(format!("{name}.env")),
            account_home,
        )?,
        None => EnvProfile::empty(),
    };
    let process = prebind_login_request(&argv, account_home, profile.entries())?;
    if let Some(config) = profile.keychain() {
        crate::keychain::unlock_keychain_if_supported(
            runner,
            config,
            &profile.redaction_boundary(account_home),
        )?;
    }
    let result = runner.run(&process).map_err(|error| WorkerError::Agent {
        code: "AGENT_EXITED",
        message: format!("session prebind failed: {error}"),
    })?;
    if !result.status.success() {
        return Err(WorkerError::Agent {
            code: "AGENT_EXITED",
            message: "session prebind command failed".into(),
        });
    }
    let session_ref = parse_prebind_session_ref(&result.stdout)?;
    let binding = SessionBinding::new(agent, session_ref, now_millis()?)?;
    match task_store.bind_session(request.project_id(), request.task_id(), binding.clone()) {
        Ok(()) => {}
        Err(error) if error.public_code() == "TASK_NOT_FOUND" => {}
        Err(error) => return Err(error),
    }
    Ok(TaskSessionResponse::new(binding))
}

fn read_text(dir: &RootedDir, name: &str, max: u64) -> Result<String, WorkerError> {
    let bytes = dir
        .read_private_regular(name, max)
        .map_err(|error| turn_error("PUBLISH_FAILED", format!("cannot read {name}: {error}")))?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

fn read_optional_text(
    dir: &RootedDir,
    name: &str,
    max: u64,
) -> Result<Option<String>, WorkerError> {
    if !dir.entry_exists(name)? {
        return Ok(None);
    }
    read_text(dir, name, max).map(Some)
}

fn read_auth_scan_tail(dir: &RootedDir, name: &str) -> Result<String, WorkerError> {
    if !dir.entry_exists(name)? {
        return Ok(String::new());
    }
    let bytes = dir
        .read_private_regular_tail(name, crate::agent::AUTH_SCAN_TAIL_BYTES)
        .map_err(|error| turn_error("PUBLISH_FAILED", format!("cannot read {name}: {error}")))?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// `record_incident` stays fatal: a failed turn stays failed and a broken
/// store is visible as `PUBLISH_FAILED`. `record_success` is not: a turn
/// that already succeeded must not fail publication because
/// `auth-incidents.json` is unreadable or non-canonical. Keep the turn's
/// outcome and append one code-only line to `supervisor.log` when that
/// diagnostic file can be opened or created. Never extend `stderr.log`:
/// its length is already bound into terminal job status.
fn note_unrecorded_auth_success(turn_dir: &RootedDir, error: &WorkerError) {
    let line = format!("auth success not recorded: {}\n", error.public_code());
    if line.len() as u64 > crate::supervisor::MAX_SUPERVISOR_LOG_BYTES {
        return;
    }
    let mut file = match turn_dir.open_private_append("supervisor.log") {
        Ok(file) => file,
        Err(io_error) if io_error.kind() == std::io::ErrorKind::NotFound => {
            match turn_dir.write_new_private_file("supervisor.log", &[]) {
                Ok(file) => file,
                Err(_) => return,
            }
        }
        Err(_) => return,
    };
    let Ok(current) = turn_dir.validate_private_append_binding("supervisor.log", &file) else {
        return;
    };
    let Some(end) = current.checked_add(line.len() as u64) else {
        return;
    };
    if end > crate::supervisor::MAX_SUPERVISOR_LOG_BYTES {
        return;
    }
    if file.write_all(line.as_bytes()).is_ok() && file.sync_all().is_ok() {
        let _ = turn_dir.sync_root();
    }
}

fn bound_string(value: &str) -> String {
    let mut end = value.len().min(crate::task_store::MAX_DIFF_BYTES);
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

fn nonempty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_owned())
}

fn now_millis() -> Result<u64, WorkerError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| turn_error("PUBLISH_FAILED", "system clock precedes the Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| turn_error("PUBLISH_FAILED", "system clock is outside the range"))
}

impl TurnResult {
    pub fn new(outcome: TaskOutcome) -> Self {
        Self {
            outcome,
            agent_committed: true,
            log_truncated: false,
            head_oid: None,
            summary: None,
            questions: Vec::new(),
            files_changed: Vec::new(),
            diff_stat: None,
        }
    }

    pub fn outcome(&self) -> &TaskOutcome {
        &self.outcome
    }

    pub fn agent_committed(&self) -> bool {
        self.agent_committed
    }

    pub fn log_truncated(&self) -> bool {
        self.log_truncated
    }

    pub fn head_oid(&self) -> Option<&BaseOid> {
        self.head_oid.as_ref()
    }

    pub fn summary(&self) -> Option<&str> {
        self.summary.as_deref()
    }

    pub fn questions(&self) -> &[Question] {
        &self.questions
    }

    pub fn files_changed(&self) -> &[String] {
        &self.files_changed
    }

    pub fn diff_stat(&self) -> Option<&str> {
        self.diff_stat.as_deref()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct EnvProfile {
    names: Vec<String>,
    entries: Vec<(OsString, OsString)>,
    keychain: Option<crate::keychain::KeychainUnlockConfig>,
}

impl fmt::Debug for EnvProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EnvProfile")
            .field("names", &self.names)
            .field("entry_count", &self.entries.len())
            .finish()
    }
}

impl EnvProfile {
    pub(crate) fn empty() -> Self {
        Self {
            names: Vec::new(),
            entries: Vec::new(),
            keychain: None,
        }
    }

    pub fn load(path: &Path) -> Result<Self, WorkerError> {
        let home = std::env::var_os("HOME")
            .map(std::path::PathBuf::from)
            .unwrap_or_default();
        Self::load_for_home(path, &home)
    }

    pub(crate) fn load_for_home(path: &Path, home: &Path) -> Result<Self, WorkerError> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .map_err(|error| {
                turn_error(
                    "ENV_PROFILE_PERMISSIONS",
                    format!("cannot open env profile: {error}"),
                )
            })?;
        let metadata = file.metadata().map_err(|error| {
            turn_error(
                "ENV_PROFILE_PERMISSIONS",
                format!("cannot inspect env profile: {error}"),
            )
        })?;
        if !metadata.file_type().is_file()
            || metadata.mode() & 0o7777 != 0o600
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.nlink() != 1
        {
            return Err(turn_error(
                "ENV_PROFILE_PERMISSIONS",
                "env profile must be an owner-only regular file",
            ));
        }
        let mut bytes = Vec::new();
        file.take(MAX_ENV_PROFILE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| {
                turn_error(
                    "ENV_PROFILE_PERMISSIONS",
                    format!("cannot read env profile: {error}"),
                )
            })?;
        if bytes.len() as u64 > MAX_ENV_PROFILE_BYTES {
            return Err(turn_error(
                "ENV_PROFILE_INVALID",
                "env profile is too large",
            ));
        }
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| turn_error("ENV_PROFILE_INVALID", "env profile is not UTF-8"))?;
        let mut entries = Vec::new();
        let mut seen = BTreeMap::new();
        let mut keychain_entries = Vec::new();
        for (line_number, raw_line) in text.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (name, value) = line.split_once('=').ok_or_else(|| {
                turn_error(
                    "ENV_PROFILE_INVALID",
                    format!("env profile line {} is not NAME=VALUE", line_number + 1),
                )
            })?;
            validate_env_name(name)?;
            if seen.insert(name.to_owned(), ()).is_some() {
                return Err(turn_error(
                    "ENV_PROFILE_INVALID",
                    "env profile repeats a variable",
                ));
            }
            validate_env_value(value)?;
            if crate::keychain::is_reserved_env_name(name) {
                keychain_entries.push((OsString::from(name), OsString::from(value)));
                continue;
            }
            if worker_controlled_env_name(name) {
                return Err(turn_error(
                    "ENV_PROFILE_INVALID",
                    "env profile cannot override worker-controlled variables",
                ));
            }
            entries.push((OsString::from(name), OsString::from(value)));
        }
        let names = entries
            .iter()
            .map(|(name, _)| name.to_string_lossy().into_owned())
            .collect();
        let keychain = crate::keychain::KeychainUnlockConfig::from_entries(&keychain_entries, home);
        Ok(Self {
            names,
            entries,
            keychain,
        })
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }

    pub(crate) fn entries(&self) -> &[(OsString, OsString)] {
        &self.entries
    }

    pub(crate) fn keychain(&self) -> Option<&crate::keychain::KeychainUnlockConfig> {
        self.keychain.as_ref()
    }

    pub(crate) fn all_entries(&self) -> Vec<(OsString, OsString)> {
        let mut entries = self.entries.clone();
        if let Some(keychain) = &self.keychain {
            entries.push((
                crate::keychain::PASSWORD_ENV_NAME.into(),
                keychain.password().to_os_string(),
            ));
            entries.push((
                crate::keychain::PATH_ENV_NAME.into(),
                keychain.path().as_os_str().to_os_string(),
            ));
        }
        entries
    }

    pub(crate) fn redaction_boundary(&self, home: &Path) -> RedactionBoundary {
        let secrets = self
            .all_entries()
            .into_iter()
            .map(|(_, value)| value.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        RedactionBoundary::new(home).with_secrets(secrets)
    }

    /// Writes the secrets this launch actually loaded. Publication reads this
    /// file instead of the profile that happens to be on disk later.
    pub(crate) fn persist_launched_redaction(&self, job: &RootedDir) -> Result<(), WorkerError> {
        let exists = job.entry_exists(LAUNCHED_REDACTION_FILE).map_err(|error| {
            snapshot_error(format!("cannot store launched redaction snapshot: {error}"))
        })?;
        if exists {
            // A crashed prelaunch attempt already stored the secrets this turn
            // launched with. Reuse that file. Do not rewrite it from the
            // profile that happens to be on disk for the retry.
            validate_existing_launched_redaction(job)?;
            return Ok(());
        }
        let secrets = self
            .all_entries()
            .into_iter()
            .map(|(_, value)| value.to_string_lossy().into_owned())
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>();
        let bytes = serde_json::to_vec(&secrets).map_err(|error| {
            snapshot_error(format!(
                "cannot encode launched redaction snapshot: {error}"
            ))
        })?;
        job.write_private_atomic_no_replace(LAUNCHED_REDACTION_FILE, &bytes)
            .map_err(|error| {
                snapshot_error(format!("cannot store launched redaction snapshot: {error}"))
            })?;
        Ok(())
    }
}

pub(crate) const LAUNCHED_REDACTION_FILE: &str = "launched-redaction.json";
const LAUNCHED_REDACTION_MAX_BYTES: u64 = 1024 * 1024;

fn snapshot_error(message: impl Into<String>) -> WorkerError {
    turn_error("REDACTION_SNAPSHOT_FAILED", message)
}

fn validate_existing_launched_redaction(job: &RootedDir) -> Result<(), WorkerError> {
    let bytes = job
        .read_private_regular(LAUNCHED_REDACTION_FILE, LAUNCHED_REDACTION_MAX_BYTES)
        .map_err(|error| {
            snapshot_error(format!("cannot read launched redaction snapshot: {error}"))
        })?;
    let _: Vec<String> = serde_json::from_slice(&bytes)
        .map_err(|_| snapshot_error("launched redaction snapshot is invalid"))?;
    Ok(())
}

/// Removes the launch snapshot once publication will not read it again.
pub(crate) fn discard_launched_redaction(job: &RootedDir) -> std::io::Result<()> {
    if !job.entry_exists(LAUNCHED_REDACTION_FILE)? {
        return Ok(());
    }
    job.remove_owned_regular(LAUNCHED_REDACTION_FILE)
}

fn publication_boundary(turn_dir: &RootedDir) -> Result<RedactionBoundary, WorkerError> {
    if !turn_dir.entry_exists(LAUNCHED_REDACTION_FILE)? {
        return Ok(RedactionBoundary::from_env());
    }
    let bytes = turn_dir
        .read_private_regular(LAUNCHED_REDACTION_FILE, LAUNCHED_REDACTION_MAX_BYTES)
        .map_err(|error| {
            turn_error(
                "PUBLISH_FAILED",
                format!("cannot read launched redaction snapshot: {error}"),
            )
        })?;
    let secrets: Vec<String> = serde_json::from_slice(&bytes)
        .map_err(|_| turn_error("PUBLISH_FAILED", "launched redaction snapshot is invalid"))?;
    Ok(RedactionBoundary::from_env().with_secrets(secrets))
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
        .map_err(|error| turn_error("TURN_INVALID", error.to_string()))
    }
}

fn agent_name(agent: AgentKind) -> &'static str {
    match agent {
        AgentKind::Codex => "codex",
        AgentKind::Claude => "claude",
        AgentKind::Cursor => "cursor",
        AgentKind::Opencode => "opencode",
    }
}

fn parse_agent(value: &str) -> Result<AgentKind, WorkerError> {
    match value {
        "codex" => Ok(AgentKind::Codex),
        "claude" => Ok(AgentKind::Claude),
        "cursor" => Ok(AgentKind::Cursor),
        "opencode" => Ok(AgentKind::Opencode),
        _ => Err(turn_error("TURN_INVALID", "unknown agent kind")),
    }
}

fn policy_name(policy: PermissionPolicy) -> &'static str {
    match policy {
        PermissionPolicy::Workspace => "workspace",
        PermissionPolicy::Unattended => "unattended",
    }
}

fn parse_policy(value: &str) -> Result<PermissionPolicy, WorkerError> {
    match value {
        "workspace" => Ok(PermissionPolicy::Workspace),
        "unattended" => Ok(PermissionPolicy::Unattended),
        _ => Err(turn_error("TURN_INVALID", "unknown permission policy")),
    }
}

fn validate_hex(value: &str, label: &str) -> Result<(), WorkerError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(turn_error("TURN_INVALID", format!("{label} is invalid")));
    }
    Ok(())
}

fn validate_text(value: &str, max: usize, label: &str) -> Result<(), WorkerError> {
    if value.is_empty() || value.len() > max || value.chars().any(char::is_control) {
        return Err(turn_error("TURN_INVALID", format!("{label} is invalid")));
    }
    Ok(())
}

fn validate_origin_url(origin: Option<&str>) -> Result<(), WorkerError> {
    let Some(origin) = origin else {
        return Ok(());
    };
    if crate::project::canonical_file_origin(origin)?.is_some() {
        return Ok(());
    }
    let normalized = crate::project::normalize_origin(origin)
        .map_err(|_| turn_error("TURN_INVALID", "turn origin URL is invalid"))?;
    if normalized != origin {
        return Err(turn_error(
            "TURN_INVALID",
            "turn origin URL is not normalized",
        ));
    }
    Ok(())
}

fn validate_env_name(name: &str) -> Result<(), WorkerError> {
    if name.is_empty()
        || name.len() > MAX_ENV_NAME_BYTES
        || !name.bytes().enumerate().all(|(index, byte)| {
            (index == 0 && (byte.is_ascii_alphabetic() || byte == b'_'))
                || (index > 0 && (byte.is_ascii_alphanumeric() || byte == b'_'))
        })
    {
        return Err(turn_error(
            "ENV_PROFILE_INVALID",
            "env profile variable name is invalid",
        ));
    }
    Ok(())
}

fn validate_env_value(value: &str) -> Result<(), WorkerError> {
    if value.len() > MAX_ENV_VALUE_BYTES
        || value.contains('\0')
        || value.contains('\n')
        || value.contains('\r')
    {
        return Err(turn_error(
            "ENV_PROFILE_INVALID",
            "env profile value is invalid",
        ));
    }
    Ok(())
}

fn worker_controlled_env_name(name: &str) -> bool {
    matches!(
        name,
        "HOME"
            | "USER"
            | "LOGNAME"
            | "SHELL"
            | "TMPDIR"
            | "MAC_WORKER_TURN_DIR"
            | "MAC_WORKER_ACCOUNT_HOME"
            | "MAC_WORKER_JOB_ID"
            | "MAC_WORKER_CLIENT_ID"
            | "MAC_WORKER_PROJECT_ID"
            | "MAC_WORKER_WORKTREE_ID"
            | "MAC_WORKER_TASK_ID"
            | "MAC_WORKER_TURN"
            | "MAC_WORKER_LEASE_DEADLINE_MILLIS"
            | "MAC_WORKER_ENV_PROFILE"
            | "GIT_AUTHOR_NAME"
            | "GIT_AUTHOR_EMAIL"
            | "GIT_COMMITTER_NAME"
            | "GIT_COMMITTER_EMAIL"
            | crate::keychain::PASSWORD_ENV_NAME
            | crate::keychain::PATH_ENV_NAME
    )
}

fn turn_error(code: &str, message: impl Into<String>) -> WorkerError {
    WorkerError::Protocol(format!("{code}: {}", message.into()))
}

#[allow(dead_code)]
fn os_str_bytes(value: &OsStr) -> &[u8] {
    use std::os::unix::ffi::OsStrExt;
    value.as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        agent::{AgentKind, PermissionPolicy},
        host_store::HostStore,
        inputs::RelativePath,
        job::{ClientId, CommandSpec, LeaseToken, RequestFingerprintMaterial},
        process::SystemProcessRunner,
        rooted_fs::RootedDir,
        task::{
            GitIdentity, PublishMode, TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskSource,
            TaskState, TurnSummary,
        },
        task_store::{SessionBinding, TaskPrebindRequest},
    };
    use tempfile::tempdir;

    const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn terminal_hook_adopts_only_complete_staged_diagnostics() {
        use crate::agent::identity::{IDENTITY_FILE, STAGED_IDENTITY_FILE};
        use crate::prepare_turn::stage_turn_diagnostic;
        use crate::project_readiness::{SETUP_RESULT_FILE, STAGED_SETUP_RESULT_FILE};
        let identity = crate::agent::AgentIdentity {
            executable: "/opt/agent/bin/codex".into(),
            version: Some("0.157.1".into()),
            version_observation: crate::agent::VersionObservation::Observed,
        };
        let staged = serde_json::to_vec(&identity).unwrap();
        let fresh = || {
            let temp = tempdir().unwrap();
            let path = temp.path().join("turn");
            std::fs::create_dir(&path).unwrap();
            std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o700))
                .unwrap();
            let turn = RootedDir::open(&path).unwrap();
            (temp, path, turn)
        };

        // No tmp scope, then an empty one: nothing to adopt.
        let (_temp, path, turn) = fresh();
        adopt_staged_turn_diagnostics(&turn);
        assert!(!turn.entry_exists(IDENTITY_FILE).unwrap());
        turn.open_child_directory(&RelativePath::parse(b"tmp").unwrap(), true)
            .unwrap();
        adopt_staged_turn_diagnostics(&turn);
        assert!(!turn.entry_exists(IDENTITY_FILE).unwrap());

        // A complete staged record becomes the retained private record.
        stage_turn_diagnostic(&path, STAGED_IDENTITY_FILE, &staged).unwrap();
        let setup = br#"{"code":"SETUP_FAILED","message":"recipe exited 1"}"#;
        stage_turn_diagnostic(&path, STAGED_SETUP_RESULT_FILE, setup).unwrap();
        adopt_staged_turn_diagnostics(&turn);
        assert_eq!(
            turn.read_private_regular(IDENTITY_FILE, 8192).unwrap(),
            staged
        );
        assert_eq!(
            turn.read_private_regular(SETUP_RESULT_FILE, 4096).unwrap(),
            setup
        );
        // An existing record is never replaced.
        std::fs::write(path.join("tmp").join(STAGED_IDENTITY_FILE), b"{}").unwrap();
        adopt_staged_turn_diagnostics(&turn);
        assert_eq!(
            turn.read_private_regular(IDENTITY_FILE, 8192).unwrap(),
            staged
        );

        // A write interrupted by a cancel leaves a partial record: ignore it.
        let (_temp, path, turn) = fresh();
        turn.open_child_directory(&RelativePath::parse(b"tmp").unwrap(), true)
            .unwrap();
        stage_turn_diagnostic(&path, STAGED_IDENTITY_FILE, &staged[..staged.len() / 2]).unwrap();
        stage_turn_diagnostic(&path, STAGED_SETUP_RESULT_FILE, br#"{"code":"#).unwrap();
        adopt_staged_turn_diagnostics(&turn);
        assert!(!turn.entry_exists(IDENTITY_FILE).unwrap());
        assert!(!turn.entry_exists(SETUP_RESULT_FILE).unwrap());
        assert!(!turn.has_private_cleanup_residue().unwrap());
    }

    #[test]
    fn prebind_rejects_an_existing_binding_for_a_different_agent() {
        let temp = tempdir().unwrap();
        let store = HostStore::open(&temp.path().join("host")).unwrap();
        let task_id = TaskId::generate();
        let task = store
            .open_task_directory(PROJECT_ID, task_id, true)
            .unwrap();
        let binding = SessionBinding::new(AgentKind::Cursor, "chat0001", 1).unwrap();
        let bytes = serde_json::to_vec(&binding).unwrap();
        RootedDir::write_private_atomic_no_replace(&task, "session.json", &bytes).unwrap();

        let request = TaskPrebindRequest::discover(PROJECT_ID, task_id, AgentKind::Opencode, None);
        let error =
            prebind_session(&store, &SystemProcessRunner, &request, temp.path()).unwrap_err();

        assert_eq!(error.public_code(), "TASK_SESSION_CONFLICT");
    }

    fn open_scan_dir() -> (tempfile::TempDir, RootedDir) {
        let temp = tempdir().unwrap();
        let store = HostStore::open(&temp.path().join("host")).unwrap();
        let turn_dir = store
            .open_task_directory(PROJECT_ID, TaskId::generate(), true)
            .unwrap();
        (temp, turn_dir)
    }

    fn write_stdout(turn_dir: &RootedDir, bytes: &[u8]) {
        RootedDir::write_private_atomic_no_replace(turn_dir, "stdout.log", bytes).unwrap();
    }

    #[test]
    fn publication_redacts_the_launched_snapshot_and_keeps_a_rotated_secret() {
        let (_temp, turn_dir) = open_scan_dir();
        let secret = "purple-lantern-secret-qq";
        let decoy = "rotated-lantern-secret-zz";
        RootedDir::write_private_atomic_no_replace(
            &turn_dir,
            "launched-redaction.json",
            &serde_json::to_vec(&vec![secret]).unwrap(),
        )
        .unwrap();
        RootedDir::write_private_atomic_no_replace(&turn_dir, "rotated.env", decoy.as_bytes())
            .unwrap();

        let boundary = publication_boundary(&turn_dir).unwrap();
        let summary = boundary.summary(&format!("saw {secret} and {decoy}"));
        let questions = boundary.questions([crate::agent::Question::open(format!(
            "ask {secret} {decoy}"
        ))]);
        let files = boundary.changed_files([format!("src/{secret}.rs"), format!("src/{decoy}.rs")]);
        let diff_stat = boundary.diff_stat(&format!("{secret} {decoy}"));
        let checks = boundary.reported_checks([crate::agent::ReportedCheck::new(
            format!("check {secret}"),
            format!("echo {decoy}"),
            crate::agent::ReportedCheckStatus::Pass,
            format!("detail {secret} {decoy}"),
        )]);
        let status = crate::task::TaskStatus::new(
            crate::task::TaskState::Open,
            None,
            None,
            false,
            None,
            Some(summary),
            questions,
            files,
            Some(diff_stat),
            Vec::new(),
            1,
        )
        .unwrap()
        .with_reported_checks(checks)
        .unwrap();
        let encoded = serde_json::to_string(&status).unwrap();
        assert!(
            !encoded.contains(secret),
            "launched secret leaked into status: {encoded}"
        );
        assert!(
            encoded.contains(decoy),
            "rotated secret must stay visible: {encoded}"
        );
    }

    #[test]
    fn publication_refuses_a_corrupt_launched_redaction_snapshot() {
        let (_temp, turn_dir) = open_scan_dir();
        RootedDir::write_private_atomic_no_replace(
            &turn_dir,
            "launched-redaction.json",
            b"not-json",
        )
        .unwrap();
        let error = publication_boundary(&turn_dir).unwrap_err();
        assert_eq!(error.public_code(), "PUBLISH_FAILED");
    }

    #[test]
    fn relaunch_reuses_a_valid_launched_redaction_snapshot() {
        let (_temp, turn_dir) = open_scan_dir();
        let bytes = br#"["purple-lantern-secret-qq"]"#;
        turn_dir
            .write_private_atomic_no_replace(LAUNCHED_REDACTION_FILE, bytes)
            .unwrap();
        EnvProfile::empty()
            .persist_launched_redaction(&turn_dir)
            .unwrap();
        let stored = turn_dir
            .read_private_regular(LAUNCHED_REDACTION_FILE, LAUNCHED_REDACTION_MAX_BYTES)
            .unwrap();
        assert_eq!(stored, bytes);
    }

    #[test]
    fn relaunch_rejects_an_invalid_launched_redaction_snapshot() {
        let (_temp, turn_dir) = open_scan_dir();
        turn_dir
            .write_private_atomic_no_replace(LAUNCHED_REDACTION_FILE, b"not-json")
            .unwrap();
        let error = EnvProfile::empty()
            .persist_launched_redaction(&turn_dir)
            .unwrap_err();
        assert_eq!(error.public_code(), "REDACTION_SNAPSHOT_FAILED");
    }

    struct PublicationFixture {
        _temp: tempfile::TempDir,
        store: HostStore,
        turn_dir: RootedDir,
        job_meta: JobMeta,
        section: TurnSection,
    }

    fn publication_fixture(stdout: Option<&str>) -> PublicationFixture {
        let temp = tempdir().unwrap();
        let store = HostStore::open(&temp.path().join("host")).unwrap();
        let task_id = TaskId::generate();
        let job_id = JobId::generate();
        let worktree = "b".repeat(64);
        let base_oid: BaseOid = "0123456789012345678901234567890123456789".parse().unwrap();
        let limits = TaskLimits::default();
        let meta = TaskMeta::new(TaskMetaInput {
            task_id,
            run_id: None,
            project_id: PROJECT_ID.into(),
            worktree_id: worktree.clone(),
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
            limits: limits.clone(),
            close_policy: ClosePolicy::Never,
            env_profile: None,
            git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
            title: None,
            prompt: "publish the change".into(),
            created_at_millis: 100,
        })
        .unwrap();
        let turn = TurnMaterial::from_prompt(
            task_id,
            1,
            AgentKind::Codex,
            None,
            None,
            PermissionPolicy::Workspace,
            limits.turn.clone(),
            base_oid,
            "publish the change",
            None,
            Uuid::from_u128(7),
            false,
        )
        .unwrap();
        let section = TurnSection::new(
            turn,
            PROJECT_ID,
            GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        )
        .unwrap();
        let status = TaskStatus::new(
            TaskState::Active,
            None,
            None,
            false,
            None,
            None,
            Vec::new(),
            Vec::new(),
            None,
            vec![TurnSummary::new(
                1,
                job_id,
                None,
                None,
                None,
                false,
                Some(1),
                None,
            )],
            1,
        )
        .unwrap();
        let task_dir = store
            .open_task_directory(PROJECT_ID, task_id, true)
            .unwrap();
        task_dir
            .write_private_atomic_no_replace("meta.json", &serde_json::to_vec(&meta).unwrap())
            .unwrap();
        task_dir
            .write_private_atomic_no_replace("status.json", &serde_json::to_vec(&status).unwrap())
            .unwrap();
        let material = RequestFingerprintMaterial::new(
            job_id,
            ClientId::generate(),
            LeaseToken::generate(),
            1,
            "mini-1".into(),
            PROJECT_ID.into(),
            worktree.clone(),
            "c".repeat(64),
            String::new(),
            30_000,
            "heavy".into(),
            CommandSpec::shell("true".into()).unwrap(),
        )
        .unwrap();
        let job_meta = JobMeta::new(&material, material.fingerprint()).unwrap();
        let turn_dir = store
            .open_directory(&format!("jobs/{PROJECT_ID}/{worktree}/{job_id}"), true)
            .unwrap();
        if let Some(stdout) = stdout {
            turn_dir
                .write_private_atomic_no_replace("stdout.log", stdout.as_bytes())
                .unwrap();
        }
        turn_dir
            .write_private_atomic_no_replace(
                LAUNCHED_REDACTION_FILE,
                br#"["purple-lantern-secret-qq"]"#,
            )
            .unwrap();
        PublicationFixture {
            _temp: temp,
            store,
            turn_dir,
            job_meta,
            section,
        }
    }

    fn invoke_publication(
        fixture: &PublicationFixture,
        terminal: TurnTerminal,
        path: TerminalPath,
        exit_code: Option<i32>,
    ) -> Result<TurnResult, WorkerError> {
        TurnTerminalHook.invoke(
            &fixture.store,
            &fixture.turn_dir,
            &fixture.job_meta,
            &fixture.section,
            terminal,
            path,
            exit_code,
            false,
            false,
        )
    }

    #[test]
    fn launched_redaction_snapshot_is_absent_after_a_successful_publish() {
        let stdout = format!(
            "{}\n{}\n",
            codex_session_line(),
            codex_result_record("shipped")
        );
        let fixture = publication_fixture(Some(&stdout));
        invoke_publication(
            &fixture,
            TurnTerminal::Succeeded,
            TerminalPath::ChildExit(0),
            Some(0),
        )
        .unwrap();
        assert!(
            !fixture
                .turn_dir
                .entry_exists(LAUNCHED_REDACTION_FILE)
                .unwrap()
        );
    }

    #[test]
    fn launched_redaction_snapshot_is_absent_after_a_failed_publish() {
        let fixture = publication_fixture(None);
        let error = invoke_publication(
            &fixture,
            TurnTerminal::Failed,
            TerminalPath::ChildExit(1),
            Some(1),
        )
        .unwrap_err();
        assert_eq!(error.public_code(), "PUBLISH_FAILED");
        assert!(
            !fixture
                .turn_dir
                .entry_exists(LAUNCHED_REDACTION_FILE)
                .unwrap()
        );
    }

    #[test]
    fn recoverable_publication_keeps_the_launched_redaction_snapshot() {
        let fixture = publication_fixture(None);
        let delivery = fixture
            .store
            .open_task_directory(PROJECT_ID, fixture.section.turn().task_id(), false)
            .unwrap()
            .open_child_directory(&RelativePath::parse(b"delivery").unwrap(), true)
            .unwrap();
        delivery
            .write_private_atomic_no_replace(&format!("{}.json", fixture.job_meta.job_id()), b"{")
            .unwrap();
        let error = invoke_publication(
            &fixture,
            TurnTerminal::Failed,
            TerminalPath::ChildExit(1),
            Some(1),
        )
        .unwrap_err();
        assert_eq!(error.public_code(), "PUBLISH_FAILED");
        assert!(
            fixture
                .turn_dir
                .entry_exists(LAUNCHED_REDACTION_FILE)
                .unwrap()
        );
    }

    fn codex_session_line() -> &'static str {
        "{\"type\":\"thread.started\",\"thread_id\":\"session-1\"}"
    }

    #[test]
    fn unknown_turn_publishes_a_safe_parse_reason() {
        let fixture = publication_fixture(Some(codex_session_line()));
        invoke_publication(
            &fixture,
            TurnTerminal::Succeeded,
            TerminalPath::ChildExit(0),
            Some(0),
        )
        .unwrap();
        let status = TaskStore::new(&fixture.store, &crate::process::SystemProcessRunner)
            .load_status(PROJECT_ID, fixture.section.turn().task_id())
            .unwrap();
        let wire = serde_json::to_value(status).unwrap();
        assert_eq!(wire["turns"][0]["outcome"]["kind"], "unknown");
        assert_eq!(wire["turns"][0]["result_parse_reason"], "no_result_json");
    }

    #[test]
    fn unknown_reason_comes_from_result_candidates_not_the_raw_stdout_tail() {
        let malformed = serde_json::json!({
            "type": "item.completed",
            "item": { "type": "agent_message", "text": "{\"status\":\"done\",\"summary\":42}" },
        })
        .to_string();
        for (record, expected) in [
            ("", "no_result_json"),
            (malformed.as_str(), "schema_mismatch:summary"),
        ] {
            let stdout = format!("{}\n{record}\n", codex_session_line());
            let fixture = publication_fixture(Some(&stdout));
            fixture
                .turn_dir
                .write_private_atomic_no_replace("tail.log", stdout.as_bytes())
                .unwrap();
            invoke_publication(
                &fixture,
                TurnTerminal::Succeeded,
                TerminalPath::ChildExit(0),
                Some(0),
            )
            .unwrap();
            let status = TaskStore::new(&fixture.store, &crate::process::SystemProcessRunner)
                .load_status(PROJECT_ID, fixture.section.turn().task_id())
                .unwrap();
            let wire = serde_json::to_value(status).unwrap();
            assert_eq!(wire["turns"][0]["outcome"]["kind"], "unknown");
            assert_eq!(wire["turns"][0]["result_parse_reason"], expected);
        }
    }

    #[test]
    fn failed_publication_keeps_redacted_launch_identity() {
        let fixture = publication_fixture(None);
        fixture.turn_dir.write_private_atomic_no_replace("agent-identity.json",
            br#"{"executable":"/opt/purple-lantern-secret-qq/agent","version":"2.3.4","version_observation":"observed"}"#).unwrap();
        invoke_publication(
            &fixture,
            TurnTerminal::Failed,
            TerminalPath::ChildExit(1),
            Some(1),
        )
        .unwrap_err();
        let status = TaskStore::new(&fixture.store, &crate::process::SystemProcessRunner)
            .load_status(PROJECT_ID, fixture.section.turn().task_id())
            .unwrap();
        let wire = serde_json::to_value(status).unwrap();
        assert_eq!(wire["turns"][0]["agent_identity"]["version"], "2.3.4");
        assert_eq!(
            wire["turns"][0]["agent_identity"]["executable"],
            "/opt/[token]/agent"
        );
        assert!(!wire.to_string().contains("purple-lantern"));
    }

    #[test]
    fn setup_input_refusal_reaches_the_public_task_outcome_with_a_hint() {
        let fixture = publication_fixture(Some(""));
        crate::project_readiness::write_setup_stage_result(
            &fixture.turn_dir,
            &WorkerError::task("SETUP_INPUTS_CHANGED", "private helper detail"),
        )
        .unwrap();
        invoke_publication(
            &fixture,
            TurnTerminal::Failed,
            TerminalPath::ChildExit(78),
            Some(78),
        )
        .unwrap();
        let status = TaskStore::new(&fixture.store, &crate::process::SystemProcessRunner)
            .load_status(PROJECT_ID, fixture.section.turn().task_id())
            .unwrap();
        let wire = serde_json::to_value(status).unwrap();
        let reason = wire["last_outcome"]["reason"].as_str().unwrap();
        assert!(reason.contains("SETUP_INPUTS_CHANGED"), "{reason}");
        assert!(reason.contains("submit a new task"), "{reason}");
        assert!(!wire.to_string().contains("private helper detail"));
    }

    #[test]
    fn refused_opencode_launch_reaches_the_public_task_outcome_with_a_hint() {
        for (code, reason) in [
            (
                crate::agent::OPENCODE_DIALECT_MISMATCH,
                "OPENCODE_DIALECT_MISMATCH: the worker's OpenCode is not the generation this turn was built for, so it was not started; run `worker workers --refresh` and retry",
            ),
            (
                crate::agent::OPENCODE_VERSION_UNVERIFIED,
                "OPENCODE_VERSION_UNVERIFIED: `opencode --version` gave no version on the worker, so the turn was not started without --standalone; check OpenCode in the worker's login shell",
            ),
        ] {
            let fixture = publication_fixture(Some(""));
            crate::project_readiness::write_setup_stage_result(
                &fixture.turn_dir,
                &WorkerError::task(code, "private helper detail"),
            )
            .unwrap();
            // The helper exits with the setup-stage code before any agent
            // ran, so no session is bound and nothing was printed.
            invoke_publication(
                &fixture,
                TurnTerminal::Failed,
                TerminalPath::ChildExit(78),
                Some(78),
            )
            .unwrap();
            let status = TaskStore::new(&fixture.store, &crate::process::SystemProcessRunner)
                .load_status(PROJECT_ID, fixture.section.turn().task_id())
                .unwrap();
            let wire = serde_json::to_value(status).unwrap();
            assert_eq!(wire["last_outcome"]["kind"], "failed", "{code}");
            assert_eq!(wire["last_outcome"]["reason"], reason, "{code}");
            assert_eq!(wire["turns"][0]["terminal"], "failed", "{code}");
            assert!(!wire.to_string().contains("private helper detail"));
        }
    }

    fn codex_result_record(summary: &str) -> String {
        let inner = serde_json::json!({
            "status": "done",
            "summary": summary,
            "questions": [],
            "files_changed": [],
        });
        serde_json::json!({
            "type": "item.completed",
            "item": {
                "type": "agent_message",
                "text": inner.to_string(),
            }
        })
        .to_string()
    }

    fn assert_unknown_result(adapter: &'static dyn crate::agent::AgentAdapter, scan: &StdoutScan) {
        assert!(
            !scan.result_text.contains("\"status\":\"done\""),
            "truncated suffix leaked into result text: {}",
            scan.result_text
        );
        assert_eq!(
            adapter
                .extract_result(&scan.result_text, None)
                .unwrap()
                .status(),
            crate::agent::ResultStatus::Unknown
        );
    }

    fn assert_done_result(adapter: &'static dyn crate::agent::AgentAdapter, scan: &StdoutScan) {
        assert_eq!(
            adapter
                .extract_result(&scan.result_text, None)
                .unwrap()
                .status(),
            crate::agent::ResultStatus::Done
        );
    }

    #[test]
    fn scan_stdout_protocol_reports_truncation_and_never_parses_partial_records() {
        let adapter = crate::agent::adapter_for(AgentKind::Codex);

        // A capped incomplete line is truncation, never a result.
        let (_temp, turn_dir) = open_scan_dir();
        let mut stdout = format!("{}\n", codex_session_line()).into_bytes();
        stdout.extend(vec![b'x'; 1024]);
        stdout.extend(b"{\"status\":\"done\"");
        write_stdout(&turn_dir, &stdout);
        let scan = scan_stdout_protocol(&turn_dir, adapter).unwrap();
        assert_eq!(scan.session_ref.as_deref(), Some("session-1"));
        assert!(scan.truncated);
        assert_unknown_result(adapter, &scan);

        // An oversized line is discarded whole; its suffix is never parsed.
        let (_temp, turn_dir) = open_scan_dir();
        let mut stdout = format!("{}\n", codex_session_line()).into_bytes();
        stdout.extend(vec![b'x'; MAX_NDJSON_RECORD_BYTES + 1]);
        stdout.extend(codex_result_record("ok").as_bytes());
        stdout.push(b'\n');
        write_stdout(&turn_dir, &stdout);
        let scan = scan_stdout_protocol(&turn_dir, adapter).unwrap();
        assert_eq!(scan.session_ref.as_deref(), Some("session-1"));
        assert!(!scan.truncated);
        assert_unknown_result(adapter, &scan);

        // Incomplete JSON at EOF is truncation.
        let (_temp, turn_dir) = open_scan_dir();
        let stdout = format!("{}\n{{\"status\":\"done\"", codex_session_line());
        write_stdout(&turn_dir, stdout.as_bytes());
        let scan = scan_stdout_protocol(&turn_dir, adapter).unwrap();
        assert_eq!(scan.session_ref.as_deref(), Some("session-1"));
        assert!(scan.truncated);
        assert_unknown_result(adapter, &scan);

        // A log cap that cuts the stream marks truncation even though a
        // complete result follows the cap.
        let (_temp, turn_dir) = open_scan_dir();
        let mut stdout = format!("{}\n", codex_session_line()).into_bytes();
        stdout.extend(vec![b'x'; 4096]);
        stdout.push(b'\n');
        stdout.extend(codex_result_record("ok").as_bytes());
        stdout.push(b'\n');
        write_stdout(&turn_dir, &stdout);
        let scan = scan_stdout_protocol_with(
            &turn_dir,
            adapter,
            ScanLimits {
                log_cap: 2048,
                max_record: MAX_NDJSON_RECORD_BYTES,
                max_result: crate::task::MAX_PROMPT_BYTES,
            },
        )
        .unwrap();
        assert_eq!(scan.session_ref.as_deref(), Some("session-1"));
        assert!(scan.truncated);
        assert_unknown_result(adapter, &scan);
    }

    #[test]
    fn scan_stdout_protocol_keeps_complete_results_beyond_tails_and_record_limits() {
        let adapter = crate::agent::adapter_for(AgentKind::Codex);

        // A complete final record without a trailing newline is a result.
        let (_temp, turn_dir) = open_scan_dir();
        let stdout = format!("{}\n{}", codex_session_line(), codex_result_record("ok"));
        write_stdout(&turn_dir, stdout.as_bytes());
        let scan = scan_stdout_protocol(&turn_dir, adapter).unwrap();
        assert_eq!(scan.session_ref.as_deref(), Some("session-1"));
        assert!(!scan.truncated);
        assert_done_result(adapter, &scan);

        // A unicode result beyond the display tail survives intact.
        let (_temp, turn_dir) = open_scan_dir();
        let filler = "{\"type\":\"item.updated\",\"delta\":\"привет\"}\n";
        let mut stdout = format!("{}\n", codex_session_line()).into_bytes();
        while stdout.len() <= LOG_TAIL_BYTES {
            stdout.extend_from_slice(filler.as_bytes());
        }
        stdout.extend_from_slice(codex_result_record("готово").as_bytes());
        stdout.push(b'\n');
        write_stdout(&turn_dir, &stdout);
        let scan = scan_stdout_protocol(&turn_dir, adapter).unwrap();
        assert_eq!(scan.session_ref.as_deref(), Some("session-1"));
        assert!(!scan.truncated);
        assert_done_result(adapter, &scan);
        assert!(scan.result_text.contains("готово"));

        // A result larger than the display tail is kept whole.
        let (_temp, turn_dir) = open_scan_dir();
        let summary = "я".repeat((LOG_TAIL_BYTES / 2) + 8);
        assert!(summary.len() > LOG_TAIL_BYTES);
        assert!(summary.len() < crate::task::MAX_PROMPT_BYTES);
        let stdout = format!(
            "{}\n{}\n",
            codex_session_line(),
            codex_result_record(&summary)
        );
        write_stdout(&turn_dir, stdout.as_bytes());
        let scan = scan_stdout_protocol(&turn_dir, adapter).unwrap();
        assert_eq!(scan.session_ref.as_deref(), Some("session-1"));
        assert!(!scan.truncated);
        assert_done_result(adapter, &scan);
        assert!(scan.result_text.len() > LOG_TAIL_BYTES);

        // A near-limit record is kept when later lines share its read chunk.
        let (_temp, turn_dir) = open_scan_dir();
        let session = format!("{}\n", codex_session_line());
        let result = format!("{}\n", codex_result_record("ok"));
        let max_record = session.len() + result.len() - 2;
        assert!(session.trim_end().len() < max_record);
        assert!(result.trim_end().len() < max_record);
        assert!(session.len() + result.len() > max_record);
        write_stdout(&turn_dir, format!("{session}{result}").as_bytes());
        let scan = scan_stdout_protocol_with(
            &turn_dir,
            adapter,
            ScanLimits {
                log_cap: LOG_CAP_BYTES,
                max_record,
                max_result: crate::task::MAX_PROMPT_BYTES,
            },
        )
        .unwrap();
        assert_eq!(scan.session_ref.as_deref(), Some("session-1"));
        assert!(!scan.truncated);
        assert_done_result(adapter, &scan);
    }

    fn structured_payload(status: &str, summary: &str) -> String {
        serde_json::json!({
            "status": status,
            "summary": summary,
            "questions": [],
            "files_changed": [],
        })
        .to_string()
    }

    fn claude_or_cursor_session_line() -> &'static str {
        r#"{"type":"system","subtype":"init","session_id":"session-1"}"#
    }

    fn explicit_result_record(inner: &str) -> String {
        serde_json::json!({ "type": "result", "result": inner }).to_string()
    }

    fn assistant_record(inner: &str) -> String {
        serde_json::json!({
            "type": "assistant",
            "message": { "content": [{ "type": "text", "text": inner }] }
        })
        .to_string()
    }

    fn assert_result_beats_later_assistant(kind: AgentKind) {
        let (_temp, turn_dir) = open_scan_dir();
        let stdout = format!(
            "{}\n{}\n{}\n",
            claude_or_cursor_session_line(),
            explicit_result_record(&structured_payload("done", "from-result")),
            assistant_record(&structured_payload("blocked", "from-assistant")),
        );
        write_stdout(&turn_dir, stdout.as_bytes());
        let adapter = crate::agent::adapter_for(kind);
        let scan = scan_stdout_protocol(&turn_dir, adapter).unwrap();
        assert_eq!(scan.session_ref.as_deref(), Some("session-1"));
        assert!(!scan.truncated);
        let extracted = adapter.extract_result(&scan.result_text, None).unwrap();
        assert_eq!(extracted.status(), crate::agent::ResultStatus::Done);
        assert!(extracted.summary().contains("from-result"));
    }

    #[test]
    fn scan_stdout_protocol_prefers_result_records_over_later_assistant_and_malformed_records() {
        for kind in [AgentKind::Cursor, AgentKind::Claude] {
            assert_result_beats_later_assistant(kind);
        }

        let (_temp, turn_dir) = open_scan_dir();
        let stdout = format!(
            "{}\n{}\n{}\n",
            claude_or_cursor_session_line(),
            explicit_result_record(&structured_payload("done", "kept")),
            r#"{"type":"result","result":"{"}"#,
        );
        write_stdout(&turn_dir, stdout.as_bytes());
        let adapter = crate::agent::adapter_for(AgentKind::Cursor);
        let scan = scan_stdout_protocol(&turn_dir, adapter).unwrap();
        assert!(!scan.truncated);
        let extracted = adapter.extract_result(&scan.result_text, None).unwrap();
        assert_eq!(extracted.status(), crate::agent::ResultStatus::Done);
        assert!(extracted.summary().contains("kept"));
    }
}
