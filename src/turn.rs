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
    git_transport::GitTransport,
    host_store::{HostStore, PublicationReceipt, StagedJob, StagingNonce},
    job::JobId,
    job::{
        CommandSpec, JobMeta, LeaseRecord, MAX_LOG_CHUNK_BYTES, RequestFingerprintMaterial,
        SubmitRequest, SubmitResponse,
    },
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnMaterial {
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
            limits,
            base_oid,
            prompt_sha256,
            env_profile,
            session_seed,
            resume,
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
        let mut record =
            serializer.serialize_struct("TurnMaterial", 11 + usize::from(self.effort.is_some()))?;
        record.serialize_field("task_id", &self.task_id)?;
        record.serialize_field("turn_number", &self.turn_number)?;
        record.serialize_field("agent", &agent_name(self.agent))?;
        record.serialize_field("model", &self.model)?;
        if self.effort.is_some() {
            record.serialize_field("effort", &self.effort)?;
        }
        record.serialize_field("policy", &policy_name(self.policy))?;
        record.serialize_field("limits", &TurnLimitsWire::from(&self.limits))?;
        record.serialize_field("base_oid", &self.base_oid)?;
        record.serialize_field("prompt_sha256", &self.prompt_sha256)?;
        record.serialize_field("env_profile", &self.env_profile)?;
        record.serialize_field("session_seed", &self.session_seed.to_string())?;
        record.serialize_field("resume", &self.resume)?;
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
            limits: TurnLimitsWire,
            base_oid: BaseOid,
            prompt_sha256: String,
            env_profile: Option<String>,
            session_seed: String,
            resume: bool,
        }
        let wire = Wire::deserialize(deserializer)?;
        let session_seed = Uuid::parse_str(&wire.session_seed)
            .map_err(|_| D::Error::custom("session seed is invalid"))?;
        Self::new(
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
        .map_err(D::Error::custom)
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
        let runner = crate::process::SystemProcessRunner;
        let task_store = TaskStore::new(store, &runner);
        let meta = task_store.load_meta(section.project_id(), section.turn().task_id())?;
        let status = task_store.load_status(meta.project_id(), meta.task_id())?;
        let task = PreparedTask::new(meta.clone(), status.clone());
        match TurnPublisher::new(store, &runner).publish_with_terminal(
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
                Ok(result)
            }
            Err(error) => {
                // Publication failure must not leave the task active. Keep
                // the workspace for inspection and make the failure visible
                // as the task's last outcome; the job status remains the
                // authoritative infrastructure terminal record.
                if let Some(turn) = status.turns().last() {
                    let fallback = TaskOutcome::Failed {
                        reason: "PUBLISH_FAILED".into(),
                    };
                    let _ = task_store.finish_turn(
                        meta.project_id(),
                        meta.task_id(),
                        turn.turn_id(),
                        terminal,
                        fallback.clone(),
                        false,
                        log_truncated,
                        status.head_oid().cloned(),
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
                Err(error)
            }
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
    let Some(home) = std::env::var_os("HOME").filter(|home| !home.is_empty()) else {
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
    let reported = crate::herdr_reporter::HerdrReporter::for_home(std::path::Path::new(&home))
        .terminal(&identity, pane_id.as_deref(), outcome, summary, questions);
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
        let structured = adapter
            .extract_result(&stream, last_message.as_deref().or(tail.as_deref()))
            .map_err(|error| turn_error("PUBLISH_FAILED", error.to_string()))?;

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
        let stdout_auth = crate::agent::tail_utf8(&stream, crate::agent::AUTH_SCAN_TAIL_BYTES);
        let stderr_auth = read_auth_scan_tail(turn_dir, "stderr.log")?;
        // Only Succeeded/Failed would otherwise become "agent exited N". A
        // Cancelled/TimedOut/Lost turn keeps that terminal even when the
        // stderr tail matches an auth phrase.
        let auth_failed = matches!(terminal, TurnTerminal::Succeeded | TurnTerminal::Failed)
            && matches!(agent_outcome, crate::agent::AgentOutcome::Failed { .. })
            && adapter.output_shows_auth_failure(stdout_auth, &stderr_auth);
        let outcome = if auth_failed {
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
        let close = meta.close_policy() == ClosePolicy::Done && outcome == TaskOutcome::Done;
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
            let mirror = self
                .store
                .mirror_if_present(meta.project_id())?
                .ok_or_else(|| turn_error("PUBLISH_FAILED", "project mirror is absent"))?;
            let branch = meta
                .publish_branch()
                .cloned()
                .unwrap_or_else(|| BranchName::for_task(meta.task_id()));
            GitTransport::new(self.runner).push_origin(
                expected_origin,
                meta.task_id(),
                &branch,
                &mirror,
            )?;
        }
        let boundary = RedactionBoundary::from_env();
        let summary = nonempty(&boundary.summary(structured.summary()));
        let questions = boundary.questions(structured.questions());
        let agent_files_changed = boundary.changed_files(structured.files_changed());
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
            structured.checks().to_vec(),
            false,
        )?;
        if close {
            let _ = TaskStore::new(self.store, self.runner).close(&TaskCloseRequest::new(
                meta.project_id(),
                meta.task_id(),
                false,
            ))?;
        }
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
                args,
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
        agent::AgentKind,
        host_store::HostStore,
        process::SystemProcessRunner,
        rooted_fs::RootedDir,
        task::TaskId,
        task_store::{SessionBinding, TaskPrebindRequest},
    };
    use tempfile::tempdir;

    const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

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

    fn codex_session_line() -> &'static str {
        "{\"type\":\"thread.started\",\"thread_id\":\"session-1\"}"
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
    fn scan_stdout_protocol_does_not_treat_a_capped_incomplete_line_as_a_result() {
        let (_temp, turn_dir) = open_scan_dir();
        let mut stdout = format!("{}\n", codex_session_line()).into_bytes();
        stdout.extend(vec![b'x'; 1024]);
        stdout.extend(b"{\"status\":\"done\"");
        write_stdout(&turn_dir, &stdout);
        let adapter = crate::agent::adapter_for(AgentKind::Codex);
        let scan = scan_stdout_protocol(&turn_dir, adapter).unwrap();
        assert_eq!(scan.session_ref.as_deref(), Some("session-1"));
        assert!(scan.truncated);
        assert_unknown_result(adapter, &scan);
    }

    #[test]
    fn scan_stdout_protocol_discards_an_oversized_line_instead_of_parsing_its_suffix() {
        let (_temp, turn_dir) = open_scan_dir();
        let mut stdout = format!("{}\n", codex_session_line()).into_bytes();
        stdout.extend(vec![b'x'; MAX_NDJSON_RECORD_BYTES + 1]);
        stdout.extend(codex_result_record("ok").as_bytes());
        stdout.push(b'\n');
        write_stdout(&turn_dir, &stdout);
        let adapter = crate::agent::adapter_for(AgentKind::Codex);
        let scan = scan_stdout_protocol(&turn_dir, adapter).unwrap();
        assert_eq!(scan.session_ref.as_deref(), Some("session-1"));
        assert!(!scan.truncated);
        assert_unknown_result(adapter, &scan);
    }

    #[test]
    fn scan_stdout_protocol_accepts_a_complete_final_json_record_without_a_newline() {
        let (_temp, turn_dir) = open_scan_dir();
        let stdout = format!("{}\n{}", codex_session_line(), codex_result_record("ok"));
        write_stdout(&turn_dir, stdout.as_bytes());
        let adapter = crate::agent::adapter_for(AgentKind::Codex);
        let scan = scan_stdout_protocol(&turn_dir, adapter).unwrap();
        assert_eq!(scan.session_ref.as_deref(), Some("session-1"));
        assert!(!scan.truncated);
        assert_done_result(adapter, &scan);
    }

    #[test]
    fn scan_stdout_protocol_keeps_a_unicode_result_beyond_the_display_tail() {
        let (_temp, turn_dir) = open_scan_dir();
        let filler = "{\"type\":\"item.updated\",\"delta\":\"привет\"}\n";
        let mut stdout = format!("{}\n", codex_session_line()).into_bytes();
        while stdout.len() <= LOG_TAIL_BYTES {
            stdout.extend_from_slice(filler.as_bytes());
        }
        stdout.extend_from_slice(codex_result_record("готово").as_bytes());
        stdout.push(b'\n');
        write_stdout(&turn_dir, &stdout);
        let adapter = crate::agent::adapter_for(AgentKind::Codex);
        let scan = scan_stdout_protocol(&turn_dir, adapter).unwrap();
        assert_eq!(scan.session_ref.as_deref(), Some("session-1"));
        assert!(!scan.truncated);
        assert_done_result(adapter, &scan);
        assert!(scan.result_text.contains("готово"));
    }

    #[test]
    fn scan_stdout_protocol_keeps_a_result_larger_than_the_display_tail() {
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
        let adapter = crate::agent::adapter_for(AgentKind::Codex);
        let scan = scan_stdout_protocol(&turn_dir, adapter).unwrap();
        assert_eq!(scan.session_ref.as_deref(), Some("session-1"));
        assert!(!scan.truncated);
        assert_done_result(adapter, &scan);
        assert!(scan.result_text.len() > LOG_TAIL_BYTES);
    }

    #[test]
    fn scan_stdout_protocol_marks_truncated_when_log_cap_cuts_the_stream() {
        let (_temp, turn_dir) = open_scan_dir();
        let mut stdout = format!("{}\n", codex_session_line()).into_bytes();
        stdout.extend(vec![b'x'; 4096]);
        stdout.push(b'\n');
        stdout.extend(codex_result_record("ok").as_bytes());
        stdout.push(b'\n');
        write_stdout(&turn_dir, &stdout);
        let adapter = crate::agent::adapter_for(AgentKind::Codex);
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
    fn scan_stdout_protocol_treats_incomplete_json_at_eof_as_truncated() {
        let (_temp, turn_dir) = open_scan_dir();
        let stdout = format!("{}\n{{\"status\":\"done\"", codex_session_line());
        write_stdout(&turn_dir, stdout.as_bytes());
        let adapter = crate::agent::adapter_for(AgentKind::Codex);
        let scan = scan_stdout_protocol(&turn_dir, adapter).unwrap();
        assert_eq!(scan.session_ref.as_deref(), Some("session-1"));
        assert!(scan.truncated);
        assert_unknown_result(adapter, &scan);
    }

    #[test]
    fn scan_stdout_protocol_keeps_a_near_limit_record_when_later_lines_share_the_chunk() {
        let (_temp, turn_dir) = open_scan_dir();
        let session = format!("{}\n", codex_session_line());
        let result = format!("{}\n", codex_result_record("ok"));
        let max_record = session.len() + result.len() - 2;
        assert!(session.trim_end().len() < max_record);
        assert!(result.trim_end().len() < max_record);
        assert!(session.len() + result.len() > max_record);
        write_stdout(&turn_dir, format!("{session}{result}").as_bytes());
        let adapter = crate::agent::adapter_for(AgentKind::Codex);
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
    fn scan_stdout_protocol_prefers_cursor_result_records_over_later_assistants() {
        assert_result_beats_later_assistant(AgentKind::Cursor);
    }

    #[test]
    fn scan_stdout_protocol_prefers_claude_result_records_over_later_assistants() {
        assert_result_beats_later_assistant(AgentKind::Claude);
    }

    #[test]
    fn scan_stdout_protocol_skips_a_malformed_later_result_and_keeps_the_earlier_one() {
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
