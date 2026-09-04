use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fmt,
    fs::OpenOptions,
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

use serde::{de::Error as DeError, ser::SerializeStruct};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    agent::{AgentKind, PermissionPolicy, TurnLaunch, TurnLimits, render_shell},
    error::WorkerError,
    host_store::{HostStore, PublicationReceipt, StagedJob, StagingNonce},
    job::JobId,
    job::{
        CommandSpec, JobMeta, LeaseRecord, RequestFingerprintMaterial, SubmitRequest,
        SubmitResponse,
    },
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
    redaction::RedactionBoundary,
    rooted_fs::RootedDir,
    task::{
        BaseOid, ClosePolicy, GitIdentity, TaskId, TaskMeta, TaskOutcome, TaskStatus, TurnTerminal,
    },
    task_store::{SessionBinding, TaskCloseRequest, TaskStore},
};

pub const LOG_CAP_BYTES: u64 = 256 * 1024 * 1024;
pub const LOG_TAIL_BYTES: usize = 64 * 1024;
const MAX_ENV_PROFILE_BYTES: u64 = 64 * 1024;
const MAX_ENV_NAME_BYTES: usize = 128;
const MAX_ENV_VALUE_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnMaterial {
    task_id: TaskId,
    turn_number: u32,
    agent: AgentKind,
    model: Option<String>,
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
        let mut record = serializer.serialize_struct("TurnMaterial", 11)?;
        record.serialize_field("task_id", &self.task_id)?;
        record.serialize_field("turn_number", &self.turn_number)?;
        record.serialize_field("agent", &agent_name(self.agent))?;
        record.serialize_field("model", &self.model)?;
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
        let section = Self {
            turn,
            project_id: project_id.into(),
            git_identity,
        };
        section.turn.validate()?;
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

    pub fn turn(&self) -> &TurnMaterial {
        &self.turn
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn git_identity(&self) -> &GitIdentity {
        &self.git_identity
    }
}

impl serde::Serialize for TurnSection {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut record = serializer.serialize_struct("TurnSection", 3)?;
        record.serialize_field("turn", &self.turn)?;
        record.serialize_field("project_id", &self.project_id)?;
        record.serialize_field("git_identity", &self.git_identity)?;
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
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.turn, wire.project_id, wire.git_identity).map_err(D::Error::custom)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct TaskTurnRequest {
    submit: SubmitRequest,
    turn: TurnMaterial,
    prompt: String,
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
        }
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        self.submit.validate()?;
        self.turn.validate()?;
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

    #[doc(hidden)]
    pub fn with_turn(&self, turn: TurnMaterial) -> Self {
        Self {
            submit: self.submit.clone(),
            turn,
            prompt: self.prompt.clone(),
        }
    }

    #[doc(hidden)]
    pub fn with_prompt(&self, prompt: impl Into<String>) -> Self {
        Self {
            submit: self.submit.clone(),
            turn: self.turn.clone(),
            prompt: prompt.into(),
        }
    }
}

impl serde::Serialize for TaskTurnRequest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(serde::ser::Error::custom)?;
        let mut record = serializer.serialize_struct("TaskTurnRequest", 3)?;
        record.serialize_field("submit", &self.submit)?;
        record.serialize_field("turn", &self.turn)?;
        record.serialize_field("prompt", &self.prompt)?;
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
        }
        let wire = Wire::deserialize(deserializer)?;
        let request = Self::new(wire.submit, wire.turn, wire.prompt);
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
        log_truncated: bool,
    ) -> Result<TurnResult, WorkerError> {
        let _ = path;
        let _ = job_meta;
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
            log_truncated,
        ) {
            Ok(result) => Ok(result),
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
                        fallback,
                        false,
                        log_truncated,
                        status.head_oid().cloned(),
                        None,
                        Vec::new(),
                        Vec::new(),
                        None,
                        false,
                    );
                }
                Err(error)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnResult {
    outcome: TaskOutcome,
    agent_committed: bool,
    log_truncated: bool,
    head_oid: Option<BaseOid>,
    summary: Option<String>,
    questions: Vec<String>,
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
        self.publish_with_terminal(task, turn_dir, terminal, exit_code, false)
    }

    pub(crate) fn publish_with_terminal(
        &self,
        task: &PreparedTask,
        turn_dir: &RootedDir,
        terminal: TurnTerminal,
        exit_code: Option<i32>,
        log_truncated: bool,
    ) -> Result<TurnResult, WorkerError> {
        let meta = task.meta();
        let turn_id = task
            .status()
            .turns()
            .last()
            .map(|turn| turn.turn_id())
            .ok_or_else(|| turn_error("PUBLISH_FAILED", "task has no turn history"))?;
        let stream = read_text(turn_dir, "stdout.log", LOG_CAP_BYTES)?;
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
        let session = ensure_session_binding(self.store, meta, &stream, tail.as_deref(), adapter)?;
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
        let outcome = TaskOutcome::from_turn(terminal, Some(agent_outcome));
        let close = meta.close_policy() == ClosePolicy::Done && outcome == TaskOutcome::Done;
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
    stream: &str,
    tail: Option<&str>,
    adapter: &'static dyn crate::agent::AgentAdapter,
) -> Result<Option<SessionBinding>, WorkerError> {
    let task_store = TaskStore::new(store, &crate::process::SystemProcessRunner);
    if let Some(binding) = task_store.session(meta.project_id(), meta.task_id())? {
        return Ok(Some(binding));
    }
    let mut session_ref = None;
    for candidate in [stream, tail.unwrap_or_default()] {
        let events = candidate
            .lines()
            .filter_map(|line| adapter.parse_event(line))
            .collect::<Vec<_>>();
        if let Some(found) = adapter.session_ref(&events) {
            session_ref = Some(found);
            break;
        }
    }
    let Some(session_ref) = session_ref else {
        return Ok(None);
    };
    let binding = SessionBinding::new(adapter.kind(), session_ref, now_millis()?)?;
    task_store.bind_session(meta.project_id(), meta.task_id(), binding.clone())?;
    Ok(Some(binding))
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

    pub fn questions(&self) -> &[String] {
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
        }
    }

    pub fn load(path: &Path) -> Result<Self, WorkerError> {
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
            if worker_controlled_env_name(name) {
                return Err(turn_error(
                    "ENV_PROFILE_INVALID",
                    "env profile cannot override worker-controlled variables",
                ));
            }
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
            if seen.insert(name.to_owned(), ()).is_some() {
                return Err(turn_error(
                    "ENV_PROFILE_INVALID",
                    "env profile repeats a variable",
                ));
            }
            entries.push((OsString::from(name), OsString::from(value)));
        }
        let names = entries
            .iter()
            .map(|(name, _)| name.to_string_lossy().into_owned())
            .collect();
        Ok(Self { names, entries })
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }

    pub(crate) fn entries(&self) -> &[(OsString, OsString)] {
        &self.entries
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
            | "GIT_AUTHOR_NAME"
            | "GIT_AUTHOR_EMAIL"
            | "GIT_COMMITTER_NAME"
            | "GIT_COMMITTER_EMAIL"
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
