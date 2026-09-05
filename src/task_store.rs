use std::{
    ffi::OsString,
    fs, io,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};

use crate::{
    agent::AgentKind,
    error::WorkerError,
    git_transport::GitTransport,
    host_store::{HostStore, TransferGuard},
    job::JobId,
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
    protocol::PROTOCOL_VERSION,
    rooted_fs::RootedDir,
    task::{
        BaseOid, TaskId, TaskMeta, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnSummary,
        TurnTerminal,
    },
};

const GIT_PROGRAM: &str = "/usr/bin/git";
const GIT_STDOUT_LIMIT: usize = 2 * 1024 * 1024;
const GIT_STDERR_LIMIT: usize = 128 * 1024;
const GIT_DEADLINE: Duration = Duration::from_secs(15 * 60);
const MAX_TASK_RECORD_BYTES: u64 = 1024 * 1024;
const MAX_CLOSE_WARNINGS: usize = 8;
const MAX_CLOSE_WARNING_BYTES: usize = 256;
const NATIVE_SESSION_DELETE_WARNING: &str = "native agent session deletion failed";
const GIT_CONFIG_GLOBAL: &str = "GIT_CONFIG_GLOBAL";
const GIT_CONFIG_NOSYSTEM: &str = "GIT_CONFIG_NOSYSTEM";
const GIT_TERMINAL_PROMPT: &str = "GIT_TERMINAL_PROMPT";

pub const MAX_DIFF_BYTES: usize = 512 * 1024;

const GIT_ENVIRONMENT_REMOVALS: &[&str] = &[
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
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionBinding {
    agent: AgentKind,
    session_ref: String,
    bound_at_millis: u64,
}

impl SessionBinding {
    pub fn new(
        agent: AgentKind,
        session_ref: impl Into<String>,
        bound_at_millis: u64,
    ) -> Result<Self, WorkerError> {
        let binding = Self {
            agent,
            session_ref: session_ref.into(),
            bound_at_millis,
        };
        binding.validate()?;
        Ok(binding)
    }

    pub fn agent(&self) -> AgentKind {
        self.agent
    }

    pub fn session_ref(&self) -> &str {
        &self.session_ref
    }

    pub fn bound_at_millis(&self) -> u64 {
        self.bound_at_millis
    }

    fn validate(&self) -> Result<(), WorkerError> {
        if self.session_ref.is_empty()
            || self.session_ref.len() > 256
            || self.session_ref.chars().any(char::is_control)
        {
            return Err(task_error(
                "TASK_SESSION_INVALID",
                "session reference is empty, too long, or contains a control character",
            ));
        }
        Ok(())
    }
}

impl Serialize for SessionBinding {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(serde::ser::Error::custom)?;
        let mut record = serializer.serialize_struct("SessionBinding", 3)?;
        record.serialize_field("agent", agent_name(self.agent))?;
        record.serialize_field("session_ref", &self.session_ref)?;
        record.serialize_field("bound_at_millis", &self.bound_at_millis)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for SessionBinding {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            agent: String,
            session_ref: String,
            bound_at_millis: u64,
        }

        let wire = Wire::deserialize(deserializer)?;
        let agent = parse_agent(&wire.agent).map_err(serde::de::Error::custom)?;
        Self::new(agent, wire.session_ref, wire.bound_at_millis).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSessionRequest {
    protocol_version: u32,
    project_id: String,
    task_id: TaskId,
}

impl TaskSessionRequest {
    pub fn new(project_id: impl Into<String>, task_id: TaskId) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            project_id: project_id.into(),
            task_id,
        }
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub(crate) fn validate(&self) -> Result<(), WorkerError> {
        ensure_protocol(self.protocol_version)?;
        validate_project_id(&self.project_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSessionResponse {
    protocol_version: u32,
    binding: SessionBinding,
}

impl TaskSessionResponse {
    pub fn new(binding: SessionBinding) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            binding,
        }
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn binding(&self) -> &SessionBinding {
        &self.binding
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskPrebindRequest {
    protocol_version: u32,
    project_id: String,
    task_id: TaskId,
    agent: String,
    env_profile: Option<String>,
    session_ref: Option<String>,
}

impl TaskPrebindRequest {
    pub fn discover(
        project_id: impl Into<String>,
        task_id: TaskId,
        agent: AgentKind,
        env_profile: Option<String>,
    ) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            project_id: project_id.into(),
            task_id,
            agent: agent_name(agent).to_string(),
            env_profile,
            session_ref: None,
        }
    }

    pub fn persist(
        project_id: impl Into<String>,
        task_id: TaskId,
        agent: AgentKind,
        env_profile: Option<String>,
        session_ref: impl Into<String>,
    ) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            project_id: project_id.into(),
            task_id,
            agent: agent_name(agent).to_string(),
            env_profile,
            session_ref: Some(session_ref.into()),
        }
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn agent(&self) -> Result<AgentKind, WorkerError> {
        parse_agent(&self.agent)
    }

    pub fn env_profile(&self) -> Option<&str> {
        self.env_profile.as_deref()
    }

    pub fn session_ref(&self) -> Option<&str> {
        self.session_ref.as_deref()
    }

    pub(crate) fn validate(&self) -> Result<(), WorkerError> {
        ensure_protocol(self.protocol_version)?;
        validate_project_id(&self.project_id)?;
        parse_agent(&self.agent)?;
        if let Some(profile) = &self.env_profile
            && (profile.is_empty()
                || profile.len() > 128
                || profile.contains('/')
                || profile.contains('\\')
                || profile == "."
                || profile == "..")
        {
            return Err(task_error(
                "TASK_CONFIG_INVALID",
                "environment profile name is invalid",
            ));
        }
        if let Some(session_ref) = &self.session_ref {
            SessionBinding::new(parse_agent(&self.agent)?, session_ref, 1)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskCancelRequest {
    protocol_version: u32,
    project_id: String,
    task_id: TaskId,
    turn_id: JobId,
}

impl TaskCancelRequest {
    pub fn new(project_id: impl Into<String>, task_id: TaskId, turn_id: JobId) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            project_id: project_id.into(),
            task_id,
            turn_id,
        }
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn turn_id(&self) -> JobId {
        self.turn_id
    }

    pub(crate) fn validate(&self) -> Result<(), WorkerError> {
        ensure_protocol(self.protocol_version)?;
        validate_project_id(&self.project_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskCancelResponse {
    protocol_version: u32,
    status: TaskStatus,
}

impl TaskCancelResponse {
    pub fn new(status: TaskStatus) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            status,
        }
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn status(&self) -> &TaskStatus {
        &self.status
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskPrepareRequest {
    protocol_version: u32,
    meta: TaskMeta,
    job_id: JobId,
    worker: String,
}

impl TaskPrepareRequest {
    pub fn new(meta: TaskMeta, job_id: JobId, worker: impl Into<String>) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            meta,
            job_id,
            worker: worker.into(),
        }
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn meta(&self) -> &TaskMeta {
        &self.meta
    }

    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn worker(&self) -> &str {
        &self.worker
    }

    fn validate(&self) -> Result<(), WorkerError> {
        ensure_protocol(self.protocol_version)?;
        serde_json::to_vec(&self.meta)
            .map_err(|error| task_error("TASK_CONFIG_INVALID", error.to_string()))?;
        validate_worker_name(&self.worker)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskPrepareResponse {
    protocol_version: u32,
    head: BaseOid,
    reused: bool,
}

impl TaskPrepareResponse {
    pub fn new(head: BaseOid, reused: bool) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            head,
            reused,
        }
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn head(&self) -> &BaseOid {
        &self.head
    }

    pub fn reused(&self) -> bool {
        self.reused
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskStatusRequest {
    protocol_version: u32,
    project_id: String,
    task_id: TaskId,
}

impl TaskStatusRequest {
    pub fn new(project_id: impl Into<String>, task_id: TaskId) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            project_id: project_id.into(),
            task_id,
        }
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    fn validate(&self) -> Result<(), WorkerError> {
        ensure_protocol(self.protocol_version)?;
        validate_project_id(&self.project_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskStatusResponse {
    protocol_version: u32,
    status: TaskStatus,
}

impl TaskStatusResponse {
    pub fn new(status: TaskStatus) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            status,
        }
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn status(&self) -> &TaskStatus {
        &self.status
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskDiffRequest {
    protocol_version: u32,
    project_id: String,
    task_id: TaskId,
    stat: bool,
}

impl TaskDiffRequest {
    pub fn new(project_id: impl Into<String>, task_id: TaskId, stat: bool) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            project_id: project_id.into(),
            task_id,
            stat,
        }
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn stat(&self) -> bool {
        self.stat
    }

    fn validate(&self) -> Result<(), WorkerError> {
        ensure_protocol(self.protocol_version)?;
        validate_project_id(&self.project_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskDiffResponse {
    protocol_version: u32,
    text: String,
    truncated: bool,
}

impl TaskDiffResponse {
    pub fn new(text: String, truncated: bool) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            text,
            truncated,
        }
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn truncated(&self) -> bool {
        self.truncated
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskCloseRequest {
    protocol_version: u32,
    project_id: String,
    task_id: TaskId,
    discard: bool,
}

impl TaskCloseRequest {
    pub fn new(project_id: impl Into<String>, task_id: TaskId, discard: bool) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            project_id: project_id.into(),
            task_id,
            discard,
        }
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn discard(&self) -> bool {
        self.discard
    }

    fn validate(&self) -> Result<(), WorkerError> {
        ensure_protocol(self.protocol_version)?;
        validate_project_id(&self.project_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskCloseResponse {
    protocol_version: u32,
    status: TaskStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
}

impl TaskCloseResponse {
    pub fn new(status: TaskStatus) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            status,
            warnings: Vec::new(),
        }
    }

    pub(crate) fn with_warnings(status: TaskStatus, warnings: Vec<String>) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            status,
            warnings,
        }
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn status(&self) -> &TaskStatus {
        &self.status
    }

    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    pub(crate) fn validate(&self) -> Result<(), WorkerError> {
        ensure_protocol(self.protocol_version)?;
        if self.warnings.len() > MAX_CLOSE_WARNINGS
            || self.warnings.iter().any(|warning| {
                warning.is_empty()
                    || warning.len() > MAX_CLOSE_WARNING_BYTES
                    || warning.chars().any(char::is_control)
            })
        {
            return Err(WorkerError::Protocol(
                "invalid task close warning metadata".into(),
            ));
        }
        Ok(())
    }
}

pub struct TaskStore<'a> {
    store: &'a HostStore,
    runner: &'a dyn ProcessRunner,
}

impl<'a> TaskStore<'a> {
    pub fn new(store: &'a HostStore, runner: &'a dyn ProcessRunner) -> Self {
        Self { store, runner }
    }

    pub fn prepare(
        &self,
        request: &TaskPrepareRequest,
        guard: &TransferGuard,
    ) -> Result<TaskPrepareResponse, WorkerError> {
        request.validate()?;
        guard.validate()?;
        if guard.job_id() != request.job_id() {
            return Err(task_error(
                "TASK_LEASE_MISMATCH",
                "task preparation requires the matching transfer guard",
            ));
        }
        let meta = request.meta();
        let task_id = meta.task_id();
        // This is intentionally the first filesystem lookup under `tasks/`:
        // a missing or invalid base must not leave a task directory behind.
        let mirror = match meta.source() {
            TaskSource::Origin { url } => {
                let mirror = self.store.mirror(meta.project_id())?;
                GitTransport::new(self.runner).fetch_origin(url, meta.base_oid(), &mirror)?;
                mirror
            }
            TaskSource::Local { .. } => self
                .store
                .mirror_if_present(meta.project_id())?
                .ok_or_else(|| git_error("BASE_UNAVAILABLE", "project mirror is absent"))?,
        };
        self.verify_base(&mirror, meta.base_oid())?;

        let task = self
            .store
            .open_task_directory(meta.project_id(), task_id, true)?;
        self.ensure_meta(&task, meta)?;

        let (reused, _workspace) = self.prepare_workspace(&task, &mirror, meta)?;
        self.ensure_active_status(&task, meta, request.worker(), request.job_id())?;
        task.sync_root()?;
        Ok(TaskPrepareResponse::new(meta.base_oid().clone(), reused))
    }

    pub fn status(&self, request: &TaskStatusRequest) -> Result<TaskStatusResponse, WorkerError> {
        request.validate()?;
        Ok(TaskStatusResponse::new(
            self.load_status(request.project_id(), request.task_id())?,
        ))
    }

    pub fn diff(&self, request: &TaskDiffRequest) -> Result<TaskDiffResponse, WorkerError> {
        request.validate()?;
        let task = self.open_existing_task(request.project_id(), request.task_id())?;
        let meta = self.read_meta(&task)?;
        let workspace = self.open_workspace(&task)?;
        let index_path = self.git_index_path(workspace.path())?;
        let index_bytes = fs::read(&index_path).map_err(|error| {
            task_error("DIFF_FAILED", format!("could not read Git index: {error}"))
        })?;
        let index_metadata = fs::symlink_metadata(&index_path).map_err(|error| {
            task_error(
                "DIFF_FAILED",
                format!("could not inspect Git index: {error}"),
            )
        })?;
        if !index_metadata.file_type().is_file() {
            return Err(task_error("DIFF_FAILED", "Git index is not a regular file"));
        }

        let temporary_name = format!(".diff-index-{}", uuid::Uuid::new_v4().simple());
        let temporary = task.write_new_private_file(&temporary_name, &index_bytes)?;
        temporary.sync_all()?;
        drop(temporary);

        let temporary_index = Some((
            OsString::from("GIT_INDEX_FILE"),
            task.path().join(&temporary_name).into_os_string(),
        ));
        let add = self.runner.run(&git_request(
            vec![
                OsString::from("-C"),
                workspace.path().as_os_str().to_os_string(),
                OsString::from("add"),
                OsString::from("--intent-to-add"),
                OsString::from("--"),
                OsString::from("."),
            ],
            temporary_index.clone(),
        ));
        let add = match add {
            Ok(add) => add,
            Err(error) => {
                let _ = remove_temporary_index(&task, &temporary_name);
                return Err(task_error("DIFF_FAILED", error.to_string()));
            }
        };
        if !add.status.success() {
            let _ = remove_temporary_index(&task, &temporary_name);
            return Err(task_error(
                "DIFF_FAILED",
                String::from_utf8_lossy(&add.stderr).trim().to_owned(),
            ));
        }

        let mut diff_args = vec![
            OsString::from("-C"),
            workspace.path().as_os_str().to_os_string(),
            OsString::from("diff"),
        ];
        if request.stat() {
            diff_args.push(OsString::from("--stat"));
        }
        diff_args.push(meta.base_oid().to_string().into());
        let result = self.runner.run(&git_request(diff_args, temporary_index));
        let cleanup = remove_temporary_index(&task, &temporary_name);
        if let Err(error) = cleanup {
            return Err(task_error(
                "DIFF_FAILED",
                format!("could not remove temporary Git index: {error}"),
            ));
        }
        let result = result.map_err(|error| task_error("DIFF_FAILED", error.to_string()))?;
        if !result.status.success() {
            return Err(task_error(
                "DIFF_FAILED",
                String::from_utf8_lossy(&result.stderr).trim().to_owned(),
            ));
        }
        let (text, truncated) = bound_diff(&result.stdout)?;
        Ok(TaskDiffResponse::new(text, truncated))
    }

    pub fn close(&self, request: &TaskCloseRequest) -> Result<TaskCloseResponse, WorkerError> {
        request.validate()?;
        let task = self.open_existing_task(request.project_id(), request.task_id())?;
        let status = self.read_status(&task)?;
        if status.state() == TaskState::Active {
            return Err(task_error("TASK_BUSY", "task has an active turn"));
        }

        // Prove the discard target before changing the task. Once the
        // terminal state is durable, any later cleanup failure is safe to
        // retry without making an open task look partially deleted.
        let mirror = if request.discard() {
            Some(
                self.store
                    .mirror_if_present(request.project_id())?
                    .ok_or_else(|| git_error("BASE_UNAVAILABLE", "project mirror is absent"))?,
            )
        } else {
            None
        };
        let next_state = if request.discard() {
            TaskState::Abandoned
        } else {
            TaskState::Closed
        };
        let status = replace_status_record_at(&task, status, next_state, None, now_millis()?)?;

        if task.entry_exists("workspace")? {
            task.validate_private_entry("workspace")?;
            self.store
                .remove_owned_child_committed(&task, "workspace")?;
        }

        let mut warnings = Vec::new();
        if let Some(mirror) = mirror {
            self.delete_ref(&mirror, &format!("refs/heads/task/{}", request.task_id()))?;
            self.delete_ref(
                &mirror,
                &format!("refs/mac-worker/bases/{}", request.task_id()),
            )?;
            if self
                .delete_native_session(request.project_id(), request.task_id())
                .is_err()
            {
                push_close_warning(&mut warnings, NATIVE_SESSION_DELETE_WARNING);
            }
        }
        task.sync_root()?;
        Ok(TaskCloseResponse::with_warnings(status, warnings))
    }

    /// Closes an idle open task as part of retention GC.  Retention closure
    /// intentionally keeps the task record, session binding, turn history,
    /// and result refs; it only removes the workspace and advances the
    /// activity timestamp so the v1 metadata retention window starts at the
    /// automatic close.
    pub(crate) fn close_for_retention(
        &self,
        project_id: &str,
        task_id: TaskId,
        now_millis: u64,
    ) -> Result<Option<TaskStatus>, WorkerError> {
        validate_project_id(project_id)?;
        let task = self.open_existing_task(project_id, task_id)?;
        let status = self.read_status(&task)?;
        if status.state() != TaskState::Open {
            return Ok(None);
        }
        if task.entry_exists("workspace")? {
            task.validate_private_entry("workspace")?;
            self.store
                .remove_owned_child_committed(&task, "workspace")?;
        }
        let next = TaskStatus::new(
            TaskState::Closed,
            status.last_outcome().cloned(),
            status.worker().map(str::to_owned),
            status.session_present(),
            status.head_oid().cloned(),
            status.summary().map(str::to_owned),
            status.questions().to_vec(),
            status.files_changed().to_vec(),
            status.diff_stat().map(str::to_owned),
            status.turns().to_vec(),
            now_millis,
        )?;
        let next = replace_status_bytes(&task, status, next)?;
        task.sync_root()?;
        Ok(Some(next))
    }

    pub fn publish_branch_into_mirror(
        &self,
        project_id: &str,
        task_id: TaskId,
    ) -> Result<(), WorkerError> {
        validate_project_id(project_id)?;
        let task = self.open_existing_task(project_id, task_id)?;
        let workspace = self.open_workspace(&task)?;
        let mirror = self
            .store
            .mirror_if_present(project_id)?
            .ok_or_else(|| git_error("PUBLISH_FAILED", "project mirror is absent"))?;
        let branch = format!("refs/heads/task/{task_id}");
        let refspec = format!("+{branch}:{branch}");
        let result = self
            .runner
            .run(&git_request(
                vec![
                    OsString::from("-C"),
                    mirror.path().as_os_str().to_os_string(),
                    OsString::from("fetch"),
                    workspace.path().as_os_str().to_os_string(),
                    refspec.into(),
                ],
                None,
            ))
            .map_err(|error| git_error("PUBLISH_FAILED", error.to_string()))?;
        if !result.status.success() {
            return Err(git_error(
                "PUBLISH_FAILED",
                String::from_utf8_lossy(&result.stderr).trim().to_owned(),
            ));
        }
        let status = self.read_status(&task)?;
        if status.state() == TaskState::Active {
            replace_status_record(&task, status, TaskState::Open, None)?;
        }
        task.sync_root()?;
        Ok(())
    }

    pub fn load_meta(&self, project_id: &str, task_id: TaskId) -> Result<TaskMeta, WorkerError> {
        let task = self.open_existing_task(project_id, task_id)?;
        self.read_meta(&task)
    }

    pub fn load_status(
        &self,
        project_id: &str,
        task_id: TaskId,
    ) -> Result<TaskStatus, WorkerError> {
        let task = self.open_existing_task(project_id, task_id)?;
        self.read_status(&task)
    }

    pub fn prepared_turn(
        &self,
        project_id: &str,
        task_id: TaskId,
        turn_id: JobId,
    ) -> Result<(TaskMeta, TaskStatus), WorkerError> {
        let task = self.open_existing_task(project_id, task_id)?;
        let meta = self.read_meta(&task)?;
        let status = self.read_status(&task)?;
        let pending = status
            .turns()
            .last()
            .filter(|turn| turn.turn_id() == turn_id && turn.terminal().is_none());
        if status.state() != TaskState::Active || pending.is_none() {
            return Err(task_error(
                "TASK_BUSY",
                "task does not have the requested prepared active turn",
            ));
        }
        Ok((meta, status))
    }

    /// Reopens an existing task workspace for a follow-up turn without
    /// recreating it from the original base. The caller must already hold
    /// the exact task-turn lease; this method is the host-side durable
    /// transition from Open back to Active.
    pub fn prepare_resume(
        &self,
        project_id: &str,
        task_id: TaskId,
        turn_id: JobId,
        turn_number: u32,
        worker: &str,
        base_oid: &BaseOid,
    ) -> Result<(TaskMeta, TaskStatus), WorkerError> {
        validate_project_id(project_id)?;
        validate_worker_name(worker)?;
        if turn_number == 0 {
            return Err(task_error(
                "REQUEST_CONFLICT",
                "resumed turn number must be positive",
            ));
        }

        let task = self.open_existing_task(project_id, task_id)?;
        let meta = self.read_meta(&task)?;
        let current = self.read_status(&task)?;
        match current.state() {
            TaskState::Open => {}
            TaskState::Active => {
                if current
                    .turns()
                    .last()
                    .is_some_and(|turn| turn.turn_id() == turn_id && turn.terminal().is_none())
                {
                    if current.worker() != Some(worker) {
                        return Err(task_error(
                            "TASK_TURN_CONFLICT",
                            "resumed turn worker does not match the task session worker",
                        ));
                    }
                    return Ok((meta, current));
                }
                return Err(task_error("TASK_BUSY", "task already has an active turn"));
            }
            TaskState::Queued => {
                return Err(task_error(
                    "TASK_BUSY",
                    "task is still queued for its first turn",
                ));
            }
            TaskState::Closed | TaskState::Abandoned | TaskState::Lost => {
                return Err(task_error(
                    "TASK_CLOSED",
                    "closed task cannot accept a follow-up turn",
                ));
            }
        }

        if current.worker() != Some(worker) {
            return Err(task_error(
                "TASK_TURN_CONFLICT",
                "resumed turn worker does not match the task session worker",
            ));
        }

        let binding = self.session(project_id, task_id)?.ok_or_else(|| {
            task_error(
                "SESSION_UNBOUND",
                "resumed turn requires a bound agent session",
            )
        })?;
        if binding.agent() != meta.agent() {
            return Err(task_error(
                "TASK_SESSION_INVALID",
                "bound session belongs to another agent",
            ));
        }

        let workspace = self.open_workspace(&task)?;
        let branch = self
            .git_workspace_query(workspace.path(), vec!["rev-parse", "--abbrev-ref", "HEAD"])
            .map_err(|error| task_error("WORKTREE_INCONSISTENT", error))?;
        let head = self
            .git_workspace_query(workspace.path(), vec!["rev-parse", "HEAD"])
            .map_err(|error| task_error("WORKTREE_INCONSISTENT", error))?;
        let expected_head = current
            .head_oid()
            .unwrap_or_else(|| meta.base_oid())
            .as_str();
        if branch != format!("task/{task_id}") || head != expected_head || base_oid.as_str() != head
        {
            return Err(task_error(
                "WORKTREE_INCONSISTENT",
                "task workspace branch or head does not match the resumable task",
            ));
        }
        let clean = self
            .git_workspace_query(
                workspace.path(),
                vec!["status", "--porcelain=v1", "--untracked-files=all"],
            )
            .map_err(|error| task_error("WORKTREE_INCONSISTENT", error))?;
        if !clean.is_empty() {
            return Err(task_error(
                "WORKTREE_INCONSISTENT",
                "task workspace contains local changes before resume",
            ));
        }

        let expected_turn_number = u32::try_from(current.turns().len())
            .ok()
            .and_then(|length| length.checked_add(1))
            .ok_or_else(|| task_error("TASK_INCONSISTENT", "turn history is too long"))?;
        if turn_number != expected_turn_number {
            return Err(task_error(
                "REQUEST_CONFLICT",
                "resumed turn number does not follow task history",
            ));
        }
        let pending = TurnSummary::new(
            turn_number,
            turn_id,
            None,
            None,
            None,
            false,
            Some(now_millis()?),
            None,
        );
        let next = TaskStatus::new(
            TaskState::Active,
            current.last_outcome().cloned(),
            Some(worker.to_owned()),
            true,
            Some(head.parse::<BaseOid>().map_err(|_| {
                task_error(
                    "WORKTREE_INCONSISTENT",
                    "task workspace head is not a valid commit ID",
                )
            })?),
            current.summary().map(str::to_owned),
            current.questions().to_vec(),
            current.files_changed().to_vec(),
            current.diff_stat().map(str::to_owned),
            current.turns().iter().cloned().chain([pending]).collect(),
            now_millis()?,
        )?;
        replace_status_bytes(&task, current, next.clone())?;
        task.sync_root()?;
        Ok((meta, next))
    }

    pub fn session_info(
        &self,
        request: &TaskSessionRequest,
    ) -> Result<TaskSessionResponse, WorkerError> {
        request.validate()?;
        let binding = self
            .session(request.project_id(), request.task_id())?
            .ok_or_else(|| task_error("SESSION_UNBOUND", "task has no bound agent session"))?;
        Ok(TaskSessionResponse::new(binding))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn finish_turn(
        &self,
        project_id: &str,
        task_id: TaskId,
        turn_id: JobId,
        terminal: TurnTerminal,
        outcome: TaskOutcome,
        agent_committed: bool,
        log_truncated: bool,
        head_oid: Option<BaseOid>,
        summary: Option<String>,
        questions: Vec<String>,
        files_changed: Vec<String>,
        diff_stat: Option<String>,
        close: bool,
    ) -> Result<TaskStatus, WorkerError> {
        let task = self.open_existing_task(project_id, task_id)?;
        let current = self.read_status(&task)?;
        if let Some(last) = current.turns().last()
            && last.turn_id() == turn_id
            && last.terminal().is_some()
        {
            return Ok(current);
        }
        let Some(last) = current.turns().last() else {
            return Err(task_error(
                "TASK_INCONSISTENT",
                "turn completion has no prepared turn record",
            ));
        };
        if last.turn_id() != turn_id || last.terminal().is_some() {
            return Err(task_error(
                "TASK_BUSY",
                "turn completion does not match the active turn",
            ));
        }
        let ended_at = now_millis()?;
        let mut turns = current.turns().to_vec();
        let replacement = TurnSummary::new(
            last.turn_number(),
            turn_id,
            Some(terminal),
            Some(outcome.clone()),
            Some(agent_committed),
            log_truncated,
            last.started_at_millis().or(Some(ended_at)),
            Some(ended_at),
        );
        let _ = turns.pop();
        turns.push(replacement);
        let next_state = if close {
            TaskState::Closed
        } else {
            TaskState::Open
        };
        let next = TaskStatus::new(
            next_state,
            Some(outcome),
            current.worker().map(str::to_owned),
            current.session_present(),
            head_oid.or_else(|| current.head_oid().cloned()),
            summary.or_else(|| current.summary().map(str::to_owned)),
            if questions.is_empty() {
                current.questions().to_vec()
            } else {
                questions
            },
            if files_changed.is_empty() {
                current.files_changed().to_vec()
            } else {
                files_changed
            },
            diff_stat.or_else(|| current.diff_stat().map(str::to_owned)),
            turns,
            ended_at,
        )?;
        replace_status_bytes(&task, current, next)
    }

    /// Completes a pending turn when its execution payload is too corrupt to
    /// recover its `TurnSection`. The durable task status is the independent
    /// locator: it is written before the turn job is accepted and binds the
    /// active turn to the globally unique job ID.
    pub(crate) fn finish_unrecoverable_active_turn(
        &self,
        project_id: &str,
        turn_id: JobId,
    ) -> Result<bool, WorkerError> {
        validate_project_id(project_id)?;
        let tasks = match self
            .store
            .open_directory(&format!("tasks/{project_id}"), false)
        {
            Ok(tasks) => tasks,
            Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        let mut matching_task = None;
        for raw_name in tasks.list_names()? {
            // RootedDir owns this private operation namespace beside task
            // directories; it is not a task record.
            if raw_name.as_slice() == b".mac-worker-rooted-fs" {
                continue;
            }
            let name = std::str::from_utf8(&raw_name).map_err(|_| {
                task_error(
                    "TASK_INCONSISTENT",
                    "task directory name is not valid UTF-8",
                )
            })?;
            let task_id = name.parse::<TaskId>().map_err(|_| {
                task_error("TASK_INCONSISTENT", "task directory name is not a task ID")
            })?;
            let task = self.open_existing_task(project_id, task_id)?;
            let status = self.read_status(&task)?;
            let pending = status.state() == TaskState::Active
                && status
                    .turns()
                    .last()
                    .is_some_and(|turn| turn.turn_id() == turn_id && turn.terminal().is_none());
            if pending && matching_task.replace(task_id).is_some() {
                return Err(task_error(
                    "TASK_INCONSISTENT",
                    "multiple active tasks reference the same turn job",
                ));
            }
        }
        let Some(task_id) = matching_task else {
            return Ok(false);
        };
        self.finish_turn(
            project_id,
            task_id,
            turn_id,
            TurnTerminal::Lost,
            TaskOutcome::Lost,
            false,
            false,
            None,
            None,
            Vec::new(),
            Vec::new(),
            None,
            false,
        )?;
        Ok(true)
    }

    pub fn bind_session(
        &self,
        project_id: &str,
        task_id: TaskId,
        binding: SessionBinding,
    ) -> Result<(), WorkerError> {
        validate_project_id(project_id)?;
        binding.validate()?;
        let task = self.open_existing_task(project_id, task_id)?;
        if task.entry_exists("session.json")? {
            let existing: SessionBinding = read_record(&task, "session.json")?;
            if existing.agent() != binding.agent()
                || existing.session_ref() != binding.session_ref()
            {
                return Err(task_error(
                    "TASK_SESSION_CONFLICT",
                    "task already has a different session binding",
                ));
            }
        } else {
            write_record_once(&task, "session.json", &binding)?;
        }
        let status = self.read_status(&task)?;
        if !status.session_present() {
            let state = status.state();
            let _ = replace_status_record(&task, status, state, Some(true))?;
        }
        task.sync_root()?;
        Ok(())
    }

    fn delete_native_session(&self, project_id: &str, task_id: TaskId) -> Result<(), WorkerError> {
        let Some(binding) = self.session(project_id, task_id)? else {
            return Ok(());
        };
        let Some(argv) =
            crate::agent::adapter_for(binding.agent()).delete_session(binding.session_ref())
        else {
            return Ok(());
        };
        if self.session_referenced_elsewhere(project_id, task_id, &binding)? {
            return Ok(());
        }
        let home = std::env::var_os("HOME")
            .map(std::path::PathBuf::from)
            .unwrap_or_default();
        let request = crate::agent::prebind_login_request(&argv, &home, &[])?;
        let result = self.runner.run(&request)?;
        if !result.status.success() {
            return Err(WorkerError::Agent {
                code: "AGENT_SESSION_DELETE_FAILED",
                message: "native session deletion command failed".into(),
            });
        }
        Ok(())
    }

    fn session_referenced_elsewhere(
        &self,
        project_id: &str,
        task_id: TaskId,
        binding: &SessionBinding,
    ) -> Result<bool, WorkerError> {
        let tasks = match self.store.open_directory("tasks", false) {
            Ok(tasks) => tasks,
            Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        for raw_project in tasks.list_names()? {
            if raw_project.as_slice() == b".mac-worker-rooted-fs" {
                continue;
            }
            let other_project = std::str::from_utf8(&raw_project).map_err(|_| {
                task_error(
                    "TASK_INCONSISTENT",
                    "task project directory name is not valid UTF-8",
                )
            })?;
            validate_project_id(other_project)?;
            let project_tasks = self
                .store
                .open_directory(&format!("tasks/{other_project}"), false)?;
            for raw_task in project_tasks.list_names()? {
                if raw_task.as_slice() == b".mac-worker-rooted-fs" {
                    continue;
                }
                let other_task_id = std::str::from_utf8(&raw_task)
                    .map_err(|_| {
                        task_error(
                            "TASK_INCONSISTENT",
                            "task directory name is not valid UTF-8",
                        )
                    })?
                    .parse::<TaskId>()
                    .map_err(|_| {
                        task_error("TASK_INCONSISTENT", "task directory name is not a task ID")
                    })?;
                if other_project == project_id && other_task_id == task_id {
                    continue;
                }
                let other_task =
                    self.store
                        .open_task_directory(other_project, other_task_id, false)?;
                if !other_task.entry_exists("session.json")? {
                    continue;
                }
                let other_binding: SessionBinding = read_record(&other_task, "session.json")?;
                if other_binding.agent() == binding.agent()
                    && other_binding.session_ref() == binding.session_ref()
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub fn session(
        &self,
        project_id: &str,
        task_id: TaskId,
    ) -> Result<Option<SessionBinding>, WorkerError> {
        validate_project_id(project_id)?;
        let task = self.open_existing_task(project_id, task_id)?;
        if !task.entry_exists("session.json")? {
            return Ok(None);
        }
        read_record(&task, "session.json").map(Some)
    }

    #[allow(dead_code)]
    pub(crate) fn replace_status_after(
        &self,
        project_id: &str,
        task_id: TaskId,
        update: impl FnOnce(TaskStatus) -> Result<TaskStatus, WorkerError>,
    ) -> Result<TaskStatus, WorkerError> {
        validate_project_id(project_id)?;
        let task = self.open_existing_task(project_id, task_id)?;
        let current = self.read_status(&task)?;
        let next = update(current.clone())?;
        replace_status_bytes(&task, current, next)
    }

    fn verify_base(&self, mirror: &RootedDir, base: &BaseOid) -> Result<(), WorkerError> {
        let result = self
            .runner
            .run(&git_request(
                vec![
                    OsString::from("--git-dir"),
                    mirror.path().as_os_str().to_os_string(),
                    OsString::from("cat-file"),
                    OsString::from("-t"),
                    base.to_string().into(),
                ],
                None,
            ))
            .map_err(|error| git_error("BASE_UNAVAILABLE", error.to_string()))?;
        if !result.status.success() || String::from_utf8_lossy(&result.stdout).trim() != "commit" {
            return Err(git_error("BASE_UNAVAILABLE", "base object is not a commit"));
        }
        Ok(())
    }

    fn ensure_meta(&self, task: &RootedDir, expected: &TaskMeta) -> Result<(), WorkerError> {
        if task.entry_exists("meta.json")? {
            let actual: TaskMeta = read_record(task, "meta.json")?;
            if actual != *expected {
                return Err(task_error(
                    "WORKTREE_INCONSISTENT",
                    "task metadata does not match the prepare request",
                ));
            }
            return Ok(());
        }
        write_record_once(task, "meta.json", expected)
    }

    fn prepare_workspace(
        &self,
        task: &RootedDir,
        mirror: &RootedDir,
        meta: &TaskMeta,
    ) -> Result<(bool, RootedDir), WorkerError> {
        let branch = format!("task/{}", meta.task_id());
        if task.entry_exists("workspace")? {
            task.validate_private_entry("workspace")
                .map_err(|error| task_error("WORKTREE_INCONSISTENT", error.to_string()))?;
            let workspace = task
                .open_child_directory(&workspace_relative()?, false)
                .map_err(|error| task_error("WORKTREE_INCONSISTENT", error.to_string()))?;
            let branch_result = self
                .git_workspace_query(workspace.path(), vec!["rev-parse", "--abbrev-ref", "HEAD"]);
            let head_result = self.git_workspace_query(workspace.path(), vec!["rev-parse", "HEAD"]);
            match (branch_result, head_result) {
                (Ok(actual_branch), Ok(actual_head))
                    if actual_branch == branch && actual_head == meta.base_oid().as_str() =>
                {
                    let clean = self.git_workspace_query(
                        workspace.path(),
                        vec!["status", "--porcelain=v1", "--untracked-files=all"],
                    );
                    return match clean {
                        Ok(output) if output.is_empty() => Ok((true, workspace)),
                        Ok(_) => Err(task_error(
                            "WORKTREE_INCONSISTENT",
                            "task workspace contains local changes",
                        )),
                        Err(error) => Err(task_error("WORKTREE_INCONSISTENT", error)),
                    };
                }
                (Ok(_), Ok(_)) => {
                    return Err(task_error(
                        "WORKTREE_INCONSISTENT",
                        "task workspace branch or base does not match",
                    ));
                }
                _ => {
                    self.store.remove_owned_child_committed(task, "workspace")?;
                }
            }
        }

        let workspace = task.create_new_child_directory("workspace")?;
        let clone = self.runner.run(&git_request(
            vec![
                OsString::from("clone"),
                OsString::from("--shared"),
                OsString::from("--no-checkout"),
                mirror.path().as_os_str().to_os_string(),
                workspace.path().as_os_str().to_os_string(),
            ],
            None,
        ));
        let clone = match clone {
            Ok(clone) => clone,
            Err(error) => {
                let _ = self.store.remove_owned_child_committed(task, "workspace");
                return Err(task_error("WORKTREE_CREATE_FAILED", error.to_string()));
            }
        };
        if !clone.status.success() {
            let _ = self.store.remove_owned_child_committed(task, "workspace");
            return Err(task_error(
                "WORKTREE_CREATE_FAILED",
                String::from_utf8_lossy(&clone.stderr).trim().to_owned(),
            ));
        }
        let checkout = self.runner.run(&git_request(
            vec![
                OsString::from("-C"),
                workspace.path().as_os_str().to_os_string(),
                OsString::from("checkout"),
                OsString::from("-b"),
                branch.into(),
                meta.base_oid().to_string().into(),
            ],
            None,
        ));
        let checkout = match checkout {
            Ok(checkout) => checkout,
            Err(error) => {
                let _ = self.store.remove_owned_child_committed(task, "workspace");
                return Err(task_error("WORKTREE_CREATE_FAILED", error.to_string()));
            }
        };
        if !checkout.status.success() {
            let _ = self.store.remove_owned_child_committed(task, "workspace");
            return Err(task_error(
                "WORKTREE_CREATE_FAILED",
                String::from_utf8_lossy(&checkout.stderr).trim().to_owned(),
            ));
        }
        let workspace = task.open_child_directory(&workspace_relative()?, false)?;
        Ok((false, workspace))
    }

    fn ensure_active_status(
        &self,
        task: &RootedDir,
        meta: &TaskMeta,
        worker: &str,
        turn_id: JobId,
    ) -> Result<(), WorkerError> {
        if task.entry_exists("status.json")? {
            let status: TaskStatus = read_record(task, "status.json")?;
            if matches!(status.state(), TaskState::Closed | TaskState::Abandoned) {
                return Err(task_error(
                    "WORKTREE_INCONSISTENT",
                    "task is already terminal",
                ));
            }
            if status.state() == TaskState::Active
                && let Some(last) = status.turns().last()
                && last.terminal().is_none()
                && last.turn_id() != turn_id
            {
                return Err(task_error("TASK_BUSY", "task already has an active turn"));
            }
            if status.state() == TaskState::Active
                && status
                    .turns()
                    .last()
                    .is_some_and(|turn| turn.turn_id() == turn_id && turn.terminal().is_none())
            {
                return Ok(());
            }
            let turn_number = u32::try_from(status.turns().len())
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| task_error("TASK_INCONSISTENT", "turn history is too long"))?;
            let pending =
                TurnSummary::new(turn_number, turn_id, None, None, None, false, None, None);
            let next = TaskStatus::new(
                TaskState::Active,
                status.last_outcome().cloned(),
                Some(worker.to_owned()),
                status.session_present(),
                status.head_oid().cloned(),
                status.summary().map(str::to_owned),
                status.questions().to_vec(),
                status.files_changed().to_vec(),
                status.diff_stat().map(str::to_owned),
                status.turns().iter().cloned().chain([pending]).collect(),
                status.updated_at_millis(),
            )?;
            replace_status_bytes(task, status, next)?;
            return Ok(());
        }
        let status = TaskStatus::new(
            TaskState::Active,
            None,
            Some(worker.to_owned()),
            false,
            Some(meta.base_oid().clone()),
            None,
            Vec::new(),
            Vec::new(),
            None,
            vec![TurnSummary::new(
                1,
                turn_id,
                None,
                None,
                None,
                false,
                Some(meta.created_at_millis()),
                None,
            )],
            meta.created_at_millis(),
        )?;
        write_record_once(task, "status.json", &status)
    }

    fn git_workspace_query(
        &self,
        workspace: &Path,
        arguments: Vec<&str>,
    ) -> Result<String, String> {
        let mut args = vec![OsString::from("-C"), workspace.as_os_str().to_os_string()];
        args.extend(arguments.into_iter().map(OsString::from));
        let result = self
            .runner
            .run(&git_request(args, None))
            .map_err(|error| error.to_string())?;
        if !result.status.success() {
            return Err(String::from_utf8_lossy(&result.stderr).trim().to_owned());
        }
        Ok(String::from_utf8_lossy(&result.stdout).trim().to_owned())
    }

    fn git_index_path(&self, workspace: &Path) -> Result<PathBuf, WorkerError> {
        let result = self
            .runner
            .run(&git_request(
                vec![
                    OsString::from("-C"),
                    workspace.as_os_str().to_os_string(),
                    OsString::from("rev-parse"),
                    OsString::from("--git-path"),
                    OsString::from("index"),
                ],
                None,
            ))
            .map_err(|error| task_error("DIFF_FAILED", error.to_string()))?;
        if !result.status.success() {
            return Err(task_error("DIFF_FAILED", "could not locate Git index"));
        }
        let value = String::from_utf8_lossy(&result.stdout).trim().to_owned();
        let path = PathBuf::from(value);
        Ok(if path.is_absolute() {
            path
        } else {
            workspace.join(path)
        })
    }

    fn delete_ref(&self, mirror: &RootedDir, reference: &str) -> Result<(), WorkerError> {
        let result = self
            .runner
            .run(&git_request(
                vec![
                    OsString::from("--git-dir"),
                    mirror.path().as_os_str().to_os_string(),
                    OsString::from("update-ref"),
                    OsString::from("-d"),
                    reference.into(),
                ],
                None,
            ))
            .map_err(|error| git_error("REF_UPDATE_FAILED", error.to_string()))?;
        if !result.status.success() {
            return Err(git_error(
                "REF_UPDATE_FAILED",
                String::from_utf8_lossy(&result.stderr).trim().to_owned(),
            ));
        }
        Ok(())
    }

    fn open_existing_task(
        &self,
        project_id: &str,
        task_id: TaskId,
    ) -> Result<RootedDir, WorkerError> {
        validate_project_id(project_id)?;
        self.store
            .open_task_directory(project_id, task_id, false)
            .map_err(map_task_not_found)
    }

    fn open_workspace(&self, task: &RootedDir) -> Result<RootedDir, WorkerError> {
        if !task.entry_exists("workspace")? {
            return Err(task_not_found());
        }
        task.validate_private_entry("workspace")
            .map_err(|error| task_error("WORKTREE_INCONSISTENT", error.to_string()))?;
        task.open_child_directory(&workspace_relative()?, false)
            .map_err(|error| task_error("WORKTREE_INCONSISTENT", error.to_string()))
    }

    fn read_meta(&self, task: &RootedDir) -> Result<TaskMeta, WorkerError> {
        read_record(task, "meta.json").map_err(map_task_not_found)
    }

    fn read_status(&self, task: &RootedDir) -> Result<TaskStatus, WorkerError> {
        read_record(task, "status.json").map_err(map_task_not_found)
    }
}

fn now_millis() -> Result<u64, WorkerError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| task_error("TASK_CLOCK_INVALID", "system clock precedes the Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| task_error("TASK_CLOCK_INVALID", "system clock is outside the range"))
}

fn git_request(
    args: Vec<OsString>,
    extra_environment: Option<(OsString, OsString)>,
) -> ProcessRequest {
    let mut environment = vec![
        (GIT_CONFIG_GLOBAL.into(), "/dev/null".into()),
        (GIT_CONFIG_NOSYSTEM.into(), "1".into()),
        (GIT_TERMINAL_PROMPT.into(), "0".into()),
    ];
    let extra_name = extra_environment.as_ref().map(|(name, _)| name);
    let environment_remove = GIT_ENVIRONMENT_REMOVALS
        .iter()
        .filter(|name| extra_name.is_none_or(|extra| extra != &OsString::from(**name)))
        .map(|name| OsString::from(*name))
        .collect();
    if let Some(extra) = extra_environment {
        environment.push(extra);
    }
    ProcessRequest {
        program: GIT_PROGRAM.into(),
        args,
        environment,
        environment_remove,
        stdin: None,
        policy: ProcessPolicy {
            stdout_limit: GIT_STDOUT_LIMIT,
            stderr_limit: GIT_STDERR_LIMIT,
            deadline: GIT_DEADLINE,
        },
    }
}

fn read_record<T: DeserializeOwned + Serialize>(
    directory: &RootedDir,
    name: &str,
) -> Result<T, WorkerError> {
    let bytes = directory.read_private_regular(name, MAX_TASK_RECORD_BYTES)?;
    let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
    let value = T::deserialize(&mut deserializer)
        .map_err(|error| WorkerError::Protocol(format!("invalid task JSON: {error}")))?;
    deserializer
        .end()
        .map_err(|error| WorkerError::Protocol(format!("trailing task JSON data: {error}")))?;
    let canonical = serde_json::to_vec(&value).map_err(|error| {
        WorkerError::Protocol(format!("failed to canonicalize task JSON: {error}"))
    })?;
    if canonical != bytes {
        return Err(WorkerError::Protocol("task JSON is not canonical".into()));
    }
    Ok(value)
}

fn write_record_once<T: Serialize + DeserializeOwned + PartialEq>(
    directory: &RootedDir,
    name: &str,
    value: &T,
) -> Result<(), WorkerError> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        WorkerError::Protocol(format!("failed to serialize task JSON: {error}"))
    })?;
    if bytes.len() as u64 > MAX_TASK_RECORD_BYTES {
        return Err(WorkerError::Protocol("task JSON exceeds 1 MiB".into()));
    }
    match directory.write_private_atomic_no_replace(name, &bytes) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let existing: T = read_record(directory, name)?;
            if existing == *value {
                Ok(())
            } else {
                Err(task_error(
                    "WORKTREE_INCONSISTENT",
                    format!("{name} already exists with different content"),
                ))
            }
        }
        Err(error) => Err(WorkerError::Io(error)),
    }
}

fn replace_status_record(
    directory: &RootedDir,
    current: TaskStatus,
    state: TaskState,
    session_present: Option<bool>,
) -> Result<TaskStatus, WorkerError> {
    replace_status_record_at(
        directory,
        current.clone(),
        state,
        session_present,
        current.updated_at_millis(),
    )
}

fn replace_status_record_at(
    directory: &RootedDir,
    current: TaskStatus,
    state: TaskState,
    session_present: Option<bool>,
    updated_at_millis: u64,
) -> Result<TaskStatus, WorkerError> {
    let next = TaskStatus::new(
        state,
        current.last_outcome().cloned(),
        current.worker().map(str::to_owned),
        session_present.unwrap_or_else(|| current.session_present()),
        current.head_oid().cloned(),
        current.summary().map(str::to_owned),
        current.questions().to_vec(),
        current.files_changed().to_vec(),
        current.diff_stat().map(str::to_owned),
        current.turns().to_vec(),
        updated_at_millis,
    )?;
    replace_status_bytes(directory, current, next.clone())?;
    Ok(next)
}

fn replace_status_bytes(
    directory: &RootedDir,
    current: TaskStatus,
    next: TaskStatus,
) -> Result<TaskStatus, WorkerError> {
    let old_bytes = serde_json::to_vec(&current).map_err(|error| {
        WorkerError::Protocol(format!("failed to serialize task status: {error}"))
    })?;
    let new_bytes = serde_json::to_vec(&next).map_err(|error| {
        WorkerError::Protocol(format!("failed to serialize task status: {error}"))
    })?;
    directory.replace_private_regular_exact("status.json", &old_bytes, &new_bytes)?;
    directory.sync_root()?;
    Ok(next)
}

fn remove_temporary_index(task: &RootedDir, name: &str) -> io::Result<()> {
    let _ = task.set_private_regular_mode(name, 0o600);
    task.remove_owned_regular(name)
}

fn bound_diff(bytes: &[u8]) -> Result<(String, bool), WorkerError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| task_error("DIFF_FAILED", "Git diff output is not UTF-8"))?;
    let mut escaped_length = 2_usize;
    let mut bounded = String::new();
    let mut truncated = false;
    for character in text.chars() {
        let length = json_escaped_char_len(character);
        if escaped_length.saturating_add(length) > MAX_DIFF_BYTES {
            truncated = true;
            break;
        }
        escaped_length += length;
        bounded.push(character);
    }
    Ok((bounded, truncated))
}

fn json_escaped_char_len(character: char) -> usize {
    match character {
        '"' | '\\' | '\u{08}' | '\u{0c}' | '\n' | '\r' | '\t' => 2,
        character if character <= '\u{1f}' => 6,
        character => character.len_utf8(),
    }
}

fn map_task_not_found(error: WorkerError) -> WorkerError {
    match error {
        WorkerError::Io(error) if error.kind() == io::ErrorKind::NotFound => task_not_found(),
        other => other,
    }
}

fn task_not_found() -> WorkerError {
    task_error("TASK_NOT_FOUND", "task metadata or workspace is absent")
}

fn task_error(code: &'static str, message: impl Into<String>) -> WorkerError {
    WorkerError::Task {
        code,
        message: message.into(),
    }
}

fn push_close_warning(warnings: &mut Vec<String>, warning: &str) {
    if warnings.len() < MAX_CLOSE_WARNINGS
        && !warning.is_empty()
        && warning.len() <= MAX_CLOSE_WARNING_BYTES
        && !warning.chars().any(char::is_control)
        && !warnings.iter().any(|existing| existing == warning)
    {
        warnings.push(warning.to_owned());
    }
}

fn git_error(code: &'static str, message: impl Into<String>) -> WorkerError {
    WorkerError::Git {
        code,
        message: message.into(),
    }
}

fn ensure_protocol(version: u32) -> Result<(), WorkerError> {
    if version == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(WorkerError::Protocol(format!(
            "INCOMPATIBLE_PROTOCOL: task protocol version {version} is unsupported"
        )))
    }
}

fn validate_project_id(value: &str) -> Result<(), WorkerError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        Ok(())
    } else {
        Err(WorkerError::Protocol(
            "INVALID_COMPONENT: project ID is not a lowercase SHA-256 component".into(),
        ))
    }
}

fn workspace_relative() -> Result<crate::inputs::RelativePath, WorkerError> {
    crate::inputs::RelativePath::parse(b"workspace").map_err(|error| {
        WorkerError::Protocol(format!("invalid task workspace component: {error:?}"))
    })
}

fn validate_worker_name(value: &str) -> Result<(), WorkerError> {
    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        Err(task_error(
            "TASK_CONFIG_INVALID",
            "worker name is empty, too long, or contains a control character",
        ))
    } else {
        Ok(())
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
        _ => Err(task_error("TASK_SESSION_INVALID", "unknown agent kind")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        host_store::HostStore,
        job::JobId,
        process::SystemProcessRunner,
        task::{TaskId, TaskOutcome, TaskState, TurnSummary, TurnTerminal},
    };
    use tempfile::tempdir;
    use uuid::Uuid;

    const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn unrecoverable_payload_finishes_matching_active_turn_by_job_id() {
        let temp = tempdir().unwrap();
        let store = HostStore::open(&temp.path().join("host")).unwrap();
        let task_id = TaskId::new(Uuid::from_u128(1));
        let turn_id = JobId::new(Uuid::from_u128(2));
        let task = store
            .open_task_directory(PROJECT_ID, task_id, true)
            .unwrap();
        let initial = TaskStatus::new(
            TaskState::Active,
            None,
            Some("worker".into()),
            false,
            None,
            None,
            Vec::new(),
            Vec::new(),
            None,
            vec![TurnSummary::new(
                1, turn_id, None, None, None, false, None, None,
            )],
            1,
        )
        .unwrap();
        write_record_once(&task, "status.json", &initial).unwrap();

        assert!(
            TaskStore::new(&store, &SystemProcessRunner)
                .finish_unrecoverable_active_turn(PROJECT_ID, turn_id)
                .unwrap()
        );

        let status = TaskStore::new(&store, &SystemProcessRunner)
            .load_status(PROJECT_ID, task_id)
            .unwrap();
        assert_eq!(status.state(), TaskState::Open);
        assert_eq!(status.last_outcome(), Some(&TaskOutcome::Lost));
        assert_eq!(status.turns()[0].terminal(), Some(TurnTerminal::Lost));
    }
}
