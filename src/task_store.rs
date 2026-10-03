use std::{
    ffi::OsString,
    fs, io,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};

use crate::{
    agent::{AgentKind, Question},
    error::WorkerError,
    git_transport::GitTransport,
    host_store::{HostStore, TransferGuard},
    job::JobId,
    lease::{LeaseService, OccupiedSlot},
    outbox::OriginOutbox,
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
    protocol::PROTOCOL_VERSION,
    rooted_fs::RootedDir,
    session_transfer::{
        MAX_FILE_BYTES, MAX_PACKAGE_BYTES, MAX_PACKAGE_FILES, PACKAGE_MANIFEST_PATH, PackageFile,
        PlaceContext, SESSION_REF_PREFIX, SessionAgent, SessionImportMeta, SessionPackage,
        SessionPlace, imported_session_id,
        place::{fs::StoreWriter, place_for},
        store_root::store_root,
    },
    task::{
        BaseOid, HerdrTurnReport, OriginDelivery, TaskId, TaskMeta, TaskOutcome, TaskSource,
        TaskState, TaskStatus, TurnSummary, TurnTerminal,
    },
    turn::EnvProfile,
};

const GIT_PROGRAM: &str = "/usr/bin/git";
const GIT_STDOUT_LIMIT: usize = 2 * 1024 * 1024;
const GIT_STDERR_LIMIT: usize = 128 * 1024;
const GIT_DEADLINE: Duration = Duration::from_secs(15 * 60);
const MAX_TASK_RECORD_BYTES: u64 = 1024 * 1024;
const MAX_CLOSE_WARNINGS: usize = 8;
const MAX_CLOSE_WARNING_BYTES: usize = 256;
const NATIVE_SESSION_DELETE_WARNING: &str = "native agent session deletion failed";
const SESSION_IMPORT_RECEIPT: &str = "session-import.json";
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

    #[cfg(any(test, feature = "test-support"))]
    pub fn bound_at_millis(&self) -> u64 {
        self.bound_at_millis
    }

    fn validate(&self) -> Result<(), WorkerError> {
        if self.session_ref.is_empty()
            || self.session_ref.len() > 256
            || self.session_ref.starts_with('-')
            || self.session_ref.chars().any(char::is_control)
        {
            return Err(task_error(
                "TASK_SESSION_INVALID",
                "session reference is empty, too long, option-like, or contains a control character",
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

    #[cfg(any(test, feature = "test-support"))]
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

    #[cfg(any(test, feature = "test-support"))]
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

    #[cfg(any(test, feature = "test-support"))]
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

    #[cfg(any(test, feature = "test-support"))]
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

    #[cfg(any(test, feature = "test-support"))]
    pub fn head(&self) -> &BaseOid {
        &self.head
    }

    #[cfg(any(test, feature = "test-support"))]
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

    #[cfg(any(test, feature = "test-support"))]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivery: Option<OriginDelivery>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    deliveries: Vec<OriginDelivery>,
}

impl TaskStatusResponse {
    pub fn new(status: TaskStatus) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            status,
            delivery: None,
            deliveries: Vec::new(),
        }
    }

    pub fn with_deliveries(mut self, deliveries: Vec<OriginDelivery>) -> Self {
        self.delivery = deliveries.first().cloned();
        self.deliveries = deliveries;
        self
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn status(&self) -> &TaskStatus {
        &self.status
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn delivery(&self) -> Option<&OriginDelivery> {
        self.delivery.as_ref()
    }

    pub fn deliveries(&self) -> &[OriginDelivery] {
        &self.deliveries
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

    #[cfg(any(test, feature = "test-support"))]
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

    #[cfg(any(test, feature = "test-support"))]
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

    #[cfg(any(test, feature = "test-support"))]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivery: Option<OriginDelivery>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    deliveries: Vec<OriginDelivery>,
}

/// Filesystem result of a retention close, plus the task whose herdr tabs
/// should be removed after the caller drops the installation lock.
pub(crate) struct RetentionClose {
    /// Status written before herdr runs. Callers that only need the task id
    /// still receive it so a later reader can see the committed close.
    #[allow(dead_code)]
    pub status: TaskStatus,
    pub herdr_task: Option<TaskId>,
}

impl TaskCloseResponse {
    #[cfg(any(test, feature = "test-support"))]
    pub fn new(status: TaskStatus) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            status,
            warnings: Vec::new(),
            delivery: None,
            deliveries: Vec::new(),
        }
    }

    pub fn with_deliveries(mut self, deliveries: Vec<OriginDelivery>) -> Self {
        self.delivery = deliveries.first().cloned();
        self.deliveries = deliveries;
        self
    }

    pub(crate) fn with_warnings(status: TaskStatus, warnings: Vec<String>) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            status,
            warnings,
            delivery: None,
            deliveries: Vec::new(),
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

    #[cfg(any(test, feature = "test-support"))]
    pub fn delivery(&self) -> Option<&OriginDelivery> {
        self.delivery.as_ref()
    }

    pub fn deliveries(&self) -> &[OriginDelivery] {
        &self.deliveries
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
        self.prepare_with_placement(request, guard, None)
    }

    /// Test seam: exercise the prepare transaction with deterministic placement,
    /// while retaining the production package reader and real StoreWriter.
    #[cfg(feature = "test-support")]
    pub fn prepare_with_session_place(
        &self,
        request: &TaskPrepareRequest,
        guard: &TransferGuard,
        place: &dyn SessionPlace,
    ) -> Result<TaskPrepareResponse, WorkerError> {
        self.prepare_with_placement(request, guard, Some(place))
    }

    fn prepare_with_placement(
        &self,
        request: &TaskPrepareRequest,
        guard: &TransferGuard,
        place: Option<&dyn SessionPlace>,
    ) -> Result<TaskPrepareResponse, WorkerError> {
        request.validate()?;
        guard.validate()?;
        if guard.job_id() != request.job_id() {
            return Err(task_error(
                "TASK_LEASE_MISMATCH",
                "task preparation requires the matching transfer guard",
            ));
        }
        let bound = LeaseService::new(self.store)
            .occupied_slot_for_job(request.job_id())?
            .ok_or_else(|| {
                task_error(
                    "TASK_LEASE_MISMATCH",
                    "task preparation requires a live lease for the job",
                )
            })?;
        let meta = request.meta();
        let task_id = meta.task_id();
        require_task_execution_lease(
            &bound,
            meta.project_id(),
            task_id,
            request.job_id(),
            request.worker(),
            Some(meta.worktree_id()),
        )?;
        // This is intentionally the first filesystem lookup under `tasks/`:
        // a missing or invalid base must not leave a task directory behind.
        let mirror = match meta.source() {
            TaskSource::Origin { url } => {
                let mirror = self.store.mirror(meta.project_id())?;
                GitTransport::new(self.runner).fetch_origin(url, meta.base_oid(), &mirror)?;
                self.pin_base_ref(&mirror, task_id, meta.base_oid())?;
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

        let (reused, workspace) = self.prepare_workspace(&task, &mirror, meta)?;
        self.ensure_active_status(&task, meta, request.worker(), request.job_id())?;
        if meta.session_import().is_some() {
            self.import_session(&task, &mirror, &workspace, meta, place)
                .map_err(|_| session_placement_failed())?;
        }
        task.sync_root()?;
        Ok(TaskPrepareResponse::new(meta.base_oid().clone(), reused))
    }

    fn import_session(
        &self,
        task: &RootedDir,
        mirror: &RootedDir,
        workspace: &RootedDir,
        meta: &TaskMeta,
        place: Option<&dyn SessionPlace>,
    ) -> Result<(), WorkerError> {
        let import = meta.session_import().ok_or_else(session_placement_failed)?;
        let session_id = imported_session_id(&meta.task_id());
        let reference = format!("{SESSION_REF_PREFIX}{}", meta.task_id());
        let existing: Option<SessionImportReceipt> = if task.entry_exists(SESSION_IMPORT_RECEIPT)? {
            let receipt: SessionImportReceipt = read_record(task, SESSION_IMPORT_RECEIPT)?;
            receipt.validate()?;
            if receipt.package_oid != import.package_oid()
                || receipt.session_id != session_id
                || receipt.agent != import.agent()
            {
                return Err(session_placement_failed());
            }
            Some(receipt)
        } else {
            None
        };
        // Completed imports may have been appended by the agent. Do not even
        // open the native store or reload a changed env profile on this path.
        if let Some(receipt) = &existing
            && receipt.stage == SessionImportStage::Complete
        {
            self.bind_session(
                meta.project_id(),
                meta.task_id(),
                SessionBinding::new(import.agent().agent_kind(), &session_id, now_millis()?)?,
            )?;
            return self.delete_session_ref(mirror, &reference, import.package_oid());
        }

        let (home, profile) = task_account_profile(meta)?;
        let root = physical_store_root(&store_root(
            import.agent(),
            &home,
            &profile_strings(&profile),
        )?)?;
        let root_text = root.to_str().ok_or_else(session_placement_failed)?;
        let planned = match existing {
            Some(receipt) if receipt.store_root == root_text => receipt,
            Some(_) => return Err(session_placement_failed()),
            None => {
                let receipt = SessionImportReceipt {
                    schema: 1,
                    stage: SessionImportStage::Planned,
                    package_oid: import.package_oid().to_owned(),
                    session_id: session_id.clone(),
                    agent: import.agent(),
                    placed_at_millis: now_millis()?,
                    store_root: root_text.to_owned(),
                    primary_relative: String::new(),
                    files: Vec::new(),
                };
                receipt.validate()?;
                write_record_once(task, SESSION_IMPORT_RECEIPT, &receipt)?;
                receipt
            }
        };
        let package = self.read_session_package(mirror, &reference, import)?;
        // Native stores may not exist yet (in particular Claude on fresh pool
        // accounts). Rooted creation makes new components private, without
        // relaxing StoreWriter's no-follow traversal below the store root.
        match fs::symlink_metadata(&root) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let created = RootedDir::create(&root)?;
                created.sync_root()?;
            }
            Err(error) => return Err(error.into()),
        }
        let store = StoreWriter::open(&root)?;
        let adapter;
        let place = match place {
            Some(place) => place,
            None => {
                adapter = place_for(import.agent());
                adapter.as_ref()
            }
        };
        if place.agent() != import.agent() {
            return Err(session_placement_failed());
        }
        let physical_workspace = workspace.path().canonicalize()?;
        let placed = place.place(
            &package,
            &PlaceContext {
                workspace: &physical_workspace,
                store: &store,
                session_id: &session_id,
                placed_at_millis: planned.placed_at_millis,
            },
        )?;
        self.bind_session(
            meta.project_id(),
            meta.task_id(),
            SessionBinding::new(import.agent().agent_kind(), &session_id, now_millis()?)?,
        )?;
        let complete = SessionImportReceipt {
            stage: SessionImportStage::Complete,
            primary_relative: placed.primary_relative,
            files: placed.files,
            ..planned.clone()
        };
        complete.validate()?;
        let old_bytes = serde_json::to_vec(&planned).map_err(|_| session_placement_failed())?;
        let new_bytes = serde_json::to_vec(&complete).map_err(|_| session_placement_failed())?;
        if new_bytes.len() as u64 > MAX_TASK_RECORD_BYTES {
            return Err(session_placement_failed());
        }
        task.replace_private_regular_exact(SESSION_IMPORT_RECEIPT, &old_bytes, &new_bytes)?;
        task.sync_root()?;
        self.delete_session_ref(mirror, &reference, import.package_oid())
    }

    fn session_git(
        &self,
        mirror: &RootedDir,
        args: &[&str],
        stdout_limit: usize,
    ) -> Result<Vec<u8>, WorkerError> {
        mirror.verify_bound()?;
        let mut argv = vec![
            OsString::from("--git-dir"),
            mirror.path().as_os_str().to_owned(),
        ];
        argv.extend(args.iter().map(OsString::from));
        let mut request = git_request(argv, None);
        request.policy.stdout_limit = stdout_limit;
        let result = self.runner.run(&request)?;
        mirror.verify_bound()?;
        if !result.status.success() {
            return Err(session_placement_failed());
        }
        Ok(result.stdout)
    }

    fn read_session_package(
        &self,
        mirror: &RootedDir,
        reference: &str,
        import: &SessionImportMeta,
    ) -> Result<SessionPackage, WorkerError> {
        let oid = self.session_git(
            mirror,
            &["rev-parse", "--verify", "--end-of-options", reference],
            128,
        )?;
        if std::str::from_utf8(&oid).ok().map(str::trim) != Some(import.package_oid()) {
            return Err(session_placement_failed());
        }
        let kind = self.session_git(mirror, &["cat-file", "-t", import.package_oid()], 32)?;
        if kind != b"commit\n" {
            return Err(session_placement_failed());
        }
        let tree = self.session_git(
            mirror,
            &["ls-tree", "-r", "-z", "-l", import.package_oid()],
            GIT_STDOUT_LIMIT,
        )?;
        let mut entries = Vec::new();
        let mut total = 0u64;
        let mut manifest_seen = false;
        if tree.last() != Some(&0) {
            return Err(session_placement_failed());
        }
        for entry in tree
            .split(|byte| *byte == 0)
            .filter(|entry| !entry.is_empty())
        {
            let text = std::str::from_utf8(entry).map_err(|_| session_placement_failed())?;
            let (header, path) = text.split_once('\t').ok_or_else(session_placement_failed)?;
            let columns: Vec<_> = header.split_ascii_whitespace().collect();
            if columns.len() != 4
                || !matches!(columns[0], "100644" | "100755")
                || columns[1] != "blob"
            {
                return Err(session_placement_failed());
            }
            // Validate blob OIDs before ever passing them as git arguments.
            SessionImportMeta::new(import.agent(), columns[2], "1")?;
            let size = columns[3]
                .parse::<u64>()
                .map_err(|_| session_placement_failed())?;
            // SessionPackage caps native bytes; manifest metadata has its
            // own per-file cap and is not charged against the raw session.
            if path != PACKAGE_MANIFEST_PATH {
                total = total
                    .checked_add(size)
                    .ok_or_else(session_placement_failed)?;
            }
            if size > MAX_FILE_BYTES
                || total > MAX_PACKAGE_BYTES
                || entries.len() > MAX_PACKAGE_FILES
            {
                return Err(session_placement_failed());
            }
            if path == PACKAGE_MANIFEST_PATH && !manifest_seen {
                manifest_seen = true;
            } else if !path
                .strip_prefix("session/")
                .is_some_and(valid_session_relative)
            {
                return Err(session_placement_failed());
            }
            entries.push((path.to_owned(), columns[2].to_owned(), size));
        }
        if !manifest_seen {
            return Err(session_placement_failed());
        }
        // All sizes and names have now passed admission. Only now read blobs,
        // bounded to their advertised sizes even if git output is malicious.
        let mut manifest = Vec::new();
        let mut files = Vec::new();
        for (path, oid, size) in entries {
            let bytes = self.session_git(mirror, &["cat-file", "blob", &oid], size as usize + 1)?;
            if bytes.len() as u64 != size {
                return Err(session_placement_failed());
            }
            if path == PACKAGE_MANIFEST_PATH {
                manifest = bytes;
            } else {
                files.push(PackageFile {
                    path: path["session/".len()..].to_owned(),
                    bytes,
                });
            }
        }
        let package = SessionPackage::from_parts(&manifest, files)?;
        if package.manifest().agent != import.agent()
            || package.manifest().source_agent_version != import.source_agent_version()
        {
            return Err(session_placement_failed());
        }
        Ok(package)
    }

    fn delete_session_ref(
        &self,
        mirror: &RootedDir,
        reference: &str,
        expected: &str,
    ) -> Result<(), WorkerError> {
        mirror.verify_bound()?;
        let mut request = git_request(
            vec![
                "--git-dir".into(),
                mirror.path().as_os_str().to_owned(),
                "rev-parse".into(),
                "--verify".into(),
                "--quiet".into(),
                "--end-of-options".into(),
                reference.into(),
            ],
            None,
        );
        request.policy.stdout_limit = 128;
        let result = self.runner.run(&request)?;
        mirror.verify_bound()?;
        if result.status.code() == Some(1) {
            return Ok(());
        }
        if !result.status.success()
            || std::str::from_utf8(&result.stdout).ok().map(str::trim) != Some(expected)
        {
            return Err(session_placement_failed());
        }
        self.session_git(mirror, &["update-ref", "-d", reference, expected], 128)?;
        Ok(())
    }

    pub fn status(&self, request: &TaskStatusRequest) -> Result<TaskStatusResponse, WorkerError> {
        request.validate()?;
        let deliveries = OriginOutbox::new(self.store, self.runner)
            .deliveries(request.project_id(), request.task_id())?;
        Ok(
            TaskStatusResponse::new(self.load_status(request.project_id(), request.task_id())?)
                .with_deliveries(deliveries),
        )
    }

    pub fn diff(&self, request: &TaskDiffRequest) -> Result<TaskDiffResponse, WorkerError> {
        request.validate()?;
        let task = self.open_existing_task(request.project_id(), request.task_id())?;
        let meta = self.read_meta(&task)?;
        let status = self.read_status(&task)?;
        if diff_uses_retained_objects(&task, &status)? {
            self.diff_retained(request.project_id(), &meta, &status, request.stat())
        } else {
            self.diff_workspace(&task, &meta, request.stat())
        }
    }

    /// Diff the live workspace, including uncommitted files, through a private
    /// index so the repository index is left unchanged.
    fn diff_workspace(
        &self,
        task: &RootedDir,
        meta: &TaskMeta,
        stat: bool,
    ) -> Result<TaskDiffResponse, WorkerError> {
        let workspace = self.open_workspace(task)?;
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
                let _ = remove_temporary_index(task, &temporary_name);
                return Err(task_error("DIFF_FAILED", error.to_string()));
            }
        };
        if !add.status.success() {
            let _ = remove_temporary_index(task, &temporary_name);
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
        if stat {
            diff_args.push(OsString::from("--stat"));
        }
        diff_args.push(meta.base_oid().to_string().into());
        let result = self.runner.run(&git_request(diff_args, temporary_index));
        let cleanup = remove_temporary_index(task, &temporary_name);
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

    /// Diff a closed task from the base and result commits kept in the host
    /// mirror. The result pin is `refs/heads/task/<id>` (the same ref upload
    /// advertises). GC drops that pin before it prunes objects, so a missing
    /// pin is "no longer retained" even while the object still exists.
    fn diff_retained(
        &self,
        project_id: &str,
        meta: &TaskMeta,
        status: &TaskStatus,
        stat: bool,
    ) -> Result<TaskDiffResponse, WorkerError> {
        let Some(mirror) = self.store.mirror_if_present(project_id)? else {
            return Err(result_not_retained());
        };
        let result_ref = format!("refs/heads/task/{}", meta.task_id());
        let Some(result) = self.read_ref(&mirror, &result_ref)? else {
            return Err(result_not_retained());
        };
        if let Some(head) = status.head_oid()
            && head.as_str() != result
        {
            return Err(retained_commits_mismatch());
        }
        if !self.mirror_has_commit(&mirror, &result)? {
            return Err(result_not_retained());
        }
        let base = meta.base_oid().as_str();
        let base_ref = format!("refs/mac-worker/bases/{}", meta.task_id());
        if let Some(pinned) = self.read_ref(&mirror, &base_ref)?
            && pinned != base
        {
            return Err(retained_commits_mismatch());
        }
        if !self.mirror_has_commit(&mirror, base)? {
            return Err(result_not_retained());
        }

        let mut args = vec![
            OsString::from("--git-dir"),
            mirror.path().as_os_str().to_os_string(),
            OsString::from("diff"),
        ];
        if stat {
            args.push(OsString::from("--stat"));
        }
        args.push(OsString::from("--end-of-options"));
        args.push(OsString::from(base));
        args.push(OsString::from(&result));
        let diff = self
            .runner
            .run(&git_request(args, None))
            .map_err(|error| task_error("DIFF_FAILED", error.to_string()))?;
        if !diff.status.success() {
            return Err(task_error(
                "DIFF_FAILED",
                String::from_utf8_lossy(&diff.stderr).trim().to_owned(),
            ));
        }
        let (text, truncated) = bound_diff(&diff.stdout)?;
        Ok(TaskDiffResponse::new(text, truncated))
    }

    fn mirror_has_commit(&self, mirror: &RootedDir, oid: &str) -> Result<bool, WorkerError> {
        let result = self
            .runner
            .run(&git_request(
                vec![
                    OsString::from("--git-dir"),
                    mirror.path().as_os_str().to_os_string(),
                    OsString::from("cat-file"),
                    OsString::from("-t"),
                    OsString::from("--end-of-options"),
                    OsString::from(oid),
                ],
                None,
            ))
            .map_err(|error| task_error("DIFF_FAILED", error.to_string()))?;
        Ok(result.status.success() && String::from_utf8_lossy(&result.stdout).trim() == "commit")
    }

    pub fn close(&self, request: &TaskCloseRequest) -> Result<TaskCloseResponse, WorkerError> {
        request.validate()?;
        // capacity (installation) then session — never reverse. GC holds
        // installation then session; acquire never takes session.
        // Herdr runs only after both guards drop: a hung sidebar must not
        // stall admission or another close.
        let (response, herdr_task) = {
            let _capacity = self.store.capacity_lock()?;
            let _session_lock = self.store.session_lock()?;
            self.close_locked(request, None)?
        };
        if let Some(task_id) = herdr_task {
            close_herdr_tabs(task_id);
        }
        Ok(response)
    }

    /// Close after this turn's result is already durable. Public `close`
    /// still treats any live task-scope lease as `TASK_BUSY`; the publisher
    /// may ignore only `own_job`'s lease, which still occupies the slot
    /// until supervisor cleanup (pin-before-release).
    pub(crate) fn close_after_own_terminal_turn(
        &self,
        request: &TaskCloseRequest,
        own_job: JobId,
    ) -> Result<TaskCloseResponse, WorkerError> {
        request.validate()?;
        if request.discard() {
            return Err(task_error("TASK_BUSY", "own-terminal close cannot discard"));
        }
        let (response, herdr_task) = {
            let _capacity = self.store.capacity_lock()?;
            let _session_lock = self.store.session_lock()?;
            self.close_locked(request, Some(own_job))?
        };
        if let Some(task_id) = herdr_task {
            close_herdr_tabs(task_id);
        }
        Ok(response)
    }

    fn close_locked(
        &self,
        request: &TaskCloseRequest,
        own_job: Option<JobId>,
    ) -> Result<(TaskCloseResponse, Option<TaskId>), WorkerError> {
        let task = self.open_existing_task(request.project_id(), request.task_id())?;
        let status = self.read_status(&task)?;
        if status.state() == TaskState::Active {
            return Err(task_error("TASK_BUSY", "task has an active turn"));
        }
        if let Some(job) = own_job {
            let Some(last) = status.turns().last() else {
                return Err(task_error("TASK_BUSY", "task has no turn history"));
            };
            if last.turn_id() != job || last.terminal().is_none() {
                return Err(task_error(
                    "TASK_BUSY",
                    "own-terminal close does not match the published turn",
                ));
            }
        }
        if self.live_foreign_task_scope(request.project_id(), request.task_id(), own_job)? {
            return Err(task_error("TASK_BUSY", "task has a live execution lease"));
        }
        if request.discard()
            && OriginOutbox::new(self.store, self.runner)
                .discard_blocked(request.project_id(), request.task_id())?
        {
            return Err(task_error("TASK_BUSY", "DELIVERY_PENDING"));
        }
        let reported_to_herdr = status.turns().iter().any(|turn| turn.herdr().is_some());

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
        let deliveries = OriginOutbox::new(self.store, self.runner)
            .deliveries(request.project_id(), request.task_id())?;
        let herdr_task = reported_to_herdr.then_some(request.task_id());
        Ok((
            TaskCloseResponse::with_warnings(status, warnings).with_deliveries(deliveries),
            herdr_task,
        ))
    }

    fn live_foreign_task_scope(
        &self,
        project_id: &str,
        task_id: TaskId,
        except_job: Option<JobId>,
    ) -> Result<bool, WorkerError> {
        Ok(LeaseService::new(self.store)
            .occupied_slots()?
            .iter()
            .any(|slot| {
                slot.owns_task(project_id, task_id) && except_job != Some(slot.lease.job_id())
            }))
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
    ) -> Result<Option<RetentionClose>, WorkerError> {
        validate_project_id(project_id)?;
        // GC already holds the installation lock for collect+apply, which
        // serializes acquire. Do not re-enter capacity_lock: it would open a
        // second installation fd and deadlock this process.
        let _session_lock = self.store.session_lock()?;
        let task = self.open_existing_task(project_id, task_id)?;
        let status = self.read_status(&task)?;
        let reported_to_herdr = status.turns().iter().any(|turn| turn.herdr().is_some());
        if status.state() != TaskState::Open {
            return Ok(None);
        }
        if LeaseService::new(self.store).task_scope_is_live(project_id, task_id)? {
            return Ok(None);
        }
        let workspace_present = task.entry_exists("workspace")?;
        if workspace_present {
            task.validate_private_entry("workspace")?;
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
        if workspace_present {
            self.store
                .remove_owned_child_committed(&task, "workspace")?;
        }
        task.sync_root()?;
        Ok(Some(RetentionClose {
            status: next,
            herdr_task: reported_to_herdr.then_some(task_id),
        }))
    }

    #[cfg(any(test, feature = "test-support"))]
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

        let bound = LeaseService::new(self.store)
            .occupied_slot_for_job(turn_id)?
            .ok_or_else(|| {
                task_error(
                    "TASK_LEASE_MISMATCH",
                    "task resume requires a live lease for the turn",
                )
            })?;
        require_task_execution_lease(&bound, project_id, task_id, turn_id, worker, None)?;

        let task = self.open_existing_task(project_id, task_id)?;
        let meta = self.read_meta(&task)?;
        if bound.lease.worktree_id() != meta.worktree_id() {
            return Err(task_error(
                "TASK_LEASE_MISMATCH",
                "live lease does not match the task request",
            ));
        }
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

    /// Notes whether the worker's herdr shows a turn.  Purely informational:
    /// it touches only the turn's `herdr` field and never the task state,
    /// outcome, or activity timestamp.
    pub(crate) fn record_turn_herdr(
        &self,
        project_id: &str,
        task_id: TaskId,
        turn_id: JobId,
        report: HerdrTurnReport,
    ) -> Result<(), WorkerError> {
        let task = self.open_existing_task(project_id, task_id)?;
        let current = self.read_status(&task)?;
        let mut turns = current.turns().to_vec();
        let Some(position) = turns.iter().position(|turn| turn.turn_id() == turn_id) else {
            return Err(task_error(
                "TASK_INCONSISTENT",
                "herdr report names an unknown turn",
            ));
        };
        turns[position] = turns[position].clone().with_herdr(Some(report));
        let next = TaskStatus::new(
            current.state(),
            current.last_outcome().cloned(),
            current.worker().map(str::to_owned),
            current.session_present(),
            current.head_oid().cloned(),
            current.summary().map(str::to_owned),
            current.questions().to_vec(),
            current.files_changed().to_vec(),
            current.diff_stat().map(str::to_owned),
            turns,
            current.updated_at_millis(),
        )?;
        replace_status_bytes(&task, current, next)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[doc(hidden)]
    pub fn finish_turn(
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
        questions: Vec<Question>,
        files_changed: Vec<String>,
        diff_stat: Option<String>,
        reported_checks: Vec<crate::agent::ReportedCheck>,
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
        // Parse reasons and agent identity are advisory. A missing launch
        // record or an unreadable diagnostic file must not keep the turn from
        // reaching its terminal state.
        turns.push(
            crate::turn::attach_turn_diagnostics(
                self.store,
                project_id,
                task_id,
                turn_id,
                replacement.clone(),
            )
            .unwrap_or(replacement),
        );
        let next_state = if close {
            TaskState::Closed
        } else {
            TaskState::Open
        };
        // A completed answer can explicitly leave no questions. Preserve the
        // prior context only for other outcomes that provide no replacement.
        let questions = if questions.is_empty() && outcome != TaskOutcome::Done {
            current.questions().to_vec()
        } else {
            questions
        };
        let next = TaskStatus::new(
            next_state,
            Some(outcome),
            current.worker().map(str::to_owned),
            current.session_present(),
            head_oid.or_else(|| current.head_oid().cloned()),
            summary.or_else(|| current.summary().map(str::to_owned)),
            questions,
            if files_changed.is_empty() {
                current.files_changed().to_vec()
            } else {
                files_changed
            },
            diff_stat.or_else(|| current.diff_stat().map(str::to_owned)),
            turns,
            ended_at,
        )?
        .with_reported_checks(reported_checks)?;
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
            Vec::new(),
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
        let _session_lock = self.store.session_lock()?;
        self.bind_session_locked(project_id, task_id, binding)
    }

    fn bind_session_locked(
        &self,
        project_id: &str,
        task_id: TaskId,
        binding: SessionBinding,
    ) -> Result<(), WorkerError> {
        let task = self.open_existing_task(project_id, task_id)?;
        let status = self.read_status(&task)?;
        if status.state().is_terminal() {
            return Err(task_error(
                "TASK_CLOSED",
                "terminal tasks cannot accept a session binding",
            ));
        }
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
        let task = self.open_existing_task(project_id, task_id)?;
        let meta = self.read_meta(&task)?;
        let (home, profile) = task_account_profile(&meta)?;
        if let Some(agent) = SessionAgent::from_agent_kind(binding.agent()) {
            // Apply the same override validation as placement, not a relative
            // directory interpreted from the endpoint's working directory.
            let _ = store_root(agent, &home, &profile_strings(&profile))?;
        }
        // An agent with dialects deletes in the form of the generation
        // installed now: OpenCode 2 needs `--standalone` to stay out of its
        // background service, and the session may be older than an upgrade.
        let argv = crate::agent::identity::installed_adapter(binding.agent(), self.runner, &home)
            .delete_session(binding.session_ref())
            .unwrap_or(argv);
        let request = crate::agent::prebind_login_request(&argv, &home, profile.entries())?;
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

    fn pin_base_ref(
        &self,
        mirror: &RootedDir,
        task_id: TaskId,
        base: &BaseOid,
    ) -> Result<(), WorkerError> {
        let reference = format!("refs/mac-worker/bases/{task_id}");
        let current = self.read_ref(mirror, &reference)?;
        if let Some(existing) = current {
            if existing == base.as_str() {
                return Ok(());
            }
            return Err(git_error(
                "BASE_REF_CONFLICT",
                "base ref already points at a different object",
            ));
        }
        let result = self
            .runner
            .run(&git_request(
                vec![
                    OsString::from("--git-dir"),
                    mirror.path().as_os_str().to_os_string(),
                    OsString::from("update-ref"),
                    reference.into(),
                    base.to_string().into(),
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

    fn read_ref(&self, mirror: &RootedDir, reference: &str) -> Result<Option<String>, WorkerError> {
        let result = self
            .runner
            .run(&git_request(
                vec![
                    OsString::from("--git-dir"),
                    mirror.path().as_os_str().to_os_string(),
                    OsString::from("rev-parse"),
                    OsString::from("--verify"),
                    OsString::from("--quiet"),
                    OsString::from("--end-of-options"),
                    reference.into(),
                ],
                None,
            ))
            .map_err(|error| git_error("REF_UPDATE_FAILED", error.to_string()))?;
        if !result.status.success() {
            return Ok(None);
        }
        let value = String::from_utf8_lossy(&result.stdout).trim().to_owned();
        if value.is_empty() {
            Ok(None)
        } else {
            Ok(Some(value))
        }
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
        read_record(task, "meta.json").map_err(|error| map_task_read_error(task, error))
    }

    fn read_status(&self, task: &RootedDir) -> Result<TaskStatus, WorkerError> {
        self.read_status_with_hook(task, || {})
    }

    fn read_status_with_hook(
        &self,
        task: &RootedDir,
        mut after_open: impl FnMut(),
    ) -> Result<TaskStatus, WorkerError> {
        let mut retries = 0;
        loop {
            match read_record_with_hook(task, "status.json", &mut after_open) {
                Err(WorkerError::Io(error))
                    if error.raw_os_error() == Some(libc::ESTALE) && retries < 3 =>
                {
                    // Publication replaces status.json atomically. Retry only
                    // within this task directory; GC disappearance stays
                    // TASK_NOT_FOUND and a replaced directory stays an error.
                    task.verify_bound()
                        .map_err(WorkerError::Io)
                        .map_err(map_task_not_found)?;
                    retries += 1;
                }
                result => return result.map_err(|error| map_task_read_error(task, error)),
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SessionImportStage {
    Planned,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionImportReceipt {
    schema: u32,
    stage: SessionImportStage,
    package_oid: String,
    session_id: String,
    agent: SessionAgent,
    placed_at_millis: u64,
    store_root: String,
    primary_relative: String,
    files: Vec<String>,
}
impl SessionImportReceipt {
    fn validate(&self) -> Result<(), WorkerError> {
        SessionImportMeta::new(self.agent, &self.package_oid, "1")?;
        let id = uuid::Uuid::parse_str(&self.session_id).map_err(|_| session_placement_failed())?;
        if self.schema != 1
            || id.hyphenated().to_string() != self.session_id
            || self.placed_at_millis == 0
            || !Path::new(&self.store_root).is_absolute()
            || self.store_root.chars().any(char::is_control)
            || self.files.len() > MAX_PACKAGE_FILES
            || self.files.iter().any(|path| !valid_session_relative(path))
            || self.files.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(session_placement_failed());
        }
        match self.stage {
            SessionImportStage::Planned
                if self.primary_relative.is_empty() && self.files.is_empty() =>
            {
                Ok(())
            }
            SessionImportStage::Complete
                if valid_session_relative(&self.primary_relative)
                    && self.files.contains(&self.primary_relative) =>
            {
                Ok(())
            }
            _ => Err(session_placement_failed()),
        }
    }
}
fn valid_session_relative(path: &str) -> bool {
    !path.is_empty()
        && !path.contains(['\\', ':'])
        && !path.chars().any(char::is_control)
        && path
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}
fn session_placement_failed() -> WorkerError {
    task_error("SESSION_PLACEMENT_FAILED", "session import failed")
}

// Keep this resolution identical to the supervisor's launch path: account
// HOME, not host data/XDG roots, and EnvProfile's full permission validation.
fn task_account_profile(meta: &TaskMeta) -> Result<(PathBuf, EnvProfile), WorkerError> {
    let home = std::env::var_os("HOME")
        .filter(|home| !home.is_empty())
        .map(PathBuf::from)
        .ok_or_else(session_placement_failed)?;
    let profile = match meta.env_profile() {
        Some(name) => EnvProfile::load_for_home(
            &home
                .join(".config/mac-worker/env")
                .join(format!("{name}.env")),
            &home,
        )?,
        None => EnvProfile::empty(),
    };
    Ok((home, profile))
}
// Persist the physical root identity, including when the native root has not
// yet been created. A planned retry must not follow a retargeted root symlink
// into a second native store. RootedDir later creates any missing suffix.
fn physical_store_root(root: &Path) -> Result<PathBuf, WorkerError> {
    match root.canonicalize() {
        Ok(physical) => Ok(physical),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            // A dangling root symlink is not a missing directory to create.
            if fs::symlink_metadata(root).is_ok() {
                return Err(session_placement_failed());
            }
            let parent = root.parent().ok_or_else(session_placement_failed)?;
            let name = root.file_name().ok_or_else(session_placement_failed)?;
            Ok(physical_store_root(parent)?.join(name))
        }
        Err(error) => Err(error.into()),
    }
}
fn profile_strings(profile: &EnvProfile) -> Vec<(String, String)> {
    // EnvProfile was decoded from UTF-8 text; these entries cannot be lossy.
    profile
        .entries()
        .iter()
        .map(|(key, value)| {
            (
                key.to_string_lossy().into_owned(),
                value.to_string_lossy().into_owned(),
            )
        })
        .collect()
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
    let mut prefixed = vec![
        OsString::from("-c"),
        OsString::from(format!(
            "core.fsync={}",
            crate::git_transport::GIT_FSYNC_COMPONENTS
        )),
        OsString::from("-c"),
        OsString::from(format!(
            "core.fsyncMethod={}",
            crate::git_transport::GIT_FSYNC_METHOD
        )),
    ];
    prefixed.extend(args);
    ProcessRequest {
        program: GIT_PROGRAM.into(),
        args: prefixed,
        environment,
        environment_remove,
        stdin: None,
        policy: ProcessPolicy {
            stdout_limit: GIT_STDOUT_LIMIT,
            stderr_limit: GIT_STDERR_LIMIT,
            deadline: GIT_DEADLINE,
        },
        isolate_parent_environment: false,
    }
}

fn read_record<T: DeserializeOwned + Serialize>(
    directory: &RootedDir,
    name: &str,
) -> Result<T, WorkerError> {
    read_record_with_hook(directory, name, || {})
}

fn read_record_with_hook<T: DeserializeOwned + Serialize>(
    directory: &RootedDir,
    name: &str,
    after_open: impl FnMut(),
) -> Result<T, WorkerError> {
    let bytes =
        directory.read_private_regular_with_hook(name, MAX_TASK_RECORD_BYTES, after_open)?;
    let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
    let value = T::deserialize(&mut deserializer)
        .map_err(|_| task_record_invalid(name, "could not be decoded"))?;
    deserializer
        .end()
        .map_err(|_| task_record_invalid(name, "has trailing data"))?;
    let canonical = serde_json::to_vec(&value)
        .map_err(|_| task_record_invalid(name, "could not be encoded"))?;
    if canonical != bytes {
        return Err(task_record_invalid(name, "is not canonical"));
    }
    Ok(value)
}

/// A stored task record that fails validation is a host-side fault, not a bad
/// request. Give it a stable code so the host does not fold it into the
/// generic `INVALID_REQUEST` that peers read as "old helper". The detail is a
/// record name and a fixed reason; decoder text can quote record content.
fn task_record_invalid(name: &str, reason: &str) -> WorkerError {
    WorkerError::Protocol(format!("{TASK_RECORD_INVALID}: {name} {reason}"))
}

const TASK_RECORD_INVALID: &str = "TASK_RECORD_INVALID";

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
    )?
    .copying_reported_checks(&current)?;
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

fn map_task_read_error(task: &RootedDir, error: WorkerError) -> WorkerError {
    if matches!(&error, WorkerError::Io(error) if error.raw_os_error() == Some(libc::ESTALE)) {
        // GC can retire the directory after it was opened. Only confirmed
        // absence of this binding has the same meaning as ENOENT; a live or
        // replaced directory must preserve the original identity failure.
        if let Err(binding_error) = task.verify_bound() {
            return map_task_not_found(binding_error.into());
        }
    }
    map_task_not_found(error)
}

fn map_task_not_found(error: WorkerError) -> WorkerError {
    match error {
        WorkerError::Io(error) if error.kind() == io::ErrorKind::NotFound => task_not_found(),
        other => other,
    }
}

fn require_task_execution_lease(
    bound: &OccupiedSlot,
    project_id: &str,
    task_id: TaskId,
    job_id: JobId,
    worker: &str,
    worktree_id: Option<&str>,
) -> Result<(), WorkerError> {
    if !bound.owns_task(project_id, task_id) {
        return Err(task_error(
            "EXECUTION_SCOPE_CONFLICT",
            "live lease is not bound to this task workspace",
        ));
    }
    if bound.lease.job_id() != job_id
        || bound.lease.project_id() != project_id
        || bound.lease.worker_name() != worker
        || worktree_id.is_some_and(|worktree| bound.lease.worktree_id() != worktree)
    {
        return Err(task_error(
            "TASK_LEASE_MISMATCH",
            "live lease does not match the task request",
        ));
    }
    Ok(())
}

fn task_not_found() -> WorkerError {
    task_error("TASK_NOT_FOUND", "task metadata or workspace is absent")
}

fn result_not_retained() -> WorkerError {
    task_error(
        "RESULT_NOT_RETAINED",
        "task workspace is closed and its result is no longer retained",
    )
}

fn retained_commits_mismatch() -> WorkerError {
    task_error(
        "DIFF_FAILED",
        "task workspace is closed and its retained commits do not match the task record",
    )
}

/// Closed tasks always diff the mirror. Other terminal states do too once
/// the workspace is gone, so a discarded or lost task is not reported as
/// missing while its metadata still exists.
fn diff_uses_retained_objects(task: &RootedDir, status: &TaskStatus) -> Result<bool, WorkerError> {
    if status.state() == TaskState::Closed {
        return Ok(true);
    }
    Ok(status.state().is_terminal() && !task.entry_exists("workspace")?)
}

fn task_error(
    code: &'static str,
    message: impl Into<std::borrow::Cow<'static, str>>,
) -> WorkerError {
    WorkerError::task(code, message)
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

/// Account home for herdr socket resolution when process `HOME` is a job home.
pub(crate) const MAC_WORKER_ACCOUNT_HOME: &str = "MAC_WORKER_ACCOUNT_HOME";

/// The worker account home herdr listens under. Prefers the additive
/// `MAC_WORKER_ACCOUNT_HOME` the supervisor injects into a job, then `HOME`.
pub(crate) fn herdr_account_home() -> Option<PathBuf> {
    for key in [MAC_WORKER_ACCOUNT_HOME, "HOME"] {
        if let Some(home) = std::env::var_os(key).filter(|home| !home.is_empty()) {
            return Some(PathBuf::from(home));
        }
    }
    None
}

/// Best effort: remove the task's tabs from the worker's herdr.  Called only
/// for tasks whose record shows a turn was reported, so a worker that never
/// reported, and every test process, never opens the socket.
fn close_herdr_tabs(task_id: TaskId) {
    let Some(home) = herdr_account_home() else {
        return;
    };
    let _ = crate::herdr_reporter::HerdrReporter::for_home(&home).close(task_id);
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

    fn status_fixture() -> (tempfile::TempDir, HostStore, RootedDir, TaskStatus) {
        let temp = tempdir().unwrap();
        let store = HostStore::open(&temp.path().join("host")).unwrap();
        let task = store
            .open_task_directory(PROJECT_ID, TaskId::generate(), true)
            .unwrap();
        let status = TaskStatus::new(
            TaskState::Open,
            None,
            Some("worker".into()),
            false,
            None,
            None,
            Vec::new(),
            Vec::new(),
            None,
            Vec::new(),
            1,
        )
        .unwrap();
        write_record_once(&task, "status.json", &status).unwrap();
        (temp, store, task, status)
    }

    fn open_status_with_summary(
        summary: &str,
    ) -> (tempfile::TempDir, HostStore, TaskId, TaskStatus) {
        let temp = tempdir().unwrap();
        let store = HostStore::open(&temp.path().join("host")).unwrap();
        let task_id = TaskId::generate();
        let task = store
            .open_task_directory(PROJECT_ID, task_id, true)
            .unwrap();
        let status = TaskStatus::new(
            TaskState::Open,
            Some(TaskOutcome::Done),
            Some("worker".into()),
            true,
            None,
            Some(summary.to_owned()),
            Vec::new(),
            vec!["apps/backend/src/plugins/auth.plugin.ts".into()],
            None,
            Vec::new(),
            1,
        )
        .unwrap();
        write_record_once(&task, "status.json", &status).unwrap();
        (temp, store, task_id, status)
    }

    /// The client re-decodes a host response and requires the same bytes.
    fn assert_response_round_trips(response: &TaskStatusResponse) {
        let wire = serde_json::to_vec(response).unwrap();
        let decoded: TaskStatusResponse = serde_json::from_slice(&wire).unwrap();
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), wire);
    }

    #[test]
    fn finished_turn_with_a_bearer_summary_stays_readable() {
        // Break caught: three Codex turns finished with "without the Bearer
        // prefix" in the summary. The turn boundary and TaskStatus::new each
        // appended a `]`, the host read appended a third, and every status,
        // diff and cancel failed as INVALID_REQUEST with the result unpublished.
        let summary = crate::redaction::RedactionBoundary::from_env()
            .summary("auth.plugin.ts: rejects a valid JWT without the Bearer prefix\u{201d}.");
        let (_temp, store, task_id, written) = open_status_with_summary(&summary);
        assert_eq!(
            written.summary(),
            Some("auth.plugin.ts: rejects a valid JWT without the Bearer [token]\u{201d}.")
        );

        let response = TaskStore::new(&store, &SystemProcessRunner)
            .status(&TaskStatusRequest::new(PROJECT_ID, task_id))
            .unwrap();
        assert_eq!(response.status(), &written);
        assert_response_round_trips(&response);
    }

    #[test]
    fn status_written_by_the_old_redactor_reads_back_unchanged() {
        // Records already on the workers carry `Bearer [token]]`. The fixed
        // boundary must accept them as written so a redeploy releases them.
        let (_temp, store, task_id, written) =
            open_status_with_summary("without the Bearer [token]]\u{201d} pinned by it.fails");
        let bytes = serde_json::to_vec(&written).unwrap();
        assert!(
            String::from_utf8(bytes)
                .unwrap()
                .contains("Bearer [token]]\u{201d}")
        );

        let response = TaskStore::new(&store, &SystemProcessRunner)
            .status(&TaskStatusRequest::new(PROJECT_ID, task_id))
            .unwrap();
        assert_eq!(response.status(), &written);
        assert_response_round_trips(&response);
    }

    #[test]
    fn invalid_task_records_report_their_own_code() {
        // Break caught: a stored record that failed its canonical check left
        // the host as INVALID_REQUEST "host request was invalid", which both
        // hid the fault and reads to peers like an old helper.
        let (_temp, store, task_id, written) = open_status_with_summary("done");
        let task = store
            .open_task_directory(PROJECT_ID, task_id, false)
            .unwrap();
        let canonical = serde_json::to_vec(&written).unwrap();
        let spaced = [b"{ ".as_slice(), &canonical[1..]].concat();
        task.replace_private_regular_exact("status.json", &canonical, &spaced)
            .unwrap();

        let error = TaskStore::new(&store, &SystemProcessRunner)
            .status(&TaskStatusRequest::new(PROJECT_ID, task_id))
            .unwrap_err();
        assert_eq!(error.public_code(), TASK_RECORD_INVALID);
        let wire = serde_json::to_value(crate::versioned_host_error(&error)).unwrap();
        assert_eq!(wire["error"]["code"], TASK_RECORD_INVALID);
        assert_eq!(wire["error"]["message"], "status.json is not canonical");

        // Decoder text can quote the record; the detail never does.
        let planted = br#"{"state":"PLANTED-RECORD-TEXT"}"#;
        task.replace_private_regular_exact("status.json", &spaced, planted)
            .unwrap();
        let error = TaskStore::new(&store, &SystemProcessRunner)
            .load_status(PROJECT_ID, task_id)
            .unwrap_err();
        assert_eq!(error.public_code(), TASK_RECORD_INVALID);
        assert!(!error.to_string().contains("PLANTED"), "{error}");
        let wire = serde_json::to_value(crate::versioned_host_error(&error)).unwrap();
        assert_eq!(wire["error"]["message"], "status.json could not be decoded");
    }

    #[test]
    fn status_read_rechecks_peer_publication_in_the_bound_task() {
        let (_temp, store, task, original) = status_fixture();
        let mut expected = original.clone();
        let mut published = false;
        let status = TaskStore::new(&store, &SystemProcessRunner)
            .read_status_with_hook(&task, || {
                if !published {
                    published = true;
                    expected = replace_status_record_at(
                        &task,
                        original.clone(),
                        TaskState::Closed,
                        None,
                        2,
                    )
                    .unwrap();
                }
            })
            .unwrap();
        assert!(published);
        assert_eq!(status, expected);
        assert_eq!(status.state(), TaskState::Closed);
    }

    #[test]
    fn status_read_rechecks_gc_retirement_as_task_not_found() {
        let (_temp, store, task, _) = status_fixture();
        let parent = store
            .open_directory(&format!("tasks/{PROJECT_ID}"), false)
            .unwrap();
        let name = task.path().file_name().unwrap().to_str().unwrap();
        let error = TaskStore::new(&store, &SystemProcessRunner)
            .read_status_with_hook(&task, || {
                store.remove_owned_child_committed(&parent, name).unwrap();
            })
            .unwrap_err();
        assert_eq!(error.public_code(), "TASK_NOT_FOUND");
        assert!(!task.path().exists());
    }

    #[test]
    fn stale_task_read_is_missing_only_after_bound_gc_retirement() {
        let (_temp, store, task, _) = status_fixture();
        let parent = store
            .open_directory(&format!("tasks/{PROJECT_ID}"), false)
            .unwrap();
        store
            .remove_owned_child_committed(
                &parent,
                task.path().file_name().unwrap().to_str().unwrap(),
            )
            .unwrap();
        // APFS may return ESTALE rather than ENOENT for a lookup through the
        // descriptor of the directory that GC just retired.
        let error = map_task_read_error(&task, io::Error::from_raw_os_error(libc::ESTALE).into());
        assert_eq!(error.public_code(), "TASK_NOT_FOUND");
    }

    #[test]
    fn stale_task_read_keeps_errors_for_a_live_or_replaced_binding() {
        for replace in [false, true] {
            let (temp, _store, task, _) = status_fixture();
            if replace {
                std::fs::rename(task.path(), temp.path().join("detached-task")).unwrap();
                RootedDir::create(task.path()).unwrap();
            }
            let error =
                map_task_read_error(&task, io::Error::from_raw_os_error(libc::ESTALE).into());
            assert!(
                matches!(error, WorkerError::Io(error) if error.raw_os_error() == Some(libc::ESTALE))
            );
        }
    }

    #[test]
    fn status_read_does_not_follow_a_replaced_task_directory() {
        let (_temp, store, task, original) = status_fixture();
        let mut reads = 0;
        let error = TaskStore::new(&store, &SystemProcessRunner)
            .read_status_with_hook(&task, || {
                reads += 1;
                let detached = task.path().with_extension("detached");
                std::fs::rename(task.path(), detached).unwrap();
                let replacement = RootedDir::create(task.path()).unwrap();
                write_record_once(&replacement, "status.json", &original).unwrap();
            })
            .unwrap_err();
        assert!(
            matches!(error, WorkerError::Io(ref error) if error.raw_os_error() == Some(libc::ESTALE))
        );
        assert_eq!(reads, 1, "do not rebind to the replacement task");
    }

    #[test]
    fn status_read_rechecks_are_bounded_under_continuous_publication() {
        let (_temp, store, task, mut current) = status_fixture();
        let mut reads = 0;
        let error = TaskStore::new(&store, &SystemProcessRunner)
            .read_status_with_hook(&task, || {
                reads += 1;
                current = replace_status_record_at(
                    &task,
                    current.clone(),
                    TaskState::Open,
                    None,
                    reads + 1,
                )
                .unwrap();
            })
            .unwrap_err();
        assert!(
            matches!(error, WorkerError::Io(ref error) if error.raw_os_error() == Some(libc::ESTALE))
        );
        assert!(
            reads > 1,
            "a concurrent status publication must be rechecked"
        );
        assert!(
            reads <= 4,
            "continuous writers must not cause an unbounded read"
        );
    }

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
