use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    io,
    path::Path,
    time::Duration,
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    error::WorkerError,
    host_store::HostStore,
    inputs::RelativePath,
    job::{JobId, JobMeta, JobStatus},
    lease::LeaseService,
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
    protocol::PROTOCOL_VERSION,
    rooted_fs::{EntryKind, RootedDir},
    task::{TaskId, TaskMeta, TaskState, TaskStatus},
    task_store::TaskStore,
};

pub const TASK_RETENTION_MILLIS: u64 = 7 * 24 * 60 * 60 * 1000;
pub const BRANCH_RETENTION_MILLIS: u64 = 30 * 24 * 60 * 60 * 1000;
pub const JOB_RETENTION_MILLIS: u64 = 7 * 24 * 60 * 60 * 1000;

const MAX_GC_CANDIDATES: usize = 4096;
const MAX_GC_IDENTIFIER_BYTES: usize = 512;
const MAX_GC_REASON_BYTES: usize = 128;
const MAX_GC_SIZE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_GC_FILE_BYTES: u64 = 1024 * 1024;
const MAX_GC_REF_BYTES: usize = 1024 * 1024;
const GC_GIT_OUTPUT_LIMIT: usize = 1024 * 1024;
const GC_GIT_DEADLINE: Duration = Duration::from_secs(30);
const GC_GIT_PROGRAM: &str = "/usr/bin/git";
const GC_REASON_LEGACY_PROTOCOL: &str = "legacy protocol";
const GC_REASON_UNREADABLE_RECORD: &str = "unreadable record";
const GC_REASON_INCONSISTENT_RECORD: &str = "inconsistent record";

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
    "GIT_ATTR_SOURCE",
    "GIT_CONFIG",
    "GIT_LITERAL_PATHSPECS",
    "GIT_GLOB_PATHSPECS",
    "GIT_NOGLOB_PATHSPECS",
    "GIT_ICASE_PATHSPECS",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcRequest {
    protocol_version: u32,
    apply: bool,
    now_millis: u64,
    task_retention_millis: u64,
    branch_retention_millis: u64,
    job_retention_millis: u64,
}

impl GcRequest {
    pub fn new(apply: bool, now_millis: u64) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            apply,
            now_millis,
            task_retention_millis: TASK_RETENTION_MILLIS,
            branch_retention_millis: BRANCH_RETENTION_MILLIS,
            job_retention_millis: JOB_RETENTION_MILLIS,
        }
    }

    pub fn with_retention(
        mut self,
        task_retention_millis: u64,
        branch_retention_millis: u64,
        job_retention_millis: u64,
    ) -> Self {
        self.task_retention_millis = task_retention_millis;
        self.branch_retention_millis = branch_retention_millis;
        self.job_retention_millis = job_retention_millis;
        self
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn apply(&self) -> bool {
        self.apply
    }

    pub fn now_millis(&self) -> u64 {
        self.now_millis
    }

    pub fn task_retention_millis(&self) -> u64 {
        self.task_retention_millis
    }

    pub fn branch_retention_millis(&self) -> u64 {
        self.branch_retention_millis
    }

    pub fn job_retention_millis(&self) -> u64 {
        self.job_retention_millis
    }

    pub(crate) fn validate(&self) -> Result<(), WorkerError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(gc_protocol(
                "GC_PROTOCOL_MISMATCH",
                "GC protocol version is incompatible",
            ));
        }
        if self.task_retention_millis == 0
            || self.branch_retention_millis == 0
            || self.job_retention_millis == 0
        {
            return Err(gc_protocol(
                "GC_RETENTION_INVALID",
                "GC retention windows must be positive",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcCandidate {
    kind: String,
    identifier: String,
    size_bytes: u64,
    reason: String,
}

impl GcCandidate {
    pub(crate) fn new(
        kind: &str,
        identifier: impl Into<String>,
        size_bytes: u64,
        reason: &str,
    ) -> Result<Self, WorkerError> {
        let candidate = Self {
            kind: kind.into(),
            identifier: identifier.into(),
            size_bytes,
            reason: reason.into(),
        };
        candidate.validate()?;
        Ok(candidate)
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn identifier(&self) -> &str {
        &self.identifier
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub(crate) fn validate(&self) -> Result<(), WorkerError> {
        if self.kind.is_empty()
            || self.kind.len() > 32
            || !self
                .kind
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
        {
            return Err(gc_protocol(
                "GC_CANDIDATE_INVALID",
                "GC candidate kind is invalid",
            ));
        }
        if self.identifier.is_empty()
            || self.identifier.len() > MAX_GC_IDENTIFIER_BYTES
            || self
                .identifier
                .bytes()
                .any(|byte| byte == 0 || byte.is_ascii_control())
        {
            return Err(gc_protocol(
                "GC_CANDIDATE_INVALID",
                "GC candidate identifier is invalid",
            ));
        }
        if self.size_bytes > MAX_GC_SIZE_BYTES {
            return Err(gc_protocol(
                "GC_CANDIDATE_INVALID",
                "GC candidate size exceeds the bounded report limit",
            ));
        }
        if self.reason.is_empty()
            || self.reason.len() > MAX_GC_REASON_BYTES
            || self
                .reason
                .bytes()
                .any(|byte| byte == 0 || byte.is_ascii_control())
        {
            return Err(gc_protocol(
                "GC_CANDIDATE_INVALID",
                "GC candidate reason is invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcReport {
    protocol_version: u32,
    apply: bool,
    candidates: Vec<GcCandidate>,
    applied: Vec<GcCandidate>,
    warnings: Vec<String>,
}

impl GcReport {
    pub(crate) fn new(
        apply: bool,
        candidates: Vec<GcCandidate>,
        applied: Vec<GcCandidate>,
        warnings: Vec<String>,
    ) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            apply,
            candidates,
            applied,
            warnings,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), WorkerError> {
        if self.protocol_version != PROTOCOL_VERSION
            || self.candidates.len() > MAX_GC_CANDIDATES
            || self.applied.len() > MAX_GC_CANDIDATES
            || self.warnings.len() > 64
        {
            return Err(gc_protocol(
                "GC_RESPONSE_INVALID",
                "GC response exceeds its bounded shape",
            ));
        }
        for candidate in self.candidates.iter().chain(self.applied.iter()) {
            candidate.validate()?;
        }
        if self.warnings.iter().any(|warning| {
            warning.is_empty()
                || warning.len() > MAX_GC_REASON_BYTES
                || warning
                    .bytes()
                    .any(|byte| byte == 0 || byte.is_ascii_control())
        }) {
            return Err(gc_protocol("GC_RESPONSE_INVALID", "GC warning is invalid"));
        }
        Ok(())
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn apply(&self) -> bool {
        self.apply
    }

    pub fn candidates(&self) -> &[GcCandidate] {
        &self.candidates
    }

    pub fn applied(&self) -> &[GcCandidate] {
        &self.applied
    }

    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }
}

pub struct HostGc<'a> {
    store: &'a HostStore,
    runner: &'a dyn ProcessRunner,
}

impl<'a> HostGc<'a> {
    pub fn new(store: &'a HostStore, runner: &'a dyn ProcessRunner) -> Self {
        Self { store, runner }
    }

    pub fn preview_at(&self, now_millis: u64) -> Result<GcReport, WorkerError> {
        self.run(&GcRequest::new(false, now_millis))
    }

    pub fn apply_at(&self, now_millis: u64) -> Result<GcReport, WorkerError> {
        self.run(&GcRequest::new(true, now_millis))
    }

    pub fn run(&self, request: &GcRequest) -> Result<GcReport, WorkerError> {
        request.validate()?;
        self.store.with_gc_lock(|| {
            let (inventory, mut candidates, mut warnings) = self.collect(request)?;
            candidates.sort_by(|left, right| {
                candidate_rank(left)
                    .cmp(&candidate_rank(right))
                    .then_with(|| left.identifier.cmp(&right.identifier))
                    .then_with(|| left.reason.cmp(&right.reason))
            });
            let mut applied = Vec::new();
            if request.apply() {
                for candidate in &candidates {
                    if self.apply_candidate(candidate, &inventory, request, &mut warnings)? {
                        applied.push(candidate.clone());
                    }
                }
                if inventory.herdr_reported {
                    sweep_herdr_tabs(&inventory.task_ids);
                }
            }
            Ok(GcReport::new(
                request.apply(),
                candidates,
                applied,
                warnings,
            ))
        })
    }

    fn collect(
        &self,
        request: &GcRequest,
    ) -> Result<(GcInventory, Vec<GcCandidate>, Vec<String>), WorkerError> {
        let mut warnings = Vec::new();
        let mut inventory = GcInventory::default();
        match LeaseService::new(self.store).load() {
            Ok(lease) => inventory.live_lease_job = lease.map(|lease| lease.job_id()),
            Err(error) => {
                inventory.lease_uncertain = true;
                push_warning(&mut warnings, "lease record unreadable; GC kept records");
                let _ = error;
            }
        }
        let mut candidates = Vec::new();
        self.collect_tasks(request, &mut inventory, &mut candidates, &mut warnings)?;
        self.collect_jobs(request, &inventory, &mut candidates, &mut warnings)?;
        self.collect_mirrors(request, &inventory, &mut candidates, &mut warnings)?;
        Ok((inventory, candidates, warnings))
    }

    fn collect_tasks(
        &self,
        request: &GcRequest,
        inventory: &mut GcInventory,
        candidates: &mut Vec<GcCandidate>,
        warnings: &mut Vec<String>,
    ) -> Result<(), WorkerError> {
        let tasks = self.store.open_directory("tasks", false)?;
        for raw_project in tasks.list_names()? {
            if is_rooted_namespace(&raw_project) {
                continue;
            }
            let project = match parse_digest_component(&raw_project, "task project") {
                Ok(project) => project,
                Err(_) => {
                    push_warning(warnings, "unreadable task project record");
                    continue;
                }
            };
            let project_dir = match tasks.open_child_directory(&relative(&project)?, false) {
                Ok(project_dir) => project_dir,
                Err(_) => {
                    inventory.uncertain_task_projects.insert(project.clone());
                    push_warning(warnings, "unreadable task project record");
                    continue;
                }
            };
            for raw_task in project_dir.list_names()? {
                if is_rooted_namespace(&raw_task) {
                    continue;
                }
                let task_name = match parse_utf8(&raw_task, "task directory") {
                    Ok(task_name) => task_name,
                    Err(_) => {
                        inventory.uncertain_task_projects.insert(project.clone());
                        push_warning(warnings, "unreadable task record");
                        continue;
                    }
                };
                let task_id = match task_name.parse::<TaskId>() {
                    Ok(task_id) => task_id,
                    Err(_) => {
                        inventory.uncertain_task_projects.insert(project.clone());
                        push_warning(warnings, "inconsistent task record");
                        continue;
                    }
                };
                let identifier = format!("{project}/{task_id}");
                let task = match project_dir.open_child_directory(&relative(&task_name)?, false) {
                    Ok(task) => task,
                    Err(_) => {
                        inventory.uncertain_task_projects.insert(project.clone());
                        push_record_warning(
                            warnings,
                            "task",
                            &identifier,
                            GC_REASON_UNREADABLE_RECORD,
                        );
                        continue;
                    }
                };
                inventory.task_projects.insert(project.clone());
                inventory.task_ids.insert(task_id.to_string());
                let result = self.collect_task_record(
                    request,
                    inventory,
                    candidates,
                    &project,
                    task_id,
                    &identifier,
                    &task,
                );
                let Err(error) = result else {
                    continue;
                };
                inventory.uncertain_task_projects.insert(project.clone());
                if is_legacy_protocol_record(&task, "meta.json", &error)
                    || is_legacy_protocol_record(&task, "status.json", &error)
                {
                    push_candidate(
                        candidates,
                        GcCandidate::new("task", identifier, 0, GC_REASON_LEGACY_PROTOCOL)?,
                    )?;
                } else {
                    push_record_warning(
                        warnings,
                        "task",
                        &identifier,
                        record_failure_reason(&error),
                    );
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)] // Keeps the rooted record context explicit at the boundary.
    fn collect_task_record(
        &self,
        request: &GcRequest,
        inventory: &mut GcInventory,
        candidates: &mut Vec<GcCandidate>,
        project: &str,
        task_id: TaskId,
        identifier: &str,
        task: &RootedDir,
    ) -> Result<(), WorkerError> {
        let meta: TaskMeta = read_gc_json(task, "meta.json", "task metadata")?;
        let status: TaskStatus = read_gc_json(task, "status.json", "task status")?;
        if status.turns().iter().any(|turn| turn.herdr().is_some()) {
            inventory.herdr_reported = true;
        }
        if meta.project_id() != project || meta.task_id() != task_id {
            return Err(gc_metadata(
                &format!("tasks/{project}/{task_id}"),
                "task metadata does not match its rooted path",
            ));
        }
        let branch_ref = format!("refs/heads/task/{task_id}");
        let base_ref = format!("refs/mac-worker/bases/{task_id}");
        let (branch_exists, base_exists) = if status.state().is_terminal() {
            if let Some(mirror) = self.store.mirror_if_present(project)? {
                (
                    git_ref_exists(self.runner, mirror.path(), &branch_ref)?,
                    git_ref_exists(self.runner, mirror.path(), &base_ref)?,
                )
            } else {
                (false, false)
            }
        } else {
            (false, false)
        };
        let active_work = inventory.lease_uncertain
            || self.task_has_active_work(
                project,
                &meta,
                &status,
                inventory.live_lease_job,
                request.now_millis(),
            )?;
        let size_bytes = bounded_tree_size(task)?;
        inventory.tasks.insert(
            identifier.to_owned(),
            TaskSnapshot {
                state: status.state(),
                updated_at_millis: status.updated_at_millis(),
                active_work,
            },
        );

        if !active_work
            && status.state() == TaskState::Open
            && expired(
                request.now_millis(),
                status.updated_at_millis(),
                request.task_retention_millis(),
            )
        {
            push_candidate(
                candidates,
                GcCandidate::new(
                    "task",
                    identifier.to_owned(),
                    size_bytes,
                    "open task retention",
                )?,
            )?;
        }

        if !active_work
            && status.state().is_terminal()
            && (branch_exists || base_exists)
            && expired(
                request.now_millis(),
                status.updated_at_millis(),
                request.branch_retention_millis(),
            )
        {
            push_candidate(
                candidates,
                GcCandidate::new("branch", identifier.to_owned(), 0, "branch retention")?,
            )?;
        }

        let metadata_expired = status.state().is_terminal()
            && expired(
                request.now_millis(),
                status.updated_at_millis(),
                request.job_retention_millis(),
            );
        if !active_work && metadata_expired {
            push_candidate(
                candidates,
                GcCandidate::new(
                    "task",
                    identifier.to_owned(),
                    size_bytes,
                    "task metadata retention",
                )?,
            )?;
        }
        Ok(())
    }

    fn task_has_active_work(
        &self,
        project: &str,
        meta: &TaskMeta,
        status: &TaskStatus,
        live_lease_job: Option<JobId>,
        now_millis: u64,
    ) -> Result<bool, WorkerError> {
        for turn in status.turns() {
            if turn.terminal().is_none() {
                return Ok(true);
            }
            let job_id = turn.turn_id();
            if live_lease_job == Some(job_id) {
                return Ok(true);
            }
            let job_path = format!("jobs/{project}/{}/{job_id}", meta.worktree_id());
            let job = match self.store.open_directory(&job_path, false) {
                Ok(job) => job,
                Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                    continue;
                }
                Err(error) => return Err(error),
            };
            let job_status: JobStatus = read_gc_json(&job, "status.json", "turn job status")?;
            if !job_status.state().is_terminal() {
                return Ok(true);
            }
        }
        crate::outbox::OriginOutbox::new(self.store, self.runner).retains(
            project,
            meta.task_id(),
            now_millis,
        )
    }

    fn task_id_has_active_work(
        &self,
        project: &str,
        task_id: TaskId,
        now_millis: u64,
    ) -> Result<bool, WorkerError> {
        let task = match self
            .store
            .open_directory(&format!("tasks/{project}/{task_id}"), false)
        {
            Ok(task) => task,
            Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        let meta: TaskMeta = read_gc_json(&task, "meta.json", "task metadata")?;
        let status: TaskStatus = read_gc_json(&task, "status.json", "task status")?;
        self.task_has_active_work(
            project,
            &meta,
            &status,
            LeaseService::new(self.store)
                .load()?
                .map(|lease| lease.job_id()),
            now_millis,
        )
    }

    fn collect_jobs(
        &self,
        request: &GcRequest,
        inventory: &GcInventory,
        candidates: &mut Vec<GcCandidate>,
        warnings: &mut Vec<String>,
    ) -> Result<(), WorkerError> {
        let jobs = self.store.open_directory("jobs", false)?;
        for raw_project in jobs.list_names()? {
            if is_rooted_namespace(&raw_project) {
                continue;
            }
            let project = match parse_digest_component(&raw_project, "job project") {
                Ok(project) => project,
                Err(_) => {
                    push_warning(warnings, "unreadable job project record");
                    continue;
                }
            };
            let project_dir = match jobs.open_child_directory(&relative(&project)?, false) {
                Ok(project_dir) => project_dir,
                Err(_) => {
                    push_warning(warnings, "unreadable job project record");
                    continue;
                }
            };
            for raw_worktree in project_dir.list_names()? {
                if is_rooted_namespace(&raw_worktree) {
                    continue;
                }
                let worktree = match parse_digest_component(&raw_worktree, "job worktree") {
                    Ok(worktree) => worktree,
                    Err(_) => {
                        push_warning(warnings, "unreadable job worktree record");
                        continue;
                    }
                };
                let worktree_dir =
                    match project_dir.open_child_directory(&relative(&worktree)?, false) {
                        Ok(worktree_dir) => worktree_dir,
                        Err(_) => {
                            push_warning(warnings, "unreadable job worktree record");
                            continue;
                        }
                    };
                for raw_job in worktree_dir.list_names()? {
                    if is_rooted_namespace(&raw_job) {
                        continue;
                    }
                    let job_name = match parse_utf8(&raw_job, "job directory") {
                        Ok(job_name) => job_name,
                        Err(_) => {
                            push_warning(warnings, "unreadable job record");
                            continue;
                        }
                    };
                    let job_id = match job_name.parse::<JobId>() {
                        Ok(job_id) => job_id,
                        Err(_) => {
                            push_warning(warnings, "inconsistent job record");
                            continue;
                        }
                    };
                    let identifier = format!("{project}/{worktree}/{job_id}");
                    let job = match worktree_dir.open_child_directory(&relative(&job_name)?, false)
                    {
                        Ok(job) => job,
                        Err(_) => {
                            push_record_warning(
                                warnings,
                                "job",
                                &identifier,
                                GC_REASON_UNREADABLE_RECORD,
                            );
                            continue;
                        }
                    };
                    let result = self.collect_job_record(
                        request,
                        inventory,
                        candidates,
                        &project,
                        &worktree,
                        job_id,
                        &identifier,
                        &job,
                    );
                    let Err(error) = result else {
                        continue;
                    };
                    if is_legacy_protocol_record(&job, "meta.json", &error)
                        || is_legacy_protocol_record(&job, "status.json", &error)
                    {
                        push_candidate(
                            candidates,
                            GcCandidate::new("job", identifier, 0, GC_REASON_LEGACY_PROTOCOL)?,
                        )?;
                    } else {
                        push_record_warning(
                            warnings,
                            "job",
                            &identifier,
                            record_failure_reason(&error),
                        );
                    }
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)] // Keeps the rooted record context explicit at the boundary.
    fn collect_job_record(
        &self,
        request: &GcRequest,
        inventory: &GcInventory,
        candidates: &mut Vec<GcCandidate>,
        project: &str,
        worktree: &str,
        job_id: JobId,
        identifier: &str,
        job: &RootedDir,
    ) -> Result<(), WorkerError> {
        let meta: JobMeta = read_gc_json(job, "meta.json", "job metadata")?;
        let status: JobStatus = read_gc_json(job, "status.json", "job status")?;
        if meta.job_id() != job_id || meta.project_id() != project || meta.worktree_id() != worktree
        {
            return Err(gc_metadata(
                &format!("jobs/{project}/{worktree}/{job_id}"),
                "job metadata does not match its rooted path",
            ));
        }
        let index = self.store.open_directory("job-index", false)?;
        let lock_namespace = self.store.open_directory("locks/jobs", false)?;
        if !index.entry_exists(&format!("{job_id}.json"))?
            || !lock_namespace.entry_exists(&format!("{job_id}.lock.json"))?
            || !lock_namespace.entry_exists(&job_id.to_string())?
        {
            return Err(gc_metadata(
                &format!("jobs/{project}/{worktree}/{job_id}"),
                "job record sibling state is incomplete",
            ));
        }
        if inventory.lease_uncertain || inventory.live_lease_job == Some(job_id) {
            return Ok(());
        }
        let mut mutable = false;
        for name in ["workspace", "home", "tmp", "execution.json"] {
            if job.entry_exists(name)? {
                mutable = true;
                break;
            }
        }
        if status.state().is_terminal()
            && !mutable
            && expired(
                request.now_millis(),
                status.updated_at_millis(),
                request.job_retention_millis(),
            )
        {
            push_candidate(
                candidates,
                GcCandidate::new(
                    "job",
                    identifier.to_owned(),
                    bounded_tree_size(job)?,
                    "job retention",
                )?,
            )?;
        }
        Ok(())
    }

    fn collect_mirrors(
        &self,
        request: &GcRequest,
        inventory: &GcInventory,
        candidates: &mut Vec<GcCandidate>,
        warnings: &mut Vec<String>,
    ) -> Result<(), WorkerError> {
        let repos = self.store.open_directory("repos", false)?;
        for raw_name in repos.list_names()? {
            if is_rooted_namespace(&raw_name) {
                continue;
            }
            let name = match parse_utf8(&raw_name, "mirror directory") {
                Ok(name) => name,
                Err(_) => {
                    push_warning(warnings, "unreadable mirror record");
                    continue;
                }
            };
            let project = match name.strip_suffix(".git") {
                Some(project) if is_lower_hex(project, 64) => project,
                _ => {
                    push_warning(warnings, "inconsistent mirror record");
                    continue;
                }
            };
            let result =
                self.collect_mirror_record(request, inventory, candidates, &repos, &name, project);
            if let Err(error) = result {
                push_record_warning(warnings, "mirror", project, record_failure_reason(&error));
            }
        }
        Ok(())
    }

    fn collect_mirror_record(
        &self,
        request: &GcRequest,
        inventory: &GcInventory,
        candidates: &mut Vec<GcCandidate>,
        repos: &RootedDir,
        name: &str,
        project: &str,
    ) -> Result<(), WorkerError> {
        let mirror = repos
            .open_child_directory(&relative(name)?, false)
            .map_err(WorkerError::Io)?;
        let refs = git_ref_names(self.runner, mirror.path())?;
        let ref_ages = git_task_ref_ages(self.runner, mirror.path())?;
        for (reference, created_at_millis) in ref_ages {
            let Some(task_text) = reference
                .strip_prefix("refs/heads/task/")
                .or_else(|| reference.strip_prefix("refs/mac-worker/bases/"))
            else {
                continue;
            };
            let task_id = task_text
                .parse::<TaskId>()
                .map_err(|_| gc_metadata("mirror ref", "task ref name is invalid"))?;
            if inventory.uncertain_task_projects.contains(project) {
                continue;
            }
            let identifier = format!("{project}/{task_id}");
            let protected_by_live_record = inventory.tasks.get(&identifier).is_some_and(|task| {
                task.active_work
                    || !task.state.is_terminal()
                    || !expired(
                        request.now_millis(),
                        task.updated_at_millis,
                        request.branch_retention_millis(),
                    )
            });
            if !protected_by_live_record
                && expired(
                    request.now_millis(),
                    created_at_millis,
                    request.branch_retention_millis(),
                )
                && !candidates.iter().any(|candidate| {
                    candidate.kind() == "branch" && candidate.identifier() == identifier
                })
            {
                push_candidate(
                    candidates,
                    GcCandidate::new("branch", identifier, 0, "branch retention")?,
                )?;
            }
        }
        if !refs.is_empty()
            || inventory.task_projects.contains(project)
            || inventory.uncertain_task_projects.contains(project)
            || !expired(
                request.now_millis(),
                rooted_modified_millis(&mirror)?,
                request.branch_retention_millis(),
            )
        {
            return Ok(());
        }
        push_candidate(
            candidates,
            GcCandidate::new("mirror", project, 0, "empty mirror retention")?,
        )?;
        Ok(())
    }

    fn apply_candidate(
        &self,
        candidate: &GcCandidate,
        inventory: &GcInventory,
        request: &GcRequest,
        warnings: &mut Vec<String>,
    ) -> Result<bool, WorkerError> {
        match candidate.kind() {
            "task" if candidate.reason() == GC_REASON_LEGACY_PROTOCOL => {
                push_record_warning(
                    warnings,
                    "task",
                    candidate.identifier(),
                    GC_REASON_LEGACY_PROTOCOL,
                );
                Ok(false)
            }
            "task" if candidate.reason() == "open task retention" => {
                let (project, task_id) = parse_task_identifier(candidate.identifier())?;
                if self.task_id_has_active_work(project, task_id, request.now_millis())? {
                    return Ok(false);
                }
                Ok(TaskStore::new(self.store, self.runner)
                    .close_for_retention(project, task_id, request.now_millis())?
                    .is_some())
            }
            "task" if candidate.reason() == "task metadata retention" => {
                let (project, task_id) = parse_task_identifier(candidate.identifier())?;
                let Some(snapshot) = inventory.tasks.get(candidate.identifier()) else {
                    return Ok(false);
                };
                if snapshot.state != TaskState::Closed
                    && snapshot.state != TaskState::Abandoned
                    && snapshot.state != TaskState::Lost
                {
                    return Ok(false);
                }
                if self.task_id_has_active_work(project, task_id, request.now_millis())? {
                    return Ok(false);
                }
                let task = match self
                    .store
                    .open_directory(&format!("tasks/{project}/{task_id}"), false)
                {
                    Ok(task) => task,
                    Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                        return Ok(false);
                    }
                    Err(error) => return Err(error),
                };
                let meta: TaskMeta = read_gc_json(&task, "meta.json", "task metadata")?;
                let status: TaskStatus = read_gc_json(&task, "status.json", "task status")?;
                if meta.project_id() != project
                    || meta.task_id() != task_id
                    || !status.state().is_terminal()
                    || !expired(
                        request.now_millis(),
                        status.updated_at_millis(),
                        request.job_retention_millis(),
                    )
                {
                    return Ok(false);
                }
                let parent = self
                    .store
                    .open_directory(&format!("tasks/{project}"), false)?;
                self.store
                    .remove_owned_child_committed(&parent, &task_id.to_string())?;
                Ok(true)
            }
            "branch" => self.apply_branch(candidate, inventory, request, warnings),
            "job" if candidate.reason() == GC_REASON_LEGACY_PROTOCOL => {
                push_record_warning(
                    warnings,
                    "job",
                    candidate.identifier(),
                    GC_REASON_LEGACY_PROTOCOL,
                );
                Ok(false)
            }
            "job" => self.apply_job(candidate, request, warnings),
            "mirror" => self.apply_mirror(candidate, inventory, request),
            _ => Err(gc_protocol(
                "GC_CANDIDATE_INVALID",
                "GC response contains an unsupported candidate",
            )),
        }
    }

    fn apply_branch(
        &self,
        candidate: &GcCandidate,
        inventory: &GcInventory,
        request: &GcRequest,
        warnings: &mut Vec<String>,
    ) -> Result<bool, WorkerError> {
        let (project, task_id) = parse_task_identifier(candidate.identifier())?;
        if let Some(snapshot) = inventory.tasks.get(candidate.identifier())
            && (snapshot.active_work
                || !snapshot.state.is_terminal()
                || !expired(
                    request.now_millis(),
                    snapshot.updated_at_millis,
                    request.branch_retention_millis(),
                ))
        {
            return Ok(false);
        }
        if let Some(snapshot) = inventory.tasks.get(candidate.identifier()) {
            let task = self
                .store
                .open_directory(&format!("tasks/{project}/{task_id}"), false)?;
            let meta: TaskMeta = read_gc_json(&task, "meta.json", "task metadata")?;
            let status: TaskStatus = read_gc_json(&task, "status.json", "task status")?;
            if self.task_has_active_work(
                project,
                &meta,
                &status,
                LeaseService::new(self.store)
                    .load()?
                    .map(|lease| lease.job_id()),
                request.now_millis(),
            )? {
                return Ok(false);
            }
            debug_assert_eq!(snapshot.state, status.state());
        }
        let mirror = match self.store.mirror_if_present(project)? {
            Some(mirror) => mirror,
            None => return Ok(false),
        };
        let branch = format!("refs/heads/task/{task_id}");
        let base = format!("refs/mac-worker/bases/{task_id}");
        let mut removed = false;
        for reference in [&branch, &base] {
            if git_ref_exists(self.runner, mirror.path(), reference)? {
                delete_git_ref(self.runner, mirror.path(), reference)?;
                removed = true;
            }
        }
        let delivery_prefix = format!("refs/mac-worker/delivery/{task_id}/");
        for reference in git_ref_names(self.runner, mirror.path())? {
            if reference.starts_with(&delivery_prefix) {
                delete_git_ref(self.runner, mirror.path(), &reference)?;
                removed = true;
            }
        }
        if removed && git_gc(self.runner, mirror.path()).is_err() {
            push_warning(warnings, "git gc was not completed for a retained mirror");
        }
        Ok(removed)
    }

    fn apply_job(
        &self,
        candidate: &GcCandidate,
        request: &GcRequest,
        warnings: &mut Vec<String>,
    ) -> Result<bool, WorkerError> {
        let (project, worktree, job_id) = parse_job_identifier(candidate.identifier())?;
        let parent = match self
            .store
            .open_directory(&format!("jobs/{project}/{worktree}"), false)
        {
            Ok(parent) => parent,
            Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        let job = match parent.open_child_directory(&relative(&job_id.to_string())?, false) {
            Ok(job) => job,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let meta: JobMeta = read_gc_json(&job, "meta.json", "job metadata")?;
        let status: JobStatus = read_gc_json(&job, "status.json", "job status")?;
        let index = self.store.open_directory("job-index", false)?;
        let lock_namespace = self.store.open_directory("locks/jobs", false)?;
        if !index.entry_exists(&format!("{job_id}.json"))?
            || !lock_namespace.entry_exists(&format!("{job_id}.lock.json"))?
            || !lock_namespace.entry_exists(&job_id.to_string())?
        {
            push_record_warning(
                warnings,
                "job",
                candidate.identifier(),
                GC_REASON_INCONSISTENT_RECORD,
            );
            return Ok(false);
        }
        if LeaseService::new(self.store)
            .load()?
            .is_some_and(|lease| lease.job_id() == job_id)
        {
            return Ok(false);
        }
        if meta.job_id() != job_id
            || meta.project_id() != project
            || meta.worktree_id() != worktree
            || !status.state().is_terminal()
            || !expired(
                request.now_millis(),
                status.updated_at_millis(),
                request.job_retention_millis(),
            )
        {
            return Ok(false);
        }
        for name in ["workspace", "home", "tmp", "execution.json"] {
            if job.entry_exists(name)? {
                return Ok(false);
            }
        }
        self.store
            .remove_owned_child_committed(&parent, &job_id.to_string())?;
        Ok(true)
    }

    fn apply_mirror(
        &self,
        candidate: &GcCandidate,
        inventory: &GcInventory,
        request: &GcRequest,
    ) -> Result<bool, WorkerError> {
        let project = candidate.identifier();
        if inventory.task_projects.contains(project)
            || inventory.uncertain_task_projects.contains(project)
            || !is_lower_hex(project, 64)
        {
            return Ok(false);
        }
        let repos = self.store.open_directory("repos", false)?;
        let name = format!("{project}.git");
        if !repos.entry_exists(&name)? {
            return Ok(false);
        }
        let mirror = repos
            .open_child_directory(&relative(&name)?, false)
            .map_err(WorkerError::Io)?;
        if !git_ref_names(self.runner, mirror.path())?.is_empty()
            || !expired(
                request.now_millis(),
                rooted_modified_millis(&mirror)?,
                request.branch_retention_millis(),
            )
        {
            return Ok(false);
        }
        self.store.remove_owned_child_committed(&repos, &name)?;
        Ok(true)
    }
}

#[derive(Default)]
struct GcInventory {
    /// Every task directory seen, for the herdr tab sweep.
    task_ids: BTreeSet<String>,
    /// Whether any task record shows a turn reported to herdr; the sweep
    /// runs only then, so a worker that never reported never opens the socket.
    herdr_reported: bool,
    live_lease_job: Option<JobId>,
    lease_uncertain: bool,
    tasks: BTreeMap<String, TaskSnapshot>,
    task_projects: BTreeSet<String>,
    uncertain_task_projects: BTreeSet<String>,
}

struct TaskSnapshot {
    state: TaskState,
    updated_at_millis: u64,
    active_work: bool,
}

fn candidate_rank(candidate: &GcCandidate) -> u8 {
    match candidate.kind() {
        "branch" => 0,
        "job" => 1,
        "task" => 2,
        "mirror" => 3,
        _ => 4,
    }
}

fn push_candidate(
    candidates: &mut Vec<GcCandidate>,
    candidate: GcCandidate,
) -> Result<(), WorkerError> {
    if candidates.len() >= MAX_GC_CANDIDATES {
        return Err(gc_protocol(
            "GC_CANDIDATE_LIMIT",
            "GC candidate count exceeds the bounded response limit",
        ));
    }
    candidates.push(candidate);
    Ok(())
}

fn push_warning(warnings: &mut Vec<String>, warning: &str) {
    if warnings.len() < 64 && !warnings.iter().any(|existing| existing == warning) {
        warnings.push(warning.to_owned());
    }
}

fn push_record_warning(warnings: &mut Vec<String>, kind: &str, identifier: &str, reason: &str) {
    let leaf = identifier.rsplit('/').next().unwrap_or(identifier);
    let warning = match reason {
        GC_REASON_INCONSISTENT_RECORD => format!("inconsistent {kind} record {leaf}"),
        GC_REASON_LEGACY_PROTOCOL => format!("legacy protocol {kind} record {leaf}"),
        _ => format!("unreadable {kind} record {leaf}"),
    };
    push_warning(warnings, &warning);
}

fn record_failure_reason(error: &WorkerError) -> &'static str {
    let message = error.to_string();
    if message.contains("does not match")
        || message.contains("is not a")
        || message.contains("missing")
        || message.contains("incomplete")
    {
        GC_REASON_INCONSISTENT_RECORD
    } else {
        GC_REASON_UNREADABLE_RECORD
    }
}

fn is_legacy_protocol_record(directory: &RootedDir, name: &str, error: &WorkerError) -> bool {
    error.to_string().contains("incompatible protocol version")
        || directory
            .read_private_regular(name, MAX_GC_FILE_BYTES)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .and_then(|value| value.get("protocol_version")?.as_u64())
            .is_some_and(|version| version < u64::from(PROTOCOL_VERSION))
}

fn expired(now_millis: u64, updated_at_millis: u64, retention_millis: u64) -> bool {
    now_millis.saturating_sub(updated_at_millis) >= retention_millis
}

fn parse_task_identifier(identifier: &str) -> Result<(&str, TaskId), WorkerError> {
    let (project, task) = identifier.split_once('/').ok_or_else(|| {
        gc_protocol(
            "GC_CANDIDATE_INVALID",
            "task candidate identifier is invalid",
        )
    })?;
    if !is_lower_hex(project, 64) || task.contains('/') {
        return Err(gc_protocol(
            "GC_CANDIDATE_INVALID",
            "task candidate identifier is invalid",
        ));
    }
    let task_id = task
        .parse::<TaskId>()
        .map_err(|_| gc_protocol("GC_CANDIDATE_INVALID", "task candidate task ID is invalid"))?;
    Ok((project, task_id))
}

fn parse_job_identifier(identifier: &str) -> Result<(&str, &str, JobId), WorkerError> {
    let mut components = identifier.split('/');
    let project = components.next().ok_or_else(|| {
        gc_protocol(
            "GC_CANDIDATE_INVALID",
            "job candidate identifier is invalid",
        )
    })?;
    let worktree = components.next().ok_or_else(|| {
        gc_protocol(
            "GC_CANDIDATE_INVALID",
            "job candidate identifier is invalid",
        )
    })?;
    let job = components.next().ok_or_else(|| {
        gc_protocol(
            "GC_CANDIDATE_INVALID",
            "job candidate identifier is invalid",
        )
    })?;
    if components.next().is_some() || !is_lower_hex(project, 64) || !is_lower_hex(worktree, 64) {
        return Err(gc_protocol(
            "GC_CANDIDATE_INVALID",
            "job candidate identifier is invalid",
        ));
    }
    let job_id = job
        .parse::<JobId>()
        .map_err(|_| gc_protocol("GC_CANDIDATE_INVALID", "job candidate job ID is invalid"))?;
    Ok((project, worktree, job_id))
}

fn parse_digest_component(raw: &[u8], context: &str) -> Result<String, WorkerError> {
    let value = parse_utf8(raw, context)?;
    if !is_lower_hex(&value, 64) {
        return Err(gc_metadata(context, "identifier is not a lowercase digest"));
    }
    Ok(value)
}

fn parse_utf8(raw: &[u8], context: &str) -> Result<String, WorkerError> {
    std::str::from_utf8(raw)
        .map(str::to_owned)
        .map_err(|_| gc_metadata(context, "rooted entry is not UTF-8"))
}

fn is_rooted_namespace(raw: &[u8]) -> bool {
    raw == b".mac-worker-rooted-fs"
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn read_gc_json<T: DeserializeOwned + Serialize>(
    directory: &RootedDir,
    name: &str,
    context: &str,
) -> Result<T, WorkerError> {
    let bytes = directory
        .read_private_regular(name, MAX_GC_FILE_BYTES)
        .map_err(|error| gc_metadata(context, &error.to_string()))?;
    let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
    let value = T::deserialize(&mut deserializer)
        .map_err(|error| gc_metadata(context, &format!("invalid JSON: {error}")))?;
    deserializer
        .end()
        .map_err(|error| gc_metadata(context, &format!("trailing JSON: {error}")))?;
    let canonical = serde_json::to_vec(&value)
        .map_err(|error| gc_metadata(context, &format!("canonicalization failed: {error}")))?;
    if canonical != bytes {
        return Err(gc_metadata(context, "JSON is not canonical"));
    }
    Ok(value)
}

fn bounded_tree_size(root: &RootedDir) -> Result<u64, WorkerError> {
    fn visit(directory: &RootedDir, total: &mut u64) -> Result<(), WorkerError> {
        for raw_name in directory.list_names()? {
            if is_rooted_namespace(&raw_name) {
                continue;
            }
            let name = parse_utf8(&raw_name, "GC tree entry")?;
            // Worktree .git directories are implementation-owned Git data;
            // they are deliberately excluded from the bounded size estimate
            // and are removed only as part of the rooted task tree cleanup.
            if name == ".git" {
                continue;
            }
            let relative = relative(&name)?;
            match directory.open_child_directory(&relative, false) {
                Ok(child) => visit(&child, total)?,
                Err(error) if error.kind() == io::ErrorKind::NotADirectory => {
                    let inspection = directory.inspect(&relative)?;
                    if inspection.kind == EntryKind::RegularFile {
                        *total = total.saturating_add(inspection.size).min(MAX_GC_SIZE_BYTES);
                    }
                }
                Err(error) => return Err(error.into()),
            }
            if *total >= MAX_GC_SIZE_BYTES {
                *total = MAX_GC_SIZE_BYTES;
                return Ok(());
            }
        }
        Ok(())
    }

    let mut total = 0;
    visit(root, &mut total)?;
    Ok(total)
}

pub(crate) fn rooted_modified_millis(root: &RootedDir) -> Result<u64, WorkerError> {
    let metadata = root.root_metadata()?;
    modified_millis(metadata.st_mtime, metadata.st_mtime_nsec)
        .ok_or_else(|| gc_metadata("GC directory", "directory modification time is invalid"))
}

fn modified_millis(seconds: i64, nanoseconds: i64) -> Option<u64> {
    if seconds < 0 || !(0..1_000_000_000).contains(&nanoseconds) {
        return None;
    }
    (seconds as u64)
        .checked_mul(1000)?
        .checked_add((nanoseconds as u64) / 1_000_000)
}

pub(crate) fn git_ref_names(
    runner: &dyn ProcessRunner,
    path: &Path,
) -> Result<Vec<String>, WorkerError> {
    let result = run_git(runner, path, ["for-each-ref", "--format=%(refname)"])?;
    if result.stdout.len() > MAX_GC_REF_BYTES {
        return Err(gc_protocol(
            "GC_REF_LIMIT",
            "mirror refs exceed the bounded GC scan limit",
        ));
    }
    let text = std::str::from_utf8(&result.stdout)
        .map_err(|_| gc_protocol("GC_REF_INVALID", "mirror refs are not UTF-8"))?;
    let mut refs = Vec::new();
    for reference in text.lines().filter(|line| !line.is_empty()) {
        if reference.len() > 1024 || reference.bytes().any(|byte| byte.is_ascii_control()) {
            return Err(gc_protocol("GC_REF_INVALID", "mirror ref name is invalid"));
        }
        refs.push(reference.to_owned());
        if refs.len() > MAX_GC_CANDIDATES {
            return Err(gc_protocol(
                "GC_REF_LIMIT",
                "mirror ref count exceeds the bounded GC scan limit",
            ));
        }
    }
    Ok(refs)
}

fn git_task_ref_ages(
    runner: &dyn ProcessRunner,
    path: &Path,
) -> Result<Vec<(String, u64)>, WorkerError> {
    let result = run_git(
        runner,
        path,
        [
            "for-each-ref",
            "--format=%(refname)%09%(creatordate:unix)",
            "refs/heads/task",
            "refs/mac-worker/bases",
        ],
    )?;
    if result.stdout.len() > MAX_GC_REF_BYTES {
        return Err(gc_protocol(
            "GC_REF_LIMIT",
            "mirror task refs exceed the bounded GC scan limit",
        ));
    }
    let text = std::str::from_utf8(&result.stdout)
        .map_err(|_| gc_protocol("GC_REF_INVALID", "mirror task refs are not UTF-8"))?;
    let mut refs = Vec::new();
    for line in text.lines().filter(|line| !line.is_empty()) {
        let (reference, seconds) = line
            .split_once('\t')
            .ok_or_else(|| gc_protocol("GC_REF_INVALID", "mirror task ref age is missing"))?;
        if reference.len() > 1024
            || reference.bytes().any(|byte| byte.is_ascii_control())
            || seconds.is_empty()
        {
            return Err(gc_protocol(
                "GC_REF_INVALID",
                "mirror task ref age is invalid",
            ));
        }
        let seconds = seconds
            .parse::<u64>()
            .map_err(|_| gc_protocol("GC_REF_INVALID", "mirror task ref age is invalid"))?;
        let created_at_millis = seconds
            .checked_mul(1000)
            .ok_or_else(|| gc_protocol("GC_REF_INVALID", "mirror task ref age is too large"))?;
        refs.push((reference.to_owned(), created_at_millis));
        if refs.len() > MAX_GC_CANDIDATES {
            return Err(gc_protocol(
                "GC_REF_LIMIT",
                "mirror task ref count exceeds the bounded GC scan limit",
            ));
        }
    }
    Ok(refs)
}

fn git_ref_exists(
    runner: &dyn ProcessRunner,
    path: &Path,
    reference: &str,
) -> Result<bool, WorkerError> {
    let result =
        run_git_allow_failure(runner, path, ["show-ref", "--verify", "--quiet", reference])?;
    match result.status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err(gc_git_error("checking a mirror ref failed")),
    }
}

fn delete_git_ref(
    runner: &dyn ProcessRunner,
    path: &Path,
    reference: &str,
) -> Result<(), WorkerError> {
    let result = run_git(runner, path, ["update-ref", "-d", reference])?;
    if result.status.success() {
        Ok(())
    } else {
        Err(gc_git_error("deleting a task ref failed"))
    }
}

fn git_gc(runner: &dyn ProcessRunner, path: &Path) -> Result<(), WorkerError> {
    let result = run_git(runner, path, ["gc", "--prune=now", "--quiet"])?;
    if result.status.success() {
        Ok(())
    } else {
        Err(gc_git_error("mirror Git GC failed"))
    }
}

fn run_git<const N: usize>(
    runner: &dyn ProcessRunner,
    path: &Path,
    args: [&str; N],
) -> Result<crate::process::ProcessResult, WorkerError> {
    let result = runner.run(&git_request(path, args))?;
    if result.status.success() {
        Ok(result)
    } else {
        Err(gc_git_error("mirror Git operation failed"))
    }
}

fn run_git_allow_failure<const N: usize>(
    runner: &dyn ProcessRunner,
    path: &Path,
    args: [&str; N],
) -> Result<crate::process::ProcessResult, WorkerError> {
    runner.run(&git_request(path, args))
}

fn git_request<const N: usize>(path: &Path, args: [&str; N]) -> ProcessRequest {
    let mut command_args = vec![OsString::from("--git-dir"), path.as_os_str().to_os_string()];
    command_args.extend(args.into_iter().map(OsString::from));
    ProcessRequest {
        program: OsString::from(GC_GIT_PROGRAM),
        args: command_args,
        environment: vec![
            (
                OsString::from("GIT_CONFIG_GLOBAL"),
                OsString::from("/dev/null"),
            ),
            (OsString::from("GIT_CONFIG_NOSYSTEM"), OsString::from("1")),
            (OsString::from("GIT_TERMINAL_PROMPT"), OsString::from("0")),
        ],
        environment_remove: GIT_ENVIRONMENT_REMOVALS
            .iter()
            .map(OsString::from)
            .collect(),
        stdin: None,
        policy: ProcessPolicy {
            stdout_limit: GC_GIT_OUTPUT_LIMIT,
            stderr_limit: GC_GIT_OUTPUT_LIMIT,
            deadline: GC_GIT_DEADLINE,
        },
        isolate_parent_environment: false,
    }
}

fn gc_protocol(code: &'static str, message: &str) -> WorkerError {
    WorkerError::Protocol(format!("{code}: {message}"))
}

fn gc_metadata(context: &str, message: &str) -> WorkerError {
    WorkerError::Protocol(format!("GC_METADATA_INVALID: {context}: {message}"))
}

fn gc_git_error(message: &str) -> WorkerError {
    WorkerError::Git {
        code: "GC_GIT_FAILED",
        message: message.to_owned(),
    }
}

fn relative(path: &str) -> Result<RelativePath, WorkerError> {
    RelativePath::parse(path.as_bytes()).map_err(|error| WorkerError::Protocol(error.to_string()))
}

/// Best effort: close herdr tabs whose task directory is gone.  Tabs of
/// tasks that still exist are left alone; `task close` removes those.
fn sweep_herdr_tabs(task_ids: &BTreeSet<String>) {
    let Some(home) = std::env::var_os("HOME").filter(|home| !home.is_empty()) else {
        return;
    };
    let live = |short: &str| task_ids.iter().any(|task_id| task_id.starts_with(short));
    let _ = crate::herdr_reporter::HerdrReporter::for_home(std::path::Path::new(&home))
        .sweep_orphans(&live);
}
