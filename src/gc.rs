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
            let (inventory, mut candidates) = self.collect(request)?;
            candidates.sort_by(|left, right| {
                candidate_rank(left)
                    .cmp(&candidate_rank(right))
                    .then_with(|| left.identifier.cmp(&right.identifier))
                    .then_with(|| left.reason.cmp(&right.reason))
            });
            let mut applied = Vec::new();
            let mut warnings = Vec::new();
            if request.apply() {
                for candidate in &candidates {
                    if self.apply_candidate(candidate, &inventory, request, &mut warnings)? {
                        applied.push(candidate.clone());
                    }
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

    fn collect(&self, request: &GcRequest) -> Result<(GcInventory, Vec<GcCandidate>), WorkerError> {
        let mut inventory = GcInventory::default();
        let mut candidates = Vec::new();
        self.collect_tasks(request, &mut inventory, &mut candidates)?;
        self.collect_jobs(request, &mut candidates)?;
        self.collect_mirrors(request, &inventory, &mut candidates)?;
        Ok((inventory, candidates))
    }

    fn collect_tasks(
        &self,
        request: &GcRequest,
        inventory: &mut GcInventory,
        candidates: &mut Vec<GcCandidate>,
    ) -> Result<(), WorkerError> {
        let tasks = self.store.open_directory("tasks", false)?;
        for raw_project in tasks.list_names()? {
            if is_rooted_namespace(&raw_project) {
                continue;
            }
            let project = parse_digest_component(&raw_project, "task project")?;
            let project_dir = tasks
                .open_child_directory(&relative(&project)?, false)
                .map_err(WorkerError::Io)?;
            for raw_task in project_dir.list_names()? {
                if is_rooted_namespace(&raw_task) {
                    continue;
                }
                let task_name = parse_utf8(&raw_task, "task directory")?;
                let task_id = task_name.parse::<TaskId>().map_err(|_| {
                    gc_metadata("task directory", "task directory name is not a task ID")
                })?;
                let task = project_dir
                    .open_child_directory(&relative(&task_name)?, false)
                    .map_err(WorkerError::Io)?;
                let meta: TaskMeta = read_gc_json(&task, "meta.json", "task metadata")?;
                let status: TaskStatus = read_gc_json(&task, "status.json", "task status")?;
                if meta.project_id() != project || meta.task_id() != task_id {
                    return Err(gc_metadata(
                        &format!("tasks/{project}/{task_name}"),
                        "task metadata does not match its rooted path",
                    ));
                }
                let branch_ref = format!("refs/heads/task/{task_id}");
                let base_ref = format!("refs/mac-worker/bases/{task_id}");
                let (branch_exists, base_exists) = if status.state().is_terminal() {
                    if let Some(mirror) = self.store.mirror_if_present(&project)? {
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
                let size_bytes = bounded_tree_size(&task)?;
                let identifier = format!("{project}/{task_id}");
                inventory.task_projects.insert(project.clone());
                inventory.tasks.insert(
                    identifier.clone(),
                    TaskSnapshot {
                        state: status.state(),
                        updated_at_millis: status.updated_at_millis(),
                    },
                );

                if status.state() == TaskState::Open
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
                            identifier.clone(),
                            size_bytes,
                            "open task retention",
                        )?,
                    )?;
                }

                if status.state().is_terminal()
                    && (branch_exists || base_exists)
                    && expired(
                        request.now_millis(),
                        status.updated_at_millis(),
                        request.branch_retention_millis(),
                    )
                {
                    push_candidate(
                        candidates,
                        GcCandidate::new("branch", identifier.clone(), 0, "branch retention")?,
                    )?;
                }

                let metadata_expired = status.state().is_terminal()
                    && expired(
                        request.now_millis(),
                        status.updated_at_millis(),
                        request.job_retention_millis(),
                    );
                if metadata_expired {
                    push_candidate(
                        candidates,
                        GcCandidate::new(
                            "task",
                            identifier,
                            size_bytes,
                            "task metadata retention",
                        )?,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn collect_jobs(
        &self,
        request: &GcRequest,
        candidates: &mut Vec<GcCandidate>,
    ) -> Result<(), WorkerError> {
        let jobs = self.store.open_directory("jobs", false)?;
        for raw_project in jobs.list_names()? {
            if is_rooted_namespace(&raw_project) {
                continue;
            }
            let project = parse_digest_component(&raw_project, "job project")?;
            let project_dir = jobs
                .open_child_directory(&relative(&project)?, false)
                .map_err(WorkerError::Io)?;
            for raw_worktree in project_dir.list_names()? {
                if is_rooted_namespace(&raw_worktree) {
                    continue;
                }
                let worktree = parse_digest_component(&raw_worktree, "job worktree")?;
                let worktree_dir = project_dir
                    .open_child_directory(&relative(&worktree)?, false)
                    .map_err(WorkerError::Io)?;
                for raw_job in worktree_dir.list_names()? {
                    if is_rooted_namespace(&raw_job) {
                        continue;
                    }
                    let job_name = parse_utf8(&raw_job, "job directory")?;
                    let job_id = job_name.parse::<JobId>().map_err(|_| {
                        gc_metadata("job directory", "job directory name is not a job ID")
                    })?;
                    let job = worktree_dir
                        .open_child_directory(&relative(&job_name)?, false)
                        .map_err(WorkerError::Io)?;
                    let meta: JobMeta = read_gc_json(&job, "meta.json", "job metadata")?;
                    let status: JobStatus = read_gc_json(&job, "status.json", "job status")?;
                    if meta.job_id() != job_id
                        || meta.project_id() != project
                        || meta.worktree_id() != worktree
                    {
                        return Err(gc_metadata(
                            &format!("jobs/{project}/{worktree}/{job_id}"),
                            "job metadata does not match its rooted path",
                        ));
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
                                format!("{project}/{worktree}/{job_id}"),
                                bounded_tree_size(&job)?,
                                "job retention",
                            )?,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn collect_mirrors(
        &self,
        request: &GcRequest,
        inventory: &GcInventory,
        candidates: &mut Vec<GcCandidate>,
    ) -> Result<(), WorkerError> {
        let repos = self.store.open_directory("repos", false)?;
        for raw_name in repos.list_names()? {
            if is_rooted_namespace(&raw_name) {
                continue;
            }
            let name = parse_utf8(&raw_name, "mirror directory")?;
            let project = name.strip_suffix(".git").ok_or_else(|| {
                gc_metadata("mirror directory", "mirror name does not end in .git")
            })?;
            if !is_lower_hex(project, 64) {
                return Err(gc_metadata(
                    "mirror directory",
                    "mirror project ID is invalid",
                ));
            }
            let mirror = repos
                .open_child_directory(&relative(&name)?, false)
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
                let Ok(task_id) = task_text.parse::<TaskId>() else {
                    return Err(gc_metadata("mirror ref", "task ref name is invalid"));
                };
                let identifier = format!("{project}/{task_id}");
                let protected_by_live_record =
                    inventory.tasks.get(&identifier).is_some_and(|task| {
                        !task.state.is_terminal()
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
                || !expired(
                    request.now_millis(),
                    rooted_modified_millis(&mirror)?,
                    request.branch_retention_millis(),
                )
            {
                continue;
            }
            push_candidate(
                candidates,
                GcCandidate::new("mirror", project, 0, "empty mirror retention")?,
            )?;
        }
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
            "task" if candidate.reason() == "open task retention" => {
                let (project, task_id) = parse_task_identifier(candidate.identifier())?;
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
            "job" => self.apply_job(candidate, request),
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
            && (!snapshot.state.is_terminal()
                || !expired(
                    request.now_millis(),
                    snapshot.updated_at_millis,
                    request.branch_retention_millis(),
                ))
        {
            return Ok(false);
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
        if removed && git_gc(self.runner, mirror.path()).is_err() {
            push_warning(warnings, "git gc was not completed for a retained mirror");
        }
        Ok(removed)
    }

    fn apply_job(&self, candidate: &GcCandidate, request: &GcRequest) -> Result<bool, WorkerError> {
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
        if inventory.task_projects.contains(project) || !is_lower_hex(project, 64) {
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
    tasks: BTreeMap<String, TaskSnapshot>,
    task_projects: BTreeSet<String>,
}

struct TaskSnapshot {
    state: TaskState,
    updated_at_millis: u64,
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
