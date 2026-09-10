use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::fd::AsRawFd,
    os::unix::{
        ffi::{OsStrExt, OsStringExt},
        fs::OpenOptionsExt,
        process::ExitStatusExt,
    },
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::Arc,
    time::Duration,
};

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    error::WorkerError,
    gc::{GcCandidate, GcReport, git_ref_names},
    inputs::{InputSelector, RelativePath, SelectedInputKind, SelectionFailure},
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner},
    project::ProjectContext,
    project_config::ProjectSettings,
    rooted_fs::{EntryKind, RootedDir},
    task::{BaseOid, GitIdentity, TaskId},
};

const GIT_PROGRAM: &str = "/usr/bin/git";
const GIT_OUTPUT_LIMIT: usize = 64 * 1024 * 1024;
const GIT_DEADLINE: Duration = Duration::from_secs(30);
const BLOB_BATCH_MAX_BYTES: usize = 1024 * 1024;
const BLOB_BATCH_MAX_OBJECTS: usize = 64;
const GET_MARK_LINE_BYTES: usize = 41;
const BASE_COMMIT_MESSAGE: &str = "mac-worker: task base";
const ZERO_OID: &str = "0000000000000000000000000000000000000000";
pub(crate) const GIT_ENVIRONMENT_REMOVALS: &[&str] = &[
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

pub(crate) fn apply_isolated_git_environment(command: &mut Command) {
    for name in GIT_ENVIRONMENT_REMOVALS {
        command.env_remove(name);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BaseKind {
    Committed,
    Wip,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirtyReport {
    pub modified: usize,
    pub added: usize,
    pub deleted: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseCommit {
    oid: BaseOid,
    kind: BaseKind,
    head_oid: BaseOid,
    branch: Option<String>,
    dirty: DirtyReport,
}

impl BaseCommit {
    pub fn oid(&self) -> &BaseOid {
        &self.oid
    }

    pub fn kind(&self) -> BaseKind {
        self.kind
    }

    pub fn head_oid(&self) -> &BaseOid {
        &self.head_oid
    }

    pub fn branch(&self) -> Option<&str> {
        self.branch.as_deref()
    }

    pub fn dirty(&self) -> &DirtyReport {
        &self.dirty
    }

    pub fn from_origin(oid: BaseOid) -> Self {
        Self {
            head_oid: oid.clone(),
            oid,
            kind: BaseKind::Committed,
            branch: None,
            dirty: DirtyReport {
                modified: 0,
                added: 0,
                deleted: 0,
            },
        }
    }

    /// Frozen base already present in the transfer cache. Resume must not
    /// recapture HEAD or the worktree.
    pub fn from_pinned(oid: BaseOid, wip: bool) -> Self {
        Self {
            head_oid: oid.clone(),
            oid,
            kind: if wip {
                BaseKind::Wip
            } else {
                BaseKind::Committed
            },
            branch: None,
            dirty: DirtyReport {
                modified: 0,
                added: 0,
                deleted: 0,
            },
        }
    }
}

/// Task 3 will reuse or alias this receipt as `FetchReceipt`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportReceipt {
    head: BaseOid,
    local_ref: String,
}

impl ImportReceipt {
    pub(crate) fn new(head: BaseOid, local_ref: String) -> Self {
        Self { head, local_ref }
    }

    pub fn head(&self) -> &BaseOid {
        &self.head
    }

    pub fn local_ref(&self) -> &str {
        &self.local_ref
    }
}

pub struct TransferRepo {
    path: PathBuf,
    repo_id: String,
    alternates_target: PathBuf,
    user_alternates: bool,
    _repo_lock: Arc<File>,
}

impl fmt::Debug for TransferRepo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransferRepo")
            .field("path", &self.path)
            .field("repo_id", &self.repo_id)
            .field("alternates_target", &self.alternates_target)
            .finish_non_exhaustive()
    }
}

impl Clone for TransferRepo {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            repo_id: self.repo_id.clone(),
            alternates_target: self.alternates_target.clone(),
            user_alternates: self.user_alternates,
            _repo_lock: self._repo_lock.clone(),
        }
    }
}

impl PartialEq for TransferRepo {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
            && self.repo_id == other.repo_id
            && self.alternates_target == other.alternates_target
            && self.user_alternates == other.user_alternates
    }
}

impl Eq for TransferRepo {}

/// Garbage collection for the MacBook-side transfer namespace.  A transfer
/// repository is eligible only when it has no base refs, contains only stale
/// result refs, and no currently protected local task refers to it.  The
/// optional protected set is supplied by the client-state owner so in-flight
/// import/publication operations remain live even before a base ref is
/// installed.
pub struct TransferGc<'a> {
    cache_root: &'a Path,
    runner: &'a dyn ProcessRunner,
    protected_repo_ids: BTreeSet<String>,
}

impl<'a> TransferGc<'a> {
    pub fn new(cache_root: &'a Path, runner: &'a dyn ProcessRunner) -> Self {
        Self {
            cache_root,
            runner,
            protected_repo_ids: BTreeSet::new(),
        }
    }

    pub fn with_protected_repo_ids(
        mut self,
        protected_repo_ids: impl IntoIterator<Item = String>,
    ) -> Self {
        self.protected_repo_ids = protected_repo_ids.into_iter().collect();
        self
    }

    pub fn preview_at(&self, now_millis: u64) -> Result<GcReport, WorkerError> {
        self.run(now_millis, false)
    }

    pub fn apply_at(&self, now_millis: u64) -> Result<GcReport, WorkerError> {
        self.run(now_millis, true)
    }

    fn run(&self, now_millis: u64, apply: bool) -> Result<GcReport, WorkerError> {
        let parent_path = self.cache_root.join("transfer");
        let parent = match RootedDir::open(&parent_path) {
            Ok(parent) => parent,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(GcReport::new(apply, Vec::new(), Vec::new(), Vec::new()));
            }
            Err(error) => return Err(error.into()),
        };
        let mut candidates = Vec::new();
        let mut warnings = Vec::new();
        for raw_name in parent.list_names()? {
            if raw_name.as_slice() == b".mac-worker-rooted-fs" || is_transfer_lock_entry(&raw_name)
            {
                continue;
            }
            match self.collect_transfer_record(&parent, &raw_name, now_millis, &mut candidates) {
                Ok(TransferRecordScan::Scanned) => {}
                Ok(TransferRecordScan::InUse(repo_id)) => {
                    push_warning(&mut warnings, &in_use_warning(&repo_id))
                }
                Err(error) => push_transfer_record_warning(&mut warnings, &error),
            }
        }
        candidates.sort_by(|left, right| left.identifier().cmp(right.identifier()));
        let mut applied = Vec::new();
        if apply {
            for candidate in &candidates {
                if self.apply_candidate(&parent, candidate, now_millis, &mut warnings)? {
                    applied.push(candidate.clone());
                }
            }
        }
        Ok(GcReport::new(apply, candidates, applied, warnings))
    }

    fn collect_transfer_record(
        &self,
        parent: &RootedDir,
        raw_name: &[u8],
        now_millis: u64,
        candidates: &mut Vec<GcCandidate>,
    ) -> Result<TransferRecordScan, WorkerError> {
        let name = std::str::from_utf8(raw_name).map_err(|_| {
            WorkerError::Protocol("GC_METADATA_INVALID: transfer repo name is not UTF-8".into())
        })?;
        let Some(repo_id) = name.strip_suffix(".git") else {
            return Err(WorkerError::Protocol(
                "GC_METADATA_INVALID: transfer repo name does not end in .git".into(),
            ));
        };
        if !is_lower_hex(repo_id, 64) {
            return Err(WorkerError::Protocol(
                "GC_METADATA_INVALID: transfer repo ID is invalid".into(),
            ));
        }
        if self.protected_repo_ids.contains(repo_id) {
            return Ok(TransferRecordScan::Scanned);
        }
        let Some(_repo_lock) = try_lock_transfer_repo_exclusive(parent, repo_id)? else {
            return Ok(TransferRecordScan::InUse(repo_id.to_owned()));
        };
        let repo = parent
            .open_child_directory(&relative_transfer(name)?, false)
            .map_err(WorkerError::Io)?;
        let refs = git_ref_names(self.runner, repo.path())?;
        if !transfer_refs_are_collectable(&refs) {
            return Ok(TransferRecordScan::Scanned);
        }
        let modified = crate::gc::rooted_modified_millis(&repo)?;
        if now_millis.saturating_sub(modified) < crate::gc::BRANCH_RETENTION_MILLIS {
            return Ok(TransferRecordScan::Scanned);
        }
        candidates.push(GcCandidate::new(
            "transfer_repo",
            repo_id,
            0,
            "transfer repository retention",
        )?);
        Ok(TransferRecordScan::Scanned)
    }

    fn apply_candidate(
        &self,
        parent: &RootedDir,
        candidate: &GcCandidate,
        now_millis: u64,
        warnings: &mut Vec<String>,
    ) -> Result<bool, WorkerError> {
        let repo_id = candidate.identifier();
        if !is_lower_hex(repo_id, 64) || self.protected_repo_ids.contains(repo_id) {
            return Ok(false);
        }
        let Some(_repo_lock) = try_lock_transfer_repo_exclusive(parent, repo_id)? else {
            push_warning(warnings, &in_use_warning(repo_id));
            return Ok(false);
        };
        let name = format!("{repo_id}.git");
        if !parent.entry_exists(&name)? {
            return Ok(false);
        }
        let repo = parent
            .open_child_directory(&relative_transfer(&name)?, false)
            .map_err(WorkerError::Io)?;
        let modified = crate::gc::rooted_modified_millis(&repo)?;
        if !transfer_refs_are_collectable(&git_ref_names(self.runner, repo.path())?)
            || now_millis.saturating_sub(modified) < crate::gc::BRANCH_RETENTION_MILLIS
        {
            return Ok(false);
        }
        parent.remove_owned_child(&name)?;
        Ok(true)
    }
}

/// Reported once per repository and GC run when it has a live handle.
fn in_use_warning(repo_id: &str) -> String {
    format!(
        "transfer repository {} in use by a task; skipped",
        &repo_id[..12.min(repo_id.len())]
    )
}

/// What one directory entry of the transfer namespace yielded to GC.
enum TransferRecordScan {
    Scanned,
    InUse(String),
}

fn push_transfer_record_warning(warnings: &mut Vec<String>, error: &WorkerError) {
    let warning =
        if error.to_string().contains("name") || error.to_string().contains("ID is invalid") {
            "inconsistent transfer repository record"
        } else {
            "unreadable transfer repository record"
        };
    push_warning(warnings, warning);
}

fn push_warning(warnings: &mut Vec<String>, warning: &str) {
    if warnings.len() < 64 && !warnings.iter().any(|existing| existing == warning) {
        warnings.push(warning.to_owned());
    }
}

/// Every user of a transfer repository holds its lock shared for the
/// lifetime of the handle.  Users never exclude each other: indexes, base
/// refs, and result refs are per task, objects are content addressed, and
/// automatic maintenance is off.  The lock exists so that transfer GC, which
/// takes it exclusively, can never delete a repository with a live handle.
fn lock_transfer_repo_shared(parent: &RootedDir, repo_id: &str) -> Result<Arc<File>, WorkerError> {
    flock_transfer_file(parent, &format!("{repo_id}.lock"), libc::LOCK_SH)
}

/// Transfer GC never waits for a repository: a live handle means the
/// repository is in use, and GC reports that and moves on.
fn try_lock_transfer_repo_exclusive(
    parent: &RootedDir,
    repo_id: &str,
) -> Result<Option<Arc<File>>, WorkerError> {
    let file = parent.open_private_lock(&format!("{repo_id}.lock"))?;
    // A shared holder can be a process in the middle of forking a child, which
    // references the lock's open file description until it execs.  A few short
    // retries see through that window without ever waiting for a real holder.
    for attempt in 0..IN_USE_ATTEMPTS {
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            return Ok(Some(Arc::new(file)));
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::WouldBlock {
            return Err(WorkerError::Io(error));
        }
        if attempt + 1 < IN_USE_ATTEMPTS {
            std::thread::sleep(IN_USE_RETRY_DELAY);
        }
    }
    Ok(None)
}

const IN_USE_ATTEMPTS: u32 = 4;
const IN_USE_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(25);

/// First creation of a repository is the one step that must not run twice.
/// It runs under a short exclusive lock on a separate file, so a shared
/// holder never upgrades the lock it already holds.
fn lock_transfer_repo_init(parent: &RootedDir, repo_id: &str) -> Result<Arc<File>, WorkerError> {
    flock_transfer_file(parent, &format!("{repo_id}.init.lock"), libc::LOCK_EX)
}

fn flock_transfer_file(
    parent: &RootedDir,
    name: &str,
    operation: libc::c_int,
) -> Result<Arc<File>, WorkerError> {
    let file = parent.open_private_lock(name)?;
    if unsafe { libc::flock(file.as_raw_fd(), operation) } != 0 {
        return Err(WorkerError::Io(io::Error::last_os_error()));
    }
    Ok(Arc::new(file))
}

fn is_transfer_lock_entry(raw_name: &[u8]) -> bool {
    let Ok(name) = std::str::from_utf8(raw_name) else {
        return false;
    };
    let Some(stem) = name.strip_suffix(".lock") else {
        return false;
    };
    let repo_id = stem.strip_suffix(".init").unwrap_or(stem);
    is_lower_hex(repo_id, 64)
}

fn relative_transfer(path: &str) -> Result<RelativePath, WorkerError> {
    RelativePath::parse(path.as_bytes()).map_err(|error| WorkerError::Protocol(error.to_string()))
}

fn transfer_refs_are_collectable(refs: &[String]) -> bool {
    refs.iter()
        .all(|reference| reference.starts_with("refs/mac-worker/results/"))
}

impl TransferRepo {
    pub fn resolve_base_oid(
        runner: &dyn ProcessRunner,
        context: &ProjectContext,
        reference: &str,
    ) -> Result<BaseOid, WorkerError> {
        resolve_commit(runner, context, reference)
    }

    pub fn open_or_create(cache_root: &Path, user_common_dir: &Path) -> Result<Self, WorkerError> {
        let repo_id = repo_id_for(user_common_dir)?;
        let alternates_target =
            fs::canonicalize(user_common_dir.join("objects")).map_err(|_| {
                git_error(
                    "BASE_UNAVAILABLE",
                    "the user repository object directory is missing",
                )
            })?;
        let cache_root = if cache_root.is_absolute() {
            cache_root.to_path_buf()
        } else {
            fs::canonicalize(cache_root).map_err(WorkerError::Io)?
        };
        let transfer_parent = cache_root.join("transfer");
        let transfer_ns = open_or_create_owner_only_dir(&transfer_parent)?;
        let repo_lock = lock_transfer_repo_shared(&transfer_ns, &repo_id)?;
        let path = transfer_parent.join(format!("{repo_id}.git"));
        let repo_dir = match open_initialized_transfer_repo(&path)? {
            Some(repo_dir) => repo_dir,
            None => {
                let _init_lock = lock_transfer_repo_init(&transfer_ns, &repo_id)?;
                match open_initialized_transfer_repo(&path)? {
                    Some(repo_dir) => repo_dir,
                    None => {
                        let repo_dir = open_or_create_owner_only_dir(&path)?;
                        run_system_git(
                            None,
                            &[
                                OsString::from("init"),
                                OsString::from("--bare"),
                                path.as_os_str().to_os_string(),
                            ],
                            None,
                            None,
                        )?;
                        let _scratch =
                            repo_dir.open_child_directory(&relative("scratch")?, true)?;
                        repo_dir
                    }
                }
            }
        };
        chmod_owner_only(&repo_dir)?;
        let _scratch = repo_dir.open_child_directory(&relative("scratch")?, true)?;
        let info = open_owner_only_subdir(&path, &["objects", "info"])?;
        write_alternates_atomic(&info, &alternates_target)?;
        let transfer = Self {
            path,
            repo_id,
            alternates_target,
            user_alternates: true,
            _repo_lock: repo_lock,
        };
        transfer.verify_alternates()?;
        Ok(transfer)
    }

    pub fn repo_id(&self) -> &str {
        &self.repo_id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Domain-separated controller Git cache identity. FLOW and the hidden
    /// host helpers must call this with the same `project_id` / `worktree_id`.
    ///
    /// `cache_id = SHA-256( CONTROLLER_TRANSFER_CACHE_DOMAIN || project_id || 0x00 || worktree_id )`
    pub const CONTROLLER_TRANSFER_CACHE_DOMAIN: &'static [u8] =
        b"mac-worker/controller-transfer-cache\0";

    pub fn controller_transfer_cache_id(
        project_id: &str,
        worktree_id: &str,
    ) -> Result<String, WorkerError> {
        if !is_lower_hex(project_id, 64) || !is_lower_hex(worktree_id, 64) {
            return Err(git_error(
                "BASE_UNAVAILABLE",
                "controller cache identity is not a lowercase SHA-256 pair",
            ));
        }
        let mut hasher = Sha256::new();
        hasher.update(Self::CONTROLLER_TRANSFER_CACHE_DOMAIN);
        hasher.update(project_id.as_bytes());
        hasher.update([0u8]);
        hasher.update(worktree_id.as_bytes());
        Ok(format!("{:x}", hasher.finalize()))
    }

    pub fn controller_transfer_git_path(
        cache_root: &Path,
        project_id: &str,
        worktree_id: &str,
    ) -> Result<PathBuf, WorkerError> {
        let cache_id = Self::controller_transfer_cache_id(project_id, worktree_id)?;
        Ok(cache_root
            .join("controller-transfer")
            .join(format!("{cache_id}.git")))
    }

    /// FLOW checkout / materialization / import / reopen hook.
    ///
    /// Open the same Git directory hidden `controller-receive-pack` /
    /// `controller-upload-pack` resolve from `PathLayout.cache` plus the
    /// registered logical `project_id` and `worktree_id`. Do not call
    /// [`TransferRepo::open_or_create`] with a laptop `common_dir` for a
    /// controller-registered context: that would use `transfer/<physical>`
    /// instead of `controller-transfer/<logical>`.
    pub fn open_or_create_controller_cache(
        cache_root: &Path,
        project_id: &str,
        worktree_id: &str,
    ) -> Result<Self, WorkerError> {
        Self::open_controller_cache_inner(cache_root, project_id, worktree_id, true)
    }

    /// Hidden-helper open. Fails closed when prepare has not created the
    /// cache yet, or when the git directory was removed.
    pub fn open_controller_cache(
        cache_root: &Path,
        project_id: &str,
        worktree_id: &str,
    ) -> Result<Self, WorkerError> {
        Self::open_controller_cache_inner(cache_root, project_id, worktree_id, false)
    }

    fn open_controller_cache_inner(
        cache_root: &Path,
        project_id: &str,
        worktree_id: &str,
        create: bool,
    ) -> Result<Self, WorkerError> {
        let repo_id = Self::controller_transfer_cache_id(project_id, worktree_id)?;
        let cache_root = if cache_root.is_absolute() {
            cache_root.to_path_buf()
        } else {
            fs::canonicalize(cache_root).map_err(WorkerError::Io)?
        };
        let transfer_parent = cache_root.join("controller-transfer");
        let path = transfer_parent.join(format!("{repo_id}.git"));
        if !create && open_initialized_transfer_repo(&path)?.is_none() {
            return Err(git_error(
                "BASE_UNAVAILABLE",
                "controller transfer cache is missing",
            ));
        }
        let transfer_ns = open_or_create_owner_only_dir(&transfer_parent)?;
        let repo_lock = lock_transfer_repo_shared(&transfer_ns, &repo_id)?;
        let repo_dir = match open_initialized_transfer_repo(&path)? {
            Some(repo_dir) => repo_dir,
            None if create => {
                let _init_lock = lock_transfer_repo_init(&transfer_ns, &repo_id)?;
                match open_initialized_transfer_repo(&path)? {
                    Some(repo_dir) => repo_dir,
                    None => {
                        let repo_dir = open_or_create_owner_only_dir(&path)?;
                        run_system_git(
                            None,
                            &[
                                OsString::from("init"),
                                OsString::from("--bare"),
                                path.as_os_str().to_os_string(),
                            ],
                            None,
                            None,
                        )?;
                        let _scratch =
                            repo_dir.open_child_directory(&relative("scratch")?, true)?;
                        repo_dir
                    }
                }
            }
            None => {
                return Err(git_error(
                    "BASE_UNAVAILABLE",
                    "controller transfer cache is missing",
                ));
            }
        };
        chmod_owner_only(&repo_dir)?;
        let _scratch = repo_dir.open_child_directory(&relative("scratch")?, true)?;
        let _info = open_owner_only_subdir(&path, &["objects", "info"])?;
        Ok(Self {
            path: path.clone(),
            repo_id,
            alternates_target: path.join("objects"),
            user_alternates: false,
            _repo_lock: repo_lock,
        })
    }

    pub fn frozen_controller_result_ref(
        request_id: &str,
        turn_id: &str,
    ) -> Result<String, WorkerError> {
        if !is_lower_hex(request_id, 32) || !is_lower_hex(turn_id, 32) {
            return Err(git_error(
                "BASE_UNAVAILABLE",
                "controller result pin identity is not a lowercase UUID pair",
            ));
        }
        Ok(format!(
            "refs/mac-worker/controller-results/{request_id}/{turn_id}"
        ))
    }

    /// Immutable request/turn result pin. Does not touch
    /// `refs/mac-worker/results/<task_id>`. Same owned-graph pin engine as
    /// [`Self::pin_frozen_source`]; the namespace is the only difference.
    pub fn pin_controller_result(
        &self,
        runner: &dyn ProcessRunner,
        request_id: &str,
        turn_id: &str,
        oid: &BaseOid,
    ) -> Result<String, WorkerError> {
        let name = Self::frozen_controller_result_ref(request_id, turn_id)?;
        self.pin_owned_commit(runner, &name, oid)?;
        Ok(name)
    }

    /// Cap for the first-slice control-frame bundle only. Ordinary repositories
    /// must stream through GitTransport SSH (`host receive-pack` /
    /// `host upload-pack`, or controller-owned `controller-receive-pack` /
    /// `controller-upload-pack`). Do not raise this cap to fit a real repo, and
    /// do not put pack bytes in the 1 MiB RPC body on the final path.
    pub const FROZEN_BUNDLE_MAX_BYTES: usize = 1024 * 1024;

    /// Immutable request-scoped source ref. `git bundle create` with a raw
    /// OID and stdout (`-`) can refuse an empty bundle; pin this named ref
    /// first, then bundle the ref.
    pub fn frozen_request_ref(request_id: &str) -> Result<String, WorkerError> {
        if !is_lower_hex(request_id, 32) {
            return Err(git_error(
                "BASE_UNAVAILABLE",
                "frozen request identity is not a lowercase UUID",
            ));
        }
        Ok(format!("refs/mac-worker/requests/{request_id}"))
    }

    /// Immutable DAG freeze / from-binding ref. Same owned-graph pin engine as
    /// [`Self::pin_frozen_source`]; the namespace is the only difference.
    pub fn frozen_dag_ref(run_id: &str, batch_id: &str) -> Result<String, WorkerError> {
        if !is_lower_hex(run_id, 32) {
            return Err(git_error(
                "BASE_UNAVAILABLE",
                "DAG run identity is not a lowercase UUID",
            ));
        }
        if !is_dag_batch_id(batch_id) {
            return Err(git_error(
                "BASE_UNAVAILABLE",
                "DAG batch id must be a short lowercase identifier",
            ));
        }
        Ok(format!("refs/mac-worker/dag/{run_id}/{batch_id}"))
    }

    pub fn pin_frozen_source(
        &self,
        runner: &dyn ProcessRunner,
        request_id: &str,
        oid: &BaseOid,
    ) -> Result<String, WorkerError> {
        let name = Self::frozen_request_ref(request_id)?;
        self.pin_owned_commit(runner, &name, oid)?;
        Ok(name)
    }

    /// Pins a frozen or imported-parent commit under a validated request, DAG,
    /// or controller-result ref. Copies the reachable graph into transfer-owned
    /// objects without mutating live `objects/info/alternates`, then
    /// create-or-same CAS plus object/refstorage fsync. Same OID retries succeed;
    /// a different OID conflicts. [`Self::has_object`] is not proof of
    /// owned-graph closure.
    pub fn pin_object(
        &self,
        runner: &dyn ProcessRunner,
        name: &str,
        oid: &BaseOid,
    ) -> Result<(), WorkerError> {
        self.pin_owned_commit(runner, name, oid)
    }

    pub fn unpin_object(&self, runner: &dyn ProcessRunner, name: &str) -> Result<(), WorkerError> {
        self.verify_alternates()?;
        validate_owned_pin_ref(name)?;
        if !self.has_ref(name) {
            return Ok(());
        }
        self.update_ref(runner, name, None)
    }

    /// Object visibility through the transfer repository, including live
    /// alternates. This is not owned-graph proof; use [`Self::pin_object`]
    /// or [`Self::pin_frozen_source`] before treating a pin as durable.
    pub fn has_object(&self, oid: &BaseOid) -> bool {
        run_system_git(
            Some(&self.path),
            &[
                OsString::from("cat-file"),
                OsString::from("-e"),
                OsString::from(oid.as_str()),
            ],
            None,
            None,
        )
        .is_ok()
    }

    /// First-slice fixture transfer: a git bundle of one frozen commit that
    /// already fits in a 1 MiB controller RPC frame. This is not ordinary
    /// repository transfer.
    pub fn bundle_frozen_source(
        &self,
        runner: &dyn ProcessRunner,
        request_id: &str,
        oid: &BaseOid,
    ) -> Result<Vec<u8>, WorkerError> {
        let source_ref = self.pin_frozen_source(runner, request_id, oid)?;
        self.with_scratch_regular(&[], |scratch_dir, name, path| {
            self.transfer_git(
                runner,
                &[
                    OsString::from("bundle"),
                    OsString::from("create"),
                    path.as_os_str().to_os_string(),
                    OsString::from(&source_ref),
                ],
                None,
                None,
            )?;
            let bytes = read_scratch_regular(scratch_dir, name)?;
            if bytes.is_empty() {
                return Err(git_error(
                    "BASE_UNAVAILABLE",
                    "frozen commit bundle was empty",
                ));
            }
            if bytes.len() > Self::FROZEN_BUNDLE_MAX_BYTES {
                return Err(git_error(
                    "BASE_UNAVAILABLE",
                    "frozen commit bundle exceeds the 1 MiB first-slice control-frame cap",
                ));
            }
            Ok(bytes)
        })
    }

    pub fn import_frozen_source(
        &self,
        runner: &dyn ProcessRunner,
        request_id: &str,
        bundle: &[u8],
        oid: &BaseOid,
    ) -> Result<(), WorkerError> {
        if bundle.is_empty() || bundle.len() > Self::FROZEN_BUNDLE_MAX_BYTES {
            return Err(git_error(
                "BASE_UNAVAILABLE",
                "frozen commit bundle is empty or exceeds the 1 MiB first-slice control-frame cap",
            ));
        }
        let source_ref = Self::frozen_request_ref(request_id)?;
        let incoming = format!("refs/mac-worker/incoming/{}", Uuid::new_v4().simple());
        let fetched = self.with_scratch_regular(bundle, |scratch_dir, name, path| {
            let written = read_scratch_regular(scratch_dir, name)?;
            if written.len() != bundle.len() || written != bundle {
                return Err(git_error(
                    "BASE_UNAVAILABLE",
                    "frozen commit bundle was truncated before unbundle",
                ));
            }
            self.transfer_git(
                runner,
                &[
                    OsString::from("-c"),
                    OsString::from("core.fsyncObjectFiles=true"),
                    OsString::from("bundle"),
                    OsString::from("verify"),
                    path.as_os_str().to_os_string(),
                ],
                None,
                None,
            )?;
            self.require_advertised_frozen_head(runner, path, &source_ref, oid)?;
            self.transfer_git(
                runner,
                &[
                    OsString::from("-c"),
                    OsString::from("core.fsyncObjectFiles=true"),
                    OsString::from("fetch"),
                    OsString::from("--no-write-fetch-head"),
                    OsString::from("--no-tags"),
                    path.as_os_str().to_os_string(),
                    OsString::from(format!("{source_ref}:{incoming}")),
                ],
                None,
                None,
            )?;
            Ok(())
        });
        let pinned = fetched.and_then(|()| {
            self.require_frozen_commit(runner, oid)?;
            self.materialize_owned_graph(runner, oid)?;
            self.require_owned_frozen_commit(runner, oid)?;
            self.pin_request_ref_cas(runner, &source_ref, oid)
        });
        self.delete_ref_if_present(runner, &incoming);
        pinned
    }

    pub fn contains_commit(
        &self,
        runner: &dyn ProcessRunner,
        oid: &BaseOid,
    ) -> Result<bool, WorkerError> {
        match self.object_type(runner, oid)? {
            Some(kind) if kind == "commit" => Ok(true),
            Some(_) | None => Ok(false),
        }
    }

    pub fn require_frozen_commit(
        &self,
        runner: &dyn ProcessRunner,
        oid: &BaseOid,
    ) -> Result<(), WorkerError> {
        match self.object_type(runner, oid)? {
            Some(kind) if kind == "commit" => Ok(()),
            Some(_) => Err(git_error("BASE_UNAVAILABLE", "frozen base is not a commit")),
            None => Err(git_error(
                "BASE_UNAVAILABLE",
                "frozen commit is missing from the transfer repository",
            )),
        }
    }

    /// Pin a frozen OID as the task base without recapturing HEAD or WIP.
    pub fn attach_pinned_base(
        &self,
        runner: &dyn ProcessRunner,
        task_id: TaskId,
        oid: &BaseOid,
        wip: bool,
    ) -> Result<BaseCommit, WorkerError> {
        self.require_owned_frozen_commit(runner, oid)?;
        self.update_ref(runner, &base_ref(task_id), Some(oid.as_str()))?;
        Ok(BaseCommit::from_pinned(oid.clone(), wip))
    }

    /// Copy the reachable graph into this repository's own object store.
    /// Does not mutate `objects/info/alternates`; concurrent transfer users
    /// keep their live alternate.
    pub(crate) fn materialize_owned_graph(
        &self,
        runner: &dyn ProcessRunner,
        oid: &BaseOid,
    ) -> Result<(), WorkerError> {
        if self.owned_graph_complete(runner, oid)? {
            return Ok(());
        }
        let pack = self.transfer_git(
            runner,
            &[
                OsString::from("-c"),
                OsString::from("core.fsyncObjectFiles=true"),
                OsString::from("pack-objects"),
                OsString::from("--revs"),
                OsString::from("--stdout"),
            ],
            None,
            Some(format!("{oid}\n").into_bytes()),
        )?;
        if pack.stdout.is_empty() {
            return Err(git_error(
                "BASE_UNAVAILABLE",
                "frozen reachable graph pack was empty",
            ));
        }
        let pack_bytes = pack.stdout.len();
        self.transfer_git(
            runner,
            &[
                OsString::from("-c"),
                OsString::from("core.fsyncObjectFiles=true"),
                OsString::from("index-pack"),
                OsString::from("--stdin"),
                OsString::from("--strict"),
            ],
            None,
            Some(pack.stdout),
        )?;
        self.require_owned_graph_view(runner, oid, "after pack+index-pack", Some(pack_bytes))
    }

    pub(crate) fn require_owned_frozen_commit(
        &self,
        runner: &dyn ProcessRunner,
        oid: &BaseOid,
    ) -> Result<(), WorkerError> {
        self.require_owned_graph_view(runner, oid, "require_owned_frozen_commit", None)
    }

    fn require_owned_graph_view(
        &self,
        runner: &dyn ProcessRunner,
        oid: &BaseOid,
        stage: &'static str,
        pack_bytes: Option<usize>,
    ) -> Result<(), WorkerError> {
        self.with_owned_object_view(|isolate| {
            let cat = git_at(
                runner,
                isolate,
                &[
                    OsString::from("cat-file"),
                    OsString::from("-t"),
                    OsString::from(oid.as_str()),
                ],
                None,
            );
            let rev = git_at(
                runner,
                isolate,
                &[
                    OsString::from("rev-list"),
                    OsString::from("--objects"),
                    OsString::from(oid.as_str()),
                ],
                None,
            );
            let cat_ok = cat
                .as_ref()
                .ok()
                .is_some_and(|result| String::from_utf8_lossy(&result.stdout).trim() == "commit");
            if cat_ok && rev.is_ok() {
                return Ok(());
            }
            Err(git_error(
                "BASE_UNAVAILABLE",
                format!(
                    "frozen graph is not fully present in transfer-owned objects stage={stage} oid={oid} pack_bytes={pack_bytes:?} transfer={} isolate={} cwd={:?} transfer_packs={} isolate_packs={} isolate_alternates={} cat-file={} rev-list={}",
                    self.path.display(),
                    isolate.display(),
                    std::env::current_dir().ok(),
                    pack_listing(&self.path.join("objects/pack")),
                    pack_listing(&isolate.join("objects/pack")),
                    isolate.join("objects/info/alternates").is_file(),
                    describe_git_outcome(&cat),
                    describe_git_outcome(&rev),
                ),
            ))
        })
    }

    fn owned_graph_complete(
        &self,
        runner: &dyn ProcessRunner,
        oid: &BaseOid,
    ) -> Result<bool, WorkerError> {
        self.with_owned_object_view(|isolate| {
            match git_at(
                runner,
                isolate,
                &[
                    OsString::from("cat-file"),
                    OsString::from("-t"),
                    OsString::from(oid.as_str()),
                ],
                None,
            ) {
                Ok(result) => {
                    let kind = String::from_utf8(result.stdout)
                        .map_err(|_| {
                            git_error("BASE_UNAVAILABLE", "owned object type was not UTF-8")
                        })?
                        .trim()
                        .to_owned();
                    if kind != "commit" {
                        return Ok(false);
                    }
                }
                Err(error) if error.public_code() == "BASE_UNAVAILABLE" => {
                    if std::env::var_os("MAC_WORKER_OWNED_GRAPH_TRACE").is_some() {
                        eprintln!(
                            "owned-graph cat-file unavailable isolate={} cwd={:?} error={error}",
                            isolate.display(),
                            std::env::current_dir().ok()
                        );
                    }
                    return Ok(false);
                }
                Err(error) => return Err(error),
            }
            match git_at(
                runner,
                isolate,
                &[
                    OsString::from("rev-list"),
                    OsString::from("--objects"),
                    OsString::from(oid.as_str()),
                ],
                None,
            ) {
                Ok(_) => Ok(true),
                Err(error) if error.public_code() == "BASE_UNAVAILABLE" => {
                    if std::env::var_os("MAC_WORKER_OWNED_GRAPH_TRACE").is_some() {
                        eprintln!(
                            "owned-graph rev-list unavailable isolate={} cwd={:?} error={error}",
                            isolate.display(),
                            std::env::current_dir().ok()
                        );
                    }
                    Ok(false)
                }
                Err(error) => Err(error),
            }
        })
    }

    fn with_owned_object_view<T>(
        &self,
        work: impl FnOnce(&Path) -> Result<T, WorkerError>,
    ) -> Result<T, WorkerError> {
        let scratch = self.path.join("scratch");
        fs::create_dir_all(&scratch).map_err(WorkerError::Io)?;
        let isolate = scratch.join(format!("owned-{}", Uuid::new_v4().simple()));
        fs::create_dir(&isolate).map_err(WorkerError::Io)?;
        let result = (|| {
            run_system_git(
                None,
                &[
                    OsString::from("init"),
                    OsString::from("-q"),
                    OsString::from("--bare"),
                    isolate.as_os_str().to_os_string(),
                ],
                None,
                None,
            )?;
            link_owned_objects(&self.path, &isolate)?;
            work(&isolate)
        })();
        let _ = fs::remove_dir_all(&isolate);
        result
    }

    fn object_type(
        &self,
        runner: &dyn ProcessRunner,
        oid: &BaseOid,
    ) -> Result<Option<String>, WorkerError> {
        match self.transfer_git(
            runner,
            &[
                OsString::from("cat-file"),
                OsString::from("-t"),
                oid.to_string().into(),
            ],
            None,
            None,
        ) {
            Ok(result) => {
                let kind = String::from_utf8(result.stdout)
                    .map_err(|_| git_error("BASE_UNAVAILABLE", "object type was not UTF-8"))?
                    .trim()
                    .to_owned();
                if kind.is_empty() {
                    return Ok(None);
                }
                Ok(Some(kind))
            }
            Err(error) if error.public_code() == "BASE_UNAVAILABLE" => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn require_advertised_frozen_head(
        &self,
        runner: &dyn ProcessRunner,
        bundle: &Path,
        source_ref: &str,
        oid: &BaseOid,
    ) -> Result<(), WorkerError> {
        let result = self.transfer_git(
            runner,
            &[
                OsString::from("bundle"),
                OsString::from("list-heads"),
                bundle.as_os_str().to_os_string(),
            ],
            None,
            None,
        )?;
        let stdout = String::from_utf8(result.stdout).map_err(|_| {
            git_error(
                "BASE_UNAVAILABLE",
                "frozen bundle advertised heads were not UTF-8",
            )
        })?;
        let mut advertised_oid = None;
        for line in stdout.lines() {
            if line.is_empty() {
                continue;
            }
            let Some((head, name)) = line.split_once(' ') else {
                return Err(git_error(
                    "BASE_UNAVAILABLE",
                    "frozen bundle advertised an unreadable head",
                ));
            };
            if name != source_ref {
                continue;
            }
            if advertised_oid.is_some() {
                return Err(git_error(
                    "BASE_UNAVAILABLE",
                    "frozen bundle advertised the request ref more than once",
                ));
            }
            advertised_oid = Some(head.to_owned());
        }
        match advertised_oid {
            Some(head) if is_lower_hex(&head, 40) && head == oid.as_str() => Ok(()),
            Some(_) => Err(git_error(
                "BASE_UNAVAILABLE",
                "frozen bundle advertised a different commit for the request ref",
            )),
            None => Err(git_error(
                "BASE_UNAVAILABLE",
                "frozen bundle does not advertise the request ref",
            )),
        }
    }

    fn delete_ref_if_present(&self, runner: &dyn ProcessRunner, name: &str) {
        let _ = self.transfer_git(
            runner,
            &[
                OsString::from("update-ref"),
                OsString::from("-d"),
                OsString::from(name),
            ],
            None,
            None,
        );
    }

    fn pin_owned_commit(
        &self,
        runner: &dyn ProcessRunner,
        name: &str,
        oid: &BaseOid,
    ) -> Result<(), WorkerError> {
        self.verify_alternates()?;
        validate_owned_pin_ref(name)?;
        self.require_frozen_commit(runner, oid)?;
        self.materialize_owned_graph(runner, oid)?;
        self.require_owned_frozen_commit(runner, oid)?;
        self.pin_request_ref_cas(runner, name, oid)
    }

    pub(crate) fn pin_request_ref_cas(
        &self,
        runner: &dyn ProcessRunner,
        name: &str,
        oid: &BaseOid,
    ) -> Result<(), WorkerError> {
        for _ in 0..16 {
            match self.read_ref_oid(runner, name)? {
                Some(existing) if existing.as_str() == oid.as_str() => {
                    self.sync_frozen_source(name)?;
                    return Ok(());
                }
                Some(_) => return Err(frozen_source_conflict()),
                None => match self.create_ref_exclusive(runner, name, oid) {
                    Ok(()) => {
                        self.sync_frozen_source(name)?;
                        return Ok(());
                    }
                    Err(error) if error.public_code() == "BASE_UNAVAILABLE" => continue,
                    Err(error) => return Err(error),
                },
            }
        }
        match self.read_ref_oid(runner, name)? {
            Some(existing) if existing.as_str() == oid.as_str() => {
                self.sync_frozen_source(name)?;
                Ok(())
            }
            Some(_) => Err(frozen_source_conflict()),
            None => Err(git_error(
                "BASE_UNAVAILABLE",
                "could not pin the owned commit ref",
            )),
        }
    }

    fn create_ref_exclusive(
        &self,
        runner: &dyn ProcessRunner,
        name: &str,
        oid: &BaseOid,
    ) -> Result<(), WorkerError> {
        self.transfer_git(
            runner,
            &[
                OsString::from("-c"),
                OsString::from("core.fsyncObjectFiles=true"),
                OsString::from("update-ref"),
                OsString::from(name),
                OsString::from(oid.as_str()),
                OsString::from(ZERO_OID),
            ],
            None,
            None,
        )?;
        Ok(())
    }

    pub(crate) fn read_ref_oid(
        &self,
        runner: &dyn ProcessRunner,
        name: &str,
    ) -> Result<Option<BaseOid>, WorkerError> {
        match self.transfer_git(
            runner,
            &[
                OsString::from("rev-parse"),
                OsString::from("--verify"),
                OsString::from("--end-of-options"),
                OsString::from(name),
            ],
            None,
            None,
        ) {
            Ok(result) => parse_oid(&result).map(Some),
            Err(error) if error.public_code() == "BASE_UNAVAILABLE" => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn sync_frozen_source(&self, name: &str) -> Result<(), WorkerError> {
        fsync_object_store(&self.path)?;
        self.fsync_frozen_ref_storage(name)?;
        fsync_directory(&self.path)
    }

    fn fsync_frozen_ref_storage(&self, name: &str) -> Result<(), WorkerError> {
        let relative_name = name
            .strip_prefix("refs/")
            .map(|rest| format!("refs/{rest}"))
            .unwrap_or_else(|| name.to_owned());
        let root = RootedDir::open(&self.path)?;
        match root.inspect(&relative(&relative_name)?) {
            Ok(inspection) => {
                if inspection.kind != EntryKind::RegularFile {
                    return Err(git_error(
                        "BASE_UNAVAILABLE",
                        "owned pin ref is not a regular file",
                    ));
                }
                fsync_regular_file(&self.path.join(&relative_name))?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let packed = root.inspect(&relative("packed-refs")?).map_err(|error| {
                    if error.kind() == io::ErrorKind::NotFound {
                        git_error(
                            "BASE_UNAVAILABLE",
                            "owned pin ref is missing from loose and packed storage",
                        )
                    } else {
                        WorkerError::Io(error)
                    }
                })?;
                if packed.kind != EntryKind::RegularFile {
                    return Err(git_error(
                        "BASE_UNAVAILABLE",
                        "packed-refs is not a regular file",
                    ));
                }
                fsync_regular_file(&self.path.join("packed-refs"))?;
            }
            Err(error) => return Err(WorkerError::Io(error)),
        }
        let mut parent = Path::new(&relative_name);
        while let Some(dir) = parent.parent() {
            let path = self.path.join(dir);
            if path.is_dir() {
                fsync_directory(&path)?;
            }
            if dir == Path::new("refs") {
                break;
            }
            parent = dir;
        }
        Ok(())
    }

    pub fn verify_alternates(&self) -> Result<(), WorkerError> {
        if !self.user_alternates {
            return Ok(());
        }
        let root = RootedDir::open(&self.path)?;
        let mut inspection = root
            .inspect(&relative("objects/info/alternates")?)
            .map_err(|_| {
                git_error(
                    "BASE_UNAVAILABLE",
                    "the transfer repository alternates file is missing",
                )
            })?;
        if inspection.kind != EntryKind::RegularFile {
            return Err(git_error(
                "BASE_UNAVAILABLE",
                "the transfer repository alternates file is missing",
            ));
        }
        let mut bytes = Vec::new();
        inspection.read_to_end(&mut bytes)?;
        let contents = String::from_utf8(bytes).map_err(|_| {
            git_error(
                "BASE_UNAVAILABLE",
                "the transfer repository alternates file is missing",
            )
        })?;
        let target = PathBuf::from(contents.trim());
        if !target.is_dir() {
            return Err(git_error(
                "BASE_UNAVAILABLE",
                "the transfer repository alternates target is missing",
            ));
        }
        if target != self.alternates_target {
            return Err(git_error(
                "BASE_UNAVAILABLE",
                "the transfer repository alternates target does not match the recorded object directory",
            ));
        }
        Ok(())
    }

    pub fn resolve_base(
        &self,
        runner: &dyn ProcessRunner,
        context: &ProjectContext,
        reference: &str,
    ) -> Result<BaseCommit, WorkerError> {
        self.verify_alternates()?;
        reject_merge_in_progress(runner, context)?;
        let oid = resolve_commit(runner, context, reference)?;
        let head_oid = resolve_commit(runner, context, "HEAD")?;
        let branch = resolve_branch_name(runner, context, reference)?;
        let dirty = dirty_report(runner, context)?;
        Ok(BaseCommit {
            oid,
            kind: BaseKind::Committed,
            head_oid,
            branch,
            dirty,
        })
    }

    pub fn build_wip_base(
        &self,
        runner: &dyn ProcessRunner,
        context: &ProjectContext,
        task_id: TaskId,
        settings: &ProjectSettings,
        identity: &GitIdentity,
    ) -> Result<BaseCommit, WorkerError> {
        self.build_wip_base_with_hook(runner, context, task_id, settings, identity, &|| Ok(()))
    }

    pub fn build_wip_base_with_hook(
        &self,
        runner: &dyn ProcessRunner,
        context: &ProjectContext,
        task_id: TaskId,
        settings: &ProjectSettings,
        identity: &GitIdentity,
        hook: &dyn Fn() -> Result<(), WorkerError>,
    ) -> Result<BaseCommit, WorkerError> {
        self.verify_alternates()?;
        reject_merge_in_progress(runner, context)?;
        let head_oid = resolve_commit(runner, context, "HEAD")?;
        let branch = resolve_branch_name(runner, context, "HEAD")?;
        let mut blob_oids = BTreeMap::<[u8; 32], String>::new();
        let first = self.capture_tree(runner, context, settings, &mut blob_oids)?;
        hook()?;
        let second = self.capture_tree(runner, context, settings, &mut blob_oids)?;
        if first.tree != second.tree {
            return Err(WorkerError::Snapshot {
                code: "SNAPSHOT_CHANGED",
                message: "worktree selection changed between captures".into(),
            });
        }
        let commit = self.commit_tree(runner, &first.tree, &head_oid, identity)?;
        self.update_ref(runner, &base_ref(task_id), Some(commit.as_str()))?;
        Ok(BaseCommit {
            oid: commit,
            kind: BaseKind::Wip,
            head_oid,
            branch,
            dirty: first.dirty,
        })
    }

    pub fn check_sensitive_tree(
        &self,
        runner: &dyn ProcessRunner,
        base: &BaseOid,
        settings: &ProjectSettings,
    ) -> Result<(), WorkerError> {
        self.verify_alternates()?;
        let output = self.transfer_git(
            runner,
            &[
                OsString::from("ls-tree"),
                OsString::from("-r"),
                OsString::from("--name-only"),
                OsString::from("-z"),
                OsString::from(base.as_str()),
            ],
            None,
            None,
        )?;
        let allow: BTreeSet<String> = settings.snapshot.allow_sensitive.iter().cloned().collect();
        let mut blocked = Vec::new();
        for path in nul_records(&output.stdout) {
            let parsed = RelativePath::parse(path).map_err(|failure| WorkerError::Snapshot {
                code: failure.code,
                message: failure.message,
            })?;
            if is_sensitive(&parsed) && !allow.contains(parsed.as_str()) {
                blocked.push(parsed.as_str().to_owned());
            }
        }
        if !blocked.is_empty() {
            return Err(WorkerError::Snapshot {
                code: "SENSITIVE_PATH",
                message: "sensitive inputs require an exact allowlist entry".into(),
            });
        }
        Ok(())
    }

    pub fn release_base(
        &self,
        runner: &dyn ProcessRunner,
        task_id: TaskId,
    ) -> Result<(), WorkerError> {
        self.verify_alternates()?;
        let name = base_ref(task_id);
        if !self.has_ref(&name) {
            return Ok(());
        }
        self.update_ref(runner, &name, None)
    }

    pub fn import_result(
        &self,
        runner: &dyn ProcessRunner,
        user_common_dir: &Path,
        worker: &str,
        task_id: TaskId,
    ) -> Result<ImportReceipt, WorkerError> {
        if repo_id_for(user_common_dir)? != self.repo_id {
            return Err(git_error(
                "BASE_UNAVAILABLE",
                "the transfer repository is bound to a different user repository",
            ));
        }
        self.verify_alternates()?;
        if !is_safe_worker_ref_component(worker) {
            return Err(task_config("worker name is invalid"));
        }
        let local_ref = format!("refs/remotes/mac-worker/{worker}/task/{task_id}");
        let source = format!("+refs/mac-worker/results/{task_id}:{local_ref}");
        user_git(
            runner,
            user_common_dir,
            &[
                OsString::from("-c"),
                OsString::from("gc.auto=0"),
                OsString::from("-c"),
                OsString::from("core.logAllRefUpdates=false"),
                OsString::from("fetch"),
                OsString::from("--no-write-fetch-head"),
                self.path.as_os_str().to_os_string(),
                OsString::from(source),
            ],
            false,
        )?;
        let head = parse_oid(&user_git(
            runner,
            user_common_dir,
            &[
                OsString::from("rev-parse"),
                OsString::from("--verify"),
                OsString::from(local_ref.as_str()),
            ],
            false,
        )?)?;
        Ok(ImportReceipt { head, local_ref })
    }

    /// Fetch a verified request/turn pin into the user remotes namespace.
    /// `source_ref` must already name `expected_oid` in this transfer repo.
    pub fn import_result_from_ref(
        &self,
        runner: &dyn ProcessRunner,
        user_common_dir: &Path,
        worker: &str,
        task_id: TaskId,
        source_ref: &str,
        expected_oid: &BaseOid,
    ) -> Result<ImportReceipt, WorkerError> {
        if self.user_alternates && repo_id_for(user_common_dir)? != self.repo_id {
            return Err(git_error(
                "BASE_UNAVAILABLE",
                "the transfer repository is bound to a different user repository",
            ));
        }
        self.verify_alternates()?;
        if !is_safe_worker_ref_component(worker) {
            return Err(task_config("worker name is invalid"));
        }
        if !source_ref.starts_with("refs/mac-worker/controller-results/")
            || source_ref.contains("..")
            || source_ref.contains('\0')
        {
            return Err(git_error(
                "BASE_UNAVAILABLE",
                "controller result import ref is not an immutable request/turn pin",
            ));
        }
        let pinned = self.read_ref_oid(runner, source_ref)?.ok_or_else(|| {
            git_error(
                "BASE_UNAVAILABLE",
                "controller result pin is missing from the transfer repository",
            )
        })?;
        if pinned.as_str() != expected_oid.as_str() {
            return Err(git_error(
                "BASE_UNAVAILABLE",
                "controller result pin does not match the imported object",
            ));
        }
        let local_ref = format!("refs/remotes/mac-worker/{worker}/task/{task_id}");
        let source = format!("+{source_ref}:{local_ref}");
        user_git(
            runner,
            user_common_dir,
            &[
                OsString::from("-c"),
                OsString::from("gc.auto=0"),
                OsString::from("-c"),
                OsString::from("core.logAllRefUpdates=false"),
                OsString::from("fetch"),
                OsString::from("--no-write-fetch-head"),
                self.path.as_os_str().to_os_string(),
                OsString::from(source),
            ],
            false,
        )?;
        let head = parse_oid(&user_git(
            runner,
            user_common_dir,
            &[
                OsString::from("rev-parse"),
                OsString::from("--verify"),
                OsString::from(local_ref.as_str()),
            ],
            false,
        )?)?;
        if head.as_str() != expected_oid.as_str() {
            return Err(git_error(
                "BASE_UNAVAILABLE",
                "user-remote import did not preserve the controller result OID",
            ));
        }
        Ok(ImportReceipt { head, local_ref })
    }

    pub fn tree_of(&self, oid: &BaseOid) -> CapturedTree {
        let listing = run_system_git(
            Some(&self.path),
            &[
                OsString::from("ls-tree"),
                OsString::from("-r"),
                OsString::from(oid.as_str()),
            ],
            None,
            None,
        )
        .expect("ls-tree");
        let mut entries = BTreeMap::new();
        for line in String::from_utf8(listing.stdout)
            .expect("utf8 ls-tree")
            .lines()
        {
            let (meta, path) = line.split_once('\t').expect("ls-tree tab");
            let mut parts = meta.split_whitespace();
            let mode = parts.next().expect("mode").to_owned();
            let _kind = parts.next();
            let object = parts.next().expect("oid").to_owned();
            let bytes = run_system_git(
                Some(&self.path),
                &[
                    OsString::from("cat-file"),
                    OsString::from("-p"),
                    OsString::from(object),
                ],
                None,
                None,
            )
            .expect("cat-file")
            .stdout;
            entries.insert(path.to_owned(), CapturedEntry { mode, bytes });
        }
        CapturedTree { entries }
    }

    pub fn parent_of(&self, oid: &BaseOid) -> BaseOid {
        let output = run_system_git(
            Some(&self.path),
            &[
                OsString::from("rev-parse"),
                OsString::from(format!("{}^", oid.as_str())),
            ],
            None,
            None,
        )
        .expect("rev-parse parent");
        parse_oid(&output).expect("parent oid")
    }

    pub fn has_ref(&self, name: &str) -> bool {
        run_system_git(
            Some(&self.path),
            &[
                OsString::from("show-ref"),
                OsString::from("--verify"),
                OsString::from("--quiet"),
                OsString::from(name),
            ],
            None,
            None,
        )
        .is_ok()
    }

    fn capture_tree(
        &self,
        runner: &dyn ProcessRunner,
        context: &ProjectContext,
        settings: &ProjectSettings,
        blob_oids: &mut BTreeMap<[u8; 32], String>,
    ) -> Result<CapturedSelection, WorkerError> {
        let selection = InputSelector::new(runner)
            .select(context, &settings.snapshot)
            .map_err(map_selection)?;
        let repo_root = RootedDir::open(&self.path)?;
        let scratch_dir = repo_root.open_child_directory(&relative("scratch")?, true)?;
        let index_name = format!("index-{}", Uuid::new_v4());
        scratch_dir.write_new_private_file(&index_name, &[])?;
        let scratch = self.path.join("scratch").join(&index_name);
        let source = RootedDir::open(&context.root)?;
        let captured = (|| {
            self.transfer_git(
                runner,
                &[OsString::from("read-tree"), OsString::from("--empty")],
                Some(&scratch),
                None,
            )?;
            let mut dirty = DirtyReport {
                modified: 0,
                added: 0,
                deleted: selection.tracked_deletions.len(),
            };
            let mut pending = PendingBlobBatch::new();
            let mut staged = Vec::new();
            for entry in &selection.entries {
                if entry.kind == SelectedInputKind::EmptyDirectory {
                    continue;
                }
                match entry.origin {
                    crate::inputs::InputOrigin::Tracked => dirty.modified += 1,
                    crate::inputs::InputOrigin::IncludedUntracked
                    | crate::inputs::InputOrigin::IncludedIgnored => dirty.added += 1,
                }
                let (mode, digest) = self.stage_worktree_blob(
                    runner,
                    &source,
                    &entry.path,
                    blob_oids,
                    &mut pending,
                )?;
                staged.push(StagedBlob {
                    mode,
                    digest,
                    path: entry.path.as_str().to_owned(),
                });
            }
            self.flush_pending_blobs(runner, blob_oids, &mut pending)?;
            let mut index_info = Vec::new();
            for staged in staged {
                let oid = blob_oids.get(&staged.digest).ok_or_else(|| {
                    git_error(
                        "BASE_UNAVAILABLE",
                        "a transfer blob was missing after the batch write",
                    )
                })?;
                index_info.extend_from_slice(staged.mode.as_bytes());
                index_info.push(b' ');
                index_info.extend_from_slice(oid.as_bytes());
                index_info.push(b'\t');
                index_info.extend_from_slice(staged.path.as_bytes());
                index_info.push(0);
            }
            if !index_info.is_empty() {
                self.transfer_git(
                    runner,
                    &[
                        OsString::from("update-index"),
                        OsString::from("--add"),
                        OsString::from("-z"),
                        OsString::from("--index-info"),
                    ],
                    Some(&scratch),
                    Some(index_info),
                )?;
            }
            let tree = parse_tree_id(&self.transfer_git(
                runner,
                &[OsString::from("write-tree")],
                Some(&scratch),
                None,
            )?)?;
            Ok(CapturedSelection { tree, dirty })
        })();
        remove_scratch_index(&scratch_dir, &index_name);
        captured
    }

    fn stage_worktree_blob(
        &self,
        runner: &dyn ProcessRunner,
        source: &RootedDir,
        path: &RelativePath,
        blob_oids: &mut BTreeMap<[u8; 32], String>,
        pending: &mut PendingBlobBatch,
    ) -> Result<(&'static str, [u8; 32]), WorkerError> {
        let mut inspection = source.inspect(path)?;
        match inspection.kind {
            EntryKind::Symlink => {
                let bytes = source.read_symlink(path)?.into_bytes();
                let digest = self.queue_blob_bytes(runner, bytes, false, blob_oids, pending)?;
                Ok(("120000", digest))
            }
            EntryKind::RegularFile => {
                let mut bytes = Vec::new();
                inspection.read_to_end(&mut bytes)?;
                let mode = if inspection.mode & 0o111 != 0 {
                    "100755"
                } else {
                    "100644"
                };
                let digest = self.queue_blob_bytes(runner, bytes, true, blob_oids, pending)?;
                Ok((mode, digest))
            }
        }
    }

    fn queue_blob_bytes(
        &self,
        runner: &dyn ProcessRunner,
        bytes: Vec<u8>,
        no_filters: bool,
        blob_oids: &mut BTreeMap<[u8; 32], String>,
        pending: &mut PendingBlobBatch,
    ) -> Result<[u8; 32], WorkerError> {
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        if blob_oids.contains_key(&digest) || pending.contains(&digest) {
            return Ok(digest);
        }
        if bytes.len() > BLOB_BATCH_MAX_BYTES {
            self.flush_pending_blobs(runner, blob_oids, pending)?;
            let oid = self.write_hash_object(runner, bytes, no_filters)?;
            blob_oids.insert(digest, oid);
            return Ok(digest);
        }
        if pending.would_exceed(bytes.len()) {
            self.flush_pending_blobs(runner, blob_oids, pending)?;
        }
        pending.push(digest, bytes, no_filters);
        Ok(digest)
    }

    fn flush_pending_blobs(
        &self,
        runner: &dyn ProcessRunner,
        blob_oids: &mut BTreeMap<[u8; 32], String>,
        pending: &mut PendingBlobBatch,
    ) -> Result<(), WorkerError> {
        match pending.take() {
            None => Ok(()),
            Some(PendingFlush::Singleton {
                digest,
                bytes,
                no_filters,
            }) => {
                let oid = self.write_hash_object(runner, bytes, no_filters)?;
                blob_oids.insert(digest, oid);
                Ok(())
            }
            Some(PendingFlush::Stream { stdin, digests }) => {
                let output = self.transfer_git(
                    runner,
                    &[
                        OsString::from("fast-import"),
                        OsString::from("--quiet"),
                        OsString::from("--done"),
                    ],
                    None,
                    Some(stdin),
                )?;
                let oids = parse_get_mark_oids(&output, digests.len())?;
                for (digest, oid) in digests.into_iter().zip(oids) {
                    blob_oids.insert(digest, oid);
                }
                Ok(())
            }
        }
    }

    fn write_hash_object(
        &self,
        runner: &dyn ProcessRunner,
        bytes: Vec<u8>,
        no_filters: bool,
    ) -> Result<String, WorkerError> {
        let mut args = vec![OsString::from("hash-object"), OsString::from("-w")];
        if no_filters {
            args.push(OsString::from("--no-filters"));
        }
        args.push(OsString::from("--stdin"));
        let output = self.transfer_git(runner, &args, None, Some(bytes))?;
        parse_hex_oid(&output)
    }

    fn commit_tree(
        &self,
        runner: &dyn ProcessRunner,
        tree: &str,
        parent: &BaseOid,
        identity: &GitIdentity,
    ) -> Result<BaseOid, WorkerError> {
        let output = self.transfer_git_with_env(
            runner,
            &[
                OsString::from("commit-tree"),
                OsString::from(tree),
                OsString::from("-p"),
                OsString::from(parent.as_str()),
                OsString::from("-m"),
                OsString::from(BASE_COMMIT_MESSAGE),
            ],
            None,
            None,
            vec![
                (
                    OsString::from("GIT_AUTHOR_NAME"),
                    OsString::from(identity.name()),
                ),
                (
                    OsString::from("GIT_AUTHOR_EMAIL"),
                    OsString::from(identity.email()),
                ),
                (
                    OsString::from("GIT_COMMITTER_NAME"),
                    OsString::from(identity.name()),
                ),
                (
                    OsString::from("GIT_COMMITTER_EMAIL"),
                    OsString::from(identity.email()),
                ),
            ],
        )?;
        parse_oid(&output)
    }

    fn update_ref(
        &self,
        runner: &dyn ProcessRunner,
        name: &str,
        oid: Option<&str>,
    ) -> Result<(), WorkerError> {
        let mut args = vec![OsString::from("update-ref")];
        if oid.is_none() {
            args.push(OsString::from("-d"));
        }
        args.push(OsString::from(name));
        if let Some(oid) = oid {
            args.push(OsString::from(oid));
        }
        self.transfer_git(runner, &args, None, None)?;
        Ok(())
    }

    fn transfer_git(
        &self,
        runner: &dyn ProcessRunner,
        args: &[OsString],
        index: Option<&Path>,
        stdin: Option<Vec<u8>>,
    ) -> Result<ProcessResult, WorkerError> {
        self.transfer_git_with_env(runner, args, index, stdin, Vec::new())
    }

    fn transfer_git_with_env(
        &self,
        runner: &dyn ProcessRunner,
        args: &[OsString],
        index: Option<&Path>,
        stdin: Option<Vec<u8>>,
        extra_env: Vec<(OsString, OsString)>,
    ) -> Result<ProcessResult, WorkerError> {
        let mut command_args = vec![
            OsString::from("--git-dir"),
            self.path.as_os_str().to_os_string(),
            OsString::from("-c"),
            OsString::from("gc.auto=0"),
        ];
        command_args.extend(args.iter().cloned());
        let mut environment = vec![
            (
                OsString::from("GIT_CONFIG_GLOBAL"),
                OsString::from("/dev/null"),
            ),
            (OsString::from("GIT_CONFIG_NOSYSTEM"), OsString::from("1")),
            (OsString::from("GIT_TERMINAL_PROMPT"), OsString::from("0")),
        ];
        if let Some(index) = index {
            environment.push((
                OsString::from("GIT_INDEX_FILE"),
                index.as_os_str().to_os_string(),
            ));
        }
        environment.extend(extra_env);
        let mut remove: Vec<OsString> = GIT_ENVIRONMENT_REMOVALS
            .iter()
            .map(OsString::from)
            .collect();
        if index.is_some() {
            remove.retain(|name| name != "GIT_INDEX_FILE");
        }
        let request = ProcessRequest {
            program: OsString::from(GIT_PROGRAM),
            args: command_args,
            environment,
            environment_remove: remove,
            stdin,
            policy: ProcessPolicy {
                stdout_limit: GIT_OUTPUT_LIMIT,
                stderr_limit: GIT_OUTPUT_LIMIT,
                deadline: GIT_DEADLINE,
            },
            isolate_parent_environment: false,
        };
        let result = runner.run(&request)?;
        if result.status.success() {
            Ok(result)
        } else {
            Err(git_error(
                "BASE_UNAVAILABLE",
                "a transfer repository Git command failed",
            ))
        }
    }

    fn with_scratch_regular<T>(
        &self,
        bytes: &[u8],
        work: impl FnOnce(&RootedDir, &str, &Path) -> Result<T, WorkerError>,
    ) -> Result<T, WorkerError> {
        let repo_root = RootedDir::open(&self.path)?;
        let scratch_dir = repo_root.open_child_directory(&relative("scratch")?, true)?;
        let name = format!("bundle-{}", Uuid::new_v4());
        let file = scratch_dir
            .write_new_private_file(&name, bytes)
            .map_err(WorkerError::Io)?;
        file.sync_all().map_err(WorkerError::Io)?;
        drop(file);
        let path = self.path.join("scratch").join(&name);
        let result = work(&scratch_dir, &name, &path);
        remove_scratch_index(&scratch_dir, &name);
        result
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedTree {
    entries: BTreeMap<String, CapturedEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CapturedEntry {
    mode: String,
    bytes: Vec<u8>,
}

impl CapturedTree {
    pub fn contains(&self, path: &str) -> bool {
        self.entries.contains_key(path)
    }

    pub fn blob(&self, path: &str) -> &[u8] {
        &self
            .entries
            .get(path)
            .unwrap_or_else(|| panic!("missing tree path {path}"))
            .bytes
    }

    pub fn mode(&self, path: &str) -> &str {
        &self
            .entries
            .get(path)
            .unwrap_or_else(|| panic!("missing tree path {path}"))
            .mode
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryFingerprint {
    head: Vec<u8>,
    index: Vec<u8>,
    status: Vec<u8>,
    refs: BTreeMap<String, String>,
    reflogs: BTreeMap<String, Vec<u8>>,
    config: Vec<u8>,
    hooks: BTreeSet<String>,
    objects: BTreeSet<String>,
}

impl RepositoryFingerprint {
    pub fn capture(worktree: &Path) -> Result<Self, WorkerError> {
        let git_dir = PathBuf::from(OsString::from_vec(parse_scalar(
            &run_system_git(
                None,
                &[
                    OsString::from("-C"),
                    worktree.as_os_str().to_os_string(),
                    OsString::from("rev-parse"),
                    OsString::from("--path-format=absolute"),
                    OsString::from("--git-dir"),
                ],
                None,
                None,
            )?
            .stdout,
        )?));
        let common_dir = PathBuf::from(OsString::from_vec(parse_scalar(
            &run_system_git(
                None,
                &[
                    OsString::from("-C"),
                    worktree.as_os_str().to_os_string(),
                    OsString::from("rev-parse"),
                    OsString::from("--path-format=absolute"),
                    OsString::from("--git-common-dir"),
                ],
                None,
                None,
            )?
            .stdout,
        )?));
        let refs_output = run_system_git(
            None,
            &[
                OsString::from("-C"),
                worktree.as_os_str().to_os_string(),
                OsString::from("for-each-ref"),
                OsString::from("--format=%(refname) %(objectname)"),
            ],
            None,
            None,
        )?;
        let mut refs = BTreeMap::new();
        for line in String::from_utf8(refs_output.stdout)
            .map_err(|_| task_config("Git ref listing was not UTF-8"))?
            .lines()
        {
            if let Some((name, oid)) = line.split_once(' ') {
                refs.insert(name.to_owned(), oid.to_owned());
            }
        }
        let status = run_system_git(
            None,
            &[
                OsString::from("-C"),
                worktree.as_os_str().to_os_string(),
                OsString::from("status"),
                OsString::from("--porcelain=v2"),
                OsString::from("-z"),
            ],
            None,
            None,
        )?
        .stdout;
        Ok(Self {
            head: fs::read(git_dir.join("HEAD")).unwrap_or_default(),
            index: fs::read(git_dir.join("index")).unwrap_or_default(),
            status,
            refs,
            reflogs: list_files(&git_dir.join("logs"))?,
            config: fs::read(common_dir.join("config")).unwrap_or_default(),
            hooks: list_names(&common_dir.join("hooks"))?,
            objects: list_names(&common_dir.join("objects"))?,
        })
    }

    pub fn diff(&self, before: &Self) -> Vec<String> {
        let mut changes = Vec::new();
        for name in self.refs.keys() {
            if !before.refs.contains_key(name) {
                changes.push(format!("+{name}"));
            }
        }
        for name in before.refs.keys() {
            if !self.refs.contains_key(name) {
                changes.push(format!("-{name}"));
            }
        }
        changes
    }
}

struct CapturedSelection {
    tree: String,
    dirty: DirtyReport,
}

struct StagedBlob {
    mode: &'static str,
    digest: [u8; 32],
    path: String,
}

enum PendingBlobBatch {
    Empty,
    Single {
        digest: [u8; 32],
        bytes: Vec<u8>,
        no_filters: bool,
    },
    Multi {
        stdin: Vec<u8>,
        digests: Vec<[u8; 32]>,
        by_digest: BTreeMap<[u8; 32], usize>,
        payload_bytes: usize,
    },
}

enum PendingFlush {
    Singleton {
        digest: [u8; 32],
        bytes: Vec<u8>,
        no_filters: bool,
    },
    Stream {
        stdin: Vec<u8>,
        digests: Vec<[u8; 32]>,
    },
}

impl PendingBlobBatch {
    fn new() -> Self {
        Self::Empty
    }

    fn contains(&self, digest: &[u8; 32]) -> bool {
        match self {
            Self::Empty => false,
            Self::Single {
                digest: pending, ..
            } => pending == digest,
            Self::Multi { by_digest, .. } => by_digest.contains_key(digest),
        }
    }

    fn unique_count(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Single { .. } => 1,
            Self::Multi { digests, .. } => digests.len(),
        }
    }

    fn payload_bytes(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Single { bytes, .. } => bytes.len(),
            Self::Multi { payload_bytes, .. } => *payload_bytes,
        }
    }

    fn would_exceed(&self, payload_len: usize) -> bool {
        if self.unique_count() == 0 {
            return false;
        }
        self.unique_count() >= BLOB_BATCH_MAX_OBJECTS
            || self.payload_bytes().saturating_add(payload_len) > BLOB_BATCH_MAX_BYTES
    }

    fn push(&mut self, digest: [u8; 32], bytes: Vec<u8>, no_filters: bool) {
        match std::mem::replace(self, Self::Empty) {
            Self::Empty => {
                *self = Self::Single {
                    digest,
                    bytes,
                    no_filters,
                };
            }
            Self::Single {
                digest: first_digest,
                bytes: first_bytes,
                no_filters: _,
            } => {
                let mut stdin = Vec::new();
                write_blob_command(&mut stdin, 1, &first_bytes);
                write_blob_command(&mut stdin, 2, &bytes);
                let payload_bytes = first_bytes.len() + bytes.len();
                drop(first_bytes);
                let mut by_digest = BTreeMap::new();
                by_digest.insert(first_digest, 1);
                by_digest.insert(digest, 2);
                *self = Self::Multi {
                    stdin,
                    digests: vec![first_digest, digest],
                    by_digest,
                    payload_bytes,
                };
            }
            Self::Multi {
                mut stdin,
                mut digests,
                mut by_digest,
                mut payload_bytes,
            } => {
                let mark = digests.len() + 1;
                write_blob_command(&mut stdin, mark, &bytes);
                by_digest.insert(digest, mark);
                digests.push(digest);
                payload_bytes += bytes.len();
                *self = Self::Multi {
                    stdin,
                    digests,
                    by_digest,
                    payload_bytes,
                };
            }
        }
    }

    fn take(&mut self) -> Option<PendingFlush> {
        match std::mem::replace(self, Self::Empty) {
            Self::Empty => None,
            Self::Single {
                digest,
                bytes,
                no_filters,
            } => Some(PendingFlush::Singleton {
                digest,
                bytes,
                no_filters,
            }),
            Self::Multi {
                mut stdin, digests, ..
            } => {
                write_get_marks_and_done(&mut stdin, digests.len());
                Some(PendingFlush::Stream { stdin, digests })
            }
        }
    }
}

fn write_blob_command(stdin: &mut Vec<u8>, mark: usize, payload: &[u8]) {
    let _ = write!(stdin, "blob\nmark :{mark}\ndata {}\n", payload.len());
    stdin.extend_from_slice(payload);
    stdin.push(b'\n');
}

fn write_get_marks_and_done(stdin: &mut Vec<u8>, count: usize) {
    for mark in 1..=count {
        let _ = writeln!(stdin, "get-mark :{mark}");
    }
    stdin.extend_from_slice(b"done\n");
}

pub fn repo_id_for(user_common_dir: &Path) -> Result<String, WorkerError> {
    let canonical = fs::canonicalize(user_common_dir).map_err(|_| {
        git_error(
            "BASE_UNAVAILABLE",
            "the user repository common directory is missing",
        )
    })?;
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_os_str().as_bytes());
    Ok(format!("{:x}", hasher.finalize()))
}

fn resolve_commit(
    runner: &dyn ProcessRunner,
    context: &ProjectContext,
    reference: &str,
) -> Result<BaseOid, WorkerError> {
    if reference.is_empty()
        || reference.starts_with('-')
        || reference.contains("..")
        || reference.contains('\0')
    {
        return Err(git_error(
            "BASE_UNAVAILABLE",
            "base reference is outside the repository or invalid",
        ));
    }
    let output = user_git(
        runner,
        &context.root,
        &[
            OsString::from("rev-parse"),
            OsString::from("--verify"),
            OsString::from("--end-of-options"),
            OsString::from(format!("{reference}^{{commit}}")),
        ],
        false,
    );
    match output {
        Ok(result) => parse_oid(&result),
        Err(_) => Err(git_error(
            "BASE_UNAVAILABLE",
            "base reference is not a commit in the user repository",
        )),
    }
}

fn resolve_branch_name(
    runner: &dyn ProcessRunner,
    context: &ProjectContext,
    reference: &str,
) -> Result<Option<String>, WorkerError> {
    let output = user_git(
        runner,
        &context.root,
        &[
            OsString::from("rev-parse"),
            OsString::from("--abbrev-ref"),
            OsString::from("--end-of-options"),
            OsString::from(reference),
        ],
        true,
    )?;
    if !output.status.success() {
        return Ok(None);
    }
    let name = String::from_utf8(output.stdout)
        .map_err(|_| task_config("Git branch name was not UTF-8"))?
        .trim()
        .to_owned();
    if name.is_empty() || name == "HEAD" || is_lower_hex(&name, 40) {
        Ok(None)
    } else {
        Ok(Some(name))
    }
}

fn reject_merge_in_progress(
    runner: &dyn ProcessRunner,
    context: &ProjectContext,
) -> Result<(), WorkerError> {
    let output = user_git(
        runner,
        &context.root,
        &[
            OsString::from("rev-parse"),
            OsString::from("-q"),
            OsString::from("--verify"),
            OsString::from("MERGE_HEAD"),
        ],
        true,
    )?;
    if output.status.success() {
        return Err(task_config("a merge is in progress"));
    }
    Ok(())
}

fn dirty_report(
    runner: &dyn ProcessRunner,
    context: &ProjectContext,
) -> Result<DirtyReport, WorkerError> {
    let output = user_git(
        runner,
        &context.root,
        &[
            OsString::from("status"),
            OsString::from("--porcelain"),
            OsString::from("--untracked-files=all"),
        ],
        false,
    )?;
    let mut dirty = DirtyReport {
        modified: 0,
        added: 0,
        deleted: 0,
    };
    for line in String::from_utf8(output.stdout)
        .map_err(|_| task_config("Git status was not UTF-8"))?
        .lines()
    {
        if line.len() < 2 {
            continue;
        }
        let index = line.as_bytes()[0] as char;
        let worktree = line.as_bytes()[1] as char;
        if index == '?' && worktree == '?' {
            dirty.added += 1;
            continue;
        }
        for flag in [index, worktree] {
            match flag {
                'M' => dirty.modified += 1,
                'A' => dirty.added += 1,
                'D' => dirty.deleted += 1,
                _ => {}
            }
        }
    }
    Ok(dirty)
}

fn user_git(
    runner: &dyn ProcessRunner,
    cwd: &Path,
    args: &[OsString],
    allow_failure: bool,
) -> Result<ProcessResult, WorkerError> {
    let mut command_args = vec![OsString::from("-C"), cwd.as_os_str().to_os_string()];
    command_args.extend(args.iter().cloned());
    let request = ProcessRequest {
        program: OsString::from(GIT_PROGRAM),
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
            stdout_limit: GIT_OUTPUT_LIMIT,
            stderr_limit: GIT_OUTPUT_LIMIT,
            deadline: GIT_DEADLINE,
        },
        isolate_parent_environment: false,
    };
    let result = runner.run(&request)?;
    if allow_failure || result.status.success() {
        Ok(result)
    } else {
        Err(git_error(
            "BASE_UNAVAILABLE",
            "a user-repository Git command failed",
        ))
    }
}

fn run_system_git(
    git_dir: Option<&Path>,
    args: &[OsString],
    index: Option<&Path>,
    stdin: Option<&[u8]>,
) -> Result<ProcessResult, WorkerError> {
    let mut command = Command::new(GIT_PROGRAM);
    if let Some(git_dir) = git_dir {
        command.arg("--git-dir").arg(git_dir);
    }
    command.args(args);
    command
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0");
    for name in GIT_ENVIRONMENT_REMOVALS {
        if *name == "GIT_INDEX_FILE" && index.is_some() {
            continue;
        }
        command.env_remove(name);
    }
    if let Some(index) = index {
        command.env("GIT_INDEX_FILE", index);
    }
    if let Some(stdin) = stdin {
        command.stdin(std::process::Stdio::piped());
        let mut child = command.spawn()?;
        use std::io::Write;
        if let Some(mut handle) = child.stdin.take() {
            handle.write_all(stdin)?;
        }
        let output = child.wait_with_output()?;
        return process_output(output);
    }
    process_output(command.output()?)
}

fn process_output(output: Output) -> Result<ProcessResult, WorkerError> {
    let result = ProcessResult {
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
    };
    if result.status.success() {
        Ok(result)
    } else {
        Err(git_error("BASE_UNAVAILABLE", "a Git helper command failed"))
    }
}

fn parse_oid(result: &ProcessResult) -> Result<BaseOid, WorkerError> {
    parse_hex_oid(result)?
        .parse()
        .map_err(|_| git_error("BASE_UNAVAILABLE", "Git returned a non-canonical object ID"))
}

fn parse_hex_oid(result: &ProcessResult) -> Result<String, WorkerError> {
    let value = String::from_utf8(result.stdout.clone())
        .map_err(|_| task_config("Git object ID was not UTF-8"))?
        .trim()
        .to_owned();
    if is_lower_hex(&value, 40) {
        Ok(value)
    } else {
        Err(git_error(
            "BASE_UNAVAILABLE",
            "Git returned a non-canonical object ID",
        ))
    }
}

fn parse_get_mark_oids(
    result: &ProcessResult,
    expected: usize,
) -> Result<Vec<String>, WorkerError> {
    let Some(want_len) = expected.checked_mul(GET_MARK_LINE_BYTES) else {
        return Err(git_error(
            "BASE_UNAVAILABLE",
            "Git returned a non-canonical object ID",
        ));
    };
    if result.stdout.len() != want_len {
        return Err(git_error(
            "BASE_UNAVAILABLE",
            "Git returned a non-canonical object ID",
        ));
    }
    let mut oids = Vec::with_capacity(expected);
    for index in 0..expected {
        let start = index * GET_MARK_LINE_BYTES;
        let line = &result.stdout[start..start + GET_MARK_LINE_BYTES];
        if line[GET_MARK_LINE_BYTES - 1] != b'\n' {
            return Err(git_error(
                "BASE_UNAVAILABLE",
                "Git returned a non-canonical object ID",
            ));
        }
        let hex = std::str::from_utf8(&line[..40])
            .map_err(|_| git_error("BASE_UNAVAILABLE", "Git returned a non-canonical object ID"))?;
        if !is_lower_hex(hex, 40) {
            return Err(git_error(
                "BASE_UNAVAILABLE",
                "Git returned a non-canonical object ID",
            ));
        }
        oids.push(hex.to_owned());
    }
    Ok(oids)
}

fn parse_tree_id(result: &ProcessResult) -> Result<String, WorkerError> {
    parse_hex_oid(result)
}

fn parse_scalar(output: &[u8]) -> Result<Vec<u8>, WorkerError> {
    let scalar = output.strip_suffix(b"\n").unwrap_or(output);
    if scalar.contains(&b'\r') || scalar.contains(&b'\n') || scalar.contains(&b'\0') {
        return Err(task_config("Git returned malformed scalar output"));
    }
    Ok(scalar.to_vec())
}

fn nul_records(output: &[u8]) -> impl Iterator<Item = &[u8]> {
    output
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
}

fn list_files(root: &Path) -> Result<BTreeMap<String, Vec<u8>>, WorkerError> {
    let mut files = BTreeMap::new();
    if !root.exists() {
        return Ok(files);
    }
    collect_files(root, root, &mut files)?;
    Ok(files)
}

fn collect_files(
    root: &Path,
    current: &Path,
    files: &mut BTreeMap<String, Vec<u8>>,
) -> Result<(), WorkerError> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files(root, &path, files)?;
        } else {
            let relative = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .into_owned();
            files.insert(relative, fs::read(path)?);
        }
    }
    Ok(())
}

fn list_names(root: &Path) -> Result<BTreeSet<String>, WorkerError> {
    let mut names = BTreeSet::new();
    if !root.exists() {
        return Ok(names);
    }
    collect_names(root, root, &mut names)?;
    Ok(names)
}

fn collect_names(
    root: &Path,
    current: &Path,
    names: &mut BTreeSet<String>,
) -> Result<(), WorkerError> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        let relative = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .into_owned();
        names.insert(relative);
        if path.is_dir() {
            collect_names(root, &path, names)?;
        }
    }
    Ok(())
}

fn remove_scratch_index(scratch_dir: &RootedDir, index_name: &str) {
    let lock_name = format!("{index_name}.lock");
    for name in [lock_name.as_str(), index_name] {
        let _ = restore_and_remove_scratch_regular(scratch_dir, name);
    }
}

fn restore_and_remove_scratch_regular(scratch_dir: &RootedDir, name: &str) -> io::Result<()> {
    if !scratch_dir.entry_exists(name)? {
        return Ok(());
    }
    scratch_dir.set_private_regular_mode(name, 0o600)?;
    scratch_dir.remove_owned_regular(name)
}

fn read_scratch_regular(scratch_dir: &RootedDir, name: &str) -> Result<Vec<u8>, WorkerError> {
    let mut inspection = scratch_dir
        .inspect(&relative(name)?)
        .map_err(WorkerError::Io)?;
    if inspection.kind != EntryKind::RegularFile {
        return Err(git_error(
            "BASE_UNAVAILABLE",
            "frozen bundle scratch is not a regular file",
        ));
    }
    let expected = inspection.size;
    let mut bytes = Vec::new();
    inspection
        .read_to_end(&mut bytes)
        .map_err(WorkerError::Io)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != expected {
        return Err(git_error(
            "BASE_UNAVAILABLE",
            "frozen bundle scratch was truncated",
        ));
    }
    Ok(bytes)
}

fn base_ref(task_id: TaskId) -> String {
    format!("refs/mac-worker/bases/{task_id}")
}

fn map_selection(failure: SelectionFailure) -> WorkerError {
    match failure.code {
        "UNTRACKED_INPUT" | "SENSITIVE_PATH" => WorkerError::Snapshot {
            code: failure.code,
            message: failure.message,
        },
        _ => WorkerError::Project {
            code: failure.code,
            message: failure.message,
        },
    }
}

fn is_sensitive(path: &RelativePath) -> bool {
    let components = path.as_str().split('/').collect::<Vec<_>>();
    let basename = components.last().copied().unwrap_or_default();
    let sensitive_basename = (basename == ".env"
        || (basename.starts_with(".env.") && !matches!(basename, ".env.example" | ".env.sample")))
        || matches!(
            basename,
            ".npmrc"
                | ".pypirc"
                | ".netrc"
                | "id_rsa"
                | "id_ed25519"
                | "credentials"
                | "credentials.json"
        );
    sensitive_basename
        || components
            .iter()
            .any(|component| matches!(*component, ".ssh" | ".aws" | ".kube"))
        || components
            .windows(2)
            .any(|pair| matches!(pair, [".config", "gcloud"]))
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_dag_batch_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

fn validate_owned_pin_ref(name: &str) -> Result<(), WorkerError> {
    if let Some(request_id) = name.strip_prefix("refs/mac-worker/requests/")
        && !request_id.contains('/')
        && is_lower_hex(request_id, 32)
    {
        return Ok(());
    }
    if let Some(rest) = name.strip_prefix("refs/mac-worker/dag/") {
        let mut parts = rest.split('/');
        match (parts.next(), parts.next(), parts.next()) {
            (Some(run_id), Some(batch_id), None)
                if is_lower_hex(run_id, 32) && is_dag_batch_id(batch_id) =>
            {
                return Ok(());
            }
            _ => {}
        }
    }
    if let Some(rest) = name.strip_prefix("refs/mac-worker/controller-results/") {
        let mut parts = rest.split('/');
        match (parts.next(), parts.next(), parts.next()) {
            (Some(request_id), Some(turn_id), None)
                if is_lower_hex(request_id, 32) && is_lower_hex(turn_id, 32) =>
            {
                return Ok(());
            }
            _ => {}
        }
    }
    Err(git_error(
        "BASE_UNAVAILABLE",
        "owned pin refs must live under refs/mac-worker/requests/, refs/mac-worker/dag/, or refs/mac-worker/controller-results/",
    ))
}

fn is_safe_worker_ref_component(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('.')
        && !value.ends_with('.')
        && !value.ends_with(".lock")
        && !value.contains("..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'@'))
}

fn relative(path: &str) -> Result<RelativePath, WorkerError> {
    RelativePath::parse(path.as_bytes()).map_err(|_| {
        git_error(
            "BASE_UNAVAILABLE",
            "a transfer cache path is not a safe relative path",
        )
    })
}

fn chmod_owner_only(dir: &RootedDir) -> io::Result<()> {
    let result = unsafe { libc::fchmod(dir.raw_directory_fd(), 0o700) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn open_or_create_owner_only_dir(path: &Path) -> io::Result<RootedDir> {
    let dir = open_or_create_dir(path)?;
    chmod_owner_only(&dir)?;
    Ok(dir)
}

/// Open a directory, creating it when absent.  A concurrent creator winning
/// the race is not an error: the directory is simply opened again.
fn open_or_create_dir(path: &Path) -> io::Result<RootedDir> {
    match RootedDir::open(path) {
        Ok(dir) => Ok(dir),
        Err(error) if error.kind() == io::ErrorKind::NotFound => match RootedDir::create(path) {
            Ok(dir) => Ok(dir),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => RootedDir::open(path),
            Err(error) => Err(error),
        },
        Err(error) => Err(error),
    }
}

/// The repository directory when it exists and has been initialized, so a
/// directory left behind by an interrupted creation is initialized again.
fn open_initialized_transfer_repo(path: &Path) -> Result<Option<RootedDir>, WorkerError> {
    let repo_dir = match RootedDir::open(path) {
        Ok(repo_dir) => repo_dir,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(WorkerError::Io(error)),
    };
    if repo_dir.entry_exists("HEAD")? {
        Ok(Some(repo_dir))
    } else {
        Ok(None)
    }
}

fn open_owner_only_subdir(root: &Path, components: &[&str]) -> Result<RootedDir, WorkerError> {
    let mut current = root.to_path_buf();
    let mut dir = RootedDir::open(root)?;
    chmod_owner_only(&dir)?;
    for component in components {
        current.push(component);
        dir = open_or_create_dir(&current).map_err(WorkerError::Io)?;
        chmod_owner_only(&dir)?;
    }
    Ok(dir)
}

fn write_alternates_atomic(info: &RootedDir, target: &Path) -> Result<(), WorkerError> {
    let bytes = format!("{}\n", target.display()).into_bytes();
    if info.entry_exists("alternates")? {
        let mut inspection = info.inspect(&relative("alternates")?)?;
        if inspection.kind != EntryKind::RegularFile {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "alternates is not a regular file",
            )
            .into());
        }
        let mut current = Vec::new();
        inspection.read_to_end(&mut current)?;
        if current == bytes {
            return Ok(());
        }
        info.replace_private_regular_exact("alternates", &current, &bytes)?;
        return Ok(());
    }
    match info.write_private_atomic_no_replace("alternates", &bytes) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            // A concurrent opener of the same repository wrote the file
            // first.  Its content is a function of the repository, so it is
            // expected to match; anything else is corrected like a stale file.
            let mut inspection = info.inspect(&relative("alternates")?)?;
            let mut current = Vec::new();
            inspection.read_to_end(&mut current)?;
            if current != bytes {
                info.replace_private_regular_exact("alternates", &current, &bytes)?;
            }
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn pack_listing(pack_dir: &Path) -> String {
    match fs::read_dir(pack_dir) {
        Ok(entries) => {
            let mut names = entries
                .filter_map(Result::ok)
                .map(|entry| {
                    let path = entry.path();
                    let name = entry.file_name().to_string_lossy().into_owned();
                    let len = fs::metadata(&path).ok().map(|meta| meta.len());
                    format!("{name}:{}", len.unwrap_or(0))
                })
                .collect::<Vec<_>>();
            names.sort();
            names.join(",")
        }
        Err(error) => format!("unreadable:{error}"),
    }
}

fn describe_git_outcome(result: &Result<ProcessResult, WorkerError>) -> String {
    match result {
        Ok(output) => format!(
            "ok status={:?} signal={:?} stdout={:?} stderr={:?}",
            output.status.code(),
            output.status.signal(),
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).replace('\n', "\\n")
        ),
        Err(error) => error.to_string().replace('\n', "\\n"),
    }
}

fn git_at(
    runner: &dyn ProcessRunner,
    git_dir: &Path,
    args: &[OsString],
    stdin: Option<Vec<u8>>,
) -> Result<ProcessResult, WorkerError> {
    let mut command_args = vec![
        OsString::from("--git-dir"),
        git_dir.as_os_str().to_os_string(),
        OsString::from("-c"),
        OsString::from("gc.auto=0"),
    ];
    command_args.extend(args.iter().cloned());
    let request = ProcessRequest {
        program: OsString::from(GIT_PROGRAM),
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
        stdin,
        policy: ProcessPolicy {
            stdout_limit: GIT_OUTPUT_LIMIT,
            stderr_limit: GIT_OUTPUT_LIMIT,
            deadline: GIT_DEADLINE,
        },
        isolate_parent_environment: false,
    };
    let result = runner.run(&request)?;
    if result.status.success() {
        Ok(result)
    } else {
        let cwd = std::env::current_dir().ok();
        let argv: Vec<String> = args
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect();
        let stderr = String::from_utf8_lossy(&result.stderr).replace('\n', "\\n");
        if std::env::var_os("MAC_WORKER_OWNED_GRAPH_TRACE").is_some() {
            eprintln!(
                "owned-graph git fail git-dir={} cwd={:?} status={:?} signal={:?} argv={argv:?} stderr={stderr}",
                git_dir.display(),
                cwd,
                result.status.code(),
                result.status.signal()
            );
        }
        Err(git_error(
            "BASE_UNAVAILABLE",
            format!(
                "a transfer repository Git command failed git-dir={} cwd={:?} status={:?} signal={:?} argv={argv:?} stderr={stderr}",
                git_dir.display(),
                cwd,
                result.status.code(),
                result.status.signal()
            ),
        ))
    }
}

fn link_owned_objects(transfer: &Path, isolate: &Path) -> Result<(), WorkerError> {
    let source_objects = transfer.join("objects");
    let destination_objects = isolate.join("objects");
    if !source_objects.is_dir() {
        return Ok(());
    }
    let source_pack = source_objects.join("pack");
    if source_pack.is_dir() {
        let destination_pack = destination_objects.join("pack");
        fs::create_dir_all(&destination_pack).map_err(WorkerError::Io)?;
        for entry in fs::read_dir(&source_pack).map_err(WorkerError::Io)? {
            let entry = entry.map_err(WorkerError::Io)?;
            let name = entry.file_name();
            let Some(name_str) = name.to_str() else {
                continue;
            };
            if !(name_str.ends_with(".pack")
                || name_str.ends_with(".idx")
                || name_str.ends_with(".keep"))
            {
                continue;
            }
            hardlink_or_copy(&entry.path(), &destination_pack.join(name))?;
        }
    }
    for entry in fs::read_dir(&source_objects).map_err(WorkerError::Io)? {
        let entry = entry.map_err(WorkerError::Io)?;
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        if name_str == "pack" || name_str == "info" {
            continue;
        }
        if name_str.len() != 2 || !name_str.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            continue;
        }
        let source_fanout = source_objects.join(name_str);
        if !source_fanout.is_dir() {
            continue;
        }
        let destination_fanout = destination_objects.join(name_str);
        fs::create_dir_all(&destination_fanout).map_err(WorkerError::Io)?;
        for object in fs::read_dir(&source_fanout).map_err(WorkerError::Io)? {
            let object = object.map_err(WorkerError::Io)?;
            if !object.path().is_file() {
                continue;
            }
            hardlink_or_copy(&object.path(), &destination_fanout.join(object.file_name()))?;
        }
    }
    Ok(())
}

fn hardlink_or_copy(source: &Path, destination: &Path) -> Result<(), WorkerError> {
    match fs::hard_link(source, destination) {
        Ok(()) => Ok(()),
        Err(_) => {
            fs::copy(source, destination).map_err(WorkerError::Io)?;
            Ok(())
        }
    }
}

fn fsync_regular_file(path: &Path) -> Result<(), WorkerError> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(WorkerError::Io)?;
    file.sync_all().map_err(WorkerError::Io)?;
    Ok(())
}

fn fsync_directory(path: &Path) -> Result<(), WorkerError> {
    let dir = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(WorkerError::Io)?;
    dir.sync_all().map_err(WorkerError::Io)?;
    Ok(())
}

fn fsync_object_store(git_dir: &Path) -> Result<(), WorkerError> {
    let objects = git_dir.join("objects");
    if !objects.is_dir() {
        return Ok(());
    }
    let pack = objects.join("pack");
    if pack.is_dir() {
        let entries = fs::read_dir(&pack).map_err(WorkerError::Io)?;
        for entry in entries {
            let entry = entry.map_err(WorkerError::Io)?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if !name.ends_with(".pack") && !name.ends_with(".idx") {
                continue;
            }
            let path = pack.join(name);
            if path.is_file() {
                fsync_regular_file(&path)?;
            }
        }
        fsync_directory(&pack)?;
    }
    let entries = fs::read_dir(&objects).map_err(WorkerError::Io)?;
    let mut fanouts = Vec::new();
    for entry in entries {
        let entry = entry.map_err(WorkerError::Io)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name == "pack" || name == "info" {
            continue;
        }
        if name.len() != 2 || !name.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            continue;
        }
        let fanout = objects.join(name);
        if !fanout.is_dir() {
            continue;
        }
        let objects_in_fanout = fs::read_dir(&fanout).map_err(WorkerError::Io)?;
        for object in objects_in_fanout {
            let object = object.map_err(WorkerError::Io)?;
            let path = object.path();
            if path.is_file() {
                fsync_regular_file(&path)?;
            }
        }
        fanouts.push(fanout);
    }
    for fanout in fanouts {
        fsync_directory(&fanout)?;
    }
    fsync_directory(&objects)
}

fn git_error(code: &'static str, message: impl Into<String>) -> WorkerError {
    WorkerError::Git {
        code,
        message: message.into(),
    }
}

fn frozen_source_conflict() -> WorkerError {
    git_error(
        "FROZEN_SOURCE_CONFLICT",
        "pin ref is already bound to a different commit",
    )
}

fn task_config(message: impl Into<std::borrow::Cow<'static, str>>) -> WorkerError {
    WorkerError::task("TASK_CONFIG_INVALID", message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process::SystemProcessRunner;

    fn user_repository(root: &Path) -> PathBuf {
        let user = root.join("user");
        fs::create_dir_all(&user).unwrap();
        run_system_git(
            None,
            &[
                OsString::from("init"),
                OsString::from("-q"),
                user.as_os_str().to_os_string(),
            ],
            None,
            None,
        )
        .unwrap();
        user.join(".git")
    }

    #[test]
    fn apply_skips_a_candidate_whose_repository_is_in_use_and_collects_it_later() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        let common_dir = user_repository(temp.path());
        let transfer = TransferRepo::open_or_create(&cache, &common_dir).unwrap();
        let repo_path = transfer.path().to_path_buf();
        let parent = RootedDir::open(&cache.join("transfer")).unwrap();
        let candidate = GcCandidate::new(
            "transfer_repo",
            transfer.repo_id(),
            0,
            "transfer repository retention",
        )
        .unwrap();
        let gc = TransferGc::new(&cache, &SystemProcessRunner);

        let mut warnings = Vec::new();
        let applied = gc
            .apply_candidate(&parent, &candidate, u64::MAX / 2, &mut warnings)
            .unwrap();
        assert!(!applied);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("in use"));
        assert!(repo_path.exists());

        drop(transfer);
        // Unit tests share this process with tests that fork without exec;
        // such a child keeps every inherited descriptor, including this lock,
        // open until it exits.  Collection therefore succeeds eventually rather
        // than immediately here, which is not a property of the code under test.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            let mut warnings = Vec::new();
            let applied = gc
                .apply_candidate(&parent, &candidate, u64::MAX / 2, &mut warnings)
                .unwrap();
            if applied {
                assert!(warnings.is_empty(), "{warnings:?}");
                break;
            }
            assert!(warnings[0].contains("in use"), "{warnings:?}");
            assert!(
                std::time::Instant::now() < deadline,
                "the released repository was never collected"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert!(!repo_path.exists());
    }

    fn git_in(dir: &Path, args: &[&str]) -> ProcessResult {
        let mut command = vec![OsString::from("-C"), dir.as_os_str().to_os_string()];
        command.extend(args.iter().map(OsString::from));
        run_system_git(None, &command, None, None).unwrap()
    }

    fn committed_user_repository(root: &Path, name: &str, contents: &[u8]) -> (PathBuf, BaseOid) {
        let user = root.join(name);
        fs::create_dir_all(&user).unwrap();
        run_system_git(
            None,
            &[
                OsString::from("init"),
                OsString::from("-q"),
                user.as_os_str().to_os_string(),
            ],
            None,
            None,
        )
        .unwrap();
        git_in(&user, &["config", "user.name", "mac-worker"]);
        git_in(&user, &["config", "user.email", "mac-worker@localhost"]);
        fs::write(user.join("README"), contents).unwrap();
        git_in(&user, &["add", "README"]);
        git_in(&user, &["commit", "-q", "-m", "init"]);
        let oid = git_in(&user, &["rev-parse", "HEAD"])
            .stdout
            .as_slice()
            .iter()
            .copied()
            .filter(|byte| *byte != b'\n')
            .map(char::from)
            .collect::<String>()
            .parse::<BaseOid>()
            .unwrap();
        (user.join(".git"), oid)
    }

    const REQUEST_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";

    #[test]
    fn a_named_request_ref_round_trips_a_tiny_commit_into_a_second_transfer_repo() {
        let temp = tempfile::tempdir().unwrap();
        let runner = SystemProcessRunner;
        let (source_common, oid) = committed_user_repository(temp.path(), "source", b"hello\n");
        let source =
            TransferRepo::open_or_create(&temp.path().join("source-cache"), &source_common)
                .unwrap();
        let bundle = source
            .bundle_frozen_source(&runner, REQUEST_ID, &oid)
            .unwrap();
        assert!(!bundle.is_empty());
        assert!(bundle.len() <= TransferRepo::FROZEN_BUNDLE_MAX_BYTES);
        assert!(!source.path().join("mw-frozen.bundle").exists());
        assert!(source.has_ref(&TransferRepo::frozen_request_ref(REQUEST_ID).unwrap()));

        let (dest_common, _) = committed_user_repository(temp.path(), "dest", b"other\n");
        let dest =
            TransferRepo::open_or_create(&temp.path().join("dest-cache"), &dest_common).unwrap();
        assert!(!dest.contains_commit(&runner, &oid).unwrap());
        dest.import_frozen_source(&runner, REQUEST_ID, &bundle, &oid)
            .unwrap();
        assert!(dest.contains_commit(&runner, &oid).unwrap());
        assert!(dest.has_ref(&TransferRepo::frozen_request_ref(REQUEST_ID).unwrap()));
        assert!(!dest.path().join("mw-frozen.bundle").exists());
        assert!(scratch_leftovers(&dest).is_empty());
        assert!(incoming_refs(&dest).is_empty());
    }

    #[test]
    fn import_keeps_the_full_graph_after_the_alternate_target_is_emptied() {
        let temp = tempfile::tempdir().unwrap();
        let runner = SystemProcessRunner;
        let (source_common, oid) = committed_user_repository(temp.path(), "source", b"hello\n");
        let source =
            TransferRepo::open_or_create(&temp.path().join("source-cache"), &source_common)
                .unwrap();
        let bundle = source
            .bundle_frozen_source(&runner, REQUEST_ID, &oid)
            .unwrap();
        let dest_common = clone_user_repository(temp.path(), "dest", &source_common);
        let dest =
            TransferRepo::open_or_create(&temp.path().join("dest-cache"), &dest_common).unwrap();
        assert!(dest.contains_commit(&runner, &oid).unwrap());
        dest.import_frozen_source(&runner, REQUEST_ID, &bundle, &oid)
            .unwrap();
        dest.import_frozen_source(&runner, REQUEST_ID, &bundle, &oid)
            .unwrap();
        let other = additional_commit(&dest_common, b"other-head\n");
        let error = dest
            .pin_frozen_source(&runner, REQUEST_ID, &other)
            .unwrap_err();
        assert_eq!(error.public_code(), "FROZEN_SOURCE_CONFLICT");
        assert!(dest.path().join("objects/info/alternates").is_file());
        hide_user_objects(&dest_common);
        assert_owned_reachable_graph(&dest, &runner, &oid);
        assert!(dest.has_ref(&TransferRepo::frozen_request_ref(REQUEST_ID).unwrap()));
    }

    #[test]
    fn import_copies_overlapping_parent_blobs_out_of_the_alternate() {
        let temp = tempfile::tempdir().unwrap();
        let runner = SystemProcessRunner;
        let (source_common, first) = committed_user_repository(temp.path(), "source", b"hello\n");
        let second = additional_commit(&source_common, b"second\n");
        let source =
            TransferRepo::open_or_create(&temp.path().join("source-cache"), &source_common)
                .unwrap();
        let bundle = source
            .bundle_frozen_source(&runner, REQUEST_ID, &second)
            .unwrap();
        let (dest_common, dest_commit) = committed_user_repository(temp.path(), "dest", b"hello\n");
        assert_ne!(dest_commit.as_str(), second.as_str());
        let dest =
            TransferRepo::open_or_create(&temp.path().join("dest-cache"), &dest_common).unwrap();
        assert!(!dest.contains_commit(&runner, &second).unwrap());
        dest.import_frozen_source(&runner, REQUEST_ID, &bundle, &second)
            .unwrap();
        assert!(dest.path().join("objects/info/alternates").is_file());
        hide_user_objects(&dest_common);
        assert_owned_reachable_graph(&dest, &runner, &second);
        assert_owned_reachable_graph(&dest, &runner, &first);
    }

    #[test]
    fn pin_keeps_the_full_graph_after_the_alternate_target_is_emptied() {
        let temp = tempfile::tempdir().unwrap();
        let runner = SystemProcessRunner;
        let (common, oid) = committed_user_repository(temp.path(), "source", b"hello\n");
        let transfer = TransferRepo::open_or_create(&temp.path().join("cache"), &common).unwrap();
        transfer
            .pin_frozen_source(&runner, REQUEST_ID, &oid)
            .unwrap();
        transfer
            .pin_frozen_source(&runner, REQUEST_ID, &oid)
            .unwrap();
        assert!(transfer.path().join("objects/info/alternates").is_file());
        hide_user_objects(&common);
        assert_owned_reachable_graph(&transfer, &runner, &oid);
        assert!(transfer.has_ref(&TransferRepo::frozen_request_ref(REQUEST_ID).unwrap()));
    }

    #[test]
    fn a_truncated_bundle_does_not_leave_a_request_ref() {
        let temp = tempfile::tempdir().unwrap();
        let runner = SystemProcessRunner;
        let (source_common, oid) = committed_user_repository(temp.path(), "source", b"hello\n");
        let source =
            TransferRepo::open_or_create(&temp.path().join("source-cache"), &source_common)
                .unwrap();
        let bundle = source
            .bundle_frozen_source(&runner, REQUEST_ID, &oid)
            .unwrap();
        let truncated = &bundle[..bundle.len() / 2];
        assert!(!truncated.is_empty());

        let (dest_common, _) = committed_user_repository(temp.path(), "dest", b"other\n");
        let dest =
            TransferRepo::open_or_create(&temp.path().join("dest-cache"), &dest_common).unwrap();
        let error = dest
            .import_frozen_source(&runner, REQUEST_ID, truncated, &oid)
            .unwrap_err();
        assert_eq!(error.public_code(), "BASE_UNAVAILABLE");
        assert!(!dest.contains_commit(&runner, &oid).unwrap());
        assert!(!dest.has_ref(&TransferRepo::frozen_request_ref(REQUEST_ID).unwrap()));
        assert!(!dest.path().join("mw-frozen.bundle").exists());
        assert!(scratch_leftovers(&dest).is_empty());
        assert!(incoming_refs(&dest).is_empty());
    }

    #[test]
    fn concurrent_imports_use_unique_scratch_and_do_not_clobber() {
        let temp = tempfile::tempdir().unwrap();
        let runner = SystemProcessRunner;
        let (source_common, oid) = committed_user_repository(temp.path(), "source", b"hello\n");
        let source =
            TransferRepo::open_or_create(&temp.path().join("source-cache"), &source_common)
                .unwrap();
        let first_bundle = source
            .bundle_frozen_source(&runner, REQUEST_ID, &oid)
            .unwrap();
        let second_id = "018f0f4a6b5c7d8e9f00112233445577";
        let second_bundle = source
            .bundle_frozen_source(&runner, second_id, &oid)
            .unwrap();
        let (dest_common, _) = committed_user_repository(temp.path(), "dest", b"other\n");
        let dest =
            TransferRepo::open_or_create(&temp.path().join("dest-cache"), &dest_common).unwrap();

        std::thread::scope(|scope| {
            let left =
                scope.spawn(|| dest.import_frozen_source(&runner, REQUEST_ID, &first_bundle, &oid));
            let right =
                scope.spawn(|| dest.import_frozen_source(&runner, second_id, &second_bundle, &oid));
            left.join().unwrap().unwrap();
            right.join().unwrap().unwrap();
        });
        assert!(dest.contains_commit(&runner, &oid).unwrap());
        assert!(dest.has_ref(&TransferRepo::frozen_request_ref(REQUEST_ID).unwrap()));
        assert!(dest.has_ref(&TransferRepo::frozen_request_ref(second_id).unwrap()));
        assert!(!dest.path().join("mw-frozen.bundle").exists());
        assert!(
            scratch_leftovers(&dest).is_empty(),
            "{:?}",
            scratch_leftovers(&dest)
        );
        assert!(
            incoming_refs(&dest).is_empty(),
            "{:?}",
            incoming_refs(&dest)
        );
    }

    #[test]
    fn import_rejects_a_bundle_advertising_a_different_request_ref() {
        let temp = tempfile::tempdir().unwrap();
        let runner = SystemProcessRunner;
        let (source_common, oid) = committed_user_repository(temp.path(), "source", b"hello\n");
        let source =
            TransferRepo::open_or_create(&temp.path().join("source-cache"), &source_common)
                .unwrap();
        let bundle = source
            .bundle_frozen_source(&runner, REQUEST_ID, &oid)
            .unwrap();
        let second_id = "018f0f4a6b5c7d8e9f00112233445577";
        let (dest_common, _) = committed_user_repository(temp.path(), "dest", b"other\n");
        let dest =
            TransferRepo::open_or_create(&temp.path().join("dest-cache"), &dest_common).unwrap();
        let error = dest
            .import_frozen_source(&runner, second_id, &bundle, &oid)
            .unwrap_err();
        assert_eq!(error.public_code(), "BASE_UNAVAILABLE");
        assert!(!dest.contains_commit(&runner, &oid).unwrap());
        assert!(!dest.has_ref(&TransferRepo::frozen_request_ref(REQUEST_ID).unwrap()));
        assert!(!dest.has_ref(&TransferRepo::frozen_request_ref(second_id).unwrap()));
        assert!(
            incoming_refs(&dest).is_empty(),
            "{:?}",
            incoming_refs(&dest)
        );
        assert!(scratch_leftovers(&dest).is_empty());
    }

    fn additional_commit(user_git_dir: &Path, contents: &[u8]) -> BaseOid {
        let user = user_git_dir.parent().unwrap();
        fs::write(user.join("README"), contents).unwrap();
        git_in(user, &["add", "README"]);
        git_in(user, &["commit", "-q", "-m", "next"]);
        git_in(user, &["rev-parse", "HEAD"])
            .stdout
            .as_slice()
            .iter()
            .copied()
            .filter(|byte| *byte != b'\n')
            .map(char::from)
            .collect::<String>()
            .parse::<BaseOid>()
            .unwrap()
    }

    fn clone_user_repository(root: &Path, name: &str, source_git_dir: &Path) -> PathBuf {
        let source = source_git_dir.parent().unwrap();
        let dest = root.join(name);
        run_system_git(
            None,
            &[
                OsString::from("clone"),
                OsString::from("-q"),
                source.as_os_str().to_os_string(),
                dest.as_os_str().to_os_string(),
            ],
            None,
            None,
        )
        .unwrap();
        dest.join(".git")
    }

    fn hide_user_objects(user_git_dir: &Path) {
        let objects = user_git_dir.join("objects");
        let hidden = user_git_dir.join(format!("objects-unavailable-{}", Uuid::new_v4().simple()));
        fs::rename(&objects, &hidden).unwrap();
        fs::create_dir(&objects).unwrap();
        fs::create_dir(objects.join("pack")).unwrap();
        fs::create_dir(objects.join("info")).unwrap();
    }

    fn assert_owned_reachable_graph(
        repo: &TransferRepo,
        runner: &SystemProcessRunner,
        oid: &BaseOid,
    ) {
        assert!(
            repo.owned_graph_complete(runner, oid).unwrap(),
            "reachable graph must live in transfer-owned objects"
        );
        assert!(repo.contains_commit(runner, oid).unwrap());
        repo.require_owned_frozen_commit(runner, oid).unwrap();
        let listing = run_system_git(
            Some(repo.path()),
            &[
                OsString::from("rev-list"),
                OsString::from("--objects"),
                OsString::from(oid.as_str()),
            ],
            None,
            None,
        )
        .unwrap();
        let stdout = String::from_utf8(listing.stdout).unwrap();
        assert!(
            stdout.contains(oid.as_str()),
            "rev-list must include the frozen commit: {stdout:?}"
        );
        let mut saw_tree = false;
        let mut saw_blob = false;
        for line in stdout.lines() {
            let Some(object) = line.split_whitespace().next() else {
                continue;
            };
            if object.len() != 40 {
                continue;
            }
            let kind = run_system_git(
                Some(repo.path()),
                &[
                    OsString::from("cat-file"),
                    OsString::from("-t"),
                    OsString::from(object),
                ],
                None,
                None,
            )
            .unwrap();
            match String::from_utf8(kind.stdout).unwrap().trim() {
                "tree" => saw_tree = true,
                "blob" => saw_blob = true,
                _ => {}
            }
        }
        assert!(saw_tree, "reachable graph must include a tree");
        assert!(saw_blob, "reachable graph must include a blob");
    }

    fn blob_oid(user_git_dir: &Path, contents: &[u8]) -> BaseOid {
        let user = user_git_dir.parent().unwrap();
        fs::write(user.join("blob-fixture"), contents).unwrap();
        git_in(user, &["hash-object", "-w", "blob-fixture"])
            .stdout
            .as_slice()
            .iter()
            .copied()
            .filter(|byte| *byte != b'\n')
            .map(char::from)
            .collect::<String>()
            .parse::<BaseOid>()
            .unwrap()
    }

    fn annotated_tag_oid(user_git_dir: &Path) -> BaseOid {
        let user = user_git_dir.parent().unwrap();
        git_in(user, &["tag", "-a", "frozen-tag", "-m", "not a commit"]);
        git_in(user, &["rev-parse", "refs/tags/frozen-tag"])
            .stdout
            .as_slice()
            .iter()
            .copied()
            .filter(|byte| *byte != b'\n')
            .map(char::from)
            .collect::<String>()
            .parse::<BaseOid>()
            .unwrap()
    }

    fn scratch_leftovers(repo: &TransferRepo) -> Vec<String> {
        fs::read_dir(repo.path().join("scratch"))
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with("bundle-"))
            .collect()
    }

    fn incoming_refs(repo: &TransferRepo) -> Vec<String> {
        let output = run_system_git(
            Some(repo.path()),
            &[
                OsString::from("for-each-ref"),
                OsString::from("--format=%(refname)"),
                OsString::from("refs/mac-worker/incoming"),
            ],
            None,
            None,
        )
        .unwrap();
        String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn pin_rejects_a_blob_masquerading_as_a_base_commit() {
        let temp = tempfile::tempdir().unwrap();
        let runner = SystemProcessRunner;
        let (common, commit) = committed_user_repository(temp.path(), "source", b"hello\n");
        let transfer = TransferRepo::open_or_create(&temp.path().join("cache"), &common).unwrap();
        assert!(transfer.contains_commit(&runner, &commit).unwrap());
        let blob = blob_oid(&common, b"not-a-commit\n");
        assert_eq!(
            transfer.object_type(&runner, &blob).unwrap().as_deref(),
            Some("blob")
        );
        assert!(!transfer.contains_commit(&runner, &blob).unwrap());
        let error = transfer
            .pin_frozen_source(&runner, REQUEST_ID, &blob)
            .unwrap_err();
        assert_eq!(error.public_code(), "BASE_UNAVAILABLE");
        match &error {
            WorkerError::Git { message, .. } => {
                assert!(message.contains("not a commit"), "{message}");
            }
            other => panic!("{other:?}"),
        }
        assert!(!transfer.has_ref(&TransferRepo::frozen_request_ref(REQUEST_ID).unwrap()));
    }

    #[test]
    fn pin_rejects_an_annotated_tag_masquerading_as_a_base_commit() {
        let temp = tempfile::tempdir().unwrap();
        let runner = SystemProcessRunner;
        let (common, commit) = committed_user_repository(temp.path(), "source", b"hello\n");
        let transfer = TransferRepo::open_or_create(&temp.path().join("cache"), &common).unwrap();
        let tag = annotated_tag_oid(&common);
        assert_ne!(tag.as_str(), commit.as_str());
        assert_eq!(
            transfer.object_type(&runner, &tag).unwrap().as_deref(),
            Some("tag")
        );
        assert!(!transfer.contains_commit(&runner, &tag).unwrap());
        let error = transfer
            .pin_frozen_source(&runner, REQUEST_ID, &tag)
            .unwrap_err();
        assert_eq!(error.public_code(), "BASE_UNAVAILABLE");
        assert!(!transfer.has_ref(&TransferRepo::frozen_request_ref(REQUEST_ID).unwrap()));
    }

    #[test]
    fn pin_is_create_or_same_and_rejects_a_different_oid() {
        let temp = tempfile::tempdir().unwrap();
        let runner = SystemProcessRunner;
        let (common, first) = committed_user_repository(temp.path(), "source", b"hello\n");
        let second = additional_commit(&common, b"second\n");
        let transfer = TransferRepo::open_or_create(&temp.path().join("cache"), &common).unwrap();
        let name = transfer
            .pin_frozen_source(&runner, REQUEST_ID, &first)
            .unwrap();
        transfer
            .pin_frozen_source(&runner, REQUEST_ID, &first)
            .unwrap();
        let error = transfer
            .pin_frozen_source(&runner, REQUEST_ID, &second)
            .unwrap_err();
        assert_eq!(error.public_code(), "FROZEN_SOURCE_CONFLICT");
        let current = transfer.read_ref_oid(&runner, &name).unwrap().unwrap();
        assert_eq!(current.as_str(), first.as_str());
    }

    #[test]
    fn same_oid_pin_syncs_packed_ref_storage() {
        let temp = tempfile::tempdir().unwrap();
        let runner = SystemProcessRunner;
        let (common, first) = committed_user_repository(temp.path(), "source", b"hello\n");
        let transfer = TransferRepo::open_or_create(&temp.path().join("cache"), &common).unwrap();
        let name = transfer
            .pin_frozen_source(&runner, REQUEST_ID, &first)
            .unwrap();
        run_system_git(
            Some(transfer.path()),
            &[OsString::from("pack-refs"), OsString::from("--all")],
            None,
            None,
        )
        .unwrap();
        assert!(!transfer.path().join(&name).exists());
        transfer
            .pin_frozen_source(&runner, REQUEST_ID, &first)
            .unwrap();
        let current = transfer.read_ref_oid(&runner, &name).unwrap().unwrap();
        assert_eq!(current.as_str(), first.as_str());
    }

    #[test]
    fn concurrent_differing_pins_keep_exactly_one_oid() {
        let temp = tempfile::tempdir().unwrap();
        let runner = SystemProcessRunner;
        let (common, first) = committed_user_repository(temp.path(), "source", b"hello\n");
        let second = additional_commit(&common, b"second\n");
        let transfer = TransferRepo::open_or_create(&temp.path().join("cache"), &common).unwrap();
        let name = TransferRepo::frozen_request_ref(REQUEST_ID).unwrap();

        let (left, right) = std::thread::scope(|scope| {
            let left = scope.spawn(|| transfer.pin_frozen_source(&runner, REQUEST_ID, &first));
            let right = scope.spawn(|| transfer.pin_frozen_source(&runner, REQUEST_ID, &second));
            (left.join().unwrap(), right.join().unwrap())
        });
        let outcomes = [left, right];
        let wins = outcomes.iter().filter(|result| result.is_ok()).count();
        let conflicts = outcomes
            .iter()
            .filter(|result| {
                result
                    .as_ref()
                    .err()
                    .is_some_and(|error| error.public_code() == "FROZEN_SOURCE_CONFLICT")
            })
            .count();
        assert_eq!(wins, 1, "{outcomes:?}");
        assert_eq!(conflicts, 1, "{outcomes:?}");
        let kept = transfer.read_ref_oid(&runner, &name).unwrap().unwrap();
        assert!(kept.as_str() == first.as_str() || kept.as_str() == second.as_str());
    }

    const DAG_RUN_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";

    #[test]
    fn has_object_through_an_alternate_is_not_owned_graph_proof() {
        let temp = tempfile::tempdir().unwrap();
        let runner = SystemProcessRunner;
        let (common, oid) = committed_user_repository(temp.path(), "source", b"hello\n");
        let transfer = TransferRepo::open_or_create(&temp.path().join("cache"), &common).unwrap();
        assert!(transfer.has_object(&oid));
        assert!(transfer.contains_commit(&runner, &oid).unwrap());
        assert!(!transfer.owned_graph_complete(&runner, &oid).unwrap());
        let name = TransferRepo::frozen_dag_ref(DAG_RUN_ID, "root").unwrap();
        transfer.pin_object(&runner, &name, &oid).unwrap();
        assert!(transfer.has_ref(&name));
        hide_user_objects(&common);
        assert_owned_reachable_graph(&transfer, &runner, &oid);
    }

    #[test]
    fn dag_from_binding_pin_copies_overlapping_parent_blobs_out_of_the_alternate() {
        let temp = tempfile::tempdir().unwrap();
        let runner = SystemProcessRunner;
        let (common, first) = committed_user_repository(temp.path(), "source", b"hello\n");
        let second = additional_commit(&common, b"second\n");
        let transfer = TransferRepo::open_or_create(&temp.path().join("cache"), &common).unwrap();
        assert!(transfer.has_object(&second));
        assert!(!transfer.owned_graph_complete(&runner, &second).unwrap());
        let name = TransferRepo::frozen_dag_ref(DAG_RUN_ID, "child").unwrap();
        transfer.pin_object(&runner, &name, &second).unwrap();
        transfer.pin_object(&runner, &name, &second).unwrap();
        assert!(transfer.path().join("objects/info/alternates").is_file());
        hide_user_objects(&common);
        assert_owned_reachable_graph(&transfer, &runner, &second);
        assert_owned_reachable_graph(&transfer, &runner, &first);
        assert!(transfer.has_ref(&name));
    }

    #[test]
    fn dag_pin_is_create_or_same_and_rejects_a_different_oid() {
        let temp = tempfile::tempdir().unwrap();
        let runner = SystemProcessRunner;
        let (common, first) = committed_user_repository(temp.path(), "source", b"hello\n");
        let second = additional_commit(&common, b"second\n");
        let transfer = TransferRepo::open_or_create(&temp.path().join("cache"), &common).unwrap();
        let name = TransferRepo::frozen_dag_ref(DAG_RUN_ID, "root").unwrap();
        transfer.pin_object(&runner, &name, &first).unwrap();
        transfer.pin_object(&runner, &name, &first).unwrap();
        let error = transfer.pin_object(&runner, &name, &second).unwrap_err();
        assert_eq!(error.public_code(), "FROZEN_SOURCE_CONFLICT");
        let current = transfer.read_ref_oid(&runner, &name).unwrap().unwrap();
        assert_eq!(current.as_str(), first.as_str());
    }

    #[test]
    fn dag_pin_rejects_refs_outside_owned_namespaces() {
        let temp = tempfile::tempdir().unwrap();
        let runner = SystemProcessRunner;
        let (common, oid) = committed_user_repository(temp.path(), "source", b"hello\n");
        let transfer = TransferRepo::open_or_create(&temp.path().join("cache"), &common).unwrap();
        for name in [
            "refs/heads/main",
            "refs/mac-worker/other/root",
            "refs/mac-worker/dag/not-a-uuid/root",
            "refs/mac-worker/dag/018f0f4a6b5c7d8e9f00112233445566/BadId",
        ] {
            let error = transfer.pin_object(&runner, name, &oid).unwrap_err();
            assert_eq!(error.public_code(), "BASE_UNAVAILABLE", "{name}");
            assert!(!transfer.has_ref(name));
        }
        assert!(TransferRepo::frozen_dag_ref("not-a-uuid", "root").is_err());
        assert!(TransferRepo::frozen_dag_ref(DAG_RUN_ID, "BadId").is_err());
    }
}
