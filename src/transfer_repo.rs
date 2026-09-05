use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fs,
    io::{self, Read},
    os::unix::ffi::{OsStrExt, OsStringExt},
    path::{Path, PathBuf},
    process::{Command, Output},
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
const BASE_COMMIT_MESSAGE: &str = "mac-worker: task base";
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferRepo {
    path: PathBuf,
    repo_id: String,
    alternates_target: PathBuf,
}

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
        for raw_name in parent.list_names()? {
            if raw_name.as_slice() == b".mac-worker-rooted-fs" {
                continue;
            }
            let name = std::str::from_utf8(&raw_name).map_err(|_| {
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
                continue;
            }
            let repo = parent
                .open_child_directory(&relative_transfer(name)?, false)
                .map_err(WorkerError::Io)?;
            let refs = git_ref_names(self.runner, repo.path())?;
            if !transfer_refs_are_collectable(&refs) {
                continue;
            }
            let modified = crate::gc::rooted_modified_millis(&repo)?;
            if now_millis.saturating_sub(modified) < crate::gc::BRANCH_RETENTION_MILLIS {
                continue;
            }
            candidates.push(GcCandidate::new(
                "transfer_repo",
                repo_id,
                0,
                "transfer repository retention",
            )?);
        }
        candidates.sort_by(|left, right| left.identifier().cmp(right.identifier()));
        let mut applied = Vec::new();
        if apply {
            for candidate in &candidates {
                if self.apply_candidate(&parent, candidate, now_millis)? {
                    applied.push(candidate.clone());
                }
            }
        }
        Ok(GcReport::new(apply, candidates, applied, Vec::new()))
    }

    fn apply_candidate(
        &self,
        parent: &RootedDir,
        candidate: &GcCandidate,
        now_millis: u64,
    ) -> Result<bool, WorkerError> {
        let repo_id = candidate.identifier();
        if !is_lower_hex(repo_id, 64) || self.protected_repo_ids.contains(repo_id) {
            return Ok(false);
        }
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
        let _transfer_ns = open_or_create_owner_only_dir(&transfer_parent)?;
        let path = transfer_parent.join(format!("{repo_id}.git"));
        let repo_dir = open_or_create_owner_only_dir(&path)?;
        if !repo_dir.entry_exists("HEAD")? {
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
        }
        chmod_owner_only(&repo_dir)?;
        let _scratch = repo_dir.open_child_directory(&relative("scratch")?, true)?;
        let info = open_owner_only_subdir(&path, &["objects", "info"])?;
        write_alternates_atomic(&info, &alternates_target)?;
        let transfer = Self {
            path,
            repo_id,
            alternates_target,
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

    pub fn verify_alternates(&self) -> Result<(), WorkerError> {
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
        let first = self.capture_tree(runner, context, settings)?;
        hook()?;
        let second = self.capture_tree(runner, context, settings)?;
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
            for entry in &selection.entries {
                if entry.kind == SelectedInputKind::EmptyDirectory {
                    continue;
                }
                match entry.origin {
                    crate::inputs::InputOrigin::Tracked => dirty.modified += 1,
                    crate::inputs::InputOrigin::IncludedUntracked
                    | crate::inputs::InputOrigin::IncludedIgnored => dirty.added += 1,
                }
                let (mode, oid) = self.hash_worktree_entry(runner, &source, &entry.path)?;
                let cacheinfo = format!("{mode},{oid},{}", entry.path.as_str());
                self.transfer_git(
                    runner,
                    &[
                        OsString::from("update-index"),
                        OsString::from("--add"),
                        OsString::from("--cacheinfo"),
                        OsString::from(cacheinfo),
                    ],
                    Some(&scratch),
                    None,
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
        let _ = scratch_dir.remove_owned_child(&index_name);
        captured
    }

    fn hash_worktree_entry(
        &self,
        runner: &dyn ProcessRunner,
        source: &RootedDir,
        path: &RelativePath,
    ) -> Result<(&'static str, String), WorkerError> {
        let mut inspection = source.inspect(path)?;
        match inspection.kind {
            EntryKind::Symlink => {
                let target = source.read_symlink(path)?;
                let output = self.transfer_git(
                    runner,
                    &[
                        OsString::from("hash-object"),
                        OsString::from("-w"),
                        OsString::from("--stdin"),
                    ],
                    None,
                    Some(target.into_bytes()),
                )?;
                Ok(("120000", parse_hex_oid(&output)?))
            }
            EntryKind::RegularFile => {
                let mut bytes = Vec::new();
                inspection.read_to_end(&mut bytes)?;
                let mode = if inspection.mode & 0o111 != 0 {
                    "100755"
                } else {
                    "100644"
                };
                let output = self.transfer_git(
                    runner,
                    &[
                        OsString::from("hash-object"),
                        OsString::from("-w"),
                        OsString::from("--no-filters"),
                        OsString::from("--stdin"),
                    ],
                    None,
                    Some(bytes),
                )?;
                Ok((mode, parse_hex_oid(&output)?))
            }
        }
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
    let dir = match RootedDir::open(path) {
        Ok(dir) => dir,
        Err(error) if error.kind() == io::ErrorKind::NotFound => RootedDir::create(path)?,
        Err(error) => return Err(error),
    };
    chmod_owner_only(&dir)?;
    Ok(dir)
}

fn open_owner_only_subdir(root: &Path, components: &[&str]) -> Result<RootedDir, WorkerError> {
    let mut current = root.to_path_buf();
    let mut dir = RootedDir::open(root)?;
    chmod_owner_only(&dir)?;
    for component in components {
        current.push(component);
        dir = match RootedDir::open(&current) {
            Ok(opened) => opened,
            Err(error) if error.kind() == io::ErrorKind::NotFound => RootedDir::create(&current)?,
            Err(error) => return Err(WorkerError::Io(error)),
        };
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
    info.write_private_atomic_no_replace("alternates", &bytes)?;
    Ok(())
}

fn git_error(code: &'static str, message: &str) -> WorkerError {
    WorkerError::Git {
        code,
        message: message.into(),
    }
}

fn task_config(message: impl Into<String>) -> WorkerError {
    WorkerError::Task {
        code: "TASK_CONFIG_INVALID",
        message: message.into(),
    }
}
