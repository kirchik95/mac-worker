//! Controlled Git: H supplies attributes and merge sides; commit parents are T,H.
use super::contracts::*;
use super::host_store::{HostIntegrationStore, invalid};
use crate::{
    error::WorkerError,
    git_transport::{GitTransport, git_request_with_config, origin_git_ssh_command},
    host_store::HostStore,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    rooted_fs::RootedDir,
    task::{BaseOid, TaskId},
};
use std::{
    collections::BTreeSet,
    ffi::OsString,
    time::{Duration, Instant},
};

const HARDENING: &[(&str, &str)] = &[
    ("core.hooksPath", "/dev/null"),
    ("core.fsmonitor", "false"),
    ("commit.gpgSign", "false"),
    ("submodule.recurse", "false"),
    ("merge.autoStash", "false"),
    ("merge.verifySignatures", "false"),
    ("fetch.recurseSubmodules", "false"),
    ("push.followTags", "false"),
    ("push.recurseSubmodules", "no"),
    ("core.attributesFile", "/dev/null"),
    ("core.logAllRefUpdates", "false"),
    ("core.fsync", "objects,derived-metadata,reference"),
    ("core.fsyncMethod", "fsync"),
];
const DRIVER_PATTERN: &str =
    "^(filter\\..*\\.(clean|smudge|process|required)|merge\\..*\\.(driver|recursive))$";

/// Read-only laptop preflight uses the same overrides as host integration Git.
/// Driver discovery is a bounded local config read; it never runs a driver.
pub(crate) fn hardened_read_request(
    runner: &dyn ProcessRunner,
    repo: &RootedDir,
    operation: Vec<OsString>,
) -> Result<ProcessRequest, WorkerError> {
    repo.verify_bound()?;
    let mut config: Vec<_> = HARDENING
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect();
    let read_policy = |mut request: ProcessRequest| {
        request.policy.deadline = Duration::from_secs(30);
        request
            .environment_remove
            .extend(["GIT_NAMESPACE".into(), "GIT_SHALLOW_FILE".into()]);
        request.environment.extend([
            ("GIT_ATTR_NOSYSTEM".into(), "1".into()),
            ("GIT_NO_LAZY_FETCH".into(), "1".into()),
            ("GIT_NO_REPLACE_OBJECTS".into(), "1".into()),
            ("GIT_GRAFT_FILE".into(), "/dev/null".into()),
        ]);
        request
    };
    let probe = read_policy(git_request_with_config(
        repo.path(),
        None,
        &config,
        vec![
            "config".into(),
            "--null".into(),
            "--name-only".into(),
            "--get-regexp".into(),
            DRIVER_PATTERN.into(),
        ],
    ));
    let names = runner.run(&probe)?;
    config.extend(driver_overrides(&names)?);
    repo.verify_bound()?;
    Ok(read_policy(git_request_with_config(
        repo.path(),
        Some(origin_git_ssh_command()?),
        &config,
        operation,
    )))
}

fn driver_overrides(names: &ProcessResult) -> Result<Vec<(String, String)>, WorkerError> {
    if !matches!(names.status.code(), Some(0 | 1)) || names.stdout.len() > GIT_OUTPUT_BYTES {
        return Err(invalid());
    }
    let mut drivers = BTreeSet::new();
    for raw in names.stdout.split(|b| *b == 0).filter(|s| !s.is_empty()) {
        let key = std::str::from_utf8(raw).map_err(|_| invalid())?;
        let (prefix, _) = key.rsplit_once('.').ok_or_else(invalid)?;
        if prefix.len() > 256 || prefix.chars().any(|c| c.is_control() || c == '=') {
            return Err(invalid());
        }
        drivers.insert(prefix.to_owned());
    }
    if drivers.len() > 64 {
        return Err(invalid());
    }
    let mut config = Vec::new();
    for driver in drivers {
        if driver.starts_with("filter.") {
            for field in ["clean", "smudge", "process"] {
                config.push((format!("{driver}.{field}"), String::new()));
            }
            config.push((format!("{driver}.required"), "false".into()));
        } else {
            // Preserve directional built-in union. A configured binary
            // replacement is unsupported and fails closed.
            if driver == "merge.binary" {
                return Err(invalid());
            }
            let command = if driver == "merge.union" {
                "/usr/bin/git merge-file --union %A %O %B"
            } else {
                "/usr/bin/git merge-file %A %O %B"
            };
            config.push((format!("{driver}.driver"), command.into()));
            config.push((
                format!("{driver}.recursive"),
                if driver == "merge.union" {
                    "union"
                } else {
                    "text"
                }
                .into(),
            ));
        }
    }
    Ok(config)
}
#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkspaceState {
    pub candidate: IntegrationCandidateId,
    pub index_digest: String,
    pub worktree_digest: String,
}
pub(crate) enum PushOutcome {
    Integrated(IntegrationReceipt),
    Moved(BaseOid),
}
// Reuse the existing credential factory while including its 5 s reads in the
// same interruptible host budget. Ordinary GitTransport behavior is unchanged.
struct CredentialRunner<'a, 'b>(&'a IntegrationGit<'b>);
impl ProcessRunner for CredentialRunner<'_, '_> {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let mut bounded = request.clone();
        bounded.policy.deadline = bounded.policy.deadline.min(self.0.remaining());
        self.0
            .runner
            .run_interruptible(&bounded, &|| self.0.stopped())
    }
}
pub struct IntegrationGit<'a> {
    store: &'a HostStore,
    runner: &'a dyn ProcessRunner,
    runtime: Option<&'a dyn IntegrationRuntime>,
    started: Instant,
    runtime_started: u64,
}
impl<'a> IntegrationGit<'a> {
    pub fn new(
        store: &'a HostStore,
        runner: &'a dyn ProcessRunner,
        runtime: &'a dyn IntegrationRuntime,
    ) -> Self {
        Self {
            store,
            runner,
            runtime: Some(runtime),
            started: Instant::now(),
            runtime_started: runtime.now_millis(),
        }
    }
    pub(crate) fn for_workspace(store: &'a HostStore, runner: &'a dyn ProcessRunner) -> Self {
        Self {
            store,
            runner,
            runtime: None,
            started: Instant::now(),
            runtime_started: 0,
        }
    }
    fn reach(&self, hook: IntegrationHook) {
        if let Some(runtime) = self.runtime {
            runtime.reach(hook);
        }
    }
    fn now_millis(&self) -> u64 {
        self.runtime.map_or(0, IntegrationRuntime::now_millis)
    }
    fn remaining(&self) -> Duration {
        let elapsed = self.started.elapsed().max(Duration::from_millis(
            self.now_millis().saturating_sub(self.runtime_started),
        ));
        HOST_DEADLINE.saturating_sub(elapsed)
    }
    fn stopped(&self) -> bool {
        self.remaining().is_zero()
    }
    fn credentials(&self, origin: &str) -> Vec<(String, String)> {
        GitTransport::new(&CredentialRunner(self)).origin_credential_config(origin)
    }
    pub(crate) fn validate_prepared_workspace(
        &self,
        record: &IntegrationRecord,
        prepared: &PreparedIntegrationTurn,
    ) -> Result<(), WorkerError> {
        prepared.validate_for(record)?;
        prepared.followup.validate_self_consistency()?;
        if record.tombstone.is_some() {
            return Err(invalid());
        }
        let candidate = record.candidates.last().ok_or_else(invalid)?;
        prepared.workspace_binding.validate_for(candidate)?;
        let workspace = self.workspace(record)?;
        self.assert_workspace(&workspace, candidate, true)?;
        if prepared.purpose == IntegrationTurnPurpose::Verify {
            self.verify_tree(&workspace, candidate)?;
        }
        let bytes = HostIntegrationStore::new(self.store)
            .read(
                &record.policy.project_id,
                record.task_id,
                "workspace.json",
                MAX_PRIVATE_RECORD_BYTES,
            )?
            .ok_or_else(invalid)?;
        let state: WorkspaceState = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if state != self.workspace_state(&workspace, candidate)? {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn push_candidate(
        &self,
        policy: &FrozenIntegrationPolicy,
        candidate: &IntegrationCandidate,
    ) -> Result<IntegrationReceipt, WorkerError> {
        policy.validate()?;
        candidate.validate()?;
        let task: TaskId = candidate
            .clean_h
            .branch
            .as_str()
            .strip_prefix("task/")
            .ok_or_else(invalid)?
            .parse()
            .map_err(|_| invalid())?;
        let record = HostIntegrationStore::new(self.store)
            .load(&policy.project_id, task)?
            .ok_or_else(invalid)?;
        if record.policy != *policy || record.candidates.last() != Some(candidate) {
            return Err(invalid());
        }
        let request = HostIntegrationRequest {
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            task_id: task,
            integration_id: Some(record.snapshot.integration_id),
            epoch: record.snapshot.epoch,
            revision: record.snapshot.revision,
            action: HostIntegrationAction::Step {
                step: IntegrationStep::Push,
                record: Box::new(record),
            },
        };
        match super::host::HostIntegrationService::new(
            self.store,
            self.runner,
            self.runtime.ok_or_else(invalid)?,
        )
        .execute(&request)?
        {
            HostIntegrationResponse::Integrated { receipt, .. } => Ok(receipt),
            HostIntegrationResponse::TargetMoved { .. } => {
                Err(IntegrationCode::IntegrationTargetMovedExhausted.error())
            }
            _ => Err(invalid()),
        }
    }
    pub(crate) fn mirror(
        &self,
        policy: &FrozenIntegrationPolicy,
    ) -> Result<RootedDir, WorkerError> {
        policy.validate()?;
        // The ordinary mirror accessor repairs config with its own Git runner.
        // Integration opens the existing rooted mirror without those commands;
        // all Git here goes through the hardened, bounded factory below.
        Ok(self
            .store
            .open_directory("repos", false)?
            .open_child_directory(
                &super::host_store::relative(&format!("{}.git", policy.project_id))?,
                false,
            )?)
    }
    fn request(
        &self,
        repo: &RootedDir,
        attrs: Option<&BaseOid>,
        operation: &[OsString],
        credentials: &[(String, String)],
    ) -> Result<ProcessRequest, WorkerError> {
        if self.stopped() {
            return Err(IntegrationCode::IntegrationNetwork.error());
        }
        repo.verify_bound()?;
        self.require_no_info_attributes(repo)?;
        let mut config: Vec<_> = HARDENING
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        config.extend_from_slice(credentials);
        let mut probe = git_request_with_config(
            repo.path(),
            None,
            &config,
            vec![
                "config".into(),
                "--null".into(),
                "--name-only".into(),
                "--get-regexp".into(),
                DRIVER_PATTERN.into(),
            ],
        );
        probe.policy.deadline = GIT_DEADLINE.min(self.remaining());
        probe
            .environment
            .push(("GIT_ATTR_NOSYSTEM".into(), "1".into()));
        let names = self
            .runner
            .run_interruptible(&probe, &|| self.stopped())
            .map_err(|_| IntegrationCode::IntegrationNetwork.error())?;
        config.extend(driver_overrides(&names)?);
        let mut args = Vec::new();
        if let Some(attrs) = attrs {
            args.push(format!("--attr-source={attrs}").into());
        }
        args.extend_from_slice(operation);
        let mut request =
            git_request_with_config(repo.path(), Some(origin_git_ssh_command()?), &config, args);
        request.policy.deadline = GIT_DEADLINE.min(self.remaining());
        request.policy.stdout_limit = GIT_OUTPUT_BYTES;
        request.policy.stderr_limit = GIT_OUTPUT_BYTES;
        request
            .environment
            .push(("GIT_ATTR_NOSYSTEM".into(), "1".into()));
        Ok(request)
    }
    fn require_no_info_attributes(&self, repo: &RootedDir) -> Result<(), WorkerError> {
        let git_path = if repo.entry_exists(".git")? {
            repo.path().join(".git")
        } else {
            repo.path().to_path_buf()
        };
        let info = match RootedDir::open(&git_path.join("info")) {
            Ok(info) => info,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => return Err(invalid()),
        };
        let metadata = info.root_metadata()?;
        if metadata.st_uid != unsafe { libc::geteuid() } || metadata.st_mode & 0o022 != 0 {
            return Err(invalid());
        }
        // Git-created files need not be owner-only; opening still forbids links and traversal.
        if info.entry_exists("attributes")? {
            let (_, _, size, _) =
                native_regular_digest(&info, "attributes", GIT_OUTPUT_BYTES as u64)?;
            if size != 0 {
                return Err(invalid());
            }
        }
        Ok(())
    }
    pub(crate) fn run(
        &self,
        repo: &RootedDir,
        attrs: Option<&BaseOid>,
        args: &[&str],
    ) -> Result<ProcessResult, WorkerError> {
        let request = self.request(
            repo,
            attrs,
            &args.iter().map(OsString::from).collect::<Vec<_>>(),
            &[],
        )?;
        let result = self
            .runner
            .run_interruptible(&request, &|| self.stopped())
            .map_err(|_| IntegrationCode::IntegrationNetwork.error())?;
        repo.verify_bound()?;
        Ok(result)
    }
    pub(crate) fn query(
        &self,
        repo: &RootedDir,
        attrs: Option<&BaseOid>,
        args: &[&str],
    ) -> Result<String, WorkerError> {
        let result = self.run(repo, attrs, args)?;
        if !result.status.success() {
            return Err(invalid());
        }
        Ok(std::str::from_utf8(&result.stdout)
            .map_err(|_| invalid())?
            .trim()
            .to_owned())
    }
    pub(crate) fn is_ancestor(
        &self,
        repo: &RootedDir,
        ancestor: &BaseOid,
        head: &BaseOid,
    ) -> Result<bool, WorkerError> {
        let result = self.run(
            repo,
            None,
            &[
                "merge-base",
                "--is-ancestor",
                ancestor.as_str(),
                head.as_str(),
            ],
        )?;
        match result.status.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => Err(invalid()),
        }
    }
    pub(crate) fn fetch_target(&self, record: &IntegrationRecord) -> Result<BaseOid, WorkerError> {
        let mirror = self.mirror(&record.policy)?;
        let credentials = self.credentials(&record.policy.origin);
        self.observe(record, &mirror, &credentials)?
            .ok_or_else(|| IntegrationCode::IntegrationTargetMissing.error())
    }
    fn observe(
        &self,
        record: &IntegrationRecord,
        mirror: &RootedDir,
        credentials: &[(String, String)],
    ) -> Result<Option<BaseOid>, WorkerError> {
        let reference = format!("refs/heads/{}", record.policy.target);
        let request = self.request(
            mirror,
            None,
            &[
                "ls-remote".into(),
                record.policy.origin.clone().into(),
                reference.clone().into(),
            ],
            credentials,
        )?;
        let result = self
            .runner
            .run_interruptible(&request, &|| self.stopped())
            .map_err(|_| IntegrationCode::IntegrationNetwork.error())?;
        if !result.status.success() {
            return Err(IntegrationCode::IntegrationNetwork.error());
        }
        let text = std::str::from_utf8(&result.stdout).map_err(|_| invalid())?;
        let mut observed = None;
        for line in text.lines() {
            let (oid, name) = line.split_once('\t').ok_or_else(invalid)?;
            if name != reference || observed.is_some() {
                return Err(invalid());
            }
            observed = Some(oid.parse::<BaseOid>().map_err(|_| invalid())?);
        }
        let Some(oid) = observed else { return Ok(None) };
        let pin = format!(
            "refs/mac-worker/integration/{}/{}/observed",
            record.snapshot.integration_id, record.snapshot.epoch
        );
        let request = self.request(
            mirror,
            None,
            &[
                "fetch".into(),
                "--no-tags".into(),
                "--no-recurse-submodules".into(),
                record.policy.origin.clone().into(),
                oid.to_string().into(),
            ],
            credentials,
        )?;
        let result = self
            .runner
            .run_interruptible(&request, &|| self.stopped())
            .map_err(|_| IntegrationCode::IntegrationNetwork.error())?;
        if !result.status.success() {
            return Err(IntegrationCode::IntegrationNetwork.error());
        }
        self.reach(IntegrationHook::AfterFetchBeforePin);
        self.query(mirror, None, &["update-ref", &pin, oid.as_str()])?;
        mirror.sync_root()?;
        self.reach(IntegrationHook::AfterTargetPin);
        Ok(Some(oid))
    }
    pub(crate) fn merge(
        &self,
        record: &IntegrationRecord,
        candidate: &mut IntegrationCandidate,
    ) -> Result<(), WorkerError> {
        let mirror = self.mirror(&record.policy)?;
        let result = self.run(
            &mirror,
            Some(&candidate.attribute_source),
            &[
                "merge-tree",
                "--write-tree",
                "--name-only",
                "-z",
                candidate.ours.as_str(),
                candidate.theirs.as_str(),
            ],
        )?;
        if !matches!(result.status.code(), Some(0 | 1)) {
            return Err(invalid());
        }
        let mut fields = result.stdout.split(|b| *b == 0);
        let tree = std::str::from_utf8(fields.next().ok_or_else(invalid)?)
            .map_err(|_| invalid())?
            .parse()
            .map_err(|_| invalid())?;
        candidate.conflict_paths = if result.status.success() {
            vec![]
        } else {
            fields
                .take_while(|field| !field.is_empty())
                .map(|field| {
                    std::str::from_utf8(field)
                        .map(str::to_owned)
                        .map_err(|_| IntegrationCode::IntegrationConflictListTooLarge.error())
                })
                .collect::<Result<_, _>>()?
        };
        validate_conflict_paths(&candidate.conflict_paths)?;
        if candidate.conflict_paths.is_empty() {
            candidate.tree_oid = Some(tree);
            self.commit(record, candidate)?;
        }
        Ok(())
    }
    pub(crate) fn workspace(&self, record: &IntegrationRecord) -> Result<RootedDir, WorkerError> {
        self.store
            .open_task_workspace(&record.policy.project_id, record.task_id)
            .map_err(|_| IntegrationCode::IntegrationWorkspaceMissing.error())
    }
    pub(crate) fn assert_workspace(
        &self,
        workspace: &RootedDir,
        candidate: &IntegrationCandidate,
        require_merge: bool,
    ) -> Result<(), WorkerError> {
        if self.query(
            workspace,
            Some(&candidate.attribute_source),
            &["symbolic-ref", "--short", "HEAD"],
        )? != candidate.clean_h.branch.as_str()
            || self.query(
                workspace,
                Some(&candidate.attribute_source),
                &["rev-parse", "HEAD"],
            )? != candidate.source_head.as_str()
        {
            return Err(invalid());
        }
        if require_merge
            && self.query(
                workspace,
                Some(&candidate.attribute_source),
                &["rev-parse", "MERGE_HEAD"],
            )? != candidate.target_head.as_str()
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub(crate) fn untracked(
        &self,
        workspace: &RootedDir,
        attrs: &BaseOid,
    ) -> Result<Vec<String>, WorkerError> {
        let result = self.run(workspace, Some(attrs), &["ls-files", "--others", "-z"])?;
        if !result.status.success() {
            return Err(invalid());
        }
        paths(&result.stdout)
    }
    pub(crate) fn require_clean_source(
        &self,
        record: &IntegrationRecord,
        candidate: &IntegrationCandidate,
    ) -> Result<(), WorkerError> {
        let workspace = self.workspace(record)?;
        self.assert_workspace(&workspace, candidate, false)?;
        if !self
            .query(
                &workspace,
                Some(&candidate.attribute_source),
                &["status", "--porcelain=v1", "--untracked-files=no"],
            )?
            .is_empty()
        {
            return Err(invalid());
        }
        let untracked = self.untracked(&workspace, &candidate.attribute_source)?;
        if let Some(tree) = &candidate.tree_oid
            && !untracked.is_empty()
        {
            let files = self.run(
                &self.mirror(&record.policy)?,
                None,
                &["ls-tree", "-r", "--name-only", "-z", tree.as_str()],
            )?;
            if !files.status.success() {
                return Err(invalid());
            }
            for raw in files
                .stdout
                .split(|byte| *byte == 0)
                .filter(|path| !path.is_empty())
            {
                let path = std::str::from_utf8(raw).map_err(|_| invalid())?;
                validate_relative_path(path)?;
                if untracked.iter().any(|file| {
                    file == path
                        || file
                            .strip_prefix(path)
                            .is_some_and(|tail| tail.starts_with('/'))
                        || path
                            .strip_prefix(file)
                            .is_some_and(|tail| tail.starts_with('/'))
                }) {
                    return Err(invalid());
                }
            }
        }
        Ok(())
    }
    pub(crate) fn workspace_state(
        &self,
        workspace: &RootedDir,
        candidate: &IntegrationCandidate,
    ) -> Result<WorkspaceState, WorkerError> {
        use sha2::{Digest, Sha256};
        let index = self.run(
            workspace,
            Some(&candidate.attribute_source),
            &["ls-files", "--stage", "-z"],
        )?;
        let diff = self.run(
            workspace,
            Some(&candidate.attribute_source),
            &["diff", "--no-ext-diff", "--no-textconv", "--binary", "--"],
        )?;
        if !index.status.success() || !diff.status.success() {
            return Err(invalid());
        }
        let tracked = self.run(
            workspace,
            Some(&candidate.attribute_source),
            &["ls-files", "--cached", "-z"],
        )?;
        if !tracked.status.success() {
            return Err(invalid());
        }
        let mut files: BTreeSet<String> = self
            .untracked(workspace, &candidate.attribute_source)?
            .into_iter()
            .collect();
        for path in tracked
            .stdout
            .split(|byte| *byte == 0)
            .filter(|path| !path.is_empty())
        {
            let path = std::str::from_utf8(path).map_err(|_| invalid())?;
            validate_relative_path(path)?;
            files.insert(path.to_owned());
        }
        let mut worktree = Sha256::new();
        worktree.update(diff.stdout);
        for path in files {
            validate_relative_path(&path)?;
            worktree.update((path.len() as u64).to_be_bytes());
            worktree.update(path.as_bytes());
            let parent = std::path::Path::new(&path)
                .parent()
                .filter(|p| !p.as_os_str().is_empty());
            let dir = match parent {
                Some(parent) => match RootedDir::open(&workspace.path().join(parent)) {
                    Ok(dir) => dir,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        worktree.update([0]);
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                },
                None => workspace.reopen()?,
            };
            let name = std::path::Path::new(&path)
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(invalid)?;
            if !dir.entry_exists(name)? {
                worktree.update([0]);
                continue;
            }
            let relative = super::host_store::relative(name)?;
            let entry = dir.inspect(&relative)?;
            match entry.kind {
                crate::rooted_fs::EntryKind::RegularFile => {
                    let (mode, digest, _, _) = native_regular_digest(&dir, name, 64 * 1024 * 1024)?;
                    worktree.update([1]);
                    worktree.update(mode.to_be_bytes());
                    worktree.update(digest);
                }
                crate::rooted_fs::EntryKind::Symlink => {
                    let target = dir.read_symlink(&relative)?;
                    if dir.inspect(&relative)?.metadata() != entry.metadata() {
                        return Err(invalid());
                    }
                    worktree.update([2]);
                    worktree.update(Sha256::digest(target.as_bytes()));
                }
            }
        }
        workspace.verify_bound()?;
        Ok(WorkspaceState {
            candidate: candidate.id,
            index_digest: format!("{:x}", Sha256::digest(index.stdout)),
            worktree_digest: format!("{:x}", worktree.finalize()),
        })
    }
    pub(crate) fn verify_tree(
        &self,
        workspace: &RootedDir,
        candidate: &IntegrationCandidate,
    ) -> Result<(), WorkerError> {
        let tree = self
            .query(
                workspace,
                Some(&candidate.attribute_source),
                &["write-tree"],
            )
            .map_err(|_| IntegrationCode::IntegrationVerifyTreeMismatch.error())?;
        if Some(tree.as_str()) != candidate.tree_oid.as_ref().map(BaseOid::as_str) {
            return Err(IntegrationCode::IntegrationVerifyTreeMismatch.error());
        }
        Ok(())
    }
    pub(crate) fn validate_accepted_workspace(
        &self,
        record: &IntegrationRecord,
        candidate: &IntegrationCandidate,
    ) -> Result<(), WorkerError> {
        let workspace = self.workspace(record)?;
        self.assert_workspace(&workspace, candidate, true)?;
        self.verify_tree(&workspace, candidate)?;
        let bytes = HostIntegrationStore::new(self.store)
            .read(
                &record.policy.project_id,
                record.task_id,
                "accepted-workspace.json",
                MAX_PRIVATE_RECORD_BYTES,
            )?
            .ok_or_else(invalid)?;
        let state: WorkspaceState = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if state != self.workspace_state(&workspace, candidate)? {
            return Err(IntegrationCode::IntegrationVerifyChangedTree.error());
        }
        Ok(())
    }
    pub(crate) fn prepare_workspace(
        &self,
        record: &IntegrationRecord,
        candidate: &IntegrationCandidate,
        purpose: IntegrationTurnPurpose,
    ) -> Result<(), WorkerError> {
        let workspace = self.workspace(record)?;
        self.assert_workspace(&workspace, candidate, false)?;
        let sidecars = HostIntegrationStore::new(self.store);
        if let Some(bytes) = sidecars.read(
            &record.policy.project_id,
            record.task_id,
            "workspace.json",
            MAX_PRIVATE_RECORD_BYTES,
        )? {
            let state: WorkspaceState = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            if state.candidate == candidate.id {
                self.assert_workspace(&workspace, candidate, true)?;
                if self.workspace_state(&workspace, candidate)? != state {
                    return Err(invalid());
                }
                if purpose == IntegrationTurnPurpose::Verify {
                    self.verify_tree(&workspace, candidate)?;
                }
                return Ok(());
            }
        }
        let merge_head = self.run(
            &workspace,
            Some(&candidate.attribute_source),
            &["rev-parse", "--verify", "MERGE_HEAD"],
        )?;
        if merge_head.status.success() {
            // A native merge may have completed before the baseline journal.
            // No admitted auxiliary can have edited this unjournaled attempt.
            if record
                .auxiliaries
                .iter()
                .any(|aux| aux.attempt == candidate.id.attempt)
                || std::str::from_utf8(&merge_head.stdout)
                    .map_err(|_| invalid())?
                    .trim()
                    != candidate.target_head.as_str()
            {
                return Err(invalid());
            }
            self.restore_source(record, candidate)?;
        }
        let mirror = self.mirror(&record.policy)?;
        self.query(
            &workspace,
            Some(&candidate.attribute_source),
            &[
                "fetch",
                "--no-tags",
                "--no-recurse-submodules",
                mirror.path().to_str().ok_or_else(invalid)?,
                candidate.target_head.as_str(),
            ],
        )?;
        self.reach(IntegrationHook::DuringWorkspacePrepare);
        let result = self.run(
            &workspace,
            Some(&candidate.attribute_source),
            &[
                "merge",
                "--no-ff",
                "--no-commit",
                candidate.target_head.as_str(),
            ],
        )?;
        if !matches!(result.status.code(), Some(0 | 1)) {
            return Err(invalid());
        }
        self.assert_workspace(&workspace, candidate, true)?;
        if purpose == IntegrationTurnPurpose::Verify {
            self.verify_tree(&workspace, candidate)?;
        }
        let state = self.workspace_state(&workspace, candidate)?;
        sidecars.write(
            &record.policy.project_id,
            record.task_id,
            "workspace.json",
            &serde_json::to_vec(&state).map_err(|_| invalid())?,
        )
    }
    pub(crate) fn commit(
        &self,
        record: &IntegrationRecord,
        candidate: &mut IntegrationCandidate,
    ) -> Result<(), WorkerError> {
        let mirror = self.mirror(&record.policy)?;
        self.commit_in(record, candidate, &mirror)
    }
    pub(crate) fn commit_in(
        &self,
        record: &IntegrationRecord,
        candidate: &mut IntegrationCandidate,
        repo: &RootedDir,
    ) -> Result<(), WorkerError> {
        let mirror = self.mirror(&record.policy)?;
        let tree = candidate.tree_oid.as_ref().ok_or_else(invalid)?;
        let sidecars = HostIntegrationStore::new(self.store);
        let mut journal = sidecars
            .load(&record.policy.project_id, record.task_id)?
            .ok_or_else(invalid)?;
        let frozen = journal.candidates.last_mut().ok_or_else(invalid)?;
        if frozen.id != candidate.id || frozen.tree_oid.as_ref().is_some_and(|old| old != tree) {
            return Err(invalid());
        }
        frozen.tree_oid = Some(tree.clone());
        // Freeze the accepted tree before creating an object or publishing its pin.
        // Replays use the same parents/message/identity/time already in this record.
        sidecars.save(&journal)?;
        let mut request = self.request(
            repo,
            Some(&candidate.attribute_source),
            &[
                "commit-tree".into(),
                tree.to_string().into(),
                "-p".into(),
                candidate.target_head.to_string().into(),
                "-p".into(),
                candidate.source_head.to_string().into(),
            ],
            &[],
        )?;
        request.stdin = Some(format!("{}\n", candidate.message).into_bytes());
        let date = format!("@{} +0000", candidate.timestamp_millis / 1000);
        for prefix in ["GIT_AUTHOR", "GIT_COMMITTER"] {
            request.environment.push((
                format!("{prefix}_NAME").into(),
                candidate.identity.name().into(),
            ));
            request.environment.push((
                format!("{prefix}_EMAIL").into(),
                candidate.identity.email().into(),
            ));
            request
                .environment
                .push((format!("{prefix}_DATE").into(), date.clone().into()));
        }
        let result = self
            .runner
            .run_interruptible(&request, &|| self.stopped())
            .map_err(|_| invalid())?;
        if !result.status.success() {
            return Err(invalid());
        }
        let oid: BaseOid = std::str::from_utf8(&result.stdout)
            .map_err(|_| invalid())?
            .trim()
            .parse()
            .map_err(|_| invalid())?;
        self.reach(IntegrationHook::AfterCommitBeforePin);
        if repo.path() != mirror.path() {
            self.query(
                &mirror,
                Some(&candidate.attribute_source),
                &[
                    "fetch",
                    "--no-tags",
                    "--no-recurse-submodules",
                    repo.path().to_str().ok_or_else(invalid)?,
                    oid.as_str(),
                ],
            )?;
        }
        let pin = candidate_pin(candidate);
        let current = self.run(&mirror, None, &["rev-parse", "--verify", "--quiet", &pin])?;
        if current.status.success() {
            if std::str::from_utf8(&current.stdout)
                .map_err(|_| invalid())?
                .trim()
                != oid.as_str()
            {
                return Err(invalid());
            }
        } else if current.status.code() == Some(1) {
            self.query(
                &mirror,
                None,
                &[
                    "update-ref",
                    &pin,
                    oid.as_str(),
                    &"0".repeat(oid.as_str().len()),
                ],
            )?;
        } else {
            return Err(invalid());
        }
        mirror.sync_root()?;
        candidate.merge_oid = Some(oid);
        self.reach(IntegrationHook::AfterMergePin);
        Ok(())
    }
    pub(crate) fn accept_workspace(
        &self,
        record: &IntegrationRecord,
        candidate: &mut IntegrationCandidate,
        purpose: IntegrationTurnPurpose,
    ) -> Result<(), WorkerError> {
        let workspace = self.workspace(record)?;
        self.assert_workspace(&workspace, candidate, true)?;
        if purpose == IntegrationTurnPurpose::Verify {
            self.verify_tree(&workspace, candidate)?;
            let bytes = HostIntegrationStore::new(self.store)
                .read(
                    &record.policy.project_id,
                    record.task_id,
                    "workspace.json",
                    MAX_PRIVATE_RECORD_BYTES,
                )?
                .ok_or_else(invalid)?;
            let expected: WorkspaceState = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            if self.workspace_state(&workspace, candidate)? != expected {
                return Err(IntegrationCode::IntegrationVerifyChangedTree.error());
            }
        }
        self.query(
            &workspace,
            Some(&candidate.attribute_source),
            &["add", "-A"],
        )?;
        let unmerged = self.query(
            &workspace,
            Some(&candidate.attribute_source),
            &["ls-files", "--unmerged", "-z"],
        )?;
        if !unmerged.is_empty() {
            return Err(IntegrationCode::IntegrationResolutionIncomplete.error());
        }
        if purpose == IntegrationTurnPurpose::Verify {
            self.verify_tree(&workspace, candidate)?;
        }
        let diff = self.run(
            &workspace,
            Some(&candidate.attribute_source),
            &[
                "diff",
                "--cached",
                "--no-ext-diff",
                "--no-textconv",
                "--no-color",
                "--output-indicator-new=+",
                "--output-indicator-old=-",
                "--output-indicator-context= ",
                "--unified=0",
                candidate.source_head.as_str(),
                "--",
            ],
        )?;
        if !diff.status.success() {
            return Err(invalid());
        }
        if added_text_markers(&diff.stdout) {
            return Err(IntegrationCode::IntegrationResolutionIncomplete.error());
        }
        for path in &candidate.conflict_paths {
            let index = self.query(
                &workspace,
                Some(&candidate.attribute_source),
                &[
                    "--literal-pathspecs",
                    "ls-files",
                    "--stage",
                    "-z",
                    "--",
                    path,
                ],
            )?;
            // Native index modes distinguish symlink/gitlink data and deletions.
            if index.is_empty() || index.starts_with("120000 ") || index.starts_with("160000 ") {
                continue;
            }
            let (_, _, _, binary) = native_regular_digest(&workspace, path, 64 * 1024 * 1024)?;
            if binary {
                continue;
            }
            let content = self.run(
                &workspace,
                Some(&candidate.attribute_source),
                &["show", &format!(":{path}")],
            )?;
            // A resolved deletion has no index blob. Symlinks/binaries are native Git data.
            if content.status.success()
                && !content.stdout.contains(&0)
                && content.stdout.split(|b| *b == b'\n').any(marker)
            {
                return Err(IntegrationCode::IntegrationResolutionIncomplete.error());
            }
        }
        if purpose == IntegrationTurnPurpose::Resolve {
            let resolved_tree = self
                .query(
                    &workspace,
                    Some(&candidate.attribute_source),
                    &["write-tree"],
                )?
                .parse()
                .map_err(|_| invalid())?;
            if candidate
                .tree_oid
                .as_ref()
                .is_some_and(|tree| tree != &resolved_tree)
            {
                return Err(invalid());
            }
            candidate.tree_oid = Some(resolved_tree);
        }
        self.commit_in(record, candidate, &workspace)?;
        let state = self.workspace_state(&workspace, candidate)?;
        HostIntegrationStore::new(self.store).write(
            &record.policy.project_id,
            record.task_id,
            "accepted-workspace.json",
            &serde_json::to_vec(&state).map_err(|_| invalid())?,
        )
    }
    pub(crate) fn push(
        &self,
        record: &IntegrationRecord,
        candidate: &IntegrationCandidate,
    ) -> Result<PushOutcome, WorkerError> {
        candidate.validate()?;
        self.assert_workspace(&self.workspace(record)?, candidate, false)?;
        let mirror = self.mirror(&record.policy)?;
        let merge = candidate.merge_oid.as_ref().ok_or_else(invalid)?;
        let parents = self.query(
            &mirror,
            None,
            &["show", "-s", "--format=%P", merge.as_str()],
        )?;
        if parents != format!("{} {}", candidate.target_head, candidate.source_head)
            || self.query(&mirror, None, &["rev-parse", &format!("{merge}^{{tree}}")])?
                != candidate.tree_oid.as_ref().ok_or_else(invalid)?.as_str()
            || self.query(&mirror, None, &["rev-parse", &candidate_pin(candidate)])?
                != merge.as_str()
            || !self.is_ancestor(&mirror, &candidate.target_head, merge)?
        {
            return Err(invalid());
        }
        let credentials = self.credentials(&record.policy.origin);
        // Every replay observes first, including a durable intent whose original reply was lost.
        if let Some(outcome) = self.observed_outcome(record, candidate, &mirror, &credentials)? {
            return Ok(outcome);
        }
        if !self.is_ancestor(&mirror, &record.cycle_base, &candidate.target_head)? {
            return Err(IntegrationCode::IntegrationBaseNotOnTarget.error());
        }
        if record.snapshot.verification == IntegrationVerification::SourceAgentReportOnly {
            self.require_clean_source(record, candidate)?;
        }
        self.reach(IntegrationHook::BeforePush);
        let status = self
            .store
            .task_status(&record.policy.project_id, record.task_id)?;
        let current = HostIntegrationStore::new(self.store)
            .load(&record.policy.project_id, record.task_id)?
            .ok_or_else(invalid)?;
        if status.state() != crate::task::TaskState::Open {
            return Err(IntegrationCode::IntegrationWorkspaceMissing.error());
        }
        if status.head_oid() != Some(&candidate.source_head)
            || current.tombstone.is_some()
            || current.candidates.last() != Some(candidate)
        {
            return Err(invalid());
        }
        let request = self.request(
            &mirror,
            None,
            &[
                "push".into(),
                "--porcelain".into(),
                "--no-verify".into(),
                format!(
                    "--force-with-lease=refs/heads/{}:{}",
                    record.policy.target, candidate.target_head
                )
                .into(),
                record.policy.origin.clone().into(),
                format!("{merge}:refs/heads/{}", record.policy.target).into(),
            ],
            &credentials,
        )?;
        let result = self.runner.run_interruptible(&request, &|| self.stopped());
        if let Ok(result) = &result
            && result.status.success()
        {
            self.reach(IntegrationHook::AfterPushBeforeReceipt);
            return Ok(PushOutcome::Integrated(self.receipt(
                record,
                candidate,
                candidate.target_head.clone(),
                IntegrationDisposition::Merged,
            )));
        }
        // A server CAS failure can look like policy rejection. Re-observe before reading that label.
        if let Some(outcome) = self.observed_outcome(record, candidate, &mirror, &credentials)? {
            return Ok(outcome);
        }
        let code = match result {
            Ok(result) if crate::git_transport::origin_auth_failed(&result.stderr) => {
                IntegrationCode::IntegrationAuthFailed
            }
            Ok(result)
                if result.stdout.split(|b| *b == b'\n').any(|line| {
                    line.starts_with(b"!\t")
                        && line.windows(17).any(|part| part == b"[remote rejected]")
                }) =>
            {
                IntegrationCode::IntegrationPolicyRejected
            }
            _ => IntegrationCode::IntegrationNetwork,
        };
        Err(code.error())
    }
    fn observed_outcome(
        &self,
        record: &IntegrationRecord,
        candidate: &IntegrationCandidate,
        mirror: &RootedDir,
        credentials: &[(String, String)],
    ) -> Result<Option<PushOutcome>, WorkerError> {
        let target = self
            .observe(record, mirror, credentials)?
            .ok_or_else(|| IntegrationCode::IntegrationTargetMissing.error())?;
        if self.is_ancestor(
            mirror,
            candidate.merge_oid.as_ref().ok_or_else(invalid)?,
            &target,
        )? {
            return Ok(Some(PushOutcome::Integrated(self.receipt(
                record,
                candidate,
                target,
                IntegrationDisposition::Merged,
            ))));
        }
        if self.is_ancestor(mirror, &candidate.source_head, &target)? {
            return Ok(Some(PushOutcome::Integrated(self.receipt(
                record,
                candidate,
                target,
                IntegrationDisposition::AlreadyIntegrated,
            ))));
        }
        if target != candidate.target_head {
            return Ok(Some(PushOutcome::Moved(target)));
        }
        Ok(None)
    }
    pub(crate) fn settle(
        &self,
        record: &IntegrationRecord,
    ) -> Result<Option<IntegrationReceipt>, WorkerError> {
        let mirror = self.mirror(&record.policy)?;
        let credentials = self.credentials(&record.policy.origin);
        let target = self
            .observe(record, &mirror, &credentials)?
            .ok_or_else(|| IntegrationCode::IntegrationTargetMissing.error())?;
        self.settle_target(record, &mirror, target)
    }
    pub(crate) fn settle_target(
        &self,
        record: &IntegrationRecord,
        mirror: &RootedDir,
        target: BaseOid,
    ) -> Result<Option<IntegrationReceipt>, WorkerError> {
        if let Some(candidate) = record.candidates.last()
            && let Some(merge) = &candidate.merge_oid
            && self.is_ancestor(mirror, merge, &target)?
        {
            return Ok(Some(self.receipt(
                record,
                candidate,
                target,
                IntegrationDisposition::Merged,
            )));
        }
        if self.is_ancestor(mirror, &record.snapshot.source_head, &target)? {
            return Ok(Some(IntegrationReceipt {
                integration_id: record.snapshot.integration_id,
                epoch: record.snapshot.epoch,
                source_turn_id: record.snapshot.source_turn_id,
                source_head: record.snapshot.source_head.clone(),
                target_head: target,
                merge_oid: None,
                disposition: IntegrationDisposition::AlreadyIntegrated,
                imported: false,
                recorded_at_millis: self.now_millis(),
            }));
        }
        Ok(None)
    }
    pub(crate) fn restore_source(
        &self,
        record: &IntegrationRecord,
        candidate: &IntegrationCandidate,
    ) -> Result<(), WorkerError> {
        if self
            .store
            .task_status(&record.policy.project_id, record.task_id)?
            .state()
            .is_terminal()
        {
            return Ok(());
        }
        let workspace = self.workspace(record)?;
        self.assert_workspace(&workspace, candidate, false)?;
        let before = self.untracked(&workspace, &candidate.attribute_source)?;
        let merge_head = self.run(
            &workspace,
            Some(&candidate.attribute_source),
            &["rev-parse", "--verify", "MERGE_HEAD"],
        )?;
        if !merge_head.status.success()
            && candidate.conflict_paths.is_empty()
            && !record
                .auxiliaries
                .iter()
                .any(|aux| aux.attempt == candidate.id.attempt)
        {
            let verify_workspace = if record.policy.verify == VerifyPolicy::MovedTarget
                && candidate.target_head != record.cycle_base
            {
                if let Some(tree) = &candidate.tree_oid {
                    self.query(
                        &self.mirror(&record.policy)?,
                        None,
                        &["rev-parse", &format!("{}^{{tree}}", candidate.source_head)],
                    )? != tree.as_str()
                } else {
                    false
                }
            } else {
                false
            };
            if !verify_workspace {
                return Ok(());
            }
        }
        if merge_head.status.success() {
            if std::str::from_utf8(&merge_head.stdout)
                .map_err(|_| invalid())?
                .trim()
                != candidate.target_head.as_str()
            {
                return Err(invalid());
            }
            self.query(
                &workspace,
                Some(&candidate.attribute_source),
                &["merge", "--abort"],
            )?;
        }
        let created: Vec<_> = self
            .untracked(&workspace, &candidate.attribute_source)?
            .into_iter()
            .filter(|path| {
                before.contains(path) && !candidate.clean_h.untracked_files.contains(path)
            })
            .collect();
        if !created.is_empty() {
            // Native staging/reset removes only inventoried auxiliary files, including symlinks and ignored files.
            let mut args = vec!["--literal-pathspecs", "add", "-f", "--"];
            args.extend(created.iter().map(String::as_str));
            self.query(&workspace, Some(&candidate.attribute_source), &args)?;
        }
        self.query(
            &workspace,
            Some(&candidate.attribute_source),
            &["reset", "--hard", candidate.source_head.as_str()],
        )?;
        workspace.sync_root()?;
        Ok(())
    }
    pub(crate) fn repair(
        &self,
        record: &IntegrationRecord,
        receipt: &IntegrationReceipt,
    ) -> Result<(), WorkerError> {
        let result = receipt.merge_oid.as_ref().unwrap_or(&receipt.target_head);
        let mirror = self.mirror(&record.policy)?;
        let reference = format!("refs/heads/task/{}", record.task_id);
        let current = self.query(&mirror, None, &["rev-parse", &reference])?;
        if current == record.snapshot.source_head.as_str() {
            self.query(
                &mirror,
                Some(result),
                &[
                    "update-ref",
                    &reference,
                    result.as_str(),
                    record.snapshot.source_head.as_str(),
                ],
            )?;
        } else if current != result.as_str() {
            return Err(invalid());
        }
        let status = self
            .store
            .task_status(&record.policy.project_id, record.task_id)?;
        if !status.state().is_terminal() {
            let workspace = self.workspace(record)?;
            if self.query(
                &workspace,
                Some(result),
                &["symbolic-ref", "--short", "HEAD"],
            )? != crate::task::BranchName::for_task(record.task_id).as_str()
            {
                return Err(invalid());
            }
            let head = self.query(&workspace, Some(result), &["rev-parse", "HEAD"])?;
            if head != record.snapshot.source_head.as_str() && head != result.as_str() {
                return Err(invalid());
            }
            self.query(
                &workspace,
                Some(result),
                &[
                    "fetch",
                    "--no-tags",
                    "--no-recurse-submodules",
                    mirror.path().to_str().ok_or_else(invalid)?,
                    result.as_str(),
                ],
            )?;
            if head != result.as_str() {
                self.query(
                    &workspace,
                    Some(result),
                    &["update-ref", &reference, result.as_str(), &head],
                )?;
            }
            self.query(
                &workspace,
                Some(result),
                &["reset", "--hard", result.as_str()],
            )?;
        }
        crate::task_store::TaskStore::new(self.store, self.runner).replace_status_after(
            &record.policy.project_id,
            record.task_id,
            |current| {
                if current.head_oid() != Some(&record.snapshot.source_head)
                    && current.head_oid() != Some(result)
                {
                    return Err(invalid());
                }
                if current.head_oid() == Some(result) {
                    return Ok(current);
                }
                crate::task::TaskStatus::new(
                    current.state(),
                    current.last_outcome().cloned(),
                    current.worker().map(str::to_owned),
                    current.session_present(),
                    Some(result.clone()),
                    current.summary().map(str::to_owned),
                    current.questions().to_vec(),
                    current.files_changed().to_vec(),
                    current.diff_stat().map(str::to_owned),
                    current.turns().to_vec(),
                    self.now_millis().max(
                        current
                            .updated_at_millis()
                            .checked_add(1)
                            .ok_or_else(invalid)?,
                    ),
                )?
                .copying_reported_checks(&current)
            },
        )?;
        mirror.sync_root()?;
        Ok(())
    }
    pub(crate) fn receipt(
        &self,
        record: &IntegrationRecord,
        candidate: &IntegrationCandidate,
        target: BaseOid,
        disposition: IntegrationDisposition,
    ) -> IntegrationReceipt {
        IntegrationReceipt {
            integration_id: record.snapshot.integration_id,
            epoch: record.snapshot.epoch,
            source_turn_id: record.snapshot.source_turn_id,
            source_head: record.snapshot.source_head.clone(),
            target_head: target,
            merge_oid: (disposition == IntegrationDisposition::Merged)
                .then(|| candidate.merge_oid.clone())
                .flatten(),
            disposition,
            imported: false,
            recorded_at_millis: self.now_millis(),
        }
    }
}
pub(crate) fn candidate_pin(candidate: &IntegrationCandidate) -> String {
    format!(
        "refs/mac-worker/integration/{}/{}/{}/merge",
        candidate.id.integration_id, candidate.id.epoch, candidate.id.attempt
    )
}
pub(crate) fn paths(bytes: &[u8]) -> Result<Vec<String>, WorkerError> {
    let paths: Vec<_> = bytes
        .split(|b| *b == 0)
        .filter(|s| !s.is_empty())
        .map(|raw| {
            let path = std::str::from_utf8(raw)
                .map_err(|_| IntegrationCode::IntegrationConflictListTooLarge.error())?
                .to_owned();
            validate_relative_path(&path)?;
            Ok(path)
        })
        .collect::<Result<_, WorkerError>>()?;
    validate_conflict_paths(&paths)?;
    Ok(paths)
}
// Read native Git files through rooted_fs's already-open, no-follow descriptor.
// Snapshot projections intentionally require different modes and cannot be used
// for a checkout's 0644/0755 files. Keep ownership/link/stability checks here.
fn native_regular_digest(
    dir: &RootedDir,
    name: &str,
    limit: u64,
) -> Result<(u32, [u8; 32], u64, bool), WorkerError> {
    use sha2::{Digest, Sha256};
    use std::{io::Read, os::unix::fs::MetadataExt};
    let relative = super::host_store::relative(name)?;
    let mut entry = dir.inspect(&relative)?;
    let before = entry.metadata();
    let metadata = entry.file.as_ref().ok_or_else(invalid)?.metadata()?;
    let root = dir.root_metadata()?;
    if metadata.uid() != unsafe { libc::geteuid() }
        || metadata.nlink() != 1
        || metadata.mode() & 0o022 != 0
        || metadata.dev() != root.st_dev as u64
        || root.st_uid != unsafe { libc::geteuid() }
        || root.st_mode & 0o022 != 0
        || metadata.len() > limit
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "unsafe native Git file",
        )
        .into());
    }
    let mut digest = Sha256::new();
    let mut total = 0u64;
    let mut binary = false;
    let mut buffer = [0u8; 16384];
    loop {
        let count = entry.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        total += count as u64;
        if total > limit {
            return Err(invalid());
        }
        digest.update(&buffer[..count]);
        binary |= buffer[..count].contains(&0);
    }
    let after = entry.file.as_ref().ok_or_else(invalid)?.metadata()?;
    if entry.restat()? != before
        || dir.inspect(&relative)?.metadata() != before
        || after.uid() != metadata.uid()
        || after.nlink() != 1
        || after.ctime() != metadata.ctime()
        || after.ctime_nsec() != metadata.ctime_nsec()
        || total != before.size
    {
        return Err(invalid());
    }
    dir.verify_bound()?;
    Ok((before.mode, digest.finalize().into(), total, binary))
}

fn added_text_markers(diff: &[u8]) -> bool {
    let mut symlink = false;
    let mut added_marker = false;
    for line in diff.split(|byte| *byte == b'\n') {
        if line.starts_with(b"diff --git ") {
            if added_marker && !symlink {
                return true;
            }
            symlink = false;
            added_marker = false;
        }
        symlink |= line == b"new file mode 120000"
            || line == b"new mode 120000"
            || (line.starts_with(b"index ") && line.ends_with(b" 120000"));
        added_marker |= line.starts_with(b"+") && marker(&line[1..]);
    }
    added_marker && !symlink
}

fn marker(line: &[u8]) -> bool {
    line.starts_with(b"<<<<<<<")
        || line.starts_with(b">>>>>>>")
        || line.starts_with(b"|||||||")
        || line == b"======="
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::{os::unix::process::ExitStatusExt, sync::Mutex};
    #[derive(Default)]
    struct FakeCredentials {
        calls: Mutex<Vec<ProcessRequest>>,
    }
    impl ProcessRunner for FakeCredentials {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            self.calls.lock().unwrap().push(request.clone());
            let stdout = if request.args.iter().any(|arg| arg == "--global") {
                match request.args.last().unwrap().to_str().unwrap() {
                    "credential.helper" => b"fixture-helper\n\n".to_vec(),
                    "credential.https://github.com.helper" => b"fixture-url-helper\n".to_vec(),
                    "credential.useHttpPath" => b"true\n".to_vec(),
                    _ => panic!("unexpected credential query"),
                }
            } else {
                vec![]
            };
            Ok(ProcessResult {
                status: std::process::ExitStatus::from_raw(0),
                stdout,
                stderr: vec![],
            })
        }
    }
    #[test]
    fn ssh_and_https_requests_preserve_only_explicit_credentials_in_the_hardened_factory() {
        let mut fixture = testing::GitIntegrationFixture::new();
        fixture.commit_base();
        fixture.commit_task();
        for origin in [
            "git@github.com:fixture/repo.git",
            "https://github.com/fixture/repo.git",
        ] {
            let runner = FakeCredentials::default();
            let git = IntegrationGit::new(&fixture.store, &runner, &fixture.runtime);
            let credentials = git.credentials(origin);
            let request = git
                .request(
                    &git.mirror(&fixture.record.policy).unwrap(),
                    None,
                    &["ls-remote".into(), origin.into(), "refs/heads/main".into()],
                    &credentials,
                )
                .unwrap();
            let env = |name: &str| {
                request
                    .environment
                    .iter()
                    .find(|(key, _)| key == name)
                    .unwrap()
                    .1
                    .to_string_lossy()
                    .into_owned()
            };
            assert_eq!(env("GIT_CONFIG_GLOBAL"), "/dev/null");
            assert_eq!(env("GIT_CONFIG_NOSYSTEM"), "1");
            assert_eq!(env("GIT_ATTR_NOSYSTEM"), "1");
            assert!(
                request
                    .environment_remove
                    .iter()
                    .any(|key| key == "GIT_ATTR_SOURCE")
            );
            let ssh = env("GIT_SSH_COMMAND");
            for flag in [
                "BatchMode=yes",
                "ForwardAgent=no",
                "ClearAllForwardings=yes",
                "ConnectTimeout=5",
            ] {
                assert!(ssh.contains(flag));
            }
            for (key, value) in HARDENING {
                assert!(
                    request
                        .args
                        .iter()
                        .any(|arg| arg == format!("{key}={value}").as_str())
                );
            }
            assert_eq!(request.policy.deadline, GIT_DEADLINE);
            assert_eq!(request.policy.stdout_limit, GIT_OUTPUT_BYTES);
            let calls = runner.calls.lock().unwrap();
            let helper_reads: Vec<_> = calls
                .iter()
                .filter(|request| request.args.iter().any(|arg| arg == "--global"))
                .collect();
            if origin.starts_with("https:") {
                assert_eq!(helper_reads.len(), 3);
                assert!(
                    helper_reads
                        .iter()
                        .all(|request| request.policy.deadline == HELPER_LOOKUP_DEADLINE)
                );
                for value in [
                    "credential.helper=fixture-helper",
                    "credential.helper=",
                    "credential.https://github.com.helper=fixture-url-helper",
                    "credential.useHttpPath=true",
                ] {
                    assert!(request.args.iter().any(|arg| arg == value));
                }
            } else {
                assert!(credentials.is_empty());
                assert!(helper_reads.is_empty());
            }
        }
    }
}
#[cfg(any(test, feature = "test-support"))]
pub mod testing {
    use super::*;
    use crate::{
        integration::{host::HostIntegrationService, host_store::HostIntegrationStore, testing::*},
        process::SystemProcessRunner,
        task::{BaseOid, BranchName, ClosePolicy, TaskMeta, TaskMetaInput, TaskSource, TaskStatus},
    };
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        path::{Path, PathBuf},
        process::Command,
    };

    pub struct GitIntegrationFixture {
        root: PathBuf,
        pub store: HostStore,
        pub runtime: ManualIntegrationRuntime,
        pub record: IntegrationRecord,
        origin: PathBuf,
        workspace: PathBuf,
    }
    impl Default for GitIntegrationFixture {
        fn default() -> Self {
            Self::new()
        }
    }
    impl GitIntegrationFixture {
        pub fn new() -> Self {
            let root = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("target/integration-git-fixtures")
                .join(uuid::Uuid::new_v4().to_string());
            Self::at(root)
        }
        pub fn at(root: PathBuf) -> Self {
            Self::at_for_task(root, fixture_task(), None)
        }
        pub fn at_for_task(
            root: PathBuf,
            task_id: crate::task::TaskId,
            existing_origin: Option<&Path>,
        ) -> Self {
            fs::create_dir_all(&root).unwrap();
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            let store = HostStore::open(&root.join("host")).unwrap();
            let origin = existing_origin
                .map(Path::to_path_buf)
                .unwrap_or_else(|| root.join("origin.git"));
            if existing_origin.is_none() {
                fs::create_dir(&origin).unwrap();
                git(&origin, &["init", "--bare", "."]);
                git(&origin, &["config", "receive.denyNonFastForwards", "true"]);
            }
            let mut record = sample_record(task_id, fixture_source(), "main");
            record.policy.origin = format!("file://{}", origin.display());
            use sha2::{Digest, Sha256};
            let mut hash = Sha256::new();
            hash.update(b"origin\0");
            hash.update(record.policy.origin.as_bytes());
            record.policy.project_id = format!("{:x}", hash.finalize());
            let task = store
                .open_task_directory(&record.policy.project_id, record.task_id, true)
                .unwrap();
            let workspace = task
                .open_child_directory(
                    &crate::inputs::RelativePath::parse(b"workspace").unwrap(),
                    true,
                )
                .unwrap()
                .path()
                .to_path_buf();
            git(&workspace, &["init", "."]);
            git(
                &workspace,
                &[
                    "checkout",
                    "-b",
                    BranchName::for_task(record.task_id).as_str(),
                ],
            );
            Self {
                root,
                store,
                runtime: ManualIntegrationRuntime::default(),
                record,
                origin,
                workspace,
            }
        }
        pub fn commit_base(&mut self) -> BaseOid {
            self.write("base.txt", b"base\n");
            let base = self.commit("base");
            git(
                &self.workspace,
                &[
                    "push",
                    self.record.policy.origin.as_str(),
                    "HEAD:refs/heads/main",
                ],
            );
            self.record.policy.base_oid = Some(base.clone());
            self.record.cycle_base = base.clone();
            base
        }
        pub fn commit_task(&mut self) -> BaseOid {
            self.write("task.txt", b"task\n");
            let head = self.commit("task");
            self.freeze_source(&head);
            head
        }
        pub fn write(&self, path: &str, bytes: &[u8]) {
            let path = self.workspace.join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, bytes).unwrap();
        }
        pub fn commit(&self, message: &str) -> BaseOid {
            git(&self.workspace, &["add", "-A"]);
            git(&self.workspace, &["commit", "-m", message]);
            git(&self.workspace, &["rev-parse", "HEAD"])
                .parse()
                .unwrap()
        }
        pub fn freeze_source(&mut self, head: &BaseOid) {
            let ordinary = sample_ordinary(self.record.task_id, fixture_source());
            let meta = TaskMeta::new(TaskMetaInput {
                session_import: None,
                task_id: self.record.task_id,
                run_id: None,
                project_id: self.record.policy.project_id.clone(),
                worktree_id: "b".repeat(64),
                agent: ordinary.meta().agent(),
                model: ordinary.meta().model().map(str::to_owned),
                effort: None,
                policy: ordinary.meta().policy(),
                source: TaskSource::Local {
                    wip: false,
                    push_target: None,
                },
                publish: ordinary.meta().publish().to_vec(),
                publish_branch: None,
                base_oid: self.record.cycle_base.clone(),
                limits: ordinary.meta().limits().clone(),
                close_policy: ClosePolicy::Never,
                env_profile: None,
                git_identity: self.record.git_identity.clone(),
                title: Some("Integration fixture".into()),
                prompt: "fixture".into(),
                created_at_millis: 1000,
            })
            .unwrap();
            let mut status = serde_json::to_value(ordinary.status()).unwrap();
            status["head_oid"] = serde_json::json!(head);
            let status: TaskStatus = serde_json::from_value(status).unwrap();
            let task = self
                .store
                .open_task_directory(&self.record.policy.project_id, self.record.task_id, false)
                .unwrap();
            task.write_private_atomic_no_replace("meta.json", &serde_json::to_vec(&meta).unwrap())
                .unwrap();
            task.write_private_atomic_no_replace(
                "status.json",
                &serde_json::to_vec(&status).unwrap(),
            )
            .unwrap();
            let mirror = self.store.mirror(&self.record.policy.project_id).unwrap();
            git(
                mirror.path(),
                &[
                    "fetch",
                    self.workspace.to_str().unwrap(),
                    &format!("HEAD:refs/heads/task/{}", self.record.task_id),
                ],
            );
            self.record.snapshot.source_head = head.clone();
            self.record.target_key = self.record.policy.target_key().unwrap();
            self.record.snapshot.integration_id = IntegrationId::derive(
                self.record.task_id,
                fixture_source(),
                head,
                &self.record.target_key,
            )
            .unwrap();
        }
        pub fn execute(
            &self,
            step: IntegrationStep,
        ) -> Result<HostIntegrationResponse, WorkerError> {
            self.execute_with(step, &SystemProcessRunner)
        }
        pub fn execute_with(
            &self,
            step: IntegrationStep,
            runner: &dyn ProcessRunner,
        ) -> Result<HostIntegrationResponse, WorkerError> {
            let request = HostIntegrationRequest {
                protocol_version: 7,
                task_id: self.record.task_id,
                integration_id: Some(self.record.snapshot.integration_id),
                epoch: self.record.snapshot.epoch,
                revision: self.record.snapshot.revision,
                action: HostIntegrationAction::Step {
                    step,
                    record: Box::new(self.record.clone()),
                },
            };
            let response = HostIntegrationService::new(&self.store, runner, &self.runtime)
                .execute(&request)?;
            response.validate_for(&request)?;
            Ok(response)
        }
        pub fn prepare(&mut self) -> BaseOid {
            let arm = HostIntegrationRequest {
                protocol_version: 7,
                task_id: self.record.task_id,
                integration_id: None,
                epoch: 0,
                revision: IntegrationRevision(0),
                action: HostIntegrationAction::Arm {
                    policy: self.record.policy.clone(),
                },
            };
            HostIntegrationService::new(&self.store, &SystemProcessRunner, &self.runtime)
                .execute(&arm)
                .expect("arm host policy");
            self.execute(IntegrationStep::Prepare).unwrap_or_else(|e| {
                panic!(
                    "prepare real merge: {e:?}; hooks: {:?}",
                    self.runtime.hooks()
                )
            });
            let record = HostIntegrationStore::new(&self.store)
                .load(&self.record.policy.project_id, self.record.task_id)
                .unwrap()
                .unwrap();
            self.record = record;
            self.record
                .candidates
                .last()
                .unwrap()
                .merge_oid
                .clone()
                .expect("pinned merge")
        }
        pub fn push(&self) {
            assert!(matches!(
                self.execute(IntegrationStep::Push).unwrap(),
                HostIntegrationResponse::Integrated { .. }
            ));
        }
        pub fn parents(&self, oid: &BaseOid) -> Vec<BaseOid> {
            let mirror = self
                .store
                .mirror_if_present(&self.record.policy.project_id)
                .unwrap()
                .unwrap();
            git(mirror.path(), &["show", "-s", "--format=%P", oid.as_str()])
                .split_whitespace()
                .map(|s| s.parse().unwrap())
                .collect()
        }
        pub fn origin_tip(&self) -> BaseOid {
            git(&self.origin, &["rev-parse", "refs/heads/main"])
                .parse()
                .unwrap()
        }
        pub fn workspace(&self) -> &Path {
            &self.workspace
        }
        pub fn advance_target(&self) -> BaseOid {
            self.advance_target_with("target.txt", b"target\n")
        }
        pub fn advance_target_with(&self, path: &str, bytes: &[u8]) -> BaseOid {
            let peer = self.root.join(uuid::Uuid::new_v4().to_string());
            git(
                &self.root,
                &[
                    "clone",
                    "-b",
                    "main",
                    self.origin.to_str().unwrap(),
                    peer.to_str().unwrap(),
                ],
            );
            let file = peer.join(path);
            fs::create_dir_all(file.parent().unwrap()).unwrap();
            fs::write(file, bytes).unwrap();
            git(&peer, &["add", "-A"]);
            git(&peer, &["commit", "-m", "advance target"]);
            let head = git(&peer, &["rev-parse", "HEAD"]).parse().unwrap();
            git(&peer, &["push", "origin", "HEAD:refs/heads/main"]);
            head
        }
        pub fn prepared(&self, purpose: IntegrationTurnPurpose) -> PreparedIntegrationTurn {
            use crate::{prepared_followup::PreparedFollowup, task::LocalTaskRecord};
            let record = HostIntegrationStore::new(&self.store)
                .load(&self.record.policy.project_id, self.record.task_id)
                .unwrap()
                .unwrap();
            let candidate = record.candidates.last().unwrap();
            let task_store = crate::task_store::TaskStore::new(&self.store, &SystemProcessRunner);
            let ordinary = LocalTaskRecord::new(
                task_store
                    .load_meta(&record.policy.project_id, record.task_id)
                    .unwrap(),
                task_store
                    .load_status(&record.policy.project_id, record.task_id)
                    .unwrap(),
                Some(1001),
                None,
                Some(record.snapshot.source_head.clone()),
                "c".repeat(64),
                Some("fixture-worker".into()),
                true,
                None,
            )
            .unwrap();
            let turn = auxiliary_turn_id(
                record.snapshot.integration_id,
                record.snapshot.epoch,
                candidate.id.attempt,
                purpose,
                1,
            )
            .unwrap();
            let prepared = PreparedIntegrationTurn {
                integration_id: record.snapshot.integration_id,
                epoch: record.snapshot.epoch,
                attempt: candidate.id.attempt,
                purpose,
                ordinal: 1,
                followup: PreparedFollowup::prepare(
                    &ordinary,
                    "Repair or verify the candidate".into(),
                    turn,
                    1002,
                )
                .unwrap(),
                workspace_binding: IntegrationWorkspaceBinding {
                    task_id: record.task_id,
                    candidate: candidate.id,
                    branch: candidate.clean_h.branch.clone(),
                    head: candidate.source_head.clone(),
                    merge_head: candidate.target_head.clone(),
                    attribute_source: candidate.attribute_source.clone(),
                    ours: candidate.ours.clone(),
                    theirs: candidate.theirs.clone(),
                    pinned_tree: candidate.tree_oid.clone(),
                    clean_h: candidate.clean_h.clone(),
                },
                approved_turn_limits: crate::agent::TurnLimits::new(600_000, None, None).unwrap(),
            };
            prepared.validate_for(&record).unwrap();
            prepared
        }
        pub fn complete_auxiliary(
            &mut self,
            purpose: IntegrationTurnPurpose,
        ) -> PreparedIntegrationTurn {
            let prepared = self.prepared(purpose);
            self.record = HostIntegrationStore::new(&self.store)
                .load(&self.record.policy.project_id, self.record.task_id)
                .unwrap()
                .unwrap();
            let mut intent = prepared.intent().unwrap();
            intent.queue_position = Some(0);
            intent.accepted = true;
            intent.completed = true;
            self.record.auxiliaries.push(intent);
            let sidecars = HostIntegrationStore::new(&self.store);
            sidecars.save(&self.record).unwrap();
            sidecars
                .write(
                    &self.record.policy.project_id,
                    self.record.task_id,
                    &format!("turn-{}.json", prepared.followup.turn_id()),
                    &encode_prepared_turn(&prepared).unwrap(),
                )
                .unwrap();
            let task = self
                .store
                .open_task_directory(&self.record.policy.project_id, self.record.task_id, false)
                .unwrap();
            let current = task
                .read_private_regular("status.json", 1024 * 1024)
                .unwrap();
            let mut wire: serde_json::Value = serde_json::from_slice(&current).unwrap();
            wire["turns"].as_array_mut().unwrap().push(
                serde_json::to_value(crate::task::TurnSummary::new(
                    prepared.followup.turn_number(),
                    prepared.followup.turn_id(),
                    Some(crate::task::TurnTerminal::Succeeded),
                    Some(crate::task::TaskOutcome::Done),
                    Some(false),
                    false,
                    Some(1002),
                    Some(1003),
                ))
                .unwrap(),
            );
            let status: crate::task::TaskStatus = serde_json::from_value(wire).unwrap();
            task.rewrite_private_regular_exact(
                "status.json",
                &current,
                &serde_json::to_vec(&status).unwrap(),
            )
            .unwrap();
            prepared
        }
        pub fn origin(&self) -> &Path {
            &self.origin
        }
        pub fn install_policy_rejection(&self) {
            let hook = self.origin.join("hooks/pre-receive");
            fs::write(&hook, b"#!/bin/sh\nexit 1\n").unwrap();
            fs::set_permissions(hook, fs::Permissions::from_mode(0o700)).unwrap();
        }
        pub fn git(&self, args: &[&str]) -> String {
            git(&self.workspace, args)
        }
    }
    impl Drop for GitIntegrationFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }
    pub fn git(path: &Path, args: &[&str]) -> String {
        let output = Command::new("/usr/bin/git")
            .arg("-C")
            .arg(path)
            .args([
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "commit.gpgSign=false",
            ])
            .args(args)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_AUTHOR_NAME", "mac-worker")
            .env("GIT_AUTHOR_EMAIL", "mac-worker@localhost")
            .env("GIT_COMMITTER_NAME", "mac-worker")
            .env("GIT_COMMITTER_EMAIL", "mac-worker@localhost")
            .env("GIT_AUTHOR_DATE", "1700000000 +0000")
            .env("GIT_COMMITTER_DATE", "1700000000 +0000")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "fixture git failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }
}
