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
use std::{collections::BTreeSet, ffi::OsString};

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
];
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
pub struct IntegrationGit<'a> {
    store: &'a HostStore,
    runner: &'a dyn ProcessRunner,
    runtime: &'a dyn IntegrationRuntime,
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
            runtime,
        }
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
        match self.push(&record, candidate)? {
            PushOutcome::Integrated(receipt) => Ok(receipt),
            PushOutcome::Moved(_) => Err(IntegrationCode::IntegrationTargetMovedExhausted.error()),
        }
    }
    pub(crate) fn mirror(
        &self,
        policy: &FrozenIntegrationPolicy,
    ) -> Result<RootedDir, WorkerError> {
        self.store
            .mirror_if_present(&policy.project_id)?
            .ok_or_else(invalid)
    }
    fn request(
        &self,
        repo: &RootedDir,
        attrs: Option<&BaseOid>,
        operation: &[OsString],
        credentials: &[(String, String)],
    ) -> Result<ProcessRequest, WorkerError> {
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
                "^(filter\\..*\\.(clean|smudge|process|required)|merge\\..*\\.(driver|recursive))$"
                    .into(),
            ],
        );
        probe.policy.deadline = GIT_DEADLINE;
        probe
            .environment
            .push(("GIT_ATTR_NOSYSTEM".into(), "1".into()));
        let names = self
            .runner
            .run_interruptible(&probe, &|| false)
            .map_err(|_| invalid())?;
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
        for driver in drivers {
            if driver.starts_with("filter.") {
                for field in ["clean", "smudge", "process"] {
                    config.push((format!("{driver}.{field}"), String::new()));
                }
                config.push((format!("{driver}.required"), "false".into()));
            } else {
                config.push((
                    format!("{driver}.driver"),
                    "/usr/bin/git merge-file %A %O %B".into(),
                ));
                config.push((format!("{driver}.recursive"), "text".into()));
            }
        }
        let mut args = Vec::new();
        if let Some(attrs) = attrs {
            args.push(format!("--attr-source={attrs}").into());
        }
        args.extend_from_slice(operation);
        let mut request =
            git_request_with_config(repo.path(), Some(origin_git_ssh_command()?), &config, args);
        request.policy.deadline = GIT_DEADLINE;
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
            let bytes = info
                .read_snapshot_regular(
                    "attributes",
                    GIT_OUTPUT_BYTES as u64,
                    crate::rooted_fs::SnapshotProjection::Workspace,
                )
                .map_err(|_| invalid())?
                .bytes;
            if !bytes.is_empty() {
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
            .run_interruptible(&request, &|| false)
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
        let credentials =
            GitTransport::new(self.runner).origin_credential_config(&record.policy.origin);
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
            .run_interruptible(&request, &|| false)
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
            .run_interruptible(&request, &|| false)
            .map_err(|_| IntegrationCode::IntegrationNetwork.error())?;
        if !result.status.success() {
            return Err(IntegrationCode::IntegrationNetwork.error());
        }
        self.runtime.reach(IntegrationHook::AfterFetchBeforePin);
        self.query(mirror, None, &["update-ref", &pin, oid.as_str()])?;
        mirror.sync_root()?;
        self.runtime.reach(IntegrationHook::AfterTargetPin);
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
        let mut worktree = Sha256::new();
        worktree.update(diff.stdout);
        for path in self.untracked(workspace, &candidate.attribute_source)? {
            validate_relative_path(&path)?;
            worktree.update((path.len() as u64).to_be_bytes());
            worktree.update(path.as_bytes());
            let parent = std::path::Path::new(&path)
                .parent()
                .filter(|p| !p.as_os_str().is_empty());
            let dir = match parent {
                Some(parent) => RootedDir::open(&workspace.path().join(parent))?,
                None => workspace.reopen()?,
            };
            let name = std::path::Path::new(&path)
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(invalid)?;
            let bytes = dir.read_snapshot_regular(
                name,
                GIT_OUTPUT_BYTES as u64,
                crate::rooted_fs::SnapshotProjection::Workspace,
            )?;
            worktree.update(bytes.mode.to_be_bytes());
            worktree.update(bytes.bytes);
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
        self.runtime.reach(IntegrationHook::DuringWorkspacePrepare);
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
        let tree = candidate.tree_oid.as_ref().ok_or_else(invalid)?;
        let mut request = self.request(
            &mirror,
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
            .run_interruptible(&request, &|| false)
            .map_err(|_| invalid())?;
        if !result.status.success() {
            return Err(invalid());
        }
        let oid: BaseOid = std::str::from_utf8(&result.stdout)
            .map_err(|_| invalid())?
            .trim()
            .parse()
            .map_err(|_| invalid())?;
        self.runtime.reach(IntegrationHook::AfterCommitBeforePin);
        let pin = candidate_pin(candidate);
        self.query(&mirror, None, &["update-ref", &pin, oid.as_str()])?;
        mirror.sync_root()?;
        candidate.merge_oid = Some(oid);
        self.runtime.reach(IntegrationHook::AfterMergePin);
        Ok(())
    }
    pub(crate) fn push(
        &self,
        record: &IntegrationRecord,
        candidate: &IntegrationCandidate,
    ) -> Result<PushOutcome, WorkerError> {
        candidate.validate()?;
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
        let credentials =
            GitTransport::new(self.runner).origin_credential_config(&record.policy.origin);
        // Every replay observes first, including a durable intent whose original reply was lost.
        if let Some(outcome) = self.observed_outcome(record, candidate, &mirror, &credentials)? {
            return Ok(outcome);
        }
        if !self.is_ancestor(&mirror, &record.cycle_base, &candidate.target_head)? {
            return Err(IntegrationCode::IntegrationBaseNotOnTarget.error());
        }
        self.runtime.reach(IntegrationHook::BeforePush);
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
        let result = self.runner.run_interruptible(&request, &|| false);
        if let Ok(result) = &result
            && result.status.success()
        {
            self.runtime.reach(IntegrationHook::AfterPushBeforeReceipt);
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
            recorded_at_millis: self.runtime.now_millis(),
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
            fs::create_dir_all(&root).unwrap();
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            let store = HostStore::open(&root.join("host")).unwrap();
            let origin = root.join("origin.git");
            fs::create_dir(&origin).unwrap();
            git(&origin, &["init", "--bare", "."]);
            git(&origin, &["config", "receive.denyNonFastForwards", "true"]);
            let mut record = sample_record(fixture_task(), fixture_source(), "main");
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
            let response =
                HostIntegrationService::new(&self.store, &SystemProcessRunner, &self.runtime)
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
        pub fn origin(&self) -> &Path {
            &self.origin
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
