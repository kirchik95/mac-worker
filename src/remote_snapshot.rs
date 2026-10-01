use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, io,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{
    error::WorkerError,
    host_store::{
        AdmissionGuard, HostStore, HostStoreWritePoint, JobDisposition, StagedJob, WorkspaceReceipt,
    },
    inputs::RelativePath,
    job::{LeaseRecord, RequestFingerprint},
    lease::LeaseService,
    manifest::{ManifestEntry, ManifestEntryKind, SnapshotManifest},
    rooted_fs::{RootedDir, SnapshotFsKind, SnapshotProjection, SnapshotTreeInspection},
};

pub use crate::legacy_snapshot_receipt::{
    LegacySnapshotReceiptService as RemoteSnapshotService, SnapshotCacheKey, VerifiedReceipt,
};
use crate::legacy_snapshot_receipt::{
    decode_canonical_json, lease_identity_mismatch, lease_token_hash, protocol_code,
    unsafe_remote_snapshot, validate_digest, validate_receipt_identity,
};

const SNAPSHOT_MANIFEST_VERSION: u32 = 1;
const MAX_MANIFEST_BYTES: u64 = 8 * 1024 * 1024;
const MAX_TEXT_FIELD_BYTES: usize = 128 * 1024;
const MAX_SYMLINK_TARGET_BYTES: usize = 64 * 1024;
const MANIFEST_FILE: &str = "manifest.json";
const TREE_DIRECTORY: &str = "tree";

pub struct VerifiedRemoteSnapshot {
    project_id: String,
    worktree_id: String,
    digest: String,
    manifest: SnapshotManifest,
    cache_root: RootedDir,
    lease: LeaseRecord,
    verified_at_millis: u64,
    cache_reused: bool,
}

impl fmt::Debug for VerifiedRemoteSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedRemoteSnapshot")
            .field("project_id", &self.project_id)
            .field("worktree_id", &self.worktree_id)
            .field("digest", &self.digest)
            .field("entry_count", &self.manifest.entries.len())
            .field(
                "tracked_deletion_count",
                &self.manifest.tracked_deletions.len(),
            )
            .field("verified_at_millis", &self.verified_at_millis)
            .field("cache_reused", &self.cache_reused)
            .finish_non_exhaustive()
    }
}

impl VerifiedRemoteSnapshot {
    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn worktree_id(&self) -> &str {
        &self.worktree_id
    }

    pub fn digest(&self) -> &str {
        &self.digest
    }

    pub fn manifest(&self) -> &SnapshotManifest {
        &self.manifest
    }

    pub fn verified_at_millis(&self) -> u64 {
        self.verified_at_millis
    }

    pub fn cache_reused(&self) -> bool {
        self.cache_reused
    }
}

impl<'a> RemoteSnapshotService<'a> {
    pub fn verify_and_promote(
        &self,
        lease: &LeaseRecord,
        expected_digest: &str,
    ) -> Result<VerifiedRemoteSnapshot, WorkerError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| WorkerError::Protocol("system clock precedes the Unix epoch".into()))?
            .as_millis()
            .try_into()
            .map_err(|_| WorkerError::Protocol("system clock timestamp overflowed".into()))?;
        self.verify_and_promote_at(lease, expected_digest, now)
    }

    #[doc(hidden)]
    pub fn verify_and_promote_at(
        &self,
        lease: &LeaseRecord,
        expected_digest: &str,
        now_millis: u64,
    ) -> Result<VerifiedRemoteSnapshot, WorkerError> {
        self.verify_and_promote_at_with_lock_hook(lease, expected_digest, now_millis, || {})
    }

    fn verify_and_promote_at_with_lock_hook(
        &self,
        lease: &LeaseRecord,
        expected_digest: &str,
        now_millis: u64,
        after_locks: impl FnOnce(),
    ) -> Result<VerifiedRemoteSnapshot, WorkerError> {
        lease.validate()?;
        validate_digest(expected_digest, "manifest digest")?;
        if lease.manifest_digest() != expected_digest {
            return Err(lease_identity_mismatch());
        }
        self.store.validate_layout()?;
        let admission = self.store.admission_lock(lease.job_id())?;
        let live = require_exact_live(self.store, &admission, lease)?;
        require_verifiable_disposition(self.store, &live)?;
        let transfer = self.store.transfer_lock_after(&admission, lease.job_id())?;
        admission.validate_for(lease.job_id())?;
        transfer.validate()?;
        let live = require_exact_live(self.store, &admission, lease)?;
        if live.manifest_digest() != expected_digest {
            return Err(lease_identity_mismatch());
        }
        require_verifiable_disposition(self.store, &live)?;
        after_locks();

        if let Some(receipt) = self.read_receipt_optional(live.job_id())? {
            validate_receipt_identity(&receipt, &live, live.request_fingerprint())?;
            let snapshot =
                self.open_and_validate_cache(&live, receipt.verified_at_millis(), true)?;
            admission.validate_for(lease.job_id())?;
            transfer.validate()?;
            return Ok(snapshot);
        }

        let incoming_path = format!("incoming/{}/{}", live.job_id(), live.lease_token());
        let incoming = match self.store.open_directory(&incoming_path, false) {
            Ok(incoming) => {
                validate_bundle(&incoming, &live, expected_digest, BundlePolicy::Incoming)?;
                admission.validate_for(lease.job_id())?;
                transfer.validate()?;
                self.fail_at(HostStoreWritePoint::AfterSnapshotValidation)?;
                incoming
                    .prepare_snapshot_for_publication_with_hook(|| {
                        self.io_fail_at(HostStoreWritePoint::DuringSnapshotConversion)
                    })
                    .map_err(unsafe_snapshot_io)?;
                self.fail_at(HostStoreWritePoint::AfterSnapshotConversion)?;
                validate_bundle(&incoming, &live, expected_digest, BundlePolicy::Prepared)?;
                Some(incoming)
            }
            Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => None,
            Err(_) => return Err(unsafe_remote_snapshot()),
        };

        let (cache_root, cache_reused) = match incoming {
            Some(incoming) => {
                admission.validate_for(lease.job_id())?;
                transfer.validate()?;
                self.publish_validated_cache_candidate(incoming, &live, expected_digest, || {
                    admission.validate_for(lease.job_id())?;
                    transfer.validate()
                })?
            }
            None => {
                let cache = self.recover_cache(&live, expected_digest)?;
                (cache, true)
            }
        };

        admission.validate_for(lease.job_id())?;
        transfer.validate()?;
        let desired = receipt_for_lease(&live, now_millis)?;
        let receipt = self.publish_receipt(&desired)?;
        validate_receipt_identity(&receipt, &live, live.request_fingerprint())?;
        validate_bundle(&cache_root, &live, expected_digest, BundlePolicy::Cache)?;
        admission.validate_for(lease.job_id())?;
        transfer.validate()?;
        snapshot_from_parts(
            cache_root,
            &live,
            receipt.verified_at_millis(),
            cache_reused,
        )
    }

    fn publish_validated_cache_candidate(
        &self,
        mut incoming: RootedDir,
        lease: &LeaseRecord,
        expected_digest: &str,
        before_publish: impl FnOnce() -> Result<(), WorkerError>,
    ) -> Result<(RootedDir, bool), WorkerError> {
        validate_bundle(&incoming, lease, expected_digest, BundlePolicy::Prepared)?;
        let cache_parent = self.store.open_directory(
            &format!("snapshots/{}/{}", lease.project_id(), lease.worktree_id()),
            true,
        )?;
        before_publish()?;
        match incoming.publish_owned_into_with_post_rename(&cache_parent, expected_digest, || {
            self.io_fail_at(HostStoreWritePoint::AfterSnapshotRename)
        }) {
            Ok(()) => {
                self.fail_at(HostStoreWritePoint::AfterSnapshotCacheSync)?;
                incoming.seal_snapshot_root().map_err(unsafe_snapshot_io)?;
                self.fail_at(HostStoreWritePoint::AfterSnapshotRootSeal)?;
                validate_bundle(&incoming, lease, expected_digest, BundlePolicy::Cache)?;
                Ok((incoming, false))
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let winner = self.recover_cache(lease, expected_digest)?;
                Ok((winner, true))
            }
            Err(_) => Err(unsafe_remote_snapshot()),
        }
    }

    pub fn load_verified(
        &self,
        lease: &LeaseRecord,
        request_fingerprint: &RequestFingerprint,
    ) -> Result<VerifiedRemoteSnapshot, WorkerError> {
        lease.validate()?;
        self.store.validate_layout()?;
        let admission = self.store.admission_lock(lease.job_id())?;
        self.load_verified_after(&admission, lease, request_fingerprint)
    }

    pub(crate) fn load_verified_after(
        &self,
        admission: &AdmissionGuard,
        lease: &LeaseRecord,
        request_fingerprint: &RequestFingerprint,
    ) -> Result<VerifiedRemoteSnapshot, WorkerError> {
        admission.validate_for(lease.job_id())?;
        let live = require_exact_live(self.store, admission, lease)?;
        if live.request_fingerprint() != request_fingerprint {
            return Err(lease_identity_mismatch());
        }
        require_verifiable_disposition(self.store, &live)?;
        let receipt = self
            .read_receipt_optional(live.job_id())?
            .ok_or_else(manifest_mismatch)?;
        validate_receipt_identity(&receipt, &live, request_fingerprint)?;
        let snapshot = self.open_and_validate_cache(&live, receipt.verified_at_millis(), true)?;
        admission.validate_for(lease.job_id())?;
        Ok(snapshot)
    }

    pub(crate) fn load_verified_for_accepted_after(
        &self,
        admission: &AdmissionGuard,
        lease: &LeaseRecord,
        request_fingerprint: &RequestFingerprint,
    ) -> Result<VerifiedRemoteSnapshot, WorkerError> {
        admission.validate_for(lease.job_id())?;
        let live = require_exact_live(self.store, admission, lease)?;
        if live.request_fingerprint() != request_fingerprint {
            return Err(lease_identity_mismatch());
        }
        require_matching_accepted_disposition(self.store, &live)?;
        let receipt = self
            .read_receipt_optional(live.job_id())?
            .ok_or_else(manifest_mismatch)?;
        validate_receipt_identity(&receipt, &live, request_fingerprint)?;
        let snapshot = self.open_and_validate_cache(&live, receipt.verified_at_millis(), true)?;
        admission.validate_for(lease.job_id())?;
        Ok(snapshot)
    }

    pub fn materialize_workspace(
        &self,
        snapshot: &VerifiedRemoteSnapshot,
        staged_job: &mut StagedJob,
    ) -> Result<WorkspaceReceipt, WorkerError> {
        self.store.validate_layout()?;
        let canonical = self
            .open_cache(&snapshot.lease, snapshot.digest())?
            .ok_or_else(manifest_mismatch)?;
        if canonical.identity()? != snapshot.cache_root.identity()? {
            return Err(unsafe_remote_snapshot());
        }
        validate_bundle(
            &snapshot.cache_root,
            &snapshot.lease,
            snapshot.digest(),
            BundlePolicy::Cache,
        )?;
        let source_tree = snapshot
            .cache_root
            .open_child_directory(&relative(TREE_DIRECTORY)?, false)
            .map_err(unsafe_snapshot_io)?;
        let workspace_tree = staged_job.create_workspace_tree()?;
        let declared = declared_tree_paths(&snapshot.manifest)?;

        for path in declared.iter().filter(|path| {
            snapshot
                .manifest
                .entries
                .iter()
                .find(|entry| entry.path == path.as_str())
                .is_none_or(|entry| entry.kind == ManifestEntryKind::Directory)
        }) {
            workspace_tree
                .create_empty_directory(path)
                .map_err(unsafe_snapshot_io)?;
        }
        for entry in &snapshot.manifest.entries {
            let path = relative(&entry.path)?;
            match entry.kind {
                ManifestEntryKind::File => source_tree
                    .copy_snapshot_regular_to_writable(&path, &workspace_tree, entry.mode == 0o755)
                    .map_err(unsafe_snapshot_io)?,
                ManifestEntryKind::Symlink => workspace_tree
                    .create_symlink(
                        &path,
                        entry
                            .symlink_target
                            .as_deref()
                            .ok_or_else(manifest_mismatch)?,
                    )
                    .map_err(unsafe_snapshot_io)?,
                ManifestEntryKind::Directory => {}
            }
        }
        validate_bundle(
            &snapshot.cache_root,
            &snapshot.lease,
            snapshot.digest(),
            BundlePolicy::Cache,
        )?;
        staged_job.complete_snapshot_materialization(
            &declared,
            snapshot.project_id(),
            snapshot.worktree_id(),
            snapshot.digest(),
        )
    }

    pub(crate) fn validate_materialized_workspace(
        &self,
        snapshot: &VerifiedRemoteSnapshot,
        workspace: &RootedDir,
    ) -> Result<(), WorkerError> {
        let canonical = self
            .open_cache(&snapshot.lease, snapshot.digest())?
            .ok_or_else(manifest_mismatch)?;
        if canonical.identity()? != snapshot.cache_root.identity()? {
            return Err(unsafe_remote_snapshot());
        }
        validate_bundle(
            &snapshot.cache_root,
            &snapshot.lease,
            snapshot.digest(),
            BundlePolicy::Cache,
        )?;
        let tree = workspace
            .inspect_snapshot_tree(TREE_DIRECTORY, SnapshotProjection::Workspace)
            .map_err(unsafe_snapshot_io)?;
        validate_exact_tree(&snapshot.manifest, &tree, BundlePolicy::Workspace)?;
        validate_bundle(
            &snapshot.cache_root,
            &snapshot.lease,
            snapshot.digest(),
            BundlePolicy::Cache,
        )
        .map(|_| ())
    }

    fn open_and_validate_cache(
        &self,
        lease: &LeaseRecord,
        verified_at_millis: u64,
        cache_reused: bool,
    ) -> Result<VerifiedRemoteSnapshot, WorkerError> {
        let cache = self
            .open_cache(lease, lease.manifest_digest())?
            .ok_or_else(manifest_mismatch)?;
        validate_bundle(&cache, lease, lease.manifest_digest(), BundlePolicy::Cache)?;
        snapshot_from_parts(cache, lease, verified_at_millis, cache_reused)
    }

    fn open_cache(
        &self,
        lease: &LeaseRecord,
        digest: &str,
    ) -> Result<Option<RootedDir>, WorkerError> {
        match self.store.open_directory(
            &format!(
                "snapshots/{}/{}/{}",
                lease.project_id(),
                lease.worktree_id(),
                digest
            ),
            false,
        ) {
            Ok(cache) => Ok(Some(cache)),
            Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(_) => Err(unsafe_remote_snapshot()),
        }
    }

    fn recover_cache(&self, lease: &LeaseRecord, digest: &str) -> Result<RootedDir, WorkerError> {
        // A sibling publisher may still be sealing the just-renamed digest
        // (0o700 → 0o500). That changes mode and ctime of the same inode, so
        // Prepared validation observes unstable metadata and would report
        // UNSAFE_REMOTE_SNAPSHOT. Re-open until the sealed cache is stable,
        // or seal crash residue once no sibling is still mutating it.
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(100))
            .ok_or_else(unsafe_remote_snapshot)?;
        loop {
            let cache = self
                .open_cache(lease, digest)?
                .ok_or_else(manifest_mismatch)?;
            match snapshot_root_mode(&cache)? {
                0o500 => {
                    validate_bundle(&cache, lease, digest, BundlePolicy::Cache)?;
                    return Ok(cache);
                }
                0o700 => match seal_recovered_prepared_cache(&cache, lease, digest) {
                    Ok(()) => return Ok(cache),
                    Err(error) => {
                        if snapshot_root_mode(&cache).ok() == Some(0o500) {
                            validate_bundle(&cache, lease, digest, BundlePolicy::Cache)?;
                            return Ok(cache);
                        }
                        if Instant::now() < deadline {
                            std::thread::sleep(Duration::from_millis(1));
                            continue;
                        }
                        return Err(error);
                    }
                },
                _ => return Err(unsafe_remote_snapshot()),
            }
        }
    }

    fn publish_receipt(&self, desired: &VerifiedReceipt) -> Result<VerifiedReceipt, WorkerError> {
        let directory = self.store.open_directory("verified", false)?;
        let name = format!("{}.json", desired.job_id());
        if directory.entry_exists(&name)? {
            let existing = self
                .read_receipt_optional(desired.job_id())?
                .ok_or_else(unsafe_remote_snapshot)?;
            if receipt_identity_equal(&existing, desired) {
                return Ok(existing);
            }
            return Err(unsafe_remote_snapshot());
        }
        let bytes = serde_json::to_vec(desired)
            .map_err(|_| WorkerError::Protocol("verified receipt is invalid".into()))?;
        let staging_name = format!(".verify-{}.json.pending", desired.job_id());
        if directory.entry_exists(".mac-worker-rooted-fs")? {
            directory
                .resume_pending_owned_regular_cleanup(&staging_name)
                .map_err(map_verified_cleanup_io)?;
        }
        if directory.entry_exists(&staging_name)? {
            let staged_bytes = directory
                .read_private_regular(&staging_name, 1024 * 1024)
                .map_err(|_| unsafe_remote_snapshot())?;
            let staged: VerifiedReceipt = decode_canonical_json(&staged_bytes, "verified receipt")?;
            if !receipt_identity_equal(&staged, desired) {
                return Err(unsafe_remote_snapshot());
            }
            self.store
                .remove_owned_regular_committed(&directory, &staging_name)
                .map_err(map_verified_cleanup_io)?;
            directory.sync_root().map_err(WorkerError::Io)?;
        }
        match directory.write_private_atomic_no_replace_with_commit_hooks(
            &name,
            &staging_name,
            &bytes,
            || self.io_fail_at(HostStoreWritePoint::AfterSnapshotReceiptFileSync),
            || self.io_fail_at(HostStoreWritePoint::AfterSnapshotReceiptPublish),
            || self.io_fail_at(HostStoreWritePoint::AfterSnapshotReceiptParentSync),
        ) {
            Ok(()) => Ok(desired.clone()),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let existing = self
                    .read_receipt_optional(desired.job_id())?
                    .ok_or_else(unsafe_remote_snapshot)?;
                if receipt_identity_equal(&existing, desired) {
                    Ok(existing)
                } else {
                    Err(unsafe_remote_snapshot())
                }
            }
            Err(error) => Err(WorkerError::Io(error)),
        }
    }

    fn fail_at(&self, point: HostStoreWritePoint) -> Result<(), WorkerError> {
        if self.store.consume_fault(point) {
            return Err(unsafe_remote_snapshot());
        }
        Ok(())
    }

    fn io_fail_at(&self, point: HostStoreWritePoint) -> io::Result<()> {
        if self.store.consume_fault(point) {
            return Err(io::Error::other("injected snapshot commit interruption"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BundlePolicy {
    Incoming,
    Prepared,
    Cache,
    Workspace,
}

impl BundlePolicy {
    fn projection(self) -> SnapshotProjection {
        match self {
            Self::Incoming => SnapshotProjection::TransportOrOwner,
            Self::Prepared | Self::Cache => SnapshotProjection::OwnerOnly,
            Self::Workspace => SnapshotProjection::Workspace,
        }
    }
}

fn validate_bundle(
    root: &RootedDir,
    lease: &LeaseRecord,
    expected_digest: &str,
    policy: BundlePolicy,
) -> Result<SnapshotManifest, WorkerError> {
    validate_bundle_with_hook(root, lease, expected_digest, policy, || {})
}

fn validate_bundle_with_hook(
    root: &RootedDir,
    lease: &LeaseRecord,
    expected_digest: &str,
    policy: BundlePolicy,
    after_manifest_read: impl FnOnce(),
) -> Result<SnapshotManifest, WorkerError> {
    let root_metadata = root.root_metadata().map_err(unsafe_snapshot_io)?;
    let root_mode = (root_metadata.st_mode & 0o7777) as u32;
    let valid_root_mode = match policy {
        BundlePolicy::Incoming => matches!(root_mode, 0o700 | 0o500),
        BundlePolicy::Prepared | BundlePolicy::Workspace => root_mode == 0o700,
        BundlePolicy::Cache => root_mode == 0o500,
    };
    if !valid_root_mode {
        return Err(unsafe_remote_snapshot());
    }
    root.validate_snapshot_root(root_mode)
        .map_err(unsafe_snapshot_io)?;
    let actual_root_names = root.list_names().map_err(unsafe_snapshot_io)?;
    let expected_root_names = BTreeSet::from([
        MANIFEST_FILE.as_bytes().to_vec(),
        TREE_DIRECTORY.as_bytes().to_vec(),
    ]);
    if actual_root_names.into_iter().collect::<BTreeSet<_>>() != expected_root_names {
        return Err(unsafe_remote_snapshot());
    }

    let manifest_file = root
        .read_snapshot_regular(MANIFEST_FILE, MAX_MANIFEST_BYTES, policy.projection())
        .map_err(unsafe_snapshot_io)?;
    let expected_manifest_mode = match policy {
        BundlePolicy::Incoming => matches!(manifest_file.mode, 0o444 | 0o400),
        BundlePolicy::Prepared | BundlePolicy::Cache => manifest_file.mode == 0o400,
        BundlePolicy::Workspace => manifest_file.mode == 0o600,
    };
    if !expected_manifest_mode {
        return Err(unsafe_remote_snapshot());
    }
    let manifest = decode_manifest(&manifest_file.bytes)?;
    validate_manifest(&manifest)?;
    let actual_digest = format!("{:x}", Sha256::digest(&manifest_file.bytes));
    if actual_digest != expected_digest
        || expected_digest != lease.manifest_digest()
        || manifest.project_id != lease.project_id()
        || manifest.worktree_id != lease.worktree_id()
    {
        return Err(manifest_mismatch());
    }
    after_manifest_read();
    let tree = root
        .inspect_snapshot_tree(TREE_DIRECTORY, policy.projection())
        .map_err(unsafe_snapshot_io)?;
    if tree.root_device != manifest_file.device
        || tree.root_inode == manifest_file.inode
        || !physical_directory_mode_matches(tree.root_mode, policy)
    {
        return Err(unsafe_remote_snapshot());
    }
    validate_exact_tree(&manifest, &tree, policy)?;
    let final_root_metadata = root.root_metadata().map_err(unsafe_snapshot_io)?;
    if !snapshot_root_metadata_stable(&root_metadata, &final_root_metadata) {
        return Err(unsafe_remote_snapshot());
    }
    root.validate_snapshot_root(root_mode)
        .map_err(unsafe_snapshot_io)?;
    Ok(manifest)
}

fn snapshot_root_metadata_stable(left: &libc::stat, right: &libc::stat) -> bool {
    left.st_dev == right.st_dev
        && left.st_ino == right.st_ino
        && left.st_mode == right.st_mode
        && left.st_nlink == right.st_nlink
        && left.st_uid == right.st_uid
        && left.st_gid == right.st_gid
        && left.st_size == right.st_size
        && left.st_mtime == right.st_mtime
        && left.st_mtime_nsec == right.st_mtime_nsec
        && left.st_ctime == right.st_ctime
        && left.st_ctime_nsec == right.st_ctime_nsec
}

fn decode_manifest(bytes: &[u8]) -> Result<SnapshotManifest, WorkerError> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let manifest =
        SnapshotManifest::deserialize(&mut deserializer).map_err(|_| manifest_mismatch())?;
    deserializer.end().map_err(|_| manifest_mismatch())?;
    let canonical = manifest
        .canonical_bytes()
        .map_err(|_| manifest_mismatch())?;
    if canonical != bytes {
        return Err(manifest_mismatch());
    }
    Ok(manifest)
}

fn validate_manifest(manifest: &SnapshotManifest) -> Result<(), WorkerError> {
    if manifest.version != SNAPSHOT_MANIFEST_VERSION
        || !is_lower_hex(&manifest.project_id, 64)
        || !is_lower_hex(&manifest.worktree_id, 64)
        || manifest
            .head
            .as_deref()
            .is_some_and(|head| !matches!(head.len(), 40 | 64) || !is_lower_hex(head, head.len()))
        || manifest.branch.as_deref().is_some_and(|branch| {
            branch.is_empty()
                || branch.len() > MAX_TEXT_FIELD_BYTES
                || branch.as_bytes().contains(&0)
        })
    {
        return Err(manifest_mismatch());
    }
    if !strictly_sorted(&manifest.entries, |entry| entry.path.as_bytes())
        || !strictly_sorted(&manifest.tracked_deletions, |path| path.as_bytes())
    {
        return Err(manifest_mismatch());
    }

    let mut kinds = BTreeMap::new();
    for entry in &manifest.entries {
        RelativePath::parse(entry.path.as_bytes()).map_err(|_| manifest_mismatch())?;
        if kinds.insert(entry.path.as_str(), entry.kind).is_some() {
            return Err(manifest_mismatch());
        }
        validate_manifest_entry(entry)?;
    }
    let mut deletions = BTreeSet::new();
    for deletion in &manifest.tracked_deletions {
        RelativePath::parse(deletion.as_bytes()).map_err(|_| manifest_mismatch())?;
        if !deletions.insert(deletion.as_str()) || kinds.contains_key(deletion.as_str()) {
            return Err(manifest_mismatch());
        }
    }
    for path in kinds.keys() {
        for ancestor in path_ancestors(path) {
            if kinds
                .get(ancestor.as_str())
                .is_some_and(|kind| *kind != ManifestEntryKind::Directory)
            {
                return Err(manifest_mismatch());
            }
        }
    }
    if !manifest.relative_working_dir.is_empty() {
        RelativePath::parse(manifest.relative_working_dir.as_bytes())
            .map_err(|_| manifest_mismatch())?;
        if kinds.get(manifest.relative_working_dir.as_str()) != Some(&ManifestEntryKind::Directory)
        {
            return Err(manifest_mismatch());
        }
    }
    Ok(())
}

fn validate_manifest_entry(entry: &ManifestEntry) -> Result<(), WorkerError> {
    if !is_lower_hex(&entry.sha256, 64) {
        return Err(manifest_mismatch());
    }
    match entry.kind {
        ManifestEntryKind::File => {
            if !matches!(entry.mode, 0o644 | 0o755) || entry.symlink_target.is_some() {
                return Err(manifest_mismatch());
            }
        }
        ManifestEntryKind::Directory => {
            let expected_hash = format!("{:x}", Sha256::digest(b"directory\0"));
            if entry.mode != 0o755
                || entry.size != 0
                || entry.symlink_target.is_some()
                || entry.sha256 != expected_hash
            {
                return Err(manifest_mismatch());
            }
        }
        ManifestEntryKind::Symlink => {
            let target = entry
                .symlink_target
                .as_deref()
                .ok_or_else(manifest_mismatch)?;
            if entry.mode != 0o777
                || target.len() > MAX_SYMLINK_TARGET_BYTES
                || target.as_bytes().contains(&0)
                || entry.size != target.len() as u64
            {
                return Err(manifest_mismatch());
            }
            let mut hasher = Sha256::new();
            hasher.update(b"symlink\0");
            hasher.update(target.as_bytes());
            if entry.sha256 != format!("{:x}", hasher.finalize()) {
                return Err(manifest_mismatch());
            }
        }
    }
    Ok(())
}

fn validate_exact_tree(
    manifest: &SnapshotManifest,
    tree: &SnapshotTreeInspection,
    policy: BundlePolicy,
) -> Result<(), WorkerError> {
    let declared = manifest
        .entries
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    let mut expected_kinds = BTreeMap::<String, ManifestEntryKind>::new();
    for entry in &manifest.entries {
        expected_kinds.insert(entry.path.clone(), entry.kind);
        for ancestor in path_ancestors(&entry.path) {
            expected_kinds
                .entry(ancestor)
                .or_insert(ManifestEntryKind::Directory);
        }
    }
    let mut actual = BTreeMap::new();
    for entry in &tree.entries {
        if entry.device != tree.root_device
            || entry.inode == tree.root_inode
            || actual.insert(entry.path.as_str(), entry).is_some()
        {
            return Err(unsafe_remote_snapshot());
        }
    }
    if actual.keys().copied().collect::<BTreeSet<_>>()
        != expected_kinds
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
    {
        return Err(manifest_mismatch());
    }
    for (path, expected_kind) in expected_kinds {
        let entry = actual.get(path.as_str()).ok_or_else(manifest_mismatch)?;
        let actual_kind = match entry.kind {
            SnapshotFsKind::RegularFile => ManifestEntryKind::File,
            SnapshotFsKind::Directory => ManifestEntryKind::Directory,
            SnapshotFsKind::Symlink => ManifestEntryKind::Symlink,
        };
        if actual_kind != expected_kind {
            return Err(manifest_mismatch());
        }
        if let Some(declaration) = declared.get(path.as_str()) {
            if !physical_mode_matches(declaration, entry.mode, policy)
                || (declaration.kind != ManifestEntryKind::Directory
                    && (declaration.size != entry.size
                        || entry.sha256.as_deref() != Some(declaration.sha256.as_str())))
                || declaration.symlink_target != entry.symlink_target
            {
                return Err(manifest_mismatch());
            }
        } else if entry.kind != SnapshotFsKind::Directory
            || !physical_directory_mode_matches(entry.mode, policy)
        {
            return Err(manifest_mismatch());
        }
    }
    Ok(())
}

fn physical_mode_matches(entry: &ManifestEntry, actual: u32, policy: BundlePolicy) -> bool {
    match entry.kind {
        ManifestEntryKind::File => match (entry.mode, policy) {
            (0o644, BundlePolicy::Incoming) => matches!(actual, 0o444 | 0o400),
            (0o755, BundlePolicy::Incoming) => matches!(actual, 0o555 | 0o500),
            (0o644, BundlePolicy::Prepared | BundlePolicy::Cache) => actual == 0o400,
            (0o755, BundlePolicy::Prepared | BundlePolicy::Cache) => actual == 0o500,
            (0o644, BundlePolicy::Workspace) => actual == 0o600,
            (0o755, BundlePolicy::Workspace) => actual == 0o700,
            _ => false,
        },
        ManifestEntryKind::Directory => physical_directory_mode_matches(actual, policy),
        ManifestEntryKind::Symlink => physical_symlink_mode_matches(actual),
    }
}

fn physical_symlink_mode_matches(actual: u32) -> bool {
    #[cfg(target_vendor = "apple")]
    {
        matches!(actual, 0o755 | 0o777)
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        actual == 0o777
    }
}

fn physical_directory_mode_matches(actual: u32, policy: BundlePolicy) -> bool {
    match policy {
        BundlePolicy::Incoming => matches!(actual, 0o555 | 0o500),
        BundlePolicy::Prepared | BundlePolicy::Cache => actual == 0o500,
        BundlePolicy::Workspace => actual == 0o700,
    }
}

fn strictly_sorted<T>(values: &[T], key: impl Fn(&T) -> &[u8]) -> bool {
    values.windows(2).all(|pair| key(&pair[0]) < key(&pair[1]))
}

fn path_ancestors(path: &str) -> Vec<String> {
    let components = path.split('/').collect::<Vec<_>>();
    (1..components.len())
        .map(|length| components[..length].join("/"))
        .collect()
}

fn declared_tree_paths(manifest: &SnapshotManifest) -> Result<BTreeSet<RelativePath>, WorkerError> {
    let mut declared = BTreeSet::new();
    for entry in &manifest.entries {
        declared.insert(relative(&entry.path)?);
        for ancestor in path_ancestors(&entry.path) {
            declared.insert(relative(&ancestor)?);
        }
    }
    Ok(declared)
}

fn relative(path: &str) -> Result<RelativePath, WorkerError> {
    RelativePath::parse(path.as_bytes()).map_err(|_| manifest_mismatch())
}

fn require_exact_live(
    store: &HostStore,
    admission: &AdmissionGuard,
    expected: &LeaseRecord,
) -> Result<LeaseRecord, WorkerError> {
    let live = LeaseService::new(store)
        .load_after(admission, expected.job_id())?
        .ok_or_else(lease_identity_mismatch)?;
    if live != *expected {
        return Err(lease_identity_mismatch());
    }
    Ok(live)
}

fn require_verifiable_disposition(
    store: &HostStore,
    live: &LeaseRecord,
) -> Result<(), WorkerError> {
    let Some(disposition) = store.disposition(live.job_id())? else {
        return Ok(());
    };
    let matches = match &disposition {
        JobDisposition::Accepted {
            job_id,
            client_id,
            project_id,
            worktree_id,
            request_fingerprint,
            ..
        } => {
            *job_id == live.job_id()
                && *client_id == live.client_id()
                && project_id == live.project_id()
                && worktree_id == live.worktree_id()
                && request_fingerprint == live.request_fingerprint()
        }
        JobDisposition::Abandoned {
            job_id,
            client_id,
            project_id,
            worktree_id,
            request_fingerprint,
            lease_token_sha256,
            ..
        } => {
            *job_id == live.job_id()
                && *client_id == live.client_id()
                && project_id == live.project_id()
                && worktree_id == live.worktree_id()
                && request_fingerprint == live.request_fingerprint()
                && lease_token_sha256 == &lease_token_hash(live.lease_token())
        }
    };
    if !matches {
        return Err(protocol_code(
            "JOB_ID_CONFLICT",
            "job ID belongs to another immutable request",
        ));
    }
    match disposition {
        JobDisposition::Accepted { .. } => {
            Err(protocol_code("JOB_ACCEPTED", "job ID was already accepted"))
        }
        JobDisposition::Abandoned { .. } => Err(protocol_code(
            "JOB_ABANDONED",
            "job ID was permanently abandoned",
        )),
    }
}

fn require_matching_accepted_disposition(
    store: &HostStore,
    live: &LeaseRecord,
) -> Result<(), WorkerError> {
    match store.disposition(live.job_id())? {
        Some(JobDisposition::Accepted {
            job_id,
            client_id,
            project_id,
            worktree_id,
            request_fingerprint,
            ..
        }) if job_id == live.job_id()
            && client_id == live.client_id()
            && project_id == live.project_id()
            && worktree_id == live.worktree_id()
            && request_fingerprint == *live.request_fingerprint() =>
        {
            Ok(())
        }
        _ => Err(protocol_code(
            "JOB_ID_CONFLICT",
            "accepted disposition does not match the live lease",
        )),
    }
}

fn receipt_for_lease(
    lease: &LeaseRecord,
    verified_at_millis: u64,
) -> Result<VerifiedReceipt, WorkerError> {
    VerifiedReceipt::new(
        lease.job_id(),
        lease.client_id(),
        lease_token_hash(lease.lease_token()),
        lease.request_fingerprint().clone(),
        SnapshotCacheKey::new(
            lease.project_id().into(),
            lease.worktree_id().into(),
            lease.manifest_digest().into(),
        )?,
        verified_at_millis,
    )
}

fn receipt_identity_equal(left: &VerifiedReceipt, right: &VerifiedReceipt) -> bool {
    left.version == right.version
        && left.job_id == right.job_id
        && left.client_id == right.client_id
        && left.lease_token_sha256 == right.lease_token_sha256
        && left.request_fingerprint == right.request_fingerprint
        && left.project_id == right.project_id
        && left.worktree_id == right.worktree_id
        && left.manifest_digest == right.manifest_digest
        && left.cache_key == right.cache_key
}

fn snapshot_from_parts(
    cache_root: RootedDir,
    lease: &LeaseRecord,
    verified_at_millis: u64,
    cache_reused: bool,
) -> Result<VerifiedRemoteSnapshot, WorkerError> {
    let manifest = validate_bundle(
        &cache_root,
        lease,
        lease.manifest_digest(),
        BundlePolicy::Cache,
    )?;
    Ok(VerifiedRemoteSnapshot {
        project_id: lease.project_id().into(),
        worktree_id: lease.worktree_id().into(),
        digest: lease.manifest_digest().into(),
        manifest,
        cache_root,
        lease: lease.clone(),
        verified_at_millis,
        cache_reused,
    })
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn manifest_mismatch() -> WorkerError {
    WorkerError::Snapshot {
        code: "MANIFEST_MISMATCH",
        message: "remote snapshot does not match its canonical manifest".into(),
    }
}

fn unsafe_snapshot_io(_error: io::Error) -> WorkerError {
    unsafe_remote_snapshot()
}

fn snapshot_root_mode(root: &RootedDir) -> Result<u32, WorkerError> {
    Ok((root.root_metadata().map_err(unsafe_snapshot_io)?.st_mode & 0o7777) as u32)
}

fn seal_recovered_prepared_cache(
    cache: &RootedDir,
    lease: &LeaseRecord,
    digest: &str,
) -> Result<(), WorkerError> {
    validate_bundle(cache, lease, digest, BundlePolicy::Prepared)?;
    cache.seal_snapshot_root().map_err(unsafe_snapshot_io)?;
    validate_bundle(cache, lease, digest, BundlePolicy::Cache)?;
    Ok(())
}

fn map_verified_cleanup_io(error: io::Error) -> WorkerError {
    if error.raw_os_error() == Some(libc::ESTALE) {
        unsafe_remote_snapshot()
    } else {
        WorkerError::Io(error)
    }
}
