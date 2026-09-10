use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, io,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer, de,
    de::DeserializeOwned,
    ser::{self, SerializeStruct},
};
use sha2::{Digest, Sha256};

use crate::{
    error::WorkerError,
    host_store::{
        AdmissionGuard, HostStore, HostStoreWritePoint, JobDisposition, ResolutionIdentity,
        StagedJob, TransferGuard, WorkspaceReceipt,
    },
    inputs::RelativePath,
    job::{ClientId, JobId, LeaseRecord, LeaseToken, RequestFingerprint},
    lease::LeaseService,
    manifest::{ManifestEntry, ManifestEntryKind, SnapshotManifest},
    protocol::PROTOCOL_VERSION,
    rooted_fs::{RootedDir, SnapshotFsKind, SnapshotProjection, SnapshotTreeInspection},
};

const VERIFIED_RECEIPT_VERSION: u32 = 1;
const SNAPSHOT_MANIFEST_VERSION: u32 = 1;
const MAX_MANIFEST_BYTES: u64 = 8 * 1024 * 1024;
const MAX_TEXT_FIELD_BYTES: usize = 128 * 1024;
const MAX_SYMLINK_TARGET_BYTES: usize = 64 * 1024;
const MANIFEST_FILE: &str = "manifest.json";
const TREE_DIRECTORY: &str = "tree";

pub struct RemoteSnapshotService<'a> {
    store: &'a HostStore,
}

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
    pub fn new(store: &'a HostStore) -> Self {
        Self { store }
    }

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

    pub fn verify_request(
        &self,
        request: &SnapshotVerifyRequest,
    ) -> Result<VerifiedSnapshotResponse, WorkerError> {
        request.validate()?;
        let live = LeaseService::new(self.store)
            .load_for_job(request.job_id())?
            .ok_or_else(lease_identity_mismatch)?;
        if request.job_id() != live.job_id()
            || request.client_id() != live.client_id()
            || request.lease_token() != live.lease_token()
            || request.request_fingerprint() != live.request_fingerprint()
            || request.project_id() != live.project_id()
            || request.worktree_id() != live.worktree_id()
            || request.manifest_digest() != live.manifest_digest()
        {
            return Err(lease_identity_mismatch());
        }
        let snapshot = self.verify_and_promote(&live, request.manifest_digest())?;
        VerifiedSnapshotResponse::new(
            live.job_id(),
            live.client_id(),
            snapshot.project_id().into(),
            snapshot.worktree_id().into(),
            snapshot.digest().into(),
            snapshot.verified_at_millis(),
            snapshot.cache_reused(),
        )
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

    pub(crate) fn validate_resolution_evidence_after(
        &self,
        admission: &AdmissionGuard,
        transfer: &TransferGuard,
        identity: &ResolutionIdentity,
    ) -> Result<(), WorkerError> {
        admission.validate_for(identity.job_id())?;
        transfer.validate()?;
        let directory = self.store.open_directory("verified", false)?;
        for name in [
            format!("{}.json", identity.job_id()),
            format!(".verify-{}.json.pending", identity.job_id()),
        ] {
            if directory.entry_exists(&name)? {
                let bytes = directory
                    .read_private_regular(&name, 1024 * 1024)
                    .map_err(|_| unsafe_remote_snapshot())?;
                let receipt: VerifiedReceipt = decode_canonical_json(&bytes, "verified receipt")?;
                require_resolution_receipt(&receipt, identity)?;
            }
        }
        admission.validate_for(identity.job_id())?;
        transfer.validate()
    }

    pub(crate) fn remove_resolution_evidence_after(
        &self,
        admission: &AdmissionGuard,
        transfer: &TransferGuard,
        identity: &ResolutionIdentity,
    ) -> Result<(), WorkerError> {
        self.validate_resolution_evidence_after(admission, transfer, identity)?;
        let directory = self.store.open_directory("verified", false)?;
        let receipt = format!("{}.json", identity.job_id());
        self.store
            .remove_owned_regular_committed(&directory, &receipt)?;
        directory.sync_root()?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterResolutionVerifiedReceiptRemoval)
        {
            return Err(WorkerError::Io(io::Error::other(
                "injected resolution verified-receipt cleanup interruption",
            )));
        }
        let staging = format!(".verify-{}.json.pending", identity.job_id());
        self.store
            .remove_owned_regular_committed(&directory, &staging)?;
        directory.sync_root()?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterResolutionVerificationStageRemoval)
        {
            return Err(WorkerError::Io(io::Error::other(
                "injected resolution verification-stage cleanup interruption",
            )));
        }
        admission.validate_for(identity.job_id())?;
        transfer.validate()
    }

    pub(crate) fn resolution_evidence_absent_after(
        &self,
        admission: &AdmissionGuard,
        transfer: &TransferGuard,
        identity: &ResolutionIdentity,
    ) -> Result<(), WorkerError> {
        admission.validate_for(identity.job_id())?;
        transfer.validate()?;
        let directory = self.store.open_directory("verified", false)?;
        for name in [
            format!("{}.json", identity.job_id()),
            format!(".verify-{}.json.pending", identity.job_id()),
        ] {
            if directory.entry_exists(&name)? {
                return Err(WorkerError::Protocol(
                    "exact verified state remains after resolution cleanup".into(),
                ));
            }
        }
        if directory.has_private_cleanup_residue()? {
            return Err(WorkerError::Protocol(
                "verified private cleanup residue remains".into(),
            ));
        }
        Ok(())
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

    fn read_receipt_optional(&self, job_id: JobId) -> Result<Option<VerifiedReceipt>, WorkerError> {
        let directory = self.store.open_directory("verified", false)?;
        let name = format!("{job_id}.json");
        if !directory.entry_exists(&name)? {
            return Ok(None);
        }
        let bytes = directory
            .read_private_regular(&name, 1024 * 1024)
            .map_err(|_| unsafe_remote_snapshot())?;
        decode_canonical_json(&bytes, "verified receipt").map(Some)
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

fn validate_receipt_identity(
    receipt: &VerifiedReceipt,
    lease: &LeaseRecord,
    request_fingerprint: &RequestFingerprint,
) -> Result<(), WorkerError> {
    receipt.validate()?;
    if receipt.job_id != lease.job_id()
        || receipt.client_id != lease.client_id()
        || receipt.lease_token_sha256 != lease_token_hash(lease.lease_token())
        || &receipt.request_fingerprint != request_fingerprint
        || receipt.project_id != lease.project_id()
        || receipt.worktree_id != lease.worktree_id()
        || receipt.manifest_digest != lease.manifest_digest()
    {
        return Err(lease_identity_mismatch());
    }
    Ok(())
}

fn require_resolution_receipt(
    receipt: &VerifiedReceipt,
    identity: &ResolutionIdentity,
) -> Result<(), WorkerError> {
    receipt.validate()?;
    if receipt.job_id() == identity.job_id()
        && receipt.client_id() == identity.client_id()
        && receipt.lease_token_sha256() == identity.token_hash()
        && receipt.request_fingerprint() == identity.request_fingerprint()
        && receipt.cache_key().project_id() == identity.project_id()
        && receipt.cache_key().worktree_id() == identity.worktree_id()
        && receipt.cache_key().manifest_digest() == identity.manifest_digest()
    {
        Ok(())
    } else {
        Err(WorkerError::Protocol(
            "JOB_ID_CONFLICT: verified state belongs to another immutable request".into(),
        ))
    }
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

fn lease_token_hash(token: LeaseToken) -> String {
    format!("{:x}", Sha256::digest(token.to_string().as_bytes()))
}

fn decode_canonical_json<T: DeserializeOwned + Serialize>(
    bytes: &[u8],
    _label: &str,
) -> Result<T, WorkerError> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = T::deserialize(&mut deserializer).map_err(|_| unsafe_remote_snapshot())?;
    deserializer.end().map_err(|_| unsafe_remote_snapshot())?;
    if serde_json::to_vec(&value).map_err(|_| unsafe_remote_snapshot())? != bytes {
        return Err(unsafe_remote_snapshot());
    }
    Ok(value)
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

fn unsafe_remote_snapshot() -> WorkerError {
    WorkerError::Snapshot {
        code: "UNSAFE_REMOTE_SNAPSHOT",
        message: "remote snapshot filesystem state is unsafe".into(),
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

fn lease_identity_mismatch() -> WorkerError {
    protocol_code(
        "LEASE_IDENTITY_MISMATCH",
        "live lease identity was rejected",
    )
}

fn protocol_code(code: &'static str, message: &str) -> WorkerError {
    WorkerError::Protocol(format!("{code}: {message}"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotCacheKey {
    project_id: String,
    worktree_id: String,
    manifest_digest: String,
}

impl SnapshotCacheKey {
    pub fn new(
        project_id: String,
        worktree_id: String,
        manifest_digest: String,
    ) -> Result<Self, WorkerError> {
        let key = Self {
            project_id,
            worktree_id,
            manifest_digest,
        };
        key.validate()?;
        Ok(key)
    }

    fn validate(&self) -> Result<(), WorkerError> {
        validate_digest(&self.project_id, "project ID")?;
        validate_digest(&self.worktree_id, "worktree ID")?;
        validate_digest(&self.manifest_digest, "manifest digest")
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn worktree_id(&self) -> &str {
        &self.worktree_id
    }

    pub fn manifest_digest(&self) -> &str {
        &self.manifest_digest
    }
}

impl Serialize for SnapshotCacheKey {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("SnapshotCacheKey", 3)?;
        record.serialize_field("project_id", &self.project_id)?;
        record.serialize_field("worktree_id", &self.worktree_id)?;
        record.serialize_field("manifest_digest", &self.manifest_digest)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for SnapshotCacheKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            project_id: String,
            worktree_id: String,
            manifest_digest: String,
        }

        let wire = Wire::deserialize(deserializer)?;
        SnapshotCacheKey::new(wire.project_id, wire.worktree_id, wire.manifest_digest)
            .map_err(de::Error::custom)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SnapshotVerifyRequest {
    protocol_version: u32,
    job_id: JobId,
    client_id: ClientId,
    lease_token: LeaseToken,
    request_fingerprint: RequestFingerprint,
    project_id: String,
    worktree_id: String,
    manifest_digest: String,
}

impl SnapshotVerifyRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        job_id: JobId,
        client_id: ClientId,
        lease_token: LeaseToken,
        request_fingerprint: RequestFingerprint,
        project_id: String,
        worktree_id: String,
        manifest_digest: String,
    ) -> Result<Self, WorkerError> {
        let request = Self {
            protocol_version: PROTOCOL_VERSION,
            job_id,
            client_id,
            lease_token,
            request_fingerprint,
            project_id,
            worktree_id,
            manifest_digest,
        };
        request.validate()?;
        Ok(request)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(protocol_error(
                "snapshot verification protocol version mismatch",
            ));
        }
        validate_digest(&self.project_id, "project ID")?;
        validate_digest(&self.worktree_id, "worktree ID")?;
        validate_digest(&self.manifest_digest, "manifest digest")
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    pub fn lease_token(&self) -> LeaseToken {
        self.lease_token
    }

    pub fn request_fingerprint(&self) -> &RequestFingerprint {
        &self.request_fingerprint
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn worktree_id(&self) -> &str {
        &self.worktree_id
    }

    pub fn manifest_digest(&self) -> &str {
        &self.manifest_digest
    }
}

impl fmt::Debug for SnapshotVerifyRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SnapshotVerifyRequest")
            .field("protocol_version", &self.protocol_version)
            .field("job_id", &self.job_id)
            .field("client_id", &self.client_id)
            .field("request_fingerprint", &self.request_fingerprint)
            .field("project_id", &self.project_id)
            .field("worktree_id", &self.worktree_id)
            .field("manifest_digest", &self.manifest_digest)
            .finish_non_exhaustive()
    }
}

impl Serialize for SnapshotVerifyRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("SnapshotVerifyRequest", 8)?;
        record.serialize_field("protocol_version", &self.protocol_version)?;
        record.serialize_field("job_id", &self.job_id)?;
        record.serialize_field("client_id", &self.client_id)?;
        record.serialize_field("lease_token", &self.lease_token)?;
        record.serialize_field("request_fingerprint", &self.request_fingerprint)?;
        record.serialize_field("project_id", &self.project_id)?;
        record.serialize_field("worktree_id", &self.worktree_id)?;
        record.serialize_field("manifest_digest", &self.manifest_digest)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for SnapshotVerifyRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            protocol_version: u32,
            job_id: JobId,
            client_id: ClientId,
            lease_token: LeaseToken,
            request_fingerprint: RequestFingerprint,
            project_id: String,
            worktree_id: String,
            manifest_digest: String,
        }

        let wire = Wire::deserialize(deserializer)?;
        let request = Self {
            protocol_version: wire.protocol_version,
            job_id: wire.job_id,
            client_id: wire.client_id,
            lease_token: wire.lease_token,
            request_fingerprint: wire.request_fingerprint,
            project_id: wire.project_id,
            worktree_id: wire.worktree_id,
            manifest_digest: wire.manifest_digest,
        };
        request.validate().map_err(de::Error::custom)?;
        Ok(request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedReceipt {
    version: u32,
    job_id: JobId,
    client_id: ClientId,
    lease_token_sha256: String,
    request_fingerprint: RequestFingerprint,
    project_id: String,
    worktree_id: String,
    manifest_digest: String,
    cache_key: SnapshotCacheKey,
    verified_at_millis: u64,
}

impl VerifiedReceipt {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        job_id: JobId,
        client_id: ClientId,
        lease_token_sha256: String,
        request_fingerprint: RequestFingerprint,
        cache_key: SnapshotCacheKey,
        verified_at_millis: u64,
    ) -> Result<Self, WorkerError> {
        let receipt = Self {
            version: VERIFIED_RECEIPT_VERSION,
            job_id,
            client_id,
            lease_token_sha256,
            request_fingerprint,
            project_id: cache_key.project_id.clone(),
            worktree_id: cache_key.worktree_id.clone(),
            manifest_digest: cache_key.manifest_digest.clone(),
            cache_key,
            verified_at_millis,
        };
        receipt.validate()?;
        Ok(receipt)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.version != VERIFIED_RECEIPT_VERSION {
            return Err(protocol_error("verified receipt version mismatch"));
        }
        validate_digest(&self.lease_token_sha256, "lease token hash")?;
        validate_digest(&self.project_id, "project ID")?;
        validate_digest(&self.worktree_id, "worktree ID")?;
        validate_digest(&self.manifest_digest, "manifest digest")?;
        self.cache_key.validate()?;
        if self.project_id != self.cache_key.project_id
            || self.worktree_id != self.cache_key.worktree_id
            || self.manifest_digest != self.cache_key.manifest_digest
        {
            return Err(protocol_error("verified receipt cache key mismatch"));
        }
        Ok(())
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    pub fn lease_token_sha256(&self) -> &str {
        &self.lease_token_sha256
    }

    pub fn request_fingerprint(&self) -> &RequestFingerprint {
        &self.request_fingerprint
    }

    pub fn cache_key(&self) -> &SnapshotCacheKey {
        &self.cache_key
    }

    pub fn verified_at_millis(&self) -> u64 {
        self.verified_at_millis
    }
}

impl Serialize for VerifiedReceipt {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("VerifiedReceipt", 10)?;
        record.serialize_field("version", &self.version)?;
        record.serialize_field("job_id", &self.job_id)?;
        record.serialize_field("client_id", &self.client_id)?;
        record.serialize_field("lease_token_sha256", &self.lease_token_sha256)?;
        record.serialize_field("request_fingerprint", &self.request_fingerprint)?;
        record.serialize_field("project_id", &self.project_id)?;
        record.serialize_field("worktree_id", &self.worktree_id)?;
        record.serialize_field("manifest_digest", &self.manifest_digest)?;
        record.serialize_field("cache_key", &self.cache_key)?;
        record.serialize_field("verified_at_millis", &self.verified_at_millis)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for VerifiedReceipt {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            version: u32,
            job_id: JobId,
            client_id: ClientId,
            lease_token_sha256: String,
            request_fingerprint: RequestFingerprint,
            project_id: String,
            worktree_id: String,
            manifest_digest: String,
            cache_key: SnapshotCacheKey,
            verified_at_millis: u64,
        }

        let wire = Wire::deserialize(deserializer)?;
        let receipt = Self {
            version: wire.version,
            job_id: wire.job_id,
            client_id: wire.client_id,
            lease_token_sha256: wire.lease_token_sha256,
            request_fingerprint: wire.request_fingerprint,
            project_id: wire.project_id,
            worktree_id: wire.worktree_id,
            manifest_digest: wire.manifest_digest,
            cache_key: wire.cache_key,
            verified_at_millis: wire.verified_at_millis,
        };
        receipt.validate().map_err(de::Error::custom)?;
        Ok(receipt)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedSnapshotResponse {
    protocol_version: u32,
    job_id: JobId,
    client_id: ClientId,
    project_id: String,
    worktree_id: String,
    manifest_digest: String,
    verified_at_millis: u64,
    cache_reused: bool,
}

impl VerifiedSnapshotResponse {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        job_id: JobId,
        client_id: ClientId,
        project_id: String,
        worktree_id: String,
        manifest_digest: String,
        verified_at_millis: u64,
        cache_reused: bool,
    ) -> Result<Self, WorkerError> {
        let response = Self {
            protocol_version: PROTOCOL_VERSION,
            job_id,
            client_id,
            project_id,
            worktree_id,
            manifest_digest,
            verified_at_millis,
            cache_reused,
        };
        response.validate()?;
        Ok(response)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(protocol_error(
                "snapshot response protocol version mismatch",
            ));
        }
        validate_digest(&self.project_id, "project ID")?;
        validate_digest(&self.worktree_id, "worktree ID")?;
        validate_digest(&self.manifest_digest, "manifest digest")
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn worktree_id(&self) -> &str {
        &self.worktree_id
    }

    pub fn manifest_digest(&self) -> &str {
        &self.manifest_digest
    }

    pub fn verified_at_millis(&self) -> u64 {
        self.verified_at_millis
    }

    pub fn cache_reused(&self) -> bool {
        self.cache_reused
    }
}

impl Serialize for VerifiedSnapshotResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("VerifiedSnapshotResponse", 8)?;
        record.serialize_field("protocol_version", &self.protocol_version)?;
        record.serialize_field("job_id", &self.job_id)?;
        record.serialize_field("client_id", &self.client_id)?;
        record.serialize_field("project_id", &self.project_id)?;
        record.serialize_field("worktree_id", &self.worktree_id)?;
        record.serialize_field("manifest_digest", &self.manifest_digest)?;
        record.serialize_field("verified_at_millis", &self.verified_at_millis)?;
        record.serialize_field("cache_reused", &self.cache_reused)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for VerifiedSnapshotResponse {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            protocol_version: u32,
            job_id: JobId,
            client_id: ClientId,
            project_id: String,
            worktree_id: String,
            manifest_digest: String,
            verified_at_millis: u64,
            cache_reused: bool,
        }

        let wire = Wire::deserialize(deserializer)?;
        let response = Self {
            protocol_version: wire.protocol_version,
            job_id: wire.job_id,
            client_id: wire.client_id,
            project_id: wire.project_id,
            worktree_id: wire.worktree_id,
            manifest_digest: wire.manifest_digest,
            verified_at_millis: wire.verified_at_millis,
            cache_reused: wire.cache_reused,
        };
        response.validate().map_err(de::Error::custom)?;
        Ok(response)
    }
}

fn validate_digest(value: &str, field: &str) -> Result<(), WorkerError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        Ok(())
    } else {
        Err(protocol_error(&format!(
            "{field} must be 64 lowercase hexadecimal bytes"
        )))
    }
}

fn protocol_error(message: &str) -> WorkerError {
    WorkerError::Protocol(message.into())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        sync::{Arc, Barrier, mpsc},
        thread,
        time::Duration,
    };

    use sha2::{Digest, Sha256};

    use super::*;
    use crate::{
        host_store::SupervisorGuard,
        job::{
            CommandSpec, LeaseAcquireRequest, LeaseAcquireResponse, RequestFingerprintMaterial,
            ResolveOrAbandonOutcome, ResolveOrAbandonRequest, SubmitRequest,
        },
        job_service::{JobService, LaunchCandidate, SupervisorLauncher},
        lease::{AdmissionFacts, LeaseService},
        protocol::MemoryPressure,
    };

    struct NeverLaunchResolution;

    impl SupervisorLauncher for NeverLaunchResolution {
        fn launch(
            &self,
            _job_id: JobId,
            _guard: SupervisorGuard,
        ) -> Result<LaunchCandidate, WorkerError> {
            panic!("verifier-first abandonment must never launch")
        }
    }

    const PROJECT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const WORKTREE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn lease(id: u128, token: u128, digest: &str) -> LeaseRecord {
        let material = RequestFingerprintMaterial::new(
            JobId::new(uuid::Uuid::from_u128(id)),
            ClientId::new(uuid::Uuid::from_u128(id + 100)),
            LeaseToken::new(uuid::Uuid::from_u128(token)),
            1,
            "mini-1".into(),
            PROJECT.into(),
            WORKTREE.into(),
            digest.into(),
            String::new(),
            60_000,
            "heavy".into(),
            CommandSpec::shell("true".into()).unwrap(),
        )
        .unwrap();
        LeaseRecord::new(&material, material.fingerprint(), 1, 60_001).unwrap()
    }

    fn create_candidate(store: &HostStore, lease: &LeaseRecord, manifest: &[u8]) {
        let incoming = store
            .incoming_job(lease.job_id(), lease.lease_token())
            .unwrap();
        fs::create_dir_all(incoming.join(TREE_DIRECTORY)).unwrap();
        fs::set_permissions(
            incoming.parent().unwrap(),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        fs::set_permissions(&incoming, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(incoming.join(MANIFEST_FILE), manifest).unwrap();
        fs::set_permissions(
            incoming.join(MANIFEST_FILE),
            fs::Permissions::from_mode(0o444),
        )
        .unwrap();
        fs::set_permissions(
            incoming.join(TREE_DIRECTORY),
            fs::Permissions::from_mode(0o555),
        )
        .unwrap();
    }

    #[test]
    fn distinct_validated_incoming_roots_race_one_no_replace_cache() {
        // Two simultaneous live heavy leases are forbidden. Exercise the
        // shared cache boundary directly after each distinct token-owned root
        // has independently passed exact validation and conversion.
        // The barrier is the no-replace claim, not a sealed-cache handshake:
        // the loser must recover while the winner may still be sealing.
        let fixture = tempfile::tempdir().unwrap();
        let host_root = fixture.path().join("host");
        let store = HostStore::open(&host_root).unwrap();
        let manifest = SnapshotManifest {
            version: SNAPSHOT_MANIFEST_VERSION,
            project_id: PROJECT.into(),
            worktree_id: WORKTREE.into(),
            head: None,
            branch: None,
            dirty: false,
            relative_working_dir: String::new(),
            entries: Vec::new(),
            tracked_deletions: Vec::new(),
        }
        .canonical_bytes()
        .unwrap();
        let digest = format!("{:x}", Sha256::digest(&manifest));
        let leases = [lease(1, 11, &digest), lease(2, 22, &digest)];
        for lease in &leases {
            create_candidate(&store, lease, &manifest);
        }
        drop(
            store
                .open_directory(&format!("snapshots/{PROJECT}/{WORKTREE}"), true)
                .unwrap(),
        );
        drop(store);

        let barrier = Arc::new(Barrier::new(2));
        let handles = leases.map(|lease| {
            let host_root = host_root.clone();
            let digest = digest.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let store = HostStore::open(&host_root).unwrap();
                let relative = format!("incoming/{}/{}", lease.job_id(), lease.lease_token());
                let incoming = store.open_directory(&relative, false).unwrap();
                validate_bundle(&incoming, &lease, &digest, BundlePolicy::Incoming).unwrap();
                incoming
                    .prepare_snapshot_for_publication_with_hook(|| Ok(()))
                    .unwrap();
                validate_bundle(&incoming, &lease, &digest, BundlePolicy::Prepared).unwrap();
                let service = RemoteSnapshotService::new(&store);
                let (cache, reused) = service
                    .publish_validated_cache_candidate(incoming, &lease, &digest, || {
                        barrier.wait();
                        Ok(())
                    })
                    .unwrap();
                validate_bundle(&cache, &lease, &digest, BundlePolicy::Cache).unwrap();
                let source_remains = store
                    .incoming_job(lease.job_id(), lease.lease_token())
                    .unwrap()
                    .exists();
                (reused, source_remains)
            })
        });
        let outcomes = handles.map(|handle| handle.join().unwrap());
        assert_eq!(outcomes.iter().filter(|(reused, _)| !reused).count(), 1);
        assert_eq!(outcomes.iter().filter(|(reused, _)| *reused).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|(_, source_remains)| *source_remains)
                .count(),
            1
        );
        let store = HostStore::open(&host_root).unwrap();
        let cache = store.snapshot(PROJECT, WORKTREE, &digest).unwrap();
        assert_eq!(fs::read_dir(cache.parent().unwrap()).unwrap().count(), 1);
        assert_eq!(
            fs::metadata(cache).unwrap().permissions().mode() & 0o777,
            0o500
        );
    }

    #[test]
    fn bundle_validation_rejects_root_child_replacement_after_manifest_read() {
        let fixture = tempfile::tempdir().unwrap();
        let host_root = fixture.path().join("host");
        let store = HostStore::open(&host_root).unwrap();
        let manifest = SnapshotManifest {
            version: SNAPSHOT_MANIFEST_VERSION,
            project_id: PROJECT.into(),
            worktree_id: WORKTREE.into(),
            head: None,
            branch: None,
            dirty: false,
            relative_working_dir: String::new(),
            entries: Vec::new(),
            tracked_deletions: Vec::new(),
        }
        .canonical_bytes()
        .unwrap();
        let digest = format!("{:x}", Sha256::digest(&manifest));
        let lease = lease(7, 77, &digest);
        create_candidate(&store, &lease, &manifest);
        let incoming_path = store
            .incoming_job(lease.job_id(), lease.lease_token())
            .unwrap();
        let incoming = store
            .open_directory(
                &format!("incoming/{}/{}", lease.job_id(), lease.lease_token()),
                false,
            )
            .unwrap();

        let error =
            validate_bundle_with_hook(&incoming, &lease, &digest, BundlePolicy::Incoming, || {
                // Darwin refuses to rename a read-only directory even when
                // its parent is writable; the owner can make it writable as
                // part of the adversarial replacement and restore the exact
                // transport projection on the new entry.
                fs::set_permissions(
                    incoming_path.join("tree"),
                    fs::Permissions::from_mode(0o755),
                )
                .unwrap();
                fs::rename(incoming_path.join("tree"), incoming_path.join("old-tree")).unwrap();
                fs::create_dir(incoming_path.join("tree")).unwrap();
                fs::set_permissions(
                    incoming_path.join("tree"),
                    fs::Permissions::from_mode(0o555),
                )
                .unwrap();
                fs::remove_dir(incoming_path.join("old-tree")).unwrap();
            })
            .unwrap_err();

        assert!(error.to_string().contains("UNSAFE_REMOTE_SNAPSHOT"));
    }

    #[test]
    fn resolver_cannot_tombstone_while_verification_owns_admission_and_transfer() {
        let fixture = tempfile::tempdir().unwrap();
        let host_root = fixture.path().join("host");
        let store = HostStore::open(&host_root).unwrap();
        let manifest = SnapshotManifest {
            version: SNAPSHOT_MANIFEST_VERSION,
            project_id: PROJECT.into(),
            worktree_id: WORKTREE.into(),
            head: None,
            branch: None,
            dirty: false,
            relative_working_dir: String::new(),
            entries: Vec::new(),
            tracked_deletions: Vec::new(),
        }
        .canonical_bytes()
        .unwrap();
        let digest = format!("{:x}", Sha256::digest(&manifest));
        let material = RequestFingerprintMaterial::new(
            JobId::new(uuid::Uuid::from_u128(31)),
            ClientId::new(uuid::Uuid::from_u128(32)),
            LeaseToken::new(uuid::Uuid::from_u128(33)),
            1,
            "mini-1".into(),
            PROJECT.into(),
            WORKTREE.into(),
            digest.clone(),
            String::new(),
            60_000,
            "heavy".into(),
            CommandSpec::shell("true".into()).unwrap(),
        )
        .unwrap();
        let request = LeaseAcquireRequest::new(material);
        let lease = match LeaseService::new(&store)
            .acquire(
                &request,
                &AdmissionFacts {
                    free_disk_bytes: 100 * 1024 * 1024 * 1024,
                    total_disk_bytes: 250 * 1024 * 1024 * 1024,
                    memory_pressure: MemoryPressure::Normal,
                    swap_used_bytes: Some(0),
                },
                1,
            )
            .unwrap()
        {
            LeaseAcquireResponse::Acquired { lease } => lease,
            LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
        };
        create_candidate(&store, &lease, &manifest);
        drop(store);
        let resolver_store = HostStore::open(&host_root).unwrap();

        let (entered_tx, entered_rx) = mpsc::channel();
        let (verified_tx, verified_rx) = mpsc::channel();
        let verify_root = host_root.clone();
        let verify_lease = lease.clone();
        let verify_digest = digest.clone();
        let verifier = thread::spawn(move || {
            let store = HostStore::open(&verify_root).unwrap();
            let result = RemoteSnapshotService::new(&store).verify_and_promote_at_with_lock_hook(
                &verify_lease,
                &verify_digest,
                42,
                || {
                    let (release, wait) = mpsc::channel();
                    entered_tx.send(release).unwrap();
                    wait.recv_timeout(Duration::from_secs(5)).unwrap();
                },
            );
            verified_tx.send(result.map(|_| ())).unwrap();
        });
        let release = entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let (resolved_tx, resolved_rx) = mpsc::channel();
        let resolver_request = ResolveOrAbandonRequest::from_submit_request(&SubmitRequest::new(
            request.material().clone(),
        ))
        .unwrap();
        let resolver = thread::spawn(move || {
            resolved_tx
                .send(
                    JobService::new(&resolver_store, &NeverLaunchResolution)
                        .resolve_or_abandon(resolver_request),
                )
                .unwrap();
        });
        assert!(
            resolved_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err()
        );
        assert!(
            !host_root
                .join("job-index")
                .join(format!("{}.json", lease.job_id()))
                .exists()
        );

        release.send(()).unwrap();
        verified_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        let resolved = resolved_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        assert!(matches!(
            resolved.outcome(),
            ResolveOrAbandonOutcome::Abandoned
        ));
        assert!(
            !HostStore::open(&host_root)
                .unwrap()
                .verified_receipt(lease.job_id())
                .unwrap()
                .exists()
        );
        verifier.join().unwrap();
        resolver.join().unwrap();
    }
}
