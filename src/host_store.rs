use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::CString,
    fmt,
    fs::File,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{ffi::OsStrExt, process::CommandExt},
    },
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::{
    error::WorkerError,
    failure_receipt::{
        RESIDUAL_CLEANUP_TREE, RESIDUAL_JOB_DIR, RESIDUAL_LEASE, RESIDUAL_SESSION,
        RESIDUAL_SUPERVISOR_LOCK, RESIDUAL_TRANSFER_LOCK, RESIDUAL_WORKSPACE,
    },
    inputs::RelativePath,
    job::{
        CancelRequest, ClientId, CommandSummary, JobId, JobMeta, JobStatus, LeaseAcquireRequest,
        LeaseRecord, LeaseToken, RequestFingerprint, RequestFingerprintMaterial,
        ResolveOrAbandonRequest, SubmitRequest,
    },
    rooted_fs::{PrivateEntryIdentity, RootedDir},
    task::{TaskId, TaskStatus},
};

pub use crate::gc::{
    BRANCH_RETENTION_MILLIS, GcCandidate, GcReport, GcRequest, HostGc, JOB_RETENTION_MILLIS,
    TASK_RETENTION_MILLIS,
};

const MAX_HOST_FILE_BYTES: u64 = 1024 * 1024;
pub const HOST_LAYOUT_VERSION: u32 = 3;
pub type StagingNonce = [u8; 16];
pub const PREVIOUS_HOST_LAYOUT_VERSION: u32 = 2;
/// FLOW rollback may restore a previous helper only while stored layout
/// still equals this value. After a successful 2→3 rewrite it is 3, so
/// rollback is refused (`HOST_UPGRADE_ROLLBACK_UNSAFE`). Never 3.
pub const ROLLBACK_HELPER_LAYOUT_VERSION: u32 = PREVIOUS_HOST_LAYOUT_VERSION;
const HOST_INSTALLATION_VERSION: u32 = 1;
const INSTALLATION_PREFIX: &str = ".mac-worker-installation-";
const INSTALLATION_IDENTITY_REFRESH_NAME: &str = ".mac-worker-anchor-refresh-identity.json";
const HOST_LAYOUT_FILE: &str = "layout.json";
const HOST_LAYOUT_REFRESH_NAME: &str = "layout.refresh.json";
const CAPACITY_LOCK_FILE: &str = "capacity.lock";
const ADMISSION_LOCK_FILE: &str = "admission.lock";
const SESSION_LOCK_FILE: &str = "session.lock";
const TRANSFER_DIRECTORY: &str = "transfer";
const TRANSFER_LOCK_FILE: &str = "transfer.lock";
const TRANSFER_IDENTITY_SUFFIX: &str = ".transfer-lock.json";
const SUPERVISOR_DIRECTORY: &str = "supervisor";
const SUPERVISOR_LOCK_FILE: &str = "supervisor.lock";
const SUPERVISOR_IDENTITY_SUFFIX: &str = ".supervisor-lock.json";
const OWNED_DIRECTORIES: &[&str] = &[
    "incoming",
    "verified",
    "jobs",
    "snapshots",
    "leases",
    "job-index",
    "locks",
    "repos",
    "tasks",
];

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostStoreWritePoint {
    AfterLeaseWrite = 1,
    AfterLeaseFileSync = 2,
    AfterLeaseDirectorySync = 3,
    AfterLeaseParentSync = 4,
    AfterLeasePublish = 5,
    AfterLeasePublishSync = 6,
    AfterHostRootCreate = 7,
    AfterHostNamespaces = 8,
    AfterHostCapacityLock = 9,
    AfterHostLayoutPublish = 10,
    BeforeInstallationLock = 11,
    AfterInstallationLock = 12,
    BeforeInstallationIdentityPublish = 13,
    AfterInstallationIdentityPublish = 14,
    AfterTransferDirectoryCreate = 15,
    AfterTransferLockSync = 16,
    AfterTransferIdentityPublish = 17,
    AfterTransferPublish = 18,
    AfterSnapshotValidation = 19,
    DuringSnapshotConversion = 20,
    AfterSnapshotConversion = 21,
    AfterSnapshotRename = 22,
    AfterSnapshotCacheSync = 23,
    AfterSnapshotRootSeal = 24,
    AfterSnapshotReceiptFileSync = 25,
    AfterSnapshotReceiptPublish = 26,
    AfterSnapshotReceiptParentSync = 27,
    AfterJobPublish = 28,
    AfterJobHomeSync = 29,
    AfterJobTmpSync = 30,
    AfterJobStdoutWrite = 31,
    AfterJobStdoutFileSync = 32,
    AfterJobStderrWrite = 33,
    AfterJobStderrFileSync = 34,
    AfterJobMetaWrite = 35,
    AfterJobMetaFileSync = 36,
    AfterJobStatusWrite = 37,
    AfterJobStatusFileSync = 38,
    AfterJobExecutionWrite = 39,
    AfterJobExecutionFileSync = 40,
    AfterJobStagingDirectorySync = 41,
    AfterJobRename = 42,
    AfterJobIndexFileSync = 43,
    AfterJobIndexRename = 44,
    AfterJobIndexParentSync = 45,
    AfterJobCleanupProof = 46,
    AfterJobLeaseRetirement = 47,
    BeforeJobStatusReplace = 48,
    BeforeJobLeaseRetirement = 49,
    AfterResolutionTombstone = 50,
    AfterResolutionExecutionRemoval = 51,
    AfterResolutionIncomingRemoval = 52,
    AfterResolutionVerifiedReceiptRemoval = 53,
    AfterResolutionVerificationStageRemoval = 54,
    AfterResolutionJobMutableRemoval = 55,
    AfterResolutionJobStageRemoval = 56,
    AfterResolutionAbsenceProof = 57,
    AfterResolutionCleanupMarker = 58,
    BeforeResolutionLeaseRelease = 59,
    AfterCleanupIntentCommit = 60,
    AfterHostLayoutRefreshUnlink = 61,
    AfterHostLayoutRefreshPublish = 62,
    AfterInstallationIdentityRefreshUnlink = 63,
    AfterInstallationIdentityRefreshPublish = 64,
    AfterOutboxPin = 65,
    AfterOutboxIntent = 66,
    AfterOutboxObjectBaselinePacks = 67,
    AfterOutboxObjectBaseline = 68,
    AfterOutboxUpdateRef = 69,
    AfterOutboxIntentPublish = 70,
}

impl LayoutEntry {
    fn new(path: String, identity: PrivateEntryIdentity) -> Self {
        Self {
            path,
            device: identity.device,
            inode: identity.inode,
            kind: identity.kind,
            owner: identity.owner,
            mode: identity.mode,
        }
    }

    fn as_private(&self) -> PrivateEntryIdentity {
        PrivateEntryIdentity {
            device: self.device,
            inode: self.inode,
            kind: self.kind,
            owner: self.owner,
            mode: self.mode,
        }
    }
}

impl HostLayoutIdentity {
    fn entry(&self, path: &str) -> Result<&LayoutEntry, WorkerError> {
        self.entries
            .iter()
            .find(|entry| entry.path == path)
            .ok_or_else(|| WorkerError::Protocol("canonical host layout entry is absent".into()))
    }
}

#[derive(Clone)]
pub struct HostStore {
    inner: Arc<HostStoreInner>,
}

struct HostStoreInner {
    display_root: PathBuf,
    installation_parent: RootedDir,
    installation_names: InstallationNames,
    installation: HostInstallationIdentity,
    installation_file_identity: PrivateEntryIdentity,
    root: RootedDir,
    namespaces: BTreeMap<&'static str, RootedDir>,
    root_identity: HostRootIdentity,
    layout: HostLayoutIdentity,
    fault: AtomicU8,
    secondary_fault: AtomicU8,
}

#[derive(Clone)]
struct InstallationNames {
    key: String,
    root_name: String,
    root_name_sha256: String,
    lock: String,
    identity: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HostRootIdentity {
    device: u64,
    inode: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct HostLayoutIdentity {
    version: u32,
    root: LayoutEntry,
    layout_file: LayoutEntry,
    entries: Vec<LayoutEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct HostInstallationIdentity {
    version: u32,
    key: String,
    root_name_sha256: String,
    parent: LayoutEntry,
    lock: LayoutEntry,
    root: LayoutEntry,
    layout: LayoutEntry,
    identity_file: LayoutEntry,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct LayoutEntry {
    path: String,
    device: u64,
    inode: u64,
    kind: u32,
    owner: u32,
    mode: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct AdmissionLockIdentity {
    version: u32,
    job_id: JobId,
    directory: LayoutEntry,
    lock: LayoutEntry,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct TransferLockIdentity {
    version: u32,
    job_id: JobId,
    directory: LayoutEntry,
    lock: LayoutEntry,
    identity_file: LayoutEntry,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SupervisorLockIdentity {
    version: u32,
    job_id: JobId,
    directory: LayoutEntry,
    lock: LayoutEntry,
    identity_file: LayoutEntry,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "disposition", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobDisposition {
    Accepted {
        job_id: JobId,
        client_id: ClientId,
        project_id: String,
        worktree_id: String,
        request_fingerprint: RequestFingerprint,
        status: JobStatus,
        recorded_at_millis: u64,
    },
    Abandoned {
        job_id: JobId,
        client_id: ClientId,
        project_id: String,
        worktree_id: String,
        request_fingerprint: RequestFingerprint,
        lease_token_sha256: String,
        recorded_at_millis: u64,
    },
}

impl JobDisposition {
    pub(crate) fn is_exact_abandonment_for(
        &self,
        material: &RequestFingerprintMaterial,
        request_fingerprint: &RequestFingerprint,
    ) -> bool {
        let token_hash = format!(
            "{:x}",
            Sha256::digest(material.lease_token().to_string().as_bytes())
        );
        matches!(
            self,
            Self::Abandoned {
                job_id,
                client_id,
                project_id,
                worktree_id,
                request_fingerprint: abandoned_fingerprint,
                lease_token_sha256,
                ..
            } if *job_id == material.job_id()
                && *client_id == material.client_id()
                && project_id == material.project_id()
                && worktree_id == material.worktree_id()
                && abandoned_fingerprint == request_fingerprint
                && lease_token_sha256 == &token_hash
        )
    }

    fn validate(&self) -> Result<(), WorkerError> {
        match self {
            Self::Accepted {
                project_id,
                worktree_id,
                status,
                ..
            } => {
                validate_digest(project_id, "accepted project ID")?;
                validate_digest(worktree_id, "accepted worktree ID")?;
                status.validate()?;
                if status.state() != crate::job::JobState::Accepted
                    || status.supervisor_identity().is_some()
                    || status.child_identity().is_some()
                {
                    return Err(protocol_code(
                        "JOB_ID_CONFLICT",
                        "accepted disposition requires the initial accepted status",
                    ));
                }
                Ok(())
            }
            Self::Abandoned {
                project_id,
                worktree_id,
                lease_token_sha256,
                ..
            } => {
                validate_digest(project_id, "abandoned project ID")?;
                validate_digest(worktree_id, "abandoned worktree ID")?;
                validate_digest(lease_token_sha256, "lease token hash")
            }
        }
    }
}

pub struct AdmissionGuard {
    outer: Arc<InstallationGuard>,
    file: File,
    namespace: RootedDir,
    file_name: String,
    file_identity: PrivateEntryIdentity,
    scope: GuardScope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GuardScope {
    Job(JobId),
    Capacity,
}

pub struct TransferGuard {
    file: File,
    namespace: RootedDir,
    file_name: String,
    file_identity: PrivateEntryIdentity,
    anchor_namespace: RootedDir,
    identity_file: File,
    identity_file_name: String,
    identity_file_identity: PrivateEntryIdentity,
    identity: TransferLockIdentity,
}

pub struct SupervisorGuard {
    file: File,
    namespace: RootedDir,
    file_name: String,
    file_identity: PrivateEntryIdentity,
    anchor_namespace: RootedDir,
    identity_file: File,
    identity_file_name: String,
    identity_file_identity: PrivateEntryIdentity,
    identity: SupervisorLockIdentity,
}

// Every host mutation follows this fixed order: the parent-anchored stable
// installation lock, then the per-job admission lock, then (as applicable)
// the transfer lock, supervisor lock, or heavy-slot capacity lock. A detached
// supervisor drops admission before long-running work and drops its supervisor
// capability before cleanup reacquires admission -> capacity. Capacity-only
// integrity operations start at the installation anchor as well.
struct InstallationGuard {
    file: File,
    parent: RootedDir,
    lock_name: String,
    lock_identity: PrivateEntryIdentity,
    identity_name: String,
    identity_file: File,
    identity_file_identity: PrivateEntryIdentity,
    installation: HostInstallationIdentity,
    root: RootedDir,
    layout_file: File,
}

impl AdmissionGuard {
    pub(crate) fn validate(&self) -> Result<(), WorkerError> {
        self.outer.validate()?;
        self.namespace.validate_private_regular_binding(
            &self.file_name,
            &self.file,
            self.file_identity,
        )?;
        Ok(())
    }

    pub(crate) fn validate_for(&self, job: JobId) -> Result<(), WorkerError> {
        self.validate()?;
        if self.scope != GuardScope::Job(job) {
            return Err(WorkerError::Protocol(
                "admission guard does not match the requested job".into(),
            ));
        }
        Ok(())
    }
}

impl TransferGuard {
    pub(crate) fn job_id(&self) -> JobId {
        self.identity.job_id
    }

    pub(crate) fn validate(&self) -> Result<(), WorkerError> {
        self.anchor_namespace.verify_bound()?;
        self.anchor_namespace.validate_private_regular_binding(
            &self.identity_file_name,
            &self.identity_file,
            self.identity_file_identity,
        )?;
        let stored: TransferLockIdentity =
            read_json_strict_at(&self.anchor_namespace, &self.identity_file_name)?;
        if stored != self.identity {
            return Err(WorkerError::Protocol(
                "canonical transfer lock identity changed".into(),
            ));
        }
        let current_namespace = self.anchor_namespace.open_child_directory(
            &relative(&format!("{}/{TRANSFER_DIRECTORY}", self.identity.job_id))?,
            false,
        )?;
        let current = build_transfer_lock_identity(
            &current_namespace,
            self.identity.job_id,
            current_namespace.private_entry_identity(TRANSFER_LOCK_FILE)?,
            self.identity_file_identity,
            &self.identity_file_name,
        )?;
        if current != self.identity {
            return Err(WorkerError::Protocol(
                "canonical transfer lock identity changed".into(),
            ));
        }
        self.namespace.verify_bound()?;
        self.namespace.validate_private_regular_binding(
            &self.file_name,
            &self.file,
            self.file_identity,
        )?;
        Ok(())
    }

    pub(crate) fn raw_lock_fd_for_exec(&self) -> Result<std::os::fd::RawFd, WorkerError> {
        self.validate()?;
        self.namespace.verify_descriptors_cloexec()?;
        self.anchor_namespace.verify_descriptors_cloexec()?;
        require_cloexec(self.identity_file.as_raw_fd())?;
        require_cloexec(self.file.as_raw_fd())?;
        Ok(self.file.as_raw_fd())
    }
}

impl SupervisorGuard {
    pub(crate) fn validate(&self) -> Result<(), WorkerError> {
        self.anchor_namespace.verify_bound()?;
        self.anchor_namespace.validate_private_regular_binding(
            &self.identity_file_name,
            &self.identity_file,
            self.identity_file_identity,
        )?;
        let stored: SupervisorLockIdentity =
            read_json_strict_at(&self.anchor_namespace, &self.identity_file_name)?;
        if stored != self.identity {
            return Err(WorkerError::Protocol(
                "canonical supervisor lock identity changed".into(),
            ));
        }
        let current_namespace = self.anchor_namespace.open_child_directory(
            &relative(&format!("{}/{SUPERVISOR_DIRECTORY}", self.identity.job_id))?,
            false,
        )?;
        let current = build_supervisor_lock_identity(
            &current_namespace,
            self.identity.job_id,
            current_namespace.private_entry_identity(SUPERVISOR_LOCK_FILE)?,
            self.identity_file_identity,
            &self.identity_file_name,
        )?;
        if current != self.identity {
            return Err(WorkerError::Protocol(
                "canonical supervisor lock identity changed".into(),
            ));
        }
        self.namespace.verify_bound()?;
        self.namespace.validate_private_regular_binding(
            &self.file_name,
            &self.file,
            self.file_identity,
        )?;
        Ok(())
    }

    pub(crate) fn raw_lock_fd(&self) -> Result<std::os::fd::RawFd, WorkerError> {
        self.validate()?;
        require_cloexec(self.file.as_raw_fd())?;
        Ok(self.file.as_raw_fd())
    }

    pub(crate) fn job_id(&self) -> JobId {
        self.identity.job_id
    }
}

impl fmt::Debug for TransferGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransferGuard")
            .field("job_id", &self.identity.job_id)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for SupervisorGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SupervisorGuard")
            .field("job_id", &self.identity.job_id)
            .finish_non_exhaustive()
    }
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

impl InstallationGuard {
    fn acquire(store: &HostStoreInner) -> Result<Self, WorkerError> {
        let file = store
            .installation_parent
            .open_existing_private_lock(&store.installation_names.lock)?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if result != 0 {
            return Err(WorkerError::Io(std::io::Error::last_os_error()));
        }
        let identity_file = store
            .installation_parent
            .open_private_regular_handle(&store.installation_names.identity)?;
        let layout_file = store.root.open_private_regular_handle(HOST_LAYOUT_FILE)?;
        let guard = Self {
            file,
            parent: store.installation_parent.reopen()?,
            lock_name: store.installation_names.lock.clone(),
            lock_identity: store.installation.lock.as_private(),
            identity_name: store.installation_names.identity.clone(),
            identity_file,
            identity_file_identity: store.installation_file_identity,
            installation: store.installation.clone(),
            root: store.root.reopen()?,
            layout_file,
        };
        guard.validate()?;
        Ok(guard)
    }

    fn validate(&self) -> Result<(), WorkerError> {
        self.parent.verify_bound()?;
        self.parent.validate_private_regular_binding(
            &self.lock_name,
            &self.file,
            self.lock_identity,
        )?;
        self.parent.validate_private_regular_binding(
            &self.identity_name,
            &self.identity_file,
            self.identity_file_identity,
        )?;
        self.root.verify_bound()?;
        self.root.validate_private_regular_binding(
            HOST_LAYOUT_FILE,
            &self.layout_file,
            self.installation.layout.as_private(),
        )?;
        Ok(())
    }
}

impl Drop for InstallationGuard {
    fn drop(&mut self) {
        unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

pub(crate) struct SessionGuard {
    file: File,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

#[allow(dead_code)] // Task 7 consumes final publication fields and the opaque receipt.
pub struct StagedJob {
    root: RootedDir,
    final_parent: RootedDir,
    final_name: String,
    job_id: JobId,
    project_id: String,
    worktree_id: String,
    snapshot_digest: Option<String>,
    receipt_nonce: [u8; 16],
    guard: AdmissionGuard,
}

pub(crate) trait PublicationReceipt {
    fn validate_for(&self, staged: &StagedJob) -> Result<(), WorkerError>;
}

#[allow(dead_code)] // Task 7 consumes the snapshot-bound opaque receipt.
pub struct WorkspaceReceipt {
    job_id: JobId,
    nonce: [u8; 16],
    project_id: String,
    worktree_id: String,
    manifest_digest: String,
}

pub struct PublishedJob {
    root: RootedDir,
    guard: AdmissionGuard,
    job_id: JobId,
    project_id: String,
    worktree_id: String,
}

pub struct CleanupReceipt {
    root_identity: HostRootIdentity,
    job_id: JobId,
    client_id: ClientId,
    lease_token: LeaseToken,
    proof: CleanupProof,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ResolutionIdentity {
    job_id: JobId,
    client_id: ClientId,
    lease_token: LeaseToken,
    created_at_millis: u64,
    request_fingerprint: RequestFingerprint,
    worker_name: String,
    project_id: String,
    worktree_id: String,
    manifest_digest: String,
    relative_working_dir: String,
    timeout_millis: u64,
    resource_class: String,
    command_summary: CommandSummary,
}

impl fmt::Debug for ResolutionIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolutionIdentity")
            .field("job_id", &self.job_id)
            .field("client_id", &self.client_id)
            .field("created_at_millis", &self.created_at_millis)
            .field("request_fingerprint", &self.request_fingerprint)
            .finish_non_exhaustive()
    }
}

impl ResolutionIdentity {
    pub(crate) fn from_request(request: &ResolveOrAbandonRequest) -> Result<Self, WorkerError> {
        request.validate()?;
        Ok(Self {
            job_id: request.job_id(),
            client_id: request.client_id(),
            lease_token: request.lease_token(),
            created_at_millis: request.created_at_millis(),
            request_fingerprint: request.request_fingerprint().clone(),
            worker_name: request.worker_name().into(),
            project_id: request.project_id().into(),
            worktree_id: request.worktree_id().into(),
            manifest_digest: request.manifest_digest().into(),
            relative_working_dir: request.relative_working_dir().into(),
            timeout_millis: request.timeout_millis(),
            resource_class: request.resource_class().into(),
            command_summary: request.command_summary().clone(),
        })
    }

    pub(crate) fn job_id(&self) -> JobId {
        self.job_id
    }
    pub(crate) fn client_id(&self) -> ClientId {
        self.client_id
    }
    pub(crate) fn lease_token(&self) -> LeaseToken {
        self.lease_token
    }
    pub(crate) fn created_at_millis(&self) -> u64 {
        self.created_at_millis
    }
    pub(crate) fn request_fingerprint(&self) -> &RequestFingerprint {
        &self.request_fingerprint
    }
    pub(crate) fn worker_name(&self) -> &str {
        &self.worker_name
    }
    pub(crate) fn project_id(&self) -> &str {
        &self.project_id
    }
    pub(crate) fn worktree_id(&self) -> &str {
        &self.worktree_id
    }
    pub(crate) fn manifest_digest(&self) -> &str {
        &self.manifest_digest
    }
    pub(crate) fn relative_working_dir(&self) -> &str {
        &self.relative_working_dir
    }
    pub(crate) fn timeout_millis(&self) -> u64 {
        self.timeout_millis
    }
    pub(crate) fn resource_class(&self) -> &str {
        &self.resource_class
    }
    pub(crate) fn command_summary(&self) -> &CommandSummary {
        &self.command_summary
    }

    pub(crate) fn token_hash(&self) -> String {
        format!(
            "{:x}",
            Sha256::digest(self.lease_token.to_string().as_bytes())
        )
    }

    pub(crate) fn matches_lease(&self, lease: &LeaseRecord) -> bool {
        lease.job_id() == self.job_id
            && lease.client_id() == self.client_id
            && lease.lease_token() == self.lease_token
            && lease.request_fingerprint() == &self.request_fingerprint
            && lease.worker_name() == self.worker_name
            && lease.project_id() == self.project_id
            && lease.worktree_id() == self.worktree_id
            && lease.manifest_digest() == self.manifest_digest
            && lease.timeout_millis() == self.timeout_millis
            && lease.resource_class() == self.resource_class
            && lease.command_summary() == &self.command_summary
    }

    pub(crate) fn matches_disposition(&self, disposition: &JobDisposition) -> bool {
        match disposition {
            JobDisposition::Accepted {
                job_id,
                client_id,
                project_id,
                worktree_id,
                request_fingerprint,
                ..
            } => {
                *job_id == self.job_id
                    && *client_id == self.client_id
                    && project_id == &self.project_id
                    && worktree_id == &self.worktree_id
                    && request_fingerprint == &self.request_fingerprint
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
                *job_id == self.job_id
                    && *client_id == self.client_id
                    && project_id == &self.project_id
                    && worktree_id == &self.worktree_id
                    && request_fingerprint == &self.request_fingerprint
                    && lease_token_sha256 == &self.token_hash()
            }
        }
    }
}

#[derive(Debug, Clone)]
enum CleanupProof {
    Terminal,
    Abandoned,
}

impl fmt::Debug for StagedJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StagedJob")
            .field("job_id", &self.job_id)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for WorkspaceReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceReceipt")
            .field("job_id", &self.job_id)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for PublishedJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PublishedJob")
            .field("job_id", &self.job_id)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for CleanupReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CleanupReceipt")
            .field("job_id", &self.job_id)
            .field("client_id", &self.client_id)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CleanupMarker {
    job_id: JobId,
    client_id: ClientId,
    project_id: String,
    worktree_id: String,
    manifest_digest: String,
    request_fingerprint: RequestFingerprint,
    lease_token_sha256: String,
    terminal_or_abandoned: bool,
}

enum UpgradeFenceOp {
    Promote {
        from: PathBuf,
        to: PathBuf,
    },
    Rollback {
        previous: Option<PathBuf>,
        to: PathBuf,
    },
}

#[cfg(test)]
thread_local! {
    static UPGRADE_FENCE_HOLD: std::cell::RefCell<Option<std::sync::Arc<std::sync::Barrier>>> =
        const { std::cell::RefCell::new(None) };
    static PUBLISHED_LAYOUT_VERSION_OVERRIDE: std::cell::Cell<Option<u32>> =
        const { std::cell::Cell::new(None) };
}

fn published_layout_version() -> u32 {
    #[cfg(test)]
    {
        if let Some(version) = PUBLISHED_LAYOUT_VERSION_OVERRIDE.with(|slot| slot.get()) {
            return version;
        }
    }
    HOST_LAYOUT_VERSION
}

impl HostStore {
    pub fn open(root: &Path) -> Result<Self, WorkerError> {
        Self::open_inner(root, None, true, false, None)?
            .ok_or_else(|| WorkerError::Protocol("host installation was not initialized".into()))
    }

    pub(crate) fn host_state_root(&self) -> &Path {
        &self.inner.display_root
    }

    pub(crate) fn open_if_present(root: &Path) -> Result<Option<Self>, WorkerError> {
        Self::open_inner(root, None, false, false, None)
    }

    /// Idle / facts-refresh entry: takes the construction installation lock,
    /// inspects drain, promotes slot directories, then refreshes `layout.json`
    /// and the parent installation identity (new inodes) under that same lock.
    /// FLOW's `complete_protocol_upgrade` already holds the lock and must call
    /// [`crate::lease::promote_slot_directories`] after inspect and binary
    /// rename instead of this function (a second flock fd deadlocks), then the
    /// same layout/installation refresh. In-place rewrite of `layout.json` is
    /// not sufficient: a handle opened before promotion would still validate.
    pub fn migrate_layout(root: &Path) -> Result<(), WorkerError> {
        let _ = Self::open_inner(root, None, false, true, None)?;
        Ok(())
    }

    /// Inspect layout-2 live work, replace `promote_from` with `promote_to`, and
    /// rewrite layout while holding the existing `open_inner` construction lock.
    /// Does not call `migrate_layout` (no second flock).
    pub fn complete_protocol_upgrade(
        root: &Path,
        promote_from: &Path,
        promote_to: &Path,
    ) -> Result<(), WorkerError> {
        let _ = Self::open_inner(
            root,
            None,
            true,
            true,
            Some(UpgradeFenceOp::Promote {
                from: promote_from.to_path_buf(),
                to: promote_to.to_path_buf(),
            }),
        )?;
        Ok(())
    }

    /// Restore `previous` onto `promote_to` only if inventory is drain-clean and
    /// the layout the previous helper would `open()` equals
    /// [`ROLLBACK_HELPER_LAYOUT_VERSION`]. On first initialization that is the
    /// layout that would be published; on an existing root it is `layout.json`.
    /// Incompatible initialize keeps the promoted helper and still first-publishes
    /// current layout under it. Never rewrites an existing layout.
    pub fn complete_unverified_rollback(
        root: &Path,
        previous: Option<&Path>,
        promote_to: &Path,
    ) -> Result<(), WorkerError> {
        let _ = Self::open_inner(
            root,
            None,
            true,
            false,
            Some(UpgradeFenceOp::Rollback {
                previous: previous.map(Path::to_path_buf),
                to: promote_to.to_path_buf(),
            }),
        )?;
        Ok(())
    }

    /// Two-phase barrier for tests: the fenced thread waits once after the
    /// construction lock is held and before helper rename/restore, then waits
    /// again until the test releases the critical section.
    #[cfg(test)]
    #[doc(hidden)]
    pub fn with_upgrade_fence_hold<T>(
        barrier: std::sync::Arc<std::sync::Barrier>,
        op: impl FnOnce() -> T,
    ) -> T {
        UPGRADE_FENCE_HOLD.with(|slot| *slot.borrow_mut() = Some(barrier));
        let result = op();
        UPGRADE_FENCE_HOLD.with(|slot| *slot.borrow_mut() = None);
        result
    }

    /// Combined SCHED fixture: pretend first-publish would write this layout
    /// version. Production uses `HOST_LAYOUT_VERSION`.
    #[cfg(test)]
    #[doc(hidden)]
    pub fn with_published_layout_version<T>(version: u32, op: impl FnOnce() -> T) -> T {
        PUBLISHED_LAYOUT_VERSION_OVERRIDE.with(|slot| slot.set(Some(version)));
        let result = op();
        PUBLISHED_LAYOUT_VERSION_OVERRIDE.with(|slot| slot.set(None));
        result
    }

    #[doc(hidden)]
    pub fn migrate_layout_with_write_fault(
        root: &Path,
        point: HostStoreWritePoint,
    ) -> Result<(), WorkerError> {
        let _ = Self::open_inner(root, Some(point), false, true, None)?;
        Ok(())
    }

    #[doc(hidden)]
    pub fn open_with_write_fault(
        root: &Path,
        point: HostStoreWritePoint,
    ) -> Result<Self, WorkerError> {
        Self::open_inner(root, Some(point), true, false, None)?
            .ok_or_else(|| WorkerError::Protocol("host installation was not initialized".into()))
    }

    #[doc(hidden)]
    pub fn open_with_write_faults(
        root: &Path,
        first: HostStoreWritePoint,
        second: HostStoreWritePoint,
    ) -> Result<Self, WorkerError> {
        let store = Self::open_with_write_fault(root, first)?;
        store
            .inner
            .secondary_fault
            .store(second as u8, Ordering::SeqCst);
        Ok(store)
    }

    fn open_inner(
        root: &Path,
        point: Option<HostStoreWritePoint>,
        create: bool,
        migrate: bool,
        fence: Option<UpgradeFenceOp>,
    ) -> Result<Option<Self>, WorkerError> {
        if !root.is_absolute() {
            return Err(WorkerError::Protocol(
                "host data root must be absolute".into(),
            ));
        }
        let parent_path = root.parent().ok_or_else(|| {
            WorkerError::Protocol("host data root has no installation parent".into())
        })?;
        let parent = if create {
            RootedDir::open_or_create_anchored_absolute(parent_path)?
        } else {
            match RootedDir::open_anchored_absolute(parent_path) {
                Ok(parent) => parent,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error.into()),
            }
        };
        let names = installation_names(&parent, root)?;
        let root_present = parent.entry_exists(&names.root_name)?;
        let mut lock_present = parent.entry_exists(&names.lock)?;
        let mut identity_present = parent.entry_exists(&names.identity)?;
        let mut reanchored = false;
        if root_present && !lock_present && !identity_present {
            reanchored = reanchor_legacy_installation(&parent, &names)?;
            if reanchored {
                lock_present = parent.entry_exists(&names.lock)?;
                identity_present = parent.entry_exists(&names.identity)?;
            }
        }
        if !root_present && !lock_present && !identity_present && !create {
            return Ok(None);
        }
        if (root_present || identity_present) && !lock_present {
            return Err(WorkerError::Protocol(
                "host installation lock is absent".into(),
            ));
        }
        let genuine_first = !root_present && !lock_present && !identity_present;
        if genuine_first {
            inject_open_fault(point, HostStoreWritePoint::BeforeInstallationLock)?;
        }
        let (installation_lock, created_lock) = if genuine_first {
            parent.open_private_lock_with_created(&names.lock)?
        } else {
            (parent.open_existing_private_lock(&names.lock)?, false)
        };
        if created_lock {
            parent.sync_root()?;
        }
        let result = unsafe { libc::flock(installation_lock.as_raw_fd(), libc::LOCK_EX) };
        if result != 0 {
            return Err(WorkerError::Io(std::io::Error::last_os_error()));
        }
        let mut installation_lock = Some(installation_lock);
        let mut root_present = parent.entry_exists(&names.root_name)?;
        let mut identity_present = parent.entry_exists(&names.identity)?;
        let initialize = genuine_first && !root_present && !identity_present;
        if !initialize && (!root_present || !identity_present) {
            drop(installation_lock.take());
            for _ in 0..64 {
                std::thread::yield_now();
                let retry = parent.open_existing_private_lock(&names.lock)?;
                let result = unsafe { libc::flock(retry.as_raw_fd(), libc::LOCK_EX) };
                if result != 0 {
                    return Err(WorkerError::Io(std::io::Error::last_os_error()));
                }
                root_present = parent.entry_exists(&names.root_name)?;
                identity_present = parent.entry_exists(&names.identity)?;
                installation_lock = Some(retry);
                if root_present && identity_present {
                    break;
                }
                drop(installation_lock.take());
            }
        }
        if !initialize && !root_present {
            return Err(WorkerError::Protocol(
                "host installation is incomplete after coordination".into(),
            ));
        }
        if !initialize && !identity_present {
            let refresh_residue = parent.entry_exists(INSTALLATION_IDENTITY_REFRESH_NAME)?;
            let legacy_identity = has_installation_identity_file(&parent)?;
            if !refresh_residue && !legacy_identity {
                return Err(WorkerError::Protocol(
                    "host installation is incomplete after coordination".into(),
                ));
            }
        }
        if installation_lock.is_none() {
            let retry = parent.open_existing_private_lock(&names.lock)?;
            let result = unsafe { libc::flock(retry.as_raw_fd(), libc::LOCK_EX) };
            if result != 0 {
                return Err(WorkerError::Io(std::io::Error::last_os_error()));
            }
            installation_lock = Some(retry);
        }
        let installation_lock = installation_lock.ok_or_else(|| {
            WorkerError::Protocol("host installation coordination was lost".into())
        })?;
        let lock_identity = parent.private_entry_identity(&names.lock)?;
        parent.validate_private_regular_binding(&names.lock, &installation_lock, lock_identity)?;
        if initialize {
            inject_open_fault(point, HostStoreWritePoint::AfterInstallationLock)?;
        }
        let mut rooted = if initialize {
            parent.create_new_child_directory(&names.root_name)?
        } else {
            let parent_device = parent.root_metadata()?.st_dev as u64;
            parent.open_private_direct_child_on_device(&names.root_name, parent_device)?
        };
        let root_metadata = rooted.root_metadata()?;
        require_private_directory_metadata(&root_metadata)?;
        let root_device = root_metadata.st_dev as u64;
        let parent_device = parent.root_metadata()?.st_dev as u64;
        if root_device != parent_device {
            return Err(WorkerError::Protocol(
                "host installation spans filesystems".into(),
            ));
        }
        rooted.bind_host_device(root_device)?;
        let root_identity = HostRootIdentity {
            device: root_device,
            inode: root_metadata.st_ino,
        };
        if initialize {
            inject_open_fault(point, HostStoreWritePoint::AfterHostRootCreate)?;
        }
        let layout_present = rooted.entry_exists(HOST_LAYOUT_FILE)?;
        let layout_refresh_present = rooted.entry_exists(HOST_LAYOUT_REFRESH_NAME)?;
        let initialized_layout = layout_present || layout_refresh_present;
        let stored_layout = if layout_present {
            Some(read_json_strict_at::<HostLayoutIdentity>(
                &rooted,
                HOST_LAYOUT_FILE,
            )?)
        } else if layout_refresh_present {
            Some(read_json_strict_at::<HostLayoutIdentity>(
                &rooted,
                HOST_LAYOUT_REFRESH_NAME,
            )?)
        } else {
            None
        };
        let needs_migration = stored_layout
            .as_ref()
            .is_some_and(|stored| stored.version != HOST_LAYOUT_VERSION);
        let allow_outdated = migrate || fence.is_some();
        if needs_migration && !allow_outdated {
            return Err(host_layout_outdated());
        }
        if needs_migration
            && stored_layout
                .as_ref()
                .is_some_and(|stored| stored.version != PREVIOUS_HOST_LAYOUT_VERSION)
            && !matches!(fence, Some(UpgradeFenceOp::Rollback { .. }))
        {
            return Err(host_layout_outdated());
        }
        if initialize == initialized_layout {
            return Err(WorkerError::Protocol(
                "host root and layout initialization state disagree".into(),
            ));
        }
        let namespaces = open_host_namespaces(&rooted, root_device, initialize || needs_migration)?;
        inject_open_fault(point, HostStoreWritePoint::AfterHostNamespaces)?;
        if (migrate || fence.is_some()) && !initialize {
            match inspect_protocol_upgrade_namespaces(&namespaces) {
                Ok(()) => {}
                Err(_) if matches!(fence, Some(UpgradeFenceOp::Rollback { .. })) => {
                    drop(installation_lock);
                    return Err(upgrade_rollback_unsafe(
                        "host work remains; the promoted helper is kept",
                    ));
                }
                Err(error) => {
                    drop(installation_lock);
                    return Err(error);
                }
            }
        }
        if fence.is_some() {
            wait_upgrade_fence_hold();
        }
        let mut refuse_rollback_keep_promoted = false;
        if let Some(UpgradeFenceOp::Rollback { previous, to }) = &fence {
            let compatible = if initialize {
                published_layout_version() == ROLLBACK_HELPER_LAYOUT_VERSION
            } else {
                stored_layout.as_ref().map(|stored| stored.version)
                    == Some(ROLLBACK_HELPER_LAYOUT_VERSION)
            };
            if !compatible {
                if initialize {
                    refuse_rollback_keep_promoted = true;
                } else {
                    drop(installation_lock);
                    return Err(upgrade_rollback_unsafe(
                        "layout version is not proven compatible with the previous helper; the promoted helper is kept",
                    ));
                }
            } else if initialize {
                restore_helper_binary(previous.as_deref(), to)?;
            } else {
                restore_helper_binary(previous.as_deref(), to)?;
                drop(installation_lock);
                return Ok(None);
            }
        }
        if let Some(UpgradeFenceOp::Promote { from, to }) = &fence {
            promote_helper_binary(from, to)?;
        }
        let leases = namespaces
            .get("leases")
            .expect("owned leases namespace was inserted");
        if initialize {
            let existed = leases.entry_exists(CAPACITY_LOCK_FILE)?;
            drop(leases.open_private_lock(CAPACITY_LOCK_FILE)?);
            if !existed {
                leases.sync_root()?;
            }
            crate::lease::promote_slot_directories(leases)?;
            inject_open_fault(point, HostStoreWritePoint::AfterHostCapacityLock)?;
        }
        if !initialize && !parent.entry_exists(&names.identity)? {
            complete_legacy_identity_reanchor(&parent, &names, lock_identity)?;
        }
        let promote = matches!(fence, Some(UpgradeFenceOp::Promote { .. }));
        let mut layout_refreshed = layout_refresh_present;
        let layout = if initialize {
            let stored = publish_host_layout(&rooted, &namespaces)?;
            inject_open_fault(point, HostStoreWritePoint::AfterHostLayoutPublish)?;
            stored
        } else {
            let stored = stored_layout.expect("initialized layout was read above");
            let current_layout = if layout_present {
                let layout_file_identity = rooted.private_entry_identity(HOST_LAYOUT_FILE)?;
                build_host_layout(&rooted, &namespaces, layout_file_identity)?
            } else {
                stored.clone()
            };
            if needs_migration {
                refuse_layout3_promotion(&namespaces)?;
                let leases = namespaces
                    .get("leases")
                    .expect("owned leases namespace was inserted");
                crate::lease::promote_slot_directories(leases)?;
                // Same construction lock as FLOW's complete_protocol_upgrade:
                // unlink→layout.refresh.json→publish (new inode), then
                // restore_or_refresh_installation_identity(..., layout_refreshed).
                // In-place rewrite leaves the pre-migration handle valid.
                layout_refreshed = true;
                refresh_host_layout(&rooted, &namespaces, point)?
            } else if promote
                || !layout_present
                || layout_is_volume_remount(&stored, &current_layout)
            {
                layout_refreshed = true;
                refresh_host_layout(&rooted, &namespaces, point)?
            } else {
                validate_host_layout(&stored, &current_layout)?;
                stored
            }
        };
        let layout_file_identity = layout.layout_file.as_private();
        let (installation, installation_file_identity) = if initialize {
            inject_open_fault(
                point,
                HostStoreWritePoint::BeforeInstallationIdentityPublish,
            )?;
            let published = publish_installation_identity(
                &parent,
                &names,
                lock_identity,
                &rooted,
                layout_file_identity,
            )?;
            inject_open_fault(point, HostStoreWritePoint::AfterInstallationIdentityPublish)?;
            published
        } else {
            restore_or_refresh_installation_identity(
                &parent,
                &names,
                lock_identity,
                &rooted,
                layout_file_identity,
                reanchored || layout_refreshed,
                point,
            )?
        };
        // Keep layout.refresh.json until installation identity matches the new
        // layout inode, so a crash between those publications remains repairable
        // without truncating the only authoritative layout.
        remove_private_if_present(&rooted, HOST_LAYOUT_REFRESH_NAME)?;
        parent.validate_private_regular_binding(
            &names.lock,
            &installation_lock,
            installation.lock.as_private(),
        )?;
        for bytes in rooted.list_names()? {
            let name = std::str::from_utf8(&bytes).map_err(|_| {
                WorkerError::Protocol("host data root contains a non-UTF-8 entry".into())
            })?;
            if !OWNED_DIRECTORIES.contains(&name)
                && name != HOST_LAYOUT_FILE
                && name != HOST_LAYOUT_REFRESH_NAME
            {
                rooted.validate_private_entry(name)?;
            }
        }
        let store = Self {
            inner: Arc::new(HostStoreInner {
                display_root: root.to_path_buf(),
                installation_parent: parent,
                installation_names: names,
                installation,
                installation_file_identity,
                root: rooted,
                namespaces,
                root_identity,
                layout,
                fault: AtomicU8::new(point.map_or(0, |point| point as u8)),
                secondary_fault: AtomicU8::new(0),
            }),
        };
        drop(installation_lock);
        store.validate_layout()?;
        if refuse_rollback_keep_promoted {
            return Err(upgrade_rollback_unsafe(
                "current layout is not proven compatible with the previous helper; the promoted helper is kept",
            ));
        }
        Ok(Some(store))
    }

    #[doc(hidden)]
    pub fn admission_lock(&self, job: JobId) -> Result<AdmissionGuard, WorkerError> {
        let outer = Arc::new(self.installation_lock()?);
        self.validate_layout_locked(&outer)?;
        let jobs = self.open_directory("locks/jobs", false)?;
        let identity_name = format!("{job}.lock.json");
        let initialized = jobs.entry_exists(&identity_name)?;
        let directory = self.open_directory(&format!("locks/jobs/{job}"), !initialized)?;
        if !initialized && !directory.entry_exists(ADMISSION_LOCK_FILE)? {
            drop(directory.open_private_lock(ADMISSION_LOCK_FILE)?);
            directory.sync_root()?;
        }
        let file = directory.open_private_lock(ADMISSION_LOCK_FILE)?;
        let current = AdmissionLockIdentity {
            version: HOST_LAYOUT_VERSION,
            job_id: job,
            directory: LayoutEntry::new(job.to_string(), directory.identity()?),
            lock: LayoutEntry::new(
                format!("{job}/{ADMISSION_LOCK_FILE}"),
                directory.private_entry_identity(ADMISSION_LOCK_FILE)?,
            ),
        };
        let identity = if initialized {
            let stored: AdmissionLockIdentity = read_json_strict_at(&jobs, &identity_name)?;
            if stored != current {
                return Err(WorkerError::Protocol(
                    "canonical admission lock identity changed".into(),
                ));
            }
            stored
        } else {
            atomic_write_at(&jobs, &identity_name, &current)?;
            current
        };
        outer.validate()?;
        directory.validate_private_regular_binding(
            ADMISSION_LOCK_FILE,
            &file,
            identity.lock.as_private(),
        )?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if result != 0 {
            return Err(WorkerError::Io(std::io::Error::last_os_error()));
        }
        let guard = AdmissionGuard {
            outer,
            file,
            namespace: directory,
            file_name: ADMISSION_LOCK_FILE.into(),
            file_identity: identity.lock.as_private(),
            scope: GuardScope::Job(job),
        };
        guard.validate()?;
        Ok(guard)
    }

    #[allow(dead_code)] // Standalone capacity locking is exercised by host integrity tests.
    pub(crate) fn capacity_lock(&self) -> Result<AdmissionGuard, WorkerError> {
        // Installation then capacity. Callers that also need session.lock
        // must acquire this first: session then capacity deadlocks with GC
        // (installation held, then session) and with this order.
        let outer = Arc::new(self.installation_lock()?);
        self.capacity_lock_with_outer(outer)
    }

    pub(crate) fn capacity_lock_after(
        &self,
        admission: &AdmissionGuard,
    ) -> Result<AdmissionGuard, WorkerError> {
        admission.validate()?;
        self.capacity_lock_with_outer(Arc::clone(&admission.outer))
    }

    fn capacity_lock_with_outer(
        &self,
        outer: Arc<InstallationGuard>,
    ) -> Result<AdmissionGuard, WorkerError> {
        self.validate_layout_locked(&outer)?;
        let leases = self.open_directory("leases", false)?;
        let expected = self
            .inner
            .layout
            .entry("leases/capacity.lock")?
            .as_private();
        let file = leases.open_private_lock(CAPACITY_LOCK_FILE)?;
        leases.validate_private_regular_binding(CAPACITY_LOCK_FILE, &file, expected)?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if result != 0 {
            return Err(WorkerError::Io(std::io::Error::last_os_error()));
        }
        let guard = AdmissionGuard {
            outer,
            file,
            namespace: leases,
            file_name: CAPACITY_LOCK_FILE.into(),
            file_identity: expected,
            scope: GuardScope::Capacity,
        };
        guard.validate()?;
        Ok(guard)
    }

    #[doc(hidden)]
    pub fn transfer_lock_after(
        &self,
        admission: &AdmissionGuard,
        job: JobId,
    ) -> Result<TransferGuard, WorkerError> {
        admission.validate()?;
        if admission.scope != GuardScope::Job(job) {
            return Err(WorkerError::Protocol(
                "transfer lock requires the matching job admission guard".into(),
            ));
        }
        let transfer_identity_name = format!("{job}{TRANSFER_IDENTITY_SUFFIX}");
        let identity_exists = self
            .open_directory("locks/jobs", false)?
            .entry_exists(&transfer_identity_name)?;
        let job_locks = admission.namespace.reopen()?;
        let jobs = self.open_directory("locks/jobs", false)?;
        let (identity, identity_file_identity, identity_file) = if identity_exists {
            let stored: TransferLockIdentity = read_json_strict_at(&jobs, &transfer_identity_name)?;
            let identity_file_identity = jobs.private_entry_identity(&transfer_identity_name)?;
            let identity_file = jobs.open_private_regular_handle(&transfer_identity_name)?;
            jobs.validate_private_regular_binding(
                &transfer_identity_name,
                &identity_file,
                stored.identity_file.as_private(),
            )?;
            if stored.version != HOST_LAYOUT_VERSION
                || stored.job_id != job
                || stored.identity_file
                    != LayoutEntry::new(transfer_identity_name.clone(), identity_file_identity)
            {
                return Err(WorkerError::Protocol(
                    "canonical transfer lock identity changed".into(),
                ));
            }
            (stored, identity_file_identity, identity_file)
        } else {
            if job_locks.entry_exists(TRANSFER_DIRECTORY)? {
                return Err(WorkerError::Protocol(
                    "canonical transfer lock identity is absent".into(),
                ));
            }
            job_locks.retry_pending_owned_children_matching(|component, _| {
                parse_generated_hex_prefix(component, b".transfer-init-")
            })?;
            for name in job_locks.list_names()? {
                let name = std::str::from_utf8(&name).map_err(|_| {
                    WorkerError::Protocol("job lock namespace contains a non-UTF-8 entry".into())
                })?;
                if let Some(suffix) = name.strip_prefix(".transfer-init-") {
                    if !is_lower_hex(suffix, 32) {
                        return Err(WorkerError::Protocol(
                            "unsafe transfer initialization residue".into(),
                        ));
                    }
                    admission.validate()?;
                    self.remove_owned_child_committed(&job_locks, name)?;
                }
            }
            let operation_name = format!(
                ".transfer-init-{}",
                hex_16(*uuid::Uuid::new_v4().as_bytes())
            );
            let mut operation = job_locks.create_new_child_directory(&operation_name)?;
            if self.consume_fault(HostStoreWritePoint::AfterTransferDirectoryCreate) {
                return Err(injected_transfer_initialization());
            }
            drop(operation.open_private_lock(TRANSFER_LOCK_FILE)?);
            operation.sync_root()?;
            if self.consume_fault(HostStoreWritePoint::AfterTransferLockSync) {
                return Err(injected_transfer_initialization());
            }
            let lock_identity = operation.private_entry_identity(TRANSFER_LOCK_FILE)?;
            let identity_file_identity = jobs.write_private_atomic_no_replace_with_identity(
                &transfer_identity_name,
                |identity_file| {
                    let identity = build_transfer_lock_identity(
                        &operation,
                        job,
                        lock_identity,
                        identity_file,
                        &transfer_identity_name,
                    )
                    .map_err(worker_error_as_io)?;
                    serde_json::to_vec(&identity).map_err(|error| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, error)
                    })
                },
            )?;
            let identity: TransferLockIdentity =
                read_json_strict_at(&jobs, &transfer_identity_name)?;
            let identity_file = jobs.open_private_regular_handle(&transfer_identity_name)?;
            jobs.validate_private_regular_binding(
                &transfer_identity_name,
                &identity_file,
                identity.identity_file.as_private(),
            )?;
            let staged = build_transfer_lock_identity(
                &operation,
                job,
                operation.private_entry_identity(TRANSFER_LOCK_FILE)?,
                identity_file_identity,
                &transfer_identity_name,
            )?;
            if staged != identity {
                return Err(WorkerError::Protocol(
                    "canonical transfer lock identity changed".into(),
                ));
            }
            if self.consume_fault(HostStoreWritePoint::AfterTransferIdentityPublish) {
                return Err(injected_transfer_initialization());
            }
            operation.sync_root()?;
            admission.validate()?;
            operation.publish_owned_into(&job_locks, TRANSFER_DIRECTORY)?;
            if self.consume_fault(HostStoreWritePoint::AfterTransferPublish) {
                return Err(injected_transfer_initialization());
            }
            job_locks.sync_root()?;
            (identity, identity_file_identity, identity_file)
        };

        if !job_locks.entry_exists(TRANSFER_DIRECTORY)? {
            let winner = identity.directory.as_private();
            job_locks.retry_pending_owned_children_matching(|component, target| {
                parse_generated_hex_prefix(component, b".transfer-init-") && target != winner
            })?;
            let mut matching_operation = None;
            for bytes in job_locks.list_names()? {
                let name = std::str::from_utf8(&bytes).map_err(|_| {
                    WorkerError::Protocol("job lock namespace contains a non-UTF-8 entry".into())
                })?;
                let Some(suffix) = name.strip_prefix(".transfer-init-") else {
                    continue;
                };
                if !is_lower_hex(suffix, 32) {
                    return Err(WorkerError::Protocol(
                        "unsafe transfer initialization residue".into(),
                    ));
                }
                let operation = job_locks.open_child_directory(&relative(name)?, false)?;
                if operation.identity()? == identity.directory.as_private() {
                    if matching_operation.is_some() {
                        return Err(WorkerError::Protocol(
                            "ambiguous transfer initialization residue".into(),
                        ));
                    }
                    matching_operation = Some(operation);
                } else {
                    admission.validate()?;
                    self.remove_owned_child_committed(&job_locks, name)?;
                }
            }
            let mut operation = matching_operation.ok_or_else(|| {
                WorkerError::Protocol("canonical transfer lock directory is absent".into())
            })?;
            let staged = build_transfer_lock_identity(
                &operation,
                job,
                operation.private_entry_identity(TRANSFER_LOCK_FILE)?,
                identity_file_identity,
                &transfer_identity_name,
            )?;
            if staged != identity {
                return Err(WorkerError::Protocol(
                    "canonical transfer lock identity changed".into(),
                ));
            }
            operation.validate_private_regular_binding(
                TRANSFER_LOCK_FILE,
                &operation.open_existing_private_lock(TRANSFER_LOCK_FILE)?,
                identity.lock.as_private(),
            )?;
            admission.validate()?;
            operation.publish_owned_into(&job_locks, TRANSFER_DIRECTORY)?;
            job_locks.sync_root()?;
        }
        let transfer = job_locks.open_child_directory(&relative(TRANSFER_DIRECTORY)?, false)?;
        let current = build_transfer_lock_identity(
            &transfer,
            job,
            transfer.private_entry_identity(TRANSFER_LOCK_FILE)?,
            identity_file_identity,
            &transfer_identity_name,
        )?;
        if identity != current {
            return Err(WorkerError::Protocol(
                "canonical transfer lock identity changed".into(),
            ));
        }
        let file = transfer.open_existing_private_lock(TRANSFER_LOCK_FILE)?;
        transfer.validate_private_regular_binding(
            TRANSFER_LOCK_FILE,
            &file,
            identity.lock.as_private(),
        )?;
        admission.validate()?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if result != 0 {
            return Err(WorkerError::Io(std::io::Error::last_os_error()));
        }
        admission.validate()?;
        let guard = TransferGuard {
            file,
            namespace: transfer,
            file_name: TRANSFER_LOCK_FILE.into(),
            file_identity: identity.lock.as_private(),
            anchor_namespace: jobs,
            identity_file,
            identity_file_name: transfer_identity_name,
            identity_file_identity,
            identity,
        };
        guard.validate()?;
        Ok(guard)
    }

    pub(crate) fn supervisor_lock_after(
        &self,
        admission: &AdmissionGuard,
        job: JobId,
        blocking: bool,
    ) -> Result<Option<SupervisorGuard>, WorkerError> {
        admission.validate_for(job)?;
        let identity_name = format!("{job}{SUPERVISOR_IDENTITY_SUFFIX}");
        let jobs = self.open_directory("locks/jobs", false)?;
        let job_locks = admission.namespace.reopen()?;
        let identity_exists = jobs.entry_exists(&identity_name)?;
        let (identity, identity_file_identity, identity_file) = if identity_exists {
            let stored: SupervisorLockIdentity = read_json_strict_at(&jobs, &identity_name)?;
            let identity_file_identity = jobs.private_entry_identity(&identity_name)?;
            let identity_file = jobs.open_private_regular_handle(&identity_name)?;
            jobs.validate_private_regular_binding(
                &identity_name,
                &identity_file,
                stored.identity_file.as_private(),
            )?;
            if stored.version != HOST_LAYOUT_VERSION
                || stored.job_id != job
                || stored.identity_file
                    != LayoutEntry::new(identity_name.clone(), identity_file_identity)
            {
                return Err(WorkerError::Protocol(
                    "canonical supervisor lock identity changed".into(),
                ));
            }
            (stored, identity_file_identity, identity_file)
        } else {
            if job_locks.entry_exists(SUPERVISOR_DIRECTORY)? {
                return Err(WorkerError::Protocol(
                    "canonical supervisor lock identity is absent".into(),
                ));
            }
            job_locks.retry_pending_owned_children_matching(|component, _| {
                parse_generated_hex_prefix(component, b".supervisor-init-")
            })?;
            for bytes in job_locks.list_names()? {
                let name = std::str::from_utf8(&bytes).map_err(|_| {
                    WorkerError::Protocol("job lock namespace contains a non-UTF-8 entry".into())
                })?;
                if let Some(suffix) = name.strip_prefix(".supervisor-init-") {
                    if !is_lower_hex(suffix, 32) {
                        return Err(WorkerError::Protocol(
                            "unsafe supervisor initialization residue".into(),
                        ));
                    }
                    admission.validate_for(job)?;
                    self.remove_owned_child_committed(&job_locks, name)?;
                }
            }
            let operation_name = format!(
                ".supervisor-init-{}",
                hex_16(*uuid::Uuid::new_v4().as_bytes())
            );
            let mut operation = job_locks.create_new_child_directory(&operation_name)?;
            drop(operation.open_private_lock(SUPERVISOR_LOCK_FILE)?);
            operation.sync_root()?;
            let lock_identity = operation.private_entry_identity(SUPERVISOR_LOCK_FILE)?;
            let identity_file_identity = jobs.write_private_atomic_no_replace_with_identity(
                &identity_name,
                |identity_file| {
                    let identity = build_supervisor_lock_identity(
                        &operation,
                        job,
                        lock_identity,
                        identity_file,
                        &identity_name,
                    )
                    .map_err(worker_error_as_io)?;
                    serde_json::to_vec(&identity).map_err(|error| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, error)
                    })
                },
            )?;
            let identity: SupervisorLockIdentity = read_json_strict_at(&jobs, &identity_name)?;
            let identity_file = jobs.open_private_regular_handle(&identity_name)?;
            jobs.validate_private_regular_binding(
                &identity_name,
                &identity_file,
                identity.identity_file.as_private(),
            )?;
            let staged = build_supervisor_lock_identity(
                &operation,
                job,
                operation.private_entry_identity(SUPERVISOR_LOCK_FILE)?,
                identity_file_identity,
                &identity_name,
            )?;
            if staged != identity {
                return Err(WorkerError::Protocol(
                    "canonical supervisor lock identity changed".into(),
                ));
            }
            operation.sync_root()?;
            admission.validate_for(job)?;
            operation.publish_owned_into(&job_locks, SUPERVISOR_DIRECTORY)?;
            job_locks.sync_root()?;
            (identity, identity_file_identity, identity_file)
        };

        if !job_locks.entry_exists(SUPERVISOR_DIRECTORY)? {
            let winner = identity.directory.as_private();
            job_locks.retry_pending_owned_children_matching(|component, target| {
                parse_generated_hex_prefix(component, b".supervisor-init-") && target != winner
            })?;
            let mut matching_operation = None;
            for bytes in job_locks.list_names()? {
                let name = std::str::from_utf8(&bytes).map_err(|_| {
                    WorkerError::Protocol("job lock namespace contains a non-UTF-8 entry".into())
                })?;
                let Some(suffix) = name.strip_prefix(".supervisor-init-") else {
                    continue;
                };
                if !is_lower_hex(suffix, 32) {
                    return Err(WorkerError::Protocol(
                        "unsafe supervisor initialization residue".into(),
                    ));
                }
                let operation = job_locks.open_child_directory(&relative(name)?, false)?;
                if operation.identity()? == identity.directory.as_private() {
                    if matching_operation.is_some() {
                        return Err(WorkerError::Protocol(
                            "ambiguous supervisor initialization residue".into(),
                        ));
                    }
                    matching_operation = Some(operation);
                } else {
                    admission.validate_for(job)?;
                    self.remove_owned_child_committed(&job_locks, name)?;
                }
            }
            let mut operation = matching_operation.ok_or_else(|| {
                WorkerError::Protocol("canonical supervisor lock directory is absent".into())
            })?;
            let staged = build_supervisor_lock_identity(
                &operation,
                job,
                operation.private_entry_identity(SUPERVISOR_LOCK_FILE)?,
                identity_file_identity,
                &identity_name,
            )?;
            if staged != identity {
                return Err(WorkerError::Protocol(
                    "canonical supervisor lock identity changed".into(),
                ));
            }
            admission.validate_for(job)?;
            operation.publish_owned_into(&job_locks, SUPERVISOR_DIRECTORY)?;
            job_locks.sync_root()?;
        }
        let namespace = job_locks.open_child_directory(&relative(SUPERVISOR_DIRECTORY)?, false)?;
        let current = build_supervisor_lock_identity(
            &namespace,
            job,
            namespace.private_entry_identity(SUPERVISOR_LOCK_FILE)?,
            identity_file_identity,
            &identity_name,
        )?;
        if current != identity {
            return Err(WorkerError::Protocol(
                "canonical supervisor lock identity changed".into(),
            ));
        }
        let file = namespace.open_existing_private_lock(SUPERVISOR_LOCK_FILE)?;
        namespace.validate_private_regular_binding(
            SUPERVISOR_LOCK_FILE,
            &file,
            identity.lock.as_private(),
        )?;
        admission.validate_for(job)?;
        let operation = libc::LOCK_EX | if blocking { 0 } else { libc::LOCK_NB };
        if unsafe { libc::flock(file.as_raw_fd(), operation) } != 0 {
            let error = std::io::Error::last_os_error();
            if !blocking && error.raw_os_error() == Some(libc::EWOULDBLOCK) {
                return Ok(None);
            }
            return Err(WorkerError::Io(error));
        }
        admission.validate_for(job)?;
        let guard = SupervisorGuard {
            file,
            namespace,
            file_name: SUPERVISOR_LOCK_FILE.into(),
            file_identity: identity.lock.as_private(),
            anchor_namespace: jobs,
            identity_file,
            identity_file_name: identity_name,
            identity_file_identity,
            identity,
        };
        guard.validate()?;
        Ok(Some(guard))
    }

    pub(crate) fn supervisor_guard_from_inherited(
        &self,
        job: JobId,
        descriptor: std::os::fd::RawFd,
    ) -> Result<SupervisorGuard, WorkerError> {
        if descriptor < 0 {
            return Err(WorkerError::Protocol(
                "inherited supervisor descriptor is invalid".into(),
            ));
        }
        let descriptor_flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        if descriptor_flags < 0 {
            return Err(WorkerError::Protocol(
                "inherited supervisor descriptor is absent".into(),
            ));
        }
        if unsafe {
            libc::fcntl(
                descriptor,
                libc::F_SETFD,
                descriptor_flags | libc::FD_CLOEXEC,
            )
        } < 0
        {
            return Err(WorkerError::Io(std::io::Error::last_os_error()));
        }
        let file = unsafe { File::from_raw_fd(descriptor) };
        let identity_name = format!("{job}{SUPERVISOR_IDENTITY_SUFFIX}");
        let jobs = self.open_directory("locks/jobs", false)?;
        let identity: SupervisorLockIdentity = read_json_strict_at(&jobs, &identity_name)?;
        let identity_file_identity = jobs.private_entry_identity(&identity_name)?;
        let identity_file = jobs.open_private_regular_handle(&identity_name)?;
        jobs.validate_private_regular_binding(
            &identity_name,
            &identity_file,
            identity.identity_file.as_private(),
        )?;
        if identity.version != HOST_LAYOUT_VERSION
            || identity.job_id != job
            || identity.identity_file
                != LayoutEntry::new(identity_name.clone(), identity_file_identity)
        {
            return Err(WorkerError::Protocol(
                "canonical supervisor lock identity changed".into(),
            ));
        }
        let namespace =
            jobs.open_child_directory(&relative(&format!("{job}/{SUPERVISOR_DIRECTORY}"))?, false)?;
        let current = build_supervisor_lock_identity(
            &namespace,
            job,
            namespace.private_entry_identity(SUPERVISOR_LOCK_FILE)?,
            identity_file_identity,
            &identity_name,
        )?;
        if current != identity {
            return Err(WorkerError::Protocol(
                "canonical supervisor lock identity changed".into(),
            ));
        }
        namespace.validate_private_regular_binding(
            SUPERVISOR_LOCK_FILE,
            &file,
            identity.lock.as_private(),
        )?;
        let contender = namespace.open_existing_private_lock(SUPERVISOR_LOCK_FILE)?;
        if unsafe { libc::flock(contender.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            unsafe { libc::flock(contender.as_raw_fd(), libc::LOCK_UN) };
            return Err(WorkerError::Protocol(
                "inherited supervisor descriptor does not retain the elected lock".into(),
            ));
        }
        let contention_error = std::io::Error::last_os_error();
        if contention_error.raw_os_error() != Some(libc::EWOULDBLOCK) {
            return Err(WorkerError::Io(contention_error));
        }
        let guard = SupervisorGuard {
            file,
            namespace,
            file_name: SUPERVISOR_LOCK_FILE.into(),
            file_identity: identity.lock.as_private(),
            anchor_namespace: jobs,
            identity_file,
            identity_file_name: identity_name,
            identity_file_identity,
            identity,
        };
        guard.validate()?;
        Ok(guard)
    }

    pub(crate) fn replace_job_status_after(
        &self,
        status: &mut SupervisorGuard,
        lease: &LeaseRecord,
        job: &RootedDir,
        expected_bytes: &[u8],
        expected: &JobStatus,
        replacement: &JobStatus,
    ) -> Result<(), WorkerError> {
        lease.validate()?;
        expected.validate()?;
        expected.transition(replacement.clone())?;
        status.validate()?;
        if status.job_id() != lease.job_id() {
            return Err(protocol_code(
                "STATUS_CAPABILITY_MISMATCH",
                "status capability belongs to another job",
            ));
        }
        if self.consume_fault(HostStoreWritePoint::BeforeJobStatusReplace) {
            return Err(WorkerError::Io(std::io::Error::other(
                "injected status replacement failure",
            )));
        }
        let canonical_expected = serde_json::to_vec(expected).map_err(|error| {
            WorkerError::Protocol(format!("failed to serialize job status: {error}"))
        })?;
        if canonical_expected != expected_bytes {
            return Err(protocol_code(
                "STATUS_CHANGED",
                "expected status bytes are not canonical for the expected status",
            ));
        }
        let replacement_bytes = serde_json::to_vec(replacement).map_err(|error| {
            WorkerError::Protocol(format!("failed to serialize job status: {error}"))
        })?;
        if replacement_bytes.len() as u64 > MAX_HOST_FILE_BYTES {
            return Err(WorkerError::Protocol("job status exceeds 1 MiB".into()));
        }
        let canonical = self.open_directory(
            &format!(
                "jobs/{}/{}/{}",
                lease.project_id(),
                lease.worktree_id(),
                lease.job_id()
            ),
            false,
        )?;
        job.verify_bound()?;
        if canonical.identity()? != job.identity()? {
            return Err(protocol_code(
                "STATUS_CAPABILITY_MISMATCH",
                "status target is not the capability's canonical job",
            ));
        }
        status.validate()?;
        job.replace_private_regular_exact("status.json", expected_bytes, &replacement_bytes)?;
        status.validate()?;
        canonical.verify_bound()?;
        job.verify_bound()?;
        Ok(())
    }

    pub fn incoming_job(&self, job: JobId, token: LeaseToken) -> Result<PathBuf, WorkerError> {
        self.validate_layout()?;
        Ok(self
            .inner
            .display_root
            .join("incoming")
            .join(job.to_string())
            .join(token.to_string()))
    }

    pub fn verified_receipt(&self, job: JobId) -> Result<PathBuf, WorkerError> {
        self.validate_layout()?;
        Ok(self
            .inner
            .display_root
            .join("verified")
            .join(format!("{job}.json")))
    }

    pub fn job(&self, project: &str, worktree: &str, job: JobId) -> Result<PathBuf, WorkerError> {
        self.validate_layout()?;
        validate_digest(project, "project ID")?;
        validate_digest(worktree, "worktree ID")?;
        Ok(self
            .inner
            .display_root
            .join("jobs")
            .join(project)
            .join(worktree)
            .join(job.to_string()))
    }

    pub fn snapshot(
        &self,
        project: &str,
        worktree: &str,
        digest: &str,
    ) -> Result<PathBuf, WorkerError> {
        self.validate_layout()?;
        validate_digest(project, "project ID")?;
        validate_digest(worktree, "worktree ID")?;
        validate_digest(digest, "snapshot digest")?;
        Ok(self
            .inner
            .display_root
            .join("snapshots")
            .join(project)
            .join(worktree)
            .join(digest))
    }

    pub fn job_index(&self, job: JobId) -> Result<PathBuf, WorkerError> {
        self.validate_layout()?;
        Ok(self
            .inner
            .display_root
            .join("job-index")
            .join(format!("{job}.json")))
    }

    pub fn root(&self) -> &Path {
        &self.inner.display_root
    }

    pub fn mirror(&self, project_id: &str) -> Result<RootedDir, WorkerError> {
        self.validate_layout()?;
        validate_digest(project_id, "project ID")?;
        let repos = self.open_directory("repos", false)?;
        let name = format!("{project_id}.git");
        let mirror = if repos.entry_exists(&name)? {
            repos.open_child_directory(&relative(&name)?, false)?
        } else {
            let mirror = repos.create_new_child_directory(&name)?;
            if let Err(error) = initialize_mirror(&mirror) {
                let _ = self.remove_owned_child_committed(&repos, &name);
                return Err(error);
            }
            mirror
        };
        ensure_mirror_directory(&mirror)?;
        Ok(mirror)
    }

    pub fn mirror_if_present(&self, project_id: &str) -> Result<Option<RootedDir>, WorkerError> {
        self.validate_layout()?;
        validate_digest(project_id, "project ID")?;
        let repos = self.open_directory("repos", false)?;
        let name = format!("{project_id}.git");
        if !repos.entry_exists(&name)? {
            return Ok(None);
        }
        let mirror = repos.open_child_directory(&relative(&name)?, false)?;
        ensure_mirror_directory(&mirror)?;
        Ok(Some(mirror))
    }

    pub fn task_dir(&self, project_id: &str, task_id: TaskId) -> Result<PathBuf, WorkerError> {
        self.validate_layout()?;
        validate_digest(project_id, "project ID")?;
        Ok(self
            .inner
            .display_root
            .join("tasks")
            .join(project_id)
            .join(task_id.to_string()))
    }

    /// Returns the display path of a task workspace while retaining the
    /// rooted validation boundary for opening it.
    pub fn task_workspace(
        &self,
        project_id: &str,
        task_id: TaskId,
    ) -> Result<PathBuf, WorkerError> {
        let task = self.open_task_directory(project_id, task_id, false)?;
        let workspace = task.open_child_directory(&relative("workspace")?, false)?;
        Ok(workspace.path().to_path_buf())
    }

    pub fn task_workspace_if_present(
        &self,
        project_id: &str,
        task_id: TaskId,
    ) -> Result<Option<PathBuf>, WorkerError> {
        let task = match self.open_task_directory(project_id, task_id, false) {
            Ok(task) => task,
            Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if !task.entry_exists("workspace")? {
            return Ok(None);
        }
        let workspace = task.open_child_directory(&relative("workspace")?, false)?;
        Ok(Some(workspace.path().to_path_buf()))
    }

    pub fn task_status(
        &self,
        project_id: &str,
        task_id: TaskId,
    ) -> Result<TaskStatus, WorkerError> {
        let task = self.open_task_directory(project_id, task_id, false)?;
        read_json_strict_at(&task, "status.json")
    }

    /// Runs a host-owned operation while holding the installation lock.
    /// Garbage collection uses the same lock domain as publication and task
    /// cleanup so its candidate preview and apply pass cannot split around a
    /// concurrent mutation.
    pub(crate) fn with_gc_lock<T>(
        &self,
        operation: impl FnOnce() -> Result<T, WorkerError>,
    ) -> Result<T, WorkerError> {
        self.with_installation_lock(operation)
    }

    /// Test and host-command wrapper around the parent-anchored installation
    /// lock. Same domain as admission and `migrate_layout`; not a new mutex.
    #[doc(hidden)]
    pub fn with_installation_lock<T>(
        &self,
        operation: impl FnOnce() -> Result<T, WorkerError>,
    ) -> Result<T, WorkerError> {
        let _guard = InstallationGuard::acquire(&self.inner)?;
        operation()
    }

    /// Fail closed while any in-flight host work remains. Must be used by
    /// setup/migrate before replacing a helper or rewriting layout.
    pub fn require_protocol_upgrade_drain(&self) -> Result<(), WorkerError> {
        self.with_installation_lock(|| self.inspect_protocol_upgrade_locked())
    }

    pub fn require_protocol_upgrade_drain_at(root: &Path) -> Result<(), WorkerError> {
        match Self::open_if_present(root)? {
            Some(store) => store.require_protocol_upgrade_drain(),
            None => Ok(()),
        }
    }

    fn inspect_protocol_upgrade_locked(&self) -> Result<(), WorkerError> {
        inspect_protocol_upgrade_namespaces(&self.inner.namespaces)
    }

    pub(crate) fn session_lock(&self) -> Result<SessionGuard, WorkerError> {
        // Session is last relative to capacity/installation. Close takes
        // capacity (installation) first, then this lock. Retention runs
        // under GC's installation lock and then this lock, never capacity.
        let locks = self.open_directory("locks", false)?;
        let file = locks.open_private_lock(SESSION_LOCK_FILE)?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if result != 0 {
            return Err(WorkerError::Io(std::io::Error::last_os_error()));
        }
        Ok(SessionGuard { file })
    }

    pub fn begin_job(
        &self,
        project: &str,
        worktree: &str,
        job: JobId,
    ) -> Result<StagedJob, WorkerError> {
        self.validate_layout()?;
        validate_digest(project, "project ID")?;
        validate_digest(worktree, "worktree ID")?;
        let guard = self.admission_lock(job)?;
        self.begin_job_after(guard, project, worktree, job)
    }

    pub(crate) fn begin_job_after(
        &self,
        guard: AdmissionGuard,
        project: &str,
        worktree: &str,
        job: JobId,
    ) -> Result<StagedJob, WorkerError> {
        guard.validate_for(job)?;
        validate_digest(project, "project ID")?;
        validate_digest(worktree, "worktree ID")?;
        let nonce = *uuid::Uuid::new_v4().as_bytes();
        let leases = self.open_directory("leases", false)?;
        let root = leases.create_new_child_directory(&format!(".job-{job}-{}", hex_16(nonce)))?;
        let final_parent = self.open_directory(&format!("jobs/{project}/{worktree}"), true)?;
        Ok(StagedJob {
            root,
            final_parent,
            final_name: job.to_string(),
            job_id: job,
            project_id: project.into(),
            worktree_id: worktree.into(),
            snapshot_digest: None,
            receipt_nonce: nonce,
            guard,
        })
    }

    pub(crate) fn reopen_published_after(
        &self,
        guard: AdmissionGuard,
        project: &str,
        worktree: &str,
        job: JobId,
    ) -> Result<PublishedJob, WorkerError> {
        guard.validate_for(job)?;
        validate_digest(project, "project ID")?;
        validate_digest(worktree, "worktree ID")?;
        let final_parent = self.open_directory(&format!("jobs/{project}/{worktree}"), false)?;
        let root = final_parent.open_child_directory(&relative(&job.to_string())?, false)?;
        let published = PublishedJob {
            root,
            guard,
            job_id: job,
            project_id: project.into(),
            worktree_id: worktree.into(),
        };
        published.validate()?;
        let leases = self.open_directory("leases", false)?;
        leases.sync_root()?;
        final_parent.sync_root()?;
        consume_write_fault_io(self, HostStoreWritePoint::AfterJobPublish)?;
        published.validate()?;
        Ok(published)
    }

    pub(crate) fn repair_indexed_publication_after(
        &self,
        admission: &AdmissionGuard,
        project: &str,
        worktree: &str,
        job: JobId,
    ) -> Result<(), WorkerError> {
        admission.validate_for(job)?;
        validate_digest(project, "project ID")?;
        validate_digest(worktree, "worktree ID")?;
        let leases = self.open_directory("leases", false)?;
        let final_parent = self.open_directory(&format!("jobs/{project}/{worktree}"), false)?;
        let final_job = final_parent.open_child_directory(&relative(&job.to_string())?, false)?;
        let index = self.open_directory("job-index", false)?;
        final_job.verify_bound()?;
        admission.validate_for(job)?;
        leases.sync_root()?;
        final_parent.sync_root()?;
        consume_write_fault_io(self, HostStoreWritePoint::AfterJobPublish)?;
        index.sync_root()?;
        consume_write_fault_io(self, HostStoreWritePoint::AfterJobIndexParentSync)?;
        final_job.verify_bound()?;
        admission.validate_for(job)
    }

    pub fn record_accepted(
        &self,
        request: &LeaseAcquireRequest,
        status: &JobStatus,
        now: u64,
    ) -> Result<(), WorkerError> {
        request.validate()?;
        status.validate()?;
        let guard = self.admission_lock(request.material().job_id())?;
        let disposition = JobDisposition::Accepted {
            job_id: request.material().job_id(),
            client_id: request.material().client_id(),
            project_id: request.material().project_id().into(),
            worktree_id: request.material().worktree_id().into(),
            request_fingerprint: request.request_fingerprint().clone(),
            status: status.clone(),
            recorded_at_millis: now,
        };
        guard.validate()?;
        self.write_new_disposition(&disposition)
    }

    pub(crate) fn record_accepted_after(
        &self,
        published: &PublishedJob,
        request: &SubmitRequest,
        status: &JobStatus,
        now: u64,
    ) -> Result<(), WorkerError> {
        request.validate()?;
        status.validate()?;
        published.validate()?;
        if status.state() != crate::job::JobState::Accepted
            || published.job_id != request.material().job_id()
            || published.project_id != request.material().project_id()
            || published.worktree_id != request.material().worktree_id()
        {
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "accepted publication identity does not match the request",
            ));
        }
        let disposition = JobDisposition::Accepted {
            job_id: request.material().job_id(),
            client_id: request.material().client_id(),
            project_id: request.material().project_id().into(),
            worktree_id: request.material().worktree_id().into(),
            request_fingerprint: request.request_fingerprint().clone(),
            status: status.clone(),
            recorded_at_millis: now,
        };
        self.write_new_disposition(&disposition)?;
        published.validate()
    }

    pub fn record_abandoned(
        &self,
        request: &LeaseAcquireRequest,
        now: u64,
    ) -> Result<(), WorkerError> {
        request.validate()?;
        let guard = self.admission_lock(request.material().job_id())?;
        self.record_abandoned_after(&guard, request, now)
    }

    pub(crate) fn record_abandoned_after(
        &self,
        guard: &AdmissionGuard,
        request: &LeaseAcquireRequest,
        now: u64,
    ) -> Result<(), WorkerError> {
        request.validate()?;
        if guard.scope != GuardScope::Job(request.material().job_id()) {
            return Err(WorkerError::Protocol(
                "abandonment requires the matching job admission guard".into(),
            ));
        }
        let token_hash = format!(
            "{:x}",
            Sha256::digest(request.material().lease_token().to_string().as_bytes())
        );
        let disposition = JobDisposition::Abandoned {
            job_id: request.material().job_id(),
            client_id: request.material().client_id(),
            project_id: request.material().project_id().into(),
            worktree_id: request.material().worktree_id().into(),
            request_fingerprint: request.request_fingerprint().clone(),
            lease_token_sha256: token_hash,
            recorded_at_millis: now,
        };
        guard.validate()?;
        self.write_new_disposition(&disposition)
    }

    pub(crate) fn record_resolution_abandoned_after(
        &self,
        guard: &AdmissionGuard,
        identity: &ResolutionIdentity,
        now: u64,
    ) -> Result<(), WorkerError> {
        guard.validate_for(identity.job_id())?;
        let disposition = JobDisposition::Abandoned {
            job_id: identity.job_id(),
            client_id: identity.client_id(),
            project_id: identity.project_id().into(),
            worktree_id: identity.worktree_id().into(),
            request_fingerprint: identity.request_fingerprint().clone(),
            lease_token_sha256: identity.token_hash(),
            recorded_at_millis: now,
        };
        self.write_new_disposition(&disposition)?;
        if self.consume_fault(HostStoreWritePoint::AfterResolutionTombstone) {
            return Err(WorkerError::Io(std::io::Error::other(
                "injected resolution tombstone interruption",
            )));
        }
        guard.validate_for(identity.job_id())
    }

    pub(crate) fn remove_resolution_execution_after(
        &self,
        admission: &AdmissionGuard,
        transfer: &TransferGuard,
        identity: &ResolutionIdentity,
        job: Option<&RootedDir>,
    ) -> Result<(), WorkerError> {
        admission.validate_for(identity.job_id())?;
        transfer.validate()?;
        if let Some(job) = job {
            job.verify_bound()?;
            self.remove_owned_regular_committed(job, "execution.json")?;
            job.sync_root()?;
        }
        if self.consume_fault(HostStoreWritePoint::AfterResolutionExecutionRemoval) {
            return Err(WorkerError::Io(std::io::Error::other(
                "injected resolution execution cleanup interruption",
            )));
        }
        admission.validate_for(identity.job_id())?;
        transfer.validate()
    }

    pub(crate) fn remove_resolution_job_mutable_after(
        &self,
        admission: &AdmissionGuard,
        transfer: &TransferGuard,
        identity: &ResolutionIdentity,
        job: Option<&RootedDir>,
    ) -> Result<(), WorkerError> {
        admission.validate_for(identity.job_id())?;
        transfer.validate()?;
        if let Some(job) = job {
            job.verify_bound()?;
            for name in ["workspace", "home", "tmp"] {
                if job.entry_exists(name)? {
                    self.remove_owned_child_committed(job, name)?;
                } else {
                    job.resume_pending_owned_child_cleanup(name)?;
                }
            }
            job.sync_root()?;
        }
        if self.consume_fault(HostStoreWritePoint::AfterResolutionJobMutableRemoval) {
            return Err(WorkerError::Io(std::io::Error::other(
                "injected resolution job cleanup interruption",
            )));
        }
        admission.validate_for(identity.job_id())?;
        transfer.validate()
    }

    pub(crate) fn remove_resolution_staging_after(
        &self,
        admission: &AdmissionGuard,
        transfer: &TransferGuard,
        identity: &ResolutionIdentity,
    ) -> Result<(), WorkerError> {
        admission.validate_for(identity.job_id())?;
        transfer.validate()?;
        let leases = self.open_directory("leases", false)?;
        self.remove_job_owned_lease_stages(&leases, identity.job_id())?;
        leases.sync_root()?;
        if self.consume_fault(HostStoreWritePoint::AfterResolutionJobStageRemoval) {
            return Err(WorkerError::Io(std::io::Error::other(
                "injected resolution staging cleanup interruption",
            )));
        }
        admission.validate_for(identity.job_id())?;
        transfer.validate()
    }

    pub(crate) fn record_resolution_cleanup_after(
        &self,
        admission: &AdmissionGuard,
        transfer: &TransferGuard,
        identity: &ResolutionIdentity,
        job: Option<&RootedDir>,
    ) -> Result<(), WorkerError> {
        admission.validate_for(identity.job_id())?;
        transfer.validate()?;
        let incoming = self.open_directory("incoming", false)?;
        if incoming.entry_exists(&identity.job_id().to_string())? {
            let job_incoming =
                incoming.open_child_directory(&relative(&identity.job_id().to_string())?, false)?;
            if job_incoming.entry_exists(&identity.lease_token().to_string())? {
                return Err(WorkerError::Protocol(
                    "exact incoming scope remains after resolution cleanup".into(),
                ));
            }
            require_no_private_cleanup_residue(&job_incoming, "incoming job")?;
        }
        require_no_private_cleanup_residue(&incoming, "incoming root")?;
        if let Some(job) = job {
            job.verify_bound()?;
            self.resume_job_replace_stages(job)?;
            for name in ["workspace", "home", "tmp", "execution.json"] {
                if job.entry_exists(name)? {
                    return Err(WorkerError::Protocol(format!(
                        "mutable resolution scope {name} remains"
                    )));
                }
            }
            require_no_private_cleanup_residue(job, "resolution job")?;
        }
        self.verify_resolution_verified_scope_absent(identity.job_id())?;
        let leases = self.open_directory("leases", false)?;
        let stage_prefix = format!(".job-{}-", identity.job_id());
        let exact_acquire = format!(".acquire-{}", identity.job_id());
        let exact_released = format!(".released-{}", identity.job_id());
        for name in leases.list_names()? {
            let name = std::str::from_utf8(&name).map_err(|_| {
                WorkerError::Protocol("lease namespace contains a non-UTF-8 entry".into())
            })?;
            if name.starts_with(&stage_prefix) || name == exact_acquire || name == exact_released {
                return Err(WorkerError::Protocol(
                    "resolution staging residue remains".into(),
                ));
            }
        }
        require_no_private_cleanup_residue(&leases, "lease staging")?;
        if self.consume_fault(HostStoreWritePoint::AfterResolutionAbsenceProof) {
            return Err(WorkerError::Io(std::io::Error::other(
                "injected resolution absence proof interruption",
            )));
        }
        admission.validate_for(identity.job_id())?;
        transfer.validate()
    }

    pub(crate) fn validate_resolution_cleanup_marker_after(
        &self,
        admission: &AdmissionGuard,
        transfer: &TransferGuard,
        identity: &ResolutionIdentity,
    ) -> Result<(), WorkerError> {
        admission.validate_for(identity.job_id())?;
        transfer.validate()?;
        let proof_dir = self.open_directory(&format!("locks/jobs/{}", identity.job_id()), false)?;
        if proof_dir.entry_exists("cleanup-complete.json")? {
            let marker: CleanupMarker = read_json_strict_at(&proof_dir, "cleanup-complete.json")
                .map_err(|_| {
                    protocol_code(
                        "JOB_ID_CONFLICT",
                        "cleanup marker is malformed or noncanonical",
                    )
                })?;
            require_resolution_marker(&marker, identity)?;
        }
        admission.validate_for(identity.job_id())?;
        transfer.validate()
    }

    /// A terminal job whose lease has already been retired is only
    /// cancellable-idempotently when the request still proves ownership of
    /// the durable cleanup marker. This prevents a stale/reused job ID from
    /// being treated as a successful cancellation after capacity release.
    pub(crate) fn validate_cancel_cleanup_marker_after(
        &self,
        admission: &AdmissionGuard,
        request: &CancelRequest,
        meta: &JobMeta,
    ) -> Result<(), WorkerError> {
        request.validate()?;
        meta.validate()?;
        admission.validate_for(request.job_id())?;
        if meta.job_id() != request.job_id()
            || meta.client_id() != request.client_id()
            || meta.request_fingerprint() != request.request_fingerprint()
        {
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "cancel request does not match durable job metadata",
            ));
        }
        let proof_dir = self
            .open_directory(&format!("locks/jobs/{}", request.job_id()), false)
            .map_err(|_| {
                protocol_code(
                    "CLEANUP_PROOF_MISSING",
                    "terminal job has no durable cleanup proof",
                )
            })?;
        let marker: CleanupMarker = read_json_strict_at(&proof_dir, "cleanup-complete.json")
            .map_err(|_| {
                protocol_code(
                    "CLEANUP_PROOF_MISSING",
                    "terminal job cleanup proof is invalid",
                )
            })?;
        let token_hash = format!(
            "{:x}",
            Sha256::digest(request.lease_token().to_string().as_bytes())
        );
        if !marker.terminal_or_abandoned
            || marker.job_id != request.job_id()
            || marker.client_id != request.client_id()
            || marker.project_id != meta.project_id()
            || marker.worktree_id != meta.worktree_id()
            || marker.manifest_digest != meta.manifest_digest()
            || marker.request_fingerprint != *request.request_fingerprint()
            || marker.lease_token_sha256 != token_hash
        {
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "terminal cleanup proof belongs to another immutable request",
            ));
        }
        admission.validate_for(request.job_id())
    }

    pub(crate) fn resolution_cleanup_receipt(
        &self,
        identity: &ResolutionIdentity,
        lease: Option<&LeaseRecord>,
    ) -> Result<Option<CleanupReceipt>, WorkerError> {
        if let Some(lease) = lease
            && !identity.matches_lease(lease)
        {
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "live lease does not match resolution identity",
            ));
        }
        let admission = self.admission_lock(identity.job_id())?;
        let capacity = self.capacity_lock_after(&admission)?;
        admission.validate_for(identity.job_id())?;
        capacity.validate()?;
        self.verify_resolution_identity_scopes_absent(identity)?;
        let proof_dir = self.open_directory(&format!("locks/jobs/{}", identity.job_id()), true)?;
        write_json_once(
            &proof_dir,
            "cleanup-complete.json",
            &CleanupMarker {
                job_id: identity.job_id(),
                client_id: identity.client_id(),
                project_id: identity.project_id().into(),
                worktree_id: identity.worktree_id().into(),
                manifest_digest: identity.manifest_digest().into(),
                request_fingerprint: identity.request_fingerprint().clone(),
                lease_token_sha256: identity.token_hash(),
                terminal_or_abandoned: true,
            },
            "cleanup marker",
        )?;
        let marker: CleanupMarker = read_json_strict_at(&proof_dir, "cleanup-complete.json")?;
        require_resolution_marker(&marker, identity)?;
        if self.consume_fault(HostStoreWritePoint::AfterResolutionCleanupMarker) {
            return Err(WorkerError::Io(std::io::Error::other(
                "injected resolution cleanup marker interruption",
            )));
        }
        let Some(lease) = lease else {
            return Ok(None);
        };
        Ok(Some(CleanupReceipt {
            root_identity: self.root_identity()?,
            job_id: lease.job_id(),
            client_id: lease.client_id(),
            lease_token: lease.lease_token(),
            proof: CleanupProof::Abandoned,
        }))
    }

    pub(crate) fn remove_incoming_after(
        &self,
        admission: &AdmissionGuard,
        transfer: &TransferGuard,
        job: JobId,
        token: LeaseToken,
    ) -> Result<(), WorkerError> {
        if admission.scope != GuardScope::Job(job) {
            return Err(WorkerError::Protocol(
                "incoming cleanup requires the matching job admission guard".into(),
            ));
        }
        admission.validate()?;
        transfer.validate()?;
        let incoming_root = self.open_directory("incoming", false)?;
        incoming_root.resume_pending_owned_child_cleanup(&job.to_string())?;
        if let Some(incoming) = self.open_optional_directory(&format!("incoming/{job}"))? {
            let token = token.to_string();
            admission.validate()?;
            transfer.validate()?;
            self.remove_owned_child_committed(&incoming, &token)?;
            incoming.sync_root()?;
        }
        if self.consume_fault(HostStoreWritePoint::AfterResolutionIncomingRemoval) {
            return Err(WorkerError::Io(std::io::Error::other(
                "injected resolution incoming cleanup interruption",
            )));
        }
        admission.validate()?;
        transfer.validate()?;
        Ok(())
    }

    pub(crate) fn disposition(&self, job: JobId) -> Result<Option<JobDisposition>, WorkerError> {
        let index = self.open_directory("job-index", false)?;
        let disposition: Option<JobDisposition> =
            read_json_optional_at(&index, &format!("{job}.json"))?;
        if let Some(disposition) = &disposition {
            disposition.validate()?;
            if disposition_job_id(disposition) != job {
                return Err(protocol_code(
                    "JOB_ID_CONFLICT",
                    "disposition job ID does not match its canonical filename",
                ));
            }
        }
        Ok(disposition)
    }

    fn write_new_disposition(&self, disposition: &JobDisposition) -> Result<(), WorkerError> {
        disposition.validate()?;
        let job = match disposition {
            JobDisposition::Accepted { job_id, .. } | JobDisposition::Abandoned { job_id, .. } => {
                *job_id
            }
        };
        let index = self.open_directory("job-index", false)?;
        let name = format!("{job}.json");
        if index.entry_exists(&name)? {
            let existing: JobDisposition = read_json_strict_at(&index, &name)?;
            existing.validate()?;
            if disposition_job_id(&existing) != job {
                return Err(protocol_code(
                    "JOB_ID_CONFLICT",
                    "disposition job ID does not match its canonical filename",
                ));
            }
            if same_disposition_identity(&existing, disposition) {
                return Ok(());
            }
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "job ID already has a permanent disposition",
            ));
        }
        if matches!(disposition, JobDisposition::Accepted { .. }) {
            let bytes = serde_json::to_vec(disposition).map_err(|error| {
                WorkerError::Protocol(format!("failed to serialize host JSON: {error}"))
            })?;
            if bytes.len() as u64 > MAX_HOST_FILE_BYTES {
                return Err(WorkerError::Protocol("host JSON exceeds 1 MiB".into()));
            }
            let staging = format!(".accept-{job}.json");
            index.resume_pending_owned_regular_cleanup(&staging)?;
            if index.entry_exists(&staging)? {
                let staged: JobDisposition = read_json_strict_at(&index, &staging)?;
                if !same_recoverable_accepted_staging(&staged, disposition) {
                    return Err(protocol_code(
                        "JOB_ID_CONFLICT",
                        "accepted-index staging evidence conflicts with the request",
                    ));
                }
                self.remove_owned_regular_committed(&index, &staging)?;
            }
            index
                .write_private_atomic_no_replace_with_commit_hooks(
                    &name,
                    &staging,
                    &bytes,
                    || consume_write_fault_io(self, HostStoreWritePoint::AfterJobIndexFileSync),
                    || consume_write_fault_io(self, HostStoreWritePoint::AfterJobIndexRename),
                    || consume_write_fault_io(self, HostStoreWritePoint::AfterJobIndexParentSync),
                )
                .map_err(|error| {
                    if error.kind() == std::io::ErrorKind::AlreadyExists {
                        protocol_code("JOB_ID_CONFLICT", "host record already exists")
                    } else {
                        WorkerError::Io(error)
                    }
                })
        } else {
            atomic_write_at(&index, &name, disposition)
        }
    }

    #[cfg(test)] // Lease cleanup fixtures create a terminal-only job without running Task 7.
    pub(crate) fn record_terminal_status(
        &self,
        lease: &LeaseRecord,
        status: &JobStatus,
    ) -> Result<(), WorkerError> {
        lease.validate()?;
        status.validate()?;
        if !status.state().is_terminal() {
            return Err(WorkerError::Protocol(
                "terminal status proof is required".into(),
            ));
        }
        let admission = self.admission_lock(lease.job_id())?;
        let capacity = self.capacity_lock_after(&admission)?;
        let job_dir = self.open_directory(
            &format!(
                "jobs/{}/{}/{}",
                lease.project_id(),
                lease.worktree_id(),
                lease.job_id()
            ),
            false,
        )?;
        admission.validate()?;
        capacity.validate()?;
        write_json_once(&job_dir, "status.json", status, "terminal status")
    }

    #[doc(hidden)]
    pub fn cleanup_job_owned(&self, lease: &LeaseRecord) -> Result<CleanupReceipt, WorkerError> {
        lease.validate()?;
        let admission = self.admission_lock(lease.job_id())?;
        let capacity = self.capacity_lock_after(&admission)?;
        let final_relative = format!(
            "jobs/{}/{}/{}",
            lease.project_id(),
            lease.worktree_id(),
            lease.job_id()
        );
        let terminal = match self.open_directory(&final_relative, false) {
            Ok(job_dir) => match read_json_optional_at::<JobStatus>(&job_dir, "status.json")? {
                Some(status) if status.state().is_terminal() => {
                    validate_terminal_job_basis(self, &job_dir, lease, false)?;
                    Some(job_dir)
                }
                _ => None,
            },
            Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let proof = match terminal {
            Some(_) => CleanupProof::Terminal,
            _ => {
                let expected_hash = format!(
                    "{:x}",
                    Sha256::digest(lease.lease_token().to_string().as_bytes())
                );
                match self.disposition(lease.job_id())? {
                    Some(JobDisposition::Abandoned {
                        job_id,
                        client_id,
                        project_id,
                        worktree_id,
                        request_fingerprint,
                        lease_token_sha256,
                        ..
                    }) if job_id == lease.job_id()
                        && client_id == lease.client_id()
                        && project_id == lease.project_id()
                        && worktree_id == lease.worktree_id()
                        && request_fingerprint == *lease.request_fingerprint()
                        && lease_token_sha256 == expected_hash =>
                    {
                        CleanupProof::Abandoned
                    }
                    _ => {
                        return Err(WorkerError::Protocol(
                            "durable terminal-or-abandoned proof is required".into(),
                        ));
                    }
                }
            }
        };
        admission.validate()?;
        capacity.validate()?;
        self.remove_job_mutable_scopes(lease)?;
        self.verify_job_mutable_scopes_absent(lease)?;
        let proof_dir = self.open_directory(&format!("locks/jobs/{}", lease.job_id()), true)?;
        admission.validate()?;
        capacity.validate()?;
        write_json_once(
            &proof_dir,
            "cleanup-complete.json",
            &CleanupMarker {
                job_id: lease.job_id(),
                client_id: lease.client_id(),
                project_id: lease.project_id().into(),
                worktree_id: lease.worktree_id().into(),
                manifest_digest: lease.manifest_digest().into(),
                request_fingerprint: lease.request_fingerprint().clone(),
                lease_token_sha256: format!(
                    "{:x}",
                    Sha256::digest(lease.lease_token().to_string().as_bytes())
                ),
                terminal_or_abandoned: true,
            },
            "cleanup marker",
        )?;
        if self.consume_fault(HostStoreWritePoint::AfterJobCleanupProof) {
            return Err(WorkerError::Io(std::io::Error::other(
                "injected cleanup-proof crash boundary",
            )));
        }
        Ok(CleanupReceipt {
            root_identity: self.root_identity()?,
            job_id: lease.job_id(),
            client_id: lease.client_id(),
            lease_token: lease.lease_token(),
            proof,
        })
    }

    pub(crate) fn consume_fault(&self, point: HostStoreWritePoint) -> bool {
        if self
            .inner
            .fault
            .compare_exchange(point as u8, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return true;
        }
        self.inner
            .secondary_fault
            .compare_exchange(point as u8, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    fn after_cleanup_intent_commit(&self) -> std::io::Result<()> {
        if self.consume_fault(HostStoreWritePoint::AfterCleanupIntentCommit) {
            Err(std::io::Error::from_raw_os_error(libc::EIO))
        } else {
            Ok(())
        }
    }

    pub(crate) fn remove_owned_child_committed(
        &self,
        parent: &RootedDir,
        name: &str,
    ) -> std::io::Result<()> {
        parent.remove_owned_child_with_cleanup_hook(name, &|| self.after_cleanup_intent_commit())
    }

    pub(crate) fn remove_owned_regular_committed(
        &self,
        parent: &RootedDir,
        name: &str,
    ) -> std::io::Result<()> {
        parent.remove_owned_regular_with_cleanup_hook(name, &|| self.after_cleanup_intent_commit())
    }

    pub(crate) fn validate_layout(&self) -> Result<(), WorkerError> {
        self.inner.installation_parent.verify_bound()?;
        let lock = self
            .inner
            .installation_parent
            .open_existing_private_lock(&self.inner.installation_names.lock)?;
        self.inner
            .installation_parent
            .validate_private_regular_binding(
                &self.inner.installation_names.lock,
                &lock,
                self.inner.installation.lock.as_private(),
            )?;
        let identity_file = self
            .inner
            .installation_parent
            .open_private_regular_handle(&self.inner.installation_names.identity)?;
        self.inner
            .installation_parent
            .validate_private_regular_binding(
                &self.inner.installation_names.identity,
                &identity_file,
                self.inner.installation_file_identity,
            )?;
        let layout_file = self
            .inner
            .root
            .open_private_regular_handle(HOST_LAYOUT_FILE)?;
        self.inner.root.validate_private_regular_binding(
            HOST_LAYOUT_FILE,
            &layout_file,
            self.inner.installation.layout.as_private(),
        )?;
        self.validate_layout_records()
    }

    pub(crate) fn verify_descriptors_cloexec(&self) -> Result<(), WorkerError> {
        self.inner
            .installation_parent
            .verify_descriptors_cloexec()?;
        self.inner.root.verify_descriptors_cloexec()?;
        for namespace in self.inner.namespaces.values() {
            namespace.verify_descriptors_cloexec()?;
        }
        Ok(())
    }

    fn installation_lock(&self) -> Result<InstallationGuard, WorkerError> {
        InstallationGuard::acquire(&self.inner)
    }

    fn validate_layout_locked(&self, coordination: &InstallationGuard) -> Result<(), WorkerError> {
        coordination.validate()?;
        self.validate_layout_records()?;
        coordination.validate()
    }

    fn validate_layout_records(&self) -> Result<(), WorkerError> {
        let current_layout_identity = self.inner.root.private_entry_identity(HOST_LAYOUT_FILE)?;
        let current = build_host_layout(
            &self.inner.root,
            &self.inner.namespaces,
            current_layout_identity,
        )?;
        validate_host_layout(&self.inner.layout, &current)?;
        let stored: HostLayoutIdentity = read_json_strict_at(&self.inner.root, HOST_LAYOUT_FILE)?;
        if stored != self.inner.layout {
            return Err(WorkerError::Protocol(
                "canonical host layout record changed".into(),
            ));
        }
        let current_installation = build_installation_identity(
            &self.inner.installation_parent,
            &self.inner.installation_names,
            self.inner.installation.lock.as_private(),
            &self.inner.root,
            current_layout_identity,
            self.inner.installation_file_identity,
        )?;
        validate_installation_identity(&self.inner.installation, &current_installation)?;
        let stored_installation: HostInstallationIdentity = read_json_strict_at(
            &self.inner.installation_parent,
            &self.inner.installation_names.identity,
        )?;
        if stored_installation != self.inner.installation {
            return Err(WorkerError::Protocol(
                "canonical host installation record changed".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn open_directory(
        &self,
        path: &str,
        create: bool,
    ) -> Result<RootedDir, WorkerError> {
        let _ = relative(path)?;
        let key = self
            .inner
            .namespaces
            .keys()
            .filter(|key| path == **key || path.starts_with(&format!("{key}/")))
            .max_by_key(|key| key.len())
            .ok_or_else(|| WorkerError::Protocol("path is outside host-owned namespaces".into()))?;
        let namespace = self
            .inner
            .namespaces
            .get(key)
            .expect("selected host namespace exists");
        let suffix = path.strip_prefix(key).unwrap().strip_prefix('/');
        let directory = match suffix {
            None => namespace.reopen()?,
            Some(suffix) => namespace.open_child_directory(&relative(suffix)?, create)?,
        };
        require_private_directory_metadata(&directory.root_metadata()?)?;
        Ok(directory)
    }

    pub(crate) fn open_task_directory(
        &self,
        project_id: &str,
        task_id: TaskId,
        create: bool,
    ) -> Result<RootedDir, WorkerError> {
        validate_digest(project_id, "project ID")?;
        self.open_directory(&format!("tasks/{project_id}/{task_id}"), create)
    }

    pub(crate) fn open_task_workspace(
        &self,
        project_id: &str,
        task_id: TaskId,
    ) -> Result<RootedDir, WorkerError> {
        let task = self.open_task_directory(project_id, task_id, false)?;
        task.open_child_directory(&relative("workspace")?, false)
            .map_err(WorkerError::Io)
    }

    fn root_identity(&self) -> Result<HostRootIdentity, WorkerError> {
        self.inner.root.verify_bound()?;
        let metadata = self.inner.root.root_metadata()?;
        let current = HostRootIdentity {
            device: metadata.st_dev as u64,
            inode: metadata.st_ino,
        };
        if current != self.inner.root_identity {
            return Err(WorkerError::Protocol("host root identity changed".into()));
        }
        Ok(current)
    }

    fn remove_job_mutable_scopes(&self, lease: &LeaseRecord) -> Result<(), WorkerError> {
        let incoming_root = self.open_directory("incoming", false)?;
        let incoming_job_name = lease.job_id().to_string();
        incoming_root.resume_pending_owned_child_cleanup(&incoming_job_name)?;
        if incoming_root.entry_exists(&incoming_job_name)? {
            let incoming =
                incoming_root.open_child_directory(&relative(&incoming_job_name)?, false)?;
            let token = lease.lease_token().to_string();
            self.remove_owned_child_committed(&incoming, &token)?;
            let remaining = incoming.list_names()?;
            let only_private_namespace = remaining.len() == 1
                && remaining[0].as_slice() == b".mac-worker-rooted-fs"
                && incoming
                    .open_child_directory(&relative(".mac-worker-rooted-fs")?, false)?
                    .list_names()?
                    .is_empty();
            if !remaining.is_empty() && !only_private_namespace {
                return Err(WorkerError::Protocol(
                    "incoming job scope contains non-lease evidence".into(),
                ));
            }
            self.remove_owned_child_committed(&incoming_root, &incoming_job_name)?;
        }
        if let Some(job) = self.open_optional_directory(&format!(
            "jobs/{}/{}/{}",
            lease.project_id(),
            lease.worktree_id(),
            lease.job_id()
        ))? {
            self.resume_job_replace_stages(&job)?;
            for name in ["workspace", "home", "tmp"] {
                if job.entry_exists(name)? {
                    self.remove_owned_child_committed(&job, name)?;
                } else {
                    job.resume_pending_owned_child_cleanup(name)?;
                }
            }
            if job.entry_exists("execution.json")? {
                self.remove_owned_regular_committed(&job, "execution.json")?;
            }
        }
        let leases = self.open_directory("leases", false)?;
        self.remove_job_owned_lease_stages(&leases, lease.job_id())?;
        Ok(())
    }

    fn verify_job_mutable_scopes_absent(&self, lease: &LeaseRecord) -> Result<(), WorkerError> {
        let incoming = self.open_directory("incoming", false)?;
        if incoming.entry_exists(&lease.job_id().to_string())? {
            return Err(WorkerError::Protocol(
                "incoming job scope remains after cleanup".into(),
            ));
        }
        require_no_private_cleanup_residue(&incoming, "incoming root")?;
        if let Some(job) = self.open_optional_directory(&format!(
            "jobs/{}/{}/{}",
            lease.project_id(),
            lease.worktree_id(),
            lease.job_id()
        ))? {
            self.resume_job_replace_stages(&job)?;
            for name in ["workspace", "home", "tmp", "execution.json"] {
                if job.entry_exists(name)? {
                    return Err(WorkerError::Protocol(format!(
                        "mutable job scope {name} remains after cleanup"
                    )));
                }
            }
            require_no_private_cleanup_residue(&job, "terminal job")?;
        }
        let leases = self.open_directory("leases", false)?;
        let stage_prefix = format!(".job-{}-", lease.job_id());
        let exact_acquire = format!(".acquire-{}", lease.job_id());
        let exact_released = format!(".released-{}", lease.job_id());
        for name in leases.list_names()? {
            let name = std::str::from_utf8(&name).map_err(|_| {
                WorkerError::Protocol("lease namespace contains a non-UTF-8 entry".into())
            })?;
            if name.starts_with(&stage_prefix) || name == exact_acquire || name == exact_released {
                return Err(WorkerError::Protocol(
                    "operation-owned residue remains after cleanup".into(),
                ));
            }
        }
        require_no_private_cleanup_residue(&leases, "lease staging")?;
        Ok(())
    }

    fn verify_resolution_scopes_absent(&self, lease: &LeaseRecord) -> Result<(), WorkerError> {
        let incoming = self.open_directory("incoming", false)?;
        if incoming.entry_exists(&lease.job_id().to_string())? {
            let job_incoming =
                incoming.open_child_directory(&relative(&lease.job_id().to_string())?, false)?;
            if job_incoming.entry_exists(&lease.lease_token().to_string())? {
                return Err(WorkerError::Protocol(
                    "exact incoming scope remains after cleanup".into(),
                ));
            }
            require_no_private_cleanup_residue(&job_incoming, "incoming job")?;
        }
        require_no_private_cleanup_residue(&incoming, "incoming root")?;
        if let Some(job) = self.open_optional_directory(&format!(
            "jobs/{}/{}/{}",
            lease.project_id(),
            lease.worktree_id(),
            lease.job_id()
        ))? {
            self.resume_job_replace_stages(&job)?;
            for name in ["workspace", "home", "tmp", "execution.json"] {
                if job.entry_exists(name)? {
                    return Err(WorkerError::Protocol(format!(
                        "mutable job scope {name} remains after cleanup"
                    )));
                }
            }
            require_no_private_cleanup_residue(&job, "resolution job")?;
        }
        self.verify_resolution_verified_scope_absent(lease.job_id())?;
        let leases = self.open_directory("leases", false)?;
        let stage_prefix = format!(".job-{}-", lease.job_id());
        let exact_acquire = format!(".acquire-{}", lease.job_id());
        let exact_released = format!(".released-{}", lease.job_id());
        for name in leases.list_names()? {
            let name = std::str::from_utf8(&name).map_err(|_| {
                WorkerError::Protocol("lease namespace contains a non-UTF-8 entry".into())
            })?;
            if name.starts_with(&stage_prefix) || name == exact_acquire || name == exact_released {
                return Err(WorkerError::Protocol(
                    "operation-owned residue remains after cleanup".into(),
                ));
            }
        }
        require_no_private_cleanup_residue(&leases, "lease staging")?;
        Ok(())
    }

    fn verify_resolution_identity_scopes_absent(
        &self,
        identity: &ResolutionIdentity,
    ) -> Result<(), WorkerError> {
        let incoming = self.open_directory("incoming", false)?;
        if incoming.entry_exists(&identity.job_id().to_string())? {
            let job_incoming =
                incoming.open_child_directory(&relative(&identity.job_id().to_string())?, false)?;
            if job_incoming.entry_exists(&identity.lease_token().to_string())? {
                return Err(WorkerError::Protocol(
                    "exact incoming scope remains after cleanup".into(),
                ));
            }
            require_no_private_cleanup_residue(&job_incoming, "incoming job")?;
        }
        require_no_private_cleanup_residue(&incoming, "incoming root")?;
        if let Some(job) = self.open_optional_directory(&format!(
            "jobs/{}/{}/{}",
            identity.project_id(),
            identity.worktree_id(),
            identity.job_id()
        ))? {
            self.resume_job_replace_stages(&job)?;
            for name in ["workspace", "home", "tmp", "execution.json"] {
                if job.entry_exists(name)? {
                    return Err(WorkerError::Protocol(format!(
                        "mutable job scope {name} remains after cleanup"
                    )));
                }
            }
            require_no_private_cleanup_residue(&job, "resolution job")?;
        }
        self.verify_resolution_verified_scope_absent(identity.job_id())?;
        let leases = self.open_directory("leases", false)?;
        let stage_prefix = format!(".job-{}-", identity.job_id());
        let exact_acquire = format!(".acquire-{}", identity.job_id());
        let exact_released = format!(".released-{}", identity.job_id());
        for name in leases.list_names()? {
            let name = std::str::from_utf8(&name).map_err(|_| {
                WorkerError::Protocol("lease namespace contains a non-UTF-8 entry".into())
            })?;
            if name.starts_with(&stage_prefix) || name == exact_acquire || name == exact_released {
                return Err(WorkerError::Protocol(
                    "operation-owned residue remains after cleanup".into(),
                ));
            }
        }
        require_no_private_cleanup_residue(&leases, "lease staging")?;
        Ok(())
    }

    fn verify_resolution_verified_scope_absent(&self, job: JobId) -> Result<(), WorkerError> {
        let verified = self.open_directory("verified", false)?;
        for name in [format!("{job}.json"), format!(".verify-{job}.json.pending")] {
            if verified.entry_exists(&name)? {
                return Err(WorkerError::Protocol(
                    "exact verified state remains after resolution cleanup".into(),
                ));
            }
        }
        require_no_private_cleanup_residue(&verified, "verified state")
    }

    fn open_optional_directory(&self, path: &str) -> Result<Option<RootedDir>, WorkerError> {
        match self.open_directory(path, false) {
            Ok(directory) => Ok(Some(directory)),
            Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Looks at what is actually left after a host I/O failure. Observation
    /// errors skip that residual instead of guessing it is present.
    pub(crate) fn observe_residuals(
        &self,
        lease: Option<&LeaseRecord>,
        job: Option<&RootedDir>,
    ) -> Vec<&'static str> {
        let mut residual = Vec::new();
        if lease.is_some_and(|lease| self.lease_still_loaded(lease)) {
            residual.push(RESIDUAL_LEASE);
        }
        if self.cleanup_tree_present(lease, job) {
            residual.push(RESIDUAL_CLEANUP_TREE);
        }
        if self.job_directory_present(lease, job) {
            residual.push(RESIDUAL_JOB_DIR);
        }
        if let Some(lease) = lease
            && self.job_lock_present(lease.job_id(), SUPERVISOR_LOCK_FILE)
        {
            residual.push(RESIDUAL_SUPERVISOR_LOCK);
        }
        if let Some(lease) = lease
            && self.job_lock_present(lease.job_id(), TRANSFER_LOCK_FILE)
        {
            residual.push(RESIDUAL_TRANSFER_LOCK);
        }
        if self.session_present(lease, job) {
            residual.push(RESIDUAL_SESSION);
        }
        if self.workspace_present(lease, job) {
            residual.push(RESIDUAL_WORKSPACE);
        }
        residual
    }

    pub(crate) fn attach_host_io(
        &self,
        error: WorkerError,
        stage: &'static str,
        lease: Option<&LeaseRecord>,
        job: Option<&RootedDir>,
    ) -> WorkerError {
        error.with_host_io_stage(stage, &self.observe_residuals(lease, job))
    }

    fn lease_still_loaded(&self, lease: &LeaseRecord) -> bool {
        matches!(
            crate::lease::LeaseService::new(self).load(),
            Ok(Some(live)) if live.job_id() == lease.job_id()
        )
    }

    fn cleanup_tree_present(&self, lease: Option<&LeaseRecord>, job: Option<&RootedDir>) -> bool {
        // Only this job's durable tree and incoming scope. The parent
        // `incoming/` and `leases/` namespaces are shared, so a leftover from
        // another job must not appear on this receipt.
        let mut present = job.is_some_and(directory_has_cleanup_residue);
        if let Some(lease) = lease {
            present |= self
                .open_optional_directory(&format!(
                    "jobs/{}/{}/{}",
                    lease.project_id(),
                    lease.worktree_id(),
                    lease.job_id()
                ))
                .ok()
                .flatten()
                .is_some_and(|directory| directory_has_cleanup_residue(&directory));
            present |= self
                .open_optional_directory(&format!("incoming/{}", lease.job_id()))
                .ok()
                .flatten()
                .is_some_and(|directory| directory_has_cleanup_residue(&directory));
        }
        present
    }

    fn job_directory_present(&self, lease: Option<&LeaseRecord>, _job: Option<&RootedDir>) -> bool {
        // The durable `jobs/…` directory is the terminal record; a leftover
        // job dir is the incoming scope cleanup failed to remove.
        let Some(lease) = lease else {
            return false;
        };
        self.open_optional_directory(&format!("incoming/{}", lease.job_id()))
            .ok()
            .flatten()
            .is_some()
    }

    fn job_lock_present(&self, job: JobId, name: &str) -> bool {
        self.open_optional_directory(&format!("locks/jobs/{job}"))
            .ok()
            .flatten()
            .is_some_and(|directory| directory_has_entry(&directory, name))
    }

    fn session_present(&self, lease: Option<&LeaseRecord>, job: Option<&RootedDir>) -> bool {
        if job.is_some_and(|job| directory_has_entry(job, "session")) {
            return true;
        }
        let Some(lease) = lease else {
            return false;
        };
        self.open_optional_directory(&format!(
            "jobs/{}/{}/{}",
            lease.project_id(),
            lease.worktree_id(),
            lease.job_id()
        ))
        .ok()
        .flatten()
        .is_some_and(|directory| directory_has_entry(&directory, "session"))
    }

    fn workspace_present(&self, lease: Option<&LeaseRecord>, job: Option<&RootedDir>) -> bool {
        if job.is_some_and(|job| directory_has_entry(job, "workspace")) {
            return true;
        }
        let Some(lease) = lease else {
            return false;
        };
        self.open_optional_directory(&format!(
            "jobs/{}/{}/{}",
            lease.project_id(),
            lease.worktree_id(),
            lease.job_id()
        ))
        .ok()
        .flatten()
        .is_some_and(|directory| directory_has_entry(&directory, "workspace"))
    }

    fn remove_job_owned_lease_stages(
        &self,
        leases: &RootedDir,
        job: JobId,
    ) -> Result<(), WorkerError> {
        let exact_acquire = format!(".acquire-{job}");
        let exact_released = format!(".released-{job}");
        self.remove_owned_child_committed(leases, &exact_acquire)?;
        self.remove_owned_child_committed(leases, &exact_released)?;
        leases.retry_pending_owned_children_matching(|component, _| {
            parse_job_stage_component(component, job)
        })?;
        for name in leases.list_names()? {
            let name = std::str::from_utf8(&name).map_err(|_| {
                WorkerError::Protocol("lease namespace contains a non-UTF-8 entry".into())
            })?;
            if let Some(suffix) = name.strip_prefix(&format!(".job-{job}-")) {
                if !is_lower_hex(suffix, 32) {
                    return Err(WorkerError::Protocol(
                        "unsafe job-owned staging replacement".into(),
                    ));
                }
                self.remove_owned_child_committed(leases, name)?;
            }
        }
        Ok(())
    }

    fn resume_job_replace_stages(&self, job: &RootedDir) -> Result<(), WorkerError> {
        job.retry_pending_owned_regulars_matching(|component, _| {
            std::str::from_utf8(component).is_ok_and(parse_replace_uuid_name)
        })?;
        for name in job.list_names()? {
            let name = std::str::from_utf8(&name)
                .map_err(|_| WorkerError::Protocol("job contains a non-UTF-8 entry".into()))?;
            if parse_replace_uuid_name(name) {
                return Err(WorkerError::Protocol(
                    "unbound replace stage remains after cleanup recovery".into(),
                ));
            }
        }
        Ok(())
    }
}

fn inject_open_fault(
    selected: Option<HostStoreWritePoint>,
    boundary: HostStoreWritePoint,
) -> Result<(), WorkerError> {
    if selected == Some(boundary) {
        return Err(WorkerError::Io(std::io::Error::other(
            "injected host store initialization failure",
        )));
    }
    Ok(())
}

fn injected_transfer_initialization() -> WorkerError {
    WorkerError::Io(std::io::Error::other(
        "injected transfer lock initialization failure",
    ))
}

fn open_host_namespaces(
    root: &RootedDir,
    device: u64,
    create: bool,
) -> Result<BTreeMap<&'static str, RootedDir>, WorkerError> {
    let mut namespaces = BTreeMap::new();
    for name in OWNED_DIRECTORIES {
        let child = root.open_child_directory_on_device(&relative(name)?, create, device)?;
        require_private_directory_metadata(&child.root_metadata()?)?;
        namespaces.insert(*name, child);
    }
    let locks = namespaces
        .get("locks")
        .expect("owned locks namespace was inserted");
    let jobs = locks.open_child_directory_on_device(&relative("jobs")?, create, device)?;
    require_private_directory_metadata(&jobs.root_metadata()?)?;
    namespaces.insert("locks/jobs", jobs);
    Ok(namespaces)
}

fn build_host_layout(
    root: &RootedDir,
    namespaces: &BTreeMap<&'static str, RootedDir>,
    layout_file_identity: PrivateEntryIdentity,
) -> Result<HostLayoutIdentity, WorkerError> {
    let mut entries = Vec::with_capacity(OWNED_DIRECTORIES.len() + 2);
    for name in OWNED_DIRECTORIES {
        let namespace = namespaces
            .get(name)
            .expect("owned namespace identity exists");
        entries.push(LayoutEntry::new((*name).into(), namespace.identity()?));
    }
    let jobs = namespaces
        .get("locks/jobs")
        .expect("owned jobs lock namespace identity exists");
    entries.push(LayoutEntry::new("locks/jobs".into(), jobs.identity()?));
    let leases = namespaces
        .get("leases")
        .expect("owned leases namespace identity exists");
    entries.push(LayoutEntry::new(
        "leases/capacity.lock".into(),
        leases.private_entry_identity(CAPACITY_LOCK_FILE)?,
    ));
    Ok(HostLayoutIdentity {
        version: HOST_LAYOUT_VERSION,
        root: LayoutEntry::new(".".into(), root.identity()?),
        layout_file: LayoutEntry::new(HOST_LAYOUT_FILE.into(), layout_file_identity),
        entries,
    })
}

fn initialize_mirror(mirror: &RootedDir) -> Result<(), WorkerError> {
    let output = run_git_in_mirror(mirror, &["init", "--bare", "."])?;
    if !output.status.success() {
        return Err(WorkerError::Git {
            code: "BASE_UNAVAILABLE",
            message: "failed to initialize the host mirror".into(),
        });
    }
    ensure_mirror_directory(mirror)
}

fn ensure_mirror_directory(mirror: &RootedDir) -> Result<(), WorkerError> {
    mirror.verify_descriptors_cloexec()?;
    require_private_directory_metadata(&mirror.root_metadata()?)?;
    let hooks = match mirror.repair_owned_child_directory_mode("hooks", 0o700) {
        Ok(()) => mirror.open_child_directory(&relative("hooks")?, false)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            mirror.create_new_child_directory("hooks")?
        }
        Err(error) => return Err(error.into()),
    };
    let expected = crate::git_transport::PRE_RECEIVE_HOOK.as_bytes();
    match hooks.read_private_regular("pre-receive", MAX_HOST_FILE_BYTES) {
        Ok(bytes) if bytes == expected => {}
        Ok(bytes) => hooks.rewrite_private_regular_exact("pre-receive", &bytes, expected)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            hooks.write_private_atomic_no_replace("pre-receive", expected)?;
        }
        Err(error) => return Err(error.into()),
    }
    hooks.set_private_regular_mode("pre-receive", 0o700)?;
    configure_mirror(mirror)
}

fn configure_mirror(mirror: &RootedDir) -> Result<(), WorkerError> {
    let listed = run_git_in_mirror(mirror, &["--git-dir", ".", "config", "--list", "--local"])?;
    let mut current = std::collections::BTreeMap::new();
    if listed.status.success() {
        for line in String::from_utf8_lossy(&listed.stdout).lines() {
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            current.insert(key.to_ascii_lowercase(), value.to_owned());
        }
    }
    for (key, value) in [
        ("core.hooksPath", "hooks"),
        ("receive.denyDeletes", "true"),
        ("core.fsync", "objects,derived-metadata,reference"),
        ("core.fsyncMethod", "fsync"),
    ] {
        if current.get(&key.to_ascii_lowercase()).map(String::as_str) == Some(value) {
            continue;
        }
        let output = run_git_in_mirror(mirror, &["--git-dir", ".", "config", key, value])?;
        if !output.status.success() {
            return Err(WorkerError::Git {
                code: "BASE_UNAVAILABLE",
                message: "failed to configure the host mirror".into(),
            });
        }
    }
    Ok(())
}

fn run_git_in_mirror(
    mirror: &RootedDir,
    arguments: &[&str],
) -> Result<std::process::Output, WorkerError> {
    mirror.verify_descriptors_cloexec()?;
    let directory_fd = mirror.raw_directory_fd();
    let mut command = Command::new("/usr/bin/git");
    command
        .args(arguments)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1");
    unsafe {
        command.pre_exec(move || {
            if libc::fchdir(directory_fd) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let output = command.output()?;
    mirror.verify_bound()?;
    Ok(output)
}

fn installation_names(parent: &RootedDir, root: &Path) -> Result<InstallationNames, WorkerError> {
    let root_component = root.file_name().ok_or_else(|| {
        WorkerError::Protocol("host data root must identify one directory entry".into())
    })?;
    let root_name = root_component
        .to_str()
        .ok_or_else(|| WorkerError::Protocol("host data root name must be UTF-8".into()))?
        .to_owned();
    let parent_identity = parent.identity()?;
    let mut digest = Sha256::new();
    digest.update(b"mac-worker-installation-v2\0");
    digest.update(parent_identity.inode.to_be_bytes());
    digest.update((root_component.as_bytes().len() as u64).to_be_bytes());
    digest.update(root_component.as_bytes());
    let key = format!("{:x}", digest.finalize());
    let root_name_sha256 = format!("{:x}", Sha256::digest(root_component.as_bytes()));
    Ok(InstallationNames {
        lock: format!("{INSTALLATION_PREFIX}{key}.lock"),
        identity: format!("{INSTALLATION_PREFIX}{key}.json"),
        key,
        root_name,
        root_name_sha256,
    })
}

fn is_installation_identity_name(name: &str) -> bool {
    name.strip_prefix(INSTALLATION_PREFIX)
        .and_then(|rest| rest.strip_suffix(".json"))
        .is_some_and(|key| is_lower_hex(key, 64))
}

fn has_installation_identity_file(parent: &RootedDir) -> Result<bool, WorkerError> {
    for bytes in parent.list_names()? {
        let Ok(name) = std::str::from_utf8(&bytes) else {
            continue;
        };
        if is_installation_identity_name(name) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn recorded_devices_uniform(devices: impl IntoIterator<Item = u64>) -> bool {
    let mut seen = None;
    for device in devices {
        match seen {
            None => seen = Some(device),
            Some(existing) if existing != device => return false,
            Some(_) => {}
        }
    }
    seen.is_some()
}

fn installation_recorded_devices(identity: &HostInstallationIdentity) -> [u64; 5] {
    [
        identity.parent.device,
        identity.lock.device,
        identity.root.device,
        identity.layout.device,
        identity.identity_file.device,
    ]
}

fn layout_recorded_devices(layout: &HostLayoutIdentity) -> impl Iterator<Item = u64> + '_ {
    std::iter::once(layout.root.device)
        .chain(std::iter::once(layout.layout_file.device))
        .chain(layout.entries.iter().map(|entry| entry.device))
}

fn installation_inodes_match(
    stored: &HostInstallationIdentity,
    current: &HostInstallationIdentity,
) -> bool {
    stored.parent.inode == current.parent.inode
        && stored.lock.inode == current.lock.inode
        && stored.root.inode == current.root.inode
        && stored.layout.inode == current.layout.inode
        && stored.identity_file.inode == current.identity_file.inode
}

fn layout_inodes_match(stored: &HostLayoutIdentity, current: &HostLayoutIdentity) -> bool {
    stored.root.inode == current.root.inode
        && stored.layout_file.inode == current.layout_file.inode
        && stored.entries.len() == current.entries.len()
        && stored
            .entries
            .iter()
            .zip(current.entries.iter())
            .all(|(stored, current)| stored.path == current.path && stored.inode == current.inode)
}

fn layout_is_volume_remount(stored: &HostLayoutIdentity, current: &HostLayoutIdentity) -> bool {
    stored.version == current.version
        && layout_inodes_match(stored, current)
        && recorded_devices_uniform(layout_recorded_devices(stored))
        && stored.root.device != current.root.device
}

fn installation_is_volume_remount(
    stored: &HostInstallationIdentity,
    current: &HostInstallationIdentity,
) -> bool {
    stored.version == current.version
        && installation_inodes_match(stored, current)
        && recorded_devices_uniform(installation_recorded_devices(stored))
        && stored.parent.device != current.parent.device
}

fn rename_private_no_replace(
    directory: &RootedDir,
    from: &str,
    to: &str,
) -> Result<(), WorkerError> {
    if from.contains('/')
        || to.contains('/')
        || from.is_empty()
        || to.is_empty()
        || from == "."
        || to == "."
        || from == ".."
        || to == ".."
    {
        return Err(WorkerError::Protocol(
            "host installation rename is not a single private entry".into(),
        ));
    }
    directory.verify_bound()?;
    let from_name = CString::new(from).map_err(|_| {
        WorkerError::Protocol("host installation rename contains an interior NUL".into())
    })?;
    let to_name = CString::new(to).map_err(|_| {
        WorkerError::Protocol("host installation rename contains an interior NUL".into())
    })?;
    let fd = directory.raw_directory_fd();
    #[cfg(target_vendor = "apple")]
    let result = unsafe {
        libc::renameatx_np(
            fd,
            from_name.as_ptr(),
            fd,
            to_name.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let result = unsafe {
        libc::renameat2(
            fd,
            from_name.as_ptr(),
            fd,
            to_name.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    #[cfg(not(any(target_vendor = "apple", target_os = "linux", target_os = "android")))]
    compile_error!("rename_private_no_replace requires renameatx_np or renameat2");
    if result != 0 {
        return Err(WorkerError::Io(std::io::Error::last_os_error()));
    }
    directory.sync_root()?;
    Ok(())
}

fn remove_private_if_present(directory: &RootedDir, name: &str) -> Result<(), WorkerError> {
    if !directory.entry_exists(name)? {
        return Ok(());
    }
    if name.contains('/') || name.is_empty() || name == "." || name == ".." {
        return Err(WorkerError::Protocol(
            "host installation cleanup is not a single private entry".into(),
        ));
    }
    directory.verify_bound()?;
    let c_name = CString::new(name).map_err(|_| {
        WorkerError::Protocol("host installation cleanup contains an interior NUL".into())
    })?;
    let result = unsafe { libc::unlinkat(directory.raw_directory_fd(), c_name.as_ptr(), 0) };
    if result != 0 {
        return Err(WorkerError::Io(std::io::Error::last_os_error()));
    }
    directory.sync_root()?;
    Ok(())
}

fn scan_legacy_installation_identities(
    parent: &RootedDir,
    names: &InstallationNames,
) -> Result<Vec<(String, HostInstallationIdentity)>, WorkerError> {
    let mut found = Vec::new();
    for bytes in parent.list_names()? {
        let Ok(name) = std::str::from_utf8(&bytes) else {
            continue;
        };
        if !is_installation_identity_name(name) || name == names.identity {
            continue;
        }
        let Ok(identity) = read_json_strict_at::<HostInstallationIdentity>(parent, name) else {
            continue;
        };
        found.push((name.to_owned(), identity));
    }
    Ok(found)
}

fn live_reanchor_inodes(
    parent: &RootedDir,
    names: &InstallationNames,
) -> Result<Option<(u64, u64, u64)>, WorkerError> {
    if !parent.entry_exists(&names.root_name)? {
        return Ok(None);
    }
    let parent_device = parent.root_metadata()?.st_dev as u64;
    let rooted = parent.open_private_direct_child_on_device(&names.root_name, parent_device)?;
    if !rooted.entry_exists(HOST_LAYOUT_FILE)? {
        return Ok(None);
    }
    let root_inode = rooted.identity()?.inode;
    let layout_inode = rooted.private_entry_identity(HOST_LAYOUT_FILE)?.inode;
    Ok(Some((parent.identity()?.inode, root_inode, layout_inode)))
}

fn legacy_identity_matches(
    stored: &HostInstallationIdentity,
    names: &InstallationNames,
    parent_inode: u64,
    root_inode: u64,
    lock_inode: u64,
    layout_inode: u64,
) -> bool {
    stored.root_name_sha256 == names.root_name_sha256
        && stored.parent.inode == parent_inode
        && stored.root.inode == root_inode
        && stored.lock.inode == lock_inode
        && stored.layout.inode == layout_inode
        && recorded_devices_uniform(installation_recorded_devices(stored))
}

fn is_installation_lock_name(name: &str) -> bool {
    name.strip_prefix(INSTALLATION_PREFIX)
        .and_then(|rest| rest.strip_suffix(".lock"))
        .is_some_and(|key| is_lower_hex(key, 64))
}

fn lock_file_with_inode(
    parent: &RootedDir,
    names: &InstallationNames,
    expected_inode: u64,
    preferred: Option<u64>,
) -> Result<Option<(String, u64)>, WorkerError> {
    if let Some(inode) = preferred {
        if inode == expected_inode {
            return Ok(Some((names.lock.clone(), inode)));
        }
        return Ok(None);
    }
    let mut found = None;
    for bytes in parent.list_names()? {
        let Ok(name) = std::str::from_utf8(&bytes) else {
            continue;
        };
        if !is_installation_lock_name(name) {
            continue;
        }
        let inode = parent.private_entry_identity(name)?.inode;
        if inode != expected_inode {
            continue;
        }
        if found.is_some() {
            return Err(WorkerError::Protocol(
                "host installation lock is absent".into(),
            ));
        }
        found = Some((name.to_owned(), inode));
    }
    Ok(found)
}

fn unique_matching_legacy_identity(
    parent: &RootedDir,
    names: &InstallationNames,
    lock_inode: Option<u64>,
) -> Result<Option<(String, String)>, WorkerError> {
    let Some((parent_inode, root_inode, layout_inode)) = live_reanchor_inodes(parent, names)?
    else {
        return Ok(None);
    };
    let mut matches = Vec::new();
    for (identity_name, stored) in scan_legacy_installation_identities(parent, names)? {
        let Some((lock_name, current_lock_inode)) =
            lock_file_with_inode(parent, names, stored.lock.inode, lock_inode)?
        else {
            continue;
        };
        if !legacy_identity_matches(
            &stored,
            names,
            parent_inode,
            root_inode,
            current_lock_inode,
            layout_inode,
        ) {
            continue;
        }
        matches.push((lock_name, identity_name));
    }
    match matches.len() {
        0 => Ok(None),
        1 => Ok(matches.pop()),
        _ => Err(WorkerError::Protocol(
            "host installation lock is absent".into(),
        )),
    }
}

fn reanchor_legacy_installation(
    parent: &RootedDir,
    names: &InstallationNames,
) -> Result<bool, WorkerError> {
    let Some((lock_name, identity_name)) = unique_matching_legacy_identity(parent, names, None)?
    else {
        return Ok(false);
    };
    if lock_name != names.lock {
        rename_private_no_replace(parent, &lock_name, &names.lock)?;
    }
    if identity_name != names.identity {
        rename_private_no_replace(parent, &identity_name, &names.identity)?;
    }
    Ok(true)
}

fn complete_legacy_identity_reanchor(
    parent: &RootedDir,
    names: &InstallationNames,
    lock_identity: PrivateEntryIdentity,
) -> Result<(), WorkerError> {
    if parent.entry_exists(&names.identity)? {
        return Ok(());
    }
    let Some((_, identity_name)) =
        unique_matching_legacy_identity(parent, names, Some(lock_identity.inode))?
    else {
        return Ok(());
    };
    if identity_name != names.identity {
        rename_private_no_replace(parent, &identity_name, &names.identity)?;
    }
    Ok(())
}

fn publish_host_layout(
    rooted: &RootedDir,
    namespaces: &BTreeMap<&'static str, RootedDir>,
) -> Result<HostLayoutIdentity, WorkerError> {
    let layout_file_identity = rooted.write_private_atomic_no_replace_with_identity(
        HOST_LAYOUT_FILE,
        |layout_file_identity| {
            let layout = build_host_layout(rooted, namespaces, layout_file_identity)
                .map_err(worker_error_as_io)?;
            serde_json::to_vec(&layout)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
        },
    )?;
    let current_layout = build_host_layout(rooted, namespaces, layout_file_identity)?;
    let stored: HostLayoutIdentity = read_json_strict_at(rooted, HOST_LAYOUT_FILE)?;
    validate_host_layout(&stored, &current_layout)?;
    Ok(stored)
}

fn publish_installation_identity(
    parent: &RootedDir,
    names: &InstallationNames,
    lock_identity: PrivateEntryIdentity,
    rooted: &RootedDir,
    layout_file_identity: PrivateEntryIdentity,
) -> Result<(HostInstallationIdentity, PrivateEntryIdentity), WorkerError> {
    let installation_file_identity = parent.write_private_atomic_no_replace_with_identity(
        &names.identity,
        |installation_file_identity| {
            let installation = build_installation_identity(
                parent,
                names,
                lock_identity,
                rooted,
                layout_file_identity,
                installation_file_identity,
            )
            .map_err(worker_error_as_io)?;
            serde_json::to_vec(&installation)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
        },
    )?;
    let current = build_installation_identity(
        parent,
        names,
        lock_identity,
        rooted,
        layout_file_identity,
        installation_file_identity,
    )?;
    let stored: HostInstallationIdentity = read_json_strict_at(parent, &names.identity)?;
    validate_installation_identity(&stored, &current)?;
    Ok((stored, installation_file_identity))
}

fn refresh_host_layout(
    rooted: &RootedDir,
    namespaces: &BTreeMap<&'static str, RootedDir>,
    point: Option<HostStoreWritePoint>,
) -> Result<HostLayoutIdentity, WorkerError> {
    if rooted.entry_exists(HOST_LAYOUT_FILE)? && !rooted.entry_exists(HOST_LAYOUT_REFRESH_NAME)? {
        rename_private_no_replace(rooted, HOST_LAYOUT_FILE, HOST_LAYOUT_REFRESH_NAME)?;
    }
    inject_open_fault(point, HostStoreWritePoint::AfterHostLayoutRefreshUnlink)?;
    if !rooted.entry_exists(HOST_LAYOUT_FILE)? {
        let stored = publish_host_layout(rooted, namespaces)?;
        inject_open_fault(point, HostStoreWritePoint::AfterHostLayoutRefreshPublish)?;
        return Ok(stored);
    }
    let layout_file_identity = rooted.private_entry_identity(HOST_LAYOUT_FILE)?;
    let current = build_host_layout(rooted, namespaces, layout_file_identity)?;
    let stored: HostLayoutIdentity = read_json_strict_at(rooted, HOST_LAYOUT_FILE)?;
    validate_host_layout(&stored, &current)?;
    inject_open_fault(point, HostStoreWritePoint::AfterHostLayoutRefreshPublish)?;
    Ok(stored)
}

fn restore_or_refresh_installation_identity(
    parent: &RootedDir,
    names: &InstallationNames,
    lock_identity: PrivateEntryIdentity,
    rooted: &RootedDir,
    layout_file_identity: PrivateEntryIdentity,
    reanchored: bool,
    point: Option<HostStoreWritePoint>,
) -> Result<(HostInstallationIdentity, PrivateEntryIdentity), WorkerError> {
    let identity_present = parent.entry_exists(&names.identity)?;
    let refresh_residue = parent.entry_exists(INSTALLATION_IDENTITY_REFRESH_NAME)?;
    if !identity_present && !refresh_residue {
        return Err(WorkerError::Protocol(
            "host installation is incomplete after coordination".into(),
        ));
    }
    if identity_present {
        let installation_file_identity = parent.private_entry_identity(&names.identity)?;
        let current = build_installation_identity(
            parent,
            names,
            lock_identity,
            rooted,
            layout_file_identity,
            installation_file_identity,
        )?;
        let stored: HostInstallationIdentity = read_json_strict_at(parent, &names.identity)?;
        if stored == current {
            remove_private_if_present(parent, INSTALLATION_IDENTITY_REFRESH_NAME)?;
            return Ok((stored, installation_file_identity));
        }
        let refresh = reanchored
            || installation_is_volume_remount(&stored, &current)
            || (installation_inodes_match(&stored, &current) && stored.key != current.key);
        if !refresh {
            validate_installation_identity(&stored, &current)?;
            remove_private_if_present(parent, INSTALLATION_IDENTITY_REFRESH_NAME)?;
            return Ok((stored, installation_file_identity));
        }
        if !refresh_residue {
            rename_private_no_replace(parent, &names.identity, INSTALLATION_IDENTITY_REFRESH_NAME)?;
        } else if identity_present {
            remove_private_if_present(parent, &names.identity)?;
        }
    } else if !refresh_residue {
        return Err(WorkerError::Protocol(
            "host installation is incomplete after coordination".into(),
        ));
    }
    inject_open_fault(
        point,
        HostStoreWritePoint::AfterInstallationIdentityRefreshUnlink,
    )?;
    if !parent.entry_exists(&names.identity)? {
        let published = publish_installation_identity(
            parent,
            names,
            lock_identity,
            rooted,
            layout_file_identity,
        )?;
        inject_open_fault(
            point,
            HostStoreWritePoint::AfterInstallationIdentityRefreshPublish,
        )?;
        remove_private_if_present(parent, INSTALLATION_IDENTITY_REFRESH_NAME)?;
        return Ok(published);
    }
    let installation_file_identity = parent.private_entry_identity(&names.identity)?;
    let current = build_installation_identity(
        parent,
        names,
        lock_identity,
        rooted,
        layout_file_identity,
        installation_file_identity,
    )?;
    let stored: HostInstallationIdentity = read_json_strict_at(parent, &names.identity)?;
    validate_installation_identity(&stored, &current)?;
    inject_open_fault(
        point,
        HostStoreWritePoint::AfterInstallationIdentityRefreshPublish,
    )?;
    remove_private_if_present(parent, INSTALLATION_IDENTITY_REFRESH_NAME)?;
    Ok((stored, installation_file_identity))
}

fn build_installation_identity(
    parent: &RootedDir,
    names: &InstallationNames,
    lock_identity: PrivateEntryIdentity,
    root: &RootedDir,
    layout_identity: PrivateEntryIdentity,
    identity_file: PrivateEntryIdentity,
) -> Result<HostInstallationIdentity, WorkerError> {
    let parent_identity = parent.identity()?;
    let root_identity = root.identity()?;
    if lock_identity.device != parent_identity.device
        || identity_file.device != parent_identity.device
        || root_identity.device != parent_identity.device
        || layout_identity.device != parent_identity.device
    {
        return Err(WorkerError::Protocol(
            "host installation spans filesystems".into(),
        ));
    }
    Ok(HostInstallationIdentity {
        version: HOST_INSTALLATION_VERSION,
        key: names.key.clone(),
        root_name_sha256: names.root_name_sha256.clone(),
        parent: LayoutEntry::new("installation-parent".into(), parent_identity),
        lock: LayoutEntry::new("installation-lock".into(), lock_identity),
        root: LayoutEntry::new("host-root".into(), root_identity),
        layout: LayoutEntry::new("host-layout".into(), layout_identity),
        identity_file: LayoutEntry::new("installation-identity".into(), identity_file),
    })
}

fn validate_installation_identity(
    stored: &HostInstallationIdentity,
    current: &HostInstallationIdentity,
) -> Result<(), WorkerError> {
    if stored.version != HOST_INSTALLATION_VERSION || stored != current {
        return Err(WorkerError::Protocol(
            "canonical host installation identity changed".into(),
        ));
    }
    Ok(())
}

fn worker_error_as_io(error: WorkerError) -> std::io::Error {
    match error {
        WorkerError::Io(error) | WorkerError::HostIo { source: error, .. } => error,
        error => std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()),
    }
}

fn consume_write_fault_io(store: &HostStore, point: HostStoreWritePoint) -> std::io::Result<()> {
    if store.consume_fault(point) {
        Err(std::io::Error::other("injected host-store write failure"))
    } else {
        Ok(())
    }
}

fn validate_host_layout(
    stored: &HostLayoutIdentity,
    current: &HostLayoutIdentity,
) -> Result<(), WorkerError> {
    if stored.version != HOST_LAYOUT_VERSION || stored != current {
        return Err(WorkerError::Protocol(
            "canonical host layout identity changed".into(),
        ));
    }
    Ok(())
}

fn host_layout_outdated() -> WorkerError {
    WorkerError::Unavailable(
        "HOST_LAYOUT_OUTDATED: host layout requires worker setup migration".to_string(),
    )
}

fn upgrade_drain_required(message: &str) -> WorkerError {
    WorkerError::Unavailable(format!("HOST_UPGRADE_DRAIN_REQUIRED: {message}"))
}

fn refuse_layout3_promotion(
    namespaces: &BTreeMap<&'static str, RootedDir>,
) -> Result<(), WorkerError> {
    inspect_upgrade_drain(namespaces)
}

fn inspect_upgrade_drain(
    namespaces: &BTreeMap<&'static str, RootedDir>,
) -> Result<(), WorkerError> {
    let leases = namespaces
        .get("leases")
        .expect("owned leases namespace was inserted");
    inspect_lease_namespace(leases)?;
    let incoming = namespaces
        .get("incoming")
        .expect("owned incoming namespace was inserted");
    inspect_residue_namespace(incoming, "incoming transfer residue is still present")?;
    let jobs = namespaces
        .get("jobs")
        .expect("owned jobs namespace was inserted");
    inspect_jobs_namespace(jobs)?;
    let index = namespaces
        .get("job-index")
        .expect("owned job-index namespace was inserted");
    inspect_job_index(index, namespaces)?;
    Ok(())
}

fn inspect_lease_namespace(leases: &RootedDir) -> Result<(), WorkerError> {
    if leases.entry_exists("heavy")? {
        return Err(upgrade_drain_required(
            "layout-2 heavy lease residue is still present",
        ));
    }
    for raw in leases.list_names()? {
        let name = utf8_inventory_name(&raw, "leases")?;
        if is_rooted_bookkeeping(&name)
            || name == "capacity.lock"
            || name == "capacity.json"
            || name == "slots"
        {
            continue;
        }
        return Err(upgrade_drain_required(
            "lease namespace still has live or leftover entries",
        ));
    }
    if leases.entry_exists("slots")? {
        let slots = leases.open_child_directory(&relative("slots")?, false)?;
        inspect_slot_tree(&slots)?;
    }
    Ok(())
}

fn inspect_slot_tree(slots: &RootedDir) -> Result<(), WorkerError> {
    for raw in slots.list_names()? {
        let name = utf8_inventory_name(&raw, "leases/slots")?;
        if is_rooted_bookkeeping(&name) {
            continue;
        }
        let Ok(slot_id) = name.parse::<u8>() else {
            return Err(upgrade_drain_required(
                "lease namespace still has live or leftover entries",
            ));
        };
        if slot_id.to_string() != name {
            return Err(upgrade_drain_required(
                "lease namespace still has live or leftover entries",
            ));
        }
        let slot = slots.open_child_directory(&relative(&name)?, false)?;
        inspect_residue_namespace(&slot, "a live lease is still present")?;
    }
    Ok(())
}

fn inspect_residue_namespace(directory: &RootedDir, message: &str) -> Result<(), WorkerError> {
    for raw in directory.list_names()? {
        let name = utf8_inventory_name(&raw, "host namespace")?;
        if is_rooted_bookkeeping(&name) {
            continue;
        }
        return Err(upgrade_drain_required(message));
    }
    Ok(())
}

fn inspect_job_index(
    index: &RootedDir,
    namespaces: &BTreeMap<&'static str, RootedDir>,
) -> Result<(), WorkerError> {
    for raw in index.list_names()? {
        let name = utf8_inventory_name(&raw, "job-index")?;
        if is_rooted_bookkeeping(&name) {
            continue;
        }
        if name.starts_with(".accept-") {
            return Err(upgrade_drain_required(
                "accepted-index staging residue is still present",
            ));
        }
        if name.starts_with('.') {
            return Err(upgrade_drain_required("job-index residue is still present"));
        }
        let Some(stem) = name.strip_suffix(".json") else {
            return Err(upgrade_drain_required("job-index inventory is unreadable"));
        };
        let parsed: JobId = match stem.parse() {
            Ok(job_id) => job_id,
            Err(_) => {
                return Err(upgrade_drain_required("job-index inventory is unreadable"));
            }
        };
        let disposition: JobDisposition = match read_json_strict_at(index, &name) {
            Ok(disposition) => disposition,
            Err(_) => {
                return Err(upgrade_drain_required("job-index inventory is unreadable"));
            }
        };
        if disposition_job_id(&disposition) != parsed {
            return Err(upgrade_drain_required(
                "job-index filename does not match its record",
            ));
        }
        match disposition {
            JobDisposition::Abandoned { .. } => {}
            JobDisposition::Accepted {
                job_id,
                client_id,
                project_id,
                worktree_id,
                request_fingerprint,
                ..
            } => {
                inspect_terminal_accepted_job(
                    namespaces,
                    job_id,
                    client_id,
                    &project_id,
                    &worktree_id,
                    &request_fingerprint,
                )?;
            }
        }
    }
    Ok(())
}

fn inspect_terminal_accepted_job(
    namespaces: &BTreeMap<&'static str, RootedDir>,
    job_id: crate::job::JobId,
    client_id: crate::job::ClientId,
    project_id: &str,
    worktree_id: &str,
    request_fingerprint: &crate::job::RequestFingerprint,
) -> Result<(), WorkerError> {
    let jobs = namespaces
        .get("jobs")
        .expect("owned jobs namespace was inserted");
    let project = match jobs.open_child_directory(&relative(project_id)?, false) {
        Ok(project) => project,
        Err(_) => {
            return Err(upgrade_drain_required(
                "accepted job directory is absent or unreadable",
            ));
        }
    };
    let worktree = match project.open_child_directory(&relative(worktree_id)?, false) {
        Ok(worktree) => worktree,
        Err(_) => {
            return Err(upgrade_drain_required(
                "accepted job directory is absent or unreadable",
            ));
        }
    };
    let job = match worktree.open_child_directory(&relative(&job_id.to_string())?, false) {
        Ok(job) => job,
        Err(_) => {
            return Err(upgrade_drain_required(
                "accepted job directory is absent or unreadable",
            ));
        }
    };
    let meta: crate::job::JobMeta = match read_json_strict_at(&job, "meta.json") {
        Ok(meta) => meta,
        Err(_) => {
            return Err(upgrade_drain_required(
                "accepted job metadata is unreadable",
            ));
        }
    };
    let status: crate::job::JobStatus = match read_json_strict_at(&job, "status.json") {
        Ok(status) => status,
        Err(_) => {
            return Err(upgrade_drain_required("accepted job status is unreadable"));
        }
    };
    if meta.job_id() != job_id
        || meta.client_id() != client_id
        || meta.project_id() != project_id
        || meta.worktree_id() != worktree_id
        || meta.request_fingerprint() != request_fingerprint
        || !status.state().is_terminal()
    {
        return Err(upgrade_drain_required(
            "accepted job is not a terminal archive",
        ));
    }
    for name in ["workspace", "home", "tmp", "execution.json"] {
        if job.entry_exists(name)? {
            return Err(upgrade_drain_required(
                "accepted job still has mutable execution residue",
            ));
        }
    }
    Ok(())
}

fn utf8_inventory_name(raw: &[u8], label: &str) -> Result<String, WorkerError> {
    std::str::from_utf8(raw)
        .map(str::to_owned)
        .map_err(|_| upgrade_drain_required(&format!("{label} inventory is not UTF-8")))
}

fn is_rooted_bookkeeping(name: &str) -> bool {
    name == ".mac-worker-rooted-fs"
}

fn inspect_jobs_namespace(jobs: &RootedDir) -> Result<(), WorkerError> {
    for project_raw in jobs.list_names()? {
        let project_name = utf8_inventory_name(&project_raw, "jobs")?;
        if is_rooted_bookkeeping(&project_name) {
            continue;
        }
        let project = match jobs.open_child_directory(&relative(&project_name)?, false) {
            Ok(project) => project,
            Err(_) => {
                return Err(upgrade_drain_required("jobs inventory is unreadable"));
            }
        };
        for worktree_raw in project.list_names()? {
            let worktree_name = utf8_inventory_name(&worktree_raw, "jobs")?;
            if is_rooted_bookkeeping(&worktree_name) {
                continue;
            }
            let worktree = match project.open_child_directory(&relative(&worktree_name)?, false) {
                Ok(worktree) => worktree,
                Err(_) => {
                    return Err(upgrade_drain_required("jobs inventory is unreadable"));
                }
            };
            for job_raw in worktree.list_names()? {
                let job_name = utf8_inventory_name(&job_raw, "jobs")?;
                if is_rooted_bookkeeping(&job_name) {
                    continue;
                }
                let job = match worktree.open_child_directory(&relative(&job_name)?, false) {
                    Ok(job) => job,
                    Err(_) => {
                        return Err(upgrade_drain_required("jobs inventory is unreadable"));
                    }
                };
                inspect_archived_job_directory(&job)?;
            }
        }
    }
    Ok(())
}

fn inspect_archived_job_directory(job: &RootedDir) -> Result<(), WorkerError> {
    let meta: crate::job::JobMeta = match read_json_strict_at(job, "meta.json") {
        Ok(meta) => meta,
        Err(_) => {
            return Err(upgrade_drain_required("job metadata is unreadable"));
        }
    };
    let status: crate::job::JobStatus = match read_json_strict_at(job, "status.json") {
        Ok(status) => status,
        Err(_) => {
            return Err(upgrade_drain_required("job status is unreadable"));
        }
    };
    if !status.state().is_terminal() {
        return Err(upgrade_drain_required(
            "a non-terminal job is still present",
        ));
    }
    let _ = meta;
    for name in ["workspace", "home", "tmp", "execution.json"] {
        if job.entry_exists(name)? {
            return Err(upgrade_drain_required(
                "accepted job still has mutable execution residue",
            ));
        }
    }
    Ok(())
}

fn build_transfer_lock_identity(
    directory: &RootedDir,
    job: JobId,
    lock: PrivateEntryIdentity,
    identity_file: PrivateEntryIdentity,
    identity_file_name: &str,
) -> Result<TransferLockIdentity, WorkerError> {
    Ok(TransferLockIdentity {
        version: HOST_LAYOUT_VERSION,
        job_id: job,
        directory: LayoutEntry::new(format!("{job}/{TRANSFER_DIRECTORY}"), directory.identity()?),
        lock: LayoutEntry::new(
            format!("{job}/{TRANSFER_DIRECTORY}/{TRANSFER_LOCK_FILE}"),
            lock,
        ),
        identity_file: LayoutEntry::new(identity_file_name.into(), identity_file),
    })
}

fn build_supervisor_lock_identity(
    directory: &RootedDir,
    job: JobId,
    lock: PrivateEntryIdentity,
    identity_file: PrivateEntryIdentity,
    identity_file_name: &str,
) -> Result<SupervisorLockIdentity, WorkerError> {
    Ok(SupervisorLockIdentity {
        version: HOST_LAYOUT_VERSION,
        job_id: job,
        directory: LayoutEntry::new(
            format!("{job}/{SUPERVISOR_DIRECTORY}"),
            directory.identity()?,
        ),
        lock: LayoutEntry::new(
            format!("{job}/{SUPERVISOR_DIRECTORY}/{SUPERVISOR_LOCK_FILE}"),
            lock,
        ),
        identity_file: LayoutEntry::new(identity_file_name.into(), identity_file),
    })
}

fn require_cloexec(descriptor: std::os::fd::RawFd) -> Result<(), WorkerError> {
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
    if flags == -1 {
        return Err(WorkerError::Io(std::io::Error::last_os_error()));
    }
    if flags & libc::FD_CLOEXEC == 0 {
        return Err(WorkerError::Protocol(
            "internal host descriptor is inheritable".into(),
        ));
    }
    Ok(())
}

fn same_disposition_identity(left: &JobDisposition, right: &JobDisposition) -> bool {
    match (left, right) {
        (
            JobDisposition::Accepted {
                job_id: left_job,
                client_id: left_client,
                project_id: left_project,
                worktree_id: left_worktree,
                request_fingerprint: left_fingerprint,
                ..
            },
            JobDisposition::Accepted {
                job_id: right_job,
                client_id: right_client,
                project_id: right_project,
                worktree_id: right_worktree,
                request_fingerprint: right_fingerprint,
                ..
            },
        ) => {
            left_job == right_job
                && left_client == right_client
                && left_project == right_project
                && left_worktree == right_worktree
                && left_fingerprint == right_fingerprint
        }
        (
            JobDisposition::Abandoned {
                job_id: left_job,
                client_id: left_client,
                project_id: left_project,
                worktree_id: left_worktree,
                request_fingerprint: left_fingerprint,
                lease_token_sha256: left_hash,
                ..
            },
            JobDisposition::Abandoned {
                job_id: right_job,
                client_id: right_client,
                project_id: right_project,
                worktree_id: right_worktree,
                request_fingerprint: right_fingerprint,
                lease_token_sha256: right_hash,
                ..
            },
        ) => {
            left_job == right_job
                && left_client == right_client
                && left_project == right_project
                && left_worktree == right_worktree
                && left_fingerprint == right_fingerprint
                && left_hash == right_hash
        }
        _ => false,
    }
}

fn same_recoverable_accepted_staging(staged: &JobDisposition, requested: &JobDisposition) -> bool {
    matches!(
        (staged, requested),
        (
            JobDisposition::Accepted {
                job_id: staged_job,
                client_id: staged_client,
                project_id: staged_project,
                worktree_id: staged_worktree,
                request_fingerprint: staged_fingerprint,
                status: staged_status,
                ..
            },
            JobDisposition::Accepted {
                job_id: requested_job,
                client_id: requested_client,
                project_id: requested_project,
                worktree_id: requested_worktree,
                request_fingerprint: requested_fingerprint,
                status: requested_status,
                ..
            }
        ) if staged_job == requested_job
            && staged_client == requested_client
            && staged_project == requested_project
            && staged_worktree == requested_worktree
            && staged_fingerprint == requested_fingerprint
            && staged_status == requested_status
    )
}

fn disposition_job_id(disposition: &JobDisposition) -> JobId {
    match disposition {
        JobDisposition::Accepted { job_id, .. } | JobDisposition::Abandoned { job_id, .. } => {
            *job_id
        }
    }
}

fn require_resolution_marker(
    marker: &CleanupMarker,
    identity: &ResolutionIdentity,
) -> Result<(), WorkerError> {
    if marker.terminal_or_abandoned
        && marker.job_id == identity.job_id()
        && marker.client_id == identity.client_id()
        && marker.project_id == identity.project_id()
        && marker.worktree_id == identity.worktree_id()
        && marker.manifest_digest == identity.manifest_digest()
        && marker.request_fingerprint == *identity.request_fingerprint()
        && marker.lease_token_sha256 == identity.token_hash()
    {
        Ok(())
    } else {
        Err(protocol_code(
            "JOB_ID_CONFLICT",
            "cleanup marker belongs to another immutable request",
        ))
    }
}

fn relative(path: &str) -> Result<RelativePath, WorkerError> {
    RelativePath::parse(path.as_bytes()).map_err(|error| WorkerError::Protocol(error.to_string()))
}

fn require_private_directory_metadata(metadata: &libc::stat) -> Result<(), WorkerError> {
    if metadata.st_mode & libc::S_IFMT != libc::S_IFDIR
        || metadata.st_uid != unsafe { libc::geteuid() }
        || metadata.st_mode & 0o077 != 0
    {
        return Err(WorkerError::Protocol(
            "host-store directory is not owner-only and descriptor-bound".into(),
        ));
    }
    Ok(())
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn parse_generated_hex_prefix(component: &[u8], prefix: &[u8]) -> bool {
    let Ok(name) = std::str::from_utf8(component) else {
        return false;
    };
    let Ok(prefix) = std::str::from_utf8(prefix) else {
        return false;
    };
    name.strip_prefix(prefix)
        .is_some_and(|suffix| is_lower_hex(suffix, 32))
}

fn parse_job_stage_component(component: &[u8], job: JobId) -> bool {
    let Ok(name) = std::str::from_utf8(component) else {
        return false;
    };
    name.strip_prefix(&format!(".job-{job}-"))
        .is_some_and(|suffix| is_lower_hex(suffix, 32))
}

fn parse_replace_uuid_name(name: &str) -> bool {
    name.strip_prefix("replace-").is_some_and(|value| {
        uuid::Uuid::parse_str(value).is_ok_and(|uuid| uuid.hyphenated().to_string() == value)
    })
}

fn read_json_strict_at<T: DeserializeOwned + Serialize>(
    directory: &RootedDir,
    name: &str,
) -> Result<T, WorkerError> {
    let bytes = directory.read_private_regular(name, MAX_HOST_FILE_BYTES)?;
    let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
    let value = T::deserialize(&mut deserializer)
        .map_err(|error| WorkerError::Protocol(format!("invalid host JSON: {error}")))?;
    deserializer
        .end()
        .map_err(|error| WorkerError::Protocol(format!("trailing host JSON data: {error}")))?;
    let canonical = serde_json::to_vec(&value).map_err(|error| {
        WorkerError::Protocol(format!("failed to canonicalize host JSON: {error}"))
    })?;
    if canonical != bytes {
        return Err(WorkerError::Protocol("host JSON is not canonical".into()));
    }
    Ok(value)
}

fn read_json_optional_at<T: DeserializeOwned + Serialize>(
    directory: &RootedDir,
    name: &str,
) -> Result<Option<T>, WorkerError> {
    if !directory.entry_exists(name)? {
        return Ok(None);
    }
    read_json_strict_at(directory, name).map(Some)
}

fn atomic_write_at<T: Serialize>(
    directory: &RootedDir,
    name: &str,
    value: &T,
) -> Result<(), WorkerError> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        WorkerError::Protocol(format!("failed to serialize host JSON: {error}"))
    })?;
    if bytes.len() as u64 > MAX_HOST_FILE_BYTES {
        return Err(WorkerError::Protocol("host JSON exceeds 1 MiB".into()));
    }
    directory
        .write_private_atomic_no_replace(name, &bytes)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                protocol_code("JOB_ID_CONFLICT", "host record already exists")
            } else {
                WorkerError::Io(error)
            }
        })
}

fn write_json_once<T: Serialize + DeserializeOwned + PartialEq>(
    directory: &RootedDir,
    name: &str,
    value: &T,
    label: &str,
) -> Result<(), WorkerError> {
    if directory.entry_exists(name)? {
        let existing: T = read_json_strict_at(directory, name)?;
        if existing == *value {
            return Ok(());
        }
        return Err(WorkerError::Protocol(format!(
            "{label} already exists with different content"
        )));
    }
    atomic_write_at(directory, name, value)
}

#[allow(dead_code)] // Task 7 consumes final staged-job publication.
impl StagedJob {
    pub(crate) fn rooted_dir(&self) -> &RootedDir {
        &self.root
    }

    pub(crate) fn job_id(&self) -> JobId {
        self.job_id
    }

    pub(crate) fn project_id(&self) -> &str {
        &self.project_id
    }

    pub(crate) fn worktree_id(&self) -> &str {
        &self.worktree_id
    }

    pub(crate) fn receipt_nonce(&self) -> StagingNonce {
        self.receipt_nonce
    }

    pub(crate) fn create_workspace_tree(&self) -> Result<RootedDir, WorkerError> {
        if self.root.entry_exists("workspace")? {
            return Err(WorkerError::Protocol(
                "staged workspace already exists".into(),
            ));
        }
        let workspace = self
            .root
            .open_child_directory(&relative("workspace")?, true)?;
        Ok(workspace.open_child_directory(&relative("tree")?, true)?)
    }

    pub(crate) fn complete_snapshot_materialization(
        &mut self,
        declared: &BTreeSet<RelativePath>,
        project_id: &str,
        worktree_id: &str,
        manifest_digest: &str,
    ) -> Result<WorkspaceReceipt, WorkerError> {
        validate_digest(project_id, "workspace project ID")?;
        validate_digest(worktree_id, "workspace worktree ID")?;
        validate_digest(manifest_digest, "workspace manifest digest")?;
        if project_id != self.project_id || worktree_id != self.worktree_id {
            return Err(WorkerError::Protocol(
                "workspace snapshot identity does not match staged job".into(),
            ));
        }
        let workspace = self
            .root
            .open_child_directory(&relative("workspace")?, false)?;
        let tree = workspace.open_child_directory(&relative("tree")?, false)?;
        tree.validate_and_sync_snapshot_workspace(declared)?;
        workspace.sync_root()?;
        self.root.sync_root()?;
        self.snapshot_digest = Some(manifest_digest.into());
        Ok(WorkspaceReceipt {
            job_id: self.job_id,
            nonce: self.receipt_nonce,
            project_id: project_id.into(),
            worktree_id: worktree_id.into(),
            manifest_digest: manifest_digest.into(),
        })
    }

    pub(crate) fn publish_complete<R: PublicationReceipt>(
        self,
        receipt: R,
    ) -> Result<PublishedJob, WorkerError> {
        self.publish_complete_with_commit_hooks(receipt, || Ok(()), || Ok(()))
    }

    pub(crate) fn publish_complete_with_commit_hooks<R: PublicationReceipt>(
        mut self,
        receipt: R,
        after_rename: impl FnOnce() -> std::io::Result<()>,
        after_parent_sync: impl FnOnce() -> std::io::Result<()>,
    ) -> Result<PublishedJob, WorkerError> {
        receipt.validate_for(&self)?;
        if self.final_parent.entry_exists(&self.final_name)? {
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "final job path already exists",
            ));
        }
        self.guard.validate()?;
        self.root.publish_owned_into_with_commit_hooks(
            &self.final_parent,
            &self.final_name,
            after_rename,
            after_parent_sync,
        )?;
        let published = PublishedJob {
            root: self.root,
            guard: self.guard,
            job_id: self.job_id,
            project_id: self.project_id,
            worktree_id: self.worktree_id,
        };
        published.validate()?;
        Ok(published)
    }
}

impl PublicationReceipt for WorkspaceReceipt {
    fn validate_for(&self, staged: &StagedJob) -> Result<(), WorkerError> {
        if self.job_id != staged.job_id
            || self.nonce != staged.receipt_nonce
            || self.project_id != staged.project_id
            || self.worktree_id != staged.worktree_id
            || staged.snapshot_digest.as_deref() != Some(self.manifest_digest.as_str())
        {
            return Err(WorkerError::Protocol(
                "workspace receipt does not match staged job".into(),
            ));
        }
        Ok(())
    }
}

impl PublishedJob {
    pub(crate) fn validate(&self) -> Result<(), WorkerError> {
        self.guard.validate_for(self.job_id)?;
        self.root.verify_bound()?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn job_id(&self) -> JobId {
        self.job_id
    }

    pub(crate) fn admission_guard(&self) -> &AdmissionGuard {
        &self.guard
    }
}

impl CleanupReceipt {
    #[allow(dead_code)] // Task 7 lifecycle consumes internal release receipts.
    pub(crate) fn matches(&self, lease: &LeaseRecord) -> bool {
        self.job_id == lease.job_id()
            && self.client_id == lease.client_id()
            && self.lease_token == lease.lease_token()
    }

    #[allow(dead_code)] // Task 7 lifecycle consumes internal release receipts.
    pub(crate) fn validate_durable(
        &self,
        store: &HostStore,
        lease: &LeaseRecord,
    ) -> Result<(), WorkerError> {
        if !self.matches(lease) {
            return Err(WorkerError::Protocol(
                "cleanup receipt identity mismatch".into(),
            ));
        }
        if store.root_identity()? != self.root_identity {
            return Err(WorkerError::Protocol(
                "cleanup receipt belongs to another host root".into(),
            ));
        }
        let proof_dir = store.open_directory(&format!("locks/jobs/{}", lease.job_id()), false)?;
        let marker: CleanupMarker = read_json_strict_at(&proof_dir, "cleanup-complete.json")?;
        let token_hash = format!(
            "{:x}",
            Sha256::digest(lease.lease_token().to_string().as_bytes())
        );
        if !marker.terminal_or_abandoned
            || marker.job_id != lease.job_id()
            || marker.client_id != lease.client_id()
            || marker.project_id != lease.project_id()
            || marker.worktree_id != lease.worktree_id()
            || marker.manifest_digest != lease.manifest_digest()
            || marker.request_fingerprint != *lease.request_fingerprint()
            || marker.lease_token_sha256 != token_hash
        {
            return Err(WorkerError::Protocol(
                "cleanup receipt is not durably valid".into(),
            ));
        }
        match &self.proof {
            CleanupProof::Terminal => {
                store.verify_job_mutable_scopes_absent(lease)?;
                let job_dir = store.open_directory(
                    &format!(
                        "jobs/{}/{}/{}",
                        lease.project_id(),
                        lease.worktree_id(),
                        lease.job_id()
                    ),
                    false,
                )?;
                validate_terminal_job_basis(store, &job_dir, lease, true)?;
            }
            CleanupProof::Abandoned => {
                store.verify_resolution_scopes_absent(lease)?;
                let disposition = store
                    .disposition(lease.job_id())?
                    .ok_or_else(|| WorkerError::Protocol("abandonment proof is absent".into()))?;
                let expected_hash = format!(
                    "{:x}",
                    Sha256::digest(lease.lease_token().to_string().as_bytes())
                );
                if !matches!(disposition, JobDisposition::Abandoned { job_id, client_id, project_id, worktree_id, request_fingerprint, lease_token_sha256, .. }
                    if job_id == lease.job_id() && client_id == lease.client_id()
                        && project_id == lease.project_id() && worktree_id == lease.worktree_id()
                        && request_fingerprint == *lease.request_fingerprint()
                        && lease_token_sha256 == expected_hash)
                {
                    return Err(WorkerError::Protocol(
                        "abandonment cleanup proof is invalid".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

fn validate_terminal_job_basis(
    store: &HostStore,
    job: &RootedDir,
    lease: &LeaseRecord,
    require_mutable_absent: bool,
) -> Result<(), WorkerError> {
    let disposition = store.disposition(lease.job_id())?;
    match disposition {
        Some(JobDisposition::Accepted {
            job_id,
            client_id,
            project_id,
            worktree_id,
            request_fingerprint,
            ..
        }) if job_id == lease.job_id()
            && client_id == lease.client_id()
            && project_id == lease.project_id()
            && worktree_id == lease.worktree_id()
            && request_fingerprint == *lease.request_fingerprint() => {}
        _ => {
            return Err(WorkerError::Protocol(
                "terminal cleanup requires the exact Accepted disposition".into(),
            ));
        }
    }
    let meta: JobMeta = read_json_strict_at(job, "meta.json")?;
    if meta.job_id() != lease.job_id()
        || meta.client_id() != lease.client_id()
        || meta.request_fingerprint() != lease.request_fingerprint()
        || meta.worker_name() != lease.worker_name()
        || meta.project_id() != lease.project_id()
        || meta.worktree_id() != lease.worktree_id()
        || meta.manifest_digest() != lease.manifest_digest()
        || meta.timeout_millis() != lease.timeout_millis()
        || meta.resource_class() != lease.resource_class()
        || meta.command_summary() != lease.command_summary()
    {
        return Err(WorkerError::Protocol(
            "terminal cleanup metadata does not match the live lease".into(),
        ));
    }
    let status: JobStatus = read_json_strict_at(job, "status.json")?;
    if !status.state().is_terminal() {
        return Err(WorkerError::Protocol(
            "terminal cleanup proof is not terminal".into(),
        ));
    }
    let prelaunch_cancelled = status.state() == crate::job::JobState::Cancelled
        && status.child_identity().is_none()
        && status.error_code() == Some("CANCELLED_PRELAUNCH");
    if job.entry_exists("execution.json")? && !prelaunch_cancelled {
        return Err(WorkerError::Protocol(
            "terminal cleanup requires erased execution payload".into(),
        ));
    }
    let expected_log_lengths = [
        status
            .final_stdout_bytes()
            .expect("validated terminal status has a stdout length"),
        status
            .final_stderr_bytes()
            .expect("validated terminal status has a stderr length"),
    ];
    for (log, expected_length) in ["stdout.log", "stderr.log"]
        .into_iter()
        .zip(expected_log_lengths)
    {
        let file = job.open_private_append(log)?;
        let actual_length = job.validate_private_append_binding(log, &file)?;
        if actual_length != expected_length {
            return Err(WorkerError::Protocol(
                "terminal cleanup log length does not match status".into(),
            ));
        }
    }
    let names = job
        .list_names()?
        .into_iter()
        .map(|name| {
            String::from_utf8(name).map_err(|_| {
                WorkerError::Protocol("terminal job contains a non-UTF-8 entry".into())
            })
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let is_turn = job.entry_exists("prompt.md")?;
    let retained = if is_turn {
        vec![
            ".mac-worker-rooted-fs",
            "meta.json",
            "prompt.md",
            "result.schema.json",
            "status.json",
            "stderr.log",
            "stdout.log",
            "tail.log",
        ]
    } else {
        vec![
            ".mac-worker-rooted-fs",
            "meta.json",
            "status.json",
            "stdout.log",
            "stderr.log",
        ]
    }
    .into_iter()
    .map(String::from)
    .collect::<BTreeSet<_>>();
    let retained_with_supervisor_log = retained
        .iter()
        .cloned()
        .chain(std::iter::once(String::from("supervisor.log")))
        .collect::<BTreeSet<_>>();
    let mut terminal_variants = vec![retained.clone(), retained_with_supervisor_log.clone()];
    if is_turn {
        let mut with_last_message = retained.clone();
        with_last_message.insert("last.md".into());
        terminal_variants.push(with_last_message);
        let mut with_last_message_and_supervisor_log = retained_with_supervisor_log.clone();
        with_last_message_and_supervisor_log.insert("last.md".into());
        terminal_variants.push(with_last_message_and_supervisor_log);
    }
    let mut allowed = retained
        .iter()
        .cloned()
        .chain(std::iter::once(String::from("supervisor.log")))
        .chain(if is_turn {
            Some(String::from("last.md"))
        } else {
            None
        })
        .chain(["workspace", "home", "tmp"].into_iter().map(String::from))
        .collect::<BTreeSet<_>>();
    if prelaunch_cancelled {
        allowed.insert("execution.json".into());
    }
    if (require_mutable_absent && !terminal_variants.contains(&names))
        || (!require_mutable_absent && !names.is_subset(&allowed))
    {
        return Err(WorkerError::Protocol(
            "terminal job cleanup scope is incomplete or unsafe".into(),
        ));
    }
    for mutable in ["workspace", "home", "tmp"] {
        if job.entry_exists(mutable)? {
            if require_mutable_absent {
                return Err(WorkerError::Protocol(
                    "terminal job mutable scope remains".into(),
                ));
            }
            job.open_child_directory(&relative(mutable)?, false)?;
        }
    }
    Ok(())
}

fn validate_digest(value: &str, label: &str) -> Result<(), WorkerError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        Ok(())
    } else {
        Err(WorkerError::Protocol(format!(
            "{label} must be 64 lowercase hexadecimal bytes"
        )))
    }
}

fn require_no_private_cleanup_residue(
    directory: &RootedDir,
    scope: &str,
) -> Result<(), WorkerError> {
    if directory.has_private_cleanup_residue()? {
        return Err(WorkerError::Protocol(format!(
            "{scope} private cleanup residue remains"
        )));
    }
    Ok(())
}

fn directory_has_cleanup_residue(directory: &RootedDir) -> bool {
    // Nested private namespace and this directory's own `remove-*`
    // quarantines. `has_private_cleanup_residue` also looks at the parent
    // namespace, which is shared among jobs.
    if let Ok(path) = relative(".mac-worker-rooted-fs")
        && let Ok(namespace) = directory.open_child_directory(&path, false)
        && namespace
            .list_names()
            .ok()
            .is_some_and(|names| !names.is_empty())
    {
        return true;
    }
    directory
        .list_names()
        .ok()
        .is_some_and(|names| names.iter().any(|name| is_owned_remove_quarantine(name)))
}

fn is_owned_remove_quarantine(name: &[u8]) -> bool {
    let Ok(name) = std::str::from_utf8(name) else {
        return false;
    };
    name.strip_prefix("remove-").is_some_and(|value| {
        uuid::Uuid::parse_str(value).is_ok_and(|uuid| uuid.hyphenated().to_string() == value)
    })
}

fn directory_has_entry(directory: &RootedDir, name: &str) -> bool {
    directory.entry_exists(name).unwrap_or(false)
}

fn protocol_code(code: &'static str, message: &str) -> WorkerError {
    WorkerError::Protocol(format!("{code}: {message}"))
}

fn upgrade_rollback_unsafe(message: &str) -> WorkerError {
    protocol_code("HOST_UPGRADE_ROLLBACK_UNSAFE", message)
}

fn is_private_namespace(name: &str) -> bool {
    name == ".mac-worker-rooted-fs"
}

fn inventory_names(directory: &RootedDir, label: &str) -> Result<Vec<String>, WorkerError> {
    directory
        .list_names()?
        .into_iter()
        .map(|raw| utf8_inventory_name(&raw, label))
        .collect()
}

fn inspect_protocol_upgrade_namespaces(
    namespaces: &BTreeMap<&'static str, RootedDir>,
) -> Result<(), WorkerError> {
    let leases = namespaces
        .get("leases")
        .ok_or_else(|| upgrade_drain_required("leases namespace is missing"))?;
    let incoming = namespaces
        .get("incoming")
        .ok_or_else(|| upgrade_drain_required("incoming namespace is missing"))?;
    let jobs = namespaces
        .get("jobs")
        .ok_or_else(|| upgrade_drain_required("jobs namespace is missing"))?;
    let index = namespaces
        .get("job-index")
        .ok_or_else(|| upgrade_drain_required("job-index namespace is missing"))?;
    if leases.has_private_cleanup_residue()?
        || incoming.has_private_cleanup_residue()?
        || jobs.has_private_cleanup_residue()?
        || index.has_private_cleanup_residue()?
    {
        return Err(upgrade_drain_required(
            "private cleanup residue remains in an upgrade-scoped namespace",
        ));
    }
    inspect_upgrade_leases(leases)?;
    inspect_upgrade_incoming(incoming)?;
    inspect_upgrade_jobs(jobs)?;
    inspect_upgrade_job_index(index, jobs)
}

fn inspect_upgrade_leases(leases: &RootedDir) -> Result<(), WorkerError> {
    for name in inventory_names(leases, "lease")? {
        if is_private_namespace(&name) {
            continue;
        }
        match name.as_str() {
            "capacity.lock" | "capacity.json" => {}
            "slots" => inspect_layout3_slots(leases)?,
            "heavy" => {
                return Err(upgrade_drain_required(
                    "live layout-2 lease remains; drain it on the previous helper",
                ));
            }
            _ => {
                return Err(upgrade_drain_required(&format!(
                    "lease inventory contains incomplete or unknown entry {name}"
                )));
            }
        }
    }
    Ok(())
}

fn inspect_layout3_slots(leases: &RootedDir) -> Result<(), WorkerError> {
    let slots = leases
        .open_child_directory(&relative("slots")?, false)
        .map_err(|_| upgrade_drain_required("leases/slots is unreadable"))?;
    if slots.has_private_cleanup_residue()? {
        return Err(upgrade_drain_required(
            "private cleanup residue remains in leases/slots",
        ));
    }
    for name in inventory_names(&slots, "lease slot")? {
        if is_private_namespace(&name) {
            continue;
        }
        if name.starts_with('.') {
            return Err(upgrade_drain_required(&format!(
                "lease slot inventory contains incomplete entry {name}"
            )));
        }
        let slot = slots
            .open_child_directory(&relative(&name)?, false)
            .map_err(|_| upgrade_drain_required("lease slot directory is unreadable"))?;
        for child in inventory_names(&slot, "lease slot entry")? {
            if is_private_namespace(&child) {
                continue;
            }
            return Err(upgrade_drain_required(&format!(
                "live or partial layout-3 slot {name}/{child} remains"
            )));
        }
    }
    Ok(())
}

fn inspect_upgrade_incoming(incoming: &RootedDir) -> Result<(), WorkerError> {
    for name in inventory_names(incoming, "incoming")? {
        if is_private_namespace(&name) {
            continue;
        }
        return Err(upgrade_drain_required(&format!(
            "incoming transfer {name} remains; drain it on the previous helper"
        )));
    }
    Ok(())
}

fn inspect_upgrade_jobs(jobs: &RootedDir) -> Result<(), WorkerError> {
    for project in inventory_names(jobs, "job project")? {
        if is_private_namespace(&project) {
            continue;
        }
        if project.starts_with('.') {
            return Err(upgrade_drain_required(&format!(
                "job inventory contains incomplete project {project}"
            )));
        }
        let project_dir = jobs
            .open_child_directory(&relative(&project)?, false)
            .map_err(|_| upgrade_drain_required("job project directory is unreadable"))?;
        for worktree in inventory_names(&project_dir, "job worktree")? {
            if is_private_namespace(&worktree) {
                continue;
            }
            if worktree.starts_with('.') {
                return Err(upgrade_drain_required(&format!(
                    "job inventory contains incomplete worktree {worktree}"
                )));
            }
            let worktree_dir = project_dir
                .open_child_directory(&relative(&worktree)?, false)
                .map_err(|_| upgrade_drain_required("job worktree directory is unreadable"))?;
            for job_name in inventory_names(&worktree_dir, "job directory")? {
                if is_private_namespace(&job_name) {
                    continue;
                }
                if job_name.starts_with('.') {
                    return Err(upgrade_drain_required(&format!(
                        "job inventory contains incomplete job {job_name}"
                    )));
                }
                let job_id = job_name.parse::<JobId>().map_err(|_| {
                    upgrade_drain_required("job inventory contains an invalid job identity")
                })?;
                let job = worktree_dir
                    .open_child_directory(&relative(&job_id.to_string())?, false)
                    .map_err(|_| upgrade_drain_required("job directory is unreadable"))?;
                inspect_upgrade_job_directory(&job)?;
            }
        }
    }
    Ok(())
}

fn inspect_upgrade_job_directory(job: &RootedDir) -> Result<(), WorkerError> {
    let meta: JobMeta = read_json_strict_at(job, "meta.json")
        .map_err(|_| upgrade_drain_required("job metadata is unreadable or noncanonical"))?;
    let status: JobStatus = read_json_strict_at(job, "status.json")
        .map_err(|_| upgrade_drain_required("job status is unreadable or noncanonical"))?;
    if !status.state().is_terminal() {
        return Err(upgrade_drain_required(&format!(
            "non-terminal job {} (protocol {}) remains; drain it on the previous helper",
            meta.job_id(),
            meta.protocol_version()
        )));
    }
    for mutable in ["workspace", "home", "tmp", "execution.json"] {
        if job.entry_exists(mutable)? {
            return Err(upgrade_drain_required(&format!(
                "job {} retains mutable execution evidence {mutable}",
                meta.job_id()
            )));
        }
    }
    Ok(())
}

fn inspect_upgrade_job_index(index: &RootedDir, jobs: &RootedDir) -> Result<(), WorkerError> {
    for name in inventory_names(index, "job index")? {
        if is_private_namespace(&name) {
            continue;
        }
        if name.starts_with(".accept-") {
            return Err(upgrade_drain_required(&format!(
                "accepted-index staging residue {name} remains"
            )));
        }
        if name.starts_with('.') {
            return Err(upgrade_drain_required(&format!(
                "job-index contains incomplete entry {name}"
            )));
        }
        let Some(job_id) = name
            .strip_suffix(".json")
            .and_then(|value| value.parse::<JobId>().ok())
        else {
            return Err(upgrade_drain_required(
                "job-index contains an invalid job identity",
            ));
        };
        let disposition: JobDisposition = read_json_strict_at(index, &name).map_err(|_| {
            upgrade_drain_required("job-index record is unreadable or noncanonical")
        })?;
        disposition
            .validate()
            .map_err(|_| upgrade_drain_required("job-index record is noncanonical"))?;
        match &disposition {
            JobDisposition::Abandoned {
                job_id: indexed, ..
            } => {
                if *indexed != job_id {
                    return Err(upgrade_drain_required(
                        "abandoned index identity does not match its filename",
                    ));
                }
            }
            JobDisposition::Accepted {
                job_id: indexed,
                client_id,
                project_id,
                worktree_id,
                request_fingerprint,
                ..
            } => {
                if *indexed != job_id {
                    return Err(upgrade_drain_required(
                        "accepted index identity does not match its filename",
                    ));
                }
                let job = jobs
                    .open_child_directory(
                        &relative(&format!("{project_id}/{worktree_id}/{job_id}"))?,
                        false,
                    )
                    .map_err(|_| {
                        upgrade_drain_required(
                            "accepted index is missing its canonical job directory",
                        )
                    })?;
                let meta: JobMeta = read_json_strict_at(&job, "meta.json").map_err(|_| {
                    upgrade_drain_required("accepted job metadata is unreadable or noncanonical")
                })?;
                let status: JobStatus = read_json_strict_at(&job, "status.json").map_err(|_| {
                    upgrade_drain_required("accepted job status is unreadable or noncanonical")
                })?;
                if meta.job_id() != job_id
                    || meta.client_id() != *client_id
                    || meta.project_id() != project_id
                    || meta.worktree_id() != worktree_id
                    || meta.request_fingerprint() != request_fingerprint
                {
                    return Err(upgrade_drain_required(
                        "accepted index does not match its canonical job identity",
                    ));
                }
                if !status.state().is_terminal() {
                    return Err(upgrade_drain_required(&format!(
                        "accepted job {job_id} is not terminal; drain it on the previous helper"
                    )));
                }
            }
        }
    }
    Ok(())
}

fn promote_helper_binary(from: &Path, to: &Path) -> Result<(), WorkerError> {
    if !from.is_absolute() || !to.is_absolute() {
        return Err(WorkerError::Protocol(
            "upgrade helper paths must be absolute".into(),
        ));
    }
    std::fs::rename(from, to).map_err(WorkerError::Io)
}

fn restore_helper_binary(previous: Option<&Path>, to: &Path) -> Result<(), WorkerError> {
    if !to.is_absolute() {
        return Err(WorkerError::Protocol(
            "upgrade helper paths must be absolute".into(),
        ));
    }
    match previous {
        Some(previous) => {
            if !previous.is_absolute() {
                return Err(WorkerError::Protocol(
                    "upgrade helper paths must be absolute".into(),
                ));
            }
            std::fs::rename(previous, to).map_err(WorkerError::Io)
        }
        None => match std::fs::remove_file(to) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(WorkerError::Io(error)),
        },
    }
}

fn wait_upgrade_fence_hold() {
    #[cfg(test)]
    {
        UPGRADE_FENCE_HOLD.with(|slot| {
            if let Some(barrier) = slot.borrow().clone() {
                barrier.wait();
                barrier.wait();
            }
        });
    }
}

fn hex_16(bytes: [u8; 16]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod review_regression_tests {
    use super::*;
    use crate::job::{
        ClientId, CommandSpec, JobMeta, LeaseAcquireResponse, LeaseToken,
        PREVIOUS_STORED_PROTOCOL_VERSION, ProcessIdentity, RequestFingerprintMaterial,
    };
    use crate::lease::{AdmissionFacts, LeaseService};
    use crate::protocol::MemoryPressure;
    use std::{
        fs,
        os::unix::{
            ffi::OsStrExt,
            fs::{MetadataExt, PermissionsExt},
        },
        path::{Path, PathBuf},
        sync::{Arc, Barrier, mpsc},
        thread,
        time::Duration,
    };
    use tempfile::tempdir;

    fn installation_entries(parent: &Path) -> Vec<PathBuf> {
        let mut entries = fs::read_dir(parent)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(".mac-worker-installation-"))
            })
            .collect::<Vec<_>>();
        entries.sort();
        entries
    }

    fn bump_layout_entry_device(entry: &mut LayoutEntry) {
        entry.device = entry.device.wrapping_add(1);
    }

    fn stale_anchor_devices(parent: &Path, root: &Path) {
        let identity = installation_entries(parent)
            .into_iter()
            .find(|path| path.extension().is_some_and(|value| value == "json"))
            .expect("installation identity");
        let mut stored: HostInstallationIdentity =
            serde_json::from_slice(&fs::read(&identity).unwrap()).unwrap();
        bump_layout_entry_device(&mut stored.parent);
        bump_layout_entry_device(&mut stored.lock);
        bump_layout_entry_device(&mut stored.root);
        bump_layout_entry_device(&mut stored.layout);
        bump_layout_entry_device(&mut stored.identity_file);
        fs::write(&identity, serde_json::to_vec(&stored).unwrap()).unwrap();

        let layout_path = root.join(HOST_LAYOUT_FILE);
        let mut layout: HostLayoutIdentity =
            serde_json::from_slice(&fs::read(&layout_path).unwrap()).unwrap();
        bump_layout_entry_device(&mut layout.root);
        bump_layout_entry_device(&mut layout.layout_file);
        for entry in &mut layout.entries {
            bump_layout_entry_device(entry);
        }
        fs::write(&layout_path, serde_json::to_vec(&layout).unwrap()).unwrap();
    }

    fn legacy_v1_installation_names(parent: &Path, root: &Path) -> (String, String) {
        let metadata = fs::metadata(parent).unwrap();
        let root_component = root.file_name().unwrap();
        let mut digest = Sha256::new();
        digest.update(b"mac-worker-installation-v1\0");
        digest.update(metadata.dev().to_be_bytes());
        digest.update(metadata.ino().to_be_bytes());
        digest.update((root_component.as_bytes().len() as u64).to_be_bytes());
        digest.update(root_component.as_bytes());
        let key = format!("{:x}", digest.finalize());
        (
            format!("{INSTALLATION_PREFIX}{key}.lock"),
            format!("{INSTALLATION_PREFIX}{key}.json"),
        )
    }

    fn relocate_installation_to_legacy_names(parent: &Path, root: &Path) -> (PathBuf, PathBuf) {
        let entries = installation_entries(parent);
        let lock = entries
            .iter()
            .find(|path| path.extension().is_some_and(|value| value == "lock"))
            .unwrap()
            .clone();
        let identity = entries
            .iter()
            .find(|path| path.extension().is_some_and(|value| value == "json"))
            .unwrap()
            .clone();
        let (legacy_lock, legacy_identity) = legacy_v1_installation_names(parent, root);
        let legacy_lock = parent.join(legacy_lock);
        let legacy_identity = parent.join(legacy_identity);
        fs::rename(&lock, &legacy_lock).unwrap();
        fs::rename(&identity, &legacy_identity).unwrap();
        (legacy_lock, legacy_identity)
    }

    fn create_owner_only_job_tree(job_path: &Path) {
        fs::create_dir_all(job_path).unwrap();
        let mut current = job_path.to_path_buf();
        for _ in 0..3 {
            fs::set_permissions(&current, fs::Permissions::from_mode(0o700)).unwrap();
            current = current.parent().unwrap().to_path_buf();
        }
    }

    fn request(seed: u128) -> LeaseAcquireRequest {
        let material = RequestFingerprintMaterial::new(
            JobId::new(uuid::Uuid::from_u128(seed)),
            ClientId::new(uuid::Uuid::from_u128(seed + 10_000)),
            LeaseToken::new(uuid::Uuid::from_u128(seed + 20_000)),
            2,
            "mini-1".into(),
            "a".repeat(64),
            "b".repeat(64),
            "c".repeat(64),
            String::new(),
            60_000,
            "heavy".into(),
            CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
        )
        .unwrap();
        LeaseAcquireRequest::new(material)
    }

    #[test]
    fn empty_staging_tree_cannot_mint_a_publishable_workspace_receipt() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let request = request(1);
        let material = request.material();
        let mut staged = store
            .begin_job(
                material.project_id(),
                material.worktree_id(),
                material.job_id(),
            )
            .unwrap();
        assert!(
            staged
                .complete_snapshot_materialization(
                    &BTreeSet::new(),
                    material.project_id(),
                    material.worktree_id(),
                    material.manifest_digest(),
                )
                .is_err()
        );
        assert!(
            !store
                .job(
                    material.project_id(),
                    material.worktree_id(),
                    material.job_id()
                )
                .unwrap()
                .exists()
        );

        let payload = RelativePath::parse(b"payload").unwrap();
        staged
            .create_workspace_tree()
            .unwrap()
            .create_empty_directory(&payload)
            .unwrap();
        let receipt = staged
            .complete_snapshot_materialization(
                &BTreeSet::from([payload]),
                material.project_id(),
                material.worktree_id(),
                material.manifest_digest(),
            )
            .unwrap();
        staged.publish_complete(receipt).unwrap();
    }

    #[test]
    fn workspace_receipt_from_another_staging_nonce_cannot_publish() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let request = request(17);
        let material = request.material();
        let payload = RelativePath::parse(b"payload").unwrap();

        let first_receipt = {
            let mut first = store
                .begin_job(
                    material.project_id(),
                    material.worktree_id(),
                    material.job_id(),
                )
                .unwrap();
            first
                .create_workspace_tree()
                .unwrap()
                .create_empty_directory(&payload)
                .unwrap();
            first
                .complete_snapshot_materialization(
                    &BTreeSet::from([payload.clone()]),
                    material.project_id(),
                    material.worktree_id(),
                    material.manifest_digest(),
                )
                .unwrap()
        };

        let mut second = store
            .begin_job(
                material.project_id(),
                material.worktree_id(),
                material.job_id(),
            )
            .unwrap();
        second
            .create_workspace_tree()
            .unwrap()
            .create_empty_directory(&payload)
            .unwrap();
        drop(
            second
                .complete_snapshot_materialization(
                    &BTreeSet::from([payload]),
                    material.project_id(),
                    material.worktree_id(),
                    material.manifest_digest(),
                )
                .unwrap(),
        );

        let error = second.publish_complete(first_receipt).unwrap_err();
        assert!(error.to_string().contains("workspace receipt"));
        assert!(
            !store
                .job(
                    material.project_id(),
                    material.worktree_id(),
                    material.job_id()
                )
                .unwrap()
                .exists()
        );
    }

    #[test]
    fn staged_publication_fails_closed_when_nested_final_ancestor_is_swapped() {
        use std::os::unix::fs::symlink;

        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let outside = temp.path().join("outside");
        fs::create_dir(&outside).unwrap();
        let store = HostStore::open(&root).unwrap();
        let request = request(1);
        let material = request.material();
        let mut staged = store
            .begin_job(
                material.project_id(),
                material.worktree_id(),
                material.job_id(),
            )
            .unwrap();
        let payload = RelativePath::parse(b"payload").unwrap();
        staged
            .create_workspace_tree()
            .unwrap()
            .create_empty_directory(&payload)
            .unwrap();
        let receipt = staged
            .complete_snapshot_materialization(
                &BTreeSet::from([payload]),
                material.project_id(),
                material.worktree_id(),
                material.manifest_digest(),
            )
            .unwrap();
        let worktree = root
            .join("jobs")
            .join(material.project_id())
            .join(material.worktree_id());
        let detached = worktree.with_file_name(format!("{}-detached", material.worktree_id()));
        fs::rename(&worktree, &detached).unwrap();
        symlink(&outside, &worktree).unwrap();

        assert!(staged.publish_complete(receipt).is_err());
        assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
        assert!(!detached.join(material.job_id().to_string()).exists());
    }

    #[test]
    fn staged_publication_fails_closed_when_final_grandparent_is_relocated() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let request = request(1);
        let material = request.material();
        let mut staged = store
            .begin_job(
                material.project_id(),
                material.worktree_id(),
                material.job_id(),
            )
            .unwrap();
        let payload = RelativePath::parse(b"payload").unwrap();
        staged
            .create_workspace_tree()
            .unwrap()
            .create_empty_directory(&payload)
            .unwrap();
        let receipt = staged
            .complete_snapshot_materialization(
                &BTreeSet::from([payload]),
                material.project_id(),
                material.worktree_id(),
                material.manifest_digest(),
            )
            .unwrap();
        let project = root.join("jobs").join(material.project_id());
        let detached = temp.path().join("detached-project");
        fs::rename(&project, &detached).unwrap();
        fs::create_dir(&project).unwrap();
        fs::set_permissions(&project, fs::Permissions::from_mode(0o700)).unwrap();
        let replacement = project.join(material.worktree_id());
        fs::create_dir(&replacement).unwrap();
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(replacement.join("sentinel"), b"keep").unwrap();

        assert!(staged.publish_complete(receipt).is_err());
        assert!(
            !detached
                .join(material.worktree_id())
                .join(material.job_id().to_string())
                .exists()
        );
        assert!(!replacement.join(material.job_id().to_string()).exists());
        assert_eq!(fs::read(replacement.join("sentinel")).unwrap(), b"keep");
    }

    #[test]
    fn admission_lock_domain_cannot_split_after_jobs_namespace_replacement() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let first = HostStore::open(&root).unwrap();
        let second = HostStore::open(&root).unwrap();
        let job = request(1).material().job_id();
        let held = first.admission_lock(job).unwrap();
        fs::rename(root.join("locks/jobs"), root.join("locks/jobs-detached")).unwrap();
        fs::create_dir(root.join("locks/jobs")).unwrap();
        fs::set_permissions(root.join("locks/jobs"), fs::Permissions::from_mode(0o700)).unwrap();

        let (sender, receiver) = mpsc::channel();
        let contender = thread::spawn(move || {
            sender.send(second.admission_lock(job).is_err()).unwrap();
        });
        assert!(matches!(
            receiver.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        drop(held);
        assert!(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        contender.join().unwrap();
        assert_eq!(fs::read_dir(root.join("locks/jobs")).unwrap().count(), 0);
    }

    #[test]
    fn capacity_lock_domain_cannot_split_after_leases_namespace_replacement() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let first = HostStore::open(&root).unwrap();
        let second = HostStore::open(&root).unwrap();
        let held = first.capacity_lock().unwrap();
        fs::rename(root.join("leases"), root.join("leases-detached")).unwrap();
        fs::create_dir(root.join("leases")).unwrap();
        fs::set_permissions(root.join("leases"), fs::Permissions::from_mode(0o700)).unwrap();

        let (sender, receiver) = mpsc::channel();
        let contender = thread::spawn(move || {
            sender.send(second.capacity_lock().is_err()).unwrap();
        });
        assert!(matches!(
            receiver.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        drop(held);
        assert!(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        contender.join().unwrap();
        assert_eq!(fs::read_dir(root.join("leases")).unwrap().count(), 0);
    }

    #[test]
    fn capacity_lock_file_replacement_cannot_create_a_second_lock_domain() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let first = HostStore::open(&root).unwrap();
        let second = HostStore::open(&root).unwrap();
        let held = first.capacity_lock().unwrap();
        let lock = root.join("leases/capacity.lock");
        let detached = root.join("leases/capacity.lock-detached");
        fs::rename(&lock, &detached).unwrap();
        fs::write(&lock, b"").unwrap();
        fs::set_permissions(&lock, fs::Permissions::from_mode(0o600)).unwrap();

        let (sender, receiver) = mpsc::channel();
        let contender = thread::spawn(move || {
            sender.send(second.capacity_lock().is_err()).unwrap();
        });
        assert!(matches!(
            receiver.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        drop(held);
        assert!(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        contender.join().unwrap();
        assert!(detached.is_file());
        assert!(lock.is_file());
    }

    #[test]
    fn admission_lock_file_replacement_cannot_create_a_second_lock_domain() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let first = HostStore::open(&root).unwrap();
        let second = HostStore::open(&root).unwrap();
        let job = request(1).material().job_id();
        let held = first.admission_lock(job).unwrap();
        let lock = root
            .join("locks/jobs")
            .join(job.to_string())
            .join("admission.lock");
        let detached = lock.with_file_name("admission.lock-detached");
        fs::rename(&lock, &detached).unwrap();
        fs::write(&lock, b"").unwrap();
        fs::set_permissions(&lock, fs::Permissions::from_mode(0o600)).unwrap();

        let (sender, receiver) = mpsc::channel();
        let contender = thread::spawn(move || {
            sender.send(second.admission_lock(job).is_err()).unwrap();
        });
        assert!(matches!(
            receiver.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        drop(held);
        assert!(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        contender.join().unwrap();
        assert!(detached.is_file());
        assert!(lock.is_file());
    }

    #[test]
    fn opening_after_leases_replacement_rejects_the_new_namespace() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let first = HostStore::open(&root).unwrap();
        let held = first.capacity_lock().unwrap();
        fs::rename(root.join("leases"), root.join("leases-detached")).unwrap();
        fs::create_dir(root.join("leases")).unwrap();
        fs::set_permissions(root.join("leases"), fs::Permissions::from_mode(0o700)).unwrap();

        let (sender, receiver) = mpsc::channel();
        let contender_root = root.clone();
        let contender = thread::spawn(move || {
            sender
                .send(HostStore::open(&contender_root).is_err())
                .unwrap();
        });
        assert!(matches!(
            receiver.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        drop(held);
        assert!(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        contender.join().unwrap();
        assert_eq!(fs::read_dir(root.join("leases")).unwrap().count(), 0);
    }

    #[test]
    fn opening_after_jobs_lock_namespace_replacement_rejects_the_new_namespace() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let first = HostStore::open(&root).unwrap();
        let job = request(1).material().job_id();
        let held = first.admission_lock(job).unwrap();
        fs::rename(root.join("locks/jobs"), root.join("locks/jobs-detached")).unwrap();
        fs::create_dir(root.join("locks/jobs")).unwrap();
        fs::set_permissions(root.join("locks/jobs"), fs::Permissions::from_mode(0o700)).unwrap();

        let (sender, receiver) = mpsc::channel();
        let contender_root = root.clone();
        let contender = thread::spawn(move || {
            sender
                .send(HostStore::open(&contender_root).is_err())
                .unwrap();
        });
        assert!(matches!(
            receiver.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        drop(held);
        assert!(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        contender.join().unwrap();
        assert_eq!(fs::read_dir(root.join("locks/jobs")).unwrap().count(), 0);
    }

    #[test]
    fn installation_layout_mismatch_without_refresh_residue_is_refused() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let _store = HostStore::open(&root).unwrap();
        let identity = installation_entries(temp.path())
            .into_iter()
            .find(|path| path.extension().is_some_and(|value| value == "json"))
            .expect("installation identity");
        let layout_bytes = fs::read(root.join(HOST_LAYOUT_FILE)).unwrap();
        let mut stored: HostInstallationIdentity =
            serde_json::from_slice(&fs::read(&identity).unwrap()).unwrap();
        stored.layout.inode = stored.layout.inode.wrapping_add(1);
        let tampered = serde_json::to_vec(&stored).unwrap();
        fs::write(&identity, &tampered).unwrap();
        fs::set_permissions(&identity, fs::Permissions::from_mode(0o600)).unwrap();

        let error = match HostStore::open(&root) {
            Ok(_) => panic!("layout-inode mismatch without residue was refreshed"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("canonical host installation identity changed"),
            "unexpected error: {error:?}"
        );
        assert!(!root.join(HOST_LAYOUT_REFRESH_NAME).exists());
        assert_eq!(fs::read(&identity).unwrap(), tampered);
        assert_eq!(fs::read(root.join(HOST_LAYOUT_FILE)).unwrap(), layout_bytes);
    }

    #[test]
    fn copied_layout_bytes_cannot_bless_a_replacement_layout_file() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let _first = HostStore::open(&root).unwrap();
        let layout = root.join(HOST_LAYOUT_FILE);
        let bytes = fs::read(&layout).unwrap();
        fs::rename(&layout, root.join("layout-original.json")).unwrap();
        fs::write(&layout, &bytes).unwrap();
        fs::set_permissions(&layout, fs::Permissions::from_mode(0o600)).unwrap();

        assert!(HostStore::open(&root).is_err());
        assert_eq!(fs::read(&layout).unwrap(), bytes);
    }

    #[test]
    fn deleted_layout_is_never_regenerated_for_an_existing_root() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let _first = HostStore::open(&root).unwrap();
        fs::remove_file(root.join(HOST_LAYOUT_FILE)).unwrap();

        assert!(HostStore::open(&root).is_err());
        assert!(!root.join(HOST_LAYOUT_FILE).exists());
    }

    #[test]
    fn replacing_an_active_root_with_an_empty_root_cannot_form_a_second_domain() {
        use crate::protocol::MemoryPressure;
        use crate::{
            job::LeaseAcquireResponse,
            lease::{AdmissionFacts, LeaseService},
        };
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let first = HostStore::open(&root).unwrap();
        let request = request(1);
        assert!(matches!(
            LeaseService::new(&first)
                .acquire(
                    &request,
                    &AdmissionFacts {
                        free_disk_bytes: 200 * 1024 * 1024 * 1024,
                        total_disk_bytes: 500 * 1024 * 1024 * 1024,
                        memory_pressure: MemoryPressure::Normal,
                        swap_used_bytes: Some(0),
                    },
                    1,
                )
                .unwrap(),
            LeaseAcquireResponse::Acquired { .. }
        ));
        fs::rename(&root, temp.path().join("detached-host")).unwrap();
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();

        assert!(HostStore::open(&root).is_err());
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    }

    #[test]
    fn interrupted_first_layout_initialization_is_never_adopted_on_reopen() {
        for point in [
            HostStoreWritePoint::AfterHostRootCreate,
            HostStoreWritePoint::AfterHostNamespaces,
            HostStoreWritePoint::AfterHostCapacityLock,
        ] {
            let temp = tempdir().unwrap();
            let root = temp.path().join("host");

            assert!(HostStore::open_with_write_fault(&root, point).is_err());
            assert!(root.is_dir());
            assert!(!root.join(HOST_LAYOUT_FILE).exists());
            assert!(HostStore::open(&root).is_err());
            assert!(!root.join(HOST_LAYOUT_FILE).exists());
        }
    }

    #[test]
    fn layout_without_the_external_installation_identity_is_not_adopted() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");

        assert!(
            HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterHostLayoutPublish,)
                .is_err()
        );
        assert!(root.join(HOST_LAYOUT_FILE).is_file());
        assert!(HostStore::open(&root).is_err());
    }

    #[test]
    fn active_root_renamed_to_an_absent_canonical_path_cannot_form_a_second_domain() {
        use crate::lease::{AdmissionFacts, LeaseService};
        use crate::protocol::MemoryPressure;

        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let detached = temp.path().join("detached-host");
        let first = HostStore::open(&root).unwrap();
        LeaseService::new(&first)
            .acquire(
                &request(1),
                &AdmissionFacts {
                    free_disk_bytes: 200 * 1024 * 1024 * 1024,
                    total_disk_bytes: 500 * 1024 * 1024 * 1024,
                    memory_pressure: MemoryPressure::Normal,
                    swap_used_bytes: Some(0),
                },
                1,
            )
            .unwrap();
        fs::rename(&root, &detached).unwrap();

        assert!(HostStore::open(&root).is_err());
        assert!(!root.exists());
        assert!(detached.join("leases/slots/0/lease.json").is_file());
    }

    #[test]
    fn installation_anchor_deletion_replacement_and_copy_fail_closed() {
        use std::os::unix::fs::PermissionsExt;

        for attack in [
            "delete-lock",
            "delete-identity",
            "replace-lock",
            "copy-identity",
            "corrupt-identity",
            "unsafe-identity",
        ] {
            let temp = tempdir().unwrap();
            let root = temp.path().join("host");
            let _store = HostStore::open(&root).unwrap();
            let entries = installation_entries(temp.path());
            assert_eq!(entries.len(), 2);
            let lock = entries
                .iter()
                .find(|path| path.extension().is_some_and(|value| value == "lock"))
                .unwrap();
            let identity = entries
                .iter()
                .find(|path| path.extension().is_some_and(|value| value == "json"))
                .unwrap();

            match attack {
                "delete-lock" => fs::remove_file(lock).unwrap(),
                "delete-identity" => fs::remove_file(identity).unwrap(),
                "replace-lock" => {
                    let retained = temp.path().join("retained-lock");
                    fs::rename(lock, &retained).unwrap();
                    fs::write(lock, b"replacement").unwrap();
                    fs::set_permissions(lock, fs::Permissions::from_mode(0o600)).unwrap();
                }
                "copy-identity" => {
                    let retained = temp.path().join("retained-identity");
                    fs::rename(identity, &retained).unwrap();
                    fs::copy(&retained, identity).unwrap();
                    fs::set_permissions(identity, fs::Permissions::from_mode(0o600)).unwrap();
                }
                "corrupt-identity" => fs::write(identity, b"{}").unwrap(),
                "unsafe-identity" => {
                    fs::set_permissions(identity, fs::Permissions::from_mode(0o644)).unwrap();
                }
                _ => unreachable!(),
            }

            assert!(HostStore::open(&root).is_err(), "attack {attack}");
        }
    }

    #[test]
    fn concurrent_first_initializers_share_one_external_anchor() {
        use std::sync::{Arc, Barrier};

        let temp = tempdir().unwrap();
        let root = Arc::new(temp.path().join("host"));
        let barrier = Arc::new(Barrier::new(8));
        let threads = (0..8)
            .map(|_| {
                let root = Arc::clone(&root);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    HostStore::open(&root)
                        .map(drop)
                        .map_err(|error| format!("{error:?}"))
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().unwrap().unwrap();
        }

        assert_eq!(installation_entries(temp.path()).len(), 2);
        assert!(root.join(HOST_LAYOUT_FILE).is_file());
    }

    #[test]
    fn installation_initialization_crashes_are_never_implicitly_adopted() {
        for point in [
            HostStoreWritePoint::AfterInstallationLock,
            HostStoreWritePoint::AfterHostRootCreate,
            HostStoreWritePoint::AfterHostLayoutPublish,
            HostStoreWritePoint::BeforeInstallationIdentityPublish,
        ] {
            let temp = tempdir().unwrap();
            let root = temp.path().join("host");
            assert!(HostStore::open_with_write_fault(&root, point).is_err());
            assert!(HostStore::open(&root).is_err());
        }

        let before = tempdir().unwrap();
        let before_root = before.path().join("host");
        assert!(
            HostStore::open_with_write_fault(
                &before_root,
                HostStoreWritePoint::BeforeInstallationLock,
            )
            .is_err()
        );
        assert!(!before_root.exists());
        assert!(installation_entries(before.path()).is_empty());
        assert!(HostStore::open(&before_root).is_ok());

        let complete = tempdir().unwrap();
        let complete_root = complete.path().join("host");
        assert!(
            HostStore::open_with_write_fault(
                &complete_root,
                HostStoreWritePoint::AfterInstallationIdentityPublish,
            )
            .is_err()
        );
        assert!(HostStore::open(&complete_root).is_ok());
    }

    #[test]
    fn installation_key_survives_a_parent_device_change() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let _store = HostStore::open(&root).unwrap();
        let before = installation_entries(temp.path());
        assert_eq!(before.len(), 2);
        stale_anchor_devices(temp.path(), &root);
        if let Err(error) = HostStore::open(&root) {
            panic!("device-change open failed: {error:?}");
        }
        assert_eq!(installation_entries(temp.path()), before);
        let identity = before
            .iter()
            .find(|path| path.extension().is_some_and(|value| value == "json"))
            .unwrap();
        let stored: HostInstallationIdentity =
            serde_json::from_slice(&fs::read(identity).unwrap()).unwrap();
        let parent_device = fs::metadata(temp.path()).unwrap().dev() as u64;
        assert_eq!(stored.parent.device, parent_device);
        assert!(recorded_devices_uniform(installation_recorded_devices(
            &stored
        )));
    }

    #[test]
    fn reanchors_a_legacy_installation_identity() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let _store = HostStore::open(&root).unwrap();
        let v2 = installation_entries(temp.path());
        let (legacy_lock, legacy_identity) =
            relocate_installation_to_legacy_names(temp.path(), &root);
        assert!(legacy_lock.is_file());
        assert!(legacy_identity.is_file());
        if let Err(error) = HostStore::open(&root) {
            panic!("legacy re-anchor open failed: {error:?}");
        }
        assert_eq!(installation_entries(temp.path()), v2);
        assert!(!legacy_lock.exists());
        assert!(!legacy_identity.exists());
    }

    #[test]
    fn refuses_legacy_reanchor_when_a_stored_inode_differs() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let _store = HostStore::open(&root).unwrap();
        let (_legacy_lock, legacy_identity) =
            relocate_installation_to_legacy_names(temp.path(), &root);
        let mut stored: HostInstallationIdentity =
            serde_json::from_slice(&fs::read(&legacy_identity).unwrap()).unwrap();
        stored.root.inode = stored.root.inode.wrapping_add(1);
        fs::write(&legacy_identity, serde_json::to_vec(&stored).unwrap()).unwrap();
        let error = match HostStore::open(&root) {
            Ok(_) => panic!("re-anchor accepted a mismatched inode"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("host installation lock is absent")
        );
    }

    #[test]
    fn refuses_legacy_reanchor_when_two_legacy_candidates_match() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let _store = HostStore::open(&root).unwrap();
        let (_legacy_lock, legacy_identity) =
            relocate_installation_to_legacy_names(temp.path(), &root);
        let duplicate = temp
            .path()
            .join(format!("{INSTALLATION_PREFIX}{}.json", "a".repeat(64)));
        let bytes = fs::read(&legacy_identity).unwrap();
        fs::write(&duplicate, &bytes).unwrap();
        fs::set_permissions(&duplicate, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(is_installation_identity_name(
            duplicate.file_name().unwrap().to_str().unwrap()
        ));
        let error = match HostStore::open(&root) {
            Ok(_) => panic!("re-anchor accepted two matching candidates"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("host installation lock is absent"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn refresh_crashes_are_repaired_on_the_next_open() {
        for point in [
            HostStoreWritePoint::AfterHostLayoutRefreshUnlink,
            HostStoreWritePoint::AfterHostLayoutRefreshPublish,
            HostStoreWritePoint::AfterInstallationIdentityRefreshUnlink,
            HostStoreWritePoint::AfterInstallationIdentityRefreshPublish,
        ] {
            let temp = tempdir().unwrap();
            let root = temp.path().join("host");
            let _store = HostStore::open(&root).unwrap();
            stale_anchor_devices(temp.path(), &root);
            assert!(
                HostStore::open_with_write_fault(&root, point).is_err(),
                "fault at {point:?}"
            );
            if let Err(error) = HostStore::open(&root) {
                panic!("repair after {point:?} failed: {error:?}");
            }
            assert!(root.join(HOST_LAYOUT_FILE).is_file());
            assert!(!root.join(HOST_LAYOUT_REFRESH_NAME).exists());
            assert!(
                !temp
                    .path()
                    .join(INSTALLATION_IDENTITY_REFRESH_NAME)
                    .exists()
            );
        }
    }

    #[test]
    fn task7_publication_retains_final_descriptor_and_matching_admission() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let acquire = request(70);
        let submit = crate::job::SubmitRequest::new(acquire.material().clone());
        let material = submit.material();
        let admission = store.admission_lock(material.job_id()).unwrap();
        let mut staged = store
            .begin_job_after(
                admission,
                material.project_id(),
                material.worktree_id(),
                material.job_id(),
            )
            .unwrap();
        let payload = RelativePath::parse(b"payload").unwrap();
        staged
            .create_workspace_tree()
            .unwrap()
            .create_empty_directory(&payload)
            .unwrap();
        let receipt = staged
            .complete_snapshot_materialization(
                &BTreeSet::from([payload]),
                material.project_id(),
                material.worktree_id(),
                material.manifest_digest(),
            )
            .unwrap();

        let published = staged.publish_complete(receipt).unwrap();
        assert_eq!(published.job_id(), material.job_id());
        published.validate().unwrap();
        store
            .record_accepted_after(&published, &submit, &JobStatus::accepted(71).unwrap(), 71)
            .unwrap();
        assert!(store.job_index(material.job_id()).unwrap().is_file());
        published.validate().unwrap();
    }

    #[test]
    fn task7_supervisor_lock_is_nonblocking_and_survives_admission_release() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let job = request(71).material().job_id();
        let admission = store.admission_lock(job).unwrap();
        let supervisor = store
            .supervisor_lock_after(&admission, job, false)
            .unwrap()
            .unwrap();
        drop(admission);
        supervisor.validate().unwrap();

        let contender_store = store.clone();
        let (sender, receiver) = mpsc::channel();
        let contender = thread::spawn(move || {
            let admission = contender_store.admission_lock(job).unwrap();
            let won = contender_store
                .supervisor_lock_after(&admission, job, false)
                .unwrap()
                .is_some();
            sender.send(won).unwrap();
        });
        assert!(!receiver.recv_timeout(Duration::from_secs(2)).unwrap());
        contender.join().unwrap();

        drop(supervisor);
        let admission = store.admission_lock(job).unwrap();
        assert!(
            store
                .supervisor_lock_after(&admission, job, false)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn task7_status_capability_serializes_expected_value_writers() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let request = request(75);
        let material = request.material();
        let lease =
            LeaseRecord::new(material, request.request_fingerprint().clone(), 1, 60_001).unwrap();
        let job = store
            .open_directory(
                &format!(
                    "jobs/{}/{}/{}",
                    lease.project_id(),
                    lease.worktree_id(),
                    lease.job_id()
                ),
                true,
            )
            .unwrap();
        let initial = JobStatus::accepted(1).unwrap();
        let initial_bytes = serde_json::to_vec(&initial).unwrap();
        job.write_private_atomic_no_replace("status.json", &initial_bytes)
            .unwrap();

        let barrier = Arc::new(Barrier::new(2));
        let outcomes = thread::scope(|scope| {
            let mut handles = Vec::new();
            for (pid, start, updated) in [(7001, 70_001, 2), (7002, 70_002, 3)] {
                let store = store.clone();
                let lease = lease.clone();
                let barrier = Arc::clone(&barrier);
                let initial = initial.clone();
                let initial_bytes = initial_bytes.clone();
                handles.push(scope.spawn(move || {
                    let replacement = initial
                        .with_supervisor(ProcessIdentity::new(pid, start).unwrap(), updated)
                        .unwrap();
                    let job = store
                        .open_directory(
                            &format!(
                                "jobs/{}/{}/{}",
                                lease.project_id(),
                                lease.worktree_id(),
                                lease.job_id()
                            ),
                            false,
                        )
                        .unwrap();
                    barrier.wait();
                    let admission = store.admission_lock(lease.job_id()).unwrap();
                    let mut status = store
                        .supervisor_lock_after(&admission, lease.job_id(), true)
                        .unwrap()
                        .unwrap();
                    drop(admission);
                    let committed = store
                        .replace_job_status_after(
                            &mut status,
                            &lease,
                            &job,
                            &initial_bytes,
                            &initial,
                            &replacement,
                        )
                        .is_ok();
                    (replacement, committed)
                }));
            }
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });

        let winners = outcomes
            .iter()
            .filter(|(_, committed)| *committed)
            .map(|(replacement, _)| replacement)
            .collect::<Vec<_>>();
        assert_eq!(winners.len(), 1, "exactly one expected-value write commits");
        let canonical: JobStatus =
            read_json_strict_at(&job, "status.json").expect("canonical winner remains readable");
        assert_eq!(
            &canonical, winners[0],
            "the loser never replaces the winner"
        );
        assert!(
            job.list_names()
                .unwrap()
                .iter()
                .all(|name| !name.starts_with(b".replace-")),
            "conditional replacement residue remains"
        );
    }

    #[test]
    fn task7_supervisor_lock_rejects_directory_file_and_identity_replacements() {
        for (seed, mutation) in [(72, "directory"), (73, "lock"), (74, "identity")] {
            let temp = tempdir().unwrap();
            let root = temp.path().join("host");
            let store = HostStore::open(&root).unwrap();
            let job = request(seed).material().job_id();
            let admission = store.admission_lock(job).unwrap();
            let supervisor = store
                .supervisor_lock_after(&admission, job, false)
                .unwrap()
                .unwrap();
            drop(supervisor);
            drop(admission);

            let job_locks = root.join("locks/jobs");
            let supervisor_dir = job_locks.join(job.to_string()).join(SUPERVISOR_DIRECTORY);
            let lock = supervisor_dir.join(SUPERVISOR_LOCK_FILE);
            let identity = job_locks.join(format!("{job}{SUPERVISOR_IDENTITY_SUFFIX}"));
            match mutation {
                "directory" => {
                    fs::rename(&supervisor_dir, supervisor_dir.with_extension("retained")).unwrap();
                    fs::create_dir(&supervisor_dir).unwrap();
                    fs::set_permissions(&supervisor_dir, fs::Permissions::from_mode(0o700))
                        .unwrap();
                    fs::write(&lock, b"").unwrap();
                    fs::set_permissions(&lock, fs::Permissions::from_mode(0o600)).unwrap();
                }
                "lock" => {
                    fs::rename(&lock, lock.with_extension("retained")).unwrap();
                    fs::write(&lock, b"").unwrap();
                    fs::set_permissions(&lock, fs::Permissions::from_mode(0o600)).unwrap();
                }
                "identity" => {
                    let bytes = fs::read(&identity).unwrap();
                    fs::rename(&identity, identity.with_extension("retained")).unwrap();
                    fs::write(&identity, bytes).unwrap();
                    fs::set_permissions(&identity, fs::Permissions::from_mode(0o600)).unwrap();
                }
                _ => unreachable!(),
            }

            let admission = store.admission_lock(job).unwrap();
            assert!(
                store.supervisor_lock_after(&admission, job, false).is_err(),
                "{mutation} replacement formed a second supervisor lock domain"
            );
        }
    }

    #[test]
    fn transfer_and_supervisor_init_preserve_winner_inode_and_bytes() {
        for (directory, prefix, seed) in [
            ("transfer", ".transfer-init-", 90u128),
            ("supervisor", ".supervisor-init-", 91u128),
        ] {
            let temp = tempdir().unwrap();
            let root = temp.path().join("host");
            let store = HostStore::open(&root).unwrap();
            let job = request(seed).material().job_id();
            let admission = store.admission_lock(job).unwrap();
            if directory == "transfer" {
                drop(store.transfer_lock_after(&admission, job).unwrap());
            } else {
                drop(store.supervisor_lock_after(&admission, job, true).unwrap());
            }
            let job_locks = root.join("locks/jobs").join(job.to_string());
            let published = job_locks.join(directory);
            fs::write(published.join("winner-leaf"), b"winner-bytes").unwrap();
            let winner = job_locks.join(format!("{prefix}{}", "a".repeat(32)));
            fs::rename(&published, &winner).unwrap();
            let winner_meta = fs::symlink_metadata(&winner).unwrap();
            let loser = job_locks.join(format!("{prefix}{}", "b".repeat(32)));
            fs::create_dir(&loser).unwrap();
            fs::set_permissions(&loser, fs::Permissions::from_mode(0o700)).unwrap();
            fs::write(loser.join("loser-leaf"), b"loser-bytes").unwrap();
            fs::set_permissions(loser.join("loser-leaf"), fs::Permissions::from_mode(0o600))
                .unwrap();
            if directory == "transfer" {
                drop(store.transfer_lock_after(&admission, job).unwrap());
            } else {
                drop(store.supervisor_lock_after(&admission, job, true).unwrap());
            }
            assert!(published.is_dir(), "{directory}");
            assert!(!winner.exists(), "{directory}");
            assert!(!loser.exists(), "{directory}");
            let current = fs::symlink_metadata(&published).unwrap();
            assert_eq!(current.dev(), winner_meta.dev(), "{directory}");
            assert_eq!(current.ino(), winner_meta.ino(), "{directory}");
            assert_eq!(
                fs::read(published.join("winner-leaf")).unwrap(),
                b"winner-bytes",
                "{directory}"
            );
        }
    }

    #[test]
    fn resume_job_replace_stages_converges_canonical_pending() {
        use crate::{
            job::LeaseAcquireResponse,
            lease::{AdmissionFacts, LeaseService},
        };

        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let request = request(92);
        let lease = match LeaseService::new(&store)
            .acquire(
                &request,
                &AdmissionFacts {
                    free_disk_bytes: 200 * 1024 * 1024 * 1024,
                    total_disk_bytes: 500 * 1024 * 1024 * 1024,
                    memory_pressure: crate::protocol::MemoryPressure::Normal,
                    swap_used_bytes: Some(0),
                },
                1,
            )
            .unwrap()
        {
            LeaseAcquireResponse::Acquired { lease } => lease,
            LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
        };
        store.record_abandoned(&request, 2).unwrap();
        let job_path = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        create_owner_only_job_tree(&job_path);
        let replace = format!(
            "replace-{}",
            uuid::Uuid::from_u128(0x1111_4111_8111_1111_1111_1111_1111_1111).hyphenated()
        );
        fs::write(job_path.join(&replace), b"pending-replace").unwrap();
        fs::set_permissions(job_path.join(&replace), fs::Permissions::from_mode(0o600)).unwrap();
        drop(store);

        let faulted =
            HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterCleanupIntentCommit)
                .unwrap();
        let job = faulted
            .open_directory(
                &format!(
                    "jobs/{}/{}/{}",
                    lease.project_id(),
                    lease.worktree_id(),
                    lease.job_id()
                ),
                false,
            )
            .unwrap();
        assert_eq!(
            faulted
                .remove_owned_regular_committed(&job, &replace)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        assert!(!job_path.join(&replace).exists());
        let namespace = job_path.join(".mac-worker-rooted-fs");
        assert!(fs::read_dir(&namespace).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("cleanup-regular-v1-")
        }));
        drop(job);
        faulted.cleanup_job_owned(&lease).unwrap();
        assert!(!job_path.join(&replace).exists());
        if namespace.exists() {
            assert_eq!(fs::read_dir(&namespace).unwrap().count(), 0);
        }
        drop(faulted);
    }

    #[test]
    fn resume_job_replace_stages_preserves_unbound_public_replace() {
        use crate::{
            job::LeaseAcquireResponse,
            lease::{AdmissionFacts, LeaseService},
        };

        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let request = request(93);
        let lease = match LeaseService::new(&store)
            .acquire(
                &request,
                &AdmissionFacts {
                    free_disk_bytes: 200 * 1024 * 1024 * 1024,
                    total_disk_bytes: 500 * 1024 * 1024 * 1024,
                    memory_pressure: crate::protocol::MemoryPressure::Normal,
                    swap_used_bytes: Some(0),
                },
                1,
            )
            .unwrap()
        {
            LeaseAcquireResponse::Acquired { lease } => lease,
            LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
        };
        store.record_abandoned(&request, 2).unwrap();
        let job_path = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        create_owner_only_job_tree(&job_path);
        let replace = format!(
            "replace-{}",
            uuid::Uuid::from_u128(0x2222_4222_8222_2222_2222_2222_2222_2222).hyphenated()
        );
        let replace_path = job_path.join(&replace);
        fs::write(&replace_path, b"unbound-replace-bytes").unwrap();
        fs::set_permissions(&replace_path, fs::Permissions::from_mode(0o600)).unwrap();
        let before = fs::symlink_metadata(&replace_path).unwrap();
        let error = store.cleanup_job_owned(&lease).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unbound replace stage remains after cleanup recovery"),
            "{error}"
        );
        let after = fs::symlink_metadata(&replace_path).unwrap();
        assert_eq!(after.dev(), before.dev());
        assert_eq!(after.ino(), before.ino());
        assert_eq!(fs::read(&replace_path).unwrap(), b"unbound-replace-bytes");
        assert!(
            !root
                .join("locks/jobs")
                .join(lease.job_id().to_string())
                .join("cleanup-complete.json")
                .exists()
        );
        assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));
    }

    fn helper_pair(dir: &Path, name: &str) -> (PathBuf, PathBuf) {
        let from = dir.join(format!("{name}.new"));
        let to = dir.join(name);
        fs::write(&from, b"candidate-helper").unwrap();
        fs::write(&to, b"previous-helper").unwrap();
        (from, to)
    }

    fn layout_and_identity_inodes(parent: &Path, root: &Path) -> (u64, u64) {
        let layout = fs::symlink_metadata(root.join(HOST_LAYOUT_FILE))
            .unwrap()
            .ino();
        let identity = installation_entries(parent)
            .into_iter()
            .find(|path| path.extension().is_some_and(|value| value == "json"))
            .expect("installation identity");
        (layout, fs::symlink_metadata(&identity).unwrap().ino())
    }

    fn healthy_facts() -> AdmissionFacts {
        AdmissionFacts {
            free_disk_bytes: 200 * 1024 * 1024 * 1024,
            total_disk_bytes: 500 * 1024 * 1024 * 1024,
            memory_pressure: MemoryPressure::Normal,
            swap_used_bytes: Some(0),
        }
    }

    fn plant_cleanup_residue(parent: &Path) {
        fs::create_dir_all(parent).unwrap();
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).unwrap();
        let namespace = parent.join(".mac-worker-rooted-fs");
        fs::create_dir_all(&namespace).unwrap();
        fs::set_permissions(&namespace, fs::Permissions::from_mode(0o700)).unwrap();
        let leftover = namespace.join("cleanup-303f0f4a-6b5c-4d8e-9f00-112233445566");
        fs::create_dir(&leftover).unwrap();
        fs::set_permissions(&leftover, fs::Permissions::from_mode(0o700)).unwrap();
    }

    fn archive_terminal_v6_job(store: &HostStore, seed: u128) {
        let material = RequestFingerprintMaterial::from_stored(
            PREVIOUS_STORED_PROTOCOL_VERSION,
            JobId::new(uuid::Uuid::from_u128(seed)),
            ClientId::new(uuid::Uuid::from_u128(seed + 10_000)),
            LeaseToken::new(uuid::Uuid::from_u128(seed + 20_000)),
            2,
            "mini-1".into(),
            "a".repeat(64),
            "b".repeat(64),
            "c".repeat(64),
            String::new(),
            60_000,
            "heavy".into(),
            CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
        )
        .unwrap();
        let job_dir = store
            .open_directory(
                &format!(
                    "jobs/{}/{}/{}",
                    material.project_id(),
                    material.worktree_id(),
                    material.job_id()
                ),
                true,
            )
            .unwrap();
        let meta = JobMeta::new(&material, material.fingerprint()).unwrap();
        job_dir
            .write_new_private_file("meta.json", &serde_json::to_vec(&meta).unwrap())
            .unwrap();
        job_dir
            .write_new_private_file(
                "status.json",
                &serde_json::to_vec(&JobStatus::succeeded(10, 0, 0).unwrap()).unwrap(),
            )
            .unwrap();
        store
            .write_new_disposition(&JobDisposition::Accepted {
                job_id: material.job_id(),
                client_id: material.client_id(),
                project_id: material.project_id().into(),
                worktree_id: material.worktree_id().into(),
                request_fingerprint: material.fingerprint(),
                status: JobStatus::accepted(10).unwrap(),
                recorded_at_millis: 10,
            })
            .unwrap();
    }

    #[test]
    fn terminal_v6_accepted_index_is_idle_protocol_upgrade() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        archive_terminal_v6_job(&store, 300);
        drop(store);
        let (from, to) = helper_pair(temp.path(), "worker");
        let before = layout_and_identity_inodes(temp.path(), &root);
        HostStore::complete_protocol_upgrade(&root, &from, &to).unwrap();
        assert!(!from.exists());
        assert_eq!(fs::read(&to).unwrap(), b"candidate-helper");
        let after = layout_and_identity_inodes(temp.path(), &root);
        assert_ne!(before.0, after.0, "layout inode must change");
        assert_ne!(before.1, after.1, "installation identity inode must change");
        HostStore::open(&root).unwrap();
        HostStore::migrate_layout(&root).unwrap();
    }

    #[test]
    fn live_lease_blocks_protocol_upgrade_before_rename() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let request = request(301);
        match LeaseService::new(&store)
            .acquire(&request, &healthy_facts(), 1)
            .unwrap()
        {
            LeaseAcquireResponse::Acquired { .. } => {}
            LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
        }
        drop(store);
        let (from, to) = helper_pair(temp.path(), "worker");
        let error = HostStore::complete_protocol_upgrade(&root, &from, &to).unwrap_err();
        assert!(
            error.to_string().contains("HOST_UPGRADE_DRAIN_REQUIRED"),
            "{error}"
        );
        assert!(from.exists(), "candidate must remain when drain fails");
        assert_eq!(fs::read(&to).unwrap(), b"previous-helper");
    }

    #[test]
    fn incoming_and_accept_residue_block_protocol_upgrade() {
        for residue in ["incoming", "accept"] {
            let temp = tempdir().unwrap();
            let root = temp.path().join("host");
            let store = HostStore::open(&root).unwrap();
            if residue == "incoming" {
                store
                    .open_directory("incoming", false)
                    .unwrap()
                    .write_new_private_file(".partial", b"xfer")
                    .unwrap();
            } else {
                store
                    .open_directory("job-index", false)
                    .unwrap()
                    .write_new_private_file(".accept-deadbeef.json", b"{}")
                    .unwrap();
            }
            drop(store);
            let (from, to) = helper_pair(temp.path(), "worker");
            let error = HostStore::complete_protocol_upgrade(&root, &from, &to).unwrap_err();
            assert!(
                error.to_string().contains("HOST_UPGRADE_DRAIN_REQUIRED"),
                "{residue}: {error}"
            );
            assert!(from.exists(), "{residue}");
        }
    }

    #[test]
    fn nonterminal_accepted_job_blocks_protocol_upgrade() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let request = request(302);
        let material = request.material();
        let job_dir = store
            .open_directory(
                &format!(
                    "jobs/{}/{}/{}",
                    material.project_id(),
                    material.worktree_id(),
                    material.job_id()
                ),
                true,
            )
            .unwrap();
        let meta = JobMeta::new(material, request.request_fingerprint().clone()).unwrap();
        job_dir
            .write_new_private_file("meta.json", &serde_json::to_vec(&meta).unwrap())
            .unwrap();
        job_dir
            .write_new_private_file(
                "status.json",
                &serde_json::to_vec(&JobStatus::accepted(10).unwrap()).unwrap(),
            )
            .unwrap();
        store
            .write_new_disposition(&JobDisposition::Accepted {
                job_id: material.job_id(),
                client_id: material.client_id(),
                project_id: material.project_id().into(),
                worktree_id: material.worktree_id().into(),
                request_fingerprint: request.request_fingerprint().clone(),
                status: JobStatus::accepted(10).unwrap(),
                recorded_at_millis: 10,
            })
            .unwrap();
        drop(store);
        let (from, to) = helper_pair(temp.path(), "worker");
        let error = HostStore::complete_protocol_upgrade(&root, &from, &to).unwrap_err();
        assert!(
            error.to_string().contains("HOST_UPGRADE_DRAIN_REQUIRED"),
            "{error}"
        );
        assert!(from.exists());
    }

    #[test]
    fn stale_handle_fails_acquire_after_protocol_upgrade() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let (from, to) = helper_pair(temp.path(), "worker");
        let job = request(303).material().job_id();
        let held = store.admission_lock(job).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let upgrade_root = root.clone();
        let upgrade_barrier = barrier.clone();
        let upgrade = thread::spawn(move || {
            HostStore::with_upgrade_fence_hold(upgrade_barrier, || {
                HostStore::complete_protocol_upgrade(&upgrade_root, &from, &to)
            })
        });
        drop(held);
        barrier.wait();
        let acquire_store = store.clone();
        let acquire = thread::spawn(move || {
            LeaseService::new(&acquire_store).acquire(&request(304), &healthy_facts(), 1)
        });
        barrier.wait();
        upgrade.join().unwrap().unwrap();
        let error = acquire.join().unwrap().unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("canonical host layout record changed")
                || message.contains("canonical host installation record changed")
                || message.contains("Stale")
                || message.contains("stale"),
            "{message}"
        );
        let fresh = HostStore::open(&root).unwrap();
        match LeaseService::new(&fresh)
            .acquire(&request(304), &healthy_facts(), 1)
            .unwrap()
        {
            LeaseAcquireResponse::Acquired { .. } => {}
            LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
        }
    }

    #[test]
    fn absent_root_protocol_upgrade_initializes_under_the_construction_lock() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let (from, to) = helper_pair(temp.path(), "worker");
        HostStore::complete_protocol_upgrade(&root, &from, &to).unwrap();
        assert!(!from.exists());
        assert_eq!(fs::read(&to).unwrap(), b"candidate-helper");
        HostStore::open(&root).unwrap();
    }

    #[test]
    fn absent_root_unverified_rollback_fences_first_admission() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let previous = temp.path().join("worker.previous");
        let target = temp.path().join("worker");
        fs::write(&previous, b"previous-helper").unwrap();
        fs::write(&target, b"candidate-helper").unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let rollback_root = root.clone();
        let rollback_barrier = barrier.clone();
        let rollback_previous = previous.clone();
        let rollback_target = target.clone();
        let rollback = thread::spawn(move || {
            HostStore::with_upgrade_fence_hold(rollback_barrier, || {
                HostStore::with_published_layout_version(ROLLBACK_HELPER_LAYOUT_VERSION, || {
                    HostStore::complete_unverified_rollback(
                        &rollback_root,
                        Some(&rollback_previous),
                        &rollback_target,
                    )
                })
            })
        });
        barrier.wait();
        let open_root = root.clone();
        let opener = thread::spawn(move || HostStore::open(&open_root));
        barrier.wait();
        rollback.join().unwrap().unwrap();
        let store = opener.join().unwrap().unwrap();
        assert!(!previous.exists());
        assert_eq!(fs::read(&target).unwrap(), b"previous-helper");
        match LeaseService::new(&store)
            .acquire(&request(306), &healthy_facts(), 1)
            .unwrap()
        {
            LeaseAcquireResponse::Acquired { .. } => {}
            LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
        }
    }

    #[test]
    fn absent_root_rollback_refuses_layout3_previous2_and_keeps_new_helper() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let previous = temp.path().join("worker.previous");
        let target = temp.path().join("worker");
        fs::write(&previous, b"previous-helper").unwrap();
        fs::write(&target, b"candidate-helper").unwrap();
        // Combined SCHED case: live publish is layout 3 while previous helper
        // still only opens 2. The override is only the compatibility check.
        let error = HostStore::with_published_layout_version(3, || {
            HostStore::complete_unverified_rollback(&root, Some(&previous), &target)
        })
        .unwrap_err();
        assert!(
            error.to_string().contains("HOST_UPGRADE_ROLLBACK_UNSAFE"),
            "{error}"
        );
        assert!(previous.exists(), "previous helper must remain");
        assert_eq!(fs::read(&target).unwrap(), b"candidate-helper");
        let layout: HostLayoutIdentity =
            serde_json::from_slice(&fs::read(root.join(HOST_LAYOUT_FILE)).unwrap()).unwrap();
        assert_eq!(layout.version, HOST_LAYOUT_VERSION);
        HostStore::open(&root).unwrap();
    }

    #[test]
    fn unverified_rollback_restores_previous_when_drain_clean() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        HostStore::open(&root).unwrap();
        let layout_path = root.join(HOST_LAYOUT_FILE);
        let mut layout: HostLayoutIdentity =
            serde_json::from_slice(&fs::read(&layout_path).unwrap()).unwrap();
        layout.version = ROLLBACK_HELPER_LAYOUT_VERSION;
        fs::write(&layout_path, serde_json::to_vec(&layout).unwrap()).unwrap();
        let previous = temp.path().join("worker.previous");
        let target = temp.path().join("worker");
        fs::write(&previous, b"previous-helper").unwrap();
        fs::write(&target, b"candidate-helper").unwrap();
        HostStore::complete_unverified_rollback(&root, Some(&previous), &target).unwrap();
        assert!(!previous.exists());
        assert_eq!(fs::read(&target).unwrap(), b"previous-helper");
    }

    #[test]
    fn unverified_rollback_keeps_new_helper_when_work_remains() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        match LeaseService::new(&store)
            .acquire(&request(305), &healthy_facts(), 1)
            .unwrap()
        {
            LeaseAcquireResponse::Acquired { .. } => {}
            LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
        }
        drop(store);
        let previous = temp.path().join("worker.previous");
        let target = temp.path().join("worker");
        fs::write(&previous, b"previous-helper").unwrap();
        fs::write(&target, b"candidate-helper").unwrap();
        let error =
            HostStore::complete_unverified_rollback(&root, Some(&previous), &target).unwrap_err();
        assert!(
            error.to_string().contains("HOST_UPGRADE_ROLLBACK_UNSAFE"),
            "{error}"
        );
        assert!(previous.exists());
        assert_eq!(fs::read(&target).unwrap(), b"candidate-helper");
    }

    #[test]
    fn unverified_rollback_keeps_new_helper_when_layout_is_not_rollback_version() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        HostStore::open(&root).unwrap();
        let layout_path = root.join(HOST_LAYOUT_FILE);
        let mut layout: HostLayoutIdentity =
            serde_json::from_slice(&fs::read(&layout_path).unwrap()).unwrap();
        layout.version = 99;
        fs::write(&layout_path, serde_json::to_vec(&layout).unwrap()).unwrap();
        let previous = temp.path().join("worker.previous");
        let target = temp.path().join("worker");
        fs::write(&previous, b"previous-helper").unwrap();
        fs::write(&target, b"candidate-helper").unwrap();
        let error =
            HostStore::complete_unverified_rollback(&root, Some(&previous), &target).unwrap_err();
        assert!(
            error.to_string().contains("HOST_UPGRADE_ROLLBACK_UNSAFE"),
            "{error}"
        );
        assert_eq!(fs::read(&target).unwrap(), b"candidate-helper");
    }

    #[test]
    fn residual_observation_ignores_another_jobs_cleanup_tree() {
        use crate::failure_receipt::RESIDUAL_CLEANUP_TREE;

        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let request = request(94);
        let lease = match LeaseService::new(&store)
            .acquire(&request, &healthy_facts(), 1)
            .unwrap()
        {
            LeaseAcquireResponse::Acquired { lease } => lease,
            LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
        };
        let job_path = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        create_owner_only_job_tree(&job_path);

        let other = crate::job::JobId::new(uuid::Uuid::from_u128(0xaaaa_bbbb_cccc_dddd));
        plant_cleanup_residue(&root.join("incoming"));
        plant_cleanup_residue(&root.join("leases"));
        plant_cleanup_residue(&root.join("incoming").join(other.to_string()));
        plant_cleanup_residue(job_path.parent().unwrap());
        plant_cleanup_residue(
            &store
                .job(lease.project_id(), lease.worktree_id(), other)
                .unwrap(),
        );

        let job = store
            .open_directory(
                &format!(
                    "jobs/{}/{}/{}",
                    lease.project_id(),
                    lease.worktree_id(),
                    lease.job_id()
                ),
                false,
            )
            .unwrap();
        let observed = store.observe_residuals(Some(&lease), Some(&job));
        assert!(
            !observed.contains(&RESIDUAL_CLEANUP_TREE),
            "another job's leftover must not appear on this receipt: {observed:?}"
        );

        plant_cleanup_residue(&job_path);
        let observed = store.observe_residuals(Some(&lease), Some(&job));
        assert!(
            observed.contains(&RESIDUAL_CLEANUP_TREE),
            "this job's own leftover must appear: {observed:?}"
        );
    }
}
