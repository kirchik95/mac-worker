use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    fs::File,
    os::{fd::AsRawFd, unix::ffi::OsStrExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::{
    error::WorkerError,
    inputs::RelativePath,
    job::{
        ClientId, JobId, JobStatus, LeaseAcquireRequest, LeaseRecord, LeaseToken,
        RequestFingerprint,
    },
    rooted_fs::{PrivateEntryIdentity, RootedDir},
};

const MAX_HOST_FILE_BYTES: u64 = 1024 * 1024;
const HOST_LAYOUT_VERSION: u32 = 1;
const HOST_INSTALLATION_VERSION: u32 = 1;
const INSTALLATION_PREFIX: &str = ".mac-worker-installation-";
const HOST_LAYOUT_FILE: &str = "layout.json";
const CAPACITY_LOCK_FILE: &str = "capacity.lock";
const ADMISSION_LOCK_FILE: &str = "admission.lock";
const OWNED_DIRECTORIES: &[&str] = &[
    "incoming",
    "verified",
    "jobs",
    "snapshots",
    "leases",
    "job-index",
    "locks",
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
        request_fingerprint: RequestFingerprint,
        lease_token_sha256: String,
        recorded_at_millis: u64,
    },
}

impl JobDisposition {
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
                if status.state() != crate::job::JobState::Accepted {
                    return Err(WorkerError::Protocol(
                        "accepted disposition requires accepted status".into(),
                    ));
                }
                Ok(())
            }
            Self::Abandoned {
                lease_token_sha256, ..
            } => validate_digest(lease_token_sha256, "lease token hash"),
        }
    }
}

pub struct AdmissionGuard {
    outer: Arc<InstallationGuard>,
    file: File,
    namespace: RootedDir,
    file_name: String,
    file_identity: PrivateEntryIdentity,
}

// Every host mutation follows this fixed order: the parent-anchored stable
// installation lock, then the per-job admission lock, then the heavy-slot
// capacity lock. Capacity-only integrity operations start at the installation
// anchor as well.
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

pub struct StagedJob {
    root: RootedDir,
    final_parent: RootedDir,
    final_name: String,
    job_id: JobId,
    receipt_nonce: [u8; 16],
    guard: AdmissionGuard,
}

pub struct WorkspaceReceipt {
    job_id: JobId,
    nonce: [u8; 16],
}

pub struct CleanupReceipt {
    root_identity: HostRootIdentity,
    job_id: JobId,
    client_id: ClientId,
    lease_token: LeaseToken,
    proof: CleanupProof,
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
    request_fingerprint: RequestFingerprint,
    terminal_or_abandoned: bool,
}

impl HostStore {
    pub fn open(root: &Path) -> Result<Self, WorkerError> {
        Self::open_inner(root, None, true)?
            .ok_or_else(|| WorkerError::Protocol("host installation was not initialized".into()))
    }

    pub(crate) fn open_if_present(root: &Path) -> Result<Option<Self>, WorkerError> {
        Self::open_inner(root, None, false)
    }

    #[doc(hidden)]
    pub fn open_with_write_fault(
        root: &Path,
        point: HostStoreWritePoint,
    ) -> Result<Self, WorkerError> {
        Self::open_inner(root, Some(point), true)?
            .ok_or_else(|| WorkerError::Protocol("host installation was not initialized".into()))
    }

    fn open_inner(
        root: &Path,
        point: Option<HostStoreWritePoint>,
        create: bool,
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
        let lock_present = parent.entry_exists(&names.lock)?;
        let identity_present = parent.entry_exists(&names.identity)?;
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
        if !initialize && (!root_present || !identity_present) {
            return Err(WorkerError::Protocol(
                "host installation is incomplete after coordination".into(),
            ));
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
        let initialized_layout = rooted.entry_exists(HOST_LAYOUT_FILE)?;
        if initialize == initialized_layout {
            return Err(WorkerError::Protocol(
                "host root and layout initialization state disagree".into(),
            ));
        }
        let namespaces = open_host_namespaces(&rooted, root_device, initialize)?;
        inject_open_fault(point, HostStoreWritePoint::AfterHostNamespaces)?;
        let leases = namespaces
            .get("leases")
            .expect("owned leases namespace was inserted");
        if initialize {
            let existed = leases.entry_exists(CAPACITY_LOCK_FILE)?;
            drop(leases.open_private_lock(CAPACITY_LOCK_FILE)?);
            if !existed {
                leases.sync_root()?;
            }
            inject_open_fault(point, HostStoreWritePoint::AfterHostCapacityLock)?;
        }
        let layout = if initialized_layout {
            let layout_file_identity = rooted.private_entry_identity(HOST_LAYOUT_FILE)?;
            let current_layout = build_host_layout(&rooted, &namespaces, layout_file_identity)?;
            let stored: HostLayoutIdentity = read_json_strict_at(&rooted, HOST_LAYOUT_FILE)?;
            validate_host_layout(&stored, &current_layout)?;
            stored
        } else {
            let layout_file_identity = rooted.write_private_atomic_no_replace_with_identity(
                HOST_LAYOUT_FILE,
                |layout_file_identity| {
                    let layout = build_host_layout(&rooted, &namespaces, layout_file_identity)
                        .map_err(worker_error_as_io)?;
                    serde_json::to_vec(&layout).map_err(|error| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, error)
                    })
                },
            )?;
            let current_layout = build_host_layout(&rooted, &namespaces, layout_file_identity)?;
            let stored: HostLayoutIdentity = read_json_strict_at(&rooted, HOST_LAYOUT_FILE)?;
            validate_host_layout(&stored, &current_layout)?;
            inject_open_fault(point, HostStoreWritePoint::AfterHostLayoutPublish)?;
            stored
        };
        let layout_file_identity = layout.layout_file.as_private();
        let (installation, installation_file_identity) = if initialize {
            inject_open_fault(
                point,
                HostStoreWritePoint::BeforeInstallationIdentityPublish,
            )?;
            let installation_file_identity = parent.write_private_atomic_no_replace_with_identity(
                &names.identity,
                |installation_file_identity| {
                    let installation = build_installation_identity(
                        &parent,
                        &names,
                        lock_identity,
                        &rooted,
                        layout_file_identity,
                        installation_file_identity,
                    )
                    .map_err(worker_error_as_io)?;
                    serde_json::to_vec(&installation).map_err(|error| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, error)
                    })
                },
            )?;
            let current = build_installation_identity(
                &parent,
                &names,
                lock_identity,
                &rooted,
                layout_file_identity,
                installation_file_identity,
            )?;
            let stored: HostInstallationIdentity = read_json_strict_at(&parent, &names.identity)?;
            validate_installation_identity(&stored, &current)?;
            inject_open_fault(point, HostStoreWritePoint::AfterInstallationIdentityPublish)?;
            (stored, installation_file_identity)
        } else {
            let installation_file_identity = parent.private_entry_identity(&names.identity)?;
            let current = build_installation_identity(
                &parent,
                &names,
                lock_identity,
                &rooted,
                layout_file_identity,
                installation_file_identity,
            )?;
            let stored: HostInstallationIdentity = read_json_strict_at(&parent, &names.identity)?;
            validate_installation_identity(&stored, &current)?;
            (stored, installation_file_identity)
        };
        parent.validate_private_regular_binding(
            &names.lock,
            &installation_lock,
            installation.lock.as_private(),
        )?;
        for bytes in rooted.list_names()? {
            let name = std::str::from_utf8(&bytes).map_err(|_| {
                WorkerError::Protocol("host data root contains a non-UTF-8 entry".into())
            })?;
            if !OWNED_DIRECTORIES.contains(&name) && name != HOST_LAYOUT_FILE {
                rooted.validate_private_entry(name)?;
            }
        }
        drop(installation_lock);
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
            }),
        };
        store.validate_layout()?;
        Ok(Some(store))
    }

    pub(crate) fn admission_lock(&self, job: JobId) -> Result<AdmissionGuard, WorkerError> {
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
        };
        guard.validate()?;
        Ok(guard)
    }

    #[allow(dead_code)] // Standalone capacity locking is exercised by host integrity tests.
    pub(crate) fn capacity_lock(&self) -> Result<AdmissionGuard, WorkerError> {
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
        };
        guard.validate()?;
        Ok(guard)
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
        let nonce = *uuid::Uuid::new_v4().as_bytes();
        let leases = self.open_directory("leases", false)?;
        let root = leases.create_new_child_directory(&format!(".job-{job}-{}", hex_16(nonce)))?;
        let final_parent = self.open_directory(&format!("jobs/{project}/{worktree}"), true)?;
        Ok(StagedJob {
            root,
            final_parent,
            final_name: job.to_string(),
            job_id: job,
            receipt_nonce: nonce,
            guard,
        })
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

    pub fn record_abandoned(
        &self,
        request: &LeaseAcquireRequest,
        now: u64,
    ) -> Result<(), WorkerError> {
        request.validate()?;
        let guard = self.admission_lock(request.material().job_id())?;
        let token_hash = format!(
            "{:x}",
            Sha256::digest(request.material().lease_token().to_string().as_bytes())
        );
        let disposition = JobDisposition::Abandoned {
            job_id: request.material().job_id(),
            client_id: request.material().client_id(),
            request_fingerprint: request.request_fingerprint().clone(),
            lease_token_sha256: token_hash,
            recorded_at_millis: now,
        };
        guard.validate()?;
        self.write_new_disposition(&disposition)
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
        atomic_write_at(&index, &name, disposition)
    }

    #[allow(dead_code)] // Task 7 lifecycle consumes this internal proof writer.
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

    #[allow(dead_code)] // Task 7 lifecycle consumes the only cleanup-receipt minting path.
    pub(crate) fn cleanup_job_owned(
        &self,
        lease: &LeaseRecord,
    ) -> Result<CleanupReceipt, WorkerError> {
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
                Some(status) if status.state().is_terminal() => Some(job_dir),
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
                        request_fingerprint,
                        lease_token_sha256,
                        ..
                    }) if job_id == lease.job_id()
                        && client_id == lease.client_id()
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
                request_fingerprint: lease.request_fingerprint().clone(),
                terminal_or_abandoned: true,
            },
            "cleanup marker",
        )?;
        Ok(CleanupReceipt {
            root_identity: self.root_identity()?,
            job_id: lease.job_id(),
            client_id: lease.client_id(),
            lease_token: lease.lease_token(),
            proof,
        })
    }

    pub(crate) fn consume_fault(&self, point: HostStoreWritePoint) -> bool {
        self.inner
            .fault
            .compare_exchange(point as u8, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
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
        if let Some(incoming) =
            self.open_optional_directory(&format!("incoming/{}", lease.job_id()))?
        {
            let token = lease.lease_token().to_string();
            if incoming.entry_exists(&token)? {
                incoming.remove_owned_child(&token)?;
            }
        }
        if let Some(job) = self.open_optional_directory(&format!(
            "jobs/{}/{}/{}",
            lease.project_id(),
            lease.worktree_id(),
            lease.job_id()
        ))? {
            for name in ["workspace", "home", "tmp"] {
                if job.entry_exists(name)? {
                    job.remove_owned_child(name)?;
                }
            }
            if job.entry_exists("execution.json")? {
                job.remove_owned_regular("execution.json")?;
            }
        }
        let leases = self.open_directory("leases", false)?;
        for name in leases.list_names()? {
            let name = std::str::from_utf8(&name).map_err(|_| {
                WorkerError::Protocol("lease namespace contains a non-UTF-8 entry".into())
            })?;
            let stage_prefix = format!(".job-{}-", lease.job_id());
            let exact_acquire = format!(".acquire-{}", lease.job_id());
            let exact_released = format!(".released-{}", lease.job_id());
            let owned = if let Some(suffix) = name.strip_prefix(&stage_prefix) {
                if !is_lower_hex(suffix, 32) {
                    return Err(WorkerError::Protocol(
                        "unsafe job-owned staging replacement".into(),
                    ));
                }
                true
            } else {
                name == exact_acquire || name == exact_released
            };
            if owned {
                leases.remove_owned_child(name)?;
            }
        }
        Ok(())
    }

    fn verify_job_mutable_scopes_absent(&self, lease: &LeaseRecord) -> Result<(), WorkerError> {
        if let Some(incoming) =
            self.open_optional_directory(&format!("incoming/{}", lease.job_id()))?
            && incoming.entry_exists(&lease.lease_token().to_string())?
        {
            return Err(WorkerError::Protocol(
                "incoming job scope remains after cleanup".into(),
            ));
        }
        if let Some(job) = self.open_optional_directory(&format!(
            "jobs/{}/{}/{}",
            lease.project_id(),
            lease.worktree_id(),
            lease.job_id()
        ))? {
            for name in ["workspace", "home", "tmp", "execution.json"] {
                if job.entry_exists(name)? {
                    return Err(WorkerError::Protocol(format!(
                        "mutable job scope {name} remains after cleanup"
                    )));
                }
            }
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
        Ok(())
    }

    fn open_optional_directory(&self, path: &str) -> Result<Option<RootedDir>, WorkerError> {
        match self.open_directory(path, false) {
            Ok(directory) => Ok(Some(directory)),
            Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
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
    digest.update(b"mac-worker-installation-v1\0");
    digest.update(parent_identity.device.to_be_bytes());
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
        WorkerError::Io(error) => error,
        error => std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()),
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
                request_fingerprint: left_fingerprint,
                lease_token_sha256: left_hash,
                ..
            },
            JobDisposition::Abandoned {
                job_id: right_job,
                client_id: right_client,
                request_fingerprint: right_fingerprint,
                lease_token_sha256: right_hash,
                ..
            },
        ) => {
            left_job == right_job
                && left_client == right_client
                && left_fingerprint == right_fingerprint
                && left_hash == right_hash
        }
        _ => false,
    }
}

fn disposition_job_id(disposition: &JobDisposition) -> JobId {
    match disposition {
        JobDisposition::Accepted { job_id, .. } | JobDisposition::Abandoned { job_id, .. } => {
            *job_id
        }
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

#[allow(dead_code)] // Task 6 consumes the opaque staging handle and receipt.
impl StagedJob {
    pub(crate) fn rooted_dir(&self) -> &RootedDir {
        &self.root
    }

    pub(crate) fn complete_materialization(
        &self,
        declared: &BTreeSet<RelativePath>,
    ) -> Result<WorkspaceReceipt, WorkerError> {
        self.root.validate_and_sync_declared_tree(declared)?;
        Ok(WorkspaceReceipt {
            job_id: self.job_id,
            nonce: self.receipt_nonce,
        })
    }

    pub(crate) fn publish_complete(mut self, receipt: WorkspaceReceipt) -> Result<(), WorkerError> {
        if receipt.job_id != self.job_id || receipt.nonce != self.receipt_nonce {
            return Err(WorkerError::Protocol(
                "workspace receipt does not match staged job".into(),
            ));
        }
        if self.final_parent.entry_exists(&self.final_name)? {
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "final job path already exists",
            ));
        }
        self.guard.validate()?;
        self.root
            .publish_owned_into(&self.final_parent, &self.final_name)?;
        self.final_parent.sync_root()?;
        Ok(())
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
        store.verify_job_mutable_scopes_absent(lease)?;
        let proof_dir = store.open_directory(&format!("locks/jobs/{}", lease.job_id()), false)?;
        let marker: CleanupMarker = read_json_strict_at(&proof_dir, "cleanup-complete.json")?;
        if !marker.terminal_or_abandoned
            || marker.job_id != lease.job_id()
            || marker.client_id != lease.client_id()
            || marker.request_fingerprint != *lease.request_fingerprint()
        {
            return Err(WorkerError::Protocol(
                "cleanup receipt is not durably valid".into(),
            ));
        }
        match &self.proof {
            CleanupProof::Terminal => {
                let job_dir = store.open_directory(
                    &format!(
                        "jobs/{}/{}/{}",
                        lease.project_id(),
                        lease.worktree_id(),
                        lease.job_id()
                    ),
                    false,
                )?;
                let status: JobStatus = read_json_strict_at(&job_dir, "status.json")?;
                if !status.state().is_terminal() {
                    return Err(WorkerError::Protocol(
                        "terminal cleanup proof is not terminal".into(),
                    ));
                }
            }
            CleanupProof::Abandoned => {
                let disposition = store
                    .disposition(lease.job_id())?
                    .ok_or_else(|| WorkerError::Protocol("abandonment proof is absent".into()))?;
                let expected_hash = format!(
                    "{:x}",
                    Sha256::digest(lease.lease_token().to_string().as_bytes())
                );
                if !matches!(disposition, JobDisposition::Abandoned { job_id, client_id, request_fingerprint, lease_token_sha256, .. }
                    if job_id == lease.job_id() && client_id == lease.client_id()
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

fn protocol_code(code: &'static str, message: &str) -> WorkerError {
    WorkerError::Protocol(format!("{code}: {message}"))
}

fn hex_16(bytes: [u8; 16]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod review_regression_tests {
    use super::*;
    use crate::job::{ClientId, CommandSpec, LeaseToken, RequestFingerprintMaterial};
    use std::{fs, sync::mpsc, thread, time::Duration};
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

    fn request(seed: u128) -> LeaseAcquireRequest {
        let material = RequestFingerprintMaterial::new(
            JobId::new(uuid::Uuid::from_u128(seed)),
            ClientId::new(uuid::Uuid::from_u128(seed + 10_000)),
            LeaseToken::new(uuid::Uuid::from_u128(seed + 20_000)),
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
        let staged = store
            .begin_job(
                material.project_id(),
                material.worktree_id(),
                material.job_id(),
            )
            .unwrap();
        assert!(staged.complete_materialization(&BTreeSet::new()).is_err());
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
            .rooted_dir()
            .create_empty_directory(&payload)
            .unwrap();
        let receipt = staged
            .complete_materialization(&BTreeSet::from([payload]))
            .unwrap();
        staged.publish_complete(receipt).unwrap();
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
        let staged = store
            .begin_job(
                material.project_id(),
                material.worktree_id(),
                material.job_id(),
            )
            .unwrap();
        let payload = RelativePath::parse(b"payload").unwrap();
        staged
            .rooted_dir()
            .create_empty_directory(&payload)
            .unwrap();
        let receipt = staged
            .complete_materialization(&BTreeSet::from([payload]))
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
        let staged = store
            .begin_job(
                material.project_id(),
                material.worktree_id(),
                material.job_id(),
            )
            .unwrap();
        let payload = RelativePath::parse(b"payload").unwrap();
        staged
            .rooted_dir()
            .create_empty_directory(&payload)
            .unwrap();
        let receipt = staged
            .complete_materialization(&BTreeSet::from([payload]))
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
        assert!(detached.join("leases/heavy/lease.json").is_file());
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
}
