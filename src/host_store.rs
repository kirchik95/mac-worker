use std::{
    ffi::CString,
    fmt,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::AsRawFd,
        unix::{
            ffi::OsStrExt,
            fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
        },
    },
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
    job::{
        ClientId, JobId, JobStatus, LeaseAcquireRequest, LeaseRecord, LeaseToken,
        RequestFingerprint,
    },
    rooted_fs::RootedDir,
};

const MAX_HOST_FILE_BYTES: u64 = 1024 * 1024;
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
}

#[derive(Clone)]
pub struct HostStore {
    inner: Arc<HostStoreInner>,
}

struct HostStoreInner {
    root: PathBuf,
    fault: AtomicU8,
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
    file: File,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

pub struct StagedJob {
    root: RootedDir,
    final_path: PathBuf,
    job_id: JobId,
    receipt_nonce: [u8; 16],
}

pub struct WorkspaceReceipt {
    job_id: JobId,
    nonce: [u8; 16],
}

#[derive(Clone)]
pub struct CleanupReceipt {
    job_id: JobId,
    client_id: ClientId,
    lease_token: LeaseToken,
    marker: PathBuf,
    proof: CleanupProof,
}

#[derive(Debug, Clone)]
enum CleanupProof {
    Terminal(PathBuf),
    Abandoned(PathBuf),
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CleanupMarker {
    job_id: JobId,
    client_id: ClientId,
    request_fingerprint: RequestFingerprint,
    terminal_or_abandoned: bool,
}

impl HostStore {
    pub fn open(root: &Path) -> Result<Self, WorkerError> {
        Self::open_inner(root, None)
    }

    #[doc(hidden)]
    pub fn open_with_write_fault(
        root: &Path,
        point: HostStoreWritePoint,
    ) -> Result<Self, WorkerError> {
        Self::open_inner(root, Some(point))
    }

    fn open_inner(root: &Path, point: Option<HostStoreWritePoint>) -> Result<Self, WorkerError> {
        if !root.is_absolute() {
            return Err(WorkerError::Protocol(
                "host data root must be absolute".into(),
            ));
        }
        let root_existed = match fs::symlink_metadata(root) {
            Ok(_) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        if !root_existed {
            create_private_tree(root)?;
        }
        if let Some(parent) = root.parent() {
            reject_symlink_or_wrong_type(parent, true)?;
        }
        reject_symlink_or_wrong_type(root, true)?;
        if root_existed {
            require_private_mode(root)?;
        } else {
            fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
        }
        for name in OWNED_DIRECTORIES {
            let path = root.join(name);
            create_owned_directory(&path)?;
        }
        create_owned_directory(&root.join("locks/jobs"))?;
        validate_owned_entries(root)?;
        sync_directory(root)?;
        Ok(Self {
            inner: Arc::new(HostStoreInner {
                root: root.to_path_buf(),
                fault: AtomicU8::new(point.map_or(0, |point| point as u8)),
            }),
        })
    }

    pub(crate) fn admission_lock(&self, job: JobId) -> Result<AdmissionGuard, WorkerError> {
        let directory = self.inner.root.join("locks/jobs").join(job.to_string());
        create_owned_directory(&directory)?;
        let file = open_private_regular(&directory.join("admission.lock"), true)?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if result != 0 {
            return Err(WorkerError::Io(std::io::Error::last_os_error()));
        }
        Ok(AdmissionGuard { file })
    }

    pub(crate) fn capacity_lock(&self) -> Result<AdmissionGuard, WorkerError> {
        let file = open_private_regular(&self.inner.root.join("leases/capacity.lock"), true)?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if result != 0 {
            return Err(WorkerError::Io(std::io::Error::last_os_error()));
        }
        Ok(AdmissionGuard { file })
    }

    pub fn incoming_job(&self, job: JobId, token: LeaseToken) -> Result<PathBuf, WorkerError> {
        self.validate_layout()?;
        Ok(self
            .inner
            .root
            .join("incoming")
            .join(job.to_string())
            .join(token.to_string()))
    }

    pub fn verified_receipt(&self, job: JobId) -> Result<PathBuf, WorkerError> {
        self.validate_layout()?;
        Ok(self.inner.root.join("verified").join(format!("{job}.json")))
    }

    pub fn job(&self, project: &str, worktree: &str, job: JobId) -> Result<PathBuf, WorkerError> {
        self.validate_layout()?;
        validate_digest(project, "project ID")?;
        validate_digest(worktree, "worktree ID")?;
        Ok(self
            .inner
            .root
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
            .root
            .join("snapshots")
            .join(project)
            .join(worktree)
            .join(digest))
    }

    pub fn job_index(&self, job: JobId) -> Result<PathBuf, WorkerError> {
        self.validate_layout()?;
        Ok(self
            .inner
            .root
            .join("job-index")
            .join(format!("{job}.json")))
    }

    pub fn begin_job(
        &self,
        project: &str,
        worktree: &str,
        job: JobId,
    ) -> Result<StagedJob, WorkerError> {
        let final_path = self.job(project, worktree, job)?;
        let nonce = *uuid::Uuid::new_v4().as_bytes();
        let staging_parent = fs::canonicalize(self.inner.root.join("leases"))?;
        let root =
            RootedDir::create(&staging_parent.join(format!(".job-{job}-{}", hex_16(nonce))))?;
        if let Some(parent) = final_path.parent() {
            create_owned_directory(parent)?;
        }
        let final_parent = fs::canonicalize(
            final_path
                .parent()
                .expect("validated final job path has a parent"),
        )?;
        let final_path = final_parent.join(job.to_string());
        Ok(StagedJob {
            root,
            final_path,
            job_id: job,
            receipt_nonce: nonce,
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
        let _guard = self.admission_lock(request.material().job_id())?;
        let disposition = JobDisposition::Accepted {
            job_id: request.material().job_id(),
            client_id: request.material().client_id(),
            project_id: request.material().project_id().into(),
            worktree_id: request.material().worktree_id().into(),
            request_fingerprint: request.request_fingerprint().clone(),
            status: status.clone(),
            recorded_at_millis: now,
        };
        self.write_new_disposition(&disposition)
    }

    pub fn record_abandoned(
        &self,
        request: &LeaseAcquireRequest,
        now: u64,
    ) -> Result<(), WorkerError> {
        request.validate()?;
        let _guard = self.admission_lock(request.material().job_id())?;
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
        self.write_new_disposition(&disposition)
    }

    pub(crate) fn disposition(&self, job: JobId) -> Result<Option<JobDisposition>, WorkerError> {
        let disposition: Option<JobDisposition> = read_json_optional(&self.job_index(job)?)?;
        if let Some(disposition) = &disposition {
            disposition.validate()?;
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
        let target = self.job_index(job)?;
        if target.exists() {
            let existing: JobDisposition = read_json_strict(&target)?;
            existing.validate()?;
            if same_disposition_identity(&existing, disposition) {
                return Ok(());
            }
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "job ID already has a permanent disposition",
            ));
        }
        atomic_write_new(&target, disposition)
    }

    #[doc(hidden)]
    pub fn record_terminal_status(
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
        let job_dir = self.job(lease.project_id(), lease.worktree_id(), lease.job_id())?;
        create_owned_directory(&job_dir)?;
        atomic_write_replace(&job_dir.join("status.json"), status)
    }

    #[doc(hidden)]
    pub fn record_cleanup_complete(
        &self,
        lease: &LeaseRecord,
    ) -> Result<CleanupReceipt, WorkerError> {
        lease.validate()?;
        let job_dir = self.job(lease.project_id(), lease.worktree_id(), lease.job_id())?;
        create_owned_directory(&job_dir)?;
        let status_path = job_dir.join("status.json");
        let proof = match read_json_optional::<JobStatus>(&status_path)? {
            Some(status) if status.state().is_terminal() => CleanupProof::Terminal(status_path),
            _ => {
                let disposition_path = self.job_index(lease.job_id())?;
                let expected_hash = format!(
                    "{:x}",
                    Sha256::digest(lease.lease_token().to_string().as_bytes())
                );
                match read_json_optional::<JobDisposition>(&disposition_path)? {
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
                        CleanupProof::Abandoned(disposition_path)
                    }
                    _ => {
                        return Err(WorkerError::Protocol(
                            "durable terminal-or-abandoned proof is required".into(),
                        ));
                    }
                }
            }
        };
        let marker = job_dir.join("cleanup-complete.json");
        atomic_write_replace(
            &marker,
            &CleanupMarker {
                job_id: lease.job_id(),
                client_id: lease.client_id(),
                request_fingerprint: lease.request_fingerprint().clone(),
                terminal_or_abandoned: true,
            },
        )?;
        Ok(CleanupReceipt {
            job_id: lease.job_id(),
            client_id: lease.client_id(),
            lease_token: lease.lease_token(),
            marker,
            proof,
        })
    }

    pub(crate) fn consume_fault(&self, point: HostStoreWritePoint) -> bool {
        self.inner
            .fault
            .compare_exchange(point as u8, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub(crate) fn lease_operation_path(&self, job: JobId) -> PathBuf {
        self.inner
            .root
            .join("leases")
            .join(format!(".acquire-{job}"))
    }

    pub(crate) fn live_lease_path(&self) -> PathBuf {
        self.inner.root.join("leases/heavy")
    }

    pub(crate) fn validate_layout(&self) -> Result<(), WorkerError> {
        reject_symlink_or_wrong_type(&self.inner.root, true)?;
        require_private_mode(&self.inner.root)?;
        validate_owned_entries(&self.inner.root)
    }
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

#[allow(dead_code)] // Task 6 consumes the opaque staging handle and receipt.
impl StagedJob {
    pub(crate) fn rooted_dir(&self) -> &RootedDir {
        &self.root
    }

    pub(crate) fn workspace_receipt(&self) -> WorkspaceReceipt {
        WorkspaceReceipt {
            job_id: self.job_id,
            nonce: self.receipt_nonce,
        }
    }

    pub(crate) fn publish_complete(mut self, receipt: WorkspaceReceipt) -> Result<(), WorkerError> {
        if receipt.job_id != self.job_id || receipt.nonce != self.receipt_nonce {
            return Err(WorkerError::Protocol(
                "workspace receipt does not match staged job".into(),
            ));
        }
        self.root.sync_root()?;
        if self.final_path.exists() {
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "final job path already exists",
            ));
        }
        self.root.publish_owned_to(&self.final_path)?;
        self.root.sync_parent()?;
        Ok(())
    }
}

impl CleanupReceipt {
    pub(crate) fn matches(&self, lease: &LeaseRecord) -> bool {
        self.job_id == lease.job_id()
            && self.client_id == lease.client_id()
            && self.lease_token == lease.lease_token()
    }

    pub(crate) fn validate_durable(&self, lease: &LeaseRecord) -> Result<(), WorkerError> {
        if !self.matches(lease) {
            return Err(WorkerError::Protocol(
                "cleanup receipt identity mismatch".into(),
            ));
        }
        let marker: CleanupMarker = read_json_strict(&self.marker)?;
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
            CleanupProof::Terminal(path) => {
                let status: JobStatus = read_json_strict(path)?;
                if !status.state().is_terminal() {
                    return Err(WorkerError::Protocol(
                        "terminal cleanup proof is not terminal".into(),
                    ));
                }
            }
            CleanupProof::Abandoned(path) => {
                let disposition: JobDisposition = read_json_strict(path)?;
                disposition.validate()?;
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

fn create_private_tree(path: &Path) -> Result<(), WorkerError> {
    if path.exists() {
        return Ok(());
    }
    let parent = path
        .parent()
        .ok_or_else(|| WorkerError::Protocol("host root has no parent".into()))?;
    if !parent.exists() {
        create_private_tree(parent)?;
    }
    reject_symlink_or_wrong_type(parent, true)?;
    match fs::create_dir(path) {
        Ok(()) => {
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
            sync_directory(parent)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    reject_symlink_or_wrong_type(path, true)
}

fn create_owned_directory(path: &Path) -> Result<(), WorkerError> {
    match fs::symlink_metadata(path) {
        Ok(_) => {
            reject_symlink_or_wrong_type(path, true)?;
            require_private_mode(path)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            create_private_tree(path)?;
            reject_symlink_or_wrong_type(path, true)?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn reject_symlink_or_wrong_type(path: &Path, directory: bool) -> Result<(), WorkerError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || (directory && !metadata.is_dir())
        || (!directory && !metadata.is_file())
    {
        return Err(WorkerError::Protocol(format!(
            "unsafe host-store component: {}",
            path.display()
        )));
    }
    if metadata.uid() != unsafe { libc::geteuid() } {
        return Err(WorkerError::Protocol(format!(
            "host-store component has the wrong owner: {}",
            path.display()
        )));
    }
    Ok(())
}

fn validate_owned_entries(root: &Path) -> Result<(), WorkerError> {
    for name in OWNED_DIRECTORIES {
        let path = root.join(name);
        reject_symlink_or_wrong_type(&path, true)?;
        require_private_mode(&path)?;
    }
    let jobs = root.join("locks/jobs");
    reject_symlink_or_wrong_type(&jobs, true)?;
    require_private_mode(&jobs)?;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            return Err(WorkerError::Protocol(
                "host data root contains a non-UTF-8 entry".into(),
            ));
        };
        if OWNED_DIRECTORIES.contains(&name.as_str()) {
            continue;
        }
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink()
            || (!metadata.is_file() && !metadata.is_dir())
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(WorkerError::Protocol(format!(
                "host data root contains an unsafe unrelated entry: {name}"
            )));
        }
    }
    Ok(())
}

fn require_private_mode(path: &Path) -> Result<(), WorkerError> {
    if fs::symlink_metadata(path)?.permissions().mode() & 0o077 != 0 {
        return Err(WorkerError::Protocol(format!(
            "host-store component is not owner-only: {}",
            path.display()
        )));
    }
    Ok(())
}

fn open_private_regular(path: &Path, create: bool) -> Result<File, WorkerError> {
    let existed = match fs::symlink_metadata(path) {
        Ok(_) => {
            reject_symlink_or_wrong_type(path, false)?;
            require_private_mode(path)?;
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    if !existed && !create {
        return Err(WorkerError::Io(std::io::Error::from(
            std::io::ErrorKind::NotFound,
        )));
    }
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    if !existed {
        options.create_new(true);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if !existed && create && error.kind() == std::io::ErrorKind::AlreadyExists => {
            return open_private_regular(path, false);
        }
        Err(error) => return Err(error.into()),
    };
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(WorkerError::Protocol(
            "host-store file is not regular".into(),
        ));
    }
    if !existed {
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

pub(crate) fn read_json_strict<T: DeserializeOwned + Serialize>(
    path: &Path,
) -> Result<T, WorkerError> {
    reject_symlink_or_wrong_type(path, false)?;
    require_private_mode(path)?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    if file.metadata()?.len() > MAX_HOST_FILE_BYTES {
        return Err(WorkerError::Protocol("host file exceeds 1 MiB".into()));
    }
    let mut bytes = Vec::new();
    file.take(MAX_HOST_FILE_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_HOST_FILE_BYTES {
        return Err(WorkerError::Protocol("host file exceeds 1 MiB".into()));
    }
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

fn read_json_optional<T: DeserializeOwned + Serialize>(
    path: &Path,
) -> Result<Option<T>, WorkerError> {
    match fs::symlink_metadata(path) {
        Ok(_) => read_json_strict(path).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn atomic_write_new<T: Serialize>(path: &Path, value: &T) -> Result<(), WorkerError> {
    atomic_write(path, value, false)
}

fn atomic_write_replace<T: Serialize>(path: &Path, value: &T) -> Result<(), WorkerError> {
    atomic_write(path, value, true)
}

fn atomic_write<T: Serialize>(path: &Path, value: &T, replace: bool) -> Result<(), WorkerError> {
    let parent = path
        .parent()
        .ok_or_else(|| WorkerError::Protocol("host file has no parent".into()))?;
    create_owned_directory(parent)?;
    let bytes = serde_json::to_vec(value).map_err(|error| {
        WorkerError::Protocol(format!("failed to serialize host JSON: {error}"))
    })?;
    if bytes.len() as u64 > MAX_HOST_FILE_BYTES {
        return Err(WorkerError::Protocol("host JSON exceeds 1 MiB".into()));
    }
    let temp = parent.join(format!(".write-{}", uuid::Uuid::new_v4().simple()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    if replace {
        fs::rename(&temp, path)?;
    } else if let Err(error) = rename_no_replace(&temp, path) {
        let _ = fs::remove_file(&temp);
        if error.to_string().contains("already exists")
            || matches!(&error, WorkerError::Io(io) if io.kind() == std::io::ErrorKind::AlreadyExists)
        {
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "host record already exists",
            ));
        }
        return Err(error);
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    sync_directory(parent)
}

pub(crate) fn sync_directory(path: &Path) -> Result<(), WorkerError> {
    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| WorkerError::Protocol("host path contains NUL".into()))?;
    let fd = unsafe {
        libc::open(
            c_path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        return Err(WorkerError::Io(std::io::Error::last_os_error()));
    }
    let result = unsafe { libc::fsync(fd) };
    let close_result = unsafe { libc::close(fd) };
    if result != 0 {
        return Err(WorkerError::Io(std::io::Error::last_os_error()));
    }
    if close_result != 0 {
        return Err(WorkerError::Io(std::io::Error::last_os_error()));
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

fn protocol_code(code: &'static str, message: &str) -> WorkerError {
    WorkerError::Protocol(format!("{code}: {message}"))
}

fn hex_16(bytes: [u8; 16]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn rename_no_replace(source: &Path, destination: &Path) -> Result<(), WorkerError> {
    let source = CString::new(source.as_os_str().as_bytes())
        .map_err(|_| WorkerError::Protocol("host source path contains NUL".into()))?;
    let destination = CString::new(destination.as_os_str().as_bytes())
        .map_err(|_| WorkerError::Protocol("host destination path contains NUL".into()))?;

    #[cfg(target_os = "linux")]
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    #[cfg(target_os = "macos")]
    let result = unsafe {
        unsafe extern "C" {
            fn renamex_np(
                from: *const libc::c_char,
                to: *const libc::c_char,
                flags: libc::c_uint,
            ) -> libc::c_int;
        }
        const RENAME_EXCL: libc::c_uint = 0x0000_0004;
        renamex_np(source.as_ptr(), destination.as_ptr(), RENAME_EXCL)
    };
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    let result = -1;

    if result == 0 {
        Ok(())
    } else {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        return Err(WorkerError::Protocol(
            "atomic no-replace publication is unsupported on this host".into(),
        ));
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        return Err(WorkerError::Io(std::io::Error::last_os_error()));
    }
}
