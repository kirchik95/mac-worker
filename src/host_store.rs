use std::{
    collections::BTreeSet,
    fmt,
    fs::File,
    os::fd::AsRawFd,
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
    display_root: PathBuf,
    root: RootedDir,
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
    final_parent: RootedDir,
    final_name: String,
    job_id: JobId,
    receipt_nonce: [u8; 16],
}

pub struct WorkspaceReceipt {
    job_id: JobId,
    nonce: [u8; 16],
}

pub struct CleanupReceipt {
    store: HostStore,
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
        let rooted = match RootedDir::open(root) {
            Ok(rooted) => rooted,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => RootedDir::create(root)?,
            Err(error) => return Err(error.into()),
        };
        require_private_directory_metadata(&rooted.root_metadata()?)?;
        for name in OWNED_DIRECTORIES {
            let child = rooted.open_child_directory(&relative(name)?, true)?;
            require_private_directory_metadata(&child.root_metadata()?)?;
        }
        let locks = rooted.open_child_directory(&relative("locks")?, false)?;
        let jobs = locks.open_child_directory(&relative("jobs")?, true)?;
        require_private_directory_metadata(&jobs.root_metadata()?)?;
        for bytes in rooted.list_names()? {
            let name = std::str::from_utf8(&bytes).map_err(|_| {
                WorkerError::Protocol("host data root contains a non-UTF-8 entry".into())
            })?;
            if !OWNED_DIRECTORIES.contains(&name) {
                rooted.validate_private_entry(name)?;
            }
        }
        let store = Self {
            inner: Arc::new(HostStoreInner {
                display_root: root.to_path_buf(),
                root: rooted,
                fault: AtomicU8::new(point.map_or(0, |point| point as u8)),
            }),
        };
        store.validate_layout()?;
        Ok(store)
    }

    pub(crate) fn admission_lock(&self, job: JobId) -> Result<AdmissionGuard, WorkerError> {
        let directory = self.open_directory(&format!("locks/jobs/{job}"), true)?;
        let file = directory.open_private_lock("admission.lock")?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if result != 0 {
            return Err(WorkerError::Io(std::io::Error::last_os_error()));
        }
        Ok(AdmissionGuard { file })
    }

    pub(crate) fn capacity_lock(&self) -> Result<AdmissionGuard, WorkerError> {
        let leases = self.open_directory("leases", false)?;
        let file = leases.open_private_lock("capacity.lock")?;
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
        let job_dir = self.open_directory(
            &format!(
                "jobs/{}/{}/{}",
                lease.project_id(),
                lease.worktree_id(),
                lease.job_id()
            ),
            false,
        )?;
        write_json_once(&job_dir, "status.json", status, "terminal status")
    }

    #[allow(dead_code)] // Task 7 lifecycle consumes the only cleanup-receipt minting path.
    pub(crate) fn cleanup_job_owned(
        &self,
        lease: &LeaseRecord,
    ) -> Result<CleanupReceipt, WorkerError> {
        lease.validate()?;
        let _admission = self.admission_lock(lease.job_id())?;
        let _capacity = self.capacity_lock()?;
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
        self.remove_job_mutable_scopes(lease)?;
        self.verify_job_mutable_scopes_absent(lease)?;
        let proof_dir = self.open_directory(&format!("locks/jobs/{}", lease.job_id()), true)?;
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
            store: self.clone(),
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
        self.inner.root.verify_bound()?;
        require_private_directory_metadata(&self.inner.root.root_metadata()?)?;
        for name in OWNED_DIRECTORIES {
            let child = self.open_directory(name, false)?;
            require_private_directory_metadata(&child.root_metadata()?)?;
        }
        let jobs = self.open_directory("locks/jobs", false)?;
        require_private_directory_metadata(&jobs.root_metadata()?)
    }

    pub(crate) fn open_directory(
        &self,
        path: &str,
        create: bool,
    ) -> Result<RootedDir, WorkerError> {
        let path = relative(path)?;
        let directory = self.inner.root.open_child_directory(&path, create)?;
        require_private_directory_metadata(&directory.root_metadata()?)?;
        Ok(directory)
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
    pub(crate) fn validate_durable(&self, lease: &LeaseRecord) -> Result<(), WorkerError> {
        if !self.matches(lease) {
            return Err(WorkerError::Protocol(
                "cleanup receipt identity mismatch".into(),
            ));
        }
        self.store.verify_job_mutable_scopes_absent(lease)?;
        let proof_dir = self
            .store
            .open_directory(&format!("locks/jobs/{}", lease.job_id()), false)?;
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
                let job_dir = self.store.open_directory(
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
                let disposition = self
                    .store
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
    use std::fs;
    use tempfile::tempdir;

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
}
