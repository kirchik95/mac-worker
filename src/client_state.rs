use std::{
    ffi::{CStr, CString, OsStr},
    fs::File,
    io::{self, Read, Write},
    mem::MaybeUninit,
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
        unix::ffi::OsStrExt,
    },
    path::{Component, Path},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

use uuid::Uuid;

use crate::{
    error::WorkerError,
    job::{ClientId, JobId, JobState, LocalJobRecord},
};

const DIRECTORY_FLAGS: libc::c_int =
    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW;
const READ_FILE_FLAGS: libc::c_int =
    libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;
const MAX_STATE_FILE_BYTES: usize = 1024 * 1024;
const CLIENT_ID_NAME: &CStr = c"client-id";
const JOBS_NAME: &CStr = c"jobs";
const OPERATIONS_NAME: &CStr = c".mac-worker-state";
const LOCK_NAME: &CStr = c"jobs.lock";
const PAYLOAD_NAME: &CStr = c"payload";

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientStateWritePoint {
    BeforePublish = 1,
    AfterPublish = 2,
    SwapOperationPayloadBeforePublish = 3,
    SwapOperationDirectoryBeforeCleanup = 4,
    SwapLiveJobBeforeReplace = 5,
    SwapOperationDirectoryAfterValidationBeforeRemoval = 6,
    SwapOperationChildAfterValidationBeforeRemoval = 7,
    SwapPublishedRollbackAfterValidationBeforeRemoval = 8,
    SwapLiveJobWithSymlinkBeforeReplace = 9,
    SwapLiveJobWithDirectoryBeforeReplace = 10,
    SwapLiveJobWithFifoBeforeReplace = 11,
    SwapLiveJobWithPermissiveFileBeforeReplace = 12,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientStateCreationRacePoint {
    RootComponent = 1,
    OwnedDirectory = 2,
    LockFile = 3,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientStateSyncCounts {
    pub parent_directories: u64,
    pub root: u64,
    pub jobs: u64,
    pub concurrent_loser_parents: u64,
}

#[derive(Clone)]
pub struct ClientStateStore {
    inner: Arc<ClientStateInner>,
}

struct ClientStateInner {
    root: OwnedFd,
    jobs: OwnedFd,
    operations: OwnedFd,
    client_id: ClientId,
    write_fault: Arc<AtomicU8>,
    sync_counts: Arc<SyncCounters>,
}

#[derive(Default)]
struct SyncCounters {
    parent_directories: std::sync::atomic::AtomicU64,
    root: std::sync::atomic::AtomicU64,
    jobs: std::sync::atomic::AtomicU64,
    operations: std::sync::atomic::AtomicU64,
    concurrent_loser_parents: std::sync::atomic::AtomicU64,
}

impl ClientStateStore {
    pub fn open(state_root: &Path) -> Result<Self, WorkerError> {
        Self::open_inner(state_root, None, None)
    }

    #[doc(hidden)]
    pub fn open_with_write_fault(
        state_root: &Path,
        point: ClientStateWritePoint,
    ) -> Result<Self, WorkerError> {
        Self::open_inner(state_root, Some(point), None)
    }

    #[doc(hidden)]
    pub fn open_with_creation_race(
        state_root: &Path,
        point: ClientStateCreationRacePoint,
    ) -> Result<Self, WorkerError> {
        Self::open_inner(state_root, None, Some(point))
    }

    fn open_inner(
        state_root: &Path,
        initial_fault: Option<ClientStateWritePoint>,
        initial_creation_race: Option<ClientStateCreationRacePoint>,
    ) -> Result<Self, WorkerError> {
        let write_fault = Arc::new(AtomicU8::new(initial_fault.map_or(0, |point| point as u8)));
        let sync_counts = Arc::new(SyncCounters::default());
        let creation_race = AtomicU8::new(initial_creation_race.map_or(0, |point| point as u8));
        let root = open_or_create_root(state_root, &sync_counts, &creation_race)?;
        require_owned_directory(root.as_raw_fd())?;
        let jobs = open_or_create_owned_directory(
            root.as_raw_fd(),
            JOBS_NAME,
            &sync_counts,
            SyncKind::Root,
            &creation_race,
        )?;
        let operations = open_or_create_owned_directory(
            root.as_raw_fd(),
            OPERATIONS_NAME,
            &sync_counts,
            SyncKind::Root,
            &creation_race,
        )?;
        ensure_lock_file(root.as_raw_fd(), &sync_counts, &creation_race)?;
        validate_root_entries(root.as_raw_fd())?;
        validate_operation_entries(operations.as_raw_fd())?;
        let client_id = load_or_create_client_id(
            root.as_raw_fd(),
            operations.as_raw_fd(),
            &write_fault,
            &sync_counts,
        )?;

        Ok(Self {
            inner: Arc::new(ClientStateInner {
                root,
                jobs,
                operations,
                client_id,
                write_fault,
                sync_counts,
            }),
        })
    }

    pub fn client_id(&self) -> ClientId {
        self.inner.client_id
    }

    pub fn create_job(&self, record: LocalJobRecord) -> Result<(), WorkerError> {
        record.validate()?;
        self.require_local_client(&record)?;
        let _lock = StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
        let name = job_file_name(record.meta().job_id())?;

        if let Some(existing) = read_job_optional(self.inner.jobs.as_raw_fd(), &name)? {
            self.require_local_client(&existing)?;
            sync_counted(
                self.inner.jobs.as_raw_fd(),
                &self.inner.sync_counts,
                SyncKind::Jobs,
            )?;
            return require_same_immutable(&existing, &record);
        }

        let bytes = canonical_record_bytes(&record)?;
        let operation = OperationFile::stage(
            self.inner.operations.as_raw_fd(),
            &bytes,
            Arc::clone(&self.inner.sync_counts),
        )?;
        if self.take_fault(ClientStateWritePoint::BeforePublish) {
            return Err(injected_failure(ClientStateWritePoint::BeforePublish));
        }

        match operation.publish_no_replace(
            self.inner.jobs.as_raw_fd(),
            &name,
            &self.inner.write_fault,
        ) {
            Ok(()) => {}
            Err(WorkerError::Io(error)) if error.raw_os_error() == Some(libc::EEXIST) => {
                operation.cleanup(&self.inner.write_fault, &[])?;
                let existing = read_job(self.inner.jobs.as_raw_fd(), &name)?;
                self.require_local_client(&existing)?;
                sync_counted(
                    self.inner.jobs.as_raw_fd(),
                    &self.inner.sync_counts,
                    SyncKind::Jobs,
                )?;
                return require_same_immutable(&existing, &record);
            }
            Err(error) => return Err(error),
        }

        if self.take_fault(ClientStateWritePoint::AfterPublish) {
            return Err(injected_failure(ClientStateWritePoint::AfterPublish));
        }
        sync_counted(
            self.inner.jobs.as_raw_fd(),
            &self.inner.sync_counts,
            SyncKind::Jobs,
        )?;
        operation.cleanup(&self.inner.write_fault, &[])?;
        Ok(())
    }

    pub fn load_job(&self, job_id: JobId) -> Result<LocalJobRecord, WorkerError> {
        let name = job_file_name(job_id)?;
        let record = read_job(self.inner.jobs.as_raw_fd(), &name)?;
        if record.meta().job_id() != job_id {
            return Err(invalid_state("job filename and record identity differ"));
        }
        self.require_local_client(&record)?;
        Ok(record)
    }

    pub fn update_job(&self, replacement: LocalJobRecord) -> Result<(), WorkerError> {
        replacement.validate()?;
        self.require_local_client(&replacement)?;
        let _lock = StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
        let name = job_file_name(replacement.meta().job_id())?;
        let (existing, identity) = read_job_with_identity(self.inner.jobs.as_raw_fd(), &name)?;
        self.require_local_client(&existing)?;
        require_same_immutable(&existing, &replacement)?;
        require_forward_observation(&existing, &replacement)?;

        let bytes = canonical_record_bytes(&replacement)?;
        let operation = OperationFile::stage(
            self.inner.operations.as_raw_fd(),
            &bytes,
            Arc::clone(&self.inner.sync_counts),
        )?;
        if self.take_fault(ClientStateWritePoint::BeforePublish) {
            return Err(injected_failure(ClientStateWritePoint::BeforePublish));
        }
        operation.replace_if_identity(
            self.inner.jobs.as_raw_fd(),
            &name,
            identity,
            &self.inner.write_fault,
        )?;
        if self.take_fault(ClientStateWritePoint::AfterPublish) {
            return Err(injected_failure(ClientStateWritePoint::AfterPublish));
        }
        sync_counted(
            self.inner.jobs.as_raw_fd(),
            &self.inner.sync_counts,
            SyncKind::Jobs,
        )?;
        operation.cleanup(&self.inner.write_fault, &[identity])?;
        Ok(())
    }

    pub fn list_jobs(&self) -> Result<Vec<LocalJobRecord>, WorkerError> {
        let _lock = StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
        let mut entries = directory_entries(self.inner.jobs.as_raw_fd())?;
        entries.sort();
        entries
            .into_iter()
            .map(|entry| {
                let name = std::str::from_utf8(entry.to_bytes())
                    .map_err(|_| invalid_state("job registry contains a non-UTF-8 entry"))?;
                let id_text = name
                    .strip_suffix(".json")
                    .ok_or_else(|| invalid_state("job registry contains an unexpected entry"))?;
                let job_id = JobId::from_str(id_text)
                    .map_err(|_| invalid_state("job registry contains an invalid job filename"))?;
                let record = read_job(self.inner.jobs.as_raw_fd(), &entry)?;
                if record.meta().job_id() != job_id {
                    return Err(invalid_state("job filename and record identity differ"));
                }
                self.require_local_client(&record)?;
                Ok(record)
            })
            .collect()
    }

    #[doc(hidden)]
    pub fn inject_write_failure_once(&self, point: ClientStateWritePoint) {
        self.inner.write_fault.store(point as u8, Ordering::SeqCst);
    }

    #[doc(hidden)]
    pub fn durability_sync_counts(&self) -> ClientStateSyncCounts {
        ClientStateSyncCounts {
            parent_directories: self
                .inner
                .sync_counts
                .parent_directories
                .load(Ordering::SeqCst),
            root: self.inner.sync_counts.root.load(Ordering::SeqCst),
            jobs: self.inner.sync_counts.jobs.load(Ordering::SeqCst),
            concurrent_loser_parents: self
                .inner
                .sync_counts
                .concurrent_loser_parents
                .load(Ordering::SeqCst),
        }
    }

    fn require_local_client(&self, record: &LocalJobRecord) -> Result<(), WorkerError> {
        if record.meta().client_id() != self.inner.client_id {
            return Err(WorkerError::Protocol(
                "CLIENT_ID_MISMATCH: local job belongs to another client identity".into(),
            ));
        }
        Ok(())
    }

    fn take_fault(&self, point: ClientStateWritePoint) -> bool {
        self.inner
            .write_fault
            .compare_exchange(point as u8, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
}

fn require_same_immutable(
    existing: &LocalJobRecord,
    candidate: &LocalJobRecord,
) -> Result<(), WorkerError> {
    if existing.meta() == candidate.meta() && existing.lease_token() == candidate.lease_token() {
        Ok(())
    } else {
        Err(WorkerError::Protocol(
            "JOB_ID_CONFLICT: job ID is already bound to different immutable metadata".into(),
        ))
    }
}

fn require_forward_observation(
    existing: &LocalJobRecord,
    replacement: &LocalJobRecord,
) -> Result<(), WorkerError> {
    match (existing.last_status(), replacement.last_status()) {
        (None, _) => Ok(()),
        (Some(_), None) => Err(WorkerError::Protocol(
            "local job observation cannot be removed".into(),
        )),
        (Some(previous), Some(next)) if previous.state() == next.state() => {
            if next.updated_at_millis() < previous.updated_at_millis() {
                Err(WorkerError::Protocol(
                    "local job observation timestamp moved backwards".into(),
                ))
            } else if previous.state().is_terminal() && previous != next {
                Err(WorkerError::Protocol(
                    "terminal local job observation cannot be rewritten".into(),
                ))
            } else {
                Ok(())
            }
        }
        (Some(previous), Some(next)) => {
            if next.updated_at_millis() < previous.updated_at_millis() {
                return Err(WorkerError::Protocol(
                    "local job observation timestamp moved backwards".into(),
                ));
            }
            if observation_can_advance(previous.state(), next.state()) {
                Ok(())
            } else {
                Err(WorkerError::Protocol(
                    "local job observation moved backwards".into(),
                ))
            }
        }
    }
}

fn observation_can_advance(previous: JobState, next: JobState) -> bool {
    match previous {
        JobState::Uploading => next != JobState::Uploading,
        JobState::Verified => !matches!(next, JobState::Uploading | JobState::Verified),
        JobState::Accepted => !matches!(
            next,
            JobState::Uploading | JobState::Verified | JobState::Accepted
        ),
        JobState::Running => next.is_terminal(),
        JobState::Succeeded
        | JobState::Failed
        | JobState::Cancelled
        | JobState::TimedOut
        | JobState::Lost => false,
    }
}

fn load_or_create_client_id(
    root: RawFd,
    operations: RawFd,
    fault: &AtomicU8,
    sync_counts: &Arc<SyncCounters>,
) -> Result<ClientId, WorkerError> {
    match read_regular_optional(root, CLIENT_ID_NAME)? {
        Some((bytes, _)) => {
            let client_id = parse_client_id(&bytes)?;
            sync_counted(root, sync_counts, SyncKind::Root)?;
            Ok(client_id)
        }
        None => {
            let candidate = ClientId::generate();
            let bytes = format!("{candidate}\n").into_bytes();
            let operation = OperationFile::stage(operations, &bytes, Arc::clone(sync_counts))?;
            match operation.publish_no_replace(root, CLIENT_ID_NAME, fault) {
                Ok(()) => {
                    sync_counted(root, sync_counts, SyncKind::Root)?;
                    operation.cleanup(fault, &[])?;
                    Ok(candidate)
                }
                Err(WorkerError::Io(error)) if error.raw_os_error() == Some(libc::EEXIST) => {
                    operation.cleanup(fault, &[])?;
                    let (winner, _) = read_regular(root, CLIENT_ID_NAME)?;
                    let winner = parse_client_id(&winner)?;
                    sync_counted(root, sync_counts, SyncKind::Root)?;
                    Ok(winner)
                }
                Err(error) => Err(error),
            }
        }
    }
}

fn parse_client_id(bytes: &[u8]) -> Result<ClientId, WorkerError> {
    if bytes.len() != 33 || bytes.last() != Some(&b'\n') {
        return Err(invalid_state("client identity is not canonical"));
    }
    let text = std::str::from_utf8(&bytes[..32])
        .map_err(|_| invalid_state("client identity is not UTF-8"))?;
    ClientId::from_str(text).map_err(|_| invalid_state("client identity is invalid"))
}

fn canonical_record_bytes(record: &LocalJobRecord) -> Result<Vec<u8>, WorkerError> {
    let mut bytes = serde_json::to_vec(record)
        .map_err(|_| invalid_state("local job record cannot be serialized"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn parse_record(bytes: &[u8]) -> Result<LocalJobRecord, WorkerError> {
    if bytes.len() < 2 || bytes.last() != Some(&b'\n') || bytes[..bytes.len() - 1].contains(&b'\n')
    {
        return Err(invalid_state("local job record is not canonical JSON"));
    }
    let record: LocalJobRecord = serde_json::from_slice(&bytes[..bytes.len() - 1])
        .map_err(|_| invalid_state("local job record is corrupt"))?;
    if canonical_record_bytes(&record)? != bytes {
        return Err(invalid_state("local job record is not canonical JSON"));
    }
    Ok(record)
}

fn read_job(directory: RawFd, name: &CStr) -> Result<LocalJobRecord, WorkerError> {
    read_job_with_identity(directory, name).map(|(record, _)| record)
}

fn read_job_optional(directory: RawFd, name: &CStr) -> Result<Option<LocalJobRecord>, WorkerError> {
    match read_regular_optional(directory, name)? {
        Some((bytes, _)) => parse_record(&bytes).map(Some),
        None => Ok(None),
    }
}

fn read_job_with_identity(
    directory: RawFd,
    name: &CStr,
) -> Result<(LocalJobRecord, FileIdentity), WorkerError> {
    let (bytes, identity) = read_regular(directory, name)?;
    Ok((parse_record(&bytes)?, identity))
}

fn job_file_name(job_id: JobId) -> Result<CString, WorkerError> {
    CString::new(format!("{job_id}.json"))
        .map_err(|_| invalid_state("job ID produced an invalid filename"))
}

struct OperationFile {
    operations: RawFd,
    name: CString,
    directory: OwnedFd,
    directory_identity: FileIdentity,
    payload: OwnedFd,
    payload_identity: FileIdentity,
    expected_bytes: Vec<u8>,
    sync_counts: Arc<SyncCounters>,
}

impl OperationFile {
    fn stage(
        operations: RawFd,
        bytes: &[u8],
        sync_counts: Arc<SyncCounters>,
    ) -> Result<Self, WorkerError> {
        let name = CString::new(Uuid::new_v4().simple().to_string())
            .map_err(|_| invalid_state("operation identity is invalid"))?;
        mkdir_at(operations, &name, 0o700)?;
        sync_counted(operations, &sync_counts, SyncKind::Operations)?;
        let directory = match open_directory_at(operations, &name) {
            Ok(directory) => directory,
            Err(error) => {
                let _ = unlink_at(operations, &name, libc::AT_REMOVEDIR);
                return Err(WorkerError::Io(error));
            }
        };
        require_owned_directory(directory.as_raw_fd())?;
        let directory_identity = FileIdentity::from_stat(stat_fd(directory.as_raw_fd())?);
        let descriptor = create_regular_at(directory.as_raw_fd(), PAYLOAD_NAME)?;
        let mut file = File::from(descriptor);
        file.write_all(bytes)?;
        file.sync_all()?;
        let payload = OwnedFd::from(file);
        let payload_identity = FileIdentity::from_stat(require_owned_regular(
            payload.as_raw_fd(),
            MAX_STATE_FILE_BYTES,
        )?);
        sync_counted(directory.as_raw_fd(), &sync_counts, SyncKind::Operations)?;
        Ok(Self {
            operations,
            name,
            directory,
            directory_identity,
            payload,
            payload_identity,
            expected_bytes: bytes.to_vec(),
            sync_counts,
        })
    }

    fn publish_no_replace(
        &self,
        destination_parent: RawFd,
        destination: &CStr,
        fault: &AtomicU8,
    ) -> Result<(), WorkerError> {
        let retained_identity = FileIdentity::from_stat(stat_fd(self.payload.as_raw_fd())?);
        if retained_identity != self.payload_identity {
            return Err(invalid_state("retained staged payload identity changed"));
        }
        if take_fault(
            fault,
            ClientStateWritePoint::SwapOperationPayloadBeforePublish,
        ) {
            self.inject_payload_swap()?;
        }
        if fault.load(Ordering::SeqCst)
            == ClientStateWritePoint::SwapPublishedRollbackAfterValidationBeforeRemoval as u8
        {
            self.inject_retained_payload_corruption()?;
        }
        link_no_replace(
            self.directory.as_raw_fd(),
            PAYLOAD_NAME,
            destination_parent,
            destination,
        )?;
        let published = open_regular_at(destination_parent, destination)?;
        let (published_bytes, published_identity) = read_open_regular(published)?;
        if published_identity != self.payload_identity || published_bytes != self.expected_bytes {
            if published_identity == self.payload_identity {
                remove_entry_if_identity(
                    destination_parent,
                    destination,
                    self.payload_identity,
                    fault,
                    &self.sync_counts,
                )?;
            }
            return Err(invalid_state(
                "staged payload identity or bytes changed before publication",
            ));
        }
        Ok(())
    }

    fn replace_if_identity(
        &self,
        destination_parent: RawFd,
        destination: &CStr,
        expected: FileIdentity,
        fault: &AtomicU8,
    ) -> Result<(), WorkerError> {
        if let Some(point) = take_live_job_swap_fault(fault) {
            inject_live_job_swap(destination_parent, destination, point)?;
        }
        exchange_entries(
            self.directory.as_raw_fd(),
            PAYLOAD_NAME,
            destination_parent,
            destination,
        )?;

        let displaced_stat = stat_at(self.directory.as_raw_fd(), PAYLOAD_NAME).ok();
        if displaced_stat.is_some_and(|stat| {
            self.exchange_state_is_valid(destination_parent, destination, expected, stat)
        }) {
            return Ok(());
        }

        self.rollback_exchange(
            destination_parent,
            destination,
            displaced_stat.map(PathIdentity::from_stat),
        )?;
        Err(invalid_state(
            "job state changed during conditional atomic replacement",
        ))
    }

    fn exchange_state_is_valid(
        &self,
        destination_parent: RawFd,
        destination: &CStr,
        expected: FileIdentity,
        displaced_stat: libc::stat,
    ) -> bool {
        if !is_owned_regular_stat(&displaced_stat, MAX_STATE_FILE_BYTES)
            || FileIdentity::from_stat(displaced_stat) != expected
        {
            return false;
        }
        let displaced = match open_regular_at(self.directory.as_raw_fd(), PAYLOAD_NAME) {
            Ok(displaced) => displaced,
            Err(_) => return false,
        };
        let displaced_open =
            match require_owned_regular(displaced.as_raw_fd(), MAX_STATE_FILE_BYTES) {
                Ok(stat) => stat,
                Err(_) => return false,
            };
        if FileIdentity::from_stat(displaced_open) != expected {
            return false;
        }
        let published = match open_regular_at(destination_parent, destination) {
            Ok(published) => published,
            Err(_) => return false,
        };
        match read_open_regular(published) {
            Ok((bytes, identity)) => {
                identity == self.payload_identity && bytes == self.expected_bytes
            }
            Err(_) => false,
        }
    }

    fn rollback_exchange(
        &self,
        destination_parent: RawFd,
        destination: &CStr,
        displaced: Option<PathIdentity>,
    ) -> Result<(), WorkerError> {
        exchange_entries(
            self.directory.as_raw_fd(),
            PAYLOAD_NAME,
            destination_parent,
            destination,
        )
        .map_err(|_| invalid_state("conditional replacement rollback failed"))?;
        sync_counted(destination_parent, &self.sync_counts, SyncKind::Jobs)
            .map_err(|_| invalid_state("conditional replacement rollback fsync failed"))?;
        sync_counted(
            self.directory.as_raw_fd(),
            &self.sync_counts,
            SyncKind::Operations,
        )
        .map_err(|_| invalid_state("conditional replacement rollback fsync failed"))?;
        let restored = stat_at(destination_parent, destination)
            .map(PathIdentity::from_stat)
            .map_err(|_| invalid_state("conditional replacement rollback verification failed"))?;
        if displaced != Some(restored) {
            return Err(invalid_state(
                "job replacement rollback did not restore the displaced entry",
            ));
        }
        Ok(())
    }

    fn cleanup(self, fault: &AtomicU8, extra_owned: &[FileIdentity]) -> Result<(), WorkerError> {
        if take_fault(
            fault,
            ClientStateWritePoint::SwapOperationDirectoryBeforeCleanup,
        ) {
            self.inject_directory_swap()?;
        }

        let (namespace_name, namespace) =
            create_private_directory(self.operations, &self.sync_counts, SyncKind::Operations)?;
        let acquired_name = c"operation";
        rename_no_replace(
            self.operations,
            &self.name,
            namespace.as_raw_fd(),
            acquired_name,
        )?;
        let acquired = open_directory_at(namespace.as_raw_fd(), acquired_name)?;
        let acquired_identity = FileIdentity::from_stat(stat_fd(acquired.as_raw_fd())?);
        if acquired_identity != self.directory_identity {
            restore_quarantine(
                namespace.as_raw_fd(),
                acquired_name,
                self.operations,
                &self.name,
            )?;
            remove_empty_directory_if_identity(self.operations, &namespace_name, &namespace)?;
            return Err(invalid_state(
                "operation directory changed before identity-bound cleanup",
            ));
        }

        if take_fault(
            fault,
            ClientStateWritePoint::SwapOperationDirectoryAfterValidationBeforeRemoval,
        ) {
            inject_directory_swap_at(
                namespace.as_raw_fd(),
                acquired_name,
                c"post-validation-directory-sentinel",
            )?;
        }

        let (retired_name, retired) = create_private_directory(
            namespace.as_raw_fd(),
            &self.sync_counts,
            SyncKind::Operations,
        )?;
        self.cleanup_owned_payload_links(
            acquired.as_raw_fd(),
            retired.as_raw_fd(),
            extra_owned,
            fault,
        )?;
        if !directory_entries(acquired.as_raw_fd())?.is_empty() {
            return Err(invalid_state(
                "operation directory contains substituted entries",
            ));
        }
        rename_no_replace(
            namespace.as_raw_fd(),
            acquired_name,
            retired.as_raw_fd(),
            acquired_name,
        )?;
        let final_object = open_directory_at(retired.as_raw_fd(), acquired_name)?;
        if FileIdentity::from_stat(stat_fd(final_object.as_raw_fd())?) != self.directory_identity {
            restore_quarantine(
                retired.as_raw_fd(),
                acquired_name,
                namespace.as_raw_fd(),
                acquired_name,
            )?;
            return Err(invalid_state(
                "operation directory changed before private retirement",
            ));
        }
        unlink_at(retired.as_raw_fd(), acquired_name, libc::AT_REMOVEDIR)?;
        sync_counted(retired.as_raw_fd(), &self.sync_counts, SyncKind::Operations)?;
        remove_empty_directory_if_identity(namespace.as_raw_fd(), &retired_name, &retired)?;
        remove_empty_directory_if_identity(self.operations, &namespace_name, &namespace)?;
        sync_counted(self.operations, &self.sync_counts, SyncKind::Operations)?;
        Ok(())
    }

    fn cleanup_owned_payload_links(
        &self,
        directory: RawFd,
        retirement: RawFd,
        extra_owned: &[FileIdentity],
        fault: &AtomicU8,
    ) -> Result<(), WorkerError> {
        let mut removed = false;
        for entry in directory_entries(directory)? {
            let descriptor = match open_regular_at(directory, &entry) {
                Ok(descriptor) => descriptor,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(WorkerError::Io(error)),
            };
            let identity = FileIdentity::from_stat(require_owned_regular(
                descriptor.as_raw_fd(),
                MAX_STATE_FILE_BYTES,
            )?);
            if identity == self.payload_identity || extra_owned.contains(&identity) {
                if take_fault(
                    fault,
                    ClientStateWritePoint::SwapOperationChildAfterValidationBeforeRemoval,
                ) {
                    let preserved = random_component();
                    rename_no_replace(directory, &entry, directory, &preserved)?;
                    write_new_file(directory, &entry, b"post-validation-child-substitution\n")?;
                    sync_directory(directory)?;
                }
                let retired_name = random_component();
                rename_no_replace(directory, &entry, retirement, &retired_name)?;
                let retired_entry = open_regular_at(retirement, &retired_name)?;
                let retired_identity = FileIdentity::from_stat(require_owned_regular(
                    retired_entry.as_raw_fd(),
                    MAX_STATE_FILE_BYTES,
                )?);
                if retired_identity != identity {
                    restore_quarantine(retirement, &retired_name, directory, &entry)?;
                    return Err(invalid_state(
                        "operation child changed before private retirement",
                    ));
                }
                unlink_at(retirement, &retired_name, 0)?;
                removed = true;
            }
        }
        if removed {
            sync_counted(directory, &self.sync_counts, SyncKind::Operations)?;
        }
        Ok(())
    }

    fn inject_payload_swap(&self) -> Result<(), WorkerError> {
        let original_name = c"payload-original";
        rename_no_replace(
            self.directory.as_raw_fd(),
            PAYLOAD_NAME,
            self.directory.as_raw_fd(),
            original_name,
        )?;
        let replacement = create_regular_at(self.directory.as_raw_fd(), PAYLOAD_NAME)?;
        let mut replacement = File::from(replacement);
        replacement.write_all(b"11111111111111111111111111111111\n")?;
        replacement.sync_all()?;
        sync_counted(
            self.directory.as_raw_fd(),
            &self.sync_counts,
            SyncKind::Operations,
        )
    }

    fn inject_retained_payload_corruption(&self) -> Result<(), WorkerError> {
        cvt(unsafe { libc::ftruncate(self.payload.as_raw_fd(), 0) })?;
        let bytes = b"rollback-trigger\n";
        let written = unsafe {
            libc::pwrite(
                self.payload.as_raw_fd(),
                bytes.as_ptr().cast(),
                bytes.len(),
                0,
            )
        };
        if written != bytes.len() as libc::ssize_t {
            return Err(WorkerError::Io(io::Error::last_os_error()));
        }
        cvt(unsafe { libc::fsync(self.payload.as_raw_fd()) })?;
        Ok(())
    }

    fn inject_directory_swap(&self) -> Result<(), WorkerError> {
        let original_name = random_component();
        rename_no_replace(self.operations, &self.name, self.operations, &original_name)?;
        mkdir_at(self.operations, &self.name, 0o700)?;
        let replacement = open_directory_at(self.operations, &self.name)?;
        let sentinel = create_regular_at(replacement.as_raw_fd(), c"substitution-sentinel")?;
        File::from(sentinel).sync_all()?;
        sync_counted(
            replacement.as_raw_fd(),
            &self.sync_counts,
            SyncKind::Operations,
        )?;
        sync_counted(self.operations, &self.sync_counts, SyncKind::Operations)
    }
}

struct StateLock(OwnedFd);

impl StateLock {
    fn acquire(root: RawFd, sync_counts: &SyncCounters) -> Result<Self, WorkerError> {
        let (descriptor, outcome) = open_lock_file_with_creation(root, None)?;
        if outcome != CreationOutcome::Existing {
            sync_counted(root, sync_counts, SyncKind::Root)?;
        }
        cvt(unsafe { libc::flock(descriptor.as_raw_fd(), libc::LOCK_EX) })?;
        Ok(Self(descriptor))
    }
}

impl Drop for StateLock {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) };
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: libc::dev_t,
    inode: libc::ino_t,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct PathIdentity {
    file: FileIdentity,
    kind: libc::mode_t,
    permissions: libc::mode_t,
    owner: libc::uid_t,
}

impl PathIdentity {
    fn from_stat(stat: libc::stat) -> Self {
        Self {
            file: FileIdentity::from_stat(stat),
            kind: file_type(stat.st_mode),
            permissions: stat.st_mode & 0o777,
            owner: stat.st_uid,
        }
    }
}

impl FileIdentity {
    fn from_stat(stat: libc::stat) -> Self {
        Self {
            device: stat.st_dev,
            inode: stat.st_ino,
        }
    }
}

#[derive(Clone, Copy)]
enum SyncKind {
    ParentDirectory,
    Root,
    Jobs,
    Operations,
}

fn open_or_create_root(
    path: &Path,
    sync_counts: &Arc<SyncCounters>,
    creation_race: &AtomicU8,
) -> Result<OwnedFd, WorkerError> {
    if path.as_os_str().is_empty() {
        return Err(invalid_state("state root path is empty"));
    }
    let mut current = if path.is_absolute() {
        open_directory_path(Path::new("/"))?
    } else {
        open_directory_path(Path::new("."))?
    };
    let mut saw_normal = false;
    for component in path.components() {
        let name = match component {
            Component::RootDir | Component::CurDir => continue,
            Component::Normal(name) => name,
            Component::ParentDir | Component::Prefix(_) => {
                return Err(invalid_state("state root path is not normalized"));
            }
        };
        saw_normal = true;
        let name = cstring(name)?;
        let next = match open_directory_at(current.as_raw_fd(), &name) {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if take_creation_race(creation_race, ClientStateCreationRacePoint::RootComponent) {
                    mkdir_at(current.as_raw_fd(), &name, 0o700)?;
                }
                let mut created = false;
                let mut concurrent_existing = false;
                match mkdir_at(current.as_raw_fd(), &name, 0o700) {
                    Ok(()) => created = true,
                    Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {
                        concurrent_existing = true;
                    }
                    Err(error) => return Err(WorkerError::Io(error)),
                }
                let directory = open_directory_at(current.as_raw_fd(), &name)?;
                if created || concurrent_existing {
                    sync_counted(current.as_raw_fd(), sync_counts, SyncKind::ParentDirectory)?;
                }
                if concurrent_existing {
                    sync_counts
                        .concurrent_loser_parents
                        .fetch_add(1, Ordering::SeqCst);
                }
                directory
            }
            Err(error) => return Err(WorkerError::Io(error)),
        };
        current = next;
    }
    if !saw_normal {
        return Err(invalid_state("state root must not be a filesystem root"));
    }
    Ok(current)
}

fn open_or_create_owned_directory(
    parent: RawFd,
    name: &CStr,
    sync_counts: &Arc<SyncCounters>,
    sync_kind: SyncKind,
    creation_race: &AtomicU8,
) -> Result<OwnedFd, WorkerError> {
    let directory = match open_directory_at(parent, name) {
        Ok(directory) => directory,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if take_creation_race(creation_race, ClientStateCreationRacePoint::OwnedDirectory) {
                mkdir_at(parent, name, 0o700)?;
            }
            let mut created = false;
            let mut concurrent_existing = false;
            match mkdir_at(parent, name, 0o700) {
                Ok(()) => created = true,
                Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {
                    concurrent_existing = true;
                }
                Err(error) => return Err(WorkerError::Io(error)),
            }
            let directory = open_directory_at(parent, name)?;
            if created || concurrent_existing {
                sync_counted(parent, sync_counts, sync_kind)?;
            }
            if concurrent_existing {
                sync_counts
                    .concurrent_loser_parents
                    .fetch_add(1, Ordering::SeqCst);
            }
            directory
        }
        Err(error) => return Err(WorkerError::Io(error)),
    };
    require_owned_directory(directory.as_raw_fd())?;
    Ok(directory)
}

fn ensure_lock_file(
    root: RawFd,
    sync_counts: &Arc<SyncCounters>,
    creation_race: &AtomicU8,
) -> Result<(), WorkerError> {
    let (descriptor, outcome) = open_lock_file_with_creation(root, Some(creation_race))?;
    drop(descriptor);
    if outcome != CreationOutcome::Existing {
        sync_counted(root, sync_counts, SyncKind::Root)?;
    }
    if outcome == CreationOutcome::ConcurrentExisting {
        sync_counts
            .concurrent_loser_parents
            .fetch_add(1, Ordering::SeqCst);
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CreationOutcome {
    Existing,
    Created,
    ConcurrentExisting,
}

fn open_lock_file_with_creation(
    root: RawFd,
    creation_race: Option<&AtomicU8>,
) -> Result<(OwnedFd, CreationOutcome), WorkerError> {
    let open_existing = || {
        cvt_fd(unsafe {
            libc::openat(
                root,
                LOCK_NAME.as_ptr(),
                libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            )
        })
    };
    let (descriptor, outcome) = match open_existing() {
        Ok(descriptor) => (descriptor, CreationOutcome::Existing),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if creation_race.is_some_and(|race| {
                take_creation_race(race, ClientStateCreationRacePoint::LockFile)
            }) {
                drop(create_lock_file(root)?);
            }
            match cvt_fd(unsafe {
                libc::openat(
                    root,
                    LOCK_NAME.as_ptr(),
                    libc::O_RDWR
                        | libc::O_CREAT
                        | libc::O_EXCL
                        | libc::O_CLOEXEC
                        | libc::O_NOFOLLOW
                        | libc::O_NONBLOCK,
                    0o600,
                )
            }) {
                Ok(descriptor) => (descriptor, CreationOutcome::Created),
                Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {
                    (open_existing()?, CreationOutcome::ConcurrentExisting)
                }
                Err(error) => return Err(WorkerError::Io(error)),
            }
        }
        Err(error) => return Err(WorkerError::Io(error)),
    };
    require_owned_regular(descriptor.as_raw_fd(), 0)?;
    Ok((descriptor, outcome))
}

fn create_lock_file(root: RawFd) -> io::Result<OwnedFd> {
    cvt_fd(unsafe {
        libc::openat(
            root,
            LOCK_NAME.as_ptr(),
            libc::O_RDWR
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK,
            0o600,
        )
    })
}

fn validate_root_entries(root: RawFd) -> Result<(), WorkerError> {
    for entry in directory_entries(root)? {
        match entry.to_bytes() {
            b"client-id" | b"jobs" | b".mac-worker-state" | b"jobs.lock" => {}
            bytes if std::str::from_utf8(bytes).is_err() => {
                return Err(invalid_state("state root contains a non-UTF-8 entry"));
            }
            _ => return Err(invalid_state("state root contains an unexpected entry")),
        }
    }
    Ok(())
}

fn validate_operation_entries(operations: RawFd) -> Result<(), WorkerError> {
    for entry in directory_entries(operations)? {
        let text = std::str::from_utf8(entry.to_bytes())
            .map_err(|_| invalid_state("operation state contains a non-UTF-8 entry"))?;
        if text.len() != 32
            || !text
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(invalid_state(
                "operation state contains an unexpected entry",
            ));
        }
        let directory = match open_directory_at(operations, &entry) {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(WorkerError::Io(error)),
        };
        require_owned_directory(directory.as_raw_fd())?;
        let children = match directory_entries(directory.as_raw_fd()) {
            Ok(children) => children,
            Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        for child in children {
            if child.as_c_str() != PAYLOAD_NAME {
                return Err(invalid_state(
                    "operation directory contains an unexpected entry",
                ));
            }
            let descriptor = match open_regular_at(directory.as_raw_fd(), &child) {
                Ok(descriptor) => descriptor,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(WorkerError::Io(error)),
            };
            require_owned_regular(descriptor.as_raw_fd(), MAX_STATE_FILE_BYTES)?;
        }
    }
    Ok(())
}

fn require_owned_directory(descriptor: RawFd) -> Result<(), WorkerError> {
    let stat = stat_fd(descriptor)?;
    if file_type(stat.st_mode) != libc::S_IFDIR
        || stat.st_uid != effective_user_id()
        || stat.st_mode & 0o777 != 0o700
    {
        return Err(invalid_state("state directory is not owner-only"));
    }
    Ok(())
}

fn require_owned_regular(descriptor: RawFd, max_size: usize) -> Result<libc::stat, WorkerError> {
    let stat = stat_fd(descriptor)?;
    if !is_owned_regular_stat(&stat, max_size) {
        return Err(invalid_state(
            "state file is not an owner-only regular file",
        ));
    }
    Ok(stat)
}

fn is_owned_regular_stat(stat: &libc::stat, max_size: usize) -> bool {
    file_type(stat.st_mode) == libc::S_IFREG
        && stat.st_uid == effective_user_id()
        && stat.st_mode & 0o777 == 0o600
        && stat.st_size >= 0
        && stat.st_size as usize <= max_size
}

fn read_regular_optional(
    directory: RawFd,
    name: &CStr,
) -> Result<Option<(Vec<u8>, FileIdentity)>, WorkerError> {
    match open_regular_at(directory, name) {
        Ok(descriptor) => read_open_regular(descriptor).map(Some),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(WorkerError::Io(error)),
    }
}

fn read_regular(directory: RawFd, name: &CStr) -> Result<(Vec<u8>, FileIdentity), WorkerError> {
    let descriptor = open_regular_at(directory, name)?;
    read_open_regular(descriptor)
}

fn read_open_regular(descriptor: OwnedFd) -> Result<(Vec<u8>, FileIdentity), WorkerError> {
    let stat = require_owned_regular(descriptor.as_raw_fd(), MAX_STATE_FILE_BYTES)?;
    let identity = FileIdentity {
        device: stat.st_dev,
        inode: stat.st_ino,
    };
    let expected = stat.st_size as usize;
    let mut file = File::from(descriptor);
    let mut bytes = Vec::with_capacity(expected);
    (&mut file)
        .take((MAX_STATE_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() != expected {
        return Err(invalid_state("state file changed while being read"));
    }
    let after = stat_fd(file.as_raw_fd())?;
    if after.st_dev != identity.device
        || after.st_ino != identity.inode
        || after.st_size != stat.st_size
    {
        return Err(invalid_state("state file changed while being read"));
    }
    Ok((bytes, identity))
}

fn directory_entries(directory: RawFd) -> Result<Vec<CString>, WorkerError> {
    let independent = open_directory_at(directory, c".")?;
    let independent = independent.into_raw_fd();
    let stream = unsafe { libc::fdopendir(independent) };
    if stream.is_null() {
        let error = io::Error::last_os_error();
        let _ = unsafe { libc::close(independent) };
        return Err(WorkerError::Io(error));
    }
    let mut entries = Vec::new();
    loop {
        clear_errno();
        let entry = unsafe { libc::readdir(stream) };
        if entry.is_null() {
            let errno = current_errno();
            let close_result = unsafe { libc::closedir(stream) };
            if errno != 0 {
                return Err(WorkerError::Io(io::Error::from_raw_os_error(errno)));
            }
            cvt(close_result)?;
            return Ok(entries);
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if name.to_bytes() != b"." && name.to_bytes() != b".." {
            entries.push(name.to_owned());
        }
    }
}

fn open_directory_path(path: &Path) -> Result<OwnedFd, WorkerError> {
    let path = cstring(path.as_os_str())?;
    cvt_fd(unsafe { libc::open(path.as_ptr(), DIRECTORY_FLAGS) }).map_err(WorkerError::Io)
}

fn open_directory_at(parent: RawFd, name: &CStr) -> io::Result<OwnedFd> {
    cvt_fd(unsafe { libc::openat(parent, name.as_ptr(), DIRECTORY_FLAGS) })
}

fn open_regular_at(parent: RawFd, name: &CStr) -> io::Result<OwnedFd> {
    cvt_fd(unsafe { libc::openat(parent, name.as_ptr(), READ_FILE_FLAGS) })
}

fn create_regular_at(parent: RawFd, name: &CStr) -> io::Result<OwnedFd> {
    cvt_fd(unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            libc::O_WRONLY
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK,
            0o600,
        )
    })
}

fn mkdir_at(parent: RawFd, name: &CStr, mode: libc::mode_t) -> io::Result<()> {
    cvt(unsafe { libc::mkdirat(parent, name.as_ptr(), mode) })
}

fn unlink_at(parent: RawFd, name: &CStr, flags: libc::c_int) -> io::Result<()> {
    cvt(unsafe { libc::unlinkat(parent, name.as_ptr(), flags) })
}

fn link_no_replace(
    source_parent: RawFd,
    source: &CStr,
    destination_parent: RawFd,
    destination: &CStr,
) -> io::Result<()> {
    cvt(unsafe {
        libc::linkat(
            source_parent,
            source.as_ptr(),
            destination_parent,
            destination.as_ptr(),
            0,
        )
    })
}

#[cfg(target_vendor = "apple")]
fn rename_no_replace(
    source_parent: RawFd,
    source: &CStr,
    destination_parent: RawFd,
    destination: &CStr,
) -> io::Result<()> {
    cvt(unsafe {
        libc::renameatx_np(
            source_parent,
            source.as_ptr(),
            destination_parent,
            destination.as_ptr(),
            libc::RENAME_EXCL,
        )
    })
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn rename_no_replace(
    source_parent: RawFd,
    source: &CStr,
    destination_parent: RawFd,
    destination: &CStr,
) -> io::Result<()> {
    cvt(unsafe {
        libc::renameat2(
            source_parent,
            source.as_ptr(),
            destination_parent,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    })
}

#[cfg(not(any(target_vendor = "apple", target_os = "linux", target_os = "android")))]
fn rename_no_replace(
    _source_parent: RawFd,
    _source: &CStr,
    _destination_parent: RawFd,
    _destination: &CStr,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic no-replace rename is unavailable",
    ))
}

#[cfg(target_vendor = "apple")]
fn exchange_entries(
    left_parent: RawFd,
    left: &CStr,
    right_parent: RawFd,
    right: &CStr,
) -> Result<(), WorkerError> {
    cvt(unsafe {
        libc::renameatx_np(
            left_parent,
            left.as_ptr(),
            right_parent,
            right.as_ptr(),
            libc::RENAME_SWAP,
        )
    })
    .map_err(WorkerError::Io)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn exchange_entries(
    left_parent: RawFd,
    left: &CStr,
    right_parent: RawFd,
    right: &CStr,
) -> Result<(), WorkerError> {
    cvt(unsafe {
        libc::renameat2(
            left_parent,
            left.as_ptr(),
            right_parent,
            right.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    })
    .map_err(WorkerError::Io)
}

#[cfg(not(any(target_vendor = "apple", target_os = "linux", target_os = "android")))]
fn exchange_entries(
    _left_parent: RawFd,
    _left: &CStr,
    _right_parent: RawFd,
    _right: &CStr,
) -> Result<(), WorkerError> {
    Err(WorkerError::Io(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic entry exchange is unavailable",
    )))
}

fn remove_entry_if_identity(
    parent: RawFd,
    name: &CStr,
    expected: FileIdentity,
    fault: &AtomicU8,
    sync_counts: &Arc<SyncCounters>,
) -> Result<(), WorkerError> {
    let (namespace_name, namespace) =
        create_private_directory(parent, sync_counts, SyncKind::Jobs)?;
    let acquired_name = c"published";
    rename_no_replace(parent, name, namespace.as_raw_fd(), acquired_name)?;
    let acquired = open_regular_at(namespace.as_raw_fd(), acquired_name)?;
    let identity = FileIdentity::from_stat(require_owned_regular(
        acquired.as_raw_fd(),
        MAX_STATE_FILE_BYTES,
    )?);
    if identity != expected {
        restore_quarantine(namespace.as_raw_fd(), acquired_name, parent, name)?;
        remove_empty_directory_if_identity(parent, &namespace_name, &namespace)?;
        return Err(invalid_state(
            "published entry changed before identity-bound rollback",
        ));
    }

    if take_fault(
        fault,
        ClientStateWritePoint::SwapPublishedRollbackAfterValidationBeforeRemoval,
    ) {
        let preserved = random_component();
        rename_no_replace(
            namespace.as_raw_fd(),
            acquired_name,
            namespace.as_raw_fd(),
            &preserved,
        )?;
        write_new_file(
            namespace.as_raw_fd(),
            acquired_name,
            b"post-validation-published-substitution\n",
        )?;
        sync_directory(namespace.as_raw_fd())?;
    }

    let (retired_name, retired) =
        create_private_directory(namespace.as_raw_fd(), sync_counts, SyncKind::Operations)?;
    rename_no_replace(
        namespace.as_raw_fd(),
        acquired_name,
        retired.as_raw_fd(),
        acquired_name,
    )?;
    let final_entry = open_regular_at(retired.as_raw_fd(), acquired_name)?;
    let final_identity = FileIdentity::from_stat(require_owned_regular(
        final_entry.as_raw_fd(),
        MAX_STATE_FILE_BYTES,
    )?);
    if final_identity != expected {
        restore_quarantine(
            retired.as_raw_fd(),
            acquired_name,
            namespace.as_raw_fd(),
            acquired_name,
        )?;
        return Err(invalid_state(
            "published entry changed before private retirement",
        ));
    }
    unlink_at(retired.as_raw_fd(), acquired_name, 0)?;
    sync_directory(retired.as_raw_fd())?;
    remove_empty_directory_if_identity(namespace.as_raw_fd(), &retired_name, &retired)?;
    remove_empty_directory_if_identity(parent, &namespace_name, &namespace)?;
    sync_directory(parent)
}

fn create_private_directory(
    parent: RawFd,
    sync_counts: &Arc<SyncCounters>,
    kind: SyncKind,
) -> Result<(CString, OwnedFd), WorkerError> {
    for _ in 0..16 {
        let name = random_component();
        match mkdir_at(parent, &name, 0o700) {
            Ok(()) => {
                sync_counted(parent, sync_counts, kind)?;
                let directory = open_directory_at(parent, &name)?;
                require_owned_directory(directory.as_raw_fd())?;
                return Ok((name, directory));
            }
            Err(error) if error.raw_os_error() == Some(libc::EEXIST) => continue,
            Err(error) => return Err(WorkerError::Io(error)),
        }
    }
    Err(WorkerError::Io(io::Error::from_raw_os_error(libc::EEXIST)))
}

fn remove_empty_directory_if_identity(
    parent: RawFd,
    name: &CStr,
    directory: &OwnedFd,
) -> Result<(), WorkerError> {
    if !directory_entries(directory.as_raw_fd())?.is_empty() {
        return Err(invalid_state("private retirement directory is not empty"));
    }
    let opened = FileIdentity::from_stat(stat_fd(directory.as_raw_fd())?);
    let current = FileIdentity::from_stat(stat_at(parent, name)?);
    if opened != current {
        return Err(invalid_state(
            "private retirement directory identity changed",
        ));
    }
    unlink_at(parent, name, libc::AT_REMOVEDIR)?;
    Ok(())
}

fn inject_directory_swap_at(
    parent: RawFd,
    name: &CStr,
    sentinel_name: &CStr,
) -> Result<(), WorkerError> {
    let preserved = random_component();
    rename_no_replace(parent, name, parent, &preserved)?;
    mkdir_at(parent, name, 0o700)?;
    let replacement = open_directory_at(parent, name)?;
    write_new_file(replacement.as_raw_fd(), sentinel_name, b"sentinel\n")?;
    sync_directory(replacement.as_raw_fd())?;
    sync_directory(parent)
}

fn restore_quarantine(
    source_parent: RawFd,
    source: &CStr,
    destination_parent: RawFd,
    destination: &CStr,
) -> Result<(), WorkerError> {
    rename_no_replace(source_parent, source, destination_parent, destination)
        .map_err(WorkerError::Io)
}

fn take_live_job_swap_fault(fault: &AtomicU8) -> Option<ClientStateWritePoint> {
    let point = match fault.load(Ordering::SeqCst) {
        value if value == ClientStateWritePoint::SwapLiveJobBeforeReplace as u8 => {
            ClientStateWritePoint::SwapLiveJobBeforeReplace
        }
        value if value == ClientStateWritePoint::SwapLiveJobWithSymlinkBeforeReplace as u8 => {
            ClientStateWritePoint::SwapLiveJobWithSymlinkBeforeReplace
        }
        value if value == ClientStateWritePoint::SwapLiveJobWithDirectoryBeforeReplace as u8 => {
            ClientStateWritePoint::SwapLiveJobWithDirectoryBeforeReplace
        }
        value if value == ClientStateWritePoint::SwapLiveJobWithFifoBeforeReplace as u8 => {
            ClientStateWritePoint::SwapLiveJobWithFifoBeforeReplace
        }
        value
            if value == ClientStateWritePoint::SwapLiveJobWithPermissiveFileBeforeReplace as u8 =>
        {
            ClientStateWritePoint::SwapLiveJobWithPermissiveFileBeforeReplace
        }
        _ => return None,
    };
    take_fault(fault, point).then_some(point)
}

fn inject_live_job_swap(
    parent: RawFd,
    name: &CStr,
    point: ClientStateWritePoint,
) -> Result<(), WorkerError> {
    let original = random_component();
    rename_no_replace(parent, name, parent, &original)?;
    match point {
        ClientStateWritePoint::SwapLiveJobBeforeReplace => {
            write_new_file(parent, name, b"injected-live-replacement\n")?;
        }
        ClientStateWritePoint::SwapLiveJobWithSymlinkBeforeReplace => {
            cvt(unsafe { libc::symlinkat(c"swap-target".as_ptr(), parent, name.as_ptr()) })?;
        }
        ClientStateWritePoint::SwapLiveJobWithDirectoryBeforeReplace => {
            mkdir_at(parent, name, 0o700)?;
            let directory = open_directory_at(parent, name)?;
            write_new_file(directory.as_raw_fd(), c"sentinel", b"sentinel\n")?;
            sync_directory(directory.as_raw_fd())?;
        }
        ClientStateWritePoint::SwapLiveJobWithFifoBeforeReplace => {
            cvt(unsafe { libc::mkfifoat(parent, name.as_ptr(), 0o600) })?;
        }
        ClientStateWritePoint::SwapLiveJobWithPermissiveFileBeforeReplace => {
            let replacement = create_regular_at(parent, name)?;
            cvt(unsafe { libc::fchmod(replacement.as_raw_fd(), 0o644) })?;
            let mut replacement = File::from(replacement);
            replacement.write_all(b"permissive-live-substitution\n")?;
            replacement.sync_all()?;
        }
        _ => return Err(invalid_state("invalid live job swap injection")),
    }
    sync_directory(parent)
}

fn write_new_file(parent: RawFd, name: &CStr, bytes: &[u8]) -> Result<(), WorkerError> {
    let descriptor = create_regular_at(parent, name)?;
    let mut file = File::from(descriptor);
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn random_component() -> CString {
    CString::new(Uuid::new_v4().simple().to_string()).expect("simple UUID contains no NUL")
}

fn stat_fd(descriptor: RawFd) -> Result<libc::stat, WorkerError> {
    let mut stat = MaybeUninit::<libc::stat>::zeroed();
    cvt(unsafe { libc::fstat(descriptor, stat.as_mut_ptr()) }).map_err(WorkerError::Io)?;
    Ok(unsafe { stat.assume_init() })
}

fn stat_at(parent: RawFd, name: &CStr) -> io::Result<libc::stat> {
    let mut stat = MaybeUninit::<libc::stat>::zeroed();
    cvt(unsafe {
        libc::fstatat(
            parent,
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    })?;
    Ok(unsafe { stat.assume_init() })
}

fn sync_directory(descriptor: RawFd) -> Result<(), WorkerError> {
    cvt(unsafe { libc::fsync(descriptor) }).map_err(WorkerError::Io)
}

fn sync_counted(
    descriptor: RawFd,
    counts: &SyncCounters,
    kind: SyncKind,
) -> Result<(), WorkerError> {
    sync_directory(descriptor)?;
    let counter = match kind {
        SyncKind::ParentDirectory => &counts.parent_directories,
        SyncKind::Root => &counts.root,
        SyncKind::Jobs => &counts.jobs,
        SyncKind::Operations => &counts.operations,
    };
    counter.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

fn take_fault(fault: &AtomicU8, point: ClientStateWritePoint) -> bool {
    fault
        .compare_exchange(point as u8, 0, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
}

fn take_creation_race(fault: &AtomicU8, point: ClientStateCreationRacePoint) -> bool {
    fault
        .compare_exchange(point as u8, 0, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
}

fn file_type(mode: libc::mode_t) -> libc::mode_t {
    mode & libc::S_IFMT
}

fn effective_user_id() -> libc::uid_t {
    unsafe { libc::geteuid() }
}

fn cstring(value: &OsStr) -> Result<CString, WorkerError> {
    CString::new(value.as_bytes()).map_err(|_| invalid_state("state path contains NUL"))
}

fn cvt(result: libc::c_int) -> io::Result<()> {
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn cvt_fd(result: libc::c_int) -> io::Result<OwnedFd> {
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(result) })
    }
}

fn invalid_state(message: &'static str) -> WorkerError {
    WorkerError::Io(io::Error::new(io::ErrorKind::InvalidData, message))
}

fn injected_failure(point: ClientStateWritePoint) -> WorkerError {
    let label = match point {
        ClientStateWritePoint::BeforePublish => "before publication",
        ClientStateWritePoint::AfterPublish => "after publication",
        ClientStateWritePoint::SwapOperationPayloadBeforePublish => {
            "after staged payload substitution"
        }
        ClientStateWritePoint::SwapOperationDirectoryBeforeCleanup => {
            "after operation directory substitution"
        }
        ClientStateWritePoint::SwapLiveJobBeforeReplace => "after live job substitution",
        ClientStateWritePoint::SwapOperationDirectoryAfterValidationBeforeRemoval => {
            "after validated operation directory substitution"
        }
        ClientStateWritePoint::SwapOperationChildAfterValidationBeforeRemoval => {
            "after validated operation child substitution"
        }
        ClientStateWritePoint::SwapPublishedRollbackAfterValidationBeforeRemoval => {
            "after validated published rollback substitution"
        }
        ClientStateWritePoint::SwapLiveJobWithSymlinkBeforeReplace => {
            "after symlink live job substitution"
        }
        ClientStateWritePoint::SwapLiveJobWithDirectoryBeforeReplace => {
            "after directory live job substitution"
        }
        ClientStateWritePoint::SwapLiveJobWithFifoBeforeReplace => {
            "after fifo live job substitution"
        }
        ClientStateWritePoint::SwapLiveJobWithPermissiveFileBeforeReplace => {
            "after permissive live job substitution"
        }
    };
    WorkerError::Io(io::Error::other(format!(
        "injected local state failure {label}"
    )))
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn clear_errno() {
    unsafe { *libc::__errno_location() = 0 };
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn current_errno() -> libc::c_int {
    unsafe { *libc::__errno_location() }
}

#[cfg(target_vendor = "apple")]
fn clear_errno() {
    unsafe { *libc::__error() = 0 };
}

#[cfg(target_vendor = "apple")]
fn current_errno() -> libc::c_int {
    unsafe { *libc::__error() }
}
