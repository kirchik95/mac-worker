use std::{
    ffi::{CStr, CString, OsStr},
    fs::File,
    io::{self, Read, Write},
    mem::MaybeUninit,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
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
    write_fault: AtomicU8,
}

impl ClientStateStore {
    pub fn open(state_root: &Path) -> Result<Self, WorkerError> {
        let root = open_or_create_root(state_root)?;
        require_owned_directory(root.as_raw_fd())?;
        let jobs = open_or_create_owned_directory(root.as_raw_fd(), JOBS_NAME)?;
        let operations = open_or_create_owned_directory(root.as_raw_fd(), OPERATIONS_NAME)?;
        ensure_lock_file(root.as_raw_fd())?;
        validate_root_entries(root.as_raw_fd())?;
        validate_operation_entries(operations.as_raw_fd())?;
        let client_id = load_or_create_client_id(root.as_raw_fd(), operations.as_raw_fd())?;

        Ok(Self {
            inner: Arc::new(ClientStateInner {
                root,
                jobs,
                operations,
                client_id,
                write_fault: AtomicU8::new(0),
            }),
        })
    }

    pub fn client_id(&self) -> ClientId {
        self.inner.client_id
    }

    pub fn create_job(&self, record: LocalJobRecord) -> Result<(), WorkerError> {
        record.validate()?;
        self.require_local_client(&record)?;
        let _lock = StateLock::acquire(self.inner.root.as_raw_fd())?;
        let name = job_file_name(record.meta().job_id())?;

        if let Some(existing) = read_job_optional(self.inner.jobs.as_raw_fd(), &name)? {
            return require_same_immutable(&existing, &record);
        }

        let bytes = canonical_record_bytes(&record)?;
        let operation = OperationFile::stage(self.inner.operations.as_raw_fd(), &bytes)?;
        if self.take_fault(ClientStateWritePoint::BeforePublish) {
            return Err(injected_failure(ClientStateWritePoint::BeforePublish));
        }

        match link_no_replace(
            operation.directory.as_raw_fd(),
            PAYLOAD_NAME,
            self.inner.jobs.as_raw_fd(),
            &name,
        ) {
            Ok(()) => {}
            Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {
                operation.cleanup()?;
                let existing = read_job(self.inner.jobs.as_raw_fd(), &name)?;
                return require_same_immutable(&existing, &record);
            }
            Err(error) => return Err(WorkerError::Io(error)),
        }

        if self.take_fault(ClientStateWritePoint::AfterPublish) {
            return Err(injected_failure(ClientStateWritePoint::AfterPublish));
        }
        sync_directory(self.inner.jobs.as_raw_fd())?;
        operation.cleanup()?;
        Ok(())
    }

    pub fn load_job(&self, job_id: JobId) -> Result<LocalJobRecord, WorkerError> {
        let name = job_file_name(job_id)?;
        let record = read_job(self.inner.jobs.as_raw_fd(), &name)?;
        if record.meta().job_id() != job_id {
            return Err(invalid_state("job filename and record identity differ"));
        }
        Ok(record)
    }

    pub fn update_job(&self, replacement: LocalJobRecord) -> Result<(), WorkerError> {
        replacement.validate()?;
        self.require_local_client(&replacement)?;
        let _lock = StateLock::acquire(self.inner.root.as_raw_fd())?;
        let name = job_file_name(replacement.meta().job_id())?;
        let (existing, identity) = read_job_with_identity(self.inner.jobs.as_raw_fd(), &name)?;
        require_same_immutable(&existing, &replacement)?;
        require_forward_observation(&existing, &replacement)?;

        let bytes = canonical_record_bytes(&replacement)?;
        let operation = OperationFile::stage(self.inner.operations.as_raw_fd(), &bytes)?;
        if self.take_fault(ClientStateWritePoint::BeforePublish) {
            return Err(injected_failure(ClientStateWritePoint::BeforePublish));
        }
        require_same_path_identity(self.inner.jobs.as_raw_fd(), &name, identity)?;
        rename_replace(
            operation.directory.as_raw_fd(),
            PAYLOAD_NAME,
            self.inner.jobs.as_raw_fd(),
            &name,
        )?;
        if self.take_fault(ClientStateWritePoint::AfterPublish) {
            return Err(injected_failure(ClientStateWritePoint::AfterPublish));
        }
        sync_directory(self.inner.jobs.as_raw_fd())?;
        operation.cleanup()?;
        Ok(())
    }

    pub fn list_jobs(&self) -> Result<Vec<LocalJobRecord>, WorkerError> {
        let _lock = StateLock::acquire(self.inner.root.as_raw_fd())?;
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
                Ok(record)
            })
            .collect()
    }

    #[doc(hidden)]
    pub fn inject_write_failure_once(&self, point: ClientStateWritePoint) {
        self.inner.write_fault.store(point as u8, Ordering::SeqCst);
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

fn load_or_create_client_id(root: RawFd, operations: RawFd) -> Result<ClientId, WorkerError> {
    match read_regular_optional(root, CLIENT_ID_NAME)? {
        Some((bytes, _)) => parse_client_id(&bytes),
        None => {
            let candidate = ClientId::generate();
            let bytes = format!("{candidate}\n").into_bytes();
            let operation = OperationFile::stage(operations, &bytes)?;
            match link_no_replace(
                operation.directory.as_raw_fd(),
                PAYLOAD_NAME,
                root,
                CLIENT_ID_NAME,
            ) {
                Ok(()) => {
                    sync_directory(root)?;
                    operation.cleanup()?;
                    Ok(candidate)
                }
                Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {
                    operation.cleanup()?;
                    let (winner, _) = read_regular(root, CLIENT_ID_NAME)?;
                    parse_client_id(&winner)
                }
                Err(error) => Err(WorkerError::Io(error)),
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
}

impl OperationFile {
    fn stage(operations: RawFd, bytes: &[u8]) -> Result<Self, WorkerError> {
        let name = CString::new(Uuid::new_v4().simple().to_string())
            .map_err(|_| invalid_state("operation identity is invalid"))?;
        mkdir_at(operations, &name, 0o700)?;
        let directory = match open_directory_at(operations, &name) {
            Ok(directory) => directory,
            Err(error) => {
                let _ = unlink_at(operations, &name, libc::AT_REMOVEDIR);
                return Err(WorkerError::Io(error));
            }
        };
        require_owned_directory(directory.as_raw_fd())?;
        let descriptor = create_regular_at(directory.as_raw_fd(), PAYLOAD_NAME)?;
        let mut file = File::from(descriptor);
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        sync_directory(directory.as_raw_fd())?;
        Ok(Self {
            operations,
            name,
            directory,
        })
    }

    fn cleanup(self) -> Result<(), WorkerError> {
        match unlink_at(self.directory.as_raw_fd(), PAYLOAD_NAME, 0) {
            Ok(()) => sync_directory(self.directory.as_raw_fd())?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(WorkerError::Io(error)),
        }
        unlink_at(self.operations, &self.name, libc::AT_REMOVEDIR)?;
        sync_directory(self.operations)?;
        Ok(())
    }
}

struct StateLock(OwnedFd);

impl StateLock {
    fn acquire(root: RawFd) -> Result<Self, WorkerError> {
        let descriptor = open_lock_file(root)?;
        cvt(unsafe { libc::flock(descriptor.as_raw_fd(), libc::LOCK_EX) })?;
        Ok(Self(descriptor))
    }
}

impl Drop for StateLock {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) };
    }
}

#[derive(Clone, Copy)]
struct FileIdentity {
    device: libc::dev_t,
    inode: libc::ino_t,
}

fn open_or_create_root(path: &Path) -> Result<OwnedFd, WorkerError> {
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
                match mkdir_at(current.as_raw_fd(), &name, 0o700) {
                    Ok(()) => {}
                    Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {}
                    Err(error) => return Err(WorkerError::Io(error)),
                }
                open_directory_at(current.as_raw_fd(), &name)?
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

fn open_or_create_owned_directory(parent: RawFd, name: &CStr) -> Result<OwnedFd, WorkerError> {
    let directory = match open_directory_at(parent, name) {
        Ok(directory) => directory,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            match mkdir_at(parent, name, 0o700) {
                Ok(()) => {}
                Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {}
                Err(error) => return Err(WorkerError::Io(error)),
            }
            open_directory_at(parent, name)?
        }
        Err(error) => return Err(WorkerError::Io(error)),
    };
    require_owned_directory(directory.as_raw_fd())?;
    Ok(directory)
}

fn ensure_lock_file(root: RawFd) -> Result<(), WorkerError> {
    drop(open_lock_file(root)?);
    Ok(())
}

fn open_lock_file(root: RawFd) -> Result<OwnedFd, WorkerError> {
    let open_existing = || {
        cvt_fd(unsafe {
            libc::openat(
                root,
                LOCK_NAME.as_ptr(),
                libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            )
        })
    };
    let descriptor = match open_existing() {
        Ok(descriptor) => descriptor,
        Err(error) if error.kind() == io::ErrorKind::NotFound => match cvt_fd(unsafe {
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
            Ok(descriptor) => descriptor,
            Err(error) if error.raw_os_error() == Some(libc::EEXIST) => open_existing()?,
            Err(error) => return Err(WorkerError::Io(error)),
        },
        Err(error) => return Err(WorkerError::Io(error)),
    };
    require_owned_regular(descriptor.as_raw_fd(), 0)?;
    Ok(descriptor)
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
    if file_type(stat.st_mode) != libc::S_IFREG
        || stat.st_uid != effective_user_id()
        || stat.st_mode & 0o777 != 0o600
    {
        return Err(invalid_state(
            "state file is not an owner-only regular file",
        ));
    }
    if stat.st_size < 0 || stat.st_size as usize > max_size {
        return Err(invalid_state("state file exceeds its size limit"));
    }
    Ok(stat)
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

fn require_same_path_identity(
    directory: RawFd,
    name: &CStr,
    expected: FileIdentity,
) -> Result<(), WorkerError> {
    let stat = stat_at(directory, name)?;
    if file_type(stat.st_mode) != libc::S_IFREG
        || stat.st_dev != expected.device
        || stat.st_ino != expected.inode
    {
        return Err(invalid_state("job state changed before atomic replacement"));
    }
    Ok(())
}

fn directory_entries(directory: RawFd) -> Result<Vec<CString>, WorkerError> {
    let duplicate = cvt_fd(unsafe { libc::fcntl(directory, libc::F_DUPFD_CLOEXEC, 0) })?;
    let stream = unsafe { libc::fdopendir(duplicate.as_raw_fd()) };
    if stream.is_null() {
        return Err(WorkerError::Io(io::Error::last_os_error()));
    }
    std::mem::forget(duplicate);
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

fn rename_replace(
    source_parent: RawFd,
    source: &CStr,
    destination_parent: RawFd,
    destination: &CStr,
) -> Result<(), WorkerError> {
    cvt(unsafe {
        libc::renameat(
            source_parent,
            source.as_ptr(),
            destination_parent,
            destination.as_ptr(),
        )
    })
    .map_err(WorkerError::Io)
}

fn stat_fd(descriptor: RawFd) -> Result<libc::stat, WorkerError> {
    let mut stat = MaybeUninit::<libc::stat>::zeroed();
    cvt(unsafe { libc::fstat(descriptor, stat.as_mut_ptr()) }).map_err(WorkerError::Io)?;
    Ok(unsafe { stat.assume_init() })
}

fn stat_at(parent: RawFd, name: &CStr) -> Result<libc::stat, WorkerError> {
    let mut stat = MaybeUninit::<libc::stat>::zeroed();
    cvt(unsafe {
        libc::fstatat(
            parent,
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    })
    .map_err(WorkerError::Io)?;
    Ok(unsafe { stat.assume_init() })
}

fn sync_directory(descriptor: RawFd) -> Result<(), WorkerError> {
    cvt(unsafe { libc::fsync(descriptor) }).map_err(WorkerError::Io)
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
