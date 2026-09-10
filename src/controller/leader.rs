use std::{fs::File, io, os::fd::AsRawFd, path::Path};

use crate::{
    error::WorkerError, job::ProcessIdentity, rooted_fs::RootedDir,
    supervisor::SystemProcessInspector,
};

const CONTROLLER_LOCK: &str = "controller.lock";
const LEADER_NAME: &str = "leader.json";
const MAX_LEADER_BYTES: u64 = 4096;

/// Dedicated controller-run leader. Holds `controller.lock` for the process
/// lifetime. This is not `StateLock` or `QueueLock` and is not taken by RPC.
/// `leader.json` is replaced atomically under that lock; the lock file inode
/// is not the identity record.
pub struct ControllerLeader {
    _lock: File,
    identity: ProcessIdentity,
}

impl ControllerLeader {
    pub fn acquire(state_root: &Path) -> Result<Self, WorkerError> {
        let root = RootedDir::open_or_create_anchored_absolute(state_root).map_err(store_io)?;
        let lock = root.open_private_lock(CONTROLLER_LOCK).map_err(store_io)?;
        try_lock_exclusive(&lock)?;
        let identity = current_process_identity()?;
        write_leader_identity(&root, &identity)?;
        Ok(Self {
            _lock: lock,
            identity,
        })
    }

    pub fn identity(&self) -> ProcessIdentity {
        self.identity
    }
}

pub(crate) fn open_controller_root(state_root: &Path) -> Result<RootedDir, WorkerError> {
    RootedDir::open_or_create_anchored_absolute(state_root).map_err(store_io)
}

pub(crate) fn open_existing_controller_root(
    state_root: &Path,
) -> Result<Option<RootedDir>, WorkerError> {
    match RootedDir::open_anchored_absolute(state_root) {
        Ok(root) => Ok(Some(root)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(store_io(error)),
    }
}

pub(crate) fn lock_exclusive(file: &File) -> Result<(), WorkerError> {
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if result == 0 {
        Ok(())
    } else {
        Err(WorkerError::Io(io::Error::last_os_error()))
    }
}

fn try_lock_exclusive(file: &File) -> Result<(), WorkerError> {
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::EWOULDBLOCK) || error.raw_os_error() == Some(libc::EAGAIN)
    {
        return Err(WorkerError::Protocol(
            "CONTROLLER_LOCK_HELD: controller leader lock is held".into(),
        ));
    }
    Err(WorkerError::Io(error))
}

fn write_leader_identity(root: &RootedDir, identity: &ProcessIdentity) -> Result<(), WorkerError> {
    let bytes = serde_json::to_vec(identity).map_err(|_| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: leader identity is invalid".into())
    })?;
    if root.entry_exists(LEADER_NAME).map_err(store_io)? {
        let previous = root
            .read_private_regular(LEADER_NAME, MAX_LEADER_BYTES)
            .map_err(store_io)?;
        root.replace_private_regular_exact(LEADER_NAME, &previous, &bytes)
            .map_err(store_io)?;
    } else {
        root.write_private_atomic_no_replace(LEADER_NAME, &bytes)
            .map_err(store_io)?;
    }
    Ok(())
}

fn current_process_identity() -> Result<ProcessIdentity, WorkerError> {
    SystemProcessInspector.identity_for_pid(std::process::id())
}

pub(crate) fn store_io(error: io::Error) -> WorkerError {
    WorkerError::Io(error)
}

pub(crate) fn now_millis() -> Result<u64, WorkerError> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| WorkerError::Io(io::Error::other("system clock predates Unix epoch")))?
        .as_millis();
    u64::try_from(millis)
        .map_err(|_| WorkerError::Io(io::Error::other("system clock is out of range")))
}
