//! Rooted, bounded host execution evidence, separate from ordinary task DTOs.
use super::contracts::*;
use crate::{
    error::WorkerError, host_store::HostStore, inputs::RelativePath, rooted_fs::RootedDir,
    task::TaskId,
};
use std::{fs::File, io, os::fd::AsRawFd};

pub struct HostIntegrationStore<'a> {
    store: &'a HostStore,
}
impl<'a> HostIntegrationStore<'a> {
    pub fn new(store: &'a HostStore) -> Self {
        Self { store }
    }
    fn directory(
        &self,
        project: &str,
        task: TaskId,
        create: bool,
    ) -> Result<RootedDir, WorkerError> {
        Ok(self
            .store
            .open_task_directory(project, task, false)?
            .open_child_directory(&relative("integration")?, create)?)
    }
    pub fn load(
        &self,
        project_id: &str,
        task: TaskId,
    ) -> Result<Option<IntegrationRecord>, WorkerError> {
        let Some(bytes) = self.read(project_id, task, "record.json", MAX_PRIVATE_RECORD_BYTES)?
        else {
            return Ok(None);
        };
        let record: IntegrationRecord = decode_bounded(&bytes, MAX_PRIVATE_RECORD_BYTES)?;
        if record.task_id != task || record.policy.project_id != project_id {
            return Err(invalid());
        }
        Ok(Some(record))
    }
    pub(crate) fn save(&self, record: &IntegrationRecord) -> Result<(), WorkerError> {
        let bytes = encode_bounded(record, MAX_PRIVATE_RECORD_BYTES)?;
        self.write(
            &record.policy.project_id,
            record.task_id,
            "record.json",
            &bytes,
        )
    }
    pub(crate) fn policy(
        &self,
        project: &str,
        task: TaskId,
    ) -> Result<Option<FrozenIntegrationPolicy>, WorkerError> {
        self.read(project, task, "policy.json", MAX_PRIVATE_RECORD_BYTES)?
            .map(|bytes| decode_bounded(&bytes, MAX_PRIVATE_RECORD_BYTES))
            .transpose()
    }
    pub(crate) fn arm(
        &self,
        project: &str,
        task: TaskId,
        policy: &FrozenIntegrationPolicy,
    ) -> Result<(), WorkerError> {
        if let Some(old) = self.policy(project, task)? {
            if old != *policy {
                return Err(invalid());
            }
            return Ok(());
        }
        self.write(
            project,
            task,
            "policy.json",
            &encode_bounded(policy, MAX_PRIVATE_RECORD_BYTES)?,
        )
    }
    pub(crate) fn lock(&self, project: &str, task: TaskId) -> Result<File, WorkerError> {
        let file = self
            .directory(project, task, true)?
            .open_private_lock("fence.lock")?;
        // Nonblocking: an unacknowledged revoke remains retryable while a bounded push owns the fence.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(IntegrationCode::IntegrationStopUnconfirmed.error());
        }
        Ok(file)
    }
    pub(crate) fn read(
        &self,
        project: &str,
        task: TaskId,
        name: &str,
        max: usize,
    ) -> Result<Option<Vec<u8>>, WorkerError> {
        for attempt in 0..3 {
            let result = self
                .directory(project, task, false)
                .and_then(|dir| Ok(dir.read_private_regular(name, max as u64)?));
            match result {
                Ok(bytes) => return Ok(Some(bytes)),
                Err(WorkerError::Io(e)) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(WorkerError::Io(e))
                    if e.raw_os_error() == Some(libc::ESTALE) && attempt < 2 =>
                {
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(invalid())
    }
    pub(crate) fn write(
        &self,
        project: &str,
        task: TaskId,
        name: &str,
        bytes: &[u8],
    ) -> Result<(), WorkerError> {
        for attempt in 0..3 {
            let dir = self.directory(project, task, true)?;
            let result = match dir.read_private_regular(name, MAX_PREPARED_TURN_BYTES as u64) {
                Ok(old) if old == bytes => return Ok(()),
                Ok(old) => dir.rewrite_private_regular_exact(name, &old, bytes),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    dir.write_private_atomic_no_replace(name, bytes)
                }
                Err(e) => Err(e),
            };
            match result {
                Ok(()) => {
                    dir.sync_root()?;
                    return Ok(());
                }
                Err(e)
                    if (e.raw_os_error() == Some(libc::ESTALE)
                        || e.kind() == io::ErrorKind::AlreadyExists)
                        && attempt < 2 =>
                {
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(invalid())
    }
}
pub(crate) fn relative(path: &str) -> Result<RelativePath, WorkerError> {
    RelativePath::parse(path.as_bytes()).map_err(|_| invalid())
}
pub(crate) fn invalid() -> WorkerError {
    IntegrationCode::IntegrationStateInvalid.error()
}
