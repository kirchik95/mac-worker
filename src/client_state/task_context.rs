use std::{
    ffi::OsString,
    fs, io,
    os::{
        fd::AsRawFd,
        unix::ffi::{OsStrExt, OsStringExt},
    },
    path::{Component, Path, PathBuf},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};

use super::{
    ClientStateStore, StateLock, canonical_json_bytes, invalid_state, parse_canonical_json,
    queue_error, relative_path,
};
use crate::{
    error::WorkerError,
    task::{LocalTaskRecord, TaskId},
};

const PROJECT_CONTEXT_FILE: &str = "project.json";
const MAX_PROJECT_CONTEXT_BYTES: usize = 16 * 1024;

/// Private execution input, kept beside the owner-only turn prompts. It is
/// never part of a task record or a queue/dashboard projection.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskProjectContext {
    version: u8,
    task_id: TaskId,
    project_id: String,
    worktree_id: String,
    repo_id: String,
    path_base64: String,
}

impl TaskProjectContext {
    fn project_path(&self, record: &LocalTaskRecord) -> Result<PathBuf, WorkerError> {
        if self.version != 1 {
            return Err(invalid_state("task project context version is unsupported"));
        }
        if self.task_id != record.meta().task_id()
            || self.project_id != record.meta().project_id()
            || self.worktree_id != record.meta().worktree_id()
            || self.repo_id != record.repo_id()
        {
            return Err(queue_error(
                "TASK_PROJECT_CONTEXT_MISMATCH",
                "task project context belongs to different task metadata",
            ));
        }
        let bytes = STANDARD
            .decode(&self.path_base64)
            .map_err(|_| invalid_state("task project path encoding is invalid"))?;
        if bytes.contains(&0) {
            return Err(invalid_state("task project path contains a null byte"));
        }
        let path = PathBuf::from(OsString::from_vec(bytes));
        if !path.is_absolute()
            || path
                .components()
                .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err(invalid_state(
                "task project path is not absolute and normalized",
            ));
        }
        Ok(path)
    }
}

impl ClientStateStore {
    /// Publishes immutable, owner-only execution context before turn handoff.
    pub fn write_task_project_path(
        &self,
        record: &LocalTaskRecord,
        project_path: &Path,
    ) -> Result<(), WorkerError> {
        let _lock = StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
        self.write_task_project_path_locked(record, project_path)
    }

    /// Queue handoff publishes inherited context before its reservation,
    /// without recursively acquiring the state lock owned by QueueLock.
    pub(super) fn write_task_project_path_locked(
        &self,
        record: &LocalTaskRecord,
        project_path: &Path,
    ) -> Result<(), WorkerError> {
        if !project_path.is_absolute() {
            return Err(invalid_state("task project path must be absolute"));
        }
        let project_path = fs::canonicalize(project_path).map_err(WorkerError::Io)?;
        if !fs::metadata(&project_path)
            .map_err(WorkerError::Io)?
            .is_dir()
        {
            return Err(invalid_state("task project path must name a directory"));
        }
        let context = TaskProjectContext {
            version: 1,
            task_id: record.meta().task_id(),
            project_id: record.meta().project_id().to_owned(),
            worktree_id: record.meta().worktree_id().to_owned(),
            repo_id: record.repo_id().to_owned(),
            path_base64: STANDARD.encode(project_path.as_os_str().as_bytes()),
        };
        let bytes = canonical_json_bytes(&context, "task project context")?;
        if bytes.len() > MAX_PROJECT_CONTEXT_BYTES {
            return Err(invalid_state("task project context exceeds the size limit"));
        }
        let task_dir = self
            .turns_dir()?
            .open_child_directory(&relative_path(&record.meta().task_id().to_string())?, true)
            .map_err(WorkerError::Io)?;
        match task_dir.write_private_atomic_no_replace(PROJECT_CONTEXT_FILE, &bytes) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if self.task_project_path(record)? == Some(project_path) {
                    Ok(())
                } else {
                    Err(queue_error(
                        "TASK_PROJECT_CONTEXT_CONFLICT",
                        "task project context is already bound to another directory",
                    ))
                }
            }
            Err(error) => Err(WorkerError::Io(error)),
        }
    }

    /// Reads immutable private context without taking StateLock. QueueLock
    /// callers can use this while selecting a runner handoff. Legacy records
    /// have no context and retain the caller's existing cwd fallback.
    pub fn task_project_path(
        &self,
        record: &LocalTaskRecord,
    ) -> Result<Option<PathBuf>, WorkerError> {
        let task_dir = match self
            .turns_dir()?
            .open_child_directory(&relative_path(&record.meta().task_id().to_string())?, false)
        {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(WorkerError::Io(error)),
        };
        let bytes = match task_dir
            .read_private_regular(PROJECT_CONTEXT_FILE, MAX_PROJECT_CONTEXT_BYTES as u64)
        {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(WorkerError::Io(error)),
        };
        let context: TaskProjectContext = parse_canonical_json(&bytes, "task project context")?;
        context.project_path(record).map(Some)
    }
}
