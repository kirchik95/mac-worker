use std::{path::PathBuf, time::Duration};

use serde::Serialize;

use crate::{
    error::WorkerError,
    inputs::RelativePath,
    job::{CommandSpec, CommandSummary, JobId, JobStatus, LocalJobRecord, RemoteUncertainty},
    protocol::PROTOCOL_VERSION,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRequest {
    pub worker: String,
    pub project: PathBuf,
    pub cli_includes: Vec<String>,
    pub timeout: Option<Duration>,
    pub command: CommandSpec,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RunReport {
    pub protocol_version: u32,
    pub job_id: JobId,
    pub worker: String,
    pub status: JobStatus,
}

impl RunReport {
    pub fn new(job_id: JobId, worker: String, status: JobStatus) -> Result<Self, WorkerError> {
        status.validate()?;
        Ok(Self {
            protocol_version: PROTOCOL_VERSION,
            job_id,
            worker,
            status,
        })
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct StatusRow {
    pub job_id: JobId,
    pub worker: String,
    pub project_id: String,
    pub worktree_id: String,
    pub manifest_digest: String,
    pub command_summary: CommandSummary,
    pub relative_working_dir: String,
    pub created_at_millis: u64,
    pub status: Option<JobStatus>,
    pub remote_uncertainty: RemoteUncertainty,
}

impl StatusRow {
    pub fn try_from_record(record: &LocalJobRecord) -> Result<Self, WorkerError> {
        record.validate()?;
        let meta = record.meta();
        let relative_working_dir = if meta.relative_working_dir().is_empty() {
            String::new()
        } else {
            RelativePath::parse(meta.relative_working_dir().as_bytes())
                .map_err(|_| {
                    WorkerError::Protocol(
                        "INVALID_LOCAL_RECORD: local job record contains an unsafe path".into(),
                    )
                })?
                .as_str()
                .to_owned()
        };
        Ok(Self {
            job_id: meta.job_id(),
            worker: meta.worker_name().to_owned(),
            project_id: meta.project_id().to_owned(),
            worktree_id: meta.worktree_id().to_owned(),
            manifest_digest: meta.manifest_digest().to_owned(),
            command_summary: meta.command_summary().clone(),
            relative_working_dir,
            created_at_millis: meta.created_at_millis(),
            status: record.last_status().cloned(),
            remote_uncertainty: record.remote_uncertainty().clone(),
        })
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct StatusReport {
    pub protocol_version: u32,
    pub jobs: Vec<StatusRow>,
    pub omitted: usize,
}

impl StatusReport {
    pub fn new(jobs: Vec<StatusRow>, omitted: usize) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            jobs,
            omitted,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunCompletion {
    pub report: RunReport,
    pub exit_code: u8,
}
