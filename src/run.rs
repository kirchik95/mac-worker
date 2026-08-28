use std::{path::PathBuf, time::Duration};

use serde::Serialize;

use crate::{
    client_state::ClientStateStore,
    config::{Config, WorkerEntry},
    error::WorkerError,
    inputs::RelativePath,
    job::{
        CommandSpec, CommandSummary, JobId, JobState, JobStatus, LocalJobRecord,
        PreacceptanceDisposition, RemoteUncertainty, ResolveOrAbandonRequest, StatusResponse,
    },
    protocol::PROTOCOL_VERSION,
    transfer::RemoteJobClient,
};

pub const STATUS_LIST_LIMIT: usize = 100;
pub const STATUS_REFRESH_LIMIT: usize = 16;
pub const STATUS_REFRESH_DEADLINE: Duration = Duration::from_secs(5);

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

pub struct StatusService<'a> {
    pub config: &'a Config,
    pub client_state: &'a ClientStateStore,
    pub remote: &'a RemoteJobClient<'a>,
}

impl StatusService<'_> {
    pub fn inspect(&self, job_id: Option<JobId>) -> Result<StatusReport, WorkerError> {
        match job_id {
            Some(job_id) => self.inspect_exact(job_id),
            None => self.inspect_list(),
        }
    }

    fn inspect_list(&self) -> Result<StatusReport, WorkerError> {
        let mut records = self.client_state.list_jobs()?;
        records.sort_by(|left, right| {
            right
                .meta()
                .created_at_millis()
                .cmp(&left.meta().created_at_millis())
                .then_with(|| {
                    left.meta()
                        .job_id()
                        .to_string()
                        .cmp(&right.meta().job_id().to_string())
                })
        });
        let omitted = records.len().saturating_sub(STATUS_LIST_LIMIT);
        records.truncate(STATUS_LIST_LIMIT);

        let mut refreshed = 0;
        for record in &mut records {
            if refreshed == STATUS_REFRESH_LIMIT || !eligible_for_list_refresh(record) {
                continue;
            }
            refreshed += 1;
            let Some(worker) = self.config.worker(record.meta().worker_name()) else {
                continue;
            };
            let response = self.remote.status_with_deadline(
                worker,
                record.meta().job_id(),
                STATUS_REFRESH_DEADLINE,
            );
            if let Ok(response) = response
                && let Ok(status) = normalize_authoritative_status(record, response)
            {
                let _ = self.apply_authoritative_status(record.meta().job_id(), status);
            }
            if let Ok(current) = self.client_state.load_job(record.meta().job_id()) {
                *record = current;
            }
        }

        let jobs = records
            .iter()
            .map(StatusRow::try_from_record)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(StatusReport::new(jobs, omitted))
    }

    fn inspect_exact(&self, job_id: JobId) -> Result<StatusReport, WorkerError> {
        let record = self.load_exact(job_id)?;
        let worker = self.worker_for(&record)?;
        let record = if matches!(record.remote_uncertainty(), RemoteUncertainty::None) {
            let response = self.remote.status(worker, job_id)?;
            let status = normalize_authoritative_status(&record, response)?;
            self.apply_authoritative_status(job_id, status)?
        } else {
            let request = ResolveOrAbandonRequest::try_from(&record)?;
            match self.remote.resolve_preacceptance(worker, &request)? {
                PreacceptanceDisposition::Accepted(response) => {
                    let status = normalize_authoritative_status(&record, response)?;
                    self.apply_authoritative_status(job_id, status)?
                }
                PreacceptanceDisposition::Abandoned => {
                    return Err(WorkerError::Protocol(
                        "JOB_ABANDONED: remote job was authoritatively abandoned".into(),
                    ));
                }
                PreacceptanceDisposition::CleanupPending { code } => self
                    .client_state
                    .set_remote_uncertainty(job_id, RemoteUncertainty::cleanup_pending(code)?)?,
                PreacceptanceDisposition::UnknownRemote { code } => self
                    .client_state
                    .set_remote_uncertainty(job_id, RemoteUncertainty::unknown_remote(code)?)?,
            }
        };
        Ok(StatusReport::new(
            vec![StatusRow::try_from_record(&record)?],
            0,
        ))
    }

    fn load_exact(&self, job_id: JobId) -> Result<LocalJobRecord, WorkerError> {
        match self.client_state.load_job(job_id) {
            Ok(record) => Ok(record),
            Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(WorkerError::Config(
                    "JOB_NOT_FOUND: no local job record exists for the requested ID".into(),
                ))
            }
            Err(error) => Err(error),
        }
    }

    fn worker_for<'a>(&'a self, record: &LocalJobRecord) -> Result<&'a WorkerEntry, WorkerError> {
        self.config
            .worker(record.meta().worker_name())
            .ok_or_else(|| {
                WorkerError::Config(
                    "WORKER_NOT_FOUND: the recorded worker is not configured".into(),
                )
            })
    }

    fn apply_authoritative_status(
        &self,
        job_id: JobId,
        status: JobStatus,
    ) -> Result<LocalJobRecord, WorkerError> {
        let current = self.client_state.load_job(job_id)?;
        let preserve_current = observation_is_forward_of(&current, &status);
        if !preserve_current
            && let Err(error) = self.client_state.update_observation(job_id, status.clone())
        {
            let reloaded = self.client_state.load_job(job_id)?;
            if !observation_is_forward_of(&reloaded, &status) {
                return Err(error);
            }
        }
        self.client_state
            .set_remote_uncertainty(job_id, RemoteUncertainty::None)
    }
}

fn observation_is_forward_of(record: &LocalJobRecord, status: &JobStatus) -> bool {
    let Some(observed) = record.last_status() else {
        return false;
    };
    if observed == status {
        return true;
    }
    if observed.updated_at_millis() < status.updated_at_millis() {
        return false;
    }
    if observed.state() == status.state() {
        return status.transition(observed.clone()).is_ok();
    }
    if status.state().is_terminal() || !can_skip_forward(status.state(), observed.state()) {
        return false;
    }
    sticky_identity_matches(status.supervisor_identity(), observed.supervisor_identity())
        && sticky_identity_matches(status.child_identity(), observed.child_identity())
}

fn eligible_for_list_refresh(record: &LocalJobRecord) -> bool {
    !matches!(record.remote_uncertainty(), RemoteUncertainty::None)
        || record
            .last_status()
            .is_none_or(|status| !status.state().is_terminal())
}

fn can_skip_forward(previous: JobState, next: JobState) -> bool {
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

fn sticky_identity_matches<T: Copy + PartialEq>(previous: Option<T>, next: Option<T>) -> bool {
    previous.is_none_or(|identity| next == Some(identity))
}

fn normalize_authoritative_status(
    local: &LocalJobRecord,
    remote: StatusResponse,
) -> Result<JobStatus, WorkerError> {
    remote.validate().map_err(|_| invalid_status_response())?;
    if remote.meta() != local.meta() {
        return Err(invalid_status_response());
    }
    Ok(remote.status().clone())
}

fn invalid_status_response() -> WorkerError {
    WorkerError::Transport {
        code: "INVALID_RESPONSE",
        message: "host response was invalid".into(),
    }
}
