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

enum StatusApply {
    Applied(LocalJobRecord),
    ConcurrentConflict(LocalJobRecord),
}

enum ObservationRelation {
    RemoteAdvances,
    CurrentAtLeastRemote,
    Conflict,
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
            let Some(worker) = self.config.worker(record.meta().worker_name()) else {
                continue;
            };
            refreshed += 1;
            let response = self.remote.status_with_deadline(
                worker,
                record.meta().job_id(),
                STATUS_REFRESH_DEADLINE,
            );
            let response = match response {
                Ok(response) => response,
                Err(_) => {
                    *record = self.load_same_immutable(record)?;
                    continue;
                }
            };
            let status = match normalize_authoritative_status(record, response) {
                Ok(status) => status,
                Err(_) => {
                    *record = self.load_same_immutable(record)?;
                    continue;
                }
            };
            *record = match self.apply_authoritative_status(record, status)? {
                StatusApply::Applied(current) | StatusApply::ConcurrentConflict(current) => current,
            };
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
            let response = match self.remote.status(worker, job_id) {
                Ok(response) => response,
                Err(error) => {
                    self.load_same_immutable(&record)?;
                    return Err(error);
                }
            };
            let status = match normalize_authoritative_status(&record, response) {
                Ok(status) => status,
                Err(error) => {
                    self.load_same_immutable(&record)?;
                    return Err(error);
                }
            };
            match self.apply_authoritative_status(&record, status)? {
                StatusApply::Applied(current) => current,
                StatusApply::ConcurrentConflict(_) => return Err(local_status_conflict()),
            }
        } else {
            let request = ResolveOrAbandonRequest::try_from(&record)?;
            let disposition = match self.remote.resolve_preacceptance(worker, &request) {
                Ok(disposition) => disposition,
                Err(error) => {
                    self.load_same_immutable(&record)?;
                    return Err(error);
                }
            };
            match disposition {
                PreacceptanceDisposition::Accepted(response) => {
                    let status = match normalize_authoritative_status(&record, response) {
                        Ok(status) => status,
                        Err(error) => {
                            self.load_same_immutable(&record)?;
                            return Err(error);
                        }
                    };
                    match self.apply_authoritative_status(&record, status)? {
                        StatusApply::Applied(current) => current,
                        StatusApply::ConcurrentConflict(_) => {
                            return Err(local_status_conflict());
                        }
                    }
                }
                PreacceptanceDisposition::Abandoned => {
                    self.load_same_immutable(&record)?;
                    return Err(WorkerError::Protocol(
                        "JOB_ABANDONED: remote job was authoritatively abandoned".into(),
                    ));
                }
                PreacceptanceDisposition::CleanupPending { code } => {
                    self.client_state.set_remote_uncertainty_if_same_immutable(
                        &record,
                        RemoteUncertainty::cleanup_pending(code)?,
                    )?
                }
                PreacceptanceDisposition::UnknownRemote { code } => {
                    self.client_state.set_remote_uncertainty_if_same_immutable(
                        &record,
                        RemoteUncertainty::unknown_remote(code)?,
                    )?
                }
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
        expected: &LocalJobRecord,
        status: JobStatus,
    ) -> Result<StatusApply, WorkerError> {
        let before = self.load_same_immutable(expected)?;
        match observation_relation(&before, &status) {
            ObservationRelation::CurrentAtLeastRemote => {
                return self
                    .client_state
                    .set_remote_uncertainty_if_same_immutable(expected, RemoteUncertainty::None)
                    .map(StatusApply::Applied);
            }
            ObservationRelation::Conflict => {
                return Ok(StatusApply::ConcurrentConflict(before));
            }
            ObservationRelation::RemoteAdvances => {}
        }
        match self
            .client_state
            .update_observation_if_same_immutable(expected, status.clone())
        {
            Ok(_) => {}
            Err(error @ WorkerError::Protocol(_)) => {
                let current = self.load_same_immutable(expected)?;
                if current.last_status() == before.last_status() {
                    return Err(error);
                }
                return match observation_relation(&current, &status) {
                    ObservationRelation::CurrentAtLeastRemote => self
                        .client_state
                        .set_remote_uncertainty_if_same_immutable(expected, RemoteUncertainty::None)
                        .map(StatusApply::Applied),
                    ObservationRelation::RemoteAdvances | ObservationRelation::Conflict => {
                        Ok(StatusApply::ConcurrentConflict(current))
                    }
                };
            }
            Err(error) => return Err(error),
        }
        self.client_state
            .set_remote_uncertainty_if_same_immutable(expected, RemoteUncertainty::None)
            .map(StatusApply::Applied)
    }

    fn load_same_immutable(
        &self,
        expected: &LocalJobRecord,
    ) -> Result<LocalJobRecord, WorkerError> {
        let current = self.client_state.load_job(expected.meta().job_id())?;
        if same_immutable(&current, expected) {
            Ok(current)
        } else {
            Err(job_id_conflict())
        }
    }
}

fn same_immutable(left: &LocalJobRecord, right: &LocalJobRecord) -> bool {
    left.meta() == right.meta() && left.lease_token() == right.lease_token()
}

fn job_id_conflict() -> WorkerError {
    WorkerError::Protocol(
        "JOB_ID_CONFLICT: job ID is already bound to different immutable metadata".into(),
    )
}

fn local_status_conflict() -> WorkerError {
    WorkerError::Protocol(
        "LOCAL_STATUS_CONFLICT: local job observation conflicts with remote authority".into(),
    )
}

fn accepted_enrichment_is_transitively_forward(previous: &JobStatus, observed: &JobStatus) -> bool {
    previous.state() == JobState::Accepted
        && previous.supervisor_identity().is_none()
        && previous.child_identity().is_none()
        && observed.supervisor_identity().is_some()
        && observed.child_identity().is_some()
        && observed.updated_at_millis() >= previous.updated_at_millis()
}

fn observation_relation(current: &LocalJobRecord, remote: &JobStatus) -> ObservationRelation {
    let Some(current) = current.last_status() else {
        return ObservationRelation::RemoteAdvances;
    };
    if current == remote || status_is_valid_forward(remote, current) {
        ObservationRelation::CurrentAtLeastRemote
    } else if status_is_valid_forward(current, remote) {
        ObservationRelation::RemoteAdvances
    } else {
        ObservationRelation::Conflict
    }
}

fn status_is_valid_forward(previous: &JobStatus, next: &JobStatus) -> bool {
    if next.updated_at_millis() < previous.updated_at_millis() {
        return false;
    }
    if previous == next {
        return true;
    }
    if next.state() == previous.state() {
        return previous.transition(next.clone()).is_ok()
            || accepted_enrichment_is_transitively_forward(previous, next);
    }
    if previous.state().is_terminal() || !can_skip_forward(previous.state(), next.state()) {
        return false;
    }
    sticky_identity_matches(previous.supervisor_identity(), next.supervisor_identity())
        && sticky_identity_matches(previous.child_identity(), next.child_identity())
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
