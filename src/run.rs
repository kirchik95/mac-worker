use std::{
    io::Write,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

use crate::{
    client_state::{
        ClientStateStore, ConditionalStatusUpdate, ObservationRelation,
        authoritative_observation_relation,
    },
    config::{Config, WorkerEntry},
    error::WorkerError,
    inputs::RelativePath,
    job::{
        CommandSpec, CommandSummary, JobId, JobMeta, JobState, JobStatus, JsonEvent,
        LeaseAcquireRequest, LeaseAcquireResponse, LeaseRecord, LeaseToken, LocalJobRecord,
        PreacceptanceDisposition, RemoteUncertainty, RequestFingerprintMaterial,
        ResolveOrAbandonRequest, StatusResponse, SubmitRequest, SubmitResponse,
    },
    paths::PathLayout,
    process::{ProcessPolicy, ProcessRunner},
    project_config::ResourceClass,
    project_state::{
        PreparedProject, ProjectPreparationError, ProjectPreparationRequest,
        ProjectPreparationStage, ProjectState,
    },
    protocol::PROTOCOL_VERSION,
    remote_snapshot::{SnapshotVerifyRequest, VerifiedSnapshotResponse},
    transfer::{
        HostOperation, RemoteJobClient, ResolutionRuntime, RsyncTransport, SshJsonTransport,
        TransferIdentity,
    },
    transport::{SshTransport, WorkersService},
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

#[doc(hidden)]
pub trait JobFollower {
    fn follow(
        &self,
        record: &LocalJobRecord,
        json: bool,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<StatusResponse, WorkerError>;
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStage {
    InitialProjectInspection,
    ExplicitWorkerProbe,
    StableProjectReload,
    SnapshotSelectionAndCapture,
    LocalRecordPublication,
    LeaseAcquire,
    SnapshotUpload,
    SnapshotVerification,
    JobSubmission,
    AcceptedOutputFlush,
    JobFollower,
    SnapshotCleanup,
}

#[doc(hidden)]
pub trait RunObserver {
    fn observe(&self, stage: RunStage) -> Result<(), WorkerError>;
}

struct NoopRunObserver;

impl RunObserver for NoopRunObserver {
    fn observe(&self, _stage: RunStage) -> Result<(), WorkerError> {
        Ok(())
    }
}

static NOOP_RUN_OBSERVER: NoopRunObserver = NoopRunObserver;

pub struct RunService<'a> {
    pub runner: &'a dyn ProcessRunner,
    pub config: &'a Config,
    pub paths: &'a PathLayout,
    pub client_state: &'a ClientStateStore,
    follower: &'a dyn JobFollower,
    resolution_runtime: Option<&'a dyn ResolutionRuntime>,
    observer: &'a dyn RunObserver,
}

impl<'a> RunService<'a> {
    #[doc(hidden)]
    pub fn with_follower(
        runner: &'a dyn ProcessRunner,
        config: &'a Config,
        paths: &'a PathLayout,
        client_state: &'a ClientStateStore,
        follower: &'a dyn JobFollower,
    ) -> Self {
        Self {
            runner,
            config,
            paths,
            client_state,
            follower,
            resolution_runtime: None,
            observer: &NOOP_RUN_OBSERVER,
        }
    }

    #[doc(hidden)]
    pub fn with_follower_and_resolution_runtime(
        runner: &'a dyn ProcessRunner,
        config: &'a Config,
        paths: &'a PathLayout,
        client_state: &'a ClientStateStore,
        follower: &'a dyn JobFollower,
        resolution_runtime: &'a dyn ResolutionRuntime,
    ) -> Self {
        Self {
            runner,
            config,
            paths,
            client_state,
            follower,
            resolution_runtime: Some(resolution_runtime),
            observer: &NOOP_RUN_OBSERVER,
        }
    }

    #[doc(hidden)]
    pub fn with_observer(mut self, observer: &'a dyn RunObserver) -> Self {
        self.observer = observer;
        self
    }

    pub fn submit_and_follow(
        &self,
        request: RunRequest,
        json: bool,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<RunCompletion, WorkerError> {
        let initial = ProjectState::load(self.runner, &request.project, &request.cli_includes)?;
        self.observer.observe(RunStage::InitialProjectInspection)?;
        if !initial.settings.artifacts.include.is_empty()
            || initial.settings.artifacts.max_total_bytes.is_some()
        {
            return Err(WorkerError::Project {
                code: "ARTIFACTS_UNSUPPORTED",
                message: "artifact collection is not supported in this phase".into(),
            });
        }

        let worker = self
            .config
            .worker(&request.worker)
            .ok_or_else(|| {
                WorkerError::Config("WORKER_NOT_FOUND: worker is not configured".into())
            })?
            .clone();
        self.require_eligible_worker(&worker, &initial.requirements)?;
        self.observer.observe(RunStage::ExplicitWorkerProbe)?;

        let prepared = ProjectState::prepare_observed(
            self.runner,
            &self.paths.cache,
            ProjectPreparationRequest {
                project: request.project.clone(),
                cli_includes: request.cli_includes.clone(),
            },
            &initial,
            &|stage| {
                self.observer.observe(match stage {
                    ProjectPreparationStage::StableReload => RunStage::StableProjectReload,
                    ProjectPreparationStage::SnapshotCaptured => {
                        RunStage::SnapshotSelectionAndCapture
                    }
                    ProjectPreparationStage::SnapshotCleanup => RunStage::SnapshotCleanup,
                })
            },
        )
        .map_err(project_preparation_error)?;

        let primary = self.submit_prepared(request, worker, &prepared, json, stdout, stderr);
        match prepared.cleanup_observed(&|stage| {
            debug_assert_eq!(stage, ProjectPreparationStage::SnapshotCleanup);
            self.observer.observe(RunStage::SnapshotCleanup)
        }) {
            Ok(()) => primary,
            Err(cleanup_error) => Err(cleanup_error),
        }
    }

    fn require_eligible_worker(
        &self,
        worker: &WorkerEntry,
        project_requirements: &[String],
    ) -> Result<(), WorkerError> {
        let one_worker = Config {
            version: self.config.version,
            workers: vec![worker.clone()],
        };
        let report = WorkersService::new(SshTransport::new(self.runner))
            .inspect_with_requirements(&one_worker, project_requirements);
        let health = report
            .workers
            .into_iter()
            .next()
            .expect("one explicit worker produces one health result");
        if !matches!(health.status, crate::protocol::HealthStatus::Ready)
            || health.probe.is_none()
            || !health.missing_capabilities.is_empty()
        {
            let code = health
                .error_code
                .unwrap_or_else(|| "WORKER_UNAVAILABLE".into());
            return Err(WorkerError::Unavailable(format!(
                "{code}: explicit worker is not ready"
            )));
        }
        let probe = health
            .probe
            .expect("ready worker health contains its validated probe");
        if probe.slot_state != crate::lease::SlotState::Idle {
            return Err(WorkerError::Capacity {
                code: "CAPACITY_BUSY",
                message: "the explicit worker heavy slot is busy".into(),
            });
        }
        Ok(())
    }

    fn submit_prepared(
        &self,
        request: RunRequest,
        worker: WorkerEntry,
        prepared: &PreparedProject,
        json: bool,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<RunCompletion, WorkerError> {
        let timeout = request.timeout.unwrap_or(prepared.state.settings.timeout);
        let timeout_millis = u64::try_from(timeout.as_millis()).map_err(|_| {
            WorkerError::Config("run timeout is outside the supported range".into())
        })?;
        let resource_class = match prepared.state.settings.resource_class {
            ResourceClass::Heavy => "heavy",
        };
        let created_at_millis = current_time_millis()?;
        let material = RequestFingerprintMaterial::new(
            JobId::generate(),
            self.client_state.client_id(),
            LeaseToken::generate(),
            created_at_millis,
            worker.name.clone(),
            prepared.state.context.project_id.clone(),
            prepared.state.context.worktree_id.clone(),
            prepared.snapshot.digest.clone(),
            prepared.snapshot.manifest.relative_working_dir.clone(),
            timeout_millis,
            resource_class.into(),
            request.command,
        )?;
        let fingerprint = material.fingerprint();
        let record = LocalJobRecord::new(
            JobMeta::new(&material, fingerprint)?,
            material.lease_token(),
            None,
            RemoteUncertainty::None,
        )?;
        self.client_state.create_job(record.clone())?;
        self.observer.observe(RunStage::LocalRecordPublication)?;

        let transport = SshJsonTransport::new(self.runner);
        let remote = match self.resolution_runtime {
            Some(runtime) => RemoteJobClient::new_with_runtime(self.runner, runtime),
            None => RemoteJobClient::new(self.runner),
        };
        let acquire = LeaseAcquireRequest::new(material.clone());
        let acquire_result: Result<LeaseAcquireResponse, WorkerError> = transport.request(
            &worker,
            HostOperation::LeaseAcquire,
            &acquire,
            control_policy(),
        );

        let accepted = match acquire_result {
            Ok(LeaseAcquireResponse::ExistingAccepted { .. }) => {
                match remote.status(&worker, material.job_id()) {
                    Ok(status) => status,
                    Err(error) => {
                        self.resolve_after_acceptance_evidence(&remote, &worker, &record, error)?
                    }
                }
            }
            Ok(LeaseAcquireResponse::Acquired { lease }) => {
                if let Err(error) = require_exact_lease(&material, &lease) {
                    self.resolve_after_error(&remote, &worker, &record, error)?
                } else {
                    self.observer.observe(RunStage::LeaseAcquire)?;
                    let identity = TransferIdentity::from_acquire_request(&acquire)?;
                    if let Err(error) = RsyncTransport::new(self.runner).upload(
                        &worker,
                        &prepared.snapshot,
                        &identity,
                    ) {
                        self.resolve_after_error(&remote, &worker, &record, error)?
                    } else {
                        self.observer.observe(RunStage::SnapshotUpload)?;
                        let verify = SnapshotVerifyRequest::new(
                            material.job_id(),
                            material.client_id(),
                            material.lease_token(),
                            material.fingerprint(),
                            material.project_id().into(),
                            material.worktree_id().into(),
                            material.manifest_digest().into(),
                        )?;
                        let verified: Result<VerifiedSnapshotResponse, WorkerError> = transport
                            .request(
                                &worker,
                                HostOperation::SnapshotVerify,
                                &verify,
                                control_policy(),
                            );
                        match verified {
                            Err(error) => {
                                self.resolve_after_error(&remote, &worker, &record, error)?
                            }
                            Ok(response) => {
                                if let Err(error) = require_exact_verification(&material, &response)
                                {
                                    self.resolve_after_error(&remote, &worker, &record, error)?
                                } else {
                                    self.observer.observe(RunStage::SnapshotVerification)?;
                                    let submit = SubmitRequest::new(material.clone());
                                    let raw: Result<SubmitResponse, WorkerError> = transport
                                        .request(
                                            &worker,
                                            HostOperation::Submit,
                                            &submit,
                                            control_policy(),
                                        );
                                    match raw {
                                        Ok(response) => {
                                            self.observer.observe(RunStage::JobSubmission)?;
                                            match remote.resolve_submission(
                                                &worker,
                                                &submit,
                                                Ok(response),
                                            ) {
                                                Ok(response) => {
                                                    match status_from_submit(&record, response) {
                                                        Ok(response) => response,
                                                        Err(error) => self
                                                            .resolve_after_acceptance_evidence(
                                                                &remote, &worker, &record, error,
                                                            )?,
                                                    }
                                                }
                                                Err(error) => self
                                                    .resolve_after_acceptance_evidence(
                                                        &remote, &worker, &record, error,
                                                    )?,
                                            }
                                        }
                                        Err(error) => self.resolve_after_error(
                                            &remote, &worker, &record, error,
                                        )?,
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(error) => self.resolve_after_error(&remote, &worker, &record, error)?,
        };

        let accepted_status = match normalize_authoritative_status(&record, accepted) {
            Ok(status) => status,
            Err(error) => {
                let resolved =
                    self.resolve_after_acceptance_evidence(&remote, &worker, &record, error)?;
                normalize_authoritative_status(&record, resolved)
                    .map_err(|_| inconsistent_acceptance_evidence())?
            }
        };
        let accepted_record = persist_authoritative(self.client_state, &record, accepted_status)?;
        write_accepted(&accepted_record, json, stdout)?;
        self.observer.observe(RunStage::AcceptedOutputFlush)?;

        let terminal = self
            .follower
            .follow(&accepted_record, json, stdout, stderr)?;
        self.observer.observe(RunStage::JobFollower)?;
        let terminal_status = normalize_authoritative_status(&accepted_record, terminal)
            .map_err(|_| inconsistent_terminal_outcome())?;
        if !terminal_status.state().is_terminal() {
            return Err(inconsistent_terminal_outcome());
        }
        let terminal_record =
            persist_authoritative(self.client_state, &accepted_record, terminal_status.clone())?;
        let terminal_status = terminal_record
            .last_status()
            .cloned()
            .ok_or_else(inconsistent_terminal_outcome)?;
        if !terminal_status.state().is_terminal() {
            return Err(inconsistent_terminal_outcome());
        }
        let exit_code = terminal_exit_code(&terminal_status)?;
        Ok(RunCompletion {
            report: RunReport::new(
                terminal_record.meta().job_id(),
                terminal_record.meta().worker_name().into(),
                terminal_status,
            )?,
            exit_code,
        })
    }

    fn resolve_after_error(
        &self,
        remote: &RemoteJobClient<'_>,
        worker: &WorkerEntry,
        record: &LocalJobRecord,
        original: WorkerError,
    ) -> Result<StatusResponse, WorkerError> {
        self.resolve_disposition(remote, worker, record, original, false)
    }

    fn resolve_after_acceptance_evidence(
        &self,
        remote: &RemoteJobClient<'_>,
        worker: &WorkerEntry,
        record: &LocalJobRecord,
        original: WorkerError,
    ) -> Result<StatusResponse, WorkerError> {
        self.resolve_disposition(remote, worker, record, original, true)
    }

    fn resolve_disposition(
        &self,
        remote: &RemoteJobClient<'_>,
        worker: &WorkerEntry,
        record: &LocalJobRecord,
        original: WorkerError,
        acceptance_evidence: bool,
    ) -> Result<StatusResponse, WorkerError> {
        let resolution = ResolveOrAbandonRequest::try_from(record)?;
        let disposition = match remote.resolve_preacceptance(worker, &resolution) {
            Ok(disposition) => disposition,
            Err(error) if !acceptance_evidence => return Err(error),
            Err(_) => {
                self.persist_uncertainty(record, "ACCEPTANCE_EVIDENCE_CONFLICT", false)?;
                return Err(inconsistent_acceptance_evidence());
            }
        };
        match disposition {
            PreacceptanceDisposition::Accepted(response) => Ok(response),
            PreacceptanceDisposition::Abandoned if !acceptance_evidence => Err(original),
            PreacceptanceDisposition::Abandoned => {
                self.persist_uncertainty(record, "ACCEPTANCE_EVIDENCE_CONFLICT", false)?;
                Err(inconsistent_acceptance_evidence())
            }
            PreacceptanceDisposition::CleanupPending { code } => {
                self.persist_uncertainty(record, &code, true)?;
                Err(recovery_error(record, &code, true))
            }
            PreacceptanceDisposition::UnknownRemote { code } => {
                self.persist_uncertainty(record, &code, false)?;
                Err(recovery_error(record, &code, false))
            }
        }
    }

    fn persist_uncertainty(
        &self,
        record: &LocalJobRecord,
        code: &str,
        cleanup_pending: bool,
    ) -> Result<(), WorkerError> {
        let uncertainty = if cleanup_pending {
            RemoteUncertainty::cleanup_pending(code.to_owned())?
        } else {
            RemoteUncertainty::unknown_remote(code.to_owned())?
        };
        self.client_state
            .set_remote_uncertainty_if_same_immutable(record, uncertainty)?;
        Ok(())
    }
}

fn project_preparation_error(error: ProjectPreparationError) -> WorkerError {
    match error {
        ProjectPreparationError::Selection(failure)
            if matches!(
                failure.code,
                "GIT_INPUT_SELECTION_FAILED" | "INVALID_GIT_OUTPUT" | "INPUT_INSPECTION_FAILED"
            ) =>
        {
            WorkerError::Snapshot {
                code: failure.code,
                message: failure.message,
            }
        }
        ProjectPreparationError::Selection(failure) => WorkerError::Project {
            code: failure.code,
            message: failure.message,
        },
        ProjectPreparationError::Worker { error, .. } => error,
    }
}

fn current_time_millis() -> Result<u64, WorkerError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| WorkerError::Io(std::io::Error::other("system clock predates Unix epoch")))?
        .as_millis();
    u64::try_from(millis)
        .map_err(|_| WorkerError::Io(std::io::Error::other("system clock is out of range")))
}

fn control_policy() -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: 1024 * 1024,
        stderr_limit: 64 * 1024,
        deadline: Duration::from_secs(30),
    }
}

fn require_exact_lease(
    material: &RequestFingerprintMaterial,
    lease: &LeaseRecord,
) -> Result<(), WorkerError> {
    lease.validate().map_err(|_| invalid_status_response())?;
    if lease.job_id() != material.job_id()
        || lease.client_id() != material.client_id()
        || lease.lease_token() != material.lease_token()
        || lease.request_fingerprint() != &material.fingerprint()
        || lease.worker_name() != material.worker_name()
        || lease.project_id() != material.project_id()
        || lease.worktree_id() != material.worktree_id()
        || lease.manifest_digest() != material.manifest_digest()
        || lease.timeout_millis() != material.timeout_millis()
        || lease.resource_class() != material.resource_class()
        || lease.command_summary() != &material.command().summary()?
    {
        return Err(invalid_status_response());
    }
    Ok(())
}

fn require_exact_verification(
    material: &RequestFingerprintMaterial,
    response: &VerifiedSnapshotResponse,
) -> Result<(), WorkerError> {
    response.validate().map_err(|_| invalid_status_response())?;
    if response.job_id() != material.job_id()
        || response.client_id() != material.client_id()
        || response.project_id() != material.project_id()
        || response.worktree_id() != material.worktree_id()
        || response.manifest_digest() != material.manifest_digest()
    {
        return Err(invalid_status_response());
    }
    Ok(())
}

fn status_from_submit(
    record: &LocalJobRecord,
    response: SubmitResponse,
) -> Result<StatusResponse, WorkerError> {
    match response {
        SubmitResponse::Accepted { meta, status } => StatusResponse::new(*meta, status),
        SubmitResponse::Existing { status } => StatusResponse::new(record.meta().clone(), status),
    }
}

fn persist_authoritative(
    client_state: &ClientStateStore,
    expected: &LocalJobRecord,
    status: JobStatus,
) -> Result<LocalJobRecord, WorkerError> {
    match client_state.reconcile_authoritative_status(expected, status)? {
        ConditionalStatusUpdate::Applied(record) => Ok(record),
        ConditionalStatusUpdate::Conflict(_) => Err(local_status_conflict()),
    }
}

fn write_accepted(
    record: &LocalJobRecord,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<(), WorkerError> {
    let status = record
        .last_status()
        .cloned()
        .ok_or_else(invalid_status_response)?;
    if json {
        let event = JsonEvent::Accepted {
            protocol_version: PROTOCOL_VERSION,
            response: SubmitResponse::Accepted {
                meta: Box::new(record.meta().clone()),
                status,
            },
        };
        serde_json::to_writer(&mut *stdout, &event)
            .map_err(|error| WorkerError::Io(std::io::Error::other(error)))?;
        stdout.write_all(b"\n")?;
    } else {
        writeln!(
            stdout,
            "job {} accepted on {}",
            record.meta().job_id(),
            record.meta().worker_name()
        )?;
    }
    stdout.flush()?;
    Ok(())
}

pub(crate) fn terminal_exit_code(status: &JobStatus) -> Result<u8, WorkerError> {
    status
        .validate()
        .map_err(|_| inconsistent_terminal_outcome())?;
    if status.state() == JobState::Succeeded
        && status.exit_code() == Some(0)
        && status.terminating_signal().is_none()
    {
        return Ok(0);
    }
    if status.state() == JobState::Failed
        && let (Some(code), None) = (status.exit_code(), status.terminating_signal())
    {
        return Ok(code);
    }
    if status.cleanup_error_code().is_some() {
        return Err(WorkerError::Protocol(
            "REMOTE_CLEANUP_FAILED: terminal remote cleanup failed".into(),
        ));
    }
    match status.state() {
        JobState::Failed => match status.terminating_signal() {
            Some(signal) if status.exit_code().is_none() => u8::try_from(signal)
                .ok()
                .and_then(|signal| 128_u8.checked_add(signal))
                .ok_or_else(|| {
                    WorkerError::Protocol("remote command signal is out of range".into())
                }),
            _ => Err(inconsistent_terminal_outcome()),
        },
        _ => Err(WorkerError::Protocol(
            "REMOTE_COMMAND_FAILED: remote command ended without an exact exit code".into(),
        )),
    }
}

fn inconsistent_terminal_outcome() -> WorkerError {
    WorkerError::Protocol(
        "REMOTE_OUTCOME_INVALID: remote command ended with an inconsistent outcome".into(),
    )
}

fn inconsistent_acceptance_evidence() -> WorkerError {
    WorkerError::Protocol(
        "ACCEPTANCE_EVIDENCE_CONFLICT: remote acceptance evidence was inconsistent".into(),
    )
}

fn recovery_error(record: &LocalJobRecord, code: &str, cleanup_pending: bool) -> WorkerError {
    let state = if cleanup_pending {
        "remote cleanup is pending"
    } else {
        "remote job state is unknown"
    };
    WorkerError::Protocol(format!(
        "{code}: {state} for job {}; recover with `worker status {}`",
        record.meta().job_id(),
        record.meta().job_id()
    ))
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
                Err(error @ WorkerError::Protocol(_)) => {
                    self.load_same_immutable(record)?;
                    return Err(error);
                }
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
        let published = match self
            .client_state
            .reconcile_authoritative_status(expected, status.clone())
        {
            Ok(ConditionalStatusUpdate::Applied(published)) => published,
            Ok(ConditionalStatusUpdate::Conflict(current)) => {
                return Ok(StatusApply::ConcurrentConflict(current));
            }
            Err(error) => return Err(error),
        };
        let current = self.load_same_immutable(expected)?;
        if current == published
            || matches!(current.remote_uncertainty(), RemoteUncertainty::None)
                && matches!(
                    authoritative_observation_relation(&current, &status),
                    ObservationRelation::CurrentAtLeastRemote
                )
        {
            Ok(StatusApply::Applied(current))
        } else {
            Ok(StatusApply::ConcurrentConflict(current))
        }
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

fn eligible_for_list_refresh(record: &LocalJobRecord) -> bool {
    !matches!(record.remote_uncertainty(), RemoteUncertainty::None)
        || record
            .last_status()
            .is_none_or(|status| !status.state().is_terminal())
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
