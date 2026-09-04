use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, HashSet},
    io::Write,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

use crate::{
    client_state::{
        ClientStateStore, ConditionalStatusUpdate, DispatchCancellationObservation,
        ObservationRelation, authoritative_observation_relation,
    },
    config::{Config, WorkerEntry},
    error::WorkerError,
    inputs::RelativePath,
    job::{
        AdmissionObservation, CancelRequest, CancelResponse, CommandSpec, CommandSummary,
        FleetReconcileJobResult, FleetReconcileRequest, JobId, JobMeta, JobState, JobStatus,
        JsonEvent, LeaseAcquireRequest, LeaseAcquireResponse, LeaseRecord, LeaseToken,
        LocalJobRecord, LogChunk, LogCursor, LogStream, PreacceptanceDisposition, ProcessIdentity,
        QueueCancel, QueueEntry, QueueEntryKind, RemoteUncertainty, RequestFingerprintMaterial,
        ResolveOrAbandonRequest, StatusResponse, SubmitRequest, SubmitResponse, TerminalLogDrain,
    },
    paths::PathLayout,
    process::{ProcessPolicy, ProcessRunner},
    project_config::ResourceClass,
    project_state::{
        PreparedProject, ProjectPreparationError, ProjectPreparationRequest,
        ProjectPreparationStage, ProjectState,
    },
    protocol::{HealthStatus, PROTOCOL_VERSION, WorkerHealth},
    remote_snapshot::{SnapshotVerifyRequest, VerifiedSnapshotResponse},
    scheduler::{
        AffinityHints, CandidateObservation, SchedulerPolicy, Selection, WorkerPreference,
    },
    scheduler_adapter::SchedulerProbeAdapter,
    supervisor::SystemProcessInspector,
    transfer::{
        HostOperation, RemoteJobClient, ResolutionRuntime, RsyncTransport, SshJsonTransport,
        TransferIdentity,
    },
    transport::{SshTransport, WorkersService},
};

pub const STATUS_LIST_LIMIT: usize = 100;
pub const STATUS_REFRESH_LIMIT: usize = 16;
pub const STATUS_REFRESH_DEADLINE: Duration = Duration::from_secs(5);
const DISPATCH_CANCEL_OBSERVE_DEADLINE: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FleetOutcome {
    Reconciled,
    Unavailable,
    InvalidResponse,
}

#[derive(Debug, Clone)]
pub struct FleetWorkerReport {
    pub worker: String,
    pub probe: WorkerHealth,
    pub outcome: FleetOutcome,
    pub jobs: Vec<StatusResponse>,
    pub error_code: Option<String>,
}

impl FleetWorkerReport {
    pub fn outcome(&self) -> FleetOutcome {
        self.outcome
    }
}

#[derive(Debug, Clone)]
pub struct FleetReconcileReport {
    pub generated_at_millis: u64,
    pub workers: Vec<FleetWorkerReport>,
}

impl FleetReconcileReport {
    pub fn workers(&self) -> &[FleetWorkerReport] {
        &self.workers
    }
}

/// Bounded recovery composition. Remote durable status is authoritative; this
/// service merely refreshes the local observation of explicitly known jobs.
pub struct FleetReconciler<'a> {
    pub config: &'a Config,
    pub client_state: &'a ClientStateStore,
    pub remote: RemoteJobClient<'a>,
}

impl FleetReconciler<'_> {
    pub fn reconcile(&self) -> Result<FleetReconcileReport, WorkerError> {
        let generated_at_millis = current_time_millis()?;
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
        records.truncate(STATUS_LIST_LIMIT);

        let mut by_worker = BTreeMap::<String, Vec<LocalJobRecord>>::new();
        for record in records.into_iter().filter(eligible_for_list_refresh) {
            by_worker
                .entry(record.meta().worker_name().to_owned())
                .or_default()
                .push(record);
        }

        let mut workers = Vec::new();
        for worker in &self.config.workers {
            if let Some(records) = by_worker.remove(&worker.name) {
                workers.push(self.reconcile_worker(worker, records, generated_at_millis)?);
            }
        }
        for (_, records) in by_worker {
            self.retain_unknown(&records, "WORKER_NOT_FOUND")?;
        }
        Ok(FleetReconcileReport {
            generated_at_millis,
            workers,
        })
    }

    fn reconcile_worker(
        &self,
        worker: &WorkerEntry,
        records: Vec<LocalJobRecord>,
        now_millis: u64,
    ) -> Result<FleetWorkerReport, WorkerError> {
        let fresh_health = RefCell::new(None);
        let cached = self
            .client_state
            .admission_observation(&worker.name, now_millis, || {
                let one_worker = Config {
                    version: self.config.version,
                    workers: vec![worker.clone()],
                };
                let health = WorkersService::new(SshTransport::new(self.remote.process_runner()))
                    .inspect(&one_worker)
                    .workers
                    .into_iter()
                    .next()
                    .expect("one configured worker produces one health record");
                let candidate = SchedulerProbeAdapter::observations(
                    &one_worker,
                    std::slice::from_ref(&health),
                )?
                .into_iter()
                .next()
                .expect("one configured worker produces one scheduler observation");
                *fresh_health.borrow_mut() = Some(health);
                admission_from_candidate(candidate, now_millis)
            });
        let cached = match cached {
            Ok(cached) => cached,
            Err(error) => {
                let code = error.public_code();
                self.retain_unknown(&records, &code)?;
                return Ok(FleetWorkerReport {
                    worker: worker.name.clone(),
                    probe: unavailable_health(worker, &code),
                    outcome: FleetOutcome::InvalidResponse,
                    jobs: Vec::new(),
                    error_code: Some(code),
                });
            }
        };
        let candidate = candidate_from_admission(cached.observation())?;
        let probe = fresh_health
            .into_inner()
            .unwrap_or_else(|| cached_health(worker, cached.observation()));
        if !candidate.ready() {
            self.retain_unknown(&records, "UNAVAILABLE")?;
            return Ok(FleetWorkerReport {
                worker: worker.name.clone(),
                probe,
                outcome: FleetOutcome::Unavailable,
                jobs: Vec::new(),
                error_code: Some("UNAVAILABLE".into()),
            });
        }

        let request = FleetReconcileRequest::new(
            records
                .iter()
                .map(|record| record.meta().job_id())
                .collect(),
        )?;
        match self.remote.reconcile(worker, &request) {
            Ok(response) => {
                let mut jobs = Vec::new();
                let mut error_code = None;
                for result in response.results() {
                    let record = records
                        .iter()
                        .find(|record| record.meta().job_id() == result.job_id())
                        .expect("remote response was validated against the request");
                    match result {
                        FleetReconcileJobResult::Status { status }
                            if status.meta() == record.meta() =>
                        {
                            let _ = self
                                .client_state
                                .reconcile_authoritative_status(record, status.status().clone())?;
                            jobs.push((**status).clone());
                        }
                        FleetReconcileJobResult::Error { error, .. } => {
                            let code = error.error().code();
                            self.retain_unknown(std::slice::from_ref(record), code)?;
                            error_code.get_or_insert_with(|| code.to_owned());
                        }
                        FleetReconcileJobResult::Status { .. } => {
                            self.retain_unknown(std::slice::from_ref(record), "INVALID_RESPONSE")?;
                            error_code.get_or_insert_with(|| "INVALID_RESPONSE".into());
                        }
                    }
                }
                Ok(FleetWorkerReport {
                    worker: worker.name.clone(),
                    probe,
                    outcome: FleetOutcome::Reconciled,
                    jobs,
                    error_code,
                })
            }
            Err(error) => {
                let code = error.public_code();
                self.retain_unknown(&records, &code)?;
                Ok(FleetWorkerReport {
                    worker: worker.name.clone(),
                    probe,
                    outcome: if code == "INVALID_RESPONSE" {
                        FleetOutcome::InvalidResponse
                    } else {
                        FleetOutcome::Unavailable
                    },
                    jobs: Vec::new(),
                    error_code: Some(code),
                })
            }
        }
    }

    fn retain_unknown(&self, records: &[LocalJobRecord], code: &str) -> Result<(), WorkerError> {
        let uncertainty = RemoteUncertainty::unknown_remote(code)?;
        for record in records {
            self.client_state
                .set_remote_uncertainty_if_same_immutable(record, uncertainty.clone())?;
        }
        Ok(())
    }
}

fn cached_health(worker: &WorkerEntry, observation: &AdmissionObservation) -> WorkerHealth {
    WorkerHealth {
        name: worker.name.clone(),
        ssh: worker.ssh.clone(),
        status: if observation.ready() {
            HealthStatus::Ready
        } else {
            HealthStatus::Unavailable
        },
        probe: None,
        missing_capabilities: Vec::new(),
        error_code: None,
        error_message: None,
    }
}

fn unavailable_health(worker: &WorkerEntry, code: &str) -> WorkerHealth {
    WorkerHealth {
        name: worker.name.clone(),
        ssh: worker.ssh.clone(),
        status: HealthStatus::Unavailable,
        probe: None,
        missing_capabilities: Vec::new(),
        error_code: Some(code.into()),
        error_message: None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRequest {
    pub preference: WorkerPreference,
    pub wait_for_capacity: bool,
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
    AdmissionObservation,
    QueueEnqueue,
    DeadDispatchRecovery,
    QueueClaim,
    StableProjectReload,
    SnapshotSelectionAndCapture,
    LocalRecordPublication,
    LeaseAcquire,
    SnapshotUpload,
    SnapshotVerification,
    JobSubmission,
    AcceptedOutputFlush,
    JobFollower,
    QueueTerminalRemoval,
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

#[doc(hidden)]
pub trait SchedulerRuntime: Send + Sync {
    fn now_millis(&self) -> Result<u64, WorkerError>;
    fn process_identity(&self) -> Result<ProcessIdentity, WorkerError>;
    fn sleep(&self, duration: Duration);
}

struct SystemSchedulerRuntime;

impl SchedulerRuntime for SystemSchedulerRuntime {
    fn now_millis(&self) -> Result<u64, WorkerError> {
        current_time_millis()
    }

    fn process_identity(&self) -> Result<ProcessIdentity, WorkerError> {
        SystemProcessInspector.identity_for_pid(std::process::id())
    }

    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

static SYSTEM_SCHEDULER_RUNTIME: SystemSchedulerRuntime = SystemSchedulerRuntime;

const LOG_CHUNK_LIMIT: u32 = 65_536;
const FOLLOW_POLL_INTERVAL: Duration = Duration::from_secs(1);

pub trait FollowRuntime: Send + Sync {
    fn sleep(&self, duration: Duration);
}

pub struct SystemFollowRuntime;

impl FollowRuntime for SystemFollowRuntime {
    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

pub struct LogsService<'a> {
    pub config: &'a Config,
    pub client_state: &'a ClientStateStore,
    pub remote: &'a RemoteJobClient<'a>,
    pub runtime: &'a dyn FollowRuntime,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
// The public cancellation contract deliberately carries the complete typed
// host response so callers can consume it without an allocation wrapper.
#[allow(clippy::large_enum_variant)]
pub enum CancelReport {
    QueuedCancelled { job_id: JobId },
    RemoteCancelled { response: CancelResponse },
}

pub struct CancelService<'a> {
    config: &'a Config,
    client_state: &'a ClientStateStore,
    remote: RemoteJobClient<'a>,
    scheduler_runtime: &'a dyn SchedulerRuntime,
}

pub struct RunService<'a> {
    pub runner: &'a dyn ProcessRunner,
    pub config: &'a Config,
    pub paths: &'a PathLayout,
    pub client_state: &'a ClientStateStore,
    follower: &'a dyn JobFollower,
    resolution_runtime: Option<&'a dyn ResolutionRuntime>,
    observer: &'a dyn RunObserver,
    scheduler_runtime: &'a dyn SchedulerRuntime,
}

#[derive(Clone, Copy)]
struct RunIdentity {
    job_id: JobId,
    lease_token: LeaseToken,
    created_at_millis: u64,
    dispatch_owner: ProcessIdentity,
}

struct AdmissionRound {
    observations: Vec<CandidateObservation>,
    refreshed_ready: HashSet<String>,
}

struct ClaimedRun {
    identity: RunIdentity,
    worker: WorkerEntry,
}

impl<'a> CancelService<'a> {
    pub fn new(
        runner: &'a dyn ProcessRunner,
        config: &'a Config,
        client_state: &'a ClientStateStore,
    ) -> Self {
        Self {
            config,
            client_state,
            remote: RemoteJobClient::new(runner),
            scheduler_runtime: &SYSTEM_SCHEDULER_RUNTIME,
        }
    }

    #[doc(hidden)]
    pub fn with_scheduler_runtime(mut self, runtime: &'a dyn SchedulerRuntime) -> Self {
        self.scheduler_runtime = runtime;
        self
    }

    pub fn cancel(&self, job_id: JobId) -> Result<CancelReport, WorkerError> {
        let requested_at = self.scheduler_runtime.now_millis()?;
        match self
            .client_state
            .request_queue_cancel(job_id, requested_at)?
        {
            Some(QueueCancel::RemovedWaiting { job_id }) => {
                Ok(CancelReport::QueuedCancelled { job_id })
            }
            Some(QueueCancel::RequestedDispatch {
                job_id,
                dispatch_owner,
            }) => self.cancel_requested_dispatch(job_id, dispatch_owner),
            None => {
                let record = load_exact_job(self.client_state, job_id)?;
                self.cancel_local_record(&record, None)
            }
        }
    }

    fn cancel_requested_dispatch(
        &self,
        job_id: JobId,
        dispatch_owner: ProcessIdentity,
    ) -> Result<CancelReport, WorkerError> {
        let started_at = self.scheduler_runtime.now_millis()?;
        loop {
            if let Some(record) = load_optional_job(self.client_state, job_id)? {
                return self.cancel_local_record(&record, Some(dispatch_owner));
            }

            match self
                .client_state
                .observe_dispatch_cancellation(job_id, dispatch_owner)?
            {
                DispatchCancellationObservation::NotRequested => {
                    return Err(WorkerError::Queue {
                        code: "CANCEL_REQUEST_LOST",
                        message: "durable dispatch cancellation request was unexpectedly cleared"
                            .into(),
                    });
                }
                DispatchCancellationObservation::WaitingCancelled => {
                    let removed = self.client_state.remove_queued(job_id)?;
                    if removed.is_some() {
                        return Ok(CancelReport::QueuedCancelled { job_id });
                    }
                }
                DispatchCancellationObservation::Gone => {
                    // Queue retirement without a local record is only legal
                    // before local publication, hence before every remote
                    // boundary. Recheck once under no lock before reporting
                    // queued cancellation.
                    if load_optional_job(self.client_state, job_id)?.is_none() {
                        return Ok(CancelReport::QueuedCancelled { job_id });
                    }
                }
                DispatchCancellationObservation::Requested => {}
            }

            let now = self.scheduler_runtime.now_millis()?;
            let elapsed = now.saturating_sub(started_at);
            if elapsed >= DISPATCH_CANCEL_OBSERVE_DEADLINE.as_millis() as u64 {
                return Err(WorkerError::Queue {
                    code: "CANCEL_PENDING",
                    message: "dispatch owner has not yet reached a durable cancellation boundary"
                        .into(),
                });
            }
            let remaining = DISPATCH_CANCEL_OBSERVE_DEADLINE
                .checked_sub(Duration::from_millis(elapsed))
                .unwrap_or_default();
            self.scheduler_runtime
                .sleep(remaining.min(Duration::from_secs(1)));
        }
    }

    fn cancel_local_record(
        &self,
        record: &LocalJobRecord,
        dispatch_owner: Option<ProcessIdentity>,
    ) -> Result<CancelReport, WorkerError> {
        record.validate()?;
        if !matches!(record.remote_uncertainty(), RemoteUncertainty::None) {
            let code = record
                .remote_uncertainty()
                .code()
                .unwrap_or("UNKNOWN_REMOTE");
            return Err(recovery_error(
                record,
                code,
                matches!(
                    record.remote_uncertainty(),
                    RemoteUncertainty::CleanupPending { .. }
                ),
            ));
        }
        let worker = configured_worker(self.config, record)?;
        let resolution = ResolveOrAbandonRequest::from_local_record(record)?;
        let resolved = self
            .remote
            .resolve_preacceptance_with_receipt(worker, &resolution)?;
        match resolved.disposition() {
            PreacceptanceDisposition::Accepted(authoritative) => {
                // Original-ID acceptance is durable before cancellation. A
                // lost cancel reply must not let later abandonment erase the
                // only exact evidence that this identity reached the host.
                let accepted_record =
                    persist_resolved_acceptance(self.client_state, record, authoritative)?;
                let response = match terminal_cancel_response(&accepted_record)? {
                    Some(response) => response,
                    None => self.cancel_accepted(worker, &accepted_record)?,
                };
                self.persist_cancel_response(&accepted_record, response.clone(), dispatch_owner)?;
                Ok(CancelReport::RemoteCancelled { response })
            }
            PreacceptanceDisposition::Abandoned => {
                reject_local_abandonment_after_acceptance_evidence(self.client_state, record)?;
                if let Some(owner) = dispatch_owner {
                    let receipt = resolved.abandonment_receipt().ok_or_else(|| {
                        WorkerError::Protocol(
                            "ABANDONMENT_PROOF_MISSING: remote abandonment lacked a receipt".into(),
                        )
                    })?;
                    self.client_state
                        .record_preacceptance_abandoned(receipt, owner)?;
                    self.client_state
                        .remove_after_terminal(record.meta().job_id(), owner)?;
                }
                Ok(CancelReport::QueuedCancelled {
                    job_id: record.meta().job_id(),
                })
            }
            PreacceptanceDisposition::CleanupPending { code } => {
                self.persist_cancel_uncertainty(record, code, true)
            }
            PreacceptanceDisposition::UnknownRemote { code } => {
                self.persist_cancel_uncertainty(record, code, false)
            }
        }
    }

    fn cancel_accepted(
        &self,
        worker: &WorkerEntry,
        record: &LocalJobRecord,
    ) -> Result<CancelResponse, WorkerError> {
        let request = CancelRequest::from_local_record(record)?;
        match self.remote.cancel(worker, &request) {
            Ok(response) if response.status().status().state().is_terminal() => Ok(response),
            Ok(_) => Err(WorkerError::Protocol(
                "CANCEL_RESPONSE_INVALID: host cancellation did not return a terminal status"
                    .into(),
            )),
            Err(error) if !is_ambiguous_cancel_transport_error(&error) => Err(error),
            Err(_) => {
                // A lost cancel response is never retried blindly. Re-resolve
                // the original job ID and accept only an exact terminal
                // observation; otherwise retain recovery-required state.
                match self.remote.status(worker, record.meta().job_id()) {
                    Ok(status) => {
                        let status = normalize_authoritative_status(record, status)?;
                        if status.state().is_terminal() {
                            let response = StatusResponse::new(record.meta().clone(), status)?;
                            CancelResponse::new(response)
                        } else {
                            self.persist_cancel_uncertainty(
                                record,
                                "CANCEL_TRANSPORT_AMBIGUOUS",
                                false,
                            )
                        }
                    }
                    Err(_) => {
                        self.persist_cancel_uncertainty(record, "CANCEL_TRANSPORT_AMBIGUOUS", false)
                    }
                }
            }
        }
    }

    fn persist_cancel_response(
        &self,
        record: &LocalJobRecord,
        response: CancelResponse,
        dispatch_owner: Option<ProcessIdentity>,
    ) -> Result<(), WorkerError> {
        let status = normalize_authoritative_status(record, response.status().clone())?;
        let persisted = persist_authoritative(self.client_state, record, status)?;
        if let Some(owner) = dispatch_owner {
            if !persisted
                .last_status()
                .is_some_and(|status| status.state().is_terminal())
            {
                return Err(WorkerError::Protocol(
                    "CANCEL_RESPONSE_INVALID: cancellation status was not terminal".into(),
                ));
            }
            retire_terminal_queue_row(self.client_state, &persisted, owner)?;
        }
        Ok(())
    }

    fn persist_cancel_uncertainty<T>(
        &self,
        record: &LocalJobRecord,
        code: &str,
        cleanup_pending: bool,
    ) -> Result<T, WorkerError> {
        let uncertainty = if cleanup_pending {
            RemoteUncertainty::cleanup_pending(code.to_owned())?
        } else {
            RemoteUncertainty::unknown_remote(code.to_owned())?
        };
        self.client_state
            .set_remote_uncertainty_if_same_immutable(record, uncertainty)?;
        Err(recovery_error(record, code, cleanup_pending))
    }
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
            scheduler_runtime: &SYSTEM_SCHEDULER_RUNTIME,
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
            scheduler_runtime: &SYSTEM_SCHEDULER_RUNTIME,
        }
    }

    #[doc(hidden)]
    pub fn with_observer(mut self, observer: &'a dyn RunObserver) -> Self {
        self.observer = observer;
        self
    }

    #[doc(hidden)]
    pub fn with_scheduler_runtime(mut self, runtime: &'a dyn SchedulerRuntime) -> Self {
        self.scheduler_runtime = runtime;
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

        self.validate_preference(&request.preference)?;
        FleetReconciler {
            config: self.config,
            client_state: self.client_state,
            remote: RemoteJobClient::new(self.runner),
        }
        .reconcile()?;
        let first_observed_at = self.scheduler_runtime.now_millis()?;
        let first_round = self.observe_admission(&request.preference, first_observed_at)?;
        self.observer.observe(RunStage::AdmissionObservation)?;
        let affinity = self
            .client_state
            .affinity_hints(&initial.context.project_id, &initial.context.worktree_id)?;
        if !request.wait_for_capacity
            && matches!(
                SchedulerPolicy::select(
                    &first_round.observations,
                    &initial.requirements,
                    &request.preference,
                    &affinity,
                ),
                Selection::NoEligible { .. }
            )
        {
            return Err(capacity_busy());
        }

        let identity = RunIdentity {
            job_id: JobId::generate(),
            lease_token: LeaseToken::generate(),
            created_at_millis: self.scheduler_runtime.now_millis()?,
            dispatch_owner: self.scheduler_runtime.process_identity()?,
        };
        let queue_entry = QueueEntry::new(
            identity.job_id,
            self.client_state.client_id(),
            initial.context.project_id.clone(),
            initial.context.worktree_id.clone(),
            request.command.summary()?,
            initial.requirements.clone(),
            request.preference.clone(),
            QueueEntryKind::Batch,
            None,
            identity.dispatch_owner,
            identity.created_at_millis,
        )?;
        self.client_state.enqueue(queue_entry)?;
        if let Err(error) = self.observer.observe(RunStage::QueueEnqueue) {
            self.client_state.remove_queued(identity.job_id)?;
            return Err(error);
        }
        if let Err(error) = self.client_state.recover_dead_dispatches() {
            self.client_state.remove_queued(identity.job_id)?;
            return Err(error);
        }
        if let Err(error) = self.observer.observe(RunStage::DeadDispatchRecovery) {
            self.client_state.remove_queued(identity.job_id)?;
            return Err(error);
        }
        let claimed = self.claim_worker(&request, &initial, identity, first_round)?;
        if self.cancel_before_local_record(&claimed)? {
            return Err(dispatch_cancelled());
        }

        let prepared = match ProjectState::prepare_observed(
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
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                self.client_state
                    .revert_dispatch(claimed.identity.job_id, claimed.identity.dispatch_owner)?;
                return Err(project_preparation_error(error));
            }
        };

        let primary = self.submit_prepared(request, claimed, &prepared, json, stdout, stderr);
        match prepared.cleanup_observed(&|stage| {
            debug_assert_eq!(stage, ProjectPreparationStage::SnapshotCleanup);
            self.observer.observe(RunStage::SnapshotCleanup)
        }) {
            Ok(()) => primary,
            Err(cleanup_error) => Err(cleanup_error),
        }
    }

    fn cancel_before_local_record(&self, claimed: &ClaimedRun) -> Result<bool, WorkerError> {
        match self.client_state.observe_dispatch_cancellation(
            claimed.identity.job_id,
            claimed.identity.dispatch_owner,
        )? {
            DispatchCancellationObservation::NotRequested => Ok(false),
            DispatchCancellationObservation::Requested => self
                .client_state
                .remove_cancelled_dispatch_before_local_record(
                    claimed.identity.job_id,
                    claimed.identity.dispatch_owner,
                )
                .map(|removed| removed.is_some()),
            DispatchCancellationObservation::WaitingCancelled => self
                .client_state
                .remove_queued(claimed.identity.job_id)
                .map(|removed| removed.is_some()),
            DispatchCancellationObservation::Gone => Ok(true),
        }
    }

    fn validate_preference(&self, preference: &WorkerPreference) -> Result<(), WorkerError> {
        if let WorkerPreference::Pinned { worker } = preference
            && self.config.worker(worker).is_none()
        {
            return Err(WorkerError::Config(
                "WORKER_NOT_FOUND: worker is not configured".into(),
            ));
        }
        Ok(())
    }

    fn observe_admission(
        &self,
        preference: &WorkerPreference,
        observed_at_millis: u64,
    ) -> Result<AdmissionRound, WorkerError> {
        let mut observations = Vec::new();
        let mut refreshed_ready = HashSet::new();
        for worker in self
            .config
            .workers
            .iter()
            .filter(|worker| match preference {
                WorkerPreference::Automatic => true,
                WorkerPreference::Pinned { worker: pinned } => worker.name == *pinned,
            })
        {
            let refreshed = Cell::new(false);
            let cached = self.client_state.admission_observation(
                &worker.name,
                observed_at_millis,
                || {
                    refreshed.set(true);
                    let one_worker = Config {
                        version: self.config.version,
                        workers: vec![worker.clone()],
                    };
                    let report =
                        WorkersService::new(SshTransport::new(self.runner)).inspect(&one_worker);
                    let candidate =
                        SchedulerProbeAdapter::observations(&one_worker, &report.workers)?
                            .into_iter()
                            .next()
                            .expect("one configured worker produces one scheduler observation");
                    admission_from_candidate(candidate, observed_at_millis)
                },
            )?;
            let candidate = candidate_from_admission(cached.observation())?;
            if refreshed.get() && candidate.ready() {
                refreshed_ready.insert(worker.name.clone());
            }
            observations.push(candidate);
        }
        Ok(AdmissionRound {
            observations,
            refreshed_ready,
        })
    }

    fn claim_worker(
        &self,
        request: &RunRequest,
        initial: &ProjectState,
        identity: RunIdentity,
        mut round: AdmissionRound,
    ) -> Result<ClaimedRun, WorkerError> {
        loop {
            let affinity = match self
                .client_state
                .affinity_hints(&initial.context.project_id, &initial.context.worktree_id)
            {
                Ok(affinity) => affinity,
                Err(error) => {
                    self.client_state.remove_queued(identity.job_id)?;
                    return Err(error);
                }
            };
            let ranked_workers = ranked_worker_names(
                &round.observations,
                &initial.requirements,
                &request.preference,
                &affinity,
            );
            let claimed_at_millis = match self.scheduler_runtime.now_millis() {
                Ok(now) => now,
                Err(error) => {
                    self.client_state.remove_queued(identity.job_id)?;
                    return Err(error);
                }
            };
            let claim = match self.client_state.claim_next(
                identity.dispatch_owner,
                &ranked_workers,
                claimed_at_millis,
            ) {
                Ok(claim) => claim,
                Err(error) => {
                    self.client_state.remove_queued(identity.job_id)?;
                    return Err(error);
                }
            };
            if let Some(claim) = claim {
                if claim.entry().job_id() != identity.job_id {
                    self.client_state
                        .revert_dispatch(claim.entry().job_id(), identity.dispatch_owner)?;
                    self.client_state.remove_queued(identity.job_id)?;
                    return Err(WorkerError::Queue {
                        code: "QUEUE_CLAIM_CONFLICT",
                        message: "scheduler claimed a different queue row".into(),
                    });
                }
                let selected_worker = match claim.entry().state() {
                    crate::job::QueueState::Dispatching {
                        selected_worker, ..
                    } => selected_worker.clone(),
                    crate::job::QueueState::Waiting { .. } => {
                        unreachable!("a successful queue claim is durably dispatching")
                    }
                };
                if let Err(error) = self.observer.observe(RunStage::QueueClaim) {
                    self.client_state
                        .revert_dispatch(identity.job_id, identity.dispatch_owner)?;
                    return Err(error);
                }
                if round.refreshed_ready.contains(&selected_worker)
                    && let Err(error) = self.client_state.record_affinity(
                        &initial.context.project_id,
                        &initial.context.worktree_id,
                        &selected_worker,
                        claimed_at_millis,
                    )
                {
                    self.client_state
                        .revert_dispatch(identity.job_id, identity.dispatch_owner)?;
                    return Err(error);
                }
                return match self.config.worker(&selected_worker) {
                    Some(worker) => Ok(ClaimedRun {
                        identity,
                        worker: worker.clone(),
                    }),
                    None => {
                        self.client_state
                            .revert_dispatch(identity.job_id, identity.dispatch_owner)?;
                        Err(WorkerError::Config(
                            "WORKER_NOT_FOUND: worker is not configured".into(),
                        ))
                    }
                };
            }

            if !request.wait_for_capacity {
                self.client_state.remove_queued(identity.job_id)?;
                return Err(capacity_busy());
            }
            self.scheduler_runtime.sleep(Duration::from_secs(1));
            let observed_at_millis = match self.scheduler_runtime.now_millis() {
                Ok(now) => now,
                Err(error) => {
                    self.client_state.remove_queued(identity.job_id)?;
                    return Err(error);
                }
            };
            round = match self.observe_admission(&request.preference, observed_at_millis) {
                Ok(round) => round,
                Err(error) => {
                    self.client_state.remove_queued(identity.job_id)?;
                    return Err(error);
                }
            };
        }
    }

    fn submit_prepared(
        &self,
        request: RunRequest,
        claimed: ClaimedRun,
        prepared: &PreparedProject,
        json: bool,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<RunCompletion, WorkerError> {
        let ClaimedRun { identity, worker } = claimed;
        let local = (|| {
            let timeout = request.timeout.unwrap_or(prepared.state.settings.timeout);
            let timeout_millis = u64::try_from(timeout.as_millis()).map_err(|_| {
                WorkerError::Config("run timeout is outside the supported range".into())
            })?;
            let resource_class = match prepared.state.settings.resource_class {
                ResourceClass::Heavy => "heavy",
            };
            let material = RequestFingerprintMaterial::new(
                identity.job_id,
                self.client_state.client_id(),
                identity.lease_token,
                identity.created_at_millis,
                worker.name.clone(),
                prepared.state.context.project_id.clone(),
                prepared.state.context.worktree_id.clone(),
                prepared.snapshot.digest.clone(),
                prepared.snapshot.manifest.relative_working_dir.clone(),
                timeout_millis,
                resource_class.into(),
                request.command,
            )?;
            let record = LocalJobRecord::new(
                JobMeta::new(&material, material.fingerprint())?,
                material.lease_token(),
                None,
                RemoteUncertainty::None,
            )?;
            let acquire = LeaseAcquireRequest::new(material.clone());
            let transfer_identity = TransferIdentity::from_acquire_request(&acquire)?;
            let verify = SnapshotVerifyRequest::new(
                material.job_id(),
                material.client_id(),
                material.lease_token(),
                material.fingerprint(),
                material.project_id().into(),
                material.worktree_id().into(),
                material.manifest_digest().into(),
            )?;
            let submit = SubmitRequest::new(material.clone());
            self.client_state.create_job(record.clone())?;
            self.observer.observe(RunStage::LocalRecordPublication)?;
            Ok::<_, WorkerError>((material, record, acquire, transfer_identity, verify, submit))
        })();
        let (material, record, acquire, transfer_identity, verify, submit) = match local {
            Ok(local) => local,
            Err(error) => {
                self.client_state
                    .revert_dispatch(identity.job_id, identity.dispatch_owner)?;
                return Err(error);
            }
        };

        let transport = SshJsonTransport::new(self.runner);
        let remote = match self.resolution_runtime {
            Some(runtime) => RemoteJobClient::new_with_runtime(self.runner, runtime),
            None => RemoteJobClient::new(self.runner),
        };
        if self.cancel_after_local_record(&remote, &worker, &record, identity.dispatch_owner)? {
            return self.cancellation_handoff(&record, identity.dispatch_owner);
        }
        let acquire_result: Result<LeaseAcquireResponse, WorkerError> = transport.request(
            &worker,
            HostOperation::LeaseAcquire,
            &acquire,
            control_policy(),
        );

        let accepted = match acquire_result {
            Ok(LeaseAcquireResponse::ExistingAccepted { .. }) => {
                if self.cancel_after_local_record(
                    &remote,
                    &worker,
                    &record,
                    identity.dispatch_owner,
                )? {
                    return self.cancellation_handoff(&record, identity.dispatch_owner);
                }
                match remote.status(&worker, material.job_id()) {
                    Ok(status) => status,
                    Err(error) => self.resolve_after_acceptance_evidence(
                        &remote,
                        &worker,
                        &record,
                        identity.dispatch_owner,
                        error,
                    )?,
                }
            }
            Ok(LeaseAcquireResponse::Acquired { lease }) => {
                if let Err(error) = require_exact_lease(&material, &lease) {
                    self.resolve_after_error(
                        &remote,
                        &worker,
                        &record,
                        identity.dispatch_owner,
                        error,
                    )?
                } else {
                    self.observer.observe(RunStage::LeaseAcquire)?;
                    if self.cancel_after_local_record(
                        &remote,
                        &worker,
                        &record,
                        identity.dispatch_owner,
                    )? {
                        return self.cancellation_handoff(&record, identity.dispatch_owner);
                    }
                    if let Err(error) = RsyncTransport::new(self.runner).upload(
                        &worker,
                        &prepared.snapshot,
                        &transfer_identity,
                    ) {
                        self.resolve_after_error(
                            &remote,
                            &worker,
                            &record,
                            identity.dispatch_owner,
                            error,
                        )?
                    } else {
                        self.observer.observe(RunStage::SnapshotUpload)?;
                        if self.cancel_after_local_record(
                            &remote,
                            &worker,
                            &record,
                            identity.dispatch_owner,
                        )? {
                            return self.cancellation_handoff(&record, identity.dispatch_owner);
                        }
                        let verified: Result<VerifiedSnapshotResponse, WorkerError> = transport
                            .request(
                                &worker,
                                HostOperation::SnapshotVerify,
                                &verify,
                                control_policy(),
                            );
                        match verified {
                            Err(error) => self.resolve_after_error(
                                &remote,
                                &worker,
                                &record,
                                identity.dispatch_owner,
                                error,
                            )?,
                            Ok(response) => {
                                if let Err(error) = require_exact_verification(&material, &response)
                                {
                                    self.resolve_after_error(
                                        &remote,
                                        &worker,
                                        &record,
                                        identity.dispatch_owner,
                                        error,
                                    )?
                                } else {
                                    self.observer.observe(RunStage::SnapshotVerification)?;
                                    if self.cancel_after_local_record(
                                        &remote,
                                        &worker,
                                        &record,
                                        identity.dispatch_owner,
                                    )? {
                                        return self.cancellation_handoff(
                                            &record,
                                            identity.dispatch_owner,
                                        );
                                    }
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
                                                                &remote,
                                                                &worker,
                                                                &record,
                                                                identity.dispatch_owner,
                                                                error,
                                                            )?,
                                                    }
                                                }
                                                Err(error) => self
                                                    .resolve_after_acceptance_evidence(
                                                        &remote,
                                                        &worker,
                                                        &record,
                                                        identity.dispatch_owner,
                                                        error,
                                                    )?,
                                            }
                                        }
                                        Err(error) => self.resolve_after_error(
                                            &remote,
                                            &worker,
                                            &record,
                                            identity.dispatch_owner,
                                            error,
                                        )?,
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(error @ WorkerError::Capacity { .. }) => {
                self.client_state
                    .revert_dispatch(identity.job_id, identity.dispatch_owner)?;
                return Err(error);
            }
            Err(error) => {
                self.resolve_after_error(&remote, &worker, &record, identity.dispatch_owner, error)?
            }
        };

        // Submit/status resolution itself can be the boundary during which a
        // durable cancellation arrives. Recheck before publishing Accepted or
        // handing the record to the follower so the dispatch owner, rather
        // than a bounded waiting caller, resolves and fences that exact job.
        if self.cancel_after_local_record(&remote, &worker, &record, identity.dispatch_owner)? {
            return self.cancellation_handoff(&record, identity.dispatch_owner);
        }

        let accepted_status = match normalize_authoritative_status(&record, accepted) {
            Ok(status) => status,
            Err(error) => {
                let resolved = self.resolve_after_acceptance_evidence(
                    &remote,
                    &worker,
                    &record,
                    identity.dispatch_owner,
                    error,
                )?;
                normalize_authoritative_status(&record, resolved)
                    .map_err(|_| inconsistent_acceptance_evidence())?
            }
        };
        let accepted_record = persist_authoritative(self.client_state, &record, accepted_status)?;
        if let Some(completion) =
            terminal_handoff_before_follow(self.client_state, &record, identity.dispatch_owner)?
        {
            return Ok(completion);
        }
        write_accepted(&accepted_record, json, stdout)?;
        self.observer.observe(RunStage::AcceptedOutputFlush)?;
        if self.cancel_after_local_record(&remote, &worker, &record, identity.dispatch_owner)? {
            return self.cancellation_handoff(&record, identity.dispatch_owner);
        }
        if let Some(completion) =
            terminal_handoff_before_follow(self.client_state, &record, identity.dispatch_owner)?
        {
            return Ok(completion);
        }

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
        retire_terminal_queue_row(self.client_state, &terminal_record, identity.dispatch_owner)?;
        self.observer.observe(RunStage::QueueTerminalRemoval)?;
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

    fn cancel_after_local_record(
        &self,
        remote: &RemoteJobClient<'_>,
        worker: &WorkerEntry,
        record: &LocalJobRecord,
        dispatch_owner: ProcessIdentity,
    ) -> Result<bool, WorkerError> {
        match self
            .client_state
            .observe_dispatch_cancellation(record.meta().job_id(), dispatch_owner)?
        {
            DispatchCancellationObservation::NotRequested => return Ok(false),
            // A public canceller has already observed either a row retirement
            // or a state transition it owns. This dispatch must not cross a
            // new remote boundary; it leaves resolution to that owner.
            DispatchCancellationObservation::WaitingCancelled
            | DispatchCancellationObservation::Gone => return Ok(true),
            DispatchCancellationObservation::Requested => {}
        }

        let resolution = ResolveOrAbandonRequest::from_local_record(record)?;
        let resolved = remote.resolve_preacceptance_with_receipt(worker, &resolution)?;
        match resolved.disposition() {
            PreacceptanceDisposition::Accepted(authoritative) => {
                let accepted_record =
                    persist_resolved_acceptance(self.client_state, record, authoritative)?;
                if accepted_record
                    .last_status()
                    .is_some_and(|status| status.state().is_terminal())
                {
                    retire_terminal_queue_row(self.client_state, &accepted_record, dispatch_owner)?;
                    return Ok(true);
                }
                let request = CancelRequest::from_local_record(&accepted_record)?;
                let response = match remote.cancel(worker, &request) {
                    Ok(response) if response.status().status().state().is_terminal() => response,
                    Ok(_) => {
                        self.persist_uncertainty(
                            &accepted_record,
                            "CANCEL_RESPONSE_INVALID",
                            false,
                        )?;
                        return Err(recovery_error(
                            &accepted_record,
                            "CANCEL_RESPONSE_INVALID",
                            false,
                        ));
                    }
                    Err(error) if !is_ambiguous_cancel_transport_error(&error) => {
                        return Err(error);
                    }
                    Err(_) => match remote.status(worker, accepted_record.meta().job_id()) {
                        Ok(status) => {
                            let status = normalize_authoritative_status(&accepted_record, status)?;
                            if status.state().is_terminal() {
                                CancelResponse::new(StatusResponse::new(
                                    accepted_record.meta().clone(),
                                    status,
                                )?)?
                            } else {
                                self.persist_uncertainty(
                                    &accepted_record,
                                    "CANCEL_TRANSPORT_AMBIGUOUS",
                                    false,
                                )?;
                                return Err(recovery_error(
                                    &accepted_record,
                                    "CANCEL_TRANSPORT_AMBIGUOUS",
                                    false,
                                ));
                            }
                        }
                        Err(_) => {
                            self.persist_uncertainty(
                                &accepted_record,
                                "CANCEL_TRANSPORT_AMBIGUOUS",
                                false,
                            )?;
                            return Err(recovery_error(
                                &accepted_record,
                                "CANCEL_TRANSPORT_AMBIGUOUS",
                                false,
                            ));
                        }
                    },
                };
                let status =
                    normalize_authoritative_status(&accepted_record, response.status().clone())?;
                let persisted = persist_authoritative(self.client_state, &accepted_record, status)?;
                retire_terminal_queue_row(self.client_state, &persisted, dispatch_owner)?;
                Ok(true)
            }
            PreacceptanceDisposition::Abandoned => {
                reject_local_abandonment_after_acceptance_evidence(self.client_state, record)?;
                let receipt = resolved.abandonment_receipt().ok_or_else(|| {
                    WorkerError::Protocol(
                        "ABANDONMENT_PROOF_MISSING: remote abandonment lacked a receipt".into(),
                    )
                })?;
                self.client_state
                    .record_preacceptance_abandoned(receipt, dispatch_owner)?;
                self.client_state
                    .remove_after_terminal(record.meta().job_id(), dispatch_owner)?;
                Ok(true)
            }
            PreacceptanceDisposition::CleanupPending { code } => {
                self.persist_uncertainty(record, code, true)?;
                Err(recovery_error(record, code, true))
            }
            PreacceptanceDisposition::UnknownRemote { code } => {
                self.persist_uncertainty(record, code, false)?;
                Err(recovery_error(record, code, false))
            }
        }
    }

    fn resolve_after_error(
        &self,
        remote: &RemoteJobClient<'_>,
        worker: &WorkerEntry,
        record: &LocalJobRecord,
        dispatch_owner: ProcessIdentity,
        original: WorkerError,
    ) -> Result<StatusResponse, WorkerError> {
        self.resolve_disposition(remote, worker, record, dispatch_owner, original, false)
    }

    fn resolve_after_acceptance_evidence(
        &self,
        remote: &RemoteJobClient<'_>,
        worker: &WorkerEntry,
        record: &LocalJobRecord,
        dispatch_owner: ProcessIdentity,
        original: WorkerError,
    ) -> Result<StatusResponse, WorkerError> {
        self.resolve_disposition(remote, worker, record, dispatch_owner, original, true)
    }

    fn resolve_disposition(
        &self,
        remote: &RemoteJobClient<'_>,
        worker: &WorkerEntry,
        record: &LocalJobRecord,
        dispatch_owner: ProcessIdentity,
        original: WorkerError,
        acceptance_evidence: bool,
    ) -> Result<StatusResponse, WorkerError> {
        let request = ResolveOrAbandonRequest::try_from(record)?;
        let resolution = match remote.resolve_preacceptance_with_receipt(worker, &request) {
            Ok(resolution) => resolution,
            Err(error) if !acceptance_evidence => return Err(error),
            Err(_) => {
                self.persist_uncertainty(record, "ACCEPTANCE_EVIDENCE_CONFLICT", false)?;
                return Err(inconsistent_acceptance_evidence());
            }
        };
        match resolution.disposition() {
            PreacceptanceDisposition::Accepted(response) => Ok(response.clone()),
            PreacceptanceDisposition::Abandoned if !acceptance_evidence => {
                let receipt = resolution.abandonment_receipt().ok_or_else(|| {
                    WorkerError::Protocol(
                        "ABANDONMENT_PROOF_MISSING: remote abandonment lacked a receipt".into(),
                    )
                })?;
                self.client_state
                    .record_preacceptance_abandoned(receipt, dispatch_owner)?;
                self.client_state
                    .remove_after_terminal(record.meta().job_id(), dispatch_owner)?;
                Err(original)
            }
            PreacceptanceDisposition::Abandoned => {
                self.persist_uncertainty(record, "ACCEPTANCE_EVIDENCE_CONFLICT", false)?;
                Err(inconsistent_acceptance_evidence())
            }
            PreacceptanceDisposition::CleanupPending { code } => {
                self.persist_uncertainty(record, code, true)?;
                Err(recovery_error(record, code, true))
            }
            PreacceptanceDisposition::UnknownRemote { code } => {
                self.persist_uncertainty(record, code, false)?;
                Err(recovery_error(record, code, false))
            }
        }
    }

    fn cancellation_handoff(
        &self,
        record: &LocalJobRecord,
        dispatch_owner: ProcessIdentity,
    ) -> Result<RunCompletion, WorkerError> {
        // A cancellation request is not authority to overwrite an already
        // terminal exact remote outcome. The cancellation path has already
        // persisted and retired any such result, so return it with its normal
        // terminal exit semantics. If no durable terminal exists, this is the
        // normal cancellation/abandonment handoff.
        terminal_handoff_before_follow(self.client_state, record, dispatch_owner)?
            .ok_or_else(dispatch_cancelled)
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

fn admission_from_candidate(
    candidate: CandidateObservation,
    observed_at_millis: u64,
) -> Result<AdmissionObservation, WorkerError> {
    AdmissionObservation::new(
        candidate.worker_name().to_owned(),
        candidate.ready(),
        candidate.slot(),
        candidate.capabilities().to_vec(),
        candidate.available_memory_bytes(),
        candidate.free_disk_bytes(),
        observed_at_millis,
    )
}

fn candidate_from_admission(
    observation: &AdmissionObservation,
) -> Result<CandidateObservation, WorkerError> {
    CandidateObservation::new(
        observation.worker_name().to_owned(),
        observation.ready(),
        observation.slot(),
        observation.capabilities().to_vec(),
        observation.available_memory_bytes(),
        observation.free_disk_bytes(),
    )
    .map_err(|_| WorkerError::Protocol("cached scheduler observation is invalid".into()))
}

fn ranked_worker_names(
    observations: &[CandidateObservation],
    requirements: &[String],
    preference: &WorkerPreference,
    affinity: &AffinityHints,
) -> Vec<String> {
    let considered = observations
        .iter()
        .filter(|observation| match preference {
            WorkerPreference::Automatic => true,
            WorkerPreference::Pinned { worker } => observation.worker_name() == worker,
        })
        .cloned()
        .collect::<Vec<_>>();
    SchedulerPolicy::rank(&considered, requirements, affinity)
        .into_iter()
        .map(|candidate| candidate.worker_name().to_owned())
        .collect()
}

fn capacity_busy() -> WorkerError {
    WorkerError::Capacity {
        code: "CAPACITY_BUSY",
        message: "no eligible worker currently has an available heavy slot".into(),
    }
}

fn dispatch_cancelled() -> WorkerError {
    WorkerError::Queue {
        code: "CANCELLED",
        message: "job cancellation was requested before dispatch completed".into(),
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

fn persist_resolved_acceptance(
    client_state: &ClientStateStore,
    record: &LocalJobRecord,
    authoritative: &StatusResponse,
) -> Result<LocalJobRecord, WorkerError> {
    let status = normalize_authoritative_status(record, authoritative.clone())?;
    persist_authoritative(client_state, record, status)
}

fn terminal_cancel_response(
    record: &LocalJobRecord,
) -> Result<Option<CancelResponse>, WorkerError> {
    let Some(status) = record.last_status().cloned() else {
        return Ok(None);
    };
    if !status.state().is_terminal() {
        return Ok(None);
    }
    Ok(Some(CancelResponse::new(StatusResponse::new(
        record.meta().clone(),
        status,
    )?)?))
}

fn reject_local_abandonment_after_acceptance_evidence(
    client_state: &ClientStateStore,
    record: &LocalJobRecord,
) -> Result<(), WorkerError> {
    if record.last_status().is_none() {
        return Ok(());
    }
    client_state.set_remote_uncertainty_if_same_immutable(
        record,
        RemoteUncertainty::unknown_remote("ACCEPTANCE_EVIDENCE_CONFLICT")?,
    )?;
    Err(inconsistent_acceptance_evidence())
}

fn retire_terminal_queue_row(
    client_state: &ClientStateStore,
    terminal_record: &LocalJobRecord,
    dispatch_owner: ProcessIdentity,
) -> Result<(), WorkerError> {
    if !terminal_record
        .last_status()
        .is_some_and(|status| status.state().is_terminal())
    {
        return Err(inconsistent_terminal_outcome());
    }
    match client_state.remove_after_terminal(terminal_record.meta().job_id(), dispatch_owner) {
        Ok(_) => Ok(()),
        Err(
            error @ WorkerError::Queue {
                code: "QUEUE_NOT_FOUND",
                ..
            },
        ) => {
            // A concurrent canceller may already have retired the exact row.
            // Accept that idempotently only when the same complete terminal
            // record remains durable; a missing row alone is never proof.
            let current = load_exact_job(client_state, terminal_record.meta().job_id())?;
            if current == *terminal_record {
                Ok(())
            } else {
                Err(error)
            }
        }
        Err(error) => Err(error),
    }
}

fn terminal_handoff_before_follow(
    client_state: &ClientStateStore,
    expected: &LocalJobRecord,
    dispatch_owner: ProcessIdentity,
) -> Result<Option<RunCompletion>, WorkerError> {
    let current = load_exact_job(client_state, expected.meta().job_id())?;
    if !same_immutable(&current, expected) {
        return Err(job_id_conflict());
    }
    let Some(status) = current.last_status().cloned() else {
        return Ok(None);
    };
    if !status.state().is_terminal() {
        return Ok(None);
    }
    retire_terminal_queue_row(client_state, &current, dispatch_owner)?;
    if status.state() == JobState::Cancelled {
        return Err(dispatch_cancelled());
    }
    Ok(Some(RunCompletion {
        report: RunReport::new(
            current.meta().job_id(),
            current.meta().worker_name().into(),
            status.clone(),
        )?,
        exit_code: terminal_exit_code(&status)?,
    }))
}

fn is_ambiguous_cancel_transport_error(error: &WorkerError) -> bool {
    matches!(
        error,
        WorkerError::Transport {
            code: "SSH_UNAVAILABLE"
                | "SSH_TIMEOUT"
                | "SSH_LAUNCH_FAILED"
                | "HOST_REQUEST_FAILED"
                | "SSH_RESPONSE_TOO_LARGE"
                | "SSH_DIAGNOSTIC_TOO_LARGE"
                | "INVALID_RESPONSE",
            ..
        }
    )
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
        write_json_event(
            stdout,
            &JsonEvent::Accepted {
                protocol_version: PROTOCOL_VERSION,
                response: SubmitResponse::Accepted {
                    meta: Box::new(record.meta().clone()),
                    status,
                },
            },
        )?;
    } else {
        writeln!(
            stdout,
            "job {} accepted on {}",
            record.meta().job_id(),
            record.meta().worker_name()
        )?;
        stdout.flush()?;
    }
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
        load_exact_job(self.client_state, job_id)
    }

    fn worker_for<'a>(&'a self, record: &LocalJobRecord) -> Result<&'a WorkerEntry, WorkerError> {
        configured_worker(self.config, record)
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

fn load_exact_job(store: &ClientStateStore, job_id: JobId) -> Result<LocalJobRecord, WorkerError> {
    match store.load_job(job_id) {
        Ok(record) => Ok(record),
        Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            Err(WorkerError::Config(
                "JOB_NOT_FOUND: no local job record exists for the requested ID".into(),
            ))
        }
        Err(error) => Err(error),
    }
}

fn load_optional_job(
    store: &ClientStateStore,
    job_id: JobId,
) -> Result<Option<LocalJobRecord>, WorkerError> {
    match store.load_job(job_id) {
        Ok(record) => Ok(Some(record)),
        Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn configured_worker<'a>(
    config: &'a Config,
    record: &LocalJobRecord,
) -> Result<&'a WorkerEntry, WorkerError> {
    config.worker(record.meta().worker_name()).ok_or_else(|| {
        WorkerError::Config("WORKER_NOT_FOUND: the recorded worker is not configured".into())
    })
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

impl LogsService<'_> {
    pub fn stream(
        &self,
        job_id: JobId,
        follow: bool,
        json: bool,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<StatusResponse, WorkerError> {
        let record = load_exact_job(self.client_state, job_id)?;
        self.stream_record(&record, follow, json, stdout, stderr)
    }

    fn stream_record(
        &self,
        record: &LocalJobRecord,
        follow: bool,
        json: bool,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<StatusResponse, WorkerError> {
        let worker = configured_worker(self.config, record)?;
        let mut drain = TerminalLogDrain::new(
            LogCursor::new(LogStream::Stdout, 0, LOG_CHUNK_LIMIT)?,
            LogCursor::new(LogStream::Stderr, 0, LOG_CHUNK_LIMIT)?,
        )?;
        let mut last_response = None;
        let mut terminal_bound = false;

        loop {
            if !terminal_bound {
                let response = self.fetch_and_persist(worker, record)?;
                if response.status().state().is_terminal() {
                    drain.set_terminal_status(&response)?;
                    terminal_bound = true;
                }
                last_response = Some(response);
            }

            let mut progressed = false;
            for stream in [LogStream::Stdout, LogStream::Stderr] {
                if drain.cursor(stream).is_drained() {
                    continue;
                }
                let cursor = drain.cursor(stream);
                let chunk = self.remote.log_chunk(
                    worker,
                    record.meta().job_id(),
                    stream,
                    cursor.next_offset(),
                    cursor.limit(),
                )?;
                let before_offset = drain.cursor(stream).next_offset();
                commit_observed_chunk(&mut drain, &chunk, json, stdout, stderr)?;
                let advanced = drain.cursor(stream).next_offset() > before_offset;
                let became_drained = drain.cursor(stream).is_drained();
                if advanced || became_drained {
                    progressed = true;
                }
            }

            if !follow {
                let response = last_response.expect("status is fetched before any log query");
                write_final_status(&response, json, stdout)?;
                return Ok(response);
            }

            if drain.cursor(LogStream::Stdout).is_drained()
                && drain.cursor(LogStream::Stderr).is_drained()
            {
                let response = self.fetch_and_persist(worker, record)?;
                drain.revalidate_terminal_status(&response)?;
                write_final_status(&response, json, stdout)?;
                return Ok(response);
            }

            if !progressed {
                self.runtime.sleep(FOLLOW_POLL_INTERVAL);
            }
        }
    }

    fn fetch_and_persist(
        &self,
        worker: &WorkerEntry,
        record: &LocalJobRecord,
    ) -> Result<StatusResponse, WorkerError> {
        let response = self.remote.status(worker, record.meta().job_id())?;
        let status = normalize_authoritative_status(record, response)
            .map_err(|_| inconsistent_terminal_outcome())?;
        let winner = persist_authoritative(self.client_state, record, status)?;
        let status = winner
            .last_status()
            .cloned()
            .ok_or_else(inconsistent_terminal_outcome)?;
        StatusResponse::new(winner.meta().clone(), status)
            .map_err(|_| inconsistent_terminal_outcome())
    }
}

impl JobFollower for LogsService<'_> {
    fn follow(
        &self,
        record: &LocalJobRecord,
        json: bool,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> Result<StatusResponse, WorkerError> {
        let current = load_exact_job(self.client_state, record.meta().job_id())?;
        if !same_immutable(&current, record) {
            return Err(job_id_conflict());
        }
        self.stream_record(&current, true, json, stdout, stderr)
    }
}

fn commit_observed_chunk(
    drain: &mut TerminalLogDrain,
    chunk: &LogChunk,
    json: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), WorkerError> {
    let mut next = drain.clone();
    next.observe_chunk(chunk)?;
    write_log_chunk(chunk, json, stdout, stderr)?;
    *drain = next;
    Ok(())
}

fn write_log_chunk(
    chunk: &LogChunk,
    json: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), WorkerError> {
    if json {
        return write_json_event(
            stdout,
            &JsonEvent::Log {
                protocol_version: PROTOCOL_VERSION,
                chunk: chunk.clone(),
            },
        );
    }
    let bytes = chunk.decoded_bytes()?;
    let writer: &mut dyn Write = match chunk.stream() {
        LogStream::Stdout => stdout,
        LogStream::Stderr => stderr,
    };
    writer.write_all(&bytes)?;
    writer.flush()?;
    Ok(())
}

fn write_final_status(
    response: &StatusResponse,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<(), WorkerError> {
    if !json {
        return Ok(());
    }
    write_json_event(
        stdout,
        &JsonEvent::Status {
            protocol_version: PROTOCOL_VERSION,
            response: Box::new(response.clone()),
        },
    )
}

fn write_json_event(stdout: &mut dyn Write, event: &JsonEvent) -> Result<(), WorkerError> {
    let encoded =
        serde_json::to_vec(event).map_err(|error| WorkerError::Io(std::io::Error::other(error)))?;
    stdout.write_all(&encoded)?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};

    use crate::{
        error::WorkerError,
        job::{
            ClientId, CommandSpec, JobId, JobMeta, JobState, JobStatus, JsonEvent, LeaseToken,
            LocalJobRecord, LogChunk, LogCursor, LogStream, RemoteUncertainty,
            RequestFingerprintMaterial, StatusResponse, SubmitResponse, TerminalLogDrain,
        },
    };

    use super::{
        LOG_CHUNK_LIMIT, commit_observed_chunk, terminal_exit_code, write_accepted,
        write_json_event,
    };

    struct FailWrite {
        bytes: Vec<u8>,
    }

    impl Write for FailWrite {
        fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "planted writer failure",
            ))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct FailFlush {
        bytes: Vec<u8>,
    }

    impl Write for FailFlush {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "planted flush failure",
            ))
        }
    }

    fn sample_record() -> LocalJobRecord {
        let material = RequestFingerprintMaterial::new(
            JobId::new(uuid::Uuid::from_u128(1)),
            ClientId::new(uuid::Uuid::from_u128(2)),
            LeaseToken::new(uuid::Uuid::from_u128(3)),
            100,
            "mini-1".into(),
            "a".repeat(64),
            "b".repeat(64),
            "c".repeat(64),
            String::new(),
            30_000,
            "heavy".into(),
            CommandSpec::argv(vec!["true".into()]).unwrap(),
        )
        .unwrap();
        let meta = JobMeta::new(&material, material.fingerprint()).unwrap();
        LocalJobRecord::new(
            meta,
            material.lease_token(),
            Some(JobStatus::accepted(101).unwrap()),
            RemoteUncertainty::None,
        )
        .unwrap()
    }

    fn signal_status(updated_at_millis: u64, signal: u32) -> JobStatus {
        JobStatus::new(
            JobState::Failed,
            updated_at_millis,
            None,
            None,
            None,
            None,
            None,
            Some(signal),
            Some(0),
            Some(0),
            None,
            None,
        )
        .unwrap()
    }

    #[test]
    fn terminal_exit_codes_0_7_64_and_signal_143_are_exact() {
        let cases = [
            (JobStatus::succeeded(160, 0, 0).unwrap(), Ok(0)),
            (
                JobStatus::failed(161, 7, 0, 0)
                    .unwrap()
                    .with_cleanup_error("REMOTE_CLEANUP_FAILED".into(), 162)
                    .unwrap(),
                Ok(7),
            ),
            (JobStatus::failed(163, 64, 0, 0).unwrap(), Ok(64)),
            (JobStatus::failed(164, 255, 0, 0).unwrap(), Ok(255)),
            (signal_status(165, 15), Ok(143)),
            (
                JobStatus::succeeded(166, 0, 0)
                    .unwrap()
                    .with_cleanup_error("REMOTE_CLEANUP_FAILED".into(), 167)
                    .unwrap(),
                Ok(0),
            ),
            (signal_status(168, 128), Err(70)),
        ];
        for (status, expected) in cases {
            match expected {
                Ok(code) => assert_eq!(terminal_exit_code(&status).unwrap(), code),
                Err(code) => {
                    assert_eq!(terminal_exit_code(&status).unwrap_err().exit_code(), code)
                }
            }
        }
    }

    #[test]
    fn json_event_serialization_failure_leaves_writer_untouched() {
        let record = sample_record();
        let status = record.last_status().cloned().unwrap();
        let events = [
            JsonEvent::Accepted {
                protocol_version: 0,
                response: SubmitResponse::Accepted {
                    meta: Box::new(record.meta().clone()),
                    status: status.clone(),
                },
            },
            JsonEvent::Log {
                protocol_version: 0,
                chunk: LogChunk::new(LogStream::Stdout, 0, b"hello".to_vec()).unwrap(),
            },
            JsonEvent::Status {
                protocol_version: 0,
                response: Box::new(StatusResponse::new(record.meta().clone(), status).unwrap()),
            },
        ];
        for event in events {
            let mut writer = vec![b'!'];
            let error = write_json_event(&mut writer, &event).unwrap_err();
            assert_eq!(error.exit_code(), 74);
            assert_eq!(writer, [b'!']);
        }
    }

    #[test]
    fn json_writer_failure_preserves_io_kind_after_complete_serialize() {
        let record = sample_record();
        let mut writer = FailWrite { bytes: Vec::new() };
        let error = write_accepted(&record, true, &mut writer).unwrap_err();
        assert_eq!(error.exit_code(), 74);
        assert!(matches!(
            error,
            WorkerError::Io(ref io) if io.kind() == io::ErrorKind::BrokenPipe
        ));
        assert!(writer.bytes.is_empty());
    }

    #[test]
    fn flush_failure_leaves_drain_and_cursor_uncommitted() {
        let mut drain = TerminalLogDrain::new(
            LogCursor::new(LogStream::Stdout, 0, LOG_CHUNK_LIMIT).unwrap(),
            LogCursor::new(LogStream::Stderr, 0, LOG_CHUNK_LIMIT).unwrap(),
        )
        .unwrap();
        let chunk = LogChunk::new(LogStream::Stdout, 0, b"hello".to_vec()).unwrap();
        let mut stdout = FailFlush { bytes: Vec::new() };
        let mut stderr = Vec::new();
        let error =
            commit_observed_chunk(&mut drain, &chunk, false, &mut stdout, &mut stderr).unwrap_err();
        assert_eq!(error.exit_code(), 74);
        assert!(matches!(
            error,
            WorkerError::Io(ref io) if io.kind() == io::ErrorKind::BrokenPipe
        ));
        assert_eq!(stdout.bytes, b"hello");
        assert!(stderr.is_empty());
        assert_eq!(drain.cursor(LogStream::Stdout).next_offset(), 0);
        assert!(!drain.cursor(LogStream::Stdout).is_drained());
        assert_eq!(drain.cursor(LogStream::Stderr).next_offset(), 0);
    }
}
