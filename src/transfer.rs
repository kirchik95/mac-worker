use std::{
    collections::HashMap,
    fmt,
    sync::{LazyLock, Mutex},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::{
    agent_settings::{
        AgentDefaultSettings, AgentSettingsGetRequest, AgentSettingsList, AgentSettingsSaveRequest,
    },
    config::WorkerEntry,
    error::{ProcessError, ProcessStream, WorkerError},
    gc::{GcReport, GcRequest},
    host_store::{HostStore, JobDisposition},
    job::{
        CancelRequest, CancelResponse, ClientId, CommandSummary, FleetReconcileRequest,
        FleetReconcileResponse, HostControlError, JobId, LeaseAcquireRequest, LeaseRecord,
        LeaseToken, LogChunk, LogChunkRequest, LogChunkResponse, LogStream, MAX_LOG_CHUNK_BYTES,
        PreacceptanceDisposition, RequestFingerprint, ResolveOrAbandonOutcome,
        ResolveOrAbandonRequest, ResolveOrAbandonResponse, StatusLogsRequest, StatusLogsResponse,
        StatusRequest, StatusResponse, SubmitRequest, SubmitResponse,
    },
    job_service::{JobService, LaunchCandidate, SupervisorLauncher},
    process::{ProcessPolicy, ProcessRunner},
    task::TaskId,
    task_store::{
        TaskCancelRequest, TaskCancelResponse, TaskCloseRequest, TaskCloseResponse,
        TaskDiffRequest, TaskDiffResponse, TaskPrebindRequest, TaskPrepareRequest,
        TaskPrepareResponse, TaskSessionRequest, TaskSessionResponse, TaskStatusRequest,
        TaskStatusResponse,
    },
    turn::{TaskTurnRequest, TaskTurnResponse},
};

pub use crate::host_store::TransferGuard;

const MAX_CONTROL_STDOUT_BYTES: usize = 1024 * 1024;
const MAX_CONTROL_STDERR_BYTES: usize = 64 * 1024;
const MAX_CONTROL_DEADLINE: Duration = Duration::from_secs(30);
/// Upper bound a caller may choose. Controller service changes wait out
/// launchd, see `controller::service::SERVICE_CHANGE_DEADLINE`.
const MAX_CONTROL_POLICY_DEADLINE: Duration = Duration::from_secs(90);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostOperation {
    LeaseAcquire,
    SnapshotVerify,
    Submit,
    Gc,
    Status,
    LogChunk,
    StatusLogs,
    ResolveOrAbandon,
    TaskPrepare,
    TaskStatus,
    TaskDiff,
    TaskClose,
    TaskSession,
    TaskPrebind,
    TaskCancel,
    TaskTurn,
    RefreshFacts,
    RefreshFactsClear,
    Cancel,
    Reconcile,
    AgentSettingsGet,
    AgentSettingsSet,
    ControllerConfigure,
    ControllerKey,
    AuthorizeControllerKey,
    ControllerService,
    ControllerProbe,
    ControllerRpc,
    ControllerReceivePack,
    ControllerUploadPack,
    OutboxRetry,
}

impl HostOperation {
    pub fn command(self) -> &'static str {
        match self {
            Self::LeaseAcquire => "~/.local/bin/worker host lease-acquire",
            Self::SnapshotVerify => "~/.local/bin/worker host snapshot-verify",
            Self::Submit => "~/.local/bin/worker host submit",
            Self::Gc => "~/.local/bin/worker host gc",
            Self::Status => "~/.local/bin/worker host status",
            Self::LogChunk => "~/.local/bin/worker host log-chunk",
            Self::StatusLogs => "~/.local/bin/worker host status-logs",
            Self::ResolveOrAbandon => "~/.local/bin/worker host resolve-or-abandon",
            Self::TaskPrepare => "~/.local/bin/worker host task-prepare",
            Self::TaskStatus => "~/.local/bin/worker host task-status",
            Self::TaskDiff => "~/.local/bin/worker host task-diff",
            Self::TaskClose => "~/.local/bin/worker host task-close",
            Self::TaskSession => "~/.local/bin/worker host task-session",
            Self::TaskPrebind => "~/.local/bin/worker host task-prebind",
            Self::TaskCancel => "~/.local/bin/worker host task-cancel",
            Self::TaskTurn => "~/.local/bin/worker host task-turn",
            Self::RefreshFacts => "~/.local/bin/worker host refresh-facts",
            Self::RefreshFactsClear => {
                "~/.local/bin/worker host refresh-facts --clear-auth-incidents"
            }
            Self::Cancel => "~/.local/bin/worker host cancel",
            Self::Reconcile => "~/.local/bin/worker host reconcile",
            Self::AgentSettingsGet => "~/.local/bin/worker host agent-settings-get",
            Self::AgentSettingsSet => "~/.local/bin/worker host agent-settings-set",
            Self::ControllerConfigure => "~/.local/bin/worker host controller-configure",
            Self::ControllerKey => "~/.local/bin/worker host controller-key",
            Self::AuthorizeControllerKey => "~/.local/bin/worker host authorize-controller-key",
            Self::ControllerService => "~/.local/bin/worker host controller-service",
            Self::ControllerProbe => "~/.local/bin/worker host controller-probe",
            Self::ControllerRpc => "~/.local/bin/worker host controller-rpc",
            Self::ControllerReceivePack => "~/.local/bin/worker host controller-receive-pack",
            Self::ControllerUploadPack => "~/.local/bin/worker host controller-upload-pack",
            Self::OutboxRetry => "~/.local/bin/worker host outbox-retry",
        }
    }
}

pub struct SshJsonTransport<'a> {
    runner: &'a dyn ProcessRunner,
}

pub trait ResolutionRuntime: Send + Sync {
    fn monotonic_now(&self) -> Duration;
    fn sleep(&self, duration: Duration);
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemResolutionRuntime;

static RESOLUTION_EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);
static SYSTEM_RESOLUTION_RUNTIME: SystemResolutionRuntime = SystemResolutionRuntime;

impl ResolutionRuntime for SystemResolutionRuntime {
    fn monotonic_now(&self) -> Duration {
        RESOLUTION_EPOCH.elapsed()
    }

    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

const STATUS_LOGS_UNKNOWN: u8 = 0;
const STATUS_LOGS_SUPPORTED: u8 = 1;
const STATUS_LOGS_UNSUPPORTED: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionalHostResponse<T> {
    Supported(T),
    Unsupported,
}

pub struct RemoteJobClient<'a> {
    transport: SshJsonTransport<'a>,
    retry: &'a dyn ResolutionRuntime,
    status_logs: Mutex<HashMap<(String, String), u8>>,
}

/// Process-local authority proving that one exact remote resolution returned
/// `Abandoned`. It deliberately has no serialization implementation.
pub struct PreacceptanceAbandonmentReceipt {
    job_id: JobId,
    client_id: ClientId,
    worker_name: String,
    project_id: String,
    worktree_id: String,
    command_summary: CommandSummary,
    request_fingerprint: RequestFingerprint,
    resolution_request_digest: [u8; 32],
}

impl PreacceptanceAbandonmentReceipt {
    fn from_remote_abandonment(request: &ResolveOrAbandonRequest) -> Result<Self, WorkerError> {
        request.validate()?;
        Ok(Self {
            job_id: request.job_id(),
            client_id: request.client_id(),
            worker_name: request.worker_name().into(),
            project_id: request.project_id().into(),
            worktree_id: request.worktree_id().into(),
            command_summary: request.command_summary().clone(),
            request_fingerprint: request.request_fingerprint().clone(),
            resolution_request_digest: resolution_request_digest(request)?,
        })
    }

    pub(crate) fn job_id(&self) -> JobId {
        self.job_id
    }

    pub(crate) fn matches_request(&self, request: &ResolveOrAbandonRequest) -> bool {
        request.validate().is_ok()
            && self.job_id == request.job_id()
            && self.client_id == request.client_id()
            && self.worker_name == request.worker_name()
            && self.project_id == request.project_id()
            && self.worktree_id == request.worktree_id()
            && self.command_summary == *request.command_summary()
            && self.request_fingerprint == *request.request_fingerprint()
            && resolution_request_digest(request)
                .is_ok_and(|digest| self.resolution_request_digest == digest)
    }
}

fn resolution_request_digest(request: &ResolveOrAbandonRequest) -> Result<[u8; 32], WorkerError> {
    let bytes = serde_json::to_vec(request)
        .map_err(|_| transport_error("INVALID_REQUEST", "control request is invalid"))?;
    Ok(Sha256::digest(bytes).into())
}

impl fmt::Debug for PreacceptanceAbandonmentReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreacceptanceAbandonmentReceipt")
            .field("job_id", &self.job_id)
            .field("client_id", &self.client_id)
            .field("worker_name", &self.worker_name)
            .field("project_id", &self.project_id)
            .field("worktree_id", &self.worktree_id)
            .field("command_summary", &self.command_summary)
            .field("request_fingerprint", &self.request_fingerprint)
            .finish()
    }
}

/// Opaque result of the receipt-bearing resolution path. Its private fields
/// keep the disposition/receipt invariant caller-unforgeable.
#[derive(Debug)]
pub struct PreacceptanceResolution {
    disposition: PreacceptanceDisposition,
    abandonment_receipt: Option<PreacceptanceAbandonmentReceipt>,
}

impl PreacceptanceResolution {
    fn from_remote_result(
        request: &ResolveOrAbandonRequest,
        disposition: PreacceptanceDisposition,
    ) -> Result<Self, WorkerError> {
        disposition.validate()?;
        let abandonment_receipt = matches!(disposition, PreacceptanceDisposition::Abandoned)
            .then(|| PreacceptanceAbandonmentReceipt::from_remote_abandonment(request))
            .transpose()?;
        Ok(Self {
            disposition,
            abandonment_receipt,
        })
    }

    pub fn disposition(&self) -> &PreacceptanceDisposition {
        &self.disposition
    }

    pub fn abandonment_receipt(&self) -> Option<&PreacceptanceAbandonmentReceipt> {
        self.abandonment_receipt.as_ref()
    }

    pub fn into_disposition(self) -> PreacceptanceDisposition {
        self.disposition
    }

    pub fn into_abandonment_receipt(self) -> Option<PreacceptanceAbandonmentReceipt> {
        self.abandonment_receipt
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct TransferIdentity {
    job_id: JobId,
    client_id: ClientId,
    lease_token: LeaseToken,
    request_fingerprint: RequestFingerprint,
}

impl TransferIdentity {
    pub fn new(
        job_id: JobId,
        client_id: ClientId,
        lease_token: LeaseToken,
        request_fingerprint: RequestFingerprint,
    ) -> Self {
        Self {
            job_id,
            client_id,
            lease_token,
            request_fingerprint,
        }
    }

    pub fn from_acquire_request(request: &LeaseAcquireRequest) -> Result<Self, WorkerError> {
        request.validate()?;
        Ok(Self::new(
            request.material().job_id(),
            request.material().client_id(),
            request.material().lease_token(),
            request.request_fingerprint().clone(),
        ))
    }

    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    pub(crate) fn lease_token(&self) -> LeaseToken {
        self.lease_token
    }

    pub fn request_fingerprint(&self) -> &RequestFingerprint {
        &self.request_fingerprint
    }
}

impl fmt::Debug for TransferIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransferIdentity")
            .field("job_id", &self.job_id)
            .field("client_id", &self.client_id)
            .field("request_fingerprint", &self.request_fingerprint)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbandonTransferResult {
    Abandoned,
}

pub struct HostTransferService<'a> {
    store: &'a HostStore,
}

struct TransferResolutionLauncher;

impl SupervisorLauncher for TransferResolutionLauncher {
    fn launch(
        &self,
        _job_id: JobId,
        _guard: crate::host_store::SupervisorGuard,
    ) -> Result<LaunchCandidate, WorkerError> {
        Err(host_transfer_error(
            "JOB_ACCEPTED",
            "accepted resolution cannot launch through transfer cleanup",
        ))
    }
}

impl<'a> HostTransferService<'a> {
    pub fn new(store: &'a HostStore) -> Self {
        Self { store }
    }

    #[doc(hidden)]
    pub fn abandon(
        &self,
        request: &LeaseAcquireRequest,
        now: u64,
    ) -> Result<AbandonTransferResult, WorkerError> {
        request.validate()?;
        let submit = SubmitRequest::new(request.material().clone());
        let resolve = ResolveOrAbandonRequest::from_submit_request(&submit)?;
        let response = JobService::new(self.store, &TransferResolutionLauncher)
            .resolve_or_abandon_at(resolve, now, false)?;
        match response.outcome() {
            ResolveOrAbandonOutcome::Abandoned => Ok(AbandonTransferResult::Abandoned),
            ResolveOrAbandonOutcome::Accepted { .. } => Err(host_transfer_error(
                "JOB_ACCEPTED",
                "job ID was already accepted",
            )),
            ResolveOrAbandonOutcome::CleanupPending { .. } => Err(host_transfer_error(
                "CLEANUP_PENDING",
                "exact abandonment cleanup is pending",
            )),
        }
    }
}

impl<'a> SshJsonTransport<'a> {
    pub fn new(runner: &'a dyn ProcessRunner) -> Self {
        Self { runner }
    }

    pub fn request<Req: Serialize, Res: DeserializeOwned + Serialize>(
        &self,
        worker: &WorkerEntry,
        operation: HostOperation,
        request: &Req,
        policy: ProcessPolicy,
    ) -> Result<Res, WorkerError> {
        let result = self.run_control(worker, operation, request, policy)?;
        if !result.status.success() {
            return Err(control_request_failure(&result, policy));
        }
        parse_control_response(&result, policy)
    }

    pub fn request_optional<Req: Serialize, Res: DeserializeOwned + Serialize>(
        &self,
        worker: &WorkerEntry,
        operation: HostOperation,
        request: &Req,
        policy: ProcessPolicy,
    ) -> Result<OptionalHostResponse<Res>, WorkerError> {
        let result = self.run_control(worker, operation, request, policy)?;
        if !result.status.success() {
            if result.status.code() == Some(255) {
                return Err(transport_error(
                    "SSH_UNAVAILABLE",
                    "SSH control request failed",
                ));
            }
            if result.stdout.len() > policy.stdout_limit
                || result.stderr.len() > policy.stderr_limit
            {
                return Err(transport_error(
                    "HOST_REQUEST_FAILED",
                    "SSH control request failed",
                ));
            }
            if let Some(error) = decode_host_control_error(&result.stdout) {
                return Err(error);
            }
            if is_unrecognized_optional_command(operation, &result) {
                return Ok(OptionalHostResponse::Unsupported);
            }
            return Err(transport_error(
                "HOST_REQUEST_FAILED",
                "SSH control request failed",
            ));
        }
        Ok(OptionalHostResponse::Supported(parse_control_response(
            &result, policy,
        )?))
    }

    fn run_control<Req: Serialize>(
        &self,
        worker: &WorkerEntry,
        operation: HostOperation,
        request: &Req,
        policy: ProcessPolicy,
    ) -> Result<crate::process::ProcessResult, WorkerError> {
        validate_worker(worker)?;
        validate_control_policy(policy)?;
        let stdin = serde_json::to_vec(request)
            .map_err(|_| transport_error("INVALID_REQUEST", "control request is invalid"))?;
        let request = crate::transport::ssh_exec_request(
            crate::transport::SshTarget::Worker,
            &worker.ssh,
            operation.command(),
            policy,
            Some(stdin),
        )?;
        self.runner.run(&request).map_err(map_control_failure)
    }

    fn run_named_command(
        &self,
        worker: &WorkerEntry,
        command: String,
        stdin: Vec<u8>,
        policy: ProcessPolicy,
    ) -> Result<crate::process::ProcessResult, WorkerError> {
        validate_worker(worker)?;
        validate_control_policy(policy)?;
        let request = crate::transport::ssh_exec_request(
            crate::transport::SshTarget::Worker,
            &worker.ssh,
            &command,
            policy,
            Some(stdin),
        )?;
        self.runner.run(&request).map_err(map_control_failure)
    }

    pub fn agent_settings_get(
        &self,
        worker: &WorkerEntry,
        request: &AgentSettingsGetRequest,
    ) -> Result<AgentSettingsList, WorkerError> {
        crate::agent_settings::validate_get_request(request)
            .map_err(|error| WorkerError::Protocol(error.to_string()))?;
        self.request(
            worker,
            HostOperation::AgentSettingsGet,
            request,
            control_policy(MAX_CONTROL_DEADLINE),
        )
    }

    pub fn agent_settings_set(
        &self,
        worker: &WorkerEntry,
        request: &AgentSettingsSaveRequest,
    ) -> Result<AgentDefaultSettings, WorkerError> {
        crate::agent_settings::validate_save_request(request)
            .map_err(|error| WorkerError::Protocol(error.to_string()))?;
        self.request(
            worker,
            HostOperation::AgentSettingsSet,
            request,
            control_policy(MAX_CONTROL_DEADLINE),
        )
    }
}

impl<'a> RemoteJobClient<'a> {
    pub fn new(runner: &'a dyn ProcessRunner) -> Self {
        Self::new_with_runtime(runner, &SYSTEM_RESOLUTION_RUNTIME)
    }

    #[doc(hidden)]
    pub fn new_with_runtime(
        runner: &'a dyn ProcessRunner,
        retry: &'a dyn ResolutionRuntime,
    ) -> Self {
        Self {
            transport: SshJsonTransport::new(runner),
            retry,
            status_logs: Mutex::new(HashMap::new()),
        }
    }

    pub fn lease_acquire(
        &self,
        worker: &WorkerEntry,
        request: &LeaseAcquireRequest,
    ) -> Result<crate::job::LeaseAcquireResponse, WorkerError> {
        request.validate()?;
        let response: crate::job::LeaseAcquireResponse = self.transport.request(
            worker,
            HostOperation::LeaseAcquire,
            request,
            control_policy(MAX_CONTROL_DEADLINE),
        )?;
        response.validate().map_err(|_| invalid_remote_response())?;
        Ok(response)
    }

    pub fn gc(&self, worker: &WorkerEntry, request: &GcRequest) -> Result<GcReport, WorkerError> {
        request.validate()?;
        let response: GcReport = self.transport.request(
            worker,
            HostOperation::Gc,
            request,
            control_policy(MAX_CONTROL_DEADLINE),
        )?;
        response.validate().map_err(|_| invalid_remote_response())?;
        Ok(response)
    }

    pub fn task_prepare(
        &self,
        worker: &WorkerEntry,
        request: &TaskPrepareRequest,
    ) -> Result<TaskPrepareResponse, WorkerError> {
        let response: TaskPrepareResponse = self.transport.request(
            worker,
            HostOperation::TaskPrepare,
            request,
            control_policy(MAX_CONTROL_DEADLINE),
        )?;
        if response.protocol_version() != crate::protocol::PROTOCOL_VERSION {
            return Err(invalid_remote_response());
        }
        Ok(response)
    }

    pub fn task_status(
        &self,
        worker: &WorkerEntry,
        request: &TaskStatusRequest,
    ) -> Result<TaskStatusResponse, WorkerError> {
        self.task_status_with_deadline(worker, request, MAX_CONTROL_DEADLINE)
    }

    pub fn task_status_with_deadline(
        &self,
        worker: &WorkerEntry,
        request: &TaskStatusRequest,
        deadline: Duration,
    ) -> Result<TaskStatusResponse, WorkerError> {
        let response: TaskStatusResponse = self.transport.request(
            worker,
            HostOperation::TaskStatus,
            request,
            control_policy(deadline),
        )?;
        if response.protocol_version() != crate::protocol::PROTOCOL_VERSION {
            return Err(invalid_remote_response());
        }
        Ok(response)
    }

    pub fn task_diff(
        &self,
        worker: &WorkerEntry,
        request: &TaskDiffRequest,
    ) -> Result<TaskDiffResponse, WorkerError> {
        let response: TaskDiffResponse = self.transport.request(
            worker,
            HostOperation::TaskDiff,
            request,
            control_policy(MAX_CONTROL_DEADLINE),
        )?;
        if response.protocol_version() != crate::protocol::PROTOCOL_VERSION {
            return Err(invalid_remote_response());
        }
        Ok(response)
    }

    pub fn task_close(
        &self,
        worker: &WorkerEntry,
        request: &TaskCloseRequest,
    ) -> Result<TaskCloseResponse, WorkerError> {
        let response: TaskCloseResponse = self.transport.request(
            worker,
            HostOperation::TaskClose,
            request,
            control_policy(MAX_CONTROL_DEADLINE),
        )?;
        if response.protocol_version() != crate::protocol::PROTOCOL_VERSION {
            return Err(invalid_remote_response());
        }
        response.validate().map_err(|_| invalid_remote_response())?;
        Ok(response)
    }

    pub fn task_session(
        &self,
        worker: &WorkerEntry,
        request: &TaskSessionRequest,
    ) -> Result<TaskSessionResponse, WorkerError> {
        let response: TaskSessionResponse = self.transport.request(
            worker,
            HostOperation::TaskSession,
            request,
            control_policy(MAX_CONTROL_DEADLINE),
        )?;
        if response.protocol_version() != crate::protocol::PROTOCOL_VERSION {
            return Err(invalid_remote_response());
        }
        Ok(response)
    }

    pub fn task_prebind(
        &self,
        worker: &WorkerEntry,
        request: &TaskPrebindRequest,
    ) -> Result<TaskSessionResponse, WorkerError> {
        let response: TaskSessionResponse = self.transport.request(
            worker,
            HostOperation::TaskPrebind,
            request,
            control_policy(MAX_CONTROL_DEADLINE),
        )?;
        if response.protocol_version() != crate::protocol::PROTOCOL_VERSION {
            return Err(invalid_remote_response());
        }
        Ok(response)
    }

    pub fn task_cancel(
        &self,
        worker: &WorkerEntry,
        request: &TaskCancelRequest,
    ) -> Result<TaskCancelResponse, WorkerError> {
        let response: TaskCancelResponse = self.transport.request(
            worker,
            HostOperation::TaskCancel,
            request,
            control_policy(MAX_CONTROL_DEADLINE),
        )?;
        if response.protocol_version() != crate::protocol::PROTOCOL_VERSION {
            return Err(invalid_remote_response());
        }
        Ok(response)
    }

    pub fn status(
        &self,
        worker: &WorkerEntry,
        job_id: JobId,
    ) -> Result<StatusResponse, WorkerError> {
        self.status_with_deadline(worker, job_id, MAX_CONTROL_DEADLINE)
    }

    pub fn submit_turn(
        &self,
        worker: &WorkerEntry,
        request: &TaskTurnRequest,
    ) -> Result<TaskTurnResponse, WorkerError> {
        self.transport.request(
            worker,
            HostOperation::TaskTurn,
            request,
            control_policy(MAX_CONTROL_DEADLINE),
        )
    }

    pub fn reconcile(
        &self,
        worker: &WorkerEntry,
        request: &FleetReconcileRequest,
    ) -> Result<FleetReconcileResponse, WorkerError> {
        request.validate()?;
        let response: FleetReconcileResponse = self.transport.request(
            worker,
            HostOperation::Reconcile,
            request,
            control_policy(MAX_CONTROL_DEADLINE),
        )?;
        response.validate().map_err(|_| invalid_remote_response())?;
        if response.results().len() != request.known_job_ids().len()
            || response.results().iter().any(|result| {
                !request.known_job_ids().contains(&result.job_id())
                    || result.status().is_some_and(|status| {
                        status.meta().worker_name() != worker.name
                            || status.meta().job_id() != result.job_id()
                    })
            })
        {
            return Err(invalid_remote_response());
        }
        Ok(response)
    }

    pub fn cancel(
        &self,
        worker: &WorkerEntry,
        request: &CancelRequest,
    ) -> Result<CancelResponse, WorkerError> {
        request.validate()?;
        let response: CancelResponse = self.transport.request(
            worker,
            HostOperation::Cancel,
            request,
            control_policy(MAX_CONTROL_DEADLINE),
        )?;
        response.validate()?;
        let meta = response.status().meta();
        if meta.job_id() != request.job_id()
            || meta.client_id() != request.client_id()
            || meta.request_fingerprint() != request.request_fingerprint()
            || meta.worker_name() != worker.name
        {
            return Err(invalid_remote_response());
        }
        Ok(response)
    }

    pub fn log_chunk(
        &self,
        worker: &WorkerEntry,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    ) -> Result<LogChunk, WorkerError> {
        let response: LogChunkResponse = self.transport.request(
            worker,
            HostOperation::LogChunk,
            &LogChunkRequest::new(job_id, stream, offset, limit),
            control_policy(MAX_CONTROL_DEADLINE),
        )?;
        let chunk = response.into_chunk();
        validate_remote_log_chunk(&chunk, stream, offset, limit)?;
        Ok(chunk)
    }

    pub fn try_combined_status_and_logs(
        &self,
        worker: &WorkerEntry,
        job_id: JobId,
        stdout_offset: u64,
        stdout_limit: u32,
        stderr_offset: u64,
        stderr_limit: u32,
    ) -> Result<Option<(StatusResponse, LogChunk, LogChunk)>, WorkerError> {
        if self.status_logs_mode(worker) != STATUS_LOGS_UNSUPPORTED {
            match self.try_status_logs(
                worker,
                job_id,
                stdout_offset,
                stdout_limit,
                stderr_offset,
                stderr_limit,
            ) {
                Ok(Some(parts)) => {
                    self.set_status_logs_mode(worker, STATUS_LOGS_SUPPORTED);
                    return Ok(Some(parts));
                }
                Ok(None) => {
                    self.set_status_logs_mode(worker, STATUS_LOGS_UNSUPPORTED);
                    return Ok(None);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }

    pub fn status_and_logs(
        &self,
        worker: &WorkerEntry,
        job_id: JobId,
        stdout_offset: u64,
        stdout_limit: u32,
        stderr_offset: u64,
        stderr_limit: u32,
    ) -> Result<(StatusResponse, LogChunk, LogChunk), WorkerError> {
        if let Some(parts) = self.try_combined_status_and_logs(
            worker,
            job_id,
            stdout_offset,
            stdout_limit,
            stderr_offset,
            stderr_limit,
        )? {
            return Ok(parts);
        }
        self.status_and_logs_sequential(
            worker,
            job_id,
            stdout_offset,
            stdout_limit,
            stderr_offset,
            stderr_limit,
        )
    }

    fn try_status_logs(
        &self,
        worker: &WorkerEntry,
        job_id: JobId,
        stdout_offset: u64,
        stdout_limit: u32,
        stderr_offset: u64,
        stderr_limit: u32,
    ) -> Result<Option<(StatusResponse, LogChunk, LogChunk)>, WorkerError> {
        match self.transport.request_optional::<_, StatusLogsResponse>(
            worker,
            HostOperation::StatusLogs,
            &StatusLogsRequest::new(
                job_id,
                stdout_offset,
                stdout_limit,
                stderr_offset,
                stderr_limit,
            ),
            control_policy(MAX_CONTROL_DEADLINE),
        )? {
            OptionalHostResponse::Unsupported => Ok(None),
            OptionalHostResponse::Supported(response) => {
                let (status, stdout, stderr) = response.into_parts();
                if status.meta().job_id() != job_id || status.meta().worker_name() != worker.name {
                    return Err(invalid_remote_response());
                }
                validate_remote_log_chunk(&stdout, LogStream::Stdout, stdout_offset, stdout_limit)?;
                validate_remote_log_chunk(&stderr, LogStream::Stderr, stderr_offset, stderr_limit)?;
                Ok(Some((status, stdout, stderr)))
            }
        }
    }

    fn status_and_logs_sequential(
        &self,
        worker: &WorkerEntry,
        job_id: JobId,
        stdout_offset: u64,
        stdout_limit: u32,
        stderr_offset: u64,
        stderr_limit: u32,
    ) -> Result<(StatusResponse, LogChunk, LogChunk), WorkerError> {
        let status = self.status(worker, job_id)?;
        let stdout = self.log_chunk(
            worker,
            job_id,
            LogStream::Stdout,
            stdout_offset,
            stdout_limit,
        )?;
        let stderr = self.log_chunk(
            worker,
            job_id,
            LogStream::Stderr,
            stderr_offset,
            stderr_limit,
        )?;
        Ok((status, stdout, stderr))
    }

    fn status_logs_mode(&self, worker: &WorkerEntry) -> u8 {
        self.status_logs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&(worker.name.clone(), worker.ssh.clone()))
            .copied()
            .unwrap_or(STATUS_LOGS_UNKNOWN)
    }

    fn set_status_logs_mode(&self, worker: &WorkerEntry, mode: u8) {
        self.status_logs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert((worker.name.clone(), worker.ssh.clone()), mode);
    }

    pub fn resolve_preacceptance(
        &self,
        worker: &WorkerEntry,
        request: &ResolveOrAbandonRequest,
    ) -> Result<PreacceptanceDisposition, WorkerError> {
        self.resolve_preacceptance_with_receipt(worker, request)
            .map(PreacceptanceResolution::into_disposition)
    }

    pub fn resolve_preacceptance_with_receipt(
        &self,
        worker: &WorkerEntry,
        request: &ResolveOrAbandonRequest,
    ) -> Result<PreacceptanceResolution, WorkerError> {
        let disposition = self.resolve_preacceptance_disposition(worker, request)?;
        PreacceptanceResolution::from_remote_result(request, disposition)
    }

    fn resolve_preacceptance_disposition(
        &self,
        worker: &WorkerEntry,
        request: &ResolveOrAbandonRequest,
    ) -> Result<PreacceptanceDisposition, WorkerError> {
        request.validate()?;
        if worker.name != request.worker_name() {
            return Err(invalid_remote_response());
        }
        let start = self.retry.monotonic_now();
        while let Some(remaining) = remaining_resolution_budget(start, self.retry.monotonic_now()) {
            match self.status_with_deadline(worker, request.job_id(), remaining) {
                Ok(response) => {
                    if validate_resolution_response(worker, request, &response).is_ok() {
                        return Ok(PreacceptanceDisposition::Accepted(response));
                    }
                }
                Err(error @ WorkerError::Protocol(_)) => match protocol_error_code(&error) {
                    Some("JOB_NOT_FOUND") => {}
                    Some("JOB_ABANDONED") => break,
                    _ => return Err(error),
                },
                Err(_) => {}
            }
            let Some(remaining) = remaining_resolution_budget(start, self.retry.monotonic_now())
            else {
                break;
            };
            self.retry.sleep(remaining.min(Duration::from_secs(1)));
        }

        let response = match self.transport.request::<_, ResolveOrAbandonResponse>(
            worker,
            HostOperation::ResolveOrAbandon,
            request,
            control_policy(MAX_CONTROL_DEADLINE),
        ) {
            Ok(response) => response,
            Err(error @ WorkerError::Protocol(_)) => return Err(error),
            Err(_) => return PreacceptanceDisposition::unknown_remote("UNKNOWN_REMOTE"),
        };
        match response.outcome() {
            ResolveOrAbandonOutcome::Accepted { response } => {
                if validate_resolution_response(worker, request, response).is_err() {
                    return PreacceptanceDisposition::unknown_remote("UNKNOWN_REMOTE");
                }
                Ok(PreacceptanceDisposition::Accepted(response.clone()))
            }
            ResolveOrAbandonOutcome::Abandoned => Ok(PreacceptanceDisposition::Abandoned),
            ResolveOrAbandonOutcome::CleanupPending { code } => {
                PreacceptanceDisposition::cleanup_pending(code.clone())
            }
        }
    }

    pub fn resolve_submission(
        &self,
        worker: &WorkerEntry,
        request: &SubmitRequest,
        submit_result: Result<SubmitResponse, WorkerError>,
    ) -> Result<SubmitResponse, WorkerError> {
        request.validate()?;
        match submit_result {
            Ok(response @ SubmitResponse::Accepted { .. }) => {
                let SubmitResponse::Accepted { meta, status } = &response else {
                    unreachable!()
                };
                let status_response = StatusResponse::new((**meta).clone(), status.clone())?;
                let resolution = ResolveOrAbandonRequest::from_submit_request(request)?;
                validate_resolution_response(worker, &resolution, &status_response)?;
                Ok(response)
            }
            Ok(SubmitResponse::Existing { .. }) => {
                let status = self.status(worker, request.material().job_id())?;
                let resolution = ResolveOrAbandonRequest::from_submit_request(request)?;
                validate_resolution_response(worker, &resolution, &status)?;
                Ok(SubmitResponse::Existing {
                    status: status.status().clone(),
                })
            }
            Err(original) => {
                let resolution = ResolveOrAbandonRequest::from_submit_request(request)?;
                match self.resolve_preacceptance(worker, &resolution)? {
                    PreacceptanceDisposition::Accepted(response) => Ok(SubmitResponse::Accepted {
                        meta: Box::new(response.meta().clone()),
                        status: response.status().clone(),
                    }),
                    PreacceptanceDisposition::Abandoned => Err(original),
                    PreacceptanceDisposition::CleanupPending { code } => Err(
                        WorkerError::Protocol(format!("{code}: remote cleanup is pending")),
                    ),
                    PreacceptanceDisposition::UnknownRemote { code } => Err(WorkerError::Protocol(
                        format!("{code}: remote job state is unknown"),
                    )),
                }
            }
        }
    }

    pub fn status_with_deadline(
        &self,
        worker: &WorkerEntry,
        job_id: JobId,
        deadline: Duration,
    ) -> Result<StatusResponse, WorkerError> {
        let response: StatusResponse = self.transport.request(
            worker,
            HostOperation::Status,
            &StatusRequest::new(job_id),
            control_policy(deadline),
        )?;
        if response.meta().job_id() != job_id || response.meta().worker_name() != worker.name {
            return Err(invalid_remote_response());
        }
        Ok(response)
    }

    pub fn outbox_retry(
        &self,
        worker: &WorkerEntry,
        task_id: TaskId,
    ) -> Result<crate::outbox::OutboxRetryResponse, WorkerError> {
        let policy = control_policy(MAX_CONTROL_DEADLINE);
        let command = format!("{} {task_id}", HostOperation::OutboxRetry.command());
        let result = self
            .transport
            .run_named_command(worker, command, Vec::new(), policy)?;
        if !result.status.success() {
            if is_unrecognized_host_subcommand("outbox-retry", &result) {
                return Err(WorkerError::Protocol(
                    "HOST_COMMAND_UNSUPPORTED: host helper does not support outbox-retry".into(),
                ));
            }
            return Err(control_request_failure(&result, policy));
        }
        let response: crate::outbox::OutboxRetryResponse = parse_control_response(&result, policy)?;
        if response.protocol_version() != crate::protocol::PROTOCOL_VERSION {
            return Err(invalid_remote_response());
        }
        Ok(response)
    }
}

fn control_policy(deadline: Duration) -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: MAX_CONTROL_STDOUT_BYTES,
        stderr_limit: MAX_CONTROL_STDERR_BYTES,
        deadline,
    }
}

fn remaining_resolution_budget(start: Duration, now: Duration) -> Option<Duration> {
    let elapsed = now.checked_sub(start)?;
    let remaining = MAX_CONTROL_DEADLINE.checked_sub(elapsed)?;
    (!remaining.is_zero()).then_some(remaining)
}

fn validate_resolution_response(
    worker: &WorkerEntry,
    request: &ResolveOrAbandonRequest,
    response: &StatusResponse,
) -> Result<(), WorkerError> {
    response.validate().map_err(|_| invalid_remote_response())?;
    let meta = response.meta();
    if meta.job_id() != request.job_id()
        || meta.client_id() != request.client_id()
        || meta.worker_name() != worker.name
        || meta.worker_name() != request.worker_name()
        || meta.project_id() != request.project_id()
        || meta.worktree_id() != request.worktree_id()
        || meta.manifest_digest() != request.manifest_digest()
        || meta.created_at_millis() != request.created_at_millis()
        || meta.request_fingerprint() != request.request_fingerprint()
        || meta.relative_working_dir() != request.relative_working_dir()
        || meta.timeout_millis() != request.timeout_millis()
        || meta.resource_class() != request.resource_class()
        || meta.command_summary() != request.command_summary()
    {
        return Err(invalid_remote_response());
    }
    Ok(())
}

fn control_request_failure(
    result: &crate::process::ProcessResult,
    policy: ProcessPolicy,
) -> WorkerError {
    if result.status.code() == Some(255) {
        return transport_error("SSH_UNAVAILABLE", "SSH control request failed");
    }
    if result.stdout.len() > policy.stdout_limit || result.stderr.len() > policy.stderr_limit {
        return transport_error("HOST_REQUEST_FAILED", "SSH control request failed");
    }
    decode_host_control_error(&result.stdout)
        .unwrap_or_else(|| transport_error("HOST_REQUEST_FAILED", "SSH control request failed"))
}

fn parse_control_response<Res: DeserializeOwned + Serialize>(
    result: &crate::process::ProcessResult,
    policy: ProcessPolicy,
) -> Result<Res, WorkerError> {
    if result.stdout.len() > policy.stdout_limit {
        return Err(transport_error(
            "SSH_RESPONSE_TOO_LARGE",
            "SSH control response exceeded its limit",
        ));
    }
    if result.stderr.len() > policy.stderr_limit {
        return Err(transport_error(
            "SSH_DIAGNOSTIC_TOO_LARGE",
            "SSH control diagnostic exceeded its limit",
        ));
    }

    let mut deserializer = serde_json::Deserializer::from_slice(&result.stdout);
    let response = Res::deserialize(&mut deserializer)
        .map_err(|_| transport_error("INVALID_RESPONSE", "host response was invalid"))?;
    deserializer
        .end()
        .map_err(|_| transport_error("INVALID_RESPONSE", "host response was invalid"))?;
    let canonical = serde_json::to_vec(&response)
        .map_err(|_| transport_error("INVALID_RESPONSE", "host response was invalid"))?;
    if result.stdout != canonical && result.stdout != [canonical.as_slice(), b"\n"].concat() {
        return Err(transport_error(
            "INVALID_RESPONSE",
            "host response was invalid",
        ));
    }
    Ok(response)
}

fn is_unrecognized_optional_command(
    operation: HostOperation,
    result: &crate::process::ProcessResult,
) -> bool {
    let Some(subcommand) = unrecognized_optional_subcommand(operation) else {
        return false;
    };
    is_unrecognized_host_subcommand(subcommand, result)
}

fn unrecognized_optional_subcommand(operation: HostOperation) -> Option<&'static str> {
    match operation {
        HostOperation::StatusLogs => Some("status-logs"),
        HostOperation::OutboxRetry => Some("outbox-retry"),
        _ => None,
    }
}

fn is_unrecognized_host_subcommand(
    subcommand: &str,
    result: &crate::process::ProcessResult,
) -> bool {
    if result.status.success() || result.status.code() == Some(255) || !result.stdout.is_empty() {
        return false;
    }
    let stderr = String::from_utf8_lossy(&result.stderr).to_ascii_lowercase();
    (stderr.contains("unrecognized subcommand") || stderr.contains("unrecognised subcommand"))
        && stderr.contains(subcommand)
}

fn validate_remote_log_chunk(
    chunk: &LogChunk,
    stream: LogStream,
    offset: u64,
    limit: u32,
) -> Result<(), WorkerError> {
    let response_len = chunk
        .decoded_bytes()
        .map_err(|_| invalid_remote_response())?
        .len();
    let effective_limit = usize::try_from(limit)
        .expect("u32 fits in usize on supported hosts")
        .min(MAX_LOG_CHUNK_BYTES);
    if chunk.stream() != stream || chunk.offset() != offset || response_len > effective_limit {
        return Err(invalid_remote_response());
    }
    Ok(())
}

pub(crate) fn decode_host_control_error(bytes: &[u8]) -> Option<WorkerError> {
    let body = bytes.strip_suffix(b"\n")?;
    if body.is_empty() || body.contains(&b'\n') || body.contains(&b'\r') {
        return None;
    }
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    let error = HostControlError::deserialize(&mut deserializer).ok()?;
    deserializer.end().ok()?;
    // SSH control responses must remain byte-canonical. Detail decoding can
    // accept future fields on other paths, but silently dropping one here would
    // violate the existing re-encoding check.
    if serde_json::to_vec(&error).ok()?.as_slice() != body {
        return None;
    }
    if let Some(decoded) =
        crate::error::error_from_host_category(error.error().code(), error.error().category())
    {
        return Some(match decoded {
            // Keep admission details for internal callers, but never promote
            // remote text to public output. The catalog still owns exit and hint.
            WorkerError::Capacity { code, .. } => WorkerError::Capacity {
                code,
                message: error.error().message().to_owned().into(),
                public: false,
            },
            other => other,
        });
    }
    Some(WorkerError::Protocol(format!(
        "{}: {}",
        error.error().code(),
        error.error().message()
    )))
}

fn protocol_error_code(error: &WorkerError) -> Option<&str> {
    let WorkerError::Protocol(message) = error else {
        return None;
    };
    message.split_once(": ").map(|(code, _)| code)
}

fn invalid_remote_response() -> WorkerError {
    transport_error("INVALID_RESPONSE", "host response was invalid")
}

fn validate_control_policy(policy: ProcessPolicy) -> Result<(), WorkerError> {
    if policy.stdout_limit == 0
        || policy.stdout_limit > MAX_CONTROL_STDOUT_BYTES
        || policy.stderr_limit == 0
        || policy.stderr_limit > MAX_CONTROL_STDERR_BYTES
        || policy.deadline.is_zero()
        || policy.deadline > MAX_CONTROL_POLICY_DEADLINE
    {
        return Err(transport_error(
            "INVALID_REQUEST",
            "control request policy is invalid",
        ));
    }
    Ok(())
}

pub(crate) fn require_live_identity(
    store: &HostStore,
    identity: &TransferIdentity,
) -> Result<LeaseRecord, WorkerError> {
    let live = crate::lease::LeaseService::new(store)
        .load_for_job(identity.job_id)?
        .ok_or_else(|| {
            host_transfer_error(
                "LEASE_IDENTITY_MISMATCH",
                "live lease identity was rejected",
            )
        })?;
    if live.job_id() != identity.job_id
        || live.client_id() != identity.client_id
        || live.lease_token() != identity.lease_token
        || live.request_fingerprint() != &identity.request_fingerprint
    {
        return Err(host_transfer_error(
            "LEASE_IDENTITY_MISMATCH",
            "live lease identity was rejected",
        ));
    }
    Ok(live)
}

pub(crate) fn require_receivable_disposition(
    store: &HostStore,
    live: &LeaseRecord,
) -> Result<(), WorkerError> {
    match store.disposition(live.job_id())? {
        None => Ok(()),
        Some(disposition @ JobDisposition::Abandoned { .. }) => {
            if disposition_matches_live_lease(&disposition, live) {
                Err(host_transfer_error(
                    "JOB_ABANDONED",
                    "job ID was permanently abandoned",
                ))
            } else {
                Err(job_id_conflict())
            }
        }
        Some(disposition @ JobDisposition::Accepted { .. }) => {
            if disposition_matches_live_lease(&disposition, live) {
                Err(host_transfer_error(
                    "JOB_ACCEPTED",
                    "job ID was already accepted",
                ))
            } else {
                Err(job_id_conflict())
            }
        }
    }
}

fn disposition_matches_live_lease(disposition: &JobDisposition, live: &LeaseRecord) -> bool {
    match disposition {
        JobDisposition::Accepted {
            job_id,
            client_id,
            project_id,
            worktree_id,
            request_fingerprint,
            ..
        } => {
            *job_id == live.job_id()
                && *client_id == live.client_id()
                && project_id == live.project_id()
                && worktree_id == live.worktree_id()
                && request_fingerprint == live.request_fingerprint()
        }
        JobDisposition::Abandoned {
            job_id,
            client_id,
            project_id,
            worktree_id,
            request_fingerprint,
            lease_token_sha256,
            ..
        } => {
            let expected_hash = format!(
                "{:x}",
                Sha256::digest(live.lease_token().to_string().as_bytes())
            );
            *job_id == live.job_id()
                && *client_id == live.client_id()
                && project_id == live.project_id()
                && worktree_id == live.worktree_id()
                && request_fingerprint == live.request_fingerprint()
                && lease_token_sha256 == &expected_hash
        }
    }
}

fn job_id_conflict() -> WorkerError {
    host_transfer_error(
        "JOB_ID_CONFLICT",
        "job ID conflicts with durable host state",
    )
}

fn validate_worker(worker: &WorkerEntry) -> Result<(), WorkerError> {
    let valid_alias = worker
        .ssh
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphanumeric)
        && worker
            .ssh
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'@'));
    if !valid_alias || worker.remote_binary != "~/.local/bin/worker" {
        return Err(transport_error(
            "INVALID_REQUEST",
            "worker transport configuration is invalid",
        ));
    }
    Ok(())
}

fn map_control_failure(error: WorkerError) -> WorkerError {
    match error {
        WorkerError::Process(ProcessError::DeadlineExceeded { .. }) => {
            transport_error("SSH_TIMEOUT", "SSH control request timed out")
        }
        WorkerError::Process(ProcessError::OutputLimitExceeded {
            stream: ProcessStream::Stdout,
            ..
        }) => transport_error(
            "SSH_RESPONSE_TOO_LARGE",
            "SSH control response exceeded its limit",
        ),
        WorkerError::Process(ProcessError::OutputLimitExceeded {
            stream: ProcessStream::Stderr,
            ..
        }) => transport_error(
            "SSH_DIAGNOSTIC_TOO_LARGE",
            "SSH control diagnostic exceeded its limit",
        ),
        _ => transport_error("SSH_LAUNCH_FAILED", "failed to launch SSH control request"),
    }
}

fn transport_error(code: &'static str, message: &'static str) -> WorkerError {
    WorkerError::Transport {
        code,
        message: message.into(),
    }
}

fn host_transfer_error(code: &'static str, message: &'static str) -> WorkerError {
    WorkerError::Protocol(format!("{code}: {message}"))
}

#[cfg(test)]
mod exec_inheritance_tests {
    use std::{
        ffi::{CString, OsString},
        fs::{File, OpenOptions},
        io::{Read, Write},
        os::fd::{AsRawFd, RawFd},
        os::unix::ffi::OsStrExt,
        os::unix::fs::MetadataExt,
        path::{Path, PathBuf},
        process::{Command, Stdio},
        sync::mpsc,
        thread,
    };

    use tempfile::tempdir;

    use super::*;
    use crate::{
        job::{CommandSpec, ProcessIdentity, RequestFingerprintMaterial},
        lease::{AdmissionFacts, LeaseService},
        protocol::MemoryPressure,
        rooted_fs::RootedDir,
        supervisor::{ProcessInspector, ProcessObservation, SystemProcessInspector},
    };

    const CHILD_FD_ENV: &str = "MAC_WORKER_TEST_TRANSFER_LOCK_FD";
    const SENTINEL_FD_ENV: &str = "MAC_WORKER_TEST_SENTINEL_FD";
    const READY_FIFO_ENV: &str = "MAC_WORKER_TEST_READY_FIFO";
    const RELEASE_FIFO_ENV: &str = "MAC_WORKER_TEST_RELEASE_FIFO";
    const TRANSFER_LOCK_PATH_ENV: &str = "MAC_WORKER_TEST_TRANSFER_LOCK_PATH";
    const INTERMEDIARY_PID_FIFO_ENV: &str = "MAC_WORKER_TEST_INTERMEDIARY_PID_FIFO";
    const LEAF_PID_FIFO_ENV: &str = "MAC_WORKER_TEST_LEAF_PID_FIFO";
    const LEAF_EXIT_FIFO_ENV: &str = "MAC_WORKER_TEST_LEAF_EXIT_FIFO";

    fn request() -> LeaseAcquireRequest {
        LeaseAcquireRequest::new(
            RequestFingerprintMaterial::new(
                JobId::new(uuid::Uuid::from_u128(901)),
                ClientId::new(uuid::Uuid::from_u128(902)),
                LeaseToken::new(uuid::Uuid::from_u128(903)),
                1,
                "mini-1".into(),
                "a".repeat(64),
                "b".repeat(64),
                "c".repeat(64),
                String::new(),
                60_000,
                "heavy".into(),
                CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
            )
            .unwrap(),
        )
    }

    fn healthy() -> AdmissionFacts {
        AdmissionFacts {
            free_disk_bytes: 100 * 1024 * 1024 * 1024,
            total_disk_bytes: 250 * 1024 * 1024 * 1024,
            memory_pressure: MemoryPressure::Normal,
            swap_used_bytes: Some(0),
        }
    }

    fn create_fifo(path: &Path) {
        let path = CString::new(path.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
    }

    struct IdentityCheckedProcessCleanup {
        identity: Option<ProcessIdentity>,
    }

    impl IdentityCheckedProcessCleanup {
        fn new(pid: libc::pid_t) -> Self {
            let pid = u32::try_from(pid).expect("reported process PID must be positive");
            let identity = SystemProcessInspector
                .identity_for_pid(pid)
                .expect("reported process must have an inspectable start identity");
            Self {
                identity: Some(identity),
            }
        }

        fn signal(&self, signal: libc::c_int) -> std::io::Result<()> {
            let identity = self.identity.expect("cleanup guard was already disarmed");
            if !matches!(
                SystemProcessInspector.observe(identity),
                ProcessObservation::Matching { .. }
            ) {
                return Err(std::io::Error::other(
                    "process no longer matches the cleanup guard identity",
                ));
            }
            if unsafe { libc::kill(identity.pid() as libc::pid_t, signal) } == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        }

        fn disarm(&mut self) {
            self.identity = None;
        }
    }

    impl Drop for IdentityCheckedProcessCleanup {
        fn drop(&mut self) {
            if let Some(identity) = self.identity
                && matches!(
                    SystemProcessInspector.observe(identity),
                    ProcessObservation::Matching { .. }
                )
            {
                let _ = unsafe { libc::kill(identity.pid() as libc::pid_t, libc::SIGKILL) };
            }
        }
    }

    fn assert_nonblocking_lock_contended(path: &Path) {
        let contender = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        assert_eq!(
            unsafe { libc::flock(contender.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            -1,
            "a separately opened transfer lock unexpectedly acquired while the exec child held it"
        );
        let error = std::io::Error::last_os_error();
        let code = error.raw_os_error();
        assert!(
            code == Some(libc::EWOULDBLOCK) || code == Some(libc::EAGAIN),
            "nonblocking transfer-lock contention failed unexpectedly: {error}"
        );
    }

    fn inheritable_sentinel(path: &Path) -> File {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) };
        assert_ne!(flags, -1);
        assert_ne!(flags & libc::FD_CLOEXEC, 0);
        assert_eq!(
            unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFD, flags & !libc::FD_CLOEXEC) },
            0
        );
        file
    }

    const GIT_ROOT_ENV: &str = "MAC_WORKER_TEST_GIT_RECEIVER_ROOT";
    const GIT_HOOKS_ENV: &str = "MAC_WORKER_TEST_GIT_RECEIVER_HOOKS";
    const GIT_ARM_ENV: &str = "MAC_WORKER_TEST_GIT_LEAF_ARM_FIFO";

    struct ProbedSystemGitExecutor {
        lock_path: PathBuf,
        sentinel_fd: RawFd,
        hooks: PathBuf,
    }

    impl ProbedSystemGitExecutor {
        fn exec_probe(
            &self,
            program: &str,
            mirror: &RootedDir,
            environment: &[(OsString, OsString)],
            transfer: Option<&TransferGuard>,
        ) -> Result<std::convert::Infallible, WorkerError> {
            let metadata = std::fs::metadata(&self.lock_path)?;
            let ceiling = unsafe { libc::sysconf(libc::_SC_OPEN_MAX) };
            assert!(ceiling > 0 && ceiling <= i64::from(i32::MAX));
            // Locate the descriptor already held by HostGitService. Opening a
            // second lock or changing CLOEXEC here would manufacture inheritance.
            let transfer_fd = (3..ceiling as RawFd)
                .find(|descriptor| {
                    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
                    (unsafe { libc::fstat(*descriptor, &mut stat) }) == 0
                        && stat.st_dev as u64 == metadata.dev()
                        && stat.st_ino == metadata.ino()
                })
                .expect("Git receive must hold the exact transfer descriptor before exec");
            let mut environment = environment.to_vec();
            environment.extend([
                (CHILD_FD_ENV.into(), transfer_fd.to_string().into()),
                (SENTINEL_FD_ENV.into(), self.sentinel_fd.to_string().into()),
                // A local pre-receive hook is our observation point after the
                // real SystemGitServerExecutor and stock receive-pack execute.
                ("GIT_CONFIG_COUNT".into(), "1".into()),
                ("GIT_CONFIG_KEY_0".into(), "core.hooksPath".into()),
                (
                    "GIT_CONFIG_VALUE_0".into(),
                    self.hooks.as_os_str().to_owned(),
                ),
            ]);
            // The shell fixture parks Git's protocol stdio on 8/9 while libtest
            // prints its preamble to stderr. Restore it before the production exec.
            assert_eq!(unsafe { libc::dup2(8, 0) }, 0);
            assert_eq!(unsafe { libc::dup2(9, 1) }, 1);
            assert_eq!(unsafe { libc::close(8) }, 0);
            assert_eq!(unsafe { libc::close(9) }, 0);
            let executor = crate::git_transport::SystemGitServerExecutor;
            match transfer {
                Some(transfer) => crate::git_transport::GitServerExecutor::exec_with_transfer_lock(
                    &executor,
                    program,
                    mirror,
                    &environment,
                    transfer,
                ),
                None => crate::git_transport::GitServerExecutor::exec(
                    &executor,
                    program,
                    mirror,
                    &environment,
                ),
            }
        }
    }

    impl crate::git_transport::GitServerExecutor for ProbedSystemGitExecutor {
        fn exec(
            &self,
            program: &str,
            mirror: &RootedDir,
            environment: &[(OsString, OsString)],
        ) -> Result<std::convert::Infallible, WorkerError> {
            self.exec_probe(program, mirror, environment, None)
        }

        fn exec_with_transfer_lock(
            &self,
            program: &str,
            mirror: &RootedDir,
            environment: &[(OsString, OsString)],
            transfer: &TransferGuard,
        ) -> Result<std::convert::Infallible, WorkerError> {
            self.exec_probe(program, mirror, environment, Some(transfer))
        }
    }

    #[test]
    fn git_receiver_process_probe() {
        let Some(root) = std::env::var_os(GIT_ROOT_ENV) else {
            return;
        };
        let root = PathBuf::from(root);
        OpenOptions::new()
            .write(true)
            .open(std::env::var_os(INTERMEDIARY_PID_FIFO_ENV).unwrap())
            .unwrap()
            .write_all(std::process::id().to_string().as_bytes())
            .unwrap();
        let store = HostStore::open(&root).unwrap();
        let request = request().with_execution_scope(crate::job::ExecutionScope::task(
            crate::task::TaskId::new(uuid::Uuid::from_u128(904)),
        ));
        let material = request.material();
        let components = crate::git_transport::ReceivePackComponents::new(
            material.job_id(),
            material.client_id(),
            material.lease_token(),
            request.request_fingerprint().clone(),
        );
        let sentinel = inheritable_sentinel(&root.parent().unwrap().join("git-sentinel"));
        let executor = ProbedSystemGitExecutor {
            lock_path: root
                .join("locks/jobs")
                .join(material.job_id().to_string())
                .join("transfer/transfer.lock"),
            sentinel_fd: sentinel.as_raw_fd(),
            hooks: std::env::var_os(GIT_HOOKS_ENV).unwrap().into(),
        };
        let result = crate::git_transport::HostGitService::new(&store).receive_pack(
            &components,
            material.project_id(),
            &executor,
        );
        panic!("production Git exec unexpectedly returned: {result:?}");
    }

    #[test]
    fn git_receiver_leaf_probe() {
        let Some(ready_fifo) = std::env::var_os(GIT_ARM_ENV) else {
            return;
        };
        OpenOptions::new()
            .write(true)
            .open(std::env::var_os(LEAF_PID_FIFO_ENV).unwrap())
            .unwrap()
            .write_all(std::process::id().to_string().as_bytes())
            .unwrap();
        let mut armed = [0_u8; 1];
        OpenOptions::new()
            .read(true)
            .open(ready_fifo)
            .unwrap()
            .read_exact(&mut armed)
            .unwrap();
        assert_eq!(
            armed, *b"A",
            "leaf must wait until exact PID cleanup is armed"
        );
        let transfer_fd: RawFd = std::env::var(CHILD_FD_ENV).unwrap().parse().unwrap();
        let sentinel_fd: RawFd = std::env::var(SENTINEL_FD_ENV).unwrap().parse().unwrap();
        let checked = std::panic::catch_unwind(|| {
            let transfer_flags = unsafe { libc::fcntl(transfer_fd, libc::F_GETFD) };
            assert_ne!(
                transfer_flags, -1,
                "transfer fd must survive Git receive-pack and hook exec"
            );
            assert_eq!(transfer_flags & libc::FD_CLOEXEC, 0);
            assert_eq!(
                unsafe { libc::fcntl(sentinel_fd, libc::F_GETFD) },
                -1,
                "unrelated inheritable fd survived Git exec"
            );
            let path_metadata =
                std::fs::metadata(std::env::var_os(TRANSFER_LOCK_PATH_ENV).unwrap()).unwrap();
            let mut fd_stat: libc::stat = unsafe { std::mem::zeroed() };
            assert_eq!(unsafe { libc::fstat(transfer_fd, &mut fd_stat) }, 0);
            assert_eq!(fd_stat.st_dev as u64, path_metadata.dev());
            assert_eq!(fd_stat.st_ino, path_metadata.ino());
        });
        let ready = match &checked {
            Ok(()) => "R".to_owned(),
            Err(panic) => format!(
                "F: {}",
                panic
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .unwrap_or("unknown descriptor assertion failure")
            ),
        };
        OpenOptions::new()
            .write(true)
            .open(std::env::var_os(READY_FIFO_ENV).unwrap())
            .unwrap()
            .write_all(ready.as_bytes())
            .unwrap();
        assert!(
            checked.is_ok(),
            "Git descriptor inheritance failed before READY"
        );
        let mut release = [0_u8; 1];
        OpenOptions::new()
            .read(true)
            .open(std::env::var_os(RELEASE_FIFO_ENV).unwrap())
            .unwrap()
            .read_exact(&mut release)
            .unwrap();
        assert_eq!(release, *b"X");
        OpenOptions::new()
            .write(true)
            .open(std::env::var_os(LEAF_EXIT_FIFO_ENV).unwrap())
            .unwrap()
            .write_all(b"E")
            .unwrap();
    }

    struct ReapedGitClient(Option<std::process::Child>);
    impl Drop for ReapedGitClient {
        fn drop(&mut self) {
            if let Some(mut child) = self.0.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    fn fifo_message(path: PathBuf) -> (mpsc::Receiver<String>, thread::JoinHandle<()>) {
        let (tx, rx) = mpsc::channel();
        let reader = thread::spawn(move || {
            let mut message = String::new();
            OpenOptions::new()
                .read(true)
                .open(path)
                .unwrap()
                .read_to_string(&mut message)
                .unwrap();
            let _ = tx.send(message);
        });
        (rx, reader)
    }

    fn shell_literal(path: &Path) -> String {
        format!("'{}'", path.to_str().unwrap().replace('\'', "'\\''"))
    }

    fn assert_system_git_lock_inheritance(kill_intermediary: bool) {
        use crate::{
            host_store::SupervisorGuard,
            job::{ExecutionScope, ResolveOrAbandonOutcome, ResolveOrAbandonRequest},
            task::TaskId,
        };
        struct NeverLaunch;
        impl SupervisorLauncher for NeverLaunch {
            fn launch(
                &self,
                _job: JobId,
                _guard: SupervisorGuard,
            ) -> Result<LaunchCandidate, WorkerError> {
                panic!("preacceptance Git resolution must never launch");
            }
        }
        let fixture = tempdir().unwrap();
        let root = fixture.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let request = request().with_execution_scope(ExecutionScope::task(TaskId::new(
            uuid::Uuid::from_u128(904),
        )));
        LeaseService::new(&store)
            .acquire(&request, &healthy(), 1)
            .unwrap();
        let lock_path = root
            .join("locks/jobs")
            .join(request.material().job_id().to_string())
            .join("transfer/transfer.lock");
        let source = fixture.path().join("source");
        std::fs::create_dir(&source).unwrap();
        let git = |args: &[&str]| {
            let output = Command::new("/usr/bin/git")
                .arg("-C")
                .arg(&source)
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_AUTHOR_NAME", "fixture")
                .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
                .env("GIT_COMMITTER_NAME", "fixture")
                .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "offline Git fixture failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        git(&["init", "--quiet"]);
        std::fs::write(source.join("payload"), b"offline Git lock fixture\n").unwrap();
        git(&["add", "payload"]);
        git(&["commit", "--quiet", "-m", "fixture"]);
        let hooks = fixture.path().join("hooks");
        std::fs::create_dir(&hooks).unwrap();
        let executable = std::env::current_exe().unwrap();
        let hook = hooks.join("pre-receive");
        std::fs::write(&hook, format!("#!/bin/sh\nexec {} --exact transfer::exec_inheritance_tests::git_receiver_leaf_probe --nocapture --test-threads=1\n", shell_literal(&executable))).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o700)).unwrap();
        let server_pid = fixture.path().join("server-pid.fifo");
        let leaf_pid = fixture.path().join("leaf-pid.fifo");
        let leaf_arm = fixture.path().join("leaf-arm.fifo");
        let ready = fixture.path().join("ready.fifo");
        let release = fixture.path().join("release.fifo");
        let exit = fixture.path().join("exit.fifo");
        let status = fixture.path().join("status.fifo");
        for fifo in [
            &server_pid,
            &leaf_pid,
            &leaf_arm,
            &ready,
            &release,
            &exit,
            &status,
        ] {
            create_fifo(fifo);
        }
        let (server_pid_rx, server_pid_thread) = fifo_message(server_pid.clone());
        let (leaf_pid_rx, leaf_pid_thread) = fifo_message(leaf_pid.clone());
        let (ready_rx, ready_thread) = fifo_message(ready.clone());
        let (exit_rx, exit_thread) = fifo_message(exit.clone());
        let (status_rx, status_thread) = fifo_message(status.clone());
        let wrapper = fixture.path().join("receive-pack");
        std::fs::write(&wrapper, format!("#!/bin/sh\nexec 8<&0\nexec 9>&1\nexec 1>&2\n{} --exact transfer::exec_inheritance_tests::git_receiver_process_probe --nocapture --test-threads=1\ncode=$?\nprintf '%s' \"$code\" > {}\nexit \"$code\"\n", shell_literal(&executable), shell_literal(&status))).unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
        let child = Command::new("/usr/bin/git")
            .arg("-C")
            .arg(&source)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env(GIT_ROOT_ENV, &root)
            .env(GIT_HOOKS_ENV, &hooks)
            .env(INTERMEDIARY_PID_FIFO_ENV, &server_pid)
            .env(LEAF_PID_FIFO_ENV, &leaf_pid)
            .env(GIT_ARM_ENV, &leaf_arm)
            .env(READY_FIFO_ENV, &ready)
            .env(RELEASE_FIFO_ENV, &release)
            .env(LEAF_EXIT_FIFO_ENV, &exit)
            .env(TRANSFER_LOCK_PATH_ENV, &lock_path)
            .arg("push")
            .arg(format!("--receive-pack={}", wrapper.display()))
            .arg(fixture.path().join("unused-repository-path"))
            .arg("HEAD:refs/mac-worker/bases/00000000000000000000000000000904")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut client = ReapedGitClient(Some(child));
        let server_pid: libc::pid_t = server_pid_rx
            .recv_timeout(crate::test_support::HANDSHAKE_TIMEOUT)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        server_pid_thread.join().unwrap();
        let mut server_cleanup = IdentityCheckedProcessCleanup::new(server_pid);
        let leaf_pid: libc::pid_t = leaf_pid_rx
            .recv_timeout(crate::test_support::HANDSHAKE_TIMEOUT)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        leaf_pid_thread.join().unwrap();
        let mut leaf_cleanup = IdentityCheckedProcessCleanup::new(leaf_pid);
        OpenOptions::new()
            .write(true)
            .open(&leaf_arm)
            .unwrap()
            .write_all(b"A")
            .unwrap();
        let ready = ready_rx
            .recv_timeout(crate::test_support::HANDSHAKE_TIMEOUT)
            .unwrap();
        ready_thread.join().unwrap();
        assert_eq!(
            ready, "R",
            "leaf must prove exact lock/descriptor inheritance across real Git exec before READY"
        );
        if kill_intermediary {
            server_cleanup
                .signal(libc::SIGKILL)
                .expect("must signal only the exact Git intermediary identity");
            assert_eq!(
                status_rx
                    .recv_timeout(crate::test_support::HANDSHAKE_TIMEOUT)
                    .unwrap()
                    .trim(),
                "137",
                "Git receiver intermediary must be reaped with the SIGKILL shell status before leaf release"
            );
            server_cleanup.disarm();
        }
        assert_nonblocking_lock_contended(&lock_path);
        let resolver_store = HostStore::open(&root).unwrap();
        let submit = SubmitRequest::new(request.material().clone())
            .with_execution_scope(request.execution_scope().clone());
        let resolve = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
        let (classified_tx, classified_rx) = mpsc::channel();
        let (resolved_tx, resolved_rx) = mpsc::channel();
        let resolver = thread::spawn(move || {
            let service = JobService::new_with_resolution_before_transfer(
                &resolver_store,
                &NeverLaunch,
                std::sync::Arc::new(move || classified_tx.send(()).unwrap()),
            );
            resolved_tx
                .send(service.resolve_or_abandon(resolve))
                .unwrap();
        });
        classified_rx
            .recv_timeout(crate::test_support::HANDSHAKE_TIMEOUT)
            .unwrap();
        assert!(
            matches!(resolved_rx.try_recv(), Err(mpsc::TryRecvError::Empty)),
            "resolver completed while the exact Git leaf still held the transfer lock"
        );
        assert!(
            !root
                .join("job-index")
                .join(format!("{}.json", request.material().job_id()))
                .exists()
        );
        OpenOptions::new()
            .write(true)
            .open(&release)
            .unwrap()
            .write_all(b"X")
            .unwrap();
        assert_eq!(
            exit_rx
                .recv_timeout(crate::test_support::HANDSHAKE_TIMEOUT)
                .unwrap()
                .as_bytes(),
            b"E",
            "leaf must acknowledge explicit release before exiting"
        );
        exit_thread.join().unwrap();
        leaf_cleanup.disarm();
        if !kill_intermediary {
            assert_eq!(
                status_rx
                    .recv_timeout(crate::test_support::HANDSHAKE_TIMEOUT)
                    .unwrap()
                    .trim(),
                "0"
            );
            server_cleanup.disarm();
        }
        status_thread.join().unwrap();
        let output = client.0.take().unwrap().wait_with_output().unwrap();
        assert_eq!(
            output.status.success(),
            !kill_intermediary,
            "offline Git push stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let response = resolved_rx
            .recv_timeout(crate::test_support::HANDSHAKE_TIMEOUT)
            .unwrap()
            .unwrap();
        assert!(matches!(
            response.outcome(),
            ResolveOrAbandonOutcome::Abandoned
        ));
        assert!(
            LeaseService::new(&store)
                .load_for_job(request.material().job_id())
                .unwrap()
                .is_none()
        );
        resolver.join().unwrap();
    }

    #[test]
    // Supersedes production_exec_inherits_only_stdio_and_transfer_lock_and_fences_resolver through stock Git receive-pack and its real child hook.
    fn git_exec_inherits_only_stdio_and_transfer_lock_and_fences_resolver() {
        assert_system_git_lock_inheritance(false);
    }

    #[test]
    // Supersedes transfer_lock_inheritance_survives_intermediary_sigkill_and_fences_resolver_until_leaf_release through stock Git receive-pack.
    fn git_transfer_lock_survives_intermediary_sigkill_until_explicit_leaf_release() {
        assert_system_git_lock_inheritance(true);
    }
}

#[cfg(test)]
mod host_control_error_tests {
    use super::{HostControlError, decode_host_control_error};

    #[test]
    fn catalog_capacity_errors_preserve_remote_details_privately() {
        let planted = "/Users/alice/PLANTED_HOST_PATH";
        for code in [
            "CAPACITY_BUSY",
            "INSUFFICIENT_DISK",
            "MEMORY_PRESSURE",
            "SWAP_LIMIT",
            "CAPABILITY_MISSING",
        ] {
            for category in [None, Some("usage"), Some("capacity")] {
                let wire = match category {
                    Some(category) => HostControlError::with_category(code, planted, category),
                    None => HostControlError::new(code, planted),
                }
                .unwrap();
                let mut bytes = serde_json::to_vec(&wire).unwrap();
                bytes.push(b'\n');
                let error = decode_host_control_error(&bytes).unwrap();
                assert!(
                    matches!(&error, crate::error::WorkerError::Capacity { message, public: false, .. } if message == planted)
                );
                assert_eq!(error.public_code(), code);
                assert_eq!(error.exit_code(), 75);
                let diagnostic = crate::error::operator_diagnostic(&error);
                assert!(diagnostic.contains(crate::error::hint_for(code).unwrap()));
                assert!(!diagnostic.contains("PLANTED"));
            }
        }
    }

    #[test]
    fn host_error_responses_still_require_canonical_bytes() {
        let wire = HostControlError::new("TASK_BUSY", "task operation failed").unwrap();
        let canonical = serde_json::to_string(&wire).unwrap();
        assert!(decode_host_control_error(format!("{canonical}\n").as_bytes()).is_some());
        for noncanonical in [
            canonical.clone(),
            format!("{canonical}\r\n"),
            format!("{canonical}\n\n"),
            format!(" {canonical}\n"),
            format!("{canonical} {{}}\n"),
            format!(
                "{}\n",
                canonical.replace("\"code\":", "\"future\":true,\"code\":")
            ),
            format!(
                "{}\n",
                canonical.replace("\"code\":", "\"code\":\"TASK_BUSY\",\"code\":")
            ),
            format!(
                "{}\n",
                canonical.replace("\"code\":", "\"category\":\"invalid\",\"code\":")
            ),
        ] {
            assert!(
                decode_host_control_error(noncanonical.as_bytes()).is_none(),
                "accepted {noncanonical:?}"
            );
        }
    }

    #[test]
    fn retained_result_error_keeps_its_static_message() {
        let planted = "/tmp/not-a-public-result-path";
        let error = HostControlError::new("RESULT_NOT_RETAINED", planted).unwrap();
        let mut bytes = serde_json::to_vec(&error).unwrap();
        bytes.push(b'\n');
        let decoded = decode_host_control_error(&bytes).unwrap();
        assert_eq!(decoded.public_code(), "RESULT_NOT_RETAINED");
        assert_eq!(
            decoded.public_message(),
            "task workspace is closed and its result is no longer retained"
        );
        assert!(!decoded.public_message().contains(planted));
    }
}

/// Fixed-table JSON boundary for additive controller provisioning operations.
pub fn controller_host_request<Req: Serialize, Res: DeserializeOwned + Serialize>(
    runner: &dyn ProcessRunner,
    worker: &WorkerEntry,
    operation: HostOperation,
    request: &Req,
) -> Result<Res, WorkerError> {
    SshJsonTransport::new(runner).request(
        worker,
        operation,
        request,
        crate::controller::provision::PROCESS_POLICY,
    )
}
