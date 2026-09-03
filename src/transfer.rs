use std::{
    ffi::OsString,
    fmt, fs,
    os::unix::{
        ffi::{OsStrExt, OsStringExt},
        process::{CommandExt, ExitStatusExt},
    },
    process::{Command, Stdio},
    sync::LazyLock,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::{
    config::WorkerEntry,
    error::{ProcessError, ProcessStream, WorkerError},
    host_store::{HostStore, JobDisposition},
    job::{
        CancelRequest, CancelResponse, ClientId, CommandSummary, FleetReconcileRequest,
        FleetReconcileResponse, HostControlError, JobId, LeaseAcquireRequest, LeaseRecord,
        LeaseToken, LogChunk, LogChunkRequest, LogChunkResponse, LogStream, MAX_LOG_CHUNK_BYTES,
        PreacceptanceDisposition, RequestFingerprint, ResolveOrAbandonOutcome,
        ResolveOrAbandonRequest, ResolveOrAbandonResponse, StatusRequest, StatusResponse,
        SubmitRequest, SubmitResponse,
    },
    job_service::{JobService, LaunchCandidate, SupervisorLauncher},
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
    rooted_fs::RootedDir,
    snapshot::Snapshot,
};

pub use crate::host_store::TransferGuard;

const SSH_PROGRAM: &str = "/usr/bin/ssh";
const MAX_CONTROL_STDOUT_BYTES: usize = 1024 * 1024;
const MAX_CONTROL_STDERR_BYTES: usize = 64 * 1024;
const MAX_CONTROL_DEADLINE: Duration = Duration::from_secs(30);
const RSYNC_PROGRAM: &str = "/usr/bin/rsync";
const RSYNC_REMOTE_SHELL: &str = "/usr/bin/ssh -o BatchMode=yes -o ConnectTimeout=5 -o ForwardAgent=no -o ClearAllForwardings=yes --";
const RSYNC_POLICY: ProcessPolicy = ProcessPolicy {
    stdout_limit: 256 * 1024,
    stderr_limit: 256 * 1024,
    deadline: Duration::from_secs(15 * 60),
};
const MAX_RSYNC_STATS_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostOperation {
    LeaseAcquire,
    SnapshotVerify,
    Submit,
    Status,
    LogChunk,
    ResolveOrAbandon,
    TaskPrepare,
    TaskStatus,
    TaskDiff,
    TaskClose,
    Cancel,
    Reconcile,
}

impl HostOperation {
    pub fn command(self) -> &'static str {
        match self {
            Self::LeaseAcquire => "~/.local/bin/worker host lease-acquire",
            Self::SnapshotVerify => "~/.local/bin/worker host snapshot-verify",
            Self::Submit => "~/.local/bin/worker host submit",
            Self::Status => "~/.local/bin/worker host status",
            Self::LogChunk => "~/.local/bin/worker host log-chunk",
            Self::ResolveOrAbandon => "~/.local/bin/worker host resolve-or-abandon",
            Self::TaskPrepare => "~/.local/bin/worker host task-prepare",
            Self::TaskStatus => "~/.local/bin/worker host task-status",
            Self::TaskDiff => "~/.local/bin/worker host task-diff",
            Self::TaskClose => "~/.local/bin/worker host task-close",
            Self::Cancel => "~/.local/bin/worker host cancel",
            Self::Reconcile => "~/.local/bin/worker host reconcile",
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

pub struct RemoteJobClient<'a> {
    transport: SshJsonTransport<'a>,
    retry: &'a dyn ResolutionRuntime,
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TransferReceipt {
    pub files_transferred: u64,
    pub file_bytes_transferred: u64,
    pub wire_bytes_sent: u64,
    pub wire_bytes_received: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferFailureDisposition {
    DefinitelyNotStarted,
    ResolveOrAbandon,
}

pub fn transfer_failure_disposition(error: &WorkerError) -> TransferFailureDisposition {
    match error {
        WorkerError::Transport {
            code:
                "INVALID_TRANSFER_CONFIGURATION"
                | "INVALID_SNAPSHOT_SOURCE"
                | "INVALID_TRANSFER_IDENTITY",
            ..
        } => TransferFailureDisposition::DefinitelyNotStarted,
        _ => TransferFailureDisposition::ResolveOrAbandon,
    }
}

pub struct RsyncTransport<'a> {
    runner: &'a dyn ProcessRunner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbandonTransferResult {
    Abandoned,
}

pub struct RsyncServerInvocation<'a> {
    server_args: &'a [OsString],
    destination: &'a RootedDir,
    transfer_guard: &'a TransferGuard,
}

impl<'a> RsyncServerInvocation<'a> {
    pub fn server_args(&self) -> &'a [OsString] {
        self.server_args
    }

    pub fn destination(&self) -> &'a RootedDir {
        self.destination
    }
}

pub trait RsyncServerExecutor: Send + Sync {
    fn execute(&self, invocation: RsyncServerInvocation<'_>) -> Result<(), WorkerError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemRsyncServerExecutor;

impl RsyncServerExecutor for SystemRsyncServerExecutor {
    fn execute(&self, invocation: RsyncServerInvocation<'_>) -> Result<(), WorkerError> {
        let mut command = Command::new(RSYNC_PROGRAM);
        command
            .args(invocation.server_args)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        execute_server_child(&mut command, invocation)
    }
}

fn execute_server_child(
    command: &mut Command,
    invocation: RsyncServerInvocation<'_>,
) -> Result<(), WorkerError> {
    invocation.destination.verify_descriptors_cloexec()?;
    let destination_fd = invocation.destination.raw_directory_fd();
    let transfer_fd = invocation.transfer_guard.raw_lock_fd_for_exec()?;
    let fd_ceiling = unsafe { libc::sysconf(libc::_SC_OPEN_MAX) };
    if fd_ceiling < 0 || fd_ceiling > i64::from(i32::MAX) {
        return Err(WorkerError::Io(std::io::Error::last_os_error()));
    }
    let fd_ceiling = fd_ceiling as i32;
    unsafe {
        command.pre_exec(move || {
            if libc::fchdir(destination_fd) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            for descriptor in 3..fd_ceiling {
                if descriptor != transfer_fd {
                    let flags = libc::fcntl(descriptor, libc::F_GETFD);
                    if flags == -1 {
                        let error = std::io::Error::last_os_error();
                        if error.raw_os_error() != Some(libc::EBADF) {
                            return Err(error);
                        }
                    } else if flags & libc::FD_CLOEXEC == 0
                        && libc::fcntl(descriptor, libc::F_SETFD, flags | libc::FD_CLOEXEC) == -1
                    {
                        return Err(std::io::Error::last_os_error());
                    }
                }
            }
            let flags = libc::fcntl(transfer_fd, libc::F_GETFD);
            if flags == -1
                || libc::fcntl(transfer_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) == -1
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let status = command.spawn()?.wait()?;
    if status.success() {
        return Ok(());
    }
    let code = status
        .code()
        .and_then(|code| u8::try_from(code).ok())
        .or_else(|| {
            status
                .signal()
                .and_then(|signal| u8::try_from(signal).ok())
                .and_then(|signal| 128_u8.checked_add(signal))
        })
        .ok_or_else(|| WorkerError::Protocol("rsync server child status was invalid".into()))?;
    Err(WorkerError::CommandExit { code })
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
    pub fn receive(
        &self,
        identity: &TransferIdentity,
        supplied_server_args: &[OsString],
        executor: &dyn RsyncServerExecutor,
    ) -> Result<(), WorkerError> {
        let server_args = validated_server_args(supplied_server_args)?;
        self.store.validate_layout()?;
        let admission = self.store.admission_lock(identity.job_id)?;
        admission.validate()?;
        if crate::lease::LeaseService::new(self.store)
            .load()?
            .is_none()
        {
            require_receivable_identity_disposition(self.store, identity)?;
        }
        let live = require_live_identity(self.store, identity)?;
        require_receivable_disposition(self.store, &live)?;
        let transfer = self
            .store
            .transfer_lock_after(&admission, identity.job_id)?;
        admission.validate()?;
        transfer.validate()?;
        let live = require_live_identity(self.store, identity)?;
        require_receivable_disposition(self.store, &live)?;
        let destination = self.store.open_directory(
            &format!("incoming/{}/{}", identity.job_id, identity.lease_token),
            true,
        )?;
        admission.validate()?;
        transfer.validate()?;
        self.store.verify_descriptors_cloexec()?;
        drop(admission);
        executor.execute(RsyncServerInvocation {
            server_args: &server_args,
            destination: &destination,
            transfer_guard: &transfer,
        })
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

impl<'a> RsyncTransport<'a> {
    pub fn new(runner: &'a dyn ProcessRunner) -> Self {
        Self { runner }
    }

    pub fn upload(
        &self,
        worker: &WorkerEntry,
        snapshot: &Snapshot,
        identity: &TransferIdentity,
    ) -> Result<TransferReceipt, WorkerError> {
        validate_rsync_worker(worker)?;
        let metadata = fs::symlink_metadata(snapshot.publication_root()).map_err(|_| {
            transport_error(
                "INVALID_SNAPSHOT_SOURCE",
                "verified snapshot publication is unavailable",
            )
        })?;
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            return Err(transport_error(
                "INVALID_SNAPSHOT_SOURCE",
                "verified snapshot publication is unavailable",
            ));
        }

        let mut source = snapshot.publication_root().as_os_str().as_bytes().to_vec();
        if !source.ends_with(b"/") {
            source.push(b'/');
        }
        let remote_command = format!(
            "--rsync-path=~/.local/bin/worker host rsync-receive {} {} {} {}",
            identity.job_id, identity.client_id, identity.lease_token, identity.request_fingerprint
        );
        let request = ProcessRequest {
            program: RSYNC_PROGRAM.into(),
            args: vec![
                "--archive".into(),
                "--delete".into(),
                "--no-owner".into(),
                "--no-group".into(),
                "--stats".into(),
                "-e".into(),
                RSYNC_REMOTE_SHELL.into(),
                remote_command.into(),
                OsString::from_vec(source),
                format!("{}:incoming", worker.ssh).into(),
            ],
            environment: vec![("LC_ALL".into(), "C".into()), ("LANG".into(), "C".into())],
            environment_remove: vec!["LANGUAGE".into()],
            stdin: None,
            policy: RSYNC_POLICY,
        };
        let result = self.runner.run(&request).map_err(map_rsync_failure)?;
        if result.stdout.len() > RSYNC_POLICY.stdout_limit {
            return Err(transport_error(
                "RSYNC_OUTPUT_TOO_LARGE",
                "rsync output exceeded its limit",
            ));
        }
        if result.stderr.len() > RSYNC_POLICY.stderr_limit {
            return Err(transport_error(
                "RSYNC_OUTPUT_TOO_LARGE",
                "rsync output exceeded its limit",
            ));
        }
        if !result.status.success() {
            return Err(transport_error("RSYNC_FAILED", "rsync upload failed"));
        }
        if !result.stderr.is_empty() {
            return Err(transport_error(
                "RSYNC_DIAGNOSTIC_PRESENT",
                "rsync emitted an unexpected diagnostic",
            ));
        }
        parse_rsync_stats(&result.stdout)
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
        validate_worker(worker)?;
        validate_control_policy(policy)?;
        let stdin = serde_json::to_vec(request)
            .map_err(|_| transport_error("INVALID_REQUEST", "control request is invalid"))?;
        let request = ProcessRequest {
            program: SSH_PROGRAM.into(),
            args: vec![
                "-o".into(),
                "BatchMode=yes".into(),
                "-o".into(),
                "ConnectTimeout=5".into(),
                "-o".into(),
                "ForwardAgent=no".into(),
                "-o".into(),
                "ClearAllForwardings=yes".into(),
                "--".into(),
                worker.ssh.clone().into(),
                operation.command().into(),
            ],
            environment: Vec::new(),
            environment_remove: Vec::new(),
            stdin: Some(stdin),
            policy,
        };
        let result = self.runner.run(&request).map_err(map_control_failure)?;
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
            return Err(
                decode_host_control_error(&result.stdout).unwrap_or_else(|| {
                    transport_error("HOST_REQUEST_FAILED", "SSH control request failed")
                }),
            );
        }
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
        }
    }

    pub(crate) fn process_runner(&self) -> &'a dyn ProcessRunner {
        self.transport.runner
    }

    pub fn status(
        &self,
        worker: &WorkerEntry,
        job_id: JobId,
    ) -> Result<StatusResponse, WorkerError> {
        self.status_with_deadline(worker, job_id, MAX_CONTROL_DEADLINE)
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
        Ok(chunk)
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

fn decode_host_control_error(bytes: &[u8]) -> Option<WorkerError> {
    let body = bytes.strip_suffix(b"\n")?;
    if body.is_empty() || body.contains(&b'\n') || body.contains(&b'\r') {
        return None;
    }
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    let error = HostControlError::deserialize(&mut deserializer).ok()?;
    deserializer.end().ok()?;
    if serde_json::to_vec(&error).ok()?.as_slice() != body {
        return None;
    }
    let capacity_code = match error.error().code() {
        "CAPACITY_BUSY" => Some("CAPACITY_BUSY"),
        "INSUFFICIENT_DISK" => Some("INSUFFICIENT_DISK"),
        "MEMORY_PRESSURE" => Some("MEMORY_PRESSURE"),
        "SWAP_LIMIT" => Some("SWAP_LIMIT"),
        _ => None,
    };
    if let Some(code) = capacity_code {
        return Some(WorkerError::Capacity {
            code,
            message: error.error().message().to_owned(),
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
        || policy.deadline > MAX_CONTROL_DEADLINE
    {
        return Err(transport_error(
            "INVALID_REQUEST",
            "control request policy is invalid",
        ));
    }
    Ok(())
}

fn validated_server_args(supplied: &[OsString]) -> Result<Vec<OsString>, WorkerError> {
    const EXPECTED: &[&str] = &[
        "--server",
        "--delete-before",
        "-l",
        "-p",
        "-D",
        "-r",
        "-t",
        "--dirs",
        ".",
        "incoming",
    ];
    if supplied.len() != EXPECTED.len()
        || supplied
            .iter()
            .zip(EXPECTED)
            .any(|(actual, expected)| actual != expected)
    {
        return Err(host_transfer_error(
            "INVALID_RSYNC_SERVER_ARGS",
            "rsync server arguments were rejected",
        ));
    }
    let mut validated = supplied.to_vec();
    *validated
        .last_mut()
        .expect("the fixed rsync server shape has a sink") = ".".into();
    Ok(validated)
}

pub(crate) fn require_live_identity(
    store: &HostStore,
    identity: &TransferIdentity,
) -> Result<LeaseRecord, WorkerError> {
    let live = crate::lease::LeaseService::new(store)
        .load()?
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

fn require_receivable_identity_disposition(
    store: &HostStore,
    identity: &TransferIdentity,
) -> Result<(), WorkerError> {
    match store.disposition(identity.job_id())? {
        None => Ok(()),
        Some(JobDisposition::Abandoned {
            job_id,
            client_id,
            request_fingerprint,
            lease_token_sha256,
            ..
        }) => {
            let token_hash = format!(
                "{:x}",
                Sha256::digest(identity.lease_token.to_string().as_bytes())
            );
            if job_id == identity.job_id
                && client_id == identity.client_id
                && request_fingerprint == identity.request_fingerprint
                && lease_token_sha256 == token_hash
            {
                Err(host_transfer_error(
                    "JOB_ABANDONED",
                    "job ID was permanently abandoned",
                ))
            } else {
                Err(job_id_conflict())
            }
        }
        Some(JobDisposition::Accepted {
            job_id,
            client_id,
            request_fingerprint,
            ..
        }) if job_id == identity.job_id
            && client_id == identity.client_id
            && request_fingerprint == identity.request_fingerprint =>
        {
            Err(host_transfer_error(
                "JOB_ACCEPTED",
                "job ID was already accepted",
            ))
        }
        Some(JobDisposition::Accepted { .. }) => Err(job_id_conflict()),
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

fn validate_rsync_worker(worker: &WorkerEntry) -> Result<(), WorkerError> {
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
            "INVALID_TRANSFER_CONFIGURATION",
            "worker transfer configuration is invalid",
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

fn map_rsync_failure(error: WorkerError) -> WorkerError {
    match error {
        WorkerError::Process(ProcessError::DeadlineExceeded { .. }) => {
            transport_error("RSYNC_TIMEOUT", "rsync upload timed out")
        }
        WorkerError::Process(ProcessError::OutputLimitExceeded { .. }) => {
            transport_error("RSYNC_OUTPUT_TOO_LARGE", "rsync output exceeded its limit")
        }
        _ => transport_error("RSYNC_LAUNCH_FAILED", "failed to launch rsync upload"),
    }
}

fn parse_rsync_stats(bytes: &[u8]) -> Result<TransferReceipt, WorkerError> {
    if bytes.len() > MAX_RSYNC_STATS_BYTES {
        return Err(invalid_stats());
    }
    let text = std::str::from_utf8(bytes).map_err(|_| invalid_stats())?;
    let lines = text.split('\n').collect::<Vec<_>>();
    let optional_line_offset = match lines.len() {
        13 => 0,
        15 => {
            millisecond_counter(lines[7], "File list generation time: ", " seconds")?;
            millisecond_counter(lines[8], "File list transfer time: ", " seconds")?;
            2
        }
        _ => return Err(invalid_stats()),
    };
    if !lines[9 + optional_line_offset].is_empty() || !lines[12 + optional_line_offset].is_empty() {
        return Err(invalid_stats());
    }

    let files = counter(lines[0], "Number of files: ", "")?;
    let files_transferred = counter(lines[1], "Number of files transferred: ", "")?;
    let total_file_size = counter(lines[2], "Total file size: ", " B")?;
    let transferred_size = counter(lines[3], "Total transferred file size: ", " B")?;
    let unmatched = counter(lines[4], "Unmatched data: ", " B")?;
    let matched = counter(lines[5], "Matched data: ", " B")?;
    let _file_list_size = counter(lines[6], "File list size: ", " B")?;
    let total_sent = counter(lines[7 + optional_line_offset], "Total sent: ", " B")?;
    let total_received = counter(lines[8 + optional_line_offset], "Total received: ", " B")?;
    if files_transferred > files
        || transferred_size > total_file_size
        || unmatched.checked_add(matched) != Some(transferred_size)
    {
        return Err(invalid_stats());
    }

    let summary = lines[10 + optional_line_offset]
        .strip_prefix("sent ")
        .and_then(|value| value.split_once(" bytes  received "))
        .ok_or_else(invalid_stats)?;
    let sent = decimal(summary.0)?;
    let (received, rate) = summary.1.split_once(" bytes  ").ok_or_else(invalid_stats)?;
    let received = decimal(received)?;
    let rate = rate.strip_suffix(" bytes/sec").ok_or_else(invalid_stats)?;
    let _rate = decimal(rate)?;
    if sent != total_sent || received != total_received {
        return Err(invalid_stats());
    }

    let total = lines[11 + optional_line_offset]
        .strip_prefix("total size is ")
        .and_then(|value| value.split_once("  speedup is "))
        .ok_or_else(invalid_stats)?;
    if decimal(total.0)? != total_file_size || !valid_decimal_fraction(total.1) {
        return Err(invalid_stats());
    }

    Ok(TransferReceipt {
        files_transferred,
        file_bytes_transferred: transferred_size,
        wire_bytes_sent: total_sent,
        wire_bytes_received: total_received,
    })
}

fn counter(line: &str, prefix: &str, suffix: &str) -> Result<u64, WorkerError> {
    let value = line
        .strip_prefix(prefix)
        .and_then(|value| value.strip_suffix(suffix))
        .ok_or_else(invalid_stats)?;
    decimal(value)
}

fn millisecond_counter(line: &str, prefix: &str, suffix: &str) -> Result<(), WorkerError> {
    let value = line
        .strip_prefix(prefix)
        .and_then(|value| value.strip_suffix(suffix))
        .ok_or_else(invalid_stats)?;
    let (seconds, milliseconds) = value.split_once('.').ok_or_else(invalid_stats)?;
    if seconds.is_empty()
        || !seconds.bytes().all(|byte| byte.is_ascii_digit())
        || milliseconds.len() != 3
        || !milliseconds.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid_stats());
    }
    Ok(())
}

fn decimal(value: &str) -> Result<u64, WorkerError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_stats());
    }
    value.parse().map_err(|_| invalid_stats())
}

fn valid_decimal_fraction(value: &str) -> bool {
    let mut components = value.split('.');
    let whole = components.next().unwrap_or_default();
    let fractional = components.next().unwrap_or_default();
    components.next().is_none()
        && !whole.is_empty()
        && !fractional.is_empty()
        && whole.bytes().all(|byte| byte.is_ascii_digit())
        && fractional.bytes().all(|byte| byte.is_ascii_digit())
}

fn invalid_stats() -> WorkerError {
    transport_error("INVALID_RSYNC_STATS", "rsync statistics were invalid")
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
        ffi::CString,
        fs::{File, OpenOptions},
        io::{Read, Write},
        os::fd::{AsRawFd, FromRawFd, RawFd},
        os::unix::ffi::OsStrExt,
        os::unix::fs::MetadataExt,
        path::{Path, PathBuf},
        process::Stdio,
        sync::mpsc,
        thread,
        time::Duration,
    };

    use tempfile::tempdir;

    use super::*;
    use crate::{
        job::{CommandSpec, ProcessIdentity, RequestFingerprintMaterial},
        lease::{AdmissionFacts, LeaseService},
        protocol::MemoryPressure,
        supervisor::{ProcessInspector, ProcessObservation, SystemProcessInspector},
    };

    const CHILD_FD_ENV: &str = "MAC_WORKER_TEST_TRANSFER_LOCK_FD";
    const SENTINEL_FD_ENV: &str = "MAC_WORKER_TEST_SENTINEL_FD";
    const READY_FIFO_ENV: &str = "MAC_WORKER_TEST_READY_FIFO";
    const RELEASE_FIFO_ENV: &str = "MAC_WORKER_TEST_RELEASE_FIFO";
    const TRANSFER_LOCK_PATH_ENV: &str = "MAC_WORKER_TEST_TRANSFER_LOCK_PATH";
    const INTERMEDIARY_PID_FIFO_ENV: &str = "MAC_WORKER_TEST_INTERMEDIARY_PID_FIFO";
    const LEAF_PID_FIFO_ENV: &str = "MAC_WORKER_TEST_LEAF_PID_FIFO";
    const LEAF_PID_REPORTER_ENV: &str = "MAC_WORKER_TEST_LEAF_PID_REPORTER";
    const LEAF_ARM_FIFO_ENV: &str = "MAC_WORKER_TEST_LEAF_ARM_FIFO";
    const LEAF_ARM_FD_ENV: &str = "MAC_WORKER_TEST_LEAF_ARM_FD";
    const INTERMEDIARY_PARK_FIFO_ENV: &str = "MAC_WORKER_TEST_INTERMEDIARY_PARK_FIFO";
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

    fn stock_server_args() -> Vec<OsString> {
        [
            "--server",
            "--delete-before",
            "-l",
            "-p",
            "-D",
            "-r",
            "-t",
            "--dirs",
            ".",
            "incoming",
        ]
        .into_iter()
        .map(OsString::from)
        .collect()
    }

    struct ProductionExecProbe {
        child_pid_fifo: PathBuf,
        ready_fifo: PathBuf,
        release_fifo: PathBuf,
        lock_path: PathBuf,
        sentinel_fd: RawFd,
    }

    impl RsyncServerExecutor for ProductionExecProbe {
        fn execute(&self, invocation: RsyncServerInvocation<'_>) -> Result<(), WorkerError> {
            let transfer_fd = invocation.transfer_guard.raw_lock_fd_for_exec()?;
            let mut command = Command::new(std::env::current_exe()?);
            command
                .arg("--exact")
                .arg("transfer::exec_inheritance_tests::production_exec_child_probe")
                .arg("--nocapture")
                .arg("--test-threads=1")
                .env(CHILD_FD_ENV, transfer_fd.to_string())
                .env(SENTINEL_FD_ENV, self.sentinel_fd.to_string())
                .env(LEAF_PID_FIFO_ENV, &self.child_pid_fifo)
                .env(LEAF_PID_REPORTER_ENV, "self")
                .env(READY_FIFO_ENV, &self.ready_fifo)
                .env(RELEASE_FIFO_ENV, &self.release_fifo)
                .env(TRANSFER_LOCK_PATH_ENV, &self.lock_path)
                .stdin(Stdio::inherit())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit());
            execute_server_child(&mut command, invocation)
        }
    }

    struct IntermediaryExecProbe {
        intermediary_pid_fifo: PathBuf,
        leaf_pid_fifo: PathBuf,
        leaf_arm_fifo: PathBuf,
        park_fifo: PathBuf,
        ready_fifo: PathBuf,
        release_fifo: PathBuf,
        exit_fifo: PathBuf,
        lock_path: PathBuf,
        sentinel_fd: RawFd,
    }

    impl RsyncServerExecutor for IntermediaryExecProbe {
        fn execute(&self, invocation: RsyncServerInvocation<'_>) -> Result<(), WorkerError> {
            let transfer_fd = invocation.transfer_guard.raw_lock_fd_for_exec()?;
            let mut command = Command::new(std::env::current_exe()?);
            command
                .arg("--exact")
                .arg("transfer::exec_inheritance_tests::production_exec_intermediary_probe")
                .arg("--nocapture")
                .arg("--test-threads=1")
                .env(CHILD_FD_ENV, transfer_fd.to_string())
                .env(SENTINEL_FD_ENV, self.sentinel_fd.to_string())
                .env(INTERMEDIARY_PID_FIFO_ENV, &self.intermediary_pid_fifo)
                .env(LEAF_PID_FIFO_ENV, &self.leaf_pid_fifo)
                .env(LEAF_ARM_FIFO_ENV, &self.leaf_arm_fifo)
                .env(INTERMEDIARY_PARK_FIFO_ENV, &self.park_fifo)
                .env(READY_FIFO_ENV, &self.ready_fifo)
                .env(RELEASE_FIFO_ENV, &self.release_fifo)
                .env(LEAF_EXIT_FIFO_ENV, &self.exit_fifo)
                .env(TRANSFER_LOCK_PATH_ENV, &self.lock_path)
                .stdin(Stdio::inherit())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit());
            execute_server_child(&mut command, invocation)
        }
    }

    struct FailedExec;

    impl RsyncServerExecutor for FailedExec {
        fn execute(&self, invocation: RsyncServerInvocation<'_>) -> Result<(), WorkerError> {
            let mut command = Command::new("/definitely/missing/mac-worker-rsync");
            execute_server_child(&mut command, invocation)
        }
    }

    struct Exit23;

    impl RsyncServerExecutor for Exit23 {
        fn execute(&self, invocation: RsyncServerInvocation<'_>) -> Result<(), WorkerError> {
            let mut command = Command::new("/bin/sh");
            command.args(["-c", "exit 23"]);
            execute_server_child(&mut command, invocation)
        }
    }

    struct SignalTerm;

    impl RsyncServerExecutor for SignalTerm {
        fn execute(&self, invocation: RsyncServerInvocation<'_>) -> Result<(), WorkerError> {
            let mut command = Command::new("/bin/sh");
            command.args(["-c", "kill -TERM $$"]);
            execute_server_child(&mut command, invocation)
        }
    }

    #[test]
    fn production_exec_child_probe() {
        let Ok(ready_fifo) = std::env::var(READY_FIFO_ENV) else {
            return;
        };
        let release_fifo = std::env::var(RELEASE_FIFO_ENV).unwrap();
        let child_pid_fifo = std::env::var(LEAF_PID_FIFO_ENV).unwrap();
        let pid_reporter = std::env::var(LEAF_PID_REPORTER_ENV).unwrap();
        let lock_path = std::env::var(TRANSFER_LOCK_PATH_ENV).unwrap();
        let transfer_fd: RawFd = std::env::var(CHILD_FD_ENV).unwrap().parse().unwrap();
        let sentinel_fd: RawFd = std::env::var(SENTINEL_FD_ENV).unwrap().parse().unwrap();
        match pid_reporter.as_str() {
            "self" => {
                OpenOptions::new()
                    .write(true)
                    .open(&child_pid_fifo)
                    .unwrap()
                    .write_all(std::process::id().to_string().as_bytes())
                    .unwrap();
            }
            "intermediary" => {
                let arm_fd: RawFd = std::env::var(LEAF_ARM_FD_ENV).unwrap().parse().unwrap();
                let mut armed = [0_u8; 1];
                unsafe { File::from_raw_fd(arm_fd) }
                    .read_exact(&mut armed)
                    .unwrap();
                assert_eq!(
                    armed, *b"A",
                    "leaf must not enter its READY path before cleanup is armed"
                );
            }
            other => panic!("unexpected leaf PID reporter marker: {other}"),
        }
        let transfer_flags = unsafe { libc::fcntl(transfer_fd, libc::F_GETFD) };
        assert_ne!(transfer_flags, -1, "transfer fd must survive exec");
        assert_eq!(transfer_flags & libc::FD_CLOEXEC, 0);
        assert_eq!(
            unsafe { libc::fcntl(sentinel_fd, libc::F_GETFD) },
            -1,
            "unrelated inheritable fd survived exec"
        );
        // Prove the inherited descriptor is the exact transfer lock, not
        // merely a same-numbered descriptor pointing elsewhere: fstat the
        // canonical lock path independently and compare device/inode.
        let path_metadata = std::fs::metadata(lock_path).unwrap();
        let mut fd_stat: libc::stat = unsafe { std::mem::zeroed() };
        assert_eq!(unsafe { libc::fstat(transfer_fd, &mut fd_stat) }, 0);
        assert_eq!(fd_stat.st_dev as u64, path_metadata.dev());
        assert_eq!(fd_stat.st_ino, path_metadata.ino());
        OpenOptions::new()
            .write(true)
            .open(ready_fifo)
            .unwrap()
            .write_all(b"R")
            .unwrap();
        let mut release = [0_u8; 1];
        OpenOptions::new()
            .read(true)
            .open(release_fifo)
            .unwrap()
            .read_exact(&mut release)
            .unwrap();
        assert_eq!(release, *b"X");
        if let Ok(exit_fifo) = std::env::var(LEAF_EXIT_FIFO_ENV) {
            OpenOptions::new()
                .write(true)
                .open(exit_fifo)
                .unwrap()
                .write_all(b"E")
                .unwrap();
        }
    }

    #[test]
    #[allow(clippy::zombie_processes)] // The harness terminates this intermediary before it can reap its leaf.
    fn production_exec_intermediary_probe() {
        let Ok(own_pid_fifo) = std::env::var(INTERMEDIARY_PID_FIFO_ENV) else {
            return;
        };
        let leaf_pid_fifo = std::env::var(LEAF_PID_FIFO_ENV).unwrap();
        let leaf_arm_fifo = std::env::var(LEAF_ARM_FIFO_ENV).unwrap();
        let park_fifo = std::env::var(INTERMEDIARY_PARK_FIFO_ENV).unwrap();
        let transfer_fd = std::env::var(CHILD_FD_ENV).unwrap();
        let sentinel_fd: RawFd = std::env::var(SENTINEL_FD_ENV).unwrap().parse().unwrap();
        let ready_fifo = std::env::var(READY_FIFO_ENV).unwrap();
        let release_fifo = std::env::var(RELEASE_FIFO_ENV).unwrap();
        let exit_fifo = std::env::var(LEAF_EXIT_FIFO_ENV).unwrap();
        let lock_path = std::env::var(TRANSFER_LOCK_PATH_ENV).unwrap();

        // Report our own PID first so the harness can terminate this exact
        // intermediary by PID once the leaf below reports READY.
        OpenOptions::new()
            .write(true)
            .open(&own_pid_fifo)
            .unwrap()
            .write_all(std::process::id().to_string().as_bytes())
            .unwrap();

        assert_eq!(
            unsafe { libc::fcntl(sentinel_fd, libc::F_GETFD) },
            -1,
            "unrelated inheritable fd survived exec"
        );
        let mut arm_pipe = [-1; 2];
        assert_eq!(unsafe { libc::pipe(arm_pipe.as_mut_ptr()) }, 0);
        let arm_reader = unsafe { File::from_raw_fd(arm_pipe[0]) };
        let mut arm_writer = unsafe { File::from_raw_fd(arm_pipe[1]) };
        let writer_flags = unsafe { libc::fcntl(arm_writer.as_raw_fd(), libc::F_GETFD) };
        assert_ne!(writer_flags, -1);
        assert_eq!(
            unsafe {
                libc::fcntl(
                    arm_writer.as_raw_fd(),
                    libc::F_SETFD,
                    writer_flags | libc::FD_CLOEXEC,
                )
            },
            0
        );

        let leaf = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("transfer::exec_inheritance_tests::production_exec_child_probe")
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(CHILD_FD_ENV, &transfer_fd)
            .env(SENTINEL_FD_ENV, sentinel_fd.to_string())
            .env(LEAF_PID_FIFO_ENV, &leaf_pid_fifo)
            .env(LEAF_PID_REPORTER_ENV, "intermediary")
            .env(LEAF_ARM_FD_ENV, arm_reader.as_raw_fd().to_string())
            .env(READY_FIFO_ENV, &ready_fifo)
            .env(RELEASE_FIFO_ENV, &release_fifo)
            .env(LEAF_EXIT_FIFO_ENV, &exit_fifo)
            .env(TRANSFER_LOCK_PATH_ENV, &lock_path)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        drop(arm_reader);

        // Command::spawn returns the Child before the leaf can pass the arm
        // pipe. Report that exact PID; the leaf is explicitly marked not to
        // duplicate this FIFO write.
        OpenOptions::new()
            .write(true)
            .open(&leaf_pid_fifo)
            .unwrap()
            .write_all(leaf.id().to_string().as_bytes())
            .unwrap();
        let mut armed = [0_u8; 1];
        OpenOptions::new()
            .read(true)
            .open(&leaf_arm_fifo)
            .unwrap()
            .read_exact(&mut armed)
            .unwrap();
        assert_eq!(armed, *b"A", "harness must confirm leaf cleanup is armed");
        arm_writer.write_all(b"A").unwrap();
        drop(arm_writer);

        // Park on a FIFO nobody writes to: the harness terminates this exact
        // intermediary PID once the leaf reports READY. This process never
        // reaps, signals, or otherwise influences the leaf itself.
        let mut sink = [0_u8; 1];
        let _ = OpenOptions::new()
            .read(true)
            .open(&park_fifo)
            .unwrap()
            .read(&mut sink);
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

    #[test]
    fn production_exec_inherits_only_stdio_and_transfer_lock_and_fences_resolver() {
        let fixture = tempdir().unwrap();
        let root = fixture.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let request = request();
        LeaseService::new(&store)
            .acquire(&request, &healthy(), 1)
            .unwrap();
        let identity = TransferIdentity::from_acquire_request(&request).unwrap();
        let lock_path = root
            .join("locks/jobs")
            .join(request.material().job_id().to_string())
            .join("transfer/transfer.lock");
        let sentinel = inheritable_sentinel(&fixture.path().join("sentinel"));
        let child_pid_fifo = fixture.path().join("child-pid.fifo");
        let ready_fifo = fixture.path().join("ready.fifo");
        let release_fifo = fixture.path().join("release.fifo");
        create_fifo(&child_pid_fifo);
        create_fifo(&ready_fifo);
        create_fifo(&release_fifo);
        let (child_pid_tx, child_pid_rx) = mpsc::channel();
        let child_pid_reader = child_pid_fifo.clone();
        let child_pid_thread = thread::spawn(move || {
            let mut buffer = String::new();
            OpenOptions::new()
                .read(true)
                .open(child_pid_reader)
                .unwrap()
                .read_to_string(&mut buffer)
                .unwrap();
            let _ = child_pid_tx.send(buffer.trim().parse::<libc::pid_t>().unwrap());
        });
        let (accepted_tx, accepted_rx) = mpsc::channel();
        let ready_reader = ready_fifo.clone();
        let acceptor = thread::spawn(move || {
            let result = (|| -> std::io::Result<u8> {
                let mut ready = [0_u8; 1];
                OpenOptions::new()
                    .read(true)
                    .open(ready_reader)?
                    .read_exact(&mut ready)?;
                Ok(ready[0])
            })();
            let _ = accepted_tx.send(result);
        });
        let receiver_store = store.clone();
        let receiver_identity = identity.clone();
        let receiver_ready = ready_fifo;
        let receiver_release = release_fifo.clone();
        let receiver_child_pid = child_pid_fifo;
        let receiver_lock_path = lock_path.clone();
        let sentinel_fd = sentinel.as_raw_fd();
        let (receiver_tx, receiver_rx) = mpsc::channel();
        let receiver = thread::spawn(move || {
            let outcome = HostTransferService::new(&receiver_store).receive(
                &receiver_identity,
                &stock_server_args(),
                &ProductionExecProbe {
                    child_pid_fifo: receiver_child_pid,
                    ready_fifo: receiver_ready,
                    release_fifo: receiver_release,
                    lock_path: receiver_lock_path,
                    sentinel_fd,
                },
            );
            let _ = receiver_tx.send(outcome);
        });
        let child_pid = child_pid_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        child_pid_thread.join().unwrap();
        let mut child_cleanup = IdentityCheckedProcessCleanup::new(child_pid);
        let ready = accepted_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        acceptor.join().unwrap();
        assert_eq!(ready, b'R');

        let resolver_store = HostStore::open(&root).unwrap();
        let resolver_request = request.clone();
        let (resolved_tx, resolved_rx) = mpsc::channel();
        let (entered_tx, entered_rx) = mpsc::channel();
        let resolver = thread::spawn(move || {
            let _ = entered_tx.send(());
            let _ = resolved_tx
                .send(HostTransferService::new(&resolver_store).abandon(&resolver_request, 2));
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            matches!(resolved_rx.try_recv(), Err(mpsc::TryRecvError::Empty)),
            "resolver completed while the transfer was still in flight"
        );
        OpenOptions::new()
            .write(true)
            .open(&release_fifo)
            .unwrap()
            .write_all(b"X")
            .unwrap();
        receiver_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("exec receiver did not finish after explicit child release")
            .unwrap();
        receiver.join().unwrap();
        child_cleanup.disarm();
        assert_eq!(
            resolved_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
            AbandonTransferResult::Abandoned
        );
        resolver.join().unwrap();
    }

    #[test]
    fn transfer_lock_inheritance_survives_intermediary_sigkill_and_fences_resolver_until_leaf_release()
     {
        let fixture = tempdir().unwrap();
        let root = fixture.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let request = request();
        LeaseService::new(&store)
            .acquire(&request, &healthy(), 1)
            .unwrap();
        let identity = TransferIdentity::from_acquire_request(&request).unwrap();
        let lock_path = root
            .join("locks/jobs")
            .join(request.material().job_id().to_string())
            .join("transfer/transfer.lock");
        let sentinel = inheritable_sentinel(&fixture.path().join("sentinel"));

        let ready_fifo = fixture.path().join("ready.fifo");
        let release_fifo = fixture.path().join("release.fifo");
        let exit_fifo = fixture.path().join("exit.fifo");
        let intermediary_pid_fifo = fixture.path().join("intermediary-pid.fifo");
        let leaf_pid_fifo = fixture.path().join("leaf-pid.fifo");
        let leaf_arm_fifo = fixture.path().join("leaf-arm.fifo");
        let park_fifo = fixture.path().join("park.fifo");
        for fifo in [
            &ready_fifo,
            &release_fifo,
            &exit_fifo,
            &intermediary_pid_fifo,
            &leaf_pid_fifo,
            &leaf_arm_fifo,
            &park_fifo,
        ] {
            create_fifo(fifo);
        }

        let (accepted_tx, accepted_rx) = mpsc::channel();
        let ready_reader = ready_fifo.clone();
        let acceptor = thread::spawn(move || {
            let result = (|| -> std::io::Result<u8> {
                let mut ready = [0_u8; 1];
                OpenOptions::new()
                    .read(true)
                    .open(ready_reader)?
                    .read_exact(&mut ready)?;
                Ok(ready[0])
            })();
            let _ = accepted_tx.send(result);
        });

        let (exit_tx, exit_rx) = mpsc::channel();
        let exit_reader = exit_fifo.clone();
        let exit_thread = thread::spawn(move || {
            let result = (|| -> std::io::Result<u8> {
                let mut exited = [0_u8; 1];
                OpenOptions::new()
                    .read(true)
                    .open(exit_reader)?
                    .read_exact(&mut exited)?;
                Ok(exited[0])
            })();
            let _ = exit_tx.send(result);
        });

        let (intermediary_pid_tx, intermediary_pid_rx) = mpsc::channel();
        let intermediary_pid_reader = intermediary_pid_fifo.clone();
        let intermediary_pid_thread = thread::spawn(move || {
            let mut buffer = String::new();
            OpenOptions::new()
                .read(true)
                .open(intermediary_pid_reader)
                .unwrap()
                .read_to_string(&mut buffer)
                .unwrap();
            let _ = intermediary_pid_tx.send(buffer.trim().parse::<libc::pid_t>().unwrap());
        });

        let (leaf_pid_tx, leaf_pid_rx) = mpsc::channel();
        let leaf_pid_reader = leaf_pid_fifo.clone();
        let leaf_pid_thread = thread::spawn(move || {
            let mut buffer = String::new();
            OpenOptions::new()
                .read(true)
                .open(leaf_pid_reader)
                .unwrap()
                .read_to_string(&mut buffer)
                .unwrap();
            let _ = leaf_pid_tx.send(buffer.trim().parse::<libc::pid_t>().unwrap());
        });

        let receiver_store = store.clone();
        let receiver_identity = identity.clone();
        let probe_release_fifo = release_fifo.clone();
        let probe_exit_fifo = exit_fifo;
        let probe_leaf_arm_fifo = leaf_arm_fifo.clone();
        let receiver_lock_path = lock_path.clone();
        let sentinel_fd = sentinel.as_raw_fd();
        let (receiver_tx, receiver_rx) = mpsc::channel();
        let receiver = thread::spawn(move || {
            let outcome = HostTransferService::new(&receiver_store).receive(
                &receiver_identity,
                &stock_server_args(),
                &IntermediaryExecProbe {
                    intermediary_pid_fifo,
                    leaf_pid_fifo,
                    leaf_arm_fifo: probe_leaf_arm_fifo,
                    park_fifo,
                    ready_fifo,
                    release_fifo: probe_release_fifo,
                    exit_fifo: probe_exit_fifo,
                    lock_path: receiver_lock_path,
                    sentinel_fd,
                },
            );
            let _ = receiver_tx.send(outcome);
        });

        let intermediary_pid = intermediary_pid_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        intermediary_pid_thread.join().unwrap();
        let mut intermediary_cleanup = IdentityCheckedProcessCleanup::new(intermediary_pid);
        let leaf_pid = leaf_pid_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        leaf_pid_thread.join().unwrap();
        let mut leaf_cleanup = IdentityCheckedProcessCleanup::new(leaf_pid);
        OpenOptions::new()
            .write(true)
            .open(&leaf_arm_fifo)
            .unwrap()
            .write_all(b"A")
            .unwrap();
        let ready = accepted_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        acceptor.join().unwrap();
        assert_eq!(
            ready, b'R',
            "leaf must independently verify and report readiness across two exec hops"
        );

        // Terminate the exact intermediary PID only; the leaf below is a
        // distinct process and is never targeted or signaled.
        intermediary_cleanup
            .signal(libc::SIGKILL)
            .expect("must be able to signal the exact intermediary pid");
        let receiver_outcome = receiver_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("receiver did not reap the killed intermediary within the deadline");
        receiver.join().unwrap();
        intermediary_cleanup.disarm();
        assert!(
            matches!(
                receiver_outcome,
                Err(WorkerError::CommandExit { code: 137 })
            ),
            "intermediary's exec-child slot must report the SIGKILL exit status, got {receiver_outcome:?}"
        );

        assert_nonblocking_lock_contended(&lock_path);
        let resolver_store = HostStore::open(&root).unwrap();
        let resolver_request = request.clone();
        let (resolved_tx, resolved_rx) = mpsc::channel();
        let (entered_tx, entered_rx) = mpsc::channel();
        let resolver = thread::spawn(move || {
            let _ = entered_tx.send(());
            let _ = resolved_tx
                .send(HostTransferService::new(&resolver_store).abandon(&resolver_request, 2));
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            matches!(resolved_rx.try_recv(), Err(mpsc::TryRecvError::Empty)),
            "resolver completed even though direct nonblocking acquisition proved the orphaned leaf held the lock"
        );

        OpenOptions::new()
            .write(true)
            .open(&release_fifo)
            .unwrap()
            .write_all(b"X")
            .unwrap();
        assert_eq!(
            exit_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
            b'E',
            "leaf must acknowledge its explicit release before exiting"
        );
        exit_thread.join().unwrap();
        leaf_cleanup.disarm();
        assert_eq!(
            resolved_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
            AbandonTransferResult::Abandoned
        );
        resolver.join().unwrap();
    }

    #[test]
    fn failed_exec_does_not_change_parent_current_directory() {
        let fixture = tempdir().unwrap();
        let root = fixture.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let request = request();
        LeaseService::new(&store)
            .acquire(&request, &healthy(), 1)
            .unwrap();
        let identity = TransferIdentity::from_acquire_request(&request).unwrap();
        let before = std::env::current_dir().unwrap();
        let error = HostTransferService::new(&store)
            .receive(&identity, &stock_server_args(), &FailedExec)
            .unwrap_err();
        assert_eq!(std::env::current_dir().unwrap(), before);
        assert!(!error.to_string().contains("/definitely/missing"));
    }

    #[test]
    fn production_exec_propagates_child_exit_status() {
        let fixture = tempdir().unwrap();
        let store = HostStore::open(&fixture.path().join("host")).unwrap();
        let request = request();
        LeaseService::new(&store)
            .acquire(&request, &healthy(), 1)
            .unwrap();
        let identity = TransferIdentity::from_acquire_request(&request).unwrap();
        let error = HostTransferService::new(&store)
            .receive(&identity, &stock_server_args(), &Exit23)
            .unwrap_err();
        assert!(matches!(error, WorkerError::CommandExit { code: 23 }));
    }

    #[test]
    fn production_exec_maps_child_signal_to_shell_status() {
        let fixture = tempdir().unwrap();
        let store = HostStore::open(&fixture.path().join("host")).unwrap();
        let request = request();
        LeaseService::new(&store)
            .acquire(&request, &healthy(), 1)
            .unwrap();
        let identity = TransferIdentity::from_acquire_request(&request).unwrap();
        let error = HostTransferService::new(&store)
            .receive(&identity, &stock_server_args(), &SignalTerm)
            .unwrap_err();
        assert!(matches!(error, WorkerError::CommandExit { code: 143 }));
    }
}
