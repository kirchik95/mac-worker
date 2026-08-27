use std::{
    ffi::OsString,
    fmt, fs,
    os::unix::{
        ffi::{OsStrExt, OsStringExt},
        process::CommandExt,
    },
    process::{Command, Stdio},
    time::Duration,
};

use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::{
    config::WorkerEntry,
    error::{ProcessError, ProcessStream, WorkerError},
    host_store::{HostStore, JobDisposition},
    job::{ClientId, JobId, LeaseAcquireRequest, LeaseToken, RequestFingerprint},
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
}

impl HostOperation {
    fn command(self) -> &'static str {
        match self {
            Self::LeaseAcquire => "~/.local/bin/worker host lease-acquire",
        }
    }
}

pub struct SshJsonTransport<'a> {
    runner: &'a dyn ProcessRunner,
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
        invocation.destination.verify_descriptors_cloexec()?;
        invocation.destination.set_as_current_directory()?;
        let mut command = Command::new(RSYNC_PROGRAM);
        command
            .args(invocation.server_args)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        let _inherited_lock = invocation.transfer_guard.inherit_for_exec()?;
        let error = command.exec();
        Err(WorkerError::Io(error))
    }
}

pub struct HostTransferService<'a> {
    store: &'a HostStore,
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
        require_receivable_disposition(self.store, identity)?;
        require_live_identity(self.store, identity)?;
        let transfer = self
            .store
            .transfer_lock_after(&admission, identity.job_id)?;
        admission.validate()?;
        transfer.validate()?;
        require_receivable_disposition(self.store, identity)?;
        require_live_identity(self.store, identity)?;
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
        let identity = TransferIdentity::from_acquire_request(request)?;
        self.store.validate_layout()?;
        let admission = self.store.admission_lock(identity.job_id)?;
        admission.validate()?;
        require_abandonable_disposition(self.store, &identity)?;
        require_live_identity(self.store, &identity)?;
        let transfer = self
            .store
            .transfer_lock_after(&admission, identity.job_id)?;
        admission.validate()?;
        transfer.validate()?;
        let existing = require_abandonable_disposition(self.store, &identity)?;
        require_live_identity(self.store, &identity)?;
        if !existing {
            self.store
                .record_abandoned_after(&admission, request, now)?;
        }
        admission.validate()?;
        transfer.validate()?;
        self.store.remove_incoming_after(
            &admission,
            &transfer,
            identity.job_id,
            identity.lease_token,
        )?;
        Ok(AbandonTransferResult::Abandoned)
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

    pub fn request<Req: Serialize, Res: DeserializeOwned>(
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
        if !result.status.success() {
            let code = if result.status.code() == Some(255) {
                "SSH_UNAVAILABLE"
            } else {
                "HOST_REQUEST_FAILED"
            };
            return Err(transport_error(code, "SSH control request failed"));
        }

        let mut deserializer = serde_json::Deserializer::from_slice(&result.stdout);
        let response = Res::deserialize(&mut deserializer)
            .map_err(|_| transport_error("INVALID_RESPONSE", "host response was invalid"))?;
        deserializer
            .end()
            .map_err(|_| transport_error("INVALID_RESPONSE", "host response was invalid"))?;
        Ok(response)
    }
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

fn require_live_identity(
    store: &HostStore,
    identity: &TransferIdentity,
) -> Result<(), WorkerError> {
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
    Ok(())
}

fn require_receivable_disposition(
    store: &HostStore,
    identity: &TransferIdentity,
) -> Result<(), WorkerError> {
    match store.disposition(identity.job_id)? {
        None => Ok(()),
        Some(JobDisposition::Abandoned { .. }) => Err(host_transfer_error(
            "JOB_ABANDONED",
            "job ID was permanently abandoned",
        )),
        Some(JobDisposition::Accepted { .. }) => Err(host_transfer_error(
            "JOB_ACCEPTED",
            "job ID was already accepted",
        )),
    }
}

fn require_abandonable_disposition(
    store: &HostStore,
    identity: &TransferIdentity,
) -> Result<bool, WorkerError> {
    match store.disposition(identity.job_id)? {
        None => Ok(false),
        Some(JobDisposition::Abandoned {
            job_id,
            client_id,
            request_fingerprint,
            lease_token_sha256,
            ..
        }) => {
            let expected_hash = format!(
                "{:x}",
                Sha256::digest(identity.lease_token.to_string().as_bytes())
            );
            if job_id == identity.job_id
                && client_id == identity.client_id
                && request_fingerprint == identity.request_fingerprint
                && lease_token_sha256 == expected_hash
            {
                Ok(true)
            } else {
                Err(host_transfer_error(
                    "JOB_ID_CONFLICT",
                    "job ID conflicts with durable host state",
                ))
            }
        }
        Some(JobDisposition::Accepted { .. }) => Err(host_transfer_error(
            "JOB_ACCEPTED",
            "job ID was already accepted",
        )),
    }
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
    if lines.len() != 13 || !lines[9].is_empty() || !lines[12].is_empty() {
        return Err(invalid_stats());
    }

    let files = counter(lines[0], "Number of files: ", "")?;
    let files_transferred = counter(lines[1], "Number of files transferred: ", "")?;
    let total_file_size = counter(lines[2], "Total file size: ", " B")?;
    let transferred_size = counter(lines[3], "Total transferred file size: ", " B")?;
    let unmatched = counter(lines[4], "Unmatched data: ", " B")?;
    let matched = counter(lines[5], "Matched data: ", " B")?;
    let _file_list_size = counter(lines[6], "File list size: ", " B")?;
    let total_sent = counter(lines[7], "Total sent: ", " B")?;
    let total_received = counter(lines[8], "Total received: ", " B")?;
    if files_transferred > files
        || transferred_size > total_file_size
        || unmatched.checked_add(matched) != Some(transferred_size)
    {
        return Err(invalid_stats());
    }

    let summary = lines[10]
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

    let total = lines[11]
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
        io::{BufRead, BufReader, Read},
        os::fd::RawFd,
        process::{Child, Stdio},
        sync::{Mutex, mpsc},
        thread,
        time::Duration,
    };

    use tempfile::tempdir;

    use super::*;
    use crate::{
        job::{CommandSpec, RequestFingerprintMaterial},
        lease::{AdmissionFacts, LeaseService},
        protocol::MemoryPressure,
    };

    const CHILD_FD_ENV: &str = "MAC_WORKER_TEST_TRANSFER_LOCK_FD";

    fn request() -> LeaseAcquireRequest {
        LeaseAcquireRequest::new(
            RequestFingerprintMaterial::new(
                JobId::new(uuid::Uuid::from_u128(901)),
                ClientId::new(uuid::Uuid::from_u128(902)),
                LeaseToken::new(uuid::Uuid::from_u128(903)),
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

    struct ExecChildExecutor {
        child: Mutex<Option<Child>>,
    }

    impl RsyncServerExecutor for ExecChildExecutor {
        fn execute(&self, invocation: RsyncServerInvocation<'_>) -> Result<(), WorkerError> {
            let inherited = invocation.transfer_guard.inherit_for_exec()?;
            let descriptor = inherited.raw_fd();
            let mut child = Command::new(std::env::current_exe()?)
                .arg("--exact")
                .arg("transfer::exec_inheritance_tests::exec_child_probe")
                .arg("--nocapture")
                .env(CHILD_FD_ENV, descriptor.to_string())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()?;
            drop(inherited);

            let stdout = child
                .stdout
                .take()
                .ok_or_else(|| WorkerError::Protocol("child stdout was absent".into()))?;
            let (ready_tx, ready_rx) = mpsc::channel();
            thread::spawn(move || {
                let ready = BufReader::new(stdout)
                    .lines()
                    .map_while(Result::ok)
                    .any(|line| line == "TRANSFER_LOCK_READY");
                let _ = ready_tx.send(ready);
            });
            if ready_rx.recv_timeout(Duration::from_secs(5)) != Ok(true) {
                let _ = child.kill();
                let _ = child.wait();
                return Err(WorkerError::Protocol(
                    "exec child did not retain the transfer descriptor".into(),
                ));
            }
            *self.child.lock().unwrap() = Some(child);
            Ok(())
        }
    }

    #[test]
    fn exec_child_probe() {
        let Ok(value) = std::env::var(CHILD_FD_ENV) else {
            return;
        };
        let descriptor: RawFd = value.parse().unwrap();
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        assert_ne!(flags, -1, "transfer lock descriptor did not survive exec");
        assert_eq!(flags & libc::FD_CLOEXEC, 0);
        println!("TRANSFER_LOCK_READY");
        let mut bytes = Vec::new();
        std::io::stdin().read_to_end(&mut bytes).unwrap();
    }

    #[test]
    fn exec_child_holds_transfer_lock_until_killed_and_reaped() {
        let fixture = tempdir().unwrap();
        let root = fixture.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let request = request();
        LeaseService::new(&store)
            .acquire(&request, &healthy(), 1)
            .unwrap();
        let identity = TransferIdentity::from_acquire_request(&request).unwrap();
        let executor = ExecChildExecutor {
            child: Mutex::new(None),
        };
        HostTransferService::new(&store)
            .receive(
                &identity,
                &[
                    "--server".into(),
                    "--delete-before".into(),
                    "-l".into(),
                    "-p".into(),
                    "-D".into(),
                    "-r".into(),
                    "-t".into(),
                    "--dirs".into(),
                    ".".into(),
                    "incoming".into(),
                ],
                &executor,
            )
            .unwrap();

        let resolver_store = HostStore::open(&root).unwrap();
        let resolver_request = request.clone();
        let (resolved_tx, resolved_rx) = mpsc::channel();
        let resolver = thread::spawn(move || {
            resolved_tx
                .send(HostTransferService::new(&resolver_store).abandon(&resolver_request, 2))
                .unwrap();
        });
        assert!(
            resolved_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err()
        );

        let mut child = executor.child.lock().unwrap().take().unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        assert_eq!(
            resolved_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
            AbandonTransferResult::Abandoned
        );
        resolver.join().unwrap();
    }
}
