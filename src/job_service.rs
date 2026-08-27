use std::{
    fmt,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    error::WorkerError,
    host_store::{HostStore, JobDisposition, SupervisorGuard},
    inputs::RelativePath,
    job::{
        ClientId, CommandSpec, JobId, JobMeta, JobState, JobStatus, LeaseRecord, LeaseToken,
        ProcessIdentity, RequestFingerprint, RequestFingerprintMaterial, SubmitRequest,
        SubmitResponse,
    },
    lease::LeaseService,
    remote_snapshot::{RemoteSnapshotService, VerifiedRemoteSnapshot},
    rooted_fs::RootedDir,
};

const EXECUTION_PAYLOAD_VERSION: u32 = 1;
const MAX_HOST_JSON_BYTES: u64 = 1024 * 1024;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecutionPayload {
    version: u32,
    job_id: JobId,
    client_id: ClientId,
    request_fingerprint: RequestFingerprint,
    lease_token: LeaseToken,
    command: CommandSpec,
}

impl fmt::Debug for ExecutionPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionPayload")
            .field("version", &self.version)
            .field("job_id", &self.job_id)
            .field("client_id", &self.client_id)
            .field("request_fingerprint", &self.request_fingerprint)
            .finish_non_exhaustive()
    }
}

impl ExecutionPayload {
    fn new(request: &SubmitRequest) -> Result<Self, WorkerError> {
        request.validate()?;
        let material = request.material();
        let payload = Self {
            version: EXECUTION_PAYLOAD_VERSION,
            job_id: material.job_id(),
            client_id: material.client_id(),
            request_fingerprint: request.request_fingerprint().clone(),
            lease_token: material.lease_token(),
            command: material.command().clone(),
        };
        payload.validate_for(request)?;
        Ok(payload)
    }

    fn validate_for(&self, request: &SubmitRequest) -> Result<(), WorkerError> {
        request.validate()?;
        if self.version != EXECUTION_PAYLOAD_VERSION
            || self.job_id != request.material().job_id()
            || self.client_id != request.material().client_id()
            || self.request_fingerprint != *request.request_fingerprint()
            || self.lease_token != request.material().lease_token()
            || self.command != *request.material().command()
        {
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "execution payload does not match the immutable request",
            ));
        }
        self.command.validate()
    }

    pub(crate) fn validate_for_durable_job(
        &self,
        lease: &LeaseRecord,
        meta: &JobMeta,
    ) -> Result<(), WorkerError> {
        lease.validate()?;
        meta.validate()?;
        let material = RequestFingerprintMaterial::new(
            self.job_id,
            self.client_id,
            self.lease_token,
            meta.worker_name().into(),
            meta.project_id().into(),
            meta.worktree_id().into(),
            meta.manifest_digest().into(),
            meta.relative_working_dir().into(),
            meta.timeout_millis(),
            meta.resource_class().into(),
            self.command.clone(),
        )?;
        let recomputed_fingerprint = material.fingerprint();
        if self.version != EXECUTION_PAYLOAD_VERSION
            || self.job_id != lease.job_id()
            || self.client_id != lease.client_id()
            || self.request_fingerprint != *lease.request_fingerprint()
            || self.lease_token != lease.lease_token()
            || self.job_id != meta.job_id()
            || self.client_id != meta.client_id()
            || self.request_fingerprint != *meta.request_fingerprint()
            || recomputed_fingerprint != self.request_fingerprint
            || self.command.summary()? != *meta.command_summary()
            || self.command.summary()? != *lease.command_summary()
        {
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "execution payload does not match durable job identity",
            ));
        }
        self.command.validate()
    }

    pub(crate) fn command(&self) -> &CommandSpec {
        &self.command
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaunchCandidate {
    identity: ProcessIdentity,
}

impl LaunchCandidate {
    pub fn new(identity: ProcessIdentity) -> Self {
        Self { identity }
    }

    pub fn identity(self) -> ProcessIdentity {
        self.identity
    }
}

pub trait SupervisorLauncher: Send + Sync {
    fn launch(&self, job_id: JobId, guard: SupervisorGuard)
    -> Result<LaunchCandidate, WorkerError>;
}

pub struct JobService<'a> {
    store: &'a HostStore,
    leases: LeaseService<'a>,
    snapshots: RemoteSnapshotService<'a>,
    launcher: &'a dyn SupervisorLauncher,
}

impl<'a> JobService<'a> {
    pub fn new(store: &'a HostStore, launcher: &'a dyn SupervisorLauncher) -> Self {
        Self {
            store,
            leases: LeaseService::new(store),
            snapshots: RemoteSnapshotService::new(store),
            launcher,
        }
    }

    pub fn submit(&self, request: SubmitRequest) -> Result<SubmitResponse, WorkerError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| WorkerError::Protocol("system clock precedes the Unix epoch".into()))?
            .as_millis()
            .try_into()
            .map_err(|_| {
                WorkerError::Protocol("system clock is outside the supported range".into())
            })?;
        self.submit_at(request, now)
    }

    #[doc(hidden)]
    pub fn submit_at(
        &self,
        request: SubmitRequest,
        now: u64,
    ) -> Result<SubmitResponse, WorkerError> {
        request.validate()?;
        if request.material().fingerprint() != *request.request_fingerprint() {
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "request fingerprint does not match its immutable material",
            ));
        }

        let job_id = request.material().job_id();
        let admission = self.store.admission_lock(job_id)?;
        let lease = self
            .leases
            .load_after(&admission, job_id)?
            .ok_or_else(|| protocol_code("LEASE_MISSING", "matching live lease is absent"))?;
        require_exact_lease(&lease, &request)?;

        if let Some(disposition) = self.store.disposition(job_id)? {
            require_matching_accepted(&disposition, &request)?;
            let (_, authoritative) = self.read_exact_job(&request, &lease)?;
            self.store.repair_indexed_publication_after(
                &admission,
                request.material().project_id(),
                request.material().worktree_id(),
                job_id,
            )?;
            if authoritative.supervisor_identity().is_some() {
                reject_prelaunch_terminal(&authoritative)?;
                return Ok(SubmitResponse::Existing {
                    status: authoritative,
                });
            }
            let supervisor = self
                .store
                .supervisor_lock_after(&admission, job_id, false)?;
            return match supervisor {
                Some(guard) => {
                    let verified = self.snapshots.load_verified_for_accepted_after(
                        &admission,
                        &lease,
                        request.request_fingerprint(),
                    )?;
                    drop(admission);
                    self.launch_after_election(job_id, guard, &request, &lease, &verified, false)
                }
                None => {
                    drop(admission);
                    self.wait_for_existing_supervisor(&request, &lease)
                }
            };
        }

        let verified = self.snapshots.load_verified_after(
            &admission,
            &lease,
            request.request_fingerprint(),
        )?;
        if let Some((_meta, status)) = self.read_repairable_final(&request, &lease, &verified)? {
            let published = self.store.reopen_published_after(
                admission,
                request.material().project_id(),
                request.material().worktree_id(),
                job_id,
            )?;
            self.store
                .record_accepted_after(&published, &request, &status, now)?;
            let supervisor =
                self.store
                    .supervisor_lock_after(published.admission_guard(), job_id, false)?;
            drop(published);
            return match supervisor {
                Some(guard) => {
                    self.launch_after_election(job_id, guard, &request, &lease, &verified, false)
                }
                None => self.wait_for_existing_supervisor(&request, &lease),
            };
        }
        let mut staged = self.store.begin_job_after(
            admission,
            request.material().project_id(),
            request.material().worktree_id(),
            job_id,
        )?;
        let workspace_receipt = self
            .snapshots
            .materialize_workspace(&verified, &mut staged)?;
        let staged_workspace = staged
            .rooted_dir()
            .open_child_directory(&relative("workspace")?, false)?;
        self.snapshots
            .validate_materialized_workspace(&verified, &staged_workspace)?;
        let meta = JobMeta::new(
            request.material(),
            request.request_fingerprint().clone(),
            now,
        )?;
        let initial_status = JobStatus::accepted(now)?;
        materialize_control_files(
            self.store,
            staged.rooted_dir(),
            &request,
            &meta,
            &initial_status,
        )?;
        let published = staged.publish_complete_with_commit_hooks(
            workspace_receipt,
            || {
                consume_job_fault(
                    self.store,
                    crate::host_store::HostStoreWritePoint::AfterJobRename,
                )
            },
            || {
                consume_job_fault(
                    self.store,
                    crate::host_store::HostStoreWritePoint::AfterJobPublish,
                )
            },
        )?;
        self.store
            .record_accepted_after(&published, &request, &initial_status, now)?;
        let supervisor =
            self.store
                .supervisor_lock_after(published.admission_guard(), job_id, false)?;
        drop(published);
        match supervisor {
            Some(guard) => {
                self.launch_after_election(job_id, guard, &request, &lease, &verified, true)
            }
            None => Ok(SubmitResponse::Accepted {
                meta: Box::new(meta),
                status: initial_status,
            }),
        }
    }

    fn launch_after_election(
        &self,
        job_id: JobId,
        guard: SupervisorGuard,
        request: &SubmitRequest,
        lease: &LeaseRecord,
        snapshot: &VerifiedRemoteSnapshot,
        newly_accepted: bool,
    ) -> Result<SubmitResponse, WorkerError> {
        guard.validate()?;
        let (meta, status) = self.read_exact_job(request, lease)?;
        if status.supervisor_identity().is_some() {
            reject_prelaunch_terminal(&status)?;
            drop(guard);
            return Ok(SubmitResponse::Existing { status });
        }
        if status.state() != JobState::Accepted || status.child_identity().is_some() {
            drop(guard);
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "supervisor election found a non-prelaunch job state",
            ));
        }
        let material = request.material();
        let job = self.store.open_directory(
            &format!(
                "jobs/{}/{}/{}",
                material.project_id(),
                material.worktree_id(),
                material.job_id()
            ),
            false,
        )?;
        validate_indexed_prelaunch_job(&job, request).map_err(|_| {
            protocol_code(
                "JOB_ID_CONFLICT",
                "indexed final job is not an exact complete prelaunch job",
            )
        })?;
        let workspace = job.open_child_directory(&relative("workspace")?, false)?;
        self.snapshots
            .validate_materialized_workspace(snapshot, &workspace)
            .map_err(|_| {
                protocol_code(
                    "JOB_ID_CONFLICT",
                    "indexed final workspace does not match the verified snapshot",
                )
            })?;
        self.launch_and_observe(job_id, guard, request, lease, newly_accepted, meta)
    }

    fn launch_and_observe(
        &self,
        job_id: JobId,
        guard: SupervisorGuard,
        request: &SubmitRequest,
        lease: &LeaseRecord,
        newly_accepted: bool,
        meta: JobMeta,
    ) -> Result<SubmitResponse, WorkerError> {
        let candidate = self.launcher.launch(job_id, guard)?;
        let deadline = Instant::now() + Duration::from_secs(5);
        let status = loop {
            let (_, status) = self.read_exact_job(request, lease)?;
            match status.supervisor_identity() {
                Some(identity) if identity != candidate.identity() => {
                    return Err(protocol_code(
                        "SUPERVISOR_HANDSHAKE",
                        "durable supervisor identity belongs to another candidate",
                    ));
                }
                Some(_) => {
                    reject_prelaunch_terminal(&status)?;
                    let durable_acceptance =
                        matches!(status.state(), JobState::Accepted | JobState::Running)
                            || (status.state().is_terminal() && status.child_identity().is_some());
                    if !durable_acceptance {
                        return Err(protocol_code(
                            "SUPERVISOR_PRELAUNCH_FAILED",
                            "supervisor recorded a terminal failure before child launch",
                        ));
                    }
                    break status;
                }
                None if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                None => {
                    return Err(protocol_code(
                        "SUPERVISOR_HANDSHAKE_TIMEOUT",
                        "supervisor identity was not durably observed within five seconds",
                    ));
                }
            }
        };
        if newly_accepted {
            Ok(SubmitResponse::Accepted {
                meta: Box::new(meta),
                status,
            })
        } else {
            Ok(SubmitResponse::Existing { status })
        }
    }

    fn wait_for_existing_supervisor(
        &self,
        request: &SubmitRequest,
        lease: &LeaseRecord,
    ) -> Result<SubmitResponse, WorkerError> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let (_, status) = self.read_exact_job(request, lease)?;
            if status.supervisor_identity().is_some() || status.state().is_terminal() {
                reject_prelaunch_terminal(&status)?;
                return Ok(SubmitResponse::Existing { status });
            }
            if Instant::now() >= deadline {
                let job_id = request.material().job_id();
                let admission = self.store.admission_lock(job_id)?;
                let authoritative_lease =
                    self.leases.load_after(&admission, job_id)?.ok_or_else(|| {
                        protocol_code("LEASE_MISSING", "matching live lease is absent")
                    })?;
                require_exact_lease(&authoritative_lease, request)?;
                let (_, status) = self.read_exact_job(request, &authoritative_lease)?;
                self.store.repair_indexed_publication_after(
                    &admission,
                    request.material().project_id(),
                    request.material().worktree_id(),
                    job_id,
                )?;
                if status.supervisor_identity().is_some() || status.state().is_terminal() {
                    drop(admission);
                    reject_prelaunch_terminal(&status)?;
                    return Ok(SubmitResponse::Existing { status });
                }
                if status.state() != JobState::Accepted || status.child_identity().is_some() {
                    drop(admission);
                    return Err(protocol_code(
                        "JOB_ID_CONFLICT",
                        "supervisor retry found a non-prelaunch job state",
                    ));
                }
                let supervisor = self
                    .store
                    .supervisor_lock_after(&admission, job_id, false)?;
                return match supervisor {
                    Some(guard) => {
                        let verified = self.snapshots.load_verified_for_accepted_after(
                            &admission,
                            &authoritative_lease,
                            request.request_fingerprint(),
                        )?;
                        drop(admission);
                        self.launch_after_election(
                            job_id,
                            guard,
                            request,
                            &authoritative_lease,
                            &verified,
                            false,
                        )
                    }
                    None => {
                        drop(admission);
                        Ok(SubmitResponse::Existing { status })
                    }
                };
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn read_exact_job(
        &self,
        request: &SubmitRequest,
        lease: &LeaseRecord,
    ) -> Result<(JobMeta, JobStatus), WorkerError> {
        let material = request.material();
        let job = self.store.open_directory(
            &format!(
                "jobs/{}/{}/{}",
                material.project_id(),
                material.worktree_id(),
                material.job_id()
            ),
            false,
        )?;
        let meta: JobMeta = read_canonical_json(&job, "meta.json")?;
        require_exact_meta(&meta, request, lease)?;
        let status: JobStatus = read_mutable_canonical_json(&job, "status.json")?;
        status.validate()?;
        Ok((meta, status))
    }

    fn read_repairable_final(
        &self,
        request: &SubmitRequest,
        lease: &LeaseRecord,
        snapshot: &VerifiedRemoteSnapshot,
    ) -> Result<Option<(JobMeta, JobStatus)>, WorkerError> {
        let material = request.material();
        let job = match self.store.open_directory(
            &format!(
                "jobs/{}/{}/{}",
                material.project_id(),
                material.worktree_id(),
                material.job_id()
            ),
            false,
        ) {
            Ok(job) => job,
            Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let validated = (|| {
            let meta: JobMeta = read_canonical_json(&job, "meta.json")?;
            require_exact_meta(&meta, request, lease)?;
            let status: JobStatus = read_canonical_json(&job, "status.json")?;
            status.validate()?;
            if status.state() != JobState::Accepted
                || status.supervisor_identity().is_some()
                || status.child_identity().is_some()
            {
                return Err(WorkerError::Protocol(
                    "unindexed final job is not an exact prelaunch Accepted job".into(),
                ));
            }
            let payload: ExecutionPayload = read_canonical_json(&job, "execution.json")?;
            payload.validate_for(request)?;
            let names = job
                .list_names()?
                .into_iter()
                .map(|name| {
                    String::from_utf8(name).map_err(|_| {
                        WorkerError::Protocol("final job contains a non-UTF-8 entry".into())
                    })
                })
                .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
            let expected = [
                "execution.json",
                "home",
                "meta.json",
                "status.json",
                "stderr.log",
                "stdout.log",
                "tmp",
                "workspace",
            ]
            .into_iter()
            .map(String::from)
            .collect::<std::collections::BTreeSet<_>>();
            if names != expected {
                return Err(WorkerError::Protocol(
                    "unindexed final job has an unsafe top-level layout".into(),
                ));
            }
            for directory in ["home", "tmp", "workspace", "workspace/tree"] {
                job.open_child_directory(&relative(directory)?, false)?
                    .sync_root()?;
            }
            let workspace = job.open_child_directory(&relative("workspace")?, false)?;
            self.snapshots
                .validate_materialized_workspace(snapshot, &workspace)?;
            for log in ["stdout.log", "stderr.log"] {
                let file = job.open_private_append(log)?;
                let length = job.validate_private_append_binding(log, &file)?;
                if length != 0 {
                    return Err(WorkerError::Protocol(
                        "unindexed prelaunch job log is not empty".into(),
                    ));
                }
            }
            Ok((meta, status))
        })()
        .map_err(|_| {
            protocol_code(
                "JOB_ID_CONFLICT",
                "unindexed final job evidence is incomplete or conflicting",
            )
        })?;
        Ok(Some(validated))
    }
}

fn materialize_control_files(
    store: &HostStore,
    root: &RootedDir,
    request: &SubmitRequest,
    meta: &JobMeta,
    status: &JobStatus,
) -> Result<(), WorkerError> {
    for (name, point) in [
        (
            "home",
            crate::host_store::HostStoreWritePoint::AfterJobHomeSync,
        ),
        (
            "tmp",
            crate::host_store::HostStoreWritePoint::AfterJobTmpSync,
        ),
    ] {
        let directory = root.open_child_directory(&relative(name)?, true)?;
        directory.sync_root()?;
        consume_job_fault(store, point)?;
    }
    for (name, after_write, after_sync) in [
        (
            "stdout.log",
            crate::host_store::HostStoreWritePoint::AfterJobStdoutWrite,
            crate::host_store::HostStoreWritePoint::AfterJobStdoutFileSync,
        ),
        (
            "stderr.log",
            crate::host_store::HostStoreWritePoint::AfterJobStderrWrite,
            crate::host_store::HostStoreWritePoint::AfterJobStderrFileSync,
        ),
    ] {
        let file = root.write_new_private_file(name, &[])?;
        consume_job_fault(store, after_write)?;
        file.sync_all()?;
        consume_job_fault(store, after_sync)?;
    }
    write_new_canonical_json(
        store,
        root,
        "meta.json",
        meta,
        crate::host_store::HostStoreWritePoint::AfterJobMetaWrite,
        crate::host_store::HostStoreWritePoint::AfterJobMetaFileSync,
    )?;
    write_new_canonical_json(
        store,
        root,
        "status.json",
        status,
        crate::host_store::HostStoreWritePoint::AfterJobStatusWrite,
        crate::host_store::HostStoreWritePoint::AfterJobStatusFileSync,
    )?;
    write_new_canonical_json(
        store,
        root,
        "execution.json",
        &ExecutionPayload::new(request)?,
        crate::host_store::HostStoreWritePoint::AfterJobExecutionWrite,
        crate::host_store::HostStoreWritePoint::AfterJobExecutionFileSync,
    )?;
    root.sync_root()?;
    consume_job_fault(
        store,
        crate::host_store::HostStoreWritePoint::AfterJobStagingDirectorySync,
    )?;
    Ok(())
}

fn validate_indexed_prelaunch_job(
    job: &RootedDir,
    request: &SubmitRequest,
) -> Result<(), WorkerError> {
    let payload: ExecutionPayload = read_canonical_json(job, "execution.json")?;
    payload.validate_for(request)?;
    let names = job
        .list_names()?
        .into_iter()
        .map(|name| {
            String::from_utf8(name)
                .map_err(|_| WorkerError::Protocol("final job has a non-UTF-8 entry".into()))
        })
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    let expected = [
        "execution.json",
        "home",
        "meta.json",
        "status.json",
        "stderr.log",
        "stdout.log",
        "tmp",
        "workspace",
    ]
    .into_iter()
    .map(String::from)
    .collect::<std::collections::BTreeSet<_>>();
    if names != expected {
        return Err(WorkerError::Protocol(
            "indexed prelaunch job has an unsafe top-level layout".into(),
        ));
    }
    for directory in ["home", "tmp", "workspace", "workspace/tree"] {
        job.open_child_directory(&relative(directory)?, false)?;
    }
    for log in ["stdout.log", "stderr.log"] {
        let file = job.open_private_append(log)?;
        if job.validate_private_append_binding(log, &file)? != 0 {
            return Err(WorkerError::Protocol(
                "indexed prelaunch job log is not empty".into(),
            ));
        }
    }
    Ok(())
}

fn write_new_canonical_json<T: Serialize>(
    store: &HostStore,
    directory: &RootedDir,
    name: &str,
    value: &T,
    after_write: crate::host_store::HostStoreWritePoint,
    after_sync: crate::host_store::HostStoreWritePoint,
) -> Result<(), WorkerError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| WorkerError::Protocol("host JSON serialization failed".into()))?;
    let file = directory.write_new_private_file(name, &bytes)?;
    consume_job_fault(store, after_write)?;
    file.sync_all()?;
    consume_job_fault(store, after_sync)?;
    Ok(())
}

fn consume_job_fault(
    store: &HostStore,
    point: crate::host_store::HostStoreWritePoint,
) -> std::io::Result<()> {
    if store.consume_fault(point) {
        Err(std::io::Error::other("injected host-store write failure"))
    } else {
        Ok(())
    }
}

fn read_canonical_json<T>(directory: &RootedDir, name: &str) -> Result<T, WorkerError>
where
    T: DeserializeOwned + Serialize,
{
    let bytes = directory.read_private_regular(name, MAX_HOST_JSON_BYTES)?;
    let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
    let value = T::deserialize(&mut deserializer)
        .map_err(|_| WorkerError::Protocol("canonical job JSON is invalid".into()))?;
    deserializer
        .end()
        .map_err(|_| WorkerError::Protocol("canonical job JSON has trailing data".into()))?;
    let canonical = serde_json::to_vec(&value)
        .map_err(|_| WorkerError::Protocol("host JSON serialization failed".into()))?;
    if canonical != bytes {
        return Err(WorkerError::Protocol(
            "canonical job JSON has a non-canonical encoding".into(),
        ));
    }
    Ok(value)
}

fn read_mutable_canonical_json<T>(directory: &RootedDir, name: &str) -> Result<T, WorkerError>
where
    T: DeserializeOwned + Serialize,
{
    for attempt in 0..32 {
        match read_canonical_json(directory, name) {
            Err(WorkerError::Io(error)) if error.raw_os_error() == Some(libc::ESTALE) => {
                if attempt == 31 {
                    return Err(WorkerError::Io(error));
                }
                std::thread::yield_now();
            }
            result => return result,
        }
    }
    unreachable!("bounded mutable JSON retry always returns")
}

fn require_exact_lease(lease: &LeaseRecord, request: &SubmitRequest) -> Result<(), WorkerError> {
    lease.validate()?;
    let material = request.material();
    if lease.job_id() != material.job_id()
        || lease.client_id() != material.client_id()
        || lease.lease_token() != material.lease_token()
        || lease.request_fingerprint() != request.request_fingerprint()
        || lease.worker_name() != material.worker_name()
        || lease.project_id() != material.project_id()
        || lease.worktree_id() != material.worktree_id()
        || lease.manifest_digest() != material.manifest_digest()
        || lease.timeout_millis() != material.timeout_millis()
        || lease.resource_class() != material.resource_class()
        || lease.command_summary() != &material.command().summary()?
    {
        return Err(protocol_code(
            "JOB_ID_CONFLICT",
            "live lease does not match the immutable submit request",
        ));
    }
    Ok(())
}

fn require_matching_accepted(
    disposition: &JobDisposition,
    request: &SubmitRequest,
) -> Result<JobStatus, WorkerError> {
    match disposition {
        JobDisposition::Accepted {
            job_id,
            client_id,
            project_id,
            worktree_id,
            request_fingerprint,
            status,
            ..
        } if *job_id == request.material().job_id()
            && *client_id == request.material().client_id()
            && project_id == request.material().project_id()
            && worktree_id == request.material().worktree_id()
            && request_fingerprint == request.request_fingerprint() =>
        {
            status.validate()?;
            Ok(status.clone())
        }
        JobDisposition::Abandoned { .. }
            if disposition
                .is_exact_abandonment_for(request.material(), request.request_fingerprint()) =>
        {
            Err(protocol_code(
                "JOB_ABANDONED",
                "job ID was permanently abandoned",
            ))
        }
        _ => Err(protocol_code(
            "JOB_ID_CONFLICT",
            "job ID belongs to another immutable request",
        )),
    }
}

fn require_exact_meta(
    meta: &JobMeta,
    request: &SubmitRequest,
    lease: &LeaseRecord,
) -> Result<(), WorkerError> {
    meta.validate()?;
    let material = request.material();
    if meta.job_id() != material.job_id()
        || meta.client_id() != material.client_id()
        || meta.request_fingerprint() != request.request_fingerprint()
        || meta.worker_name() != material.worker_name()
        || meta.project_id() != material.project_id()
        || meta.worktree_id() != material.worktree_id()
        || meta.manifest_digest() != material.manifest_digest()
        || meta.relative_working_dir() != material.relative_working_dir()
        || meta.timeout_millis() != material.timeout_millis()
        || meta.resource_class() != material.resource_class()
        || meta.command_summary() != &material.command().summary()?
        || lease.job_id() != meta.job_id()
        || lease.client_id() != meta.client_id()
        || lease.request_fingerprint() != meta.request_fingerprint()
    {
        return Err(protocol_code(
            "JOB_ID_CONFLICT",
            "canonical job metadata does not match the immutable request",
        ));
    }
    Ok(())
}

fn relative(path: &str) -> Result<RelativePath, WorkerError> {
    RelativePath::parse(path.as_bytes())
        .map_err(|_| WorkerError::Protocol("host-owned relative directory name is invalid".into()))
}

fn protocol_code(code: &'static str, message: &str) -> WorkerError {
    WorkerError::Protocol(format!("{code}: {message}"))
}

fn is_prelaunch_terminal(status: &JobStatus) -> bool {
    status.state().is_terminal()
        && (status.child_identity().is_none()
            || matches!(
                status.error_code(),
                Some(
                    "PAYLOAD_ERASURE_FAILED"
                        | "CHILD_GATE_INVALID"
                        | "CHILD_EXEC_INVALID"
                        | "CHILD_GO_FAILED"
                        | "CHILD_PRE_GO_FAILED"
                        | "EXEC_FAILED"
                )
            ))
}

fn reject_prelaunch_terminal(status: &JobStatus) -> Result<(), WorkerError> {
    if is_prelaunch_terminal(status) {
        return Err(protocol_code(
            "SUPERVISOR_PRELAUNCH_FAILED",
            "supervisor recorded a terminal failure before command execution",
        ));
    }
    Ok(())
}
