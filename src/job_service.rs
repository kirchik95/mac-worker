use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    error::WorkerError,
    host_store::{
        AdmissionGuard, HostStore, HostStoreWritePoint, JobDisposition, ResolutionIdentity,
        SupervisorGuard,
    },
    inputs::RelativePath,
    job::{
        ClientId, CommandSpec, JobId, JobMeta, JobState, JobStatus, LeaseRecord, LeaseToken,
        LogChunk, LogStream, ProcessIdentity, RequestFingerprint, RequestFingerprintMaterial,
        ResolveOrAbandonRequest, ResolveOrAbandonResponse, StatusResponse, SubmitRequest,
        SubmitResponse,
    },
    lease::LeaseService,
    remote_snapshot::{RemoteSnapshotService, VerifiedRemoteSnapshot},
    rooted_fs::{RootedDir, is_log_offset_beyond_eof},
    supervisor::{ReconciliationRuntime, SystemReconciliationRuntime, reconcile_orphan_processes},
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
            meta.created_at_millis(),
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

    fn validate_for_resolution(&self, identity: &ResolutionIdentity) -> Result<(), WorkerError> {
        let material = RequestFingerprintMaterial::new(
            self.job_id,
            self.client_id,
            self.lease_token,
            identity.created_at_millis(),
            identity.worker_name().into(),
            identity.project_id().into(),
            identity.worktree_id().into(),
            identity.manifest_digest().into(),
            identity.relative_working_dir().into(),
            identity.timeout_millis(),
            identity.resource_class().into(),
            self.command.clone(),
        )?;
        if self.version == EXECUTION_PAYLOAD_VERSION
            && self.job_id == identity.job_id()
            && self.client_id == identity.client_id()
            && self.lease_token == identity.lease_token()
            && self.request_fingerprint == *identity.request_fingerprint()
            && material.fingerprint() == self.request_fingerprint
            && self.command.summary()? == *identity.command_summary()
        {
            Ok(())
        } else {
            Err(job_id_conflict(
                "incomplete execution payload belongs to another immutable request",
            ))
        }
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

struct AuthoritativeJob {
    response: StatusResponse,
    directory: RootedDir,
}

enum ResolutionFinal {
    Complete,
    Incomplete(RootedDir),
}

impl AuthoritativeJob {
    fn new(directory: RootedDir, meta: JobMeta, status: JobStatus) -> Result<Self, WorkerError> {
        let response = StatusResponse::new(meta, status)
            .map_err(|_| job_state_invalid("job status response is inconsistent"))?;
        Ok(Self {
            response,
            directory,
        })
    }

    fn from_response(directory: RootedDir, response: StatusResponse) -> Self {
        Self {
            response,
            directory,
        }
    }

    fn into_response(self) -> StatusResponse {
        self.response
    }
}

pub struct JobService<'a> {
    store: &'a HostStore,
    leases: LeaseService<'a>,
    snapshots: RemoteSnapshotService<'a>,
    launcher: &'a dyn SupervisorLauncher,
    reconciliation: Arc<dyn ReconciliationRuntime>,
    log_read_boundary: Option<Arc<dyn Fn() + Send + Sync>>,
    resolution_before_transfer: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl<'a> JobService<'a> {
    pub fn new(store: &'a HostStore, launcher: &'a dyn SupervisorLauncher) -> Self {
        Self {
            store,
            leases: LeaseService::new(store),
            snapshots: RemoteSnapshotService::new(store),
            launcher,
            reconciliation: Arc::new(SystemReconciliationRuntime::new()),
            log_read_boundary: None,
            resolution_before_transfer: None,
        }
    }

    #[doc(hidden)]
    pub fn new_with_reconciliation(
        store: &'a HostStore,
        launcher: &'a dyn SupervisorLauncher,
        reconciliation: Arc<dyn ReconciliationRuntime>,
    ) -> Self {
        Self {
            store,
            leases: LeaseService::new(store),
            snapshots: RemoteSnapshotService::new(store),
            launcher,
            reconciliation,
            log_read_boundary: None,
            resolution_before_transfer: None,
        }
    }

    #[doc(hidden)]
    pub fn new_with_log_read_boundary(
        store: &'a HostStore,
        launcher: &'a dyn SupervisorLauncher,
        log_read_boundary: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        Self {
            store,
            leases: LeaseService::new(store),
            snapshots: RemoteSnapshotService::new(store),
            launcher,
            reconciliation: Arc::new(SystemReconciliationRuntime::new()),
            log_read_boundary: Some(log_read_boundary),
            resolution_before_transfer: None,
        }
    }

    #[doc(hidden)]
    pub fn new_with_resolution_before_transfer(
        store: &'a HostStore,
        launcher: &'a dyn SupervisorLauncher,
        resolution_before_transfer: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        Self {
            store,
            leases: LeaseService::new(store),
            snapshots: RemoteSnapshotService::new(store),
            launcher,
            reconciliation: Arc::new(SystemReconciliationRuntime::new()),
            log_read_boundary: None,
            resolution_before_transfer: Some(resolution_before_transfer),
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

    pub fn status(&self, job_id: JobId) -> Result<StatusResponse, WorkerError> {
        self.authoritative_job_with_supervisor_ensure(job_id, true)
            .map(AuthoritativeJob::into_response)
    }

    pub fn read_log(
        &self,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    ) -> Result<LogChunk, WorkerError> {
        let authoritative = self.authoritative_job_with_supervisor_ensure(job_id, true)?;
        if let Some(boundary) = &self.log_read_boundary {
            boundary();
        }
        let name = match stream {
            LogStream::Stdout => "stdout.log",
            LogStream::Stderr => "stderr.log",
        };
        let bytes = authoritative
            .directory
            .read_private_regular_chunk(
                name,
                offset,
                usize::try_from(limit).expect("u32 fits in usize on supported hosts"),
            )
            .map_err(|error| {
                if is_log_offset_beyond_eof(&error) {
                    protocol_code(
                        "LOG_OFFSET_BEYOND_EOF",
                        "requested log offset is beyond EOF",
                    )
                } else {
                    WorkerError::Io(error)
                }
            })?;
        LogChunk::new(stream, offset, bytes)
    }

    pub fn reconcile_job(&self, job_id: JobId) -> Result<StatusResponse, WorkerError> {
        self.authoritative_job_with_supervisor_ensure(job_id, true)
            .map(AuthoritativeJob::into_response)
    }

    pub fn resolve_or_abandon(
        &self,
        request: ResolveOrAbandonRequest,
    ) -> Result<ResolveOrAbandonResponse, WorkerError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| WorkerError::Protocol("system clock precedes the Unix epoch".into()))?
            .as_millis()
            .try_into()
            .map_err(|_| {
                WorkerError::Protocol("system clock is outside the supported range".into())
            })?;
        self.resolve_or_abandon_at(request, now, true)
    }

    pub(crate) fn resolve_or_abandon_at(
        &self,
        request: ResolveOrAbandonRequest,
        now: u64,
        ensure_supervisor: bool,
    ) -> Result<ResolveOrAbandonResponse, WorkerError> {
        request.validate()?;
        let identity = ResolutionIdentity::from_request(&request)?;
        self.store.validate_layout()?;
        let admission = self.store.admission_lock(identity.job_id())?;
        admission.validate_for(identity.job_id())?;

        let disposition = self.store.disposition(identity.job_id())?;
        if let Some(disposition @ JobDisposition::Accepted { .. }) = disposition {
            self.require_resolution_accepted_after(&admission, &identity, &disposition)?;
            let authoritative =
                self.status_from_accepted_after(admission, disposition, ensure_supervisor)?;
            require_resolution_response(&identity, &authoritative.response)?;
            return ResolveOrAbandonResponse::accepted(authoritative.into_response());
        }
        if let Some(disposition @ JobDisposition::Abandoned { .. }) = disposition.as_ref()
            && !identity.matches_disposition(disposition)
        {
            return Err(job_id_conflict(
                "abandonment belongs to another immutable request",
            ));
        }

        let live = self.resolution_live_after(&admission, &identity)?;
        if matches!(
            self.classify_resolution_final(&identity)?,
            Some(ResolutionFinal::Complete)
        ) {
            if disposition.is_some() || live.is_none() {
                return Err(job_id_conflict(
                    "complete accepted evidence conflicts with abandonment state",
                ));
            }
            let authoritative = self.status_without_disposition_after(
                admission,
                identity.job_id(),
                ensure_supervisor,
            )?;
            require_resolution_response(&identity, &authoritative.response)?;
            return ResolveOrAbandonResponse::accepted(authoritative.into_response());
        }

        if let Some(boundary) = &self.resolution_before_transfer {
            boundary();
        }
        let transfer = self
            .store
            .transfer_lock_after(&admission, identity.job_id())?;
        admission.validate_for(identity.job_id())?;
        transfer.validate()?;

        let disposition = self.store.disposition(identity.job_id())?;
        if let Some(disposition @ JobDisposition::Accepted { .. }) = disposition {
            self.require_resolution_accepted_after(&admission, &identity, &disposition)?;
            drop(transfer);
            let authoritative =
                self.status_from_accepted_after(admission, disposition, ensure_supervisor)?;
            require_resolution_response(&identity, &authoritative.response)?;
            return ResolveOrAbandonResponse::accepted(authoritative.into_response());
        }
        if let Some(disposition @ JobDisposition::Abandoned { .. }) = disposition.as_ref()
            && !identity.matches_disposition(disposition)
        {
            return Err(job_id_conflict(
                "abandonment belongs to another immutable request",
            ));
        }
        let live_after_wait = self.resolution_live_after(&admission, &identity)?;
        let live = match (live, live_after_wait) {
            (Some(before), Some(after)) if before == after => Some(after),
            (None, Some(after)) => Some(after),
            (Some(_), None) => {
                return Err(job_id_conflict(
                    "exact live lease disappeared before abandonment selection",
                ));
            }
            (Some(_), Some(_)) => {
                return Err(job_id_conflict(
                    "live lease changed before abandonment selection",
                ));
            }
            (None, None) => None,
        };

        let final_job = match self.classify_resolution_final(&identity)? {
            Some(ResolutionFinal::Complete) => {
                if disposition.is_some() || live.is_none() {
                    return Err(job_id_conflict(
                        "complete accepted evidence conflicts with abandonment state",
                    ));
                }
                drop(transfer);
                let authoritative = self.status_without_disposition_after(
                    admission,
                    identity.job_id(),
                    ensure_supervisor,
                )?;
                require_resolution_response(&identity, &authoritative.response)?;
                return ResolveOrAbandonResponse::accepted(authoritative.into_response());
            }
            Some(ResolutionFinal::Incomplete(job)) => Some(job),
            None => None,
        };

        self.store
            .validate_resolution_cleanup_marker_after(&admission, &transfer, &identity)?;
        self.snapshots
            .validate_resolution_evidence_after(&admission, &transfer, &identity)?;
        if disposition.is_none()
            && let Err(error) = self
                .store
                .record_resolution_abandoned_after(&admission, &identity, now)
        {
            return match self.store.disposition(identity.job_id()) {
                Ok(Some(disposition)) if identity.matches_disposition(&disposition) => {
                    ResolveOrAbandonResponse::cleanup_pending("MUTABLE_CLEANUP_FAILED")
                }
                _ => Err(error),
            };
        }

        let cleanup = (|| {
            self.store.remove_resolution_execution_after(
                &admission,
                &transfer,
                &identity,
                final_job.as_ref(),
            )?;
            self.store.remove_incoming_after(
                &admission,
                &transfer,
                identity.job_id(),
                identity.lease_token(),
            )?;
            self.snapshots
                .remove_resolution_evidence_after(&admission, &transfer, &identity)?;
            self.store.remove_resolution_job_mutable_after(
                &admission,
                &transfer,
                &identity,
                final_job.as_ref(),
            )?;
            self.store
                .remove_resolution_staging_after(&admission, &transfer, &identity)?;
            self.snapshots
                .resolution_evidence_absent_after(&admission, &transfer, &identity)?;
            self.store.record_resolution_cleanup_after(
                &admission,
                &transfer,
                &identity,
                final_job.as_ref(),
            )
        })();
        if cleanup.is_err() {
            return ResolveOrAbandonResponse::cleanup_pending("MUTABLE_CLEANUP_FAILED");
        }
        drop(transfer);
        drop(admission);

        let receipt = match self
            .store
            .resolution_cleanup_receipt(&identity, live.as_ref())
        {
            Ok(receipt) => receipt,
            Err(_) => {
                return ResolveOrAbandonResponse::cleanup_pending("MUTABLE_CLEANUP_FAILED");
            }
        };
        if let (Some(lease), Some(receipt)) = (live, receipt) {
            if self
                .store
                .consume_fault(HostStoreWritePoint::BeforeResolutionLeaseRelease)
            {
                return ResolveOrAbandonResponse::cleanup_pending("LEASE_RELEASE_FAILED");
            }
            if self.leases.release_after_cleanup(&lease, &receipt).is_err() {
                return ResolveOrAbandonResponse::cleanup_pending("LEASE_RELEASE_FAILED");
            }
        }
        Ok(ResolveOrAbandonResponse::abandoned())
    }

    fn resolution_live_after(
        &self,
        admission: &AdmissionGuard,
        identity: &ResolutionIdentity,
    ) -> Result<Option<LeaseRecord>, WorkerError> {
        let Some(live) = self.leases.load_after(admission, identity.job_id())? else {
            return Ok(None);
        };
        if live.job_id() == identity.job_id() {
            if identity.matches_lease(&live) {
                Ok(Some(live))
            } else {
                Err(job_id_conflict(
                    "live lease belongs to another immutable request",
                ))
            }
        } else {
            Ok(None)
        }
    }

    fn classify_resolution_final(
        &self,
        identity: &ResolutionIdentity,
    ) -> Result<Option<ResolutionFinal>, WorkerError> {
        let job = match self.store.open_directory(
            &format!(
                "jobs/{}/{}/{}",
                identity.project_id(),
                identity.worktree_id(),
                identity.job_id()
            ),
            false,
        ) {
            Ok(job) => job,
            Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(_) => return Err(job_id_conflict("final job evidence is unsafe")),
        };
        let names = job
            .list_names()
            .map_err(|_| job_id_conflict("final job layout is unsafe"))?
            .into_iter()
            .map(|name| {
                String::from_utf8(name)
                    .map_err(|_| job_id_conflict("final job has a non-UTF-8 entry"))
            })
            .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
        let allowed = [
            ".mac-worker-rooted-fs",
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
        if !names.is_subset(&allowed) {
            return Err(job_id_conflict("final job contains unrelated evidence"));
        }

        let meta = if job.entry_exists("meta.json")? {
            let meta: JobMeta = read_canonical_json(&job, "meta.json")
                .map_err(|_| job_id_conflict("incomplete final metadata is invalid"))?;
            require_resolution_meta(identity, &meta)?;
            Some(meta)
        } else {
            None
        };
        let payload_present = job.entry_exists("execution.json")?;
        if payload_present {
            let payload: ExecutionPayload = read_canonical_json(&job, "execution.json")
                .map_err(|_| job_id_conflict("incomplete execution payload is invalid"))?;
            payload.validate_for_resolution(identity)?;
        }
        let status_present = job.entry_exists("status.json")?;
        if status_present {
            let status: JobStatus = read_canonical_json(&job, "status.json")
                .map_err(|_| job_id_conflict("incomplete final status is invalid"))?;
            let meta = meta.as_ref().ok_or_else(|| {
                job_id_conflict("status without exact metadata cannot prove final ownership")
            })?;
            let expected = JobStatus::accepted(meta.created_at_millis())
                .map_err(|_| job_id_conflict("incomplete acceptance timestamp is invalid"))?;
            if status != expected {
                return Err(job_id_conflict(
                    "started or terminal final evidence cannot be abandoned",
                ));
            }
        }
        if meta.is_none() && !payload_present {
            return Err(job_id_conflict(
                "final job has no positive immutable ownership proof",
            ));
        }
        for directory in ["home", "tmp", "workspace"] {
            if job.entry_exists(directory)? {
                job.open_child_directory(&relative(directory)?, false)
                    .map_err(|_| job_id_conflict("final mutable directory is unsafe"))?;
            }
        }
        for log in ["stdout.log", "stderr.log"] {
            if job.entry_exists(log)? {
                let file = job
                    .open_private_append(log)
                    .map_err(|_| job_id_conflict("final log is unsafe"))?;
                if job
                    .validate_private_append_binding(log, &file)
                    .map_err(|_| job_id_conflict("final log binding is unsafe"))?
                    != 0
                {
                    return Err(job_id_conflict(
                        "nonempty unindexed logs are possible launch evidence",
                    ));
                }
            }
        }
        let complete = [
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
        Ok(Some(if names == complete {
            ResolutionFinal::Complete
        } else {
            ResolutionFinal::Incomplete(job)
        }))
    }

    fn require_resolution_accepted_after(
        &self,
        admission: &AdmissionGuard,
        identity: &ResolutionIdentity,
        disposition: &JobDisposition,
    ) -> Result<(), WorkerError> {
        admission.validate_for(identity.job_id())?;
        if !identity.matches_disposition(disposition) {
            return Err(job_id_conflict(
                "accepted disposition belongs to another immutable request",
            ));
        }
        let job = self
            .store
            .open_directory(
                &format!(
                    "jobs/{}/{}/{}",
                    identity.project_id(),
                    identity.worktree_id(),
                    identity.job_id()
                ),
                false,
            )
            .map_err(|_| job_id_conflict("accepted final job is absent or unsafe"))?;
        let meta: JobMeta = read_canonical_json(&job, "meta.json")
            .map_err(|_| job_id_conflict("accepted metadata is invalid"))?;
        require_resolution_meta(identity, &meta)
    }

    fn authoritative_job_with_supervisor_ensure(
        &self,
        job_id: JobId,
        ensure_supervisor: bool,
    ) -> Result<AuthoritativeJob, WorkerError> {
        let admission = self.store.admission_lock(job_id)?;
        match self.store.disposition(job_id).map_err(|error| {
            if matches!(&error, WorkerError::Protocol(message) if message.starts_with("JOB_ID_CONFLICT:")) {
                error
            } else {
                job_state_invalid("job disposition is invalid")
            }
        })? {
            Some(JobDisposition::Abandoned { .. }) => Err(protocol_code(
                "JOB_ABANDONED",
                "job ID was permanently abandoned",
            )),
            Some(disposition @ JobDisposition::Accepted { .. }) => {
                self.status_from_accepted_after(admission, disposition, ensure_supervisor)
            }
            None => {
                self.status_without_disposition_after(admission, job_id, ensure_supervisor)
            }
        }
    }

    fn status_without_disposition_after(
        &self,
        admission: AdmissionGuard,
        job_id: JobId,
        ensure_supervisor: bool,
    ) -> Result<AuthoritativeJob, WorkerError> {
        admission.validate_for(job_id)?;
        let Some(lease) = self
            .leases
            .load_after(&admission, job_id)
            .map_err(|_| job_state_invalid("live lease is invalid"))?
        else {
            return Err(protocol_code("JOB_NOT_FOUND", "job ID is not indexed"));
        };
        if lease.job_id() != job_id {
            return Err(protocol_code("JOB_NOT_FOUND", "job ID is not indexed"));
        }
        let job = match self.store.open_directory(
            &format!(
                "jobs/{}/{}/{}",
                lease.project_id(),
                lease.worktree_id(),
                lease.job_id()
            ),
            false,
        ) {
            Ok(job) => job,
            Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(protocol_code("JOB_NOT_FOUND", "job ID is not indexed"));
            }
            Err(_) => {
                return Err(job_state_invalid("unindexed final job directory is unsafe"));
            }
        };
        let meta: JobMeta = read_canonical_json(&job, "meta.json")
            .map_err(|_| job_state_invalid("unindexed final metadata is invalid"))?;
        require_meta_matches_live_lease(&meta, &lease)?;
        let status: JobStatus = read_canonical_json(&job, "status.json")
            .map_err(|_| job_state_invalid("unindexed final status is invalid"))?;
        let expected = JobStatus::accepted(meta.created_at_millis())
            .map_err(|_| job_state_invalid("unindexed acceptance timestamp is invalid"))?;
        if status != expected {
            return Err(job_state_invalid(
                "unindexed final job is not an exact prelaunch Accepted job",
            ));
        }
        validate_empty_prelaunch_private_directories(&job)?;
        let request = reconstruct_submit_request(&job, &meta, &lease)?;
        let verified = self.snapshots.load_verified_after(
            &admission,
            &lease,
            request.request_fingerprint(),
        )?;
        let Some((validated_meta, validated_status)) =
            self.read_repairable_final(&request, &lease, &verified)?
        else {
            return Err(protocol_code("JOB_NOT_FOUND", "job ID is not indexed"));
        };
        if validated_meta != meta || validated_status != status {
            return Err(job_state_invalid(
                "unindexed final job changed during validation",
            ));
        }
        let published = self.store.reopen_published_after(
            admission,
            lease.project_id(),
            lease.worktree_id(),
            job_id,
        )?;
        self.store.record_accepted_after(
            &published,
            &request,
            &status,
            meta.created_at_millis(),
        )?;
        drop(published);
        self.authoritative_job_with_supervisor_ensure(job_id, ensure_supervisor)
    }

    fn status_from_accepted_after(
        &self,
        admission: AdmissionGuard,
        disposition: JobDisposition,
        ensure_supervisor: bool,
    ) -> Result<AuthoritativeJob, WorkerError> {
        let JobDisposition::Accepted {
            job_id,
            client_id,
            project_id,
            worktree_id,
            request_fingerprint,
            status: initial_status,
            ..
        } = disposition
        else {
            unreachable!("accepted status loader receives an Accepted disposition")
        };
        admission.validate_for(job_id)?;
        let job = self
            .store
            .open_directory(&format!("jobs/{project_id}/{worktree_id}/{job_id}"), false)
            .map_err(|_| job_state_invalid("accepted job directory is absent or unsafe"))?;
        let meta: JobMeta = read_canonical_json(&job, "meta.json")
            .map_err(|_| job_state_invalid("canonical job metadata is invalid"))?;
        if meta.job_id() != job_id
            || meta.client_id() != client_id
            || meta.project_id() != project_id
            || meta.worktree_id() != worktree_id
            || meta.request_fingerprint() != &request_fingerprint
        {
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "accepted index does not match canonical job metadata",
            ));
        }
        let expected_initial = JobStatus::accepted(meta.created_at_millis())
            .map_err(|_| job_state_invalid("accepted job timestamp is invalid"))?;
        if initial_status != expected_initial {
            return Err(job_state_invalid(
                "accepted index does not contain the immutable initial status",
            ));
        }
        let status: JobStatus = read_mutable_canonical_json(&job, "status.json")
            .map_err(|_| job_state_invalid("canonical mutable job status is invalid"))?;
        validate_queryable_status(&meta, &status)?;

        let loaded_lease = self
            .leases
            .load_after(&admission, job_id)
            .map_err(|_| job_state_invalid("live lease is invalid"))?;
        let lease = if status.state().is_terminal() {
            validate_terminal_log_lengths(&job, &status)?;
            match loaded_lease {
                Some(lease) if lease.job_id() == job_id => {
                    require_meta_matches_live_lease(&meta, &lease)?;
                    Some(lease)
                }
                Some(_) | None => None,
            }
        } else {
            let lease = loaded_lease
                .ok_or_else(|| job_state_invalid("nonterminal job has no live lease"))?;
            require_meta_matches_live_lease(&meta, &lease)?;
            Some(lease)
        };
        let response = StatusResponse::new(meta.clone(), status.clone())
            .map_err(|_| job_state_invalid("job status response is inconsistent"))?;

        if !ensure_supervisor {
            drop(admission);
            return Ok(AuthoritativeJob::from_response(job, response));
        }

        let identityless_accepted = status.state() == JobState::Accepted
            && status.supervisor_identity().is_none()
            && status.child_identity().is_none();
        let Some(lease) = lease else {
            drop(admission);
            return Ok(AuthoritativeJob::from_response(job, response));
        };
        let supervisor = self
            .store
            .supervisor_lock_after(&admission, job_id, false)?;
        let Some(mut supervisor) = supervisor else {
            drop(admission);
            return Ok(AuthoritativeJob::from_response(job, response));
        };
        let (current_bytes, current): (Vec<u8>, JobStatus) =
            read_mutable_canonical_json_with_bytes(&job, "status.json")
                .map_err(|_| job_state_invalid("canonical mutable job status is invalid"))?;
        validate_queryable_status(&meta, &current)?;
        if current != status {
            drop(supervisor);
            drop(admission);
            return self.authoritative_job_with_supervisor_ensure(job_id, false);
        }
        if !identityless_accepted {
            if current.state().is_terminal() {
                drop(admission);
                drop(supervisor);
                self.cleanup_and_release_reconciled(&lease, &job, &current)?;
                return AuthoritativeJob::new(job, meta, current);
            }
            let supervisor_identity = current.supervisor_identity().ok_or_else(|| {
                job_state_invalid("nonterminal supervised job has no supervisor identity")
            })?;
            drop(admission);
            reconcile_orphan_processes(
                self.reconciliation.as_ref(),
                supervisor_identity,
                current.child_identity(),
            )?;
            if job.entry_exists("execution.json")? {
                job.remove_owned_regular("execution.json")?;
            }
            let stdout = job.open_private_append("stdout.log")?;
            let stderr = job.open_private_append("stderr.log")?;
            stdout.sync_all()?;
            stderr.sync_all()?;
            let stdout_length = job.validate_private_append_binding("stdout.log", &stdout)?;
            let stderr_length = job.validate_private_append_binding("stderr.log", &stderr)?;
            let lost = current.into_infrastructure_terminal(
                JobState::Lost,
                reconciliation_timestamp(current.updated_at_millis())?,
                stdout_length,
                stderr_length,
                "SUPERVISOR_LOST".into(),
            )?;
            self.store.replace_job_status_after(
                &mut supervisor,
                &lease,
                &job,
                &current_bytes,
                &current,
                &lost,
            )?;
            drop(supervisor);
            self.cleanup_and_release_reconciled(&lease, &job, &lost)?;
            return AuthoritativeJob::new(job, meta, lost);
        }

        validate_empty_prelaunch_private_directories(&job)?;
        let request = reconstruct_submit_request(&job, &meta, &lease)?;
        let verified = self.snapshots.load_verified_for_accepted_after(
            &admission,
            &lease,
            meta.request_fingerprint(),
        )?;
        drop(admission);
        match self.launch_after_election(job_id, supervisor, &request, &lease, &verified, false) {
            Ok(_) => self.authoritative_job_with_supervisor_ensure(job_id, false),
            Err(error) if matches!(&error, WorkerError::Protocol(message) if message.starts_with("SUPERVISOR_PRELAUNCH_FAILED:")) => {
                match self.authoritative_job_with_supervisor_ensure(job_id, false) {
                    Ok(authoritative) if authoritative.response.status() != &status => {
                        Ok(authoritative)
                    }
                    Ok(_) => Err(error),
                    Err(authority_error) => Err(authority_error),
                }
            }
            Err(error) => Err(error),
        }
    }

    fn cleanup_and_release_reconciled(
        &self,
        lease: &LeaseRecord,
        job: &RootedDir,
        terminal: &JobStatus,
    ) -> Result<(), WorkerError> {
        match self.store.cleanup_job_owned(lease) {
            Ok(receipt) => {
                if let Err(error) = self.leases.release_after_cleanup(lease, &receipt) {
                    let _ = self.enrich_reconciliation_cleanup_error(
                        lease,
                        job,
                        terminal,
                        "LEASE_RELEASE_FAILED",
                    );
                    return Err(error);
                }
                Ok(())
            }
            Err(error) => {
                let _ = self.enrich_reconciliation_cleanup_error(
                    lease,
                    job,
                    terminal,
                    "MUTABLE_CLEANUP_FAILED",
                );
                Err(error)
            }
        }
    }

    fn enrich_reconciliation_cleanup_error(
        &self,
        lease: &LeaseRecord,
        job: &RootedDir,
        terminal: &JobStatus,
        code: &str,
    ) -> Result<(), WorkerError> {
        if terminal.cleanup_error_code().is_some() {
            return Ok(());
        }
        let admission = self.store.admission_lock(lease.job_id())?;
        let supervisor = self
            .store
            .supervisor_lock_after(&admission, lease.job_id(), false)?;
        let Some(mut supervisor) = supervisor else {
            return Ok(());
        };
        drop(admission);
        let (bytes, current): (Vec<u8>, JobStatus) =
            read_mutable_canonical_json_with_bytes(job, "status.json")?;
        if current != *terminal {
            return Err(protocol_code(
                "STATUS_CHANGED",
                "terminal status changed before cleanup enrichment",
            ));
        }
        let enriched = current.with_cleanup_error(
            code.into(),
            reconciliation_timestamp(current.updated_at_millis())?,
        )?;
        self.store.replace_job_status_after(
            &mut supervisor,
            lease,
            job,
            &bytes,
            &current,
            &enriched,
        )
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
        if let Some(disposition) = self.store.disposition(job_id)? {
            require_matching_accepted(&disposition, &request)?;
            if let Some(lease) = self
                .leases
                .load_after(&admission, job_id)?
                .filter(|lease| lease.job_id() == job_id)
            {
                require_exact_lease(&lease, &request)?;
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
                        self.launch_after_election(
                            job_id, guard, &request, &lease, &verified, false,
                        )
                    }
                    None => {
                        drop(admission);
                        self.wait_for_existing_supervisor(&request, &lease)
                    }
                };
            }
            let resolution_request = ResolveOrAbandonRequest::from_submit_request(&request)?;
            let identity = ResolutionIdentity::from_request(&resolution_request)?;
            drop(admission);
            let authoritative = self.status(job_id)?;
            require_resolution_response(&identity, &authoritative)?;
            reject_prelaunch_terminal(authoritative.status())?;
            return Ok(SubmitResponse::Existing {
                status: authoritative.status().clone(),
            });
        }
        let lease = self
            .leases
            .load_after(&admission, job_id)?
            .filter(|lease| lease.job_id() == job_id)
            .ok_or_else(|| protocol_code("LEASE_MISSING", "matching live lease is absent"))?;
        require_exact_lease(&lease, &request)?;

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
        let meta = JobMeta::new(request.material(), request.request_fingerprint().clone())?;
        let initial_status = JobStatus::accepted(request.material().created_at_millis())?;
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

fn read_mutable_canonical_json_with_bytes<T>(
    directory: &RootedDir,
    name: &str,
) -> Result<(Vec<u8>, T), WorkerError>
where
    T: DeserializeOwned + Serialize,
{
    for attempt in 0..32 {
        let bytes = match directory.read_private_regular(name, MAX_HOST_JSON_BYTES) {
            Err(error) if error.raw_os_error() == Some(libc::ESTALE) => {
                if attempt == 31 {
                    return Err(WorkerError::Io(error));
                }
                std::thread::yield_now();
                continue;
            }
            result => result?,
        };
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
        return Ok((bytes, value));
    }
    unreachable!("bounded mutable JSON retry always returns")
}

fn reconciliation_timestamp(previous: u64) -> Result<u64, WorkerError> {
    let now: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| WorkerError::Protocol("system clock precedes the Unix epoch".into()))?
        .as_millis()
        .try_into()
        .map_err(|_| WorkerError::Protocol("system clock is outside the supported range".into()))?;
    Ok(now.max(previous))
}

fn validate_queryable_status(meta: &JobMeta, status: &JobStatus) -> Result<(), WorkerError> {
    meta.validate()
        .map_err(|_| job_state_invalid("canonical job metadata is invalid"))?;
    status
        .validate()
        .map_err(|_| job_state_invalid("canonical mutable job status is invalid"))?;
    if matches!(status.state(), JobState::Uploading | JobState::Verified) {
        return Err(job_state_invalid(
            "an accepted job cannot return to a pre-acceptance state",
        ));
    }
    if status.state() == JobState::Accepted
        && status.supervisor_identity().is_none()
        && status.child_identity().is_none()
        && status
            != &JobStatus::accepted(meta.created_at_millis())
                .map_err(|_| job_state_invalid("accepted job timestamp is invalid"))?
    {
        return Err(job_state_invalid(
            "identityless accepted status must equal durable initial acceptance",
        ));
    }
    match status.state() {
        JobState::Succeeded | JobState::Failed | JobState::Cancelled | JobState::TimedOut
            if status.supervisor_identity().is_none() || status.child_identity().is_none() =>
        {
            return Err(job_state_invalid(
                "command terminal status requires both durable process identities",
            ));
        }
        JobState::Lost if status.supervisor_identity().is_none() => {
            return Err(job_state_invalid(
                "lost accepted job requires a durable supervisor identity",
            ));
        }
        _ => {}
    }
    match status.state() {
        JobState::Accepted | JobState::Running | JobState::Succeeded | JobState::Failed
            if status.error_code().is_some() =>
        {
            return Err(job_state_invalid(
                "ordinary lifecycle status cannot carry an infrastructure error",
            ));
        }
        JobState::Cancelled | JobState::TimedOut | JobState::Lost
            if status.error_code().is_none() =>
        {
            return Err(job_state_invalid(
                "infrastructure terminal status requires an error code",
            ));
        }
        _ => {}
    }
    if status.updated_at_millis() < meta.created_at_millis() {
        return Err(job_state_invalid(
            "mutable job status predates durable acceptance",
        ));
    }
    Ok(())
}

fn require_meta_matches_live_lease(meta: &JobMeta, lease: &LeaseRecord) -> Result<(), WorkerError> {
    lease
        .validate()
        .map_err(|_| job_state_invalid("live lease is invalid"))?;
    if meta.job_id() != lease.job_id()
        || meta.client_id() != lease.client_id()
        || meta.request_fingerprint() != lease.request_fingerprint()
        || meta.worker_name() != lease.worker_name()
        || meta.project_id() != lease.project_id()
        || meta.worktree_id() != lease.worktree_id()
        || meta.manifest_digest() != lease.manifest_digest()
        || meta.timeout_millis() != lease.timeout_millis()
        || meta.resource_class() != lease.resource_class()
        || meta.command_summary() != lease.command_summary()
    {
        return Err(protocol_code(
            "JOB_ID_CONFLICT",
            "nonterminal job metadata does not match the live lease",
        ));
    }
    Ok(())
}

fn validate_terminal_log_lengths(job: &RootedDir, status: &JobStatus) -> Result<(), WorkerError> {
    let expected = [
        status
            .final_stdout_bytes()
            .expect("validated terminal status has a stdout length"),
        status
            .final_stderr_bytes()
            .expect("validated terminal status has a stderr length"),
    ];
    for (name, expected_length) in ["stdout.log", "stderr.log"].into_iter().zip(expected) {
        let file = job
            .open_private_append(name)
            .map_err(|_| job_state_invalid("terminal log is absent or unsafe"))?;
        let actual_length = job
            .validate_private_append_binding(name, &file)
            .map_err(|_| job_state_invalid("terminal log binding is invalid"))?;
        if actual_length != expected_length {
            return Err(job_state_invalid(
                "terminal log length does not match durable status",
            ));
        }
    }
    Ok(())
}

fn reconstruct_submit_request(
    job: &RootedDir,
    meta: &JobMeta,
    lease: &LeaseRecord,
) -> Result<SubmitRequest, WorkerError> {
    let payload: ExecutionPayload = read_canonical_json(job, "execution.json")
        .map_err(|_| job_state_invalid("identityless accepted payload is invalid"))?;
    payload
        .validate_for_durable_job(lease, meta)
        .map_err(|_| protocol_code("JOB_ID_CONFLICT", "execution payload identity differs"))?;
    let material = RequestFingerprintMaterial::new(
        meta.job_id(),
        meta.client_id(),
        lease.lease_token(),
        meta.created_at_millis(),
        meta.worker_name().into(),
        meta.project_id().into(),
        meta.worktree_id().into(),
        meta.manifest_digest().into(),
        meta.relative_working_dir().into(),
        meta.timeout_millis(),
        meta.resource_class().into(),
        payload.command().clone(),
    )
    .map_err(|_| job_state_invalid("identityless accepted intent is invalid"))?;
    let request = SubmitRequest::new(material);
    if request.request_fingerprint() != meta.request_fingerprint() {
        return Err(protocol_code(
            "JOB_ID_CONFLICT",
            "identityless accepted intent has a different fingerprint",
        ));
    }
    require_exact_lease(lease, &request)?;
    require_exact_meta(meta, &request, lease)?;
    Ok(request)
}

fn validate_empty_prelaunch_private_directories(job: &RootedDir) -> Result<(), WorkerError> {
    for name in ["home", "tmp"] {
        let directory = job
            .open_child_directory(&relative(name)?, false)
            .map_err(|_| job_state_invalid("prelaunch private directory is absent or unsafe"))?;
        if !directory
            .list_names()
            .map_err(|_| job_state_invalid("prelaunch private directory is unsafe"))?
            .is_empty()
        {
            return Err(job_state_invalid(
                "prelaunch private directory is not empty",
            ));
        }
    }
    Ok(())
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
        || meta.created_at_millis() != material.created_at_millis()
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

fn job_id_conflict(message: &str) -> WorkerError {
    protocol_code("JOB_ID_CONFLICT", message)
}

fn require_resolution_meta(
    identity: &ResolutionIdentity,
    meta: &JobMeta,
) -> Result<(), WorkerError> {
    meta.validate()?;
    if meta.job_id() == identity.job_id()
        && meta.client_id() == identity.client_id()
        && meta.request_fingerprint() == identity.request_fingerprint()
        && meta.worker_name() == identity.worker_name()
        && meta.project_id() == identity.project_id()
        && meta.worktree_id() == identity.worktree_id()
        && meta.manifest_digest() == identity.manifest_digest()
        && meta.created_at_millis() == identity.created_at_millis()
        && meta.relative_working_dir() == identity.relative_working_dir()
        && meta.timeout_millis() == identity.timeout_millis()
        && meta.resource_class() == identity.resource_class()
        && meta.command_summary() == identity.command_summary()
    {
        Ok(())
    } else {
        Err(job_id_conflict(
            "accepted metadata belongs to another immutable request",
        ))
    }
}

fn require_resolution_response(
    identity: &ResolutionIdentity,
    response: &StatusResponse,
) -> Result<(), WorkerError> {
    response.validate()?;
    require_resolution_meta(identity, response.meta())
}

fn job_state_invalid(message: &str) -> WorkerError {
    protocol_code("JOB_STATE_INVALID", message)
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
