use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::{
    error::WorkerError,
    host_store::{
        AdmissionGuard, HostStore, HostStoreWritePoint, JobDisposition, ResolutionIdentity,
        SupervisorGuard,
    },
    inputs::RelativePath,
    job::{
        CancelRequest, CancelResponse, ClientId, CommandSpec, JobId, JobMeta, JobState, JobStatus,
        LeaseRecord, LeaseToken, LogChunk, LogStream, ProcessIdentity, RequestFingerprint,
        RequestFingerprintMaterial, ResolveOrAbandonRequest, ResolveOrAbandonResponse,
        StatusResponse, SubmitRequest, SubmitResponse,
    },
    lease::LeaseService,
    process::SystemProcessRunner,
    remote_snapshot::{RemoteSnapshotService, VerifiedRemoteSnapshot},
    rooted_fs::{RootedDir, is_log_offset_beyond_eof},
    supervisor::{
        ProcessObservation, ReconciliationRuntime, SystemReconciliationRuntime,
        reconcile_orphan_processes, terminate_exact_recorded_group,
    },
    task::PublishMode,
    task_store::{TaskCancelRequest, TaskStore},
    turn::{
        TaskTurnRequest, TaskTurnResponse, TerminalPath, TurnReceipt, TurnSection, TurnTerminalHook,
    },
};

pub(crate) const EXECUTION_PAYLOAD_VERSION: u32 = 2;
const MAX_HOST_JSON_BYTES: u64 = 1024 * 1024;
const CANCEL_SUPERVISOR_HANDOFF_DEADLINE: Duration = Duration::from_secs(5);
const CANCEL_SUPERVISOR_HANDOFF_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecutionPayload {
    version: u32,
    job_id: JobId,
    client_id: ClientId,
    request_fingerprint: RequestFingerprint,
    lease_token: LeaseToken,
    command: CommandSpec,
    turn: Option<TurnSection>,
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
        Self::new_with_turn(request, None)
    }

    pub(crate) fn new_with_turn(
        request: &SubmitRequest,
        turn: Option<TurnSection>,
    ) -> Result<Self, WorkerError> {
        request.validate()?;
        let material = request.material();
        let payload = Self {
            version: EXECUTION_PAYLOAD_VERSION,
            job_id: material.job_id(),
            client_id: material.client_id(),
            request_fingerprint: request.request_fingerprint().clone(),
            lease_token: material.lease_token(),
            command: material.command().clone(),
            turn,
        };
        if let Some(turn) = &payload.turn {
            payload.validate_for_turn(request, turn)?;
        } else {
            payload.validate_for(request)?;
        }
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
            || self.turn.is_some()
        {
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "execution payload does not match the immutable request",
            ));
        }
        self.command.validate()
    }

    pub(crate) fn validate_for_turn(
        &self,
        request: &SubmitRequest,
        section: &TurnSection,
    ) -> Result<(), WorkerError> {
        self.validate_for_common(request)?;
        if self.turn.as_ref() != Some(section)
            || section.turn().digest() != request.material().manifest_digest()
        {
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "execution payload turn does not match the immutable request",
            ));
        }
        Ok(())
    }

    fn validate_for_common(&self, request: &SubmitRequest) -> Result<(), WorkerError> {
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
        if let Some(turn) = &self.turn
            && (turn.project_id() != meta.project_id()
                || turn.turn().digest() != meta.manifest_digest())
        {
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "execution payload turn does not match durable job metadata",
            ));
        }
        self.command.validate()
    }

    pub(crate) fn command(&self) -> &CommandSpec {
        &self.command
    }

    pub(crate) fn turn(&self) -> Option<&TurnSection> {
        self.turn.as_ref()
    }

    fn validate_for_resolution(&self, identity: &ResolutionIdentity) -> Result<(), WorkerError> {
        let turn_matches = self.turn.as_ref().is_none_or(|section| {
            section.project_id() == identity.project_id()
                && section.turn().digest() == identity.manifest_digest()
        });
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
            && turn_matches
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

struct CancelAuthority {
    lease: Option<LeaseRecord>,
    directory: RootedDir,
    meta: JobMeta,
    status_bytes: Vec<u8>,
    status: JobStatus,
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

    pub fn submit_turn(&self, request: TaskTurnRequest) -> Result<TaskTurnResponse, WorkerError> {
        request.validate()?;
        let submit = request.submit().clone();
        let turn = request.turn().clone();
        let material = submit.material();
        if material.project_id().is_empty() {
            return Err(protocol_code("REQUEST_CONFLICT", "turn project is invalid"));
        }
        let job_id = material.job_id();
        let task_store = TaskStore::new(self.store, &SystemProcessRunner);
        let admission = self.store.admission_lock(job_id)?;
        // A completed first turn is still an idempotent replay. Check the
        // immutable job index before asking the task store for a pending
        // turn, because the latter is deliberately no longer pending after
        // publication.
        if let Some(disposition) = self.store.disposition(job_id)? {
            require_matching_accepted(&disposition, &submit)?;
            let replay_meta = task_store.load_meta(material.project_id(), turn.task_id())?;
            validate_turn_origin(&replay_meta, request.origin_url())?;
            let task_status = task_store.load_status(material.project_id(), turn.task_id())?;
            if let Some(lease) = self
                .leases
                .load_after(&admission, job_id)?
                .filter(|lease| lease.job_id() == job_id)
            {
                require_exact_lease(&lease, &submit)?;
                let (meta, status) = self.read_exact_job(&submit, &lease)?;
                if status.state().is_terminal() {
                    let authoritative =
                        self.status_from_accepted_after(admission, disposition, true, false)?;
                    return Ok(TaskTurnResponse::new(
                        SubmitResponse::Existing {
                            status: authoritative.response.status().clone(),
                        },
                        task_store.load_status(material.project_id(), turn.task_id())?,
                    ));
                }
                if status.state() == JobState::Accepted
                    && status.supervisor_identity().is_none()
                    && status.child_identity().is_none()
                {
                    let job = self.store.open_directory(
                        &format!(
                            "jobs/{}/{}/{}",
                            material.project_id(),
                            material.worktree_id(),
                            material.job_id()
                        ),
                        false,
                    )?;
                    let payload: ExecutionPayload = read_canonical_json(&job, "execution.json")?;
                    let section = payload.turn().cloned().ok_or_else(|| {
                        protocol_code(
                            "JOB_ID_CONFLICT",
                            "accepted turn job has no durable turn section",
                        )
                    })?;
                    payload.validate_for_turn(&submit, &section)?;
                    if section.turn() != &turn {
                        return Err(protocol_code(
                            "REQUEST_CONFLICT",
                            "accepted turn job belongs to another turn",
                        ));
                    }
                    let supervisor = self
                        .store
                        .supervisor_lock_after(&admission, job_id, false)?;
                    drop(admission);
                    let submit_response = match supervisor {
                        Some(guard) => {
                            self.launch_and_observe(job_id, guard, &submit, &lease, false, meta)?
                        }
                        None => SubmitResponse::Existing { status },
                    };
                    return Ok(TaskTurnResponse::new(
                        submit_response,
                        task_store.load_status(material.project_id(), turn.task_id())?,
                    ));
                }
                drop(admission);
                return Ok(TaskTurnResponse::new(
                    SubmitResponse::Existing { status },
                    task_status,
                ));
            }
            let authoritative =
                self.status_from_accepted_after(admission, disposition, true, false)?;
            return Ok(TaskTurnResponse::new(
                SubmitResponse::Existing {
                    status: authoritative.response.status().clone(),
                },
                task_store.load_status(material.project_id(), turn.task_id())?,
            ));
        }

        let lease = self
            .leases
            .load_after(&admission, job_id)?
            .filter(|lease| lease.job_id() == job_id)
            .ok_or_else(|| protocol_code("LEASE_MISSING", "matching task lease is absent"))?;
        require_exact_lease(&lease, &submit)?;

        let (meta, prepared_status) = {
            let current = task_store.load_status(material.project_id(), turn.task_id())?;
            let meta = task_store.load_meta(material.project_id(), turn.task_id())?;
            let prepared = current.state() == crate::task::TaskState::Active
                && current.turns().last().is_some_and(|pending| {
                    pending.turn_id() == job_id && pending.terminal().is_none()
                });
            if prepared {
                (meta, current)
            } else if turn.resume() && current.state() == crate::task::TaskState::Open {
                task_store.prepare_resume(
                    material.project_id(),
                    turn.task_id(),
                    job_id,
                    turn.turn_number(),
                    lease.worker_name(),
                    turn.base_oid(),
                )?
            } else {
                return Err(protocol_code(
                    "TASK_BUSY",
                    "task does not have the requested prepared active turn",
                ));
            }
        };
        let pending_turn = prepared_status
            .turns()
            .last()
            .ok_or_else(|| protocol_code("REQUEST_CONFLICT", "prepared turn history is absent"))?;
        if prepared_status.worker() != Some(lease.worker_name()) {
            return Err(protocol_code(
                "TASK_TURN_CONFLICT",
                "turn worker does not match the task's selected worker",
            ));
        }
        if material.worktree_id() != meta.worktree_id()
            || turn.turn_number() != pending_turn.turn_number()
            || turn.agent() != meta.agent()
            || turn.model() != meta.model()
            || turn.policy() != meta.policy()
            || turn.limits() != &meta.limits().turn
            || turn.env_profile() != meta.env_profile()
            || turn.base_oid() != prepared_status.head_oid().unwrap_or(meta.base_oid())
        {
            return Err(protocol_code(
                "REQUEST_CONFLICT",
                "turn does not match the prepared task",
            ));
        }
        if turn.resume()
            && task_store
                .session(material.project_id(), turn.task_id())?
                .is_none()
        {
            return Err(protocol_code(
                "SESSION_UNBOUND",
                "resumed turn requires a bound agent session",
            ));
        }
        validate_turn_origin(&meta, request.origin_url())?;
        let section = TurnSection::new_with_origin(
            turn.clone(),
            meta.project_id(),
            meta.git_identity().clone(),
            request.origin_url().map(str::to_owned),
        )?
        .with_herdr_reporter(request.herdr_reporter());
        if let Some((job_meta, initial_status)) =
            self.read_repairable_turn_final(&submit, &lease, &section)?
        {
            let published = self.store.reopen_published_after(
                admission,
                material.project_id(),
                material.worktree_id(),
                job_id,
            )?;
            self.store.record_accepted_after(
                &published,
                &submit,
                &initial_status,
                job_meta.created_at_millis(),
            )?;
            if turn.agent() == crate::agent::AgentKind::Claude && !turn.resume() {
                task_store.bind_session(
                    material.project_id(),
                    turn.task_id(),
                    crate::task_store::SessionBinding::new(
                        crate::agent::AgentKind::Claude,
                        turn.session_seed().to_string(),
                        now_millis()?,
                    )?,
                )?;
            }
            let supervisor =
                self.store
                    .supervisor_lock_after(published.admission_guard(), job_id, false)?;
            drop(published);
            let submit_response = match supervisor {
                Some(guard) => {
                    self.launch_and_observe(job_id, guard, &submit, &lease, true, job_meta)?
                }
                None => SubmitResponse::Accepted {
                    meta: Box::new(job_meta),
                    status: initial_status,
                },
            };
            return Ok(TaskTurnResponse::new(
                submit_response,
                task_store.load_status(material.project_id(), turn.task_id())?,
            ));
        }
        let staged = self.store.begin_job_after(
            admission,
            material.project_id(),
            material.worktree_id(),
            job_id,
        )?;
        let receipt = TurnReceipt::new(
            job_id,
            turn.task_id(),
            turn.base_oid().clone(),
            staged.receipt_nonce(),
        );
        let job_meta = JobMeta::new(
            &submit.material().clone(),
            submit.request_fingerprint().clone(),
        )?;
        let initial_status = JobStatus::accepted(material.created_at_millis())?;
        materialize_turn_control_files(
            self.store,
            staged.rooted_dir(),
            &submit,
            request.prompt(),
            &job_meta,
            &initial_status,
            &section,
        )?;
        let published = staged.publish_complete_with_commit_hooks(
            receipt,
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
            .record_accepted_after(&published, &submit, &initial_status, now_millis()?)?;
        if turn.agent() == crate::agent::AgentKind::Claude && !turn.resume() {
            task_store.bind_session(
                material.project_id(),
                turn.task_id(),
                crate::task_store::SessionBinding::new(
                    crate::agent::AgentKind::Claude,
                    turn.session_seed().to_string(),
                    now_millis()?,
                )?,
            )?;
        }
        let supervisor =
            self.store
                .supervisor_lock_after(published.admission_guard(), job_id, false)?;
        drop(published);
        let submit_response = match supervisor {
            Some(guard) => {
                self.launch_and_observe(job_id, guard, &submit, &lease, true, job_meta)?
            }
            None => SubmitResponse::Accepted {
                meta: Box::new(job_meta),
                status: initial_status,
            },
        };
        Ok(TaskTurnResponse::new(
            submit_response,
            task_store.load_status(material.project_id(), turn.task_id())?,
        ))
    }

    pub fn status(&self, job_id: JobId) -> Result<StatusResponse, WorkerError> {
        self.authoritative_job_with_supervisor_ensure(job_id, true, false)
            .map(AuthoritativeJob::into_response)
    }

    pub fn read_log(
        &self,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    ) -> Result<LogChunk, WorkerError> {
        let authoritative = self.authoritative_job_with_supervisor_ensure(job_id, true, false)?;
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
        self.authoritative_job_with_supervisor_ensure(job_id, true, true)
            .map(AuthoritativeJob::into_response)
    }

    /// Cancels one exactly identified accepted job. The admission lock proves
    /// immutable ownership, the supervisor lock fences launch/terminal
    /// publication, and both locks are released before TERM/KILL waits or
    /// mutable cleanup reacquires admission -> capacity.
    pub fn cancel(&self, request: CancelRequest) -> Result<CancelResponse, WorkerError> {
        request.validate()?;
        self.store.validate_layout()?;
        let job_id = request.job_id();
        let handoff_deadline = Instant::now() + CANCEL_SUPERVISOR_HANDOFF_DEADLINE;

        // A cancellation never blocks on the supervisor while holding
        // admission: a pre-Running supervisor may need admission after
        // releasing its own guard to publish cleanup. Each retry gets fresh
        // admission authority, revalidates every durable identity, then
        // attempts the supervisor handoff nonblocking.
        let (admission, mut supervisor, current) = loop {
            let admission = self.store.admission_lock(job_id)?;
            let initial = self.read_cancel_authority_after(&admission, &request)?;

            // A lease-retired terminal has already completed cleanup. It
            // must prove the original token through the durable marker rather
            // than being accepted on a job ID/meta match alone.
            if initial.status.state().is_terminal() && initial.lease.is_none() {
                self.store.validate_cancel_cleanup_marker_after(
                    &admission,
                    &request,
                    &initial.meta,
                )?;
                drop(admission);
                return cancel_response(initial.meta, initial.status);
            }

            let Some(supervisor) = self
                .store
                .supervisor_lock_after(&admission, job_id, false)?
            else {
                drop(admission);
                if Instant::now() >= handoff_deadline {
                    return Err(protocol_code(
                        "SUPERVISOR_LOCK_PENDING",
                        "supervisor handoff did not become available during cancellation",
                    ));
                }
                std::thread::sleep(CANCEL_SUPERVISOR_HANDOFF_POLL_INTERVAL);
                continue;
            };

            // A normal supervisor may have completed while this attempt was
            // waiting outside admission. Re-read every durable authority
            // while both capabilities are held; never infer a current state
            // from the pre-wait observation or overwrite its terminal result.
            let current = self.read_cancel_authority_after(&admission, &request)?;
            break (admission, supervisor, current);
        };

        if current.status.state().is_terminal() {
            if let Some(lease) = current.lease.as_ref() {
                let terminal = current.status.clone();
                let meta = current.meta.clone();
                let job = current.directory;
                drop(supervisor);
                drop(admission);
                self.cleanup_and_release_reconciled(lease, &job, &terminal)?;
                let observed = observed_terminal_status(&job, terminal);
                return cancel_response(meta, observed);
            }
            self.store
                .validate_cancel_cleanup_marker_after(&admission, &request, &current.meta)?;
            drop(supervisor);
            drop(admission);
            return cancel_response(current.meta, current.status);
        }

        let lease = current.lease.as_ref().ok_or_else(|| {
            job_state_invalid("nonterminal cancellation target has no live lease")
        })?;
        let status = current.status;
        let status_bytes = current.status_bytes;
        let job = current.directory;
        let meta = current.meta;

        if status.child_identity().is_none() {
            // This is the accepted/no-child launch fence. There is no process
            // identity to signal; publish the deliberate no-child terminal
            // form before any mutable data is removed.
            let (stdout_length, stderr_length) = synced_log_lengths(&job)?;
            let cancelled = status.into_prelaunch_cancelled(
                reconciliation_timestamp(status.updated_at_millis())?,
                stdout_length,
                stderr_length,
            )?;
            self.store.replace_job_status_after(
                &mut supervisor,
                lease,
                &job,
                &status_bytes,
                &status,
                &cancelled,
            )?;
            let publication = self.publish_cancelled_task_turn(lease, &job, &meta);
            let payload_removal = if job.entry_exists("execution.json")? {
                self.store
                    .remove_owned_regular_committed(&job, "execution.json")
            } else {
                Ok(())
            };
            drop(supervisor);
            drop(admission);
            let cleanup = match payload_removal {
                Ok(()) => self.cleanup_and_release_reconciled(lease, &job, &cancelled),
                Err(error) => Err(WorkerError::Io(error)),
            };
            if let Err(error) = publication {
                let _ = cleanup;
                return Err(error);
            }
            cleanup?;
            cancel_response(meta, cancelled)
        } else {
            // Do not keep admission across the bounded group proof. The
            // supervisor capability remains held so a natural terminal write
            // cannot race this cancellation decision.
            drop(admission);
            terminate_exact_recorded_group(self.reconciliation.as_ref(), status.child_identity())?;
            let (stdout_length, stderr_length) = synced_log_lengths(&job)?;
            let cancelled = status.into_infrastructure_terminal(
                JobState::Cancelled,
                reconciliation_timestamp(status.updated_at_millis())?,
                stdout_length,
                stderr_length,
                "CANCELLED".into(),
            )?;
            self.store.replace_job_status_after(
                &mut supervisor,
                lease,
                &job,
                &status_bytes,
                &status,
                &cancelled,
            )?;
            let publication = self.publish_cancelled_task_turn(lease, &job, &meta);
            let payload_removal = if job.entry_exists("execution.json")? {
                self.store
                    .remove_owned_regular_committed(&job, "execution.json")
            } else {
                Ok(())
            };
            drop(supervisor);
            let cleanup = match payload_removal {
                Ok(()) => self.cleanup_and_release_reconciled(lease, &job, &cancelled),
                Err(error) => Err(WorkerError::Io(error)),
            };
            if let Err(error) = publication {
                let _ = cleanup;
                return Err(error);
            }
            cleanup?;
            cancel_response(meta, cancelled)
        }
    }

    /// Publishes a task turn when the host-side accepted job is cancelled.
    /// Ordinary supervisor terminal paths already invoke this hook; the
    /// explicit cancellation path has to do so before it removes the job
    /// payload and releases the lease.
    fn publish_cancelled_task_turn(
        &self,
        lease: &LeaseRecord,
        job: &RootedDir,
        meta: &JobMeta,
    ) -> Result<(), WorkerError> {
        if !job.entry_exists("execution.json")? {
            return Ok(());
        }
        let payload: ExecutionPayload = read_canonical_json(job, "execution.json")?;
        payload.validate_for_durable_job(lease, meta)?;
        let Some(section) = payload.turn() else {
            return Ok(());
        };
        let task_store = TaskStore::new(self.store, &SystemProcessRunner);
        let task_status = task_store.load_status(section.project_id(), section.turn().task_id())?;
        let pending = task_status.state() == crate::task::TaskState::Active
            && task_status
                .turns()
                .last()
                .is_some_and(|turn| turn.turn_id() == lease.job_id() && turn.terminal().is_none());
        if !pending {
            return Ok(());
        }
        TurnTerminalHook
            .invoke(
                self.store,
                job,
                meta,
                section,
                crate::task::TurnTerminal::Cancelled,
                TerminalPath::HostCancel,
                None,
                false,
            )
            .map(|_| ())
    }

    /// Cancels the active turn identified by a task ID and turn ID. The host
    /// resolves the task's private accepted-job identity, then delegates to
    /// the existing exact lease/supervisor cancellation protocol.
    pub fn cancel_task(
        &self,
        request: TaskCancelRequest,
    ) -> Result<crate::task_store::TaskCancelResponse, WorkerError> {
        request.validate()?;
        let task_store = TaskStore::new(self.store, &SystemProcessRunner);
        let initial = task_store.load_status(request.project_id(), request.task_id())?;
        if initial.state() == crate::task::TaskState::Open
            && initial.turns().last().is_some_and(|turn| {
                turn.turn_id() == request.turn_id() && turn.terminal().is_some()
            })
        {
            return Ok(crate::task_store::TaskCancelResponse::new(initial));
        }
        if initial.state() != crate::task::TaskState::Active
            || !initial.turns().last().is_some_and(|turn| {
                turn.turn_id() == request.turn_id() && turn.terminal().is_none()
            })
        {
            return Err(protocol_code(
                "TASK_CLOSED",
                "task does not have the requested active turn",
            ));
        }
        let meta = task_store.load_meta(request.project_id(), request.task_id())?;
        let admission = self.store.admission_lock(request.turn_id())?;
        let lease = self
            .leases
            .load_after(&admission, request.turn_id())?
            .filter(|lease| lease.job_id() == request.turn_id())
            .ok_or_else(|| protocol_code("TASK_NOT_FOUND", "active task lease is absent"))?;
        if lease.project_id() != meta.project_id() || lease.worktree_id() != meta.worktree_id() {
            return Err(protocol_code(
                "TASK_TURN_CONFLICT",
                "active task lease does not match task metadata",
            ));
        }
        let cancel = CancelRequest::new(
            request.turn_id(),
            lease.client_id(),
            lease.lease_token(),
            lease.request_fingerprint().clone(),
        )?;
        drop(admission);
        self.cancel(cancel)?;
        let status = task_store.load_status(request.project_id(), request.task_id())?;
        Ok(crate::task_store::TaskCancelResponse::new(status))
    }

    fn read_cancel_authority_after(
        &self,
        admission: &AdmissionGuard,
        request: &CancelRequest,
    ) -> Result<CancelAuthority, WorkerError> {
        request.validate()?;
        let job_id = request.job_id();
        admission.validate_for(job_id)?;
        let disposition = self
            .store
            .disposition(job_id)?
            .ok_or_else(|| protocol_code("JOB_NOT_FOUND", "job ID is not indexed"))?;
        let (project_id, worktree_id, indexed_initial) = match disposition {
            JobDisposition::Accepted {
                job_id: indexed_job,
                client_id,
                project_id,
                worktree_id,
                request_fingerprint,
                status: indexed_initial,
                ..
            } => {
                if indexed_job != job_id
                    || client_id != request.client_id()
                    || request_fingerprint != *request.request_fingerprint()
                {
                    return Err(protocol_code(
                        "JOB_ID_CONFLICT",
                        "cancel request does not match the accepted job index",
                    ));
                }
                (project_id, worktree_id, indexed_initial)
            }
            JobDisposition::Abandoned { .. } => {
                return Err(protocol_code(
                    "JOB_ABANDONED",
                    "job ID was permanently abandoned",
                ));
            }
        };
        let directory = self
            .store
            .open_directory(&format!("jobs/{project_id}/{worktree_id}/{job_id}"), false)
            .map_err(|_| job_state_invalid("accepted job directory is absent or unsafe"))?;
        let meta: JobMeta = read_canonical_json(&directory, "meta.json")
            .map_err(|_| job_state_invalid("canonical job metadata is invalid"))?;
        if meta.job_id() != job_id
            || meta.client_id() != request.client_id()
            || meta.project_id() != project_id
            || meta.worktree_id() != worktree_id
            || meta.request_fingerprint() != request.request_fingerprint()
        {
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "accepted index does not match canonical job metadata",
            ));
        }
        let expected_initial = JobStatus::accepted(meta.created_at_millis())
            .map_err(|_| job_state_invalid("accepted job timestamp is invalid"))?;
        if indexed_initial != expected_initial {
            return Err(job_state_invalid(
                "accepted index does not contain the immutable initial status",
            ));
        }
        let (status_bytes, status): (Vec<u8>, JobStatus) =
            read_mutable_canonical_json_with_bytes(&directory, "status.json")
                .map_err(|_| job_state_invalid("canonical mutable job status is invalid"))?;
        validate_queryable_status(&meta, &status)?;
        if status.state().is_terminal() {
            validate_terminal_log_lengths(&directory, &status)?;
        }

        let loaded = self
            .leases
            .load_after(admission, job_id)
            .map_err(|_| job_state_invalid("live lease is invalid"))?;
        let lease = match loaded {
            Some(lease) if lease.job_id() == job_id => {
                require_meta_matches_live_lease(&meta, &lease)?;
                require_cancel_matches_lease(request, &lease)?;
                Some(lease)
            }
            Some(_) if status.state().is_terminal() => None,
            Some(_) => {
                return Err(job_state_invalid(
                    "nonterminal cancellation target has no live lease",
                ));
            }
            None if !status.state().is_terminal() => {
                return Err(job_state_invalid(
                    "nonterminal cancellation target has no live lease",
                ));
            }
            None => None,
        };
        admission.validate_for(job_id)?;
        Ok(CancelAuthority {
            lease,
            directory,
            meta,
            status_bytes,
            status,
        })
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
            let authoritative = self.status_from_accepted_after(
                admission,
                disposition,
                ensure_supervisor,
                ensure_supervisor,
            )?;
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
            let authoritative = self.status_from_accepted_after(
                admission,
                disposition,
                ensure_supervisor,
                ensure_supervisor,
            )?;
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
            "last.md",
            "prompt.md",
            "result.schema.json",
            "status.json",
            "stderr.log",
            "supervisor.log",
            "stdout.log",
            "tail.log",
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
        let mut turn_payload = false;
        if payload_present {
            let payload: ExecutionPayload = read_canonical_json(&job, "execution.json")
                .map_err(|_| job_id_conflict("incomplete execution payload is invalid"))?;
            payload.validate_for_resolution(identity)?;
            if let Some(section) = payload.turn() {
                turn_payload = true;
                if job.entry_exists("prompt.md")? {
                    let prompt = job
                        .read_private_regular("prompt.md", crate::task::MAX_PROMPT_BYTES as u64)
                        .map_err(|_| job_id_conflict("incomplete turn prompt is unsafe"))?;
                    if format!("{:x}", Sha256::digest(&prompt)) != section.turn().prompt_sha256() {
                        return Err(job_id_conflict(
                            "incomplete turn prompt does not match its digest",
                        ));
                    }
                }
                if job.entry_exists("result.schema.json")?
                    && job
                        .read_private_regular("result.schema.json", MAX_HOST_JSON_BYTES)
                        .map_err(|_| job_id_conflict("incomplete turn schema is unsafe"))?
                        != crate::agent::RESULT_SCHEMA_JSON.as_bytes()
                {
                    return Err(job_id_conflict("incomplete turn schema is not canonical"));
                }
                if job.entry_exists("tail.log")? {
                    let tail = job
                        .open_private_append("tail.log")
                        .map_err(|_| job_id_conflict("incomplete turn tail is unsafe"))?;
                    if job
                        .validate_private_append_binding("tail.log", &tail)
                        .map_err(|_| job_id_conflict("incomplete turn tail binding is unsafe"))?
                        != 0
                    {
                        return Err(job_id_conflict(
                            "nonempty unindexed turn tail is possible launch evidence",
                        ));
                    }
                }
            }
        }
        if !turn_payload
            && names.iter().any(|name| {
                matches!(
                    name.as_str(),
                    "last.md" | "prompt.md" | "result.schema.json" | "tail.log"
                )
            })
        {
            return Err(job_id_conflict(
                "turn evidence belongs to a non-turn execution payload",
            ));
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
        for log in ["stdout.log", "stderr.log", "supervisor.log"] {
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
        let complete_with_supervisor_log = complete
            .iter()
            .cloned()
            .chain(std::iter::once(String::from("supervisor.log")))
            .collect::<std::collections::BTreeSet<_>>();
        Ok(Some(
            if (names == complete || names == complete_with_supervisor_log) && !turn_payload {
                ResolutionFinal::Complete
            } else {
                ResolutionFinal::Incomplete(job)
            },
        ))
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
        require_cleanup: bool,
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
                self.status_from_accepted_after(
                    admission,
                    disposition,
                    ensure_supervisor,
                    require_cleanup,
                )
            }
            None => {
                self.status_without_disposition_after(
                    admission,
                    job_id,
                    ensure_supervisor,
                    require_cleanup,
                )
            }
        }
    }

    fn status_without_disposition_after(
        &self,
        admission: AdmissionGuard,
        job_id: JobId,
        ensure_supervisor: bool,
        require_cleanup: bool,
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
        self.authoritative_job_with_supervisor_ensure(job_id, ensure_supervisor, require_cleanup)
    }

    fn status_from_accepted_after(
        &self,
        admission: AdmissionGuard,
        disposition: JobDisposition,
        ensure_supervisor: bool,
        require_cleanup: bool,
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
            return self.authoritative_job_with_supervisor_ensure(job_id, false, false);
        }
        if !identityless_accepted {
            if current.state().is_terminal() {
                let turn_section = if job.entry_exists("execution.json")? {
                    let payload: ExecutionPayload = read_canonical_json(&job, "execution.json")?;
                    payload.validate_for_durable_job(&lease, &meta)?;
                    payload.turn().cloned()
                } else {
                    None
                };
                let publication = if let Some(section) = turn_section.as_ref() {
                    let task_store = TaskStore::new(self.store, &SystemProcessRunner);
                    let task_status =
                        task_store.load_status(section.project_id(), section.turn().task_id())?;
                    let pending = task_status.turns().last().is_some_and(|turn| {
                        turn.turn_id() == lease.job_id() && turn.terminal().is_none()
                    });
                    if task_status.state() == crate::task::TaskState::Active && pending {
                        let (terminal, path, exit_code) = terminal_turn_recovery(&current)?;
                        TurnTerminalHook
                            .invoke(
                                self.store, &job, &meta, section, terminal, path, exit_code, false,
                            )
                            .map(|_| ())
                    } else if task_status.state() == crate::task::TaskState::Active {
                        return Err(protocol_code(
                            "TASK_TURN_CONFLICT",
                            "terminal job retains a turn payload but task has another active turn",
                        ));
                    } else {
                        Ok(())
                    }
                } else {
                    Ok(())
                };
                let payload_removal = if job.entry_exists("execution.json")? {
                    self.store
                        .remove_owned_regular_committed(&job, "execution.json")
                } else {
                    Ok(())
                };
                drop(admission);
                drop(supervisor);
                let cleanup = match payload_removal {
                    Ok(()) => self.cleanup_and_release_reconciled(&lease, &job, &current),
                    Err(error) => Err(WorkerError::Io(error)),
                };
                if let Err(error) = publication {
                    let _ = cleanup;
                    return Err(error);
                }
                // Terminal log/status reads still retry cleanup so a leftover
                // FIFO can be drained later, but they must not fail the read.
                // Reconcile and resolution keep returning the cleanup error so
                // the busy lease stays the operator-visible signal.
                return Self::authoritative_after_terminal_cleanup(
                    job,
                    meta,
                    current,
                    cleanup,
                    require_cleanup,
                );
            }
            let supervisor_identity = current.supervisor_identity().ok_or_else(|| {
                job_state_invalid("nonterminal supervised job has no supervisor identity")
            })?;
            // A normal supervisor deliberately releases its lock only after
            // durable Running publication. If that exact detached supervisor
            // is still live in its own recorded process group, this is a live
            // handoff rather than an orphan: do not write Lost or signal the
            // child. Reused, ambiguous, and wrong-group observations retain
            // the existing fail-closed reconciliation path below.
            let supervisor_observation = self.reconciliation.observe(supervisor_identity);
            if current.state() == JobState::Running
                && matches!(
                    supervisor_observation,
                    ProcessObservation::Matching { process_group }
                        if process_group == supervisor_identity.pid()
                )
            {
                drop(supervisor);
                drop(admission);
                return AuthoritativeJob::new(job, meta, current);
            }
            drop(admission);
            reconcile_orphan_processes(
                self.reconciliation.as_ref(),
                supervisor_observation,
                current.child_identity(),
            )?;
            let turn_section = if job.entry_exists("execution.json")? {
                let payload: ExecutionPayload = read_canonical_json(&job, "execution.json")?;
                payload.validate_for_durable_job(&lease, &meta)?;
                payload.turn().cloned()
            } else {
                None
            };
            if job.entry_exists("execution.json")? {
                self.store
                    .remove_owned_regular_committed(&job, "execution.json")?;
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
            let publication = turn_section.as_ref().map_or(Ok(()), |section| {
                TurnTerminalHook
                    .invoke(
                        self.store,
                        &job,
                        &meta,
                        section,
                        crate::task::TurnTerminal::Lost,
                        TerminalPath::LostReconciliation,
                        None,
                        false,
                    )
                    .map(|_| ())
            });
            drop(supervisor);
            let cleanup = self.cleanup_and_release_reconciled(&lease, &job, &lost);
            if let Err(error) = publication {
                let _ = cleanup;
                return Err(error);
            }
            cleanup?;
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
            Ok(_) => self.authoritative_job_with_supervisor_ensure(job_id, false, false),
            Err(error) if matches!(&error, WorkerError::Protocol(message) if message.starts_with("SUPERVISOR_PRELAUNCH_FAILED:")) => {
                match self.authoritative_job_with_supervisor_ensure(job_id, false, false) {
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

    fn authoritative_after_terminal_cleanup(
        job: RootedDir,
        meta: JobMeta,
        current: JobStatus,
        cleanup: Result<(), WorkerError>,
        require_cleanup: bool,
    ) -> Result<AuthoritativeJob, WorkerError> {
        let observed = observed_terminal_status(&job, current);
        match cleanup {
            Ok(()) => AuthoritativeJob::new(job, meta, observed),
            Err(error) if require_cleanup => Err(error),
            Err(_) => AuthoritativeJob::new(job, meta, observed),
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
                self.clear_reconciliation_cleanup_error(lease, job, terminal)
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

    /// Clears a stale `cleanup_error_code` after a retried deferred cleanup
    /// and lease release both succeed. A later status read must be able to
    /// say the failure no longer exists; if the record moved, leave it for
    /// the next read.
    fn clear_reconciliation_cleanup_error(
        &self,
        lease: &LeaseRecord,
        job: &RootedDir,
        terminal: &JobStatus,
    ) -> Result<(), WorkerError> {
        if terminal.cleanup_error_code().is_none() {
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
            return Ok(());
        }
        let cleared = current
            .without_cleanup_error(reconciliation_timestamp(current.updated_at_millis())?)?;
        match self.store.replace_job_status_after(
            &mut supervisor,
            lease,
            job,
            &bytes,
            &current,
            &cleared,
        ) {
            Ok(()) => Ok(()),
            Err(error) if is_status_changed(&error) => Ok(()),
            Err(error) => Err(error),
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

    fn read_repairable_turn_final(
        &self,
        request: &SubmitRequest,
        lease: &LeaseRecord,
        section: &TurnSection,
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
            if status != JobStatus::accepted(meta.created_at_millis())? {
                return Err(protocol_code(
                    "JOB_ID_CONFLICT",
                    "unindexed final turn is not an exact prelaunch Accepted job",
                ));
            }
            validate_indexed_turn_prelaunch_job(&job, request, section)?;
            Ok((meta, status))
        })()
        .map_err(|_| {
            protocol_code(
                "JOB_ID_CONFLICT",
                "unindexed final turn evidence is incomplete or conflicting",
            )
        })?;
        Ok(Some(validated))
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
            let expected_with_supervisor_log = expected
                .iter()
                .cloned()
                .chain(std::iter::once(String::from("supervisor.log")))
                .collect::<std::collections::BTreeSet<_>>();
            if names != expected && names != expected_with_supervisor_log {
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
            if job.entry_exists("supervisor.log")? {
                // A failed earlier attempt leaves its diagnostic here; that
                // must not stop the retry.  The supervisor lock and the
                // recorded identities guard against a second execution, so
                // only the bound is checked.
                let file = job.open_private_append("supervisor.log")?;
                if job.validate_private_append_binding("supervisor.log", &file)?
                    > crate::supervisor::MAX_SUPERVISOR_LOG_BYTES
                {
                    return Err(WorkerError::Protocol(
                        "unindexed prelaunch supervisor log exceeds its bound".into(),
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
    let supervisor_log = root.write_new_private_file("supervisor.log", &[])?;
    supervisor_log.sync_all()?;
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

fn materialize_turn_control_files(
    store: &HostStore,
    root: &RootedDir,
    request: &SubmitRequest,
    prompt: &str,
    meta: &JobMeta,
    status: &JobStatus,
    section: &TurnSection,
) -> Result<(), WorkerError> {
    let tmp = root.open_child_directory(&relative("tmp")?, true)?;
    tmp.sync_root()?;
    for name in ["stdout.log", "stderr.log", "tail.log"] {
        let file = root.write_new_private_file(name, &[])?;
        file.sync_all()?;
    }
    let supervisor_log = root.write_new_private_file("supervisor.log", &[])?;
    supervisor_log.sync_all()?;
    let prompt_file = root.write_new_private_file("prompt.md", prompt.as_bytes())?;
    prompt_file.sync_all()?;
    drop(prompt_file);
    let schema = root.write_new_private_file(
        crate::agent::SCHEMA_FILE_NAME,
        crate::agent::RESULT_SCHEMA_JSON.as_bytes(),
    )?;
    schema.sync_all()?;
    drop(schema);
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
        &ExecutionPayload::new_with_turn(request, Some(section.clone()))?,
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
    let expected_with_supervisor_log = expected
        .iter()
        .cloned()
        .chain(std::iter::once(String::from("supervisor.log")))
        .collect::<std::collections::BTreeSet<_>>();
    if names != expected && names != expected_with_supervisor_log {
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
    // A failed earlier attempt leaves its diagnostic in `supervisor.log`;
    // that must not stop the retry.  The supervisor lock and the recorded
    // identities guard against a second execution, so only the bound is
    // checked here.
    if job.entry_exists("supervisor.log")?
        && job.validate_private_append_binding(
            "supervisor.log",
            &job.open_private_append("supervisor.log")?,
        )? > crate::supervisor::MAX_SUPERVISOR_LOG_BYTES
    {
        return Err(WorkerError::Protocol(
            "indexed prelaunch supervisor log exceeds its bound".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_indexed_turn_prelaunch_job(
    job: &RootedDir,
    request: &SubmitRequest,
    section: &TurnSection,
) -> Result<(), WorkerError> {
    let payload: ExecutionPayload = read_canonical_json(job, "execution.json")?;
    payload.validate_for_turn(request, section)?;
    let names = job
        .list_names()?
        .into_iter()
        .map(|name| {
            String::from_utf8(name)
                .map_err(|_| WorkerError::Protocol("final turn has a non-UTF-8 entry".into()))
        })
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    let expected = [
        ".mac-worker-rooted-fs",
        "execution.json",
        "meta.json",
        "prompt.md",
        "result.schema.json",
        "status.json",
        "stderr.log",
        "stdout.log",
        "tail.log",
        "tmp",
    ]
    .into_iter()
    .map(String::from)
    .collect::<std::collections::BTreeSet<_>>();
    let expected_with_supervisor_log = expected
        .iter()
        .cloned()
        .chain(std::iter::once(String::from("supervisor.log")))
        .collect::<std::collections::BTreeSet<_>>();
    if names != expected && names != expected_with_supervisor_log {
        return Err(WorkerError::Protocol(
            "indexed prelaunch turn has an unsafe top-level layout".into(),
        ));
    }
    job.open_child_directory(&relative("tmp")?, false)?;
    let prompt = job.read_private_regular("prompt.md", crate::task::MAX_PROMPT_BYTES as u64)?;
    if format!("{:x}", sha2::Sha256::digest(&prompt)) != section.turn().prompt_sha256() {
        return Err(WorkerError::Protocol(
            "indexed prelaunch turn prompt does not match its digest".into(),
        ));
    }
    if job.read_private_regular("result.schema.json", MAX_HOST_JSON_BYTES)?
        != crate::agent::RESULT_SCHEMA_JSON.as_bytes()
    {
        return Err(WorkerError::Protocol(
            "indexed prelaunch turn result schema is not canonical".into(),
        ));
    }
    for log in ["stdout.log", "stderr.log", "tail.log"] {
        let file = job.open_private_append(log)?;
        if job.validate_private_append_binding(log, &file)? != 0 {
            return Err(WorkerError::Protocol(
                "indexed prelaunch turn log is not empty".into(),
            ));
        }
    }
    // A failed earlier attempt leaves its diagnostic in `supervisor.log`;
    // that must not stop the retry.  The supervisor lock and the recorded
    // identities guard against a second execution, so only the bound is
    // checked here.
    if job.entry_exists("supervisor.log")?
        && job.validate_private_append_binding(
            "supervisor.log",
            &job.open_private_append("supervisor.log")?,
        )? > crate::supervisor::MAX_SUPERVISOR_LOG_BYTES
    {
        return Err(WorkerError::Protocol(
            "indexed prelaunch supervisor log exceeds its bound".into(),
        ));
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

pub(crate) fn read_canonical_json<T>(directory: &RootedDir, name: &str) -> Result<T, WorkerError>
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

fn observed_terminal_status(job: &RootedDir, fallback: JobStatus) -> JobStatus {
    read_mutable_canonical_json(job, "status.json").unwrap_or(fallback)
}

fn is_status_changed(error: &WorkerError) -> bool {
    matches!(error, WorkerError::Protocol(message) if message.starts_with("STATUS_CHANGED:"))
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

fn cancel_response(meta: JobMeta, status: JobStatus) -> Result<CancelResponse, WorkerError> {
    CancelResponse::new(StatusResponse::new(meta, status)?)
}

fn require_cancel_matches_lease(
    request: &CancelRequest,
    lease: &LeaseRecord,
) -> Result<(), WorkerError> {
    request.validate()?;
    lease
        .validate()
        .map_err(|_| job_state_invalid("live lease is invalid"))?;
    if request.job_id() != lease.job_id()
        || request.client_id() != lease.client_id()
        || request.lease_token() != lease.lease_token()
        || request.request_fingerprint() != lease.request_fingerprint()
    {
        return Err(protocol_code(
            "JOB_ID_CONFLICT",
            "cancel request does not match the live lease",
        ));
    }
    Ok(())
}

fn synced_log_lengths(job: &RootedDir) -> Result<(u64, u64), WorkerError> {
    let stdout = job.open_private_append("stdout.log")?;
    let stderr = job.open_private_append("stderr.log")?;
    stdout.sync_all()?;
    stderr.sync_all()?;
    let stdout_length = job.validate_private_append_binding("stdout.log", &stdout)?;
    let stderr_length = job.validate_private_append_binding("stderr.log", &stderr)?;
    Ok((stdout_length, stderr_length))
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
        JobState::Succeeded | JobState::Failed | JobState::TimedOut
            if status.supervisor_identity().is_none() || status.child_identity().is_none() =>
        {
            return Err(job_state_invalid(
                "command terminal status requires both durable process identities",
            ));
        }
        JobState::Cancelled if status.child_identity().is_none() => {
            if status.error_code() != Some("CANCELLED_PRELAUNCH") {
                return Err(job_state_invalid(
                    "no-child cancelled status must be the deliberate prelaunch cancellation form",
                ));
            }
        }
        JobState::Cancelled if status.supervisor_identity().is_none() => {
            return Err(job_state_invalid(
                "running cancellation requires both durable process identities",
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

pub(crate) fn reconstruct_submit_request(
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

fn validate_turn_origin(
    meta: &crate::task::TaskMeta,
    origin_url: Option<&str>,
) -> Result<(), WorkerError> {
    let pushing = meta.publish().contains(&PublishMode::Push);
    match (meta.push_origin_url(), pushing, origin_url) {
        (Some(expected), true, Some(actual)) if actual == expected => Ok(()),
        (_, false, None) => Ok(()),
        (_, true, None) => Err(protocol_code(
            "REQUEST_CONFLICT",
            "push publication has no origin target",
        )),
        (_, false, Some(_)) => Err(protocol_code(
            "REQUEST_CONFLICT",
            "turn has an unexpected origin target",
        )),
        (Some(_), true, Some(_)) => Err(protocol_code(
            "REQUEST_CONFLICT",
            "turn origin target does not match the task",
        )),
        (None, true, Some(_)) => Err(protocol_code(
            "REQUEST_CONFLICT",
            "push publication has no pinned origin target",
        )),
    }
}

fn relative(path: &str) -> Result<RelativePath, WorkerError> {
    RelativePath::parse(path.as_bytes())
        .map_err(|_| WorkerError::Protocol("host-owned relative directory name is invalid".into()))
}

fn protocol_code(code: &'static str, message: &str) -> WorkerError {
    WorkerError::Protocol(format!("{code}: {message}"))
}

fn terminal_turn_recovery(
    status: &JobStatus,
) -> Result<(crate::task::TurnTerminal, TerminalPath, Option<i32>), WorkerError> {
    match status.state() {
        JobState::Succeeded => Ok((
            crate::task::TurnTerminal::Succeeded,
            TerminalPath::ChildExit(i32::from(status.exit_code().unwrap_or(0))),
            status.exit_code().map(i32::from),
        )),
        JobState::Failed => {
            if let Some(code) = status.exit_code() {
                Ok((
                    crate::task::TurnTerminal::Failed,
                    TerminalPath::ChildExit(i32::from(code)),
                    Some(i32::from(code)),
                ))
            } else if let Some(signal) = status.terminating_signal() {
                Ok((
                    crate::task::TurnTerminal::Cancelled,
                    TerminalPath::ChildExit(-i32::try_from(signal).unwrap_or(1)),
                    None,
                ))
            } else {
                Err(job_state_invalid(
                    "failed turn status has no command outcome",
                ))
            }
        }
        JobState::Cancelled => Ok((
            crate::task::TurnTerminal::Cancelled,
            TerminalPath::HostCancel,
            None,
        )),
        JobState::TimedOut => Ok((
            crate::task::TurnTerminal::TimedOut,
            TerminalPath::Timeout,
            None,
        )),
        JobState::Lost => Ok((
            crate::task::TurnTerminal::Lost,
            TerminalPath::LostReconciliation,
            None,
        )),
        _ => Err(job_state_invalid(
            "turn publication recovery requires a terminal job status",
        )),
    }
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

fn now_millis() -> Result<u64, WorkerError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| protocol_code("CLOCK_INVALID", "system clock precedes the Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| protocol_code("CLOCK_INVALID", "system clock is outside the range"))
}
