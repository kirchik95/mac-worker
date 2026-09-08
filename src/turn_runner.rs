//! Client-side execution of one durable task turn.
//!
//! A task turn is deliberately driven by the local queue owner.  The queue
//! claim is the durable hand-off point; everything after it is retryable from
//! the task and turn records and never relies on a public legacy job record.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::{
        fs::{OpenOptionsExt, PermissionsExt},
        process::CommandExt,
    },
    process::{Command, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    agent::{TurnParams, adapter_for, render_shell},
    client_state::ClientStateStore,
    config::{Config, WorkerEntry},
    error::WorkerError,
    git_transport::GitTransport,
    job::{
        AdmissionObservation, CommandSpec, LeaseAcquireRequest, LeaseAcquireResponse, LeaseRecord,
        LeaseToken, LogStream, ProcessIdentity, QueueEntryKind, QueueState,
        RequestFingerprintMaterial, ResolveOrAbandonRequest, SubmitRequest,
    },
    paths::PathLayout,
    process::ProcessRunner,
    project_state::ProjectState,
    protocol::HealthStatus,
    scheduler::{CandidateObservation, SchedulerPolicy, WorkerPreference},
    scheduler_adapter::SchedulerProbeAdapter,
    supervisor::SystemProcessInspector,
    task::{
        LocalTaskRecord, RunnerIdentity, TaskId, TaskOutcome, TaskState, TaskStatus, TurnId,
        TurnSummary,
    },
    task_store::{
        TaskCancelRequest, TaskPrebindRequest, TaskPrepareRequest, TaskSessionRequest,
        TaskStatusRequest,
    },
    transfer::{RemoteJobClient, TransferIdentity},
    transfer_repo::TransferRepo,
    transport::{SshTransport, WorkersService},
    turn::{TaskTurnRequest, TurnMaterial},
};

const LOG_CHUNK_LIMIT: u32 = 64 * 1024;
const RUNNER_LOG_MODE: u32 = 0o600;
const RUNNER_DIRECTORY_MODE: u32 = 0o700;
const EARLY_EXIT_DIAGNOSTIC_LIMIT: usize = 256;
const WAIT_POLL: Duration = Duration::from_millis(100);
const MAX_CAPACITY_BACKOFF_SECS: u64 = 30;
const RUNNER_HANDOFF_TIMEOUT: Duration = Duration::from_secs(5);

/// Starts the durable process responsible for one queued task turn.
pub trait RunnerExecutor: Send + Sync {
    fn start(
        &self,
        paths: &PathLayout,
        task_id: TaskId,
        turn_id: TurnId,
    ) -> Result<RunnerIdentity, WorkerError>;
}

/// Test/attached executor.  The current process owns the queue row and the
/// caller immediately invokes [`TurnRunner::run`].
#[derive(Debug, Clone, Copy, Default)]
pub struct InlineRunnerExecutor;

impl RunnerExecutor for InlineRunnerExecutor {
    fn start(
        &self,
        _paths: &PathLayout,
        _task_id: TaskId,
        _turn_id: TurnId,
    ) -> Result<RunnerIdentity, WorkerError> {
        Ok(RunnerIdentity::new(current_process_identity()?))
    }
}

/// Production executor. The child has its own session and writes only through
/// the runner's explicit log writer. All task/turn inputs are reloaded by the
/// child through the hidden `runner` command.
#[derive(Debug, Clone, Copy, Default)]
pub struct DetachedRunnerExecutor;

impl RunnerExecutor for DetachedRunnerExecutor {
    fn start(
        &self,
        paths: &PathLayout,
        task_id: TaskId,
        turn_id: TurnId,
    ) -> Result<RunnerIdentity, WorkerError> {
        let runner_dir = paths.state.join("runners").join(task_id.to_string());
        fs::create_dir_all(&runner_dir).map_err(WorkerError::Io)?;
        fs::set_permissions(
            &runner_dir,
            fs::Permissions::from_mode(RUNNER_DIRECTORY_MODE),
        )
        .map_err(WorkerError::Io)?;
        let log_path = runner_dir.join(format!("{turn_id}.log"));
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(RUNNER_LOG_MODE)
            .open(&log_path)
            .map_err(WorkerError::Io)?;
        fs::set_permissions(&log_path, fs::Permissions::from_mode(RUNNER_LOG_MODE))
            .map_err(WorkerError::Io)?;
        drop(log);
        let executable = std::env::current_exe().map_err(WorkerError::Io)?;
        let mut command = Command::new(executable);
        if !paths.config.as_os_str().is_empty() {
            command.arg("--config").arg(&paths.config);
        }
        command
            .arg("runner")
            .arg(task_id.to_string())
            .arg(turn_id.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        // The supervisor/runner must not share the caller's process group:
        // Ctrl-C and terminal teardown are local client concerns.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
        let child = command.spawn().map_err(WorkerError::Io)?;
        let identity = SystemProcessInspector
            .identity_for_pid(child.id())
            .or_else(|_| fallback_process_identity(child.id()))?;
        // Dropping Child intentionally leaves the runner detached.  Its
        // durable state and owner identity are what reconciliation observes.
        drop(child);
        Ok(RunnerIdentity::new(identity))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnOutcomeReport {
    status: TaskStatus,
    events: Vec<serde_json::Value>,
    exit_code: u8,
}

impl TurnOutcomeReport {
    pub fn status(&self) -> &TaskStatus {
        &self.status
    }

    pub fn events(&self) -> &[serde_json::Value] {
        &self.events
    }

    pub fn exit_code(&self) -> u8 {
        self.exit_code
    }
}

pub struct TurnRunner<'a> {
    pub(crate) runner: &'a dyn ProcessRunner,
    pub(crate) config: &'a Config,
    pub(crate) paths: &'a PathLayout,
    pub(crate) client_state: &'a ClientStateStore,
    pub(crate) executor: &'a dyn RunnerExecutor,
}

impl<'a> TurnRunner<'a> {
    pub fn new(
        runner: &'a dyn ProcessRunner,
        config: &'a Config,
        paths: &'a PathLayout,
        client_state: &'a ClientStateStore,
        executor: &'a dyn RunnerExecutor,
    ) -> Self {
        Self {
            runner,
            config,
            paths,
            client_state,
            executor,
        }
    }

    /// Claims and executes exactly one local task-turn queue row.
    pub fn run(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        mut follow: Option<&mut dyn Write>,
    ) -> Result<TurnOutcomeReport, WorkerError> {
        ignore_sigpipe();
        // Legacy batch dispatch recovery deliberately skips task-turn rows,
        // but keeping the call here preserves the runner's recovery stage
        // and lets future mixed queues repair old rows without touching the
        // task-turn ownership protocol.
        let _ = self.client_state.recover_dead_dispatches()?;
        let result = (|| {
            let owner = self.claim(task_id, turn_id)?;
            let result = self.execute(task_id, turn_id, owner, &mut follow);
            match result {
                Ok(report) => {
                    self.start_next_parked(owner)?;
                    Ok(report)
                }
                Err(error) => {
                    // Before the host accepted a job, the dispatch is safe to
                    // retry.  Once accepted, the queue row remains durable and
                    // reconciliation will hand it to a replacement runner.
                    if matches!(self.can_revert_preacceptance(task_id, turn_id), Ok(true)) {
                        let _ = self.client_state.revert_dispatch(turn_id, owner);
                    }
                    let _ = self.client_state.record_runner(task_id, None);
                    Err(error)
                }
            }
        })();
        if let Err(error) = &result {
            let _ = self.write_early_exit_diagnostic(task_id, turn_id, error);
        }
        result
    }

    fn write_early_exit_diagnostic(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        error: &WorkerError,
    ) -> Result<(), WorkerError> {
        let mut log = self.client_state.open_runner_log(task_id, turn_id)?;
        if log.metadata().map_err(WorkerError::Io)?.len() > 0 {
            return Ok(());
        }
        let workers = self
            .config
            .workers
            .iter()
            .map(|worker| worker.name.as_str())
            .collect::<Vec<_>>();
        let code = self
            .client_state
            .task_blocking_codes(self.config)
            .ok()
            .and_then(|codes| codes.get(&task_id).cloned())
            .unwrap_or_else(|| error.public_code());
        log.write_all(early_exit_diagnostic_line(&code, &workers).as_bytes())
            .map_err(WorkerError::Io)
    }

    fn start_next_parked(&self, owner: ProcessIdentity) -> Result<(), WorkerError> {
        let Some(entry) = self.client_state.unpark_oldest(owner)? else {
            return Ok(());
        };
        let task_id = self
            .client_state
            .task_id_for_turn(entry.job_id())?
            .ok_or_else(|| {
                task_error("TASK_INCONSISTENT", "parked task turn has no task record")
            })?;
        let identity = match self.executor.start(self.paths, task_id, entry.job_id()) {
            Ok(identity) => identity,
            Err(error) => {
                self.abandon_handoff_failure(task_id, entry.job_id(), owner)?;
                return Err(task_error(
                    "RUNNER_HANDOFF_FAILED",
                    format!("runner handoff failed: {error}"),
                ));
            }
        };
        self.client_state
            .record_runner(task_id, Some(identity.clone()))?;
        if let Err(error) = adopt_row_with_retry(
            self.client_state,
            entry.job_id(),
            identity.process_identity(),
        ) {
            let _ = self.client_state.record_runner(task_id, None);
            self.abandon_handoff_failure(task_id, entry.job_id(), owner)?;
            return Err(error);
        }
        Ok(())
    }

    fn abandon_handoff_failure(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        owner: ProcessIdentity,
    ) -> Result<(), WorkerError> {
        let record = self.client_state.load_task(task_id)?;
        let status = TaskStatus::new(
            TaskState::Abandoned,
            Some(TaskOutcome::failed("RUNNER_HANDOFF_FAILED")),
            record.status().worker().map(str::to_owned),
            record.status().session_present(),
            record.status().head_oid().cloned(),
            record.status().summary().map(str::to_owned),
            record.status().questions().to_vec(),
            record.status().files_changed().to_vec(),
            record.status().diff_stat().map(str::to_owned),
            record.status().turns().to_vec(),
            now_millis()?,
        )?;
        self.client_state.update_task(
            record
                .with_status(status)?
                .with_abandon_code(Some("RUNNER_HANDOFF_FAILED".to_owned()))?,
        )?;
        let _ = self
            .client_state
            .remove_task_turn_after_terminal(turn_id, owner)?;
        let _ = self.client_state.remove_turn_prompt(task_id, turn_id);
        if let Ok(project_path) = std::env::current_dir()
            && let Ok(project) =
                ProjectState::load_for_task(self.runner, &project_path, &[], record.meta())
            && project.context.project_id == record.meta().project_id()
            && let Ok(transfer) =
                TransferRepo::open_or_create(&self.paths.cache, &project.context.common_dir)
        {
            let _ = transfer.release_base(self.runner, task_id);
        }
        Ok(())
    }

    fn claim(&self, task_id: TaskId, turn_id: TurnId) -> Result<ProcessIdentity, WorkerError> {
        let mut backoff = 1_u64;
        let runner_owner = current_process_identity()?;
        loop {
            let record = self.client_state.load_task(task_id)?;
            let entry = self
                .client_state
                .queue_entry(turn_id)?
                .ok_or_else(|| task_error("TASK_QUEUE_MISSING", "task turn is not queued"))?;
            if entry.kind() != QueueEntryKind::TaskTurn {
                return Err(task_error(
                    "TASK_QUEUE_KIND",
                    "queue row is not a task turn",
                ));
            }
            match entry.state() {
                QueueState::Dispatching { dispatch_owner, .. }
                    if *dispatch_owner == runner_owner =>
                {
                    return Ok(*dispatch_owner);
                }
                QueueState::Dispatching { .. } => {
                    return Err(queue_error(
                        "QUEUE_OWNER_MISMATCH",
                        "task turn is dispatched to another runner",
                    ));
                }
                QueueState::Parked => {
                    return Err(capacity_error(
                        "task turn is parked until a runner slot becomes available",
                    ));
                }
                QueueState::Waiting { owner } => {
                    if *owner != runner_owner {
                        // The detached parent starts the child before it can
                        // transfer ownership of the waiting row. Do not
                        // claim as the submitter during that window: the
                        // child must wait for its own identity to be adopted.
                        std::thread::sleep(WAIT_POLL);
                        continue;
                    }
                    let observations = self.observe_admission(entry.preference())?;
                    let affinity = self
                        .client_state
                        .affinity_hints(entry.project_id(), entry.worktree_id())?;
                    let ranked =
                        SchedulerPolicy::rank(&observations, entry.requirements(), &affinity)
                            .into_iter()
                            .map(|candidate| candidate.worker_name().to_owned())
                            .collect::<Vec<_>>();
                    if let Some(claim) =
                        self.client_state
                            .claim_next(runner_owner, &ranked, now_millis()?)?
                    {
                        return match claim.entry().state() {
                            QueueState::Dispatching { dispatch_owner, .. } => Ok(*dispatch_owner),
                            _ => Err(task_error(
                                "TASK_QUEUE_STATE",
                                "queue claim did not produce a dispatch",
                            )),
                        };
                    }
                    if !record.wait_for_capacity() {
                        self.abandon_capacity(&record, turn_id, *owner)?;
                        return Err(capacity_busy());
                    }
                }
            }
            std::thread::sleep(Duration::from_secs(backoff));
            backoff = (backoff * 2).min(MAX_CAPACITY_BACKOFF_SECS);
        }
    }

    fn execute(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        owner: ProcessIdentity,
        follow: &mut Option<&mut dyn Write>,
    ) -> Result<TurnOutcomeReport, WorkerError> {
        let initial_record = self.client_state.load_task(task_id)?;
        let project_path = std::env::current_dir().map_err(WorkerError::Io)?;
        let project =
            ProjectState::load_for_task(self.runner, &project_path, &[], initial_record.meta())?;
        require_project_match(
            &project,
            initial_record.meta().project_id(),
            initial_record.meta().worktree_id(),
        )?;
        let worker_name = self
            .client_state
            .queue_entry(turn_id)?
            .and_then(|entry| match entry.state() {
                QueueState::Dispatching {
                    selected_worker, ..
                } => Some(selected_worker.to_owned()),
                _ => None,
            })
            .or_else(|| initial_record.status().worker().map(str::to_owned))
            .ok_or_else(|| task_error("WORKER_NOT_FOUND", "task turn has no selected worker"))?;
        let worker = self
            .config
            .worker(&worker_name)
            .ok_or_else(|| task_error("WORKER_NOT_FOUND", "selected worker is not configured"))?;
        let transfer =
            TransferRepo::open_or_create(&self.paths.cache, &project.context.common_dir)?;
        let prompt = match self.client_state.read_turn_prompt(task_id, turn_id) {
            Ok(prompt) => prompt,
            Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return self.execute_accepted_without_prompt(
                    task_id,
                    turn_id,
                    owner,
                    &initial_record,
                    &project,
                    transfer,
                    worker,
                    follow,
                );
            }
            Err(error) => return Err(error),
        };
        if self
            .client_state
            .queue_entry(turn_id)?
            .is_some_and(|entry| entry.is_cancel_requested())
        {
            self.cancel_before_acceptance(&initial_record, task_id, turn_id, owner)?;
            return Err(task_error(
                "TASK_CANCELLED",
                "task turn was cancelled before host acceptance",
            ));
        }
        let log = self.client_state.open_runner_log(task_id, turn_id)?;
        let mut log = LogWriter { file: log };
        let turn_number = initial_record
            .status()
            .turns()
            .iter()
            .find(|turn| turn.turn_id() == turn_id)
            .map(|turn| turn.turn_number())
            .or_else(|| {
                u32::try_from(initial_record.status().turns().len())
                    .ok()
                    .and_then(|number| number.checked_add(1))
            })
            .ok_or_else(|| task_error("TASK_INCONSISTENT", "task turn history is too long"))?;
        // A first turn may already have bound its agent session before a
        // crash leaves the local prompt in place. Turn number, rather than
        // the mutable session-present projection, is the durable distinction
        // between first launch and a follow-up launch.
        let resume = turn_number > 1;
        let turn_limits = initial_record.meta().limits().turn.clone();
        let turn = TurnMaterial::from_prompt(
            task_id,
            turn_number,
            initial_record.meta().agent(),
            initial_record.meta().model().map(str::to_owned),
            initial_record.meta().effort().map(str::to_owned),
            initial_record.meta().policy(),
            turn_limits.clone(),
            initial_record
                .status()
                .head_oid()
                .cloned()
                .unwrap_or_else(|| initial_record.meta().base_oid().clone()),
            &prompt,
            initial_record.meta().env_profile().map(str::to_owned),
            turn_id.as_uuid(),
            resume,
        )?;
        let params = TurnParams {
            kind: turn.agent(),
            model: turn.model().map(str::to_owned),
            effort: turn.effort().map(str::to_owned),
            policy: turn.policy(),
            limits: turn_limits,
            session_seed: turn.session_seed(),
        };
        let adapter = adapter_for(turn.agent());
        let remote = RemoteJobClient::new(self.runner);
        let prebound = if !turn.resume() && adapter.prebind_session().is_some() {
            Some(
                remote
                    .task_prebind(
                        worker,
                        &TaskPrebindRequest::discover(
                            initial_record.meta().project_id(),
                            task_id,
                            turn.agent(),
                            turn.env_profile().map(str::to_owned),
                        ),
                    )?
                    .binding()
                    .session_ref()
                    .to_owned(),
            )
        } else {
            None
        };
        let session_ref = if turn.resume() {
            Some(
                remote
                    .task_session(
                        worker,
                        &TaskSessionRequest::new(initial_record.meta().project_id(), task_id),
                    )?
                    .binding()
                    .session_ref()
                    .to_owned(),
            )
        } else {
            prebound.clone()
        };
        let launch = if let Some(ref session) = session_ref {
            adapter
                .resume_turn(&params, session)
                .map_err(|error| task_error("TURN_COMMAND_INVALID", error.to_string()))?
        } else {
            adapter
                .first_turn(&params)
                .map_err(|error| task_error("TURN_COMMAND_INVALID", error.to_string()))?
        };
        let shell = render_shell(&launch)
            .map_err(|error| task_error("TURN_COMMAND_INVALID", error.to_string()))?;
        let lease_token = LeaseToken::new(turn_id.as_uuid());
        let material = RequestFingerprintMaterial::new(
            turn_id,
            self.client_state.client_id(),
            lease_token,
            initial_record.meta().created_at_millis(),
            worker.name.clone(),
            initial_record.meta().project_id().to_owned(),
            initial_record.meta().worktree_id().to_owned(),
            turn.digest(),
            String::new(),
            turn.limits().timeout_millis,
            "heavy".into(),
            CommandSpec::shell(shell)?,
        )?;
        let lease_request = LeaseAcquireRequest::new(material.clone());
        let acquired = match remote.lease_acquire(worker, &lease_request) {
            Ok(acquired) => acquired,
            Err(error)
                if !initial_record.wait_for_capacity()
                    && error.public_code() == "CAPACITY_BUSY" =>
            {
                self.abandon_capacity(&initial_record, turn_id, owner)?;
                return Err(capacity_busy());
            }
            Err(error) => return Err(error),
        };
        let task_status = match acquired {
            LeaseAcquireResponse::Acquired { lease } => {
                require_exact_lease(&lease, &lease_request, worker)?;
                let identity = TransferIdentity::from_acquire_request(&lease_request)?;
                if self.queue_cancel_requested(turn_id)? {
                    let resolve = ResolveOrAbandonRequest::from_submit_request(
                        &SubmitRequest::new(material.clone()),
                    )?;
                    let _ = remote.resolve_preacceptance(worker, &resolve);
                    self.cancel_before_acceptance(&initial_record, task_id, turn_id, owner)?;
                    return Err(task_error(
                        "TASK_CANCELLED",
                        "task turn was cancelled before host acceptance",
                    ));
                }
                let prepared = if turn.resume() {
                    remote.task_status(
                        worker,
                        &TaskStatusRequest::new(initial_record.meta().project_id(), task_id),
                    )
                } else {
                    (|| {
                        GitTransport::new(self.runner).push_base(
                            worker,
                            &identity,
                            initial_record.meta().project_id(),
                            task_id,
                            turn.base_oid(),
                            transfer.path(),
                        )?;
                        remote.task_prepare(
                            worker,
                            &TaskPrepareRequest::new(
                                initial_record.meta().clone(),
                                turn_id,
                                worker.name.clone(),
                            ),
                        )?;
                        remote.task_status(
                            worker,
                            &TaskStatusRequest::new(initial_record.meta().project_id(), task_id),
                        )
                    })()
                };
                let prepared = match prepared {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        let resolve = ResolveOrAbandonRequest::from_submit_request(
                            &SubmitRequest::new(material.clone()),
                        )?;
                        let _ = remote.resolve_preacceptance(worker, &resolve);
                        if !initial_record.wait_for_capacity()
                            && error.public_code() == "CAPACITY_BUSY"
                        {
                            self.abandon_capacity(&initial_record, turn_id, owner)?;
                            return Err(capacity_busy());
                        }
                        return Err(error);
                    }
                };
                if self.queue_cancel_requested(turn_id)? {
                    let resolve = ResolveOrAbandonRequest::from_submit_request(
                        &SubmitRequest::new(material.clone()),
                    )?;
                    let _ = remote.resolve_preacceptance(worker, &resolve);
                    self.cancel_before_acceptance(&initial_record, task_id, turn_id, owner)?;
                    return Err(task_error(
                        "TASK_CANCELLED",
                        "task turn was cancelled before host acceptance",
                    ));
                }
                if !turn.resume() {
                    if let Some(ref session) = prebound {
                        remote.task_prebind(
                            worker,
                            &TaskPrebindRequest::persist(
                                initial_record.meta().project_id(),
                                task_id,
                                turn.agent(),
                                turn.env_profile().map(str::to_owned),
                                session,
                            ),
                        )?;
                    }
                    self.persist_status(task_id, prepared.status().clone())?;
                }
                if prepared.status().state().is_terminal()
                    || (!turn.resume() && matches!(prepared.status().state(), TaskState::Open))
                {
                    prepared.status().clone()
                } else {
                    let request = TaskTurnRequest::new_with_origin(
                        SubmitRequest::new(material.clone()),
                        turn.clone(),
                        prompt.clone(),
                        turn_origin_url(initial_record.meta()),
                    )?;
                    let response = match remote.submit_turn(worker, &request) {
                        Ok(response) => response,
                        Err(error) => {
                            let resolve = ResolveOrAbandonRequest::from_submit_request(
                                &SubmitRequest::new(material.clone()),
                            )?;
                            let _ = remote.resolve_preacceptance(worker, &resolve);
                            if !initial_record.wait_for_capacity()
                                && error.public_code() == "CAPACITY_BUSY"
                            {
                                self.abandon_capacity(&initial_record, turn_id, owner)?;
                                return Err(capacity_busy());
                            }
                            return Err(error);
                        }
                    };
                    let status = if self.queue_cancel_requested(turn_id)? {
                        remote
                            .task_cancel(
                                worker,
                                &TaskCancelRequest::new(
                                    initial_record.meta().project_id(),
                                    task_id,
                                    turn_id,
                                ),
                            )?
                            .status()
                            .clone()
                    } else {
                        response.task().clone()
                    };
                    self.persist_status(task_id, status.clone())?;
                    self.client_state.remove_turn_prompt(task_id, turn_id)?;
                    append_event(
                        &mut log,
                        follow,
                        serde_json::json!({
                            "type": "turn_accepted",
                            "protocol_version": crate::protocol::PROTOCOL_VERSION,
                            "task_id": task_id.to_string(),
                            "turn_id": turn_id.to_string(),
                            "worker": worker.name,
                        }),
                    )?;
                    status
                }
            }
            LeaseAcquireResponse::ExistingAccepted { .. } => {
                let status = remote
                    .task_status(
                        worker,
                        &TaskStatusRequest::new(initial_record.meta().project_id(), task_id),
                    )?
                    .status()
                    .clone();
                self.persist_status(task_id, status.clone())?;
                self.client_state.remove_turn_prompt(task_id, turn_id)?;
                status
            }
        };

        let terminal = if task_status.state().is_terminal() {
            task_status
        } else {
            self.follow_remote(worker, task_id, turn_id, task_status, &mut log, follow)?
        };
        self.finish_terminal(
            task_id,
            turn_id,
            owner,
            &initial_record,
            &project,
            transfer,
            worker,
            terminal,
            &mut log,
            follow,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_accepted_without_prompt(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        owner: ProcessIdentity,
        initial_record: &LocalTaskRecord,
        project: &ProjectState,
        transfer: TransferRepo,
        worker: &WorkerEntry,
        follow: &mut Option<&mut dyn Write>,
    ) -> Result<TurnOutcomeReport, WorkerError> {
        let mut log = LogWriter {
            file: self.client_state.open_runner_log(task_id, turn_id)?,
        };
        let remote = RemoteJobClient::new(self.runner);
        let mut status = remote
            .task_status(
                worker,
                &TaskStatusRequest::new(initial_record.meta().project_id(), task_id),
            )?
            .status()
            .clone();
        self.persist_status(task_id, status.clone())?;
        let terminal = if status.state().is_terminal() || status.state() == TaskState::Open {
            status
        } else {
            status = self.follow_remote(worker, task_id, turn_id, status, &mut log, follow)?;
            status
        };
        self.finish_terminal(
            task_id,
            turn_id,
            owner,
            initial_record,
            project,
            transfer,
            worker,
            terminal,
            &mut log,
            follow,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_terminal(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        owner: ProcessIdentity,
        initial_record: &LocalTaskRecord,
        project: &ProjectState,
        transfer: TransferRepo,
        worker: &WorkerEntry,
        terminal: TaskStatus,
        log: &mut LogWriter,
        follow: &mut Option<&mut dyn Write>,
    ) -> Result<TurnOutcomeReport, WorkerError> {
        self.persist_status(task_id, terminal.clone())?;
        let fetched = if matches!(terminal.state(), TaskState::Open | TaskState::Closed) {
            if let Err(_error) = GitTransport::new(self.runner).fetch_result(
                worker,
                self.client_state.client_id(),
                initial_record.meta().project_id(),
                task_id,
                transfer.path(),
            ) {
                return self.finish_publication_failure(
                    task_id,
                    turn_id,
                    owner,
                    terminal,
                    transfer,
                    "RESULT_FETCH_FAILED",
                    log,
                    follow,
                );
            }
            match transfer.import_result(
                self.runner,
                &project.context.common_dir,
                worker.name.as_str(),
                task_id,
            ) {
                Ok(receipt) => Some(receipt.head().clone()),
                Err(_error) => {
                    return self.finish_publication_failure(
                        task_id,
                        turn_id,
                        owner,
                        terminal,
                        transfer,
                        "PUBLISH_FAILED",
                        log,
                        follow,
                    );
                }
            }
        } else {
            None
        };
        let record = self.client_state.load_task(task_id)?;
        let record = if let Some(head) = fetched {
            record.with_fetched_head(Some(head))?
        } else {
            record
        };
        self.client_state.update_task(record.with_runner(None)?)?;
        transfer.release_base(self.runner, task_id)?;
        self.client_state
            .remove_task_turn_after_terminal(turn_id, owner)?;
        let outcome = terminal
            .last_outcome()
            .cloned()
            .unwrap_or_else(|| TaskOutcome::failed("TASK_TERMINAL_OUTCOME_MISSING"));
        let exit_code = turn_exit_code(&outcome);
        let event = serde_json::json!({
            "type": "turn_terminal",
            "protocol_version": crate::protocol::PROTOCOL_VERSION,
            "task_id": task_id.to_string(),
            "turn_id": turn_id.to_string(),
            "outcome": outcome,
        });
        append_event(log, follow, event.clone())?;
        Ok(TurnOutcomeReport {
            status: terminal,
            events: vec![event],
            exit_code,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_publication_failure(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        owner: ProcessIdentity,
        terminal: TaskStatus,
        transfer: TransferRepo,
        failure_code: &'static str,
        log: &mut LogWriter,
        follow: &mut Option<&mut dyn Write>,
    ) -> Result<TurnOutcomeReport, WorkerError> {
        let outcome = TaskOutcome::failed(failure_code);
        let status = publication_failure_status(&terminal, outcome.clone(), now_millis()?)?;
        let record = self.client_state.load_task(task_id)?;
        self.client_state
            .update_task(record.with_status(status.clone())?.with_runner(None)?)?;
        transfer.release_base(self.runner, task_id)?;
        self.client_state
            .remove_task_turn_after_terminal(turn_id, owner)?;
        let event = serde_json::json!({
            "type": "turn_terminal",
            "protocol_version": crate::protocol::PROTOCOL_VERSION,
            "task_id": task_id.to_string(),
            "turn_id": turn_id.to_string(),
            "outcome": outcome,
        });
        append_event(log, follow, event.clone())?;
        Ok(TurnOutcomeReport {
            status,
            events: vec![event],
            exit_code: turn_exit_code(&TaskOutcome::failed(failure_code)),
        })
    }

    fn can_revert_preacceptance(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
    ) -> Result<bool, WorkerError> {
        if self
            .client_state
            .read_turn_prompt(task_id, turn_id)
            .is_err()
        {
            return Ok(false);
        }
        let record = self.client_state.load_task(task_id)?;
        let Some(entry) = self.client_state.queue_entry(turn_id)? else {
            return Ok(false);
        };
        let worker_name = match entry.state() {
            QueueState::Dispatching {
                selected_worker, ..
            } => selected_worker.clone(),
            QueueState::Waiting { .. } | QueueState::Parked => {
                return Ok(true);
            }
        };
        let Some(worker) = self.config.worker(&worker_name) else {
            return Ok(true);
        };
        match RemoteJobClient::new(self.runner).task_status(
            worker,
            &TaskStatusRequest::new(record.meta().project_id(), task_id),
        ) {
            Ok(_) => Ok(false),
            Err(error) => Ok(matches!(
                error.public_code().as_str(),
                "TASK_NOT_FOUND" | "JOB_NOT_FOUND"
            )),
        }
    }

    fn follow_remote(
        &self,
        worker: &WorkerEntry,
        task_id: TaskId,
        turn_id: TurnId,
        mut status: TaskStatus,
        log: &mut LogWriter,
        follow: &mut Option<&mut dyn Write>,
    ) -> Result<TaskStatus, WorkerError> {
        let remote = RemoteJobClient::new(self.runner);
        let mut offsets = [0_u64, 0_u64];
        loop {
            if status.state().is_terminal() || matches!(status.state(), TaskState::Open) {
                return Ok(status);
            }
            if self.queue_cancel_requested(turn_id)? {
                status = remote
                    .task_cancel(
                        worker,
                        &TaskCancelRequest::new(
                            self.client_state.load_task(task_id)?.meta().project_id(),
                            task_id,
                            turn_id,
                        ),
                    )?
                    .status()
                    .clone();
                self.persist_status(task_id, status.clone())?;
                continue;
            }
            for (index, stream) in [LogStream::Stdout, LogStream::Stderr]
                .into_iter()
                .enumerate()
            {
                let chunk =
                    remote.log_chunk(worker, turn_id, stream, offsets[index], LOG_CHUNK_LIMIT)?;
                let bytes = chunk.decoded_bytes()?;
                if !bytes.is_empty() {
                    log.file.write_all(&bytes).map_err(WorkerError::Io)?;
                    write_follower(follow, &bytes);
                }
                offsets[index] = chunk.next_offset();
            }
            std::thread::sleep(WAIT_POLL);
            status = remote
                .task_status(
                    worker,
                    &TaskStatusRequest::new(
                        self.client_state.load_task(task_id)?.meta().project_id(),
                        task_id,
                    ),
                )?
                .status()
                .clone();
            self.persist_status(task_id, status.clone())?;
        }
    }

    fn persist_status(&self, task_id: TaskId, status: TaskStatus) -> Result<(), WorkerError> {
        let record = self.client_state.load_task(task_id)?;
        let observed = status.updated_at_millis();
        self.client_state.update_task(
            record
                .with_status(status)?
                .with_status_observed_at(Some(observed))?,
        )
    }

    fn queue_cancel_requested(&self, turn_id: TurnId) -> Result<bool, WorkerError> {
        Ok(self
            .client_state
            .queue_entry(turn_id)?
            .is_some_and(|entry| entry.is_cancel_requested()))
    }

    fn abandon_capacity(
        &self,
        record: &LocalTaskRecord,
        turn_id: TurnId,
        owner: ProcessIdentity,
    ) -> Result<(), WorkerError> {
        let status = TaskStatus::new(
            TaskState::Abandoned,
            Some(TaskOutcome::failed("CAPACITY_BUSY")),
            record.status().worker().map(str::to_owned),
            record.status().session_present(),
            record.status().head_oid().cloned(),
            record.status().summary().map(str::to_owned),
            record.status().questions().to_vec(),
            record.status().files_changed().to_vec(),
            record.status().diff_stat().map(str::to_owned),
            record.status().turns().to_vec(),
            now_millis()?,
        )?;
        self.client_state.update_task(
            record
                .with_status(status)?
                .with_abandon_code(Some("CAPACITY_BUSY".into()))?,
        )?;
        let _ = self
            .client_state
            .remove_task_turn_after_terminal(turn_id, owner)?;
        let _ = self
            .client_state
            .remove_turn_prompt(record.meta().task_id(), turn_id);
        if let Ok(project_path) = std::env::current_dir()
            && let Ok(project) =
                ProjectState::load_for_task(self.runner, &project_path, &[], record.meta())
            && project.context.project_id == record.meta().project_id()
            && let Ok(transfer) =
                TransferRepo::open_or_create(&self.paths.cache, &project.context.common_dir)
        {
            let _ = transfer.release_base(self.runner, record.meta().task_id());
        }
        Ok(())
    }

    fn cancel_before_acceptance(
        &self,
        record: &LocalTaskRecord,
        task_id: TaskId,
        turn_id: TurnId,
        owner: ProcessIdentity,
    ) -> Result<(), WorkerError> {
        let status = if record.status().state() == TaskState::Active {
            cancelled_followup_status(record.status())?
        } else {
            TaskStatus::new(
                TaskState::Abandoned,
                Some(TaskOutcome::Cancelled),
                record.status().worker().map(str::to_owned),
                record.status().session_present(),
                record.status().head_oid().cloned(),
                record.status().summary().map(str::to_owned),
                record.status().questions().to_vec(),
                record.status().files_changed().to_vec(),
                record.status().diff_stat().map(str::to_owned),
                record.status().turns().to_vec(),
                now_millis()?,
            )?
        };
        let abandoned = record.status().state() != TaskState::Active;
        self.client_state.update_task(
            record
                .with_status(status)?
                .with_runner(None)?
                .with_abandon_code(abandoned.then_some("CANCELLED".to_owned()))?,
        )?;
        let _ = self
            .client_state
            .remove_task_turn_after_terminal(turn_id, owner)?;
        let _ = self.client_state.remove_turn_prompt(task_id, turn_id);
        if abandoned
            && let Ok(project_path) = std::env::current_dir()
            && let Ok(project) =
                ProjectState::load_for_task(self.runner, &project_path, &[], record.meta())
            && project.context.project_id == record.meta().project_id()
            && let Ok(transfer) =
                TransferRepo::open_or_create(&self.paths.cache, &project.context.common_dir)
        {
            let _ = transfer.release_base(self.runner, task_id);
        }
        Ok(())
    }

    fn observe_admission(
        &self,
        preference: &WorkerPreference,
    ) -> Result<Vec<CandidateObservation>, WorkerError> {
        self.config
            .workers
            .iter()
            .filter(|worker| match preference {
                WorkerPreference::Automatic => true,
                WorkerPreference::Pinned { worker: pinned } => worker.name == *pinned,
            })
            .map(|worker| {
                let one = Config {
                    version: self.config.version,
                    workers: vec![worker.clone()],
                };
                let observed_at = now_millis()?;
                let mut health = WorkersService::new(SshTransport::new(self.runner))
                    .inspect(&one)
                    .workers
                    .into_iter()
                    .next()
                    .ok_or_else(|| WorkerError::Protocol("worker probe was empty".into()))?;
                let facts_stale = health.status == HealthStatus::Ready
                    && health.probe.as_ref().is_none_or(|probe| {
                        probe
                            .agent_facts
                            .as_ref()
                            .is_none_or(|facts| facts.is_stale(observed_at))
                    });
                if facts_stale {
                    match SshTransport::new(self.runner).refresh_facts(worker) {
                        Ok(()) => {
                            health = WorkersService::new(SshTransport::new(self.runner))
                                .inspect(&one)
                                .workers
                                .into_iter()
                                .next()
                                .ok_or_else(|| {
                                    WorkerError::Protocol("worker probe was empty".into())
                                })?;
                        }
                        Err(
                            error @ WorkerError::Transport {
                                code: "REFRESH_FACTS_FAILED",
                                ..
                            },
                        ) => {
                            health.status = HealthStatus::Unavailable;
                            health.error_code = Some(error.public_code());
                            health.error_message = Some(error.public_message());
                        }
                        Err(error) => return Err(error),
                    }
                }
                if facts_stale || health.status == HealthStatus::Unavailable {
                    // A failed worker is an unavailable candidate, not a
                    // failed fleet observation. Publish it so an earlier
                    // ready cache entry cannot admit work to this worker.
                    let candidate = candidate_from_health(&one, &health)?;
                    self.client_state
                        .publish_admission_observation(admission_from_health(
                            &one,
                            &health,
                            observed_at,
                        )?)?;
                    return Ok(candidate);
                }

                let cached =
                    self.client_state
                        .admission_observation(&worker.name, observed_at, || {
                            admission_from_health(&one, &health, observed_at)
                        })?;
                CandidateObservation::new(
                    cached.observation().worker_name().to_owned(),
                    cached.observation().ready(),
                    cached.observation().slot(),
                    cached.observation().capabilities().to_vec(),
                    cached.observation().available_memory_bytes(),
                    cached.observation().free_disk_bytes(),
                )
                .map_err(|_| {
                    WorkerError::Protocol("cached scheduler observation is invalid".into())
                })
            })
            .collect()
    }
}

fn candidate_from_health(
    config: &Config,
    health: &crate::protocol::WorkerHealth,
) -> Result<CandidateObservation, WorkerError> {
    SchedulerProbeAdapter::observations(config, std::slice::from_ref(health))?
        .into_iter()
        .next()
        .ok_or_else(|| WorkerError::Protocol("worker probe was empty".into()))
}

fn admission_from_health(
    config: &Config,
    health: &crate::protocol::WorkerHealth,
    observed_at: u64,
) -> Result<AdmissionObservation, WorkerError> {
    let candidate = candidate_from_health(config, health)?;
    AdmissionObservation::new(
        candidate.worker_name().to_owned(),
        candidate.ready(),
        candidate.slot(),
        candidate.capabilities().to_vec(),
        candidate.available_memory_bytes(),
        candidate.free_disk_bytes(),
        observed_at,
    )
}

/// Transfer a waiting or dispatching task row to the process that will
/// execute it. A detached child can start before its parent reaches this
/// write, so the child-side claim path waits for its own identity to appear.
pub(crate) fn adopt_row_with_retry(
    client_state: &ClientStateStore,
    turn_id: TurnId,
    owner: ProcessIdentity,
) -> Result<crate::job::QueueEntry, WorkerError> {
    let deadline = Instant::now() + RUNNER_HANDOFF_TIMEOUT;
    loop {
        match client_state.adopt_row(turn_id, owner) {
            Ok(entry) => return Ok(entry),
            Err(_error) if Instant::now() < deadline => {
                std::thread::sleep(WAIT_POLL);
            }
            Err(error) => return Err(error),
        }
    }
}

struct LogWriter {
    file: File,
}

fn append_event(
    log: &mut LogWriter,
    follow: &mut Option<&mut dyn Write>,
    event: serde_json::Value,
) -> Result<(), WorkerError> {
    let mut line = serde_json::to_vec(&event)
        .map_err(|error| task_error("TASK_EVENT_INVALID", error.to_string()))?;
    line.push(b'\n');
    log.file.write_all(&line).map_err(WorkerError::Io)?;
    write_follower(follow, &line);
    Ok(())
}

fn write_follower(follow: &mut Option<&mut dyn Write>, bytes: &[u8]) {
    let failed = follow
        .as_deref_mut()
        .is_some_and(|writer| writer.write_all(bytes).is_err() || writer.flush().is_err());
    if failed {
        *follow = None;
    }
}

fn ignore_sigpipe() {
    // Rust normally installs SIG_IGN for SIGPIPE. Keep that invariant for
    // callers embedding the runner so a closed follower is reported by its
    // writer instead of terminating the process.
    unsafe {
        let _ = libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
}

fn require_exact_lease(
    lease: &LeaseRecord,
    request: &LeaseAcquireRequest,
    worker: &WorkerEntry,
) -> Result<(), WorkerError> {
    let material = request.material();
    if lease.job_id() != material.job_id()
        || lease.client_id() != material.client_id()
        || lease.lease_token() != material.lease_token()
        || lease.request_fingerprint() != request.request_fingerprint()
        || lease.worker_name() != worker.name
        || lease.project_id() != material.project_id()
        || lease.worktree_id() != material.worktree_id()
        || lease.manifest_digest() != material.manifest_digest()
        || lease.timeout_millis() != material.timeout_millis()
        || lease.resource_class() != material.resource_class()
        || lease.command_summary() != &material.command().summary()?
    {
        return Err(WorkerError::Transport {
            code: "LEASE_IDENTITY_MISMATCH",
            message: "remote lease does not match the queued turn".into(),
        });
    }
    Ok(())
}

fn require_project_match(
    project: &ProjectState,
    project_id: &str,
    worktree_id: &str,
) -> Result<(), WorkerError> {
    if project.context.project_id != project_id || project.context.worktree_id != worktree_id {
        return Err(task_error(
            "PROJECT_MISMATCH",
            "current project is not the task's project",
        ));
    }
    Ok(())
}

fn turn_origin_url(meta: &crate::task::TaskMeta) -> Option<String> {
    meta.push_origin_url().map(str::to_owned)
}

fn early_exit_diagnostic_line(code: &str, workers: &[&str]) -> String {
    let mut line = if workers.is_empty() {
        format!("exited: {code}")
    } else {
        format!("exited: {code} workers={}", workers.join(","))
    };
    if line.len() > EARLY_EXIT_DIAGNOSTIC_LIMIT {
        line.truncate(EARLY_EXIT_DIAGNOSTIC_LIMIT);
    }
    line.push('\n');
    line
}

fn capacity_busy() -> WorkerError {
    WorkerError::Capacity {
        code: "CAPACITY_BUSY",
        message: "no eligible worker currently has an available heavy slot".into(),
    }
}

fn capacity_error(message: impl Into<String>) -> WorkerError {
    WorkerError::Capacity {
        code: "CAPACITY_BUSY",
        message: message.into(),
    }
}

fn queue_error(code: &'static str, message: impl Into<String>) -> WorkerError {
    WorkerError::Queue {
        code,
        message: message.into(),
    }
}

fn turn_exit_code(outcome: &TaskOutcome) -> u8 {
    match outcome {
        TaskOutcome::Done | TaskOutcome::NeedsInput | TaskOutcome::Unknown => 0,
        TaskOutcome::Failed { reason } => match reason.as_str() {
            "PUBLISH_FAILED" | "RESULT_FETCH_FAILED" | "RESULT_UNPARSEABLE" => 70,
            _ => reason
                .strip_prefix("agent exited ")
                .and_then(|code| code.parse::<u8>().ok())
                .unwrap_or(1),
        },
        TaskOutcome::Blocked
        | TaskOutcome::Cancelled
        | TaskOutcome::TimedOut
        | TaskOutcome::Lost => 1,
    }
}

fn publication_failure_status(
    terminal: &TaskStatus,
    outcome: TaskOutcome,
    ended_at_millis: u64,
) -> Result<TaskStatus, WorkerError> {
    let mut turns = terminal.turns().to_vec();
    if let Some(last) = turns.last().cloned() {
        let replacement = TurnSummary::new(
            last.turn_number(),
            last.turn_id(),
            last.terminal(),
            Some(outcome.clone()),
            last.agent_committed(),
            last.log_truncated(),
            last.started_at_millis(),
            Some(ended_at_millis),
        );
        *turns
            .last_mut()
            .expect("cloned last turn must remain present") = replacement;
    }
    TaskStatus::new(
        TaskState::Open,
        Some(outcome),
        terminal.worker().map(str::to_owned),
        terminal.session_present(),
        terminal.head_oid().cloned(),
        terminal.summary().map(str::to_owned),
        terminal.questions().to_vec(),
        terminal.files_changed().to_vec(),
        terminal.diff_stat().map(str::to_owned),
        turns,
        ended_at_millis,
    )
}

fn cancelled_followup_status(status: &TaskStatus) -> Result<TaskStatus, WorkerError> {
    let Some(last) = status.turns().last() else {
        return Err(task_error(
            "TASK_INCONSISTENT",
            "cancelled follow-up has no turn record",
        ));
    };
    if last.terminal().is_some() {
        return Ok(status.clone());
    }
    let ended_at = now_millis()?;
    let replacement = crate::task::TurnSummary::new(
        last.turn_number(),
        last.turn_id(),
        Some(crate::task::TurnTerminal::Cancelled),
        Some(TaskOutcome::Cancelled),
        Some(false),
        false,
        last.started_at_millis().or(Some(ended_at)),
        Some(ended_at),
    );
    let mut turns = status.turns().to_vec();
    let _ = turns.pop();
    turns.push(replacement);
    TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Cancelled),
        status.worker().map(str::to_owned),
        status.session_present(),
        status.head_oid().cloned(),
        status.summary().map(str::to_owned),
        status.questions().to_vec(),
        status.files_changed().to_vec(),
        status.diff_stat().map(str::to_owned),
        turns,
        ended_at,
    )
}

fn task_error(code: &'static str, message: impl Into<String>) -> WorkerError {
    WorkerError::Task {
        code,
        message: message.into(),
    }
}

fn now_millis() -> Result<u64, WorkerError> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| WorkerError::Io(std::io::Error::other("system clock predates Unix epoch")))?
        .as_millis();
    u64::try_from(value)
        .map_err(|_| WorkerError::Io(std::io::Error::other("system clock is out of range")))
}

fn current_process_identity() -> Result<ProcessIdentity, WorkerError> {
    let pid = std::process::id();
    SystemProcessInspector
        .identity_for_pid(pid)
        .or_else(|_| fallback_process_identity(pid))
}

fn fallback_process_identity(pid: u32) -> Result<ProcessIdentity, WorkerError> {
    ProcessIdentity::new(pid, now_millis()?.saturating_mul(1000))
}

#[cfg(test)]
mod tests {
    use super::turn_exit_code;
    use crate::task::TaskOutcome;

    #[test]
    fn agent_failure_exit_code_is_preserved_for_attached_turns() {
        assert_eq!(
            turn_exit_code(&TaskOutcome::Failed {
                reason: "agent exited 17".into(),
            }),
            17
        );
        assert_eq!(
            turn_exit_code(&TaskOutcome::Failed {
                reason: "PUBLISH_FAILED".into(),
            }),
            70
        );
        assert_eq!(turn_exit_code(&TaskOutcome::Blocked), 1);
        assert_eq!(turn_exit_code(&TaskOutcome::Done), 0);
    }
}
