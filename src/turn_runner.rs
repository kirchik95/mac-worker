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
    time::{Duration, SystemTime, UNIX_EPOCH},
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
    scheduler::{CandidateObservation, SchedulerPolicy, WorkerPreference},
    scheduler_adapter::SchedulerProbeAdapter,
    supervisor::SystemProcessInspector,
    task::{LocalTaskRecord, RunnerIdentity, TaskId, TaskOutcome, TaskState, TaskStatus, TurnId},
    task_store::{TaskPrepareRequest, TaskStatusRequest},
    transfer::{RemoteJobClient, TransferIdentity},
    transfer_repo::TransferRepo,
    transport::{SshTransport, WorkersService},
    turn::{TaskTurnRequest, TurnMaterial},
};

const LOG_CHUNK_LIMIT: u32 = 64 * 1024;
const RUNNER_LOG_MODE: u32 = 0o600;
const RUNNER_DIRECTORY_MODE: u32 = 0o700;
const WAIT_POLL: Duration = Duration::from_millis(100);
const MAX_CAPACITY_BACKOFF_SECS: u64 = 30;

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

/// Production executor.  The child has its own session and writes only to
/// the owner-readable runner log.  All task/turn inputs are reloaded by the
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
        let stderr = log.try_clone().map_err(WorkerError::Io)?;
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
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(stderr));
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
        // Legacy batch dispatch recovery deliberately skips task-turn rows,
        // but keeping the call here preserves the runner's recovery stage
        // and lets future mixed queues repair old rows without touching the
        // task-turn ownership protocol.
        let _ = self.client_state.recover_dead_dispatches()?;
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
                if self.can_revert_preacceptance(task_id, turn_id)? {
                    let _ = self.client_state.revert_dispatch(turn_id, owner);
                }
                let _ = self.client_state.record_runner(task_id, None);
                Err(error)
            }
        }
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
        if let Err(error) = self
            .client_state
            .adopt_row(entry.job_id(), identity.process_identity())
        {
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
            && let Ok(project) = ProjectState::load(self.runner, &project_path, &[])
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
                QueueState::Dispatching { dispatch_owner, .. } => return Ok(*dispatch_owner),
                QueueState::Parked => {
                    return Err(capacity_error(
                        "task turn is parked until a runner slot becomes available",
                    ));
                }
                QueueState::Waiting { owner } => {
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
                            .claim_next(*owner, &ranked, now_millis()?)?
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
        let project = ProjectState::load(self.runner, &project_path, &[])?;
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
        let resume = initial_record.status().session_present();
        let turn_limits = initial_record.meta().limits().turn.clone();
        let turn = TurnMaterial::from_prompt(
            task_id,
            turn_number,
            initial_record.meta().agent(),
            initial_record.meta().model().map(str::to_owned),
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
            policy: turn.policy(),
            limits: turn_limits,
            session_seed: turn.session_seed(),
        };
        let adapter = adapter_for(turn.agent());
        let launch = if turn.resume() {
            // Task 7 only accepts first turns.  Keeping this branch explicit
            // makes the Task 8 session binding extension safe to add without
            // changing the queue or fingerprint protocol.
            adapter
                .resume_turn(&params, "$(cat ../session.json)")
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
        let remote = RemoteJobClient::new(self.runner);
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
                let prepared = (|| {
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
                    remote
                        .task_status(
                            worker,
                            &TaskStatusRequest::new(initial_record.meta().project_id(), task_id),
                        )
                        .map_err(WorkerError::from)
                })();
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
                self.persist_status(task_id, prepared.status().clone())?;
                if prepared.status().state().is_terminal()
                    || matches!(prepared.status().state(), TaskState::Open)
                {
                    prepared.status().clone()
                } else {
                    let request = TaskTurnRequest::new(
                        SubmitRequest::new(material.clone()),
                        turn.clone(),
                        prompt.clone(),
                    );
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
                    let status = response.task().clone();
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
            GitTransport::new(self.runner).fetch_result(
                worker,
                self.client_state.client_id(),
                initial_record.meta().project_id(),
                task_id,
                transfer.path(),
            )?;
            Some(
                transfer
                    .import_result(
                        self.runner,
                        &project.context.common_dir,
                        worker.name.as_str(),
                        task_id,
                    )?
                    .head()
                    .clone(),
            )
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
        let exit_code = if matches!(
            outcome,
            TaskOutcome::Done
                | TaskOutcome::NeedsInput
                | TaskOutcome::Blocked
                | TaskOutcome::Unknown
        ) {
            0
        } else {
            1
        };
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
            for (index, stream) in [LogStream::Stdout, LogStream::Stderr]
                .into_iter()
                .enumerate()
            {
                let chunk =
                    remote.log_chunk(worker, turn_id, stream, offsets[index], LOG_CHUNK_LIMIT)?;
                let bytes = chunk.decoded_bytes()?;
                if !bytes.is_empty() {
                    log.file.write_all(&bytes).map_err(WorkerError::Io)?;
                    if let Some(stdout) = follow.as_deref_mut() {
                        stdout.write_all(&bytes).map_err(WorkerError::Io)?;
                        stdout.flush().map_err(WorkerError::Io)?;
                    }
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
            && let Ok(project) = ProjectState::load(self.runner, &project_path, &[])
            && project.context.project_id == record.meta().project_id()
        {
            if let Ok(transfer) =
                TransferRepo::open_or_create(&self.paths.cache, &project.context.common_dir)
            {
                let _ = transfer.release_base(self.runner, record.meta().task_id());
            }
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
                let cached =
                    self.client_state
                        .admission_observation(&worker.name, now_millis()?, || {
                            let health = WorkersService::new(SshTransport::new(self.runner))
                                .inspect(&one)
                                .workers
                                .into_iter()
                                .next()
                                .ok_or_else(|| {
                                    WorkerError::Protocol("worker probe was empty".into())
                                })?;
                            let candidate = SchedulerProbeAdapter::observations(
                                &one,
                                std::slice::from_ref(&health),
                            )?
                            .into_iter()
                            .next()
                            .ok_or_else(|| {
                                WorkerError::Protocol("worker probe was empty".into())
                            })?;
                            AdmissionObservation::new(
                                candidate.worker_name().to_owned(),
                                candidate.ready(),
                                candidate.slot(),
                                candidate.capabilities().to_vec(),
                                candidate.available_memory_bytes(),
                                candidate.free_disk_bytes(),
                                now_millis()?,
                            )
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

struct LogWriter {
    file: File,
}

fn append_event(
    log: &mut LogWriter,
    follow: &mut Option<&mut dyn Write>,
    event: serde_json::Value,
) -> Result<(), WorkerError> {
    let line = serde_json::to_vec(&event)
        .map_err(|error| task_error("TASK_EVENT_INVALID", error.to_string()))?;
    log.file.write_all(&line).map_err(WorkerError::Io)?;
    log.file.write_all(b"\n").map_err(WorkerError::Io)?;
    if let Some(stdout) = follow.as_deref_mut() {
        stdout.write_all(&line).map_err(WorkerError::Io)?;
        stdout.write_all(b"\n").map_err(WorkerError::Io)?;
        stdout.flush().map_err(WorkerError::Io)?;
    }
    Ok(())
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
