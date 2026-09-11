//! Client-side execution of one durable task turn.
//!
//! A task turn is deliberately driven by the local queue owner.  The queue
//! claim is the durable hand-off point; everything after it is retryable from
//! the task and turn records and never relies on a public legacy job record.

use std::{
    io::Write,
    os::unix::process::CommandExt,
    process::{Command, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use uuid::Uuid;

use crate::{
    agent::{TurnParams, adapter_for, render_shell},
    client_state::{ClientStateStore, ReservedSlotTakeover, RunnerSlotDecision},
    config::{Config, WorkerEntry},
    error::WorkerError,
    git_transport::GitTransport,
    job::{
        CommandSpec, ExecutionScope, LeaseAcquireRequest, LeaseAcquireResponse, LeaseRecord,
        LeaseToken, LogCursor, LogStream, ProcessIdentity, QueueEntryKind, QueueState,
        RequestFingerprintMaterial, ResolveOrAbandonRequest, SubmitRequest, TerminalLogDrain,
    },
    paths::PathLayout,
    process::ProcessRunner,
    project_state::ProjectState,
    runner_log::{Completion, RunnerLog as LogWriter},
    scheduler::{CandidateObservation, SchedulerPolicy, WorkerPreference},
    supervisor::SystemProcessInspector,
    task::{
        LocalTaskRecord, RunnerIdentity, TaskId, TaskOutcome, TaskState, TaskStatus, TurnId,
        TurnSummary, TurnTerminal,
    },
    task_store::{
        TaskCancelRequest, TaskPrebindRequest, TaskPrepareRequest, TaskSessionRequest,
        TaskStatusRequest,
    },
    transfer::{RemoteJobClient, TransferIdentity},
    transfer_repo::TransferRepo,
    turn::{TaskTurnRequest, TurnMaterial},
};

const LOG_CHUNK_LIMIT: u32 = 64 * 1024;
const EARLY_EXIT_DIAGNOSTIC_LIMIT: usize = 256;
const WAIT_POLL: Duration = Duration::from_millis(100);
const MAX_CAPACITY_BACKOFF_SECS: u64 = 30;
const RUNNER_HANDOFF_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a runner waits for a waiting row held by another identity to be
/// adopted to it.  The detached parent adopts within `RUNNER_HANDOFF_TIMEOUT`
/// or abandons the handoff; a runner started by hand for a row that belongs
/// to someone else would otherwise wait forever.
const ADOPTION_WAIT: Duration = Duration::from_secs(10);

/// Starts the durable process responsible for one queued task turn.
pub trait RunnerExecutor: Send + Sync {
    fn start(
        &self,
        paths: &PathLayout,
        task_id: TaskId,
        turn_id: TurnId,
    ) -> Result<RunnerIdentity, WorkerError>;

    /// Production detached spawn may carry the reservation token. Default
    /// implementations ignore it and call [`Self::start`].
    fn start_with_slot(
        &self,
        paths: &PathLayout,
        task_id: TaskId,
        turn_id: TurnId,
        slot_token: Option<Uuid>,
    ) -> Result<RunnerIdentity, WorkerError> {
        let _ = slot_token;
        self.start(paths, task_id, turn_id)
    }
}

/// Outcome of a reserved spawn attempt. Only [`Self::Started`] ran `executor.start`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunnerStart {
    Started(RunnerIdentity),
    Pending,
    Saturated,
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
        self.start_with_slot(paths, task_id, turn_id, None)
    }

    fn start_with_slot(
        &self,
        paths: &PathLayout,
        task_id: TaskId,
        turn_id: TurnId,
        slot_token: Option<Uuid>,
    ) -> Result<RunnerIdentity, WorkerError> {
        // Use the same rooted, locked initialization as the child. Never follow
        // a substituted log or chmod an existing target through a pathname.
        let store = ClientStateStore::open(&paths.state)?;
        drop(open_handoff_journal_for_spawn(
            &store, paths, task_id, turn_id, slot_token,
        )?);
        let executable = std::env::current_exe().map_err(WorkerError::Io)?;
        let mut command = Command::new(executable);
        if !paths.config.as_os_str().is_empty() {
            command.arg("--config").arg(&paths.config);
        }
        command
            .arg("runner")
            .arg(task_id.to_string())
            .arg(turn_id.to_string());
        if let Some(token) = slot_token {
            command.arg("--slot-token").arg(token.to_string());
        }
        command
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

/// Whether a queue row still authorizes this parent's spawn attempt.
///
/// Real task cancellation (`retain_task_turn_cancel`) sets only the cancel
/// flag and leaves state and reservation untouched, so a token-only check
/// would spawn for a cancelled turn. Likewise `adopt_row` swaps the owner
/// while keeping the token. Require the full permit: a live TaskTurn row for
/// this turn, no requested cancellation, the row still owned by the identity
/// pinned when the handoff started, and a reservation carrying this attempt's
/// token, still owned by this process, with no child bound yet. A missing row
/// fails closed as well.
fn handoff_spawn_admitted(
    entry: Option<&crate::job::QueueEntry>,
    turn_id: TurnId,
    token: Uuid,
    reserver: ProcessIdentity,
    owner: Option<ProcessIdentity>,
) -> bool {
    let Some(entry) = entry else {
        return false;
    };
    entry.kind() == QueueEntryKind::TaskTurn
        && entry.job_id() == turn_id
        && !entry.is_cancel_requested()
        && entry.owner_opt().copied() == owner
        && entry.slot_reservation().is_some_and(|reservation| {
            reservation.token() == token
                && reservation.child().is_none()
                && reservation.reserver() == reserver
        })
}

/// Bounded pre-spawn journal fence wait for the detached parent.
///
/// A concurrent selected refresh may briefly hold the published journal flock
/// (`LOCK_EX`) while the parent initializes the same journal. That transient
/// `WouldBlock` must not fail a durable handoff, so retry through
/// [`LogWriter::try_open`] within `RUNNER_HANDOFF_TIMEOUT` (strictly below the
/// child `ADOPTION_WAIT`). Only `WouldBlock`/`None` retries; every other open
/// error stays hard, and no lock is held while waiting.
///
/// When the spawn carries a reservation token, the spawn permit is
/// revalidated before every attempt through [`handoff_spawn_admitted`], and
/// again under the acquired journal through `log.current_entry` (the journal
/// flock is the finalization fence: refresh/finalizer mutations of this exact
/// row serialize with it, so only the post-acquire check sees the task's
/// current turn). The row owner is pinned on first sight: adoption away
/// mid-handoff keeps the token but voids this attempt's permit. Cancelled,
/// adopted-away, bound-elsewhere, or retired work therefore returns
/// `TASK_BUSY` at the flock check; the caller then drops the journal before
/// spawn, so a later mutation in that residual window is out of this helper's
/// scope and is caught by the post-bind fence. Bare starts without a token keep
/// the historical behavior (journal only).
fn open_handoff_journal_for_spawn(
    store: &ClientStateStore,
    paths: &PathLayout,
    task_id: TaskId,
    turn_id: TurnId,
    slot_token: Option<Uuid>,
) -> Result<LogWriter, WorkerError> {
    let deadline = Instant::now() + RUNNER_HANDOFF_TIMEOUT;
    // The reservation is taken synchronously by this same process, so its
    // identity is the permit's reserver on every attempt. The row owner is
    // pinned from the first read: a concurrent adopt keeps the token but
    // must still void this attempt.
    let permit = slot_token
        .map(|token| {
            let reserver = current_process_identity()?;
            let owner = store
                .queue_entry(turn_id)?
                .as_ref()
                .and_then(|entry| entry.owner_opt().copied());
            Ok::<_, WorkerError>((token, reserver, owner))
        })
        .transpose()?;
    loop {
        if let Some((token, reserver, owner)) = permit
            && !handoff_spawn_admitted(
                store.queue_entry(turn_id)?.as_ref(),
                turn_id,
                token,
                reserver,
                owner,
            )
        {
            return Err(task_error("TASK_BUSY", "task turn changed before handoff"));
        }
        match LogWriter::try_open(&paths.state, task_id, turn_id)? {
            Some(log) => {
                let admitted = match permit {
                    Some((token, reserver, owner)) => handoff_spawn_admitted(
                        log.current_entry(store)?.as_ref(),
                        turn_id,
                        token,
                        reserver,
                        owner,
                    ),
                    None => true,
                };
                if admitted {
                    return Ok(log);
                }
                drop(log);
                return Err(task_error("TASK_BUSY", "task turn changed before handoff"));
            }
            None if Instant::now() < deadline => {
                store.runner_log_contention();
                std::thread::sleep(WAIT_POLL);
            }
            None => {
                return Err(WorkerError::Io(std::io::Error::from(
                    std::io::ErrorKind::WouldBlock,
                )));
            }
        }
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
    /// The herdr session to tell about finished turns; absent when the
    /// operator turned notifications off or nobody injected a session.
    pub(crate) notifier: Option<crate::herdr::HerdrSocket>,
    adoption_wait: Duration,
    slot_token: Option<Uuid>,
}

impl<'a> TurnRunner<'a> {
    /// Runs a detached queue worker, which may serve another eligible turn.
    pub fn run_detached(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
    ) -> Result<TurnOutcomeReport, WorkerError> {
        self.run_mode(task_id, turn_id, None, true)
    }

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
            notifier: None,
            adoption_wait: ADOPTION_WAIT,
            slot_token: None,
        }
    }

    /// Hidden detached children refuse claim/adopt unless the row still holds
    /// this exact spawn-attempt token.
    pub fn with_slot_token(mut self, token: Option<Uuid>) -> Self {
        self.slot_token = token;
        self
    }

    /// Notify this herdr session when a turn ends.  Nothing reads the
    /// process environment here: the caller decides which session, so tests
    /// and other embedders never reach an operator's herdr by accident.
    pub fn with_notifier(mut self, socket: Option<crate::herdr::HerdrSocket>) -> Self {
        self.notifier = socket;
        self
    }

    /// Bound the wait for a waiting row held by another identity.  Tests
    /// shorten it; the default covers the detached handoff twice over.
    pub fn with_adoption_wait(mut self, wait: Duration) -> Self {
        self.adoption_wait = wait;
        self
    }

    /// Claims and executes exactly one local task-turn queue row.
    pub fn run(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        follow: Option<&mut dyn Write>,
    ) -> Result<TurnOutcomeReport, WorkerError> {
        self.run_mode(task_id, turn_id, follow, false)
    }

    fn run_mode(
        &self,
        mut task_id: TaskId,
        mut turn_id: TurnId,
        mut follow: Option<&mut dyn Write>,
        allow_reassignment: bool,
    ) -> Result<TurnOutcomeReport, WorkerError> {
        ignore_sigpipe();
        // Legacy batch dispatch recovery deliberately skips task-turn rows,
        // but keeping the call here preserves the runner's recovery stage
        // and lets future mixed queues repair old rows without touching the
        // task-turn ownership protocol.
        let _ = self.client_state.recover_dead_dispatches()?;
        let result = (|| {
            let (claimed_task, claimed_turn, owner) =
                self.claim(task_id, turn_id, allow_reassignment)?;
            task_id = claimed_task;
            turn_id = claimed_turn;
            let result = self.execute(task_id, turn_id, owner, &mut follow);
            self.notify_turn_finished(task_id, turn_id, &result);
            match result {
                Ok(report) => {
                    self.start_next_parked(owner)?;
                    Ok(report)
                }
                Err(error) => {
                    // Before the host accepted a job, the dispatch is safe to
                    // retry.  Once accepted, the queue row remains durable and
                    // reconciliation will hand it to a replacement runner.
                    if let Ok(Some(log)) = LogWriter::try_open(&self.paths.state, task_id, turn_id)
                        && log.require_owner(self.client_state, owner).is_ok()
                    {
                        if matches!(self.can_revert_preacceptance(task_id, turn_id), Ok(true)) {
                            let _ = self.client_state.revert_dispatch(turn_id, owner);
                        }
                        let _ = self.client_state.record_runner(task_id, None);
                    }
                    Err(error)
                }
            }
        })();
        if let Err(error) = &result
            && let (Ok(true), Ok(now)) = (
                self.write_early_exit_diagnostic(task_id, turn_id, error),
                now_millis(),
            )
        {
            let _ =
                self.client_state
                    .record_replacement_failure(turn_id, &error.public_code(), now);
        }
        result
    }

    /// One herdr notification per terminal turn, from the durable record,
    /// after everything that matters has been written.  Best effort.
    fn notify_turn_finished(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        result: &Result<TurnOutcomeReport, WorkerError>,
    ) {
        let Some(socket) = &self.notifier else {
            return;
        };
        if !self.config.notifications.herdr {
            return;
        }
        let Ok(record) = self.client_state.load_task(task_id) else {
            return;
        };
        let status = match result {
            Ok(report) => report.status(),
            Err(_) => record.status(),
        };
        let Some(turn) = status.turns().iter().find(|turn| turn.turn_id() == turn_id) else {
            return;
        };
        if turn.terminal().is_none() {
            return;
        }
        let Some(outcome) = turn.outcome().or(status.last_outcome()) else {
            return;
        };
        let _ = crate::herdr_notify::HerdrNotifier::new(socket.clone()).turn_finished(
            task_id,
            record.meta().title().as_str(),
            outcome,
            status.summary(),
            status.questions(),
        );
    }

    fn write_early_exit_diagnostic(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        error: &WorkerError,
    ) -> Result<bool, WorkerError> {
        let mut log = LogWriter::open(&self.paths.state, task_id, turn_id)?;
        let after_acceptance = log.len() > 0;
        let workers = self
            .config
            .workers
            .iter()
            .map(|worker| worker.name.as_str())
            .collect::<Vec<_>>();
        let blocking = self
            .client_state
            .task_blocking_codes(self.config)
            .ok()
            .and_then(|codes| codes.get(&task_id).cloned());
        let public_code = error.public_code();
        let message = crate::redaction::RedactionBoundary::from_env()
            .text(&error.to_string(), EARLY_EXIT_MESSAGE_LIMIT);
        if let Some(receipt) = error.failure_receipt()
            && let Ok(record) = self.client_state.load_task(task_id)
            && let Ok(updated) = record.with_failure_receipt(Some(receipt))
        {
            let _ = self.client_state.update_task(updated);
        }
        log.append_bytes(
            early_exit_diagnostic_line(
                after_acceptance,
                blocking.as_deref(),
                &public_code,
                &message,
                &workers,
                error.failure_receipt().as_ref(),
            )
            .as_bytes(),
        )?;
        Ok(after_acceptance)
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
        match start_runner_with_reservation(
            self.client_state,
            self.executor,
            self.paths,
            task_id,
            entry.job_id(),
            self.config.configured_runner_slots(),
            true,
        ) {
            Ok(RunnerStart::Started(_)) | Ok(RunnerStart::Pending) => Ok(()),
            Ok(RunnerStart::Saturated) => {
                self.client_state.park_row(entry.job_id())?;
                Ok(())
            }
            Err(error) => {
                self.abandon_handoff_failure(task_id, entry.job_id(), owner)?;
                Err(task_error(
                    "RUNNER_HANDOFF_FAILED",
                    format!("runner handoff failed: {error}"),
                ))
            }
        }
    }

    fn abandon_handoff_failure(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        owner: ProcessIdentity,
    ) -> Result<(), WorkerError> {
        let Some(mut log) = LogWriter::try_open(&self.paths.state, task_id, turn_id)? else {
            return Ok(());
        };
        log.require_owner(self.client_state, owner)?;
        if self
            .client_state
            .cancel_unstarted_handoff(turn_id, owner, now_millis()?)?
            .is_none()
        {
            return Ok(());
        }
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
        log.finish_local(
            task_id,
            turn_id,
            TaskOutcome::failed("RUNNER_HANDOFF_FAILED"),
        )?;
        if let Ok(project) = self.load_project_for_record(&record)
            && project.context.project_id == record.meta().project_id()
            && let Ok(transfer) =
                crate::controller::registry::open_transfer_repo(self.paths, &project, record.meta())
        {
            transfer.release_base(self.runner, task_id)?;
            self.client_state.record_runner(task_id, None)?;
            let _ = self
                .client_state
                .remove_task_turn_after_terminal(turn_id, owner)?;
            let _ = self.client_state.remove_turn_prompt(task_id, turn_id);
        }
        Ok(())
    }

    fn claim(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        allow_reassignment: bool,
    ) -> Result<(TaskId, TurnId, ProcessIdentity), WorkerError> {
        let mut backoff = 1_u64;
        let runner_owner = current_process_identity()?;
        let adoption_deadline = Instant::now() + self.adoption_wait;
        loop {
            let record = self.client_state.load_task(task_id)?;
            let mut entry = self
                .client_state
                .queue_entry(turn_id)?
                .ok_or_else(|| task_error("TASK_QUEUE_MISSING", "task turn is not queued"))?;
            if entry.kind() != QueueEntryKind::TaskTurn {
                return Err(task_error(
                    "TASK_QUEUE_KIND",
                    "queue row is not a task turn",
                ));
            }
            if let Some(expected) = self.slot_token {
                match self
                    .client_state
                    .take_over_reserved_slot(turn_id, expected, runner_owner)?
                {
                    ReservedSlotTakeover::Ready => {
                        entry = self.client_state.queue_entry(turn_id)?.ok_or_else(|| {
                            task_error("TASK_QUEUE_MISSING", "task turn is not queued")
                        })?;
                    }
                    ReservedSlotTakeover::WaitingForLiveOwner => {
                        if Instant::now() >= adoption_deadline {
                            return Err(queue_error(
                                "QUEUE_SLOT_TOKEN_MISMATCH",
                                "runner slot token is no longer current",
                            ));
                        }
                        std::thread::sleep(WAIT_POLL);
                        continue;
                    }
                }
            }
            if crate::runner_log::snapshot(&self.paths.state, task_id, turn_id)?
                .is_some_and(|s| s.completion.is_some())
            {
                if entry.owner_opt() != Some(&runner_owner) {
                    return Err(queue_error(
                        "QUEUE_OWNER_MISMATCH",
                        "completed turn cleanup belongs to another runner",
                    ));
                }
                return Ok((task_id, turn_id, runner_owner));
            }
            if entry.is_cancel_requested()
                && !matches!(entry.state(), QueueState::Dispatching { .. })
            {
                self.cancel_before_acceptance(
                    &record,
                    task_id,
                    turn_id,
                    entry.owner_opt().copied().unwrap_or(runner_owner),
                    None,
                )?;
                return Err(task_error(
                    "TASK_CANCELLED",
                    "turn cancelled before admission",
                ));
            }
            match entry.state() {
                QueueState::Dispatching { dispatch_owner, .. }
                    if *dispatch_owner == runner_owner =>
                {
                    return Ok((task_id, turn_id, *dispatch_owner));
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
                        // A row nobody adopts to this runner belongs to
                        // another one; stop rather than wait forever.
                        if Instant::now() >= adoption_deadline {
                            return Err(queue_error(
                                "QUEUE_OWNER_MISMATCH",
                                "task turn is waiting under another runner; `worker task reconcile` re-owns a dead one",
                            ));
                        }
                        std::thread::sleep(WAIT_POLL);
                        continue;
                    }
                    let may_yield = allow_reassignment && record.wait_for_capacity();
                    let mut observations = self.observe_admission(entry.preference())?;
                    let affinity = self
                        .client_state
                        .affinity_hints(entry.project_id(), entry.worktree_id())?;
                    let ranked =
                        SchedulerPolicy::rank(&observations, entry.requirements(), &affinity)
                            .into_iter()
                            .filter(|candidate| match entry.preference() {
                                WorkerPreference::Automatic => true,
                                WorkerPreference::Pinned { worker } => {
                                    candidate.worker_name() == worker
                                }
                            })
                            .map(|candidate| candidate.worker_name().to_owned())
                            .collect::<Vec<_>>();
                    if let Some(claim) = self.client_state.claim_task_turn_with_slot_ceilings(
                        runner_owner,
                        turn_id,
                        &ranked,
                        now_millis()?,
                        &self.config.worker_slot_ceilings(),
                    )? {
                        return match claim.entry().state() {
                            QueueState::Dispatching { dispatch_owner, .. } => {
                                Ok((task_id, turn_id, *dispatch_owner))
                            }
                            _ => Err(task_error(
                                "TASK_QUEUE_STATE",
                                "queue claim did not produce a dispatch",
                            )),
                        };
                    }
                    if !record.wait_for_capacity() {
                        self.abandon_capacity(&record, turn_id, *owner, None)?;
                        return Err(capacity_busy());
                    }
                    if may_yield
                        && self
                            .client_state
                            .queue_snapshot()?
                            .entries()
                            .iter()
                            .any(|entry| matches!(entry.state(), QueueState::Parked))
                    {
                        // A ready pinned turn takes its fast path above. Only
                        // a blocked donor with parked work needs observations
                        // for the remaining fleet; keep its existing sample.
                        let missing = self
                            .config
                            .workers
                            .iter()
                            .filter(|worker| {
                                !observations
                                    .iter()
                                    .any(|observation| observation.worker_name() == worker.name)
                            })
                            .collect::<Vec<_>>();
                        if !missing.is_empty() {
                            observations.extend(crate::admission::observe_workers(
                                self.runner,
                                self.config,
                                self.client_state,
                                &missing,
                            )?);
                        }
                        if let Some((next_task, claim)) =
                            self.client_state.claim_parked_for_waiting_runner(
                                task_id,
                                turn_id,
                                runner_owner,
                                &observations,
                                now_millis()?,
                            )?
                        {
                            return Ok((next_task, claim.entry().job_id(), runner_owner));
                        }
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
        // Refresh and parent handoff publication briefly hold the same fence.
        // Wait without holding state locks, but never wait for another turn or
        // owner: retirement/reassignment revokes this runner's authority.
        let mut log = loop {
            if !self
                .client_state
                .queue_entry_for_task_turn(task_id)?
                .is_some_and(|entry| entry.job_id() == turn_id && entry.owner_opt() == Some(&owner))
            {
                return Err(task_error(
                    "TASK_BUSY",
                    "task turn ownership changed while waiting for its journal",
                ));
            }
            if let Some(log) = LogWriter::try_open(&self.paths.state, task_id, turn_id)? {
                log.require_owner(self.client_state, owner)?;
                break log;
            }
            self.client_state.runner_log_contention();
            std::thread::sleep(WAIT_POLL);
        };
        let initial_record = self.client_state.load_task(task_id)?;
        let project = self.load_project_for_record(&initial_record)?;
        require_project_match(
            &project,
            initial_record.meta().project_id(),
            initial_record.meta().worktree_id(),
        )?;
        let transfer = crate::controller::registry::open_transfer_repo(
            self.paths,
            &project,
            initial_record.meta(),
        )?;
        {
            if let Some(completion) = log.completion() {
                let exit_code = turn_exit_code(&completion.outcome);
                let status = finalize_completed_turn(
                    self.client_state,
                    self.runner,
                    self.config,
                    self.paths,
                    task_id,
                    turn_id,
                    owner,
                    completion,
                )?;
                return Ok(TurnOutcomeReport {
                    status,
                    events: vec![],
                    exit_code,
                });
            }
        }
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
        // Acceptance is the durable signal that the host already took the
        // turn. A replacement runner must drain and import from the committed
        // offsets rather than submit again, even if the prompt file is still
        // on disk. TaskStatus alone is never treated as drain evidence.
        if log.is_accepted() {
            return self.execute_accepted_without_prompt(
                task_id,
                turn_id,
                owner,
                &initial_record,
                &project,
                transfer,
                worker,
                &mut log,
                follow,
            );
        }
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
                    &mut log,
                    follow,
                );
            }
            Err(error) => return Err(error),
        };
        if self
            .client_state
            .queue_entry(turn_id)?
            .is_some_and(|entry| entry.is_cancel_requested())
            && self.can_revert_preacceptance(task_id, turn_id)?
        {
            self.cancel_before_acceptance(
                &initial_record,
                task_id,
                turn_id,
                owner,
                Some(&mut log),
            )?;
            return Err(task_error(
                "TASK_CANCELLED",
                "task turn was cancelled before host acceptance",
            ));
        }
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
        let execution_scope = ExecutionScope::task(task_id);
        let lease_request = LeaseAcquireRequest::new(material.clone())
            .with_execution_scope(execution_scope.clone());
        let acquired = match remote.lease_acquire(worker, &lease_request) {
            Ok(acquired) => acquired,
            Err(error)
                if !initial_record.wait_for_capacity()
                    && error.public_code() == "CAPACITY_BUSY" =>
            {
                self.abandon_capacity(&initial_record, turn_id, owner, Some(&mut log))?;
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
                    self.cancel_before_acceptance(
                        &initial_record,
                        task_id,
                        turn_id,
                        owner,
                        Some(&mut log),
                    )?;
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
                        if !matches!(
                            initial_record.meta().source(),
                            crate::task::TaskSource::Origin { .. }
                        ) {
                            GitTransport::new(self.runner).push_base(
                                worker,
                                &identity,
                                initial_record.meta().project_id(),
                                task_id,
                                turn.base_oid(),
                                transfer.path(),
                            )?;
                        }
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
                            self.abandon_capacity(&initial_record, turn_id, owner, Some(&mut log))?;
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
                    self.cancel_before_acceptance(
                        &initial_record,
                        task_id,
                        turn_id,
                        owner,
                        Some(&mut log),
                    )?;
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
                        SubmitRequest::new(material.clone())
                            .with_execution_scope(execution_scope.clone()),
                        turn.clone(),
                        prompt.clone(),
                        turn_origin_url(initial_record.meta()),
                    )?
                    .with_herdr_reporter(worker.herdr);
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
                                self.abandon_capacity(
                                    &initial_record,
                                    turn_id,
                                    owner,
                                    Some(&mut log),
                                )?;
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

        append_event(
            &mut log,
            follow,
            serde_json::json!({"type":"turn_accepted","protocol_version":crate::protocol::PROTOCOL_VERSION,"task_id":task_id,"turn_id":turn_id,"worker":worker.name}),
        )?;
        let terminal =
            match self.follow_remote(worker, task_id, turn_id, task_status, &mut log, follow) {
                Ok(status) => status,
                Err(error) => {
                    return Err(self.recover_undrainable(task_id, turn_id, owner, &mut log, error));
                }
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
        log: &mut LogWriter,
        follow: &mut Option<&mut dyn Write>,
    ) -> Result<TurnOutcomeReport, WorkerError> {
        let remote = RemoteJobClient::new(self.runner);
        let status = match remote.task_status(
            worker,
            &TaskStatusRequest::new(initial_record.meta().project_id(), task_id),
        ) {
            Ok(response) => response.status().clone(),
            Err(error) => {
                return Err(self.recover_undrainable(task_id, turn_id, owner, log, error));
            }
        };
        self.persist_status(task_id, status.clone())?;
        append_event(
            log,
            follow,
            serde_json::json!({"type":"turn_accepted","protocol_version":crate::protocol::PROTOCOL_VERSION,"task_id":task_id,"turn_id":turn_id,"worker":worker.name}),
        )?;
        let terminal = match self.follow_remote(worker, task_id, turn_id, status, log, follow) {
            Ok(status) => status,
            Err(error) => {
                return Err(self.recover_undrainable(task_id, turn_id, owner, log, error));
            }
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
            log,
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
        self.client_state.update_task(record)?;
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
        transfer.release_base(self.runner, task_id)?;
        drop(transfer);
        self.client_state.record_runner(task_id, None)?;
        self.client_state
            .remove_task_turn_after_terminal(turn_id, owner)?;
        self.client_state.remove_turn_prompt(task_id, turn_id)?;
        self.advance_pending_dags_after_transfer_drop()?;
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
            .update_task(record.with_status(status.clone())?)?;
        let event = serde_json::json!({
            "type": "turn_terminal",
            "protocol_version": crate::protocol::PROTOCOL_VERSION,
            "task_id": task_id.to_string(),
            "turn_id": turn_id.to_string(),
            "outcome": outcome,
        });
        append_event(log, follow, event.clone())?;
        transfer.release_base(self.runner, task_id)?;
        drop(transfer);
        self.client_state.record_runner(task_id, None)?;
        self.client_state
            .remove_task_turn_after_terminal(turn_id, owner)?;
        self.client_state.remove_turn_prompt(task_id, turn_id)?;
        self.advance_pending_dags_after_transfer_drop()?;
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
        let record = self.client_state.load_task(task_id)?;
        let offsets = log.offsets();
        let mut drain = TerminalLogDrain::new(
            LogCursor::new(LogStream::Stdout, offsets[0], LOG_CHUNK_LIMIT)?,
            LogCursor::new(LogStream::Stderr, offsets[1], LOG_CHUNK_LIMIT)?,
        )?;
        loop {
            if self.queue_cancel_requested(turn_id)? && status.state() == TaskState::Active {
                status = remote
                    .task_cancel(
                        worker,
                        &TaskCancelRequest::new(record.meta().project_id(), task_id, turn_id),
                    )?
                    .status()
                    .clone();
                self.persist_status(task_id, status.clone())?;
            }
            let combined = remote.try_combined_status_and_logs(
                worker,
                turn_id,
                drain.cursor(LogStream::Stdout).next_offset(),
                LOG_CHUNK_LIMIT,
                drain.cursor(LogStream::Stderr).next_offset(),
                LOG_CHUNK_LIMIT,
            )?;
            let mut progressed = false;
            if let Some((job, stdout_chunk, stderr_chunk)) = combined {
                self.validate_follow_job(&record, worker, turn_id, &job)?;
                if job.status().state().is_terminal() {
                    drain.set_terminal_status(&job)?;
                }
                progressed |= Self::apply_log_chunk(&mut drain, log, follow, stdout_chunk)?;
                progressed |= Self::apply_log_chunk(&mut drain, log, follow, stderr_chunk)?;
            } else {
                let job = remote.status(worker, turn_id)?;
                self.validate_follow_job(&record, worker, turn_id, &job)?;
                if job.status().state().is_terminal() {
                    drain.set_terminal_status(&job)?;
                }
                for stream in [LogStream::Stdout, LogStream::Stderr] {
                    let chunk = remote.log_chunk(
                        worker,
                        turn_id,
                        stream,
                        drain.cursor(stream).next_offset(),
                        LOG_CHUNK_LIMIT,
                    )?;
                    progressed |= Self::apply_log_chunk(&mut drain, log, follow, chunk)?;
                }
            }
            if drain.cursor(LogStream::Stdout).is_drained()
                && drain.cursor(LogStream::Stderr).is_drained()
            {
                drain.revalidate_terminal_status(&remote.status(worker, turn_id)?)?;
                status = remote
                    .task_status(
                        worker,
                        &TaskStatusRequest::new(record.meta().project_id(), task_id),
                    )?
                    .status()
                    .clone();
                self.persist_status(task_id, status.clone())?;
                if status.state().is_terminal() || status.state() == TaskState::Open {
                    return Ok(status);
                }
            }
            if !progressed {
                std::thread::sleep(WAIT_POLL);
            }
            status = remote
                .task_status(
                    worker,
                    &TaskStatusRequest::new(record.meta().project_id(), task_id),
                )?
                .status()
                .clone();
            self.persist_status(task_id, status.clone())?;
        }
    }

    fn validate_follow_job(
        &self,
        record: &LocalTaskRecord,
        worker: &WorkerEntry,
        turn_id: TurnId,
        job: &crate::job::StatusResponse,
    ) -> Result<(), WorkerError> {
        if job.meta().job_id() != turn_id
            || job.meta().worker_name() != worker.name
            || job.meta().client_id() != self.client_state.client_id()
            || job.meta().project_id() != record.meta().project_id()
            || job.meta().worktree_id() != record.meta().worktree_id()
        {
            return Err(task_error(
                "LOG_JOB_IDENTITY_MISMATCH",
                "remote job differs from the selected turn",
            ));
        }
        Ok(())
    }

    fn apply_log_chunk(
        drain: &mut crate::job::TerminalLogDrain,
        log: &mut LogWriter,
        follow: &mut Option<&mut dyn Write>,
        chunk: crate::job::LogChunk,
    ) -> Result<bool, WorkerError> {
        let mut next = drain.clone();
        next.observe_chunk(&chunk)?;
        let bytes = chunk.decoded_bytes()?;
        log.append_chunk(&chunk)?;
        *drain = next;
        write_follower(follow, &bytes);
        Ok(!bytes.is_empty())
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

    /// JOB_NOT_FOUND / TASK_NOT_FOUND after acceptance means remaining
    /// stdout/stderr cannot be proven. Finish the journal as an explicit
    /// undrainable failure (`drained=false`) so `task logs -f` stops, then
    /// publish local Failed/abandon_code through the shared finalizer. A crash
    /// after the journal write is recovered by the same finalizer.
    fn recover_undrainable(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        owner: ProcessIdentity,
        log: &mut LogWriter,
        error: WorkerError,
    ) -> WorkerError {
        let mapped = map_log_drain_unavailable(error);
        if mapped.public_code() == "LOG_DRAIN_UNAVAILABLE"
            && let Err(persist_error) =
                self.persist_log_drain_unavailable(task_id, turn_id, owner, log)
        {
            return persist_error;
        }
        mapped
    }

    fn persist_log_drain_unavailable(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        owner: ProcessIdentity,
        log: &mut LogWriter,
    ) -> Result<(), WorkerError> {
        let workers = self
            .config
            .workers
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>();
        let blocking = self
            .client_state
            .task_blocking_codes(self.config)
            .ok()
            .and_then(|codes| codes.get(&task_id).cloned());
        let diagnostic = task_error(
            "LOG_DRAIN_UNAVAILABLE",
            "worker job or logs are gone; remaining stdout/stderr cannot be drained",
        );
        let message = crate::redaction::RedactionBoundary::from_env()
            .text(&diagnostic.to_string(), EARLY_EXIT_MESSAGE_LIMIT);
        let line = early_exit_diagnostic_line(
            log.len() > 0,
            blocking.as_deref(),
            "LOG_DRAIN_UNAVAILABLE",
            &message,
            &workers,
            None,
        );
        let outcome = TaskOutcome::failed("LOG_DRAIN_UNAVAILABLE");
        log.finish(
            Completion {
                outcome: outcome.clone(),
                drained: false,
            },
            line.as_bytes(),
        )?;
        let completion = log.completion().cloned().ok_or_else(|| {
            task_error("LOG_CHECKPOINT_INVALID", "undrainable journal is missing")
        })?;
        finalize_completed_turn(
            self.client_state,
            self.runner,
            self.config,
            self.paths,
            task_id,
            turn_id,
            owner,
            &completion,
        )?;
        Ok(())
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
        log: Option<&mut LogWriter>,
    ) -> Result<(), WorkerError> {
        let task_id = record.meta().task_id();
        let mut owned_log;
        let log = match log {
            Some(log) => log,
            None => {
                owned_log = LogWriter::open(&self.paths.state, task_id, turn_id)?;
                &mut owned_log
            }
        };
        log.require_owner(self.client_state, owner)?;
        let record = &self.client_state.load_task(task_id)?;
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
        let workers = self
            .config
            .workers
            .iter()
            .map(|w| w.name.as_str())
            .collect::<Vec<_>>();
        let line = early_exit_diagnostic_line(
            false,
            Some("CAPACITY_BUSY"),
            "CAPACITY_BUSY",
            "",
            &workers,
            None,
        );
        let completion = Completion {
            outcome: TaskOutcome::failed("CAPACITY_BUSY"),
            drained: false,
        };
        log.finish(completion, line.as_bytes())?;
        if let Ok(project) = self.load_project_for_record(record)
            && project.context.project_id == record.meta().project_id()
            && let Ok(transfer) =
                crate::controller::registry::open_transfer_repo(self.paths, &project, record.meta())
        {
            transfer.release_base(self.runner, record.meta().task_id())?;
            self.client_state
                .record_runner(record.meta().task_id(), None)?;
            let _ = self
                .client_state
                .remove_task_turn_after_terminal(turn_id, owner)?;
            let _ = self
                .client_state
                .remove_turn_prompt(record.meta().task_id(), turn_id);
        }
        Ok(())
    }

    fn cancel_before_acceptance(
        &self,
        _record: &LocalTaskRecord,
        task_id: TaskId,
        turn_id: TurnId,
        owner: ProcessIdentity,
        log: Option<&mut LogWriter>,
    ) -> Result<(), WorkerError> {
        let mut owned_log;
        let log = match log {
            Some(log) => log,
            None => {
                owned_log = LogWriter::open(&self.paths.state, task_id, turn_id)?;
                &mut owned_log
            }
        };
        let entry = log
            .current_entry(self.client_state)?
            .ok_or_else(|| task_error("TASK_BUSY", "task turn was retired"))?;
        if entry.owner_opt().is_some_and(|current| *current != owner) {
            return Err(task_error("TASK_BUSY", "task turn ownership changed"));
        }
        let record = &self.client_state.load_task(task_id)?;
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
                .with_abandon_code(abandoned.then_some("CANCELLED".to_owned()))?,
        )?;
        log.finish_local(task_id, turn_id, TaskOutcome::Cancelled)?;
        if let Ok(project) = self.load_project_for_record(record)
            && project.context.project_id == record.meta().project_id()
            && let Ok(transfer) =
                crate::controller::registry::open_transfer_repo(self.paths, &project, record.meta())
        {
            transfer.release_base(self.runner, task_id)?;
            self.client_state.record_runner(task_id, None)?;
            let _ = self
                .client_state
                .remove_task_turn_after_terminal(turn_id, owner)?;
            let _ = self.client_state.remove_turn_prompt(task_id, turn_id);
        }
        Ok(())
    }

    fn advance_pending_dags_after_transfer_drop(&self) -> Result<(), WorkerError> {
        crate::task_client::TaskClient::new(
            self.runner,
            self.config,
            self.paths,
            self.client_state,
            self.executor,
        )
        .advance_pending_dags()
    }

    fn load_project_for_record(
        &self,
        record: &LocalTaskRecord,
    ) -> Result<ProjectState, WorkerError> {
        let frozen = self.client_state.frozen_spec_for_record(record)?;
        let project = ProjectState::load_validated_for_task(
            self.runner,
            self.paths,
            frozen.as_ref(),
            || {
                crate::controller::registry::load_registered_or_local(
                    self.runner,
                    self.paths,
                    &self.task_project_path(record)?,
                    &[],
                    record.meta(),
                )
            },
        )?;
        require_project_match(
            &project,
            record.meta().project_id(),
            record.meta().worktree_id(),
        )?;
        Ok(project)
    }

    fn task_project_path(
        &self,
        record: &LocalTaskRecord,
    ) -> Result<std::path::PathBuf, WorkerError> {
        match self.client_state.task_project_path(record)? {
            Some(path) => Ok(path),
            None => std::env::current_dir().map_err(WorkerError::Io),
        }
    }

    fn observe_admission(
        &self,
        preference: &WorkerPreference,
    ) -> Result<Vec<CandidateObservation>, WorkerError> {
        crate::admission::observe_admission(self.runner, self.config, self.client_state, preference)
    }
}

/// Journal completion is not by itself proof that local status, abandon_code,
/// fetched_head, or the base pin were published. Both reconcile and a replacement
/// runner must run this finalizer: restore an undrainable Failed record when
/// needed, import this turn's result, release the pin only after that import,
/// then retire the row.
///
/// `say` refuses with TASK_BUSY while this queue row remains, so a follow-up
/// cannot appear until retirement. `fetched_head` is still kept across
/// `with_status`/`say` as last successful import history; it is not proof that
/// the current turn's result was imported. Publication uses expected-record CAS
/// across the remote fetch, and rewrites only this journal's turn.
#[allow(clippy::too_many_arguments)]
pub(crate) fn finalize_completed_turn(
    client_state: &ClientStateStore,
    runner: &dyn ProcessRunner,
    config: &Config,
    paths: &PathLayout,
    task_id: TaskId,
    turn_id: TurnId,
    owner: ProcessIdentity,
    completion: &Completion,
) -> Result<TaskStatus, WorkerError> {
    let record = client_state.load_task(task_id)?;
    let (project, transfer) = transfer_for_completed_turn(client_state, runner, paths, &record)?;
    if completion.is_undrainable() {
        let imported = try_import_completed_result(
            client_state,
            runner,
            config,
            &project,
            &transfer,
            &record,
            task_id,
        );
        let mut next = record.clone();
        if !record.retains_log_drain_unavailable() {
            let status = undrainable_failure_status(record.status(), turn_id, now_millis()?)?;
            next = next
                .with_status(status)?
                .with_abandon_code(Some("LOG_DRAIN_UNAVAILABLE".into()))?;
        }
        if let Some(head) = imported.clone() {
            next = next.with_fetched_head(Some(head))?;
        }
        if next != record {
            client_state.undrainable_record_fault()?;
            if !client_state.update_task_if_current(&record, next)? {
                return Err(task_error(
                    "TASK_BUSY",
                    "task record changed during undrainable finalization",
                ));
            }
        }
        let published = client_state.load_task(task_id)?;
        if imported.is_some() {
            transfer.release_base(runner, task_id)?;
        }
        retire_completed_turn(client_state, turn_id, owner, task_id)?;
        return Ok(published.status().clone());
    }
    transfer.release_base(runner, task_id)?;
    retire_completed_turn(client_state, turn_id, owner, task_id)?;
    Ok(client_state.load_task(task_id)?.status().clone())
}

fn transfer_for_completed_turn(
    client_state: &ClientStateStore,
    runner: &dyn ProcessRunner,
    paths: &PathLayout,
    record: &LocalTaskRecord,
) -> Result<(ProjectState, TransferRepo), WorkerError> {
    let project_path = match client_state.task_project_path(record)? {
        Some(path) => path,
        None => std::env::current_dir().map_err(WorkerError::Io)?,
    };
    let project = crate::controller::registry::load_registered_or_local(
        runner,
        paths,
        &project_path,
        &[],
        record.meta(),
    )?;
    require_project_match(
        &project,
        record.meta().project_id(),
        record.meta().worktree_id(),
    )?;
    let transfer = crate::controller::registry::open_transfer_repo(paths, &project, record.meta())?;
    Ok((project, transfer))
}

fn try_import_completed_result(
    client_state: &ClientStateStore,
    runner: &dyn ProcessRunner,
    config: &Config,
    project: &ProjectState,
    transfer: &TransferRepo,
    record: &LocalTaskRecord,
    task_id: TaskId,
) -> Option<crate::task::BaseOid> {
    let worker_name = record
        .status()
        .worker()
        .or_else(|| record.pinned_worker())?;
    let worker = config.worker(worker_name)?;
    GitTransport::new(runner)
        .fetch_result(
            worker,
            client_state.client_id(),
            record.meta().project_id(),
            task_id,
            transfer.path(),
        )
        .ok()
        .and_then(|_| {
            transfer
                .import_result(
                    runner,
                    &project.context.common_dir,
                    worker.name.as_str(),
                    task_id,
                )
                .ok()
                .map(|receipt| receipt.head().clone())
        })
}

fn retire_completed_turn(
    client_state: &ClientStateStore,
    turn_id: TurnId,
    owner: ProcessIdentity,
    task_id: TaskId,
) -> Result<(), WorkerError> {
    client_state.record_runner(task_id, None)?;
    client_state.remove_task_turn_after_terminal(turn_id, owner)?;
    client_state.remove_turn_prompt(task_id, turn_id)?;
    Ok(())
}

fn undrainable_failure_status(
    terminal: &TaskStatus,
    turn_id: TurnId,
    ended_at_millis: u64,
) -> Result<TaskStatus, WorkerError> {
    let last = terminal.turns().last().ok_or_else(|| {
        task_error(
            "TASK_INCONSISTENT",
            "undrainable journal has no turn to publish",
        )
    })?;
    if last.turn_id() != turn_id {
        return Err(task_error(
            "TASK_BUSY",
            "a newer turn exists; refusing to rewrite it",
        ));
    }
    let outcome = TaskOutcome::failed("LOG_DRAIN_UNAVAILABLE");
    let mut turns = terminal.turns().to_vec();
    let replacement = TurnSummary::new(
        last.turn_number(),
        last.turn_id(),
        Some(TurnTerminal::Failed),
        Some(outcome.clone()),
        last.agent_committed(),
        last.log_truncated(),
        last.started_at_millis(),
        Some(ended_at_millis),
    );
    *turns
        .last_mut()
        .expect("cloned last turn must remain present") = replacement;
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

/// Reserve a spawn permit, start only on [`RunnerSlotDecision::Acquired`], then
/// adopt the child and clear that token. `Pending` and `Saturated` never spawn.
pub fn start_runner_with_reservation(
    client_state: &ClientStateStore,
    executor: &dyn RunnerExecutor,
    paths: &PathLayout,
    task_id: TaskId,
    turn_id: TurnId,
    slot_limit: usize,
    exclude_reserver: bool,
) -> Result<RunnerStart, WorkerError> {
    let reserver = current_process_identity()?;
    match client_state.reserve_runner_slot(turn_id, reserver, slot_limit, exclude_reserver)? {
        RunnerSlotDecision::Saturated => Ok(RunnerStart::Saturated),
        RunnerSlotDecision::Pending { .. } => Ok(RunnerStart::Pending),
        RunnerSlotDecision::Acquired { token } => {
            let expected = client_state
                .queue_entry(turn_id)?
                .ok_or_else(|| task_error("TASK_BUSY", "task turn was retired before handoff"))?;
            match executor.start_with_slot(paths, task_id, turn_id, Some(token)) {
                Ok(identity) => {
                    let child = identity.process_identity();
                    if let Err(error) = client_state.bind_runner_slot_child(turn_id, token, child) {
                        let _ = client_state.release_runner_slot(turn_id, token, reserver);
                        return Err(error);
                    }
                    let expected = match token_fenced_row_after_bind(expected, token, child) {
                        Ok(expected) => expected,
                        Err(error) => {
                            let _ = client_state.release_runner_slot(turn_id, token, reserver);
                            return Err(error);
                        }
                    };
                    let log = open_handoff_journal_after_bind(
                        client_state,
                        paths,
                        task_id,
                        turn_id,
                        token,
                        reserver,
                        &expected,
                    )?;
                    if log.current_entry(client_state)?.as_ref() != Some(&expected) {
                        let _ = client_state.release_runner_slot(turn_id, token, reserver);
                        return Err(task_error("TASK_BUSY", "task turn changed during handoff"));
                    }
                    match client_state.complete_runner_spawn(task_id, turn_id, token, child) {
                        Ok(_) => Ok(RunnerStart::Started(identity)),
                        Err(error) => {
                            let _ = client_state.release_runner_slot(turn_id, token, reserver);
                            Err(error)
                        }
                    }
                }
                Err(error) => {
                    let _ = client_state.release_runner_slot(turn_id, token, reserver);
                    Err(error)
                }
            }
        }
    }
}

/// Bounded post-bind journal fence wait for the reserving parent.
///
/// After spawn and `bind_runner_slot_child` the parent reopens the journal to
/// prove the exact fenced row before `complete_runner_spawn`. A concurrent
/// selected refresh may briefly hold that flock; retry the transient
/// `WouldBlock` through [`LogWriter::try_open`] within
/// `RUNNER_HANDOFF_TIMEOUT` without rerunning the executor and without
/// holding state locks while waiting. The fenced row is revalidated before
/// every attempt: a changed or retired turn goes `TASK_BUSY` through the
/// existing branch instead of completing stale work. The release call there
/// is intentionally preserved but is a no-op once a child is bound
/// (`release_runner_slot` only clears childless reservations held by this
/// reserver), so the bound permit stays with the live child. A genuine
/// timeout likewise keeps the bound reservation; releasing it here would
/// double-allocate the permit, so `WouldBlock` propagates exactly as the
/// old hard open did.
fn open_handoff_journal_after_bind(
    client_state: &ClientStateStore,
    paths: &PathLayout,
    task_id: TaskId,
    turn_id: TurnId,
    token: Uuid,
    reserver: ProcessIdentity,
    expected: &crate::job::QueueEntry,
) -> Result<LogWriter, WorkerError> {
    let deadline = Instant::now() + RUNNER_HANDOFF_TIMEOUT;
    loop {
        if client_state.queue_entry(turn_id)?.as_ref() != Some(expected) {
            let _ = client_state.release_runner_slot(turn_id, token, reserver);
            return Err(task_error("TASK_BUSY", "task turn changed during handoff"));
        }
        match LogWriter::try_open(&paths.state, task_id, turn_id)? {
            Some(log) => return Ok(log),
            None if Instant::now() < deadline => {
                client_state.runner_log_contention();
                std::thread::sleep(WAIT_POLL);
            }
            None => {
                return Err(WorkerError::Io(std::io::Error::from(
                    std::io::ErrorKind::WouldBlock,
                )));
            }
        }
    }
}

/// Bind is the only mutation allowed between reserve and complete. Compare
/// against the pre-spawn row with this token's child attached, not the
/// unbound snapshot.
fn token_fenced_row_after_bind(
    mut expected: crate::job::QueueEntry,
    token: Uuid,
    child: ProcessIdentity,
) -> Result<crate::job::QueueEntry, WorkerError> {
    match expected.slot_reservation() {
        Some(reservation) if reservation.token() == token => {
            expected.set_slot_reservation(Some(reservation.with_child(child)?))?;
            Ok(expected)
        }
        _ => Err(task_error("TASK_BUSY", "task turn changed during handoff")),
    }
}

/// Transfer a waiting or dispatching task row to the process that will
/// execute it. A detached child can start before its parent reaches this
/// write, so the child-side claim path waits for its own identity to appear.
#[allow(dead_code)]
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

fn append_event(
    log: &mut LogWriter,
    follow: &mut Option<&mut dyn Write>,
    event: serde_json::Value,
) -> Result<(), WorkerError> {
    let mut line = serde_json::to_vec(&event)
        .map_err(|error| task_error("TASK_EVENT_INVALID", error.to_string()))?;
    line.push(b'\n');
    let wrote = match event.get("type").and_then(|v| v.as_str()) {
        Some("turn_accepted") => log.accepted(&line)?,
        Some("turn_terminal") => {
            let outcome = serde_json::from_value(event["outcome"].clone())
                .map_err(|_| task_error("TASK_EVENT_INVALID", "terminal outcome is invalid"))?;
            log.finish(
                Completion {
                    outcome,
                    drained: true,
                },
                &line,
            )?
        }
        _ => {
            log.append_bytes(&line)?;
            true
        }
    };
    if wrote {
        write_follower(follow, &line);
    }
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

/// Longest redacted error message kept in the runner's early-exit line.
const EARLY_EXIT_MESSAGE_LIMIT: usize = 160;

/// The redacted message body, if it tells the operator more than the code.
///
/// `AGENT_EXITED` is specific enough that a class-name rule would drop the
/// prebind reason; a message that is empty or only repeats the code is still
/// omitted so `exited: CAPACITY_BUSY workers=...` stays a code-only line.
fn early_exit_message_body<'a>(public_code: &str, message: &'a str) -> Option<&'a str> {
    let body = message
        .strip_prefix(public_code)
        .and_then(|rest| rest.strip_prefix(": "))
        .unwrap_or(message);
    if body.is_empty() || body == public_code {
        None
    } else {
        Some(body)
    }
}

/// The runner's one line about a turn it gave up on: the queue's view of
/// why the task waits, then the error that actually ended this runner with
/// its redacted message, then the configured workers.  The two codes are
/// printed once when they agree, so a plain capacity wait still reads
/// `exited: CAPACITY_BUSY workers=...`. After the journal already has content
/// (the host accepted the turn), the line is prefixed `exited after
/// acceptance:` so a reader can tell it from the pre-acceptance form; both
/// are plain text and pass through `task logs` verbatim.
fn early_exit_diagnostic_line(
    after_acceptance: bool,
    blocking: Option<&str>,
    public_code: &str,
    message: &str,
    workers: &[&str],
    receipt: Option<&crate::failure_receipt::FailureReceipt>,
) -> String {
    let prefix = if after_acceptance {
        "exited after acceptance: "
    } else {
        "exited: "
    };
    let mut line = format!("{prefix}{}", blocking.unwrap_or(public_code));
    if blocking.is_some_and(|blocking| blocking != public_code) {
        line.push_str(&format!(" error={public_code}"));
    }
    if let Some(receipt) = receipt {
        line.push_str(&format!(" stage={}", receipt.stage()));
    }
    if let Some(message) = early_exit_message_body(public_code, message) {
        line.push_str(&format!(" message={message}"));
    }
    if !workers.is_empty() {
        line.push_str(&format!(" workers={}", workers.join(",")));
    }
    if line.len() > EARLY_EXIT_DIAGNOSTIC_LIMIT {
        line.truncate(EARLY_EXIT_DIAGNOSTIC_LIMIT);
    }
    line.push('\n');
    line
}

/// Last `exited after acceptance:` public code in a runner journal.
///
/// Reconcile uses this when the queue row has no restart budget yet: the
/// diagnostic is the durable signal that a replacement already died, and
/// parsing it avoids a second on-disk record besides the queue field.
pub(crate) fn last_post_acceptance_public_code(log: &[u8]) -> Option<&str> {
    const PREFIX: &str = "exited after acceptance: ";
    let text = std::str::from_utf8(log).ok()?;
    for line in text.lines().rev() {
        let Some(rest) = line.strip_prefix(PREFIX) else {
            continue;
        };
        let code = rest
            .split_whitespace()
            .find_map(|part| part.strip_prefix("error="))
            .or_else(|| rest.split_whitespace().next())?;
        if crate::error::is_stable_public_code(code) {
            return Some(code);
        }
    }
    None
}

fn capacity_busy() -> WorkerError {
    WorkerError::capacity(
        "CAPACITY_BUSY",
        "no eligible worker currently has an available heavy slot",
    )
}

fn capacity_error(message: &'static str) -> WorkerError {
    WorkerError::capacity("CAPACITY_BUSY", message)
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

fn task_error(
    code: &'static str,
    message: impl Into<std::borrow::Cow<'static, str>>,
) -> WorkerError {
    WorkerError::task(code, message)
}

fn map_log_drain_unavailable(error: WorkerError) -> WorkerError {
    match error.public_code().as_str() {
        "JOB_NOT_FOUND" | "TASK_NOT_FOUND" => task_error(
            "LOG_DRAIN_UNAVAILABLE",
            "worker job or logs are gone; remaining stdout/stderr cannot be drained",
        ),
        _ => error,
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
    use super::{
        current_process_identity, early_exit_diagnostic_line, handoff_spawn_admitted,
        last_post_acceptance_public_code, open_handoff_journal_after_bind,
        open_handoff_journal_for_spawn, turn_exit_code,
    };
    use crate::task::TaskOutcome;

    #[test]
    fn handoff_spawn_permit_rejects_cancelled_adopted_bound_foreign_or_missing_rows() {
        use crate::job::{
            ClientId, CommandSummary, QueueEntry, QueueEntryKind, RunnerSlotReservation,
        };
        use crate::scheduler::WorkerPreference;
        use crate::task::TurnId;

        let reserver = current_process_identity().unwrap();
        let turn = TurnId::generate();
        let token = uuid::Uuid::new_v4();
        let owner = crate::job::ProcessIdentity::new(1, 1).unwrap();
        let mut healthy = QueueEntry::new(
            turn,
            ClientId::generate(),
            "a".repeat(64),
            "b".repeat(64),
            CommandSummary::argv(2).unwrap(),
            Vec::new(),
            WorkerPreference::Automatic,
            QueueEntryKind::TaskTurn,
            None,
            owner,
            10,
        )
        .unwrap();
        healthy
            .set_slot_reservation(Some(RunnerSlotReservation::new(reserver, token).unwrap()))
            .unwrap();
        assert!(handoff_spawn_admitted(
            Some(&healthy),
            turn,
            token,
            reserver,
            Some(owner)
        ));

        // Real task cancellation keeps the row and its token: must refuse.
        let mut cancelled = healthy.clone();
        cancelled.request_cancel(11).unwrap();
        assert!(!handoff_spawn_admitted(
            Some(&cancelled),
            turn,
            token,
            reserver,
            Some(owner)
        ));

        // Adopted away with the token intact: must refuse.
        let mut adopted = healthy.clone();
        adopted
            .adopt(crate::job::ProcessIdentity::new(3, 3).unwrap())
            .unwrap();
        assert!(!handoff_spawn_admitted(
            Some(&adopted),
            turn,
            token,
            reserver,
            Some(owner)
        ));

        // Already bound to a child elsewhere: must refuse.
        let mut bound = healthy.clone();
        let child = crate::job::ProcessIdentity::new(2, 2).unwrap();
        bound
            .set_slot_reservation(Some(
                RunnerSlotReservation::new(reserver, token)
                    .unwrap()
                    .with_child(child)
                    .unwrap(),
            ))
            .unwrap();
        assert!(!handoff_spawn_admitted(
            Some(&bound),
            turn,
            token,
            reserver,
            Some(owner)
        ));

        // Another process holds the reservation: must refuse.
        let mut foreign = healthy.clone();
        foreign
            .set_slot_reservation(Some(RunnerSlotReservation::new(owner, token).unwrap()))
            .unwrap();
        assert!(!handoff_spawn_admitted(
            Some(&foreign),
            turn,
            token,
            reserver,
            Some(owner)
        ));

        // Stale token and missing row fail closed as well.
        assert!(!handoff_spawn_admitted(
            Some(&healthy),
            turn,
            uuid::Uuid::new_v4(),
            reserver,
            Some(owner)
        ));
        assert!(!handoff_spawn_admitted(
            None,
            turn,
            token,
            reserver,
            Some(owner)
        ));
    }

    #[test]
    fn the_early_exit_line_keeps_the_runner_error_beside_the_blocking_code() {
        assert_eq!(
            early_exit_diagnostic_line(
                false,
                Some("WAITING_FOR_DISPATCH"),
                "PROJECT_MISMATCH",
                "PROJECT_MISMATCH: current project is not the task's project",
                &["mini-1"],
                None,
            ),
            "exited: WAITING_FOR_DISPATCH error=PROJECT_MISMATCH message=current project is not the task's project workers=mini-1\n"
        );
        assert_eq!(
            early_exit_diagnostic_line(
                false,
                Some("CAPACITY_BUSY"),
                "CAPACITY_BUSY",
                "",
                &["a", "b"],
                None,
            ),
            "exited: CAPACITY_BUSY workers=a,b\n"
        );
        assert_eq!(
            early_exit_diagnostic_line(
                false,
                None,
                "PROJECT_MISMATCH",
                "PROJECT_MISMATCH: x",
                &[],
                None
            ),
            "exited: PROJECT_MISMATCH message=x\n"
        );
    }

    #[test]
    fn the_early_exit_line_adds_the_message_when_it_says_more_than_the_code() {
        assert_eq!(
            early_exit_diagnostic_line(
                false,
                None,
                "IO",
                "IO: permission denied",
                &["mini-1"],
                None
            ),
            "exited: IO message=permission denied workers=mini-1\n"
        );
        assert_eq!(
            early_exit_diagnostic_line(
                false,
                Some("WAITING_FOR_DISPATCH"),
                "PROTOCOL",
                "worker probe was empty",
                &["mini-1"],
                None,
            ),
            "exited: WAITING_FOR_DISPATCH error=PROTOCOL message=worker probe was empty workers=mini-1\n"
        );
        assert_eq!(
            early_exit_diagnostic_line(
                false,
                Some("WAITING_FOR_DISPATCH"),
                "AGENT_EXITED",
                "AGENT_EXITED: session prebind failed: agent exited 1",
                &["mini-1"],
                None,
            ),
            "exited: WAITING_FOR_DISPATCH error=AGENT_EXITED message=session prebind failed: agent exited 1 workers=mini-1\n"
        );
        assert_eq!(
            early_exit_diagnostic_line(
                false,
                None,
                "LEASE_IDENTITY_MISMATCH",
                "long story",
                &["m"],
                None,
            ),
            "exited: LEASE_IDENTITY_MISMATCH message=long story workers=m\n"
        );
        assert_eq!(
            early_exit_diagnostic_line(false, None, "CAPACITY_BUSY", "CAPACITY_BUSY", &["a"], None),
            "exited: CAPACITY_BUSY workers=a\n"
        );
    }

    #[test]
    fn the_post_acceptance_early_exit_line_uses_a_distinct_prefix() {
        assert_eq!(
            early_exit_diagnostic_line(
                true,
                None,
                "PROJECT_MISMATCH",
                "PROJECT_MISMATCH: current project is not the task's project",
                &["mini-1"],
                None,
            ),
            "exited after acceptance: PROJECT_MISMATCH message=current project is not the task's project workers=mini-1\n"
        );
    }

    #[test]
    fn the_post_acceptance_early_exit_line_includes_a_host_io_stage() {
        let receipt = crate::failure_receipt::FailureReceipt::new(
            crate::failure_receipt::STAGE_CLEANUP,
            &[
                crate::failure_receipt::RESIDUAL_LEASE,
                crate::failure_receipt::RESIDUAL_CLEANUP_TREE,
            ],
        )
        .unwrap();
        assert_eq!(
            early_exit_diagnostic_line(
                true,
                None,
                "HOST_IO",
                "protocol error: HOST_IO: host state operation failed",
                &["mini-1"],
                Some(&receipt),
            ),
            "exited after acceptance: HOST_IO stage=cleanup message=protocol error: HOST_IO: host state operation failed workers=mini-1\n"
        );
        assert_eq!(
            last_post_acceptance_public_code(
                b"exited after acceptance: HOST_IO stage=cleanup workers=mini-1\n"
            ),
            Some("HOST_IO")
        );
    }

    #[test]
    fn last_post_acceptance_code_prefers_the_error_field_on_the_latest_line() {
        let log = b"exited after acceptance: WAITING_FOR_DISPATCH error=HOST_IO message=x workers=mini-1\n\
exited after acceptance: HOST_IO message=again workers=mini-1\n";
        assert_eq!(last_post_acceptance_public_code(log), Some("HOST_IO"));
        assert_eq!(
            last_post_acceptance_public_code(b"exited: CAPACITY_BUSY workers=mini-1\n"),
            None
        );
    }

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

    struct JournalContentionHook {
        entered: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
        resume: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl crate::client_state::ClientStateConcurrencyHook for JournalContentionHook {
        fn reach(&self, point: crate::client_state::ClientStateConcurrencyPoint) {
            if point == crate::client_state::ClientStateConcurrencyPoint::RunnerLogContention
                && let Some(sender) = self.entered.lock().unwrap().take()
            {
                sender.send(()).unwrap();
                self.resume
                    .lock()
                    .unwrap()
                    .recv_timeout(std::time::Duration::from_secs(20))
                    .expect("release journal contention hook");
            }
        }
    }

    struct ResumeOnDrop(Option<std::sync::mpsc::Sender<()>>);

    impl ResumeOnDrop {
        fn release(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    impl Drop for ResumeOnDrop {
        fn drop(&mut self) {
            self.release();
        }
    }

    fn isolated_handoff_paths() -> (tempfile::TempDir, crate::paths::PathLayout) {
        let root = tempfile::tempdir().unwrap();
        let state = root.path().canonicalize().unwrap().join("state");
        let paths = crate::paths::PathLayout {
            config: root.path().join("config.toml"),
            state,
            cache: root.path().join("cache"),
            data: root.path().join("data"),
        };
        (root, paths)
    }

    fn plant_reserved_turn(
        store: &crate::client_state::ClientStateStore,
        owner: crate::job::ProcessIdentity,
        with_locator: bool,
    ) -> (crate::task::TaskId, crate::task::TurnId, uuid::Uuid) {
        use crate::job::{CommandSummary, QueueEntry, QueueEntryKind};
        use crate::scheduler::WorkerPreference;

        let task_id = crate::task::TaskId::generate();
        let turn_id = crate::task::TurnId::generate();
        if with_locator {
            store.write_turn_prompt(task_id, turn_id, "prompt").unwrap();
        }
        store
            .enqueue(
                QueueEntry::new(
                    turn_id,
                    store.client_id(),
                    "a".repeat(64),
                    "b".repeat(64),
                    CommandSummary::argv(1).unwrap(),
                    Vec::new(),
                    WorkerPreference::Automatic,
                    QueueEntryKind::TaskTurn,
                    None,
                    owner,
                    10,
                )
                .unwrap(),
            )
            .unwrap();
        let crate::client_state::RunnerSlotDecision::Acquired { token } =
            store.reserve_runner_slot(turn_id, owner, 8, false).unwrap()
        else {
            panic!("expected acquired reservation");
        };
        (task_id, turn_id, token)
    }

    fn wait_confirmed_contention_then_mutate(
        entered: std::sync::mpsc::Receiver<()>,
        mut resume: ResumeOnDrop,
        held: crate::runner_log::RunnerLog,
        mutate: impl FnOnce(),
    ) {
        entered
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("helper reached RunnerLogContention under a live flock");
        mutate();
        drop(held);
        resume.release();
    }

    fn expect_task_busy(result: Result<crate::runner_log::RunnerLog, crate::error::WorkerError>) {
        match result {
            Err(error) => assert_eq!(error.public_code(), "TASK_BUSY"),
            Ok(_) => panic!("expected TASK_BUSY from handoff journal helper"),
        }
    }

    #[test]
    fn handoff_spawn_helper_refuses_cancel_after_confirmed_contention() {
        let owner = current_process_identity().unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let (_root, paths) = isolated_handoff_paths();
        let store = crate::client_state::ClientStateStore::open_with_concurrency_hook(
            &paths.state,
            std::sync::Arc::new(JournalContentionHook {
                entered: std::sync::Mutex::new(Some(entered_tx)),
                resume: std::sync::Mutex::new(resume_rx),
            }),
        )
        .unwrap();
        let (task_id, turn_id, token) = plant_reserved_turn(&store, owner, true);
        let held = crate::runner_log::RunnerLog::open(&paths.state, task_id, turn_id).unwrap();
        std::thread::scope(|scope| {
            let resume = ResumeOnDrop(Some(resume_tx));
            let helper = scope.spawn(|| {
                open_handoff_journal_for_spawn(&store, &paths, task_id, turn_id, Some(token))
            });
            wait_confirmed_contention_then_mutate(entered_rx, resume, held, || {
                store.retain_task_turn_cancel(turn_id, 11).unwrap();
            });
            expect_task_busy(helper.join().unwrap());
            assert!(
                store
                    .queue_entry(turn_id)
                    .unwrap()
                    .unwrap()
                    .is_cancel_requested(),
                "retained cancel must survive the refused spawn helper"
            );
        });
    }

    #[test]
    fn handoff_spawn_helper_refuses_adopt_after_confirmed_contention() {
        let owner = current_process_identity().unwrap();
        let replacement = crate::job::ProcessIdentity::new(999_997, 997).unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let (_root, paths) = isolated_handoff_paths();
        let store = crate::client_state::ClientStateStore::open_with_concurrency_hook(
            &paths.state,
            std::sync::Arc::new(JournalContentionHook {
                entered: std::sync::Mutex::new(Some(entered_tx)),
                resume: std::sync::Mutex::new(resume_rx),
            }),
        )
        .unwrap();
        let (task_id, turn_id, token) = plant_reserved_turn(&store, owner, true);
        let held = crate::runner_log::RunnerLog::open(&paths.state, task_id, turn_id).unwrap();
        std::thread::scope(|scope| {
            let resume = ResumeOnDrop(Some(resume_tx));
            let helper = scope.spawn(|| {
                open_handoff_journal_for_spawn(&store, &paths, task_id, turn_id, Some(token))
            });
            wait_confirmed_contention_then_mutate(entered_rx, resume, held, || {
                store.adopt_row(turn_id, replacement).unwrap();
            });
            expect_task_busy(helper.join().unwrap());
            assert_eq!(
                store.queue_entry(turn_id).unwrap().unwrap().owner_opt(),
                Some(&replacement),
                "adopted owner must survive the refused spawn helper"
            );
        });
    }

    #[test]
    fn handoff_spawn_helper_refuses_when_journal_task_has_no_turn_locator() {
        let owner = current_process_identity().unwrap();
        let (_root, paths) = isolated_handoff_paths();
        let store = crate::client_state::ClientStateStore::open(&paths.state).unwrap();
        let (task_id, turn_id, token) = plant_reserved_turn(&store, owner, false);
        assert!(
            store.queue_entry(turn_id).unwrap().is_some(),
            "raw queue lookup still sees the reserved turn"
        );
        expect_task_busy(open_handoff_journal_for_spawn(
            &store,
            &paths,
            task_id,
            turn_id,
            Some(token),
        ));
        assert!(
            store.queue_entry(turn_id).unwrap().is_some(),
            "mapping refusal must not delete the reserved row"
        );
    }

    #[test]
    fn handoff_after_bind_helper_refuses_cancel_after_confirmed_contention() {
        let owner = current_process_identity().unwrap();
        let child = crate::job::ProcessIdentity::new(8, 8).unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let (_root, paths) = isolated_handoff_paths();
        let store = crate::client_state::ClientStateStore::open_with_concurrency_hook(
            &paths.state,
            std::sync::Arc::new(JournalContentionHook {
                entered: std::sync::Mutex::new(Some(entered_tx)),
                resume: std::sync::Mutex::new(resume_rx),
            }),
        )
        .unwrap();
        let (task_id, turn_id, token) = plant_reserved_turn(&store, owner, true);
        let expected = store.bind_runner_slot_child(turn_id, token, child).unwrap();
        let held = crate::runner_log::RunnerLog::open(&paths.state, task_id, turn_id).unwrap();
        std::thread::scope(|scope| {
            let resume = ResumeOnDrop(Some(resume_tx));
            let helper = scope.spawn(|| {
                open_handoff_journal_after_bind(
                    &store, &paths, task_id, turn_id, token, owner, &expected,
                )
            });
            wait_confirmed_contention_then_mutate(entered_rx, resume, held, || {
                store.retain_task_turn_cancel(turn_id, 11).unwrap();
            });
            expect_task_busy(helper.join().unwrap());
            let row = store.queue_entry(turn_id).unwrap().unwrap();
            assert!(row.is_cancel_requested());
            assert_eq!(
                row.slot_reservation()
                    .and_then(|reservation| reservation.child()),
                Some(child),
                "bound reservation must remain after post-bind TASK_BUSY"
            );
        });
    }

    #[test]
    fn handoff_after_bind_helper_refuses_adopt_after_confirmed_contention() {
        let owner = current_process_identity().unwrap();
        let child = crate::job::ProcessIdentity::new(8, 8).unwrap();
        let replacement = crate::job::ProcessIdentity::new(999_997, 997).unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let (_root, paths) = isolated_handoff_paths();
        let store = crate::client_state::ClientStateStore::open_with_concurrency_hook(
            &paths.state,
            std::sync::Arc::new(JournalContentionHook {
                entered: std::sync::Mutex::new(Some(entered_tx)),
                resume: std::sync::Mutex::new(resume_rx),
            }),
        )
        .unwrap();
        let (task_id, turn_id, token) = plant_reserved_turn(&store, owner, true);
        let expected = store.bind_runner_slot_child(turn_id, token, child).unwrap();
        let held = crate::runner_log::RunnerLog::open(&paths.state, task_id, turn_id).unwrap();
        std::thread::scope(|scope| {
            let resume = ResumeOnDrop(Some(resume_tx));
            let helper = scope.spawn(|| {
                open_handoff_journal_after_bind(
                    &store, &paths, task_id, turn_id, token, owner, &expected,
                )
            });
            wait_confirmed_contention_then_mutate(entered_rx, resume, held, || {
                store.adopt_row(turn_id, replacement).unwrap();
            });
            expect_task_busy(helper.join().unwrap());
            let row = store.queue_entry(turn_id).unwrap().unwrap();
            assert_eq!(row.owner_opt(), Some(&replacement));
            assert_eq!(
                row.slot_reservation()
                    .and_then(|reservation| reservation.child()),
                Some(child),
                "bound reservation must remain after post-bind TASK_BUSY"
            );
        });
    }
}
