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
    agent::{AgentKind, TurnParams, adapter_for_launch, has_dialects, render_shell},
    client_state::{ClientStateStore, ReservedSlotTakeover, RunnerSlotDecision},
    config::{Config, WorkerEntry},
    error::WorkerError,
    git_transport::{GitTransport, SessionRefPush},
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
    session_transfer::imported_session_id,
    supervisor::SystemProcessInspector,
    task::{
        ClosePolicy, LocalTaskRecord, RunnerIdentity, TaskId, TaskOutcome, TaskState, TaskStatus,
        TurnId, TurnSummary, TurnTerminal,
    },
    task_store::{
        TaskCancelRequest, TaskPrebindRequest, TaskPrepareRequest, TaskSessionRequest,
        TaskStatusRequest,
    },
    transfer::{RemoteJobClient, TransferIdentity},
    transfer_repo::TransferRepo,
    turn::{TaskTurnRequest, TurnMaterial},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TurnStart {
    Fresh,
    Imported,
    FollowUp,
}

const LOG_CHUNK_LIMIT: u32 = 64 * 1024;
const EARLY_EXIT_DIAGNOSTIC_LIMIT: usize = 256;
const WAIT_POLL: Duration = Duration::from_millis(100);
/// Idle follow starts here and doubles until [`FOLLOW_POLL_MAX`].
const FOLLOW_POLL_MIN: Duration = Duration::from_millis(100);
/// Longest wait after an observation before the next follow poll.
/// Cancellation and a terminal job are noticed on the next observation, so
/// that wait is at most this long after the previous SSH call returns, plus
/// the deadline of an SSH call already in flight.
const FOLLOW_POLL_MAX: Duration = Duration::from_secs(2);
/// `task_status` while a non-terminal job is idle. Terminal jobs poll every
/// observation; this only covers a turn that stays quiet.
const FOLLOW_TASK_STATUS_PERIOD: Duration = Duration::from_secs(10);
const MAX_CAPACITY_BACKOFF_SECS: u64 = 30;
const RUNNER_HANDOFF_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a runner waits for a waiting row held by another identity to be
/// adopted to it.  The detached parent adopts within `RUNNER_HANDOFF_TIMEOUT`
/// or abandons the handoff; a runner started by hand for a row that belongs
/// to someone else would otherwise wait forever.
const ADOPTION_WAIT: Duration = Duration::from_secs(10);

/// Idle wait used only by [`TurnRunner::follow_remote`].
pub(crate) trait FollowClock: Send + Sync {
    fn sleep(&self, duration: Duration);
}

struct SystemFollowClock;

impl FollowClock for SystemFollowClock {
    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

static SYSTEM_FOLLOW_CLOCK: SystemFollowClock = SystemFollowClock;

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

    /// A local wait may end while the detached task keeps running. Executors
    /// that wait on local fences must share that caller's absolute budget.
    fn start_with_slot_until(
        &self,
        paths: &PathLayout,
        task_id: TaskId,
        turn_id: TurnId,
        slot_token: Option<Uuid>,
        deadline: Option<Instant>,
    ) -> Result<RunnerIdentity, WorkerError> {
        crate::client_state::WaitDeadline::until(deadline).remaining()?;
        self.start_with_slot(paths, task_id, turn_id, slot_token)
    }
}

/// Outcome of a reserved spawn attempt. Only [`Self::Started`] ran `executor.start`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunnerStart {
    Started(RunnerIdentity),
    /// Admission was refused by the persistent controller drain gate.
    Drained,
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

/// Explicit installed executable for runners launched by a generation-pinned
/// RPC child. Keeping the unit executor preserves all existing callers.
pub struct ConfiguredDetachedRunnerExecutor {
    executable: std::path::PathBuf,
}

impl DetachedRunnerExecutor {
    pub fn with_executable(
        executable: std::path::PathBuf,
    ) -> Result<ConfiguredDetachedRunnerExecutor, WorkerError> {
        use std::os::unix::fs::MetadataExt;
        let invalid = || {
            WorkerError::Protocol(
                "INVALID_REQUEST: invalid detached runner executable input".into(),
            )
        };
        if !executable.is_absolute()
            || executable
                .to_str()
                .is_none_or(|text| text.len() > 4096 || text.chars().any(char::is_control))
            || executable.components().any(|part| {
                matches!(
                    part,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
        {
            return Err(invalid());
        }
        let metadata = std::fs::symlink_metadata(&executable).map_err(|_| invalid())?;
        if !metadata.is_file()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o7022 != 0
            || metadata.mode() & 0o100 == 0
        {
            return Err(invalid());
        }
        Ok(ConfiguredDetachedRunnerExecutor { executable })
    }
}

impl RunnerExecutor for ConfiguredDetachedRunnerExecutor {
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
        self.start_with_slot_until(paths, task_id, turn_id, slot_token, None)
    }

    fn start_with_slot_until(
        &self,
        paths: &PathLayout,
        task_id: TaskId,
        turn_id: TurnId,
        slot_token: Option<Uuid>,
        deadline: Option<Instant>,
    ) -> Result<RunnerIdentity, WorkerError> {
        start_detached_runner(
            paths,
            task_id,
            turn_id,
            slot_token,
            deadline,
            Some(&self.executable),
        )
    }
}

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
        self.start_with_slot_until(paths, task_id, turn_id, slot_token, None)
    }

    fn start_with_slot_until(
        &self,
        paths: &PathLayout,
        task_id: TaskId,
        turn_id: TurnId,
        slot_token: Option<Uuid>,
        deadline: Option<Instant>,
    ) -> Result<RunnerIdentity, WorkerError> {
        start_detached_runner(paths, task_id, turn_id, slot_token, deadline, None)
    }
}

fn start_detached_runner(
    paths: &PathLayout,
    task_id: TaskId,
    turn_id: TurnId,
    slot_token: Option<Uuid>,
    deadline: Option<Instant>,
    executable: Option<&std::path::Path>,
) -> Result<RunnerIdentity, WorkerError> {
    // Use the same rooted, locked initialization as the child. Never follow
    // a substituted log or chmod an existing target through a pathname.
    let store = ClientStateStore::open_until(&paths.state, deadline)?;
    drop(open_handoff_journal_for_spawn(
        &store, paths, task_id, turn_id, slot_token,
    )?);
    let executable = match executable {
        Some(path) => path.to_owned(),
        None => std::env::current_exe().map_err(WorkerError::Io)?,
    };
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
    store.wait_deadline().remaining()?;
    let child = command.spawn().map_err(WorkerError::Io)?;
    let identity = SystemProcessInspector
        .identity_for_pid(child.id())
        .or_else(|_| fallback_process_identity(child.id()))?;
    // Dropping Child intentionally leaves the runner detached.  Its
    // durable state and owner identity are what reconciliation observes.
    drop(child);
    Ok(RunnerIdentity::new(identity))
}

/// Whether a queue row still authorizes this parent's spawn attempt.
///
/// Real task cancellation (`retain_task_turn_cancel`) sets only the cancel
/// flag and leaves state and reservation untouched, so a token-only check
/// would spawn for a cancelled turn. Likewise `adopt_row` swaps the owner
/// while keeping the token. Require the full permit: a live TaskTurn row for
/// this turn, no cancellation before acceptance, the row still owned by the identity
/// pinned when the handoff started, and a reservation carrying this attempt's
/// token, still owned by this process, with no child bound yet. A missing row
/// fails closed as well.
fn handoff_spawn_admitted(
    entry: Option<&crate::job::QueueEntry>,
    turn_id: TurnId,
    token: Uuid,
    reserver: ProcessIdentity,
    owner: Option<ProcessIdentity>,
    accepted: bool,
) -> bool {
    let Some(entry) = entry else {
        return false;
    };
    entry.kind() == QueueEntryKind::TaskTurn
        && entry.job_id() == turn_id
        && (accepted || !entry.is_cancel_requested())
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
/// When the spawn carries a reservation token, ownership and reservation are
/// revalidated before every attempt through [`handoff_spawn_admitted`]. The
/// cancellation gate is checked under the acquired journal, where acceptance
/// distinguishes a remote drainer from an unstarted turn. The full permit is
/// checked through `log.current_entry` (the journal
/// flock is the finalization fence: refresh/finalizer mutations of this exact
/// row serialize with it, so only the post-acquire check sees the task's
/// current turn). The row owner is pinned on first sight: adoption away
/// mid-handoff keeps the token but voids this attempt's permit. Cancelled
/// unaccepted, adopted-away, bound-elsewhere, or retired work therefore returns
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
        store.wait_deadline().remaining()?;
        if let Some((token, reserver, owner)) = permit
            && !handoff_spawn_admitted(
                store.queue_entry(turn_id)?.as_ref(),
                turn_id,
                token,
                reserver,
                owner,
                // Acceptance is checked under the journal below. A cancelled
                // accepted turn still needs a replacement to drain its worker.
                true,
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
                        log.is_accepted(),
                    ),
                    None => true,
                };
                store.wait_deadline().remaining()?;
                if admitted {
                    return Ok(log);
                }
                drop(log);
                return Err(task_error("TASK_BUSY", "task turn changed before handoff"));
            }
            None if Instant::now() < deadline => {
                store.runner_log_contention();
                std::thread::sleep(
                    store
                        .wait_deadline()
                        .cap(WAIT_POLL.min(deadline.saturating_duration_since(Instant::now())))?,
                );
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
    follow_clock: &'a dyn FollowClock,
    integration: Option<&'a crate::integration::coordinator::IntegrationCoordinator<'a>>,
}

impl<'a> TurnRunner<'a> {
    pub fn approved_turn_limits(
        &self,
        record: &LocalTaskRecord,
        turn: TurnId,
    ) -> Result<crate::agent::TurnLimits, WorkerError> {
        if let Some(prepared) = crate::integration::store::RootedIntegrationState::read_auxiliary(
            self.paths,
            record.meta().task_id(),
            turn,
        )? {
            if prepared.followup.expected().meta() != record.meta()
                || record.status().worker() != Some(prepared.followup.worker())
            {
                return Err(
                    crate::integration::contracts::IntegrationCode::IntegrationStateInvalid.error(),
                );
            }
            return Ok(prepared.approved_turn_limits);
        }
        Ok(record.meta().limits().turn.clone())
    }
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
            follow_clock: &SYSTEM_FOLLOW_CLOCK,
            integration: None,
        }
    }

    /// T6 injects owner ports for detached production runners.
    pub fn with_integration(
        mut self,
        coordinator: &'a crate::integration::coordinator::IntegrationCoordinator<'a>,
    ) -> Self {
        self.integration = Some(coordinator);
        self
    }
    pub(crate) fn with_optional_integration(
        mut self,
        coordinator: Option<&'a crate::integration::coordinator::IntegrationCoordinator<'a>>,
    ) -> Self {
        match coordinator {
            Some(coordinator) => self.with_integration(coordinator),
            None => {
                self.integration = None;
                self
            }
        }
    }

    /// Clock for [`Self::follow_remote`] idle waits. Production sleeps.
    /// Tests record the durations and do not wait.
    #[cfg(test)]
    pub(crate) fn with_follow_clock(mut self, clock: &'a dyn FollowClock) -> Self {
        self.follow_clock = clock;
        self
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
    #[cfg(any(test, feature = "test-support"))]
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
        if result.is_ok()
            && let Some(coordinator) = self.integration
        {
            // execute's journal, queue and transfer fences have all retired here.
            coordinator.on_terminal(task_id, turn_id)?;
            crate::task_client::stamp_integration_run_position(
                self.client_state,
                coordinator,
                task_id,
            )?;
        } else if result.is_ok()
            && let Err(error) = crate::integration::runner::recover_selected(
                self.runner,
                self.config,
                self.paths,
                self.client_state,
                self.executor,
                &[task_id],
            )
        {
            // The ordinary result is already imported and retired. Deferred
            // integration recovery must not rewrite its successful exit.
            eprintln!(
                "warning: integration recovery deferred [{}]",
                error.public_code()
            );
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
        {
            let _ = self.client_state.mutate_task(
                task_id,
                record.status().turns().last().map(TurnSummary::turn_id),
                |current| current.with_failure_receipt(Some(receipt)),
            );
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
            Ok(RunnerStart::Started(_)) | Ok(RunnerStart::Pending | RunnerStart::Drained) => Ok(()),
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
        if log.is_accepted() {
            return Ok(());
        }
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
        )?
        .copying_reported_checks(record.status())?;
        self.client_state.mutate_task(
            task_id,
            record.status().turns().last().map(TurnSummary::turn_id),
            |current| {
                current
                    .with_status(status)?
                    .with_abandon_code(Some("RUNNER_HANDOFF_FAILED".to_owned()))
            },
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
            transfer.release_task_refs(self.runner, task_id)?;
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
            let mut record = self.client_state.load_task(task_id)?;
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
            // Recover pending journal transactions before considering admission
            // or cancellation. A parked accepted turn still belongs to its host.
            if entry.owner_opt() == Some(&runner_owner) {
                let mut log = self.open_owned_journal(task_id, turn_id, runner_owner)?;
                // A refresh or the publishing parent may have changed the
                // projection while we waited. Legacy acceptance migration must
                // use the same-turn projection protected by the acquired fence.
                record = self.client_state.load_task(task_id)?;
                entry = self.require_runner_entry(task_id, turn_id, runner_owner)?;
                if log.completion().is_some() {
                    return Ok((task_id, turn_id, runner_owner));
                }
                if let Some(assignment) = log.accepted_assignment(&record)? {
                    self.client_state.resume_accepted_turn(
                        &assignment,
                        runner_owner,
                        now_millis()?,
                    )?;
                    return Ok((task_id, turn_id, runner_owner));
                }
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
                    let Some(_auxiliary_permit) =
                        crate::integration::runner::auxiliary_launch_permit(
                            self.paths,
                            self.client_state,
                            task_id,
                            turn_id,
                        )?
                    else {
                        std::thread::sleep(WAIT_POLL);
                        continue;
                    };
                    let Some(_permit) = crate::controller::drain::launch_permit(
                        &self.paths.controller_state_root(),
                        self.client_state.wait_deadline(),
                    )?
                    else {
                        drop(_auxiliary_permit);
                        std::thread::sleep(WAIT_POLL);
                        continue;
                    };
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
                    let claim = {
                        let Some(auxiliary_permit) =
                            crate::integration::runner::auxiliary_launch_permit(
                                self.paths,
                                self.client_state,
                                task_id,
                                turn_id,
                            )?
                        else {
                            std::thread::sleep(WAIT_POLL);
                            continue;
                        };
                        let Some(_permit) = crate::controller::drain::launch_permit(
                            &self.paths.controller_state_root(),
                            self.client_state.wait_deadline(),
                        )?
                        else {
                            drop(auxiliary_permit);
                            std::thread::sleep(WAIT_POLL);
                            continue;
                        };
                        self.client_state.claim_task_turn_with_slot_ceilings(
                            runner_owner,
                            turn_id,
                            &ranked,
                            now_millis()?,
                            &self.config.worker_slot_ceilings(),
                        )?
                    };
                    if let Some(claim) = claim {
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
                        if let Some((next_task, claim)) = self.claim_parked_if_admitted(
                            task_id,
                            turn_id,
                            runner_owner,
                            &observations,
                        )? {
                            return Ok((next_task, claim.entry().job_id(), runner_owner));
                        }
                    }
                }
            }
            std::thread::sleep(Duration::from_secs(backoff));
            backoff = (backoff * 2).min(MAX_CAPACITY_BACKOFF_SECS);
        }
    }

    fn claim_parked_if_admitted(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        owner: ProcessIdentity,
        observations: &[CandidateObservation],
    ) -> Result<Option<(TaskId, crate::job::QueueClaim)>, WorkerError> {
        let _hints = self.client_state.event_scope();
        // Reusing a process still starts a different turn. Fence the queue
        // claim and recipient ownership publication just like a new spawn.
        let Some(_drain_permit) = crate::controller::drain::launch_permit(
            &self.paths.controller_state_root(),
            self.client_state.wait_deadline(),
        )?
        else {
            return Ok(None);
        };
        let mut eligible = std::collections::HashSet::new();
        for entry in self.client_state.queue_snapshot()?.entries() {
            if !matches!(entry.state(), QueueState::Parked) {
                continue;
            }
            let Ok(Some(recipient)) = self.client_state.task_id_for_turn(entry.job_id()) else {
                continue;
            };
            if crate::integration::runner::auxiliary_queue_eligible(
                self.paths,
                self.client_state,
                recipient,
                entry.job_id(),
            )
            .unwrap_or(false)
            {
                eligible.insert(entry.job_id());
            }
        }
        while !eligible.is_empty() {
            let source = self
                .client_state
                .queue_entry(turn_id)?
                .ok_or_else(|| task_error("TASK_QUEUE_MISSING", "waiting runner has no row"))?;
            let Some((recipient, claim)) =
                self.client_state.claim_parked_filtered_for_waiting_runner(
                    task_id,
                    turn_id,
                    owner,
                    observations,
                    now_millis()?,
                    Some(&eligible),
                )?
            else {
                return Ok(None);
            };
            if let Ok(Some(_permit)) = crate::integration::runner::auxiliary_launch_permit(
                self.paths,
                self.client_state,
                recipient,
                claim.entry().job_id(),
            ) {
                return Ok(Some((recipient, claim)));
            }
            // A recipient's refusal belongs to that row. Restore the donor's
            // ownership before trying another candidate; never adopt its code.
            self.client_state
                .release_waiting_runner_reassignment(task_id, &source, recipient, &claim, owner)?;
            eligible.remove(&claim.entry().job_id());
        }
        Ok(None)
    }

    fn require_runner_entry(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        owner: ProcessIdentity,
    ) -> Result<crate::job::QueueEntry, WorkerError> {
        let entry = self
            .client_state
            .queue_entry_for_task_turn(task_id)?
            .filter(|entry| entry.job_id() == turn_id && entry.owner_opt() == Some(&owner))
            .ok_or_else(|| {
                task_error(
                    "TASK_BUSY",
                    "task turn ownership changed while waiting for its journal",
                )
            })?;
        if let Some(expected) = self.slot_token
            && entry
                .slot_reservation()
                .is_some_and(|reservation| reservation.token() != expected)
        {
            return Err(queue_error(
                "QUEUE_SLOT_TOKEN_MISMATCH",
                "runner slot token is no longer current",
            ));
        }
        // A completed parent handoff clears the token after publishing this
        // child's ownership, so absence of a reservation is legitimate.
        Ok(entry)
    }

    fn open_owned_journal(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        owner: ProcessIdentity,
    ) -> Result<LogWriter, WorkerError> {
        // Refresh and parent handoff publication briefly hold the same fence.
        // Wait without holding state locks, but never wait for another turn or
        // owner: retirement/reassignment revokes this runner's authority.
        loop {
            self.require_runner_entry(task_id, turn_id, owner)?;
            if let Some(log) = LogWriter::try_open(&self.paths.state, task_id, turn_id)? {
                self.require_runner_entry(task_id, turn_id, owner)?;
                return Ok(log);
            }
            self.client_state.runner_log_contention();
            std::thread::sleep(WAIT_POLL);
        }
    }

    /// The version of `agent` in the selected worker's recorded facts. Read
    /// only for an agent whose launch argv depends on its CLI generation, at
    /// the cost of one probe.
    ///
    /// The admission cache keeps capabilities, not versions, so the facts
    /// come from the worker. An unreachable worker or facts without the
    /// version leave it unknown, which keeps the default argv: the worker
    /// checks the argv against its installed generation before exec.
    fn recorded_agent_version(&self, worker: &WorkerEntry, agent: AgentKind) -> Option<String> {
        if !has_dialects(agent) {
            return None;
        }
        crate::transport::SshTransport::new(self.runner)
            .probe(worker)
            .probe?
            .agent_facts?
            .agents
            .into_iter()
            .find(|probe| probe.name == agent.as_str())?
            .version
    }

    fn execute(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        owner: ProcessIdentity,
        follow: &mut Option<&mut dyn Write>,
    ) -> Result<TurnOutcomeReport, WorkerError> {
        let mut log = self.open_owned_journal(task_id, turn_id, owner)?;
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
                drop(transfer);
                finalize_completed_turn(
                    self.client_state,
                    self.runner,
                    self.config,
                    self.paths,
                    task_id,
                    turn_id,
                    owner,
                    completion,
                )?;
                self.auto_continue_after_terminal(task_id, turn_id, follow);
                self.advance_pending_dags_after_transfer_drop()?;
                let status = self.client_state.load_task(task_id)?.status().clone();
                return Ok(TurnOutcomeReport {
                    status,
                    events: vec![],
                    exit_code,
                });
            }
        }
        let worker_name = log
            .accepted_assignment(&initial_record)?
            .map(|assignment| assignment.worker)
            .or(self
                .client_state
                .queue_entry(turn_id)?
                .and_then(|entry| match entry.state() {
                    QueueState::Dispatching {
                        selected_worker, ..
                    } => Some(selected_worker.to_owned()),
                    _ => None,
                }))
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
        // between first launch and a follow-up launch. Immutable import meta
        // distinguishes a fresh first launch from an imported first resume.
        let start = match (turn_number, initial_record.meta().session_import()) {
            (1, Some(_)) => TurnStart::Imported,
            (1, None) => TurnStart::Fresh,
            _ => TurnStart::FollowUp,
        };
        let turn_limits = self.approved_turn_limits(&initial_record, turn_id)?;
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
            start != TurnStart::Fresh,
        )?;
        let turn = match initial_record.meta().effective_policy() {
            Some(effective) => turn.with_effective_policy(effective)?,
            None => turn,
        }
        .with_frozen_setup(crate::project_readiness::FrozenSetup::from_snapshot(
            self.runner,
            transfer.path(),
            initial_record.meta().base_oid(),
        )?)?;
        crate::integration::runner::record_source_base(
            self.paths,
            &initial_record,
            turn_id,
            turn.base_oid(),
        )?;
        let params = TurnParams {
            kind: turn.agent(),
            model: turn.model().map(str::to_owned),
            effort: turn.effort().map(str::to_owned),
            policy: turn.policy(),
            limits: turn_limits,
            session_seed: turn.session_seed(),
            allow_permission_fallback: turn.effective_policy().is_some(),
        };
        let adapter = adapter_for_launch(
            turn.agent(),
            self.recorded_agent_version(worker, turn.agent()).as_deref(),
        );
        let remote = RemoteJobClient::new(self.runner);
        let prebound = if start == TurnStart::Fresh && adapter.prebind_session().is_some() {
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
        let session_ref = match start {
            TurnStart::Imported => Some(imported_session_id(&task_id)),
            TurnStart::FollowUp => Some(
                remote
                    .task_session(
                        worker,
                        &TaskSessionRequest::new(initial_record.meta().project_id(), task_id),
                    )?
                    .binding()
                    .session_ref()
                    .to_owned(),
            ),
            TurnStart::Fresh => prebound.clone(),
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
                let prepared = if start == TurnStart::FollowUp {
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
                                initial_record.meta().session_import().map(|import| {
                                    SessionRefPush {
                                        package_oid: import.package_oid(),
                                    }
                                }),
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
                        if start == TurnStart::Imported {
                            verify_imported_session(
                                &remote,
                                worker,
                                initial_record.meta().project_id(),
                                task_id,
                                initial_record.meta().agent(),
                            )?;
                        }
                        if let Some(policy) =
                            crate::integration::store::RootedIntegrationState::read_task(
                                self.paths, task_id,
                            )?
                            .0
                        {
                            use crate::integration::contracts::{
                                HostIntegrationAction, HostIntegrationRequest, IntegrationHost,
                                IntegrationRevision,
                            };
                            crate::integration::remote::RemoteIntegrationHost::new(&remote, worker)
                                .execute(&HostIntegrationRequest {
                                    protocol_version: crate::protocol::PROTOCOL_VERSION,
                                    task_id,
                                    integration_id: None,
                                    epoch: 0,
                                    revision: IntegrationRevision(0),
                                    action: HostIntegrationAction::Arm { policy },
                                })?;
                        }
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
                if start != TurnStart::FollowUp {
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
                    || (start != TurnStart::FollowUp
                        && matches!(prepared.status().state(), TaskState::Open))
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
                    let auxiliary =
                        crate::integration::store::RootedIntegrationState::read_auxiliary(
                            self.paths, task_id, turn_id,
                        )?;
                    let submitted = match auxiliary {
                        Some(auxiliary) => {
                            remote.submit_integration_turn(worker, &auxiliary, &request)
                        }
                        None => remote.submit_turn(worker, &request),
                    };
                    let response = match submitted {
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
                    append_event(
                        &mut log,
                        follow,
                        serde_json::json!({"type":"turn_accepted","protocol_version":crate::protocol::PROTOCOL_VERSION,"task_id":task_id,"turn_id":turn_id,"worker":worker.name}),
                    )?;
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
                    self.capture_accepted_status(
                        task_id,
                        turn_id,
                        initial_record.meta().run_id(),
                        worker,
                    );
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
                append_event(
                    &mut log,
                    follow,
                    serde_json::json!({"type":"turn_accepted","protocol_version":crate::protocol::PROTOCOL_VERSION,"task_id":task_id,"turn_id":turn_id,"worker":worker.name}),
                )?;
                let status = remote
                    .task_status(
                        worker,
                        &TaskStatusRequest::new(initial_record.meta().project_id(), task_id),
                    )?
                    .status()
                    .clone();
                self.persist_status(task_id, status.clone())?;
                self.capture_accepted_status(
                    task_id,
                    turn_id,
                    initial_record.meta().run_id(),
                    worker,
                );
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
        let auxiliary = crate::integration::store::RootedIntegrationState::read_auxiliary(
            self.paths, task_id, turn_id,
        )?
        .is_some();
        self.persist_status(task_id, terminal.clone())?;
        let fetched =
            if !auxiliary && matches!(terminal.state(), TaskState::Open | TaskState::Closed) {
                let import = transfer.result_import(task_id)?;
                if let Err(error) = GitTransport::new(self.runner).fetch_result(
                    worker,
                    self.client_state.client_id(),
                    initial_record.meta().project_id(),
                    task_id,
                    transfer.path(),
                ) {
                    if error.public_code() == "RESULT_REF_BUSY" {
                        return Err(error);
                    }
                    drop(import);
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
                match import.import_result(
                    self.runner,
                    &project.context.common_dir,
                    worker.name.as_str(),
                ) {
                    Ok(receipt) => Some(receipt.head().clone()),
                    Err(error) => {
                        if error.public_code() == "RESULT_REF_BUSY" {
                            return Err(error);
                        }
                        drop(import);
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
        if let Some(head) = fetched {
            self.client_state
                .mutate_task(task_id, Some(turn_id), |current| {
                    current.with_fetched_head(Some(head))
                })?;
        }
        if !auxiliary {
            self.persist_host_auto_close(worker, task_id)?;
        }
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
        if !auxiliary {
            transfer.release_task_refs(self.runner, task_id)?;
        }
        drop(transfer);
        retire_completed_turn(self.client_state, self.paths, turn_id, owner, task_id)?;
        self.auto_continue_after_terminal(task_id, turn_id, follow);
        self.advance_pending_dags_after_transfer_drop()?;
        let status = self.client_state.load_task(task_id)?.status().clone();
        Ok(TurnOutcomeReport {
            status,
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
        self.client_state
            .mutate_task(task_id, Some(turn_id), |current| {
                current.with_status(status.clone())
            })?;
        let event = serde_json::json!({
            "type": "turn_terminal",
            "protocol_version": crate::protocol::PROTOCOL_VERSION,
            "task_id": task_id.to_string(),
            "turn_id": turn_id.to_string(),
            "outcome": outcome,
        });
        append_event(log, follow, event.clone())?;
        transfer.release_task_refs(self.runner, task_id)?;
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
        if crate::runner_log::snapshot(&self.paths.state, task_id, turn_id)?
            .is_some_and(|snapshot| snapshot.accepted)
        {
            return Ok(false);
        }
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
        let mut poll_delay = FOLLOW_POLL_MIN;
        let mut idle_for = Duration::ZERO;
        let mut seen_job_state = None;
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
            let job = if let Some((job, stdout_chunk, stderr_chunk)) = combined {
                self.validate_follow_job(&record, worker, turn_id, &job)?;
                if job.status().state().is_terminal() {
                    drain.set_terminal_status(&job)?;
                }
                progressed |= Self::apply_log_chunk(&mut drain, log, follow, stdout_chunk)?;
                progressed |= Self::apply_log_chunk(&mut drain, log, follow, stderr_chunk)?;
                job
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
                job
            };
            let observed = job.status().state();
            let state_changed = seen_job_state.is_some_and(|previous| previous != observed);
            seen_job_state = Some(observed);
            if progressed || state_changed {
                poll_delay = FOLLOW_POLL_MIN;
                idle_for = Duration::ZERO;
            }
            let mut polled_task_status = false;
            if drain.cursor(LogStream::Stdout).is_drained()
                && drain.cursor(LogStream::Stderr).is_drained()
            {
                drain.revalidate_terminal_status(&remote.status(worker, turn_id)?)?;
                status = self.poll_follow_task_status(&remote, worker, &record, task_id)?;
                polled_task_status = true;
                if follow_is_complete(&status) {
                    return Ok(status);
                }
            }
            if !polled_task_status
                && (observed.is_terminal() || idle_for >= FOLLOW_TASK_STATUS_PERIOD)
            {
                status = self.poll_follow_task_status(&remote, worker, &record, task_id)?;
                if !observed.is_terminal() {
                    idle_for = Duration::ZERO;
                }
            }
            if !progressed && !state_changed {
                self.follow_clock.sleep(poll_delay);
                idle_for = idle_for.saturating_add(poll_delay);
                poll_delay = poll_delay.saturating_mul(2).min(FOLLOW_POLL_MAX);
            }
        }
    }

    fn poll_follow_task_status(
        &self,
        remote: &RemoteJobClient<'_>,
        worker: &WorkerEntry,
        record: &LocalTaskRecord,
        task_id: TaskId,
    ) -> Result<TaskStatus, WorkerError> {
        let status = remote
            .task_status(
                worker,
                &TaskStatusRequest::new(record.meta().project_id(), task_id),
            )?
            .status()
            .clone();
        self.persist_status(task_id, status.clone())?;
        Ok(status)
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

    fn persist_host_auto_close(
        &self,
        worker: &WorkerEntry,
        task_id: TaskId,
    ) -> Result<(), WorkerError> {
        let record = self.client_state.load_task(task_id)?;
        if record.meta().close_policy() != ClosePolicy::Done
            || record.status().state() != TaskState::Open
            || record.status().last_outcome() != Some(&TaskOutcome::Done)
        {
            return Ok(());
        }
        let status = RemoteJobClient::new(self.runner)
            .task_status(
                worker,
                &TaskStatusRequest::new(record.meta().project_id(), task_id),
            )?
            .status()
            .clone();
        if status.state() == TaskState::Closed {
            self.persist_status(task_id, status)?;
        }
        Ok(())
    }

    fn capture_accepted_status(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        run_id: Option<crate::task::RunId>,
        worker: &WorkerEntry,
    ) {
        if let Ok(worker) = crate::controller::events::WorkerName::parse(&worker.name) {
            self.client_state
                .capture_accepted_turn(crate::controller::events::AcceptedHint {
                    task_id,
                    turn_id,
                    run_id,
                    worker,
                });
        }
    }

    fn persist_status(&self, task_id: TaskId, status: TaskStatus) -> Result<(), WorkerError> {
        let record = self.client_state.load_task(task_id)?;
        let observed = status.updated_at_millis();
        self.client_state
            .mutate_task(
                task_id,
                record.status().turns().last().map(TurnSummary::turn_id),
                |current| {
                    current
                        .with_status(status)?
                        .with_status_observed_at(Some(observed))
                },
            )
            .map(|_| ())
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
        if log.is_accepted() {
            return Err(task_error(
                "TASK_BUSY",
                "accepted turn cannot be abandoned for capacity",
            ));
        }
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
        )?
        .copying_reported_checks(record.status())?;
        self.client_state.mutate_task(
            task_id,
            record.status().turns().last().map(TurnSummary::turn_id),
            |current| {
                current
                    .with_status(status)?
                    .with_abandon_code(Some("CAPACITY_BUSY".into()))
            },
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
            transfer.release_task_refs(self.runner, record.meta().task_id())?;
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
        if log.is_accepted() {
            return Err(task_error(
                "TASK_BUSY",
                "accepted turn requires remote cancellation",
            ));
        }
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
            .copying_reported_checks(record.status())?
        };
        let abandoned = record.status().state() != TaskState::Active;
        self.client_state.mutate_task(
            task_id,
            record.status().turns().last().map(TurnSummary::turn_id),
            |current| {
                current
                    .with_status(status)?
                    .with_abandon_code(abandoned.then_some("CANCELLED".to_owned()))
            },
        )?;
        log.finish_local(task_id, turn_id, TaskOutcome::Cancelled)?;
        if let Ok(project) = self.load_project_for_record(record)
            && project.context.project_id == record.meta().project_id()
            && let Ok(transfer) =
                crate::controller::registry::open_transfer_repo(self.paths, &project, record.meta())
        {
            transfer.release_task_refs(self.runner, task_id)?;
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

    fn auto_continue_after_terminal(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        follow: &mut Option<&mut dyn Write>,
    ) {
        let result = crate::task_client::TaskClient::new(
            self.runner,
            self.config,
            self.paths,
            self.client_state,
            self.executor,
        )
        .with_herdr_notifier(self.notifier.clone())
        .with_optional_integration(self.integration)
        .auto_continue_after_terminal(task_id, turn_id);
        if result.is_err() {
            // Completed journals are immutable. Emit only a fixed code to
            // diagnostics; failure cannot roll back the completed turn.
            let _ = writeln!(std::io::stderr().lock(), "AUTO_CONTINUE_FAILED");
            write_follower(follow, b"AUTO_CONTINUE_FAILED\n");
        }
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
    if crate::integration::store::RootedIntegrationState::read_auxiliary(paths, task_id, turn_id)?
        .is_some()
    {
        if completion.is_undrainable() && !record.retains_log_drain_unavailable() {
            let status = undrainable_failure_status(record.status(), turn_id, now_millis()?)?;
            let next = record
                .with_status(status)?
                .with_abandon_code(Some("LOG_DRAIN_UNAVAILABLE".into()))?;
            if !client_state.update_task_if_current(&record, next)? {
                return Err(task_error(
                    "TASK_BUSY",
                    "task changed during auxiliary finalization",
                ));
            }
        }
        retire_completed_turn(client_state, paths, turn_id, owner, task_id)?;
        return Ok(client_state.load_task(task_id)?.status().clone());
    }
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
            transfer.release_task_refs(runner, task_id)?;
        }
        retire_completed_turn(client_state, paths, turn_id, owner, task_id)?;
        return Ok(published.status().clone());
    }
    transfer.release_task_refs(runner, task_id)?;
    retire_completed_turn(client_state, paths, turn_id, owner, task_id)?;
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
    let transfer = crate::controller::registry::open_transfer_repo_until(
        runner,
        paths,
        &project,
        record.meta(),
        client_state.wait_deadline(),
    )?;
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
    let import = transfer.result_import(task_id).ok()?;
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
            import
                .import_result(runner, &project.context.common_dir, worker.name.as_str())
                .ok()
                .map(|receipt| receipt.head().clone())
        })
}

fn retire_completed_turn(
    client_state: &ClientStateStore,
    paths: &PathLayout,
    turn_id: TurnId,
    owner: ProcessIdentity,
    task_id: TaskId,
) -> Result<(), WorkerError> {
    if crate::integration::store::RootedIntegrationState::read_auxiliary(paths, task_id, turn_id)?
        .is_none()
        && crate::task_client::stage_auto_continue_before_retirement(client_state, task_id, turn_id)
            .is_err()
    {
        // Preparing a continuation must never undo the completed turn.
        let _ = writeln!(std::io::stderr().lock(), "AUTO_CONTINUE_FAILED");
        let _ = crate::task_client::log_auto_continue_failure(
            client_state,
            paths,
            task_id,
            turn_id,
            crate::prepared_followup::PreparedFollowup::automatic_turn_id(turn_id),
        );
    }
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
    )
    .with_parse_reason(last.result_parse_reason())
    .with_agent_identity(last.agent_identity().cloned());
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
    )?
    .copying_reported_checks(terminal)
}

/// Reserve a spawn permit, start only on [`RunnerSlotDecision::Acquired`], then
/// adopt the child and clear that token. `Drained`, `Pending`, and `Saturated` never spawn.
pub fn start_runner_with_reservation(
    client_state: &ClientStateStore,
    executor: &dyn RunnerExecutor,
    paths: &PathLayout,
    task_id: TaskId,
    turn_id: TurnId,
    slot_limit: usize,
    exclude_reserver: bool,
) -> Result<RunnerStart, WorkerError> {
    let _hints = client_state.event_scope();
    let Some(_auxiliary_permit) =
        crate::integration::runner::auxiliary_launch_permit(paths, client_state, task_id, turn_id)?
    else {
        return Ok(RunnerStart::Drained);
    };
    let Some(_drain_permit) = crate::controller::drain::launch_permit(
        &paths.controller_state_root(),
        client_state.wait_deadline(),
    )?
    else {
        return Ok(RunnerStart::Drained);
    };
    if let Some(entry) = client_state.queue_entry(turn_id)? {
        crate::integration::runner::record_position(
            paths,
            task_id,
            turn_id,
            entry.queue_id().value(),
        )?;
    }
    let reserver = current_process_identity()?;
    match client_state.reserve_runner_slot(turn_id, reserver, slot_limit, exclude_reserver)? {
        RunnerSlotDecision::Saturated => Ok(RunnerStart::Saturated),
        RunnerSlotDecision::Pending { .. } => Ok(RunnerStart::Pending),
        RunnerSlotDecision::Acquired { token } => {
            let expected = match client_state.queue_entry(turn_id).and_then(|entry| {
                entry.ok_or_else(|| task_error("TASK_BUSY", "task turn was retired before handoff"))
            }) {
                Ok(expected) => expected,
                Err(error) => {
                    // No child has been started. Even expiry between reservation
                    // publication and this read must leave the permit retryable.
                    let _ = client_state.try_release_runner_slot(turn_id, token, reserver);
                    return Err(error);
                }
            };
            match executor.start_with_slot_until(
                paths,
                task_id,
                turn_id,
                Some(token),
                client_state.wait_deadline().expires(),
            ) {
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
                    if error.public_code() == "WAIT_TIMEOUT" {
                        let _ = client_state.try_release_runner_slot(turn_id, token, reserver);
                    } else {
                        let _ = client_state.release_runner_slot(turn_id, token, reserver);
                    }
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
        client_state.wait_deadline().remaining()?;
        if client_state.queue_entry(turn_id)?.as_ref() != Some(expected) {
            let _ = client_state.release_runner_slot(turn_id, token, reserver);
            return Err(task_error("TASK_BUSY", "task turn changed during handoff"));
        }
        match LogWriter::try_open(&paths.state, task_id, turn_id)? {
            Some(log) => {
                client_state.wait_deadline().remaining()?;
                return Ok(log);
            }
            None if Instant::now() < deadline => {
                client_state.runner_log_contention();
                std::thread::sleep(
                    client_state
                        .wait_deadline()
                        .cap(WAIT_POLL.min(deadline.saturating_duration_since(Instant::now())))?,
                );
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

fn follow_is_complete(status: &TaskStatus) -> bool {
    status.state().is_terminal() || status.state() == TaskState::Open
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
        Some("turn_accepted") => log.accepted(
            event
                .get("worker")
                .and_then(|value| value.as_str())
                .ok_or_else(|| task_error("TASK_EVENT_INVALID", "accepted worker is missing"))?,
            &line,
        )?,
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
pub(crate) fn last_post_acceptance_public_code(log: &[u8]) -> Option<String> {
    const PREFIX: &str = "exited after acceptance: ";
    let text = String::from_utf8_lossy(log);
    for line in text.lines().rev() {
        let Some(rest) = line.strip_prefix(PREFIX) else {
            continue;
        };
        let code = rest
            .split_whitespace()
            .find_map(|part| part.strip_prefix("error="))
            .or_else(|| rest.split_whitespace().next())?;
        if crate::error::is_stable_public_code(code) {
            return Some(code.to_owned());
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
    crate::task::classify_task_outcome(outcome).runner
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
        )
        .with_parse_reason(last.result_parse_reason())
        .with_agent_identity(last.agent_identity().cloned());
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
    )?
    .copying_reported_checks(terminal)
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
    )?
    .copying_reported_checks(status)
}

fn verify_imported_session(
    remote: &RemoteJobClient<'_>,
    worker: &WorkerEntry,
    project_id: &str,
    task_id: TaskId,
    agent: AgentKind,
) -> Result<(), WorkerError> {
    let session = remote
        .task_session(worker, &TaskSessionRequest::new(project_id, task_id))
        .map_err(|error| {
            if error.public_code() == "SESSION_UNBOUND" {
                task_error(
                    "SESSION_PLACEMENT_FAILED",
                    "prepared import has no session binding",
                )
            } else {
                error
            }
        })?;
    if session.binding().agent() != agent
        || session.binding().session_ref() != imported_session_id(&task_id)
    {
        return Err(task_error(
            "SESSION_PLACEMENT_FAILED",
            "prepared import has a different agent session binding",
        ));
    }
    Ok(())
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

pub(crate) fn current_process_identity() -> Result<ProcessIdentity, WorkerError> {
    let pid = std::process::id();
    SystemProcessInspector
        .identity_for_pid(pid)
        .or_else(|_| fallback_process_identity(pid))
}

fn fallback_process_identity(pid: u32) -> Result<ProcessIdentity, WorkerError> {
    ProcessIdentity::new(pid, now_millis()?.saturating_mul(1000))
}

#[cfg(test)]
mod session_pin_cleanup_tests {
    use super::*;
    use crate::{
        agent::PermissionPolicy,
        job::QueueEntry,
        process::{ProcessRequest, ProcessResult, SystemProcessRunner},
        session_transfer::{
            CODEX_ROLLOUT_FILE, PackageFile, PackageSource, SessionAgent, SessionImportMeta,
            SessionPackage, testing::codex_fixture, tokens::normalize,
        },
        task::{GitIdentity, PublishMode, TaskLimits, TaskMeta, TaskMetaInput, TaskSource},
    };
    use std::{
        fs,
        sync::atomic::{AtomicBool, Ordering},
    };

    struct LocalOnlyRunner {
        fail_session_release: AtomicBool,
    }
    impl ProcessRunner for LocalOnlyRunner {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            assert_eq!(
                request.program, "/usr/bin/git",
                "cleanup must not contact a worker or provider"
            );
            if self.fail_session_release.load(Ordering::SeqCst)
                && request.args.iter().any(|arg| arg == "update-ref")
                && request.args.iter().any(|arg| arg == "-d")
                && request.args.iter().any(|arg| {
                    arg.to_str()
                        .is_some_and(|text| text.starts_with("refs/mac-worker/sessions/"))
                })
            {
                return Err(task_error(
                    "BASE_UNAVAILABLE",
                    "injected session pin release failure",
                ));
            }
            SystemProcessRunner.run(request)
        }
    }

    struct Fixture {
        _root: tempfile::TempDir,
        paths: PathLayout,
        config: Config,
        store: ClientStateStore,
        runner: LocalOnlyRunner,
        transfer: TransferRepo,
        task: TaskId,
        turn: TurnId,
        owner: ProcessIdentity,
    }
    impl Fixture {
        fn new(imported: bool, wip: bool) -> Self {
            let root = tempfile::tempdir().unwrap();
            let base = root.path().canonicalize().unwrap();
            let project = base.join("project");
            fs::create_dir(&project).unwrap();
            let git = |args: &[&str]| {
                let result = Command::new("/usr/bin/git")
                    .env_clear()
                    .env("HOME", &base)
                    .env("GIT_CONFIG_GLOBAL", "/dev/null")
                    .env("GIT_CONFIG_NOSYSTEM", "1")
                    .current_dir(&project)
                    .args(args)
                    .output()
                    .unwrap();
                assert!(
                    result.status.success(),
                    "{}",
                    String::from_utf8_lossy(&result.stderr)
                );
            };
            git(&["init", "--initial-branch=main"]);
            git(&["config", "user.name", "Fixture"]);
            git(&["config", "user.email", "fixture@example.test"]);
            fs::write(project.join("README"), b"fixture\n").unwrap();
            git(&["add", "README"]);
            git(&["commit", "-m", "fixture"]);
            let paths = PathLayout {
                config: base.join("config.toml"),
                state: base.join("state"),
                cache: base.join("cache"),
                data: base.join("data"),
            };
            let config = Config::parse(
                "version = 1\n[[workers]]\nname = 'fixture'\nssh = 'never-connect'\nslots = 1\n",
            )
            .unwrap();
            let store = ClientStateStore::open(&paths.state).unwrap();
            let runner = LocalOnlyRunner {
                fail_session_release: AtomicBool::new(false),
            };
            let state = ProjectState::load(&runner, &project, &[]).unwrap();
            let transfer =
                TransferRepo::open_or_create(&paths.cache, &state.context.common_dir).unwrap();
            let (task, turn) = (TaskId::generate(), TurnId::generate());
            let identity = GitIdentity::new("Fixture", "fixture@example.test").unwrap();
            let base_commit = if wip {
                transfer
                    .build_wip_base(&runner, &state.context, task, &state.settings, &identity)
                    .unwrap()
            } else {
                transfer
                    .resolve_base(&runner, &state.context, "HEAD")
                    .unwrap()
            };
            let import = imported.then(|| {
                let id = "018f0f4a-6b5c-7d8e-9f00-112233445566";
                let package = SessionPackage::build(
                    PackageSource {
                        agent: SessionAgent::Codex,
                        source_session_id: id.into(),
                        source_agent_version: "0.160.0".into(),
                        source_cwd_relative: String::new(),
                        scrubbed: 0,
                    },
                    vec![PackageFile {
                        path: CODEX_ROLLOUT_FILE.into(),
                        bytes: normalize(
                            &codex_fixture(id, project.to_str().unwrap(), "0.160.0", 1),
                            &[project.to_str().unwrap()],
                            id,
                        )
                        .unwrap(),
                    }],
                )
                .unwrap();
                SessionImportMeta::new(
                    SessionAgent::Codex,
                    transfer
                        .write_session_package(&runner, task, &package)
                        .unwrap(),
                    "0.160.0",
                )
                .unwrap()
            });
            let now = now_millis().unwrap();
            let meta = TaskMeta::new(TaskMetaInput {
                session_import: import,
                task_id: task,
                run_id: None,
                project_id: state.context.project_id.clone(),
                worktree_id: state.context.worktree_id.clone(),
                agent: AgentKind::Codex,
                model: None,
                effort: None,
                policy: PermissionPolicy::Workspace,
                source: TaskSource::Local {
                    wip,
                    push_target: None,
                },
                publish: vec![PublishMode::Fetch],
                publish_branch: None,
                base_oid: base_commit.oid().clone(),
                limits: TaskLimits::default(),
                close_policy: ClosePolicy::Never,
                env_profile: None,
                git_identity: identity,
                title: None,
                prompt: "continue".into(),
                created_at_millis: now,
            })
            .unwrap();
            let record = LocalTaskRecord::new(
                meta,
                TaskStatus::new(
                    TaskState::Queued,
                    None,
                    None,
                    false,
                    Some(base_commit.oid().clone()),
                    None,
                    vec![],
                    vec![],
                    None,
                    vec![],
                    now,
                )
                .unwrap(),
                None,
                None,
                None,
                transfer.repo_id().into(),
                None,
                true,
                None,
            )
            .unwrap();
            store.create_task(record.clone()).unwrap();
            store.write_task_project_path(&record, &project).unwrap();
            store.write_turn_prompt(task, turn, "continue").unwrap();
            let owner = current_process_identity().unwrap();
            store
                .enqueue(
                    QueueEntry::new(
                        turn,
                        store.client_id(),
                        state.context.project_id,
                        state.context.worktree_id,
                        CommandSpec::argv(vec!["task".into()])
                            .unwrap()
                            .summary()
                            .unwrap(),
                        vec![],
                        WorkerPreference::Automatic,
                        QueueEntryKind::TaskTurn,
                        None,
                        owner,
                        now,
                    )
                    .unwrap(),
                )
                .unwrap();
            drop(LogWriter::open(&paths.state, task, turn).unwrap());
            Self {
                _root: root,
                paths,
                config,
                store,
                runner,
                transfer,
                task,
                turn,
                owner,
            }
        }
        fn turn_runner(&self) -> TurnRunner<'_> {
            TurnRunner::new(
                &self.runner,
                &self.config,
                &self.paths,
                &self.store,
                &InlineRunnerExecutor,
            )
        }
        fn assert_retired(&self) {
            for prefix in ["refs/mac-worker/bases/", "refs/mac-worker/sessions/"] {
                assert!(
                    !self.transfer.has_ref(&format!("{prefix}{}", self.task)),
                    "cleanup leaked {prefix}"
                );
            }
            assert!(self.store.queue_entry(self.turn).unwrap().is_none());
            assert!(self.store.read_turn_prompt(self.task, self.turn).is_err());
            assert_eq!(
                self.store.load_task(self.task).unwrap().status().state(),
                TaskState::Abandoned
            );
        }
    }

    #[test]
    fn imported_and_sessionless_handoff_failure_retire_task_pins() {
        for imported in [false, true] {
            for wip in [false, true] {
                let fixture = Fixture::new(imported, wip);
                fixture
                    .turn_runner()
                    .abandon_handoff_failure(fixture.task, fixture.turn, fixture.owner)
                    .unwrap();
                fixture.assert_retired();
                assert_eq!(
                    fixture
                        .store
                        .load_task(fixture.task)
                        .unwrap()
                        .abandon_code(),
                    Some("RUNNER_HANDOFF_FAILED")
                );
            }
        }
    }

    #[test]
    fn imported_and_sessionless_cancel_before_acceptance_retire_task_pins() {
        for imported in [false, true] {
            for wip in [false, true] {
                let fixture = Fixture::new(imported, wip);
                let record = fixture.store.load_task(fixture.task).unwrap();
                fixture
                    .turn_runner()
                    .cancel_before_acceptance(
                        &record,
                        fixture.task,
                        fixture.turn,
                        fixture.owner,
                        None,
                    )
                    .unwrap();
                fixture.assert_retired();
                assert_eq!(
                    fixture
                        .store
                        .load_task(fixture.task)
                        .unwrap()
                        .status()
                        .last_outcome(),
                    Some(&TaskOutcome::Cancelled)
                );
            }
        }
    }

    #[test]
    fn abandoned_turns_keep_the_last_result_reported_checks() {
        use crate::agent::{ReportedCheck, ReportedCheckStatus};
        let checks = vec![ReportedCheck::new(
            "unit",
            "cargo test",
            ReportedCheckStatus::Fail,
            "1 failed",
        )];
        let paths = ["handoff", "capacity", "cancel"];
        let mut kept = Vec::new();
        for path in paths {
            let fixture = Fixture::new(false, false);
            let record = fixture.store.load_task(fixture.task).unwrap();
            let seeded = record
                .with_status(
                    record
                        .status()
                        .clone()
                        .with_reported_checks(checks.clone())
                        .unwrap(),
                )
                .unwrap();
            assert!(
                fixture
                    .store
                    .update_task_if_current(&record, seeded.clone())
                    .unwrap()
            );
            let runner = fixture.turn_runner();
            match path {
                "handoff" => {
                    runner.abandon_handoff_failure(fixture.task, fixture.turn, fixture.owner)
                }
                "capacity" => runner.abandon_capacity(&seeded, fixture.turn, fixture.owner, None),
                _ => runner.cancel_before_acceptance(
                    &seeded,
                    fixture.task,
                    fixture.turn,
                    fixture.owner,
                    None,
                ),
            }
            .unwrap();
            let abandoned = fixture.store.load_task(fixture.task).unwrap();
            kept.push((
                path,
                abandoned.status().state(),
                abandoned.status().reported_checks().to_vec(),
            ));
        }
        assert_eq!(
            kept,
            paths
                .map(|path| (path, TaskState::Abandoned, checks.clone()))
                .to_vec()
        );
    }

    #[test]
    fn accepted_imported_handoff_and_cancel_keep_pins_and_records() {
        // `cargo test --lib` runs this beside other tests in one process.
        // `Command::pre_exec` forks without taking `HeldFork`, so that child
        // can inherit this journal's flock fd until exec. `LogWriter::open`
        // then fails `LOCK_EX | LOCK_NB` and `cancel_before_acceptance`
        // reports IO before it can see that the turn was accepted. The body
        // runs in its own process, where no neighbour can hold that fd.
        const MARKER: &str = "MAC_WORKER_ACCEPTED_PIN_BODY";
        if std::env::var_os(MARKER).is_none() {
            let output = Command::new(std::env::current_exe().unwrap())
                .env(MARKER, "1")
                .args([
                    "--exact",
                    "turn_runner::session_pin_cleanup_tests::accepted_imported_handoff_and_cancel_keep_pins_and_records",
                    "--test-threads=1",
                ])
                .output()
                .unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success() && stdout.contains("1 passed"),
                "isolated pin body failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
            );
            return;
        }
        let fixture = Fixture::new(true, true);
        let before = serde_json::to_vec(&fixture.store.load_task(fixture.task).unwrap()).unwrap();
        let row = fixture.store.queue_entry(fixture.turn).unwrap();
        let mut log = LogWriter::open(&fixture.paths.state, fixture.task, fixture.turn).unwrap();
        log.accepted("fixture", b"accepted\n").unwrap();
        drop(log);
        fixture
            .turn_runner()
            .abandon_handoff_failure(fixture.task, fixture.turn, fixture.owner)
            .unwrap();
        let record = fixture.store.load_task(fixture.task).unwrap();
        assert_eq!(
            fixture
                .turn_runner()
                .cancel_before_acceptance(&record, fixture.task, fixture.turn, fixture.owner, None)
                .unwrap_err()
                .public_code(),
            "TASK_BUSY"
        );
        assert_eq!(
            serde_json::to_vec(&fixture.store.load_task(fixture.task).unwrap()).unwrap(),
            before
        );
        assert_eq!(fixture.store.queue_entry(fixture.turn).unwrap(), row);
        for prefix in ["refs/mac-worker/bases/", "refs/mac-worker/sessions/"] {
            assert!(
                fixture
                    .transfer
                    .has_ref(&format!("{prefix}{}", fixture.task))
            );
        }
    }

    #[test]
    fn session_release_failure_keeps_completion_marker_and_queue_until_retry() {
        for handoff in [false, true] {
            let fixture = Fixture::new(true, true);
            fixture
                .runner
                .fail_session_release
                .store(true, Ordering::SeqCst);
            let clean = || {
                if handoff {
                    fixture.turn_runner().abandon_handoff_failure(
                        fixture.task,
                        fixture.turn,
                        fixture.owner,
                    )
                } else {
                    fixture.turn_runner().cancel_before_acceptance(
                        &fixture.store.load_task(fixture.task).unwrap(),
                        fixture.task,
                        fixture.turn,
                        fixture.owner,
                        None,
                    )
                }
            };
            assert_eq!(clean().unwrap_err().public_code(), "BASE_UNAVAILABLE");
            assert!(fixture.store.queue_entry(fixture.turn).unwrap().is_some());
            assert!(
                fixture
                    .store
                    .read_turn_prompt(fixture.task, fixture.turn)
                    .is_ok()
            );
            assert!(
                crate::runner_log::snapshot(&fixture.paths.state, fixture.task, fixture.turn)
                    .unwrap()
                    .unwrap()
                    .completion
                    .is_some()
            );
            assert!(
                fixture
                    .transfer
                    .has_ref(&format!("refs/mac-worker/sessions/{}", fixture.task))
            );
            fixture
                .runner
                .fail_session_release
                .store(false, Ordering::SeqCst);
            clean().unwrap();
            fixture.assert_retired();
        }
    }
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
    fn integration_auxiliary_finalizer_keeps_source_refs_and_never_auto_continues() {
        use crate::integration::{contracts::*, store::RootedIntegrationState, testing::*};
        use crate::{
            client_state::ClientStateStore,
            config::Config,
            paths::PathLayout,
            task::{TaskState, TaskStatus, TurnSummary, TurnTerminal},
        };
        let f = IntegrationFixture::new();
        let paths = PathLayout {
            config: f.root().join("config"),
            state: f.root().join("state"),
            cache: f.root().join("cache"),
            data: f.root().join("data"),
        };
        let store = ClientStateStore::open(&paths.state).unwrap();
        let state = RootedIntegrationState::open(
            &paths,
            std::sync::Arc::new(ManualIntegrationRuntime::default()),
        )
        .unwrap();
        let mut record = sample_record(f.task(), f.source(), "main");
        record.candidates.push(sample_candidate(&record));
        record.snapshot.attempts = 1;
        let prepared = sample_prepared_turn(&record, IntegrationTurnPurpose::Resolve, 1, 1);
        let turn = prepared.followup.turn_id();
        record.auxiliaries.push(prepared.intent().unwrap());
        record.snapshot.resolve_turns = 1;
        record.followups_spent = 1;
        state.publish_policy(f.task(), &record.policy).unwrap();
        state.publish_prepared(f.task(), &prepared).unwrap();
        state
            .replace(f.task(), IntegrationRevision(0), &record)
            .unwrap();
        let ordinary = sample_ordinary(f.task(), f.source());
        let old = ordinary.status();
        let status = TaskStatus::new(
            TaskState::Open,
            Some(TaskOutcome::NeedsInput),
            old.worker().map(str::to_owned),
            true,
            old.head_oid().cloned(),
            Some("auxiliary blocked".into()),
            vec!["resolve this".into()],
            vec![],
            None,
            old.turns()
                .iter()
                .cloned()
                .chain([TurnSummary::new(
                    2,
                    turn,
                    Some(TurnTerminal::Succeeded),
                    Some(TaskOutcome::NeedsInput),
                    Some(false),
                    false,
                    Some(1001),
                    Some(1002),
                )])
                .collect(),
            1002,
        )
        .unwrap();
        let ordinary = ordinary
            .with_status(status)
            .unwrap()
            .with_questions_policy(crate::task::QuestionsPolicy::Decide);
        store.create_task(ordinary.clone()).unwrap();
        store
            .write_turn_prompt(f.task(), turn, prepared.followup.composed_prompt())
            .unwrap();
        let config = Config {
            version: 1,
            notifications: Default::default(),
            controller: Default::default(),
            ssh: Default::default(),
            workers: vec![],
        };
        // This runner accepts only task-session reads. Any ordinary Git publication panics.
        let runner = ImportedSessionRunner(None);
        let approved = super::TurnRunner::new(
            &runner,
            &config,
            &paths,
            &store,
            &super::InlineRunnerExecutor,
        )
        .approved_turn_limits(&ordinary, turn)
        .unwrap();
        assert_eq!(approved.timeout_millis, 600000);
        super::finalize_completed_turn(
            &store,
            &runner,
            &config,
            &paths,
            f.task(),
            turn,
            f.runtime().actor(),
            &crate::runner_log::Completion {
                outcome: TaskOutcome::NeedsInput,
                drained: true,
            },
        )
        .unwrap();
        let finalized = store.load_task(f.task()).unwrap();
        assert_eq!(finalized.fetched_head(), ordinary.fetched_head());
        assert!(finalized.auto_continue_intent().is_none());
        assert_eq!(state.load(f.task()).unwrap(), Some(record));
    }

    struct ImportedSessionRunner(Option<crate::task_store::SessionBinding>);

    impl crate::process::ProcessRunner for ImportedSessionRunner {
        fn run(
            &self,
            request: &crate::process::ProcessRequest,
        ) -> Result<crate::process::ProcessResult, crate::error::WorkerError> {
            use std::os::unix::process::ExitStatusExt;
            assert!(
                request
                    .args
                    .iter()
                    .any(|arg| arg == crate::transfer::HostOperation::TaskSession.command())
            );
            let (status, mut stdout) = match &self.0 {
                Some(binding) => (
                    0,
                    serde_json::to_vec(&crate::task_store::TaskSessionResponse::new(
                        binding.clone(),
                    ))
                    .unwrap(),
                ),
                None => (
                    1 << 8,
                    serde_json::to_vec(
                        &crate::job::HostControlError::new(
                            "SESSION_UNBOUND",
                            "fixture has no session",
                        )
                        .unwrap(),
                    )
                    .unwrap(),
                ),
            };
            stdout.push(b'\n');
            Ok(crate::process::ProcessResult {
                status: std::process::ExitStatus::from_raw(status),
                stdout,
                stderr: vec![],
            })
        }
    }

    #[test]
    fn imported_post_prepare_session_verification_requires_exact_agent_and_ref() {
        use crate::{
            agent::AgentKind, config::Config, session_transfer::imported_session_id, task::TaskId,
            task_store::SessionBinding, transfer::RemoteJobClient,
        };
        let task_id = TaskId::generate();
        let config = Config::parse(
            "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
        )
        .unwrap();
        let worker = config.worker("mini-1").unwrap();
        for agent in [AgentKind::Claude, AgentKind::Codex] {
            for (bound_agent, reference, valid) in [
                (agent, imported_session_id(&task_id), true),
                (agent, uuid::Uuid::new_v4().to_string(), false),
                (AgentKind::Cursor, imported_session_id(&task_id), false),
            ] {
                let runner = ImportedSessionRunner(Some(
                    SessionBinding::new(bound_agent, reference, 1).unwrap(),
                ));
                let result = super::verify_imported_session(
                    &RemoteJobClient::new(&runner),
                    worker,
                    &"a".repeat(64),
                    task_id,
                    agent,
                );
                if valid {
                    result.unwrap();
                } else {
                    assert_eq!(
                        result.unwrap_err().public_code(),
                        "SESSION_PLACEMENT_FAILED"
                    );
                }
            }
        }
    }

    #[test]
    fn imported_post_prepare_missing_session_is_a_placement_failure() {
        use crate::{agent::AgentKind, config::Config, task::TaskId, transfer::RemoteJobClient};
        let config = Config::parse(
            "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
        )
        .unwrap();
        let runner = ImportedSessionRunner(None);
        let error = super::verify_imported_session(
            &RemoteJobClient::new(&runner),
            config.worker("mini-1").unwrap(),
            &"a".repeat(64),
            TaskId::generate(),
            AgentKind::Claude,
        )
        .unwrap_err();
        assert_eq!(error.public_code(), "SESSION_PLACEMENT_FAILED");
    }

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
        let owner =
            crate::job::ProcessIdentity::new(crate::fixture_pid::fixture_pid(1), 1).unwrap();
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
            Some(owner),
            false
        ));

        // Real task cancellation keeps the row and its token: must refuse.
        let mut cancelled = healthy.clone();
        cancelled.request_cancel(11).unwrap();
        assert!(!handoff_spawn_admitted(
            Some(&cancelled),
            turn,
            token,
            reserver,
            Some(owner),
            false
        ));

        // Adopted away with the token intact: must refuse.
        let mut adopted = healthy.clone();
        adopted
            .adopt(crate::job::ProcessIdentity::new(crate::fixture_pid::fixture_pid(3), 3).unwrap())
            .unwrap();
        assert!(!handoff_spawn_admitted(
            Some(&adopted),
            turn,
            token,
            reserver,
            Some(owner),
            false
        ));

        // Already bound to a child elsewhere: must refuse.
        let mut bound = healthy.clone();
        let child =
            crate::job::ProcessIdentity::new(crate::fixture_pid::fixture_pid(2), 2).unwrap();
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
            Some(owner),
            false
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
            Some(owner),
            false
        ));

        // Stale token and missing row fail closed as well.
        assert!(!handoff_spawn_admitted(
            Some(&healthy),
            turn,
            uuid::Uuid::new_v4(),
            reserver,
            Some(owner),
            false
        ));
        assert!(!handoff_spawn_admitted(
            None,
            turn,
            token,
            reserver,
            Some(owner),
            false
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
            )
            .as_deref(),
            Some("HOST_IO")
        );
    }

    #[test]
    fn last_post_acceptance_code_prefers_the_error_field_on_the_latest_line() {
        let log = b"exited after acceptance: WAITING_FOR_DISPATCH error=HOST_IO message=x workers=mini-1\n\
exited after acceptance: HOST_IO message=again workers=mini-1\n";
        assert_eq!(
            last_post_acceptance_public_code(log).as_deref(),
            Some("HOST_IO")
        );
        assert_eq!(
            last_post_acceptance_public_code(b"exited: CAPACITY_BUSY workers=mini-1\n"),
            None
        );
    }

    #[test]
    fn last_post_acceptance_code_survives_invalid_utf8_in_the_window() {
        assert_eq!(
            last_post_acceptance_public_code(
                b"agent output: \xff\nexited after acceptance: WAITING_FOR_DISPATCH error=HOST_IO message=\xfe\nmore output: \x80\n"
            ).as_deref(),
            Some("HOST_IO")
        );
    }

    #[test]
    fn last_post_acceptance_code_preserves_exact_matching_rules() {
        for (log, expected) in [
            (
                "exited after acceptance: HOST_IO\n exited after acceptance: PUBLISH_FAILED\n",
                Some("HOST_IO"),
            ),
            (
                "exited after acceptance: HOST_IO\nexited after acceptance: WAITING_FOR_DISPATCH error=PUBLISH_FAILED\n",
                Some("PUBLISH_FAILED"),
            ),
            (
                "exited after acceptance: WAITING_FOR_DISPATCH\u{2003}error=HOST_IO\n",
                Some("HOST_IO"),
            ),
            (
                "exited after acceptance: HOST_IO\nexited after acceptance: invalid/path\n",
                Some("HOST_IO"),
            ),
            (
                "exited after acceptance: HOST_IO\nexited after acceptance: \n",
                None,
            ),
            (
                "exited after acceptance: WAITING_FOR_DISPATCH error=\n",
                None,
            ),
            ("exited: HOST_IO\n", None),
        ] {
            assert_eq!(
                last_post_acceptance_public_code(log.as_bytes()).as_deref(),
                expected,
                "{log:?}"
            );
        }
    }

    #[test]
    fn local_failure_rewrites_preserve_remote_turn_diagnostics() {
        use super::{publication_failure_status, undrainable_failure_status};
        use crate::task::{TaskState, TaskStatus, TurnId, TurnSummary, TurnTerminal};
        let identity = crate::agent::AgentIdentity {
            executable: "~/bin/agent".into(),
            version: Some("2.3.4".into()),
            version_observation: crate::agent::VersionObservation::Observed,
        };
        let turn = TurnSummary::new(
            1,
            TurnId::new(uuid::Uuid::new_v4()),
            Some(TurnTerminal::Succeeded),
            Some(TaskOutcome::Unknown),
            Some(false),
            false,
            Some(1),
            Some(2),
        )
        .with_agent_identity(Some(identity.clone()))
        .with_parse_reason(Some(crate::agent::ResultParseReason::NoResultJson));
        let terminal = TaskStatus::new(
            TaskState::Open,
            Some(TaskOutcome::Unknown),
            Some("mini-1".into()),
            true,
            None,
            None,
            Vec::new(),
            Vec::new(),
            None,
            vec![turn.clone()],
            2,
        )
        .unwrap();
        for rewritten in [
            publication_failure_status(&terminal, TaskOutcome::failed("PUBLISH_FAILED"), 3)
                .unwrap(),
            undrainable_failure_status(&terminal, turn.turn_id(), 3).unwrap(),
        ] {
            let last = rewritten.turns().last().unwrap();
            assert_eq!(last.agent_identity(), Some(&identity));
            assert_eq!(
                last.result_parse_reason(),
                Some(crate::agent::ResultParseReason::NoResultJson)
            );
        }
    }

    #[test]
    fn local_failure_and_cancel_rewrites_keep_the_reported_checks() {
        // They keep the summary and files of the last result, as the
        // owner's abandoned and cancelled statuses in task_client do.
        use super::{
            cancelled_followup_status, publication_failure_status, undrainable_failure_status,
        };
        use crate::agent::{ReportedCheck, ReportedCheckStatus};
        use crate::task::{TaskState, TaskStatus, TurnId, TurnSummary, TurnTerminal};
        let checks = vec![ReportedCheck::new(
            "unit",
            "cargo test",
            ReportedCheckStatus::Fail,
            "1 failed",
        )];
        let finished = TurnSummary::new(
            1,
            TurnId::new(uuid::Uuid::new_v4()),
            Some(TurnTerminal::Succeeded),
            Some(TaskOutcome::Done),
            Some(true),
            false,
            Some(1),
            Some(2),
        );
        let status = |state, turns| {
            TaskStatus::new(
                state,
                Some(TaskOutcome::Done),
                Some("mini-1".into()),
                true,
                None,
                Some("done".into()),
                Vec::new(),
                vec!["src/lib.rs".into()],
                None,
                turns,
                2,
            )
            .unwrap()
            .with_reported_checks(checks.clone())
            .unwrap()
        };
        let terminal = status(TaskState::Open, vec![finished.clone()]);
        let pending = TurnSummary::new(
            2,
            TurnId::new(uuid::Uuid::new_v4()),
            None,
            None,
            None,
            false,
            Some(3),
            None,
        );
        let active = status(TaskState::Active, vec![finished.clone(), pending]);
        let rewritten = [
            (
                "publication",
                publication_failure_status(
                    &terminal,
                    TaskOutcome::failed("RESULT_FETCH_FAILED"),
                    3,
                )
                .unwrap(),
            ),
            (
                "undrainable",
                undrainable_failure_status(&terminal, finished.turn_id(), 3).unwrap(),
            ),
            ("cancelled", cancelled_followup_status(&active).unwrap()),
        ];
        assert_eq!(
            rewritten
                .iter()
                .map(|(name, status)| (*name, status.summary(), status.reported_checks()))
                .collect::<Vec<_>>(),
            ["publication", "undrainable", "cancelled"]
                .map(|name| (name, Some("done"), checks.as_slice()))
                .to_vec()
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

    #[test]
    fn unknown_outcome_is_an_infrastructure_exit_while_needs_input_stays_successful() {
        assert_eq!(turn_exit_code(&TaskOutcome::Unknown), 70);
        assert_eq!(turn_exit_code(&TaskOutcome::NeedsInput), 0);
        assert_eq!(turn_exit_code(&TaskOutcome::failed("agent exited 0")), 1);
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

    #[test]
    fn claim_reassignment_drain_defers_before_queue_mutation() {
        let (_root, paths) = isolated_handoff_paths();
        let store = crate::client_state::ClientStateStore::open(&paths.state).unwrap();
        crate::controller::drain::set_drained(&paths.controller_state_root(), true).unwrap();
        let config = crate::config::Config::parse(
            "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"never-connect\"\nslots = 1\n",
        )
        .unwrap();
        let owner = super::current_process_identity().unwrap();
        let before = store.queue_snapshot().unwrap();
        let result = super::TurnRunner::new(
            &crate::process::SystemProcessRunner,
            &config,
            &paths,
            &store,
            &super::InlineRunnerExecutor,
        )
        .claim_parked_if_admitted(
            crate::task::TaskId::generate(),
            crate::task::TurnId::generate(),
            owner,
            &[],
        )
        .expect("drain must defer reassignment before attempting a new queue claim");
        assert!(result.is_none());
        assert_eq!(store.queue_snapshot().unwrap(), before);
    }

    #[test]
    fn ordinary_donor_ignores_another_auxiliary_refusal_and_owner_retires_its_row() {
        use crate::integration::{contracts::*, testing::*};
        use crate::job::{CommandSummary, QueueEntry, QueueEntryKind};
        use crate::scheduler::WorkerPreference;
        struct NoProcesses;
        impl crate::process::ProcessRunner for NoProcesses {
            fn run(
                &self,
                request: &crate::process::ProcessRequest,
            ) -> Result<crate::process::ProcessResult, crate::error::WorkerError> {
                panic!("local queue refusal must not launch a process: {request:?}");
            }
        }
        for refusal in ["tombstone", "blocked", "expired", "claimed_expired"] {
            let (_root, paths) = isolated_handoff_paths();
            let store = crate::client_state::ClientStateStore::open(&paths.state)
                .unwrap()
                .with_admission_clock(std::sync::Arc::new(|| Ok(10000)));
            let (state, mut record, _, entry) =
                crate::integration::runner::native_launch_tests::queued_auxiliary(
                    &paths,
                    &store,
                    if refusal.ends_with("expired") {
                        9999
                    } else {
                        600000
                    },
                );
            if refusal == "blocked" {
                record.snapshot.state = IntegrationStatus::Blocked;
                record.snapshot.blocked_code = Some(IntegrationCode::IntegrationWorkerOffline);
            } else if refusal == "tombstone" {
                record.snapshot.state = IntegrationStatus::Revoked;
                record.tombstone = Some(IntegrationTombstone {
                    epoch: record.snapshot.epoch,
                    revision: record.snapshot.revision,
                    requested_at_millis: 1001,
                    acknowledged: true,
                });
            }
            let previous = state.load(record.task_id).unwrap().unwrap();
            record.snapshot.revision = previous.snapshot.revision.next().unwrap();
            state
                .replace(record.task_id, previous.snapshot.revision, &record)
                .unwrap();
            store.park_row(entry.job_id()).unwrap();
            let donor = crate::task::TaskId::generate();
            let turn = crate::task::TurnId::generate();
            let owner = current_process_identity().unwrap();
            let ordinary = sample_ordinary(donor, turn);
            let status = crate::task::TaskStatus::new(
                crate::task::TaskState::Queued,
                None,
                Some("fixture-worker".into()),
                true,
                ordinary.status().head_oid().cloned(),
                None,
                vec![],
                vec![],
                None,
                vec![crate::task::TurnSummary::new(
                    1, turn, None, None, None, false, None, None,
                )],
                1001,
            )
            .unwrap();
            let ordinary = ordinary
                .with_status(status)
                .unwrap()
                .with_runner(Some(crate::task::RunnerIdentity::new(owner)))
                .unwrap();
            store.create_task(ordinary.clone()).unwrap();
            store
                .write_task_project_path(&ordinary, _root.path())
                .unwrap();
            store
                .write_turn_prompt(donor, turn, "ordinary donor")
                .unwrap();
            store
                .enqueue(
                    QueueEntry::new(
                        turn,
                        store.client_id(),
                        ordinary.meta().project_id().into(),
                        ordinary.meta().worktree_id().into(),
                        CommandSummary::argv(1).unwrap(),
                        vec![],
                        WorkerPreference::Pinned {
                            worker: "unavailable-donor-worker".into(),
                        },
                        QueueEntryKind::TaskTurn,
                        None,
                        owner,
                        1001,
                    )
                    .unwrap(),
                )
                .unwrap();
            let config = crate::config::Config::parse(
                "version = 1\n[[workers]]\nname = 'fixture-worker'\nssh = 'never-connect'\nslots = 1\n"
            ).unwrap();
            let before_queue = store.queue_snapshot().unwrap();
            let observations = if refusal == "claimed_expired" {
                let now = super::now_millis().unwrap();
                let slot = crate::scheduler::CandidateSlot::Idle;
                store
                    .admission_observation("fixture-worker", now, || {
                        Ok(crate::job::AdmissionObservation::new(
                            "fixture-worker".into(),
                            true,
                            slot,
                            vec!["agent:codex".into()],
                            Some(16 << 30),
                            64 << 30,
                            now,
                        )
                        .unwrap())
                    })
                    .unwrap();
                vec![
                    crate::scheduler::CandidateObservation::new(
                        "fixture-worker".into(),
                        true,
                        slot,
                        vec!["agent:codex".into()],
                        Some(16 << 30),
                        64 << 30,
                    )
                    .unwrap(),
                ]
            } else {
                vec![]
            };
            let result = super::TurnRunner::new(
                &NoProcesses,
                &config,
                &paths,
                &store,
                &super::InlineRunnerExecutor,
            )
            .claim_parked_if_admitted(donor, turn, owner, &observations);
            assert!(
                result.is_ok(),
                "{refusal} escaped into the unrelated donor: {}",
                result.err().unwrap()
            );
            assert_eq!(store.load_task(donor).unwrap(), ordinary);
            assert_eq!(store.queue_snapshot().unwrap(), before_queue);
            let after = state.load(record.task_id).unwrap().unwrap();
            if refusal == "claimed_expired" {
                assert_eq!(after.snapshot.state, IntegrationStatus::Blocked);
                assert_eq!(
                    after.snapshot.blocked_code,
                    Some(IntegrationCode::IntegrationTurnQueueTimeout)
                );
                assert!(
                    store.load_task(record.task_id).unwrap().runner().is_none(),
                    "refused claim retained recipient ownership"
                );
            } else {
                assert_eq!(after, record, "scan mutated another task's budget/state");
            }
            crate::integration::runner::OwnerIntegration::new(
                &NoProcesses,
                &config,
                &paths,
                &store,
                &super::InlineRunnerExecutor,
            )
            .unwrap()
            .stage(record.task_id)
            .unwrap();
            assert!(
                store.queue_entry(entry.job_id()).unwrap().is_none(),
                "owner left a refused Parked auxiliary row"
            );
            assert_eq!(store.load_task(donor).unwrap(), ordinary);
        }
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
    fn accepted_cancelled_handoff_allows_a_replacement_drainer() {
        use super::{ClientStateStore, LogWriter, ProcessIdentity};
        let owner = current_process_identity().unwrap();
        let (_root, paths) = isolated_handoff_paths();
        let store = ClientStateStore::open(&paths.state).unwrap();
        let (task_id, turn_id, token) = plant_reserved_turn(&store, owner, true);
        let mut log = LogWriter::open(&paths.state, task_id, turn_id).unwrap();
        log.accepted("mini-1", b"accepted\n").unwrap();
        drop(log);
        store.retain_task_turn_cancel(turn_id, 11).unwrap();
        let log = open_handoff_journal_for_spawn(&store, &paths, task_id, turn_id, Some(token))
            .expect("accepted cancellation still needs a remote drainer");
        assert!(log.is_accepted());
        drop(log);
        let child = ProcessIdentity::new(crate::fixture_pid::fixture_pid(8), 8).unwrap();
        let expected = store.bind_runner_slot_child(turn_id, token, child).unwrap();
        let log = open_handoff_journal_after_bind(
            &store, &paths, task_id, turn_id, token, owner, &expected,
        )
        .unwrap();
        assert_eq!(log.current_entry(&store).unwrap().as_ref(), Some(&expected));
    }

    #[test]
    fn accepted_handoff_failure_cannot_abandon_or_cancel_the_turn() {
        use super::{
            ClientStateStore, Config, InlineRunnerExecutor, LocalTaskRecord, LogWriter, TaskState,
            TaskStatus, TurnRunner, TurnSummary,
        };
        use crate::task::{
            ClosePolicy, GitIdentity, PublishMode, TaskLimits, TaskMeta, TaskMetaInput, TaskSource,
        };
        let owner = current_process_identity().unwrap();
        let (_root, paths) = isolated_handoff_paths();
        let store = ClientStateStore::open(&paths.state).unwrap();
        let (task_id, turn_id, _) = plant_reserved_turn(&store, owner, true);
        let meta = TaskMeta::new(TaskMetaInput {
            session_import: None,
            task_id,
            run_id: None,
            project_id: "a".repeat(64),
            worktree_id: "b".repeat(64),
            agent: crate::agent::AgentKind::Codex,
            model: None,
            effort: None,
            policy: crate::agent::PermissionPolicy::Workspace,
            source: TaskSource::Local {
                wip: false,
                push_target: None,
            },
            publish: vec![PublishMode::Fetch],
            publish_branch: None,
            base_oid: "a".repeat(40).parse().unwrap(),
            limits: TaskLimits::default(),
            close_policy: ClosePolicy::Never,
            env_profile: None,
            git_identity: GitIdentity::new("Fixture", "fixture@example.test").unwrap(),
            title: None,
            prompt: "work".into(),
            created_at_millis: 10,
        })
        .unwrap();
        let status = TaskStatus::new(
            TaskState::Active,
            None,
            Some("mini-1".into()),
            true,
            None,
            None,
            vec![],
            vec![],
            None,
            vec![TurnSummary::new(
                1,
                turn_id,
                None,
                None,
                None,
                false,
                Some(10),
                None,
            )],
            10,
        )
        .unwrap();
        let record = LocalTaskRecord::new(
            meta,
            status,
            None,
            None,
            None,
            "c".repeat(64),
            None,
            true,
            None,
        )
        .unwrap();
        store.create_task(record.clone()).unwrap();
        let mut log = LogWriter::open(&paths.state, task_id, turn_id).unwrap();
        log.accepted("mini-1", b"accepted\n").unwrap();
        drop(log);
        let config = Config::parse(
            "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
        )
        .unwrap();
        let result = TurnRunner::new(
            &crate::process::SystemProcessRunner,
            &config,
            &paths,
            &store,
            &InlineRunnerExecutor,
        )
        .abandon_handoff_failure(task_id, turn_id, owner);
        assert_eq!(
            store.load_task(task_id).unwrap(),
            record,
            "handoff failure rewrote an accepted turn"
        );
        result.unwrap();
        assert!(
            !store
                .queue_entry(turn_id)
                .unwrap()
                .unwrap()
                .is_cancel_requested()
        );
        assert!(
            crate::runner_log::snapshot(&paths.state, task_id, turn_id)
                .unwrap()
                .unwrap()
                .completion
                .is_none()
        );
    }

    #[test]
    fn handoff_helpers_refuse_cancel_and_adopt_after_confirmed_contention() {
        // The spawn helper and the post-bind helper both answer TASK_BUSY once
        // contention is confirmed, the row mutation that raced them (a retained
        // cancel or an adoption) survives, and after bind the bound child
        // reservation survives as well.
        for after_bind in [false, true] {
            for adopt in [false, true] {
                let label = format!("after_bind={after_bind} adopt={adopt}");
                let owner = current_process_identity().unwrap();
                let child = crate::job::ProcessIdentity::new(crate::fixture_pid::fixture_pid(8), 8)
                    .unwrap();
                let replacement =
                    crate::job::ProcessIdentity::new(crate::fixture_pid::fixture_pid(999_997), 997)
                        .unwrap();
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
                let expected = after_bind
                    .then(|| store.bind_runner_slot_child(turn_id, token, child).unwrap());
                let held =
                    crate::runner_log::RunnerLog::open(&paths.state, task_id, turn_id).unwrap();
                std::thread::scope(|scope| {
                    let resume = ResumeOnDrop(Some(resume_tx));
                    let helper = scope.spawn(|| {
                        let result = match expected.as_ref() {
                            Some(expected) => open_handoff_journal_after_bind(
                                &store, &paths, task_id, turn_id, token, owner, expected,
                            ),
                            None => open_handoff_journal_for_spawn(
                                &store,
                                &paths,
                                task_id,
                                turn_id,
                                Some(token),
                            ),
                        };
                        // The log's deferred scope must stay on its owning thread.
                        expect_task_busy(result);
                    });
                    wait_confirmed_contention_then_mutate(entered_rx, resume, held, || {
                        if adopt {
                            store.adopt_row(turn_id, replacement).unwrap();
                        } else {
                            store.retain_task_turn_cancel(turn_id, 11).unwrap();
                        }
                    });
                    helper.join().unwrap();
                    let row = store.queue_entry(turn_id).unwrap().unwrap();
                    if adopt {
                        assert_eq!(
                            row.owner_opt(),
                            Some(&replacement),
                            "{label}: adopted owner must survive the refused helper"
                        );
                    } else {
                        assert!(
                            row.is_cancel_requested(),
                            "{label}: retained cancel must survive the refused helper"
                        );
                    }
                    if after_bind {
                        assert_eq!(
                            row.slot_reservation()
                                .and_then(|reservation| reservation.child()),
                            Some(child),
                            "{label}: bound reservation must remain after post-bind TASK_BUSY"
                        );
                    }
                });
            }
        }
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

    use super::{
        FollowClock, ProcessRunner, TaskState, TaskStatus, TurnId, TurnRunner, TurnSummary,
    };
    use crate::error::WorkerError;
    use crate::process::ProcessResult;
    use std::time::Duration;

    struct FollowStep {
        status: crate::job::JobStatus,
        stdout: Vec<u8>,
    }

    struct FollowScript {
        meta: crate::job::JobMeta,
        steps: std::sync::Mutex<std::collections::VecDeque<FollowStep>>,
        terminal: std::sync::Mutex<Option<crate::job::StatusResponse>>,
        status_logs: std::sync::atomic::AtomicUsize,
        statuses: std::sync::atomic::AtomicUsize,
        task_statuses: std::sync::atomic::AtomicUsize,
        active: TaskStatus,
        open: TaskStatus,
    }

    struct RecordingClock {
        sleeps: std::sync::Mutex<Vec<Duration>>,
    }

    impl FollowClock for RecordingClock {
        fn sleep(&self, duration: Duration) {
            self.sleeps.lock().unwrap().push(duration);
        }
    }

    impl ProcessRunner for FollowScript {
        fn run(
            &self,
            request: &crate::process::ProcessRequest,
        ) -> Result<ProcessResult, WorkerError> {
            use crate::job::{LogChunk, LogStream, StatusLogsRequest, StatusResponse};
            use crate::task_store::TaskStatusResponse;
            use crate::transfer::HostOperation;
            use std::os::unix::process::ExitStatusExt;
            use std::sync::atomic::Ordering;

            let operation = request.args.last().and_then(|arg| arg.to_str());
            fn ok(value: &impl serde::Serialize) -> Result<ProcessResult, WorkerError> {
                Ok(ProcessResult {
                    status: std::process::ExitStatus::from_raw(0),
                    stdout: serde_json::to_vec(value).unwrap(),
                    stderr: Vec::new(),
                })
            }
            if operation == Some(HostOperation::StatusLogs.command()) {
                let query: StatusLogsRequest =
                    serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
                let step = self
                    .steps
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("follow script ended before the turn finished");
                self.status_logs.fetch_add(1, Ordering::Relaxed);
                let response = StatusResponse::new(self.meta.clone(), step.status.clone()).unwrap();
                if step.status.state().is_terminal() {
                    *self.terminal.lock().unwrap() = Some(response.clone());
                }
                let stdout = LogChunk::new(LogStream::Stdout, query.stdout_offset(), step.stdout)?;
                let stderr = LogChunk::new(LogStream::Stderr, query.stderr_offset(), Vec::new())?;
                return ok(&crate::job::StatusLogsResponse::new(
                    response, stdout, stderr,
                )?);
            }
            if operation == Some(HostOperation::Status.command()) {
                self.statuses.fetch_add(1, Ordering::Relaxed);
                let response = self
                    .terminal
                    .lock()
                    .unwrap()
                    .clone()
                    .expect("status before a terminal job");
                let response = if response.status().cleanup_error_code().is_some() {
                    StatusResponse::new(
                        response.meta().clone(),
                        response
                            .status()
                            .without_cleanup_error(response.status().updated_at_millis() + 1)?,
                    )?
                } else {
                    response
                };
                return ok(&response);
            }
            if operation == Some(HostOperation::TaskStatus.command()) {
                self.task_statuses.fetch_add(1, Ordering::Relaxed);
                let status = if self.terminal.lock().unwrap().is_some() {
                    self.open.clone()
                } else {
                    self.active.clone()
                };
                return ok(&TaskStatusResponse::new(status));
            }
            panic!("unexpected follow command: {operation:?}");
        }
    }

    struct FollowPace {
        sleeps: Vec<Duration>,
        status_logs: usize,
        statuses: usize,
        task_statuses: usize,
    }

    fn follow_task_status(state: TaskState, turn_id: TurnId) -> TaskStatus {
        TaskStatus::new(
            state,
            None,
            Some("mini-1".into()),
            false,
            None,
            None,
            Vec::new(),
            Vec::new(),
            None,
            vec![TurnSummary::new(
                1,
                turn_id,
                None,
                None,
                None,
                false,
                Some(10),
                None,
            )],
            20,
        )
        .unwrap()
    }

    fn run_follow_script(steps: Vec<FollowStep>) -> FollowPace {
        use super::{ClientStateStore, Config, InlineRunnerExecutor, LocalTaskRecord, LogWriter};
        use crate::job::{CommandSpec, JobMeta, LeaseToken, RequestFingerprintMaterial};
        use crate::task::{
            ClosePolicy, GitIdentity, PublishMode, TaskLimits, TaskMeta, TaskMetaInput, TaskSource,
        };
        use std::sync::atomic::Ordering;

        let (_root, paths) = isolated_handoff_paths();
        let store = ClientStateStore::open(&paths.state).unwrap();
        let task_id = crate::task::TaskId::generate();
        let turn_id = TurnId::generate();
        let active = follow_task_status(TaskState::Active, turn_id);
        let open = follow_task_status(TaskState::Open, turn_id);
        let meta = TaskMeta::new(TaskMetaInput {
            session_import: None,
            task_id,
            run_id: None,
            project_id: "a".repeat(64),
            worktree_id: "b".repeat(64),
            agent: crate::agent::AgentKind::Codex,
            model: None,
            effort: None,
            policy: crate::agent::PermissionPolicy::Workspace,
            source: TaskSource::Local {
                wip: false,
                push_target: None,
            },
            publish: vec![PublishMode::Fetch],
            publish_branch: None,
            base_oid: "a".repeat(40).parse().unwrap(),
            limits: TaskLimits::default(),
            close_policy: ClosePolicy::Never,
            env_profile: None,
            git_identity: GitIdentity::new("Fixture", "fixture@example.test").unwrap(),
            title: None,
            prompt: "work".into(),
            created_at_millis: 10,
        })
        .unwrap();
        store
            .create_task(
                LocalTaskRecord::new(
                    meta,
                    active.clone(),
                    None,
                    None,
                    None,
                    "c".repeat(64),
                    None,
                    false,
                    None,
                )
                .unwrap(),
            )
            .unwrap();
        let material = RequestFingerprintMaterial::new(
            turn_id,
            store.client_id(),
            LeaseToken::new(turn_id.as_uuid()),
            10,
            "mini-1".into(),
            "a".repeat(64),
            "b".repeat(64),
            "d".repeat(64),
            String::new(),
            60_000,
            "heavy".into(),
            CommandSpec::argv(vec!["true".into()]).unwrap(),
        )
        .unwrap();
        let script = FollowScript {
            meta: JobMeta::new(&material, material.fingerprint()).unwrap(),
            steps: std::sync::Mutex::new(steps.into()),
            terminal: std::sync::Mutex::new(None),
            status_logs: std::sync::atomic::AtomicUsize::new(0),
            statuses: std::sync::atomic::AtomicUsize::new(0),
            task_statuses: std::sync::atomic::AtomicUsize::new(0),
            active,
            open,
        };
        let config = Config::parse(
            "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
        )
        .unwrap();
        let worker = config.worker("mini-1").unwrap().clone();
        let clock = RecordingClock {
            sleeps: std::sync::Mutex::new(Vec::new()),
        };
        let runner = TurnRunner::new(&script, &config, &paths, &store, &InlineRunnerExecutor)
            .with_follow_clock(&clock);
        let mut log = LogWriter::open(&paths.state, task_id, turn_id).unwrap();
        let mut follow = None;
        let status = runner
            .follow_remote(
                &worker,
                task_id,
                turn_id,
                follow_task_status(TaskState::Active, turn_id),
                &mut log,
                &mut follow,
            )
            .unwrap();
        assert_eq!(status.state(), TaskState::Open);
        FollowPace {
            sleeps: clock.sleeps.lock().unwrap().clone(),
            status_logs: script.status_logs.load(Ordering::Relaxed),
            statuses: script.statuses.load(Ordering::Relaxed),
            task_statuses: script.task_statuses.load(Ordering::Relaxed),
        }
    }

    #[test]
    fn follow_survives_terminal_cleanup_recovery() {
        use crate::job::JobStatus;
        let pace = run_follow_script(vec![
            FollowStep {
                status: JobStatus::running(13, 1, 1, 2, 2).unwrap(),
                stdout: b"{\"type\":\"item.completed\"}\n".to_vec(),
            },
            FollowStep {
                status: JobStatus::succeeded(14, 26, 0)
                    .unwrap()
                    .with_cleanup_error("MUTABLE_CLEANUP_FAILED".into(), 15)
                    .unwrap(),
                stdout: Vec::new(),
            },
        ]);
        assert_eq!(pace.status_logs, 2);
        assert_eq!(pace.statuses, 1);
    }

    #[test]
    fn follow_backs_off_while_idle_and_resets_on_bytes_or_state_change() {
        use crate::job::JobStatus;
        let pace = run_follow_script(vec![
            FollowStep {
                status: JobStatus::accepted(11).unwrap(),
                stdout: Vec::new(),
            },
            FollowStep {
                status: JobStatus::accepted(12).unwrap(),
                stdout: Vec::new(),
            },
            FollowStep {
                status: JobStatus::accepted(12).unwrap(),
                stdout: vec![b'x'],
            },
            FollowStep {
                status: JobStatus::accepted(12).unwrap(),
                stdout: Vec::new(),
            },
            FollowStep {
                status: JobStatus::running(13, 1, 1, 2, 2).unwrap(),
                stdout: Vec::new(),
            },
            FollowStep {
                status: JobStatus::succeeded(14, 1, 0).unwrap(),
                stdout: Vec::new(),
            },
        ]);
        assert_eq!(
            pace.sleeps,
            [
                Duration::from_millis(100),
                Duration::from_millis(200),
                Duration::from_millis(100),
            ]
        );
        assert_eq!(pace.status_logs, 6);
        assert_eq!(pace.statuses, 1);
        assert_eq!(pace.task_statuses, 1);
    }

    #[test]
    fn follow_caps_idle_waits_and_polls_task_status_on_a_ten_second_idle_period() {
        use crate::job::JobStatus;
        let mut steps = Vec::new();
        for _ in 0..10 {
            steps.push(FollowStep {
                status: JobStatus::accepted(11).unwrap(),
                stdout: Vec::new(),
            });
        }
        steps.push(FollowStep {
            status: JobStatus::succeeded(12, 0, 0).unwrap(),
            stdout: Vec::new(),
        });
        let pace = run_follow_script(steps);
        assert_eq!(
            pace.sleeps,
            [
                Duration::from_millis(100),
                Duration::from_millis(200),
                Duration::from_millis(400),
                Duration::from_millis(800),
                Duration::from_millis(1_600),
                Duration::from_secs(2),
                Duration::from_secs(2),
                Duration::from_secs(2),
                Duration::from_secs(2),
                Duration::from_secs(2),
            ]
        );
        assert!(
            pace.sleeps
                .iter()
                .all(|delay| *delay <= Duration::from_secs(2))
        );
        assert_eq!(pace.status_logs, 11);
        assert_eq!(pace.statuses, 1);
        assert_eq!(pace.task_statuses, 2);
    }

    /// Answers `host probe` like a worker, or like an unreachable one.
    struct FactsProbe {
        stdout: Option<Vec<u8>>,
        probes: std::sync::atomic::AtomicUsize,
    }

    impl ProcessRunner for FactsProbe {
        fn run(
            &self,
            request: &crate::process::ProcessRequest,
        ) -> Result<ProcessResult, WorkerError> {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(
                request.args.last().and_then(|arg| arg.to_str()),
                Some("~/.local/bin/worker host probe")
            );
            self.probes
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(match &self.stdout {
                Some(stdout) => ProcessResult {
                    status: std::process::ExitStatus::from_raw(0),
                    stdout: stdout.clone(),
                    stderr: Vec::new(),
                },
                None => ProcessResult {
                    status: std::process::ExitStatus::from_raw(255 << 8),
                    stdout: Vec::new(),
                    stderr: b"ssh: connect to host mac1: Operation timed out\n".to_vec(),
                },
            })
        }
    }

    /// A probe reply as a helper sends it. Written as JSON and not as a
    /// `ProbeResponse` literal, so a field added to the probe later does not
    /// have to be listed here.
    fn probe_with_agents(agents: Option<Vec<(&str, Option<&str>)>>) -> Vec<u8> {
        let agent_facts = agents.map(|agents| {
            let agents = agents
                .into_iter()
                .map(|(name, version)| {
                    serde_json::json!({
                        "name": name,
                        "version": version,
                        "auth": "authenticated",
                        "auth_by_profile": [],
                    })
                })
                .collect::<Vec<_>>();
            serde_json::json!({
                "agents": agents,
                "env_profiles": [],
                "git_identity": true,
                "collected_at_millis": 1,
            })
        });
        serde_json::to_vec(&serde_json::json!({
            "protocol_version": crate::protocol::PROTOCOL_VERSION,
            "supervision_version": crate::protocol::SUPERVISION_VERSION,
            "hostname": "mini-1.local",
            "arch": "arm64",
            "os_version": "26.2",
            "free_disk_bytes": 500,
            "total_disk_bytes": 1_000,
            "memory_pressure": "normal",
            "swap_used_bytes": null,
            "slot_state": "idle",
            "active_lease": null,
            "capabilities": [],
            "facts_age_millis": agent_facts.as_ref().map(|_| 0),
            "agent_facts": agent_facts,
        }))
        .unwrap()
    }

    #[test]
    fn launch_version_is_read_from_the_selected_workers_facts_for_dialect_agents_only() {
        use crate::agent::AgentKind;
        use std::sync::atomic::Ordering;

        let (_root, paths) = isolated_handoff_paths();
        let store = super::ClientStateStore::open(&paths.state).unwrap();
        let config = super::Config::parse(
            "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
        )
        .unwrap();
        let worker = config.worker("mini-1").unwrap();
        let recorded = |stdout: Option<Vec<u8>>, agent: AgentKind| {
            let probe = FactsProbe {
                stdout,
                probes: std::sync::atomic::AtomicUsize::new(0),
            };
            let version = TurnRunner::new(
                &probe,
                &config,
                &paths,
                &store,
                &super::InlineRunnerExecutor,
            )
            .recorded_agent_version(worker, agent);
            (version, probe.probes.load(Ordering::Relaxed))
        };

        for (version, expected) in [
            (Some("2.0.18"), Some("2.0.18")),
            (Some("1.18.32"), Some("1.18.32")),
            (None, None),
        ] {
            let facts = probe_with_agents(Some(vec![
                ("codex", Some("0.154.0")),
                ("opencode", version),
            ]));
            assert_eq!(
                recorded(Some(facts), AgentKind::Opencode),
                (expected.map(str::to_owned), 1),
                "{version:?}"
            );
        }
        // Facts without the agent, a probe without facts, a reply that is not
        // a probe and an unreachable worker all leave the version unknown.
        for stdout in [
            Some(probe_with_agents(Some(vec![("codex", Some("2.0.18"))]))),
            Some(probe_with_agents(None)),
            Some(b"not a probe response".to_vec()),
            None,
        ] {
            assert_eq!(recorded(stdout, AgentKind::Opencode), (None, 1));
        }
        // The other agents have one dialect: no probe is spent on them.
        for agent in [AgentKind::Codex, AgentKind::Claude, AgentKind::Cursor] {
            let facts = probe_with_agents(Some(vec![
                ("codex", Some("2.0.18")),
                ("claude", Some("2.0.18")),
                ("cursor", Some("2.0.18")),
            ]));
            assert_eq!(recorded(Some(facts), agent), (None, 0), "{agent:?}");
        }
    }
}
