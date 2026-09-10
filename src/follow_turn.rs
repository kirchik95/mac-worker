//! The read-only pane process behind `worker host follow-turn`.
//!
//! The worker's herdr reporter opens one tab per turn and types this command
//! into the tab's pane. The process owns the pane's terminal title, renders
//! the turn's event stream the way `worker task logs -f` renders it on the
//! laptop, prints one outcome line when the turn ends, and then waits until
//! herdr closes the tab. It only ever reads: it takes no lock, creates and
//! writes nothing under the job or task directory, and never opens the
//! prompt. Its argv carries identifiers only, so nothing it prints or is
//! given names a filesystem path.

use std::{
    io::{self, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};

use crate::{
    RuntimeContext,
    agent::AgentKind,
    error::WorkerError,
    failure_receipt::STAGE_FOLLOW,
    host_store::HostStore,
    job::{JobId, MAX_LOG_CHUNK_BYTES},
    job_service::{ExecutionPayload, read_canonical_json},
    process::SystemProcessRunner,
    rooted_fs::{RootedDir, is_log_offset_beyond_eof},
    task::{TaskId, TaskOutcome, TaskStatus, TurnSummary, TurnTerminal},
    task_store::TaskStore,
    turn::LOG_CAP_BYTES,
    turn_log::render_agent_log,
};

/// How often the pane process looks for new log bytes and a terminal status.
pub const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Follows one turn until it ends and `stop` asks the process to leave.
///
/// Writes the OSC 0 title `task <id12> · <title>` to `out`, then every
/// complete line appended to the turn's `stdout.log` (rendered through the
/// shared turn log renderer) and `stderr.log` (verbatim), polling every
/// [`POLL_INTERVAL`]. When the task status records the turn as terminal it
/// prints one outcome line and keeps running until `stop()` returns true. A
/// `stop()` before the turn ends returns quietly.
///
/// Malformed identifiers are a usage error (exit `64`); a host root without
/// an installation, a missing job directory, or a job that is not a task turn
/// are protocol errors (exit `70`).
pub fn follow_turn(
    host_root: &Path,
    project_id: &str,
    worktree_id: &str,
    job_id: &str,
    out: &mut dyn Write,
    stop: &dyn Fn() -> bool,
) -> Result<(), WorkerError> {
    follow_turn_with_poll_interval(
        host_root,
        project_id,
        worktree_id,
        job_id,
        POLL_INTERVAL,
        out,
        stop,
    )
}

/// [`follow_turn`] with an explicit poll interval, for tests that drive the
/// log and the status themselves.
#[doc(hidden)]
pub fn follow_turn_with_poll_interval(
    host_root: &Path,
    project_id: &str,
    worktree_id: &str,
    job_id: &str,
    poll_interval: Duration,
    out: &mut dyn Write,
    stop: &dyn Fn() -> bool,
) -> Result<(), WorkerError> {
    validate_digest_component(project_id, "project identifier")?;
    validate_digest_component(worktree_id, "worktree identifier")?;
    let job_id: JobId = job_id
        .parse()
        .map_err(|_| invalid_component("job identifier"))?;

    let store = HostStore::open_if_present(host_root)?.ok_or_else(|| {
        WorkerError::Protocol("HOST_NOT_INSTALLED: host installation was not initialized".into())
    })?;
    let job = open_job_directory(&store, project_id, worktree_id, job_id)?;
    let turn = resolve_turn(&store, &job, project_id, job_id)?;
    let tasks = TaskStore::new(&store, &SystemProcessRunner);
    let meta = tasks.load_meta(project_id, turn.task_id)?;

    write!(
        out,
        "\x1b]0;task {} · {}\x07",
        short_task_id(turn.task_id),
        meta.title().as_str()
    )?;
    out.flush()?;

    let mut stdout_log = LogFollower::new("stdout.log");
    let mut stderr_log = LogFollower::new("stderr.log");
    let agent = turn.agent;
    let wrap = |error: WorkerError| {
        let lease = crate::lease::LeaseService::new(&store)
            .load()
            .ok()
            .flatten()
            .filter(|live| live.job_id() == job_id);
        store.attach_host_io(error, STAGE_FOLLOW, lease.as_ref(), Some(&job))
    };
    loop {
        stdout_log
            .pump(&job, &mut |lines| render_agent_log(lines, agent, out))
            .map_err(&wrap)?;
        stderr_log
            .pump(&job, &mut |lines| {
                out.write_all(lines).map_err(WorkerError::Io)
            })
            .map_err(&wrap)?;
        if let Some((summary, status)) = terminal_turn(&tasks, project_id, turn.task_id, job_id)? {
            // The status is written after the last log byte; drain what
            // landed between the log read above and the status read, then
            // show a trailing line the agent never terminated.
            stdout_log
                .pump(&job, &mut |lines| render_agent_log(lines, agent, out))
                .map_err(&wrap)?;
            stderr_log
                .pump(&job, &mut |lines| {
                    out.write_all(lines).map_err(WorkerError::Io)
                })
                .map_err(&wrap)?;
            stdout_log
                .finish(&mut |lines| render_agent_log(lines, agent, out))
                .map_err(&wrap)?;
            stderr_log
                .finish(&mut |lines| out.write_all(lines).map_err(WorkerError::Io))
                .map_err(&wrap)?;
            writeln!(out, "{}", outcome_line(&summary, &status))?;
            out.flush()?;
            break;
        }
        out.flush()?;
        if stop() {
            return Ok(());
        }
        thread::sleep(poll_interval);
    }

    while !stop() {
        thread::sleep(poll_interval);
    }
    Ok(())
}

/// Runs the hidden `worker host follow-turn` command: discovers the host
/// root, arms `SIGTERM` and `SIGHUP` as the stop request herdr sends when the
/// tab closes, and follows the turn on `stdout`. Returns the process exit
/// code.
pub fn run_host_follow_turn(
    config_override: Option<PathBuf>,
    runtime: &RuntimeContext,
    project_id: &str,
    worktree_id: &str,
    job_id: &str,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    let result = (|| -> Result<(), WorkerError> {
        let paths = crate::discover_paths(config_override, runtime)?;
        install_stop_signals()?;
        follow_turn(
            &paths.host_state_root(),
            project_id,
            worktree_id,
            job_id,
            stdout,
            &|| STOP_REQUESTED.load(Ordering::SeqCst),
        )
    })();
    match result {
        Ok(()) => 0,
        Err(error) => {
            crate::write_error(stderr, &error);
            error.exit_code()
        }
    }
}

static STOP_REQUESTED: AtomicBool = AtomicBool::new(false);

extern "C" fn request_stop(_signal: libc::c_int) {
    STOP_REQUESTED.store(true, Ordering::SeqCst);
}

fn install_stop_signals() -> Result<(), WorkerError> {
    let handler = request_stop as extern "C" fn(libc::c_int) as libc::sighandler_t;
    for signal in [libc::SIGTERM, libc::SIGHUP] {
        if unsafe { libc::signal(signal, handler) } == libc::SIG_ERR {
            return Err(WorkerError::Io(io::Error::last_os_error()));
        }
    }
    Ok(())
}

/// What the pane follows: the task the turn belongs to and the agent whose
/// stream is in `stdout.log`.
struct FollowedTurn {
    task_id: TaskId,
    agent: AgentKind,
}

fn open_job_directory(
    store: &HostStore,
    project_id: &str,
    worktree_id: &str,
    job_id: JobId,
) -> Result<RootedDir, WorkerError> {
    match store.open_directory(&format!("jobs/{project_id}/{worktree_id}/{job_id}"), false) {
        Ok(job) => Ok(job),
        Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => Err(
            WorkerError::Protocol("JOB_NOT_FOUND: job directory does not exist".into()),
        ),
        Err(error) => Err(error),
    }
}

/// Finds the task turn a job directory belongs to. A running turn carries its
/// section in `execution.json`; that payload is removed once the turn has
/// been published, so a finished turn is found through the task whose
/// history lists the job id. A batch job matches neither.
fn resolve_turn(
    store: &HostStore,
    job: &RootedDir,
    project_id: &str,
    job_id: JobId,
) -> Result<FollowedTurn, WorkerError> {
    if job.entry_exists("execution.json")? {
        let payload: ExecutionPayload = read_canonical_json(job, "execution.json")?;
        let section = payload.turn().ok_or_else(not_a_turn)?;
        if section.project_id() != project_id {
            return Err(not_a_turn());
        }
        return Ok(FollowedTurn {
            task_id: section.turn().task_id(),
            agent: section.turn().agent(),
        });
    }
    find_published_turn(store, project_id, job_id)?.ok_or_else(not_a_turn)
}

fn find_published_turn(
    store: &HostStore,
    project_id: &str,
    job_id: JobId,
) -> Result<Option<FollowedTurn>, WorkerError> {
    let tasks_dir = match store.open_directory(&format!("tasks/{project_id}"), false) {
        Ok(directory) => directory,
        Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let tasks = TaskStore::new(store, &SystemProcessRunner);
    for name in tasks_dir.list_names()? {
        let Ok(task_id) = String::from_utf8(name)
            .ok()
            .and_then(|name| name.parse::<TaskId>().ok())
            .ok_or(())
        else {
            continue;
        };
        let Ok(status) = tasks.load_status(project_id, task_id) else {
            continue;
        };
        if status.turns().iter().any(|turn| turn.turn_id() == job_id) {
            let meta = tasks.load_meta(project_id, task_id)?;
            return Ok(Some(FollowedTurn {
                task_id,
                agent: meta.agent(),
            }));
        }
    }
    Ok(None)
}

/// The task status once it records the followed turn as terminal.
fn terminal_turn(
    tasks: &TaskStore<'_>,
    project_id: &str,
    task_id: TaskId,
    job_id: JobId,
) -> Result<Option<(TurnSummary, TaskStatus)>, WorkerError> {
    let status = match tasks.load_status(project_id, task_id) {
        Ok(status) => status,
        // The publisher replaces the record atomically; a read that raced the
        // replacement is retried on the next poll.
        Err(WorkerError::Io(error)) if is_transient(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    let turn = status
        .turns()
        .iter()
        .find(|turn| turn.turn_id() == job_id && turn.terminal().is_some())
        .cloned();
    Ok(turn.map(|turn| (turn, status)))
}

/// One line for the pane once the turn has ended: `turn <n>: <outcome kind>`
/// with the summary, the first question, or the failure reason after an em
/// dash when the outcome carries one. The texts are the redacted values the
/// publisher wrote into the task status, the same ones `task status` shows.
fn outcome_line(turn: &TurnSummary, status: &TaskStatus) -> String {
    let number = turn.turn_number();
    let Some(outcome) = turn.outcome() else {
        let terminal = match turn.terminal() {
            Some(TurnTerminal::Succeeded) => "succeeded",
            Some(TurnTerminal::Failed) => "failed",
            Some(TurnTerminal::Cancelled) => "cancelled",
            Some(TurnTerminal::TimedOut) => "timed_out",
            Some(TurnTerminal::Lost) | None => "lost",
        };
        return format!("turn {number}: {terminal}");
    };
    let summary = status.summary().filter(|summary| !summary.is_empty());
    let question = status.questions().first().map(|question| question.text());
    let detail = match outcome {
        TaskOutcome::Done | TaskOutcome::Unknown => summary,
        TaskOutcome::NeedsInput => question.or(summary),
        TaskOutcome::Blocked => summary.or(question),
        TaskOutcome::Failed { reason } => Some(reason.as_str()),
        TaskOutcome::Cancelled | TaskOutcome::TimedOut | TaskOutcome::Lost => None,
    };
    match detail {
        Some(detail) => format!(
            "turn {number}: {} — {}",
            outcome.kind(),
            detail.replace(['\r', '\n'], " ")
        ),
        None => format!("turn {number}: {}", outcome.kind()),
    }
}

/// Reads one turn log by offset, exactly like `log-chunk`, and hands out only
/// complete lines so an event the agent is still writing is never rendered
/// in two halves.
struct LogFollower {
    name: &'static str,
    offset: u64,
    pending: Vec<u8>,
}

impl LogFollower {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            offset: 0,
            pending: Vec::new(),
        }
    }

    /// Reads every byte appended since the previous poll, up to the log cap,
    /// and passes the complete lines among them to `sink`.
    fn pump(
        &mut self,
        job: &RootedDir,
        sink: &mut dyn FnMut(&[u8]) -> Result<(), WorkerError>,
    ) -> Result<(), WorkerError> {
        while self.offset < LOG_CAP_BYTES {
            let limit =
                usize::try_from((LOG_CAP_BYTES - self.offset).min(MAX_LOG_CHUNK_BYTES as u64))
                    .expect("a bounded chunk fits in usize");
            let bytes = match job.read_private_regular_chunk(self.name, self.offset, limit) {
                Ok(bytes) => bytes,
                Err(error) if is_transient(&error) => break,
                Err(error) if is_log_offset_beyond_eof(&error) => {
                    return Err(WorkerError::Protocol(
                        "LOG_OFFSET_BEYOND_EOF: turn log shrank while it was followed".into(),
                    ));
                }
                Err(error) => return Err(WorkerError::Io(error)),
            };
            if bytes.is_empty() {
                break;
            }
            let read = bytes.len();
            self.offset += read as u64;
            self.pending.extend_from_slice(&bytes);
            if read < limit {
                break;
            }
        }
        if let Some(end) = self.pending.iter().rposition(|byte| *byte == b'\n') {
            let complete: Vec<u8> = self.pending.drain(..=end).collect();
            sink(&complete)?;
        }
        Ok(())
    }

    /// Hands the unterminated tail to `sink` once nothing more can arrive.
    fn finish(
        &mut self,
        sink: &mut dyn FnMut(&[u8]) -> Result<(), WorkerError>,
    ) -> Result<(), WorkerError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let rest = std::mem::take(&mut self.pending);
        sink(&rest)
    }
}

fn is_transient(error: &io::Error) -> bool {
    error.raw_os_error() == Some(libc::ESTALE) || error.kind() == io::ErrorKind::Interrupted
}

fn short_task_id(task_id: TaskId) -> String {
    task_id.to_string().chars().take(12).collect()
}

fn validate_digest_component(value: &str, label: &str) -> Result<(), WorkerError> {
    let valid = value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase());
    if valid {
        Ok(())
    } else {
        Err(invalid_component(label))
    }
}

fn invalid_component(label: &str) -> WorkerError {
    WorkerError::task("INVALID_COMPONENT", format!("invalid {label}"))
}

fn not_a_turn() -> WorkerError {
    WorkerError::Protocol("NOT_A_TURN: job is not a task turn".into())
}
