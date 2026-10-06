use std::{
    collections::VecDeque,
    ffi::OsString,
    io::{self, Read, Write},
    os::unix::process::CommandExt,
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};

use crate::error::{ProcessError, ProcessStream, WorkerError};

// A fork racing killpg can survive the first signal in the owned group.
const PROCESS_GROUP_KILL_BUDGET: Duration = Duration::from_secs(2);
// Escaped descendants can retain pipe FDs even after the owned group is gone.
const TERMINATED_DRAIN_GRACE: Duration = Duration::from_secs(2);
// Capture, stdin and exit events wake the poll loop at once and its waits end
// at the deadline, so this interval only sets how soon a `should_stop` that
// turned true is noticed: far below the 2 s kill and drain budgets and any
// Ctrl-C delay an operator notices, with 20 timers a second instead of 500.
const POLL_INTERVAL: Duration = Duration::from_millis(50);
// Status polling once every event sender is gone, which only happens when the
// exit watcher could not start: then nothing else wakes the loop on exit.
const DISCONNECTED_STATUS_POLL: Duration = Duration::from_millis(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessPolicy {
    pub stdout_limit: usize,
    pub stderr_limit: usize,
    pub deadline: Duration,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ProcessRequest {
    pub program: OsString,
    pub args: Vec<OsString>,
    pub environment: Vec<(OsString, OsString)>,
    pub environment_remove: Vec<OsString>,
    pub stdin: Option<Vec<u8>>,
    pub policy: ProcessPolicy,
    /// When true, the runner clears inherited environment variables before
    /// applying `environment`.
    pub isolate_parent_environment: bool,
}

impl std::fmt::Debug for ProcessRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessRequest")
            .field("program", &self.program)
            .field("argument_count", &self.args.len())
            .field("environment_count", &self.environment.len())
            .field("environment_remove_count", &self.environment_remove.len())
            .field("stdin_bytes", &self.stdin.as_ref().map(Vec::len))
            .field("policy", &self.policy)
            .field(
                "isolate_parent_environment",
                &self.isolate_parent_environment,
            )
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct ProcessResult {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// Positive ownership cleanup evidence, independent of the application outcome.
/// Completed requires reap/owned-group absence and joined capture/stdin threads
/// (or proof no child existed). Expired grace, detachment or panic is Unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupState {
    Completed,
    Unknown,
}

#[derive(Debug)]
pub struct ProcessCompletion {
    pub outcome: Result<ProcessResult, WorkerError>,
    pub cleanup: CleanupState,
}

/// Companion seam; legacy ProcessRunner retains exactly its three methods.
/// Implementors must supply evidence explicitly, including error/cancel paths.
pub trait TrackedProcessRunner: ProcessRunner {
    fn run_interruptible_with_cleanup(
        &self,
        request: &ProcessRequest,
        should_stop: &dyn Fn() -> bool,
    ) -> ProcessCompletion;
}

pub trait ProcessRunner: Send + Sync {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError>;

    fn run_in_new_session(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.run(request)
    }

    /// Runs `request` while polling `should_stop`. The default implementation
    /// checks once before `run` and cannot interrupt a child that is already
    /// executing; `SystemProcessRunner` monitors the live process group.
    fn run_interruptible(
        &self,
        request: &ProcessRequest,
        should_stop: &dyn Fn() -> bool,
    ) -> Result<ProcessResult, WorkerError> {
        if should_stop() {
            return Err(ProcessError::Cancelled.into());
        }
        self.run(request)
    }
}

impl<T: ProcessRunner + ?Sized> ProcessRunner for &T {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        (**self).run(request)
    }

    fn run_in_new_session(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        (**self).run_in_new_session(request)
    }

    fn run_interruptible(
        &self,
        request: &ProcessRequest,
        should_stop: &dyn Fn() -> bool,
    ) -> Result<ProcessResult, WorkerError> {
        (**self).run_interruptible(request, should_stop)
    }
}

impl<T: ProcessRunner + ?Sized> ProcessRunner for Arc<T> {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        (**self).run(request)
    }
    fn run_in_new_session(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        (**self).run_in_new_session(request)
    }
    fn run_interruptible(
        &self,
        request: &ProcessRequest,
        should_stop: &dyn Fn() -> bool,
    ) -> Result<ProcessResult, WorkerError> {
        (**self).run_interruptible(request, should_stop)
    }
}

impl<T: TrackedProcessRunner + ?Sized> TrackedProcessRunner for &T {
    fn run_interruptible_with_cleanup(
        &self,
        request: &ProcessRequest,
        should_stop: &dyn Fn() -> bool,
    ) -> ProcessCompletion {
        (**self).run_interruptible_with_cleanup(request, should_stop)
    }
}
impl<T: TrackedProcessRunner + ?Sized> TrackedProcessRunner for Arc<T> {
    fn run_interruptible_with_cleanup(
        &self,
        request: &ProcessRequest,
        should_stop: &dyn Fn() -> bool,
    ) -> ProcessCompletion {
        (**self).run_interruptible_with_cleanup(request, should_stop)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemProcessRunner;

/// Runs children in the caller's process group so a recorded `killpg` of the
/// gated turn child also reaps setup descendants. Local timeout/cap/cancel
/// kills only the spawned PID and does not join capture threads: grandchildren
/// may keep pipe FDs, and joining would wait until they exit. The caller must
/// `_exit` the recorded child so supervisor group cleanup finishes the reap.
#[derive(Debug, Clone, Copy, Default)]
pub struct InheritProcessGroupRunner;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionKind {
    Inherit,
    NewProcessGroup,
    NewSession,
}

impl ProcessRunner for SystemProcessRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.run_with_session(request, SessionKind::NewProcessGroup, &|| false)
            .outcome
    }

    fn run_in_new_session(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.run_with_session(request, SessionKind::NewSession, &|| false)
            .outcome
    }

    fn run_interruptible(
        &self,
        request: &ProcessRequest,
        should_stop: &dyn Fn() -> bool,
    ) -> Result<ProcessResult, WorkerError> {
        self.run_with_session(request, SessionKind::NewProcessGroup, should_stop)
            .outcome
    }
}

impl TrackedProcessRunner for SystemProcessRunner {
    fn run_interruptible_with_cleanup(
        &self,
        request: &ProcessRequest,
        should_stop: &dyn Fn() -> bool,
    ) -> ProcessCompletion {
        self.run_with_session(request, SessionKind::NewProcessGroup, should_stop)
    }
}

impl ProcessRunner for InheritProcessGroupRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        SystemProcessRunner
            .run_with_session(request, SessionKind::Inherit, &|| false)
            .outcome
    }

    fn run_in_new_session(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        SystemProcessRunner
            .run_with_session(request, SessionKind::NewSession, &|| false)
            .outcome
    }

    fn run_interruptible(
        &self,
        request: &ProcessRequest,
        should_stop: &dyn Fn() -> bool,
    ) -> Result<ProcessResult, WorkerError> {
        SystemProcessRunner
            .run_with_session(request, SessionKind::Inherit, should_stop)
            .outcome
    }
}

/// Wait policy of the poll loop. Unit tests stand in a lost timer wakeup with
/// a much longer interval and observe the loop right before each wait.
struct PollControl {
    interval: Duration,
    #[cfg(test)]
    before_wait: Option<Arc<dyn Fn(PollObservation) + Send + Sync>>,
}

impl PollControl {
    fn production() -> Self {
        Self {
            interval: POLL_INTERVAL,
            #[cfg(test)]
            before_wait: None,
        }
    }
}

/// What the poll loop has observed when it is about to wait.
#[cfg(test)]
#[derive(Debug, Clone, Copy)]
struct PollObservation {
    status: bool,
    stdout: bool,
    stderr: bool,
    stdin: bool,
}

impl SystemProcessRunner {
    fn run_with_session(
        &self,
        request: &ProcessRequest,
        session: SessionKind,
        should_stop: &dyn Fn() -> bool,
    ) -> ProcessCompletion {
        self.run_with_poll(request, session, should_stop, &PollControl::production())
    }

    fn run_with_poll(
        &self,
        request: &ProcessRequest,
        session: SessionKind,
        should_stop: &dyn Fn() -> bool,
        control: &PollControl,
    ) -> ProcessCompletion {
        // A spawn failure proves that no child or I/O threads existed. Once a
        // child exists, only observed reap/group absence AND joins restore it.
        let mut cleanup = CleanupState::Completed;
        let outcome =
            self.run_with_session_inner(request, session, should_stop, control, &mut cleanup);
        ProcessCompletion { outcome, cleanup }
    }

    fn run_with_session_inner(
        &self,
        request: &ProcessRequest,
        session: SessionKind,
        should_stop: &dyn Fn() -> bool,
        control: &PollControl,
        cleanup: &mut CleanupState,
    ) -> Result<ProcessResult, WorkerError> {
        let mut command = Command::new(&request.program);
        if request.isolate_parent_environment {
            command.env_clear();
        }
        command
            .args(&request.args)
            .envs(request.environment.iter().map(|(key, value)| (key, value)));
        for key in &request.environment_remove {
            command.env_remove(key);
        }
        match session {
            SessionKind::NewSession => unsafe {
                command.pre_exec(|| {
                    if libc::setsid() == -1 {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(())
                    }
                });
            },
            SessionKind::NewProcessGroup => {
                command.process_group(0);
            }
            SessionKind::Inherit => {}
        }
        command
            .stdin(if request.stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = command.spawn()?;
        *cleanup = CleanupState::Unknown;
        let process_group = match session {
            SessionKind::Inherit => None,
            SessionKind::NewProcessGroup | SessionKind::NewSession => {
                Some(child.id() as libc::pid_t)
            }
        };
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("child process stdout was not available"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| io::Error::other("child process stderr was not available"))?;

        let (sender, receiver) = mpsc::channel();
        let stdout_handle = spawn_capture(
            stdout,
            ProcessStream::Stdout,
            request.policy.stdout_limit,
            sender.clone(),
        );
        let stderr_handle = spawn_capture(
            stderr,
            ProcessStream::Stderr,
            request.policy.stderr_limit,
            sender.clone(),
        );
        let stdin_handle = if let Some(input) = request.stdin.clone() {
            let mut stdin = child.stdin.take().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "child process stdin was not available",
                )
            })?;
            let sender = sender.clone();
            Some(thread::spawn(move || {
                let result = stdin.write_all(&input);
                let _ = sender.send(ProcessEvent::Stdin(result));
            }))
        } else {
            None
        };
        // The watcher takes the last sender: the channel disconnects only
        // once it and every I/O thread are done.
        spawn_exit_watcher(child.id(), sender);

        let started = Instant::now();
        let mut status = None;
        let mut stdout = None;
        let mut stderr = None;
        let mut stdin_complete = stdin_handle.is_none();
        let mut stdin_error = None;
        let mut woken_by = None;

        loop {
            while let Some(event) = woken_by.take().or_else(|| receiver.try_recv().ok()) {
                match event {
                    ProcessEvent::Captured(stream, Ok(CaptureOutcome::Complete(bytes))) => {
                        match stream {
                            ProcessStream::Stdout => stdout = Some(bytes),
                            ProcessStream::Stderr => stderr = Some(bytes),
                        }
                    }
                    ProcessEvent::Captured(stream, Ok(CaptureOutcome::LimitExceeded)) => {
                        *cleanup = cleanup_terminated_child(
                            &mut child,
                            process_group,
                            stdout_handle,
                            stderr_handle,
                            stdin_handle,
                            session,
                        )?;
                        let limit = match stream {
                            ProcessStream::Stdout => request.policy.stdout_limit,
                            ProcessStream::Stderr => request.policy.stderr_limit,
                        };
                        return Err(ProcessError::OutputLimitExceeded { stream, limit }.into());
                    }
                    ProcessEvent::Captured(_, Err(error)) => {
                        *cleanup = cleanup_terminated_child(
                            &mut child,
                            process_group,
                            stdout_handle,
                            stderr_handle,
                            stdin_handle,
                            session,
                        )?;
                        return Err(error.into());
                    }
                    ProcessEvent::Stdin(Err(error)) => {
                        stdin_error = Some(error);
                        stdin_complete = true;
                    }
                    ProcessEvent::Stdin(Ok(())) => stdin_complete = true,
                    // Only a wakeup: `try_wait` below still reaps the child.
                    ProcessEvent::Exited => {}
                }
            }

            if status.is_none() {
                status = child.try_wait()?;
            }
            if status.is_some() && stdout.is_some() && stderr.is_some() && stdin_complete {
                break;
            }

            if should_stop() {
                *cleanup = cleanup_terminated_child(
                    &mut child,
                    process_group,
                    stdout_handle,
                    stderr_handle,
                    stdin_handle,
                    session,
                )?;
                return Err(ProcessError::Cancelled.into());
            }

            let elapsed = started.elapsed();
            if elapsed >= request.policy.deadline {
                *cleanup = cleanup_terminated_child(
                    &mut child,
                    process_group,
                    stdout_handle,
                    stderr_handle,
                    stdin_handle,
                    session,
                )?;
                return Err(ProcessError::DeadlineExceeded {
                    deadline: request.policy.deadline,
                }
                .into());
            }

            #[cfg(test)]
            if let Some(observe) = &control.before_wait {
                observe(PollObservation {
                    status: status.is_some(),
                    stdout: stdout.is_some(),
                    stderr: stderr.is_some(),
                    stdin: stdin_complete,
                });
            }
            woken_by = wait_for_event(
                &receiver,
                control
                    .interval
                    .min(request.policy.deadline.saturating_sub(elapsed)),
            );
        }

        join_threads(stdout_handle, stderr_handle, stdin_handle)?;
        if process_group.is_some_and(saved_process_group_is_gone) {
            *cleanup = CleanupState::Completed;
        }
        let status = status.expect("completed child must have an exit status");
        if status.success()
            && let Some(error) = stdin_error
        {
            return Err(error.into());
        }
        Ok(ProcessResult {
            status,
            stdout: stdout.expect("completed stdout capture must have bytes"),
            stderr: stderr.expect("completed stderr capture must have bytes"),
        })
    }
}

enum ProcessEvent {
    Captured(ProcessStream, io::Result<CaptureOutcome>),
    Stdin(io::Result<()>),
    /// The exit watcher returned: the child exited, or it can no longer wait.
    Exited,
}

/// Blocks until the next event or `timeout`. Every completion event wakes the
/// loop directly, so finishing a run never depends on a timer firing; a timer
/// lost across system sleep can only delay cancellation and the deadline.
fn wait_for_event(
    receiver: &mpsc::Receiver<ProcessEvent>,
    timeout: Duration,
) -> Option<ProcessEvent> {
    match receiver.recv_timeout(timeout) {
        Ok(event) => Some(event),
        Err(mpsc::RecvTimeoutError::Timeout) => None,
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            thread::sleep(DISCONNECTED_STATUS_POLL.min(timeout));
            None
        }
    }
}

/// Sends `Exited` once `pid` has exited, without reaping it: WNOWAIT leaves
/// the status for `try_wait`, which stays the only reaper, so the status and
/// every cleanup path are unchanged. The thread holds nothing of the child and
/// is never joined: after the reap `waitid` fails at once with ECHILD. If the
/// thread cannot start, the loop still wakes on its bounded interval.
fn spawn_exit_watcher(pid: u32, sender: mpsc::Sender<ProcessEvent>) {
    let _ = thread::Builder::new().spawn(move || {
        loop {
            let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
            // SAFETY: `info` is a writable siginfo_t for the duration of the call.
            let result = unsafe {
                libc::waitid(
                    libc::P_PID,
                    pid as libc::id_t,
                    info.as_mut_ptr(),
                    libc::WEXITED | libc::WNOWAIT,
                )
            };
            if result == 0 || io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                break;
            }
        }
        let _ = sender.send(ProcessEvent::Exited);
    });
}

enum CaptureOutcome {
    Complete(Vec<u8>),
    LimitExceeded,
}

struct CaptureThread {
    handle: thread::JoinHandle<()>,
    limit_release: mpsc::Sender<()>,
}

fn spawn_capture<R: Read + Send + 'static>(
    mut reader: R,
    stream: ProcessStream,
    limit: usize,
    sender: mpsc::Sender<ProcessEvent>,
) -> CaptureThread {
    let (limit_release, release_receiver) = mpsc::channel();
    let handle = thread::spawn(move || {
        let mut captured = Vec::with_capacity(limit.min(8 * 1024));
        let mut buffer = [0_u8; 8 * 1024];
        let result = loop {
            match reader.read(&mut buffer) {
                Ok(0) => break Ok(CaptureOutcome::Complete(captured)),
                Ok(read) => {
                    let remaining = limit.saturating_sub(captured.len());
                    let accepted = remaining.min(read);
                    captured.extend_from_slice(&buffer[..accepted]);
                    if read > remaining {
                        let _ = sender.send(ProcessEvent::Captured(
                            stream,
                            Ok(CaptureOutcome::LimitExceeded),
                        ));
                        // Keep draining so a finite writer can exit. Teardown
                        // bounds the wait for this thread if a descendant
                        // retains the pipe, including outside the owned group.
                        drain_remaining(&mut reader);
                        let _ = release_receiver.recv();
                        return;
                    }
                }
                Err(error) => break Err(error),
            }
        };
        let _ = sender.send(ProcessEvent::Captured(stream, result));
    });
    CaptureThread {
        handle,
        limit_release,
    }
}

fn drain_remaining<R: Read>(reader: &mut R) {
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => break,
        }
    }
}

fn finish_terminated_child(
    stdout: CaptureThread,
    stderr: CaptureThread,
    stdin: Option<thread::JoinHandle<()>>,
    session: SessionKind,
) -> io::Result<CleanupState> {
    let started = Instant::now();
    finish_terminated_child_with_budget(stdout, stderr, stdin, session, &|| {
        started.elapsed() >= TERMINATED_DRAIN_GRACE
    })
}

fn finish_terminated_child_with_budget(
    stdout: CaptureThread,
    stderr: CaptureThread,
    stdin: Option<thread::JoinHandle<()>>,
    session: SessionKind,
    expired: &dyn Fn() -> bool,
) -> io::Result<CleanupState> {
    if session == SessionKind::Inherit {
        // ENV setup runs inside the recorded gated child's group. Local
        // timeout/cap/cancel must not killpg that group (it would suicide the
        // exec'd helper) and must not join capture threads: grandchildren can
        // keep pipe FDs, and joining would wait until they exit. Detach the
        // handles and `_exit` the helper so supervisor `killpg` finishes reap.
        abandon_capture_threads(stdout, stderr, stdin);
        return Ok(CleanupState::Unknown);
    }

    // Overflow captures wait on this gate after draining. Release it before
    // polling completion, otherwise they cannot finish even after EOF.
    let _ = stdout.limit_release.send(());
    let _ = stderr.limit_release.send(());
    while !stdout.handle.is_finished()
        || !stderr.handle.is_finished()
        || stdin.as_ref().is_some_and(|handle| !handle.is_finished())
    {
        if expired() {
            abandon_capture_threads(stdout, stderr, stdin);
            return Ok(CleanupState::Unknown);
        }
        thread::sleep(Duration::from_millis(2));
    }
    join_threads(stdout, stderr, stdin).map(|_| CleanupState::Completed)
}

fn abandon_capture_threads(
    stdout: CaptureThread,
    stderr: CaptureThread,
    stdin: Option<thread::JoinHandle<()>>,
) {
    let _ = stdout.limit_release.send(());
    let _ = stderr.limit_release.send(());
    // Dropping a JoinHandle detaches the thread without leaking the handle's
    // allocation. Late event sends already ignore a disconnected receiver.
    drop((stdout.handle, stderr.handle, stdin));
}

fn reap_owned_child(child: &mut Child) -> io::Result<()> {
    if child.try_wait()?.is_some() {
        return Ok(());
    }
    if let Err(error) = child.kill()
        && error.raw_os_error() != Some(libc::ESRCH)
    {
        return Err(error);
    }
    child.wait().map(|_| ())
}

/// Non-signalling probe of the saved pgid. Signal 0 does not kill a reused
/// group. Only ESRCH means `pgrp_find` missed; POSIX empty/zombie-only groups
/// still return EPERM and must not be treated as gone.
fn saved_process_group_is_gone(process_group: libc::pid_t) -> bool {
    (unsafe { libc::killpg(process_group, 0) }) == -1
        && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
}

fn recover_after_killpg_eperm(
    child: &mut Child,
    process_group: libc::pid_t,
    error: io::Error,
) -> io::Result<()> {
    if error.raw_os_error() != Some(libc::EPERM) {
        return Err(error);
    }
    // Reap the owned leader only. getpgid(leader) ESRCH and kill(pid) do not
    // prove the saved group is empty. Accept the original EPERM only if
    // killpg of that same pgid with signal 0 is ESRCH.
    reap_owned_child(child)?;
    if saved_process_group_is_gone(process_group) {
        return Ok(());
    }
    Err(error)
}

fn terminate_child(
    child: &mut Child,
    process_group: Option<libc::pid_t>,
) -> io::Result<CleanupState> {
    let started = Instant::now();
    terminate_child_with_evidence(
        child,
        process_group,
        &|| started.elapsed() >= PROCESS_GROUP_KILL_BUDGET,
        &saved_process_group_is_gone,
    )
}

fn cleanup_terminated_child(
    child: &mut Child,
    process_group: Option<libc::pid_t>,
    stdout: CaptureThread,
    stderr: CaptureThread,
    stdin: Option<thread::JoinHandle<()>>,
    session: SessionKind,
) -> io::Result<CleanupState> {
    let process = terminate_child(child, process_group)?;
    let captures = finish_terminated_child(stdout, stderr, stdin, session)?;
    Ok(
        if process == CleanupState::Completed && captures == CleanupState::Completed {
            CleanupState::Completed
        } else {
            CleanupState::Unknown
        },
    )
}

fn terminate_child_with_evidence(
    child: &mut Child,
    process_group: Option<libc::pid_t>,
    expired: &dyn Fn() -> bool,
    group_is_gone: &dyn Fn(libc::pid_t) -> bool,
) -> io::Result<CleanupState> {
    if let Some(process_group) = process_group {
        loop {
            if let Err(error) = terminate_process_group(process_group) {
                recover_after_killpg_eperm(child, process_group, error)?;
                return Ok(if group_is_gone(process_group) {
                    CleanupState::Completed
                } else {
                    CleanupState::Unknown
                });
            }
            // Reap before probing: our own zombie leader would keep the group
            // present. Child caches this status on subsequent iterations.
            child.wait()?;
            if group_is_gone(process_group) {
                return Ok(CleanupState::Completed);
            }
            if expired() {
                return Ok(CleanupState::Unknown);
            }
            thread::sleep(Duration::from_millis(2));
        }
    }
    if let Err(error) = child.kill()
        && error.raw_os_error() != Some(libc::ESRCH)
    {
        return Err(error);
    }
    child.wait().map(|_| CleanupState::Completed)
}

fn terminate_process_group(process_group: libc::pid_t) -> io::Result<()> {
    if process_group <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid child process group",
        ));
    }

    if unsafe { libc::killpg(process_group, libc::SIGKILL) } == -1 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error);
        }
    }
    Ok(())
}

fn join_threads(
    stdout: CaptureThread,
    stderr: CaptureThread,
    stdin: Option<thread::JoinHandle<()>>,
) -> io::Result<()> {
    let _ = stdout.limit_release.send(());
    let _ = stderr.limit_release.send(());
    let stdout_result = stdout
        .handle
        .join()
        .map_err(|_| io::Error::other("stdout capture thread panicked"));
    let stderr_result = stderr
        .handle
        .join()
        .map_err(|_| io::Error::other("stderr capture thread panicked"));
    let stdin_result = if let Some(stdin) = stdin {
        stdin
            .join()
            .map_err(|_| io::Error::other("stdin writer thread panicked"))
    } else {
        Ok(())
    };
    stdout_result.and(stderr_result).and(stdin_result)
}

/// Bounded rolling byte window for later fixed-phrase scanners.
///
/// The window size is the maximum needle length. Auth-incident can scan this
/// without allocating a 256 MiB stderr buffer.
#[derive(Debug)]
pub(crate) struct RollingByteWindow {
    bytes: VecDeque<u8>,
    max: usize,
}

impl RollingByteWindow {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            bytes: VecDeque::with_capacity(max.min(8 * 1024)),
            max: max.max(1),
        }
    }

    pub(crate) fn extend(&mut self, incoming: &[u8]) {
        if incoming.len() >= self.max {
            self.bytes.clear();
            self.bytes
                .extend(incoming[incoming.len() - self.max..].iter().copied());
            return;
        }
        let over = self
            .bytes
            .len()
            .saturating_add(incoming.len())
            .saturating_sub(self.max);
        for _ in 0..over {
            let _ = self.bytes.pop_front();
        }
        self.bytes.extend(incoming.iter().copied());
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn as_bytes(&self) -> Vec<u8> {
        self.bytes.iter().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ProcessStream;
    use std::{
        os::unix::process::CommandExt,
        time::{Duration, Instant},
    };

    fn policy() -> ProcessPolicy {
        ProcessPolicy {
            stdout_limit: 4096,
            stderr_limit: 4096,
            deadline: Duration::from_secs(2),
        }
    }

    fn cleanup_request(script: &str) -> ProcessRequest {
        ProcessRequest {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), script.into()],
            environment: Vec::new(),
            environment_remove: Vec::new(),
            stdin: None,
            policy: ProcessPolicy {
                stdout_limit: 4096,
                stderr_limit: 4096,
                deadline: Duration::from_secs(30),
            },
            isolate_parent_environment: false,
        }
    }

    #[test]
    fn tracked_success_and_nonzero_exit_preserve_legacy_outcomes() {
        for script in [
            "printf out; printf err >&2",
            "printf out; printf err >&2; exit 75",
        ] {
            let request = cleanup_request(script);
            let legacy = SystemProcessRunner.run(&request).unwrap();
            let completion =
                SystemProcessRunner.run_interruptible_with_cleanup(&request, &|| false);
            assert_eq!(completion.cleanup, CleanupState::Completed);
            let tracked = completion.outcome.unwrap();
            assert_eq!(tracked.status, legacy.status);
            assert_eq!(tracked.stdout, legacy.stdout);
            assert_eq!(tracked.stderr, legacy.stderr);
        }
    }

    /// Stands in for a timer wakeup lost across system sleep: the production
    /// loop waits a bounded interval, these runs would wait an hour.
    const LOST_WAKEUP: Duration = Duration::from_secs(3600);

    /// Runs `request` with an hour-long poll interval on a helper thread, so a
    /// loop that waits on its timer instead of an event fails the test after
    /// the coordination timeout rather than hanging it.
    fn run_with_lost_wakeup(
        mut request: ProcessRequest,
        before_wait: impl Fn(PollObservation) + Send + Sync + 'static,
    ) -> ProcessCompletion {
        request.policy.deadline = LOST_WAKEUP;
        let control = PollControl {
            interval: LOST_WAKEUP,
            before_wait: Some(Arc::new(before_wait)),
        };
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            let completion = SystemProcessRunner.run_with_poll(
                &request,
                SessionKind::NewProcessGroup,
                &|| false,
                &control,
            );
            let _ = sender.send(completion);
        });
        receiver
            .recv_timeout(crate::test_support::HANDSHAKE_TIMEOUT)
            .expect("the poll loop waited on its timer instead of a process event")
    }

    fn fixture_fifo(temp: &tempfile::TempDir) -> std::path::PathBuf {
        let fifo = temp.path().join("gate");
        let name = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        fifo
    }

    /// Opens `fifo` for writing once `observed` fires, writes `bytes` and
    /// closes it; the blocked reader then sees them followed by EOF.
    fn release_fifo_after(
        observed: mpsc::Receiver<()>,
        fifo: std::path::PathBuf,
        bytes: &'static [u8],
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            observed
                .recv()
                .expect("the poll loop never reached the observed state");
            let mut writer = std::fs::OpenOptions::new().write(true).open(fifo).unwrap();
            writer.write_all(bytes).unwrap();
        })
    }

    #[test]
    fn written_stdin_and_captures_complete_without_a_timer_wakeup() {
        // Drain stdin first so the one-byte write cannot race the exit.
        let mut request = cleanup_request("cat >/dev/null; printf out; printf err >&2");
        request.stdin = Some(b"x".to_vec());
        let completion = run_with_lost_wakeup(request, |_| {});
        assert_eq!(completion.cleanup, CleanupState::Completed);
        let result = completion.outcome.unwrap();
        assert!(result.status.success());
        assert_eq!(result.stdout, b"out");
        assert_eq!(result.stderr, b"err");
    }

    #[test]
    fn capture_after_an_observed_exit_completes_without_a_timer_wakeup() {
        // The incident state: stdin written and the exit reaped while both
        // captures are still pending. An exec'd grandchild holds the output
        // pipes until the FIFO is released after the loop saw that state.
        let temp = tempfile::tempdir().unwrap();
        let fifo = fixture_fifo(&temp);
        let mut request = cleanup_request(&format!(
            "cat >/dev/null; printf out; printf err >&2; /bin/cat '{}' & exit 0",
            fifo.display()
        ));
        request.stdin = Some(b"x".to_vec());
        let (observed, gate) = mpsc::channel();
        let release = release_fifo_after(gate, fifo, b"late");
        let completion = run_with_lost_wakeup(request, move |seen| {
            if seen.status && seen.stdin && !seen.stdout && !seen.stderr {
                let _ = observed.send(());
            }
        });
        release.join().unwrap();
        let result = completion.outcome.unwrap();
        assert!(result.status.success());
        assert_eq!(result.stdout, b"outlate");
        assert_eq!(result.stderr, b"err");
    }

    #[test]
    fn exit_after_observed_captures_completes_without_a_timer_wakeup() {
        // Both captures finish first; only the child's later exit is left.
        let temp = tempfile::tempdir().unwrap();
        let fifo = fixture_fifo(&temp);
        let request = cleanup_request(&format!(
            "printf out; printf err >&2; exec >/dev/null 2>&1; read line <'{}'; exit 7",
            fifo.display()
        ));
        let (observed, gate) = mpsc::channel();
        let release = release_fifo_after(gate, fifo, b"");
        let completion = run_with_lost_wakeup(request, move |seen| {
            if seen.stdout && seen.stderr && !seen.status {
                let _ = observed.send(());
            }
        });
        release.join().unwrap();
        assert_eq!(completion.cleanup, CleanupState::Completed);
        let result = completion.outcome.unwrap();
        // try_wait still reaps the exit status the watcher only observed.
        assert_eq!(result.status.code(), Some(7));
        assert_eq!(result.stdout, b"out");
        assert_eq!(result.stderr, b"err");
    }

    #[test]
    fn tracked_spawn_failure_proves_no_child_cleanup() {
        let mut request = cleanup_request("exit 0");
        request.program = "/nonexistent/mac-worker-cleanup-fixture".into();
        let completion = SystemProcessRunner.run_interruptible_with_cleanup(&request, &|| false);
        assert_eq!(completion.cleanup, CleanupState::Completed);
        assert!(
            matches!(completion.outcome, Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound)
        );
    }

    #[test]
    fn tracked_deadline_and_cancel_report_completed_after_reap_and_join() {
        let mut request = cleanup_request("while :; do :; done");
        request.policy.deadline = Duration::ZERO;
        let completion = SystemProcessRunner.run_interruptible_with_cleanup(&request, &|| false);
        assert_eq!(completion.cleanup, CleanupState::Completed);
        assert!(
            matches!(completion.outcome, Err(WorkerError::Process(ProcessError::DeadlineExceeded { deadline })) if deadline == Duration::ZERO)
        );

        request.policy.deadline = Duration::from_secs(30);
        let stopped = std::rc::Rc::new(std::cell::Cell::new(false));
        let predicate = || {
            stopped.set(true);
            stopped.get()
        };
        let completion = SystemProcessRunner.run_interruptible_with_cleanup(&request, &predicate);
        assert!(stopped.get());
        assert_eq!(completion.cleanup, CleanupState::Completed);
        assert!(matches!(
            completion.outcome,
            Err(WorkerError::Process(ProcessError::Cancelled))
        ));
    }

    #[test]
    fn tracked_stdin_failure_has_joined_cleanup_and_original_error() {
        let mut request = cleanup_request("exec 0<&-; printf done");
        request.stdin = Some(vec![b'x'; 2 * 1024 * 1024]);
        let completion = SystemProcessRunner.run_interruptible_with_cleanup(&request, &|| false);
        assert_eq!(completion.cleanup, CleanupState::Completed);
        assert!(
            matches!(completion.outcome, Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::BrokenPipe)
        );
        assert!(
            matches!(SystemProcessRunner.run(&request), Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::BrokenPipe)
        );
    }

    #[test]
    fn tracked_success_with_live_owned_descendant_is_unknown_even_after_capture_eof() {
        let temp = tempfile::tempdir().unwrap();
        let gate = temp.path().join("gate");
        let name = std::ffi::CString::new(gate.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        let script = format!(
            "/bin/cat >/dev/null 2>/dev/null <'{}' & printf '%s' $$",
            gate.display()
        );
        let completion = SystemProcessRunner
            .run_interruptible_with_cleanup(&cleanup_request(&script), &|| false);
        let result = completion.outcome.unwrap();
        let group = parsed_pgid(&result.stdout);
        let mut guard = SavedGroup::new(group);
        assert_eq!(completion.cleanup, CleanupState::Unknown);
        assert!(result.status.success());
        assert!(!saved_process_group_is_gone(group));
        guard.cleanup_once();
    }

    #[test]
    fn tracked_capture_abandonment_and_panic_never_prove_completion() {
        use std::os::unix::net::UnixStream;
        let (reader, writer) = UnixStream::pair().unwrap();
        let (sender, _receiver) = mpsc::channel();
        let stdout = spawn_capture(reader, ProcessStream::Stdout, 32, sender.clone());
        let stderr = spawn_capture(io::empty(), ProcessStream::Stderr, 32, sender);
        let cleanup = finish_terminated_child_with_budget(
            stdout,
            stderr,
            None,
            SessionKind::NewProcessGroup,
            &|| true,
        )
        .unwrap();
        assert_eq!(cleanup, CleanupState::Unknown);
        drop(writer);

        struct PanickingReader;
        impl Read for PanickingReader {
            fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
                panic!("fixture capture panic")
            }
        }
        let (sender, _receiver) = mpsc::channel();
        let stdout = spawn_capture(PanickingReader, ProcessStream::Stdout, 32, sender.clone());
        let stderr = spawn_capture(io::empty(), ProcessStream::Stderr, 32, sender);
        assert!(join_threads(stdout, stderr, None).is_err());
    }

    #[test]
    fn tracked_expired_kill_budget_is_unknown_even_with_joined_pipes() {
        let mut child = Command::new("/bin/sh")
            .args(["-c", "exit 0"])
            .process_group(0)
            .spawn()
            .unwrap();
        let group = child.id() as libc::pid_t;
        let cleanup =
            terminate_child_with_evidence(&mut child, Some(group), &|| true, &|_| false).unwrap();
        assert_eq!(cleanup, CleanupState::Unknown);
        assert!(child.try_wait().unwrap().is_some());
    }

    #[test]
    fn tracked_reference_and_arc_delegation_preserve_predicate_and_explicit_evidence() {
        use crate::controller::channel::testing::RecordingTrackedRunner;
        use std::sync::Arc;
        let raw = Arc::new(RecordingTrackedRunner::new(vec![ProcessCompletion {
            outcome: Err(ProcessError::Cancelled.into()),
            cleanup: CleanupState::Completed,
        }]));
        let runner: Arc<dyn TrackedProcessRunner> = raw.clone();
        let borrowed = &runner;
        let flag = std::rc::Rc::new(std::cell::Cell::new(false));
        let stop = || {
            flag.set(true);
            true
        };
        let completion = borrowed.run_interruptible_with_cleanup(&cleanup_request("exit 0"), &stop);
        assert!(flag.get());
        assert_eq!(completion.cleanup, CleanupState::Completed);
        assert!(matches!(
            completion.outcome,
            Err(WorkerError::Process(ProcessError::Cancelled))
        ));
        assert_eq!(raw.calls().len(), 1);
    }

    fn pgid_request() -> ProcessRequest {
        ProcessRequest {
            program: OsString::from("/bin/sh"),
            args: vec![OsString::from("-c"), OsString::from("ps -o pgid= -p $$")],
            environment: Vec::new(),
            environment_remove: Vec::new(),
            stdin: None,
            policy: policy(),
            isolate_parent_environment: false,
        }
    }

    fn parsed_pgid(stdout: &[u8]) -> i32 {
        String::from_utf8_lossy(stdout)
            .split_whitespace()
            .next()
            .expect("pgid")
            .parse()
            .expect("numeric pgid")
    }

    #[test]
    fn inherit_process_group_runner_keeps_the_caller_group() {
        let result = InheritProcessGroupRunner.run(&pgid_request()).unwrap();
        assert!(result.status.success());
        let child_pgid = parsed_pgid(&result.stdout);
        let caller_pgid = unsafe { libc::getpgid(0) };
        assert_eq!(child_pgid, caller_pgid);
    }

    #[test]
    fn system_process_runner_starts_a_new_group() {
        let result = SystemProcessRunner.run(&pgid_request()).unwrap();
        assert!(result.status.success());
        let child_pgid = parsed_pgid(&result.stdout);
        let caller_pgid = unsafe { libc::getpgid(0) };
        assert_ne!(child_pgid, caller_pgid);
    }

    #[test]
    fn termination_bounds_drain_when_pipe_holders_outlive_the_child() {
        use std::os::unix::net::UnixStream;

        for session in [SessionKind::NewProcessGroup, SessionKind::NewSession] {
            let (stdout, stdout_writer) = UnixStream::pair().unwrap();
            let (stderr, stderr_writer) = UnixStream::pair().unwrap();
            let (sender, receiver) = mpsc::channel();
            let stdout = spawn_capture(stdout, ProcessStream::Stdout, 32, sender.clone());
            let stderr = spawn_capture(stderr, ProcessStream::Stderr, 32, sender);
            let (finished, completion) = mpsc::channel();
            let finishing = thread::spawn(move || {
                let result = finish_terminated_child(stdout, stderr, None, session);
                let _ = finished.send(result);
            });

            // The writers stay open until after the result: unbounded joins
            // cannot complete. This wide bound tolerates a loaded test host.
            let result = completion.recv_timeout(crate::test_support::HANDSHAKE_TIMEOUT);
            drop(receiver);
            drop((stdout_writer, stderr_writer));
            finishing.join().unwrap();
            result
                .expect("termination waited for the pipe holders")
                .unwrap();
        }
    }

    #[test]
    fn capture_tolerates_a_dropped_event_receiver() {
        use std::os::unix::net::UnixStream;

        for bytes in [
            &b"within limit"[..],
            &b"past the sixteen byte output limit"[..],
        ] {
            let (reader, mut writer) = UnixStream::pair().unwrap();
            let (sender, receiver) = mpsc::channel();
            let capture = spawn_capture(reader, ProcessStream::Stdout, 16, sender);
            drop(receiver);
            // Abandonment releases this gate even when overflow is still
            // draining. EOF after the runner returns must not panic or block.
            drop(capture.limit_release);
            writer.write_all(bytes).unwrap();
            drop(writer);
            capture.handle.join().unwrap();
        }
    }

    fn grandchild_pipe_request(
        policy: ProcessPolicy,
        command: &str,
    ) -> (tempfile::TempDir, ProcessRequest) {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("grandchild.pid");
        let script = format!(
            "PATH=/bin:/usr/bin /bin/sleep 60 & printf $! > {}; {command}",
            pid_file.display()
        );
        let request = ProcessRequest {
            program: OsString::from("/bin/sh"),
            args: vec![OsString::from("-c"), OsString::from(script)],
            environment: Vec::new(),
            environment_remove: Vec::new(),
            stdin: None,
            policy,
            isolate_parent_environment: false,
        };
        (temp, request)
    }

    fn kill_recorded_grandchild(temp: &tempfile::TempDir) {
        let path = temp.path().join("grandchild.pid");
        if let Ok(bytes) = std::fs::read(&path)
            && let Ok(pid) = String::from_utf8_lossy(&bytes)
                .trim()
                .parse::<libc::pid_t>()
        {
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
        }
    }

    #[test]
    fn inherit_deadline_returns_without_joining_grandchild_pipe_holders() {
        let (temp, request) = grandchild_pipe_request(
            ProcessPolicy {
                stdout_limit: 4096,
                stderr_limit: 4096,
                deadline: Duration::from_millis(150),
            },
            "wait",
        );
        let started = Instant::now();
        let error = InheritProcessGroupRunner.run(&request).unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "inherit deadline waited {:?}",
            started.elapsed()
        );
        assert!(matches!(
            error,
            WorkerError::Process(ProcessError::DeadlineExceeded { .. })
        ));
        kill_recorded_grandchild(&temp);
    }

    #[test]
    fn inherit_output_cap_returns_without_joining_grandchild_pipe_holders() {
        let (temp, request) = grandchild_pipe_request(
            ProcessPolicy {
                stdout_limit: 32,
                stderr_limit: 4096,
                deadline: Duration::from_secs(5),
            },
            "while :; do printf 0123456789; done",
        );
        let started = Instant::now();
        let error = InheritProcessGroupRunner.run(&request).unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "inherit cap waited {:?}",
            started.elapsed()
        );
        assert!(matches!(
            error,
            WorkerError::Process(ProcessError::OutputLimitExceeded {
                stream: ProcessStream::Stdout,
                limit: 32
            })
        ));
        kill_recorded_grandchild(&temp);
    }

    struct SavedGroup {
        pgid: libc::pid_t,
        armed: bool,
    }

    impl SavedGroup {
        fn new(pgid: libc::pid_t) -> Self {
            Self { pgid, armed: true }
        }

        fn cleanup_once(&mut self) {
            if !self.armed {
                return;
            }
            unsafe {
                libc::killpg(self.pgid, libc::SIGKILL);
            }
            self.armed = false;
        }
    }

    impl Drop for SavedGroup {
        fn drop(&mut self) {
            if self.armed {
                unsafe {
                    libc::killpg(self.pgid, libc::SIGKILL);
                }
            }
        }
    }

    fn injected_eperm() -> io::Error {
        io::Error::from_raw_os_error(libc::EPERM)
    }

    #[test]
    fn eperm_recovery_preserves_error_while_a_fixture_descendant_holds_the_group() {
        // Leader exits; `cat` stays blocked on the stdin pipe we hold, so the
        // saved pgid remains. Noninteractive sh redirects a bare `cat &` to
        // /dev/null; dup the pipe onto fd 3 so the descendant actually holds
        // it. Injected EPERM must not be swallowed (0d8 Ok-after-reap /
        // getpgid-only).
        let mut child = Command::new("/bin/sh")
            .args(["-c", "exec 3<&0; PATH=/bin:/usr/bin /bin/cat <&3 & exit 0"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap();
        let stdin = child.stdin.take();
        let process_group = child.id() as libc::pid_t;
        let mut guard = SavedGroup::new(process_group);
        assert!(child.wait().unwrap().success());
        assert_eq!(
            unsafe { libc::killpg(process_group, 0) },
            0,
            "fixture cat must remain live in the saved group, not zombie-only"
        );
        assert_eq!(
            recover_after_killpg_eperm(&mut child, process_group, injected_eperm())
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EPERM),
            "recovery must keep EPERM while the saved group still exists"
        );
        assert!(!saved_process_group_is_gone(process_group));
        guard.cleanup_once();
        drop(stdin);
    }

    #[test]
    fn eperm_recovery_accepts_a_reaped_finite_group_that_is_absent() {
        let mut child = Command::new("/bin/sh")
            .args(["-c", "dd if=/dev/zero bs=65536 count=32 2>/dev/null"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap();
        let process_group = child.id() as libc::pid_t;
        let mut stdout = child.stdout.take().unwrap();
        let mut buf = [0_u8; 32];
        let _ = stdout.read(&mut buf);
        let mut rest = [0_u8; 8192];
        while stdout.read(&mut rest).unwrap_or(0) > 0 {}
        drop(stdout);
        child.wait().unwrap();
        recover_after_killpg_eperm(&mut child, process_group, injected_eperm())
            .expect("empty saved group after reap is the accepted EPERM recovery");
        assert!(saved_process_group_is_gone(process_group));
    }
}
