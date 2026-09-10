use std::{
    collections::VecDeque,
    ffi::OsString,
    io::{self, Read, Write},
    os::unix::process::CommandExt,
    process::{Child, Command, ExitStatus, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use crate::error::{ProcessError, ProcessStream, WorkerError};

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
    }

    fn run_in_new_session(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.run_with_session(request, SessionKind::NewSession, &|| false)
    }

    fn run_interruptible(
        &self,
        request: &ProcessRequest,
        should_stop: &dyn Fn() -> bool,
    ) -> Result<ProcessResult, WorkerError> {
        self.run_with_session(request, SessionKind::NewProcessGroup, should_stop)
    }
}

impl ProcessRunner for InheritProcessGroupRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        SystemProcessRunner.run_with_session(request, SessionKind::Inherit, &|| false)
    }

    fn run_in_new_session(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        SystemProcessRunner.run_with_session(request, SessionKind::NewSession, &|| false)
    }

    fn run_interruptible(
        &self,
        request: &ProcessRequest,
        should_stop: &dyn Fn() -> bool,
    ) -> Result<ProcessResult, WorkerError> {
        SystemProcessRunner.run_with_session(request, SessionKind::Inherit, should_stop)
    }
}

impl SystemProcessRunner {
    fn run_with_session(
        &self,
        request: &ProcessRequest,
        session: SessionKind,
        should_stop: &dyn Fn() -> bool,
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
        drop(sender);

        let started = Instant::now();
        let mut status = None;
        let mut stdout = None;
        let mut stderr = None;
        let mut stdin_complete = stdin_handle.is_none();
        let mut stdin_error = None;

        loop {
            while let Ok(event) = receiver.try_recv() {
                match event {
                    ProcessEvent::Captured(stream, Ok(CaptureOutcome::Complete(bytes))) => {
                        match stream {
                            ProcessStream::Stdout => stdout = Some(bytes),
                            ProcessStream::Stderr => stderr = Some(bytes),
                        }
                    }
                    ProcessEvent::Captured(stream, Ok(CaptureOutcome::LimitExceeded)) => {
                        terminate_child(&mut child, process_group)?;
                        finish_terminated_child(
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
                        terminate_child(&mut child, process_group)?;
                        finish_terminated_child(
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
                }
            }

            if status.is_none() {
                status = child.try_wait()?;
            }
            if status.is_some() && stdout.is_some() && stderr.is_some() && stdin_complete {
                break;
            }

            if should_stop() {
                terminate_child(&mut child, process_group)?;
                finish_terminated_child(stdout_handle, stderr_handle, stdin_handle, session)?;
                return Err(ProcessError::Cancelled.into());
            }

            if started.elapsed() >= request.policy.deadline {
                terminate_child(&mut child, process_group)?;
                finish_terminated_child(stdout_handle, stderr_handle, stdin_handle, session)?;
                return Err(ProcessError::DeadlineExceeded {
                    deadline: request.policy.deadline,
                }
                .into());
            }

            thread::sleep(Duration::from_millis(2));
        }

        join_threads(stdout_handle, stderr_handle, stdin_handle)?;
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
                        // PERF drain is bounded on the new-group path because
                        // terminate/reap makes EOF. Inherit setup does not join
                        // these threads while grandchildren can retain FDs.
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
) -> io::Result<()> {
    if session == SessionKind::Inherit {
        // ENV setup runs inside the recorded gated child's group. Local
        // timeout/cap/cancel must not killpg that group (it would suicide the
        // exec'd helper) and must not join capture threads: grandchildren can
        // keep pipe FDs, and joining would wait until they exit. Forget the
        // handles and `_exit` the helper so supervisor `killpg` finishes reap.
        // PERF's bounded drain_remaining belongs only on the new-group path
        // below, where termination already makes EOF bounded.
        abandon_capture_threads(stdout, stderr, stdin);
        Ok(())
    } else {
        join_threads(stdout, stderr, stdin)
    }
}

fn abandon_capture_threads(
    stdout: CaptureThread,
    stderr: CaptureThread,
    stdin: Option<thread::JoinHandle<()>>,
) {
    let _ = stdout.limit_release.send(());
    let _ = stderr.limit_release.send(());
    std::mem::forget(stdout.handle);
    std::mem::forget(stderr.handle);
    if let Some(stdin) = stdin {
        std::mem::forget(stdin);
    }
}

fn terminate_child(child: &mut Child, process_group: Option<libc::pid_t>) -> io::Result<()> {
    if let Some(process_group) = process_group {
        terminate_process_group(process_group)?;
    } else if let Err(error) = child.kill()
        && error.raw_os_error() != Some(libc::ESRCH)
    {
        return Err(error);
    }
    child.wait().map(|_| ())
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
    stdout
        .handle
        .join()
        .map_err(|_| io::Error::other("stdout capture thread panicked"))?;
    stderr
        .handle
        .join()
        .map_err(|_| io::Error::other("stderr capture thread panicked"))?;
    if let Some(stdin) = stdin {
        stdin
            .join()
            .map_err(|_| io::Error::other("stdin writer thread panicked"))?;
    }
    Ok(())
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
    use std::time::{Duration, Instant};

    fn policy() -> ProcessPolicy {
        ProcessPolicy {
            stdout_limit: 4096,
            stderr_limit: 4096,
            deadline: Duration::from_secs(2),
        }
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
}
