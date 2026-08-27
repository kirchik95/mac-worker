use std::{
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
}

impl<T: ProcessRunner + ?Sized> ProcessRunner for &T {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        (**self).run(request)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemProcessRunner;

impl ProcessRunner for SystemProcessRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let mut command = Command::new(&request.program);
        command
            .args(&request.args)
            .envs(request.environment.iter().map(|(key, value)| (key, value)));
        for key in &request.environment_remove {
            command.env_remove(key);
        }
        command
            .process_group(0)
            .stdin(if request.stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = command.spawn()?;
        let process_group = child.id() as libc::pid_t;
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
                        terminate_and_reap(&mut child, process_group)?;
                        join_threads(stdout_handle, stderr_handle, stdin_handle)?;
                        let limit = match stream {
                            ProcessStream::Stdout => request.policy.stdout_limit,
                            ProcessStream::Stderr => request.policy.stderr_limit,
                        };
                        return Err(ProcessError::OutputLimitExceeded { stream, limit }.into());
                    }
                    ProcessEvent::Captured(_, Err(error)) => {
                        terminate_and_reap(&mut child, process_group)?;
                        join_threads(stdout_handle, stderr_handle, stdin_handle)?;
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

            if started.elapsed() >= request.policy.deadline {
                terminate_and_reap(&mut child, process_group)?;
                join_threads(stdout_handle, stderr_handle, stdin_handle)?;
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

fn terminate_and_reap(child: &mut Child, process_group: libc::pid_t) -> io::Result<()> {
    terminate_process_group(process_group)?;
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
