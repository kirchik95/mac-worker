use std::{
    ffi::OsString,
    io::Write,
    process::{Command, ExitStatus, Stdio},
};

use crate::error::WorkerError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessRequest {
    pub program: OsString,
    pub args: Vec<OsString>,
    pub stdin: Option<Vec<u8>>,
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
            .stdin(if request.stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = command.spawn()?;
        if let Some(input) = &request.stdin {
            let mut stdin = child.stdin.take().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "child process stdin was not available",
                )
            })?;
            stdin.write_all(input)?;
        }
        let output = child.wait_with_output()?;

        Ok(ProcessResult {
            status: output.status,
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}
