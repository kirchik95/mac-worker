#![allow(dead_code)]

use std::{
    collections::VecDeque,
    ffi::OsString,
    os::unix::process::ExitStatusExt,
    process::ExitStatus,
    sync::{Arc, Mutex},
};

use mac_worker::{
    error::WorkerError,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
};

#[derive(Clone)]
pub struct RecordingRunner {
    requests: Arc<Mutex<Vec<ProcessRequest>>>,
    results: Arc<Mutex<VecDeque<Result<ProcessResult, WorkerError>>>>,
    passthrough: bool,
}

impl Default for RecordingRunner {
    fn default() -> Self {
        Self::returning_results(Vec::new())
    }
}

impl RecordingRunner {
    pub fn returning(result: ProcessResult) -> Self {
        Self::returning_results(vec![Ok(result)])
    }

    pub fn returning_success() -> Self {
        Self::returning(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: Vec::new(),
            stderr: Vec::new(),
        })
    }

    pub fn returning_results(results: Vec<Result<ProcessResult, WorkerError>>) -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            results: Arc::new(Mutex::new(VecDeque::from(results))),
            passthrough: false,
        }
    }

    pub fn passthrough() -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            results: Arc::new(Mutex::new(VecDeque::new())),
            passthrough: true,
        }
    }

    pub fn requests(&self) -> Vec<ProcessRequest> {
        self.requests.lock().expect("recording runner lock").clone()
    }

    pub fn single_request(&self) -> ProcessRequest {
        let requests = self.requests();
        assert_eq!(requests.len(), 1, "expected one process request");
        requests.into_iter().next().expect("one request")
    }

    pub fn write_args(&self) -> Vec<Vec<OsString>> {
        self.requests()
            .into_iter()
            .filter(|request| {
                request.args.iter().any(|argument| {
                    matches!(
                        argument.to_str(),
                        Some(
                            "fast-import"
                                | "hash-object"
                                | "update-index"
                                | "write-tree"
                                | "commit-tree"
                                | "update-ref"
                        )
                    )
                })
            })
            .map(|request| request.args)
            .collect()
    }
}

impl ProcessRunner for RecordingRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.requests
            .lock()
            .expect("recording runner lock")
            .push(request.clone());
        if self.passthrough {
            return SystemProcessRunner.run(request);
        }
        self.results
            .lock()
            .expect("recording runner results lock")
            .pop_front()
            .unwrap_or_else(|| {
                Ok(ProcessResult {
                    status: ExitStatus::from_raw(0),
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                })
            })
    }
}
