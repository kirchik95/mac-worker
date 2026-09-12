use std::{
    collections::VecDeque, os::unix::process::ExitStatusExt, process::ExitStatus, sync::Mutex,
    time::Duration,
};

use mac_worker::{
    config::WorkerEntry,
    error::WorkerError,
    job::JobId,
    outbox::OutboxRetryResponse,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::PROTOCOL_VERSION,
    task::{BaseOid, DeliveryState, OriginDelivery, TaskId},
    transfer::{HostOperation, RemoteJobClient},
};
use uuid::Uuid;

struct RecordingRunner {
    results: Mutex<VecDeque<Result<ProcessResult, WorkerError>>>,
    requests: Mutex<Vec<ProcessRequest>>,
}

impl RecordingRunner {
    fn returning(results: Vec<Result<ProcessResult, WorkerError>>) -> Self {
        Self {
            results: Mutex::new(results.into()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<ProcessRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl ProcessRunner for RecordingRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.requests.lock().unwrap().push(request.clone());
        self.results
            .lock()
            .unwrap()
            .pop_front()
            .expect("a scripted result must exist")
    }
}

fn worker() -> WorkerEntry {
    WorkerEntry {
        name: "mini-1".into(),
        ssh: "mac1".into(),
        slots: 1,
        capabilities: vec!["darwin-arm64".into()],
        remote_binary: "~/.local/bin/worker".into(),
        herdr: false,
    }
}

fn status(code: i32) -> ExitStatus {
    ExitStatus::from_raw(code << 8)
}

fn result(status: ExitStatus, stdout: &[u8], stderr: &[u8]) -> ProcessResult {
    ProcessResult {
        status,
        stdout: stdout.to_vec(),
        stderr: stderr.to_vec(),
    }
}

fn canonical_line(value: &impl serde::Serialize) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    bytes
}

fn task_id() -> TaskId {
    TaskId::new(Uuid::from_u128(1))
}

fn retry_response() -> OutboxRetryResponse {
    let delivery = OriginDelivery::new(
        JobId::new(Uuid::from_u128(2)),
        DeliveryState::Retrying,
        "0123456789abcdef0123456789abcdef01234567"
            .parse::<BaseOid>()
            .unwrap(),
        "https://example.test/repo.git".into(),
        "refs/heads/release-candidate".into(),
        0,
        9_000,
        Some("ORIGIN_AUTH_FAILED".into()),
        None,
        1,
        9_000,
    )
    .unwrap();
    OutboxRetryResponse::new(vec![delivery])
}

#[test]
fn outbox_retry_sends_the_task_id_as_the_only_host_argument() {
    let response = retry_response();
    let runner =
        RecordingRunner::returning(vec![Ok(result(status(0), &canonical_line(&response), b""))]);
    let client = RemoteJobClient::new(&runner);
    let got = client.outbox_retry(&worker(), task_id()).unwrap();
    assert_eq!(got.protocol_version(), PROTOCOL_VERSION);
    assert_eq!(got.deliveries()[0].attempt(), 0);
    assert_eq!(got.deliveries()[0].state(), DeliveryState::Retrying);
    let request = &runner.requests()[0];
    let command = format!("{} {}", HostOperation::OutboxRetry.command(), task_id());
    assert_eq!(
        request.args.last().and_then(|argument| argument.to_str()),
        Some(command.as_str())
    );
    assert_eq!(request.policy.deadline, Duration::from_secs(30));
    assert!(request.stdin.as_ref().is_some_and(Vec::is_empty));
}

#[test]
fn outbox_retry_maps_an_unknown_host_command_to_host_command_unsupported() {
    let runner = RecordingRunner::returning(vec![Ok(result(
        status(2),
        b"",
        b"error: unrecognized subcommand 'outbox-retry'\n",
    ))]);
    let error = RemoteJobClient::new(&runner)
        .outbox_retry(&worker(), task_id())
        .unwrap_err();
    assert_eq!(error.public_code(), "HOST_COMMAND_UNSUPPORTED");
    assert_eq!(runner.requests().len(), 1);
}
