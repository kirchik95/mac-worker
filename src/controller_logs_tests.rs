//! Deterministic tests of the CLI follow loop through the real framed RPC decoder.
use std::{
    collections::VecDeque, os::unix::process::ExitStatusExt, process::ExitStatus, sync::Mutex,
    time::Duration,
};

use base64::Engine;
use serde_json::{Value, json};

use crate::{
    config::Config,
    controller::{ControllerRequest, decode_request, encode_json_frame},
    error::{ProcessError, WorkerError},
    job::HostControlError,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::PROTOCOL_VERSION,
    task::{TaskId, TurnId},
    transfer::ResolutionRuntime,
};

#[derive(Default)]
struct Clock {
    now: Mutex<Duration>,
    sleeps: Mutex<Vec<Duration>>,
}
impl ResolutionRuntime for Clock {
    fn monotonic_now(&self) -> Duration {
        *self.now.lock().unwrap()
    }
    fn sleep(&self, delay: Duration) {
        *self.now.lock().unwrap() += delay;
        self.sleeps.lock().unwrap().push(delay);
    }
}

#[derive(Clone)]
enum Step {
    Chunk(&'static [u8], bool),
    Empty(i32),
    FailedExchange,
    Deadline,
    SpawnIo,
    Typed(&'static str),
    Invalid(fn(&mut Value)),
    InvalidFrame,
    TruncatedFailure,
}

struct Rpc<'a> {
    clock: &'a Clock,
    health: Option<Value>, // None is the old-controller Unsupported response.
    health_failures: Mutex<usize>,
    health_reads: Mutex<Vec<usize>>, // Log requests seen at each discovery attempt.
    steps: Mutex<VecDeque<Step>>,
    requests: Mutex<Vec<ControllerRequest>>,
}
impl<'a> Rpc<'a> {
    fn new(clock: &'a Clock, health: Option<Value>, steps: Vec<Step>) -> Self {
        Self {
            clock,
            health,
            health_failures: Mutex::new(0),
            health_reads: Mutex::new(Vec::new()),
            steps: Mutex::new(steps.into()),
            requests: Mutex::new(Vec::new()),
        }
    }
}

fn turn_id() -> TurnId {
    TurnId::new(uuid::Uuid::from_u128(2))
}
fn task_id() -> TaskId {
    TaskId::new(uuid::Uuid::from_u128(1))
}
fn health(features: Option<Value>) -> Value {
    let mut result = json!({"state": "stale", "reason": "missing"});
    if let Some(features) = features {
        result["features"] = features;
    }
    result
}
fn supports_wait() -> Option<Value> {
    Some(health(Some(json!(["controller.task-logs-wait"]))))
}

impl ProcessRunner for Rpc<'_> {
    fn run(&self, process: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert!(!process.policy.deadline.is_zero());
        assert!(process.policy.deadline <= Duration::from_secs(30));
        assert_eq!(process.program, "/usr/bin/ssh");
        let request = decode_request(process.stdin.as_ref().unwrap())?;
        let frame = |code, value: &Value| -> Result<ProcessResult, WorkerError> {
            Ok(ProcessResult {
                status: ExitStatus::from_raw(code << 8),
                stdout: encode_json_frame(value)?,
                stderr: b"secret should never be displayed".to_vec(),
            })
        };
        let envelope = |result| {
            json!({
                "protocol_version": PROTOCOL_VERSION, "command": request.command(),
                "request_id": request.request_id(), "payload_sha256": request.payload_sha256(), "result": result,
            })
        };
        if request.command() == "task.list" {
            assert_eq!(request.body(), &json!({"controller_health": true}));
            self.health_reads
                .lock()
                .unwrap()
                .push(self.requests.lock().unwrap().len());
            let mut failures = self.health_failures.lock().unwrap();
            if *failures > 0 {
                *failures -= 1;
                return Ok(ProcessResult {
                    status: ExitStatus::from_raw(255 << 8),
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                });
            }
            return match &self.health {
                Some(health) => frame(0, &envelope(health.clone())),
                None => frame(
                    1,
                    &serde_json::to_value(
                        HostControlError::new("INVALID_REQUEST", "old controller").unwrap(),
                    )
                    .unwrap(),
                ),
            };
        }
        assert_eq!(request.command(), "task.logs");
        self.requests.lock().unwrap().push(request.clone());
        let step = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra logs request");
        let (bytes, complete) = match &step {
            Step::Chunk(bytes, complete) => (*bytes, *complete),
            _ => (b"".as_slice(), true),
        };
        let offset = request.body()["offset"].as_u64().unwrap();
        let mut reply = envelope(json!({
            "task_id": task_id(), "turn_id": turn_id(), "turn_number": 1, "agent": "codex",
            "offset": offset, "next_offset": offset + bytes.len() as u64,
            "raw": request.body()["raw"], "exhausted": true, "complete": complete,
            "bytes_base64": base64::engine::general_purpose::STANDARD.encode(bytes),
        }));
        match step {
            Step::Chunk(..) => frame(0, &reply),
            Step::Empty(code) => Ok(ProcessResult {
                status: ExitStatus::from_raw(code << 8),
                stdout: Vec::new(),
                stderr: b"private ssh diagnostic".to_vec(),
            }),
            Step::FailedExchange => frame(255, &json!({})),
            Step::Deadline => {
                *self.clock.now.lock().unwrap() += process.policy.deadline;
                Err(ProcessError::DeadlineExceeded {
                    deadline: process.policy.deadline,
                }
                .into())
            }
            Step::SpawnIo => Err(std::io::Error::from_raw_os_error(libc::EAGAIN).into()),
            Step::Typed(code) => frame(
                1,
                &serde_json::to_value(
                    HostControlError::new(code, "private controller diagnostic").unwrap(),
                )
                .unwrap(),
            ),
            Step::Invalid(mutate) => {
                mutate(&mut reply);
                frame(0, &reply)
            }
            Step::InvalidFrame => Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout: b"truncated frame".to_vec(),
                stderr: Vec::new(),
            }),
            Step::TruncatedFailure => Ok(ProcessResult {
                status: ExitStatus::from_raw(255 << 8),
                stdout: b"truncated frame".to_vec(),
                stderr: Vec::new(),
            }),
        }
    }
}

fn invoke(rpc: &Rpc<'_>, follow: bool, raw: bool) -> (Result<(), WorkerError>, Vec<u8>, String) {
    let config =
        Config::parse("version=1\n[controller]\nenabled=true\nssh='controller'\n").unwrap();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let result = crate::controller_task_logs_with_runtime(
        rpc,
        &config,
        task_id(),
        Some(1),
        follow,
        raw,
        &mut stdout,
        &mut stderr,
        rpc.clock,
    );
    (result, stdout, String::from_utf8(stderr).unwrap())
}

#[test]
fn follow_negotiates_once_and_does_not_sleep_between_long_poll_replies() {
    let clock = Clock::default();
    let rpc = Rpc::new(
        &clock,
        supports_wait(),
        vec![
            Step::Chunk(b"", false),
            Step::Chunk(b"", false),
            Step::Chunk(b"a", false),
            Step::Chunk(b"", false),
            Step::Chunk(b"b", true),
        ],
    );
    let (result, out, err) = invoke(&rpc, true, true);
    result.unwrap();
    assert_eq!(out, b"ab");
    assert!(err.is_empty());
    assert_eq!(*rpc.health_reads.lock().unwrap(), [0]);
    assert!(clock.sleeps.lock().unwrap().is_empty());
    let requests = rpc.requests.lock().unwrap();
    for request in requests.iter() {
        assert_eq!(request.body()["wait_ms"], 15_000);
    }
    assert!(requests[0].body().get("turn_id").is_none());
    for request in &requests[1..] {
        assert_eq!(request.body()["turn_id"], turn_id().to_string());
    }
}

#[test]
fn unavailable_discovery_retries_after_the_first_successful_log_read() {
    for failed_log_reads in [0, 1] {
        let clock = Clock::default();
        let mut steps = vec![Step::Empty(255); failed_log_reads];
        steps.extend([
            Step::Chunk(b"before", false),
            Step::Chunk(b"", false),
            Step::Chunk(b"after", true),
        ]);
        let rpc = Rpc::new(&clock, supports_wait(), steps);
        *rpc.health_failures.lock().unwrap() = 1;

        let (result, out, err) = invoke(&rpc, true, true);
        result.unwrap();
        assert_eq!(out, b"beforeafter");
        assert_eq!(
            *rpc.health_reads.lock().unwrap(),
            [0, failed_log_reads + 1],
            "retry discovery only after a successful logs read"
        );
        assert_eq!(
            err,
            if failed_log_reads == 0 {
                ""
            } else {
                "controller unreachable; retrying…\ncontroller reachable again\n"
            }
        );
        assert_eq!(
            *clock.sleeps.lock().unwrap(),
            vec![Duration::from_secs(1); failed_log_reads],
            "confirmed long-poll support removes legacy idle sleeps"
        );
        let requests = rpc.requests.lock().unwrap();
        for request in &requests[..=failed_log_reads] {
            assert!(request.body().get("wait_ms").is_none());
        }
        for request in &requests[failed_log_reads + 1..] {
            assert!(request.body()["wait_ms"].as_u64().is_some());
            assert_eq!(request.body()["offset"], 6);
            assert_eq!(request.body()["turn_id"], turn_id().to_string());
        }
    }
}

#[test]
fn unavailable_discovery_retries_only_once_without_assuming_features() {
    for (failures, health) in [
        (1, None),
        (1, Some(health(None))),
        (1, Some(health(Some(json!([]))))),
        (2, supports_wait()),
    ] {
        let clock = Clock::default();
        let rpc = Rpc::new(
            &clock,
            health,
            vec![
                Step::Chunk(b"", false),
                Step::Chunk(b"", false),
                Step::Chunk(b"tail", true),
            ],
        );
        *rpc.health_failures.lock().unwrap() = failures;

        let (result, out, err) = invoke(&rpc, true, true);
        result.unwrap();
        assert_eq!(out, b"tail");
        assert!(err.is_empty());
        assert_eq!(*rpc.health_reads.lock().unwrap(), [0, 1]);
        assert!(
            rpc.requests
                .lock()
                .unwrap()
                .iter()
                .all(|request| request.body().get("wait_ms").is_none())
        );
        assert_eq!(
            *clock.sleeps.lock().unwrap(),
            [100, 200].map(Duration::from_millis)
        );
    }
}

#[test]
fn unknown_or_absent_features_use_capped_idle_backoff_and_reset_on_progress() {
    for health in [
        Some(health(None)),
        Some(health(Some(json!([])))),
        Some(health(Some(json!(["future.feature"])))),
        Some(json!({"state": "unavailable", "reason": "read_failed", "features": []})),
        None,
    ] {
        let clock = Clock::default();
        let mut steps = vec![Step::Chunk(b"", false); 7];
        steps.extend([
            Step::Chunk(b"a", false),
            Step::Chunk(b"", false),
            Step::Chunk(b"b", true),
        ]);
        let rpc = Rpc::new(&clock, health, steps);
        let (result, out, _) = invoke(&rpc, true, true);
        result.unwrap();
        assert_eq!(out, b"ab");
        assert_eq!(*rpc.health_reads.lock().unwrap(), [0]);
        assert!(
            rpc.requests
                .lock()
                .unwrap()
                .iter()
                .all(|request| request.body().get("wait_ms").is_none())
        );
        assert_eq!(
            *clock.sleeps.lock().unwrap(),
            [100, 200, 400, 800, 1600, 2000, 2000, 100].map(Duration::from_millis)
        );
    }
}

#[test]
fn follow_retries_empty_failed_and_deadline_exchanges_at_the_pinned_offset() {
    let clock = Clock::default();
    let rpc = Rpc::new(
        &clock,
        supports_wait(),
        vec![
            Step::Chunk(b"before", false),
            Step::Empty(0),
            Step::Empty(255),
            Step::FailedExchange,
            Step::Deadline,
            Step::Chunk(b"after", true),
        ],
    );
    let (result, out, err) = invoke(&rpc, true, true);
    result.unwrap();
    assert_eq!(out, b"beforeafter");
    assert_eq!(
        err,
        "controller unreachable; retrying…\ncontroller reachable again\n"
    );
    assert_eq!(
        *clock.sleeps.lock().unwrap(),
        [1, 2, 4, 8].map(Duration::from_secs)
    );
    for request in &rpc.requests.lock().unwrap()[1..] {
        assert_eq!(request.body()["offset"], 6);
        assert_eq!(request.body()["turn_id"], turn_id().to_string());
    }
}

#[test]
fn follow_retries_ssh_spawn_io_errors_at_the_pinned_offset() {
    let clock = Clock::default();
    let rpc = Rpc::new(
        &clock,
        supports_wait(),
        vec![
            Step::Chunk(b"before", false),
            Step::SpawnIo,
            Step::SpawnIo,
            Step::Chunk(b"after", true),
        ],
    );
    let (result, out, err) = invoke(&rpc, true, true);
    result.unwrap();
    assert_eq!(out, b"beforeafter");
    assert_eq!(
        err,
        "controller unreachable; retrying…\ncontroller reachable again\n"
    );
    assert_eq!(
        *clock.sleeps.lock().unwrap(),
        [1, 2].map(Duration::from_secs)
    );
    for request in &rpc.requests.lock().unwrap()[1..] {
        assert_eq!(request.body()["offset"], 6);
        assert_eq!(request.body()["turn_id"], turn_id().to_string());
    }
}

#[test]
fn ten_minutes_of_continuous_failure_returns_the_original_error() {
    let clock = Clock::default();
    let mut steps = vec![Step::Deadline];
    steps.extend(vec![Step::Empty(255); 100]);
    let rpc = Rpc::new(&clock, supports_wait(), steps);
    let (result, out, err) = invoke(&rpc, true, true);
    assert!(matches!(
        result,
        Err(WorkerError::Process(ProcessError::DeadlineExceeded { .. }))
    ));
    assert!(out.is_empty());
    assert_eq!(err, "controller unreachable; retrying…\n");
    assert_eq!(clock.monotonic_now(), Duration::from_secs(600));
    let sleeps = clock.sleeps.lock().unwrap();
    assert_eq!(sleeps[..5], [1, 2, 4, 8, 10].map(Duration::from_secs));
    assert!(sleeps.iter().all(|delay| *delay <= Duration::from_secs(10)));
}

#[test]
fn recovery_resets_the_outage_budget_and_retry_backoff() {
    let clock = Clock::default();
    let mut steps = vec![Step::Empty(255); 40];
    steps.push(Step::Chunk(b"first", false));
    steps.extend(vec![Step::Empty(255); 40]);
    steps.push(Step::Chunk(b"second", true));
    let rpc = Rpc::new(&clock, supports_wait(), steps);
    let (result, out, err) = invoke(&rpc, true, true);
    result.unwrap();
    assert_eq!(out, b"firstsecond");
    assert_eq!(
        err.matches("controller unreachable; retrying…\n").count(),
        2
    );
    assert_eq!(err.matches("controller reachable again\n").count(), 2);
    assert_eq!(clock.sleeps.lock().unwrap()[40], Duration::from_secs(1));
    assert!(clock.monotonic_now() > Duration::from_secs(600));
}

#[test]
fn verification_and_typed_errors_end_follow_without_retry() {
    for failure in [
        Step::Invalid(|value| value["request_id"] = json!("00000000000000000000000000000000")),
        Step::Invalid(|value| value["payload_sha256"] = json!("a".repeat(64))),
        Step::Invalid(|value| {
            value["result"]["turn_id"] = json!(uuid::Uuid::from_u128(3).to_string())
        }),
        Step::Invalid(|value| value["result"]["agent"] = json!("claude")),
        Step::Invalid(|value| value["result"]["offset"] = json!(999)),
        Step::Invalid(|value| value["result"]["bytes_base64"] = json!("%%%")),
        Step::InvalidFrame,
        Step::Typed("TASK_NOT_FOUND"),
        Step::Typed("CONTROLLER_UNAVAILABLE"),
        Step::Typed("IO"),
    ] {
        let clock = Clock::default();
        let rpc = Rpc::new(
            &clock,
            supports_wait(),
            vec![Step::Chunk(b"safe", false), failure],
        );
        let (result, out, err) = invoke(&rpc, true, true);
        assert!(result.is_err());
        assert_eq!(out, b"safe");
        assert!(
            err.is_empty(),
            "verification/typed errors must not announce transport retries"
        );
        assert!(clock.sleeps.lock().unwrap().is_empty());
    }
}

#[test]
fn nonfollow_keeps_immediate_reads_without_discovery_or_transport_retry() {
    for failure in [
        Step::Empty(0),
        Step::Empty(255),
        Step::Deadline,
        Step::SpawnIo,
    ] {
        let clock = Clock::default();
        let rpc = Rpc::new(&clock, supports_wait(), vec![failure]);
        let (result, _, err) = invoke(&rpc, false, true);
        assert!(result.is_err());
        assert!(err.is_empty());
        assert!(rpc.health_reads.lock().unwrap().is_empty());
        assert!(clock.sleeps.lock().unwrap().is_empty());
        assert!(
            rpc.requests.lock().unwrap()[0]
                .body()
                .get("wait_ms")
                .is_none()
        );
    }
}

#[test]
fn rendered_follow_preserves_partial_lines_across_an_outage() {
    let clock = Clock::default();
    let rpc = Rpc::new(&clock, supports_wait(), vec![
        Step::Chunk(b"{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"hello", false),
        Step::Empty(255),
        Step::Chunk(b" world\"}}\n", true),
    ]);
    let (result, out, err) = invoke(&rpc, true, false);
    result.unwrap();
    assert_eq!(String::from_utf8(out).unwrap(), "hello world\n");
    assert_eq!(
        err,
        "controller unreachable; retrying…\ncontroller reachable again\n"
    );
}

#[test]
fn a_failed_ssh_with_a_partial_frame_is_retried() {
    let clock = Clock::default();
    let rpc = Rpc::new(
        &clock,
        supports_wait(),
        vec![
            Step::Chunk(b"first", false),
            Step::TruncatedFailure,
            Step::Chunk(b"second", true),
        ],
    );
    let (result, out, err) = invoke(&rpc, true, true);
    result.unwrap();
    assert_eq!(out, b"firstsecond");
    assert_eq!(
        err,
        "controller unreachable; retrying…\ncontroller reachable again\n"
    );
    assert_eq!(*clock.sleeps.lock().unwrap(), [Duration::from_secs(1)]);
}

#[test]
fn deadline_failures_cannot_extend_the_outage_past_ten_minutes() {
    let clock = Clock::default();
    let rpc = Rpc::new(&clock, supports_wait(), vec![Step::Deadline; 100]);
    let (result, _, err) = invoke(&rpc, true, true);
    assert!(
        matches!(result, Err(WorkerError::Process(ProcessError::DeadlineExceeded { deadline })) if deadline == Duration::from_secs(30))
    );
    assert_eq!(err, "controller unreachable; retrying…\n");
    assert_eq!(clock.monotonic_now(), Duration::from_secs(600));
}
