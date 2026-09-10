use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    ffi::OsString,
    fs,
    os::unix::process::ExitStatusExt,
    process::ExitStatus,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError},
    },
    thread,
    time::{Duration, Instant},
};

use mac_worker::{
    RuntimeContext,
    agent_facts::{
        AgentAuth, AgentFacts, AgentProbe, FACTS_TTL, HerdrFactState, HerdrFacts, ProfileProbe,
        turn_auth_failure_reason,
    },
    cli::{Cli, Command},
    config::{Config, WorkerEntry},
    error::{ProcessError, ProcessStream, WorkerError},
    lease::{LeaseSummary, SlotState},
    output::CommandOutput,
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    protocol::{
        CpuCounters, HealthStatus, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, WorkerHealth,
        WorkersReport,
    },
    run_with_io_in_context,
    transport::{ProbeClock, SshTransport, WorkersService},
};

const TEST_COORDINATION_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct RecordingRunner {
    requests: Arc<Mutex<Vec<ProcessRequest>>>,
    results: Arc<Mutex<VecDeque<Result<ProcessResult, WorkerError>>>>,
}

impl RecordingRunner {
    fn returning_json(stdout: Vec<u8>) -> Self {
        Self::returning(ProcessResult {
            status: exit_status(0),
            stdout,
            stderr: Vec::new(),
        })
    }

    fn returning(result: ProcessResult) -> Self {
        Self::returning_result(Ok(result))
    }

    fn returning_result(result: Result<ProcessResult, WorkerError>) -> Self {
        Self::returning_results(vec![result])
    }

    fn returning_results(results: Vec<Result<ProcessResult, WorkerError>>) -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            results: Arc::new(Mutex::new(VecDeque::from(results))),
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
            .expect("a predetermined process result")
    }
}

struct PanickingRunner;

impl ProcessRunner for PanickingRunner {
    fn run(&self, _request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        panic!("injected legacy runner panic");
    }
}

#[derive(Clone, Default)]
struct ManualProbeClock {
    millis: Arc<AtomicU64>,
}

impl ManualProbeClock {
    fn set_millis(&self, millis: u64) {
        self.millis.store(millis, Ordering::SeqCst);
    }
}

impl ProbeClock for ManualProbeClock {
    fn now(&self) -> Duration {
        Duration::from_millis(self.millis.load(Ordering::SeqCst))
    }
}

#[derive(Default)]
struct ControlledState {
    active: usize,
    max_active: usize,
    started: Vec<String>,
    released: HashSet<String>,
    panic_destinations: HashSet<String>,
    requests: Vec<ProcessRequest>,
    release_all: bool,
}

#[derive(Clone, Default)]
struct ControlledRunner {
    state: Arc<(Mutex<ControlledState>, Condvar)>,
}

impl ControlledRunner {
    fn wait_for_started(&self, count: usize) {
        let (state, changed) = &*self.state;
        let mut state = state.lock().unwrap();
        let deadline = Instant::now() + TEST_COORDINATION_TIMEOUT;
        while state.started.len() < count {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let started = state.started.clone();
                state.release_all = true;
                changed.notify_all();
                drop(state);
                panic!(
                    "timed out after {TEST_COORDINATION_TIMEOUT:?} waiting for {count} probes to start; started {started:?}"
                );
            }
            let (next, timeout) = changed.wait_timeout(state, remaining).unwrap();
            state = next;
            if timeout.timed_out() && state.started.len() < count {
                let started = state.started.clone();
                state.release_all = true;
                changed.notify_all();
                drop(state);
                panic!(
                    "timed out after {TEST_COORDINATION_TIMEOUT:?} waiting for {count} probes to start; started {started:?}"
                );
            }
        }
    }

    fn release(&self, destination: &str) {
        let (state, changed) = &*self.state;
        let mut state = state.lock().unwrap();
        state.released.insert(destination.to_owned());
        changed.notify_all();
    }

    fn panic_on(&self, destination: &str) {
        let (state, _) = &*self.state;
        state
            .lock()
            .unwrap()
            .panic_destinations
            .insert(destination.to_owned());
    }

    fn release_all(&self) {
        let (state, changed) = &*self.state;
        state.lock().unwrap().release_all = true;
        changed.notify_all();
    }

    fn started(&self) -> Vec<String> {
        self.state.0.lock().unwrap().started.clone()
    }

    fn max_active(&self) -> usize {
        self.state.0.lock().unwrap().max_active
    }

    fn requests_by_destination(&self) -> HashMap<String, ProcessRequest> {
        self.state
            .0
            .lock()
            .unwrap()
            .requests
            .iter()
            .cloned()
            .map(|request| (request_destination(&request), request))
            .collect()
    }
}

impl ProcessRunner for ControlledRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let destination = request_destination(request);
        let should_panic = {
            let (state, changed) = &*self.state;
            let mut state = state.lock().unwrap();
            state.active += 1;
            state.max_active = state.max_active.max(state.active);
            state.started.push(destination.clone());
            state.requests.push(request.clone());
            changed.notify_all();
            let deadline = Instant::now() + TEST_COORDINATION_TIMEOUT;
            while !state.release_all && !state.released.contains(&destination) {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    state.release_all = true;
                    changed.notify_all();
                    drop(state);
                    panic!(
                        "timed out after {TEST_COORDINATION_TIMEOUT:?} waiting to release probe {destination}"
                    );
                }
                let (next, timeout) = changed.wait_timeout(state, remaining).unwrap();
                state = next;
                if timeout.timed_out()
                    && !state.release_all
                    && !state.released.contains(&destination)
                {
                    state.release_all = true;
                    changed.notify_all();
                    drop(state);
                    panic!(
                        "timed out after {TEST_COORDINATION_TIMEOUT:?} waiting to release probe {destination}"
                    );
                }
            }
            state.active -= 1;
            state.panic_destinations.contains(&destination)
        };
        if should_panic {
            panic!("controlled probe panic for {destination}");
        }
        Ok(ProcessResult {
            status: exit_status(0),
            stdout: structured_probe_json(
                PROTOCOL_VERSION,
                &format!("{destination}.local"),
                "arm64",
                "26.2",
                vec!["darwin-arm64".into(), "node".into()],
            ),
            stderr: Vec::new(),
        })
    }
}

fn request_destination(request: &ProcessRequest) -> String {
    request.args[9].to_string_lossy().into_owned()
}

fn config_with_workers(count: usize) -> Config {
    Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        workers: (1..=count)
            .map(|index| {
                worker(
                    &format!("mini-{index}"),
                    &format!("mac{index}"),
                    &["darwin-arm64"],
                )
            })
            .collect(),
    }
}

fn spawn_budgeted_inspection(
    service: WorkersService<ControlledRunner>,
    config: Config,
    budget: Duration,
) -> Receiver<WorkersReport> {
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let report = service.inspect_with_budget(&config, budget);
        let _ = sender.send(report);
    });
    receiver
}

fn receive_budgeted_report(
    receiver: Receiver<WorkersReport>,
    runner: &ControlledRunner,
) -> WorkersReport {
    match receiver.recv_timeout(TEST_COORDINATION_TIMEOUT) {
        Ok(report) => report,
        Err(error) => {
            runner.release_all();
            let reason = match error {
                RecvTimeoutError::Timeout => "timed out",
                RecvTimeoutError::Disconnected => "worker thread disconnected",
            };
            panic!(
                "budgeted inspection {reason} before reporting within {TEST_COORDINATION_TIMEOUT:?}; started {:?}",
                runner.started()
            );
        }
    }
}

fn exit_status(code: i32) -> ExitStatus {
    ExitStatus::from_raw(code << 8)
}

fn valid_probe_json() -> Vec<u8> {
    structured_probe_json(
        PROTOCOL_VERSION,
        "mini-1.local",
        "arm64",
        "26.2",
        vec!["darwin-arm64".into(), "git".into()],
    )
}

fn structured_probe_json(
    protocol_version: u32,
    hostname: &str,
    arch: &str,
    os_version: &str,
    capabilities: Vec<String>,
) -> Vec<u8> {
    let mut value = serde_json::json!({
        "protocol_version": protocol_version,
        "hostname": hostname,
        "arch": arch,
        "os_version": os_version,
        "free_disk_bytes": 536_870_912_u64,
        "memory_pressure": "normal",
        "swap_used_bytes": 134_217_728_u64,
        "capabilities": capabilities,
    });
    if protocol_version == PROTOCOL_VERSION {
        value["supervision_version"] = serde_json::json!(mac_worker::protocol::SUPERVISION_VERSION);
        value["total_disk_bytes"] = serde_json::json!(1_073_741_824_u64);
        value["slot_state"] = serde_json::json!("idle");
        value["active_lease"] = serde_json::Value::Null;
    }
    serde_json::to_vec(&value).unwrap()
}

fn worker(name: &str, ssh: &str, capabilities: &[&str]) -> WorkerEntry {
    WorkerEntry {
        name: name.into(),
        ssh: ssh.into(),
        slots: 1,
        capabilities: capabilities
            .iter()
            .map(|capability| (*capability).into())
            .collect(),
        remote_binary: "~/.local/bin/worker".into(),
        herdr: false,
    }
}

fn probe_policy() -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: 1024 * 1024,
        stderr_limit: 1024 * 1024,
        deadline: Duration::from_secs(15),
    }
}

fn local_test_policy() -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: 1024,
        stderr_limit: 1024,
        deadline: Duration::from_secs(2),
    }
}

#[test]
fn probe_disables_forwarding_and_uses_the_fixed_host_command() {
    // Regression: probe requests inherited forwarding settings from SSH
    // configuration because the fixed argv did not disable them explicitly.
    let runner = RecordingRunner::returning_json(valid_probe_json());
    let transport = SshTransport::new(runner.clone());
    let worker = worker("mini-1", "mac1", &["darwin-arm64"]);

    let health = transport.probe(&worker);

    assert_eq!(health.status, HealthStatus::Ready);
    assert_eq!(
        runner.requests(),
        vec![ProcessRequest {
            program: OsString::from("/usr/bin/ssh"),
            args: vec![
                "-o".into(),
                "BatchMode=yes".into(),
                "-o".into(),
                "ConnectTimeout=5".into(),
                "-o".into(),
                "ForwardAgent=no".into(),
                "-o".into(),
                "ClearAllForwardings=yes".into(),
                "--".into(),
                "mac1".into(),
                "~/.local/bin/worker host probe".into(),
            ],
            environment: Vec::new(),
            environment_remove: Vec::new(),
            stdin: None,
            policy: probe_policy(),
            isolate_parent_environment: false,
        }]
    );
}

#[test]
fn legacy_probe_preserves_runner_panic_propagation() {
    // Break caught: panic isolation is moved into the shared probe primitive,
    // silently changing legacy probe, workers, Doctor, and setup behavior.
    let transport = SshTransport::new(PanickingRunner);
    let worker = worker("mini-1", "mac1", &["darwin-arm64"]);

    let outcome =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| transport.probe(&worker)));

    assert!(outcome.is_err(), "legacy probe swallowed the runner panic");
}

#[test]
fn probe_with_deadline_uses_the_exact_nonzero_deadline_and_fifteen_second_cap() {
    // Break caught: a caller budget is ignored, rounded, or allowed to exceed
    // the established per-probe SSH bound.
    let exact_runner = RecordingRunner::returning_json(valid_probe_json());
    let exact_transport = SshTransport::new(exact_runner.clone());
    let capped_runner = RecordingRunner::returning_json(valid_probe_json());
    let capped_transport = SshTransport::new(capped_runner.clone());
    let worker = worker("mini-1", "mac1", &["darwin-arm64"]);

    assert_eq!(
        exact_transport
            .probe_with_deadline(&worker, Duration::from_millis(2_750))
            .status,
        HealthStatus::Ready
    );
    assert_eq!(
        capped_transport
            .probe_with_deadline(&worker, Duration::from_secs(60))
            .status,
        HealthStatus::Ready
    );

    assert_eq!(
        exact_runner.requests()[0].policy.deadline,
        Duration::from_millis(2_750)
    );
    assert_eq!(
        capped_runner.requests()[0].policy.deadline,
        Duration::from_secs(15)
    );
}

#[test]
fn zero_probe_deadline_skips_the_runner_and_returns_bounded_unavailable() {
    // Break caught: an already-expired fleet budget still launches SSH.
    let runner = RecordingRunner::returning_results(Vec::new());
    let transport = SshTransport::new(runner.clone());

    let health =
        transport.probe_with_deadline(&worker("mini-1", "mac1", &["darwin-arm64"]), Duration::ZERO);

    assert_eq!(health.status, HealthStatus::Unavailable);
    assert_eq!(health.error_code.as_deref(), Some("SSH_UNAVAILABLE"));
    assert!(health.probe.is_none());
    assert!(runner.requests().is_empty());
}

#[test]
fn budgeted_inspection_runs_three_probes_concurrently_and_retains_config_order() {
    // Break caught: collection becomes sequential, exceeds three active SSH
    // probes, or returns completion order instead of inventory order.
    let runner = ControlledRunner::default();
    let clock = ManualProbeClock::default();
    let service = WorkersService::with_clock(SshTransport::new(runner.clone()), Arc::new(clock));
    let config = config_with_workers(4);
    let report = spawn_budgeted_inspection(service, config, Duration::from_secs(20));

    runner.wait_for_started(3);
    assert_eq!(runner.max_active(), 3);
    assert_eq!(
        runner.started().into_iter().collect::<HashSet<_>>(),
        HashSet::from(["mac1".into(), "mac2".into(), "mac3".into()])
    );

    runner.release("mac3");
    runner.wait_for_started(4);
    runner.release("mac4");
    runner.release("mac2");
    runner.release("mac1");
    let report = receive_budgeted_report(report, &runner);

    assert_eq!(runner.max_active(), 3);
    assert_eq!(
        report
            .workers
            .iter()
            .map(|health| health.name.as_str())
            .collect::<Vec<_>>(),
        ["mini-1", "mini-2", "mini-3", "mini-4"]
    );
    assert!(
        report
            .workers
            .iter()
            .all(|health| health.status == HealthStatus::Ready)
    );
}

#[test]
fn later_probe_batch_receives_only_the_remaining_global_budget() {
    // Break caught: each pool claim restarts the caller's full fleet budget.
    let runner = ControlledRunner::default();
    let clock = ManualProbeClock::default();
    let service =
        WorkersService::with_clock(SshTransport::new(runner.clone()), Arc::new(clock.clone()));
    let config = config_with_workers(4);
    let report = spawn_budgeted_inspection(service, config, Duration::from_secs(10));

    runner.wait_for_started(3);
    clock.set_millis(4_000);
    runner.release("mac1");
    runner.wait_for_started(4);
    runner.release("mac2");
    runner.release("mac3");
    runner.release("mac4");
    let report = receive_budgeted_report(report, &runner);

    assert!(
        report
            .workers
            .iter()
            .all(|health| health.status == HealthStatus::Ready)
    );
    let requests = runner.requests_by_destination();
    assert_eq!(requests["mac1"].policy.deadline, Duration::from_secs(10));
    assert_eq!(requests["mac2"].policy.deadline, Duration::from_secs(10));
    assert_eq!(requests["mac3"].policy.deadline, Duration::from_secs(10));
    assert_eq!(requests["mac4"].policy.deadline, Duration::from_secs(6));
}

#[test]
fn expired_global_budget_skips_unstarted_hosts_instead_of_multiplying_duration() {
    // Break caught: a six-host fleet consumes one complete budget per host or
    // starts later SSH probes after the monotonic deadline has expired.
    let runner = ControlledRunner::default();
    let clock = ManualProbeClock::default();
    let service =
        WorkersService::with_clock(SshTransport::new(runner.clone()), Arc::new(clock.clone()));
    let config = config_with_workers(6);
    let report = spawn_budgeted_inspection(service, config, Duration::from_secs(10));

    runner.wait_for_started(3);
    clock.set_millis(10_000);
    runner.release("mac1");
    runner.release("mac2");
    runner.release("mac3");
    let report = receive_budgeted_report(report, &runner);

    assert_eq!(runner.started().len(), 3);
    assert_eq!(report.workers.len(), 6);
    assert!(
        report.workers[..3]
            .iter()
            .all(|health| health.status == HealthStatus::Ready)
    );
    assert!(
        report.workers[3..]
            .iter()
            .all(|health| { health.status == HealthStatus::Unavailable && health.probe.is_none() })
    );
}

#[test]
fn panicking_probe_becomes_one_unavailable_row_without_losing_peers() {
    // Break caught: one runner panic unwinds the fleet collection or drops
    // successful rows that completed in the same pool.
    let runner = ControlledRunner::default();
    runner.panic_on("mac2");
    let service = WorkersService::with_clock(
        SshTransport::new(runner.clone()),
        Arc::new(ManualProbeClock::default()),
    );
    let config = config_with_workers(4);
    let report = spawn_budgeted_inspection(service, config, Duration::from_secs(20));

    runner.wait_for_started(3);
    runner.release("mac1");
    runner.release("mac2");
    runner.release("mac3");
    runner.wait_for_started(4);
    runner.release("mac4");
    let report = receive_budgeted_report(report, &runner);

    assert_eq!(report.workers.len(), 4);
    assert_eq!(report.workers[1].name, "mini-2");
    assert_eq!(report.workers[1].status, HealthStatus::Unavailable);
    assert_eq!(
        report.workers[1].error_code.as_deref(),
        Some("SSH_UNAVAILABLE")
    );
    assert!(report.workers[1].probe.is_none());
    assert!(
        [0, 2, 3]
            .into_iter()
            .all(|index| report.workers[index].status == HealthStatus::Ready)
    );
}

#[test]
fn budgeted_requirement_inspection_keeps_inventory_first_stable_union() {
    // Break caught: the budgeted path diverges from the established project
    // requirement ordering or mutates configured capabilities.
    let runner = RecordingRunner::returning_json(structured_probe_json(
        PROTOCOL_VERSION,
        "mini-1.local",
        "arm64",
        "26.2",
        vec!["node".into()],
    ));
    let config = Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        workers: vec![worker(
            "mini-1",
            "mac1",
            &["darwin-arm64", "docker", "darwin-arm64", "ruby"],
        )],
    };
    let original_inventory = config.workers[0].capabilities.clone();
    let service = WorkersService::with_clock(
        SshTransport::new(runner.clone()),
        Arc::new(ManualProbeClock::default()),
    );

    let report = service.inspect_with_requirements_and_budget(
        &config,
        &[
            "node".into(),
            "docker".into(),
            "swift".into(),
            "node".into(),
            "go".into(),
        ],
        Duration::from_secs(4),
    );

    assert_eq!(
        report.workers[0].missing_capabilities,
        ["darwin-arm64", "docker", "ruby", "swift", "go"]
    );
    assert_eq!(config.workers[0].capabilities, original_inventory);
    assert_eq!(runner.requests()[0].policy.deadline, Duration::from_secs(4));
}

#[test]
fn nonzero_ssh_exit_is_reported_as_unavailable() {
    let runner = RecordingRunner::returning(ProcessResult {
        status: exit_status(255),
        stdout: Vec::new(),
        stderr: b"connection refused".to_vec(),
    });
    let transport = SshTransport::new(runner);

    let health = transport.probe(&worker("mini-1", "mac1", &[]));

    assert_eq!(health.status, HealthStatus::Unavailable);
    assert_eq!(health.error_code.as_deref(), Some("SSH_UNAVAILABLE"));
    assert!(
        health
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("connection refused"))
    );
    assert!(health.probe.is_none());
}

#[test]
fn process_launch_failure_is_reported_as_unavailable() {
    let runner = RecordingRunner::returning_result(Err(WorkerError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "ssh was not found",
    ))));
    let transport = SshTransport::new(runner);

    let health = transport.probe(&worker("mini-1", "mac1", &[]));

    assert_eq!(health.status, HealthStatus::Unavailable);
    assert_eq!(health.error_code.as_deref(), Some("SSH_UNAVAILABLE"));
    assert!(health.error_message.is_some());
}

#[test]
fn stdout_probe_response_overflow_is_reported_as_an_invalid_response() {
    // This catches classifying an oversized protocol response as a transport outage.
    let runner = RecordingRunner::returning_result(Err(WorkerError::Process(
        ProcessError::OutputLimitExceeded {
            stream: ProcessStream::Stdout,
            limit: 1024 * 1024,
        },
    )));
    let transport = SshTransport::new(runner);

    let health = transport.probe(&worker("mini-1", "mac1", &[]));

    assert_eq!(health.status, HealthStatus::Unavailable);
    assert_eq!(health.error_code.as_deref(), Some("INVALID_RESPONSE"));
    assert!(health.error_message.is_some());
}

#[test]
fn malformed_json_is_reported_as_an_invalid_response() {
    let runner = RecordingRunner::returning_json(b"not JSON".to_vec());
    let transport = SshTransport::new(runner);

    let health = transport.probe(&worker("mini-1", "mac1", &[]));

    assert_eq!(health.status, HealthStatus::Unavailable);
    assert_eq!(health.error_code.as_deref(), Some("INVALID_RESPONSE"));
    assert!(health.error_message.is_some());
    assert!(health.probe.is_none());
}

#[test]
fn invalid_utf8_is_reported_as_an_invalid_response() {
    let runner = RecordingRunner::returning_json(vec![0xff]);
    let transport = SshTransport::new(runner);

    let health = transport.probe(&worker("mini-1", "mac1", &[]));

    assert_eq!(health.status, HealthStatus::Unavailable);
    assert_eq!(health.error_code.as_deref(), Some("INVALID_RESPONSE"));
    assert!(health.error_message.is_some());
}

#[test]
fn output_larger_than_one_mib_is_reported_as_an_invalid_response() {
    let runner = RecordingRunner::returning_json(vec![b' '; 1024 * 1024 + 1]);
    let transport = SshTransport::new(runner);

    let health = transport.probe(&worker("mini-1", "mac1", &[]));

    assert_eq!(health.status, HealthStatus::Unavailable);
    assert_eq!(health.error_code.as_deref(), Some("INVALID_RESPONSE"));
    assert!(health.error_message.is_some());
}

#[test]
fn invalid_structured_probe_fields_are_rejected_before_other_classification() {
    // Catches parsed remote strings surviving into protocol-mismatch or
    // missing-capability reports before their shape and bounds are checked.
    let oversized_hostname = format!("{}-hostname-oversize-secret", "h".repeat(280));
    let oversized_arch = format!("{}-arch-oversize-secret", "a".repeat(80));
    let oversized_os_version = "7".repeat(160);
    let oversized_capability = format!("{}-capability-oversize-secret", "c".repeat(100));
    let mut excessive_capabilities = (0..128)
        .map(|index| format!("capability-{index}"))
        .collect::<Vec<_>>();
    excessive_capabilities[0] = "capability-collection-secret".into();
    let cases = vec![
        (
            "hostname environment/newline",
            structured_probe_json(
                2,
                "mini.local\nHOSTNAME_SECRET=value",
                "arm64",
                "26.2",
                vec!["darwin-arm64".into()],
            ),
            "HOSTNAME_SECRET=value".to_owned(),
        ),
        (
            "hostname bound",
            structured_probe_json(1, &oversized_hostname, "arm64", "26.2", Vec::new()),
            oversized_hostname,
        ),
        (
            "architecture terminal control",
            structured_probe_json(
                1,
                "mini.local",
                "arm64\u{1b}]0;ARCH_SECRET\u{7}",
                "26.2",
                Vec::new(),
            ),
            "ARCH_SECRET".to_owned(),
        ),
        (
            "architecture bound",
            structured_probe_json(1, "mini.local", &oversized_arch, "26.2", Vec::new()),
            oversized_arch,
        ),
        (
            "OS version environment/newline",
            structured_probe_json(
                1,
                "mini.local",
                "arm64",
                "26.2\r\nOS_VERSION_SECRET=value",
                Vec::new(),
            ),
            "OS_VERSION_SECRET=value".to_owned(),
        ),
        (
            "OS version bound",
            structured_probe_json(1, "mini.local", "arm64", &oversized_os_version, Vec::new()),
            oversized_os_version,
        ),
        (
            "capability environment/newline",
            structured_probe_json(
                1,
                "mini.local",
                "arm64",
                "26.2",
                vec!["docker\nCAPABILITY_SECRET=value".into()],
            ),
            "CAPABILITY_SECRET=value".to_owned(),
        ),
        (
            "capability bound",
            structured_probe_json(
                1,
                "mini.local",
                "arm64",
                "26.2",
                vec![oversized_capability.clone()],
            ),
            oversized_capability,
        ),
        (
            "capability collection bound",
            structured_probe_json(1, "mini.local", "arm64", "26.2", excessive_capabilities),
            "capability-collection-secret".to_owned(),
        ),
    ];

    for (label, response, forbidden) in cases {
        let runner = RecordingRunner::returning_json(response);
        let transport = SshTransport::new(runner);

        let health = transport.probe(&worker("mini-1", "mac1", &["required-safe"]));

        assert_eq!(health.status, HealthStatus::Unavailable, "{label}");
        assert_eq!(
            health.error_code.as_deref(),
            Some("INVALID_RESPONSE"),
            "{label}"
        );
        assert_eq!(
            health.error_message.as_deref(),
            Some("SSH probe response contained invalid structured fields"),
            "{label}"
        );
        assert!(health.probe.is_none(), "{label}");
        assert!(health.missing_capabilities.is_empty(), "{label}");
        let serialized = serde_json::to_string(&health).unwrap();
        let human = CommandOutput::Workers(WorkersReport {
            protocol_version: PROTOCOL_VERSION,
            workers: vec![health],
        })
        .render_human();
        assert!(!serialized.contains(&forbidden), "{label}: {serialized}");
        assert!(!human.contains(&forbidden), "{label}: {human}");
    }
}

#[test]
fn protocol_mismatch_is_reported_as_unavailable() {
    let response = br#"{"protocol_version":1,"hostname":"mini-1.local","arch":"arm64","os_version":"26.2","free_disk_bytes":536870912,"memory_pressure":"normal","swap_used_bytes":134217728,"capabilities":[]}"#.to_vec();
    let runner = RecordingRunner::returning_json(response);
    let transport = SshTransport::new(runner);

    let health = transport.probe(&worker("mini-1", "mac1", &[]));

    assert_eq!(health.status, HealthStatus::Unavailable);
    assert_eq!(health.error_code.as_deref(), Some("PROTOCOL_MISMATCH"));
    assert!(health.error_message.is_some());
    assert_eq!(
        health.probe.as_ref().map(|probe| probe.protocol_version),
        Some(1)
    );
}

#[test]
fn protocol_six_helper_is_an_explicit_mismatch_before_jobs() {
    // New laptop → old helper: probe mismatch is the preflight. The transport
    // must not issue task/lease host commands after a v6 advertisement.
    let mut response: serde_json::Value = serde_json::from_slice(&valid_probe_json()).unwrap();
    response["protocol_version"] = serde_json::json!(6);
    let runner = RecordingRunner::returning_json(serde_json::to_vec(&response).unwrap());
    let health = SshTransport::new(runner.clone()).probe(&worker("mini-1", "mac1", &[]));

    assert_eq!(health.status, HealthStatus::Unavailable);
    assert_eq!(health.error_code.as_deref(), Some("PROTOCOL_MISMATCH"));
    assert!(
        health
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("protocol version"))
    );
    assert_eq!(
        health.probe.as_ref().map(|probe| probe.protocol_version),
        Some(6)
    );
    let requests = runner.requests();
    assert_eq!(requests.len(), 1, "mismatch must stop at the probe request");
    let probe_command = requests[0]
        .args
        .last()
        .and_then(|argument| argument.to_str())
        .unwrap_or_default();
    assert_eq!(probe_command, "~/.local/bin/worker host probe");
    assert!(
        !requests
            .iter()
            .any(|request| request.args.iter().any(|argument| {
                argument.to_str().is_some_and(|value| {
                    value.contains("task-status")
                        || value.contains("task-close")
                        || value.contains("lease")
                        || value.contains("submit")
                })
            })),
        "v6 probe mismatch must not issue task or lease host commands"
    );
}

#[test]
fn protocol_two_missing_occupancy_is_an_explicit_version_mismatch() {
    let response = br#"{"protocol_version":2,"hostname":"mini-1.local","arch":"arm64","os_version":"26.2","free_disk_bytes":536870912,"memory_pressure":"normal","swap_used_bytes":0,"capabilities":[]}"#.to_vec();
    let health = SshTransport::new(RecordingRunner::returning_json(response)).probe(&worker(
        "mini-1",
        "mac1",
        &[],
    ));

    assert_eq!(health.error_code.as_deref(), Some("PROTOCOL_MISMATCH"));
    assert_eq!(
        health.probe.as_ref().map(|probe| probe.protocol_version),
        Some(2)
    );
}

#[test]
fn protocol_two_without_supervision_capability_is_an_explicit_version_mismatch() {
    let response = br#"{"protocol_version":2,"hostname":"mini-1.local","arch":"arm64","os_version":"26.2","free_disk_bytes":536870912,"total_disk_bytes":1073741824,"memory_pressure":"normal","swap_used_bytes":0,"slot_state":"idle","active_lease":null,"capabilities":[]}"#.to_vec();
    let health = SshTransport::new(RecordingRunner::returning_json(response)).probe(&worker(
        "mini-1",
        "mac1",
        &[],
    ));

    assert_eq!(health.error_code.as_deref(), Some("PROTOCOL_MISMATCH"));
    assert!(
        health
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("protocol version"))
    );
    assert_eq!(
        health.probe.as_ref().map(|probe| probe.supervision_version),
        Some(0)
    );
}

#[test]
fn phase_four_protocol_three_probe_with_supervision_remains_a_protocol_mismatch() {
    let mut response: serde_json::Value = serde_json::from_slice(&valid_probe_json()).unwrap();
    response["protocol_version"] = serde_json::json!(3);
    response["supervision_version"] = serde_json::json!(2);
    let health = SshTransport::new(RecordingRunner::returning_json(
        serde_json::to_vec(&response).unwrap(),
    ))
    .probe(&worker("mini-1", "mac1", &[]));

    assert_eq!(health.status, HealthStatus::Unavailable);
    assert_eq!(health.error_code.as_deref(), Some("PROTOCOL_MISMATCH"));
    assert_eq!(
        health.probe.as_ref().map(|probe| probe.protocol_version),
        Some(3)
    );
    assert_eq!(
        health.probe.as_ref().map(|probe| probe.supervision_version),
        Some(2)
    );
}

#[test]
fn task7_only_supervision_helper_is_rejected_after_query_capability_bump() {
    // Catches treating a Task 7 helper (supervision v1) as if it implemented
    // the Task 8 status/log/resolve control surface.
    let mut response: serde_json::Value = serde_json::from_slice(&valid_probe_json()).unwrap();
    response["supervision_version"] = serde_json::json!(1);
    let runner = RecordingRunner::returning_json(serde_json::to_vec(&response).unwrap());
    let health = SshTransport::new(runner).probe(&worker("mini-1", "mac1", &[]));

    assert_eq!(mac_worker::protocol::SUPERVISION_VERSION, 3);
    assert_eq!(health.status, HealthStatus::Unavailable);
    assert_eq!(health.error_code.as_deref(), Some("PROTOCOL_MISMATCH"));
    assert_eq!(
        health.probe.as_ref().map(|probe| probe.supervision_version),
        Some(1)
    );
}

#[test]
fn absent_declared_capabilities_exclude_the_worker() {
    let runner = RecordingRunner::returning_json(valid_probe_json());
    let transport = SshTransport::new(runner);

    let health = transport.probe(&worker("mini-1", "mac1", &["docker", "swift"]));

    assert_eq!(health.status, HealthStatus::Unavailable);
    assert_eq!(health.error_code.as_deref(), Some("MISSING_CAPABILITIES"));
    assert_eq!(health.missing_capabilities, vec!["docker", "swift"]);
    assert!(health.error_message.is_some());
    assert!(health.probe.is_some());
}

#[test]
fn inventory_keeps_ready_and_failed_workers_in_config_order() {
    let protocol_mismatch = structured_probe_json(1, "mini-1.local", "arm64", "26.2", Vec::new());
    let runner = RecordingRunner::returning_results(vec![
        Ok(ProcessResult {
            status: exit_status(0),
            stdout: valid_probe_json(),
            stderr: Vec::new(),
        }),
        Ok(ProcessResult {
            status: exit_status(255),
            stdout: Vec::new(),
            stderr: b"offline".to_vec(),
        }),
        Ok(ProcessResult {
            status: exit_status(0),
            stdout: b"not JSON".to_vec(),
            stderr: Vec::new(),
        }),
        Ok(ProcessResult {
            status: exit_status(0),
            stdout: protocol_mismatch,
            stderr: Vec::new(),
        }),
    ]);
    let config = Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        workers: vec![
            worker("ready", "mac1", &["darwin-arm64"]),
            worker("offline", "mac2", &[]),
            worker("malformed", "mac3", &[]),
            worker("mismatched", "mac4", &[]),
        ],
    };
    let service = WorkersService::new(SshTransport::new(runner));

    let report = service.inspect(&config);

    assert_eq!(report.protocol_version, PROTOCOL_VERSION);
    assert_eq!(
        report
            .workers
            .iter()
            .map(|health| health.name.as_str())
            .collect::<Vec<_>>(),
        vec!["ready", "offline", "malformed", "mismatched"]
    );
    assert_eq!(report.workers[0].status, HealthStatus::Ready);
    assert_eq!(report.workers[0].error_code, None);
    assert_eq!(
        report.workers[1].error_code.as_deref(),
        Some("SSH_UNAVAILABLE")
    );
    assert_eq!(
        report.workers[2].error_code.as_deref(),
        Some("INVALID_RESPONSE")
    );
    assert_eq!(
        report.workers[3].error_code.as_deref(),
        Some("PROTOCOL_MISMATCH")
    );
    assert!(
        report.workers[1..]
            .iter()
            .all(|health| health.status == HealthStatus::Unavailable
                && health.error_message.is_some())
    );

    let json = serde_json::to_string(&report).expect("workers report must serialize as JSON");
    let value: serde_json::Value =
        serde_json::from_str(&json).expect("serialized workers report must be valid JSON");
    assert_eq!(value["workers"][1]["error_code"], "SSH_UNAVAILABLE");
}

#[test]
fn inspect_with_requirements_adds_project_capabilities_without_changing_inventory_inspection() {
    // Catches project requirements being ignored, copied into the inventory,
    // or applied before the worker's declared capability order.
    let runner = RecordingRunner::returning_results(vec![
        Ok(ProcessResult {
            status: exit_status(0),
            stdout: structured_probe_json(
                PROTOCOL_VERSION,
                "mini-1.local",
                "arm64",
                "26.2",
                vec!["darwin-arm64".into(), "node".into()],
            ),
            stderr: Vec::new(),
        }),
        Ok(ProcessResult {
            status: exit_status(0),
            stdout: structured_probe_json(
                PROTOCOL_VERSION,
                "mini-1.local",
                "arm64",
                "26.2",
                vec!["darwin-arm64".into(), "node".into()],
            ),
            stderr: Vec::new(),
        }),
    ]);
    let config = Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        workers: vec![worker("mini-1", "mac1", &["darwin-arm64"])],
    };
    let service = WorkersService::new(SshTransport::new(runner));

    let project_report = service.inspect_with_requirements(
        &config,
        &[
            "node".into(),
            "darwin-arm64".into(),
            "node".into(),
            "docker".into(),
        ],
    );
    let inventory_report = service.inspect(&config);

    assert_eq!(project_report.workers[0].status, HealthStatus::Unavailable);
    assert_eq!(
        project_report.workers[0].missing_capabilities,
        vec!["docker"]
    );
    assert_eq!(
        project_report.workers[0].error_message.as_deref(),
        Some("worker is missing required capabilities: docker")
    );
    assert_eq!(inventory_report.workers[0].status, HealthStatus::Ready);
    assert!(inventory_report.workers[0].missing_capabilities.is_empty());
    assert_eq!(config.workers[0].capabilities, ["darwin-arm64"]);
}

#[test]
fn inspect_with_requirements_uses_inventory_first_stable_union_for_multiple_missing_values() {
    // Catches project-first probing, duplicate requirements, or mutating the
    // configured inventory while constructing the required-capability union.
    let runner = RecordingRunner::returning_json(structured_probe_json(
        PROTOCOL_VERSION,
        "mini-1.local",
        "arm64",
        "26.2",
        vec!["node".into()],
    ));
    let config = Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        workers: vec![worker(
            "mini-1",
            "mac1",
            &["darwin-arm64", "docker", "darwin-arm64", "ruby"],
        )],
    };
    let original_inventory = config.workers[0].capabilities.clone();
    let service = WorkersService::new(SshTransport::new(runner));

    let report = service.inspect_with_requirements(
        &config,
        &[
            "node".into(),
            "docker".into(),
            "swift".into(),
            "node".into(),
            "go".into(),
        ],
    );

    assert_eq!(report.workers[0].status, HealthStatus::Unavailable);
    assert_eq!(
        report.workers[0].missing_capabilities,
        ["darwin-arm64", "docker", "ruby", "swift", "go"]
    );
    assert_eq!(
        report.workers[0].error_message.as_deref(),
        Some("worker is missing required capabilities: darwin-arm64, docker, ruby, swift, go")
    );
    assert_eq!(config.workers[0].capabilities, original_inventory);
}

#[test]
fn system_runner_passes_arguments_without_shell_interpretation() {
    let literal = "$(printf injected); $HOME *";
    let request = ProcessRequest {
        program: "/usr/bin/printf".into(),
        args: vec!["%s".into(), literal.into()],
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: None,
        policy: local_test_policy(),
        isolate_parent_environment: false,
    };

    let result = SystemProcessRunner.run(&request).unwrap();

    assert!(result.status.success());
    assert_eq!(result.stdout, literal.as_bytes());
    assert!(result.stderr.is_empty());
}

#[test]
fn system_runner_writes_the_requested_stdin() {
    let request = ProcessRequest {
        program: "/bin/cat".into(),
        args: Vec::new(),
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: Some(b"raw stdin bytes\n".to_vec()),
        policy: local_test_policy(),
        isolate_parent_environment: false,
    };

    let result = SystemProcessRunner.run(&request).unwrap();

    assert!(result.status.success());
    assert_eq!(result.stdout, b"raw stdin bytes\n");
    assert!(result.stderr.is_empty());
}

#[test]
fn human_workers_output_includes_all_parsed_health_facts() {
    // This catches dropping detected capabilities or host resource facts from
    // the stable human view while JSON remains complete.
    let output = CommandOutput::Workers(WorkersReport {
        protocol_version: PROTOCOL_VERSION,
        workers: vec![WorkerHealth {
            name: "mini-1".into(),
            ssh: "mac1".into(),
            status: HealthStatus::Ready,
            probe: Some(ProbeResponse {
                protocol_version: PROTOCOL_VERSION,
                supervision_version: mac_worker::protocol::SUPERVISION_VERSION,
                hostname: "mini-1.local".into(),
                arch: "arm64".into(),
                os_version: "26.2".into(),
                free_disk_bytes: 536_870_912,
                total_disk_bytes: 1_073_741_824,
                memory_pressure: MemoryPressure::Warn,
                swap_used_bytes: Some(134_217_728),
                available_memory_bytes: Some(12 * 1024 * 1024 * 1024),
                cpu_counters: Some(CpuCounters {
                    user_ticks: 10,
                    system_ticks: 20,
                    idle_ticks: 30,
                    nice_ticks: 40,
                }),
                slot_state: SlotState::Busy,
                active_lease: Some(LeaseSummary {
                    job_id: "00000000000000000000000000000001".parse().unwrap(),
                    project_id: "a".repeat(64),
                    worktree_id: "b".repeat(64),
                    created_at_millis: 10,
                }),
                capabilities: vec!["darwin-arm64".into(), "git".into()],
                agent_facts: None,
                facts_age_millis: None,
            }),
            missing_capabilities: Vec::new(),
            error_code: None,
            error_message: None,
        }],
    });

    let rendered = output.render_human();

    for fragment in [
        "slot: busy",
        "active job: 00000000000000000000000000000001",
        "capabilities: darwin-arm64, git",
        "free disk bytes: 536870912",
        "memory pressure: warn",
        "swap used bytes: 134217728",
    ] {
        assert!(
            rendered.contains(fragment),
            "missing {fragment:?} in {rendered:?}"
        );
    }
}

#[test]
fn human_unavailable_worker_keeps_error_missing_capabilities_and_unknown_swap_visible() {
    // This catches treating a parsed but capability-ineligible probe as if no
    // diagnostic or missing-capability detail existed.
    let output = CommandOutput::Workers(WorkersReport {
        protocol_version: PROTOCOL_VERSION,
        workers: vec![WorkerHealth {
            name: "mini-1".into(),
            ssh: "mac1".into(),
            status: HealthStatus::Unavailable,
            probe: Some(ProbeResponse {
                protocol_version: PROTOCOL_VERSION,
                supervision_version: mac_worker::protocol::SUPERVISION_VERSION,
                hostname: "mini-1.local".into(),
                arch: "arm64".into(),
                os_version: "26.2".into(),
                free_disk_bytes: 536_870_912,
                total_disk_bytes: 1_073_741_824,
                memory_pressure: MemoryPressure::Unknown,
                swap_used_bytes: None,
                available_memory_bytes: None,
                cpu_counters: None,
                slot_state: SlotState::Idle,
                active_lease: None,
                capabilities: vec!["darwin-arm64".into()],
                agent_facts: None,
                facts_age_millis: None,
            }),
            missing_capabilities: vec!["docker".into(), "swift".into()],
            error_code: Some("MISSING_CAPABILITIES".into()),
            error_message: Some("worker is missing declared capabilities".into()),
        }],
    });

    let rendered = output.render_human();

    for fragment in [
        "mini-1: unavailable [MISSING_CAPABILITIES]: worker is missing declared capabilities",
        "missing capabilities: docker, swift",
        "capabilities: darwin-arm64",
        "swap used bytes: unavailable",
    ] {
        assert!(
            rendered.contains(fragment),
            "missing {fragment:?} in {rendered:?}"
        );
    }
}

#[test]
fn workers_output_includes_profile_keyed_facts_without_values() {
    // Additive Task 1 grammar: workers reports names, secure state, and age
    // without profile values. Retention reports belong to `worker gc`.
    let planted = "workers-command-profile-value-must-not-escape";
    let output = CommandOutput::Workers(WorkersReport {
        protocol_version: PROTOCOL_VERSION,
        workers: vec![WorkerHealth {
            name: "mini-1".into(),
            ssh: "mac1".into(),
            status: HealthStatus::Ready,
            probe: Some(ProbeResponse {
                protocol_version: PROTOCOL_VERSION,
                supervision_version: mac_worker::protocol::SUPERVISION_VERSION,
                hostname: "mini-1.local".into(),
                arch: "arm64".into(),
                os_version: "26.2".into(),
                free_disk_bytes: 536_870_912,
                total_disk_bytes: 1_073_741_824,
                memory_pressure: MemoryPressure::Normal,
                swap_used_bytes: None,
                available_memory_bytes: None,
                cpu_counters: None,
                slot_state: SlotState::Idle,
                active_lease: None,
                capabilities: vec!["darwin-arm64".into(), "agent:cursor@agents".into()],
                agent_facts: Some(AgentFacts {
                    agents: vec![AgentProbe {
                        name: "cursor".into(),
                        version: Some("1.0.0".into()),
                        auth: AgentAuth::Authenticated,
                        auth_by_profile: vec![("agents".into(), AgentAuth::Authenticated)],
                    }],
                    env_profiles: vec![ProfileProbe {
                        name: "agents".into(),
                        secure: true,
                    }],
                    git_identity: true,
                    collected_at_millis: 10,
                    herdr: None,
                }),
                facts_age_millis: Some(1_000),
            }),
            missing_capabilities: Vec::new(),
            error_code: None,
            error_message: None,
        }],
    });

    let rendered = output.render_human();
    let json = output.render_json().unwrap();

    for fragment in [
        "agent facts age millis: 1000",
        "git identity: configured",
        "cursor 1.0.0: authenticated",
        "agents: secure",
        "agent:cursor@agents",
    ] {
        assert!(
            rendered.contains(fragment),
            "missing {fragment:?} in {rendered:?}"
        );
    }
    assert!(!rendered.contains(planted));
    assert!(!json.contains(planted));
    assert!(json.contains("\"facts_age_millis\":1000"));
    assert!(json.contains("\"secure\":true"));
    assert!(!json.contains("/Users/"));
}

#[test]
fn workers_render_a_turn_auth_failure_reason() {
    let reason = turn_auth_failure_reason(1_704_067_200_000).unwrap();
    let output = CommandOutput::Workers(WorkersReport {
        protocol_version: PROTOCOL_VERSION,
        workers: vec![WorkerHealth {
            name: "mini-1".into(),
            ssh: "mac1".into(),
            status: HealthStatus::Ready,
            probe: Some(ProbeResponse {
                protocol_version: PROTOCOL_VERSION,
                supervision_version: mac_worker::protocol::SUPERVISION_VERSION,
                hostname: "mini-1.local".into(),
                arch: "arm64".into(),
                os_version: "26.2".into(),
                free_disk_bytes: 500,
                total_disk_bytes: 1_000,
                memory_pressure: MemoryPressure::Normal,
                swap_used_bytes: None,
                available_memory_bytes: None,
                cpu_counters: None,
                slot_state: SlotState::Idle,
                active_lease: None,
                capabilities: vec!["darwin-arm64".into()],
                agent_facts: Some(AgentFacts {
                    agents: vec![AgentProbe {
                        name: "codex".into(),
                        version: Some("0.152.1".into()),
                        auth: AgentAuth::UnknownWithReason(reason),
                        auth_by_profile: Vec::new(),
                    }],
                    env_profiles: Vec::new(),
                    git_identity: true,
                    collected_at_millis: 1,
                    herdr: None,
                }),
                facts_age_millis: Some(0),
            }),
            missing_capabilities: Vec::new(),
            error_code: None,
            error_message: None,
        }],
    })
    .render_human();
    assert!(
        output.contains("codex 0.152.1: unknown (auth failed in a turn at 2024-01-01T00:00Z)"),
        "{output}"
    );
}

#[test]
fn workers_refresh_clear_auth_incidents_uses_the_clearing_host_command() {
    let runner = RecordingRunner::returning_results(vec![refresh_ok(), probe_ok()]);
    let temporary = tempfile::tempdir().unwrap();
    let config_path = temporary.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap();
    let runtime = RuntimeContext::isolated(
        BTreeMap::new(),
        temporary.path().join("home"),
        temporary.path().to_path_buf(),
    );
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_io_in_context(
        Cli {
            config: Some(config_path),
            json: false,
            command: Command::Workers {
                refresh: true,
                clear_auth_incidents: true,
            },
        },
        &runner,
        &runtime,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&stderr));
    assert_eq!(
        ssh_destinations(&runner, "host refresh-facts --clear-auth-incidents"),
        ["mac1"]
    );
}

fn refresh_ok() -> Result<ProcessResult, WorkerError> {
    Ok(ProcessResult {
        status: exit_status(0),
        stdout: Vec::new(),
        stderr: Vec::new(),
    })
}

fn refresh_unreachable() -> Result<ProcessResult, WorkerError> {
    Ok(ProcessResult {
        status: exit_status(255),
        stdout: Vec::new(),
        stderr: b"connection refused".to_vec(),
    })
}

fn probe_ok() -> Result<ProcessResult, WorkerError> {
    Ok(ProcessResult {
        status: exit_status(0),
        stdout: valid_probe_json(),
        stderr: Vec::new(),
    })
}

fn probe_unreachable() -> Result<ProcessResult, WorkerError> {
    Ok(ProcessResult {
        status: exit_status(255),
        stdout: Vec::new(),
        stderr: b"connection refused".to_vec(),
    })
}

fn run_workers_refresh(
    runner: &RecordingRunner,
    worker_count: usize,
    json: bool,
) -> (u8, String, String) {
    let temporary = tempfile::tempdir().unwrap();
    let mut body = String::from("version = 1\n");
    for index in 1..=worker_count {
        body.push_str(&format!(
            "[[workers]]\nname = \"mini-{index}\"\nssh = \"mac{index}\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n"
        ));
    }
    let config_path = temporary.path().join("config.toml");
    fs::write(&config_path, body).unwrap();
    let runtime = RuntimeContext::isolated(
        BTreeMap::new(),
        temporary.path().join("home"),
        temporary.path().to_path_buf(),
    );
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_io_in_context(
        Cli {
            config: Some(config_path),
            json,
            command: Command::Workers {
                refresh: true,
                clear_auth_incidents: false,
            },
        },
        runner,
        &runtime,
        &mut stdout,
        &mut stderr,
    );
    (
        exit,
        String::from_utf8(stdout).unwrap(),
        String::from_utf8_lossy(&stderr).into_owned(),
    )
}

fn ssh_destinations(runner: &RecordingRunner, command_suffix: &str) -> Vec<String> {
    runner
        .requests()
        .into_iter()
        .filter(|request| {
            request.program == "/usr/bin/ssh"
                && request
                    .args
                    .last()
                    .is_some_and(|argument| argument.to_string_lossy().ends_with(command_suffix))
        })
        .map(|request| request_destination(&request))
        .collect()
}

fn parse_workers_json(stdout: &str) -> serde_json::Value {
    serde_json::from_str(stdout.trim()).expect("workers --refresh JSON")
}

#[test]
fn refresh_reports_every_worker_when_all_succeed() {
    let runner = RecordingRunner::returning_results(vec![
        refresh_ok(),
        refresh_ok(),
        probe_ok(),
        probe_ok(),
    ]);

    let (exit, stdout, stderr) = run_workers_refresh(&runner, 2, true);

    assert_eq!(exit, 0, "{stderr}");
    assert_eq!(
        ssh_destinations(&runner, "host refresh-facts"),
        ["mac1", "mac2"]
    );
    assert_eq!(ssh_destinations(&runner, "host probe"), ["mac1", "mac2"]);
    let value = parse_workers_json(&stdout);
    assert_eq!(value["kind"], "workers");
    assert_eq!(value["workers"][0]["name"], "mini-1");
    assert_eq!(value["workers"][0]["ssh"], "mac1");
    assert_eq!(value["workers"][0]["status"], "ready");
    assert_eq!(value["workers"][0]["refresh"]["ok"], true);
    assert_eq!(value["workers"][1]["name"], "mini-2");
    assert_eq!(value["workers"][1]["status"], "ready");
    assert_eq!(value["workers"][1]["refresh"]["ok"], true);

    let runner = RecordingRunner::returning_results(vec![
        refresh_ok(),
        refresh_ok(),
        probe_ok(),
        probe_ok(),
    ]);
    let (exit, stdout, stderr) = run_workers_refresh(&runner, 2, false);
    assert_eq!(exit, 0, "{stderr}");
    assert!(
        stdout.contains("mini-1") && stdout.contains("mini-2"),
        "{stdout}"
    );
    assert_eq!(stdout.matches("refresh: ok").count(), 2, "{stdout}");
}

#[test]
fn refresh_keeps_reporting_when_one_worker_is_unreachable() {
    let runner = RecordingRunner::returning_results(vec![
        refresh_ok(),
        refresh_unreachable(),
        probe_ok(),
        probe_unreachable(),
    ]);

    let (exit, stdout, stderr) = run_workers_refresh(&runner, 2, true);

    assert_eq!(exit, 0, "{stderr}");
    assert_eq!(
        ssh_destinations(&runner, "host refresh-facts"),
        ["mac1", "mac2"]
    );
    assert_eq!(ssh_destinations(&runner, "host probe"), ["mac1", "mac2"]);
    let value = parse_workers_json(&stdout);
    assert_eq!(value["workers"][0]["name"], "mini-1");
    assert_eq!(value["workers"][0]["status"], "ready");
    assert_eq!(value["workers"][0]["refresh"]["ok"], true);
    assert!(value["workers"][0]["probe"].is_object());
    assert_eq!(value["workers"][1]["name"], "mini-2");
    assert_eq!(value["workers"][1]["refresh"]["ok"], false);
    assert_eq!(
        value["workers"][1]["refresh"]["error_code"],
        "REFRESH_FACTS_FAILED"
    );
    assert!(
        value["workers"][1]["refresh"]["error_message"]
            .as_str()
            .is_some_and(|message| message.contains("worker fact refresh failed")),
        "{value}"
    );

    let runner = RecordingRunner::returning_results(vec![
        refresh_ok(),
        refresh_unreachable(),
        probe_ok(),
        probe_unreachable(),
    ]);
    let (exit, stdout, stderr) = run_workers_refresh(&runner, 2, false);
    assert_eq!(exit, 0, "{stderr}");
    assert!(stdout.contains("mini-1: ready"), "{stdout}");
    assert!(stdout.contains("mini-2: unavailable"), "{stdout}");
    assert!(stdout.contains("refresh: ok"), "{stdout}");
    assert!(
        stdout.contains("refresh: failed [REFRESH_FACTS_FAILED]: worker fact refresh failed"),
        "{stdout}"
    );
}

#[test]
fn refresh_fails_only_when_every_worker_is_unreachable() {
    let runner = RecordingRunner::returning_results(vec![
        refresh_unreachable(),
        refresh_unreachable(),
        probe_unreachable(),
        probe_unreachable(),
    ]);

    let (exit, stdout, stderr) = run_workers_refresh(&runner, 2, true);

    assert_eq!(exit, 69, "{stderr}");
    assert_eq!(
        ssh_destinations(&runner, "host refresh-facts"),
        ["mac1", "mac2"]
    );
    let value = parse_workers_json(&stdout);
    assert_eq!(value["workers"][0]["name"], "mini-1");
    assert_eq!(value["workers"][1]["name"], "mini-2");
    assert_eq!(value["workers"][0]["refresh"]["ok"], false);
    assert_eq!(value["workers"][1]["refresh"]["ok"], false);
    assert_eq!(
        value["workers"][0]["refresh"]["error_code"],
        "REFRESH_FACTS_FAILED"
    );
    assert_eq!(
        value["workers"][1]["refresh"]["error_code"],
        "REFRESH_FACTS_FAILED"
    );

    let runner = RecordingRunner::returning_results(vec![
        refresh_unreachable(),
        refresh_unreachable(),
        probe_unreachable(),
        probe_unreachable(),
    ]);
    let (exit, stdout, stderr) = run_workers_refresh(&runner, 2, false);
    assert_eq!(exit, 69, "{stderr}");
    assert!(stdout.contains("mini-1"), "{stdout}");
    assert!(stdout.contains("mini-2"), "{stdout}");
    assert_eq!(
        stdout
            .matches("refresh: failed [REFRESH_FACTS_FAILED]")
            .count(),
        2,
        "{stdout}"
    );
}

fn herdr_health(facts: Option<AgentFacts>, facts_age_millis: Option<u64>) -> WorkerHealth {
    WorkerHealth {
        name: "mini-1".into(),
        ssh: "mac1".into(),
        status: HealthStatus::Ready,
        probe: Some(ProbeResponse {
            protocol_version: PROTOCOL_VERSION,
            supervision_version: mac_worker::protocol::SUPERVISION_VERSION,
            hostname: "mini-1.local".into(),
            arch: "arm64".into(),
            os_version: "26.2".into(),
            free_disk_bytes: 536_870_912,
            total_disk_bytes: 1_073_741_824,
            memory_pressure: MemoryPressure::Normal,
            swap_used_bytes: None,
            available_memory_bytes: None,
            cpu_counters: None,
            slot_state: SlotState::Idle,
            active_lease: None,
            capabilities: vec!["darwin-arm64".into()],
            agent_facts: facts,
            facts_age_millis,
        }),
        missing_capabilities: Vec::new(),
        error_code: None,
        error_message: None,
    }
}

fn facts_with_herdr(herdr: Option<HerdrFacts>) -> AgentFacts {
    AgentFacts {
        agents: vec![AgentProbe {
            name: "cursor".into(),
            version: Some("1.0.0".into()),
            auth: AgentAuth::Authenticated,
            auth_by_profile: vec![("agents".into(), AgentAuth::Authenticated)],
        }],
        env_profiles: vec![ProfileProbe {
            name: "agents".into(),
            secure: true,
        }],
        git_identity: true,
        collected_at_millis: 10,
        herdr,
    }
}

fn render_workers(health: WorkerHealth) -> (String, String) {
    let output = CommandOutput::Workers(WorkersReport {
        protocol_version: PROTOCOL_VERSION,
        workers: vec![health],
    });
    (output.render_human(), output.render_json().unwrap())
}

#[test]
fn workers_output_renders_the_herdr_line_for_every_state_after_the_agents_block() {
    // Spec 5.3: one `herdr:` line per worker; the operator reads availability
    // without a second command, and JSON carries the fact through the facts.
    let version = Some("0.9.0".to_owned());
    for (state, expected) in [
        (HerdrFactState::Available, "  herdr: available (0.9.0)"),
        (HerdrFactState::NotInstalled, "  herdr: not installed"),
        (
            HerdrFactState::NoSocket,
            "  herdr: installed (0.9.0), no socket",
        ),
        (
            HerdrFactState::NoResponse,
            "  herdr: installed (0.9.0), no response",
        ),
    ] {
        let herdr = HerdrFacts {
            state,
            version: if state == HerdrFactState::NotInstalled {
                None
            } else {
                version.clone()
            },
            interactive_agents: None,
        };
        let (rendered, json) = render_workers(herdr_health(
            Some(facts_with_herdr(Some(herdr.clone()))),
            Some(1_000),
        ));
        let lines = rendered.lines().collect::<Vec<_>>();
        let herdr_line = lines
            .iter()
            .position(|line| *line == expected)
            .unwrap_or_else(|| panic!("missing {expected:?} in {rendered}"));
        let agents_line = lines
            .iter()
            .position(|line| *line == "  agents:")
            .expect("agents block");
        let profiles_line = lines
            .iter()
            .position(|line| *line == "  profiles:")
            .expect("profiles block");
        assert!(
            agents_line < herdr_line && herdr_line < profiles_line,
            "the herdr line follows the agents block: {rendered}"
        );
        assert_eq!(
            rendered.matches("herdr:").count(),
            1,
            "exactly one herdr line: {rendered}"
        );
        let expected_json = serde_json::to_string(&serde_json::json!({ "herdr": herdr }))
            .unwrap()
            .trim_start_matches('{')
            .trim_end_matches('}')
            .to_owned();
        assert!(
            json.contains(&expected_json),
            "{expected_json} not in {json}"
        );
    }
}

#[test]
fn workers_output_renders_herdr_unknown_when_facts_are_missing_or_predate_the_fact() {
    let available = HerdrFacts {
        state: HerdrFactState::Available,
        version: Some("0.9.0".into()),
        interactive_agents: None,
    };
    for (label, health) in [
        ("missing facts", herdr_health(None, None)),
        (
            "facts predating the fact",
            herdr_health(Some(facts_with_herdr(None)), Some(0)),
        ),
        (
            "facts without a reported age",
            herdr_health(Some(facts_with_herdr(Some(available.clone()))), None),
        ),
    ] {
        let (rendered, _) = render_workers(health);
        assert!(
            rendered.lines().any(|line| line == "  herdr: unknown"),
            "{label}: expected `herdr: unknown` in {rendered}"
        );
        assert_eq!(rendered.matches("herdr:").count(), 1, "{label}: {rendered}");
    }

    let (stale, _) = render_workers(herdr_health(
        Some(facts_with_herdr(Some(available.clone()))),
        Some(4_120_000),
    ));
    assert!(
        stale
            .lines()
            .any(|line| line == "  herdr: available (0.9.0), stale 69m"),
        "stale facts keep the last collected fact: {stale}"
    );

    let (fresh, _) = render_workers(herdr_health(
        Some(facts_with_herdr(Some(available))),
        Some(FACTS_TTL),
    ));
    assert!(
        fresh.contains("  herdr: available (0.9.0)"),
        "facts at exactly the TTL are still fresh: {fresh}"
    );

    let (installed_without_version, _) = render_workers(herdr_health(
        Some(facts_with_herdr(Some(HerdrFacts {
            state: HerdrFactState::NoSocket,
            version: None,
            interactive_agents: None,
        }))),
        Some(0),
    ));
    assert!(
        installed_without_version.contains("  herdr: installed, no socket"),
        "{installed_without_version}"
    );
}

#[test]
fn workers_output_appends_interactive_agents_when_the_count_is_known_and_nonzero() {
    let with_two = HerdrFacts {
        state: HerdrFactState::Available,
        version: Some("0.9.0".into()),
        interactive_agents: Some(2),
    };
    let (rendered, json) = render_workers(herdr_health(
        Some(facts_with_herdr(Some(with_two.clone()))),
        Some(0),
    ));
    assert!(
        rendered
            .lines()
            .any(|line| line == "  herdr: available (0.9.0), 2 interactive agents"),
        "{rendered}"
    );
    assert!(json.contains(r#""interactive_agents":2"#), "{json}");

    let with_one = HerdrFacts {
        state: HerdrFactState::Available,
        version: Some("0.9.0".into()),
        interactive_agents: Some(1),
    };
    let (one, _) = render_workers(herdr_health(
        Some(facts_with_herdr(Some(with_one))),
        Some(0),
    ));
    assert!(
        one.lines()
            .any(|line| line == "  herdr: available (0.9.0), 1 interactive agent"),
        "{one}"
    );

    let zero = HerdrFacts {
        state: HerdrFactState::Available,
        version: Some("0.9.0".into()),
        interactive_agents: Some(0),
    };
    let (none, _) = render_workers(herdr_health(Some(facts_with_herdr(Some(zero))), Some(0)));
    assert!(
        none.lines()
            .any(|line| line == "  herdr: available (0.9.0)"),
        "a zero count is omitted: {none}"
    );
}
