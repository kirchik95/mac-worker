use std::{
    collections::VecDeque,
    ffi::OsString,
    os::unix::process::ExitStatusExt,
    process::ExitStatus,
    sync::{Arc, Mutex},
    time::Duration,
};

use mac_worker::{
    config::{Config, WorkerEntry},
    error::{ProcessError, ProcessStream, WorkerError},
    lease::{LeaseSummary, SlotState},
    output::CommandOutput,
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    protocol::{
        HealthStatus, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, WorkerHealth, WorkersReport,
    },
    transport::{SshTransport, WorkersService},
};

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
        }]
    );
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
fn protocol_two_missing_occupancy_is_invalid_not_a_version_mismatch() {
    let response = br#"{"protocol_version":2,"hostname":"mini-1.local","arch":"arm64","os_version":"26.2","free_disk_bytes":536870912,"memory_pressure":"normal","swap_used_bytes":0,"capabilities":[]}"#.to_vec();
    let health = SshTransport::new(RecordingRunner::returning_json(response)).probe(&worker(
        "mini-1",
        "mac1",
        &[],
    ));

    assert_eq!(health.error_code.as_deref(), Some("INVALID_RESPONSE"));
    assert!(health.probe.is_none());
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
            .is_some_and(|message| message.contains("supervision"))
    );
    assert_eq!(
        health.probe.as_ref().map(|probe| probe.supervision_version),
        Some(0)
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

    assert_eq!(mac_worker::protocol::SUPERVISION_VERSION, 2);
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
                slot_state: SlotState::Busy,
                active_lease: Some(LeaseSummary {
                    job_id: "00000000000000000000000000000001".parse().unwrap(),
                    project_id: "a".repeat(64),
                    worktree_id: "b".repeat(64),
                    created_at_millis: 10,
                }),
                capabilities: vec!["darwin-arm64".into(), "git".into()],
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
                slot_state: SlotState::Idle,
                active_lease: None,
                capabilities: vec!["darwin-arm64".into()],
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
