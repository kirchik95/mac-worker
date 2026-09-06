use std::{
    os::unix::process::ExitStatusExt,
    process::ExitStatus,
    sync::{Arc, Mutex},
};

use mac_worker::{
    agent::{AgentKind, AuthProbeResult, adapter_for},
    config::{Config, WorkerEntry},
    error::WorkerError,
    lease::SlotState,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::{
        HealthStatus, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION,
        WorkerHealth,
    },
    scheduler_adapter::SchedulerProbeAdapter,
    transport::SshTransport,
};

fn result(stdout: &[u8]) -> ProcessResult {
    ProcessResult {
        status: ExitStatus::from_raw(0),
        stdout: stdout.to_vec(),
        stderr: Vec::new(),
    }
}

#[test]
fn every_adapter_owns_its_auth_probe_command_and_classifier() {
    let cases = [
        (
            AgentKind::Codex,
            ["login", "status"].as_slice(),
            b"Logged in using ChatGPT\n".as_slice(),
            b"Not logged in\n".as_slice(),
        ),
        (
            AgentKind::Claude,
            ["auth", "status"].as_slice(),
            br#"{"loggedIn":true}"#.as_slice(),
            br#"{"loggedIn":false}"#.as_slice(),
        ),
        (
            AgentKind::Cursor,
            ["status"].as_slice(),
            b"Authenticated as test-user\n".as_slice(),
            b"Not authenticated\n".as_slice(),
        ),
        (
            AgentKind::Opencode,
            ["auth", "list"].as_slice(),
            br#"[{"provider":"openai"}]"#.as_slice(),
            b"[]".as_slice(),
        ),
    ];

    for (kind, args, authenticated, unauthenticated) in cases {
        let probe = adapter_for(kind).auth_probe();
        assert_eq!(probe.args(), args);
        assert_eq!(
            probe.classify(&result(authenticated)),
            AuthProbeResult::Authenticated
        );
        assert_eq!(
            probe.classify(&result(unauthenticated)),
            AuthProbeResult::Unauthenticated
        );
    }
}

#[test]
fn ambiguous_agent_auth_output_is_not_projected_as_authenticated() {
    let cursor = adapter_for(AgentKind::Cursor).auth_probe();
    assert_eq!(
        cursor.classify(&result(b"Authenticated: false\n")),
        AuthProbeResult::Unauthenticated
    );
    assert_eq!(
        cursor.classify(&result(
            b"Authenticated as test-user\nwarning: status cache is stale\n"
        )),
        AuthProbeResult::Unknown
    );

    let opencode = adapter_for(AgentKind::Opencode).auth_probe();
    assert_eq!(opencode.classify(&result(b"")), AuthProbeResult::Unknown);
    assert_eq!(
        opencode.classify(&result(br#"{"error":"not authenticated"}"#)),
        AuthProbeResult::Unknown
    );
    assert_eq!(
        opencode.classify(&result(br#"{"status":"ok"}"#)),
        AuthProbeResult::Unknown
    );
    assert_eq!(
        opencode.classify(&result(br#"{"version":"1"}"#)),
        AuthProbeResult::Unknown
    );
    // OpenCode 1.18 prints a decorated human listing, not JSON.
    let listing = b"\x1b[0m\n\xe2\x94\x8c  Credentials \x1b[90m~/.local/share/opencode/auth.json\n\xe2\x94\x82\n\xe2\x97\x8f  GitHub Copilot \x1b[90moauth\n\xe2\x94\x82\n\xe2\x97\x8f  OpenCode Go \x1b[90mapi\n\xe2\x94\x94\n";
    assert_eq!(
        opencode.classify(&result(listing)),
        AuthProbeResult::Authenticated
    );
    let empty_listing =
        b"\xe2\x94\x8c  Credentials \x1b[90m~/.local/share/opencode/auth.json\n\xe2\x94\x94\n";
    assert_eq!(
        opencode.classify(&result(empty_listing)),
        AuthProbeResult::Unauthenticated
    );
    assert_eq!(
        opencode.classify(&result(b"No credentials found\n")),
        AuthProbeResult::Unauthenticated
    );
}

#[test]
fn cursor_reports_a_locked_login_keychain_with_an_operator_reason() {
    let cursor = adapter_for(AgentKind::Cursor).auth_probe();
    assert_eq!(
        cursor.classify(&result(
            b"Error: Your macOS login keychain is locked. Run security unlock-keychain and try again.\nadditional diagnostics\n",
        )),
        AuthProbeResult::UnknownWithReason("keychain locked")
    );
}

#[derive(Clone, Default)]
struct RecordingRunner {
    requests: Arc<Mutex<Vec<ProcessRequest>>>,
}

impl ProcessRunner for RecordingRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.requests.lock().unwrap().push(request.clone());
        Ok(result(&probe_bytes()))
    }
}

fn probe_bytes() -> Vec<u8> {
    serde_json::to_vec(&ProbeResponse {
        protocol_version: PROTOCOL_VERSION,
        supervision_version: SUPERVISION_VERSION,
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
        agent_facts: None,
        facts_age_millis: None,
    })
    .unwrap()
}

fn origin_worker() -> WorkerEntry {
    WorkerEntry {
        name: "mini-1".into(),
        ssh: "mac1".into(),
        slots: 1,
        capabilities: vec!["darwin-arm64".into(), "origin:gitlab.example.com".into()],
        remote_binary: "~/.local/bin/worker".into(),
    }
}

fn origin_config() -> Config {
    Config {
        version: 1,
        workers: vec![origin_worker()],
    }
}

fn raw_health_without_origin() -> WorkerHealth {
    WorkerHealth {
        name: "mini-1".into(),
        ssh: "mac1".into(),
        status: HealthStatus::Ready,
        probe: Some(ProbeResponse {
            protocol_version: PROTOCOL_VERSION,
            supervision_version: SUPERVISION_VERSION,
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
            agent_facts: None,
            facts_age_millis: None,
        }),
        missing_capabilities: Vec::new(),
        error_code: None,
        error_message: None,
    }
}

#[test]
fn configured_origin_capability_is_projected_into_worker_probe_and_scheduler_records() {
    let runner = RecordingRunner::default();
    let health = SshTransport::new(runner).probe(&origin_worker());

    assert_eq!(health.status, HealthStatus::Ready);
    assert!(
        health
            .probe
            .as_ref()
            .unwrap()
            .capabilities
            .contains(&"origin:gitlab.example.com".to_owned())
    );

    let observations =
        SchedulerProbeAdapter::observations(&origin_config(), &[raw_health_without_origin()])
            .unwrap();
    assert!(
        observations[0]
            .capabilities()
            .contains(&"origin:gitlab.example.com".to_owned())
    );
}

#[test]
fn scheduler_uses_only_inventory_declared_origin_capabilities() {
    let mut health = raw_health_without_origin();
    health
        .probe
        .as_mut()
        .unwrap()
        .capabilities
        .push("origin:untrusted.example.com".into());

    let observations = SchedulerProbeAdapter::observations(&origin_config(), &[health]).unwrap();

    assert!(
        observations[0]
            .capabilities()
            .contains(&"origin:gitlab.example.com".to_owned())
    );
    assert!(
        !observations[0]
            .capabilities()
            .contains(&"origin:untrusted.example.com".to_owned())
    );
}
