//! Build identity shown by `worker --version` and host skew in workers/doctor.

use std::process::Command;

use mac_worker::{
    binary_identity::current_binary_sha256,
    build_id::BUILD_ID,
    output::CommandOutput,
    protocol::{
        HealthStatus, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION,
        WorkerHealth, WorkersReport,
    },
};

#[test]
fn worker_version_prints_the_baked_build_id() {
    let binary = env!("CARGO_BIN_EXE_worker");
    let output = Command::new(binary)
        .arg("--version")
        .output()
        .expect("worker --version");
    assert!(output.status.success(), "stderr: {:?}", output.stderr);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout, format!("worker {BUILD_ID}\n"));
}

#[test]
fn workers_show_a_build_mismatch_for_one_host_and_unknown_for_an_older_helper() {
    let laptop_sha = current_binary_sha256().expect("laptop binary sha");
    let mismatched = "ab".repeat(32);
    assert_ne!(mismatched, laptop_sha);

    let output = CommandOutput::Workers(WorkersReport {
        protocol_version: PROTOCOL_VERSION,
        workers: vec![
            host("mini-1", Some(BUILD_ID), Some(laptop_sha)),
            host(
                "mini-2",
                Some("0.1.0+0123456789ab-release"),
                Some(mismatched),
            ),
            host("mini-3", None, None),
        ],
    });
    let human = output.render_human();
    let json = output.render_json().unwrap();

    assert!(human.contains(&format!("  build: {BUILD_ID}")), "{human}");
    assert!(human.contains("mini-2"), "{human}");
    assert!(
        human.contains(&format!(
            "warning [BUILD_MISMATCH]: mini-2 is 0.1.0+0123456789ab-release; laptop is {BUILD_ID}"
        )),
        "{human}"
    );
    assert!(
        !human.contains("BUILD_MISMATCH]: mini-1") && !human.contains("BUILD_MISMATCH]: mini-3"),
        "{human}"
    );
    assert!(human.contains("build: unknown (older helper)"), "{human}");
    assert!(!human.contains("mini-3: unavailable"), "{human}");

    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    let warnings = value["build_warnings"].as_array().expect("json warnings");
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0]["host"], "mini-2");
    assert_eq!(warnings[0]["code"], "BUILD_MISMATCH");
    assert_eq!(warnings[0]["host_build"], "0.1.0+0123456789ab-release");
    assert_eq!(warnings[0]["laptop_build"], BUILD_ID);
    assert!(value["workers"][2]["probe"]["build_id"].is_null());
    assert!(value["workers"][2]["probe"]["binary_sha256"].is_null());
}

fn host(name: &str, build_id: Option<&str>, binary_sha256: Option<String>) -> WorkerHealth {
    WorkerHealth {
        name: name.into(),
        ssh: format!("{name}-ssh"),
        status: HealthStatus::Ready,
        probe: Some(ProbeResponse {
            features: None,
            protocol_version: PROTOCOL_VERSION,
            supervision_version: SUPERVISION_VERSION,
            hostname: format!("{name}.local"),
            arch: "arm64".into(),
            os_version: "26.2".into(),
            free_disk_bytes: 1,
            total_disk_bytes: 2,
            memory_pressure: MemoryPressure::Normal,
            swap_used_bytes: None,
            available_memory_bytes: None,
            cpu_counters: None,
            slot_state: mac_worker::lease::SlotState::Idle,
            active_lease: None,
            capabilities: vec!["darwin-arm64".into()],
            agent_facts: None,
            facts_age_millis: None,
            configured_slots: 0,
            busy_slots: 0,
            build_id: build_id.map(str::to_owned),
            binary_sha256,
        }),
        missing_capabilities: Vec::new(),
        error_code: None,
        error_message: None,
    }
}
