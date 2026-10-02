use mac_worker::test_support::{
    client_state::{
        scheduler::{
            AffinityHints, CandidateRejection, SchedulerPolicy, Selection, WorkerPreference,
        },
        scheduler_adapter::SchedulerProbeAdapter,
    },
    core::{
        config::{Config, WorkerEntry},
        protocol::{HealthStatus, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, WorkerHealth},
    },
    host::lease::SlotState,
};

const SESSION_FEATURE: &str = "task.session-import";

fn config() -> Config {
    Config {
        version: 1,
        notifications: Default::default(),
        controller: Default::default(),
        ssh: Default::default(),
        workers: vec![WorkerEntry {
            name: "mini-1".into(),
            ssh: "mac1".into(),
            slots: 1,
            capabilities: vec!["node".into()],
            remote_binary: "~/.local/bin/worker".into(),
            herdr: false,
        }],
    }
}

fn ready_health() -> WorkerHealth {
    WorkerHealth {
        name: "mini-1".into(),
        ssh: "mac1".into(),
        status: HealthStatus::Ready,
        probe: Some(ProbeResponse {
            features: None,
            protocol_version: PROTOCOL_VERSION,
            supervision_version: 2,
            hostname: "mini-1.local".into(),
            arch: "arm64".into(),
            os_version: "26.2".into(),
            free_disk_bytes: 500,
            total_disk_bytes: 512,
            memory_pressure: MemoryPressure::Normal,
            swap_used_bytes: None,
            available_memory_bytes: Some(12),
            cpu_counters: None,
            slot_state: SlotState::Idle,
            active_lease: None,
            capabilities: vec!["node".into()],
            agent_facts: None,
            facts_age_millis: None,
            configured_slots: 0,
            busy_slots: 0,
            build_id: None,
            binary_sha256: None,
        }),
        missing_capabilities: Vec::new(),
        error_code: None,
        error_message: None,
    }
}

fn select(health: WorkerHealth, requirements: &[String]) -> Selection {
    let observations = SchedulerProbeAdapter::observations_at(&config(), &[health], 1).unwrap();
    SchedulerPolicy::select(
        &observations,
        requirements,
        &WorkerPreference::Pinned {
            worker: "mini-1".into(),
        },
        &AffinityHints::none(),
    )
}

fn assert_missing(selection: Selection, missing: &[String]) {
    assert_eq!(
        selection,
        Selection::NoEligible {
            rejections: vec![CandidateRejection::MissingCapabilities {
                name: "mini-1".into(),
                missing: missing.to_vec(),
            }],
        }
    );
}

#[test]
fn advertised_host_features_become_unique_capabilities() {
    let mut health = ready_health();
    let probe = health.probe.as_mut().unwrap();
    probe.features = Some(vec![
        SESSION_FEATURE.into(),
        "task.other-feature".into(),
        SESSION_FEATURE.into(),
    ]);
    probe.capabilities.push("feature:task.other-feature".into());
    let observations = SchedulerProbeAdapter::observations_at(&config(), &[health], 1).unwrap();
    assert_eq!(
        observations[0].capabilities(),
        &[
            "node",
            "feature:task.other-feature",
            "feature:task.session-import"
        ]
    );
    let requirements = vec![format!("feature:{SESSION_FEATURE}")];
    assert_eq!(
        SchedulerPolicy::rank(&observations, &requirements, &AffinityHints::none()).len(),
        1
    );
}

#[test]
fn missing_or_empty_host_features_do_not_satisfy_feature_requirements() {
    let requirements = vec![format!("feature:{SESSION_FEATURE}")];
    for features in [
        None,
        Some(Vec::new()),
        Some(vec!["task.other-feature".into()]),
    ] {
        let mut health = ready_health();
        health.probe.as_mut().unwrap().features = features;
        assert_missing(select(health, &requirements), &requirements);
    }
}
