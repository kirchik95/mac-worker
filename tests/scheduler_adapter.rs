use mac_worker::{
    agent_facts::{AgentAuth, AgentFacts, AgentProbe},
    config::{Config, WorkerEntry},
    error::WorkerError,
    lease::SlotState,
    protocol::{
        CpuCounters, HealthStatus, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, WorkerHealth,
    },
    scheduler::CandidateSlot,
    scheduler_adapter::SchedulerProbeAdapter,
};

const GIB: u64 = 1024 * 1024 * 1024;

fn config() -> Config {
    Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        controller: Default::default(),
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
            protocol_version: PROTOCOL_VERSION,
            supervision_version: 2,
            hostname: "mini-1.local".into(),
            arch: "arm64".into(),
            os_version: "26.2".into(),
            free_disk_bytes: 500 * GIB,
            total_disk_bytes: 512 * GIB,
            memory_pressure: MemoryPressure::Normal,
            swap_used_bytes: None,
            available_memory_bytes: Some(12 * GIB),
            cpu_counters: Some(CpuCounters {
                user_ticks: 10,
                system_ticks: 20,
                idle_ticks: 30,
                nice_ticks: 40,
            }),
            slot_state: SlotState::Idle,
            active_lease: None,
            capabilities: vec!["node".into()],
            agent_facts: None,
            facts_age_millis: None,
            configured_slots: 0,
            busy_slots: 0,
        }),
        missing_capabilities: Vec::new(),
        error_code: None,
        error_message: None,
    }
}

#[test]
fn adapter_projects_only_matching_ready_inventory_probe_into_policy_fact() {
    // Removing the ready-probe projection must make this scheduler fact unavailable.
    let facts = SchedulerProbeAdapter::observations(&config(), &[ready_health()]).unwrap();

    assert_eq!(facts[0].worker_name(), "mini-1");
    assert_eq!(facts[0].available_memory_bytes(), Some(12 * GIB));
}

#[test]
fn protocol_v2_probe_is_ineligible_after_the_single_v3_bump() {
    // Accepting a prior wire version would silently omit the v3 scheduler/dashboard facts.
    let mut health = ready_health();
    health.probe.as_mut().unwrap().protocol_version = 2;

    assert!(matches!(
        SchedulerProbeAdapter::observations(&config(), &[health]),
        Err(WorkerError::Protocol(_))
    ));
}

#[test]
fn unavailable_or_missing_probe_projects_to_busy_unready_policy_fact() {
    // Treating an unavailable host as idle would make it schedulable.
    let mut health = ready_health();
    health.status = HealthStatus::Unavailable;
    health.probe = None;

    let facts = SchedulerProbeAdapter::observations(&config(), &[health]).unwrap();
    let ranked = mac_worker::scheduler::SchedulerPolicy::rank(
        &facts,
        &["node".into()],
        &mac_worker::scheduler::AffinityHints::none(),
    );

    assert!(ranked.is_empty());
}

#[test]
fn mismatched_inventory_ssh_identity_is_a_protocol_error() {
    // Projecting a probe under a different configured SSH identity misattributes host facts.
    let mut health = ready_health();
    health.ssh = "other-host".into();

    assert!(matches!(
        SchedulerProbeAdapter::observations(&config(), &[health]),
        Err(WorkerError::Protocol(_))
    ));
}

#[test]
fn unknown_inventory_name_is_a_protocol_error() {
    // Accepting an unconfigured health name would allow an untrusted host into scheduling.
    let mut health = ready_health();
    health.name = "unconfigured".into();

    assert!(matches!(
        SchedulerProbeAdapter::observations(&config(), &[health]),
        Err(WorkerError::Protocol(_))
    ));
}

#[test]
fn ready_probe_without_memory_fact_preserves_unavailable_memory() {
    // Replacing a missing fact with zero would bias the scheduler toward a fabricated value.
    let mut health = ready_health();
    health.probe.as_mut().unwrap().available_memory_bytes = None;

    let facts = SchedulerProbeAdapter::observations(&config(), &[health]).unwrap();

    assert_eq!(facts[0].available_memory_bytes(), None);
}

#[test]
fn adapter_threads_interactive_agents_from_a_fresh_herdr_fact() {
    use mac_worker::agent_facts::{AgentFacts, HerdrFactState, HerdrFacts};

    let mut health = ready_health();
    let probe = health.probe.as_mut().unwrap();
    probe.agent_facts = Some(AgentFacts {
        agents: Vec::new(),
        env_profiles: Vec::new(),
        git_identity: false,
        collected_at_millis: 1,
        herdr: Some(HerdrFacts {
            state: HerdrFactState::Available,
            version: Some("0.9.0".into()),
            interactive_agents: Some(3),
        }),
        origin_https_helpers: Default::default(),
    });
    probe.facts_age_millis = Some(0);

    let facts = SchedulerProbeAdapter::observations(&config(), &[health]).unwrap();
    assert_eq!(facts[0].interactive_agents(), Some(3));

    let mut stale = ready_health();
    let probe = stale.probe.as_mut().unwrap();
    probe.agent_facts = Some(AgentFacts {
        agents: Vec::new(),
        env_profiles: Vec::new(),
        git_identity: false,
        collected_at_millis: 1,
        herdr: Some(HerdrFacts {
            state: HerdrFactState::Available,
            version: Some("0.9.0".into()),
            interactive_agents: Some(3),
        }),
        origin_https_helpers: Default::default(),
    });
    probe.facts_age_millis = Some(mac_worker::agent_facts::FACTS_TTL + 1);
    let stale_facts = SchedulerProbeAdapter::observations(&config(), &[stale]).unwrap();
    assert_eq!(stale_facts[0].interactive_agents(), None);
}

#[test]
fn a_turn_auth_failure_reason_is_not_an_agent_capability() {
    let reason = mac_worker::agent_facts::turn_auth_failure_reason(1_704_067_200_000).unwrap();
    let mut health = ready_health();
    let probe = health.probe.as_mut().unwrap();
    probe.agent_facts = Some(AgentFacts {
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
        origin_https_helpers: Default::default(),
    });
    probe.facts_age_millis = Some(0);
    let observations = SchedulerProbeAdapter::observations(&config(), &[health]).unwrap();
    assert!(
        !observations[0]
            .capabilities()
            .iter()
            .any(|capability| capability.starts_with("agent:")),
        "{:?}",
        observations[0].capabilities()
    );
}

#[test]
fn one_busy_slot_on_a_two_slot_probe_stays_idle_for_admission() {
    let mut health = ready_health();
    let probe = health.probe.as_mut().unwrap();
    probe.slot_state = SlotState::Busy;
    probe.configured_slots = 2;
    probe.busy_slots = 1;
    let facts = SchedulerProbeAdapter::observations(&config(), &[health]).unwrap();
    assert_eq!(facts[0].slot(), CandidateSlot::Idle);
    assert!(probe_has_free_slot());
}

fn probe_has_free_slot() -> bool {
    let mut probe = ready_health().probe.unwrap();
    probe.configured_slots = 2;
    probe.busy_slots = 1;
    probe.slot_state = SlotState::Busy;
    probe.has_free_execution_slot()
}
