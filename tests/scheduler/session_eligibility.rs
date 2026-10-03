use mac_worker::test_support::{
    agents::agent_facts::{AgentAuth, AgentFacts, AgentProbe, FACTS_TTL},
    client_state::{
        scheduler::{
            AffinityHints, CandidateRejection, SchedulerPolicy, Selection, WorkerPreference,
            rejection_code_for_missing,
        },
        scheduler_adapter::SchedulerProbeAdapter,
    },
    core::{
        config::{Config, WorkerEntry},
        protocol::{HealthStatus, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, WorkerHealth},
    },
    host::lease::SlotState,
    session::{SessionAgent, agent_min_requirement},
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

fn health_with_version(agent: SessionAgent, version: Option<&str>) -> WorkerHealth {
    let mut health = ready_health();
    let probe = health.probe.as_mut().unwrap();
    probe.features = Some(vec![SESSION_FEATURE.into()]);
    probe.agent_facts = Some(AgentFacts {
        agents: vec![AgentProbe {
            name: agent.as_str().into(),
            version: version.map(str::to_owned),
            auth: AgentAuth::Authenticated,
            auth_by_profile: Vec::new(),
            autoupdate: None,
        }],
        env_profiles: Vec::new(),
        git_identity: true,
        collected_at_millis: 1,
        herdr: None,
        origin_https_helpers: Default::default(),
    });
    probe.facts_age_millis = Some(0);
    health
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
fn rejection_code_for_missing_prioritizes_agent_version_requirements() {
    for missing in [
        Vec::new(),
        vec![format!("feature:{SESSION_FEATURE}")],
        vec!["agent:codex".into(), "node".into()],
        vec!["feature:agent-min:codex@0.160.0".into()],
    ] {
        assert_eq!(rejection_code_for_missing(&missing), "CAPABILITY_MISSING");
    }
    for missing in [
        vec![agent_min_requirement(SessionAgent::Codex, "0.160.0")],
        vec![
            format!("feature:{SESSION_FEATURE}"),
            agent_min_requirement(SessionAgent::Claude, "2.1.288"),
        ],
        vec!["agent-min:invalid".into(), "node".into()],
    ] {
        assert_eq!(
            rejection_code_for_missing(&missing),
            "SESSION_AGENT_TOO_OLD"
        );
    }
}

#[test]
fn agent_min_requirement_accepts_supported_versions() {
    for (agent, observed, minimum) in [
        (SessionAgent::Codex, "0.159.3", "0.160.0"),
        (SessionAgent::Codex, "0.160", "0.160.0"),
        (SessionAgent::Codex, "0.159.3-beta", "0.160.0+build.42"),
        (SessionAgent::Codex, "1.0.0", "0.160.0"),
        (SessionAgent::Claude, "2.1.285 (Claude Code)", "2.1.288"),
    ] {
        let requirements = vec![agent_min_requirement(agent, minimum)];
        let health = health_with_version(agent, Some(observed));
        assert!(
            matches!(
                select(health.clone(), &requirements),
                Selection::Selected(_)
            ),
            "{observed} vs {minimum}"
        );
        let observations = SchedulerProbeAdapter::observations_at(&config(), &[health], 1).unwrap();
        assert_eq!(observations[0].agent_versions().len(), 1);
        assert_eq!(
            observations[0]
                .agent_versions()
                .get(agent.as_str())
                .map(String::as_str),
            Some(observed)
        );
        assert_eq!(
            SchedulerPolicy::rank(&observations, &requirements, &AffinityHints::none()).len(),
            1
        );
    }
}

#[test]
fn agent_min_requirement_rejects_old_unparsable_or_absent_versions() {
    for observed in [Some("0.158.9"), Some("garbage"), None] {
        let requirements = vec![agent_min_requirement(SessionAgent::Codex, "0.160.0")];
        assert_missing(
            select(
                health_with_version(SessionAgent::Codex, observed),
                &requirements,
            ),
            &requirements,
        );
    }
    let requirements = vec![agent_min_requirement(SessionAgent::Claude, "2.0.0")];
    assert_missing(
        select(
            health_with_version(SessionAgent::Claude, Some("1.9.0")),
            &requirements,
        ),
        &requirements,
    );
}

#[test]
fn agent_min_requirement_needs_fresh_facts_with_an_age() {
    let requirements = vec![agent_min_requirement(SessionAgent::Codex, "0.160.0")];
    let fresh = health_with_version(SessionAgent::Codex, Some("0.159.3"));
    let mut ttl_boundary = fresh.clone();
    ttl_boundary.probe.as_mut().unwrap().facts_age_millis = Some(FACTS_TTL);
    assert!(matches!(
        select(ttl_boundary, &requirements),
        Selection::Selected(_)
    ));

    let mut stale = fresh.clone();
    stale.probe.as_mut().unwrap().facts_age_millis = Some(FACTS_TTL + 1);
    let mut missing_age = fresh.clone();
    missing_age.probe.as_mut().unwrap().facts_age_millis = None;
    let mut missing_facts = fresh;
    missing_facts.probe.as_mut().unwrap().agent_facts = None;
    for health in [stale, missing_age, missing_facts] {
        let observations =
            SchedulerProbeAdapter::observations_at(&config(), std::slice::from_ref(&health), 1)
                .unwrap();
        assert!(observations[0].agent_versions().is_empty());
        assert_missing(select(health.clone(), &requirements), &requirements);
        // Wire features are independent of the age of cached agent facts.
        assert!(matches!(
            select(health, &[format!("feature:{SESSION_FEATURE}")]),
            Selection::Selected(_)
        ));
    }
}

#[test]
fn agent_min_requirements_do_not_fall_back_to_exact_capability_membership() {
    for requirement in [
        "agent-min:codex@0.160.0",
        "agent-min:cursor@1.0.0",
        "agent-min:codex",
        "agent-min:codex@",
        "agent-min:codex@garbage",
        "agent-min:codex@0.160.0@extra",
    ] {
        let mut health = health_with_version(SessionAgent::Codex, Some("0.158.9"));
        health
            .probe
            .as_mut()
            .unwrap()
            .capabilities
            .push(requirement.into());
        let requirements = vec![requirement.into()];
        assert_missing(select(health, &requirements), &requirements);
    }
}

#[test]
fn agent_min_requirement_uses_only_the_named_agents_version() {
    let requirements = vec![agent_min_requirement(SessionAgent::Claude, "2.1.288")];
    assert_missing(
        select(
            health_with_version(SessionAgent::Codex, Some("2.1.288")),
            &requirements,
        ),
        &requirements,
    );
}

#[test]
fn mixed_requirements_keep_exact_capabilities_and_version_checks() {
    let requirements = vec![
        "node".into(),
        format!("feature:{SESSION_FEATURE}"),
        "agent:codex".into(),
        agent_min_requirement(SessionAgent::Codex, "0.160.0"),
    ];
    let fresh = health_with_version(SessionAgent::Codex, Some("0.159.3"));
    assert!(matches!(
        select(fresh.clone(), &requirements),
        Selection::Selected(_)
    ));

    let mut no_node = fresh.clone();
    no_node.probe.as_mut().unwrap().capabilities = vec!["nodejs".into()];
    assert_missing(select(no_node, &requirements), &requirements[..1]);

    let mut no_auth = fresh;
    no_auth
        .probe
        .as_mut()
        .unwrap()
        .agent_facts
        .as_mut()
        .unwrap()
        .agents[0]
        .auth = AgentAuth::Unknown;
    assert_missing(select(no_auth, &requirements), &requirements[2..3]);

    let mut old_without_feature = health_with_version(SessionAgent::Codex, Some("0.158.9"));
    old_without_feature.probe.as_mut().unwrap().features = None;
    assert_missing(
        select(old_without_feature, &requirements),
        &[requirements[1].clone(), requirements[3].clone()],
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
