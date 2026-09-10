use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    agent_facts::{AgentAuth, AgentFacts, FACTS_TTL},
    config::Config,
    error::WorkerError,
    protocol::{HealthStatus, PROTOCOL_VERSION, WorkerHealth},
    scheduler::{CandidateObservation, CandidateSlot},
};

pub struct SchedulerProbeAdapter;

impl SchedulerProbeAdapter {
    pub fn observations(
        config: &Config,
        health: &[WorkerHealth],
    ) -> Result<Vec<CandidateObservation>, WorkerError> {
        Self::observations_at(config, health, current_time_millis())
    }

    #[doc(hidden)]
    pub fn observations_at(
        config: &Config,
        health: &[WorkerHealth],
        _now_millis: u64,
    ) -> Result<Vec<CandidateObservation>, WorkerError> {
        health
            .iter()
            .map(|worker_health| {
                let worker = config.worker(&worker_health.name).ok_or_else(|| {
                    WorkerError::Protocol("scheduler probe inventory identity mismatch".into())
                })?;
                if worker.ssh != worker_health.ssh {
                    return Err(WorkerError::Protocol(
                        "scheduler probe inventory identity mismatch".into(),
                    ));
                }

                match (&worker_health.status, worker_health.probe.as_ref()) {
                    (HealthStatus::Ready, Some(probe)) => {
                        if probe.protocol_version != PROTOCOL_VERSION {
                            return Err(WorkerError::Protocol(format!(
                                "worker protocol version {} does not match required version {PROTOCOL_VERSION}",
                                probe.protocol_version
                            )));
                        }
                        let slot = if probe.has_free_execution_slot() {
                            CandidateSlot::Idle
                        } else {
                            CandidateSlot::Busy
                        };
                        let mut capabilities = probe
                            .capabilities
                            .iter()
                            .filter(|capability| !is_origin_capability(capability))
                            .cloned()
                            .collect();
                        append_inventory_origin_capabilities(&mut capabilities, worker);
                        append_agent_capabilities(
                            &mut capabilities,
                            probe.agent_facts.as_ref(),
                            probe.facts_age_millis,
                        );
                        CandidateObservation::new(
                            worker_health.name.clone(),
                            true,
                            slot,
                            capabilities,
                            probe.available_memory_bytes,
                            probe.free_disk_bytes,
                        )
                        .map(|observation| {
                            observation.with_interactive_agents(
                                probe.herdr_fact().and_then(|herdr| herdr.interactive_agents),
                            )
                        })
                    }
                    _ => CandidateObservation::new(
                        worker_health.name.clone(),
                        false,
                        CandidateSlot::Busy,
                        Vec::new(),
                        None,
                        0,
                    ),
                }
                .map_err(|_| WorkerError::Protocol("scheduler probe projection is invalid".into()))
            })
            .collect()
    }
}

fn append_inventory_origin_capabilities(
    capabilities: &mut Vec<String>,
    worker: &crate::config::WorkerEntry,
) {
    for capability in worker
        .capabilities
        .iter()
        .filter(|capability| is_origin_capability(capability))
    {
        push_unique(capabilities, capability.clone());
    }
}

fn is_origin_capability(capability: &str) -> bool {
    capability
        .strip_prefix("origin:")
        .is_some_and(|host| !host.is_empty())
}

fn append_agent_capabilities(
    capabilities: &mut Vec<String>,
    facts: Option<&AgentFacts>,
    facts_age_millis: Option<u64>,
) {
    let (Some(facts), Some(facts_age_millis)) = (facts, facts_age_millis) else {
        return;
    };
    if facts_age_millis > FACTS_TTL {
        return;
    }
    let secure_profiles = facts
        .env_profiles
        .iter()
        .filter(|profile| profile.secure)
        .map(|profile| profile.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for agent in &facts.agents {
        if agent.auth == AgentAuth::Authenticated {
            push_unique(capabilities, format!("agent:{}", agent.name));
        }
        for (profile, auth) in &agent.auth_by_profile {
            if *auth == AgentAuth::Authenticated && secure_profiles.contains(profile.as_str()) {
                push_unique(capabilities, format!("agent:{}@{profile}", agent.name));
            }
        }
    }
}

fn push_unique(capabilities: &mut Vec<String>, capability: String) {
    if !capabilities.contains(&capability) {
        capabilities.push(capability);
    }
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
