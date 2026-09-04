use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    agent_facts::{AgentAuth, AgentFacts},
    config::Config,
    error::WorkerError,
    lease::SlotState,
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
        now_millis: u64,
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
                        let slot = match probe.slot_state {
                            SlotState::Idle => CandidateSlot::Idle,
                            SlotState::Busy => CandidateSlot::Busy,
                        };
                        let mut capabilities = probe.capabilities.clone();
                        append_agent_capabilities(
                            &mut capabilities,
                            probe.agent_facts.as_ref(),
                            now_millis,
                        );
                        CandidateObservation::new(
                            worker_health.name.clone(),
                            true,
                            slot,
                            capabilities,
                            probe.available_memory_bytes,
                            probe.free_disk_bytes,
                        )
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

fn append_agent_capabilities(
    capabilities: &mut Vec<String>,
    facts: Option<&AgentFacts>,
    now_millis: u64,
) {
    let Some(facts) = facts.filter(|facts| !facts.is_stale(now_millis)) else {
        return;
    };
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
