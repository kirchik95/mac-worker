use crate::{
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
                        CandidateObservation::new(
                            worker_health.name.clone(),
                            true,
                            slot,
                            probe.capabilities.clone(),
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
