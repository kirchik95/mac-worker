use std::{cmp::Reverse, collections::BTreeMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateSlot {
    Idle,
    Busy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidateObservationError {
    InvalidWorkerName,
    DuplicateCapability { capability: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateObservation {
    worker_name: String,
    ready: bool,
    slot: CandidateSlot,
    capabilities: Vec<String>,
    available_memory_bytes: Option<u64>,
    free_disk_bytes: u64,
}

impl CandidateObservation {
    pub fn new(
        worker_name: String,
        ready: bool,
        slot: CandidateSlot,
        capabilities: Vec<String>,
        available_memory_bytes: Option<u64>,
        free_disk_bytes: u64,
    ) -> Result<Self, CandidateObservationError> {
        if worker_name.trim().is_empty() {
            return Err(CandidateObservationError::InvalidWorkerName);
        }

        let mut seen = BTreeMap::new();
        for capability in &capabilities {
            if seen.insert(capability, ()).is_some() {
                return Err(CandidateObservationError::DuplicateCapability {
                    capability: capability.clone(),
                });
            }
        }

        Ok(Self {
            worker_name,
            ready,
            slot,
            capabilities,
            available_memory_bytes,
            free_disk_bytes,
        })
    }

    pub fn worker_name(&self) -> &str {
        &self.worker_name
    }

    pub fn ready(&self) -> bool {
        self.ready
    }

    pub fn slot(&self) -> CandidateSlot {
        self.slot
    }

    pub fn capabilities(&self) -> &[String] {
        &self.capabilities
    }

    pub fn available_memory_bytes(&self) -> Option<u64> {
        self.available_memory_bytes
    }

    pub fn free_disk_bytes(&self) -> u64 {
        self.free_disk_bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AffinityHints {
    pub worktree_worker: Option<String>,
    pub project_worker: Option<String>,
}

impl AffinityHints {
    pub fn none() -> Self {
        Self {
            worktree_worker: None,
            project_worker: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerPreference {
    Automatic,
    Pinned { worker: String },
}

/// Advisory explanation for a waiting queue row.  This is derived only from
/// the persisted admission cache and local queue state; it never grants
/// capacity or changes remote lifecycle authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueBlockingReason {
    PinnedWorkerBusy { worker: String },
    CapabilityMissing { missing: Vec<String> },
    RunCap,
    NoEligibleWorker,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidateRejection {
    DuplicateIdentity { name: String },
    Unavailable { name: String },
    MissingCapabilities { name: String, missing: Vec<String> },
    Busy { name: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankedCandidate {
    observation: CandidateObservation,
}

impl RankedCandidate {
    pub fn worker_name(&self) -> &str {
        self.observation.worker_name()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selection {
    Selected(RankedCandidate),
    NoEligible { rejections: Vec<CandidateRejection> },
}

pub struct SchedulerPolicy;

impl SchedulerPolicy {
    pub fn rank(
        observations: &[CandidateObservation],
        requirements: &[String],
        affinity: &AffinityHints,
    ) -> Vec<RankedCandidate> {
        let (eligible, _) = evaluate(observations, requirements);
        rank_eligible(eligible, affinity)
    }

    pub fn select(
        observations: &[CandidateObservation],
        requirements: &[String],
        preference: &WorkerPreference,
        affinity: &AffinityHints,
    ) -> Selection {
        let filtered = observations
            .iter()
            .filter(|observation| match preference {
                WorkerPreference::Automatic => true,
                WorkerPreference::Pinned { worker } => observation.worker_name() == worker,
            })
            .collect::<Vec<_>>();
        let (eligible, mut rejections) = evaluate_refs(&filtered, requirements);
        let ranked = rank_eligible(eligible, affinity);

        if let Some(candidate) = ranked.into_iter().next() {
            Selection::Selected(candidate)
        } else {
            rejections.sort_by(|left, right| rejection_name(left).cmp(rejection_name(right)));
            Selection::NoEligible { rejections }
        }
    }
}

fn evaluate<'a>(
    observations: &'a [CandidateObservation],
    requirements: &[String],
) -> (Vec<&'a CandidateObservation>, Vec<CandidateRejection>) {
    evaluate_refs(&observations.iter().collect::<Vec<_>>(), requirements)
}

fn evaluate_refs<'a>(
    observations: &[&'a CandidateObservation],
    requirements: &[String],
) -> (Vec<&'a CandidateObservation>, Vec<CandidateRejection>) {
    let names =
        observations
            .iter()
            .fold(BTreeMap::<&str, usize>::new(), |mut counts, observation| {
                *counts.entry(observation.worker_name()).or_default() += 1;
                counts
            });
    let mut eligible = Vec::new();
    let mut rejections = Vec::new();

    for observation in observations {
        let name = observation.worker_name().to_owned();
        let rejection = if names[observation.worker_name()] > 1 {
            Some(CandidateRejection::DuplicateIdentity { name })
        } else if !observation.ready {
            Some(CandidateRejection::Unavailable { name })
        } else {
            let missing = requirements
                .iter()
                .filter(|requirement| !observation.capabilities.contains(*requirement))
                .cloned()
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                Some(CandidateRejection::MissingCapabilities { name, missing })
            } else if observation.slot == CandidateSlot::Busy {
                Some(CandidateRejection::Busy { name })
            } else {
                None
            }
        };

        if let Some(rejection) = rejection {
            rejections.push(rejection);
        } else {
            eligible.push(*observation);
        }
    }

    (eligible, rejections)
}

fn rank_eligible(
    mut eligible: Vec<&CandidateObservation>,
    affinity: &AffinityHints,
) -> Vec<RankedCandidate> {
    eligible.sort_by(|left, right| compare_candidates(left, right, affinity));
    eligible
        .into_iter()
        .cloned()
        .map(|observation| RankedCandidate { observation })
        .collect()
}

fn compare_candidates(
    left: &CandidateObservation,
    right: &CandidateObservation,
    affinity: &AffinityHints,
) -> std::cmp::Ordering {
    ranking_key(left, affinity).cmp(&ranking_key(right, affinity))
}

fn ranking_key<'a>(
    observation: &'a CandidateObservation,
    affinity: &AffinityHints,
) -> (u8, u8, Reverse<u64>, Reverse<u64>, &'a str) {
    (
        affinity_class(observation, affinity),
        u8::from(observation.available_memory_bytes.is_none()),
        Reverse(observation.available_memory_bytes.unwrap_or_default()),
        Reverse(observation.free_disk_bytes),
        observation.worker_name(),
    )
}

fn affinity_class(observation: &CandidateObservation, affinity: &AffinityHints) -> u8 {
    if affinity.worktree_worker.as_deref() == Some(observation.worker_name()) {
        0
    } else if affinity.project_worker.as_deref() == Some(observation.worker_name()) {
        1
    } else {
        2
    }
}

fn rejection_name(rejection: &CandidateRejection) -> &str {
    match rejection {
        CandidateRejection::DuplicateIdentity { name }
        | CandidateRejection::Unavailable { name }
        | CandidateRejection::MissingCapabilities { name, .. }
        | CandidateRejection::Busy { name } => name,
    }
}
