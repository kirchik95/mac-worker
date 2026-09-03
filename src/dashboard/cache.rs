use std::collections::{BTreeMap, VecDeque};

use crate::dashboard::model::{DashboardError, DashboardWorker, Freshness};

pub const SNAPSHOT_INTERVAL_MILLIS: u64 = 2_000;
pub const OBSERVATION_TTL_MILLIS: u64 = 10_000;
pub const MAX_SAMPLES_PER_WORKER: usize = 150;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuCounters {
    total_ticks: u64,
    idle_ticks: u64,
}

impl CpuCounters {
    pub fn new(total_ticks: u64, idle_ticks: u64) -> Result<Self, DashboardError> {
        if idle_ticks > total_ticks {
            return Err(DashboardError::new(
                "INVALID_CPU_COUNTERS",
                "CPU idle ticks cannot exceed total ticks",
            ));
        }
        Ok(Self {
            total_ticks,
            idle_ticks,
        })
    }

    pub fn total_ticks(self) -> u64 {
        self.total_ticks
    }

    pub fn idle_ticks(self) -> u64 {
        self.idle_ticks
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CpuBusyPercent(f64);

impl CpuBusyPercent {
    pub fn new(value: f64) -> Result<Self, DashboardError> {
        if !value.is_finite() || !(0.0..=100.0).contains(&value) {
            return Err(DashboardError::new(
                "INVALID_CPU_BUSY_PERCENT",
                "CPU busy percent must be finite and between 0 and 100",
            ));
        }
        Ok(Self(value))
    }

    pub fn value(self) -> f64 {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct Observation {
    pub worker: DashboardWorker,
    pub observed_at_millis: u64,
    pub cpu_counters: Option<CpuCounters>,
}

#[derive(Debug, Clone)]
pub struct CachedObservation {
    pub worker: DashboardWorker,
    pub observed_at_millis: u64,
}

enum ObservationSample {
    Full(Box<Observation>),
    CpuOnly {
        observed_at_millis: u64,
        counters: CpuCounters,
    },
}

impl ObservationSample {
    fn observed_at_millis(&self) -> u64 {
        match self {
            Self::Full(observation) => observation.observed_at_millis,
            Self::CpuOnly {
                observed_at_millis, ..
            } => *observed_at_millis,
        }
    }

    fn cpu_counters(&self) -> Option<CpuCounters> {
        match self {
            Self::Full(observation) => observation.cpu_counters,
            Self::CpuOnly { counters, .. } => Some(*counters),
        }
    }
}

pub struct ObservationCache {
    samples: BTreeMap<String, VecDeque<ObservationSample>>,
}

impl ObservationCache {
    pub fn new() -> Self {
        Self {
            samples: BTreeMap::new(),
        }
    }

    pub fn record(&mut self, mut observation: Observation) -> Option<CachedObservation> {
        let worker_name = observation.worker.name.clone();
        let samples = self.samples.entry(worker_name).or_default();
        if !accepts_timestamp(samples, observation.observed_at_millis) {
            return None;
        }

        let cpu_busy_percent = observation.cpu_counters.and_then(|counters| {
            latest_cpu_counters(samples).and_then(|previous| calculate_cpu_busy(previous, counters))
        });
        observation.worker.freshness = Freshness::Current;
        observation.worker.observed_at_millis = Some(observation.observed_at_millis);
        observation.worker.system.cpu_busy_percent = cpu_busy_percent.map(CpuBusyPercent::value);

        let cached = CachedObservation {
            worker: observation.worker.clone(),
            observed_at_millis: observation.observed_at_millis,
        };
        push_bounded(samples, ObservationSample::Full(Box::new(observation)));
        Some(cached)
    }

    pub fn latest(&self, worker_name: &str) -> Option<&Observation> {
        self.samples
            .get(worker_name)?
            .iter()
            .rev()
            .find_map(|sample| {
                if let ObservationSample::Full(observation) = sample {
                    Some(observation.as_ref())
                } else {
                    None
                }
            })
    }

    pub fn stale_worker(&self, worker_name: &str, now_millis: u64) -> Option<DashboardWorker> {
        let observation = self.latest(worker_name)?;
        let age = now_millis.saturating_sub(observation.observed_at_millis);
        if age > OBSERVATION_TTL_MILLIS {
            return None;
        }

        let mut worker = observation.worker.clone();
        worker.freshness = Freshness::Stale;
        Some(worker)
    }

    pub fn record_cpu(
        &mut self,
        worker_name: &str,
        observed_at_millis: u64,
        counters: CpuCounters,
    ) -> Option<CpuBusyPercent> {
        let samples = self.samples.entry(worker_name.to_owned()).or_default();
        if !accepts_timestamp(samples, observed_at_millis) {
            return None;
        }

        let cpu_busy_percent = latest_cpu_counters(samples)
            .and_then(|previous| calculate_cpu_busy(previous, counters));
        push_bounded(
            samples,
            ObservationSample::CpuOnly {
                observed_at_millis,
                counters,
            },
        );
        cpu_busy_percent
    }

    pub fn sample_count(&self, worker_name: &str) -> usize {
        self.samples.get(worker_name).map_or(0, VecDeque::len)
    }
}

impl Default for ObservationCache {
    fn default() -> Self {
        Self::new()
    }
}

fn accepts_timestamp(samples: &VecDeque<ObservationSample>, observed_at_millis: u64) -> bool {
    samples
        .back()
        .is_none_or(|sample| observed_at_millis > sample.observed_at_millis())
}

fn latest_cpu_counters(samples: &VecDeque<ObservationSample>) -> Option<CpuCounters> {
    samples
        .iter()
        .rev()
        .find_map(ObservationSample::cpu_counters)
}

fn calculate_cpu_busy(previous: CpuCounters, current: CpuCounters) -> Option<CpuBusyPercent> {
    let delta_total = current.total_ticks.checked_sub(previous.total_ticks)?;
    let delta_idle = current.idle_ticks.checked_sub(previous.idle_ticks)?;
    if delta_total == 0 || delta_idle > delta_total {
        return None;
    }

    let busy_ticks = delta_total - delta_idle;
    let value = (busy_ticks as f64 / delta_total as f64) * 100.0;
    CpuBusyPercent::new(value).ok()
}

fn push_bounded(samples: &mut VecDeque<ObservationSample>, sample: ObservationSample) {
    samples.push_back(sample);
    if samples.len() > MAX_SAMPLES_PER_WORKER {
        samples.pop_front();
    }
}
