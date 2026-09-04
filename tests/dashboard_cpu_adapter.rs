use std::{collections::VecDeque, sync::Mutex, time::Duration};

use mac_worker::{
    dashboard::{
        model::{DashboardError, DashboardJob, DashboardQueueEntry, DashboardSnapshot},
        service::{
            Clock, DashboardDataSource, DashboardService, MonotonicClock, WorkerObservationResult,
        },
        source::project_worker,
    },
    lease::SlotState,
    protocol::{
        CpuCounters, HealthStatus, MemoryPressure, PROTOCOL_VERSION, ProbeResponse,
        SUPERVISION_VERSION, WorkerHealth,
    },
};

#[test]
fn cache_counters_sums_coordinated_probe_ticks_without_overflow() {
    let counters = mac_worker::dashboard::source::cache_counters(CpuCounters {
        user_ticks: 100,
        system_ticks: 200,
        idle_ticks: 300,
        nice_ticks: 400,
    })
    .unwrap();

    assert_eq!(counters.total_ticks(), 1_000);
    assert_eq!(counters.idle_ticks(), 300);
    assert!(
        mac_worker::dashboard::source::cache_counters(CpuCounters {
            user_ticks: u64::MAX,
            system_ticks: 1,
            idle_ticks: 0,
            nice_ticks: 0,
        })
        .is_none()
    );
}

#[test]
fn dashboard_computes_cpu_only_from_two_current_coordinated_probe_samples() {
    let source = SequentialSource::new([
        (1_000, probe_with_counters(1_000, 200)),
        (3_000, probe_with_counters(1_100, 220)),
    ]);
    let service = DashboardService::new(source, FixedClock, SystemMonotonicClock);

    let first = service.snapshot(Default::default()).unwrap();
    assert_eq!(cpu(&first), None);

    let second = service.snapshot(Default::default()).unwrap();
    assert_eq!(cpu(&second), Some(80.0));
}

#[test]
fn reset_or_missing_probe_counters_emit_null_and_keep_other_probe_facts() {
    let source = SequentialSource::new([
        (1_000, probe_with_counters(1_000, 200)),
        (3_000, probe_with_counters(1_100, 220)),
        (5_000, probe_with_counters(900, 180)),
        (7_000, probe_without_counters()),
    ]);
    let service = DashboardService::new(source, FixedClock, SystemMonotonicClock);

    assert_eq!(cpu(&service.snapshot(Default::default()).unwrap()), None);
    assert_eq!(
        cpu(&service.snapshot(Default::default()).unwrap()),
        Some(80.0)
    );
    assert_eq!(cpu(&service.snapshot(Default::default()).unwrap()), None);

    let missing = service.snapshot(Default::default()).unwrap();
    assert_eq!(cpu(&missing), None);
    assert_eq!(missing.workers[0].system.free_disk_bytes, Some(400));
    assert_eq!(missing.workers[0].system.total_disk_bytes, Some(1_000));
    assert_eq!(
        missing.workers[0].system.memory_pressure,
        Some(mac_worker::dashboard::model::DashboardMemoryPressure::Warn)
    );
    assert_eq!(missing.workers[0].system.swap_used_bytes, Some(42));
    let wire = serde_json::to_string(&missing).unwrap();
    assert!(!wire.contains("available_memory_bytes"));
}

#[test]
fn invalid_probe_counter_pair_does_not_make_the_worker_unavailable() {
    let mut probe = probe_without_counters();
    probe.cpu_counters = Some(CpuCounters {
        user_ticks: u64::MAX,
        system_ticks: 0,
        idle_ticks: 1,
        nice_ticks: 0,
    });
    let observation = project_worker(&ready_health(probe), 1_000).unwrap();

    assert_eq!(observation.cpu_counters, None);
    assert_eq!(observation.worker.system.free_disk_bytes, Some(400));
    assert_eq!(
        observation.worker.health,
        mac_worker::dashboard::model::WorkerHealth::Ready
    );
}

struct SequentialSource {
    reports: Mutex<VecDeque<(u64, ProbeResponse)>>,
}

impl SequentialSource {
    fn new(reports: impl IntoIterator<Item = (u64, ProbeResponse)>) -> Self {
        Self {
            reports: Mutex::new(reports.into_iter().collect()),
        }
    }
}

impl DashboardDataSource for SequentialSource {
    fn configured_workers(&self) -> Result<Vec<String>, DashboardError> {
        Ok(vec!["mini-1".into()])
    }

    fn collect_workers(&self, _deadline: Duration) -> Vec<WorkerObservationResult> {
        let Some((observed_at, probe)) = self.reports.lock().unwrap().pop_front() else {
            return Vec::new();
        };
        vec![WorkerObservationResult::Current(
            project_worker(&ready_health(probe), observed_at).unwrap(),
        )]
    }

    fn local_jobs(&self) -> Result<Vec<DashboardJob>, DashboardError> {
        Ok(Vec::new())
    }

    fn authoritative_active_jobs(
        &self,
        _deadline: Duration,
    ) -> Vec<Result<mac_worker::dashboard::model::DashboardJob, DashboardError>> {
        Vec::new()
    }

    fn queue_entries(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError> {
        Ok(Vec::new())
    }
}

struct FixedClock;

impl Clock for FixedClock {
    fn now_millis(&self) -> u64 {
        10_000
    }
}

struct SystemMonotonicClock;

impl MonotonicClock for SystemMonotonicClock {
    fn now_millis(&self) -> u64 {
        1
    }
}

fn cpu(snapshot: &DashboardSnapshot) -> Option<f64> {
    snapshot.workers[0].system.cpu_busy_percent
}

fn ready_health(probe: ProbeResponse) -> WorkerHealth {
    WorkerHealth {
        name: "mini-1".into(),
        ssh: "mac1".into(),
        status: HealthStatus::Ready,
        probe: Some(probe),
        missing_capabilities: Vec::new(),
        error_code: None,
        error_message: None,
    }
}

fn probe_with_counters(total_ticks: u64, idle_ticks: u64) -> ProbeResponse {
    ProbeResponse {
        protocol_version: PROTOCOL_VERSION,
        supervision_version: SUPERVISION_VERSION,
        hostname: "mini-1.local".into(),
        arch: "arm64".into(),
        os_version: "26.2".into(),
        free_disk_bytes: 400,
        total_disk_bytes: 1_000,
        memory_pressure: MemoryPressure::Warn,
        swap_used_bytes: Some(42),
        available_memory_bytes: Some(123),
        cpu_counters: Some(CpuCounters {
            user_ticks: total_ticks - idle_ticks,
            system_ticks: 0,
            idle_ticks,
            nice_ticks: 0,
        }),
        slot_state: SlotState::Idle,
        active_lease: None,
        capabilities: vec!["swift".into()],
        agent_facts: None,
        facts_age_millis: None,
    }
}

fn probe_without_counters() -> ProbeResponse {
    let mut probe = probe_with_counters(1_000, 200);
    probe.cpu_counters = None;
    probe
}
