use mac_worker::dashboard::{
    cache::{
        CpuBusyPercent, CpuCounters, MAX_SAMPLES_PER_WORKER, OBSERVATION_TTL_MILLIS, Observation,
        ObservationCache,
    },
    model::{
        DashboardMemoryPressure, DashboardSlotState, DashboardWorker, Freshness, SlotSummary,
        SystemSummary, WorkerHealth,
    },
};

#[test]
fn all_full_samples_share_one_bounded_history_and_keep_the_newest_full_observation() {
    let mut cache = ObservationCache::new();

    for observed_at_millis in 1..=151 {
        assert!(
            cache
                .record(observation("mini-1", observed_at_millis, None))
                .is_some()
        );
    }

    assert_eq!(cache.sample_count("mini-1"), MAX_SAMPLES_PER_WORKER);
    assert_eq!(cache.latest("mini-1").unwrap().observed_at_millis, 151);
}

#[test]
fn all_cpu_only_samples_share_one_bounded_history_without_creating_a_full_observation() {
    let mut cache = ObservationCache::new();

    for observed_at_millis in 1..=151 {
        let total_ticks = 1_000 + observed_at_millis;
        let idle_ticks = 200 + observed_at_millis;
        cache.record_cpu(
            "mini-1",
            observed_at_millis,
            CpuCounters::new(total_ticks, idle_ticks).unwrap(),
        );
    }

    assert_eq!(cache.sample_count("mini-1"), MAX_SAMPLES_PER_WORKER);
    assert!(cache.latest("mini-1").is_none());
}

#[test]
fn mixed_full_and_cpu_only_samples_share_the_same_bound_and_latest_searches_backward() {
    let mut cache = ObservationCache::new();

    for observed_at_millis in 1..=150 {
        if observed_at_millis % 2 == 0 {
            cache.record_cpu(
                "mini-1",
                observed_at_millis,
                CpuCounters::new(1_000 + observed_at_millis, 200).unwrap(),
            );
        } else {
            cache.record(observation("mini-1", observed_at_millis, None));
        }
    }
    cache.record_cpu("mini-1", 151, CpuCounters::new(1_151, 200).unwrap());

    assert_eq!(cache.sample_count("mini-1"), MAX_SAMPLES_PER_WORKER);
    assert_eq!(cache.latest("mini-1").unwrap().observed_at_millis, 149);
}

#[test]
fn evicting_the_oldest_counter_sample_removes_it_as_a_future_cpu_baseline() {
    let mut cache = ObservationCache::new();
    assert_eq!(
        cache.record_cpu("mini-1", 1, CpuCounters::new(1_000, 200).unwrap()),
        None
    );
    for observed_at_millis in 2..=151 {
        cache.record(observation("mini-1", observed_at_millis, None));
    }

    assert_eq!(cache.sample_count("mini-1"), MAX_SAMPLES_PER_WORKER);
    assert_eq!(
        cache.record_cpu("mini-1", 152, CpuCounters::new(1_100, 220).unwrap()),
        None,
        "an evicted counter must not remain as a hidden baseline"
    );
}

#[test]
fn duplicate_and_decreasing_timestamps_change_neither_history_full_worker_nor_cpu_baseline() {
    let mut cache = ObservationCache::new();
    let mut first = observation("mini-1", 100, Some(CpuCounters::new(1_000, 200).unwrap()));
    first.worker.hostname = Some("accepted.local".into());
    assert!(cache.record(first).is_some());

    assert_eq!(
        cache.record_cpu("mini-1", 100, CpuCounters::new(9_000, 100).unwrap()),
        None
    );
    let mut older = observation("mini-1", 99, Some(CpuCounters::new(8_000, 100).unwrap()));
    older.worker.hostname = Some("rejected.local".into());
    assert!(cache.record(older).is_none());

    assert_eq!(cache.sample_count("mini-1"), 1);
    assert_eq!(
        cache.latest("mini-1").unwrap().worker.hostname.as_deref(),
        Some("accepted.local")
    );
    assert_eq!(
        cache.record_cpu("mini-1", 200, CpuCounters::new(1_100, 220).unwrap()),
        Some(CpuBusyPercent::new(80.0).unwrap()),
        "the next accepted sample must compare with the last accepted baseline"
    );
}

#[test]
fn timestamps_are_ordered_independently_for_each_worker() {
    let mut cache = ObservationCache::new();

    assert!(cache.record(observation("mini-1", 10, None)).is_some());
    assert!(cache.record(observation("mini-2", 10, None)).is_some());
    assert!(cache.record(observation("mini-1", 9, None)).is_none());
    assert!(cache.record(observation("mini-2", 11, None)).is_some());

    assert_eq!(cache.sample_count("mini-1"), 1);
    assert_eq!(cache.sample_count("mini-2"), 2);
}

#[test]
fn cpu_busy_uses_only_consecutive_accepted_counter_samples() {
    let mut cache = ObservationCache::new();

    assert_eq!(
        cache.record_cpu("mini-1", 1_000, CpuCounters::new(1_000, 200).unwrap()),
        None
    );
    assert_eq!(
        cache.record_cpu("mini-1", 3_000, CpuCounters::new(1_100, 220).unwrap()),
        Some(CpuBusyPercent::new(80.0).unwrap())
    );
    assert_eq!(
        cache.record_cpu("mini-1", 5_000, CpuCounters::new(1_100, 220).unwrap()),
        None,
        "an equal total discards the interval"
    );
    assert_eq!(
        cache.record_cpu("mini-1", 7_000, CpuCounters::new(1_200, 230).unwrap()),
        Some(CpuBusyPercent::new(90.0).unwrap()),
        "the discarded equal-total sample still becomes the baseline"
    );
    assert_eq!(
        cache.record_cpu("mini-1", 9_000, CpuCounters::new(900, 180).unwrap()),
        None,
        "a decreased total discards the reset interval"
    );
    assert_eq!(
        cache.record_cpu("mini-1", 11_000, CpuCounters::new(1_000, 200).unwrap()),
        Some(CpuBusyPercent::new(80.0).unwrap()),
        "the reset sample still becomes the baseline"
    );
    assert_eq!(
        cache.record_cpu("mini-1", 13_000, CpuCounters::new(1_050, 260).unwrap()),
        None,
        "an idle delta larger than the total delta is invalid"
    );
    assert_eq!(
        cache.record_cpu("mini-1", 15_000, CpuCounters::new(1_150, 280).unwrap()),
        Some(CpuBusyPercent::new(80.0).unwrap()),
        "the invalid-delta sample still becomes the baseline"
    );
    assert_eq!(
        cache.record_cpu("mini-1", 17_000, CpuCounters::new(1_250, 270).unwrap()),
        None,
        "a decreased idle counter discards the interval"
    );
    assert_eq!(
        cache.record_cpu("mini-1", 19_000, CpuCounters::new(1_350, 290).unwrap()),
        Some(CpuBusyPercent::new(80.0).unwrap()),
        "the decreased-idle sample still becomes the baseline"
    );
}

#[test]
fn counter_and_percentage_value_objects_reject_invalid_values_and_preserve_valid_ones() {
    let counters = CpuCounters::new(1_000, 200).unwrap();
    assert_eq!(counters.total_ticks(), 1_000);
    assert_eq!(counters.idle_ticks(), 200);
    assert_eq!(
        CpuCounters::new(10, 11).unwrap_err().code,
        "INVALID_CPU_COUNTERS"
    );

    for valid in [0.0, 37.5, 100.0] {
        assert_eq!(CpuBusyPercent::new(valid).unwrap().value(), valid);
    }
    for invalid in [f64::NEG_INFINITY, -0.1, 100.1, f64::INFINITY, f64::NAN] {
        assert_eq!(
            CpuBusyPercent::new(invalid).unwrap_err().code,
            "INVALID_CPU_BUSY_PERCENT"
        );
    }
}

#[test]
fn full_records_normalize_and_return_the_exact_stored_worker_projection() {
    let mut cache = ObservationCache::new();
    let mut first = observation("mini-1", 1_000, Some(CpuCounters::new(1_000, 200).unwrap()));
    first.worker.freshness = Freshness::Offline;
    first.worker.observed_at_millis = Some(7);
    first.worker.system.cpu_busy_percent = Some(99.0);

    let first_cached = cache.record(first).unwrap();
    assert_eq!(first_cached.worker.freshness, Freshness::Current);
    assert_eq!(first_cached.worker.observed_at_millis, Some(1_000));
    assert_eq!(first_cached.worker.system.cpu_busy_percent, None);
    assert_eq!(first_cached.observed_at_millis, 1_000);

    let second_cached = cache
        .record(observation(
            "mini-1",
            3_000,
            Some(CpuCounters::new(1_100, 220).unwrap()),
        ))
        .unwrap();
    assert_eq!(second_cached.worker.freshness, Freshness::Current);
    assert_eq!(second_cached.worker.observed_at_millis, Some(3_000));
    assert_eq!(second_cached.worker.system.cpu_busy_percent, Some(80.0));
    assert_eq!(second_cached.observed_at_millis, 3_000);

    let stored = cache.latest("mini-1").unwrap();
    assert_eq!(stored.worker, second_cached.worker);
    assert_eq!(stored.observed_at_millis, second_cached.observed_at_millis);
}

#[test]
fn cpu_only_samples_never_retroactively_mutate_the_latest_full_worker() {
    let mut cache = ObservationCache::new();
    cache.record(observation(
        "mini-1",
        1_000,
        Some(CpuCounters::new(1_000, 200).unwrap()),
    ));

    assert_eq!(
        cache.record_cpu("mini-1", 3_000, CpuCounters::new(1_100, 220).unwrap()),
        Some(CpuBusyPercent::new(80.0).unwrap())
    );

    let latest = cache.latest("mini-1").unwrap();
    assert_eq!(latest.observed_at_millis, 1_000);
    assert_eq!(latest.worker.system.cpu_busy_percent, None);
}

#[test]
fn a_full_sample_without_counters_emits_null_without_erasing_the_retained_baseline() {
    let mut cache = ObservationCache::new();
    cache.record_cpu("mini-1", 1_000, CpuCounters::new(1_000, 200).unwrap());

    let without_counters = cache.record(observation("mini-1", 2_000, None)).unwrap();
    assert_eq!(without_counters.worker.system.cpu_busy_percent, None);

    let with_counters = cache
        .record(observation(
            "mini-1",
            3_000,
            Some(CpuCounters::new(1_100, 220).unwrap()),
        ))
        .unwrap();
    assert_eq!(with_counters.worker.system.cpu_busy_percent, Some(80.0));
}

#[test]
fn stale_projection_uses_inclusive_ttl_saturating_age_and_does_not_mutate_storage() {
    let mut cache = ObservationCache::new();
    cache.record(observation("mini-1", 20_000, None));

    let at_boundary = cache
        .stale_worker("mini-1", 20_000 + OBSERVATION_TTL_MILLIS)
        .unwrap();
    assert_eq!(at_boundary.freshness, Freshness::Stale);
    assert_eq!(at_boundary.observed_at_millis, Some(20_000));

    assert!(
        cache
            .stale_worker("mini-1", 20_001 + OBSERVATION_TTL_MILLIS)
            .is_none()
    );
    let regressed_clock = cache.stale_worker("mini-1", 19_999).unwrap();
    assert_eq!(regressed_clock.freshness, Freshness::Stale);
    assert_eq!(regressed_clock.observed_at_millis, Some(20_000));
    assert!(cache.stale_worker("missing", 20_000).is_none());

    assert_eq!(
        cache.latest("mini-1").unwrap().worker.freshness,
        Freshness::Current,
        "stale projection must not mutate the cached current worker"
    );
}

fn observation(
    worker_name: &str,
    observed_at_millis: u64,
    cpu_counters: Option<CpuCounters>,
) -> Observation {
    Observation {
        worker: DashboardWorker {
            name: worker_name.into(),
            health: WorkerHealth::Ready,
            freshness: Freshness::Stale,
            observed_at_millis: None,
            hostname: Some(format!("{worker_name}.local")),
            slot: SlotSummary {
                state: DashboardSlotState::Idle,
                capacity: 1,
                active_job_id: None,
            },
            capabilities: vec!["swift".into()],
            missing_capabilities: Vec::new(),
            system: SystemSummary {
                free_disk_bytes: Some(100),
                total_disk_bytes: Some(200),
                memory_pressure: Some(DashboardMemoryPressure::Normal),
                swap_used_bytes: Some(0),
                cpu_busy_percent: Some(75.0),
            },
            error: None,
            active_task: None,
        },
        observed_at_millis,
        cpu_counters,
    }
}
