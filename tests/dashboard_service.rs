use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use mac_worker::{
    agent_facts::{AgentFacts, FACTS_TTL},
    dashboard::{
        cache::{CpuCounters, OBSERVATION_TTL_MILLIS, Observation},
        model::{
            CollectionSummary, DASHBOARD_API_VERSION, DashboardCommandMode,
            DashboardCommandSummary, DashboardError, DashboardJob, DashboardJobState,
            DashboardMemoryPressure, DashboardQueueEntry, DashboardQueueEntryKind,
            DashboardSlotState, DashboardSnapshot, DashboardWorker, Freshness, SlotSummary,
            SystemSummary, WorkerHealth,
        },
        service::{
            Clock, DashboardDataSource, DashboardDeadlines, DashboardQueueReader, DashboardService,
            DashboardSnapshotRequest, EmptyDashboardQueueReader, GLOBAL_COLLECTION_DEADLINE,
            MAX_COLLECTION_ERRORS, MAX_RECENT_TERMINAL_JOBS, MonotonicClock, SNAPSHOT_PENDING,
            WORKER_COLLECTION_DEADLINE, WorkerObservationResult,
        },
        source::project_worker,
    },
    error::WorkerError,
    job::JobId,
    lease::SlotState,
    protocol::{
        HealthStatus, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION,
        WorkerHealth as ProbeWorkerHealth,
    },
    task_view::TaskListProjection,
};

#[test]
fn deadlines_and_empty_queue_reader_keep_the_bounded_default_contract() {
    assert_eq!(WORKER_COLLECTION_DEADLINE, Duration::from_secs(15));
    assert_eq!(GLOBAL_COLLECTION_DEADLINE, Duration::from_secs(20));
    assert_eq!(MAX_COLLECTION_ERRORS, 64);
    assert_eq!(MAX_RECENT_TERMINAL_JOBS, 100);
    assert!(DashboardDeadlines::new(Duration::ZERO, Duration::from_secs(1)).is_err());
    assert!(DashboardDeadlines::new(Duration::from_secs(1), Duration::ZERO).is_err());
    assert!(DashboardDeadlines::new(Duration::from_secs(2), Duration::from_secs(1)).is_err());
    DashboardDeadlines::new(Duration::from_millis(1), Duration::from_millis(1)).unwrap();
    assert!(
        EmptyDashboardQueueReader
            .ordered_pending()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn workers_follow_first_configured_order_and_reject_ambiguous_or_unknown_rows() {
    let source = FakeSource::new();
    source.set_configured(Ok(vec![
        "mini-b".into(),
        "mini-a".into(),
        "mini-b".into(),
        "mini-c".into(),
    ]));
    source.set_workers(vec![
        WorkerObservationResult::Current(observation("mini-a", 100, "accepted-a.local")),
        WorkerObservationResult::Current(observation("unknown-z", 100, "unknown.local")),
        WorkerObservationResult::Current(observation("mini-b", 100, "rejected-1.local")),
        WorkerObservationResult::Failed {
            worker_name: "mini-b".into(),
            error: error("B_FAILED", "second row makes mini-b ambiguous"),
        },
    ]);

    let snapshot = service(source.clone())
        .snapshot(Default::default())
        .unwrap();

    assert_eq!(worker_names(&snapshot), vec!["mini-b", "mini-a", "mini-c"]);
    assert_eq!(snapshot.workers[0].freshness, Freshness::Offline);
    assert_eq!(snapshot.workers[0].hostname, None);
    assert_eq!(snapshot.workers[1].freshness, Freshness::Current);
    assert_eq!(
        snapshot.workers[1].hostname.as_deref(),
        Some("accepted-a.local")
    );
    assert_eq!(snapshot.workers[2].freshness, Freshness::Offline);
    assert_eq!(
        error_codes(&snapshot),
        vec![
            "DUPLICATE_CONFIGURED_WORKER",
            "DUPLICATE_WORKER_OBSERVATION",
            "MISSING_WORKER_OBSERVATION",
            "UNKNOWN_WORKER_OBSERVATION",
        ]
    );
    assert_read_only(&source);
}

#[test]
fn offline_worker_uses_the_laptop_config_slot_count_as_capacity() {
    let source = FakeSource::new();
    source.set_slots("mini-1", 2);
    source.set_workers(vec![WorkerObservationResult::Failed {
        worker_name: "mini-1".into(),
        error: error("WORKER_DOWN", "current probe failed"),
    }]);

    let snapshot = service(source.clone())
        .snapshot(Default::default())
        .unwrap();

    assert_eq!(snapshot.workers[0].freshness, Freshness::Offline);
    assert_eq!(snapshot.workers[0].slot.capacity, 2);
    assert_eq!(snapshot.workers[0].slot.busy, 0);
    assert_eq!(snapshot.workers[0].slot.state, DashboardSlotState::Idle);
    assert!(snapshot.workers[0].slot.active_job_ids.is_empty());
    assert_read_only(&source);
}

#[test]
fn snapshot_slot_totals_sum_busy_and_capacity_across_workers() {
    let source = FakeSource::new();
    source.set_configured(Ok(vec!["mini-1".into(), "mini-2".into()]));
    let first_id = job_id(11);
    let second_id = job_id(22);
    let mut first = observation("mini-1", 100, "mini-1.local");
    first.worker.slot = SlotSummary {
        state: DashboardSlotState::Idle,
        capacity: 2,
        busy: 1,
        active_job_id: Some(first_id),
        active_job_ids: vec![first_id],
    };
    let mut second = observation("mini-2", 100, "mini-2.local");
    second.worker.slot = SlotSummary {
        state: DashboardSlotState::Busy,
        capacity: 2,
        busy: 2,
        active_job_id: Some(second_id),
        active_job_ids: vec![second_id, job_id(23)],
    };
    source.set_workers(vec![
        WorkerObservationResult::Current(first),
        WorkerObservationResult::Current(second),
    ]);

    let snapshot = service(source.clone())
        .snapshot(Default::default())
        .unwrap();

    let busy: u32 = snapshot
        .workers
        .iter()
        .map(|worker| u32::from(worker.slot.busy))
        .sum();
    let capacity: u32 = snapshot
        .workers
        .iter()
        .map(|worker| u32::from(worker.slot.capacity))
        .sum();
    assert_eq!(busy, 3);
    assert_eq!(capacity, 4);
    assert_eq!(snapshot.workers[0].slot.state, DashboardSlotState::Idle);
    assert_eq!(snapshot.workers[1].slot.state, DashboardSlotState::Busy);
    assert_read_only(&source);
}

#[test]
fn cache_normalization_stale_expiry_and_rejected_timestamps_have_exact_shapes() {
    let source = FakeSource::new();
    let wall = ManualClock::new(1_000);
    let monotonic = ManualMonotonic::new(0);
    let service = DashboardService::new(source.clone(), wall.clone(), monotonic);

    let mut unnormalized = observation_with_counters("mini-1", 1_000, "accepted.local", 1_000, 200);
    unnormalized.worker.freshness = Freshness::Offline;
    unnormalized.worker.observed_at_millis = Some(7);
    unnormalized.worker.system.cpu_busy_percent = Some(99.0);
    unnormalized.worker.error = Some(error("SHOULD_CLEAR", "source payload error"));
    source.set_workers(vec![WorkerObservationResult::Current(unnormalized)]);
    let current = service.snapshot(Default::default()).unwrap();
    assert_eq!(current.collection.freshness, Freshness::Current);
    assert_eq!(current.workers[0].freshness, Freshness::Current);
    assert_eq!(current.workers[0].observed_at_millis, Some(1_000));
    assert_eq!(current.workers[0].system.cpu_busy_percent, None);
    assert_eq!(current.workers[0].error, None);

    wall.set(1_000 + OBSERVATION_TTL_MILLIS);
    source.set_workers(vec![WorkerObservationResult::Failed {
        worker_name: "mini-1".into(),
        error: error("WORKER_DOWN", "current probe failed"),
    }]);
    let stale = service.snapshot(Default::default()).unwrap();
    assert_eq!(stale.collection.freshness, Freshness::Current);
    assert_eq!(stale.workers[0].freshness, Freshness::Stale);
    assert_eq!(stale.workers[0].observed_at_millis, Some(1_000));
    assert_eq!(stale.workers[0].hostname.as_deref(), Some("accepted.local"));
    assert_eq!(stale.workers[0].error.as_ref().unwrap().code, "WORKER_DOWN");

    source.set_workers(vec![WorkerObservationResult::Current(observation(
        "mini-1",
        1_000,
        "rejected.local",
    ))]);
    let rejected = service.snapshot(Default::default()).unwrap();
    assert_eq!(rejected.workers[0].freshness, Freshness::Stale);
    assert_eq!(
        rejected.workers[0].hostname.as_deref(),
        Some("accepted.local")
    );
    assert_eq!(
        rejected.workers[0].error.as_ref().unwrap().code,
        "INVALID_OBSERVATION_TIMESTAMP"
    );

    wall.set(1_001 + OBSERVATION_TTL_MILLIS);
    let expired = service.snapshot(Default::default()).unwrap();
    assert_eq!(
        expired.workers[0],
        offline_worker("mini-1", "INVALID_OBSERVATION_TIMESTAMP")
    );
    assert_read_only(&source);
}

#[test]
fn current_and_cached_workers_recheck_their_own_agent_facts_freshness() {
    let source = FakeSource::new();
    let wall = ManualClock::new(10_000);
    let observed = project_worker(&worker_with_facts(u64::MAX - 1, FACTS_TTL - 5), 10_000).unwrap();
    source.set_workers(vec![WorkerObservationResult::Current(observed)]);

    let service = DashboardService::new(source.clone(), wall.clone(), ManualMonotonic::new(0));
    let snapshot = service.snapshot(Default::default()).unwrap();

    assert_eq!(snapshot.workers[0].freshness, Freshness::Current);
    assert_eq!(
        snapshot.workers[0].agent_facts.as_ref().unwrap().freshness,
        mac_worker::dashboard::model::AgentFactsFreshness::Current
    );

    wall.set(10_006);
    source.set_workers(vec![WorkerObservationResult::Failed {
        worker_name: "mini-1".into(),
        error: error("WORKER_DOWN", "current probe failed"),
    }]);
    let cached = service.snapshot(Default::default()).unwrap();

    assert_eq!(cached.workers[0].freshness, Freshness::Stale);
    assert_eq!(
        cached.workers[0].agent_facts.as_ref().unwrap().freshness,
        mac_worker::dashboard::model::AgentFactsFreshness::Stale
    );
}

#[test]
fn queue_is_byte_for_field_passthrough_and_failure_is_partial_success() {
    let source = FakeSource::new();
    let entries = vec![queue_entry(2, 22), queue_entry(1, 11)];
    source.set_queue(Ok(entries.clone()));
    let service = service(source.clone());

    let snapshot = service.snapshot(Default::default()).unwrap();
    assert_eq!(
        snapshot.queue, entries,
        "the service must not reorder queue data"
    );

    source.set_queue(Err(error("QUEUE_LOCKED", "queue is temporarily locked")));
    let failed = service.snapshot(Default::default()).unwrap();
    assert!(failed.queue.is_empty());
    assert_eq!(failed.collection.freshness, Freshness::Current);
    assert_eq!(
        failed.collection.errors.last().unwrap().code,
        "QUEUE_LOCKED"
    );
    assert_read_only(&source);
}

#[test]
fn source_failures_stay_partial_and_error_count_is_bounded_in_stage_order() {
    let source = FakeSource::new();
    source.set_configured(Ok(std::iter::repeat_n("mini-1".to_owned(), 66).collect()));
    source.set_local_jobs(Err(error("LOCAL_FAILED", "local state failed")));
    source.set_remote(vec![Err(error("REMOTE_FAILED", "remote state failed"))]);
    source.set_queue(Err(error("QUEUE_FAILED", "queue failed")));

    let snapshot = service(source.clone())
        .snapshot(Default::default())
        .unwrap();
    assert_eq!(snapshot.collection.freshness, Freshness::Current);
    assert_eq!(snapshot.collection.errors.len(), MAX_COLLECTION_ERRORS);
    assert!(
        snapshot
            .collection
            .errors
            .iter()
            .all(|error| error.code == "DUPLICATE_CONFIGURED_WORKER")
    );

    source.set_configured(Err(error("CONFIG_FAILED", "configuration unavailable")));
    let partial = service(source.clone())
        .snapshot(Default::default())
        .unwrap();
    assert!(partial.workers.is_empty());
    assert!(partial.active_jobs.is_empty());
    assert!(partial.recent_jobs.is_empty());
    assert!(partial.queue.is_empty());
    assert_eq!(
        error_codes(&partial),
        vec![
            "CONFIG_FAILED",
            "LOCAL_FAILED",
            "REMOTE_FAILED",
            "QUEUE_FAILED"
        ]
    );
    assert_read_only(&source);
}

#[test]
fn local_job_errors_precede_remote_job_errors_and_queue_errors() {
    let source = FakeSource::new();
    let duplicate = job(job_id(90), "mini-1", DashboardJobState::Accepted, 1, 1);
    source.set_local_jobs(Ok(vec![duplicate.clone(), duplicate]));
    source.set_remote(vec![Err(error("REMOTE_FAILED", "remote status failed"))]);
    source.set_queue(Err(error("QUEUE_FAILED", "queue failed")));

    let snapshot = service(source).snapshot(Default::default()).unwrap();

    assert_eq!(
        error_codes(&snapshot),
        vec!["DUPLICATE_LOCAL_JOB", "REMOTE_FAILED", "QUEUE_FAILED"]
    );
}

#[test]
fn remote_job_authority_lease_inconsistency_and_no_fabricated_job_are_exact() {
    let source = FakeSource::new();
    let remote_id = job_id(10);
    let terminal_lease_id = job_id(20);
    let lease_only_id = job_id(30);
    source.set_configured(Ok(vec!["mini-1".into(), "mini-2".into()]));
    let mut first_worker = observation("mini-1", 100, "mini-1.local");
    first_worker.worker.slot = busy_slot(terminal_lease_id);
    let mut second_worker = observation("mini-2", 100, "mini-2.local");
    second_worker.worker.slot = busy_slot(lease_only_id);
    source.set_workers(vec![
        WorkerObservationResult::Current(first_worker),
        WorkerObservationResult::Current(second_worker),
    ]);
    source.set_local_jobs(Ok(vec![
        job(remote_id, "mini-1", DashboardJobState::Accepted, 30, 40),
        job(
            terminal_lease_id,
            "mini-1",
            DashboardJobState::Succeeded,
            10,
            50,
        ),
    ]));
    source.set_remote(vec![Ok(job(
        remote_id,
        "mini-1",
        DashboardJobState::Running,
        30,
        60,
    ))]);

    let snapshot = service(source.clone())
        .snapshot(Default::default())
        .unwrap();

    assert_eq!(snapshot.active_jobs.len(), 1);
    assert_eq!(snapshot.active_jobs[0].job_id, remote_id);
    assert_eq!(snapshot.active_jobs[0].state, DashboardJobState::Running);
    assert_eq!(snapshot.recent_jobs.len(), 1);
    assert_eq!(snapshot.recent_jobs[0].job_id, terminal_lease_id);
    assert!(
        snapshot
            .active_jobs
            .iter()
            .chain(&snapshot.recent_jobs)
            .all(|job| job.job_id != lease_only_id)
    );
    assert_eq!(
        snapshot.workers[0].slot.active_job_id,
        Some(terminal_lease_id)
    );
    assert_eq!(snapshot.workers[1].slot.active_job_id, Some(lease_only_id));
    assert!(error_codes(&snapshot).contains(&"TERMINAL_JOB_LEASE_INCONSISTENCY"));
    assert_read_only(&source);
}

#[test]
fn same_authority_duplicates_and_remote_quarantine_are_permutation_independent() {
    let local_duplicate_id = job_id(40);
    let remote_duplicate_id = job_id(50);
    let unique_id = job_id(60);
    let local_rows = vec![
        job(
            local_duplicate_id,
            "mini-1",
            DashboardJobState::Accepted,
            1,
            1,
        ),
        job(
            local_duplicate_id,
            "mini-2",
            DashboardJobState::Running,
            2,
            2,
        ),
        job(
            remote_duplicate_id,
            "mini-1",
            DashboardJobState::Accepted,
            3,
            3,
        ),
        job(unique_id, "mini-1", DashboardJobState::Uploading, 4, 4),
    ];
    let remote_rows = vec![
        Ok(job(
            remote_duplicate_id,
            "mini-1",
            DashboardJobState::Running,
            3,
            5,
        )),
        Ok(job(
            remote_duplicate_id,
            "mini-2",
            DashboardJobState::Accepted,
            3,
            6,
        )),
    ];

    let first = job_duplicate_snapshot(local_rows.clone(), remote_rows.clone());
    let second = job_duplicate_snapshot(
        local_rows.into_iter().rev().collect(),
        remote_rows.into_iter().rev().collect(),
    );

    assert_eq!(job_ids(&first.active_jobs), vec![unique_id]);
    assert_eq!(first.active_jobs, second.active_jobs);
    assert_eq!(first.recent_jobs, second.recent_jobs);
    assert_eq!(error_codes(&first), error_codes(&second));
    assert_eq!(
        error_codes(&first)
            .into_iter()
            .filter(|code| *code == "DUPLICATE_LOCAL_JOB")
            .count(),
        1
    );
    assert_eq!(
        error_codes(&first)
            .into_iter()
            .filter(|code| *code == "DUPLICATE_REMOTE_JOB")
            .count(),
        1
    );
}

#[test]
fn jobs_partition_sort_and_cap_are_stable_under_permutation() {
    let mut jobs = vec![
        job(job_id(3), "mini", DashboardJobState::Running, 10, 30),
        job(job_id(1), "mini", DashboardJobState::Uploading, 10, 10),
        job(job_id(2), "mini", DashboardJobState::Accepted, 9, 20),
    ];
    for index in 100..=201 {
        jobs.push(job(
            job_id(index),
            "mini",
            match index % 5 {
                0 => DashboardJobState::Succeeded,
                1 => DashboardJobState::Failed,
                2 => DashboardJobState::Cancelled,
                3 => DashboardJobState::TimedOut,
                _ => DashboardJobState::Lost,
            },
            index as u64,
            if index % 2 == 0 { 500 } else { 499 },
        ));
    }
    let first = jobs_snapshot(jobs.clone());
    jobs.reverse();
    let second = jobs_snapshot(jobs);

    assert_eq!(
        job_ids(&first.active_jobs),
        vec![job_id(2), job_id(1), job_id(3)]
    );
    assert_eq!(first.active_jobs, second.active_jobs);
    assert_eq!(first.recent_jobs, second.recent_jobs);
    assert_eq!(first.recent_jobs.len(), MAX_RECENT_TERMINAL_JOBS);
    assert!(
        first
            .recent_jobs
            .windows(2)
            .all(|pair| recent_key(&pair[0]) <= recent_key(&pair[1]))
    );
}

#[test]
fn one_absolute_budget_is_recomputed_and_zero_budget_skips_remote_stages() {
    let source = FakeSource::new();
    let monotonic = ManualMonotonic::new(1_000);
    source.attach_monotonic(monotonic.clone());
    source.set_worker_advance(7_000);
    let deadlines =
        DashboardDeadlines::new(Duration::from_secs(15), Duration::from_secs(20)).unwrap();
    let service = DashboardService::with_deadlines(
        source.clone(),
        ManualClock::new(10_000),
        monotonic.clone(),
        deadlines,
    );

    service.snapshot(Default::default()).unwrap();
    assert_eq!(source.worker_budgets(), vec![Duration::from_secs(15)]);
    assert_eq!(source.remote_budgets(), vec![Duration::from_secs(13)]);

    source.clear_budgets();
    source.set_worker_advance(20_000);
    let timed = service.snapshot(Default::default()).unwrap();
    assert_eq!(source.worker_budgets(), vec![Duration::from_secs(15)]);
    assert!(source.remote_budgets().is_empty());
    assert!(error_codes(&timed).contains(&"ACTIVE_JOB_COLLECTION_DEADLINE_EXCEEDED"));
    assert_read_only(&source);
}

#[test]
fn revisions_and_wall_generation_time_advance_only_for_completed_leaders() {
    let source = FakeSource::new();
    let wall = ManualClock::new(100);
    source.set_finish_wall(wall.clone(), 200);
    let service = DashboardService::new(source.clone(), wall.clone(), ManualMonotonic::new(0));

    let first = service.snapshot(Default::default()).unwrap();
    assert_eq!((first.revision, first.generated_at_millis), (1, 200));
    source.set_finish_wall(wall.clone(), 300);
    let second = service.snapshot(Default::default()).unwrap();
    assert_eq!((second.revision, second.generated_at_millis), (2, 300));
}

#[test]
fn overlapping_calls_coalesce_exactly_and_later_calls_start_new_generations() {
    let source = FakeSource::new();
    let gate = Arc::new(Gate::default());
    source.set_collect_gate(Some(Arc::clone(&gate)));
    let monotonic = ManualMonotonic::new(0);
    let service = Arc::new(DashboardService::new(
        source.clone(),
        ManualClock::new(100),
        monotonic.clone(),
    ));

    let leader = spawn_snapshot(Arc::clone(&service));
    gate.wait_until_entered();
    let waiter = spawn_snapshot(Arc::clone(&service));
    monotonic.wait_for_calls(3);
    gate.release();

    let first = leader.join().unwrap().unwrap();
    let coalesced = waiter.join().unwrap().unwrap();
    assert_eq!(coalesced, first);
    assert_eq!(source.worker_call_count(), 1);

    source.set_collect_gate(None);
    let next = service.snapshot(Default::default()).unwrap();
    assert_eq!(next.revision, first.revision + 1);
    assert_eq!(source.worker_call_count(), 2);
}

#[test]
fn a_waiter_stays_bound_to_its_generation_while_the_next_generation_runs() {
    let source = FakeSource::new();
    let first_gate = Arc::new(Gate::default());
    source.set_collect_gate(Some(Arc::clone(&first_gate)));
    let monotonic = ManualMonotonic::new(0);
    let service = Arc::new(DashboardService::new(
        source.clone(),
        ManualClock::new(100),
        monotonic.clone(),
    ));

    let first_leader = spawn_snapshot(Arc::clone(&service));
    first_gate.wait_until_entered();
    let first_waiter = spawn_snapshot(Arc::clone(&service));
    monotonic.wait_for_calls(3);
    first_gate.release();
    let generation_one = first_leader.join().unwrap().unwrap();

    let second_gate = Arc::new(Gate::default());
    source.set_collect_gate(Some(Arc::clone(&second_gate)));
    let second_leader = spawn_snapshot(Arc::clone(&service));
    second_gate.wait_until_entered();
    let waiter_result = first_waiter.join().unwrap().unwrap();
    assert_eq!(waiter_result.revision, generation_one.revision);
    second_gate.release();
    let generation_two = second_leader.join().unwrap().unwrap();
    assert_eq!(generation_two.revision, generation_one.revision + 1);
}

#[test]
fn waiter_timeout_without_prior_snapshot_returns_fixed_offline_fallback() {
    let source = FakeSource::new();
    let gate = Arc::new(Gate::default());
    source.set_collect_gate(Some(Arc::clone(&gate)));
    let monotonic = ManualMonotonic::new(0);
    monotonic.set_script([0, 0, 0, 2]);
    let deadlines =
        DashboardDeadlines::new(Duration::from_millis(1), Duration::from_millis(1)).unwrap();
    let service = Arc::new(DashboardService::with_deadlines(
        source.clone(),
        ManualClock::new(777),
        monotonic,
        deadlines,
    ));

    let leader = spawn_snapshot(Arc::clone(&service));
    gate.wait_until_entered();
    let fallback = service.snapshot(Default::default()).unwrap();
    assert_eq!(fallback, empty_timeout_snapshot(777));
    gate.release();
    leader.join().unwrap().unwrap();
}

#[test]
fn waiter_timeout_with_prior_snapshot_preserves_data_revision_and_generation_time() {
    let source = FakeSource::new();
    source.set_workers(vec![WorkerObservationResult::Current(
        project_worker(&worker_with_facts(u64::MAX - 1, 0), 500).unwrap(),
    )]);
    let monotonic = ManualMonotonic::new(0);
    let deadlines =
        DashboardDeadlines::new(Duration::from_millis(1), Duration::from_millis(1)).unwrap();
    let service = Arc::new(DashboardService::with_deadlines(
        source.clone(),
        ManualClock::new(500),
        monotonic.clone(),
        deadlines,
    ));
    monotonic.set_script([0, 0, 0]);
    let completed = service.snapshot(Default::default()).unwrap();

    let gate = Arc::new(Gate::default());
    source.set_collect_gate(Some(Arc::clone(&gate)));
    monotonic.set_script([0, 0, 0, 2]);
    let leader = spawn_snapshot(Arc::clone(&service));
    gate.wait_until_entered();
    let stale = service.snapshot(Default::default()).unwrap();
    assert_eq!(stale.revision, completed.revision);
    assert_eq!(stale.generated_at_millis, completed.generated_at_millis);
    assert_eq!(completed.workers[0].freshness, Freshness::Current);
    assert_eq!(stale.workers[0].freshness, Freshness::Stale);
    assert_eq!(
        stale.workers[0].agent_facts.as_ref().unwrap().freshness,
        mac_worker::dashboard::model::AgentFactsFreshness::Current,
        "worker connectivity and agent-facts age remain independent"
    );
    assert_eq!(stale.active_jobs, completed.active_jobs);
    assert_eq!(stale.recent_jobs, completed.recent_jobs);
    assert_eq!(stale.queue, completed.queue);
    assert_eq!(stale.collection.freshness, Freshness::Stale);
    assert_eq!(
        stale.collection.errors.last().unwrap().code,
        "DASHBOARD_REFRESH_TIMEOUT"
    );
    gate.release();
    leader.join().unwrap().unwrap();

    source.set_collect_gate(None);
    monotonic.set_script([0, 0, 0]);
    let next = service.snapshot(Default::default()).unwrap();
    assert_eq!(next.revision, completed.revision + 2);
}

#[test]
fn leader_panic_completes_the_flight_wakes_waiters_and_allows_later_progress() {
    let source = FakeSource::new();
    let gate = Arc::new(Gate::default());
    source.set_collect_gate(Some(Arc::clone(&gate)));
    source.panic_collect_once();
    let monotonic = ManualMonotonic::new(0);
    let service = Arc::new(DashboardService::new(
        source.clone(),
        ManualClock::new(100),
        monotonic.clone(),
    ));
    let leader = spawn_snapshot(Arc::clone(&service));
    gate.wait_until_entered();
    let waiter = spawn_snapshot(Arc::clone(&service));
    monotonic.wait_for_calls(3);
    gate.release();

    assert_eq!(
        leader.join().unwrap().unwrap_err().code,
        "DASHBOARD_REFRESH_ABORTED"
    );
    assert_eq!(
        waiter.join().unwrap().unwrap_err().code,
        "DASHBOARD_REFRESH_ABORTED"
    );

    source.set_collect_gate(None);
    let recovered = service.snapshot(Default::default()).unwrap();
    assert_eq!(recovered.revision, 1);
    assert_read_only(&source);
}

#[test]
fn panicking_monotonic_completes_installed_flight_and_allows_a_later_leader() {
    let source = FakeSource::new();
    let monotonic = PanickingFirstMonotonic::new();
    let deadlines =
        DashboardDeadlines::new(Duration::from_millis(100), Duration::from_millis(100)).unwrap();
    let service = Arc::new(DashboardService::with_deadlines(
        source.clone(),
        ManualClock::new(100),
        monotonic.clone(),
        deadlines,
    ));

    let leader = spawn_snapshot(Arc::clone(&service));
    monotonic.first_call.wait_until_entered();
    let waiter = spawn_snapshot(Arc::clone(&service));
    monotonic.waiter_remaining.wait_until_entered();
    monotonic.first_call.release();
    monotonic.waiter_remaining.release();

    let leader_result = leader.join();
    let waiter_result = waiter.join().unwrap();
    assert!(leader_result.is_ok(), "the leader panic escaped snapshot()");
    assert_eq!(
        leader_result.unwrap().unwrap_err().code,
        "DASHBOARD_REFRESH_ABORTED"
    );
    assert_eq!(waiter_result.unwrap_err().code, "DASHBOARD_REFRESH_ABORTED");

    let recovered = service.snapshot(Default::default()).unwrap();
    assert_eq!(recovered.revision, 1);
    assert_eq!(source.worker_call_count(), 0);
    assert_read_only(&source);
}

#[test]
fn read_snapshot_is_pending_until_the_first_completed_collection() {
    let source = FakeSource::new();
    let service = DashboardService::new(
        source.clone(),
        ManualClock::new(100),
        ManualMonotonic::new(0),
    );
    let error = service.read_snapshot().unwrap_err();
    assert_eq!(error.code, SNAPSHOT_PENDING);
    assert_eq!(source.worker_call_count(), 0);
}

#[test]
fn background_collection_proceeds_without_http_and_reads_do_not_probe() {
    let source = FakeSource::new();
    let service = Arc::new(
        DashboardService::new(
            source.clone(),
            ManualClock::new(100),
            ManualMonotonic::new(0),
        )
        .with_collection_interval(Duration::from_secs(60)),
    );
    let collector = service.start_background_collection().unwrap();
    let first = wait_for_read(&service, Duration::from_secs(2));
    assert_eq!(first.collection.freshness, Freshness::Current);
    assert_eq!(first.generated_at_millis, 100);
    let probes = source.worker_call_count();
    assert!(probes >= 1, "collector must collect without HTTP {probes}");
    let _ = service.read_snapshot().unwrap();
    let _ = service.read_snapshot().unwrap();
    assert_eq!(source.worker_call_count(), probes);
    collector.join();
    assert_read_only(&source);
}

#[test]
fn slow_collection_does_not_stall_a_ready_read() {
    let source = FakeSource::new();
    let service = Arc::new(
        DashboardService::new(
            source.clone(),
            ManualClock::new(100),
            ManualMonotonic::new(0),
        )
        .with_collection_interval(Duration::from_secs(60)),
    );
    let first = service.snapshot(Default::default()).unwrap();
    let gate = Arc::new(Gate::default());
    source.set_collect_gate(Some(Arc::clone(&gate)));
    let collector = service.start_background_collection().unwrap();
    gate.wait_until_entered();
    let started = Instant::now();
    let ready = service.read_snapshot().unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "ready read waited {:?}",
        started.elapsed()
    );
    assert_eq!(ready.revision, first.revision);
    assert_eq!(ready.generated_at_millis, first.generated_at_millis);
    assert_eq!(ready.collection.freshness, Freshness::Current);
    gate.release();
    collector.join();
}

#[test]
fn collector_stop_and_restart_does_not_overlap_collectors() {
    let source = FakeSource::new();
    let service = Arc::new(
        DashboardService::new(
            source.clone(),
            ManualClock::new(100),
            ManualMonotonic::new(0),
        )
        .with_collection_interval(Duration::from_secs(60)),
    );
    let first = service.start_background_collection().unwrap();
    let first_snapshot = wait_for_read(&service, Duration::from_secs(2));
    let after_first = source.worker_call_count();
    assert!(after_first >= 1);
    let first_revision = first_snapshot.revision;
    first.join();
    thread::sleep(Duration::from_millis(50));
    assert_eq!(source.worker_call_count(), after_first);
    let second = service.start_background_collection().unwrap();
    let restarted = wait_for_revision(&service, first_revision, Duration::from_secs(2));
    assert!(
        restarted.revision > first_revision,
        "restarted collector reused revision {} after {first_revision}",
        restarted.revision
    );
    assert!(
        source.worker_call_count() > after_first,
        "restarted collector never collected"
    );
    second.join();
    assert_read_only(&source);
}

#[test]
fn collector_stop_without_join_does_not_leave_a_recurring_orphan() {
    let source = FakeSource::new();
    let gate = Arc::new(Gate::default());
    source.set_collect_gate(Some(Arc::clone(&gate)));
    let service = Arc::new(
        DashboardService::new(
            source.clone(),
            ManualClock::new(100),
            ManualMonotonic::new(0),
        )
        .with_collection_interval(Duration::from_millis(20)),
    );
    let first = service.start_background_collection().unwrap();
    gate.wait_until_entered();
    assert_eq!(source.worker_call_count(), 1);
    first.stop();
    thread::sleep(Duration::from_millis(40));
    assert_eq!(source.worker_call_count(), 1);
    gate.release();
    let published = wait_for_read(&service, Duration::from_secs(2));
    thread::sleep(Duration::from_millis(120));
    assert_eq!(
        source.worker_call_count(),
        1,
        "stopped collector kept collecting after the in-flight snapshot finished"
    );

    let second = service.start_background_collection().unwrap();
    let restarted = wait_for_revision(&service, published.revision, Duration::from_secs(2));
    assert!(restarted.revision > published.revision);
    assert!(source.worker_call_count() >= 2);
    second.join();
    assert_read_only(&source);
}

#[test]
fn collector_drop_stops_future_collection() {
    let source = FakeSource::new();
    let service = Arc::new(
        DashboardService::new(
            source.clone(),
            ManualClock::new(100),
            ManualMonotonic::new(0),
        )
        .with_collection_interval(Duration::from_millis(20)),
    );
    let collector = service.start_background_collection().unwrap();
    let _ = wait_for_read(&service, Duration::from_secs(2));
    let after_start = source.worker_call_count();
    drop(collector);
    thread::sleep(Duration::from_millis(120));
    assert_eq!(source.worker_call_count(), after_start);
    assert_read_only(&source);
}

#[test]
fn failed_collection_keeps_last_good_stale_and_original_generation_time() {
    let source = FakeSource::new();
    source.set_workers(vec![WorkerObservationResult::Current(observation(
        "mini-1",
        500,
        "accepted.local",
    ))]);
    let wall = ManualClock::new(500);
    let service = Arc::new(
        DashboardService::new(source.clone(), wall.clone(), ManualMonotonic::new(0))
            .with_collection_interval(Duration::from_secs(60)),
    );
    let completed = service.snapshot(Default::default()).unwrap();
    assert_eq!(completed.generated_at_millis, 500);
    assert_eq!(completed.collection.freshness, Freshness::Current);

    source.panic_collect_once();
    assert_eq!(
        service.snapshot(Default::default()).unwrap_err().code,
        "DASHBOARD_REFRESH_ABORTED"
    );
    wall.set(800);
    let stale = service.read_snapshot().unwrap();
    assert_eq!(stale.revision, completed.revision);
    assert_eq!(stale.generated_at_millis, completed.generated_at_millis);
    assert_eq!(stale.collection.freshness, Freshness::Stale);
    assert_eq!(stale.workers[0].hostname.as_deref(), Some("accepted.local"));
    assert!(
        error_codes(&stale).contains(&"DASHBOARD_REFRESH_ABORTED"),
        "{:?}",
        error_codes(&stale)
    );
}

fn wait_for_read<S, C, M>(
    service: &DashboardService<S, C, M>,
    timeout: Duration,
) -> DashboardSnapshot
where
    S: DashboardDataSource,
    C: Clock,
    M: MonotonicClock,
{
    let deadline = Instant::now() + timeout;
    loop {
        match service.read_snapshot() {
            Ok(snapshot) => return snapshot,
            Err(error) if error.code == SNAPSHOT_PENDING && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("read_snapshot failed: {} {}", error.code, error.message),
        }
    }
}

fn wait_for_revision<S, C, M>(
    service: &DashboardService<S, C, M>,
    min_revision: u64,
    timeout: Duration,
) -> DashboardSnapshot
where
    S: DashboardDataSource,
    C: Clock,
    M: MonotonicClock,
{
    let deadline = Instant::now() + timeout;
    loop {
        match service.read_snapshot() {
            Ok(snapshot) if snapshot.revision > min_revision => return snapshot,
            Ok(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
            Err(error) if error.code == SNAPSHOT_PENDING && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Ok(snapshot) => panic!(
                "collector snapshot stayed at revision {} (wanted > {min_revision})",
                snapshot.revision
            ),
            Err(error) => panic!("read_snapshot failed: {} {}", error.code, error.message),
        }
    }
}

#[test]
fn a_panicking_completion_clock_does_not_consume_a_revision() {
    let source = FakeSource::new();
    let clock = PanicOnceClock::new(900);
    let service = DashboardService::new(source, clock, ManualMonotonic::new(0));

    assert_eq!(
        service.snapshot(Default::default()).unwrap_err().code,
        "DASHBOARD_REFRESH_ABORTED"
    );
    let recovered = service.snapshot(Default::default()).unwrap();
    assert_eq!(recovered.revision, 1);
    assert_eq!(recovered.generated_at_millis, 900);
}

#[test]
fn stale_worker_triggers_exactly_one_refresh_per_ttl_across_repeated_collections() {
    let source = FakeSource::new();
    source.set_workers(vec![WorkerObservationResult::Current(
        stale_facts_observation(),
    )]);
    let wall = ManualClock::new(10_000);
    let service = DashboardService::new(source.clone(), wall.clone(), ManualMonotonic::new(0));

    let snapshot = service.snapshot(Default::default()).unwrap();
    assert_eq!(snapshot.workers[0].name, "mini-1");
    source.wait_for_refresh_calls(1);

    let _ = service.snapshot(Default::default()).unwrap();
    thread::sleep(Duration::from_millis(50));
    assert_eq!(source.refresh_calls(), vec!["mini-1"]);

    wall.set(10_000 + FACTS_TTL + 1);
    let _ = service.snapshot(Default::default()).unwrap();
    source.wait_for_refresh_calls(2);
    assert_eq!(source.refresh_calls(), vec!["mini-1", "mini-1"]);
}

#[test]
fn a_failed_stale_facts_refresh_is_not_retried_before_the_ttl() {
    let source = FakeSource::new();
    source.set_workers(vec![WorkerObservationResult::Current(
        stale_facts_observation(),
    )]);
    source.set_refresh_fail(true);
    let service = service(source.clone());

    let _ = service.snapshot(Default::default()).unwrap();
    source.wait_for_refresh_calls(1);
    let _ = service.snapshot(Default::default()).unwrap();
    thread::sleep(Duration::from_millis(50));
    assert_eq!(source.refresh_calls(), vec!["mini-1"]);
}

#[test]
fn stale_facts_refresh_does_not_block_or_shrink_the_snapshot() {
    let source = FakeSource::new();
    source.set_workers(vec![WorkerObservationResult::Current(
        stale_facts_observation(),
    )]);
    let gate = Arc::new(Gate::default());
    source.set_refresh_gate(Some(Arc::clone(&gate)));
    let service = Arc::new(service(source.clone()));

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let snapshot_service = Arc::clone(&service);
    thread::spawn(move || {
        done_tx
            .send(snapshot_service.snapshot(Default::default()))
            .ok();
    });
    gate.wait_until_entered();
    let snapshot = done_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("snapshot must not wait for the facts refresh")
        .unwrap();
    assert_eq!(snapshot.workers[0].name, "mini-1");
    assert_eq!(source.refresh_calls(), vec!["mini-1"]);
    gate.release();
}

#[test]
fn no_facts_refresh_flag_disables_stale_facts_refresh() {
    let source = FakeSource::new();
    source.set_workers(vec![WorkerObservationResult::Current(
        stale_facts_observation(),
    )]);
    let service = DashboardService::new(
        source.clone(),
        ManualClock::new(10_000),
        ManualMonotonic::new(0),
    )
    .with_refresh_stale_facts(false);

    let _ = service.snapshot(Default::default()).unwrap();
    thread::sleep(Duration::from_millis(50));
    assert!(source.refresh_calls().is_empty());
}

#[test]
fn unreachable_workers_are_not_refreshed() {
    let source = FakeSource::new();
    source.set_workers(vec![WorkerObservationResult::Failed {
        worker_name: "mini-1".into(),
        error: error("SSH_UNAVAILABLE", "worker is unreachable"),
    }]);
    let service = service(source.clone());

    let _ = service.snapshot(Default::default()).unwrap();
    thread::sleep(Duration::from_millis(50));
    assert!(source.refresh_calls().is_empty());
}

#[test]
fn snapshot_sets_laptop_binary_outdated_when_the_installed_file_changed() {
    let source = FakeSource::new();
    let started = mac_worker::laptop::BinaryIdentity {
        path: "/tmp/worker".into(),
        inode: 1,
        size: 10,
        mtime_millis: 100,
    };
    let mut installed = started.clone();
    installed.inode = 2;
    let service = DashboardService::new(source, ManualClock::new(10_000), ManualMonotonic::new(0))
        .with_binary_source(std::sync::Arc::new(
            mac_worker::laptop::FixedBinaryIdentitySource {
                started: Some(started),
                installed: Some(installed),
            },
        ));
    let snapshot = service.snapshot(Default::default()).unwrap();
    assert_eq!(
        snapshot.laptop,
        Some(mac_worker::dashboard::model::DashboardLaptop {
            binary_outdated: true
        })
    );
    let json = serde_json::to_value(&snapshot).unwrap();
    assert_eq!(json["laptop"]["binary_outdated"], true);
}

#[test]
fn matching_laptop_binary_omits_the_additive_laptop_object() {
    let identity = mac_worker::laptop::BinaryIdentity {
        path: "/tmp/worker".into(),
        inode: 1,
        size: 10,
        mtime_millis: 100,
    };
    let service = DashboardService::new(
        FakeSource::new(),
        ManualClock::new(10_000),
        ManualMonotonic::new(0),
    )
    .with_binary_source(std::sync::Arc::new(
        mac_worker::laptop::FixedBinaryIdentitySource {
            started: Some(identity.clone()),
            installed: Some(identity),
        },
    ));
    let snapshot = service.snapshot(Default::default()).unwrap();
    assert_eq!(snapshot.laptop, None);
    let json = serde_json::to_value(&snapshot).unwrap();
    assert!(json.get("laptop").is_none());
}

fn stale_facts_observation() -> Observation {
    project_worker(&worker_with_facts(1, FACTS_TTL + 1), 10_000).unwrap()
}

fn service(source: FakeSource) -> DashboardService<FakeSource, ManualClock, ManualMonotonic> {
    DashboardService::new(source, ManualClock::new(10_000), ManualMonotonic::new(0))
}

fn spawn_snapshot<S, C, M>(
    service: Arc<DashboardService<S, C, M>>,
) -> thread::JoinHandle<Result<DashboardSnapshot, DashboardError>>
where
    S: DashboardDataSource,
    C: Clock,
    M: MonotonicClock,
{
    thread::spawn(move || service.snapshot(DashboardSnapshotRequest))
}

#[derive(Clone)]
struct ManualClock(Arc<AtomicU64>);

impl ManualClock {
    fn new(now: u64) -> Self {
        Self(Arc::new(AtomicU64::new(now)))
    }

    fn set(&self, now: u64) {
        self.0.store(now, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

struct PanicOnceClock {
    panics: AtomicBool,
    now: u64,
}

impl PanicOnceClock {
    fn new(now: u64) -> Self {
        Self {
            panics: AtomicBool::new(true),
            now,
        }
    }
}

impl Clock for PanicOnceClock {
    fn now_millis(&self) -> u64 {
        if self.panics.swap(false, Ordering::SeqCst) {
            panic!("intentional completion clock panic");
        }
        self.now
    }
}

#[derive(Clone)]
struct PanickingFirstMonotonic {
    calls: Arc<AtomicUsize>,
    first_call: Arc<Gate>,
    waiter_remaining: Arc<Gate>,
}

impl PanickingFirstMonotonic {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            first_call: Arc::new(Gate::default()),
            waiter_remaining: Arc::new(Gate::default()),
        }
    }
}

impl MonotonicClock for PanickingFirstMonotonic {
    fn now_millis(&self) -> u64 {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        match call {
            1 => {
                self.first_call.enter_and_wait();
                panic!("intentional first monotonic clock panic");
            }
            2 => 0,
            3 => {
                self.waiter_remaining.enter_and_wait();
                0
            }
            _ => u64::try_from(call - 3).unwrap() * 200,
        }
    }
}

#[derive(Clone)]
struct ManualMonotonic(Arc<ManualMonotonicState>);

struct ManualMonotonicState {
    value: AtomicU64,
    calls: Mutex<usize>,
    script: Mutex<VecDeque<u64>>,
    changed: Condvar,
}

impl ManualMonotonic {
    fn new(now: u64) -> Self {
        Self(Arc::new(ManualMonotonicState {
            value: AtomicU64::new(now),
            calls: Mutex::new(0),
            script: Mutex::new(VecDeque::new()),
            changed: Condvar::new(),
        }))
    }

    fn advance(&self, millis: u64) {
        self.0.value.fetch_add(millis, Ordering::SeqCst);
    }

    fn set_script(&self, values: impl IntoIterator<Item = u64>) {
        *lock(&self.0.script) = values.into_iter().collect();
    }

    fn wait_for_calls(&self, target: usize) {
        let mut calls = lock(&self.0.calls);
        while *calls < target {
            calls = wait(&self.0.changed, calls);
        }
    }
}

impl MonotonicClock for ManualMonotonic {
    fn now_millis(&self) -> u64 {
        let mut calls = lock(&self.0.calls);
        *calls += 1;
        self.0.changed.notify_all();
        drop(calls);
        if let Some(value) = lock(&self.0.script).pop_front() {
            self.0.value.store(value, Ordering::SeqCst);
            value
        } else {
            self.0.value.load(Ordering::SeqCst)
        }
    }
}

#[derive(Default)]
struct Gate {
    state: Mutex<GateState>,
    changed: Condvar,
}

#[derive(Default)]
struct GateState {
    entered: bool,
    released: bool,
}

impl Gate {
    fn enter_and_wait(&self) {
        let mut state = lock(&self.state);
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            state = wait(&self.changed, state);
        }
    }

    fn wait_until_entered(&self) {
        let mut state = lock(&self.state);
        while !state.entered {
            state = wait(&self.changed, state);
        }
    }

    fn release(&self) {
        let mut state = lock(&self.state);
        state.released = true;
        self.changed.notify_all();
    }
}

#[derive(Clone)]
struct FakeSource(Arc<FakeSourceState>);

struct FakeSourceState {
    configured: Mutex<Result<Vec<String>, DashboardError>>,
    slots: Mutex<HashMap<String, u8>>,
    workers: Mutex<Vec<WorkerObservationResult>>,
    local_jobs: Mutex<Result<Vec<DashboardJob>, DashboardError>>,
    remote: Mutex<Vec<Result<DashboardJob, DashboardError>>>,
    queue: Mutex<Result<Vec<DashboardQueueEntry>, DashboardError>>,
    worker_budgets: Mutex<Vec<Duration>>,
    remote_budgets: Mutex<Vec<Duration>>,
    worker_calls: AtomicUsize,
    remote_calls: AtomicUsize,
    queue_calls: AtomicUsize,
    mutation_calls: AtomicUsize,
    collect_gate: Mutex<Option<Arc<Gate>>>,
    panic_collect_once: AtomicBool,
    monotonic: Mutex<Option<ManualMonotonic>>,
    worker_advance: AtomicU64,
    finish_wall: Mutex<Option<(ManualClock, u64)>>,
    refresh_calls: Mutex<Vec<String>>,
    refresh_calls_changed: Condvar,
    refresh_fail: AtomicBool,
    refresh_gate: Mutex<Option<Arc<Gate>>>,
}

impl FakeSource {
    fn new() -> Self {
        Self(Arc::new(FakeSourceState {
            configured: Mutex::new(Ok(vec!["mini-1".into()])),
            slots: Mutex::new(HashMap::new()),
            workers: Mutex::new(vec![WorkerObservationResult::Current(observation(
                "mini-1",
                100,
                "mini-1.local",
            ))]),
            local_jobs: Mutex::new(Ok(Vec::new())),
            remote: Mutex::new(Vec::new()),
            queue: Mutex::new(Ok(Vec::new())),
            worker_budgets: Mutex::new(Vec::new()),
            remote_budgets: Mutex::new(Vec::new()),
            worker_calls: AtomicUsize::new(0),
            remote_calls: AtomicUsize::new(0),
            queue_calls: AtomicUsize::new(0),
            mutation_calls: AtomicUsize::new(0),
            collect_gate: Mutex::new(None),
            panic_collect_once: AtomicBool::new(false),
            monotonic: Mutex::new(None),
            worker_advance: AtomicU64::new(0),
            finish_wall: Mutex::new(None),
            refresh_calls: Mutex::new(Vec::new()),
            refresh_calls_changed: Condvar::new(),
            refresh_fail: AtomicBool::new(false),
            refresh_gate: Mutex::new(None),
        }))
    }

    fn set_configured(&self, configured: Result<Vec<String>, DashboardError>) {
        *lock(&self.0.configured) = configured;
    }

    fn set_slots(&self, worker_name: &str, slots: u8) {
        lock(&self.0.slots).insert(worker_name.to_owned(), slots);
    }

    fn set_workers(&self, workers: Vec<WorkerObservationResult>) {
        *lock(&self.0.workers) = workers;
    }

    fn set_local_jobs(&self, jobs: Result<Vec<DashboardJob>, DashboardError>) {
        *lock(&self.0.local_jobs) = jobs;
    }

    fn set_remote(&self, jobs: Vec<Result<DashboardJob, DashboardError>>) {
        *lock(&self.0.remote) = jobs;
    }

    fn set_queue(&self, queue: Result<Vec<DashboardQueueEntry>, DashboardError>) {
        *lock(&self.0.queue) = queue;
    }

    fn set_collect_gate(&self, gate: Option<Arc<Gate>>) {
        *lock(&self.0.collect_gate) = gate;
    }

    fn panic_collect_once(&self) {
        self.0.panic_collect_once.store(true, Ordering::SeqCst);
    }

    fn attach_monotonic(&self, monotonic: ManualMonotonic) {
        *lock(&self.0.monotonic) = Some(monotonic);
    }

    fn set_worker_advance(&self, millis: u64) {
        self.0.worker_advance.store(millis, Ordering::SeqCst);
    }

    fn set_finish_wall(&self, clock: ManualClock, now: u64) {
        *lock(&self.0.finish_wall) = Some((clock, now));
    }

    fn worker_budgets(&self) -> Vec<Duration> {
        lock(&self.0.worker_budgets).clone()
    }

    fn remote_budgets(&self) -> Vec<Duration> {
        lock(&self.0.remote_budgets).clone()
    }

    fn clear_budgets(&self) {
        lock(&self.0.worker_budgets).clear();
        lock(&self.0.remote_budgets).clear();
    }

    fn worker_call_count(&self) -> usize {
        self.0.worker_calls.load(Ordering::SeqCst)
    }

    fn set_refresh_fail(&self, fail: bool) {
        self.0.refresh_fail.store(fail, Ordering::SeqCst);
    }

    fn set_refresh_gate(&self, gate: Option<Arc<Gate>>) {
        *lock(&self.0.refresh_gate) = gate;
    }

    fn refresh_calls(&self) -> Vec<String> {
        lock(&self.0.refresh_calls).clone()
    }

    fn wait_for_refresh_calls(&self, target: usize) {
        let mut calls = lock(&self.0.refresh_calls);
        while calls.len() < target {
            calls = wait(&self.0.refresh_calls_changed, calls);
        }
    }
}

impl DashboardDataSource for FakeSource {
    fn configured_workers(&self) -> Result<Vec<String>, DashboardError> {
        lock(&self.0.configured).clone()
    }

    fn configured_worker_slots(&self, worker_name: &str) -> u8 {
        lock(&self.0.slots).get(worker_name).copied().unwrap_or(1)
    }

    fn collect_workers(&self, deadline: Duration) -> Vec<WorkerObservationResult> {
        self.0.worker_calls.fetch_add(1, Ordering::SeqCst);
        lock(&self.0.worker_budgets).push(deadline);
        let gate = lock(&self.0.collect_gate).clone();
        if let Some(gate) = gate {
            gate.enter_and_wait();
        }
        if self.0.panic_collect_once.swap(false, Ordering::SeqCst) {
            panic!("intentional fake source panic");
        }
        if let Some(monotonic) = lock(&self.0.monotonic).clone() {
            monotonic.advance(self.0.worker_advance.load(Ordering::SeqCst));
        }
        lock(&self.0.workers).clone()
    }

    fn local_jobs(&self) -> Result<Vec<DashboardJob>, DashboardError> {
        lock(&self.0.local_jobs).clone()
    }

    fn authoritative_active_jobs(
        &self,
        deadline: Duration,
    ) -> Vec<Result<DashboardJob, DashboardError>> {
        self.0.remote_calls.fetch_add(1, Ordering::SeqCst);
        lock(&self.0.remote_budgets).push(deadline);
        lock(&self.0.remote).clone()
    }

    fn queue_entries(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError> {
        self.0.queue_calls.fetch_add(1, Ordering::SeqCst);
        if let Some((clock, now)) = lock(&self.0.finish_wall).clone() {
            clock.set(now);
        }
        lock(&self.0.queue).clone()
    }

    fn refresh_worker_facts(&self, worker_name: &str) -> Result<(), WorkerError> {
        {
            let mut calls = lock(&self.0.refresh_calls);
            calls.push(worker_name.to_owned());
            self.0.refresh_calls_changed.notify_all();
        }
        if let Some(gate) = lock(&self.0.refresh_gate).clone() {
            gate.enter_and_wait();
        }
        if self.0.refresh_fail.load(Ordering::SeqCst) {
            return Err(WorkerError::Unavailable(
                "REFRESH_FACTS_FAILED: worker fact refresh failed".into(),
            ));
        }
        Ok(())
    }
}

fn observation(worker_name: &str, observed_at_millis: u64, hostname: &str) -> Observation {
    Observation {
        worker: DashboardWorker {
            name: worker_name.into(),
            health: WorkerHealth::Ready,
            freshness: Freshness::Current,
            observed_at_millis: Some(observed_at_millis),
            hostname: Some(hostname.into()),
            agent_facts: None,
            herdr: None,
            slot: idle_slot(),
            capabilities: vec!["swift".into()],
            missing_capabilities: Vec::new(),
            system: SystemSummary {
                free_disk_bytes: Some(100),
                total_disk_bytes: Some(200),
                memory_pressure: Some(DashboardMemoryPressure::Normal),
                swap_used_bytes: Some(0),
                cpu_busy_percent: None,
            },
            error: None,
            active_task: None,
        },
        observed_at_millis,
        cpu_counters: None,
    }
}

fn worker_with_facts(collected_at_millis: u64, facts_age_millis: u64) -> ProbeWorkerHealth {
    ProbeWorkerHealth {
        name: "mini-1".into(),
        ssh: "operator@mini-1.internal".into(),
        status: HealthStatus::Ready,
        probe: Some(ProbeResponse {
            protocol_version: PROTOCOL_VERSION,
            supervision_version: SUPERVISION_VERSION,
            hostname: "mini-1.local".into(),
            arch: "arm64".into(),
            os_version: "26.2".into(),
            free_disk_bytes: 100,
            total_disk_bytes: 200,
            memory_pressure: MemoryPressure::Normal,
            swap_used_bytes: None,
            available_memory_bytes: None,
            cpu_counters: None,
            slot_state: SlotState::Idle,
            active_lease: None,
            capabilities: vec!["swift".into()],
            agent_facts: Some(AgentFacts {
                agents: Vec::new(),
                env_profiles: Vec::new(),
                git_identity: false,
                collected_at_millis,
                herdr: None,
                origin_https_helpers: Default::default(),
            }),
            facts_age_millis: Some(facts_age_millis),
            configured_slots: 0,
            busy_slots: 0,
        }),
        missing_capabilities: Vec::new(),
        error_code: None,
        error_message: None,
    }
}

fn observation_with_counters(
    worker_name: &str,
    observed_at_millis: u64,
    hostname: &str,
    total: u64,
    idle: u64,
) -> Observation {
    let mut observation = observation(worker_name, observed_at_millis, hostname);
    observation.cpu_counters = Some(CpuCounters::new(total, idle).unwrap());
    observation
}

fn offline_worker(worker_name: &str, error_code: &str) -> DashboardWorker {
    DashboardWorker {
        name: worker_name.into(),
        health: WorkerHealth::Unavailable,
        freshness: Freshness::Offline,
        observed_at_millis: None,
        hostname: None,
        agent_facts: None,
        herdr: None,
        slot: idle_slot(),
        capabilities: Vec::new(),
        missing_capabilities: Vec::new(),
        system: SystemSummary {
            free_disk_bytes: None,
            total_disk_bytes: None,
            memory_pressure: None,
            swap_used_bytes: None,
            cpu_busy_percent: None,
        },
        error: Some(error(
            error_code,
            "worker observation timestamp was not newer",
        )),
        active_task: None,
    }
}

fn idle_slot() -> SlotSummary {
    SlotSummary::idle(1)
}

fn busy_slot(job_id: JobId) -> SlotSummary {
    SlotSummary {
        state: DashboardSlotState::Busy,
        capacity: 1,
        busy: 1,
        active_job_id: Some(job_id),
        active_job_ids: vec![job_id],
    }
}

fn job(
    job_id: JobId,
    worker_name: &str,
    state: DashboardJobState,
    created_at_millis: u64,
    updated_at_millis: u64,
) -> DashboardJob {
    DashboardJob {
        job_id,
        worker_name: worker_name.into(),
        project_id: format!("project-{job_id}"),
        worktree_id: format!("worktree-{job_id}"),
        project_label: Some(format!("Project {job_id}")),
        manifest_digest: "a".repeat(64),
        command_summary: DashboardCommandSummary {
            mode: DashboardCommandMode::Argv,
            arg_count: Some(2),
        },
        resource_class: "default".into(),
        created_at_millis,
        updated_at_millis,
        state,
        exit_code: None,
        terminating_signal: None,
        final_stdout_bytes: None,
        final_stderr_bytes: None,
        artifact_status: None,
        remote_uncertainty: None,
    }
}

fn queue_entry(position: u32, id: u128) -> DashboardQueueEntry {
    DashboardQueueEntry {
        position,
        job_id: job_id(id),
        entry_kind: DashboardQueueEntryKind::Batch,
        task_id: None,
        turn_id: None,
        run_id: None,
        run_max_parallel: None,
        pinned_worker: None,
        project_id: format!("project-{id}"),
        worktree_id: format!("worktree-{id}"),
        project_label: Some(format!("Queue {id}")),
        command_summary: DashboardCommandSummary {
            mode: DashboardCommandMode::Shell,
            arg_count: None,
        },
        created_at_millis: id as u64,
        requirements: vec!["swift".into()],
        blocking_code: "NO_IDLE_WORKER".into(),
    }
}

fn job_id(value: u128) -> JobId {
    format!("{value:032x}").parse().unwrap()
}

fn job_duplicate_snapshot(
    local: Vec<DashboardJob>,
    remote: Vec<Result<DashboardJob, DashboardError>>,
) -> DashboardSnapshot {
    let source = FakeSource::new();
    source.set_local_jobs(Ok(local));
    source.set_remote(remote);
    service(source).snapshot(Default::default()).unwrap()
}

fn jobs_snapshot(jobs: Vec<DashboardJob>) -> DashboardSnapshot {
    let source = FakeSource::new();
    source.set_local_jobs(Ok(jobs));
    service(source).snapshot(Default::default()).unwrap()
}

fn empty_timeout_snapshot(generated_at_millis: u64) -> DashboardSnapshot {
    DashboardSnapshot {
        api_version: DASHBOARD_API_VERSION,
        revision: 0,
        generated_at_millis,
        collection: CollectionSummary {
            freshness: Freshness::Offline,
            errors: vec![error(
                "DASHBOARD_REFRESH_TIMEOUT",
                "dashboard refresh wait timed out",
            )],
        },
        project_defaults: None,
        task_view: TaskListProjection::empty(),
        workers: Vec::new(),
        queue: Vec::new(),
        active_jobs: Vec::new(),
        recent_jobs: Vec::new(),
        laptop: None,
    }
}

fn worker_names(snapshot: &DashboardSnapshot) -> Vec<&str> {
    snapshot
        .workers
        .iter()
        .map(|worker| worker.name.as_str())
        .collect()
}

fn job_ids(jobs: &[DashboardJob]) -> Vec<JobId> {
    jobs.iter().map(|job| job.job_id).collect()
}

fn error_codes(snapshot: &DashboardSnapshot) -> Vec<&str> {
    snapshot
        .collection
        .errors
        .iter()
        .map(|error| error.code.as_str())
        .collect()
}

fn recent_key(job: &DashboardJob) -> (std::cmp::Reverse<u64>, String) {
    (
        std::cmp::Reverse(job.updated_at_millis),
        job.job_id.to_string(),
    )
}

fn error(code: &str, message: &str) -> DashboardError {
    DashboardError::new(code, message)
}

fn assert_read_only(source: &FakeSource) {
    assert_eq!(source.0.mutation_calls.load(Ordering::SeqCst), 0);
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn wait<'a, T>(
    condvar: &Condvar,
    guard: std::sync::MutexGuard<'a, T>,
) -> std::sync::MutexGuard<'a, T> {
    condvar
        .wait(guard)
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
