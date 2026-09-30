use mac_worker::{
    controller::events::{
        EventBatch, EventCursor, EventReadResult, EventRuntime, JournalReader, JournalWindow,
        JournalWriter, LocalProjectionRefresh, NewEvent, ReadQuery, SSE_CAPACITY, SSE_MAX_STREAMS,
        Seq, SnapshotRequired, ViewerEventSource, ViewerMessage,
        testing::{
            FakeLocalProjectionRefresh, ManualEventRuntime, MemoryJournal, MemoryViewerEventSource,
        },
    },
    dashboard::events::LocalViewerEventSource,
    error::WorkerError,
};
use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

fn append(journal: &MemoryJournal) {
    journal
        .append(
            EventBatch::try_new(vec![NewEvent::ControllerDrainChanged { drained: false }]).unwrap(),
            Duration::MAX,
        )
        .unwrap();
}
fn cursor(seq: u64) -> EventCursor {
    EventCursor {
        journal_id: uuid::Uuid::from_u128(1),
        seq: Seq::new(seq),
    }
}

type Hook = Box<dyn FnOnce() + Send>;
#[derive(Default)]
struct JournalHooks {
    before_window: Mutex<Option<Hook>>,
    after_window: Mutex<Option<Hook>>,
    before_read: Mutex<Option<Hook>>,
    after_read: Mutex<Option<Hook>>,
    unavailable: AtomicBool,
}
fn run_hook(slot: &Mutex<Option<Hook>>) {
    let hook = slot.lock().unwrap().take();
    if let Some(hook) = hook {
        hook();
    }
}
struct HookJournal {
    journal: MemoryJournal,
    hooks: Arc<JournalHooks>,
}
impl JournalReader for HookJournal {
    fn window(&self, deadline: Duration) -> Result<JournalWindow, WorkerError> {
        if self.hooks.unavailable.load(Ordering::SeqCst) {
            return Err(WorkerError::Unavailable(
                "private path must not escape".into(),
            ));
        }
        run_hook(&self.hooks.before_window);
        let result = self.journal.window(deadline);
        run_hook(&self.hooks.after_window);
        result
    }
    fn read(&self, query: ReadQuery, deadline: Duration) -> Result<EventReadResult, WorkerError> {
        // Only replay has hooks; the tail reader still uses the same T1 fake.
        if std::thread::current().name() != Some("dashboard-event-tailer") {
            run_hook(&self.hooks.before_read);
        }
        let result = self.journal.read(query, deadline);
        if std::thread::current().name() != Some("dashboard-event-tailer") {
            run_hook(&self.hooks.after_read);
        }
        result
    }
}
#[derive(Default)]
struct Gate {
    ticks: usize,
    permits: usize,
}
struct Ticks {
    runtime: ManualEventRuntime,
    gate: Arc<(Mutex<Gate>, Condvar)>,
}
impl Ticks {
    fn new(runtime: ManualEventRuntime) -> Self {
        let gate = Arc::new((Mutex::new(Gate::default()), Condvar::new()));
        let signal = gate.clone();
        let cancellation = runtime.clone();
        runtime.on_sleep(move |_| {
            let (lock, changed) = &*signal;
            let mut gate = lock.lock().unwrap();
            gate.ticks += 1;
            changed.notify_all();
            while gate.permits == 0 && !cancellation.cancelled() {
                gate = changed.wait(gate).unwrap();
            }
            gate.permits = gate.permits.saturating_sub(1);
        });
        Self { runtime, gate }
    }
    fn tick(&self, millis: u64) {
        let (lock, changed) = &*self.gate;
        let mut gate = lock.lock().unwrap();
        while gate.ticks == 0 {
            gate = changed.wait(gate).unwrap();
        }
        // T1 sleep has already advanced the next 200 ms check.
        self.runtime
            .advance(Duration::from_millis(millis.saturating_sub(200)));
        let before = gate.ticks;
        gate.permits += 1;
        changed.notify_all();
        while gate.ticks == before {
            gate = changed.wait(gate).unwrap();
        }
    }
    fn cancel(&self) {
        self.runtime.cancel();
        let mut gate = self.gate.0.lock().unwrap();
        gate.permits += 1;
        self.gate.1.notify_all();
        drop(gate);
        self.runtime.clear_sleep_hook();
    }
}
struct StreamHarness {
    journal: MemoryJournal,
    hooks: Arc<JournalHooks>,
    ticks: Arc<Ticks>,
    refresh: Arc<FakeLocalProjectionRefresh>,
    source: Arc<LocalViewerEventSource>,
}
impl StreamHarness {
    fn new() -> Self {
        let journal = MemoryJournal::new();
        let hooks = Arc::new(JournalHooks::default());
        let ticks = Arc::new(Ticks::new(ManualEventRuntime::new()));
        let refresh = Arc::new(FakeLocalProjectionRefresh::new());
        let source = LocalViewerEventSource::new(
            Arc::new(HookJournal {
                journal: journal.clone(),
                hooks: hooks.clone(),
            }),
            Arc::new(ticks.runtime.clone()),
            refresh.clone(),
        );
        Self {
            journal,
            hooks,
            ticks,
            refresh,
            source,
        }
    }
}
impl Drop for StreamHarness {
    fn drop(&mut self) {
        self.source.stop();
        self.ticks.cancel();
    }
}

#[test]
fn sse_controls_never_advance_the_journal_cursor() {
    let window = JournalWindow {
        journal_id: uuid::Uuid::from_u128(1),
        oldest_seq: Seq::new(1),
        head_seq: Seq::new(42),
    };
    for (message, name) in [
        (ViewerMessage::Ready(window.clone()), "ready"),
        (
            ViewerMessage::SnapshotRequired(SnapshotRequired {
                reason: "bootstrap".into(),
                window,
            }),
            "snapshot_required",
        ),
        (
            ViewerMessage::SnapshotReady { revision: 42 },
            "snapshot.ready",
        ),
        (ViewerMessage::Heartbeat, "heartbeat"),
        (
            ViewerMessage::Unavailable {
                code: "CONTROLLER_EVENTS_UNAVAILABLE".into(),
            },
            "snapshot_required",
        ),
    ] {
        assert_eq!(message.event_name(), name);
        assert!(message.cursor().is_none());
        let value = serde_json::to_value(message).unwrap();
        assert_eq!(value["event"], name);
        assert!(value.get("id").is_none());
    }
}

#[tokio::test]
async fn replay_live_handoff_deduplicates_an_append_after_capturing_head() {
    let h = StreamHarness::new();
    append(&h.journal);
    let journal = h.journal.clone();
    *h.hooks.after_window.lock().unwrap() = Some(Box::new(move || append(&journal)));
    let mut stream = h.source.subscribe(Some(cursor(0))).unwrap();
    assert!(matches!(stream.recv().await, Some(ViewerMessage::Ready(_))));
    assert!(
        matches!(stream.recv().await, Some(ViewerMessage::ControllerEvent(event)) if event.seq.as_u64() == 1)
    );
    h.ticks.tick(200);
    assert!(
        matches!(stream.recv().await, Some(ViewerMessage::ControllerEvent(event)) if event.seq.as_u64() == 2)
    );
    h.ticks.tick(10_000);
    assert!(matches!(
        stream.recv().await,
        Some(ViewerMessage::Heartbeat)
    ));
    assert!(stream.try_recv().is_err());
    assert!(h.refresh.request_count() > 0);
}

#[tokio::test]
async fn eight_streams_are_admitted_and_cancellation_closes_every_receiver() {
    let h = StreamHarness::new();
    let mut streams = (0..SSE_MAX_STREAMS)
        .map(|_| h.source.subscribe(Some(cursor(0))).unwrap())
        .collect::<Vec<_>>();
    assert!(h.source.subscribe(None).is_err());
    for stream in &mut streams {
        assert!(matches!(stream.recv().await, Some(ViewerMessage::Ready(_))));
    }
    h.source.stop();
    for stream in &mut streams {
        assert!(stream.recv().await.is_none());
    }
    assert!(h.source.subscribe(None).is_err());
}

#[tokio::test]
async fn unavailable_sends_one_safe_repair_and_closes_without_a_baseline() {
    let h = StreamHarness::new();
    h.hooks.unavailable.store(true, Ordering::SeqCst);
    let mut stream = h.source.subscribe(None).unwrap();
    assert!(
        matches!(stream.recv().await, Some(ViewerMessage::Unavailable { code }) if code == "CONTROLLER_EVENTS_UNAVAILABLE")
    );
    assert!(stream.recv().await.is_none());
}

#[tokio::test]
async fn snapshot_publications_and_heartbeats_do_not_advance_the_journal_cursor() {
    let h = StreamHarness::new();
    let mut stream = h.source.subscribe(Some(cursor(0))).unwrap();
    assert!(matches!(stream.recv().await, Some(ViewerMessage::Ready(_))));
    h.refresh.publish(900);
    h.ticks.tick(10_000);
    assert_eq!(
        stream.recv().await,
        Some(ViewerMessage::SnapshotReady { revision: 900 })
    );
    assert_eq!(stream.recv().await, Some(ViewerMessage::Heartbeat));
    append(&h.journal);
    h.ticks.tick(200);
    assert!(
        matches!(stream.recv().await, Some(ViewerMessage::ControllerEvent(event)) if event.seq.as_u64() == 1)
    );
}

#[tokio::test]
async fn bootstrap_reset_expired_and_ahead_send_explicit_repair_baselines() {
    let h = StreamHarness::new();
    for _ in 0..4 {
        append(&h.journal);
    }
    h.journal.trim_to(Seq::new(3)).unwrap();
    let foreign = EventCursor {
        journal_id: uuid::Uuid::from_u128(9),
        seq: Seq::ZERO,
    };
    for (after, expected) in [
        (None, "bootstrap"),
        (Some(foreign), "journal_changed"),
        (Some(cursor(0)), "cursor_expired"),
        (Some(cursor(9)), "cursor_ahead"),
    ] {
        let mut stream = h.source.subscribe(after).unwrap();
        assert!(
            matches!(stream.recv().await, Some(ViewerMessage::SnapshotRequired(repair))
                if repair.reason == expected && repair.window.head_seq.as_u64() == 4)
        );
        assert!(
            matches!(stream.recv().await, Some(ViewerMessage::Ready(window)) if window.head_seq.as_u64() == 4)
        );
    }
}

#[tokio::test]
async fn lag_during_replay_sends_repair_and_closes_without_skipping_to_head() {
    let h = StreamHarness::new();
    append(&h.journal);
    let journal = h.journal.clone();
    let ticks = Arc::clone(&h.ticks);
    *h.hooks.after_window.lock().unwrap() = Some(Box::new(move || {
        for _ in 0..300 {
            append(&journal);
        }
        ticks.tick(200);
        ticks.tick(200);
    }));
    let mut stream = h.source.subscribe(Some(cursor(0))).unwrap();
    assert!(matches!(stream.recv().await, Some(ViewerMessage::Ready(_))));
    assert!(
        matches!(stream.recv().await, Some(ViewerMessage::ControllerEvent(event)) if event.seq.as_u64() == 1)
    );
    assert!(
        matches!(stream.recv().await, Some(ViewerMessage::SnapshotRequired(repair)) if repair.reason == "lagged")
    );
    assert!(stream.recv().await.is_none());
    assert_eq!(
        h.journal.window(Duration::MAX).unwrap().head_seq.as_u64(),
        301
    );
}

#[tokio::test]
async fn lag_closes_stream_and_never_blocks_append() {
    let h = StreamHarness::new();
    let mut stream = h.source.subscribe(Some(cursor(0))).unwrap();
    assert!(matches!(stream.recv().await, Some(ViewerMessage::Ready(_))));
    for _ in 0..260 {
        append(&h.journal);
        h.ticks.tick(200);
        tokio::task::yield_now().await;
    }
    let mut messages = Vec::new();
    while let Some(message) = stream.recv().await {
        messages.push(message);
    }
    assert!(messages.len() <= SSE_CAPACITY);
    assert!(
        matches!(messages.last(), Some(ViewerMessage::SnapshotRequired(repair)) if repair.reason == "lagged")
    );
    assert_eq!(
        h.journal.window(Duration::MAX).unwrap().head_seq.as_u64(),
        260
    );
}

#[tokio::test]
async fn cancellation_closes_stream_before_a_held_replay_reader_returns() {
    let h = StreamHarness::new();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    *h.hooks.after_window.lock().unwrap() = Some(Box::new(move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    }));
    let mut stream = h.source.subscribe(None).unwrap();
    tokio::task::spawn_blocking(move || started_rx.recv().unwrap())
        .await
        .unwrap();
    h.source.stop();
    assert!(stream.recv().await.is_none());
    release_tx.send(()).unwrap();
}

use mac_worker::{
    dashboard::{
        cache::Observation,
        model::{
            ApiError, DashboardError, DashboardJob, DashboardLogChunk, DashboardQueueEntry,
            DashboardWorker, Freshness, SlotSummary, SystemSummary, WorkerHealth,
        },
        service::{
            Clock, DashboardDataSource, DashboardService, DashboardTaskCollection, MonotonicClock,
            WorkerObservationResult,
        },
        task::DashboardTaskSource,
        web::{DashboardHttpServer, DashboardHttpState, DashboardLogSource},
    },
    job::{JobId, LogStream},
    task::{BranchName, ClosePolicy, TaskId, TaskState, TurnId},
    task_view::{
        ReviewState, TaskDetailProjection, TaskFreshness, TaskListProjection, TaskListRow,
    },
};
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpStream},
    sync::{
        atomic::{AtomicU64, AtomicUsize},
        mpsc,
    },
    thread,
};

struct GateClock {
    clock: ManualClock,
    hook: ProjectionHold,
}
impl MonotonicClock for GateClock {
    fn now_millis(&self) -> u64 {
        if thread::current().name() == Some("dashboard-local-projector") {
            let hook = self.hook.lock().unwrap().take();
            if let Some((started, release)) = hook {
                started.send(()).unwrap();
                release.recv().unwrap();
            }
        }
        MonotonicClock::now_millis(&self.clock)
    }
}
struct FixedClock;
impl Clock for FixedClock {
    fn now_millis(&self) -> u64 {
        1_000
    }
}
impl MonotonicClock for FixedClock {
    fn now_millis(&self) -> u64 {
        0
    }
}

#[derive(Clone)]
struct ManualClock(Arc<AtomicU64>);
impl Clock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
impl MonotonicClock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

type ProjectionHold = Arc<Mutex<Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>>>;

#[derive(Clone)]
struct ProjectionSource {
    local_version: Arc<AtomicU64>,
    remote_calls: Arc<AtomicUsize>,
    worker_calls: Arc<AtomicUsize>,
    worker_failure: Arc<AtomicBool>,
    local_calls: Arc<AtomicUsize>,
    worker_stamp: Arc<AtomicU64>,
    hold: ProjectionHold,
    local_hold: ProjectionHold,
}
impl ProjectionSource {
    fn new() -> Self {
        Self {
            local_version: Arc::new(AtomicU64::new(1)),
            remote_calls: Arc::new(AtomicUsize::new(0)),
            worker_calls: Arc::new(AtomicUsize::new(0)),
            worker_failure: Arc::new(AtomicBool::new(false)),
            local_calls: Arc::new(AtomicUsize::new(0)),
            worker_stamp: Arc::new(AtomicU64::new(100)),
            hold: Arc::new(Mutex::new(None)),
            local_hold: Arc::new(Mutex::new(None)),
        }
    }
    fn projection(&self) -> DashboardTaskCollection {
        let task_id = TaskId::new(uuid::Uuid::from_u128(1));
        let mut projection = TaskListProjection::empty();
        projection.tasks.push(TaskListRow {
            task_id,
            run_id: None,
            run_position: None,
            title: "fixture".into(),
            agent: "codex".into(),
            model: None,
            effort: None,
            permissions: None,
            env_profile: None,
            state: TaskState::Open,
            blocking_code: None,
            stage: None,
            residual: None,
            last_outcome: None,
            worker: None,
            branch: BranchName::for_task(task_id),
            turn_count: 0,
            runner: None,
            freshness: TaskFreshness::Current,
            created_at_millis: 1,
            updated_at_millis: self.local_version.load(Ordering::SeqCst),
            active_turn_id: None,
            close_policy: ClosePolicy::Never,
            review_state: ReviewState::NotReviewable,
            delivery: None,
            deliveries: Vec::new(),
            publish_push: false,
        });
        projection.progress.total = 1;
        projection.progress.open = 1;
        DashboardTaskCollection {
            projection,
            errors: Vec::new(),
        }
    }
}
impl DashboardDataSource for ProjectionSource {
    fn configured_workers(&self) -> Result<Vec<String>, DashboardError> {
        Ok(vec!["mini-1".into()])
    }
    fn collect_workers(&self, _: Duration) -> Vec<WorkerObservationResult> {
        self.worker_calls.fetch_add(1, Ordering::SeqCst);
        if self.worker_failure.load(Ordering::SeqCst) {
            return vec![WorkerObservationResult::Failed {
                worker_name: "mini-1".into(),
                error: DashboardError::new("WORKER_UNAVAILABLE", "fixture worker unavailable"),
            }];
        }
        let stamp = self.worker_stamp.load(Ordering::SeqCst);
        vec![WorkerObservationResult::Current(Observation {
            observed_at_millis: stamp,
            cpu_counters: None,
            worker: DashboardWorker {
                name: "mini-1".into(),
                health: WorkerHealth::Ready,
                freshness: Freshness::Current,
                observed_at_millis: Some(stamp),
                hostname: Some(format!("worker-{stamp}")),
                agent_facts: None,
                herdr: None,
                slot: SlotSummary::idle(1),
                capabilities: Vec::new(),
                missing_capabilities: Vec::new(),
                system: SystemSummary {
                    free_disk_bytes: Some(stamp),
                    total_disk_bytes: None,
                    memory_pressure: None,
                    swap_used_bytes: None,
                    cpu_busy_percent: None,
                },
                error: None,
                active_task: None,
            },
        })]
    }
    fn local_jobs(&self) -> Result<Vec<DashboardJob>, DashboardError> {
        Ok(Vec::new())
    }
    fn authoritative_active_jobs(&self, _: Duration) -> Vec<Result<DashboardJob, DashboardError>> {
        Vec::new()
    }
    fn queue_entries(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError> {
        Ok(Vec::new())
    }
    fn local_task_projection(&self) -> Result<DashboardTaskCollection, DashboardError> {
        self.local_calls.fetch_add(1, Ordering::SeqCst);
        let projection = self.projection();
        if let Some((started, release)) = self.local_hold.lock().unwrap().take() {
            started.send(()).unwrap();
            release.recv().unwrap();
        }
        Ok(projection)
    }
    fn task_projection(&self, _: Duration) -> Result<DashboardTaskCollection, DashboardError> {
        self.remote_calls.fetch_add(1, Ordering::SeqCst);
        let projection = self.projection();
        if let Some((started, release)) = self.hold.lock().unwrap().take() {
            started.send(()).unwrap();
            release.recv().unwrap();
        }
        Ok(projection)
    }
}

#[test]
fn lost_generation_fence_preserves_tasks_and_ages_merged_workers() {
    let source = ProjectionSource::new();
    let clock = ManualClock(Arc::new(AtomicU64::new(1_000)));
    let service = Arc::new(DashboardService::new(
        source.clone(),
        clock.clone(),
        FixedClock,
    ));
    service.snapshot(Default::default()).unwrap();
    source.worker_stamp.store(101, Ordering::SeqCst);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    *source.hold.lock().unwrap() = Some((started_tx, release_rx));
    let collector = {
        let service = Arc::clone(&service);
        thread::spawn(move || service.snapshot(Default::default()).unwrap())
    };
    started_rx.recv().unwrap();
    source.local_version.store(2, Ordering::SeqCst);
    clock.0.store(20_000, Ordering::SeqCst);
    let local_result = service.refresh_local_projection();
    release_tx.send(()).unwrap();
    let full = collector.join().unwrap();
    local_result.unwrap();
    let visible = service.read_snapshot().unwrap();
    assert_eq!(visible.task_view.tasks[0].updated_at_millis, 2);
    assert_eq!(full.task_view.tasks[0].updated_at_millis, 2);
    assert_eq!(visible.workers[0].system.free_disk_bytes, Some(101));
    assert_eq!(visible.workers[0].hostname.as_deref(), Some("worker-101"));
    assert_eq!(visible.workers[0].freshness, Freshness::Stale);
}

#[test]
fn local_refresh_calls_no_remote_collector() {
    let source = ProjectionSource::new();
    let service = DashboardService::new(source.clone(), FixedClock, FixedClock);
    service.snapshot(Default::default()).unwrap();
    source.remote_calls.store(0, Ordering::SeqCst);
    source.local_version.store(2, Ordering::SeqCst);
    service.refresh_local_projection().unwrap();
    assert_eq!(
        service.read_snapshot().unwrap().task_view.tasks[0].updated_at_millis,
        2
    );
    assert_eq!(source.remote_calls.load(Ordering::SeqCst), 0);
    assert_eq!(source.worker_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn full_publication_sends_control_after_cache_visibility() {
    let service = DashboardService::new(ProjectionSource::new(), FixedClock, FixedClock);
    let mut publications = service.subscribe_publications();
    let full = service.snapshot(Default::default()).unwrap();
    let revision = publications.try_recv().unwrap();
    assert_eq!(service.read_snapshot().unwrap().revision, revision);
    assert_eq!(full.revision, revision);
}

#[test]
fn local_publication_is_visible_before_its_revision() {
    let source = ProjectionSource::new();
    let service = DashboardService::new(source.clone(), FixedClock, FixedClock);
    service.snapshot(Default::default()).unwrap();
    let mut publications = service.subscribe_publications();
    source.local_version.store(2, Ordering::SeqCst);
    service.refresh_local_projection().unwrap();
    let revision = publications.try_recv().unwrap();
    let visible = service.read_snapshot().unwrap();
    assert_eq!(visible.revision, revision);
    assert_eq!(visible.task_view.tasks[0].updated_at_millis, 2);
}

#[test]
fn cancelled_local_work_cannot_publish_after_a_held_projector_returns() {
    let source = ProjectionSource::new();
    let service = Arc::new(DashboardService::new(
        source.clone(),
        FixedClock,
        FixedClock,
    ));
    let full = service.snapshot(Default::default()).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    *source.local_hold.lock().unwrap() = Some((started_tx, release_rx));
    source.local_version.store(2, Ordering::SeqCst);
    let flight = {
        let service = service.clone();
        thread::spawn(move || service.refresh_local_projection())
    };
    started_rx.recv().unwrap();
    let handle = service.start_local_projection_refresh().unwrap();
    handle.stop();
    release_tx.send(()).unwrap();
    assert!(flight.join().unwrap().is_err());
    assert_eq!(service.read_snapshot().unwrap().revision, full.revision);
    assert_eq!(
        service.read_snapshot().unwrap().task_view.tasks[0].updated_at_millis,
        1
    );
}
#[test]
fn ttl_freshness_without_worker_event_keeps_metrics_and_makes_no_remote_call() {
    let source = ProjectionSource::new();
    let clock = ManualClock(Arc::new(AtomicU64::new(100)));
    let service = DashboardService::new(source.clone(), clock.clone(), FixedClock);
    service.snapshot(Default::default()).unwrap();
    clock.0.store(20_000, Ordering::SeqCst);
    service.refresh_local_projection().unwrap();
    let visible = service.read_snapshot().unwrap();
    assert_eq!(visible.workers[0].freshness, Freshness::Stale);
    assert_eq!(visible.workers[0].system.free_disk_bytes, Some(100));
    assert_eq!(source.remote_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn real_local_worker_coalesces_and_repeats_a_dirty_flight_with_injected_time() {
    let source = ProjectionSource::new();
    let clock = ManualClock(Arc::new(AtomicU64::new(0)));
    let clock_hook: ProjectionHold = Arc::new(Mutex::new(None));
    let (clock_started_tx, clock_started_rx) = mpsc::channel();
    let (clock_release_tx, clock_release_rx) = mpsc::channel();
    *clock_hook.lock().unwrap() = Some((clock_started_tx, clock_release_rx));
    let service = Arc::new(DashboardService::new(
        source.clone(),
        FixedClock,
        GateClock {
            clock: clock.clone(),
            hook: clock_hook.clone(),
        },
    ));
    service.snapshot(Default::default()).unwrap();
    let mut publications = service.subscribe_publications();
    let worker = service.start_local_projection_refresh().unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    *source.local_hold.lock().unwrap() = Some((started_tx, release_rx));
    source.local_version.store(2, Ordering::SeqCst);
    service.request_refresh();
    clock_started_rx.recv().unwrap();
    clock.0.store(100, Ordering::SeqCst);
    clock_release_tx.send(()).unwrap();
    started_rx.recv().unwrap();
    source.local_version.store(3, Ordering::SeqCst);
    clock.0.store(200, Ordering::SeqCst);
    for _ in 0..100 {
        service.request_refresh();
    }
    clock.0.store(300, Ordering::SeqCst);
    release_tx.send(()).unwrap();
    assert_eq!(publications.blocking_recv().unwrap(), 2);
    assert_eq!(publications.blocking_recv().unwrap(), 3);
    worker.stop();
    assert_eq!(
        service.read_snapshot().unwrap().task_view.tasks[0].updated_at_millis,
        3
    );
    assert!(publications.try_recv().is_err());
    assert_eq!(source.remote_calls.load(Ordering::SeqCst), 1);
}
struct Empty;
impl Clock for Empty {
    fn now_millis(&self) -> u64 {
        1_000
    }
}
impl MonotonicClock for Empty {
    fn now_millis(&self) -> u64 {
        0
    }
}
impl DashboardDataSource for Empty {
    fn configured_workers(&self) -> Result<Vec<String>, DashboardError> {
        Ok(Vec::new())
    }
    fn collect_workers(&self, _: Duration) -> Vec<WorkerObservationResult> {
        Vec::new()
    }
    fn local_jobs(&self) -> Result<Vec<DashboardJob>, DashboardError> {
        Ok(Vec::new())
    }
    fn authoritative_active_jobs(&self, _: Duration) -> Vec<Result<DashboardJob, DashboardError>> {
        Vec::new()
    }
    fn queue_entries(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError> {
        Ok(Vec::new())
    }
}
impl DashboardLogSource for Empty {
    fn job_detail(&self, _: JobId) -> Result<DashboardJob, ApiError> {
        Err(ApiError::new("NOT_FOUND", "fixture"))
    }
    fn read_log(
        &self,
        _: JobId,
        _: LogStream,
        _: u64,
        _: u32,
    ) -> Result<DashboardLogChunk, ApiError> {
        Err(ApiError::new("NOT_FOUND", "fixture"))
    }
}
impl DashboardTaskSource for Empty {
    fn task_detail(&self, _: TaskId) -> Result<TaskDetailProjection, ApiError> {
        Err(ApiError::new("NOT_FOUND", "fixture"))
    }
    fn read_task_log(
        &self,
        _: TaskId,
        _: TurnId,
        _: LogStream,
        _: u64,
        _: u32,
    ) -> Result<DashboardLogChunk, ApiError> {
        Err(ApiError::new("NOT_FOUND", "fixture"))
    }
}

#[derive(Default)]
struct ReadyViewer {
    fake: MemoryViewerEventSource,
}
impl ViewerEventSource for ReadyViewer {
    fn subscribe(
        &self,
        after: Option<EventCursor>,
    ) -> Result<tokio::sync::mpsc::Receiver<ViewerMessage>, WorkerError> {
        let receiver = self.fake.subscribe(after)?;
        self.fake.push(ViewerMessage::Ready(JournalWindow {
            journal_id: cursor(0).journal_id,
            oldest_seq: Seq::new(1),
            head_seq: Seq::ZERO,
        }));
        Ok(receiver)
    }
    fn stop(&self) {
        self.fake.stop();
    }
}
fn state() -> Arc<DashboardHttpState<Empty, Empty, Empty>> {
    Arc::new(DashboardHttpState {
        service: Arc::new(DashboardService::new(Empty, Empty, Empty)),
        log_source: Arc::new(Empty),
        task_source: Arc::new(Empty),
        settings_source: None,
        mutation_source: None,
    })
}
async fn request(
    server: &DashboardHttpServer,
    path: &str,
    headers: &str,
    read_ready: bool,
) -> String {
    let address = server
        .local_url()
        .trim_start_matches("http://")
        .parse::<SocketAddr>()
        .unwrap();
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: {address}\r\n{headers}Connection: close\r\n\r\n");
    tokio::task::spawn_blocking(move || {
        let mut socket = TcpStream::connect(address).unwrap();
        socket.write_all(request.as_bytes()).unwrap();
        let mut reader = BufReader::new(socket);
        let mut response = String::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            response.push_str(&line);
            if line == "\r\n" || line.is_empty() {
                break;
            }
        }
        if read_ready && response.starts_with("HTTP/1.1 200") {
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                response.push_str(&line);
                if line.starts_with("data:") || line.is_empty() {
                    break;
                }
            }
        }
        response
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn events_route_preserves_local_404_and_serves_no_store_named_controls() {
    let local = DashboardHttpServer::bind(None, state()).await.unwrap();
    assert!(
        request(&local, "/api/v1/events", "", false)
            .await
            .starts_with("HTTP/1.1 404")
    );
    local.shutdown().await.unwrap();
    let source = Arc::new(ReadyViewer::default());
    let server = DashboardHttpServer::bind_with_events(None, state(), source.clone())
        .await
        .unwrap();
    let response = request(&server, "/api/v1/events", "", true).await;
    server.shutdown().await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("content-type: text/event-stream"));
    assert!(response.contains("cache-control: no-store"));
    assert!(response.contains("event: ready"));
    assert!(!response.contains("id:"));
    assert!(!response.contains("access-control-allow-origin"));
    assert!(response.contains("connect-src 'self'"));
}

#[tokio::test]
async fn events_route_rejects_foreign_origin_cross_site_and_cursor_conflicts() {
    let source = Arc::new(ReadyViewer::default());
    let server = DashboardHttpServer::bind_with_events(None, state(), source.clone())
        .await
        .unwrap();
    for (headers, status) in [
        ("Origin: https://foreign.test\r\n", "403"),
        ("Sec-Fetch-Site: cross-site\r\n", "403"),
        ("Last-Event-ID: malformed\r\n", "400"),
    ] {
        let response = request(&server, "/api/v1/events", headers, false).await;
        assert!(
            response.starts_with(&format!("HTTP/1.1 {status}")),
            "{response}"
        );
    }
    let id = "00000000-0000-0000-0000-000000000001";
    let response = request(
        &server,
        &format!("/api/v1/events?after={id}%3A1"),
        &format!("Last-Event-ID: {id}:2\r\n"),
        false,
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 400"));
    assert!(source.fake.subscriptions().is_empty());
    let response = request(
        &server,
        &format!("/api/v1/events?after={id}%3A9007199254740993"),
        &format!("Origin: {}\r\n", server.local_url()),
        true,
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200"));
    assert_eq!(
        source.fake.subscriptions()[0]
            .as_ref()
            .unwrap()
            .seq
            .as_u64(),
        9_007_199_254_740_993
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_precedes_graceful_join_and_closes_sse_tcp() {
    let source = Arc::new(ReadyViewer::default());
    let server = DashboardHttpServer::bind_with_events(None, state(), source.clone())
        .await
        .unwrap();
    let address = server
        .local_url()
        .trim_start_matches("http://")
        .parse::<SocketAddr>()
        .unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let reading = tokio::task::spawn_blocking(move || {
        let mut socket = TcpStream::connect(address).unwrap();
        write!(
            socket,
            "GET /api/v1/events HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        let mut reader = BufReader::new(socket);
        let mut prefix = String::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            prefix.push_str(&line);
            if line.starts_with("data:") || line.is_empty() {
                break;
            }
        }
        ready_tx.send(prefix).unwrap();
        let mut rest = String::new();
        reader.read_to_string(&mut rest).unwrap();
        rest
    });
    let prefix = ready_rx.await.unwrap();
    server.shutdown().await.unwrap();
    reading.await.unwrap();
    assert!(prefix.contains("event: ready"), "{prefix}");
    assert!(source.fake.subscribe(None).is_err());
}

struct SseConnection {
    socket: BufReader<TcpStream>,
    pending: String,
    names: Vec<String>,
    closed: bool,
}
impl SseConnection {
    fn open(address: SocketAddr, after: Option<EventCursor>) -> Self {
        let path = after.map_or_else(
            || "/api/v1/events".into(),
            |after| format!("/api/v1/events?after={}:{}", after.journal_id, after.seq),
        );
        let mut socket = TcpStream::connect(address).unwrap();
        write!(
            socket,
            "GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        let mut socket = BufReader::new(socket);
        let mut headers = String::new();
        loop {
            let mut line = String::new();
            socket.read_line(&mut line).unwrap();
            headers.push_str(&line);
            if line == "\r\n" || line.is_empty() {
                break;
            }
        }
        assert!(headers.starts_with("HTTP/1.1 200"), "{headers}");
        assert!(headers.contains("transfer-encoding: chunked"), "{headers}");
        Self {
            socket,
            pending: String::new(),
            names: Vec::new(),
            closed: false,
        }
    }
    fn next_frame(&mut self) -> Option<String> {
        loop {
            if let Some(end) = self.pending.find("\n\n") {
                let frame: String = self.pending.drain(..end + 2).collect();
                if let Some(name) = frame.lines().find_map(|line| line.strip_prefix("event: ")) {
                    self.names.push(name.into());
                }
                return Some(frame);
            }
            let mut size = String::new();
            if self.socket.read_line(&mut size).unwrap() == 0 {
                self.closed = true;
                return None;
            }
            let size = usize::from_str_radix(size.trim().split(';').next().unwrap(), 16).unwrap();
            if size == 0 {
                self.closed = true;
                return None;
            }
            let mut bytes = vec![0; size];
            self.socket.read_exact(&mut bytes).unwrap();
            let mut delimiter = [0; 2];
            self.socket.read_exact(&mut delimiter).unwrap();
            assert_eq!(delimiter, *b"\r\n");
            self.pending.push_str(std::str::from_utf8(&bytes).unwrap());
        }
    }
}

// A fake stdin watchdog owns a separate monotonic deadline. SSE frames have
// no access to this clock or its last-input timestamp.
struct ViewerInput {
    now: Duration,
    last_input: Duration,
    eof: bool,
}
impl ViewerInput {
    fn new() -> Self {
        Self {
            now: Duration::ZERO,
            last_input: Duration::ZERO,
            eof: false,
        }
    }
    fn expired(&self) -> bool {
        self.eof
            || self.now.saturating_sub(self.last_input)
                >= mac_worker::controller::events::TUNNEL_TIMEOUT
    }
}

struct SseHarness {
    journal: MemoryJournal,
    ticks: Arc<Ticks>,
    source: ProjectionSource,
    service: Arc<DashboardService<ProjectionSource, FixedClock, FixedClock>>,
    events: Arc<LocalViewerEventSource>,
    server: Option<DashboardHttpServer>,
    connection: Option<SseConnection>,
    collector: Option<thread::JoinHandle<mac_worker::dashboard::model::DashboardSnapshot>>,
    release: Option<mpsc::Sender<()>>,
    viewer: ViewerInput,
}
impl SseHarness {
    async fn new(journal: MemoryJournal, runtime: ManualEventRuntime) -> Self {
        let ticks = Arc::new(Ticks::new(runtime));
        let source = ProjectionSource::new();
        let service = Arc::new(
            DashboardService::new(source.clone(), FixedClock, FixedClock)
                .with_collection_interval(Duration::from_secs(3_600)),
        );
        let mut publications = service.subscribe_publications();
        let events = LocalViewerEventSource::new(
            Arc::new(journal.clone()),
            Arc::new(ticks.runtime.clone()),
            service.clone(),
        );
        let state = Arc::new(DashboardHttpState {
            service: service.clone(),
            log_source: Arc::new(Empty),
            task_source: Arc::new(Empty),
            settings_source: None,
            mutation_source: None,
        });
        let server = DashboardHttpServer::bind_with_events(None, state, events.clone())
            .await
            .unwrap();
        tokio::task::spawn_blocking(move || publications.blocking_recv().unwrap())
            .await
            .unwrap();
        Self {
            journal,
            ticks,
            source,
            service,
            events,
            server: Some(server),
            connection: None,
            collector: None,
            release: None,
            viewer: ViewerInput::new(),
        }
    }
    async fn subscribe(&mut self, after: Option<EventCursor>) {
        let address = self
            .server
            .as_ref()
            .unwrap()
            .local_url()
            .trim_start_matches("http://")
            .parse::<SocketAddr>()
            .unwrap();
        self.connection = Some(
            tokio::task::spawn_blocking(move || SseConnection::open(address, after))
                .await
                .unwrap(),
        );
    }
    async fn next_frame(&mut self) -> Option<String> {
        let mut connection = self.connection.take().unwrap();
        let (connection, frame) = tokio::task::spawn_blocking(move || {
            let frame = connection.next_frame();
            (connection, frame)
        })
        .await
        .unwrap();
        self.connection = Some(connection);
        frame
    }
    fn received_names(&self) -> &[String] {
        &self.connection.as_ref().unwrap().names
    }
    fn stream_closed(&self) -> bool {
        self.connection.as_ref().unwrap().closed
    }
    fn publish_local_revision(&self, task_version: u64) -> u64 {
        self.source
            .local_version
            .store(task_version, Ordering::SeqCst);
        self.service.refresh_local_projection().unwrap()
    }
    async fn hold_full_collector(&mut self) {
        self.source.worker_stamp.store(101, Ordering::SeqCst);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        *self.source.hold.lock().unwrap() = Some((started_tx, release_rx));
        let service = self.service.clone();
        self.collector = Some(thread::spawn(move || {
            service.snapshot(Default::default()).unwrap()
        }));
        self.release = Some(release_tx);
        tokio::task::spawn_blocking(move || started_rx.recv().unwrap())
            .await
            .unwrap();
    }
    async fn release_full_collector(&mut self) {
        self.release.take().unwrap().send(()).unwrap();
        let collector = self.collector.take().unwrap();
        tokio::task::spawn_blocking(move || collector.join().unwrap())
            .await
            .unwrap();
    }
    fn visible_task_revision(&self) -> u64 {
        self.service.read_snapshot().unwrap().task_view.tasks[0].updated_at_millis
    }
    fn worker_observations_merged(&self) -> bool {
        let snapshot = self.service.read_snapshot().unwrap();
        snapshot.workers[0].observed_at_millis == Some(101)
            && snapshot.workers[0].system.free_disk_bytes == Some(101)
    }
    async fn shutdown(&mut self) {
        if let Some(server) = self.server.take() {
            server.shutdown().await.unwrap();
        }
        self.ticks.cancel();
    }
    async fn fire_viewer_timeout(&mut self) {
        self.viewer.now = mac_worker::controller::events::TUNNEL_TIMEOUT;
        assert!(self.viewer.expired());
        self.shutdown().await;
    }
    async fn security_request(
        &self,
        host: &str,
        origin: Option<&str>,
        fetch_site: Option<&str>,
    ) -> String {
        let address = self
            .server
            .as_ref()
            .unwrap()
            .local_url()
            .trim_start_matches("http://")
            .parse::<SocketAddr>()
            .unwrap();
        let mut headers = format!("GET /api/v1/events HTTP/1.1\r\nHost: {host}\r\n");
        if let Some(origin) = origin {
            headers.push_str(&format!("Origin: {origin}\r\n"));
        }
        if let Some(fetch_site) = fetch_site {
            headers.push_str(&format!("Sec-Fetch-Site: {fetch_site}\r\n"));
        }
        headers.push_str("Connection: close\r\n\r\n");
        tokio::task::spawn_blocking(move || {
            let mut socket = TcpStream::connect(address).unwrap();
            socket.write_all(headers.as_bytes()).unwrap();
            let mut reader = BufReader::new(socket);
            let mut response = String::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                response.push_str(&line);
                if line == "\r\n" || line.is_empty() {
                    break;
                }
            }
            response
        })
        .await
        .unwrap()
    }
}
impl Drop for SseHarness {
    fn drop(&mut self) {
        self.events.stop();
        self.ticks.cancel();
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

#[tokio::test]
async fn a_late_full_collector_cannot_restore_older_local_tasks() {
    let mut h = SseHarness::new(MemoryJournal::new(), ManualEventRuntime::new()).await;
    h.subscribe(Some(cursor(0))).await;
    assert!(h.next_frame().await.unwrap().contains("event: ready"));
    h.hold_full_collector().await;
    h.publish_local_revision(2);
    h.release_full_collector().await;
    assert_eq!(h.visible_task_revision(), 2);
    assert!(h.worker_observations_merged());
    // These are deliberately independent values: local task fixture version 2,
    // cache revision 3 and journal sequence 0.
    assert_eq!(h.service.read_snapshot().unwrap().revision, 3);
    assert_eq!(h.journal.window(Duration::MAX).unwrap().head_seq, Seq::ZERO);
    h.fire_viewer_timeout().await;
    while h.next_frame().await.is_some() {}
    assert!(h.stream_closed());
}

#[tokio::test]
async fn event_then_snapshot_ready_reads_fresh_cache() {
    let mut h = SseHarness::new(MemoryJournal::new(), ManualEventRuntime::new()).await;
    h.subscribe(Some(cursor(0))).await;
    h.next_frame().await.unwrap();
    // Drain the initial full publication before the event being asserted.
    h.ticks.tick(200);
    assert!(
        h.next_frame()
            .await
            .unwrap()
            .contains("event: snapshot.ready")
    );
    append(&h.journal);
    h.ticks.tick(200);
    assert!(
        h.next_frame()
            .await
            .unwrap()
            .contains("event: controller.event")
    );
    let revision = h.publish_local_revision(9);
    h.ticks.tick(200);
    let frame = h.next_frame().await.unwrap();
    assert!(frame.contains("event: snapshot.ready"));
    assert!(frame.contains(&format!("\"revision\":{revision}")));
    assert_eq!(h.visible_task_revision(), 9);
    assert_eq!(h.service.read_snapshot().unwrap().revision, revision);
    assert_eq!(
        h.journal.window(Duration::MAX).unwrap().head_seq,
        Seq::new(1)
    );
    assert_eq!(
        h.received_names(),
        [
            "ready",
            "snapshot.ready",
            "controller.event",
            "snapshot.ready"
        ]
    );
    h.shutdown().await;
}

#[tokio::test]
async fn slow_full_collection_does_not_stall_heartbeat() {
    let mut h = SseHarness::new(MemoryJournal::new(), ManualEventRuntime::new()).await;
    h.subscribe(Some(cursor(0))).await;
    h.next_frame().await.unwrap();
    h.ticks.tick(200);
    h.next_frame().await.unwrap();
    h.hold_full_collector().await;
    h.ticks.tick(10_000);
    let frame = h.next_frame().await.unwrap();
    assert!(frame.contains("event: heartbeat"));
    assert!(frame.contains(": keepalive"));
    assert!(!frame.contains("id:"));
    assert!(h.release.is_some());
    h.release_full_collector().await;
    h.shutdown().await;
}

#[tokio::test]
async fn exact_host_and_origin_are_checked_on_the_actual_events_route() {
    let mut h = SseHarness::new(MemoryJournal::new(), ManualEventRuntime::new()).await;
    let url = h.server.as_ref().unwrap().local_url();
    let host = url.trim_start_matches("http://");
    assert!(
        h.security_request("foreign.test", None, None)
            .await
            .starts_with("HTTP/1.1 400")
    );
    assert!(
        h.security_request(host, Some("https://foreign.test"), None)
            .await
            .starts_with("HTTP/1.1 403")
    );
    assert!(
        h.security_request(host, None, Some("cross-site"))
            .await
            .starts_with("HTTP/1.1 403")
    );
    assert!(
        h.security_request(host, Some(&url), Some("same-origin"))
            .await
            .starts_with("HTTP/1.1 200")
    );
    assert!(
        h.security_request(host, None, None)
            .await
            .starts_with("HTTP/1.1 200")
    );
    h.shutdown().await;
}

#[tokio::test]
async fn tunnel_timeout_closes_tcp_and_reconnect_can_resume() {
    let journal = MemoryJournal::new();
    let mut h = SseHarness::new(journal.clone(), ManualEventRuntime::new()).await;
    h.subscribe(Some(cursor(0))).await;
    h.next_frame().await.unwrap();
    h.ticks.tick(200);
    h.next_frame().await.unwrap();
    append(&journal);
    h.ticks.tick(200);
    let event = h.next_frame().await.unwrap();
    assert!(event.contains(&format!("id: {}:1", cursor(0).journal_id)));
    for _ in 0..3 {
        h.ticks.tick(10_000);
        assert!(h.next_frame().await.unwrap().contains("event: heartbeat"));
    }
    assert_eq!(h.viewer.last_input, Duration::ZERO);
    h.fire_viewer_timeout().await;
    while h.next_frame().await.is_some() {}
    assert!(h.stream_closed());
    append(&journal);
    let mut reconnect = SseHarness::new(journal, ManualEventRuntime::new()).await;
    reconnect.subscribe(Some(cursor(1))).await;
    assert!(
        reconnect
            .next_frame()
            .await
            .unwrap()
            .contains("event: ready")
    );
    let replay = reconnect.next_frame().await.unwrap();
    assert!(replay.contains(&format!("id: {}:2", cursor(0).journal_id)));
    reconnect.viewer.eof = true;
    assert!(reconnect.viewer.expired());
    reconnect.shutdown().await;
    while reconnect.next_frame().await.is_some() {}
    assert!(reconnect.stream_closed());
}

#[tokio::test]
async fn append_at_each_replay_live_handoff_has_no_gap_or_duplicate() {
    for point in 0..4 {
        let h = StreamHarness::new();
        append(&h.journal);
        let journal = h.journal.clone();
        let ticks = h.ticks.clone();
        let hook = Box::new(move || {
            append(&journal);
            ticks.tick(200);
        });
        let slot = match point {
            0 => &h.hooks.before_window,
            1 => &h.hooks.after_window,
            2 => &h.hooks.before_read,
            _ => &h.hooks.after_read,
        };
        *slot.lock().unwrap() = Some(hook);
        let mut stream = h.source.subscribe(Some(cursor(0))).unwrap();
        assert!(matches!(stream.recv().await, Some(ViewerMessage::Ready(_))));
        for seq in [1, 2] {
            assert!(
                matches!(stream.recv().await, Some(ViewerMessage::ControllerEvent(event)) if event.seq == Seq::new(seq)),
                "handoff {point}, seq {seq}"
            );
        }
        h.ticks.tick(10_000);
        assert_eq!(stream.recv().await, Some(ViewerMessage::Heartbeat));
        assert!(stream.try_recv().is_err());
    }
}

#[tokio::test]
async fn leader_health_poll_independent_of_sse() {
    let h = StreamHarness::new();
    let mut stream = h.source.subscribe(Some(cursor(0))).unwrap();
    assert!(matches!(stream.recv().await, Some(ViewerMessage::Ready(_))));
    let source = ProjectionSource::new();
    let clock = ManualClock(Arc::new(AtomicU64::new(100)));
    let service = DashboardService::new(source.clone(), clock.clone(), FixedClock);
    service.snapshot(Default::default()).unwrap();
    assert_eq!(source.worker_calls.load(Ordering::SeqCst), 1);
    append(&h.journal);
    h.ticks.tick(200);
    assert!(matches!(
        stream.recv().await,
        Some(ViewerMessage::ControllerEvent(_))
    ));
    // The two-second full collection and ten-second idle probe are independent
    // of the journal's event and heartbeat clocks.
    clock.0.store(2_100, Ordering::SeqCst);
    service.snapshot(Default::default()).unwrap();
    assert_eq!(source.remote_calls.load(Ordering::SeqCst), 2);
    assert_eq!(source.worker_calls.load(Ordering::SeqCst), 1);
    clock.0.store(10_100, Ordering::SeqCst);
    service.snapshot(Default::default()).unwrap();
    assert_eq!(source.worker_calls.load(Ordering::SeqCst), 2);
    h.ticks.tick(10_000);
    assert_eq!(stream.recv().await, Some(ViewerMessage::Heartbeat));
}

#[tokio::test]
async fn recovering_an_unavailable_tail_baseline_requires_explicit_repair() {
    let journal = MemoryJournal::new();
    let hooks = Arc::new(JournalHooks::default());
    hooks.unavailable.store(true, Ordering::SeqCst);
    let ticks = Arc::new(Ticks::new(ManualEventRuntime::new()));
    let source = LocalViewerEventSource::new(
        Arc::new(HookJournal {
            journal: journal.clone(),
            hooks: hooks.clone(),
        }),
        Arc::new(ticks.runtime.clone()),
        Arc::new(FakeLocalProjectionRefresh::new()),
    );
    hooks.unavailable.store(false, Ordering::SeqCst);
    let mut stream = source.subscribe(Some(cursor(0))).unwrap();
    assert!(matches!(stream.recv().await, Some(ViewerMessage::Ready(_))));
    append(&journal);
    ticks.tick(200);
    let repair = stream.recv().await;
    source.stop();
    ticks.cancel();
    assert!(
        matches!(repair, Some(ViewerMessage::SnapshotRequired(repair))
        if repair.reason == "bootstrap" && repair.window.head_seq == Seq::new(1))
    );
}

#[tokio::test]
async fn local_projection_does_not_stall_heartbeat_or_shutdown() {
    let mut h = SseHarness::new(MemoryJournal::new(), ManualEventRuntime::new()).await;
    h.subscribe(Some(cursor(0))).await;
    h.next_frame().await.unwrap();
    h.ticks.tick(200);
    h.next_frame().await.unwrap();
    let before = h.service.read_snapshot().unwrap().revision;
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    *h.source.local_hold.lock().unwrap() = Some((started_tx, release_rx));
    h.source.local_version.store(10, Ordering::SeqCst);
    let service = h.service.clone();
    let flight = thread::spawn(move || service.refresh_local_projection());
    tokio::task::spawn_blocking(move || started_rx.recv().unwrap())
        .await
        .unwrap();
    h.ticks.tick(10_000);
    assert!(h.next_frame().await.unwrap().contains("event: heartbeat"));
    h.shutdown().await;
    while h.next_frame().await.is_some() {}
    assert!(h.stream_closed());
    release_tx.send(()).unwrap();
    assert!(
        tokio::task::spawn_blocking(move || flight.join().unwrap())
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(h.service.read_snapshot().unwrap().revision, before);
    assert_eq!(h.visible_task_revision(), 1);
}

#[tokio::test]
async fn replacing_the_live_journal_epoch_sends_a_repair_before_new_events() {
    let h = StreamHarness::new();
    let mut stream = h.source.subscribe(Some(cursor(0))).unwrap();
    assert!(matches!(stream.recv().await, Some(ViewerMessage::Ready(_))));
    let next_epoch = uuid::Uuid::from_u128(9);
    h.journal.replace_epoch(next_epoch).unwrap();
    h.ticks.tick(200);
    assert!(
        matches!(stream.recv().await, Some(ViewerMessage::SnapshotRequired(repair))
        if repair.reason == "journal_changed" && repair.window.journal_id == next_epoch)
    );
    assert!(
        matches!(stream.recv().await, Some(ViewerMessage::Ready(window))
        if window.journal_id == next_epoch && window.head_seq == Seq::ZERO)
    );
    append(&h.journal);
    h.ticks.tick(200);
    assert!(
        matches!(stream.recv().await, Some(ViewerMessage::ControllerEvent(event))
        if event.journal_id == next_epoch && event.seq == Seq::new(1))
    );
}

#[test]
fn a_lost_task_fence_preserves_a_new_offline_worker_result() {
    let source = ProjectionSource::new();
    let clock = ManualClock(Arc::new(AtomicU64::new(100)));
    let service = Arc::new(DashboardService::new(
        source.clone(),
        clock.clone(),
        FixedClock,
    ));
    service.snapshot(Default::default()).unwrap();
    clock.0.store(20_000, Ordering::SeqCst);
    source.worker_failure.store(true, Ordering::SeqCst);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    *source.hold.lock().unwrap() = Some((started_tx, release_rx));
    let flight_service = service.clone();
    let flight = thread::spawn(move || flight_service.snapshot(Default::default()).unwrap());
    started_rx.recv().unwrap();
    source.local_version.store(2, Ordering::SeqCst);
    service.refresh_local_projection().unwrap();
    release_tx.send(()).unwrap();
    let full = flight.join().unwrap();
    let visible = service.read_snapshot().unwrap();
    assert_eq!(visible.task_view.tasks[0].updated_at_millis, 2);
    assert_eq!(full.workers[0].health, WorkerHealth::Unavailable);
    assert_eq!(visible.workers[0].freshness, Freshness::Offline);
    assert_eq!(visible.workers[0].observed_at_millis, None);
    assert_eq!(
        visible.workers[0].error.as_ref().unwrap().code,
        "WORKER_UNAVAILABLE"
    );
}

struct SubscribeRuntime {
    runtime: ManualEventRuntime,
    hook: Mutex<Option<Hook>>,
}
impl EventRuntime for SubscribeRuntime {
    fn now(&self) -> Duration {
        self.runtime.now()
    }
    fn sleep(&self, duration: Duration) {
        self.runtime.sleep(duration);
    }
    fn cancelled(&self) -> bool {
        if thread::current().name() != Some("dashboard-event-tailer") {
            run_hook(&self.hook);
        }
        self.runtime.cancelled()
    }
}

#[tokio::test]
async fn stop_between_admission_check_and_watch_registration_closes_the_stream() {
    let ticks = Arc::new(Ticks::new(ManualEventRuntime::new()));
    let runtime = Arc::new(SubscribeRuntime {
        runtime: ticks.runtime.clone(),
        hook: Mutex::new(None),
    });
    let source = LocalViewerEventSource::new(
        Arc::new(MemoryJournal::new()),
        runtime.clone(),
        Arc::new(FakeLocalProjectionRefresh::new()),
    );
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    *runtime.hook.lock().unwrap() = Some(Box::new(move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    }));
    let stopping = source.clone();
    let stopper = thread::spawn(move || {
        started_rx.recv().unwrap();
        stopping.stop();
        release_tx.send(()).unwrap();
    });
    let mut stream = source.subscribe(Some(cursor(0))).unwrap();
    let message = stream.recv().await;
    stopper.join().unwrap();
    source.stop();
    ticks.cancel();
    assert_eq!(message, None);
}

#[test]
fn a_held_local_projection_cannot_replace_a_later_full_publication() {
    let source = ProjectionSource::new();
    let service = Arc::new(DashboardService::new(
        source.clone(),
        FixedClock,
        FixedClock,
    ));
    service.snapshot(Default::default()).unwrap();
    let mut publications = service.subscribe_publications();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    *source.local_hold.lock().unwrap() = Some((started_tx, release_rx));
    let flight_service = service.clone();
    let flight = thread::spawn(move || flight_service.refresh_local_projection());
    started_rx.recv().unwrap();
    source.local_version.store(2, Ordering::SeqCst);
    let full = service.snapshot(Default::default()).unwrap();
    assert_eq!(publications.blocking_recv().unwrap(), full.revision);
    release_tx.send(()).unwrap();
    let result = flight.join().unwrap();
    let visible = service.read_snapshot().unwrap();
    assert_eq!(visible.task_view.tasks[0].updated_at_millis, 2);
    assert_eq!(visible.revision, full.revision);
    assert!(result.is_err());
    assert!(publications.try_recv().is_err());
}

#[derive(Default)]
struct StepMonotonic(AtomicU64);
impl MonotonicClock for StepMonotonic {
    fn now_millis(&self) -> u64 {
        self.0.fetch_add(100, Ordering::SeqCst)
    }
}

#[test]
fn superseded_local_work_is_requeued_without_publishing_its_old_projection() {
    let source = ProjectionSource::new();
    let service = Arc::new(DashboardService::new(
        source.clone(),
        FixedClock,
        StepMonotonic::default(),
    ));
    service.snapshot(Default::default()).unwrap();
    let mut publications = service.subscribe_publications();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    *source.local_hold.lock().unwrap() = Some((started_tx, release_rx));
    let worker = service.start_local_projection_refresh().unwrap();
    service.request_refresh();
    started_rx.recv().unwrap();
    source.local_version.store(2, Ordering::SeqCst);
    let full = service.snapshot(Default::default()).unwrap();
    assert_eq!(publications.blocking_recv().unwrap(), full.revision);
    release_tx.send(()).unwrap();
    let local_revision = publications.blocking_recv().unwrap();
    worker.stop();
    let visible = service.read_snapshot().unwrap();
    assert_eq!(source.local_calls.load(Ordering::SeqCst), 2);
    assert_eq!(visible.task_view.tasks[0].updated_at_millis, 2);
    assert_eq!(visible.revision, local_revision);
    assert_eq!(local_revision, full.revision + 1);
    assert!(publications.try_recv().is_err());
}
