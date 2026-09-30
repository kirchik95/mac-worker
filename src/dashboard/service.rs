use std::{
    collections::{HashMap, HashSet},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Condvar, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    agent_facts::FACTS_TTL,
    controller::events::LocalProjectionRefresh,
    dashboard::{
        cache::{
            OBSERVATION_TTL_MILLIS, Observation, ObservationCache, SNAPSHOT_INTERVAL_MILLIS,
            idle_probe_due,
        },
        model::{
            AgentFactsFreshness, CollectionSummary, DASHBOARD_API_VERSION, DashboardActiveTask,
            DashboardError, DashboardJob, DashboardJobState, DashboardLaptop,
            DashboardProjectDefaults, DashboardQueueEntry, DashboardSlotState, DashboardSnapshot,
            DashboardWorker, Freshness, SlotSummary, SystemSummary, WorkerHealth,
        },
    },
    error::WorkerError,
    job::JobId,
    laptop::{BinaryIdentitySource, SystemBinaryIdentitySource, binary_is_outdated},
    task::TaskState,
    task_view::{TaskFreshness, TaskListProjection},
};

pub const MAX_RECENT_TERMINAL_JOBS: usize = 100;
pub const MAX_COLLECTION_ERRORS: usize = 64;
pub const WORKER_COLLECTION_DEADLINE: Duration = Duration::from_secs(15);
pub const GLOBAL_COLLECTION_DEADLINE: Duration = Duration::from_secs(20);
pub const SNAPSHOT_PENDING: &str = "DASHBOARD_SNAPSHOT_PENDING";
pub const COLLECTOR_START_FAILED: &str = "DASHBOARD_COLLECTOR_START_FAILED";

const INVALID_DEADLINES: &str = "INVALID_DASHBOARD_DEADLINES";
const DEADLINE_OVERFLOW: &str = "DASHBOARD_DEADLINE_OVERFLOW";
const REFRESH_TIMEOUT: &str = "DASHBOARD_REFRESH_TIMEOUT";
const REFRESH_ABORTED: &str = "DASHBOARD_REFRESH_ABORTED";
const WORKER_DEADLINE_EXCEEDED: &str = "WORKER_COLLECTION_DEADLINE_EXCEEDED";
const ACTIVE_JOB_DEADLINE_EXCEEDED: &str = "ACTIVE_JOB_COLLECTION_DEADLINE_EXCEEDED";

pub trait Clock: Send + Sync + 'static {
    fn now_millis(&self) -> u64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock predates Unix epoch")
            .as_millis()
            .try_into()
            .expect("millisecond clock exceeds u64")
    }
}

pub trait MonotonicClock: Send + Sync + 'static {
    fn now_millis(&self) -> u64;
}

pub struct SystemMonotonicClock {
    origin: Instant,
}

impl SystemMonotonicClock {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemMonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl MonotonicClock for SystemMonotonicClock {
    fn now_millis(&self) -> u64 {
        self.origin
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DashboardDeadlines {
    worker: Duration,
    global: Duration,
}

impl DashboardDeadlines {
    pub fn new(worker: Duration, global: Duration) -> Result<Self, DashboardError> {
        if worker.is_zero() || global.is_zero() || worker > global {
            return Err(DashboardError::new(
                INVALID_DEADLINES,
                "dashboard deadlines must be nonzero and worker must not exceed global",
            ));
        }
        duration_millis(worker)?;
        duration_millis(global)?;
        Ok(Self { worker, global })
    }
}

impl Default for DashboardDeadlines {
    fn default() -> Self {
        Self {
            worker: WORKER_COLLECTION_DEADLINE,
            global: GLOBAL_COLLECTION_DEADLINE,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum WorkerObservationResult {
    Current(Observation),
    Failed {
        worker_name: String,
        error: DashboardError,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DashboardTaskCollection {
    pub projection: TaskListProjection,
    pub errors: Vec<DashboardError>,
}

impl DashboardTaskCollection {
    pub fn empty() -> Self {
        Self {
            projection: TaskListProjection::empty(),
            errors: Vec::new(),
        }
    }
}

#[derive(Debug, Default)]
pub struct DashboardSnapshotRequest;

pub trait DashboardDataSource: Send + Sync + 'static {
    fn project_defaults(&self) -> Option<DashboardProjectDefaults> {
        None
    }

    fn configured_workers(&self) -> Result<Vec<String>, DashboardError>;

    /// Laptop-config slot count used as the occupancy ceiling when this
    /// worker has no live probe (offline, or a stale cached projection).
    fn configured_worker_slots(&self, _worker_name: &str) -> u8 {
        1
    }

    fn collect_workers(&self, deadline: Duration) -> Vec<WorkerObservationResult>;

    /// Probe only the named workers. The default collects everyone so existing
    /// sources keep compiling; the mac-worker source skips SSH for names that
    /// are not due.
    fn probe_workers(&self, names: &[String], deadline: Duration) -> Vec<WorkerObservationResult> {
        let _ = names;
        self.collect_workers(deadline)
    }
    fn local_jobs(&self) -> Result<Vec<DashboardJob>, DashboardError>;
    fn authoritative_active_jobs(
        &self,
        deadline: Duration,
    ) -> Vec<Result<DashboardJob, DashboardError>>;
    fn queue_entries(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError>;
    fn task_projection(
        &self,
        _deadline: Duration,
    ) -> Result<DashboardTaskCollection, DashboardError> {
        Ok(DashboardTaskCollection::empty())
    }

    /// Saved task/run projection only; this path must never contact workers.
    fn local_task_projection(&self) -> Result<DashboardTaskCollection, DashboardError> {
        Ok(DashboardTaskCollection::empty())
    }

    /// Refresh cached agent facts on a reachable worker. The dashboard
    /// collector calls this off the snapshot path so a slow refresh cannot
    /// shrink the collection deadline. Default is a no-op so read-only
    /// fakes stay read-only until a test injects a recorder.
    fn refresh_worker_facts(&self, _worker_name: &str) -> Result<(), WorkerError> {
        Ok(())
    }
}

pub trait DashboardQueueReader: Send + Sync + 'static {
    fn ordered_pending(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError>;
}

pub struct EmptyDashboardQueueReader;

impl DashboardQueueReader for EmptyDashboardQueueReader {
    fn ordered_pending(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError> {
        Ok(Vec::new())
    }
}

/// Collector options that are not deadlines. `refresh_stale_facts` is on
/// by default so an idle pool does not degrade to `unknown` after the TTL;
/// `--no-facts-refresh` turns it off for a read-only dashboard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DashboardConfig {
    pub refresh_stale_facts: bool,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            refresh_stale_facts: true,
        }
    }
}

pub struct DashboardService<S, C, M> {
    source: Arc<S>,
    cache: Mutex<ObservationCache>,
    completed: Arc<Mutex<Option<Arc<DashboardSnapshot>>>>,
    last_failure: Mutex<Option<DashboardError>>,
    revision: AtomicU64,
    local_generation: AtomicU64,
    publications: tokio::sync::broadcast::Sender<u64>,
    local_refresh: Arc<(Mutex<LocalRefreshState>, Condvar)>,
    refresh: Mutex<RefreshState>,
    facts_refresh: Arc<Mutex<FactsRefreshTracker>>,
    probe_records: Mutex<HashMap<String, WorkerProbeRecord>>,
    clock: C,
    monotonic: M,
    deadlines: DashboardDeadlines,
    refresh_stale_facts: bool,
    collection_interval: Duration,
    binary: Arc<dyn BinaryIdentitySource>,
}

/// Stop handle for the single background observation collector.
///
/// [`Self::stop`] never waits out an in-flight collect. Tests that need the
/// thread to exit can call [`Self::join`].
pub struct CollectorHandle {
    stop: Arc<(Mutex<bool>, Condvar)>,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
}

impl CollectorHandle {
    pub fn stop(self) {
        self.signal_stop();
        let _ = lock_recover(&self.thread).take();
    }

    pub fn join(self) {
        self.signal_stop();
        if let Some(thread) = lock_recover(&self.thread).take() {
            let _ = thread.join();
        }
    }

    fn signal_stop(&self) {
        let (lock, changed) = &*self.stop;
        *lock_recover(lock) = true;
        changed.notify_all();
    }
}

impl Drop for CollectorHandle {
    fn drop(&mut self) {
        self.signal_stop();
        let _ = lock_recover(&self.thread).take();
    }
}

struct RefreshState {
    next_generation: u64,
    in_flight: Option<Arc<RefreshFlight>>,
}

#[derive(Default)]
struct LocalRefreshState {
    dirty: bool,
    running: bool,
    stopped: bool,
    started: bool,
    not_before: u64,
}
impl LocalRefreshState {
    fn request(&mut self, now: u64) {
        if !self.stopped {
            self.dirty = true;
            self.not_before = now.saturating_add(100);
        }
    }
    fn begin(&mut self, now: u64) -> bool {
        if self.stopped || self.running || !self.dirty || now < self.not_before {
            return false;
        }
        self.running = true;
        self.dirty = false;
        true
    }
    fn finish(&mut self) {
        self.running = false;
    }
    fn stop(&mut self) {
        self.stopped = true;
        self.dirty = false;
    }
}

pub struct LocalRefreshHandle {
    signal: Arc<(Mutex<LocalRefreshState>, Condvar)>,
    completed: Arc<Mutex<Option<Arc<DashboardSnapshot>>>>,
}
impl LocalRefreshHandle {
    pub fn stop(&self) {
        let _publication = lock_recover(&self.completed);
        lock_recover(&self.signal.0).stop();
        self.signal.1.notify_all();
    }
}
impl Drop for LocalRefreshHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Last attempt is stamped when a refresh is *started*, including a failed
/// one, so a broken worker is retried at most once per TTL. `in_flight`
/// is the single-flight latch: a still-running refresh is not started again
/// even if the wall clock has already passed the TTL.
#[derive(Default)]
struct FactsRefreshTracker {
    last_attempt_millis: HashMap<String, u64>,
    in_flight: HashSet<String>,
}

impl FactsRefreshTracker {
    fn try_begin(&mut self, worker_name: &str, now_millis: u64) -> bool {
        if self.in_flight.contains(worker_name) {
            return false;
        }
        if self
            .last_attempt_millis
            .get(worker_name)
            .is_some_and(|last| now_millis.saturating_sub(*last) < FACTS_TTL)
        {
            return false;
        }
        self.in_flight.insert(worker_name.to_owned());
        self.last_attempt_millis
            .insert(worker_name.to_owned(), now_millis);
        true
    }

    fn finish(&mut self, worker_name: &str) {
        self.in_flight.remove(worker_name);
    }
}

struct FactsRefreshGuard {
    tracker: Arc<Mutex<FactsRefreshTracker>>,
    worker_name: String,
}

impl Drop for FactsRefreshGuard {
    fn drop(&mut self) {
        lock_recover(&self.tracker).finish(&self.worker_name);
    }
}

struct RefreshFlight {
    generation: u64,
    result: Mutex<Option<Result<Arc<DashboardSnapshot>, DashboardError>>>,
    changed: Condvar,
}

enum RefreshRole {
    Leader(Arc<RefreshFlight>),
    Waiter(Arc<RefreshFlight>),
}

impl<S: DashboardDataSource, C: Clock, M: MonotonicClock> DashboardService<S, C, M> {
    pub fn new(source: S, clock: C, monotonic: M) -> Self {
        Self::with_deadlines(source, clock, monotonic, DashboardDeadlines::default())
    }

    pub fn with_deadlines(
        source: S,
        clock: C,
        monotonic: M,
        deadlines: DashboardDeadlines,
    ) -> Self {
        Self::with_options(
            source,
            clock,
            monotonic,
            deadlines,
            DashboardConfig::default(),
        )
    }

    pub fn with_options(
        source: S,
        clock: C,
        monotonic: M,
        deadlines: DashboardDeadlines,
        config: DashboardConfig,
    ) -> Self {
        Self {
            source: Arc::new(source),
            cache: Mutex::new(ObservationCache::new()),
            completed: Arc::new(Mutex::new(None)),
            last_failure: Mutex::new(None),
            revision: AtomicU64::new(0),
            local_generation: AtomicU64::new(0),
            publications: tokio::sync::broadcast::channel(256).0,
            local_refresh: Arc::new((Mutex::new(LocalRefreshState::default()), Condvar::new())),
            refresh: Mutex::new(RefreshState {
                next_generation: 1,
                in_flight: None,
            }),
            facts_refresh: Arc::new(Mutex::new(FactsRefreshTracker::default())),
            probe_records: Mutex::new(HashMap::new()),
            clock,
            monotonic,
            deadlines,
            refresh_stale_facts: config.refresh_stale_facts,
            collection_interval: Duration::from_millis(SNAPSHOT_INTERVAL_MILLIS),
            binary: Arc::new(SystemBinaryIdentitySource::capture()),
        }
    }

    pub fn with_binary_source(mut self, source: Arc<dyn BinaryIdentitySource>) -> Self {
        self.binary = source;
        self
    }

    pub fn with_refresh_stale_facts(mut self, enabled: bool) -> Self {
        self.refresh_stale_facts = enabled;
        self
    }

    pub fn with_collection_interval(mut self, interval: Duration) -> Self {
        if !interval.is_zero() {
            self.collection_interval = interval;
        }
        self
    }

    /// HTTP snapshot path: last completed snapshot only. Never probes.
    pub fn read_snapshot(&self) -> Result<DashboardSnapshot, DashboardError> {
        let now_millis = self.clock.now_millis();
        let completed = lock_recover(&self.completed).as_ref().map(Arc::clone);
        let failure = lock_recover(&self.last_failure).clone();
        match completed {
            None => Err(failure.unwrap_or_else(snapshot_pending_error)),
            Some(snapshot) => {
                let mut snapshot = snapshot.as_ref().clone();
                refresh_agent_facts_freshness(&mut snapshot.workers, now_millis);
                apply_laptop_binary(&self.binary, &mut snapshot);
                if let Some(error) = failure {
                    apply_stale_collection(&mut snapshot, error);
                }
                Ok(snapshot)
            }
        }
    }

    pub fn refresh_local_projection(&self) -> Result<u64, DashboardError> {
        // Do all record reads outside the publication lock. In particular this
        // does not share the collector flight or any worker/SSH deadline.
        let tasks = self.source.local_task_projection()?;
        let queue = self.source.queue_entries()?;
        let now_millis = self.clock.now_millis();
        let mut completed = lock_recover(&self.completed);
        if lock_recover(&self.local_refresh.0).stopped {
            return Err(DashboardError::new(
                "DASHBOARD_REFRESH_ABORTED",
                "local projection refresh was cancelled",
            ));
        }
        let mut snapshot = completed
            .as_ref()
            .map(|value| value.as_ref().clone())
            .unwrap_or_else(|| DashboardSnapshot {
                api_version: DASHBOARD_API_VERSION,
                revision: 0,
                generated_at_millis: now_millis,
                collection: CollectionSummary {
                    freshness: Freshness::Current,
                    errors: Vec::new(),
                },
                project_defaults: self.source.project_defaults(),
                task_view: TaskListProjection::empty(),
                workers: Vec::new(),
                queue: Vec::new(),
                active_jobs: Vec::new(),
                recent_jobs: Vec::new(),
                laptop: None,
            });
        let generation = self
            .local_generation
            .load(Ordering::SeqCst)
            .checked_add(1)
            .ok_or_else(|| {
                DashboardError::new(
                    "DASHBOARD_REVISION_OVERFLOW",
                    "dashboard local generation exceeds u64",
                )
            })?;
        snapshot.task_view = tasks.projection;
        snapshot.queue = queue;
        for error in tasks.errors {
            push_error(&mut snapshot.collection.errors, error);
        }
        snapshot.generated_at_millis = now_millis;
        age_cached_workers(&mut snapshot.workers, now_millis);
        refresh_agent_facts_freshness(&mut snapshot.workers, now_millis);
        enrich_active_tasks(&mut snapshot.workers, &snapshot.task_view);
        snapshot.revision = self.next_revision()?;
        let revision = snapshot.revision;
        *completed = Some(Arc::new(snapshot));
        self.local_generation.store(generation, Ordering::SeqCst);
        // Receivers can only observe this control once the completed cache is
        // installed; readers use the same lock to see it.
        let _ = self.publications.send(revision);
        Ok(revision)
    }

    pub fn subscribe_publications(&self) -> tokio::sync::broadcast::Receiver<u64> {
        self.publications.subscribe()
    }

    fn publish_full_snapshot(
        &self,
        mut snapshot: DashboardSnapshot,
        local_generation: u64,
    ) -> Result<Arc<DashboardSnapshot>, DashboardError> {
        let mut completed = lock_recover(&self.completed);
        if self.local_generation.load(Ordering::SeqCst) != local_generation
            && let Some(current) = completed.as_ref()
        {
            snapshot.task_view = current.task_view.clone();
            snapshot.queue = current.queue.clone();
            // Worker collection is independently stamped and remains useful
            // even when the task part of this collection lost its fence.
            for worker in &mut snapshot.workers {
                if let Some(cached) = current
                    .workers
                    .iter()
                    .find(|cached| cached.name == worker.name)
                    && cached.observed_at_millis > worker.observed_at_millis
                {
                    *worker = cached.clone();
                }
            }
            snapshot.generated_at_millis = snapshot
                .generated_at_millis
                .max(current.generated_at_millis);
            age_cached_workers(&mut snapshot.workers, snapshot.generated_at_millis);
            enrich_active_tasks(&mut snapshot.workers, &snapshot.task_view);
        }
        snapshot.revision = self.next_revision()?;
        let snapshot = Arc::new(snapshot);
        *completed = Some(Arc::clone(&snapshot));
        *lock_recover(&self.last_failure) = None;
        let _ = self.publications.send(snapshot.revision);
        Ok(snapshot)
    }

    pub fn start_background_collection(
        self: &Arc<Self>,
    ) -> Result<CollectorHandle, DashboardError> {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let wait = Arc::clone(&stop);
        let service = Arc::clone(self);
        let interval = self.collection_interval;
        let thread = thread::Builder::new()
            .name("dashboard-collector".into())
            .spawn(move || service.run_collector(wait, interval))
            .map_err(|_| {
                DashboardError::new(
                    COLLECTOR_START_FAILED,
                    "dashboard observation collector failed to start",
                )
            })?;
        Ok(CollectorHandle {
            stop,
            thread: Mutex::new(Some(thread)),
        })
    }

    pub fn start_local_projection_refresh(
        self: &Arc<Self>,
    ) -> Result<LocalRefreshHandle, DashboardError> {
        let signal = Arc::clone(&self.local_refresh);
        {
            let mut state = lock_recover(&signal.0);
            if state.started {
                return Err(DashboardError::new(
                    COLLECTOR_START_FAILED,
                    "local projection worker is already running",
                ));
            }
            state.started = true;
            state.stopped = false;
        }
        let service = Arc::clone(self);
        if thread::Builder::new()
            .name("dashboard-local-projector".into())
            .spawn(move || service.run_local_projector())
            .is_err()
        {
            lock_recover(&signal.0).started = false;
            return Err(DashboardError::new(
                COLLECTOR_START_FAILED,
                "local projection worker could not start",
            ));
        }
        Ok(LocalRefreshHandle {
            signal,
            completed: Arc::clone(&self.completed),
        })
    }

    fn run_local_projector(&self) {
        let (lock, changed) = &*self.local_refresh;
        let mut state = lock_recover(lock);
        loop {
            if state.stopped {
                state.started = false;
                return;
            }
            if !state.dirty {
                state = changed
                    .wait(state)
                    .unwrap_or_else(|error| error.into_inner());
                continue;
            }
            let now = self.monotonic.now_millis();
            if !state.begin(now) {
                let remaining = state.not_before.saturating_sub(now).max(1);
                state = changed
                    .wait_timeout(state, Duration::from_millis(remaining))
                    .unwrap_or_else(|error| error.into_inner())
                    .0;
                continue;
            }
            drop(state);
            let _ = catch_unwind(AssertUnwindSafe(|| self.refresh_local_projection()));
            state = lock_recover(lock);
            state.finish();
        }
    }

    fn run_collector(&self, stop: Arc<(Mutex<bool>, Condvar)>, interval: Duration) {
        let (lock, changed) = &*stop;
        loop {
            if *lock_recover(lock) {
                break;
            }
            let _ = self.snapshot(DashboardSnapshotRequest);
            let mut stopped = lock_recover(lock);
            if *stopped {
                break;
            }
            let waited = changed.wait_timeout(stopped, interval);
            let (next, _) = match waited {
                Ok(pair) => pair,
                Err(poisoned) => poisoned.into_inner(),
            };
            stopped = next;
            if *stopped {
                break;
            }
        }
    }

    pub fn snapshot(
        &self,
        _request: DashboardSnapshotRequest,
    ) -> Result<DashboardSnapshot, DashboardError> {
        match self.register_refresh() {
            RefreshRole::Leader(flight) => self.lead_refresh(flight),
            RefreshRole::Waiter(flight) => self.wait_for_refresh(flight),
        }
        .map(|snapshot| snapshot.as_ref().clone())
    }

    fn register_refresh(&self) -> RefreshRole {
        let mut refresh = lock_recover(&self.refresh);
        if let Some(flight) = &refresh.in_flight {
            return RefreshRole::Waiter(Arc::clone(flight));
        }

        let generation = refresh.next_generation;
        refresh.next_generation = refresh.next_generation.saturating_add(1);
        let flight = Arc::new(RefreshFlight {
            generation,
            result: Mutex::new(None),
            changed: Condvar::new(),
        });
        refresh.in_flight = Some(Arc::clone(&flight));
        RefreshRole::Leader(flight)
    }

    fn lead_refresh(
        &self,
        flight: Arc<RefreshFlight>,
    ) -> Result<Arc<DashboardSnapshot>, DashboardError> {
        let local_generation = self.local_generation.load(Ordering::SeqCst);
        let result = catch_unwind(AssertUnwindSafe(|| {
            let deadline = absolute_deadline(&self.monotonic, self.deadlines.global)?;
            let snapshot = self.collect_snapshot(deadline)?;
            self.publish_full_snapshot(snapshot, local_generation)
        }))
        .unwrap_or_else(|_| Err(refresh_aborted_error()));

        if let Err(error) = &result {
            *lock_recover(&self.last_failure) = Some(error.clone());
        }
        *lock_recover(&flight.result) = Some(result.clone());

        {
            let mut refresh = lock_recover(&self.refresh);
            if refresh.in_flight.as_ref().is_some_and(|current| {
                current.generation == flight.generation && Arc::ptr_eq(current, &flight)
            }) {
                refresh.in_flight = None;
            }
        }
        flight.changed.notify_all();
        result
    }

    fn wait_for_refresh(
        &self,
        flight: Arc<RefreshFlight>,
    ) -> Result<Arc<DashboardSnapshot>, DashboardError> {
        let deadline = absolute_deadline(&self.monotonic, self.deadlines.global)?;
        let mut result = lock_recover(&flight.result);
        loop {
            if let Some(result) = result.as_ref() {
                return result.clone();
            }

            let remaining = remaining_duration(&self.monotonic, deadline);
            if remaining.is_zero() {
                drop(result);
                return Ok(Arc::new(self.timeout_fallback()));
            }

            let waited = flight.changed.wait_timeout(result, remaining);
            let (next_result, _) = match waited {
                Ok(pair) => pair,
                Err(poisoned) => poisoned.into_inner(),
            };
            result = next_result;
        }
    }

    fn collect_snapshot(&self, deadline: u64) -> Result<DashboardSnapshot, DashboardError> {
        let mut errors = Vec::new();
        let configured = self.collect_configured_workers(&mut errors);
        let mut workers = self.collect_workers(&configured, deadline, &mut errors);
        let local_jobs = dedupe_local_jobs(self.collect_local_jobs(&mut errors), &mut errors);
        let (remote_jobs, remote_duplicates) =
            dedupe_remote_jobs(self.collect_remote_jobs(deadline, &mut errors), &mut errors);
        let (active_jobs, recent_jobs) = reconcile_jobs(
            local_jobs,
            remote_jobs,
            remote_duplicates,
            &workers,
            &mut errors,
        );
        let task_view = self.collect_tasks(deadline, &mut errors);
        enrich_active_tasks(&mut workers, &task_view);
        let queue = match self.source.queue_entries() {
            Ok(queue) => queue,
            Err(error) => {
                push_error(&mut errors, error);
                Vec::new()
            }
        };

        let generated_at_millis = self.clock.now_millis();
        refresh_agent_facts_freshness(&mut workers, generated_at_millis);
        let mut snapshot = DashboardSnapshot {
            api_version: DASHBOARD_API_VERSION,
            revision: 0,
            generated_at_millis,
            collection: CollectionSummary {
                freshness: Freshness::Current,
                errors,
            },
            project_defaults: self.source.project_defaults(),
            task_view,
            workers,
            queue,
            active_jobs,
            recent_jobs,
            laptop: None,
        };
        apply_laptop_binary(&self.binary, &mut snapshot);
        Ok(snapshot)
    }

    fn collect_tasks(&self, deadline: u64, errors: &mut Vec<DashboardError>) -> TaskListProjection {
        let remaining = remaining_duration(&self.monotonic, deadline);
        match self.source.task_projection(remaining) {
            Ok(collection) => {
                for error in collection.errors {
                    push_error(errors, error);
                }
                collection.projection
            }
            Err(error) => {
                push_error(errors, error);
                DashboardTaskCollection::empty().projection
            }
        }
    }

    fn collect_configured_workers(&self, errors: &mut Vec<DashboardError>) -> Vec<String> {
        let configured = match self.source.configured_workers() {
            Ok(configured) => configured,
            Err(error) => {
                push_error(errors, error);
                return Vec::new();
            }
        };

        let mut seen = HashSet::new();
        let mut normalized = Vec::new();
        for worker_name in configured {
            if seen.insert(worker_name.clone()) {
                normalized.push(worker_name);
            } else {
                push_error(
                    errors,
                    DashboardError::new(
                        "DUPLICATE_CONFIGURED_WORKER",
                        format!("configured worker {worker_name} appears more than once"),
                    ),
                );
            }
        }
        normalized
    }

    fn collect_workers(
        &self,
        configured: &[String],
        deadline: u64,
        errors: &mut Vec<DashboardError>,
    ) -> Vec<DashboardWorker> {
        if configured.is_empty() {
            return Vec::new();
        }

        let remaining = remaining_duration(&self.monotonic, deadline);
        if remaining.is_zero() {
            return configured
                .iter()
                .map(|worker_name| {
                    let error = DashboardError::new(
                        WORKER_DEADLINE_EXCEEDED,
                        format!("worker collection budget expired before observing {worker_name}"),
                    );
                    push_error(errors, error.clone());
                    self.fallback_worker(worker_name, error)
                })
                .collect();
        }

        let budget = self.deadlines.worker.min(remaining);
        let now = self.clock.now_millis();
        let due = self.workers_due(configured, now);
        let rows = if due.is_empty() {
            Vec::new()
        } else {
            self.source.probe_workers(&due, budget)
        };
        let configured_set: HashSet<&str> = configured.iter().map(String::as_str).collect();
        let due_set: HashSet<&str> = due.iter().map(String::as_str).collect();
        let mut grouped: HashMap<String, Vec<WorkerObservationResult>> = HashMap::new();
        let mut unknown = Vec::new();
        for row in rows {
            let worker_name = worker_result_name(&row).to_owned();
            if configured_set.contains(worker_name.as_str()) {
                if due_set.contains(worker_name.as_str()) {
                    grouped.entry(worker_name).or_default().push(row);
                }
            } else {
                unknown.push(worker_name);
            }
        }
        unknown.sort();

        let mut workers = Vec::with_capacity(configured.len());
        for worker_name in configured {
            if !due_set.contains(worker_name.as_str())
                && let Some(worker) = self.reused_worker(worker_name, now)
            {
                workers.push(worker);
                continue;
            }
            let rows = grouped.remove(worker_name).unwrap_or_default();
            let worker = match rows.len() {
                0 => {
                    let error = DashboardError::new(
                        "MISSING_WORKER_OBSERVATION",
                        format!("configured worker {worker_name} returned no observation row"),
                    );
                    push_error(errors, error.clone());
                    self.note_probe_failure(worker_name, now);
                    self.fallback_worker(worker_name, error)
                }
                1 => match rows.into_iter().next().expect("one row exists") {
                    WorkerObservationResult::Current(observation) => {
                        self.note_probe_success(worker_name, now, &observation.worker);
                        self.maybe_start_stale_facts_refresh(&observation);
                        let cached = lock_recover(&self.cache).record(observation);
                        if let Some(cached) = cached {
                            let mut worker = cached.worker;
                            worker.error = None;
                            worker
                        } else {
                            let error = DashboardError::new(
                                "INVALID_OBSERVATION_TIMESTAMP",
                                "worker observation timestamp was not newer",
                            );
                            push_error(errors, error.clone());
                            self.note_probe_failure(worker_name, now);
                            self.fallback_worker(worker_name, error)
                        }
                    }
                    WorkerObservationResult::Failed { error, .. } => {
                        let error = bounded_error(error);
                        push_error(errors, error.clone());
                        self.note_probe_failure(worker_name, now);
                        self.fallback_worker(worker_name, error)
                    }
                },
                _ => {
                    let error = DashboardError::new(
                        "DUPLICATE_WORKER_OBSERVATION",
                        format!("worker {worker_name} returned multiple observation rows"),
                    );
                    push_error(errors, error.clone());
                    self.note_probe_failure(worker_name, now);
                    self.fallback_worker(worker_name, error)
                }
            };
            workers.push(worker);
        }

        for worker_name in unknown {
            push_error(
                errors,
                DashboardError::new(
                    "UNKNOWN_WORKER_OBSERVATION",
                    format!("observation row names unknown worker {worker_name}"),
                ),
            );
        }
        workers
    }

    fn workers_due(&self, configured: &[String], now: u64) -> Vec<String> {
        let records = lock_recover(&self.probe_records);
        let cache = lock_recover(&self.cache);
        configured
            .iter()
            .filter(|name| {
                let record = records.get(name.as_str());
                let has_cache = cache.latest(name).is_some();
                !has_cache
                    || idle_probe_due(
                        now,
                        record.map(|record| record.last_probe_millis),
                        record.is_some_and(|record| record.idle),
                        record.is_some_and(|record| record.unchanged),
                    )
            })
            .cloned()
            .collect()
    }

    fn reused_worker(&self, worker_name: &str, now: u64) -> Option<DashboardWorker> {
        let mut observation = lock_recover(&self.cache).latest(worker_name)?.clone();
        let mut workers = [observation.worker.clone()];
        refresh_agent_facts_freshness(&mut workers, now);
        observation.worker = workers[0].clone();
        observation.worker.freshness = Freshness::Current;
        observation.worker.error = None;
        self.maybe_start_stale_facts_refresh(&observation);
        Some(observation.worker)
    }

    fn note_probe_success(&self, worker_name: &str, now: u64, worker: &DashboardWorker) {
        let signature = probe_signature(worker);
        let idle = worker_is_idle(worker);
        let mut records = lock_recover(&self.probe_records);
        let unchanged = records
            .get(worker_name)
            .is_none_or(|previous| previous.signature == signature);
        records.insert(
            worker_name.to_owned(),
            WorkerProbeRecord {
                last_probe_millis: now,
                signature,
                idle,
                unchanged,
            },
        );
    }

    fn note_probe_failure(&self, worker_name: &str, now: u64) {
        let mut records = lock_recover(&self.probe_records);
        let signature = records
            .get(worker_name)
            .map(|record| record.signature.clone())
            .unwrap_or_else(empty_probe_signature);
        records.insert(
            worker_name.to_owned(),
            WorkerProbeRecord {
                last_probe_millis: now,
                signature,
                idle: false,
                unchanged: false,
            },
        );
    }

    fn fallback_worker(&self, worker_name: &str, error: DashboardError) -> DashboardWorker {
        let now_millis = self.clock.now_millis();
        let capacity = self.source.configured_worker_slots(worker_name);
        if let Some(mut worker) = lock_recover(&self.cache).stale_worker(worker_name, now_millis) {
            worker.error = Some(error);
            worker.slot.capacity = capacity;
            return worker;
        }
        offline_worker(worker_name, error, capacity)
    }

    /// Start a facts refresh outside the collection deadline. Unreachable
    /// workers are skipped; a busy slot is not. The snapshot is not held
    /// until the SSH round-trip returns.
    fn maybe_start_stale_facts_refresh(&self, observation: &Observation) {
        if !self.refresh_stale_facts {
            return;
        }
        let worker = &observation.worker;
        if worker.health != WorkerHealth::Ready {
            return;
        }
        let Some(facts) = &worker.agent_facts else {
            return;
        };
        if facts.freshness != AgentFactsFreshness::Stale {
            return;
        }
        let now_millis = self.clock.now_millis();
        if !lock_recover(&self.facts_refresh).try_begin(&worker.name, now_millis) {
            return;
        }
        let source = Arc::clone(&self.source);
        let tracker = Arc::clone(&self.facts_refresh);
        let worker_name = worker.name.clone();
        let _ = thread::Builder::new()
            .name(format!("facts-refresh-{worker_name}"))
            .spawn(move || {
                let _guard = FactsRefreshGuard {
                    tracker,
                    worker_name: worker_name.clone(),
                };
                let _ = source.refresh_worker_facts(&worker_name);
            });
    }

    fn collect_local_jobs(&self, errors: &mut Vec<DashboardError>) -> Vec<DashboardJob> {
        match self.source.local_jobs() {
            Ok(jobs) => jobs,
            Err(error) => {
                push_error(errors, error);
                Vec::new()
            }
        }
    }

    fn collect_remote_jobs(
        &self,
        deadline: u64,
        errors: &mut Vec<DashboardError>,
    ) -> Vec<DashboardJob> {
        let remaining = remaining_duration(&self.monotonic, deadline);
        if remaining.is_zero() {
            push_error(
                errors,
                DashboardError::new(
                    ACTIVE_JOB_DEADLINE_EXCEEDED,
                    "active-job collection budget expired before remote status collection",
                ),
            );
            return Vec::new();
        }

        let mut jobs = Vec::new();
        let mut failures = Vec::new();
        for row in self.source.authoritative_active_jobs(remaining) {
            match row {
                Ok(job) => jobs.push(job),
                Err(error) => failures.push(bounded_error(error)),
            }
        }
        failures
            .sort_by(|left, right| (&left.code, &left.message).cmp(&(&right.code, &right.message)));
        failures
            .into_iter()
            .for_each(|error| push_error(errors, error));
        jobs
    }

    fn next_revision(&self) -> Result<u64, DashboardError> {
        let current = self.revision.load(Ordering::SeqCst);
        let next = current.checked_add(1).ok_or_else(|| {
            DashboardError::new(
                "DASHBOARD_REVISION_OVERFLOW",
                "dashboard revision exceeds u64",
            )
        })?;
        self.revision.store(next, Ordering::SeqCst);
        Ok(next)
    }

    fn timeout_fallback(&self) -> DashboardSnapshot {
        let timeout = DashboardError::new(REFRESH_TIMEOUT, "dashboard refresh wait timed out");
        if let Some(completed) = lock_recover(&self.completed).as_ref() {
            let mut fallback = completed.as_ref().clone();
            refresh_agent_facts_freshness(&mut fallback.workers, self.clock.now_millis());
            apply_laptop_binary(&self.binary, &mut fallback);
            apply_stale_collection(&mut fallback, timeout);
            return fallback;
        }

        let mut snapshot = DashboardSnapshot {
            api_version: DASHBOARD_API_VERSION,
            revision: 0,
            generated_at_millis: self.clock.now_millis(),
            collection: CollectionSummary {
                freshness: Freshness::Offline,
                errors: vec![timeout],
            },
            project_defaults: self.source.project_defaults(),
            task_view: DashboardTaskCollection::empty().projection,
            workers: Vec::new(),
            queue: Vec::new(),
            active_jobs: Vec::new(),
            recent_jobs: Vec::new(),
            laptop: None,
        };
        apply_laptop_binary(&self.binary, &mut snapshot);
        snapshot
    }
}

impl<S: DashboardDataSource, C: Clock, M: MonotonicClock> LocalProjectionRefresh
    for DashboardService<S, C, M>
{
    fn request_refresh(&self) {
        lock_recover(&self.local_refresh.0).request(self.monotonic.now_millis());
        self.local_refresh.1.notify_all();
    }
    fn subscribe_publications(&self) -> tokio::sync::broadcast::Receiver<u64> {
        DashboardService::subscribe_publications(self)
    }
}

fn apply_laptop_binary(source: &Arc<dyn BinaryIdentitySource>, snapshot: &mut DashboardSnapshot) {
    snapshot.laptop = binary_is_outdated(source.as_ref()).then_some(DashboardLaptop {
        binary_outdated: true,
    });
}

fn apply_stale_collection(snapshot: &mut DashboardSnapshot, error: DashboardError) {
    snapshot.collection.freshness = Freshness::Stale;
    for worker in &mut snapshot.workers {
        if worker.freshness == Freshness::Current {
            worker.freshness = Freshness::Stale;
        }
    }
    for task in &mut snapshot.task_view.tasks {
        if task.freshness == TaskFreshness::Current {
            task.freshness = TaskFreshness::Stale;
        }
    }
    push_error(&mut snapshot.collection.errors, error);
}

fn snapshot_pending_error() -> DashboardError {
    DashboardError::new(
        SNAPSHOT_PENDING,
        "dashboard snapshot has not been collected yet",
    )
}

fn enrich_active_tasks(workers: &mut [DashboardWorker], task_view: &TaskListProjection) {
    for worker in workers {
        worker.active_task = None;
        let Some(active_turn_id) = worker.slot.active_job_id else {
            continue;
        };
        let Some(task) = task_view.tasks.iter().find(|task| {
            task.state == TaskState::Active && task.active_turn_id == Some(active_turn_id)
        }) else {
            continue;
        };
        worker.active_task = Some(DashboardActiveTask {
            task_id: task.task_id,
            title: task.title.clone(),
            agent: task.agent.clone(),
            model: task.model.clone(),
            effort: task.effort.clone(),
            turn_number: task.turn_count,
            started_at_millis: None,
            runner: task.runner,
        });
    }
}

fn age_cached_workers(workers: &mut [DashboardWorker], now_millis: u64) {
    for worker in workers {
        if worker.observed_at_millis.is_some_and(|observed| {
            now_millis < observed || now_millis.saturating_sub(observed) > OBSERVATION_TTL_MILLIS
        }) && worker.freshness != Freshness::Offline
        {
            worker.freshness = Freshness::Stale;
        }
    }
}

fn refresh_agent_facts_freshness(workers: &mut [DashboardWorker], now_millis: u64) {
    for worker in workers {
        if let Some(facts) = &mut worker.agent_facts {
            facts.refresh_freshness(now_millis);
        }
        if let Some(herdr) = &mut worker.herdr {
            herdr.refresh_age(now_millis);
        }
    }
}

fn dedupe_local_jobs(
    local_rows: Vec<DashboardJob>,
    errors: &mut Vec<DashboardError>,
) -> HashMap<JobId, DashboardJob> {
    let (local, local_duplicates) = unique_jobs(local_rows);
    for job_id in local_duplicates {
        push_error(
            errors,
            DashboardError::new(
                "DUPLICATE_LOCAL_JOB",
                format!("local job {job_id} appears more than once"),
            ),
        );
    }
    local
}

fn dedupe_remote_jobs(
    remote_rows: Vec<DashboardJob>,
    errors: &mut Vec<DashboardError>,
) -> (HashMap<JobId, DashboardJob>, Vec<JobId>) {
    let (remote, remote_duplicates) = unique_jobs(remote_rows);
    for job_id in &remote_duplicates {
        push_error(
            errors,
            DashboardError::new(
                "DUPLICATE_REMOTE_JOB",
                format!("remote job {job_id} appears more than once and is quarantined"),
            ),
        );
    }
    (remote, remote_duplicates)
}

fn reconcile_jobs(
    mut local: HashMap<JobId, DashboardJob>,
    remote: HashMap<JobId, DashboardJob>,
    remote_duplicates: Vec<JobId>,
    workers: &[DashboardWorker],
    errors: &mut Vec<DashboardError>,
) -> (Vec<DashboardJob>, Vec<DashboardJob>) {
    for job_id in &remote_duplicates {
        local.remove(job_id);
    }
    for (job_id, remote_job) in remote {
        local.insert(job_id, remote_job);
    }

    for worker in workers {
        let Some(job_id) = worker.slot.active_job_id else {
            continue;
        };
        if local
            .get(&job_id)
            .is_some_and(|job| is_terminal(&job.state))
        {
            push_error(
                errors,
                DashboardError::new(
                    "TERMINAL_JOB_LEASE_INCONSISTENCY",
                    format!(
                        "worker {} reports occupied slot for terminal job {job_id}",
                        worker.name
                    ),
                ),
            );
        }
    }

    let mut active = Vec::new();
    let mut recent = Vec::new();
    for job in local.into_values() {
        if is_active(&job.state) {
            active.push(job);
        } else {
            recent.push(job);
        }
    }
    active.sort_by(|left, right| {
        (left.created_at_millis, left.job_id.to_string())
            .cmp(&(right.created_at_millis, right.job_id.to_string()))
    });
    recent.sort_by(|left, right| {
        right
            .updated_at_millis
            .cmp(&left.updated_at_millis)
            .then_with(|| left.job_id.to_string().cmp(&right.job_id.to_string()))
    });
    recent.truncate(MAX_RECENT_TERMINAL_JOBS);
    (active, recent)
}

fn unique_jobs(rows: Vec<DashboardJob>) -> (HashMap<JobId, DashboardJob>, Vec<JobId>) {
    let mut grouped: HashMap<JobId, Vec<DashboardJob>> = HashMap::new();
    for job in rows {
        grouped.entry(job.job_id).or_default().push(job);
    }

    let mut unique = HashMap::new();
    let mut duplicates = Vec::new();
    for (job_id, mut rows) in grouped {
        if rows.len() == 1 {
            unique.insert(job_id, rows.pop().expect("one row exists"));
        } else {
            duplicates.push(job_id);
        }
    }
    duplicates.sort_by_key(ToString::to_string);
    (unique, duplicates)
}

#[derive(Clone, PartialEq)]
struct WorkerProbeSignature {
    health: WorkerHealth,
    hostname: Option<String>,
    slot_state: DashboardSlotState,
    busy: u8,
    active_job_ids: Vec<JobId>,
    capabilities: Vec<String>,
}

struct WorkerProbeRecord {
    last_probe_millis: u64,
    signature: WorkerProbeSignature,
    idle: bool,
    unchanged: bool,
}

fn probe_signature(worker: &DashboardWorker) -> WorkerProbeSignature {
    WorkerProbeSignature {
        health: worker.health.clone(),
        hostname: worker.hostname.clone(),
        slot_state: worker.slot.state.clone(),
        busy: worker.slot.busy,
        active_job_ids: worker.slot.active_job_ids.clone(),
        capabilities: worker.capabilities.clone(),
    }
}

fn worker_is_idle(worker: &DashboardWorker) -> bool {
    worker.health == WorkerHealth::Ready
        && worker.slot.busy == 0
        && worker.slot.state != DashboardSlotState::Busy
}

fn empty_probe_signature() -> WorkerProbeSignature {
    WorkerProbeSignature {
        health: WorkerHealth::Unavailable,
        hostname: None,
        slot_state: DashboardSlotState::Idle,
        busy: 0,
        active_job_ids: Vec::new(),
        capabilities: Vec::new(),
    }
}

fn worker_result_name(result: &WorkerObservationResult) -> &str {
    match result {
        WorkerObservationResult::Current(observation) => &observation.worker.name,
        WorkerObservationResult::Failed { worker_name, .. } => worker_name,
    }
}

fn offline_worker(worker_name: &str, error: DashboardError, capacity: u8) -> DashboardWorker {
    DashboardWorker {
        name: worker_name.to_owned(),
        health: WorkerHealth::Unavailable,
        freshness: Freshness::Offline,
        observed_at_millis: None,
        hostname: None,
        agent_facts: None,
        slot: SlotSummary::idle(capacity),
        capabilities: Vec::new(),
        missing_capabilities: Vec::new(),
        system: SystemSummary {
            free_disk_bytes: None,
            total_disk_bytes: None,
            memory_pressure: None,
            swap_used_bytes: None,
            cpu_busy_percent: None,
        },
        error: Some(error),
        active_task: None,
        herdr: None,
    }
}

fn is_active(state: &DashboardJobState) -> bool {
    matches!(
        state,
        DashboardJobState::Uploading
            | DashboardJobState::Verified
            | DashboardJobState::Accepted
            | DashboardJobState::Running
    )
}

fn is_terminal(state: &DashboardJobState) -> bool {
    matches!(
        state,
        DashboardJobState::Succeeded
            | DashboardJobState::Failed
            | DashboardJobState::Cancelled
            | DashboardJobState::TimedOut
            | DashboardJobState::Lost
    )
}

fn absolute_deadline<M: MonotonicClock>(
    monotonic: &M,
    budget: Duration,
) -> Result<u64, DashboardError> {
    monotonic
        .now_millis()
        .checked_add(duration_millis(budget)?)
        .ok_or_else(|| {
            DashboardError::new(
                DEADLINE_OVERFLOW,
                "dashboard absolute monotonic deadline exceeds u64",
            )
        })
}

fn duration_millis(duration: Duration) -> Result<u64, DashboardError> {
    duration.as_millis().try_into().map_err(|_| {
        DashboardError::new(
            DEADLINE_OVERFLOW,
            "dashboard duration in milliseconds exceeds u64",
        )
    })
}

fn remaining_duration<M: MonotonicClock>(monotonic: &M, deadline: u64) -> Duration {
    Duration::from_millis(deadline.saturating_sub(monotonic.now_millis()))
}

fn bounded_error(error: DashboardError) -> DashboardError {
    DashboardError::new(error.code, error.message)
}

fn push_error(errors: &mut Vec<DashboardError>, error: DashboardError) {
    if errors.len() < MAX_COLLECTION_ERRORS {
        errors.push(bounded_error(error));
    }
}

fn refresh_aborted_error() -> DashboardError {
    DashboardError::new(REFRESH_ABORTED, "dashboard refresh leader aborted")
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Barrier, atomic::AtomicBool},
        thread,
    };

    use super::*;

    struct EmptySource;

    impl DashboardDataSource for EmptySource {
        fn configured_workers(&self) -> Result<Vec<String>, DashboardError> {
            Ok(Vec::new())
        }

        fn collect_workers(&self, _deadline: Duration) -> Vec<WorkerObservationResult> {
            Vec::new()
        }

        fn local_jobs(&self) -> Result<Vec<DashboardJob>, DashboardError> {
            Ok(Vec::new())
        }

        fn authoritative_active_jobs(
            &self,
            _deadline: Duration,
        ) -> Vec<Result<DashboardJob, DashboardError>> {
            Vec::new()
        }

        fn queue_entries(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError> {
            Ok(Vec::new())
        }
    }

    struct FixedClock;

    impl Clock for FixedClock {
        fn now_millis(&self) -> u64 {
            100
        }
    }

    impl MonotonicClock for FixedClock {
        fn now_millis(&self) -> u64 {
            0
        }
    }

    #[test]
    fn poisoned_service_mutexes_are_recovered_by_snapshot() {
        let service = Arc::new(DashboardService::new(EmptySource, FixedClock, FixedClock));
        for poison in [
            |service: &DashboardService<_, _, _>| {
                drop(lock_recover(&service.cache));
                let _guard = service.cache.lock().unwrap();
                panic!("poison cache");
            },
            |service: &DashboardService<_, _, _>| {
                drop(lock_recover(&service.completed));
                let _guard = service.completed.lock().unwrap();
                panic!("poison completed");
            },
            |service: &DashboardService<_, _, _>| {
                drop(lock_recover(&service.last_failure));
                let _guard = service.last_failure.lock().unwrap();
                panic!("poison last_failure");
            },
            |service: &DashboardService<_, _, _>| {
                drop(lock_recover(&service.refresh));
                let _guard = service.refresh.lock().unwrap();
                panic!("poison refresh");
            },
        ] {
            let service_for_thread = Arc::clone(&service);
            assert!(
                thread::spawn(move || poison(&service_for_thread))
                    .join()
                    .is_err()
            );
        }

        let snapshot = service.snapshot(Default::default()).unwrap();
        assert_eq!(snapshot.revision, 1);
    }

    struct BlockingOnceSource {
        blocks: AtomicBool,
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
    }

    impl DashboardDataSource for BlockingOnceSource {
        fn configured_workers(&self) -> Result<Vec<String>, DashboardError> {
            if self.blocks.swap(false, Ordering::SeqCst) {
                self.entered.wait();
                self.release.wait();
            }
            Ok(Vec::new())
        }

        fn collect_workers(&self, _deadline: Duration) -> Vec<WorkerObservationResult> {
            Vec::new()
        }

        fn local_jobs(&self) -> Result<Vec<DashboardJob>, DashboardError> {
            Ok(Vec::new())
        }

        fn authoritative_active_jobs(
            &self,
            _deadline: Duration,
        ) -> Vec<Result<DashboardJob, DashboardError>> {
            Vec::new()
        }

        fn queue_entries(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError> {
            Ok(Vec::new())
        }
    }

    struct CoordinatedMonotonic {
        calls: AtomicU64,
        waiter_entered: Arc<Barrier>,
        waiter_release: Arc<Barrier>,
    }

    impl MonotonicClock for CoordinatedMonotonic {
        fn now_millis(&self) -> u64 {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == 3 {
                self.waiter_entered.wait();
                self.waiter_release.wait();
            }
            0
        }
    }

    #[test]
    fn poisoned_flight_notifies_waiter_cleans_up_and_allows_subsequent_refresh() {
        let source_entered = Arc::new(Barrier::new(2));
        let source_release = Arc::new(Barrier::new(2));
        let waiter_entered = Arc::new(Barrier::new(2));
        let waiter_release = Arc::new(Barrier::new(2));
        let service = Arc::new(DashboardService::new(
            BlockingOnceSource {
                blocks: AtomicBool::new(true),
                entered: Arc::clone(&source_entered),
                release: Arc::clone(&source_release),
            },
            FixedClock,
            CoordinatedMonotonic {
                calls: AtomicU64::new(0),
                waiter_entered: Arc::clone(&waiter_entered),
                waiter_release: Arc::clone(&waiter_release),
            },
        ));

        let leader_service = Arc::clone(&service);
        let leader = thread::spawn(move || leader_service.snapshot(Default::default()));
        source_entered.wait();
        let flight = Arc::clone(
            lock_recover(&service.refresh)
                .in_flight
                .as_ref()
                .expect("leader installed its flight"),
        );

        let flight_for_thread = Arc::clone(&flight);
        assert!(
            thread::spawn(move || {
                let _guard = flight_for_thread.result.lock().unwrap();
                panic!("poison flight result");
            })
            .join()
            .is_err()
        );

        let waiter_service = Arc::clone(&service);
        let waiter = thread::spawn(move || waiter_service.snapshot(Default::default()));
        waiter_entered.wait();
        source_release.wait();
        waiter_release.wait();

        let leader_snapshot = leader.join().unwrap().unwrap();
        let waiter_snapshot = waiter.join().unwrap().unwrap();
        assert_eq!(waiter_snapshot, leader_snapshot);
        assert!(lock_recover(&service.refresh).in_flight.is_none());

        let subsequent = service.snapshot(Default::default()).unwrap();
        assert_eq!(subsequent.revision, leader_snapshot.revision + 1);
    }
}

#[cfg(test)]
mod event_tests {
    use super::*;
    use crate::{
        dashboard::model::SystemSummary,
        task::{BranchName, ClosePolicy, TaskId},
        task_view::{ReviewState, TaskListRow},
    };
    use std::sync::{atomic::AtomicUsize, mpsc};

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
    struct Source {
        local_version: Arc<AtomicU64>,
        remote_calls: Arc<AtomicUsize>,
        worker_stamp: Arc<AtomicU64>,
        hold: ProjectionHold,
        local_hold: ProjectionHold,
    }
    impl Source {
        fn new() -> Self {
            Self {
                local_version: Arc::new(AtomicU64::new(1)),
                remote_calls: Arc::new(AtomicUsize::new(0)),
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
    impl DashboardDataSource for Source {
        fn configured_workers(&self) -> Result<Vec<String>, DashboardError> {
            Ok(vec!["mini-1".into()])
        }
        fn collect_workers(&self, _: Duration) -> Vec<WorkerObservationResult> {
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
        fn authoritative_active_jobs(
            &self,
            _: Duration,
        ) -> Vec<Result<DashboardJob, DashboardError>> {
            Vec::new()
        }
        fn queue_entries(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError> {
            Ok(Vec::new())
        }
        fn local_task_projection(&self) -> Result<DashboardTaskCollection, DashboardError> {
            let projection = self.projection();
            if let Some((started, release)) = lock_recover(&self.local_hold).take() {
                started.send(()).unwrap();
                release.recv().unwrap();
            }
            Ok(projection)
        }
        fn task_projection(&self, _: Duration) -> Result<DashboardTaskCollection, DashboardError> {
            self.remote_calls.fetch_add(1, Ordering::SeqCst);
            let projection = self.projection();
            if let Some((started, release)) = lock_recover(&self.hold).take() {
                started.send(()).unwrap();
                release.recv().unwrap();
            }
            Ok(projection)
        }
    }

    #[test]
    fn a_late_full_collector_cannot_restore_older_local_tasks() {
        let source = Source::new();
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
        *lock_recover(&source.hold) = Some((started_tx, release_rx));
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
        let source = Source::new();
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
    }

    #[test]
    fn debounce_coalesces_requests_and_preserves_dirty_work_during_a_flight() {
        let mut state = LocalRefreshState::default();
        state.request(0);
        state.request(50);
        assert!(!state.begin(149));
        assert!(state.begin(150));
        state.request(160);
        assert!(!state.begin(300));
        state.finish();
        assert!(state.begin(300));
        state.finish();
        assert!(!state.begin(300));
        state.stop();
        state.request(400);
        assert!(!state.begin(500));
    }

    #[test]
    fn full_publication_sends_control_after_cache_visibility() {
        let service = DashboardService::new(Source::new(), FixedClock, FixedClock);
        let mut publications = service.subscribe_publications();
        let full = service.snapshot(Default::default()).unwrap();
        let revision = publications.try_recv().unwrap();
        assert_eq!(service.read_snapshot().unwrap().revision, revision);
        assert_eq!(full.revision, revision);
    }

    #[test]
    fn event_then_snapshot_ready_reads_fresh_cache() {
        let source = Source::new();
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
        let source = Source::new();
        let service = Arc::new(DashboardService::new(
            source.clone(),
            FixedClock,
            FixedClock,
        ));
        let full = service.snapshot(Default::default()).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        *lock_recover(&source.local_hold) = Some((started_tx, release_rx));
        source.local_version.store(2, Ordering::SeqCst);
        let flight = {
            let service = service.clone();
            thread::spawn(move || service.refresh_local_projection())
        };
        started_rx.recv().unwrap();
        let handle = LocalRefreshHandle {
            signal: Arc::clone(&service.local_refresh),
            completed: Arc::clone(&service.completed),
        };
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
        let source = Source::new();
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
        let source = Source::new();
        let clock = ManualClock(Arc::new(AtomicU64::new(0)));
        let service = Arc::new(DashboardService::new(
            source.clone(),
            FixedClock,
            clock.clone(),
        ));
        service.snapshot(Default::default()).unwrap();
        let mut publications = service.subscribe_publications();
        let worker = service.start_local_projection_refresh().unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        *lock_recover(&source.local_hold) = Some((started_tx, release_rx));
        source.local_version.store(2, Ordering::SeqCst);
        service.request_refresh();
        clock.0.store(100, Ordering::SeqCst);
        service.local_refresh.1.notify_all();
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
}
