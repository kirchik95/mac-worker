# Local Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a read-only, loopback-only `worker dashboard` that shows the local three-worker fleet, durable jobs, queue, and reconnectable logs without becoming a scheduler or control plane.

**Architecture:** Dashboard-owned projection types isolate the browser API from host protocol and scheduler internals. A `DashboardService` builds snapshots from an injected read-only data source and an in-memory bounded observation cache; a small ephemeral Rust HTTP server renders embedded static assets and versioned JSON. The initial dashboard-owned code is independently testable with fakes; remote job/log, queue, CLI, and CPU-collection adapters are integration tasks explicitly gated on their prerequisite phases.

**Tech Stack:** Rust 2024; existing `serde`/`serde_json`, OpenSSH transport, typed job/lease/probe records; `axum` 0.8, `tokio` 1 with `macros`, `net`, `rt-multi-thread`, `signal`, `sync`, and `time`, plus embedded HTML/CSS/JavaScript with `include_str!` added only at the HTTP/CLI integration task.

**Spec:** `docs/superpowers/specs/2026-08-26-local-dashboard-design.md`

## Global Constraints

- The dashboard is a read-only observer: it must never acquire/release a lease, submit, cancel, retry, delete, reconcile, mutate local job state, or mutate remote state.
- The CLI, locked local state, remote durable job records, and remote leases remain authoritative; closing the browser or dashboard process must leave jobs unchanged.
- Bind the server only to `127.0.0.1`; expose no flag that binds wildcard, LAN, IPv6, or a worker-side listener.
- `worker dashboard [--port PORT] [--no-open]` uses an OS-selected loopback port by default; `--port` fails when unavailable and `--no-open` prints rather than opens the URL.
- Serve only embedded first-party assets. Do not load CDNs, external fonts, analytics, scripts, WebSockets, or Server-Sent Events.
- Browser polling is bounded: snapshot every two seconds; an open log panel every one second; every worker collection has a 15-second per-host deadline and a 20-second global deadline.
- Coalesce concurrent snapshot refreshes so multiple tabs do not multiply SSH requests.
- The observation cache is process-local, stores at most 150 two-second samples (five minutes) per configured worker, and is discarded when the dashboard exits.
- A failed current worker query is partial success: no cache means `offline`; a prior successful cached observation means `stale` and always carries its observation timestamp.
- Authority order is remote durable job status, remote lease occupancy, locked local queue state, then cached worker observation only after a current query fails.
- API v1 is dashboard-owned and projection-only. It must not serialize `Config`, `ProbeResponse`, `LocalJobRecord`, `JobMeta`, `JobStatus`, `LeaseRecord`, or queue-store records directly.
- `queue` is always present in snapshot JSON; before the Phase 4 queue adapter it is an empty array, not an invented queue model.
- `cpu_busy_percent` and `artifact_status` are nullable. Do not fabricate either before an authoritative typed source exists.
- Project labels are optional; when absent, clients render short project/worktree IDs. Do not persist complete local paths just to make labels friendly.
- Exact argv/shell text, environment values, SSH destinations/configuration, credentials, and local filesystem paths must never be persisted or returned. Existing `CommandSummary` (`argv` plus count, or `shell`) is the only initial command display data.
- Job IDs must be parsed as `JobId`; log stream is exactly `stdout` or `stderr`; offsets are unsigned bytes; limits are `1..=65_536`; requests never accept a path, command, worker destination, or shell text.
- APIs use `Cache-Control: no-store`, restrictive CSP, `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`, no CORS header, text-only rendering, and Host validation against the actual loopback listener address.
- API errors have the exact envelope `{ "error": { "code": "UPPER_SNAKE_CASE", "message": "bounded safe text" } }`; raw SSH stderr and unbounded internal errors are not browser data.
- Do not add mutating HTTP endpoints, authentication, a dashboard daemon, a database, Prometheus/Grafana, cache/artifact controls, or a second scheduler.
- Phase 3 currently owns `src/cli.rs`, `src/lib.rs`, `src/transfer.rs`, `src/client_state.rs`, `src/output.rs`, `src/error.rs`, and `src/protocol.rs`. Early dashboard tasks must not edit those files except Task 1's one-line `src/lib.rs` module export, which is an isolated branch conflict to rebase/resolve before Phase 3 integration.

## File Map

```text
src/dashboard/mod.rs                 staged dashboard module exports and shared constants
src/dashboard/model.rs               stable API-v1 projection DTOs, validation, safe ID/display helpers
src/dashboard/cache.rs               bounded per-worker observation cache and CPU delta calculation
src/dashboard/service.rs             read-only DashboardDataSource, snapshot assembly, authority merge, refresh coalescing
src/dashboard/static/index.html      embedded application shell with no remote resources
src/dashboard/static/dashboard.css   local responsive worker/job/log layout
src/dashboard/static/dashboard.mjs   polling renderer using textContent and offset log continuation
src/dashboard/web.rs                 loopback HTTP router, headers, Host/range validation, embedded assets
src/dashboard/source.rs              Phase-3-gated adapters from typed probe/local job/remote status/log services
src/dashboard/queue.rs               Phase-4-gated adapter from the scheduler's locked FIFO records
src/dashboard/command.rs             Phase-3-gated dashboard command lifecycle and default-browser opener
src/cli.rs                           later public Dashboard subcommand and --port/--no-open parsing
src/lib.rs                           Task-1 module export; later command dispatch with the dashboard process lifetime
Cargo.toml                           later HTTP runtime dependencies
tests/dashboard_model.rs             projection JSON and privacy contract tests
tests/dashboard_cache.rs             cache bounds, stale/offline, and CPU delta tests
tests/dashboard_service.rs           fake-source snapshot, authority, deadline, and coalescing tests
tests/dashboard_client.mjs           executable browser-client rendering, polling, and offset tests using Node's built-in runner
tests/dashboard_web.rs               loopback HTTP, headers, Host, routes, and range integration tests
tests/dashboard_source.rs            Phase-3-gated typed source adapter tests
tests/dashboard_queue.rs             Phase-4-gated FIFO queue adapter tests
tests/dashboard_cpu_adapter.rs       Phase-4-gated CPU probe-to-dashboard adapter tests
tests/dashboard_command.rs           Phase-3-gated CLI/server lifecycle tests
```

---

### Task 1: Dashboard-Owned Projection Contract

**Files:**
- Modify: `src/lib.rs`
- Create: `src/dashboard/mod.rs`
- Create: `src/dashboard/model.rs`
- Create: `tests/dashboard_model.rs`

**Interfaces:**
- Consumes: `crate::job::{CommandSummary, JobId, JobState, LogStream}` and `crate::protocol::MemoryPressure` only through explicit conversion functions in later tasks.
- Produces: `DashboardSnapshot`, `DashboardWorker`, `DashboardJob`, `DashboardJobState`, `DashboardQueueEntry`, `DashboardError`, `Freshness`, `WorkerHealth`, `SlotSummary`, `SystemSummary`, `ArtifactStatus`, `DashboardLogChunk`, `ApiError`, `short_identifier`, and `project_label_or_fallback`.

- [x] **Step 1: Write failing projection-contract tests**

Create `tests/dashboard_model.rs` with exact JSON assertions: a snapshot has `api_version: 1`, `queue: []`, nullable `cpu_busy_percent`/`artifact_status`, a typed job ID, and no keys named `ssh`, `lease_token`, `command`, `argv`, `shell`, `path`, or `environment`. Cover `project_label: null` falling back to `project-0123456789ab/worktree-fedcba987654` and a supplied label being bounded and control-character escaped. Cover `DashboardLogChunk` for base64 data and byte-exact `offset`/`next_offset`.

```rust
#[test]
fn snapshot_v1_keeps_empty_queue_and_never_serializes_private_job_fields() {
    let snapshot = fixture_snapshot();
    let value = serde_json::to_value(snapshot).unwrap();
    assert_eq!(value["api_version"], 1);
    assert_eq!(value["queue"], serde_json::json!([]));
    assert!(value["workers"][0]["system"]["cpu_busy_percent"].is_null());
    assert!(value["recent_jobs"][0]["artifact_status"].is_null());
    assert_absent_object_keys(&value, &["ssh", "lease_token", "command", "argv", "shell", "path", "environment"]);
}

fn assert_absent_object_keys(value: &serde_json::Value, forbidden: &[&str]) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map {
                assert!(!forbidden.contains(&key.as_str()), "leaked object key {key}");
                assert_absent_object_keys(child, forbidden);
            }
        }
        serde_json::Value::Array(values) => values.iter().for_each(|child| assert_absent_object_keys(child, forbidden)),
        _ => {}
    }
}
```

- [x] **Step 2: Run the model test to verify it fails**

Run: `cargo test --locked --test dashboard_model -- --nocapture`

Expected: FAIL because module `dashboard` and its DTOs do not exist.

- [x] **Step 3: Implement the projection DTOs and validation**

Create `src/dashboard/mod.rs`:

```rust
pub mod model;
```

Add exactly `pub mod dashboard;` beside the existing module exports in `src/lib.rs`. Do not export `cache`, `service`, `web`, `source`, `queue`, or `command` until the task creating that file. This is the sole early edit to a Phase-3-owned file and must be resolved as a one-line module-list conflict if this branch is rebased.

Create `src/dashboard/model.rs` with these public types and serde spelling:

```rust
pub const DASHBOARD_API_VERSION: u32 = 1;
pub const MAX_ERROR_MESSAGE_CHARS: usize = 512;
pub const MAX_PROJECT_LABEL_CHARS: usize = 96;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DashboardSnapshot {
    pub api_version: u32,
    pub revision: u64,
    pub generated_at_millis: u64,
    pub collection: CollectionSummary,
    pub workers: Vec<DashboardWorker>,
    pub queue: Vec<DashboardQueueEntry>,
    pub active_jobs: Vec<DashboardJob>,
    pub recent_jobs: Vec<DashboardJob>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Freshness { Current, Stale, Offline }

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WorkerHealth { Ready, Unavailable }

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DashboardWorker {
    pub name: String,
    pub health: WorkerHealth,
    pub freshness: Freshness,
    pub observed_at_millis: Option<u64>,
    pub hostname: Option<String>,
    pub slot: SlotSummary,
    pub capabilities: Vec<String>,
    pub missing_capabilities: Vec<String>,
    pub system: SystemSummary,
    pub error: Option<DashboardError>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DashboardJob {
    pub job_id: JobId,
    pub worker_name: String,
    pub project_id: String,
    pub worktree_id: String,
    pub project_label: Option<String>,
    pub manifest_digest: String,
    pub command_summary: DashboardCommandSummary,
    pub resource_class: String,
    pub created_at_millis: u64,
    pub updated_at_millis: u64,
    pub state: DashboardJobState,
    pub exit_code: Option<u8>,
    pub terminating_signal: Option<u32>,
    pub final_stdout_bytes: Option<u64>,
    pub final_stderr_bytes: Option<u64>,
    pub artifact_status: Option<ArtifactStatus>,
    pub remote_uncertainty: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SlotSummary { pub state: DashboardSlotState, pub capacity: u8, pub active_job_id: Option<JobId> }
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SystemSummary { pub free_disk_bytes: Option<u64>, pub total_disk_bytes: Option<u64>, pub memory_pressure: Option<DashboardMemoryPressure>, pub swap_used_bytes: Option<u64>, pub cpu_busy_percent: Option<f64> }
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DashboardQueueEntry { pub position: u32, pub job_id: JobId, pub project_id: String, pub worktree_id: String, pub project_label: Option<String>, pub command_summary: DashboardCommandSummary, pub created_at_millis: u64, pub requirements: Vec<String>, pub blocking_code: String }
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CollectionSummary { pub freshness: Freshness, pub errors: Vec<DashboardError> }
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DashboardJobState { Uploading, Verified, Accepted, Running, Succeeded, Failed, Cancelled, TimedOut, Lost }
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DashboardSlotState { Idle, Busy }
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DashboardMemoryPressure { Normal, Warn, Critical, Unknown }
```

Use a dashboard-specific `DashboardCommandSummary { mode: DashboardCommandMode, arg_count: Option<u16> }`, so serializing a `DashboardJob` cannot accidentally inherit future private fields from `CommandSummary`. `DashboardError` holds only a code and `sanitize_bounded` message. `DashboardLogChunk` holds `stream`, `offset`, `next_offset`, and base64 `data`; construct it only from an existing validated `LogChunk` or decoded bounded bytes. `ApiError` serializes exactly as the global error envelope. Set `project_label` to `Option<String>` and implement `project_label_or_fallback(&DashboardJob) -> String` using 12-character IDs without retaining paths.

- [x] **Step 4: Run focused model tests and formatting**

Run:

```bash
cargo test --locked --test dashboard_model -- --nocapture
cargo fmt --all --check
```

Expected: both commands exit `0`.

- [x] **Step 5: Commit the independently usable contract**

```bash
git add src/lib.rs src/dashboard/mod.rs src/dashboard/model.rs tests/dashboard_model.rs
git commit -m "feat: define dashboard projection contract"
```

---

### Task 2: Bounded Observation Cache and CPU Delta Math

**Files:**
- Create: `src/dashboard/cache.rs`
- Create: `tests/dashboard_cache.rs`
- Modify: `src/dashboard/mod.rs`
- Modify: `src/dashboard/model.rs`
- Modify: `tests/dashboard_model.rs`

**Interfaces:**
- Consumes: `DashboardWorker`, `Freshness`, `SystemSummary`, and `DASHBOARD_API_VERSION` from `dashboard::model`.
- Produces: `CpuCounters`, `Observation`, `ObservationCache`, `CachedObservation`, `CpuBusyPercent`, `MAX_SAMPLES_PER_WORKER`, and `OBSERVATION_TTL_MILLIS`.

At the implementation step, add exactly `pub mod cache;` to `src/dashboard/mod.rs`; retain the existing `pub mod model;` line and export no future dashboard modules.

- [x] **Step 1: Write failing bounded-cache tests**

Create `tests/dashboard_cache.rs`. Test all-full, all-CPU-only, and mixed 151-insert sequences retain the newest 150 samples across one canonical per-worker history; `latest` selects the newest full observation. Per-worker timestamps must be strictly increasing across both insertion APIs: duplicate or older input changes neither the history nor the CPU baseline. A current failure with a cached full observation whose age is `<= OBSERVATION_TTL_MILLIS` becomes `stale`; an older observation or cache miss becomes `offline` through `None`. Use saturating age so a regressed caller clock treats the last observation as age zero, and always preserve its original `observed_at_millis`.

Test CPU delta exactly: `(total=1000,idle=200)` followed by `(1100,220)` is `80.0`; the first sample, a non-increasing total, an idle delta greater than total delta, and a counter reset all return `None`; every accepted valid counter sample becomes the next baseline even when its interval is discarded. A full observation stores and returns the calculated percentage in its worker, while a CPU-only sample never retroactively changes the last full worker. Reject `idle_ticks > total_ticks` and non-finite or out-of-range percentages. Add model regressions proving `SystemSummary.cpu_busy_percent` serializes only `None` or a finite value in `0..=100` and fails rather than emitting a fabricated wire value.

```rust
#[test]
fn cpu_busy_uses_consecutive_cumulative_counter_deltas_only() {
    let mut cache = ObservationCache::new();
    assert_eq!(cache.record_cpu("mini-1", 1_000, CpuCounters::new(1_000, 200).unwrap()), None);
    assert_eq!(cache.record_cpu("mini-1", 3_000, CpuCounters::new(1_100, 220).unwrap()), Some(CpuBusyPercent::new(80.0).unwrap()));
    assert_eq!(cache.record_cpu("mini-1", 5_000, CpuCounters::new(1_100, 220).unwrap()), None);
}
```

- [x] **Step 2: Run cache tests to verify they fail**

Run: `cargo test --locked --test dashboard_cache -- --nocapture`

Expected: FAIL because `dashboard::cache` is absent.

- [x] **Step 3: Implement cache-only behavior**

Define exact bounds and methods:

```rust
pub const SNAPSHOT_INTERVAL_MILLIS: u64 = 2_000;
pub const OBSERVATION_TTL_MILLIS: u64 = 10_000;
pub const MAX_SAMPLES_PER_WORKER: usize = 150;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuCounters { total_ticks: u64, idle_ticks: u64 }
impl CpuCounters {
    pub fn new(total_ticks: u64, idle_ticks: u64) -> Result<Self, DashboardError>;
    pub fn total_ticks(self) -> u64;
    pub fn idle_ticks(self) -> u64;
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CpuBusyPercent(f64);
impl CpuBusyPercent {
    pub fn new(value: f64) -> Result<Self, DashboardError>;
    pub fn value(self) -> f64;
}

#[derive(Debug, Clone)]
pub struct Observation { pub worker: DashboardWorker, pub observed_at_millis: u64, pub cpu_counters: Option<CpuCounters> }
#[derive(Debug, Clone)]
pub struct CachedObservation { pub worker: DashboardWorker, pub observed_at_millis: u64 }

pub struct ObservationCache {
    samples: BTreeMap<String, VecDeque<ObservationSample>>,
}

impl ObservationCache {
    pub fn new() -> Self;
    pub fn record(&mut self, observation: Observation) -> Option<CachedObservation>;
    pub fn latest(&self, worker_name: &str) -> Option<&Observation>;
    pub fn stale_worker(&self, worker_name: &str, now_millis: u64) -> Option<DashboardWorker>;
    pub fn record_cpu(&mut self, worker_name: &str, observed_at_millis: u64, counters: CpuCounters) -> Option<CpuBusyPercent>;
    pub fn sample_count(&self, worker_name: &str) -> usize;
}
```

`ObservationSample` is a private enum with full-observation and CPU-only variants. Both variants share the same timestamp ordering, 150-entry bound, and previous-counter lookup. `record` is the only production path that appends a full observation: it normalizes the stored worker to `freshness = Current`, sets `observed_at_millis`, writes the validated delta percentage or `None` into the worker, and returns that exact `CachedObservation`. `record_cpu` is a CPU-only helper and must not be called in addition to `record` for the same probe.

`stale_worker` clones the newest full worker, changes only `freshness` to `Stale`, retains `observed_at_millis`, and never calls an external source. A cache miss or expired full observation is represented by `None`; the service turns it into an offline projection. Add a field-level serialization guard to `SystemSummary.cpu_busy_percent` so arbitrary public construction cannot emit NaN, infinity, or a value outside `0..=100`. The cache does not read clocks, sleep, touch disk, or retain source objects.

- [x] **Step 4: Run cache/model regressions**

Run:

```bash
cargo test --locked --test dashboard_cache --test dashboard_model -- --nocapture
cargo clippy --locked --test dashboard_cache -- -D warnings
```

Expected: both commands exit `0`.

- [x] **Step 5: Commit the cache boundary**

```bash
git add src/dashboard/cache.rs src/dashboard/mod.rs src/dashboard/model.rs tests/dashboard_cache.rs tests/dashboard_model.rs
git commit -m "feat: add bounded dashboard observation cache"
```

---

### Task 3: Fake-Driven Snapshot Assembly and Refresh Coalescing

**Files:**
- Create: `src/dashboard/service.rs`
- Create: `tests/dashboard_service.rs`
- Modify: `src/dashboard/mod.rs`

**Interfaces:**
- Consumes: all `dashboard::model` types and `ObservationCache` from Task 2.
- Produces: `Clock`, `SystemClock`, `DashboardDataSource`, `DashboardQueueReader`, `EmptyDashboardQueueReader`, `WorkerObservationResult`, `DashboardService`, `DashboardSnapshotRequest`, `SnapshotCollection`, `MAX_RECENT_TERMINAL_JOBS`, `WORKER_COLLECTION_DEADLINE`, and `GLOBAL_COLLECTION_DEADLINE`.

At the implementation step, add exactly `pub mod service;` to `src/dashboard/mod.rs`; retain `model` and `cache` and export no future dashboard modules.

- [x] **Step 1: Write failing service tests with a deterministic fake**

Create a `FakeSource` in `tests/dashboard_service.rs` that returns three configured names, deterministic worker observations, a local job list, and an explicitly supplied queue. Assert:

- configured worker order is preserved;
- a remote status-derived active job replaces the same local record's stale observation;
- a busy lease without a status still sets the worker slot and active ID;
- current worker failure uses cached stale data, while no cache produces offline;
- `queue` remains empty when `queue_entries()` returns `Vec::new()`;
- terminal jobs sort newest-first, cap at `100`, and active jobs never appear in `recent_jobs`;
- one source error adds a bounded collection error without failing the snapshot;
- two concurrent calls while the fake blocks cause exactly one `collect_workers` invocation and receive the same revision.

```rust
#[test]
fn snapshot_uses_remote_status_before_lease_and_local_observation() {
    let source = FakeSource::with_active_remote_status("mini-1", job_id());
    let service = DashboardService::new(source, fixed_clock(20_000));
    let snapshot = service.snapshot(DashboardSnapshotRequest::default()).unwrap();
    assert_eq!(snapshot.active_jobs[0].state, DashboardJobState::Running);
    assert_eq!(snapshot.workers[0].slot.active_job_id, Some(job_id()));
}
```

- [x] **Step 2: Run service tests to verify they fail**

Run: `cargo test --locked --test dashboard_service -- --nocapture`

Expected: FAIL because `DashboardService` and `DashboardDataSource` do not exist.

- [x] **Step 3: Implement a source-neutral, read-only service**

Define:

```rust
pub const MAX_RECENT_TERMINAL_JOBS: usize = 100;
pub const WORKER_COLLECTION_DEADLINE: Duration = Duration::from_secs(15);
pub const GLOBAL_COLLECTION_DEADLINE: Duration = Duration::from_secs(20);

pub trait Clock: Send + Sync + 'static { fn now_millis(&self) -> u64; }
pub struct SystemClock;
impl Clock for SystemClock {
    fn now_millis(&self) -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).expect("system clock predates Unix epoch").as_millis().try_into().expect("millisecond clock exceeds u64")
    }
}
pub enum WorkerObservationResult { Current(Observation), Failed { worker_name: String, error: DashboardError } }
pub struct SnapshotCollection { pub snapshot: DashboardSnapshot, pub used_cached_snapshot: bool }
#[derive(Debug, Default)]
pub struct DashboardSnapshotRequest;

pub trait DashboardDataSource: Send + Sync + 'static {
    fn configured_workers(&self) -> Result<Vec<String>, DashboardError>;
    fn collect_workers(&self, deadline: Duration) -> Vec<WorkerObservationResult>;
    fn local_jobs(&self) -> Result<Vec<DashboardJob>, DashboardError>;
    fn authoritative_active_jobs(&self, deadline: Duration) -> Vec<Result<DashboardJob, DashboardError>>;
    fn queue_entries(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError>;
}
pub trait DashboardQueueReader: Send + Sync + 'static {
    fn ordered_pending(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError>;
}
pub struct EmptyDashboardQueueReader;
impl DashboardQueueReader for EmptyDashboardQueueReader {
    fn ordered_pending(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError> { Ok(Vec::new()) }
}

pub struct DashboardService<S, C> {
    source: S,
    cache: Mutex<ObservationCache>,
    completed: Mutex<Option<DashboardSnapshot>>,
    revision: AtomicU64,
    refresh: Mutex<RefreshState>,
    refresh_changed: Condvar,
    clock: C,
}
struct RefreshState { in_progress: bool }

impl<S: DashboardDataSource, C: Clock> DashboardService<S, C> {
    pub fn new(source: S, clock: C) -> Self;
    pub fn snapshot(&self, request: DashboardSnapshotRequest) -> Result<DashboardSnapshot, DashboardError>;
}
```

`DashboardSnapshotRequest` has no caller-controlled worker, command, path, or deadline; it is an empty public struct. Use the global deadline to bound source work and return a last completed snapshot marked stale only if the local collection lock cannot be acquired before that deadline. For each `WorkerObservationResult::Current`, call `ObservationCache::record` exactly once and project the returned `CachedObservation.worker`; the source never borrows or owns the cache, and the service never separately calls `record_cpu` for that probe. If `record` returns `None` for a duplicate or out-of-order timestamp, add the bounded collection error code `INVALID_OBSERVATION_TIMESTAMP`, then use `stale_worker(worker_name, clock.now_millis())` or the normal offline projection on a cache miss; never render the rejected payload as current or silently omit its configured worker. Keep refresh coalescing inside the service: exactly one leader refreshes; waiters consume the same completed immutable snapshot. The service never invokes a command, writes a `ClientStateStore`, or calls a mutating lease/job API.

- [x] **Step 4: Run focused service/cache/model tests**

Run:

```bash
cargo test --locked --test dashboard_service --test dashboard_cache --test dashboard_model -- --nocapture
cargo fmt --all --check
```

Expected: both commands exit `0`.

- [x] **Step 5: Commit the testable snapshot service**

```bash
git add src/dashboard/service.rs src/dashboard/mod.rs tests/dashboard_service.rs
git commit -m "feat: assemble read-only dashboard snapshots"
```

---

### Task 4: Embedded Static Dashboard Client

**Files:**
- Create: `src/dashboard/static/index.html`
- Create: `src/dashboard/static/dashboard.css`
- Create: `src/dashboard/static/dashboard.mjs`
- Create: `tests/dashboard_client.mjs`

**Interfaces:**
- Consumes: `GET /api/v1/snapshot`, `GET /api/v1/jobs/{job_id}`, and `GET /api/v1/jobs/{job_id}/logs` JSON shapes from Task 1.
- Produces: a static no-network dashboard shell using `data-testid` identifiers `worker-grid`, `queue-list`, `active-jobs`, `recent-jobs`, `job-detail`, `stdout-log`, and `stderr-log`.


- [x] **Step 1: Write failing executable client behavior tests**

Create `tests/dashboard_client.mjs` using Node's built-in `node:test` and `node:assert/strict`. Import `createDashboardClient` from `../src/dashboard/static/dashboard.mjs`. Build injected fakes: `FakeDocument` supplies test-id nodes; every `FakeNode` has `textContent`, `children`, `append`, and `replaceChildren`, but deliberately no `innerHTML`, `insertAdjacentHTML`, or HTML parser. `FakeFetch` records paths and returns fixtures. `FakeTimers` records delay values and exposes `fire(id)`/`cleared(id)`.

Test exact observable behavior: snapshot requests use only `/api/v1/snapshot`; a project label containing `<img src=x onerror=1>` is stored as node `textContent` unchanged and creates no executable/HTML node; polling schedules snapshot at 2,000 ms and open logs at 1,000 ms; stdout and stderr requests start at independent offsets; each decoded chunk advances only its own exact `next_offset`; after a terminal job reaches both recorded final byte lengths, the log timer is cleared and no later fetch occurs.

```javascript
test('renderer treats project labels as text and log cursors independently', async () => {
  const env = fakeEnvironment({
    snapshot: snapshotWithLabel('<img src=x onerror=1>'),
    stdout: chunk('stdout', 0, 3, 'YWJj'),
    stderr: chunk('stderr', 0, 2, 'ZGU='),
  });
  const client = createDashboardClient(env);
  await client.refreshSnapshot();
  assert.equal(env.document.node('active-jobs').children[0].textContent.includes('<img src=x onerror=1>'), true);
  await client.openJob(JOB_ID);
  await client.refreshLogs();
  assert.deepEqual(env.fetch.paths(), [
    '/api/v1/snapshot',
    `/api/v1/jobs/${JOB_ID}`,
    `/api/v1/jobs/${JOB_ID}/logs?stream=stdout&offset=0&limit=65536`,
    `/api/v1/jobs/${JOB_ID}/logs?stream=stderr&offset=0&limit=65536`,
  ]);
  assert.equal(client.offsets().stdout, 3);
  assert.equal(client.offsets().stderr, 2);
});
```


- [x] **Step 2: Run client tests to verify they fail**

Run: `node --test tests/dashboard_client.mjs`

Expected: FAIL because `dashboard.mjs` does not export `createDashboardClient`.

- [x] **Step 3: Add the static shell and polling renderer**

Write `index.html` with only local `<link rel="stylesheet" href="/assets/dashboard.css">` and `<script type="module" src="/assets/dashboard.mjs"></script>` resources. Use semantic headings, `aria-live="polite"` for refresh status, buttons only for read-only job-detail selection, and `<pre>` elements for logs.

In `dashboard.mjs`, export this dependency-injected surface so Node and the browser use the same behavior:

```javascript
export function createDashboardClient({ document, fetch, timers }) {
  let currentJob = null, stdoutOffset = 0, stderrOffset = 0, snapshotTimer = null, logTimer = null;
  const api = (path) => fetch(path, { cache: 'no-store' });
  const renderText = (node, value) => { node.textContent = value == null ? '—' : String(value); };
  const refreshSnapshot = async () => { const response = await api('/api/v1/snapshot'); renderSnapshot(await response.json()); };
  const openJob = async (jobId) => { const response = await api(`/api/v1/jobs/${encodeURIComponent(jobId)}`); currentJob = await response.json(); stdoutOffset = 0; stderrOffset = 0; };
  const refreshLogs = async () => { if (!currentJob) return; stdoutOffset = await refreshStream('stdout', stdoutOffset); stderrOffset = await refreshStream('stderr', stderrOffset); if (isTerminal(currentJob.state) && stdoutOffset === currentJob.final_stdout_bytes && stderrOffset === currentJob.final_stderr_bytes) timers.clearInterval(logTimer); };
  const start = () => { snapshotTimer = timers.setInterval(refreshSnapshot, 2000); logTimer = timers.setInterval(refreshLogs, 1000); };
  return { refreshSnapshot, openJob, refreshLogs, start, offsets: () => ({ stdout: stdoutOffset, stderr: stderrOffset }) };
}
```

Define `renderSnapshot`, `refreshStream`, and `isTerminal` in the same module. `renderSnapshot` constructs nodes with `document.createElement`, `replaceChildren`, `append`, and `textContent` only; `refreshStream` calls `/api/v1/jobs/{job_id}/logs?stream={stream}&offset={offset}&limit=65536`, base64-decodes into text for the `<pre>`, and returns `next_offset`. The browser entrypoint calls `createDashboardClient({ document, fetch: window.fetch.bind(window), timers: window })` and `start()`. Do not add action endpoints or buttons for cancel/retry/delete. Use `project_label ?? shortId(project_id) + '/' + shortId(worktree_id)` and render command metadata as `argv (N arguments)` or `shell`, never a command string.

- [x] **Step 4: Run asset tests**

Run:

```bash
node --test tests/dashboard_client.mjs
git diff --check
```

Expected: both commands exit `0`.

- [x] **Step 5: Commit the embedded-client source**

```bash
git add src/dashboard/static/index.html src/dashboard/static/dashboard.css src/dashboard/static/dashboard.mjs tests/dashboard_client.mjs
git commit -m "feat: add dashboard static client"
```

---

### Task 5: Loopback HTTP Adapter and Embedded-Asset Tests

**Gate:** Start only after the dashboard branch is based on `main` with the complete Phase 3 public job lifecycle integrated, because this task changes `Cargo.toml` and establishes the runtime that later command wiring uses. It still does not modify Phase 3 service logic.

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Create: `src/dashboard/web.rs`
- Modify: `src/dashboard/mod.rs`
- Create: `tests/dashboard_web.rs`

**Interfaces:**
- Consumes: `DashboardService::snapshot`, `DashboardLogChunk`, `ApiError`, and static files from Tasks 1–4.
- Produces: `DashboardHttpState`, `DashboardHttpServer`, `DashboardHttpServer::bind`, `DashboardHttpServer::local_url`, and `DashboardHttpServer::shutdown`.

- [x] **Step 1: Write failing HTTP integration tests**

Using a fake `DashboardDataSource` and an actual TCP client against an OS-assigned port, test all routes and the exact security policy:

- bind succeeds only on `127.0.0.1:0` and `local_url()` starts `http://127.0.0.1:`;
- `GET /` and `/assets/dashboard.mjs` return expected content types and no third-party URL;
- `GET /api/v1/snapshot` returns projection JSON with `Cache-Control: no-store`;
- invalid job ID, invalid stream, `limit=0`, and `limit=65537` return `400` plus the API error envelope;
- unknown job returns `404` with `JOB_NOT_FOUND`;
- non-loopback or mismatched `Host` returns `400` with `INVALID_HOST`;
- every response includes CSP, `nosniff`, and no-referrer; no response includes `Access-Control-Allow-Origin`.

```rust
#[tokio::test]
async fn api_rejects_unbounded_log_ranges_without_calling_source() {
    let (server, client) = started_fake_server().await;
    let response = client.get("/api/v1/jobs/018f0f4a6b5c7d8e9f00112233445566/logs?stream=stdout&offset=0&limit=65537").send().await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.json::<serde_json::Value>().await.unwrap()["error"]["code"], "INVALID_LOG_RANGE");
    assert_eq!(server.source_call_count(), 0);
}
```

- [x] **Step 2: Run web tests to verify they fail**

Run: `cargo test --locked --test dashboard_web -- --nocapture`

Expected: FAIL because the HTTP adapter and `axum`/`tokio` dependencies do not exist.

- [x] **Step 3: Add dependencies and implement the loopback-only router**

Add exact runtime dependencies:

```toml
axum = "0.8"
tokio = { version = "1", features = ["macros", "net", "rt-multi-thread", "signal", "sync", "time"] }
```

Define:

```rust
#[derive(Clone)]
pub struct DashboardHttpState<S, C> { pub service: Arc<DashboardService<S, C>>, pub log_source: Arc<dyn DashboardLogSource> }
pub trait DashboardLogSource: Send + Sync + 'static {
    fn job_detail(&self, job_id: JobId) -> Result<DashboardJob, ApiError>;
    fn read_log(&self, job_id: JobId, stream: LogStream, offset: u64, limit: u32) -> Result<DashboardLogChunk, ApiError>;
}
pub struct DashboardHttpServer {
    local_addr: SocketAddr,
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<(), ApiError>>,
}
impl DashboardHttpServer {
    pub async fn bind<S, C>(port: Option<u16>, state: Arc<DashboardHttpState<S, C>>) -> Result<Self, ApiError>;
    pub fn local_url(&self) -> String;
    pub async fn shutdown(self) -> Result<(), ApiError>;
}
```

Add exactly `pub mod web;` to `src/dashboard/mod.rs` in this task. Use `TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port.unwrap_or(0))))`; `bind` starts the owned `task` which serves the router until a cloned `watch` receiver observes `true`. `shutdown(self)` sends `true`, awaits `self.task`, maps a join failure to `ApiError { code: "DASHBOARD_SERVER_FAILED", message: "dashboard server task stopped unexpectedly" }`, and returns the task's inner result. Reject any configured address because the only bind input is `Option<u16>`. Embed assets with `include_str!`. Add route handlers only for the five specified GET routes. Parse `JobId` before source lookup, parse stream with a two-value enum, reject `limit` outside `1..=65_536`, and use source errors only after validation. Return browser text as escaped DOM data, never server-rendered untrusted HTML.

- [x] **Step 4: Run HTTP and existing dashboard tests**

Run:

```bash
node --test tests/dashboard_client.mjs
cargo test --locked --test dashboard_web --test dashboard_service -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
```

Expected: both commands exit `0`.

- [x] **Step 5: Commit the HTTP adapter**

```bash
git add Cargo.toml Cargo.lock src/dashboard/mod.rs src/dashboard/web.rs tests/dashboard_web.rs
git commit -m "feat: serve dashboard over loopback"
```

---

### Task 6: Typed Phase-3 Remote Status and Log Adapter

**Gate:** Start only when `main` provides the completed Phase 3 `RemoteJobClient::status` and `RemoteJobClient::log_chunk` interfaces plus public/local job orchestration and stable local record listing. Rebase this branch on that `main` before editing.

**Files:**
- Create: `src/dashboard/source.rs`
- Modify: `src/dashboard/mod.rs`
- Create: `tests/dashboard_source.rs`

**Interfaces:**
- Consumes: `Config`, `WorkersService`, `ClientStateStore::list_jobs`, `RemoteJobClient::status`, `RemoteJobClient::log_chunk`, `JobMeta`, `JobStatus`, `StatusResponse`, `LogChunkResponse`, and `ProbeResponse`.
- Produces: `MacWorkerDashboardSource`, `MacWorkerLogSource`, `DashboardDataSource` implementation, `DashboardLogSource` implementation, `project_job`, `project_worker`, and `map_remote_error`.

- [x] **Step 1: Write failing typed-adapter tests**

Create `tests/dashboard_source.rs` using recording fakes for worker probes and `RemoteJobClient`. Prove the adapter:

- calls the typed probe service, not `worker workers` or any subprocess CLI;
- maps `ProbeResponse` fields to `DashboardWorker` without exposing `WorkerEntry.ssh`;
- queries authoritative status only for local records whose last state is accepted/running or whose remote uncertainty is non-none;
- preserves a typed remote `JOB_NOT_FOUND` as a bounded dashboard error;
- maps `LogChunkResponse` exactly without decoding/re-encoding bytes;
- never invokes lease acquire, submit, resolve-or-abandon, or cancellation methods.

```rust
#[test]
fn adapter_uses_authoritative_status_for_active_local_record() {
    let remote = RecordingRemote::status(job_id(), running_status());
    let source = fixture_source(remote, vec![local_record(JobState::Accepted)]);
    let jobs = source.authoritative_active_jobs(Duration::from_secs(20));
    assert_eq!(jobs[0].as_ref().unwrap().state, DashboardJobState::Running);
    assert_eq!(source.remote().status_calls(), vec![job_id()]);
    assert!(source.remote().mutating_calls().is_empty());
}
```

- [x] **Step 2: Run adapter tests to verify they fail**

Run: `cargo test --locked --test dashboard_source -- --nocapture`

Expected: FAIL because `MacWorkerDashboardSource` and the completed Phase 3 remote-client interfaces are absent.

- [x] **Step 3: Implement direct-library projection adapters**

Define constructor dependencies as traits so tests remain fake-driven:

```rust
pub struct MacWorkerDashboardSource {
    pub config: Arc<Config>,
    pub workers: Arc<dyn DashboardWorkerReader>,
    pub local_jobs: Arc<ClientStateStore>,
    pub remote: Arc<dyn DashboardRemoteReader>,
    pub queue: Arc<dyn DashboardQueueReader>,
}
pub struct MacWorkerLogSource {
    pub config: Arc<Config>,
    pub local_jobs: Arc<ClientStateStore>,
    pub remote: Arc<dyn DashboardRemoteReader>,
}
pub trait DashboardWorkerReader: Send + Sync + 'static { fn inspect(&self, config: &Config) -> WorkersReport; }
pub trait DashboardRemoteReader: Send + Sync + 'static {
    fn status(&self, worker: &WorkerEntry, job_id: JobId) -> Result<StatusResponse, WorkerError>;
    fn log_chunk(&self, worker: &WorkerEntry, request: LogChunkRequest) -> Result<LogChunkResponse, WorkerError>;
}
```

Add exactly `pub mod source;` to `src/dashboard/mod.rs` in this task. Construct the pre-Phase-4 source with `queue: Arc::new(EmptyDashboardQueueReader)`; that makes the required snapshot `queue` an empty array without inventing scheduler semantics. The `Arc` fields make `MacWorkerDashboardSource: Send + Sync + 'static`, so it can satisfy Task 3's `DashboardDataSource` bound and can be owned by `Arc<DashboardService<MacWorkerDashboardSource, SystemClock>>` in Task 5. `project_job` derives only the fields stated in Task 1 from `JobMeta`/`JobStatus`, sets `project_label` and `artifact_status` to `None`, and maps `RemoteUncertainty` to its code only. `MacWorkerLogSource` resolves the worker only from a locally owned `JobId` record and config inventory name; it never accepts an HTTP worker/SSH value. Enforce remote status authority in this adapter; the service only merges its typed output.

- [x] **Step 4: Run source/service/web regression tests**

Run:

```bash
cargo test --locked --test dashboard_source --test dashboard_service --test dashboard_web -- --nocapture
cargo fmt --all --check
```

Expected: both commands exit `0`.

- [x] **Step 5: Commit the Phase-3 adapter**

```bash
git add src/dashboard/mod.rs src/dashboard/source.rs tests/dashboard_source.rs
git commit -m "feat: project typed job data for dashboard"
```

---

### Task 7: Phase-4 FIFO Queue Adapter

**Gate:** Start only after Phase 4 defines a durable locked FIFO queue with typed records, assignment states, capability requirements, and blocking reasons. Do not infer queue order from local job files or worker probes.

**Files:**
- Create: `src/dashboard/queue.rs`
- Modify: `src/dashboard/mod.rs`
- Create: `tests/dashboard_queue.rs`

**Interfaces:**
- Consumes: the Phase-4 queue service through the Task-3-owned read-only `DashboardQueueReader` trait.
- Produces: `SchedulerQueueAdapter` implementing `DashboardQueueReader` and stable FIFO `DashboardQueueEntry` values.

- [x] **Step 1: Write failing FIFO-adapter tests**

Create tests against a fake queue reader with four records. Assert exact sequence order is preserved, positions are one-based, abandoned records are absent, assigned/running records are absent from the queue list, and each entry contains only `job_id`, optional project label, IDs, content-free command summary, creation time, requirements, and the scheduler-provided blocking code. Assert that `worker_name`, `ssh`, command strings, lease tokens, and paths are absent.

```rust
#[test]
fn queue_adapter_preserves_scheduler_fifo_order_without_reimplementing_selection() {
    let adapter = SchedulerQueueAdapter::new(FakeQueue::ordered([first(), second()]));
    let queue = adapter.queue_entries().unwrap();
    assert_eq!(queue.iter().map(|entry| entry.position).collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(queue[0].blocking_code, "NO_COMPATIBLE_IDLE_WORKER");
}
```

- [x] **Step 2: Run queue tests to verify they fail**

Run: `cargo test --locked --test dashboard_queue -- --nocapture`

Expected: FAIL because Phase 4 queue types and `SchedulerQueueAdapter` are absent.

- [x] **Step 3: Implement the adapter with no scheduling logic**

Define:

```rust
pub struct PhaseFourQueueEntry {
    pub position: u32,
    pub job: DashboardJob,
    pub requirements: Vec<String>,
    pub blocking_code: String,
}
pub trait PhaseFourQueueReader: Send + Sync + 'static {
    fn ordered_pending(&self) -> Result<Vec<PhaseFourQueueEntry>, DashboardError>;
}
pub struct SchedulerQueueAdapter<R> { reader: R }
impl<R: PhaseFourQueueReader + Send + Sync + 'static> SchedulerQueueAdapter<R> {
    pub fn new(reader: R) -> Self;
}
impl<R: PhaseFourQueueReader + Send + Sync + 'static> DashboardQueueReader for SchedulerQueueAdapter<R> {
    fn ordered_pending(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError>;
}
```

Define `PhaseFourQueueReader` in this task as a private adapter trait returning `PhaseFourQueueEntry`; its production implementation is the only place that maps Phase-4 concrete queue records. Add exactly `pub mod queue;` to `src/dashboard/mod.rs`. The adapter only maps precomputed Phase-4 `position`, requirements, and blocking code. It must not choose workers, clean abandoned entries, reorder records, acquire capacity, or change queue state. Wire `Arc::new(SchedulerQueueAdapter::new(phase_four_queue_reader))` into `MacWorkerDashboardSource.queue` only after the adapter tests pass.

> Integration note: the merged Phase-4 contract currently exposes `ClientStateStore::queue_snapshot()` but does not expose scheduler-computed blocking reasons or a complete read-only producer projection. The production wiring is therefore deferred until that accessor exists; this task supplies the safe pass-through seam without inferring queue state from job files or probes.

- [x] **Step 4: Run queue and snapshot integration tests**

Run:

```bash
cargo test --locked --test dashboard_queue --test dashboard_service --test dashboard_web -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
```

Expected: both commands exit `0`.

- [x] **Step 5: Commit the queue projection**

```bash
git add src/dashboard/mod.rs src/dashboard/queue.rs tests/dashboard_queue.rs
git commit -m "feat: project scheduler queue in dashboard"
```

---

### Task 8: Coordinated CPU Probe Adapter

**Gate:** Start only after the Phase 4 probe-contract task has merged its single coordinated protocol bump. That owner must add both agreed optional facts together: `ProbeResponse::cpu_counters: Option<ProbeCpuCounters>` with cumulative `total_ticks`/`idle_ticks`, and `ProbeResponse::available_memory_bytes: Option<u64>`. The dashboard plan does not edit `src/protocol.rs`, `src/probe.rs`, `src/transport.rs`, protocol versions, or shared probe fixtures; it consumes that completed contract only.

**Files:**
- Modify: `src/dashboard/source.rs`
- Create: `tests/dashboard_cpu_adapter.rs`

**Interfaces:**
- Consumes: the Phase-4-owned `ProbeResponse::cpu_counters` and `ProbeCpuCounters { total_ticks, idle_ticks }` contract, Task 2's `Observation::cpu_counters`, and the Task 3 rule that the service alone calls `ObservationCache::record`.
- Produces: `cache_counters` and current source observations carrying optional cumulative counters; nullable `DashboardWorker.system.cpu_busy_percent` remains derived entirely by the service-owned MacBook cache.

- [x] **Step 1: Write failing adapter tests against the coordinated probe fixture**

Create `tests/dashboard_cpu_adapter.rs` using a typed `ProbeResponse` fixture supplied by the completed Phase 4 contract and the real `DashboardService` over a sequential fake/current source adapter. On the first current observation, assert `cpu_busy_percent` is null; on the second, assert counters `(1000,200)` then `(1100,220)` produce `80.0`; counter reset `(900,180)` produces null; `cpu_counters: None` produces null while preserving disk, memory-pressure, swap, and availability fields. Assert each probe contributes exactly one cache sample, the source has no cache reference, and `available_memory_bytes` is ignored by this dashboard version rather than creating a competing dashboard-specific memory metric.

```rust
#[test]
fn dashboard_computes_cpu_only_from_two_current_coordinated_probe_samples() {
    let source = sequential_probe_source([
        probe_with_counters(1_000, 200),
        probe_with_counters(1_100, 220),
    ]);
    let service = DashboardService::new(source, stepped_clock([1_000, 3_000]));
    assert_eq!(service.snapshot(Default::default()).unwrap().workers[0].system.cpu_busy_percent, None);
    assert_eq!(service.snapshot(Default::default()).unwrap().workers[0].system.cpu_busy_percent, Some(80.0));
}
```

- [x] **Step 2: Run CPU adapter tests to verify they fail**

Run: `cargo test --locked --test dashboard_cpu_adapter --test dashboard_cache -- --nocapture`

Expected: FAIL because the Phase-4 coordinated probe contract and dashboard adapter mapping are absent.

- [x] **Step 3: Implement MacBook-side mapping only**

Define in `src/dashboard/source.rs`:

```rust
pub fn cache_counters(counters: crate::protocol::ProbeCpuCounters) -> Option<crate::dashboard::cache::CpuCounters>;
```

`cache_counters` validates and copies the two cumulative fields without calculating a percentage, returning `None` for an invalid pair. Extend the existing current-probe projection so it maps the optional wire counters through this function into the single `Observation` returned to `DashboardService`. The source must not own, borrow, or call `ObservationCache`; the service's one `record` call computes and writes the delta percentage. Never calculate a percentage on a worker, sleep to obtain a second sample, persist samples, double-record one probe, or treat missing/invalid CPU counters as a failed worker.

- [x] **Step 4: Run adapter/source/cache regressions**

Run:

```bash
cargo test --locked --test dashboard_cpu_adapter --test dashboard_source --test dashboard_cache -- --nocapture
cargo fmt --all --check
```

Expected: both commands exit `0`.

- [x] **Step 5: Commit the adapter-only CPU projection**

```bash
git add src/dashboard/source.rs tests/dashboard_cpu_adapter.rs
git commit -m "feat: project coordinated worker cpu metrics"
```

---

### Task 9: Dashboard CLI Lifecycle and Browser Opening

**Gate:** Start after Tasks 5–6 are integrated on a branch based on `main` after Phase 3. Tasks 7–8 remain gated on Phase 4 and do not block this phase-3 dashboard CLI lifecycle. Because this task changes the active CLI/parser/dispatch files, rebase first and resolve changes in favor of the Phase 3 public command orchestration.

**Files:**
- Create: `src/dashboard/command.rs`
- Modify: `src/dashboard/mod.rs`
- Modify: `src/cli.rs`
- Modify: `src/lib.rs`
- Create: `tests/dashboard_command.rs`
- Modify: `tests/cli_help.rs`

**Interfaces:**
- Consumes: `DashboardHttpServer::bind`, `DashboardHttpServer::local_url`, `DashboardHttpServer::shutdown`, completed `MacWorkerDashboardSource`, and `MacWorkerLogSource`.
- Produces: `DashboardCommandRequest`, `DashboardRunResult`, `DashboardLauncher`, `SystemDashboardLauncher`, `SystemBrowserOpener`, public `Command::Dashboard { port, no_open }`, and `run_dashboard`.

- [x] **Step 1: Write failing command/lifecycle tests**

Add CLI parsing tests for exactly:

```text
worker dashboard
worker dashboard --port 9173
worker dashboard --no-open
worker dashboard --port 9173 --no-open
```

Reject `--port 0`, `--port 65536`, duplicate ports, and positional values. With a fake launcher/opener, prove the command binds loopback, prints the exact URL, opens it only without `--no-open`, treats opener failure as a bounded warning while the server remains usable, and on shutdown leaves fake source mutation counts at zero.

```rust
#[tokio::test]
async fn no_open_prints_loopback_url_without_invoking_browser() {
    let result = run_dashboard(DashboardCommandRequest { port: None, no_open: true }, &fake_launcher(), &fake_opener(), Box::pin(async {}), &mut output(), &mut warnings()).await.unwrap();
    assert!(result.url.starts_with("http://127.0.0.1:"));
    assert_eq!(fake_launcher().open_calls(), 0);
}
```

- [x] **Step 2: Run command tests to verify they fail**

Run: `cargo test --locked --test dashboard_command --test cli_help -- --nocapture`

Expected: FAIL because the public dashboard command does not exist.

- [x] **Step 3: Implement the ephemeral command lifecycle**

Add to `src/cli.rs`:

```rust
Dashboard {
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..=65535))]
    port: Option<u16>,
    #[arg(long)]
    no_open: bool,
},
```

Define:

```rust
pub struct DashboardCommandRequest { pub port: Option<u16>, pub no_open: bool }
pub struct DashboardRunResult { pub url: String }
pub struct SystemDashboardLauncher { pub state: Arc<DashboardHttpState<MacWorkerDashboardSource, SystemClock>> }
pub trait DashboardLauncher: Send + Sync {
    fn launch<'a>(&'a self, request: DashboardCommandRequest) -> Pin<Box<dyn Future<Output = Result<DashboardHttpServer, WorkerError>> + Send + 'a>>;
}
pub trait BrowserOpener { fn open(&self, url: &str) -> Result<(), WorkerError>; }
pub async fn run_dashboard(request: DashboardCommandRequest, launcher: &dyn DashboardLauncher, opener: &dyn BrowserOpener, shutdown_signal: Pin<Box<dyn Future<Output = ()> + Send>>, stdout: &mut dyn Write, stderr: &mut dyn Write) -> Result<DashboardRunResult, WorkerError>;
```

Add exactly `pub mod command;` to `src/dashboard/mod.rs` in this task. `SystemDashboardLauncher::launch` returns `Box::pin(DashboardHttpServer::bind(request.port, Arc::clone(&self.state)))`, so it does not borrow command-local state across the asynchronous server lifetime. `SystemBrowserOpener` invokes `/usr/bin/open` with a single validated loopback URL. It never accepts a browser executable, URL, or arbitrary path from HTTP input. In the synchronous `src/lib.rs` dispatch branch, build a Tokio runtime with `tokio::runtime::Builder::new_multi_thread().enable_all().build()?` and invoke `runtime.block_on(run_dashboard(request, &launcher, &opener, Box::pin(tokio::signal::ctrl_c()), stdout, stderr))`. `run_dashboard` writes and flushes the URL, awaits the injected shutdown signal, then awaits `DashboardHttpServer::shutdown`. This is the exact async/sync boundary; the `tokio` `signal` feature in Task 5 is required for the production `ctrl_c` future. Do not add it to `--json` streaming output and do not make a browser-launch warning an execution failure.

- [x] **Step 4: Run CLI and dashboard regression tests**

Run:

```bash
cargo test --locked --test dashboard_command --test dashboard_web --test dashboard_source --test cli_help -- --nocapture
cargo build --locked --release
```

Expected: both commands exit `0`.

- [x] **Step 5: Commit public dashboard startup**

```bash
git add src/dashboard/command.rs src/dashboard/mod.rs src/cli.rs src/lib.rs tests/dashboard_command.rs tests/cli_help.rs
git commit -m "feat: add loopback dashboard command"
```

---

### Task 10: Three-Worker Live Acceptance and Documentation

**Gate:** Start only after Tasks 5–9 are integrated on `main` after Phase 3, all three helpers run the Phase-4-coordinated probe contract consumed by Task 8, and Phase 4 FIFO scheduling is integrated. This is an operational validation task, not a substitute for fake-source tests.

**Files:**
- Modify: `README.md`
- Create: `docs/dashboard-validation.md`
- Modify: `tests/dashboard_web.rs`
- Modify: `tests/dashboard_command.rs`

**Interfaces:**
- Consumes: completed public `worker dashboard`, all dashboard HTTP routes, completed three-worker scheduler, status/log adapters, and CPU counters.
- Produces: documented sanitized live acceptance evidence and user-facing dashboard usage.

- [ ] **Step 1: Write final automated regression cases**

Extend HTTP/command tests to prove browser requests, closing a client connection, and server shutdown do not call fake submit/cancel/retry/delete/lease methods. Add explicit refresh-coalescing coverage with two HTTP clients. Add fixture cases for one stale worker plus two current workers, a queue entry blocked by no compatible idle worker, terminal logs continued to exact final byte lengths, and a `project_label: null` fallback.

- [ ] **Step 2: Run final automated dashboard suite**

Run:

```bash
node --test tests/dashboard_client.mjs
cargo test --locked --test dashboard_model --test dashboard_cache --test dashboard_service --test dashboard_web --test dashboard_source --test dashboard_queue --test dashboard_command -- --nocapture
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
git diff --check
```

Expected: all commands exit `0`.

- [ ] **Step 3: Add README operation and safety guidance**

Document these exact commands and constraints:

```bash
worker dashboard
worker dashboard --no-open
worker dashboard --port 9173
```

State that it binds `127.0.0.1` only, is read-only and ephemeral, polls rather than pushes, keeps no metrics database, does not expose command/environment/path secrets, may show application log secrets to the local user, and does not replace CLI operations.

- [ ] **Step 4: Run sanitized live acceptance on three workers**

With an isolated project and sanitized command labels, verify and record in `docs/dashboard-validation.md`:

1. Start `worker dashboard --no-open`; verify one loopback URL and no worker-side listener.
2. Confirm all three idle workers appear within five seconds with current observation timestamps.
3. Submit three known short jobs through the CLI; confirm three busy cards and matching job/project short IDs.
4. Submit a fourth compatible job; confirm its Phase-4 FIFO position and scheduler-provided blocking code.
5. Refresh an active job detail; confirm stdout/stderr resume at exact offsets without duplicated bytes.
6. Temporarily disable SSH to one worker; confirm only its card becomes stale/offline and the remaining cards keep refreshing.
7. Compare each terminal state and result with `worker status <job-id>`.
8. Stop the dashboard; confirm all jobs remain queryable and no job/lease state changed due to the dashboard.
9. Attempt a request through a non-loopback listener and verify none exists.

Record only commit ID, protocol version, shortened IDs, state transitions, counts, timings, and test outcomes. Do not record full paths, SSH destinations, source data, environment values, or raw logs.

- [ ] **Step 5: Run the full repository gate**

Run:

```bash
cargo test --locked --all-targets
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked --release
git diff --check
```

Expected: all commands exit `0`.

- [ ] **Step 6: Commit validation and documentation**

```bash
git add README.md docs/dashboard-validation.md tests/dashboard_web.rs tests/dashboard_command.rs
git commit -m "test: validate local dashboard"
```

## Self-Review

### Spec coverage

- Read-only observer, direct library calls, no separate daemon/control plane: Tasks 1, 3, 5, 6, and 9; mutation-count tests in Task 10.
- Three-worker health, slot use, active job, disk/memory/swap/capabilities, current/stale/offline behavior: Tasks 1–3 and 6.
- FIFO queue and scheduler semantics: Task 7, explicitly gated on the only authoritative producer.
- Active/recent jobs, durable status authority, logs, byte ranges and reconnect: Tasks 1, 5, and 6.
- CPU delta math and no probe sleep: Tasks 2 and 8.
- Null artifacts and optional project labels/fallback IDs: Task 1 and source tests in Task 6.
- Loopback bind, embedded assets, polling, headers, Host validation, no CORS, safe text rendering: Tasks 4, 5, and 9.
- Partial collection, cache bounds, coalescing, acceptance and shutdown behavior: Tasks 2, 3, 5, and 10.

### Placeholder scan

The plan contains no `TODO`, `TBD`, “implement later”, “appropriate error handling”, or cross-task “similar to” instructions. Each gated task states the exact prerequisite, files, tests, commands, interface, and commit.

### Type consistency

- `DashboardSnapshot`, `DashboardJob`, `DashboardQueueEntry`, `DashboardLogChunk`, and `ApiError` originate in Task 1 and are used consistently by Tasks 3–10.
- `DashboardDataSource` originates in Task 3; `DashboardLogSource` originates in Task 5; `MacWorkerDashboardSource` and `MacWorkerLogSource` implement them in Task 6.
- `CpuCounters` originates in Task 2 as a dashboard-cache input. Task 8 consumes the Phase-4-owned coordinated probe contract and converts `ProbeCpuCounters` into it; this plan owns no protocol bump or shared probe field.
- `DashboardQueueReader` originates in Task 3; Task 7's `SchedulerQueueAdapter` implements it, preventing the dashboard service from depending on a Phase-4 concrete store.
