# Three-Worker Scheduler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `worker run` select an eligible Mac mini automatically, wait in a durable local FIFO queue, and provide explicit cancellation plus fleet-wide reconciliation without weakening the durable single-worker lifecycle.

**Architecture:** Phase 3 host state remains authoritative for leases, accepted jobs, process groups, and terminal outcomes. The new local scheduler owns only pending queue entries beneath the locked client-state root; after selection and atomic lease acquisition it delegates immutable snapshot upload and submission to Phase 3 `RunService`. Queue cancellation is local until acceptance; remote cancellation uses a fixed typed host operation and the supervisor's exact cleanup/release path.

**Tech Stack:** Rust 2024, existing `serde`/`serde_json` canonical records, `libc` locks and descriptor-relative filesystem operations, OpenSSH through `ProcessRunner`, and existing `proptest`/`tempfile`/`assert_cmd` test support.

**Spec:** `docs/superpowers/specs/2026-08-25-mac-worker-design.md` sections 6--13 and 17--21; `docs/superpowers/plans/2026-08-27-single-worker-execution.md` Tasks 8--10.

## Global Constraints

- Task 1 is independently startable now on `8acab2d`: it is a pure scheduler-owned policy with no Phase 3, probe, protocol, filesystem, clock, SSH, or process dependency. Tasks 2 and later begin only after Phase 3 Tasks 8--10 land and pass their live gate.
- Automatic selection is default. `--worker NAME` is a configured-name pin for diagnosis, never a raw SSH destination.
- Every worker has one heavy slot. Fresh probes are advisory; the existing atomic host lease is the only admission authority.
- The MacBook is the source of truth. Upload only a verified immutable `Snapshot`; never sync a live worktree or source changes back.
- Queue state is canonical owner-only JSON below `PathLayout::state`, protected by the existing cross-process lock and no-follow filesystem layer. No daemon/database is introduced.
- Phase 5 consumes the queue fields and admission semantics defined by this plan. Queue schemas are canonical JSON with strict unknown-field rejection, like every other persisted record; later task work must not need to migrate or reinterpret Phase 4 rows.
- Queue records hold only IDs, sanitized command summary, requirements, worker preference, timestamps, owner identity, and bounded error codes—never argv/shell, environment values, paths, snapshot paths, or lease tokens.
- FIFO is immutable enqueue order and means per-worker claim/admission consideration order, not wall-clock child-start order. A queue row is persistently `Waiting` or `Dispatching`; an identity-proven dead batch dispatcher is recovered to `Waiting`, never silently deleted, while task-turn ownership follows the binding amendment below. Multiple distinct-worker dispatches may be in flight to fill three slots.
- Eligible means a ready/protocol/capability-compatible idle admission observation plus successful atomic lease. Admission observations are cached and advisory; the atomic host lease is authoritative. Ranking is worktree affinity, project affinity, greatest available memory, greatest free disk, lexical worker name; affinity never overrides eligibility.
- `--no-wait` returns `CAPACITY_BUSY` without queue entry, snapshot, lease, or remote mutation.
- Once remotely accepted, resolve every ambiguity with its original job/client/token/fingerprint through Phase 3 `resolve-or-abandon`; never create a replacement job.
- Cancellation is explicit. Ctrl-C or log-follow disconnect never cancels an accepted job.
- Remote cancellation targets only the recorded process group: TERM, ten-second wait, KILL if needed, proof of absence, atomic `cancelled`, job-owned cleanup, then exact lease release. No broad process kill.
- Fleet reconciliation issues bounded typed per-job requests for known IDs. A failed probe is never evidence a retained job is gone.
- Dashboard/watch, artifacts/fetch, caches/env profiles, Docker, retention/general GC, notifications, and Phase 5 acceptance work are out of scope.

### Binding Phase 5 queue compatibility amendments

The agent-task pool design supersedes the otherwise-conflicting queue wording in Tasks 3, 4, and 6 below. Phase 4 implements this durable compatibility surface; Phase 5 consumes it without a queue migration.

- `QueueEntry` has a canonical `kind` (`batch` or `task_turn`) and optional run reference (`RunId`, an opaque validated string, and `max_parallel`). A `task_turn` has a replaceable `ProcessIdentity` owner while `Waiting` and while `Dispatching`; enqueue records the initial owner and `adopt_row(job_id, new_owner)` rewrites it. Batch rows retain the Phase 3/4 ownership meaning: the invoking CLI process is the row owner.
- Dead-owner reaping applies only to `batch` rows. The scheduler never removes or reaps a dead-owner `task_turn`; `worker run` skips such rows so a later Phase 5 mutating task command can re-own the row.
- `claim_next(owner, ranked_workers, now)` is owner-scoped and per-worker FIFO. For each ranked idle worker that is not already selected by a live dispatch, it may claim the caller-owned row only when that row is eligible for the worker (kind requirements, pin, and run cap) and no older uncancelled `Waiting` row with a live owner is eligible for the same worker. Otherwise it returns `None`. Rows ineligible for every idle worker do not block younger rows for other workers. For batches, `owner` is the calling CLI process, preserving Phase 4 batch behaviour except that a pinned or capability-blocked head no longer blocks unrelated workers.
- The run cap is evaluated inside `claim_next` under the queue lock from local state only: count sibling rows in `Dispatching` plus same-run records durably accepted and not terminal in the local job registry. A successful claim consumes a slot until reversion or terminal completion. No SSH, probe, or remote status operation may run while this lock is held.
- Admission observations use a shared per-worker cache below the local state root at `observations/<worker>.json`. It is canonical, owner-only JSON with strict decoding. A cache entry has a two-second TTL; the first dispatcher finding it stale refreshes it under a per-worker single-flight marker, while other dispatchers use the cached observation and its age. The dashboard may later read this cache read-only.
- A waiting `WorkerPreference::Pinned` row waits only for its named worker and is never rerouted. Under per-worker FIFO it therefore does not delay work eligible for another idle worker.

---

## File Map

```text
src/protocol.rs                 one Phase 4 probe protocol bump: memory and CPU counter DTOs
src/probe.rs                    bounded macOS available-memory and cumulative CPU collection
src/scheduler.rs                pure scheduler-owned observations, eligibility, and ranking
src/scheduler_adapter.rs        post-Phase-3 projection from WorkerHealth/ProbeResponse to policy facts
src/client_state.rs             descriptor-bound queue persistence, affinity history, and shared admission observations
src/job.rs                      QueueId/QueueEntry and cancellation records
src/job_service.rs              host cancellation and exact-job reconciliation entry points
src/transfer.rs                 fixed cancel/reconcile host operations and remote client helpers
src/run.rs                      Phase 3 RunService integration: enqueue, wait/no-wait, dispatch
src/cli.rs                      worker preference/no-wait/cancel grammar and hidden host commands
src/lib.rs                      host stdio dispatch and streaming public commands
src/output.rs                   sanitized run/queue/cancel reports
tests/scheduler_policy.rs       ranking and proptest properties
tests/scheduler_adapter.rs      typed probe projection and protocol-v3 fixtures
tests/scheduler_queue.rs        queue durability/FIFO/owner lifecycle
tests/run_command.rs            automatic/pinned/no-wait orchestration
tests/job_queries.rs            host cancellation/reconciliation operations
tests/fleet_reconciliation.rs   partial-fleet recovery
tests/scheduler_concurrency.rs  races, duplicate-execution, and privacy matrices
docs/phase-four-validation.md   sanitized three-Mac validation evidence
README.md                       Phase 4 usage and explicit remaining boundary
```

### Task 1: Pure Deterministic Candidate-Ranking Contract

**Files:**
- Create: `src/scheduler.rs`
- Modify: `src/lib.rs`
- Create: `tests/scheduler_policy.rs`

**Interfaces:**
- Consumes: only scheduler-owned strings, booleans, `CandidateSlot`, optional numeric facts, and requirement strings.
- Produces: `CandidateObservation`, `AffinityHints`, `CandidateRejection`, `RankedCandidate`, `Selection`, `WorkerPreference`, and `SchedulerPolicy::{rank,select}`.

- [ ] **Step 1: Write failing probe and ranking tests**

```rust
#[test]
fn ranks_worktree_affinity_before_more_memory_and_disk() {
    let ranked = SchedulerPolicy::rank(&observations(), &["node".into()],
        &AffinityHints { worktree_worker: Some("mini-2".into()), project_worker: Some("mini-1".into()) });
    assert_eq!(names(&ranked), vec!["mini-2", "mini-1", "mini-3"]);
}

#[test]
fn rejects_busy_offline_or_missing_capability() {
    assert!(matches!(SchedulerPolicy::select(&mixed_observations(), &["ruby".into()], &WorkerPreference::Automatic, &AffinityHints::none()),
        Selection::NoEligible { rejections } if rejections.len() == 4));
}

proptest! {
    #[test]
    fn ranking_is_deterministic_and_unique(input in arbitrary_health_sets()) {
        let a = SchedulerPolicy::rank(&input, &[], &AffinityHints::none());
        let b = SchedulerPolicy::rank(&input, &[], &AffinityHints::none());
        prop_assert_eq!(a, b); prop_assert!(has_unique_names(&a));
    }
}
```

Test a missing memory fact, duplicate worker identity, an unavailable observation, an occupied slot, missing capabilities, affinity, disk ties, and pin filtering. A missing memory fact sorts below every known fact but does not itself make a candidate ineligible.

- [ ] **Step 2: Run focused tests to verify RED**

Run: `cargo test --locked --test scheduler_policy -- --nocapture`

Expected: FAIL because scheduler-owned observation and policy types do not exist.

- [ ] **Step 3: Implement the pure policy**

Create `src/scheduler.rs`; it must not import `protocol`, `probe`, `transport`, `Config`, or `WorkerEntry`. Add exactly `pub mod scheduler;` to `src/lib.rs`; this is Task 1's only start-now shared-file edit and is an isolated one-line rebase conflict with later module-export work:

```rust
pub enum CandidateSlot { Idle, Busy }
pub enum CandidateObservationError { InvalidWorkerName, DuplicateCapability { capability: String } }
pub struct CandidateObservation { worker_name: String, ready: bool, slot: CandidateSlot, capabilities: Vec<String>, available_memory_bytes: Option<u64>, free_disk_bytes: u64 }
impl CandidateObservation { pub fn new(worker_name: String, ready: bool, slot: CandidateSlot, capabilities: Vec<String>, available_memory_bytes: Option<u64>, free_disk_bytes: u64) -> Result<Self, CandidateObservationError>; pub fn worker_name(&self) -> &str; pub fn available_memory_bytes(&self) -> Option<u64>; }
pub struct AffinityHints { pub worktree_worker: Option<String>, pub project_worker: Option<String> }
impl AffinityHints { pub fn none() -> Self; }
pub enum WorkerPreference { Automatic, Pinned { worker: String } }
pub enum CandidateRejection { DuplicateIdentity { name: String }, Unavailable { name: String }, MissingCapabilities { name: String, missing: Vec<String> }, Busy { name: String } }
pub struct RankedCandidate { observation: CandidateObservation }
impl RankedCandidate { pub fn worker_name(&self) -> &str; }
pub enum Selection { Selected(RankedCandidate), NoEligible { rejections: Vec<CandidateRejection> } }
pub struct SchedulerPolicy;
impl SchedulerPolicy {
    pub fn rank(observations: &[CandidateObservation], requirements: &[String], affinity: &AffinityHints) -> Vec<RankedCandidate>;
    pub fn select(observations: &[CandidateObservation], requirements: &[String], preference: &WorkerPreference, affinity: &AffinityHints) -> Selection;
}
```

Reject duplicate names. Require ready, idle, and every requirement. Sort by one explicit tuple: affinity class, reverse known-memory class/value, reverse disk, name; report rejections in lexical worker order. The later adapter is solely responsible for proving that observation identity came from a configured worker.

- [ ] **Step 4: Run focused policy/probe regression tests**

Run: `cargo test --locked --test scheduler_policy -- --nocapture`

Expected: PASS; this deliverable compiles/runs without any Phase 3, host-probe, or client-state change.

- [ ] **Step 5: Commit the independent policy**

```bash
git add src/lib.rs src/scheduler.rs tests/scheduler_policy.rs
git commit -m "feat: rank compatible scheduler candidates"
```

---

### Task 2: Shared Protocol-v3 Probe Adapter

**Gate:** Start only after the Phase 3 Task 10 commit, its full local gate, and its sanitized one-worker live acceptance. This is the sole Phase 4/4.5 probe wire-format change.

**Files:**
- Modify: `src/protocol.rs`
- Modify: `src/probe.rs`
- Modify: `src/transport.rs`
- Create: `src/scheduler_adapter.rs`
- Create: `tests/scheduler_adapter.rs`
- Modify: `tests/workers_command.rs`
- Modify: `tests/doctor_command.rs`
- Modify: `tests/setup_command.rs`
- Modify: `tests/job_protocol.rs`

**Interfaces:**
- Consumes: Task 1 `CandidateObservation`, current `WorkerHealth`/`ProbeResponse`, and configured `WorkerEntry` identities.
- Produces: protocol version `3`, `CpuCounters`, `ProbeResponse::{available_memory_bytes,cpu_counters}`, and `SchedulerProbeAdapter::observations`.

- [ ] **Step 1: Write failing protocol-v3 and adapter tests**

```rust
#[test]
fn adapter_projects_only_matching_ready_inventory_probe_into_policy_fact() {
    let facts = SchedulerProbeAdapter::observations(&config(), &worker_healths()).unwrap();
    assert_eq!(facts[0].worker_name(), "mini-1");
    assert_eq!(facts[0].available_memory_bytes(), Some(12 * GIB));
}

#[test]
fn protocol_v2_probe_is_ineligible_after_the_single_v3_bump() {
    let health = protocol_v2_worker_health();
    assert!(matches!(SchedulerProbeAdapter::observations(&config(), &[health]), Err(WorkerError::Protocol(_))));
}
```

Test canonical protocol-v3 fixtures in workers/Doctor/setup/job-protocol suites; valid/missing memory; valid CPU tick counters; counter overflow; malformed `vm_stat` and `host_statistics64(HOST_CPU_LOAD_INFO)` results; inventory/health name or SSH mismatch; and the absence of a probe. Assert that both new fact groups are added by one version change, never two.

- [ ] **Step 2: Run adapter tests to verify RED**

Run: `cargo test --locked --test scheduler_adapter --test workers_command --test doctor_command --test setup_command --test job_protocol -- --nocapture`

Expected: FAIL because protocol 3 fields and the adapter do not exist.

- [ ] **Step 3: Add the one probe wire-version change**

Set `PROTOCOL_VERSION` to `3` while retaining `SUPERVISION_VERSION == 2`. Add these exact DTOs to `src/protocol.rs`:

```rust
pub struct CpuCounters { pub user_ticks: u64, pub system_ticks: u64, pub idle_ticks: u64, pub nice_ticks: u64 }
pub struct ProbeResponse {
    // existing fields unchanged
    pub available_memory_bytes: Option<u64>,
    pub cpu_counters: Option<CpuCounters>,
}
```

Use `#[serde(default)]` only for field decoding; protocol-2 helpers remain ineligible because the version field is now `3`. In `probe.rs`, compute available memory from bounded `vm_stat` `(Pages free + Pages speculative) * page_size`; collect cumulative user/system/idle/nice CPU ticks with bounded macOS `host_statistics64(HOST_CPU_LOAD_INFO)`; return `None` for unavailable/invalid facts, never fabricate a zero, and never sleep to obtain a sample. Update every literal probe/setup/Doctor fixture to protocol 3.

The later Phase 4.5 dashboard consumes `CpuCounters` through this adapter and computes deltas locally; it must not modify `ProbeResponse`, `PROTOCOL_VERSION`, or host probe collection. This task is the only planned version bump for both scheduler memory and dashboard CPU facts.

- [ ] **Step 4: Implement the typed projection**

```rust
pub struct SchedulerProbeAdapter;
impl SchedulerProbeAdapter {
    pub fn observations(config: &Config, health: &[WorkerHealth]) -> Result<Vec<CandidateObservation>, WorkerError>;
}
```

Require one matching configured worker entry per health name and matching SSH identity; turn unavailable/no-probe into `CandidateObservation::new(..., false, CandidateSlot::Busy, Vec::new(), None, 0)`. For a ready probe, map `SlotState::{Idle,Busy}` to `CandidateSlot::{Idle,Busy}`, then map capabilities, memory, and disk. The adapter does not rank, mutate state, acquire a lease, or calculate CPU percentage.
Map an impossible `CandidateObservationError` from already validated probe text to `WorkerError::Protocol("scheduler probe projection is invalid")`; do not disclose probe payloads.

- [ ] **Step 5: Run the protocol/adapter regression suite**

Run: `cargo test --locked --test scheduler_policy --test scheduler_adapter --test workers_command --test doctor_command --test setup_command --test job_protocol -- --nocapture`

Expected: PASS; all protocol fixtures say `3`, all protocol-2 helpers are excluded, and dashboard CPU data requires no later protocol change.

- [ ] **Step 6: Commit the shared adapter**

```bash
git add src/protocol.rs src/probe.rs src/transport.rs src/scheduler_adapter.rs tests/scheduler_adapter.rs tests/workers_command.rs tests/doctor_command.rs tests/setup_command.rs tests/job_protocol.rs
git commit -m "feat: expose scheduler and dashboard probe facts"
```

---

### Task 3: Durable Queue Contract and Canonical Queue Persistence

**Gate:** Start after Phase 3 Task 10's full/live gate and Task 2's protocol-v3 adapter commit; it may not change Phase 3 job/orchestration records before that gate.

**Files:**
- Modify: `src/job.rs`
- Modify: `src/client_state.rs`
- Modify: `src/error.rs`
- Create: `tests/scheduler_queue.rs`
- Modify: `tests/client_state.rs`

**Interfaces:**
- Consumes: Phase 3 `JobId`, `ClientId`, `CommandSummary`, `ProcessIdentity`, `ClientStateStore`, Task 1 `WorkerPreference`/`AffinityHints`, and Task 2 configured candidate projections.
- Produces: `QueueId`, validated opaque `RunId`, `QueueEntryKind`, `QueueRunReference`, `QueueState`, `QueueEntry`, `QueueSnapshot`, `QueueClaim`, `QueueCancel`, canonical admission observations, and `ClientStateStore::{enqueue,queue_snapshot,adopt_row,claim_next,revert_dispatch,request_queue_cancel,remove_after_terminal,recover_dead_dispatches,remove_queued,record_affinity,affinity_hints}`.

- [ ] **Step 1: Write failing queue schema and persistence tests**

After the stated Phase 3 gate and Task 2 adapter commit, create these tests and extend them for canonical JSON/mode/containment:

```rust
#[test]
fn claim_marks_oldest_eligible_waiting_entry_dispatching_without_removing_it() {
    let store = open_queue();
    let first = queued("00000000000000000000000000000001", 10);
    let second = queued("00000000000000000000000000000002", 11);
    store.enqueue(first.clone()).unwrap(); store.enqueue(second).unwrap();
    let claim = store.claim_next(dispatcher(), &["mini-1".into()], 13).unwrap();
    assert_eq!(claim.entry().job_id(), first.job_id());
    assert!(matches!(claim.entry().state(), QueueState::Dispatching { selected_worker, .. } if selected_worker == "mini-1"));
    assert_eq!(store.queue_snapshot().unwrap().entries().len(), 2);
}

#[test]
fn live_or_ambiguous_owner_is_never_reaped_as_abandoned() {
    let store = open_queue_with_owner_inspector(AmbiguousOwner);
    store.enqueue(queued("00000000000000000000000000000003", 12)).unwrap();
    assert_eq!(store.queue_snapshot().unwrap().entries().len(), 1);
}
```

Cover duplicate job/sequence IDs, stale lock state, malformed/noncanonical JSON (including unknown fields), symlink/FIFO/device replacement, interrupted atomic write, simultaneous enqueue, crash after retirement rename, PID reuse, cancel racing `claim_next`, and lease-busy/failed-preacceptance reversion. Prove a dead batch dispatcher is first persisted back to `Waiting`, not removed, and a dead-owner `task_turn` row survives unchanged for Phase 5 re-ownership. Prove three rows can be `Dispatching` only when their selected workers are distinct. Add explicit owner-scoped claim tests: an owner cannot claim another owner's row, an older live-owned row wins for the same worker, a busy pinned head does not block a younger row eligible for another worker, and an ineligible head blocks no unrelated worker. Add a two-dispatcher race proving the local run cap admits at most its limit, and a single-flight two-second observation-cache refresh test.

- [ ] **Step 2: Run queue tests to verify RED**

Run: `cargo test --locked --test scheduler_queue --test client_state -- --nocapture`

Expected: FAIL because queue records and queue methods do not exist.

- [ ] **Step 3: Define validated queue records**

Add strict manual serialization/validation in `src/job.rs`:

```rust
pub struct QueueId(u64);
pub struct RunId(String); // opaque, validated canonical string
pub enum QueueEntryKind { Batch, TaskTurn }
pub struct QueueRunReference { run_id: RunId, max_parallel: u32 }
pub enum QueueState {
    Waiting { owner: ProcessIdentity },
    Dispatching { dispatch_owner: ProcessIdentity, selected_worker: String, claimed_at_millis: u64 },
}
pub struct QueueEntry {
    queue_id: QueueId, job_id: JobId, client_id: ClientId, project_id: String,
    worktree_id: String, command_summary: CommandSummary, requirements: Vec<String>,
    preference: WorkerPreference, kind: QueueEntryKind, run: Option<QueueRunReference>,
    enqueue_owner: ProcessIdentity, state: QueueState,
    cancel_requested_at_millis: Option<u64>, enqueued_at_millis: u64,
}
impl QueueEntry { pub fn job_id(&self) -> JobId; pub fn kind(&self) -> QueueEntryKind; pub fn run(&self) -> Option<&QueueRunReference>; pub fn owner(&self) -> &ProcessIdentity; pub fn state(&self) -> &QueueState; pub fn is_cancel_requested(&self) -> bool; }
pub struct QueueSnapshot { next_id: QueueId, entries: Vec<QueueEntry> }
pub struct QueueClaim { entry: QueueEntry }
impl QueueClaim { pub fn entry(&self) -> &QueueEntry; }
pub enum QueueCancel { RemovedWaiting { job_id: JobId }, RequestedDispatch { job_id: JobId, dispatch_owner: ProcessIdentity } }
```

`QueueEntry::new` and `validate` reuse canonical identifiers and bounded lowercase capabilities. `kind` is exactly `batch` or `task_turn`; a run reference validates both its opaque ID and positive cap. A `task_turn` owns its row in both persistent states, and `adopt_row` may replace that owner only while it is `Waiting`; enqueue records the initial owner. Batch ownership remains the invoking CLI process. Records must not contain exact command data, environment values, local paths, manifest digest, snapshot path, lease token, prompt text, or Phase 5 session data. `QueueSnapshot::validate` requires monotonically increasing IDs/timestamps, unique dispatching worker names, and valid owner identities. It accepts persisted `Dispatching` so a crash can be recovered precisely.

- [ ] **Step 4: Implement descriptor-bound queue operations**

Create only `queue/state.json`, `queue/lock`, and owner-scoped retirement records below the existing local state root. Reuse `ClientStateStore`'s root lock, component-by-component creation, canonical staging/fsync/rename, and retirement cleanup. Implement:

```rust
pub fn enqueue(&self, entry: QueueEntry) -> Result<QueueEntry, WorkerError>;
pub fn queue_snapshot(&self) -> Result<QueueSnapshot, WorkerError>;
pub fn adopt_row(&self, job_id: JobId, new_owner: ProcessIdentity) -> Result<QueueEntry, WorkerError>;
pub fn claim_next(&self, owner: ProcessIdentity, ranked_workers: &[String], claimed_at_millis: u64) -> Result<Option<QueueClaim>, WorkerError>;
pub fn revert_dispatch(&self, job_id: JobId, dispatch_owner: ProcessIdentity) -> Result<QueueEntry, WorkerError>;
pub fn request_queue_cancel(&self, job_id: JobId, requested_at_millis: u64) -> Result<Option<QueueCancel>, WorkerError>;
pub fn remove_after_terminal(&self, job_id: JobId, dispatch_owner: ProcessIdentity) -> Result<QueueEntry, WorkerError>;
pub fn recover_dead_dispatches(&self) -> Result<Vec<JobId>, WorkerError>; // batch rows only
pub fn remove_queued(&self, job_id: JobId) -> Result<Option<QueueEntry>, WorkerError>;
pub fn record_affinity(&self, project_id: &str, worktree_id: &str, worker: &str, observed_at_millis: u64) -> Result<(), WorkerError>;
pub fn affinity_hints(&self, project_id: &str, worktree_id: &str) -> Result<AffinityHints, WorkerError>;
```

`claim_next` is owner-scoped and per-worker FIFO. For each ranked idle worker not selected by a live dispatch, it considers the caller's own uncancelled `Waiting` row; it claims that row only if it is eligible for that worker (requirements, pin, and local run cap) and no older uncancelled live-owned row is eligible for the same worker. It returns `None` when no caller-owned row meets those conditions. A pinned, capability-blocked, or run-capped row never blocks younger work eligible for another worker. It mutates only the selected row to `Dispatching` and never holds the queue lock during SSH, observation refresh, lease acquisition, upload, or remote status work. Count the run cap under the same lock using same-run dispatches plus locally accepted/nonterminal same-run job records; a claim reserves its slot until reversion or terminal removal. `revert_dispatch` changes exactly matching dispatcher/job rows back to `Waiting` after lease busy or any pre-acceptance failure. `request_queue_cancel` removes a waiting row immediately, or persists `cancel_requested_at_millis` on a dispatching row and returns its dispatcher identity. Dispatch code rereads/checks that flag before snapshot, lease, upload, verify, and submit; after any race it uses the original-ID Phase 3 resolution before deciding whether remote cancellation is needed. `remove_after_terminal` removes only a durably accepted-and-terminal or durably abandoned row. `recover_dead_dispatches` changes identity-proven dead batch dispatchers to `Waiting`; it leaves every `task_turn` row untouched, including a dead owner. `remove_queued` is an internal waiting-only primitive. Keep project/worktree and project-level healthy worker names as separate canonical affinity records; malformed records fail closed. Persist `observations/<worker>.json` and a guarded per-worker refresh marker beneath the same rooted state tree; stale entries refresh single-flight with a two-second TTL and readers retain the observation age.

- [ ] **Step 5: Run queue and local state tests**

Run: `cargo test --locked --test scheduler_queue --test client_state -- --nocapture`

Expected: PASS; every crash retains either old state or a fully published replacement and cannot delete a live/ambiguous owner.

- [ ] **Step 6: Commit queue persistence**

```bash
git add src/job.rs src/client_state.rs src/error.rs tests/scheduler_queue.rs tests/client_state.rs
git commit -m "feat: persist durable local scheduler queue"
```

---

### Task 4: Wait/No-Wait Dispatch and Automatic/Pinned Run Integration

**Gate:** Start after Tasks 1--3; it is the first task allowed to modify Phase 3 public orchestration files.

**Files:**
- Modify: `src/run.rs`
- Modify: `src/cli.rs`
- Modify: `src/lib.rs`
- Modify: `src/output.rs`
- Modify: `src/error.rs`
- Modify: `tests/run_command.rs`
- Modify: `tests/cli_help.rs`

**Interfaces:**
- Consumes: Phase 3 `RunService`, `RunCompletion`, `RemoteJobClient`, `ProjectState`, `PreparedProject`; Task 1 `SchedulerPolicy`; Task 2 `SchedulerProbeAdapter`; Task 3 queue APIs.
- Produces: updated `RunRequest`, `SchedulerService::submit_and_follow`, and automatic/pinned `worker run [--worker NAME] [--no-wait]`.

- [ ] **Step 1: Write failing CLI/orchestration tests**

Pin `worker run -- npm test`, `worker run --worker mini-2 -- npm test`, `worker run --no-wait -- npm test`, and `worker run --worker mini-2 --no-wait -- npm test`. Record effects in this exact order: inspect/settings/requirements; cached fleet admission observation (single-flight refresh when stale); no-wait rejection before enqueue/snapshot; enqueue; recover dead batch dispatches; owner-scoped per-worker FIFO claim; persist `Dispatching`; snapshot; lease; upload/verify/submit; flush acceptance; follow logs. Assert pinned busy/unavailable workers are never silently rerouted; a busy pinned head does not block a younger row eligible for another worker; an older live-owned row wins for the same worker; and up to three distinct worker rows may be dispatching simultaneously.

- [ ] **Step 2: Run command tests to verify RED**

Run: `cargo test --locked --test run_command --test cli_help -- --nocapture`

Expected: FAIL because Phase 3 requires `--worker` and has no scheduler path.

- [ ] **Step 3: Replace the public worker string with a preference**

```rust
pub struct RunRequest {
    pub preference: WorkerPreference,
    pub wait_for_capacity: bool,
    pub project: PathBuf,
    pub cli_includes: Vec<String>,
    pub timeout: Option<Duration>,
    pub command: CommandSpec,
}
pub struct SchedulerService<'a> { pub runner: &'a dyn ProcessRunner, pub config: &'a Config, pub paths: &'a PathLayout, pub client_state: &'a ClientStateStore }
impl SchedulerService<'_> { pub fn submit_and_follow(&self, request: RunRequest, stdout: &mut dyn Write, stderr: &mut dyn Write) -> Result<RunCompletion, WorkerError>; }
```

Omitted `--worker` becomes `Automatic`; supplied values must pass `Config::worker` before mutation. `--no-wait` sets `wait_for_capacity: false`, including for a pin.

- [ ] **Step 4: Implement bounded scheduling and delegate submission**

Admission reads configured-worker observations through the shared two-second cache, never Doctor data. A dispatcher that finds a stale worker observation owns that worker's single-flight refresh marker, obtains the fresh result outside the queue lock, canonically publishes the cache record, and releases the marker; other dispatchers read the cached record and age. For a pin, rank only that health record. `NoEligible` plus no-wait returns `WorkerError::Capacity { code: "CAPACITY_BUSY", .. }` before enqueue. Waiting requests generate the original job/client identity, enqueue, and poll under one-second bounded waits; never hold queue lock during SSH, cache refresh, capture, rsync, or log follow.

When an owner-owned row has a per-worker FIFO candidate, `claim_next` atomically persists its `Dispatching` owner/worker and reserves any run-cap slot; the scheduler then releases the queue lock and calls Phase 3 preparation/upload/lease/submit with the original identity and selected configured worker. A lease race or any pre-acceptance failure calls `revert_dispatch` for the same queue ID and dispatcher; no re-enqueue occurs. A durably accepted job remains dispatching until terminal or durable abandonment, when `remove_after_terminal` removes it. Command starts may occur out of order between independently dispatching workers, but FIFO is preserved among rows eligible for each worker. Record affinity only after a fresh successful remote observation.

- [ ] **Step 5: Run orchestration regressions**

Run: `cargo test --locked --test run_command --test cli_help --test job_queries --test snapshot_transfer -- --nocapture`

Expected: PASS; Phase 3 explicit worker remains the diagnostic pin and accepted jobs retain original immutable identity.

- [ ] **Step 6: Commit scheduler dispatch**

```bash
git add src/run.rs src/cli.rs src/lib.rs src/output.rs src/error.rs tests/run_command.rs tests/cli_help.rs
git commit -m "feat: schedule queued jobs across workers"
```

---

### Task 5: Explicit Queue and Remote Job Cancellation

**Gate:** Start after Task 4 proves automatic/pinned dispatch and dispatch reversion.

**Files:**
- Modify: `src/job.rs`
- Modify: `src/job_service.rs`
- Modify: `src/supervisor.rs`
- Modify: `src/transfer.rs`
- Modify: `src/cli.rs`
- Modify: `src/lib.rs`
- Modify: `src/run.rs`
- Modify: `src/output.rs`
- Modify: `tests/job_queries.rs`
- Modify: `tests/run_command.rs`
- Modify: `tests/cli_help.rs`

**Interfaces:**
- Consumes: Task 3 queue persistence, Phase 3 `StatusResponse`, `JobService`, exact lease cleanup, and supervisor process inspection.
- Produces: `CancelRequest`, `CancelResponse`, `JobService::cancel`, `RemoteJobClient::cancel`, hidden `host cancel`, public `worker cancel JOB_ID`, and `CancelReport`.

- [ ] **Step 1: Write failing cancellation tests**

```rust
#[test]
fn cancel_removes_waiting_entry_without_ssh_or_snapshot() {
    assert_eq!(service.cancel(waiting_job_id()).unwrap(), CancelReport::QueuedCancelled { job_id: waiting_job_id() });
    assert!(runner.requests().is_empty());
}

#[test]
fn remote_cancel_terms_then_kills_only_recorded_group_and_releases_after_cleanup() {
    let response = host.cancel(cancel_request(running_job_id())).unwrap();
    assert_eq!(response.status().state(), JobState::Cancelled);
    assert_group_signals(&inspector, &[Signal::Term, Signal::Kill]);
    assert_cleanup_precedes_exact_lease_release(&trace);
}
```

Cover terminal idempotency, missing job, accepted-without-child launch race, child exit during cancel, stale/reused/ambiguous identities, TERM-only success, KILL failure, cleanup failure retaining lease, malformed token/fingerprint, and queue-cancel racing claim. Prove cancellation cannot remove a claimed entry and never signals any unrecorded process group.

- [ ] **Step 2: Run focused tests to verify RED**

Run: `cargo test --locked --test job_queries --test run_command --test cli_help -- --nocapture`

Expected: FAIL because `worker cancel` and `host cancel` do not exist.

- [ ] **Step 3: Define fixed-operation records**

```rust
pub struct CancelRequest { job_id: JobId, client_id: ClientId, lease_token: LeaseToken, request_fingerprint: RequestFingerprint }
pub struct CancelResponse { protocol_version: u32, status: StatusResponse }
pub enum CancelReport { QueuedCancelled { job_id: JobId }, RemoteCancelled { response: CancelResponse } }
```

Use `Serialize`, `Deserialize`, `deny_unknown_fields`, constructors, and `validate` methods. The request must exactly match accepted metadata/live lease. Terminal `Succeeded`, `Failed`, `TimedOut`, `Cancelled`, and `Lost` return existing status without signal. Add `HostOperation::Cancel` mapping only to `~/.local/bin/worker host cancel`.

- [ ] **Step 4: Implement host and public cancellation**

Implement `JobService::cancel(&self, request: CancelRequest) -> Result<CancelResponse, WorkerError>`. Under admission lock, validate indexed job and reconcile it first; then take supervisor lock. Reuse the supervisor TERM/wait/KILL/proof path; atomically transition `Cancelled`, remove only execution payload/workspace/home/tmp, fsync, then invoke `release_after_cleanup`. For accepted/no-child jobs fence launch under the supervisor lock, transition/cancel/clean/release directly.

`worker cancel JOB_ID` first calls `request_queue_cancel`. For `RemovedWaiting`, emit a queued-cancelled report with no SSH. For `RequestedDispatch`, the dispatch owner observes the durable cancellation flag before every remote boundary and removes the row after a proven pre-acceptance abandonment; if a host may already have accepted, it resolves the original ID and sends typed remote cancel only for the accepted job. Otherwise load the local record; reject `UnknownRemote`/`CleanupPending` as recovery-required infrastructure; then send exactly one typed cancel request to the recorded worker. Any transport ambiguity is resolved by original-ID status, never another host/job.

- [ ] **Step 5: Run cancellation/lifecycle regressions**

Run: `cargo test --locked --test job_queries --test run_command --test supervisor --test cli_help -- --nocapture`

Expected: PASS; queued cancellation has no remote effects and remote cancellation cannot release capacity before cleanup succeeds.

- [ ] **Step 6: Commit cancellation**

```bash
git add src/job.rs src/job_service.rs src/supervisor.rs src/transfer.rs src/cli.rs src/lib.rs src/run.rs src/output.rs tests/job_queries.rs tests/run_command.rs tests/cli_help.rs
git commit -m "feat: cancel queued and running jobs"
```

---

### Task 6: Fleet Reconciliation and Scheduler Recovery

**Gate:** Start after Task 5's cancellation contract and Phase 3 Task 8's typed per-job reconciliation are both passing.

**Files:**
- Modify: `src/job.rs`
- Modify: `src/job_service.rs`
- Modify: `src/transfer.rs`
- Modify: `src/run.rs`
- Modify: `src/cli.rs`
- Modify: `src/lib.rs`
- Modify: `src/output.rs`
- Create: `tests/fleet_reconciliation.rs`
- Modify: `tests/job_queries.rs`

**Interfaces:**
- Consumes: Task 2 typed probes, Task 3 queue/affinity records, Task 5 cancellation, and Phase 3 `JobService::reconcile_job`/`RemoteJobClient::status`.
- Produces: `FleetReconcileRequest`, `FleetWorkerReport`, `FleetReconcileReport`, `RemoteJobClient::reconcile`, and `FleetReconciler::reconcile`.

- [ ] **Step 1: Write failing fleet tests**

```rust
#[test]
fn one_unavailable_worker_does_not_block_other_reconciliation_or_queue_progress() {
    let report = reconciler.reconcile().unwrap();
    assert_eq!(report.workers()[0].outcome(), FleetOutcome::Unavailable);
    assert_eq!(report.workers()[1].outcome(), FleetOutcome::Reconciled);
    assert_eq!(report.workers()[2].outcome(), FleetOutcome::Reconciled);
}

#[test]
fn failed_probe_is_not_evidence_that_a_retained_job_is_lost() {
    reconciler.reconcile().unwrap();
    assert!(matches!(store.load_job(known_remote_job()).unwrap().remote_uncertainty(), RemoteUncertainty::UnknownRemote { .. }));
    assert_no_lease_release(&offline_worker);
}
```

Cover all terminal states, indexed/non-indexed crash repair delegated to Phase 3 Task 8, corrupt status/lease, timeout, stale observations, jobs from another client, stale affinity, and repeated reconciliation. Assert every repair names a known job ID; no worker-wide file scan, arbitrary path, or broad cleanup is allowed.

- [ ] **Step 2: Run fleet tests to verify RED**

Run: `cargo test --locked --test fleet_reconciliation --test job_queries -- --nocapture`

Expected: FAIL because fleet DTOs and reconciler do not exist.

- [ ] **Step 3: Define bounded fleet reports and operation**

```rust
pub struct FleetReconcileRequest { pub known_job_ids: Vec<JobId> }
pub enum FleetOutcome { Reconciled, Unavailable, InvalidResponse }
pub struct FleetWorkerReport { pub worker: String, pub probe: WorkerHealth, pub outcome: FleetOutcome, pub jobs: Vec<StatusResponse>, pub error_code: Option<String> }
pub struct FleetReconcileReport { pub generated_at_millis: u64, pub workers: Vec<FleetWorkerReport> }
pub struct FleetReconciler<'a> { pub config: &'a Config, pub client_state: &'a ClientStateStore, pub remote: RemoteJobClient<'a> }
impl FleetWorkerReport { pub fn outcome(&self) -> FleetOutcome; }
impl FleetReconciler<'_> { pub fn reconcile(&self) -> Result<FleetReconcileReport, WorkerError>; }
```

Require unique canonical IDs and bound each host request to 100 IDs. Add fixed `HostOperation::Reconcile`/`host reconcile`; host calls `JobService::reconcile_job` only for supplied IDs and returns bounded typed per-ID status/error results.

- [ ] **Step 4: Implement recovery without a second authority**

Before scheduler admission and `worker status` listing refresh, call `FleetReconciler::reconcile`. Load at most 100 newest local records, group known nonterminal/uncertain IDs by recorded worker, refresh/probe workers through the shared single-flight observation cache, then request remote reconciliation for only those IDs. Persist authoritative status with `update_observation`; retain `UnknownRemote` on unavailable/invalid response. Reconciliation and observation refresh happen outside the queue lock. Remove affinity only after a fresh successful ineligible/unavailable probe, never an SSH failure.

Use remote durable status first, remote lease second, local waiting queue third; cached local observations cannot free capacity. Return partial worker errors but continue scheduling on other freshly eligible workers.

- [ ] **Step 5: Run fleet/scheduler regressions**

Run: `cargo test --locked --test fleet_reconciliation --test job_queries --test run_command --test workers_command -- --nocapture`

Expected: PASS; partial fleet failure preserves uncertainty but does not stop a ready idle worker accepting the next FIFO-compatible job.

- [ ] **Step 6: Commit fleet recovery**

```bash
git add src/job.rs src/job_service.rs src/transfer.rs src/run.rs src/cli.rs src/lib.rs src/output.rs tests/fleet_reconciliation.rs tests/job_queries.rs
git commit -m "feat: reconcile scheduler fleet state"
```

---

### Task 7: Scheduler Concurrency, Lifecycle, and Privacy Matrix

**Gate:** Start after Tasks 1--6; it adds only deterministic test seams and does not add a production control plane.

**Files:**
- Create: `tests/scheduler_concurrency.rs`
- Modify: `tests/run_command.rs`
- Modify: `tests/job_queries.rs`
- Modify: `tests/client_state.rs`
- Modify: `tests/scheduler_policy.rs`

**Interfaces:**
- Consumes: Tasks 1--6 public and host interfaces.
- Produces: deterministic evidence for FIFO, one-slot leases, cancellation, reconciliation, and non-disclosure contracts.

- [ ] **Step 1: Add deterministic adversarial tests**

```rust
#[test]
fn fifty_simultaneous_clients_preserve_fifo_and_never_start_two_heavy_jobs_per_worker() {
    let result = run_barrier_clients(50, fake_three_host_transport());
    assert_fifo(result.queue_events()); assert_at_most_one_lease_per_worker(result.leases());
}

#[test]
fn cancel_dispatch_race_has_one_terminal_outcome_and_zero_duplicate_launches() {
    let result = run_cancel_claim_race();
    assert!(result.is_cancelled() || result.is_completed());
    assert!(result.launch_count() <= 1);
}

#[test]
fn automatic_placement_never_overrides_a_pinned_worker() {
    assert!(pinned_job_waits_when_only_another_worker_is_idle());
}

#[test]
fn owner_scoped_per_worker_claim_leaves_another_live_owner_ahead() {
    let result = run_owner_claim_ordering_race();
    assert!(result.younger_owner_claim_is_none());
    assert!(result.older_owner_claims_matching_worker());
}

#[test]
fn run_cap_and_observation_refresh_are_atomic_under_competing_dispatchers() {
    let result = run_cap_and_cache_race();
    assert_eq!(result.accepted_claims_for_run(), 1);
    assert_eq!(result.refreshes_for_stale_worker(), 1);
}
```

In `tests/scheduler_queue.rs`, add direct persistence tests for task-turn waiting ownership/adoption, owner-scoped claims, a busy pinned head not blocking a younger row for another worker, and a dead-owner `task_turn` surviving `recover_dead_dispatches`. In `tests/scheduler_concurrency.rs`, race two dispatchers against the final run-cap slot and race stale readers against one observation refresh marker; prove exactly one claim consumes the cap and one refresh publishes the canonical cache entry. Run 100 deterministic interleavings for enqueue/claim, claim/lease, cancel/claim, cancel/launch, reboot/reconcile, and disconnect during acquire/upload/verify/submit. Each case must prove: one ID has zero/one launch; no worker has two leases; an older eligible entry is not bypassed for the same worker; a pinned/capability-blocked row does not block another worker; ambiguous owner records remain; dead task-turn rows remain; cleanup precedes release; and no unavailable host triggers local execution.

Add property tests for ranking permutation/determinism, queue sequence monotonicity, bounded fleet requests, and non-secret record serialization. Plant argv, shell, env-like values, project paths, and local hostnames; prove absence from queue/affinity records, human/JSON output, and mac-worker-generated errors.

- [ ] **Step 2: Run matrix tests to verify RED**

Run: `cargo test --locked --test scheduler_concurrency --test run_command --test job_queries --test client_state -- --nocapture`

Expected: FAIL until deterministic synchronization seams expose every required race boundary.

- [ ] **Step 3: Add only inert test seams and close race windows**

Add `#[doc(hidden)]` test-only barriers/fault points at queue publication, owner-scoped claim/run-cap evaluation, observation-cache refresh publication, dispatch-to-lease handoff, remote-cancel handoff, and reconciliation response handling. Production defaults remain inert. Fix failures by retaining Task 1--6 authority rules; do not add background threads, hidden daemons, broad cleanup, or alternate retry/lifecycle state.

- [ ] **Step 4: Run focused Phase 3/4 lifecycle suite**

Run: `cargo test --locked --test scheduler_concurrency --test scheduler_policy --test scheduler_queue --test fleet_reconciliation --test run_command --test job_queries --test supervisor --test remote_snapshot -- --nocapture`

Expected: PASS with zero duplicate execution, no extra lease, no FIFO bypass, and no planted-value disclosure.

- [ ] **Step 5: Commit adversarial coverage**

```bash
git add tests/scheduler_concurrency.rs tests/run_command.rs tests/job_queries.rs tests/client_state.rs tests/scheduler_policy.rs
git commit -m "test: harden three-worker scheduler lifecycle"
```

---

### Task 8: Documentation, Three-Mac Acceptance, and Final Phase 4 Gate

**Gate:** Start after Task 7 passes the focused scheduler lifecycle matrix.

**Files:**
- Modify: `README.md`
- Create: `docs/phase-four-validation.md`
- Modify: `tests/cli_help.rs`

**Interfaces:**
- Consumes: all Phase 4 public commands/reports.
- Produces: automatic scheduling/cancellation documentation and sanitized three-worker evidence.

- [ ] **Step 1: Write failing help tests**

Assert help includes `worker run [--worker NAME] [--no-wait] -- COMMAND`, `worker cancel JOB_ID`, and automatic selection when `--worker` is omitted. Assert it excludes dashboard, fetch, artifacts, cache, Docker, and GC commands. Assert an unconfigured `--worker raw-hostname` is rejected.

- [ ] **Step 2: Run help tests to verify RED**

Run: `cargo test --locked --test cli_help -- --nocapture`

Expected: FAIL until Phase 4 grammar/help text exists.

- [ ] **Step 3: Update README and validation record**

Document:

```bash
worker run -- npm test
worker run --no-wait -- npm test
worker run --worker mini-2 -- npm test
worker status
worker cancel <job-id>
```

State: automatic runs queue per-worker FIFO for compatible configured workers; pins wait only for their named worker without blocking another worker; no-wait returns `CAPACITY_BUSY`; log disconnect/Ctrl-C does not cancel; cancellation is explicit/targeted; remote source changes never return. State that scheduler admission uses its local shared observation cache only as advisory input and the remote lease remains authoritative. State dashboard is Phase 4.5 and artifacts/fetch, caches, Docker, and general GC are later work.

Create `docs/phase-four-validation.md` headings: `Commit and protocol`, `Automated gate`, `Three-worker setup`, `Automatic placement`, `FIFO fourth job`, `Pinned worker`, `No-wait capacity`, `Cancellation`, `Disconnect and reconciliation`, `Cleanup and privacy`, and `Remaining boundary`. Record only commit/version, shortened IDs, aliases, counts, states, durations, exits, and mac-worker-owned namespace fingerprints.

- [ ] **Step 4: Perform sanitized three-Mac acceptance**

With an isolated temporary clone/XDG roots and the same release helper installed on all configured workers: submit three bounded jobs without a pin and prove distinct leases; submit a fourth and prove per-worker FIFO/blocking reason; finish one and prove only the eligible head dispatches. Prove a pin waits despite another idle worker while a younger compatible row may use that other worker. Fill all slots and prove no-wait changes neither local snapshot nor remote namespace. Cancel one waiting and one running job, proving no SSH for the first and targeted cleanup/release for the second. Interrupt a follower/reconnect by original ID; stop SSH or reboot one worker and prove the other two continue while the affected job remains stale/unknown, not assumed lost. Record the cache as scheduler-owned advisory evidence only; do not record raw probe payloads.

- [ ] **Step 5: Run final local gate**

Run:

```bash
cargo fmt --all --check
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked --release
git diff --check
```

Expected: all commands exit `0`; recorded evidence distinguishes fake-transport tests from live three-Mac observations.

- [ ] **Step 6: Commit Phase 4 documentation/validation**

```bash
git add README.md docs/phase-four-validation.md tests/cli_help.rs
git commit -m "test: validate three-worker scheduler"
```

## Plan Self-Review Results

- **Spec coverage:** Task 1 is start-now pure ranking; Task 2 is the single protocol-v3 probe adapter shared with Phase 4.5; Task 3 covers durable canonical queue records, task-turn ownership, run references, per-worker FIFO, dispatch recovery, and shared admission observations; Task 4 covers automatic selection, pins, no-wait, and cached admission; Task 5 covers queued/running cancellation; Task 6 covers fleet/reboot/offline recovery; Task 7 covers concurrency/lifecycle/privacy; Task 8 covers docs and three-Mac acceptance. Dashboard, artifacts/fetch, Docker, general GC, and Phase 5 task command implementation are intentionally absent.
- **Placeholder scan:** Clean: every task specifies files, interfaces, concrete assertions, RED/PASS commands, implementation boundary, and commit.
- **Type consistency:** Task 1 defines `CandidateObservation`, `AffinityHints`, `WorkerPreference`, and `SchedulerPolicy`; Task 2 maps version-3 `WorkerHealth`/`ProbeResponse` into that observation; Task 3 persists preference/affinity, entry kind, owner, run reference, `Dispatching`, and observation cache records; Task 4 consumes them for `RunRequest`; Task 5 defines cancellation; Task 6 composes status and cache refreshes into fleet reports; Tasks 7--8 consume prior public interfaces.
