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
- Queue records hold only IDs, sanitized command summary, requirements, worker preference, timestamps, owner identity, and bounded error codes—never argv/shell, environment values, paths, snapshot paths, or lease tokens.
- FIFO is immutable enqueue order and means claim/admission consideration order, not wall-clock child-start order. A queue row is persistently `Waiting` or `Dispatching`; a dead dispatcher is recovered to `Waiting`, never silently deleted. Multiple distinct-worker dispatches may be in flight to fill three slots.
- Eligible means fresh ready/protocol/capability-compatible idle probe plus successful atomic lease. Ranking is worktree affinity, project affinity, greatest available memory, greatest free disk, lexical worker name; affinity never overrides eligibility.
- `--no-wait` returns `CAPACITY_BUSY` without queue entry, snapshot, lease, or remote mutation.
- Once remotely accepted, resolve every ambiguity with its original job/client/token/fingerprint through Phase 3 `resolve-or-abandon`; never create a replacement job.
- Cancellation is explicit. Ctrl-C or log-follow disconnect never cancels an accepted job.
- Remote cancellation targets only the recorded process group: TERM, ten-second wait, KILL if needed, proof of absence, atomic `cancelled`, job-owned cleanup, then exact lease release. No broad process kill.
- Fleet reconciliation issues bounded typed per-job requests for known IDs. A failed probe is never evidence a retained job is gone.
- Dashboard/watch, artifacts/fetch, caches/env profiles, Docker, retention/general GC, notifications, and Phase 5 acceptance work are out of scope.

---

## File Map

```text
src/protocol.rs                 one Phase 4 probe protocol bump: memory and CPU counter DTOs
src/probe.rs                    bounded macOS available-memory and cumulative CPU collection
src/scheduler.rs                pure scheduler-owned observations, eligibility, and ranking
src/scheduler_adapter.rs        post-Phase-3 projection from WorkerHealth/ProbeResponse to policy facts
src/client_state.rs             descriptor-bound queue persistence and affinity history
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

Create `src/scheduler.rs`; it must not import `protocol`, `probe`, `transport`, `Config`, or `WorkerEntry`:

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
git add src/scheduler.rs tests/scheduler_policy.rs
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

Test canonical protocol-v3 fixtures in workers/Doctor/setup/job-protocol suites; valid/missing memory; valid CPU tick counters; counter overflow; malformed `vm_stat` and `host_processor_info`; inventory/health name or SSH mismatch; and the absence of a probe. Assert that both new fact groups are added by one version change, never two.

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

Use `#[serde(default)]` only for field decoding; protocol-2 helpers remain ineligible because the version field is now `3`. In `probe.rs`, compute available memory from bounded `vm_stat` `(Pages free + Pages speculative) * page_size`; collect cumulative CPU ticks with a bounded macOS `host_processor_info` call; return `None` for unavailable/invalid facts and never fabricate a zero. Update every literal probe/setup/Doctor fixture to protocol 3.

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
- Produces: `QueueId`, `QueueState`, `QueueEntry`, `QueueSnapshot`, `QueueClaim`, `QueueCancel`, and `ClientStateStore::{enqueue,queue_snapshot,claim_next,revert_dispatch,request_queue_cancel,remove_after_terminal,recover_dead_dispatches,remove_queued,record_affinity,affinity_hints}`.

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

Cover duplicate job/sequence IDs, stale lock state, malformed/noncanonical JSON, symlink/FIFO/device replacement, interrupted atomic write, simultaneous enqueue, crash after retirement rename, PID reuse, cancel racing `claim_next`, and lease-busy/failed-preacceptance reversion. Prove a dead dispatcher is first persisted back to `Waiting`, not removed. Prove three rows can be `Dispatching` only when their selected workers are distinct; no later compatible `Waiting` row is claimed before an older compatible `Waiting` row.

- [ ] **Step 2: Run queue tests to verify RED**

Run: `cargo test --locked --test scheduler_queue --test client_state -- --nocapture`

Expected: FAIL because queue records and queue methods do not exist.

- [ ] **Step 3: Define validated queue records**

Add strict manual serialization/validation in `src/job.rs`:

```rust
pub struct QueueId(u64);
pub enum QueueState { Waiting, Dispatching { dispatch_owner: ProcessIdentity, selected_worker: String, claimed_at_millis: u64 } }
pub struct QueueEntry {
    queue_id: QueueId, job_id: JobId, client_id: ClientId, project_id: String,
    worktree_id: String, command_summary: CommandSummary, requirements: Vec<String>,
    preference: WorkerPreference, enqueue_owner: ProcessIdentity, state: QueueState,
    cancel_requested_at_millis: Option<u64>, enqueued_at_millis: u64,
}
impl QueueEntry { pub fn job_id(&self) -> JobId; pub fn state(&self) -> &QueueState; pub fn is_cancel_requested(&self) -> bool; }
pub struct QueueSnapshot { next_id: QueueId, entries: Vec<QueueEntry> }
pub struct QueueClaim { entry: QueueEntry }
impl QueueClaim { pub fn entry(&self) -> &QueueEntry; }
pub enum QueueCancel { RemovedWaiting { job_id: JobId }, RequestedDispatch { job_id: JobId, dispatch_owner: ProcessIdentity } }
```

`QueueEntry::new` and `validate` reuse canonical identifiers and bounded lowercase capabilities. It must not contain exact command data, environment values, local paths, manifest digest, snapshot path, or lease token. `QueueSnapshot::validate` requires monotonically increasing IDs/timestamps, unique dispatching worker names, and valid owner identities. It accepts persisted `Dispatching` so a crash can be recovered precisely.

- [ ] **Step 4: Implement descriptor-bound queue operations**

Create only `queue/state.json`, `queue/lock`, and owner-scoped retirement records below the existing local state root. Reuse `ClientStateStore`'s root lock, component-by-component creation, canonical staging/fsync/rename, and retirement cleanup. Implement:

```rust
pub fn enqueue(&self, entry: QueueEntry) -> Result<QueueEntry, WorkerError>;
pub fn queue_snapshot(&self) -> Result<QueueSnapshot, WorkerError>;
pub fn claim_next(&self, dispatch_owner: ProcessIdentity, ranked_workers: &[String], claimed_at_millis: u64) -> Result<Option<QueueClaim>, WorkerError>;
pub fn revert_dispatch(&self, job_id: JobId, dispatch_owner: ProcessIdentity) -> Result<QueueEntry, WorkerError>;
pub fn request_queue_cancel(&self, job_id: JobId, requested_at_millis: u64) -> Result<Option<QueueCancel>, WorkerError>;
pub fn remove_after_terminal(&self, job_id: JobId, dispatch_owner: ProcessIdentity) -> Result<QueueEntry, WorkerError>;
pub fn recover_dead_dispatches(&self) -> Result<Vec<JobId>, WorkerError>;
pub fn remove_queued(&self, job_id: JobId) -> Result<Option<QueueEntry>, WorkerError>;
pub fn record_affinity(&self, project_id: &str, worktree_id: &str, worker: &str, observed_at_millis: u64) -> Result<(), WorkerError>;
pub fn affinity_hints(&self, project_id: &str, worktree_id: &str) -> Result<AffinityHints, WorkerError>;
```

`claim_next` examines the oldest uncancelled `Waiting` row only; it marks that row `Dispatching` with the first ranked compatible worker not already selected by a live dispatch. It never holds the queue lock during SSH. If the head has no eligible worker, it returns `None` and does not inspect later waiting rows. `revert_dispatch` changes exactly matching dispatcher/job rows back to `Waiting` after lease busy or any pre-acceptance failure. `request_queue_cancel` removes a waiting row immediately, or persists `cancel_requested_at_millis` on a dispatching row and returns its dispatcher identity. Dispatch code rereads/checks that flag before snapshot, lease, upload, verify, and submit; after any race it uses the original-ID Phase 3 resolution before deciding whether remote cancellation is needed. `remove_after_terminal` removes only a durably accepted-and-terminal or durably abandoned row. `recover_dead_dispatches` changes identity-proven dead dispatchers to `Waiting`; it never deletes them. `remove_queued` is an internal waiting-only primitive. Keep project/worktree and project-level healthy worker names as separate canonical affinity records; malformed records fail closed.

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

Pin `worker run -- npm test`, `worker run --worker mini-2 -- npm test`, `worker run --no-wait -- npm test`, and `worker run --worker mini-2 --no-wait -- npm test`. Record effects in this exact order: inspect/settings/requirements; concurrent fresh fleet probe; no-wait rejection before enqueue/snapshot; enqueue; recover dead dispatches; rank the oldest waiting row; persist `Dispatching`; snapshot; lease; upload/verify/submit; flush acceptance; follow logs. Assert pinned busy/unavailable workers are never silently rerouted; no later compatible waiting row is claimed before an older compatible waiting row; and up to three distinct worker rows may be dispatching simultaneously.

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

Probe configured workers concurrently with `WorkersService::inspect_with_requirements`; never reuse Doctor data. For a pin, rank only that health record. `NoEligible` plus no-wait returns `WorkerError::Capacity { code: "CAPACITY_BUSY", .. }` before enqueue. Waiting requests generate the original job/client identity, enqueue, and poll under one-second bounded waits; never hold queue lock during SSH, capture, rsync, or log follow.

When the head has a candidate, `claim_next` atomically persists its `Dispatching` owner/worker then the scheduler releases the queue lock and calls Phase 3 preparation/upload/lease/submit with the original identity and selected configured worker. A lease race or any pre-acceptance failure calls `revert_dispatch` for the same queue ID and dispatcher; no re-enqueue occurs. A durably accepted job remains dispatching until terminal or durable abandonment, when `remove_after_terminal` removes it. Command starts may occur out of order between independently dispatching workers, but admission consideration remains FIFO. Record affinity only after a fresh successful remote observation.

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

Before scheduler admission and `worker status` listing refresh, call `FleetReconciler::reconcile`. Load at most 100 newest local records, group known nonterminal/uncertain IDs by recorded worker, concurrently probe all workers, then request remote reconciliation for only those IDs. Persist authoritative status with `update_observation`; retain `UnknownRemote` on unavailable/invalid response. Remove affinity only after a fresh successful ineligible/unavailable probe, never an SSH failure.

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
```

Run 100 deterministic interleavings for enqueue/claim, claim/lease, cancel/claim, cancel/launch, reboot/reconcile, and disconnect during acquire/upload/verify/submit. Each case must prove: one ID has zero/one launch; no worker has two leases; an older compatible entry is not bypassed; ambiguous owner records remain; cleanup precedes release; and no unavailable host triggers local execution.

Add property tests for ranking permutation/determinism, queue sequence monotonicity, bounded fleet requests, and non-secret record serialization. Plant argv, shell, env-like values, project paths, and local hostnames; prove absence from queue/affinity records, human/JSON output, and mac-worker-generated errors.

- [ ] **Step 2: Run matrix tests to verify RED**

Run: `cargo test --locked --test scheduler_concurrency --test run_command --test job_queries --test client_state -- --nocapture`

Expected: FAIL until deterministic synchronization seams expose every required race boundary.

- [ ] **Step 3: Add only inert test seams and close race windows**

Add `#[doc(hidden)]` test-only barriers/fault points at queue publication, dispatch-to-lease handoff, remote-cancel handoff, and reconciliation response handling. Production defaults remain inert. Fix failures by retaining Task 1--6 authority rules; do not add background threads, hidden daemons, broad cleanup, or alternate retry/lifecycle state.

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

State: automatic runs queue FIFO for compatible configured workers; pins wait only for their named worker; no-wait returns `CAPACITY_BUSY`; log disconnect/Ctrl-C does not cancel; cancellation is explicit/targeted; remote source changes never return. State dashboard is Phase 4.5 and artifacts/fetch, caches, Docker, and general GC are later work.

Create `docs/phase-four-validation.md` headings: `Commit and protocol`, `Automated gate`, `Three-worker setup`, `Automatic placement`, `FIFO fourth job`, `Pinned worker`, `No-wait capacity`, `Cancellation`, `Disconnect and reconciliation`, `Cleanup and privacy`, and `Remaining boundary`. Record only commit/version, shortened IDs, aliases, counts, states, durations, exits, and mac-worker-owned namespace fingerprints.

- [ ] **Step 4: Perform sanitized three-Mac acceptance**

With an isolated temporary clone/XDG roots and the same release helper installed on all configured workers: submit three bounded jobs without a pin and prove distinct leases; submit a fourth and prove FIFO/blocking reason; finish one and prove only head dispatches. Prove a pin waits despite another idle worker. Fill all slots and prove no-wait changes neither local snapshot nor remote namespace. Cancel one waiting and one running job, proving no SSH for the first and targeted cleanup/release for the second. Interrupt a follower/reconnect by original ID; stop SSH or reboot one worker and prove the other two continue while the affected job remains stale/unknown, not assumed lost.

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

- **Spec coverage:** Task 1 is start-now pure ranking; Task 2 is the single protocol-v3 probe adapter shared with Phase 4.5; Task 3 covers durable FIFO/dispatch recovery; Task 4 covers automatic selection, pins, and no-wait; Task 5 covers queued/running cancellation; Task 6 covers fleet/reboot/offline recovery; Task 7 covers concurrency/lifecycle/privacy; Task 8 covers docs and three-Mac acceptance. Dashboard, artifacts, caches, Docker, general GC, and Phase 5 hardening are intentionally absent.
- **Placeholder scan:** Clean: every task specifies files, interfaces, concrete assertions, RED/PASS commands, implementation boundary, and commit.
- **Type consistency:** Task 1 defines `CandidateObservation`, `AffinityHints`, `WorkerPreference`, and `SchedulerPolicy`; Task 2 maps version-3 `WorkerHealth`/`ProbeResponse` into that observation; Task 3 persists preference/affinity and `Dispatching`; Task 4 consumes all three for `RunRequest`; Task 5 defines cancellation; Task 6 composes status into fleet reports; Tasks 7--8 consume prior public interfaces.
