# Three-Worker Scheduler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `worker run` select an eligible Mac mini automatically, wait in a durable local FIFO queue, and provide explicit cancellation plus fleet-wide reconciliation without weakening the durable single-worker lifecycle.

**Architecture:** Phase 3 host state remains authoritative for leases, accepted jobs, process groups, and terminal outcomes. The new local scheduler owns only pending queue entries beneath the locked client-state root; after selection and atomic lease acquisition it delegates immutable snapshot upload and submission to Phase 3 `RunService`. Queue cancellation is local until acceptance; remote cancellation uses a fixed typed host operation and the supervisor's exact cleanup/release path.

**Tech Stack:** Rust 2024, existing `serde`/`serde_json` canonical records, `libc` locks and descriptor-relative filesystem operations, OpenSSH through `ProcessRunner`, and existing `proptest`/`tempfile`/`assert_cmd` test support.

**Spec:** `docs/superpowers/specs/2026-08-25-mac-worker-design.md` sections 6--13 and 17--21; `docs/superpowers/plans/2026-08-27-single-worker-execution.md` Tasks 8--10.

## Global Constraints

- Begin after Phase 3 Tasks 8--10 land: public `RunService`, typed remote status/log/resolve, reconnectable logs, and the single-worker live gate.
- Automatic selection is default. `--worker NAME` is a configured-name pin for diagnosis, never a raw SSH destination.
- Every worker has one heavy slot. Fresh probes are advisory; the existing atomic host lease is the only admission authority.
- The MacBook is the source of truth. Upload only a verified immutable `Snapshot`; never sync a live worktree or source changes back.
- Queue state is canonical owner-only JSON below `PathLayout::state`, protected by the existing cross-process lock and no-follow filesystem layer. No daemon/database is introduced.
- Queue records hold only IDs, sanitized command summary, requirements, worker preference, timestamps, owner identity, and bounded error codes—never argv/shell, environment values, paths, snapshot paths, or lease tokens.
- FIFO is immutable enqueue order. Reap only an identity-proven absent queue owner under the queue lock; retain live, reused, or ambiguous identities.
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
src/protocol.rs                 available-memory probe field and typed queue/cancel/fleet DTOs
src/probe.rs                    bounded macOS available-memory collection
src/scheduler.rs                pure eligibility and deterministic candidate ranking
src/client_state.rs             descriptor-bound queue persistence and affinity history
src/job.rs                      QueueId/QueueEntry and cancellation records
src/job_service.rs              host cancellation and exact-job reconciliation entry points
src/transfer.rs                 fixed cancel/reconcile host operations and remote client helpers
src/run.rs                      Phase 3 RunService integration: enqueue, wait/no-wait, dispatch
src/cli.rs                      worker preference/no-wait/cancel grammar and hidden host commands
src/lib.rs                      host stdio dispatch and streaming public commands
src/output.rs                   sanitized run/queue/cancel reports
tests/scheduler_policy.rs       ranking and proptest properties
tests/scheduler_queue.rs        queue durability/FIFO/owner lifecycle
tests/run_command.rs            automatic/pinned/no-wait orchestration
tests/job_queries.rs            host cancellation/reconciliation operations
tests/fleet_reconciliation.rs   partial-fleet recovery
tests/scheduler_concurrency.rs  races, duplicate-execution, and privacy matrices
docs/phase-four-validation.md   sanitized three-Mac validation evidence
README.md                       Phase 4 usage and explicit remaining boundary
```

### Task 1: Probe Memory Fact and Pure Deterministic Candidate Ranking

**Files:**
- Modify: `src/protocol.rs`
- Modify: `src/probe.rs`
- Create: `src/scheduler.rs`
- Create: `tests/scheduler_policy.rs`
- Modify: `tests/workers_command.rs`

**Interfaces:**
- Consumes: `Config`, `WorkerEntry`, `ProbeResponse`, `WorkerHealth`, `HealthStatus`, `SlotState`, and requirement strings.
- Produces: `ProbeResponse::available_memory_bytes`, `AffinityHints`, `CandidateRejection`, `RankedWorker`, `Selection`, and `SchedulerPolicy::{rank,select}`.

- [ ] **Step 1: Write failing probe and ranking tests**

```rust
#[test]
fn ranks_worktree_affinity_before_more_memory_and_disk() {
    let ranked = SchedulerPolicy::rank(&config(), &healths(), &["node".into()],
        &AffinityHints { worktree_worker: Some("mini-2".into()), project_worker: Some("mini-1".into()) });
    assert_eq!(names(&ranked), vec!["mini-2", "mini-1", "mini-3"]);
}

#[test]
fn rejects_busy_offline_missing_capability_and_unknown_inventory_workers() {
    assert!(matches!(SchedulerPolicy::select(&config(), &mixed_healths(), &["ruby".into()], &AffinityHints::none()),
        Selection::NoEligible { rejections } if rejections.len() == 4));
}

proptest! {
    #[test]
    fn ranking_is_deterministic_and_unique(input in arbitrary_health_sets()) {
        let a = SchedulerPolicy::rank(&config_for(&input), &input, &[], &AffinityHints::none());
        let b = SchedulerPolicy::rank(&config_for(&input), &input, &[], &AffinityHints::none());
        prop_assert_eq!(a, b); prop_assert!(has_unique_names(&a));
    }
}
```

Also test valid available bytes, missing fact, overflow/malformed `vm_stat`, and unavailable memory collection. A missing memory fact must sort below every known fact; it cannot itself make a worker ineligible.

- [ ] **Step 2: Run focused tests to verify RED**

Run: `cargo test --locked --test scheduler_policy --test workers_command -- --nocapture`

Expected: FAIL because the memory field and scheduler policy do not exist.

- [ ] **Step 3: Implement the bounded fact and pure policy**

Add this `#[serde(default)]` field to `ProbeResponse`:

```rust
pub available_memory_bytes: Option<u64>,
```

Add controlled bounded `vm_stat` parsing in `probe.rs`: calculate `(Pages free + Pages speculative) * page_size`, return `None` for unavailable/invalid results, and preserve existing memory-pressure/swap admission behavior.

Create `src/scheduler.rs` with no clock, filesystem, SSH, or process calls:

```rust
pub struct AffinityHints { pub worktree_worker: Option<String>, pub project_worker: Option<String> }
pub enum CandidateRejection { UnknownWorker { name: String }, Unavailable { name: String }, MissingCapabilities { name: String, missing: Vec<String> }, Busy { name: String } }
pub struct RankedWorker { worker: WorkerEntry, probe: ProbeResponse }
pub enum Selection { Selected(RankedWorker), NoEligible { rejections: Vec<CandidateRejection> } }
pub struct SchedulerPolicy;
impl SchedulerPolicy {
    pub fn rank(config: &Config, health: &[WorkerHealth], requirements: &[String], affinity: &AffinityHints) -> Vec<RankedWorker>;
    pub fn select(config: &Config, health: &[WorkerHealth], requirements: &[String], affinity: &AffinityHints) -> Selection;
}
```

Validate inventory/health identity pairing. Require `Ready`, a probe, `Idle`, no missing capability, and every declared/project requirement. Sort by one explicit tuple: affinity class, reverse known-memory class/value, reverse disk, name; report rejections in lexical worker order.

- [ ] **Step 4: Run focused policy/probe regression tests**

Run: `cargo test --locked --test scheduler_policy --test workers_command --lib probe::tests -- --nocapture`

Expected: PASS; no test performs SSH or mutates client/host state.

- [ ] **Step 5: Commit the independent policy**

```bash
git add src/protocol.rs src/probe.rs src/scheduler.rs tests/scheduler_policy.rs tests/workers_command.rs
git commit -m "feat: rank compatible scheduler candidates"
```

---

### Task 2: Durable Queue Contract and Canonical Queue Persistence

**Files:**
- Modify: `src/job.rs`
- Modify: `src/client_state.rs`
- Modify: `src/error.rs`
- Create: `tests/scheduler_queue.rs`
- Modify: `tests/client_state.rs`

**Interfaces:**
- Consumes: Phase 3 `JobId`, `ClientId`, `CommandSummary`, `ClientStateStore`, and Task 1 `AffinityHints`.
- Produces: `QueueId`, `QueueState`, `WorkerPreference`, `QueueEntry`, `QueueSnapshot`, and `ClientStateStore::{enqueue,queue_snapshot,claim_head,remove_queued,record_affinity,affinity_hints}`.

- [ ] **Step 1: Write failing queue schema and persistence tests**

After Task 9's public orchestration schema lands, create these tests and extend them for canonical JSON/mode/containment:

```rust
#[test]
fn only_oldest_claimable_entry_is_claimed() {
    let store = open_queue();
    let first = queued("00000000000000000000000000000001", 10);
    let second = queued("00000000000000000000000000000002", 11);
    store.enqueue(first.clone()).unwrap(); store.enqueue(second).unwrap();
    assert_eq!(store.claim_head().unwrap().unwrap().job_id(), first.job_id());
    assert_eq!(store.queue_snapshot().unwrap().entries().len(), 1);
}

#[test]
fn live_or_ambiguous_owner_is_never_reaped_as_abandoned() {
    let store = open_queue_with_owner_inspector(AmbiguousOwner);
    store.enqueue(queued("00000000000000000000000000000003", 12)).unwrap();
    assert_eq!(store.queue_snapshot().unwrap().entries().len(), 1);
}
```

Cover duplicate job/sequence IDs, stale lock state, malformed/noncanonical JSON, symlink/FIFO/device replacement, interrupted atomic write, simultaneous enqueue, crash after retirement rename, PID reuse, and cancel racing `claim_head`. Assert only identity-proven absent owners are reaped.

- [ ] **Step 2: Run queue tests to verify RED**

Run: `cargo test --locked --test scheduler_queue --test client_state -- --nocapture`

Expected: FAIL because queue records and queue methods do not exist.

- [ ] **Step 3: Define validated queue records**

Add strict manual serialization/validation in `src/job.rs`:

```rust
pub struct QueueId(u64);
pub enum WorkerPreference { Automatic, Pinned { worker: String } }
pub enum QueueState { Waiting, Claimed }
pub struct QueueEntry {
    queue_id: QueueId, job_id: JobId, client_id: ClientId, project_id: String,
    worktree_id: String, command_summary: CommandSummary, requirements: Vec<String>,
    preference: WorkerPreference, owner: ProcessIdentity, state: QueueState, enqueued_at_millis: u64,
}
pub struct QueueSnapshot { next_id: QueueId, entries: Vec<QueueEntry> }
```

`QueueEntry::new` and `validate` reuse canonical identifiers and bounded lowercase capabilities. It must not contain exact command data, environment values, local paths, manifest digest, snapshot path, or lease token. `QueueSnapshot::validate` requires monotonically increasing IDs/timestamps and rejects persisted `Claimed`: a crash can never wedge a head.

- [ ] **Step 4: Implement descriptor-bound queue operations**

Create only `queue/state.json`, `queue/lock`, and owner-scoped retirement records below the existing local state root. Reuse `ClientStateStore`'s root lock, component-by-component creation, canonical staging/fsync/rename, and retirement cleanup. Implement:

```rust
pub fn enqueue(&self, entry: QueueEntry) -> Result<QueueEntry, WorkerError>;
pub fn queue_snapshot(&self) -> Result<QueueSnapshot, WorkerError>;
pub fn claim_head(&self) -> Result<Option<QueueEntry>, WorkerError>;
pub fn remove_queued(&self, job_id: JobId) -> Result<Option<QueueEntry>, WorkerError>;
pub fn record_affinity(&self, project_id: &str, worktree_id: &str, worker: &str, observed_at_millis: u64) -> Result<(), WorkerError>;
pub fn affinity_hints(&self, project_id: &str, worktree_id: &str) -> Result<AffinityHints, WorkerError>;
```

`claim_head` repairs identity-proven abandoned waiting entries, then removes/returns exactly the first remaining entry under one lock. `remove_queued` is idempotent and refuses another client mapping. Keep project/worktree and project-level healthy worker names as separate canonical affinity records; malformed records fail closed.

- [ ] **Step 5: Run queue and local state tests**

Run: `cargo test --locked --test scheduler_queue --test client_state -- --nocapture`

Expected: PASS; every crash retains either old state or a fully published replacement and cannot delete a live/ambiguous owner.

- [ ] **Step 6: Commit queue persistence**

```bash
git add src/job.rs src/client_state.rs src/error.rs tests/scheduler_queue.rs tests/client_state.rs
git commit -m "feat: persist durable local scheduler queue"
```

---

### Task 3: Wait/No-Wait Dispatch and Automatic/Pinned Run Integration

**Files:**
- Modify: `src/run.rs`
- Modify: `src/cli.rs`
- Modify: `src/lib.rs`
- Modify: `src/output.rs`
- Modify: `src/error.rs`
- Modify: `tests/run_command.rs`
- Modify: `tests/cli_help.rs`

**Interfaces:**
- Consumes: Phase 3 `RunService`, `RunCompletion`, `RemoteJobClient`, `ProjectState`, `PreparedProject`; Task 1 `SchedulerPolicy`; Task 2 queue APIs.
- Produces: updated `RunRequest`, `SchedulerService::submit_and_follow`, and automatic/pinned `worker run [--worker NAME] [--no-wait]`.

- [ ] **Step 1: Write failing CLI/orchestration tests**

Pin `worker run -- npm test`, `worker run --worker mini-2 -- npm test`, `worker run --no-wait -- npm test`, and `worker run --worker mini-2 --no-wait -- npm test`. Record effects in this exact order: inspect/settings/requirements; concurrent fresh fleet probe; no-wait rejection before enqueue/snapshot; enqueue; claim head; affinity/ranking; snapshot; lease; upload/verify/submit; flush acceptance; follow logs. Assert pinned busy/unavailable workers are never silently rerouted, and a later entry cannot snapshot/upload while an earlier compatible entry waits.

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

When the head has a candidate, atomically claim it then call Phase 3 preparation/upload/lease/submit with the original identity and selected configured worker. On lease race, re-enqueue unchanged at its original ID; never bypass an older compatible entry. On preparation failure remove only that entry. Record affinity only after a fresh successful remote observation.

- [ ] **Step 5: Run orchestration regressions**

Run: `cargo test --locked --test run_command --test cli_help --test job_queries --test snapshot_transfer -- --nocapture`

Expected: PASS; Phase 3 explicit worker remains the diagnostic pin and accepted jobs retain original immutable identity.

- [ ] **Step 6: Commit scheduler dispatch**

```bash
git add src/run.rs src/cli.rs src/lib.rs src/output.rs src/error.rs tests/run_command.rs tests/cli_help.rs
git commit -m "feat: schedule queued jobs across workers"
```

---

### Task 4: Explicit Queue and Remote Job Cancellation

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
- Consumes: Task 2 queue persistence, Phase 3 `StatusResponse`, `JobService`, exact lease cleanup, and supervisor process inspection.
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

`worker cancel JOB_ID` first calls `remove_queued`; if found emit a queued-cancelled report. Otherwise load the local record; reject `UnknownRemote`/`CleanupPending` as recovery-required infrastructure; then send exactly one typed cancel request to the recorded worker. Any transport ambiguity is resolved by original-ID status, never another host/job.

- [ ] **Step 5: Run cancellation/lifecycle regressions**

Run: `cargo test --locked --test job_queries --test run_command --test supervisor --test cli_help -- --nocapture`

Expected: PASS; queued cancellation has no remote effects and remote cancellation cannot release capacity before cleanup succeeds.

- [ ] **Step 6: Commit cancellation**

```bash
git add src/job.rs src/job_service.rs src/supervisor.rs src/transfer.rs src/cli.rs src/lib.rs src/run.rs src/output.rs tests/job_queries.rs tests/run_command.rs tests/cli_help.rs
git commit -m "feat: cancel queued and running jobs"
```

---

### Task 5: Fleet Reconciliation and Scheduler Recovery

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
- Consumes: Task 1 probes, Task 2 queue/affinity records, Task 4 cancellation, and Phase 3 `JobService::reconcile_job`/`RemoteJobClient::status`.
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

Cover all terminal states, indexed/non-indexed crash repair delegated to Task 8, corrupt status/lease, timeout, stale observations, jobs from another client, stale affinity, and repeated reconciliation. Assert every repair names a known job ID; no worker-wide file scan, arbitrary path, or broad cleanup is allowed.

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

### Task 6: Scheduler Concurrency, Lifecycle, and Privacy Matrix

**Files:**
- Create: `tests/scheduler_concurrency.rs`
- Modify: `tests/run_command.rs`
- Modify: `tests/job_queries.rs`
- Modify: `tests/client_state.rs`
- Modify: `tests/scheduler_policy.rs`

**Interfaces:**
- Consumes: Tasks 1--5 public and host interfaces.
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

Add `#[doc(hidden)]` test-only barriers/fault points at queue publication, lease-acquire handoff, remote-cancel handoff, and reconciliation response handling. Production defaults remain inert. Fix failures by retaining Task 1--5 authority rules; do not add background threads, hidden daemons, broad cleanup, or alternate retry/lifecycle state.

- [ ] **Step 4: Run focused Phase 3/4 lifecycle suite**

Run: `cargo test --locked --test scheduler_concurrency --test scheduler_policy --test scheduler_queue --test fleet_reconciliation --test run_command --test job_queries --test supervisor --test remote_snapshot -- --nocapture`

Expected: PASS with zero duplicate execution, no extra lease, no FIFO bypass, and no planted-value disclosure.

- [ ] **Step 5: Commit adversarial coverage**

```bash
git add tests/scheduler_concurrency.rs tests/run_command.rs tests/job_queries.rs tests/client_state.rs tests/scheduler_policy.rs
git commit -m "test: harden three-worker scheduler lifecycle"
```

---

### Task 7: Documentation, Three-Mac Acceptance, and Final Phase 4 Gate

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

- **Spec coverage:** Task 1 covers candidate facts/ranking; Task 2 covers durable FIFO and owner cleanup; Task 3 covers default automatic selection, pins, and no-wait; Task 4 covers queued/running cancellation; Task 5 covers fleet/reboot/offline recovery; Task 6 covers concurrency, lifecycle, and privacy; Task 7 covers docs and three-Mac acceptance. Dashboard, artifacts, caches, Docker, general GC, and Phase 5 hardening are intentionally absent.
- **Placeholder scan:** Clean: every task specifies files, interfaces, concrete assertions, RED/PASS commands, implementation boundary, and commit.
- **Type consistency:** Task 1 defines `AffinityHints`/`SchedulerPolicy`; Task 2 returns `AffinityHints` from persisted affinity; Task 3 consumes both and defines public `RunRequest`; Task 4 defines cancellation; Task 5 composes status into fleet reports; Tasks 6--7 consume prior public interfaces.
