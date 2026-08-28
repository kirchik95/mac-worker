# Single-Worker Remote Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an explicit-host `worker run`, durable remote supervision, `worker status`, and reconnectable `worker logs` so one trusted batch command can execute safely on one selected Mac mini and survive client disconnects.

**Architecture:** The client reuses the verified project snapshot pipeline, records a random job ID locally, acquires the selected host's one heavy lease, and uploads the immutable snapshot with system rsync. The host helper verifies the manifest, promotes an immutable remote snapshot, creates an isolated job workspace, durably records acceptance, launches a detached supervisor, and serves bounded status/log queries over fixed SSH commands. Phase 3 requires `--worker NAME`; automatic worker selection, FIFO waiting, sticky affinity, cancellation, and fleet-wide reconciliation remain Phase 4. Exact per-job repair is included now so a dead supervisor cannot permanently wedge the single-worker path.

**Tech Stack:** Rust 2024, system Git, OpenSSH and rsync through injectable process boundaries, `serde`/`serde_json`, SHA-256, UUID v4, `base64`, `humantime`, `libc`, descriptor-relative filesystem operations, `tempfile`, `assert_cmd`, and property/concurrency tests.

**Spec:** `docs/superpowers/specs/2026-08-25-mac-worker-design.md`, especially sections 3, 5–13, 15, 18–20, and 21.

## Global Constraints

- The MacBook remains the only source of truth; remote workspaces are disposable and never synchronize source changes back.
- Phase 3 requires `worker run --worker NAME`; it does not implement automatic selection, FIFO waiting, or sticky affinity.
- The selected host still exposes exactly one heavy slot. Admission uses an atomic host lease now so Phase 4 can consume the same primitive without changing safety semantics.
- Phase 3 bumps the installed host protocol from `1` to `2`. A protocol-1 helper is ineligible until `worker setup NAME` installs the Phase 3 binary; the client never guesses that an older helper supports job commands.
- `worker run -- COMMAND ARG...` preserves a literal UTF-8 argv array. Shell parsing occurs only through explicit `--shell STRING` using `/bin/zsh -lc` on the worker.
- Jobs are non-interactive: no PTY, no stdin forwarding, no SSH-agent forwarding, no privilege elevation, and no automatic runtime or credential installation.
- `worker workers` and Doctor probes remain read-only on every host; observing an absent host data root or lease never creates it.
- The client uploads only a verified immutable `Snapshot`; it never rsyncs a live worktree.
- A host verifies canonical manifest digest, exact entry set, types, modes, sizes, file hashes, and symlink targets before durable acceptance.
- Every remote path is derived from validated `job_id`, `project_id`, `worktree_id`, and manifest digest components below the resolved mac-worker data root. No caller supplies an arbitrary remote path.
- A repeated submission with the same job ID and identical immutable request is idempotent and never executes twice; a different request under that ID is a conflict.
- `accepted` is durable before the client receives success. Ambiguous SSH completion is resolved by querying the same job ID, never by generating or submitting another ID.
- Every mutating host request carries the same random lease token plus a stable request fingerprint. Every failure after possible lease acquisition but before durable acceptance uses those identities for atomic `resolve-or-abandon`; a durable accepted job wins, otherwise a durable abandonment tombstone fences every delayed request before cleanup and lease release. If the host is unreachable, local state retains a recoverable pre-acceptance cleanup marker; no replacement ID is created.
- The detached supervisor owns the command process group, append-only logs, timeout, terminal transition, workspace cleanup, and exact lease release.
- Client disconnect or Ctrl-C while following logs never cancels a job. The job ID is flushed before log following begins.
- Remote stdout/stderr chunks are bounded to 64 KiB per stream/query and resume from byte offsets. Application output is trusted but may contain secrets; mac-worker diagnostics must not add secret values.
- Human streaming commands write raw application bytes; `--json` streaming commands emit versioned NDJSON events and base64 log chunks, never raw application bytes inside JSON output.
- Local and remote JSON state writes use owner-only staging, fsync, atomic rename, and no-follow validation. Corrupt or structurally unexpected state fails closed.
- Persistent job metadata stores a canonical request fingerprint and a content-free command summary, never exact argv/shell contents. The exact execution payload is an owner-only transient file removed and parent-fsynced immediately after successful child launch, and before cleanup/release on every launch-failure, lost, or abandonment path.
- Exit `64` is project/config/usage, `69` is pre-acceptance transport unavailability, `70` is protocol/infrastructure/unknown-acceptance, `74` is local I/O, and `75` is capacity. Once a command starts, its representable exit code is returned unchanged.
- Artifacts, package caches, Docker profiles, `cancel`, safe GC, automatic scheduling, and the dashboard are outside this plan.
- A project that declares any artifact setting fails run preflight with `ARTIFACTS_UNSUPPORTED`; Phase 3 never silently executes and discards requested outputs.

## Delivery Boundary

The independently testable Phase 3 command forms are:

```text
worker run --worker mini-1 -- npm test
worker run --worker mini-1 --timeout 30m --shell 'npm run build && npm test'
worker status [JOB_ID]
worker logs [-f] JOB_ID
```

The public client may use any configured worker explicitly, but it submits only one job to one host per invocation. A busy lease returns `CAPACITY_BUSY` immediately. Phase 4 will make automatic selection and queueing the default while retaining the explicit worker override for diagnosis.

## File Map

```text
Cargo.toml                         base64 dependency
src/job.rs                        bounded IDs, command request, lifecycle records, validation
src/project_state.rs              reusable A/B/C project state and verified snapshot preparation
src/client_state.rs               persistent client ID and atomic local job registry
src/host_store.rs                 descriptor-bound remote data root and atomic JSON/files
src/lease.rs                      one-slot atomic heavy lease and exact release
src/transfer.rs                   fixed-argv SSH control calls and rsync snapshot upload
src/remote_snapshot.rs            incoming verification, immutable cache promotion, workspace copy
src/supervisor.rs                 detached command process group, logs, timeout, terminal state
src/job_service.rs                host acceptance, idempotency, status, log chunks, reconciliation
src/cli.rs                        public run/status/logs and hidden host lifecycle syntax
src/protocol.rs                   versioned job/lease/submit/status/log DTOs
src/error.rs                      capacity, transport, and exact command-exit semantics
src/output.rs                     typed human/JSON run and status output
src/lib.rs                        client and hidden-host dispatch with injectable stdin/output
src/transport.rs                  reusable bounded SSH JSON request primitive
src/snapshot.rs                   explicit publication-root access for transfer
tests/job_protocol.rs             model/state-machine/CLI boundary tests
tests/project_state.rs            reusable stability and cleanup tests
tests/client_state.rs             local registry locking, atomicity, and containment
tests/host_lease.rs               lease races, idempotency, admission, and exact release
tests/snapshot_transfer.rs        rsync argv, ambiguous transfer, cleanup, and path validation
tests/remote_snapshot.rs          manifest verification, cache promotion, workspace isolation
tests/supervisor.rs               exit/signal/timeout/log/process-group lifecycle
tests/run_command.rs              end-to-end fake-transport client and executable CLI tests
docs/phase-three-validation.md    sanitized live validation against one configured Mac mini
README.md                         Phase 3 usage and explicit remaining boundary
```

---

### Task 1: Bounded Job Model and Lifecycle Protocol

**Files:**
- Modify: `Cargo.toml`
- Create: `src/job.rs`
- Modify: `src/protocol.rs`
- Modify: `tests/workers_command.rs`
- Modify: `tests/doctor_command.rs`
- Modify: `tests/setup_command.rs`
- Modify: `src/error.rs`
- Modify: `src/lib.rs`
- Create: `tests/job_protocol.rs`

**Interfaces:**
- Consumes: `protocol::PROTOCOL_VERSION`, `error::{ExitKind, WorkerError}`, `uuid::Uuid`.
- Produces: `JobId`, `ClientId`, `LeaseToken`, `RequestFingerprintMaterial`, `RequestFingerprint`, `CommandSpec`, `CommandSummary`, `JobState`, `JobMeta`, `JobStatus`, `LocalJobRecord`, `LeaseRecord`, `LeaseAcquireRequest`, `LeaseAcquireResponse`, `SubmitRequest`, `SubmitResponse`, `StatusResponse`, `LogStream`, `LogChunk`, and streaming `JsonEvent` DTOs.

- [ ] **Step 1: Write failing identifier, command, and state-transition tests**

Create `tests/job_protocol.rs` with literal tests for:

```rust
#[test]
fn job_ids_are_canonical_lowercase_uuid_components() {
    let id: JobId = "018f0f4a6b5c7d8e9f00112233445566".parse().unwrap();
    assert_eq!(id.to_string(), "018f0f4a6b5c7d8e9f00112233445566");
    for invalid in ["", "../job", "018F0F4A6B5C7D8E9F00112233445566", "018f0f4a-6b5c-7d8e-9f00-112233445566"] {
        assert!(invalid.parse::<JobId>().is_err(), "{invalid:?}");
    }
}

#[test]
fn command_specs_are_bounded_and_mutually_exclusive() {
    assert!(CommandSpec::argv(vec!["npm".into(), "test".into()]).is_ok());
    assert!(CommandSpec::argv(Vec::new()).is_err());
    assert!(CommandSpec::shell("npm test".into()).is_ok());
    assert!(CommandSpec::shell(String::new()).is_err());
}

#[test]
fn lifecycle_allows_only_documented_forward_transitions() {
    assert!(JobState::Accepted.can_transition_to(JobState::Running));
    assert!(JobState::Running.can_transition_to(JobState::Succeeded));
    assert!(!JobState::Succeeded.can_transition_to(JobState::Running));
    assert!(!JobState::Verified.can_transition_to(JobState::Succeeded));
}
```

Add serialization fixtures that assert exact compact JSON keys and one hand-derived request-fingerprint digest, reject unknown or duplicate JSON fields, oversized argv, command values containing NUL, non-canonical 64-character project/worktree/digest identifiers, log chunks over 65,536 decoded bytes, inconsistent terminal outcomes, and timestamps that move backwards. Repeated argument values are valid. Literal argv and explicit shell strings may contain tabs/newlines and other non-NUL UTF-8; JSON escaping and argv transport—not rejection—preserve them byte-for-byte.

Pin `PROTOCOL_VERSION == 2`, update existing probe/setup/Doctor fixtures to derive their expected version from the constant, and retain a literal protocol-1 response test that is classified as `PROTOCOL_MISMATCH`. Config file version remains `1`; only the host wire protocol changes.

- [ ] **Step 2: Run the protocol tests and verify RED**

Run: `cargo test --locked --test job_protocol -- --nocapture`

Expected: FAIL because `crate::job` and the lifecycle DTOs do not exist.

- [ ] **Step 3: Implement the exact bounded model**

Add `base64 = "0.22"` and define in `src/job.rs`:

```rust
pub const MAX_ARG_COUNT: usize = 256;
pub const MAX_ARG_BYTES: usize = 16 * 1024;
pub const MAX_COMMAND_BYTES: usize = 128 * 1024;
pub const MAX_LOG_CHUNK_BYTES: usize = 64 * 1024;
pub const MAX_TIMEOUT_MILLIS: u64 = 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JobId(Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LeaseToken(Uuid);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommandSpec {
    Argv { argv: Vec<String> },
    Shell { shell: String },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Uploading,
    Verified,
    Accepted,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    Lost,
}
```

Use manual `Display`, `FromStr`, `Serialize`, and `Deserialize` implementations so all three UUID-backed IDs accept and emit only simple lower-case 32-hex strings; `Uuid`'s more permissive hyphenated/uppercase parser must not define the wire or path contract. `LeaseToken` is client-generated once beside `JobId` and reused at every mutating host boundary. Use lowercase 64-hex strings for project/worktree/digest/request-fingerprint values and Unix epoch milliseconds as `u64`. `CommandSummary` exposes only `{ mode: argv, arg_count }` or `{ mode: shell }`; it never embeds executable, argument, or shell contents. Implement constructors and `validate()` methods rather than exposing unchecked fields. `JobStatus::transition` must enforce the state graph and monotonic timestamps, distinguish supervisor and child process-start identities, and reject a `running` state without both. Terminal status records final stdout/stderr byte lengths. `LogChunk::new` base64-encodes arbitrary bytes and validates `next_offset == offset + decoded_len`. Versioned `JsonEvent` records cover accepted, base64 log chunk, status, and typed error events for NDJSON streaming.

`RequestFingerprint` is SHA-256 over the canonical compact JSON bytes of a dedicated `RequestFingerprintMaterial` with fixed fields in this order: protocol version, job ID, client ID, lease token, worker inventory name, project ID, worktree ID, manifest digest, relative working directory, timeout milliseconds, resource class, and exact `CommandSpec`. The fingerprint itself is never part of those bytes. The host recomputes it from the full acquire and submit requests; intermediate verify/resolve requests must exactly match the fingerprint already bound into the live lease or tombstone.

Add `WorkerError::Capacity { code, message }`, `WorkerError::Transport { code, message }`, and `WorkerError::CommandExit { code: u8 }`; map them to `75`, `69`, and the exact command code respectively without changing existing mappings.

- [ ] **Step 4: Run focused and existing error/protocol tests**

Run:

```bash
cargo test --locked --test job_protocol -- --nocapture
cargo test --locked error::tests -- --nocapture
cargo test --locked protocol::tests -- --nocapture
```

Expected: all selected tests PASS; updated Doctor/setup fixtures retain their existing behavior while speaking protocol 2.

- [ ] **Step 5: Run format and Clippy for the new model**

Run:

```bash
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
```

Expected: exit `0` with no warnings.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock src/job.rs src/protocol.rs src/error.rs src/lib.rs tests/job_protocol.rs tests/workers_command.rs tests/doctor_command.rs tests/setup_command.rs
git commit -m "feat: define durable job protocol"
```

---

### Task 2: Reusable Stable Project Preparation

**Files:**
- Create: `src/project_state.rs`
- Modify: `src/doctor.rs`
- Modify: `src/lib.rs`
- Modify: `src/snapshot.rs`
- Create: `tests/project_state.rs`
- Modify: `tests/doctor_command.rs`

**Interfaces:**
- Consumes: `ProjectInspector`, `ProjectSettings`, `RequirementDetector`, `InputSelector`, `SnapshotBuilder`, and `Snapshot`.
- Produces: `ProjectState::load`, `ProjectState::prepare`, `ProjectPreparationRequest`, `PreparedProject`, `PreparedProject::cleanup`, and `Snapshot::publication_root`.

- [ ] **Step 1: Write failing stable-preparation tests**

Create real-Git tests which prove:

```rust
let before = ProjectState::load(&runner, repo.root(), &[]).unwrap();
let prepared = ProjectState::prepare(
    &runner,
    &paths.cache,
    ProjectPreparationRequest { project: repo.root().into(), cli_includes: vec![] },
    &before,
).unwrap();
assert_eq!(prepared.state, before);
assert!(prepared.snapshot.publication_root().is_dir());
prepared.cleanup().unwrap();
```

Cover A/B mismatch before selection, B/C mismatch after capture, settings/requirements/HEAD changes, poison-cache proof on the pre-capture mismatch path, exact cleanup on post-capture mismatch, cleanup-I/O precedence, and preservation of all Doctor JSON/human/exit behavior after refactoring. Prove the worktree root uses an empty relative working directory as the implicit publication `tree/` root. From a nested empty current directory, prove the prepared manifest contains that non-empty relative directory and the publication tree materializes it even when no selected file descends from it.

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `cargo test --locked --test project_state --test doctor_command -- --nocapture`

Expected: FAIL because `ProjectState` and `PreparedProject` are not public reusable units.

- [ ] **Step 3: Extract the reusable state boundary without weakening Doctor**

Define:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectState {
    pub context: ProjectContext,
    pub settings: ProjectSettings,
    pub requirements: Vec<String>,
}

pub struct ProjectPreparationRequest {
    pub project: PathBuf,
    pub cli_includes: Vec<String>,
}

pub struct PreparedProject {
    pub state: ProjectState,
    pub selection_warnings: Vec<SelectionWarning>,
    pub snapshot: Snapshot,
}
```

`ProjectState::prepare` reloads the requested state before selection, requires equality with the probed state, selects inputs, captures, reloads after capture, and returns `SNAPSHOT_CHANGED` after exact cleanup on any mismatch. If cleanup fails, the I/O error remains authoritative. `PreparedProject::cleanup(self)` consumes the wrapper and removes only its owned snapshot.

Move `merge_project_requirements` from `doctor.rs` into this module. Make Doctor call the extracted functions and keep its coherent A-based report on probe-window drift. Treat an empty relative working directory as the already-materialized snapshot `tree/` root. Require every non-empty validated relative working directory to exist as a real directory entry and materialize it in the snapshot even when empty; symlink/non-directory working paths fail closed. Add `Snapshot::publication_root(&self) -> &Path`; it returns the owned ready-capture directory containing exactly `tree/` and `manifest.json`, never a caller-derived parent.

- [ ] **Step 4: Run project preparation, Doctor, and snapshot tests**

Run: `cargo test --locked --test project_state --test doctor_command --test snapshot_capture -- --nocapture`

Expected: all tests PASS, including the 1,000-mutation matrix and exact cleanup tests.

- [ ] **Step 5: Commit**

```bash
git add src/project_state.rs src/doctor.rs src/lib.rs src/snapshot.rs tests/project_state.rs tests/doctor_command.rs
git commit -m "refactor: expose stable project preparation"
```

---

### Task 3: Atomic Local Client Identity and Job Registry

**Files:**
- Create: `src/client_state.rs`
- Modify: `src/lib.rs`
- Create: `tests/client_state.rs`

**Interfaces:**
- Consumes: `PathLayout::state`, `JobId`, `ClientId`, `LocalJobRecord`, and `RootedDir` containment rules.
- Produces: `ClientStateStore::open`, `ClientStateStore::client_id`, `create_job`, `load_job`, `update_job`, and `list_jobs`.

- [ ] **Step 1: Write failing registry atomicity and containment tests**

Test first creation/reload of a stable client ID, mode `0600`, exact job JSON round-trip, lexically sorted listing, concurrent create of the same ID, no-clobber conflict for different immutable records, atomic status observation updates, corrupt/truncated JSON, symlinked state roots/components/final files, directory substitution, non-UTF-8 unexpected entries, and an injected failure before/after rename that never exposes partial JSON.

Use a 64-thread barrier test:

```rust
let results = run_concurrently(64, || store.create_job(record.clone()));
assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 64);
assert_eq!(store.load_job(record.job_id).unwrap(), record);
```

All callers submit the identical immutable record; a separate test submits one changed digest and requires `JOB_ID_CONFLICT`.

- [ ] **Step 2: Run the registry tests and verify RED**

Run: `cargo test --locked --test client_state -- --nocapture`

Expected: FAIL because `ClientStateStore` does not exist.

- [ ] **Step 3: Implement descriptor-bound local state**

Store data as:

```text
<state>/client-id
<state>/jobs/<job_id>.json
<state>/.mac-worker-state/<operation-id>/
```

`ClientStateStore::open` creates only missing owner-only directories (`0700`) and rejects symlinks/non-directories. Generate a candidate `client-id` in an owner-only operation file, write canonical UUID bytes plus one LF, fsync it, and publish the complete file with a no-replace link/rename before fsyncing the parent. Concurrent losers delete only their operation files and load the winner; a crash can never expose an empty/truncated final identity. Job writes use owner-only operation files, canonical compact JSON plus LF, fsync, no-replace publication for first creation, and identity-checked atomic replacement for mutable observations. An OS `flock` on `<state>/jobs.lock` serializes list/update; never hold it across SSH or rsync.

Define the immutable portion of the owner-only `LocalJobRecord` as job/client/lease-token/worker/project/worktree/digest/request-fingerprint/content-free-command-summary/created fields. The lease token is a recovery credential: persist it, validate it, and never render it in human/JSON output or diagnostics. Never persist the exact argv or shell string locally. Updates may change only the last observed remote state, timestamps, and uncertainty/cleanup marker.

- [ ] **Step 4: Run registry and rooted filesystem tests**

Run: `cargo test --locked --test client_state --test rooted_fs -- --nocapture`

Expected: all tests PASS and no operation entries remain after successful writes.

- [ ] **Step 5: Commit**

```bash
git add src/client_state.rs src/lib.rs tests/client_state.rs
git commit -m "feat: persist local job identities"
```

---

### Task 4: Safe Host Store and Atomic Heavy Lease

**Files:**
- Create: `src/host_store.rs`
- Create: `src/lease.rs`
- Modify: `src/cli.rs`
- Modify: `src/protocol.rs`
- Modify: `src/probe.rs`
- Modify: `src/transport.rs`
- Modify: `src/doctor.rs`
- Modify: `src/output.rs`
- Modify: `src/paths.rs`
- Modify: `src/lib.rs`
- Create: `tests/host_lease.rs`
- Modify: `tests/workers_command.rs`
- Modify: `tests/doctor_command.rs`

**Interfaces:**
- Consumes: `PathLayout::data`, `LeaseRecord`, `LeaseAcquireRequest`, `JobId`, `ClientId`, and host probe facts.
- Produces: `HostStore::open`, `HostStore::incoming_job`, `HostStore::verified_receipt`, `HostStore::job`, `HostStore::snapshot`, `HostStore::begin_job`, opaque `StagedJob`/`WorkspaceReceipt` types, `HostStore::job_index`, `LeaseService::acquire`, host-internal `LeaseService::release_after_cleanup`, `LeaseService::load`, `SlotState`, a bounded `LeaseSummary`, protocol-2 probe occupancy, and hidden `host lease-acquire`. There is no client-callable raw lease-status or lease-release command; public occupancy is the bounded probe summary and exact resolution arrives in Task 8.

- [ ] **Step 1: Write failing host path and lease-race tests**

Cover creation beneath an isolated XDG data root, modes `0700`/`0600`, rejection of symlinked ancestors/final components, validated identifier-to-path mapping (including the global job index), and preservation of unrelated entries. Inject crashes after every lease staging write/fsync/directory-fsync/publish boundary and prove recovery sees either no lease or one complete valid lease, never an empty/corrupt live slot; operation residue alone never reports busy. Add a simultaneous 64-contender test with distinct job IDs and client-generated lease tokens:

```rust
let outcomes = acquire_concurrently(&store, 64);
assert_eq!(outcomes.iter().filter(|outcome| outcome.acquired).count(), 1);
let winner = outcomes.iter().find(|outcome| outcome.acquired).unwrap();
assert_eq!(LeaseService::new(&store).load().unwrap().unwrap().job_id, winner.job_id);
```

Also prove identical reacquisition is idempotent, changed client/job/token returns `CAPACITY_BUSY`, release is unavailable at the CLI boundary, internal release requires an exact job/client/token plus durable terminal-or-abandoned proof and a completed-cleanup receipt, a mismatched release preserves the lease, unsafe or corrupt lease JSON never gets removed, and expiry metadata alone never authorizes deletion without supervisor reconciliation. Under the admission lock, an existing matching accepted disposition returns `ExistingAccepted` without acquiring a lease, a different fingerprint/client is `JOB_ID_CONFLICT`, and an abandoned disposition is `JOB_ABANDONED`; no completed job ID can strand a fresh lease.

For admission, inject host facts and test rejection when free space is below `max(50 GiB, 20% of total)`, memory pressure is critical, or swap exceeds `2 GiB`.

Extend probe/CLI coverage so an absent data root or lease reports `slot_state: idle` without creating directories; a valid lease reports `busy` plus only a bounded job/project/worktree summary; and a corrupt lease fails the host probe closed. Human `worker workers` output must visibly distinguish idle from busy, while JSON inventory and Doctor consume the same typed occupancy fields. A busy host may still be healthy, but it is not immediately eligible for a new Phase 3 run. Keep atomic lease acquisition—not the probe snapshot—as the authoritative race check. Add `total_disk_bytes` to the probe facts and prove the 20% admission calculation uses the total of the same filesystem as the data root.

- [ ] **Step 2: Run lease tests and verify RED**

Run: `cargo test --locked --test host_lease -- --nocapture`

Expected: FAIL because host storage and lease services do not exist.

- [ ] **Step 3: Implement the host-owned directory and lease protocol**

`HostStore` resolves one absolute data root from the host runtime and owns only:

```text
incoming/<job_id>/<lease_token>/
verified/<job_id>.json
jobs/<project_id>/<worktree_id>/<job_id>/
snapshots/<project_id>/<worktree_id>/<digest>/
leases/heavy/
job-index/<job_id>.json
locks/jobs/<job_id>/
```

Each global index file is one canonical atomic `JobDisposition`: `accepted` contains validated project/worktree IDs plus request fingerprint; `abandoned` contains the request fingerprint plus SHA-256 of the random lease token, never the token itself. Both contain job/client IDs and timestamps. The disposition is permanent until future safe GC and is the authority for global job-ID uniqueness and delayed-request fencing.

Use descriptor-relative no-follow operations and validate all ID components before lookup. `LeaseAcquireRequest` carries the full fingerprint material plus claimed fingerprint; the host recomputes it before admission but persists only the fingerprint and content-free summary. Heavy lease acquisition first builds an owner-only operation directory containing a fully written/fsynced canonical `lease.json`, including job ID, client ID, a client-generated 32-hex lease token, request fingerprint, creation/expiry milliseconds, timeout, project/worktree IDs, digest, and resource class; it fsyncs that directory. While holding the same per-job admission lock used by resolve/verify/submit, it first loads the global disposition: a matching accepted job returns `LeaseAcquireResponse::ExistingAccepted`, an abandoned or conflicting disposition fails without a lease, and only an unused job ID may publish the complete directory as `leases/heavy` with a no-replace atomic rename. A crash can leave only operation-owned staging, never an empty live lock. If the live directory exists, read and validate the record; return idempotent `Acquired` only for the exact immutable request and token, otherwise return a typed busy response.

`HostStore::begin_job` returns an opaque descriptor-bound `StagedJob` beneath the operation namespace for validated project/worktree/job IDs. Tasks 6–7 may add only declared workspace and job files through that handle; only `StagedJob::publish_complete`, supplied the matching workspace receipt, can fsync the tree and no-replace rename it to the final nested job path. No component may construct or publish a final job path independently.

Define hidden CLI forms:

```text
worker host lease-acquire
```

Acquire requests arrive as bounded JSON on stdin. Extend the executable boundary with:

```rust
pub fn run_with_stdio_in_context(
    cli: Cli,
    runner: &dyn ProcessRunner,
    runtime: &RuntimeContext,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8;
```

Keep existing `run_with_io`/`run_with_io_in_context` as compatibility wrappers over an empty reader. Hidden host responses always emit one compact JSON record independent of global `--json`. Bound request input to 1 MiB and reject trailing non-whitespace bytes.

Extend the protocol-2 `ProbeResponse` with:

```rust
pub slot_state: SlotState,
pub active_lease: Option<LeaseSummary>,
pub total_disk_bytes: u64,
```

The public summary omits client identity and absolute paths. `ProbeCollector` discovers the same host data root and uses a read-only `LeaseService::load_if_present` path that treats a missing root as idle and never calls a create-on-open API; transport, Doctor, JSON output, and the human worker table consume these typed fields rather than inferring occupancy from prose. Decode the protocol version before enforcing the full protocol-2 shape: an actual legacy protocol-1 probe payload must remain a clean `PROTOCOL_MISMATCH`, while a protocol-2 payload missing occupancy or disk-total fields is `INVALID_RESPONSE`.

- [ ] **Step 4: Run host lease, CLI, and setup regression tests**

Run: `cargo test --locked --test host_lease --test workers_command --test doctor_command --test cli_help --test setup_command -- --nocapture`

Expected: all tests PASS; host commands do not load client inventory and setup behavior is unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/host_store.rs src/lease.rs src/cli.rs src/protocol.rs src/probe.rs src/transport.rs src/doctor.rs src/output.rs src/paths.rs src/lib.rs src/main.rs tests/host_lease.rs tests/workers_command.rs tests/doctor_command.rs tests/cli_help.rs
git commit -m "feat: acquire atomic worker leases"
```

---

### Task 5: Fixed-Argv SSH Control and Rsync Snapshot Transfer

**Files:**
- Create: `src/transfer.rs`
- Modify: `src/transport.rs`
- Modify: `src/snapshot.rs`
- Modify: `src/protocol.rs`
- Modify: `src/host_store.rs`
- Modify: `src/lease.rs`
- Modify: `src/cli.rs`
- Modify: `src/lib.rs`
- Create: `tests/snapshot_transfer.rs`
- Modify: `tests/host_lease.rs`

**Interfaces:**
- Consumes: `WorkerEntry`, `Snapshot::publication_root`, `JobId`, `ClientId`, `LeaseToken`, `RequestFingerprint`, `ProcessRunner`, and bounded host JSON DTOs.
- Produces: `SshJsonTransport::request`, `RsyncTransport::upload`, hidden binary-pass-through `host rsync-receive`, `RsyncServerExecutor`, a per-job `TransferGuard`, token-scoped incoming storage, `TransferReceipt`, and `TransferFailureDisposition`.

- [ ] **Step 1: Write failing transport request tests**

Use a recording runner to assert an acquire request is exactly:

```text
/usr/bin/ssh
-o BatchMode=yes
-o ConnectTimeout=5
-o ForwardAgent=no
-o ClearAllForwardings=yes
--
mac1
~/.local/bin/worker host lease-acquire
```

The compact request JSON must be the only stdin payload. Add success/non-zero/255/launch/deadline/stdout-overflow/invalid-UTF-8/invalid-JSON/trailing-data tests and ensure raw SSH stderr is replaced by bounded content-free diagnostics before it reaches typed reports.

For rsync, assert a literal fixed argv shape containing only stock macOS `/usr/bin/rsync`-compatible flags: `--archive`, `--delete`, `--no-owner`, `--no-group`, a fixed SSH transport string with forwarding disabled, and a generated `--rsync-path` containing only the fixed installed helper command plus canonical job/client/token/fingerprint components. The exact local publication root is one argv element with a trailing slash; the remote destination is the configured SSH alias plus a fixed literal sink name, never a host-returned path. Do not require unsupported `--protect-args`. Test a local publication root with spaces plus spaces/newlines/Unicode in snapshot children without placing any child path on argv, and run a local compatibility test against `/usr/bin/rsync --help`/a real temporary transfer.

Race an interrupted/lost-response upload against `resolve-or-abandon`. The remote receiver must hold a token-bound `TransferGuard` for its entire lifetime: if it started first, resolution waits and then cleans; if the tombstone won first, the receiver refuses before opening its sink. After an `Abandoned` response no delayed receiver may recreate/write `incoming`. A killed receiver releases its OS lock and leaves only token-scoped evidence eligible for the next exact resolution.

- [ ] **Step 2: Run transfer tests and verify RED**

Run: `cargo test --locked --test snapshot_transfer -- --nocapture`

Expected: FAIL because `SshJsonTransport` and `RsyncTransport` do not exist.

- [ ] **Step 3: Implement bounded control requests and upload**

Define:

```rust
pub struct SshJsonTransport<'a> {
    runner: &'a dyn ProcessRunner,
}

impl SshJsonTransport<'_> {
    pub fn request<Req: Serialize, Res: DeserializeOwned>(
        &self,
        worker: &WorkerEntry,
        operation: HostOperation,
        request: &Req,
        policy: ProcessPolicy,
    ) -> Result<Res, WorkerError>;
}

pub struct RsyncTransport<'a> {
    runner: &'a dyn ProcessRunner,
}

impl RsyncTransport<'_> {
pub fn upload(
        &self,
        worker: &WorkerEntry,
        snapshot: &Snapshot,
        identity: &TransferIdentity,
    ) -> Result<TransferReceipt, WorkerError>;
}
```

`HostOperation` maps only enum variants to fixed remote command literals. Every mutating request DTO includes job/client/lease-token/request-fingerprint fields; retries serialize identical bytes. For rsync only, generate `--rsync-path=~/.local/bin/worker host rsync-receive <job> <client> <token> <fingerprint>` from those strict lowercase hex components and use the fixed remote sink operand `incoming`; no project-controlled text or arbitrary path enters the remote command.

`host rsync-receive` validates the exact live lease and disposition while holding the admission lock, then acquires a per-job OS transfer lock in the global order admission→transfer. It rechecks the tombstone/lease, releases admission while retaining `TransferGuard`, and uses an injectable `RsyncServerExecutor` to `execve` stock `/usr/bin/rsync --server` with inherited binary stdin/stdout only after validating the appended server argv against the one allowed receive shape and replacing the literal sink with the descriptor-resolved `incoming/<job_id>/<lease_token>/` directory. Immediately before exec it deliberately clears `FD_CLOEXEC` only on the transfer-lock descriptor, closes every unrelated descriptor, and proves in tests that the rsync child retains that lock until exit; otherwise exec would silently break fencing. Resolve-or-abandon acquires admission then the same transfer lock, so it can never publish `Abandoned` until a live receiver has exited. A receiver arriving after the tombstone fails closed. No shell or caller path selects the actual destination. This one hidden command is explicitly an rsync binary-protocol pass-through rather than a JSON endpoint; on pre-exec failure it emits only a bounded content-free stderr code and no stdout bytes.

The rsync policy allows 15 minutes, captures at most 256 KiB per stream, and returns transferred file/byte counters only from a separately bounded `--stats` parser. A failure before host verification never reports acceptance. The caller retains the exact job ID and lease for explicit abandon/reconciliation.

- [ ] **Step 4: Run transfer, process, and worker transport tests**

Run: `cargo test --locked --test snapshot_transfer --test process_runner --test workers_command -- --nocapture`

Expected: all tests PASS and no request invokes a local or remote shell with project-controlled text.

- [ ] **Step 5: Commit**

```bash
git add src/transfer.rs src/transport.rs src/snapshot.rs src/protocol.rs src/host_store.rs src/lease.rs src/cli.rs src/lib.rs tests/snapshot_transfer.rs tests/host_lease.rs
git commit -m "feat: upload verified snapshots"
```

---

### Task 6: Remote Snapshot Verification, Cache Promotion, and Workspace Copy

**Files:**
- Create: `src/remote_snapshot.rs`
- Modify: `src/host_store.rs`
- Modify: `src/rooted_fs.rs`
- Modify: `src/cli.rs`
- Modify: `src/protocol.rs`
- Modify: `src/lib.rs`
- Create: `tests/remote_snapshot.rs`

**Interfaces:**
- Consumes: `HostStore`, `SnapshotManifest`, `ManifestEntry`, acquired `LeaseRecord`, and incoming `tree/` plus `manifest.json`.
- Produces: `RemoteSnapshotService::verify_and_promote`, `RemoteSnapshotService::load_verified`, `RemoteSnapshotService::materialize_workspace`, persistent `VerifiedReceipt`, revalidated `VerifiedRemoteSnapshot`, `WorkspaceReceipt`, and hidden `host snapshot-verify`.

- [ ] **Step 1: Write failing adversarial remote verification tests**

Create a literal valid incoming bundle, then independently mutate manifest bytes, digest, project/worktree IDs, entry order, duplicate path, missing/extra file, file bytes, mode, type, symlink target, tracked-deletion order, relative-working-directory entry, and root structure. Every mutation must return `MANIFEST_MISMATCH` or `UNSAFE_REMOTE_SNAPSHOT`, publish no cache entry, create no workspace, retain only exact job-owned evidence, and never touch a sentinel outside the host root. A delayed verify carrying a stale/wrong lease token or an existing abandonment tombstone must be fenced before publication.

Add symlink attacks in every ancestor/final position, FIFO/socket/device entries, hard-link aliasing where supported, cross-device source/destination injection, concurrent promotion of the same digest, and pre-existing cache tests. Identical concurrent promotion may reuse one verified cache; a conflicting pre-existing digest path fails closed. Simulate a fresh host-helper process after verification: `load_verified` must reopen the canonical receipt/cache and succeed only for the exact live lease token hash/fingerprint/IDs; corrupt, missing, stale-token, tombstoned, or cache-mutated receipts fail closed.

- [ ] **Step 2: Run remote snapshot tests and verify RED**

Run: `cargo test --locked --test remote_snapshot -- --nocapture`

Expected: FAIL because remote verification and promotion do not exist.

- [ ] **Step 3: Implement exact verification and immutable promotion**

Define:

```rust
pub struct VerifiedRemoteSnapshot {
    pub project_id: String,
    pub worktree_id: String,
    pub digest: String,
    pub cache_root: PathBuf,
    pub manifest: SnapshotManifest,
}

pub fn verify_and_promote(
    &self,
    lease: &LeaseRecord,
    expected_digest: &str,
) -> Result<VerifiedRemoteSnapshot, WorkerError>;

pub fn load_verified(
    &self,
    lease: &LeaseRecord,
    request_fingerprint: &RequestFingerprint,
) -> Result<VerifiedRemoteSnapshot, WorkerError>;

pub fn materialize_workspace(
    &self,
    snapshot: &VerifiedRemoteSnapshot,
    staged_job: &mut StagedJob,
) -> Result<WorkspaceReceipt, WorkerError>;
```

Acquire locks in the global admission→transfer order so verification cannot race a live rsync receiver. Read `manifest.json` through an opened descriptor with an 8 MiB limit; require its bytes to equal `manifest.canonical_bytes()` and its digest to equal the request digest and live lease digest, while job/client/token/fingerprint separately equal the live lease. Reject an abandonment disposition before and again under those locks. Validate all paths with the existing `RelativePath` rules, require one unique manifest entry per exact tree entry, inspect/re-hash through `RootedDir`, and never follow symlinks. An empty relative working directory denotes the implicit publication `tree/` root; every non-empty relative working directory must be represented by a real directory entry in the manifest and tree.

Promote an operation-owned verified directory into `snapshots/<project>/<worktree>/<digest>` with no-replace atomic rename. If the exact cache already exists, fully revalidate it before reuse. Make cached files/tree read-only. After promotion/revalidation, atomically persist canonical `verified/<job_id>.json` containing job/client IDs, SHA-256 of the lease token, request fingerprint, project/worktree IDs, manifest digest, cache key, and timestamp—never arbitrary paths or the raw token. Only then may `host snapshot-verify` return success; identical retries validate and reuse the receipt.

Because `host submit` is a separate process, it calls `load_verified`: open the receipt through `HostStore`, match its token hash/fingerprint/IDs against the exact live lease and submit request, reopen the immutable cache, and fully revalidate its canonical manifest/tree before returning a fresh in-memory `VerifiedRemoteSnapshot`. Through the opaque `StagedJob` handle, materialize `workspace/tree` by cloning/copying only manifest-declared entries into its unpublished owner-only tree; it must never hard-link mutable job files to the immutable cache and must not publish the final job directory. Apply writable owner modes `0700` for directories, `0600`/`0700` for regular files, recreate symlinks byte-exact, and return a receipt bound to the staging handle for Task 7's complete-job publication.

The hidden `snapshot-verify` command accepts only expected IDs/digest and returns a compact `VerifiedSnapshotResponse`; it does not accept paths.

- [ ] **Step 4: Run remote snapshot, rooted filesystem, and manifest suites**

Run: `cargo test --locked --test remote_snapshot --test rooted_fs --test snapshot_capture -- --nocapture`

Expected: all tests PASS, including the existing 1,000 local mutation captures.

- [ ] **Step 5: Commit**

```bash
git add src/remote_snapshot.rs src/host_store.rs src/rooted_fs.rs src/cli.rs src/protocol.rs src/lib.rs tests/remote_snapshot.rs
git commit -m "feat: verify remote snapshot bundles"
```

---

### Task 7: Durable Acceptance and Detached Supervisor

**Files:**
- Create: `src/supervisor.rs`
- Create: `src/job_service.rs`
- Modify: `src/host_store.rs`
- Modify: `src/lease.rs`
- Modify: `src/cli.rs`
- Modify: `src/protocol.rs`
- Modify: `src/lib.rs`
- Create: `tests/supervisor.rs`

**Interfaces:**
- Consumes: `VerifiedRemoteSnapshot`, `WorkspaceReceipt`, `LeaseRecord`, `SubmitRequest`, `CommandSpec`, `JobMeta`, `JobStatus`, and the staged job workspace.
- Produces: `JobService::submit`, `SupervisorLauncher`, `Supervisor::run`, hidden `host submit` and `host supervise`, an exclusive per-job admission lock, append-only `stdout.log`/`stderr.log`, and terminal lease cleanup.

- [ ] **Step 1: Write failing durable-acceptance and execution tests**

Use injected clocks, launchers, and child executors to prove:

- a complete job directory containing canonical owner-only meta/status/logs/transient execution payload is published atomically, the global job index is recoverably published before launch, and `accepted` exists before submit success;
- a lost submit response followed by identical submit/status never starts twice;
- the same job ID with different command/digest/worker fields returns `JOB_ID_CONFLICT`;
- delayed submit after an abandonment tombstone is fenced; concurrent same-ID submit calls publish one job directory/index and launch one supervisor;
- argv mode preserves metacharacters as literal arguments and never invokes a shell;
- shell mode invokes exactly `/bin/zsh -lc <string>` and is the only shell path;
- the child receives no PTY/stdin/SSH agent, a clean environment, controlled `PATH`, unique `HOME`/`TMPDIR`, `MAC_WORKER_*` IDs, and the manifest's relative working directory;
- stdout/stderr append byte-exact to separate regular files and terminal status records their exact final lengths;
- exit `0`, exit `7`, SIGTERM, timeout TERM→10s→KILL, launch failure, status-write failure, and workspace-cleanup failure produce exact states and error fields;
- every launch/lost/abandon failure path deletes and parent-fsyncs the transient execution payload before any lease release; deletion failure retains the lease and no diagnostic exposes payload bytes;
- lease release is host-internal and happens only after a terminal state, successful targeted workspace cleanup, and an exact cleanup receipt;
- supervisor death/reboot evidence is never guessed as success, and a surviving recorded command process group is terminated and proven absent before lost cleanup/release.

Inject crashes after every job-directory file write/fsync, directory fsync/publish, index write/fsync/publish, transient-payload read/removal, and launcher handshake boundary. Recovery must observe either pre-acceptance state that atomic resolve-or-abandon can fence and clean, or one complete accepted indexed job; never partial meta/status and never duplicate execution.

Add a real subprocess test whose argv contains spaces, quotes, `$()`, backticks, semicolons, Unicode, and newlines and assert no marker file from shell interpretation exists.

- [ ] **Step 2: Run supervisor tests and verify RED**

Run: `cargo test --locked --test supervisor -- --nocapture`

Expected: FAIL because `JobService` and `Supervisor` do not exist.

- [ ] **Step 3: Implement durable handoff and process-group supervision**

Define:

```rust
pub trait SupervisorLauncher: Send + Sync {
    fn launch(&self, job_id: JobId) -> Result<(), WorkerError>;
}

pub struct JobService<'a> {
    store: &'a HostStore,
    leases: LeaseService<'a>,
    snapshots: RemoteSnapshotService<'a>,
    launcher: &'a dyn SupervisorLauncher,
}

impl JobService<'_> {
    pub fn submit(&self, request: SubmitRequest) -> Result<SubmitResponse, WorkerError>;
}
```

Submission first acquires the exclusive no-follow admission lock for the job ID and rejects an abandonment disposition. Under that lock it validates the exact lease token/fingerprint, calls `RemoteSnapshotService::load_verified` to reopen/revalidate the durable receipt and immutable cache in this submit process, obtains `HostStore::begin_job`, asks `RemoteSnapshotService::materialize_workspace` to populate that opaque staging handle, and retains its staging-bound `WorkspaceReceipt`. It adds home/tmp, empty logs, initial `accepted` `status.json`, and immutable `meta.json` with exactly these non-secret fields: protocol version, job/client IDs, worker inventory name, project/worktree IDs, manifest digest, request fingerprint, content-free command summary, relative working directory, timeout/resource class, and creation timestamp. Lease token and exact command are excluded from persistent remote metadata. The owner-only transient `execution.json` contains the exact command plus token only until launch/cleanup. `StagedJob::publish_complete` requires the matching workspace receipt, fsyncs every file and directory, and no-replace renames the complete directory into `jobs/<project>/<worktree>/<job>`. It then publishes an `accepted` disposition at `job-index/<job>.json` with the validated project/worktree mapping and fingerprint; a crash between those publications is recoverable from the exact live lease plus request IDs before any launch. Identical repeat submissions repair/return the same durable state, while a different immutable request conflicts. The accepted/abandoned disposition makes job ID uniqueness global across project/worktree paths.

Only the first indexed submission launches the same installed worker binary as `host supervise <job-id>`. The launcher detaches into a new session/process group with null stdio, then waits up to five seconds for the supervisor to record its own process-start identity and remain `accepted`, advance to `running`, or record a proven terminal launch failure; only an accepted/running observation with that identity may be acknowledged as success. A handshake timeout never writes `lost` merely because time elapsed: while holding the admission/supervisor coordination lock, it may mark `lost` only after proving the recorded supervisor process identity is absent. Otherwise it returns an ambiguous accepted result for same-ID status reconciliation.

Crash recovery distinguishes two accepted cases. A complete indexed job with no supervisor identity has not passed the supervisor's pre-command identity write, so `ensure_supervisor` may launch again; the exclusive supervisor lock lets at most one candidate record identity and execute, while all losers exit. Once any supervisor identity has ever been recorded, its later absence is never retried as a new execution because the child-launch boundary could be ambiguous; reconciliation follows the `lost` path instead. This preserves at-most-once execution while allowing a crash between acceptance and the first launch to recover.

`Supervisor::run` resolves the job ID only through the validated global index, acquires an exclusive per-job supervisor lock, refuses a second live supervisor, and first atomically records its own PID/start identity while status remains `accepted`; this is the launch handshake. It opens and validates the transient execution payload under the job descriptor, copies it into memory, launches the command as a new process group, records the child PID/start identity, transitions `accepted -> running`, and unlinks/fsyncs the transient payload immediately after successful launch. It polls without busy waiting, applies the configured timeout, closes/fsyncs both logs, atomically records the terminal outcome plus exact final log lengths, removes only the mutable workspace/home/tmp through exact rooted cleanup, creates an unforgeable in-process cleanup receipt, and calls host-internal exact lease release. Retained sanitized meta/status/logs/index remain queryable.

Every path after opening `execution.json` owns its erasure: child-launch failure, invalid payload, handshake loss proven dead, dead-supervisor reconciliation, and pre-acceptance abandonment unlink the transient file and fsync its parent before producing a cleanup receipt or releasing the lease. Erasure failure is cleanup failure, so the lease stays held and retryable; exact command bytes are never retained as diagnostic evidence.

The supervisor environment is exactly controlled locale, fixed `/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin`, job HOME/TMPDIR, and non-secret IDs. Environment profiles, package caches, Docker, and artifacts are not added here.

- [ ] **Step 4: Run supervisor, lease, and process tests**

Run: `cargo test --locked --test supervisor --test host_lease --test process_runner -- --nocapture`

Expected: all tests PASS; no duplicate execution and no broad process killing.

- [ ] **Step 5: Commit**

```bash
git add src/supervisor.rs src/job_service.rs src/host_store.rs src/lease.rs src/cli.rs src/protocol.rs src/lib.rs tests/supervisor.rs
git commit -m "feat: supervise durable remote jobs"
```

---

### Task 8: Authoritative Status, Bounded Logs, and Ambiguous-Acceptance Reconciliation

**Files:**
- Modify: `src/job_service.rs`
- Modify: `src/client_state.rs`
- Modify: `src/transfer.rs`
- Modify: `src/cli.rs`
- Modify: `src/protocol.rs`
- Modify: `src/lib.rs`
- Create: `tests/job_queries.rs`
- Modify: `tests/host_lease.rs`

**Interfaces:**
- Consumes: local job-to-worker mapping, remote `meta.json`/`status.json`, append-only logs, and `SshJsonTransport`.
- Produces: `JobService::status`, `JobService::read_log`, `JobService::resolve_or_abandon`, `JobService::reconcile_job`, `RemoteJobClient::status`, `RemoteJobClient::log_chunk`, `RemoteJobClient::resolve_preacceptance`, `PreacceptanceDisposition`, `RemoteJobClient::resolve_submission`, hidden `host status`/`host log-chunk`/`host resolve-or-abandon`, and bounded follow cursors.

- [ ] **Step 1: Write failing status/log/reconciliation tests**

Test every lifecycle state and terminal outcome, unknown/missing/corrupt job state, metadata/status disagreement, invalid transitions, stale PID identity, index-to-meta disagreement, indexed lookup after lease release, and partial local observations. Log tests cover empty/binary/UTF-8/non-UTF-8 bytes, independent stdout/stderr offsets, exact 64 KiB boundary, offset at EOF, offset beyond EOF, file growth between calls, truncation/pathname-replacement rejection, symlink/device log rejection, base64 round-trip, and terminal logs larger than one chunk draining fully to recorded final lengths.

Add reboot/dead-supervisor reconciliation tests proving supervisor absence alone never releases capacity: if the recorded child process group survives, reconciliation sends targeted TERM, waits ten seconds, sends targeted KILL if needed, and verifies that exact group is gone before marking `lost`, cleaning, and releasing. A live/reused/ambiguous supervisor or process-group identity fails closed and retains the lease. Also prove exact mutable cleanup precedes exact lease release and cleanup failure retains the lease. Race `host resolve-or-abandon` against delayed lease-acquire/verify/submit operations: the shared admission lock plus job tombstone must yield either a fenced clean abandonment or one durable accepted indexed job, never a deleted accepted job, resurrected lease, or second execution. Status absence alone is never sufficient evidence to abandon.

Run a deterministic 100-case acceptance matrix. At each boundary—before host receipt, after receipt before durable verified, after meta write, after accepted write, after supervisor launch, and after SSH stdout—inject a disconnect. Assert each original job ID produces either zero executions with a typed pre-acceptance failure, or exactly one execution discoverable by status; never create a replacement ID and never report a second run.

- [ ] **Step 2: Run query tests and verify RED**

Run: `cargo test --locked --test job_queries -- --nocapture`

Expected: FAIL because query and reconciliation services do not exist.

- [ ] **Step 3: Implement fixed query operations**

Define:

```rust
impl JobService<'_> {
    pub fn status(&self, job_id: JobId) -> Result<StatusResponse, WorkerError>;
    pub fn read_log(
        &self,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    ) -> Result<LogChunk, WorkerError>;
    pub fn resolve_or_abandon(
        &self,
        request: ResolveOrAbandonRequest,
    ) -> Result<ResolveOrAbandonResponse, WorkerError>;
    pub fn reconcile_job(&self, job_id: JobId) -> Result<StatusResponse, WorkerError>;
}

impl RemoteJobClient<'_> {
    pub fn resolve_preacceptance(
        &self,
        worker: &WorkerEntry,
        request: &ResolveOrAbandonRequest,
    ) -> Result<PreacceptanceDisposition, WorkerError>;
    pub fn resolve_submission(
        &self,
        worker: &WorkerEntry,
        request: &SubmitRequest,
        submit_result: Result<SubmitResponse, WorkerError>,
    ) -> Result<SubmitResponse, WorkerError>;
}
```

Status resolves job ID through the validated global disposition index (or, during a pre-index crash window, the exact matching live lease), then reads immutable meta and mutable status through descriptors and validates their shared identifiers/digest. Before replying, the host runs narrowly scoped reconciliation for that exact job. A complete accepted job that has never recorded a supervisor identity calls idempotent `ensure_supervisor`; the exclusive supervisor lock elects at most one executor. Once an identity was recorded, it is never re-executed. If that supervisor identity is proven absent but the recorded command process group remains, reconciliation takes ownership of the same targeted timeout path: TERM, ten-second wait, KILL if needed, then proof the exact group is absent. Only after both supervisor and command group are proven absent may it mark `lost`, remove the transient execution payload plus that job's mutable workspace/home/tmp, fsync, and release only its exact matching lease through the internal cleanup-proof API. Ambiguous or reused identities retain the lease and return infrastructure failure. This is lifecycle repair required to keep the single worker usable after a supervisor death or reboot; Phase 4 will add fleet-wide discovery and scheduling around the same primitive.

Log reads use `pread` on an opened regular no-follow file and return at most `min(limit, 65_536)` bytes. Before and after the read, compare the opened descriptor's device/inode with a fresh descriptor-relative no-follow lookup of the log name; pathname replacement or truncation below the requested offset yields a typed infrastructure error. The client polls each stream from its own `next_offset`; `-f` sleeps one second between empty non-terminal responses. After terminal status it keeps reading both streams until their offsets equal the exact final lengths recorded in status, then requires stable EOF before stopping. A query disconnect changes no state.

Ambiguous submit resolution performs bounded status queries for the same ID for up to 30 seconds, then invokes atomic `resolve-or-abandon` with the same job/client/lease-token/fingerprint. Under the same admission lock used by acquire/verify/submit, the host returns the durable accepted state if a matching accepted disposition/job exists. A complete matching accepted job directory from the crash window before index publication also wins: validate it and repair the accepted disposition before returning. Only when neither accepted proof exists does it acquire the same per-job transfer lock held by `rsync-receive`, publish and fsync an abandonment disposition, remove matching token-scoped `incoming`/verified-receipt/incomplete-workspace state, delete/fsync any transient execution payload, and release the exact lease through the internal cleanup-proof API. Cleanup failure leaves the tombstone and lease intact for an idempotent retry. The response is idempotently `Accepted`, `Abandoned`, or `CleanupPending`; status absence alone never decides, and `Abandoned` proves no delayed receiver can still write. An unreachable host returns `UNKNOWN_REMOTE` with the same job ID and recovery commands.

Lease-acquire, upload, and verify failures cannot have started a command. `resolve_preacceptance` nevertheless invokes the same atomic `resolve-or-abandon` with the original token/fingerprint because a response or delayed request may still be in flight. It returns `PreacceptanceDisposition::{Abandoned, Accepted(StatusResponse), CleanupPending { code }}` with bounded content-free diagnostics. The caller preserves the original typed failure only for `Abandoned`; `Accepted` switches to same-ID status/log following, and `CleanupPending` records that marker locally with the job ID plus recovery command. A later `worker status <id>` retries the exact resolution before reporting the local record.

`resolve_preacceptance` returns `Result` because a valid nonzero versioned host error is authoritative, not network ambiguity: preserve its validated code/message as the existing coded `WorkerError::Protocol` form. Only transport timeout/unavailability or an invalid/missing response becomes `PreacceptanceDisposition::UnknownRemote { code: "UNKNOWN_REMOTE" }`; a valid `JOB_ID_CONFLICT` must never be rewritten as unknown, cleanup-pending, or the original submit error.

- [ ] **Step 4: Run query, supervisor, and transport tests**

Run: `cargo test --locked --test job_queries --test supervisor --test snapshot_transfer -- --nocapture`

Expected: all tests PASS; the 100 injected disconnects produce no duplicates.

- [ ] **Step 5: Commit**

```bash
git add src/job_service.rs src/client_state.rs src/transfer.rs src/cli.rs src/protocol.rs src/lib.rs tests/job_queries.rs tests/host_lease.rs
git commit -m "feat: query remote jobs and logs"
```

---

### Task 9: Public Run, Status, and Logs Orchestration

**Files:**
- Modify: `src/cli.rs`
- Modify: `src/lib.rs`
- Modify: `src/output.rs`
- Modify: `src/error.rs`
- Create: `src/run.rs`
- Create: `tests/run_command.rs`
- Modify: `tests/cli_help.rs`

**Interfaces:**
- Consumes: `ProjectState`, `PreparedProject`, `ClientStateStore`, `WorkersService`, `LeaseService` host operations, `RsyncTransport`, `RemoteJobClient`, and typed reports.
- Produces: public `worker run`, `worker status`, `worker logs`, `RunService::submit_and_follow`, `StatusService`, `LogsService`, `RunReport`, `StatusReport`, and exact executable exit behavior.

- [ ] **Step 1: Write failing CLI and orchestration tests**

Pin these forms:

```text
worker run --worker mini-1 -- npm test -- --literal
worker run --worker mini-1 --project /repo --include fixtures/generated/** --timeout 45m -- npm test
worker run --worker mini-1 --shell 'npm run build && npm test'
worker status
worker status 018f0f4a6b5c7d8e9f00112233445566
worker logs 018f0f4a6b5c7d8e9f00112233445566
worker logs -f 018f0f4a6b5c7d8e9f00112233445566
```

Reject missing/unknown worker, empty argv/shell, simultaneous argv+shell, invalid timeout, artifacts/env/cache flags from future phases, any non-default `[artifacts]` project configuration, and extra status/log positionals. Artifact-config rejection must happen before probing, snapshot capture, lease acquisition, or any remote mutation and use stable code `ARTIFACTS_UNSUPPORTED`.

Pin streaming JSON behavior: `worker --json run` and `worker --json logs [-f]` emit one compact versioned NDJSON `JsonEvent` per line; log data is base64 with explicit stream/offset/next-offset. Accepted/status/error events use the same stream, and JSON-mode stderr stays empty unless writing stdout itself fails. No raw application byte may appear outside an event. Human mode retains raw stdout/stderr bytes. Non-streaming `status`, `workers`, and Doctor remain one ordinary JSON document.

With a recording runner and isolated runtime, prove the exact order:

1. inspect project/settings/requirements;
2. probe only the explicitly selected inventory worker;
3. reload stable project state;
4. select and capture snapshot;
5. reload state and create local job record;
6. acquire lease;
7. rsync publication root;
8. verify remote snapshot;
9. submit the same job ID;
10. flush job ID;
11. follow bounded logs/status;
12. clean the exact local snapshot on every path.

Add poison objects at every later phase to prove blockers stop before downstream effects. Test command exits `0`, `7`, `64`, and signal-derived `143` are returned exactly after execution, while project/transport/infrastructure/I/O/capacity retain `64/69/70/74/75`. A broken stdout/stderr writer remains I/O `74` and never cancels the remote job.

At each post-lease pre-acceptance failure (lost acquire reply, interrupted rsync, lost verify reply, and acknowledged verification failure), assert atomic resolve-or-abandon uses the original job/client/token/fingerprint. If resolution reports accepted, continue that same job; if unreachable, the local record remains `cleanup_pending`. `worker status <id>` later retries resolution and never invents or executes a replacement job.

If lease acquisition returns `ExistingAccepted` for the same persisted local identity/fingerprint, skip upload/verify/submit and resume status/log following for that indexed job. Conflicting or abandoned dispositions never reach transfer.

- [ ] **Step 2: Run public command tests and verify RED**

Run: `cargo test --locked --test run_command --test cli_help -- --nocapture`

Expected: FAIL because public run/status/log commands are absent.

- [ ] **Step 3: Implement public orchestration without a scheduler**

Define:

```rust
pub struct RunRequest {
    pub worker: String,
    pub project: PathBuf,
    pub cli_includes: Vec<String>,
    pub timeout: Option<Duration>,
    pub command: CommandSpec,
}

pub struct RunService<'a> {
    pub runner: &'a dyn ProcessRunner,
    pub config: &'a Config,
    pub paths: &'a PathLayout,
    pub client_state: &'a ClientStateStore,
}

pub fn submit_and_follow(
    &self,
    request: RunRequest,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<RunCompletion, WorkerError>;
```

Resolve `--worker` only through `Config::worker`; never accept an SSH destination. Load project settings first and reject declared artifacts before any remote operation. Merge worker-declared and project requirements exactly as Doctor does and require one ready, idle probe. Use the project-config timeout unless CLI overrides it. The job ID, client-generated lease token, canonical request fingerprint, and sanitized local record are created once and reused through every reconciliation path.

After acceptance, human mode writes and flushes `job <id> accepted on <worker>\n` before following raw logs; JSON mode writes and flushes the equivalent typed accepted event before base64 log events. Human status output omits full local paths, exact commands, and secret environment values; JSON derives from the same sanitized typed reports. `worker status` without an ID lists at most the 100 newest local records in descending creation order, says when older rows were omitted, and performs at most 16 five-second remote refreshes for active/unknown jobs; `worker status JOB_ID` always addresses that exact record. `worker logs` never mutates remote state apart from host-internal dead-supervisor reconciliation performed by the status query.

Restructure `run_with_stdio_in_context` so streaming commands receive writers directly while setup/doctor/workers continue through `CommandOutput`. Do not buffer unbounded application logs in a `CommandOutput` string.

- [ ] **Step 4: Run all public command and snapshot regression tests**

Run: `cargo test --locked --test run_command --test cli_help --test doctor_command --test snapshot_capture -- --nocapture`

Expected: all tests PASS; Doctor remains read-only and temporary snapshots are still cleaned.

- [ ] **Step 5: Run the full local gate**

Run:

```bash
cargo fmt --all --check
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked --release
```

Expected: all commands exit `0` with no Clippy warnings.

- [ ] **Step 6: Commit**

```bash
git add src/cli.rs src/lib.rs src/output.rs src/error.rs src/run.rs tests/run_command.rs tests/cli_help.rs
git commit -m "feat: run one durable remote job"
```

---

### Task 10: Adversarial Coverage, Documentation, and Phase 3 Live Acceptance

**Files:**
- Modify: `README.md`
- Create: `docs/phase-three-validation.md`
- Modify: `tests/run_command.rs`
- Modify: `tests/job_queries.rs`
- Modify: `tests/remote_snapshot.rs`
- Modify: `tests/supervisor.rs`

**Interfaces:**
- Consumes: all Phase 3 public/hidden interfaces and the configured three-worker inventory.
- Produces: documented single-worker execution evidence and the go/no-go handoff to Phase 4.

- [ ] **Step 1: Add the final adversarial matrix**

Add deterministic tests for:

- 100 disconnect points with zero duplicate executions;
- 100 simultaneous same-ID submit calls with exactly one supervisor launch;
- 256 literal argv cases containing shell metacharacters with zero shell interpretation;
- 100 remote manifest/tree mutations with zero accepted jobs;
- delayed lease/verify/submit operations raced against resolve-or-abandon with one fenced terminal decision and no slot resurrection;
- planted secrets in snapshot candidates, exact command argv/shell strings, local/remote persistent metadata, host-helper stderr, config errors, and mac-worker-generated diagnostics; exact commands may exist only in memory and the transient owner-only execution payload before child launch;
- oversized request/response/log/state files, truncated JSON, terminal NUL, non-UTF-8 paths/bytes, symlink swaps, and operation cleanup failures;
- binary/non-UTF-8 multi-chunk terminal logs in human and NDJSON modes, with both streams drained to recorded final lengths;
- empty nested relative working directories and artifact-config preflight rejection before any remote effect;
- two linked local worktrees submitting distinct immutable snapshots under the same project ID;
- Ctrl-C/client-disconnect simulation proving the accepted supervisor continues and status/log reconnect by the original ID.

- [ ] **Step 2: Run the adversarial tests**

Run: `cargo test --locked --test run_command --test job_queries --test remote_snapshot --test supervisor -- --nocapture`

Expected: all matrices PASS with no duplicate execution, unsafe publication, or planted-secret disclosure by mac-worker.

- [ ] **Step 3: Update README usage and boundaries**

Document:

```bash
cargo build --release
./target/release/worker run --worker mini-1 -- npm test
./target/release/worker status
./target/release/worker logs -f <job-id>
```

Explain that Phase 3 requires an explicit worker, runs trusted non-interactive batch commands, preserves jobs after client disconnect, and returns no source changes. Mark automatic scheduling/queueing, cancellation, artifact transfer, caches, Docker profiles, safe GC, and dashboard as subsequent phases; say explicitly that configured artifacts cause preflight rejection in Phase 3. Document human raw streaming versus `--json` NDJSON/base64 events and state that application logs may contain application-emitted secrets.

- [ ] **Step 4: Run sanitized live acceptance on one Mac mini**

Use an isolated temporary clone and isolated local XDG roots. Reinstall the release helper only on `mini-1`, then run:

```text
worker setup mini-1
worker run --worker mini-1 -- /usr/bin/printf 'phase-three-ok\n'
worker run --worker mini-1 --shell 'printf start; sleep 5; printf end'
worker status <job-id>
worker logs <job-id>
worker logs -f <job-id>
worker run --worker mini-1 -- /bin/sh -c 'exit 7'
```

For the exit-7 check, `/bin/sh -c` is passed as literal argv and intentionally selected by the operator; mac-worker itself must not synthesize it. During the sleep job, terminate the local log follower, reconnect by the same ID, and prove one remote execution and byte-exact continuation. Attempt a concurrent second job and require `CAPACITY_BUSY` without execution. Verify the remote lease is released only after terminal cleanup, retained job metadata/logs contain no local complete paths or planted values, no unrelated remote entries change, and the original clone remains untouched.

Record only sanitized commands, job/worker IDs shortened to non-sensitive prefixes, counts, states, timings, exit results, and before/after fingerprints of mac-worker-owned remote namespaces. Do not record source, origins, credentials, complete paths, or raw environment values.

- [ ] **Step 5: Write `docs/phase-three-validation.md`**

Include the exact software commit, worker protocol version, test/gate results, successful and non-zero command outcomes, disconnect/reconnect proof, capacity proof, cleanup/lease evidence, and remaining Phase 4 boundary. Explicitly distinguish automated fake-transport evidence from live single-host evidence.

- [ ] **Step 6: Run the final gate**

Run:

```bash
cargo fmt --all --check
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked --release
git diff --check
```

Expected: all commands exit `0`; the full suite reports zero failures and Clippy reports zero warnings.

- [ ] **Step 7: Commit**

```bash
git add README.md docs/phase-three-validation.md tests/run_command.rs tests/job_queries.rs tests/remote_snapshot.rs tests/supervisor.rs
git commit -m "test: validate single-worker execution"
```

## Final Review Checklist

- [ ] Every public/control host command uses a typed versioned response; the one documented `rsync-receive` endpoint is binary pass-through, and diagnostics never contaminate JSON or rsync-protocol stdout.
- [ ] The exact immutable local snapshot—not the live worktree—is the only uploaded source.
- [ ] Lease publication is crash-atomic; release is host-internal and requires terminal/abandoned proof plus completed targeted cleanup.
- [ ] Atomic resolve-or-abandon tombstones fence every delayed mutating request and cannot race an accepted job.
- [ ] A global validated job index resolves retained jobs by job ID after lease release and enforces global ID uniqueness.
- [ ] Manifest verification and workspace creation never follow symlinks or cross the host data root.
- [ ] Acceptance is durable and repeat submission cannot execute twice.
- [ ] Every ambiguous network boundary retains and queries the original job ID.
- [ ] Supervisor/log/status behavior survives client disconnect and preserves exact terminal results.
- [ ] Terminal log followers drain both streams to recorded final lengths; JSON streaming is valid NDJSON with base64 bytes.
- [ ] Persistent metadata contains only request fingerprints and sanitized command summaries; exact execution payloads are transient.
- [ ] The relative working directory is materialized and artifact configuration fails before any remote side effect.
- [ ] No local or remote broad delete, global process kill, Docker prune, runtime install, or credential copy exists.
- [ ] Phase 4 can reuse lease, local registry, status, and log interfaces without changing their wire contracts.
- [ ] README and live validation make the explicit-worker/no-scheduler boundary unmistakable.
