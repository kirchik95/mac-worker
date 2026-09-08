# Runner log recovery implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Drain terminal logs fully, resume without duplicated bytes, and complete the selected CLI follower after publication.

**Architecture:** A private per-turn append journal commits native byte positions together with visible log bytes. Existing `TerminalLogDrain` validates remote EOF. Durable completion is shared by runner recovery, cancellation/reconciliation and the read-only CLI.

**Tech Stack:** Existing Rust, serde, libc flock, RootedDir and local fixture tests; no new dependency.

**Spec:** `docs/superpowers/specs/2026-09-08-runner-log-recovery-design.md`.

## Global Constraints

- No remote protocol change or new dependency is required.
- Never truncate or rewrite the visible log.
- Preserve pins, capacity limits, live/ambiguous owner checks, saved project contexts, and submission rollback fencing.
- Legacy logs remain readable with non-follow commands.
- A nonempty log without a sidecar has ambiguous native offsets: writer recovery returns `LOG_CHECKPOINT_MISSING` and preserves all bytes.
- Follow without provable completion reports `LOG_COMPLETION_UNKNOWN`.
- No real fleet, provider agent, push or deployment is part of local verification.

### Task 1: Durable turn logs across runner, recovery and CLI

This is one reviewable task because every producer, recovery path and reader must agree on when a turn is finished. Do not ship a checkpoint writer while cancellation/reconciliation can still discard it.

**Files:**
- Create `src/runner_log.rs` (journal, append writer, strictly read-only snapshot/committed reads and focused crash-window unit tests).
- Modify `src/lib.rs` only for its module declaration.
- Modify `src/turn_runner.rs` for managed writes, restored cursors, native drain and completion/cleanup ordering.
- Modify `src/task_client.rs` for completion-aware follow, recovery/cancel/say lifecycle.
- Modify `src/client_state.rs` or its focused private modules only if existing rooted access/queue operations cannot expose the required safe operation.
- Tests: `tests/turn_runner.rs`, `tests/task_logs.rs`, `tests/task_conversation.rs`; existing related suites may need faithful remote status fixture extensions.
- Update README and the pool reliability roadmap for final behavior and test evidence.

**Interfaces:** consume `RemoteJobClient::{status,log_chunk,task_status}`, `TerminalLogDrain::{new,observe_chunk,set_terminal_status,revalidate_terminal_status}`, `RootedDir::{open_private_append,validate_private_append_binding,read_private_regular_chunk,write_private_atomic_no_replace,replace_private_regular_exact}`. Produce one crate-private runner-log API supporting writer open, offsets, append chunk, accepted event once, finish, and read-only committed snapshot. Keep concrete Rust names/types cohesive inside this task; do not expand public JSON task records.

- [x] **Step 1: Confirm RED and complete the failure matrix.**

Three failing tests already exist in the working tree and have been run individually against the original runtime:

```sh
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --test turn_runner runner_drains_both_final_streams_beyond_one_chunk -- --exact
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --test turn_runner runner_reads_logs_when_submit_already_reports_a_terminal_task -- --exact
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --test turn_runner restarted_runner_resumes_both_logs_without_repeating_a_committed_prefix -- --exact
```

Observed failures: 65,536 rather than 150,011 stdout bytes; zero rather than 150,011 after immediate terminal response; 65,536 after restart. The fixture returns immutable seekable stdout (150,011 bytes of `0xf1`) and stderr (91,017 bytes of `0xfe`), exposing both loss and replay. Expected public failure code for the injected interrupted SSH is `SSH_LAUNCH_FAILED`.

Add focused tests before the corresponding implementation for journal crash boundaries, unsafe/corrupt state, completion/cancel/reconcile/say and selected-turn follow. Use production calls with real local files. Test-only crash setup belongs inside the journal test module; do not add public test-only methods.

- [x] **Step 2: Implement the append transaction.**

For committed log length `L`, pending payload `P` and actual length `N`, enforce this recovery rule:

```rust
// After strict deserialization and rooted binding checks:
if n < l || n > l + payload.len() as u64 { return Err(invalid_state()); }
let written = (n - l) as usize;
if stored_suffix != payload[..written] { return Err(invalid_state()); }
// Append payload[written..], sync, validate final binding/length, then CAS
// the sidecar from pending state to next committed state.
```

Use checked arithmetic, strict bounded serde input and task/turn identity validation. Persist intent before log bytes. Reopen/recover after any ambiguous write failure. Record accepted-event and terminal completion atomically with their bytes. Read snapshots never take ownership or repair state. Test committed-prefix visibility during pending writes and after failed checkpoint publication.

- [x] **Step 3: Integrate draining and finalization.**

```rust
let mut next = drain.clone();
next.observe_chunk(&chunk)?;
log.append_chunk(&chunk)?;
drain = next;
```

Restore cursors from the journal. Remove both terminal-status bypasses in `execute` and `execute_accepted_without_prompt`. Validate terminal job identity and bind final byte lengths; confirm EOF for both streams and revalidate terminal job status before finalization. Preserve follower-detach-on-write-error behavior. Apply the exact finish/cleanup ordering in the spec and short-circuit already-completed retries to local cleanup.

- [x] **Step 4: Integrate recovery and read-only follow.**

Audit all terminal cleanup/skip paths in `reconcile_runners`, `terminal_task_for_turn`, `cancel`, `close` and `say`. Incomplete accepted turns keep a recovery row and ownership even after host task completion. Local never-started cancellation commits its outcome. Read-only follow uses the selected journal's committed length and completion, not whole-task closure. Preserve stage 3.1 diagnostics and raw behavior; update its synthetic fixtures to express the new completion contract explicitly. Never trust native JSON as a completion marker.

- [x] **Step 5: Verify and document.**

```sh
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --test turn_runner --test task_logs --test task_conversation --test task_project_context --test runner_dispatch --test scheduler_queue --test agent_adapters --test cli_help
cargo fmt --all --check
cargo clippy --locked --offline --target-dir /private/tmp/mac-worker-admission-target --all-targets -- -D warnings
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --all-targets
```

Run the full suite once after focused checks pass. Tests that bind a loopback socket may require managed sandbox escalation; do not remove those tests. Record exact evidence and disclose the legacy recovery boundary in README/roadmap and the task report. Commit only task files; leave `mac-worker-architecture.html` alone. Request independent review through the controller; do not spawn a reviewer.

- [ ] **Step 6: Resolve review and integrate.**

Controller performs task review (spec and quality), scoped fixes as needed, whole-branch review, then verifies fast-forward integration into local main under the existing user authorization. No remote push or installation.

## Implementation verification note

Implementation and local checks are complete; independent controller review/integration remains pending. The full run passed 1,585 tests across 62 top-level sections. Two bounded late reader/journal guards were subsequently covered by the complete final-source library (316) and reader (21) suites plus fmt/Clippy; see the pool reliability roadmap for exact source-delta disclosure and durable commands. Some tests followed implementation; critical journal guarantees additionally received controlled mutation validation.
