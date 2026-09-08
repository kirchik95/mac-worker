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

- [x] **Step 6: Resolve review and integrate.**

Controller performs task review (spec and quality), scoped fixes as needed, whole-branch review, then verifies fast-forward integration into local main under the existing user authorization. No remote push or installation.

## Implementation verification note

Implementation, task review, whole-branch review and the scoped re-review of the final fix are complete. Runtime commit `cb7530743b8027d6e180521a3c6806bac2ee4850` was integrated into local main by fast-forward from `f29cf81`. The frozen all-target suite passed **1,598 tests across 62 top-level Cargo sections**, with zero failures or ignored tests and exit 0. No Rust source changes or concurrent builds occurred during this run. The subsequent closeout changes documentation only.

The final scoped run passed 618 tests, including all 57 runner tests; fmt, all-target Clippy with warnings denied, and whitespace checks passed. Full verification uses actual local private files/Git and fake remotes, without a live fleet or provider agent.

Some journal/lifecycle tests were added with or after implementation. Controlled mutations independently detected broken partial-append recovery, skipped suffix validation, absent writer exclusion and omitted durable completion; this is mutation evidence, not a claim that every test originally ran RED. The initial three tail/restart regressions and later concurrency regressions did fail before their fixes.

The earlier 1,585-test full run had two late reader/journal guard changes and a shared build directory, so it did not establish an immutable final binary set. A later run on `290f8c6` was interrupted with SIGINT/exit 130 before the final fix wave. Neither is final-source evidence. The successful frozen run on `cb75307` supersedes that coverage limitation.

## Review fix 1: finalization ownership

The per-turn journal flock now fences cancellation, completed reconciliation, runner cleanup, handoff publication/failure and queued close. Each path checks the current exact task/turn row and owner after acquiring the fence, reloads task state before mutation, and retains the writer through base release, runner clearing and row retirement. Dead-owner adoption rechecks both the row snapshot and process observation under the same fence. A competing finalizer skips a busy writer; `say` continues to reject the follow-up until old-row retirement.

Three deterministic real-Git/channel regressions reproduced the original cancellation, completed-reconciliation and completed-runner-retry races before the fix. They assert that old cleanup cannot clear a newer runner or delete its task-scoped base pin. Final scoped verification and the subsequent controller-owned frozen full suite are recorded in the roadmap; decoder flag/cursor strengthening remains outside this fix.

## Review fix 2: ordinary fence contention

Runner execution now retries only a busy journal fence, with exact task/turn/owner validation before every attempt and again under the acquired writer. This lets a legitimate runner survive queued status refresh and the short parent-handoff window while retaining the complete cleanup fence. Ownership loss or old-row replacement stops execution with `TASK_BUSY`; other journal/I/O errors still propagate. Channel-gated regressions cover successful execution exactly once, owner change, and replacement-turn preservation. The existing `ClientStateConcurrencyHook` supplies the inert contention synchronization point. The final scoped and frozen full-suite results below cover this fix.

## Final review wave: conditional task projections

Accepted cancellation and idle/queued remote status refresh now publish their response only if the complete task snapshot used for the request still matches under the existing state lock. This preserves successor turn history, runner ownership and fetched/publication metadata without waiting for the journal's drain lifetime. A stale response is a no-op; local write/corruption failures propagate, while failed remote observations remain best effort. The earlier deferred idle-refresh race is included and resolved by this final-wave scope ruling. Deterministic regressions finish A and optionally start B through public `say` before releasing delayed cancellation/refresh responses, preserve A's completed log/checkpoint, and verify cancellation responsiveness and local failure propagation. The final scoped and frozen full-suite results below cover this fix.


## Final verification and review record

| Check | Result | Evidence |
| --- | --- | --- |
| Frozen full all-target suite on `cb75307` | 1,598 passed, 0 failed, 0 ignored; 62 suites; exit 0 | `/private/tmp/runner-final-frozen-v2-all-targets.log` |
| Frozen final scoped suite | 618 passed, 0 failed; 11 suites; exit 0 | `/private/tmp/final-fix-scoped.log` |
| Formatting | Exit 0, empty output | `/private/tmp/final-fix-fmt.log` |
| All-target Clippy, warnings denied | Exit 0, no warnings | `/private/tmp/final-fix-clippy.log` |
| Whitespace check | Exit 0 | `git diff --check` |

Suite totals use the last summary in each top-level Cargo `Running` section; nested filtered library subprocess summaries are excluded. The final scoped suite includes library 316, agent adapters 50, Cursor/OpenCode 21, client state 42, runner dispatch 15, scheduler queue 64, task command 9, conversation 15, logs 21, project context 8, and runner 57.

```sh
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --all-targets
cargo test --locked --offline --lib --test client_state --test scheduler_queue --test runner_dispatch --test task_conversation --test turn_runner --test task_logs --test task_project_context --test task_command --test agent_adapters --test agent_cursor_opencode
cargo fmt --all --check
cargo clippy --locked --offline --all-targets -- -D warnings
git diff --check
```

Independent task review identified stale cleanup and then transient fence contention; commits `2323cb1` and `290f8c6` resolved both and passed scoped re-review. Final whole-branch review identified stale accepted-cancel publication. Commit `cb75307` resolved it and the sibling idle-refresh race; its scoped final re-review found no new Critical, Important or Minor issue. Six deterministic regressions cover late cancellation with/without a successor, late idle refresh, responsive cancellation during drain, local write failure, and current-record corruption. Required metadata, exact queue ownership and completed log/checkpoint bytes survive delayed responses.

Optional decoder strengthening for nonzero cursors with `accepted=false` remains excluded: both production drain paths record acceptance first and reviewers found no demonstrated replay, byte loss or false-completion failure from that state combination.

## Controller decisions, in order

Ruling: Legacy nonempty logs without a checkpoint fail explicit writer recovery/follow completion rather than infer offsets — original mixed bytes cannot establish each stream's position — cost: an old in-flight turn needs manual recovery/migration, while its existing log remains readable.

Ruling: One integrated implementation task owns journal, runner and task lifecycle — cancellation and reconciliation participate in the same completion invariant — cost: a larger patch/review than separate file edits.

Ruling: Verify the two late invalid-state guards with the complete library suite, complete task_logs suite, fmt and all-target Clippy after the ongoing full suite — only selected terminal startup waiting and pending drained-without-acceptance validation changed after the full build; avoid another broad rerun without a wider risk — cost: the full-suite result is not byte-identical to final source and must be disclosed; review may require broader validation. Agent must freeze after these changes and identify the exact delta in the report.

Ruling: Include the pre-existing idle/no-queue refresh race in the final accepted-cancel fix wave using the same conditional task-projection mechanism — both stale writers use the same task replacement primitive, and leaving the sibling race would preserve the same metadata-loss failure — cost: a slightly wider final patch and an additional deterministic regression rather than a separate later change.

The third ruling records an earlier verification decision; the final frozen all-target pass above removes its remaining coverage limitation. These decisions are retained here before deleting this plan's temporary SDD workspace.
