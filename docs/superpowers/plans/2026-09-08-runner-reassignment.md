# Waiting Runner Reassignment Implementation Plan

> **For agentic workers:** Execute the independently owned context/TaskClient and queue/runner tasks, then integrate and review together. Use superpowers:executing-plans or superpowers:subagent-driven-development for the task checkpoints.

**Goal:** Start eligible parked tasks on idle Mac workers without adding waiting runner processes.

**Architecture:** Transfer an existing detached PID between task turns using one fenced queue publication. Persist private execution context so that the recipient can belong to another worktree. Existing task/turn records, worker leases and reconciliation remain authoritative at their existing boundaries.

**Tech Stack:** Rust 2024, rooted filesystem storage, flock-protected JSON queue, existing SSH/Git transport.

**Spec:** `docs/superpowers/specs/2026-09-08-runner-reassignment-design.md`

## Global constraints

- Do not change public task/queue JSON or the remote wire protocol.
- Preserve one heavy lease per worker, run max_parallel, pins, per-worker FIFO, exact process ownership and recovery.
- Use the current isolated branch, including admission `03450e2` and main's transfer lock `b305e32`.
- Tests use local Git repositories and fake worker boundaries; no real coding agents are required.

## Task 1: Private execution context and startup accounting

Files: create `src/client_state/task_context.rs`, `tests/task_project_context.rs`; modify `src/task_client.rs` and owner-recovery tests in `tests/task_conversation.rs`.

- [x] Add storage/roundtrip, permissions, non-UTF8, mismatched identity and redirected-file regressions before implementation.
- [x] Implement the two context APIs from the spec with immutable rooted publication.
- [x] Persist the inspected worktree root before submission handoff, inside existing rollback handling.
- [x] Resolve per-task paths for execution-related TaskClient operations with legacy fallback.
- [x] Count unique live identities and test the live-queue-owner/missing-runner startup window.

## Task 2: Fenced queue transfer and FIFO

Files: create `src/client_state/runner_dispatch.rs`, `tests/runner_dispatch.rs`; modify `src/client_state.rs`.

- [x] Reproduce the saturated pinned-pool case and the displaced older-row FIFO failure.
- [x] Build one lock-internal task/turn index, excluding incomplete/terminal recipients.
- [x] Select with existing worker policy, pins, reservations and run-cap checks; publish both queue transitions together.
- [x] Update both runner records without releasing the state lock; preserve rows after publication errors.
- [x] Extend normal claims' older-row checks to executable parked tasks.
- [x] Cover pending intents, run caps, cancellation, stale owners, concurrent donors and partial publication recovery.

## Task 3: Detached execution and cross-project regression

Files: modify `src/turn_runner.rs`, `src/lib.rs`, `tests/turn_runner.rs`; amend the pool spec's runner/FIFO description.

- [x] Add explicit `run_detached` mode; the existing `run` method remains task-specific.
- [x] On a failed own claim, observe the fleet and attempt the fenced transfer before sleeping.
- [x] Execute the selected turn with its task/turn identity and private project path; keep cleanup and result import bound to it.
- [x] Run a bounded integration scenario where the parked recipient finishes on an idle peer while the donor stays queued.
- [x] Cover distinct repositories and linked worktrees, and verify attached mode retains the original task.

## Task 4: Integration and review

- [x] Run targeted tests and inspect failures before broadening:

```sh
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --test task_project_context --test runner_dispatch --test task_conversation --test scheduler_queue --test turn_runner
cargo fmt --all --check
cargo clippy --locked --offline --target-dir /private/tmp/mac-worker-admission-target --all-targets -- -D warnings
cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --all-targets
```

- [x] Review the complete diff against the acceptance contracts; address blocking findings.
- [x] Commit the verified stage separately and update the root roadmap with actual results.

## Main implementation verification — `b68aff9`, 2026-09-08

- Final focused run: 15 queue-reassignment tests, 8 private-context tests and 41 turn-runner tests passed (64 total). Existing conversation and scheduler suites also passed in the complete run.
- Final `cargo test --locked --offline --target-dir /private/tmp/mac-worker-admission-target --all-targets`: exit 0; the harness summaries report 1,551 passed, 0 failed, 0 ignored across 61 test binaries (64 summaries including nested harnesses).
- `cargo fmt --all --check`, `cargo clippy --locked --offline --target-dir /private/tmp/mac-worker-admission-target --all-targets -- -D warnings`, and `git diff --check`: exit 0.
- Independent review against `9a893bd` identified and verified fixes for legacy FIFO/context eligibility, context durability before queue publication, and unvalidated cwd inheritance. No remaining actionable findings in the scoped review.
- Integration tests use actual local Git repositories and linked worktrees with simulated SSH/agent boundaries. The worker fleet and real coding agents were not run or changed.
- Existing concurrent count/spawn/adopt startup accounting remains a separate reliability follow-up; the new reassignment transition starts no process.

## Ready-pin fast-path follow-up

A final regression reproduced unnecessary peer SSH probes before a ready pinned turn's own claim. The runner now first attempts its requested worker; after a miss it extends observations to remaining workers only when parked work exists. Existing observations are reused.

The follow-up passed 140 tests across all five affected suites: `turn_runner` (42), `runner_dispatch` (15), `task_project_context` (8), `scheduler_queue` (64) and `task_conversation` (11). Final fmt, Clippy with `-D warnings`, diff checks and independent review passed. The complete all-targets result above belongs to `b68aff9`; after this narrow observation-order change the affected suites were rerun, without another complete all-targets run.
