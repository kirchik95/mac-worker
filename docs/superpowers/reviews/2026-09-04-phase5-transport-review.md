# Phase 5 transport and turn execution review - 2026-09-04

## Scope and outcome

Reviewed Task 3 (`f79c755`), Task 5 (`1576b17`), and Task 6 (`c4bf26c`)
against the Phase 5 plan, task-pool design, prior review hardening bar, and
the confirmed agent behaviour in `docs/agent-task-spike.md`.

The review found no blockers, seven major findings, and one minor finding.
Six major findings and the minor finding were fixed on this branch. One major
finding is deferred because its live cancellation writer is Phase 4 code that
the review brief explicitly protects from modification.

## Findings

1. **Major - deferred** - `src/client_state.rs:608-638`,
   `src/job_service.rs:2635-2638` - `TerminalPath::HostCancel`

   Quote: `request_queue_cancel` only removes a waiting entry or marks a
   dispatching entry for cancellation, while `HostCancel` is selected only by
   terminal recovery.

   Scenario: a live host cancellation can persist a cancelled job and remove
   mutable execution state without invoking `TurnTerminalHook`. The task then
   remains Active, its result branch is not published, and its session and
   lease cleanup do not follow the required terminal ordering.

   Deferred plan: in Phase 4's live cancellation writer, after atomically
   persisting `JobState::Cancelled` and before deleting `execution.json`,
   cleaning mutable job state, or releasing the lease, load and validate the
   turn payload and invoke `TurnTerminalHook::invoke(store, job, meta,
   section, TurnTerminal::Cancelled, TerminalPath::HostCancel, None,
   log_truncated)`. Wire the host-cancel transport/runner to that writer and
   add cancel-then-resume end-to-end coverage proving the task becomes Open,
   the session survives, the result branch publishes, and the lease releases
   only afterward. This requires changing `src/client_state.rs` (and likely
   its Phase 4 caller), which the brief forbids.

2. **Major - fixed** - `src/supervisor.rs:2218-2257`,
   `src/task_store.rs:713-781` - `finish_unrecoverable_active_turn`

   Quote: `recover that turn before releasing the lease rather than leaving
   it Active forever`.

   Scenario: if a claimed turn's execution payload is corrupt before its
   `TurnSection` can be decoded, the normal terminal hook has no safe input.
   The old prelaunch-failure cleanup terminalized the job and released its
   lease but left the independently prepared task turn Active forever.

   Fix: locate the unique Active task whose pending turn references the job
   ID and finish it Lost before cleanup and lease release. A regression test
   covers corrupt-payload recovery. Commit `b82d7fe`.

3. **Major - fixed** - `src/supervisor.rs:2651-2721` -
   `Do not wait for pipe EOF`

   Quote: `if stop.load(Ordering::Acquire) { ... break; }`.

   Scenario: an escaped descendant can retain the stdout pipe after its
   process group is gone and write indefinitely. The old pump performed an
   additional blocking read after the supervisor's grace period, preventing
   terminal processing and lease release.

   Fix: retain the current read, then stop the pump immediately when the
   grace-period stop flag is observed. The regression fixture keeps an escaped
   writer open and proves the pump returns. Commit `cbd2a0b`.

4. **Major - fixed** - `src/host_store.rs:3148-3205`,
   `src/git_transport.rs:401-426` - `fchdir(directory_fd)`

   Quote: `hooks.read_private_regular("pre-receive", MAX_HOST_FILE_BYTES)`
   and `mirror.verify_bound()?`.

   Scenario: path-based mirror repair and Git invocation allowed an
   attacker-controlled same-account filesystem change to redirect hook
   replacement or the Git working directory between validation and use. A
   hardlinked `pre-receive` could also be overwritten through repair.

   Fix: perform hook operations through checked rooted descriptors, reject
   unsafe links, run mirror Git commands after descriptor-bound `fchdir`, and
   revalidate the binding after execution. Hardlink repair and descriptor
   binding regressions are covered. Commits `34fde88` and `a7b2a5b`.

5. **Major - fixed** - `src/git_transport.rs:372-399` -
   `RESULT_FETCH_FAILED`

   Quote: `fetched result ref is missing or invalid`.

   Scenario: a failed local `rev-parse` after result fetch used to fabricate
   an all-zero object ID. That could turn a missing or invalid import receipt
   into a plausible result value instead of rejecting the transfer.

   Fix: execute receipt verification through the bounded process runner and
   return `RESULT_FETCH_FAILED` for a failed, non-UTF-8, or invalid ref head.
   A missing-local-ref regression test covers the rejection. Commit `6b35b7e`.

6. **Major - fixed** - `src/turn.rs:673-702` -
   `successful turn did not bind an agent session`

   Quote: `if session.is_none() && terminal == TurnTerminal::Succeeded`.

   Scenario: a successful first turn could be published even if its agent
   output never bound a resumable session. The next submitted turn would then
   lack the required session reference despite the task showing success.

   Fix: reject successful publication without a bound session and route it to
   the existing terminal failure handling. The Codex regression covers this
   invariant. Commit `0647e70`.

7. **Major - fixed** - `src/turn.rs:1036-1079` -
   `O_NOFOLLOW | O_CLOEXEC`

   Quote: `metadata.nlink() != 1`.

   Scenario: checking env-profile metadata by pathname and reopening it for
   reading left a time-of-check/time-of-use swap window; a hardlinked
   owner-readable profile could expose a secret value from a second name.

   Fix: open once with no-follow and close-on-exec, validate that exact
   descriptor as a 0600 single-link regular file owned by the effective user,
   and read a bounded byte stream from it. The hardlink regression is covered.
   Commit `b63014b`.

8. **Minor - fixed** - `tests/remote_snapshot.rs:242-244,1489-1491` -
   `protocol_version: 3` (pre-fix)

   Quote: `PROTOCOL_VERSION`.

   Scenario: Task 3 raised the protocol version to 4, but two negative
   snapshot fixtures kept a literal version 3. Their intended schema failures
   were therefore masked by an earlier `fixture needle` failure during the
   all-target test gate.

   Fix: derive the fixture version from `PROTOCOL_VERSION`, preserving each
   test's intended rejection. Commit `da994ca`.

## Verification after fixes

The following final gate passed in the terminal session:

- `cargo fmt --check`
- `cargo test --locked --all-targets -- --format terse --test-threads=1`
- `cargo clippy --locked --all-targets -- -D warnings`
- `git diff --check`

The complete all-target run was performed outside the restricted terminal
sandbox because its loopback policy otherwise prevents the dashboard listener
from binding; under the normal host environment the dashboard command and web
tests pass. A preceding default-parallel run timed out in two supervisor tests
with fixed polling deadlines (`dead_preidentity_owner_is_reelected_once_after_the_bounded_wait`
and `hidden_submit_detaches_the_same_worker_and_inherited_lock_runs_supervisor`).
Both passed when rerun alone with `--test-threads=1`, as required by the brief,
and the final complete serial all-target gate passed.
