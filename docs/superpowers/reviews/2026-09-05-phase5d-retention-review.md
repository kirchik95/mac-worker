# Phase 5d retention and discard review - 2026-09-05

## Scope and outcome

Reviewed the retention and discard implementation from `b529f40` (`feat:
retain and discard agent task state safely`) and `8d232bd` (`feat: delete
agent sessions on discard where supported`) against Task 4 of the phase 5d
plan and spec sections 6, 12, and 16. The branch was rebased onto `main` at
`3e57eae` before the final gate.

The review found nine Major findings and two Minor findings. All nine Major
findings and one Minor finding are fixed in this branch. One Minor timestamp
finding and one Major descriptor-bound Git finding remain deferred. No
Critical finding remains.

The supplied sanitized metadata was reproduced locally without contacting a
worker. The JSON-only v3 job metadata from the mac1 and mac2 samples is in
`tests/fixtures/gc/legacy-job-meta-v3.json`,
`tests/fixtures/gc/legacy-job-status.json`,
`tests/fixtures/gc/legacy-job-meta-v3-mini2.json`, and
`tests/fixtures/gc/legacy-job-status-v3-mini2.json`. The tests place those
records beside a valid v4 job and also recreate the mini-1 hand-cleaned state
by removing its job-index and lock siblings. Before the fix, the v3 record
propagated `GC_METADATA_INVALID`; after the fix preview continues, reports a
`legacy protocol` candidate or an `inconsistent record` warning, and leaves
the damaged record in place during apply.

## Findings

1. **Major - fixed** - `src/gc.rs:498` (pre-fix `b529f40`), now handled at
   `src/gc.rs:692-719` - per-record GC metadata failure

   > `let meta: JobMeta = read_gc_json(&job, "meta.json", "job metadata")?;`

   Scenario: one legacy protocol-3 job, malformed task, or hand-cleaned job
   sibling caused the whole worker preview to return `GC_METADATA_INVALID`.
   That is exactly what acceptance observed on mini-2/mini-3 and mini-1,
   preventing the operator from seeing or safely applying the rest of the
   inventory.

   Fix: collect each task, job, and mirror record independently. Legacy
   protocol records become non-destructive candidates with reason `legacy
   protocol`; unreadable and inconsistent records become bounded warnings,
   and uncertain projects prevent mirror deletion. Apply skips legacy
   candidates and rechecks sibling state before deleting a valid job. Commits
   `404e1b8` and `9f807f8`. Regressions:
   `gc_preview_reports_legacy_job_metadata_without_aborting_other_records`,
   `gc_preview_reports_a_legacy_task_record_without_aborting`,
   `gc_preview_warns_and_keeps_a_job_with_missing_index_and_lock_records`,
   and `gc_fails_closed_on_malformed_task_metadata`.

2. **Major - fixed** - `src/gc.rs:495-496, 561-589` - active task and turn
   protection

   > `let active_work = inventory.lease_uncertain || self.task_has_active_work(project, &meta, &status, inventory.live_lease_job)?;`

   Scenario: a stale terminal timestamp alone could make GC select a task or
   job while its turn summary was nonterminal, its job status was nonterminal,
   or its lease was live. Applying the candidate could remove the task branch,
   task metadata, or turn job during execution.

   Fix: inventory treats an unreadable lease as fail-closed, protects a live
   lease, nonterminal turn summaries, and nonterminal job status, and repeats
   those checks during apply. Commit `aaa39d3`. Regressions:
   `gc_does_not_prune_a_terminal_task_with_a_live_turn_lease`,
   `gc_does_not_remove_a_terminal_job_while_its_lease_is_live`,
   `gc_does_not_prune_a_task_with_a_nonterminal_turn_job`, and
   `gc_does_not_touch_active_tasks_or_their_base_refs`.

3. **Major - fixed** - `src/lib.rs:1555-1559` - unfetched local task result
   protection

   > `|| task.fetched_head().is_none()`

   Scenario: a terminal local task whose result had not yet been imported
   could leave its transfer repository looking orphaned. Transfer GC could
   then remove the only local copy needed to fetch that result.

   Fix: the client now protects transfer repositories for every nonterminal
   task, task with a live runner, or terminal task without `fetched_head`.
   Commit `9c8ea63`. Regression:
   `terminal_tasks_without_fetched_results_protect_their_transfer_repo`.

4. **Major - fixed** - `src/transfer_repo.rs:264-268, 378-401` - transfer
   repository collection race

   > `let _repo_lock = lock_transfer_repo(parent, repo_id)?;`

   Scenario: submission or result import could be opening or using a transfer
   repository before its base ref or local task protection was visible. A
   concurrent transfer GC could otherwise delete that repository while the
   operation still held a live handle.

   Fix: collection and apply take the per-repository owner-only lock and
   recheck refs and age; `TransferRepo` holds the same lock for its entire
   handle lifetime. The submitter drops its handoff handle before an attached
   runner reopens the same repository in-process, avoiding a self-deadlock.
   Different repositories remain independent. Commits `a4a982f` and
   `92ea209`. Regressions:
   `transfer_gc_waits_for_a_live_transfer_repository_handle` and the full
   attached agent lifecycle suite (`agent_cursor_opencode`, 13 tests).

   Amended 2026-09-08: the lock mode was wrong, not the lock. Holding it
   exclusively for the handle's lifetime made every `submit` and `fetch` for a
   project wait behind a running turn on any worker. Users of a transfer
   repository now hold the lock shared, transfer GC takes it exclusively
   without waiting and reports a repository in use as skipped, and first
   creation runs under a short exclusive lock on `<repo_id>.init.lock`.
   Regressions: `transfer_gc_skips_a_live_transfer_repository_handle_with_a_warning`,
   `two_handles_for_one_repository_open_concurrently`,
   `concurrent_first_creation_yields_one_initialized_repository`.

5. **Major - fixed** - `src/task_store.rs:791-815` (pre-fix
   `8d232bd:791-818`) - discard precondition and durable terminal ordering

   > `self.store.remove_owned_child_committed(&task, "workspace")?;`

   Scenario: discard removed the workspace before proving that the mirror
   existed and before persisting the terminal state. A missing mirror or later
   cleanup failure could leave an open-looking task with part of its state
   removed, while a retry could not reliably distinguish an unfinished discard
   from an ordinary open task.

   Fix: discard proves the mirror first, durably writes `Abandoned` with a
   refreshed timestamp, then removes the workspace, task refs, and optional
   native session. Cleanup remains retryable and native-session failure is a
   bounded warning. Commit `3f1890b`. Regressions:
   `discard_keeps_task_intact_when_the_mirror_is_missing` and
   `close_removes_only_workspace_and_discard_prunes_published_refs`.

6. **Minor - fixed** - `src/task_store.rs:815` (pre-fix
   `8d232bd:818`) - explicit-close retention timestamp

   > `let status = replace_status_record_at(&task, status, next_state, None, now_millis()?)?;`

   Scenario: explicit close reused the old task activity timestamp. A task
   closed just after creation could therefore appear to have been terminal
   for the full old age and become eligible for branch or metadata retention
   immediately.

   Fix: explicit close records the current time for the terminal transition.
   Commit `5c0197a`. Regression:
   `explicit_close_refreshes_the_task_retention_timestamp`.

7. **Major - fixed** - `src/task_store.rs:88-96`,
   `src/agent/mod.rs:600-612` (pre-fix `8d232bd:587-596`) - option-like
   session reference

   > `if session_ref.len() > 256 || session_ref.chars().any(char::is_control) {`

   Scenario: a stored session reference beginning with `-` passed the old
   validation and was placed after agent deletion options. A malformed or
   attacker-controlled binding could consequently change the deletion
   command's option parsing.

   Fix: session bindings and adapter resume/delete paths reject empty,
   option-like, control-containing, and overlong references. Native cleanup
   runs through `/bin/zsh -lc` with fixed shell quoting, the worker login
   environment, a 15-second deadline, and 4 KiB output bounds. Commit
   `60c9e3b`. The adapter and materialization suites cover the rejection and
   bounded request shape.

8. **Major - fixed** - `src/host_store.rs:1997-2004`,
   `src/task_store.rs:785-788, 1275-1284` - discard/session-binding race

   > `let _session_lock = self.store.session_lock()?;`

   Scenario: discard could scan for other references while a concurrent
   session bind was writing the same task or another task. The scan could
   observe the old graph and delete a native session that had just become
   referenced. The existing cross-task reference check also needed to remain
   inside that serialization boundary.

   Fix: close, retention close, and session binding serialize under the
   owner-only `locks/session.lock`; deletion still skips any session referenced
   by another task. Commit `c972223`. Regression:
   `discard_serializes_native_session_delete_with_a_concurrent_session_binding`;
   the shared-reference regression is
   `discard_skips_native_deletion_when_another_task_references_the_session`.

9. **Major - fixed** - `src/task_store.rs:1293-1299` - late binding after
   discard

   > `if status.state().is_terminal() {`

   Scenario: a delayed agent session event arriving after discard could create
   `session.json` on an already terminal task after native deletion had been
   attempted. The durable task would then point at an untracked native
   session, and a later discard could not safely retry deletion.

   Fix: terminal tasks reject new session bindings with `TASK_CLOSED` while
   retaining idempotent binding behavior for live tasks. Commit `3049735`.
   Regression: `discard_rejects_a_late_session_binding_on_the_terminal_task`.

10. **Major - fixed** - `src/task_store.rs:859-879` (pre-fix
    `8d232bd:840-858`) - retention-close failure ordering

    > `self.store.remove_owned_child_committed(&task, "workspace")?;`

    Scenario: automatic retention close removed the workspace and then failed
    while atomically replacing `status.json`. The task could remain Open with
    no workspace, making a later retry observe a state that no longer matched
    the original retention candidate.

    Fix: validate the workspace before mutation, durably write `Closed` and
    its retention timestamp, then remove the workspace. If cleanup fails after
    the state transition, the terminal task remains safe for retry or later
    metadata cleanup. Commit `fe4203b`; the retention regression is
    `gc_closes_idle_open_task_but_preserves_result_branch_and_metadata`.

11. **Minor - deferred** - `src/gc.rs:1358-1362, 1411-1444`,
    `src/transfer_repo.rs:272-275` - mutable or indirect retention clocks

    > `let metadata = root.root_metadata()?;`

    Scenario: mirror and transfer retention use mutable directory `st_mtime`,
    while orphaned mirror refs use Git `creatordate`. A same-account process
    can change a directory timestamp, and a newly-created ref can point to an
    old commit. That can cause premature cleanup under an owner/cooperating
    process model, or under-cleaning for future timestamps.

    Deferred plan: add an owner-only, durable creation/last-use marker for
    each cache/ref namespace and make both preview and apply use that marker,
    with migration rules for existing mirrors and transfer repositories. The
    current task status timestamps and active/reference checks still protect
    valid task-owned state; this is retention accuracy hardening rather than a
    path-escape fix.

12. **Major - deferred** - `src/gc.rs:1491-1518` - descriptor-bound Git
    operations

    > `let mut command_args = vec![OsString::from("--git-dir"), path.as_os_str().to_os_string()];`

    Scenario: rooted filesystem inspection and removal use retained
    descriptors, but GC's `for-each-ref`, `show-ref`, `update-ref`, and
    `git gc` child processes receive the best-effort display path. A hostile
    same-account process could replace that path between inspection and the
    child command, redirecting Git ref inspection or mutation to a replacement
    repository. The rooted removal boundary itself remains descriptor-safe.

    Deferred plan: add a descriptor-aware process-runner seam that performs
    `fchdir` in the child, invokes Git with `--git-dir .`, and verifies the
    rooted binding after every command while retaining the current output and
    deadline bounds. This is a cross-cutting ProcessRunner change and was
    deferred rather than altering the protected process/transport seams in
    this review.

## Seam checks

- Mirror ref inventory uses `git for-each-ref`, so packed refs and refs outside
  `refs/heads` keep a mirror non-empty; only the task-derived branch and base
  refs are ever deleted.
- Valid task records and uncertain projects protect mirror deletion, and
  transfer GC rejects base refs or any non-result ref as collectible. Rooted
  identifiers are validated before every filesystem removal.
- GC holds the installation lock across collection and apply, and apply
  rechecks task state, active work, lease, job siblings, refs, and retention
  age. An apply report is built only from the candidates collected by that
  invocation; there is no hidden path-based candidate input.
- GC reports contain bounded kind/identifier/reason values and generic
  warnings; they do not return worker data-root paths or native session paths.
- Native session deletion is not part of worker GC. It is limited to explicit
  discard, uses the existing cross-task reference check, and treats unsupported
  adapters as a no-op.
- No worker or Mac mini was contacted. The archive samples were used only as
  sanitized JSON fixtures.

## Verification and gate

Per-fix gates passed for the fix commits: `cargo fmt --check`, each touched
suite, `cargo clippy --locked --all-targets -- -D warnings`, and
`git diff --check`.

The final post-rebase gate passed after rebasing onto `main` at `3e57eae`:

- `cargo fmt --check` - passed.
- `cargo test --locked --all-targets` - passed for all unit and integration
  targets. The first sandbox run reached the dashboard target but its three
  loopback-binding cases failed with `DASHBOARD_BIND_FAILED`; the permitted
  host-enabled dashboard rerun passed, followed by the complete host-enabled
  all-target rerun.
- The timing-sensitive lifecycle regression was isolated and passed:
  `cargo test --locked --test agent_cursor_opencode -- --test-threads=1`
  (13 passed), and the retained-transfer-handle close case passed alone.
- `cargo clippy --locked --all-targets -- -D warnings` - passed.
- `git diff --check` - passed.

### Explicit `worker gc --apply` safety statement

`worker gc --apply` is safe to run against the workers' retained state under
the supported owner-only/cooperating-process model: the live protocol-3 and
hand-cleaned-record failures are now non-aborting, damaged records are kept
or warned, active/live-lease/unfetched state is protected, and apply removes
only candidates from its own locked collection pass after rechecking them.
Legacy candidates are reported but not deleted, and worker GC never invokes
native agent-session deletion.

This is not an unconditional guarantee against a hostile same-account process
that swaps a mirror path during an external Git command; that hardening is the
deferred finding 12. A separate earlier preview is not a durable authorization:
`--apply` recomputes and reports its own candidate list, and only that list is
eligible for application.
