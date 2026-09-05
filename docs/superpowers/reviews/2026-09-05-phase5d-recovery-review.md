# Phase 5d recovery hardening review - 2026-09-05

## Scope and outcome

Reviewed the original recovery-hardening series, `478dd6b~18..478dd6b`,
commit by commit, against sections 5.1, 7.1, 9, 12, 13, and 18 of
`docs/superpowers/specs/2026-09-03-agent-task-pool-design.md` and findings 6
and 7 of `docs/superpowers/reviews/2026-09-05-phase5d-review.md`.

The operator confirmed that `fix/p5d-deferred` landed on `main` unchanged at
`478dd6b`; this branch therefore needed no rebase. The review found three
Major findings: the two deferred findings are fixed, and one additional
read-only-side-effect integration defect is fixed in `5f5f083`. No Critical
or Minor finding was opened. No original commit needs to be reverted.

Closing verdict: **land listed fixes** — land the original 18 commits together
with `5f5f083` and this report.

## Findings

### 1. Major - fixed: read-only task/run views recovered and deleted residue

**Location:** pre-fix `478dd6b:src/client_state.rs:1905,2091`; current
`src/client_state.rs:1904-1912,2089-2096,2099-2137`.

**Evidence (pre-fix):**

> `self.recover_task_replacement_residue(&tasks, &names)?;`

The analogous `load_run` and `list_runs` paths called
`recover_run_replacement_residue` as well.

**Failure scenario:** an exchange can publish the new task/run record while
leaving a private `replace-<uuid>` displaced record after a crash or injected
sync failure. `worker task list`, dashboard projection, or any other
read-only path using task/run enumeration then retried the cleanup namespace
and removed that residue. The displaced record is normally expected old state,
but the deletion is still a mutation from a read-only command and could race
with the owner recovery path. This violated the spec's requirement that
`list`, `status`, `result`, `diff`, `logs`, and `dashboard` never mutate or
start work.

**Fix:** `list_tasks_locked`, `load_run`, and `list_runs` now only read and
filter private replacement names. Recovery is exposed through the mutating
`ClientStateStore::recover_replacement_residue` path and is invoked from
mutating update/reconciliation flows. The regression tests
`task_enumeration_does_not_recover_replacement_residue` and
`run_enumeration_does_not_recover_replacement_residue` leave the residue
count unchanged after enumeration and prove that a later mutation cleans it
up. Commit `5f5f083`.

**Verdict:** FIX, then LAND the residue-recovery commits.

### 2. Major - fixed: post-create submission rollback

**Location:** `src/task_client.rs:736-799`; prior finding 6 in the earlier
Phase 5d review.

**Evidence (pre-fix):**

> `let _ = transfer.release_base(self.runner, task_id);`

**Failure scenario:** after `create_task` succeeded, a prompt write, run
reference load, queue publication, report, or equivalent pre-handoff step
could fail after only the transfer base was released. The task record,
prompt, queue row, and task-owned run reservation could then disagree, while
reconciliation had no complete durable description of the submission to
repair.

**Fix:** submission intent is recorded before post-create work; failures use a
durable rollback marker and compensating cleanup for the task-owned run
reservation, transfer base, queue row, prompt tree, and task record. Cleanup
is ownership-checked and retryable. Intent clearance is the explicit handoff
boundary: after it, the complete submission is retained for reconciliation so
it cannot remove a row a runner may already have adopted. The recovery path
also reloads the current record under the transfer lock before acting.

Regression coverage includes prompt-write, run-reference, queue-publication,
rollback-retry, restart, reopen, and handoff-boundary fault cases in the
task-client and turn-runner suites.

**Verdict:** FIX confirmed and LAND the rollback series; no revert.

### 3. Major - fixed: local push origin target is pinned

**Location:** `src/task.rs:188-255,581-589,662-685`,
`src/job_service.rs:3127-3150`, `src/turn_runner.rs:641-646,1287-1289`, and
`src/turn.rs:794-817`; prior finding 7 in the earlier Phase 5d review.

**Evidence (pre-fix):**

> `(TaskSource::Local { .. }, true, Some(_)) => Ok(()),`

**Failure scenario:** a local push task stored only `TaskSource::Local`, while
the client reread the repository origin for every turn and the host accepted
any supplied origin. Changing the repository origin after submit could send a
retry or publication to a different normalized destination than the one used
for admission and task creation.

**Fix:** local push metadata now stores a normalized credential-free URL and
its derived `origin:<host>` requirement. Every turn uses that persisted target;
it does not reread Git origin. Host validation and terminal publication require
exact equality and return `REQUEST_CONFLICT` for missing, unexpected, or
changed targets. Tests cover origin mutation after submit, accepted and
rejected replay/request mismatches, and terminal publication.

**Verdict:** FIX confirmed and LAND the pinning series; no revert.

## Commit-by-commit audit

| # | Commit | Adversarial result | Verdict |
|---:|---|---|---|
| 1 | `dcdca5a` - roll back failed task submission state | Real initial post-create rollback; incomplete on its own, completed by the following durable-marker and cleanup commits. | LAND |
| 2 | `e4630ef` - preserve state after task handoff failure | Real ownership boundary: state is retained when a runner may already have adopted the queue row. | LAND |
| 3 | `6151ee8` - recover incomplete submission rollback | Real durable rollback marker and restart/retry path; bounded to the task's own resources. | LAND |
| 4 | `db25522` - pin local push origin target | Real immutable target capture and host requirement for local push. | LAND |
| 5 | `0cb6a25` - avoid origin reads after task submission | Real removal of mutable Git-origin rereads from the turn path. | LAND |
| 6 | `306ae15` - preserve submission rollback recovery | Real ordering fix that keeps pre-handoff recovery possible across fallible boundaries. | LAND |
| 7 | `3cc91dc` - preserve pinned origin task metadata | Real preservation of immutable origin metadata through host/request parsing. | LAND |
| 8 | `afa5740` - retain parked submission after publication fault | Real protection against deleting a submission whose parked row may have transferred ownership. | LAND |
| 9 | `93e6054` - cover origin recovery and replay | Deterministic regression coverage for origin replay/recovery; no speculative production mechanism. | LAND |
| 10 | `c0e71fe` - recover rollback after turn retirement | Real retention of rollback identity after the prompt/turn tree is retired. | LAND |
| 11 | `98ebac2` - recover submission intent after marker failure | Real durable intent-before-failure ordering and explicit durable intent clearing. | LAND |
| 12 | `6450fec` - compile retention fixture with pinned origin | Test-fixture compatibility for the new immutable field; no runtime risk found. | LAND |
| 13 | `346e5c2` - guard submission intent recovery races | Real transfer-lock/current-record reload guard against stale rollback or adoption. | LAND |
| 14 | `e08794e` - preserve submission rollback ownership | Real task-scoped run-branch ownership; rollback cannot release another task's branch. | LAND |
| 15 | `a6b7684` - treat intent clear as handoff boundary | Real atomic-exchange/final-sync ambiguity handling; avoids compensating after ownership may transfer. | LAND |
| 16 | `d15bb95` - recover task replacement residue after exchange | Real rooted task-record recovery, but its enumeration call made read-only views mutate. | FIX, THEN LAND |
| 17 | `2723ba9` - serialize task replacement residue recovery | Real serialized recovery and rooted-FS fault coverage; retain with the read-side-effect fix. | FIX, THEN LAND |
| 18 | `478dd6b` - recover run replacement residue and report runner | Real run-residue recovery and runner reporting; `load_run`/`list_runs` inherited the same read-side-effect defect. | FIX, THEN LAND |

No commit was speculative or harmful enough to revert. The defect spanning
commits 16-18 was a call-site integration error, not evidence that the
underlying replacement recovery should be removed.

## Seam checks

- Recovery mutation is confined to mutating command paths and reconciliation.
  Read projections use owner-only state locks and reads; they do not enqueue,
  start runners, remove replacement residue, or reread mutable origin state.
- Submission rollback cannot remove a live runner's adopted work: it requires
  the pre-handoff state, checks for no runner and no active task, and only
  removes a queue row in the waiting state with the original enqueue owner.
  Transfer locking plus a fresh record load closes the stale-snapshot race.
- Durable markers are canonical bounded IDs only. Task records are owner-only,
  unknown fields are rejected, and markers contain no prompt, environment,
  credential, or filesystem path data.
- Local push targets are normalized before persistence; credentials, query, and
  fragment material are not retained. A legitimate Git-origin change after
  submit does not change the pinned task target; a mismatched request fails
  with `REQUEST_CONFLICT` before publication.
- Run publish reservations are task-owned and release checks preserve another
  task's reservation. Rooted-FS replacement recovery retains root-name,
  owner-only regular-file, no-follow, device/inode, exchange, and fsync
  invariants.
- Recovery helpers remain reachable from mutating update and runner
  reconciliation paths. Fault injection is deterministic and covers exchange,
  sync, prompt, queue, origin, restart, and handoff boundaries.

## Verification and gate

Per-fix verification passed:

- `cargo fmt --check`
- `cargo test --locked --test task_model --test turn_runner` (29 + 31 passed)
- `cargo clippy --locked --all-targets -- -D warnings`
- `git diff --check`

The required final `cargo test --locked --all-targets` passed every target
except the three dashboard tests that were blocked by the restricted
environment's loopback bind and returned `DASHBOARD_BIND_FAILED`. Per the
brief, `cargo test --locked --test dashboard_command` was rerun with host
network permission and all 4 dashboard tests passed. No serial timing rerun
was needed. No worker or Mac mini was contacted.

## Closing verdict

**LAND LISTED FIXES.** Land the original 18 recovery-hardening commits with
`5f5f083` and this report; do not revert any of the 18 commits.
