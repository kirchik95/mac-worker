# Dashboard validation record

This record contains only build identities, protocol versions, shortened identifiers, state transitions, counts, timings, and outcomes. It intentionally omits paths, connection details, environment values, source data, and raw logs.

## Build identities

The initial dashboard validation used `5232361d99e89fb59ef23db71be1a64dfb261b8c` with protocol version `4`. The queue-projection rerun used `fdd156d9afe38df211f30c29ac36e7e7a6b6a9b6`, also with protocol version `4`.

In both rounds, setup completed for `3/3` configured workers and the readiness check reported protocol `4` for all `3/3`.

## Automated dashboard evidence

The browser-client suite passed (`16` tests). The focused Rust dashboard suites passed (`62` tests) across model, cache, service, web, source, queue, and command behavior. Coverage includes browser requests, a disconnected client, and shutdown preserving zero fake mutation calls; two-client refresh coalescing; stale/current worker projections; FIFO queue rows and blocking codes; terminal log cursors; and the production client-state queue reader.

## Three-worker live acceptance

Items 1–3 and 9 were established in the initial round. Items 4–8 were rerun after the production queue projection was added.

| Item | Sanitized outcome |
| --- | --- |
| 1. Loopback endpoint | Passed. The command reported exactly one loopback URL; no worker-side listener was present. |
| 2. Idle fleet | Passed. `3/3` workers appeared `ready`, `current`, and `idle` in `3.8` seconds. |
| 3. Busy fleet | Passed. Three compatible holding jobs appeared as `3/3` current busy cards with matching shortened job, project, and worktree identifiers. |
| 4. FIFO queue | Passed on the queue-projection rerun. With all three slots busy, the fourth compatible job (`c6f0cc032baf…`) appeared at FIFO position `1` with blocking code `NO_COMPATIBLE_IDLE_WORKER`. |
| 5. Active log reconnect | Passed. For active job `dc995fc6472f…`, both streams resumed exactly from offsets `0` to `3` to `6`, with no duplicated bytes and final lengths of `6` bytes per stream. |
| 6. One-worker outage | Passed. During the isolated one-worker transport outage, exactly one card became `offline` while the remaining `2/3` cards remained `current`; normal isolated inventory configuration was restored afterward. |
| 7. Terminal status comparison | Passed for dashboard-projected terminal fields. The dashboard and `worker status` agreed on four jobs: two `succeeded` with exit `0`, and two `cancelled`, including final byte counts of `0/0` and `6/6`. Both cancelled authoritative statuses retained `LEASE_RELEASE_FAILED`; the final worker inventory nevertheless showed `3/3` ready, idle workers with no active lease. That remote cleanup diagnostic is not a dashboard mutation and is outside this read-only queue-projection scope. |
| 8. Dashboard shutdown | Passed. The dashboard was stopped while an active job was still `running`; its direct status was unchanged before and after shutdown. After terminal cleanup, all four jobs remained queryable with unchanged terminal results and the final worker inventory remained `3/3` ready and idle with no active lease. |
| 9. Non-loopback access | Passed. A non-loopback request was refused. |

## Cleanup and result

The queued job and active jobs were explicitly terminalized during cleanup. The upgraded helpers were left installed. The final isolated inventory had no active lease, and the isolated scratch data was removed after validation.

## Final gate

The browser-client suite (`16` tests), focused dashboard Rust suites (`62` tests), formatting check, and lint check passed. The parallel full-suite attempt surfaced two timing-sensitive failures; each passed its required isolated `--test-threads=1` rerun, and the complete serial full suite completed without a failure marker. The locked release build and diff check passed.

## Phase 5e automated local evidence

This section records the local documentation and dashboard-task gate for the phase-5e implementation. It uses fixtures and local test servers only; this worktree did not contact a worker or execute the live acceptance.

| Check | Sanitized result |
| --- | --- |
| Focused Rust projection/privacy/route suites (`task_view`, `dashboard_tasks`, `dashboard_web`) | Passed (20 tests: 5 + 6 + 9) |
| Browser client suite (`tests/dashboard_client.mjs`) | Passed (20 tests) |
| `cargo fmt --check` | Passed |
| `cargo clippy --locked --all-targets -- -D warnings` | Passed |
| `cargo test --locked --all-targets` | Passed after required isolation (serial all-targets: 0 failures; timing-sensitive case: 1/1) |
| `cargo build --locked --release` | Passed |
| `git diff --check` | Passed |

## Phase 5e live acceptance

These rows record the v7 live dashboard observation against the matched release/client revision. They retain only shortened IDs, state names, counts, durations, outcome categories, revision/protocol matches, and privacy-safe pass/fail observations.

| Acceptance item | Evidence |
| --- | --- |
| 1. Loopback endpoint and no worker-side listener | Passed: one loopback dashboard endpoint was live; no worker-side listener was started. |
| 2. Idle three-worker cards with current readiness | Passed: final refresh showed `3/3` ready/current/idle workers, protocol `4`, Codex `0.153.2`. |
| 3. Three active task turns mapped to titles/agents and worker cards | Passed during item 1: three active rows occupied positions 1–3 on distinct workers and matched the CLI; snapshot progress was total 5, queued 2, active 3. The separate active-job projection was 0. |
| 4. Queued task-turn and batch rows with FIFO position, pin/run-cap fields, and blocking reason | Passed for a live pinned row: position `1`, `pinned_worker=mini-1`, `run_max_parallel=null`, and `blocking_code=CAPABILITY_MISSING` were visible in the snapshot; the rendered card showed “Capability missing · agent:codex”. |
| 5. Browser run/state/worker/agent filters without a network mutation | Passed for state (`active`/`queued`), worker (`mini-1`/`mini-2`), and agent (`codex`) filters; the run filter had only `All runs` because no named run existed. Filter changes caused no network mutation. |
| 6. Task detail result card, questions, changed files, diff stat, and turn timeline | Passed for the active task: detail matched status, worker, turn, and session; timeline was present, while terminal result fields were correctly absent. |
| 7. Active-turn stdout/stderr reconnect with exact 1-second cursors and no duplication | Passed for active stdout: the bounded route returned `0 → 1550`; two reconnect reads at `1550` returned `1550` with zero new bytes. No duplication was observed; stderr and exact one-second cadence were not separately exercised. |
| 8. Stale remote status and dead runner presentation without recovery | Passed observation: after the validated runner kill, the queued row showed `Runner Dead` while remaining current and the active holder showed `Runner Unknown` with no outcome; after reconcile the queued row returned to `Runner Live`. |
| 9. Dashboard shutdown preserving task/turn status and zero mutation | Passed operationally: dashboard stopped cleanly; final CLI state was terminal-only with five closed run rows and no active/queued rows. No mutation was observed; a direct before/after status comparison was not fully captured. |
| 10. Non-loopback rejection, privacy scan, and CLI/snapshot JSON parity | Refusal/parity passed: the non-loopback request failed with connection refusal; CLI and snapshot each exposed 5 task rows with matching normalized fields. Marker/secret counts were `0/0`; `paths/paths_logs` were local `0/16`, mini-1 `162/66`, mini-2 `65/80`, mini-3 `3/52`, so the worker `paths` criterion was not fully passed. |

### Operator checklist

1. Start `worker dashboard --no-open`, then inspect the worker cards, run-progress cards, queue, tasks table, selected task detail/timeline, and active stdout/stderr panes. Exercise the run/state/worker/agent filters locally.
2. Compare the task rows and run progress with `worker --json task list --run <run-id>`, compare selected state with `worker --json task status <task-id>`, compare the result with `worker --json task result <task-id>`, and inspect reconnect behavior with `worker task logs <task-id>`. Keep task turns distinct from legacy `worker status`/`worker logs` jobs.
3. Note only shortened task/run/turn IDs, state and last-outcome categories, task/queue/worker/byte counts, observed durations and polling cadence, revision/protocol matches, and pass/fail results. For privacy checks, record hit counts or a pass/fail result—not the values found. Record zero dashboard mutation calls and unchanged task/turn status after shutdown.
