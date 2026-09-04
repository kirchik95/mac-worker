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
