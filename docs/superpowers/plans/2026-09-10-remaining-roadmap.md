# Remaining roadmap — 2026-09-10

**Goal:** finish leftover pool reliability (atomic runner slots, queue clock), align release gates with current CI, keep architecture four-pack designed-not-built until assigned after this foundation.

**Architecture:** one laptop-owned `ClientStateStore` queue, `QueueLock` + PID/start-time fencing, one heavy lease per Mac unless a later reviewed multi-slot change. No parallel scheduler, store, or transport. Controller / multi-slot / DAG / origin outbox are **in scope for ALL** and scheduled after this increment; not pending user clarification.

**Constraints:** base `faf6eff`. No push/tag/live SSH jobs. Focused tests only until integration. External reconcile-repair owns completed dispatching-row + early-exit append; auth-incident owns auth facts. SCHED reservation hunks in `reconcile_runners` stay in the start/count section only.

**Interfaces:** `/private/tmp/mac-worker-roadmap-jn00iibh/sched-contract.md`, `arch-contract.md`. Peers: PERF observations/transport; FLOW review/dashboard mutations; ENV readiness/batch preview.

## Known landed (do not claim in-flight work)

| SHA | What |
|---|---|
| `8f10ed2` | Fork/race-sensitive lib tests deterministic (includes remote snapshot race coverage) |
| `9f27723` / `7519441` / `96ccc2e` / `0745f2b` | Stage 5.2 admission cache (measurements 8f10ed2→7519441; suite on 0745f2b) |
| `915227f` | Stage 5.3a no-op task writes + non-overlapping polls |
| `f309f96` | logs: `accepted by <worker>` |
| `ba32d33` | operator docs sync |
| `77bc222` | dashboard stale facts TTL refresh |
| `308e52e` | `task list` run projection + `--run` names |
| `1c1c378` | rooted_fs cleanup-guard wait |
| `faf6eff` | pool-run record (findings 8–11 still open except as other sessions take them) |

Open from the pool run and not this track: completed `dispatching` row (reconcile-repair), Codex auth incident (auth-incident), worker toolchain lag, full-lib-in-sandbox.

## This increment (SCHED)

1. Atomic `slot_reserved_by` before spawn; recover stale reservation without clearing live runners.
2. Coalesce enqueue timestamps under `QueueLock` (`QUEUE_TIME_REGRESSION`).
3. `.github/workflows/release.yml` → macos-15, checkout@v7, Node 22 UI gates matching `ci.yml`, keep draft-on-tag packaging. No `--test-threads=1`.
4. Architecture contract published (`proposed; in scope`). Implementation of controller / multi-slot / DAG / origin outbox **waits for assignment after this foundation**.

## Other tracks (not done here)

- PERF: 5.3b background observations, 5.4 status/log transport, 5.5 origin/memory.
- FLOW: 6.1–6.3 review/result/dashboard replies + UI debt.
- ENV: 6.4 readiness/setup/cache/batch preview; git identity HOME; preview must not enforce DAG.
- Integration: root combines, full all-targets once.
- Live acceptance: after integrated gate; disposable project; existing Cursor auth only.

## Architecture (not silently dropped)

See `arch-contract.md`: persistent controller (opt-in, same durable queue, flock leadership); slots default 1; DAG `depends_on` with parent `Closed`+`Done` and `DEPENDENCY_FAILED`; origin outbox before lease release. Additive serde defaults. After PERF 5.5 and ENV schema freeze as listed there.
