# Remaining roadmap — 2026-09-10

**Goal:** finish leftover pool reliability (atomic runner slots, queue clock), align release gates with current CI, keep architecture four-pack designed-not-built until assigned after this foundation.

**Architecture:** one laptop-owned `ClientStateStore` queue, `QueueLock` + PID/start-time fencing, one heavy lease per Mac unless a later reviewed multi-slot change. No parallel scheduler, store, or transport. Controller / multi-slot / DAG / origin outbox are **in scope for ALL** and scheduled after this increment; not pending user clarification.

**Constraints:** base `faf6eff437390dc1f996b498f9fb8aae48c6c83d`. No push/tag/live SSH jobs. Focused tests only until integration. External reconcile-repair owns completed dispatching-row + early-exit append; auth-incident owns auth facts. SCHED reservation hunks in `reconcile_runners` stay in the start/count section only.

**Interfaces:** repo-local `docs/superpowers/specs/2026-09-10-runner-slot-reservation-design.md` is the occupancy-token spec. Slots design is a follow-on repo spec after this foundation; implementation waits for root review. Peers: PERF observations/transport; FLOW review/dashboard mutations and protocol-storage compatibility; ENV readiness/batch preview.

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
| `e1ef1ff` | SCHED: `QueueEntry.slot_reservation` before spawn; enqueue clock coalesce |
| `f81adfa` | SCHED: `release.yml` matches `ci.yml` (macos-15, checkout@v7, Node 22 UI, no `--test-threads=1`) |

Open from the pool run and not this track: completed `dispatching` row (reconcile-repair), Codex auth incident (auth-incident), worker toolchain lag, full-lib-in-sandbox.

## This increment (SCHED)

1. Atomic `slot_reservation` `{reserver, token, child?}` before spawn; bind child after start; complete publishes with the reservation kept, persists `task.runner`, then clears that token. Steal/release/takeover are token-fenced. Recovered controller Dispatching owner without a token or live runner may acquire. Reconcile unpark does not exclude the reconciler PID.
2. Coalesce enqueue timestamps under `QueueLock` (`enqueued_at_millis = max(caller, last)`; FIFO is `queue_id`).
3. `.github/workflows/release.yml` → macos-15, checkout@v7, Node 22 UI gates matching `ci.yml`, keep draft-on-tag packaging. No `--test-threads=1`.
4. Architecture four-pack remains designed-not-built. Slots implementation waits for root design review. FLOW owns `PROTOCOL_VERSION` and persisted JobMeta/fingerprint compatibility; SCHED must not bump that constant to carry slot fields.

Focused gate on the reservation commit (log `focused-suites-after-parked-exclude-fix.log`): `runner_dispatch` 15, `scheduler_concurrency` 9, `scheduler_queue` 73, `task_conversation` 17, `turn_runner` 77 → **191 passed, 0 failed**.

## Other tracks (not done here)

- PERF: 5.3b background observations, 5.4 status/log transport, 5.5 origin/memory.
- FLOW: 6.1–6.3 review/result/dashboard replies + UI debt; protocol7 + stored JobMeta/RequestFingerprintMaterial compatibility.
- ENV: 6.4 readiness/setup/cache/batch preview; git identity HOME; preview must not enforce DAG.
- Integration: root combines, full all-targets once.
- Live acceptance: after integrated gate; disposable project; existing Cursor auth only.

## Architecture (not silently dropped)

Persistent controller is opt-in on one `ClientStateStore` with flock leadership; default local mode stays. Slots stay default 1 until a reviewed host-layout/probe change; host capacity is authoritative. DAG uses ENV batch `id`/`depends_on` with parent `Closed`+`Done`; origin outbox persists intent+OID pin before lease release. New on-disk fields are not old-reader compatible via `serde(default)` when the type is `deny_unknown_fields` or when `validate()` compares a stored `protocol_version` to the process constant.
