# Durable origin outbox (PERF)

Date: 2026-09-10. Base: `9297256f5ee3584ab8772dd0cac54304af60843e`.
Status: implements architecture-wave Origin outbox after `outbox-root-review.md`. FLOW owns `PROTOCOL_VERSION` 7; this track does not bump it. SCHED owns host layout 3; this track does not bump `HOST_LAYOUT_VERSION` and does not duplicate slot lookup APIs.

## Goal

Publish `publish=push` results through host-owned **per-turn** durable intents so a slow or failing origin cannot hold the heavy execution slot. A follow-up turn while an earlier delivery is still pending is a core flow. Delivery retries the exact pinned OID. Agent outcome and delivery state stay independent.

## Reuse

- `HostStore` / `RootedDir` atomic replace + fsync; existing `locks/` namespace
- `GitTransport` (OID push, ls-remote, merge-base, durable pin)
- `ProcessIdentity` for the outbox worker
- `LeaseService::release_after_cleanup` **after** intent+pin durability. Occupancy via existing `load()` / `occupancy()`; no `load_for_job`/`load_all` adapters
- `worker host outbox --once|--watch|--enable|--write-agent`
- `SessionKind::{OwnProcessGroup, NewSession}` drain/cap path; do not drop ENV `InheritProcessGroup` at integration

Do not copy peer dirty files. Do not cherry-pick auth `327710c` or reconcile `2c5d0ef`. ENV DAG pins are a different namespace from host delivery pins.

## On-disk (no layout bump)

| Path | Role |
|---|---|
| `tasks/<project>/<task_id>/delivery/<turn_id>.json` | Per-turn intent (identity + attempts) |
| `refs/mac-worker/delivery/<task_id>/<turn_id>` | Immutable OID pin for that turn |
| `locks/outbox-<task_id>-<turn_id>.lock` | Shared RMW flock for that intent |
| `locks/outbox.lock` | Pump worker (may span push; commit/GC never take it) |
| `locks/outbox-worker.json` | Live `ProcessIdentity` |
| `locks/outbox-enabled.json` | Opt-in reboot worker |
| `locks/outbox-target-<sha256>.lock` | Brief target flock (ledger only) |
| `locks/outbox-target-<sha256>.json` | Last delivered OID, generation, turn |

`sha256` = SHA-256(`origin NUL branch`) hex.

## DTO and CLI projection

`OriginDelivery` (typed, independent of `TaskOutcome`):

```json
{
  "turn_id": "<32 hex>",
  "state": "pending",
  "oid": "<40 hex>",
  "origin": "https://example.test/repo.git",
  "target": "refs/heads/release-candidate",
  "attempt": 1,
  "next_attempt_at_millis": 0,
  "last_error": null,
  "superseded_by": null,
  "created_at_millis": 1,
  "updated_at_millis": 1
}
```

`DeliveryState`: `pending` | `retrying` | `delivered` | `failed`.

Projection hook for FLOW/integration (same DTO):

| Surface | Field |
|---|---|
| `TaskStatusResponse` | `delivery` = latest by `created_at_millis`; `deliveries` = all retained intents |
| `LocalTaskRecord.delivery` | latest; `#[serde(default)]`; omitted when none |
| `TaskReport.delivery` | latest |
| CLI `worker task status --json` | `delivery` and `deliveries` |

New readers accept old records via `serde(default)`. Old `deny_unknown_fields` readers reject a present `delivery` field — FLOW folds into protocol 7. A pending origin never flips `Done` to `Failed`.

## Lock order

Global order: **installation → capacity → admission → session → intent → target**.

| Actor | Locks | Across `git push` |
|---|---|---|
| `commit_intent` | intent only | no |
| Pump snapshot / publish | intent; then target only for ledger | **no** (copy, drop, push, re-lock, verify) |
| `--watch` worker | `outbox.lock` (optional, may span push) | yes, but commit/GC never take it |
| Close / GC | session (existing) then intent | no |

Never hold session/admission/capacity during origin push. Do not acquire installation while holding session (SCHED close/retention contract). `RootedDir::replace_private_regular_exact` is not CAS; the intent flock is the RMW fence. After push, re-lock and verify `turn_id`+`oid`+`created_at_millis` before publishing attempts. GC sees pending intents as retained, so it cannot delete the pin during I/O.

## Durability before heavy lease release

1. Under intent lock, write `delivery/<turn>.json` (identity: project, task, turn, origin, branch, oid, `created_at`) and fsync the task/delivery directory.
2. `git cat-file -t <oid>` must be `commit`. `git update-ref <pin> <oid>` create-or-same-OID (`DELIVERY_REF_CONFLICT` on a different OID). Fsync the loose object `objects/aa/…` if present, the ref file, and parent directories.
3. Drop intent lock. `finish_turn` records agent outcome. Supervisor `release_after_cleanup` only after step 1–2.

Recovery:

| Crash | Action |
|---|---|
| Intent durable, pin missing | Re-pin **intent.oid** (never mutable task branch / latest status) |
| Pin present, intent missing | **Fail closed**; retain the pin; do not invent an intent |
| After release, before push | Slot idle; `pending` |
| Mid-push | Retry the pinned OID |
| Push ok, before `delivered` fsync | Restart re-pushes; remote already-equal is delivered |

## Activation

Ordinary `publish=push` **does** wake a bounded pump on this boot: after lease release, `OriginOutbox::wake_once` execs `worker host outbox --once` and records `ProcessIdentity`. That is same-boot delivery, not reboot recovery.

Reboot recovery is opt-in:

- `worker host outbox --enable` writes `locks/outbox-enabled.json`
- `worker host outbox --watch` runs until stopped; stolen if the recorded identity is dead
- `worker host outbox --write-agent DIR` writes a LaunchAgent plist (`KeepAlive=true`, argv `host outbox --watch`) into **DIR only** (tests never `~/Library`)

A plist that was never enabled/started is not recovery. If `--once`/`--watch` cannot start, the intent stays pending and `last_error` is `OUTBOX_WORKER_REQUIRED` (visible on the DTO/CLI; agent outcome unchanged). A detached child without `--enable`/`--watch` is not reboot recovery.

## Pump

`worker host outbox --once` (tests: `pump_due`). Due = `pending`/`retrying` and `next_attempt_at_millis <= now`. Order: `(origin, branch)` then `created_at_millis` then `turn_id`.

Push (no `--force`): `git -C <mirror> -c gc.auto=0 push --no-verify <origin> <oid>:refs/heads/<branch>`

Ledger fencing before push:

- `last_oid == ours` → delivered
- `ours` ancestor of `last_oid` → delivered + `superseded_by` (ledger proof)
- `last_oid` ancestor of `ours` → push
- else → retrying, not superseded

After failed push, `ls-remote` the exact target. Remote == ours → delivered. Remote == ledger last and we are ancestor → superseded with proof. Generic non-fast-forward is **not** superseded.

Max 12 attempts. Backoff `min(1000 * 2^(attempt-1), 300_000)` ms.

## Close and GC

Non-discard close keeps pins/intents. Discard while any intent is `pending`/`retrying` → `TASK_BUSY` (`DELIVERY_PENDING`). GC retains those refs (`task` branch, base pin, **every** delivery pin for the task) plus failed intents inside branch retention. Delivered intents follow ordinary branch retention.

## Tests (local bare origin only)

1. Delayed origin frees the heavy slot while the hook is blocked
2. Failing origin: idle occupancy, retrying/failed delivery, `TaskOutcome::Done`
3. Crash after intent before lease retirement
4. Pin-only crash fail-closed; intent-without-pin re-pins intent.oid
5. Two turns on the **same** task: first pending, second different OID, no `DELIVERY_REF_CONFLICT`, restart, older then newer delivery order, no rollback
6. Mutating `refs/heads/task/<id>` cannot change a pinned push
7. Older retry cannot rewind a newer target
8. Generic non-fast-forward without proof stays retrying
9. Close/GC retain pending pins
10. Spawn watch loop, kill, spawn again (process restart). Detached child without enable is not reboot recovery. Plist contains KeepAlive. Unconfigured spawn surfaces `OUTBOX_WORKER_REQUIRED`

No live origin, no credentials, no global service install.
