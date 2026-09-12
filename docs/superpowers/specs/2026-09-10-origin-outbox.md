# Durable origin outbox (PERF)

Date: 2026-09-10. Foundation: `9297256f5ee3584ab8772dd0cac54304af60843e`.
Runtime: `c6d0517fc3b72d62ce690dee07c73ee8cec8aba3` plus docs/UI follow-up `ccad0f00c681aa7651416e7a95bff3ebed53eefa`.
This track does not bump `PROTOCOL_VERSION` (FLOW) or `HOST_LAYOUT_VERSION` (SCHED).

## Goal

Publish `publish=push` results through host-owned **per-turn** durable intents so a slow or failing origin cannot hold the heavy execution slot. A follow-up turn while an earlier delivery is still pending is a core flow. Delivery retries the exact pinned OID. Agent outcome and delivery state stay independent: a pending origin never flips `Done` to `Failed`.

Evidence is local `file://` bare-origin fixtures. This is not a fleet or live-origin guarantee.

## On-disk (no layout bump)

| Path | Role |
|---|---|
| `tasks/<project>/<task_id>/delivery/<turn_id>.json` | Per-turn intent (identity + attempts) |
| `refs/mac-worker/delivery/<task_id>/<turn_id>` | Immutable OID pin for that turn |
| `mw-object-sync.json` (mirror root) | Object-store baseline receipt `{fsync, fsync_method, packs}` |
| `locks/outbox-<task_id>-<turn_id>.lock` | Shared RMW flock for that intent |
| `locks/outbox.lock` | Pump worker (may span push; commit/GC never take it) |
| `locks/outbox-worker.json` | Live watcher `ProcessIdentity` (`watch: true`) |
| `locks/outbox-enabled.json` | Opt-in reboot worker marker |
| `locks/outbox-due/<task_id>-<turn_id>.json` | Durable active due registry (`next_attempt_at_millis`) |
| `locks/outbox-target-<sha256>.lock` | Brief target flock (ledger only) |
| `locks/outbox-target-<sha256>.json` | Last delivered OID, generation, turn |

`sha256` = SHA-256(`origin NUL branch`) hex. Due entries exist only while the intent `retains_objects()` (`pending` or `retrying`).

## Durability

### Object-store baseline (not tip-only)

`pin_delivery_ref` is create-or-same-OID. Before the first pin on a mirror, `ensure_object_store_durable` fsyncs **every** current pack file and **every** loose object plus fanout directories, then publishes `mw-object-sync.json`, then `update-ref` and fsync of the delivery ref. Syncing only `objects/<tip>` is not the guarantee. There is no per-result `rev-list` / `pack-objects` / `for-each-ref` walk and no growing anchor list. Loose objects are **not** packed at pin time.

A valid receipt with matching `fsync` / `fsync_method` means later pins fsync only packs not already listed. Same-OID retry reuses the receipt bytes and does not rewrite history.

Fault order:

- `AfterOutboxObjectBaselinePacks` — after pack **and** loose fsync, before receipt. Fail closed: no receipt, no pin (including all-loose with zero packs).
- `AfterOutboxObjectBaseline` — after first receipt, before pin. Receipt retained; recovery pin does not re-baseline.
- `AfterOutboxIntent` / `AfterOutboxPin` — intent then pin; pin-only crash fail-closed (retain pin, do not invent an intent); intent-without-pin re-pins **intent.oid**.

### Scoped fsync producers (not user/global Git config)

Mirror local config, skipped when already set (keys compared lowercase as `git config --list --local` emits them):

- `core.fsync=objects,derived-metadata,reference`
- `core.fsyncMethod=fsync`
- existing `core.hooksPath=hooks` and `receive.denyDeletes=true`

The same fsync pair is passed as per-command `-c` on GitTransport (including origin push), TurnPublisher workspace→mirror commits, and TaskStore Git. `GIT_CONFIG_GLOBAL=/dev/null` and `GIT_CONFIG_NOSYSTEM=1`. No user or global Git config writes.

## Lock order

Global order: **installation → capacity → admission → session → intent → target**.

| Actor | Locks | Across `git push` |
|---|---|---|
| `commit_intent` | intent only | no |
| Pump snapshot / publish | intent; then target only for ledger | **no** (copy, drop, push, re-lock, verify `turn_id`+`oid`+`created_at_millis`) |
| `--watch` worker | `outbox.lock` (may span push) | yes, but commit/GC never take it |
| Close / GC | session (existing) then intent | no |

Never hold session/admission/capacity during origin push. Do not acquire installation while holding session. `RootedDir::replace_private_regular_exact` is not CAS; the intent flock is the RMW fence.

## Commit path before heavy lease release

1. Under intent lock, write `delivery/<turn>.json` and fsync the delivery and task directories; insert/update the due registry entry.
2. Pin `refs/mac-worker/delivery/<task>/<turn>` to **intent.oid** (baseline as above).
3. Drop intent lock. `finish_turn` records agent outcome. Supervisor `release_after_cleanup` only after steps 1–2, then `wake`.

Push (no `--force`): `git -C <mirror> -c gc.auto=0 -c core.fsync=… -c core.fsyncMethod=fsync push --no-verify <origin> <oid>:refs/heads/<branch>`.

## Activation

`worker host outbox` requires one of `--watch`, `--once`, `--enable`, `--write-agent DIR`, or hidden `--wake`. Spawned watchers pass `--host-root` as an absolute HostStore root (no live PathLayout fallback).

| Command | Behavior |
|---|---|
| `--wake` | If a live watcher identity exists, no-op. Else spawn `host outbox --watch --host-root <abs>` in a new session (`setsid`) and **exit**. Handshake fails closed (`OUTBOX_WORKER_REQUIRED`) if `outbox-worker.json` is not a live `watch` identity. |
| `--watch` | Take `outbox.lock` or **exit immediately** if another pump holds it (no queue). Publish worker identity, **recover the due registry once** by scanning task delivery files, then loop `pump_due` using the due index (no per-idle task-directory walk). |
| `--once` | Blocking one-shot pump. If the pump lock is held, `OUTBOX_BUSY`. Does not spawn `--watch`. |
| `--enable` | Write `locks/outbox-enabled.json`. Does not start a watcher. |
| `--write-agent DIR` | Write `com.mac-worker.outbox.plist` into **DIR only** (`KeepAlive=true`, argv `host outbox --watch --host-root …`). Tests never write `~/Library`. |

After lease release, production `wake` uses `SystemOutboxLauncher` (current `worker` executable). An unconfigured binary returns `Inactive`; if the executable looks like `worker` but the watcher does not acknowledge, intents stay pending and `last_error` is `OUTBOX_WORKER_REQUIRED`. A detached child without `--enable` is not reboot recovery. `--enable` plus a LaunchAgent that actually starts `--watch` is the reboot path.

## Pump and due registry

Due = `pending`/`retrying` and `next_attempt_at_millis <= now`, discovered from `locks/outbox-due/*.json` then re-read intent. Order: `(origin, branch)` then `created_at_millis` then `turn_id`.

Ledger fencing **before** push (no rewind of a newer target):

- `last_oid == ours` → delivered
- `ours` ancestor of `last_oid` → delivered + `superseded_by` (ledger proof)
- `last_oid` ancestor of `ours` → push
- else → retrying, **not** superseded

After failed push, `ls-remote` the exact target. Remote == ours → delivered. Remote == ledger last and we are ancestor → superseded with proof. Generic non-fast-forward is **not** superseded.

Max 12 attempts. Backoff `min(1000 * 2^(attempt-1), 300_000)` ms.

Hidden `worker host outbox-retry <task_id>` (laptop `worker task publish-retry`) resets failed or retrying intents to `retrying`, `attempt = 0`, `next_attempt_at_millis = now`, records additive `retry_requested_at_millis`, rewrites the due registry, and wakes. Delivered or superseded intents are `DELIVERY_ALREADY_DELIVERED`. Missing intents are `DELIVERY_NOT_FOUND`. `ORIGIN_AUTH_FAILED` is eligible like any other failure. The next 12 attempts start from zero.

## Close, observation, GC

Non-discard close keeps pins/intents and returns `TaskCloseResponse.delivery` / `deliveries` from the outbox. The laptop close mutation **merges** those deliveries into the local record. Discard while any intent is `pending`/`retrying` (or unreadable) → `TASK_BUSY` (`DELIVERY_PENDING`).

`worker task status` and dashboard detail **project** remote deliveries without writing task/queue bytes. Closed/Abandoned keep local status and head; unresolved local deliveries (`pending`/`retrying`) still refresh from the worker. Remote Open cannot reopen a Closed task. TurnRunner `persist_status` still writes status only. Reconcile persists remote observation for Active/Open only.

GC retains: pending/retrying intents, failed intents inside branch retention, **every** delivery pin for the task while `retains()` is true, plus orphaned pins (pin present, intent missing). Delivered intents follow ordinary branch retention. Mutating `refs/heads/task/<id>` cannot change a pinned push OID.

## Vectors (same DTO)

`OriginDelivery` is typed and independent of `TaskOutcome`. `DeliveryState`: `pending` | `retrying` | `delivered` | `failed`.

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

| Surface | Fields |
|---|---|
| `TaskStatusResponse` / `TaskCloseResponse` | `delivery` latest; `deliveries` all retained (`skip_serializing_if` empty) |
| `LocalTaskRecord` | same; `#[serde(default)]`; omitted when none |
| `TaskReport` / CLI `worker task status --json` | `delivery` and `deliveries` |
| Dashboard `TaskListRow` / `TaskDetailProjection` | skip-empty; detail UI shows a DELIVERY fact; list UI does not |
| `ui/src/lib/api.ts` | `OriginDelivery`; TaskDetail reads top-level detail fields |

New readers accept old records via `serde(default)`. Old `deny_unknown_fields` readers reject a present `delivery` field until FLOW folds protocol 7.

## Local fixture coverage (not fleet)

`tests/origin_outbox.rs` (31) plus observation and turn sidecars:

- Delayed origin frees the heavy slot while a hook is blocked
- Failing origin: idle occupancy, retrying delivery, `TaskOutcome::Done`
- Crash after intent before lease retirement; pin-only fail-closed; intent-without-pin re-pins intent.oid
- Two turns on the same task: independent pins, no `DELIVERY_REF_CONFLICT`, older then newer delivery, no ledger rewind
- Mutating the task branch cannot change the pinned OID
- Generic non-fast-forward without proof stays retrying
- Close/GC retain pending pins; orphaned pin retains; unrelated completed-branch GC does not break a later pin
- Packed and all-loose baseline fault ordering; same-OID retry does not walk history; second distinct packed pin skips `rev-list`/`pack-objects`/`for-each-ref`
- `--once` is `OUTBOX_BUSY` while `--watch` holds the pump; extra watchers exit; `--wake` parent exits after the watcher acknowledges; unconfigured spawn surfaces `OUTBOX_WORKER_REQUIRED`
- Closed status/dashboard project Delivered without writing; close learns Pending after a transient pre-close status failure
- Turn `publish=push` records Pending and keeps the task Open/`Done` when origin is unreachable

No live origin, no credentials, no global service install, no user/global Git config writes.
