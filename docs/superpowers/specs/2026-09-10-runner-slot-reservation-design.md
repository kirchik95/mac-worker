# Runner slot reservation (2026-09-10)

Normative description of the landed occupancy-token protocol. Detached spawn under `QueueLock`; not a new scheduler.

Landed in `e1ef1ffff7a6418dd160102f573b34b440b5d88a` (reservation + enqueue coalesce), `f81adfac87a30549a767423d95ac228ca32f6719` (`release.yml`), this spec aligned after focused gate 191/0.

## Why

`live_runner_count` → `executor.start` → `record_runner`/`adopt_row` races with concurrent submit/reconcile. Counting unique reserver PIDs under-counts several outstanding spawns from one process.

## Record

Optional `QueueEntry.slot_reservation` of type `RunnerSlotReservation`:

```json
{
  "reserver": { "pid": 1, "start_time_micros": 1 },
  "token": "018f0f4a-6b5c-7d8e-9f00-112233445566",
  "child": { "pid": 2, "start_time_micros": 2 }
}
```

- Missing on existing queue JSON (`#[serde(default)]`). Canonical serialize omits the field when `None`.
- `token` is a fresh non-nil UUID **per spawn attempt**, never `max+1`, never reused after release, steal, or completion. Manual serde as a hyphenated UUID string; do not enable uuid's serde feature.
- `child` is omitted until `bind_runner_slot_child`. After bind it is the spawned process identity.
- `RunnerSlotReservation::new` rejects a nil token.

## Occupancy (`occupied_runner_slots_locked`)

Cap for detached start is `config.workers.len()`. Attached start uses `usize::MAX`.

```
occupied = unique live task.runner identities
         + unique live adopted/dispatching queue holders
         + one count per outstanding reservation (not unique-PID)
```

Details that the code actually uses:

- Live runner identities come from `task.runner()` with `Matching` or `Ambiguous` process observation. Reservations are **not** process identities.
- Queue holders are `acting_queue_holder`: a live `Dispatching.dispatch_owner`, or a live `Waiting` owner that is **not** the enqueue owner (adopted waiter). Enqueue owners and parked rows do not occupy by themselves.
- Each `slot_reservation.is_some()` adds **one** extra occupancy, including several reservations that share a reserver PID.
- `exclude_reserver` drops that PID from runner identities and queue holders only. It never drops reservations. `true` is only `TurnRunner::start_next_parked` (finishing owner replacing its own slot). `TaskClient::start_oldest_parked_runner` is called from `reconcile_runners` and must pass `false`; subtracting the reconciler that already holds a started slot lets a parked row start too.
- Recovered-controller exception, **per reserving row only**: when reserving job X as reserver R, do not count X's queue holder if holder == R, the row has no reservation, and there is no live current-turn runner. Reconcile may `adopt_row` onto the controller before spawn; that Dispatching owner is not a runner. This is not "ignore every current PID".
- Same-turn live holder (independent of reservation): `live_runner_for_current_turn` uses `status.turns().last()` when present, else the submission-intent turn or an exact `turns/<task>/<turn>` locator. It does **not** scan `turn_ids_for_task` under `QueueLock`.
- Prefer overcount to overspawn. Parked PID yield starts no process and takes no extra reservation. Park clears `slot_reservation`.

## Reserve / bind / complete

Under `QueueLock`, `reserve_runner_slot`:

1. Existing reservation whose reserver, bound child, same-turn live runner, or foreign live queue holder still counts → `Pending { token }` (the live reservation's token). **Do not spawn.**
2. Existing reservation with dead reserver, no live bound child, no same-turn holder, no foreign live queue holder → **steal** with a new UUID (`Acquired`). The old token must not act.
3. No reservation, but a live same-turn runner or a **different** live queue owner → `Pending { token: Uuid::nil() }`. Nil is not installable.
4. Else if occupancy ≥ cap → `Saturated` (caller parks; park clears any reservation).
5. Else install `{reserver, token: new_uuid, child: omitted}` (`Acquired`). Concurrency point `RunnerSlotReservation`. Spawn **outside** the lock **only for `Acquired`**.

After spawn:

1. `bind_runner_slot_child(turn, token, child)` requires the current token. Wrong/nil token → `QUEUE_SLOT_TOKEN_MISMATCH`.
2. Handoff compares the live row to the **pre-spawn snapshot with this token's child attached** (`token_fenced_row_after_bind`), not the unbound reservation.
3. `complete_runner_spawn`: adopt child if needed, **bind child on the reservation**, publish queue **with the reservation still present**, persist `task.runner`, then clear that token and publish again. A crash between the two publishes still occupies via the bound reservation.
4. Hidden CLI: `worker runner <task> <turn> --slot-token <uuid>`. Child `TurnRunner::with_slot_token` calls `take_over_reserved_slot`. Token-current child may adopt a dead parent; a live bound child that is not this process, a live waiting owner, or a newer steal token refuses. No unconditional adopt.
5. `release_runner_slot` clears only when token **and** reserver match **and** `child` is `None`. After bind, release is a no-op so a failed complete cannot drop a live child. Complete-error cleanup is that same token-fenced release; it never `record_runner(None)`.

FIFO, pins, `max_parallel`, and PID reassignment are unchanged. Enqueue under `QueueLock` coalesces `enqueued_at_millis = max(caller, last_entry)`; FIFO is `queue_id`.

## Tests (barriers / PID fixtures, not sleep-as-race)

- `same_reserver_two_jobs_cap_one_grants_exactly_one_acquired`
- `same_reserver_same_job_starts_executor_once`
- `same_job_after_adoption_does_not_spawn_again`
- `exclude_reserver_still_counts_that_pid_reservations_against_cap`
- `queue_publish_before_runner_record_does_not_free_the_slot`
- `stale_complete_after_steal_does_not_clear_the_new_child`
- `token_current_child_takes_over_dead_parent_without_a_second_spawn`
- `released_then_reused_job_rejects_the_old_token`
- `old_queue_snapshot_without_slot_reservation_deserializes`
- `recovered_dispatch_owner_without_token_or_runner_may_acquire`
- `duplicate_enqueue_and_backwards_time_do_not_mutate_fifo_state` / `concurrent_enqueue_coalesces_inverted_clocks`
- `submit_completes_handoff_after_bind_records_the_child`
- `interrupted_legacy_reassignment_recovers_from_an_unrelated_working_directory` (reconcile unpark must not exclude the reconciler)
- `reconcile_replaces_a_dead_runner_for_an_active_task_turn`
