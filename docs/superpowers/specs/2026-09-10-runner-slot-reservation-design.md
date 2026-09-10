# Runner slot reservation (2026-09-10)

Approved SCHED design after root contract review 1. Occupancy tokens for detached spawn; not a new scheduler.

## Why

`live_runner_count` → `executor.start` → `record_runner`/`adopt_row` races with concurrent submit/reconcile. Counting unique reserver PIDs under-counts multiple outstanding spawns from one process.

## Record

Optional `QueueEntry.slot_reservation` `{ reserver: ProcessIdentity, token: Uuid, child?: ProcessIdentity }`. Missing on existing queue JSON. `token` is a fresh UUID for every spawn attempt and is never reused after release, steal, or completion. `child` is bound after spawn and is omitted until then.

## Occupancy

`occupied = unique live runner identities + unique live adopted/dispatching queue owners + outstanding reservations`.

- Each reservation counts once per job/token, including several with the same reserver PID. `exclude_reserver` applies only to the finishing runner identity, never to reservations.
- `exclude_reserver=true` is only `TurnRunner::start_next_parked` (the finishing owner replacing its own slot). `TaskClient::start_oldest_parked_runner` is called from `reconcile_runners`, which is not that owner, and must pass `false`. Subtracting the reconciler PID that already holds a started slot lets a parked row start too and overshoots the cap.
- Do not suppress a reservation because some other live task runner exists.
- A same-turn live runner, bound child, or adopted/dispatching owner blocks a second acquire even after the reservation field is cleared.
- Prefer overcount to overspawn.
- Parked PID yield starts no process and takes no extra reservation.

## Protocol

Reserve under `QueueLock` before spawn. Only `Acquired` (newly installed token) may spawn; `Pending` means a live attempt already holds the row. Spawn the hidden runner with `--slot-token <uuid>` and bind that child on the reservation. Handoff compares the live row to the pre-spawn snapshot with this token's child attached, not the unbound reservation. Complete records `task.runner` before clearing the reservation. Child takeover: if the token still matches and the waiting owner is dead, the original child adopts itself; steal is refused while that child is live. Release after bind is a no-op. Park clears a reservation.

## Tests

Same caller PID, two job IDs, cap=1 → one acquired. Same PID/same job → one acquired / one start. reserve→release/steal→reserve same job rejects the old token (ABA). Parent-death and PID-reuse windows with barriers, not sleeps.
