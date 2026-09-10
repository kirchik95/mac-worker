# Multiple execution slots

A host may run more than one concurrent job. Default remains **one** live lease until the operator opts in. This is a host-layout change, not a protocol bump: `PROTOCOL_VERSION` and `SUPERVISION_VERSION` stay as the current release advertises.

Laptop `[[workers]].slots` is a **client ceiling** used for admission and runner-cap sums. The host’s durable `leases/capacity.json` is the authority for how many live leases that Mac will accept.

## Defaults

| Surface | Default | Bound |
|---|---|---|
| New host `leases/capacity.json` | `{ "slot_count": 1 }` | `u8` in `1..=8` (`MAX_HOST_SLOTS`) |
| Laptop `WorkerEntry.slots` | `1` after `worker init` | same `1..=8`; `0` or `>8` is config error |
| Detached runner cap | `sum(worker.slots)` | not `workers.len()` |
| Same `task_id` | serialized | second live acquire is `WORKSPACE_BUSY` |
| Distinct task IDs from one checkout | may overlap | when host `slot_count >= 2` |
| Legacy snapshot jobs (`kind: job`) | may overlap | distinct `job_id`s |

`N` is a single durable value. There is no desired/effective pair.

## Public vs hidden commands

Public:

- `worker init` / config `[[workers]].slots` — laptop ceiling only.
- `worker workers --refresh` — probe reports occupancy (`configured_slots`, `busy_slots`, `slot_state`).
- `worker setup` — helper install/update. Layout 2 helpers stay fail-closed (`HOST_LAYOUT_OUTDATED`) until an explicit layout migrate runs under the upgrade fence.
- `worker task submit` / `batch` — admission uses the **sum** of declared ceilings. `--no-wait` returns `CAPACITY_BUSY` when every eligible host is full. Do not pass a selected slot id.

Hidden (under `worker host`, which is itself hidden from `--help`):

- `worker host set-slots N` — writes host `slot_count`. Rejects shrink while any live lease occupies `slot_id >= N`. All-idle shrink is allowed. Never moves a live lease.
- `worker host migrate-layout` — explicit 2→3 directory promotion under the host installation lock.
- `worker host probe` and lease/task host verbs — occupancy and per-job lease identity.

Library (upgrade path that already holds the installation lock):

- `HostStore::promote_slot_directories` — create `leases/slots/` and default `capacity.json`. Setup’s `complete_protocol_upgrade` must call this after drain inspect and binary rename. It must **not** call `HostStore::migrate_layout` (that takes the same flock again and deadlocks).
- Idle/facts-refresh may call `HostStore::migrate_layout` after the upgrade fence.

## Host layout 3

Host state root (under the worker account data directory) owns:

```text
installation-lock
layout.json                         # version 3
layout.refresh.json                 # crash residue only; see migrate
leases/capacity.lock
leases/capacity.json                # { "slot_count": N }
leases/slots/<id>/lease.json        # id is 0..slot_count-1 while occupied
leases/slots/<id>/scope.json        # ExecutionScope; required on a live slot
```

Installed layout 3 **requires** a readable `leases/capacity.json`. Missing or invalid capacity is `HOST_SLOT_CAPACITY_INVALID` (fail closed). Default `{ "slot_count": 1 }` is written only by new-host initialization and validated 2→3 `promote_slot_directories`. Slot directory names must be canonical decimal (`0`…`7`, not `00` or `08`). A live lease must have `slot_id < slot_count` (`HOST_SLOT_ID_INVALID` otherwise). Empty leftover directories at `slot_id >= slot_count` after a shrink are ignored; they are not occupancy.

`PREVIOUS_HOST_LAYOUT_VERSION = 2`. Layout 1 cannot jump to 3. New installs write `capacity.json` and `leases/slots/`. `LeaseRecord` JSON is unchanged; the slot id is the directory name. `LeaseToken` remains generation fencing. `JobMeta` and request-fingerprint bytes are unchanged.

`ROLLBACK_HELPER_LAYOUT_VERSION` stays **2**. After a successful 2→3 rewrite, restoring a layout-2 helper is `HOST_UPGRADE_ROLLBACK_UNSAFE`.

## Drain and migrate

Normal `HostStore::open` is fail-closed on layout 2 (`HOST_LAYOUT_OUTDATED`). `migrate_layout` is the only in-process 2→3 rewrite.

Promotion refuses (`HOST_UPGRADE_DRAIN_REQUIRED`) while any of these remain: leftover `leases/heavy`, incoming residue, live or uncertain lease names, unreadable inventory, or a **non-terminal** job directory. An immutable Accepted index next to a genuine terminal archived job is allowed. Live `heavy` is not moved into `slots/0`.

Crash residue: `layout.refresh.json` is kept until installation identity is published. A stored layout that does not match the current helper **without** that residue is refused. It is not an implicit refresh.

Stale `HostStore` handles from before migrate must fail closed on the next layout-sensitive operation.

## Execution scope

Admission-only field on `LeaseAcquireRequest` / `SubmitRequest`. **Not** hashed into the request fingerprint.

```text
execution_scope = { kind: job } | { kind: task, task_id }
```

Omitted deserializes as `job`. Default `job` is omitted on serialize. Task turns always send `task` with the real `TaskId`. Stored next to the lease as `scope.json`, not inside `LeaseRecord` / `JobMeta`. Submit and `task-prepare` must match the bound scope (`EXECUTION_SCOPE_CONFLICT` otherwise).

| Kind | Exclusive key | `WORKSPACE_BUSY` when |
|---|---|---|
| `job` | `job_id` | never, across distinct jobs |
| `task` | `(project_id, task_id)` | another live task lease shares that pair |

`task` vs `job` do not conflict (different trees). Two turns of the same `task_id` still conflict. `project_id`+`worktree_id` names the laptop checkout, not the exclusive workspace.

Host selects the slot. Resolve live work by `job_id` + `LeaseToken`. Do not persist `selected_slot` on the queue.

## Occupancy and probe

`slot_state` is Idle when `busy_slots < slot_count`. One busy slot must not mark the whole worker Busy for admission.

`ProbeResponse` carries `configured_slots` and `busy_slots` with `serde(default)`: `0` means “legacy 1-slot, derive from `slot_state`”. A host with `configured_slots >= 1` must keep Idle iff `busy < configured`, with `busy <= configured` and `configured` in `1..=8`. Idle with a remaining peer lease (`configured=2`, `busy=1`) is valid.

Laptop scheduler treats a worker as Idle when the probe has a free execution slot. Client reservation is `sum` of declared ceilings.

Peers must load **that job’s** lease (`load_for_job` / `release_after_cleanup`), never “the” heavy slot. `load()` remains the default-1 helper: more than one live lease is a protocol error so missed call sites fail closed.

## Lock order

Acquire: admission → capacity. Close: capacity → session. Retention GC: installation → session (no capacity re-entry). `migrate_layout` holds the same `installation-lock` as setup promotion.

## APIs

```text
LeaseService::load_all() -> Result<Vec<(u8, LeaseRecord)>, WorkerError>
LeaseService::load_for_job(job_id) -> Result<Option<LeaseRecord>, WorkerError>
LeaseService::occupied_slot_for_job(job_id) -> Result<Option<OccupiedSlot>, WorkerError>
LeaseService::slot_count() -> Result<u8, WorkerError>
LeaseService::set_slot_count(n: u8) -> Result<(), WorkerError>
LeaseService::release_after_cleanup(expected, receipt)  # find slot by job+token
LeaseService::promotion_blocked() -> Result<(), WorkerError>
HostStore::promote_slot_directories(leases) -> Result<(), WorkerError>
HostStore::migrate_layout(root) -> Result<(), WorkerError>
```

`OccupiedSlot` is `{ slot_id, lease, execution_scope }`.
