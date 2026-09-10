# Batch DAG

Laptop `worker task batch` can execute named dependencies. Independent batches (no `depends_on`, no `base = "from:<id>"`) keep the existing create-run-and-submit path and do not write a DAG file.

Protocol 7, host layout, and origin-outbox pins are unchanged. Laptop DAG pins live in the transfer repository (`refs/mac-worker/dag/<run_id>/<batch_id>`). They are not host origin-delivery pins. GC must not drop a pin named by a waiting, claimed, or `from:`-bound node. That is logical retention of named pins, not a hardware durability claim.

User CLI and laptop-agent flow: [README](../README.md), [usage](usage.md). Host occupancy: [slots](superpowers/specs/2026-09-10-slots-design.md). Origin delivery: [outbox](superpowers/specs/2026-09-10-origin-outbox.md).

## Public CLI

```text
worker task batch FILE [--name NAME] [--max-parallel N] [--wait | --preview]
worker task wait (--task-id TASK_ID | --run RUN_ID) [--timeout DURATION]
worker task reconcile
```

`--max-parallel` is CLI-only. It is not a batch-file or `.worker.toml` key. Omitted, it defaults to `sum(worker.slots)` (each worker defaults to 1, operator may set `1..=8`). An explicit positive value is not rejected for exceeding that sum; extra tasks wait. Zero is `TASK_CONFIG_INVALID`. `--preview` conflicts with `--wait`. Preview does not open client state or dispatch.

Graph errors are `TASK_CONFIG_INVALID` and create no run, tasks, or pins.

## Batch schema

`[[tasks]]` fields used by the DAG path, in addition to the ordinary task fields:

| Field | Role |
|---|---|
| `id` | Stable name in this batch. Required when `depends_on` is non-empty or `base` is `from:<id>`. |
| `depends_on` | Parent `id` list. Empty means no named parents in this field. |
| `base` | Ordinary ref, or `from:<id>` for the parent's accepted imported result. Public `from:` is a dependency edge even when omitted from `depends_on`; validation inserts that parent. |
| `files` | Advisory overlap hints for preview. |
| `acceptance` | Copied into the agent prompt as declared instructions, not proven by mac-worker. |
| `close_on` | Batch top-level or per task (`done` or `never`). Not a `.worker.toml` `[task]` key. |

Invalid or cyclic graphs, `depends_on` or `from:` without `id`, and unknown parent ids fail validation before any mutation. Public `from:` does not have to be repeated in `depends_on`.

## Independent vs dependent

- **Independent:** every task has empty `depends_on` and no `from:` base. Submit uses today's `TaskClient::batch` path. No `dags/<run_id>.json`.
- **Dependent:** at least one `depends_on` or `from:`. Submit freezes the graph, writes the DAG file, then materializes only currently eligible nodes. Later nodes wait for the parent gate.

`worker task batch FILE --preview` reports `dag.status = "enforced"` with message `Dependencies execute when parents are Closed and Done.`

## Parent gate

A parent unblocks children only when `state == Closed` **and** last outcome is `Done`. That includes `close_on = never`: the human `worker task close` after a Done turn is the accept. Closing Failed or NeedsInput is not acceptance.

| Parent | Children |
|---|---|
| Closed + Done | Eligible (Ready) |
| Open + NeedsInput, Open + Done, Queued, Active | Wait |
| Abandoned, Lost, Failed, Blocked, Cancelled, TimedOut, Closed without Done | Block (`DAG_PARENT_FAILED`); descendants are not launched |

NeedsInput is answered with `worker task say`; the child still waits until a later Closed+Done.

## `from:` bind

`base = "from:<id>"` copies that parent's **current accepted turn** imported object:

- parent gate Ready (Closed+Done)
- no unfinished queue/journal for that turn
- `fetched_head == head_oid` for that turn
- the object exists locally
- then pin; never rebind

This is the laptop current-turn import, not origin delivery. A `done` parent with origin `pending` can still bind if the local import proof is complete. A prior turn's `fetched_head` is not evidence. Follow-up on the parent after bind does not change the child's bound OID.

## Freeze and restart

At initial dependent submit the runtime freezes each node's prompt, settings, project identity, and explicit/WIP base OID, and pins frozen objects. Restart reloads the DAG and does not reread the batch file or `.worker.toml` to change those frozen inputs. Independent batches and tasks with no run still load project toml as today.

Stable `task_id` and first `turn_id` are persisted on every node before `create_task` / host prepare. `RunRecord.task_ids` lists only materialized tasks; pending names live in the DAG file.

## Wait and reconcile

`worker task wait --run` is not complete while any DAG node is still waiting or claimed. An empty `RunRecord.task_ids` list is not completion. The run is quiescent when every node is `submitted` or `blocked`. `wait` already polls `reconcile_runners`.

`worker task reconcile` re-owns dead runners and re-enqueues orphans, then advances eligible DAG nodes. It does not submit work the operator did not already freeze in that run. Close after durable Closed, and a finished turn after this-turn import, also advance pending nodes.

List rows for not-yet-submitted nodes may show `DAG_WAITING` or `DAG_CLAIMED`.

## Technical store (implementation)

Topology is laptop `ClientStateStore` file `dags/<run_id>.json` (`deny_unknown`). Node states: `waiting` | `claimed` | `submitted` | `blocked`. Claim is under StateLock with no Git/SSH held. Two reconcilers produce one claim.

Idle advance walks `dag-pending/<run_id>` (a dedicated index, not `dags/`). Markers are written before the DAG file so a crash cannot hide a live graph; they retire when every node is `submitted` or `blocked`. Existing stores get a bounded-once `dag-pending/bootstrap.json` receipt on reconcile.

The persisted DAG file stores the **normalized** edge list. After `from:` is folded into `depends_on`, a stored node whose `from:` parent is missing from that list is invalid. That consistency check is not a public batch-file requirement.

## Out of scope

- Dashboard DAG graph chrome.
- Persistent remote controller. Default laptop-owned queue is unchanged.
- Origin outbox retry.
- Protocol or host-layout bumps.
