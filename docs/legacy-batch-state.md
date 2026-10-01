# Retired batch state

The snapshot-backed v1 batch product was retired on 2026-10-01: `worker run`, top-level `worker status`, `worker logs`, and `worker cancel`, plus the host batch submit/verify/reconcile and rsync receive path. The supported execution path is [agent tasks](usage.md#task-lifecycle) with Git transport. `worker task batch`, task DAGs, turn identifiers, and shared job archives remain part of that path.

## Drain before upgrading

Drain old work **before replacing either the laptop CLI or the worker helper**. Keep the matching old binaries available until cleanup has completed.

1. Stop new batch submissions from every old laptop. Batch commands used laptop-local state even when a remote task controller was enabled; check each submitting laptop.
2. Let executions finish, or cancel them and complete cleanup with the old tools. Resolve unfinished uploads, preacceptance ambiguity, and pending cleanup with those tools before upgrading. Helper promotion also requires active task turns to drain.
3. Confirm authoritative terminal state, completed cleanup, and no occupied host leases or unfinished upload/execution evidence. A follower disconnect, timeout, absent PID, or inert queue row does not prove that remote execution has stopped. Preserve the evidence and stop if ownership or cleanup cannot be verified.
4. Run the normal [helper update](getting-started.md#update-or-remove) only after drain. Setup's installation, layout, and protocol-upgrade checks still refuse live, incomplete, corrupt, or ambiguous state. Valid terminal archives can remain when those checks accept them; deleting evidence to make setup succeed is not a recovery procedure.

This retirement keeps protocol **7**, supervision **3**, and host layout **3**. A matching protocol number does not grant batch compatibility: an old laptop's new `Job`-scope lease request is refused before any host-state write, including when the request omits scope and defaults to `Job`. There is no batch relaunch or automatic conversion to a task.

## What remains readable

- Valid legacy `Batch` queue rows stay on disk. Task claims ignore them as candidates, FIFO blockers, worker reservations, and run-cap occupancy. The task runner does not reap them.
- Legacy job records and dashboard projections remain readable. The read-only `/api/v1/jobs/{id}` and `/logs` routes, `active_jobs` / `recent_jobs`, and queue adapters remain available; they do not resume retired work.
- Existing host leases with `Job` scope still count as busy in inventory, probe, and GC. Ignoring a local queue row does not free a remote lease.
- Existing decoders remain strict. Unknown fields or kinds, corrupt records, mismatched identities or receipts, foreign ownership, unsafe permissions, symlinks, and replaced inode identities still fail closed.

There is no new legacy GC or automatic cleanup. Use ordinary `worker gc` preview and `--apply` only for candidates it already supports, including eligible terminal job archives under its existing age, identity, lease, and lock checks. Unsupported or unsafe records remain warnings or refusals; GC does not delete all legacy local records, queue rows, or rsync snapshot caches.

## Resolve the actual storage roots

The defaults below are separate stores. Respect the configured absolute XDG roots and any explicit host-root override; do not infer a host path from the laptop state path.

| Store | Default root | Legacy artifacts within it |
|---|---|---|
| Laptop or controller's client state | `~/.local/state/mac-worker` (`$XDG_STATE_HOME/mac-worker`) | Individual `jobs/<job-id>.json` records and `kind=batch` entries in `queue/state.json` |
| Controller requests and events | `~/.local/state/mac-worker-controller` | Shared request, leader, journal, and notifier state; outside the batch cleanup boundary |
| Worker execution state | `~/.local/share/mac-worker/host` (`$XDG_DATA_HOME/mac-worker/host`) | `incoming/<job-id>/<lease-token>`, `verified/<job-id>.json`, `snapshots/<project>/<worktree>/<digest>`, and `jobs/<project>/<worktree>/<job-id>` |
| Local capture cache | `~/.cache/mac-worker/snapshots` (`$XDG_CACHE_HOME/mac-worker/snapshots`) | Individual ready captures and staging residue; local capture is also used by doctor and task `--wip` |

The worker's parent data container also owns installer state. Moving that container or a host namespace aside is not batch cleanup. The separately documented [pre-anchor installation recovery](setup-recovery.md#migrating-an-older-installation) applies only to that older installation condition.

## Explicit manual cleanup boundary

Leaving well-formed legacy local rows and caches in place is supported. If an operator chooses to remove them, use a separately reviewed, offline procedure scoped to **named artifacts**, after the drain above:

1. Record the exact store root and canonical artifact IDs. Prove each artifact belongs exclusively to a completed batch execution, with no live or ambiguous lease, transfer, task, runner, or cleanup reference. Stop writers to the affected store, including any controller leader, before editing it. An unverified or unavailable writer is not offline.
2. Back up the original queue snapshot and every selected record or cache leaf outside the owned store. Keep ownership and private permissions, and retain enough identity evidence to recover the edit.
3. Limit the change to selected local batch job records, selected `Batch` rows, or proven unreferenced batch-only cache leaves. A queue edit must publish a complete canonical snapshot atomically, preserve `next_id`, ordering, all remaining rows and their fields, and the trailing newline. Never remove or rewrite a `TaskTurn` row, task/run/DAG record, reservation, or identity to make a validation error disappear.
4. Revalidate the resulting state and normal read-only observations before restarting writers. Keep the backup; failed validation stops the procedure rather than authorizing further deletion.

Host job archives belong to the existing GC path when eligible. A snapshot cache leaf is identified by the full project/worktree/digest triple; a local capture leaf requires its own capture identity and proof that doctor or task WIP capture does not use it. A directory named `jobs` or `snapshots` is not proof that every child belongs to the retired product.

**Outside this boundary:** leases and their scope/capacity files; accepted indexes and abandonment tombstones; admission, transfer, supervisor, and state locks; `incoming` uploads and `verified` receipts or cleanup residue; installation anchors, `layout.json`, inode-bound namespace directories, controller requests/events, Git mirrors/pins, task workspaces, and agent sessions. Resolve interrupted or unsupported state with the matching old helper or a separately reviewed recovery procedure. Preserve unreadable or mismatched evidence for diagnosis.

Do not use blanket `rm -rf`, globs, namespace replacement, permission changes, or deletion of locks/tombstones to force setup or scheduling through. This document provides no bulk deletion script and authorizes no automatic removal.

## Historical records

Dated design documents, plans, benchmarks, and acceptance records retain their original results. Their batch command examples and promises to preserve batch execution describe the release at that date. Retirement notes link here so they can be read without treating those examples as current operator instructions.
