# Controller Events, Laptop Notifications, and Live Dashboard

Date: 2026-09-30

Status: proposed; owner approval is required before implementation

Code baseline: `37915a9c21c20dd022d2d140156cd234a267ee14`

Implementation plan: [2026-09-30-controller-events.md](../plans/2026-09-30-controller-events.md)

All existing-code references below are `path:line` at that baseline. Names under `src/controller/events/`, `src/client_state/events.rs`, `src/dashboard/events.rs`, and the new UI event client are proposed files, not claims about existing code.

## Purpose and boundaries

Phase 1 adds a safe controller event journal, bounded read RPCs, an inspection CLI, and event-assisted task waits. Phase 2 adds a laptop notifier and a controller-viewer SSE stream. Events are wake-up hints: durable task, queue, run, DAG, observation, and drain records remain authoritative. A missing event must affect latency only.

Phase 3 is an optional transport sketch below; it has no first-wave implementation tasks. Phase 4 phone/Tailscale access remains parked. Phase 5 per-mini daemons remains out of scope. This design does not change scheduling, result import, mutation retry identities, or the definition of task quiescence.

The brief correctly identifies multiple controller writers. The leader collects both request execution and recovery (`src/controller/health.rs:44`), runners persist task status independently (`src/turn_runner.rs:1860`), and one-shot RPCs instantiate their own state store (`src/controller/execute.rs:549`). Host-side task execution on other minis uses a different store; a host acknowledgement becomes a controller event only after the controller persists its observation. The supplied 40 ms RPC figure is context, not a benchmark verified by this documentation track.

## Decision 1 — extend existing mechanisms and preserve authority

Existing mechanism: additive controller feature discovery (`src/features.rs:8`, `src/controller/health_read.rs:282`), read reply identity verification (`src/controller/read.rs:69`), injected long-poll clocks (`src/controller/read.rs:492`, `src/transfer.rs:142`), private rooted append and atomic replacement (`src/rooted_fs.rs:1752`, `src/rooted_fs.rs:1805`), laptop operation cache (`src/controller/envelope.rs:127`), Herdr notification transport (`src/herdr.rs:619`), and dashboard collection/cache infrastructure (`src/dashboard/service.rs:419`). Reuse these boundaries.

Not found — checked: controller command routing (`src/controller/execute.rs:559`), feature registry (`src/features.rs:8`), local task persistence (`src/client_state.rs:2908`), existing status event annotations (`src/controller/read.rs:138`), runner JSON logs (`src/turn_runner.rs:1370`), and viewer routes (`src/dashboard/web.rs:156`). None supplies a shared durable lifecycle journal or browser SSE feed. Status annotations and runner log records are different mechanisms and must not be relabeled as this journal.

Keep `PROTOCOL_VERSION = 7` (`src/protocol.rs:5`). Advertise `controller.events` in the controller feature list only. Do not add it to host `ProbeResponse.features`: host and controller registries are distinct (`src/features.rs:7`). Feature discovery describes the serving RPC binary, not necessarily the live leader binary (`src/controller/health_read.rs:50`); mixed-version writers are therefore expected to leave event gaps.

Rejected alternatives: leader-only emission misses runner/RPC writes; forwarding the mini-1 notifier does not reach the laptop socket; replacing durable records with events changes ownership and failure recovery.

## Decision 2 — typed, small, privacy-safe events

Use an internal Rust enum with a typed payload per known event. Serialize a separate tolerant wire envelope:

```json
{
  "schema_version": 1,
  "journal_id": "614dc3be-668f-4922-bd31-b1d7a0056790",
  "seq": "42",
  "time_millis": 1790726400000,
  "kind": "turn.finished",
  "data": {
    "task_id": "0e7b7f91-5a91-4dbd-bcc2-c33338890752",
    "turn_id": "16366c4c-a0aa-419c-8b78-3dcd61e68102",
    "run_id": null,
    "outcome": "needs_input",
    "code": null
  }
}
```

`seq` is a canonical decimal-string u64, without leading zeros except `"0"`. JavaScript must not round it through a number. A cursor is `{journal_id, seq}`; ordering is meaningful only within one journal. Times are diagnostic wall-clock times and never determine ordering, liveness, or timeout expiry. Journal IDs survive leader restarts; replacement of the journal creates a new UUID.

Every event, including its envelope, is at most 1,024 UTF-8 bytes. Identifiers are existing UUID types or validated inventory worker names. Worker name validation already excludes paths/control characters and bounds length (`src/job.rs:6034`, `src/client_state.rs:5690`). Do not expose user run names, project/worktree labels, branch names, OIDs, SSH destinations, process tokens, titles, prompts, questions, summaries, failure prose, raw logs, environment values, or configuration paths. Failed outcomes contain a closed allowlist of stable codes; unknown/free-form reasons become `TURN_FAILED`, never the original string. `TaskOutcome::Failed.reason` is explicitly free-form in the current model (`src/task.rs:337`, `src/task.rs:360`).

Construct events from selected fields; never serialize a `LocalTaskRecord`, `TaskStatus`, queue owner, or `AdmissionObservation` wholesale. The observation includes SSH and binary paths (`src/job.rs:2002`). Apply `RedactionBoundary` to any display text as defense in depth (`src/redaction.rs:46`, `src/redaction.rs:171`); redaction is not permission to put prose into events. Tests plant paths and tokens in every excluded field and assert they cannot enter RPC, journal, SSE, or notification output.

| Kind | Safe payload and meaning | Existing-code basis |
| --- | --- | --- |
| `task.created`, `task.changed`, `task.removed` | Task ID, optional run/latest-turn ID, state, stable code; changed is a resource invalidation. Removed is a durable submission-rollback tombstone. | `src/client_state.rs:2908`, `src/client_state.rs:3131`, `src/client_state.rs:3290` |
| `turn.started` | Task/turn/run IDs and worker name; remote acceptance has been observed and saved locally. It is not a queue claim or a submission ACK. | `src/turn_runner.rs:1349`, `src/turn_runner.rs:1390`, `src/turn_runner.rs:1419` |
| `turn.finished` | Task/turn/run IDs, terminal outcome, stable code. First saved terminal outcome for this turn. | `src/turn_runner.rs:1500`, `src/task.rs:327`, `src/task.rs:337` |
| `turn.outcome_changed` | Same identifiers plus corrected outcome/code, when a saved terminal result is corrected. | `src/turn_runner.rs:1598` |
| `task.auto_continue_scheduled` | Task/run IDs, previous turn ID and planned next turn ID. Saved intent, not a promise that admission succeeded. | `src/task_client.rs:139`, `src/task_client.rs:3681` |
| `task.closed`, `task.abandoned` | Task/run/latest-turn IDs, new state, stable code. Cancelling a turn alone does not close a task. | `src/task_client.rs:2680`, `src/task_client.rs:2732`, `src/task_client.rs:4397` |
| `queue.changed` | Optional affected job/turn ID, entry kind/state and stable reason; no full queue array or owner identity. Removal has a null state. Affinity changes invalidate queue scheduling views without exposing affinity keys. | `src/client_state.rs:5385`, `src/client_state.rs:2113`, `src/client_state.rs:2208` |
| `run.changed` | Run ID and optional affected task ID; covers membership, DAG gates, and publication-reservation changes. | `src/client_state.rs:3501`, `src/client_state.rs:4178`, `src/client_state.rs:4771`, `src/client_state.rs:4896` |
| `dag.child_admitted` | Run/task/turn IDs; emit when membership and `Submitted` are both durable. Do not expose the user batch-node label or emit on `Claimed`. | `src/client_state.rs:3747`, `src/client_state.rs:3825`, `src/client_state.rs:3838` |
| `run.finished`, `run.reopened` | Run ID, current quiescent boolean and aggregate exit code. Settlement can be reversed by a later follow-up. | `src/task.rs:2245`, `src/task_client.rs:4607`, `src/dag.rs:737` |
| `worker.changed`, `worker.unavailable`, `worker.available` | Validated worker name, recorded ready/unknown state, observation time, generic stable code. Semantic availability events compare committed ready states. | `src/client_state.rs:2338`, `src/client_state.rs:2382`, `src/admission.rs:272` |
| `controller.drained`, `controller.undrained` | Saved drain boolean only. Idempotent writes do not repeat the semantic event. | `src/controller/drain.rs:61` |

Terminal outcomes include `done`, `needs_input`, `blocked`, `unknown`, `failed`, `cancelled`, `timed_out`, and `lost`, matching all eight existing `TaskOutcome` variants (`src/task.rs:337`). Do not narrow this to the five examples in the brief.

Generic changes may accompany a semantic event in the same batch. No-op writes and failed CAS attempts emit nothing. Retries may still duplicate hints across crash recovery; consumers reconcile and deduplicate rather than assuming exactly-once emission. Journal order is publication order, not cross-domain causal history: recovery can observe a completed accepted turn and publish the acceptance hint after the saved terminal observation.

Rejected alternatives: a closed wire enum would break a newer-controller/older-event-client pair; carrying result prose would expand the existing redaction surface; interpreting `Active` as a start is false because follow-up preparation saves `Active` and a start timestamp before host acceptance (`src/task_client.rs:3839`).

## Decision 3 — instrument durable publication, across every writer

Add an optional event sink to `ClientStateStore`, shared by its clones. Attach it explicitly at controller runtime construction and when reopening the store with a deadline. Bare `ClientStateStore::open` remains a no-event API for existing tests and laptop-local mode (`src/client_state.rs:553`, `src/client_state.rs:646`). Controller leader and `events.read` initialize the journal; direct local commands/runner children attach an already initialized journal discovered from `PathLayout`. A laptop consumer never opens or creates the laptop task store. The controller read path is already remote-only (`src/controller/execute.rs:746`).

The integration audit must cover these entry points, not just the leader:

| Process | Store entry point | Required attachment |
| --- | --- | --- |
| Leader tick | `src/lib.rs:1423`; execution/recovery at `src/lib.rs:1477` | Initialized journal sink passed into the leader state store and all handlers. |
| One-shot RPC handler / host helper `host controller-rpc` | `src/lib.rs:1528`, `src/controller/execute.rs:564`, `src/controller/execute.rs:568`, `src/controller/execute.rs:579` | Sink on read routes that reconcile, lifecycle routes, and mutation handlers; drain uses the same journal directly. |
| Detached runner and runner-launch/recovery helper | `src/lib.rs:1625`, `src/lib.rs:1646`, `src/turn_runner.rs:181` | Attach initialized journal even though the controller host loads a local execution configuration. Preserve the sink on `open_until`. No new hidden runner flag, which would break an older helper parser. |
| Controller-viewer HTTP mutation helper and local collector | `src/lib.rs:1124`, `src/dashboard/command.rs:117`, `src/dashboard/command.rs:122` | Its store writes/reconciliation use the sink; its event tailer is read-only and uses local files, not RPC. |
| Direct controller-host CLI helpers | Status/cancel at `src/lib.rs:326`, `src/lib.rs:350`; streaming legacy queue operations at `src/lib.rs:610`; drain at `src/lib.rs:1308` | Attach only an initialized controller journal. Cover shared queue and admission writers, without adding a journal to unrelated local/laptop stores. |
| Read-only export/GC helper | `src/controller/stream_rpc.rs:383`, `src/lib.rs:3396` | No lifecycle producer currently: these loads do not mutate task/run/queue state. Re-audit if implementation introduces a write. |

The following is the durable-writer coverage map. Central hooks carry the routine coverage; semantic helpers cover transitions which need acceptance proof or more than one record. The process column names the caller that can write, not an assertion that the leader owns every file.

| Durable family / functions | Processes | Publication point and events |
| --- | --- | --- |
| `create_task`, `mutate_task`, `update_task_if_current`, `update_task_locked_before_final_sync`, `replace_task_locked_if_current` (`src/client_state.rs:2908`, `src/client_state.rs:2989`, `src/client_state.rs:3044`, `src/client_state.rs:3063`, `src/client_state.rs:3131`) | RPC submit/say/close/cancel; leader recovery; detached runner; viewer/direct helper | After successful authoritative atomic publication, emit `task.created/changed` and before/after semantic changes. At `src/client_state.rs:3204`, publication is complete but later active-index retirement can fail (`src/client_state.rs:3205`); hook here, before derived cleanup. Never use the `before_final_sync` callback as an event hook. |
| Follow-up, acceptance, terminal, imported-head, and retirement (`src/task_client.rs:3839`, `src/turn_runner.rs:1390`, `src/turn_runner.rs:1419`, `src/turn_runner.rs:1860`, `src/turn_runner.rs:1500`, `src/client_state.rs:3080`, `src/turn_runner.rs:2325`) | RPC say; runner; leader/RPC reconciliation | Central task hook covers outcomes, runner presence, and result-proof changes. Emit `turn.started` explicitly after the accepted response/status is saved; recovery may repeat that hint. A saved fetched head contributes only a boolean proof to snapshots, never an OID. Runner clearing and row removal each invalidate quiescence. |
| Auto-continuation intent and execution/rollback (`src/task_client.rs:139`, `src/task_client.rs:3681`, `src/task_client.rs:3704`, `src/task_client.rs:3723`) | Runner retirement; leader/RPC recovery | Saved intent creation emits `task.auto_continue_scheduled`; clearing or rollback emits `task.changed`. It is not emitted a second time just because continuation execution retries. |
| Close/discard/close-intent and cancellation (`src/task_client.rs:2732`, `src/task_client.rs:2783`, `src/task_client.rs:2824`, `src/task_client.rs:4480`, `src/task_client.rs:4506`, `src/task_client.rs:4539`) | RPC/direct/viewer helper; runner observes cancellation | Central task hook covers close fences and saved state/outcome. Queue-only cancellation also goes through queue publication. `task.closed/abandoned` compares task states, not just a cancel ACK. |
| Capacity/handoff/remote-recovery failures (`src/turn_runner.rs:631`, `src/turn_runner.rs:1962`, `src/turn_runner.rs:2091`, `src/task_client.rs:5942`, `src/task_client.rs:5987`) | Runner; leader recovery; RPC wait/reconcile/status helper | Same task hook covers abandoned, lost, cancelled, and corrected terminal state. Failure codes are allowlisted. Liveness absence alone is not sufficient (`src/client_state.rs:141`). |
| Submission rollback record and turn-tree removal (`src/client_state.rs:3290`, `src/client_state.rs:3318`) | RPC/leader submission helper; runner handoff rollback | Emit `task.removed` only after durable record deletion; turn-tree retirement alone is an invalidation, not a terminal result. If failure is injected after deletion before emission, snapshot absence repairs it. |
| Queue enqueue, reserve/release/bind/takeover slots, complete spawn, adopt/claim/revert, cancellation, dead dispatch recovery, remove/restore, park/unpark, replacement failure counters (`src/client_state.rs:853`, `src/client_state.rs:931`, `src/client_state.rs:1005`, `src/client_state.rs:1059`, `src/client_state.rs:1140`, `src/client_state.rs:1455`, `src/client_state.rs:1566`, `src/client_state.rs:1721`, `src/client_state.rs:1739`, `src/client_state.rs:1820`, `src/client_state.rs:1970`, `src/client_state.rs:2029`, `src/client_state.rs:2079`, `src/client_state.rs:4386`, `src/client_state.rs:4443`) | RPC/helper submit/cancel; runner launch/handoff/retirement; leader recovery | Hook `publish_queue_snapshot` after the directory sync at `src/client_state.rs:5408`. Hooking `update_queue` alone misses direct publication by spawn completion and dispatch handoff (`src/client_state.rs:1188`, `src/client_state/runner_dispatch.rs:102`). A large queue replacement emits bounded hints; if over 32 changed entries, emit one global `queue.changed` rather than an unbounded array. |
| Affinity publication/removal (`src/client_state.rs:2113`, `src/client_state.rs:2149`, `src/client_state.rs:2208`) | Admission/runner/helper scheduling callers | After each successful durable change, global `queue.changed` with `AFFINITY_UPDATED`; omit project/worktree keys. A partial two-record update is repaired by a snapshot/refetch. |
| Run creation, DAG creation/recovery, membership, branch reservation/release (`src/client_state.rs:3501`, `src/client_state.rs:3526`, `src/client_state.rs:3963`, `src/client_state.rs:4178`, `src/client_state.rs:4208`, `src/client_state.rs:4255`, `src/client_state.rs:4896`) | RPC/leader batch helper; runner/DAG advancement; recovery helper | `run.changed` after the corresponding durable run write, including recovered partial DAG/run creation. Do not declare admission/settlement while only the pending index or DAG file exists (`src/client_state.rs:3564`, `src/client_state.rs:3587`). Reservation events omit branch names. |
| DAG claim/retake/block, bound execution proof, and submit (`src/client_state.rs:3683`, `src/client_state.rs:3747`, `src/client_state.rs:3807`, `src/client_state.rs:3843`, `src/client_state.rs:4771`, `src/client_state.rs:4843`) | Leader recovery; runner completion; RPC wait/close/DAG helper (`src/task_client.rs:5118`) | `run.changed` after DAG publication. `dag.child_admitted` only after saved run membership and `Submitted`; include the recovery `MarkSubmitted` path, not only normal submit. Binding data are not payload data. |
| Admission observation publish/conditional commit/invalidate (`src/client_state.rs:2338`, `src/client_state.rs:2359`, `src/client_state.rs:2382`, `src/client_state.rs:5611`) | Admission probes in runner/leader/RPC/direct helper; worker pipeline at `src/admission.rs:228` | Only the committed winning observation emits `worker.changed` and a ready-state edge. Stale writes and CAS losers do not. Invalidation emits unknown-state `worker.changed`, not an unavailable alert. |
| Drain flag (`src/controller/drain.rs:61`, `src/controller/control.rs:70`) | RPC handler; direct controller helper | Compare old flag under the existing exclusive drain lock, save flag durably, then append `controller.drained/undrained`. Keep the launch-permit fence (`src/controller/drain.rs:95`). |

Private prompts, prepared bindings, raw runner logs, active/pending indexes, and controller request bookkeeping are not additional public lifecycle authorities. Their writes are accounted for by the enclosing task/queue/run transition, or remain private/derived only (`src/client_state.rs:4550`, `src/client_state.rs:4601`, `src/client_state.rs:4716`, `src/client_state/active_tasks.rs:27`, `src/client_state.rs:3672`, `src/controller/store.rs:419`). Legacy job records remain legacy job state (`src/client_state.rs:2692`, `src/client_state.rs:2768`); their shared queue/admission effects are covered, without inventing task events for jobs.

`RunRecord` has no persisted finished flag (`src/task.rs:2245`). Compute affected-run quiescence before/after task, queue, membership, or DAG changes from saved records using the same pure busy/terminal predicates as wait (`src/client_state.rs:122`, `src/task_client.rs:6093`, `src/task_client.rs:6349`, `src/dag.rs:737`). Emit `run.finished` only on false-to-true and `run.reopened` on true-to-false. Do not add a strict RunRecord field or a new unbounded settlement-latch store. Evaluate only affected runs; no historical scan on every leader tick. The existing active tick caps work at 32 tasks (`src/client_state/active_tasks.rs:46`). If an affected-run calculation exceeds its bounded work budget, emit `run.changed` and let snapshot reconciliation derive settlement.

Worker availability means the committed admission observation's ready state, not an always-on reachability guarantee (`src/admission.rs:505`, `src/job.rs:2117`). The first observation emits `worker.changed`; known ready-to-not-ready and not-ready-to-ready emit the semantic edges. There is no new idle heartbeat. Dashboard memory-only probes do not become journal producers. Snapshot worker rows carry an observation time and unknown/stale state rather than asserting stale ready data are live.

Rejected alternatives: scattering writes after high-level command success misses durable writes followed by cleanup failure; exporting runner `turn_accepted` logs repeats records already duplicated in existing code (`src/turn_runner.rs:1370`, `src/turn_runner.rs:1392`, `src/turn_runner.rs:1425`); permanently sealing runs contradicts resumable tasks.

## Decision 4 — an append-only journal, with bounded cross-process publication

Choose option (a), a shared append-only journal. Option (b) still needs every writer to update a global revision, then retain per-record versions/tombstones and calculate cross-domain diffs. Current records have no such common revision (`src/task.rs:2174`, `src/task.rs:2245`), and strict saved-record decoding makes changing them risky (`src/task.rs:2174`, `src/task.rs:2428`). A separate journal leaves N-1 state readers intact and uses rooted append primitives already present (`src/rooted_fs.rs:1752`).

Location: `PathLayout::controller_state_root()/events/`, mode 0700, files 0600. This is a sibling of the client state root (`src/paths.rs:54`). Do not put `events/` inside `paths.state`: that root has a closed allowed-entry list (`src/client_state.rs:6896`). Controller request enumeration selects only `req-*.json` and ignores the new directory (`src/controller/store.rs:1164`).

Proposed layout: stable `journal.lock`, bounded `manifest.json`, one bounded `pending.json`, and `segment-<first-seq>.jsonl`. The manifest records schema, persistent journal UUID, oldest/head sequence, active file identity and committed byte end. File names are constructed internally from validated numeric sequences; clients never choose a file path.

| Bound | Decision |
| --- | --- |
| Event / append batch | 1,024 bytes per event; at most 32 events / 32 KiB per batch |
| Segment / retained window | 256 KiB per segment; at most 64 retained segments / 16 MiB of committed data |
| Rotation residue | At most one extra segment during recovery; manifest and pending each at most 64 KiB |
| Lock admission | Nonblocking flock retries, at most 50 ms to acquire the journal lock for a state writer; reader lock admission also consumes its request budget |
| Local read wake-up | Recheck at most every 200 ms through an injected runtime; no store/journal lock held across a wait |

These are proposed operational defaults, not measured pool requirements. They bound replies, disk growth, and lock admission using the existing private-lock/read primitives (`src/rooted_fs.rs:1301`, `src/rooted_fs.rs:1343`, `src/rooted_fs.rs:1357`). A 50 ms lock budget does not pretend to bound a kernel fsync stall; report that limitation and keep any lock-held I/O small.

Publication algorithm:

1. The authoritative state writer completes its existing durable write under its existing lock. Capture typed before/after hints. Failed/no-op publication yields no events.
2. Acquire `journal.lock` exclusive as the final lock in the operation. Recover any validated pending append/rotation before allocating sequence numbers.
3. Save a private, fsynced pending batch containing the expected manifest identity/head, target segment/offset, next sequence numbers, and exact bounded event bytes. Create/rotate only using rooted fd-relative operations.
4. Append complete newline records with `RootedDir::open_private_append`, validate the open file/path binding, and `sync_all` the file (`src/rooted_fs.rs:1752`, `src/rooted_fs.rs:1775`). Atomically replace and sync the manifest to commit the new head/byte end.
5. Remove pending and fsync its parent. Retire only manifest-excluded owned segments and sync the directory. Rotation never deletes retained records before the new manifest is durable. Finish cleanup before allocating another segment, so orphan accumulation cannot grow the bound.
6. Release the journal lock. The event sink logs a stable `CONTROLLER_EVENTS_UNAVAILABLE` diagnostic on failure without returning an error for an already successful state mutation. Do not roll back state, revoke a lease, or repeat a controller mutation because event append failed.

Use private stable lock validation, `O_NOFOLLOW`, ownership/type/inode checks, atomic no-replace creation, and directory fsync (`src/rooted_fs.rs:1343`, `src/rooted_fs.rs:1752`, `src/rooted_fs.rs:2014`, `src/rooted_fs.rs:3627`). `write_new_private_file` alone is not a durability boundary (`src/rooted_fs.rs:2193`). Never unlink/recreate a live lock file to break contention.

Readers serve only the committed manifest prefix under a short shared journal lock. If pending work exists, release the shared lock, finish validated recovery under exclusive lock, and reread; do not expose an ambiguous head. Recovery compares exact pending bytes/identities with the segment: finish a matching complete append once, or repair a proven matching partial uncommitted suffix, then commit the manifest. Reuse no sequence already committed/served in that epoch. An unexpected suffix, inode swap, duplicate sequence, invalid ownership, malformed complete record, or contradictory manifest is corruption; fail closed with the stable event error and use snapshots. Never silently truncate unknown bytes or reset a corrupt journal. A deliberate operator reset after diagnosis creates a new UUID; no new public reset command is part of this wave.

Lock order stays the current order with the journal appended last. Queue operations already acquire StateLock before queue.lock (`src/client_state.rs:5201`). A held runner-log fence may enclose an existing state operation, but no state operation acquires a runner-log fence while holding StateLock (`src/runner_log.rs:156`, `src/runner_log.rs:195`). The journal code never calls back into state, queue, drain, Git, SSH, Herdr, or HTTP. A reader releases the journal lock before obtaining a state snapshot; drain is read separately, never nested beneath StateLock. The lifetime controller leader lock remains separate (`src/controller/leader.rs:22`); journal locks are transaction-scoped.

Rejected alternatives: in-memory broadcast alone cannot cross runner/RPC processes or survive restart; leader revision scanning delays or loses transitions; journaling before state creates false facts; blocking a successful task on an optional notification journal changes existing failure behavior.

## Decision 5 — bounded read RPCs and explicit snapshot repair

Add `events.read` and an additive companion `events.snapshot`, both under `controller.events`. The companion is necessary: current status/list replies do not supply one bounded, privacy-safe snapshot of queue, drain, admission observations, and run/DAG quiescence (`src/controller/read.rs:91`, `src/controller/lifecycle.rs:234`). Route both before the generic durable-mutation fallback (`src/controller/execute.rs:559`); neither writes request rows, changes lifecycle state, or mints task/turn IDs.

`events.read` arguments are exactly:

```json
{"after":{"journal_id":"614dc3be-668f-4922-bd31-b1d7a0056790","seq":"41"},"limit":128,"wait_ms":15000}
```

Missing/null `after` is bootstrap, not permission to replay all retained history. Default limit 128, clamp integral limits to 1..256. Default server wait 0; clamp valid u64 `wait_ms` to 20,000. Laptop follow default is 15,000 ms. Reject wrong types, duplicate keys, invalid UUID/decimal cursor, negative numbers, or unknown argument keys; use existing strict request parsing (`src/controller/protocol.rs:29`, `src/controller/protocol.rs:211`).

Wrap the result in the unchanged `ControllerReadReply<T>` envelope: protocol version, command, request ID, payload digest, result (`src/controller/read.rs:24`). Example results:

```json
{
  "type": "batch",
  "schema_version": 1,
  "journal_id": "614dc3be-668f-4922-bd31-b1d7a0056790",
  "oldest_seq": "1",
  "head_seq": "44",
  "next_after": {"journal_id":"614dc3be-668f-4922-bd31-b1d7a0056790","seq":"42"},
    "events": [{
      "schema_version": 1,
      "journal_id": "614dc3be-668f-4922-bd31-b1d7a0056790",
      "seq": "42",
      "time_millis": 1790726400000,
      "kind": "turn.finished",
      "data": {
        "task_id": "0e7b7f91-5a91-4dbd-bcc2-c33338890752",
        "turn_id": "16366c4c-a0aa-419c-8b78-3dcd61e68102",
        "run_id": null,
        "outcome": "needs_input",
        "code": null
      }
    }],
  "has_more": true
}
```

An empty batch keeps `next_after == after`, has `has_more == false`, and is returned on long-poll expiry. `head_seq` is informational, not a cursor to checkpoint past unconsumed rows. Arrays are strictly increasing and contiguous within the delivered window; `next_after` equals the last delivered sequence. Clients validate envelope identity, journal equality, ordering, bounds, count, and cursor consistency before consuming.

```json
{
  "type": "snapshot_required",
  "schema_version": 1,
  "reason": "cursor_expired",
  "journal_id": "614dc3be-668f-4922-bd31-b1d7a0056790",
  "oldest_seq": "500",
  "head_seq": "700"
}
```

Reasons: `bootstrap`, `journal_changed`, `cursor_expired` when `after.seq < oldest_seq - 1`, and `cursor_ahead` when `after.seq > head_seq`. An empty journal has head 0 and oldest 1. A cursor at oldest minus one is valid. Unknown future reasons mean repair, not a fatal decode. A corrupt/unreadable journal yields the stable event-unavailable host error, not a fabricated empty batch or a healthy snapshot_required head.

Reuse `ResolutionRuntime` and the log long-poll loop shape (`src/transfer.rs:142`, `src/controller/read.rs:516`). All sleep occurs outside locks. Each reply contains at most 256 KiB of event records plus bounded metadata/envelope, well below the 1 MiB frame cap (`src/controller/protocol.rs:11`). Still measure serialized bytes before framing, and stop a batch before its budget. Reads, retries, lock waits and sleep share one monotonic deadline. The 20 s wait cap leaves transport work within the existing 30 s RPC policy (`src/controller/mod.rs:90`); clients cap requested wait to their remaining deadline. No new busy-loop process or held store lock for 20 s.

`events.snapshot` arguments:

```json
{"baseline_after":{"journal_id":"614dc3be-668f-4922-bd31-b1d7a0056790","seq":"700"},"after_key":null,"limit":128}
```

Its new result has `schema_version`, echoed `baseline_after`, `rows`, `next_key`, and `complete`. Default/clamped limit is 128/1..256; each row is at most 1,024 bytes. `next_key` is a validated typed key `{kind,id}` in fixed kind order (task, run, queue, worker, controller), then identifier order; no filesystem path, arbitrary filename or server lease. Requests/replies have strict identity/key/count checks. A row contains only:

| Row | Safe snapshot fields |
| --- | --- |
| Task | Task/run/latest-turn IDs, saved run-membership boolean, task state, latest terminal outcome/code, runner-present, close/continuation/submission-intent booleans, queue/busy code, quiescent boolean, result-import-proof boolean |
| Run | Run ID, member count, pending DAG count, current quiescent boolean, aggregate exit code; membership itself is obtained from task rows with the membership flag, not an unbounded array |
| Queue | Job/turn ID, task ID when mapped, entry kind/state, stable blocking/replacement code |
| Worker | Inventory name, recorded ready/unknown state, observation time/freshness; no SSH binding or raw metrics |
| Controller | Saved drained boolean |

Take each state page under bounded existing store locks; return at most the requested number of rows and bound enumeration, record reads and derived-run work by the request deadline. A run row can require extra saved member/queue/DAG reads, so the row limit is not a claim that total file reads equal that limit. The task membership flag comes from saved RunRecord membership, not TaskMeta.run_id alone: the DAG path appends membership separately (`src/client_state.rs:3825`, `src/client_state.rs:4896`). Read drain with its own lock after releasing state locks. No SSH, Git, remote probing, runner-log acquisition, or mutations in this endpoint. Factor the saved-record quiescence predicate out of wait rather than calling `TaskClient::wait_poll`, which reconciles/advances work (`src/task_client.rs:4607`). Page construction that hits its work deadline returns a stable error, never a falsely complete page. A null baseline is allowed when the journal is unavailable; it means state-only reconciliation and cannot advance a journal cursor.

Repair procedure for CLI/notifier:

1. Obtain a journal baseline H with a zero-wait read (bootstrap/reset response or batch head). Release journal locks.
2. Read snapshot pages into a replacement safe projection. Do not overwrite a previous complete projection after a partial failure.
3. Replay events after H, invalidate/refetch affected state rows, and checkpoint only consumed cursors. Do not jump to a post-snapshot head.
4. Run a full paginated anti-entropy sweep every 15 s, without overlapping sweeps, even if events are continuously arriving. A sweep may exceed 15 s on large history; the next sweep starts after completion.

Pages are individually coherent, not one frozen cross-domain snapshot. Concurrent insertion/deletion or a missed post-write event can require the next full sweep. Once writers settle, a completed sweep converges; events do not promise reconstruction of missed historical transitions. Snapshot absence removes stale task/queue entries. Journal failure keeps state-only sweeps available; retry event discovery/read with bounded reconnect backoff.

Rejected alternatives: making snapshots solely on an explicit cursor gap misses the state-write/event-write crash gap with a still-valid cursor; treating current list RPC as an unlimited journal snapshot risks frame growth; a snapshot transaction spanning drain/state/journal locks would invert existing locks and add a long critical section.

## Decision 6 — compatibility audit at 37915a9

The first wave changes no existing DTO shape. Adding optional fields to a strict old decoder is not N-1 compatible. New event wire/result/row envelopes deliberately tolerate unknown fields and unknown kinds; validate required fields and use unknown kinds/versions as a global invalidation plus snapshot refresh. Known internal producer payloads remain closed and typed.

| Existing DTO / schema | Unknown-field behavior at baseline | Design action |
| --- | --- | --- |
| `ProbeResponse`, features | Tolerant struct; optional features with default (`src/protocol.rs:42`, `src/protocol.rs:63`) | Unchanged; controller capability is not a host capability. |
| `ControllerHealthStatus`, features | Tolerant struct; optional features (`src/controller/health_read.rs:48`, `src/controller/health_read.rs:51`) | Extend list contents, not fields. Existing safe selector discovers features even without healthy leader state (`src/controller/health_read.rs:247`, `src/controller/health_read.rs:282`). |
| `WireRequest` | `deny_unknown_fields` (`src/controller/protocol.rs:29`) | Keep envelope fixed; new commands use its existing body slot. Validate new body keys explicitly. |
| `ControllerReadReply<T>` | `deny_unknown_fields` (`src/controller/read.rs:24`) | Reuse unchanged envelope and verification. |
| `ControllerTaskStatusResult` | `deny_unknown_fields`; existing `events: Vec<Value>` (`src/controller/read.rs:115`) | Unchanged; do not add cursor/quiet/notify fields or repurpose status annotations. |
| Task list projection | `TaskListProjection`/`TaskListRow`/`TaskRunProjection` tolerate unknown fields (`src/task_view.rs:149`, `src/task_view.rs:158`, `src/task_view.rs:197`); nested `RunProgress` is strict (`src/task.rs:2460`) | Unchanged; `task.list` returns this projection (`src/controller/read.rs:446`). Polling fallback selects safe fields from the existing result. |
| `ControllerTaskLogsResult` | `deny_unknown_fields` (`src/controller/read.rs:207`) | Unchanged; logs and their byte cursor are separate from lifecycle events. |
| `ControllerWaitPollResult` | `deny_unknown_fields` (`src/controller/lifecycle.rs:42`) | Unchanged: task_ids, quiescent, exit_code. Events only change laptop wake-up policy. |
| `ControllerReconcileResult`, `ControllerPublishRetryResult` | `deny_unknown_fields` (`src/controller/lifecycle.rs:85`, `src/controller/lifecycle.rs:119`) | Unchanged; event sinks observe resulting saved state, not new result fields. |
| `DrainBody`, `DrainResult` | Both `deny_unknown_fields` (`src/controller/control.rs:18`, `src/controller/control.rs:29`) | Unchanged; publish through the durable drain implementation. |
| Private `DrainState` | `deny_unknown_fields` (`src/controller/drain.rs:27`) | Unchanged version/boolean file; compare values for idempotent semantic emission. |
| `TaskOutcome`, `TurnSummary`, TaskStatus wire, LocalTaskRecord wire, RunRecord wire | Strict enums/structs/custom wire decoders (`src/task.rs:336`, `src/task.rs:1185`, `src/task.rs:1518`, `src/task.rs:2174`, `src/task.rs:2428`) | No persisted or existing protocol schema additions. |
| `Config`, `ControllerConfig`, `NotificationsConfig` | All strict (`src/config.rs:15`, `src/config.rs:67`, `src/config.rs:89`) | Preferences are CLI options and private new cache schema, not new TOML keys. Existing `notifications.herdr` still governs automatic Herdr selection. |
| `DashboardSnapshot` | Serialize-only viewer DTO; API v1; viewer-local revision (`src/dashboard/model.rs:26`) | Shape unchanged. New SSE route/control messages carry stream metadata; never treat dashboard revision as journal sequence. |

Tests pin the task list nested decoder behavior as well as baseline-shaped strict decoders against new replies and newer clients against missing/null feature lists. Never send `events.read` to a controller before successful feature discovery: unknown commands currently reach durable request handling (`src/controller/execute.rs:577`). A transport failure is not evidence of feature absence.

Rejected alternative: a protocol bump or top-level cursor on existing responses imposes a lockstep deployment that the brief explicitly forbids.

## Decision 7 — inspection CLI and task wait use hints differently

Add `worker events [-f] [--json] [--since N]`. The existing global JSON flag already supports this position (`src/cli.rs:17`); `-f` follows the existing log convention (`src/cli.rs:124`). Commands require an enabled controller route. Human output is identifiers, kinds, states/codes and cursor only. JSON is newline-delimited typed records: `event`, `snapshot` page, and `snapshot_required` control records, clearly distinguishable.

Without `--since`, print a current safe snapshot, then exit or follow from the captured baseline. `--since N` binds N to the currently discovered journal UUID for this invocation, drains the retained suffix, and optionally follows; it is not a restart-safe cursor across controller resets. Non-follow replay captures a head at startup and drains only through that head, so continuously arriving events cannot keep a one-shot command open forever. Cursor expiration/ahead/reset prints its control reason and performs snapshot repair. Future event kinds print validated envelope metadata only and invalidate the projection; opaque unknown data/fields are not copied into CLI output. `--json` never includes raw server errors/paths.

Older controller: without `--since`, use existing remote task/list/status and drain reads as a reduced polling snapshot (2 s in follow mode), labeled `source: polling`; unavailable queue/worker snapshot domains are marked unsupported, not synthesized. With `--since`, return a stable `CONTROLLER_EVENTS_UNSUPPORTED` error because there is no ordered replay. Feature absence is a supported fallback; authentication/transport failure follows normal errors or bounded follow reconnect, never a laptop-local task store.

For `worker task wait`, retain `task.wait.poll` as the sole authority for reconciliation, DAG admission, selected task IDs, quiescence, blocked errors, and aggregate exit status (`src/controller/lifecycle.rs:164`, `src/task_client.rs:4607`). An event cannot prove that import, runner cleanup or queue retirement finished: terminal status is saved before import (`src/turn_runner.rs:1500`) and the wait guarantee includes immediate close/say/fetch usability (`src/task_client.rs:4572`).

Algorithm:

1. Construct the existing monotonic `WaitDeadline` and deadline-wrapped runner; make the usual `task.wait.poll` immediately. Return an already-quiescent result with existing exit semantics.
2. If busy, discover features within that same deadline. With events, capture a zero-wait journal baseline and poll `task.wait.poll` again after capture, covering the subscribe/check race.
3. While busy, read after the cursor with `wait_ms = min(15_000, 1_000, remaining_budget_ms)`. Any batch, reset or read timeout wakes another `task.wait.poll`; a periodic 1 s ceiling keeps reconciliation and DAG advancement moving even when no event was published.
4. Advance only the validated consumed cursor. On journal/read incompatibility or optional event failure, disable events for that wait invocation and resume the existing 100 ms polling delay; authoritative wait errors still propagate unchanged.
5. Check the original deadline before and after each RPC and before returning. Feature discovery, subscriptions, lock waits, retries and sleeps never reset or extend it. Exhausted budget keeps the existing timeout outcome.

Use the pure predicates and aggregate classification already present (`src/task_client.rs:6093`, `src/task_client.rs:6349`, `src/task.rs:355`). Preserve `WAIT_BLOCKED/RUNNER_REPEATED_FAILURE` behavior (`src/task_client.rs:5737`), liveness confirmation, run membership expansion, and the distinction between attached runner exits and aggregate wait exits (`tests/controller/controller_say_wait_exit.rs:229`). No new wait-result field or event-based shortcut.

Rejected alternatives: waiting exclusively for a terminal event can hang after a crash and finish before quiescence; a 15 s unbroken event wait delays reconciliation-driven progress; recalculating deadlines after feature discovery changes timeout semantics.

## Decision 8 — foreground laptop notifier, using existing channels

Command: `worker notify [--follow] [--quiet] [--channel auto|macos|herdr|both]`. Without follow, reconcile once, process available new eligible decisions, and exit. With follow, run foreground with interruptible long-poll and bounded reconnect backoff (1, 2, 4, then 5 s cap, injected clock). Print only safe operational summaries to stderr; reserve stdout JSON mode for safe decision records. A second notifier for the same target fails with a stable already-running code before subscribing.

Default hosting is foreground; the user may run it in a laptop Herdr pane. Do not install a laptop LaunchAgent in this wave. Existing launchd behavior already involves GUI login, throttle/restart constraints, supervised leader verification, and detached/LaunchAgent lock conflicts (`docs/usage.md:485`, `docs/usage.md:498`, `docs/usage.md:500`, `docs/usage.md:198`). A notifier does not need a second service-management lifecycle to satisfy laptop delivery.

Channels:

- `auto`: when existing `notifications.herdr` is enabled and a usable laptop socket exists, call Herdr `notification.show`; otherwise use macOS Notification Center. Resolve the socket from the laptop runtime, never from controller state (`src/herdr.rs:98`, `src/herdr.rs:619`, `src/config.rs:89`).
- `macos`: invoke `/usr/bin/osascript` through `ProcessRunner` with a fixed AppleScript `on run argv` handler and title/body as separate arguments. No shell or interpolated AppleScript text. Use a 2 s process budget, bounded captured output, and stable failure codes. No new notification dependency; existing process policies and Herdr notifier budget provide the pattern (`src/herdr_notify.rs:20`, `src/controller/mod.rs:90`).
- `herdr`: explicit socket-only delivery; missing socket is a clear channel error. `both` deliberately attempts both channels. Automatic mode attempts a macOS fallback only when Herdr failed, never when it succeeded.

Messages use task/run ID abbreviations and fixed outcome text, e.g. “Task 0e7b7f91 needs input.” No task title, questions, paths, summaries or secrets. Herdr's current richer formatter is not reused wholesale (`src/herdr_notify.rs:33`). Existing runner notification is sent on the runner host (`src/turn_runner.rs:513`, `src/lib.rs:1640`); the new consumer resolves and delivers on the laptop. Existing per-host behavior remains compatible.

Notify about a reconciled latest turn that is quiescent: done, needs_input, blocked, unknown, failed, cancelled, timed_out or lost; and abandoned tasks with no terminal turn. A needs_input result with a saved auto-continuation intent or newer turn is not a human-input banner. A run settlement can replace member-done banners from that reconciliation batch with one run summary; errors/needs_input remain visible. Normal turn start, queue, admission, drain and worker availability events do not produce banners.

Do not notify directly on `turn.finished`: it can be followed by publication failure (`src/turn_runner.rs:1598`), continuation staging (`src/turn_runner.rs:2332`), or row cleanup (`src/turn_runner.rs:2346`). Hold a bounded candidate set until a fresh saved-state snapshot establishes eligibility; snapshots also discover eligible transitions whose event was lost. Older controllers use remote task/list/status polling at 2 s with the same dedup/eligibility rules, not journal emulation.

Rejected alternatives: mini-1 notification transport cannot reach the laptop's account socket; a new notifier crate is unnecessary; a default LaunchAgent adds operational work and a second lifetime owner before the foreground path is proven.

## Decision 9 — restart-safe dedup, explicit delivery limits, and coalescing

Use a new private laptop cache subtree `PathLayout::controller_cache_root()/events/<target-sha256>/`, with `notify.lock` and `notify.json`. Hash the configured SSH destination plus remote binary identity string for a stable target namespace; do not print them. Use rooted_fs 0700/0600 validation and atomic fsynced replacement. The operation-envelope cache already has independent per-operation locks (`src/controller/envelope.rs:127`), and the controller cache is explicitly transport-only (`src/paths.rs:64`). This file never contains a prompt or laptop task registry.

State contains a schema version, last consumed `(journal_id,seq)`, last complete reconciliation time, at most 4,096 decision fingerprints `(task_id,turn_id,outcome)` or `(run_id,settlement-signature)`, and at most 256 pending candidates. Derive the run signature from safe member IDs/latest turn IDs/outcomes using the saved membership flag, not from Git or prose. An epoch change cannot carry a numeric cursor forward. Persist decision fingerprints across an epoch change to suppress duplicate alerts for unchanged current state. A corrupt cache fails closed with a stable diagnostic; re-baseline current state without replaying historical banners.

Delivery ordering is deliberately at-most-once attempts: reconcile, form/coalesce decisions, atomically save the consumed cursor, pending candidates and decided fingerprints, then attempt channels. A crash after the save and before delivery can lose a banner. Saving only after OS delivery instead risks duplicate banners; neither osascript nor the existing Herdr call provides a transaction with this cache (`src/herdr_notify.rs:99`). Do not claim exactly-once delivery. `both` is one decision with two bounded attempts. Report channel failure without replaying the decision after restart.

To keep bounded dedup from replaying old history, unchanged rows during anti-entropy are not new decisions. On bootstrap/reset/expired cursor, baseline the current projection and consider only current attention or newly observed settlement within that reconciliation, never every historical completed turn. Cache ring eviction does not turn a completed historical row into a new candidate. If candidates exceed 256, retain current attention plus one overflow-summary marker instead of dropping unbounded events or issuing hundreds of banners.

Coalesce into one current-state summary when the laptop was disconnected more than 60 s, a snapshot reset/retention repair occurred, or more than five eligible decisions accumulate in one batch. Counts are of current reconciled eligible states, not blindly of historical events. Initial startup without a saved cursor can show one current-attention summary; it does not announce all historic done turns. `--quiet` suppresses all channel calls and sounds while still reconciling, saving cursors/fingerprints, and printing safe summaries. Leaving quiet mode does not replay suppressed banners.

Rejected alternatives: a cursor alone cannot deduplicate snapshot-derived decisions; an unbounded event-ID set grows forever; saving after notification trades crashes for duplicate alerts; replaying every retained completion after downtime produces the backlog the brief rejects.

## Decision 10 — local controller-viewer SSE with bounded fan-out

Add `GET /api/v1/events` only when a controller-viewer journal source is injected. Local/laptop dashboard mode without that source returns 404; the browser falls back to polling. Existing controller-viewer execution serves the local controller store, while the laptop launches only a tunnel (`src/lib.rs:819`, `src/lib.rs:834`, `src/lib.rs:845`). Use one local journal tailer per viewer process; no `events.read` RPC and no SSH in the tailer. Multiple browser tabs share its bounded broadcast queue.

Use a new `bind_with_events` entry point and launcher builder option, leaving the existing `DashboardHttpState` field layout and `bind` construction usable by old fixtures (`src/dashboard/web.rs:58`, `src/dashboard/web.rs:73`, `src/dashboard/command.rs:127`). Add `tokio-stream = { version = "0.1", features = ["sync"] }` for the bounded SSE adapter in the backend task; notification channels add no dependency. Axum and Tokio sync/time/runtime are already dependencies (`Cargo.toml:14`, `Cargo.toml:24`).

SSE contracts:

- Cursor encoding `<journal-uuid>:<decimal-seq>` in `id:`. Accept validated `after` query or `Last-Event-ID`; reject conflicting values. An initial connection without a cursor receives `snapshot_required: bootstrap` and a `ready` control message after establishing a current baseline. Reset/expired/ahead conditions mirror RPC.
- Named messages `controller.event` contain the same safe event envelope; `snapshot_required` carries a safe reason/current baseline; `ready` identifies the stream journal; `snapshot.ready` carries the viewer cache revision; `heartbeat` has an empty safe payload. Only durable events use replay cursor IDs. Control messages never consume or invent journal sequences.
- Recheck local journal every 200 ms using an injectable runtime. Broadcast capacity 256 events, maximum eight concurrent streams. A slow subscriber receives a repair control message if possible and is closed; clients repair on any disconnect. No blocking journal writer on a browser, unbounded channel, or per-tab controller polling.
- `Content-Type: text/event-stream`, `Cache-Control: no-store`, keepalive comment and named `heartbeat` every 10 s. Comments keep transport alive; the named message lets native EventSource expose activity to JavaScript. Client watchdog is 30 s without stream activity. Request cancellation/viewer heartbeat loss/shutdown cancels tailing and streams before HTTP graceful shutdown waits; existing shutdown already stops collection (`src/dashboard/web.rs:130`).

Preserve loopback binding (`src/dashboard/web.rs:83`) and exact allowed Host middleware (`src/dashboard/web.rs:205`). Reject a present foreign Origin on the SSE GET; allow absent Origin for native same-origin EventSource, and reject `Sec-Fetch-Site: cross-site` when present. No wildcard CORS. CSP stays `connect-src 'self'` (`src/dashboard/web.rs:39`). Existing JSON mutation headers/Origin checks and expected-generation body fences remain unchanged (`src/dashboard/web.rs:451`, `src/dashboard/web.rs:541`, `ui/src/views/TaskDetail.tsx:45`).

An event by itself cannot guarantee a fresh `/api/v1/snapshot`: `read_snapshot` returns the last completed cache (`src/dashboard/service.rs:400`). On lifecycle hints and bootstrap/reset, request a debounced local task/run/queue projection refresh and publish `snapshot.ready` after the new cache is visible. Reuse the saved-record projector, preserve cached worker metrics/freshness, and do not call remote per-task status on this fast path (`src/dashboard/task.rs:167`, `src/dashboard/task.rs:223`, `src/task_view.rs:334`). Give local projection publication a generation fence so an older slow full collector cannot overwrite it; when that full collector completes, retain the newer local projection while merging eligible remote-worker results. Emit `snapshot.ready` for successful full-cache publications too, without journal sequence IDs. Periodic full collection remains responsible for remote worker facts; its 15/20 s deadlines cannot be the event refresh path (`src/dashboard/service.rs:32`, `src/dashboard/service.rs:503`).

Rejected alternatives: browser-to-controller RPC adds SSH requests despite a local viewer; unbounded fan-out lets a slow tab hold memory indefinitely; refetching a stale cache only once loses the visible update; probing workers on every event makes lifecycle updates depend on offline peers.

## Decision 11 — refetch authoritative UI resources and keep log cursors separate

Create one shared browser EventSource client at App root (`ui/src/App.tsx:67`), with injectable source/timer factories. Parse safe envelopes, track string cursors, and publish invalidations; do not mutate task/run state from event payloads. Unknown well-formed kinds/versions invalidate the whole view. Malformed data triggers repair/fallback, never a partial local state mutation.

Debounce lifecycle invalidations for 100 ms. Task/turn/continuation/close hints refetch the snapshot, affected selected task detail, and that task's attention questions; run/DAG hints refetch run/task views; queue/worker/drain hints refetch the snapshot. `snapshot.ready` triggers another snapshot read after server cache publication. A selected task's new turn/generation must refresh questions even if the set of needs-input task IDs is unchanged (`ui/src/hooks/useAttentionQuestions.ts:5`). Preserve the six-request question concurrency cap and abort stale requests.

While SSE is healthy, snapshot/detail polling uses a 15 s anti-entropy interval; events cause immediate refetches. On 404, disconnect, parse error, reset repair, or watchdog expiry, resume today's 2 s snapshot/detail polling and reconnect with bounded backoff. The shared client closes an errored native EventSource and constructs a fresh one using its latest validated cursor; it does not leave an old `after` query to conflict with a newer native `Last-Event-ID` during automatic reconnect. Keep the last good snapshot on network error, one fetch per resource, cancellation/generation checks, and task input drafts/mutation revision guards (`ui/src/hooks/useSnapshot.ts:20`, `ui/src/views/TaskDetail.tsx:128`). Visibility/reconnect triggers a fresh snapshot. SSE is an optimization, not a new UI dependency for correctness.

Lifecycle events do not announce log bytes. Keep the existing 1 s live log polling, exact task/turn/stream cursor identity, UTF-8 byte cursor, completed current-end drain, and stale-response protection (`ui/src/hooks/useTurnLog.ts:5`, `ui/src/hooks/useTurnLog.ts:15`, `ui/src/hooks/useTurnLog.ts:41`, `ui/src/hooks/useTurnLog.ts:90`). A terminal hint can request an extra bounded read; it cannot prove final EOF or truncate delayed trailing bytes.

Build integration is explicit. `npm run build` is `tsc -b && vite build` (`ui/package.json:8`), Vite empties and writes stable-named assets in `src/dashboard/static/app` (`ui/vite.config.ts:19`), and Rust embeds those files with `include_str!/include_bytes!` (`src/dashboard/web.rs:41`, `src/dashboard/web.rs:267`). `build.rs` generates build identity, not UI assets (`build.rs:15`). Only the final integration task rebuilds/commits the generated directory once; UI/backend agents modify source and tests only. Existing CI checks source tests/lint and diffs a fresh temporary asset build (`.github/workflows/ci.yml:24`); keep that parity check, including fonts.

Rejected alternatives: replacing log polling with lifecycle events invents a final-byte protocol; using only task-ID-set changes leaves new questions stale; letting each parallel agent rebuild assets creates generated-file conflicts and stale embedded UI.

## Decision 12 — deterministic tests and staged acceptance

Follow targeted `CARGO_BUILD_JOBS=4` runs and injected clocks/hooks from `docs/testing.md:26` and `docs/testing.md:90`. Reuse the long-poll test clock/hook pattern (`tests/controller/controller_read_routes.rs:329`) and rooted filesystem fault/concurrency seams (`src/client_state.rs:4935`, `src/client_state.rs:5026`). No sleeps or assertions on real elapsed timing. The implementation plan assigns each contract, fault boundary, and integration test to one owner.

Required invariants: no event before successful state durability; losing CAS/no-op produces no hint; cross-process sequences/epochs/retention are consistent; uncertain journal data fails closed without failing task mutation; valid-cursor crash gaps converge through periodic snapshots; task waits preserve timeout/quiescence/exit semantics; old peers never receive unknown commands/fields; notification restarts coalesce/dedup; SSE cannot bypass Host/Origin or stall shutdown; UI preserves drafts, questions, and byte-cursor logs; compiled embedded assets match source.

Deploy only the fully integrated first wave, never an intermediate producer-free feature-advertising build. Validate laptop-new/controller-old, laptop-old/controller-new, and new RPC with an old leader/runner. Existing same-ID mutation retry is unchanged (`tests/controller/controller_retry.rs:463`). Live pool acceptance is a checklist for the owner/integrator after approval, not authorization for this documentation track to contact hosts.

Rejected alternatives: real sleeps make long-poll/crash tests flaky; claiming event completeness from same-version happy paths misses mixed-version writers; asset rebuilds before source integration ship stale frontend behavior.

## Decision 13 — optional persistent transport, design sketch only

A future controller-only Unix socket may use the existing controller authority and laptop SSH identity. Put controller and laptop-forward sockets in validated private 0700 directories, create sockets 0600, verify owner/type/inode, and unlink only the owned stale socket inside the private directory. No token database or socket on every mini. The laptop forward lives in transport cache, not the task state root (`src/paths.rs:64`).

Forward shape: `ssh -N -L <laptop.sock>:<controller.sock>` with `StreamLocalBindUnlink=yes`, restrictive umask/`StreamLocalBindMask`, BatchMode, no agent forwarding, ExitOnForwardFailure, keepalives, and private control-path handling. Existing dashboard forwarding separates forwards from multiplexed per-request SSH and falls back from broken masters (`src/transport.rs:904`, `src/transport.rs:938`, `src/transport.rs:943`, `src/transport.rs:962`). Reuse that lifecycle; do not weaken SSH host-key validation. Discover/validate any remote socket path without accepting a client-selected arbitrary server file; check platform pathname limits before binding.

Before requests on each connected channel, validate a handshake containing protocol 7, controller client identity, configured route binding, server process identity, fresh random server-generation UUID, journal UUID, and feature list. The server generation changes on each socket-service start, unlike the persistent journal UUID. Current provisioning supplies account home/username/uid (`src/protocol.rs:503`, `src/controller/init.rs:654`), while ControllerConfig stores only enabled/SSH/binary (`src/config.rs:68`); there is no existing pinned controller-client UUID. A future feature-gated identity read over authenticated stdio SSH must bootstrap a private laptop transport-cache pin of account plus controller client UUID, and obtain the expected socket generation before channel requests. Compare that bootstrap with the socket handshake and current ProcessIdentity (`src/controller/leader.rs:27`). An unexpected account/client identity fails closed rather than replacing the pin silently; an unsupported bootstrap retains per-request SSH. Recheck generation on reconnect and associate every response with its request ID/digest.

The current stdio decoder requires exactly one frame and EOF (`src/controller/protocol.rs:90`). A persistent service needs a separate bounded multi-frame decoder, handshake and cancellation/backpressure contract; do not relax the old stdio route. On channel loss or unsupported handshake, use current per-request SSH. Retried mutations carry the same existing durable request envelope/ID; reconnect does not create a new logical operation.

Build this only if measurement after phases 1–2 shows SSH setup overhead or process count still causes a user-visible problem. Multiplexing already uses `ControlPersist=60` (`src/transport.rs:943`), and 15 s long-poll amortizes requests. Avoid committing to a socket daemon solely for the unverified 40 ms figure. No phase-3 files, migration, tests or deployment steps are in the first-wave plan.

Rejected alternatives: sockets on every worker change the out-of-scope ownership model; tokens duplicate existing same-account/SSH authority; persistent framing without identity/generation checks can silently reuse the wrong or restarted controller.

## Open questions for owner approval

These do not block authoring this documentation track. The stated defaults are the safest first-wave choices; approval is required before code.

1. **Repair endpoint:** approve additive `events.snapshot` under `controller.events` rather than changing strict existing DTOs. It covers queue/drain/worker/run state and bounded paging; existing read routes do not cover that combination (`src/controller/read.rs:91`).
2. **Delivery tradeoff:** approve at-most-once notification attempts, including the save-before-display crash window, and foreground hosting. Exactly-once desktop delivery is unavailable with the current channels (`src/herdr_notify.rs:99`); a laptop LaunchAgent is deferred.
3. **Notification policy:** approve all terminal outcomes, suppression during auto-continuation, one run completion summary, quiet consumption, initial current-attention summary, and the 60 s / five-decision coalescing thresholds. Outcome distinctions and continuation intent already exist (`src/task.rs:337`, `src/task_client.rs:139`).
4. **Retention and cost:** approve 16 MiB/64 segments, 1 KiB records, 32-event batches, 50 ms journal-lock admission, 15 s anti-entropy, and eight SSE clients. Measure append fsync cost and historical snapshot sweep cost before tuning. Fsync and listing are already synchronous (`src/rooted_fs.rs:1913`, `src/client_state.rs:3216`); the defaults are not performance claims.
5. **Worker meaning:** approve observed ready-state transitions without new periodic idle probes. An idle mini changing health produces no event until an existing probe runs (`src/admission.rs:228`). Faster idle alerts require separately approved probe scheduling.
6. **Run meaning:** approve reversible current settlement, rather than a durable sealed run. Existing records have no finished flag, and waits account for DAG membership and busy state (`src/task.rs:2245`, `src/task_client.rs:4607`). Snapshot repair recovers current settlement, not every missed historical settlement edge.
7. **N-1 observability:** approve correctness through periodic snapshots when old leader/runner binaries miss emissions, and reduced old-controller CLI snapshots with ordered replay unsupported. Serving-binary features do not prove producer uniformity (`src/controller/health_read.rs:50`).
8. **Persistent transport:** leave phase 3 uncommitted until a measured bottleneck and separate identity-bootstrap/handshake review justify it. Existing config has no controller-client identity pin (`src/config.rs:68`); multiplexing and strict single-frame transport already exist (`src/transport.rs:943`, `src/controller/protocol.rs:90`).
