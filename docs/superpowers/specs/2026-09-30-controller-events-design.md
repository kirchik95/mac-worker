# Controller Events, Laptop Notifications, and Live Dashboard

Date: 2026-09-30

Status: Round 2 proposal; incorporates the orchestrator's scope and compatibility decisions. Owner approval precedes implementation.

Code baseline: `37915a9c21c20dd022d2d140156cd234a267ee14`

Implementation plan: [2026-09-30-controller-events.md](../plans/2026-09-30-controller-events.md)

All existing-code references are `path:line` at that baseline. Event modules, selector DTOs, traits and methods described below are proposed additions. This document does not claim that their durability or latency has already been tested.

## Purpose and boundaries

The first wave provides lifecycle hints, promptly confirmed laptop task notifications, and a live controller dashboard. Task, queue, run, DAG, admission and drain records keep their current authority. The journal is a lossy wake-up mechanism: separate state/event transactions, old producers, bounded publisher drops and crashes can leave gaps. Periodic state repair is mandatory even with a valid cursor.

The first wave leaves the authoritative 100 ms `task wait` loop intact (`src/controller/lifecycle.rs:164`, `src/client_state/deadline.rs:95`). It has no run settlement, run banners, semantic worker availability, universal snapshot RPC, or replay inspection CLI. The Deferred section records those cuts. Phase 3 remains a transport sketch; phone/Tailscale access and per-mini daemons remain outside this design. The brief's approximately 40 ms SSH cost is context, not a measurement made here.

## Decision 1 — use the existing boundaries

Existing mechanism: additive controller features (`src/features.rs:8`), verified read reply identity (`src/controller/read.rs:69`), injected long-poll timing (`src/controller/read.rs:492`, `src/transfer.rs:142`), private rooted append/replacement (`src/rooted_fs.rs:1752`, `src/rooted_fs.rs:1805`), transport-only laptop cache (`src/paths.rs:64`), Herdr notification transport (`src/herdr.rs:619`), and the viewer's completed-snapshot cache (`src/dashboard/service.rs:401`). Reuse these.

Not found — checked: routing (`src/controller/execute.rs:549`), feature registry (`src/features.rs:8`), task publication (`src/client_state.rs:3131`), status event annotations (`src/controller/read.rs:123`, `src/controller/read.rs:138`), runner acceptance logs (`src/turn_runner.rs:1370`), and dashboard routes (`src/dashboard/web.rs:156`). None is a shared durable lifecycle journal or browser SSE source.

Keep protocol 7 (`src/protocol.rs:5`). Add `controller.events` only to the controller registry; host capabilities remain separate (`src/features.rs:7`). Advertise after integrated producer/client/viewer wiring. Discovery describes the serving RPC binary, not the live leader (`src/controller/health_read.rs:50`), and is an optimisation. Safety comes from the selector dispatch in Decision 5, including rollback between discovery and execution.

Rejected alternatives: leader-only emission misses detached runners and one-shot RPCs; replacing state records with events changes recovery; forwarding the controller account's notifier does not reach the laptop account.

## Decision 2 — typed, bounded and title-free events

An internal `NewEvent` enum selects safe fields. A separate tolerant envelope carries string kinds and data so newer kinds do not break older event consumers:

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

A cursor is `{journal_id, seq}`. Sequence is canonical decimal u64 text, with no leading zeros except `"0"`; JavaScript never converts it to Number. Ordering applies only within one journal UUID. Wall time is diagnostic, never an ordering or timeout authority. The UUID survives leader restarts; an explicitly replaced journal gets a different UUID.

Each encoded event, including the JSONL newline, is at most 1,024 UTF-8 bytes. UUIDs and validated worker names are allowed (`src/client_state.rs:5690`, `src/job.rs:6034`). Exclude titles, prompts, questions, summaries, paths, project/worktree labels, run names, branches, OIDs, SSH destinations, environment values, process tokens and failure prose. `TaskOutcome::Failed.reason` is free text (`src/task.rs:342`); emit an allowlisted stable code or `TURN_FAILED`. Never serialize whole task/queue/observation records; observations contain route and binary paths (`src/job.rs:2002`). Redaction is defense in depth, not permission to put prose in the journal (`src/redaction.rs:46`, `src/redaction.rs:171`).

| Kind | Payload and publication meaning | Existing-code basis |
| --- | --- | --- |
| `task.created`, `task.changed`, `task.removed` | Task ID, optional run/latest-turn ID, state and stable code. Removed is a durable rollback deletion. | `src/client_state.rs:2908`, `src/client_state.rs:3131`, `src/client_state.rs:3290` |
| `turn.started` | Task/turn/run IDs and worker name, after an accepted response's status is saved. Prepared Active status is insufficient. | Accepted saves: `src/turn_runner.rs:1390`, `src/turn_runner.rs:1419`; pre-acceptance save: `src/turn_runner.rs:1334` |
| `turn.finished` | First saved terminal outcome for the turn, including outcome and stable code. It does not prove retirement/import finished. | `src/turn_runner.rs:1501`, `src/turn_runner.rs:1514`, `src/task.rs:337` |
| `turn.outcome_changed` | Corrected saved terminal outcome/code for the same turn. | Publication failure: `src/turn_runner.rs:1599`; undrainable finalization: `src/turn_runner.rs:2208`, `src/turn_runner.rs:2242` |
| `task.auto_continue_scheduled` | Task/run IDs, previous and planned next turn IDs, after saving the intent. | `src/task_client.rs:139`, `src/turn_runner.rs:2332` |
| `task.closed`, `task.abandoned` | Task/run/latest-turn IDs, saved state and stable code. A cancellation ACK alone is neither. | `src/task_client.rs:2732`, `src/task_client.rs:4397`, `src/turn_runner.rs:631` |
| `queue.changed` | Optional affected job/turn ID, kind/state/code; a global invalidation when too many entries changed. No owner or affinity keys. | `src/client_state.rs:5385`, `src/client_state.rs:2149`, `src/client_state.rs:2208` |
| `run.changed` | Run ID and optional affected task ID; plain invalidation for membership, reservation and DAG writes. | `src/client_state.rs:4178`, `src/client_state.rs:4771`, `src/client_state.rs:4896` |
| `dag.child_admitted` | Run/task/turn IDs, only after both membership and Submitted are durable; includes recovery MarkSubmitted. No user node label. | `src/client_state.rs:3747`, `src/client_state.rs:3825`, `src/client_state.rs:3838` |
| `worker.changed` | Worker name, recorded ready/unknown fact, observation time and stable code from a committed admission observation or its invalidation. | `src/admission.rs:272`, `src/client_state.rs:2338`, `src/client_state.rs:2382` |
| `controller.drained`, `controller.undrained` | Saved boolean after a changed drain write. | `src/controller/drain.rs:61`, `src/controller/drain.rs:75` |

All eight existing terminal outcomes are represented: done, needs_input, blocked, unknown, failed, cancelled, timed_out and lost (`src/task.rs:337`). Failed CAS and semantic no-op writes emit nothing. A batch may contain generic and semantic hints. Duplicates are possible; consumers confirm saved state. Sequence orders publication, not cross-domain causality or host execution. A recovered acceptance hint can follow a terminal hint.

Rejected alternatives: a closed wire enum breaks additive evolution; raw log export includes private data and repeated acceptance records (`src/turn_runner.rs:1392`, `src/turn_runner.rs:1425`); prepared Active status is not host acceptance (`src/task_client.rs:3839`).

## Decision 3 — attach by initialized host journal, and publish outside fences

Only a controller leader holding the leader lock initializes `controller_state_root()/events/` (`src/lib.rs:1421`). Other processes attach an optional sink only if that same host already has a safely bound, initialized journal. Config `controller.enabled` is a laptop routing choice, not an emission test: controller hosts use local execution config (`src/controller/provision.rs:350`). No RPC, viewer, runner or debugging read creates a journal. A laptop in local mode has no initialized journal and emits nothing. Bare `ClientStateStore::open` stays no-event (`src/client_state.rs:553`). An unsafe or unreadable event binding disables the optional sink without failing authoritative work.

| Process | Real entry point | Wiring rule |
| --- | --- | --- |
| Controller leader | `src/lib.rs:1423`; tick at `src/lib.rs:1477` | Initialize after leadership; attach to leader store and recovery handlers. |
| One-shot `host controller-rpc` | `src/lib.rs:1528`; stores at `src/controller/execute.rs:564`, `src/controller/execute.rs:568`, `src/controller/execute.rs:579` | Attach existing journal for lifecycle/mutation writes. Ordinary task.status/list do not persist remote state. Selector task reads use a separate read-only state opener. |
| Detached runner child and local command client | Shared fresh store at `src/lib.rs:1625`; child arm at `src/lib.rs:1646` | Check initialized host journal here. A sink on the parent's `open_until` cannot cross spawn (`src/turn_runner.rs:181`, `src/turn_runner.rs:186`). Reopens preserve/re-attach within the same binding; no hidden runner flag. |
| Controller viewer and local dashboard | Shared store at `src/lib.rs:1124`; viewer branch at `src/lib.rs:834` | Same existing-journal attachment rule. Inject SSE only in controller-viewer mode; laptop-local dashboard remains polling. |
| Direct host helpers | `src/lib.rs:326`, `src/lib.rs:350`, `src/lib.rs:610`, `src/lib.rs:1308` | Attach only an initialized host journal. Shared queue/admission hooks cover legacy job effects. |
| Export and host retention GC | `src/controller/stream_rpc.rs:383`, `src/gc.rs:1008` | Export is read-only. GC's worker TaskStore close is a separate authority; see exclusions below. |

Use a nonblocking `EventSink::try_publish(EventBatch)`. Its production implementation only enqueues into a bounded process-local publisher; it never opens files, flocks, sleeps, fsyncs or joins. Collect selected before/after hints after successful durability under existing locks. A nesting-aware, thread-bound `DeferredHints` scope buffers at most 32 events and releases the batch only after all StateLock, QueueLock, drain permit/lock and runner-log fences enclosing those writes have been dropped. Public mutators start scopes before acquiring locks; runner/drain outer scopes cover fences around nested store calls. Error unwinding also releases already-durable hints after guard destruction, including later derived-index cleanup failure. Overflow coalesces generic invalidations when possible, otherwise drops with a stable diagnostic; repair supplies current state.

The publisher has one worker and capacity 128 batches (at most 4 MiB of event bytes) per process. Enqueue is try-only. Full, unavailable or stopping publishers drop hints; state success is unchanged. Journal lock admission is at most 50 ms and occurs only on that worker. A one-shot process may give the worker at most 50 ms of exit grace outside all fences, then exit without joining stalled disk I/O. Long-lived processes stop admission on shutdown; they do not wait indefinitely for a blocked fsync. This is bounded best effort, not a wall-clock disk SLA. Pending/uncommitted work remains recoverable by another journal process.

Do not shorten or release the runner-log finalization fence to make events faster (`src/runner_log.rs:156`, `src/runner_log.rs:195`). When that fence spans a turn, buffered `turn.started` can be delayed until it is released. Submission's task/queue hints and the existing collector expose Active state in the meantime. Finished notification eligibility is checked after retirement, so a log-fence release is part of the existing completion path, not a new journal-induced delay. This limitation is explicit; events are not an execution trace.

| Durable family | Writers | Capture point; batch release is always outside enclosing fences |
| --- | --- | --- |
| Task create, mutate, CAS and central replacement (`src/client_state.rs:2908`, `src/client_state.rs:2989`, `src/client_state.rs:3044`, `src/client_state.rs:3131`) | RPC, leader recovery, runner, viewer/direct helpers | Capture after successful authoritative publication at `src/client_state.rs:3204`, before active-index retirement at `src/client_state.rs:3211`. Never emit from before_final_sync. Central diff supplies state/outcome/runner/result-proof hints. |
| Acceptance, terminal result and finalization (`src/turn_runner.rs:1390`, `src/turn_runner.rs:1419`, `src/turn_runner.rs:1860`, `src/turn_runner.rs:2208`) | Runner; leader/lifecycle reconcile | Acceptance helper requires saved accepted-status proof. Central terminal diff covers first outcome and both publication/finalization corrections. Fetched OIDs become only a proof boolean in task reads. |
| Continuation intent, execution and rollback (`src/task_client.rs:139`, `src/task_client.rs:3681`, `src/task_client.rs:3704`, `src/task_client.rs:3723`) | Runner retirement, leader/lifecycle recovery | Saved intent creation schedules; clear/rollback invalidates. Retried execution alone is not another schedule. |
| Close intent, close/discard, cancellation and failures (`src/task_client.rs:2783`, `src/task_client.rs:2824`, `src/task_client.rs:4480`, `src/task_client.rs:4506`, `src/task_client.rs:4539`, `src/turn_runner.rs:1962`, `src/turn_runner.rs:2092`, `src/task_client.rs:5987`) | RPC/direct/viewer; runner; leader | Central task and queue diffs. Absence is not proof of death (`src/client_state.rs:141`). |
| Submission rollback and turn-tree removal (`src/client_state.rs:3290`, `src/client_state.rs:3318`) | Submit/handoff helpers | Task tombstone after durable deletion; turn-tree removal is an invalidation. A lost tombstone is repaired by completed-sweep absence. |
| All queue paths, including direct spawn completion and dispatch bypass | Runner/helper, RPC, leader | Capture centrally after queue directory sync (`src/client_state.rs:5408`). `update_queue` alone misses `src/client_state.rs:1188`, `src/client_state.rs:1212`, `src/client_state/runner_dispatch.rs:102`. Over 32 changed entries becomes one global queue hint. |
| Affinity publish/remove (`src/client_state.rs:2149`, `src/client_state.rs:2208`) | Admission/runner/helpers | Global queue invalidation after durable change, without affinity keys. |
| Run/DAG creation, recovery, membership and reservations (`src/client_state.rs:3501`, `src/client_state.rs:3526`, `src/client_state.rs:4178`, `src/client_state.rs:4208`, `src/client_state.rs:4255`, `src/client_state.rs:4896`) | RPC batch, leader, runner/DAG helper | Plain run.changed; no before/after settlement scans. Partial pending/DAG creation is not child admission (`src/client_state.rs:3564`, `src/client_state.rs:3587`). |
| DAG claim/bind/block/submit/recovery (`src/client_state.rs:3683`, `src/client_state.rs:3747`, `src/client_state.rs:3807`, `src/client_state.rs:3843`, `src/client_state.rs:4771`, `src/client_state.rs:4843`) | Leader, runner, lifecycle/DAG helper | run.changed after publication; child_admitted after both durable membership and Submitted, including MarkSubmitted recovery. |
| Admission observation commit/invalidate (`src/client_state.rs:2338`, `src/client_state.rs:2359`, `src/client_state.rs:2382`, `src/client_state.rs:5611`) | Existing admission pipeline (`src/admission.rs:228`) | Only committed winners produce worker.changed. CAS losers and memory-only probes produce none. |
| Drain (`src/controller/drain.rs:61`, `src/controller/control.rs:70`) | RPC/direct helper | Capture changed boolean after durability, drop exclusive drain lock, then enqueue. Preserve launch permit fencing (`src/controller/drain.rs:95`). |

Excluded authorities and signals:

- Leader/health replacements are diagnostics, not journal events (`src/controller/leader.rs:77`, `src/controller/health.rs:342`, `src/lib.rs:1474`). Failover/dead ticks remain visible through health polling; an event heartbeat does not prove leader health. Request phase/receipt bookkeeping is also private (`src/controller/store.rs:807`, `src/controller/store.rs:1111`).
- Host `TaskStore::close_for_retention` writes Closed (`src/task_store.rs:1095`, `src/gc.rs:1008`). It becomes a controller task hint only when reconciliation copies it (`src/task_client.rs:5460`, `src/task_client.rs:5942`). Ordinary status/list are non-persisting reads (`src/task_client.rs:2209`, `src/task_client.rs:3117`); lifecycle reconcile uses a different route (`src/controller/execute.rs:568`, `src/controller/lifecycle.rs:259`). There is no new host-to-controller GC feed.
- Admission readiness expires after 2 s without a write (`src/client_state.rs:88`); TTL expiry is not an event. Dashboard facts/probes are a different source (`src/probe.rs:450`, `src/dashboard/source.rs:117`). Time-based freshness stays in the local projector and regular collection.
- Runner-log completion/checkpoints can precede a status rewrite and wake log followers/DAG gates (`src/task_client.rs:2312`, `src/task_client.rs:5216`). They do not generate lifecycle events. Keep log polling and authoritative reconciliation.
- Worker Herdr metadata rewrites preserve state/outcome/timestamp (`src/task_store.rs:1399`, `src/task_store.rs:1430`); semantic hooks must not false-fire on bytes. Job Running is a separate model (`src/supervisor.rs:1949`). Private prompts, indexes and bindings remain private/derived (`src/client_state.rs:4550`, `src/client_state/active_tasks.rs:27`).

Rejected alternatives: config-based attachment misclassifies both detached controller runners and laptop-local commands; synchronous append inside a task/drain/log fence lets optional disk I/O block unrelated authoritative work; hooks only after command success lose committed changes followed by cleanup failure.

## Decision 4 — append journal, pinned manifest and bounded crash residue

Choose append option (a). A global revision/diff feed still requires every writer plus per-record versions/tombstones. Saved records have no shared revision and strict wire decoders (`src/task.rs:2174`, `src/task.rs:2428`). A sibling journal avoids state schema migration and uses existing rooted append.

Location is `PathLayout::controller_state_root()/events/`, private 0700 directories and 0600 regular files. It is outside the closed client-state root (`src/paths.rs:54`, `src/client_state.rs:6896`); controller request enumeration selects req files (`src/controller/store.rs:1164`). Client inputs never select filesystem names.

Layout: stable `journal.lock`; manifest and pending roles; numeric `segment-<first-seq>.jsonl`; private fixed staging roles. Manifest has schema, journal UUID, oldest/head sequence and a table for **every** retained segment: name, first/last seq, committed length, device/inode/owner/type/mode identity and sealed flag. Empty active segment uses an explicit empty range. Compare every pinned binding at open, recovery and retirement; compare each segment used by a read again around its read. Sealed size must equal committed length exactly. Active extra bytes are permitted only when the pending transaction proves their exact content/range. An owned replacement before first open is detected, including sealed segments. Identity pinning is not protection against the same uid rewriting all journal metadata/content in place; no cryptographic tamper-proof claim is made (`src/rooted_fs.rs:780`, `src/rooted_fs.rs:1050`).

| Bound | First-wave value |
| --- | --- |
| Event / append batch | 1,024 bytes including newline; 32 events / 32 KiB |
| Retained segments | 64 × 256 KiB = 16 MiB |
| Recovery segment | At most one additional 256 KiB segment |
| Manifest / pending / each metadata stage | At most 64 KiB each |
| Recovery evidence | At most 512 KiB and 32 files across the journal's rooted cleanup namespaces |
| Entire journal, including all staging/displaced/evidence | At most 18 MiB and 128 regular files; validation precedes every allocation |
| Publisher / lock admission | 128 batches per process; 50 ms journal admission outside authoritative fences |
| Read check / ESTALE attempts | 200 ms injected check interval; three retries within original deadline and retained root binding |

Use named staging support for journal operations rather than unconstrained random leftovers. Extend the rooted replacement helper with an explicit validated staging role; existing no-replace internals already accept a staging name (`src/rooted_fs.rs:2022`). Roles are `manifest.stage`, `pending.stage`, `segment.stage`, and one retirement role, under journal-owned private directories. At most one operation per role exists under journal EX lock. The manifest/pending/role records identify journal UUID, role, transaction ID, expected generation/identity and bounded content digest. Staging and displaced files are associated through those records plus actual binding/content, never deleted because a filename matches a prefix. Rooted cleanup evidence and any displaced file count toward the same hard budget, including journal-owned evidence in adjacent rooted namespaces; unrelated controller-operation evidence is distinguished by parent/target identity (`src/rooted_fs.rs:1257`, `src/rooted_fs.rs:2855`).

Before a fresh allocation, finish verified cleanup/recovery for all roles and their cleanup intents. A partially written uncommitted stage may be discarded only with durable creation/role identity evidence; otherwise fail unavailable without deleting it. The rooted adapter must retain that creation evidence before writing stage content. Reuse rooted cleanup intent machinery rather than inventing prefix cleanup (`src/rooted_fs.rs:2487`, `src/rooted_fs.rs:2622`). Unknown, unsafe, conflicting or over-budget residue makes the journal unavailable; it never grows further or resets itself. Initialization is leader-only and resumable with the same named/evidenced roles. The lock is stable and never exchanged.

Append transaction, wholly outside authoritative fences:

1. Acquire validated journal EX within admission budget. Recover existing pending/staging work before allocating sequences. Bind the current manifest and pinned segments.
2. Assign contiguous seq values, serialize the exact batch, and durably publish pending with previous manifest digest/head, target segment identities/offsets, exact batch bytes encoded as base64, and a bounded next-manifest delta plus expected digest. Do not copy the whole manifest into pending: 32 KiB batch base64 plus ≤4 KiB delta/evidence fits the 64 KiB pending cap. Measure final encoding before publication. Rotation can allocate only the one recovery segment. Pending becomes durable before any segment append.
3. Append the exact remaining bytes, verify bindings and fsync segment files. Recovery matches any partial tail against pending bytes; only a proven matching partial batch is completed. Unexplained bytes are not truncated.
4. Publish/fsync the next manifest. Its committed prefix is the sole visibility boundary. Readers never expose speculative tail bytes or pending head.
5. Remove verified pending/stages, sync directories, then retire only manifest-excluded segments with identity evidence. Cleanup precedes the next allocation. Retirement cannot remove a retained file.

Existing exact replacement does exchange, directory fsync, displaced removal and a second sync (`src/rooted_fs.rs:1892`, `src/rooted_fs.rs:1913`, `src/rooted_fs.rs:1914`, `src/rooted_fs.rs:1922`). New named-role support must preserve that discipline. The fault matrix is required for **each** pending/manifest/initialization/rotation/retirement role:

| Fault boundary | Recovery obligation |
| --- | --- |
| Creation evidence durable; stage content partial | Validate pinned creation evidence; discard only uncommitted stage; committed head unchanged. |
| Stage fsynced before exchange/no-replace publication | Validate staged role/content and previous generation; discard or resume the described transaction. No speculative seq visible. |
| Exchange complete before first directory fsync | Inspect both names/identities; accept only old/new pairing justified by transaction; re-sync before deciding committed generation. |
| First directory fsync complete before displaced removal | Keep committed new target, remove only proven displaced old file, sync cleanup. |
| Removal complete before final directory fsync | Resume rooted cleanup evidence and final sync idempotently. |
| Pending durable; partial/full segment append; manifest not committed | Complete exact pending bytes and publish manifest once; no double seq allocation. |
| Manifest committed; pending or retired segments still present | Serve only pinned committed prefix after recovery; clean evidence without re-appending. |

Ordinary deterministic tests exercise every distinct point. An ignored stress test repeats hundreds of kills/faults and asserts ≤18 MiB, ≤128 files, one seq per committed record and complete cleanup before allocation. This is an implementation acceptance requirement, not a proof supplied by documentation.

ESTALE is retryable: rooted append/read can report a normal replacement race (`src/rooted_fs.rs:1768`, `src/rooted_fs.rs:1066`). Recheck the retained root binding, reload the current transaction/manifest and retry up to three times without resetting the deadline or allocating a second sequence range, as task/health readers do (`src/task_store.rs:2154`, `src/controller/health.rs:327`). Persistent mismatch with a manifest-pinned segment, malformed committed JSON/order, unsafe binding or unexplained tail fails `CONTROLLER_EVENTS_UNAVAILABLE`; transient exhausted races return a retryable unavailable result. Neither case silently truncates, adopts a foreign root or creates a new epoch.

Readers hold journal SH for a bounded committed-prefix read. Pending/ambiguous metadata requires releasing SH and attempting EX recovery; an unavailable recovery is an unavailable read, not a healthy empty head. State reads never hold journal locks. The journal never calls state, drain, runner logs, Git, SSH or task reconciliation. Existing State→Queue and log/drain ownership stay intact (`src/client_state.rs:5201`, `src/runner_log.rs:156`, `src/controller/drain.rs:95`). A controlled fsync gate must show unrelated state and drain operations still proceed.

Rejected alternatives: pinning only the active inode misses sealed swaps; ignoring rooted replace/write residue invalidates retention bounds (`src/rooted_fs.rs:1874`, `src/rooted_fs.rs:2150`); treating first ESTALE as corruption confuses races with damage; an I/O timeout around synchronous fsync does not bound a mutation return.

## Decision 5 — safe read selectors, never a new top-level command

All three logical reads use existing command **`task.list`** with exactly one new body key, `controller_events`. Dispatch checks that selector before ordinary list handling and before any durable mutation route. A body containing both selectors/filters or an invalid selector is rejected in the read branch. There is no top-level `events.read`, `events.tasks` or `events.repair` request.

```json
{"controller_events":{"op":"read","after":{"journal_id":"614dc3be-668f-4922-bd31-b1d7a0056790","seq":"42"},"limit":128,"wait_ms":15000}}
```

```json
{"controller_events":{"op":"tasks","task_ids":["0e7b7f91-5a91-4dbd-bcc2-c33338890752"],"include_titles":true,"proof_after":null}}
```

```json
{"controller_events":{"op":"repair","after":null,"limit":64,"baseline_after":null}}
```

Verified N-1 safety: task.list is already dispatched to `serve_read_command` (`src/controller/execute.rs:563`), whose list handler rejects keys outside run/state/outcome/full before `client.list` (`src/controller/read.rs:450`). A selector request therefore cannot enter `ControllerStore::handle_with`, where a receipt is created before prepare (`src/controller/store.rs:523`, `src/controller/store.rs:528`). Health discovery uses this same safety pattern (`src/controller/health_read.rs:233`). The old read may open normal state; it creates no active controller receipt or req row. Discovery against a new fixture followed by execution against the baseline fixture must assert this for all three selectors.

Feature discovery lets consumers avoid unnecessary unsupported requests, but cached or raced discovery is safe. Reuse `ControllerReadReply<T>` unchanged: command remains task.list and request ID/digest verification still applies (`src/controller/read.rs:25`, `src/controller/read.rs:69`). New selector replies are tolerant on additive fields; selector requests validate their exact grammar and sizes. Final encoded frame, including envelope, must be below the existing 1 MiB limit (`src/controller/protocol.rs:11`).

Logical events.read arguments remain after/limit/wait_ms. Limit defaults 128 and clamps to 1..256; wait defaults 0, caps at 20,000 ms; laptop default is 15,000 ms. The monotonic 30 s RPC policy includes discovery, lock retries and reads (`src/controller/mod.rs:90`). Long-poll releases locks before sleeping, checks cancellation/deadline and uses an injected clock (`src/transfer.rs:142`). Result discriminator is `type: batch|snapshot_required`. Batch fields: schema_version, journal_id, oldest_seq, head_seq, next_after, events and has_more. Snapshot-required fields are reason and window, with window containing journal_id/oldest_seq/head_seq. All sequence fields are strings; next_after is last delivered, never an undelivered head. Empty timeout keeps after unchanged.

Missing/null cursor returns snapshot_required bootstrap. Other reasons are journal_changed, cursor_expired (`after < oldest-1`) and cursor_ahead (`after > head`). The control contains the current safe window/baseline, not invented events. Uninitialized/unsafe/corrupt journal returns a stable unavailable error. Only events.read requires a journal. Task/addressed and repair handlers open existing state independently, optionally and lazily read a baseline, and succeed with `baseline_after: null` when event directory/lock/manifest cannot open. They do not initialize the journal or fail solely on its binding. Optional baseline capture releases journal before opening/locking state.

Rejected alternatives: feature discovery cannot pin the next SSH executable (`src/controller/mod.rs:98`, `src/transfer.rs:130`); sending an unknown command first leaves durable receipts (`src/controller/execute.rs:577`); a required EventJournal argument in state handlers defeats repair when the event root is damaged.

## Decision 6 — addressed eligibility and resumable task-only repair

Use a new read-only `TaskEventReadStore::open_existing`. It anchors existing client-id/tasks/queue/turns, verifies owned bindings and takes the same authoritative directory flock for coherent facts (`src/client_state.rs:6595`). It never initializes state, boots indexes or enumerates affinity/observations. Ordinary `ClientStateStore::open` performs validation/creation work outside the requested task set (`src/client_state.rs:798`, `src/client_state.rs:814`, `src/client_state.rs:819`); it is not this handler's opener. Neither read calls TaskClient.wait_poll, Git, SSH, reconcile, process-liveness inspection or runner-log locking.

Addressed read accepts 1..16 distinct task UUIDs, `include_titles` (default false), and optional bounded proof continuation. It returns one row or explicit missing result per requested ID. A row is at most 2 KiB: IDs, saved state/latest turn/outcome/stable code, runner-present and close/continuation-intent flags, queue-dispatch fact, result-import proof boolean, `busy: bool|null`, `quiescent: bool|null`, and task fact digest. Titles, when requested by the laptop notifier, are redacted/control-escaped/truncated to 512 bytes using `RedactionBoundary`; they are display-only and never journal/cache fingerprints. Repair never includes titles. No member arrays, run aggregates or queue/worker rows.

Quiescence uses the existing task definition: Open/Closed/Abandoned/Lost plus no operator-busy reason (`src/task_client.rs:6093`, `src/task_client.rs:6349`, `src/client_state.rs:122`). Close intent, auto-continuation intent, Active state, a matching Dispatching row, or **any recorded runner identity**, even Dead, is busy. Do not infer absence from RunnerState::Dead (`src/client_state.rs:3360`). The result-import boolean is informational, not a new wait condition. Notification policy additionally requires the latest turn/outcome and the absence of continuation/close work.

Avoid `queue_entry_for_task_turn`: it enumerates all turn directories (`src/client_state.rs:4330`, `src/client_state.rs:4361`). Instead read/validate queue state once per request (≤1 MiB, like existing state files, `src/client_state.rs:68`, `src/client_state.rs:5358`). For each Dispatching task-turn row, test its exact job/turn directory under the addressed task, without reading prompts. Known state/intents/runner can prove busy immediately; proving not busy requires finishing this bounded queue association check. Scan at most 32 dispatching associations per request, with a continuation carrying task digest, queue digest, rooted identities, each task's turn-directory metadata generation (or proven absence) and next queue index. Task/queue changes or namespace insertion/removal invalidate the proof and restart it. Exhaustion returns null quiescence plus continuation, never a false confirmed result. The notifier prioritizes these small addressed continuations before historical repair. Large changing queues can delay confirmation; no banner is preferable to an unconfirmed needs-input alert.

Task repair uses a separate opaque cursor (≤2 KiB), not the journal cursor. Pages default 64 rows, cap 128, with maximum 128 examined directory entries, 8 MiB task-record input, 32 association checks and a 50 ms cooperative CPU/work budget per request. Each state file is bounded by 1 MiB; no per-row scan of historical turns, run members or other tasks. Budget exhaustion returns the completed rows and a resumable next cursor, including zero-row pages that progressed through residue entries. If even one allowed record must be processed, reserve its byte allowance at page start; process it once, then yield. A syscall/one bounded JSON decode may overrun cooperative time, but never restarts a frozen large row forever. Replies stay below the frame cap.

Enumeration must not call list_tasks/list_names then sort the whole registry (`src/client_state.rs:3226`, `src/rooted_fs.rs:1247`, `src/rooted_fs.rs:11800`). Add a read-only native directory-block iterator anchored to the tasks fd. The opaque token pins client/root/task-directory identity, a directory metadata generation (ctime/mtime at native precision), native block offset and next entry within that bounded block. Resume via the native directory offset on a separately opened descriptor; do not serialize a DIR pointer or assume a POSIX telldir token from another DIR is portable. Bound blocks to 64 KiB, validate lengths/names, check generation/binding before and after the page, and advance through known replacement residue without deleting it. On changed generation return repair_restart; unsafe binding fails. Darwin/APFS cross-process offset/resume behavior is a required platform test, including a registry larger than one work budget. complete=true means native EOF with all rows represented, including explicitly unknown proof facts; it does not assert every task is eligible. No server lease files or historical run work.

After writers settle, a sweep advances to EOF regardless of registry/run size. While directory membership/replacement keeps changing, restarts can prevent a complete replacement; addressed notification checks remain independent. Kernel I/O stalls are outside cooperative progress guarantees. These limits must appear in operator docs. A platform where safe native offsets cannot pass the acceptance test must report repair unavailable rather than silently using full rescans; support there requires a separate persistent enumeration design.

Repair captures journal baseline H before state enumeration if available, then returns it unchanged on every page. The consumer assembles a replacement task projection only after a complete sweep, then replays **after H**, confirming affected IDs again. It never jumps to post-sweep head. Incomplete/restarted sweeps retain the previous projection. If replay expires during a long sweep, restart repair; null baseline gives state-only repair and no cursor advancement. Addressed reads can update candidates throughout a sweep. A nonoverlapping sweep starts every 15 s, even under continuous feed traffic; budgeted pages continue across iterations rather than restarting on the timer.

Rejected alternatives: mandatory all-domain aggregates make one large RunRecord block notification checks (`src/task.rs:2295`, `src/task.rs:2368`, `src/task_client.rs:4641`); a row limit does not bound enumeration; returning page timeout without a continuation does not guarantee progress; old list/status lack the hidden busy facts.

## Decision 7 — N-1 DTO audit, minimal tail, unchanged waits

No fields are added to existing strict DTOs or persisted task/config schemas. Only new selector result types and tolerant event envelopes are added.

| Existing DTO | Baseline decoding | First-wave treatment |
| --- | --- | --- |
| ProbeResponse / ControllerHealthStatus | Tolerant with optional features (`src/protocol.rs:42`, `src/protocol.rs:66`, `src/controller/health_read.rs:48`, `src/controller/health_read.rs:52`) | Controller feature discovery only; no host feature or diagnostic event. |
| ControllerWireRequest / ControllerReadReply | Strict (`src/controller/protocol.rs:30`, `src/controller/read.rs:25`) | Existing task.list command, body selector, unchanged reply envelope. |
| ControllerTaskStatusResult / task log result | Strict (`src/controller/read.rs:115`, `src/controller/read.rs:207`) | No additions; existing events Vec<Value> is an annotation at `src/controller/read.rs:123`, not this feed. |
| TaskListProjection / TaskListRow / RunProjection | Tolerant (`src/task_view.rs:149`, `src/task_view.rs:158`, `src/task_view.rs:197`); nested RunProgress strict (`src/task.rs:2460`) | Entire existing shape unchanged, including run progress. |
| WaitPoll / reconcile / publish-retry / drain bodies/results and saved drain state | Strict (`src/controller/lifecycle.rs:43`, `src/controller/lifecycle.rs:85`, `src/controller/lifecycle.rs:119`, `src/controller/control.rs:19`, `src/controller/control.rs:30`, `src/controller/drain.rs:27`) | No new wait transport or fields; drain schema unchanged. |
| TaskOutcome / TurnSummary / TaskStatus / LocalTaskRecord / RunRecord | Strict saved-wire decoders (`src/task.rs:336`, `src/task.rs:1186`, `src/task.rs:1518`, `src/task.rs:2174`, `src/task.rs:2428`) | No schema migration, settlement latch or new record field. |
| Config / ControllerConfig / NotificationsConfig | Strict (`src/config.rs:15`, `src/config.rs:67`, `src/config.rs:89`) | CLI-only controls, reuse notifications.herdr; no TOML keys. |
| DashboardSnapshot | Serialize-only view with local revision (`src/dashboard/model.rs:26`) | Revision remains cache revision, never a journal seq; SSE is a separate route. |

Minimal debugging command: `worker events -f [--json]`. Require controller mode and -f; no one-shot inspection, --since, snapshots masquerading as events or laptop-local state. Establish current head, print a safe ready control, then tail future records through the selector. Reset/expiry prints snapshot_required and rebaselines; no state repair or notification decisions in this CLI. JSON is one safe event/control object per line. Unknown kinds print envelope metadata only, never opaque unvalidated data. An older controller says `CONTROLLER_EVENTS_UNSUPPORTED` and exits; it does not emulate an ordered feed with polling. Ctrl-C cancels the read loop.

Task waits continue their baseline loop, deadlines, WAIT_BLOCKED errors, reconciliation/DAG advancement, task IDs and aggregate exit semantics (`src/controller/lifecycle.rs:173`, `src/controller/lifecycle.rs:183`, `src/task_client.rs:4613`, `src/task_client.rs:4619`, `src/task_client.rs:4645`, `src/task_client.rs:5737`). No discovery or event read is added to a wait. The runner's worker follow polling (100 ms up to 2 s) and host follow-turn's 250 ms read-only polling also remain (`src/turn_runner.rs:50`, `src/turn_runner.rs:55`, `src/turn_runner.rs:1698`, `src/follow_turn.rs:7`, `src/follow_turn.rs:37`).

Rejected alternatives: extending old strict status DTOs breaks old laptops; unknown top-level commands leave receipts; old-controller inspection emulation invents journal semantics; slowing wait's authoritative polls can change a short-timeout result.

## Decision 8 — foreground laptop notifications with local titles and sound policy

Command: `worker notify [--follow] [--quiet] [--no-titles] [--channel auto|macos|herdr|both]`. Without follow, reconcile current attention once and exit; with follow, run until cancelled. Default hosting is foreground, optionally in a user-owned laptop Herdr pane. No first-wave LaunchAgent or automatic registration; current service setup already has GUI/session restart constraints (`docs/usage.md:485`, `docs/usage.md:498`, `docs/usage.md:500`). Reconnect backoff is 1, 2, 4, then 5 s, with injected timing. A repair or read failure never opens laptop task state.

The laptop consumes EventSource hints, immediately addresses up to 16 affected/candidate IDs, and confirms current latest-turn eligibility. Busy/unknown rows remain candidates; reread on task/queue hints and repair ticks. A saved terminal can precede import failure or continuation/retirement (`src/turn_runner.rs:1514`, `src/turn_runner.rs:1599`, `src/turn_runner.rs:2332`, `src/turn_runner.rs:2345`). Never display directly from turn.finished. All eight latest terminal outcomes can notify after confirmed quiescence; abandonment without a terminal turn uses task/state/stable code. Closed historical NeedsInput is not current attention. NeedsInput/Blocked attention requires current Open state, the matching latest turn and confirmed absence of continuation/close/runner/dispatch work. No run banners. Queue, start, drain and worker hints only trigger rechecks.

Default include the task title, as decided by the owner. Obtain it through the addressed read with include_titles or existing status RPC; reapply local RedactionBoundary and 512-byte/control escaping. Show it only in laptop output/channel text; journal, SSE and persisted dedup remain title-free. --no-titles skips fetching/displaying titles. Never add summaries/questions/failure prose to a banner. Use fixed outcome copy plus safe task ID/title. Title retrieval failure falls back to ID copy without changing eligibility.

Channel auto uses the laptop's reachable Herdr socket when notifications.herdr is enabled, otherwise macOS. Socket discovery is `HerdrSocket::from_env_or_home` (`src/herdr.rs:110`), not the controller account socket. Explicit herdr reports unavailable; both makes one bounded attempt per channel. macOS uses ProcessRunner with a fixed AppleScript `on run argv`, title/body as separate argv and a 2 s timeout. No shell or AppleScript interpolation, no notification dependency. Herdr uses notification_show and its existing sound enum (`src/herdr.rs:147`, `src/herdr.rs:619`): Done for done; Request for needs_input and blocked; None for unknown, failed, cancelled, timed_out, lost and nonterminal abandonment. Quiet makes no channel or sound calls. A coalesced summary uses Request if it includes current attention, otherwise Done for done-only, otherwise None.

Keep worker Herdr reporting and the existing runner notifier independent. The former runs from supervisor/turn reporting on the worker account (`src/supervisor.rs:2932`, `src/turn.rs:1053`, `src/herdr_reporter.rs:1`); the latter fires on the controller runner host in controller mode (`src/turn_runner.rs:515`, `src/lib.rs:1640`, `src/lib.rs:1646`). Neither is the laptop consumer. Browser toasts remain a snapshot derivation (`ui/src/hooks/useAttentionToasts.tsx:32`). Reuse sound constants, not the richer existing formatter that adds questions/prose (`src/herdr_notify.rs:33`, `src/herdr_notify.rs:40`).

N-1 fallback: poll existing remote reads at 2 s for safe diagnostics/current candidate IDs, report **eligibility unknown**, and never raise unconfirmed completion, needs-input or abandonment banners. N-1 DTOs omit continuation/close intents, queue fence and true runner presence (`src/client_state.rs:126`, `src/client_state.rs:128`, `src/client_state.rs:132`, `src/client_state.rs:134`, `src/task_view.rs:159`, `src/controller/read.rs:115`). Do not call mutating task.wait.poll as a notification proof. Retry discovery on reconnect; safe selectors can resume after upgrade. This reduced fallback is intentional.

Rejected alternatives: older-controller list/status cannot distinguish pending continuation with runner=None from human attention; launching a new laptop agent by default adds a lifetime owner; passing notification strings as script code expands injection risk.

## Decision 9 — derived changes, cold baselines and persisted decisions

Private cache: `controller_cache_root()/events/<target-sha256>/notify.lock` and `notify.json`, 0700/0600 rooted validation and atomic fsynced save. Hash SSH destination plus configured remote binary string without printing them. The cache is transport-only (`src/paths.rs:64`), separate from mutation operation locks (`src/controller/envelope.rs:127`). One notifier owns the per-target lock; a second exits safely.

Persist schema, last consumed cursor, last complete repair time, up to 4,096 task/turn/outcome or abandonment decision fingerprints, up to 256 pending candidate IDs/facts, and one independent stable attention-overflow summary fingerprint. No titles or registry copy. Fingerprints survive epoch changes. Delivery is at-most-once **attempts**: save consumed cursor, decisions and pending candidates before channels; crash after save or channel failure can lose display, but does not replay it. Both is two attempts for one saved decision. Exactly-once OS delivery is unavailable (`src/herdr_notify.rs:110`). Quiet consumes/saves identically.

Reconciliation explicitly distinguishes `PreviousProjection::Absent` from `PreviousProjection::Present(empty)`. Current task projection comparison produces `DerivedTaskChange` separately from `WireEvent`; derived facts have no seq and never invent a journal record. Warm repair compares old/new latest eligibility signatures, including busy→quiescent with the same turn/outcome. Signature covers latest turn/outcome/code, busy/quiescence, current-attention eligibility and abandonment; unrelated title/metadata or Open→Closed on an already quiescent Done is not another completion. Unchanged historical rows are not candidates even if their decision was evicted from the ring. Complete-sweep absence removes rows; partial sweep absence does not. Addressed confirmation always precedes a decision, so a queued old hint cannot resurrect a superseded turn.

Every cold process start has Absent previous projection, **even with a valid saved cursor**. Baseline historical completions without banners. Current confirmed attention can produce one summary; retained turn.finished/turn.outcome_changed or task.abandoned hints strictly after the saved cursor remain separate candidates and can reveal matching current outcomes. Generic task/queue invalidation alone cannot make a cold historical completion new. Warm unchanged eligible rows suppress duplicate hints too. A completion whose hint was lost during a restart interval cannot be distinguished from history, so it may be baselined without a banner. This is the safe accepted loss boundary, not an exactly-once claim. Corrupt cache disables display, rebuilds a current baseline and reports a stable diagnostic.

Attention overflow has a stable persisted fingerprint: hash the sorted current confirmed attention tuples (task_id, latest turn_id, outcome or abandonment code), including all overflow members and count, independent of journal UUID, cursor, wall time and page boundaries. Compute it during the completed repair, with bounded page work; maintain it separately from the evicting ring. Persist the same current-attention set fingerprint even below the overflow threshold and during quiet, so an unchanged cold-start summary cannot replay merely because individual attention decisions were evicted. Repeated unchanged overflow after quiet, restart or epoch repair does not re-alert. Before the full set is known, retain candidates/print partial diagnostics without claiming a complete summary. Keep at most 256 individual candidates; overflow sets a persisted repair-needed marker rather than dropping correctness silently.

Coalesce when disconnected >60 s, cursor/epoch repair occurs, or >5 eligible decisions arrive in one reconciliation. Counts come from fresh confirmed current states. Initial startup summarizes current attention, not all completed history. Complete current and previous task projections are in-memory reconciliation state and scale with registry size; cache and per-step candidates/changes are bounded. If memory/work limits prevent a complete repair, keep the previous baseline and report incomplete repair; do not mark unseen rows removed or alert historical completions. Measure this cost before deployment.

Rejected alternatives: cursor-only dedup cannot suppress snapshot-derived decisions; an empty map is not proof a baseline previously existed; synthesizing WireEvent hides missing seq; a ring alone cannot give an unchanged overflow summary stable identity.

## Decision 10 — local viewer SSE and fresh cache publication

Only a controller-viewer with an already initialized journal exposes `GET /api/v1/events`; ordinary local/laptop mode returns 404 (`src/lib.rs:834`, `src/lib.rs:845`, `src/lib.rs:1124`). One local tailer per viewer uses the JournalReader trait; no event RPC or SSH. New bind_with_events/launcher builder preserves existing DashboardHttpState construction and bind (`src/dashboard/web.rs:58`, `src/dashboard/web.rs:74`, `src/dashboard/command.rs:127`). Add only `tokio-stream = { version = "0.1", features = ["sync"] }` for the SSE adapter; Axum/Tokio are already present (`Cargo.toml:14`, `Cargo.toml:24`).

SSE names are frozen in T1: controller.event, snapshot_required, ready, snapshot.ready, heartbeat. Only controller.event has `id: <journal-uuid>:<decimal-seq>`. Controls do not advance cursor. Ready includes the journal window/baseline; snapshot.ready contains local cache revision, never journal sequence. Accept validated after query or Last-Event-ID, reject conflicting values. Bootstrap/reset/expired/ahead sends repair control and baseline; journal unavailable sends snapshot_required with reason unavailable, stable code and null window, then closes safely without inventing a baseline.

Subscribe to bounded live broadcast **before** capturing replay head H; replay through H, then drain only newer buffered events, deduplicate cursor and enter live mode. Lag at either side of the barrier closes with repair if possible; never silently skip to head. Use 200 ms injected journal checks, capacity 256, maximum eight streams, no blocking producer on subscriber. Slow subscribers disconnect for repair. Set text/event-stream and no-store, send a keepalive comment plus named heartbeat every 10 s. Browser watchdog is 30 s without observable activity.

Preserve loopback bind (`src/dashboard/web.rs:84`) and exact Host checks (`src/dashboard/web.rs:205`). Reject a present foreign Origin and Sec-Fetch-Site cross-site; allow absent Origin for same-origin native EventSource. No wildcard CORS; keep connect-src self (`src/dashboard/web.rs:39`). Mutation Origin/JSON guards and generation fences stay intact (`src/dashboard/web.rs:451`, `src/dashboard/web.rs:541`, `ui/src/views/TaskDetail.tsx:45`).

Tailer, subscriptions and refresh work cancel before graceful HTTP shutdown joins (`src/dashboard/web.rs:130`). The tunnel has a **separate** laptop-to-viewer heartbeat every 5 s and viewer timeout after 30 s silence (`src/dashboard/tunnel.rs:46`, `src/dashboard/tunnel.rs:47`, `src/lib.rs:910`). EOF or timeout triggers viewer shutdown, closes SSE TCP connections, causes browser polling fallback, and allows the existing tunnel supervisor to reconnect. SSE heartbeat cannot reset the stdin tunnel timer. Test timeout with an injected watcher/hook, not a real 30 s wait.

Events trigger a 100 ms debounced, single-flight local task/run/queue refresh using **project_task_list_with_blocking_codes** (`src/task_view.rs:334`). Do not call collect_task_projection: it overlays remote task status (`src/dashboard/task.rs:167`, `src/dashboard/task.rs:223`). Preserve cached workers/metrics and derive freshness locally. Publish snapshot.ready only after the refreshed cache is visible, because ordinary HTTP read returns the last completed snapshot (`src/dashboard/service.rs:401`). Keep a dirty flag for changes during refresh, not an unbounded work queue. A generation fence prevents an older full collector overwriting the newer local projection, while merging eligible worker observations.

The regular collector remains on its existing **2 s** cadence (`src/dashboard/cache.rs:5`, `src/dashboard/service.rs:378`), with **10 s** idle probes (`src/dashboard/cache.rs:9`); 15/20 s are worker/global deadlines, not intervals (`src/dashboard/service.rs:32`, `src/dashboard/service.rs:33`). Successful full publications also send snapshot.ready (`src/dashboard/service.rs:503`). Queue/run/worker dashboard refresh comes from these local projectors/collector, not task selector RPCs. Large local projection work may lag hints; it must not block tailer heartbeats or authoritative mutations.

Rejected alternatives: refetching only once can read stale cache; using the SSH collector for the fast path ties lifecycle refresh to offline workers; conflating stream and tunnel heartbeats leaves viewer shutdown untested.

## Decision 11 — shared browser invalidation and one asset owner

One injectable EventSource client at App root (`ui/src/App.tsx:67`) validates string cursors and envelopes; events invalidate resources, never edit saved task state in the browser. Unknown well-formed kinds/versions invalidate all resources; malformed/oversized data closes stream and repairs. Debounce 100 ms; lifecycle hints refresh snapshot, affected selected detail, previews and attention questions, with snapshot.ready causing another read after publication. Queue/run/DAG/worker/drain invalidates corresponding snapshot views.

A new turn must refresh questions even when waiting task IDs did not change: existing hook keys only on that set (`ui/src/hooks/useAttentionQuestions.ts:12`, `ui/src/hooks/useAttentionQuestions.ts:20`). Preserve MAX_ATTENTION_FETCHES=6 (`ui/src/hooks/useAttentionQuestions.ts:6`), cancellation and stale-response guards. Previews already refetch on snapshot task-revision key, not on their own timer (`ui/src/hooks/useTaskPreviews.ts:11`). Browser toasts continue to derive from fresh snapshots.

Healthy stream uses 15 s browser snapshot/detail anti-entropy; server collection remains 2 s. Error, 404, parse failure, repair or 30 s watchdog immediately restores current 2 s polling (`ui/src/hooks/useSnapshot.ts:6`) and bounded reconnect. Close errored native EventSource, create a new URL using the latest validated cursor, and avoid stale after-query/native Last-Event-ID conflicts. Keep last good snapshot, single in-flight fetch per resource, abort/generation guards and drafts (`ui/src/views/TaskDetail.tsx:128`). Visibility/reconnect triggers repair.

Keep 1 s log polling, task/turn/stream identity, UTF-8 byte cursors and completed current-end draining (`ui/src/hooks/useTurnLog.ts:5`, `ui/src/hooks/useTurnLog.ts:15`, `ui/src/hooks/useTurnLog.ts:41`, `ui/src/hooks/useTurnLog.ts:90`). Neither a lifecycle event nor runner completion proves final log EOF. No log protocol or cursor change.

UI build is tsc -b plus Vite (`ui/package.json:8`), with stable assets in src/dashboard/static/app (`ui/vite.config.ts:19`), embedded by Rust (`src/dashboard/web.rs:41`, `src/dashboard/web.rs:267`). build.rs only creates build identity (`build.rs:26`, `build.rs:32`). T9 alone builds/commits generated assets after all UI source merges, then compares a complete fresh temporary build including fonts. Existing CI already runs UI tests/lint and checks this (`.github/workflows/ci.yml:32`, `.github/workflows/ci.yml:34`, `.github/workflows/ci.yml:36`). Parallel agents own source/tests only.

Rejected alternatives: per-hook streams multiply connections; retaining old native auto-reconnect conflicts with an after query; lifecycle feed cannot replace log-byte polling; rebuilding assets in parallel produces conflicts and stale binaries.

## Decision 12 — interface-first parallel work and deterministic acceptance

T1 commits all shared envelopes, cursor/bounds, producer and journal traits, EventSource/reconciliation/derived-change interfaces, selector DTOs, SSE names and in-memory fakes. After T1, journal, producers, RPC/client, viewer, browser and notifier core proceed independently against those contracts. T8 wires them; T9 owns docs/assets/acceptance. Strict file leases include module roots and test roots. Core Rust/durability/concurrency tracks suit Codex; UI, notifier channels and docs suit Cursor. Labels describe task fit, not different architectural authority.

Current tests are consolidated area binaries: tests/controller/main.rs, tests/dashboard/main.rs, tests/cli/main.rs; module filenames are not Cargo targets (`docs/testing.md:6`, `tests/controller/main.rs:40`, `tests/dashboard/main.rs:20`). Use CARGO_BUILD_JOBS=4 targeted nextest, or plain Cargo integration tests serially (`docs/testing.md:26`, `docs/testing.md:34`). Inject clocks, channels and fault hooks; never prove ordering with sleeps (`docs/testing.md:90`). Reuse the log-wait clock pattern (`tests/controller/controller_read_routes.rs:329`).

Acceptance includes all journal fault points/pinned sealed swaps/ESTALE, durable hints outside every fence, detached runner versus laptop-local attachment, addressed proof under hidden intents/dead runner/dispatch, large registry repair progression, corrupted event directory/lock with healthy state reads, new discovery followed by old selector execution without receipts, cold baseline/eviction/quiet/overflow, SSE barrier/security/lag/tunnel timeout, cache-generation merge and UI questions/drafts/log identity. Wait/retry regression tests confirm existing semantics, with no new wait implementation (`tests/controller/controller_say_wait_exit.rs:229`, `tests/controller/controller_retry.rs:463`).

Deploy only fully integrated feature-advertising builds. Validate new laptop/old controller (eligibility unknown), old laptop/new controller (strict shapes unchanged), new RPC/old leader/runner (missing hints repaired), and rollback between discovery/read. Live pool checks are an integrator checklist after implementation approval, not authorization for this documentation track.

Rejected alternatives: serially depending on a concrete journal prevents independent agent work; guessed per-file test target names do not run consolidated tests; same-version happy paths miss rollback and detached process attachment.

## Decision 13 — optional persistent transport sketch

A future controller-only Unix socket uses private 0700 directories, 0600 sockets and same-account SSH authority; no new token store or per-mini daemons. The laptop forwards `ssh -N -L <laptop.sock>:<controller.sock>` with StreamLocalBindUnlink, restrictive mask, BatchMode, no agent forwarding, ExitOnForwardFailure and keepalives. Reuse private transport-cache and tunnel lifecycle boundaries (`src/paths.rs:64`, `src/transport.rs:904`, `src/transport.rs:938`, `src/transport.rs:943`, `src/transport.rs:962`). Validate owner/type/inode and unlink only proven owned stale sockets.

Handshake must verify protocol 7, controller client/account identity, configured route, ProcessIdentity, fresh service-generation UUID, persistent journal UUID and features. Service generation differs from journal epoch. Current account identity has home/user/uid (`src/protocol.rs:503`, `src/controller/init.rs:654`), but ControllerConfig has only enabled/SSH/binary (`src/config.rs:68`) and no pinned controller-client UUID. A future authenticated stdio identity read must bootstrap a private laptop pin and expected generation before socket requests; unexpected identity fails closed. This requires separate design approval. Unsupported service/channel loss falls back to current per-request SSH with the same mutation ID/digest.

Persistent multi-frame parsing/cancellation needs a separate bounded decoder; current stdio requires one frame plus EOF (`src/controller/protocol.rs:90`). Build only after measuring remaining SSH cost after long-poll/SSE; multiplexing already uses ControlPersist=60 (`src/transport.rs:946`). No socket files or implementation tasks belong to this first wave.

Rejected alternatives: unverified generation can bind to the wrong restarted service; relaxing stdio EOF changes existing safety; the brief's latency estimate alone does not justify another daemon.

## Deferred

| Cut from first wave | Reason and retained behavior |
| --- | --- |
| Event-assisted task wait | H4: short deadlines and reconcile-driven DAG progress require the original authoritative 100 ms loop. No discovery/read is added (`src/controller/lifecycle.rs:183`). |
| run.finished/run.reopened, settlement tracking, run banners and server-side run aggregates | H6 / coverage High #2: RunRecord has unbounded membership relative to an RPC page, and DAG submission quiescence is not wait completion (`src/task.rs:2368`, `src/dag.rs:737`, `src/task_client.rs:4645`). Keep run.changed and existing local dashboard projection. |
| worker.available/worker.unavailable | Committed observations do not provide continuous availability; TTL expiry has no write (`src/client_state.rs:88`). Keep worker.changed, observation time and local freshness. |
| Rich events inspection, --since and old-controller feed emulation | Not needed for prompt notifications/live dashboard. Keep only worker events -f [--json]; unsupported old server is explicit. |
| Universal all-domain events.snapshot | H6: notification confirmation should address small task sets; task-only resumable repair handles anti-entropy. Dashboard queue/run/worker refresh is local (`src/task_view.rs:334`). |
| Laptop LaunchAgent; persistent socket implementation | Foreground notifier first; service lifetime/identity bootstrap and measured benefit require separate work (`docs/usage.md:500`, `src/config.rs:68`). Phase 3 above is a sketch only. |

## Changes after review

Finding labels C-H/C-M/C-L below identify the coverage report's ordered items, in addition to the adversarial report's own labels. Fixed means a concrete revised contract plus plan acceptance test; cut means removed from first-wave execution; accepted limitation is stated and tested where observable.

| Review finding | Resolution | Spec / plan location |
| --- | --- | --- |
| H1: discovery/old executable race | Fixed: task.list selector is safe on N-1 independent of discovery; receipt/request-row regression. | Decision 5; T4, T8 |
| H2: only active inode pinned | Fixed: manifest pins all retained segment identities/ranges/lengths, sealed exact-size checks and reopen swap test. | Decision 4; T2 |
| H3: staging/displaced crash residue | Fixed: named evidenced roles, cleanup budget and exchange/fsync/removal fault matrix including repeated failures. | Decision 4; T2 |
| H4: short task.wait result changes | Cut: event-assisted wait; baseline loop unchanged and regression tested. | Decision 7, Deferred; T8 |
| H5: N-1 eligibility cannot be confirmed | Fixed as reduced fallback: eligibility unknown; no unconfirmed banners. | Decision 8; T4, T7, T8 |
| H6: run row/enumeration blocks repair and addressed notices | Cut run aggregates; fixed task-only native paged enumeration and independent addressed/proof continuation. | Decision 6, Deferred; T4 |
| M1: mandatory journal prevents state-only repair | Fixed: independent minimal state opener; optional lazy journal baseline; unsafe directory/lock tests. | Decisions 5–6; T4 |
| M2: eviction/restart/empty baseline/overflow ambiguity | Fixed: separate DerivedTaskChange, Absent vs Present(empty), cold history baseline regardless of cursor, stable persisted overflow fingerprint. Lost-hint completion across cold restart is an accepted loss boundary. | Decision 9; T1, T4, T7 |
| M3: optional I/O under authoritative fences | Fixed: deferred hints, try-only bounded publisher, append/exit grace after all fences; fsync-gate test. Kernel I/O can still stall the background worker. | Decisions 3–4; T2, T3, T8 |
| C-H1: fresh detached child/local viewer store | Fixed: host-initialized-journal test at actual shared opens, not config.enabled or parent sink. | Decision 3; T8 |
| C-H2: run predicate is not wait | Cut all settlement semantics; no claim that dag_run_is_quiescent proves finished. | Deferred; T3 excludes it |
| C-H3: fast path calls SSH collector | Fixed: local project_task_list_with_blocking_codes entry point, worker merge generation fence. | Decision 10; T5 |
| C-M1: cadence vs deadlines | Fixed: 2 s collection / 10 s idle probes; 15/20 s deadlines separately stated. | Decision 10; T5 |
| C-M2: acceptance/terminal hook anchors | Fixed: accepted-status saves 1390/1419; prepared save excluded; actual finish functions cited. | Decision 2; T3 |
| C-M3: host retention close absent/mislabeled | Fixed coverage statement: host GC writes worker Closed, controller hints require later lifecycle persistence. This propagation delay is accepted. | Decision 3 exclusions; T3, T8 |
| C-M4: leader/health and request bookkeeping | Accepted exclusion explicitly documented; health polling is the failover signal, journal heartbeat is not health. Correct receipt-phase cites. | Decisions 1, 3; T5, T9 |
| C-M5: ready TTL expires without write | Cut semantic availability; accepted TTL silence, worker.changed only from committed observations. | Decisions 2–3, Deferred; T3 |
| C-M6: runner-log completion sidecar | Accepted separate signal; lifecycle feed does not wake byte followers. Keep log/reconcile polling. | Decisions 3, 7, 11; T6, T8 |
| C-M7: SSE/tunnel heartbeat timeout | Fixed: separate 5 s/30 s tunnel timer; cancel streams on viewer timeout, restore polling/reconnect. | Decision 10; T5, T6, T8 |
| C-M8: worker Herdr vs laptop channels/sounds/toasts | Fixed: distinct processes/channels; explicit Done/Request/None map; toasts remain snapshot-derived. | Decision 8; T7 |
| C-M9: ESTALE treated as damage | Fixed: three same-binding deadline-bounded retries; persistent pinned mismatch fails unavailable. | Decision 4; T2, T4 |
| C-L1: off-by-one/nearby references | Fixed: exact anchor table below, used throughout revised docs. | Anchor table; T1–T9 |
| C-L2: unnamed polls, questions, Herdr metadata, job state, finalizer correction | Fixed coverage statements; keep worker/host/log polls, previews key-driven, question turn invalidation, no byte-watch false hints, finalizer outcome hook. | Decisions 2–3, 7–8, 11; T3, T6 |

Corrected anchors (these describe baseline, not proposed functions):

| Claim | Correct path:line |
| --- | --- |
| Strict ControllerReadReply | `src/controller/read.rs:25` |
| Status annotation events | `src/controller/read.rs:123`, `src/controller/read.rs:138` |
| Loopback bind / completed cache read | `src/dashboard/web.rs:84`, `src/dashboard/service.rs:401` |
| Tick collection / active tick limit 32 | `src/controller/health.rs:45`, `src/client_state/active_tasks.rs:46` |
| Derived active-index retirement | `src/client_state.rs:3211` |
| wait_poll_reply, not list/status snapshot | `src/controller/lifecycle.rs:234` |
| Socket discovery | `src/herdr.rs:110` |
| Finalization fence / actual flock / empty log creation | `src/runner_log.rs:156`, `src/runner_log.rs:195`, `src/runner_log.rs:192` |
| Cancel-before-acceptance mutation | `src/turn_runner.rs:2092` |
| Terminal / publication-failure functions | `src/turn_runner.rs:1501`, `src/turn_runner.rs:1599` |
| Runner notifier / worker Herdr flag | `src/turn_runner.rs:515`, `src/turn_runner.rs:1348` |
| Attention set key / six-fetch cap | `ui/src/hooks/useAttentionQuestions.ts:20`, `ui/src/hooks/useAttentionQuestions.ts:6` |
| Request phase publication/drive, not store open | `src/controller/store.rs:1111`, `src/controller/store.rs:807` |

## Open questions for owner approval

Round 2's scope cuts, selector dispatch, titles-by-default, sound reuse, eligibility-unknown N-1 fallback and interface-first parallel plan are decisions, not reopened questions. Defaults below are safe choices for implementation; no question blocks this documentation track.

1. **Operational bounds:** confirm 18 MiB inclusive disk cap, 128 files, 32-event batches, 128-batch process publishers, 50 ms admission/exit grace and eight streams after measurements. Rooted fsync is synchronous (`src/rooted_fs.rs:1913`); no syscall latency guarantee is possible. A dropped hint is repaired after the next completed sweep.
2. **Enumeration platform gate:** approve Darwin/APFS native-offset conformance as a release gate. Existing enumeration is whole-directory (`src/rooted_fs.rs:11800`); unsafe/unsupported offsets must fail repair explicitly. Another filesystem/platform may need a separately designed enumerator. Continuous writes can postpone a complete sweep; measure that risk against actual registry size.
3. **Proof and memory cost:** confirm conservative unknown with resumable dispatch proof and in-memory O(task registry) previous/current projection. Existing busy association scans historical turn directories (`src/client_state.rs:4330`); bounded RPCs must not reproduce it. Large queues can delay notification confirmation; keep unknown instead of guessing.
4. **Delivery/loss policy:** confirm at-most-once attempts, save-before-display loss, cold-start lost-hint history baselining and delayed acceptance hints behind existing log fences. These follow OS/channel nontransactionality and log ownership (`src/herdr_notify.rs:110`, `src/runner_log.rs:156`). No exactly-once promise.
5. **Coalescing/hosting defaults:** confirm foreground lifetime, >60 s / >5-decision summaries and initial current-attention summary. Titles and --no-titles are already decided. No automatic LaunchAgent setup (`docs/usage.md:500`).
6. **Deferred transport:** keep socket implementation parked until measurements and authenticated identity pin/generation design warrant it (`src/config.rs:68`, `src/controller/protocol.rs:90`).
