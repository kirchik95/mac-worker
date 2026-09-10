# Persistent controller — design

Date: 2026-09-10. Owner: FLOW. Operator steps (default off): [usage — Remote controller](../../usage.md#remote-controller). This file keeps locks, digests, and protocol detail.

Foundation: `bdc08d8c30ff80b0e3610fa89c8e15da37244682`. PROTOCOL_VERSION **7** transports typed `TaskStatus.reported_checks`. Helpers and clients still on protocol 6 mismatch at preflight; upgrade them together.

## Rejected

A Unix daemon on the **submitting MacBook** (`controller.sock` next to laptop `PathLayout.state`, laptop CLI connecting over AF_UNIX) does **not** satisfy disconnect/sleep/offline continuation. That draft is withdrawn.

`ClientStateStore::open` does **not** take a lifetime exclusive writer lease. In `open_inner` (`src/client_state.rs`) `StateLock` is a local `_lock` used only while creating/validating the root; it is **not** stored on `ClientStateInner`. After `open` returns, every mutation takes a **fresh** `StateLock`/`QueueLock` and drops it. Holding those locks for a process lifetime, then calling `TaskClient`, deadlocks detached runners that `open` the same store and flock new fds.

## Required

Remote persistent controller on an **always-on host**, reached from the laptop with **authenticated SSH** (`SshTransport`, same destination rules as `WorkerEntry.ssh`). One controller-owned `ClientStateStore`. Laptop freeze of local/WIP/origin OID, then exact Git object transfer **before** ACK. All opted-in `TaskCommand` + dashboard snapshot/detail/log/reply/accept go to that store. Controller imports results so DAG can progress while the MacBook is offline. Laptop `fetch` is a later, separate materialization. Default local mode unchanged.

## Where state lives

| | MacBook | Controller host | Execution worker |
|---|---|---|---|
| laptop `config.toml` `[controller]` | `enabled` + `ssh` + `remote_binary` | — | — |
| host `config.toml` `workers[]` | `worker workers` / `setup` / `run` only | dispatch inventory | — |
| `$XDG_STATE_HOME/mac-worker` `ClientStateStore` | **not opened** for task/dashboard when enabled | **only** authoritative task/run/queue/runner store | no |
| `$XDG_CACHE_HOME/mac-worker/transfer` | freeze objects to push; later user-repo import | durable objects + **controller** result import (DAG pin) | no |
| `HostStore` | no | no | yes |
| `worker runner` children | no | yes (open **same** controller state, per-tx locks) | no |
| Dashboard HTTP | SSH `-L` to controller loopback; **no** laptop store | loopback bind only | no |
| Control plane | `/usr/bin/ssh` → `remote_binary host controller-rpc` | stdio RPC (optional Unix socket **on this host only**, never the MacBook) | existing `host *` |

## Reuse

- SSH: `SshTransport` / `valid_ssh_destination` / `WorkerEntry.remote_binary` (`src/transport.rs`, `src/config.rs`).
- RPC: same class of bounded stdio JSON as `RemoteJobClient` (`src/transfer.rs`), new hidden `HostCommand`s, not TCP.
- Locks: per-method `StateLock`/`QueueLock`. Election: dedicated `controller.lock` + `ProcessIdentity` (`src/job.rs`).
- Freeze: `ProjectState::load`, `TransferRepo::resolve_base` / `build_wip_base`, `GitTransport::preflight_origin` (`src/task_client.rs` submit). `inspect_with_pinned_project_id` only on controller checkout of transferred objects — never a MacBook path.
- ACK: `submission_intent_turn_id`, `submit_with_ids`, `update_task_if_current`, `close_intent`.
- Results: controller `GitTransport::fetch_result` + `TransferRepo::import_result` + `fetched_head`. Laptop fetch uses `controller-upload-pack` then user-repo `import_result`. Host outbox independent. Controller transfer store holds one global `xfer.lock` during `prepare_source_receive` cache setup, `finish_source_receive` owned-graph/pin Git, `prepare_result_upload` pin Git, and a short bind-metadata lookup. A slow repository can delay unrelated transfer preparation/finalization. `receive_pack` / `upload_pack` streaming children do **not** call `lock_transfers` and do not hold that lock for the pack lifetime.
- Logs: `LogChunk` / `MAX_LOG_CHUNK_BYTES`.

## Default vs opt-in

No `[controller]` / `enabled=false`: today’s `run_task_command` / `run_dashboard_command` open laptop state. No store migration.

`enabled=true` and SSH/rpc fail: `CONTROLLER_UNAVAILABLE`, **zero** laptop `ClientStateStore::open` for those verbs. No fallback. `host controller-rpc` is a separate process from `worker controller run`. A missing leader is not transport failure. Keep `controller run` up for autonomous recovery, DAG readiness, and runners.

`worker controller run` is invoked **on the controller host**. Not started from the MacBook as a local daemon. No launchd in this task.

## Freeze then ACK (no second task store)

Laptop only:

1. `ProjectState::load` on `--project`/cwd. Snapshot settings, prompt, limits, `close_policy`, includes, `project_id`, `worktree_id`.
2. Local committed: `resolve_base`. `--wip`: `build_wip_base` (worktree tree, not a later HEAD). `source=origin`: `resolve_base_oid` + `preflight_origin`, exact OID on the request.
3. `ssh … host controller-receive-pack` with request identity (controller-issued receive token + fingerprint). **No** remote path arguments.
4. RPC `task.submit` with pinned ids, `base_oid`, settings hash, prompt, `transfer_receipt`. **No** MacBook path.

Controller registers `ControllerProject { project_id, worktree_id, settings_sha256, base_oid, repo_id }` in **its** store. Retry identity is `request_id` + payload digest; it must not re-read laptop HEAD.

## Routes (`src/cli.rs` `TaskCommand`)

When enabled, **all** of: Submit, Batch, List, Status, Logs, Diff, Say, Cancel, Result, Fetch, Close, Wait, Reconcile — plus dashboard GET snapshot/detail/log and POST reply/accept.

Stay on the laptop: `init`, `setup`, `doctor`, `workers`, `gc`, `run`, **job** status/logs/cancel, ENV preview (read-only, no controller tasks). Hidden `Runner` only on the controller host. Empty laptop `[[workers]]` fails `at least one worker is required` on `setup` / `doctor` / `workers` / `run` / streaming job `logs` / `gc`. Job `status` and `cancel` do not use that inventory guard.

## ACK / restart

Persist `(request_id, server payload digest, task_id, turn_id)` **before** enqueue/spawn. Request identity is **protocol version + command + canonical body** (sorted object keys), computed server-side. A client `payload_sha256` is ignored. Duplicate JSON keys are rejected before that canonicalization; `serde_json::Value` last-key-wins is not the request boundary. Same id + same digest → same ACK. Same id, different digest (including a different command with `body={}`) → `CONTROLLER_REQUEST_CONFLICT`. Death before the row: retry may allocate. After the row: resume the stored record — do **not** call `submit_with_ids` / `create_task` again. CLI exit after ACK: the request remains on the controller store; ACK is not a runner start. Autonomous progress still needs `controller run`. Close retry resumes `close_intent` only. Frames: length-prefixed JSON, protocol **7**, 1 MiB RPC / `MAX_LOG_CHUNK_BYTES` logs; partial/EOF → `CONTROLLER_TRANSPORT`, retry same `request_id`.

Retry handle: persist an operation envelope (`request_id`, digest, command, body) in the **laptop transport cache** (`$XDG_CACHE_HOME/mac-worker/controller`). That cache is not a second task/queue store. A second fresh CLI invocation is not automatically the same request.

Checkpoint 1 executor is **honestly fake**: durable publish/ACK only. It does not call `TaskClient` or enqueue work.

## Dashboard transport

Laptop `worker dashboard` uses a **managed SSH local-forward** to the controller loopback HTTP service (existing Host/Origin/CAS). Dashboard is not an RPC DTO family. Public flags remain `--port`, `--no-open`, `--no-facts-refresh`. The laptop command allocates loopback, waits until the forwarded URL answers, prints `http://127.0.0.1:<port>`, and holds the SSH child until SIGINT/SIGHUP/SIGTERM. Failure is `CONTROLLER_UNAVAILABLE` with no laptop-store fallback.

## DAG

Controller import + controller `fetched_head` is the pin descendants wait on (Closed+Done **and** that import). A missing MacBook branch is not a failed agent outcome. After `reconcile_runners` (must not auto-complete `close_intent`), drop tx locks, then DAG wake on controller (ENV hook; FLOW still performs the import). Sidecars preserved.

## Tests (later; fake SSH; no live roots)

Default local regression; enabled + SSH/rpc fail → `CONTROLLER_UNAVAILABLE`; laptop parent exits after ACK; restart idempotency; same vs conflicting payload; WIP/local capture ignores later HEAD; controller import without laptop fetch, then laptop fetch same OID; forwarded dashboard CAS; two `controller run` → `CONTROLLER_LOCK_HELD`.
