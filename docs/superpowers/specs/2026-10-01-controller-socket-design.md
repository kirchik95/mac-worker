# Persistent Controller Channel

Date: 2026-10-01

Status: Phase 3 design for orchestrator review. The owner approved design and implementation; the defaults below are conservative implementation decisions unless a reviewer objects.

Code baseline: `0802421541679443e7d1982988a8c6482e5fdbe9` (`integ/ev-wave`). Recheck anchors against the final events-wave head before T1 on `integ/p3`.

Implementation plan: [2026-10-01-controller-socket.md](../plans/2026-10-01-controller-socket.md).

Existing-code citations are `path:line` at this baseline. Channel modules, DTOs, limits and operator commands below are proposed additions, not measured or implemented behavior. The independent [.briefs/p3-survey-report.md](../../../.briefs/p3-survey-report.md), particularly sections 3, 4 and 7, was checked against the cited code; it is an implementation handoff, not an empirical benchmark.

## Purpose and boundaries

Remove the SSH session start from repeated laptop controller RPCs while retaining the existing per-request process boundary. A leader-owned private Unix socket carries sequential framed RPCs through an on-demand forward on the laptop's existing ControlMaster. Each admitted RPC still starts the existing `worker host controller-rpc` child. Thus the expected saving is an SSH session/local SSH invocation per warm RPC, **not** elimination of remote worker startup. Measure both costs in Phase 3.

This implements and supersedes the transport sketch in Decision 13 of [the events design](2026-09-30-controller-events-design.md). Its "build only after measuring" gate no longer applies: measurement is part of this phase's validation. Protocol remains 7 (`src/protocol.rs:5`). Task state, journals, event policy, wait semantics, Git streams and dashboard SSE keep their existing authority and formats (`src/controller/lifecycle.rs:164`, `src/controller/events/client.rs:132`, `src/controller/stream_client.rs:87`, `src/dashboard/events.rs:25`). Phase 4, Phase 5 and a laptop LaunchAgent are outside this phase.

## Decision 1 — reuse the transport and request boundaries

Existing mechanism: one-shot framed RPC (`src/controller/protocol.rs:62`, `src/controller/protocol.rs:105`); canonical request identity (`src/controller/protocol.rs:165`); verified ACK/read identity (`src/controller/execute.rs:809`, `src/controller/read.rs:69`); durable same-ID retry and envelope settlement (`src/controller/execute.rs:695`, `src/controller/envelope.rs:125`, `src/controller/envelope.rs:165`); private controller transport cache (`src/paths.rs:64`); multiplexed SSH (`src/transport.rs:943`); managed tunnel ownership/backoff examples (`src/dashboard/tunnel.rs:156`, `src/dashboard/tunnel.rs:592`); rooted private publication and binding checks (`src/rooted_fs.rs:1003`, `src/rooted_fs.rs:1831`, `src/rooted_fs.rs:2216`). Reuse these.

Not found — checked: controller routing (`src/controller/execute.rs:568`), leader lifecycle (`src/lib.rs:1631`, `src/controller/runtime.rs:56`), SSH constructors (`src/transport.rs:868`, `src/transport.rs:1125`), account/config identity (`src/protocol.rs:503`, `src/config.rs:68`) and socket handling (`src/transport.rs:1053`). There is no controller socket listener, session decoder, application pin or ControlMaster forward lifecycle API. Herdr's socket is a notification endpoint, not this service (`src/herdr.rs:311`).

Add one controller-only feature, `controller.socket`. Common features continue to describe the serving binary; this feature is included only after a live, owned listener has published its service record. Keep `HOST_FEATURES` unchanged (`src/features.rs:7`). No new TOML key, token, task record, durable request schema or daemon is needed.

Rejected alternatives: replace the durable request kernel; fold Git or HTTP into the framed channel; use journal UUID as controller-account identity; change stdio EOF to accommodate sessions.

## Decision 2 — serve with the existing RPC child, not in-process handlers

The existing controller leader process, including its LaunchAgent invocation, owns the listener. Bind only after `ControllerLeader::acquire`; its lock already covers the process lifetime (`src/controller/leader.rs:12`, `src/lib.rs:1631`). No second service/plist is added (`src/controller/service.rs:330`). For each complete, validated application frame, invoke the leader's captured absolute executable with fixed argv `--config <captured absolute config> host controller-rpc`, the leader's pinned HOME/XDG layout, and **that exact frame followed by stdin EOF**. Neither the executable, config nor environment comes from the socket request (`src/controller/service.rs:339`, `src/lib.rs:1765`). The child calls `serve_rpc_with_runtime` unchanged for application dispatch (`src/lib.rs:1767`).

Use `SystemProcessRunner::run_interruptible` for the child, with stdout `MAX_FRAME_BYTES + 4`, stderr 256 KiB and a 30 s request deadline (`src/process.rs:131`, `src/controller/mod.rs:91`). Capture output into the connection, never the leader's stdout. Preserve framed host errors and child exit status. A panic, unexpected exit, missing reply, output overflow or stalled child affects its session; it must not unwind the leader tick or terminate the listener. Guard the supervisor task against unwinding; do not call task handlers in that task. Retain the existing process-group cancellation and bounded capture-drain behavior (`src/process.rs:185`, `src/process.rs:401`, `src/process.rs:485`).

The survey's ten process assumptions are resolved as follows. This is an explicit serving-model audit of [.briefs/p3-survey-report.md, section 3](../../../.briefs/p3-survey-report.md), with current-code anchors:

| Assumption | Phase 3 treatment | Code basis |
| --- | --- | --- |
| 1. Cross-process lock exclusion | Each application handler remains in a different OS process. No new claim that existing flock handles serialize handler threads. | `src/controller/store.rs:499`, `src/client_state.rs:5467`, `src/controller/drain.rs:190` |
| 2. Request lifetime / stdio | One child, one frame, EOF, reply and exit. Leader stdio remains diagnostics only. | `src/controller/protocol.rs:105`, `src/lib.rs:1778`, `src/controller/service.rs:360` |
| 3. Process-scoped publisher | Each child retains its existing optional publisher and exit grace; the leader keeps its own publisher. The socket supervisor creates none. Cross-process journal locking remains the existing mechanism. | `src/lib.rs:261`, `src/lib.rs:330`, `src/controller/execute.rs:603`, `src/lib.rs:1635` |
| 4. Thread-local DeferredHints | No handler thread is reused across requests; hint guards and process-global dropped counters remain child-local. | `src/client_state/events.rs:16`, `src/client_state/events.rs:28` |
| 5. Thread-local WaitDeadline | Child handlers retain their own CURRENT scope. Socket deadlines additionally supervise the child; they do not replace wait deadlines. | `src/client_state/deadline.rs:16`, `src/controller/lifecycle.rs:170` |
| 6. Config / other local state | Child config load and SSH settings remain process-local; the leader captures socket-launch inputs once. No per-request config load in the supervisor. | `src/config.rs:47`, `src/transport.rs:826`, `src/rooted_fs.rs:276` |
| 7. Signals | Listener shares the leader's existing shutdown flag and runtime/signal registration. Children do not install leader handlers. | `src/lib.rs:1678`, `src/lib.rs:1683`, `src/controller/runtime.rs:17` |
| 8. Exit / panic | CLI exit mapping happens in the child, outside the leader. Detached runner behavior stays intact. | `src/lib.rs:1778`, `src/controller/execute.rs:624`, `src/prepare_turn.rs:69` |
| 9. EOF framing | New bounded session decoder is only for the socket. Child stdin still satisfies strict read_frame EOF. | `src/controller/protocol.rs:90`, `src/controller/protocol.rs:110` |
| 10. Clock / cancellation | Supervisor explicitly combines session EOF, request deadline and leader shutdown to interrupt the child. Existing false-cancelled event runtime is not mistaken for leader cancellation. | `src/controller/execute.rs:563`, `src/lib.rs:214`, `src/controller/runtime.rs:24` |

Compare alternatives:

| Serving model | Cost saved | Safety / complexity | Choice |
| --- | --- | --- | --- |
| Child per socket request | Repeated laptop SSH invocation and SSH session; worker startup remains | Reuses verified process ownership, error framing, TLS scopes, locks and cancellation | **Phase 3** |
| In-process dispatch through an extracted handler | Also saves worker startup / repeated config/store opens | Requires a lock/thread-local/config/publisher/cancellation audit across all ten assumptions; panic recovery does not cancel a blocked syscall | Deferred until measurements justify a separate change |
| Persistent request-worker pool | Amortizes child startup | Adds reusable-process state, lifecycle and recovery contracts without preserving one-request lifetime | Rejected for this phase |

Capture `SystemBinaryIdentitySource` at service start. Require known started and installed identities matching the captured executable; missing evidence or an installed identity change stops socket admission and withdraws advertising. Use stdio until the leader restarts. Reuse the existing inexpensive identity source, but explicitly reject unknown identity rather than relying on binary_is_outdated's unknown-is-false result (`src/binary_identity.rs:62`, `src/binary_identity.rs:126`). No per-RPC binary hashing. A socket child must not silently start a replaced executable under an old service generation.

## Decision 3 — bound sessions, child work and shutdown

The accept/session machinery runs beside the tick on the leader's existing Tokio runtime (`Cargo.toml:24`, `src/lib.rs:1678`, `src/controller/runtime.rs:56`). Each admitted child runs in one native supervisor thread holding its permit, returning through a oneshot; at most eight exist. Do not use Tokio spawn_blocking for child supervision, whose runtime shutdown could wait on stuck work. No worker pool or unbounded join is added. Admission is try-only; there is no unbounded queue or thread-per-byte path.

| Bound | Value and effect |
| --- | --- |
| Live sessions | 16, including incomplete handshakes and idle sessions; excess connections close |
| Running or still-cleaning RPC supervisors | 8, at most one child each; excess requests close before spawning |
| RPC concurrency | One application request in flight per connection; separate connections can progress independently |
| Frame | 4-byte big-endian length; 1..1,048,576 payload bytes, matching MAX_FRAME_BYTES |
| Hello / ready / service record | At most 8 KiB encoded each |
| Pin | At most 4 KiB encoded |
| Read scratch / decoder | 8 KiB scratch plus at most one frame and prefix; no accumulated frame queue |
| Application output | Child stdout at most 1 MiB + prefix, stderr 256 KiB; final wrapped reply at most 1 MiB |
| Handshake / partial-frame deadline | 5 s from accept / first byte; not refreshed by trickled bytes |
| Idle session expiry | 60 s with no request; a running long-poll is not idle |
| Application deadline | 30 s from completed-frame admission; existing handler-specific long-poll caps remain |
| Client setup budget | At most 5 s, taken from the caller's existing RPC budget; skip cold setup if that budget is at most 5 s |

These are resource and hang guards, not speed assertions. Decoder allocation follows length validation, including zero, overflow and oversize rejection (`src/controller/protocol.rs:199`). Worst-case retained request/reply/capture bytes and supervisor counts must be measured; JSON representations and allocator overhead are additional to wire bytes.

Cancellation uses **connection close**, with no cancel wire message. EOF, unexpected inbound bytes during a request, malformed framing, deadline or leader shutdown close that session and set its child cancellation flag. Do not let an old reply enter a new session. Cancellation is transport cancellation, not `task.cancel`, rollback, a definitive mutation rejection or a promise that an accepted operation had no effects. Published mutations remain recoverable by the existing leader (`src/controller/store.rs:553`).

A supervisor slot is released only on a fully captured ProcessResult, when the existing runner has joined its I/O threads. Stuck work keeps its slot. The runner can abandon capture/stdin threads after error/cancellation (`src/process.rs:401`, `src/process.rs:437`), without exposing their completion. Therefore any runner Err or supervisor panic conservatively retains that slot for this leader lifetime, even when cleanup probably succeeded. Never replenish these slots or restart the listener inside the same leader to evade the bound. Exhausting the eight slots withdraws availability and routes clients to stdio until leader restart. This can retire the optimization after eight faults/cancellations; it avoids a new process-cleanup API while bounding abandoned I/O work. Do not unconditionally join blocked supervisor/capture threads from the leader's runtime, tick or shutdown path. Existing synchronous filesystem work and child.wait can stall in the kernel (`src/process.rs:447`, `src/process.rs:494`); no userspace deadline promises to interrupt a syscall. Shared state locks/disk can still delay the tick just as with an existing SSH RPC. Isolation prevents a handler panic/stall from taking down the leader's control loop; it is not independent-disk availability.

On shutdown: stop acceptance; withdraw the matching service record; close session streams; cancel all owned child groups; finish or detach supervisor bookkeeping without an unbounded join. Keep the existing tick shutdown/join ordering and leader guard lifetime (`src/controller/runtime.rs:97`, `src/lib.rs:1738`). Remove only the listener inode this generation created. No queued request is admitted after shutdown begins. Graceful tests prove group cancellation and admission cessation with barriers; hard-kill/restart tests prove same-ID recovery and stale-socket behavior. The listener never takes StateLock, QueueLock, drain, runner-log or per-request locks; those remain in the child, in their existing order (`src/controller/store.rs:516`, `src/client_state.rs:3386`).

## Decision 4 — private controller socket and proven stale cleanup

Controller layout is `controller_state_root()/rpc/`: directory 0700, socket `s` 0600 and private regular `service.json` 0600. It is outside the closed client-state namespace (`src/paths.rs:54`). The leader binds only its own account's socket. Validate the accepted local peer euid equals the controller account, using macOS getpeereid; sshd's stream-local endpoint uses that account. Peer uid is a same-account check, not laptop identification.

`service.json` has schema 1, the service identity in Decision 6, and socket evidence: parent and socket device/inode/uid/type/mode. No task data, secrets or request bodies. Publish it only after the accept loop is ready, with existing rooted no-replace/exact replacement and fsync/rename semantics (`src/rooted_fs.rs:2216`, `src/rooted_fs.rs:1831`). Retain rooted directory/entry bindings before and after pathname bind/connect; Unix bind itself has no portable bindat in this code. All unlink is descriptor-relative and exact-identity checked. Do not use process-global chdir or change a running multithreaded process's umask. Create the private directory before bind and enforce/verify 0600 immediately before advertising.

Startup holds the leader lock, then opens the owned `rpc` directory without following symlinks. If `s` exists, unlink it only when **all** of these hold:

1. Parent is the retained account-owned 0700 directory and entry is an account-owned 0600 socket.
2. A valid private prior service record pins exactly that parent/socket identity and canonical `rpc/s` path.
3. The prior ProcessIdentity is absent/reused, the new leader holds controller.lock, and a bounded connect proves ECONNREFUSED. A timeout, access error, successful connect or ambiguous process observation is not stale proof.
4. Recheck the same evidence immediately before descriptor-relative unlink and again afterward. A swapped entry is preserved.

If the socket has no complete creation/service evidence, including a crash between bind and publication, **leave it in place and disable this optional channel** with a stable diagnostic. This conservative loss of optimization is preferable to a new recovery journal for a socket. Missing socket with a safely readable stale record is recoverable by exact record replacement. Never delete a regular file, FIFO, symlink, wrong-owner socket, listening socket or unknown residue. Socket setup failure leaves leader recovery and stdio RPC available. Administrative residue removal is a deliberate local operator inspection, not an automatic "reset everything" command.

Rejected alternatives: blind unlink by filename; use the current transport control-directory scan as ownership evidence (`src/transport.rs:1032`); put the listener in shared /tmp; store it inside ClientStateStore; invent another durable cleanup transaction framework.

## Decision 5 — on-demand ControlMaster forward; no laptop daemon

Evaluate the existing master first. OpenSSH supports socket-to-socket `-L` and `-O forward` / `-O cancel` against an active master. Therefore `ssh -S <private-control-path> -O forward -L <local.sock>:<remote.sock> -- <configured destination>` can carry this channel without an additional persistent SSH process. A short control invocation is still required. ([OpenSSH ssh manual](https://man.openbsd.org/ssh#O), [forward syntax](https://man.openbsd.org/ssh#L)). The existing RPC bootstrap already uses ControlMaster=auto, the private %C namespace and ControlPersist=60 (`src/transport.rs:943`, `src/transport.rs:979`).

**Choose that mechanism when `[ssh] multiplex = true`.** Bootstrap through the raw existing stdio runner first; then request the forward on exactly the same worker SSH configuration/control namespace. With multiplex off, no usable private master, unsupported stream-local forwarding or failed control command, use stdio. Do not start a dedicated tunnel as a second fallback. This preserves the existing default-off setting (`src/config.rs:32`) and gives the configured fleet the requested warm-RPC optimization without another long-lived process.

| Candidate | Lifetime / ownership | Cost / failure behavior |
| --- | --- | --- |
| Existing master + on-demand forward | Foreground CLI owns one unique forward; graceful exit cancels only that forward. Master remains shared under existing ControlPersist policy. | Short setup/cancel control calls, no new persistent process. Master expiry/loss triggers fresh bootstrap or stdio. **Chosen.** |
| Dedicated `ssh -N -L` | CLI must own, signal and reap a persistent child. Unlike the dashboard's remote viewer, an -N forward has no remote stdin-consuming process to implement its EOF lifetime. | Adds a connection/process even with multiplex enabled; hard parent death needs another lifetime mechanism or accepts an orphan. Deferred. |

The master applies its own forward options to a mux forward, so the -O invocation cannot be assumed to replace those settings ([OpenSSH mux implementation](https://github.com/openssh/openssh-portable/blob/master/mux.c), `mux_master_process_open_fwd` using `options.fwd_opts`). Set `StreamLocalBindMask=0177` and `StreamLocalBindUnlink=no` when creating mac-worker masters; also pass them on control requests. Use **no** because a fresh command-private socket name is empty and stale cleanup is explicit; OpenSSH must not unlink an unverified existing entry. Keep restrictive child-only umask 077 for spawning a master/control subprocess, BatchMode=yes, ForwardAgent=no, ExitOnForwardFailure=yes and existing 10 s/3 keepalives. Forward requests must omit ClearAllForwardings=yes, which ordinary exec requests retain (`src/transport.rs:938`). Mode and handshake validation still apply to masters started by an older binary. ExitOnForwardFailure is not proof of the ultimate socket endpoint; ready/identity verification is mandatory. ([OpenSSH configuration manual](https://man.openbsd.org/ssh_config#StreamLocalBindMask), [forward failure semantics](https://man.openbsd.org/ssh_config#ExitOnForwardFailure)).

The T1 gate adds a narrow `ProcessRunner::run_private_interruptible` hook: its fake/default implementation delegates to run_interruptible, while SystemProcessRunner sets umask 077 in the child pre-exec path. Borrowed and Arc runner wrappers delegate every method, preserving this hook. Bootstrap and control calls use it. There is no ProcessRequest schema field, shell wrapper or process-wide parent umask change. Existing normal run/session behavior is unchanged (`src/process.rs:64`, `src/process.rs:86`, `src/process.rs:158`).

Extend the existing transport constructor privately; preserve -F selection and origin/worker separation (`src/transport.rs:894`, `src/transport.rs:1001`). Do not recreate or broadly scan/delete control sockets, parse credentials, alter the user's SSH trust or send -O exit to a shared master. The channel validates its own directories and treats an unusable control namespace as fallback. Dashboard's dedicated TCP/viewer tunnel remains unchanged (`src/transport.rs:879`).

Laptops use `controller_cache_root()/channel/`, private 0700. Each foreground command creates a unique `c<16 lowercase hex random digits>/` directory with mkdir-no-replace, and forwards to its `s` socket; collisions retry allocation, never reuse an entry. Pins use a separate subdirectory (Decision 7). This compact, command-local path avoids the 64-digit route hash in a sockaddr path. Validate both local and authenticated remote absolute UTF-8 paths: no NUL/control/colon characters and **fewer than 104 bytes** (`src/transport.rs:1058`). Long/custom XDG paths fail to stdio; no shared /tmp shortening or hashing broker is introduced.

Validate owner, 0700 directory, 0600 socket, socket type and device/inode before connect, after connect and during cleanup. Retain exact bindings; never chmod or adopt an unsafe existing entry. Concurrent commands have disjoint forwards and sessions; there is no cross-process laptop connection broker or lease cache. One foreground command owns one session and one forward at a time. Cancel uses the captured control path and exact -L pair, then removes only its retained socket/directory identity. If cancel fails, leave uncertain residue; do not kill the shared master. On hard CLI death, its streams close, server EOF cancels the current request, and the master follows existing idle expiry. A leftover forward cannot be reused by another command. Do not promise deletion at exactly 60 s while other SSH clients keep the master alive; stale command directories can require inspected cleanup. This is bounded allocation per command, not automatic cache GC.

## Decision 6 — authenticated stdio identity bootstrap and session handshake

Add a **read selector**, never a new top-level wire command:

```json
{"command":"task.list","body":{"controller_socket":{"op":"identity","route_sha256":"<64 lowercase hex>"}}}
```

It uses the ordinary version/request_id/body envelope. Recognize controller_socket before event/health/list dispatch and before ControllerStore; require exactly that selector, op and route digest. Mixing selectors, filters or unknown keys rejects without an active receipt/req row. N-1 task.list already rejects unknown keys before list (`src/controller/read.rs:446`); unknown top-level commands instead reach durable preparation (`src/controller/execute.rs:624`, `src/controller/store.rs:523`). New discovery followed by old execution is safe regardless of a cached feature list.

The configured route is SHA-256 of canonical JSON `{schema_version:1,ssh,remote_binary,ssh_config_file}` using the literal validated destination/binary and an absolute optional SSH config path. This binds local configuration, not DNS, resolved host keys or file contents. SSH still authenticates the selected host/account. The selector echoes route_sha256; the service cannot independently interpret a laptop SSH alias. Document that echo as context binding, not extra authentication (`src/config.rs:68`, `src/transport.rs:894`).

Reply is an unchanged `ControllerReadReply<SocketIdentityResult>` (`src/controller/read.rs:25`). The new result is either `type:available` with route_sha256 and the following service identity, or `type:unavailable` with a fixed code and no controller.socket feature. New result objects tolerate additive reply fields; required values, duplicate keys, sizes and identifiers are validated.

| Field | Meaning / verification |
| --- | --- |
| protocol_version | Exactly 7, also checked in the existing read envelope |
| channel_version | Exactly 1; independent additive session grammar |
| controller_client_id | Existing canonical ClientId from the controller's state/client-id, never the laptop ID (`src/client_state.rs:6142`, `src/controller/events/task_reads.rs:851`) |
| account | Controller euid, username and absolute home, from existing host_identity (`src/controller/init.rs:654`, `src/protocol.rs:503`) |
| leader | Exact ProcessIdentity, pid plus start_time_micros, matching leader.json and the live lock holder (`src/job.rs:687`, `src/controller/health_read.rs:213`) |
| service_generation | Fresh UUID v4 for each successful service start/bind, distinct from journal_id; never persisted as stable host identity |
| journal_id | Validated persistent journal UUID from JournalReader::window; survives leader restart (`src/controller/events/journal.rs:355`, `src/controller/events/journal.rs:426`) |
| socket_path | Validated canonical absolute path to this service's rpc/s, tied to the service record's socket evidence |
| features | Sorted, unique, bounded list including controller.socket only when served; common binary features remain separate from leader health |

Identity read is read-only: read an existing client-id, existing service record and existing journal. It must not create a state store, journal, pin on the controller or request receipt. Verify current account, client ID, journal ID, retained service/socket bindings, live leader and a local hello/ready probe before returning available. Use the existing lock-plus-ProcessIdentity health check instead of PID-only liveness (`src/controller/health_read.rs:175`). An uninitialized/unavailable journal disables socket startup/discovery, not healthy stdio state reads. No nullable/fabricated journal UUID. Running services retain their original journal binding and never silently adopt a new epoch.

Before **every new socket connection**, obtain a fresh identity over authenticated stdio and compare/write the laptop pin. Do not bootstrap using the forwarded socket or a disk-cached generation. Then the first socket frame is:

```json
{"kind":"hello","channel_version":1,"route_sha256":"<64 hex>","expected_service":{"protocol_version":7,"channel_version":1,"controller_client_id":"<32 hex>","account":{"uid":501,"username":"controller","home":"/Users/controller"},"leader":{"pid":1234,"start_time_micros":123456},"service_generation":"<UUID>","journal_id":"<different UUID>","socket_path":"/Users/controller/.local/state/mac-worker-controller/rpc/s","features":["controller.events","controller.socket","controller.task-logs-wait"]}}
```

The server compares all expected service identity fields to its captured service (features may contain unknown additive names but must match this snapshot), checks same-account peer and replies `{"kind":"ready","route_sha256":...,"service":...}`. Client independently verifies every field against the authenticated snapshot and the pin before sending an application request. Reject unknown hello fields/duplicate keys; ready can tolerate additive fields, never missing identity. There is no bearer token or cryptographic challenge: service generation guards accidental/stale endpoint reuse within existing SSH/same-account authority. Same-uid compromise can rewrite metadata and is outside that authority boundary.

A restart between bootstrap and hello closes/refuses the handshake. Disable that attempt, use stdio, and re-bootstrap on a later eligible request. Never accept the new generation merely because it answered the socket. Journal generation changes obtained from a fresh trusted stdio read use the events wave's existing cursor reset/repair; ordinary leader restart is not a journal reset (`src/controller/events/client.rs:135`).

## Decision 7 — minimal private pin, deliberate reinstall rotation

Pin location: `controller_cache_root()/channel/pins/<full-route-sha256>.json`, directory 0700/file 0600. Format:

```json
{"schema_version":1,"route_sha256":"<64 hex>","controller_client_id":"<32 hex>","account":{"uid":501,"username":"controller","home":"/Users/controller"}}
```

No generation, leader PID, journal epoch, features, timestamps, tasks or tokens belong in the stable pin. These live in the fresh in-memory bootstrap snapshot. Pin validation is strict and bounded at 4 KiB, requires one private regular link and retained owner/type/inode/directory bindings, and compares the complete stable identity. Reuse rooted no-replace creation and exact-content replacement (`src/rooted_fs.rs:1383`, `src/rooted_fs.rs:1831`, `src/rooted_fs.rs:2216`). Concurrent first bootstrap rereads the winning pin and requires equality; disagreement does not replace it.

Missing pin is created only from the verified stdio identity result and durably published before any socket application request. An unreadable, stale, conflicting or unsafe pin disables the channel, emits a bounded/redacted reason once per command, and **does not get overwritten automatically**. Fallback still uses the existing configured SSH route and its existing reply/ownership checks; "fail closed" here means no socket RPC to an unexpected identity, not removal of the already-authorized stdio transport. This explicit distinction reconciles identity failure with the required stdio fallback.

Ordinary binary upgrade/restart and journal replacement do not rotate the pin. Legitimate controller state reinstall/new client-id or deliberate account migration needs operator repinning:

1. `worker controller channel identity --json` performs authenticated stdio only and prints the observed stable/volatile identity or safe unavailable reason.
2. Operator checks the intended route/account and runs `worker controller channel repin --expect-client-id <observed canonical ClientId>`.
3. Repin performs another authenticated stdio identity read, verifies the expected client ID and live service, then exact-replaces the safely bound old pin (or creates a missing one). It prints the new stable identity. It does not connect through an old forward, mutate config/trust, erase operation envelopes/events cache or reset controller state.

A damaged/unsafe pin cannot be forced away by this command; local inspected filesystem repair is required. No interactive prompt, auto-repin on restart, or general delete/reset CLI is added. Repin grants authority for this configured route; it does not bless a socket-learned identity.

## Decision 8 — sequential session framing and unchanged inner RPC DTOs

Every session frame uses the existing four-byte big-endian length encoding and MAX_FRAME_BYTES. The new decoder extracts one frame at a time without an EOF check; its push result reports consumed bytes so callers cannot accumulate a pipelined backlog. The strict stdio `decode_frame` and `read_frame` remain unchanged (`src/controller/protocol.rs:90`, `src/controller/protocol.rs:105`).

States are `hello -> ready -> request -> reply -> request ... -> closed`. After ready, requests are the **existing ControllerRequest JSON**, with no new fields, using parse_request for duplicate-key rejection, protocol, request ID and server-computed digest (`src/controller/protocol.rs:127`, `src/controller/protocol.rs:211`). No pipelining: bytes arriving while a request/reply is in flight close and cancel the session, even if they are a well-formed second request. That first request may already have effects. Request IDs and digest semantics are unchanged; same-ID replay is legal and a changed body conflicts (`src/controller/store.rs:516`).

Socket replies add one channel-only wrapper to carry child exit status and identity even for a HostControlError:

```json
{"kind":"reply","request_id":"<32 hex>","payload_sha256":"<64 hex>","exit_code":0,"payload":{"protocol_version":7,"command":"task.list","request_id":"<same ID>","payload_sha256":"<same digest>","result":{}}}
```

`payload` is the unchanged JSON child reply, not text/base64 and not a new strict old DTO. Child status becomes the same ProcessResult classification input as stdio; do not discard a nonzero exit status. Before exposing that result, verify wrapper kind, request ID/digest, integer exit code 0..255, unique keys and one complete payload. Existing inner ACK/envelope/payload checks **still run**, including task/turn/log range/cursor identity (`src/controller/execute.rs:809`, `src/controller/read.rs:69`, `src/controller/events/client.rs:132`). A frame with a valid rejection is an application outcome, not channel loss. A signalled child, malformed/empty stdout or missing wrapper is transport ambiguity.

The wrapper is included in the 1 MiB limit. If a valid child reply plus wrapper exceeds that limit, close the channel and obtain the existing full-size reply via stdio; never truncate or raise the old frame limit. This uncommon fallback is an accepted cost of reusing the bound. Preserve the original request frame for that replay. Test the exact boundary and ensure the optional channel cannot prevent a maximum-size stdio reply.

No request queue, response multiplexer, RPC batch format, cancellation frame or server push is added. Long-polls occupy only their connection/child slot, leaving other sessions independent; saturation uses stdio rather than a priority scheduler. The client uses a nonblocking session checkout: another caller sharing the command while a long-poll is in flight uses stdio rather than waiting behind it. All session I/O checks the caller deadline/cancellation and closes on cancellation.

## Decision 9 — select all controller RPCs through one scoped adapter

Use a command-scoped `ChannelProcessRunner` around the existing raw ProcessRunner. It recognizes only the exact configured controller-rpc process shape and valid framed stdin, and delegates other processes unchanged. This keeps existing send functions, mutation classification/settlement and the separate event client on one transport decision (`src/controller/execute.rs:727`, `src/controller/execute.rs:842`, `src/controller/events/client.rs:52`). The adapter's bootstrap and -O operations always use the raw runner to avoid recursion. Public send signatures remain compatible; no ambient global connection cache.

| Caller / surface | Phase 3 selection | Existing anchor |
| --- | --- | --- |
| task status/list/logs/diff/result | Channel when eligible; logs long-poll payload/caps unchanged | `src/lib.rs:6068`, `src/lib.rs:6140` |
| task wait and submit/say/batch --wait polls | Same task.wait.poll and deadlines, now over a warm session; no event-assisted wait | `src/controller/lifecycle.rs:164`, `src/controller/lifecycle.rs:283` |
| events -f and notify discovery/read/tasks/repair | Same selectors, cancellation, cursor checks and reconciliation; same adapter via Arc runner | `src/controller/events/client.rs:31`, `src/controller/events/client.rs:58` |
| task submit/say/cancel/close/batch and controller retry | Existing envelope first, same-ID attempts and settlement | `src/lib.rs:1398`, `src/controller/execute.rs:728` |
| reconcile/publish-retry/drain/health and transfer prepare/finish | Channel transport; application policy unchanged | `src/controller/lifecycle.rs:187`, `src/controller/control.rs:50`, `src/controller/stream_rpc.rs:28` |
| controller channel identity/repin and bootstrap | Always raw authenticated stdio | Decision 7 |
| Git receive/upload-pack, host setup/service/probe, worker/origin SSH, dashboard HTTP/SSE | Existing transport, outside this channel | `src/transfer.rs:125`, `src/transfer.rs:131`, `src/dashboard/tunnel.rs:106` |

Local mode opens no channel/pin/client state for this optimization. Administrative install/restart verification uses raw stdio so it observes the replacement binary/leader directly (`src/controller/service.rs:648`). An unprepared channel is skipped for a remaining RPC deadline of at most 5 s; setup is charged to, and never resets, the caller's existing deadline. This protects short task.wait behavior and one-shot notify's 30 s budget (`src/controller/lifecycle.rs:170`, `src/controller/events/notify/follow.rs:57`).

The server accepts only the framed controller RPC command families already handled at this baseline: ordinary reads, lifecycle reads, controller.health/drain, transfer prepare/finish and the six durable commands listed in `src/controller/execute.rs:102`. Unknown commands close before child spawn; do not manufacture durable receipts for a shell-looking string. The adapter delegates unrecognized future requests to stdio. Neither the socket protocol nor hello selects a program, argv, path to execute, env, host operation or arbitrary shell.

## Decision 10 — fallback, reconnect and mutation ambiguity

For multiplex off, unsupported/unavailable service, unsafe/long paths, no master, forward failure, stale pin or handshake failure: send the original RPC through the existing stdio path. No socket application request has been sent in these cases. An unexpected identity cannot update a pin or proceed on the socket.

For mid-request channel loss: close the connection, cancel the server child best effort, retain the frozen request, and fall back once to stdio with the **same request ID, command, body and server-computed digest**. For mutations the envelope was already durable before entering the adapter. This stdio replay happens inside the current outer exchange attempt; subsequent ambiguous/resumable replies still use the existing four-attempt retry/backoff loop (`src/controller/execute.rs:729`, `src/controller/execute.rs:757`). There may therefore be one extra socket transmission before those bounded stdio attempts. No retry mints a new mutation ID, re-freezes a body, re-streams source or marks an unknown envelope rejected.

| Observed point of loss / reply | Required action |
| --- | --- |
| Before socket RPC bytes | Raw stdio fallback; original envelope still governs mutations |
| Partial/full request write, no reply, truncated/wrapped oversize reply, EOF, child panic/timeout | Outcome unknown; same-frame stdio replay while caller budget permits |
| Complete wrapper and verified ACK | Return ACK; existing acknowledged settlement, including cache-mark failure diagnostic |
| Complete wrapper and typed definitive HostControlError | Return rejection to existing classifier; settle rejected; no transport replay |
| Complete wrapper and resumable HostControlError | Existing same-ID retry; pending envelope remains resumable |
| Complete reply with wrong wrapper or inner request/turn/digest identity | Fail closed as unverified evidence; do not auto-replay that reply or settle the envelope |
| Caller cancellation / expired deadline | Close and stop; no automatic fallback after cancellation; mutation remains pending unless already definitively observed |
| Stdio fallback also loses reply | Existing bounded retries and final outcome-unknown/controller retry guidance |

An invalid identity-bearing reply is not reclassified as mere EOF. This preserves UnverifiedAck's current no-retry behavior (`src/controller/execute.rs:756`). Socket overload/decoder failure closes rather than emitting a faux definitive RPC rejection. Transport cancellation does not claim task cancellation.

This needs one explicit internal error classification seam: `WorkerError::ControllerUnverifiedReply(Box<WorkerError>)`, mapped to UnverifiedAck before the existing generic runner-error-to-Ambiguous branch (`src/controller/execute.rs:666`). Otherwise an adapter returning Err for a wrong outer ID would accidentally trigger mutation retries. T1 freezes this guard and its pending-envelope/no-retry test; it adds no error DTO or wire field. Correctly wrapped inner replies continue through the existing classifier unchanged.

Most reads are repeatable. Do not replay arbitrary future command families: explicit supported list only. The current non-envelope setters/recovery reads require special coverage: controller.drain is an idempotent desired-value setter (`src/controller/control.rs:1`); task.wait.poll/reconcile/publish-retry use their existing recovery semantics (`src/controller/lifecycle.rs:143`); transfer prepare/finish retain their existing identities/order (`src/controller/stream_client.rs:66`, `src/controller/stream_client.rs:101`). If a surface cannot be demonstrated safe for a lost-reply replay, keep that surface stdio-only and raise a contract issue before integration. No new mutation journal for these calls.

After ordinary channel failure, set an in-memory next-eligible instant with backoff 1, 2, 4, then 5 s. Requests before it use stdio immediately; the adapter does not sleep or change the application's poll cadence. At eligibility, at most one caller tries fresh stdio identity, forward and hello. Reset backoff after a verified application reply, not merely socket connect. Unsupported service or identity/pin/path safety failure disables attempts for this command lifetime; a new command or explicit operator repin can try again. No persisted backoff or reconnect daemon. Existing logs/notify outage policy stays outside this mechanism (`src/lib.rs:6218`, `src/controller/events/notify/follow.rs:125`).

## Decision 11 — feature advertising and N-1 matrix

Define a controller socket feature constant beside the existing registry; compose a sorted feature vector at discovery/health/ready time. Do **not** insert controller.socket into an unconditional static vector that would advertise it without a leader. The serving service record must pass owned binding, leader-liveness and hello checks. Startup error, stopped leader, stale record, missing journal or replaced binary omits the feature. An already-established service may carry ordinary state reads while an event read reports journal unavailability; never equate socket liveness with tick health (`src/controller/health_read.rs:48`, `src/controller/health_read.rs:139`).

| Pair / transition | Required behavior |
| --- | --- |
| Old laptop / new controller | Same stdio one-frame-plus-EOF path; unchanged strict request/read/status/log/wait/drain/ACK and service DTOs. Tolerant feature list can include the new string. |
| New laptop / old controller | New selector rejected by old task.list; no req row/active receipt; use original per-request SSH for the command. |
| New discovery / old executable at next request | Socket unavailable or identity bootstrap selector rejected; safe stdio fallback; no unknown top-level command. |
| New binary / old running leader | No owned served record/hello, no socket feature; stdio works until restart. |
| Old binary replaces child executable / new leader | Installed identity change withdraws socket; stdio and existing upgrade/restart checks decide application compatibility. |
| Leader restart / same client and journal | New ProcessIdentity/service UUID required via stdio; stable pin retained; event cursor retained. |
| Controller reinstall / changed client or account | Socket pin mismatch; stdio fallback; deliberate repin needed for future socket use. |
| Journal reset / same controller identity | Fresh stdio journal UUID, new service generation on restarted listener; event consumer repairs under its existing rules, no stable pin rotation. |

Strict persisted/wire DTOs and `PROTOCOL_VERSION = 7` remain unchanged (`src/controller/protocol.rs:30`, `src/controller/read.rs:25`, `src/controller/envelope.rs:34`). Session version 1 and new selector result types are additive. Common status/list/log/wait/drain shape regression tests must decode through baseline types, including controller-service's byte-canonical response treatment (`src/controller/service.rs:67`).

## Decision 12 — prove the saving with observations and failure fixtures

Phase 3 validation has two layers. Neither uses wall-clock speed assertions or sleep-based ordering; clocks, child-entry/exit hooks, channels and fake SSH provide deterministic correctness (`docs/testing.md:90`). Hang guards are at least 30 s. Only consolidated nextest area targets are run, with `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4`; tracks never run the whole suite (`docs/testing.md:6`).

**Local fixture benchmark:** one isolated controller layout/leader, identical cheap health and representative seeded task status/wait/log/event requests, fake-SSH stdio path versus forwarded Unix socket with real RPC children. Record 10 warmups and 200 measured exchanges per scenario, explicitly separate cold command setup (identity, pin, -O, hello), warm repeated exchanges, and fallback/reconnect. Measure per-exchange median/p95/mean, local SSH/fake-SSH launches, SSH control invocations, RPC children, framing bytes and max retained buffers/active supervisors. Zero-wait long-poll requests measure transport cost separately from requested idle waiting; injected event/log readiness measures correctness. Both serving paths start one worker child per exchange; the warm socket path starts no per-exchange SSH. Count-based assertions prove the structural saving; timings are observations, not pass/fail thresholds. Fake SSH does not measure network/auth/session negotiation, so do not present fixture milliseconds as live SSH latency.

**Post-deploy live measurement, only by the authorized integrator:** record laptop/controller commit/build identity, macOS/OpenSSH version, route mode, warm/cold state, client/account ID, ProcessIdentity, service UUID and journal UUID. Use the same disposable task and sample counts in paired runs with the channel available and stdio explicitly selected by a test/measurement injection, without editing application semantics. Report RPC end-to-end p50/p95/mean and setup cost; task.wait effective start-to-start poll cadence alongside its unchanged 100 ms sleep; quiet/busy logs, events and notify long-poll requests, child/SSH/control counts, CPU and reconnect/fallback reasons. Separate requested wait from transport/dispatch time. Observe restart/network outage recovery and mutation settlement on an approved disposable task. Numbers are observations; investigate lack of gain rather than publishing an unverified speed claim. Include the one-shot cold-start penalty and the remaining child/publisher/store cost.

No live benchmark, deployment, SSH, setup, launchctl, credential/keychain access, Herdr or real notification is authorized by this documentation task. The orchestrator owns independent reviews and the full integration gate. Docs/validation must distinguish completed local checks from pending live measurements.

## Decision 13 — interface gate, five parallel tracks, then integration

T1 commits all new session/identity/pin/resource contracts, runtime/cancellation interfaces, fakes, module declarations and seeded tests; production paths still use stdio. Then five disjoint tracks implement codec/session I/O, server/child supervision, identity/files/pin, master-forward control, and client/runner selection against the gate's fakes. No sibling concrete implementation is a parallel dependency. Integration wires the leader, selectors, feature advertising, CLI/runner scope and real fixture processes, then operator docs and acceptance follow.

Contract files freeze during the parallel wave. A needed signature or safety correction stops the affected track, reports `CONTRACT ISSUE: <track>`, and requires one serial orchestrator contract commit before dependents continue. No internal subagent/reviewer rounds; the orchestrator reviews independently. Each behavior gets a meaningful red/green test with a nonzero exact filter and a buildable conventional commit. Only the integration/docs owner leases predecessor files after their tracks finish. No changes to UI/assets, unrelated CI, other worktrees or the real pool.

## Deferred

| Cut | Reason / retained behavior |
| --- | --- |
| Phase 4 phone/Tailscale and Phase 5 per-mini daemons | Explicit owner exclusion; SSH account authority and single existing controller leader remain |
| Laptop LaunchAgent, connection broker and cross-command reuse | Foreground ownership is sufficient; no daemon/token/lease service |
| In-process dispatch or a reusable RPC child pool | Preserving the survey's ten process assumptions outweighs speculative worker-start savings |
| Dedicated -N fallback / keep-master-alive process | One forwarding mechanism; multiplex-off/unavailable uses stdio |
| Pipelining, priorities, server push, cancel frames and arbitrary RPC batches | One in-flight request and EOF cancellation suffice; overload falls back |
| Durable socket cleanup journal / automatic unknown-residue deletion | Incomplete ownership evidence disables an optional optimization safely |
| Path shortening outside the private cache, custom crypto, host-key/token store | Existing SSH trust; long socket paths use stdio |
| New wait/event/notify policy, snapshots or paging | Phase 3 changes transport only; events-wave semantics and repairs remain |
| Git/data streams or dashboard tunnel consolidation | Different existing streaming/liveness boundaries; keep their current transports |

## Open questions and recommended defaults

These do not block the design track. The owner is asleep; the orchestrator should use each stated default unless review finds a concrete defect.

1. **Serving cost:** default to one existing RPC child per request. Do not upgrade to in-process handlers to chase an unmeasured win. Report child startup separately and reconsider only in a later reviewed design if it dominates.
2. **Forward availability:** default to existing ControlMaster only when multiplex is enabled. Preserve default-off config and stdio fallback; do not add a dedicated child or enable multiplex implicitly.
3. **Bounds:** default to 16 sessions / 8 supervisors, one in flight, 5 s handshake/partial/setup, 60 s idle and 30 s application guard. Runner errors/panics conservatively consume a slot until leader restart because capture cleanup completion is not exposed. Tune only after resource/latency observations; saturated/stuck slots use stdio, with no listener restart to replenish them.
4. **Journal requirement:** default to a validated non-null journal UUID before serving/bootstrap. Unsafe/uninitialized journal disables the optional channel, never authoritative stdio state reads.
5. **Pin scope / reinstall:** default to route digest + client/account only in a 4 KiB private pin; fresh generation/leader/journal in memory per connection. Reinstall requires authenticated `repin --expect-client-id`; no auto rotation or unsafe-file force deletion.
6. **StreamLocalBindUnlink / residue:** default **no**, explicit 0177 bind mask, command-private fresh paths and exact owned cleanup. Unknown controller residue disables socket startup; unknown laptop residue is preserved for inspected cleanup. No new cleanup transaction/broker.
7. **Lost-reply replay beyond durable mutations:** default to only the enumerated existing controller surfaces, with explicit recovery/idempotence tests for drain, wait/reconcile/publish-retry and transfer prepare/finish. Any failing surface stays stdio-only and is reported as a contract issue; do not invent another request journal.
8. **Measurement / release evidence:** default to the 10-warmup/200-exchange fixture observations plus a pending post-deploy paired live record. Structural SSH/session removal can be locally verified; speed benefit is claimed only after observed results. No test latency threshold and no live access in this track.
