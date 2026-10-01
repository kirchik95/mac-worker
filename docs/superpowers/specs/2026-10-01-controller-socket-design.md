# Persistent Controller Channel

Date: 2026-10-01

Status: Final Round 3 design for the T1 gate. The owner's Phase 3 approval, [Round 2 decisions D1–D10](../../../.briefs/p3-spec-round2.md) and [Round 3 decisions E1–E5](../../../.briefs/p3-spec-round3.md) govern this revision; those decisions are settled.

Code baseline: `0802421541679443e7d1982988a8c6482e5fdbe9` (`integ/ev-wave`). Recheck anchors against its final accepted head before T1 on `integ/p3`. Proposed APIs, limits and measurements below are implementation requirements, not implemented behavior.

Plan: [2026-10-01-controller-socket.md](../plans/2026-10-01-controller-socket.md). Evidence: [survey](../../../.briefs/p3-survey-report.md), [review R1–R9](../../../.briefs/p3-review-report.md), [coverage](../../../.briefs/p3-coverage-report.md), [re-review F1–F4](../../../.briefs/p3-review2-report.md). Current-code citations use `path:line` at the baseline.

## Purpose and boundaries

Remove repeated SSH execution sessions from **read loops within one foreground command**, retaining one existing `worker host controller-rpc` child per request. Eligible loops are task waits, task logs follow, events follow and notify. Standalone reads, mutations, operator setters, transfer RPCs, doctor, general health and administrative proof remain on raw stdio. A submit/say/batch operation with --wait performs its mutation and transfers on stdio, then may use the channel for its wait loop.

This supersedes the sketch in Decision 13 of [the events design](2026-09-30-controller-events-design.md): the owner approved implementation, so measurement is Phase 3 validation rather than a pre-build gate. Round 2 narrows its journal-handshake requirement: event cursor validation already enforces epoch identity end to end (`src/controller/events/client.rs:132`). Protocol stays 7 (`src/protocol.rs:5`); task/journal formats, retry policy, event eligibility/repair, wait cadence, Git and dashboard semantics remain (`src/controller/lifecycle.rs:164`, `src/controller/execute.rs:695`, `src/dashboard/events.rs:25`). Phase 4, Phase 5 and a laptop LaunchAgent are excluded.

## Decision 1 — reuse existing mechanisms and freeze read-loop selection

Existing mechanism: strict framed RPC and canonical request identity (`src/controller/protocol.rs:62`, `src/controller/protocol.rs:105`, `src/controller/protocol.rs:165`); read envelope/payload verification (`src/controller/read.rs:69`); multiplexed SSH and private control namespace (`src/transport.rs:943`, `src/transport.rs:979`); private controller cache (`src/paths.rs:64`); rooted private reads/publication/exact replacement (`src/rooted_fs.rs:1383`, `src/rooted_fs.rs:2216`, `src/rooted_fs.rs:1831`); tunnel reconnect examples (`src/dashboard/tunnel.rs:156`, `src/dashboard/tunnel.rs:329`). Reuse these.

Not found — checked: handler routing (`src/controller/execute.rs:568`), leader lifecycle (`src/lib.rs:1631`, `src/controller/runtime.rs:56`), transport constructors/socket handling (`src/transport.rs:868`, `src/transport.rs:1053`), identity (`src/binary_identity.rs:31`, `src/controller/health_read.rs:175`). No controller session listener, stable application pin, exclusive mux-forward lifecycle, loaded-image hard-link lease or public cleanup-completion result exists. Herdr's socket is a different endpoint (`src/herdr.rs:311`).

T1 freezes the following allowlist as both **call-site scope and request grammar**, not merely command-name matching:

| Foreground scope | Eligible requests | Basis |
| --- | --- | --- |
| Task wait / run wait / --wait / interrupt-settle wait | Valid task.wait.poll only | `src/controller/lifecycle.rs:164`, `src/controller/lifecycle.rs:283`, `src/lib.rs:2462` |
| task logs -f | Valid task.logs reads issued by that follow loop and its loop-specific health discovery | `src/lib.rs:6140`, `src/lib.rs:6160`, `src/lib.rs:6195` |
| worker events -f | task.list controller_events read/tasks/repair selectors and health discovery belonging to the loop | `src/controller/events/client.rs:84`, `src/controller/events/client.rs:149`, `src/controller/events/tail.rs:151` |
| worker notify, including the bounded one-shot loop | Same event selectors and loop-local discovery/health | `src/controller/events/notify/follow.rs:57`, `src/controller/events/notify/follow.rs:507` |

Mixed selectors, unknown operations and invalid bodies are ineligible. Loop health is exactly task.list with controller_health:true, not an arbitrary health command. The server accepts only the union of these read grammars. The client additionally requires its explicit loop scope; a one-shot task.logs with identical bytes is raw stdio. Logs follow without a wait-capable peer retains its existing polling behavior and fallback.

Always raw stdio: task submit/say/cancel/close/batch/checkpoint; controller retry; drain set **and observe**; task.reconcile; task.publish-retry; all transfer prepare/finish; one-shot status/list/logs/diff/result; events without follow; doctor/general health; setup/service/restart proof; identity/repin. No mutation/envelope transport changes or new unverified-mutation error classifier. Existing same-ID retries and settlement remain exactly in their current path (`src/controller/execute.rs:662`, `src/controller/execute.rs:727`). No new TOML setting, task schema or daemon.

## Decision 2 — existing child per request, launched from a pinned executable

The existing leader owns the listener only after ControllerLeader::acquire (`src/controller/leader.rs:12`, `src/lib.rs:1631`), including its LaunchAgent invocation (`src/controller/service.rs:330`). Every admitted application frame launches the existing host controller-rpc entry with fixed argv `--config <captured absolute config> host controller-rpc`, pinned HOME/XDG roots, exact framed stdin and EOF (`src/lib.rs:1755`, `src/lib.rs:1765`). Config **path** is captured; config contents still load per child, as today (`src/config.rs:47`). No handler dispatch in the leader.

At leader startup a bounded native control job captures current_exe's canonical installed executable path and establishes the **loaded image's** device/inode, comparing it with that installed pathname opened without following replacements. A pathname-only SystemBinaryIdentitySource stat is insufficient (`src/binary_identity.rs:31`, `src/binary_identity.rs:68`). On macOS, the proposed image source uses the main Mach-O header's mapped region and libproc PROC_PIDREGIONPATHINFO vnode identity, checking the returned region contains that header. Unsupported/unverifiable evidence disables the optional channel. This is existing OS image introspection, not per-RPC hashing. ([Apple process-info definitions](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/sys/proc_info.h), [dyld header API](https://github.com/apple-oss-distributions/dyld/blob/main/include/mach-o/dyld.h)).

Hard-link that inode into the private rpc directory as `e<service UUID without hyphens>`, with no replacement; verify retained source, link and loaded-image dev/ino all agree before advertising. EXDEV, source replacement during linking, unsafe mode/owner, unexpected destination or missing identity disables the channel and leaves stdio healthy. The executable is an owned regular executable, not a 0600 data file; **never chmod the hard link**, which would change the installed inode. The directory is 0700 and excludes other accounts. Store its exact path/binding in the service record. Children spawn only from that generation-specific private link. Installer rename/rollback onto the original worker pathname leaves the link running the old inode; the next leader pins its own image. No installer-fence coupling. Unknown old links are preserved; only a record-proven owned link is cleaned. Generation-specific names prevent a late old supervisor from executing a subsequent generation's reused pathname.

Detached runners use the captured **installed path**, never the generation link. T1 freezes `ChildRpcSpec.detached_runner_executable: PathBuf` and `DETACHED_RUNNER_EXECUTABLE_ENV = "MAC_WORKER_DETACHED_RUNNER_EXECUTABLE"`; the field is the canonical installed path verified at generation startup. T3 explicitly sets that environment input on every socket RPC child. T7a reads it in run_host_controller_rpc and injects the configured DetachedRunnerExecutor through the existing RunnerExecutor seam (`src/turn_runner.rs:83`, `src/controller/execute.rs:616`). With input present it replaces current_exe at detached spawn (`src/turn_runner.rs:185`); without input, stdio RPC/CLI/leader behavior stays unchanged. Socket-started runners execute the installed path, and later self-exec handoffs pick up its replacement just as stdio-started runners do (`src/turn_runner.rs:481`, `src/turn_runner.rs:597`, `src/turn_runner.rs:2466`). Generation links are never removed at RPC exit or shutdown. Native startup cleanup removes only exact-bound links older than the previous generation, with proven RPC exits and a last-use stamp (creation or later proven exit) older than ten minutes on the injected retention clock. The current and previous links remain protected; unknown proof or an incomparable clock epoch retains residue. Normally only those two links remain, but pending grace and uncertain crash residue take precedence over that count. This prevents macOS executable validation from losing a recently used pathname. Detached task groups are neither cancelled nor awaited for link cleanup. E1's installed-path lifetime correction from [re-review F1](../../../.briefs/p3-review2-report.md) is unchanged.

Use the existing process-group runner/capture implementation via the T1 cleanup-completion seam, stdout 1 MiB+4, stderr 256 KiB, application deadline 30 s (`src/controller/mod.rs:91`, `src/process.rs:131`, `src/process.rs:185`). Capture child stdout for its connection, never the leader log. Child panic/exit/cap/deadline affects that exchange; native supervisor panic is contained. Detached task runners remain outside the transient RPC group.

The [survey section 3](../../../.briefs/p3-survey-report.md) audit remains explicit:

| Assumption | Phase 3 treatment | Correct code basis |
| --- | --- | --- |
| 1. Cross-process flock exclusion | Each handler is a fresh OS process; supervisor acquires no authoritative state/request locks | `src/controller/store.rs:499`, `src/client_state.rs:3262`, `src/client_state.rs:3386` |
| 2. Request lifetime / stdio | One child, exact frame, EOF, capture and exit; leader stdout remains diagnostics | `src/controller/protocol.rs:105`, `src/lib.rs:1778`, `src/controller/service.rs:360` |
| 3. Process-scoped publisher | Existing optional child publisher and exit grace; leader publisher remains separate | `src/lib.rs:261`, `src/lib.rs:330`, `src/controller/execute.rs:603`, `src/lib.rs:1635` |
| 4. DeferredHints TLS | Hint guards/counters stay child-local; no reusable handler thread | `src/client_state/events.rs:16`, `src/client_state/events.rs:28` |
| 5. WaitDeadline TLS | Child CURRENT scopes unchanged; supervisor adds its independent transport deadline | `src/client_state/deadline.rs:16`, `src/controller/lifecycle.rs:170` |
| 6. Config / other local state | Per-child config/SSH/TLS load; captured launch paths and roots | `src/config.rs:47`, `src/transport.rs:826`, `src/rooted_fs.rs:276` |
| 7. Signals | Reuse the leader's single signal registration; no blocking channel work on that runtime | `src/lib.rs:1678`, `src/lib.rs:1683`, `src/controller/runtime.rs:81` |
| 8. Exit / panic | CLI exit mapping/forked preparation remain outside the leader | `src/lib.rs:1778`, `src/prepare_turn.rs:69` |
| 9. EOF framing | New decoder is socket-only; child read_frame still waits for EOF | `src/controller/protocol.rs:90`, `src/controller/protocol.rs:110` |
| 10. Clock / cancellation | Session EOF, deadline and shutdown actively cancel the owned child group; RPC's false event cancellation is not the leader flag | `src/controller/execute.rs:563`, `src/lib.rs:214`, `src/controller/runtime.rs:24` |

Child serving saves SSH invocation/session cost while retaining worker startup and handler cost. In-process dispatch and reusable worker pools remain deferred: they require changing all ten assumptions and do not belong to this read-loop transport.

## Decision 3 — bounded nonblocking runtime, cleanup evidence and ordered shutdown

Listener/session I/O is nonblocking Tokio on the existing current-thread leader signal runtime (`Cargo.toml:24`, `src/lib.rs:1678`). All blocking channel stat, image checks, bind, record write/withdrawal and rooted cleanup run on **one bounded native control job at a time**. Results/retirement reach the runtime through shared state/oneshots. Child execution uses at most eight native supervisor jobs. No Tokio spawn_blocking, unbounded blocking pool, unbounded queue or blocking filesystem call in hello/admission/runtime teardown.

| Bound | Requirement |
| --- | --- |
| Sessions | 16, including incomplete handshakes and idle sessions; try-only admission |
| Child supervisors | 8 running or uncertain-cleanup slots; at most one child each |
| Native thread accounting | Up to 8 supervisors + 16 capture threads + 8 stdin writers = 32 child-related threads, plus one channel control thread; uncertain work remains charged |
| Application concurrency | One request in flight per connection; no request queue |
| Frames / buffers | 4-byte big-endian length, payload 1..1,048,576; 8 KiB scratch and one retained frame/prefix |
| Smaller data | hello/ready/service record <=8 KiB each; stable pin <=4 KiB |
| Child capture / wrapper | stdout <=1 MiB+4, stderr <=256 KiB; complete wrapped payload <=1 MiB |
| Time guards | 5 s handshake/partial frame/setup/cleanup; 60 s idle; 30 s application |
| Client budget | Setup/fallback consume the existing per-call deadline; skip cold setup when <=5 s remains |

Guards are resource/hang controls, not speed assertions; measure retained wire/capture bytes separately from JSON/allocator overhead. Retain baseline 15 s client / 20 s server long-poll caps and application policies (`src/controller/read.rs:516`, `src/controller/events/contracts.rs:26`, `src/client_state/deadline.rs:95`). A running poll is not idle.

T1 exposes a narrow cleanup result from the existing runner using a companion **TrackedProcessRunner** interface; the three ProcessRunner methods and their return types stay intact (`src/process.rs:64`). Completed means the owned process was reaped, owned transient group is proven gone, and all capture/stdin threads joined (or no child ever existed). Error/cancel may be Completed. Unknown covers abandoned cleanup, missing proof and panic. Reuse the internal distinction between joining and grace-expired detach (`src/process.rs:401`, `src/process.rs:422`, `src/process.rs:437`, `src/process.rs:457`). Release a permit only on Completed; never solely on elapsed deadline or runner error. More than eight fully cleaned cancellations preserve availability; eight Unknown slots retire the channel for that leader lifetime. No replacement behind stuck work or same-leader restart to replenish unknown slots.

Connection close is request cancellation, not task.cancel, rollback or a definitive application rejection. EOF, extra request bytes, malformed frame, deadline or leader shutdown closes the session and drives actual owned-group termination. The client also keeps its **borrowed per-call should_stop** live throughout bootstrap/control/socket I/O; it need not equal command cancellation, Send, Sync or 'static (`src/process.rs:71`, `src/controller/events/client.rs:58`). Server context is owned and separate.

Shutdown must be driven **inside the still-polled runtime**: stop acceptance, close streams, set child cancellation and observe actual group/cleanup completion or a bounded Unknown outcome; schedule matching record/socket withdrawal and save RPC-exit proof on native control work, retaining the image for Decision 2's later startup/grace cleanup. One common before-unpoll finalization path runs on **every** block_on exit: signal, tick error, diagnostic-write error and diagnostic-channel closure (`src/controller/runtime.rs:86`, `src/controller/runtime.rs:88`, `src/controller/runtime.rs:90`). Capture the original loop result, await finalization, and preserve that result. Then leave block_on and perform the existing tick join, stopped-health write and leader-guard drop. Current run_tick_loop leaves block_on before setting its flag/joining (`src/controller/runtime.rs:95`); T7 adds the async hook with a compatibility wrapper for existing callers. A blocked tick must not prevent socket closure/cancellation. T7a's live-child barriers prove stream closure and actual group cancellation before the runtime is left on each exit path. Do not wait indefinitely for native control/kernel cleanup; unknown residue retains ownership evidence. Filesystem/child.wait syscalls can still stall (`src/process.rs:447`, `src/process.rs:494`); shared disk/state locks can delay the existing tick. No independent-disk availability claim. See E3 / [re-review F3](../../../.briefs/p3-review2-report.md).

## Decision 4 — private controller files and proven stale cleanup

Layout: `controller_state_root()/rpc/` directory 0700, `s` socket 0600, `service.json` regular file 0600, plus the private executable link from Decision 2. The directory is outside PathLayout.state's closed client namespace (`src/paths.rs:54`). Validate same-euid accepted peers with macOS getpeereid; this is account authority, not laptop identity.

Record schema 1 contains service identity, optional journal hint, retained parent/socket dev/ino/uid/type/mode and executable link evidence. Publish only after runtime readiness with rooted no-replace/exact replacement and fsync/rename (`src/rooted_fs.rs:2216`, `src/rooted_fs.rs:1831`). All channel filesystem operations run on native control work. Pathname bind/connect retain lineage before/after; unlink is descriptor-relative and exact. No parent chdir/umask, blind cleanup or adoption/chmod of unsafe residue.

Startup holds the leader lock. Unlink an old s only when its retained private parent/socket match a valid prior record, prior ProcessIdentity is absent/reused, a bounded local connect returns ECONNREFUSED, and exact evidence is rechecked immediately before/after unlink. Success, timeout, access errors or ambiguous process liveness are not stale proof. Executable cleanup additionally requires its own exact retained binding, proof that all socket RPC children of that generation have exited, older-than-previous generation order and the ten-minute last-use grace in Decision 2; missing exit proof or uncertain age retains the link. Detached runners use the installed path and impose no link-lifetime dependency. Never remove by prefix or name alone.

Missing creation evidence, including a bind/link-to-record crash gap, leaves residue and disables the optional channel. A safely read stale record with a missing socket can be exact-replaced. Unknown regular/FIFO/symlink/wrong-owner/live sockets are preserved. Stdio and leader recovery remain healthy; inspected local repair is an operator task. No cleanup journal, shared /tmp namespace or broad control-directory scan (`src/transport.rs:1032`).

## Decision 5 — concrete existing master and config-free control operations

Use existing ControlMaster only when multiplex=true; preserve default-off configuration (`src/config.rs:32`). Compare: a dedicated ssh -N -L adds persistent process/ownership and parent-death work, while the existing master supports on-demand Unix forwarding. Choose the master and stdio fallback, with no dedicated tunnel or laptop daemon. Dashboard's dedicated viewer/stdin heartbeat remains (`src/dashboard/tunnel.rs:156`, `src/transport.rs:879`).

**Resolve once per connection setup.** Run a bounded ssh -G using exactly the worker destination, original -F/trust/options and command-line master options of bootstrap. Read its single expanded controlpath; OpenSSH expands it before dumping configuration. Require a literal absolute UTF-8 path in the retained private worker control namespace, not none/empty/template, owner/type/mode evidence and <104 bytes. Reject '%'/'$'/control characters so a later -S cannot expand it again. For a not-yet-created master also budget its creation suffix below 104 bytes. Do not compute %C in Rust. ([OpenSSH expansion and config dump](https://github.com/openssh/openssh-portable/blob/V_9_9_P2/ssh.c#L1389)).

Pass that literal **-S** on authenticated stdio bootstrap/master creation while retaining the original worker -F, trust and ClearAllForwardings=yes; this makes the authenticated master endpoint the one later controlled, even if configuration changes. The managed file's final Host * / ControlMaster no / ControlPath none (`src/controller/provision.rs:536`) is overridden by explicit worker command-line ControlMaster=auto, ControlPath/-S and ControlPersist=60 (`src/transport.rs:943`). Decline optimization if resolution/evidence cannot be established.

After bootstrap validate the live concrete socket (same account, socket type, mode, parent/entry binding), then -O check/open/cancel use **-F /dev/null, literal captured -S**, no config files, and only the owned -L local:remote pair (check carries no forward). Config-defined LocalForward/RemoteForward/DynamicForward never enter these control messages. Changes to -F/alias between open and cancel cannot redirect teardown. No raw mux implementation, credentials parser or shared-master -O exit. ([OpenSSH mux forwarding](https://github.com/openssh/openssh-portable/blob/V_9_9_P2/mux.c#L1747)).

Set StreamLocalBindMask=0177 and StreamLocalBindUnlink=no on worker master creation; also supply control policy, BatchMode=yes, ForwardAgent=no, ExitOnForwardFailure=yes and existing 10 s/3 keepalives (`src/transport.rs:938`). Omit ClearAllForwardings only on the config-free control calls. The master's own bind settings govern listeners; old masters with unsafe socket modes decline. **No private-umask runner hook or new pre-exec policy.** ([OpenSSH listener options](https://man.openbsd.org/ssh_config#StreamLocalBindMask)).

Laptop cache is `controller_cache_root()/channel/`, private 0700. Allocate fresh `c<16 random hex>/s` via mkdir-no-replace; no reuse after collision. Local/remote/master socket paths use byte lengths <104, absolute UTF-8 without NUL/control/colon/'%'/'$'; long/custom roots use stdio. Pin subdirectory is separate. Validate local parent/socket owner, 0700/0600, type and dev/ino before/after connect and cleanup.

E5 makes a default cold-path limitation explicit: the current managed-config layout (`src/transport.rs:988`, `src/transport.rs:1001`) expands to `/Users/kirchik/.cache/mac-worker/ssh-<16hex>/<40hex>`, **94 bytes**. OpenSSH's **17-byte** temporary master-creation suffix gives **111**, exceeding sun_path[104]. Decline cold channel setup for that route; do not shorten paths in Phase 3. A pre-existing safe master whose literal endpoint fits remains eligible.

Close application streams **before** -O cancel. For an established/settled forward, exit 0 is not cancellation evidence: require a bounded local connect returning ECONNREFUSED plus exact retained bindings, then exact owned unlink/rmdir. T1's cleanup_if_refused has a **settled-producer precondition**: the master can no longer still create/listen for that open. An interrupted or unacknowledged -O forward with no demonstrable terminal result **always returns ForwardOpenFailure.disposition = Retained**. Never invoke refusal cleanup to prove such an open settled: bind-before-listen can return ECONNREFUSED with unchanged bindings while creation continues. Best-effort cancellation does not change that rule without terminal creation evidence. Preserve residue and permanently retire setup for **that foreground command**, including after many backoff advances. At most one uncertain allocation per command; no GC/broker/cleanup journal or new trait method. This applies E2 / [re-review F2](../../../.briefs/p3-review2-report.md); established-forward cancellation remains proof-based. ([OpenSSH cancel behavior](https://github.com/openssh/openssh-portable/blob/V_9_9_P2/mux.c#L2225)).

Healthy open data sessions keep the master active; listening forwards alone do not defeat ControlPersist idle expiry. Hard CLI death closes its streams, cancelling the current RPC by EOF; other users can keep residual forwards alive beyond 60 s. Never promise deletion at that interval. Graceful cleanup cancels only the captured pair and preserves the shared master.

## Decision 6 — authenticated identity and an optional journal hint

Identity bootstrap remains a read-only **task.list selector**, recognized before health/events/list/durable dispatch:

```json
{"command":"task.list","body":{"controller_socket":{"op":"identity","route_sha256":"<64 lowercase hex>"}}}
```

Use the existing protocol/request envelope; reject mixed/unknown selectors and op/route fields without receipts. Old task.list rejects unknown keys before state/list handling (`src/controller/read.rs:446`, `src/controller/read.rs:450`), unlike unknown top-level commands (`src/controller/execute.rs:624`). Reply is unchanged ControllerReadReply with a new available/unavailable result (`src/controller/read.rs:25`).

Route digest is SHA-256 of canonical `{schema_version:1,ssh,remote_binary,ssh_config_file}` (literal route/binary, absolute optional config path). Its echo binds context, not DNS, host keys or config contents; SSH supplies authentication. Never create identity through the forwarded socket.

| Field | Requirement |
| --- | --- |
| protocol_version / channel_version | Exactly 7 / 1 |
| controller_client_id | Read existing **PathLayout.state/client-id**, normally ~/.local/state/mac-worker/client-id; never controller_state_root and never load_or_create_client_id (`src/client_state.rs:70`, `src/controller/events/task_reads.rs:851`) |
| account | Existing host_identity euid/username/absolute home (`src/controller/init.rs:654`, `src/protocol.rs:503`) |
| leader | Exact live ProcessIdentity, pid/start_time_micros plus leader-lock evidence (`src/job.rs:687`, `src/controller/health_read.rs:175`) |
| service_generation | Fresh canonical non-nil UUID v4 string for each successful service; private image link belongs to it |
| socket_path / features | Canonical owned rpc/s and sorted unique bounded features including controller.socket only when served |
| journal_id | **Optional** canonical UUID string/null hint when available; never a channel availability/identity prerequisite |

Identity reader opens existing client/record/bindings/leader data, with a local hello proof; it creates no state, client-id, journal, request receipt or active row. Journal health/window is not queried as a serving prerequisite. Reuse the leader's available initialization UUID as an optional hint; missing/reset/unavailable journal never blocks ordinary wait/log channel traffic. Journal hint equality is excluded from service authentication. Events replies/cursors continue to enforce their actual epoch, reset and range (`src/controller/events/client.rs:132`); the hint is not cursor authority. This is the explicit D9 narrowing of the earlier sketch, avoiding coupling unrelated reads to journal health.

Before each new connection: resolve master, authenticated stdio identity, stable pin validation/durable bootstrap, owned forward, hello/ready, then application bytes. Hello carries kind, channel_version, route_sha256 and expected_service; ready carries kind, route_sha256 and service. Both include journal_id when known. All required service identity fields/features match the trusted snapshot exactly; journal hints may differ/be absent without failing the channel. Reject unknown hello keys, missing required fields, duplicate keys and bad sizes; additive ready/reply fields are allowed. UUIDs serialize/validate as **strings**, using explicit wrappers and existing patterns (`src/job.rs:40`); do not enable uuid serde features (`Cargo.toml:27`). If a journal hint exists it cannot equal the service UUID.

Restart between discovery and hello refuses that session and uses stdio; never adopt a socket-learned generation. Peer euid and fresh generation are same-account endpoint protections, not custom crypto. Same-uid compromise is outside the authority boundary.

## Decision 7 — stable private pin and deliberate repin

Pin: `controller_cache_root()/channel/pins/<full-route-sha256>.json`, directory 0700/file 0600, <=4 KiB:

```json
{"schema_version":1,"route_sha256":"<64 hex>","controller_client_id":"<32 hex>","account":{"uid":501,"username":"controller","home":"/Users/controller"}}
```

Strict private regular single-link and retained parent/inode/owner/mode validation; no volatile fields, tasks, tokens or timestamps. Read via read_private_regular (`src/rooted_fs.rs:1383`), create no-replace (`src/rooted_fs.rs:2216`), exact-replace (`src/rooted_fs.rs:1831`) with fsync/rename. Concurrent creation rereads the winning pin and requires full equality. Unexpected/unsafe/corrupt pins cannot auto-rotate; socket use fails closed while raw configured stdio remains authorized.

`worker controller channel identity --json` always reads authenticated raw stdio. `worker controller channel repin --expect-client-id <canonical ClientId>` reads fresh identity again, validates expected client/account/route, and exact-replaces a safely bound prior pin or creates a missing one. No prompt, force-delete, config/trust mutation or state reset. Unsafe pins require inspected local repair. Restart/journal replacement does not repin.

Notify cache **keeps its existing independent key** sha256(ssh || 0xff || remote_binary) and notify.lock (`src/controller/events/notify/cache.rs:520`, `src/controller/events/notify/cache.rs:606`). Do not relocate/rekey it to the new route pin, which includes ssh_config_file. Envelopes also remain separate and unchanged (`src/paths.rs:66`, `src/controller/envelope.rs:21`).

## Decision 8 — sequential bounded framing and complete-reply handoff

Keep existing 4-byte big-endian framing and MAX_FRAME_BYTES, with a new socket-only decoder returning consumed bytes and one payload; allocate only after validating length. Strict stdio read_frame/decode_frame keep EOF/trailing-byte rules (`src/controller/protocol.rs:90`, `src/controller/protocol.rs:105`, `src/controller/protocol.rs:199`). Parse existing ControllerRequest JSON with duplicate-key rejection and server-computed digest (`src/controller/protocol.rs:127`, `src/controller/protocol.rs:211`).

State: hello → ready → request → reply → request. One in flight; arriving request bytes while handler/reply is incomplete close/cancel, with no queue. **Reply completion is the handoff:** when final reply-byte write completes, transition to request-ready before processing readable next-request bytes. A client sending immediately after receiving a complete reply must succeed. Test simultaneous read/write readiness and an explicit reply-completion barrier so scheduling does not create false pipelining.

Channel-only wrapper:

```json
{"kind":"reply","request_id":"<32 hex>","payload_sha256":"<64 hex>","exit_code":0,"payload":{"protocol_version":7,"command":"task.list","request_id":"<same ID>","payload_sha256":"<same digest>","result":{}}}
```

Inner JSON/old strict DTOs stay unchanged. Preserve child exit code 0..255 in ProcessResult; signalled/missing/malformed output is loss. Validate wrapper identity/unique keys and, for non-error replies, the existing ControllerReadReply<Value>::verify_envelope before returning ProcessResult (`src/controller/read.rs:69`). Existing typed payload/cursor/turn/log checks still run downstream (`src/controller/events/client.rs:132`). Child stderr stays bounded and is not added to the wire. Complete verified errors are application outcomes; do not turn capacity/cursor errors into loss. Wrong complete identity is unverified evidence: stop that exchange with no automatic replay, without a new mutation classifier.

Wrapper counts toward 1 MiB. A maximum-size valid inner reply that cannot fit wrapped closes the channel and falls back to the intact stdio reply; no truncation or old limit increase. No batches, push, cancel frames, pipelining or response multiplexer. Separate sessions allow another read during a long-poll; concurrent checkout of a busy command session uses stdio without waiting behind it.

## Decision 9 — scope the adapter to each read loop

ChannelProcessRunner wraps a raw ProcessRunner only inside the four frozen loop scopes. Generic outer command routing does not wrap all RPCs. Ordinary wait/log loops use a borrowed raw runner; events/notify own an Arc runner because their current foreground setup bypasses the injected runner (`src/lib.rs:940`, `src/controller/events/client.rs:31`). Preserve public send signatures and supply no second signal listener. Borrowed should_stop remains live through every synchronous interface; command cancellation/deadline are additional checks.

Mutation/transfer phases before --wait and all excluded requests delegate byte-identically; never pass them through socket setup, pin or fallback logic. Local mode opens none of these resources. Loop health discovery is scoped; doctor, standalone controller status/drain and restart verification stay raw (`src/doctor.rs:52`, `src/controller/service.rs:648`). Existing 100 ms wait delay, logs outage/backoff, events empty-batch pause and notify repair/budget remain (`src/client_state/deadline.rs:95`, `src/lib.rs:6218`, `src/controller/events/tail.rs:104`, `src/controller/events/notify/follow.rs:57`).

Unprepared channel with <=5 s remaining uses stdio. Setup and its optional reconnect never reset the per-call deadline. No persistent laptop connection cache or daemon.

## Decision 10 — one read fallback, cancellation and bounded reconnect

Unavailable/unsupported service, invalid paths/master, pin mismatch or handshake failure uses raw stdio before application socket bytes. Mid-request loss closes the stream and retries the **same eligible read frame/ID/body/digest once** over stdio inside its original budget. Eligible task.wait.poll reconciliation converges under its existing loop; drain/operator-reconcile/publish-retry are explicitly excluded because replay can override intervening decisions (`src/controller/control.rs:1`, `src/controller/control.rs:76`, `src/controller/lifecycle.rs:143`). No channel mutation replay/envelope settlement interplay.

| Outcome | Action |
| --- | --- |
| EOF/partial reply/child failure/oversized wrapper | One same-read stdio fallback if live budget permits |
| Verified complete reply/error | Return original outcome; existing validators/application loops decide |
| Complete wrong identity | No automatic replay; retire the channel for this command; existing read error policy remains |
| Per-call should_stop / command cancel / expired budget | Close and stop; **no post-cancel fallback** |
| Unacknowledged/unsettled forward open, or uncertain settled-forward cancellation/cleanup | Retained: preserve one allocation and retire setup for this command, even after future eligibility instants; no refusal-cleanup proof for an unsettled open |

**Transmission bound:** each eligible read exchange has at most one channel application attempt plus one immediate stdio fallback. The adapter adds no retry loop. Any existing retry transmissions for that same exchange remain stdio; new poll/read calls may reconnect only under policy. Reconnect eligibility expiring inside an exchange cannot cause a second socket transmission. Identity/-G/control calls are separate setup work, counted in benchmarks. All mutation attempts remain their existing stdio transmissions.

Ordinary recoverable loss sets in-memory eligibility 1, 2, 4, then 5 s, with no sleeps. Before eligibility use stdio; one caller attempts setup; reset only after a verified application reply. Unsupported/pin/path/unverified-reply or uncertain cleanup retires setup for this command. Cleanup uses a separate bounded clock-only context after closing the session; it ignores an already-consumed foreground cancel without admitting application work.

Existing stdio mutation retry sleeps ~1/3/9 s (jittered) can elapse wholly inside launchd's 30 s ThrottleInterval; four attempts can still end outcome-unknown (`src/controller/execute.rs:757`, `src/controller/execute.rs:790`, `src/controller/service.rs:356`). Phase 3 neither widens that budget nor claims the socket fixes it, because mutations stay stdio. Raw envelope/retry/operator guidance remains authoritative.

## Decision 11 — live advertising and N-1

Add a controller.socket constant and compose features only for a verified owned live listener. Do not add unconditional HOST_FEATURES/CONTROLLER_FEATURES support (`src/features.rs:6`, `src/features.rs:7`). Record alone is insufficient; live hello/leader evidence is required. Disabled startup, unknown cleanup retirement or stopped leader omits it; missing/unhealthy journal alone does **not**. Health of the tick remains separate (`src/controller/health_read.rs:50`). Leader-side blocking probes run on native control work; stdio readers run in their own RPC children.

| Pair / transition | Behavior |
| --- | --- |
| Old laptop / new controller | Original EOF stdio and strict read/log/wait/drain/ACK/service shapes; tolerant features may include socket |
| New laptop / old controller | Unknown task.list selector rejects without receipts; loop stays stdio |
| New binary / old running leader | No owned served generation; stdio until restart |
| Installed binary replaced during a generation | Existing socket RPC children execute pinned old image; their detached runners and later handoffs use installed path like raw stdio; next leader pins new image |
| Leader restart | New ProcessIdentity/service UUID from trusted bootstrap; stable pin retained |
| Controller reinstall/account change | No socket requests on unexpected stable identity; raw stdio plus deliberate authenticated repin |
| Journal missing/reset while service runs | Wait/log traffic remains served; events validate/reset their own epochs/cursors; no stable repin |
| Rollback | Old installed stdio still works; any surviving generation uses its own private image; restart replaces generation |

PROTOCOL_VERSION remains 7 (`src/protocol.rs:5`). Old strict wire/persisted DTOs remain (`src/controller/protocol.rs:30`, `src/controller/read.rs:25`, `src/controller/envelope.rs:34`); byte-canonical controller-service responses remain raw (`src/controller/service.rs:67`).

## Decision 12 — measure S - O and complete command costs

Warm stdio cost model is S + W + H + D: SSH invocation/mux execution session, worker startup/config, handler/store/publisher and framing/data. Socket cost is W + H + D + O: added socket/supervisor/wrapper overhead. Expected saving is **S - O**; existing ControlMaster already removes TCP/authentication setup (`src/transport.rs:943`). No millisecond speedup is measured here.

Local ignored fixture benchmark: real RPC children and fake SSH/mux; 10 warmups and 200 samples/class, paired or alternating path order with comparable state. Include cheap wait/read and representative logs/events, zero-wait and deliberate-wait scenarios (label requested waiting separately). Measure complete **cold command lifetimes**, including identity, master resolution/open, pin verification/creation, hello, requests, cancel/evidence/cleanup. Include fallback and reconnect cases, process/child/control counts, frame/buffer/supervisor observations, mean/p50/p95. Assert correctness and structural counts only: zero per-warm-read SSH execution sessions and one RPC child/read. Fake timing is not live network/session timing.

Later authorized integrator: paired raw-stdio/channel runs with same disposable tasks and path-order alternation; record build/macOS/OpenSSH/route/client/leader/service/journal-hint identity, warm/cold state, total command/RPC latency, requested waits, task.wait start-to-start cadence beside unchanged 100 ms sleep, logs/events/notify long-poll process/CPU cost, fallback/reconnect and cleanup behavior. Separate S, O and remaining W/H. Whole-command break-even and no-gain results are observations. Mutation outage/retry behavior stays a separate unchanged stdio observation. Live deployment/pool/SSH/setup/launchctl/credential/Herdr/notification work is not authorized in this documentation track.

## Decision 13 — corrected interface gate and five parallel tracks

T1 freezes eligibility, UUID strings/optional journal hint, borrowed client cancellation versus owned server context, ChildRpcSpec's installed-runner path/environment input and generation-link withdrawal semantics, concrete master plan, settled-producer cleanup/Retained-open semantics and tracked runner cleanup. No private-umask hook, mutation classifier or envelope changes. Then five disjoint tracks: codec/I/O, server/native control/child, rooted identity/image/pin, concrete master forward, scoped read client. Each tests against T1 sibling fakes. T7 integrates serially in four buildable commits: leader/selector/features/runner injection, scoped loops/CLI, compatibility/recovery, ignored measurements. T8 documents acceptance and pending live work.

Frozen contract errors stop the affected track with CONTRACT ISSUE: <track> for a serial orchestrator correction. No internal subagent/reviewer rounds. Red/green exact nonzero filters and buildable commits; exclusive predecessor leases only after their tracks finish. The orchestrator owns independent review/full gate. Track T6 remains independent after narrowing: it owns client policy, not application routing or mutation semantics.

## Deferred

| Cut | Reason / retained behavior |
| --- | --- |
| Phase 4, Phase 5, laptop LaunchAgent/broker | Owner exclusion; same-account SSH/foreground ownership |
| Mutation, setter, transfer or one-shot optimization | D1: read-loop benefit only; existing stdio/retry/settlement remains |
| In-process dispatch, reusable child pool | Preserve all ten survey process assumptions |
| Dedicated -N, master keepalive helper | One existing-master mechanism; stdio fallback |
| Pipelines, priority queues, server push, cancel wire frames | One in-flight read and close cancellation suffice |
| GC, cleanup journal, automatic unknown residue deletion | Positive evidence or optional-channel retirement |
| Custom crypto/tokens, path broker/shared /tmp | Existing SSH authority and long-path fallback |
| Shorter SSH control directory | E5: default cold managed path can exceed sun_path[104] with the creation suffix and disable this optimization; shortening is an owner follow-up |
| Wait/event/notify policy, cache rekeying, dashboard/Git consolidation | Transport-only phase; existing semantics/keying |

## Open questions and conservative validation defaults

D1–D10 and E1–E5 are applied, not open for reconsideration in this track. Defaults: read-loop allowlist; pinned private RPC image with installed-path detached runners; concrete config-free mux operations; unsettled open always Retained and one uncertain allocation/command; common before-unpoll finalization on every runtime exit plus bounded native jobs; live borrowed cancellation; cleanup-proof permits; explicit SSH bind options without a private-umask hook; optional journal hint; unchanged old DTOs and retry policies.

The remaining questions concern validation, not alternative mechanisms. They do not block T1 or reopen D1–D10:

1. Resource observations: retain 16/8/1 concurrency and stated guards until measured resource/latency evidence supports tuning. No speed threshold or automatic tuning.
2. Loaded-image API availability on the fleet: use the verified hard-link design; unsupported/unverifiable image evidence disables that leader generation's optional channel, with stdio healthy.
3. Fleet SSH resolution/stream-local permissions: require reliable literal master resolution and owned private bindings; unsupported versions or unsafe evidence decline the route. Preserve unknown cleanup rather than add a broker/journal.
4. Whole-command benefit: use the required paired 10-warmup/200-sample fixture observations and pending authorized live measurements. Claim no empirical speedup until measured, including cold setup/teardown and requested wait.
5. Ordinary SSH multiplexing on the default cold managed path: does the same 94+17-byte limit affect its existing master creation? The orchestrator will check in the performance phase. Make no claim or fix here; Phase 3 conservatively declines cold channel setup when its creation path exceeds the limit, and control-directory shortening stays deferred to the owner.
