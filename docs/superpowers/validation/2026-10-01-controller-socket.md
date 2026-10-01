# Persistent controller read channel validation record

Written 2026-10-01. This record separates completed local evidence from the full gate and live
acceptance. A pending row is not a pass. No pool host, real SSH session, LaunchAgent, credential,
keychain, Herdr command, or notification was used by T8. CLI checks below are `--help` and
`--version` only.

## Identity

| Item | Value |
| --- | --- |
| Events baseline | `0802421541679443e7d1982988a8c6482e5fdbe9` `docs(controller): document event notifications and rebuild dashboard assets` |
| Final design / T1 starting head | `bad365b62b33753cf7f91d10475cd3b27add7cd2` `docs(controller): settle runner executable and forward-open cleanup` |
| Phase 3 implementation head before T8 docs | `8398b0a946a3547d4cf0b2b63e4d95b88a97220c` `test(controller): verify read channel compatibility and recovery` |
| T8 docs commit | `docs(controller): document the persistent read channel` (this commit) |
| Protocol / channel | protocol 7 / channel 1 |
| Locally built binary | `target/debug/worker`, `worker 0.1.0+8398b0a946a3-debug`; dev profile, not a release artifact |
| Locally built binary SHA-256 | `e22dd99a08f2636d7afdac87a35f1145f1fdcfa25a89dbafa8b3d8afeb5eaf87` |
| rustc | `1.98.1 (48a229cea 2026-09-01) (Homebrew)` |
| Fixture measurement build | macOS/aarch64, package 0.1.0, protocol 7, channel 1; copied worker SHA-256 `9e9999c653940b0f2cb5d3065723be4100ed34ef678eab4d7efef9837f913ba5` |

The binary was built before the documentation edits, from the exact implementation head above.
T7d and T7c changed tests only after T7b's production integration, but the build id still records
the current Git head.

## T8 build and CLI checks

| Command | Result |
| --- | --- |
| `CARGO_BUILD_JOBS=4 cargo build --locked --bin worker` | passed, exit 0 |
| `target/debug/worker --version` | exit 0; `worker 0.1.0+8398b0a946a3-debug` |
| `target/debug/worker --help` | exit 0; lists `controller` |
| `target/debug/worker controller --help` | exit 0; lists `channel` |
| `target/debug/worker controller channel --help` | exit 0; lists `identity` and `repin`; global `--json` is shown |
| `target/debug/worker controller channel identity --help` | exit 0; usage `worker controller channel identity [OPTIONS]`; `--json` is shown |
| `target/debug/worker controller channel repin --help` | exit 0; requires `--expect-client-id <EXPECT_CLIENT_ID>`; `--json` is shown |
| `cargo fmt --all --check` | passed, exit 0 |
| `CARGO_BUILD_JOBS=4 cargo clippy --locked --all-targets -- -D warnings` | passed, exit 0; no warnings |

No identity or repin operation was sent to a controller. The help output agrees with
[the operator guide](../../usage.md#identity-pin-and-controller-replacement).

## Track test evidence

These are the final nonzero targeted commands and counts recorded by T1–T7d. They ran at their
track heads, not again in T8. Selections overlap, so their counts must not be added into a
whole-suite total.

### T1 — contracts and cleanup evidence

| Command | Recorded result |
| --- | --- |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_socket_contracts::/)'` | 13 selected, 13 passed |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_socket_/)'` | 19 selected, 19 passed |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test transfer -E 'test(/^controller_socket_forward::/)'` | 1 selected, 1 passed (gate seed only) |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test cli -E 'test(/^controller_channel::/)'` | 1 selected, 1 passed (gate seed only) |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --lib -E 'test(/^process::tests::/)'` | 16 selected, 16 passed |

Commit: `44de71b` `feat(controller): freeze read channel contracts and cleanup evidence`.

### T2 — codec and synchronous socket I/O

| Command | Recorded result |
| --- | --- |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_socket_codec::/)'` | 33 selected, 33 passed |

Final track commit: `b09b357` `refactor(controller): adapt codec sessions to frozen contracts`.

### T3 — service, native control, and child supervision

| Command | Recorded result |
| --- | --- |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_socket_service::/)'` | 28 selected, 28 passed before and after review fixes |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --lib -E 'test(/^controller::channel::server::tests::/)'` | 3 selected, 3 passed before review; superseded by the post-fix server selection below |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --lib -E 'test(/^controller::channel::server::/)'` | 5 selected, 5 passed after A1/A2 |

Track commit: `7dd24dd` `feat(controller): supervise pinned read RPC children`. Review fixes:
`7756a75` and `1477e63`, recorded under [Review passes](#review-passes).

### T4 — image, files, identity, and pin

| Command | Recorded result |
| --- | --- |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_socket_identity::/)'` | 33 selected, 33 passed |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --lib -E 'test(/^rooted_fs::tests::.*channel_socket/)'` | 8 selected, 8 passed |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --lib -E 'test(/^controller::channel::image::core::tests::/)'` | 4 selected, 4 passed |

Final track commit: `5b3fc3a` `feat(controller): pin service images and channel identity`.

### T5 — concrete master and owned forward

| Command | Recorded result |
| --- | --- |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test transfer -E 'test(/^controller_socket_forward::/)'` | 14 selected, 14 passed at T5 |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --lib -E 'test(/^transport::tests::.*(multiplex|forward|managed|ssh_argv)/)'` | 7 selected, 7 passed |

Final track commit: `eb923df` `test(transport): adopt frozen channel contracts and fixtures`.
The 94+17=111-byte cold managed-path case is included in the first selection. T7c's later combined
forward/transport filter, which includes the leased integration cases, is recorded as 20/20 below.

### T6 — scoped client, fallback, and retirement

| Command | Recorded result |
| --- | --- |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_socket_client::/)'` | 41 selected, 41 passed at the track head; 43 selected, 43 passed after B1 |

Track commit: `198587d` `fix(controller): preserve scoped read capture policy`. Review fix:
`a54a798` `fix(controller): keep every channel-attempted read on stdio`.

### T7a — leader, selector, feature, and shutdown integration

| Command | Recorded result |
| --- | --- |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_socket_wiring::.*(leader|selector|feature|shutdown|image|journal)/)'` | 15 selected, 15 passed |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_event_rpc::|^controller_event_wiring::|^controller_event_notifier::|^controller_features::|^controller_health_routes::|^controller_health_runtime::|^controller_service::/)'` | 164 selected, 164 passed |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_drain::|^controller_drain_attached::|^controller_drain_rpc::|^controller_publish_retry::|^controller_retry::|^controller_task_mutations::|^controller_transfer::|^controller_streamed_submit::/)'` | 68 selected, 68 passed |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_socket_service::/)'` | 28 selected, 28 passed |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --lib -E 'test(/^controller::runtime::tests::|^process::tests::/)'` | 16 selected, 16 passed |

Commit: `3b11fba` `feat(controller): serve pinned read channel generations`.

### T7b — foreground loop and operator CLI integration

| Command | Recorded result |
| --- | --- |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_socket_wiring::.*(loop|raw|route)/)'` | 16 selected, 16 passed; includes the three required named routing families |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test cli -E 'test(/^controller_channel::|^cli_help::/)'` | 39 selected, 39 passed |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_socket_wiring::|^controller_lifecycle_compat::|^controller_say_wait_exit::|^controller_say_interrupt::|^controller_read_routes::/)'` | 76 selected, 76 passed at T7b; later expanded to 95/95 by T7c |
| Event/feature/health/service command shown under T7a | 164 selected, 164 passed |
| Drain/retry/mutation/transfer command shown under T7a | 68 selected, 68 passed |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --lib -E 'test(/^controller_logs_tests::/)'` | 13 selected, 13 passed |

Commit: `b9fd472` `feat(controller): scope persistent reads to foreground loops`.

### T7c — compatibility, isolation, and recovery

| Command | Recorded result |
| --- | --- |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_socket_wiring::|^controller_lifecycle_compat::|^controller_say_wait_exit::|^controller_say_interrupt::|^controller_read_routes::/)'` | 95 selected, 95 passed |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_event_rpc::|^controller_event_wiring::|^controller_event_notifier::|^controller_features::|^controller_health_routes::|^controller_health_runtime::|^controller_service::/)'` | 164 selected, 164 passed |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_drain::|^controller_drain_attached::|^controller_drain_rpc::|^controller_publish_retry::|^controller_retry::|^controller_task_mutations::|^controller_transfer::|^controller_streamed_submit::/)'` | 68 selected, 68 passed |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test transfer -E 'test(/^controller_socket_forward::|^transport::/)'` | 20 selected, 20 passed |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test dashboard -E 'test(/^dashboard_tunnel_reconnect::|^dashboard_events::/)'` | 48 selected, 48 passed |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --lib -E 'test(/^controller::runtime::tests::|^process::tests::/)'` | 16 selected, 16 passed |
| `NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --lib -E 'test(/^controller_logs_tests::|^controller::execute::mutation_retry_tests::|^controller::execute::tests::mutation_retry_lost_acks/)'` | 25 selected, 25 passed |

Commit: `8398b0a` `test(controller): verify read channel compatibility and recovery`.
The transfer test-file lease was released with this commit.

### T7d — ignored paired fixture

```sh
NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 NEXTEST_PROFILE=ci NEXTEST_RETRIES=0 \
  cargo nextest run --locked --test controller --run-ignored only --no-capture \
  -E 'test(/^controller_socket_benchmark::fixture_transport_cost_observations$/)'
```

Result: 1 selected, 1 passed, 792 skipped; 18 JSON rows; 265.065 seconds. The
360-second nextest profile limit was a hang guard, not a speed assertion. Commits:
`213a039` `test(controller): observe paired RPC fixture lifetimes` and
`e8e1107` `test(controller): measure paired read channel fixture costs`.

Every implementation track also recorded passing `cargo fmt --all`/check,
`CARGO_BUILD_JOBS=4 cargo clippy --locked --all-targets -- -D warnings`, and
`git diff --check` where listed. T8's own final results are recorded in the T8 table.

## Review passes

The two independent Phase 3 reviews both returned **land after fixes**. Their baseline review
commands and reproductions were local, targeted checks; neither was a full gate.

| Review | Finding | Reproduction | Fix commit | Final targeted evidence |
| --- | --- | --- | --- | --- |
| p3-rev-b | B1, Medium: an interleaved read could overwrite the single same-read attempt marker and allow A to use the socket twice | exact temporary regression: 1 selected, 1 failed with two A socket frames instead of one | `a54a798` `fix(controller): keep every channel-attempted read on stdio` | T6 filter 43/43; bounded command-scoped attempted-ID evidence, conservative retirement on exhaustion |
| p3-rev-a | A1, Medium: queued early input could survive simultaneous read/write readiness while a reply was incomplete | exact temporary regression: 1 selected, 1 failed because the incomplete writer returned success | `7756a75` `fix(controller): reject early input before incomplete reply writes` | service 28/28; server selection 4/4 after A1 |
| p3-rev-a | A2, Medium: completed session tasks could accumulate outside the 16-session accounting | exact temporary regression: 1 selected, 1 failed after observing 17 retained tasks with zero active sessions | `1477e63` `fix(controller): bound retained session tasks during admission` | service 28/28; server selection 5/5 after A2 |

Review command record:

| Command | Result |
| --- | --- |
| p3-rev-a: `cargo nextest run --locked --test controller -E 'test(/^controller_socket_service::|^controller_socket_identity::/)'` | 61 selected, 61 passed |
| p3-rev-a: `cargo nextest run --locked --lib -E 'test(/^process::tests::|^rooted_fs::tests::.*channel_socket|^controller::channel::image::|^controller::channel::server::/)'` | 31 selected, 31 passed |
| p3-rev-b: `cargo nextest run --locked --test controller -E 'test(/^controller_socket_(contracts|codec|client)::/)'` | 87 selected, 87 passed on the restored tree |
| p3-rev-b: `cargo nextest run --locked --test transfer -E 'test(/^controller_socket_forward::/) & !test(real_openssh_mux_controls_ignore_config_forwards_and_edits)'` | 13 selected, 13 passed; the excluded local OpenSSH mux test was inspected but not executed by this review |
| p3-rev-b: `cargo nextest run --locked --lib -E 'test(/^transport::tests::.*(multiplex|forward|managed|ssh_argv)/)'` | 7 selected, 7 passed |

The fixes were verified by their owning tracks and integrated before T7. No additional independent
post-fix review pass is claimed.

## Local fixture observations

These are T7d's local fixture observations, not live latency. The fixture used real local RPC
children and a fake local SSH/mux with 10 warmups and 200 paired or alternating samples per class.

```text
stdio  = S + W + H + D
socket =     W + H + D + O
expected difference = S - O
```

`S` is the SSH execution-session/process cost after ControlMaster has already removed TCP/auth
setup. `W + H + D` is worker startup/config, handler/store/publisher, and framing/data. `O` is the
new socket/supervisor/wrapper overhead. The fixture does not measure real SSH `S`.

Each cell is command mean / p50 / p95 in milliseconds:

| Scenario | Stdio ms | Socket-path ms |
| --- | --- | --- |
| Cold CLI wait, pin create | 41.549 / 41.368 / 46.241 | 163.514 / 161.442 / 181.007 |
| Cold CLI wait, pin verify | 41.608 / 41.010 / 45.947 | 153.670 / 149.634 / 167.944 |
| Cold CLI followed logs, pin verify | 71.610 / 71.058 / 77.657 | 158.923 / 157.149 / 170.441 |
| Retired after unacknowledged cancel | 35.526 / 35.142 / 38.538 | 35.103 / 35.060 / 37.940 |
| Warm wait, zero requested wait | 35.031 / 35.024 / 37.990 | 10.209 / 10.079 / 12.368 |
| Warm logs, zero requested wait | 34.357 / 33.574 / 38.380 | 10.228 / 10.316 / 10.902 |
| Warm events, zero requested wait | 33.249 / 32.982 / 36.602 | 8.396 / 8.195 / 10.906 |
| Warm events, 5-ms requested wait | 39.963 / 39.446 / 43.800 | 14.339 / 13.647 / 16.703 |
| Fallback/reconnect, two wait reads | 72.890 / 71.241 / 82.558 | 189.272 / 184.220 / 211.990 |

Structural observations:

- 4,400 application reads/children and 1,200 fresh CLI processes.
- Every warm socket class used zero SSH application executions and one worker child per read.
- Each cold socket class used 200 identity/bootstrap workers, 200 resolution processes,
  600 control processes, 200 allocations, and 200 positive cancels. Wait had 200 application
  children; followed logs had 400 because it performs health plus logs.
- Recovery used 400 application children: 200 raw fallbacks and 200 socket reads, with
  200 reconnect allocations. Its 201 cancels include final command-owner teardown.
- The unacknowledged-open case retained one exact socket/directory binding across 200 eligibility
  advances, with zero later allocations, cancels, or socket attempts.
- Maximum observed request/reply frames were 309 / 6,211 bytes; maximum retained decoder
  payload/frame was 6,207 bytes; maximum active supervisors was one. Configured limits remained
  8 KiB scratch and 1 MiB payload.
- Two drain mutations ran raw with zero channel attempts or allocations.

Cold means a fresh command/channel lifetime with warmed local caches and a pre-existing private
master, not cold network or authentication. These observations show local cold overhead and warm
structure. They do not establish a deployed speedup.

## Gates still open

| Gate | Status |
| --- | --- |
| T8 `cargo fmt --all --check` | passed, exit 0 |
| T8 `CARGO_BUILD_JOBS=4 cargo clippy --locked --all-targets -- -D warnings` | passed, exit 0; no warnings |
| Full `scripts/test-gate.sh` / all-target nextest gate | Run by the integrator on `8398b0a` (all Phase 3 code; this docs commit changes no code): `CARGO_BUILD_JOBS=8 NEXTEST_TEST_THREADS=12 MAC_WORKER_GATE_RAMDISK_MB=0 scripts/test-gate.sh --profile ci`, exit 0. 3,752 tests run: 3,752 passed, 22 skipped, no flaky retries; 632.0 s nextest, 666 s wall |
| Deployment and live paired measurement | **pending — live, after deploy** |

## Live acceptance checklist

Nothing in this table was run. Every plan row is `pending — live, after deploy`; deployment
requires the owner's approval.

| Plan item | Status |
| --- | --- |
| Confirm fleet/OpenSSH stream-local support and permissions with isolated approved tasks; verify old/new pairing, managed `-F` master override, concrete control path, and config-free exclusive forward behavior | pending — live, after deploy |
| Pair or alternate raw/channel read loops with identical state and sample counts; record complete cold command lifecycle, warm RPCs, requested waits, start-to-start wait cadence, events/notify process and CPU cost, fallback/reconnect, `S - O`, and remaining worker cost | pending — live, after deploy |
| Exercise approved leader restart, master loss, network loss, and actual transient-child cancellation; verify wait/log traffic with an unavailable journal, event cursor identity/reset, and unchanged mutation stdio retries inside throttle | pending — live, after deploy |
| Verify positive graceful cleanup and exactly one retained residue after uncertain cancel; preserve the shared master and unrelated configured forwards; confirm no later allocation in that command; inspect identity/repin only on an approved fixture reinstall without moving or deleting notify/envelope state | pending — live, after deploy |
| Verify dashboard and Git behavior remain unchanged | pending — live, after deploy |

Phase 4 phone/Tailscale access and Phase 5 per-mini daemons are out of scope by owner decision.
Shortening the SSH control directory remains an owner follow-up. Ordinary multiplexing behavior
for the 94+17-byte cold managed path is still an open performance-phase question.
