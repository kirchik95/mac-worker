# Controller events validation record

Written 2026-10-01. Help and clap checks were taken on `faf29641f86f` before the T8c rebase. T9's UI, parity, fmt, clippy, and targeted tests were run on `154f6e7e12a2`. The docs commit is `0802421`, already in `integ/ev-wave`. This record was finalized on wave head `b0c07d8`. A row that was not run is pending. Pending is not a pass. Gate counts supplied by the orchestrator are labeled as such.

The live checklist is for an authorized integrator after deploy. Nothing in that table was run. No pool host, SSH session, LaunchAgent, credential, or Herdr command was used. `worker events` and `worker notify` were not started against a controller; only `--help` and clap usage errors, which exit before configuration is loaded.

## Identity

| Item | Value |
| --- | --- |
| Wave head | `b0c07d8` `fix(events): recover coalescing when a sweep finds its cursor expired` |
| T9 measurement base | `154f6e7e12a2`. Help text was captured on the pre-rebase binary `faf29641f86f`. |
| Docs commit | `0802421` `docs(controller): document event notifications and rebuild dashboard assets`, already in integ |
| Binary measured for `--help` | Before the rebase: `target/debug/worker`, `worker 0.1.0+faf29641f86f-debug`. After the rebased test build, with these docs still uncommitted: `worker 0.1.0+154f6e7e12a2.dirty-debug`. Dev profile, not a release artifact. T8c phase 2 did not change the events or notify flags. |
| rustc | 1.98.1 (48a229cea 2026-09-01) (Homebrew) |
| Node on this machine | v24.13.0. Known deviation: CI is Node 22. Node 22 was not installed. |
| Node in CI | 22 (`.github/workflows/ci.yml`). CI re-checks asset parity on Node 22 on push. |
| `controller.events` | present in `CONTROLLER_FEATURES`, sorted beside `controller.task-logs-wait` (`src/features.rs`). Matches the usage sentence. |
| Protocol | 7 |

## Commands run on this base

| Command | Result |
| --- | --- |
| `CARGO_BUILD_JOBS=4 cargo build --locked --bin worker` | exit 0, dev profile, 1m 05s |
| `target/debug/worker events --help` | exit 0. Usage `worker events [OPTIONS] -f`. Flags: `--config`, `-f` (required), `--json`, `--help`. No `--since`. |
| `target/debug/worker notify --help` | exit 0. Usage `worker notify [OPTIONS]`. Flags: `--follow`, `--quiet`, `--no-titles`, `--channel` default `auto` with values `auto`, `macos`, `herdr`, `both`. Help text: "auto prefers reachable Herdr, then macOS". |
| `target/debug/worker --help` | exit 0. Lists `events` and `notify`. |
| `target/debug/worker --version` | exit 0. `worker 0.1.0+faf29641f86f-debug` |
| `worker events` (no `-f`) | exit 64. Clap: required `-f` missing. |
| `worker events --json` | exit 64. Same missing `-f`. |
| `worker events -f --since 5` | exit 64. Unexpected argument `--since`. |
| `worker events --follow` | exit 64. `--follow` is not an events flag. |
| `worker notify -f` | exit 64. |
| `worker notify --channel remote` | exit 64. Invalid value; possible values `auto`, `macos`, `herdr`, `both`. |
| `worker notify --channel` | exit 64. Value required. |
| `cd ui && npm ci && npm test && npm run lint && npx tsc -b && npm run build` | exit 0 on Node v24.13.0. `npm ci` added 453 packages. `npm test`: 24 files, 262 tests passed (22.85s). Vitest then logged timeouts while terminating fork workers; the run still exited 0. `npm run lint` (oxlint) exited 0 with 19 warnings, all in existing components. `npx tsc -b` exited 0. One production `npm run build` wrote `src/dashboard/static/app/`, including `assets/inter-variable.ttf` and `assets/plex-mono-regular.ttf`. |
| CI parity `npm run build -- --outDir "$tmp"` then `diff -rq` | exit 0. Diff empty across the whole tree (index.html, favicon.svg, assets/index.css, assets/index.js, both fonts). Known deviation: Node v24.13.0 here; CI is Node 22 and re-checks parity on push. |
| `cargo fmt --all --check` | exit 0 |
| `CARGO_BUILD_JOBS=4 cargo clippy --locked --all-targets -- -D warnings` | exit 0. No warnings. |
| controller nextest filter (`controller_event_`, `controller_features::`, `controller_health_routes::`) | exit 100 under the default nextest profile. 248 tests run: 247 passed (2 slow), 1 timed out, 352 skipped. `real_names_cost_cap_and_independent_addressed_reads` was killed at 180.011s (slow-timeout period 60s, terminate-after 3). Its stdout had printed the 1,000 and 10,000 rows and had not printed 100,000. |
| retry of that one test | exit 0. Temporary nextest config, not in the repo: slow-timeout period 120s, terminate-after 8. 1 passed in 362.663s (slow), 599 skipped. |
| dashboard nextest filter (`dashboard_events::`, `dashboard_tunnel_reconnect::`) | exit 0. 48 passed, 163 skipped, 10.047s |
| cli nextest filter (`cli_help::`) | exit 0. 32 passed, 21 skipped, 10.537s |
| library nextest filter (`events`) | exit 0. 122 passed (2 slow), 743 skipped, 80.401s |
| `scripts/test-gate.sh` | not run by T9. The orchestrator's later gates are under Wave gates. |

## T4 repair results

Source: `.briefs/ev-t4-report.md`, branch `feat/ev-t4-rpc`, verification date 2026-10-01. T4's final controller filter passed 107/107 and its kernel units passed 17/17. On `154f6e7` the combined names test was killed at 180s by the default nextest profile and then passed in 362.663s. `1fb565c` then split that fixture: the default test keeps the 1,000-entry sanity check and the addressed-read assertions, and the 10,000 / 100,000 / 100,001 measurement is the ignored `real_names_cost_cap_and_independent_addressed_reads_stress` in the nightly stress group. The first table stays T4's published observations from the 107-test run. The second table is the captured stress stdout from T4 follow-up 2. Neither table is a guarantee.

| Behaviour | Test | T4 result |
| --- | --- | --- |
| Key pages over a registry larger than one work budget | `controller_event_rpc::large_frozen_registry_key_pages_complete_under_injected_work_budget` | passed in T4's 107 |
| Insertion below and above the cursor, and cursor-key removal | `controller_event_rpc::removed_cursor_and_insertions_above_and_below_it_converge_by_keys` | passed in T4's 107 |
| Cursor reused by another process | `controller_event_rpc::cursor_reused_in_new_process` | passed in T4's 107 |
| 100,000 admitted; 100,001 including residue rejected before record, queue, and fact reads | `controller_event_rpc::real_names_cost_cap_and_independent_addressed_reads`; units `registry_over_cap_is_rejected_before_name_validation`, `residue_counts_toward_cap_but_at_cap_is_admitted` | passed. Addressed reads still succeeded on the over-cap fixture. |
| `complete=true` means the current sorted keys are exhausted, not an atomic snapshot | T4 report, repair page contract | recorded. Below-cursor insertion is the next sweep. |

### Names and record work (observations, not guarantees)

Same fixture and numbers as `docs/usage.md`. Times are microseconds from T4's final 107-test run. Each admitted page deliberately read one record. The 100,001 row is collection and the count only: rejection is before validation, sort, and every record read, so its names time is not a cheaper listing than the 100,000 row.

| Entries | Name bytes | Names collection, validation, sort (µs) | Record and fact work (µs) | Records | Task input bytes | Queue reads | Associations |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 37,677 | 7,390 | 9,073 | 1 | 1,357 | 1 | 0 |
| 10,000 | 376,977 | 30,463 | 4,086 | 1 | 1,357 | 1 | 0 |
| 100,000 | 3,769,977 | 176,736 | 1,170 | 1 | 1,357 | 1 | 0 |
| 100,001 | 3,770,021 | 46,120 | 0 | 0 | 0 | 0 | 0 |

`list_names` allocates the whole directory before the cap rejects it. T4 makes no bounded whole-directory allocation or syscall-time claim.

### Stress timings (T4 follow-up 2, observations)

Captured once from the ignored stress test after `1fb565c` (`9fa548e` on the T4 branch, same subject). Default profile, `--run-ignored only`, 1 passed, test time 10.223 s, wall 10.45 s. The 100,001 row is still collection and the count only, so its names time is not a cheaper listing than the 100,000 row. Timings vary with machine load.

| Entries | Name bytes | Names (µs) | Record and fact work (µs) | Records | Task input bytes | Queue reads | Associations |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 376,977 | 14,719 | 6,631 | 1 | 1,357 | 1 | 0 |
| 100,000 | 3,769,977 | 147,856 | 874 | 1 | 1,357 | 1 | 0 |
| 100,001 | 3,770,021 | 38,548 | 0 | 0 | 0 | 0 | 0 |

## T2 journal crash coverage

Source: `.briefs/reports/ev-t2-report.md`. Ordinary injected-fault coverage, all recorded as passed by T2:

- initialization role matrix, 21 cases (3 roles × 7 boundaries), including empty evidence-less stages removed only by the leader;
- public operation matrix, 28 cases (4 roles × 7 boundaries);
- primitive role matrix, 40 cases;
- sealed-swap, ESTALE (three same-binding retries), unsafe symlink/hardlink/mode/lock/epoch without reset, and the publisher fsync gate.

T2's last recorded controller filter was 76/76. The post-rebase controller filter on this tree re-ran the non-ignored journal tests, including the fault-boundary test, and they passed.

### Ignored 200× stress

`tests/controller/controller_event_journal.rs` `public_journal_fault_matrix_stress` is `#[ignore]` ("200 deterministic role-matrix iterations; explicit stress gate only"). T2 defines 200 × 28 = 5,600 cases and did not execute them. Status: **not run**. It is a scheduled release gate, not a pass. On `154f6e7` the non-ignored sibling `public_append_fault_boundaries_and_repeated_crashes_never_duplicate_sequences` took 79.6s inside the controller filter, so T9 did not start the ignored test. The final wave also left it unrun.

## Local gates still open

| Gate | Status |
| --- | --- |
| UI `npm test`, `npm run lint`, `npx tsc -b` | passed on Node v24.13.0. 262 tests, lint exit 0 with 19 oxlint warnings, `tsc -b` exit 0 |
| One production `npm run build`, fonts included | passed. Both font files are in `src/dashboard/static/app/assets/` |
| Fresh `--outDir` diff against `src/dashboard/static/app` | passed. Empty diff. Node v24.13.0 here; CI Node 22 re-checks on push |
| `cargo fmt --all --check` | passed, exit 0 |
| clippy `-D warnings` | passed, exit 0 |
| targeted nextest filters from the T9 brief | passed, with one default-profile timeout retried. Controller 247/248 then the names-cap test passed in 362.663s. Dashboard 48/48. CLI help 32/32. Library `events` 122/122 |
| `scripts/test-gate.sh` | recorded under Wave gates. T9 did not run it |
| `git diff --stat integ/ev-wave...HEAD` at `0802421` | docs, `ui/README.md`, this file, and the two rebuilt asset files |
| Source leases | none |

## Review passes

SHAs below are the integ commits named for this wave. T9 did not re-run the fixes.

### Pass 1

ev-rev-a (Claude Fable) reviewed T2 and T3 and returned F1–F7. ev-rev-b (Codex) reviewed T1, T5, T6, and T7 and returned H1, H2, and M1–M4.

| Item | Commit | Final status |
| --- | --- | --- |
| F1 full-content re-validation on every read | `b574483` | landed |
| F2 terminal hints delayed past notifier handoff | `85cfad3` | landed |
| F3 readers fail after the writer's 50 ms admission | `3496dec` | landed |
| F4 no runner-level terminal or publication-failure coverage | `3011aad` | landed |
| F5 producer drop counter | `e52a2f7` | landed. `dropped_hint_count()` plus the `CONTROLLER_EVENT_HINTS_DROPPED` exit line |
| F6 evidence-less stage left the journal permanently unavailable | `690abb1` | landed. Leader discards a provably empty stage; other residue stays |
| F7 queue publication re-reads the queue file | `9007bcf` | landed |
| H1 browser bootstrap/reset loop | `e707af2` | landed |
| H2 disconnect admits unbounded journal work | `68d6d2d` | landed |
| M1 oversized corrupt cache cannot rebaseline | `b0009c4` | landed |
| M2 stale Herdr socket selects the wrong auto channel | `110c55d` | landed |
| M3 same-epoch repair does not coalesce | `d588a78` | policy landed here. The remaining path is C2, then P3-M1 |
| M4 invalid combined notifier regression | `33fba0e`, `efd6083` | landed |

### Pass 2

ev-rev-a2 (Fable) returned **land**. ev-rev-c (Codex) returned land after fixes.

| Item | Commit | Final status |
| --- | --- | --- |
| N1 healthy attach decodes 0 segment contents | `cfcf904` | landed |
| N2 `result_imported` differs between repair facts and the producer | `266d913` | fixed completely (pass 3) |
| N3 server echoes `baseline_after` and does not capture H | none | documented in `docs/usage.md` |
| N4 display titles stay at 120 bytes | none | documented in `docs/usage.md` |
| C1 cold start of current attention was consumed with no summary | `1c637b8` | fixed completely (pass 3) |
| C2 same-epoch cursor repair emitted individual banners | `daa971a` | partial at pass 3. The remaining path is P3-M1 |
| C3 RPC client rewrote `CONTROLLER_EVENTS_REPAIR_REGISTRY_TOO_LARGE` as generic unavailable | `f3a2c3f` | fixed completely (pass 3) |

### Pass 3

ev-rev-c3 found C1, C3, and N2 fixed completely, and C2 partial. The remaining path, P3-M1, is a periodic sweep that finds its own validated cursor expired. It was fixed in `b0c07d8`, with two permanent T4-to-T7 regressions, one one-page and one paged. P3-M1 was not re-reviewed. Those regressions cover it.

| Item | Commit | Final status |
| --- | --- | --- |
| P3-M1 periodic sweep finds its validated cursor expired | `b0c07d8` | landed, not re-reviewed. Covered by the one-page and paged regressions |

## Test-robustness fixes from the full gate

| Commit | What changed |
| --- | --- |
| `f9b98cf` | tunnel child-exit wait is a 30 s hang guard. This was a pre-existing flaky test |
| `86a10c2` | wait-wiring deadlines are hang guards. This is a new T8c test that failed under gate load |
| `1fb565c` | the real 10k/100k/100,001-entry names measurement is the ignored `real_names_cost_cap_and_independent_addressed_reads_stress`, in the nightly stress group. The default test keeps the 1k sanity check and the addressed-read assertions |

## Wave gates

These counts are the orchestrator's. This follow-up did not re-run them.

| Gate | Result |
| --- | --- |
| Preliminary full gate at `154f6e7` | default profile, no RAM disk, loaded machine. 3,492 tests: 3,488 passed, 3 failed, 1 timed out, 1,184 s wall. Two of those four were new tests, later fixed by `86a10c2` and `1fb565c`. The other two were pre-existing load flakes in unchanged code: `project_readiness::tests::cancel_after_child_starts_reaps_descendants_without_a_receipt` (passed 30/30 in isolation) and `origin_outbox::identity_hit_restores_missing_due_for_a_running_watcher` (a test-side race between due-key deletion and the watcher poll) |
| Final full gate at `1fb565c` | `CARGO_BUILD_JOBS=8 NEXTEST_TEST_THREADS=12 MAC_WORKER_GATE_RAMDISK_MB=0 scripts/test-gate.sh --profile ci`, exit 0. 3,500 tests run: 3,500 passed, 21 skipped, no flaky retries. 581.9 s nextest, 614 s wall |
| After `b0c07d8` | P3-M1 touches `client.rs` and tests. Targeted controller + cli filter (`controller_event_`, `controller_features::`, `cli_help::`): 280/280. Library `events`: 122/122 |
| fmt and clippy | clean on every track's final commit. The orchestrator re-checks at deploy time |
| Ignored journal 200× fault-matrix stress | not run. Scheduled release gate |

## Live acceptance checklist

Deploy, this live checklist, and push to main wait for the owner's explicit approval. Every item is `pending — live, after deploy`. The kit in `.briefs/acceptance-kit.md` is the integrator's script map. T9 did not run any kit script.

| Plan item | Kit | Status |
| --- | --- | --- |
| Deploy the integrated build; record identities and protocol 7 / features; old laptop still decodes list/status/log/wait/drain | §1, §2, §7.7 | pending — live, after deploy |
| Submit a task and a follow-up; viewer shows lifecycle; one title-bearing banner after confirmed retirement; `--no-titles` removes the title | §4, §5.1, §5.3 | pending — live, after deploy |
| NeedsInput and Blocked use the Request sound, Done uses Done, remaining outcomes None; `--channel both` is two attempts for one decision | §5.5, §5.7 | pending — live, after deploy |
| Auto-continuation and a dead-but-not-retired runner never banner; a dispatching queue row and a close intent do not | §7.6 needs explicit drain authorization; dead runner is fixture-only (§8) | pending — live, after deploy |
| Detached controller child attaches the existing journal; laptop-local neither initializes nor emits | §7.1 | pending — live, after deploy |
| Drop an event after state commit; addressed checks and warm repair converge without advancing to an undelivered head | §7.2 (live approximation); exact drop is fixture-only (§8) | pending — live, after deploy |
| Restart notifier after dedup eviction; historical completions stay silent; current attention coalesces; quiet/overflow stays consumed | §5.2, §5.4, §5.6, §5.8; overflow volume is fixture-only (§8) | pending — live, after deploy |
| Publisher backpressure / journal unavailable: state and drain and state-only selectors still work; drop and unavailable diagnostics | §8 fixture-only (T2 fault matrix, T8 fsync gate) | pending — live, after deploy |
| Registry larger than one page completes over multiple key pages, including cursor reuse by another RPC process; insert below the cursor; >100,000 fixture returns registry-too-large with zero record reads | §8 fixture-only (T4 tests above) | pending — live, after deploy |
| N-1: debug tail unsupported, notifier eligibility unknown and no banners; new discovery then old execution leaves no receipts or req rows | §7.4. New laptop against an old controller needs a rollback window and stays pending even during the rest of the kit | pending — live, after deploy |
| Tunnel or heartbeat loss: viewer closes SSE at the existing 30 s timeout; UI keeps last good data and drafts; polls; resumes after reconnect. SSE heartbeat is independent | §6.5 | pending — live, after deploy |
| Slow worker collector: event refresh uses the local projector; `snapshot.ready` refreshes task/queue/run; stale full collection merges workers only. Idle TTL and leader health stay their own polling signals | §8 fixture-only (T5) | pending — live, after deploy |
| Slow tab repairs after lag; foreign Host/Origin fails; new turn on the same waiting task refreshes questions; UTF-8 trailing log bytes survive a terminal hint | §6.2, §6.3, §7.8 | pending — live, after deploy |
| Unchanged `task.wait`: short timeout, `WAIT_BLOCKED`, run DAG, aggregate exit, same-id mutation retry | §7.3 | pending — live, after deploy |
| Deployed binary's embedded UI equals the accepted asset build | §7.7. Compare artifact to artifact, not a fresh rebuild | pending — live, after deploy |

Kit rows with no separate plan line, also unrun:

| Kit | Status |
| --- | --- |
| §3 journal baseline | pending — live, after deploy |
| §4.2 misuse guards on a deployed binary | pending — live, after deploy. Local clap exits are recorded above and are not this row. |
| §6.1 SSE shape and heartbeat | pending — live, after deploy |
| §6.4 local mode 404 | pending — live, after deploy |
| §7.5 journal after the run | pending — live, after deploy |
| §8 fixture-only table (backpressure, oversized registry, crash points, overflow volume, slow collector) | pending — live, after deploy. Local coverage is the T2/T4/T5 tests named in the kit, not a live pass. |
| §9 rollback | not needed — not executed |

No names-versus-record measurement was taken on a live `tasks/` directory. The table above is T4's fixture. A live registry count and a turn-finished-to-banner delay remain `pending — live, after deploy`.

## Live acceptance results, 2026-10-01

The orchestrator ran these checks on the deployed pool after the owner approved the deploy. The tables above stay as the pre-deploy record. A row marked pending here was not run live; pending is not a pass. Task ids are shortened to 8 hex digits.

### Deploys

| Step | Result |
| --- | --- |
| First deploy: main `d3cae87` (events wave + Phase 3 + dead-code removal) | Release gate 3,763/3,763. Laptop `0.1.0+d3cae878bc0e-release`; `worker setup` reached all three minis and restarted the controller. `worker controller status` lists `controller.events`, `controller.socket`, `controller.task-logs-wait`; protocol 7 |
| Live defect after the first smoke turn | The mini-1 journal kept an empty `manifest.stage` without evidence and a `pending.json` holding events 5..11, so every reader got `CONTROLLER_EVENTS_UNAVAILABLE`. Cause: a short-lived publisher exited mid-append inside the 50 ms exit grace, and only leader init discarded an empty stage |
| Live fix: main `6f51701` | Crash residue is recovered under journal EX, and `PUBLISHER_EXIT_GRACE` is 3 s. After deploy, the controller restart recovered the journal: head seq 11, events 5..11 preserved |
| Stabilization: main `ca86290` | Gate 3,778/3,778 with no flaky retries; journal head 56 and clean after the restart |

### Plan items

| Plan item | Live result |
| --- | --- |
| Deploy; identities, protocol 7, features; old laptop decodes list/status/log/wait/drain | **Pass.** See Deploys above and N-1 below. The old laptop decodes the drain state (`drained: false`) in `controller status`; drain was toggled only from the new laptop, six times, for the latency measurement |
| Task lifecycle in the viewer; one title-bearing banner; `--no-titles` | **Pass.** Smoke `d304f3cd` (mini-3): `worker events -f` received seq 12..29, including `turn.started` and `turn.finished` done; `task wait` exited 0 in 32 s. `--channel both` on `0b050265` showed one notice per channel, and the operator saw both with the title. `--no-titles` on `8180e7bc`: the macOS banner showed the id and outcome with no title |
| NeedsInput/Blocked → Request sound, Done → Done; `--channel both` = two attempts, one decision | **Partial.** `--channel both`: one decision, two channel notices, seen by the operator. `needs_input` on `8180e7bc` (`--questions ask --close-on never`): one decision, the task stayed `open`, and the operator saw the macOS banner and heard the herdr sound. The operator did not compare the Request and Done sounds side by side. Blocked was not exercised |
| Auto-continuation, dead runner, dispatching row, close intent never banner | **Partial.** Close intent: `8180e7bc` closed with `--discard` became `abandoned`; the next notifier run consumed seq 131→134 and recorded no decision. While `needs_input` persisted, the notifier re-checked it every 15 s and kept one decision. Auto-continuation and a dispatching row: pending (needs explicit drain authorization). Dead runner: fixture-only |
| Detached controller child attaches the journal; laptop-local never emits | **Partial.** Socket-spawned RPC children and the leader share one journal on mini-1 (seq grows across turns). A local-mode new binary refuses `events -f` (exit 64) and creates no journal (N-1 kit step 8) |
| Restart after dedup eviction; history silent; attention coalesces; quiet consumed | **Partial.** A restart with no new work (20:00:01) consumed seq 131→134 with 0 new decisions and no output. Outage of over 60 s: `7edf541a` and `8c0aa442` finished while the notifier was down; the restart at 20:07:53 recorded both decisions at once (5→7, seq 134→173). The owner deferred the visual check of the coalesced summary, so that part is pending. `--quiet` not run |
| N-1: old laptop against the new controller; old controller rejects selectors with no artifact | **Pass.** Old laptop `0.1.0+37915a9c21c2-release`: `controller status` decodes and prints the controller feature strings as received (it does not use them); `task list` decodes (protocol 7, 28 tasks); `task status`, `task logs` and `task wait` decode, wait exit 0; `events` and `notify` are usage errors (exit 64). The old binary as an isolated `host controller-rpc` rejects `read`, `tasks` and `repair` selectors with `INVALID_REQUEST: task.list body contained unexpected key controller_events`, leaving 0 `req-*` rows and 0 `active/` receipts. Kit fix: the probe needs pre-created 0700 XDG directories, otherwise every request, including the positive control, answers `HOST_IO`. New laptop against an old controller: pending (rollback window) |
| Tunnel or heartbeat loss; UI keeps data and drafts; resumes | **Pass, one gap.** Browser on `http://127.0.0.1:9173`. Before the loss the stream sent `snapshot_required` (bootstrap), `ready`, `heartbeat`, then `snapshot.ready` about every 2 s as the collection revision advanced. At 20:22:54 the laptop `worker dashboard` got SIGINT. The page kept the last snapshot and showed "The dashboard API stopped answering" and "Showing last snapshot"; worker cards turned "Stale · capacity unknown" with their last-reported age. Snapshot polls continued every 2–3 s and the event source retried with backoff, all refused. A restart at 20:23:23 on the same port answered `/api/v1/events` with 503 while the tunnel came up, then 200; the page returned to "Dashboard refreshed 0s ago". Second run at 22:25 with an open `needs_input` task (`9f0644da`): an unsent draft typed into the reply box survived the loss and the restart unchanged. The task page showed "Failed to fetch" and "Showing the last snapshot · retrying every 2s", and kept the question and task details. On mini-1 the `--controller-viewer` process (pid 88076) was gone 5–10 s after the laptop SIGINT, within the 30 s bound; the restart started a new viewer (pid 6268). Not checked: cursor reuse, because no `controller.event` arrived in either window |
| Slow tab, foreign Host/Origin, question refresh, UTF-8 trailing bytes | Pending — not run |
| Unchanged `task.wait` | **Partial.** `task wait` exited 0 for done (`f1b903ea`, `0b050265`, `7edf541a`, `8c0aa442`) and for `needs_input` (`8180e7bc`). WAIT_BLOCKED, run DAG and aggregate exit were not exercised live |
| Embedded UI equals the accepted asset build | **Pass.** At 22:24 the deployed laptop binary (`0.1.0+ca862905b8fa-release`) served `index.html`, `assets/index.css`, `assets/index.js`, `favicon.svg` and both fonts with SHA-256 equal to the committed artifacts at `ca86290`. Those artifacts differ from T9's accepted build at `154f6e7` only by the `0802421` rebuild of unchanged `ui/src`: minifier identifier renames in `index.js` and rule order in `index.css` |
| Fixture-only rows (§8) | Fixture-only, as planned |

### Measurements

| Measurement | Result |
| --- | --- |
| Event → laptop latency | p50 about 340 ms (330–420 ms) over six drain toggles, corrected for mini-1's clock running about 1.43 s ahead of the laptop |
| Turn finished → notify decision | Under 1 s: for `f1b903ea` the decision was saved at 19:51:04, the same second `task wait` returned |
| Notifier lock | A second `worker notify` exits with `CONTROLLER_EVENTS_NOTIFY_LOCK_HELD` while one runs |

### Channel selection note

`--channel auto` delivers to herdr whenever the laptop herdr socket is reachable, because `[notifications] herdr` defaults to `true`. On a laptop where herdr runs, that means herdr notices and sounds, not macOS banners. `--channel macos` or `--channel both` produces a macOS banner, which also stays in Notification Center.
