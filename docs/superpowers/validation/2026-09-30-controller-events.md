# Controller events validation record

Written 2026-10-01. Help and clap checks were taken on `faf29641f86f` before the T8c rebase. The tree was then rebased onto `integ/ev-wave` at `154f6e7e12a2` (`feat(controller): wire event producers and laptop consumers`). UI, parity, fmt, clippy, and targeted tests below are on that tree. A row that was not run is pending. Pending is not a pass.

The live checklist is for an authorized integrator after deploy. Nothing in that table was run. No pool host, SSH session, LaunchAgent, credential, or Herdr command was used. `worker events` and `worker notify` were not started against a controller; only `--help` and clap usage errors, which exit before configuration is loaded.

## Identity

| Item | Value |
| --- | --- |
| Base commit | `154f6e7e12a2` on `integ/ev-wave` after `git rebase`. Help text was captured on the pre-rebase binary `faf29641f86f`. |
| Docs commit | the commit that adds this file, subject `docs(controller): document event notifications and rebuild dashboard assets` |
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
| `scripts/test-gate.sh` | pending — integrator. Not run by T9. |

## T4 repair results

Source: `.briefs/ev-t4-report.md`, branch `feat/ev-t4-rpc`, verification date 2026-10-01. T4's final controller filter passed 107/107 and its kernel units passed 17/17. The post-rebase controller filter on this tree includes those tests. The four named integration tests passed there, except `real_names_cost_cap_and_independent_addressed_reads`, which the default 180s kill stopped and which then passed in 362.663s. The two cap units passed in the library filter (122). The table below stays T4's published observations. This machine's partial stdout (1,000 and 10,000 only) is not substituted for it.

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

## T2 journal crash coverage

Source: `.briefs/reports/ev-t2-report.md`. Ordinary injected-fault coverage, all recorded as passed by T2:

- initialization role matrix, 21 cases (3 roles × 7 boundaries), including empty evidence-less stages removed only by the leader;
- public operation matrix, 28 cases (4 roles × 7 boundaries);
- primitive role matrix, 40 cases;
- sealed-swap, ESTALE (three same-binding retries), unsafe symlink/hardlink/mode/lock/epoch without reset, and the publisher fsync gate.

T2's last recorded controller filter was 76/76. The post-rebase controller filter on this tree re-ran the non-ignored journal tests, including the fault-boundary test, and they passed.

### Ignored 200× stress

`tests/controller/controller_event_journal.rs` `public_journal_fault_matrix_stress` is `#[ignore]` ("200 deterministic role-matrix iterations; explicit stress gate only"). T2 defines 200 × 28 = 5,600 cases and did not execute them. Status: **not run**. It is a scheduled release gate, not a pass. On this tree the non-ignored sibling `public_append_fault_boundaries_and_repeated_crashes_never_duplicate_sequences` took 79.6s inside the controller filter. Two hundred iterations of that matrix would be far past 10 minutes, so T9 did not start the ignored test.

## Local gates still open

| Gate | Status |
| --- | --- |
| UI `npm test`, `npm run lint`, `npx tsc -b` | passed on Node v24.13.0. 262 tests, lint exit 0 with 19 oxlint warnings, `tsc -b` exit 0 |
| One production `npm run build`, fonts included | passed. Both font files are in `src/dashboard/static/app/assets/` |
| Fresh `--outDir` diff against `src/dashboard/static/app` | passed. Empty diff. Node v24.13.0 here; CI Node 22 re-checks on push |
| `cargo fmt --all --check` | passed, exit 0 |
| clippy `-D warnings` | passed, exit 0 |
| targeted nextest filters from the T9 brief | passed, with one default-profile timeout retried. Controller 247/248 then the names-cap test passed in 362.663s. Dashboard 48/48. CLI help 32/32. Library `events` 122/122 |
| `scripts/test-gate.sh` | pending — integrator |
| `git diff --stat integ/ev-wave...HEAD` scope check | empty until this commit, because `HEAD` is `154f6e7e12a2`. The working tree against that commit is only `docs/usage.md`, `ui/README.md`, `src/dashboard/static/app/assets/index.css`, `src/dashboard/static/app/assets/index.js`, and this new file |
| Source leases | none |

## Review passes

Two review passes reported findings. The commits below are the ones named in `.briefs/ev-rev-a-pass2-report.md` and `.briefs/ev-rev-c-report.md`. T9 has not re-run those fixes. **Final status is pending** on every row until the orchestrator names the commit that closes it. A report verdict is not that final status.

Operator docs already state the intended behaviour for the open consumer items: a first start summarizes current attention (C1), a cursor repair coalesces its fresh decisions (C2), the laptop keeps `CONTROLLER_EVENTS_REPAIR_REGISTRY_TOO_LARGE` (C3), the notifier captures the journal head and the server only echoes `baseline_after` (N3), and banner titles are capped at 120 bytes (N4). Those sentences are the contract. They are not evidence that the client fixes have landed.

### Pass 1

ev-rev-a (Claude Fable) reviewed T2 and T3 and returned F1–F7. ev-rev-b (Codex) reviewed T1, T5, T6, and T7 and returned H1, H2, and M1–M4. Pass 2 then checked those fixes.

| Item | Report verdict | Commit named in the pass-2 reports | Final status |
| --- | --- | --- | --- |
| F1 full-content re-validation on every read | fixed completely | `b574483` fix(events): bound steady-state journal validation | pending |
| F2 terminal hints delayed past notifier handoff | fixed completely | `85cfad3` fix(events): release terminal hints before runner handoff | pending |
| F3 readers fail after the writer's 50 ms admission | fixed completely | `3496dec` fix(events): let readers wait within their admission deadline | pending |
| F4 no runner-level terminal or publication-failure coverage | fixed completely | `3011aad` test(events): cover runner terminal and fetch failure hints | pending |
| F5 write-only producer drop counter | not fixed in the pass-2 tree; left with T8a. No commit named | — | pending |
| F6 evidence-less stage left the journal permanently unavailable | fixed (leader discards a provably empty stage; other residue stays) | `690abb1` fix(events): recover empty stages before creation evidence | pending |
| F7 queue publication re-reads the queue file | fixed completely | `9007bcf` perf(events): reuse caller snapshots for queue hints | pending |
| H1 browser bootstrap/reset loop | fixed completely | `e707af2` fix(ui): complete controller event bootstrap and reset recovery | pending |
| H2 disconnect admits unbounded journal work | fixed completely | `68d6d2d` fix(dashboard): bound viewer journal work across disconnects | pending |
| M1 oversized corrupt cache cannot rebaseline | fixed completely | `b0009c4` fix(events): rebaseline an oversized notifier cache without reading it | pending |
| M2 stale Herdr socket selects the wrong auto channel | fixed completely | `110c55d` fix(events): probe a Herdr socket before auto selection | pending |
| M3 same-epoch repair does not coalesce | partially fixed. Policy commit landed; ev-rev-c says the reconciler still drops the signal (see C2) | `d588a78` fix(events): coalesce notices across a same-epoch cursor repair | pending |
| M4 invalid combined notifier regression | fixed completely | `33fba0e` test(events): validate chunked overflow membership in the notifier regression; `efd6083` test(events): avoid explicit loop counters in the overflow regression | pending |

### Pass 2

ev-rev-a2 verified the pass-1 fixes, the T4 server side, and T8c routing, and returned **land** with Lows N1–N4. ev-rev-c verified H1, H2, M1, M2, and M4, found M3 only partially fixed, and returned C1–C3 on the T4 client. Neither pass-2 report contains a fix commit for N1–N4 or C1–C3.

| Item | What the report says | Commit | Final status |
| --- | --- | --- | --- |
| N1 | every one-shot RPC event read validates full journal contents at attach | none named | pending |
| N2 | `result_imported` differs between repair facts and the producer | none named | pending |
| N3 | the server echoes the client's `baseline_after` and does not capture H | none required; documented in `docs/usage.md` | pending |
| N4 | display titles are truncated to 120 bytes, not 512. The 120-byte cap stays | none required; documented in `docs/usage.md` | pending |
| C1 | a cold start of current attention was consumed with no summary | none named. Intended: one-shot and first start show one summary of tasks waiting for attention | pending |
| C2 | a same-epoch cursor repair emitted individual banners | none named. Intended: a cursor repair coalesces its fresh decisions into one summary. This is the rest of M3 | pending |
| C3 | the RPC client rewrote `CONTROLLER_EVENTS_REPAIR_REGISTRY_TOO_LARGE` as generic unavailable | none named. Intended: the laptop keeps the explicit code and the fixed message `repair unavailable, registry too large` | pending |

## Live acceptance checklist

Every item is `pending — live, after deploy`. The kit in `.briefs/acceptance-kit.md` is the integrator's script map. T9 did not run any kit script.

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
