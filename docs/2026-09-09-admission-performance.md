# Admission cache before SSH (2026-09-09)

This records **Stage 5 item 2** (bound skip-SSH admission plus the existing 3-wide `WorkersService` pipeline), not the whole stage. Item 1 snapshot/blob work is unchanged: [2026-09-09-snapshot-batch-performance.md](2026-09-09-snapshot-batch-performance.md), [2026-09-09-performance-improvements.md](2026-09-09-performance-improvements.md). No-op writes, background observations, SSH multiplexing, status/log transport, and origin delivery are still open.

**Measured pair:** `8f10ed2936d1156c6d8631f0efb61de15d6cad7d` → `7519441fe3da6a45ef4e01a7b511efa55dfe1586`. Identical V2 harness source sha256 `ccc87bdcf8e615f39d3fbed77177c0f447b6ad90a0deb2420c3fe4f10a4f71f3`. Executables: before `9a30e3d895fb3cc1160bf9ca5be71421e69f5707f5ee5b7cb8dd26a9cce47bc8`, after `3e9e3a776dd4aaeee105af2d53d0296c5296fc39f115e69d139622f5da4b15a7`.

**Combined runtime (not retimed):** `96ccc2e356a547ff3ce99052995304617bd3de03` (merge of `7519441` with exact main `1db117d50d8edead6fb935017d78b0b951ad0a23`; follow-up threads Herdr `interactive_agents` through the shared admission path). Production admission/runtime logic is unchanged since that SHA. Test-only `ee6cec34bb1fe65c9c164d0accadb57318f33b28` (init fixture), `1c06046cef58ae7078d78aa18bf85a4460f9890a` (revisit/near-TTL fixtures), and `0745f2bb37ff8d031deb756e389dc0c4f313c32a` (publication receipt) sit on top; `#[cfg(test)]` in `src/admission.rs` and `tests/init_command.rs` changed. Fully tested source: `0745f2b`.

## Combined-runtime gates

Failed attempts are not green and are not PENDING. Nested helper-subprocess summaries are excluded from the success totals (two `1 passed / 342 filtered` lib helpers and one `1 passed / 10 filtered` `project_inspection` helper). Previous failed log hashes are unchanged.

| Attempt | SHA | Result | Log |
|---|---|---|---|
| 1 | `96ccc2e` | `--all-targets` exit **101**, 823 passed, 1 failed (stale init fixture); later crates not reached | `validation/final/all-targets.log` sha256 `3d18e01e3e377ab30d00b63fc8e13f09e57ff82958e70d35e6b27e6a6e1a41b8` |
| 2 | `ee6cec3` | lib 342 passed, 1 failed (revisit fixture timing); later crates not reached | `validation/final-retry/all-targets.log` sha256 `6647d3cd1bc57c15ce5b9a98999ed2f2c766653c0e84473a06f9e9766e1db068` |
| 3 | `0745f2b` | **green** (worktree clean, harness absent from `tests/`) | `validation/final-retry2/` |

Third-attempt captured statuses (`ec=$?` immediately after each cargo process), `RUST_TEST_THREADS` unset, isolated `validation/v2/target`:

| Gate | Exit | Elapsed | Log sha256 |
|---|---|---|---|
| `cargo fmt --all --check` | **0** | 1 s | `f0d0e9762ac1065cf3af5f5f85681fdf5bcead5ef4c9d80ae7cc39bd0bd27e2e` |
| `cargo test --locked --offline --all-targets` | **0** | 585 s | `4ef8de1fa16211a14cff6ba2d7be23c56236dd16cb034104df37d692f6bd2035` |
| `cargo clippy --locked --offline --all-targets -- -D warnings` | **0** | 4 s | `09a5fa9a210f7af15bfa3f99d6039f3277f516bc02ce4c36e87fb6b979064718` |

`--all-targets`: **68** top-level `Running` targets, **1794 passed, 0 failed, 0 ignored**. Includes lib 343 passed and `init_command` 17 passed. Report: `validation/final-retry2/result.md`. This closes the Stage 5.2 combined gate; the rest of Stage 5 stays open. The measured pair was **not** retimed.

## What landed

Shared `src/admission.rs` for `TaskClient` submit, `TurnRunner`, and remaining-peer reassignment. Existing pieces reused, not replaced: `WorkersService` (max 3), per-worker refresh lock, CAS publish, `SchedulerProbeAdapter`, pins / FIFO / run caps, claim / lease / prebind, Stage 1 worker isolation. Local worker config is validated before any cache hit (`slots != 1` is `WorkerError::Config` without SSH).

A complete binding that matches the inventory, is not in the future, and is within the **2 s** outer TTL (`OBSERVATION_TTL_MILLIS`) may skip SSH. Ready rows also need usable inner facts (15 min `FACTS_TTL`). Bound `ready=false` within the outer TTL may skip SSH without `facts_age`. Incomplete/legacy 7- or 8-field files are misses. No deletion.

Misses enter one 3-wide pool: that worker’s refresh lock, re-peek, probe, optional refresh, re-probe, CAS. One absolute **60 s** round covers lock waits + 15 s probe + 30 s refresh + 15 s re-probe. Unstarted work at remaining 0 does not publish. Budget skips after a Ready probe (no refresh or re-probe budget) stay ephemeral (`Ok(None)`); they do not CAS a synthetic negative. Sampled ranking uses the **final probe start**. Cached reused hits re-validate outer TTL at return. Ready cache never skips lease/prebind/claim. Local caller errors still propagate; remote worker failures stay isolated as unavailable. `inspect` / `inspect_with_budget` and `RunService` workflow are unchanged. `PROTOCOL_VERSION` is unchanged.

## Observed benefit (counts, not fleet RTT)

Local fake **50 ms** delay on probe and refresh-facts; 9 scenarios × 5 reps = 45 samples each side; `test` unoptimized + debuginfo; rustc 1.98.0; busy uncontrolled host (before load 16.87 12.89 11.96, after 15.83 12.93 12.08). Filter: `baseline_harness_records_fixed_admission_samples` only. Counts are the stable evidence. Wall differences are observed on this host; **do not treat them as a guaranteed fleet speedup**, and **do not assert host load caused the increases**. No independent overhead breakdown exists. Live fleet, 15/30/60 s budgets, and provider contention were not measured.

Named after-side count changes that held:

- **cold n=5 submit:** still 5 probes; peak 1→3 (existing pool). Median wall 834→681 ms; `admission_ssh_span_ms` **302→126** ms because synthetic probes overlap.
- **runner after a fresh submit:** host probes 1→0 (runner reuses the submit row). Median wall 886→752 ms (runner-only; priming submit excluded).
- **one offline worker:** probes 4→2 (submit probes both peers at peak 2; runner does not add probes). Median wall 1591→1384 ms. Span no longer includes the submit→runner local gap.

Warm, repeated, n=1, and stale-facts **skip-SSH / refresh counts are unchanged**. Median walls that **increased** (ms): n1 563→599, warm 415→475, stale-facts 679→731, repeated 415→467, overlap-cold 312→357, expired-contention 303→373.

## Timer scope

From the frozen V2 functions (`validation/v2/frozen/admission_baseline_harness.rs`):

- `cold_automatic_n1_submit` / `cold_automatic_n5_submit` / `stale_facts_refresh_submit`: timed `TaskClient::submit` only (`baseline_harness_cold_n`, `baseline_harness_stale_facts`).
- `warm_submit_after_fresh_cache`: untimed priming submit, then timed `TaskClient::submit` on a fresh counted transport (`baseline_harness_warm_submit`).
- `repeated_submit_within_ttl`: untimed first submit, then timed second `TaskClient::submit` (`baseline_harness_repeated_submit`; extra probes subtracted).
- `runner_after_fresh_submit`: untimed priming submit, then **only** `TurnRunner::run` on a fresh counted transport (`baseline_harness_runner_after_submit`). Priming SSH/Git is outside the wall and counts.
- `one_offline_worker_automatic`: timed `TaskClient::submit` then `TurnRunner::run` on the same counted transport (`baseline_harness_offline_peer`).
- Two overlap scenarios (`concurrent_cold_same_worker`, `expired_cache_refresh_contention`): time to post-admission WIP `write-tree` **before enqueue**; `full_submit: false`. They are not full-submit latency and do not prove RefreshLock wait or overlapping admission SSH (`overlapping_admission_ssh` is false; peak is 1 on both sides). Both already had 1 host probe. Directly observed: both `TaskClient` calls in flight during the leader probe gate; loser `ProcessRunner` during that gate; both hit write-tree.
- `admission_ssh_span_ms`: first admission SSH enter to last exit in that window, **including gaps**. Not SSH-active time. Offline includes the submit→runner local gap on that same transport.
- V1 concurrent-claim labels are **withdrawn**; V2 source corrected the stop. Do not compare V2 overlap walls to V1.

## All nine V2 scenarios

Median is the middle of five sorted walls. Probe/refresh/peak are constant across the five reps. From `/private/tmp/mac-worker-admission-cache-c1h3wX/validation/v2/raw/before-samples.json` and `after-samples.json`.

| Scenario | probe b→a | refresh | peak b→a | wall min/med/max before | wall min/med/max after | span before | span after | timer |
|---|---|---|---|---|---|---|---|---|
| cold_automatic_n1_submit | 1→1 | 0 | 1→1 | 545 / **563** / 680 | 588 / **599** / 631 | 50–55 | 50–55 | full submit |
| cold_automatic_n5_submit | 5→5 | 0 | 1→**3** | 803 / **834** / 893 | 667 / **681** / 697 | 294–309 | 123–133 | full submit |
| warm_submit_after_fresh_cache | 0→0 | 0 | 0→0 | 401 / **415** / 497 | 455 / **475** / 501 | none | none | full submit; priming excluded |
| runner_after_fresh_submit | **1→0** | 0 | 1→0 | 765 / **886** / 920 | 682 / **752** / 816 | 52–55 | none | runner only; priming submit excluded |
| stale_facts_refresh_submit | 2→2 | 1→1 | 1→1 | 661 / **679** / 882 | 695 / **731** / 758 | 159–165 | 157–163 | full submit |
| one_offline_worker_automatic | **4→2** | 0 | 1→**2** | 1538 / **1591** / 1653 | 1319 / **1384** / 1473 | 650–682 | 53–55 | full submit+run |
| repeated_submit_within_ttl | 0→0 | 0 | 0→0 | 409 / **415** / 448 | 439 / **467** / 482 | none | none | full submit; first excluded |
| concurrent_cold_same_worker | 1→1 | 0 | 1→1 | 307 / **312** / 328 | 339 / **357** / 385 | 50–55 | 52–55 | write-tree stop |
| expired_cache_refresh_contention | 1→1 | 0 | 1→1 | 299 / **303** / 325 | 331 / **373** / 434 | 50–55 | 51–55 | write-tree stop |

The two overlap rows use the write-tree stop above. All 10 after overlap samples matched the V2 stop/overlap invariants. jsonl walls match JSON. Submit/run outcomes were not ignored.

## Reproducing the pair

Artifact root: `/private/tmp/mac-worker-admission-cache-c1h3wX`. Frozen V2 source is `validation/v2/frozen/admission_baseline_harness.rs`; it is **not** in shipped `tests/`. Do not point `--target-dir` or `ADMISSION_BASELINE_OUT` at the archived `validation/v2/target` or `validation/v2/raw/` files. This is a reconstruction recipe, not a rerun.

Copy the frozen harness into an **isolated** worktree’s `tests/` first. Example **before** at `8f10ed2` (HEAD must match `ADMISSION_HARNESS_HEAD`):

```
cp /private/tmp/mac-worker-admission-cache-c1h3wX/validation/v2/frozen/admission_baseline_harness.rs \
  tests/admission_baseline_harness.rs
ADMISSION_HARNESS_KIND=before \
ADMISSION_HARNESS_HEAD="$(git rev-parse HEAD)" \
ADMISSION_BASELINE_OUT=/tmp/admission-v2-recheck-before-samples.json \
cargo test --locked --offline --test admission_baseline_harness \
  baseline_harness_records_fixed_admission_samples \
  --target-dir /tmp/admission-v2-recheck-target \
  -- --exact --nocapture
```

For **after**, check out `7519441fe3da6a45ef4e01a7b511efa55dfe1586`, copy the same frozen harness again, set `ADMISSION_HARNESS_KIND=after`, `ADMISSION_HARNESS_HEAD="$(git rev-parse HEAD)"`, and a **new** `ADMISSION_BASELINE_OUT` (for example `/tmp/admission-v2-recheck-after-samples.json`). Keep a separate `--target-dir`. Archived measurement JSON/logs stay the evidence of record.

Archived pair (do not overwrite): `validation/v2/comparison-result.md`; `validation/v2/raw/before-samples.json` / `after-samples.json`; `validation/v2/raw/before-run.log` (captured exit 0, real 43.75 s); `after-run.log` (test summary: 1 passed; command exit status not captured (wrapper metadata error); note `/private/tmp/mac-worker-admission-cache-c1h3wX/validation/v2/raw/after-run.exit-metadata.md`); `validation/v2/frozen/hashes.json`. V1 frozen source/binary under `validation/` were not overwritten. The `exit=0` now at the end of `after-run.log` is a later log edit, not a captured cargo wait status.

## Compatibility

Old unbound 7-field rows, and main’s 8-field unbound rows with `interactive_agents`, load as skip-SSH **misses** unless the six binding keys are also present and complete. New bound 13/14-field rows are not readable by an older `deny_unknown_fields` binary. No remote protocol bump, no task-record migration, no observation deletion. Downgrade is fail-closed on that worker’s cache row, not seamless.

## Still open

- Pre-existing `QUEUE_TIME_REGRESSION` on ungated concurrent full submit (enqueue timestamps). Diagnostic: `/private/tmp/mac-worker-admission-cache-c1h3wX/validation/diag/queue-time-regression.md`. The two overlap cells stop before enqueue to avoid it; the runtime queue is **not** fixed.
- Rest of Stage 5 (no-op writes, observation collection, status/log transport, origin delivery, …). Stage 5.2’s combined `fmt` / `--all-targets` / Clippy gate on `0745f2b` succeeded as recorded above; it does not close the rest of the stage.

## Material validation limit

An earlier faulty fixture forwarded `/usr/bin/git` (including worker push). One run reached `BASE_PUSH_FAILED: base push failed`: production spawned git with `GIT_SSH_COMMAND=/usr/bin/ssh` and `mac1:<project_id>` and got a non-zero status. No captured stderr/request dump proves SSH/TCP or a data transfer. Do **not** claim “no network attempts occurred.” V2 simulates known remote Git, panics on unknown remote ops, and forces `GIT_ALLOW_PROTOCOL=file` on real passthrough. Diagnostic: `/private/tmp/mac-worker-admission-cache-c1h3wX/validation/diag/base-push-failed-transport.md`. Temp fixture `base.txt`, not a user repo; no credentials/profile reads.
