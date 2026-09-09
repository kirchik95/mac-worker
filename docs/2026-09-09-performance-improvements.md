# Isolated snapshot and dashboard lookup performance (2026-09-09)

This records a **completed subset** of roadmap Stage 5, not the whole stage. Source was integrated only on `performance-validation-20260909`. Main was not mutated.

## What landed

Cherry-picked onto starting base `69aa8a652633c9b1b708a180ca8e9b3a5e3730ab` (relevant implementation vs original baseline `7ea2bf355e840798623ff89c7fd995747464aa59` was empty). Trees of the three immutable commits match the cherry-picks.

| Immutable commit | Cherry-pick on this branch | Behavior |
|---|---|---|
| `1a11257a9befa5d3d834d00491cbebd157d4bdf1` | `3177fc66ec2835036f3836843e17b99b663561cb` | One NUL-framed `update-index --add -z --index-info` per nonempty `capture_tree`; H=0 skips `update-index` (`read-tree --empty` + `write-tree`). Scratch index cleanup restores owner-regular mode then `remove_owned_regular` for `{index}.lock` and `{index}`. |
| `41b02be70527021f25af7baade4ac9d51a2d99e5` | `b4184a19d6ddfbf941364efd78446165f0b1ecec` | Per-`build_wip_base` memo: SHA-256 of freshly read bytes → Git blob OID. Both captures still inspect and read every selected non-directory. Mode comes from the current inspection and is not memoized. The map dies with the build. |
| `45f02013da71ff809d8ad95777ba781c79bd573a` | `1c0bb51703c65587d25c658c046f236f671c6fab` | `MacWorkerTaskSource::owned_record` uses locked `ClientStateStore::load_task_optional`. Missing addressed JSON is `TASK_NOT_FOUND`. Missing/damaged `tasks/` and corrupt targets stay `IO`. Collection still uses `list_tasks()`. |

**Measured source HEAD** for CI and timings: `1c0bb51703c65587d25c658c046f236f671c6fab`.

Addressed detail/log no longer fail because an unrelated sibling record is corrupt. That independence is intentional. Target corruption still fails. `list_tasks` / collection still scan and still fail on a corrupt sibling. Writer exclusion is preserved: the point read holds `StateLock`, the same lock `update_task` uses around replacement. Unlocked `load_task` is unchanged for nested callers.

Snapshot safety kept: rooted no-follow reads, two full captures, `SNAPSHOT_CHANGED` on mismatch, Git still constructs objects. Cleanup is **best effort** (`let _ =` on scratch removal); this does not claim every failure path is now impossible.

Memory of the memo is digest keys, OID strings, and map/string overhead for distinct contents in one build. It is not an exact 32+40 byte total per entry. On a hash miss the owned file `Vec` is moved into `ProcessRequest.stdin`; `SystemProcessRunner` may still clone that buffer for its writer thread.

This increment does **not** complete Stage 5 item 1 as written (first capture still runs `hash-object` per distinct content; there is no batched hash-object protocol), does not change admission/probes, and does not add no-op-write skipping, background observations, SSH multiplexing, or origin-delivery changes.

Historical findings in [2026-09-08-architecture-and-flow-review.md](2026-09-08-architecture-and-flow-review.md) stay dated observations, not a rewrite of today’s source. An earlier `transfer_repo` green run that failed until `GIT_DIR` was unset was a contaminated fixture environment, not a current product regression. Unusual-name coverage uses `ls-tree -r -z`; it does not demonstrate that old `--cacheinfo` dropped comma filenames.

## Commands and evidence

Artifact root: `/private/tmp/mac-worker-performance-implementation-hq98vgb7`. Throwaway probe preserved under `validation-optimized/` and removed from the worktree before `--all-targets` and after timings. `CARGO_TARGET_DIR=/private/tmp/mac-worker-performance-validation-target`. Git probe variables unset for cargo/git. `--locked --offline` on test/clippy/measurements.

| Step | Log | Exit |
|---|---|---|
| `cargo fmt --all --check` | `logs/ci-fmt-check.log` | 0 |
| `cargo test --locked --offline --all-targets -- --test-threads=1` | `logs/ci-test-all-targets.log` | 0 |
| `cargo clippy --locked --offline --all-targets -- -D warnings` | `logs/ci-clippy.log` | 0 |
| A: `STAGE5_COUNT_CONTRACT=optimized` `--exact stage5_validation_a --test-threads=1` (release) | `logs/measure-a-optimized.log` | 0 |
| B: same pin, `--exact stage5_baseline_b` | `logs/measure-b-optimized.log` | 0 |

Full Rust on `1c0bb51`: **67** `Running` target headers and **67** unfiltered `test result: ok` rows; **1710** passed, **0** failed, **0** ignored. Three filtered helper-subprocess result rows are excluded from that total. Command wall ~797.8 s (`logs/ci-test-all-targets.*`). Do not copy the older Stage 4 figure of 1615 / 738.252 s. No UI rebuild. GitHub Actions and the live pool were not run.

Harness and JSON: `validation-optimized/stage5_performance_probe.rs` SHA256 `58d8be876096caf53ceb050c15cc860b8c3d18bd09956cf501558aac16565ed6`; `validation-a-results.json` `46b109cb303026733a0ad7759aa93551cfc608bd210b7172b0d792beaacab7af`; `validation-b-results.json` `524f5731a74ea5a6159f4909e4e24566358b614234b207170ff3201231d12f7d`; combined `validation-results.json` `f866e030dfd46ae3fb98bf5891aa5ee86c6ec390d3a8372ca95a9b853d43a4d8`. Before-values are the immutable accepted baseline at `7ea2bf3` (`/private/tmp/mac-worker-performance-baseline-e6vwxdie`), not a rerun of old A/B/C.

**Metadata erratum (do not treat as a timing defect):** top-level `original_baseline_commit` / `starting_base_commit` / `measured_commit` in the executed JSON are `7ea2bf3` / `69aa8a6` / `1c0bb51`. Nested `count_contract.baseline.measured_starting_base` was filled from `MEASURED_COMMIT` (`1c0bb51`); the intended starting-base label is `69aa8a6`. Raw capture is preserved; this document uses the correct top-level IDs. No timing rerun for the label.

Host during combined B metadata: Darwin 25.2.0 arm64, ncpu 16, rustc 1.98.0, git 2.50.1 (Apple Git-155), loadavg `{ 7.24 9.03 12.43 }`. Load was uncontrolled.

## Measured before / after

Wall samples are 1 discarded warmup + `repeats_for(pilot)` measured; median is the even-sample arithmetic mean of the two central values when `n` is even. A times only `build_wip_base`. B does not include setup in request timers. Count pass is separate (`CountingRunner`); wall uses `SystemProcessRunner`. `STAGE5_COUNT_CONTRACT=optimized` asserted labelled D / 2-or-0, not counts inferred after the fact.

Unique-payload fixtures: D=H. Observed counts matched that labelled contract: H=0 → 0 hash-object, 0 update-index (17 other git); H=100 → 100 / 2 (121 git); H=1000 → 1000 / 2 (1021 git). Correctness (NUL-safe `ls-tree -z` + `cat-file`) passed.

### A — reused clean / empty WIP (`build_wip_base`)

| Cell | Before `7ea2bf3` median | After `1c0bb51` median |
|---|---|---|
| H=0 empty reused | 166.892 ms (n=5) | 214.283 ms (n=5) |
| H=100 clean reused | 4155.256 ms (n=5) | 1068.689 ms (n=5) |
| H=1000 clean reused | 56431.915 ms (n=2) | 8207.761 ms (n=5) |

H=0 got slower on this host. Extra scratch cleanup and different load were **not** timed separately; do not treat the empty-case increase as a proven causal cost of the new cleanup, and do not treat H=100/H=1000 median drops as a guaranteed speedup. H=1000 before/after also use different `n`.

### B — Closed tasks, fake remote, durable turn dirs

Byte equality held on the 1 MiB drain (16 chunks / 16 log requests). Closed `task_detail` asserted 0 remote `task_status` calls.

| N | Call | Before `7ea2bf3` median | After `1c0bb51` median |
|---|---|---|---|
| 100 | `task_detail` | 3.312 ms | 0.204 ms |
| 1000 | `task_detail` | 30.711 ms | 0.208 ms |
| 10000 | `task_detail` | 344.566 ms | 0.205 ms |
| 100 | one 64 KiB log chunk | 3.150 ms | 0.257 ms |
| 1000 | one 64 KiB log chunk | 30.695 ms | 0.257 ms |
| 10000 | one 64 KiB log chunk | 363.330 ms | 0.252 ms |
| 100 | 1 MiB / 16-chunk drain | 52.266 ms | 4.383 ms |
| 1000 | 1 MiB / 16-chunk drain | 510.913 ms | 4.487 ms |
| 10000 | 1 MiB / 16-chunk drain | 5601.241 ms | 4.322 ms |
| 10000 | `list_tasks` (control, still full scan) | 337.158 ms | 331.743 ms |
| 10000 | `load_task` (unlocked lower bound) | 0.091 ms | 0.090 ms |

Detail/log no longer scale with history size in this fixture; collection/`list_tasks` still does. Host load and OS cache were not purged. These are not fleet RTT, fsync, RSS, or submit-path measurements. No C/admission group.

## Caveats

- Isolated branch only; await root for main integration.
- Do not quote a raw sample as a median; do not include fixture setup in request time.
- No claim that all Stage 5 work is done.
