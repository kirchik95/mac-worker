# Task-write skip and non-overlapping dashboard polls (2026-09-09)

This records **Stage 5 item 3 in part**: skip ordinary identical `LocalTaskRecord` replacements, and poll TaskDetail / `useSnapshot` only after the previous request settles. It is **not** the whole of item 3 and **not** the whole of Stage 5. Point lookup by task ID landed earlier ([2026-09-09-performance-improvements.md](2026-09-09-performance-improvements.md)). Background observation collection independent of HTTP remains open. Item 2 admission cache is unchanged: [2026-09-09-admission-performance.md](2026-09-09-admission-performance.md).

**Measured pair:** `e8c1bb24db30aadf3e7f54ce5933cf1fd3d1087b` → `915227f354a847a1dabab453ca35d528493db567`. Fully tested source is that after SHA. File identity is a count of atomic replacements (`symlink_metadata` `dev`/`ino`), not an fsync census and not a fleet latency claim. UI numbers are request starts and live in-flight under fake timers, not wall-clock speedup.

## Full gate on `915227f`

Default-parallel Rust and full UI on the isolated validation worktree `io-polls-review-20260909`, `CARGO_TARGET_DIR=/private/tmp/mac-worker-io-polls-n5b3h3by/validation/target`, pre/post HEAD `915227f`. Evidence: `/private/tmp/mac-worker-io-polls-n5b3h3by/validation/final-gate/`. GitHub Actions and the live pool were not run.

| Gate | Exit | Elapsed | Log sha256 |
|---|---|---|---|
| `cargo fmt --all --check` | **0** | 1.011 s | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `cargo test --locked --offline --all-targets` | **0** | 610.342 s | `73c5d42d7c4cebaf411ca7149ba8316602e2be69b3c4c63aa405b14ed896dd1c` |
| `cargo clippy --locked --offline --all-targets -- -D warnings` | **0** | 10.786 s | `fb65b8654ee0c5bcaedd8e28b899c1269db7d71ca44f70994fdb5c72df7bc610` |
| `npm test` | **0** | 3.026 s | `0030963d7be79dadaa50b58db0c66c72f196960573025c77bc086b9a567878c9` |
| `npm run lint` | **0** | 0.820 s | `9ee40e30498acd376d133b190158cce71adba56bb3f72f89134c9072defa585a` |
| `npm run build -- --outDir …/final-gate/ui-build-out` | **0** | 2.607 s | `a5dc47583c40e017e8c2814e38d2c2ee3f925da0b47b552b557acf6f16842dfe` |
| `diff -r` build vs `src/dashboard/static/app` | **0** | 0.004 s | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` (empty) |
| `git diff --check` | **0** | 0.011 s | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` (empty) |

`--all-targets`: **68** `Running` targets and **68** unfiltered top-level result rows; **1812 passed, 0 failed, 0 ignored**. Three nested helper rows are excluded (two `1 passed / 345 filtered` lib helpers and one `1 passed / 10 filtered` `project_inspection` helper). Do not count 1815. UI: **12 files / 105 tests** passed. Lint exit 0 with **15** existing oxlint warnings (including `useTurnLog.ts` and `TaskDetail.tsx` set-state/ref notes); no unrelated warning cleanup. Fresh build compared to tracked embed is byte-equal (empty `diff -r`). Tests ran 2026-09-09T20:52:43Z–21:02:53Z; UI build+diff finished 21:03:10Z. rustc `1.98.0 (88d9e12ae 2026-08-18) (Homebrew)`.

## What landed

Ordinary `update_task` and `update_task_if_current` (`pub(crate)`) pass `IdenticalTaskWrite::Skip`. Under StateLock both still run the rollback-fault probe, canonical serialize, `tasks_dir`, residue recovery, secure read, canonical parse, and filename identity. Expected-record comparison applies only when `update_task_if_current` supplies an expected snapshot: a stale expected returns `Ok(false)` even if `replacement == existing`. After those checks, an identical replacement skips the durable exchange (`replace_private_regular_exact_with_sync_hooks`). The private helper returns `Ok(true)` on that skip; public `update_task` still returns unit success. Equality is the complete `LocalTaskRecord` after canonical parse (timestamps included). There is no timestamp or schema normalization to manufacture a match. Residue directory scans remain.

`IdenticalTaskWrite::Replace` still exchanges (and runs `before_final_sync`) for `clear_submission_intent` and runner-handoff locked updates even when the parsed record is already identical. Changed status / runner / `status_observed_at` still replace. Corrupt / unsafe / non-canonical still fail closed.

TaskDetail and `useSnapshot`: one in-flight request per effect generation; first call is immediate; next poll is **2 s after the previous request settles** (success or non-abort error). Cleanup aborts and clears the timer so a stale generation cannot apply a result or schedule. Last-good on error of the same task, retry, task-switch blanking, and unmount abort are preserved. `useTurnLog` is unchanged. No request timeout was added. Collection is still on the HTTP path.

## Observed counts (not fleet RTT)

Artifact root: `/private/tmp/mac-worker-io-polls-n5b3h3by`. Isolated harness crate, `--locked --offline`. `ClientStateSyncCounts` is not a task-write census. `update_task_if_current` is not driven from the public inode harness (crate tests cover matching skip and stale CAS).

Frozen harness (identical before/after):

| Item | sha256 |
|---|---|
| `validation/frozen/task_update_baseline.rs` | `afe0ae9220a29c395c82a7db1726336bcaf2798b703baa45f6e74848c4fb2071` |
| `validation/frozen/Cargo.toml` | `805cfd169036549f13370931d754a6038e76e565cf8a93991bb9057e57f6786e` |
| `validation/frozen/Cargo.lock` | `bd37361f282d714ac9c355aa4dce5e2832a134dd48144d941cacdc586ae941ef` |
| `validation/frozen/poll-baseline.test.tsx` | `5de0da4140301ebb22729748691b686a64ecf9da869ac727f071392f457cedd2` |
| `ui/vite.config.ts` | `0095f5fc291aa282d3ba06c26e03ce890b353406fb7f142da96266eb13097f31` |

Rust (public `update_task` / `clear_submission_intent`; 5 identical or 5 changed reps where noted):

```
export CARGO_TARGET_DIR=/private/tmp/mac-worker-io-polls-n5b3h3by/validation/target
export BASELINE_KIND=before   # or after
export BASELINE_OUT=/private/tmp/mac-worker-io-polls-n5b3h3by/validation/raw/${BASELINE_KIND}-task-update.json
cargo run --locked --offline --release --manifest-path /private/tmp/mac-worker-io-polls-n5b3h3by/validation/harness/Cargo.toml
```

UI (Vitest 5.0.0 from worktree `ui/node_modules/.bin/vitest`; existing `ui/vite.config.ts`; temporary untracked `ui/src/poll-baseline.test.tsx` copy, removed before the full gate):

```
export BASELINE_KIND=before   # or after
export BASELINE_OUT=/private/tmp/mac-worker-io-polls-n5b3h3by/validation/raw/${BASELINE_KIND}-ui-poll.json
cd ui && ./node_modules/.bin/vitest run --config ./vite.config.ts src/poll-baseline.test.tsx
```

All four measurement commands `ec=0`. JSON:

| Run | HEAD | json sha256 |
|---|---|---|
| Rust before | e8c1bb2 | `d73fb6940589003293465ee6e1c3038e9d5461d9edc105f8b78662bddc9fdc7e` |
| UI before | e8c1bb2 | `8a63c21c2941e12513c83c9e168341df4eb469d2229611a7cc87f81f7302bf1f` |
| Rust after | 915227f | `73c0a437253f4f8ab6750c7503468decd6bdfc042fbcb7b3c782f278e074cede` |
| UI after | 915227f | `010582d0e1e45f7be1be9982943d6d4eb9c832b698312283768f2f0dae946e4a` |

| Scenario | Before | After |
|---|---|---|
| Ordinary identical `update_task` ×5 | 5 calls, **5** inode changes, residue 0 | 5 calls, **0** inode changes, residue 0 |
| Changed `status_observed_at` ×5 | 5 / **5** / 0 | 5 / **5** / 0 |
| Changed inject then identical retry of now-current | **2** inode changes; plant residue 1 then 0 | **1** (plant only; retry inode stable, residue 0) |
| Corrupt bytes then `update_task` | inode unchanged; error `task record is corrupt` | same reject |
| `clear_submission_intent` then repeat already-cleared | **2** inode changes | **2** (Replace kept) |
| TaskDetail pending 6000 ms | requests **4**, live peak **4** | **1** / **1** |
| `useSnapshot` pending 6000 ms | **4** / **4** | **1** / **1** |
| TaskDetail error recovery | 3 requests, live peak 1 | 3 / 1 (last-good recovered) |
| TaskDetail task switch | 2 starts, 1 aborted, live peak 1 | unchanged |
| StrictMode at mount | 2 starts, 1 aborted, live peak 1 | unchanged |
| StrictMode after 6000 ms | **5** starts, live peak **4**, 1 aborted | **2** / **1** / 1 aborted |

`abortedBySignal` is the StrictMode / task-switch predecessor. It is **not** live network concurrency. After: a pending first request does not start more polls; that aborted predecessor does not schedule another 2 s timer.

Changed-fault error string both sides: `injected local state failure after task replacement exchange before first directory sync`.

## Verification history (not the successful pair)

A production skip prototype was discarded before assertion RED on compilable exactbase. Archived harness setup failures are preserved and excluded from the pair: `validation/raw/superseded-pre-correction-20260909T203245/` and `validation/raw/superseded-ui-timeout-20260909T204231/`.

## Still open

Background observations independent of HTTP, status/log transport, SSH multiplexing, origin delivery, and the rest of Stage 5. `QUEUE_TIME_REGRESSION` on ungated concurrent enqueue remains a separate Stage 5.2 leftover. This increment does not close item 3 or Stage 5 as a whole.
