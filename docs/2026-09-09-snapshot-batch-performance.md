# Bounded Git blob batching (2026-09-09)

This records a **completed Stage 5 item 1** (bounded Git blob protocol), not the whole stage. Admission/probes, no-op writes, background observations, SSH multiplexing, and origin delivery are unchanged. The prior subset (batched `update-index`, per-build digest memo, scratch cleanup, locked dashboard lookup) remains in [2026-09-09-performance-improvements.md](2026-09-09-performance-improvements.md) at `1c0bb51`.

**Measured source HEAD:** `39f5909f02dc77e14e9203d42e2788414613df8a` (merge of local main `d1c239d`, then cherry-picks of immutable `afff5641e407d5bbb4727f4b6a29eb137c6394cc` and `ece7bf9b8bac0ea8a543dfc01e5bb4e00f3f536c`). Frozen current-before: `08a6cd8560c85445f97b9e8bc795d471dd326218`. `src/transfer_repo.rs` blob `cc17d04eb409f1fe856809b1b94ae823a7d44a26` is afff564 plus the retained main `task_config` helper (`WorkerError::task` / `Cow`); the file is not byte-identical to afff564. `tests/transfer_repo.rs` matches ece7bf9 (`342798baae59b0c7409f2d2e11a701afe11fefaa`).

**Full Rust CI HEAD:** `6603f448cc5a216069f47e96d31d2f70a3b72f06`. After retaining parallel main, `tests/init_command.rs` still treated the Gatekeeper `--version` warm-up reply as the verification probe. One scripted successful `--version` response was added in that fixture only. Snapshot runtime did not change; timings on `39f5909` remain the measured source. The first `--all-targets` on `39f5909` was not green (`logs/ci-test-fail-init-warmup.*`).

Artifact root: `/private/tmp/mac-worker-snapshot-batch-wge5a0er`.

## What landed

WIP blob writes in `TransferRepo::build_wip_base` still do two fresh rooted captures. Distinct payloads are batched:

- Unique pending bytes **≤ 1 MiB** and **≤ 64** unique objects per `git fast-import --quiet --done` stream (length-framed `blob` / `mark` / `get-mark`; payload bound excludes framing).
- A pending flush with **one** unique object uses existing `hash-object -w --stdin` (`--no-filters` for regular files). Two or more unique objects use `fast-import`. Exactly **1 MiB** is not oversized; a lone 1 MiB blob hashes because the batch is a singleton. A 1 MiB payload plus a unique empty blob still share one two-object `fast-import`.
- Payloads **strictly > 1 MiB** flush pending first, then one `hash-object` (8 MiB control).
- Zero-byte is an ordinary unique pending object (one slot, 0 payload bytes). No hardcoded empty OID.
- Per-build SHA-256 → OID memo; mode is not memoized. Second capture of unchanged bytes is memo-only.
- Writes use the existing transfer Git envelope (`--git-dir`, `gc.auto=0`, isolated config). Crash residue stays under existing GC; this does not add a cleanup protocol. Default loose vs pack is representation, not a correctness promise. Pack+idx remains valid Git if unpackLimit keeps the pack.

The first pending payload stays raw until a second unique digest arrives; a multi-object flush frames ≤1 MiB of unique payload plus bounded headers, and the runner may still clone stdin. This is not a total-app memory cap: the current file or oversized `Vec` can be large; the per-build digest→OID memo and staged path/index metadata grow with D/H; two raw payloads may coexist on Single→Multi while their sum fits the cap; there is no whole-project byte buffer.

An earlier candidate on `db973ba` (`b114ed9`) flushed every batch through `fast-import`, including 16 singleton 1 MiB streams. A controlled command microbenchmark showed that singleton `fast-import` adds nested `unpack-objects` without reducing outer process count vs `hash-object`. That candidate is **not** the final result. The singleton-hash path is the approved fix (`afff564`).

## Commands and evidence

`CARGO_TARGET_DIR=/private/tmp/mac-worker-performance-validation-target`. `--locked --offline`. Git/Trace2 vars unset. Throwaway probe archived under `validation-singleton-harness/` and removed before `--all-targets`. Frozen current/eventual JSON were not rewritten.

Executed harness identities (timers, fixtures, warmup, n unchanged; expected-count pin/output only on the final copy): original current/eventual `efb9c5c75cc0ba53ceaf849f5ce57348d989e4f94d084f6a29b1a78ce4513943`; final `547d9deb930d890a5a5a8ab810a6c93c985e3c7e5860297d952790cc893157f4`. Wall A JSON: current `7d7f19bffa71c42a006a9edf021402e9bae06dfd7fd2141bd26dd3fda8fbee34`; final `745e6d1a8cbe377b3664d8d7cd7440c4a45a21d1c36925932a262356f91309ec`. Remaining hashes: `SHA256SUMS-final.txt` in the artifact root.

| Step | Log | Exit |
|---|---|---|
| A wall, n=5+warmup, `STAGE5_COUNT_CONTRACT=final` | `logs/measure-final-wall.log` | 0 |
| RSS one-file / tiny-many / payload-heavy (separate processes) | `logs/measure-final-memory-*.log` | 0 |
| Nested H=1000 Trace2 | `logs/measure-final-nested-h1000.log` | 0 |
| `cargo fmt --all --check` | `logs/ci-fmt.log` | 0 |
| `cargo test --locked --offline --all-targets -- --test-threads=1` | `logs/ci-test.log` | 0 |
| `cargo clippy --locked --offline --all-targets -- -D warnings` | `logs/ci-clippy.log` | 0 |

Full Rust on `6603f44`: **67** `Running` target headers and **67** unfiltered `test result: ok` rows; **1737** passed / **0** failed / **0** ignored. Three nested filtered helper-subprocess rows are excluded. Command wall 784 s (`logs/ci-test.meta` 13:39:34–13:52:38 EEST). First all-targets on `39f5909` failed the init fixture (`logs/ci-test-fail-init-warmup.*`). On `6603f44`, existing test `remote_snapshot::tests::distinct_validated_incoming_roots_race_one_no_replace_cache` failed once with `UNSAFE_REMOTE_SNAPSHOT` (`logs/ci-test-fail-remote-snapshot-race.*`); the subsequent full `--all-targets` on the same HEAD passed (`logs/ci-test.log`). Cause is not established; no runtime change or test suppression was made. `src/remote_snapshot.rs`, `src/rooted_fs.rs`, and `src/host_store.rs` are unchanged since frozen before `08a6cd8`. Clippy finished 0 on the same HEAD before that successful rerun; fmt 0. No UI rebuild. GitHub Actions and the live pool were not run.

## Measured before / after

Wall: 1 excluded warmup + n=5; median = middle of 5. Times only `build_wip_base`. Count pass is a separate `CountingRunner`. Labelled counts are ProcessRunner outer requests, **not** an OS-process census. Host load and OS cache were not purged. These are fixture captures, not live worker latency.

**H** is all selected non-directory entries in one capture, including unchanged tracked files. **D** is distinct fresh byte payloads across both captures of one `build_wip_base`. **ho / ui / fi** are ProcessRunner `hash-object` / `update-index` / `fast-import` counts.

| Cell | H | D | Current `08a6cd8` median | Final `39f5909` median | Current ho/ui/fi | Final ho/ui/fi |
|---|---:|---:|---:|---:|---|---|
| empty | 0 | 0 | 227.851 ms | 242.030 ms | 0/0/0 | 0/0/0 |
| clean | 100 | 100 | 1189.295 ms | 295.401 ms | 100/2/0 | 0/2/2 |
| clean | 1000 | 1000 | 9635.761 ms | 545.458 ms | 1000/2/0 | 0/2/16 |
| duplicate | 100 | 10 | 374.914 ms | 274.472 ms | 10/2/0 | 0/2/1 |
| 16×1 MiB | 16 | 16 | 539.115 ms | 475.998 ms | 16/2/0 | 16/2/0 |
| 8 MiB fallback | 1 | 1 | 345.954 ms | 309.675 ms | 1/2/0 | 1/2/0 |
| 8×200 KiB | 8 | 8 | 407.235 ms | 284.711 ms | 8/2/0 | 0/2/2 |
| empty-blob | 3 | 1 | 375.772 ms | 257.677 ms | 1/2/0 | 1/2/0 |

H=1000 outer ProcessRunner Git: **1021 → 37**. Nested Trace2 on that cell: current 1021 unique sids / 1021 JSONL with 0 `unpack-objects`; final 53 unique sids, **37 outer + 16 `unpack-objects`** (Git 2.50.1 default `unpack_limit=100`). This is not an exhaustive system-wide process census.

Historic empty 166.892→214.283 ms remains uncontrolled; the new empty +14.179 ms is not a proven cleanup cost.

### RSS (Darwin bytes; do not sum)

`ru_maxrss` is a lifetime high-water, setup-influenced, not live heap. Parent after-build vs children after-build are not simultaneous. 1 MiB = 1048576 bytes.

| Cell | Current parent after-build | Final parent after-build | Current children after-build | Final children after-build |
|---|---:|---:|---:|---:|
| one-file 8 MiB | 48332800 | 48283648 | 14745600 | 14729216 |
| tiny-many H=1000 | 4587520 | 5013504 | 6799360 | 6782976 |
| payload-heavy 16×1 MiB | 8486912 | 10551296 | 6422528 | 6619136 |

Parent peaks moved by about −0.05 MiB / +0.41 MiB / +1.97 MiB on these three fixtures. That is a bounded extra on H=1000 and payload-heavy, not an exact isolated allocation of the new framing.

## Caveats

- Do not quote a raw sample as a median; do not include fixture setup in wall time.
- Host load during final wall started at `{ 7.96 14.60 15.89 }`. Not a quiet-host floor.
- Stage 5 as a whole is not done: remaining items are still admission/probes, no-op-write skipping, background observations, SSH multiplexing, and origin delivery.
