# Session Transfer (Phase A) — Implementation Record

> **Local sources:** briefs and reports cited below live in `.briefs/`, an excluded working directory; they are not published repository pages.

Date: 2026-10-03.

Branch: `integ/session-transfer`, based on `main` `3a1a097`. **Not pushed, not deployed.**

Spec: [2026-10-03-session-transfer-design.md](../specs/2026-10-03-session-transfer-design.md); its Round 2 section is binding.

Plan: [2026-10-03-session-transfer.md](../plans/2026-10-03-session-transfer.md).

Spike: [2026-10-03-session-transfer-spike.md](2026-10-03-session-transfer-spike.md).

## Identity
- **Feature:** `worker task submit --from-session claude[:<uuid>] | codex[:<uuid>]` continues a copy of a laptop agent session in the pool. It works in direct mode and through the controller.
- **Size:** 73 commits, 132 files, about +18,000 / −260 lines against `3a1a097`.
- **Executors:** pi agents in herdr panes, at most 10 at once, on the user's default pi configuration. The orchestrator wrote the spec, plan, briefs and contract amendments A1/A2/FA, reviewed every track and merged it.

## Tracks and merges

| Merge | Track | Content |
| --- | --- | --- |
| `31bbaad` | F1 | Claude adapter passes `--verbose` (pool bug: stream-json print mode refused without it). Branch `fix/claude-stream-verbose` from `main` |
| `369096b` | T1 | Contracts, tokens, Claude dir encoding, store root, additive wire fields, error catalog, signature freezes, test seeds; amendments A1, A2 |
| `143e83e` | S1 | Structure-aware scrubber |
| `c741dd2` | S2 | No-follow atomic `StoreWriter` |
| `0fdb2fe`, `0cbea27` | W4, W3 | Codex and Claude placement |
| `72bc87d` | W10 | Usage docs, handoff recipe, pool-dispatch skill |
| `295bfaf`, `d65ae93` | W2, W1 | Codex and Claude capture |
| `7c81d1c` | W9 | `feature:` capabilities, `agent-min:` gate (at most one minor behind) |
| `25effa9` | W6 | Package commit and pins, atomic two-ref pushes, host hook |
| `4db2d89` | W8 | `TurnStart::Imported`, post-prepare binding check, host first-turn validation |
| `9c3ad70` | W7 | Controller source stream carries and verifies the session package |
| `b5ec1fc` | W5 | Durable prepare import (planned/complete receipt), profile-aware store root, session-ref GC |
| `e4fcb6b`, `30e0c93`, `7c4e91a` | FA, FB1, FB2 | Code-review fixes (below) |
| `5e74772` | FC | Full-suite regressions: create-or-same source pinning restored; verbose fragment fixture moved |
| `eb007f6` | T7 | CLI, direct and controller submit, version-aware queue claims, paired runner pin retirement, features advertised |
| `cebbd0c`, `e447143` | FD, FE | Final-review fixes (below) |

## Reviews and their fixes
- **Spec reviews R1/R2** (before code): 19 findings, all folded into spec Round 2. Among them: prepare ordering, the import receipt, no session lookup before prepare, the version gate interface, the pin lifecycle, validating deserialization, JSON-safe tokens, feature strings without digits.
- **R3, laptop code:** the token boundary after JSON escapes and around non-ASCII; sidecar symlink substitution; Claude "latest" picking the wrong project. Fixed by **FA**.
- **R4, host code:**
  - `StoreWriter` parent-chain binding, inode-checked publication, durable `Unchanged` → fixed by **FB1**;
  - agent versions lost through the admission cache → fixed by **FB2** (compatible envelope inside `inventory_capabilities`);
  - the pinned rejection code → fixed in **T7**.
- **T7 contract issues:** literal capability recheck at queue claim; base-only runner cleanups. Both fixed in T7 under orchestrator-expanded ownership.
- **R6, security:**
  - version metadata could carry a scrubbed token or a path → fixed by **FD** (strict grammar plus a scrubber check, also enforced by `SessionImportMeta`);
  - main-transcript ancestor symlink substitution → fixed by **FD** (rooted reads).
- **R5, end to end:**
  - follow-up and recovered rows dropped the import gates;
  - controller-mode laptop pins were never retired;
  - an orphan controller task pin on pre-record failure.

  All fixed by **FE**.

## Verification
- **Full suite on `eb007f6`:** 3897 tests, 3894 passed. The 3 failures were stale feature-list expectations, updated in `d5c63cb`. Clippy is clean.
- **Full suite on the final head:** see "Final gate" below.
- **Live:** none yet. The T0 spike proved real Claude Code 2.1.288 and Codex 0.160.0 → 0.159.3 resumes of placed sessions. T8 (pool acceptance) has not run.

## Final gate
- **`e447143`** (all tracks and fixes merged): 3927 tests, 3908 passed, 18 failed, 1 timeout. All 19 came from one integration conflict: FD tightened the `SessionImportMeta` version grammar, but host prepare (W5) reused `SessionImportMeta::new(agent, oid, "1")` only to validate object ids, in the package reader and in the receipt check. Fixed in `2c249ef` with an explicit object-id check.
- **`2c249ef`:**
  - `cargo fmt --all --check` clean;
  - `cargo clippy --locked --all-targets -- -D warnings` clean;
  - `cargo nextest run --locked`: **3927 tests, 3927 passed**, 22 skipped, 685 s.

## Open
- **T8 live acceptance:** requires deploying the integration build to the laptop, controller and minis. Coordinate with other pool work; no open tasks may be running.
- **Claude transcripts** of imported tasks stay in the worker's `~/.claude/projects`, which is pre-existing behaviour for Claude pool tasks.
- **Deferred:** controller request-cache GC; legacy laptop pins created before retirement markers.
- **Phase B** (pull a pool session back to the laptop) is next.
