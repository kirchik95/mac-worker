# Phase 5 validation record

Phase 5 adds agent tasks: submit a prompt, run a headless coding agent on a Mac mini, and collect the result as a Git branch. This record is the sanitized template for the three-worker live acceptance in spec section 20.2 items 1 to 5 and 7 to 10. Item 6 (`source = origin` and `publish = push`) belongs to the later publication plan and is out of scope here.

This record covers the fresh v7 live acceptance against the resumed-turn supervisor revision. Attempts 1 to 5, including the superseded v4, v5, and v6 roots, are not evidence for this run; their tasks were closed or abandoned before the corrected v7 run. The v7 setup gate reached three ready workers with authenticated Codex 0.153.2. No profile was created, uploaded, printed, or used.

Every live row below identifies either observed sanitized evidence or what was not proven. A blocked row is not a passing substitute for the corresponding runbook item.

The record may retain only shortened identifiers, sanitized command categories, terminal states, exit results, durations, before/after fingerprints of mac-worker-owned namespaces and of the isolated clone, and the helper/client revision match. It must omit application output, complete paths, clone origin, credentials, raw environment values, source content, and raw logs.

## Automated local and fake-transport evidence

The post-rebase local gate was run against the final source. It is not a substitute for live three-worker acceptance. The ordinary sandbox initially prevented dashboard loopback binds; the escalated all-targets rerun passed.

| Command | Revision measured | Recorded result |
| --- | --- | --- |
| `cargo fmt --check` | final source | exit 0 |
| `cargo test --locked --all-targets` | final source | exit 0 after escalated loopback-enabled rerun |
| `cargo clippy --locked --all-targets -- -D warnings` | final source | exit 0 |
| `cargo build --locked --release` | final source | exit 0 |
| `git diff --check` | final source | exit 0 |

## Live three-worker acceptance

The v7 run used a fresh isolated root and a copied three-worker inventory. Claude Code is deferred by operator decision; Codex is the only agent for this run. The dashboard tasks view was observed separately; source = origin, publish = push, Cursor, OpenCode, and worker gc remain later phases.

| Required evidence | Sanitized record |
| --- | --- |
| Live-task source commit | `a88a4cd`. The v7 task executions used the isolated source clone at this revision. |
| Final post-rebase build/setup | Release build and worker setup were rerun after `main` advanced; helper/client digests matched on all three workers. |
| Worker protocol version | 4 on setup and final ready-worker refresh. |
| Helper/client revision match | Match on all three workers during v7 and again after the final post-rebase setup. The installed helper digest equaled the corresponding release client digest; installation locations are omitted. |
| Selected worker names | mini-1, mini-2, mini-3. |
| Agent capabilities | agent:codex on all three; Codex 0.153.2 authenticated; no env profiles; worker Git identity absent on all three. Claude and Cursor remain deferred. |
| Sanitized command categories | Isolated setup; five-task batch; needs_input then say; cancel then say; pinned follow-up; --wip identity; disconnect/reconnect and killed runner; dashboard observe-and-shutdown; retained-metadata scan; Git-identity task. |
| 1. Five-task run, default max_parallel, submitting shell exits | Run `040843ed`. Positions 1–3 were active on distinct workers and positions 4–5 queued. All five ended `closed/done`; new-shell wait exited 0. The kit’s inherited origin first caused `INVALID_ORIGIN` (exit 64), with no task created; removing that origin and retrying produced the recorded run. |
| 2. needs_input then say | Task `d6e98229` on mini-2: the first turn ended `needs_input` with a session present; the prescribed resume hit the stale commit instruction’s read-only `.git` gate, then a corrective same-session follow-up completed `done`. Session continuity was present across all three turns. |
| 3. cancel during a turn, then say | Task `73cf7888` on mini-1: the first turn was cancelled, the resumed turn ran the required test but hit the read-only `.git` gate, and a corrective same-session follow-up completed `done`. Cancel-then-resume completion was proven. |
| 4. Follow-up pinned to a busy worker | Busy holder `fa39e6f5` stayed on mini-3; its pinned follow-up did not migrate. Independent idle-first task `c65fedde` was admitted on mini-1 and completed. Live task projection exposed no blocking-code field. |
| 5. --wip base | Task `bc81e32a` preserved the source clone’s HEAD, index, intentional dirty state, and diff-check result. After the marker-only untracked input was excluded from preflight, the turn ran; the read-only task sandbox blocked the agent’s attempted commit, but the publisher produced temporary result head `d3f58fb4`, which fetched successfully. `--publish push` was refused at preflight with `TASK_CONFIG_INVALID` (exit 64), with no task or remote mutation. |
| 6. `source = origin` and `publish = push` | Out of scope for this record. Later publication plan. |
| 7. Disconnect and killed runner | Two `logs -f` followers disconnected and reattached to the same turn. A `submit --wait` disconnect and a separately validated killed runner each reconciled with one runner replacement; status/logs reconnected by the original task/turn. No second turn was accepted. Probe tasks were terminalized during cleanup. |
| 8. Dashboard observe and shutdown | While item 1 was active, the loopback dashboard showed total 5, queued 2, active 3, and task/run counts matching the CLI; its active-job projection was 0. Selected detail and bounded stdout matched the active CLI state. Final snapshot showed all five closed and no active/queued records. |
| 9. Retained metadata privacy | Fixed-script count-only scans found marker `0` and secret `0` everywhere; local records `0`; worker records `0/1/0` for mini-1/2/3. The single mini-2 record hit was a `tasks/<project>/<task>/status.json`; count-only correlation showed its task was created before 2026-09-04 22:00 local, so it predates the redaction fix (an attempt-4/5 task). Application-log counts are transparency-only: local `16`, mini-1 `66`, mini-2 `80`, mini-3 `52`. |
| 10. Locked-keychain adapters and Git identity | Task `bd6ee54d` on mini-3 completed and fetched result head `c5362a52`; author and committer fields were present, but the worker had no configured Git identity, so attribution was not proven. |
| Worker namespace fingerprints | Final ownership fingerprints are below. Helper digest matched the client on all three; data namespaces changed as task metadata accumulated. |
| Unrelated remote entries | Immediate sibling counts and fingerprints were unchanged for each worker. |
| Original isolated clone | Baseline HEAD `ad15b09`, intentional `README.md` modification plus untracked scratch state, and clean diff check remained unchanged after submit and remote execution. |

### Namespace fingerprints

Local isolated namespaces:

| Namespace | Before | After |
| --- | --- | --- |
| config | 1 / `4e39df499c7ffec1` | 1 / `4e39df499c7ffec1` |
| state | 0 / `e3b0c44298fc1c14` | 131 / `a0cb56454c9cbe75` |
| cache | 0 / `e3b0c44298fc1c14` | 193 / `060015f422768acb` |
| data | 0 / `e3b0c44298fc1c14` | 0 / `e3b0c44298fc1c14` |

Worker-owned namespaces and immediate sibling sets:

| Worker | Data before → after | Helper before → after | Data siblings before → after | Helper siblings before → after |
| --- | --- | --- | --- | --- |
| mini-1 | 1784 / `0dafcd5465ce7ac5` → 2294 / `acd0675177795e1c` | 1 / `2289bb5e6c55af5c` → 1 / `2289bb5e6c55af5c` | 8 / `be0eadec22acfb36` → 8 / `be0eadec22acfb36` | 11 / `1505472269bf4716` → 11 / `1505472269bf4716` |
| mini-2 | 800 / `6c53fb6b223cd2fb` → 1290 / `27024ae47f578a00` | 1 / `2289bb5e6c55af5c` → 1 / `2289bb5e6c55af5c` | 8 / `0bb9bc0127fafb03` → 8 / `0bb9bc0127fafb03` | 18 / `5b3d5df63db065e3` → 18 / `5b3d5df63db065e3` |
| mini-3 | 753 / `4ea1a6993456a90f` → 1167 / `8aaa686f70e81612` | 1 / `2289bb5e6c55af5c` → 1 / `2289bb5e6c55af5c` | 7 / `f07984e28cc77ee3` → 7 / `f07984e28cc77ee3` | 10 / `b4920d4ad5d0e033` → 10 / `b4920d4ad5d0e033` |

The live record states that the validated helper and client revisions matched, without recording installation locations or connection details. It records only sanitized command categories and outcomes; application output is not safe evidence by default because it may contain application-emitted secrets.

## Delivery boundary

**A completed live record does not authorize later phases.** `source = origin`, successful `publish = push`, `--publish-branch`, Cursor Agent, OpenCode, and retention through `worker gc` remain later plans. In this execution core those options must continue to reject at preflight rather than run.

Claude Code remains deferred on the workers by operator decision even after this acceptance, unless the operator records a later decision here.
