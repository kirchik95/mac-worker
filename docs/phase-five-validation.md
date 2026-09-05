# Phase 5 validation record

Phase 5 adds agent tasks: submit a prompt, run a headless coding agent on a Mac mini, and collect the result as a Git branch. This record is the sanitized template for the three-worker live acceptance in spec section 20.2 items 1 to 5 and 7 to 10. Item 6 (`source = origin` and `publish = push`), the Cursor and OpenCode adapter rows, and retention through `worker gc` are recorded in the Phase 5d section below; this worktree did not run them.

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

The v7 run used a fresh isolated root and a copied three-worker inventory. Claude Code is deferred by operator decision; Codex is the only agent for this run. The dashboard tasks view was observed separately. Source origin, push publication, Cursor, OpenCode, and worker gc are Phase 5d rows below and remain `PENDING LIVE RUN`.

| Required evidence | Sanitized record |
| --- | --- |
| Live-task source commit | `a88a4cd`. The v7 task executions used the isolated source clone at this revision. |
| Final post-rebase build/setup | Release build and worker setup were rerun after `main` advanced; helper/client digests matched on all three workers. |
| Worker protocol version | 4 on setup and final ready-worker refresh. |
| Helper/client revision match | Match on all three workers during v7 and again after the final post-rebase setup. The installed helper digest equaled the corresponding release client digest; installation locations are omitted. |
| Selected worker names | mini-1, mini-2, mini-3. |
| Agent capabilities | agent:codex on all three; Codex 0.153.2 authenticated; no env profiles; worker Git identity absent on all three. Claude remains deferred. Cursor and OpenCode are Phase 5d rows below. |
| Sanitized command categories | Isolated setup; five-task batch; needs_input then say; cancel then say; pinned follow-up; --wip identity; disconnect/reconnect and killed runner; dashboard observe-and-shutdown; retained-metadata scan; Git-identity task. |
| 1. Five-task run, default max_parallel, submitting shell exits | Run `040843ed`. Positions 1–3 were active on distinct workers and positions 4–5 queued. All five ended `closed/done`; new-shell wait exited 0. The kit’s inherited origin first caused `INVALID_ORIGIN` (exit 64), with no task created; removing that origin and retrying produced the recorded run. |
| 2. needs_input then say | Task `d6e98229` on mini-2: the first turn ended `needs_input` with a session present; the prescribed resume hit the stale commit instruction’s read-only `.git` gate, then a corrective same-session follow-up completed `done`. Session continuity was present across all three turns. |
| 3. cancel during a turn, then say | Task `73cf7888` on mini-1: the first turn was cancelled, the resumed turn ran the required test but hit the read-only `.git` gate, and a corrective same-session follow-up completed `done`. Cancel-then-resume completion was proven. |
| 4. Follow-up pinned to a busy worker | Busy holder `fa39e6f5` stayed on mini-3; its pinned follow-up did not migrate. Independent idle-first task `c65fedde` was admitted on mini-1 and completed. Live task projection exposed no blocking-code field. |
| 5. --wip base | Task `bc81e32a` preserved the source clone’s HEAD, index, intentional dirty state, and diff-check result. After the marker-only untracked input was excluded from preflight, the turn ran; the read-only task sandbox blocked the agent’s attempted commit, but the publisher produced temporary result head `d3f58fb4`, which fetched successfully. `--publish push` was refused at preflight with `TASK_CONFIG_INVALID` (exit 64), with no task or remote mutation. |
| 6. `source = origin` and `publish = push` | PENDING LIVE RUN |
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

## Phase 5d

This section is the sanitized skeleton for spec section 20.2 item 6 plus the Cursor, OpenCode, and retention rows. Every evidence cell is `PENDING LIVE RUN` until an operator-authorized live gate. This worktree did not contact a worker.

Required evidence to collect on that run, without recording secrets or complete local paths:

- Source origin and push publication: exact base preflight; `origin:<host>` routing; branch appears as `--publish-branch`; exit/state
- Cursor env-profile turn: `prebind_session` before first turn; profile-keyed capability; same chat on say; exit/state
- OpenCode env-profile turn: JSON session binding; `--auto` and pointer prompt; same session on say; exit/state
- Retention and discard: `worker gc` preview/apply; branch/mirror/transfer candidates; native delete if available; exit/state

| Acceptance item | Exact-object / session | Capability / policy | Publication / continuity | Exit / state |
| --- | --- | --- | --- | --- |
| Source origin and push publication | PENDING LIVE RUN | PENDING LIVE RUN | PENDING LIVE RUN | PENDING LIVE RUN |
| Cursor env-profile turn | PENDING LIVE RUN | PENDING LIVE RUN | PENDING LIVE RUN | PENDING LIVE RUN |
| OpenCode env-profile turn | PENDING LIVE RUN | PENDING LIVE RUN | PENDING LIVE RUN | PENDING LIVE RUN |
| Retention and discard | PENDING LIVE RUN | PENDING LIVE RUN | PENDING LIVE RUN | PENDING LIVE RUN |

### Operator checklist

1. Use an isolated clone and isolated XDG roots. Install the release helper only after core Task 10. Do not record hostnames, complete paths, tokens, session identifiers, or transcript text.
2. Run spec section 20.2 item 6: submit with `source = origin` and `publish = push`; confirm only origin-capable workers are selected; confirm the pushed branch name matches `--publish-branch`; pin a worker that lacks `origin:<host>` and confirm `CAPABILITY_MISSING` without reroute.
3. Run a Cursor env-profile turn from a locked-keychain SSH session through `CURSOR_API_KEY`. Confirm `prebind_session` before the first turn, the profile-keyed capability, and the same chat on `say`.
4. Run an OpenCode env-profile turn through its secure profile. Confirm JSON session binding, `--auto` and the pointer prompt, and the same session on `say`.
5. Confirm a push failure leaves the task open with `PUBLISH_FAILED`. Preview then apply `worker gc`; confirm protected branches remain and only expired candidates are removed. Scan retained records for planted profile values and complete local paths; record hit counts only.
6. Record shortened IDs, aliases, states, exit codes, durations, helper/client revision match, and before/after fingerprints only.

## Delivery boundary

The v7 Codex record above does not complete Phase 5d. Item 6, Cursor, OpenCode, and `worker gc` stay `PENDING LIVE RUN` until the operator-authorized live gate. Claude Code remains deferred on the workers by operator decision unless the operator records a later decision here.
