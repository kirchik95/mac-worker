# Phase 5 three-Mac acceptance runbook

This is the operator (or agent) procedure for spec section 20.2 items 1 to 5 and 7 to 10. Do not run it from the docs-only `docs/phase5-acceptance` worktree against live workers. Wait until Tasks 7, 8, and 9 are in the software under test, then execute from an isolated clone of that revision.

Fill [docs/phase-five-validation.md](phase-five-validation.md) with sanitized evidence only. Item 6 is the later publication plan; skip it.

On the current `main`-based docs branch, `src/cli.rs` has no `worker task` family and no `worker workers --refresh`. The command shapes below are the spec section 7.1 grammar the landed binary must accept. If a flag is still missing when you run, stop and record the gap; do not invent a substitute against a worker.

## What this run proves

Against the three configured workers, with Codex only:

1. Five tasks in one run at the default `max_parallel` (equal to the worker count): three `active` on three distinct workers, two waiting with a visible blocking reason, and `wait --run` from a **new** shell completing all five.
2. `needs_input` becomes `open`; `say` resumes the same session on the same worker.
3. `cancel` during a turn, then `say` resumes the session and completes.
4. A follow-up pinned to a busy worker waits and never moves, and does not delay a first turn on an idle worker.
5. `--wip`: the user repository is byte-identical after submit; the result branch contains the temporary commit; `push` is refused at preflight.
7. Disconnect during `logs -f` and during `submit --wait`, plus a killed runner mid-turn: one turn, runner replaced, status/logs reconnect by the original IDs.
8. Dashboard matches those states; shutting it down changes nothing.
9. Retained metadata has no planted secrets and no complete local paths.
10. A worker without a Git identity produces correctly attributed commits. Claude and Cursor from a locked keychain are **not** part of this run (see Env profiles).

## Shells that must exit before completion

These items are invalid if the submitting command stays attached until the turn ends:

| Item | Why the submitting shell must exit |
| --- | --- |
| 1 | Spec: submit the five-task run from a shell that exits immediately. `batch` / `submit` **without** `--wait` returns once the run, task records, base commits, and runners exist. Exit that shell. Run `worker task wait --run <id>` from a **new** shell. |
| 7 | Disconnect `worker task logs -f` and `worker task submit --wait` before the turn ends, then reconnect. Also kill the local runner mid-turn. The next mutating `worker task` command must replace the runner. |

Items 2, 3, 4, 5, 8, 9, and 10 may use `--wait` or `wait` after submit. Prefer submit without `--wait` plus a later `wait` so a dropped follower cannot be mistaken for the item 7 proof.

## Sanitization rules

Record in the validation file only:

- shortened identifiers (first eight hex characters of task, run, turn, and job IDs)
- worker aliases (`mini-1`, not SSH destinations)
- sanitized command categories (the names in the table above, not prompt text)
- terminal states, last outcomes, blocking reasons, public error codes
- process exit codes and approximate durations
- before/after fingerprints and entry counts of mac-worker-owned namespaces
- helper/client SHA-256 equality (match / mismatch)
- protocol version and the software commit

Never record: complete local or remote paths, clone origin, SSH host names or keys, credentials, env-profile values, planted-secret values, prompt text, transcripts, raw logs, or application output. Inspect those locally, then write hit counts and categories only.

## 0. Isolated roots

Create one acceptance root. All local mac-worker state for this run lives under it. Do not use the operator's daily `~/.config/mac-worker` or default XDG roots.

```bash
ACCEPT_ROOT=$(mktemp -d)
export XDG_CONFIG_HOME="$ACCEPT_ROOT/xdg/config"
export XDG_STATE_HOME="$ACCEPT_ROOT/xdg/state"
export XDG_CACHE_HOME="$ACCEPT_ROOT/xdg/cache"
export XDG_DATA_HOME="$ACCEPT_ROOT/xdg/data"
mkdir -p \
  "$XDG_CONFIG_HOME/mac-worker" \
  "$XDG_STATE_HOME" \
  "$XDG_CACHE_HOME" \
  "$XDG_DATA_HOME"
```

Copy the three-worker inventory into the isolated config. Keep the same `name`, `ssh`, and `slots = 1` entries as the operator inventory. Do not add `origin:` capabilities; this run is `source = local` only. Point every later command at that file:

```bash
INVENTORY="$XDG_CONFIG_HOME/mac-worker/config.toml"
# write version = 1 and the three [[workers]] tables, then:
<release-worker> --config "$INVENTORY" …
```

Clone the software under test into `$ACCEPT_ROOT/src` and a separate throwaway project clone into `$ACCEPT_ROOT/clone`. Work only in those clones. Fingerprint the clone before any submit (`HEAD`, `git status --porcelain`, `git diff --check`) and keep a one-line marker-only dirty state if item 5 needs `--wip`.

Build the release binary from `$ACCEPT_ROOT/src` at the commit you will record:

```bash
cargo build --locked --release
WORKER="$ACCEPT_ROOT/src/target/release/worker"
```

Set the submitting account's `user.name` and `user.email` in the isolated clone (not secret). Leave at least one worker without `user.name` / `user.email` for item 10.

## 1. Env profiles

**Codex only for this run.** Codex uses its file-based login on each worker. Do not create, upload, or print a profile. Do not pass `--env-profile`.

Claude Code is deferred by operator decision. Do not submit `--agent claude`, do not require `agent:claude@agents`, and do not place `CLAUDE_CODE_OAUTH_TOKEN` or `ANTHROPIC_API_KEY` for this acceptance.

Cursor and OpenCode are later-plan adapters. Do not submit `--agent cursor` or `--agent opencode`.

If a worker already has `~/.config/mac-worker/env/*.env` from earlier provisioning, leave those files untouched and unused. mac-worker must never print them.

## 2. Setup and inventory

Install the release helper on all three workers. `worker setup` is the layout-migration and first agent-facts collection path. Rerun it on every worker even if a helper is already present.

```bash
"$WORKER" --config "$INVENTORY" setup mini-1 mini-2 mini-3
"$WORKER" --config "$INVENTORY" workers
"$WORKER" --config "$INVENTORY" --json workers
```

After Task 9, also recollect facts:

```bash
"$WORKER" --config "$INVENTORY" workers --refresh
```

Pass only when:

- each worker is ready
- protocol version is `4`
- every worker reports `agent:codex`
- the installed helper SHA-256 equals `$WORKER` on each host

Record the software commit, protocol version, helper/client match, and the three aliases. Take before fingerprints of each worker's mac-worker-owned data and helper namespaces, and of the non-owned sibling sets, using the same listing method as the phase 3 record. Do not store complete paths.

## 3. Item 1 — five-task run, submitting shell exits

Write five independent long-enough Codex briefs (each must stay `active` long enough to observe the queue) and a batch file. Default `max_parallel` equals the number of configured workers (three). Do not pass `--max-parallel`.

```toml
version = 1
agent = "codex"
base = "HEAD"
source = "local"
publish = ["fetch"]
timeout = "45m"

[[tasks]]
title = "Accept 1a"
prompt_file = "tasks/accept-1a.md"

[[tasks]]
title = "Accept 1b"
prompt_file = "tasks/accept-1b.md"

[[tasks]]
title = "Accept 1c"
prompt_file = "tasks/accept-1c.md"

[[tasks]]
title = "Accept 1d"
prompt_file = "tasks/accept-1d.md"

[[tasks]]
title = "Accept 1e"
prompt_file = "tasks/accept-1e.md"
```

From a throwaway shell that will exit immediately:

```bash
"$WORKER" --config "$INVENTORY" --json task batch tasks/accept-five.toml
# keep run_id and all five task_id values, then exit this shell
```

From a **new** shell, with the same isolated XDG and `--config`:

```bash
"$WORKER" --config "$INVENTORY" --json task list --run <run_id>
# expect three active on three distinct workers; two waiting with a blocking reason
"$WORKER" --config "$INVENTORY" task wait --run <run_id>
"$WORKER" --config "$INVENTORY" --json task list --run <run_id>
```

Evidence: shortened run/task IDs, three worker aliases, blocking reason, `wait` exit, duration, terminal states.

## 4. Item 2 — `needs_input` then `say`

The brief must order the agent to finish the first turn with structured status `needs_input` and one concrete question, without editing beyond a marker.

```bash
"$WORKER" --config "$INVENTORY" --json task submit --agent codex --prompt-file tasks/accept-needs-input.md
"$WORKER" --config "$INVENTORY" task wait <task_id>
"$WORKER" --config "$INVENTORY" --json task result <task_id>
"$WORKER" --config "$INVENTORY" --json task status <task_id>
"$WORKER" --config "$INVENTORY" --json task say <task_id> --message-file tasks/accept-needs-input-answer.md --wait
"$WORKER" --config "$INVENTORY" task logs <task_id>
```

Evidence: task became `open` with last outcome `needs_input`; `say` stayed on the same worker; session continuity yes/no; exits and durations. Do not keep transcript text.

## 5. Item 3 — `cancel` then `say`

Submit a turn that will still be running when you cancel (a long approved command in the brief).

```bash
"$WORKER" --config "$INVENTORY" --json task submit --agent codex --prompt-file tasks/accept-cancel.md
# while the turn is active:
"$WORKER" --config "$INVENTORY" --json task cancel <task_id>
"$WORKER" --config "$INVENTORY" --json task status <task_id>
"$WORKER" --config "$INVENTORY" --json task say <task_id> --message-file tasks/accept-cancel-resume.md --wait
```

Evidence: `open` after cancel; `say` completed; same worker / session present; exits and durations.

## 6. Item 4 — pinned follow-up does not steal an idle worker

1. Finish a short task so it is `open` on worker A, or reuse an `open` task from item 2 or 3.
2. Start a long first-turn task that will hold worker A's slot (pin is not required; the pool may land it on A, or occupy A with a separate holder). Confirm A is busy.
3. `say` the `open` task (hard-pinned to A). It must wait with a visible blocking reason and must not appear `active` on B or C.
4. Submit a new first-turn task. It must admit to an idle worker without waiting on the pinned follow-up.

```bash
"$WORKER" --config "$INVENTORY" --json task say <open_task_id> --message-file tasks/accept-pinned-followup.md
"$WORKER" --config "$INVENTORY" --json task submit --agent codex --prompt-file tasks/accept-idle-first-turn.md
"$WORKER" --config "$INVENTORY" --json task list --run <run_id>
"$WORKER" --config "$INVENTORY" --json task status <open_task_id>
"$WORKER" --config "$INVENTORY" --json task status <first_turn_task_id>
```

Evidence: pin, blocking reason, worker aliases, and that the first turn did not wait behind the follow-up.

## 7. Item 5 — `--wip` identity and push refused

Create a tracked edit and, if needed, an explicit `--include` for one untracked file in `$ACCEPT_ROOT/clone`. Capture byte identity of `HEAD`, index, working tree, refs, reflogs, configuration, and hooks.

```bash
"$WORKER" --config "$INVENTORY" --json task submit --agent codex --wip \
  --include '<exact-relative-untracked>' \
  --prompt-file tasks/accept-wip.md
```

Re-check the clone identity immediately after submit returns. It must be unchanged. After the turn:

```bash
"$WORKER" --config "$INVENTORY" task fetch <task_id>
"$WORKER" --config "$INVENTORY" --json task result <task_id>
```

The fetched result branch must contain the temporary WIP commit. Then refuse push at preflight (do not expect a remote push):

```bash
"$WORKER" --config "$INVENTORY" --json task submit --agent codex --wip \
  --publish push --prompt-file tasks/accept-wip.md
```

Expect a preflight error, no task record, no remote mutation. Record the public code (`PUBLISH_REQUIRES_COMMITTED_BASE` and/or `TASK_CONFIG_INVALID` naming the later plan — see the operator questions in the docs commit report).

## 8. Item 7 — disconnect, `submit --wait`, killed runner

Use one long task.

```bash
"$WORKER" --config "$INVENTORY" --json task submit --agent codex --prompt-file tasks/accept-disconnect.md
"$WORKER" --config "$INVENTORY" task logs -f <task_id>
# disconnect the follower (Ctrl-C or kill the client). Do not cancel the task.
"$WORKER" --config "$INVENTORY" task logs <task_id>
"$WORKER" --config "$INVENTORY" task logs -f <task_id>
```

In a separate trial:

```bash
"$WORKER" --config "$INVENTORY" --json task submit --agent codex --wait --prompt-file tasks/accept-disconnect.md
# kill that submitting process before the turn ends
"$WORKER" --config "$INVENTORY" --json task status <task_id>
```

Kill the local turn runner mid-turn (the mac-worker-owned runner process, not the remote agent). Then run any mutating command:

```bash
"$WORKER" --config "$INVENTORY" task reconcile
# or: task say / task close / task wait / task cancel
"$WORKER" --config "$INVENTORY" --json task status <task_id>
"$WORKER" --config "$INVENTORY" task logs <task_id>
```

Evidence: exactly one turn accepted; runner replaced; status and logs reconnect by the original IDs; no second acceptance.

## 9. Item 8 — dashboard

```bash
"$WORKER" --config "$INVENTORY" dashboard --no-open
```

While items 1 to 7 have known states (or a short replay of active / waiting / open), confirm the loopback page matches `task list` / `task status` for those states. Then shut the dashboard down and repeat `task status` / `task list`. Nothing may change because of the dashboard.

The tasks-and-runs view is a later phase. Record the projection the landed binary actually shows (worker cards with agent/title/elapsed are in spec section 19; a dedicated tasks view is phase 5e). Do not start a non-loopback listener.

## 10. Item 9 — planted secrets and paths

Before remaining submits, plant a unique marker in a prompt file and, separately, a conventional-looking secret-shaped string that must not be copied into mac-worker records. After the run, inspect only mac-worker-owned local state under the isolated XDG roots and mac-worker-owned remote namespaces.

Evidence: zero planted-marker hits and zero complete-local-path hits in retained metadata, queue rows, JSON output, and dashboard responses. Do not write the marker or any path into the validation file.

## 11. Item 10 — Git identity (Codex); Claude/Cursor deferred

Submit a Codex task that creates a commit, pinned only if needed so it lands on the worker that has no `user.name` / `user.email`:

```bash
"$WORKER" --config "$INVENTORY" --json task submit --agent codex --prompt-file tasks/accept-identity.md
"$WORKER" --config "$INVENTORY" task wait <task_id>
"$WORKER" --config "$INVENTORY" task fetch <task_id>
```

Confirm the result commit uses the submitting clone's recorded identity (or the fixed `mac-worker` fallback), never the worker hostname. Do not run Claude or Cursor turns for this item unless the operator lifts the deferral in writing.

## 12. Close and fingerprints

Close finished tasks. Discard only work that must not leave a result branch.

```bash
"$WORKER" --config "$INVENTORY" task close <task_id>
"$WORKER" --config "$INVENTORY" task close <task_id> --discard
```

Re-take worker namespace fingerprints and the isolated-clone identity. Compare to the before values. Expected changes stay inside mac-worker-owned namespaces. The clone's baseline commit and marker-only state must match the opening fingerprint except for the remote-tracking refs `fetch` is allowed to write.

## 13. Write the record

Replace every `PENDING` live row in `docs/phase-five-validation.md`. Leave item 6 marked out of scope. Commit the sanitized record on the validation branch; do not commit isolated clones, XDG trees, prompt files that contain planted markers, or env files.
