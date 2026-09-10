---
name: pool-dispatch
description: "Dispatch independent coding tasks through the mac-worker pool and collect their results. Use on an explicit /pool-dispatch call, or whenever the user asks to run work in the pool or on the Mac minis: «отправь выполняться в пул», «отправь в пул», «запусти в пуле», «запусти на mini», «через worker task», «send it to the pool», «run it on the minis», «dispatch to the workers»."
---

# Pool Dispatch

## When To Use

- The user invokes `/pool-dispatch` explicitly.
- The user asks to run work in the pool or on the minis, in any wording: «отправь выполняться в пул», «отправь в пул», «запусти в пуле», «запусти на mini», «через worker task», "send it to the pool", "run it on the minis", "dispatch to the workers".
- The user asks to check, follow, answer, or fetch a pool task: «что с задачей в пуле», «забери результат», «ответь агенту», "task status", "fetch the result".

Before the first submit, write the brief with `pool-task-authoring` unless the user already supplied a prompt file. Do not send work to the pool on your own initiative without one of the triggers above; when work merely looks parallelisable, say so and ask.

This skill is the mechanical task loop. One task is one independent unit of work. The pool chooses workers. Never choose a worker yourself.

The CLI is the only interface. The skill contains no scheduling logic.

**Command availability:** the release exposes `worker task …`, `worker dashboard`, `worker controller run`, and `worker workers --refresh`. Run `worker skills get pool-dispatch --grammar-only` (or `worker task <cmd> --help`) and use that output as the only grammar; do not replace a rejected public form with direct worker access, SSH, or another tool.

Default dispatch is the laptop queue (`[controller]` missing or `enabled = false`). If the operator already enabled a remote controller, keep this same `worker task …` grammar — do not invent a second CLI, and do not start `worker controller run` from this skill. A controller-only laptop config may omit `[[workers]]`; commands that need a local worker list then fail with `at least one worker is required` (for example `worker workers`, `setup`, `doctor`, `run`, `gc`, and streaming `worker logs`). Public job `status` and `cancel` stay laptop-local and do not use that inventory error. Operator notes: repository `docs/usage.md` section Remote controller.

## Grammar Source

Do not copy CLI flags from this file. Run `worker skills get pool-dispatch --grammar-only` (or `worker task <cmd> --help`) and use that output as the only grammar.

Default laptop queue: without `--wait`, `submit`, `batch`, and `say` return as soon as the task record, the base commit in the transfer repository, and the queue row exist and a local turn runner has taken ownership of the row, or the row is parked behind the per-worker runner cap. With `--wait` the same work happens in the foreground and the command follows the turn's event log to its end.

Enabled remote controller: without `--wait`, those commands return on a durable `host controller-rpc` ACK. That ACK means the request is persisted on the controller store. It does not mean a runner has started, or that the agent is running — enabled submit can stay queued (`park_only`) until the leader starts a runner. Keep `worker controller run` for autonomous progress. With `--wait` the CLI waits until the selected task or run is quiescent. Logs remain a separate command (`worker task logs`).

`list`, `status`, `result`, `diff`, and `logs` are read-only; `submit`, `batch`, `say`, `cancel`, `close`, `wait`, and `reconcile` perform runner recovery first.

`--json` on any command emits the same typed records the dashboard consumes; `submit`, `say`, `logs -f`, and `wait` emit versioned NDJSON events, with log chunks base64-encoded as in v1.

The current release accepts `source = local|origin` and `publish = fetch|push`; `--publish-branch` is valid only with `push`, and a `--wip` base cannot push (`PUBLISH_REQUIRES_COMMITTED_BASE`). Agents on this pool:

- `codex`: Codex with the configured defaults; pass `--model`/`--effort` only when the task needs a different one; see `worker skills get pool-dispatch` for the effective values. Only Codex reads `--effort` — the other agents ignore it.
- `opencode`: no model flag uses the worker's default (OpenCode Zen, Muse Spark 1.3, free); OpenCode Go models are `--model opencode-go/<model>`.
- `cursor`: always `--env-profile agents`; that worker-side profile carries the Cursor login and the login-keychain unlock. Never read or copy it.
- `claude`: deferred on the workers by operator decision; do not submit it until the operator enables it.

Watch the pool while tasks run: `worker dashboard --port 8765 --no-open`, then open `http://127.0.0.1:8765`.

## Submit

For each task, put its prompt in a Markdown file and submit it:

```text
worker task submit --agent <name> --prompt-file <file> --json
```

Keep every returned `task_id`. For a list of tasks, use the batch command with its batch file:

```text
worker task batch <file> --json
```

Keep the returned run identifier and all task identifiers.

## Follow

Poll a run with:

```text
worker task list --run <id> --json
```

Or block until the run completes:

```text
worker task wait --run <id>
```

For one task, use the release form:

```text
worker task wait --task-id <id>
```

Inspect one task without mutating it:

```text
worker task status <id>
worker task diff <id> --stat
```

List exactly the tasks waiting on an answer:

```text
worker task list --state open --outcome needs-input --json
```

For a task that reports `needs_input`, read its questions from `worker task status <id> --json`: each question is `{"text": …, "options": [...]}`, and a question with no options is a bare string. When a question offers options, answer with one of them verbatim. Write the answer to a message file and send it as a new turn:

```text
worker task say <id> --message-file <file> --wait
```

Do not send a message to an active turn. `say` is for the next turn. A `say` while a turn is running is `TASK_BUSY`.

When a task reports `done`, fetch its result:

```text
worker task fetch <id>
```

Report the remote-tracking ref returned by `fetch`.

When a task reports `blocked`, or its turn fails, inspect both the structured result and the logs:

```text
worker task result <id> --json
worker task logs <id>
```

Then either write guidance and use `say`, or discard the task:

```text
worker task close <id> --discard
```

Close every finished task:

```text
worker task close <id>
```

`worker task reconcile` re-owns dead runners and re-enqueues orphaned tasks. It does not submit work.

## Liveness And Settlement

Absence of evidence is not evidence of death. A `wait` timeout, an empty `list` poll, a task that has not changed state, or a row shown as `dispatching` under a pid you cannot see is a checkpoint, not a failure. Do not cancel, close, resubmit, or `reconcile` on a checkpoint alone.

Act only on positive proof: the task status is terminal, the structured result is present, `logs` show the agent's exit, or `task list` names a blocking code such as `RUNNER_REPEATED_FAILURE:<code>`, `WAIT_BLOCKED`, or `LOG_DRAIN_UNAVAILABLE`. After three consecutive empty waits, enumerate instead of waiting blindly:

```text
worker task list --run <id> --json
worker task list --state open --outcome needs-input --json
worker workers --refresh
```

and act on each row's blocking code. `worker task reconcile` is the operator's reset for a parked turn; run it after reading the worker, not instead of reading it.

A finished task owes exactly one decision after `fetch`: a follow-up with `say` (the same agent session continues), keeping it open for inspection, or `close` (`--discard` also deletes the session on the worker). `close` is post-settlement cleanup, never a cancellation; use `cancel` for a running turn.

## Rules

- Never merge, check out, or push from this skill.
- Never read environment profiles.
- Never pick workers. The pool does. `--worker` is a diagnostic pin, never an SSH destination.
- Keep task identifiers and the run identifier; do not infer them from display order.
- Keep tasks independent. Do not use one task's workspace as another task's workspace.
- Never write a message into a running agent. Conversation is `say` between turns only.
- Never ask an agent to commit, switch branches, or push. The publisher commits the worktree changes after the turn; on Codex the sandbox keeps `.git` read-only and a commit attempt ends the turn `blocked`.
- A task with the default `--close-on done` closes itself after a `done` turn. `close --discard` also deletes the agent's session on the worker.
- Read the durable task outcome as well as the process exit status. A zero exit with status `blocked` is a failed turn.
- Never restart, resubmit, or repair a turn on an unverifiable observation. Restart only on positive proof the runner or the worker job exited; otherwise keep waiting or inspect.
- Verify the installed grammar before every dispatch: `worker skills get pool-dispatch --grammar-only` and `worker task submit --help` are the source of truth, not this file.

## Exit Codes

CLI exit codes keep v1 semantics. New stable codes sit in the v1 classes plus two new classes:

- `project`: `TASK_CONFIG_INVALID`, `PUBLISH_REQUIRES_COMMITTED_BASE`, `AGENT_UNSUPPORTED`, `BASE_NOT_ON_ORIGIN`.
- `snapshot`: `SNAPSHOT_CHANGED`, `SENSITIVE_PATH`, `UNTRACKED_INPUT` as in v1.
- `capacity`: unchanged, plus `CAPABILITY_MISSING` for pins.
- `git` (new): `BASE_PUSH_FAILED`, `BASE_UNAVAILABLE`, `WORKTREE_CREATE_FAILED`, `WORKTREE_INCONSISTENT`, `RESULT_FETCH_FAILED`, `PUBLISH_FAILED`.
- `infrastructure`: `HOST_LAYOUT_OUTDATED`, reported by the probe as unavailable until `worker setup` migrates the worker.
- `agent` (new): `AGENT_NOT_INSTALLED`, `AGENT_NOT_AUTHENTICATED`, `AGENT_EXITED`, `AGENT_LIMIT_REACHED`, `RESULT_UNPARSEABLE`, `SESSION_UNBOUND`, `ENV_PROFILE_PERMISSIONS`.
- `task`: `TASK_BUSY`, `FOLLOWUP_LIMIT`, `TASK_CLOSED`, `TASK_NOT_FOUND`, and `RUNNER_HANDOFF_FAILED`, which alone maps to the local I/O exit status `74`.

CLI exit codes: `64` usage and configuration, `69` pre-acceptance transport, `70` protocol or infrastructure, `74` local I/O, `75` capacity. Commands that end with a turn map the turn's outcome as follows:

| Turn outcome | `submit --wait`, `say --wait` | `wait` (any task in the set) |
|---|---|---|
| agent exited zero, status `done`, `needs_input`, or `unknown` | `0` | `0` if every task ended this way |
| agent exited zero, status `blocked` | `1` | `1` |
| agent exited non-zero with code N | `N`, unchanged, public code `AGENT_EXITED` | `1` |
| signalled, timed out, or cancelled | the status v1 assigns to a signalled, timed-out, or cancelled command | `1` |
| turn `lost`, `PUBLISH_FAILED`, `RESULT_UNPARSEABLE` | `70` | `1` |
| `wait --timeout` elapsed | not applicable | `70`, nothing cancelled |

The durable task record remains the authoritative distinction; JSON output always carries the outcome and code alongside the exit status.
