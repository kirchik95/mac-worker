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

**Command availability:** the release exposes `worker task …` and `worker workers --refresh`. Verify the exact installed grammar with `worker task --help` and the relevant subcommand help before dispatching; do not replace a rejected public form with direct worker access, SSH, or another tool.

## Grammar

Exact public grammar in the current release. The execution-core scope restrictions below still apply.

```text
worker task submit [options] (--prompt TEXT | --prompt-file PATH) [--title TEXT]
worker task batch FILE [--name NAME] [--max-parallel N] [--wait]
worker task list [--run RUN_ID] [--state STATE] [--outcome KIND] [--full]
worker task status TASK_ID [--full]
worker task logs [-f] TASK_ID [--turn N] [--raw]
worker task diff TASK_ID [--stat]
worker task say TASK_ID (--message TEXT | --message-file PATH) [--wait]
worker task cancel TASK_ID
worker task result TASK_ID
worker task fetch TASK_ID
worker task close TASK_ID [--discard]
worker task wait (--task-id TASK_ID | --run RUN_ID) [--timeout DURATION]
worker task reconcile     # re-own dead runners and re-enqueue orphaned tasks without submitting anything
worker workers [--refresh]
worker dashboard [--port N] [--no-open]   # read-only observer on loopback with the tasks-and-runs view
```

`submit` options:

```text
--agent codex|claude|cursor|opencode     optional; defaults from [task].default_agent
--model ID                               optional, agent-specific; default from [task].model
--effort LEVEL                           optional reasoning effort; default from [task].effort
--project PATH                           default: current worktree
--base REF                               default: HEAD
--wip                                    include uncommitted changes as a temporary base commit
--include PATTERN                        untracked inputs for --wip, same policy as v1
--source local|origin                    default from .worker.toml, else local
--publish fetch|push                     one CLI value; default from .worker.toml, else fetch
--publish-branch NAME                    origin branch name used only by publish = push
--timeout DURATION                       per turn; default 45m; max 24h
--max-turns N                            agent-internal turn cap where supported
--max-budget CENTS                       where supported
--max-followups N                        follow-up turns allowed after the first; default 10
--close-on done|never                    default done
--env-profile NAME                       overrides .worker.toml
--worker NAME                            diagnostic pin, never a raw SSH destination
--no-wait                                CAPACITY_BUSY instead of queueing
--wait                                   stay attached until the first turn ends
```

Without `--wait`, `submit`, `batch`, and `say` return as soon as the task record, the base commit in the transfer repository, and the queue row exist and a local turn runner has taken ownership of the row, or the row is parked behind the per-worker runner cap. With `--wait` the same work happens in the foreground and the command follows the turn's event log to its end. `list`, `status`, `result`, `diff`, and `logs` are read-only; `submit`, `batch`, `say`, `cancel`, `close`, `wait`, and `reconcile` perform runner recovery first.

`--json` on any command emits the same typed records the dashboard consumes; `submit`, `say`, `logs -f`, and `wait` emit versioned NDJSON events, with log chunks base64-encoded as in v1.

The current release accepts `source = local|origin` and `publish = fetch|push`; `--publish-branch` is valid only with `push`, and a `--wip` base cannot push (`PUBLISH_REQUIRES_COMMITTED_BASE`). Agents on this pool:

- `codex`: pass `--model gpt-6-astra --effort xhigh` (the pool default; the workers' own Codex configuration is set to the same values). The effort reaches Codex as `-c model_reasoning_effort="xhigh"`; without the flag the worker's own Codex configuration decides. Only Codex reads it — the other agents ignore it.
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
- Verify the installed grammar before every dispatch: `worker task --help` and `worker task submit --help` are the source of truth, not this file.

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
