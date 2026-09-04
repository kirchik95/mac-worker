---
name: pool-dispatch
description: "Dispatch independent coding tasks through the mac-worker pool and collect their results."
---

# Pool Dispatch

This skill is the mechanical task loop. One task is one independent unit of work. The pool chooses workers. Never choose a worker yourself.

The CLI is the only interface. The skill contains no scheduling logic.

**Command availability on this branch:** `src/cli.rs` exposes `worker workers` and `worker dashboard` today. Every `worker task …` form, and `worker workers --refresh`, is still absent. Those forms are the phase 5c contract; do not replace them with direct worker access, SSH, or another tool.

## Grammar

Exact public grammar (spec section 7.1). Forms marked *phase 5c* are not in this binary yet.

```text
worker task submit [options] (--prompt TEXT | --prompt-file PATH)   # phase 5c
worker task batch FILE [--run-name NAME] [--max-parallel N]         # phase 5c
worker task list [--run RUN_ID] [--state STATE]                     # phase 5c
worker task status TASK_ID                                          # phase 5c
worker task logs [-f] TASK_ID [--turn N] [--raw]                    # phase 5c
worker task diff TASK_ID [--stat]                                   # phase 5c
worker task say TASK_ID (--message TEXT | --message-file PATH) [--wait]  # phase 5c
worker task cancel TASK_ID                                          # phase 5c
worker task result TASK_ID                                          # phase 5c
worker task fetch TASK_ID                                           # phase 5c
worker task close TASK_ID [--discard]                               # phase 5c
worker task wait (TASK_ID... | --run RUN_ID) [--timeout DURATION]   # phase 5c
worker task reconcile     # phase 5c; re-own dead runners and re-enqueue orphaned tasks without submitting anything
worker workers [--refresh]  # `worker workers` exists; `--refresh` is phase 5c
worker dashboard          # exists; the tasks-and-runs view is a later phase
```

`submit` options:

```text
--agent codex|claude|cursor|opencode     required
--model ID                               optional, agent-specific
--project PATH                           default: current worktree
--base REF                               default: HEAD
--wip                                    include uncommitted changes as a temporary base commit
--include PATTERN                        untracked inputs for --wip, same policy as v1
--source local|origin                    default from .worker.toml, else local
--publish fetch|push                     repeatable; default from .worker.toml, else fetch
--publish-branch NAME                    origin branch name used only by publish = push
--timeout DURATION                       per turn; default 45m; max 24h
--max-turns N                            agent-internal turn cap where supported
--max-budget-usd AMOUNT                  where supported
--max-followups N                        follow-up turns allowed after the first; default 10
--close-on done|never                    default done
--env-profile NAME                       overrides .worker.toml
--worker NAME                            diagnostic pin, never a raw SSH destination
--no-wait                                CAPACITY_BUSY instead of queueing
--wait                                   stay attached until the first turn ends
```

Without `--wait`, `submit`, `batch`, and `say` return as soon as the task record, the base commit in the transfer repository, and the queue row exist and a local turn runner has taken ownership of the row, or the row is parked behind the per-worker runner cap. With `--wait` the same work happens in the foreground and the command follows the turn's event log to its end. `list`, `status`, `result`, `diff`, and `logs` are read-only; `submit`, `batch`, `say`, `cancel`, `close`, `wait`, and `reconcile` perform runner recovery first.

`--json` on any command emits the same typed records the dashboard consumes; `submit`, `say`, `logs -f`, and `wait` emit versioned NDJSON events, with log chunks base64-encoded as in v1.

This execution core accepts `source = local` and `publish = fetch` only. `source = origin`, `publish = push`, `--publish-branch`, `--agent cursor`, and `--agent opencode` are later phases and must be rejected at preflight. Claude Code is deferred on the workers by operator decision; do not submit `--agent claude` until the operator enables it.

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

Inspect one task without mutating it:

```text
worker task status <id>
worker task diff <id> --stat
```

For a task that reports `needs_input`, write the answer to a message file and send it as a new turn:

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

## Rules

- Never merge, check out, or push from this skill.
- Never read environment profiles.
- Never pick workers. The pool does. `--worker` is a diagnostic pin, never an SSH destination.
- Keep task identifiers and the run identifier; do not infer them from display order.
- Keep tasks independent. Do not use one task's workspace as another task's workspace.
- Never write a message into a running agent. Conversation is `say` between turns only.
- Read the durable task outcome as well as the process exit status. A zero exit with status `blocked` is a failed turn.

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
