---
name: pool-dispatch
description: "Dispatch independent coding tasks through the mac-worker pool and collect their results."
---

# Pool Dispatch

> **Phase 5c contract note:** The `worker task` commands land with phase 5c of the execution plan. Until that phase lands, this skill documents the contract; do not replace it with direct worker access.

This skill is the mechanical task loop. One task is one independent unit of work. The pool chooses workers. Never choose a worker yourself.

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

For a task that reports `needs_input`, write the answer to a message file and send it as a new turn:

```text
worker task say <id> --message-file <file> --wait
```

Do not send a message to an active turn. `say` is for the next turn.

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

## Rules

- Never merge, check out, or push from this skill.
- Never read environment profiles.
- Never pick workers. The pool does.
- Keep task identifiers and the run identifier; do not infer them from display order.
- Keep tasks independent. Do not use one task's workspace as another task's workspace.

## Exit Codes

Read a script result using the durable task outcome as well as the process exit status. The CLI uses these classes:

| Situation | Exit code |
|---|---:|
| Usage or configuration error | `64` |
| Pre-acceptance transport error | `69` |
| Protocol or infrastructure error | `70` |
| Local I/O error | `74` |
| Capacity error | `75` |

For commands that end a turn:

| Turn outcome | `submit --wait`, `say --wait` | `wait` |
|---|---:|---:|
| Agent exited zero; status `done`, `needs_input`, or `unknown` | `0` | `0` if every task has this outcome |
| Agent exited zero; status `blocked` | `1` | `1` |
| Agent exited non-zero with code `N` | `N` | `1` |
| Signalled, timed out, or cancelled | the v1 status for that command | `1` |
| Turn is `lost`, `PUBLISH_FAILED`, or `RESULT_UNPARSEABLE` | `70` | `1` |
| `wait --timeout` elapsed | not applicable | `70`; nothing is cancelled |

The JSON result remains authoritative when it distinguishes a task outcome from the CLI exit code.
