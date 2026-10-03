---
name: pool-dispatch
description: "Dispatch independent coding tasks through the mac-worker pool and collect their results. Use on an explicit /pool-dispatch call, or whenever the user asks to run work in the pool or on the Mac minis: «отправь выполняться в пул», «отправь в пул», «запусти в пуле», «запусти на mini», «через worker task», «send it to the pool», «run it on the minis», «dispatch to the workers»."
---

# Pool Dispatch

## When To Use

- The user invokes `/pool-dispatch` explicitly.
- The user asks to run work in the pool or on the minis, in any wording: «отправь выполняться в пул», «отправь в пул», «запусти в пуле», «запусти на mini», «через worker task», "send it to the pool", "run it on the minis", "dispatch to the workers".
- The user asks to check, follow, answer, or fetch a pool task: «что с задачей в пуле», «забери результат», «ответь агенту», "task status", "fetch the result".

For fresh work, before the first submit write the brief with `pool-task-authoring` unless the user already supplied a prompt file. For an existing conversation, choose the context option below. Do not send work to the pool on your own initiative without one of the triggers above; when work merely looks parallelisable, say so and ask.

This skill is the mechanical task loop. One task is one independent unit of work. The pool chooses workers. Never choose a worker yourself.

The CLI is the only interface. The skill contains no scheduling logic.

**Command availability:** the release exposes `worker task …`, `worker dashboard`, `worker controller run`, and `worker workers --refresh`. Run `worker skills get pool-dispatch --grammar-only` (or `worker task <cmd> --help`) and use that output as the only grammar; do not replace a rejected public form with direct worker access, SSH, or another tool.

Default dispatch is the laptop queue (`[controller]` missing or `enabled = false`). If the operator already enabled a remote controller, keep this same `worker task …` grammar — do not invent a second CLI, and do not start `worker controller run` from this skill. A controller-only laptop config may omit `[[workers]]`; commands that need a local worker list then fail with `at least one worker is required` (for example `worker workers`, `setup`, `doctor`, and `gc`). Operator notes: repository `docs/usage.md` section Remote controller.

## Grammar Source

Do not copy CLI flags from this file. Run `worker skills get pool-dispatch --grammar-only` (or `worker task <cmd> --help`) and use that output as the only grammar.

That generated grammar includes `task integrate` for re-driving blocked configured tasks.

Default laptop queue: without `--wait`, `submit`, `batch`, and `say` return as soon as the task record, the base commit in the transfer repository, and the queue row exist and a local turn runner has taken ownership of the row, or the row is parked behind the per-worker runner cap. With `--wait` the same work happens in the foreground and the command follows the turn's event log to its end.

Enabled remote controller: without `--wait`, those commands return on a durable `host controller-rpc` ACK. That ACK means the request is persisted on the controller store. It does not mean a runner has started, or that the agent is running — enabled submit can stay queued (`park_only`) until the leader starts a runner. Keep `worker controller run` for autonomous progress. With `--wait` the CLI waits until the selected task or run is quiescent. Logs remain a separate command (`worker task logs`).

`list`, `status`, `result`, `diff`, and `logs` are read-only. `submit`, `batch`, `say`, `wait`, and `reconcile` drive ordinary recovery. Configured close/cancel/interrupt records its stop authority before integration recovery can start more work; do not run a separate reconcile before a requested stop.

`--json` on any command emits the same typed records the dashboard consumes; `submit`, `say`, `logs -f`, and `wait` emit versioned NDJSON events. Serialized log chunks use standard padded base64 in the `data` field.

The current release accepts `source = local|origin` and `publish = fetch|push`; `--publish-branch` is valid only with `push`, and a `--wip` base cannot push (`PUBLISH_REQUIRES_COMMITTED_BASE`). Agents on this pool:

- `codex`: Codex with the configured defaults; pass `--model`/`--effort` only when the task needs a different one; see `worker skills get pool-dispatch` for the effective values. Only Codex reads `--effort` — the other agents ignore it.
- `opencode`: no model flag uses the worker's default (OpenCode Zen, Muse Spark 1.3, free); OpenCode Go models are `--model opencode-go/<model>`.
- `cursor`: always `--env-profile agents`; that worker-side profile carries the Cursor login and the login-keychain unlock. Never read or copy it.
- `claude`: deferred on the workers by operator decision; do not submit it until the operator enables it.

Watch the pool while tasks run: `worker dashboard --port 8765 --no-open`, then open `http://127.0.0.1:8765`.

## Submit

For each fresh task, put its prompt in a Markdown file and submit it:

```text
worker task submit --agent <name> --prompt-file <file> --json
```

Keep every returned `task_id`. For a list of tasks, use the batch command with its batch file:

```text
worker task batch <file> --json
```

Keep the returned run identifier and all task identifiers.

### Choose The Context

- **Native session:** use `worker task submit --from-session claude[:<uuid>]` or `codex[:<uuid>]` to continue an interactive conversation with lots of useful context. Start it in this project's root; omit the id for the latest matching session. Still supply `--prompt "<what to do next>"`. A dirty checkout needs `--wip`, which is incompatible with integration (`INTEGRATION_WIP_BASE`): commit the intended base before integrating, or use `--no-integrate` when the user wants a manual result. The laptop original stays usable; the pool continues a copy under pool policy. Confirm the user is comfortable copying it: secret scrubbing is incomplete. Only snapshot-sourced `task submit` supports this, not batches or DAG children.
- **Handoff note:** for Cursor, OpenCode, unsupported worker versions, or a different target agent, ask: “Write `.worker/handoff.md` with the goal, what is done, current state including uncommitted changes, decisions and constraints, open questions, and exact next steps. Do not include secrets.” Review it, then submit a fresh conversation:

  ```text
  worker task submit --no-integrate --wip --include .worker/handoff.md --prompt "Read .worker/handoff.md and continue the work it describes."
  ```

- **Fresh brief:** for independent work that does not need the old conversation, use `pool-task-authoring` and the normal prompt-file submit above.

Check the installed grammar before using these forms. Details and failure hints: repository `docs/usage.md`, “Continue a laptop session in the pool”.

For an integrating session submit, retry the same frozen controller envelope only after the source stream is complete. Keep its paired base/session pins until acknowledgement or request retirement; do not capture the live laptop conversation again. Pre-record failure or submission rollback releases paired task pins idempotently. Auxiliary repair resumes the imported worker session without importing or replacing it again. Integration never sends session packages or transport refs to origin.

## Automatic Integration

Use the project's configured automatic integration policy. It is opt-in and disabled by default. Do not enable it, pick another target, or disable an inherited policy merely to bypass a refusal. Submit/task overrides win over batch defaults, then `.worker.toml` `[task]` settings. The target is a short branch on the project's own canonical origin. An unavailable helper/controller refuses enabled work with `INTEGRATION_UNAVAILABLE`; there is no ordinary fallback.

Follow the `integration` snapshot and `workflow_state`, not ordinary `done` alone. Pending/parked work is still automatic; integrated success needs no accept or manual close. Conflicts use the same worker, agent and session. If integration is blocked, read its stable code, result and logs, repair the cause within the user's scope, then use the installed `worker task integrate <id>` grammar to re-drive the same source and target. If repair needs code changes, use a normal `say` with guidance after the stop barrier; its next Done result starts a new cycle.

Verification defaults to `never`. Resolve and verify turns consume the ordinary `max_followups` allowance; re-drive cannot replenish it. A source with checks requires a nonempty all-pass recovery report; a source with no checks may have an empty recovery report. Any `fail` or `error` blocks. The host does not run project checks, and agent claims are not independent verification.

Controller drain pauses ordinary handoffs and integration; `worker controller drain --off` resumes both. Disable pauses integration only and leaves ordinary drain unchanged; re-enable preserves the integration pause until explicit `drain --off`. A controller restart creates no pause. Helper rollback also parks integration. Admitted steps and running turns finish before parking. Respect the operator's pause and restore compatible support before resuming. Parking preserves remaining active admission time, auxiliary ID, queue position and spent follow-ups; missing or reset pause history can expire admission early, never renew it. A running auxiliary keeps its execution timeout. A pre-feature helper can lose the repair workspace after seven idle host-status days; see repository `docs/usage.md`, section "Pause, stop and rollback", for the operator warning and settlement limits.

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

When configured integration covers the latest ordinary work, `wait` waits for integration settlement and runner retirement. Integrated work returns exit 0 after receipt/result import, even when requested `never` leaves the task Open. Blocked work returns the integration code's exit status; read `status` or `result` for its cause. Pending or parked work keeps waiting; `--timeout` returns `WAIT_TIMEOUT` (70) without cancelling it. A newer ordinary turn follows ordinary wait rules until its own integration cycle starts. With integration disabled, ordinary wait behavior is unchanged.

Inspect one task without mutating it:

```text
worker task status <id>
worker task diff <id> --stat
```

`diff --stat` still works after the default auto-close. The worker has removed the task workspace; the diff compares the retained base commit with the result commit in the worker project mirror. If those commits have been collected, the command fails with `RESULT_NOT_RETAINED` and the message `task workspace is closed and its result is no longer retained`.

List exactly the tasks waiting on an answer:

```text
worker task list --state open --outcome needs-input --json
```

For a task that reports `needs_input`, read its questions from `worker task status <id> --json`: each question is `{"text": …, "options": [...]}`, and a question with no options is a bare string. When a question offers options, answer with one of them verbatim. Write the answer to a message file and send it as a new turn:

```text
worker task say <id> --message-file <file> --wait
```

Do not send a message to an active turn. `say` is for the next turn. A `say` while a turn is running is `TASK_BUSY`.

With integration disabled, an ordinary `done` result is ready for manual review. Fetch it:

```text
worker task fetch <id>
```

Report the remote-tracking ref returned by `fetch`. For configured integration, report its target/state and merge or observed OID; fetch remains available for inspection after settlement. Do not close a pending or parked integration just because its source turn said `done`.

When an ordinary turn reports `blocked` or fails, inspect both the structured result and the logs below. An integration-blocked task follows the re-drive loop above instead of automatic discard:

```text
worker task result <id> --json
worker task logs <id>
```

Then either write guidance and use `say`, or discard the task:

```text
worker task close <id> --discard
```

For a settled manual result, close only when keeping the result or giving up integration is the intended decision:

```text
worker task close <id>
```

`worker task reconcile` re-owns dead runners, re-enqueues orphaned tasks and recovers retained integration intents; it does not submit new work. In direct mode a dead integration child needs laptop `wait` or `reconcile`; a powered-off laptop provides no unattended recovery. Controller tasks remain with their controller owner after disable.

## Liveness And Settlement

Absence of evidence is not evidence of death. A `wait` timeout, an empty `list` poll, a task that has not changed state, or a row shown as `dispatching` under a pid you cannot see is a checkpoint, not a failure. Do not cancel, close, resubmit, or `reconcile` on a checkpoint alone.

Act only on positive proof: the task status is terminal, the structured result is present, `logs` show the agent's exit, or `task list` names a blocking code such as `RUNNER_REPEATED_FAILURE:<code>`, `WAIT_BLOCKED`, or `LOG_DRAIN_UNAVAILABLE`. After three consecutive empty waits, enumerate instead of waiting blindly:

```text
worker task list --run <id> --json
worker task list --state open --outcome needs-input --json
worker workers --refresh
```

and act on each row's blocking code. `worker task reconcile` is the operator's reset for a parked turn; run it after reading the worker, not instead of reading it.

A settled manual task needs a decision: follow up with `say`, keep it Open for inspection, or close while keeping the result (`--discard` also deletes the session and retained result on the worker). Automatic integrated success needs no accept. Manual close gives up unfinished integration or keeps its result; it is never permission to merge. Use `cancel` for a running ordinary turn.

If an integration stop cannot be confirmed, `INTEGRATION_STOP_UNCONFIRMED` leaves the task nonterminal with the mutation pending. Restore connectivity and observe again; do not report close/cancel/discard as complete. A committed target update is retained, never undone by this skill. `INTEGRATION_ALREADY_COMMITTED` means integration won before cancellation.

## Rules

- Use configured automatic integration through the task CLI. Never run this skill's own laptop Git merge, checkout, or push.
- Never read environment profiles.
- Never pick workers. The pool does. `--worker` is a diagnostic pin, never an SSH destination.
- Keep task identifiers and the run identifier; do not infer them from display order.
- Keep tasks independent. Do not use one task's workspace as another task's workspace.
- Never write a message into a running agent. Conversation is `say` between turns only.
- Never ask an agent to commit, switch branches, or push. The publisher commits ordinary turn changes; the integration helper commits accepted resolution. On Codex the sandbox keeps `.git` read-only and a commit attempt ends the turn `blocked`.
- With integration disabled, default `--close-on done` closes after a `done` turn and removes the workspace. Configured tasks settle integration and result import first; requested `never` keeps the Open session. `diff` after close reads retained mirror commits. Discard follows confirmed integration stop, deletes the worker session and drops retained task commits; it never rewinds origin.
- Read the durable task outcome as well as the process exit status. A zero exit with status `blocked` is a failed turn.
- Never restart, resubmit, or repair a turn on an unverifiable observation. Restart only on positive proof the runner or the worker job exited; otherwise keep waiting or inspect.
- Verify the installed grammar before every dispatch: `worker skills get pool-dispatch --grammar-only` and `worker task submit --help` are the source of truth, not this file.

## Exit Codes

Stable public error codes are grouped by error class:

- `project`: `TASK_CONFIG_INVALID`, `PUBLISH_REQUIRES_COMMITTED_BASE`, `AGENT_UNSUPPORTED`, `BASE_NOT_ON_ORIGIN`.
- `snapshot`: `SNAPSHOT_CHANGED`, `SENSITIVE_PATH`, `UNTRACKED_INPUT` (exit `70`).
- `capacity`: `CAPACITY_BUSY`, plus `CAPABILITY_MISSING` for pins (exit `75`).
- `git`: `BASE_PUSH_FAILED`, `BASE_UNAVAILABLE`, `WORKTREE_CREATE_FAILED`, `WORKTREE_INCONSISTENT`, `RESULT_FETCH_FAILED`, `PUBLISH_FAILED`, `ORIGIN_AUTH_FAILED`. After origin credentials are repaired, re-drive a terminal failed delivery with `worker task publish-retry <task_id>` instead of resubmitting.
- `infrastructure`: `HOST_LAYOUT_OUTDATED`, reported by the probe as unavailable until `worker setup` migrates the worker.
- `agent`: `AGENT_NOT_INSTALLED`, `AGENT_NOT_AUTHENTICATED`, `AGENT_EXITED`, `AGENT_LIMIT_REACHED`, `RESULT_UNPARSEABLE`, `SESSION_UNBOUND`, `ENV_PROFILE_PERMISSIONS`.
- `task`: `TASK_BUSY`, `FOLLOWUP_LIMIT`, `TASK_CLOSED`, `TASK_NOT_FOUND`, `RESULT_NOT_RETAINED` (closed task whose mirror commits are gone: `task workspace is closed and its result is no longer retained`), and `RUNNER_HANDOFF_FAILED`, which alone maps to the local I/O exit status `74`.

CLI exit codes: `64` usage and configuration, `69` pre-acceptance transport, `70` protocol or infrastructure, `74` local I/O, `75` capacity. The table describes ordinary turn outcomes; configured integration wait follows the settlement rules above.

| Turn outcome | `submit --wait`, `say --wait` | `wait` (any task in the set) |
|---|---|---|
| agent exited zero, status `done` or `needs_input` | `0` | `0` if every task ended this way |
| status `unknown` | `70` | `70` |
| agent exited zero, status `blocked` | `1` | `1` |
| agent exited non-zero with code N | `N`, unchanged, public code `AGENT_EXITED` | `1` |
| signalled, timed out, or cancelled | `1` | `1` |
| turn `lost`, `PUBLISH_FAILED`, `ORIGIN_AUTH_FAILED`, `RESULT_UNPARSEABLE` | `70` | `1` |
| `wait --timeout` elapsed | not applicable | `70`, nothing cancelled |

The durable task record remains the authoritative distinction. Read `status` or `result` JSON for its outcome and code; wait completion JSON reports selected `task_ids` and the aggregate `exit_code`.
