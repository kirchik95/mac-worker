# Using mac-worker

Start with the [quick start](../README.md#quick-start) to install the CLI and connect one Mac. This reference covers agents, task settings, review, batches, capacity, origin delivery, the dashboard, and remote commands. Laptop skills live in the repository at [`.claude/skills/`](../.claude/skills/); they are a local-agent install, not a worker install.

## Agents

| Agent | Flag | Login lives | Notes |
|---|---|---|---|
| Codex | `--agent codex` | file-based login on the worker | `--model` / `--effort` are passed through; runs in the Codex workspace sandbox |
| OpenCode | `--agent opencode` | OpenCode auth store on the worker | uses the worker's default model; pick another with `--model opencode-go/<model>` |
| Cursor | `--agent cursor --env-profile agents` | Cursor login on the worker + an env profile | needs the profile described below because Cursor keeps its login in the macOS keychain |
| Claude Code | `--agent claude` | worker login or env profile | check with `worker init <ssh> --agent claude`; add `--env-profile` for profile credentials |

Honor the agent, model, and profile you configured. Do not copy credential profile contents. Every agent gets the same contract: work only in the task worktree, do not switch branches or push, and end with a JSON result. Agents that cannot ask questions interactively return `needs_input` with the exact question; you answer with `worker task say <id> --message "…" --wait`, which starts the next turn in the same agent session.

### Env profiles

Some agents need environment variables or a keychain unlock on the worker. Put them in an owner-only file on each worker, never in the repository:

```sh
umask 077
mkdir -p ~/.config/mac-worker/env && chmod 700 ~/.config/mac-worker/env
$EDITOR ~/.config/mac-worker/env/agents.env      # KEY=value per line
chmod 600 ~/.config/mac-worker/env/agents.env
```

Recognised keys include `CURSOR_API_KEY`, and the host-only `MAC_WORKER_KEYCHAIN_PASSWORD` (plus optional `MAC_WORKER_KEYCHAIN_PATH`), which mac-worker feeds to `security unlock-keychain` on stdin right before an agent that needs the login keychain runs. The two keychain values are consumed by the helper and never exported to the agent, logged, or shown anywhere. A profile that is group- or world-readable is refused.

You own SDKs, language tools, agent logins, and secrets on each Mac. mac-worker does not install toolchains or copy those profiles.

## Task lifecycle

<p align="center">
  <img src="images/task-lifecycle.png" alt="Task lifecycle: submit, queue, turn, publish, fetch, close, with done / needs_input / blocked outcomes" width="100%">
</p>

```text
worker task submit   --agent <a> (--prompt TEXT | --prompt-file PATH) [--wait] [--model M] [--effort E] [--close-on done|never]
worker task batch    FILE [--name NAME] [--max-parallel N] [--wait | --preview]
worker task list     [--run ID|NAME] [--state open] [--outcome needs-input]
worker task status   <id> [--full]     worker task logs <id> [-f]     worker task diff <id> --stat
worker task wait     --task-id <id> | --run <ID|NAME> [--timeout 30m]
worker task say      <id> (--message TEXT | --message-file PATH) [--wait]
worker task result   <id>          worker task fetch <id>
worker task cancel   <id>          worker task close <id> [--discard]
worker task reconcile              # re-own dead runners, re-queue orphaned turns; may launch already-frozen eligible DAG children
worker gc [--apply]                # preview, then reclaim old tasks, branches, mirrors on the workers
```

Confirm the installed grammar with `worker task --help`. There is no `worker task accept` verb.

`worker task batch FILE --preview` validates the file and prints the plan (`dag.status = "enforced"`). It does not open client state or dispatch. `--preview` conflicts with `--wait`. Submit of a named graph (`depends_on` or `base = "from:<id>"`) freezes that run and launches eligible roots; invalid or cyclic graphs are `TASK_CONFIG_INVALID` and create no run. Independent batches (empty `depends_on` and no `from:`) keep today's create-run-and-submit path.

`--max-parallel` is a **requested run cap**. Omitted, it defaults to `sum(worker.slots)` (each worker defaults to 1). An explicit positive value is accepted even when it is larger than that sum or than current host occupancy; extra tasks wait for a free execution slot. Zero is `TASK_CONFIG_INVALID`. The CLI does not reject “too many” relative to host capacity.

`worker task logs` without `--raw` prints recognised agent events one line at a time, hides per-token noise, and folds consecutive unrecognised structured events into `event: <type>[/<subtype>] ×N` summaries (the count is omitted for one event). It keeps stderr and launch failures verbatim and prints failure lines such as `turn 1 failed: …` even when the agent wrote nothing; use `--raw` for the original log bytes.

`worker task wait` returns only when all selected tasks are quiescent and their runners have released ownership, so `worker task close`, `worker task say`, and `worker task fetch` can run immediately afterward. `wait --run` is not complete while DAG nodes are still waiting or claimed; an empty materialized task list is not completion. After runner recovery, `worker task reconcile` also advances already-frozen eligible DAG nodes; it does not start a new operator batch. `TASK_BUSY` and capacity errors such as `CAPABILITY_MISSING` include their reason.

Outcomes are recorded on the task, independent of the process exit code:

- `done`: the agent finished and the branch is published. That is **not** human acceptance. Default `--close-on done` then closes the task. Origin `delivery` may still be `pending` / `retrying`. For a human review loop (ready for review → follow-up → accepted), submit with `--close-on never`, then `worker task say` as needed and `worker task close` when you accept.
- `needs_input`: the agent has a bounded question; `say` answers it.
- `blocked`: the agent could not finish. Read `result` and `logs`, then `say` guidance or `close --discard`.
- `unknown`: the agent did not return a structured result; the branch is still published.

`worker task result` / `status --json` show the outcome, summary, and any **agent-reported** checks. Those checks are claims from the worker agent, not independent verification. `worker task diff <id> --stat` lists the published change. `worker task fetch <id>` prints the remote-tracking ref (`refs/remotes/mac-worker/<worker>/task/<id>`) — the current-turn import proof on the laptop. Your working tree stays unchanged.

### Review and close

Default `--close-on done` auto-closes after agent `done`. For an explicit review loop:

```bash
worker task submit --close-on never --agent <a> --prompt-file brief.md
worker task wait --task-id <id>
worker task result <id>
worker task diff <id> --stat
worker task fetch <id>
worker task close <id>            # human accept
# or: worker task say <id> --message-file followup.md --wait
# or: worker task close <id> --discard
```

Public CLI `say` / `close` have no revision flags. Wait first. Dashboard reply/accept are the same operations with a current-card check ([Dashboard](#dashboard)).

### Project setup

Optional `[setup]` in `.worker.toml` is opt-in. Absent, behavior is unchanged. Commands are operator-written; mac-worker never infers packages.

```toml
[setup]
timeout = "10m"
commands = ["cargo fetch --locked"]
check = "cargo fetch --locked --offline"
lockfiles = ["Cargo.lock"]
```

`check` proves **this** workspace only. A matching identity in another worktree is not readiness. You own the toolchain. Profile **names** may appear in identity hashes; profile values and secrets do not.

## Capacity and slots

Each `[[workers]]` entry defaults to `slots = 1` **per worker** (`1..=8`). That laptop value is a client **ceiling**. The Mac’s durable authority is `leases/capacity.json` `{ "slot_count": N }` on the host (hidden `worker host set-slots N`). Combined detached runner capacity is `sum(worker.slots)`, not the number of workers.

Same `task_id` is serialized (`WORKSPACE_BUSY`). Distinct task IDs from one checkout may overlap when that host’s `slot_count >= 2`. Layout, migrate, and probe occupancy: [multiple execution slots](superpowers/specs/2026-09-10-slots-design.md).

## Origin delivery

With `publish = push`, each turn pins a durable delivery intent **before** the execution slot is released. Agent outcome and origin state are independent: `done` plus origin `pending` is valid. `status` / dashboard **project** remote delivery without rewriting a Closed local record. Discard while delivery is `pending`/`retrying` is `TASK_BUSY` (`DELIVERY_PENDING`).

Host pump (hidden `worker host`, not in top-level `--help`):

| Flag | Semantics |
|---|---|
| `--watch` | Take `outbox.lock` or exit immediately if another pump holds it. Publish watcher identity, recover the due registry once, then loop. |
| `--once` | Blocking one-shot pump. If the pump lock is held, `OUTBOX_BUSY`. Does not spawn `--watch`. |
| `--enable` | Write `locks/outbox-enabled.json`. Does not start a watcher. Reboot recovery is `--enable` plus a LaunchAgent that actually starts `--watch`. |
| `--wake` | Hidden. If a live watcher exists, no-op; else spawn `--watch` and exit. |

`--once` is a one-shot pump, not a substitute for `--watch`. Do not treat a single `--once` after a crash as proof that due-registry recovery already ran; `--watch` recovers the due index on start. Host layout of intents, pins, and the due registry: [durable origin outbox](superpowers/specs/2026-09-10-origin-outbox.md).

Project defaults:

```toml
[task]
default_agent = "codex"
source = "local"            # or "origin": start from the exact commit on your Git remote
publish = ["fetch"]         # add "push" to also push task/<id> to origin
timeout = "45m"
max_followups = 10
```

`.worker.toml` `[task]` has no `close_on` field. Set close policy with `--close-on` on submit, or `close_on` at the batch top level or on a `[[tasks]]` entry (`done` or `never`).

To use the exact base commit from your Git remote and push the result branch back to that remote:

```toml
[task]
source = "origin"
publish = ["fetch", "push"]
```

The base commit must already be on the remote. The worker account needs its own Git access to that remote; SSH agent forwarding from the laptop is disabled. `--wip` cannot push (`PUBLISH_REQUIRES_COMMITTED_BASE`).

A batch file groups tasks into a run with shared defaults. Independent tasks (no `depends_on`, no `base = "from:<id>"`) still submit together:

```toml
version = 1
agent = "codex"
timeout = "45m"

[[tasks]]
title = "Flaky login spec"
prompt_file = "tasks/fix-flaky-login.md"

[[tasks]]
title = "Extract billing client"
prompt = "Move the billing HTTP client into packages/billing-client …"
agent = "opencode"
```

Named dependencies execute when each parent is **Closed and Done** (including `close_on = never`, which needs human `close` after a Done turn). Open+NeedsInput and Open+Done wait. Failed, Abandoned, Lost, or Closed without Done block descendants (`DAG_PARENT_FAILED`); they are not launched. `from:` copies that parent's current accepted imported OID on the laptop; origin `pending` does not block the bind when the local import proof is complete. Submit freezes each node's prompt, settings, and base OID; restart does not reread the batch file. List rows for not-yet-submitted nodes may show `DAG_WAITING` or `DAG_CLAIMED`.

```toml
version = 1
agent = "codex"
timeout = "45m"

[[tasks]]
id = "api"
prompt_file = "tasks/api-validation.md"
close_on = "never"

[[tasks]]
id = "tests"
depends_on = ["api"]
base = "from:api"
prompt_file = "tasks/api-tests.md"
```

Preview with `worker task batch tasks.toml --preview` before dispatch. Lifecycle: [batch DAG](dag-design.md).

```toml
[task]
default_agent = "codex"
model = "gpt-5.6-luna"
effort = "max"
env_profile = "agents"
source = "local"            # or "origin": start from the exact commit on your Git remote
publish = ["fetch"]         # add "push" to also push task/<id> to origin
timeout = "45m"
max_followups = 10

[setup]
commands = ["cargo fetch --locked"]
check = "cargo fetch --locked --offline"   # current workspace only
lockfiles = ["Cargo.lock"]
```

Optional `[setup]` runs in the task workspace with the same account and env-profile as the agent, before the agent starts. Absent `[setup]` keeps today's defaults. `check` proves **this** workspace is ready; a matching identity in another worktree is not skip proof. Toolchains stay user-owned.

`worker task batch FILE --preview` resolves agent/model/worker, declared files, acceptance, and setup without creating tasks, opening client state, or talking to workers. Preview reports `dag.status = "enforced"` (`Dependencies execute when parents are Closed and Done.`). Submit of `depends_on` or `base = "from:<id>"` executes that graph; independent batches stay on today's path. Declared `files` are advisory overlap hints. Declared `acceptance` is copied into the agent prompt as instructions, not proven by mac-worker.

To use the exact base commit from your Git remote and push the result branch back to that remote, change these project settings:

```toml
[task]
source = "origin"
publish = ["fetch", "push"]
```

The base commit must already be on the remote. The worker account needs its own Git access to that remote; SSH agent forwarding from the laptop is disabled.

Writing good briefs is its own skill. Two Claude Code skills ship with the repository and work from any project: [`pool-task-authoring`](../.claude/skills/pool-task-authoring/SKILL.md) turns an objective into one-turn, headless-safe tasks, and [`pool-dispatch`](../.claude/skills/pool-dispatch/SKILL.md) is the mechanical submit / wait / answer / fetch loop. Say "send it to the pool" and Claude Code uses them.

## Dashboard

<p align="center">
  <img src="images/dashboard.png" alt="The dashboard: three machine cards, queue, active work, and the task ledger" width="100%">
</p>

```bash
worker dashboard                 # opens http://127.0.0.1:<port>
worker dashboard --port 8765 --no-open
worker dashboard --no-facts-refresh   # skip stale agent-facts refresh only
```

The dashboard listens only on loopback. It does not start or cancel tasks. It shows workers (including slot occupancy and host load), the FIFO queue, active turns, the task ledger, per-task detail (outcome, summary, agent-reported checks, changed files, fetch ref, delivery), and run history. Deep link `#/tasks/<id>` selects that card. It never shows prompts, credentials, or profile values.

`--no-facts-refresh` disables the optional fifteen-minute agent-facts refresh so Overview does not probe idle workers. It is **not** a read-only switch: Settings can still save native model/effort defaults, and task cards can still reply or accept.

Reply and accept use the same `TaskClient::say` / `close` paths as the CLI. They require the **current card**. A stale card is rejected (`TASK_REVISION_CONFLICT`, HTTP 409) and must not enqueue another turn.

```text
POST /api/v1/tasks/{task_id}/reply
POST /api/v1/tasks/{task_id}/accept
```

Loopback `Host` / matching `Origin`, `Content-Type: application/json`, `X-Mac-Worker-Task: 1`, body ≤ 8192 bytes. JSON:

```json
{
  "message": "optional for reply",
  "expected_task_id": "<canonical>",
  "expected_turn_id": "<last turn>",
  "expected_turn_count": 1,
  "expected_head_oid": "<40 hex or null>",
  "expected_updated_at_millis": 123,
  "expected_state": "open"
}
```

Public CLI `say` / `close` do not take these fields. Snapshot GET JSON remains at `GET /api/v1/snapshot`, `GET /api/v1/tasks/<id>`, and `GET /api/v1/tasks/<id>/turns/<turn>/logs`.

The interface is a React application in `ui/`, built with Tailwind and shadcn/ui and embedded into the binary at build time, so `cargo build` needs no JavaScript toolchain.

### Herdr

If you run [herdr](https://herdr.dev) on the workers and on your laptop, the pool can show its turns there.

- **Sidebar rows on the worker's herdr.** A worker with `herdr = true` in `config.toml` opens a `mac-worker` workspace in its own herdr and one tab per running turn, labelled `task <id> · turn <n>`. `worker task close` removes the task tab; when it was the last task tab and no operator tab remains, the reporter removes the workspace too. The row carries the task title, the agent's icon, and the turn's state: `working` while the agent runs, `blocked` when it needs input, `done` when it finished, `unknown` with the reason when it failed, was cancelled, timed out, or was lost. Herdr 0.9's machine link and the herdr-mirror plugin both bring those rows to your laptop next to your local agents. A follow-up turn replaces the tab.
- **A readable log in the pane.** The tab's pane runs `worker host follow-turn`, a read-only command that renders the turn's event stream the way `worker task logs -f` does and prints the outcome line when the turn ends. You cannot type to the agent there: the turn stays headless.
- **Notifications on the laptop.** With `[notifications] herdr = true` (the default) the turn runner tells the herdr you started the command in that a turn ended: `task <id>: done` with the `done` sound, `needs_input` and `blocked` with the `request` sound. Without a reachable herdr socket nothing happens.
- **Where to check.** `worker workers` and `worker doctor` print a `herdr:` line per worker (`available (0.9.0)`, `not installed`, `installed, no socket`, `installed, no response`, or `unknown` when facts are missing); a known fact older than the TTL is printed with a `stale` suffix (`available (0.9.0), stale 69m`) rather than `unknown`; `doctor` and `setup` warn with `HERDR_UNAVAILABLE` when a worker has `herdr = true` but its herdr cannot be reached. Turns still run without the reporter, and `worker task status --json` records `herdr: attached`, `unavailable`, or nothing for each turn.
- **Dashboard chip and scheduling.** When the count is known and non-zero, the `herdr:` line from `worker workers` or `worker doctor` ends with `, N interactive agents`. The worker card and capabilities view show the same herdr fact in a chip (`herdr 0.9.0`, `herdr 0.9.0 · 2 agents`, `no herdr`, `herdr: no socket`, …), not counting mac-worker's own reporter tabs. Scheduling uses the count only as a last tie-breaker among otherwise equal workers; it never excludes a worker.

Sidebar tokens `task`, `turn`, `mw_title`, `mw_agent`, and `mw_outcome` are published with every row for custom herdr row layouts. The design and its budgets are in [the herdr reporter design](superpowers/specs/2026-09-08-herdr-reporter-design.md).

## Configuration

`~/.config/mac-worker/config.toml` is written by `worker init` and holds one `[[workers]]` block per Mac. Two optional keys concern herdr, the terminal workspace manager the pool can report into:

```toml
[notifications]
herdr = true      # default true: notify this laptop's herdr when a turn ends; silent without a socket

[[workers]]
name = "mini-1"
ssh = "yourname@mini.local"
slots = 1         # per-worker client ceiling; default 1, max 8; combined cap is the sum
herdr = false     # default false: show this worker's turns in its own herdr sidebar
```

The design behind both keys is in [the herdr reporter design](superpowers/specs/2026-09-08-herdr-reporter-design.md).

## What the pool will and will not do

- A task worktree is isolation for your repository, not a security boundary: agent turns run with the worker account's full access. Only dispatch prompts you trust, on machines you own.
- Your working tree is never modified. Results arrive as remote-tracking refs; merging is your decision.
- You own SDKs, tools, auth, and secrets. Optional `[setup]` does not install arbitrary packages.
- Agent-reported checks are not independent verification. Review summary, diff, and fetch ref before you accept.
- Workers hold a bare mirror per project, a worktree per task, agent sessions, and bounded logs. `worker gc` previews and reclaims them: idle open tasks after 7 days, result branches after 30 days or on `close --discard`.
- The CLI adds no secrets to its own diagnostics, redacts worker paths from agent summaries, and refuses insecure profiles. Application logs can still contain whatever the agent printed.
- `worker task reconcile` repairs task ownership after a laptop reboot. `worker setup` updates helpers; older host layouts may require the steps in [installation recovery](setup-recovery.md).
- The laptop owns the queue in the default (controller-disabled) mode. A remote controller that keeps the queue after laptop disconnect is not in this release.

## Plain remote commands

The task system is built on a simpler layer that is still available: run any trusted, non-interactive command on a worker from a snapshot of the current worktree.

```bash
worker run -- cargo test --locked           # scheduler picks a worker
worker run --worker mini-2 -- npm test      # pin one
worker status                               # recent jobs
worker logs -f <job-id>
worker cancel <job-id>
worker doctor --project .                   # validate a project before its first job
```
