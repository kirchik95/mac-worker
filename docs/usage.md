# Using mac-worker

Start with the [quick start](../README.md#quick-start) to install the CLI and connect one Mac. This reference covers additional agents, task settings, batches, the dashboard and remote commands.

## Agents

| Agent | Flag | Login lives | Notes |
|---|---|---|---|
| Codex | `--agent codex` | file-based login on the worker | `--model gpt-5.6-luna --effort max` are passed through; runs in the Codex workspace sandbox |
| OpenCode | `--agent opencode` | OpenCode auth store on the worker | uses the worker's default model; pick another with `--model opencode-go/<model>` |
| Cursor | `--agent cursor --env-profile agents` | Cursor login on the worker + an env profile | needs the profile described below because Cursor keeps its login in the macOS keychain |
| Claude Code | `--agent claude` | worker login or env profile | check with `worker init <ssh> --agent claude`; add `--env-profile` for profile credentials |

Every agent gets the same contract: work only in the task worktree, do not switch branches or push, and end with a JSON result. Agents that cannot ask questions interactively return `needs_input` with the exact question; you answer with `worker task say <id> --message "…" --wait`, which starts the next turn in the same agent session.

### Env profiles

Some agents need environment variables or a keychain unlock on the worker. Put them in an owner-only file on each worker, never in the repository:

```sh
umask 077
mkdir -p ~/.config/mac-worker/env && chmod 700 ~/.config/mac-worker/env
$EDITOR ~/.config/mac-worker/env/agents.env      # KEY=value per line
chmod 600 ~/.config/mac-worker/env/agents.env
```

Recognised keys include `CURSOR_API_KEY`, and the host-only `MAC_WORKER_KEYCHAIN_PASSWORD` (plus optional `MAC_WORKER_KEYCHAIN_PATH`), which mac-worker feeds to `security unlock-keychain` on stdin right before an agent that needs the login keychain runs. The two keychain values are consumed by the helper and never exported to the agent, logged, or shown anywhere. A profile that is group- or world-readable is refused.

## Task lifecycle

<p align="center">
  <img src="images/task-lifecycle.png" alt="Task lifecycle: submit, queue, turn, publish, fetch, close, with done / needs_input / blocked outcomes" width="100%">
</p>

```text
worker task submit   --agent <a> (--prompt TEXT | --prompt-file PATH) [--wait] [--model M] [--effort E]
worker task batch    tasks.toml [--max-parallel N] [--wait]      # several tasks as one named run
worker task list     [--run ID] [--state open] [--outcome needs-input]
worker task status   <id>          worker task logs <id> [-f]     worker task diff <id> --stat
worker task wait     --task-id <id> | --run <run-id> [--timeout 30m]
worker task say      <id> --message "…" [--wait]                  # answer or steer, next turn
worker task result   <id>          worker task fetch <id>
worker task cancel   <id>          worker task close <id> [--discard]
worker task reconcile              # re-own dead runners, re-queue orphaned turns; submits nothing
worker gc [--apply]                # preview, then reclaim old tasks, branches, mirrors on the workers
```

Outcomes are recorded on the task, independent of the process exit code:

- `done`: the agent finished and the branch is published. Tasks close themselves by default (`--close-on done`).
- `needs_input`: the agent has a bounded question; `say` answers it.
- `blocked`: the agent could not finish. Read `result` and `logs`, then `say` guidance or `close --discard`.
- `unknown`: the agent did not return a structured result; the branch is still published.

A batch file groups independent tasks into a run with shared defaults:

```toml
version = 1
agent = "codex"
model = "gpt-5.6-luna"
timeout = "45m"

[[tasks]]
title = "Flaky login spec"
prompt_file = "tasks/fix-flaky-login.md"

[[tasks]]
title = "Extract billing client"
prompt = "Move the billing HTTP client into packages/billing-client …"
agent = "opencode"
```

Project-wide defaults live in `.worker.toml` next to your code:

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
```

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
```

The dashboard is a loopback-only observer: workers with slot state and host load, the FIFO queue with blocking reasons, active turns with live logs, the task ledger with filters, per-task detail with the outcome, changed files, and the exact `worker task fetch` command, and a run history. It polls, keeps no database, never starts or cancels anything, and never shows prompts, credentials, or profile values. Its Settings view can save native model and effort defaults for future launches; that is its only write. The JSON it renders is available at `GET /api/v1/snapshot`, `GET /api/v1/tasks/<id>`, and `GET /api/v1/tasks/<id>/turns/<turn>/logs`.

The interface is a React application in `ui/`, built with Tailwind and shadcn/ui and embedded into the binary at build time, so `cargo build` needs no JavaScript toolchain.

## What the pool will and will not do

- A task worktree is isolation for your repository, not a security boundary: agent turns run with the worker account's full access. Only dispatch prompts you trust, on machines you own.
- Your working tree is never modified. Results arrive as remote-tracking refs; merging is your decision.
- Workers hold a bare mirror per project, a worktree per task, agent sessions, and bounded logs. `worker gc` previews and reclaims them: idle open tasks after 7 days, result branches after 30 days or on `close --discard`.
- The CLI adds no secrets to its own diagnostics, redacts worker paths from agent summaries, and refuses insecure profiles. Application logs can still contain whatever the agent printed.
- `worker task reconcile` repairs task ownership after a laptop reboot. `worker setup` updates helpers; older host layouts may require the steps in [installation recovery](setup-recovery.md).

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
