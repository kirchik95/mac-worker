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

Each completed turn records the executable resolved **after** login-shell startup and the env profile, plus a bounded `--version` observation. `task status`, `task result`, and dashboard detail JSON expose `turns[].agent_identity`; account-home paths are shown relative to `~/` and secrets remain redacted. The private job `agent-identity.json` retains the resolved path. Version observation is best effort: an unsupported command, a timeout after two seconds (or the remaining turn deadline), or output above 4 KiB per stream does not prevent agent execution. Timeout and overflow retain the path with `version: null` and the fixed `version_observation` reason `timed_out` or `output_limit`; human output shows `version unavailable(timeout)` or `version unavailable(output_too_large)`. Diagnostic-storage errors also do not block launch. The probe's direct child is reaped on a bound, while descendants remain in the supervised process group for turn cleanup. An unresolved executable or a failed exec still fails launch.

`worker workers` and `worker doctor` emit informational `AGENT_VERSION_SKEW` notes when hosts report different versions of the same agent. They do not affect eligibility or scheduling. Facts refresh reads only the global OpenCode `~/.config/opencode/opencode.json` / `.jsonc` autoupdate setting. `AGENT_AUTOUPDATE_ENABLED` means automatic updates are not confirmed disabled there; set `"autoupdate": false` to disable them. OpenCode's `"notify"` mode also disables automatic installation. Missing, unreadable, malformed, or conflicting config produces a note; configuration contents are never emitted. Project/environment/managed overrides may differ from this global observation. No config is edited by mac-worker.

Cursor CLI `2026.09.26-dd393fe` advertises `update` but no disable-auto-update option in `--help`; mac-worker does not infer Cursor's effective update setting from undocumented configuration.


An `unknown` result includes `result_parse_reason`: `no_result_json`, `schema_mismatch:<field>`, `truncated`, or `empty_output`. These fixed codes contain no raw agent output. Truncated protocol still fails publication conservatively and records `truncated` for diagnosis.


### Env profiles

Some agents need environment variables or a keychain unlock on the worker. Put them in an owner-only file on each worker, never in the repository:

```sh
umask 077
mkdir -p ~/.config/mac-worker/env && chmod 700 ~/.config/mac-worker/env
$EDITOR ~/.config/mac-worker/env/agents.env      # KEY=value per line
chmod 600 ~/.config/mac-worker/env/agents.env
```

Recognised keys include `CURSOR_API_KEY`, and the host-only `MAC_WORKER_KEYCHAIN_PASSWORD` (plus optional `MAC_WORKER_KEYCHAIN_PATH`), which mac-worker feeds to `security unlock-keychain` on stdin right before an agent that needs the login keychain runs. The two keychain values are consumed by the helper and never exported to the agent, logged, or shown anywhere. A profile that is group- or world-readable is refused.

A finished turn whose output matches an agent's authentication-failure signature has outcome `agent authentication failed`. `worker workers` then renders that agent as `unknown (auth failed in a turn at <UTC minute>)` until a later turn of the same agent and profile succeeds, 24 hours pass, Codex's `~/.codex/auth.json` is newer than the incident, or you run `worker workers --refresh --clear-auth-incidents`; on the worker, `worker host refresh-facts --clear-auth-incidents` does the same. The private incident record stores only the agent, profile name (or none), fixed reason `auth failed in a turn`, and time. It never stores log content.

A facts refresh also stops when its collection budget runs out and still writes what it finished. Agents it did not finish show `unknown (facts refresh budget exhausted)`. The default budget is 25 seconds, under the 30 second refresh deadline; a shorter caller leaves a few seconds of margin. `worker setup` uses the longer installer allowance.

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

`--max-parallel` is a **requested run cap**. Omitted, it defaults to `sum(worker.slots)` on the machine that owns the queue (each worker defaults to 1). An explicit positive value is accepted even when it is larger than that sum or than current host occupancy; extra tasks wait for a free execution slot. Zero is `TASK_CONFIG_INVALID`. The CLI does not reject “too many” relative to host capacity. On a controller-only laptop config, omit the flag rather than resolving it from an empty `[[workers]]` list; see [Remote controller](#remote-controller).

`worker task logs` without `--raw` prints recognised agent events one line at a time, hides per-token noise, and folds consecutive unrecognised structured events into `event: <type>[/<subtype>] ×N` summaries (the count is omitted for one event). Terminal controls in decoded events, plain stdout, and stderr (ESC, OSC including clipboard and title sequences, CSI, and other C0/C1 controls) are shown as visible text such as `\x1b`; newlines and tabs stay. It prints failure lines such as `turn 1 failed: …` even when the agent wrote nothing. `--raw` stays byte-exact.

`worker task wait` returns only when all selected tasks are quiescent and their runners have released ownership, so `worker task close`, `worker task say`, and `worker task fetch` can run immediately afterward. `wait --run` is not complete while DAG nodes are still waiting or claimed; an empty materialized task list is not completion. After runner recovery, `worker task reconcile` also advances already-frozen eligible DAG nodes; it does not start a new operator batch. Capacity errors such as `CAPACITY_BUSY` and `CAPABILITY_MISSING` retain their public reason and exit code 75 through the controller.

`worker task reconcile` adopts, restarts, or finalizes a row only on positive proof that the previous runner exited: the pid was reused by a different process, or `Absent` was seen twice at least 750 ms apart. A single missed lookup, an ambiguous process-table read, or a transient error is unverifiable — the row is left alone, the report counts it, and `task list` shows `RUNNER_UNVERIFIABLE` only after that state has lasted 30 s. The operator path uses the same rule; it does not treat unverifiable as exited.

Outcomes are recorded on the task, independent of the process exit code:

- `done`: the agent finished and the branch is published. That is **not** human acceptance. Default `--close-on done` then closes the task. Origin `delivery` may still be `pending` / `retrying`. For a human review loop (ready for review → follow-up → accepted), submit with `--close-on never`, then `worker task say` as needed and `worker task close` when you accept.
- `needs_input`: the agent has a bounded question; `say` answers it.
- `blocked`: the agent could not finish. Read `result` and `logs`, then `say` guidance or `close --discard`.
- `unknown`: the agent did not return a structured result; the branch is still published. Attached turn commands and `worker task wait` (including `--run`) exit **70** (`Infrastructure`) for this outcome. A run containing any `unknown` outcome also exits 70. `done` and `needs_input` keep exit 0; waits aggregate other unsuccessful outcomes as exit 1, while attached turns preserve a reported agent exit code.

If a worker job or its logs vanish after acceptance, the task outcome is `failed: LOG_DRAIN_UNAVAILABLE`. `worker task wait --task-id <id>` completes with exit 1, `worker task logs -f <id>` stops, and the dashboard shows the same outcome. A later `worker task say <id> --message "…"` starts a fresh turn. The result may still have been imported before the failure was finalized: inspect `worker task result <id>` and use `worker task fetch <id>` to check or import it.

When a replacement runner exits, its journal line distinguishes whether the worker accepted the turn. `exited: <code> …` is the pre-acceptance form: the worker did not accept that turn, so `worker task reconcile` can retry the handoff. `exited after acceptance: <code> …` means the journal already records acceptance; `worker task reconcile` resumes that turn from its committed offsets instead of submitting it again. After the post-acceptance form, inspect the worker with `worker workers --refresh`, especially if the job or its logs may have disappeared. Both lines are passed through `worker task logs` verbatim.

`worker task result` / `status --json` show the outcome, summary, and any **agent-reported** checks (`reported_checks` on protocol 7). Those checks are claims from the worker agent, not independent verification. `worker task diff <id> --stat` lists the published change. While the task is open, that diff is the workspace, including uncommitted files. After `--close-on done` removes the workspace, `diff` uses the base and result commits retained in the worker project mirror. If those commits have been collected, the error is `RESULT_NOT_RETAINED` (`task workspace is closed and its result is no longer retained`). `worker task fetch <id>` prints the remote-tracking ref (`refs/remotes/mac-worker/<worker>/task/<id>`) — the current-turn import proof on the laptop. Your working tree stays unchanged.

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

`worker task diff <id> --stat` in that loop reads the open workspace. The same command after `worker task close <id>` reads the retained mirror commits described above. `close --discard` removes those commits.

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

Same `task_id` is serialized (`WORKSPACE_BUSY`). Distinct task IDs from one checkout may overlap when that host’s `slot_count >= 2`. Layout, migrate, and probe occupancy: [multiple execution slots](superpowers/specs/2026-09-10-slots-design.md). Extra slots do not make Git/object transfers independent: a worker still serializes a job behind its transfer lock. On a controller, slow object verification and pinning can delay unrelated transfer preparation and finalization. The streaming pack child does not hold the global transfer lock for its lifetime.

`worker task submit` waits for capacity by default. Omitting `--wait` lets the CLI return after admission; it does not disable queueing. Add `--no-wait` to reject a submit when eligible workers are at capacity (`CAPACITY_BUSY`, exit 75). In controller mode an admission rejection with `CAPACITY_BUSY` or `CAPABILITY_MISSING` is saved as a final result: it keeps the public reason and cannot become a delayed task when capacity changes. To try again, submit a new request. Transport failures follow the controller's recovery path and are not capacity rejections.

## Origin delivery

With `publish = push`, each turn pins a durable delivery intent **before** the execution slot is released. Agent outcome and origin state are independent: `done` plus origin `pending` is valid. `status` / dashboard **project** remote delivery without rewriting a Closed local record. Discard while delivery is `pending`/`retrying` is `TASK_BUSY` (`DELIVERY_PENDING`).

Host pump (hidden `worker host`, not in top-level `--help`):

| Flag | Semantics |
|---|---|
| `--watch` | Take `outbox.lock` or exit immediately if another pump holds it. Publish watcher identity, recover the due registry once, then loop. Between pump cycles (at most every ~10 s) the watcher stats its own executable; if the file on disk no longer matches, it logs `outbox watcher exiting: binary replaced` and returns so a KeepAlive LaunchAgent restarts the current binary. |
| `--once` | Blocking one-shot pump. If the pump lock is held, `OUTBOX_BUSY`. Does not spawn `--watch`. |
| `--enable` | Write `locks/outbox-enabled.json`. Does not start a watcher. Reboot recovery is `--enable` plus a LaunchAgent that actually starts `--watch`. |
| `--wake` | Hidden. If a live watcher is already this binary, no-op (`woken`). If it is a replaced image, SIGTERM it, wait up to 15 s for the pump lock, and start `--watch` (`restarted`). If `locks/outbox-enabled.json` is absent the setup path prints `not_enabled` and does not treat that as an error. |

KeepAlive recipe (no `launchctl` from the helper itself):

```sh
worker host outbox --enable
worker host outbox --write-agent ~/Library/LaunchAgents
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.mac-worker.outbox.plist
```

`--write-agent` writes a KeepAlive plist whose `ProgramArguments` pass `--host-root` and whose stdout/stderr go to `<host_root>/logs/outbox.log`. After `worker setup` the worker helper wakes that watcher; the self-check plus KeepAlive keep it on the binary just installed. Setup prints the per-worker outcome under the installed line (`outbox: woken`, `restarted`, `not_enabled`, or `failed OUTBOX_WAKE_FAILED`); a wake failure is a worker warning, not a failed install.

```text
mini-1: installed (protocol 7)
  outbox: restarted
```

Pick one owner for the watcher on a worker. Without a LaunchAgent, `worker setup` (through `--wake`) starts a detached watcher and replaces it after every reinstall; a reboot needs one `worker setup` or `host outbox --wake` to bring it back. With a LaunchAgent loaded, the self-check plus `KeepAlive` already restart the watcher on the new binary, and a detached watcher started by `--wake` would hold the pump lock while launchd keeps relaunching its own instance every throttle interval; do not combine the two until `--wake` is launchd-aware.

`--once` is a one-shot pump, not a substitute for `--watch`. Do not treat a single `--once` after a crash as proof that due-registry recovery already ran; `--watch` recovers the due index on start. Host layout of intents, pins, and the due registry: [durable origin outbox](superpowers/specs/2026-09-10-origin-outbox.md).

After a delivery reaches terminal `failed` (12 attempts, typically while origin credentials were broken), the watcher ignores it. Repair the worker login, then `worker task publish-retry <task_id>` resets that task's failed or retrying intents to `retrying` with attempt 0, records `retry_requested_at_millis`, and wakes the pump. The host's returned deliveries are merged into the laptop task record, so `worker task status`/`result`/`list` show them even when the task is already Closed and is no longer re-observed from the worker. If a concurrent local write wins the record after the host accepted the retry, the command still succeeds and prints a warning. Delivered or superseded intents are refused (`DELIVERY_ALREADY_DELIVERED`). An older helper that does not know `host outbox-retry` returns `HOST_COMMAND_UNSUPPORTED`. The task itself can already be Closed; this command does not reopen it.

Project defaults:

```toml
[task]
default_agent = "codex"
source = "local"            # or "origin": start from the exact commit on your Git remote
publish = ["fetch"]         # add "push" to also push task/<id> to origin
timeout = "45m"
max_followups = 10
```

Permissions default to `workspace` for Codex and `unattended` for Claude, Cursor, and OpenCode. A partial `[task.permissions]` map keeps those defaults for omitted agents. Codex is the only agent with a workspace sandbox. Requesting `workspace` for Claude, Cursor, or OpenCode is `TASK_CONFIG_INVALID`: those adapters have no sandbox, so the requested mode would otherwise launch full unattended access. Set that agent to `unattended`, or opt in explicitly:

```toml
[task.permissions]
claude = "workspace"

[task.permission_fallback]
claude = true
```

An opt-in keeps the requested permission as `workspace` and records the effective permission as `unattended` on the task and the turn, with a warning on `worker task status` and `worker task result`.

`.worker.toml` `[task]` has no `close_on` field. Set close policy with `--close-on` on submit, or `close_on` at the batch top level or on a `[[tasks]]` entry (`done` or `never`).

To use the exact base commit from your Git remote and push the result branch back to that remote:

```toml
[task]
source = "origin"
publish = ["fetch", "push"]
```

The base commit must already be on the remote. The worker account needs its own Git access to that remote; SSH agent forwarding from the laptop is disabled. `--wip` cannot push (`PUBLISH_REQUIRES_COMMITTED_BASE`).

### Origin URL and worker credentials

The origin URL is the project's `origin` remote. mac-worker does not take a separate origin URL.

Declare the host on each worker that should fetch from or push to that remote:

```toml
[[workers]]
name = "mini-1"
ssh = "yourname@mini.local"
slots = 1
capabilities = ["darwin-arm64", "origin:github.com"]
```

`source = origin` and `publish = push` require `origin:<host>` where `<host>` matches the remote (`github.com`, `gitlab.example.com`). A pinned worker without it fails with `CAPABILITY_MISSING`. See [Prepare a Mac worker](setup-macos-worker.md#origin-remotes-optional) for the worker-side Git login.

Git on the worker stays hermetic during origin operations (the account `~/.gitconfig` is not loaded into the push). For **HTTPS** origins the worker account needs a credential helper: `gh auth login` then `gh auth setup-git` on the worker. For **SSH** origins the worker's own key must be registered with the remote.

`worker workers` prints `origin:github.com: helper configured` or `helper missing`. `worker doctor` warns `ORIGIN_HELPER_MISSING` when a declared origin has no HTTPS helper. A push git rejects as unauthenticated is `ORIGIN_AUTH_FAILED` (not the generic `PUBLISH_FAILED`) and keeps retrying with the existing backoff.

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

Optional `[setup]` runs in the task workspace with the same account and env-profile as the agent, before the agent starts. The recipe and its digest are frozen from the original submit commit (or operator WIP snapshot); agent edits to `.worker.toml` cannot change it. Absent `[setup]` runs no setup. Older tasks without a frozen recipe also skip setup; submit a new task if setup is needed.

Declare every file that setup or its check executes or consumes in `lockfiles` or `inputs`, including `package.json` and local lifecycle scripts when using npm. Prefer `npm ci --ignore-scripts` if lifecycle scripts are unnecessary. A frozen command alone does not make undeclared, agent-modified scripts safe. Before any setup command **or check**, declared inputs must still match the approved snapshot. A change, deletion, or unsafe file yields `SETUP_INPUTS_CHANGED`: review the changes and submit a new task with the reviewed commit/WIP to approve them. Setup should not rewrite its declared inputs.

For unchanged inputs the per-task receipt cache behaves as before: `check` proves **this** workspace is ready, and a failed check repairs it using the approved commands. A receipt in another worktree is not skip proof. Toolchains stay user-owned.

`worker task batch FILE --preview` resolves agent/model/worker, declared files, acceptance, and setup without creating tasks, opening client state, or talking to workers. Preview reports `dag.status = "enforced"` (`Dependencies execute when parents are Closed and Done.`). Submit of `depends_on` or `base = "from:<id>"` executes that graph; independent batches stay on today's path. Declared `files` are advisory overlap hints. Declared `acceptance` is copied into the agent prompt as instructions, not proven by mac-worker.

To use the exact base commit from your Git remote and push the result branch back to that remote, change these project settings:

```toml
[task]
source = "origin"
publish = ["fetch", "push"]
```

The base commit must already be on the remote. The worker account needs its own Git access to that remote; SSH agent forwarding from the laptop is disabled.

Writing good briefs is its own skill. Two Claude Code skills ship with the repository and work from any project: [`pool-task-authoring`](../.claude/skills/pool-task-authoring/SKILL.md) turns an objective into one-turn, headless-safe tasks, and [`pool-dispatch`](../.claude/skills/pool-dispatch/SKILL.md) is the mechanical submit / wait / answer / fetch loop. Say "send it to the pool" and Claude Code uses them.

## When a turn fails

A turn can fail instead of returning `done`, `needs_input`, or `blocked`.
The outcome or error code is a fixed public string. Start with the command
listed for that string.

- `agent exited N`: read `worker task logs <id>`.
- `agent authentication failed`: re-login on the worker, then run
  `worker workers --refresh --clear-auth-incidents`.
- `LOG_DRAIN_UNAVAILABLE`: check `worker task result <id>`; the worker's
  job is gone and remaining stdout/stderr cannot be drained.
- `CAPACITY_BUSY` on a pinned submit: wait, or choose another worker.
- `TASK_BUSY` on close: wait for `worker task wait` to return, then retry.
- `ORIGIN_AUTH_FAILED`: the worker could not authenticate to origin. Check
  `worker workers` for `helper missing`, then on the worker run
  `gh auth login` and `gh auth setup-git` (HTTPS) or register the worker's
  SSH key. Delivery keeps retrying until 12 attempts; after `failed`, run
  `worker task publish-retry <task_id>` instead of resubmitting the task.

`worker task wait` is also the gate before `say` and `fetch` after a busy
turn.

A host I/O failure may include `HOST_IO (stage=…, residual=…)` on that job.
`stage` is one of `admission`, `prepare`, `launch`, `drain`, `publish`,
`cleanup`, `lease-release`, `cancel`, `follow`. `residual` lists leftovers of
**this job only** (`lease`, `cleanup-tree`, `job-dir`, `supervisor-lock`,
`transfer-lock`, `session`, `workspace`). Observation errors omit a residual
rather than guessing it is still there. This is diagnostic text on the existing
`HOST_IO` string, not a new CLI verb.

## Dashboard

<p align="center">
  <img src="images/dashboard.png" alt="The dashboard: three machine cards, queue, active work, and the task ledger" width="100%">
</p>

```bash
worker dashboard                 # opens http://127.0.0.1:<port>
worker dashboard --port 8765 --no-open
worker dashboard --no-facts-refresh   # skip stale agent-facts refresh only
```

The dashboard listens only on loopback. It does not start or cancel tasks. In the default (controller-disabled) mode it serves the laptop queue. When `[controller] enabled = true`, the same command does not open laptop task state: it starts a managed SSH local-forward to the controller host’s loopback dashboard, waits until that URL answers, prints `http://127.0.0.1:<port>`, and holds the tunnel until you stop the command. `--port`, `--no-open`, and `--no-facts-refresh` still apply. Tunnel or transport failure is `CONTROLLER_UNAVAILABLE` with no laptop-store fallback.

It shows workers (including slot occupancy and host load), the FIFO queue, active turns, the task ledger, per-task detail (outcome, summary, agent-reported checks, changed files, fetch ref, delivery), and run history. Deep link `#/tasks/<id>` selects that card. It never shows prompts, credentials, or profile values.

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

- **Sidebar rows on the worker's herdr.** A worker with `herdr = true` in `config.toml` opens a `mac-worker` workspace in its own herdr and one tab per running turn, labelled `task <id> · turn <n>`. `worker task close` and garbage collection update the task record first, then ask herdr to remove the task tab. A herdr that accepts and does not answer is abandoned after the 5 second close budget, without holding the host installation or session lock; leftover tabs are removed on a later pass. `--close-on done` auto-close releases the pane the same way. When it was the last task tab and no operator tab remains, the reporter removes the workspace too. The row carries the task title, the agent's icon, and the turn's state: `working` while the agent runs, `blocked` when it needs input, `done` when it finished, `unknown` with the reason when it failed, was cancelled, timed out, or was lost. Herdr 0.9's machine link and the herdr-mirror plugin both bring those rows to your laptop next to your local agents. A follow-up turn replaces the tab.
- **A readable log in the pane.** The tab's pane runs `worker host follow-turn`, a read-only command that renders the turn's event stream the way `worker task logs -f` does and prints the outcome line when the turn ends. You cannot type to the agent there: the turn stays headless.
- **Notifications on the laptop.** With `[notifications] herdr = true` (the default) the turn runner tells the herdr you started the command in that a turn ended: `task <id>: done` with the `done` sound, `needs_input` and `blocked` with the `request` sound. Without a reachable herdr socket nothing happens.
- **Where to check.** `worker workers` and `worker doctor` print a `herdr:` line per worker (`available (0.9.0)`, `not installed`, `installed, no socket`, `installed, no response`, or `unknown` when facts are missing); a known fact older than the TTL is printed with a `stale` suffix (`available (0.9.0), stale 69m`) rather than `unknown`; `doctor` and `setup` warn with `HERDR_UNAVAILABLE` when a worker has `herdr = true` but its herdr cannot be reached. Turns still run without the reporter, and `worker task status --json` records `herdr: attached`, `unavailable`, or nothing for each turn.
- **Dashboard chip and scheduling.** When the count is known and non-zero, the `herdr:` line from `worker workers` or `worker doctor` ends with `, N interactive agents`. The worker card and capabilities view show the same herdr fact in a chip (`herdr 0.9.0`, `herdr 0.9.0 · 2 agents`, `no herdr`, `herdr: no socket`, …), not counting mac-worker's own reporter tabs. Scheduling uses the count only as a last tie-breaker among otherwise equal workers; it never excludes a worker.

Sidebar tokens `task`, `turn`, `mw_title`, `mw_agent`, and `mw_outcome` are published with every row for custom herdr row layouts. The design and its budgets are in [the herdr reporter design](superpowers/specs/2026-09-08-herdr-reporter-design.md).

## Configuration

`~/.config/mac-worker/config.toml` is written by `worker init` and holds one `[[workers]]` block per Mac in the default laptop-owned mode. A controller-only laptop config may omit `[[workers]]` (see [Remote controller](#remote-controller)). Two optional keys concern herdr, the terminal workspace manager the pool can report into:

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

Optional remote controller (off unless you set it; see [Remote controller](#remote-controller)):

```toml
[controller]
enabled = false
# ssh = "yourname@always-on-host"
# remote_binary = "~/.local/bin/worker"   # only this path is accepted
```

## SSH

mac-worker opens a fresh SSH connection for each remote call unless multiplexing is enabled.

```toml
[ssh]
multiplex = false
```

Set `multiplex = true` on a machine that makes many SSH calls, especially the controller host. mac-worker then passes `ControlMaster=auto`, a `ControlPath` under `~/.cache/mac-worker/ssh/` (mode `0700`, `%C`), `ControlPersist=60`, `ServerAliveInterval=10`, and `ServerAliveCountMax=3`. If that directory or a control socket cannot be used, the command connects directly instead, so one stuck master does not block later commands. The default is off so existing SSH behavior stays the same until you opt in.

## Remote controller

Opt-in. Default remains the laptop-owned queue: omit `[controller]`, or keep `enabled = false`. Confirm flags with `worker controller --help` and `worker dashboard --help`. Do not treat a missing table as a second store.

Use this when an always-on Mac should keep the queue after the laptop sleeps or disconnects. The laptop still freezes the prompt and Git objects. Execution workers stay ordinary `host` helpers. There is one task store — on the controller host — not a laptop copy that silently takes over.

### Enable

Run from the laptop with a worker inventory that includes the controller host:

```bash
worker setup
worker controller init mac1 --worker-ssh mini-2=kirchik@10.0.0.2
worker controller status
```

`init` requires the controller helper's SHA-256 to match the running laptop CLI. Run `worker setup` for the whole inventory first: every worker needs the new helper operation that authorizes the controller key. Init resolves the inventory with laptop `ssh -G`, removes a first ProxyJump through the controller, and maps the controller's own worker to loopback. Generated `mac-worker-controller-<sanitized>-<hash8>` SSH aliases preserve each resolved user and port; worker names stay unchanged. The label replaces unsafe bytes with `_`, keeps its first 32 bytes, and adds eight SHA-256 hex digits of the full name. Init refuses any remaining alias collision. Repeat `--worker-ssh NAME=DESTINATION` for explicit controller-reachable overrides. Remaining jump chains require a direct override; they are never copied silently from the laptop.

The controller receives `~/.config/mac-worker/config.toml`, a dedicated `~/.ssh/mac-worker-controller_ed25519` key, managed SSH settings, and laptop-trusted host keys in per-worker `~/.ssh/<alias>.known_hosts` files (with a combined `mac-worker-controller_known_hosts` copy). Only public keys returned by laptop `ssh-keygen -F` are seeded; missing or revoked trust stops setup for that worker. Strict host-key checking stays enabled, including loopback; the verification name is pinned explicitly and each worker has its own trusted-key file. The controller helper records the absolute managed config path in `[ssh] config_file`. Every controller-to-worker SSH connection, including Git and rsync, uses `-F` with that file, bypassing the account and system SSH configs. Init does not edit `~/.ssh/config`. The managed file disables multiplexing; opting into `[ssh] multiplex = true` uses a separate private control directory named for a hash of the managed config path. Origin Git connections keep the account's SSH settings and shared control directory. To debug a worker alias on the controller, run `ssh -F ~/.ssh/mac-worker-controller.conf <alias>` (add `-G` to inspect settings without connecting). Authorization appends one recognizable `mac-worker-controller` line, preserves all existing restrictions/keys, and is idempotent. Existing controller private keys are retained.

Rerun `init` after correcting access or inventory. Identical controller configuration is kept; a different existing config produces a diff and requires `--force`. Init installs and restarts the service, probes every worker from the controller, and checks a live leader through RPC before enabling laptop controller mode. It prints a per-worker result. Partial initialization is recoverable by rerunning the command. The laptop keeps its `[[workers]]`, so `setup`, `doctor`, `workers`, and `gc` remain usable there.

The resulting laptop settings are:

```toml
[controller]
enabled = true
ssh = "mac1"
remote_binary = "~/.local/bin/worker"
```

The controller host's own config has controller mode disabled and contains its dispatch inventory. `remote_binary` must be exactly `~/.local/bin/worker`. Task commands use authenticated `host controller-rpc`; transport failure never silently switches to a laptop task queue. RPC can persist requests even while the leader is stopped.

The service is `~/Library/LaunchAgents/com.mac-worker.controller.plist` in `gui/<uid>`, with RunAtLoad, KeepAlive, and a 30-second restart throttle. It runs `worker controller run --supervised`; stdout and stderr share `~/Library/Logs/mac-worker/controller.log`. Each supervised process start truncates that log in place, including lock-contention restarts. Long uninterrupted runs retain their current log until the next start. `worker setup` restarts an enabled configured controller after installing its helper and reports `controller: restarted`; restart failures retain the installed helper and produce `CONTROLLER_RESTART_FAILED`.

A LaunchAgent **requires a logged-in GUI session after reboot**. No auto-login means it will not start before login. Successful init prints the complete, account-specific `sudo` commands and equivalent LaunchDaemon plist (including `UserName=kirchik` for the pilot) for operators who want boot startup. These commands are instructions only and are never executed by init. Review and run them on the controller; they unload the agent before loading the daemon so there is only one leader. For automatic recovery after power loss, also consider running `sudo pmset -a autorestart 1` on that host. Never-sleep settings alone do not enable boot or power-loss recovery. The init/disable/setup service commands manage the LaunchAgent; operators choosing the printed LaunchDaemon alternative manage its system-domain lifecycle themselves.

For manual foreground operation, stop/unload the service first, then run `worker controller run` on the controller. It holds the leader lock, resumes the same durable store, and prints `controller leader acquired`; a second leader gets `CONTROLLER_LOCK_HELD`.

These stay on the laptop in controller mode: `init`, `setup`, `doctor`, `workers`, `gc`, `run`, job status/logs/cancel, and `worker task batch FILE --preview`. Controller-only hand-written laptop configs may omit workers, but inventory-based commands still require them. Turn runners run on the controller host.

### Health and shutdown

Use `worker controller status` or `worker controller status --json` on either host.
With laptop controller mode enabled, it reads the remote health, LaunchAgent state, and persisted
`drained` flag. On the controller it reads local state. Service lookup failure is shown as unknown,
separately from the leader's process identity. This is a read-only diagnostic. The JSON includes the leader PID and process start time,
binary version, tick start/end/duration, last success and progress, failure counts by public code,
active request count and oldest pending age, and scheduling truncation/cursor flags. Unknown
pending ages are marked incomplete. Counts cover the current leader invocation; an idle successful
tick updates success without inventing progress.

The leader writes owner-only `health.json` in the controller state directory using atomic replacement.
The record is capped at 16 KiB and 32 failure-code buckets (overflow uses `PROTOCOL`); it contains
no request bodies or raw error messages. Failed ticks also produce a code-only stderr line at most
once per 30 seconds, with cumulative counts so repeated failures stay visible. Ticks run with a
2-second pause between them. A stopped or absent leader, reused PID, or record older than 10 seconds
is **stale**. An unverifiable process identity is **unknown**, never proof that its work exited.
A long-running tick becomes stale until it finishes; inspect its duration and last progress.

On the **laptop**, `worker doctor [--json]` includes controller health over the existing authenticated
controller RPC. An older controller produces an upgrade/restart message. A controller-only laptop
needs no local worker inventory for this check: doctor checks its project/snapshot and controller
health; execution-worker capacity still belongs to the controller host. Local inventory checks remain
available when the laptop config includes workers. Stale/unknown/unavailable health blocks doctor
readiness; reported tick failures are warnings and remain visible in the controller section.

Ctrl-C or SIGTERM stops scheduling ticks, cancels interruptible Git/SSH subprocess I/O, and joins the
active tick before writing the stopped record and releasing the leader lock. Filesystem operations
and code without an interruption hook must finish before that join completes.

Pause new runner handoffs with `worker controller drain`; resume with `worker controller drain --off`.
The flag survives leader restarts. Requests continue to be accepted and persisted while drained;
already-running turns finish. Local attached `task submit --wait` and `task say --wait` keep their
turn queued and wait for drain to lift, then retry admission. A wait timeout leaves the turn queued
for recovery. The shared launch gate covers ordinary, recovery, replacement, and completion-triggered
runners, plus reassignment to parked turns. `status` reports `drained`; unreadable drain state fails closed.

`worker controller disable` unloads the remote LaunchAgent and sets laptop `enabled = false`.
It preserves both inventories, task state, keys, and trusted hosts. Finish controller work before
switching back: local mode does not import the controller's task store. Rerun init to re-enable it.

`worker task publish-retry TASK_ID` is routed to the controller, which retries the worker outbox
and persists refreshed delivery state. `worker task result` includes the same effective-permission
warnings as local task results.

### Submit, disconnect, reconnect

Task commands use the same public grammar as today (`worker task --help`): `submit`, `batch`, `list`, `status`, `logs` (`-f` / `--raw` / `--turn`), `diff`, `say`, `cancel`, `result`, `fetch`, `close`, `wait`, `reconcile`. Confirm the installed form with `worker skills get pool-dispatch --grammar-only` rather than copying flags from a skill file.

On submit the laptop freezes the prompt, project identity, settings, and base (`HEAD`, `--base`, or `--wip` / `--include`) and transfers that snapshot before the controller accepts the request. A retry of the **same original envelope** keeps that freeze; it does not recapture a later HEAD or `.worker.toml`. After accept you can close the laptop CLI. That ACK means the request is persisted on the controller store; it does **not** mean a runner or the agent has started — enabled submit can stay queued until `worker controller run` advances it. Reconnect with `status`, `logs`, `wait`, `list`, and the dashboard.

Without `--wait`, `submit` / `batch` / `say` return after that accept. Autonomous progress on the controller still needs `worker controller run`. With `--wait`, the same command follows until the selected task or run is quiescent. `worker task wait --task-id` waits for one task; `worker task wait --run` waits until that run is quiescent, including DAG children that become eligible after a parent `close`.

`--wip` is still opt-in local snapshot with fetch-only publication. Origin source still needs the exact remote commit. `publish = push` still cannot use a WIP base (`PUBLISH_REQUIRES_COMMITTED_BASE`).

### Import is not fetch

The controller imports the worker’s published result so later DAG work can proceed while the laptop is away. That import is not your working copy. `worker task fetch <id>` is the explicit laptop materialization: it prints the remote-tracking ref for you to inspect. A missing laptop branch is not an agent failure.

### Batches and slots

`worker task batch FILE --preview` stays local: it validates the file and does not open the controller store or dispatch. It works with a controller-only laptop configuration, including worker pins. The preview preserves those names; the controller checks its own inventory when you submit. A successful preview does not prove that a worker exists or is currently available.

Named `depends_on` / `base = "from:<id>"` still wait for each parent to be **Closed and Done** (including `close_on = never`, which needs human `close` before a `from:` child may run). Open+NeedsInput and Open+Done wait. Failed, Abandoned, Lost, or Closed without Done block descendants (`DAG_PARENT_FAILED`). `from:` binds that parent’s accepted **controller** import, not origin `pending` and not a laptop `fetch` you have not run.

`--max-parallel` remains CLI-only (not a batch-file key). Omitted, the default is the **controller host** `sum(worker.slots)`, not an empty laptop `[[workers]]` list. An explicit positive value is accepted even when it is larger than that sum; extra tasks wait. Zero is `TASK_CONFIG_INVALID`. The CLI does not reject “too many” relative to host capacity. Host occupancy is still each Mac’s `slot_count`.

Controller Git object transfer uses one global lock for object verification and pinning. A slow repository can delay unrelated transfer preparation and finalization. Extra slots do not make those steps independent. The streaming pack child does not hold that lock for its lifetime.

### Dashboard

`worker dashboard [--port N] [--no-open] [--no-facts-refresh]` on an enabled laptop is the tunnel described in [Dashboard](#dashboard). Reply and accept still require the current card against the store the dashboard is serving.

Protocol 7 stays the version: a host may add fields to facts and probe JSON; a laptop must not reject them. Probe responses may include optional `build_id` and `binary_sha256`. `worker workers` and `worker doctor` print each host's build. When that SHA-256 differs from the laptop binary they warn `BUILD_MISMATCH` with the host name, the host build, and the laptop build; `--json` repeats that warning as `build_warnings`. A helper that omits the fields is `build unknown (older helper)`, not an error. The host still rejects unknown keys in its own `facts.json` and in requests it validates. After updating the CLI, restart a running dashboard or `worker controller run` — already-running processes keep the old code after a binary replacement. A still-running dashboard that started this build compares the file it launched from with the installed binary and sets additive `laptop.binary_outdated = true` so the UI can show `worker was updated, restart the dashboard`. `worker setup` and `worker doctor` warn when they find those processes. When a process's build id or SHA-256 is known, that comparison is used instead of the binary's modification time. See [Update or remove](getting-started.md#update-or-remove).

### Close request recovery

A saved `task.close` request whose target has changed is rejected before any close action with `TASK_REVISION_CONFLICT` or `TASK_CLOSED`. The controller saves that rejection and removes the request from the active retry index. Replaying the same request returns the saved rejection; it does not close a newer turn. Existing stale requests are settled when the updated controller next processes them, including through its recovery tick. Their journal records remain available for diagnosis.

Errors from an already-started close, including transport failures after retaining the close intent, remain retryable. A repeated close of the same completed target is still idempotent. Wait for `worker task wait --task-id <id>` before closing an active task.

## What the pool will and will not do

- A task worktree is isolation for your repository, not a security boundary: agent turns run with the worker account's full access. Only dispatch prompts you trust, on machines you own.
- Your working tree is never modified. Results arrive as remote-tracking refs; merging is your decision.
- You own SDKs, tools, auth, and secrets. Optional `[setup]` does not install arbitrary packages.
- Agent-reported checks are not independent verification. Review summary, diff, and fetch ref before you accept.
- Workers hold a bare mirror per project, a worktree per task, agent sessions, and bounded logs. `worker gc` previews and reclaims them: idle open tasks after 7 days, result branches after 30 days or on `close --discard`. Unreachable objects in a mirror stay for two weeks, so objects another task is still writing remain available. Each host is reported as success, error, or unknown when its control request times out. The report includes every host, and the command exits non-zero after that report when any host is an error or unknown.
- The CLI adds no secrets to its own diagnostics, redacts worker paths from agent summaries, and refuses insecure profiles. Application logs can still contain whatever the agent printed.
- `worker task reconcile` repairs task ownership after a laptop reboot (or on the controller host when enabled). It waits 750 ms to confirm an `Absent` owner in that same invocation; a still-unverifiable owner is not treated as dead. `worker setup` updates helpers; older host layouts may require the steps in [installation recovery](setup-recovery.md).
- The laptop owns the queue unless you opt in to a remote controller (`[controller] enabled = true`). That mode is off by default. Setup: [Remote controller](#remote-controller).

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

## Exit codes and errors

The process exit status is one of the categories below. Codes in the laptop's catalog keep the same status and hint locally, over SSH, and through the controller, whether or not the helper sends a category. A hint is a fixed next step: it never includes a path or text taken from a remote message. For an unknown code, a valid category from a future helper determines the status; without one, the previous mapping applies. Current helpers omit the category field to remain compatible with older laptops.

JSON error events keep a plain public `message`. Human-readable diagnostics on stderr put the hint on a separate line.

| Exit | Meaning |
|---|---|
| 0 | The command finished |
| 1 | The agent's own status, including `AGENT_LIMIT_REACHED` |
| 64 | Usage: the command or configuration needs a change |
| 69 | Unavailable: SSH or the controller could not be reached, or a Git transfer can be retried |
| 70 | Infrastructure: the worker, protocol, or wait failed |
| 74 | I/O: a local read or write failed |
| 75 | Capacity: a slot, resource, or agent login is not available |

When `worker run` finishes, the process status is the remote command's own exit code.

<!-- error-catalog:start -->
| Code | Exit | Hint |
|---|---|---|
| `CONFIG_MISSING` | 64 | connect your first Mac with `worker init user@mini.local` (keep --config if you use a custom path) |
| `CONFIG` | 64 | check the configuration syntax, worker names, and SSH destinations |
| `TASK_BUSY` | 64 | wait for `worker task wait` to finish, then retry |
| `TASK_CLOSED` | 64 | start a new task; this one is already closed |
| `TASK_NOT_FOUND` | 64 | check the id with `worker task list` |
| `TASK_CONFIG_INVALID` | 64 | fix the task options and submit again |
| `FOLLOWUP_LIMIT` | 64 | close the task, or submit a new one with a higher follow-up limit |
| `TASK_REVISION_CONFLICT` | 64 | refresh the task status and retry the close |
| `RESULT_NOT_RETAINED` | 64 | the closed workspace is no longer retained |
| `AGENT_UNSUPPORTED` | 64 | choose codex, cursor, opencode, or claude |
| `NOT_A_WORKTREE` | 64 | run the command inside a Git worktree |
| `SSH_UNAVAILABLE` | 69 | check SSH to the worker and retry |
| `CONTROLLER_UNAVAILABLE` | 69 | check the controller host with `worker controller status` |
| `BASE_PUSH_FAILED` | 69 | retry; the worker did not receive the base commit |
| `RESULT_FETCH_FAILED` | 69 | retry the fetch; the result is still on the worker |
| `WAIT_TIMEOUT` | 70 | the wait timed out; the task is still running |
| `WAIT_BLOCKED` | 70 | inspect `worker task status` for the blocked turn |
| `HOST_LAYOUT_OUTDATED` | 70 | run `worker setup` to update the helper |
| `PUBLISH_FAILED` | 70 | retry publishing; the result is still on the worker |
| `BASE_UNAVAILABLE` | 70 | choose a base commit that exists in the worktree |
| `RUNNER_HANDOFF_FAILED` | 74 | retry; the local runner handoff failed |
| `IO` | 74 | retry the command |
| `CAPACITY_BUSY` | 75 | wait for a free heavy slot, or choose another worker |
| `CAPABILITY_MISSING` | 75 | install the missing capability or pin a worker that has it |
| `INSUFFICIENT_DISK` | 75 | free disk space on the worker and retry |
| `MEMORY_PRESSURE` | 75 | wait until the worker has free memory and retry |
| `SWAP_LIMIT` | 75 | wait until swap pressure drops and retry |
| `AGENT_NOT_INSTALLED` | 75 | install the agent on the worker account |
| `AGENT_NOT_AUTHENTICATED` | 75 | finish the agent's headless login on the worker |
| `AGENT_LIMIT_REACHED` | 1 | raise the turn or budget limit and submit again |
<!-- error-catalog:end -->
