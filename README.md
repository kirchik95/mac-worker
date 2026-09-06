# mac-worker

`mac-worker` is a personal remote-execution tool for dispatching heavy local-development commands from a MacBook to a small pool of trusted Mac mini workers.

## Phase 3: setup, inventory, and one remote job

First provision each host according to the [macOS worker setup guide](docs/setup-macos-worker.md). In particular, the worker alias must support non-interactive SSH with the existing macOS account selected for worker jobs before running setup. A separate worker-only account is optional hardening, not a prerequisite.

```bash
cargo test --all-targets
cargo build --release
mkdir -p ~/.config/mac-worker
cp config.example.toml ~/.config/mac-worker/config.toml
./target/release/worker setup mini-1
./target/release/worker workers
./target/release/worker --json workers | jq .
```

`worker setup mini-1` installs the current helper for that configured worker. `worker workers` lists configured workers and performs bounded read-only SSH health probes; it may report a worker unavailable before setup. Phase 3 required an explicit worker name; Phase 4 adds automatic scheduling when the pin is omitted.

Run trusted, non-interactive batch commands from a Git worktree:

```bash
./target/release/worker run --worker mini-1 -- /usr/bin/printf 'hello\n'
./target/release/worker status
./target/release/worker status <job-id>
./target/release/worker logs -f <job-id>
```

Bare `worker status` lists recent jobs and may report `N older jobs omitted`; `worker status <job-id>` reports one exact job.

The `--` form sends a literal argument vector; use `--shell '...'` only when shell syntax is intentional. Jobs have no interactive PTY or stdin forwarding. Phase 3 is for trusted batch work only: a worker job runs with the access of its configured macOS account.

Before submission, the client captures a verified immutable snapshot rather than uploading the live worktree. The host verifies and promotes that upload before running it from an isolated workspace. Once the host durably accepts a job, it continues if the client or log follower disconnects; reconnect with `status` or `logs` using the original job ID. Source changes made in the remote workspace are never returned to the local worktree.

Human log streaming writes raw application bytes to stdout or stderr. With `--json`, `run` and `logs` emit versioned NDJSON events; log chunks are base64-encoded instead of appearing as raw bytes. mac-worker avoids adding secret values to its own diagnostics, but application logs can contain secrets emitted by the application.

Phase 3 deliberately does not provide automatic scheduling or queueing, cancellation, artifact transfer, package caches, Docker profiles, safe garbage collection, or a dashboard. Any configured artifact collection causes `worker run` to reject the job during preflight with `ARTIFACTS_UNSUPPORTED`, rather than running the command and silently discarding requested outputs.

## Phase 4: automatic scheduling, cancellation, and reconciliation

Use the scheduler with a literal command vector:

```bash
worker run -- npm test
worker run --no-wait -- npm test
worker run --worker mini-2 -- npm test
worker status
worker cancel <job-id>
```

An automatic run is admitted to a compatible configured worker and queues in that worker's FIFO order. `--worker NAME` pins the run to that worker; a pinned run waits only for its named worker and does not block compatible work on another worker. `--no-wait` returns `CAPACITY_BUSY` when no eligible slot is immediately available and does not publish a queue row, snapshot, local job record, lease, or other remote mutation. Cancellation is explicit and targeted: it can cancel a waiting row locally or cancel a running job on its recorded worker. Disconnecting a log follower or pressing Ctrl-C does not cancel an accepted job; reconnect with its original job ID. Source changes made in a remote workspace are never returned to the local worktree.

Scheduler admission uses the local shared observation cache only as advisory input. The remote lease remains authoritative, so a stale or unavailable cache observation cannot free capacity or prove that a worker is idle. The dashboard is Phase 4.5. Artifact transfer/fetch, package caches, Docker profiles, and general garbage collection are later work and are not public Phase 4 commands.

## Validate a project locally

Build the release binary, then run Doctor from either human-readable or JSON-oriented tooling:

```bash
cargo build --release
./target/release/worker doctor --project /path/to/worktree
./target/release/worker --json doctor --project /path/to/worktree | jq .
./target/release/worker doctor --project /path/to/worktree \
  --include 'fixtures/generated/**'
```

Doctor inspects the Git worktree, probes configured workers read-only, creates a unique local snapshot, verifies the selected source a second time, and deletes that exact snapshot before a successful return. It does not upload project data or start a user command. A cleanup failure is an I/O failure, never a ready result.

`UNTRACKED_INPUT` means local inputs are not covered by an explicit policy. Commit them, ignore or remove them when appropriate, or include only the exact file or narrow project-owned subtree needed by the command. Do not use a catch-all include. `SENSITIVE_PATH` means a conventional credential path is selected: remove it from the project input and use a documented example file or separately provisioned worker configuration. If the name is intentionally non-secret, review it and add only that exact relative path to `snapshot.allow_sensitive` in `.worker.toml`; Doctor will emit a content-free warning.

## Local dashboard

`worker dashboard` is a local observer for the configured fleet. It is not a scheduler or a replacement for CLI operations such as `worker run`, `worker status`, `worker logs`, or `worker cancel`.

```bash
worker dashboard
worker dashboard --no-open
worker dashboard --port 9173
```

The dashboard binds `127.0.0.1` only and validates the request `Host` against that loopback listener. It is read-only and ephemeral: it polls rather than pushes updates and keeps no database. API JSON responses use `Cache-Control: no-store`; the server also sends a restrictive CSP, `nosniff`, and `no-referrer` headers and does not enable CORS.

The phase-5e tasks view is a read-only extension of this observer for durable agent tasks and named runs:

- The tasks table shows title, agent, state, worker, runner state, turn count, last outcome, run position, branch, freshness, and age.
- Run, state, worker, and agent filters are local browser filters; they do not make network requests.
- Run cards show top-level and per-run progress. Queue rows retain the scheduler's batch/task-turn kind, pin, run cap, FIFO position, and Phase-4 blocking reason.
- Task detail shows the safe result summary, questions, changed files, diff stat, base/head IDs, turn timeline, runner liveness, and the exact `worker task fetch <task-id>` command.

The snapshot endpoint is `GET /api/v1/snapshot`. Task detail is `GET /api/v1/tasks/<task-id>`, and a turn log is read with `GET /api/v1/tasks/<task-id>/turns/<turn-id>/logs?stream=stdout|stderr&offset=<byte-offset>&limit=<bytes>`. Task IDs and turn IDs must be canonical typed identifiers; the existing legacy `/api/v1/jobs/...` detail and log routes remain separate. Snapshot polling runs every two seconds. A selected active task turn polls stdout and stderr independently every one second with byte cursors and decoder state; each log request is bounded to `1..=65,536` bytes and polling stops at the terminal stream lengths.

`worker task list --json` and the dashboard snapshot use the same `tasks`, `runs`, and `progress` projection. The CLI adds its protocol-version envelope; the snapshot flattens the projection beside its worker, queue, and legacy-job fields.

The dashboard only observes local records, process liveness, cached worker observations, and bounded remote status/log reads. It never starts or recovers a runner, reconciles, cancels, closes, fetches a result, or sends a task message. Prompts and prompt-file contents, environment profiles and values, credentials, session references, complete paths, raw host diagnostics, and unbounded output are not exposed in task rows, queue rows, JSON, detail, or timeline data. Changed files remain repository-relative or become `[path]`; log chunks are bounded base64 bytes and browser content is rendered as text only. Application log content is trusted text for the local operator and may contain application-emitted secrets.

## Phase 5

Phase 5 adds agent tasks: submit a prompt instead of a command, run a headless coding agent on a Mac mini, and collect the result as a Git branch. This release exposes the `worker task` family and `worker workers --refresh`; verify the exact installed grammar with `worker task --help` and the relevant subcommand help before dispatching. The task lifecycle commands are available in this branch, and phase 5e adds the read-only dashboard tasks view described above. The dashboard does not replace the task CLI or add mutation controls.

The orchestrator loop is documented in [`.claude/skills/pool-dispatch/SKILL.md`](.claude/skills/pool-dispatch/SKILL.md). How to write a brief is in [`.claude/skills/pool-task-authoring/SKILL.md`](.claude/skills/pool-task-authoring/SKILL.md). The three-Mac live procedure is [docs/phase-five-acceptance-runbook.md](docs/phase-five-acceptance-runbook.md); the sanitized record template is [docs/phase-five-validation.md](docs/phase-five-validation.md).

Prepare each worker first using the [macOS worker setup guide](docs/setup-macos-worker.md). After a helper that migrates the host layout or collects agent facts for the first time, rerun `worker setup` on every worker. This release moves the protocol to version 5 because the turn material and the task status changed shape, so `worker setup` must be rerun on every worker before it is eligible again.

Task commands:

```text
worker task submit --agent codex --prompt-file tasks/fix-login.md
worker task submit --agent codex --model gpt-5.6-luna --effort max --prompt-file tasks/fix-login.md
worker task batch tasks/sprint.toml --max-parallel 3
worker task list --run <run_id> --json
worker task list --state open --outcome needs-input --json
worker task wait --run <run_id>
worker task say <task_id> --message-file answer.md --wait
worker task result <task_id> --json
worker task fetch <task_id>
worker task close <task_id>
worker task reconcile
worker workers --refresh
```

`worker workers --refresh` recollects agent, profile, and Git-identity facts. `worker task reconcile` re-owns dead runners and re-enqueues orphaned tasks without submitting anything.

`--model` and `--effort` are recorded with the task and reach only the agents that accept them: Codex takes the effort as `-c model_reasoning_effort="<value>"` on the first turn and on resume, while Claude, Cursor, and OpenCode ignore it as they already ignore `--max-turns` and `--max-budget`. An effort value may contain only ASCII letters, digits, `-`, and `_`, and at most 32 bytes.

`worker task list --outcome <kind>` filters by the recorded last outcome (`done`, `needs_input`, `blocked`, `unknown`, `failed`, `cancelled`, `timed_out`, `lost`; the dashed spelling is accepted) and composes with `--state`. `--state open --outcome needs-input` lists the tasks waiting on an answer.

An agent's questions carry the answers it will accept: `worker task status --json` and `worker task result --json` report each question as `{"text": …, "options": [...]}`, and a question with no options stays a bare string. Answer with `worker task say`.

For an individual task, use `worker task wait --task-id <task_id>`; `worker task wait --run <run_id>` waits for every task in a run.

A batch file looks like this:

```toml
version = 1
agent = "codex"
model = "gpt-5.6-luna"
effort = "max"
base = "main"
source = "local"
publish = ["fetch"]
timeout = "45m"

[[tasks]]
title = "Flaky login spec"
prompt_file = "tasks/fix-flaky-login.md"

[[tasks]]
title = "Extract billing client"
prompt = """
Move the billing HTTP client into packages/billing-client …
"""
agent = "codex"
publish = ["fetch", "push"]
```

Top-level keys are defaults; each task may override them. Project defaults live in `.worker.toml`:

```toml
[task]
source = "local"
publish = ["fetch"]
env_profile = "agents"
default_agent = "codex"
model = "gpt-5.6-luna"     # optional; --model wins
effort = "max"             # optional; --effort wins
timeout = "45m"
max_followups = 10

[task.permissions]
codex = "workspace"        # workspace | unattended
claude = "unattended"
cursor = "unattended"
opencode = "unattended"
```

Claude Code is deferred on the workers by operator decision. When it is enabled, put its token in an owner-only profile on each worker (`~/.config/mac-worker/env/agents.env`, mode `0600`) with `CLAUDE_CODE_OAUTH_TOKEN` or `ANTHROPIC_API_KEY`. Codex uses its file-based login and needs no profile. mac-worker never creates, uploads, or prints a profile.

Agent turns run with the worker account's full access: its files, processes, caches, agent configuration, and any credentials that account holds. A task workspace is not a security boundary. Only dispatch trusted prompts.

## Phase 5d: origin publication, Cursor, and OpenCode

Phase 5d extends the task core with origin-backed bases, optional origin publication, Cursor and OpenCode turns, and retention through `worker gc`. Dashboard task views remain phase 5e. Verify the exact installed grammar with the debug-build help before dispatching; the shapes below match `cargo run -q -- task submit --help`, `cargo run -q -- workers --help`, and `cargo run -q -- gc --help`.

```text
worker task submit --agent cursor --env-profile agents --prompt-file tasks/fix-login.md
worker task submit --agent opencode --prompt-file tasks/update-api.md
worker task submit --source origin --base main --agent codex --prompt "…"
worker task submit --publish fetch --publish push --publish-branch feature/api --agent claude --prompt "…"
worker workers --refresh
worker task reconcile
worker gc --apply
```

`worker task submit [OPTIONS]` help prints `--agent <AGENT>`, `--source <SOURCE>`, `--publish <PUBLISH>`, `--publish-branch <PUBLISH_BRANCH>`, and `--env-profile <ENV_PROFILE>`. Accepted `--source` values are `local` and `origin` (default from `.worker.toml`, else `local`). `--publish` is repeatable; accepted values are `fetch` and `push` (default from `.worker.toml`, else `fetch`). `--publish-branch` is valid only with `publish = push`. Accepted `--agent` values are `codex`, `claude`, `cursor`, and `opencode`. Claude Code remains deferred on the workers by operator decision.

A project may set `source = "origin"` and `publish = ["fetch", "push"]` in `.worker.toml` or in a batch file. `source = origin` fails before task creation with `BASE_NOT_ON_ORIGIN` when the exact base is absent from the normalized origin; preparation fails with `BASE_UNAVAILABLE` when the worker cannot fetch or verify it. `publish = push` requires a committed base and the inventory capability `origin:<host>`, reserves a unique run branch, always performs fetch publication, and leaves the task open with `PUBLISH_FAILED` when origin rejects the push. `--wip` with push is `PUBLISH_REQUIRES_COMMITTED_BASE`. A pinned worker that lacks `origin:<host>` is `CAPABILITY_MISSING` and is not rerouted.

As in the execution core, agent turns run with the worker account's full access. Cursor (`--agent cursor`) and OpenCode (`--agent opencode`) use bound sessions and pointer prompts. Cursor prebinds a chat before the first turn and launches with `--force`; OpenCode binds the session from its first JSON event and launches with `--auto`. Resume uses the recorded session reference and fails with `SESSION_UNBOUND` when it is absent. Profile values stay on the worker: they are never copied, logged, or returned through `worker workers`.

Cursor headless turns need `CURSOR_API_KEY` in a secure env profile. If Cursor's login is backed by the macOS login keychain, the keychain must be unlocked in the same headless launch session because Cursor refuses commands while that keychain is locked. On each mini, an operator can add the host-only `MAC_WORKER_KEYCHAIN_PASSWORD` and optional `MAC_WORKER_KEYCHAIN_PATH` to the profile; mac-worker sends the password to `/usr/bin/security` on stdin immediately before Cursor probes, prebinds, and turns. The default path is `$HOME/Library/Keychains/login.keychain-db`.

Provision the profile on each mini yourself, pasting the values directly into the owner-only file:

```sh
umask 077
mkdir -p ~/.config/mac-worker/env
chmod 700 ~/.config/mac-worker/env
$EDITOR ~/.config/mac-worker/env/agents.env
chmod 600 ~/.config/mac-worker/env/agents.env
```

The file may contain `CURSOR_API_KEY=…`, `MAC_WORKER_KEYCHAIN_PASSWORD=…`, and, when needed, `MAC_WORKER_KEYCHAIN_PATH=…`. mac-worker never prints, records, uploads, or exports the two host-only values. Codex and OpenCode do not need keychain unlock variables. OpenCode uses file-based or provider login plus any provider variables that CLI needs in the same profile. OpenCode's worker-local server is loopback-only for the lifetime of that process. See [macOS worker setup](docs/setup-macos-worker.md) for the agent and profile caveats.

Profile files are operator-provisioned owner-only files (`~/.config/mac-worker/env/<name>.env`, mode `0600`). mac-worker never creates, uploads, or prints a profile. An insecure (group- or world-readable) profile is reported as insecure and is never applied; naming it on a turn is `ENV_PROFILE_PERMISSIONS`.

`worker workers --refresh` recollects agent, profile, and Git-identity facts. `worker task reconcile` re-owns dead runners and re-enqueues orphaned tasks without submitting anything.

`worker gc [OPTIONS]` help prints `--apply`. Without `--apply`, `worker gc` previews every candidate with a reason. `worker gc --apply` applies the preview: idle open tasks close after task retention (default seven days) while the result branch is preserved; result branches prune after branch retention (default thirty days) or immediately on `worker task close --discard`; empty mirrors and transfer repositories become candidates only when no task or base ref protects them. `gc` never prunes another task's mirror ref. Native session deletion runs on discard only when the installed adapter exposes it.
