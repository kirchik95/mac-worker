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

`worker setup mini-1` installs the current helper for that configured worker. `worker workers` lists configured workers and performs bounded read-only SSH health probes; it may report a worker unavailable before setup. In Phase 3, `worker run` requires one of those worker names explicitly.

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

## Phase 4 and Phase 5

Phase 4 adds automatic scheduling and queueing across the configured workers, cancellation, and a local loopback dashboard. `worker dashboard` is available today. Automatic worker selection, the FIFO queue, and cancellation are planned and are not in the current CLI.

Phase 5 adds agent tasks: submit a prompt instead of a command, run a headless coding agent on a Mac mini, and collect the result as a Git branch. The following are planned and are not in the current CLI: `worker task …` (including `worker task reconcile`) and `worker workers --refresh`. Prepare each worker first using the [macOS worker setup guide](docs/setup-macos-worker.md). After a helper that migrates the host layout or collects agent facts for the first time, rerun `worker setup` on every worker.

When those commands exist, a batch file looks like this:

```toml
version = 1
agent = "codex"
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
agent = "claude"
publish = ["fetch", "push"]
publish_branch = "feat/billing-client"
```

Top-level keys are defaults; each task may override them. Project defaults live in `.worker.toml`:

```toml
[task]
source = "local"
publish = ["fetch"]
env_profile = "agents"
default_agent = "codex"
timeout = "45m"
max_followups = 10

[task.permissions]
codex = "workspace"        # workspace | unattended
claude = "unattended"
cursor = "unattended"
opencode = "unattended"
```

Claude Code is deferred on the workers by operator decision. When it is enabled, put its token in an owner-only profile on each worker (`~/.config/mac-worker/env/agents.env`, mode `0600`) with `CLAUDE_CODE_OAUTH_TOKEN` or `ANTHROPIC_API_KEY`. Codex uses its file-based login and needs no profile. mac-worker never creates, uploads, or prints a profile.

`worker workers --refresh` and `worker task reconcile` are planned: refresh recollects agent, profile, and Git-identity facts; reconcile re-owns dead runners and re-enqueues orphaned tasks without submitting anything.

These remain later phases, not this execution core: `source = origin`, `publish = push`, Cursor Agent, OpenCode, retention through `worker gc`, and the dashboard tasks view.

Agent turns run with the worker account's full access: its files, processes, caches, agent configuration, and any credentials that account holds. A task workspace is not a security boundary. Only dispatch trusted prompts.
