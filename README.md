# mac-worker

**English** · [Русский](README.ru.md) · [Documentation website](https://kirchik95.github.io/mac-worker/)

<!-- Keep README.md and README.ru.md in sync. Detailed instructions belong in docs/getting-started.md. -->

**Send a coding task to another Mac. Get a Git branch back.**

Run Codex, Cursor, OpenCode or Claude Code on your spare Macs over SSH. Each task gets its own Git worktree; you review and merge the result on your laptop.

## What you need

- Two Apple Silicon Macs with Git: your laptop and at least one worker.
- SSH key access to the worker, with Remote Login enabled. Keep the worker awake while tasks run.
- One coding agent installed and signed in on the worker. The examples use Codex.

[Worker preparation](docs/setup-macos-worker.md) covers SSH, agents and project tools. Run trusted tasks: agents have the worker account's access to files and credentials.

## Quick start

Run the commands below on your **laptop**.

### 1. Install the CLI on your laptop

Build from source with Rust and Git:

```bash
git clone https://github.com/kirchik95/mac-worker.git
cd mac-worker
cargo build --locked --release
mkdir -p "$HOME/.local/bin"
install -m 755 target/release/worker "$HOME/.local/bin/worker"
export PATH="$HOME/.local/bin:$PATH"
```

Add the `export PATH` line to `~/.zprofile` for new terminal sessions. The dashboard is included. [Installation options and releases](docs/getting-started.md#1-install-the-cli-on-your-laptop).

### 2. Connect one Mac

```bash
worker init yourname@mini.local --agent codex
```

Replace the SSH address with yours. `init` installs the helper and checks the agent's login. If it reports a missing setup step, complete it and rerun the command. For another agent, use its `--agent` value here and when submitting.

### 3. Get your first branch

Open your project, a Git repository with at least one commit:

```bash
cd /path/to/your/project
worker task submit --agent codex --wait \
  --prompt "Create SETUP_CHECK.md containing: mac-worker works."
```

Use the printed task ID in place of `<task-id>`:

```bash
worker task result <task-id>
worker task diff <task-id> --stat
worker task fetch <task-id>
```

`fetch` prints the Git ref to review. Your current working tree stays unchanged; you decide what to merge. Tasks start from `HEAD` by default; sending uncommitted edits requires [`--wip`](docs/getting-started.md#manual-cli).

## Daily use

Open the dashboard to follow workers and tasks:

```bash
worker dashboard
```

Prefer to ask your laptop's coding agent? [Install the two pool skills](docs/getting-started.md#ask-your-laptop-agent), then ask:

> Send this task to the pool: create SETUP_CHECK.md containing mac-worker works. Wait for the result and show me the branch.

By default the laptop manages the queue. Enable a [remote controller](docs/usage.md#remote-controller) to keep the queue and runners on an always-on Mac.

## Documentation

- [Detailed guide](docs/getting-started.md) — installation, laptop skills, workflow and architecture.
- [Worker setup](docs/setup-macos-worker.md) — SSH, agent logins and project dependencies.
- [Usage reference](docs/usage.md) — follow-ups, parallel tasks, dependencies, settings and known limitations.
- [Updating and removal](docs/getting-started.md#update-or-remove) — backups, helper updates and dashboard restart.
- [Development and releases](docs/releasing.md) · [Dashboard development](ui/README.md).

[MIT License](LICENSE)
