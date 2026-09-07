<p align="center">
  <img src="docs/images/hero.png" alt="mac-worker: send a coding task to a Mac and get a Git branch back" width="100%">
</p>

# mac-worker

**Send a coding task to another Mac. Get a Git branch back.**

`mac-worker` runs coding agents on your spare Macs while you keep working on your laptop. Submit a prompt from a Git repository; a worker runs Codex, Cursor, OpenCode or Claude Code in a separate worktree and returns a branch you can review and merge. Start with one Mac and add more when you need them.

<p align="center">
  <img src="docs/images/dashboard.png" alt="Dashboard showing connected Macs, queued tasks and completed work" width="100%">
</p>

## What you need

- **Your laptop:** an Apple Silicon Mac, Git and SSH.
- **One worker:** another Apple Silicon Mac with Remote Login enabled and Git installed. It needs network access to the agent provider and must stay awake while working.
- **One coding agent on the worker**, signed in as the account you connect to. Codex is the default; the setup guide covers its installation and login.

The first supported installation path is Apple Silicon Mac → Apple Silicon Mac. Daily use has been validated on macOS 26. Intel Macs and Linux controllers are not supported by the first-run installer yet.

Only run trusted tasks on machines you control: a task worktree isolates repository changes, but jobs can access the worker account's files and credentials. Agent providers receive task content according to the selected agent's settings.

## Quick start

### 1. Install the CLI on your laptop

The release installer downloads a prebuilt binary and verifies its SHA-256 checksum. Rust and Node.js are not required; the dashboard is included.

**For a published release:** download `install.sh` from that release's source tag, then run it:

```bash
curl -fsSLo /tmp/mac-worker-install.sh \
  https://raw.githubusercontent.com/kirchik95/mac-worker/v0.1.0/install.sh
sh /tmp/mac-worker-install.sh --version v0.1.0
export PATH="$HOME/.local/bin:$PATH"
worker --version
```

Add `export PATH="$HOME/.local/bin:$PATH"` to `~/.zprofile` to make it available in new macOS terminal sessions. To choose a different location, pass `--bin-dir /your/bin` to the installer.

**Before the first release is published**, use [Build from source](#build-from-source) below. The repository includes release automation and a Homebrew formula generator; a public release and tap must be published before their download/install commands are available. Check [Releases](https://github.com/kirchik95/mac-worker/releases) for downloadable versions.

### 2. Connect one Mac

On the worker, enable **System Settings → General → Sharing → Remote Login** for your account. Then, on the laptop:

```bash
worker init yourname@mini.local
```

Use the worker's hostname or IP address; an existing SSH alias also works. If SSH keys are not set up, follow [SSH preparation](docs/setup-macos-worker.md#2-connect-over-ssh).

`init` checks the connection, architecture and Git; creates your worker configuration; installs the helper; and checks Codex's login over SSH. It gives instructions for missing steps. Complete the reported step and rerun the command — existing workers and configuration comments are preserved.

For another agent or a custom name:

```bash
worker init yourname@mini.local --name build-mini --agent opencode
```

No manual TOML or extra SSH alias is required. The [worker setup guide](docs/setup-macos-worker.md) covers agent installation, login and keeping the Mac awake.

### 3. Get your first branch

In a Git repository with at least one commit, submit a small task:

```bash
worker task submit --agent codex --wait \
  --prompt "Create SETUP_CHECK.md containing: mac-worker works."
```

The command prints a task ID. Substitute it for `<task-id>` below:

```bash
worker task result <task-id>
worker task fetch <task-id>
```

`result` shows the outcome and summary. `fetch` prints the remote-tracking ref to inspect with `git show` or `git diff`. Your current working tree stays unchanged; you choose whether to merge. Use the same `--agent` you checked with `init`.

For real coding tasks, install your project's language tools and dependencies on the worker too. Start with this small file task to check the connection and agent before running a build.

### 4. Follow the work

```bash
worker dashboard
worker task list
worker workers --refresh
```

The dashboard opens locally in your browser. To submit and return immediately, omit `--wait`.

## Add another Mac

```bash
worker init yourname@second-mini.local --name mini-2
```

The scheduler uses an available compatible worker. Each Mac runs one task turn at a time. Use `--worker <name>` on `task submit` to choose a machine.

## Update or remove

Install a newer published CLI with `install.sh --version <release-tag>`, then update the helpers:

```bash
worker setup
worker workers --refresh
```

`worker setup` with no names updates every configured worker. It does not install or update the agents themselves. See [installation recovery](docs/setup-recovery.md) if an older installation needs attention.

To remove the CLI installed by the script, delete `~/.local/bin/worker` (or the file in your chosen `--bin-dir`). This keeps configuration and task history. See [removal and stored data](docs/setup-macos-worker.md#removal-and-stored-data) before retiring a worker.

## How it works

<p align="center">
  <img src="docs/images/architecture.png" alt="The laptop sends a task over SSH; a Mac runs an agent and publishes a branch" width="100%">
</p>

The laptop queues tasks and sends their base commit to workers over SSH. Each worker runs an agent in a task worktree and publishes the result as `task/<id>`. The laptop fetches that branch for review. Workers keep project mirrors, task worktrees and agent sessions; `worker gc` previews retained data that can be reclaimed.

There is no cloud control plane, database or service to administer on the laptop. Workers contact their agent providers directly. The dashboard is embedded in the CLI and listens only on loopback.

## Build from source

For development or before the first binary release, install a current stable Rust toolchain and Git on the laptop, then:

```bash
git clone https://github.com/kirchik95/mac-worker.git
cd mac-worker
cargo build --locked --release
mkdir -p "$HOME/.local/bin"
install -m 755 target/release/worker "$HOME/.local/bin/worker"
export PATH="$HOME/.local/bin:$PATH"
worker init yourname@mini.local
```

Node.js is needed only when changing the dashboard source in `ui/`; its built assets are checked in and included by Cargo. See [UI development](ui/README.md).

## More documentation

- [Prepare a Mac worker](docs/setup-macos-worker.md): SSH, agents, profiles, power settings and removal.
- [Usage reference](docs/usage.md): tasks, follow-ups, batches, defaults, dashboard and remote commands.
- [Installation recovery](docs/setup-recovery.md): retained installer state and older host layouts.
- [Build and publish a release](docs/releasing.md): archives, checksums and Homebrew distribution.
- [Acceptance runbook](docs/phase-five-acceptance-runbook.md) and [validation record](docs/phase-five-validation.md).
- [Design notes](docs/superpowers/specs/) and [implementation plans](docs/superpowers/plans/).

## License

[MIT](LICENSE)
