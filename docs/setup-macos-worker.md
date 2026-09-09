# Prepare a Mac worker

Start with one Apple Silicon Mac and one agent. Commands below say whether they run on the **laptop** (controller) or the **worker**. Replace `yourname@mini.local` with the worker account and hostname or IP address.

## 1. Prepare the worker

On the **worker**, use your existing macOS account or a dedicated standard account. Jobs run with that account's access. Enable **System Settings → General → Sharing → Remote Login** and allow that account.

Open Terminal on the worker and check Git:

```bash
git --version
```

If macOS asks to install Command Line Tools, complete the installation. You can also start it with `xcode-select --install`.

Keep the Mac powered and awake while jobs run. On a desktop Mac, enable the setting to prevent automatic sleep when the display is off. Check the power settings again after rebooting. The worker needs outbound access to its agent provider; mac-worker needs only SSH inbound.

## 2. Connect over SSH

If `ssh -o BatchMode=yes yourname@mini.local true` already succeeds without a prompt, skip to [Connect the worker](#3-connect-the-worker).

On the **laptop**, create a dedicated key. Choose a passphrase when prompted. If that filename already exists, reuse the existing key or choose a new filename; do not overwrite it.

```bash
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
ssh-keygen -t ed25519 -f "$HOME/.ssh/mac-worker_ed25519" -C mac-worker
ssh-add --apple-use-keychain "$HOME/.ssh/mac-worker_ed25519"
```

For the first connection, verify the server's fingerprint. On the **worker**, display it locally:

```bash
ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
```

On the **laptop**, connect once and compare the displayed ED25519 fingerprint before accepting it:

```bash
ssh -o HostKeyAlgorithms=ssh-ed25519 -o ForwardAgent=no yourname@mini.local
```

Enter the worker account's password, then `exit` to return to the laptop. If macOS has a different host-key configuration, verify its corresponding public-key fingerprint instead.

On the **laptop**, append the public key to the worker's authorized keys. This asks for the worker password once more:

```bash
cat "$HOME/.ssh/mac-worker_ed25519.pub" | \
  ssh -o ForwardAgent=no yourname@mini.local \
  'umask 077; mkdir -p "$HOME/.ssh" && chmod 700 "$HOME/.ssh" && cat >> "$HOME/.ssh/authorized_keys" && chmod 600 "$HOME/.ssh/authorized_keys"'
```

Add this block to the laptop's `~/.ssh/config`, replacing the hostname and username. It lets SSH find and unlock the dedicated key; an extra alias is optional:

```sshconfig
Host mini.local
    User yourname
    IdentityFile ~/.ssh/mac-worker_ed25519
    IdentitiesOnly yes
    AddKeysToAgent yes
    UseKeychain yes
    StrictHostKeyChecking yes
    ForwardAgent no
```

Use the same address in the `Host` line that you pass to `worker init`. Keep existing settings such as `Port` or `ProxyJump` if you need them.

Check from the **laptop**:

```bash
ssh -o BatchMode=yes -o ConnectTimeout=5 -o ForwardAgent=no \
  -o ClearAllForwardings=yes yourname@mini.local /usr/bin/true
```

Success means exit code `0` and no prompt. If it fails, check Remote Login, the account name, the loaded key and `~/.ssh/config`. Do not disable host-key checking to make the check pass.

## 3. Connect the worker

Install the CLI on the laptop using the [README](../README.md#1-install-the-cli-on-your-laptop), then run on the **laptop**:

```bash
worker init yourname@mini.local
```

The command checks SSH, architecture and Git; creates `~/.config/mac-worker/config.toml`; installs the helper at `~/.local/bin/worker` on the worker; and checks Codex. A custom `--config PATH` is supported, as are the existing XDG configuration paths.

The default worker name is the hostname without `.local`. Use `--name mini-1` to choose another name. A retry preserves a registered destination's existing name. Adding another address appends a worker and preserves existing entries and comments.

An agent/login failure leaves the configuration available for the next attempt. Complete the reported step and rerun the command. `init` reuses a healthy compatible helper and refreshes agent checks; `worker setup` explicitly reinstalls or updates helpers.

## 4. Install and log in to one agent

Do these steps as the same **worker account** used by SSH. Agent credentials stay on that Mac.

### Codex (default)

On the **worker**, with Node.js and npm available (Homebrew's `node` is enough):

```bash
npm install -g --prefix "$HOME/.local" @openai/codex
codex login --device-auth
codex login status
```

`~/.local/bin` must be on the login shell's PATH ahead of Homebrew. Prefer this npm install over `brew install --cask codex`: the cask's binaries carry a quarantine attribute, and on a headless Mac Gatekeeper's first-launch assessment of its `codex-code-mode-host` helper can hang, after which every Codex shell command fails with "timed out negotiating with the code-mode host". Files unpacked by npm are not quarantined. If a cask is already installed, remove it with `brew uninstall --cask codex` so the login shell resolves the npm copy.

Open the device-login URL in a browser and enter the code. Device login may need to be enabled in your ChatGPT account or workspace. If unavailable, run `codex login` in the worker's desktop session and sign in there. See the [official installation guide](https://developers.openai.com/codex/cli/) for other installation methods and [authentication guide](https://developers.openai.com/codex/auth/) for login options. mac-worker checks the login again through SSH.

On the **laptop**:

```bash
worker init yourname@mini.local --agent codex
```

No environment profile is required for a working Codex login.

### Other agents

| Agent | Install on the worker | Login on the worker | Check from the laptop |
| --- | --- | --- | --- |
| OpenCode | [Official installer](https://opencode.ai/docs/) | `opencode auth login` | `worker init yourname@mini.local --agent opencode` |
| Cursor | [Official installer](https://cursor.com/docs/cli/installation); see executable name below | `cursor-agent login` or an API-key profile | `worker init yourname@mini.local --agent cursor --env-profile agents` |
| Claude Code | [Official installer](https://code.claude.com/docs/en/setup) | Sign in with Claude Code or provision a supported token/API-key profile | `worker init yourname@mini.local --agent claude` (add `--env-profile agents` when needed) |

This version of mac-worker invokes Cursor as `cursor-agent`. If the official installer provides only `agent`, confirm it is the Cursor executable, then expose that executable under the expected name on the worker's login `PATH`. Do not replace an unrelated `agent` or an existing `cursor-agent`. Probes and turns use the account's login-shell PATH; a tool visible only in an interactive shell may need its PATH setup moved to the login-shell configuration. `worker workers` reports `unknown (login unverified: user details unavailable)` when `cursor-agent status` cannot fetch user details; fix that with `cursor-agent login` on the worker, or put `CURSOR_API_KEY` in the env profile.

Use only the selected agent to finish your first task. Additional agents can be installed and checked later.

## Show turns in herdr (optional)

If [herdr](https://herdr.dev) runs on the worker, the pool can show its turns in that herdr, and through herdr's machine link or the herdr-mirror plugin, in the herdr on your laptop.

1. Start herdr's server on the worker as the worker account and keep it running. Its default session must own the socket `~/.config/herdr/herdr.sock`:

   ```sh
   herdr status server
   ```

2. On the laptop, turn the reporter on for that worker in `~/.config/mac-worker/config.toml` and rerun setup so the worker's facts include herdr:

   ```toml
   [[workers]]
   name = "mini-1"
   ssh = "yourname@mini.local"
   slots = 1
   herdr = true
   ```

   ```sh
   worker setup mini-1
   worker doctor
   ```

   `doctor` prints `herdr: available (<version>)` for the worker, or warns with `HERDR_UNAVAILABLE` and says why. Turns run either way; the warning only means nothing will show up in herdr.

3. Add the worker as a machine in your laptop's herdr (`herdr machine add <ssh-target>`) or run the herdr-mirror plugin, so the `mac-worker` workspace and its `task <id> · turn <n>` tabs appear beside your local agents. Custom sidebar rows can name the tokens `task`, `turn`, `mw_title`, `mw_agent`, and `mw_outcome`.

The reporter only reads the worker's task records and writes nothing to disk on its own; `worker task close` and `worker gc --apply` remove the tabs it opened.

## Environment profiles

Profiles are optional. They supply agent variables or unlock a headless login keychain. On the **worker**, create a private profile named `agents`:

```bash
umask 077
mkdir -p "$HOME/.config/mac-worker/env"
chmod 700 "$HOME/.config/mac-worker/env"
${EDITOR:-vi} "$HOME/.config/mac-worker/env/agents.env"
chmod 600 "$HOME/.config/mac-worker/env/agents.env"
```

Write `KEY=value` lines, without shell commands. The file must be regular, owned by the worker account and mode `0600`. Never put it in the repository or a task prompt.

| Agent | Supported profile values |
| --- | --- |
| Cursor | `CURSOR_API_KEY` |
| Claude Code | `CLAUDE_CODE_OAUTH_TOKEN` or `ANTHROPIC_API_KEY` |
| OpenCode | Provider variables needed by the selected CLI/provider |

A Cursor login stored in the macOS keychain can be locked over SSH. For that login, put `MAC_WORKER_KEYCHAIN_PASSWORD` in the profile, with optional `MAC_WORKER_KEYCHAIN_PATH` for a non-default keychain. The helper consumes these reserved values and sends the password to `security unlock-keychain` on stdin; it does not export them to the agent. Provision the values locally on the worker.

Check the selected profile and use it for tasks:

```bash
worker init yourname@mini.local --agent cursor --env-profile agents
worker task submit --agent cursor --env-profile agents --wait --prompt "Create SETUP_CHECK.md containing: mac-worker works."
```

`init` reports readiness only when the named profile is secure and the selected agent authenticates with it. It does not save a project-wide agent/profile default; use the printed task command or configure [project defaults](usage.md).

### Login startup and profile precedence

mac-worker runs agent probes, prebind checks and turns through the worker account's login shell (`/bin/zsh -lc`), not a direct binary exec with a captured PATH. The effective environment is built in this order:

1. Account scaffold for the worker account home used by the request: `HOME` is set to that supplied account home. `USER`, `LOGNAME`, and `SHELL` follow the same turn semantics as task launches—the mac-worker daemon's non-empty ambient values when present, otherwise the account-home directory name for `USER`/`LOGNAME` and `/bin/zsh` for `SHELL`.
2. Secure profile entries selected for the task or probe, when present.
3. Login startup files such as `~/.zprofile`, which may override earlier values including `PATH` and agent credentials.

Profile variables are applied before login startup runs, but login startup wins on conflicts. If `~/.zprofile` exports a different `CURSOR_API_KEY` or replaces `PATH`, facts, prebind and turns all observe that post-login value. Put durable PATH and credential setup in the login-shell configuration the worker account actually uses over SSH, and treat profile files as explicit overrides only when you intend them to apply before login startup.

Keep the worker account's login startup silent on stdout and stderr, including in `~/.zprofile` and `~/.zshenv`. Login files run before the agent binary, so anything they print is included in the bounded version and auth probe output that mac-worker classifies. Extra lines can make a valid credential report as `Unknown` during `worker init` or refresh—for example when login startup adds an unrecognised status line, when a warning contains a scanned error keyword, or when login output on stdout or stderr exceeds its own 4 KiB probe limit. mac-worker does not strip or rewrite that output; fixing chatty login files is an operator configuration change.

Account-bound launches clear the parent process environment before applying the scaffold and profile entries, so credentials present only in the laptop shell or SSH client are not inherited. Ordinary non-isolated process launches still inherit the parent environment.

## 5. Run a task, then prepare your project

Follow [Get your first branch](../README.md#3-get-your-first-branch). Builds also need your project's runtime, package manager, dependencies and any required development services.

Configure your Git name and email on the **laptop**. mac-worker passes the submitter's identity to worker commits; absent an identity, it uses a fixed mac-worker fallback.

For a Rust project under Codex's workspace sandbox, populate the worker account's Cargo registry before a build task. In a worker checkout with the same `Cargo.lock`, run:

```bash
cargo fetch --locked
cargo fetch --locked --offline
```

The offline check succeeds once the lockfile's crates are cached. This avoids registry writes outside the task worktree during a sandboxed turn. The [usage reference](usage.md) covers other project options.

## Updating and installation recovery

After updating the laptop CLI, run on the **laptop**:

```bash
worker setup
worker workers --refresh
```

Use inventory names to update a subset, for example `worker setup mini`. These commands update mac-worker helpers, not agents or project dependencies. Setup warms up the helper's first launch before the 15 s verification probe; if the warm-up fails but verification succeeds, setup reports `WARMUP_FAILED` as a warning. It refreshes the worker's agent facts under a separate 120 s deadline; if verification succeeds, a slow or other non-lock/non-layout refresh failure is reported as a `FACTS_REFRESH_FAILED` warning. If that step reports a locked state or outdated layout, setup still rolls back the promotion. Verification remains decisive: if it fails, setup rolls back the promoted helper and reports `VERIFICATION_FAILED`. If a helper reports a retained installation lock or an outdated layout, use [installation recovery](setup-recovery.md).

## Removal and stored data

Finish or cancel active tasks and fetch results you want to keep before retiring a worker.

- Remove its `[[workers]]` block from the laptop's configuration to stop scheduling work there. To retire the whole pool, stop submissions and keep or archive that configuration.
- Delete `~/.local/bin/worker` on the laptop and, if retiring it, on the worker account. If installed with Homebrew later, use `brew uninstall mac-worker` on the laptop instead.
- Configuration and local task history remain in `~/.config/mac-worker`, `~/.local/state/mac-worker`, `~/.cache/mac-worker` and `~/.local/share/mac-worker` (or your XDG locations).
- The worker's `~/.local/share/mac-worker` holds installation state, project mirrors, task worktrees and logs. `worker gc` previews reclaimable task data; `worker gc --apply` performs supported cleanup while the worker is still configured and installed. Keep or archive remaining data separately.
- Agents keep credentials and sessions outside mac-worker's directories. Removing mac-worker does not sign out agents or remove their session history.

To revoke SSH access, remove only the dedicated public key's line from the worker's `authorized_keys` and the matching laptop SSH configuration entry. Removing the helper alone does not revoke account access.
