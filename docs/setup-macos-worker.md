# Set up a macOS worker

This guide prepares a trusted Mac mini for the phase-one `worker setup` and `worker workers` commands. It deliberately separates account administration from tool installation: `worker setup` installs the worker helper, but does not create accounts, grant privileges, or change Remote Login settings.

## 1. Create the worker account

Create a dedicated standard macOS account manually. `worker setup` never creates or elevates accounts. Do not make this account an administrator and do not grant it sudo access.

Keep Codex, Claude, personal cloud credentials, production credentials, and sudo access out of the worker account. Treat this account as a narrowly scoped execution identity, not as a personal login.

## 2. Enable SSH only for that account

In macOS **System Settings**, enable **General → Sharing → Remote Login** and allow access only for the dedicated worker account. Do not enable Remote Login for general users merely to make the worker reachable.

Install one dedicated public key in that account's `~/.ssh/authorized_keys`. Set mode `0700` on `.ssh` and `0600` on `authorized_keys`:

```bash
chmod 0700 ~/.ssh
chmod 0600 ~/.ssh/authorized_keys
```

Use the worker account's own shell for these commands. The key must be dedicated to this worker access path; do not reuse a personal key that has broader access.

## 3. Configure the local SSH alias

Configure the alias used by `config.example.toml` (for example, `mac1`) to log in as the dedicated worker account. Pin the worker host key locally and disable SSH agent forwarding for each worker alias. A representative `~/.ssh/config` entry is:

```sshconfig
Host mac1
    HostName <worker-hostname-or-address>
    User <dedicated-worker-account>
    IdentityFile ~/.ssh/<dedicated-worker-key>
    IdentitiesOnly yes
    UserKnownHostsFile ~/.ssh/known_hosts
    StrictHostKeyChecking yes
    ForwardAgent no
```

Add and verify the worker's host key in the local `~/.ssh/known_hosts` through your normal trusted host-key verification process. Do not weaken host-key checking or rely on agent forwarding as a workaround.

## 4. Verify the prerequisite

Before installing anything, confirm that the exact alias completes a non-interactive connection:

```bash
ssh -o BatchMode=yes -o ConnectTimeout=5 mac1 /usr/bin/true
```

This command must exit `0`, print no stdout, and never prompt. If it fails, correct the dedicated-account, key, or pinned-host-key setup first. Do not run `worker setup` until it succeeds.

## 5. Install and check the helper

After the SSH prerequisite succeeds, use the local phase-one commands:

```bash
./target/release/worker setup mini-1
./target/release/worker workers
./target/release/worker --json workers | jq .
```

`setup` is safe to run again for the same worker; it replaces the helper installation. Phase one only supports setup and inventory/probe reporting. `worker run`, snapshots, queues, logs, and artifacts are not yet implemented.

## Recovery from retained setup state

If `setup` reports `INSTALL_LOCKED`, `UNKNOWN_INSTALLATION_STATE`, or a cleanup/rollback warning, connect as the dedicated worker account and recover only the transaction owned by the retained lock. Do not retry setup until the following checks are complete. Never use `sudo`, `rm -rf`, globs, or broad cleanup under `~/.local/share/mac-worker/setup/`.

Read and validate the exact lock owner before forming a transaction path. The owner must be the 32-character lowercase hexadecimal installation ID; otherwise retain the lock and stop.

```bash
setup_root="$HOME/.local/share/mac-worker/setup"
lock_dir="$setup_root/.install-lock"
owner="$(/bin/cat "$lock_dir/owner" 2>/dev/null)"

if ! printf '%s\n' "$owner" | /usr/bin/grep -Eq '^[0-9a-f]{32}$'; then
    printf '%s\n' 'setup lock owner is missing or invalid; retain the lock and stop' >&2
    exit 1
fi

transaction="$setup_root/$owner"
if [ ! -d "$transaction" ]; then
    printf '%s\n' 'owner-scoped setup transaction is missing; retain the lock and stop' >&2
    exit 1
fi

/bin/ls -ld "$lock_dir" "$transaction"
/bin/cat "$transaction/state" 2>/dev/null
/bin/cat "$transaction/candidate.sha256" 2>/dev/null
/bin/cat "$transaction/previous.sha256" 2>/dev/null
/usr/bin/shasum -a 256 "$HOME/.local/bin/worker" 2>/dev/null
```

Only proceed if the lock owner still equals `$owner`, the active helper SHA-256 equals the recorded `candidate.sha256` or `previous.sha256`, and the active helper succeeds:

```bash
active_digest="$(/usr/bin/shasum -a 256 "$HOME/.local/bin/worker" 2>/dev/null | /usr/bin/awk '{print $1}')"
candidate_digest="$(/bin/cat "$transaction/candidate.sha256" 2>/dev/null)"
previous_digest="$(/bin/cat "$transaction/previous.sha256" 2>/dev/null)"
current_owner="$(/bin/cat "$lock_dir/owner" 2>/dev/null)"

if [ "$current_owner" != "$owner" ] || { [ "$active_digest" != "$candidate_digest" ] && [ "$active_digest" != "$previous_digest" ]; }; then
    printf '%s\n' 'setup state or helper digest is unknown; retain the lock and stop' >&2
    exit 1
fi

"$HOME/.local/bin/worker" host probe
```

After that probe succeeds, re-read the owner and digest immediately before performing the targeted cleanup. This removes only the named files from the exact owner-scoped transaction and releases only its matching lock.

```bash
current_owner="$(/bin/cat "$lock_dir/owner" 2>/dev/null)"
active_digest="$(/usr/bin/shasum -a 256 "$HOME/.local/bin/worker" 2>/dev/null | /usr/bin/awk '{print $1}')"

if [ "$current_owner" != "$owner" ] || { [ "$active_digest" != "$candidate_digest" ] && [ "$active_digest" != "$previous_digest" ]; }; then
    printf '%s\n' 'ownership or helper digest changed; retain the lock and stop' >&2
    exit 1
fi

/bin/rm -f "$transaction/worker.new" "$transaction/worker.previous" \
    "$transaction/candidate.sha256" "$transaction/previous.sha256" \
    "$transaction/no-previous" "$transaction/state"
/bin/rmdir "$transaction"
/bin/rm -f "$lock_dir/owner"
/bin/rmdir "$lock_dir"
```

If any inspection, identity check, probe, removal, or `rmdir` step fails, stop and retain the remaining state for investigation. Do not delete unrelated setup directories or release a lock whose owner no longer matches.
