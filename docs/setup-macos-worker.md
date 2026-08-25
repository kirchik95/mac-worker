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
