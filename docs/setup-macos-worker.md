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

Before creating or changing setup state, the installer resolves the account home physically, creates missing `.local` directories one level at a time, and rejects symlinks or non-directories at `.local`, `.local/bin`, `.local/share`, `.local/share/mac-worker`, `setup`, the installation lock, and the owner transaction. It repeats those containment and type checks before digest recording, backup preparation, promotion, verification, cleanup, and rollback. A rejected or changed path leaves owner-scoped evidence in place and is reported as a setup failure or cleanup/rollback warning.

## Recovery from retained setup state

If `setup` reports `INSTALL_LOCKED`, `UNKNOWN_INSTALLATION_STATE`, or a cleanup/rollback warning, connect as the dedicated worker account and recover only the transaction owned by the retained lock. Do not retry setup until the following checks are complete. Never use `sudo`, `rm -rf`, globs, or broad cleanup under `~/.local/share/mac-worker/setup/`.

Read and validate the exact lock owner before forming a transaction path. The owner file must contain exactly 32 lowercase hexadecimal bytes followed by one newline and no other bytes or lines. Recorded digest files use the same canonical representation with 64 lowercase hexadecimal bytes followed by one newline. Any other representation must retain the lock and stop.

```bash
(
    set -eu
    LC_ALL=C
    export LC_ALL

    fail() {
        printf '%s\n' "$1" >&2
        exit 1
    }

    require_regular_file() {
        if [ -L "$1" ] || [ ! -f "$1" ]; then
            fail "$2 is missing, a symlink, or not a regular file; retain the lock and stop"
        fi
    }

    read_canonical_hex_file() {
        hex_file=$1
        hex_length=$2
        require_regular_file "$hex_file" "$3"
        byte_count=$(/usr/bin/wc -c < "$hex_file") \
            || fail "$3 cannot be sized; retain the lock and stop"
        if [ "$byte_count" -ne "$((hex_length + 1))" ]; then
            fail "$3 is not a canonical lowercase hexadecimal file; retain the lock and stop"
        fi
        hex_value=$(/bin/cat "$hex_file") \
            || fail "$3 cannot be read; retain the lock and stop"
        if [ "${#hex_value}" -ne "$hex_length" ]; then
            fail "$3 is not a canonical lowercase hexadecimal file; retain the lock and stop"
        fi
        case "$hex_value" in
            *[!0-9a-f]*)
                fail "$3 is not a canonical lowercase hexadecimal file; retain the lock and stop"
                ;;
        esac
        canonical_hex=$hex_value
    }

    setup_root="$HOME/.local/share/mac-worker/setup"
    lock_dir="$setup_root/.install-lock"
    owner_path="$lock_dir/owner"
    read_canonical_hex_file "$owner_path" 32 'setup lock owner'
    owner=$canonical_hex

    transaction="$setup_root/$owner"
    if [ -L "$transaction" ] || [ ! -d "$transaction" ]; then
        fail 'owner-scoped setup transaction is missing or a symlink; retain the lock and stop'
    fi

    /bin/ls -ld "$lock_dir" "$transaction"
    /bin/cat "$transaction/state" 2>/dev/null
    if [ -e "$transaction/candidate.sha256" ] || [ -L "$transaction/candidate.sha256" ]; then
        read_canonical_hex_file "$transaction/candidate.sha256" 64 'candidate digest'
        printf '%s\n' "$canonical_hex"
    fi
    if [ -e "$transaction/previous.sha256" ] || [ -L "$transaction/previous.sha256" ]; then
        read_canonical_hex_file "$transaction/previous.sha256" 64 'previous digest'
        printf '%s\n' "$canonical_hex"
    fi
    /usr/bin/shasum -a 256 "$HOME/.local/bin/worker" 2>/dev/null
)
```

If inspection shows an expected state, run this complete cleanup block as one command. It deliberately recomputes every value rather than relying on the inspection block, so it is safe to copy independently. It performs no removal until the owner, transaction, recorded digests, active helper, and probe have all been validated, then reads them again immediately before cleanup.

```bash
(
    set -eu
    LC_ALL=C
    export LC_ALL

    fail() {
        printf '%s\n' "$1" >&2
        exit 1
    }

    valid_hex_value() {
        [ "${#1}" -eq "$2" ] || return 1
        case "$1" in
            *[!0-9a-f]*) return 1 ;;
        esac
    }

    require_regular_file() {
        if [ -L "$1" ] || [ ! -f "$1" ]; then
            fail "$2 is missing, a symlink, or not a regular file; retain the lock and stop"
        fi
    }

    read_canonical_hex_file() {
        hex_file=$1
        hex_length=$2
        require_regular_file "$hex_file" "$3"
        byte_count=$(/usr/bin/wc -c < "$hex_file") \
            || fail "$3 cannot be sized; retain the lock and stop"
        if [ "$byte_count" -ne "$((hex_length + 1))" ]; then
            fail "$3 is not a canonical lowercase hexadecimal file; retain the lock and stop"
        fi
        hex_value=$(/bin/cat "$hex_file") \
            || fail "$3 cannot be read; retain the lock and stop"
        if ! valid_hex_value "$hex_value" "$hex_length"; then
            fail "$3 is not a canonical lowercase hexadecimal file; retain the lock and stop"
        fi
        canonical_hex=$hex_value
    }

    allow_absent_regular_file() {
        if [ -L "$1" ] || { [ -e "$1" ] && [ ! -f "$1" ]; }; then
            fail "$2 is a symlink or not a regular file; retain the lock and stop"
        fi
    }

    verify_setup_directory() {
        if [ -L "$setup_root" ] || [ ! -d "$setup_root" ]; then
            fail 'setup directory is missing or a symlink; retain the lock and stop'
        fi
        setup_physical="$(cd -P "$setup_root" 2>/dev/null && /bin/pwd)" \
            || fail 'setup directory cannot be resolved; retain the lock and stop'
        if [ "$setup_physical" != "$data_root/setup" ]; then
            fail 'setup directory is outside the mac-worker data root; retain the lock and stop'
        fi
    }

    verify_lock_directory() {
        if [ -L "$lock_dir" ] || [ ! -d "$lock_dir" ]; then
            fail 'setup lock directory is missing or a symlink; retain the lock and stop'
        fi
        lock_physical="$(cd -P "$lock_dir" 2>/dev/null && /bin/pwd)" \
            || fail 'setup lock directory cannot be resolved; retain the lock and stop'
        if [ "$lock_physical" != "$setup_physical/.install-lock" ]; then
            fail 'setup lock directory is outside the setup directory; retain the lock and stop'
        fi
    }

    verify_transaction_directory() {
        if [ -L "$transaction" ] || [ ! -d "$transaction" ]; then
            fail 'owner-scoped setup transaction is missing or a symlink; retain the lock and stop'
        fi
        transaction_physical="$(cd -P "$transaction" 2>/dev/null && /bin/pwd)" \
            || fail 'owner-scoped setup transaction cannot be resolved; retain the lock and stop'
        if [ "$transaction_physical" != "$setup_physical/$owner" ]; then
            fail 'owner-scoped setup transaction is outside the setup directory; retain the lock and stop'
        fi
    }

    verify_cleanup_file_types() {
        allow_absent_regular_file "$transaction/worker.new" 'staged helper'
        allow_absent_regular_file "$transaction/worker.previous" 'previous helper'
        allow_absent_regular_file "$transaction/candidate.sha256" 'candidate digest'
        allow_absent_regular_file "$transaction/previous.sha256" 'previous digest'
        allow_absent_regular_file "$transaction/no-previous" 'no-previous marker'
        allow_absent_regular_file "$transaction/state" 'setup state marker'
    }

    verify_transaction_entries() {
        /usr/bin/find "$transaction" ! -path "$transaction" -prune \
            -exec /bin/sh -c '
                transaction=$1
                shift
                for entry do
                    case "$entry" in
                        "$transaction/worker.new"|"$transaction/worker.previous"|\
                        "$transaction/candidate.sha256"|"$transaction/previous.sha256"|\
                        "$transaction/no-previous"|"$transaction/state") ;;
                        *)
                            printf "%s\n" "unexpected owner-scoped transaction entry: $entry; retain the lock and stop" >&2
                            exit 1
                            ;;
                    esac
                done
            ' sh "$transaction" {} + \
            || fail 'owner-scoped setup transaction cannot be enumerated or contains unexpected entries; retain the lock and stop'
    }

    verify_lock_entries() {
        /usr/bin/find "$lock_dir" ! -path "$lock_dir" -prune \
            -exec /bin/sh -c '
                transaction_owner_path=$1
                shift
                for entry do
                    case "$entry" in
                        "$transaction_owner_path") ;;
                        *)
                            printf "%s\n" "unexpected setup lock entry: $entry; retain the lock and stop" >&2
                            exit 1
                            ;;
                    esac
                done
            ' sh "$transaction_owner_path" {} + \
            || fail 'setup lock directory cannot be enumerated or contains unexpected entries; retain the lock and stop'
    }

    data_root="$(cd -P "$HOME/.local/share/mac-worker" 2>/dev/null && /bin/pwd)" \
        || fail 'mac-worker data root cannot be resolved; retain the lock and stop'
    setup_root="$data_root/setup"
    lock_dir="$setup_root/.install-lock"
    transaction_owner_path="$lock_dir/owner"
    worker="$HOME/.local/bin/worker"

    verify_setup_directory
    verify_lock_directory
    read_canonical_hex_file "$transaction_owner_path" 32 'setup lock owner'
    owner=$canonical_hex

    transaction="$setup_root/$owner"
    verify_transaction_directory
    verify_cleanup_file_types
    verify_transaction_entries
    verify_lock_entries

    candidate_present=0
    candidate_digest=''
    if [ -f "$transaction/candidate.sha256" ]; then
        candidate_present=1
        read_canonical_hex_file "$transaction/candidate.sha256" 64 'candidate digest'
        candidate_digest=$canonical_hex
    fi

    previous_present=0
    previous_digest=''
    if [ -f "$transaction/previous.sha256" ]; then
        previous_present=1
        read_canonical_hex_file "$transaction/previous.sha256" 64 'previous digest'
        previous_digest=$canonical_hex
    fi

    if [ "$candidate_present" -eq 0 ] && [ "$previous_present" -eq 0 ]; then
        fail 'no recorded helper digest is present; retain the lock and stop'
    fi

    active_digest="$(/usr/bin/shasum -a 256 "$worker" 2>/dev/null)" \
        || fail 'active helper digest cannot be read; retain the lock and stop'
    active_digest="${active_digest%% *}"
    if ! valid_hex_value "$active_digest" 64; then
        fail 'active helper digest is invalid; retain the lock and stop'
    fi
    if [ "$active_digest" != "$candidate_digest" ] && [ "$active_digest" != "$previous_digest" ]; then
        fail 'active helper digest is not a recorded digest; retain the lock and stop'
    fi

    if ! "$worker" host probe; then
        fail 'active helper probe failed; retain the lock and stop'
    fi

    verify_setup_directory
    verify_lock_directory
    read_canonical_hex_file "$transaction_owner_path" 32 'setup lock owner before cleanup'
    current_owner=$canonical_hex
    if [ "$current_owner" != "$owner" ]; then
        fail 'setup lock owner changed or is invalid; retain the lock and stop'
    fi
    verify_transaction_directory
    verify_cleanup_file_types

    current_candidate_present=0
    current_candidate_digest=''
    if [ -f "$transaction/candidate.sha256" ]; then
        current_candidate_present=1
        read_canonical_hex_file "$transaction/candidate.sha256" 64 'candidate digest before cleanup'
        current_candidate_digest=$canonical_hex
    fi

    current_previous_present=0
    current_previous_digest=''
    if [ -f "$transaction/previous.sha256" ]; then
        current_previous_present=1
        read_canonical_hex_file "$transaction/previous.sha256" 64 'previous digest before cleanup'
        current_previous_digest=$canonical_hex
    fi

    if [ "$current_candidate_present" -ne "$candidate_present" ] \
        || [ "$current_previous_present" -ne "$previous_present" ] \
        || [ "$current_candidate_digest" != "$candidate_digest" ] \
        || [ "$current_previous_digest" != "$previous_digest" ]; then
        fail 'recorded helper digests changed before cleanup; retain the lock and stop'
    fi

    current_active_digest="$(/usr/bin/shasum -a 256 "$worker" 2>/dev/null)" \
        || fail 'active helper digest disappeared before cleanup; retain the lock and stop'
    current_active_digest="${current_active_digest%% *}"
    if ! valid_hex_value "$current_active_digest" 64; then
        fail 'active helper digest is invalid before cleanup; retain the lock and stop'
    fi
    if [ "$current_active_digest" != "$active_digest" ] \
        || { [ "$current_active_digest" != "$candidate_digest" ] && [ "$current_active_digest" != "$previous_digest" ]; }; then
        fail 'active helper digest changed before cleanup; retain the lock and stop'
    fi

    verify_setup_directory
    verify_lock_directory
    read_canonical_hex_file "$transaction_owner_path" 32 'setup lock owner immediately before cleanup'
    final_owner=$canonical_hex
    if [ "$final_owner" != "$owner" ]; then
        fail 'setup lock owner changed or is invalid immediately before cleanup; retain the lock and stop'
    fi
    verify_transaction_directory
    verify_cleanup_file_types
    verify_transaction_entries
    verify_lock_entries

    /bin/rm -f "$transaction/worker.new" "$transaction/worker.previous" \
        "$transaction/candidate.sha256" "$transaction/previous.sha256" \
        "$transaction/no-previous" "$transaction/state"
    /bin/rmdir "$transaction"
    /bin/rm -f "$transaction_owner_path"
    /bin/rmdir "$lock_dir"
)
```

If any inspection, identity check, probe, removal, or `rmdir` step fails, stop, retain the lock and transaction as evidence, and do not retry setup. Do not delete unrelated setup directories or release a lock whose owner no longer matches.
