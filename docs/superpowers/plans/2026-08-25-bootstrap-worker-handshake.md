# Bootstrap and Worker Handshake Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first production-shaped vertical slice of `mac-worker`: one Rust binary that loads the worker inventory on the MacBook, installs itself without sudo into the configured existing macOS account, probes every configured worker over system SSH, and renders the same typed health report as human-readable or JSON output.

**Architecture:** The public client and hidden host helper are subcommands of the same `worker` binary. The client owns configuration and invokes fixed host-helper commands through an injectable process boundary; the helper gathers host facts and emits one versioned JSON response on stdout. This slice deliberately stops before project snapshots and user command execution, so the next plan can build `worker run` on a verified transport and protocol rather than a throwaway prototype.

**Tech Stack:** Rust 2024 edition, Cargo, `clap`, `serde`, `serde_json`, `toml`, `thiserror`, system OpenSSH/SCP, Rust unit and integration tests.

**Spec:** `docs/superpowers/specs/2026-08-25-mac-worker-design.md`

## Global Constraints

- The installed executable is named `worker`; the Cargo package is named `mac-worker`.
- The same native arm64 binary runs as the MacBook client and the SSH-invoked host helper.
- There is no persistent daemon, inbound service, sudo use, account creation, or runtime installation.
- Configuration honors `XDG_CONFIG_HOME`; its default path is `~/.config/mac-worker/config.toml`.
- Worker-owned data defaults to `~/.local/share/mac-worker/`; setup may touch only `.local/bin/worker` and paths below that data root.
- Every v1 worker declares exactly one heavy slot.
- Machine-readable responses are versioned typed records; diagnostics never contaminate JSON stdout.
- SSH aliases and worker names are data, never interpolated into a shell expression.
- Setup requires an existing macOS account selected for worker jobs and working non-interactive SSH authentication; a dedicated non-admin account is optional hardening.
- No personal credentials, SSH-agent forwarding, project files, or secrets are copied in this slice.

## Delivery Sequence

This is plan 1 of the implementation series:

1. Bootstrap and worker handshake — this document.
2. Project inspection and immutable local snapshots.
3. Single-worker submission, durable supervision, status, and reconnectable logs.
4. Three-worker FIFO scheduling, leases, cancellation, reconciliation, and safe cleanup.
5. Artifacts, Docker profiles, hardening, Codex/Claude skills, and the acceptance matrix.

The independently testable deliverable from this plan is:

```text
worker setup mini-1
worker workers
worker --json workers
```

## File Map

```text
Cargo.toml                              package, binary, runtime and test dependencies
src/main.rs                            process entry point and exit-code mapping
src/lib.rs                             public crate modules and top-level dispatch
src/cli.rs                             clap command model only
src/error.rs                           typed application errors and reserved exit codes
src/output.rs                          human/JSON rendering boundary, added after services are complete
src/paths.rs                           XDG and remote owned-path resolution
src/config.rs                          TOML schema, loading, and validation
src/protocol.rs                        protocol version and wire records
src/process.rs                         injectable child-process execution boundary
src/probe.rs                           local macOS fact and capability collection
src/transport.rs                       fixed SSH host-helper invocation and response parsing
src/install.rs                         non-sudo remote binary installation sequence
tests/cli_help.rs                      executable-level CLI contract tests
tests/workers_command.rs               inventory service tests with a recording process runner
tests/setup_command.rs                 installer and dispatch tests with a recording process runner
config.example.toml                    documented three-worker configuration
docs/setup-macos-worker.md             manual account and SSH prerequisites
README.md                              phase-one quick start and explicit current boundary
```

---

### Task 1: Scaffold the Rust binary and freeze the CLI boundary

**Files:**
- Create: `Cargo.toml`
- Create: `src/main.rs`
- Create: `src/lib.rs`
- Create: `src/cli.rs`
- Create: `src/error.rs`
- Create: `tests/cli_help.rs`

**Interfaces:**
- Produces: `cli::Cli`, `cli::Command`, `cli::HostCommand`, `error::WorkerError`, and `error::ExitKind`.
- Produces: process exit `0` for a successful command, `64` for usage/configuration errors, `69` for unavailable workers, `70` for protocol/infrastructure errors, and `74` for local I/O errors.
- Consumes: no earlier task.

- [ ] **Step 1: Add a failing executable-level help test**

```rust
// tests/cli_help.rs
use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn help_exposes_only_the_phase_one_public_commands() {
    let mut command = Command::cargo_bin("worker").unwrap();
    command.arg("--help");

    command
        .assert()
        .success()
        .stdout(predicate::str::contains("setup"))
        .stdout(predicate::str::contains("workers"))
        .stdout(predicate::str::contains("host").not());
}

#[test]
fn json_is_a_global_output_mode() {
    let mut command = Command::cargo_bin("worker").unwrap();
    command.args(["--json", "workers", "--help"]);
    command.assert().success();
}
```

- [ ] **Step 2: Run the test and confirm the binary does not exist**

Run: `cargo test --test cli_help`

Expected: FAIL because `Cargo.toml` and the `worker` binary target do not exist.

- [ ] **Step 3: Create the package and dependencies**

```toml
# Cargo.toml
[package]
name = "mac-worker"
version = "0.1.0"
edition = "2024"
publish = false

[[bin]]
name = "worker"
path = "src/main.rs"

[dependencies]
clap = { version = "4.5", features = ["derive"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
toml = "0.9"
uuid = { version = "1", features = ["v4"] }

[dev-dependencies]
assert_cmd = "2"
predicates = "3"
tempfile = "3"
```

- [ ] **Step 4: Implement the command model with a hidden host namespace**

```rust
// src/cli.rs
use std::path::PathBuf;
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "worker", version, about = "Run trusted development jobs on macOS workers")]
pub struct Cli {
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,
    #[arg(long, global = true)]
    pub json: bool,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Setup { hosts: Vec<String> },
    Workers,
    #[command(hide = true)]
    Host {
        #[command(subcommand)]
        command: HostCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum HostCommand {
    Probe,
}
```

- [ ] **Step 5: Implement typed errors and the parse-only scaffold entry point**

Use these exact top-level types:

```rust
// src/error.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitKind { Usage = 64, Unavailable = 69, Infrastructure = 70, Io = 74 }

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("worker unavailable: {0}")]
    Unavailable(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
```

```rust
// src/main.rs
use clap::Parser;

fn main() {
    let _parsed = mac_worker::cli::Cli::parse();
}
```

This entry point intentionally does nothing after successful parsing in the scaffold commit. Task 5 replaces it with the complete phase-one dispatcher only after all three operations have real implementations; no command returns fabricated success data in the intermediate commits.

- [ ] **Step 6: Run formatting and the focused test**

Run: `cargo fmt --all && cargo test --test cli_help`

Expected: both commands PASS.

- [ ] **Step 7: Commit the CLI scaffold**

```bash
git add Cargo.toml Cargo.lock src tests/cli_help.rs
git commit -m "feat: scaffold worker CLI"
```

---

### Task 2: Resolve owned paths and validate the worker inventory

**Files:**
- Create: `src/paths.rs`
- Create: `src/config.rs`
- Modify: `src/lib.rs`
- Create: `config.example.toml`

**Interfaces:**
- Consumes: `WorkerError::Config` and `Cli.config` from Task 1.
- Produces: `PathLayout::discover(config_override, env, home) -> Result<PathLayout, WorkerError>`.
- Produces: `Config::load(path) -> Result<Config, WorkerError>` and `Config::validate() -> Result<(), WorkerError>`.
- Produces: `WorkerEntry { name, ssh, slots, capabilities, remote_binary }` with lookup by logical worker name.

- [ ] **Step 1: Write failing unit tests for XDG precedence and invalid inventory entries**

```rust
#[test]
fn explicit_config_overrides_xdg_and_home() {
    let paths = PathLayout::discover(
        Some(PathBuf::from("/tmp/explicit.toml")),
        &BTreeMap::from([("XDG_CONFIG_HOME".into(), "/tmp/xdg".into())]),
        Path::new("/Users/tester"),
    ).unwrap();
    assert_eq!(paths.config, PathBuf::from("/tmp/explicit.toml"));
}

#[test]
fn duplicate_worker_names_are_rejected() {
    let config = Config::parse(include_str!("../config.example.toml")).unwrap();
    let mut duplicate = config.clone();
    duplicate.workers.push(duplicate.workers[0].clone());
    assert!(matches!(duplicate.validate(), Err(WorkerError::Config(_))));
}

#[test]
fn slots_other_than_one_are_rejected_in_v1() {
    let mut config = Config::parse(include_str!("../config.example.toml")).unwrap();
    config.workers[0].slots = 2;
    assert!(matches!(config.validate(), Err(WorkerError::Config(_))));
}
```

- [ ] **Step 2: Run the library tests and confirm the modules are missing**

Run: `cargo test --lib`

Expected: FAIL because `PathLayout` and `Config` are undefined.

- [ ] **Step 3: Implement deterministic local path discovery**

```rust
pub struct PathLayout {
    pub config: PathBuf,
    pub state: PathBuf,
    pub cache: PathBuf,
    pub data: PathBuf,
}
```

Resolution rules are exact:

1. `--config` wins for the config file only.
2. Otherwise use `$XDG_CONFIG_HOME/mac-worker/config.toml`, falling back to `$HOME/.config/mac-worker/config.toml`.
3. State, cache, and data use their matching XDG variables and fall back to `$HOME/.local/state/mac-worker`, `$HOME/.cache/mac-worker`, and `$HOME/.local/share/mac-worker`.
4. An absent home directory returns `WorkerError::Config`; no path is guessed from the current directory.

- [ ] **Step 4: Implement and validate the TOML schema**

```rust
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    pub workers: Vec<WorkerEntry>,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerEntry {
    pub name: String,
    pub ssh: String,
    pub slots: u8,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default = "default_remote_binary")]
    pub remote_binary: String,
}
```

Validation requires `version == 1`, at least one worker, unique non-empty names, unique non-empty SSH destinations, `slots == 1`, unique capability strings, and `remote_binary == "~/.local/bin/worker"` for this slice. Reject worker names or SSH destinations containing whitespace or shell metacharacters; accepted characters are ASCII letters, digits, `.`, `_`, `-`, and `@`.

- [ ] **Step 5: Add the concrete three-worker example**

```toml
version = 1

[[workers]]
name = "mini-1"
ssh = "mac1"
slots = 1
capabilities = ["darwin-arm64"]

[[workers]]
name = "mini-2"
ssh = "mac2"
slots = 1
capabilities = ["darwin-arm64"]

[[workers]]
name = "mini-3"
ssh = "mac3"
slots = 1
capabilities = ["darwin-arm64"]
```

- [ ] **Step 6: Run config tests and lint**

Run: `cargo test --lib && cargo clippy --all-targets -- -D warnings`

Expected: all tests PASS and Clippy emits no warnings.

- [ ] **Step 7: Commit configuration support**

```bash
git add src/lib.rs src/paths.rs src/config.rs config.example.toml
git commit -m "feat: load worker inventory"
```

---

### Task 3: Define the protocol and implement the local host probe

**Files:**
- Create: `src/protocol.rs`
- Create: `src/probe.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `WorkerError` from Task 1.
- Produces: `PROTOCOL_VERSION: u32 = 1`.
- Produces: `ProbeResponse`, `WorkerHealth`, `WorkersReport`, `SetupHostResult`, and `SetupReport` wire records.
- Produces: `ProbeCollector::collect() -> Result<ProbeResponse, WorkerError>`.

- [ ] **Step 1: Write failing serialization and probe-classification tests**

```rust
#[test]
fn probe_response_has_an_explicit_protocol_version() {
    let response = ProbeResponse::fixture();
    let value = serde_json::to_value(response).unwrap();
    assert_eq!(value["protocol_version"], 1);
    assert_eq!(value["arch"], "arm64");
}

#[test]
fn declared_capability_must_also_be_detected() {
    let response = ProbeResponse::fixture_with_capabilities(["darwin-arm64"]);
    let missing = missing_capabilities(&["darwin-arm64".into(), "docker".into()], &response);
    assert_eq!(missing, vec!["docker"]);
}
```

- [ ] **Step 2: Run the focused tests and confirm protocol types are absent**

Run: `cargo test --lib`

Expected: FAIL because the protocol module and collector do not exist.

- [ ] **Step 3: Implement versioned wire records**

```rust
pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ProbeResponse {
    pub protocol_version: u32,
    pub hostname: String,
    pub arch: String,
    pub os_version: String,
    pub free_disk_bytes: u64,
    pub memory_pressure: MemoryPressure,
    pub swap_used_bytes: Option<u64>,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryPressure { Normal, Warn, Critical, Unknown }

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus { Ready, Unavailable }

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerHealth {
    pub name: String,
    pub ssh: String,
    pub status: HealthStatus,
    pub probe: Option<ProbeResponse>,
    pub missing_capabilities: Vec<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkersReport { pub protocol_version: u32, pub workers: Vec<WorkerHealth> }

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SetupHostResult {
    pub name: String,
    pub ssh: String,
    pub installed: bool,
    pub protocol_version: Option<u32>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SetupReport {
    pub protocol_version: u32,
    pub workers: Vec<SetupHostResult>,
}
```

Implement `missing_capabilities(required: &[String], probe: &ProbeResponse) -> Vec<String>` so it preserves required-capability order and removes no information through set sorting. Test-only fixture constructors live inside `protocol.rs` under `#[cfg(test)]` and return fully populated records with deterministic values.

- [ ] **Step 4: Implement fact collection without a shell**

`ProbeCollector` invokes absolute programs as argv arrays: `/usr/bin/sw_vers -productVersion`, `/usr/bin/memory_pressure -Q`, `/usr/sbin/sysctl -n vm.swapusage`, `/bin/df -k /`, and fixed `--version` probes for known tools. A failed optional metric becomes `Unknown` or `None`; failure to determine hostname, architecture, OS version, or free disk is a protocol error. Capability discovery always includes `darwin-arm64` only when both OS and architecture match, and includes `git`, `rsync`, `node`, `ruby`, `python`, `go`, `dotnet`, `swift`, and `docker` only when the executable is found in the controlled host `PATH`.

- [ ] **Step 5: Add the host-probe serialization boundary**

Implement `probe::collect_json() -> Result<Vec<u8>, WorkerError>`. It collects once and serializes one compact `ProbeResponse`; on failure it returns an error and no partial JSON bytes. Task 5 connects this boundary to the hidden host command.

- [ ] **Step 6: Test protocol and local collection behavior**

Run: `cargo test --lib`

Expected: all tests PASS, including a test that deserializes `collect_json()` and verifies protocol version `1` plus a non-empty hostname.

- [ ] **Step 7: Commit the host protocol**

```bash
git add src/lib.rs src/protocol.rs src/probe.rs
git commit -m "feat: add versioned host probe"
```

---

### Task 4: Add the injectable process boundary and SSH transport

**Files:**
- Create: `src/process.rs`
- Create: `src/transport.rs`
- Modify: `src/lib.rs`
- Create: `tests/workers_command.rs`

**Interfaces:**
- Consumes: `WorkerEntry`, `ProbeResponse`, `WorkerHealth`, and `WorkersReport`.
- Produces: `ProcessRunner::run(&ProcessRequest) -> Result<ProcessResult, WorkerError>`.
- Produces: `SshTransport::probe(&WorkerEntry) -> WorkerHealth`.
- Produces: `WorkersService::inspect(&Config) -> WorkersReport`, preserving configured order.

- [ ] **Step 1: Write a failing test that captures the exact SSH argv**

```rust
#[test]
fn probe_uses_batch_ssh_and_the_fixed_host_command() {
    let runner = RecordingRunner::returning_json(valid_probe_json());
    let transport = SshTransport::new(runner.clone());
    let worker = WorkerEntry::fixture("mini-1", "mac1");

    let health = transport.probe(&worker);

    assert_eq!(health.status, HealthStatus::Ready);
    assert_eq!(runner.requests(), vec![ProcessRequest {
        program: "/usr/bin/ssh".into(),
        args: vec![
            "-o".into(), "BatchMode=yes".into(),
            "-o".into(), "ConnectTimeout=5".into(),
            "mac1".into(),
            "~/.local/bin/worker host probe".into(),
        ],
        stdin: None,
    }]);
}
```

- [ ] **Step 2: Run the test and confirm the transport is undefined**

Run: `cargo test --test workers_command probe_uses_batch_ssh`

Expected: FAIL because `ProcessRunner` and `SshTransport` are missing.

- [ ] **Step 3: Implement the argv-only process abstraction**

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessRequest {
    pub program: OsString,
    pub args: Vec<OsString>,
    pub stdin: Option<Vec<u8>>,
}

pub struct ProcessResult {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

pub trait ProcessRunner: Send + Sync {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError>;
}
```

The production runner uses `std::process::Command`, passes every argument separately, captures stdout/stderr, and never invokes `/bin/sh`, `zsh -c`, or `Command::new` with a concatenated local command.

- [ ] **Step 4: Parse the fixed remote response defensively**

`SshTransport::probe` requires exit `0`, UTF-8 JSON, `protocol_version == 1`, no more than 1 MiB of stdout, and declared capabilities present in the detected host response. SSH failure, malformed JSON, oversized output, and protocol mismatch become `WorkerHealth` entries with stable error codes rather than aborting the entire inventory.

- [ ] **Step 5: Implement the inventory service**

`WorkersService::inspect` probes each configured entry, retains ready and unavailable results, and returns them in config order. A worker failure is report data rather than an early function error; Task 5 renders the typed report and returns exit `0` because health is the command's requested data.

- [ ] **Step 6: Test ready, offline, malformed, and mismatched workers**

Use `RecordingRunner` instances with predetermined `ProcessResult` values. Cover these four cases:

```text
ready                 valid protocol-1 probe, status ready
offline               SSH exit 255, status unavailable/SSH_UNAVAILABLE
malformed             exit 0 with non-JSON stdout, status unavailable/INVALID_RESPONSE
protocol-mismatch     protocol_version 2, status unavailable/PROTOCOL_MISMATCH
```

Run: `cargo test --test workers_command`

Expected: all four scenarios PASS; every diagnostic is held in the typed error fields and `serde_json::to_string(&report)` remains valid JSON.

- [ ] **Step 7: Commit transport and inventory reporting**

```bash
git add src/lib.rs src/process.rs src/transport.rs tests/workers_command.rs
git commit -m "feat: probe configured workers over SSH"
```

---

### Task 5: Install and verify the binary without sudo

**Files:**
- Create: `src/install.rs`
- Create: `src/output.rs`
- Modify: `src/main.rs`
- Modify: `src/cli.rs`
- Modify: `src/lib.rs`
- Modify: `src/process.rs`
- Create: `tests/setup_command.rs`

**Interfaces:**
- Consumes: `ProcessRunner`, `SshTransport::probe`, `WorkerEntry`, and `SetupReport`.
- Produces: `Installer::install(current_exe, worker) -> SetupHostResult`.
- Produces: public `worker setup HOST...`; an empty host list means all configured workers.
- Produces: `execute_with(Cli, &dyn ProcessRunner) -> Result<CommandOutput, WorkerError>` and the final process exit mapping.

- [ ] **Step 1: Write a failing test for the exact installation sequence**

Use a fixed test installation ID `00112233445566778899aabbccddeeff`. Assert this process order:

```text
/usr/bin/ssh -o BatchMode=yes -o ConnectTimeout=5 mac1 \
  "umask 077 && mkdir -p ~/.local/bin ~/.local/share/mac-worker/setup/00112233445566778899aabbccddeeff"

/usr/bin/scp -q -o BatchMode=yes -o ConnectTimeout=5 /absolute/path/to/worker \
  mac1:~/.local/share/mac-worker/setup/00112233445566778899aabbccddeeff/worker.new

/usr/bin/ssh -o BatchMode=yes -o ConnectTimeout=5 mac1 \
  "chmod 0755 ~/.local/share/mac-worker/setup/00112233445566778899aabbccddeeff/worker.new && mv ~/.local/share/mac-worker/setup/00112233445566778899aabbccddeeff/worker.new ~/.local/bin/worker && rmdir ~/.local/share/mac-worker/setup/00112233445566778899aabbccddeeff"

/usr/bin/ssh -o BatchMode=yes -o ConnectTimeout=5 mac1 \
  "~/.local/bin/worker host probe"
```

Every remote expression is assembled exclusively from fixed literals plus a lowercase hexadecimal UUID. Worker destinations have already passed Task 2 validation.

- [ ] **Step 2: Run the focused test and confirm setup is unimplemented**

Run: `cargo test --test setup_command`

Expected: FAIL because `Installer` does not exist.

- [ ] **Step 3: Implement selection and preflight validation**

Resolve requested arguments only as logical names from the loaded inventory; never treat a CLI value as an arbitrary SSH destination. Before transfer, require `std::env::current_exe()` to be a regular executable file and run the local hidden probe to verify the binary speaks protocol `1`.

- [ ] **Step 4: Implement staged transfer, atomic promotion, and verification**

Create the remote owned staging directory, transfer with SCP, atomically rename within the account, and immediately call `SshTransport::probe`. A failed transfer or verification leaves a typed failure in `SetupHostResult`; setup continues with later requested hosts. It never deletes an existing working binary before the replacement is present.

- [ ] **Step 5: Add rollback for a failed verification**

Before promotion, rename an existing `~/.local/bin/worker` to `worker.previous`. If the new probe fails, atomically restore `worker.previous`; if it succeeds, remove only that exact backup. Add test scenarios for first install, successful replacement, failed transfer, and failed verification with restoration.

- [ ] **Step 6: Wire the complete phase-one dispatcher and renderer**

`CommandOutput` is the sole successful rendering input:

```rust
#[derive(Debug, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommandOutput {
    Setup(crate::protocol::SetupReport),
    Workers(crate::protocol::WorkersReport),
    Probe(crate::protocol::ProbeResponse),
}
```

`execute_with` handles all three parsed variants: `Setup` selects inventory entries and calls `Installer`; `Workers` calls `WorkersService`; hidden `Host::Probe` collects local facts. `main` parses once, creates `SystemProcessRunner`, prints successful JSON only to stdout when `--json` is set, prints human output otherwise, prints errors only to stderr, and maps the `WorkerError` variants to the reserved exit codes from Task 1. The hidden host probe always prints compact JSON so an SSH client can parse it regardless of the global display flag.

- [ ] **Step 7: Run all setup and regression tests**

Run: `cargo test --all-targets && cargo clippy --all-targets -- -D warnings`

Expected: all tests PASS and Clippy emits no warnings.

- [ ] **Step 8: Commit the installer and dispatcher**

```bash
git add src/main.rs src/lib.rs src/cli.rs src/process.rs src/install.rs src/output.rs tests/setup_command.rs
git commit -m "feat: install worker host helper"
```

---

### Task 6: Document provisioning and perform the first real handshake

**Files:**
- Create: `docs/setup-macos-worker.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: the completed `setup` and `workers` commands.
- Produces: a reproducible operator checklist and captured validation evidence for one Mac mini.
- Produces: the prerequisite for plan 2; no project source or user command is sent yet.

- [ ] **Step 1: Document the account boundary and SSH prerequisite**

The guide must state exactly:

1. Select the existing macOS account that will run worker jobs; `worker setup` never creates or elevates accounts.
2. Enable Remote Login for that account through macOS settings and limit access to accounts that need it.
3. Install one worker-specific public key in that account's `~/.ssh/authorized_keys` with modes `0700` for `.ssh` and `0600` for the file.
4. Pin the worker host key locally and disable SSH agent forwarding for each worker alias.
5. Confirm `ssh -o BatchMode=yes -o ConnectTimeout=5 mac1 /usr/bin/true` exits `0` without a prompt.
6. Only run trusted code, avoid passwordless sudo, and keep production or other high-value credentials off the worker machines where practical.

- [ ] **Step 2: Update the README with the phase-one quick start**

```bash
cargo test --all-targets
cargo build --release
mkdir -p ~/.config/mac-worker
cp config.example.toml ~/.config/mac-worker/config.toml
./target/release/worker setup mini-1
./target/release/worker workers
./target/release/worker --json workers | jq .
```

The README must clearly say that `worker run`, snapshots, queues, logs, and artifacts are not yet implemented at this checkpoint.

- [ ] **Step 3: Run the complete local quality gate**

Run: `cargo fmt --check && cargo test --all-targets && cargo clippy --all-targets -- -D warnings && cargo build --release`

Expected: all commands exit `0`.

- [ ] **Step 4: Verify non-interactive SSH on the selected mini**

Run: `ssh -o BatchMode=yes -o ConnectTimeout=5 mac1 /usr/bin/true`

Expected: exit `0` with no prompt and no stdout. If this fails, stop before installation and report the authentication prerequisite; do not weaken host-key or authentication settings automatically.

- [ ] **Step 5: Install and probe one mini**

Run: `./target/release/worker setup mini-1 && ./target/release/worker --json workers`

Expected: setup reports `installed: true`; `mini-1` is `ready`; its protocol version is `1`, architecture is `arm64`, OS version and hostname are non-empty, and `darwin-arm64` is present.

- [ ] **Step 6: Re-run setup to prove idempotent replacement**

Run: `./target/release/worker setup mini-1 && ./target/release/worker --json workers`

Expected: both commands exit `0`, the helper remains ready, and no setup directory for the completed installation remains on the worker.

- [ ] **Step 7: Commit documentation and validation notes**

```bash
git add README.md docs/setup-macos-worker.md
git commit -m "docs: add worker bootstrap guide"
```

## Plan Self-Review Results

- Spec coverage: this plan covers the shared binary, XDG config, worker inventory, SSH transport, protocol versioning, host probe, non-sudo setup, capability visibility, JSON/human output, and configured-account prerequisite. Snapshot, execution lifecycle, scheduling, artifacts, Docker, and GC are assigned to plans 2–5 rather than partially implemented here.
- Placeholder scan: every task names concrete files, interfaces, commands, expected results, and commit boundaries; no deferred implementation marker is used.
- Type consistency: `WorkerEntry`, `ProbeResponse`, `WorkerHealth`, `WorkersReport`, `ProcessRequest`, `ProcessRunner`, `SshTransport`, and `Installer` have a single spelling and ownership point throughout the plan.
- Safety check: no test or setup step creates accounts, enables Remote Login, changes sudo policy, disables host-key checking, forwards an agent, or deletes paths outside the exact setup staging directory and binary backup.
