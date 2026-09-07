# First-run onboarding Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Install mac-worker without Rust and connect a first worker without editing TOML.

**Architecture:** Add a focused onboarding service over the existing installer and SSH/agent probes. Keep packaging independent from runtime code. Use a one-worker README and separate operational references.

**Tech Stack:** Rust, clap, serde/toml, existing ProcessRunner, POSIX shell, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-09-08-onboarding-design.md`

## Global Constraints

- First supported onboarding/release target: macOS on Apple Silicon on both controller and worker.
- Preserve existing setup/task behavior, inventory entries and comments.
- No credentials or remote stderr in onboarding reports; no automatic account or package-manager changes.
- Use existing installer, transport and agent probe implementations.
- Do not publish releases, push shared branches, change real workers, or touch the user's untracked `mac-worker-architecture.html`.

### Task 1: Resumable init

Files: create `src/onboarding.rs`, `tests/init_command.rs`; modify `src/cli.rs`, `src/lib.rs`, `src/output.rs`, `src/config.rs`.

- [x] Add failing CLI/behavior tests using `Cli::try_parse_from(["worker", "init", "alice@mini.local"])`, isolated RuntimeContext, real temporary files, and recorded ProcessRunner responses. Assert missing-agent exit is nonzero, configuration remains readable and reruns do not duplicate workers; failed SSH/platform checks do not create inventory.
- [x] Implement the service returning a serializable `InitReport` with `ready`, worker identity, stages and next steps. Append validated worker blocks to the original TOML text under a lock and atomic replacement; preserve existing worker names on retry.
- [x] Wire `Command::Init` through existing output and exit handling. Validate `--name`, `--agent`, `--env-profile` before executing remote commands. Improve public command help and missing-config guidance.
- [x] Run `cargo test --locked --offline --test init_command --test setup_command --test workers_command --test cli_help --test doctor_command` and fix regressions.

### Task 2: Binary distribution

Files: create `install.sh`, `scripts/package-release.sh`, `scripts/test-install.sh`, `.github/workflows/release.yml`, `docs/releasing.md`, and packaging files if required. Own these paths only; do not modify Cargo or onboarding/README files.

- [x] Inspect Cargo and embedded dashboard build, verify official GitHub/Homebrew distribution documentation as needed, and choose consistent versioned archive/checksum naming for macOS arm64.
- [x] Write executable installer fixture tests before implementation, covering successful verified installation into a fresh prefix, bad checksum, unsupported platform, and preservation of an existing binary on download/verification failure. Use local fixtures/stubbed networking, not real user configuration.
- [x] Implement an installer that fetches a tagged release, verifies SHA-256, creates destination directories and installs atomically, with clear PATH guidance. Support version selection and a custom installation directory; provide no Rust or Node prerequisite.
- [x] Implement a packaging script producing a release tarball, checksum and version-specific Homebrew formula, and a tag-driven workflow that runs checks and creates draft release assets. Do not pretend a Homebrew tap is already public. Build scripts must reject mismatched version tags and incompatible architectures.
- [x] Run installer fixtures and shell syntax checks. Produce a local native release archive, inspect it, and write release/publishing instructions. Document remaining external publication steps precisely.

### Task 3: Onboarding documentation and final validation

Files: modify `README.md`, `docs/setup-macos-worker.md`, `config.example.toml`; create `docs/usage.md`, `docs/setup-recovery.md` as needed.

- [x] Move operational recovery out of first-run instructions, preserving its exact commands. Rewrite the setup guide around one account, enabling SSH, generating/installing a key, verifying host trust, one agent, and rerunning init.
- [x] Present installation, `worker init user@mini.local`, a small first task and fetch before advanced features. Only advertise distribution commands that will be valid once the described release is published, with source-build fallback today.
- [x] Document agent/profile options, upgrades, uninstall scope, and checked platform support. Remove obsolete phase claims and personal three-worker assumptions.
- [x] Run Rust formatting/full tests, build a release, validate docs/CLI consistency, and review runtime plus packaging changes. Record all limitations without claiming a clean-Mac or public-release test that was not performed.

Final validation: `cargo test --locked --offline --all-targets --no-fail-fast` exited 0 (loopback tests required sandbox escalation); `cargo clippy --locked --offline --all-targets -- -D warnings`, `cargo fmt --all --check`, installer/package fixtures (10 tests), shell syntax, workflow YAML and generated formula syntax checks passed. Native release archive rebuilt after runtime fixes and verified by checksum, extraction and local installation. Independent review findings were fixed and re-reviewed. Documentation links/anchors and preserved recovery procedures were checked. No clean-Mac authenticated end-to-end run or public release/tap publication was performed.
