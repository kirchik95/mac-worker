# First-run onboarding

Approved direction: the user accepted the installation assessment and recommendation in conversation on 2026-09-08.

## User outcome

A developer can install a prebuilt CLI, connect one Apple Silicon Mac with `worker init user@mini.local`, and receive a branch from a first task. No manually authored inventory is required. The existing SSH transport, transactional helper installer and agent fact probes remain authoritative.

## Scope

- A resumable `init` command accepts an SSH destination, optional inventory name, selected agent (Codex by default), and optional environment profile. It checks passwordless SSH and the remote platform before adding a worker. It preserves existing inventory entries and comments, refuses conflicting names and invalid configuration, and writes configuration atomically.
- Init installs the current helper using `Installer`, refreshes/probes the selected machine, checks the selected agent and profile, and renders staged readiness plus actionable instructions. A missing agent or login is a resumable blocked result, not success. It gives commands for installation/login on the remote account; it does not collect credentials, change Remote Login or run package-manager installers automatically.
- Reports support existing `--json` and reserved exit codes. Errors must not echo remote stderr or credential material. Installation warnings remain visible.
- First supported onboarding/release target: macOS on Apple Silicon on both controller and worker. Reject incompatible targets before uploading an executable. Existing setup/task behavior is preserved.
- Prebuilt release archives include the embedded dashboard, license and an installer with checksum verification. A tag-driven CI workflow validates and produces versioned assets; publishing is a separate explicit action. Prepare Homebrew distribution without claiming an unpublished tap is available.
- Rewrite the README around one worker and one successful task; move advanced commands and installation recovery into separate documents. Include clean-machine SSH preparation, agent installation/login, updating, and removal.

## Validation

Test init against real temporary configuration files and a recording process boundary: first install, retries, multiple workers, conflicting names, invalid input, failed SSH, unsupported host, failed helper installation, missing/unauthenticated agents, profiles, JSON/exit status, and configuration preservation. Use the existing installer tests for transactional promotion/rollback. Validate release scripts through controlled local fixtures and build a native release archive. Run formatting, relevant integration suites, full Rust tests, and review the resulting diff.

## Boundary

No deployment, remote-machine provisioning, credentials, shared-branch push, or public release is performed during implementation. These are user-visible operations of the delivered tool or later release actions.
