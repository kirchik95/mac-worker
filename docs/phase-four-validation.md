# Phase 4 validation record

This record separates automated fake-transport evidence from live observations of the configured Mac workers. It contains only commit/version data, shortened identifiers, aliases, counts, states, durations, exits, and fingerprints of mac-worker-owned namespaces. It intentionally omits raw probe payloads, raw command output and logs, complete paths, repository origins, credentials, and environment values.

## Commit and protocol

The isolated acceptance release was built from source commit `d971783d3b94` with client version `0.1.0` and the authorized Clap help metadata change. SSH preflight reached all three aliases, but fresh setup exited `70` for `3/3` workers before any acceptance case could start; no worker protocol version was accepted.

## Automated gate

The focused CLI-help RED→GREEN gate is automated local evidence and does not exercise live workers. It passed `11/11` tests after the minimal Clap metadata change. The final local gate also passed each command with exit `0`: `cargo fmt --all --check`; `cargo test --locked --all-targets` (`1,041` passed, `0` failed, `0` ignored); `cargo clippy --locked --all-targets -- -D warnings`; `cargo build --locked --release` (version `0.1.0`); and `git diff --check`.

## Three-worker setup

SSH preflight: `3/3` aliases exited `0`. Fresh setup: aliases `mini-1`/`mac1`, `mini-2`/`mac2`, and `mini-3`/`mac3` all reported `installed=false`, no protocol version, and the aggregate command exited `70`. The live acceptance stopped at this gate; no retry, repair, or worker-data cleanup was performed.

## Automatic placement

Not proven: setup did not complete, so no live unpinned jobs were submitted and no distinct-lease count exists.

## FIFO fourth job

Not proven: the live setup gate stopped before a fourth job could be submitted.

## Pinned worker

Not proven: the live setup gate stopped before a pinned or younger compatible job could be submitted.

## No-wait capacity

Not proven: the live setup gate stopped before slots could be occupied. No live `CAPACITY_BUSY` exit or before/after namespace comparison was attempted.

## Cancellation

Not proven: the live setup gate stopped before waiting or running cancellation could be exercised.

## Disconnect and reconciliation

Not proven: the live setup gate stopped before follower reconnect, one-worker outage, or reconciliation could be exercised. No cache value is treated as lease evidence.

## Cleanup and privacy

Not proven live: setup stopped before acceptance fingerprints or retained-job privacy checks could be taken. No raw payloads, logs, paths, origins, credentials, or environment values were recorded; the isolated clone was not used for a remote run.

## Remaining boundary

The local implementation and fake-transport tests cover automatic placement, per-worker FIFO, pins, no-wait admission, explicit cancellation, fleet reconciliation, and the one-slot remote lease contract. Live three-worker behavior remains not proven because fresh setup exited `70` with `3/3` workers uninstalled. Dashboard is Phase 4.5. Artifact transfer/fetch, package caches, Docker profiles, general garbage collection, and other later-phase controls remain outside this validation.
