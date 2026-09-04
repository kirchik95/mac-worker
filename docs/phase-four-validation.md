# Phase 4 validation record

This record separates automated fake-transport evidence from live observations of the configured Mac workers. It contains only commit/version data, shortened identifiers, aliases, counts, states, durations, exits, and fingerprints of mac-worker-owned namespaces. It intentionally omits raw probe payloads, raw command output and logs, complete paths, repository origins, credentials, and environment values.

## Commit and protocol

The reproducible validation revision is Task 8 commit `814f89526ce154e7afba5b1ea26993cfc564435d`, with client version `0.1.0`; the authorized `src/cli.rs` Clap help metadata is part of that commit. The isolated acceptance release was built from the same source contents immediately before the commit, with no source-affecting change between build and commit, so no separate binary digest is needed. SSH preflight reached all three aliases, but fresh setup returned the typed result `UNKNOWN_INSTALLATION_STATE` for `mini-1`/`mac1`, `mini-2`/`mac2`, and `mini-3`/`mac3`, with exit `70` for each worker and the aggregate command. No worker protocol version was accepted.

## Automated gate

The focused CLI-help RED→GREEN gate is automated local evidence and does not exercise live workers. It passed `11/11` tests after the minimal Clap metadata change. The final local gate also passed each command with exit `0`: `cargo fmt --all --check`; `cargo test --locked --all-targets` (`1,041` passed, `0` failed, `0` ignored); `cargo clippy --locked --all-targets -- -D warnings`; `cargo build --locked --release` (version `0.1.0`); and `git diff --check`.

## Three-worker setup

SSH preflight: `3/3` aliases exited `0`. Fresh setup returned `UNKNOWN_INSTALLATION_STATE` (exit `70`) for `mini-1`/`mac1`, `mini-2`/`mac2`, and `mini-3`/`mac3`; all three reported `installed=false` and no protocol version. The live attempt stopped immediately at this setup result. There was no retry, repair, cleanup, rename, deletion, or remote run.

## Automatic placement

Not proven: setup stopped the live attempt before any unpinned job was submitted; no distinct-lease count exists.

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
