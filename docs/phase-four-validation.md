# Phase 4 validation record

This record separates automated local evidence from live observations of the configured Mac workers. All live commands were executed directly from an isolated terminal environment. It contains only revision and version data, shortened identifiers, logical worker aliases, counts, states, result categories, and fingerprints of mac-worker-owned namespaces. It intentionally omits raw payloads, command output and logs, complete paths, repository origins, credentials, and environment values.

## Commit and protocol

The live acceptance release was built from `cc2939a5565c` (client version `0.1.0`, protocol version `3`). Direct SSH preflight reached all three configured logical workers. Fresh setup exited `0` for `3/3`: each installed the release helper, reported protocol version `3`, and became `ready`. The helper version was `0.1.0` on every worker and its release-binary identity matched the client build.

## Automated gate

The final local gate passed each command with exit `0`: `cargo fmt --all --check`; `cargo test --locked --all-targets`; `cargo clippy --locked --all-targets -- -D warnings`; `cargo build --locked --release`; and `git diff --check`. The automated suite is local evidence; the following sections record the separate live acceptance observations.

## Three-worker setup

The workers began in `ready`/`idle` state with no active lease and the required `darwin-arm64` capability. Setup installed the helper at protocol `3` on all `3/3` workers. Baseline mac-worker namespace fingerprints were recorded before submissions: `mini-1` `1f7e51668912cbb6` (`227` files), `mini-2` `e3b0c44298fc1c14` (`0` files), and `mini-3` `e3b0c44298fc1c14` (`0` files).

## Automatic placement

Three concurrent compatible bounded jobs were accepted as three distinct leases: `acec120c1069…` on `mini-1`, `f165ce99404c…` on `mini-2`, and `6c923a002645…` on `mini-3`. The fleet inventory concurrently reported all three workers `busy`, each with the corresponding distinct active job identifier. This demonstrates automatic placement across the three independent one-slot workers.

## FIFO fourth job

With all three slots held by pinned bounded jobs, a fourth compatible job, `14aff6a46428…`, entered the queue at position `1` with blocking reason `no_eligible_worker`. After the holding job on `mini-1` was cancelled, that original queued identifier was accepted on `mini-1`; no replacement submission was made. This is the observed fourth-job FIFO handoff.

## Pinned worker

With `mini-1` idle and `mini-2`/`mini-3` busy, the older job `317e657089ee…` pinned to `mini-2` remained queued at position `1` with reason `pinned_worker_busy`. A younger compatible unpinned job, `587afd0cb1a2…`, was accepted on the idle `mini-1`. The pinned row therefore waited for its target without blocking unrelated compatible capacity.

## No-wait capacity

While every slot was occupied, a compatible `--no-wait` submission exited `75` with result category `CAPACITY_BUSY`. It created neither an accepted lease nor a queue row: the pre-existing queue count remained `1` before and after the attempt. The relevant namespace fingerprints were unchanged across the attempt: `mini-1` `8bbb889f46f7098f`, `mini-2` `14a30e9c62e1b63d`, and `mini-3` `8f209073b0f2309a`.

## Cancellation

Waiting cancellation was exercised on queued job `cf7e6cef26ed…`: it reached the terminal `cancelled` state without a remote execution attempt. Running cancellation was exercised on accepted job `6c923a002645…`; the public cancellation command exited `0` and the job became terminal `cancelled`. Subsequent bounded cleanup cancellations left no active lease or queued row.

## Disconnect and reconciliation

For accepted job `ad4aefbe70a7…` pinned to `mini-1`, the local follower was terminated while its remote lease remained active. Reattaching to the job completed under the same original identifier, with `0` new acceptance events and one terminal status event.

For the one-worker outage observation, active jobs were held on all three workers and the follower for `mini-3` was terminated. After the bounded observation interval, that worker was reported unavailable with category `SSH_UNAVAILABLE`; a fleet status reconciliation recorded the active `mini-3` job as `running` with uncertainty `unknown_remote` and category `UNAVAILABLE`, rather than declaring it lost. Normal reachability was restored, all three jobs were cancelled, and the final inventory reported `3/3 ready`, `3/3 idle`, and no active lease.

## Cleanup and privacy

The final safe status had `0` queued, `0` active, and `19` terminal records, with no omitted rows. Final mac-worker namespace fingerprints were `mini-1` `f74db9eb97f15d86` (`419` files), `mini-2` `38eb8bd899a61ba4` (`170` files), and `mini-3` `00c8345d610fc8f6` (`157` files). The installed helper identity still matched the release build on every worker.

A scoped scan of retained job metadata and logs, excluding immutable snapshot trees, found `0` acceptance-marker hits and `0` generic absolute-local-path signatures across the retained record sets (`137`, `57`, and `47` files respectively). The isolated clone remained at `cc2939a5565c` with only its intentional marker change; its marker-diff fingerprint remained `8476d436dadee12e` and `git diff --check` passed. No source, origin, credential, environment, or raw probe data was retained in this record.

## Remaining boundary

This acceptance covers Phase 4's three-worker scheduling contract: automatic placement, FIFO waiting, pin behavior, no-wait admission, explicit cancellation, follower reconnection, and one-worker reconciliation. Dashboard work is Phase 4.5. Artifact transfer/fetch, package caches, Docker profiles, general garbage collection, and other later-phase controls remain outside this validation.
