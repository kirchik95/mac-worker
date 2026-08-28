# Budgeted Worker Probe Amendment

**Goal:** Give the dashboard and Phase 4 scheduler one shared, bounded worker-probe implementation without moving concurrency or deadline accounting into either adapter.

## Contract

- Add `SshTransport::probe_with_deadline(worker, deadline)`. A zero deadline does not call the runner and returns an unavailable row without a probe payload. A nonzero deadline is passed to `ProcessRunner` as `min(deadline, 15 seconds)`.
- Add `WorkersService::inspect_with_budget(config, budget)` and `inspect_with_requirements_and_budget(config, requirements, budget)`.
- Each budgeted inspection starts one monotonic fleet budget. Remaining time is recomputed before every probe and is never reset per host or batch.
- Run at most three probes concurrently. Arbitrary inventory sizes are claimed from one bounded worker pool; results are restored to configuration order.
- A panic in one probe becomes one bounded unavailable row without a probe payload. It does not unwind the inspection or discard peer results.
- Workers not started before budget expiry return unavailable rows without calling the runner.
- Requirement-aware inspection preserves the existing inventory-first stable capability union.
- `ProcessRunner` remains `Send + Sync` so one runner can safely serve the bounded pool.

## Compatibility and consumers

The existing public `WorkersService::inspect` and `inspect_with_requirements` methods retain their current fixed-policy behavior. Doctor, setup, and `worker workers` therefore keep their existing contracts. Dashboard Task 6 must implement its worker reader with `inspect_with_budget`; Phase 4 dispatch must use `inspect_with_requirements_and_budget`. Neither adapter may add a second host-level concurrency layer or restart the supplied budget per host.

## TDD and verification

Add worker-command tests for the exact deadline/cap and zero-call rules; three-way concurrency and maximum active count; out-of-order completion with configuration-order output; shrinking later-batch budgets; one-budget fleet expiry with skipped unstarted workers; panic isolation; stable requirement union; and compatibility behavior. Prefer destination-keyed runners and channel/barrier hooks over sleeps. Then run the focused worker, Doctor, setup, and process-runner suites followed by format, all targets, strict Clippy, and release build.
