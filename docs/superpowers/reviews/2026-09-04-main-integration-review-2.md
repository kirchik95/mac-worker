# Main integration review follow-up - 2026-09-04

## Scope and outcome

Reviewed the integrated `main` baseline across Phase 4, the Phase 5 host
transport and turn work, cached agent facts, and dashboard Tasks 7, 8, and 10.
The review used the task-pool and dashboard specifications, the two earlier
review reports, dispatch and error boundaries, and the relevant focused test
suites. Closed findings from the earlier reports are not repeated here.

The review found six issues: two major findings and one minor finding were
fixed on this branch; three major findings remain deferred because their
correct repairs require files the brief protects. The required dashboard suites
passed outside the restricted terminal sandbox, where the loopback listener is
permitted to bind.

## Findings

1. **Major - fixed** - `src/scheduler_adapter.rs:53-57,82-91` -
   `probe.facts_age_millis`

   Quote: `let (Some(facts), Some(facts_age_millis)) = (facts,
   facts_age_millis) else { return; };`.

   Scenario: the scheduler derived freshness from the client wall clock and a
   worker's `collected_at` timestamp. A worker clock ahead of the client made
   an expired fact cache appear fresh, so it could advertise an unavailable
   agent or profile capability for admission.

   Fix: consume the age measured by the worker, require that age whenever
   facts are projected, and reject it after the facts TTL. Regressions cover
   a clock-ahead worker, a missing age, stale facts, and insecure profiles.
   Commit `ee4c043`.

2. **Major - fixed** - `src/dashboard/source.rs:197-205` -
   `let observed_at_millis = current_time_millis();`

   Quote: the timestamp is now obtained after `self.workers.inspect(...)`
   returns.

   Scenario: a successful worker collection that took longer than the
   dashboard observation TTL was stamped at collection start. On the next
   failed refresh, the just-completed result was already too old for the
   stale-cache fallback and the dashboard reported the worker offline.

   Fix: stamp observations at winner-refresh completion. A delayed collection
   regression proves the stored observation cannot predate the completed
   collection. Commit `8010652`.

3. **Minor - fixed** - `src/transport.rs:569-595` -
   `match serde_json::from_str::<ProbeResponse>(response)`

   Quote: the complete current probe shape is decoded before either older
   fallback shape.

   Scenario: a real Phase 4 protocol-3 fixture includes
   `supervision_version`. The previous fallback decoders rejected that known
   field and converted a safe protocol mismatch into `INVALID_RESPONSE`,
   hiding the peer version and making setup diagnostics misleading.

   Fix: parse a complete probe regardless of its protocol version, then use
   the strict legacy decoders only when the full shape is absent. The fixture
   now produces `PROTOCOL_MISMATCH` while retaining protocol 3 and supervision
   2 in the typed observation. Commit `a49edea`.

4. **Major - deferred** - `src/cli.rs:74-136` - `#[command(hide = true)]`
   only on `Command::Host`

   Quote: `Host { #[command(subcommand)] command: HostCommand }`.

   Scenario: the public root help hides `host`, but `worker host --help`
   lists every internal operation, including control, task, migration, and Git
   transport helpers. Those commands remain reachable from
   `src/lib.rs:618-815`, so this is a help-surface disclosure rather than a
   dead-command issue.

   Deferred plan: mark every `HostCommand` variant hidden (or apply the
   equivalent supported Clap enum configuration that preserves parsing and
   dispatch), and add a `worker host --help` regression proving none of the
   internal names are listed while direct helper parsing still works. The
   needed source and help test are protected by the brief.

5. **Major - deferred** - `src/client_state.rs:538-586,1717-1727,2865-2899`
   and `src/dashboard/queue.rs:52-56`

   Quote: `cvt(unsafe { libc::flock(marker.as_raw_fd(), libc::LOCK_EX) })?`.

   Scenario: the dashboard synchronously calls
   `queue_rows_with_blocking_reasons`, which takes the same blocking state and
   queue locks used by a scheduler or runner. A paused lock holder can hold
   the dashboard refresh leader beyond its global deadline; subsequent
   requests wait for that leader and eventually fall back, while the leader
   remains blocked indefinitely. RAII releases the lock after an ordinary
   read error, so this is blocking/starvation rather than lock poisoning.

   Deferred plan: add a nonblocking or deadline-bounded client-state queue
   projection that acquires both locks with `LOCK_NB` (or an equivalent timed
   operation) and returns a stable busy code. Have the dashboard turn that
   code into its existing stale/partial fallback within the snapshot deadline.
   Add held-lock, bounded-response, and subsequent-success regressions. The
   required client-state locking API is protected by the brief.

6. **Major - deferred** - `src/job_service.rs:736-810` -
   `self.cleanup_and_release_reconciled(lease, &job, &cancelled)?`

   Quote: both live-cancellation paths persist `Cancelled` and then call
   cleanup/release without a preceding `TurnTerminalHook::invoke`.

   Scenario: cancelling an accepted turn can delete its execution payload and
   release its lease while the task remains Active. Its terminal outcome,
   session continuity, and result-branch publication are then skipped. The
   reconciliation paths at `src/job_service.rs:1555-1596` and `1658-1678`,
   and the supervisor terminal writer, already invoke the hook before cleanup;
   live cancellation is the remaining terminal path.

   Deferred plan: after durably writing `Cancelled` and before deleting
   `execution.json`, cleanup, or lease release, load and validate the turn
   section and invoke `TurnTerminalHook::invoke` with
   `TurnTerminal::Cancelled` and `TerminalPath::HostCancel`. Cover
   cancel-then-resume end to end: the task becomes Open, the session survives,
   the result branch is published, and the lease releases afterward. The
   cancellation writer is protected by the brief.

## Seam checks without a further finding

- The setup verification script runs `migrate-layout`, then `refresh-facts`,
  then the final probe (`src/install.rs:790-799`). Failure of either new
  helper fails verification and rolls back the promoted helper; setup records
  `VERIFICATION_FAILED`, preserving I/O classification when the probe launch
  itself fails.
- All hidden helper variants have a live route in the rebased dispatch. Probe
  intentionally reaches the normal execution boundary; the remaining
  variants are explicitly routed before it (`src/lib.rs:618-815`).
- `worker status` and the dashboard both use the same
  `queue_rows_with_blocking_reasons` projection, filter waiting rows, preserve
  FIFO position, and map the same blocking reasons. Queue runner ownership
  stays in the durable queue entry and is intentionally absent from the safe
  dashboard DTO.
- Reconciliation of terminal pending turns invokes the terminal hook before
  cleanup; only the live cancellation writer above misses that contract.
- The reviewed task, queue, fact, and dashboard projections do not serialize
  prompt bodies, profile values, SSH destinations, local paths, or runner
  identities. The loopback dashboard's observed-hostname field is an explicit
  pre-existing dashboard contract, not a new task-record field.

## Verification after fixes

For each fix, the focused regression was first observed failing, then passed
after the change. Each fix also passed `cargo fmt --check`, its touched test
suite, `cargo clippy --locked --all-targets -- -D warnings`, and
`git diff --check`.

The dashboard suites passed outside the restricted sandbox:

- `cargo test --locked --test dashboard_model --test dashboard_cache --test dashboard_service --test dashboard_web --test dashboard_source --test dashboard_queue --test dashboard_command --test dashboard_cpu_adapter`

The final all-target gate also passed outside the restricted sandbox:

- `cargo test --locked --all-targets`
