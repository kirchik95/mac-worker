# Testing

The suite has about 4,500 tests: the library's unit tests and nine integration test binaries grouped by product
area. Most of its cost is filesystem sync and real processes, not CPU.

Cargo discovers each `tests/<area>/main.rs` as an integration target: `agents`, `cli`, `controller`, `dashboard`,
`host`, `scheduler`, `setup`, `task`, and `transfer`. Former test files are modules within those targets, so a
test such as `task_turn::setup_and_agent_share_one_total_turn_budget` runs in the `task` binary. Shared fixtures
remain in `tests/support/` and are declared once in each area's root.
New integration test modules go in an existing area and must be declared in that area's `main.rs`.

Integration tests import implementation contracts through `mac_worker::test_support`. Its explicit
domain namespaces include `core`, `runtime`, `cli`, `client_state`, `controller`, `channel`, `events`,
`host`, `task`, `transfer`, `dashboard`, and `agents`. Use the CLI fixture accessors and `from_parts`
constructor instead of accessing `Cli` fields. Source unit tests keep importing `crate::...` so their
types belong to the unit-test crate instance.

The non-default `test-support` feature is enabled by the package's self dev-dependency. Cargo test
targets therefore compile the facade into the integration library and the spawned `worker` binary;
ordinary builds leave the feature disabled. The permanent fixture probe checks both artifacts with a
cleared child environment and `--version`:

```sh
CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=6 cargo nextest run --locked --test cli \
  -E 'test(test_support::spawned_worker_has_test_support_feature)'
```

Existing SSH and tunnel timing fixtures still depend on `debug_assertions`. Run their process tests
with the existing debug profiles; enabling `test-support` does not enable those hooks in release profiles.

Implementation modules remain private in both modes. Ordinary callers use the narrow library-root
boundary: `Cli`, the process runner and its signature types, `run_with_stdio`, and the prepare-turn
helper entries. The CLI fields and runtime fixture entries are accessible through the facade only.

All-target Clippy enables the self dev-dependency's support feature. Check the ordinary production
graph separately, without dev targets, and build the release without that feature:

```sh
CARGO_BUILD_JOBS=4 cargo clippy --locked --release --no-default-features --lib --bin worker -- -D warnings
CARGO_BUILD_JOBS=4 cargo build --locked --release --no-default-features
```

The CLI lint fixture compiles metadata in both feature modes and verifies that an unused helper in
a private owner is diagnosed as `dead_code`. It also checks that explicit facade exports do not
exempt unrelated helpers from the lint.

## Commands

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
scripts/test-gate.sh                       # the whole suite, before landing
```

`scripts/test-gate.sh` runs `cargo nextest run --locked --all-targets` with `TMPDIR` on a temporary RAM disk and
passes any extra arguments through. It needs cargo-nextest (`brew install cargo-nextest`).
Recorded full runs took 11 to 26 minutes, so run it once per change rather than after every edit.

- `MAC_WORKER_GATE_RAMDISK_MB` sets the RAM disk size in MB (default 4096).
- `0` keeps the normal `TMPDIR`.

While you work, run only the binaries you touched:

```sh
CARGO_BUILD_JOBS=4 cargo nextest run --locked --test <area>
CARGO_BUILD_JOBS=4 cargo nextest run --locked --test task -E 'test(/^task_turn::/)'
CARGO_BUILD_JOBS=4 cargo nextest run --locked --lib -E 'test(/<module>::/)'
```

**Integration binaries must run under nextest, or serially under plain Cargo:**

```sh
CARGO_BUILD_JOBS=4 cargo test --locked --test task -- --test-threads=1
```

Tests in different modules can change process-wide state such as the current directory or environment. Their
existing module-local locks do not synchronize an entire area binary. Do not run plain integration targets with
multiple test threads, even when selecting a subset of modules.

## Persistent controller read channel

The channel tests remain modules of the existing `controller`, `transfer`, and `cli` integration
targets. Use six nextest workers and four Cargo build jobs for these focused selections:

```sh
NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_socket_contracts::/)'
NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_socket_codec::/)'
NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_socket_service::/)'
NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_socket_identity::/)'
NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_socket_client::/)'
NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test controller -E 'test(/^controller_socket_wiring::.*(loop|raw|route)/)'
NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test transfer -E 'test(/^controller_socket_forward::|^transport::/)'
NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test cli -E 'test(/^controller_channel::|^cli_help::/)'
```

The final track runs recorded 13/13 contracts, 33/33 codec, 28/28 service, 33/33
identity, 43/43 client, 16/16 loop/raw/route wiring, 20/20 integrated forward/transport,
and 39/39 channel CLI/help tests. Counts can grow as cases are added; a filter selecting zero tests
is always an error. The broader compatibility filters and exact per-track counts are retained in
the [validation record](superpowers/validation/2026-10-01-controller-socket.md).

The paired transport observation is intentionally ignored by the ordinary gate. Run it alone,
with captured JSON output, when changing the channel's lifecycle or measurement fixture:

```sh
NEXTEST_TEST_THREADS=6 CARGO_BUILD_JOBS=4 NEXTEST_PROFILE=ci NEXTEST_RETRIES=0 \
  cargo nextest run --locked --test controller --run-ignored only --no-capture \
  -E 'test(/^controller_socket_benchmark::fixture_transport_cost_observations$/)'
```

The accepted run selected one test, passed one, and emitted 18 observation rows from 10 warmups
and 200 paired samples per class. Its local fake SSH/mux timings are observations, not a latency
threshold or a substitute for live acceptance.

When a fixture re-executes its test binary, pass `support::libtest_name(module_path!(), "test_name")` to
`--exact` or `support::agent_launch_fixture::assert_subprocess_success`. The helper drops the crate component
and preserves all module components, including nested fixture modules.

## Why nextest and a RAM disk

- **Isolation.** nextest runs every test in its own process. A test that changes the working directory, `HOME` or
  another process-wide setting cannot disturb its neighbours.
- **Parallelism.** Tests from different binaries run in parallel. Plain `cargo test` runs one binary at a time.
- **RAM disk.** The host and client stores `fsync` on every durable write. On a RAM disk the heaviest binaries run
  about twice as fast.
- **The volume root.** A fresh APFS volume root is group-writable, and the host code refuses paths under a directory
  that others can write to. The script therefore makes the volume root and `TMPDIR` private.

`Cargo.toml` sets `debug = "line-tables-only"` for the dev profile. Backtraces keep file and line numbers, and
`target/` shrinks by several gigabytes. The gate also sets `CARGO_INCREMENTAL=0`, because its build is thrown away.

## Configuration

`.config/nextest.toml` has two profiles:

- **`default`** reports a test as slow after 60 s and kills it after 3 minutes. It never retries and does not stop
  at the first failure. The native-integration group, both host task-turn matrices, the publication-crash frontier
  test, and the envelope-pruning unit test are killed after 360 s instead.
- **`ci`** is the same with one retry and a JUnit report.

Two test groups limit how many tests run at once:

- **`stress`**: one at a time, for the `_stress` tests.
- **`fork_heavy`**: four at a time, for the `supervisor` and `job_queries` modules in `host` and the `task_turn`
  module in `task`, which fork, signal and reap real process groups against deadlines. The filter is scoped to
  those binaries so similarly named library unit tests keep their existing scheduling.

## Stress tests

Heavy repetition lives in tests whose names end in `_stress`. They are `#[ignore = "stress: ..."]` and run nightly:

```sh
scripts/test-gate.sh --run-ignored only -E 'test(/_stress$/)'
```

The ordinary version of each test keeps every distinct case once. A matrix of 100 iterations that repeated four
schedules now runs each schedule once. The stress version keeps the old counts, such as 1,000 source mutations or 50
concurrent clients, and the real 10 s supervisor TERM grace.

## Writing tests

- **Do not sleep and do not measure wall-clock time to prove ordering.** Wait for an event instead: a channel, a
  concurrency hook, or a public status.
- **Lower bounds are fine** (for example, the TERM grace elapsed). **Upper bounds on elapsed time fail on a loaded
  machine.** Prove the outcome instead, such as the process group being gone.
- **When only time tells two outcomes apart, make the slow outcome much slower.** Give the fixture a long sleep or
  timeout, and put the bound well between the two. For example, a cancel test gives setup a 120 s timeout and allows
  60 s.
- **`exec` the last long-running command in a fixture script** (`exec sleep 60`). A fork that races the kill of the
  process group can survive it and hold the output pipes open.
- **Inject time instead of waiting for it:**
  - `SupervisorTimings::fast()`, `Supervisor::with_term_grace`;
  - `ClientStateTimings::fast()`;
  - `ClientStateStore::with_liveness_clock`, `with_admission_clock` and `advance_liveness_clock`;
  - `note_confirmed_runner_absence`.

  The production defaults do not change.
- **Fixture process identities with made-up pids** are built with `fixture_pid` (`FIXTURE_PID_BASE` in
  `tests/support/fixture_pid.rs` and `src/fixture_pid.rs`), so the pid cannot exist. A fake `ProcessInspector`,
  for example `AbsentOwnerInspector` in `tests/scheduler/run_command.rs` or `LiveSetInspector` in
  `tests/scheduler/scheduler_queue.rs`,
  is how a test chooses Alive, Reused, or Ambiguous. With the system inspector and a pid in the real range:
  - the result depends on whichever real process holds that pid at the moment;
  - a root-owned process reads as `Ambiguous`, which never confirms absence.
- **Never run a real agent CLI** (`codex`, `claude`, `cursor-agent`, `opencode`) from a test. Put a fixture script
  first on `PATH` through the temporary `HOME`'s `.zprofile`, because login shells rebuild `PATH`.
- **Put every durable path under the test's own temporary directory.** Parallel processes share nothing else.

## CI

- **Pushes and pull requests** run formatting, all-target and production Clippy, and the library and binary unit tests.
- **The full suite** runs nightly and on manual dispatch, through `scripts/test-gate.sh --profile ci` without a RAM
  disk, followed by the stress tests.
- Both CI tiers and release verification check the production graph separately from support-enabled targets.
