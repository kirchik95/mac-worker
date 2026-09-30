# Testing

The suite has about 3,000 tests: the library's unit tests and one integration test binary per file in `tests/`.
Most of its cost is filesystem sync and real processes, not CPU.

## Commands

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
scripts/test-gate.sh                       # the whole suite, before landing
```

`scripts/test-gate.sh` runs `cargo nextest run --locked --all-targets` with `TMPDIR` on a temporary RAM disk and
passes any extra arguments through. It needs cargo-nextest (`brew install cargo-nextest`).

- `MAC_WORKER_GATE_RAMDISK_MB` sets the RAM disk size in MB (default 4096).
- `0` keeps the normal `TMPDIR`.

While you work, run only the binaries you touched:

```sh
CARGO_BUILD_JOBS=4 cargo test --locked --test <binary>
cargo test --locked --lib <module>::tests::
```

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
  at the first failure.
- **`ci`** is the same with one retry and a JUnit report.

Two test groups limit how many tests run at once:

- **`stress`**: one at a time, for the `_stress` tests.
- **`fork_heavy`**: four at a time, for the `supervisor`, `job_queries` and `task_turn` binaries, which fork, signal
  and reap real process groups against deadlines.

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
  for example `AbsentOwnerInspector` in `tests/run_command.rs` or `LiveSetInspector` in `tests/scheduler_queue.rs`,
  is how a test chooses Alive, Reused, or Ambiguous. With the system inspector and a pid in the real range:
  - the result depends on whichever real process holds that pid at the moment;
  - a root-owned process reads as `Ambiguous`, which never confirms absence.
- **Never run a real agent CLI** (`codex`, `claude`, `cursor-agent`, `opencode`) from a test. Put a fixture script
  first on `PATH` through the temporary `HOME`'s `.zprofile`, because login shells rebuild `PATH`.
- **Put every durable path under the test's own temporary directory.** Parallel processes share nothing else.

## CI

- **Pushes and pull requests** run formatting, clippy and the library and binary unit tests.
- **The full suite** runs nightly and on manual dispatch, through `scripts/test-gate.sh --profile ci` without a RAM
  disk, followed by the stress tests.
