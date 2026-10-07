# Coding standards

These conventions hold across the codebase, but you would have to read many files to find them. rustfmt and
Clippy (`-D warnings`) handle formatting and lints; `AGENTS.md` has the commands.

## Layout and visibility

- The crate uses edition 2024 on Rust 1.98.1, and the code uses let-chains (`if let … && …`).
- A module with submodules is a `foo.rs` file next to a `foo/` directory. A few older roots still use `mod.rs`
  (`agent`, `controller`, `dashboard`, `session_transfer`, `test_support`).
- Modules are private. `lib.rs` declares `mod x;` and re-exports only a few items at the root (`Cli`,
  `run_with_stdio`, the process-runner types, `WorkerError`). `session_transfer` is the only `pub mod`. When an
  integration test needs a new item, add a re-export to the test facade instead of making a module public.
- Test doubles for a domain go in a `testing` submodule gated with `#[cfg(any(test, feature = "test-support"))]`
  (`src/integration/testing.rs`, `src/session_transfer/testing.rs`).

## Errors

- The crate has one error type: `WorkerError` in `src/error.rs`, built with thiserror. Functions return
  `Result<T, WorkerError>`. There is no `Result` alias and no anyhow.
- Every failure carries a public code: a `&'static str` in SCREAMING_SNAKE_CASE (checked by
  `is_stable_public_code`), often a named const such as
  `pub const DAG_PARENT_FAILED: &str = "DAG_PARENT_FAILED";`. Codes are public API, listed in the "Exit codes and
  errors" section of `docs/usage.md`, so keep existing codes unchanged.
- Exit statuses follow sysexits (`ExitKind`): 64 usage, 69 unavailable, 70 infrastructure, 74 I/O, 75 capacity.
  The code decides the kind (`task_exit_kind`, `git_exit_kind`, `agent_exit_kind`).
- Public text is redacted by design. `public_message()` prints a `'static` literal and replaces an owned
  (formatted) string with a generic phrase. Write operator-facing messages as literals with
  `WorkerError::task(code, "…")` or `WorkerError::capacity(code, "…")`. A message built around a path, prompt,
  environment value or remote message stays private.
- To add a code to the catalog, add a `CATALOG` entry (code, exit status, static hint) and the matching row in the
  `docs/usage.md` table, in the same position.
- When host I/O fails at a known stage, attach a `FailureReceipt` with `with_host_io_stage`.

## Untrusted text and secrets

- Treat agent output, host replies and remote messages as untrusted. Before storing or printing them, cap their
  size with the `MAX_*` limits and redact them with `src/redaction.rs`.
- Types that hold tokens, argv, environment or prompts have hand-written `Debug` impls that print counts or
  `[REDACTED]` instead of the values (`LeaseToken`, `CommandSpec`, `SubmitRequest` in `src/job.rs`,
  `ProcessRequest` in `src/process.rs`).

## Side effects behind traits

- Processes, clocks, process inspectors, launchers and browsers sit behind traits, each with a `System*` production
  implementation: `ProcessRunner`/`SystemProcessRunner`, `ProcessInspector`/`SystemProcessInspector`,
  `Clock`/`SystemClock`, `BrowserOpener`/`SystemBrowserOpener`. Accept the trait (`&dyn ProcessRunner`) so a test
  can pass a fake.
- Bounded child processes (ssh, git, agent CLIs) go through a `ProcessRequest` with a `ProcessPolicy`, which sets
  stdout and stderr byte limits and a deadline.
- Host-side files go through `RootedDir` (`src/rooted_fs.rs`). It opens paths relative to directory descriptors
  with `O_NOFOLLOW`, keeps private modes, and refuses any path below a directory that others can write to. Durable
  writes are atomic and fsynced (`write_private_atomic_no_replace`, `atomic_write_at`).

## Wire and on-disk records

- Records derive serde with `#[serde(deny_unknown_fields)]`. Enums use `rename_all = "snake_case"`, plus
  `tag = "kind"` when tagged.
- Make a new field optional: `#[serde(default, skip_serializing_if = "Option::is_none")]`. Older records then
  still load, and an unset field is not written. Because of `deny_unknown_fields`, an older binary rejects a record
  where the field is set, so state in a comment what an older peer does (for example
  `/// Absent on older helpers.`).
- `PROTOCOL_VERSION` in `src/protocol.rs` (currently 7) versions the protocol between the queue owner and hosts.
  `tests/support/baseline_ce7f62f.rs` freezes the protocol-7 codecs from an older commit for N-1 decode tests.
  Keep that file independent of `mac_worker`.

## Test hooks in production code

- Races and crash points are made deterministic with hooks in production code: thread-local slots, `with_*_hook`
  helpers and write-fault points (for example `HostStore::with_root_entry_check_hook`,
  `migrate_layout_with_write_fault`). The production defaults stay unchanged.
- A `#[cfg(test)]` hook is reachable only from unit tests in the same crate. Integration tests compile the library
  without `cfg(test)` and can only reach items gated with `feature = "test-support"`. That feature exposes the
  facade and turns on no instrumentation; SSH and tunnel timing fixtures still depend on `debug_assertions`.

## Comments

- Comments explain why: the race, the invariant, what an older peer does, which case is still refused. Write one or
  two full sentences directly above the code they explain.
- A doc comment on an optional field says what an absent value or `None` means.
- Use a `//!` module doc where the module's contract is subtle (`src/turn_runner.rs`, `src/test_sync.rs`).

## CLI

- Every public clap argument has help text; `every_public_argument_has_help` in `tests/cli/cli_help.rs` checks this.
- Host-side and internal subcommands are marked `#[command(hide = true)]`.

## Naming

- Command inputs and outputs are named `*Request`, `*Response` and `*Report`. Types that hold a lock are named
  `*Guard`.
- A test name is a sentence that states the behavior, for example
  `close_lost_response_after_remote_success_retries_to_closed`.

## Tests

Before you write or change a test, read `docs/testing.md`. It covers the layout and commands, and its "Writing
tests" rules cover sleeps and wall-clock bounds, injected clocks, fixture pids, fake agent CLIs and per-test temp
directories.

Conventions beyond that file:

- Unit tests go in `#[cfg(test)] mod tests` at the end of the file, or in a sibling `tests.rs` for a large module.
  They import `crate::…`.
- A new integration test module goes in an existing `tests/<area>/` directory and is declared in that area's
  `main.rs`. It imports from `mac_worker::test_support::<domain>`. If it needs a new item, add an explicit
  `pub use` to `src/test_support/<domain>.rs`.
- Shared fixtures live in `tests/support/` (`GitRepo`, `RecordingRunner`, `task_harness`, `fake_herdr`,
  `fixture_pid`). Each area root pulls them in with `#[path = "../support/…"]`.
- `RecordingRunner` returns scripted `ProcessRunner` results, so a test can drive ssh and git callers without a
  real host.
- When a test needs its own process environment (`HOME`, environment variables), split it in two. The body is
  marked `#[ignore = "subprocess body: run by its *_wrapper test with the fixture environment"]` and returns early
  when `skip_unless_subtest()` is true. A `<name>_wrapper` test runs it again in a child process with
  `agent_launch_fixture::assert_subprocess_success(&support::libtest_name(module_path!(), "<name>"), env, env_clear)`.
- macOS checks every new executable on its first run, which can take seconds under load. Run a new fixture script
  once (for example with a `--warm` argument that exits 0) before a time-limited probe uses it.
- `.unwrap()` is normal in tests.
- Put heavy repetition in a `_stress` copy of the test marked `#[ignore = "stress: …"]`. The ordinary test runs
  each distinct case once.
- When a test is legitimately slow, give it an override in `.config/nextest.toml` and record the measured timings
  in the override's comment. Keep the global timeout as it is.
- Recorded samples (agent JSONL output, herdr schema, turn logs, legacy records) live in `tests/fixtures/`.
