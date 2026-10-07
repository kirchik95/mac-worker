# Coding standards

## Modules

- Give a new parent module a `foo.rs` file beside its `foo/` directory.
- Keep modules private. When an integration test needs an item, add an explicit `pub use` to
  `src/test_support/<domain>.rs`.
- Put a domain's test doubles in a `testing` submodule gated with `#[cfg(any(test, feature = "test-support"))]`
  (`src/integration/testing.rs`).

## Errors

- Return `Result<T, WorkerError>` (`src/error.rs`). Every failure carries a public code in SCREAMING_SNAKE_CASE.
  Codes are public API, so keep existing codes as they are.
- A new code needs a `CATALOG` entry (code, exit status, static hint) and the matching row, in the same position, in
  the table between `<!-- error-catalog:start -->` and `<!-- error-catalog:end -->` in `docs/usage.md`.
- Write operator-facing messages as string literals, with `WorkerError::task(code, "…")` or
  `WorkerError::capacity(code, "…")`. `public_message()` prints a `'static` literal and replaces a formatted string
  with a generic phrase, so a message built from a path, prompt, environment value or remote message stays private.
- When host I/O fails at a known stage, attach a `FailureReceipt` with `with_host_io_stage`.

## Untrusted text and secrets

- Agent output, host replies and remote messages are untrusted. Before storing or printing them, cap them with the
  `MAX_*` limits and redact them with `src/redaction.rs`.
- Give a type that holds tokens, argv, environment or prompts a hand-written `Debug` that prints counts or
  `[REDACTED]` (`CommandSpec` in `src/job.rs`).

## Side effects

- Take processes, clocks, process inspectors and browser openers as traits (`&dyn ProcessRunner`), each with a
  `System*` production implementation, so a test can pass a fake.
- Run a bounded child process (ssh, git, an agent CLI) through a `ProcessRequest` whose `ProcessPolicy` caps stdout,
  stderr and time.
- Open host-side files through `RootedDir` (`src/rooted_fs.rs`), which opens paths relative to directory descriptors
  with `O_NOFOLLOW`, keeps private modes, and refuses any path below a directory that others can write to. Make
  durable writes atomic and fsynced (`write_private_atomic_no_replace`, `atomic_write_at`).

## Wire and on-disk records

- Records use `#[serde(deny_unknown_fields)]`, and the queue owner and hosts can run different versions, so an older
  binary rejects a record that sets a field it does not know. Add a field as
  `#[serde(default, skip_serializing_if = "Option::is_none")]`, and say in its doc comment what `None` means and
  what an older peer does (`/// Absent on older helpers.`).
- `tests/support/baseline_ce7f62f.rs` freezes the previous protocol's codecs for N-1 decode tests. Keep it
  independent of `mac_worker`.

## Comments and CLI

- Comments state why: the race, the invariant, what an older peer does, which case is still refused. Write one or
  two full sentences directly above the code they explain.
- Give every public clap argument help text (`every_public_argument_has_help` checks it), and mark host-side and
  internal subcommands `#[command(hide = true)]`.

## Tests

Before you write or change a test, read `docs/testing.md`. Its "Writing tests" rules cover sleeps and time bounds,
injected clocks, fixture pids, fake agent CLIs and temporary directories. In addition:

- Name a test with a sentence that states the behaviour, for example
  `close_lost_response_after_remote_success_retries_to_closed`.
- Make races and crash points deterministic with hooks in production code (thread-local slots, `with_*_hook`
  helpers, write-fault points such as `HostStore::with_root_entry_check_hook`) whose production defaults stay
  unchanged. Integration tests compile the library without `cfg(test)`, so gate a hook they need with
  `#[cfg(any(test, feature = "test-support"))]`.
- Drive ssh and git callers with `RecordingRunner` (`tests/support/recording_runner.rs`), which returns scripted
  `ProcessRunner` results.
- When a test needs its own process environment (`HOME`, environment variables), split it in two. The body is
  marked `#[ignore = "subprocess body: run by its *_wrapper test with the fixture environment"]` and returns early
  when `skip_unless_subtest()` is true. A `<name>_wrapper` test runs it in a child process with
  `agent_launch_fixture::assert_subprocess_success(&support::libtest_name(module_path!(), "<name>"), env, env_clear)`.
- macOS checks every new executable on its first run, which can take seconds under load. Run a new fixture script
  once (a `--warm` argument that exits 0) before a time-limited probe uses it.
- Give a legitimately slow test an override in `.config/nextest.toml`, with its measured timings in the override's
  comment, and keep the global timeout as it is.
