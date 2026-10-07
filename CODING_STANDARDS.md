# Coding standards

## Errors

- `WorkerError` codes (`src/error.rs`) are public API: keep existing codes as they are.
- A new code needs a `CATALOG` entry (code, exit status, static hint) and, in the same position, a row in the
  `docs/usage.md` table between `<!-- error-catalog:start -->` and `<!-- error-catalog:end -->`.
- Write operator-facing messages as string literals (`WorkerError::task(code, "…")`, `WorkerError::capacity`).
  `public_message()` replaces a formatted string with a generic phrase, so a message built from a path, prompt,
  environment value or remote message stays private.
- When host I/O fails at a known stage, attach a `FailureReceipt` with `with_host_io_stage`.

## Untrusted text and secrets

- Cap agent output, host replies and remote messages with the `MAX_*` limits and redact them with
  `src/redaction.rs` before storing or printing them.
- Give a type that holds tokens, argv, environment or prompts a hand-written `Debug` that prints counts or
  `[REDACTED]` (`CommandSpec` in `src/job.rs`).

## Side effects

- Take processes, clocks, process inspectors and browser openers as traits (`&dyn ProcessRunner`), each with a
  `System*` production implementation, so a test can pass a fake.
- Run a bounded child process (ssh, git, an agent CLI) through a `ProcessRequest` whose `ProcessPolicy` caps stdout,
  stderr and time.
- Open host-side files through `RootedDir` (`src/rooted_fs.rs`): descriptor-relative, `O_NOFOLLOW`, private modes,
  and no path below a directory others can write to. Write durable files atomically and fsynced
  (`write_private_atomic_no_replace`, `atomic_write_at`).

## Wire and on-disk records

- The queue owner and hosts can run different versions, and records use `#[serde(deny_unknown_fields)]`, so an older
  binary rejects a field it does not know. Add a field as `#[serde(default, skip_serializing_if = "Option::is_none")]`
  and say in its doc comment what `None` means and what an older peer does (`/// Absent on older helpers.`).
- Keep `tests/support/baseline_ce7f62f.rs`, the previous protocol's frozen codecs for N-1 decode tests, independent
  of `mac_worker`.

## Modules and test seams

Integration tests compile the library without `cfg(test)`, so gate a test-only seam they need with
`#[cfg(any(test, feature = "test-support"))]`.

- Keep modules private, and give a new parent module a `foo.rs` beside its `foo/`. Expose an item to integration
  tests with an explicit `pub use` in `src/test_support/<domain>.rs`.
- Put a domain's test doubles in a `testing` submodule (`src/integration/testing.rs`).
- Make races and crash points deterministic with hooks in production code (thread-local slots, `with_*_hook`
  helpers, write-fault points such as `HostStore::with_root_entry_check_hook`) whose production defaults stay
  unchanged.

## Comments and CLI

- Comments state why: the race, the invariant, what an older peer does, which case is still refused. Write one or
  two full sentences directly above the code they explain.
- Give every public clap argument help text, and mark host-side and internal subcommands `#[command(hide = true)]`.

## Tests

Before writing or changing a test, read "Writing tests" in `docs/testing.md`.
