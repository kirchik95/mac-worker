# mac-worker

`worker` is a Rust CLI that runs coding-agent tasks on worker Macs over SSH and returns one Git branch per task.
One binary plays three roles. The **queue owner** (the laptop, or an opt-in remote controller) holds the queue and
drives each turn. Each **host** (a worker Mac) runs every task's agent in its own worktree, driven over SSH through
hidden `worker host …` subcommands. The **dashboard** serves the React app in `ui/` on loopback.

## Read first

- Before writing Rust or tests, read [CODING_STANDARDS.md](CODING_STANDARDS.md).
- Before changing user-visible behaviour, read its section of `docs/usage.md`, and update that section on the same
  branch in a `docs(usage): …` commit.
- Before changing `ui/` or the snapshot types in `src/dashboard/model.rs` that `ui/src/lib/api.ts` mirrors, read
  `ui/README.md`. For how the dashboard looks or reads, also read `PRODUCT.md` and `DESIGN.md`.
- Before editing `README.md`, `README.ru.md` or `docs/`, read `docs/documentation-site.md`.
- Before a version bump or a release, read `docs/releasing.md`.
- `.claude/skills/pool-dispatch/SKILL.md` and `.claude/skills/pool-task-authoring/SKILL.md` are compiled into the
  binary (`src/skills.rs`), so an edit there changes what `worker skills get` prints.
- `docs/superpowers/` is dated design history. Where it disagrees with the code or `docs/usage.md`, trust the code
  and `docs/usage.md`.

## Checks

Pass `--locked` to every Cargo command. While you work, run these checks and only the tests your change touches
(`<area>` is a directory under `tests/`):

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
CARGO_BUILD_JOBS=4 cargo clippy --locked --release --no-default-features --lib --bin worker -- -D warnings
CARGO_BUILD_JOBS=4 cargo nextest run --locked --test <area> -E 'test(/^<module>::/)'
CARGO_BUILD_JOBS=4 cargo nextest run --locked --lib -E 'test(/<module>::/)'
```

- The second Clippy is the only check of production code without the `test-support` feature.
- CI lints with the Rust that `rust-toolchain.toml` pins, and only rustup honours that file. A newer `cargo`, such
  as Homebrew's, reports Clippy errors in code you did not touch; fix the ones your change introduces.
- Integration tests change process-wide state, so run them under nextest, or under `cargo test` with
  `-- --test-threads=1`.
- CI on pushes and PRs runs only these checks and the unit tests. Before landing, read `docs/testing.md` and run the
  whole suite with the **gate**, `scripts/test-gate.sh`.

## Workflow

- Work in a worktree under `.worktrees/`, on a `feat/`, `fix/`, `test/` or `docs/` branch.
- Fix a bug test-first: a `test(scope): cover …` commit that is **red** on the base, then the `fix(scope): …` commit.
- Prove a change with tests and temporary roots. `worker init`, `worker setup`,
  `worker controller init|drain|disable` and installing into `~/.local/bin` change the owner's live Macs and CLI, so
  run them only when the owner asks.
