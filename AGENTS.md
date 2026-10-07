# mac-worker

`worker` is a Rust CLI. It sends coding-agent tasks (Codex, Cursor, OpenCode, Claude Code) from a laptop to
Apple Silicon Macs over SSH and returns one Git branch per task. The single binary has three roles:

- **queue owner**: by default the laptop, or an opt-in remote controller (`src/controller/`). It holds the queue
  (`client_state`), schedules work and drives each turn.
- **host**: a worker Mac. The queue owner calls hidden `worker host …` subcommands on it over SSH. Each task gets
  its own worktree and a supervised agent process.
- **dashboard**: a loopback axum server (`src/dashboard/`). It serves the React UI from `ui/`, which is embedded
  at compile time.

The code runs on macOS only (launchd, `hdiutil`, `libc` filesystem calls), and CI runs on `macos-15`.

- Behavior reference: `docs/usage.md`. Architecture and installation: `docs/getting-started.md` ("How it works").
- Dashboard product and design rules: `PRODUCT.md`, `DESIGN.md`, `ui/README.md`.
- Before writing Rust or tests, read [CODING_STANDARDS.md](CODING_STANDARDS.md).

## Commands

`rust-toolchain.toml` pins Rust 1.98.1. Pass `--locked` to every Cargo command.

```sh
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
CARGO_BUILD_JOBS=4 cargo clippy --locked --release --no-default-features --lib --bin worker -- -D warnings
```

Run both Clippy commands. `--all-targets` turns on the `test-support` feature through the crate's dependency on
itself, so only the second command checks the production code without it.

While you work, run only the test binaries you changed. There are nine areas: `agents cli controller dashboard
host scheduler setup task transfer`.

```sh
CARGO_BUILD_JOBS=4 cargo nextest run --locked --test <area> -E 'test(/^<module>::/)'
CARGO_BUILD_JOBS=4 cargo nextest run --locked --lib -E 'test(/<module>::/)'
```

- Run integration binaries under nextest. Under plain Cargo, use `-- --test-threads=1`, because tests change
  process-wide state (the working directory, the environment, `HOME`).
- A filter that selects zero tests counts as a failure.
- Run the **gate**, `scripts/test-gate.sh`, before landing. It runs the whole suite (about 4,500 tests) and needs
  cargo-nextest. It puts `TMPDIR` on a 4 GB RAM disk; set `MAC_WORKER_GATE_RAMDISK_MB=0` to skip the RAM disk.
  Recorded full runs took 11–21 min, so run it once per change, not after every edit.
- Stress tests (named `_stress`, ignored by default): `scripts/test-gate.sh --run-ignored only -E 'test(/_stress$/)'`.
- `docs/testing.md` explains the per-area commands, the nextest profiles and test groups, and why they exist.

CI (`.github/workflows/ci.yml`) on pushes and PRs runs fmt, both Clippy commands and
`cargo test --locked --lib --bins`. The integration suite runs only nightly, on manual dispatch and in the
release workflow. A green PR does not show that the gate passes.

Dashboard UI (`ui/`, Node 22): `npm ci && npm test && npm run lint && npm run build`.

## Where things live

| Path | Contents |
|---|---|
| `src/cli.rs`, `src/lib.rs` | clap tree; `run_with_stdio` in `lib.rs` (about 9k lines) dispatches every command |
| `src/client_state*`, `task_client.rs`, `scheduler.rs`, `admission.rs`, `turn_runner.rs`, `transport.rs` | queue owner: queue store, task commands, scheduling, turn driving, SSH |
| `src/host_store.rs`, `task_store.rs`, `job*.rs`, `supervisor.rs`, `outbox.rs`, `rooted_fs.rs` | host: durable job and task state, agent supervision, origin delivery, fd-relative filesystem |
| `src/agent/` | one adapter per agent CLI |
| `src/integration*`, `src/controller/`, `src/session_transfer/` | automatic integration; remote controller (`channel` is the persistent read socket, `events` the journal and notify); `--from-session` |
| `src/error.rs` | `WorkerError`, public codes, exit kinds, the error `CATALOG` |
| `src/test_support/` | feature-gated facade that integration tests import from |
| `tests/<area>/main.rs`, `tests/support/` | the nine integration binaries; shared fixtures |
| `docs/superpowers/{specs,plans,reviews,validation}/` | dated design history. When it disagrees with the code or `docs/usage.md`, the code and `docs/usage.md` are correct |

## Files that change together

- The `CATALOG` in `src/error.rs` and the table between `<!-- error-catalog:start -->` and
  `<!-- error-catalog:end -->` in `docs/usage.md` must list the same codes, in the same order, with the same exit
  statuses and hints. The unit test `usage_exit_reference_matches_the_catalog` checks this.
- The `ui/` source and the committed bundle in `src/dashboard/static/app/`. Cargo embeds the bundle and never runs
  Node, and CI rebuilds the bundle and diffs the whole tree. Change the bundle only by rebuilding it from `ui/` in
  one worktree, then commit the full `app/` tree with the source change (see "Generated assets" in
  `ui/README.md`).
- The Rust dashboard snapshot types and `ui/src/lib/api.ts`, which mirrors them.
- `.claude/skills/pool-dispatch/SKILL.md` and `.claude/skills/pool-task-authoring/SKILL.md` are compiled into the
  binary (`include_str!` in `src/skills.rs`) and printed by `worker skills get`. Editing them changes what the
  binary prints.
- Tests in `tests/task/project_config.rs` parse `config.example.toml`.
- `README.md` and `README.ru.md` must stay in sync, because the docs site builds its quick starts from both. Put
  detailed instructions in `docs/getting-started.md`.
- `docs/` is the source for the website. `docs-site/content/` is generated; edit `docs/` instead (see
  `docs/documentation-site.md`).
- A version bump changes `Cargo.toml` and the default version in `install.sh` together (`docs/releasing.md`).

## Workflow

- Work on a branch in its own worktree under `.worktrees/` (gitignored). Branch prefixes are `feat/`, `fix/`,
  `test/` and `docs/`. An `integ/<name>` branch collects several of them before they go to main.
- Commit subjects use `type(scope): summary`. Types: `fix feat test docs refactor perf ci chore build`. The
  scope is the area, for example `integration`, `controller`, `events`, `session`, `host`, `supervisor`, `task`,
  `dashboard`, `usage` or `validation`. The summary is lowercase, has no final period and states the behavior,
  for example "a stop never reports success over a push that won during an auxiliary turn". The body explains
  what broke, why, and which cases are still refused.
- A fix usually takes two commits. First `test(scope): cover …`, which fails (is **red**) on the base, and the
  body may say so ("Red on 383c429"). Then `fix(scope): …`.
- Branches land on main as merge commits titled `Merge <branch>: <summary>`. A merge of an `integ/` branch lists
  its fixes as bullets in the body.
- A change to user-visible behavior updates `docs/usage.md` on the same branch, as a `docs(usage): …` commit.
- Only the owner decides when to deploy. Installing into `~/.local/bin`, `worker init`, `worker setup`, and
  restarting a controller or dashboard all affect the live pool, so do them only when the owner directly asks.
  `worker setup` refuses a debug build unless you pass `--allow-debug`.
- To replace an installed `worker`, create a new inode: use `install -m 755`, or stage the file and `mv` it as
  `install.sh` does. If you `cp` over the binary in place, macOS kills it on its next run (exit 137).
- Release steps are in `docs/releasing.md`.
