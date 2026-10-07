# mac-worker

`worker` is a Rust CLI: the **queue owner** (the laptop, or a remote controller) runs coding-agent tasks on
**hosts** (worker Macs) over SSH and returns one Git branch per task.

## Guardrails

- Prove a change with tests and temporary roots. Touch the owner's live pool (`worker init`, `worker setup`,
  `worker controller init|drain|disable`, an install into `~/.local/bin`) only when the owner asks.
- Pair a user-visible behaviour change with its `docs/usage.md` section, on the same branch, in a `docs(usage): …`
  commit.
- Work in a `.worktrees/` worktree on a `feat/`, `fix/`, `test/` or `docs/` branch.
- Fix a bug test-first: a `test(scope): cover …` commit **red** on the base, then the `fix(scope): …` commit.
- Trust the code and `docs/usage.md` over `docs/superpowers/`, which is dated design history.

## Read first

- Before writing Rust code or tests → `CODING_STANDARDS.md`.
- Before running checks or tests → `docs/testing.md`.
- Before UI work, including the `src/dashboard/model.rs` snapshot types that `ui/` mirrors → `ui/README.md`.
- Before editing `README.md`, `README.ru.md` or `docs/` → `docs/documentation-site.md`.
- Before editing a pool skill (`.claude/skills/pool-*/`) → `src/skills.rs`, which embeds it in `worker skills get`.
- Before a version bump or release → `docs/releasing.md`.
