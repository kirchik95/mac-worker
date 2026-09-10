# Project readiness, setup recipe, batch preview, Git identity (ENV)

Date: 2026-09-10. Track ENV. Base: `faf6eff437390dc1f996b498f9fb8aae48c6c83d`.

**Goal:** Optional project setup recipe and preflight before expensive agent turns; skip recipe only after a current-workspace `check` succeeds and a per-task identity receipt matches; read-only batch preview that never pretends DAG is enforced; Git identity probe uses the account login environment.

**Architecture:** Optional `[setup]` in `.worker.toml`. After GO, `gated_child_main` only does the existing async-signal-safe gate then `execve`s `worker __mac_worker_prepare_turn -- <agent>`. The helper (same recorded PID/group, fresh Rust runtime) runs `prepare_project_setup` with `InheritProcessGroupRunner`, then `execve`s the agent. Recipe timeout/cap `_exit`s that child; supervisor `killpg` reaps descendants. Optional `check` validates this workspace. Per-task receipts live in a rooted cache dir. Batch `--preview` is non-mutating and does not open client state. `depends_on` is rejected on submit. Declared `acceptance` is copied into the composed prompt; FLOW still owns result-instruction and agent-reported checks.

**Constraints:** No HostStore layout bump, no protocol bump, no inferred packages, no laptop scripts, no agent permission broadening, no dashboard UI, no auth-incident detection, no global receipt skip. Existing defaults unchanged when `[setup]` is absent.

**Interfaces:** `/private/tmp/mac-worker-roadmap-jn00iibh/env-contract.md`.

## Reused mechanisms

- `ProjectSettings` deny-unknown-fields and relative-path validators
- `account_login_shell_request` / `ProcessPolicy`
- `TaskStore::prepare_workspace`
- `RootedDir` for cache open (nofollow, owner-only)
- `BatchFile` defaults merge
- `compose_turn_prompt` + FLOW `result_instruction`
- Laptop commit identity in `TaskClient` (unchanged)

## Work

1. Parse `[setup]` with `commands`, optional `check`, `lockfiles`, `inputs`.
2. `project_readiness`: identity includes env_profile name and input hashes; skip only check+per-task receipt; two-workspace regression.
3. Helper exec after GO (`src/prepare_turn.rs`); remaining lease budget; setup-result.json. Lib tests use `current_exe --exact`; JobService tests inject `CARGO_BIN_EXE_worker`.
4. Batch `--preview` via shared `resolve_batch_task`; `dag.status = unsupported`; no client-state open; submit rejects `depends_on`.
5. Append declared acceptance to composed prompts.
6. Git identity uses account login isolation.
7. Docs: toolchain is user-owned; `check` is current-workspace proof.

## Tests

- Two workspaces, identical lockfiles, local outputs in both; delete output and repair
- Changed `inputs` script does not skip
- Failed/cancelled/timeout prep: no receipt; grandchild holding pipes is reaped without waiting the turn deadline
- Isolated HOME; receipts omit secrets
- Gated-child setup timeout/cap: no agent launch, setup-result SETUP_TIMEOUT / SETUP_FAILED
- JobService cancel during setup (no receipt). Detached supervisor death while setup descendants run: no second launch, recover/cancel reaps, no agent.
- One total turn budget: successful setup may publish a receipt; agent starts then times out at original `expires_at`. Expired leases do not renew a budget. Submit elapsed may include TERM/group cleanup grace.
- Read-only preview, overlaps, cyclic deps, trailing-slash files, local HEAD OID; submit `BATCH_DEPENDENCIES_UNSUPPORTED`
- Acceptance in composed prompt
