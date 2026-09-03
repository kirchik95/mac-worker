---
name: pool-task-authoring
description: "Turn an objective into independent, testable prompts for headless agents in the mac-worker pool."
---

# Pool Task Authoring

Use this skill to turn an objective into tasks that a headless coding agent can finish in one turn. Keep the text tool-agnostic so it can also be used as `AGENTS.md` material.

## Slice The Work

- Make each task one independent unit of work.
- Fit the task in one turn, about 45 minutes.
- Use one repository per task.
- Do not run concurrent tasks that share files.
- Split by ownership and dependency, not by arbitrary file count.
- Make the final state testable by a command, an assertion, or an explicit artifact.

Size limits from the spec. Keep every task inside them:

| Limit | Scope | Value |
|---|---|---|
| `timeout` | one turn | default `45m`, max `24h` |
| `max_turns`, `max_budget_usd` | one turn | only agents that support them; otherwise recorded as unsupported |
| `max_followups` | one task | default `10` |
| `max_parallel` | one run | caps how many of its tasks may be active at once |
| `slots = 1` | one worker | one turn at a time |
| log bounds | one turn | 64 KiB chunks; event stream capped at 256 MiB with a 64 KiB tail |

Keep prompts under 256 KiB. Keep requested output bounded. Do not rely on an unbounded transcript.

## Choose The Base

Use `--base HEAD` when the task should start from the current committed work. Say so in the brief.

Use `--wip` when the task must include the current uncommitted changes. State which untracked inputs are included. A WIP base is a temporary commit; `publish = push` is rejected for it (`PUBLISH_REQUIRES_COMMITTED_BASE`).

Say explicitly when the task must not push. The agent must commit its completed work, but the orchestrator only fetches the result and decides what happens next.

## Write The Brief

Every brief should contain these parts, in this order:

1. **Objective:** the narrow outcome and why it matters.
2. **Where to work:** the repository, base choice, and the task's working boundary.
3. **Boundaries:** files or areas allowed, forbidden files, and actions the agent must not take. State that it must not switch branches, push, open a merge request, or touch another worktree unless the task explicitly requires it.
4. **Exact steps:** the implementation sequence. Prefer failing tests first, minimal implementation, focused verification, then the final check.
5. **Gate:** exact verification commands and the acceptance condition.
6. **Report:** the required final message, including changed files, tests, commit hash, and any blocker or unverified claim.

Tell the agent to commit completed work with a descriptive message. A successful task is not complete merely because files changed: the requested behavior must be verified and the commit must exist.

The prompt preamble already tells the agent to work in a dedicated task branch, commit, avoid interactive questions, and finish with a structured result. Keep task-specific instructions concrete and do not put secrets in the prompt.

## Read The Result

Expect one of these structured statuses:

- `done`: the requested work and gate completed. Fetch the result branch.
- `needs_input`: the agent has a bounded question needed for the next turn. Answer it with a follow-up message, or close the task if the decision is out of scope.
- `blocked`: the agent could not complete the task. Read its result and logs, then write a follow-up that removes the concrete blocker, or close with discard.

If the turn fails, inspect the structured result and logs before authoring a follow-up. A follow-up should contain the missing decision, file, command, or constraint. Do not repeat the same prompt. If the problem is a task boundary or dependency, split or reorder the work instead.

## Route By Work

| Work type | Agent |
|---|---|
| Rust implementation, Rust tests, builds, or work needing shell judgement | Codex |
| TypeScript or frontend implementation | Cursor |
| Second opinion or documentation | OpenCode |
| Claude Code on workers | Deferred by the operator for now |

Routing is guidance for `--agent`; do not invent worker names or SSH destinations. The pool selects the worker.

## Example Brief

```markdown
# Add request-id validation

## Objective

Reject malformed request IDs at the API boundary and add regression coverage.

## Where to work

Use `--base HEAD` in the repository's current worktree. Work only on the request validation module and its tests.

## Boundaries

Allowed: the validator, its unit tests, and the API error mapping. Do not edit persistence, deployment files, or unrelated endpoints. Do not switch branches, push, open a merge request, or touch another worktree.

## Exact steps

1. Add failing tests for empty, oversized, and invalid request IDs.
2. Run the focused test command and confirm the failures are caused by missing validation.
3. Implement the smallest validation change.
4. Run the focused tests and the repository formatter.
5. Commit the completed work as `fix: validate request ids`.

## Gate

Run `cargo test --locked --test request_validation` and `cargo fmt --check`. Both must pass.

## Report

Report the commit hash, changed files, test result, and any remaining limitation. Return `done` only when the gate passes; return `blocked` with the exact failing command otherwise.
```

## Example Batch File

```toml
version = 1
agent = "codex"
base = "main"
source = "local"
publish = ["fetch"]
timeout = "45m"

[[tasks]]
title = "Flaky login spec"
prompt_file = "tasks/fix-flaky-login.md"

[[tasks]]
title = "Extract billing client"
prompt = """
Move the billing HTTP client into packages/billing-client …
"""
agent = "claude"
publish = ["fetch", "push"]
publish_branch = "feat/billing-client"
```

Top-level keys are defaults. A task may override them. Validate every entry before submitting the batch. Tasks in a run are independent; the run groups them for status, waiting, and `max_parallel`.
