---
name: pool-task-authoring
description: "Turn an objective into independent, testable prompts for headless agents in the mac-worker pool. Use before every pool dispatch, on an explicit /pool-task-authoring call, and when the user asks to prepare work for the pool: «подготовь задачи для пула», «нарежь на задачи для mini», «напиши бриф для пула», «prepare tasks for the pool», «write a brief for the workers»."
---

# Pool Task Authoring

## When To Use

- Before every `pool-dispatch` submit that does not already have a prompt file.
- The user invokes `/pool-task-authoring` explicitly.
- The user asks to prepare, slice, or brief work for the pool: «подготовь задачи для пула», «нарежь на задачи для mini», «напиши бриф для пула», "prepare tasks for the pool", "write a brief for the workers".

Use this skill to turn an objective into tasks that a headless coding agent can finish in one turn. Keep the text tool-agnostic so it can also be used as `AGENTS.md` material.

## Slice The Work

- Make each task one independent unit of work.
- Fit the task in one turn, about 45 minutes.
- Use one repository per task.
- Do not run concurrent tasks that share files. If several tasks share one repository, each brief must name the other tasks' files as forbidden.
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

Say explicitly that the task must not run git. The agent leaves its changes in the task worktree; the publisher commits them after the turn, and the orchestrator fetches the result branch and decides what happens next.

## Write The Brief

Every brief should contain these parts, in this order:

1. **Objective:** the narrow outcome and why it matters.
2. **Where to work:** the repository, base choice, and the task's working boundary.
3. **Boundaries:** files or areas allowed, forbidden files, and actions the agent must not take. State that it must not switch branches, push, open a merge request, or touch another worktree unless the task explicitly requires it.
4. **Exact steps:** the implementation sequence. Prefer failing tests first, minimal implementation, focused verification, then the final check.
5. **Gate:** exact verification commands and the acceptance condition. If any of those commands is timing-sensitive, require a serial rerun under load.
6. **Report:** the required final message, including changed files, tests, and any blocker or unverified claim.

Do not ask the agent to commit, switch branches, or push: on Codex the sandbox keeps `.git` read-only and the attempt ends the turn `blocked`. A successful task is not complete merely because files changed: the requested behavior must be verified by the gate.

The prompt preamble already tells the agent that it works in an isolated task worktree, must not switch branches or push, leaves its changes for publication, and, for Cursor and OpenCode, must end with exactly the JSON result object. Keep task-specific instructions concrete and do not put secrets in the prompt.

## Headless-run lessons

These constraints come from live headless turns. Keep the brief itself tool-agnostic; the failure modes apply to every agent in the pool.

- **The first prompt can be swallowed.** After startup some agents drop the first stdin payload. Put the complete assignment in `--prompt-file` and treat that file as the only copy of the work. Do not rely on a later `say` to deliver the objective, and do not split the assignment across a "warmup" message and a real one.
- **A zero process exit is not success.** Some agents, including Codex, exit `0` while the structured status is `blocked`. Read `status` from the result envelope. Return `done` only when that field is `done` and the gate passed. Treat `blocked` as a failed turn even when the CLI exited zero.
- **Headless agents cannot ask questions.** There is no TTY and no permission prompt. Every decision the agent needs must be in the brief: names, paths, commands, acceptance numbers, and what to do on the obvious branches. If a decision is genuinely missing, the agent must finish the turn with `needs_input` or `blocked` and a concrete question. Do not write "ask me if unsure."
- **Name forbidden files when several tasks share a repository.** "Do not edit unrelated files" is not enough. List the exact paths or directories another in-flight task owns, and list the paths this task must not touch. Two tasks on one repository are still one-repo-per-task only if their write sets are disjoint and each brief says so.
- **Do not displace the structured result.** Cursor and OpenCode have no output schema; the preamble demands a bare JSON object as the last message. A brief that asks for a Markdown summary at the end pushes the JSON out and the outcome becomes `unknown`. Ask for the summary inside the JSON `summary` field.
- **Timing-sensitive tests need a serial rerun under load.** A single green run in an idle worktree is not the gate when the change can race. Say so in the Gate: run the focused tests once, then rerun them serially (not in a wide parallel harness) while other work is in flight, and treat a flake on that rerun as `blocked`.

## Read The Result

Expect one of these structured statuses. The process exit status is not enough; read the envelope.

- `done`: the requested work and gate completed. Fetch the result branch.
- `needs_input`: the agent has a bounded question needed for the next turn. Answer it with a follow-up message, or close the task if the decision is out of scope.
- `blocked`: the agent could not complete the task, including the case where the process exited zero. Read its result and logs, then write a follow-up that removes the concrete blocker, or close with discard.

If the turn fails, inspect the structured result and logs before authoring a follow-up. A follow-up should contain the missing decision, file, command, or constraint. Do not repeat the same prompt. If the problem is a task boundary or dependency, split or reorder the work instead.

## Route By Work

| Work type | Agent |
|---|---|
| Rust implementation, Rust tests, builds, or work needing shell judgement | Codex, `--model gpt-6-astra --effort xhigh` |
| TypeScript or frontend implementation | Cursor, `--env-profile agents` |
| Second opinion or documentation | OpenCode, default Zen model or `--model opencode-go/<model>` |
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
5. Leave the changes uncommitted in the worktree; do not run git.

## Gate

Run `cargo test --locked --test request_validation` and `cargo fmt --check`. Both must pass.

## Report

Report the changed files, the test result, and any remaining limitation. Return `done` only when the gate passes; return `blocked` with the exact failing command otherwise.
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
agent = "opencode"
publish = ["fetch", "push"]
publish_branch = "feat/billing-client"
```

Top-level keys are defaults. A task may override them. Validate every entry before submitting the batch. Tasks in a run are independent; the run groups them for status, waiting, and `max_parallel`.
