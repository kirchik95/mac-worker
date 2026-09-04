# Phase 5 validation record

Phase 5 adds agent tasks: submit a prompt, run a headless coding agent on a Mac mini, and collect the result as a Git branch. This record is the sanitized template for the three-worker live acceptance in spec section 20.2 items 1 to 5 and 7 to 10. Item 6 (`source = origin` and `publish = push`) belongs to the later publication plan and is out of scope here.

This document is **not** evidence of a configured live worker. Every live row is `PENDING` until the operator or an agent executes [docs/phase-five-acceptance-runbook.md](phase-five-acceptance-runbook.md) against a revision that contains Tasks 7, 8, and 9, then replaces the placeholders with sanitized values. This docs-only preparation did not contact any Mac mini and did not run the local gate.

The record may retain only shortened identifiers, sanitized command categories, terminal states, exit results, durations, before/after fingerprints of mac-worker-owned namespaces and of the isolated clone, and the helper/client revision match. It must omit application output, complete paths, clone origin, credentials, raw environment values, source content, and raw logs.

## Automated local and fake-transport evidence

The Task 10 local gate is recorded here once it has been run at the software commit below. It is not a substitute for live three-worker acceptance.

| Command | Revision measured | Recorded result |
| --- | --- | --- |
| `cargo test --locked --all-targets` | PENDING | PENDING |
| `cargo clippy --locked --all-targets -- -D warnings` | PENDING | PENDING |
| `cargo build --locked --release` | PENDING | PENDING |
| `git diff --check` | PENDING | PENDING |

## Live three-worker acceptance

An isolated live run must use a copy of the three-worker inventory and isolated XDG roots, then select all three configured workers. Claude Code is deferred by operator decision; Codex is the only agent for this run. The dashboard tasks view, `source = origin`, `publish = push`, Cursor, OpenCode, and `worker gc` remain later phases.

| Required evidence | Sanitized record |
| --- | --- |
| Software commit | PENDING. Record the full commit of the validated client/helper revision. The final local gate must have passed at that revision. |
| Worker protocol version | PENDING. Expect `4` from setup and ready-worker probes after the Task 3 bump. This docs branch still compiles protocol `2`. |
| Helper/client revision match | PENDING. Setup must verify the promoted helper, and a final SHA-256 equality check must confirm the installed helper on each of the three workers matches the validated client release binary. Record match/mismatch only; omit installation paths. |
| Selected worker names | PENDING. Record the three configured aliases only, for example `mini-1`, `mini-2`, `mini-3`. |
| Agent capabilities | PENDING. Expect `agent:codex` on every worker. Do not require `agent:claude@agents`; Claude is deferred. |
| Sanitized command categories | PENDING. Record only categories: isolated setup on three workers; five-task batch with default `max_parallel`; `needs_input` then `say`; `cancel` then `say`; pinned follow-up behind a busy worker; `--wip` identity and push-refused preflight; disconnect of `logs -f` and `submit --wait` plus killed runner; dashboard observe-and-shutdown; planted-secret metadata scan; Git-identity attribution on a worker without `user.name`/`user.email`. |
| 1. Five-task run, default `max_parallel`, submitting shell exits | PENDING. Three tasks `active` on three distinct workers; two wait with a visible blocking reason; `worker task wait --run` from a new shell completes all five. Record shortened run/task IDs, states, blocking reason, exit, duration. |
| 2. `needs_input` then `say` | PENDING. Task becomes `open` with last outcome `needs_input`; `say` resumes the same session on the same worker; transcript shows continuity. Record shortened IDs, worker alias, session-present yes/no, exit, duration. |
| 3. `cancel` during a turn, then `say` | PENDING. Cancel leaves the task `open`; `say` resumes the session and the task completes. Record shortened IDs, states, exit, duration. |
| 4. Follow-up pinned to a busy worker | PENDING. The pinned follow-up waits and never moves; a first turn admits to an idle worker without delay. Record shortened IDs, pin, blocking reason, the other task's worker alias. |
| 5. `--wip` base | PENDING. User repository byte-identical after submission (`HEAD`, index, working tree, refs, reflogs, configuration, hooks); result branch contains the temporary commit; `push` is refused at preflight. Record identity check, shortened result ref, refusal code. |
| 6. `source = origin` and `publish = push` | Out of scope for this record. Later publication plan. |
| 7. Disconnect and killed runner | PENDING. Disconnect during `logs -f` and during `submit --wait`, plus a killed runner mid-turn: exactly one turn ran; the next mutating command replaces the runner; status and logs reconnect by the original identifiers. Record shortened IDs, reconnect states, exit, duration. |
| 8. Dashboard observe and shutdown | PENDING. Dashboard shows every state above truthfully; shutdown changes nothing. Record only that the loopback view matched CLI state and that post-shutdown `status`/`list` were unchanged. The tasks view is a later phase; record whichever projection the landed binary actually shows. |
| 9. Retained metadata privacy | PENDING. Retained metadata contains no planted secret values and no complete local paths. Record hit counts only (expect zero and zero). |
| 10. Locked-keychain adapters and Git identity | PENDING. Spec asks Claude and Cursor to succeed from a locked-keychain SSH session using env profiles only, and a worker without a Git identity to produce correctly attributed commits. This run uses Codex only: record Codex attribution on the worker that has no `user.name`/`user.email`. Claude and Cursor rows stay deferred / later-plan unless the operator lifts that decision. |
| Worker namespace fingerprints | PENDING. Before/after entry counts and shortened fingerprints of mac-worker-owned data and helper namespaces on each of the three workers. Expected changes stay inside those namespaces. |
| Unrelated remote entries | PENDING. Before/after count and fingerprint of the immediate non-owned sibling sets of the data and helper parents on each worker. |
| Original isolated clone | PENDING. Baseline commit, intentional marker-only status/diff, and clean diff check unchanged after all remote execution. |

The live record must state that the validated helper and client revisions matched, without recording installation locations or connection details. It records only sanitized command categories and outcomes; application output is not safe evidence by default because it may contain application-emitted secrets.

## Delivery boundary

**A completed live record does not authorize later phases.** `source = origin`, `publish = push`, `--publish-branch`, Cursor Agent, OpenCode, retention through `worker gc`, and the dashboard tasks view remain later plans. In this execution core those options must continue to reject at preflight rather than run.

Claude Code remains deferred on the workers by operator decision even after this acceptance, unless the operator records a later decision here.
