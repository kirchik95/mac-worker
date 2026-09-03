# Agent task shell spike

## Scope and handling

This was a disposable, non-interactive shell spike against one configured
macOS worker. All repositories, worktrees, scripts, event streams, results,
and temporary Git configuration were kept in scratch space and will be
removed after this record is committed. This document intentionally omits
worker names, filesystem paths, tokens, session identifiers, and transcript
text.

Codex was available through its existing file-based login.

## Confirmed CLI flags and result envelopes

| Agent | First-turn flags tested | Resume flags tested | Result |
| --- | --- | --- | --- |
| Codex | `exec --json -o … --output-schema … -s workspace-write --approve-for-me -c sandbox_workspace_write.network_access=true -` | n/a | Rejected before launch (exit 2): `--sandbox` and `--approve-for-me` are incompatible in this installed CLI. |
| Codex | `exec --json -o … --output-schema … -s workspace-write -c approval_policy="never" -c sandbox_workspace_write.network_access=true -` | `exec resume <session> --json -o … --output-schema … -c sandbox_mode="workspace-write" -c sandbox_workspace_write.network_access=true -c approval_policy="never" -` | Accepted. A no-op success prompt exited 0; a prompt that deliberately ran a failing shell command also exited 0 but produced structured status `blocked`. |
| Claude Code | Deferred by operator decision. | Deferred by operator decision. | Deferred by operator decision. |

For successful Codex turns, the JSONL stream contained a `thread.started`
event shaped as `{"type", "thread_id"}`. The identifier was captured only
in scratch. The last-message file was a JSON object with exactly the requested
top-level fields: `status`, `summary`, `questions`, and `files_changed`.

`approval_policy="never"` is accepted as a configuration override for a
resumed workspace-sandboxed turn. The resumed stream emitted the same thread
identifier as the original. The top-level `--ask-for-approval` flag shown in
general help is not accepted by `codex exec`; the configuration override is
the working form observed here.

## Cancellation and session persistence

A Codex turn was started in its own process group. After it created the
requested marker file, `TERM` was sent to that process group. The marker
persisted; the interrupted turn had no final structured result. Resuming the
captured session exited 0, retained the original marker, created the
follow-up marker, and returned structured status `done`. The resumed stream
reported the same thread identifier.

Codex stores the canonical session in its per-user sessions store. `codex
delete --force <session>` exited 0 and removed that canonical session entry,
but did not remove other non-session Codex-state references to the identifier.

## Mirror worktree and Git commits

The worker scratch repository was a fresh clone whose bare mirror was made
with `git init --bare`; its workspace was created with `git worktree add`.
Under the working workspace sandbox configuration, Codex created the target
file but could not stage or commit it. Its structured result reported that the
linked worktree index lies outside the writable sandbox and Git returned
`Operation not permitted`. No commit was created.

This is a **no-go** for the current workspace-sandbox launch shape: a linked
worktree needs its associated Git directory to be writable as well as its
checkout. It is not evidence that Git itself or the mirror layout is broken.

The worker had no effective configured `user.name` or `user.email`, and no
Git author/committer identity environment variables. A direct scratch commit
nevertheless exited 0 because Git's automatic identity fallback was enabled.
The production launcher must still export the recorded task identity (or the
specified fixed fallback) so commit attribution never depends on that fallback.

## Raw Git transport proof

The disposable bare mirror used a mirror-local `core.hooksPath`, a mode-checked
pre-receive hook that allowed only base refs, and `receive.denyDeletes=true`.
A temporary account-global hook that rejects all pushes was installed for the
test and restored byte-for-byte afterwards.

- An allowed base-ref push through a `--receive-pack` program override
  succeeded despite the global rejecting hook, proving the mirror-local hook
  pin won.
- A client push to `refs/heads/x` and a deletion of the base ref each failed
  with a pre-receive-hook rejection.
- Repeating the allowed push reported `Everything up-to-date`, i.e. no object
  transfer.
- A result-branch commit made in a mirror worktree returned through the
  `--upload-pack` program override, the transfer repository, and then the
  scratch clone.
- The scratch clone retained its index, `HEAD`, working status, configuration,
  and hooks. Its refs and reflogs changed when the result remote-tracking ref
  was imported; this is the observed import side effect, not an untouched-clone
  proof.
- With `GIT_TRACE=1`, the fetch invocation showed Git inserting
  `-o SendEnv=GIT_PROTOCOL` before the remote host. The observed push traces
  did not include that option.

This proves the raw program-override and pinned-hook mechanics only. It does
not prove the planned hidden host helper's identity validation, lease/request
fingerprint checks, path validation, or neutralized global/system Git config.

## Memory and network observation

While Codex attempted `cargo test --locked`, four samples of `vm_stat` and
`memory_pressure` were recorded at five-second intervals. The first agent
event arrived about 2.0 seconds after SSH start. The command did not reach the
test suite: it exited 101 because the locked `globset` dependency was missing
from the worker's local Cargo cache while offline. Codex returned structured
status `blocked` and CLI exit 0.

The samples reported 85% free memory and zero swap growth. The
`memory_pressure` command did not emit a kernel pressure class, so peak class
is recorded as `unknown`. A network-enabled retry was not performed because
it could populate missing dependencies on the worker, which this spike was
instructed not to install or upgrade.

The current repository is a Cargo project, not an npm project. Therefore the
requested npm-specific network conclusion is not proven; the Cargo attempt
does show that the offline cache was insufficient for its locked suite.

## Go / no-go

| Gate | Result | Evidence |
| --- | --- | --- |
| Headless auth via environment profile | **DEFERRED by operator decision** | Deferred by operator decision. |
| Structured result | **GO for Codex** | JSONL session-start event and schema-shaped final JSON were captured. |
| Resume after cancel | **GO for Codex** | TERM after an edit, same-session resume, and both edits persisted. |
| Transport with program overrides and pinned hooks | **GO for raw mechanics** | Allowed base push, rejection coverage, repeat zero-object push, result fetch, and temporary global-hook restoration succeeded. Hidden-host-helper coverage remains unproven. |
| Codex commits inside a mirror worktree | **NO-GO** | Workspace sandbox denied writes to the linked worktree index outside the checkout. |
| Claude `--max-turns` | **DEFERRED by operator decision** | Deferred by operator decision. |
| Memory profile during a real test suite | **NO-GO** | Sampling and first-event latency were captured, but the Cargo suite could not start with the available offline cache. |

The immediate implementation blocker is the linked-worktree sandbox boundary
for Codex. Transport can proceed only with the stated distinction between this
raw proof and the future host-helper contract.
