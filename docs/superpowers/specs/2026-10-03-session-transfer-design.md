# Agent Session Transfer

Date: 2026-10-03

Status: Round 1 approved by the owner on 2026-10-03 («Давай это делать»): Phase A first (Q1), scrubber plus explicit opt-in (Q2), strict version gate until T0 says otherwise (Q3). The T0 live spike gates freezing the T1 contracts. Agent on-disk formats and resume behaviour marked **(spike)** are not yet verified against real agents.

Code baseline: `3a1a097` (`main`, after the parallel orphan-receipt fix; anchors were collected at `e417089` and only `src/controller/{store,health}.rs` moved, which the spec does not cite). Current-code citations use `path:line`. Proposed APIs, limits and codes below are requirements, not implemented behaviour.

Plan: [2026-10-03-session-transfer.md](../plans/2026-10-03-session-transfer.md).

Origin: study of herdr-gpui Teleport (`github.com/penso/herdr-gpui`, Apache-2.0, `crates/herdr-gpui/src/teleport/{sessions,launch,git}.rs`) on 2026-10-03. Teleport moves an interactive herdr workspace between hosts, including each agent's native session. We take only the session part.

## Purpose and boundaries

A pool task can start from an existing agent conversation instead of an empty one. Later, a pool task's conversation can be pulled back to the laptop and continued interactively.

- **Phase A (this wave):** `worker task submit --from-session <agent>[:<id>]`. The laptop captures a Claude Code or Codex session and ships a normalized copy next to the snapshot. The host places it in the agent's native store before the first turn, and the first turn resumes it under pool policy.
- **Phase B (next wave, same codec):** `worker task session pull <task> --into <dir>`. Decision 10 sketches it.
- **Phase C (deferred):** moving a running task's session to another host on drain.

**Copy, never move.** The laptop session stays usable, and the pool copy gets its own id. Teleport retires the source workspace. We do not, because a pool task is an independent unit of work and the user may keep working locally.

**Out of scope:**
- tabs, splits and process rebuild (herdr GUI specifics);
- credential lending (origin pushes already use forwarded credential helpers);
- Cursor and OpenCode import;
- converting a session from one agent to another.

Protocol stays 7 (`src/protocol.rs:5`). Every change is additive and feature-gated.

## Decision 1 — reuse existing mechanisms

**Existing mechanisms:**
- **Binding a session before the first turn, without running the agent:** prebind (`src/turn_runner.rs:1251-1269`, `src/turn.rs:1776-1847`, `TaskPrebindRequest::persist` near `src/task_store.rs:220`). Cursor already treats every turn as a resume (`src/agent/cursor.rs:217-232`).
- **Resume argv that restates pool policy:**
  - Claude `--resume <ref>`: `src/agent/claude.rs:45-71`.
  - Codex `exec resume <thread_id>`: `src/agent/codex.rs:58-87`, resume policy `src/agent/codex.rs:173-194`.
- **Session binding:**
  - Bound once; a different ref fails with `TASK_SESSION_CONFLICT` (`src/task_store.rs:1599-1644`).
  - Claude binds its seed on a first turn (`src/job_service.rs:580-590`, `652-662`). The seed is the first turn id (`src/turn_runner.rs:1225`, `1243`).
- **Bulk transport is git only:**
  - Direct: `push_base` (`src/git_transport.rs:81-121`).
  - Laptop → controller: source stream (`src/git_transport.rs:465-516`, `src/controller/stream_client.rs:33-122`).
  - Controller → host: `src/controller/registry.rs:304-343`.
- **Additive feature registry:** `src/features.rs:1-10`, with `HOST_FEATURES` empty (`src/features.rs:7`). Probe advertises features (`src/protocol.rs:63-66`, `src/probe.rs:370-375`). Scheduling requirements: `src/task_client.rs:6514-6535`, `src/scheduler_adapter.rs:52-62`.
- **Agent version facts** per host (`src/agent_facts.rs:296-303`, `1750-1790`) and per turn (`src/identity.rs:21-36`, `TurnSummary.agent_identity`).

**Not found (checked):**
- No code references `~/.claude/projects` or `~/.codex/sessions`.
- No host command returns an arbitrary file.
- No non-git bulk channel exists. Every JSON path is about 1 MiB (`src/controller/protocol.rs:11`, `src/lib.rs:4018-4040`, `src/transfer.rs:49`).
- `src/redaction.rs:177-191` is unsuitable for transcripts. It escapes newlines, rewrites every absolute path and every hex/base64 run, and truncates.
- `FROZEN_BUNDLE_MAX_BYTES` is test-only (`src/transfer_repo.rs:788-794`).

## Decision 2 — a canonical session package

The laptop turns a native session into a **package**: a tree committed without parents in the laptop transfer repo.

```text
manifest.json   {schema: 1, agent, format, source_session_id, source_agent_version,
                 source_cwd_relative, files: [{path, bytes, sha256}], scrubbed: <count>}
session/...     native files, normalized
```

**Formats:**
- **`claude-jsonl-v1`:** `session/main.jsonl`, plus the optional sidecar directory `session/sidecar/...` (subagent transcripts, tool results). **(spike)** Exact sidecar layout: Teleport copies `<id>.jsonl` and the `<id>/` directory next to it.
- **`codex-rollout-v1`:** `session/rollout.jsonl`. The dated file name is regenerated on placement.

**Normalization on the laptop:**
1. Read complete lines only. A live session is cut at its last `\n`. Every line must parse as JSON, otherwise `SESSION_UNREADABLE`.
2. Replace the checkout root with the token `@@MW_WORKSPACE@@`, in both its given and canonical forms, and only at a path boundary. The rule comes from Teleport's `rewrite_paths`: `/w/feat` never matches `/w/feature`. If the input already contains a token, refuse with `SESSION_UNREADABLE`.
3. Replace the source session id everywhere with `@@MW_SESSION@@`. A UUID string is unique, so raw replacement is safe.
4. Scrub secrets (Decision 6).
5. Enforce caps: 64 MiB raw in total, 64 MiB per file, 2,000 files. Anything larger fails with `SESSION_TOO_LARGE`.

Measured on the laptop on 2026-10-03:
- **Claude:** 37 sessions of this repo total 322 MB. The largest is 19 MB. A 6.6 MB example is current.
- **Codex:** 61 rollouts exceed 5 MB.

JSONL compresses well in git objects.

**Why tokens rather than paths.** The laptop cannot know the host workspace path in advance. The worker is chosen at admission, and the host root depends on the host's XDG/HOME (`src/paths.rs:50-51`, `src/host_store.rs:3157-3175`). The host substitutes the tokens. Phase B export emits the same tokens in reverse, so one codec serves both directions.

**Rejected alternatives:**
- **Shipping raw laptop paths and rewriting them on the host:** laptop paths leave the machine, and the two directions become asymmetric.
- **A chunked host command over the JSON channel:** it would be a new data plane under 1 MiB frames.
- **`--wip --include <transcript>`:** pollutes the workspace, base and result commit, and needs `--wip`.

## Decision 3 — transport: a second ref next to the base

**Laptop pin.** The laptop pins the package at `refs/mac-worker/sessions/<task_id>` in its transfer repo before the submission intent clears. This is the same durability as the WIP base ref (`src/transfer_repo.rs:1571`). It is removed together with the base pin.

**Direct mode.**
- `push_base` pushes `--atomic` with two refspecs: the base, and `<package>:refs/mac-worker/sessions/<task_id>`.
- The host pre-receive hook (`src/git_transport.rs:56`) allows `refs/mac-worker/sessions/*`. The hook is rewritten on every mirror open (`src/host_store.rs:3696-3703`), so a new helper widens it automatically.

**Controller mode.**
- The laptop's source stream pushes `refs/mac-worker/requests/<id>` and `refs/mac-worker/request-sessions/<id>`.
- `SourceSubmitBind` (`src/controller/transfer.rs:117`) gains an optional `session_oid`. The receive hook (`src/controller/transfer.rs:932-966`) checks both exact OIDs.
- `FrozenSubmitBody` (`src/prepared_submit.rs:21`) gains `session_import`. The request digest covers the package OID.
- Controller checkout (`src/controller/execute.rs:414-427`) fetches both refs. The controller → host push uses the same two-refspec `push_base`.
- `controller retry` resends only the envelope (`src/lib.rs:1090-1150`). This is safe because both refs land in the same stream before the mutation is sent.

**Origin-sourced tasks** never push a base (`src/turn_runner.rs:1345-1364`). In v1, `--from-session` requires a laptop snapshot source, otherwise `SESSION_REQUIRES_SNAPSHOT`.

**GC.**
- Host GC parses every `refs/mac-worker/bases/*` name as a TaskId (`src/gc.rs:921-927`). Session refs therefore get their own namespace and sweep: delete after placement, and delete orphans of terminal tasks.
- Laptop and controller pins follow their base pins.

## Decision 4 — placement in task prepare; the session id is the first turn id

Placement runs inside `TaskStore::prepare` after `prepare_workspace` (`src/task_store.rs:723-779`, `1867-1964`). There it runs under the admission and transfer locks, once per task, before any turn, and repeating it is harmless.

1. Read the package from the mirror ref. Verify the manifest hashes and caps.
2. Set the target id to the first turn's `session_seed` (hyphenated UUID). Claude already binds exactly this value (`src/job_service.rs:586`).
   - A retried prepare rewrites the same file.
   - Claude's existing seed bind agrees with the placed session instead of conflicting with it.
   - **(spike)** Codex accepts a v4 UUID where it normally mints v7.
3. Substitute the tokens: `@@MW_WORKSPACE@@` becomes the physical workspace path, and `@@MW_SESSION@@` becomes the target id.
4. Resolve the store root:
   - Claude: `CLAUDE_CONFIG_DIR` from the task's env profile, else `$HOME/.claude`.
   - Codex: `CODEX_HOME` from the profile, else `$HOME/.codex`.
   - Overrides made in login-shell rc files are unsupported and documented as such. Today `delete_native_session` ignores the profile (`src/task_store.rs:1667`); this wave applies the same resolution there.
5. Write the native files:
   - **Claude:** `<root>/projects/<encode(workspace)>/<id>.jsonl`, plus `<id>/` from the sidecar. The encoding is ported from Teleport's `claude_project_dir`, with attribution; it includes the JavaScript hash used for paths over 200 characters. Our workspace path is about 156 characters (`<home>/.local/share/mac-worker/host/tasks/<64 hex>/<32 hex>/workspace`). **(spike)** The encoding matches what Claude Code produces for that path.
   - **Codex:** `<root>/sessions/YYYY/MM/DD/rollout-<timestamp>-<id>.jsonl`, dated at placement time. **(spike)** Whether `state_5.sqlite`, `thread_history_1.sqlite` or `session_index.jsonl` must learn the thread. If so, prefer an agent-native import command if one exists over writing to its databases.
   - **All writes:** owner-only modes (0600 files, 0700 directories), no following of symlinks, temp file then rename. An existing file with different content is refused.
6. Bind `(agent, id)` with `bind_session`. Binding the same ref again is a no-op.
7. Delete the mirror ref.

Any failure stops prepare with `SESSION_PLACEMENT_FAILED` before the agent is spawned.

**Rejected alternatives:**
- **Placement in the supervisor per turn** (`src/supervisor.rs:1693-1711`): it runs on every turn and needs an extra once-only guard.
- **Placement in the prepare-turn helper:** that helper is cancel-unsafe (`src/prepare_turn.rs:198-220`).

**Cleanup.**
- An imported Codex session is deleted by `close --discard` through the existing `delete_session` (`src/task_store.rs:1646-1676`).
- An imported Claude transcript stays on disk, as it already does for every Claude pool task. That gap is pre-existing and deferred.

## Decision 5 — an imported first turn is a resume

- `TaskMeta` (`src/task.rs:753`) gains `session_import: Option<SessionImportMeta { agent, format, package_oid, source_agent_version }>`. It is frozen at submit and is the durable marker of an imported start. Turn number cannot serve as that marker, for the reason in the comment at `src/turn_runner.rs:1204-1207`.
- The runner replaces `resume = turn_number > 1` (`src/turn_runner.rs:1208`) with `TurnStart::{Fresh, Imported, FollowUp}`. An `Imported` turn:
  1. pushes the base and the session ref, then runs `task_prepare`, which performs placement;
  2. skips prebind discovery;
  3. calls `resume_turn(params, seed)`.
- The wire flag `TurnMaterial.resume` stays. Host `submit_turn` accepts resume on turn 1 only when the meta carries `session_import` and a binding exists (`src/job_service.rs:533-556`). The Claude seed bind is already skipped on resume.
- **Policy.** Resume argv restates the pool model and permission (`src/agent/claude.rs:62-63`, `src/agent/codex.rs:76-78`). If neither the task nor `.worker.toml` sets a model, the agent's default on the host applies. That default may differ from the laptop session's model, so submit prints the model that will run.
- **Prompt.** The composed task prompt (`src/task_client.rs:6567`) becomes the next user message of the resumed conversation. **(spike)** Claude `-p --resume` with `--json-schema` and stream-json continues a session that was created interactively. Codex `exec resume` continues a rollout whose originator is `codex-tui`.

## Decision 6 — secrets: explicit opt-in plus a structure-aware scrubber

- `--from-session` is the explicit consent. Without it nothing is shipped.
- The scrubber walks JSON string values only, never keys or structure. It replaces high-confidence token shapes with `[scrubbed]`:
  - `Bearer <token>`, `sk-ant-…`, `sk-…` of at least 20 characters;
  - `ghp_`, `gho_`, `ghs_`, `ghu_`, `github_pat_`;
  - `xox[abpr]-…`, `AKIA[0-9A-Z]{16}`;
  - PEM private-key blocks;
  - exact values from the task env profile's secrets, when the task uses a profile.

  The output stays valid JSONL.
- The scrub count is reported in submit output and in the manifest. v1 has no `--no-scrub`.
- **Not covered:** secrets the agent saw in file contents, such as a `.env` it read. This is documented. **(spike)** A scrubbed transcript still resumes.

**Rejected:** reusing `src/redaction.rs`, which breaks JSONL and corrupts tool output (Decision 1).

## Decision 7 — eligibility and the version gate

- **Host feature.** `task.session-import.v1` enters `HOST_FEATURES`. The scheduler maps host features into the capability `feature:<name>`; this mapping is new, because features are display-only today (`src/output.rs:44-51`). `--from-session` adds that requirement to `requires`. Old hosts are then excluded before the lease with `CAPABILITY_MISSING`, instead of failing late with `BASE_PUSH_FAILED` or `INVALID_REQUEST` (`src/git_transport.rs:113-114`, `src/lib.rs:4161-4166`).
- **Controller feature.** `controller.session-import.v1` is checked via health before freezing (pattern at `src/lib.rs:5855-5865`). Older controllers reject unknown `FrozenSubmitBody` fields (`src/prepared_submit.rs:22-24`).
- **Version.** The host's agent version (from facts) must be at least the version that produced the session:
  - Claude: per-line `version`;
  - Codex: `session_meta.cli_version`.

  A host below that is not eligible. A pinned worker fails with `SESSION_AGENT_TOO_OLD`; automatic placement waits or returns `CAPACITY_BUSY`. The laptop currently runs Codex 0.160.0 and the minis 0.159.2, so the spike exercises this case directly. **(spike)** It may justify relaxing the rule.
- **Mismatch.** `--agent cursor --from-session claude:…` fails with `SESSION_AGENT_MISMATCH`. Unsupported agents fail with `SESSION_IMPORT_UNSUPPORTED`, which carries the Decision 9 recipe as a hint.

## Decision 8 — finding the session on the laptop

- **Explicit:** `claude:<uuid>`, `codex:<uuid>`.
- **Latest:** `claude` or `codex` alone selects the newest session whose cwd is the project root or below it.
  - Claude: by mtime in `~/.claude/projects/<encode(root)>/*.jsonl`.
  - Codex: the newest 200 rollouts, by `session_meta.cwd`.

  Submit prints the chosen agent, id prefix, a preview of the first user prompt, the size and the scrub count.
- **Outside the project.** The session cwd must lie inside the project root, otherwise `SESSION_OUTSIDE_PROJECT`. The relative cwd is recorded. v1 places the session at the workspace root.
- **Live session.** A session modified less than 10 seconds ago gets a warning: the pool receives a snapshot as of now.
- **Dirty checkout.** The conversation assumes the current uncommitted state. A dirty checkout without `--wip` fails with `SESSION_NEEDS_WIP`, with the hint to add `--wip`.

## Decision 9 — handoff-note fallback (no transfer)

For Cursor, OpenCode, hosts that are too old, or continuing with a different agent, there is a documented recipe that uses existing features only:

1. Ask the agent to write `.worker/handoff.md`.
2. Run `worker task submit --wip --include .worker/handoff.md --prompt "Read .worker/handoff.md and continue the work it describes."`

The handoff prompt is adapted from Teleport's `handoff_prompt`: the goal, what is done, the current state including uncommitted changes, decisions and constraints, open questions, exact next steps. It ships as docs and as a section of the pool-dispatch skill. v1 adds no code for it.

## Decision 10 — Phase B sketch (pulling a session back)

- **Host operation `task-session-export`** (feature `task.session-export.v1`).
  - Allowed: Open and not busy; Closed with `session.json` still present.
  - Refused: Active, Queued, Abandoned, Lost. These mirror the fetch refusals (`src/task_client.rs:2582-2588`).
  - It exports the bound native session as a package (same codec, tokens in reverse) at `refs/mac-worker/session-exports/<task>/<nonce>`.
  - The upload-pack allowlist is widened (`src/git_transport.rs:821-858`).
- **Controller mode** takes two hops, like result fetch: prepare, pin, token (`src/controller/stream_rpc.rs:298-494`, `src/git_transport.rs:518-576`).
- **Laptop.** `--into <dir>` must be a git checkout, or `--worktree` creates one from `refs/remotes/mac-worker/<worker>/task/<id>`. The session is placed under a fresh UUID (a fork). The command prints `cd <dir> && claude --resume <id>`.

Phase B is detailed after Phase A lands.

## Deferred

| Cut | Reason |
| --- | --- |
| OpenCode (`opencode export` / `import`) | SQLite store and dialects (`src/agent/opencode.rs:61-63`); add after A proves the codec |
| Cursor | Chat store format unknown; adapter is resume-only |
| herdr pane as source (`--from-session pane:<id>`) | herdr 0.9.1 reports Claude session ids in `api snapshot` but not Codex (checked 2026-10-03) |
| Drain migration (Phase C) | Drain is pool-wide and gates handoffs only (`src/controller/drain.rs:1-6`); follow-ups are pinned to their worker (`src/task_client.rs:5243-5282`) |
| Events and dashboard badges | Events allow ids and stable codes only; failure codes are enough for v1 |
| Claude transcript cleanup on close | Pre-existing gap for every Claude task |
| Session cwd in a subdirectory | v1 places at the workspace root |

## Failure codes (v1)

All codes join the public catalog (`CATALOG` in `src/error.rs`, mapped in `build_catalog_error`) and its mirror in `docs/usage.md` between `<!-- error-catalog:start -->` and `<!-- error-catalog:end -->`, which a test compares.

| Code | Exit | Hint |
| --- | --- | --- |
| `SESSION_NOT_FOUND` | 64 | check the session id, or omit it to pick the newest session of this project |
| `SESSION_UNREADABLE` | 64 | the session is not valid JSONL; pick another session |
| `SESSION_TOO_LARGE` | 64 | the session exceeds 64 MiB; use the handoff-note recipe instead |
| `SESSION_OUTSIDE_PROJECT` | 64 | run the command from the project the session was started in |
| `SESSION_NEEDS_WIP` | 64 | add --wip so the pool sees the uncommitted state the session assumes |
| `SESSION_REQUIRES_SNAPSHOT` | 64 | origin-sourced tasks cannot carry a session; submit from the laptop snapshot |
| `SESSION_AGENT_MISMATCH` | 64 | drop --agent or make it match the session's agent |
| `SESSION_IMPORT_UNSUPPORTED` | 64 | only claude and codex sessions can be continued; use the handoff-note recipe |
| `SESSION_AGENT_TOO_OLD` | 75 | update the agent on the worker to at least the session's version |
| `SESSION_PLACEMENT_FAILED` | 70 | the worker could not install the session; check the worker and retry |

## T0 live spike — facts to establish before wave 2

The spike runs laptop-first with throwaway repos and throwaway sessions, never real user transcripts. Over plain SSH the minis' login keychain is locked, so Claude Code cannot authenticate there outside the pool's own keychain unlock (`src/supervisor.rs:1826`). Claude-on-mini evidence therefore comes from read-only listings in T0 and from the pool in T8. Codex uses file credentials and runs over SSH on mini-3 (`mac3`). Results go to `docs/superpowers/validation/2026-10-03-session-transfer-spike.md`.

| # | Experiment | Where | Pass criterion |
| --- | --- | --- | --- |
| S1 | Claude: an interactive source session (2–3 turns with file edits), normalized, id rewritten to a fresh v4, placed under the encoded host-shaped workspace path, then the exact pool resume argv from `src/agent/claude.rs` | laptop | Recalls earlier turns; the stream `session_id` equals the placed id (no fork); appends to the same file |
| S2 | Claude sidecar (`<id>/` subagent transcripts, tool results) present versus dropped | laptop | Resume works in both cases; record whether the sidecar matters |
| S3 | Claude project-dir encoding: a ~156-character host-shaped path and a >200-character path, compared with the directory Claude creates; plus a read-only listing of `~/.claude/projects` against real pool workspaces | laptop, mini (read-only) | The algorithm in Decision 4 reproduces every observed directory name |
| S4 | Codex: interactive (`codex-tui`) source rollout from laptop 0.160.0 placed with v4 and v7 ids, no database entries, then the exact pool resume argv from `src/agent/codex.rs`; read the Codex source for how `exec resume` finds a thread | laptop and mini-3 at 0.159.2 | Thread found and context recalled; database side effects recorded; `codex delete --force <id>` works; version skew result recorded |
| S5 | Codex account: compare `session_meta.creator_account_id` of a laptop rollout with one created on mini-3 (never read `auth.json`) | laptop, mini-3 | Resume unaffected, or the constraint is documented |
| S6 | A scrubbed token in a message, for both agents | laptop | Resume still works |
| S7 | Large transcripts | deferred to T8 | Measured with a real session by the owner |
| S8 | Reverse: a resumed session copied to a third checkout path with a new id, then resumed there | laptop | Continues with context (Phase B evidence) |

## Owner decisions

1. **Scope:** Phase A in this wave; Phase B next. Approved 2026-10-03.
2. **Secrets:** high-confidence scrubber plus explicit opt-in, limitation documented. Approved 2026-10-03.
3. **Version gate:** strict (host ≥ source) until T0 S4 justifies relaxing it. Approved 2026-10-03.
4. **T0:** laptop-first; Codex experiments on mini-3 outside the pool's host tree, cleaned up afterwards. Decided by the orchestrator under the owner's go-ahead.
