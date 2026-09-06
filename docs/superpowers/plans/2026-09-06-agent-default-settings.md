# Agent default settings implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Show and edit each connected worker's native agent model and supported effort defaults in the existing light Settings page.

**Architecture:** Reuse the fixed-command `SshJsonTransport` for two typed host operations and expose a separate dashboard settings API. Read only allowlisted fields from the native CLI configuration. Save explicitly with an optimistic revision check and an atomic file update, preserving unrelated configuration. The UI keeps drafts separate from the polling dashboard snapshot.

**Tech Stack:** Rust, axum, serde, TOML/JSON configuration, vanilla JavaScript and CSS.

**Spec:** The user requested showing and changing defaults after inspecting the defaults on mini-1, mini-2 and mini-3. Native per-worker CLI defaults are the working scope; an optional clarification was offered. The editor belongs in the approved Observatory Light Settings page. Authentication and CLI version remain informational. Changes affect subsequent CLI launches; task, environment and project settings may override native user settings. No actual model choices are changed during development or verification.

## Global Constraints

- Implementers and reviewers use `gpt-5.6-luna` with reasoning effort `max`, as explicitly requested by the user.
- Preserve all existing dirty files and the concurrent Claude Code implementation of task `--effort` and question options. No commits, resets, broad replacement scripts or unrelated reformatting.
- Work in the user's existing shared checkout so the already implemented UI and the parallel session's compatible changes remain integrated. Use narrow edits and re-read shared sections immediately before changing them.
- No raw configs, secrets, credentials or environment dictionaries in API responses or logs.
- Only four agent IDs are accepted: `codex`, `claude`, `cursor`, `opencode`. No caller-supplied file paths or shell command fragments.
- HTTP remains loopback-only with exact Host validation, existing CSP and no CORS. Mutation requires an exact same-origin Origin, JSON content type, and `X-Mac-Worker-Settings: 1`; reject invalid requests before invoking the backend. Cap settings request bodies at 8192 bytes.
- Reads have no filesystem writes. Settings save requires a revision matching current source bytes; stale revisions return a conflict and do not overwrite. Invalid or ambiguous config remains untouched. Preserve unrelated settings and comments; use parser-backed edits where available. Native defaults are not guessed from the last task.
- Model values are optional bounded single-line nonempty strings; null means remove the explicit native override. Effort values must be in the selected agent's verified supported set; unsupported fields are not editable.
- UI draft values survive ordinary snapshot refreshes. Saving is explicit, prevents duplicate requests and keeps errors visible. Worker/agent switching cannot apply a stale response or silently save a draft to a different destination.
- Test all writes in temporary fixture homes. Live pool verification is read-only. Do not alter real model/effort defaults as a test.

---

### Task 1: Native settings store, typed transport and protected HTTP API

**Files:**
- Create `src/agent_settings.rs` and focused helper modules under `src/agent_settings/` if needed for native document editing.
- Create `src/dashboard/settings.rs`.
- Modify `src/dashboard/mod.rs`, `src/dashboard/command.rs`, `src/dashboard/web.rs`.
- Modify only the new-operation registration sections of `src/cli.rs`, `src/lib.rs`, `src/transfer.rs`.
- Add `tests/agent_settings.rs` and `tests/dashboard_settings.rs`; update dashboard HTTP state test constructors mechanically where required.
- Add focused parser dependencies only when they materially improve lossless edits; record the reason.

**Interfaces:**
- Native DTO `AgentDefaultSettings`: `agent: String`, `model: Option<String>`, `effort: Option<String>`, `effort_options: Vec<String>`, `source: String`, `revision: Option<String>`, `writable: bool`, `message: Option<String>`.
- List response: `{ agents: AgentDefaultSettings[] }` with stable order codex, cursor, opencode, claude; each agent reports its own read/config error without failing the other three.
- Save request: `{ agent: String, model: String|null, effort: String|null, revision: String }`. Reject unknown fields. Save response is the fresh `AgentDefaultSettings` object after persistence.
- Host commands `worker host agent-settings-get` (empty JSON request) and `worker host agent-settings-set` (typed save request), using existing canonical control-response/error conventions.
- Dashboard `GET /api/v1/workers/{worker_name}/agent-settings` returns the list response; `POST` at the same route accepts the save request and returns the fresh agent entry. Worker names must resolve exactly in configured workers. A missing or older remote operation produces a safe useful unavailable message, not a panic.
- `DashboardSettingsSource` trait provides `read(worker_name)` and `save(worker_name, request)`; the system implementation holds config and process runner. HTTP state can optionally hold this source with an explicit unavailable fallback for tests not using settings.
- Conflict error code `SETTINGS_CONFLICT`, HTTP 409. Invalid request 400, unavailable/transport failure 502 or 503. Error messages contain no config bytes or raw child stderr.

- [x] Read `/private/tmp/mac-worker-settings-discovery.md` for native field/path evidence. Confirm the native precedence and supported effort values before implementing each adapter; surface source/override caveats rather than inventing effective values.
- [x] Write focused behavioral tests using temporary homes for all supported native formats, preserving unrelated fields/comments, missing explicit values, malformed config, invalid input, revision conflict, missing-file creation, symlink/nonregular refusal and read-without-write behavior. Test missing and unsupported efforts with the same request shape as HTTP.
- [x] Run the new tests to establish failures, then implement the bounded native store and parser-backed field edits. Use existing filesystem primitives where compatible with native config permissions; do not chmod existing user directories as part of a read or save.
- [x] Add typed fixed-command transport and host dispatch with bounded input/output and deadline. Test the exact fixed remote argv and that user values travel in JSON stdin only. Re-read concurrent CLI/lib changes immediately before inserting new arms.
- [x] Add the dashboard source and protected endpoints. Test valid read/save, cross-origin and absent custom-header rejection, invalid agent and unknown worker rejection, oversized JSON rejection, conflict status and secret-free errors. Rejected requests must not call the source's save function.
- [x] Run focused native, transport and dashboard tests; format only owned Rust files. Write a report with exact commands, resulting DTO JSON examples and remaining compatibility limits.

### Task 2: Settings editor in the approved light dashboard

**Files:**
- Modify `src/dashboard/static/dashboard.mjs`, `src/dashboard/static/dashboard.css`, `src/dashboard/static/index.html` and `tests/dashboard_client.mjs`.
- Modify `README.md` only in dashboard/settings documentation, after re-reading concurrent edits.

**Interfaces:**
- Consume Task 1's GET/POST endpoint and DTOs exactly as defined above.
- Keep `createDashboardClient` existing dependency injection and safe DOM helpers. Add a settings fetch/save controller through existing view callbacks or a focused helper; do not introduce a framework.
- Native model and effort appear in the agent table and selected agent detail. Latest-task values remain distinctly labeled when useful, never displayed as native defaults.
- In the detail panel show model input, an effort select for verified supported agents, source/scope help, Save changes and Cancel. Null values display as “CLI default”; unsupported effort displays “Not supported”. Preserve existing connection, authentication, CLI version and project-default information.

- [x] Inspect the current Settings rendering and existing fake DOM tests. Add focused tests for loading real defaults, drafts surviving snapshot polls, Cancel, explicit save payload/headers, loading and failure states, revision conflict and stale worker/agent responses. Verify invalid text remains text content.
- [x] Fetch settings when opening Settings or changing selected worker, cache by worker, and provide refresh/retry. Disable editing until a writable current response is available. Do not launch new settings requests on every 2-second snapshot poll.
- [x] Implement model and effort controls in the existing layout, preserving visual tokens and mobile behavior. Prevent selection switches from silently losing a dirty draft by retaining drafts per worker/agent or by an explicit inline discard action. On save, update only that destination's cache; errors retain the draft.
- [x] Remove obsolete blanket “READ ONLY” UI copy where settings mutations make it inaccurate. Explain native user scope and future launches in plain language.
- [x] Run `node --test tests/dashboard_client.mjs`; format only owned frontend files and visually verify desktop/mobile screenshots through a fixture server. Write a report with exact commands and screenshot paths.

### Task 3: Integration, documentation and verification

**Files:**
- Narrow follow-up integration changes in Task 1/2-owned files only, with focused tests as needed.
- Update this plan's checkboxes and reports with outcomes.

- [x] Review Task 1 and Task 2 diffs independently for spec compliance and quality on Luna max. Resolve concrete findings before completion.
- [x] Reconcile the parallel Claude Code effort changes. If metadata now has effort, ensure existing dashboard task projections expose that value instead of a hardcoded unknown; delegate any required implementation to Luna max.
- [x] Run focused Rust dashboard/settings tests, frontend tests and lint/format checks on the integrated result; broaden to all-target compilation or full tests when new integration failures require it.
- [x] Verify browser Save/Cancel and save/read roundtrips with in-memory fixtures, protected Rust HTTP writes with temporary fixture tests, and real embedded-server desktop/mobile reads without changing live defaults. The composed verification scope is recorded in root-verification.md.
- [x] Deploy the updated binary through the project's existing install/setup flow where required to make the new remote RPC usable, preserving active tasks and native configuration. Restart the user's dashboard and verify real mini defaults read successfully. If deployment is incompatible with active tasks, report the exact blocker and finish unaffected work.
- [x] Complete a final Luna max review of the feature, report the running URL and the implemented behavior with any concrete remaining limitation.
