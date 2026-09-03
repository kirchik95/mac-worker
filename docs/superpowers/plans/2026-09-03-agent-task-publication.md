# Agent Task Publication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the agent-task core with origin-backed bases, optional origin publication, profile-keyed capabilities, end-to-end Cursor and OpenCode turns, and bounded task retention without changing the durable task or queue authority contracts.

**Architecture:** The phase 5d work consumes the phase 5a–5c transfer repository, mirror, task store, turn publisher, runner, queue, and adapter seams. Origin source and push are host-side Git operations guarded by the configured `origin:<host>` capability; Cursor and OpenCode remain pure adapter descriptions while the host owns session binding, prompt delivery, environment loading, and turn supervision. Retention is a projection of existing task, mirror, job, and transfer namespaces, applied only by the explicit `worker gc --apply` command.

**Tech Stack:** Rust 2024; existing `serde`/`serde_json` canonical records, `sha2`, `uuid`, `humantime`, `libc`, descriptor-relative `rooted_fs`; system Git, OpenSSH, and agent CLIs through `ProcessRunner`; existing `assert_cmd`, `predicates`, `proptest`, and `tempfile` test support. No new crates.

**Spec:** `docs/superpowers/specs/2026-09-03-agent-task-pool-design.md` sections 10.1, 10.3, 11.2, 12.4, 13.1, 16, 17, 20.2, 21, and 22; the core contracts in `docs/superpowers/plans/2026-09-03-agent-task-execution-core.md`, especially its Global Constraints, Tasks 9–10, and Delivery Boundary.

## Global Constraints

- Every task in this plan is gated on the core plan's Task 10 landing in `main` with its focused suites, full local gate, and sanitized acceptance record complete. A phase 5d implementation must rebase on that exact `main` state before touching any core-owned file.
- The standalone `src/agent_facts.rs` collector is a separate pure extraction of core Task 9 delivered by this worktree. It has no probe, protocol, transport, install, or CLI wiring; phase 5d consumes it only after the core Task 10 gate.
- Do not add public options or commands beyond spec section 7.1. `--publish-branch` is valid only with `publish = push`; `source = origin` and `publish = push` remain rejected until the corresponding task in this plan is complete.
- `source = origin` requires a normalized origin URL and the configured `origin:<host>` capability. Submission checks that the exact `base_oid` is advertised by origin and reports `BASE_NOT_ON_ORIGIN` before creating a task, queue row, base ref, or remote state.
- Origin preparation fetches the exact recorded object into the worker mirror and verifies that it is a commit before creating any workspace. Missing, mismatched, or non-commit objects report `BASE_UNAVAILABLE` and leave no workspace.
- `publish = push` is additive to the mandatory fetch publication, requires a committed base, validates a branch name, and uses a branch unique within the run. A push failure is `PUBLISH_FAILED`; it leaves the task open and retains the mirror branch for retry or inspection.
- The mirror branch remains `task/<task_id>` regardless of `--publish-branch`. No task may share a mirror branch, and no host operation may accept a caller-supplied path.
- Cursor and OpenCode turns use the existing pure adapters, the host's login-shell launch, and the argv-pointer prompt contract. Prompts never appear in argv, process listings, task records, queue rows, or public errors. Cursor uses `--force`; OpenCode uses `--auto`; workspace policy falls back to unattended only where the adapter records that fallback.
- Cursor's first turn is bound by a host-side `prebind_session` call before launch. OpenCode's session is captured from its first JSON event. Resume always uses the recorded session reference and fails with `SESSION_UNBOUND` when it is absent; no adapter may resume the most recent session.
- Authentication facts are profile-keyed and stale facts count as `unknown`. Plain authentication produces `agent:<name>`; a secure profile produces `agent:<name>@<profile>`. Insecure profiles are reported as insecure and are never applied to a probe or turn.
- Profile values are read only on the worker, never copied, serialized, logged, included in errors, or returned through `worker workers`. Only profile names, secure state, and environment-variable names may cross a record boundary.
- All agent checks use the `ProcessRunner` boundary with a two-second deadline and 4 KiB stdout and stderr bounds. Agent command errors become `Unknown` facts without copying command output into diagnostics.
- Retention is explicit and rooted: open tasks close after the configured task retention; mirror branches prune after branch retention; empty mirrors and transfer repositories are candidates only when no task or base ref protects them; task metadata and turn jobs follow v1 retention. `gc` previews every candidate and never prunes another mirror ref.
- Every new persisted record uses strict canonical serialization, rejects unknown and duplicate fields, uses owner-only modes and no-follow validation, and contains no credentials, environment values, session secrets, or unbounded agent output.
- No task in this plan contacts a worker during implementation or local verification. The live acceptance task is an operator-run gate after the core prerequisite is on `main`; this worktree records no live hostnames, paths, tokens, session identifiers, or transcript text.

## File Map

```text
src/agent_facts.rs                  pure profile-keyed agent facts collector (standalone prerequisite from core Task 9)
src/agent/mod.rs                    adapter probe descriptors and optional native-session deletion seam
src/agent/cursor.rs                 Cursor status/prebind/delete probe descriptions, if supported by the installed CLI
src/agent/opencode.rs               OpenCode auth probe description and session cleanup capability
src/probe.rs                        refresh-facts cache read/write and stale-fact projection after the core gate
src/protocol.rs                     protocol-4 agent facts, profile facts, age, and origin capability DTO fields
src/scheduler_adapter.rs            profile-keyed agent capabilities and origin capability requirements
src/transport.rs                    fixed Git SSH and host-operation clients for origin fetch and push
src/git_transport.rs                origin base fetch and result push through the validated worker mirror
src/transfer.rs                     typed task-prepare/task-publish requests and bounded remote responses
src/task.rs                         source/publish validation, committed-base requirement, and run branch uniqueness
src/task_store.rs                   origin preparation metadata, retained branch markers, and discard cleanup
src/turn.rs                          Cursor/OpenCode launch/session binding and push publication hook
src/task_client.rs                  source/publish preflight and publish-branch/run validation
src/turn_runner.rs                   origin capability admission and stale-fact refresh before claiming
src/host_store.rs                    mirror/task retention namespaces and rooted GC candidates
src/cli.rs                           only the section 7.1 task and workers --refresh grammar
src/lib.rs                           hidden refresh/publish/runner dispatch after the core gate
src/output.rs                        sanitized agent/profile/origin/retention reports
tests/agent_publication.rs           origin source, push publication, and branch uniqueness
tests/agent_capabilities.rs          profile-keyed facts, origin capabilities, stale facts, and privacy
tests/agent_cursor_opencode.rs       prebind, argv pointers, policies, env names, resume, and result continuity
tests/task_gc.rs                     open-task, branch, mirror, transfer, and session retention
tests/phase-five-d-acceptance.rs     sanitized live-acceptance evidence shape and CLI/output assertions
docs/phase-five-validation.md        core validation record extended with phase 5d item 6 and adapter rows
README.md                            phase 5d commands, profiles, origin publication, and retention guidance
```

The current worktree's pure module and its focused tests are deliberately absent from the phase 5d integration files above: they are delivered independently so the core branch can consume them without modifying `probe.rs`, `protocol.rs`, `transport.rs`, or `install.rs`.

---

### Task 1: Profile-Keyed Facts and Capability Integration

**Gate:** The core plan's Task 10 must be present on `main`, including protocol 4, the core `AgentFacts` collector contract, the phase-five validation record, and its complete local/live gates. The standalone facts module may be independently reviewed, but no integration change in this task starts before that gate.

**Files:**
- Consume: `src/agent_facts.rs`
- Modify: `src/agent/mod.rs`, `src/probe.rs`, `src/protocol.rs`, `src/scheduler_adapter.rs`, `src/transport.rs`, `src/install.rs`, `src/turn_runner.rs`, `src/cli.rs`, `src/lib.rs`
- Create: `tests/agent_capabilities.rs`
- Modify: `tests/workers_command.rs`, `tests/doctor_command.rs`, `tests/setup_command.rs`, `tests/scheduler_adapter.rs`, `tests/job_protocol.rs`

**Interfaces:**
- Consumes: `AgentFacts`, `AgentProbe`, `AgentAuth`, `ProfileProbe`, `FACTS_TTL`, `AgentKind`, configured worker entries, and the core protocol-4 probe/worker-health records.
- Produces: `host refresh-facts`, `worker workers --refresh`, cached facts with age, profile-keyed `agent:<name>` and `agent:<name>@<profile>` capabilities, `git_identity`, and adapter-owned auth probe specifications for Codex, Claude, Cursor, and OpenCode.

- [ ] **Step 1: Write the failing integration tests**

Add tests that exercise the existing pure collector through the phase 5d boundaries:

```rust
#[test]
fn authenticated_profile_adds_only_the_profile_keyed_agent_capability() {
    let facts = facts_with(
        vec![agent("claude", AgentAuth::Unauthenticated, vec![("agents", AgentAuth::Authenticated)])],
        vec![profile("agents", true)],
        false,
        10_000,
    );
    let observation = observation_from_facts(facts, 10_001);
    assert!(observation.capabilities().contains(&"agent:claude@agents".to_owned()));
    assert!(!observation.capabilities().contains(&"agent:claude".to_owned()));
}

#[test]
fn stale_facts_are_reported_but_never_satisfy_an_agent_requirement() {
    let facts = facts_with(vec![agent("codex", AgentAuth::Authenticated, vec![])], vec![], true, 0);
    let observation = observation_from_facts(facts, FACTS_TTL + 1);
    assert!(observation.facts_age_millis() > FACTS_TTL);
    assert!(!observation.capabilities().iter().any(|capability| capability == "agent:codex"));
}

#[test]
fn refresh_facts_is_the_only_path_that_runs_agent_commands() {
    let runner = RecordingRunner::with_probe_results();
    let cached = host_probe_with_cached_facts(&runner);
    assert!(cached.is_ok());
    assert!(runner.requests().iter().all(|request| !is_agent_request(request)));
    refresh_facts(&runner).unwrap();
    assert!(runner.requests().iter().any(|request| is_agent_request(request)));
}
```

Also assert that `cursor-agent status` and `opencode auth list` are bounded and profile-keyed, secure profiles are the only profiles applied, `git_identity` is true only when both login-shell Git values exist, profile values are absent from every record/error, and `worker workers` reports names, secure state, versions, auth states, and fact age without raw probe output.

- [ ] **Step 2: Run the focused integration tests to verify RED**

Run: `cargo test --locked --test agent_capabilities --test workers_command --test scheduler_adapter -- --nocapture`

Expected: FAIL because the core probe response and scheduler projection do not yet consume the standalone facts record for phase 5d agent/profile capabilities.

- [ ] **Step 3: Implement the profile/origin capability projection**

Add adapter-owned probe descriptors with these exact command categories and classifiers:

```rust
Codex:    binary "codex",       version ["--version"], auth ["login", "status"]
Claude:   binary "claude",      version ["--version"], auth ["auth", "status"]
Cursor:   binary "cursor-agent",version ["--version"], auth ["status"]
OpenCode: binary "opencode",    version ["--version"], auth ["auth", "list"]
```

Resolve each binary with `zsh -lc 'command -v <bin>'` once. Run the version check once in the plain login-shell environment. Run the auth check once plainly and once for each secure profile, adding that profile's entries to the child environment without ever copying the values to a record. Classify Codex as authenticated only for a successful `login status` containing `Logged in` and explicitly unauthenticated for a successful not-logged-in response; classify Claude from JSON `loggedIn: true|false`; classify Cursor from an unambiguous successful authenticated/not-authenticated `status` response; classify OpenCode as authenticated when a successful `auth list` reports at least one configured provider and unauthenticated when it succeeds with an empty list. Any timeout, non-UTF-8 output, nonzero exit, keychain/network error, or ambiguous output is `Unknown`. The auth output is discarded after classification.

Persist `AgentFacts` under the host facts cache using the existing canonical atomic-write/no-follow boundary. `host probe` reads the cache and reports its age without launching an agent. `host refresh-facts`, setup's post-install refresh, `worker workers --refresh`, and a stale runner refresh are the only collection paths. `SchedulerProbeAdapter` appends plain and profile-keyed capabilities only when `is_stale(now)` is false; origin capabilities remain inventory-declared and are never inferred from a Git command.

- [ ] **Step 4: Run the integration regression suite to verify GREEN**

Run: `cargo test --locked --test agent_capabilities --test workers_command --test doctor_command --test setup_command --test scheduler_adapter --test job_protocol -- --nocapture`

Expected: PASS with no protocol bump beyond the core version 4 and no agent command from the read-only probe path.

- [ ] **Step 5: Commit the capability integration**

```bash
git add src/agent/mod.rs src/agent/cursor.rs src/agent/opencode.rs src/probe.rs src/protocol.rs src/scheduler_adapter.rs src/transport.rs src/install.rs src/turn_runner.rs src/cli.rs src/lib.rs tests/agent_capabilities.rs tests/workers_command.rs tests/doctor_command.rs tests/setup_command.rs tests/scheduler_adapter.rs tests/job_protocol.rs
git commit -m "feat: project profile-keyed agent capabilities"
```

---

### Task 2: Origin Bases and Push Publication

**Gate:** The core plan's Task 10 is on `main`, and Task 1 of this plan's capability projection passes its focused suite. The phase 5a transfer/mirror and phase 5c runner contracts must be present exactly as landed by the core plan.

**Files:**
- Modify: `src/task.rs`, `src/task_client.rs`, `src/transfer_repo.rs`, `src/git_transport.rs`, `src/transfer.rs`, `src/task_store.rs`, `src/turn.rs`, `src/turn_runner.rs`, `src/output.rs`, `src/error.rs`, `src/cli.rs`, `src/lib.rs`
- Create: `tests/agent_publication.rs`
- Modify: `tests/task_command.rs`, `tests/task_materialization.rs`, `tests/task_turn.rs`, `tests/turn_runner.rs`, `tests/cli_help.rs`

**Interfaces:**
- Consumes: normalized origin URL/host, `BaseOid`, `TaskSource`, `PublishMode`, `TaskId`, `RunRecord`, `AgentFacts` capability projections, `HostStore::mirror`, `GitTransport`, and the existing turn publisher.
- Produces: exact-origin preflight, host origin fetch, committed-base validation, unique `--publish-branch` validation within a run, origin push, `BASE_NOT_ON_ORIGIN`, `BASE_UNAVAILABLE`, `PUBLISH_REQUIRES_COMMITTED_BASE`, and `PUBLISH_FAILED` behavior.

- [ ] **Step 1: Write the failing origin/publication tests**

Add concrete fake-transport tests:

```rust
#[test]
fn origin_source_rejects_a_base_not_advertised_before_any_local_mutation() {
    let harness = TaskHarness::with_origin_base("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef", false);
    let error = harness.submit_origin_task().unwrap_err();
    assert_eq!(error.public_code(), "BASE_NOT_ON_ORIGIN");
    assert!(harness.local_tasks().is_empty());
    assert!(harness.queue().is_empty());
    assert!(!harness.transfer_repo_has_base_ref());
    assert!(harness.user_repository_unchanged());
}

#[test]
fn origin_prepare_fails_before_workspace_when_the_exact_commit_is_unavailable() {
    let harness = TaskHarness::with_origin_base("0123456789012345678901234567890123456789", true)
        .with_worker_mirror_without_base();
    let error = harness.drive_to_prepare().unwrap_err();
    assert_eq!(error.public_code(), "BASE_UNAVAILABLE");
    assert!(!harness.workspace_exists());
}

#[test]
fn push_requires_a_committed_base_and_unique_run_branches() {
    let wip = TaskHarness::with_wip_base().submit_with_push().unwrap_err();
    assert_eq!(wip.public_code(), "PUBLISH_REQUIRES_COMMITTED_BASE");

    let run = TaskHarness::new_run(2);
    run.submit_push_task("release-candidate").unwrap();
    let duplicate = run.submit_push_task("release-candidate").unwrap_err();
    assert_eq!(duplicate.public_code(), "TASK_CONFIG_INVALID");
}

#[test]
fn push_uses_publish_branch_and_failure_keeps_the_task_open() {
    let success = TaskHarness::with_origin_capability().complete_push("release-candidate").unwrap();
    assert_eq!(success.pushed_branch(), "release-candidate");
    assert!(success.mirror_ref_exists("refs/heads/task/task0001"));

    let failed = TaskHarness::with_origin_capability().push_result_fails();
    let report = failed.complete_turn().unwrap();
    assert_eq!(report.public_code(), "PUBLISH_FAILED");
    assert_eq!(failed.task_state(), TaskState::Open);
    assert!(failed.mirror_ref_exists("refs/heads/task/task0001"));
}
```

Also assert that origin requirements exclude workers without `origin:<host>`, a pinned worker lacking that capability returns `CAPABILITY_MISSING`, local `fetch` publication still occurs when `push` is requested, normalized URLs are the only values passed to Git, and a failed push never deletes the base or result branch.

- [ ] **Step 2: Run the publication tests to verify RED**

Run: `cargo test --locked --test agent_publication --test task_command --test task_materialization --test task_turn -- --nocapture`

Expected: FAIL because core publication rejects origin/push at task validation and has no origin fetch/push host operation.

- [ ] **Step 3: Implement exact-object origin fetch and push publication**

Keep reference resolution on the MacBook. For `source = origin`, normalize and validate the project's origin URL, run a bounded `git ls-remote` preflight for the exact `base_oid`, and stop with `BASE_NOT_ON_ORIGIN` before writing the transfer repository or local task state when the object is absent. Persist only the normalized host and exact object ID needed by the task record; do not persist credentials or raw remote output.

Add a typed host operation for origin preparation that receives only validated task/turn identity, normalized origin URL, and `base_oid`. Under the live lease, run `git fetch <origin-url> <base_oid>` in the mirror with global/system Git configuration neutralized, verify `git cat-file -t <base_oid> == commit`, and return `BASE_UNAVAILABLE` before any workspace is created when the fetch or verification fails. The mirror's existing receive hook remains limited to base refs; origin fetch is a host-side operation and never permits arbitrary client refs.

For `publish = push`, reject `BaseKind::Wip` at task preflight, require the inventory capability `origin:<host>`, validate `--publish-branch` using `BranchName`, and reserve the branch name under the run record before enqueueing. After the publisher brings `refs/heads/task/<task_id>` into the mirror, push that mirror ref to the normalized origin branch through a bounded, typed host operation. Always perform the normal fetch publication as well. A push error records `PUBLISH_FAILED`, keeps the task `Open`, keeps the workspace and mirror branch, and leaves retry/close to a later mutating task command.

- [ ] **Step 4: Run the publication regression suite to verify GREEN**

Run: `cargo test --locked --test agent_publication --test task_command --test task_materialization --test task_turn --test turn_runner --test cli_help -- --nocapture`

Expected: PASS with no user-repository writes before result import, exact origin capability routing, and stable public error codes.

- [ ] **Step 5: Commit origin publication**

```bash
git add src/task.rs src/task_client.rs src/transfer_repo.rs src/git_transport.rs src/transfer.rs src/task_store.rs src/turn.rs src/turn_runner.rs src/output.rs src/error.rs src/cli.rs src/lib.rs tests/agent_publication.rs tests/task_command.rs tests/task_materialization.rs tests/task_turn.rs tests/turn_runner.rs tests/cli_help.rs
git commit -m "feat: fetch task bases from origin and publish branches"
```

---

### Task 3: Cursor and OpenCode End-to-End Turns

**Gate:** The core plan's Task 10 and Tasks 1–2 of this plan are on `main` and their focused suites pass. No live worker is used by the local tests; the adapter/host behavior is proven by fake ProcessRunner and fake host transport first.

**Files:**
- Modify: `src/agent/mod.rs`, `src/agent/cursor.rs`, `src/agent/opencode.rs`, `src/turn.rs`, `src/task_store.rs`, `src/transfer.rs`, `src/turn_runner.rs`, `src/supervisor.rs`, `src/task_client.rs`, `src/cli.rs`, `src/lib.rs`, `src/output.rs`
- Create: `tests/agent_cursor_opencode.rs`
- Modify: `tests/agent_adapters.rs`, `tests/task_command.rs`, `tests/task_conversation.rs`, `tests/task_turn.rs`, `tests/turn_runner.rs`, `tests/agent_capabilities.rs`

**Interfaces:**
- Consumes: existing `CursorAdapter`, `OpencodeAdapter`, `AgentAdapter::prebind_session`, `render_shell`, `TaskTurnRequest`, `SessionBinding`, `EnvProfile`, `TurnPublisher`, profile-keyed capabilities, and the worker account's login-shell environment.
- Produces: host prebinding for Cursor, event-bound sessions for OpenCode, argv-pointer prompt delivery, `--force`/`--auto` policies, env-profile application, resume continuity, and live-acceptance evidence requirements for both agents.

- [ ] **Step 1: Write the failing Cursor/OpenCode lifecycle tests**

Add tests with a recording host and process runner:

```rust
#[test]
fn cursor_prebind_happens_before_first_turn_and_the_chat_id_is_persisted() {
    let harness = TaskHarness::cursor();
    harness.submit_first_turn().unwrap();
    assert_eq!(harness.host_calls(), vec!["cursor-agent create-chat", "task-turn"]);
    assert_eq!(harness.persisted_session(), Some("chat0001"));
    assert!(harness.last_shell().contains("'--resume' 'chat0001'"));
    assert!(harness.last_shell().contains("'--force'"));
    assert!(harness.last_shell().contains("\"$MAC_WORKER_TURN_DIR/prompt.md\""));
    assert!(!harness.last_shell().contains("Fix the planted secret"));
}

#[test]
fn opencode_binds_session_from_json_and_resumes_with_auto() {
    let harness = TaskHarness::opencode().with_stream("ses0001");
    harness.submit_first_turn().unwrap();
    assert_eq!(harness.persisted_session(), Some("ses0001"));
    harness.say("continue the migration").unwrap();
    assert!(harness.last_shell().contains("'--session' 'ses0001'"));
    assert!(harness.last_shell().contains("'--auto'"));
    assert!(harness.last_shell().contains("\"$MAC_WORKER_TURN_DIR/prompt.md\""));
}

#[test]
fn cursor_and_opencode_use_profile_values_only_in_the_child_environment() {
    let harness = TaskHarness::cursor().with_profile("agents", "CURSOR_API_KEY", "secret-value");
    harness.submit_first_turn().unwrap();
    assert_eq!(harness.child_env("CURSOR_API_KEY"), Some("secret-value"));
    assert!(!harness.task_json().contains("secret-value"));
    assert!(!harness.error_text().contains("secret-value"));
}
```

Also cover OpenCode's `--auto`, Cursor's `--force`, workspace-policy fallback metadata, no path/prompt in argv, `SESSION_UNBOUND` on resume without a binding, malformed/truncated trailer results remaining `Unknown`, cancellation followed by same-session resume, and a failed first turn retaining the binding for `say`.

- [ ] **Step 2: Run the adapter/lifecycle tests to verify RED**

Run: `cargo test --locked --test agent_cursor_opencode --test agent_adapters --test task_conversation --test task_turn -- --nocapture`

Expected: FAIL because the host does not yet execute prebinding, persist Cursor/OpenCode sessions through the turn boundary, or include their profile-keyed capability requirements in the runner path.

- [ ] **Step 3: Wire the existing pure adapters through the host turn lifecycle**

Before a first Cursor turn, execute the adapter's fixed `prebind_session` argv in the worker login shell with the selected secure profile, bounded output, and no prompt. Parse only the returned chat identifier, validate and bound it, and write `session.json` before `task-turn` launch. The first Cursor launch uses the recorded identifier with `--resume`, `--trust`, `--force`, and the quoted `"$MAC_WORKER_TURN_DIR/prompt.md"` pointer. Do not add `--workspace` or any absolute worker path to the fingerprinted shell; the supervisor's working directory is the task workspace.

For OpenCode, launch the existing `opencode run --format json --auto [--model]` adapter from the task workspace with the quoted prompt pointer. Bind the first `sessionID` observed in the JSON stream before publication; use `opencode run --session <id> --format json --auto` for later turns. Keep trailer result extraction and `Unknown` classification unchanged. Both agents inherit the worker account's real `HOME`/login-shell environment plus only the selected secure profile; all profile values remain child-only.

Make `say` refresh the recorded worker and require the persisted binding. Reuse the existing hard pin and new turn runner, so a busy worker waits without rerouting. On cancel, timeout, prelaunch failure, or lost-turn reconciliation, retain the session binding unless the user explicitly discards the task. Native deletion is an optional adapter operation; call it on discard only when the installed adapter exposes a documented delete command, and record no failure when the agent has no deletion operation.

- [ ] **Step 4: Run the Cursor/OpenCode regression suite to verify GREEN**

Run: `cargo test --locked --test agent_cursor_opencode --test agent_adapters --test task_conversation --test task_turn --test turn_runner --test task_command -- --nocapture`

Expected: PASS with one session per task, no prompt in argv, no secret in local records/errors, and no message injected into a running process.

- [ ] **Step 5: Commit the adapter lifecycle**

```bash
git add src/agent/mod.rs src/agent/cursor.rs src/agent/opencode.rs src/turn.rs src/task_store.rs src/transfer.rs src/turn_runner.rs src/supervisor.rs src/task_client.rs src/cli.rs src/lib.rs src/output.rs tests/agent_cursor_opencode.rs tests/agent_adapters.rs tests/task_command.rs tests/task_conversation.rs tests/task_turn.rs tests/turn_runner.rs tests/agent_capabilities.rs
git commit -m "feat: run cursor and opencode task turns"
```

---

### Task 4: Task, Mirror, Transfer, and Agent-Native Retention

**Gate:** The core plan's Task 10 and Tasks 1–3 of this plan are on `main`; the local GC and session-discard suites are green. The live worker acceptance is not a substitute for the rooted local cleanup tests.

**Files:**
- Modify: `src/host_store.rs`, `src/task_store.rs`, `src/transfer_repo.rs`, `src/turn.rs`, `src/agent/mod.rs`, `src/agent/cursor.rs`, `src/agent/opencode.rs`, `src/task_client.rs`, `src/cli.rs`, `src/lib.rs`, `src/output.rs`
- Create: `tests/task_gc.rs`
- Modify: `tests/task_materialization.rs`, `tests/task_turn.rs`, `tests/task_command.rs`, `tests/agent_cursor_opencode.rs`, `tests/cli_help.rs`

**Interfaces:**
- Consumes: `TaskState`, `TaskOutcome`, task retention settings, `TaskStore::close`, mirror refs, transfer repository base refs, adapter session deletion hooks, and the existing rooted cleanup/GC candidate API.
- Produces: task-specific `worker gc` candidates, preview/apply reports, open-task closure, branch pruning, empty-mirror cleanup, transfer-repository cleanup, and discard-time agent-native deletion where available.

- [ ] **Step 1: Write the failing retention tests**

Add deterministic clock and fake-agent tests:

```rust
#[test]
fn gc_closes_an_idle_open_task_but_preserves_its_result_branch() {
    let harness = GcHarness::open_task_at(1).with_last_turn_at(1);
    let preview = harness.preview_at(1 + TASK_RETENTION_MILLIS).unwrap();
    assert!(preview.candidates().iter().any(|candidate| candidate.reason() == "open task retention"));
    harness.apply_at(1 + TASK_RETENTION_MILLIS).unwrap();
    assert_eq!(harness.task_state(), TaskState::Closed);
    assert!(harness.mirror_ref_exists("refs/heads/task/task0001"));
}

#[test]
fn gc_prunes_expired_branch_then_collects_only_an_empty_mirror_and_transfer_repo() {
    let harness = GcHarness::closed_task_at(1).with_branch_at(1).without_other_refs();
    harness.apply_at(1 + BRANCH_RETENTION_MILLIS).unwrap();
    assert!(!harness.mirror_ref_exists("refs/heads/task/task0001"));
    assert!(harness.mirror_candidate_was_previewed_before_removal());
    assert!(harness.transfer_repo_candidate_was_previewed_before_removal());
}

#[test]
fn discard_requests_native_session_deletion_without_crossing_the_task_root() {
    let harness = GcHarness::cursor_task().with_native_delete_result(true);
    harness.close_discard().unwrap();
    assert_eq!(harness.native_delete_calls(), vec!["chat0001"]);
    assert!(harness.unrelated_mirror_refs_intact());
    assert!(harness.no_path_escape_was_attempted());
}
```

Also cover active-task protection, mirrors referenced by another task, base refs retained while a task is open, transfer repositories with a live base ref, malformed candidate metadata, preview/apply idempotency, agent adapters without native deletion, and branch pruning without workspace deletion outside the target task.

- [ ] **Step 2: Run retention tests to verify RED**

Run: `cargo test --locked --test task_gc --test task_materialization --test task_turn --test task_command -- --nocapture`

Expected: FAIL because phase 5d task candidates and mirror/transfer cleanup are not yet connected to `worker gc`.

- [ ] **Step 3: Implement rooted task retention and discard cleanup**

Add task candidates to the existing GC preview with a reason, bounded size, and validated identifier. Close an open task with no turn for the configured retention while preserving metadata, turn job directories within v1 retention, the mirror branch, and the session binding. Prune a result branch only after branch retention or immediately for `close --discard`; delete only `refs/heads/task/<task_id>` and the matching `refs/mac-worker/bases/<task_id>` under the mirror lock. Never delete another task's branch or any non-mac-worker ref.

Mark an empty mirror as a candidate only when no task record references its project, no task/result/base refs remain, and the mirror age exceeds branch retention. Mark a transfer repository as a candidate only when it has no base refs and no in-flight import/publication operation. Apply deletion through the rooted filesystem and Git ref APIs, preview each candidate first, and run mirror `git gc` only under the explicit apply command with the existing bounded runtime.

Add an optional adapter-native session deletion method. A discard calls it after task metadata and branch protection have been resolved; a missing method is a successful no-op. Codex/Cursor/OpenCode deletion output is discarded and any error is recorded as a bounded warning without exposing a session path or credential. Open tasks are never deleted merely because a worker probe failed.

- [ ] **Step 4: Run retention regressions to verify GREEN**

Run: `cargo test --locked --test task_gc --test task_materialization --test task_turn --test task_command --test agent_cursor_opencode --test cli_help -- --nocapture`

Expected: PASS with candidate previews, rooted deletion, branch protection, and stable discard behavior.

- [ ] **Step 5: Commit retention**

```bash
git add src/host_store.rs src/task_store.rs src/transfer_repo.rs src/turn.rs src/agent/mod.rs src/agent/cursor.rs src/agent/opencode.rs src/task_client.rs src/cli.rs src/lib.rs src/output.rs tests/task_gc.rs tests/task_materialization.rs tests/task_turn.rs tests/task_command.rs tests/agent_cursor_opencode.rs tests/cli_help.rs
git commit -m "feat: retain and discard agent task state safely"
```

---

### Task 5: Phase 5d Acceptance and Documentation

**Gate:** The core plan's Task 10 and Tasks 1–4 of this plan are on `main`, all listed local suites pass, and the operator has authorized a separate sanitized live run. This task is the only task that may use the configured workers; it is not run in this worktree under the brief's no-worker boundary.

**Files:**
- Modify: `README.md`, `docs/phase-five-validation.md`, `tests/cli_help.rs`, `tests/workers_command.rs`
- Create: `tests/phase-five-d-acceptance.rs`

**Interfaces:**
- Consumes: the complete section 7.1 task grammar, `worker workers --refresh`, `worker task reconcile`, origin source/publish, Cursor/OpenCode adapters, and `worker gc` reports.
- Produces: sanitized phase 5d live evidence, operator documentation, and help/output assertions for the new boundary.

- [ ] **Step 1: Write the failing documentation and evidence tests**

Assert the README and help text contain the exact supported forms and boundaries:

```rust
#[test]
fn phase_five_d_help_and_readme_document_origin_cursor_opencode_and_gc() {
    let help = worker_help();
    assert!(help.contains("--source local|origin"));
    assert!(help.contains("--publish fetch|push"));
    assert!(help.contains("--publish-branch NAME"));
    assert!(help.contains("--agent codex|claude|cursor|opencode"));
    assert!(help.contains("worker workers --refresh"));
    assert!(help.contains("worker gc --apply"));

    let readme = read_repository("README.md");
    for phrase in [
        "source = \"origin\"",
        "publish = [\"fetch\", \"push\"]",
        "CURSOR_API_KEY",
        "worker task reconcile",
        "worker gc --apply",
        "agent turns run with the worker account's full access",
    ] {
        assert!(readme.contains(phrase), "missing documentation phrase: {phrase}");
    }
}

#[test]
fn sanitized_phase_five_d_evidence_has_origin_and_adapter_rows_without_secrets() {
    let evidence = read_repository("docs/phase-five-validation.md");
    assert!(evidence.contains("Source origin and push publication"));
    assert!(evidence.contains("Cursor"));
    assert!(evidence.contains("OpenCode"));
    assert!(!evidence.contains("PLANTED_SECRET"));
    assert!(!evidence.contains("/Users/"));
}
```

- [ ] **Step 2: Run documentation tests to verify RED**

Run: `cargo test --locked --test phase-five-d-acceptance --test cli_help --test workers_command -- --nocapture`

Expected: FAIL until the README, validation record, and help snapshots describe phase 5d.

- [ ] **Step 3: Document and execute the sanitized live acceptance**

Extend `docs/phase-five-validation.md` with a `Phase 5d` section containing these rows and only sanitized evidence:

```text
Source origin and push publication | exact base preflight | origin:<host> routing | branch appears as --publish-branch | exit/state
Cursor env-profile turn            | prebind_session before first turn | profile-keyed capability | same chat on say | exit/state
OpenCode env-profile turn          | JSON session binding | --auto and pointer prompt | same session on say | exit/state
Retention and discard              | worker gc preview/apply | branch/mirror/transfer candidates | native delete if available | exit/state
```

The live procedure must use an isolated clone and isolated XDG roots, install the release helper only after core Task 10, and run spec section 20.2 item 6 plus the Cursor/OpenCode rows above. It must prove: origin-only workers are selected for `source = origin` and `publish = push`; a pushed branch appears under `--publish-branch`; a task rejected for a missing origin capability is not rerouted when pinned; Cursor succeeds from a locked-keychain SSH session through `CURSOR_API_KEY`; OpenCode succeeds through its secure profile; `say` preserves each session; a push failure leaves the task open; `gc` preserves protected branches and removes only expired candidates; and no retained record contains planted profile values or complete local paths. Record shortened IDs, aliases, states, exit codes, durations, helper/client revision match, and before/after fingerprints only.

Update README with the section 7.1 examples for `source = "origin"`, `publish = ["fetch", "push"]`, `--publish-branch`, `--agent cursor`, `--agent opencode`, secure profile setup, `worker workers --refresh`, `worker task reconcile`, and `worker gc --apply`. State that profile files are operator-provisioned owner-only files, that agent turns have the worker account's full access, that OpenCode's worker-local server is loopback-only, and that dashboard task views remain phase 5e. Do not document secrets or raw worker paths.

- [ ] **Step 4: Run the final phase 5d local gate**

Run:

```bash
cargo fmt --check
cargo test --locked --test agent_facts --test agent_publication --test agent_capabilities --test agent_cursor_opencode --test task_gc --test phase-five-d-acceptance
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
git diff --check
```

Expected: every command exits `0`; the known fixed-port dashboard command test is rerun alone if it fails in the full suite, and any remaining failure is investigated before the plan is considered complete.

- [ ] **Step 5: Commit the acceptance/documentation update**

```bash
git add README.md docs/phase-five-validation.md tests/cli_help.rs tests/workers_command.rs tests/phase-five-d-acceptance.rs
git commit -m "docs: record phase 5d publication and adapter acceptance"
```

## Delivery Boundary

After Task 5, the following section 7.1 flows are supported by the phase 5d extension, with every turn still carried by the bounded local runner:

```text
worker task submit --agent cursor --env-profile agents --prompt-file tasks/fix-login.md
worker task submit --agent opencode --prompt-file tasks/update-api.md
worker task submit --source origin --base main --agent codex --prompt "…"
worker task submit --publish fetch --publish push --publish-branch feature/api --agent claude --prompt "…"
worker workers --refresh
worker task reconcile
worker gc --apply
```

`source = origin` fails before task creation with `BASE_NOT_ON_ORIGIN` when the exact base is absent from the normalized origin; preparation fails with `BASE_UNAVAILABLE` when the worker cannot fetch or verify it. `publish = push` requires a committed base and `origin:<host>`, reserves a unique run branch, always performs fetch publication, and leaves the task open with `PUBLISH_FAILED` when origin rejects the push. Cursor and OpenCode use their bound sessions and pointer prompts; profile values remain worker-local. GC retains protected metadata/branches and deletes only previewed, rooted candidates.

The dashboard tasks view remains phase 5e. Interactive sessions, live input into a running agent, long-lived Claude processes, merge-request creation, non-loopback access, multiple worker slots, and any other item in spec section 22 remain deferred designs and are not part of this plan.

## Plan Self-Review Results

- **Spec coverage:** Task 1 covers profile-keyed facts, stale capability behavior, origin inventory capabilities, and the no-hot-path rule; Task 2 covers source `origin`, `BASE_NOT_ON_ORIGIN`, `BASE_UNAVAILABLE`, committed-base enforcement, unique publish branches, and `PUBLISH_FAILED`; Task 3 covers Cursor prebinding, OpenCode event binding, argv-pointer prompts, policies, env names, and resume continuity; Task 4 covers task/mirror/transfer/session retention and discard; Task 5 covers section 20.2 item 6, Cursor/OpenCode live rows, and documentation. The dashboard and all section 22 deferred designs remain outside the boundary.
- **Placeholder scan:** No `TBD`, `TODO`, or unspecified test step remains. Each task names files, interfaces, a failing test command, concrete assertions, implementation behavior, a passing regression command, and a commit.
- **Type consistency:** `AgentFacts`/`AgentProbe`/`AgentAuth`/`ProfileProbe` are consumed by Task 1; Task 2 uses the same `BaseOid`, `TaskSource`, `PublishMode`, and `RunRecord`; Task 3 uses the existing `AgentAdapter`, `SessionBinding`, and `TurnPublisher`; Task 4 consumes task/mirror refs from Task 2; Task 5 consumes all public commands and reports without introducing new CLI grammar.
