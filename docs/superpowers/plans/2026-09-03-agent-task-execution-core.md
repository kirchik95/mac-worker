# Agent Task Execution Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the agent task job kind so that `worker task submit` sends a prompt to the pool, a Mac mini materializes a Git worktree for it, runs Codex or Claude Code headless inside that worktree under the existing durable supervisor and one-slot lease, and the MacBook can watch, converse between turns, fetch the result branch, and close the task, with a bounded local runner carrying every turn after the CLI has returned.

**Architecture:** A turn is a v1 job stored in the v1 `jobs/` tree. The v1 lease, durable acceptance, log chunks, status, resolve-or-abandon, and rooted cleanup are reused unchanged for every turn; the detached supervisor gains a versioned turn section in its execution payload, and the phase 4 queue gains entry kinds, detached dispatch owners, and per-worker FIFO. New code adds a mac-worker-owned transfer repository per project on the MacBook, a per-project bare mirror on each worker, Git transport routed through hidden host-helper commands with the same identity discipline as `rsync-receive`, pure agent adapters for Codex and Claude Code, task and run records on both sides, a detached local turn runner, and the client commands.

**Tech Stack:** Rust 2024; system Git and OpenSSH through the existing `ProcessRunner` boundary; existing `serde`/`serde_json` canonical records, `sha2`, `uuid`, `base64`, `humantime`, `libc`, descriptor-relative `rooted_fs`; `proptest`, `tempfile`, `assert_cmd`, `predicates`; recorded agent event fixtures. No new crates.

**Spec:** `docs/superpowers/specs/2026-09-03-agent-task-pool-design.md` revision 2 (all sections; this plan delivers phases 5a, 5b, and 5c of section 21 for Codex and Claude Code, plus the phase 3 and phase 4 amendments of section 5.1). Prerequisite contracts: `docs/superpowers/plans/2026-08-27-single-worker-execution.md` and `docs/superpowers/plans/2026-08-28-three-worker-scheduler.md`.

## Global Constraints

- Only Tasks 0 and 1 are startable on `main` today. Every other task starts after phase 3 and phase 4 have both landed on `main` with their live gates recorded, because they modify files those phases own: `src/job.rs`, `src/host_store.rs`, `src/supervisor.rs`, `src/job_service.rs`, `src/transfer.rs`, `src/transport.rs`, `src/run.rs`, `src/client_state.rs`, `src/protocol.rs`, `src/probe.rs`, `src/scheduler_adapter.rs`, `src/error.rs`, `src/cli.rs`, and `src/lib.rs`. Task order within this plan is 1, 2, 3, 4, 5, 6, 7, 8, 9, 10; Task 4 may run in parallel with Task 3 and Task 5.
- Scope is spec section 21 phases 5a to 5c: `source = local`, `publish = fetch`, adapters for Codex and Claude Code, runners, conversation between turns, limits, runs, and the orchestrator skill. `source = origin`, `publish = push`, `--publish-branch` behaviour, origin capabilities, Cursor and OpenCode adapters, retention through `gc`, and the dashboard extension are later plans and must not be started here. Their options are parsed and rejected with `TASK_CONFIG_INVALID` naming the later plan.
- A turn is a v1 job in `jobs/<project_id>/<worktree_id>/<turn_id>/` and the job index. It holds the one-slot lease from acquisition through publisher completion and exact release; no second admission authority is introduced.
- The user's repository is never written during submission. Every Git write for submission happens in the transfer repository under the cache root. The only writes into the user's repository are `refs/remotes/mac-worker/<worker>/task/<task_id>` and its objects during result import. Tests prove byte identity of `HEAD`, index, working tree, refs, reflogs, configuration, and hooks around `submit`, `say`, and `--no-wait`.
- Every remote path is derived from validated `project_id`, `task_id`, and `turn_id` components below the resolved data root. Git transport passes only the `project_id` as the remote path; hidden components travel in the `--receive-pack` and `--upload-pack` program strings, with the turn's job ID first, exactly as `rsync-receive` carries its identities.
- The mirror's pre-receive hook permits only `refs/mac-worker/bases/*` creation and update; `receive.denyDeletes` is set; `core.hooksPath` is pinned in the mirror's own configuration; the host verifies hook content and mode before every `receive-pack`; server programs run with `GIT_CONFIG_GLOBAL` and `GIT_CONFIG_NOSYSTEM` neutralizing the account's global configuration.
- The Git SSH command for transport is built from the same options as `SshTransport` without the trailing `--`, because Git appends `-o SendEnv=GIT_PROTOCOL` before the host. Client Git invocations run in the transfer repository with `--no-verify`, `-c gc.auto=0`, `GIT_TERMINAL_PROMPT=0`, and neutralized global configuration.
- A turn's `CommandSpec` is `Shell` with the adapter's launch rendered by Task 1's `render_shell`, so `CommandSummary`, `JobMeta`, `LeaseRecord`, `validate_for_durable_job`, and the phase 4 `QueueEntry` are unchanged. The turn-specific launch behaviour comes only from the execution payload's version 2 `turn` section.
- `RequestFingerprintMaterial` is not extended. Wherever a v1 record requires a 64-hex digest for a job, a turn stores the SHA-256 of its canonical `TurnMaterial` in `manifest_digest`; `base_oid` (40 hex) lives inside `TurnMaterial`, which travels beside the material in `TaskTurnRequest` and in the payload's turn section, and the host verifies `sha256(turn) == manifest_digest` before acceptance and before launch. Every v1 path that re-derives the fingerprint from persisted fields therefore keeps working. For turns, `relative_working_dir` is empty and `resource_class` is `heavy`.
- The rendered shell string contains no worker paths: file arguments are the double-quoted environment references `"$MAC_WORKER_TURN_DIR/result.schema.json"` and `"$MAC_WORKER_TURN_DIR/last.md"`, expanded by the login shell; `LaunchPlan::turn` exports `MAC_WORKER_TURN_DIR`, and `submit_turn` writes `result.schema.json` beside `prompt.md`.
- Every terminal path of a turn runs the turn publisher before lease release: child exit, timeout, prelaunch failure, host cancellation, and lost-turn reconciliation. A turn can never leave its task `active`.
- Turn stdout goes through a supervisor-owned pipe and pump thread that caps the recorded stream, retains the tail, scans for the session-started event, and records the capped length as the terminal stdout length; the pump never waits for pipe EOF and stops at the process-group-absence proof, so an agent-detached process cannot hang the terminal transition. Batch jobs keep the direct descriptor under the golden test.
- `TurnTerminalHook` is invoked from the supervisor's common status writer whenever the new state is terminal, and from host cancellation and lost-turn reconciliation, so every writer including the ambiguous-child path is covered.
- The host layout migration runs only through the hidden `host migrate-layout` command, which `worker setup` executes before its final probe; it takes the installation lock itself, is a no-op on an uninitialized root or a current layout, and rewrites the layout record atomically. `HostStore::open` and the probe's `open_if_present` fail closed on an outdated layout with `HOST_LAYOUT_OUTDATED`, and the probe reports the worker unavailable with that code.
- `wait_for_capacity` is persisted in the local task record; a runner whose first claim finds no eligible worker for a task submitted with `--no-wait` abandons the row and marks the task `abandoned` with `CAPACITY_BUSY`.
- A claim is owner-scoped: a runner claims only the row it owns, and only when no older waiting row with a live owner is eligible for the same idle worker. Parked rows have no owner, are never claimed, and are unparked oldest-first, so they are always younger than every owned waiting row.
- The composed prompt of every turn awaiting acceptance is persisted locally as owner-only `turns/<task_id>/<turn_id>/prompt.md` under the state root before the runner handoff, so a runner or its replacement can build `TaskTurnRequest`; it is removed once acceptance is flushed, because the worker then holds it.
- Wire changes happen under one protocol version bump to `4` in Task 3, one host layout version bump with migration in Task 3, and one supervision version bump to `3` with execution payload version `2` in Task 6. Task 9 adds probe fields under version 4 without a further bump. Development against live workers requires `worker setup` after Tasks 3, 6, and 9.
- Agent turns run through `/bin/zsh -lc` with the worker account's real `HOME`, `USER`, `LOGNAME`, `SHELL`, a per-turn `TMPDIR`, the recorded Git identity, and one env profile; batch jobs keep v1's controlled environment. A golden test pins the batch launch plan to today's values.
- No message is ever written into a running agent process. `say` creates a new turn and is rejected with `TASK_BUSY` while a turn is active.
- Adapters are pure: no paths, credentials, SSH, or process execution. Every adapter behaviour is verified against recorded fixtures. Session binding has no fallback: Claude sessions are generated before the first turn, Codex sessions are captured from the thread-started event, and a resume without a bound session fails with `SESSION_UNBOUND`.
- Env profiles are read on the worker only, must be owner-only regular files, and are never uploaded, printed, or logged. Records store variable names only.
- The agent's stdout is its event stream. The turn's `stdout` log is that stream, `LogChunk` still serializes `stream: stdout`, and the client labels it `events` when rendering. `stderr` remains diagnostic output. The recorded stream is capped at 256 MiB; the supervisor keeps draining past the cap and retains a 64 KiB tail so the structured result survives.
- Prompts are user content. They travel in `TaskTurnRequest`, are verified against `prompt_sha256` in the fingerprinted material, are stored once per turn as `prompt.md` in the turn's job directory, bounded to 256 KiB, and are rendered as text.
- Task state authority is spec section 8.3: worker status for `active`, `open`, `closed`, `lost`, and `last_outcome`; the local record for `queued`, pre-acceptance `abandoned`, runner identity, run membership, and fetched heads. `say`, `close`, `list`, `wait`, and run cap counting refresh from the recorded worker first.
- Exit codes keep v1 semantics: `64` usage/configuration, `69` pre-acceptance transport, `70` protocol/infrastructure, `74` local I/O, `75` capacity. Turn outcomes map to CLI status by the table in spec section 18.
- Queue records for turns carry only IDs, agent name, a bounded title, requirements, preference, run reference, and timestamps; never prompt text, session identifiers, or branch names beyond the task ID.
- All new JSON records use the v1 pattern: strict manual serialization, unknown-field rejection, canonical bytes, owner-only staging, fsync, atomic rename, and no-follow validation.
- Test support is a deliverable: a shared `RecordingRunner` (Task 3) and a `TaskHarness` with an inline runner executor and a fake three-host transport (Task 7). Tests never assume helpers this plan does not create.
- A `task_turn` queue row is owned by its runner in the waiting state as well as the dispatching state. Dead-owner `task_turn` rows are never reaped: `worker run` skips them and only mutating task commands re-own them. A `queued` task whose row is missing is re-enqueued at the tail. A failed runner handoff abandons the task and releases its base ref rather than leaving an unowned record.
- Runner recovery runs only in `submit`, `batch`, `say`, `cancel`, `close`, `wait`, and `worker task reconcile`. `list`, `status`, `result`, `diff`, `logs`, and the dashboard never mutate state or start a process.
- At most one runner per configured worker waits for capacity; younger rows are parked without a process and are started by a finishing runner or the next mutating command. Waiting runners back off from one to thirty seconds and read fleet health from a shared per-worker observation cache under the local state root with single-flight refresh.
- The run cap is enforced under the local queue lock, counting refreshed active siblings, dispatching sibling rows, and locally accepted but not yet refreshed sibling turns; a claim consumes a slot until reverted.
- Agent, profile, and Git identity facts are collected by a separate `host refresh-facts` operation with a TTL cache; the read-only probe reports cached facts with their age, and stale facts count as `unknown`.
- Session bindings are persisted at turn acceptance for mac-worker-generated identifiers and at first observation of the session-started event for agent-generated identifiers, never only at publication.
- `--wip` selection runs read-only inside the user's repository with the user's configuration; only object creation happens in the transfer repository, which is keyed by `repo_id` (the hash of the repository's common directory) and whose alternates target is verified before every transfer operation.

## File Map

```text
docs/agent-task-spike.md                sanitized findings of the disposable shell spike
src/agent/mod.rs                        AgentKind, PermissionPolicy, TurnLimits, PromptDelivery, TurnLaunch, AgentEvent, StructuredResult, AgentAdapter, render_shell
src/agent/codex.rs                      Codex adapter
src/agent/claude.rs                     Claude Code adapter
src/task.rs                             TaskId, RunId, BaseOid, BranchName, TaskSource, TaskLimits, ClosePolicy, TaskState, TaskOutcome, GitIdentity, records and DTOs
src/transfer_repo.rs                    repo_id-keyed transfer repository with verified alternates, hash-object plus cacheinfo base commits, sensitive-tree check, base ref lifecycle, result import
src/git_transport.rs                    client push/fetch argv, hidden receive-pack/upload-pack execution, mirror creation and hook
src/task_store.rs                       worker-side task directories, prepare/status/diff/close, session binding
src/turn.rs                             execution payload turn section, env profile loading, turn launch plan inputs, publisher
src/turn_runner.rs                      detached local turn runner, handoff, waiting-state ownership, per-worker cap and parking, backoff, recovery, inline executor for tests
src/task_client.rs                      client task lifecycle: submit, say, status, list, logs, diff, result, fetch, close, wait, batch
src/job.rs                              TurnMaterial, QueueEntryKind and run reference on QueueEntry, JsonEvent task variants
src/host_store.rs                       layout version 2 migration, repos/ and tasks/ namespaces, task locks
src/supervisor.rs                       LaunchPlan seam, turn launch path, log cap with tail, workspace preservation
src/job_service.rs                      submit_turn, publisher at terminal transition, lost-turn task update
src/transfer.rs                         HostOperation additions, TaskRemoteClient over SshJsonTransport
src/transport.rs                        pinned ssh argv without trailing -- for Git
src/protocol.rs                         protocol version 4, agent/profile/identity probe DTOs
src/probe.rs                            cached agent, profile, and identity facts with age; refresh-facts collection off the probe hot path
src/scheduler_adapter.rs                agent:<name> and agent:<name>@<profile> capabilities
src/client_state.rs                     local task/run/runner records, row ownership handoff in both queue states, shared observation cache, per-worker FIFO claim with run caps
src/run.rs                              scheduler service reuse by task_client
src/project_config.rs                   [task] settings table in .worker.toml
src/cli.rs                              worker task ... grammar including reconcile, worker workers --refresh; hidden host and runner commands
src/lib.rs                              dispatch for task, runner, and hidden host commands
src/output.rs                           task/run/turn human and JSON reports
src/error.rs                            Git, Agent, and Task error classes and exit kinds
tests/support/recording_runner.rs       shared fake ProcessRunner with request recording and scripted results
tests/support/task_harness.rs           fake three-host transport, inline runner executor, repository fingerprints, stage recording
tests/fixtures/agents/                  recorded Codex and Claude event streams and result files
tests/agent_adapters.rs                 adapter argv, parsing, result, exit classification, shell rendering
tests/task_model.rs                     identifiers, records, state machine, privacy
tests/git_transport.rs                  argv, hidden command validation, hook enforcement, layout migration, local end-to-end push/fetch
tests/transfer_repo.rs                  untouched-repository proofs, --wip selection, second-capture check, sensitive/untracked preflights, import
tests/task_materialization.rs           mirror and worktree creation, idempotency, isolation, status/diff/close
tests/task_turn.rs                      turn launch plan, stdin prompt, env profile, log cap, publisher, result capture, lost mapping
tests/turn_runner.rs                    detach, handoff, crash recovery, replacement, terminal exits
tests/task_command.rs                   end-to-end fake-transport client and executable CLI tests
tests/task_conversation.rs              say, pins, per-worker FIFO, busy rejection, follow-up limits, cancel then resume, runs and wait
tests/agent_probe.rs                    protocol-4 probe facts and capability projection
docs/phase-five-validation.md           sanitized three-Mac acceptance record
README.md                               Phase 5 usage and remaining boundary
.claude/skills/pool-dispatch/SKILL.md   orchestrator loop over the CLI
```

Existing test suites modified along the way: `tests/support/mod.rs`, `tests/cli_help.rs`, `tests/client_state.rs`, `tests/doctor_command.rs`, `tests/job_protocol.rs`, `tests/job_queries.rs`, `tests/project_config.rs`, `tests/run_command.rs`, `tests/scheduler_adapter.rs`, `tests/scheduler_queue.rs`, `tests/setup_command.rs`, `tests/snapshot_transfer.rs`, `tests/supervisor.rs`, `tests/workers_command.rs`.

---

### Task 0: Disposable Shell Spike

**Gate:** None. Startable now, on one worker, with one scratch clone. Nothing from this task is merged except `docs/agent-task-spike.md`.

**Files:**
- Create: `docs/agent-task-spike.md`

**Interfaces:**
- Consumes: one configured worker, Codex authenticated there, an env profile prepared by hand for Claude Code.
- Produces: go/no-go evidence for Tasks 3, 6, and 8, and the exact agent flag set the adapters will encode.

- [ ] **Step 1: Prepare the spike outside the repository**

Create a scratch directory under the session scratchpad or `~/.cache/mac-worker-spike/`, a throwaway clone of a small real project, and a hand-written `~/.config/mac-worker/env/agents.env` on the worker with mode `0600` holding a Claude Code token from `claude setup-token`. Do not commit the scripts.

- [ ] **Step 2: Prove headless agents from a locked-keychain SSH session**

Run over plain `ssh` with `BatchMode=yes`, through the login shell so binary resolution matches the probe:

```bash
ssh <worker> 'zsh -lc "cd <scratch-worktree> && codex exec --json -o last.md --output-schema result.schema.json -s workspace-write --approve-for-me -c sandbox_workspace_write.network_access=true - < prompt.md"'
ssh <worker> 'zsh -lc "set -a; . ~/.config/mac-worker/env/agents.env; set +a; cd <scratch-worktree> && claude -p --output-format stream-json --session-id <uuid> --max-turns 40 --permission-mode bypassPermissions --json-schema \"\$(cat result.schema.json)\" < prompt.md"'
```

Record: exit codes for success and for a prompt that instructs the agent to fail; whether the network override is needed for `npm test`; whether Claude needs `--dangerously-skip-permissions` in addition to the permission mode; that Claude accepts `--max-turns` although its help omits it; where the session identifier appears in each stream (Codex thread-started event); the shape of the structured final message for both agents; whether Codex commits succeed inside a linked worktree of a bare mirror under `workspace-write`.

- [ ] **Step 3: Prove resume and cancel-then-resume**

```bash
ssh <worker> 'zsh -lc "cd <scratch-worktree> && codex exec resume <session_id> --json -o last.md --output-schema result.schema.json -c sandbox_mode=\"workspace-write\" - < followup.md"'
ssh <worker> 'zsh -lc "set -a; . ~/.config/mac-worker/env/agents.env; set +a; cd <scratch-worktree> && claude -p --output-format stream-json --resume <uuid> --permission-mode bypassPermissions < followup.md"'
```

Kill a running turn with `TERM` to the process group after the agent has made an edit, then resume; record whether the session resumes and what was lost. Record where each agent stores its session on disk and whether `codex delete <session>` removes it.

- [ ] **Step 4: Prove Git transport with program overrides**

On the worker create `<data>/repos/<id>.git` with `git init --bare`, set `core.hooksPath` to its hooks directory and `receive.denyDeletes=true` in its local config, install a `pre-receive` hook that rejects any ref outside `refs/mac-worker/bases/`, and temporarily set a global `core.hooksPath` in the account's `~/.gitconfig` to confirm the mirror-local setting still wins. From the MacBook, using a scratch bare repository with `objects/info/alternates` pointing at the clone's objects:

```bash
git -C <transfer.git> -c gc.auto=0 push --no-verify --receive-pack='/bin/sh -c "exec git-receive-pack <data>/repos/<id>.git"' <worker>:<id> <base_oid>:refs/mac-worker/bases/<task_id>
ssh <worker> 'git -C <data>/repos/<id>.git worktree add -b task/<task_id> <data>/tasks/<id>/<task_id>/workspace <base_oid>'
git -C <transfer.git> -c gc.auto=0 fetch --no-write-fetch-head --upload-pack='/bin/sh -c "exec git-upload-pack <data>/repos/<id>.git"' <worker>:<id> +refs/heads/task/<task_id>:refs/mac-worker/results/<task_id>
git -C <clone> -c gc.auto=0 fetch --no-write-fetch-head <transfer.git> +refs/mac-worker/results/<task_id>:refs/remotes/mac-worker/<worker>/task/<task_id>
```

Record that the hook rejects a push to `refs/heads/x` and a deletion, that a second push of the same commit transfers zero objects, that the fetch chain returns the agent's commits, that the clone's index, `HEAD`, and reflogs are unchanged, and that `GIT_TRACE=1` shows Git appending `-o SendEnv=GIT_PROTOCOL` before the host.

- [ ] **Step 5: Measure**

While the Codex turn runs a real test suite, sample `vm_stat` and `memory_pressure` every five seconds for the duration; record peak pressure class and whether swap grew. Record wall-clock from `ssh` start to the first agent event. Record whether the worker account has `user.name`/`user.email` and what a commit without them looks like.

- [ ] **Step 6: Write the record**

Create `docs/agent-task-spike.md` with sanitized results only: no hostnames, paths, tokens, session identifiers, or transcript text. Include a table of confirmed flags per agent and a go/no-go line for each of: headless auth via env profile, structured result, resume after cancel, transport with program overrides and pinned hooks, Codex commits inside a mirror worktree, `--max-turns` on Claude, memory profile.

- [ ] **Step 7: Commit the record**

```bash
git add docs/agent-task-spike.md
git commit -m "docs: record agent task spike findings"
```

---

### Task 1: Pure Agent Adapters for Codex and Claude Code

**Gate:** None. Startable now on `main`; depends only on `serde_json` and `uuid`. It defines its own bounded error type and does not touch `src/error.rs`; Task 2 maps that error into `WorkerError` after phase 3 lands. The one-line `pub mod agent;` in `src/lib.rs` is an isolated rebase conflict, as in phase 4 Task 1.

**Files:**
- Create: `src/agent/mod.rs`
- Create: `src/agent/codex.rs`
- Create: `src/agent/claude.rs`
- Create: `tests/fixtures/agents/codex-success.jsonl`, `tests/fixtures/agents/codex-needs-input.jsonl`, `tests/fixtures/agents/codex-truncated.jsonl`, `tests/fixtures/agents/claude-success.jsonl`, `tests/fixtures/agents/claude-blocked.jsonl`, `tests/fixtures/agents/claude-malformed.jsonl`
- Create: `tests/agent_adapters.rs`
- Modify: `src/lib.rs` (one line: `pub mod agent;`)

**Interfaces:**
- Consumes: adapter-owned strings and the recorded fixtures.
- Produces: `AgentKind`, `PermissionPolicy`, `TurnLimits`, `PromptDelivery`, `TurnParams`, `TurnLaunch`, `AgentEvent`, `StructuredResult`, `ResultStatus`, `AgentOutcome`, `AdapterError`, `AgentAdapter`, `adapter_for`, `render_shell`, `RESULT_SCHEMA_JSON`.

- [ ] **Step 1: Write failing adapter tests**

Create `tests/agent_adapters.rs`:

```rust
#[test]
fn codex_first_turn_reads_prompt_from_stdin_and_requests_schema() {
    let launch = adapter_for(AgentKind::Codex).first_turn(&params(PermissionPolicy::Workspace)).unwrap();
    assert_eq!(launch.program(), "codex");
    assert!(launch.args().starts_with(&["exec".into(), "--json".into()]));
    assert!(launch.args().windows(2).any(|w| w[0] == "-s" && w[1] == "workspace-write"));
    assert!(launch.args().windows(2).any(|w| w[0] == "-c" && w[1] == "approval_policy=\"never\""));
    assert!(!launch.args().contains(&"--approve-for-me".into()));            // incompatible with --sandbox in Codex 0.153
    assert!(launch.args().windows(2).any(|w| w[0] == "--output-schema" && w[1] == "{schema}"));
    assert!(launch.args().windows(2).any(|w| w[0] == "-o" && w[1] == "{last_message}"));
    assert_eq!(launch.args().last().map(String::as_str), Some("-"));
    assert_eq!(launch.prompt_delivery(), PromptDelivery::Stdin);
    assert!(launch.env_names().is_empty());
}

#[test]
fn codex_resume_uses_config_sandbox_and_keeps_schema() {
    let launch = adapter_for(AgentKind::Codex).resume_turn(&params(PermissionPolicy::Workspace), "0d3c…").unwrap();
    assert!(launch.args().starts_with(&["exec".into(), "resume".into(), "0d3c…".into()]));
    for key in ["sandbox_mode=\"workspace-write\"", "sandbox_workspace_write.network_access=true", "approval_policy=\"never\""] {
        assert!(launch.args().windows(2).any(|w| w[0] == "-c" && w[1] == key), "{key}");
    }
    assert!(launch.args().iter().all(|a| a != "-C" && a != "-s" && a != "--approve-for-me"));
    assert!(launch.args().windows(2).any(|w| w[0] == "--output-schema"));
}

#[test]
fn claude_first_turn_binds_generated_session_budget_and_turns() {
    let mut params = params(PermissionPolicy::Unattended);
    params.limits.max_turns = Some(40);
    params.limits.max_budget_usd_cents = Some(1_250);
    let launch = adapter_for(AgentKind::Claude).first_turn(&params).unwrap();
    assert!(launch.args().windows(2).any(|w| w[0] == "--session-id" && w[1] == params.session_seed.to_string()));
    assert!(launch.args().windows(2).any(|w| w[0] == "--max-turns" && w[1] == "40"));
    assert!(launch.args().windows(2).any(|w| w[0] == "--max-budget-usd" && w[1] == "12.50"));
    assert!(launch.args().windows(2).any(|w| w[0] == "--permission-mode" && w[1] == "bypassPermissions"));
    assert_eq!(launch.env_names(), &["CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY"]);
    assert_eq!(launch.prompt_delivery(), PromptDelivery::Stdin);
}

#[test]
fn codex_stream_yields_session_ref_and_normalized_events() {
    let adapter = adapter_for(AgentKind::Codex);
    let events: Vec<AgentEvent> = fixture_lines("codex-success.jsonl").filter_map(|l| adapter.parse_event(&l)).collect();
    assert_eq!(adapter.session_ref(&events).as_deref(), Some("0d3c…"));
    assert!(events.iter().any(|e| matches!(e, AgentEvent::Command { exit_code: Some(0), .. })));
    assert!(matches!(events.last(), Some(AgentEvent::TurnEnd { .. })));
}

#[test]
fn structured_result_is_extracted_or_unknown_never_an_error() {
    let adapter = adapter_for(AgentKind::Claude);
    assert_eq!(adapter.extract_result(&fixture("claude-success.jsonl"), None).unwrap().status(), ResultStatus::Done);
    assert_eq!(adapter.extract_result(&fixture("claude-malformed.jsonl"), None).unwrap().status(), ResultStatus::Unknown);
}

#[test]
fn render_shell_quotes_arguments_and_renders_file_placeholders_as_env_references() {
    let launch = adapter_for(AgentKind::Codex).first_turn(&params(PermissionPolicy::Workspace)).unwrap();
    let shell = render_shell(&launch).unwrap();
    assert!(shell.starts_with("exec 'codex' 'exec' '--json'"));
    assert!(shell.contains("--output-schema' \"$MAC_WORKER_TURN_DIR/result.schema.json\""));
    assert!(shell.contains("'-o' \"$MAC_WORKER_TURN_DIR/last.md\""));
    assert!(!shell.contains("{schema}") && !shell.contains("'/"));          // no quoted absolute paths, deterministic across workers
    assert_eq!(render_shell(&launch).unwrap(), shell);
}
```

Also test: `workspace` policy on Claude falls back to `unattended` and reports `permission_fallback() == true`; `--model` pass-through; summaries of tool calls and commands are bounded to 512 bytes with control characters escaped; a truncated final line is ignored; `TurnLimits` rejects zero timeout, timeouts above 24h, and a budget above 100000 cents; the schema constant parses as JSON with exactly the keys `status`, `summary`, `questions`, `files_changed`; `render_shell` rejects an argument containing NUL; `classify` maps `(Some(0), Done)` to `Done`, `(Some(0), NeedsInput)` to `NeedsInput`, `(Some(1), Done)` to `Failed { exit_code: 1 }`, `(None, _)` to `Signalled`; `resume_turn` for Claude passes `--resume <ref>` and never `--session-id`.

- [ ] **Step 2: Run adapter tests to verify RED**

Run: `cargo test --locked --test agent_adapters -- --nocapture`

Expected: FAIL because `crate::agent` does not exist.

- [ ] **Step 3: Implement the adapter contract**

Create `src/agent/mod.rs`:

```rust
pub const MAX_SUMMARY_BYTES: usize = 512;
pub const RESULT_SCHEMA_JSON: &str = r#"{"type":"object","properties":{"status":{"enum":["done","needs_input","blocked"]},"summary":{"type":"string"},"questions":{"type":"array","items":{"type":"string"}},"files_changed":{"type":"array","items":{"type":"string"}}},"required":["status","summary"],"additionalProperties":false}"#;

pub enum AgentKind { Codex, Claude }
pub enum PermissionPolicy { Workspace, Unattended }
pub enum PromptDelivery { Stdin }
pub struct TurnLimits { pub timeout_millis: u64, pub max_turns: Option<u32>, pub max_budget_usd_cents: Option<u64> }
pub struct TurnParams { pub kind: AgentKind, pub model: Option<String>, pub policy: PermissionPolicy, pub limits: TurnLimits, pub session_seed: uuid::Uuid }
pub struct TurnLaunch { program: String, args: Vec<String>, prompt_delivery: PromptDelivery, env_names: Vec<&'static str>, permission_fallback: bool }
pub const TURN_DIR_ENV: &str = "MAC_WORKER_TURN_DIR";
pub const SCHEMA_FILE_NAME: &str = "result.schema.json";
pub const LAST_MESSAGE_FILE_NAME: &str = "last.md";
pub enum AgentEvent {
    AssistantMessage { text: String },
    ToolCall { name: String, summary: String },
    FileChange { paths: Vec<String> },
    Command { summary: String, exit_code: Option<i32> },
    Usage { input_tokens: Option<u64>, output_tokens: Option<u64>, cost_usd_cents: Option<u64> },
    SessionStarted { session_ref: String },
    TurnEnd { reason: String },
}
pub enum ResultStatus { Done, NeedsInput, Blocked, Unknown }
pub struct StructuredResult { status: ResultStatus, summary: String, questions: Vec<String>, files_changed: Vec<String> }
pub enum AgentOutcome { Done, NeedsInput, Blocked, Unknown, Failed { exit_code: u8 }, Signalled }
pub struct AdapterError(String);
pub trait AgentAdapter: Send + Sync {
    fn kind(&self) -> AgentKind;
    fn binary(&self) -> &'static str;
    fn first_turn(&self, params: &TurnParams) -> Result<TurnLaunch, AdapterError>;
    fn resume_turn(&self, params: &TurnParams, session_ref: &str) -> Result<TurnLaunch, AdapterError>;
    fn parse_event(&self, line: &str) -> Option<AgentEvent>;
    fn session_ref(&self, events: &[AgentEvent]) -> Option<String>;
    fn extract_result(&self, stream: &str, last_message_file: Option<&str>) -> Result<StructuredResult, AdapterError>;
    fn classify(&self, exit_code: Option<i32>, status: ResultStatus) -> AgentOutcome;
}
pub fn adapter_for(kind: AgentKind) -> &'static dyn AgentAdapter;
pub fn render_shell(launch: &TurnLaunch) -> Result<String, AdapterError>;
```

Arguments use the placeholders `{schema}` and `{last_message}`; `render_shell` prefixes `exec`, single-quotes every ordinary argument, and renders the two placeholders as the double-quoted environment references `"$MAC_WORKER_TURN_DIR/result.schema.json"` and `"$MAC_WORKER_TURN_DIR/last.md"`, so the string is identical for every worker and contains no paths. Codex first turn: `exec --json -o {last_message} --output-schema {schema} [-m MODEL] <policy> -` where `Workspace` is `-s workspace-write -c approval_policy="never" -c sandbox_workspace_write.network_access=true` (the spike showed `--approve-for-me` is incompatible with `--sandbox`) and `Unattended` is `--dangerously-bypass-approvals-and-sandbox`; Codex resume: `exec resume <ref> --json -o {last_message} --output-schema {schema} -c sandbox_mode="workspace-write" -c sandbox_workspace_write.network_access=true -c approval_policy="never" -` for `Workspace`, and the `-c` equivalents of the bypass flag for `Unattended`; never `-C`, `-s`, or `--approve-for-me`. Exit classification consults the structured status before the exit code, because a failing Codex turn can exit `0` with status `blocked`. Claude first turn: `-p --output-format stream-json --session-id <seed> --json-schema <RESULT_SCHEMA_JSON> [--model] [--max-turns] [--max-budget-usd] --permission-mode bypassPermissions`; resume replaces `--session-id` with `--resume <ref>`; env names `CLAUDE_CODE_OAUTH_TOKEN`, `ANTHROPIC_API_KEY`. Adjust details to the spike record, never from memory. Result extraction prefers the last-message file, then the final assistant text; malformed JSON is `Unknown`.

- [ ] **Step 4: Run adapter tests to verify GREEN**

Run: `cargo test --locked --test agent_adapters -- --nocapture`

Expected: PASS.

- [ ] **Step 5: Commit the adapters**

```bash
git add src/lib.rs src/agent tests/agent_adapters.rs tests/fixtures/agents
git commit -m "feat: describe codex and claude turns as pure adapters"
```

---

### Task 2: Task Model, Records, and Error Classes

**Gate:** Start after phase 3 and phase 4 are on `main`; it uses `JobId`, `WorkerError::public_code`, and the v1 identifier macro.

**Files:**
- Create: `src/task.rs`
- Create: `tests/task_model.rs`
- Modify: `src/lib.rs` (one line: `pub mod task;`)
- Modify: `src/error.rs`

**Interfaces:**
- Consumes: `job::JobId`, `error::WorkerError`, `uuid`, `serde_json`, Task 1 `AgentKind`, `PermissionPolicy`, `TurnLimits`, `AgentOutcome`, `AdapterError`.
- Produces: `TaskId`, `RunId`, `TurnId` (alias of `JobId`), `BaseOid`, `BranchName`, `TaskTitle`, `TaskSource`, `PublishMode`, `ClosePolicy`, `TaskState`, `TaskOutcome`, `GitIdentity`, `TaskLimits`, `TaskMeta`, `TaskSummary`, `TurnSummary`, `TaskStatus`, `LocalTaskRecord`, `RunRecord`, `RunProgress`, `RunnerIdentity`, new `WorkerError` variants, `From<AdapterError> for WorkerError`.

- [ ] **Step 1: Write failing model tests**

```rust
#[test]
fn base_oid_and_branch_names_are_validated() {
    assert!("0123456789abcdef0123456789abcdef01234567".parse::<BaseOid>().is_ok());
    assert!("0123456789ABCDEF0123456789abcdef01234567".parse::<BaseOid>().is_err());
    for invalid in ["", "-x", "a..b", "a/", "/a", "a.lock", "a//b", "a b", "a\u{7}b", "refs/heads/x"] {
        assert!(invalid.parse::<BranchName>().is_err(), "{invalid:?}");
    }
    assert_eq!(BranchName::for_task(task_id()).as_str(), "task/00000000000000000000000000000001");
}

#[test]
fn task_state_allows_only_documented_transitions() {
    use TaskState::*;
    assert!(Queued.can_transition_to(Active) && Queued.can_transition_to(Abandoned));
    assert!(Active.can_transition_to(Open) && Active.can_transition_to(Closed) && Active.can_transition_to(Lost));
    assert!(Open.can_transition_to(Active) && Open.can_transition_to(Closed) && Open.can_transition_to(Abandoned));
    assert!(!Closed.can_transition_to(Open) && !Queued.can_transition_to(Open) && !Lost.can_transition_to(Open));
}

#[test]
fn lost_turn_leaves_task_open_with_lost_outcome() {
    assert_eq!(TaskOutcome::from_turn(TurnTerminal::Lost, None), TaskOutcome::Lost);
    assert_eq!(TaskOutcome::from_turn(TurnTerminal::TimedOut, None), TaskOutcome::TimedOut);
    assert_eq!(TaskOutcome::from_turn(TurnTerminal::Succeeded, Some(AgentOutcome::NeedsInput)), TaskOutcome::NeedsInput);
    assert_eq!(TaskOutcome::from_turn(TurnTerminal::Failed, Some(AgentOutcome::Failed { exit_code: 3 })), TaskOutcome::Failed { reason: "agent exited 3".into() });
}

#[test]
fn task_meta_bounds_prompt_and_summary_hides_it() {
    assert_eq!(TaskMeta::new(fields_with_prompt("x".repeat(256 * 1024 + 1))).unwrap_err().public_code(), "TASK_CONFIG_INVALID");
    let meta = TaskMeta::new(fields_with_prompt("Fix the flaky login spec\n\nDetails…".into())).unwrap();
    let json = serde_json::to_value(meta.summary()).unwrap();
    assert_eq!(json["title"], "Fix the flaky login spec");
    assert!(json.get("prompt").is_none() && json.get("session_ref").is_none());
}

proptest! {
    #[test]
    fn records_round_trip_canonically(record in arbitrary_local_task_record()) {
        let bytes = record.canonical_bytes().unwrap();
        let parsed: LocalTaskRecord = serde_json::from_slice(&bytes).unwrap();
        prop_assert_eq!(parsed.canonical_bytes().unwrap(), bytes);
    }
}
```

Also test: unknown and duplicate JSON fields are rejected; `TaskLimits` bounds (`max_followups` 0..=100, default 10); `GitIdentity` bounds name and email to 256 bytes each and rejects control characters and `<`/`>`; `RunProgress` counts by state; `TaskSummary` contains no prompt, session, or path fields; titles are an explicit optional field, and when absent they are the first non-empty prompt line after the shared redaction boundary (home paths, `~`-prefixed paths, and token-like strings removed) then bounded to 120 bytes with control characters escaped; `RunnerIdentity` wraps `ProcessIdentity`; `TaskSource::Origin` and `PublishMode::Push` parse but `TaskMeta::validate_core_scope` rejects them with `TASK_CONFIG_INVALID` naming the later plan; every new `WorkerError` variant maps to the exit kind in Step 3.

- [ ] **Step 2: Run model tests to verify RED**

Run: `cargo test --locked --test task_model -- --nocapture`

Expected: FAIL because `crate::task` does not exist.

- [ ] **Step 3: Implement task records and error classes**

In `src/task.rs`:

```rust
pub const MAX_PROMPT_BYTES: usize = 256 * 1024;
pub const MAX_TITLE_BYTES: usize = 120;
pub const MAX_FOLLOWUPS: u32 = 100;

pub type TurnId = JobId;
pub struct TaskId(Uuid); pub struct RunId(Uuid);        // v1 identifier macro
pub struct BaseOid(String);                            // exactly 40 lowercase hex
pub struct BranchName(String);                         // conservative ref subset, see tests
pub struct TaskTitle(String);
pub enum TaskSource { Local { wip: bool }, Origin { url: String } }
pub enum PublishMode { Fetch, Push }
pub enum ClosePolicy { Done, Never }
pub enum TaskState { Queued, Active, Open, Closed, Abandoned, Lost }
pub enum TurnTerminal { Succeeded, Failed, Cancelled, TimedOut, Lost }
pub enum TaskOutcome { Done, NeedsInput, Blocked, Unknown, Failed { reason: String }, Cancelled, TimedOut, Lost }
pub struct GitIdentity { name: String, email: String }
pub struct TaskLimits { pub turn: TurnLimits, pub max_followups: u32 }
pub struct TaskMeta { task_id, run_id: Option<RunId>, project_id, worktree_id, agent: AgentKind, model: Option<String>, policy: PermissionPolicy, source: TaskSource, publish: Vec<PublishMode>, publish_branch: Option<BranchName>, base_oid: BaseOid, limits: TaskLimits, close_policy: ClosePolicy, env_profile: Option<String>, git_identity: GitIdentity, title: TaskTitle, prompt: String, created_at_millis: u64 }
pub struct TaskSummary { task_id, run_id, agent, title, state, last_outcome, worker: Option<String>, turns: u32, runner: Option<RunnerState>, updated_at_millis }
pub struct TurnSummary { turn_number: u32, turn_id: TurnId, terminal: Option<TurnTerminal>, outcome: Option<TaskOutcome>, agent_committed: Option<bool>, log_truncated: bool, started_at_millis: Option<u64>, ended_at_millis: Option<u64> }
pub struct TaskStatus { state: TaskState, last_outcome: Option<TaskOutcome>, worker: Option<String>, session_present: bool, head_oid: Option<BaseOid>, summary: Option<String>, questions: Vec<String>, files_changed: Vec<String>, diff_stat: Option<String>, turns: Vec<TurnSummary>, updated_at_millis: u64 }
pub struct RunnerIdentity(ProcessIdentity);
pub enum RunnerState { Live, Dead, Exited }
pub struct LocalTaskRecord { meta: TaskMeta, status: TaskStatus, status_observed_at_millis: Option<u64>, runner: Option<RunnerIdentity>, fetched_head: Option<BaseOid>, repo_id: String, alternates_target: PathBuf, preference: WorkerPreference, wait_for_capacity: bool, abandon_code: Option<String> }   // repo_id and alternates_target never appear in public JSON
pub struct RunRecord { run_id, name: Option<String>, task_ids: Vec<TaskId>, max_parallel: u32, created_at_millis: u64 }
pub struct RunProgress { total: usize, queued: usize, active: usize, open: usize, closed: usize, failed_like: usize }
```

Add to `src/error.rs`:

```rust
#[error("git error [{code}]: {message}")]     Git { code: &'static str, message: String },
#[error("agent error [{code}]: {message}")]   Agent { code: &'static str, message: String },
#[error("task error [{code}]: {message}")]    Task { code: &'static str, message: String },
```

Exit kinds: `Git` with `BASE_PUSH_FAILED`/`RESULT_FETCH_FAILED` is transport (`69`); `WORKTREE_CREATE_FAILED`/`WORKTREE_INCONSISTENT`/`BASE_UNAVAILABLE`/`PUBLISH_FAILED` is infrastructure (`70`); `Agent` with `AGENT_NOT_INSTALLED`/`AGENT_NOT_AUTHENTICATED` is capacity (`75`), `AGENT_UNSUPPORTED` is usage (`64`), `RESULT_UNPARSEABLE`/`SESSION_UNBOUND`/`ENV_PROFILE_PERMISSIONS` is infrastructure, `AGENT_EXITED` carries the agent's exit code through `WorkerError::CommandExit` unchanged, and `AGENT_LIMIT_REACHED` (an agent-reported turn or budget limit) maps to `1` like `blocked`; `Task` codes (`TASK_BUSY`, `FOLLOWUP_LIMIT`, `TASK_CLOSED`, `TASK_NOT_FOUND`, `TASK_CONFIG_INVALID`) are usage, and `RUNNER_HANDOFF_FAILED` is local I/O (`74`). `HOST_LAYOUT_OUTDATED` is an infrastructure code reported through the existing `Unavailable` path. Public messages are bounded and content-free. `From<AdapterError>` maps to `Agent { code: "AGENT_UNSUPPORTED", .. }`.

- [ ] **Step 4: Run model tests to verify GREEN**

Run: `cargo test --locked --test task_model --test job_protocol --test cli_help -- --nocapture`

Expected: PASS.

- [ ] **Step 5: Commit the model**

```bash
git add src/lib.rs src/task.rs src/error.rs tests/task_model.rs
git commit -m "feat: define agent task records and lifecycle"
```

---

### Task 3: Git Transport, Mirror, Host Layout Migration, and Protocol 4

**Gate:** Start after Task 2. Modifies phase 3 and phase 4 owned files (`host_store.rs`, `transfer.rs`, `transport.rs`, `protocol.rs`, `cli.rs`, `lib.rs`) and every protocol fixture.

**Files:**
- Create: `src/git_transport.rs`
- Create: `tests/support/recording_runner.rs`
- Create: `tests/git_transport.rs`
- Modify: `src/host_store.rs`
- Modify: `src/transport.rs`
- Modify: `src/transfer.rs`
- Modify: `src/protocol.rs`
- Modify: `src/install.rs`
- Modify: `src/cli.rs`
- Modify: `src/lib.rs`
- Modify: `tests/support/mod.rs`, `tests/workers_command.rs`, `tests/doctor_command.rs`, `tests/setup_command.rs`, `tests/job_protocol.rs`, `tests/scheduler_adapter.rs`, `tests/snapshot_transfer.rs`

**Interfaces:**
- Consumes: `HostStore`, `AdmissionGuard`, `TransferGuard`, `LeaseRecord`, `TransferIdentity`, `HiddenComponent`, `ProcessRunner`, `ProcessRequest`, `SshTransport` options, Task 2 `TaskId`/`BaseOid`/`BranchName`.
- Produces: `PROTOCOL_VERSION == 4`, `HOST_LAYOUT_VERSION` bump with `HostStore::migrate_layout` behind hidden `host migrate-layout` (`HostCommand::MigrateLayout`) run by the setup script, `HostStore::{mirror, mirror_if_present}`, `PRE_RECEIVE_HOOK`, `GitTransport::{push_base, fetch_result}`, `GitServerExecutor`, `SystemGitServerExecutor`, `HostGitService::{receive_pack, upload_pack}`, `HostCommand::{ReceivePack, UploadPack}`, `SshTransport::git_ssh_command`, shared `RecordingRunner`.

- [ ] **Step 1: Write failing transport tests**

```rust
#[test]
fn push_base_runs_in_transfer_repo_with_pinned_ssh_and_hidden_receive_pack() {
    let runner = RecordingRunner::default();
    GitTransport::new(&runner).push_base(&worker("mini-1", "mac1"), &identity(), &project_id(), task_id(), &base_oid(), &transfer_repo()).unwrap();
    let request = runner.single_request();
    assert_eq!(request.program, "git");
    assert!(request.args.windows(2).any(|w| w[0] == "-C" && w[1] == transfer_repo().as_os_str()));
    assert!(request.args.contains(&"--no-verify".into()) && request.args.windows(2).any(|w| w[0] == "-c" && w[1] == "gc.auto=0"));
    let receive = request.args.iter().find(|a| a.to_str().unwrap().starts_with("--receive-pack=")).unwrap().to_str().unwrap();
    assert!(receive.starts_with(&format!("--receive-pack=~/.local/bin/worker host receive-pack {} ", identity().job_id())));
    assert!(request.args.contains(&format!("mac1:{}", project_id()).into()));
    assert!(request.args.last().unwrap().to_str().unwrap().ends_with(&format!(":refs/mac-worker/bases/{}", task_id())));
    let ssh = env(&request, "GIT_SSH_COMMAND");
    assert!(!ssh.split(' ').any(|part| part == "--"));
    assert_eq!(env(&request, "GIT_CONFIG_GLOBAL"), "/dev/null");
    assert_eq!(env(&request, "GIT_CONFIG_NOSYSTEM"), "1");
    assert_eq!(env(&request, "GIT_TERMINAL_PROMPT"), "0");
}

#[test]
fn receive_pack_is_keyed_by_turn_job_id_and_validates_lease_like_rsync() {
    let store = temp_store_with_lease(job_id(), lease_token());
    let executor = RecordingExecutor::default();                                   // returns a sentinel error instead of exec
    let err = HostGitService::new(&store).receive_pack(&components(job_id(), client_id(), lease_token(), fingerprint()), &project_id(), &executor).unwrap_err();
    assert_eq!(err.public_code(), "TEST_EXECUTOR_INVOKED");
    assert_eq!(executor.invocations(), vec![("git-receive-pack", store.mirror(&project_id()).unwrap().path().to_path_buf())]);
    let err = HostGitService::new(&store).receive_pack(&components(other_job_id(), client_id(), lease_token(), fingerprint()), &project_id(), &executor).unwrap_err();
    assert_eq!(err.public_code(), rsync_identity_mismatch_code());
    for bad in ["../x", "ABC", "", &"a".repeat(65)] {
        let err = HostGitService::new(&store).receive_pack(&components(job_id(), client_id(), lease_token(), fingerprint()), bad, &executor).unwrap_err();
        assert_eq!(err.public_code(), rsync_invalid_component_code());
    }
    assert_eq!(executor.invocations().len(), 1);
}

#[test]
fn mirror_hook_wins_over_global_hooks_path_and_denies_heads_and_deletions() {
    let (store, mirror) = store_with_mirror();
    with_global_gitconfig("[core]\n\thooksPath = /nonexistent\n", || {
        assert!(push_local(&mirror, "HEAD:refs/heads/main").is_err());
        assert!(push_local(&mirror, "HEAD:refs/mac-worker/bases/0000…0001").is_ok());
        assert!(push_local(&mirror, ":refs/mac-worker/bases/0000…0001").is_err());
    });
    assert_eq!(git(&mirror, ["config", "receive.denyDeletes"]), "true");
}

#[test]
fn outdated_layout_fails_closed_until_setup_migrates_it() {
    let root = v1_layout_store();
    assert_eq!(HostStore::open(root.path()).unwrap_err().public_code(), "HOST_LAYOUT_OUTDATED");
    assert_eq!(probe_against(root.path()).status(), HealthStatus::Unavailable);      // probe never migrates
    run_hidden_host_command(root.path(), ["host", "migrate-layout"]).unwrap();       // what the setup script runs before its final probe
    assert!(root.path().join("repos").is_dir() && root.path().join("tasks").is_dir());
    assert_eq!(layout_version(root.path()), HOST_LAYOUT_VERSION);
    assert!(HostStore::open(root.path()).is_ok());
    run_hidden_host_command(root.path(), ["host", "migrate-layout"]).unwrap();       // idempotent on a current layout
    run_hidden_host_command(&uninitialized_root(), ["host", "migrate-layout"]).unwrap();   // no-op before setup has created the root
}

#[test]
fn local_end_to_end_push_then_fetch_transfers_only_missing_objects() {
    let (transfer, store) = transfer_repo_and_store();
    assert!(push_via_local_executor(&transfer, &store).unwrap().objects_written() > 0);
    assert_eq!(push_via_local_executor(&transfer, &store).unwrap().objects_written(), 0);
    prepare_task_metadata_and_branch(&store, task_id());
    let fetched = fetch_via_local_executor(&transfer, &store, task_id()).unwrap();
    assert_eq!(fetched.head(), expected_head());
    assert_eq!(fetched.local_ref(), format!("refs/mac-worker/results/{}", task_id()));
}
```

Also test: `upload_pack` requires `tasks/<project_id>/<task_id>/meta.json` and `refs/heads/task/<task_id>` in the mirror, never creates the mirror, and refuses extra server arguments; both hidden commands exec with `GIT_CONFIG_GLOBAL=/dev/null` and `GIT_CONFIG_NOSYSTEM=1`; the mirror is created owner-only with `core.hooksPath` pointing at its own hooks directory and the hook file mode `0700`, and a modified hook is rewritten before exec; every protocol fixture derives its version from the constant and a protocol-3 helper is classified `PROTOCOL_MISMATCH`; `git_ssh_command` equals the JSON transport's options minus `--`; the shared `RecordingRunner` records requests, returns scripted results, and is used by at least one existing suite without behaviour change.

- [ ] **Step 2: Run transport tests to verify RED**

Run: `cargo test --locked --test git_transport -- --nocapture`

Expected: FAIL because `crate::git_transport`, the hidden commands, and the layout migration do not exist.

- [ ] **Step 3: Implement the mirror, migration, transport, and version bump**

Bump `PROTOCOL_VERSION` to `4` and update every fixture. Add `"repos"` and `"tasks"` to the owned directories, bump `HOST_LAYOUT_VERSION`, and add `HostStore::migrate_layout` as an `open_inner` mode that acquires the installation lock itself: it creates the two namespaces component by component and rewrites the layout record atomically; a store already at the new version and an uninitialized root are no-ops. It is reachable only through the hidden `host migrate-layout` command (`HostCommand::MigrateLayout`, dispatched in `src/lib.rs`), which the `worker setup` remote script in `src/install.rs` runs after installing the binary and before its final `host probe`. `HostStore::open` and `open_if_present` fail closed on an outdated layout with `HOST_LAYOUT_OUTDATED`; the probe reports the worker unavailable with that code and never migrates. Add:

```rust
impl HostStore {
    pub fn mirror(&self, project_id: &str) -> Result<RootedDir, WorkerError>;                  // repos/<project_id>.git, created on first use
    pub(crate) fn mirror_if_present(&self, project_id: &str) -> Result<Option<RootedDir>, WorkerError>;
}
```

Create `src/git_transport.rs`:

```rust
pub const PRE_RECEIVE_HOOK: &str = "#!/bin/sh\nstatus=0\nwhile read old new ref; do\n  case \"$ref\" in refs/mac-worker/bases/*) ;; *) echo \"mac-worker: ref not allowed: $ref\" >&2; status=1;; esac\n  case \"$new\" in 0000000000000000000000000000000000000000) echo \"mac-worker: deletion not allowed\" >&2; status=1;; esac\ndone\nexit $status\n";

pub struct GitTransport<'a> { runner: &'a dyn ProcessRunner }
pub struct PushReceipt { objects_written: u64 }
pub struct FetchReceipt { head: BaseOid, local_ref: String }
impl<'a> GitTransport<'a> {
    pub fn push_base(&self, worker: &WorkerEntry, identity: &TransferIdentity, project_id: &str, task_id: TaskId, base: &BaseOid, transfer_repo: &Path) -> Result<PushReceipt, WorkerError>;
    pub fn fetch_result(&self, worker: &WorkerEntry, client_id: ClientId, project_id: &str, task_id: TaskId, transfer_repo: &Path) -> Result<FetchReceipt, WorkerError>;
}
pub struct ReceivePackComponents { job_id: JobId, client_id: ClientId, lease_token: LeaseToken, request_fingerprint: RequestFingerprint }
pub struct UploadPackComponents { task_id: TaskId, client_id: ClientId }
pub trait GitServerExecutor: Send + Sync { fn exec(&self, program: &str, mirror: &RootedDir, environment: &[(OsString, OsString)]) -> Result<Infallible, WorkerError>; }
pub struct SystemGitServerExecutor;
pub struct HostGitService<'a> { store: &'a HostStore }
impl<'a> HostGitService<'a> {
    pub fn receive_pack(&self, components: &ReceivePackComponents, path_arg: &str, executor: &dyn GitServerExecutor) -> Result<Infallible, WorkerError>;
    pub fn upload_pack(&self, components: &UploadPackComponents, path_arg: &str, executor: &dyn GitServerExecutor) -> Result<Infallible, WorkerError>;
}
```

`push_base` runs `git -C <transfer> -c gc.auto=0 push --no-verify --receive-pack='~/.local/bin/worker host receive-pack <job_id> <client_id> <lease_token> <fingerprint>' <ssh>:<project_id> <base_oid>:refs/mac-worker/bases/<task_id>` with `GIT_SSH_COMMAND` from `SshTransport::git_ssh_command(worker)` (same options, no trailing `--`), `GIT_CONFIG_GLOBAL=/dev/null`, `GIT_CONFIG_NOSYSTEM=1`, `GIT_TERMINAL_PROMPT=0`. `fetch_result` runs `git -C <transfer> -c gc.auto=0 fetch --no-write-fetch-head --upload-pack='~/.local/bin/worker host upload-pack <task_id> <client_id>' <ssh>:<project_id> +refs/heads/task/<task_id>:refs/mac-worker/results/<task_id>`. Add `HostCommand::ReceivePack { job_id, client_id, lease_token, request_fingerprint, path: HiddenComponent }` and `HostCommand::UploadPack { task_id, client_id, path: HiddenComponent }`; dispatch them in `src/lib.rs` beside `run_host_rsync_receive` with the same binary-stdio boundary and public error mapping. `receive_pack` takes the admission and transfer locks for the job ID, validates identity against the live lease with the existing `require_live_identity` logic, opens or creates the mirror, verifies hook content, mode, `core.hooksPath`, and `receive.denyDeletes`, then execs `git-receive-pack <mirror>` with neutralized global configuration. `upload_pack` verifies task metadata and the branch, then execs `git-upload-pack <mirror>`.

- [ ] **Step 4: Run transport tests to verify GREEN**

Run: `cargo test --locked --test git_transport --test snapshot_transfer --test workers_command --test doctor_command --test setup_command --test job_protocol --test scheduler_adapter --test cli_help -- --nocapture`

Expected: PASS; the rsync path and every protocol-4 fixture agree.

- [ ] **Step 5: Commit the transport**

```bash
git add src/git_transport.rs src/host_store.rs src/transport.rs src/transfer.rs src/protocol.rs src/install.rs src/cli.rs src/lib.rs tests/support tests/git_transport.rs tests/workers_command.rs tests/doctor_command.rs tests/setup_command.rs tests/job_protocol.rs tests/scheduler_adapter.rs tests/snapshot_transfer.rs
git commit -m "feat: route task git transport through host helper"
```

---

### Task 4: Transfer Repository and Base Commits

**Gate:** Start after Task 2; may run in parallel with Task 3. Uses `InputSelector`, `ProjectInspector`, `ProjectSettings`, `ProcessRunner`, and the v1 isolated Git environment, all present after phase 3.

**Files:**
- Create: `src/transfer_repo.rs`
- Create: `tests/transfer_repo.rs`
- Modify: `src/lib.rs` (one line: `pub mod transfer_repo;`)

**Interfaces:**
- Consumes: `ProjectContext`, `ProjectSettings`, `InputSelector`, `InputSelection`, `ProcessRunner`, `PathLayout`, Task 2 `TaskId`/`BaseOid`/`GitIdentity`.
- Produces: `TransferRepo::{open_or_create, path, resolve_base, build_wip_base, check_sensitive_tree, release_base, import_result}`, `BaseCommit`, `BaseKind`, `DirtyReport`, `RepositoryFingerprint` (test support exported under `#[doc(hidden)]`).

- [ ] **Step 1: Write failing transfer-repository tests**

```rust
#[test]
fn committed_base_resolves_without_any_write_to_the_user_repository() {
    let repo = repo_with_commits();
    let before = RepositoryFingerprint::capture(&repo).unwrap();       // HEAD, index bytes, status, all refs, reflogs, config, hooks listing, objects dir listing
    let transfer = TransferRepo::open_or_create(&cache_root(), &repo.common_dir()).unwrap();
    assert_eq!(transfer.repo_id(), repo_id_of(&repo.common_dir()));
    let base = transfer.resolve_base(&runner(), &repo.context(), "HEAD").unwrap();
    assert_eq!(base.kind(), BaseKind::Committed);
    assert_eq!(RepositoryFingerprint::capture(&repo).unwrap(), before);
    assert!(read(transfer.path().join("objects/info/alternates")).trim().ends_with("objects"));
}

#[test]
fn wip_selection_honours_the_user_repository_configuration() {
    let repo = repo_with_commits();
    write(repo.path().join("scratch.log"), "x");
    append(repo.git_path("info/exclude"), "scratch.log\n");
    let transfer = open_transfer(&repo);
    let base = transfer.build_wip_base(&runner(), &repo.context(), task_id(), &settings(), &identity()).unwrap();   // no UNTRACKED_INPUT: excluded by the user's own rules
    assert!(!transfer.tree_of(base.oid()).contains("scratch.log"));
}

#[test]
fn clones_of_one_origin_get_separate_transfer_repositories_and_missing_alternates_fail_early() {
    let (clone_a, clone_b) = two_clones_of_one_origin();
    let a = TransferRepo::open_or_create(&cache_root(), &clone_a.common_dir()).unwrap();
    let b = TransferRepo::open_or_create(&cache_root(), &clone_b.common_dir()).unwrap();
    assert_ne!(a.path(), b.path());
    assert!(b.resolve_base(&runner(), &clone_b.context(), "HEAD").is_ok());
    remove_dir_all(clone_b.path());
    assert_eq!(b.verify_alternates().unwrap_err().public_code(), "BASE_UNAVAILABLE");
}

#[test]
fn wip_base_captures_selection_into_transfer_repo_only() {
    let repo = repo_with_dirty_worktree();               // modified tracked, staged new, deleted tracked, untracked fixture, ignored output
    let before = RepositoryFingerprint::capture(&repo).unwrap();
    let transfer = open_transfer(&repo);
    let base = transfer.build_wip_base(&runner(), &repo.context(), task_id(), &settings_including("fixtures/**"), &identity()).unwrap();
    assert_eq!(base.kind(), BaseKind::Wip);
    let tree = transfer.tree_of(base.oid());
    assert_eq!(tree.blob("src/app.rs"), repo.worktree_bytes("src/app.rs"));
    assert!(tree.contains("fixtures/generated.txt") && !tree.contains("deleted.rs") && !tree.contains("target/out.bin"));
    assert_eq!(transfer.parent_of(base.oid()), repo.head());
    assert_eq!(RepositoryFingerprint::capture(&repo).unwrap(), before);
    assert!(transfer.has_ref(&format!("refs/mac-worker/bases/{}", task_id())));
    assert!(!repo.has_ref(&format!("refs/mac-worker/bases/{}", task_id())));
}

#[test]
fn second_capture_mismatch_is_snapshot_changed_and_leaves_no_ref() {
    let repo = repo_with_commits();
    let transfer = open_transfer(&repo);
    let err = transfer.build_wip_base_with_hook(&runner(), &repo.context(), task_id(), &settings(), &identity(), mutate_between_captures(&repo)).unwrap_err();
    assert_eq!(err.public_code(), "SNAPSHOT_CHANGED");
    assert!(!transfer.has_ref(&format!("refs/mac-worker/bases/{}", task_id())));
}

#[test]
fn committed_base_with_tracked_secret_fails_sensitive_path() {
    let repo = repo_with_tracked(".env");
    let transfer = open_transfer(&repo);
    let base = transfer.resolve_base(&runner(), &repo.context(), "HEAD").unwrap();
    assert_eq!(transfer.check_sensitive_tree(&runner(), base.oid(), &settings()).unwrap_err().public_code(), "SENSITIVE_PATH");
}

#[test]
fn import_result_writes_exactly_one_remote_tracking_ref() {
    let (repo, transfer) = repo_and_transfer_with_result(task_id());
    let before = RepositoryFingerprint::capture(&repo).unwrap();
    let receipt = transfer.import_result(&runner(), &repo.common_dir(), "mini-1", task_id()).unwrap();
    let after = RepositoryFingerprint::capture(&repo).unwrap();
    assert_eq!(after.diff(&before), vec![format!("+refs/remotes/mac-worker/mini-1/task/{}", task_id())]);   // no reflog, no FETCH_HEAD
    assert_eq!(receipt.head(), expected_head());
    assert!(!repo.git_path("FETCH_HEAD").exists());
    assert!(!repo.git_path(&format!("logs/refs/remotes/mac-worker/mini-1/task/{}", task_id())).exists());
}

#[test]
fn base_refs_resolve_in_the_user_repository_not_the_bare_transfer_repository() {
    let repo = repo_with_branch("feature");
    let transfer = open_transfer(&repo);
    let base = transfer.resolve_base(&runner(), &repo.context(), "feature").unwrap();
    assert_eq!(base.oid(), &repo.rev_parse("feature"));
    assert!(git_in(transfer.path(), ["rev-parse", "--verify", "feature"]).is_err());   // the transfer repository has no branches
}
```

Also test: selection works from a linked worktree whose common directory is elsewhere; symlinks recorded as symlinks (`120000` cacheinfo entries from `hash-object --stdin` of the link target) and executable bits preserved; `UNTRACKED_INPUT` for uncovered untracked files; `resolve_base` rejects non-commit objects and refs outside the repository; a merge in progress yields `TASK_CONFIG_INVALID`; `DirtyReport` counts modified/added/deleted without contents; `release_base` removes only the task's base ref; every command that writes runs with `--git-dir=<transfer>` and never with the user's `.git`; a concurrent `index.lock` in the user's repository never blocks the builder because it never opens the user's index.

- [ ] **Step 2: Run transfer-repository tests to verify RED**

Run: `cargo test --locked --test transfer_repo -- --nocapture`

Expected: FAIL because `crate::transfer_repo` does not exist.

- [ ] **Step 3: Implement the transfer repository**

```rust
pub enum BaseKind { Committed, Wip }
pub struct BaseCommit { oid: BaseOid, kind: BaseKind, head_oid: BaseOid, branch: Option<String>, dirty: DirtyReport }
pub struct DirtyReport { modified: usize, added: usize, deleted: usize }
pub struct TransferRepo { path: PathBuf, repo_id: String, alternates_target: PathBuf }
impl TransferRepo {
    pub fn open_or_create(cache_root: &Path, user_common_dir: &Path) -> Result<Self, WorkerError>;   // repo_id = sha256(canonical common dir)
    pub fn repo_id(&self) -> &str;
    pub fn verify_alternates(&self) -> Result<(), WorkerError>;
    pub fn path(&self) -> &Path;
    pub fn resolve_base(&self, runner: &dyn ProcessRunner, context: &ProjectContext, reference: &str) -> Result<BaseCommit, WorkerError>;
    pub fn build_wip_base(&self, runner: &dyn ProcessRunner, context: &ProjectContext, task_id: TaskId, settings: &ProjectSettings, identity: &GitIdentity) -> Result<BaseCommit, WorkerError>;
    pub fn check_sensitive_tree(&self, runner: &dyn ProcessRunner, base: &BaseOid, settings: &ProjectSettings) -> Result<(), WorkerError>;
    pub fn release_base(&self, runner: &dyn ProcessRunner, task_id: TaskId) -> Result<(), WorkerError>;
    pub fn import_result(&self, runner: &dyn ProcessRunner, user_common_dir: &Path, worker: &str, task_id: TaskId) -> Result<FetchReceipt, WorkerError>;
}
```

`open_or_create` runs `git init --bare` under `transfer/<repo_id>.git` and writes `objects/info/alternates` with the repository's `objects` directory; `verify_alternates` fails with `BASE_UNAVAILABLE` when that directory is gone and is called before every resolve, build, push, and import. `build_wip_base` runs the v1 `InputSelector` read-only inside the user's worktree with the user's own configuration, then, with `--git-dir=<transfer>` and a scratch `GIT_INDEX_FILE` inside the transfer repository, writes each selected regular file with `hash-object -w --no-filters`, each symlink target with `hash-object -w --stdin`, populates the scratch index with `update-index --add --cacheinfo <mode>,<oid>,<path>` (tracked deletions are simply absent), writes the tree, repeats the selection and hashing into a second scratch index, compares tree IDs (`SNAPSHOT_CHANGED` on mismatch), creates the commit with `git commit-tree` using `GIT_AUTHOR_*`/`GIT_COMMITTER_*` from the recorded identity and the fixed message, and records `refs/mac-worker/bases/<task_id>` in the transfer repository. `GIT_INDEX_FILE` is set only on the `--git-dir=<transfer>` invocations and is never exported to the selection or resolution commands that run in the user's repository, which keep the v1 environment that removes it; a leaked scratch index would make the user's own `git status` report it as the real index. Ref resolution never happens in the bare transfer repository, which shares objects through alternates but has no branches: `resolve_base` runs a read-only `git -C <user worktree> rev-parse --verify <ref>^{commit}` in the user's repository and every later command (`commit-tree -p`, `push`, `check_sensitive_tree`) receives the explicit OID. `check_sensitive_tree` applies the v1 sensitive-path policy to `git --git-dir=<transfer> ls-tree -r --name-only -z <oid>`. `import_result` runs `git -C <user common dir> -c gc.auto=0 -c core.logAllRefUpdates=false fetch --no-write-fetch-head <transfer> +refs/mac-worker/results/<task_id>:refs/remotes/mac-worker/<worker>/task/<task_id>` with neutralized global configuration, so the only change to the user's repository is the one ref and its objects.

- [ ] **Step 4: Run transfer-repository tests to verify GREEN**

Run: `cargo test --locked --test transfer_repo --test input_selection --test snapshot_capture -- --nocapture`

Expected: PASS; snapshot capture is unchanged.

- [ ] **Step 5: Commit the transfer repository**

```bash
git add src/lib.rs src/transfer_repo.rs tests/transfer_repo.rs
git commit -m "feat: build task bases in a transfer repository"
```

---

### Task 5: Worker-Side Task Store and Materialization

**Gate:** Start after Task 3.

**Files:**
- Create: `src/task_store.rs`
- Create: `tests/task_materialization.rs`
- Modify: `src/host_store.rs`
- Modify: `src/transfer.rs`
- Modify: `src/cli.rs`
- Modify: `src/lib.rs`
- Modify: `src/task.rs`

**Interfaces:**
- Consumes: `HostStore`, `RootedDir`, `LeaseRecord`, `TransferGuard`, `ProcessRunner`, Task 2 records, Task 3 mirror.
- Produces: `TaskStore::{prepare, status, diff, close, publish_branch_into_mirror, load_meta, load_status, replace_status_after, bind_session, session}`, `SessionBinding`, `TaskPrepareRequest`, `TaskPrepareResponse`, `TaskStatusRequest`, `TaskStatusResponse`, `TaskDiffRequest`, `TaskDiffResponse`, `TaskCloseRequest`, `TaskCloseResponse`, `HostOperation::{TaskPrepare, TaskStatus, TaskDiff, TaskClose}`, hidden `host task-prepare|task-status|task-diff|task-close`, `MAX_DIFF_BYTES`.

- [ ] **Step 1: Write failing materialization tests**

```rust
#[test]
fn prepare_creates_shared_clone_on_task_branch_at_verified_base_under_lease() {
    let (store, mirror) = store_with_mirror_containing(base_oid());
    let response = TaskStore::new(&store, &runner()).prepare(&prepare_request(base_oid()), &transfer_guard()).unwrap();
    assert_eq!(response.head(), &base_oid());
    assert!(!response.reused());
    let workspace = store.task_workspace(&project_id(), task_id()).unwrap();
    assert_eq!(git(&workspace, ["rev-parse", "--abbrev-ref", "HEAD"]), format!("task/{}", task_id()));
    assert!(workspace.join(".git").is_dir());                                     // own git dir inside the writable checkout
    assert!(read(workspace.join(".git/objects/info/alternates")).trim().ends_with("objects"));
    assert!(!git_ref_exists(&mirror, &format!("refs/heads/task/{}", task_id())));  // the mirror learns the branch only at publication
    assert_eq!(store.task_status(&project_id(), task_id()).unwrap().state(), TaskState::Active);
}

#[test]
fn prepare_is_idempotent_on_retry_and_refuses_inconsistent_state() {
    let (store, _) = store_with_mirror_containing(base_oid());
    TaskStore::new(&store, &runner()).prepare(&prepare_request(base_oid()), &transfer_guard()).unwrap();
    assert!(TaskStore::new(&store, &runner()).prepare(&prepare_request(base_oid()), &transfer_guard()).unwrap().reused());
    truncate_clone_to_partial(store.task_workspace(&project_id(), task_id()).unwrap());   // crash mid-clone: no valid HEAD
    assert!(!TaskStore::new(&store, &runner()).prepare(&prepare_request(base_oid()), &transfer_guard()).unwrap().reused());
    commit_in_workspace(&store, task_id());
    let err = TaskStore::new(&store, &runner()).prepare(&prepare_request(other_oid()), &transfer_guard()).unwrap_err();
    assert_eq!(err.public_code(), "WORKTREE_INCONSISTENT");
}

#[test]
fn codex_workspace_sandbox_can_commit_inside_the_shared_clone() {
    // Runs only when `codex` is installed locally; otherwise it is skipped with a message.
    let (store, _) = store_with_prepared_task();
    let workspace = store.task_workspace(&project_id(), task_id()).unwrap();
    let status = codex_sandbox_run(&workspace, "git -C . commit --allow-empty -m probe");
    assert!(status.success(), "workspace-write sandbox must allow commits inside the clone");
}

#[test]
fn diff_uses_private_index_and_never_locks_the_workspace() {
    let (store, _) = store_with_prepared_task();
    let workspace = store.task_workspace(&project_id(), task_id()).unwrap();
    write(workspace.join("a.txt"), "changed");
    let index = git_path(&workspace, "index");
    let before = fingerprint(&index);
    let diff = TaskStore::new(&store, &runner()).diff(&diff_request(false)).unwrap();
    assert!(diff.text().contains("+changed") && !diff.truncated());
    assert_eq!(fingerprint(&index), before);
    assert!(!index.with_extension("lock").exists());
    let big = TaskStore::new(&store, &runner()).diff(&diff_request_for_large_change()).unwrap();
    assert!(big.truncated() && big.text().len() <= MAX_DIFF_BYTES);
}

#[test]
fn close_removes_only_the_workspace_and_discard_prunes_refs() {
    let (store, mirror) = store_with_prepared_task();
    commit_in_workspace(&store, task_id());
    publish_branch_into_mirror(&store, task_id());                                 // what the publisher does at turn end
    TaskStore::new(&store, &runner()).close(&close_request(task_id(), false)).unwrap();
    assert!(store.task_workspace_if_present(&project_id(), task_id()).unwrap().is_none());
    assert!(store.task_dir(&project_id(), task_id()).unwrap().join("meta.json").exists());
    assert_eq!(store.task_status(&project_id(), task_id()).unwrap().state(), TaskState::Closed);
    assert!(git_ref_exists(&mirror, &format!("refs/heads/task/{}", task_id())));
    TaskStore::new(&store, &runner()).close(&close_request(task_id(), true)).unwrap();
    assert!(!git_ref_exists(&mirror, &format!("refs/heads/task/{}", task_id())));
    assert!(!git_ref_exists(&mirror, &format!("refs/mac-worker/bases/{}", task_id())));
}
```

Also test: two tasks on one mirror do not see each other's uncommitted work; `diff --stat` on a task with ignored files present works and `close` succeeds with ignored files present; `publish_branch_into_mirror` uses `git -C <mirror> fetch <workspace> +refs/heads/task/<id>:refs/heads/task/<id>` and succeeds although the mirror's pre-receive hook rejects client pushes to `refs/heads/*`; `status` on an unknown task is `TASK_NOT_FOUND`; `close` on a task with an active turn is `TASK_BUSY`; task meta, status, and session files are owner-only canonical JSON validated no-follow; symlinked `workspace` entries are refused; `bind_session` writes `session.json` once and a second binding with a different reference is rejected; `prepare` fails with `BASE_UNAVAILABLE` before any directory when the base is missing or not a commit; every DTO carries `protocol_version` and rejects unknown fields; diff output is measured after JSON escaping against `MAX_DIFF_BYTES = 512 * 1024`.

- [ ] **Step 2: Run materialization tests to verify RED**

Run: `cargo test --locked --test task_materialization -- --nocapture`

Expected: FAIL because `crate::task_store` does not exist.

- [ ] **Step 3: Implement the task store**

```rust
pub const MAX_DIFF_BYTES: usize = 512 * 1024;
pub struct SessionBinding { agent: AgentKind, session_ref: String, bound_at_millis: u64 }
pub struct TaskStore<'a> { store: &'a HostStore, runner: &'a dyn ProcessRunner }
impl<'a> TaskStore<'a> {
    pub fn prepare(&self, request: &TaskPrepareRequest, guard: &TransferGuard) -> Result<TaskPrepareResponse, WorkerError>;
    pub fn status(&self, request: &TaskStatusRequest) -> Result<TaskStatusResponse, WorkerError>;
    pub fn diff(&self, request: &TaskDiffRequest) -> Result<TaskDiffResponse, WorkerError>;
    pub fn close(&self, request: &TaskCloseRequest) -> Result<TaskCloseResponse, WorkerError>;
    pub fn bind_session(&self, project_id: &str, task_id: TaskId, binding: SessionBinding) -> Result<(), WorkerError>;
    pub fn session(&self, project_id: &str, task_id: TaskId) -> Result<Option<SessionBinding>, WorkerError>;
    pub(crate) fn replace_status_after(&self, project_id: &str, task_id: TaskId, update: impl FnOnce(TaskStatus) -> Result<TaskStatus, WorkerError>) -> Result<TaskStatus, WorkerError>;
}
```

`prepare` runs under the turn's transfer lock, verifies `git cat-file -t <base_oid>` is `commit` in the mirror, then: if the workspace exists on branch `task/<task_id>` with `HEAD == base_oid` and a clean `status --porcelain`, returns `reused`; if a workspace directory exists without a valid `HEAD` (a crash mid-clone), removes it through the rooted filesystem layer and recreates it; if no workspace exists, creates the task directory component by component and runs `git clone --shared --no-checkout <mirror> <workspace>` then `git -C <workspace> checkout -b task/<task_id> <base_oid>`; anything else (a different branch, a different base, local changes) is `WORKTREE_INCONSISTENT`. The clone's `.git` lives inside the workspace so sandboxes that confine writes to the checkout can commit; the mirror receives the branch only when the publisher fetches it at turn end. It writes `meta.json` and `status.json` (`Active`, worker name, turn 1 pending) and fsyncs. `diff` copies the workspace index (`git rev-parse --git-path index`) to an owner-only temporary file, runs `git diff [--stat] <base_oid>` with `GIT_INDEX_FILE` pointing at the copy and the isolated Git environment, bounds the escaped output to `MAX_DIFF_BYTES`, and reports truncation. `close` refuses while a turn is active, removes only `workspace/` through the rooted filesystem layer, marks `Closed`; with `discard` it also deletes `refs/heads/task/<task_id>` and `refs/mac-worker/bases/<task_id>` from the mirror and marks `Abandoned`. Add the four `HostOperation` variants with commands `~/.local/bin/worker host task-prepare|task-status|task-diff|task-close`, their `HostCommand` entries, and `src/lib.rs` dispatch through the `SshJsonTransport` stdin/stdout JSON boundary with its 1 MiB bounds.

- [ ] **Step 4: Run materialization tests to verify GREEN**

Run: `cargo test --locked --test task_materialization --test git_transport --test host_lease -- --nocapture`

Expected: PASS; lease behaviour is unchanged.

- [ ] **Step 5: Commit the task store**

```bash
git add src/task_store.rs src/host_store.rs src/transfer.rs src/cli.rs src/lib.rs src/task.rs tests/task_materialization.rs
git commit -m "feat: materialize task worktrees on workers"
```

---

### Task 6: Agent Turn Execution and Publication

**Gate:** Start after Tasks 1, 2, and 5. Modifies `job.rs`, `job_service.rs`, and `supervisor.rs`, which phase 3 and phase 4 own; rebase on their final `main` state.

**Files:**
- Create: `src/turn.rs`
- Create: `tests/task_turn.rs`
- Modify: `src/job.rs`
- Modify: `src/job_service.rs`
- Modify: `src/host_store.rs`
- Modify: `src/supervisor.rs`
- Modify: `src/task.rs`
- Modify: `src/task_store.rs`
- Modify: `src/transfer.rs`
- Modify: `src/cli.rs`
- Modify: `src/lib.rs`
- Modify: `tests/supervisor.rs`
- Modify: `tests/job_protocol.rs`
- Modify: `tests/job_queries.rs`

**Interfaces:**
- Consumes: `RequestFingerprintMaterial` (unchanged), `SubmitRequest`, `JobService`, `StagedJob`, `WorkspaceReceipt`, `Supervisor`, `SupervisorGuard`, `GatedChild`, `ExecutionPayload`, `CommandSpec::Shell`, `LogStream`, Task 1 adapters and `render_shell`, Task 5 store.
- Produces: `TurnMaterial` with canonical bytes and digest, `EXECUTION_PAYLOAD_VERSION == 2` (the existing private constant in `job_service.rs`), `SUPERVISION_VERSION == 3`, `TurnSection`, `TurnReceipt`, `TaskTurnRequest`, `TaskTurnResponse`, `EnvProfile`, `LaunchPlan`, `StdinSource`, `TurnTerminalHook`, `TurnPublisher`, `TurnResult`, `LOG_CAP_BYTES`, `LOG_TAIL_BYTES`, `HostOperation::TaskTurn`, hidden `host task-turn`.

- [ ] **Step 1: Write failing turn tests**

```rust
#[test]
fn turn_material_commits_through_the_digest_slot_and_leaves_v1_material_unchanged() {
    let turn_a = turn_material("prompt-a");
    let turn_b = turn_material("prompt-b");
    let a = material_for_turn(&turn_a);
    let b = material_for_turn(&turn_b);
    assert_ne!(a.fingerprint(), b.fingerprint());
    assert_eq!(a.manifest_digest(), turn_a.digest());                          // sha256 of canonical TurnMaterial bytes
    assert!(serde_json::to_value(&a).unwrap().get("turn").is_none());          // RequestFingerprintMaterial has no new field
    assert_eq!(a.relative_working_dir(), "");
    assert!(matches!(a.command(), CommandSpec::Shell { .. }));
    let json = serde_json::to_value(&turn_a).unwrap();
    assert!(json["prompt_sha256"].is_string() && json["base_oid"].is_string());
    assert!(json.get("prompt").is_none() && json.get("session_ref").is_none());
    assert_eq!(reconstructed_fingerprint_from_job_meta(&a), a.fingerprint());   // every v1 re-derivation path still matches
}

#[test]
fn host_rejects_turn_material_or_prompt_that_do_not_match_the_digests() {
    let (store, prepared) = store_with_prepared_task();
    let request = task_turn_request(&prepared, turn_material("prompt-a"), "prompt-a");
    assert!(JobService::new(&store, &launcher()).submit_turn(request.clone()).is_ok());
    let tampered_turn = request.with_turn(turn_material("prompt-b"));
    assert_eq!(JobService::new(&store, &launcher()).submit_turn(tampered_turn).unwrap_err().public_code(), request_conflict_code());
    let tampered_prompt = request.with_prompt("prompt-c");
    assert_eq!(JobService::new(&store, &launcher()).submit_turn(tampered_prompt).unwrap_err().public_code(), request_conflict_code());
}

#[test]
fn turn_launch_plan_uses_login_shell_real_home_profile_identity_and_stdin() {
    let plan = LaunchPlan::turn(&shell_command(), &lease(), &turn_section(), &account_home("/Users/w"), &profile_with(["CLAUDE_CODE_OAUTH_TOKEN"]), &identity()).unwrap();
    assert_eq!(plan.program(), "/bin/zsh");
    assert_eq!(plan.args()[1], "-lc");
    assert_eq!(plan.cwd(), task_workspace_path());
    assert!(plan.env().contains(&("HOME".into(), "/Users/w".into())));
    for name in ["USER", "LOGNAME", "SHELL", "TMPDIR", "GIT_AUTHOR_NAME", "GIT_COMMITTER_EMAIL", "CLAUDE_CODE_OAUTH_TOKEN", "MAC_WORKER_TASK_ID", "MAC_WORKER_TURN"] {
        assert!(plan.env().iter().any(|(k, _)| k == name), "{name}");
    }
    assert!(plan.env().iter().all(|(k, _)| k != "PATH"));
    assert_eq!(plan.stdin(), &StdinSource::File("prompt.md".into()));
}

#[test]
fn batch_launch_plan_matches_golden() {
    let plan = LaunchPlan::batch(&argv_command(), &lease(), &per_job_home(), &per_job_tmp()).unwrap();
    assert_eq!(plan, golden_batch_plan());          // environment, cwd, stdin /dev/null, controlled PATH, exactly today's values
}

#[test]
fn env_profile_requires_owner_only_regular_file_and_hides_values() {
    let path = temp_profile("CLAUDE_CODE_OAUTH_TOKEN=secret-value\n", 0o644);
    assert_eq!(EnvProfile::load(&path).unwrap_err().public_code(), "ENV_PROFILE_PERMISSIONS");
    set_mode(&path, 0o600);
    let profile = EnvProfile::load(&path).unwrap();
    assert_eq!(profile.names(), &["CLAUDE_CODE_OAUTH_TOKEN"]);
    assert!(!format!("{profile:?}").contains("secret-value"));
}

#[test]
fn log_cap_keeps_draining_and_retains_tail_with_result() {
    let (store, guard) = supervised_turn_with_stream(generate_stream(300 * MIB, final_result_event()));
    let status = run_supervisor(&store, guard).unwrap();
    assert!(status.log_truncated());
    assert!(log_size(&store) <= LOG_CAP_BYTES + 64 * 1024);
    assert_eq!(status.final_stdout_bytes(), log_size(&store));                  // validate_terminal_log_lengths still holds
    assert!(read_tail(&store).ends_with(final_result_event()));
    assert_eq!(store.task_status(&project_id(), task_id()).unwrap().last_outcome(), Some(&TaskOutcome::Done));
}

#[test]
fn every_terminal_path_publishes_before_lease_release() {
    for path in [TerminalPath::ChildExit(0), TerminalPath::Timeout, TerminalPath::PrelaunchFailure, TerminalPath::AmbiguousChild, TerminalPath::HostCancel, TerminalPath::LostReconciliation] {
        let (store, task) = store_with_prepared_task();
        drive_turn_to(&store, &task, path);
        let status = store.task_status(&task.project_id(), task.id()).unwrap();
        assert_ne!(status.state(), TaskState::Active, "{path:?}");
        assert!(lease_released(&store), "{path:?}");
    }
}

#[test]
fn publisher_commits_leftovers_binds_session_and_marks_task_open() {
    let (store, task) = store_with_prepared_task();
    write_uncommitted(&task.workspace(), "src/new.rs");
    let result = TurnPublisher::new(&store, &runner()).publish(&task, &turn_dir_with(fixture("codex-needs-input.jsonl")), Some(0)).unwrap();
    assert_eq!(result.outcome(), TaskOutcome::NeedsInput);
    assert!(!result.agent_committed());
    assert_eq!(git(&task.workspace(), ["status", "--porcelain"]), "");
    assert_eq!(git(&task.workspace(), ["log", "-1", "--format=%an <%ae> %s"]), "Submitter <s@example> mac-worker: uncommitted changes after turn <turn_id>");
    let status = store.task_status(&task.project_id(), task.id()).unwrap();
    assert_eq!(status.state(), TaskState::Open);
    assert_eq!(status.questions().len(), 1);
    assert!(store.session(&task.project_id(), task.id()).unwrap().is_some());
}
```

Also test: the supervisor binds the session the moment the adapter's session-started event appears in the stream, so a turn cancelled or timed out before publication still has `session.json`; a mac-worker-generated Claude session is bound at turn acceptance; reconciliation of a `lost` turn re-extracts a missing binding from the recorded stream; the publisher fetches the result branch from the clone into the mirror at every terminal path that has a workspace, so `fetch` works after close; publisher records `diff_stat`, `files_changed`, and `head_oid`; a clean workspace yields `agent_committed = true` and no extra commit; `timed_out` and `cancelled` turns publish and leave the task `Open` with the matching outcome; a `lost` turn (reconciliation) leaves the task `Open` with `TaskOutcome::Lost` and the workspace in place; `close_policy = done` plus outcome `Done` closes the task through `TaskStore::close` under the lease and keeps metadata; the turn's job directory contains `prompt.md` owner-only and no workspace; supervisor terminal cleanup for a turn removes nothing under `tasks/`; `TaskTurnRequest` prompt hash mismatch is rejected before acceptance and a repeated identical request is idempotent; a resumed turn without `session.json` fails with `SESSION_UNBOUND` before launch; a batch payload at version 2 carries `turn: null` and launches unchanged; `SUPERVISION_VERSION` and `EXECUTION_PAYLOAD_VERSION` fixtures are pinned; the prelaunch validator accepts the turn layout (`prompt.md`, `result.schema.json`, no `workspace`) and still rejects extra entries in batch jobs; turn cleanup tolerates the absent `workspace` entry and never touches `tasks/`; a workspace-write fallback for Claude is recorded as `permission_fallback`.

- [ ] **Step 2: Run turn tests to verify RED**

Run: `cargo test --locked --test task_turn -- --nocapture`

Expected: FAIL because turn material, the launch plan, env profiles, and the publisher do not exist.

- [ ] **Step 3: Implement turn material, the launch seam, and the publisher**

`RequestFingerprintMaterial`, `JobMeta`, and `LeaseRecord` stay byte-for-byte as phase 3 left them. In `src/turn.rs` define the turn material with canonical bytes, and build the v1 material for a turn with `manifest_digest = turn.digest()`, `relative_working_dir = ""`, `resource_class = "heavy"`, and `command = CommandSpec::Shell { shell: render_shell(&launch) }`:

```rust
pub struct TurnMaterial { task_id: TaskId, turn_number: u32, agent: AgentKind, model: Option<String>, policy: PermissionPolicy, limits: TurnLimits, base_oid: BaseOid, prompt_sha256: String, env_profile: Option<String>, session_seed: Uuid, resume: bool }
impl TurnMaterial { pub fn canonical_bytes(&self) -> Result<Vec<u8>, WorkerError>; pub fn digest(&self) -> String; }
```

The host verifies `turn.digest() == material.manifest_digest()` and `sha256(prompt) == turn.prompt_sha256` in `submit_turn` before acceptance and again from the payload before launch, so every v1 path that re-derives the fingerprint from persisted fields keeps matching. Also in `src/turn.rs`:

```rust
pub const LOG_CAP_BYTES: u64 = 256 * 1024 * 1024;
pub const LOG_TAIL_BYTES: usize = 64 * 1024;
pub struct TurnSection { turn: TurnMaterial, project_id: String, git_identity: GitIdentity }
pub struct TurnReceipt { job_id: JobId, task_id: TaskId, prepared_head: BaseOid, staging_nonce: StagingNonce }   // minted by submit_turn, never by task-prepare
pub struct TaskTurnRequest { submit: SubmitRequest, turn: TurnMaterial, prompt: String }
pub enum TerminalPath { ChildExit(i32), Timeout, PrelaunchFailure, AmbiguousChild, HostCancel, LostReconciliation }
pub struct TurnTerminalHook;   // invoked from the common status writer on every terminal transition, before lease release
pub struct TaskTurnResponse { submit: SubmitResponse, task: TaskStatus }
pub struct EnvProfile { names: Vec<String>, entries: Vec<(OsString, OsString)> }
impl EnvProfile { pub fn load(path: &Path) -> Result<Self, WorkerError>; pub fn names(&self) -> &[String]; }
pub struct TurnPublisher<'a> { store: &'a HostStore, runner: &'a dyn ProcessRunner }
impl<'a> TurnPublisher<'a> { pub fn publish(&self, task: &PreparedTask, turn_dir: &RootedDir, exit_code: Option<i32>) -> Result<TurnResult, WorkerError>; }
```

In `src/supervisor.rs` introduce a `#[doc(hidden)] pub struct LaunchPlan { program, args, env: Vec<(OsString, OsString)>, cwd: PathBuf, stdin: StdinSource, stdout: StdoutSink }` with `LaunchPlan::batch` producing exactly today's launch (golden test, direct log descriptor) and `LaunchPlan::turn` producing `/bin/zsh -lc <shell>` with `HOME` set to the account home, `USER`, `LOGNAME`, `SHELL` from the account, `TMPDIR` to the turn temp directory, `MAC_WORKER_TURN_DIR` to the turn's job directory, the `MAC_WORKER_*` variables plus `MAC_WORKER_TASK_ID` and `MAC_WORKER_TURN`, `GIT_AUTHOR_*` and `GIT_COMMITTER_*` from the recorded identity, env-profile entries, no controlled `PATH`, cwd `tasks/<project_id>/<task_id>/workspace`, stdin from the turn's `prompt.md`, and stdout through a pipe. Bump `SUPERVISION_VERSION` to `3` and the existing private `EXECUTION_PAYLOAD_VERSION` in `job_service.rs` to `2`; batch payloads carry `turn: null`. For turns, `GatedChild::spawn` creates the stdout pipe and a pump thread that appends to `stdout.log` until `LOG_CAP_BYTES`, keeps draining afterwards, retains the last `LOG_TAIL_BYTES` in memory, writes them to `tail.log` at exit, marks `log_truncated`, and records the capped byte count as the terminal stdout length so `validate_terminal_log_lengths` holds. The pump never waits for pipe EOF: it stops at the supervisor's process-group-absence proof plus a one-second grace, closes the read end, and records the count at that point, so a process the agent detached with `setsid` or `nohup` while holding the write end cannot hang the terminal transition. The pump scans complete lines with the adapter's `parse_event` until it sees `SessionStarted`, then calls `TaskStore::bind_session` once and stops scanning; `submit_turn` binds mac-worker-generated identifiers at acceptance; reconciliation that marks a turn `lost` re-extracts a missing binding from the recorded stream.

In `src/host_store.rs` extend `publish_complete` to accept a `TurnReceipt` carrying the staging nonce that `begin_job_after` generated for this job, in place of a `WorkspaceReceipt`; `task-prepare` cannot mint it because the nonce is created inside `submit_turn`, a later host process, so `TaskPrepareResponse` stays informational. Extend the prelaunch validator with the turn layout set (`meta.json`, `status.json`, `execution.json`, `prompt.md`, `result.schema.json`, log files, no `workspace`); make `remove_job_mutable_scopes` tolerate the absent `workspace` entry; and keep every job cleanup path below the job directory, never touching `tasks/`.

`TurnTerminalHook` is invoked from the supervisor's common status writer (`replace_status`, which wraps `replace_job_status_after`) whenever the new state is terminal, and from phase 4's host cancellation path in `job_service.rs` and lost-turn reconciliation; the named paths (child exit, timeout, prelaunch failure, ambiguous child, host cancel, lost reconciliation) are the test matrix, not the call sites, so the ambiguous-child writer that records `Lost` is covered too. The hook runs `TurnPublisher::publish` where a workspace exists and otherwise `TaskStore::replace_status_after` with the matching outcome. `publish` commits leftovers with the recorded identity, then brings the branch into the mirror with `git -C <mirror> fetch <workspace> +refs/heads/task/<task_id>:refs/heads/task/<task_id>` (a fetch, so the client-facing pre-receive hook does not apply), then records the diff summary, result, and session; publication failure is recorded as `PUBLISH_FAILED` on the turn and the task stays `Open`. In `src/job_service.rs` add `submit_turn(TaskTurnRequest)`, which under the admission lock verifies that the task's `status.json` records this job ID as the prepared turn with `head == turn.base_oid` and state `Active`, mints the staging-bound `TurnReceipt`, verifies both digests, stores `prompt.md` and `result.schema.json`, requires `session.json` for `resume`, and otherwise reuses durable acceptance, idempotency, and launch. Add `HostOperation::TaskTurn` and hidden `host task-turn`.

- [ ] **Step 4: Run turn and supervisor regressions**

Run: `cargo test --locked --test task_turn --test supervisor --test job_protocol --test job_queries --test task_materialization -- --nocapture`

Expected: PASS; every existing supervisor assertion holds for batch launches through the golden plan.

- [ ] **Step 5: Commit turn execution**

```bash
git add src/turn.rs src/job.rs src/job_service.rs src/host_store.rs src/supervisor.rs src/task.rs src/task_store.rs src/transfer.rs src/cli.rs src/lib.rs tests/task_turn.rs tests/supervisor.rs tests/job_protocol.rs tests/job_queries.rs
git commit -m "feat: run agent turns under the durable supervisor"
```

---

### Task 7: Client Lifecycle, Local Turn Runner, and Scheduler Integration

**Gate:** Start after Tasks 4 and 6. Modifies `run.rs`, `client_state.rs`, and `job.rs` queue records that phase 4 owns; rebase on their final `main` state.

**Files:**
- Create: `src/task_client.rs`
- Create: `src/turn_runner.rs`
- Create: `tests/support/task_harness.rs`
- Create: `tests/turn_runner.rs`
- Create: `tests/task_command.rs`
- Modify: `src/job.rs`
- Modify: `src/client_state.rs`
- Modify: `src/run.rs`
- Modify: `src/transfer.rs`
- Modify: `src/cli.rs`
- Modify: `src/lib.rs`
- Modify: `src/output.rs`
- Modify: `src/project_config.rs`
- Modify: `tests/cli_help.rs`, `tests/client_state.rs`, `tests/project_config.rs`, `tests/scheduler_queue.rs`, `tests/run_command.rs`

**Interfaces:**
- Consumes: `SchedulerService`, `QueueEntry`, `WorkerPreference`, `RunService` follow path, `RemoteJobClient`, `ClientStateStore`, `ProjectState`, `WorkersService`, Task 3 `GitTransport`, Task 4 `TransferRepo`, Task 5 DTOs, Task 6 `TurnMaterial` and `TaskTurnRequest`.
- Produces: `TaskSubmitRequest`, `TaskClient::{submit, status, list, logs, diff, result, fetch, close, reconcile_runners}`, `TurnRunner`, `RunnerExecutor` (`Detached`, `Inline`), `ClientStateStore::{create_task, load_task, update_task, list_tasks, adopt_row, record_runner, runner_liveness, observation_cache, park_row, unpark_oldest}`, `QueueEntryKind::TaskTurn`, `QueueEntry::run`, `QueueEntry::owner` valid in both states, `JsonEvent::{TaskCreated, TaskState, TurnAccepted, TurnTerminal, ResultImported}`, `ProjectSettings::task`, `CAPABILITY_MISSING`, public `worker task submit|list|status|logs|diff|result|fetch|close|reconcile`, hidden `worker runner <task_id> <turn_id>`.

- [ ] **Step 1: Write failing runner and client tests**

```rust
#[test]
fn submit_returns_after_handoff_and_runner_completes_the_turn() {
    let harness = TaskHarness::inline_runner();
    let report = harness.run(["task", "submit", "--agent", "codex", "--prompt-file", "p.md", "--json"]).unwrap();
    assert_eq!(harness.stages_before_return(), vec![
        Stage::ProjectInspection, Stage::SettingsAndRequirements, Stage::FleetProbe, Stage::NoWaitCheck,
        Stage::BaseCapture, Stage::LocalTaskRecord, Stage::Enqueue, Stage::RunnerStart, Stage::RunnerHandoff,
    ]);
    assert!(report.events().iter().any(|e| e["type"] == "task_created" && e["protocol_version"] == 4));
    harness.drive_runners();
    assert_eq!(harness.runner_stages(), vec![
        Stage::RecoverDeadDispatches, Stage::RefreshSiblings, Stage::Claim, Stage::LeaseAcquire, Stage::BasePush, Stage::TaskPrepare,
        Stage::TurnSubmit, Stage::AcceptedFlush, Stage::Follow, Stage::OutcomeRecord, Stage::ResultImport, Stage::UnparkNext, Stage::RunnerExit,
    ]);
    assert!(harness.user_repository_unchanged_except(&[format!("refs/remotes/mac-worker/mini-2/task/{}", harness.task_id())]));
    assert!(harness.transfer_repo_has_no_base_ref());
}

#[test]
fn no_wait_probes_first_and_creates_nothing_when_busy() {
    let harness = TaskHarness::all_busy();
    let err = harness.run(["task", "submit", "--agent", "codex", "--no-wait", "--prompt", "x"]).unwrap_err();
    assert_eq!(err.exit_code(), 75);
    assert!(harness.local_tasks().is_empty() && harness.queue().is_empty());
    assert!(harness.user_repository_unchanged() && !harness.transfer_repo_exists());
}

#[test]
fn handoff_failure_abandons_the_task_and_releases_the_base() {
    let harness = TaskHarness::with_runner_that_never_adopts();
    let err = harness.run(["task", "submit", "--agent", "codex", "--prompt", "x"]).unwrap_err();
    assert_eq!(err.public_code(), "RUNNER_HANDOFF_FAILED");
    assert_eq!(err.exit_code(), 74);
    assert!(harness.queue().is_empty());
    assert_eq!(harness.local_tasks()[0].state(), TaskState::Abandoned);
    assert!(harness.transfer_repo_has_no_base_ref());
}

#[test]
fn waiting_rows_are_owned_by_runners_and_only_mutating_commands_replace_dead_ones() {
    let harness = TaskHarness::inline_runner().all_busy();
    harness.run(["task", "submit", "--agent", "codex", "--prompt", "x"]).unwrap();
    assert_eq!(harness.queue_head().owner(), harness.runner_identity());
    harness.kill_runner();
    harness.run_one_poll(["run", "--", "true"]);                              // a waiting batch run executes the reaper and must skip task_turn rows
    assert_eq!(harness.queue().len(), 2);
    assert!(harness.queue().iter().any(|row| row.kind() == QueueEntryKind::TaskTurn && !row.owner_live()));
    harness.run(["task", "list"]).unwrap();
    assert!(harness.live_runners().is_empty() && harness.list_output_marks_runner_dead());
    harness.run(["task", "reconcile"]).unwrap();
    assert_eq!(harness.live_runners().len(), 1);
    harness.free_all();
    harness.drive_runners();
    assert_eq!(harness.task_status().state(), TaskState::Open);
    assert!(harness.fetched_head().is_some());
}

#[test]
fn runner_cap_parks_excess_rows_and_shares_one_observation_cache() {
    let harness = TaskHarness::inline_runner().with_workers_busy(3);
    for _ in 0..7 { harness.run(["task", "submit", "--agent", "codex", "--prompt", "x"]).unwrap(); }
    assert_eq!(harness.live_runners().len(), 3);
    assert_eq!(harness.parked_rows().len(), 4);
    harness.advance_time(Duration::from_secs(60));
    harness.drive_runners();
    assert!(harness.probe_requests_per_worker() <= 2);                          // single-flight cache, not one probe per runner per poll
    harness.free("mini-1");
    harness.drive_runners();
    assert_eq!(harness.live_runners().len(), 3);                                // a finishing runner started a parked row's runner
}

#[test]
fn no_wait_runner_abandons_when_capacity_vanished_after_the_probe() {
    let harness = TaskHarness::inline_runner().with_workers_idle(1);
    harness.run(["task", "submit", "--agent", "codex", "--no-wait", "--prompt", "x"]).unwrap();
    harness.make_busy("mini-1");                                              // taken between the probe and the claim
    harness.drive_runners();
    let status = harness.task_status();
    assert_eq!(status.state(), TaskState::Abandoned);
    assert_eq!(harness.local_tasks()[0].abandon_code(), Some("CAPACITY_BUSY"));
    assert!(harness.queue().is_empty() && harness.transfer_repo_has_no_base_ref());
}

#[test]
fn submit_requires_agent_capability_and_never_reroutes_a_pin() {
    let harness = TaskHarness::with_workers(&[("mini-1", &["darwin-arm64", "agent:claude"]), ("mini-2", &["darwin-arm64", "agent:codex"])]);
    harness.run(["task", "submit", "--agent", "codex", "--prompt", "x"]).unwrap();
    harness.drive_runners();
    assert_eq!(harness.selected_worker(), "mini-2");
    let err = harness.run(["task", "submit", "--agent", "codex", "--worker", "mini-1", "--prompt", "x"]).unwrap_err();
    assert_eq!(err.public_code(), "CAPABILITY_MISSING");
}

#[test]
fn killed_follower_cannot_keep_a_task_active_locally() {
    let harness = TaskHarness::with_task_active_locally_but_open_remotely();
    let status = harness.run(["task", "status", &harness.task_id()]).unwrap();
    assert_eq!(status.state(), TaskState::Open);
}
```

The harness created in this task provides what these tests use: a fake three-host transport with per-worker busy and idle control, an inline runner executor that records stages and can be paused between stages or killed after one, a controllable clock, per-worker probe request counting, queue and parked-row inspection, runner liveness inspection, and `RepositoryFingerprint` comparisons for the user's repository and the transfer repository.

Also test: `--wait` performs the runner's stages in the foreground and follows logs; a lease race or pre-acceptance failure reverts the queue row through phase 4 reversion and resolves or abandons through the original identity; `release_base` runs after a successful push and after abandonment; `status`/`list` show tasks and turns without prompt bodies unless `--full`; `logs` renders normalized events through the adapter parser and `--raw` streams bytes; `diff --stat` works while `Active`; `result` prints summary, questions, files, branch, and the `worker task fetch` instruction; `fetch` on a closed task succeeds and on a discarded task fails with `TASK_CLOSED`; `close --discard`; `.worker.toml` `[task]` parsing with defaults and rejection of unknown keys, `source = "origin"`, and `push`; `cli_help` snapshots; JSON rows have `protocol_version` and no `prompt`, `session_ref`, or path keys; queue rows for turns carry only the allowed fields; the detached executor starts the runner in its own session with stdio on `runners/<task_id>/<turn_id>.log` owner-only.

- [ ] **Step 2: Run client tests to verify RED**

Run: `cargo test --locked --test turn_runner --test task_command --test cli_help --test project_config -- --nocapture`

Expected: FAIL because the `task` command family and the runner do not exist.

- [ ] **Step 3: Implement the client and the runner**

```rust
pub struct TaskSubmitRequest { pub agent: AgentKind, pub model: Option<String>, pub prompt: String, pub project: PathBuf, pub base: String, pub wip: bool, pub cli_includes: Vec<String>, pub limits: TaskLimits, pub close_policy: ClosePolicy, pub env_profile: Option<String>, pub preference: WorkerPreference, pub wait_for_capacity: bool, pub attached: bool, pub run_id: Option<RunId> }   // preference and wait_for_capacity are persisted in LocalTaskRecord for the runner
pub struct TaskClient<'a> { runner: &'a dyn ProcessRunner, config: &'a Config, paths: &'a PathLayout, client_state: &'a ClientStateStore, executor: &'a dyn RunnerExecutor }
impl<'a> TaskClient<'a> {
    pub fn submit(&self, request: TaskSubmitRequest, stdout: &mut dyn Write, stderr: &mut dyn Write) -> Result<TaskReport, WorkerError>;
    pub fn status(&self, task_id: TaskId) -> Result<TaskReport, WorkerError>;
    pub fn list(&self, filter: TaskListFilter) -> Result<TaskListReport, WorkerError>;
    pub fn logs(&self, task_id: TaskId, turn: Option<u32>, follow: bool, raw: bool, stdout: &mut dyn Write, stderr: &mut dyn Write) -> Result<(), WorkerError>;
    pub fn diff(&self, task_id: TaskId, stat: bool, stdout: &mut dyn Write) -> Result<(), WorkerError>;
    pub fn result(&self, task_id: TaskId) -> Result<TaskResultReport, WorkerError>;
    pub fn fetch(&self, task_id: TaskId) -> Result<FetchReport, WorkerError>;
    pub fn close(&self, task_id: TaskId, discard: bool) -> Result<TaskReport, WorkerError>;
    pub fn reconcile_runners(&self) -> Result<ReconcileReport, WorkerError>;
}
pub trait RunnerExecutor: Send + Sync { fn start(&self, paths: &PathLayout, task_id: TaskId, turn_id: TurnId) -> Result<RunnerIdentity, WorkerError>; }
pub struct DetachedRunnerExecutor; pub struct InlineRunnerExecutor;      // tests drive inline runners explicitly
pub struct TurnRunner<'a> { /* same borrows as TaskClient */ }
impl<'a> TurnRunner<'a> { pub fn run(&self, task_id: TaskId, turn_id: TurnId, follow: Option<&mut dyn Write>) -> Result<TurnOutcomeReport, WorkerError>; }
```

`submit` order: inspect; settings and requirements (`agent:<name>` or `agent:<name>@<profile>` plus project requirements); fleet probe through the shared observation cache; with `--no-wait`, `CAPACITY_BUSY` before anything is created; base capture in the transfer repository (Task 4) with the sensitive-tree check; local task record; the composed prompt written owner-only to `turns/<task_id>/<turn_id>/prompt.md` under the state root; enqueue a `QueueEntryKind::TaskTurn` row with the run reference; if fewer than one runner per configured worker is waiting, start the runner through the executor and wait up to five seconds for `adopt_row` to record it as the row's owner (waiting state), else park the row; a failed handoff removes the row, marks the task `abandoned`, releases the base, and fails with `RUNNER_HANDOFF_FAILED`. The runner: `recover_dead_dispatches` scoped to its own row; claim under phase 4 (per-worker FIFO after Task 8's amendment, head-of-line until then) with backoff from one to thirty seconds and the observation cache refreshed single-flight when older than two seconds, except that a task persisted with `wait_for_capacity: false` whose first claim finds no eligible worker is abandoned with `CAPACITY_BUSY` (row removed, base released, task `abandoned`); lease; `push_base`; `task-prepare`; `task-turn` with the prompt read from the local turn file; flush acceptance into the local record and remove the local prompt file; poll status until terminal; record the outcome; `release_base`; `fetch_result` and `import_result`; record the fetched head; start a runner for the oldest parked row if any; exit. Pre-acceptance failures revert the row and resolve or abandon through the original identity. `reconcile_runners` runs first in `submit`, `batch`, `say`, `cancel`, `close`, `wait`, and `worker task reconcile` only: it refreshes referenced tasks from their workers, re-owns dead-owner `task_turn` rows, re-enqueues `queued` tasks whose row is missing, and starts replacement runners for waiting rows and active turns whose recorded runner is dead, within the per-worker cap; terminal tasks never get a runner. `list`, `status`, `result`, `diff`, and `logs` never call it; they compare the recorded runner identity with the process table read-only and label it `dead`. Phase 4's abandoned-row reaping in `worker run` is amended to skip `task_turn` rows. `--wait` runs `TurnRunner::run` in the foreground with log following and is the row's owner. Add `[task]` to `ProjectSettings`; reject `source = "origin"` and `push` with `TASK_CONFIG_INVALID` naming the later plan. Add hidden `worker runner <task_id> <turn_id>` dispatch in `src/lib.rs`, public `worker task reconcile`, and `JsonEvent` task variants.

- [ ] **Step 4: Run client regressions**

Run: `cargo test --locked --test turn_runner --test task_command --test cli_help --test project_config --test client_state --test run_command --test scheduler_queue -- --nocapture`

Expected: PASS; batch `run` ordering and queue semantics are unchanged.

- [ ] **Step 5: Commit the client and runner**

```bash
git add src/task_client.rs src/turn_runner.rs src/job.rs src/client_state.rs src/run.rs src/transfer.rs src/cli.rs src/lib.rs src/output.rs src/project_config.rs tests/support/task_harness.rs tests/turn_runner.rs tests/task_command.rs tests/cli_help.rs tests/client_state.rs tests/project_config.rs tests/scheduler_queue.rs tests/run_command.rs
git commit -m "feat: submit and collect agent tasks through local runners"
```

---

### Task 8: Per-Worker FIFO, Conversation, Runs, and Cancellation

**Gate:** Start after Task 7 and after phase 4 cancellation is on `main`. This task delivers the phase 4 amendments of spec section 5.1 (`claim_next` per-worker FIFO, run caps, entry kinds in ranking) if phase 4 landed without them; if phase 4 already carries them, only the tests in Step 1 are added.

**Files:**
- Create: `tests/task_conversation.rs`
- Modify: `src/client_state.rs`
- Modify: `src/run.rs`
- Modify: `src/task_client.rs`
- Modify: `src/turn_runner.rs`
- Modify: `src/task.rs`
- Modify: `src/task_store.rs`
- Modify: `src/job_service.rs`
- Modify: `src/supervisor.rs`
- Modify: `src/cli.rs`
- Modify: `src/lib.rs`
- Modify: `src/output.rs`
- Modify: `tests/scheduler_queue.rs`, `tests/task_command.rs`, `tests/cli_help.rs`

**Interfaces:**
- Consumes: Task 7 `TaskClient` and `TurnRunner`, phase 4 `WorkerPreference::Pinned` with wait, phase 4 host cancellation, Task 6 `TurnTerminalHook` and resume launches.
- Produces: `ClientStateStore::claim_next` per-worker FIFO, `TaskClient::{say, cancel, wait, batch}`, `RunRecord` persistence, `BatchFile`, public `worker task say|cancel|wait|batch`, the exit-code table.

- [ ] **Step 1: Write failing scheduling and conversation tests**

```rust
#[test]
fn pinned_head_waiting_for_a_busy_worker_does_not_block_a_younger_first_turn() {
    let store = open_queue();
    store.enqueue(pinned_turn("0000…0001", "mini-2", 10)).unwrap();          // mini-2 busy
    store.enqueue(first_turn("0000…0002", 11)).unwrap();
    let claim = store.claim_next(dispatcher(), &["mini-1".into(), "mini-3".into()], 12).unwrap().unwrap();
    assert_eq!(claim.entry().job_id(), job("0000…0002"));
    assert!(store.claim_next(dispatcher(), &["mini-1".into()], 13).unwrap().is_none());  // still no eligible worker for the pinned head
}

#[test]
fn a_runner_claims_only_its_own_row_and_yields_to_an_older_live_owner() {
    let store = open_queue();
    store.enqueue(first_turn_owned("0000…0001", runner_a(), 10)).unwrap();
    store.enqueue(first_turn_owned("0000…0002", runner_b(), 11)).unwrap();
    assert!(store.claim_next(runner_b(), &["mini-1".into()], 12).unwrap().is_none());          // older live-owned row is eligible for mini-1
    let claim = store.claim_next(runner_a(), &["mini-1".into()], 13).unwrap().unwrap();
    assert_eq!(claim.entry().job_id(), job("0000…0001"));
    store.enqueue(parked_turn("0000…0003", 14)).unwrap();                                       // no owner
    assert!(store.claim_next(runner_b(), &["mini-2".into()], 15).unwrap().map(|c| c.entry().job_id()) == Some(job("0000…0002")));
    assert!(store.claim_next(runner_b(), &["mini-3".into()], 16).unwrap().is_none());          // runner_b never receives the parked row
}

#[test]
fn older_row_always_wins_the_same_worker() {
    let store = open_queue();
    store.enqueue(first_turn("0000…0001", 10)).unwrap();
    store.enqueue(first_turn("0000…0002", 11)).unwrap();
    let claim = store.claim_next(dispatcher(), &["mini-1".into()], 12).unwrap().unwrap();
    assert_eq!(claim.entry().job_id(), job("0000…0001"));
}

#[test]
fn say_starts_a_resumed_turn_pinned_to_the_session_worker_and_waits_if_busy() {
    let harness = TaskHarness::with_open_task_on("mini-2");
    harness.make_busy("mini-2");
    harness.run(["task", "say", &harness.task_id(), "--message", "also drop the legacy endpoint"]).unwrap();
    assert_eq!(harness.queue_head().preference(), &WorkerPreference::Pinned { worker: "mini-2".into() });
    harness.drive_runners();
    assert!(harness.no_dispatch_happened());
    harness.free("mini-2");
    harness.drive_runners();
    assert!(harness.last_turn_material().resume);
    assert!(harness.last_shell_command().contains("'resume'"));
    assert_eq!(harness.task_status().turns().len(), 2);
}

#[test]
fn say_is_rejected_while_active_after_the_limit_and_on_terminal_tasks() {
    let harness = TaskHarness::with_active_task();
    assert_eq!(harness.run(["task", "say", &harness.task_id(), "--message", "x"]).unwrap_err().public_code(), "TASK_BUSY");
    let harness = TaskHarness::with_open_task_after_followups(10);
    assert_eq!(harness.run(["task", "say", &harness.task_id(), "--message", "x"]).unwrap_err().public_code(), "FOLLOWUP_LIMIT");
    let harness = TaskHarness::with_closed_task();
    assert_eq!(harness.run(["task", "say", &harness.task_id(), "--message", "x"]).unwrap_err().public_code(), "TASK_CLOSED");
}

#[test]
fn sibling_runners_cannot_both_take_the_last_run_slot() {
    let harness = TaskHarness::inline_runner().with_workers_idle(3);
    let report = harness.run(["task", "batch", "tasks.toml", "--max-parallel", "1", "--json"]).unwrap();
    harness.pause_runners_between(Stage::RefreshSiblings, Stage::Claim);        // both refreshed zero active siblings
    harness.drive_runners();
    assert_eq!(harness.active_count_for_run(report.run_id()), 1);
    assert_eq!(harness.queue_rows_with_blocking_reason("run_max_parallel").len(), report.task_ids().len() - 1);
}

#[test]
fn cancel_stops_only_the_active_turn_and_leaves_task_open_with_session() {
    let harness = TaskHarness::with_active_task();
    harness.run(["task", "cancel", &harness.task_id()]).unwrap();
    let status = harness.task_status();
    assert_eq!(status.state(), TaskState::Open);
    assert_eq!(status.last_outcome(), Some(&TaskOutcome::Cancelled));
    assert!(status.session_present() && harness.workspace_exists());
}

#[test]
fn batch_run_cap_and_wait_complete_after_the_submitting_shell_exits() {
    let harness = TaskHarness::with_workers_idle(3);
    let report = harness.run(["task", "batch", "tasks.toml", "--max-parallel", "2", "--json"]).unwrap();
    assert_eq!(report.task_ids().len(), 5);
    harness.drop_submitting_process();
    harness.drive_runners();
    assert_eq!(harness.active_count(), 2);
    let wait = harness.run_with_runner_driving(["task", "wait", "--run", report.run_id(), "--timeout", "10m"]).unwrap();
    assert_eq!(wait.exit_code(), 0);
    assert_eq!(harness.task_states().iter().filter(|s| **s == TaskState::Closed).count(), 5);
}
```

Also test: `say --wait` returns the agent's exit code unchanged on failure and `1` for `blocked`; `wait` returns `1` when any task ended failed, blocked, cancelled, timed out, or lost and `70` on `--timeout` without cancelling; the resume preamble references the turn number; a cancelled turn followed by `say` resumes the same `session_ref`; `close_policy = done` closes after a later turn reports `done`; `batch` rejects unknown keys and per-task overrides that fail validation before creating any task; a run-capped head does not block other runs' rows; `wait` reconciles runners on every poll; `list --run` and `RunProgress` counts.

- [ ] **Step 2: Run conversation tests to verify RED**

Run: `cargo test --locked --test task_conversation --test scheduler_queue --test cli_help -- --nocapture`

Expected: FAIL because per-worker FIFO, `say`, `cancel`, `wait`, and `batch` do not exist for tasks.

- [ ] **Step 3: Implement per-worker FIFO, conversation, and runs**

Amend `claim_next(owner, ranked_workers, now)` so that it considers only the row owned by `owner`: for each ranked idle worker not already selected by a live dispatch, the owner's row is claimed when it is eligible for that worker (kind requirements, pin, run cap counted under the lock from local state) and no older uncancelled `Waiting` row with a live owner is eligible for the same worker; otherwise it returns `None`. Rows ineligible for every idle worker never block younger rows for other workers; parked rows have no owner and are neither claimed nor counted as blockers, because unparking is oldest-first. Batch rows keep their phase 4 owner semantics (the CLI process is the owner). Preserve reversion, cancellation, and dead-dispatcher recovery. Then:

```rust
impl<'a> TaskClient<'a> {
    pub fn say(&self, task_id: TaskId, message: String, attached: bool, stdout: &mut dyn Write, stderr: &mut dyn Write) -> Result<TaskReport, WorkerError>;
    pub fn cancel(&self, task_id: TaskId) -> Result<TaskReport, WorkerError>;
    pub fn wait(&self, selector: WaitSelector, timeout: Option<Duration>) -> Result<WaitReport, WorkerError>;
    pub fn batch(&self, file: &Path, run_name: Option<String>, max_parallel: Option<u32>, stdout: &mut dyn Write) -> Result<RunReport, WorkerError>;
}
pub struct BatchFile { defaults: BatchDefaults, tasks: Vec<BatchTask> }
```

`say` refreshes the task from its worker, refuses `Active` (`TASK_BUSY`), terminal states (`TASK_CLOSED`), and the follow-up limit, writes the composed follow-up prompt to the local turn file exactly as `submit` does, builds `TurnMaterial { resume: true, turn_number: n + 1 }`, and submits through Task 7's path with `WorkerPreference::Pinned { worker }` and `wait_for_capacity: true`, starting a runner unless attached; the host `task-turn` for a resumed turn skips base push and prepare, verifies the workspace and branch, requires `session.json`, and uses the adapter's resume launch. `cancel` delegates to phase 4 host cancellation for the active turn ID; that host path invokes `TurnTerminalHook` with `HostCancel` before releasing the lease, so the task is `Open` with `Cancelled` by the time `cancel` returns, and this task adds that call to the phase 4 cancellation code in `job_service.rs` and `supervisor.rs`. `wait` polls with bounded intervals, reconciles runners on each poll, and applies the exit table. `batch` parses the TOML, validates every task, creates the run and tasks, and starts runners up to the per-worker cap, parking the rest. Runners enforce `max_parallel` inside `claim_next` under the local queue lock from local state only: sibling rows in `Dispatching` plus sibling tasks recorded `Active` locally, with acceptance recorded under the same lock; the pre-lock refresh only updates local records and never runs while the lock is held. A claim consumes a slot until reverted or terminal, and a capped row records `run_max_parallel` as its blocking reason.

- [ ] **Step 4: Run conversation regressions**

Run: `cargo test --locked --test task_conversation --test scheduler_queue --test task_command --test turn_runner --test cli_help --test job_queries -- --nocapture`

Expected: PASS.

- [ ] **Step 5: Commit conversation and runs**

```bash
git add src/client_state.rs src/run.rs src/task_client.rs src/turn_runner.rs src/task.rs src/task_store.rs src/job_service.rs src/supervisor.rs src/cli.rs src/lib.rs src/output.rs tests/task_conversation.rs tests/scheduler_queue.rs tests/task_command.rs tests/cli_help.rs
git commit -m "feat: converse with tasks between turns and batch runs"
```

---

### Task 9: Agent, Profile, and Identity Probe Facts

**Gate:** Start after Task 7 (protocol 4 exists since Task 3; this task adds fields under it).

**Files:**
- Modify: `src/protocol.rs`
- Modify: `src/probe.rs`
- Modify: `src/scheduler_adapter.rs`
- Modify: `src/transport.rs`
- Modify: `src/transfer.rs`
- Modify: `src/install.rs`
- Modify: `src/turn_runner.rs`
- Modify: `src/cli.rs`
- Modify: `src/lib.rs`
- Create: `tests/agent_probe.rs`
- Modify: `tests/workers_command.rs`, `tests/doctor_command.rs`, `tests/setup_command.rs`, `tests/scheduler_adapter.rs`

**Interfaces:**
- Consumes: `ProbeResponse`, `WorkerHealth`, `SchedulerProbeAdapter`, Task 1 adapter binaries and env names, Task 6 `EnvProfile`.
- Produces: `AgentProbe`, `AgentAuth`, `ProfileProbe`, `AgentFacts` with `collected_at_millis`, `FACTS_TTL`, `ProbeResponse::{agent_facts}` carrying the cached facts and their age, `HostOperation::RefreshFacts`, hidden `host refresh-facts`, `worker workers --refresh`, runner-triggered refresh when stale, `agent:<name>` and `agent:<name>@<profile>` capabilities, `worker workers` agent and profile columns.

- [ ] **Step 1: Write failing probe tests**

```rust
#[test]
fn probe_reports_agents_with_auth_per_profile() {
    let response = probe_with_fake_commands(&[
        ("zsh", "-lc command -v codex", "/opt/homebrew/bin/codex"), ("codex", "--version", "codex-cli 0.152.1"), ("codex", "login status", "Logged in using ChatGPT"),
        ("zsh", "-lc command -v claude", "/Users/w/.local/bin/claude"), ("claude", "--version", "2.1.252 (Claude Code)"),
        ("claude", "auth status", r#"{"loggedIn":false}"#), ("claude@agents", "auth status", r#"{"loggedIn":true}"#),
    ], &[profile("agents", 0o600)]);
    assert_eq!(response.agents, vec![
        AgentProbe { name: "codex".into(), version: Some("0.152.1".into()), auth: AgentAuth::Authenticated, auth_by_profile: vec![] },
        AgentProbe { name: "claude".into(), version: Some("2.1.252".into()), auth: AgentAuth::Unauthenticated, auth_by_profile: vec![("agents".into(), AgentAuth::Authenticated)] },
    ]);
    assert_eq!(response.env_profiles, vec![ProfileProbe { name: "agents".into(), secure: true }]);
}

#[test]
fn keychain_locked_timeout_or_insecure_profile_is_unknown_never_authenticated() {
    let response = probe_with_fake_commands(&[("claude", "auth status", "Error: Your macOS login keychain is locked.")], &[profile("agents", 0o644)]);
    assert_eq!(response.agents[0].auth, AgentAuth::Unknown);
    assert_eq!(response.env_profiles[0].secure, false);
    assert!(response.agents[0].auth_by_profile.is_empty());
}

#[test]
fn capabilities_are_keyed_by_profile() {
    let facts = SchedulerProbeAdapter::observations(&config(), &[health_with_agents(&[("codex", AgentAuth::Authenticated, &[]), ("claude", AgentAuth::Unauthenticated, &[("agents", AgentAuth::Authenticated)])])]).unwrap();
    let caps = facts[0].capabilities();
    assert!(caps.contains(&"agent:codex".to_string()) && caps.contains(&"agent:claude@agents".to_string()));
    assert!(!caps.iter().any(|c| c == "agent:claude"));
}
```

Also test: agent facts are collected only by `host refresh-facts`, never by `host probe`, which reads the cache and reports its age without launching any agent binary; facts older than `FACTS_TTL` (fifteen minutes) are reported but project to no capability; `worker setup` and `worker workers --refresh` invoke the refresh; a runner invokes it before claiming when the cached facts are stale and never otherwise; each agent check has a two-second deadline and a 4 KiB output bound; a missing binary yields no entry; profile values never appear in probe output, records, or errors; insecure profiles are never applied to a check; `git_identity` is `true` only when both `user.name` and `user.email` resolve through the login shell; `worker workers` human and JSON output show agents, profiles, and fact age; every protocol fixture derives its version from the constant and no version bump occurs in this task.

- [ ] **Step 2: Run probe tests to verify RED**

Run: `cargo test --locked --test agent_probe --test workers_command --test scheduler_adapter -- --nocapture`

Expected: FAIL because the probe facts do not exist.

- [ ] **Step 3: Implement the probe facts**

```rust
pub enum AgentAuth { Authenticated, Unauthenticated, Unknown }
pub struct AgentProbe { pub name: String, pub version: Option<String>, pub auth: AgentAuth, pub auth_by_profile: Vec<(String, AgentAuth)> }
pub struct ProfileProbe { pub name: String, pub secure: bool }
pub struct ProbeResponse { /* existing */ pub agents: Vec<AgentProbe>, pub env_profiles: Vec<ProfileProbe>, pub git_identity: bool }
```

In `probe.rs` add `collect_agent_facts`, invoked only by the hidden `host refresh-facts` operation, which writes `facts.json` under the host data root atomically with `collected_at_millis`; `host probe` reads that file, if present, and reports it with its age. `collect_agent_facts` resolves each adapter binary once through `zsh -lc 'command -v <bin>'`, runs the version and authentication commands through `ProcessRunner` with the bounds above, once plainly and once per secure env profile with that profile's entries added to the environment, and classifies: Codex `login status` containing `Logged in` is `Authenticated`; Claude `auth status` JSON `loggedIn: true` is `Authenticated`, `false` is `Unauthenticated`; any error, timeout, or keychain message is `Unknown`. `SchedulerProbeAdapter` appends `agent:<name>` for plain `Authenticated` and `agent:<name>@<profile>` per authenticated profile, only while the facts are younger than `FACTS_TTL`. `worker setup` runs `refresh-facts` after installing the helper; `worker workers --refresh` runs it before probing; a runner runs it before claiming when the cached facts are stale. Update `worker workers` output with agents, profiles, identity, and fact age.

- [ ] **Step 4: Run probe regressions**

Run: `cargo test --locked --test agent_probe --test workers_command --test doctor_command --test setup_command --test scheduler_adapter --test task_command -- --nocapture`

Expected: PASS.

- [ ] **Step 5: Commit the probe**

```bash
git add src/protocol.rs src/probe.rs src/scheduler_adapter.rs src/transport.rs src/transfer.rs src/install.rs src/turn_runner.rs src/cli.rs src/lib.rs tests/agent_probe.rs tests/workers_command.rs tests/doctor_command.rs tests/setup_command.rs tests/scheduler_adapter.rs
git commit -m "feat: cache agent facts as scheduler capabilities"
```

---

### Task 10: Orchestrator Skill, Documentation, Three-Mac Acceptance, and Final Gate

**Gate:** Start after Tasks 1 to 9 pass their focused suites.

**Files:**
- Create: `.claude/skills/pool-dispatch/SKILL.md`
- Create: `docs/phase-five-validation.md`
- Modify: `README.md`
- Modify: `tests/cli_help.rs`

**Interfaces:**
- Consumes: the complete `worker task` command family with `--json`.
- Produces: the orchestrator loop, Phase 5 usage documentation, a sanitized acceptance record, and the final gate result.

- [ ] **Step 1: Write the skill**

`.claude/skills/pool-dispatch/SKILL.md` documents exactly: submit each task with `worker task submit --agent <name> --prompt-file <file> --json` and keep the `task_id`; poll with `worker task list --run <id> --json` or block with `worker task wait --run <id>`; on `needs_input`, answer with `worker task say <id> --message-file <file> --wait`; on `done`, run `worker task fetch <id>` and report the remote-tracking ref; on `blocked` or a failed turn, read `worker task result <id> --json` and `worker task logs <id>`, then either `say` with guidance or `close --discard`; close finished tasks. It states that the skill must never merge, check out, or push, and must never read env profiles.

- [ ] **Step 2: Document Phase 5**

Add a README section with the batch file example, the `[task]` settings, the env-profile setup instruction for Claude Code, the requirement to rerun `worker setup` on every worker for the layout migration and the first agent-facts collection, `worker workers --refresh` and `worker task reconcile`, the explicit statement that `source = origin`, `publish = push`, Cursor, OpenCode, retention, and the dashboard tasks view are later phases, and the security note that agent turns run with the worker account's full access. Update `tests/cli_help.rs` snapshots.

- [ ] **Step 3: Run the full local gate**

Run:

```bash
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked --release
git diff --check
```

Expected: all pass with zero warnings.

- [ ] **Step 4: Run three-Mac live acceptance**

Install the release helper on all three workers with `worker setup` (which performs the layout migration), verify `worker workers` shows protocol 4, `agent:codex` on every worker, and `agent:claude@agents` on the workers with an env profile, then execute spec section 20.2 items 1 to 5 and 7 to 10 (item 6 belongs to the later plan) with an isolated clone and isolated XDG roots, including the submitting shell exiting before completion. Record only sanitized evidence in `docs/phase-five-validation.md`: shortened identifiers, sanitized command categories, terminal states, exit results, durations, before/after fingerprints of mac-worker-owned namespaces and of the isolated clone, and the helper/client revision match.

- [ ] **Step 5: Commit documentation and evidence**

```bash
git add .claude/skills/pool-dispatch/SKILL.md docs/phase-five-validation.md README.md tests/cli_help.rs
git commit -m "docs: describe agent task execution core"
```

---

## Delivery Boundary

After Task 10 the following work independently, with runners carrying every turn after the shell returns:

```text
worker task submit --agent codex --prompt-file tasks/fix-login.md
worker task submit --agent claude --model opus --wip --prompt "…" --wait
worker task batch tasks/sprint.toml --max-parallel 3
worker task list --run <run_id> --json
worker task logs -f <task_id>
worker task diff <task_id> --stat
worker task say <task_id> --message "…" --wait
worker task cancel <task_id>
worker task result <task_id>
worker task fetch <task_id>
worker task close <task_id> [--discard]
worker task wait --run <run_id> --timeout 2h
```

Explicitly outside this plan and rejected at preflight with a message naming the later plan: `source = origin`, `publish = push`, `--publish-branch` behaviour, `--agent cursor`, `--agent opencode`, retention through `worker gc`, and the dashboard tasks view.
