# Herdr reporter implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Show every pool turn in the operator's herdr sidebar with live state and a rendered log pane, notify the MacBook's herdr when a turn ends, and make herdr availability a worker fact that `doctor`, `setup`, and `workers` report.

**Architecture:** A synchronous Unix-socket client for herdr's JSON-lines protocol inside the `worker` binary. On the worker, a stateless reporter driven from three existing choke points: the supervisor after the durable `Running` handoff, the turn terminal hook, and the task close paths. A hidden read-only host command renders a turn's log inside the herdr pane. On the laptop, a notifier at the runner's single terminal convergence point. A `herdr` fact collected by `refresh-facts` under the existing TTL.

**Tech Stack:** Rust 2024, `std::os::unix::net::UnixStream`, `serde_json`, existing `ProcessRunner`, `RedactionBoundary`, `TaskStore`, `Supervisor`, `TurnRunner`, `AgentFacts`, `DoctorService`, `Installer`, and `CommandOutput`.

**Spec:** `docs/superpowers/specs/2026-09-08-herdr-reporter-design.md`

## Global Constraints

- The reporter and the notifier are best effort: no error, timeout, or absent socket may change a turn's outcome, exit code, status bytes other than the new `herdr` object, lease handling, or a runner's exit. Every socket call has a connect deadline of 500 ms and a response deadline of 3 s; the per-event budgets are 10 s at turn start, 5 s at terminal, close, and gc, and 2 s for a notification.
- Nothing that crosses the socket may contain a prompt, an environment or profile value, a model string, or a filesystem path. `cwd` is always the account home, `follow-turn` argv carries identifiers only, and messages come from the redaction boundary.
- The supervisor's process-group proof, descriptor ceiling sweep, `O_CLOEXEC` discipline, closed turn-directory layout, and stdout accounting are untouched. Reporter calls run only after `drop(guard)` and inside the terminal hook, never while the supervisor guard is held before the Running handoff.
- One protocol bump, `PROTOCOL_VERSION` 5 to 6, in Task 2 and nowhere else; `SUPERVISION_VERSION` stays at 3. Every request struct keeps `deny_unknown_fields`.
- Reuse the existing shapes: `SystemProcessRunner` for the `herdr --version` probe, `RedactionBoundary` for every message, `SetupWarning` for the setup line, `render_worker_health_with_labels` for the per-worker line, and the `task logs` renderer for the pane. Do not add a daemon, a config file on the worker, or a state file for the reporter.
- Do not touch `src/agent/*`, the scheduler, the dashboard UI under `ui/`, or the v1 batch job path except where a shared renderer moves.

### Task 1: Herdr socket client and fake server

Files: create `src/herdr.rs`, `tests/herdr_client.rs`, `tests/support/fake_herdr.rs`; modify `src/lib.rs`.

- [x] Add `tests/support/fake_herdr.rs`: a `UnixListener` at `<home>/.config/herdr/herdr.sock` in a temporary home, a queue of canned JSON responses keyed by method, an optional per-method stall, and a recording of every request line. Expose `requests()` so later suites can assert payload contents.
- [x] Add failing tests in `tests/herdr_client.rs`: request framing `{"id","method","params"}` plus newline; the response with the matching id is returned and a mismatched id is an error; `error` bodies become `HerdrError::Server { code, message }`; an absent socket returns `HerdrError::Absent` within the connect deadline; a stalling server returns `HerdrError::Timeout` within the response deadline; the descriptor is `O_CLOEXEC` while open and closed after the call.
- [x] Implement `src/herdr.rs`: `HerdrSocket::default_for_home(home)` and `from_env_or_home(env, home)` (honouring `HERDR_SOCKET_PATH`), `HerdrClient::request(method, params) -> Result<Value, HerdrError>`, typed wrappers for `ping`, `workspace_list`, `workspace_create`, `tab_list`, `tab_create`, `tab_close`, `pane_process_info`, `pane_send_input`, `pane_report_agent`, `pane_report_metadata`, `pane_release_agent`, and `notification_show`, ids `mac-worker:<millis>:<counter>`, `seq` as nanoseconds, a hand-written `Debug` that omits params.
- [x] Keep the relevant subset of `herdr api schema --json` (protocol 22) under `tests/fixtures/herdr/schema-subset.json` and add a test that every typed wrapper's params validate against it.
- [x] Run `cargo test --locked --offline --test herdr_client` and fix regressions.

### Task 2: Configuration, wire fields, and the protocol bump

Files: modify `src/config.rs`, `config.example.toml`, `src/turn.rs`, `src/task.rs`, `src/task_store.rs`, `src/agent_facts.rs`, `src/protocol.rs`, `docs/usage.md`; tests `tests/project_config.rs`, `tests/init_command.rs`, `tests/task_model.rs`, `tests/task_turn.rs`, `tests/agent_facts.rs`, `tests/job_protocol.rs`.

- [ ] Add failing tests: `WorkerEntry.herdr` parses and defaults to `false`; `[notifications] herdr` parses and defaults to `true`; the example configuration parses; an unknown key in either table is still rejected; `worker init` round-trips a file that carries both keys.
- [ ] Add failing tests: `TurnSection` serializes `herdr_reporter` only when true and its absence keeps canonical bytes of an existing fixture identical; the turn summary's optional `herdr` object round-trips `{state, pane_id}` and is absent for old records; `AgentFacts.herdr` round-trips every state and is `None` for old `facts.json`; `PROTOCOL_VERSION` is 6 and a protocol 5 probe is refused with `PROTOCOL_MISMATCH`.
- [ ] Implement the fields with `#[serde(default, skip_serializing_if = ...)]`, the `HerdrFactState` and `HerdrTurnState` enums with snake_case tags, and bump `PROTOCOL_VERSION` to 6; update `ProbeResponse::fixture()` and every test fixture that pins the version.
- [ ] Update `config.example.toml` with both keys and one comment each, and add a short **Configuration** note to `docs/usage.md` naming them; `worker init` keeps writing neither, since both defaults hold.
- [ ] Run `cargo test --locked --offline --test project_config --test init_command --test task_model --test task_turn --test agent_facts --test job_protocol --test cli_help` and fix regressions.

### Task 3: Worker reporter, follow-turn, and the hooks

Files: create `src/herdr_reporter.rs`, `src/turn_log.rs`, `tests/herdr_reporter.rs`, `tests/follow_turn.rs`; modify `src/supervisor.rs`, `src/turn.rs`, `src/task_store.rs`, `src/gc.rs`, `src/cli.rs`, `src/lib.rs`, `src/task_client.rs`; tests `tests/supervisor.rs`, `tests/task_turn.rs`, `tests/task_gc.rs`, `tests/cli_help.rs`.

- [ ] Move the `task logs` event renderer from `src/task_client.rs` into `src/turn_log.rs` with no behaviour change, and add a test that `task logs` output for the recorded Codex, Cursor, and OpenCode fixtures is byte-identical before and after the move.
- [ ] Add failing tests in `tests/herdr_reporter.rs` against the fake server: `start` finds an existing `mac-worker` workspace and never creates a second; `start` closes every tab whose label starts with `task <id12>` and no other tab; `start` creates `task <id12> · turn <n>` with `focus: false` and `cwd` equal to the account home; `start` polls `pane.process_info` until the foreground is only the shell, then sends exactly `exec ~/.local/bin/worker host follow-turn <project_id> <worktree_id> <job_id>` plus Enter, and gives up after 5 s without failing; `start` sends `report_agent` with `source = "mac-worker"`, the adapter's herdr kind, `working`, and the title, then `report_metadata` with title, `display_agent`, state labels, and the tokens `task`, `turn`, `mw_title`, `mw_agent`, `mw_outcome`; `terminal` maps every `TaskOutcome` to the state and message of spec section 7 and re-creates a missing tab first; `close` releases and sweeps; `sweep_orphans` closes only tabs whose task directory is gone; every budget holds against a stalling server; every failure returns `Ok(HerdrTurnState::Unavailable)` and logs one line.
- [ ] Add a privacy test over the fake server's recordings: no prompt bytes, no environment or profile value, no model string, no `/Users/`, `/home/`, or `~` in any request.
- [ ] Implement `src/herdr_reporter.rs` with `HerdrReporter::start`, `terminal`, `close`, and `sweep_orphans`, reading the title through `TaskStore::load_meta`, taking the redacted summary and questions from the finished task status, and writing diagnostics to the turn's `supervisor.log`.
- [ ] Hook the supervisor: call `start` immediately after `drop(guard)` in `run_turn_after_payload` when `section.herdr_reporter` is true, and store the returned `herdr` object in the turn summary; call `terminal` from `TurnTerminalHook::invoke` after `finish_turn` for every `TerminalPath`; call `close` from `TaskStore::close_locked` and `close_for_retention` after the workspace removal; call `sweep_orphans` from `HostGc::run` under `--apply`.
- [ ] Add failing tests in `tests/supervisor.rs` and `tests/task_turn.rs`: a turn with `herdr_reporter = true` and no socket produces a status identical to the reference except `herdr: {state: "unavailable"}` and one `supervisor.log` line; host cancel and lost reconciliation reach `terminal`; the agent's process group membership, the pump's stdout accounting, and the terminal log length validation are unchanged.
- [ ] Add the hidden `HostCommand::FollowTurn { project_id, worktree_id, job_id }`: validate identifiers like the other host commands, open the job directory read-only, refuse a batch job and malformed ids (`64`, `70`), write the OSC 0 title `task <id12> · <title>`, follow `stdout.log` and `stderr.log` with the offset reads `log-chunk` uses, render through `turn_log`, print the outcome line at terminal, then block until `SIGTERM` or `SIGHUP`.
- [ ] Add failing tests in `tests/follow_turn.rs` for the rendering parity, the outcome line, the log cap and tail path, the signal exit, the refusals, and that no file in the job directory is opened for writing; extend `tests/cli_help.rs` so the command is hidden from `--help` and present in the grammar test.
- [ ] Run `cargo test --locked --offline --test herdr_reporter --test follow_turn --test supervisor --test task_turn --test task_gc --test cli_help --test task_command` and fix regressions.

### Task 4: Laptop notifier

Files: modify `src/turn_runner.rs`, `src/lib.rs`; create `src/herdr_notify.rs`; tests `tests/turn_runner.rs`, `tests/task_conversation.rs`.

- [ ] Add failing tests: a detached runner and an inline `submit --wait` runner each send exactly one `notification.show` at terminal with title `task <id12>: <outcome>`, the task title and the redacted summary or first question as body, and the sound `done` for `done`, `request` for `needs_input` and `blocked`, `none` otherwise; `finish_publication_failure` notifies with `failed`; `[notifications] herdr = false` sends none; an absent socket sends none and adds less than 2 s; `HERDR_SOCKET_PATH` wins over the default path; the notification body contains no path or prompt.
- [ ] Implement `src/herdr_notify.rs` with `notify_turn_finished(config, env, home, record, outcome)` over the Task 1 client and call it in `TurnRunner::run` after `execute` returns, for both `Ok` and the recorded failure, after the durable record is written and before the runner exits.
- [ ] Run `cargo test --locked --offline --test turn_runner --test task_conversation --test task_command` and fix regressions.

### Task 5: Herdr facts in refresh-facts, doctor, setup, and workers

Files: modify `src/agent_facts.rs`, `src/probe.rs`, `src/doctor.rs`, `src/install.rs`, `src/output.rs`, `src/protocol.rs`; tests `tests/agent_facts.rs`, `tests/agent_probe.rs`, `tests/doctor_command.rs`, `tests/setup_command.rs`, `tests/workers_command.rs`, `tests/dashboard_source.rs`.

- [ ] Add failing tests: `collect_agent_facts` resolves `herdr` on the controlled host paths, records `--version` through `SystemProcessRunner` under a 5 s deadline, checks the default socket, and pings it under the client deadlines, producing each of `available`, `not_installed`, `no_socket`, and `no_response`; the probe hot path only reads the cached fact.
- [ ] Add failing rendering tests: `worker workers`, `worker doctor`, and `worker setup` print the `herdr:` line of spec section 5.3 for every state in text and carry the fact in JSON; stale or missing facts print `unknown`; `doctor` emits `warning [HERDR_UNAVAILABLE]` only for a `herdr = true` worker whose fact is not `available`, and never a blocker; `setup` carries the same as `SetupWarningCode::HerdrUnavailable` while still reporting `installed`; `dashboard_source` passes the fact through `project_agent_facts` unchanged.
- [ ] Implement the collection, the `HERDR_UNAVAILABLE` issue in `worker_issues`, the setup warning after the verification probe in `Installer::install`, and the line in `render_worker_health_with_labels`.
- [ ] Run `cargo test --locked --offline --test agent_facts --test agent_probe --test doctor_command --test setup_command --test workers_command --test dashboard_source` and fix regressions.

### Task 6: Documentation and live acceptance

Files: modify `README.md`, `docs/usage.md`, `docs/setup-macos-worker.md`, `docs/superpowers/specs/2026-09-03-agent-task-pool-design.md`; create `docs/herdr-reporter-validation.md`.

- [ ] `docs/usage.md`: a short **Herdr** subsection under Dashboard describing the sidebar rows, the notifications, the two config keys, and the `doctor` line. README: the "More documentation" list gains the validation record; nothing else in the README changes.
- [ ] Setup guide (`docs/setup-macos-worker.md` as rewritten in `d162379`): an optional section after "4. Install and log in to one agent" on enabling herdr on a worker (`herdr` running as the worker account, `herdr status server`), the `herdr = true` key, rerunning `worker setup`, and an optional note on adding `$mw_title` or `$task` tokens to a custom sidebar row.
- [ ] v2 spec: confirm sections 22 and 23 carry the cross-reference to the herdr reporter design.
- [ ] Run the live acceptance of spec section 13.2 on mac1 and record sanitized evidence in `docs/herdr-reporter-validation.md`, including the `task status --json` excerpt with the `herdr` object, the `doctor` and `setup` lines, and the privacy check over the fake-server recordings.
- [ ] Run `cargo fmt --all --check`, `cargo test --locked --offline --all-targets`, and `cargo clippy --locked --offline --all-targets -- -D warnings`.

## Gate

The plan is done when all six tasks are checked, the full gate is green, `worker setup` has been rerun on the three workers at protocol 6, and `docs/herdr-reporter-validation.md` records items 1 to 8 of spec section 13.2 with sanitized evidence.
