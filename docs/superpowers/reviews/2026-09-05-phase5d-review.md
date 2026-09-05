# Phase 5d Tasks 1-3 adversarial review - 2026-09-05

## Scope and outcome

Reviewed Task 1 (`262f06b`), Task 2 (`dcab021`), and Task 3 (`720f0c7`)
against the Phase 5 plan's Global Constraints and Tasks 1-3, including the
specified sections on authentication facts, origin transfer, publication,
session binding, output, and recovery. The branch was finally rebased onto
`main` at `cdc795c`, which includes the Task 4 worker-GC and subsequent
blocking-reason and runner-log follow-ups.

The review found seven Major findings. Five are fixed in six `fix:` commits;
two remain deferred because the brief protects the required Task 4 client
state and local-origin integration. No Critical or Minor findings were
opened. The deferred items block live acceptance of local-origin push and the
post-create failure path; they do not invalidate the fixed Cursor/OpenCode
adapter or origin-transport checks.

## Findings

1. **Major - fixed** - `src/agent/cursor.rs:96-104` (pre-fix line 103) -
   authentication probe classification

   Quote (pre-fix, `720f0c7:src/agent/cursor.rs:103`):
   `} else if text.contains("authenticated") || text.contains("logged in") {`

   Scenario: the adapter-owned probe classified substrings rather than one
   unambiguous fact. `authenticated: false`, a diagnostic containing
   "logged in", or contradictory stdout/stderr could be projected as
   authenticated. Scheduler admission could then treat an agent as eligible
   for work requiring the corresponding authenticated capability.

   Fix: require exactly one non-empty status line and accept only the
   documented true/false forms; ambiguous or mixed output is `Unknown`.
   Regression coverage is in `agent_capabilities` and `agent_facts`. Commit
   `a8bc72f`.

2. **Major - fixed** - `src/agent/opencode.rs:103-121` (pre-fix line 120) -
   authentication probe classification

   Quote (pre-fix, `720f0c7:src/agent/opencode.rs:120`):
   `Value::Object(_) => AuthProbeResult::Authenticated,`

   Scenario: any non-empty JSON object, including an error, status, version,
   or malformed provider report, was treated as authenticated. This could
   turn a failed or unrelated OpenCode response into a capability admission.

   Fix: classify only explicit provider-bearing arrays/objects, keep empty
   provider lists unauthenticated, and return `Unknown` for unrelated or
   malformed structures. Commits `a8bc72f` and `a213965`.

3. **Major - fixed** - `src/agent/mod.rs:435-468` (pre-fix line 456) -
   prebind session-reference parsing

   Quote (pre-fix, `720f0c7:src/agent/mod.rs:456`):
   `validate_prebind_id(line)`

   Scenario: after failing to identify a session in JSON, the parser accepted
   the first non-empty line. A warning before a real ID, embedded JSON in log
   text, or a complete JSON response such as `{"status":"ok"}` could become
   the durable session reference. A later resume would then target an invalid
   or attacker-controlled conversation instead of failing closed.

   Fix: reject multiple non-empty lines, embedded or malformed JSON, and
   identifier-less JSON; accept only a complete recognized ID or one plain
   line, with whitespace/control/length validation. Commits `ca20adc` and
   `ccbf80e`.

4. **Major - fixed** - `src/turn.rs:1029-1051` (pre-fix line 1038) -
   prebind agent/session identity binding

   Quote (pre-fix, `720f0c7:src/turn.rs:1038`):
   `Ok(Some(binding)) => return Ok(TaskSessionResponse::new(binding)),`

   Scenario: a prebind request for one agent returned an existing binding for
   another agent without checking identity. The caller could believe an
   OpenCode or Cursor prebind succeeded, while the subsequent resume either
   used the wrong adapter/session or failed only later during task
   preparation.

   Fix: parse the requested agent before lookup, return the existing binding
   only when its agent matches, and otherwise return
   `TASK_SESSION_CONFLICT`. Commit `a9672c7`, with a regression test.

5. **Major - fixed** - `src/git_transport.rs:104-189` (pre-fix origin request
   at line 475) - origin SSH transport policy

   Quote (pre-fix, `720f0c7:src/git_transport.rs:475`):
   `environment: vec![`

   Scenario: origin preflight used `git ls-remote`, and origin fetch/push used
   the generic Git request without an explicit SSH command. Ambient OpenSSH
   configuration or agent forwarding could therefore alter credentials,
   destination access, or timeout behavior for an origin operation.

   Fix: pin `/usr/bin/ssh` with batch mode, bounded connect timeout, agent
   forwarding disabled, and forwarding cleared for preflight, fetch, and
   push. Tests assert the exact environment and operation. Commit `e806bb8`.

6. **Major - deferred** - `src/task_client.rs:715-730` - post-create local
   state rollback

   Quote: `let _ = transfer.release_base(self.runner, task_id);`

   Scenario: after `create_task` succeeds, a failure writing the first turn
   prompt returns immediately after releasing the transfer base. The local
   task record remains without its required prompt, and a run publish-branch
   reservation (when present) is not released. The same class of stuck state
   can occur when loading the run reference fails after the prompt write;
   reconciliation cannot enqueue a task that has no readable turn prompt.

   Deferred plan: make the post-create sequence transactional or add a
   compensating rollback covering the task record, prompt, run-branch
   reservation, queue row if created, and transfer base on every failure after
   creation. Add injected failures at prompt-write and run-reference stages
   and prove no orphaned task/reservation remains. This requires the protected
   `src/task_client.rs` Task 4 file.

7. **Major - deferred** - `src/job_service.rs:3125-3147`,
   `src/turn_runner.rs:1284-1298` - local push origin target is not pinned

   Quote: `(TaskSource::Local { .. }, true, Some(_)) => Ok(()),`

   Scenario: a local task with push publication stores only `TaskSource::Local`.
   The client re-reads the repository origin for each turn, while the host
   accepts any supplied origin for a local push. If the project origin changes
   after submit, scheduler admission and the initial capability check refer to
   one host but publication can be directed to another normalized origin.

   Deferred plan: persist the full normalized origin target (or an equivalent
   immutable target identity) in the local push task metadata; use that value
   for requirements, every turn request, retry, and push; and have host
   validation require exact equality before `push_origin`. Add an origin
   mutation/request-mismatch regression test. The integration crosses the
   protected `src/task_client.rs` Task 4 file, so it was not changed here.

## Seam checks

- Origin-source submission preflights one exact advertised base OID; origin
  preparation fetches that OID before task-directory creation and verifies it
  locally. Mirror publication uses the task-derived ref and preserves the
  recoverable `PUBLISH_FAILED`/Open-task path.
- Run publish-branch reservation, mandatory fetch/publish capabilities, exact
  base checks, and mirror/ref overwrite protections passed the focused
  publication, Git, task-materialization, scheduler, and task-turn tests.
- Cursor prebind plus resume, OpenCode first-event binding plus resume, and
  `SESSION_UNBOUND` behavior are covered; the parser and identity fixes close
  the ambiguous cases above.
- Process requests do not expose prompt/profile values through argv or debug
  output; profile values are child-environment-only, and adapter output is
  bounded. The earlier review's title-derived prompt disclosure and
  structured-result bound deferrals remain prior findings and are not counted
  again here.
- No live worker or Mac mini was contacted, per the brief. This review did
  not claim real-agent authentication or remote-origin connectivity.

## Verification

Per-fix gates passed for every fix commit: `cargo fmt --check`, the touched
test suites, `cargo clippy --locked --all-targets -- -D warnings`, and
`git diff --check`.

The final post-rebase serialized gate passed:

- `cargo test --locked --all-targets -- --test-threads=1`
- `cargo fmt --check`
- `cargo clippy --locked --all-targets -- -D warnings`
- `git diff --check`

An earlier default-parallel all-target attempt was blocked by restricted
loopback binding in `dashboard_command`; the dashboard suite passed under the
required host-permission rerun. Two unconfigured-parallel attempts each hit a
different cleanup timeout; both exact tests passed alone. The final
post-rebase serialized all-target run passed every target, including the
dashboard, Task 4, and main follow-up suites.
