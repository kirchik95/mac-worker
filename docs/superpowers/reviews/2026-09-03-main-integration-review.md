# Main integration review - 2026-09-03

## Scope and outcome

Reviewed the dashboard, agent-adapter, task-model, and transfer-repository work
integrated on `main` as of the review baseline, together with the corresponding
specifications, plans, tests, CLI/library wiring, and Cargo dependency graph.
The dashboard is loopback-only and read-only; its client makes only relative
same-origin requests and renders server data as text. The current dependency
versions satisfy the dashboard plan's Axum/Tokio requirements.

This review found 16 issues: 11 were fixed in this review branch and 5 are
deferred with concrete next steps. There are no unresolved blockers.

## Findings

1. **Major - fixed** - `src/task.rs:836-952` (pre-fix) - `alternates_target: PathBuf`

   Scenario: the pre-review task record also serialized the transfer alternates
   target. That is a machine-local object-directory path, so publishing a task
   record could reveal local filesystem layout.

   Fix: removed that record field and its construction argument, and added a
   serialization regression test. Commit `aa7b1ae`.

2. **Major - fixed** - `src/task.rs:529-541` - `publish fetch is required for every task`

   Scenario: the task model accepted an empty publish list even though fetch is
   the always-on task-record publication mode. A caller could silently create a
   task that never becomes visible to its owner.

   Fix: require `fetch` in the validated list and cover the rejected empty-list
   case. Commit `4054445`.

3. **Major - fixed** - `src/agent/mod.rs:380-392` - `#[serde(deny_unknown_fields)]`

   Scenario: structured agent results passed through an untyped JSON value,
   allowing unknown fields and malformed status payloads to be accepted despite
   the canonical result schema.

   Fix: deserialize a typed, unknown-field-denying wire format before creating
   `StructuredResult`; tests now reject malformed Codex and Claude payloads.
   Commit `ec24e5e`.

4. **Major - fixed** - `src/transfer_repo.rs:318-320` - `is_safe_worker_ref_component(worker)`

   Scenario: an unvalidated worker string was incorporated into a result-ref
   name. Separator-like values could create unintended ref hierarchy or bypass
   expected worker naming constraints.

   Fix: validate the component, reject dot/lock/traversal forms, and test a
   nested-ref attempt. Commit `b4525fc`.

5. **Major - fixed** - `src/task.rs:1171-1242` - `UniqueValue`

   Scenario: duplicate-field detection applied only to the outer JSON object;
   duplicate fields inside a nested task status object were collapsed by JSON
   value parsing and escaped canonical-record validation.

   Fix: recursively preserve and check every object while deserializing,
   including nested values; added a nested-duplicate regression test. Commit
   `1e6bbf1`.

6. **Minor - fixed** - `src/dashboard/model.rs:441-456` - `sanitize_error_code(code: &str)`

   Scenario: remote or local errors could place arbitrary-length, arbitrary
   characters into the dashboard's structured error `code`, undermining the
   bounded-output contract and stable API vocabulary.

   Fix: normalize valid codes to bounded upper-snake case and replace invalid
   values with a stable fallback. Commit `5b96165`.

7. **Major - fixed** - `src/dashboard/source.rs:237-244` - `deadline.saturating_sub(started.elapsed())`

   Scenario: a dashboard snapshot accepted a deadline but each active remote
   status lookup used the remote client's default deadline. Several active jobs
   could therefore exceed the request's declared bounded-time budget.

   Fix: propagate the remaining snapshot budget into status requests and stop
   when it expires; covered with a deadline-observing reader test. Commit
   `d359950`.

8. **Major - fixed** - `src/task.rs:409-613` (pre-fix) - `record.serialize_field("prompt", &self.prompt)?`

   Scenario: the pre-review canonical task record retained the full prompt.
   Prompts are owner-only per-turn material and may contain credentials or
   local-path details, so serializing them violates the task privacy boundary.

   Fix: retain the prompt only long enough to validate it and derive metadata;
   omit it from the persisted model and add a serialization regression test.
   Commit `55ff6d9`.

9. **Major - fixed** - `src/transfer_repo.rs:311-318` - `repo_id_for(user_common_dir)? != self.repo_id`

   Scenario: a transfer repository initialized for one user repository could
   import a result when passed a different repository directory. That breaks
   repository isolation and can write refs in an unintended target.

   Fix: bind import to the transfer repository's derived repository identity
   before any ref action, with a cross-repository rejection test. Commit
   `bc60619`.

10. **Major - fixed** - `src/task.rs:539-541` - `publish branch is deferred to a later plan`

    Scenario: the model accepted `publish_branch` although branch publication
    is explicitly deferred. That silently narrowed a requested behavior into a
    non-publishing placeholder instead of returning `TASK_CONFIG_INVALID`.

    Fix: reject the option at validation time and cover the error contract.
    Commit `a104997`.

11. **Minor - fixed** - `src/agent/mod.rs:66-68` - `max turns must be greater than zero`

    Scenario: an explicitly configured zero turn limit was forwarded to an
    agent CLI. Its meaning is adapter-specific and could be interpreted as
    unlimited, invalid, or immediately exhausted.

    Fix: reject zero at the shared turn-limit validation boundary and test the
    error. Commit `ff676f0`.

12. **Major - deferred** - `src/inputs.rs:442-448` - `GIT_CONFIG_GLOBAL`, `"/dev/null"`

    Scenario: input selection deliberately disables all global Git
    configuration. This also suppresses the user's `core.excludesFile`, even
    though the task specification requires selection to honor that ignore
    source alongside repository configuration. An otherwise ignored untracked
    file can therefore be included in a captured task base.

    Deferred plan: add a controlled resolver for only `core.excludesFile`,
    validate the resolved path against the worker's rooted-file policy, and
    pass that exact value to selection Git commands while retaining global and
    system configuration neutralization. Add an end-to-end selection test.
    This is deferred because the required safe config-reading policy is outside
    the reviewed task/transfer scope; enabling arbitrary global configuration
    would be an unsafe substitute.

13. **Minor - deferred** - `src/transfer_repo.rs:118-145,420-489` - `fs::create_dir_all` and `fs::symlink_metadata(path)`

    Scenario: transfer cache creation, alternates writing, scratch-index setup,
    and worktree hashing use path-based filesystem operations without the
    project's rooted descriptor/no-follow abstraction or owner-only mode
    enforcement. A same-account symlink/race in the cache namespace can change
    what is initialized, written, or hashed between checks.

    Deferred plan: move the transfer-cache namespace to rooted,
    descriptor-relative operations; create owner-only directories and staged
    alternates atomically; and pass validated handles or explicit safe paths to
    Git. Add race/symlink fixtures. This crosses cache lifecycle and later
    transfer integration, so changing it in the isolated review would be
    higher risk than documenting the hardening boundary.

14. **Major - deferred** - `src/task.rs:1291-1297` - `title_from_prompt(prompt)`

    Scenario: although raw prompts no longer persist, the first nonempty prompt
    line becomes the canonical task title. That line can itself contain a
    secret or machine-local path and will be exposed through task metadata.

    Deferred plan: make title an explicitly supplied safe field, or adopt a
    specified deterministic redaction policy, then update the task plan and
    tests. The current plan explicitly defines the first-prompt-line title, so
    silently changing it here would alter product semantics rather than merely
    harden implementation.

15. **Major - deferred** - `src/task.rs:242-250,688-731` - `Failed { reason: String }` and `questions: Vec<String>`

    Scenario: task status accepts summary text, questions, changed-file names,
    turn history, and failure strings without shared cardinality, size, or
    secret/local-path redaction rules. Those values are intended for later
    publication and dashboard output, so an agent or tool error could become
    unbounded or reveal sensitive host details.

    Deferred plan: define one typed, bounded, redacting structured-result
    boundary shared by task publication and dashboard rendering, including
    explicit limits and tests for sensitive-looking paths and secrets. The
    current branch lacks the planned publisher/output implementation where
    those lifetime and display contracts must be enforced together.

16. **Major - deferred** - `src/agent/claude.rs:22-68`, `tests/agent_adapters.rs:23-25`, `docs/agent-task-spike.md:20` - `Deferred by operator decision.`

    Scenario: production code launches Claude with session, schema, resume,
    limit, and permission flags, but the spike deferred all live Claude checks
    and the tests state that its fixtures are hand-derived. A CLI version or
    envelope mismatch would only surface after task execution is enabled.

    Deferred plan: an operator with authorized Claude access should run the
    same disposable first-turn, failure, cancellation, and resume spike used
    for Codex; capture sanitized fixtures; then promote the adapter from
    model-only coverage to validated support. This requires external operator
    authority and must not be simulated by weakening the adapter or its tests.

## Verification after fixes

The final verification commands and their results are recorded in the terminal
session for this review. They cover formatting, all Rust test targets, Clippy
with warnings denied, release build, whitespace validation, and the dashboard
client test.
