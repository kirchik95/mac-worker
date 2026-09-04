# Dashboard Tasks View Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Extend the existing read-only loopback dashboard so operators can understand every durable agent task, its run membership and scheduler position, its current or stale authority, its turn history and result, and its active log without confusing a task turn with a legacy command job.

**Architecture:** Add one typed, privacy-safe task/run projection in src/task_view.rs. The task CLI list report and the dashboard snapshot both serialize that projection; the dashboard snapshot flattens the same tasks, runs, and progress fields beside the existing worker, queue, and legacy-job fields. The dashboard service collects local task/run records, bounded remote task status for active/open tasks, persisted runner liveness, and the existing Phase-4 queue advisory. A task-specific read-only source serves detail and cursor-based turn logs through new loopback GET routes. The browser keeps the existing two-second snapshot and one-second active-log polling, renders all application text as DOM text nodes, and leaves legacy job routes intact.

**Tech Stack:** Rust 2024; existing serde/serde_json, axum, tokio, typed task/turn/queue records, ProcessRunner, and rooted ClientStateStore APIs; embedded HTML/CSS/JavaScript; Node’s built-in test runner; existing cargo test, clippy, format, and release-build gates. No new crates, daemon, database, WebSocket, SSE stream, or telemetry service.

**Spec:** docs/superpowers/specs/2026-09-03-agent-task-pool-design.md sections 7.1, 8.1–8.3, 12.1–12.2, 13, 19, 20.1–20.2, and 21; docs/superpowers/specs/2026-08-26-local-dashboard-design.md; the existing dashboard implementation in src/dashboard/; docs/superpowers/plans/2026-08-28-local-dashboard.md; and the phase-5 execution/publication plans that establish the task and turn records consumed here.

## Global Constraints

- Start only after the phase-5 core and publication prerequisites, including the task/turn records, queue accessors, runner, result publication, and their local gates, are present on main. Rebase this branch onto that exact prerequisite state before implementation.
- This worktree is plan-only and must not contact a worker. The live three-worker acceptance in the final task is an operator-run gate after implementation; it is described but not executed by this plan.
- The dashboard remains a read-only observer. Snapshot collection may read local state, inspect the local process table, issue bounded remote status/log GET-like host operations, and read the existing worker observation cache. It must not start or recover a runner, reconcile tasks, refresh scheduler facts, acquire or release a lease, submit/cancel/close a turn, fetch a result, mutate local records, or mutate remote records.
- Preserve the existing coalesced snapshot refresh in DashboardService. The task projection participates in the one global collection budget and does not create a second refresh loop or per-tab SSH fan-out. A failed refresh keeps the existing completed snapshot fallback behavior.
- Apply the authority order from task-pool section 8.3: remote durable task/turn status for active/open tasks, remote lease occupancy for worker slots, local task/queue records for queued and terminal task facts, and cached worker observations only when a current worker observation fails. A failed remote task-status read keeps the safe local row and marks it stale; it never turns a dead or unknown runner into a live one.
- Use the existing typed ClientStateStore::list_tasks, list_runs, runner_liveness, turn_ids_for_task, queue_rows_with_blocking_reasons, and queue_entry_for_task_turn accessors. Do not reimplement FIFO, worker eligibility, run-cap accounting, or blocking-reason selection in dashboard code.
- The shared projection belongs in domain-neutral src/task_view.rs, not in the CLI writer or a browser-only dashboard module. worker task list --json and the snapshot endpoint must use the same TaskListProjection, TaskListRow, TaskRunProjection, RunProgress, and serialization rules. The CLI may add its existing protocol-version envelope; the projection fields beneath it must be identical.
- Keep task-turn identity distinct from legacy job identity in the UI and API. TurnId is currently the JobId alias, and a task turn intentionally has no legacy LocalJobRecord; resolve a task turn to its task only through the task-owned turn namespace and status records.
- Do not expose prompts, prompt-file contents, environment-profile values, environment variable values, SSH destinations or configuration, session references, credentials, complete local filesystem paths, raw host stderr, or unbounded agent output in records, queue rows, CLI JSON, snapshot JSON, task detail, or normalized timeline data. Titles, summaries, questions, failure reasons, diff stats, changed-file names, and agent log text pass through the existing RedactionBoundary; changed files must remain repository-relative or be replaced by a path placeholder.
- Render agent/application text as text only. Browser code must use textContent, createTextNode, or DOM element construction; it must never interpret Markdown, HTML, ANSI escape sequences, or log content as markup. Log responses remain bounded base64 byte chunks through DashboardLogChunk.
- Keep loopback-only binding, Host validation, Cache-Control: no-store, restrictive CSP, nosniff, no-referrer, no CORS, typed identifier parsing, and the existing 1..=65_536 log limit. New task routes accept only canonical task/turn IDs and stream/offset/limit query values.
- Snapshot polling remains two seconds. A selected active task turn polls stdout and stderr at one second with independent byte cursors and decoder state; it stops only after both terminal stream lengths are reached, or when the task is no longer active and the final lengths are unavailable. Selection-generation checks prevent an older response from writing into a newer task panel.
- Keep all current legacy job snapshot fields, job detail routes, job log routes, and legacy browser tests working. Task turns appear in the tasks view and task routes rather than being fabricated as legacy jobs.
- Use typed fakes and fixture records in tests. Do not make unit tests depend on a live Mac mini, real credentials, real agent output, wall-clock sleeps, or a mutable scheduler refresh. Live acceptance evidence contains only shortened IDs, state/count/timing/outcome facts, and privacy-safe pass/fail observations.
- Do not add public dashboard mutation controls in this phase. Cancel, retry, reconcile, runner recovery, task saying, task closing, and task result fetching remain CLI operations outside the dashboard.

## File Map

~~~text
src/task_view.rs                         shared safe task/run list, detail, turn, and timeline projection DTOs
src/lib.rs                              module export and task-list JSON envelope wiring
src/task_client.rs                      TaskListReport backed by the shared projection
src/transfer.rs                         bounded remote task-status method matching job-status deadlines

src/dashboard/mod.rs                    dashboard task module export
src/dashboard/model.rs                  snapshot/task/worker/queue DTO extensions and safe serialization
src/dashboard/service.rs                 coalesced task collection and active-task worker-card enrichment
src/dashboard/source.rs                  remote-reader task-status seam and production task collection wiring
src/dashboard/task.rs                    read-only task detail and task-turn log source
src/dashboard/queue.rs                   Phase-4 queue metadata projection without scheduler decisions
src/dashboard/command.rs                 production HTTP-state wiring for the task source
src/dashboard/web.rs                    loopback task/detail/log routes and typed validation
src/dashboard/static/index.html          tasks table, filters, run cards, detail, timeline, and task log shell
src/dashboard/static/dashboard.css       responsive task/run/detail/log styles
src/dashboard/static/dashboard.mjs       client-side filters, safe task rendering, detail fetch, and one-second logs

tests/task_view.rs                       shared projection JSON, run position, progress, and privacy contracts
tests/task_command.rs                    CLI list JSON parity and retained command behavior
tests/dashboard_model.rs                 snapshot fixture updates and safe task/queue fields
tests/dashboard_service.rs               task authority, stale rows, coalescing, and active-task enrichment
tests/dashboard_source.rs                production source and bounded remote task-status fakes
tests/dashboard_queue.rs                 queue entry kind, task-turn mapping, pins, run caps, and reason codes
tests/dashboard_tasks.rs                 typed cross-surface, privacy, and read-only regression fixtures
tests/dashboard_web.rs                   task routes, typed IDs, query bounds, headers, and source errors
tests/dashboard_command.rs               production launcher state wiring
tests/dashboard_cpu_adapter.rs           existing source-trait fixture compatibility
tests/dashboard_client.mjs                browser task/filter/detail/timeline/log tests and legacy regression

README.md                               local dashboard and task-view usage/security documentation
docs/dashboard-validation.md            phase-5e automated and sanitized live acceptance record
~~~

### Parallelism

After Task 1 has landed and its serialized fixture schema is reviewed, Tasks 2 and 3 are parallel-safe: Task 2 owns Rust collection/projection adapters and Rust fixtures; Task 3 owns only the embedded HTML/CSS/JavaScript and browser fixtures. Task 3 uses the Task 1 snapshot shape and route contract as a typed fixture and does not edit Rust files. Task 4 waits for both because it wires the Rust HTTP state to the backend source and validates the browser-facing routes. Task 5 is sequential after the complete local gate.

---

### Task 1: Define the Shared Task/Run Projection and Align CLI JSON

**Gate:** The phase-5 task model, run records, turn summaries, result fields, and worker task list command exist on main, and the existing task model/command suites pass. No worker access is needed or permitted.

**Files:**

- Create: src/task_view.rs
- Modify: src/lib.rs
- Modify: src/task_client.rs
- Create: tests/task_view.rs
- Modify: tests/task_command.rs

**Interfaces:**

Define the following serializable, safe DTOs in src/task_view.rs:

~~~rust
pub enum TaskFreshness {
    Current,
    Stale,
}

pub struct TaskListProjection {
    pub tasks: Vec<TaskListRow>,
    pub runs: Vec<TaskRunProjection>,
    pub progress: RunProgress,
}

pub struct TaskListRow {
    pub task_id: TaskId,
    pub run_id: Option<RunId>,
    pub run_position: Option<u32>,
    pub title: String,
    pub agent: String,
    pub state: TaskState,
    pub last_outcome: Option<TaskOutcome>,
    pub worker: Option<String>,
    pub branch: BranchName,
    pub turn_count: u32,
    pub runner: Option<RunnerState>,
    pub freshness: TaskFreshness,
    pub created_at_millis: u64,
    pub updated_at_millis: u64,
    pub active_turn_id: Option<TurnId>,
}

pub struct TaskRunProjection {
    pub run_id: RunId,
    pub name: Option<String>,
    pub max_parallel: u32,
    pub created_at_millis: u64,
    pub progress: RunProgress,
}

pub struct TaskDetailProjection {
    pub task: TaskListRow,
    pub project_id: String,
    pub worktree_id: String,
    pub base_oid: Option<BaseOid>,
    pub head_oid: Option<BaseOid>,
    pub session_present: bool,
    pub summary: Option<String>,
    pub questions: Vec<String>,
    pub files_changed: Vec<String>,
    pub diff_stat: Option<String>,
    pub fetch_command: String,
    pub turns: Vec<TaskTurnProjection>,
    pub timeline: Vec<TaskTimelineEvent>,
}

pub struct TaskTurnProjection {
    pub turn_number: u32,
    pub turn_id: TurnId,
    pub terminal: Option<TurnTerminal>,
    pub outcome: Option<TaskOutcome>,
    pub agent_committed: Option<bool>,
    pub log_truncated: bool,
    pub started_at_millis: Option<u64>,
    pub ended_at_millis: Option<u64>,
}

pub struct TaskTimelineEvent {
    pub turn_number: u32,
    pub turn_id: TurnId,
    pub outcome: Option<TaskOutcome>,
    pub started_at_millis: Option<u64>,
    pub ended_at_millis: Option<u64>,
    pub terminal: Option<TurnTerminal>,
}
~~~

Use the existing BranchName::for_task, TaskTitle::as_str, TaskMeta, TaskStatus, TurnSummary, RunRecord, RunProgress, RedactionBoundary, and agent-name conversion. The row builder must derive one-based run_position from the ordered RunRecord::task_ids, preserve task titles without prompts, use TaskState and TaskOutcome wire names, and set active_turn_id only for the current nonterminal turn. Checked numeric conversions must return a bounded TaskViewError rather than wrapping.

Add pure constructors with these concrete responsibilities:

~~~rust
pub fn project_task_list(
    records: &[LocalTaskRecord],
    runs: &[RunRecord],
    runner_states: &HashMap<TaskId, Option<RunnerState>>,
    freshness: &HashMap<TaskId, TaskFreshness>,
) -> Result<TaskListProjection, TaskViewError>;

pub fn project_task_detail(
    record: &LocalTaskRecord,
    status: &TaskStatus,
    runner: Option<RunnerState>,
    freshness: TaskFreshness,
) -> Result<TaskDetailProjection, TaskViewError>;
~~~

project_task_list must sort rows by updated_at_millis and then canonical task ID, retain run order only in run_position, compute the top-level progress from the resulting effective states, and compute each run card from its task IDs. A missing task ID in a run produces a safe collection error in the caller and does not create a fabricated row. project_task_detail copies only the safe status fields, applies redaction to summary/questions/changed files/diff stat/outcomes, creates worker task fetch <task-id> as the exact fetch command, and produces the ordered turn/timeline projections from TaskStatus::turns.

Update TaskListReport so it stores a TaskListProjection, exposes projection(), tasks(), runs(), and progress() accessors over that projection, and has TaskClient::list load local tasks/runs plus runner_liveness and call project_task_list. Existing --run, --state, and --full parsing remains accepted; the safe projection is the JSON shape for every list invocation and never includes prompt data.

Add a TaskListJson envelope in the shared module with protocol_version plus a flattened TaskListProjection. write_task_list_report must serialize that envelope instead of constructing an independent serde_json::json! map. A dashboard snapshot stores the same projection in a task_view field with serde flattening and omits the protocol envelope.

- [ ] **Step 1: Write the failing projection and parity tests**

Create typed task/run fixtures with a run ordered as task-a, task-b, an active task with two turn summaries, a terminal task, a pinned worker, a hostile title/summary/path-like value, and a dead runner. Add tests with these assertions:

~~~rust
#[test]
fn cli_and_snapshot_projection_fields_are_identical() {
    let projection = fixture_projection();
    let cli = serde_json::to_value(TaskListJson::new(PROTOCOL_VERSION, projection.clone())).unwrap();
    let snapshot = serde_json::to_value(fixture_snapshot_with_tasks(projection)).unwrap();
    assert_eq!(cli["tasks"], snapshot["tasks"]);
    assert_eq!(cli["runs"], snapshot["runs"]);
    assert_eq!(cli["progress"], snapshot["progress"]);
    assert_eq!(cli["protocol_version"], PROTOCOL_VERSION);
}

#[test]
fn rows_keep_run_position_and_safe_task_fields() {
    let projection = fixture_projection();
    assert_eq!(projection.tasks[0].run_position, Some(1));
    assert_eq!(projection.tasks[1].run_position, Some(2));
    assert_eq!(projection.tasks[0].branch.as_str(), format!("task/{}", projection.tasks[0].task_id));
    let value = serde_json::to_value(projection).unwrap();
    assert_absent_keys_and_values(&value, &["prompt", "env", "session_ref", "ssh", "path"]);
}

#[test]
fn detail_timeline_is_derived_from_turn_records_without_raw_events() {
    let detail = fixture_detail();
    assert_eq!(detail.timeline.len(), detail.turns.len());
    assert_eq!(detail.timeline[0].turn_id, detail.turns[0].turn_id);
    assert!(detail.summary.as_deref().is_some());
    assert!(detail.questions.len() <= 16);
    assert!(detail.files_changed.iter().all(|path| !path.starts_with('/')));
}
~~~

Also assert that TaskOutcome::Failed reasons, hostile controls, home paths, tilde paths, token-shaped values, prompt text, environment values, and session references cannot appear in list/detail JSON, and that TaskRunProjection::progress counts queued/active/open/closed/abandoned/lost exactly through RunProgress.

- [ ] **Step 2: Run the focused suites to verify RED**

Run:

~~~bash
cargo test --locked --test task_view --test task_command -- --nocapture
~~~

Expected: FAIL to compile because the shared projection module, report wrapper, and CLI envelope do not yet exist.

- [ ] **Step 3: Implement the shared projection and CLI wiring**

Add strict serde implementations with canonical task/run/turn IDs and snake-case enums. Keep DTO fields bounded and public only through the projection module. Implement TaskViewError with a stable uppercase code and bounded message, map it to WorkerError in TaskClient, and ensure list filtering occurs before projection. Replace the current TaskListReport { tasks: Vec<TaskSummary> } implementation and the manual JSON map in write_task_list_report with the shared projection/envelope. Preserve human list output, but source its state/worker/title from TaskListRow.

- [ ] **Step 4: Run the focused suites to verify GREEN**

Run:

~~~bash
cargo fmt --all --check
cargo test --locked --test task_view --test task_command --test task_model -- --nocapture
~~~

Expected: all listed tests pass; the list JSON has one protocol envelope and identical tasks, runs, and progress values wherever the projection is serialized.

- [ ] **Step 5: Commit the shared contract**

~~~bash
git add src/task_view.rs src/lib.rs src/task_client.rs tests/task_view.rs tests/task_command.rs
git commit -m "feat: define shared dashboard task projection"
~~~

---

### Task 2: Collect Task State Read-Only and Preserve Scheduler Queue Facts

**Gate:** Task 1 is landed; the v1 dashboard service/source/model, the Phase-4 queue accessor, and the task runner/status protocol are present on main. All tests in this task use typed fakes and local state only.

**Files:**

- Modify: src/dashboard/mod.rs
- Modify: src/dashboard/model.rs
- Modify: src/dashboard/service.rs
- Modify: src/dashboard/source.rs
- Create: src/dashboard/task.rs
- Modify: src/dashboard/queue.rs
- Modify: src/transfer.rs
- Modify: tests/dashboard_model.rs
- Modify: tests/dashboard_service.rs
- Modify: tests/dashboard_source.rs
- Modify: tests/dashboard_queue.rs
- Modify: tests/dashboard_command.rs
- Modify: tests/dashboard_cpu_adapter.rs
- Create: tests/dashboard_tasks.rs

**Interfaces:**

Extend DashboardSnapshot with a task_view: TaskListProjection field using serde flattening, so its JSON has top-level tasks, runs, and progress beside the existing workers, queue, active_jobs, and recent_jobs. Add the following safe worker/queue types:

~~~rust
pub struct DashboardActiveTask {
    pub task_id: TaskId,
    pub title: String,
    pub agent: String,
    pub turn_number: u32,
    pub started_at_millis: Option<u64>,
    pub runner: Option<RunnerState>,
}

pub enum DashboardQueueEntryKind {
    Batch,
    TaskTurn,
}
~~~

Add active_task: Option<DashboardActiveTask> to DashboardWorker. Add entry_kind, task_id, turn_id, run_id, run_max_parallel, and pinned_worker to DashboardQueueEntry while retaining the existing safe legacy fields and job_id for compatibility. Serialize the new fields explicitly and sanitize any display text; queue capability requirements and blocking codes stay typed/bounded.

Add a dashboard-only collection wrapper and task source seam:

~~~rust
pub struct DashboardTaskCollection {
    pub projection: TaskListProjection,
    pub errors: Vec<DashboardError>,
}

pub trait DashboardTaskSource: Send + Sync + 'static {
    fn task_detail(&self, task_id: TaskId) -> Result<TaskDetailProjection, ApiError>;
    fn read_task_log(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    ) -> Result<DashboardLogChunk, ApiError>;
}
~~~

Extend DashboardDataSource with task_projection(&self, deadline: Duration) -> Result<DashboardTaskCollection, DashboardError>. Give existing test-only sources a default empty collection so legacy dashboard tests remain focused, while MacWorkerDashboardSource supplies the production projection. Add task_status_with_deadline to DashboardRemoteReader; implement RemoteJobClient::task_status_with_deadline by validating the response and using control_policy(deadline), and make the existing task_status delegate to the maximum control deadline.

In MacWorkerDashboardSource::task_projection:

1. Read ClientStateStore::list_tasks and list_runs once per snapshot, and obtain persisted runner liveness for every listed task without writing state.
2. For queued, closed, abandoned, and lost tasks, use the local status as the effective status. For active/open tasks with a recorded worker, call DashboardRemoteReader::task_status_with_deadline using the remaining snapshot budget and TaskStatusRequest::new(project_id, task_id).
3. On a successful remote response, use its status fields with the local immutable metadata and runner observation. On a failed or expired remote response, keep the local status, set TaskFreshness::Stale, and append one bounded TASK_STATUS_STALE collection error that contains no worker destination, SSH diagnostic, prompt, or path.
4. Build the list using project_task_list; build detail from the same effective status using project_task_detail. Do not call TaskClient::status, reconcile_runners, refresh-facts, or any mutating path.
5. Map local read errors to the existing safe dashboard error boundary. Never return a raw WorkerError message to the browser.

After task projection exists, enrich each worker card in DashboardService::collect_snapshot: match DashboardWorker.slot.active_job_id to a task row’s active_turn_id, require TaskState::Active, and attach the title, agent, turn number, started timestamp, and runner state. Leave active_job_id present for legacy compatibility and leave active_task absent when the mapping is unknown. Do not infer a task from project/worktree labels or from a worker’s command text.

Extend PhaseFourQueueEntry and DashboardQueueEntry from the existing phase-4 row without moving scheduler logic. ClientStateDashboardQueueReader must read the durable row’s kind, run, and preference, map a task-turn job_id to task_id by the existing turn_ids_for_task namespace, preserve one-based FIFO position, and carry the phase-4 blocking_code. SchedulerQueueAdapter must copy these fields from its typed input. A batch row has task_id/turn_id absent; a task-turn row never treats its turn ID as its task ID.

- [ ] **Step 1: Write failing collection, queue, and privacy tests**

Add typed fake source/remote/worker readers and local task fixtures. Cover these exact cases:

~~~rust
#[test]
fn active_remote_task_status_overrides_local_status_without_a_write() {
    let harness = DashboardTaskHarness::active_local_task()
        .with_remote_status(TaskState::Open, Some(TaskOutcome::NeedsInput))
        .with_remote_runner(RunnerState::Live);
    let before = harness.local_state_fingerprint();
    let snapshot = harness.snapshot().unwrap();
    let row = snapshot.task_view.tasks.iter().find(|row| row.task_id == harness.task_id()).unwrap();
    assert_eq!(row.state, TaskState::Open);
    assert_eq!(row.last_outcome, Some(TaskOutcome::NeedsInput));
    assert_eq!(row.freshness, TaskFreshness::Current);
    assert_eq!(before, harness.local_state_fingerprint());
    assert_eq!(harness.mutation_calls(), 0);
}

#[test]
fn remote_status_failure_keeps_a_stale_row_and_dead_runner() {
    let harness = DashboardTaskHarness::active_local_task()
        .with_runner_liveness(Some(RunnerState::Dead))
        .with_remote_failure("SSH_UNAVAILABLE");
    let snapshot = harness.snapshot().unwrap();
    let row = snapshot.task_view.tasks.iter().find(|row| row.task_id == harness.task_id()).unwrap();
    assert_eq!(row.freshness, TaskFreshness::Stale);
    assert_eq!(row.runner, Some(RunnerState::Dead));
    assert!(snapshot.collection.errors.iter().any(|error| error.code == "TASK_STATUS_STALE"));
    assert_eq!(harness.mutation_calls(), 0);
}

#[test]
fn queue_projection_keeps_kind_pin_run_cap_and_phase_four_reason() {
    let snapshot = DashboardTaskHarness::queue_with_task_turn_and_batch().snapshot().unwrap();
    assert_eq!(snapshot.queue[0].entry_kind, DashboardQueueEntryKind::TaskTurn);
    assert!(snapshot.queue[0].task_id.is_some());
    assert_eq!(snapshot.queue[0].run_max_parallel, Some(2));
    assert_eq!(snapshot.queue[0].pinned_worker.as_deref(), Some("mini-2"));
    assert_eq!(snapshot.queue[0].blocking_code, "RUN_MAX_PARALLEL");
    assert_eq!(snapshot.queue[1].entry_kind, DashboardQueueEntryKind::Batch);
    assert!(snapshot.queue[1].task_id.is_none());
}

#[test]
fn active_worker_card_maps_turn_identity_to_task_title_and_agent() {
    let snapshot = DashboardTaskHarness::active_local_task().snapshot().unwrap();
    let worker = snapshot.workers.iter().find(|worker| worker.name == "mini-1").unwrap();
    let active = worker.active_task.as_ref().unwrap();
    assert_eq!(active.task_id, snapshot.task_view.tasks[0].task_id);
    assert_eq!(active.title, "Repair login");
    assert_eq!(active.agent, "codex");
    assert_eq!(active.turn_number, 1);
}
~~~

Assert recursively that serialized snapshot/task/detail/queue values contain no prompt, env, environment value, session reference, SSH destination, home path, absolute path, raw command, or raw error diagnostic. Add read-only counters to fake ClientStateStore/remote boundaries and assert no refresh, runner start, recovery, reconciliation, lease, or state-write method is called. Update every existing DashboardSnapshot fixture to use TaskListProjection::empty() and every existing data-source fake to return an empty task collection.

- [ ] **Step 2: Run the focused suites to verify RED**

Run:

~~~bash
cargo test --locked --test dashboard_tasks --test dashboard_model --test dashboard_service --test dashboard_source --test dashboard_queue --test dashboard_command --test dashboard_cpu_adapter -- --nocapture
~~~

Expected: FAIL to compile because the snapshot fields, task source, remote deadline method, and queue metadata do not exist.

- [ ] **Step 3: Implement bounded task collection and queue projection**

Add the new DTO fields with explicit serialization, update the service’s snapshot assembly to collect tasks within the remaining global deadline and add collection errors with push_error, and keep task collection inside the existing single-flight refresh. Add the task projection helpers in src/dashboard/task.rs for local/remote status selection, detail, task-turn identity validation, and log-source methods. Use DashboardLogChunk::from_log_chunk for remote turn logs and enforce the existing max limit before calling the remote reader.

Implement MacWorkerDashboardSource and MacWorkerTaskSource with shared Arc<Config>, Arc<ClientStateStore>, and Arc<dyn DashboardRemoteReader>. Resolve a requested task’s worker only from its safe local record/status, resolve a requested turn only from turn_ids_for_task, and return TASK_NOT_FOUND or TURN_NOT_FOUND without echoing raw identifiers in error messages. The task source must return local stale detail when remote status is unavailable and never update the local task record.

- [ ] **Step 4: Run the backend dashboard regression suite to verify GREEN**

Run:

~~~bash
cargo fmt --all --check
cargo test --locked --test dashboard_model --test dashboard_cache --test dashboard_service --test dashboard_source --test dashboard_queue --test dashboard_command --test dashboard_cpu_adapter --test dashboard_tasks -- --nocapture
~~~

Expected: all existing v1 tests and new task/queue/privacy tests pass, task remote failures are stale rather than destructive, and fake mutation counters remain zero.

- [ ] **Step 5: Commit the read-only backend projection**

~~~bash
git add src/dashboard/mod.rs src/dashboard/model.rs src/dashboard/service.rs src/dashboard/source.rs src/dashboard/task.rs src/dashboard/queue.rs src/transfer.rs tests/dashboard_model.rs tests/dashboard_service.rs tests/dashboard_source.rs tests/dashboard_queue.rs tests/dashboard_command.rs tests/dashboard_cpu_adapter.rs tests/dashboard_tasks.rs
git commit -m "feat: collect dashboard task state read-only"
~~~

---

### Task 3: Build the Embedded Tasks/Run Browser Surface

**Gate:** Task 1’s flattened snapshot shape and task-detail/log route contract are reviewed. This task may run in parallel with Task 2 and must modify only embedded assets and tests/dashboard_client.mjs; it uses typed JSON fixtures rather than a running Rust server.

**Files:**

- Modify: src/dashboard/static/index.html
- Modify: src/dashboard/static/dashboard.css
- Modify: src/dashboard/static/dashboard.mjs
- Modify: tests/dashboard_client.mjs

**Interfaces:**

Add these stable DOM nodes and test IDs:

~~~text
task-filter-run
task-filter-state
task-filter-worker
task-filter-agent
run-progress
task-list
task-detail
task-timeline
task-stdout-log
task-stderr-log
~~~

Place the task view before the legacy job history/detail area. The shell must label the run cards, filters, task table, immutable detail, normalized turn timeline, result card, and raw stdout/stderr log panel. Include an accessible empty state and a visible read only label. Do not add form submission, mutation buttons, external resources, or inline event-handler attributes.

Extend createDashboardClient with:

- renderRunProgress for the top-level progress and each TaskRunProjection, showing total/queued/active/open/closed/failed-like counts and retaining run ID/name as text.
- renderTaskTable over snapshot.tasks, with local filters for run, state, worker, and agent. Filter changes re-render the current snapshot without a network request; options are built from the current typed rows and preserve a selected valid value across snapshot revisions.
- renderTaskRow using task title, agent, state, worker, runner, freshness, turn count, run position, last outcome, and age. Each row is a button that calls openTask with the canonical task ID.
- openTask using selection generation checks and GET /api/v1/tasks/<task-id>, while preserving the existing openJob path for legacy jobs. The detail must show the title, state/freshness, agent/worker/runner, run position, branch, base/head IDs, session-present boolean, summary, questions, changed files, diff stat, and the exact safe fetch command.
- renderTaskTimeline from the detail’s timeline/turns fields. Show turn number, opaque shortened turn ID, start/end values when present, terminal/outcome, commit flag, and truncation flag as text. Do not reconstruct timestamps from browser time or parse raw logs into HTML.
- refreshTaskLogs for the selected active turn using /api/v1/tasks/<task-id>/turns/<turn-id>/logs?... , independent stdout/stderr offsets, streaming TextDecoders, base64 decoding, and one-second timer cadence. Stop at the exact final byte lengths when available, flush decoder state once, and keep stale/error text in the stderr pane as text.
- Worker-card rendering of worker.active_task with task title, agent, turn number, and elapsed/started value while retaining active legacy job ID.

Keep all existing legacy rendering and polling APIs. Every fetch call must carry { cache: 'no-store' }; no client method may issue a non-GET request. Keep the current stale snapshot revision guard and add task-detail selection guards for task/detail/log responses.

- [ ] **Step 1: Write the failing browser tests**

Extend the fixture snapshot with three workers, two runs, queued/active/open/closed/lost task rows, a task-turn queue row with a blocking reason, an active worker task, one detail payload with two turns, and base64 log chunks. Add tests with these assertions:

~~~javascript
test('task filters are client-side and never call a mutating endpoint', async () => {
  const harness = createHarness(taskSnapshotFixture());
  const client = createDashboardClient(harness);
  await client.refreshSnapshot();
  harness.nodes['task-filter-state'].value = 'active';
  harness.nodes['task-filter-state'].dispatchEvent(new Event('change'));
  assert.deepEqual(harness.visibleTaskIds(), ['task-active']);
  assert.ok(harness.fetchCalls.every(({ path, options }) => path.startsWith('/api/v1/') && (!options?.method || options.method === 'GET')));
});

test('task detail renders result and normalized timeline as text', async () => {
  const harness = createHarness(taskSnapshotFixture(), {
    '/api/v1/tasks/task-active': taskDetailFixture('<img src=x onerror=bad>'),
  });
  const client = createDashboardClient(harness);
  await client.openTask('task-active');
  assert.match(harness.nodes['task-detail'].textContent, /worker task fetch task-active/);
  assert.match(harness.nodes['task-timeline'].textContent, /Turn 1/);
  assert.equal(harness.nodes['task-detail'].querySelector('img'), null);
});

test('active task logs poll every second with independent byte cursors', async () => {
  const harness = createHarness(taskSnapshotFixture(), taskLogResponses());
  const client = createDashboardClient(harness);
  client.start();
  await client.openTask('task-active');
  await harness.timers.tick(1_000);
  await harness.timers.tick(1_000);
  assert.deepEqual(client.taskOffsets(), { stdout: 6, stderr: 6 });
  assert.equal(harness.nodes['task-stdout-log'].textContent, 'out-1out-2');
  assert.equal(harness.nodes['task-stderr-log'].textContent, 'err-1err-2');
});

test('worker cards show task title and agent without losing active turn identity', async () => {
  const harness = createHarness(taskSnapshotFixture());
  await createDashboardClient(harness).refreshSnapshot();
  assert.match(harness.nodes['worker-grid'].textContent, /Repair login/);
  assert.match(harness.nodes['worker-grid'].textContent, /codex/);
});
~~~

Retain and run the existing hostile-text, stale-revision, log-cursor, terminal-stop, selection-race, polling-cadence, and no-mutation tests. Add assertions that prompt-like fixture text, environment values, absolute paths, and raw markup appear only as literal text or redacted values.

- [ ] **Step 2: Run the browser suite to verify RED**

Run:

~~~bash
node --test tests/dashboard_client.mjs
~~~

Expected: FAIL because the required task nodes, renderers, selection API, and task log route handling are absent.

- [ ] **Step 3: Implement the task/run browser surface**

Add the task sections and responsive styles using the existing visual language. Keep the implementation dependency-free and use DOM construction helpers for every dynamic value. Populate filter options deterministically, render no row from an unvalidated ID, clear detail/log panes on a selection change, and make old task-detail responses harmless after a newer task is selected. Keep timer handles separate for snapshot, legacy job logs, and active task logs; coalesce overlapping active-task log refreshes.

- [ ] **Step 4: Run the browser regression suite to verify GREEN**

Run:

~~~bash
node --test tests/dashboard_client.mjs
~~~

Expected: all legacy and task browser tests pass, including safe rendering, two-second snapshots, one-second active-task log polling, exact byte cursors, terminal stopping, and selection races.

- [ ] **Step 5: Commit the embedded browser surface**

~~~bash
git add src/dashboard/static/index.html src/dashboard/static/dashboard.css src/dashboard/static/dashboard.mjs tests/dashboard_client.mjs
git commit -m "feat: render dashboard tasks view"
~~~

---

### Task 4: Expose Typed Task Detail/Log Routes and Cross-Surface Contracts

**Gate:** Tasks 1–3 are complete, Task 2’s DashboardTaskSource is available, and the focused backend/browser suites pass. This task wires the server; it does not broaden the dashboard to any mutation.

**Files:**

- Modify: src/dashboard/web.rs
- Modify: src/dashboard/command.rs
- Modify: tests/dashboard_web.rs
- Modify: tests/dashboard_command.rs
- Modify: tests/dashboard_tasks.rs

**Interfaces:**

Extend DashboardHttpState<S, C, M> with task_source: Arc<dyn DashboardTaskSource>. SystemDashboardLauncher::from_system constructs MacWorkerTaskSource from the same config, client-state store, and remote reader used by the dashboard/legacy log sources.

Add exactly these GET routes:

~~~text
GET /api/v1/tasks/{task_id}
GET /api/v1/tasks/{task_id}/turns/{turn_id}/logs?stream=stdout|stderr&offset=N&limit=N
~~~

Run both source calls in spawn_blocking, preserve no-store JSON responses and existing security middleware, and keep /api/v1/jobs/{job_id} and /api/v1/jobs/{job_id}/logs unchanged. Parse task IDs and turn IDs into their typed canonical forms before calling a source. Validate that the turn belongs to the task in the source before reading any bytes. Reuse parse_log_query for stream/offset/limit and reject duplicate/unknown/missing query keys exactly as legacy logs do.

Use these safe error mappings:

~~~text
INVALID_TASK_ID / INVALID_TURN_ID / INVALID_LOG_QUERY / INVALID_LOG_STREAM / INVALID_LOG_RANGE -> 400
TASK_NOT_FOUND / TURN_NOT_FOUND -> 404
TASK_SOURCE_FAILED / remote source failures -> 502; a task-status outage returns a 200 detail marked stale
unexpected task handler join failure -> 500
~~~

Messages remain bounded and generic. Do not include the raw path parameter, worker name, SSH command, prompt, environment, or transport stderr in an error. Keep Host validation against the actual loopback listener and all existing security headers.

- [ ] **Step 1: Write the failing route and parity tests**

Add typed fake task source tests:

~~~rust
#[tokio::test]
async fn task_detail_route_returns_safe_detail_and_preserves_legacy_routes() {
    let server = fixture_server(FixtureTaskSource::with_detail(fixture_detail())).await;
    let response = get(server.url("/api/v1/tasks/018f0f4a6b5c7d8e9f00112233445566")).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.json["task"]["task_id"], "018f0f4a6b5c7d8e9f00112233445566");
    assert!(response.json["task"]["prompt"].is_null());
    assert!(response.json["fetch_command"].as_str().unwrap().starts_with("worker task fetch "));
    assert_legacy_job_route_still_works(&server).await;
}

#[tokio::test]
async fn task_log_route_validates_turn_membership_and_byte_ranges() {
    let server = fixture_server(FixtureTaskSource::with_log_chunk()).await;
    let good = get(server.url("/api/v1/tasks/018f0f4a6b5c7d8e9f00112233445566/turns/018f0f4a6b5c7d8e9f00112233445567/logs?stream=stdout&offset=3&limit=4")).await;
    assert_eq!(good.status(), StatusCode::OK);
    assert_eq!(good.json["offset"], 3);
    assert_eq!(good.json["next_offset"], 7);
    assert_eq!(get_status(server.url("/api/v1/tasks/not-an-id")).await, StatusCode::BAD_REQUEST);
    assert_eq!(get_status(server.url("/api/v1/tasks/018f0f4a6b5c7d8e9f00112233445566/turns/018f0f4a6b5c7d8e9f00112233445568/logs?stream=stdout&offset=0&limit=1")).await, StatusCode::NOT_FOUND);
    assert_eq!(get_status(server.url("/api/v1/tasks/018f0f4a6b5c7d8e9f00112233445566/turns/018f0f4a6b5c7d8e9f00112233445567/logs?stream=stdout&offset=0&limit=0")).await, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn task_routes_keep_loopback_security_and_issue_no_mutations() {
    let server = fixture_server(FixtureTaskSource::with_detail(fixture_detail())).await;
    let response = get_with_host(server.url("/api/v1/tasks/018f0f4a6b5c7d8e9f00112233445566"), "evil.example").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(server.mutation_calls(), 0);
    assert_eq!(response.headers["cache-control"], "no-store");
    assert_eq!(response.headers["x-content-type-options"], "nosniff");
}
~~~

In dashboard_tasks.rs, serialize the exact same fixture through TaskListJson and DashboardSnapshot and compare tasks, runs, and progress. Assert that the task source receives typed IDs and GET-range values only, that stale detail is marked stale, and that no source call can start/recover/reconcile a runner.

- [ ] **Step 2: Run the route suites to verify RED**

Run:

~~~bash
cargo test --locked --test dashboard_web --test dashboard_command --test dashboard_tasks -- --nocapture
~~~

Expected: FAIL because the task source is not in HTTP state and the task routes/handlers are not registered.

- [ ] **Step 3: Implement the HTTP wiring**

Register the two task routes, add typed path parsers and task-specific handlers, add task source error mapping, and wire the system launcher. Keep route order unambiguous so the task-turn log route is selected before the task detail fallback. Use the existing api_json, api_error, source_error, and security-header helpers where their contracts apply; add task-specific status mapping without exposing data in errors.

- [ ] **Step 4: Run the complete local surface suites**

Run:

~~~bash
cargo fmt --all --check
cargo test --locked --test dashboard_model --test dashboard_cache --test dashboard_service --test dashboard_source --test dashboard_queue --test dashboard_command --test dashboard_cpu_adapter --test dashboard_tasks --test dashboard_web -- --nocapture
node --test tests/dashboard_client.mjs
~~~

Expected: all focused Rust suites and all browser tests pass; legacy job APIs remain compatible; task list JSON and snapshot JSON have identical projection fields; malformed IDs, mismatched turns, hostile Host values, and out-of-range logs are rejected.

- [ ] **Step 5: Commit the HTTP task surface**

~~~bash
git add src/dashboard/web.rs src/dashboard/command.rs tests/dashboard_web.rs tests/dashboard_command.rs tests/dashboard_tasks.rs
git commit -m "feat: expose dashboard task routes"
~~~

---

### Task 5: Document, Sanitize, and Run the Operator Acceptance Gate

**Gate:** Tasks 1–4 are landed; the focused suites are green; the release helper/client revision is installed only by the operator on the three configured workers. The current plan worktree must not run the live procedure.

**Files:**

- Modify: README.md
- Modify: docs/dashboard-validation.md

**Interfaces:**

Document the dashboard task view as a read-only extension of worker dashboard:

- the tasks table shows title, agent, state, worker, runner state, turn count, last outcome, run position, branch, freshness, and age;
- filters are local browser filters for run/state/worker/agent;
- run cards show top-level and per-run progress;
- queue rows show batch/task-turn kind, pin, run cap, FIFO position, and the phase-4 blocking reason;
- task detail shows safe result summary, questions, changed files, diff stat, base/head IDs, turn timeline, runner liveness, and worker task fetch <task-id>;
- the active task log uses one-second cursor polling while the snapshot remains two seconds;
- task list JSON and snapshot projection fields are aligned;
- the dashboard never starts/reconciles/recoveries/cancels/closes/fetches/says, stores no database, and exposes no prompt/env/path/credential fields;
- application log content is trusted text for the local operator and is never treated as HTML.

Extend docs/dashboard-validation.md without overwriting the existing v1 evidence. Add a Phase 5e automated section that records the actual focused Rust/browser test counts and the passed format/lint/build/diff checks. Add a sanitized live acceptance matrix covering:

~~~text
1. loopback endpoint and no worker-side listener
2. idle three-worker cards with current readiness
3. three active task turns mapped to titles/agents and worker cards
4. queued task-turn and batch rows with FIFO position, pin/run-cap fields, and blocking reason
5. browser run/state/worker/agent filters without a network mutation
6. task detail result card, questions, changed files, diff stat, and turn timeline
7. active-turn stdout/stderr reconnect with exact 1-second cursors and no duplication
8. stale remote status and dead runner presentation without recovery
9. dashboard shutdown preserving task/turn status and zero mutation
10. non-loopback rejection, privacy scan, and CLI/snapshot JSON parity
~~~

The live record may include only shortened IDs, state names, counts, durations, outcome categories, revision/protocol match, and pass/fail results. It must not include worker hostnames or SSH strings, local paths, prompts, environment values, tokens, session references, raw logs, or raw diagnostics.

- [ ] **Step 1: Add the documentation acceptance checklist**

Update the README’s Local dashboard section and replace the Phase-5 sentence that calls the dashboard tasks view a later phase with the implemented phase-5e boundary. Add the projection/privacy/polling/route behavior above. Append the automated and live acceptance headings to docs/dashboard-validation.md; leave pre-existing v1 counts and evidence intact and do not invent live results before the operator runs them.

- [ ] **Step 2: Verify the documentation and privacy contract locally**

Run:

~~~bash
rg -n "tasks view|task list|task detail|run progress|blocking reason|one second|read-only|prompt|environment|local path" README.md docs/dashboard-validation.md
node --test tests/dashboard_client.mjs
cargo test --locked --test task_view --test dashboard_tasks --test dashboard_web -- --nocapture
~~~

Expected: the documented task-view boundary and safe-field rules are present, browser regressions pass, and focused Rust parity/privacy/route tests pass. No live worker command is run.

- [ ] **Step 3: Run the sanitized three-worker acceptance as an operator-only gate**

After rebasing the implementation branch onto the prerequisite main, the operator may install the matched release helper on all three workers and execute the phase-5e matrix with isolated local state and a disposable project. Exercise only the existing task submission/list/status/log/result/fetch lifecycle needed to create the observations; use worker dashboard --no-open, the loopback URL, and the browser suite. Verify that the dashboard’s task rows agree with worker task list --json, that active turn logs advance without duplicate bytes, that a deliberate isolated transport outage marks rows stale without recovery, and that stopping the dashboard leaves task/turn status unchanged. Record only the sanitized evidence fields listed above in docs/dashboard-validation.md. This step is explicitly not run from the current plan worktree.

- [ ] **Step 4: Run the final local gate without workers**

Run:

~~~bash
cargo fmt --all --check
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked --release
node --test tests/dashboard_client.mjs
git diff --check
~~~

Expected: all tests pass, clippy emits no warnings, the release build succeeds, the browser suite passes, and the diff contains only sanitized documentation changes for this task.

- [ ] **Step 5: Commit the documentation and acceptance record**

~~~bash
git add README.md docs/dashboard-validation.md
git commit -m "docs: record dashboard tasks acceptance"
~~~

## Delivery Boundary

After this plan is implemented and its final gate is recorded:

- worker dashboard remains a local loopback observer but now shows every locally known task and run with safe task metadata, freshness, runner state, worker mapping, turn count, last outcome, run position, branch, and progress.
- The queue view retains Phase-4 FIFO order and producer-owned blocking reasons while distinguishing batch entries from task turns and showing pins/run caps.
- The task detail view exposes safe turn outcomes, result summary, questions, repository-relative changed files, diff stat, runner liveness, task/run IDs, base/head IDs, normalized turn timeline, exact fetch command, and cursor-based raw application logs.
- worker task list --json and the dashboard snapshot serialize the same shared task/run projection fields; the CLI adds only its protocol envelope.
- The dashboard has no cancel/retry/reconcile/recover/close/say/fetch controls, no task mutation endpoint, no database, no remote dashboard listener, and no secret/prompt/path projection.
- Existing legacy job cards, job detail, job log routes, queue adapter behavior, Host validation, security headers, snapshot coalescing, stale/offline worker rules, and read-only shutdown behavior remain supported.
- Per-agent event persistence, dashboard mutation controls, live push transport, task result fetching from the browser, and any richer server-side event stream remain outside this phase. The normalized timeline is deliberately limited to lifecycle facts already persisted in TurnSummary.

## Plan Self-Review Results

### Spec coverage

- Sections 7.1 and 19 are covered by the shared CLI/snapshot projection, worker active-task cards, tasks/runs table, queue metadata, task detail/result card, normalized timeline, and one-second active-turn logs.
- Sections 8.1–8.3 are covered by the task-owned turn mapping, read-only collection rules, remote/local authority order, stale fallback, dead-runner presentation, and absence of runner recovery.
- Sections 12.1–12.2 and 13 are covered by terminal turn fields, task states/outcomes, run progress, per-worker FIFO rows, pins, run caps, and producer-owned blocking reasons.
- Section 20.1 is covered by typed fakes, fixture snapshots, browser tests, safe text rendering, privacy recursion, CLI/snapshot parity, and zero-mutation assertions.
- Section 20.2 and 21 are covered by the operator-only three-worker matrix, sanitized validation record, local final gate, and phase-5e delivery boundary.
- The existing local dashboard design is preserved through loopback-only binding, polling, coalesced refresh, stale/offline semantics, bounded cursor logs, security headers, legacy routes, and no control plane.

### Placeholder and scope review

The plan has concrete target files, public field names, route paths, test assertions, commands, expected outcomes, and commit commands for every implementation task. It contains no unresolved implementation placeholder and does not authorize worker contact from this worktree.

### Type consistency

TaskId and RunId remain the existing canonical UUID wrappers; TurnId remains the existing JobId alias; BaseOid and BranchName remain typed task values; TaskState, TaskOutcome, TurnTerminal, and RunnerState are reused rather than duplicated. The shared projection is the only source of list/run JSON fields, while DashboardSnapshot flattens it and preserves existing v1 fields. Legacy job log routes continue to use JobId; task-turn log routes validate the task-to-turn relationship before calling the task source.

### Ambiguity resolution

The task records currently persist turn summaries and lifecycle timestamps, not a timestamped copy of every agent event. Therefore the normalized event timeline is defined as one ordered lifecycle event per persisted TurnSummary, with optional start/end timestamps, terminal state, outcome, commit flag, and truncation flag. Raw stdout/stderr remains a separate bounded text log; no event timestamps or parsed markup are invented. A task-turn queue row’s opaque job_id is resolved to its task only through ClientStateStore::turn_ids_for_task, so the dashboard never mistakes a turn ID for a task ID. CLI/snapshot parity is achieved by flattening the same TaskListProjection; only the CLI’s protocol-version envelope differs.
