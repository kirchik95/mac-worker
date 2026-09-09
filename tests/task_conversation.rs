#[allow(dead_code)]
mod support;

use std::{
    ffi::OsStr, os::unix::process::ExitStatusExt, path::PathBuf, process::ExitStatus, sync::Mutex,
    time::Duration,
};

use mac_worker::{
    agent::{AgentKind, Question},
    client_state::ClientStateStore,
    config::Config,
    error::WorkerError,
    job::{
        AdmissionObservation, CommandSummary, JobId, ProcessIdentity, QueueEntry, QueueEntryKind,
        QueueRunReference, QueueState, RunId,
    },
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    project_state::ProjectState,
    scheduler::{
        AffinityHints, CandidateObservation, CandidateRejection, CandidateSlot, SchedulerPolicy,
        Selection, WorkerPreference,
    },
    supervisor::{
        ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
    },
    task::{
        BaseOid, ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, RunId as TaskRunId,
        RunRecord, RunnerIdentity, TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome,
        TaskSource, TaskState, TaskStatus, TurnSummary, TurnTerminal,
    },
    task_client::{TaskClient, TaskListFilter, WaitSelector},
    task_store::{TaskCloseRequest, TaskCloseResponse, TaskStatusResponse},
    transfer::HostOperation,
    turn_runner::InlineRunnerExecutor,
};
use tempfile::TempDir;
use uuid::Uuid;

use mac_worker::job::RunId as QueueRunId;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

static CURRENT_DIR_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy)]
struct LiveOwners;

impl ProcessInspector for LiveOwners {
    fn identity_for_pid(
        &self,
        pid: u32,
    ) -> Result<ProcessIdentity, mac_worker::error::WorkerError> {
        ProcessIdentity::new(pid, u64::from(pid) * 10_000 + 7)
    }

    fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
        ProcessObservation::Matching {
            process_group: expected.pid(),
        }
    }

    fn observe_group(&self, _process_group: u32) -> ProcessGroupObservation {
        ProcessGroupObservation::Ambiguous
    }

    fn observe_group_members(&self, _leader: u32) -> ProcessGroupMembership {
        ProcessGroupMembership::Ambiguous
    }
}

struct Fixture {
    _temp: TempDir,
    store: ClientStateStore,
}

#[derive(Clone, Copy)]
struct DeadOwnerInspector {
    dead_owner: ProcessIdentity,
}

impl ProcessInspector for DeadOwnerInspector {
    fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
        ProcessIdentity::new(pid, u64::from(pid) * 10_000 + 7)
    }

    fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
        if expected == self.dead_owner {
            ProcessObservation::Absent
        } else {
            ProcessObservation::Matching {
                process_group: expected.pid(),
            }
        }
    }

    fn observe_group(&self, _process_group: u32) -> ProcessGroupObservation {
        ProcessGroupObservation::Ambiguous
    }

    fn observe_group_members(&self, _leader: u32) -> ProcessGroupMembership {
        ProcessGroupMembership::Ambiguous
    }
}

struct CurrentDirGuard {
    previous: PathBuf,
}

impl CurrentDirGuard {
    fn enter(path: &std::path::Path) -> Self {
        let previous = std::env::current_dir().unwrap();
        std::env::set_current_dir(path).unwrap();
        Self { previous }
    }
}

impl Drop for CurrentDirGuard {
    fn drop(&mut self) {
        std::env::set_current_dir(&self.previous).unwrap();
    }
}

struct TaskRemoteRunner {
    status: Mutex<TaskStatus>,
}

impl TaskRemoteRunner {
    fn new(status: TaskStatus) -> Self {
        Self {
            status: Mutex::new(status),
        }
    }
}

impl ProcessRunner for TaskRemoteRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/git") {
            return SystemProcessRunner.run(request);
        }
        if request.program != OsStr::new("/usr/bin/ssh") {
            return Err(WorkerError::Protocol("unexpected fixture process".into()));
        }
        let operation = request
            .args
            .last()
            .and_then(|argument| argument.to_str())
            .unwrap_or_default();
        match operation {
            value if value == HostOperation::TaskStatus.command() => {
                let status = self.status.lock().unwrap().clone();
                canonical_process(&TaskStatusResponse::new(status))
            }
            value if value == HostOperation::TaskClose.command() => {
                let close: TaskCloseRequest = decode_request(request)?;
                let current = self.status.lock().unwrap().clone();
                let next = TaskStatus::new(
                    if close.discard() {
                        TaskState::Abandoned
                    } else {
                        TaskState::Closed
                    },
                    current.last_outcome().cloned(),
                    current.worker().map(str::to_owned),
                    current.session_present(),
                    current.head_oid().cloned(),
                    current.summary().map(str::to_owned),
                    current.questions().to_vec(),
                    current.files_changed().to_vec(),
                    current.diff_stat().map(str::to_owned),
                    current.turns().to_vec(),
                    current.updated_at_millis() + 1,
                )?;
                *self.status.lock().unwrap() = next.clone();
                canonical_process(&TaskCloseResponse::new(next))
            }
            other => Err(WorkerError::Protocol(format!(
                "unexpected fixture worker operation: {other}"
            ))),
        }
    }
}

fn decode_request<T: serde::de::DeserializeOwned>(
    request: &ProcessRequest,
) -> Result<T, WorkerError> {
    serde_json::from_slice(
        request
            .stdin
            .as_deref()
            .ok_or_else(|| WorkerError::Protocol("fixture worker request had no stdin".into()))?,
    )
    .map_err(|error| WorkerError::Protocol(error.to_string()))
}

fn canonical_process<T: serde::Serialize>(value: &T) -> Result<ProcessResult, WorkerError> {
    let mut stdout =
        serde_json::to_vec(value).map_err(|error| WorkerError::Protocol(error.to_string()))?;
    stdout.push(b'\n');
    Ok(ProcessResult {
        status: ExitStatus::from_raw(0),
        stdout,
        stderr: Vec::new(),
    })
}

fn task_config() -> Config {
    Config::parse("version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n")
        .unwrap()
}

fn task_record(
    task_id: TaskId,
    turn_id: JobId,
    state: TaskState,
    project_id: String,
    worktree_id: String,
    worker: &str,
    runner: Option<ProcessIdentity>,
) -> LocalTaskRecord {
    task_record_in_run(
        task_id,
        turn_id,
        state,
        project_id,
        worktree_id,
        worker,
        runner,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn task_record_in_run(
    task_id: TaskId,
    turn_id: JobId,
    state: TaskState,
    project_id: String,
    worktree_id: String,
    worker: &str,
    runner: Option<ProcessIdentity>,
    run_id: Option<TaskRunId>,
) -> LocalTaskRecord {
    let base_oid: BaseOid = "a".repeat(40).parse().unwrap();
    let meta = TaskMeta::new(TaskMetaInput {
        task_id,
        run_id,
        project_id,
        worktree_id,
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        policy: mac_worker::agent::PermissionPolicy::Workspace,
        source: TaskSource::Local {
            wip: false,
            push_target: None,
        },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid: base_oid.clone(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("mac-worker", "mac-worker@example.test").unwrap(),
        title: None,
        prompt: "fixture task".into(),
        created_at_millis: 1,
    })
    .unwrap();
    let terminal = (state != TaskState::Active).then_some(TurnTerminal::Succeeded);
    let outcome = terminal.map(|_| TaskOutcome::Done);
    let turn = TurnSummary::new(
        1,
        turn_id,
        terminal,
        outcome.clone(),
        terminal.map(|_| true),
        false,
        Some(1),
        terminal.map(|_| 2),
    );
    let status = TaskStatus::new(
        state,
        outcome,
        Some(worker.into()),
        state != TaskState::Active,
        Some(base_oid),
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![turn],
        2,
    )
    .unwrap();
    LocalTaskRecord::new(
        meta,
        status,
        None,
        runner.map(RunnerIdentity::new),
        None,
        PROJECT_ID.into(),
        None,
        true,
        None,
    )
    .unwrap()
}

fn origin_queued_task_record(
    task_id: TaskId,
    turn_id: JobId,
    project_id: String,
    worktree_id: String,
    runner: ProcessIdentity,
) -> LocalTaskRecord {
    let base_oid: BaseOid = "a".repeat(40).parse().unwrap();
    let meta = TaskMeta::new(TaskMetaInput {
        task_id,
        run_id: None,
        project_id,
        worktree_id,
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        policy: mac_worker::agent::PermissionPolicy::Workspace,
        source: TaskSource::Origin {
            url: "https://origin.example.test/repo.git".into(),
        },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid: base_oid.clone(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("mac-worker", "mac-worker@example.test").unwrap(),
        title: None,
        prompt: "fixture origin task".into(),
        created_at_millis: 1,
    })
    .unwrap();
    let pending = TurnSummary::new(1, turn_id, None, None, None, false, Some(1), None);
    let status = TaskStatus::new(
        TaskState::Queued,
        None,
        Some("mini-1".into()),
        false,
        Some(base_oid),
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![pending],
        1,
    )
    .unwrap();
    LocalTaskRecord::new(
        meta,
        status,
        None,
        Some(RunnerIdentity::new(runner)),
        None,
        PROJECT_ID.into(),
        None,
        true,
        None,
    )
    .unwrap()
}

impl Fixture {
    fn open() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap().join("state");
        let store = ClientStateStore::open_with_owner_inspector(&root, LiveOwners).unwrap();
        Self { _temp: temp, store }
    }
}

fn owner(pid: u32) -> ProcessIdentity {
    ProcessIdentity::new(pid, u64::from(pid) * 10_000 + 7).unwrap()
}

fn job(number: u128) -> JobId {
    JobId::new(Uuid::from_u128(number))
}

fn cache_idle(store: &ClientStateStore, worker: &str, now: u64) {
    let observation = AdmissionObservation::new(
        worker.to_owned(),
        true,
        CandidateSlot::Idle,
        Vec::new(),
        Some(8 * 1024 * 1024 * 1024),
        64 * 1024 * 1024 * 1024,
        now,
    )
    .unwrap();
    store
        .admission_observation(worker, now, || Ok(observation))
        .unwrap();
}

fn cache_busy(store: &ClientStateStore, worker: &str, now: u64) {
    let observation = AdmissionObservation::new(
        worker.to_owned(),
        true,
        CandidateSlot::Busy,
        Vec::new(),
        Some(8 * 1024 * 1024 * 1024),
        64 * 1024 * 1024 * 1024,
        now,
    )
    .unwrap();
    store
        .admission_observation(worker, now, || Ok(observation))
        .unwrap();
}

fn turn(
    store: &ClientStateStore,
    number: u128,
    at: u64,
    row_owner: ProcessIdentity,
    preference: WorkerPreference,
    run: Option<QueueRunReference>,
) -> QueueEntry {
    QueueEntry::new(
        job(number),
        store.client_id(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        CommandSummary::argv(2).unwrap(),
        Vec::new(),
        preference,
        QueueEntryKind::TaskTurn,
        run,
        row_owner,
        at,
    )
    .unwrap()
}

fn active_task(store: &ClientStateStore, task_number: u128, turn_id: JobId) {
    let task_id = TaskId::new(Uuid::from_u128(task_number));
    let base_oid: BaseOid = "a".repeat(40).parse().unwrap();
    let meta = TaskMeta::new(TaskMetaInput {
        task_id,
        run_id: Some(TaskRunId::new(Uuid::from_u128(99))),
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
        agent: mac_worker::agent::AgentKind::Codex,
        model: None,
        effort: None,
        policy: mac_worker::agent::PermissionPolicy::Workspace,
        source: TaskSource::Local {
            wip: false,
            push_target: None,
        },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid: base_oid.clone(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("mac-worker", "mac-worker@example.test").unwrap(),
        title: None,
        prompt: "active task".into(),
        created_at_millis: 1,
    })
    .unwrap();
    let pending = TurnSummary::new(1, turn_id, None, None, None, false, Some(1), None);
    let status = TaskStatus::new(
        TaskState::Active,
        None,
        Some("mini-1".into()),
        false,
        Some(base_oid),
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![pending],
        1,
    )
    .unwrap();
    store
        .create_task(
            LocalTaskRecord::new(
                meta,
                status,
                None,
                None,
                None,
                PROJECT_ID.into(),
                None,
                true,
                None,
            )
            .unwrap(),
        )
        .unwrap();
}

#[test]
fn task_list_filters_by_last_outcome_for_the_orchestrator_loop() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    let _current_dir = CurrentDirGuard::enter(repo.root());
    let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let state_root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
    let store = ClientStateStore::open_with_owner_inspector(&paths.state, LiveOwners).unwrap();

    let done = task_record(
        TaskId::new(Uuid::from_u128(950)),
        job(951),
        TaskState::Open,
        project.context.project_id.clone(),
        project.context.worktree_id.clone(),
        "mini-1",
        None,
    );
    let waiting_id = TaskId::new(Uuid::from_u128(952));
    let waiting = task_record(
        waiting_id,
        job(953),
        TaskState::Open,
        project.context.project_id.clone(),
        project.context.worktree_id.clone(),
        "mini-1",
        None,
    );
    let waiting_status = TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::NeedsInput),
        Some("mini-1".into()),
        true,
        None,
        Some("pick a base".into()),
        vec![Question::new(
            "Which base?",
            vec!["main".into(), "release".into()],
        )],
        Vec::new(),
        None,
        Vec::new(),
        3,
    )
    .unwrap();
    let waiting = waiting.with_status(waiting_status.clone()).unwrap();
    store.create_task(done).unwrap();
    store.create_task(waiting).unwrap();

    let remote = TaskRemoteRunner::new(waiting_status);
    let config = task_config();
    let executor = InlineRunnerExecutor;
    let client = TaskClient::new(&remote, &config, &paths, &store, &executor);

    let all = client.list(TaskListFilter::default()).unwrap();
    assert_eq!(all.tasks().len(), 2);

    let waiting_rows = client
        .list(TaskListFilter {
            outcome: Some("needs_input".into()),
            ..TaskListFilter::default()
        })
        .unwrap();
    assert_eq!(waiting_rows.tasks().len(), 1);
    assert_eq!(waiting_rows.tasks()[0].task_id, waiting_id);

    let none = client
        .list(TaskListFilter {
            state: Some(TaskState::Active),
            outcome: Some("needs_input".into()),
            ..TaskListFilter::default()
        })
        .unwrap();
    assert!(none.tasks().is_empty());
}

#[test]
fn task_list_state_filter_keeps_the_active_row_of_a_mixed_run() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    let _current_dir = CurrentDirGuard::enter(repo.root());
    let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let state_root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
    let store = ClientStateStore::open_with_owner_inspector(&paths.state, LiveOwners).unwrap();

    let run_id = TaskRunId::new(Uuid::from_u128(99));
    let active_id = TaskId::new(Uuid::from_u128(960));
    let closed_id = TaskId::new(Uuid::from_u128(961));
    store
        .create_run(
            RunRecord::new(
                run_id,
                Some("polish-2026-09-10".into()),
                vec![active_id, closed_id],
                1,
                1,
            )
            .unwrap(),
        )
        .unwrap();
    store
        .create_task(task_record_in_run(
            active_id,
            job(962),
            TaskState::Active,
            project.context.project_id.clone(),
            project.context.worktree_id.clone(),
            "mini-1",
            None,
            Some(run_id),
        ))
        .unwrap();
    store
        .create_task(task_record_in_run(
            closed_id,
            job(963),
            TaskState::Closed,
            project.context.project_id.clone(),
            project.context.worktree_id.clone(),
            "mini-1",
            None,
            Some(run_id),
        ))
        .unwrap();

    let remote = TaskRemoteRunner::new(store.load_task(active_id).unwrap().status().clone());
    let config = task_config();
    let executor = InlineRunnerExecutor;
    let client = TaskClient::new(&remote, &config, &paths, &store, &executor);

    let filtered = client
        .list(TaskListFilter {
            state: Some(TaskState::Active),
            ..TaskListFilter::default()
        })
        .unwrap();
    assert_eq!(filtered.tasks().len(), 1);
    assert_eq!(filtered.tasks()[0].task_id, active_id);
    assert_eq!(filtered.tasks()[0].run_position, Some(1));
}

#[test]
fn reconcile_requeues_fetch_only_origin_task_with_pinned_origin_requirement() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    let _current_dir = CurrentDirGuard::enter(repo.root());
    let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let state_root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
    let task_id = TaskId::new(Uuid::from_u128(900));
    let turn_id = job(901);
    let record = origin_queued_task_record(
        task_id,
        turn_id,
        project.context.project_id.clone(),
        project.context.worktree_id.clone(),
        owner(900),
    );
    let remote = TaskRemoteRunner::new(record.status().clone());
    let store = ClientStateStore::open_with_owner_inspector(&paths.state, LiveOwners).unwrap();
    store.create_task(record).unwrap();
    store
        .write_turn_prompt(task_id, turn_id, "fixture prompt")
        .unwrap();

    let config = task_config();
    let executor = InlineRunnerExecutor;
    TaskClient::new(&remote, &config, &paths, &store, &executor)
        .reconcile_runners()
        .unwrap();

    let entry = store
        .queue_entry_for_task_turn(task_id)
        .unwrap()
        .expect("missing queued turn must be reconstructed");
    assert!(
        entry
            .requirements()
            .contains(&"origin:origin.example.test".to_owned())
    );

    let worker_without_origin = CandidateObservation::new(
        "mini-1".into(),
        true,
        CandidateSlot::Idle,
        entry
            .requirements()
            .iter()
            .filter(|requirement| requirement.as_str() != "origin:origin.example.test")
            .cloned()
            .collect(),
        Some(8 * 1024 * 1024 * 1024),
        64 * 1024 * 1024 * 1024,
    )
    .unwrap();
    assert!(matches!(
        SchedulerPolicy::select(
            &[worker_without_origin],
            entry.requirements(),
            entry.preference(),
            &AffinityHints::none(),
        ),
        Selection::NoEligible { rejections }
            if rejections == vec![CandidateRejection::MissingCapabilities {
                name: "mini-1".into(),
                missing: vec!["origin:origin.example.test".into()],
            }]
    ));
}

#[test]
fn reconcile_removes_dead_dispatching_terminal_task_turn_before_close() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    let _current_dir = CurrentDirGuard::enter(repo.root());
    let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let state_root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
    let dead_owner = owner(901);
    let task_id = TaskId::new(Uuid::from_u128(901));
    let turn_id = job(902);
    let record = task_record(
        task_id,
        turn_id,
        TaskState::Open,
        project.context.project_id.clone(),
        project.context.worktree_id.clone(),
        "mini-1",
        None,
    );
    let remote = TaskRemoteRunner::new(record.status().clone());
    let store = ClientStateStore::open_with_owner_inspector(
        &paths.state,
        DeadOwnerInspector { dead_owner },
    )
    .unwrap();
    store.create_task(record).unwrap();
    store
        .write_turn_prompt(task_id, turn_id, "fixture prompt")
        .unwrap();
    cache_idle(&store, "mini-1", 10);
    store
        .enqueue(turn(
            &store,
            902,
            10,
            dead_owner,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            None,
        ))
        .unwrap();
    assert!(matches!(
        store
            .claim_next(dead_owner, &["mini-1".into()], 11)
            .unwrap()
            .unwrap()
            .entry()
            .state(),
        QueueState::Dispatching { .. }
    ));

    // Task status alone is not completion: model the completed runner journal.
    store.open_runner_log(task_id, turn_id).unwrap();
    let checkpoint = paths
        .state
        .join("runners")
        .join(task_id.to_string())
        .join(format!("{turn_id}.checkpoint.json"));
    std::fs::write(&checkpoint,serde_json::to_vec(&serde_json::json!({"version":1,"task_id":task_id,"turn_id":turn_id,"committed":{"offsets":[0,0],"len":0,"accepted":true,"completion":{"outcome":{"kind":"done"},"drained":true}},"pending":null})).unwrap()).unwrap();
    std::fs::set_permissions(
        &checkpoint,
        <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o600),
    )
    .unwrap();
    let config = task_config();
    let executor = InlineRunnerExecutor;
    let client = TaskClient::new(&remote, &config, &paths, &store, &executor);
    let report = client.reconcile_runners().unwrap();

    assert_eq!(report.repaired_rows(), 1);
    assert!(store.queue_entry_for_task_turn(task_id).unwrap().is_none());
    let closed = client.close(task_id, true).unwrap();
    assert_eq!(closed.status().state(), TaskState::Abandoned);
}

#[test]
fn reconcile_leaves_dispatching_task_turn_with_live_owner_untouched() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    let _current_dir = CurrentDirGuard::enter(repo.root());
    let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let state_root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
    let live_owner = owner(903);
    let dead_owner = owner(904);
    let task_id = TaskId::new(Uuid::from_u128(903));
    let turn_id = job(904);
    let record = task_record(
        task_id,
        turn_id,
        TaskState::Open,
        project.context.project_id.clone(),
        project.context.worktree_id.clone(),
        "mini-1",
        None,
    );
    let remote = TaskRemoteRunner::new(record.status().clone());
    let store = ClientStateStore::open_with_owner_inspector(
        &paths.state,
        DeadOwnerInspector { dead_owner },
    )
    .unwrap();
    store.create_task(record).unwrap();
    store
        .write_turn_prompt(task_id, turn_id, "fixture prompt")
        .unwrap();
    cache_idle(&store, "mini-1", 10);
    store
        .enqueue(turn(
            &store,
            904,
            10,
            live_owner,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            None,
        ))
        .unwrap();
    store
        .claim_next(live_owner, &["mini-1".into()], 11)
        .unwrap()
        .unwrap();

    let config = task_config();
    let executor = InlineRunnerExecutor;
    let client = TaskClient::new(&remote, &config, &paths, &store, &executor);
    client.reconcile_runners().unwrap();

    let entry = store.queue_entry(turn_id).unwrap().unwrap();
    assert_eq!(entry.owner_opt(), Some(&live_owner));
    assert!(matches!(entry.state(), QueueState::Dispatching { .. }));
}

#[test]
fn wait_does_not_return_while_a_finished_turn_is_still_dispatching() {
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    let _current_dir = CurrentDirGuard::enter(repo.root());
    let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let state_root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
    let live_owner = owner(907);
    let dead_owner = owner(908);
    let task_id = TaskId::new(Uuid::from_u128(907));
    let turn_id = job(908);
    let record = task_record(
        task_id,
        turn_id,
        TaskState::Open,
        project.context.project_id.clone(),
        project.context.worktree_id.clone(),
        "mini-1",
        Some(live_owner),
    );
    let remote = TaskRemoteRunner::new(record.status().clone());
    let store = ClientStateStore::open_with_owner_inspector(
        &paths.state,
        DeadOwnerInspector { dead_owner },
    )
    .unwrap();
    store.create_task(record).unwrap();
    store
        .write_turn_prompt(task_id, turn_id, "fixture prompt")
        .unwrap();
    cache_idle(&store, "mini-1", 10);
    store
        .enqueue(turn(
            &store,
            908,
            10,
            live_owner,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            None,
        ))
        .unwrap();
    store
        .claim_next(live_owner, &["mini-1".into()], 11)
        .unwrap()
        .unwrap();
    assert!(matches!(
        store.queue_entry(turn_id).unwrap().unwrap().state(),
        QueueState::Dispatching { .. }
    ));

    let config = task_config();
    let executor = InlineRunnerExecutor;
    let client = TaskClient::new(&remote, &config, &paths, &store, &executor);
    let error = client
        .wait(
            WaitSelector::Task(task_id),
            Some(Duration::from_millis(350)),
        )
        .unwrap_err();
    assert_eq!(error.public_code(), "WAIT_TIMEOUT");
    assert!(matches!(
        store.queue_entry(turn_id).unwrap().unwrap().state(),
        QueueState::Dispatching { .. }
    ));

    store
        .remove_task_turn_after_terminal(turn_id, live_owner)
        .unwrap();
    let error = client
        .wait(
            WaitSelector::Task(task_id),
            Some(Duration::from_millis(350)),
        )
        .unwrap_err();
    assert_eq!(error.public_code(), "WAIT_TIMEOUT");
    assert!(store.load_task(task_id).unwrap().runner().is_some());
    let error = client.close(task_id, true).unwrap_err();
    assert_eq!(error.public_code(), "TASK_BUSY");
    assert_eq!(error.public_message(), "task runner is still finishing");

    store.record_runner(task_id, None).unwrap();
    let waited = client
        .wait(WaitSelector::Task(task_id), Some(Duration::from_secs(2)))
        .unwrap();
    assert_eq!(waited.exit_code(), 0);
    assert_eq!(waited.task_ids(), &[task_id]);
}

#[test]
fn reconcile_replaces_a_dead_runner_for_an_active_task_turn() {
    let state_root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
    let dead_owner = owner(905);
    let task_id = TaskId::new(Uuid::from_u128(905));
    let turn_id = job(906);
    let record = task_record(
        task_id,
        turn_id,
        TaskState::Active,
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        "mini-1",
        Some(dead_owner),
    );
    let remote = TaskRemoteRunner::new(record.status().clone());
    let store = ClientStateStore::open_with_owner_inspector(
        &paths.state,
        DeadOwnerInspector { dead_owner },
    )
    .unwrap();
    store.create_task(record).unwrap();
    store
        .write_turn_prompt(task_id, turn_id, "fixture prompt")
        .unwrap();
    cache_idle(&store, "mini-1", 10);
    store
        .enqueue(turn(
            &store,
            906,
            10,
            dead_owner,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            None,
        ))
        .unwrap();
    store
        .claim_next(dead_owner, &["mini-1".into()], 11)
        .unwrap()
        .unwrap();

    let config = task_config();
    let executor = InlineRunnerExecutor;
    let client = TaskClient::new(&remote, &config, &paths, &store, &executor);
    let report = client.reconcile_runners().unwrap();

    assert_eq!(report.replaced_runners(), 1);
    assert_eq!(report.started_runners(), 1);
    let entry = store.queue_entry(turn_id).unwrap().unwrap();
    assert!(matches!(entry.state(), QueueState::Dispatching { .. }));
    assert!(matches!(
        store.process_observation(*entry.owner()),
        ProcessObservation::Matching { .. }
    ));
}

#[test]
fn reconcile_does_not_replace_a_live_handoff_owner_without_runner_metadata() {
    // Break caught: a queue handoff has committed before runner metadata is
    // written, and reconciliation starts a second process for the same turn.
    struct HandoffInspector {
        ambiguous: bool,
    }
    impl ProcessInspector for HandoffInspector {
        fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
            LiveOwners.identity_for_pid(pid)
        }

        fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
            if self.ambiguous {
                ProcessObservation::Ambiguous
            } else {
                LiveOwners.observe(expected)
            }
        }

        fn observe_group(&self, group: u32) -> ProcessGroupObservation {
            LiveOwners.observe_group(group)
        }

        fn observe_group_members(&self, leader: u32) -> ProcessGroupMembership {
            LiveOwners.observe_group_members(leader)
        }
    }

    for ambiguous in [false, true] {
        for dispatching in [false, true] {
            let state_root = tempfile::tempdir().unwrap();
            let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
            let handoff_owner = owner(907);
            let task_id = TaskId::new(Uuid::from_u128(907));
            let turn_id = job(908);
            let record = task_record(
                task_id,
                turn_id,
                TaskState::Active,
                PROJECT_ID.into(),
                WORKTREE_ID.into(),
                "mini-1",
                None,
            );
            let remote = TaskRemoteRunner::new(record.status().clone());
            let store = ClientStateStore::open_with_owner_inspector(
                &paths.state,
                HandoffInspector { ambiguous },
            )
            .unwrap();
            store.create_task(record).unwrap();
            store
                .write_turn_prompt(task_id, turn_id, "fixture prompt")
                .unwrap();
            cache_idle(&store, "mini-1", 10);
            store
                .enqueue(turn(
                    &store,
                    908,
                    10,
                    handoff_owner,
                    WorkerPreference::Pinned {
                        worker: "mini-1".into(),
                    },
                    None,
                ))
                .unwrap();
            if dispatching {
                store
                    .claim_next(handoff_owner, &["mini-1".into()], 11)
                    .unwrap()
                    .unwrap();
            }
            let before = store.queue_entry(turn_id).unwrap().unwrap();
            let config = task_config();
            let report = TaskClient::new(&remote, &config, &paths, &store, &InlineRunnerExecutor)
                .reconcile_runners()
                .unwrap();
            assert_eq!(report.started_runners(), 0);
            assert!(store.load_task(task_id).unwrap().runner().is_none());
            assert_eq!(store.queue_entry(turn_id).unwrap().unwrap(), before);
        }
    }
}

#[test]
fn pinned_head_waiting_for_a_busy_worker_does_not_block_a_younger_first_turn() {
    let fixture = Fixture::open();
    cache_idle(&fixture.store, "mini-1", 10);
    cache_idle(&fixture.store, "mini-3", 10);
    cache_busy(&fixture.store, "mini-2", 10);
    let dispatcher = owner(101);

    fixture
        .store
        .enqueue(turn(
            &fixture.store,
            1,
            10,
            dispatcher,
            WorkerPreference::Pinned {
                worker: "mini-2".into(),
            },
            None,
        ))
        .unwrap();
    fixture
        .store
        .enqueue(turn(
            &fixture.store,
            2,
            11,
            dispatcher,
            WorkerPreference::Automatic,
            None,
        ))
        .unwrap();

    let claim = fixture
        .store
        .claim_next(dispatcher, &["mini-1".into(), "mini-3".into()], 12)
        .unwrap()
        .unwrap();
    assert_eq!(claim.entry().job_id(), job(2));
    assert!(
        fixture
            .store
            .claim_next(dispatcher, &["mini-1".into()], 13)
            .unwrap()
            .is_none()
    );
}

#[test]
fn a_runner_claims_only_its_own_row_and_yields_to_an_older_live_owner() {
    let fixture = Fixture::open();
    cache_idle(&fixture.store, "mini-1", 10);
    cache_idle(&fixture.store, "mini-2", 10);
    cache_idle(&fixture.store, "mini-3", 10);
    let runner_a = owner(201);
    let runner_b = owner(202);

    fixture
        .store
        .enqueue(turn(
            &fixture.store,
            1,
            10,
            runner_a,
            WorkerPreference::Automatic,
            None,
        ))
        .unwrap();
    fixture
        .store
        .enqueue(turn(
            &fixture.store,
            2,
            11,
            runner_b,
            WorkerPreference::Automatic,
            None,
        ))
        .unwrap();

    assert!(
        fixture
            .store
            .claim_next(runner_b, &["mini-1".into()], 12)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fixture
            .store
            .claim_next(runner_a, &["mini-1".into()], 13)
            .unwrap()
            .unwrap()
            .entry()
            .job_id(),
        job(1)
    );

    let parked = fixture
        .store
        .enqueue(turn(
            &fixture.store,
            3,
            14,
            runner_b,
            WorkerPreference::Automatic,
            None,
        ))
        .unwrap();
    fixture.store.park_row(parked.job_id()).unwrap();
    assert_eq!(
        fixture
            .store
            .claim_next(runner_b, &["mini-2".into()], 15)
            .unwrap()
            .unwrap()
            .entry()
            .job_id(),
        job(2)
    );
    assert!(
        fixture
            .store
            .claim_next(runner_b, &["mini-3".into()], 16)
            .unwrap()
            .is_none()
    );
}

#[test]
fn older_row_always_wins_the_same_worker() {
    let fixture = Fixture::open();
    cache_idle(&fixture.store, "mini-1", 10);
    let dispatcher = owner(301);
    fixture
        .store
        .enqueue(turn(
            &fixture.store,
            1,
            10,
            dispatcher,
            WorkerPreference::Automatic,
            None,
        ))
        .unwrap();
    fixture
        .store
        .enqueue(turn(
            &fixture.store,
            2,
            11,
            dispatcher,
            WorkerPreference::Automatic,
            None,
        ))
        .unwrap();

    assert_eq!(
        fixture
            .store
            .claim_next(dispatcher, &["mini-1".into()], 12)
            .unwrap()
            .unwrap()
            .entry()
            .job_id(),
        job(1)
    );
}

#[test]
fn sibling_runners_cannot_both_take_the_last_run_slot() {
    let fixture = Fixture::open();
    cache_idle(&fixture.store, "mini-1", 10);
    cache_idle(&fixture.store, "mini-2", 10);
    let dispatcher = owner(401);
    let run =
        QueueRunReference::new(RunId::new(Uuid::from_u128(99).to_string()).unwrap(), 1).unwrap();

    fixture
        .store
        .enqueue(turn(
            &fixture.store,
            1,
            10,
            dispatcher,
            WorkerPreference::Automatic,
            Some(run.clone()),
        ))
        .unwrap();
    fixture
        .store
        .enqueue(turn(
            &fixture.store,
            2,
            11,
            dispatcher,
            WorkerPreference::Automatic,
            Some(run),
        ))
        .unwrap();

    fixture
        .store
        .claim_next(dispatcher, &["mini-1".into()], 12)
        .unwrap()
        .unwrap();
    assert!(
        fixture
            .store
            .claim_next(dispatcher, &["mini-2".into()], 13)
            .unwrap()
            .is_none()
    );
    fixture.store.revert_dispatch(job(1), dispatcher).unwrap();
    assert_eq!(
        fixture
            .store
            .claim_next(dispatcher, &["mini-2".into()], 14)
            .unwrap()
            .unwrap()
            .entry()
            .job_id(),
        job(1)
    );
    assert!(matches!(
        fixture.store.queue_entry(job(2)).unwrap().unwrap().state(),
        QueueState::Waiting { .. }
    ));
}

#[test]
fn an_active_task_turn_consumes_one_run_slot_not_two() {
    let fixture = Fixture::open();
    cache_idle(&fixture.store, "mini-1", 10);
    cache_idle(&fixture.store, "mini-2", 10);
    let dispatcher = owner(501);
    let run_name = format!("{:x}", Uuid::from_u128(99).simple());
    let run = QueueRunReference::new(QueueRunId::new(run_name).unwrap(), 2).unwrap();

    fixture
        .store
        .enqueue(turn(
            &fixture.store,
            1,
            10,
            dispatcher,
            WorkerPreference::Automatic,
            Some(run.clone()),
        ))
        .unwrap();
    fixture
        .store
        .claim_next(dispatcher, &["mini-1".into()], 11)
        .unwrap()
        .unwrap();
    active_task(&fixture.store, 101, job(1));

    fixture
        .store
        .enqueue(turn(
            &fixture.store,
            2,
            12,
            owner(502),
            WorkerPreference::Automatic,
            Some(run),
        ))
        .unwrap();
    assert_eq!(
        fixture
            .store
            .claim_next(owner(502), &["mini-2".into()], 13)
            .unwrap()
            .unwrap()
            .entry()
            .job_id(),
        job(2)
    );
}

#[test]
fn failed_local_cancellation_keeps_a_recoverable_row_until_completion() {
    use std::io::Write;
    let _lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    let _cwd = CurrentDirGuard::enter(repo.root());
    let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
    let store = ClientStateStore::open_with_owner_inspector(&paths.state, LiveOwners).unwrap();
    let task = TaskId::generate();
    let id = job(9901);
    let own = owner(9901);
    let record = task_record(
        task,
        id,
        TaskState::Queued,
        project.context.project_id.clone(),
        project.context.worktree_id.clone(),
        "mini-1",
        Some(own),
    );
    let remote = TaskRemoteRunner::new(record.status().clone());
    store.create_task(record).unwrap();
    store.write_turn_prompt(task, id, "cancel me").unwrap();
    store
        .enqueue(turn(
            &store,
            9901,
            10,
            own,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            None,
        ))
        .unwrap();
    store
        .open_runner_log(task, id)
        .unwrap()
        .write_all(b"legacy bytes")
        .unwrap();
    let config = task_config();
    let client = TaskClient::new(&remote, &config, &paths, &store, &InlineRunnerExecutor);
    let error = client.cancel(task).unwrap_err();
    assert_eq!(error.public_code(), "LOG_CHECKPOINT_MISSING");
    let entry = store
        .queue_entry(id)
        .unwrap()
        .expect("cancellation must retain recovery row");
    assert!(entry.is_cancel_requested());
    assert!(
        store
            .claim_task_turn(own, id, &["mini-1".into()], 20)
            .unwrap()
            .is_none()
    );
    let log = paths
        .state
        .join("runners")
        .join(task.to_string())
        .join(format!("{id}.log"));
    assert_eq!(std::fs::read(&log).unwrap(), b"legacy bytes");
    // Explicit fixture repair preserves the legacy bytes separately; the runtime never migrates them.
    std::fs::rename(&log, log.with_extension("legacy")).unwrap();
    client.reconcile_runners().unwrap();
    assert!(store.queue_entry(id).unwrap().is_none());
    let sidecar = log.with_file_name(format!("{id}.checkpoint.json"));
    let journal: serde_json::Value =
        serde_json::from_slice(&std::fs::read(sidecar).unwrap()).unwrap();
    assert_eq!(
        journal["committed"]["completion"]["outcome"]["kind"],
        "cancelled"
    );
}

// Drop releases a blocked peer even if an assertion unwinds the scope. The
// receiver's deadline is a final bound against a broken fixture, not ordering.
struct FinalizerRelease(Option<std::sync::mpsc::Sender<()>>);
impl FinalizerRelease {
    fn release(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}
impl Drop for FinalizerRelease {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Clone)]
struct FinalizerInspector {
    dead: Option<ProcessIdentity>,
    observed: std::sync::Arc<Mutex<Option<std::sync::mpsc::Sender<()>>>>,
    resume: std::sync::Arc<Mutex<std::sync::mpsc::Receiver<()>>>,
}
impl ProcessInspector for FinalizerInspector {
    fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
        ProcessIdentity::new(pid, u64::from(pid) * 10_000 + 7)
    }
    fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
        if self.dead == Some(expected) {
            if std::thread::current().name() == Some("stale-finalizer")
                && let Some(sender) = self.observed.lock().unwrap().take()
            {
                sender.send(()).unwrap();
                self.resume
                    .lock()
                    .unwrap()
                    .recv_timeout(std::time::Duration::from_secs(20))
                    .expect("finalizer gate released");
            }
            ProcessObservation::Absent
        } else {
            ProcessObservation::Matching {
                process_group: expected.pid(),
            }
        }
    }
    fn observe_group(&self, _: u32) -> ProcessGroupObservation {
        ProcessGroupObservation::Ambiguous
    }
    fn observe_group_members(&self, _: u32) -> ProcessGroupMembership {
        ProcessGroupMembership::Ambiguous
    }
}
struct PausedFinalizerRemote {
    inner: TaskRemoteRunner,
    entered: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    resume: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl ProcessRunner for PausedFinalizerRemote {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if std::thread::current().name() == Some("old-finalizer")
            && request.program == OsStr::new("/usr/bin/git")
            && request.args.iter().any(|a| a == "update-ref")
            && request.args.iter().any(|a| a == "-d")
            && request
                .args
                .iter()
                .any(|a| a.to_string_lossy().starts_with("refs/mac-worker/bases/"))
            && let Some(entered) = self.entered.lock().unwrap().take()
        {
            entered.send(()).unwrap();
            self.resume
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(20))
                .expect("finalizer gate released");
        }
        self.inner.run(request)
    }
}
fn finalizer_cannot_damage_a_new_turn(mode: &str) {
    use std::{
        sync::{Arc, mpsc},
        time::Duration,
    };
    let _cwd_lock = CURRENT_DIR_LOCK.lock().unwrap();
    let repo = support::GitRepo::init();
    repo.write("base.txt", b"base");
    repo.commit_all("base");
    let _cwd = CurrentDirGuard::enter(repo.root());
    let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
    let task = TaskId::generate();
    let turn_id = JobId::generate();
    let old_owner = owner(9971);
    let (observed_tx, observed_rx) = mpsc::channel();
    let (resume_observe_tx, resume_observe_rx) = mpsc::channel();
    let inspector = FinalizerInspector {
        dead: (mode != "cancel").then_some(old_owner),
        observed: Arc::new(Mutex::new(Some(observed_tx))),
        resume: Arc::new(Mutex::new(resume_observe_rx)),
    };
    let store = ClientStateStore::open_with_owner_inspector(&paths.state, inspector).unwrap();
    let record = task_record(
        task,
        turn_id,
        TaskState::Open,
        project.context.project_id.clone(),
        project.context.worktree_id.clone(),
        "mini-1",
        Some(old_owner),
    );
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let remote = PausedFinalizerRemote {
        inner: TaskRemoteRunner::new(record.status().clone()),
        entered: Mutex::new(Some(entered_tx)),
        resume: Mutex::new(resume_rx),
    };
    store.create_task(record).unwrap();
    store.write_turn_prompt(task, turn_id, "old turn").unwrap();
    store
        .enqueue(
            QueueEntry::new(
                turn_id,
                store.client_id(),
                project.context.project_id.clone(),
                project.context.worktree_id.clone(),
                CommandSummary::argv(1).unwrap(),
                vec![],
                WorkerPreference::Pinned {
                    worker: "mini-1".into(),
                },
                QueueEntryKind::TaskTurn,
                None,
                old_owner,
                10,
            )
            .unwrap(),
        )
        .unwrap();
    cache_idle(&store, "mini-1", 10);
    if mode != "cancel" {
        store
            .claim_next(old_owner, &["mini-1".into()], 11)
            .unwrap()
            .unwrap();
        store.open_runner_log(task, turn_id).unwrap();
        let checkpoint = paths
            .state
            .join("runners")
            .join(task.to_string())
            .join(format!("{turn_id}.checkpoint.json"));
        std::fs::write(&checkpoint,serde_json::to_vec(&serde_json::json!({"version":1,"task_id":task,"turn_id":turn_id,"committed":{"offsets":[0,0],"len":0,"accepted":true,"completion":{"outcome":{"kind":"done"},"drained":true}},"pending":null})).unwrap()).unwrap();
        std::fs::set_permissions(
            checkpoint,
            <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o600),
        )
        .unwrap();
    }
    let config = task_config();
    let client = TaskClient::new(&remote, &config, &paths, &store, &InlineRunnerExecutor);
    let transfer = mac_worker::transfer_repo::TransferRepo::open_or_create(
        &paths.cache,
        &project.context.common_dir,
    )
    .unwrap();
    let transfer_path = transfer.path().to_owned();
    drop(transfer);
    let base_ref = format!("refs/mac-worker/bases/{task}");
    let head = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout).unwrap();
    let pin = || {
        assert!(
            std::process::Command::new("/usr/bin/git")
                .arg("--git-dir")
                .arg(&transfer_path)
                .args(["update-ref", &base_ref, head.trim()])
                .status()
                .unwrap()
                .success()
        );
    };
    pin();
    std::thread::scope(|scope| {
        let mut resume_old = FinalizerRelease(Some(resume_tx));
        let mut resume_stale = FinalizerRelease(Some(resume_observe_tx));
        let stale = if mode != "cancel" {
            let handle = std::thread::Builder::new()
                .name("stale-finalizer".into())
                .spawn_scoped(scope, || client.reconcile_runners())
                .unwrap();
            observed_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("stale reconciler must observe the old owner");
            Some(handle)
        } else {
            None
        };
        if mode == "retry" {
            let owner = mac_worker::turn_runner::RunnerExecutor::start(
                &InlineRunnerExecutor,
                &paths,
                task,
                turn_id,
            )
            .unwrap()
            .process_identity();
            store.adopt_row(turn_id, owner).unwrap();
        }
        let old = std::thread::Builder::new()
            .name("old-finalizer".into())
            .spawn_scoped(scope, || match mode {
                "cancel" => client.cancel(task).map(|_| ()),
                "reconcile" => client.reconcile_runners().map(|_| ()),
                "retry" => mac_worker::turn_runner::TurnRunner::new(
                    &remote,
                    &config,
                    &paths,
                    &store,
                    &InlineRunnerExecutor,
                )
                .run(task, turn_id, None)
                .map(|_| ()),
                _ => unreachable!(),
            })
            .unwrap();
        entered_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("old finalizer must pause immediately before base release");
        if let Some(stale) = stale {
            resume_stale.release();
            stale.join().unwrap().unwrap();
        } else {
            client.reconcile_runners().unwrap();
        }
        let started = match client.say(task, "next turn".into(), false, &mut vec![], &mut vec![]) {
            Ok(_) => true,
            Err(error) => {
                assert_eq!(error.public_code(), "TASK_BUSY");
                false
            }
        };
        if started {
            pin();
        }
        resume_old.release();
        old.join().unwrap().unwrap();
        if !started {
            client
                .say(task, "next turn".into(), false, &mut vec![], &mut vec![])
                .unwrap();
            pin();
        }
        let current = store.load_task(task).unwrap();
        assert!(
            current.runner().is_some(),
            "old {mode} finalizer cleared the new runner"
        );
        assert_ne!(
            store
                .queue_entry_for_task_turn(task)
                .unwrap()
                .unwrap()
                .job_id(),
            turn_id
        );
        assert!(
            std::process::Command::new("/usr/bin/git")
                .arg("--git-dir")
                .arg(&transfer_path)
                .args(["show-ref", "--verify", &base_ref])
                .output()
                .unwrap()
                .status
                .success(),
            "old {mode} finalizer released the new base"
        );
    });
}
#[test]
fn concurrent_cancel_finalizers_cannot_damage_a_new_turn() {
    finalizer_cannot_damage_a_new_turn("cancel");
}
#[test]
fn stale_completed_reconciler_cannot_damage_a_new_turn() {
    finalizer_cannot_damage_a_new_turn("reconcile");
}
#[test]
fn completed_runner_retry_fences_a_stale_reconciler() {
    finalizer_cannot_damage_a_new_turn("retry");
}
