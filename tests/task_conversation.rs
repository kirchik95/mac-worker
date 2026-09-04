#[allow(dead_code)]
mod support;

use std::{
    ffi::OsStr, os::unix::process::ExitStatusExt, path::PathBuf, process::ExitStatus, sync::Mutex,
};

use mac_worker::{
    agent::AgentKind,
    client_state::ClientStateStore,
    config::Config,
    error::WorkerError,
    job::{
        AdmissionObservation, CommandSummary, JobId, ProcessIdentity, QueueEntry, QueueEntryKind,
        QueueRunReference, QueueState, RunId,
    },
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    project_state::ProjectState,
    scheduler::{CandidateSlot, WorkerPreference},
    supervisor::{
        ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
    },
    task::{
        BaseOid, ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, RunId as TaskRunId,
        RunnerIdentity, TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome, TaskSource,
        TaskState, TaskStatus, TurnSummary, TurnTerminal,
    },
    task_client::TaskClient,
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
    let base_oid: BaseOid = "a".repeat(40).parse().unwrap();
    let meta = TaskMeta::new(TaskMetaInput {
        task_id,
        run_id: None,
        project_id,
        worktree_id,
        agent: AgentKind::Codex,
        model: None,
        policy: mac_worker::agent::PermissionPolicy::Workspace,
        source: TaskSource::Local { wip: false },
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
        policy: mac_worker::agent::PermissionPolicy::Workspace,
        source: TaskSource::Local { wip: false },
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
