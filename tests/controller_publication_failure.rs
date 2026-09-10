use std::{
    ffi::OsStr, os::unix::process::ExitStatusExt, path::Path, process::ExitStatus, sync::Mutex,
    time::Duration,
};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy},
    client_state::ClientStateStore,
    config::Config,
    error::WorkerError,
    job::JobId,
    paths::PathLayout,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    task::{
        BaseOid, ClosePolicy, DeliveryState, GitIdentity, LocalTaskRecord, OriginDelivery,
        PublishMode, TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome, TaskSource,
        TaskState, TaskStatus, TurnSummary, TurnTerminal,
    },
    task_client::{TaskClient, WaitSelector},
    task_store::TaskStatusResponse,
    transfer::HostOperation,
    turn_runner::InlineRunnerExecutor,
};
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const RESULT_OID: &str = "e00f61b2fcb717b15ab6da195cbb19c6969f1261";

struct TaskRemoteRunner {
    status: Mutex<TaskStatus>,
    deliveries: Mutex<Vec<OriginDelivery>>,
}

impl TaskRemoteRunner {
    fn new(status: TaskStatus) -> Self {
        Self {
            status: Mutex::new(status),
            deliveries: Mutex::new(Vec::new()),
        }
    }

    fn set_deliveries(&self, deliveries: Vec<OriginDelivery>) {
        *self.deliveries.lock().unwrap() = deliveries;
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
        if operation != HostOperation::TaskStatus.command() {
            return Err(WorkerError::Protocol(format!(
                "unexpected fixture worker operation: {operation}"
            )));
        }
        let status = self.status.lock().unwrap().clone();
        let deliveries = self.deliveries.lock().unwrap().clone();
        let mut stdout =
            serde_json::to_vec(&TaskStatusResponse::new(status).with_deliveries(deliveries))
                .map_err(|error| WorkerError::Protocol(error.to_string()))?;
        stdout.push(b'\n');
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout,
            stderr: Vec::new(),
        })
    }
}

fn task_config() -> Config {
    Config::parse("version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n")
        .unwrap()
}

fn isolated_paths(root: impl AsRef<Path>) -> PathLayout {
    let root = root.as_ref();
    PathLayout {
        config: root.join("config.toml"),
        state: root.join("state"),
        cache: root.join("cache"),
        data: root.join("data"),
    }
}

fn task_id(n: u128) -> TaskId {
    TaskId::new(Uuid::from_u128(n))
}

fn turn_id(n: u128) -> JobId {
    JobId::new(Uuid::from_u128(n))
}

fn result_oid() -> BaseOid {
    RESULT_OID.parse().unwrap()
}

fn meta(task: TaskId) -> TaskMeta {
    TaskMeta::new(TaskMetaInput {
        task_id: task,
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        policy: PermissionPolicy::Workspace,
        source: TaskSource::Local {
            wip: false,
            push_target: None,
        },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid: "a".repeat(40).parse().unwrap(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("mac-worker", "mac-worker@example.test").unwrap(),
        title: None,
        prompt: "fixture task".into(),
        created_at_millis: 1,
    })
    .unwrap()
}

fn open_failed_record(task: TaskId, turn: JobId, reason: &str) -> LocalTaskRecord {
    let outcome = TaskOutcome::failed(reason);
    let turn = TurnSummary::new(
        1,
        turn,
        Some(TurnTerminal::Succeeded),
        Some(outcome.clone()),
        Some(true),
        false,
        Some(1),
        Some(2),
    );
    let status = TaskStatus::new(
        TaskState::Open,
        Some(outcome),
        Some("mini-1".into()),
        true,
        Some(result_oid()),
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![turn],
        2,
    )
    .unwrap();
    LocalTaskRecord::new(
        meta(task),
        status,
        None,
        None,
        None,
        PROJECT_ID.into(),
        None,
        true,
        None,
    )
    .unwrap()
}

fn remote_done_status(turn: JobId, updated_at_millis: u64) -> TaskStatus {
    let turn = TurnSummary::new(
        1,
        turn,
        Some(TurnTerminal::Succeeded),
        Some(TaskOutcome::Done),
        Some(true),
        false,
        Some(1),
        Some(updated_at_millis),
    );
    TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Done),
        Some("mini-1".into()),
        true,
        Some(result_oid()),
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![turn],
        updated_at_millis,
    )
    .unwrap()
}

fn wait_once(
    remote: &TaskRemoteRunner,
    paths: &PathLayout,
    store: &ClientStateStore,
    task: TaskId,
) -> mac_worker::task_client::WaitReport {
    let config = task_config();
    let client = TaskClient::new(remote, &config, paths, store, &InlineRunnerExecutor);
    client
        .wait(WaitSelector::Task(task), Some(Duration::from_secs(2)))
        .unwrap()
}

fn wait_planted(
    record: LocalTaskRecord,
    remote: &TaskRemoteRunner,
) -> (mac_worker::task_client::WaitReport, LocalTaskRecord) {
    let state_root = tempfile::tempdir().unwrap();
    let paths = isolated_paths(state_root.path().canonicalize().unwrap());
    let store = ClientStateStore::open(&paths.state).unwrap();
    let task_id = record.meta().task_id();
    store.create_task(record).unwrap();
    assert!(store.queue_entry_for_task_turn(task_id).unwrap().is_none());
    let waited = wait_once(remote, &paths, &store, task_id);
    let saved = store.load_task(task_id).unwrap();
    assert!(saved.runner().is_none());
    (waited, saved)
}

#[test]
fn wait_exits_1_and_keeps_publish_failed_when_remote_is_done() {
    let task = task_id(0x11);
    let turn = turn_id(0x12);
    let record = open_failed_record(task, turn, "PUBLISH_FAILED");
    let delivery = OriginDelivery::new(
        turn,
        DeliveryState::Pending,
        result_oid(),
        "https://example.test/repo.git".into(),
        "refs/heads/release-candidate".into(),
        1,
        1,
        None,
        None,
        1,
        4,
    )
    .unwrap();
    let remote = TaskRemoteRunner::new(remote_done_status(turn, 9));
    remote.set_deliveries(vec![delivery.clone()]);

    let state_root = tempfile::tempdir().unwrap();
    let paths = isolated_paths(state_root.path().canonicalize().unwrap());
    let store = ClientStateStore::open(&paths.state).unwrap();
    store.create_task(record).unwrap();
    let waited = wait_once(&remote, &paths, &store, task);
    assert_eq!(waited.exit_code(), 1);
    assert_eq!(waited.task_ids(), &[task]);

    let saved = store.load_task(task).unwrap();
    assert_eq!(
        saved.status().last_outcome(),
        Some(&TaskOutcome::failed("PUBLISH_FAILED"))
    );
    assert_eq!(
        saved.status().turns().last().and_then(TurnSummary::outcome),
        Some(&TaskOutcome::failed("PUBLISH_FAILED"))
    );
    assert!(saved.fetched_head().is_none());
    assert_eq!(
        saved.deliveries().first().map(OriginDelivery::state),
        Some(DeliveryState::Pending)
    );
    assert_eq!(
        saved.deliveries().first().map(OriginDelivery::turn_id),
        Some(turn)
    );
}

#[test]
fn wait_exits_1_and_keeps_result_fetch_failed_when_remote_is_done() {
    let task = task_id(0x21);
    let turn = turn_id(0x22);
    let record = open_failed_record(task, turn, "RESULT_FETCH_FAILED");
    let remote = TaskRemoteRunner::new(remote_done_status(turn, 9));
    let (waited, saved) = wait_planted(record, &remote);
    assert_eq!(waited.exit_code(), 1);
    assert_eq!(
        saved.status().last_outcome(),
        Some(&TaskOutcome::failed("RESULT_FETCH_FAILED"))
    );
    assert!(saved.fetched_head().is_none());
}

#[test]
fn wait_observes_remote_done_for_ordinary_agent_failed() {
    let task = task_id(0x31);
    let turn = turn_id(0x32);
    let record = open_failed_record(task, turn, "agent exited 1");
    let remote = TaskRemoteRunner::new(remote_done_status(turn, 9));
    let (waited, saved) = wait_planted(record, &remote);
    assert_eq!(waited.exit_code(), 0);
    assert_eq!(saved.status().last_outcome(), Some(&TaskOutcome::Done));
}

#[test]
fn wait_observes_remote_done_for_active_followup_after_publication_failure() {
    let task = task_id(0x41);
    let completed = turn_id(0x42);
    let pending = turn_id(0x43);
    let failed = TurnSummary::new(
        1,
        completed,
        Some(TurnTerminal::Succeeded),
        Some(TaskOutcome::failed("PUBLISH_FAILED")),
        Some(true),
        false,
        Some(1),
        Some(2),
    );
    let pending_turn = TurnSummary::new(2, pending, None, None, None, false, Some(3), None);
    let local = TaskStatus::new(
        TaskState::Active,
        Some(TaskOutcome::failed("PUBLISH_FAILED")),
        Some("mini-1".into()),
        true,
        Some(result_oid()),
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![failed, pending_turn],
        3,
    )
    .unwrap();
    let record = LocalTaskRecord::new(
        meta(task),
        local,
        None,
        None,
        None,
        PROJECT_ID.into(),
        None,
        true,
        None,
    )
    .unwrap();
    let remote_turn = TurnSummary::new(
        2,
        pending,
        Some(TurnTerminal::Succeeded),
        Some(TaskOutcome::Done),
        Some(true),
        false,
        Some(3),
        Some(9),
    );
    let remote_status = TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Done),
        Some("mini-1".into()),
        true,
        Some(result_oid()),
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![
            TurnSummary::new(
                1,
                completed,
                Some(TurnTerminal::Succeeded),
                Some(TaskOutcome::Done),
                Some(true),
                false,
                Some(1),
                Some(2),
            ),
            remote_turn,
        ],
        9,
    )
    .unwrap();
    let remote = TaskRemoteRunner::new(remote_status);
    let state_root = tempfile::tempdir().unwrap();
    let paths = isolated_paths(state_root.path().canonicalize().unwrap());
    let store = ClientStateStore::open(&paths.state).unwrap();
    store.create_task(record).unwrap();
    let waited = wait_once(&remote, &paths, &store, task);
    assert_eq!(waited.exit_code(), 0);
    let saved = store.load_task(task).unwrap();
    assert_eq!(saved.status().last_outcome(), Some(&TaskOutcome::Done));
    assert_eq!(
        saved.status().turns().last().map(TurnSummary::turn_id),
        Some(pending)
    );
}
