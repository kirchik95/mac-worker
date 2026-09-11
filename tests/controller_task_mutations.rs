//! Bounded controller task-mutation gates: server snapshot, no-effect rejects,
//! JSON identity, and delegation to the existing fenced TaskClient methods.

#[allow(dead_code)]
mod support;

use std::{
    ffi::OsStr,
    os::unix::process::ExitStatusExt,
    path::PathBuf,
    process::ExitStatus,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy},
    client_state::ClientStateStore,
    config::Config,
    controller::{
        ActiveResumeConfig, ControllerFault, ControllerStore, PreparedTaskMutation, RequestPhase,
        TaskSubmitHandler, execute_task_mutation, parse_request, prepare_task_mutation,
    },
    error::WorkerError,
    job::ProcessIdentity,
    paths::PathLayout,
    prepared_followup::PreparedFollowup,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    project_state::ProjectState,
    protocol::PROTOCOL_VERSION,
    supervisor::{ProcessInspector, ProcessObservation},
    task::{
        BaseOid, ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskId, TaskLimits,
        TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnId,
        TurnSummary, TurnTerminal,
    },
    task_client::TaskClient,
    task_store::{TaskCloseRequest, TaskCloseResponse},
    transfer::HostOperation,
    transfer_repo::repo_id_for,
    turn_runner::RunnerExecutor,
};
use serde_json::{Value, json};
use uuid::Uuid;

static CURRENT_DIR_LOCK: Mutex<()> = Mutex::new(());

const REQUEST_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";
const REQUEST_ID_2: &str = "018f0f4a6b5c7d8e9f00112233445567";

#[derive(Clone, Copy)]
struct LiveOwners;

impl ProcessInspector for LiveOwners {
    fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
        ProcessIdentity::new(pid, u64::from(pid) * 10_000 + 7)
    }

    fn observe(&self, _expected: ProcessIdentity) -> ProcessObservation {
        ProcessObservation::Matching { process_group: 1 }
    }

    fn observe_group(
        &self,
        _process_group: u32,
    ) -> mac_worker::supervisor::ProcessGroupObservation {
        mac_worker::supervisor::ProcessGroupObservation::Ambiguous
    }

    fn observe_group_members(
        &self,
        _leader: u32,
    ) -> mac_worker::supervisor::ProcessGroupMembership {
        mac_worker::supervisor::ProcessGroupMembership::Ambiguous
    }
}

struct CountingExecutor {
    starts: AtomicUsize,
}

impl CountingExecutor {
    fn new() -> Self {
        Self {
            starts: AtomicUsize::new(0),
        }
    }

    fn starts(&self) -> usize {
        self.starts.load(Ordering::SeqCst)
    }
}

impl RunnerExecutor for CountingExecutor {
    fn start(
        &self,
        _paths: &PathLayout,
        _task_id: TaskId,
        _turn_id: TurnId,
    ) -> Result<mac_worker::task::RunnerIdentity, WorkerError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(mac_worker::task::RunnerIdentity::new(ProcessIdentity::new(
            2_000_000_011,
            9_999_999,
        )?))
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
        let _ = std::env::set_current_dir(&self.previous);
    }
}

struct Harness {
    executor: CountingExecutor,
    store: ClientStateStore,
    config: Config,
    project_id: String,
    worktree_id: String,
    project_root: PathBuf,
    common_dir: PathBuf,
    paths: PathLayout,
    _state_root: tempfile::TempDir,
    // Struct fields drop in declaration order, unlike locals. Restore cwd
    // while the repo still exists, drop the repo next, and hold the mutex last.
    _current_dir: CurrentDirGuard,
    _repo: support::GitRepo,
    _lock: std::sync::MutexGuard<'static, ()>,
}

fn task_config() -> Config {
    Config::parse("version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n")
        .unwrap()
}

fn base_oid() -> BaseOid {
    "a".repeat(40).parse().unwrap()
}

fn open_task_record(
    task_id: TaskId,
    turn_id: TurnId,
    project_id: &str,
    worktree_id: &str,
    repo_id: String,
) -> LocalTaskRecord {
    let meta = TaskMeta::new(TaskMetaInput {
        task_id,
        run_id: None,
        project_id: project_id.to_owned(),
        worktree_id: worktree_id.to_owned(),
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
        base_oid: base_oid(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("mac-worker", "mac-worker@example.test").unwrap(),
        title: None,
        prompt: "fixture task".into(),
        created_at_millis: 1,
    })
    .unwrap();
    let turn = TurnSummary::new(
        1,
        turn_id,
        Some(TurnTerminal::Succeeded),
        Some(TaskOutcome::Done),
        Some(true),
        false,
        Some(1),
        Some(2),
    );
    let status = TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Done),
        Some("mini-1".into()),
        true,
        Some(base_oid()),
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![turn],
        2,
    )
    .unwrap();
    LocalTaskRecord::new(meta, status, None, None, None, repo_id, None, true, None).unwrap()
}

impl Harness {
    fn new() -> Self {
        let lock = CURRENT_DIR_LOCK.lock().unwrap();
        let repo = support::GitRepo::init();
        repo.write("base.txt", b"base\n");
        repo.commit_all("base");
        let current_dir = CurrentDirGuard::enter(repo.root());
        let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
        let state_root = tempfile::tempdir().unwrap();
        let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
        let store = ClientStateStore::open_with_owner_inspector(&paths.state, LiveOwners).unwrap();
        Self {
            executor: CountingExecutor::new(),
            store,
            config: task_config(),
            project_id: project.context.project_id.clone(),
            worktree_id: project.context.worktree_id.clone(),
            project_root: project.context.root.clone(),
            common_dir: project.context.common_dir.clone(),
            paths,
            _state_root: state_root,
            _current_dir: current_dir,
            _repo: repo,
            _lock: lock,
        }
    }

    fn client<'a>(&'a self, runner: &'a dyn ProcessRunner) -> TaskClient<'a> {
        TaskClient::new(
            runner,
            &self.config,
            &self.paths,
            &self.store,
            &self.executor,
        )
    }

    fn plant_open_task(&self, task_number: u128, turn_number: u128) -> LocalTaskRecord {
        let record = open_task_record(
            TaskId::new(Uuid::from_u128(task_number)),
            TurnId::new(Uuid::from_u128(turn_number)),
            &self.project_id,
            &self.worktree_id,
            repo_id_for(&self.common_dir).unwrap(),
        );
        self.store.create_task(record.clone()).unwrap();
        self.store
            .write_task_project_path(&record, &self.project_root)
            .unwrap();
        record
    }

    fn queue_present(&self, task_id: TaskId) -> bool {
        self.store
            .queue_entry_for_task_turn(task_id)
            .unwrap()
            .is_some()
    }
}

fn request(
    request_id: &str,
    command: &str,
    body: Value,
) -> mac_worker::controller::ControllerRequest {
    let payload = serde_json::to_vec(&json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": request_id,
        "command": command,
        "body": body,
    }))
    .unwrap();
    parse_request(&payload).unwrap()
}

fn code(error: &WorkerError) -> String {
    error.public_code()
}

fn bump_updated_at(store: &ClientStateStore, record: &LocalTaskRecord) -> LocalTaskRecord {
    let status = TaskStatus::new(
        record.status().state(),
        record.status().last_outcome().cloned(),
        record.status().worker().map(str::to_owned),
        record.status().session_present(),
        record.status().head_oid().cloned(),
        record.status().summary().map(str::to_owned),
        record.status().questions().to_vec(),
        record.status().files_changed().to_vec(),
        record.status().diff_stat().map(str::to_owned),
        record.status().turns().to_vec(),
        record.status().updated_at_millis() + 1,
    )
    .unwrap()
    .copying_reported_checks(record.status())
    .unwrap();
    let next = record.with_status(status).unwrap();
    store.update_task(next.clone()).unwrap();
    next
}

struct CloseHost {
    status: Mutex<TaskStatus>,
}

impl CloseHost {
    fn new(status: TaskStatus) -> Self {
        Self {
            status: Mutex::new(status),
        }
    }
}

impl ProcessRunner for CloseHost {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/git") {
            return SystemProcessRunner.run(request);
        }
        assert_eq!(request.program, OsStr::new("/usr/bin/ssh"));
        let operation = request
            .args
            .last()
            .and_then(|argument| argument.to_str())
            .unwrap_or_default();
        assert_eq!(operation, HostOperation::TaskClose.command());
        let close: TaskCloseRequest = serde_json::from_slice(request.stdin.as_deref().unwrap())
            .map_err(|error| WorkerError::Protocol(error.to_string()))?;
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
        )?
        .copying_reported_checks(&current)?;
        *self.status.lock().unwrap() = next.clone();
        let mut stdout = serde_json::to_vec(&TaskCloseResponse::new(next))
            .map_err(|error| WorkerError::Protocol(error.to_string()))?;
        stdout.push(b'\n');
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout,
            stderr: Vec::new(),
        })
    }
}

#[test]
fn preparation_uses_the_server_snapshot_and_rejects_client_payloads() {
    let harness = Harness::new();
    let expected = harness.plant_open_task(11, 12);
    let stuffed = request(
        REQUEST_ID,
        "task.say",
        json!({
            "task_id": expected.meta().task_id().to_string(),
            "message": "follow up",
            "expected": expected,
            "prepared": {"ignored": true},
        }),
    );
    let error = prepare_task_mutation(&stuffed, &harness.store, 4_000).unwrap_err();
    assert_eq!(code(&error), "INVALID_REQUEST");
    assert_eq!(
        harness.store.load_task(expected.meta().task_id()).unwrap(),
        expected
    );
    assert!(!harness.queue_present(expected.meta().task_id()));

    let prepared = prepare_task_mutation(
        &request(
            REQUEST_ID,
            "task.say",
            json!({
                "task_id": expected.meta().task_id().to_string(),
                "message": "follow up",
            }),
        ),
        &harness.store,
        4_000,
    )
    .unwrap();
    let PreparedTaskMutation::Say { prepared: follow } = &prepared else {
        panic!("expected say preparation");
    };
    assert_eq!(follow.expected(), &expected);
    assert_eq!(follow.message(), "follow up");
    assert_eq!(follow.created_at_millis(), 4_000);
    assert_ne!(follow.turn_id(), expected.status().turns()[0].turn_id());
    assert_eq!(prepared.task_id(), expected.meta().task_id());
    assert!(!harness.queue_present(expected.meta().task_id()));
}

#[test]
fn bad_bodies_and_commands_have_no_task_or_queue_effects() {
    let harness = Harness::new();
    let expected = harness.plant_open_task(21, 22);
    let task_id = expected.meta().task_id();
    let invalid = [
        request(REQUEST_ID, "checkpoint.submit", json!({"prompt": "no"})),
        request(
            REQUEST_ID,
            "task.submit",
            json!({"task_id": task_id.to_string()}),
        ),
        request(
            REQUEST_ID,
            "task.say",
            json!({"task_id": "not-a-task-id", "message": "x"}),
        ),
        request(REQUEST_ID, "task.say", json!({"message": "missing id"})),
        request(
            REQUEST_ID,
            "task.cancel",
            json!({"task_id": task_id.to_string(), "message": "no"}),
        ),
        request(
            REQUEST_ID,
            "task.close",
            json!({"task_id": task_id.to_string(), "discard": "yes"}),
        ),
    ];
    for req in &invalid {
        let error = prepare_task_mutation(req, &harness.store, 1).unwrap_err();
        assert_eq!(code(&error), "INVALID_REQUEST", "{}", req.command());
    }
    let missing = prepare_task_mutation(
        &request(
            REQUEST_ID,
            "task.say",
            json!({
                "task_id": "00000000000000000000000000000001",
                "message": "ghost",
            }),
        ),
        &harness.store,
        1,
    )
    .unwrap_err();
    assert_eq!(code(&missing), "TASK_NOT_FOUND");
    assert_eq!(harness.store.load_task(task_id).unwrap(), expected);
    assert!(!harness.queue_present(task_id));
}

#[test]
fn json_roundtrip_preserves_the_exact_prepared_value_and_identity() {
    let harness = Harness::new();
    let expected = harness.plant_open_task(31, 32);
    let task = expected.meta().task_id().to_string();
    let say = prepare_task_mutation(
        &request(
            REQUEST_ID,
            "task.say",
            json!({"task_id": task, "message": "round trip"}),
        ),
        &harness.store,
        7_000,
    )
    .unwrap();
    let cancel = prepare_task_mutation(
        &request(REQUEST_ID_2, "task.cancel", json!({"task_id": task})),
        &harness.store,
        8_000,
    )
    .unwrap();
    let close = prepare_task_mutation(
        &request(
            "018f0f4a6b5c7d8e9f00112233445568",
            "task.close",
            json!({"task_id": task}),
        ),
        &harness.store,
        9_000,
    )
    .unwrap();
    let close_discard = prepare_task_mutation(
        &request(
            "018f0f4a6b5c7d8e9f00112233445569",
            "task.close",
            json!({"task_id": task, "discard": true}),
        ),
        &harness.store,
        9_001,
    )
    .unwrap();

    for original in [&say, &cancel, &close, &close_discard] {
        let restored: PreparedTaskMutation =
            serde_json::from_slice(&serde_json::to_vec(original).unwrap()).unwrap();
        assert_eq!(&restored, original);
        assert_eq!(restored.task_id(), original.task_id());
        assert_eq!(restored.turn_id(), original.turn_id());
        assert_eq!(restored.created_at_millis(), original.created_at_millis());
        assert_eq!(restored.command(), original.command());
        assert_eq!(restored.discard(), original.discard());
    }
    assert_eq!(say.created_at_millis(), 7_000);
    assert_eq!(cancel.created_at_millis(), 8_000);
    assert_eq!(close.discard(), Some(false));
    assert_eq!(close_discard.discard(), Some(true));
    assert_eq!(
        cancel.turn_id(),
        Some(expected.status().turns()[0].turn_id())
    );
}

#[test]
fn execute_say_selects_the_saved_turn_and_rejects_a_later_revision() {
    let harness = Harness::new();
    let expected = harness.plant_open_task(41, 42);
    let prepared = prepare_task_mutation(
        &request(
            REQUEST_ID,
            "task.say",
            json!({
                "task_id": expected.meta().task_id().to_string(),
                "message": "saved turn",
            }),
        ),
        &harness.store,
        5_000,
    )
    .unwrap();
    let saved_turn = prepared.turn_id().unwrap();
    let runner = SystemProcessRunner;
    let report = execute_task_mutation(&harness.client(&runner), &prepared).unwrap();
    assert_eq!(
        report.status().turns().last().unwrap().turn_id(),
        saved_turn
    );
    assert_eq!(harness.executor.starts(), 1);
    assert!(harness.queue_present(expected.meta().task_id()));

    let other = PreparedTaskMutation::Say {
        prepared: PreparedFollowup::prepare(
            &expected,
            "other".into(),
            TurnId::new(Uuid::from_u128(99)),
            5_200,
        )
        .unwrap(),
    };
    let error = execute_task_mutation(&harness.client(&runner), &other).unwrap_err();
    assert_eq!(code(&error), "TASK_REVISION_CONFLICT");
    assert_eq!(
        harness
            .store
            .load_task(expected.meta().task_id())
            .unwrap()
            .status()
            .turns()
            .last()
            .unwrap()
            .turn_id(),
        saved_turn
    );
}

#[test]
fn execute_cancel_rejects_a_later_revision_instead_of_the_new_turn() {
    let harness = Harness::new();
    let expected = harness.plant_open_task(51, 52);
    let cancel = prepare_task_mutation(
        &request(
            REQUEST_ID,
            "task.cancel",
            json!({"task_id": expected.meta().task_id().to_string()}),
        ),
        &harness.store,
        6_000,
    )
    .unwrap();
    assert_eq!(
        cancel.turn_id(),
        Some(expected.status().turns()[0].turn_id())
    );

    let say = prepare_task_mutation(
        &request(
            REQUEST_ID_2,
            "task.say",
            json!({
                "task_id": expected.meta().task_id().to_string(),
                "message": "later turn",
            }),
        ),
        &harness.store,
        6_100,
    )
    .unwrap();
    let runner = SystemProcessRunner;
    execute_task_mutation(&harness.client(&runner), &say).unwrap();
    let later = harness
        .store
        .load_task(expected.meta().task_id())
        .unwrap()
        .status()
        .turns()
        .last()
        .unwrap()
        .turn_id();
    assert_ne!(later, expected.status().turns()[0].turn_id());

    let error = execute_task_mutation(&harness.client(&runner), &cancel).unwrap_err();
    assert_eq!(code(&error), "TASK_REVISION_CONFLICT");
    assert_eq!(
        harness
            .store
            .load_task(expected.meta().task_id())
            .unwrap()
            .status()
            .turns()
            .last()
            .unwrap()
            .turn_id(),
        later
    );
}

#[test]
fn execute_close_uses_saved_discard_and_rejects_a_later_revision() {
    let harness = Harness::new();
    let expected = harness.plant_open_task(61, 62);
    let close_keep = prepare_task_mutation(
        &request(
            REQUEST_ID,
            "task.close",
            json!({"task_id": expected.meta().task_id().to_string()}),
        ),
        &harness.store,
        7_000,
    )
    .unwrap();
    assert_eq!(close_keep.discard(), Some(false));
    bump_updated_at(&harness.store, &expected);
    let runner = SystemProcessRunner;
    let error = execute_task_mutation(&harness.client(&runner), &close_keep).unwrap_err();
    assert_eq!(code(&error), "TASK_REVISION_CONFLICT");
    assert_eq!(
        harness
            .store
            .load_task(expected.meta().task_id())
            .unwrap()
            .status()
            .state(),
        TaskState::Open
    );

    let current = harness.store.load_task(expected.meta().task_id()).unwrap();
    let close_discard = prepare_task_mutation(
        &request(
            REQUEST_ID_2,
            "task.close",
            json!({
                "task_id": current.meta().task_id().to_string(),
                "discard": true,
            }),
        ),
        &harness.store,
        7_100,
    )
    .unwrap();
    assert_eq!(close_discard.discard(), Some(true));
    let host = CloseHost::new(current.status().clone());
    let report = execute_task_mutation(&harness.client(&host), &close_discard).unwrap();
    assert_eq!(report.status().state(), TaskState::Abandoned);
}

#[test]
fn close_without_discard_field_defaults_to_keep() {
    let harness = Harness::new();
    let expected = harness.plant_open_task(71, 72);
    let close = prepare_task_mutation(
        &request(
            REQUEST_ID,
            "task.close",
            json!({"task_id": expected.meta().task_id().to_string()}),
        ),
        &harness.store,
        1,
    )
    .unwrap();
    let host = CloseHost::new(expected.status().clone());
    let report = execute_task_mutation(&harness.client(&host), &close).unwrap();
    assert_eq!(report.status().state(), TaskState::Closed);
}

/// Keep the real controller, journal and TaskClient; only the SSH boundary is
/// unavailable. A stale close must not reach it, including after a restart.
struct UnavailableCloseHost {
    calls: AtomicUsize,
}

impl ProcessRunner for UnavailableCloseHost {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/git") {
            return SystemProcessRunner.run(request);
        }
        assert_eq!(request.program, OsStr::new("/usr/bin/ssh"));
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(WorkerError::Unavailable("test host is offline".into()))
    }
}

fn assert_stale_close_is_settled(already_closed: bool) {
    let harness = Harness::new();
    let expected = harness.plant_open_task(81, 82);
    let task_id = expected.meta().task_id();
    let envelope = request(REQUEST_ID, "task.close", json!({"task_id": task_id}));
    let host = UnavailableCloseHost {
        calls: AtomicUsize::new(0),
    };
    let handler = TaskSubmitHandler::new(&host, &harness.config, &harness.paths, &harness.store);
    let root = harness.paths.controller_state_root();
    let journal = ControllerStore::open(&root).unwrap();
    journal
        .handle_with(&envelope, &handler, ControllerFault::StopAfterPublish)
        .unwrap();
    let frozen = journal.load(REQUEST_ID).unwrap().unwrap();
    assert_eq!(frozen.phase(), RequestPhase::Published);
    assert!(frozen.result().is_none());
    // Preparation may reconcile the selected task. Only execution of the
    // already-published stale envelope must avoid further host calls.
    let host_calls_before_execute = host.calls.load(Ordering::SeqCst);

    let mut changed = serde_json::to_value(&expected).unwrap();
    changed["status"]["updated_at_millis"] = json!(3);
    let (expected_code, expected_message) = if already_closed {
        changed["status"]["state"] = json!("closed");
        changed["status"]["head_oid"] = json!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        ("TASK_CLOSED", "task is terminal")
    } else {
        ("TASK_REVISION_CONFLICT", "task changed before close")
    };
    let changed: LocalTaskRecord = serde_json::from_value(changed).unwrap();
    harness.store.update_task(changed.clone()).unwrap();

    let error = journal
        .handle_with(&envelope, &handler, ControllerFault::None)
        .unwrap_err();
    assert_eq!(error.public_code(), expected_code);
    assert_eq!(error.public_message(), expected_message);
    assert!(matches!(error, WorkerError::Task { .. }));
    let settled = journal.load(REQUEST_ID).unwrap().unwrap();
    assert_eq!(
        settled.phase(),
        RequestPhase::Acked,
        "stale close must stop consuming retries"
    );
    assert_eq!(settled.prepared(), frozen.prepared());
    assert_eq!(
        settled.result().unwrap()["controller_rejection"]["code"],
        expected_code
    );
    assert!(
        !root
            .join("active")
            .join(format!("{REQUEST_ID}.json"))
            .exists()
    );
    drop(journal);

    let reopened = ControllerStore::open(&root).unwrap();
    for _ in 0..2 {
        let tick = reopened
            .resume_active_bounded(&handler, &ActiveResumeConfig::default())
            .unwrap();
        assert!(tick.completed.is_empty());
        assert!(tick.failed.is_empty());
        let replay = reopened
            .handle_with(&envelope, &handler, ControllerFault::None)
            .unwrap_err();
        assert_eq!(replay.public_code(), expected_code);
        assert_eq!(replay.public_message(), expected_message);
    }
    assert_eq!(harness.store.load_task(task_id).unwrap(), changed);
    assert_eq!(host.calls.load(Ordering::SeqCst), host_calls_before_execute);
}

#[test]
fn journal_stale_close_revision_is_retired_and_replays_the_saved_rejection() {
    assert_stale_close_is_settled(false);
}

#[test]
fn journal_stale_close_of_a_different_terminal_head_is_retired() {
    assert_stale_close_is_settled(true);
}

struct RetryCloseHost {
    failure: Mutex<Option<WorkerError>>,
    calls: AtomicUsize,
    host: CloseHost,
}

impl ProcessRunner for RetryCloseHost {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/git") {
            return SystemProcessRunner.run(request);
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(error) = self.failure.lock().unwrap().take() {
            return Err(error);
        }
        self.host.run(request)
    }
}

#[test]
fn journal_close_failure_after_intent_stays_pending_and_recovers() {
    // A transport failure after the durable close intent must keep enough
    // state for a later retry to finish the same close.
    let failure = WorkerError::Unavailable("test host is offline".into());
    let harness = Harness::new();
    let expected = harness.plant_open_task(91, 92);
    let task_id = expected.meta().task_id();
    let envelope = request(REQUEST_ID, "task.close", json!({"task_id": task_id}));
    let preparation_host = UnavailableCloseHost {
        calls: AtomicUsize::new(0),
    };
    let preparation = TaskSubmitHandler::new(
        &preparation_host,
        &harness.config,
        &harness.paths,
        &harness.store,
    );
    let root = harness.paths.controller_state_root();
    let journal = ControllerStore::open(&root).unwrap();
    journal
        .handle_with(&envelope, &preparation, ControllerFault::StopAfterPublish)
        .unwrap();
    let host = RetryCloseHost {
        failure: Mutex::new(Some(failure)),
        calls: AtomicUsize::new(0),
        host: CloseHost::new(expected.status().clone()),
    };
    let handler = TaskSubmitHandler::new(&host, &harness.config, &harness.paths, &harness.store);
    journal
        .handle_with(&envelope, &handler, ControllerFault::None)
        .unwrap_err();
    let pending = journal.load(REQUEST_ID).unwrap().unwrap();
    assert_eq!(pending.phase(), RequestPhase::Published);
    assert!(pending.result().is_none());
    assert!(
        root.join("active")
            .join(format!("{REQUEST_ID}.json"))
            .exists()
    );
    assert!(
        harness
            .store
            .load_task(task_id)
            .unwrap()
            .close_intent()
            .is_some()
    );
    drop(journal);

    let reopened = ControllerStore::open(&root).unwrap();
    let tick = reopened
        .resume_active_bounded(&handler, &ActiveResumeConfig::default())
        .unwrap();
    assert!(tick.failed.is_empty(), "{:?}", tick.failed);
    assert_eq!(tick.completed.len(), 1);
    assert_eq!(
        reopened.load(REQUEST_ID).unwrap().unwrap().phase(),
        RequestPhase::Acked
    );
    assert!(
        !root
            .join("active")
            .join(format!("{REQUEST_ID}.json"))
            .exists()
    );
    let closed = harness.store.load_task(task_id).unwrap();
    assert_eq!(closed.status().state(), TaskState::Closed);
    assert!(closed.close_intent().is_none());
    assert_eq!(host.calls.load(Ordering::SeqCst), 2);

    reopened
        .handle_with(&envelope, &handler, ControllerFault::None)
        .unwrap();
    assert_eq!(host.calls.load(Ordering::SeqCst), 2);
    let fresh_close = request(REQUEST_ID_2, "task.close", json!({"task_id": task_id}));
    reopened
        .handle_with(&fresh_close, &handler, ControllerFault::None)
        .expect("a fresh close of the same terminal target remains idempotent");
    assert_eq!(host.calls.load(Ordering::SeqCst), 2);
}
