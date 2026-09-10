#[allow(dead_code)]
mod support;

use std::{
    ffi::OsStr,
    os::unix::process::ExitStatusExt,
    path::PathBuf,
    process::ExitStatus,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    thread,
};

use mac_worker::{
    agent::{AgentKind, ReportedCheck, ReportedCheckStatus},
    client_state::ClientStateStore,
    config::Config,
    error::WorkerError,
    job::JobId,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    project_state::ProjectState,
    task::{
        BaseOid, ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskCloseIntent, TaskId,
        TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus,
        TurnSummary, TurnTerminal,
    },
    task_client::{TaskClient, TaskListFilter},
    task_store::{TaskCloseRequest, TaskCloseResponse, TaskStatusResponse},
    task_view::{ReviewState, remote_status_refresh_allowed, review_state},
    transfer::HostOperation,
    turn_runner::InlineRunnerExecutor,
};
use uuid::Uuid;

static CURRENT_DIR_LOCK: Mutex<()> = Mutex::new(());

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
                apply_close(&self.status, request)
            }
            other => Err(WorkerError::Protocol(format!(
                "unexpected fixture worker operation: {other}"
            ))),
        }
    }
}

struct FailingCloseRunner {
    inner: TaskRemoteRunner,
    fail_remaining: AtomicU32,
    close_then_drop_remaining: AtomicU32,
}

impl ProcessRunner for FailingCloseRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/git") {
            return SystemProcessRunner.run(request);
        }
        let operation = request
            .args
            .last()
            .and_then(|argument| argument.to_str())
            .unwrap_or_default();
        if operation == HostOperation::TaskClose.command() {
            if self.fail_remaining.load(Ordering::SeqCst) > 0 {
                self.fail_remaining.fetch_sub(1, Ordering::SeqCst);
                return Ok(failed_host_request());
            }
            if self.close_then_drop_remaining.load(Ordering::SeqCst) > 0 {
                self.close_then_drop_remaining
                    .fetch_sub(1, Ordering::SeqCst);
                apply_close(&self.inner.status, request)?;
                return Ok(failed_host_request());
            }
        }
        self.inner.run(request)
    }
}

fn apply_close(
    status: &Mutex<TaskStatus>,
    request: &ProcessRequest,
) -> Result<ProcessResult, WorkerError> {
    let close: TaskCloseRequest = decode_request(request)?;
    let current = status.lock().unwrap().clone();
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
    *status.lock().unwrap() = next.clone();
    canonical_process(&TaskCloseResponse::new(next))
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

fn failed_host_request() -> ProcessResult {
    ProcessResult {
        status: ExitStatus::from_raw(23 << 8),
        stdout: Vec::new(),
        stderr: Vec::new(),
    }
}

fn task_config() -> Config {
    Config::parse("version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n")
        .unwrap()
}

fn open_done_record(
    task_id: TaskId,
    turn_id: JobId,
    project_id: String,
    worktree_id: String,
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
        Some(base_oid),
        Some("ready for review".into()),
        Vec::new(),
        Vec::new(),
        None,
        vec![turn],
        2,
    )
    .unwrap()
    .with_reported_checks(vec![ReportedCheck::new(
        "unit",
        "cargo test",
        ReportedCheckStatus::Pass,
        "agent ran tests",
    )])
    .unwrap();
    LocalTaskRecord::new(
        meta,
        status,
        None,
        None,
        None,
        "c".repeat(64),
        None,
        true,
        None,
    )
    .unwrap()
}

struct CloseHarness {
    _lock: std::sync::MutexGuard<'static, ()>,
    _repo: support::GitRepo,
    _current_dir: CurrentDirGuard,
    _state_root: tempfile::TempDir,
    store: ClientStateStore,
    paths: mac_worker::paths::PathLayout,
    config: Config,
    task_id: TaskId,
}

fn open_review_task() -> (CloseHarness, LocalTaskRecord) {
    let lock = CURRENT_DIR_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let repo = support::GitRepo::init();
    let current_dir = CurrentDirGuard::enter(repo.root());
    let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let state_root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
    let store = ClientStateStore::open(&paths.state).unwrap();
    let task_id = TaskId::new(Uuid::from_u128(0x11));
    let turn_id = JobId::new(Uuid::from_u128(0x22));
    let record = open_done_record(
        task_id,
        turn_id,
        project.context.project_id.clone(),
        project.context.worktree_id.clone(),
    );
    store.create_task(record.clone()).unwrap();
    (
        CloseHarness {
            _lock: lock,
            _repo: repo,
            _current_dir: current_dir,
            _state_root: state_root,
            store,
            paths,
            config: task_config(),
            task_id,
        },
        record,
    )
}

#[test]
fn task_status_omits_empty_reported_checks_and_defaults_legacy_disk() {
    let status = TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Done),
        None,
        true,
        None,
        None,
        Vec::new(),
        Vec::new(),
        None,
        Vec::new(),
        1,
    )
    .unwrap();
    let value = serde_json::to_value(&status).unwrap();
    assert!(value.get("reported_checks").is_none());
    let mut legacy = value.clone();
    legacy.as_object_mut().unwrap().remove("reported_checks");
    let parsed: TaskStatus = serde_json::from_value(legacy).unwrap();
    assert!(parsed.reported_checks().is_empty());

    let with_checks = status
        .with_reported_checks(vec![ReportedCheck::new(
            "lint",
            "",
            ReportedCheckStatus::NotRun,
            "",
        )])
        .unwrap();
    let encoded = serde_json::to_value(&with_checks).unwrap();
    assert_eq!(encoded["reported_checks"][0]["status"], "not_run");
}

#[test]
fn close_transport_failure_after_fence_is_not_accepted() {
    let (harness, record) = open_review_task();
    let remote = FailingCloseRunner {
        inner: TaskRemoteRunner::new(record.status().clone()),
        fail_remaining: AtomicU32::new(1),
        close_then_drop_remaining: AtomicU32::new(0),
    };
    let executor = InlineRunnerExecutor;
    let closer = TaskClient::new(
        &remote,
        &harness.config,
        &harness.paths,
        &harness.store,
        &executor,
    );
    let speaker = TaskClient::new(
        &remote,
        &harness.config,
        &harness.paths,
        &harness.store,
        &executor,
    );

    let error = closer.close(harness.task_id, false).unwrap_err();
    assert_eq!(error.public_code(), "HOST_REQUEST_FAILED");
    let current = harness.store.load_task(harness.task_id).unwrap();
    assert_eq!(current.status().state(), TaskState::Open);
    assert!(current.close_intent().is_some());
    assert_eq!(
        review_state(&current, current.status()),
        ReviewState::ClosePending
    );
    assert_ne!(
        review_state(&current, current.status()),
        ReviewState::Accepted
    );

    let say = speaker
        .say(
            harness.task_id,
            "please also add tests".into(),
            false,
            &mut vec![],
            &mut vec![],
        )
        .unwrap_err();
    assert_eq!(say.public_code(), "TASK_BUSY");
    assert_eq!(say.public_message(), "task close is in progress");
    assert_eq!(
        harness
            .store
            .load_task(harness.task_id)
            .unwrap()
            .status()
            .turns()
            .len(),
        1
    );

    let closed = closer.close(harness.task_id, false).unwrap();
    assert_eq!(closed.status().state(), TaskState::Closed);
    let accepted = harness.store.load_task(harness.task_id).unwrap();
    assert!(accepted.close_intent().is_none());
    assert_eq!(
        review_state(&accepted, accepted.status()),
        ReviewState::Accepted
    );
    assert_eq!(
        accepted.status().reported_checks()[0].status(),
        ReportedCheckStatus::Pass
    );
}

#[test]
fn unresolved_close_intent_stays_pending_when_projected_status_is_closed() {
    let (_harness, record) = open_review_task();
    let fenced = record
        .with_close_intent(TaskCloseIntent::from_record(&record, false).unwrap())
        .unwrap();
    let projected = TaskStatus::new(
        TaskState::Closed,
        Some(TaskOutcome::Done),
        Some("mini-1".into()),
        true,
        record.status().head_oid().cloned(),
        record.status().summary().map(str::to_owned),
        Vec::new(),
        Vec::new(),
        None,
        record.status().turns().to_vec(),
        record.status().updated_at_millis() + 1,
    )
    .unwrap();
    assert_eq!(review_state(&fenced, &projected), ReviewState::ClosePending);
    assert_ne!(review_state(&fenced, &projected), ReviewState::Accepted);
}

#[test]
fn close_lost_response_after_remote_success_retries_to_closed() {
    let (harness, record) = open_review_task();
    let remote = FailingCloseRunner {
        inner: TaskRemoteRunner::new(record.status().clone()),
        fail_remaining: AtomicU32::new(0),
        close_then_drop_remaining: AtomicU32::new(1),
    };
    let executor = InlineRunnerExecutor;
    let client = TaskClient::new(
        &remote,
        &harness.config,
        &harness.paths,
        &harness.store,
        &executor,
    );

    let error = client.close(harness.task_id, false).unwrap_err();
    assert_eq!(error.public_code(), "HOST_REQUEST_FAILED");
    let pending = harness.store.load_task(harness.task_id).unwrap();
    assert_eq!(pending.status().state(), TaskState::Open);
    assert!(pending.close_intent().is_some());

    let closed = client.close(harness.task_id, false).unwrap();
    assert_eq!(closed.status().state(), TaskState::Closed);
    assert!(
        harness
            .store
            .load_task(harness.task_id)
            .unwrap()
            .close_intent()
            .is_none()
    );
}

#[test]
fn stale_followup_rollback_does_not_overwrite_a_close_fence() {
    let (harness, record) = open_review_task();
    let remote = TaskRemoteRunner::new(record.status().clone());
    let executor = InlineRunnerExecutor;
    let client = TaskClient::new(
        &remote,
        &harness.config,
        &harness.paths,
        &harness.store,
        &executor,
    );
    client.close(harness.task_id, false).unwrap();
    let winner = harness.store.load_task(harness.task_id).unwrap();
    assert_eq!(winner.status().state(), TaskState::Closed);

    let restored = harness
        .store
        .update_task_if_current(&record, record.clone())
        .unwrap();
    assert!(
        !restored,
        "stale Open snapshot must not roll back an accepted close"
    );
    assert_eq!(
        harness
            .store
            .load_task(harness.task_id)
            .unwrap()
            .status()
            .state(),
        TaskState::Closed
    );
}

fn fetched_head_oid() -> BaseOid {
    "d".repeat(40).parse().unwrap()
}

fn status_clone_with(status: &TaskStatus, state: TaskState, turns: Vec<TurnSummary>) -> TaskStatus {
    TaskStatus::new(
        state,
        status.last_outcome().cloned(),
        status.worker().map(str::to_owned),
        status.session_present(),
        status.head_oid().cloned(),
        status.summary().map(str::to_owned),
        status.questions().to_vec(),
        status.files_changed().to_vec(),
        status.diff_stat().map(str::to_owned),
        turns,
        status.updated_at_millis() + 1,
    )
    .unwrap()
    .copying_reported_checks(status)
    .unwrap()
}

#[test]
fn fetched_head_update_preserves_concurrent_close_intent() {
    let (harness, record) = open_review_task();
    let head = fetched_head_oid();
    let fenced = record
        .with_close_intent(TaskCloseIntent::from_record(&record, false).unwrap())
        .unwrap();
    harness.store.update_task(fenced).unwrap();

    assert!(
        harness
            .store
            .update_fetched_head_for_current_turn(&record, head.clone())
            .unwrap()
    );

    let current = harness.store.load_task(harness.task_id).unwrap();
    assert!(current.close_intent().is_some());
    assert_eq!(current.status().state(), TaskState::Open);
    assert_eq!(current.fetched_head(), Some(&head));
}

#[test]
fn fetched_head_update_preserves_concurrent_closed_record() {
    let (harness, record) = open_review_task();
    let head = fetched_head_oid();
    let closed = record
        .with_status(status_clone_with(
            record.status(),
            TaskState::Closed,
            record.status().turns().to_vec(),
        ))
        .unwrap();
    harness.store.update_task(closed).unwrap();

    assert!(
        harness
            .store
            .update_fetched_head_for_current_turn(&record, head.clone())
            .unwrap()
    );

    let current = harness.store.load_task(harness.task_id).unwrap();
    assert_eq!(current.status().state(), TaskState::Closed);
    assert!(current.close_intent().is_none());
    assert_eq!(current.fetched_head(), Some(&head));
}

#[test]
fn fetched_head_update_skips_a_newer_turn() {
    let (harness, record) = open_review_task();
    let mut turns = record.status().turns().to_vec();
    turns.push(TurnSummary::new(
        2,
        JobId::new(Uuid::from_u128(0x33)),
        Some(TurnTerminal::Succeeded),
        Some(TaskOutcome::Done),
        Some(true),
        false,
        Some(3),
        Some(4),
    ));
    let newer = record
        .with_status(status_clone_with(record.status(), TaskState::Open, turns))
        .unwrap();
    harness.store.update_task(newer).unwrap();

    assert!(
        !harness
            .store
            .update_fetched_head_for_current_turn(&record, fetched_head_oid())
            .unwrap()
    );
    assert!(
        harness
            .store
            .load_task(harness.task_id)
            .unwrap()
            .fetched_head()
            .is_none()
    );
}

#[test]
fn fetched_head_update_interleaves_with_close_intent_publish() {
    let (harness, record) = open_review_task();
    let store = harness.store.clone();
    let stale = record.clone();
    let head = fetched_head_oid();
    let barrier = Arc::new(Barrier::new(2));
    let closer = {
        let store = store.clone();
        let barrier = barrier.clone();
        let fenced = record
            .with_close_intent(TaskCloseIntent::from_record(&record, false).unwrap())
            .unwrap();
        thread::spawn(move || {
            barrier.wait();
            store.update_task(fenced).unwrap();
            barrier.wait();
        })
    };

    barrier.wait();
    barrier.wait();
    assert!(
        store
            .update_fetched_head_for_current_turn(&stale, head.clone())
            .unwrap()
    );
    closer.join().unwrap();

    let current = store.load_task(harness.task_id).unwrap();
    assert!(current.close_intent().is_some());
    assert_eq!(current.status().state(), TaskState::Open);
    assert_eq!(current.fetched_head(), Some(&head));
}

#[test]
fn cli_status_skips_remote_closed_overlay_while_close_intent_is_set() {
    let (harness, record) = open_review_task();
    let fenced = record
        .with_close_intent(TaskCloseIntent::from_record(&record, false).unwrap())
        .unwrap();
    harness.store.update_task(fenced).unwrap();
    let remote = TaskRemoteRunner::new(status_clone_with(
        record.status(),
        TaskState::Closed,
        record.status().turns().to_vec(),
    ));
    let executor = InlineRunnerExecutor;
    let client = TaskClient::new(
        &remote,
        &harness.config,
        &harness.paths,
        &harness.store,
        &executor,
    );

    let report = client.status(harness.task_id).unwrap();
    assert_eq!(report.status().state(), TaskState::Open);
    assert_ne!(report.status().state(), TaskState::Closed);
    let listed = client
        .list(TaskListFilter {
            run_id: None,
            state: None,
            outcome: None,
            full: false,
        })
        .unwrap();
    let row = listed
        .tasks()
        .iter()
        .find(|row| row.task_id == harness.task_id)
        .unwrap();
    assert_eq!(row.state, TaskState::Open);
    assert_eq!(row.review_state, ReviewState::ClosePending);
    assert_ne!(row.review_state, ReviewState::Accepted);
}

#[test]
fn cli_status_keeps_log_drain_unavailable_instead_of_remote_closed() {
    let (harness, record) = open_review_task();
    let drained = record
        .with_abandon_code(Some("LOG_DRAIN_UNAVAILABLE".into()))
        .unwrap();
    assert!(!remote_status_refresh_allowed(&drained));
    harness.store.update_task(drained).unwrap();
    let remote = TaskRemoteRunner::new(status_clone_with(
        record.status(),
        TaskState::Closed,
        record.status().turns().to_vec(),
    ));
    let executor = InlineRunnerExecutor;
    let client = TaskClient::new(
        &remote,
        &harness.config,
        &harness.paths,
        &harness.store,
        &executor,
    );

    let report = client.status(harness.task_id).unwrap();
    assert_eq!(report.status().state(), TaskState::Open);
    assert_eq!(
        harness
            .store
            .load_task(harness.task_id)
            .unwrap()
            .abandon_code(),
        Some("LOG_DRAIN_UNAVAILABLE")
    );
}
