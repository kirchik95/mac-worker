//! Replay-safe prepared follow-up and fenced cancel: crash/reopen, payload
//! fence, revision fence, cancellation fence, sidecar preservation, and the
//! summed-slot capacity invariant. Real [`TaskClient`] over a real
//! [`ClientStateStore`], queue, and prompt tree; runner spawns are faked and
//! nothing touches the network.

#[allow(dead_code)]
mod support;

use std::{
    ffi::OsStr,
    os::unix::process::ExitStatusExt,
    path::PathBuf,
    process::ExitStatus,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy, ReportedCheck, ReportedCheckStatus},
    client_state::{ClientStateStore, ClientStateWritePoint},
    config::Config,
    error::WorkerError,
    job::ProcessIdentity,
    paths::PathLayout,
    prepared_followup::PreparedFollowup,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    project_state::ProjectState,
    supervisor::{ProcessInspector, ProcessObservation},
    task::{
        BaseOid, ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskId, TaskLimits,
        TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnId,
        TurnSummary, TurnTerminal,
    },
    task_client::TaskClient,
    task_store::{TaskCancelRequest, TaskCancelResponse},
    transfer::HostOperation,
    turn_runner::RunnerExecutor,
};
use uuid::Uuid;

static CURRENT_DIR_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy)]
struct LiveOwners;

impl ProcessInspector for LiveOwners {
    fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
        ProcessIdentity::new(pid, u64::from(pid) * 10_000 + 7)
    }

    fn observe(&self, _expected: ProcessIdentity) -> ProcessObservation {
        // Every holder counts as live, so handoff never spawns and a replay
        // observes Pending instead of stealing the row.
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
        std::env::set_current_dir(&self.previous).unwrap();
    }
}

struct Harness {
    paths: PathLayout,
    store: ClientStateStore,
    config: Config,
    project_id: String,
    worktree_id: String,
    project_root: PathBuf,
    executor: CountingExecutor,
    // Drop order is declaration order and matters: cwd is restored while the
    // repo is still alive and the mutex still held; repo removal comes next;
    // the lock releases last. Releasing the lock first (or deleting the repo
    // before restoring cwd) lets another test observe a deleted cwd.
    _current_dir: CurrentDirGuard,
    _repo: support::GitRepo,
    _state_root: tempfile::TempDir,
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
    LocalTaskRecord::new(
        meta,
        status,
        None,
        None,
        None,
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        None,
        true,
        None,
    )
    .unwrap()
}

impl Harness {
    fn new() -> Self {
        let lock = CURRENT_DIR_LOCK.lock().unwrap();
        let repo = support::GitRepo::init();
        let current_dir = CurrentDirGuard::enter(repo.root());
        let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
        let state_root = tempfile::tempdir().unwrap();
        let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
        let store = ClientStateStore::open_with_owner_inspector(&paths.state, LiveOwners).unwrap();
        Self {
            _lock: lock,
            project_id: project.context.project_id.clone(),
            worktree_id: project.context.worktree_id.clone(),
            project_root: project.context.root.clone(),
            _repo: repo,
            _current_dir: current_dir,
            _state_root: state_root,
            paths,
            store,
            config: task_config(),
            executor: CountingExecutor::new(),
        }
    }

    fn client<'a>(&'a self, runner: &'a dyn mac_worker::process::ProcessRunner) -> TaskClient<'a> {
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
        );
        self.store.create_task(record.clone()).unwrap();
        self.store
            .write_task_project_path(&record, &self.project_root)
            .unwrap();
        record
    }

    fn prepare(
        &self,
        expected: &LocalTaskRecord,
        message: &str,
        turn_number: u128,
        now: u64,
    ) -> PreparedFollowup {
        PreparedFollowup::prepare(
            expected,
            message.to_owned(),
            TurnId::new(Uuid::from_u128(turn_number)),
            now,
        )
        .unwrap()
    }

    fn queue_rows_for(&self, task_id: TaskId) -> usize {
        self.store
            .queue_entry_for_task_turn(task_id)
            .unwrap()
            .into_iter()
            .count()
    }
}

fn code(error: &WorkerError) -> String {
    error.public_code()
}

/// Emulates a finished runner's finalizer publication for the last turn:
/// terminal turn, Open state, runner cleared, result head advanced. The queue
/// row and prompt are left alone so callers can keep them (stale row) or
/// retire them (clean completion through the normal path).
fn emulate_completion(
    store: &ClientStateStore,
    task_id: TaskId,
    ended_at: u64,
    head: BaseOid,
) -> LocalTaskRecord {
    let record = store.load_task(task_id).unwrap();
    let mut turns = record.status().turns().to_vec();
    let last = turns.pop().unwrap();
    turns.push(TurnSummary::new(
        last.turn_number(),
        last.turn_id(),
        Some(TurnTerminal::Succeeded),
        Some(TaskOutcome::Done),
        Some(true),
        false,
        last.started_at_millis(),
        Some(ended_at),
    ));
    let status = TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Done),
        record.status().worker().map(str::to_owned),
        record.status().session_present(),
        Some(head),
        record.status().summary().map(str::to_owned),
        record.status().questions().to_vec(),
        record.status().files_changed().to_vec(),
        record.status().diff_stat().map(str::to_owned),
        turns,
        ended_at,
    )
    .unwrap()
    .copying_reported_checks(record.status())
    .unwrap();
    let record = record
        .with_status(status)
        .unwrap()
        .with_runner(None)
        .unwrap();
    store.update_task(record.clone()).unwrap();
    record
}

/// Retires a completed turn through the normal path: queue row and prompt
/// removed, durable binding left in place.
fn retire_completed_turn(store: &ClientStateStore, task_id: TaskId, turn_id: TurnId) {
    if let Some(row) = store.queue_entry_for_task_turn(task_id).unwrap()
        && row.job_id() == turn_id
    {
        store.remove_queued(row.job_id()).unwrap();
    }
    store.remove_turn_prompt(task_id, turn_id).unwrap();
}

#[test]
fn replay_after_reopen_converges_on_one_turn() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let task_id = TaskId::new(Uuid::from_u128(11));
    let expected = harness.plant_open_task(11, 12);
    let prepared = harness.prepare(&expected, "follow up", 13, 1_000);

    let first = harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    assert_eq!(first.status().turns().len(), 2);
    assert_eq!(
        first.status().turns().last().unwrap().turn_id(),
        prepared.turn_id()
    );
    assert_eq!(harness.queue_rows_for(task_id), 1);
    assert_eq!(harness.executor.starts(), 1);

    // Crash: drop everything, reopen the same state roots, replay the byte
    // identical persisted value (round-tripped through JSON like a
    // controller would).
    let persisted = serde_json::to_vec(&prepared).unwrap();
    drop(expected);
    let paths = harness.paths.clone();
    let store = ClientStateStore::open_with_owner_inspector(&paths.state, LiveOwners).unwrap();
    let config = task_config();
    let executor = CountingExecutor::new();
    let client = TaskClient::new(&runner, &config, &paths, &store, &executor);
    let replayed: PreparedFollowup = serde_json::from_slice(&persisted).unwrap();
    assert_eq!(replayed, prepared);

    let second = client
        .say_prepared(&replayed, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    let live = store.load_task(task_id).unwrap();
    assert_eq!(live.status().turns().len(), 2);
    assert_eq!(
        live.status().turns().last().unwrap().turn_id(),
        prepared.turn_id()
    );
    assert_eq!(
        second.status().turns().last().unwrap().turn_id(),
        prepared.turn_id()
    );
    assert_eq!(
        store
            .queue_entry_for_task_turn(task_id)
            .unwrap()
            .unwrap()
            .job_id(),
        prepared.turn_id()
    );
    assert_eq!(
        store.read_turn_prompt(task_id, prepared.turn_id()).unwrap(),
        prepared.composed_prompt()
    );
    // No second spawn on replay: the live owner keeps the row Pending.
    assert_eq!(executor.starts(), 0);
}

#[test]
fn prompt_only_crash_resumes_without_a_second_turn() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let task_id = TaskId::new(Uuid::from_u128(21));
    let expected = harness.plant_open_task(21, 22);
    let prepared = harness.prepare(&expected, "resume me", 23, 2_000);

    // Crash between the prompt write and the task publish.
    harness
        .store
        .write_turn_prompt(task_id, prepared.turn_id(), prepared.composed_prompt())
        .unwrap();

    let report = harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    let live = harness.store.load_task(task_id).unwrap();
    assert_eq!(live.status().turns().len(), 2);
    assert_eq!(
        live.status().turns().last().unwrap().turn_id(),
        prepared.turn_id()
    );
    assert_eq!(report.status().turns().len(), 2);
    assert_eq!(harness.queue_rows_for(task_id), 1);
}

#[test]
fn prompt_write_fault_then_retry_converges() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let task_id = TaskId::new(Uuid::from_u128(31));
    let expected = harness.plant_open_task(31, 32);
    let prepared = harness.prepare(&expected, "faulty first try", 33, 3_000);

    harness
        .store
        .inject_write_failure_once(ClientStateWritePoint::BeforeTurnPromptWrite);
    let error = harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap_err();
    assert!(!code(&error).is_empty());

    // The one-shot fault is consumed; the retry converges on the same turn.
    let report = harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    assert_eq!(report.status().turns().len(), 2);
    assert_eq!(
        harness
            .store
            .load_task(task_id)
            .unwrap()
            .status()
            .turns()
            .len(),
        2
    );
    assert_eq!(harness.queue_rows_for(task_id), 1);
}

#[test]
fn same_turn_with_different_input_conflicts() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let expected = harness.plant_open_task(41, 42);
    let prepared = harness.prepare(&expected, "original", 43, 4_000);
    harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();

    // Same turn id, different message: consistent with itself, but the prompt
    // evidence already materialized another meaning.
    let tampered =
        PreparedFollowup::prepare(&expected, "tampered".to_owned(), prepared.turn_id(), 4_000)
            .unwrap();
    let error = harness
        .client(&runner)
        .say_prepared(&tampered, false, &mut Vec::new(), &mut Vec::new())
        .unwrap_err();
    assert_eq!(code(&error), "TASK_REVISION_CONFLICT");

    // Same turn id and message, different frozen time.
    let drifted =
        PreparedFollowup::prepare(&expected, "original".to_owned(), prepared.turn_id(), 4_001)
            .unwrap();
    let error = harness
        .client(&runner)
        .say_prepared(&drifted, false, &mut Vec::new(), &mut Vec::new())
        .unwrap_err();
    assert_eq!(code(&error), "TASK_REVISION_CONFLICT");
}

#[test]
fn replay_after_a_later_turn_conflicts() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let expected = harness.plant_open_task(51, 52);
    let first = harness.prepare(&expected, "first", 53, 5_000);
    harness
        .client(&runner)
        .say_prepared(&first, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();

    // A peer follow-up appends turn three. The stale replay must not resurrect
    // turn two or touch the newer row. First emulate a completed turn two the
    // way a finished runner's finalizer would publish it: terminal turn, Open
    // state, queue row retired, prompt kept.
    let second_expected = {
        let mut record = harness.store.load_task(first.task_id()).unwrap();
        // Emulate a completed turn two: terminal turn, Open state, queue row
        // retired, prompt kept. This mirrors finalizer publication without a
        // live agent.
        let mut turns = record.status().turns().to_vec();
        let last = turns.pop().unwrap();
        turns.push(TurnSummary::new(
            last.turn_number(),
            last.turn_id(),
            Some(TurnTerminal::Succeeded),
            Some(TaskOutcome::Done),
            Some(true),
            false,
            last.started_at_millis(),
            Some(5_010),
        ));
        let status = TaskStatus::new(
            TaskState::Open,
            Some(TaskOutcome::Done),
            record.status().worker().map(str::to_owned),
            record.status().session_present(),
            record.status().head_oid().cloned(),
            record.status().summary().map(str::to_owned),
            record.status().questions().to_vec(),
            record.status().files_changed().to_vec(),
            record.status().diff_stat().map(str::to_owned),
            turns,
            5_010,
        )
        .unwrap()
        .copying_reported_checks(record.status())
        .unwrap();
        record = record.with_status(status).unwrap();
        record = record.with_runner(None).unwrap();
        harness.store.update_task(record.clone()).unwrap();
        let entry = harness
            .store
            .queue_entry_for_task_turn(first.task_id())
            .unwrap()
            .unwrap();
        harness.store.remove_queued(entry.job_id()).unwrap();
        record
    };
    let second = harness.prepare(&second_expected, "second", 54, 5_020);
    harness
        .client(&runner)
        .say_prepared(&second, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();

    let error = harness
        .client(&runner)
        .say_prepared(&first, false, &mut Vec::new(), &mut Vec::new())
        .unwrap_err();
    assert_eq!(code(&error), "TASK_REVISION_CONFLICT");
    // The newer turn is untouched.
    let live = harness.store.load_task(first.task_id()).unwrap();
    assert_eq!(live.status().turns().len(), 3);
    assert_eq!(
        live.status().turns().last().unwrap().turn_id(),
        second.turn_id()
    );
}

#[test]
fn cancel_fence_rejects_a_later_turn_and_replays_idempotently() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let task_id = TaskId::new(Uuid::from_u128(61));
    let expected = harness.plant_open_task(61, 62);
    let prepared = harness.prepare(&expected, "to cancel", 63, 6_000);
    harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();

    // A stale snapshot from before the follow-up cannot cancel the new turn.
    let stale_error = harness
        .client(&runner)
        .cancel_from_expected(&expected)
        .unwrap_err();
    assert_eq!(code(&stale_error), "TASK_REVISION_CONFLICT");

    // Fenced cancel of the live turn succeeds through the local waiting path.
    let live = harness.store.load_task(task_id).unwrap();
    let report = harness.client(&runner).cancel_from_expected(&live).unwrap();
    assert_eq!(report.status().state(), TaskState::Open);
    assert_eq!(
        report.status().last_outcome(),
        Some(&TaskOutcome::Cancelled)
    );

    // Replay of the same fence is idempotent, with no queue row left behind.
    let replayed = harness.client(&runner).cancel_from_expected(&live).unwrap();
    assert_eq!(replayed.status().state(), TaskState::Open);
    assert!(
        harness
            .store
            .queue_entry_for_task_turn(task_id)
            .unwrap()
            .is_none()
    );

    // A new follow-up still works, and the old fence still cannot touch it.
    let current = harness.store.load_task(task_id).unwrap();
    let next = harness.prepare(&current, "after cancel", 64, 6_010);
    harness
        .client(&runner)
        .say_prepared(&next, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    let fenced_error = harness
        .client(&runner)
        .cancel_from_expected(&live)
        .unwrap_err();
    assert_eq!(code(&fenced_error), "TASK_REVISION_CONFLICT");
    let live = harness.store.load_task(task_id).unwrap();
    assert_eq!(
        live.status().turns().last().unwrap().turn_id(),
        next.turn_id()
    );

    // The ordinary path still delegates to the fence.
    let ordinary = harness.client(&runner).cancel(task_id).unwrap();
    assert_eq!(ordinary.status().state(), TaskState::Open);
}

#[test]
fn resume_preserves_independent_sidecars() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let task_id = TaskId::new(Uuid::from_u128(71));
    let expected = harness.plant_open_task(71, 72);
    let prepared = harness.prepare(&expected, "sidecars", 73, 7_000);
    harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();

    // Independent evidence lands after materialization: a fetched head plus an
    // agent-reported check.
    let fetched: BaseOid = "b".repeat(40).parse().unwrap();
    let live = harness.store.load_task(task_id).unwrap();
    let checked = live
        .status()
        .clone()
        .with_reported_checks(vec![ReportedCheck::new(
            "unit",
            "cargo test",
            ReportedCheckStatus::Pass,
            "agent ran tests",
        )])
        .unwrap();
    let updated = live
        .with_status(checked)
        .unwrap()
        .with_fetched_head(Some(fetched.clone()))
        .unwrap();
    harness.store.update_task(updated).unwrap();

    let report = harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    assert_eq!(report.status().turns().len(), 2);
    let live = harness.store.load_task(task_id).unwrap();
    assert_eq!(live.status().turns().len(), 2);
    assert_eq!(live.fetched_head(), Some(&fetched));
    assert_eq!(live.status().reported_checks().len(), 1);
    assert_eq!(harness.queue_rows_for(task_id), 1);
}

#[test]
fn capacity_is_summed_slots_and_park_replay_is_stable() {
    // Regression pin: ordinary capacity is the summed-slot helper, never
    // workers.len().
    let multi = Config::parse(
        "version = 1\n\n[[workers]]\nname = \"a\"\nssh = \"h1\"\nslots = 2\n\n[[workers]]\nname = \"b\"\nssh = \"h2\"\nslots = 3\n",
    )
    .unwrap();
    assert_eq!(multi.configured_runner_slots(), 5);
    assert_eq!(multi.worker_slot_ceilings().len(), 2);
    assert_ne!(
        multi.configured_runner_slots(),
        multi.worker_slot_ceilings().len()
    );

    // With one summed slot, the second task saturates and parks; replaying the
    // same prepared value keeps the single parked row instead of erroring on
    // the already-parked state.
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let first_expected = harness.plant_open_task(81, 82);
    let first = harness.prepare(&first_expected, "first", 83, 8_000);
    harness
        .client(&runner)
        .say_prepared(&first, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    assert_eq!(harness.executor.starts(), 1);

    let second_expected = harness.plant_open_task(84, 85);
    let second = harness.prepare(&second_expected, "second", 86, 8_010);
    harness
        .client(&runner)
        .say_prepared(&second, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    let parked = harness
        .store
        .queue_entry_for_task_turn(second.task_id())
        .unwrap()
        .unwrap();
    assert_eq!(parked.job_id(), second.turn_id());
    assert!(matches!(
        parked.state(),
        mac_worker::job::QueueState::Parked
    ));
    assert_eq!(harness.executor.starts(), 1);

    // Replay while still saturated: still converged, still parked, no spawn.
    harness
        .client(&runner)
        .say_prepared(&second, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    let parked = harness
        .store
        .queue_entry_for_task_turn(second.task_id())
        .unwrap()
        .unwrap();
    assert_eq!(parked.job_id(), second.turn_id());
    assert!(matches!(
        parked.state(),
        mac_worker::job::QueueState::Parked
    ));
    assert_eq!(harness.executor.starts(), 1);
    let live = harness.store.load_task(second.task_id()).unwrap();
    assert_eq!(live.status().turns().len(), 2);
}

#[test]
fn tampered_deserialized_prepared_conflicts() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let expected = harness.plant_open_task(101, 102);
    let prepared = harness.prepare(&expected, "original", 103, 10_000);

    // A deserialized value with a redefined turn number is not the original
    // intent, even though its own snapshot is intact.
    let mut tampered = serde_json::to_value(&prepared).unwrap();
    tampered["turn_number"] = serde_json::json!(99u32);
    let tampered: PreparedFollowup = serde_json::from_value(tampered).unwrap();
    let error = harness
        .client(&runner)
        .say_prepared(&tampered, false, &mut Vec::new(), &mut Vec::new())
        .unwrap_err();
    assert_eq!(code(&error), "TASK_REVISION_CONFLICT");

    // Same for a redefined composed prompt.
    let mut tampered = serde_json::to_value(&prepared).unwrap();
    tampered["composed_prompt"] = serde_json::json!("forged prompt");
    let tampered: PreparedFollowup = serde_json::from_value(tampered).unwrap();
    let error = harness
        .client(&runner)
        .say_prepared(&tampered, false, &mut Vec::new(), &mut Vec::new())
        .unwrap_err();
    assert_eq!(code(&error), "TASK_REVISION_CONFLICT");

    // The untampered value still works and materializes exactly one turn.
    harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    assert_eq!(
        harness
            .store
            .load_task(prepared.task_id())
            .unwrap()
            .status()
            .turns()
            .len(),
        2
    );
}

#[test]
fn terminal_turn_with_stale_queue_row_reports_without_reenqueue() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let task_id = TaskId::new(Uuid::from_u128(111));
    let expected = harness.plant_open_task(111, 112);
    let prepared = harness.prepare(&expected, "finish me", 113, 11_000);
    harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    let starts = harness.executor.starts();

    // The turn completed but its queue row is still present (stale row): the
    // replay must report the durable outcome, never restart N.
    emulate_completion(&harness.store, task_id, 11_010, base_oid());
    let report = harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    assert_eq!(report.status().turns().len(), 2);
    assert_eq!(
        report.status().turns().last().unwrap().turn_id(),
        prepared.turn_id()
    );
    assert_eq!(harness.executor.starts(), starts);
    let live = harness.store.load_task(task_id).unwrap();
    assert_eq!(live.status().turns().len(), 2);
}

#[test]
fn missing_row_with_live_runner_is_busy() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let task_id = TaskId::new(Uuid::from_u128(121));
    let expected = harness.plant_open_task(121, 122);
    let prepared = harness.prepare(&expected, "still running", 123, 12_000);
    harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();

    // Queue row lost while the runner is still recorded: live execution, not
    // a never-queued publication gap. Refuse instead of re-enqueueing.
    let row = harness
        .store
        .queue_entry_for_task_turn(task_id)
        .unwrap()
        .unwrap();
    harness.store.remove_queued(row.job_id()).unwrap();
    assert!(harness.store.load_task(task_id).unwrap().runner().is_some());

    let error = harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap_err();
    assert_eq!(code(&error), "TASK_BUSY");
    let live = harness.store.load_task(task_id).unwrap();
    assert_eq!(live.status().turns().len(), 2);
    assert!(
        harness
            .store
            .queue_entry_for_task_turn(task_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn concurrent_same_prepared_handles_converge_on_one_turn() {
    let harness = Harness::new();
    let expected = harness.plant_open_task(131, 132);
    let prepared = harness.prepare(&expected, "race me", 133, 13_000);

    // Independent handles share only real task/queue CAS. The static lock
    // stays held for the whole test (standard discipline); threads share
    // only the Sync pieces, never the guard-holding Harness.
    let runner = SystemProcessRunner;
    let barrier = std::sync::Barrier::new(4);
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..4 {
            handles.push(scope.spawn(|| {
                barrier.wait();
                TaskClient::new(
                    &runner,
                    &harness.config,
                    &harness.paths,
                    &harness.store,
                    &harness.executor,
                )
                .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
                .map(|report| report.status().turns().len())
            }));
        }
        for handle in handles {
            assert_eq!(handle.join().unwrap().unwrap(), 2);
        }
    });

    // Real CAS decided one winner; every handle converged on the same N.
    let live = harness.store.load_task(prepared.task_id()).unwrap();
    assert_eq!(live.status().turns().len(), 2);
    assert_eq!(
        live.status().turns().last().unwrap().turn_id(),
        prepared.turn_id()
    );
    assert_eq!(
        harness
            .store
            .read_turn_prompt(prepared.task_id(), prepared.turn_id())
            .unwrap(),
        prepared.composed_prompt()
    );
    assert_eq!(
        harness
            .store
            .queue_entry_for_task_turn(prepared.task_id())
            .unwrap()
            .unwrap()
            .job_id(),
        prepared.turn_id()
    );
    assert_eq!(harness.executor.starts(), 1);
}

#[test]
fn cancel_closed_task_conflicts_for_stale_snapshot_and_reports_fresh() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let task_id = TaskId::new(Uuid::from_u128(141));
    let _planted = harness.plant_open_task(141, 142);
    let stale = harness.store.load_task(task_id).unwrap();

    let live = harness.store.load_task(task_id).unwrap();
    let closed_status = TaskStatus::new(
        TaskState::Closed,
        live.status().last_outcome().cloned(),
        live.status().worker().map(str::to_owned),
        live.status().session_present(),
        live.status().head_oid().cloned(),
        live.status().summary().map(str::to_owned),
        live.status().questions().to_vec(),
        live.status().files_changed().to_vec(),
        live.status().diff_stat().map(str::to_owned),
        live.status().turns().to_vec(),
        14_000,
    )
    .unwrap();
    let closed = live.with_status(closed_status).unwrap();
    harness.store.update_task(closed.clone()).unwrap();

    // Stale pre-close snapshot on a closed task: conflict, never a loose
    // success presenting the closed task as the cancellation.
    let error = harness
        .client(&runner)
        .cancel_from_expected(&stale)
        .unwrap_err();
    assert_eq!(code(&error), "TASK_REVISION_CONFLICT");

    // Fresh snapshot preserves legacy behavior: current report, no error.
    let report = harness
        .client(&runner)
        .cancel_from_expected(&closed)
        .unwrap();
    assert_eq!(report.status().state(), TaskState::Closed);
    let ordinary = harness.client(&runner).cancel(task_id).unwrap();
    assert_eq!(ordinary.status().state(), TaskState::Closed);
}

#[test]
fn cancel_after_terminal_done_conflicts_for_stale_snapshot() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let expected = harness.plant_open_task(151, 152);
    let prepared = harness.prepare(&expected, "will finish", 153, 15_000);
    harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    let row = harness
        .store
        .queue_entry_for_task_turn(prepared.task_id())
        .unwrap()
        .unwrap();
    harness.store.remove_queued(row.job_id()).unwrap();
    // Stale pre-completion snapshot: same turn, non-terminal view.
    let stale = harness.store.load_task(prepared.task_id()).unwrap();
    let completed = emulate_completion(&harness.store, prepared.task_id(), 15_010, base_oid());
    let _ = completed;

    // Stale fence: conflicts even though the live turn is terminal Done —
    // never a loose success for terminal outcomes.
    let error = harness
        .client(&runner)
        .cancel_from_expected(&stale)
        .unwrap_err();
    assert_eq!(code(&error), "TASK_REVISION_CONFLICT");

    // Fresh eyes on the completed turn: safe no-op report, so ordinary
    // cancel stays usable after completion.
    let current = harness.store.load_task(prepared.task_id()).unwrap();
    let report = harness
        .client(&runner)
        .cancel_from_expected(&current)
        .unwrap();
    assert_eq!(report.status().state(), TaskState::Open);
}

#[test]
fn cancel_with_same_turn_sidecar_drift_conflicts() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let task_id = TaskId::new(Uuid::from_u128(161));
    let expected = harness.plant_open_task(161, 162);
    let prepared = harness.prepare(&expected, "drift me", 163, 16_000);
    harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();

    // Same turn, but the live revision moved (remote progress publication
    // with a newer timestamp): the stale fence conflicts instead of
    // cancelling loosely.
    let snapshot = harness.store.load_task(task_id).unwrap();
    let drifted = TaskStatus::new(
        snapshot.status().state(),
        snapshot.status().last_outcome().cloned(),
        snapshot.status().worker().map(str::to_owned),
        snapshot.status().session_present(),
        snapshot.status().head_oid().cloned(),
        Some("agent progress update".to_owned()),
        snapshot.status().questions().to_vec(),
        snapshot.status().files_changed().to_vec(),
        snapshot.status().diff_stat().map(str::to_owned),
        snapshot.status().turns().to_vec(),
        snapshot.status().updated_at_millis() + 7,
    )
    .unwrap()
    .copying_reported_checks(snapshot.status())
    .unwrap();
    harness
        .store
        .update_task(snapshot.with_status(drifted).unwrap())
        .unwrap();

    let error = harness
        .client(&runner)
        .cancel_from_expected(&snapshot)
        .unwrap_err();
    assert_eq!(code(&error), "TASK_REVISION_CONFLICT");

    // A re-read fence still cancels.
    let current = harness.store.load_task(task_id).unwrap();
    let report = harness
        .client(&runner)
        .cancel_from_expected(&current)
        .unwrap();
    assert_eq!(
        report.status().last_outcome(),
        Some(&TaskOutcome::Cancelled)
    );
}

#[test]
fn same_n_different_expected_revision_conflicts_before_effects() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let task_id = TaskId::new(Uuid::from_u128(171));
    let expected = harness.plant_open_task(171, 172);
    let prepared = harness.prepare(&expected, "original", 173, 17_000);
    harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    let starts = harness.executor.starts();
    let rows_before = harness.queue_rows_for(task_id);

    // Same N, same message/time/base, but a different self-consistent
    // expected revision (newer timestamp, same turns and head). The
    // full-struct digest binds the expected snapshot, so this is a
    // different preparation, not a replay.
    let revised_status = TaskStatus::new(
        expected.status().state(),
        expected.status().last_outcome().cloned(),
        expected.status().worker().map(str::to_owned),
        expected.status().session_present(),
        expected.status().head_oid().cloned(),
        expected.status().summary().map(str::to_owned),
        expected.status().questions().to_vec(),
        expected.status().files_changed().to_vec(),
        expected.status().diff_stat().map(str::to_owned),
        expected.status().turns().to_vec(),
        expected.status().updated_at_millis() + 50,
    )
    .unwrap()
    .copying_reported_checks(expected.status())
    .unwrap();
    let revised = expected.with_status(revised_status).unwrap();
    let other =
        PreparedFollowup::prepare(&revised, "original".to_owned(), prepared.turn_id(), 17_000)
            .unwrap();
    assert_ne!(other.binding(), prepared.binding());

    let error = harness
        .client(&runner)
        .say_prepared(&other, false, &mut Vec::new(), &mut Vec::new())
        .unwrap_err();
    assert_eq!(code(&error), "TASK_REVISION_CONFLICT");

    // Conflict happened before effects: same two turns, same queue row,
    // same prompt bytes, no new spawn.
    let live = harness.store.load_task(task_id).unwrap();
    assert_eq!(live.status().turns().len(), 2);
    assert_eq!(
        live.status().turns().last().unwrap().turn_id(),
        prepared.turn_id()
    );
    assert_eq!(harness.queue_rows_for(task_id), rows_before);
    assert_eq!(
        harness
            .store
            .read_turn_prompt(task_id, prepared.turn_id())
            .unwrap(),
        prepared.composed_prompt()
    );
    assert_eq!(
        harness
            .store
            .read_turn_prepared_binding(task_id, prepared.turn_id())
            .unwrap(),
        prepared.binding()
    );
    assert_eq!(harness.executor.starts(), starts);
}

#[test]
fn completed_followup_with_changed_head_replays_terminal_report() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let task_id = TaskId::new(Uuid::from_u128(181));
    let expected = harness.plant_open_task(181, 182);
    let prepared = harness.prepare(&expected, "ship it", 183, 18_000);
    harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    let starts = harness.executor.starts();

    // Actual completion: result HEAD advances, queue row and prompt retire
    // through the normal path, binding persists.
    let result_head: BaseOid = "c".repeat(40).parse().unwrap();
    emulate_completion(&harness.store, task_id, 18_010, result_head.clone());
    retire_completed_turn(&harness.store, task_id, prepared.turn_id());
    assert!(
        harness
            .store
            .queue_entry_for_task_turn(task_id)
            .unwrap()
            .is_none()
    );
    assert!(
        harness
            .store
            .read_turn_prompt(task_id, prepared.turn_id())
            .is_err()
    );
    assert_eq!(
        harness
            .store
            .read_turn_prepared_binding(task_id, prepared.turn_id())
            .unwrap(),
        prepared.binding()
    );

    // Exact replay of the original preparation: terminal report, no second
    // execution, no prompt rewrite, head untouched.
    let report = harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    assert_eq!(report.status().state(), TaskState::Open);
    assert_eq!(report.status().turns().len(), 2);
    assert_eq!(
        report.status().turns().last().unwrap().turn_id(),
        prepared.turn_id()
    );
    assert_eq!(
        harness
            .store
            .load_task(task_id)
            .unwrap()
            .status()
            .head_oid(),
        Some(&result_head)
    );
    assert!(
        harness
            .store
            .queue_entry_for_task_turn(task_id)
            .unwrap()
            .is_none()
    );
    assert!(
        harness
            .store
            .read_turn_prompt(task_id, prepared.turn_id())
            .is_err()
    );
    assert_eq!(harness.executor.starts(), starts);
}

#[test]
fn terminal_replay_rejects_different_message_same_turn() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let expected = harness.plant_open_task(191, 192);
    let prepared = harness.prepare(&expected, "original", 193, 19_000);
    harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    let result_head: BaseOid = "d".repeat(40).parse().unwrap();
    emulate_completion(&harness.store, prepared.task_id(), 19_010, result_head);
    retire_completed_turn(&harness.store, prepared.task_id(), prepared.turn_id());

    // Same N, same time, different message: self-consistent on its own, but
    // the durable binding proves it is not the executed preparation — even
    // though the prompt is retired and the head advanced.
    let tampered = PreparedFollowup::prepare(
        &expected,
        "different".to_owned(),
        prepared.turn_id(),
        19_000,
    )
    .unwrap();
    let error = harness
        .client(&runner)
        .say_prepared(&tampered, false, &mut Vec::new(), &mut Vec::new())
        .unwrap_err();
    assert_eq!(code(&error), "TASK_REVISION_CONFLICT");
    let live = harness.store.load_task(prepared.task_id()).unwrap();
    assert_eq!(live.status().turns().len(), 2);
}

#[test]
fn sidecar_only_drift_still_says_fresh_and_keeps_binding() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let task_id = TaskId::new(Uuid::from_u128(201));
    let expected = harness.plant_open_task(201, 202);
    let prepared = harness.prepare(&expected, "drifted live", 203, 20_000);

    // fetched_head/check-only update preserves the operator revision. The
    // forward CAS must refence and preserve these sidecars instead of
    // false-conflicting, and the persisted binding must not change.
    let fetched: BaseOid = "b".repeat(40).parse().unwrap();
    let snapshot = harness.store.load_task(task_id).unwrap();
    let checked = snapshot
        .status()
        .clone()
        .with_reported_checks(vec![ReportedCheck::new(
            "unit",
            "cargo test",
            ReportedCheckStatus::Pass,
            "agent ran tests",
        )])
        .unwrap();
    harness
        .store
        .update_task(
            snapshot
                .with_status(checked)
                .unwrap()
                .with_fetched_head(Some(fetched.clone()))
                .unwrap(),
        )
        .unwrap();

    let report = harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    assert_eq!(report.status().turns().len(), 2);
    assert_eq!(
        report.status().turns().last().unwrap().turn_id(),
        prepared.turn_id()
    );
    let live = harness.store.load_task(task_id).unwrap();
    assert_eq!(live.status().turns().len(), 2);
    assert_eq!(live.fetched_head(), Some(&fetched));
    assert_eq!(live.status().reported_checks().len(), 1);
    assert_eq!(
        harness
            .store
            .read_turn_prepared_binding(task_id, prepared.turn_id())
            .unwrap(),
        prepared.binding()
    );
    assert_eq!(harness.queue_rows_for(task_id), 1);
}

#[test]
fn non_notfound_prompt_read_does_not_publish() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let task_id = TaskId::new(Uuid::from_u128(211));
    let expected = harness.plant_open_task(211, 212);
    let prepared = harness.prepare(&expected, "blocked path", 213, 21_000);

    // A regular file where the turn directory goes makes the prompt read
    // fail with ENOTDIR, not NotFound: no authority to publish. (The task
    // directory itself already exists: planting binds the project context
    // there, so the blocker goes one level deeper.)
    let blocker = harness
        .paths
        .state
        .join("turns")
        .join(task_id.to_string())
        .join(prepared.turn_id().to_string());
    std::fs::write(&blocker, b"blocker").unwrap();
    let error = harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap_err();
    assert_ne!(code(&error), "TASK_REVISION_CONFLICT");
    assert_eq!(
        harness
            .store
            .load_task(task_id)
            .unwrap()
            .status()
            .turns()
            .len(),
        1
    );
    assert_eq!(std::fs::read(&blocker).unwrap(), b"blocker");

    // After removing the blocker the same preparation converges.
    std::fs::remove_file(&blocker).unwrap();
    let report = harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    assert_eq!(report.status().turns().len(), 2);
}

#[test]
fn concurrent_stress_over_many_tasks() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    for i in 0..6u128 {
        let task_number = 220 + i * 10;
        let task_id = TaskId::new(Uuid::from_u128(task_number));
        let expected = harness.plant_open_task(task_number, 320 + i * 10);
        let prepared = harness.prepare(&expected, "stress race", 420 + i * 10, 22_000 + i as u64);
        let barrier = std::sync::Barrier::new(4);
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for _ in 0..4 {
                handles.push(scope.spawn(|| {
                    barrier.wait();
                    TaskClient::new(
                        &runner,
                        &harness.config,
                        &harness.paths,
                        &harness.store,
                        &harness.executor,
                    )
                    .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
                    .map(|report| report.status().turns().len())
                }));
            }
            for handle in handles {
                assert_eq!(handle.join().unwrap().unwrap(), 2);
            }
        });
        let live = harness.store.load_task(task_id).unwrap();
        assert_eq!(live.status().turns().len(), 2);
        assert_eq!(
            live.status().turns().last().unwrap().turn_id(),
            prepared.turn_id()
        );
        assert_eq!(
            harness
                .store
                .read_turn_prepared_binding(task_id, prepared.turn_id())
                .unwrap(),
            prepared.binding()
        );
    }
}

#[test]
fn post_park_fault_preserves_published_turn_and_peer_replay_converges() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    // Task A occupies the single summed slot with a live runner.
    let first_expected = harness.plant_open_task(231, 232);
    let first = harness.prepare(&first_expected, "first", 233, 23_000);
    harness
        .client(&runner)
        .say_prepared(&first, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    assert_eq!(harness.executor.starts(), 1);
    // Task B saturates. The existing post-park publication fault fires after
    // the Parked row is durable: the say fails, but Active+N plus every
    // artifact must survive — no rollback, no CAS-back.
    let task_id = TaskId::new(Uuid::from_u128(234));
    let expected = harness.plant_open_task(234, 235);
    let prepared = harness.prepare(&expected, "second", 236, 23_010);
    harness
        .store
        .inject_write_failure_once(ClientStateWritePoint::AfterParkedTaskTurnPublication);
    let error = harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap_err();
    assert!(!code(&error).is_empty());

    let live = harness.store.load_task(task_id).unwrap();
    assert_eq!(live.status().state(), TaskState::Active);
    assert_eq!(live.status().turns().len(), 2);
    assert_eq!(
        live.status().turns().last().unwrap().turn_id(),
        prepared.turn_id()
    );
    let row = harness
        .store
        .queue_entry_for_task_turn(task_id)
        .unwrap()
        .unwrap();
    assert_eq!(row.job_id(), prepared.turn_id());
    assert!(matches!(row.state(), mac_worker::job::QueueState::Parked));
    assert_eq!(
        harness
            .store
            .read_turn_prompt(task_id, prepared.turn_id())
            .unwrap(),
        prepared.composed_prompt()
    );
    assert_eq!(
        harness
            .store
            .read_turn_prepared_binding(task_id, prepared.turn_id())
            .unwrap(),
        prepared.binding()
    );

    // A same-N peer replay (fault consumed) converges on the parked row:
    // two turns, no duplicate execution, no new spawn.
    let report = harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    assert_eq!(report.status().turns().len(), 2);
    assert_eq!(
        report.status().turns().last().unwrap().turn_id(),
        prepared.turn_id()
    );
    assert_eq!(harness.executor.starts(), 1);
    let live = harness.store.load_task(task_id).unwrap();
    assert_eq!(live.status().turns().len(), 2);
}

/// Fake worker remote that answers one `task-cancel` with a canned (old)
/// response, publishing a late world — terminal N plus a later N+1 — while
/// the cancellation is in flight. Proves the post-I/O fence: no overwrite,
/// no N+1/Done presented as the cancellation.
struct LateCancelRemote<'a> {
    store: &'a ClientStateStore,
    task_id: TaskId,
    expected_turn: TurnId,
    next_turn: TurnId,
    result_head: BaseOid,
    done_at: u64,
    next_started_at: u64,
    next_updated_at: u64,
    response_status: Mutex<TaskStatus>,
    calls: AtomicUsize,
    late_applied: AtomicBool,
}

impl<'a> LateCancelRemote<'a> {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn canonical<T: serde::Serialize>(value: &T) -> Result<ProcessResult, WorkerError> {
        let mut stdout =
            serde_json::to_vec(value).map_err(|error| WorkerError::Protocol(error.to_string()))?;
        stdout.push(b'\n');
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout,
            stderr: Vec::new(),
        })
    }

    fn publish_late_world(&self) {
        // Normal terminal N with an advanced result head, then a later N+1
        // pending turn — exactly what a late host response races with.
        let record = self.store.load_task(self.task_id).unwrap();
        let mut turns = record.status().turns().to_vec();
        let last = turns.pop().unwrap();
        assert_eq!(last.turn_id(), self.expected_turn);
        turns.push(TurnSummary::new(
            last.turn_number(),
            last.turn_id(),
            Some(TurnTerminal::Succeeded),
            Some(TaskOutcome::Done),
            Some(true),
            false,
            last.started_at_millis(),
            Some(self.done_at),
        ));
        turns.push(TurnSummary::new(
            last.turn_number() + 1,
            self.next_turn,
            None,
            None,
            None,
            false,
            Some(self.next_started_at),
            None,
        ));
        let status = TaskStatus::new(
            TaskState::Active,
            Some(TaskOutcome::Done),
            record.status().worker().map(str::to_owned),
            record.status().session_present(),
            Some(self.result_head.clone()),
            record.status().summary().map(str::to_owned),
            record.status().questions().to_vec(),
            record.status().files_changed().to_vec(),
            record.status().diff_stat().map(str::to_owned),
            turns,
            self.next_updated_at,
        )
        .unwrap()
        .copying_reported_checks(record.status())
        .unwrap();
        self.store
            .update_task(record.with_status(status).unwrap())
            .unwrap();
    }
}

impl ProcessRunner for LateCancelRemote<'_> {
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
        if operation != HostOperation::TaskCancel.command() {
            return Err(WorkerError::Protocol(format!(
                "unexpected fixture operation: {operation}"
            )));
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        let stdin = request
            .stdin
            .as_deref()
            .ok_or_else(|| WorkerError::Protocol("cancel request had no stdin".into()))?;
        let cancel: TaskCancelRequest = serde_json::from_slice(stdin)
            .map_err(|error| WorkerError::Protocol(error.to_string()))?;
        assert_eq!(cancel.task_id(), self.task_id);
        assert_eq!(cancel.turn_id(), self.expected_turn);
        if !self.late_applied.swap(true, Ordering::SeqCst) {
            self.publish_late_world();
        }
        let status = self.response_status.lock().unwrap().clone();
        Self::canonical(&TaskCancelResponse::new(status))
    }
}

fn cancelled_for_turn(record: &LocalTaskRecord, ended_at: u64) -> TaskStatus {
    let mut turns = record.status().turns().to_vec();
    let last = turns.pop().unwrap();
    turns.push(TurnSummary::new(
        last.turn_number(),
        last.turn_id(),
        Some(TurnTerminal::Cancelled),
        Some(TaskOutcome::Cancelled),
        Some(false),
        false,
        last.started_at_millis(),
        Some(ended_at),
    ));
    TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Cancelled),
        record.status().worker().map(str::to_owned),
        record.status().session_present(),
        record.status().head_oid().cloned(),
        record.status().summary().map(str::to_owned),
        record.status().questions().to_vec(),
        record.status().files_changed().to_vec(),
        record.status().diff_stat().map(str::to_owned),
        turns,
        ended_at,
    )
    .unwrap()
    .copying_reported_checks(record.status())
    .unwrap()
}

#[test]
fn late_cancel_response_cannot_overwrite_newer_turn() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let task_id = TaskId::new(Uuid::from_u128(241));
    let expected = harness.plant_open_task(241, 242);
    let prepared = harness.prepare(&expected, "cancel me late", 243, 24_000);
    harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    // Rowless Active N with the prompt kept forces the host cancellation
    // path (the local waiting path has no row to retain).
    let row = harness
        .store
        .queue_entry_for_task_turn(task_id)
        .unwrap()
        .unwrap();
    harness.store.remove_queued(row.job_id()).unwrap();
    let snapshot = harness.store.load_task(task_id).unwrap();

    let result_head: BaseOid = "e".repeat(40).parse().unwrap();
    let fake = LateCancelRemote {
        store: &harness.store,
        task_id,
        expected_turn: prepared.turn_id(),
        next_turn: TurnId::new(Uuid::from_u128(244)),
        result_head: result_head.clone(),
        done_at: 24_010,
        next_started_at: 24_015,
        next_updated_at: 24_020,
        response_status: Mutex::new(cancelled_for_turn(&snapshot, 24_005)),
        calls: AtomicUsize::new(0),
        late_applied: AtomicBool::new(false),
    };
    let client = TaskClient::new(
        &fake,
        &harness.config,
        &harness.paths,
        &harness.store,
        &harness.executor,
    );
    let error = client.cancel_from_expected(&snapshot).unwrap_err();
    assert_eq!(code(&error), "TASK_REVISION_CONFLICT");
    assert_eq!(fake.calls(), 1);

    // No overwrite: terminal N Done with the advanced head plus pending N+1
    // survive intact; N's prompt and binding are untouched.
    let live = harness.store.load_task(task_id).unwrap();
    assert_eq!(live.status().turns().len(), 3);
    assert_eq!(live.status().turns()[1].turn_id(), prepared.turn_id());
    assert_eq!(live.status().turns()[1].outcome(), Some(&TaskOutcome::Done));
    assert_eq!(
        live.status().turns().last().unwrap().turn_id(),
        TurnId::new(Uuid::from_u128(244))
    );
    assert_eq!(live.status().head_oid(), Some(&result_head));
    assert_eq!(
        harness
            .store
            .read_turn_prompt(task_id, prepared.turn_id())
            .unwrap(),
        prepared.composed_prompt()
    );
    assert_eq!(
        harness
            .store
            .read_turn_prepared_binding(task_id, prepared.turn_id())
            .unwrap(),
        prepared.binding()
    );
}

#[test]
fn already_cancelled_same_turn_converges_without_host_call() {
    let harness = Harness::new();
    let runner = SystemProcessRunner;
    let task_id = TaskId::new(Uuid::from_u128(251));
    let expected = harness.plant_open_task(251, 252);
    let prepared = harness.prepare(&expected, "already done", 253, 25_000);
    harness
        .client(&runner)
        .say_prepared(&prepared, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();
    let snapshot = harness.store.load_task(task_id).unwrap();

    // The cancellation lands through another handle first: same N terminal
    // Cancelled, queue row retired.
    let row = harness
        .store
        .queue_entry_for_task_turn(task_id)
        .unwrap()
        .unwrap();
    harness.store.remove_queued(row.job_id()).unwrap();
    harness
        .store
        .update_task(
            snapshot
                .with_status(cancelled_for_turn(&snapshot, 25_010))
                .unwrap(),
        )
        .unwrap();

    let fake = LateCancelRemote {
        store: &harness.store,
        task_id,
        expected_turn: prepared.turn_id(),
        next_turn: TurnId::new(Uuid::from_u128(254)),
        result_head: base_oid(),
        done_at: 25_010,
        next_started_at: 25_015,
        next_updated_at: 25_020,
        response_status: Mutex::new(cancelled_for_turn(&snapshot, 25_005)),
        calls: AtomicUsize::new(0),
        late_applied: AtomicBool::new(false),
    };
    let client = TaskClient::new(
        &fake,
        &harness.config,
        &harness.paths,
        &harness.store,
        &harness.executor,
    );
    let report = client.cancel_from_expected(&snapshot).unwrap();
    assert_eq!(report.status().state(), TaskState::Open);
    assert_eq!(
        report.status().last_outcome(),
        Some(&TaskOutcome::Cancelled)
    );
    assert_eq!(fake.calls(), 0);
    let live = harness.store.load_task(task_id).unwrap();
    assert_eq!(live.status().turns().len(), 2);
}
