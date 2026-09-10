use std::{
    fs::{self, OpenOptions},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    sync::{Arc, Barrier},
    thread,
};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy, TurnLimits},
    client_state::{
        ActiveTaskConfig, ClientStateStore, ClientStateWritePoint, RunnerSlotDecision,
        task_record_needs_active_index,
    },
    job::{CommandSummary, JobId, ProcessIdentity, QueueEntry, QueueEntryKind, QueueRunReference},
    scheduler::WorkerPreference,
    supervisor::{
        ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
    },
    task::{
        ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, RunId as TaskRunId, RunnerIdentity,
        TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState,
        TaskStatus, TurnSummary,
    },
};
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const BASE_OID: &str = "0123456789abcdef0123456789abcdef01234567";
const REPO_ID: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

fn open_store() -> (tempfile::TempDir, ClientStateStore, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().canonicalize().unwrap().join("state");
    let store = ClientStateStore::open(&state_path).unwrap();
    (dir, store, state_path)
}

#[derive(Clone, Copy)]
struct MatchingInspector;

impl ProcessInspector for MatchingInspector {
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

fn open_live_store() -> (tempfile::TempDir, ClientStateStore, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().canonicalize().unwrap().join("state");
    let store =
        ClientStateStore::open_with_owner_inspector(&state_path, MatchingInspector).unwrap();
    (dir, store, state_path)
}

fn live_identity(pid: u32) -> ProcessIdentity {
    ProcessIdentity::new(pid, u64::from(pid) * 10_000 + 7).expect("fixture live identity")
}

fn open_holder(n: u128, pid: u32) -> LocalTaskRecord {
    record_with_id(n, TaskState::Open, false)
        .with_runner(Some(RunnerIdentity::new(live_identity(pid))))
        .expect("open holder")
}

fn record_with_id(n: u128, state: TaskState, runner: bool) -> LocalTaskRecord {
    let meta = TaskMeta::new(TaskMetaInput {
        task_id: TaskId::new(Uuid::from_u128(n)),
        run_id: None,
        project_id: PROJECT_ID.to_owned(),
        worktree_id: WORKTREE_ID.to_owned(),
        agent: AgentKind::Codex,
        model: Some("gpt-5".into()),
        effort: None,
        policy: PermissionPolicy::Workspace,
        source: TaskSource::Local {
            wip: false,
            push_target: None,
        },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid: BASE_OID.parse().expect("fixture base oid"),
        limits: TaskLimits::new(TurnLimits::new(30 * 60 * 1000, None, None).unwrap(), 10)
            .expect("fixture limits"),
        close_policy: ClosePolicy::Done,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: "Fix the flaky login spec\n\nDetails…".into(),
        created_at_millis: 1_700_000_000_000,
    })
    .expect("fixture meta");
    let turn_id = format!("{n:032x}").parse().expect("fixture turn id");
    let status = TaskStatus::new(
        state,
        None,
        None,
        false,
        None,
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![TurnSummary::new(
            1, turn_id, None, None, None, false, None, None,
        )],
        1_700_000_000_000,
    )
    .expect("fixture status");
    let runner = runner.then(|| {
        RunnerIdentity::new(ProcessIdentity::new(42, 1_700_000_000_001).expect("fixture process"))
    });
    LocalTaskRecord::new(
        meta,
        status,
        None,
        runner,
        None,
        REPO_ID.to_owned(),
        Some("mini-1".into()),
        true,
        None,
    )
    .expect("fixture record")
}

fn select_all(store: &ClientStateStore) -> mac_worker::client_state::ActiveTaskSelection {
    store
        .select_active_task_ids(&ActiveTaskConfig::default())
        .unwrap()
}

fn receipt_path(state: &Path, task_id: TaskId) -> PathBuf {
    state.join("active-tasks").join(format!("{task_id}.json"))
}

fn task_path(state: &Path, task_id: TaskId) -> PathBuf {
    state.join("tasks").join(format!("{task_id}.json"))
}

fn queue_owner() -> ProcessIdentity {
    ProcessIdentity::new(90_000, 90_000_001).expect("fixture queue owner")
}

fn enqueue_task_turn(store: &ClientStateStore, record: &LocalTaskRecord) {
    let turn_id = record.status().turns()[0].turn_id();
    let entry = QueueEntry::new(
        turn_id,
        store.client_id(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        CommandSummary::shell(),
        Vec::new(),
        WorkerPreference::Automatic,
        QueueEntryKind::TaskTurn,
        None,
        queue_owner(),
        1_700_000_000_000,
    )
    .unwrap();
    store.enqueue(entry).unwrap();
}

fn task_run(n: u128) -> TaskRunId {
    TaskRunId::new(Uuid::from_u128(n))
}

fn queue_run(n: u128, max_parallel: u32) -> QueueRunReference {
    QueueRunReference::new(
        mac_worker::job::RunId::new(task_run(n).to_string()).unwrap(),
        max_parallel,
    )
    .unwrap()
}

fn job_id(n: u128) -> JobId {
    JobId::new(Uuid::from_u128(n))
}

fn record_in_run(n: u128, state: TaskState, runner: bool, run_id: TaskRunId) -> LocalTaskRecord {
    let meta = TaskMeta::new(TaskMetaInput {
        task_id: TaskId::new(Uuid::from_u128(n)),
        run_id: Some(run_id),
        project_id: PROJECT_ID.to_owned(),
        worktree_id: WORKTREE_ID.to_owned(),
        agent: AgentKind::Codex,
        model: Some("gpt-5".into()),
        effort: None,
        policy: PermissionPolicy::Workspace,
        source: TaskSource::Local {
            wip: false,
            push_target: None,
        },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid: BASE_OID.parse().expect("fixture base oid"),
        limits: TaskLimits::new(TurnLimits::new(30 * 60 * 1000, None, None).unwrap(), 10)
            .expect("fixture limits"),
        close_policy: ClosePolicy::Done,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: "Fix the flaky login spec\n\nDetails…".into(),
        created_at_millis: 1_700_000_000_000,
    })
    .expect("fixture meta");
    let turn_id = job_id(n);
    let status = TaskStatus::new(
        state,
        None,
        None,
        false,
        None,
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![TurnSummary::new(
            1, turn_id, None, None, None, false, None, None,
        )],
        1_700_000_000_000,
    )
    .expect("fixture status");
    let runner = runner.then(|| {
        RunnerIdentity::new(ProcessIdentity::new(42, 1_700_000_000_001).expect("fixture process"))
    });
    LocalTaskRecord::new(
        meta,
        status,
        None,
        runner,
        None,
        REPO_ID.to_owned(),
        Some("mini-1".into()),
        true,
        None,
    )
    .expect("fixture record")
}

fn closed_done_in_run(n: u128, run_id: TaskRunId) -> LocalTaskRecord {
    let base = record_in_run(n, TaskState::Closed, true, run_id);
    let status = TaskStatus::new(
        TaskState::Closed,
        Some(TaskOutcome::Done),
        None,
        false,
        None,
        None,
        Vec::new(),
        Vec::new(),
        None,
        base.status().turns().to_vec(),
        1_700_000_000_000,
    )
    .expect("closed done status");
    base.with_status(status).expect("closed done record")
}

fn enqueue_run_turn(
    store: &ClientStateStore,
    turn_id: JobId,
    owner: ProcessIdentity,
    run: QueueRunReference,
) {
    let entry = QueueEntry::new(
        turn_id,
        store.client_id(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        CommandSummary::shell(),
        Vec::new(),
        WorkerPreference::Automatic,
        QueueEntryKind::TaskTurn,
        Some(run),
        owner,
        1_700_000_000_000,
    )
    .unwrap();
    store.enqueue(entry).unwrap();
}

fn closed_done_with_runner(n: u128) -> LocalTaskRecord {
    let base = record_with_id(n, TaskState::Closed, true);
    let status = TaskStatus::new(
        TaskState::Closed,
        Some(TaskOutcome::Done),
        None,
        false,
        None,
        None,
        Vec::new(),
        Vec::new(),
        None,
        base.status().turns().to_vec(),
        1_700_000_000_000,
    )
    .expect("closed done status");
    base.with_status(status).expect("closed done record")
}

#[test]
fn queued_record_without_a_queue_row_is_selected() {
    let (_dir, store, _) = open_store();
    let record = record_with_id(1, TaskState::Queued, false);
    assert!(task_record_needs_active_index(&record));
    store.create_task(record.clone()).unwrap();
    let report = select_all(&store);
    assert_eq!(report.selected, vec![record.meta().task_id()]);
    assert!(report.orphan_ids.is_empty());
    assert!(report.failed.is_empty());
}

#[test]
fn active_record_without_a_queue_row_stays_selected_after_ack_shape() {
    let (_dir, store, _) = open_store();
    let record = record_with_id(2, TaskState::Active, false);
    store.create_task(record.clone()).unwrap();
    let report = select_all(&store);
    assert_eq!(report.selected, vec![record.meta().task_id()]);
}

#[test]
fn index_before_task_publish_crash_leaves_orphan_without_hiding_others() {
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().canonicalize().unwrap().join("state");
    let store = ClientStateStore::open_with_write_fault(
        &state_path,
        ClientStateWritePoint::AfterActiveTaskIndexBeforeTaskPublish,
    )
    .unwrap();
    let orphan = record_with_id(3, TaskState::Queued, false);
    store.create_task(orphan.clone()).unwrap_err();
    assert!(!task_path(&state_path, orphan.meta().task_id()).exists());
    assert!(receipt_path(&state_path, orphan.meta().task_id()).exists());

    let healthy = record_with_id(4, TaskState::Queued, false);
    store.create_task(healthy.clone()).unwrap();
    let report = select_all(&store);
    assert_eq!(report.selected, vec![healthy.meta().task_id()]);
    assert_eq!(report.orphan_ids, vec![orphan.meta().task_id()]);
    assert!(report.failed.is_empty());
}

#[test]
fn terminal_row_with_runner_or_rollback_stays_recoverable() {
    let (_dir, store, _) = open_store();
    let closed_runner = record_with_id(5, TaskState::Closed, true);
    store.create_task(closed_runner.clone()).unwrap();
    let rollback = record_with_id(6, TaskState::Abandoned, false)
        .with_abandon_code(Some("SUBMISSION_ROLLBACK_INCOMPLETE".into()))
        .unwrap()
        .with_submission_rollback_turn_id(
            record_with_id(6, TaskState::Abandoned, false)
                .status()
                .turns()[0]
                .turn_id(),
        )
        .unwrap();
    store.create_task(rollback.clone()).unwrap();
    let quiescent = record_with_id(7, TaskState::Closed, false);
    store.create_task(quiescent.clone()).unwrap();

    let report = select_all(&store);
    assert!(report.selected.contains(&closed_runner.meta().task_id()));
    assert!(report.selected.contains(&rollback.meta().task_id()));
    assert!(!report.selected.contains(&quiescent.meta().task_id()));
    assert!(!task_record_needs_active_index(&quiescent));
}

#[test]
fn quiescent_update_retires_and_stale_cas_cannot_drop_an_active_winner() {
    let (_dir, store, _) = open_store();
    let original = record_with_id(8, TaskState::Queued, true);
    store.create_task(original.clone()).unwrap();
    let winner = original.clone().with_runner(None).unwrap();
    store.update_task(winner.clone()).unwrap();
    assert_eq!(select_all(&store).selected, vec![winner.meta().task_id()]);

    let quiescent = record_with_id(8, TaskState::Closed, false);
    assert!(!store.update_task_if_current(&original, quiescent).unwrap());
    assert_eq!(select_all(&store).selected, vec![winner.meta().task_id()]);

    let still_active = winner
        .clone()
        .with_runner(Some(RunnerIdentity::new(
            ProcessIdentity::new(43, 1_700_000_000_002).expect("fixture process"),
        )))
        .unwrap();
    assert!(
        store
            .update_task_if_current(&winner, still_active.clone())
            .unwrap()
    );
    assert_eq!(
        select_all(&store).selected,
        vec![still_active.meta().task_id()]
    );

    store
        .update_task(record_with_id(8, TaskState::Closed, false))
        .unwrap();
    assert!(select_all(&store).selected.is_empty());
}

#[test]
fn bootstrap_rebuilds_an_old_store_and_is_stable_on_retry() {
    let (_dir, store, state_path) = open_store();
    let live = record_with_id(9, TaskState::Queued, false);
    let closed = record_with_id(10, TaskState::Closed, false);
    store.create_task(live.clone()).unwrap();
    store.create_task(closed.clone()).unwrap();
    fs::remove_file(receipt_path(&state_path, live.meta().task_id())).unwrap();
    let bootstrap = store.bootstrap_active_task_index().unwrap();
    assert!(!bootstrap.already_bootstrapped);
    assert_eq!(bootstrap.rebuilt, vec![live.meta().task_id()]);
    assert!(bootstrap.corrupt.is_empty());
    assert_eq!(select_all(&store).selected, vec![live.meta().task_id()]);

    let again = store.bootstrap_active_task_index().unwrap();
    assert!(again.already_bootstrapped);
    assert_eq!(again.rebuilt, bootstrap.rebuilt);
    assert_eq!(again.corrupt, bootstrap.corrupt);
}

#[test]
fn idle_select_skips_a_poisoned_historical_closed_file() {
    let (_dir, store, state_path) = open_store();
    let live = record_with_id(11, TaskState::Queued, false);
    store.create_task(live.clone()).unwrap();
    let poisoned_id = TaskId::new(Uuid::from_u128(12));
    fs::write(task_path(&state_path, poisoned_id), b"{broken-closed").unwrap();
    let report = select_all(&store);
    assert_eq!(report.selected, vec![live.meta().task_id()]);
    assert!(report.failed.is_empty());
}

#[test]
fn busy_and_corrupt_early_entries_do_not_starve_later_work() {
    let (_dir, store, state_path) = open_store();
    let a = record_with_id(21, TaskState::Queued, false);
    let b = record_with_id(22, TaskState::Queued, false);
    let c = record_with_id(23, TaskState::Queued, false);
    let d = record_with_id(24, TaskState::Queued, false);
    for record in [&a, &b, &c, &d] {
        store.create_task(record.clone()).unwrap();
    }
    fs::write(receipt_path(&state_path, b.meta().task_id()), b"{broken").unwrap();

    let hold = OpenOptions::new()
        .read(true)
        .write(true)
        .open(receipt_path(&state_path, a.meta().task_id()))
        .unwrap();
    assert_eq!(unsafe { libc::flock(hold.as_raw_fd(), libc::LOCK_EX) }, 0);
    let started = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let started_thread = Arc::clone(&started);
    let release_thread = Arc::clone(&release);
    let worker = thread::spawn(move || {
        started_thread.wait();
        release_thread.wait();
        drop(hold);
    });
    started.wait();

    let tight = ActiveTaskConfig {
        max_tasks_per_tick: 2,
    };
    let first = store.select_active_task_ids(&tight).unwrap();
    assert_eq!(first.busy_skipped, vec![a.meta().task_id()]);
    assert_eq!(first.failed.len(), 1);
    assert_eq!(first.failed[0].0, b.meta().task_id().to_string());
    assert!(first.selected.is_empty());
    assert!(first.truncated);

    let second = store.select_active_task_ids(&tight).unwrap();
    assert_eq!(
        second.selected,
        vec![c.meta().task_id(), d.meta().task_id()]
    );
    assert!(second.failed.is_empty());
    assert!(!second.truncated);

    release.wait();
    worker.join().unwrap();
}

#[test]
fn quiescent_publish_crash_before_retire_leaves_index_until_heal() {
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().canonicalize().unwrap().join("state");
    let store = ClientStateStore::open(&state_path).unwrap();
    let live = record_with_id(13, TaskState::Queued, false);
    store.create_task(live.clone()).unwrap();
    store.inject_write_failure_once(ClientStateWritePoint::AfterQuiescentTaskBeforeIndexRetire);
    store
        .update_task(record_with_id(13, TaskState::Closed, false))
        .unwrap_err();
    assert!(receipt_path(&state_path, live.meta().task_id()).exists());
    let leftover = select_all(&store);
    assert_eq!(leftover.selected, vec![live.meta().task_id()]);
    assert!(leftover.orphan_ids.is_empty());
    assert!(leftover.failed.is_empty());

    let refresh = store
        .refresh_active_task_index(&leftover.selected, &ActiveTaskConfig::default())
        .unwrap();
    assert_eq!(refresh.retired, vec![live.meta().task_id()]);
    assert!(!receipt_path(&state_path, live.meta().task_id()).exists());
    assert!(select_all(&store).selected.is_empty());
}

#[test]
fn bootstrap_reports_corrupt_task_rows_without_hiding_healthy_work() {
    let (_dir, store, state_path) = open_store();
    let live = record_with_id(14, TaskState::Queued, false);
    store.create_task(live.clone()).unwrap();
    fs::remove_file(receipt_path(&state_path, live.meta().task_id())).unwrap();
    let poisoned_id = TaskId::new(Uuid::from_u128(15));
    fs::write(task_path(&state_path, poisoned_id), b"{broken").unwrap();
    let bootstrap = store.bootstrap_active_task_index().unwrap();
    assert!(bootstrap.rebuilt.contains(&live.meta().task_id()));
    assert!(
        bootstrap
            .corrupt
            .iter()
            .any(|entry| entry.contains(&poisoned_id.to_string()))
    );
    assert_eq!(select_all(&store).selected, vec![live.meta().task_id()]);
}

#[test]
fn identical_cas_heals_a_missing_index_and_conflict_does_not_index_a_peer() {
    let (_dir, store, state_path) = open_store();
    let record = record_with_id(16, TaskState::Queued, false);
    store.create_task(record.clone()).unwrap();
    fs::remove_file(receipt_path(&state_path, record.meta().task_id())).unwrap();
    assert!(
        store
            .update_task_if_current(&record, record.clone())
            .unwrap()
    );
    assert_eq!(select_all(&store).selected, vec![record.meta().task_id()]);

    store
        .create_task(record.clone())
        .expect("idempotent same-record create");
    let conflicted = record_with_id(16, TaskState::Active, false);
    let error = store.create_task(conflicted).unwrap_err();
    assert!(
        error.to_string().contains("TASK_ID_CONFLICT")
            || error.to_string().contains("already present")
    );
    assert_eq!(select_all(&store).selected, vec![record.meta().task_id()]);
}

#[test]
fn closed_done_clears_runner_but_queue_row_stays_selected() {
    let (_dir, store, state_path) = open_store();
    let live = closed_done_with_runner(31);
    let task_id = live.meta().task_id();
    let turn_id = live.status().turns()[0].turn_id();
    store.create_task(live).unwrap();
    enqueue_task_turn(&store, &store.load_task(task_id).unwrap());
    let cleared = store.record_runner(task_id, None).unwrap();
    assert!(!task_record_needs_active_index(&cleared));
    assert!(receipt_path(&state_path, task_id).exists());
    let selected = select_all(&store);
    assert_eq!(selected.selected, vec![task_id]);
    assert!(selected.orphan_ids.is_empty());
    assert!(selected.failed.is_empty());

    store
        .remove_task_turn_after_terminal(turn_id, queue_owner())
        .unwrap();
    assert_eq!(select_all(&store).selected, vec![task_id]);
    let refresh = store
        .refresh_active_task_index(&selected.selected, &ActiveTaskConfig::default())
        .unwrap();
    assert_eq!(refresh.retired, vec![task_id]);
    assert!(refresh.retained.is_empty());
    assert!(refresh.failed.is_empty());
    assert!(!receipt_path(&state_path, task_id).exists());
    assert!(select_all(&store).selected.is_empty());
}

#[test]
fn crash_after_queue_removal_is_healed_by_refresh() {
    let (_dir, store, state_path) = open_store();
    let live = closed_done_with_runner(32);
    let task_id = live.meta().task_id();
    let turn_id = live.status().turns()[0].turn_id();
    store.create_task(live).unwrap();
    enqueue_task_turn(&store, &store.load_task(task_id).unwrap());
    store.record_runner(task_id, None).unwrap();
    store
        .remove_task_turn_after_terminal(turn_id, queue_owner())
        .unwrap();
    assert!(receipt_path(&state_path, task_id).exists());
    assert_eq!(select_all(&store).selected, vec![task_id]);

    let refresh = store
        .refresh_active_task_index(&[task_id], &ActiveTaskConfig::default())
        .unwrap();
    assert_eq!(refresh.retired, vec![task_id]);
    assert!(!receipt_path(&state_path, task_id).exists());
    assert!(select_all(&store).selected.is_empty());
}

#[test]
fn newer_say_before_refresh_preserves_the_index() {
    let (_dir, store, state_path) = open_store();
    let live = closed_done_with_runner(33);
    let task_id = live.meta().task_id();
    let turn_id = live.status().turns()[0].turn_id();
    store.create_task(live.clone()).unwrap();
    enqueue_task_turn(&store, &live);
    let quiescent = store.record_runner(task_id, None).unwrap();
    store
        .remove_task_turn_after_terminal(turn_id, queue_owner())
        .unwrap();
    assert_eq!(select_all(&store).selected, vec![task_id]);

    let say = record_with_id(33, TaskState::Open, false);
    assert!(
        store
            .update_task_if_current(&quiescent, say.clone())
            .unwrap()
    );
    assert!(receipt_path(&state_path, task_id).exists());
    let refresh = store
        .refresh_active_task_index(
            &[task_id],
            &ActiveTaskConfig {
                max_tasks_per_tick: 1,
            },
        )
        .unwrap();
    assert_eq!(refresh.retained, vec![task_id]);
    assert!(refresh.retired.is_empty());
    assert!(!refresh.truncated);
    assert_eq!(select_all(&store).selected, vec![task_id]);
}

#[test]
fn bootstrap_retains_a_quiescent_terminal_with_queue_evidence() {
    let (_dir, store, state_path) = open_store();
    let live = closed_done_with_runner(34);
    let task_id = live.meta().task_id();
    store.create_task(live.clone()).unwrap();
    enqueue_task_turn(&store, &live);
    store.record_runner(task_id, None).unwrap();
    fs::remove_file(receipt_path(&state_path, task_id)).unwrap();
    let bootstrap = store.bootstrap_active_task_index().unwrap();
    assert!(!bootstrap.already_bootstrapped);
    assert_eq!(bootstrap.rebuilt, vec![task_id]);
    assert!(bootstrap.corrupt.is_empty());
    assert_eq!(select_all(&store).selected, vec![task_id]);
}

#[test]
fn occupancy_after_bootstrap_matches_legacy_live_holders_and_reservations() {
    let (_dir, store, _) = open_live_store();
    store.create_task(open_holder(40, 40)).unwrap();
    store.create_task(open_holder(41, 41)).unwrap();
    let queued = record_with_id(42, TaskState::Queued, false);
    store.create_task(queued.clone()).unwrap();
    enqueue_task_turn(&store, &queued);
    assert!(matches!(
        store
            .reserve_runner_slot(
                queued.status().turns()[0].turn_id(),
                queue_owner(),
                8,
                false
            )
            .unwrap(),
        RunnerSlotDecision::Acquired { .. }
    ));
    let before = store.live_runner_slot_count().unwrap();
    store.bootstrap_active_task_index().unwrap();
    assert_eq!(store.live_runner_slot_count().unwrap(), before);
    assert_eq!(before, 3);
}

#[test]
fn open_runner_without_a_queue_row_occupies_a_slot() {
    let (_dir, store, _) = open_live_store();
    store.create_task(open_holder(43, 43)).unwrap();
    store.bootstrap_active_task_index().unwrap();
    assert_eq!(store.live_runner_slot_count().unwrap(), 1);
    let queued = record_with_id(44, TaskState::Queued, false);
    store.create_task(queued.clone()).unwrap();
    enqueue_task_turn(&store, &queued);
    assert!(matches!(
        store
            .reserve_runner_slot(
                queued.status().turns()[0].turn_id(),
                queue_owner(),
                1,
                false
            )
            .unwrap(),
        RunnerSlotDecision::Saturated
    ));
}

#[test]
fn same_turn_live_runner_is_pending() {
    let (_dir, store, _) = open_live_store();
    let live = record_with_id(45, TaskState::Active, false)
        .with_runner(Some(RunnerIdentity::new(live_identity(45))))
        .unwrap();
    store.create_task(live.clone()).unwrap();
    enqueue_task_turn(&store, &live);
    store.bootstrap_active_task_index().unwrap();
    assert!(matches!(
        store
            .reserve_runner_slot(live.status().turns()[0].turn_id(), queue_owner(), 4, false)
            .unwrap(),
        RunnerSlotDecision::Pending { .. }
    ));
}

#[test]
fn acquired_only_below_limit_after_bootstrap() {
    let (_dir, store, _) = open_live_store();
    let first = record_with_id(46, TaskState::Queued, false);
    let second = record_with_id(47, TaskState::Queued, false);
    store.create_task(first.clone()).unwrap();
    store.create_task(second.clone()).unwrap();
    enqueue_task_turn(&store, &first);
    enqueue_task_turn(&store, &second);
    store.bootstrap_active_task_index().unwrap();
    assert!(matches!(
        store
            .reserve_runner_slot(first.status().turns()[0].turn_id(), queue_owner(), 1, false)
            .unwrap(),
        RunnerSlotDecision::Acquired { .. }
    ));
    assert!(matches!(
        store
            .reserve_runner_slot(
                second.status().turns()[0].turn_id(),
                queue_owner(),
                1,
                false
            )
            .unwrap(),
        RunnerSlotDecision::Saturated
    ));
}

#[test]
fn page_32_must_not_omit_other_indexed_live_holders() {
    let (_dir, store, _) = open_live_store();
    for i in 0..33u32 {
        store
            .create_task(open_holder(u128::from(60 + i), 200 + i))
            .unwrap();
    }
    store.bootstrap_active_task_index().unwrap();
    let page = store
        .select_active_task_ids(&ActiveTaskConfig {
            max_tasks_per_tick: 32,
        })
        .unwrap();
    assert_eq!(page.selected.len(), 32);
    assert!(page.truncated);
    let queued = record_with_id(99, TaskState::Queued, false);
    store.create_task(queued.clone()).unwrap();
    enqueue_task_turn(&store, &queued);
    assert_eq!(store.live_runner_slot_count().unwrap(), 33);
    assert!(matches!(
        store
            .reserve_runner_slot(
                queued.status().turns()[0].turn_id(),
                queue_owner(),
                33,
                false
            )
            .unwrap(),
        RunnerSlotDecision::Saturated
    ));
}

#[test]
fn occupancy_orphan_is_ignored_after_bootstrap() {
    let (_dir, store, state_path) = open_live_store();
    let holder = open_holder(48, 48);
    store.create_task(holder.clone()).unwrap();
    store.bootstrap_active_task_index().unwrap();
    fs::remove_file(task_path(&state_path, holder.meta().task_id())).unwrap();
    assert!(receipt_path(&state_path, holder.meta().task_id()).exists());
    assert_eq!(store.live_runner_slot_count().unwrap(), 0);
    let queued = record_with_id(57, TaskState::Queued, false);
    store.create_task(queued.clone()).unwrap();
    enqueue_task_turn(&store, &queued);
    assert!(matches!(
        store
            .reserve_runner_slot(
                queued.status().turns()[0].turn_id(),
                queue_owner(),
                1,
                false
            )
            .unwrap(),
        RunnerSlotDecision::Acquired { .. }
    ));
}

#[test]
fn poisoned_historical_closed_is_not_opened_after_bootstrap() {
    let (_dir, store, state_path) = open_live_store();
    store.create_task(open_holder(49, 49)).unwrap();
    store.bootstrap_active_task_index().unwrap();
    let poisoned = TaskId::new(Uuid::from_u128(50));
    fs::write(task_path(&state_path, poisoned), b"{broken-closed").unwrap();
    assert_eq!(store.live_runner_slot_count().unwrap(), 1);
    let queued = record_with_id(51, TaskState::Queued, false);
    store.create_task(queued.clone()).unwrap();
    enqueue_task_turn(&store, &queued);
    assert!(matches!(
        store
            .reserve_runner_slot(
                queued.status().turns()[0].turn_id(),
                queue_owner(),
                2,
                false
            )
            .unwrap(),
        RunnerSlotDecision::Acquired { .. }
    ));
}

#[test]
fn absent_bootstrap_marker_keeps_legacy_occupancy_scan() {
    let (_dir, store, state_path) = open_live_store();
    let holder = open_holder(52, 52);
    store.create_task(holder.clone()).unwrap();
    fs::remove_file(receipt_path(&state_path, holder.meta().task_id())).unwrap();
    assert!(
        !state_path
            .join("active-tasks")
            .join("bootstrap-v1.json")
            .exists()
    );
    assert_eq!(store.live_runner_slot_count().unwrap(), 1);
}

#[test]
fn malformed_bootstrap_cannot_permit_oversubscription() {
    let (_dir, store, state_path) = open_live_store();
    store.create_task(open_holder(53, 53)).unwrap();
    store.bootstrap_active_task_index().unwrap();
    fs::write(
        state_path.join("active-tasks").join("bootstrap-v1.json"),
        b"{broken-marker",
    )
    .unwrap();
    let queued = record_with_id(54, TaskState::Queued, false);
    store.create_task(queued.clone()).unwrap();
    enqueue_task_turn(&store, &queued);
    let error = store
        .reserve_runner_slot(
            queued.status().turns()[0].turn_id(),
            queue_owner(),
            8,
            false,
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("corrupt")
            || error.to_string().contains("unsupported")
            || error.to_string().contains("invalid")
    );
}

#[test]
fn malformed_indexed_task_cannot_permit_oversubscription() {
    let (_dir, store, state_path) = open_live_store();
    let holder = open_holder(55, 55);
    store.create_task(holder.clone()).unwrap();
    store.bootstrap_active_task_index().unwrap();
    fs::write(
        task_path(&state_path, holder.meta().task_id()),
        b"{broken-task",
    )
    .unwrap();
    let queued = record_with_id(56, TaskState::Queued, false);
    store.create_task(queued.clone()).unwrap();
    enqueue_task_turn(&store, &queued);
    assert!(
        store
            .reserve_runner_slot(
                queued.status().turns()[0].turn_id(),
                queue_owner(),
                8,
                false
            )
            .is_err()
    );
}

#[test]
fn indexed_run_claim_skips_retired_corrupt_closed_and_exempts_follow_up() {
    let (_dir, store, state_path) = open_live_store();
    let run_n = 80u128;
    let run = queue_run(run_n, 2);
    let task_run_id = task_run(run_n);

    let historical = closed_done_in_run(81, task_run_id);
    let historical_id = historical.meta().task_id();
    store.create_task(historical).unwrap();
    store.record_runner(historical_id, None).unwrap();
    assert!(!receipt_path(&state_path, historical_id).exists());

    let follow_up = job_id(84);
    let mut sibling_a = record_in_run(82, TaskState::Active, false, task_run_id);
    let a_status = TaskStatus::new(
        TaskState::Active,
        None,
        None,
        false,
        None,
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![
            TurnSummary::new(
                1,
                sibling_a.status().turns()[0].turn_id(),
                None,
                None,
                None,
                false,
                None,
                None,
            ),
            TurnSummary::new(2, follow_up, None, None, None, false, None, None),
        ],
        1_700_000_000_000,
    )
    .unwrap();
    sibling_a = sibling_a.with_status(a_status).unwrap();
    let sibling_b = record_in_run(83, TaskState::Active, false, task_run_id);
    store.create_task(sibling_a.clone()).unwrap();
    store.create_task(sibling_b).unwrap();
    store.bootstrap_active_task_index().unwrap();

    fs::write(task_path(&state_path, historical_id), b"{broken-closed").unwrap();
    assert!(store.list_tasks().is_err());

    let sibling_c = record_in_run(85, TaskState::Queued, false, task_run_id);
    store.create_task(sibling_c.clone()).unwrap();
    let c_owner = live_identity(85);
    enqueue_run_turn(
        &store,
        sibling_c.status().turns()[0].turn_id(),
        c_owner,
        run.clone(),
    );
    assert!(
        store
            .claim_task_turn(
                c_owner,
                sibling_c.status().turns()[0].turn_id(),
                &["mini-2".into()],
                1_700_000_000_100
            )
            .unwrap()
            .is_none()
    );

    let follow_owner = live_identity(84);
    enqueue_run_turn(&store, follow_up, follow_owner, run);
    let claimed = store
        .claim_task_turn(
            follow_owner,
            follow_up,
            &["mini-1".into()],
            1_700_000_000_101,
        )
        .unwrap()
        .expect("same-task follow-up must not fill the run cap against itself");
    assert_eq!(claimed.entry().job_id(), follow_up);
    assert!(
        store
            .claim_task_turn(
                c_owner,
                sibling_c.status().turns()[0].turn_id(),
                &["mini-2".into()],
                1_700_000_000_102
            )
            .unwrap()
            .is_none()
    );
}
