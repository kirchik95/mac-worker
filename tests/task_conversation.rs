use mac_worker::{
    client_state::ClientStateStore,
    job::{
        AdmissionObservation, CommandSummary, JobId, ProcessIdentity, QueueEntry, QueueEntryKind,
        QueueRunReference, QueueState, RunId,
    },
    scheduler::{CandidateSlot, WorkerPreference},
    supervisor::{
        ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
    },
    task::{
        BaseOid, ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, RunId as TaskRunId,
        TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskSource, TaskState, TaskStatus,
        TurnSummary,
    },
};
use tempfile::TempDir;
use uuid::Uuid;

use mac_worker::job::RunId as QueueRunId;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

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
