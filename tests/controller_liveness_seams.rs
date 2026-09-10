//! Slot occupancy and DAG claim occupy until a positive `Exited` verdict.
//! First `Absent` is Unverifiable; only confirmed absence or `Reused` releases.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy},
    client_state::{ClientStateStore, RUNNER_ABSENCE_CONFIRMATION, RunnerSlotDecision},
    dag::{DagBase, DagFrozenSpec, DagNode, DagNodeState, DagRecord, dag_pin_ref},
    error::WorkerError,
    job::{CommandSummary, ProcessIdentity, QueueEntry, QueueEntryKind},
    scheduler::WorkerPreference,
    supervisor::{
        ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
    },
    task::{
        ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, RunId, RunRecord, RunnerIdentity,
        TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskSource, TaskState, TaskStatus, TurnId,
        TurnSummary,
    },
};
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const BASE_OID: &str = "0123456789abcdef0123456789abcdef01234567";
const REPO_ID: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

#[derive(Clone)]
struct MappedInspector {
    by_pid: Arc<Mutex<BTreeMap<u32, ProcessObservation>>>,
    default: ProcessObservation,
}

impl MappedInspector {
    fn new(default: ProcessObservation) -> Self {
        Self {
            by_pid: Arc::new(Mutex::new(BTreeMap::new())),
            default,
        }
    }

    fn set(&self, identity: ProcessIdentity, observation: ProcessObservation) {
        self.by_pid
            .lock()
            .expect("inspector map")
            .insert(identity.pid(), observation);
    }
}

impl ProcessInspector for MappedInspector {
    fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
        ProcessIdentity::new(pid, u64::from(pid) * 10_000 + 7)
    }

    fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
        self.by_pid
            .lock()
            .expect("inspector map")
            .get(&expected.pid())
            .copied()
            .unwrap_or(self.default)
    }

    fn observe_group(&self, _process_group: u32) -> ProcessGroupObservation {
        ProcessGroupObservation::Ambiguous
    }

    fn observe_group_members(&self, _leader: u32) -> ProcessGroupMembership {
        ProcessGroupMembership::Ambiguous
    }
}

fn owner(pid: u32) -> ProcessIdentity {
    ProcessIdentity::new(pid, u64::from(pid) * 10_000 + 7).unwrap()
}

fn open_store(inspector: MappedInspector) -> (tempfile::TempDir, ClientStateStore) {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().canonicalize().unwrap().join("state");
    let store = ClientStateStore::open_with_owner_inspector(&state, inspector).unwrap();
    (dir, store)
}

fn plant_task_turn(
    store: &ClientStateStore,
    number: u128,
    enqueue_owner: ProcessIdentity,
    runner: Option<ProcessIdentity>,
) -> TurnId {
    let task_id = TaskId::new(Uuid::from_u128(number));
    let turn_id = TurnId::new(Uuid::from_u128(number + 1_000));
    let meta = TaskMeta::new(TaskMetaInput {
        task_id,
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
        git_identity: GitIdentity::new("Fixture", "fixture@example.test").unwrap(),
        title: None,
        prompt: "private prompt".into(),
        created_at_millis: number as u64,
    })
    .unwrap();
    let status = TaskStatus::new(
        TaskState::Active,
        None,
        None,
        false,
        Some(meta.base_oid().clone()),
        None,
        vec![],
        vec![],
        None,
        vec![TurnSummary::new(
            1, turn_id, None, None, None, false, None, None,
        )],
        number as u64,
    )
    .unwrap();
    store
        .create_task(
            LocalTaskRecord::new(
                meta,
                status,
                None,
                runner.map(RunnerIdentity::new),
                None,
                REPO_ID.into(),
                None,
                true,
                None,
            )
            .unwrap(),
        )
        .unwrap();
    store
        .enqueue(
            QueueEntry::new(
                turn_id,
                store.client_id(),
                PROJECT_ID.into(),
                WORKTREE_ID.into(),
                CommandSummary::argv(2).unwrap(),
                Vec::new(),
                WorkerPreference::Automatic,
                QueueEntryKind::TaskTurn,
                None,
                enqueue_owner,
                number as u64,
            )
            .unwrap(),
        )
        .unwrap();
    turn_id
}

fn frozen() -> DagFrozenSpec {
    DagFrozenSpec {
        prompt: "do work".into(),
        title: None,
        agent: "codex".into(),
        model: None,
        effort: None,
        source: "local".into(),
        origin_url: None,
        publish: vec!["fetch".into()],
        publish_branch: None,
        close_on: ClosePolicy::Done,
        env_profile: None,
        worker: None,
        wip: false,
        project_path: "/tmp/project".into(),
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
        timeout_millis: 45 * 60 * 1000,
        max_turns: None,
        max_budget_usd_cents: None,
        max_followups: 10,
        permissions: "workspace".into(),
        requires: vec!["agent:codex".into()],
        include_untracked: Vec::new(),
        include_empty_dirs: Vec::new(),
        allow_sensitive: Vec::new(),
        cli_includes: Vec::new(),
        branch: Some("main".into()),
    }
}

fn plant_claimed_run(
    store: &ClientStateStore,
    foreign: ProcessIdentity,
) -> (RunId, TaskId, TurnId) {
    let run_id = RunId::generate();
    let task_id = TaskId::generate();
    let turn_id = TurnId::generate();
    let node = DagNode {
        batch_id: "root".into(),
        task_id,
        turn_id,
        depends_on: Vec::new(),
        base: DagBase::Frozen {
            oid: BASE_OID.parse().unwrap(),
            pin_ref: dag_pin_ref(run_id, "root"),
            wip: false,
        },
        frozen: frozen(),
        state: DagNodeState::Claimed,
        bound_oid: None,
        bound_turn_id: None,
        pin_ref: None,
        blocked_by: None,
        claimed_by: Some(foreign),
        claimed_at_millis: Some(30),
    };
    let dag = DagRecord::new(run_id, BTreeMap::from([("root".into(), node)]), 1, None, 5).unwrap();
    store
        .create_run_with_dag(RunRecord::new(run_id, None, Vec::new(), 1, 5).unwrap(), dag)
        .unwrap();
    (run_id, task_id, turn_id)
}

#[test]
fn first_absent_runner_cannot_free_a_slot_until_absence_is_confirmed() {
    let occupying = owner(810);
    let inspector = MappedInspector::new(ProcessObservation::Absent);
    let (_dir, store) = open_store(inspector);
    plant_task_turn(&store, 810, occupying, Some(occupying));
    let extra = plant_task_turn(&store, 811, occupying, None);
    assert_eq!(store.live_runner_slot_count().unwrap(), 1);
    assert!(matches!(
        store
            .reserve_runner_slot(extra, occupying, 1, false)
            .unwrap(),
        RunnerSlotDecision::Saturated
    ));
    store.advance_liveness_clock(RUNNER_ABSENCE_CONFIRMATION);
    assert_eq!(store.live_runner_slot_count().unwrap(), 0);
    assert!(matches!(
        store
            .reserve_runner_slot(extra, occupying, 1, false)
            .unwrap(),
        RunnerSlotDecision::Acquired { .. }
    ));
}

#[test]
fn reused_runner_frees_a_slot_immediately() {
    let occupying = owner(820);
    let inspector = MappedInspector::new(ProcessObservation::Reused);
    let (_dir, store) = open_store(inspector);
    plant_task_turn(&store, 820, occupying, Some(occupying));
    let extra = plant_task_turn(&store, 821, occupying, None);
    assert_eq!(store.live_runner_slot_count().unwrap(), 0);
    assert!(matches!(
        store
            .reserve_runner_slot(extra, occupying, 1, false)
            .unwrap(),
        RunnerSlotDecision::Acquired { .. }
    ));
}

#[test]
fn ambiguous_runner_stays_occupied_after_the_absence_window() {
    let occupying = owner(830);
    let inspector = MappedInspector::new(ProcessObservation::Ambiguous);
    let (_dir, store) = open_store(inspector);
    plant_task_turn(&store, 830, occupying, Some(occupying));
    let extra = plant_task_turn(&store, 831, occupying, None);
    assert_eq!(store.live_runner_slot_count().unwrap(), 1);
    store.advance_liveness_clock(RUNNER_ABSENCE_CONFIRMATION);
    store.advance_liveness_clock(Duration::from_secs(30));
    assert_eq!(store.live_runner_slot_count().unwrap(), 1);
    assert!(matches!(
        store
            .reserve_runner_slot(extra, occupying, 1, false)
            .unwrap(),
        RunnerSlotDecision::Saturated
    ));
}

#[test]
fn first_absent_reservation_cannot_be_stolen_until_absence_is_confirmed() {
    let parent = owner(801);
    let child = owner(802);
    let inspector = MappedInspector::new(ProcessObservation::Absent);
    inspector.set(parent, ProcessObservation::Matching { process_group: 801 });
    inspector.set(child, ProcessObservation::Matching { process_group: 802 });
    let (_dir, store) = open_store(inspector.clone());
    let turn_id = plant_task_turn(&store, 801, parent, None);
    let RunnerSlotDecision::Acquired { token: old } = store
        .reserve_runner_slot(turn_id, parent, 1, false)
        .unwrap()
    else {
        panic!("expected acquire");
    };
    inspector.set(parent, ProcessObservation::Absent);
    assert!(
        matches!(
            store.reserve_runner_slot(turn_id, child, 1, false).unwrap(),
            RunnerSlotDecision::Pending { token } if token == old
        ),
        "first Absent must keep the reservation"
    );
    store.advance_liveness_clock(RUNNER_ABSENCE_CONFIRMATION);
    let RunnerSlotDecision::Acquired { token: new } =
        store.reserve_runner_slot(turn_id, child, 1, false).unwrap()
    else {
        panic!("confirmed absence must steal");
    };
    assert_ne!(old, new);
}

#[test]
fn first_absent_foreign_dag_claim_cannot_be_retaken_until_absence_is_confirmed() {
    let foreign = owner(20);
    let caller = owner(21);
    let inspector = MappedInspector::new(ProcessObservation::Absent);
    let (_dir, store) = open_store(inspector);
    let (run_id, task_id, turn_id) = plant_claimed_run(&store, foreign);
    assert!(
        store
            .claim_next_eligible_dag_node(run_id, caller, 31)
            .unwrap()
            .is_none()
    );
    let dag = store.load_run_dag(run_id).unwrap().unwrap();
    assert_eq!(dag.nodes["root"].claimed_by, Some(foreign));
    assert_eq!(dag.nodes["root"].state, DagNodeState::Claimed);

    store.advance_liveness_clock(RUNNER_ABSENCE_CONFIRMATION);
    let claim = store
        .claim_next_eligible_dag_node(run_id, caller, 32)
        .unwrap()
        .expect("confirmed absence retakes");
    assert_eq!(claim.node.task_id, task_id);
    assert_eq!(claim.node.turn_id, turn_id);
    assert_eq!(claim.node.claimed_by, Some(caller));
}

#[test]
fn reused_foreign_dag_claim_is_retaken_immediately() {
    let foreign = owner(40);
    let caller = owner(41);
    let inspector = MappedInspector::new(ProcessObservation::Reused);
    let (_dir, store) = open_store(inspector);
    let (run_id, task_id, turn_id) = plant_claimed_run(&store, foreign);
    let claim = store
        .claim_next_eligible_dag_node(run_id, caller, 31)
        .unwrap()
        .expect("Reused is Exited");
    assert_eq!(claim.node.task_id, task_id);
    assert_eq!(claim.node.turn_id, turn_id);
    assert_eq!(claim.node.claimed_by, Some(caller));
}

#[test]
fn ambiguous_foreign_dag_claim_is_not_retaken() {
    let foreign = owner(50);
    let caller = owner(51);
    let inspector = MappedInspector::new(ProcessObservation::Ambiguous);
    let (_dir, store) = open_store(inspector);
    let (run_id, _, _) = plant_claimed_run(&store, foreign);
    store.advance_liveness_clock(RUNNER_ABSENCE_CONFIRMATION);
    assert!(
        store
            .claim_next_eligible_dag_node(run_id, caller, 31)
            .unwrap()
            .is_none()
    );
    let dag = store.load_run_dag(run_id).unwrap().unwrap();
    assert_eq!(dag.nodes["root"].claimed_by, Some(foreign));
}
