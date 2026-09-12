#[allow(dead_code)]
mod support;

use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fs,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy, TurnLimits},
    agent_facts::{AgentAuth, AgentFacts, AgentProbe},
    client_state::{
        ClientStateConcurrencyHook, ClientStateConcurrencyPoint, ClientStateStore,
        ClientStateWritePoint,
    },
    config::Config,
    controller::ProjectRegistry,
    dag::{
        DAG_PARENT_FAILED, DagBase, DagFrozenSpec, DagNode, DagNodeState, DagRecord, dag_pin_ref,
    },
    error::WorkerError,
    git_transport::GitTransport,
    job::{
        AdmissionObservation, JobMeta, JobState, JobStatus, LeaseAcquireRequest,
        LeaseAcquireResponse, LeaseRecord, LogChunk, LogChunkRequest, LogChunkResponse, LogStream,
        ProcessIdentity, QueueState, StatusLogsRequest, StatusLogsResponse, StatusRequest,
        StatusResponse, SubmitResponse,
    },
    lease::SlotState,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    project_config::ProjectSettings,
    project_state::ProjectState,
    protocol::{CpuCounters, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION},
    scheduler::{CandidateSlot, WorkerPreference},
    supervisor::{
        ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
    },
    task::{
        BaseOid, ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, RunId, RunRecord,
        RunnerIdentity, TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome, TaskSource,
        TaskState, TaskStatus, TurnId, TurnSummary, TurnTerminal,
    },
    task_client::{TaskClient, TaskListFilter, TaskSubmitRequest, WaitSelector},
    task_store::{
        SessionBinding, TaskCloseRequest, TaskCloseResponse, TaskPrepareRequest,
        TaskPrepareResponse, TaskSessionRequest, TaskSessionResponse, TaskStatusRequest,
        TaskStatusResponse,
    },
    transfer::HostOperation,
    transfer_repo::TransferRepo,
    turn::{TaskTurnRequest, TaskTurnResponse},
    turn_runner::{InlineRunnerExecutor, RunnerExecutor, TurnRunner},
};

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const BASE_OID: &str = "0123456789abcdef0123456789abcdef01234567";
const REPO_ID: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

static CWD_LOCK: Mutex<()> = Mutex::new(());

struct MappedInspector {
    by_pid: Mutex<BTreeMap<u32, ProcessObservation>>,
    default: ProcessObservation,
}

impl MappedInspector {
    fn new(default: ProcessObservation) -> Self {
        Self {
            by_pid: Mutex::new(BTreeMap::new()),
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
        ProcessIdentity::new(pid, u64::from(pid) * 1_000 + 1)
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

fn waiting_node(run_id: RunId, batch_id: &str, task_id: TaskId, turn_id: TurnId) -> DagNode {
    DagNode {
        batch_id: batch_id.to_owned(),
        task_id,
        turn_id,
        depends_on: Vec::new(),
        base: DagBase::Frozen {
            oid: BASE_OID.parse().unwrap(),
            pin_ref: dag_pin_ref(run_id, batch_id),
            wip: false,
        },
        frozen: frozen(),
        state: DagNodeState::Waiting,
        bound_oid: None,
        bound_turn_id: None,
        pin_ref: None,
        blocked_by: None,
        claimed_by: None,
        claimed_at_millis: None,
    }
}

fn open_store() -> (tempfile::TempDir, PathBuf, ClientStateStore) {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().canonicalize().unwrap().join("state");
    let store = ClientStateStore::open(&state).unwrap();
    (dir, state, store)
}

fn intent_task(run_id: RunId, task_id: TaskId, turn_id: TurnId) -> LocalTaskRecord {
    let meta = TaskMeta::new(TaskMetaInput {
        task_id,
        run_id: Some(run_id),
        project_id: PROJECT_ID.to_owned(),
        worktree_id: WORKTREE_ID.to_owned(),
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
        base_oid: BASE_OID.parse().unwrap(),
        limits: TaskLimits::new(TurnLimits::new(30 * 60 * 1000, None, None).unwrap(), 10).unwrap(),
        close_policy: ClosePolicy::Done,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: "do work".into(),
        created_at_millis: 1_700_000_000_000,
    })
    .unwrap();
    let status = TaskStatus::new(
        TaskState::Queued,
        None,
        None,
        false,
        Some(BASE_OID.parse().unwrap()),
        None,
        Vec::new(),
        Vec::new(),
        None,
        Vec::new(),
        1_700_000_000_000,
    )
    .unwrap();
    LocalTaskRecord::new(
        meta,
        status,
        None,
        None,
        None,
        REPO_ID.to_owned(),
        None,
        true,
        None,
    )
    .unwrap()
    .with_submission_intent_turn_id(turn_id)
    .unwrap()
}

#[test]
fn dag_crash_after_publish_before_run_recovers_same_run_and_does_not_dispatch_first() {
    let (_dir, state_path, store) = open_store();
    let run_id = RunId::generate();
    let task_id = TaskId::generate();
    let turn_id = TurnId::generate();
    let node = waiting_node(run_id, "root", task_id, turn_id);
    let pin_ref = dag_pin_ref(run_id, "root");
    let dag = DagRecord::new(
        run_id,
        BTreeMap::from([("root".into(), node)]),
        2,
        Some("named-batch".into()),
        1_700_000_000_111,
    )
    .unwrap();
    let run = RunRecord::new(
        run_id,
        Some("named-batch".into()),
        Vec::new(),
        2,
        1_700_000_000_111,
    )
    .unwrap();
    store.inject_write_failure_once(ClientStateWritePoint::AfterDagPublishBeforeRun);
    assert!(store.create_run_with_dag(run, dag).is_err());
    assert_eq!(store.list_pending_run_ids().unwrap(), vec![run_id]);

    let published = store.load_run_dag(run_id).unwrap().expect("DAG file kept");
    assert_eq!(published.name.as_deref(), Some("named-batch"));
    assert_eq!(published.max_parallel, 2);
    assert_eq!(published.created_at_millis, 1_700_000_000_111);
    assert_eq!(
        published.nodes["root"].base,
        DagBase::Frozen {
            oid: BASE_OID.parse().unwrap(),
            pin_ref: pin_ref.clone(),
            wip: false,
        }
    );
    assert!(store.load_run(run_id).is_err(), "run must not exist yet");

    let restarted = ClientStateStore::open(&state_path).unwrap();
    assert!(restarted.load_run(run_id).is_err());
    let caller = owner(11);
    let claim = restarted
        .claim_next_eligible_dag_node(run_id, caller, 1_700_000_000_222)
        .unwrap();
    let recovered = restarted.load_run(run_id).unwrap();
    assert_eq!(recovered.name(), Some("named-batch"));
    assert_eq!(recovered.max_parallel(), 2);
    assert_eq!(recovered.created_at_millis(), 1_700_000_000_111);
    assert!(recovered.task_ids().is_empty());
    let claim = claim.expect("dispatch only after run is recoverable");
    assert_eq!(claim.node.task_id, task_id);
    assert_eq!(claim.node.turn_id, turn_id);
    assert_eq!(
        restarted.load_run_dag(run_id).unwrap().unwrap().nodes["root"].state,
        DagNodeState::Claimed
    );
}

#[test]
fn dag_crash_after_pending_index_before_dag_does_not_dispatch() {
    let (_dir, state_path, store) = open_store();
    let run_id = RunId::generate();
    let task_id = TaskId::generate();
    let turn_id = TurnId::generate();
    let dag = DagRecord::new(
        run_id,
        BTreeMap::from([(
            "root".into(),
            waiting_node(run_id, "root", task_id, turn_id),
        )]),
        1,
        None,
        1_700_000_000_111,
    )
    .unwrap();
    let run = RunRecord::new(run_id, None, Vec::new(), 1, 1_700_000_000_111).unwrap();
    store.inject_write_failure_once(ClientStateWritePoint::AfterDagPendingBeforeDag);
    assert!(store.create_run_with_dag(run, dag).is_err());
    assert!(store.load_run_dag(run_id).unwrap().is_none());
    assert_eq!(store.list_pending_run_ids().unwrap(), vec![run_id]);
    let restarted = ClientStateStore::open(&state_path).unwrap();
    assert!(restarted.load_run_dag(run_id).unwrap().is_none());
    assert_eq!(restarted.list_pending_run_ids().unwrap(), vec![run_id]);
    assert!(
        restarted
            .claim_next_eligible_dag_node(run_id, owner(11), 1_700_000_000_222)
            .unwrap()
            .is_none()
    );
    assert!(restarted.load_task_optional(task_id).unwrap().is_none());
}

#[test]
fn dag_membership_is_appended_before_submitted_and_repair_is_idempotent() {
    let (_dir, _, store) = open_store();
    let run_id = RunId::generate();
    let task_id = TaskId::generate();
    let turn_id = TurnId::generate();
    let dag = DagRecord::new(
        run_id,
        BTreeMap::from([(
            "root".into(),
            waiting_node(run_id, "root", task_id, turn_id),
        )]),
        1,
        None,
        9,
    )
    .unwrap();
    store
        .create_run_with_dag(RunRecord::new(run_id, None, Vec::new(), 1, 9).unwrap(), dag)
        .unwrap();
    let caller = owner(12);
    let claim = store
        .claim_next_eligible_dag_node(run_id, caller, 10)
        .unwrap()
        .expect("root claimed");
    store.inject_write_failure_once(ClientStateWritePoint::AfterDagRunMembership);
    assert!(
        store
            .mark_dag_node_submitted(run_id, &claim.batch_id, task_id)
            .is_err()
    );
    assert_eq!(store.load_run(run_id).unwrap().task_ids(), &[task_id]);
    assert_eq!(
        store.load_run_dag(run_id).unwrap().unwrap().nodes["root"].state,
        DagNodeState::Claimed
    );

    store
        .mark_dag_node_submitted(run_id, &claim.batch_id, task_id)
        .unwrap();
    assert_eq!(store.load_run(run_id).unwrap().task_ids(), &[task_id]);
    assert_eq!(
        store.load_run_dag(run_id).unwrap().unwrap().nodes["root"].state,
        DagNodeState::Submitted
    );
    store
        .mark_dag_node_submitted(run_id, &claim.batch_id, task_id)
        .unwrap();
    assert_eq!(store.load_run(run_id).unwrap().task_ids(), &[task_id]);
}

#[test]
fn dag_submitted_missing_membership_is_repaired_once() {
    let (_dir, _, store) = open_store();
    let run_id = RunId::generate();
    let task_id = TaskId::generate();
    let turn_id = TurnId::generate();
    let mut node = waiting_node(run_id, "root", task_id, turn_id);
    node.state = DagNodeState::Submitted;
    let dag = DagRecord::new(run_id, BTreeMap::from([("root".into(), node)]), 1, None, 3).unwrap();
    store
        .create_run_with_dag(RunRecord::new(run_id, None, Vec::new(), 1, 3).unwrap(), dag)
        .unwrap();
    assert!(store.load_run(run_id).unwrap().task_ids().is_empty());
    assert!(
        store
            .claim_next_eligible_dag_node(run_id, owner(13), 11)
            .unwrap()
            .is_none()
    );
    assert_eq!(store.load_run(run_id).unwrap().task_ids(), &[task_id]);
    assert!(
        store
            .claim_next_eligible_dag_node(run_id, owner(13), 12)
            .unwrap()
            .is_none()
    );
    assert_eq!(store.load_run(run_id).unwrap().task_ids(), &[task_id]);
}

#[test]
fn dag_incomplete_local_submit_is_not_marked_submitted() {
    let (_dir, _, store) = open_store();
    let run_id = RunId::generate();
    let task_id = TaskId::generate();
    let turn_id = TurnId::generate();
    let mut node = waiting_node(run_id, "root", task_id, turn_id);
    node.state = DagNodeState::Claimed;
    node.claimed_by = Some(owner(14));
    node.claimed_at_millis = Some(20);
    let dag = DagRecord::new(run_id, BTreeMap::from([("root".into(), node)]), 1, None, 4).unwrap();
    store
        .create_run_with_dag(RunRecord::new(run_id, None, Vec::new(), 1, 4).unwrap(), dag)
        .unwrap();
    store
        .create_task(intent_task(run_id, task_id, turn_id))
        .unwrap();
    let claim = store
        .claim_next_eligible_dag_node(run_id, owner(14), 21)
        .unwrap()
        .expect("same IDs continue");
    assert_eq!(claim.node.task_id, task_id);
    assert_eq!(claim.node.turn_id, turn_id);
    assert_eq!(
        store.load_run_dag(run_id).unwrap().unwrap().nodes["root"].state,
        DagNodeState::Claimed
    );
    assert!(store.load_run(run_id).unwrap().task_ids().is_empty());
}

#[test]
fn dag_live_foreign_claim_is_not_taken() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().canonicalize().unwrap().join("state");
    let foreign = owner(20);
    let inspector = MappedInspector::new(ProcessObservation::Absent);
    inspector.set(foreign, ProcessObservation::Matching { process_group: 20 });
    let store = ClientStateStore::open_with_owner_inspector(&state, inspector).unwrap();
    let run_id = RunId::generate();
    let task_id = TaskId::generate();
    let turn_id = TurnId::generate();
    let mut node = waiting_node(run_id, "root", task_id, turn_id);
    node.state = DagNodeState::Claimed;
    node.claimed_by = Some(foreign);
    node.claimed_at_millis = Some(30);
    let dag = DagRecord::new(run_id, BTreeMap::from([("root".into(), node)]), 1, None, 5).unwrap();
    store
        .create_run_with_dag(RunRecord::new(run_id, None, Vec::new(), 1, 5).unwrap(), dag)
        .unwrap();
    assert!(
        store
            .claim_next_eligible_dag_node(run_id, owner(21), 31)
            .unwrap()
            .is_none()
    );
    let dag = store.load_run_dag(run_id).unwrap().unwrap();
    assert_eq!(dag.nodes["root"].claimed_by, Some(foreign));
    assert_eq!(dag.nodes["root"].state, DagNodeState::Claimed);
}

#[test]
fn dag_dead_claim_is_retaken_once_by_concurrent_reconcilers() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().canonicalize().unwrap().join("state");
    let dead = owner(30);
    let first = owner(31);
    let second = owner(32);
    let inspector = MappedInspector::new(ProcessObservation::Absent);
    inspector.set(dead, ProcessObservation::Absent);
    inspector.set(first, ProcessObservation::Matching { process_group: 31 });
    inspector.set(second, ProcessObservation::Matching { process_group: 32 });
    let store = Arc::new(ClientStateStore::open_with_owner_inspector(&state, inspector).unwrap());
    let run_id = RunId::generate();
    let task_id = TaskId::generate();
    let turn_id = TurnId::generate();
    let mut node = waiting_node(run_id, "root", task_id, turn_id);
    node.state = DagNodeState::Claimed;
    node.claimed_by = Some(dead);
    node.claimed_at_millis = Some(40);
    let dag = DagRecord::new(run_id, BTreeMap::from([("root".into(), node)]), 1, None, 6).unwrap();
    store
        .create_run_with_dag(RunRecord::new(run_id, None, Vec::new(), 1, 6).unwrap(), dag)
        .unwrap();
    store.note_confirmed_runner_absence(dead);

    let a = {
        let store = Arc::clone(&store);
        thread::spawn(move || store.claim_next_eligible_dag_node(run_id, first, 41))
    };
    let b = {
        let store = Arc::clone(&store);
        thread::spawn(move || store.claim_next_eligible_dag_node(run_id, second, 41))
    };
    let first_result = a.join().expect("first reconciler").unwrap();
    let second_result = b.join().expect("second reconciler").unwrap();
    let claims = [first_result, second_result]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(claims.len(), 1, "exactly one durable retake");
    assert_eq!(claims[0].node.task_id, task_id);
    assert_eq!(claims[0].node.turn_id, turn_id);
    let dag = store.load_run_dag(run_id).unwrap().unwrap();
    assert_eq!(dag.nodes["root"].state, DagNodeState::Claimed);
    assert_eq!(
        dag.nodes["root"].claimed_by,
        Some(claims[0].node.claimed_by.unwrap())
    );
    assert!(
        dag.nodes["root"].claimed_by == Some(first) || dag.nodes["root"].claimed_by == Some(second)
    );
    assert_ne!(dag.nodes["root"].claimed_by, Some(dead));
}

#[test]
fn dag_dead_claim_with_partial_task_is_retaken_once_keeping_same_ids() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().canonicalize().unwrap().join("state");
    let dead = owner(40);
    let first = owner(41);
    let second = owner(42);
    let inspector = MappedInspector::new(ProcessObservation::Absent);
    inspector.set(dead, ProcessObservation::Absent);
    inspector.set(first, ProcessObservation::Matching { process_group: 41 });
    inspector.set(second, ProcessObservation::Matching { process_group: 42 });
    let store = Arc::new(ClientStateStore::open_with_owner_inspector(&state, inspector).unwrap());
    let run_id = RunId::generate();
    let task_id = TaskId::generate();
    let turn_id = TurnId::generate();
    let mut node = waiting_node(run_id, "root", task_id, turn_id);
    node.state = DagNodeState::Claimed;
    node.claimed_by = Some(dead);
    node.claimed_at_millis = Some(50);
    let dag = DagRecord::new(run_id, BTreeMap::from([("root".into(), node)]), 1, None, 7).unwrap();
    store
        .create_run_with_dag(RunRecord::new(run_id, None, Vec::new(), 1, 7).unwrap(), dag)
        .unwrap();
    store
        .create_task(intent_task(run_id, task_id, turn_id))
        .unwrap();
    store.note_confirmed_runner_absence(dead);

    let a = {
        let store = Arc::clone(&store);
        thread::spawn(move || store.claim_next_eligible_dag_node(run_id, first, 51))
    };
    let b = {
        let store = Arc::clone(&store);
        thread::spawn(move || store.claim_next_eligible_dag_node(run_id, second, 51))
    };
    let first_result = a.join().expect("first reconciler").unwrap();
    let second_result = b.join().expect("second reconciler").unwrap();
    let claims = [first_result, second_result]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(claims.len(), 1, "exactly one durable retake");
    assert_eq!(claims[0].node.task_id, task_id);
    assert_eq!(claims[0].node.turn_id, turn_id);
    let dag = store.load_run_dag(run_id).unwrap().unwrap();
    assert_eq!(dag.nodes["root"].state, DagNodeState::Claimed);
    assert_eq!(
        dag.nodes["root"].claimed_by,
        Some(claims[0].node.claimed_by.unwrap())
    );
    assert!(
        dag.nodes["root"].claimed_by == Some(first) || dag.nodes["root"].claimed_by == Some(second)
    );
    assert_ne!(dag.nodes["root"].claimed_by, Some(dead));
    let retained = store.load_task(task_id).unwrap();
    assert_eq!(retained.meta().task_id(), task_id);
    assert_eq!(retained.submission_intent_turn_id(), Some(turn_id));
    assert_eq!(retained.meta().created_at_millis(), 1_700_000_000_000);
    assert!(store.load_run(run_id).unwrap().task_ids().is_empty());
}

#[test]
fn dag_intent_cleared_without_turn_dir_is_not_marked_submitted() {
    let (_dir, _, store) = open_store();
    let run_id = RunId::generate();
    let task_id = TaskId::generate();
    let turn_id = TurnId::generate();
    let mut node = waiting_node(run_id, "root", task_id, turn_id);
    node.state = DagNodeState::Claimed;
    node.claimed_by = Some(owner(15));
    node.claimed_at_millis = Some(22);
    let dag = DagRecord::new(run_id, BTreeMap::from([("root".into(), node)]), 1, None, 8).unwrap();
    store
        .create_run_with_dag(RunRecord::new(run_id, None, Vec::new(), 1, 8).unwrap(), dag)
        .unwrap();
    store
        .create_task(
            intent_task(run_id, task_id, turn_id)
                .without_submission_intent()
                .unwrap(),
        )
        .unwrap();
    let claim = store
        .claim_next_eligible_dag_node(run_id, owner(15), 23)
        .unwrap()
        .expect("placeholder is not Submitted");
    assert_eq!(claim.node.task_id, task_id);
    assert_eq!(claim.node.turn_id, turn_id);
    assert_eq!(
        store.load_run_dag(run_id).unwrap().unwrap().nodes["root"].state,
        DagNodeState::Claimed
    );
    assert!(store.load_run(run_id).unwrap().task_ids().is_empty());
}

fn dag_test_config() -> Config {
    Config::parse(
        "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n",
    )
    .unwrap()
}

fn plant_bound_mini1_ready(state: &ClientStateStore) {
    plant_bound_mini1_ready_with(state, vec!["darwin-arm64".into(), "agent:codex".into()]);
}

fn plant_bound_mini1_ready_with(state: &ClientStateStore, capabilities: Vec<String>) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    state
        .publish_admission_observation(
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                CandidateSlot::Idle,
                capabilities,
                Some(8 * 1024 * 1024 * 1024),
                64 * 1024 * 1024 * 1024,
                now,
            )
            .unwrap()
            .with_local_binding(
                "mac1".into(),
                "~/.local/bin/worker".into(),
                vec!["darwin-arm64".into()],
                1,
                Some(0),
                now,
            ),
        )
        .unwrap();
}

/// Isolated DAG tests keep production `SystemProcessRunner` for Git and
/// inject a private `HOME` the same way [`support::GitRepo`] does. Inspect
/// already sets `GIT_CONFIG_GLOBAL=/dev/null`; this runner does not change
/// production Git deadlines. `ssh = mac1` is inventory-only: unexpected SSH
/// is rejected, and `host probe` / refresh-facts are faked so admission TTL
/// expiry cannot dial the network.
///
/// Set `MAC_WORKER_DAG_PROC_TRACE` to sample pid/pgid/`ps`/lock state.
#[derive(Clone)]
struct IsolatedDagRunner {
    inner: Arc<IsolatedDagInner>,
}

struct IsolatedDagInner {
    origin: Instant,
    events: Mutex<Vec<IsolatedProcEvent>>,
    home: PathBuf,
    _home: tempfile::TempDir,
}

fn dag_proc_trace() -> bool {
    std::env::var_os("MAC_WORKER_DAG_PROC_TRACE").is_some()
}

struct IsolatedProcEvent {
    thread: String,
    at: Duration,
    duration: Duration,
    deadline: Duration,
    caller_pid: u32,
    caller_pgid: i32,
    program: String,
    args: Vec<String>,
    outcome: String,
    samples: Vec<String>,
}

fn caller_pgid() -> i32 {
    unsafe { libc::getpgid(0) }
}

fn request_git_target(request: &ProcessRequest) -> Option<PathBuf> {
    let args = &request.args;
    for index in 0..args.len().saturating_sub(1) {
        if args[index] == "--git-dir" || args[index] == "-C" {
            return Some(PathBuf::from(&args[index + 1]));
        }
    }
    None
}

fn describe_run(result: &Result<ProcessResult, WorkerError>) -> String {
    match result {
        Ok(output) if output.status.success() => "runner=ok git=success".to_owned(),
        Ok(output) => format!(
            "runner=ok git=failed exit={:?} signal={:?} stderr={}",
            output.status.code(),
            output.status.signal(),
            String::from_utf8_lossy(&output.stderr)
                .replace('\n', "\\n")
                .chars()
                .take(400)
                .collect::<String>()
        ),
        Err(error) => format!("runner=err {error}"),
    }
}

fn git_lock_state(target: &Path) -> String {
    let candidates = [
        target.join("HEAD.lock"),
        target.join("index.lock"),
        target.join("packed-refs.lock"),
        target.join(".git/HEAD.lock"),
        target.join(".git/index.lock"),
        target.join(".git/packed-refs.lock"),
    ];
    let mut parts = Vec::new();
    for path in candidates {
        match std::fs::metadata(&path) {
            Ok(meta) => parts.push(format!("{} exists len={}", path.display(), meta.len())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => parts.push(format!("{} err={error}", path.display())),
        }
    }
    if parts.is_empty() {
        format!("locks=none target={}", target.display())
    } else {
        format!("locks=[{}] target={}", parts.join("; "), target.display())
    }
}

fn sample_slow_git_state(target: Option<&Path>) -> String {
    let our_pid = std::process::id();
    let our_pgid = caller_pgid();
    let ps = Command::new("/bin/ps")
        .args(["-axo", "pid=,pgid=,ppid=,stat=,etime=,wchan=,command="])
        .output();
    let ps_lines = match ps {
        Ok(output) => String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|line| {
                let our = line
                    .split_whitespace()
                    .next()
                    .is_some_and(|pid| pid.parse::<u32>().is_ok_and(|pid| pid == our_pid));
                our || line.contains("/usr/bin/git") || line.contains(" git ")
            })
            .take(40)
            .collect::<Vec<_>>()
            .join(" || "),
        Err(error) => format!("ps-err={error}"),
    };
    let locks = target
        .map(git_lock_state)
        .unwrap_or_else(|| "locks=no-git-target".into());
    format!("caller_pid={our_pid} caller_pgid={our_pgid} ps=[{ps_lines}] {locks}")
}

impl IsolatedDagRunner {
    fn new() -> Self {
        let home = tempfile::tempdir().expect("isolated git HOME");
        std::fs::create_dir(home.path().join("home")).expect("isolated git HOME dir");
        Self {
            inner: Arc::new(IsolatedDagInner {
                origin: Instant::now(),
                events: Mutex::new(Vec::new()),
                home: home.path().join("home"),
                _home: home,
            }),
        }
    }

    fn with_isolated_git_home(&self, request: &ProcessRequest) -> ProcessRequest {
        let mut request = request.clone();
        let mut ensure = |key: &str, value: OsString| {
            if !request.environment.iter().any(|(name, _)| name == key) {
                request.environment.push((OsString::from(key), value));
            }
        };
        ensure("HOME", self.inner.home.as_os_str().to_os_string());
        ensure("GIT_TERMINAL_PROMPT", OsString::from("0"));
        request
    }

    fn dump(&self, step: &str) {
        if !dag_proc_trace() {
            return;
        }
        let events = self.inner.events.lock().expect("isolated runner events");
        eprintln!(
            "isolated-dag-runner dump step={step} thread={:?} events={}",
            thread::current().name(),
            events.len()
        );
        for (index, event) in events.iter().enumerate() {
            eprintln!(
                "isolated-dag-runner #{index} at={:?} dur={:?} deadline={:?} caller_pid={} caller_pgid={} thread={} outcome={} argv={} {}",
                event.at,
                event.duration,
                event.deadline,
                event.caller_pid,
                event.caller_pgid,
                event.thread,
                event.outcome,
                event.program,
                event.args.join(" ")
            );
            for (sample_index, sample) in event.samples.iter().enumerate() {
                eprintln!("isolated-dag-runner #{index} sample#{sample_index} {sample}");
            }
        }
    }

    fn record(&self, event: IsolatedProcEvent) {
        if !dag_proc_trace() {
            return;
        }
        eprintln!(
            "isolated-dag-runner at={:?} dur={:?} deadline={:?} caller_pid={} caller_pgid={} thread={:?} outcome={} argv={} {}",
            event.at,
            event.duration,
            event.deadline,
            event.caller_pid,
            event.caller_pgid,
            thread::current().name(),
            event.outcome,
            event.program,
            event.args.join(" ")
        );
        for (sample_index, sample) in event.samples.iter().enumerate() {
            eprintln!("isolated-dag-runner sample#{sample_index} {sample}");
        }
        self.inner
            .events
            .lock()
            .expect("isolated runner events")
            .push(event);
    }

    fn run_ssh(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let operation = request
            .args
            .last()
            .and_then(|argument| argument.to_str())
            .unwrap_or_default();
        match operation {
            "~/.local/bin/worker host probe" => canonical_host_process(&ready_host_probe()),
            value if value == HostOperation::RefreshFacts.command() => Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout: Vec::new(),
                stderr: Vec::new(),
            }),
            other => Err(WorkerError::Protocol(format!(
                "unexpected isolated SSH operation: {other}"
            ))),
        }
    }

    fn run_isolated(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/git") {
            SystemProcessRunner.run(&self.with_isolated_git_home(request))
        } else if request.program == OsStr::new("/usr/bin/ssh") {
            self.run_ssh(request)
        } else {
            Err(WorkerError::Protocol(format!(
                "unexpected isolated process: {}",
                request.program.to_string_lossy()
            )))
        }
    }

    fn run_traced(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let started = Instant::now();
        let stop = Arc::new(AtomicBool::new(false));
        let samples = Arc::new(Mutex::new(Vec::new()));
        let watcher = if request.program == OsStr::new("/usr/bin/git") {
            let stop = stop.clone();
            let samples = samples.clone();
            let target = request_git_target(request);
            Some(thread::spawn(move || {
                let started = Instant::now();
                while !stop.load(Ordering::SeqCst) && started.elapsed() < Duration::from_millis(200)
                {
                    thread::sleep(Duration::from_millis(10));
                }
                let mut index = 0;
                while !stop.load(Ordering::SeqCst) {
                    samples.lock().expect("git samples").push(format!(
                        "#{index} {}",
                        sample_slow_git_state(target.as_deref())
                    ));
                    index += 1;
                    let wait = Instant::now();
                    while !stop.load(Ordering::SeqCst)
                        && wait.elapsed() < Duration::from_millis(250)
                    {
                        thread::sleep(Duration::from_millis(10));
                    }
                }
            }))
        } else {
            None
        };
        let result = self.run_isolated(request);
        stop.store(true, Ordering::SeqCst);
        if let Some(watcher) = watcher {
            let _ = watcher.join();
        }
        let outcome = describe_run(&result);
        if started.elapsed() >= Duration::from_millis(200)
            || !outcome.starts_with("runner=ok git=success")
        {
            samples.lock().expect("git samples").push(format!(
                "#final {}",
                sample_slow_git_state(request_git_target(request).as_deref())
            ));
        }
        self.record(IsolatedProcEvent {
            thread: thread::current().name().unwrap_or("unnamed").to_owned(),
            at: self.inner.origin.elapsed(),
            duration: started.elapsed(),
            deadline: request.policy.deadline,
            caller_pid: std::process::id(),
            caller_pgid: caller_pgid(),
            program: request.program.to_string_lossy().into_owned(),
            args: request
                .args
                .iter()
                .map(|argument| argument.to_string_lossy().into_owned())
                .collect(),
            outcome,
            samples: samples.lock().expect("git samples").clone(),
        });
        result
    }
}

impl ProcessRunner for IsolatedDagRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if dag_proc_trace() {
            self.run_traced(request)
        } else {
            self.run_isolated(request)
        }
    }
}

struct FrozenDagFixture {
    _repo: support::GitRepo,
    _root: tempfile::TempDir,
    paths: mac_worker::paths::PathLayout,
    store: Arc<ClientStateStore>,
    runner: IsolatedDagRunner,
    run_id: RunId,
    task_id: TaskId,
    turn_id: TurnId,
}

fn prepared_frozen_dag() -> FrozenDagFixture {
    prepared_frozen_dag_inner(None)
}

fn prepared_frozen_dag_inner(
    hook: Option<Arc<dyn ClientStateConcurrencyHook>>,
) -> FrozenDagFixture {
    prepared_frozen_dag_mutated(hook, |_| {})
}

fn prepared_frozen_dag_mutated(
    hook: Option<Arc<dyn ClientStateConcurrencyHook>>,
    mutate: impl FnOnce(&mut DagNode),
) -> FrozenDagFixture {
    // `ProcessRequest` carries no working directory (`src/process.rs`), so every
    // Git child this builder spawns - `GitRepo` setup, `ProjectState::load`,
    // `pin_object` - inherits the PROCESS-GLOBAL cwd. `CWD_LOCK` exists to own
    // that global, but only the tests that MOVE the cwd took it; a builder that
    // merely READS it did not. A `CwdLockedRepo` elsewhere could therefore
    // delete the directory these children had already inherited, and Git failed
    // with `Unable to read current working directory` even though every object
    // was present. Participating in the same lock closes that window without
    // serializing the suite: only fixture construction is excluded against the
    // cwd movers, and each test body still runs in parallel afterwards.
    let _cwd_lock = CWD_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let repo = support::GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    let root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
    let store = Arc::new(match hook {
        Some(hook) => ClientStateStore::open_with_concurrency_hook(&paths.state, hook).unwrap(),
        None => ClientStateStore::open(&paths.state).unwrap(),
    });
    plant_bound_mini1_ready(&store);
    let runner = IsolatedDagRunner::new();
    let project = match ProjectState::load(&runner, repo.root(), &[]) {
        Ok(project) => project,
        Err(error) => {
            runner.dump("prepared_frozen_dag ProjectState::load");
            panic!("prepared_frozen_dag ProjectState::load: {error}");
        }
    };
    let oid = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let run_id = RunId::generate();
    let task_id = TaskId::generate();
    let turn_id = TurnId::generate();
    let pin_ref = dag_pin_ref(run_id, "root");
    let transfer = TransferRepo::open_or_create(&paths.cache, &project.context.common_dir).unwrap();
    transfer.pin_object(&runner, &pin_ref, &oid).unwrap();
    drop(transfer);
    let mut requires = project.requirements.clone();
    if !requires.iter().any(|item| item == "agent:codex") {
        requires.push("agent:codex".into());
    }
    let mut node = DagNode {
        batch_id: "root".into(),
        task_id,
        turn_id,
        depends_on: Vec::new(),
        base: DagBase::Frozen {
            oid,
            pin_ref,
            wip: false,
        },
        frozen: DagFrozenSpec {
            prompt: "do work".into(),
            title: None,
            agent: "codex".into(),
            model: None,
            effort: None,
            source: project.settings.task.source.clone(),
            origin_url: project.origin.clone(),
            publish: project.settings.task.publish.clone(),
            publish_branch: None,
            close_on: ClosePolicy::Done,
            env_profile: None,
            worker: None,
            wip: false,
            project_path: project.context.root.to_string_lossy().into_owned(),
            project_id: project.context.project_id.clone(),
            worktree_id: project.context.worktree_id.clone(),
            timeout_millis: project.settings.task.timeout.as_millis() as u64,
            max_turns: None,
            max_budget_usd_cents: None,
            max_followups: project.settings.task.max_followups,
            permissions: "workspace".into(),
            requires,
            include_untracked: project.settings.snapshot.include_untracked.clone(),
            include_empty_dirs: project.settings.snapshot.include_empty_dirs.clone(),
            allow_sensitive: project.settings.snapshot.allow_sensitive.clone(),
            cli_includes: Vec::new(),
            branch: project.context.branch.clone(),
        },
        state: DagNodeState::Waiting,
        bound_oid: None,
        bound_turn_id: None,
        pin_ref: None,
        blocked_by: None,
        claimed_by: None,
        claimed_at_millis: None,
    };
    mutate(&mut node);
    node.state = DagNodeState::Waiting;
    let dag = DagRecord::new(run_id, BTreeMap::from([("root".into(), node)]), 1, None, 9).unwrap();
    store
        .create_run_with_dag(RunRecord::new(run_id, None, Vec::new(), 1, 9).unwrap(), dag)
        .unwrap();
    FrozenDagFixture {
        _repo: repo,
        _root: root,
        paths,
        store,
        runner,
        run_id,
        task_id,
        turn_id,
    }
}

fn advance_dags(fixture: &FrozenDagFixture) -> Result<(), WorkerError> {
    let config = dag_test_config();
    let executor = InlineRunnerExecutor;
    TaskClient::new(
        &fixture.runner,
        &config,
        &fixture.paths,
        &fixture.store,
        &executor,
    )
    .advance_pending_dags()
}

fn occupy_worker_slot(fixture: &FrozenDagFixture) {
    let dummy_task = TaskId::generate();
    let dummy_turn = TurnId::generate();
    fixture
        .store
        .create_task(intent_task(RunId::generate(), dummy_task, dummy_turn))
        .unwrap();
    let identity = InlineRunnerExecutor
        .start(&fixture.paths, dummy_task, dummy_turn)
        .unwrap();
    fixture
        .store
        .record_runner(dummy_task, Some(identity))
        .unwrap();
}

fn assert_exact_once_submit(fixture: &FrozenDagFixture, created_at: u64) {
    let record = fixture.store.load_task(fixture.task_id).unwrap();
    assert_eq!(record.meta().task_id(), fixture.task_id);
    assert_eq!(record.meta().created_at_millis(), created_at);
    assert!(record.submission_intent_turn_id().is_none());
    assert!(
        fixture
            .store
            .queue_entry(fixture.turn_id)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        fixture.store.turn_ids_for_task(fixture.task_id).unwrap(),
        vec![fixture.turn_id]
    );
    assert_eq!(
        fixture.store.load_run(fixture.run_id).unwrap().task_ids(),
        &[fixture.task_id]
    );
    assert_eq!(
        fixture
            .store
            .load_run_dag(fixture.run_id)
            .unwrap()
            .unwrap()
            .nodes["root"]
            .state,
        DagNodeState::Submitted
    );
    let prompt = fixture
        .store
        .read_turn_prompt(fixture.task_id, fixture.turn_id)
        .unwrap();
    assert!(prompt.contains("User request:\ndo work"), "{prompt}");
    assert!(prompt.contains("Base branch: main"), "{prompt}");
}

fn recover_after_fault(point: ClientStateWritePoint, occupy: bool) {
    eprintln!(
        "recover_after_fault begin point={point:?} occupy={occupy} thread={:?}",
        thread::current().name()
    );
    let fixture = prepared_frozen_dag();
    if occupy {
        occupy_worker_slot(&fixture);
    }
    fixture.store.inject_write_failure_once(point);
    let first_started = Instant::now();
    let first = match advance_dags(&fixture) {
        Err(error) => error,
        Ok(()) => {
            fixture.runner.dump("first advance unexpectedly succeeded");
            panic!("expected injected failure at {point:?} occupy={occupy}");
        }
    };
    eprintln!(
        "recover_after_fault first advance point={point:?} occupy={occupy} elapsed={:?} error={first}",
        first_started.elapsed()
    );
    if !(first.to_string().contains("injected failure")
        || first.to_string().contains("before ")
        || first.to_string().contains("after "))
    {
        fixture
            .runner
            .dump(&format!("first advance at {point:?} occupy={occupy}"));
        panic!("at {point:?} occupy={occupy}: {first}");
    }
    assert!(
        fixture
            .store
            .load_run_dag(fixture.run_id)
            .unwrap()
            .is_some()
    );
    let created_at = fixture
        .store
        .load_task(fixture.task_id)
        .unwrap()
        .meta()
        .created_at_millis();
    let resume_started = Instant::now();
    if let Err(error) = advance_dags(&fixture) {
        fixture
            .runner
            .dump(&format!("resume advance at {point:?} occupy={occupy}"));
        panic!("resume at {point:?} occupy={occupy}: {error}");
    }
    eprintln!(
        "recover_after_fault resume ok point={point:?} occupy={occupy} elapsed={:?}",
        resume_started.elapsed()
    );
    assert_exact_once_submit(&fixture, created_at);
}

#[test]
fn dag_submit_resumes_same_ids_after_create_prompt_enqueue_and_handoff_faults() {
    recover_after_fault(ClientStateWritePoint::BeforeTurnPromptWrite, false);
    recover_after_fault(ClientStateWritePoint::BeforePublish, false);
    recover_after_fault(
        ClientStateWritePoint::AfterSubmissionIntentClearPublicationBeforeFinalSync,
        false,
    );
    recover_after_fault(ClientStateWritePoint::AfterParkedTaskTurnPublication, true);
}

#[test]
fn dag_same_pid_concurrent_advancers_submit_one_identity() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let hook = Arc::new(SubmitGuardBarrier {
        entered: entered_tx,
        release: Mutex::new(Some(release_rx)),
        first: AtomicBool::new(false),
    });
    let fixture = prepared_frozen_dag_inner(Some(hook));
    let second_done = AtomicBool::new(false);
    thread::scope(|scope| {
        let first = scope.spawn(|| advance_dags(&fixture));
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("first holder reached DagSubmit before TransferRepo");
        let second = scope.spawn(|| {
            let result = advance_dags(&fixture);
            second_done.store(true, Ordering::SeqCst);
            result
        });
        thread::sleep(Duration::from_millis(200));
        assert!(
            !second_done.load(Ordering::SeqCst),
            "same-PID waiter must block on the store/task guard"
        );
        release_tx.send(()).unwrap();
        first.join().expect("first advancer").unwrap();
        second.join().expect("waiter").unwrap();
    });
    let created_at = fixture
        .store
        .load_task(fixture.task_id)
        .unwrap()
        .meta()
        .created_at_millis();
    assert_exact_once_submit(&fixture, created_at);
}

const SIDECAR_OBSERVED_AT: u64 = 1_700_000_000_042;
const SIDECAR_DELIVERY_OID: &str = "fedcba9876543210fedcba9876543210fedcba98";

fn plant_rollback_incomplete_matching_frozen(fixture: &FrozenDagFixture) {
    let dag = fixture.store.load_run_dag(fixture.run_id).unwrap().unwrap();
    let node = &dag.nodes["root"];
    assert_eq!(node.frozen.source, "local");
    let oid = node.execution_oid().cloned().expect("frozen OID");
    let publish = node
        .frozen
        .publish
        .iter()
        .map(|mode| match mode.as_str() {
            "fetch" => PublishMode::Fetch,
            "push" => PublishMode::Push,
            other => panic!("unexpected frozen publish {other}"),
        })
        .collect();
    let meta = TaskMeta::new(TaskMetaInput {
        task_id: node.task_id,
        run_id: Some(fixture.run_id),
        project_id: node.frozen.project_id.clone(),
        worktree_id: node.frozen.worktree_id.clone(),
        agent: AgentKind::Codex,
        model: node.frozen.model.clone(),
        effort: node.frozen.effort.clone(),
        policy: node.frozen.permission_policy().unwrap(),
        source: TaskSource::Local {
            wip: node.frozen.wip,
            push_target: None,
        },
        publish,
        publish_branch: None,
        base_oid: oid.clone(),
        limits: node.frozen.limits().unwrap(),
        close_policy: node.frozen.close_on,
        env_profile: node.frozen.env_profile.clone(),
        git_identity: GitIdentity::new("mac-worker", "mac-worker@localhost").unwrap(),
        title: node.frozen.title.clone(),
        prompt: node.frozen.prompt.clone(),
        created_at_millis: 1_700_000_000_000,
    })
    .unwrap();
    let status = TaskStatus::new(
        TaskState::Abandoned,
        Some(TaskOutcome::failed("SUBMISSION_ROLLBACK_INCOMPLETE")),
        None,
        false,
        Some(oid),
        None,
        Vec::new(),
        Vec::new(),
        None,
        Vec::new(),
        1_700_000_000_000,
    )
    .unwrap();
    let record = LocalTaskRecord::new(
        meta,
        status,
        Some(1_700_000_000_001),
        None,
        None,
        REPO_ID.to_owned(),
        None,
        true,
        Some("SUBMISSION_ROLLBACK_INCOMPLETE".into()),
    )
    .unwrap()
    .with_submission_rollback_turn_id(node.turn_id)
    .unwrap();
    fixture.store.create_task(record).unwrap();
}

fn bump_rollback_sidecars(fixture: &FrozenDagFixture, observed_at: u64) {
    let current = fixture.store.load_task(fixture.task_id).unwrap();
    assert_eq!(
        current.abandon_code(),
        Some("SUBMISSION_ROLLBACK_INCOMPLETE")
    );
    let bumped = current
        .with_status_observed_at(Some(observed_at))
        .unwrap()
        .with_fetched_head(Some(SIDECAR_DELIVERY_OID.parse().unwrap()))
        .unwrap()
        .with_runner(Some(RunnerIdentity::new(owner(99))))
        .unwrap();
    fixture.store.update_task(bumped).unwrap();
}

struct RollbackRecoverBarrier {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    used: AtomicBool,
    once: bool,
}

impl ClientStateConcurrencyHook for RollbackRecoverBarrier {
    fn reach(&self, point: ClientStateConcurrencyPoint) {
        if point != ClientStateConcurrencyPoint::SubmissionRollbackRecover {
            return;
        }
        if self.once && self.used.swap(true, Ordering::SeqCst) {
            return;
        }
        let _ = self.entered.send(());
        let _ = self
            .release
            .lock()
            .expect("rollback recover barrier")
            .recv();
    }
}

#[test]
fn dag_submit_retries_rollback_cas_after_sidecar_change_and_keeps_latest_fields() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let hook = Arc::new(RollbackRecoverBarrier {
        entered: entered_tx,
        release: Mutex::new(release_rx),
        used: AtomicBool::new(false),
        once: true,
    });
    let fixture = prepared_frozen_dag_inner(Some(hook));
    plant_rollback_incomplete_matching_frozen(&fixture);
    occupy_worker_slot(&fixture);
    thread::scope(|scope| {
        let advancer = scope.spawn(|| advance_dags(&fixture));
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("rollback recover reached CAS window");
        bump_rollback_sidecars(&fixture, SIDECAR_OBSERVED_AT);
        release_tx.send(()).unwrap();
        advancer.join().expect("advancer").unwrap();
    });
    let record = fixture.store.load_task(fixture.task_id).unwrap();
    assert_ne!(record.status().state(), TaskState::Abandoned);
    assert!(record.abandon_code().is_none());
    assert!(record.submission_rollback_turn_id().is_none());
    assert_eq!(
        record.status_observed_at_millis(),
        Some(SIDECAR_OBSERVED_AT)
    );
    assert_eq!(
        record.fetched_head(),
        Some(&SIDECAR_DELIVERY_OID.parse::<BaseOid>().unwrap())
    );
    assert_exact_once_submit(&fixture, 1_700_000_000_000);
}

#[test]
fn dag_submit_reports_conflict_when_rollback_cas_cannot_catch_sidecar_writes() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let hook = Arc::new(RollbackRecoverBarrier {
        entered: entered_tx,
        release: Mutex::new(release_rx),
        used: AtomicBool::new(false),
        once: false,
    });
    let fixture = prepared_frozen_dag_inner(Some(hook));
    plant_rollback_incomplete_matching_frozen(&fixture);
    thread::scope(|scope| {
        let advancer = scope.spawn(|| advance_dags(&fixture));
        for attempt in 0..2 {
            entered_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("rollback recover attempt");
            bump_rollback_sidecars(&fixture, SIDECAR_OBSERVED_AT + attempt);
            release_tx.send(()).unwrap();
        }
        let error = advancer.join().expect("advancer").unwrap_err();
        assert_eq!(error.public_code(), "TASK_ID_CONFLICT");
    });
    let record = fixture.store.load_task(fixture.task_id).unwrap();
    assert_eq!(
        record.abandon_code(),
        Some("SUBMISSION_ROLLBACK_INCOMPLETE")
    );
    assert_eq!(record.status().state(), TaskState::Abandoned);
    assert_eq!(
        record.status_observed_at_millis(),
        Some(SIDECAR_OBSERVED_AT + 1)
    );
    assert_eq!(
        record.fetched_head(),
        Some(&SIDECAR_DELIVERY_OID.parse::<BaseOid>().unwrap())
    );
    assert!(
        fixture
            .store
            .load_run(fixture.run_id)
            .unwrap()
            .task_ids()
            .is_empty()
    );
}

struct SubmitGuardBarrier {
    entered: mpsc::Sender<()>,
    release: Mutex<Option<mpsc::Receiver<()>>>,
    first: AtomicBool,
}

impl ClientStateConcurrencyHook for SubmitGuardBarrier {
    fn reach(&self, point: ClientStateConcurrencyPoint) {
        if point != ClientStateConcurrencyPoint::DagSubmit {
            return;
        }
        if self.first.swap(true, Ordering::SeqCst) {
            return;
        }
        let _ = self.entered.send(());
        let release = self
            .release
            .lock()
            .expect("submit barrier")
            .take()
            .expect("submit barrier release");
        let _ = release.recv();
    }
}

struct FailingStartExecutor;

impl RunnerExecutor for FailingStartExecutor {
    fn start(
        &self,
        _paths: &mac_worker::paths::PathLayout,
        _task_id: TaskId,
        _turn_id: TurnId,
    ) -> Result<RunnerIdentity, WorkerError> {
        Err(WorkerError::task(
            "RUNNER_START_FAILED",
            "injected start failure",
        ))
    }
}

#[test]
fn dag_first_submit_start_runner_failure_is_not_hidden_by_a_queued_row() {
    let repo = support::GitRepo::init();
    repo.write("base.txt", b"base\n");
    repo.commit_all("base");
    let root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
    let store = ClientStateStore::open(&paths.state).unwrap();
    plant_bound_mini1_ready(&store);
    let config = dag_test_config();
    let runner = IsolatedDagRunner::new();
    let executor = FailingStartExecutor;
    let error = TaskClient::new(&runner, &config, &paths, &store, &executor)
        .submit(
            TaskSubmitRequest {
                agent: AgentKind::Codex,
                model: None,
                effort: None,
                prompt: "do work".into(),
                project: repo.root().to_path_buf(),
                base: "main".into(),
                wip: false,
                source: Some("local".into()),
                publish: Some(vec!["fetch".into()]),
                publish_branch: None,
                cli_includes: Vec::new(),
                limits: TaskLimits::default(),
                close_policy: ClosePolicy::Never,
                env_profile: None,
                preference: WorkerPreference::Automatic,
                wait_for_capacity: true,
                attached: false,
                run_id: None,
            },
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("injected start failure"),
        "{error}"
    );
    let tasks = store.list_tasks().unwrap();
    assert_eq!(tasks.len(), 1);
    let task_id = tasks[0].meta().task_id();
    assert!(store.queue_entry_for_task_turn(task_id).unwrap().is_some());
    assert!(store.runner_liveness(task_id).unwrap().is_none());
}

struct CurrentDirGuard {
    previous: PathBuf,
}

impl CurrentDirGuard {
    fn enter(path: &Path) -> Self {
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

/// Holds `CWD_LOCK` and process cwd for a temporary Git worktree.
///
/// Drop restores cwd while the worktree still exists, then removes the
/// worktree, then releases the lock. User `Drop` runs before field
/// destructors, so cwd is taken first and the lock field is left for last.
struct CwdLockedRepo {
    cwd: Option<CurrentDirGuard>,
    repo: Option<support::GitRepo>,
    _cwd_lock: std::sync::MutexGuard<'static, ()>,
}

impl CwdLockedRepo {
    fn enter(repo: support::GitRepo) -> Self {
        let cwd_lock = CWD_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let cwd = CurrentDirGuard::enter(repo.root());
        Self {
            cwd: Some(cwd),
            repo: Some(repo),
            _cwd_lock: cwd_lock,
        }
    }

    fn repo(&self) -> &support::GitRepo {
        self.repo.as_ref().expect("cwd-locked repo still present")
    }
}

impl Drop for CwdLockedRepo {
    fn drop(&mut self) {
        let root = self.repo.as_ref().map(|repo| repo.root().to_path_buf());
        drop(self.cwd.take());
        // `_cwd_lock` is still held: field destructors run after this function.
        if let Some(root) = &root {
            match std::env::current_dir() {
                Ok(cwd) if cwd != *root && cwd.exists() => {
                    assert!(
                        root.exists(),
                        "batch worktree must still exist after cwd restore"
                    );
                }
                other => panic!(
                    "cwd must be restored before deleting the batch worktree; cwd={other:?} root={}",
                    root.display()
                ),
            }
        }
        drop(self.repo.take());
    }
}

#[test]
fn cwd_locked_repo_restores_cwd_while_the_worktree_still_exists() {
    let repo = support::GitRepo::init();
    let root = repo.root().to_path_buf();
    let locked = CwdLockedRepo::enter(repo);
    assert_eq!(
        std::env::current_dir().unwrap().canonicalize().unwrap(),
        root.canonicalize().unwrap()
    );
    assert!(root.exists());
    drop(locked);
    assert!(!root.exists());
}

/// The frozen-DAG fixture spawns Git children that inherit the process-global
/// cwd, so it must not build while another test owns that global. Proven
/// directly: with `CWD_LOCK` held here, construction on another thread cannot
/// complete, and it completes once the lock is released.
///
/// Before the fixture participated in the lock this assertion failed, because
/// construction raced straight through while a `CwdLockedRepo` elsewhere could
/// delete the very directory its Git children had inherited.
#[test]
fn frozen_dag_fixture_does_not_build_while_another_test_owns_the_cwd() {
    let guard = CWD_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let (built, observe) = mpsc::channel();
    let builder = thread::spawn(move || {
        let fixture = prepared_frozen_dag();
        let _ = built.send(());
        drop(fixture);
    });

    let raced = observe.recv_timeout(Duration::from_secs(3));
    assert!(
        raced.is_err(),
        "fixture construction ran implicit-cwd Git while another test owned the process cwd"
    );

    drop(guard);
    builder.join().expect("fixture builder thread");
}

#[test]
fn deleted_inherited_cwd_fails_owned_view_git_in_an_isolated_child() {
    let root = tempfile::tempdir().unwrap();
    let isolate = root.path().join("isolate.git");
    let work = root.path().join("work");
    let victim = root.path().join("victim");
    fs::create_dir(&work).unwrap();
    fs::create_dir(&victim).unwrap();
    assert!(
        Command::new("/usr/bin/git")
            .args(["init", "-q", "--bare"])
            .arg(&isolate)
            .status()
            .unwrap()
            .success()
    );
    let mut commit = Command::new("/usr/bin/git");
    commit
        .args([
            "--git-dir",
            isolate.to_str().unwrap(),
            "--work-tree",
            work.to_str().unwrap(),
            "commit",
            "--allow-empty",
            "-q",
            "-m",
            "base",
        ])
        .env("GIT_AUTHOR_NAME", "cwd-drop")
        .env("GIT_AUTHOR_EMAIL", "cwd-drop@example.test")
        .env("GIT_COMMITTER_NAME", "cwd-drop")
        .env("GIT_COMMITTER_EMAIL", "cwd-drop@example.test")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0");
    assert!(commit.status().unwrap().success());
    let oid = String::from_utf8(
        Command::new("/usr/bin/git")
            .args(["--git-dir", isolate.to_str().unwrap(), "rev-parse", "HEAD"])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_owned();

    let _cwd_lock = CWD_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let parent_cwd = std::env::current_dir().unwrap();
    let output = Command::new("/bin/sh")
        .arg("-c")
        .arg(
            r#"
set +e
cd "$VICTIM" || exit 90
rmdir "$VICTIM" || exit 91
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_COMMON_DIR GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 GIT_TERMINAL_PROMPT=0
/usr/bin/git --git-dir="$ISOLATE" -c gc.auto=0 cat-file -t "$OID"
echo CAT_STATUS:$?
/usr/bin/git --git-dir="$ISOLATE" -c gc.auto=0 rev-list --objects "$OID"
echo REV_STATUS:$?
"#,
        )
        .env("VICTIM", &victim)
        .env("ISOLATE", &isolate)
        .env("OID", &oid)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(std::env::current_dir().unwrap(), parent_cwd);
    assert!(
        stdout.contains("CAT_STATUS:128") && stdout.contains("REV_STATUS:128"),
        "stdout={stdout} stderr={stderr}"
    );
    assert!(
        stderr.contains("Unable to read current working directory"),
        "stdout={stdout} stderr={stderr}"
    );
}

fn plant_closed_done_import(
    store: &ClientStateStore,
    task_id: TaskId,
    turn_id: TurnId,
    result_oid: mac_worker::task::BaseOid,
) {
    if let Some(entry) = store.queue_entry_for_task_turn(task_id).unwrap() {
        let owner = entry
            .owner_opt()
            .copied()
            .unwrap_or_else(|| owner(std::process::id()));
        store.record_runner(task_id, None).unwrap();
        store
            .remove_task_turn_after_terminal(entry.job_id(), owner)
            .unwrap();
    }
    let record = store.load_task(task_id).unwrap();
    let status = TaskStatus::new(
        TaskState::Closed,
        Some(TaskOutcome::Done),
        Some("mini-1".into()),
        false,
        Some(result_oid.clone()),
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![TurnSummary::new(
            1,
            turn_id,
            Some(TurnTerminal::Succeeded),
            Some(TaskOutcome::Done),
            Some(true),
            false,
            Some(1),
            Some(2),
        )],
        record.status().updated_at_millis() + 1,
    )
    .unwrap();
    store
        .update_task(
            record
                .with_status(status)
                .unwrap()
                .with_fetched_head(Some(result_oid))
                .unwrap(),
        )
        .unwrap();
    store
        .finish_local_runner_log(task_id, turn_id, TaskOutcome::Done)
        .unwrap();
}

fn plant_parent_status(
    store: &ClientStateStore,
    task_id: TaskId,
    turn_id: TurnId,
    result_oid: mac_worker::task::BaseOid,
    state: TaskState,
    outcome: TaskOutcome,
    terminal: TurnTerminal,
) {
    if let Some(entry) = store.queue_entry_for_task_turn(task_id).unwrap() {
        let owner = entry
            .owner_opt()
            .copied()
            .unwrap_or_else(|| owner(std::process::id()));
        store.record_runner(task_id, None).unwrap();
        store
            .remove_task_turn_after_terminal(entry.job_id(), owner)
            .unwrap();
    }
    let record = store.load_task(task_id).unwrap();
    let status = TaskStatus::new(
        state,
        Some(outcome.clone()),
        Some("mini-1".into()),
        false,
        Some(result_oid.clone()),
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![TurnSummary::new(
            1,
            turn_id,
            Some(terminal),
            Some(outcome.clone()),
            Some(true),
            false,
            Some(1),
            Some(2),
        )],
        record.status().updated_at_millis() + 1,
    )
    .unwrap();
    store
        .update_task(
            record
                .with_status(status)
                .unwrap()
                .with_fetched_head(Some(result_oid))
                .unwrap(),
        )
        .unwrap();
    if matches!(outcome, TaskOutcome::Done) {
        store
            .finish_local_runner_log(task_id, turn_id, outcome)
            .unwrap();
    }
}

struct FollowupWorker {
    requests: Mutex<Vec<ProcessRequest>>,
    first_turn: TurnSummary,
    first_oid: BaseOid,
    result_oid: BaseOid,
    session_task: Mutex<Option<TaskId>>,
    prepared: Mutex<Option<(TaskId, TurnId)>>,
    turn_submitted: AtomicBool,
    /// Host already auto-closed after Done. First-turn DAG parents use this
    /// so laptop follow persists Closed before it advances `from:` children.
    close_after_done: bool,
}

impl FollowupWorker {
    fn new(first_turn: TurnSummary, first_oid: BaseOid, result_oid: BaseOid) -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            first_turn,
            first_oid,
            result_oid,
            session_task: Mutex::new(None),
            prepared: Mutex::new(None),
            turn_submitted: AtomicBool::new(false),
            close_after_done: false,
        }
    }

    fn auto_closed(result_oid: BaseOid) -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            first_turn: TurnSummary::new(
                1,
                TurnId::generate(),
                None,
                None,
                None,
                false,
                Some(1),
                None,
            ),
            first_oid: result_oid.clone(),
            result_oid,
            session_task: Mutex::new(None),
            prepared: Mutex::new(None),
            turn_submitted: AtomicBool::new(false),
            close_after_done: true,
        }
    }

    fn requests(&self) -> Vec<ProcessRequest> {
        self.requests.lock().unwrap().clone()
    }

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    fn bind_ids(&self, task_id: TaskId, turn_id: TurnId) {
        *self.prepared.lock().unwrap() = Some((task_id, turn_id));
    }

    fn task_status(&self, task_id: TaskId) -> Result<TaskStatus, WorkerError> {
        let (prepared_task, followup) = self
            .prepared
            .lock()
            .unwrap()
            .ok_or_else(|| WorkerError::Protocol("task status before lease".into()))?;
        assert_eq!(prepared_task, task_id);
        let done = self.turn_submitted.load(Ordering::SeqCst);
        let now = Self::now_millis();
        let current = TurnSummary::new(
            if self.close_after_done { 1 } else { 2 },
            followup,
            done.then_some(TurnTerminal::Succeeded),
            done.then_some(TaskOutcome::Done),
            done.then_some(true),
            false,
            Some(now.saturating_sub(1).max(1)),
            done.then_some(now),
        );
        let turns = if self.close_after_done {
            vec![current]
        } else {
            vec![self.first_turn.clone(), current]
        };
        let state = if done {
            if self.close_after_done {
                TaskState::Closed
            } else {
                TaskState::Open
            }
        } else {
            TaskState::Active
        };
        let last_outcome = if done {
            Some(TaskOutcome::Done)
        } else if self.close_after_done {
            None
        } else {
            Some(TaskOutcome::NeedsInput)
        };
        TaskStatus::new(
            state,
            last_outcome,
            Some("mini-1".into()),
            true,
            Some(if done {
                self.result_oid.clone()
            } else {
                self.first_oid.clone()
            }),
            None,
            Vec::new(),
            Vec::new(),
            None,
            turns,
            now,
        )
    }

    fn plant_result_ref(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let transfer = request
            .args
            .windows(2)
            .find(|window| window[0] == OsStr::new("-C"))
            .map(|window| PathBuf::from(&window[1]))
            .ok_or_else(|| WorkerError::Protocol("result fetch missing -C".into()))?;
        let local_ref = format!("refs/mac-worker/results/{}", self.followup_task_id());
        let status = std::process::Command::new("/usr/bin/git")
            .arg("-C")
            .arg(&transfer)
            .args(["update-ref", &local_ref, self.result_oid.as_str()])
            .status()
            .map_err(WorkerError::Io)?;
        if !status.success() {
            return Err(WorkerError::Protocol(
                "fixture could not plant result ref".into(),
            ));
        }
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: Vec::new(),
            stderr: Vec::new(),
        })
    }

    fn followup_task_id(&self) -> TaskId {
        self.prepared
            .lock()
            .unwrap()
            .as_ref()
            .map(|(task, _)| *task)
            .expect("task-prepare must run before result fetch")
    }

    fn terminal_status(
        &self,
        job_id: mac_worker::job::JobId,
    ) -> Result<StatusResponse, WorkerError> {
        let requests = self.requests();
        let material = if let Some(request) = requests.iter().rev().find(|request| {
            request
                .args
                .last()
                .is_some_and(|argument| argument == HostOperation::TaskTurn.command())
        }) {
            let turn: TaskTurnRequest = decode_host_request(request)?;
            turn.submit().material().clone()
        } else {
            let request = requests
                .iter()
                .rev()
                .find(|request| {
                    request
                        .args
                        .last()
                        .is_some_and(|argument| argument == HostOperation::LeaseAcquire.command())
                })
                .ok_or_else(|| WorkerError::Protocol("status before lease".into()))?;
            let acquire: LeaseAcquireRequest = decode_host_request(request)?;
            acquire.material().clone()
        };
        let meta = JobMeta::new(&material, material.fingerprint())?;
        assert_eq!(job_id, meta.job_id());
        StatusResponse::new(
            meta,
            JobStatus::new(
                JobState::Succeeded,
                material.created_at_millis() + 2,
                None,
                None,
                None,
                None,
                Some(0),
                None,
                Some(0),
                Some(0),
                None,
                None,
            )?,
        )
    }

    fn terminal_job(&self, query: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let query: StatusRequest = decode_host_request(query)?;
        canonical_host_process(&self.terminal_status(query.job_id())?)
    }
}

impl ProcessRunner for FollowupWorker {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.requests.lock().unwrap().push(request.clone());
        if request.program == OsStr::new("/usr/bin/git") {
            let is_result_fetch = request.args.iter().any(|argument| argument == "fetch")
                && request
                    .args
                    .iter()
                    .any(|argument| argument.to_string_lossy().starts_with("--upload-pack="));
            if is_result_fetch {
                return self.plant_result_ref(request);
            }
            if request.args.iter().any(|argument| argument == "push")
                && request
                    .args
                    .iter()
                    .any(|argument| argument.to_string_lossy().starts_with("--receive-pack="))
            {
                return Ok(ProcessResult {
                    status: ExitStatus::from_raw(0),
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                });
            }
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
            "~/.local/bin/worker host probe" => canonical_host_process(&ready_host_probe()),
            value if value == HostOperation::RefreshFacts.command() => Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout: Vec::new(),
                stderr: Vec::new(),
            }),
            value if value == HostOperation::LeaseAcquire.command() => {
                let acquire: LeaseAcquireRequest = decode_host_request(request)?;
                let material = acquire.material();
                if let Some(task_id) = *self.session_task.lock().unwrap() {
                    self.bind_ids(task_id, material.job_id());
                }
                let lease = LeaseRecord::new(
                    material,
                    acquire.request_fingerprint().clone(),
                    material.created_at_millis(),
                    material.created_at_millis() + material.timeout_millis(),
                )?;
                canonical_host_process(&LeaseAcquireResponse::Acquired { lease })
            }
            value if value == HostOperation::TaskPrepare.command() => {
                let prepare: TaskPrepareRequest = decode_host_request(request)?;
                self.bind_ids(prepare.meta().task_id(), prepare.job_id());
                canonical_host_process(&TaskPrepareResponse::new(
                    prepare.meta().base_oid().clone(),
                    false,
                ))
            }
            value if value == HostOperation::TaskSession.command() => {
                let session: TaskSessionRequest = decode_host_request(request)?;
                *self.session_task.lock().unwrap() = Some(session.task_id());
                canonical_host_process(&TaskSessionResponse::new(SessionBinding::new(
                    AgentKind::Codex,
                    "session-1",
                    1,
                )?))
            }
            value if value == HostOperation::TaskStatus.command() => {
                let status: TaskStatusRequest = decode_host_request(request)?;
                canonical_host_process(&TaskStatusResponse::new(
                    self.task_status(status.task_id())?,
                ))
            }
            value if value == HostOperation::TaskTurn.command() => {
                let turn: TaskTurnRequest = decode_host_request(request)?;
                let material = turn.submit().material();
                self.bind_ids(turn.turn().task_id(), material.job_id());
                self.turn_submitted.store(true, Ordering::SeqCst);
                let job_meta = JobMeta::new(material, material.fingerprint())?;
                let submit = SubmitResponse::Accepted {
                    meta: Box::new(job_meta),
                    status: JobStatus::accepted(material.created_at_millis() + 1)?,
                };
                canonical_host_process(&TaskTurnResponse::new(
                    submit,
                    self.task_status(turn.turn().task_id())?,
                ))
            }
            value if value == HostOperation::Status.command() => self.terminal_job(request),
            value if value == HostOperation::StatusLogs.command() => {
                let query: StatusLogsRequest = decode_host_request(request)?;
                canonical_host_process(&StatusLogsResponse::new(
                    self.terminal_status(query.job_id())?,
                    LogChunk::new(LogStream::Stdout, query.stdout_offset(), Vec::new())?,
                    LogChunk::new(LogStream::Stderr, query.stderr_offset(), Vec::new())?,
                )?)
            }
            value if value == HostOperation::LogChunk.command() => {
                let query: LogChunkRequest = decode_host_request(request)?;
                canonical_host_process(&LogChunkResponse::new(LogChunk::new(
                    query.stream(),
                    query.offset(),
                    Vec::new(),
                )?)?)
            }
            value if value == HostOperation::TaskClose.command() => {
                let close: TaskCloseRequest = decode_host_request(request)?;
                let current = self.task_status(close.task_id())?;
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
                canonical_host_process(&TaskCloseResponse::new(next))
            }
            other => Err(WorkerError::Protocol(format!(
                "unexpected fixture worker operation: {other}"
            ))),
        }
    }
}

fn ready_host_probe() -> ProbeResponse {
    ProbeResponse {
        protocol_version: PROTOCOL_VERSION,
        supervision_version: SUPERVISION_VERSION,
        hostname: "mini-1.local".into(),
        arch: "arm64".into(),
        os_version: "26.2".into(),
        free_disk_bytes: 100 * 1024 * 1024 * 1024,
        total_disk_bytes: 250 * 1024 * 1024 * 1024,
        memory_pressure: MemoryPressure::Normal,
        swap_used_bytes: Some(0),
        available_memory_bytes: Some(12 * 1024 * 1024 * 1024),
        cpu_counters: Some(CpuCounters {
            user_ticks: 10,
            system_ticks: 20,
            idle_ticks: 30,
            nice_ticks: 40,
        }),
        slot_state: SlotState::Idle,
        active_lease: None,
        capabilities: vec!["darwin-arm64".into()],
        agent_facts: Some(AgentFacts {
            agents: vec![AgentProbe {
                name: "codex".into(),
                version: Some("0.1.0".into()),
                auth: AgentAuth::Authenticated,
                auth_by_profile: Vec::new(),
            }],
            env_profiles: Vec::new(),
            git_identity: true,
            collected_at_millis: u64::MAX / 2,
            herdr: None,
            origin_https_helpers: Default::default(),
        }),
        facts_age_millis: Some(0),
        configured_slots: 1,
        busy_slots: 0,
    }
}

fn finish_before_watchdog<T>(timeout: Duration, work: impl FnOnce() -> T) -> T {
    let finished = Arc::new(AtomicBool::new(false));
    let flag = finished.clone();
    thread::spawn(move || {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if flag.load(Ordering::SeqCst) {
                return;
            }
            thread::sleep(Duration::from_millis(50));
        }
        if !flag.load(Ordering::SeqCst) {
            eprintln!("watchdog: DAG follow-up runner exceeded {timeout:?}");
            std::process::exit(1);
        }
    });
    let result = work();
    finished.store(true, Ordering::SeqCst);
    result
}

fn decode_host_request<T: serde::de::DeserializeOwned>(
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

fn canonical_host_process<T: serde::Serialize>(value: &T) -> Result<ProcessResult, WorkerError> {
    let mut stdout =
        serde_json::to_vec(value).map_err(|error| WorkerError::Protocol(error.to_string()))?;
    stdout.push(b'\n');
    Ok(ProcessResult {
        status: ExitStatus::from_raw(0),
        stdout,
        stderr: Vec::new(),
    })
}

struct BatchHarness {
    _root: tempfile::TempDir,
    paths: mac_worker::paths::PathLayout,
    store: ClientStateStore,
    config: Config,
    runner: IsolatedDagRunner,
    executor: InlineRunnerExecutor,
    worktree: CwdLockedRepo,
}

impl BatchHarness {
    fn new(tasks_toml: &[u8]) -> Self {
        let repo = support::GitRepo::init();
        repo.write("src/lib.rs", b"fn main() {}\n");
        repo.commit_all("base");
        repo.write("tasks.toml", tasks_toml);
        let root = tempfile::tempdir().unwrap();
        let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
        let store = ClientStateStore::open(&paths.state).unwrap();
        plant_bound_mini1_ready(&store);
        Self {
            _root: root,
            paths,
            store,
            config: dag_test_config(),
            runner: IsolatedDagRunner::new(),
            executor: InlineRunnerExecutor,
            worktree: CwdLockedRepo::enter(repo),
        }
    }

    fn repo(&self) -> &support::GitRepo {
        self.worktree.repo()
    }

    fn client(&self) -> TaskClient<'_> {
        TaskClient::new(
            &self.runner,
            &self.config,
            &self.paths,
            &self.store,
            &self.executor,
        )
    }

    fn client_with<'a>(&'a self, runner: &'a dyn ProcessRunner) -> TaskClient<'a> {
        TaskClient::new(
            runner,
            &self.config,
            &self.paths,
            &self.store,
            &self.executor,
        )
    }

    #[allow(dead_code)]
    fn occupy_slot(&self) -> TaskId {
        let dummy_task = TaskId::generate();
        let dummy_turn = TurnId::generate();
        self.store
            .create_task(intent_task(RunId::generate(), dummy_task, dummy_turn))
            .unwrap();
        let identity = InlineRunnerExecutor
            .start(&self.paths, dummy_task, dummy_turn)
            .unwrap();
        self.store
            .record_runner(dummy_task, Some(identity))
            .unwrap();
        dummy_task
    }

    #[allow(dead_code)]
    fn release_runner(&self, task_id: TaskId) {
        self.store.record_runner(task_id, None).unwrap();
    }

    fn batch(&self) -> mac_worker::task_client::RunReport {
        self.client()
            .batch(
                &self.repo().root().join("tasks.toml"),
                None,
                None,
                &mut std::io::sink(),
            )
            .unwrap()
    }

    fn head(&self) -> String {
        String::from_utf8(self.repo().git(&["rev-parse", "HEAD"]).stdout)
            .unwrap()
            .trim()
            .to_owned()
    }

    fn commit_result(&self, contents: &[u8]) -> mac_worker::task::BaseOid {
        self.repo().write("result.txt", contents);
        self.repo().commit_all("imported turn");
        self.head().parse().unwrap()
    }
}

#[test]
fn dag_batch_root_waits_then_binds_exact_imported_turn() {
    let _cwd_lock = CWD_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let repo = support::GitRepo::init();
    repo.write("src/lib.rs", b"fn main() {}\n");
    repo.commit_all("base");
    let original_head = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    repo.write(
        "tasks.toml",
        br#"
version = 1
agent = "codex"

[[tasks]]
id = "root"
prompt = "root work"

[[tasks]]
id = "child"
prompt = "child work"
depends_on = ["root"]
base = "from:root"
"#,
    );
    let root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
    let store = ClientStateStore::open(&paths.state).unwrap();
    plant_bound_mini1_ready(&store);
    let _cwd = CurrentDirGuard::enter(repo.root());
    let config = dag_test_config();
    let runner = IsolatedDagRunner::new();
    let executor = InlineRunnerExecutor;
    let client = TaskClient::new(&runner, &config, &paths, &store, &executor);
    let report = client
        .batch(
            &repo.root().join("tasks.toml"),
            None,
            None,
            &mut std::io::sink(),
        )
        .unwrap();

    let dag = store
        .load_run_dag(report.run_id())
        .unwrap()
        .expect("dependent batch writes a DAG");
    let root_node = &dag.nodes["root"];
    let child_node = &dag.nodes["child"];
    let root_task_id = root_node.task_id;
    let root_turn_id = root_node.turn_id;
    let child_task_id = child_node.task_id;
    let child_turn_id = child_node.turn_id;
    assert_eq!(root_node.state, DagNodeState::Submitted);
    assert_eq!(child_node.state, DagNodeState::Waiting);
    assert!(child_node.bound_oid.is_none());
    assert_eq!(
        store.load_run(report.run_id()).unwrap().task_ids(),
        &[root_task_id]
    );
    assert_eq!(report.task_ids(), &[root_task_id]);
    assert!(store.queue_entry(root_turn_id).unwrap().is_some());
    assert!(store.load_task_optional(child_task_id).unwrap().is_none());

    repo.write("result.txt", b"imported parent result\n");
    repo.commit_all("imported turn");
    let result_oid: mac_worker::task::BaseOid =
        String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
    assert_ne!(result_oid.to_string(), original_head);

    plant_closed_done_import(&store, root_task_id, root_turn_id, result_oid.clone());
    drop(dag);
    client.advance_pending_dags().unwrap();

    let dag = store.load_run_dag(report.run_id()).unwrap().unwrap();
    let child = &dag.nodes["child"];
    assert_eq!(child.bound_oid.as_ref(), Some(&result_oid));
    assert_eq!(child.bound_turn_id, Some(root_turn_id));
    assert_eq!(
        child.pin_ref.as_deref(),
        Some(dag_pin_ref(report.run_id(), "child").as_str())
    );
    assert_eq!(child.state, DagNodeState::Submitted);
    assert_eq!(child.task_id, child_task_id);
    let child_task = store.load_task(child.task_id).unwrap();
    assert_eq!(child_task.meta().base_oid(), &result_oid);
    assert_ne!(child_task.meta().base_oid().to_string(), original_head);
    assert!(store.queue_entry(child_turn_id).unwrap().is_some());
}

const PENDING_ORIGIN: &str = "ssh://127.0.0.1:1/repo.git";

#[test]
fn dag_origin_from_child_uses_local_cache_while_origin_delivery_is_pending() {
    let _cwd_lock = CWD_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let repo = support::GitRepo::init();
    repo.write("src/lib.rs", b"fn main() {}\n");
    repo.commit_all("base");
    assert!(
        repo.git(&["remote", "add", "origin", PENDING_ORIGIN])
            .status
            .success()
    );
    let original_head = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    repo.write(
        "tasks.toml",
        br#"
version = 1
agent = "codex"

[[tasks]]
id = "root"
prompt = "root work"
source = "local"

[[tasks]]
id = "child"
prompt = "child work"
depends_on = ["root"]
base = "from:root"
source = "origin"
publish = ["fetch", "push"]
"#,
    );
    let root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
    let store = ClientStateStore::open(&paths.state).unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    store
        .publish_admission_observation(
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                CandidateSlot::Idle,
                vec![
                    "darwin-arm64".into(),
                    "agent:codex".into(),
                    "origin:127.0.0.1".into(),
                ],
                Some(8 * 1024 * 1024 * 1024),
                64 * 1024 * 1024 * 1024,
                now,
            )
            .unwrap()
            .with_local_binding(
                "mac1".into(),
                "~/.local/bin/worker".into(),
                vec!["darwin-arm64".into()],
                1,
                Some(0),
                now,
            ),
        )
        .unwrap();
    let _cwd = CurrentDirGuard::enter(repo.root());
    let config = dag_test_config();
    let runner = IsolatedDagRunner::new();
    let executor = InlineRunnerExecutor;
    let client = TaskClient::new(&runner, &config, &paths, &store, &executor);
    let report = client
        .batch(
            &repo.root().join("tasks.toml"),
            None,
            None,
            &mut std::io::sink(),
        )
        .unwrap();

    let dag = store
        .load_run_dag(report.run_id())
        .unwrap()
        .expect("dependent batch writes a DAG");
    let root_task_id = dag.nodes["root"].task_id;
    let root_turn_id = dag.nodes["root"].turn_id;
    let child_task_id = dag.nodes["child"].task_id;
    let child_turn_id = dag.nodes["child"].turn_id;
    assert_eq!(dag.nodes["child"].state, DagNodeState::Waiting);
    assert_eq!(dag.nodes["child"].frozen.source, "origin");
    assert!(dag.nodes["child"].bound_oid.is_none());
    drop(dag);

    repo.write("result.txt", b"imported parent result\n");
    repo.commit_all("imported turn");
    let result_oid: mac_worker::task::BaseOid =
        String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
    assert_ne!(result_oid.to_string(), original_head);
    let preflight = GitTransport::new(&runner).preflight_origin(PENDING_ORIGIN, &result_oid);
    assert_eq!(
        preflight.unwrap_err().public_code(),
        "BASE_NOT_ON_ORIGIN",
        "origin must not advertise the unpublished imported parent"
    );

    plant_closed_done_import(&store, root_task_id, root_turn_id, result_oid.clone());
    client.advance_pending_dags().unwrap();

    let dag = store.load_run_dag(report.run_id()).unwrap().unwrap();
    let child = &dag.nodes["child"];
    assert_eq!(child.bound_oid.as_ref(), Some(&result_oid));
    assert_eq!(child.state, DagNodeState::Submitted);
    assert_eq!(child.frozen.source, "origin");
    let child_task = store.load_task(child_task_id).unwrap();
    assert_eq!(child_task.meta().base_oid(), &result_oid);
    match child_task.meta().source() {
        TaskSource::Local { wip, push_target } => {
            assert!(!*wip, "bound from: execution is a committed cache object");
            let target = push_target
                .as_ref()
                .expect("origin push destination is retained");
            assert_eq!(target.url(), PENDING_ORIGIN);
            assert_eq!(target.requirement(), "origin:127.0.0.1");
        }
        other => panic!("bound origin child must use local cache source, got {other:?}"),
    }
    assert_eq!(
        child_task.meta().origin_requirement().as_deref(),
        Some("origin:127.0.0.1")
    );
    assert!(child_task.meta().publish().contains(&PublishMode::Push));
    let queued = store
        .queue_entry(child_turn_id)
        .unwrap()
        .expect("child queued");
    assert!(
        queued
            .requirements()
            .iter()
            .any(|item| item == "origin:127.0.0.1"),
        "frozen origin requirement must remain on the queue row: {:?}",
        queued.requirements()
    );
}

const ROOT_FROM_CHILD: &[u8] = br#"
version = 1
agent = "codex"

[[tasks]]
id = "root"
prompt = "root work"

[[tasks]]
id = "child"
prompt = "child work"
depends_on = ["root"]
base = "from:root"
"#;

#[test]
fn independent_two_task_batch_does_not_write_a_dag() {
    let harness = BatchHarness::new(
        br#"
version = 1
agent = "codex"

[[tasks]]
prompt = "first"

[[tasks]]
prompt = "second"
"#,
    );
    let report = harness.batch();
    assert_eq!(report.task_ids().len(), 2);
    assert!(
        harness
            .store
            .load_run_dag(report.run_id())
            .unwrap()
            .is_none()
    );
    assert!(harness.store.list_pending_run_ids().unwrap().is_empty());
}

#[test]
fn dag_fan_out_binds_both_from_children_to_the_same_imported_oid() {
    let harness = BatchHarness::new(
        br#"
version = 1
agent = "codex"

[[tasks]]
id = "root"
prompt = "root work"

[[tasks]]
id = "a"
prompt = "child a"
depends_on = ["root"]
base = "from:root"

[[tasks]]
id = "b"
prompt = "child b"
depends_on = ["root"]
base = "from:root"
"#,
    );
    let report = harness.batch();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    let root_task = dag.nodes["root"].task_id;
    let root_turn = dag.nodes["root"].turn_id;
    drop(dag);
    let result = harness.commit_result(b"fan-out result\n");
    plant_closed_done_import(&harness.store, root_task, root_turn, result.clone());
    harness.client().advance_pending_dags().unwrap();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    assert_eq!(dag.nodes["a"].bound_oid.as_ref(), Some(&result));
    assert_eq!(dag.nodes["b"].bound_oid.as_ref(), Some(&result));
    assert_eq!(dag.nodes["a"].state, DagNodeState::Submitted);
    assert_eq!(dag.nodes["b"].state, DagNodeState::Submitted);
    assert_eq!(
        harness
            .store
            .load_task(dag.nodes["a"].task_id)
            .unwrap()
            .meta()
            .base_oid(),
        &result
    );
    assert_eq!(
        harness
            .store
            .load_task(dag.nodes["b"].task_id)
            .unwrap()
            .meta()
            .base_oid(),
        &result
    );
}

#[test]
fn dag_fan_in_without_from_submits_after_both_parents_close() {
    let harness = BatchHarness::new(
        br#"
version = 1
agent = "codex"

[[tasks]]
id = "left"
prompt = "left work"

[[tasks]]
id = "right"
prompt = "right work"

[[tasks]]
id = "join"
prompt = "join work"
depends_on = ["left", "right"]
"#,
    );
    let frozen_head: mac_worker::task::BaseOid = harness.head().parse().unwrap();
    let report = harness.batch();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    assert_eq!(dag.nodes["join"].state, DagNodeState::Waiting);
    let left = dag.nodes["left"].task_id;
    let left_turn = dag.nodes["left"].turn_id;
    let right = dag.nodes["right"].task_id;
    let right_turn = dag.nodes["right"].turn_id;
    let join_id = dag.nodes["join"].task_id;
    match &dag.nodes["join"].base {
        DagBase::Frozen { oid, .. } => assert_eq!(oid, &frozen_head),
        other => panic!("fan-in join must freeze HEAD, got {other:?}"),
    }
    drop(dag);
    let left_result = harness.commit_result(b"left result\n");
    let right_result = harness.commit_result(b"right result\n");
    plant_closed_done_import(&harness.store, left, left_turn, left_result);
    harness.client().advance_pending_dags().unwrap();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    assert_eq!(dag.nodes["join"].state, DagNodeState::Waiting);
    drop(dag);
    plant_closed_done_import(&harness.store, right, right_turn, right_result);
    harness.client().advance_pending_dags().unwrap();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    assert_eq!(dag.nodes["join"].state, DagNodeState::Submitted);
    let join = harness.store.load_task(join_id).unwrap();
    assert_eq!(join.meta().base_oid(), &frozen_head);
}

#[test]
fn dag_needs_input_waits_then_reconcile_unblocks_after_closed_done() {
    // State-machine plant only: overwrites the same first turn as Closed+Done.
    // It does not create a follow-up via TaskClient::say. The lifecycle case
    // below covers say → new turn → Done → close/accept.
    let harness = BatchHarness::new(ROOT_FROM_CHILD);
    let report = harness.batch();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    let root_task = dag.nodes["root"].task_id;
    let root_turn = dag.nodes["root"].turn_id;
    let child_id = dag.nodes["child"].task_id;
    drop(dag);
    let result = harness.commit_result(b"needs-input later\n");
    plant_parent_status(
        &harness.store,
        root_task,
        root_turn,
        result.clone(),
        TaskState::Open,
        TaskOutcome::NeedsInput,
        TurnTerminal::Succeeded,
    );
    harness.client().advance_pending_dags().unwrap();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    assert_eq!(dag.nodes["child"].state, DagNodeState::Waiting);
    assert!(dag.nodes["child"].bound_oid.is_none());
    assert!(
        harness
            .store
            .load_task_optional(child_id)
            .unwrap()
            .is_none()
    );
    drop(dag);
    plant_closed_done_import(&harness.store, root_task, root_turn, result.clone());
    harness.client().reconcile_runners().unwrap();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    assert_eq!(dag.nodes["child"].bound_oid.as_ref(), Some(&result));
    assert_eq!(dag.nodes["child"].state, DagNodeState::Submitted);
}

#[test]
fn dag_needs_input_say_second_turn_close_binds_followup_oid_not_prior_head() {
    let harness = BatchHarness::new(ROOT_FROM_CHILD);
    let report = harness.batch();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    let root_task = dag.nodes["root"].task_id;
    let root_turn = dag.nodes["root"].turn_id;
    let child_id = dag.nodes["child"].task_id;
    drop(dag);
    let first_oid = harness.commit_result(b"needs-input first turn\n");
    plant_parent_status(
        &harness.store,
        root_task,
        root_turn,
        first_oid.clone(),
        TaskState::Open,
        TaskOutcome::NeedsInput,
        TurnTerminal::Succeeded,
    );
    harness.client().advance_pending_dags().unwrap();
    // Invalidate mutable settings after freeze. say/enqueue_followup, close,
    // and the follow-up TurnRunner must use frozen DAG context.
    harness.repo().write(
        ".worker.toml",
        b"[task]\nsource = \"bogus\"\nmax_followups = 101\n",
    );
    let toml_error = ProjectSettings::load(harness.repo().root(), &[]).unwrap_err();
    assert_eq!(toml_error.public_code(), "TASK_CONFIG_INVALID");
    let first_turn = harness
        .store
        .load_task(root_task)
        .unwrap()
        .status()
        .turns()
        .first()
        .cloned()
        .expect("planted NeedsInput turn");
    let second_oid = harness.commit_result(b"accepted follow-up turn\n");
    assert_ne!(second_oid, first_oid);
    // Bound admission TTL is 2s. Expire it so claim must live-probe instead
    // of ranking the planted Idle cache, which becomes ready=false/busy
    // with empty capabilities when host probe is missing.
    thread::sleep(Duration::from_millis(2_100));
    let worker = FollowupWorker::new(first_turn, first_oid.clone(), second_oid.clone());
    let said = finish_before_watchdog(Duration::from_secs(20), || {
        harness
            .client_with(&worker)
            .say(
                root_task,
                "continue after questions".into(),
                true,
                &mut std::io::sink(),
                &mut std::io::sink(),
            )
            .unwrap()
    });
    let followup_turn = said.status().turns().last().unwrap().turn_id();
    assert_ne!(followup_turn, root_turn);
    assert_eq!(said.status().turns().len(), 2);
    assert_eq!(said.status().state(), TaskState::Open);
    assert_eq!(said.status().last_outcome(), Some(&TaskOutcome::Done));
    assert_eq!(said.exit_code(), Some(0));
    assert_eq!(said.status().head_oid(), Some(&second_oid));
    assert_eq!(
        said.status().turns().last().unwrap().turn_id(),
        followup_turn
    );
    assert!(
        worker.requests().iter().any(|request| request
            .args
            .last()
            .is_some_and(|argument| argument == "~/.local/bin/worker host probe")),
        "stale bound admission must refresh via host probe"
    );
    assert!(
        worker.requests().iter().any(|request| request
            .args
            .last()
            .is_some_and(|argument| argument == HostOperation::TaskSession.command())),
        "follow-up must resume via task-session"
    );
    assert!(
        worker.requests().iter().any(|request| request
            .args
            .last()
            .is_some_and(|argument| argument == HostOperation::TaskTurn.command())),
        "follow-up must submit a real task-turn"
    );
    let journal = std::fs::read_to_string(
        harness
            .paths
            .state
            .join("runners")
            .join(root_task.to_string())
            .join(format!("{followup_turn}.log")),
    )
    .unwrap();
    assert!(journal.contains("\"type\":\"turn_accepted\""), "{journal}");
    assert!(journal.contains("\"type\":\"turn_terminal\""), "{journal}");
    assert!(journal.contains("\"kind\":\"done\""), "{journal}");
    assert!(
        harness
            .store
            .queue_entry_for_task_turn(root_task)
            .unwrap()
            .is_none()
    );
    let parent = harness.store.load_task(root_task).unwrap();
    assert!(parent.runner().is_none());
    assert_eq!(parent.fetched_head(), Some(&second_oid));
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    assert_eq!(dag.nodes["child"].state, DagNodeState::Waiting);
    assert!(dag.nodes["child"].bound_oid.is_none());
    drop(dag);
    harness
        .client_with(&worker)
        .close(root_task, false)
        .unwrap();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    assert_eq!(dag.nodes["child"].bound_oid.as_ref(), Some(&second_oid));
    assert_ne!(dag.nodes["child"].bound_oid.as_ref(), Some(&first_oid));
    assert_eq!(dag.nodes["child"].bound_turn_id, Some(followup_turn));
    assert_eq!(dag.nodes["child"].state, DagNodeState::Submitted);
    drop(dag);
    let child = harness.store.load_task(child_id).unwrap();
    assert_eq!(child.meta().base_oid(), &second_oid);
    assert_ne!(child.meta().base_oid(), &first_oid);
    let parent = harness.store.load_task(root_task).unwrap();
    assert_eq!(parent.fetched_head(), Some(&second_oid));
    assert_eq!(parent.status().state(), TaskState::Closed);
    assert_eq!(parent.status().last_outcome(), Some(&TaskOutcome::Done));
}

fn assert_prepared_resume_conflict(mutate: impl FnOnce(&mut DagNode)) {
    let fixture = prepared_frozen_dag();
    occupy_worker_slot(&fixture);
    advance_dags(&fixture).unwrap();
    let original_prompt = fixture
        .store
        .read_turn_prompt(fixture.task_id, fixture.turn_id)
        .unwrap();
    let mut node = fixture
        .store
        .load_run_dag(fixture.run_id)
        .unwrap()
        .unwrap()
        .nodes
        .remove("root")
        .unwrap();
    mutate(&mut node);
    let config = dag_test_config();
    let executor = InlineRunnerExecutor;
    let error = TaskClient::new(
        &fixture.runner,
        &config,
        &fixture.paths,
        &fixture.store,
        &executor,
    )
    .submit_prepared_dag_node(fixture.run_id, &node, true)
    .unwrap_err();
    assert_eq!(error.public_code(), "TASK_ID_CONFLICT");
    assert_eq!(
        fixture
            .store
            .read_turn_prompt(fixture.task_id, fixture.turn_id)
            .unwrap(),
        original_prompt
    );
}

#[test]
fn dag_prepared_resume_conflicts_when_close_policy_changes() {
    assert_prepared_resume_conflict(|node| {
        node.frozen.close_on = ClosePolicy::Never;
    });
}

#[test]
fn dag_prepared_resume_conflicts_when_title_changes() {
    assert_prepared_resume_conflict(|node| {
        node.frozen.title = Some("hijacked title".into());
    });
}

#[test]
fn dag_prepared_resume_conflicts_when_prompt_changes() {
    assert_prepared_resume_conflict(|node| {
        node.frozen.prompt = "hijacked prompt".into();
    });
}

#[test]
fn dag_prepared_resume_conflicts_when_publish_branch_changes() {
    let fixture = prepared_frozen_dag_mutated(None, |node| {
        node.frozen.publish = vec!["fetch".into(), "push".into()];
        node.frozen.origin_url = Some("ssh://127.0.0.1:1/repo.git".into());
        node.frozen.publish_branch = Some("branch-a".into());
    });
    occupy_worker_slot(&fixture);
    advance_dags(&fixture).unwrap();
    let original_prompt = fixture
        .store
        .read_turn_prompt(fixture.task_id, fixture.turn_id)
        .unwrap();
    let mut node = fixture
        .store
        .load_run_dag(fixture.run_id)
        .unwrap()
        .unwrap()
        .nodes
        .remove("root")
        .unwrap();
    node.frozen.publish_branch = Some("branch-b".into());
    let config = dag_test_config();
    let executor = InlineRunnerExecutor;
    let error = TaskClient::new(
        &fixture.runner,
        &config,
        &fixture.paths,
        &fixture.store,
        &executor,
    )
    .submit_prepared_dag_node(fixture.run_id, &node, true)
    .unwrap_err();
    assert_eq!(error.public_code(), "TASK_ID_CONFLICT");
    assert_eq!(
        fixture
            .store
            .read_turn_prompt(fixture.task_id, fixture.turn_id)
            .unwrap(),
        original_prompt
    );
}

#[test]
fn dag_frozen_detached_head_child_prompt_ignores_later_branch() {
    let harness = BatchHarness::new(ROOT_FROM_CHILD);
    assert!(
        harness
            .repo()
            .git(&["checkout", "--detach", "HEAD"])
            .status
            .success()
    );
    let report = harness.batch();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    let root_task = dag.nodes["root"].task_id;
    let root_turn = dag.nodes["root"].turn_id;
    let child_id = dag.nodes["child"].task_id;
    let child_turn = dag.nodes["child"].turn_id;
    assert_eq!(dag.nodes["child"].frozen.branch.as_deref(), None);
    drop(dag);
    let root_prompt = harness
        .store
        .read_turn_prompt(root_task, root_turn)
        .unwrap();
    assert!(
        root_prompt.contains("Base branch: (detached HEAD)"),
        "{root_prompt}"
    );
    assert!(
        harness
            .repo()
            .git(&["checkout", "-B", "after-freeze"])
            .status
            .success()
    );
    let result = harness.commit_result(b"detached freeze still binds\n");
    plant_closed_done_import(&harness.store, root_task, root_turn, result);
    harness.client().advance_pending_dags().unwrap();
    let prompt = harness
        .store
        .read_turn_prompt(child_id, child_turn)
        .unwrap();
    assert!(prompt.contains("Base branch: (detached HEAD)"), "{prompt}");
    assert!(!prompt.contains("after-freeze"), "{prompt}");
}

#[test]
fn dag_corrupt_pending_bootstrap_rebuilds_index_and_exact_receipt() {
    let harness = BatchHarness::new(ROOT_FROM_CHILD);
    let report = harness.batch();
    let pending_dir = harness.paths.state.join("dag-pending");
    let marker = pending_dir.join(report.run_id().to_string());
    let receipt = pending_dir.join("bootstrap.json");
    assert!(marker.exists());
    std::fs::remove_file(&marker).unwrap();
    std::fs::write(&receipt, b"{\"version\":2}\n").unwrap();
    assert!(harness.store.list_pending_run_ids().unwrap().is_empty());
    harness.client().reconcile_runners().unwrap();
    assert_eq!(
        harness.store.list_pending_run_ids().unwrap(),
        vec![report.run_id()]
    );
    assert_eq!(std::fs::read(&receipt).unwrap(), b"{\"version\":1}\n");
}

#[test]
fn dag_cancelled_parent_blocks_descendants_without_host_prepare() {
    let harness = BatchHarness::new(ROOT_FROM_CHILD);
    let report = harness.batch();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    let root_task = dag.nodes["root"].task_id;
    let root_turn = dag.nodes["root"].turn_id;
    let child_id = dag.nodes["child"].task_id;
    drop(dag);
    let result = harness.commit_result(b"cancelled parent\n");
    plant_parent_status(
        &harness.store,
        root_task,
        root_turn,
        result,
        TaskState::Closed,
        TaskOutcome::Cancelled,
        TurnTerminal::Cancelled,
    );
    harness.client().advance_pending_dags().unwrap();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    assert_eq!(dag.nodes["child"].state, DagNodeState::Blocked);
    assert_eq!(
        dag.nodes["child"].blocked_by.as_deref(),
        Some(DAG_PARENT_FAILED)
    );
    assert!(
        harness
            .store
            .load_task_optional(child_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn dag_failed_parent_blocks_descendants_without_host_prepare() {
    let harness = BatchHarness::new(ROOT_FROM_CHILD);
    let report = harness.batch();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    let root_task = dag.nodes["root"].task_id;
    let root_turn = dag.nodes["root"].turn_id;
    let child_id = dag.nodes["child"].task_id;
    drop(dag);
    let result = harness.commit_result(b"failed parent\n");
    plant_parent_status(
        &harness.store,
        root_task,
        root_turn,
        result,
        TaskState::Closed,
        TaskOutcome::failed("parent failed"),
        TurnTerminal::Failed,
    );
    harness.client().advance_pending_dags().unwrap();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    assert_eq!(dag.nodes["child"].state, DagNodeState::Blocked);
    assert_eq!(
        dag.nodes["child"].blocked_by.as_deref(),
        Some(DAG_PARENT_FAILED)
    );
    assert!(
        harness
            .store
            .load_task_optional(child_id)
            .unwrap()
            .is_none()
    );
    assert!(harness.store.list_pending_run_ids().unwrap().is_empty());
}

#[test]
fn dag_wait_does_not_exit_while_unscheduled_nodes_remain() {
    let harness = BatchHarness::new(ROOT_FROM_CHILD);
    let report = harness.batch();
    let error = harness
        .client()
        .wait(
            WaitSelector::Run(report.run_id()),
            Some(Duration::from_millis(400)),
        )
        .unwrap_err();
    assert_eq!(error.public_code(), "WAIT_TIMEOUT");
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    assert_eq!(dag.nodes["child"].state, DagNodeState::Waiting);
}

#[test]
fn dag_restart_keeps_frozen_prompt_and_binds_imported_oid_not_new_head() {
    let harness = BatchHarness::new(ROOT_FROM_CHILD);
    let original_head = harness.head();
    let report = harness.batch();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    let frozen_prompt = dag.nodes["child"].frozen.prompt.clone();
    let frozen_created = dag.created_at_millis;
    let root_task = dag.nodes["root"].task_id;
    let root_turn = dag.nodes["root"].turn_id;
    let child_id = dag.nodes["child"].task_id;
    drop(dag);
    harness.repo().write(
        "tasks.toml",
        br#"
version = 1
agent = "codex"
[[tasks]]
id = "root"
prompt = "changed root"
[[tasks]]
id = "child"
prompt = "changed child after freeze"
depends_on = ["root"]
base = "from:root"
"#,
    );
    harness
        .repo()
        .write("later.txt", b"new head after freeze\n");
    harness.repo().commit_all("later head");
    let new_head = harness.head();
    assert_ne!(new_head, original_head);
    let result = harness.commit_result(b"accepted import after restart\n");
    assert_ne!(result.to_string(), new_head);
    plant_closed_done_import(&harness.store, root_task, root_turn, result.clone());
    harness.client().advance_pending_dags().unwrap();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    assert_eq!(dag.nodes["child"].frozen.prompt, frozen_prompt);
    assert_eq!(dag.created_at_millis, frozen_created);
    assert_eq!(dag.nodes["child"].bound_oid.as_ref(), Some(&result));
    let child = harness.store.load_task(child_id).unwrap();
    assert_eq!(child.meta().base_oid(), &result);
    assert_ne!(child.meta().base_oid().to_string(), new_head);
    let prompt = harness
        .store
        .read_turn_prompt(child_id, dag.nodes["child"].turn_id)
        .unwrap();
    assert!(prompt.contains("User request:\nchild work"), "{prompt}");
    assert!(!prompt.contains("changed child after freeze"), "{prompt}");
}

#[test]
fn dag_bind_publication_fault_recovers_the_same_bound_identity() {
    let harness = BatchHarness::new(ROOT_FROM_CHILD);
    let report = harness.batch();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    let root_task = dag.nodes["root"].task_id;
    let root_turn = dag.nodes["root"].turn_id;
    let child_id = dag.nodes["child"].task_id;
    let child_turn = dag.nodes["child"].turn_id;
    drop(dag);
    let result = harness.commit_result(b"bind fault\n");
    plant_closed_done_import(&harness.store, root_task, root_turn, result.clone());
    harness
        .store
        .inject_write_failure_once(ClientStateWritePoint::AfterDagBind);
    let error = harness.client().advance_pending_dags().unwrap_err();
    assert!(
        error.to_string().contains("injected local state failure"),
        "{error}"
    );
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    assert_eq!(dag.nodes["child"].bound_oid.as_ref(), Some(&result));
    assert_eq!(dag.nodes["child"].bound_turn_id, Some(root_turn));
    assert_eq!(dag.nodes["child"].state, DagNodeState::Waiting);
    drop(dag);
    harness.client().advance_pending_dags().unwrap();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    assert_eq!(dag.nodes["child"].task_id, child_id);
    assert_eq!(dag.nodes["child"].turn_id, child_turn);
    assert_eq!(dag.nodes["child"].bound_oid.as_ref(), Some(&result));
    assert_eq!(dag.nodes["child"].state, DagNodeState::Submitted);
}

#[test]
fn dag_idle_pending_index_does_not_list_or_read_history_directory() {
    let harness = BatchHarness::new(ROOT_FROM_CHILD);
    let first = harness.batch();
    assert_eq!(
        harness.store.list_pending_run_ids().unwrap(),
        vec![first.run_id()]
    );
    let dag = harness.store.load_run_dag(first.run_id()).unwrap().unwrap();
    let root_task = dag.nodes["root"].task_id;
    let root_turn = dag.nodes["root"].turn_id;
    drop(dag);
    let result = harness.commit_result(b"first dag done\n");
    plant_closed_done_import(&harness.store, root_task, root_turn, result);
    harness.client().advance_pending_dags().unwrap();
    assert!(harness.store.list_pending_run_ids().unwrap().is_empty());

    let dags_dir = harness.paths.state.join("dags");
    let runs_dir = harness.paths.state.join("runs");
    for _ in 0..32 {
        std::fs::write(
            dags_dir.join(format!("{}.json", RunId::generate())),
            b"corrupt-history\n",
        )
        .unwrap();
    }
    std::fs::write(dags_dir.join("poison"), b"unexpected").unwrap();
    std::fs::write(runs_dir.join("poison"), b"unexpected").unwrap();
    assert!(
        harness.store.list_run_dags().is_err(),
        "history listing must still see the dags/ directory"
    );
    assert!(harness.store.list_pending_run_ids().unwrap().is_empty());
    harness.client().advance_pending_dags().unwrap();

    harness.repo().write("tasks.toml", ROOT_FROM_CHILD);
    let second = harness.batch();
    assert_ne!(second.run_id(), first.run_id());
    assert_eq!(
        harness.store.list_pending_run_ids().unwrap(),
        vec![second.run_id()]
    );
    let dag = harness
        .store
        .load_run_dag(second.run_id())
        .unwrap()
        .unwrap();
    let root_task = dag.nodes["root"].task_id;
    let root_turn = dag.nodes["root"].turn_id;
    drop(dag);
    let result = harness.commit_result(b"second dag\n");
    plant_closed_done_import(&harness.store, root_task, root_turn, result);
    harness.client().advance_pending_dags().unwrap();
    assert!(harness.store.list_pending_run_ids().unwrap().is_empty());
}

/// Bound is run candidates, not nodes inside a graph. A persistent failure on
/// the lexicographically earliest pending run must not starve a later healthy
/// run in the same page, and the cursor must still advance.
#[test]
fn pending_dag_page_advances_later_runs_after_an_earlier_failure() {
    let harness = BatchHarness::new(ROOT_FROM_CHILD);
    let first = harness.batch();
    let second = harness.batch();
    let mut ordered = [first.run_id(), second.run_id()];
    ordered.sort_by_key(|id| id.to_string());
    let earlier = ordered[0];
    let later = ordered[1];

    let later_dag = harness.store.load_run_dag(later).unwrap().unwrap();
    let later_root = later_dag.nodes["root"].task_id;
    let later_root_turn = later_dag.nodes["root"].turn_id;
    let later_child = later_dag.nodes["child"].task_id;
    drop(later_dag);

    let result = harness.commit_result(b"later parent done\n");
    plant_closed_done_import(&harness.store, later_root, later_root_turn, result);

    fs::write(
        harness
            .paths
            .state
            .join("dags")
            .join(format!("{earlier}.json")),
        b"corrupt-pending-run\n",
    )
    .unwrap();

    let error = harness
        .client()
        .advance_pending_dags_page(2)
        .expect_err("earliest corrupt run must still surface");
    assert!(
        !error.to_string().is_empty(),
        "page must report the earlier failure: {error}"
    );

    let later_dag = harness.store.load_run_dag(later).unwrap().unwrap();
    assert_eq!(later_dag.nodes["child"].task_id, later_child);
    assert_eq!(
        later_dag.nodes["child"].state,
        DagNodeState::Submitted,
        "later healthy run must progress in the same bounded page"
    );
}

fn fetch_oid_into(cache: &Path, source: &Path, oid: &BaseOid) {
    let spec = format!("{oid}:refs/mac-worker/scratch/{oid}");
    let output = Command::new("/usr/bin/git")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .args(["-C"])
        .arg(cache)
        .args(["fetch", "--quiet", "--no-write-fetch-head"])
        .arg(source)
        .arg(&spec)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "seed fetch failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn seed_result_only_in_controller_cache(
    cache: &Path,
    project_id: &str,
    worktree_id: &str,
    contents: &[u8],
) -> BaseOid {
    let source = support::GitRepo::init();
    source.write("result.txt", contents);
    source.commit_all("imported turn");
    let oid: BaseOid = String::from_utf8(source.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let transfer =
        TransferRepo::open_or_create_controller_cache(cache, project_id, worktree_id).unwrap();
    fetch_oid_into(transfer.path(), &source.root().join(".git"), &oid);
    assert!(
        transfer.has_object(&oid),
        "logical controller-transfer must contain the imported parent OID"
    );
    oid
}

/// Registered controller DAG: parent Closed+Done import lives only in
/// `controller-transfer/<logical>`. The physical hash of the registered
/// checkout is not seeded. Existing Closed+Done / import-OID gate is unchanged.
#[test]
fn registered_controller_dag_binds_parent_oid_from_logical_cache_only() {
    let harness = BatchHarness::new(ROOT_FROM_CHILD);
    let report = harness.batch();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    let project_id = dag.nodes["root"].frozen.project_id.clone();
    let worktree_id = dag.nodes["root"].frozen.worktree_id.clone();
    let root_task = dag.nodes["root"].task_id;
    let root_turn = dag.nodes["root"].turn_id;
    let child_id = dag.nodes["child"].task_id;
    let child_turn = dag.nodes["child"].turn_id;
    assert!(dag.nodes["child"].bound_oid.is_none());
    drop(dag);

    let checkout = support::GitRepo::init();
    checkout.write("owned.txt", b"controller checkout\n");
    checkout.commit_all("checkout base");
    let checkout_root = checkout.root().canonicalize().unwrap();
    ProjectRegistry::open(&harness.paths.controller_state_root())
        .unwrap()
        .register(&project_id, &worktree_id, &checkout_root)
        .unwrap();

    let result_oid = seed_result_only_in_controller_cache(
        &harness.paths.cache,
        &project_id,
        &worktree_id,
        b"imported parent result only in logical cache\n",
    );
    assert_ne!(result_oid.to_string(), harness.head());

    plant_closed_done_import(&harness.store, root_task, root_turn, result_oid.clone());
    harness.client().advance_pending_dags().unwrap();

    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    let child = &dag.nodes["child"];
    assert_eq!(child.bound_oid.as_ref(), Some(&result_oid));
    assert_eq!(child.bound_turn_id, Some(root_turn));
    let pin_ref = dag_pin_ref(report.run_id(), "child");
    assert_eq!(child.pin_ref.as_deref(), Some(pin_ref.as_str()));
    assert_eq!(child.state, DagNodeState::Submitted);
    assert_eq!(child.task_id, child_id);
    let child_task = harness.store.load_task(child.task_id).unwrap();
    assert_eq!(child_task.meta().base_oid(), &result_oid);
    assert!(harness.store.queue_entry(child_turn).unwrap().is_some());

    let logical =
        TransferRepo::open_controller_cache(&harness.paths.cache, &project_id, &worktree_id)
            .unwrap();
    assert!(logical.has_object(&result_oid));
    assert!(logical.has_ref(&pin_ref));
    drop(logical);

    let physical =
        TransferRepo::open_or_create(&harness.paths.cache, &checkout_root.join(".git")).unwrap();
    assert!(
        !physical.has_object(&result_oid),
        "physical transfer cache must not see a result that exists only in controller-transfer"
    );
}

/// Ordinary local DAG still uses the physical hash cache. An object that exists
/// only in `controller-transfer` is not an accepted import.
#[test]
fn unregistered_local_dag_does_not_bind_from_controller_transfer_cache() {
    let harness = BatchHarness::new(ROOT_FROM_CHILD);
    let report = harness.batch();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    let project_id = dag.nodes["root"].frozen.project_id.clone();
    let worktree_id = dag.nodes["root"].frozen.worktree_id.clone();
    let root_task = dag.nodes["root"].task_id;
    let root_turn = dag.nodes["root"].turn_id;
    drop(dag);

    let result_oid = seed_result_only_in_controller_cache(
        &harness.paths.cache,
        &project_id,
        &worktree_id,
        b"logical-only object is not a local DAG import\n",
    );
    plant_closed_done_import(&harness.store, root_task, root_turn, result_oid.clone());
    harness.client().advance_pending_dags().unwrap();

    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    assert!(
        dag.nodes["child"].bound_oid.is_none(),
        "local DAG must not bind an OID that exists only in controller-transfer"
    );
    assert_eq!(dag.nodes["child"].state, DagNodeState::Waiting);
}

const ROOT_FROM_CHILD_CLOSE_ON_DONE: &[u8] = br#"
version = 1
agent = "codex"
close_on = "done"

[[tasks]]
id = "root"
prompt = "root work"

[[tasks]]
id = "child"
prompt = "child work"
depends_on = ["root"]
base = "from:root"
"#;

const ROOT_FROM_CHILD_PUSH: &[u8] = br#"
version = 1
agent = "codex"
close_on = "done"
publish = ["fetch", "push"]

[[tasks]]
id = "root"
prompt = "root work"

[[tasks]]
id = "child"
prompt = "child work"
depends_on = ["root"]
base = "from:root"
"#;

fn run_auto_closed_turn(
    harness: &BatchHarness,
    worker: &FollowupWorker,
    task_id: TaskId,
    turn_id: TurnId,
) -> mac_worker::turn_runner::TurnOutcomeReport {
    finish_before_watchdog(Duration::from_secs(20), || {
        TurnRunner::new(
            worker,
            &harness.config,
            &harness.paths,
            &harness.store,
            &harness.executor,
        )
        .run(task_id, turn_id, Some(&mut std::io::sink()))
        .unwrap()
    })
}

/// A dependent batch without `--wait` must finish itself: parent Done
/// auto-closes, the `from:` child is bound and started, then the child
/// auto-closes. The operator does not call `wait` or `reconcile`.
#[test]
fn dependent_batch_without_wait_auto_closes_and_starts_the_child() {
    let harness = BatchHarness::new(ROOT_FROM_CHILD_CLOSE_ON_DONE);
    let report = harness.batch();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    let root_task = dag.nodes["root"].task_id;
    let root_turn = dag.nodes["root"].turn_id;
    let child_id = dag.nodes["child"].task_id;
    let child_turn = dag.nodes["child"].turn_id;
    assert_eq!(dag.nodes["child"].state, DagNodeState::Waiting);
    drop(dag);

    let parent_oid = harness.commit_result(b"parent auto-close result\n");
    let parent_worker = FollowupWorker::auto_closed(parent_oid.clone());
    let parent = run_auto_closed_turn(&harness, &parent_worker, root_task, root_turn);
    assert_eq!(parent.status().state(), TaskState::Closed);
    assert_eq!(parent.status().last_outcome(), Some(&TaskOutcome::Done));

    let parent_record = harness.store.load_task(root_task).unwrap();
    assert_eq!(parent_record.status().state(), TaskState::Closed);
    assert_eq!(
        parent_record.status().last_outcome(),
        Some(&TaskOutcome::Done)
    );
    assert_eq!(parent_record.fetched_head(), Some(&parent_oid));
    assert!(parent_record.runner().is_none());
    assert!(
        harness
            .store
            .queue_entry_for_task_turn(root_task)
            .unwrap()
            .is_none()
    );

    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    assert_eq!(dag.nodes["child"].state, DagNodeState::Submitted);
    assert_eq!(dag.nodes["child"].bound_oid.as_ref(), Some(&parent_oid));
    assert_eq!(dag.nodes["child"].bound_turn_id, Some(root_turn));
    drop(dag);
    let child = harness.store.load_task(child_id).unwrap();
    assert_eq!(child.meta().base_oid(), &parent_oid);
    assert!(
        harness.store.queue_entry(child_turn).unwrap().is_some(),
        "child must be queued after the parent auto-closes"
    );
    assert!(
        child.runner().is_some(),
        "closing runner must start the child without wait/reconcile"
    );

    let child_oid = harness.commit_result(b"child auto-close result\n");
    let child_worker = FollowupWorker::auto_closed(child_oid.clone());
    let child_outcome = run_auto_closed_turn(&harness, &child_worker, child_id, child_turn);
    assert_eq!(child_outcome.status().state(), TaskState::Closed);
    assert_eq!(
        child_outcome.status().last_outcome(),
        Some(&TaskOutcome::Done)
    );
    let child = harness.store.load_task(child_id).unwrap();
    assert_eq!(child.status().state(), TaskState::Closed);
    assert_eq!(child.status().last_outcome(), Some(&TaskOutcome::Done));
    assert_eq!(child.fetched_head(), Some(&child_oid));
    assert!(
        harness
            .store
            .queue_entry_for_task_turn(child_id)
            .unwrap()
            .is_none()
    );
}

/// A parked root that requires `origin:github.com` must name that stall in
/// `task list`. After the operator adds the capability, one `task reconcile`
/// starts exactly one runner.
#[test]
fn parked_root_missing_origin_capability_shows_code_and_reconcile_starts_it() {
    let harness = BatchHarness::new(ROOT_FROM_CHILD_PUSH);
    assert!(
        harness
            .repo()
            .git(&[
                "remote",
                "add",
                "origin",
                "https://github.com/example/project.git"
            ])
            .status
            .success()
    );
    let report = harness.batch();
    let dag = harness
        .store
        .load_run_dag(report.run_id())
        .unwrap()
        .unwrap();
    let root_task = dag.nodes["root"].task_id;
    let root_turn = dag.nodes["root"].turn_id;
    drop(dag);
    let queued = harness
        .store
        .queue_entry(root_turn)
        .unwrap()
        .expect("root queued");
    assert!(
        queued
            .requirements()
            .iter()
            .any(|item| item == "origin:github.com"),
        "push publish must require origin:github.com: {:?}",
        queued.requirements()
    );
    harness.store.record_runner(root_task, None).unwrap();
    harness.store.park_row(root_turn).unwrap();
    assert!(matches!(
        harness
            .store
            .queue_entry(root_turn)
            .unwrap()
            .unwrap()
            .state(),
        QueueState::Parked
    ));

    let listed = harness.client().list(TaskListFilter::default()).unwrap();
    let row = listed
        .tasks()
        .iter()
        .find(|task| task.task_id == root_task)
        .expect("parked root is listed");
    assert_eq!(
        row.blocking_code.as_deref(),
        Some("CAPABILITY_MISSING:origin:github.com")
    );

    plant_bound_mini1_ready_with(
        &harness.store,
        vec![
            "darwin-arm64".into(),
            "agent:codex".into(),
            "origin:github.com".into(),
        ],
    );
    let config = Config::parse(
        "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\ncapabilities = [\"darwin-arm64\", \"origin:github.com\"]\n",
    )
    .unwrap();
    let client = TaskClient::new(
        &harness.runner,
        &config,
        &harness.paths,
        &harness.store,
        &harness.executor,
    );
    let reconcile = client.operator_reconcile().unwrap();
    assert_eq!(reconcile.started_runners(), 1);
    let entry = harness.store.queue_entry(root_turn).unwrap().unwrap();
    assert!(!matches!(entry.state(), QueueState::Parked));
    assert!(
        harness
            .store
            .load_task(root_task)
            .unwrap()
            .runner()
            .is_some()
    );
}
