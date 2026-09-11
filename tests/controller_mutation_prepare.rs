//! Selected recovery in the mutation prepare adapter (`task.say` /
//! `task.close`): the adapter must run bounded selected recovery after full
//! typed body validation and before freezing the expected snapshot.
//! Fixture is the completed crash residue: Open + Succeeded/Done + worker,
//! dead Dispatching row, accepted/drained completion journal, turn prompt
//! tree. `task.cancel` keeps its existing path. Real client state and real
//! local git; no worker runs, no network.

mod support;

use std::{
    collections::BTreeMap,
    ffi::OsString,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::{Mutex, MutexGuard},
};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy},
    client_state::ClientStateStore,
    config::Config,
    controller::TaskSubmitHandler,
    job::{
        AdmissionObservation, CommandSummary, ProcessIdentity, QueueEntry,
        QueueEntryKind,
    },
    paths::PathLayout,
    process::SystemProcessRunner,
    project_state::ProjectState,
    protocol::PROTOCOL_VERSION,
    scheduler::{CandidateSlot, WorkerPreference},
    supervisor::{
        ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
    },
    task::{
        BaseOid, ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskId, TaskLimits,
        TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnId,
        TurnSummary, TurnTerminal, RunnerIdentity,
    },
};
use serde_json::{Value, json};
use support::GitRepo;
use uuid::Uuid;

const RUNNER: SystemProcessRunner = SystemProcessRunner;

fn task_n(n: u128) -> TaskId {
    TaskId::new(Uuid::from_u128(n))
}

fn turn_n(n: u128) -> TurnId {
    TurnId::new(Uuid::from_u128(n))
}

fn request_hex(n: u128) -> String {
    format!("{:x}", Uuid::from_u128(n).simple())
}

fn base_oid() -> BaseOid {
    "dddddddddddddddddddddddddddddddddddddddd".parse().unwrap()
}

fn dead_owner() -> ProcessIdentity {
    ProcessIdentity::new(424_244, 4_242_447).unwrap()
}

struct DeadOwnerReusedInspector {
    dead_owner: ProcessIdentity,
}

impl ProcessInspector for DeadOwnerReusedInspector {
    fn identity_for_pid(
        &self,
        pid: u32,
    ) -> Result<ProcessIdentity, mac_worker::error::WorkerError> {
        ProcessIdentity::new(pid, u64::from(pid) * 10_000 + 7)
    }

    fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
        if expected == self.dead_owner {
            // Positive death proof without sleeps; 698 has no Exited guard.
            ProcessObservation::Reused
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

static CURRENT_DIR_LOCK: Mutex<()> = Mutex::new(());

struct CurrentDirGuard {
    previous: PathBuf,
    _lock: MutexGuard<'static, ()>,
}

impl CurrentDirGuard {
    fn enter(path: &std::path::Path) -> Self {
        // Poisoning-proof: tests are independent; a prior test failure must
        // not cascade into unrelated tests. Mutual exclusion still holds.
        let lock = CURRENT_DIR_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let previous = std::env::current_dir().unwrap();
        std::env::set_current_dir(path).unwrap();
        Self {
            previous,
            _lock: lock,
        }
    }
}

impl Drop for CurrentDirGuard {
    fn drop(&mut self) {
        std::env::set_current_dir(&self.previous).unwrap();
    }
}

struct Fixture {
    _temp: tempfile::TempDir,
    _repo: GitRepo,
    _guard: CurrentDirGuard,
    paths: PathLayout,
    config: Config,
    project_id: String,
    worktree_id: String,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        for dir in ["home", "state", "cache", "config", "data"] {
            support::create_directory(root.join(dir));
        }
        let home = root.join("home");
        let environment = BTreeMap::from([
            (OsString::from("HOME"), home.as_os_str().to_os_string()),
            (
                OsString::from("XDG_STATE_HOME"),
                root.join("state").into_os_string(),
            ),
            (
                OsString::from("XDG_CACHE_HOME"),
                root.join("cache").into_os_string(),
            ),
            (
                OsString::from("XDG_CONFIG_HOME"),
                root.join("config").into_os_string(),
            ),
            (
                OsString::from("XDG_DATA_HOME"),
                root.join("data").into_os_string(),
            ),
        ]);
        let paths = PathLayout::discover(None, &environment, &home).unwrap();
        let config = Config::parse("version = 1\n").unwrap();
        let repo = GitRepo::init();
        repo.write("src.txt", b"fixture source\n");
        repo.commit_all("fixture");
        let guard = CurrentDirGuard::enter(repo.root());
        let project = ProjectState::load(&RUNNER, repo.root(), &[]).unwrap();
        Self {
            _temp: temp,
            _repo: repo,
            _guard: guard,
            paths,
            config,
            project_id: project.context.project_id.clone(),
            worktree_id: project.context.worktree_id.clone(),
        }
    }

    /// Completed crash residue: Open + Succeeded/Done + worker with a dead
    /// runner retained, dead Dispatching row, accepted/drained completion
    /// journal, turn prompt tree. Returns the inspector store plus the
    /// pre-prepare record snapshot for ordering comparison.
    fn finalized_dead_row(&self, task: u128, turn: u128) -> (ClientStateStore, LocalTaskRecord) {
        let meta = TaskMeta::new(TaskMetaInput {
            task_id: task_n(task),
            run_id: None,
            project_id: self.project_id.clone(),
            worktree_id: self.worktree_id.clone(),
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
            git_identity: GitIdentity::new("Fixture", "fixture@example.test").unwrap(),
            title: None,
            prompt: "mutation recovery".into(),
            created_at_millis: 1_700_000_000_000,
        })
        .unwrap();
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
            vec![TurnSummary::new(
                1,
                turn_n(turn),
                Some(TurnTerminal::Succeeded),
                Some(TaskOutcome::Done),
                Some(true),
                false,
                Some(1),
                Some(2),
            )],
            2,
        )
        .unwrap();
        let record = LocalTaskRecord::new(
            meta,
            status,
            None,
            Some(RunnerIdentity::new(dead_owner())),
            None,
            "c".repeat(64),
            None,
            true,
            None,
        )
        .unwrap();
        let plain = ClientStateStore::open(&self.paths.state).unwrap();
        plain.create_task(record.clone()).unwrap();
        plain
            .write_turn_prompt(task_n(task), turn_n(turn), "fixture prompt")
            .unwrap();
        cache_idle(&plain, 10);
        plain
            .enqueue(
                QueueEntry::new(
                    turn_n(turn),
                    plain.client_id(),
                    self.project_id.clone(),
                    self.worktree_id.clone(),
                    CommandSummary::argv(2).unwrap(),
                    Vec::new(),
                    WorkerPreference::Pinned {
                        worker: "mini-1".into(),
                    },
                    QueueEntryKind::TaskTurn,
                    None,
                    dead_owner(),
                    10,
                )
                .unwrap(),
            )
            .unwrap();
        let claimed = plain
            .claim_next(dead_owner(), &["mini-1".into()], 11)
            .unwrap()
            .expect("waiting turn must become dispatching");
        assert!(
            matches!(claimed.entry().state(), mac_worker::job::QueueState::Dispatching { .. }),
            "fixture row must be dispatching"
        );
        drop(plain.open_runner_log(task_n(task), turn_n(turn)).unwrap());
        let checkpoint = self
            .paths
            .state
            .join("runners")
            .join(task_n(task).to_string())
            .join(format!("{}.checkpoint.json", turn_n(turn)));
        std::fs::write(
            &checkpoint,
            serde_json::to_vec(&json!({
                "version": 1,
                "task_id": task_n(task),
                "turn_id": turn_n(turn),
                "committed": {
                    "offsets": [0, 0],
                    "len": 0,
                    "accepted": true,
                    "completion": { "outcome": { "kind": "done" }, "drained": true },
                },
                "pending": null,
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::set_permissions(
            &checkpoint,
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let store = ClientStateStore::open_with_owner_inspector(
            &self.paths.state,
            DeadOwnerReusedInspector {
                dead_owner: dead_owner(),
            },
        )
        .unwrap();
        (store, record)
    }
}

fn cache_idle(store: &ClientStateStore, now: u64) {
    let observation = AdmissionObservation::new(
        "mini-1".to_owned(),
        true,
        CandidateSlot::Idle,
        Vec::new(),
        Some(8 * 1024 * 1024 * 1024),
        64 * 1024 * 1024 * 1024,
        now,
    )
    .unwrap();
    store
        .admission_observation("mini-1", now, || Ok(observation))
        .unwrap();
}

fn mutation_request(command: &str, request_id: &str, body: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": request_id,
        "command": command,
        "body": body,
    }))
    .unwrap()
}

fn prepare_via_adapter(
    fixture: &Fixture,
    store: &ClientStateStore,
    command: &str,
    request_id: &str,
    body: serde_json::Value,
) -> Result<mac_worker::controller::OperationMeta, mac_worker::error::WorkerError> {
    let payload = mutation_request(command, request_id, body);
    let request = mac_worker::controller::parse_request(&payload).unwrap();
    let handler = TaskSubmitHandler::new(&RUNNER, &fixture.config, &fixture.paths, store);
    mac_worker::controller::ControllerCommandHandler::prepare(&handler, &request)
}

fn task_body(task: u128, extra: serde_json::Value) -> serde_json::Value {
    let mut body = serde_json::Map::new();
    body.insert("task_id".into(), json!(task_n(task).to_string()));
    if let Some(object) = extra.as_object() {
        for (key, value) in object {
            body.insert(key.clone(), value.clone());
        }
    }
    Value::Object(body)
}

fn row_entry(
    store: &ClientStateStore,
    turn: u128,
) -> Option<mac_worker::job::QueueEntry> {
    store.queue_entry(turn_n(turn)).unwrap()
}

fn frozen_expected(command: &str, meta: &mac_worker::controller::OperationMeta) -> Value {
    match command {
        "task.say" => meta.prepared["prepared"]["expected"].clone(),
        _ => meta.prepared["expected"].clone(),
    }
}

#[test]
fn say_prepare_recovers_dead_row_before_freeze() {
    let fixture = Fixture::new();
    let (store, before) = fixture.finalized_dead_row(0x1001, 0x1002);
    assert!(row_entry(&store, 0x1002).is_some());
    let meta = prepare_via_adapter(
        &fixture,
        &store,
        "task.say",
        &request_hex(0x11),
        task_body(0x1001, json!({ "message": "hello" })),
    )
    .expect("say prepare must succeed after selected recovery");
    assert_eq!(meta.task_id.as_deref(), Some(task_n(0x1001).to_string()).as_deref());
    assert!(
        row_entry(&store, 0x1002).is_none(),
        "completed dead row must be retired, not merely adopted"
    );
    let after = store.load_task(task_n(0x1001)).unwrap();
    assert_eq!(
        frozen_expected("task.say", &meta),
        serde_json::to_value(&after).unwrap(),
        "frozen expected must equal the AFTER record"
    );
    assert_ne!(
        frozen_expected("task.say", &meta),
        serde_json::to_value(&before).unwrap(),
        "frozen expected must differ from BEFORE: recovery ran before freeze"
    );
    let expected = &frozen_expected("task.say", &meta);
    assert_eq!(
        expected["status"]["state"].as_str(),
        Some("open"),
        "frozen expected stays Open with the completed turn"
    );
}

#[test]
fn close_prepare_recovers_dead_row_before_freeze() {
    let fixture = Fixture::new();
    let (store, before) = fixture.finalized_dead_row(0x2001, 0x2002);
    let meta = prepare_via_adapter(
        &fixture,
        &store,
        "task.close",
        &request_hex(0x21),
        task_body(0x2001, json!({})),
    )
    .expect("close prepare must succeed after selected recovery");
    assert_eq!(meta.task_id.as_deref(), Some(task_n(0x2001).to_string()).as_deref());
    assert!(
        row_entry(&store, 0x2002).is_none(),
        "completed dead row must be retired, not merely adopted"
    );
    let after = store.load_task(task_n(0x2001)).unwrap();
    assert_eq!(
        frozen_expected("task.close", &meta),
        serde_json::to_value(&after).unwrap(),
        "frozen expected must equal the AFTER record"
    );
    assert_ne!(
        frozen_expected("task.close", &meta),
        serde_json::to_value(&before).unwrap(),
        "frozen expected must differ from BEFORE: recovery ran before freeze"
    );
}

#[test]
fn invalid_mutation_body_rejects_before_recovery() {
    let fixture = Fixture::new();
    let (store, _) = fixture.finalized_dead_row(0x3001, 0x3002);
    let checkpoint = fixture
        .paths
        .state
        .join("runners")
        .join(task_n(0x3001).to_string())
        .join(format!("{}.checkpoint.json", turn_n(0x3002)));
    let checkpoint_before = std::fs::read(&checkpoint).unwrap();
    // Valid task id but otherwise invalid body: full typed validation runs
    // before selected recovery, so nothing may be adopted or retired.
    let error = prepare_via_adapter(
        &fixture,
        &store,
        "task.say",
        &request_hex(0x31),
        task_body(0x3001, json!({ "message": 7 })),
    )
    .expect_err("invalid body must be rejected");
    assert!(
        error.to_string().contains("INVALID_REQUEST"),
        "unexpected error: {error}"
    );
    let row = row_entry(&store, 0x3002).expect("row must be untouched");
    assert_eq!(
        row.owner_opt(),
        Some(&dead_owner()),
        "rejected body must not mutate store state"
    );
    assert_eq!(
        std::fs::read(&checkpoint).unwrap(),
        checkpoint_before,
        "rejected body must not touch the journal"
    );
}

#[test]
fn cancel_prepare_keeps_existing_path_without_recovery() {
    let fixture = Fixture::new();
    let (store, _) = fixture.finalized_dead_row(0x4001, 0x4002);
    prepare_via_adapter(
        &fixture,
        &store,
        "task.cancel",
        &request_hex(0x41),
        task_body(0x4001, json!({})),
    )
    .expect("cancel prepare keeps working");
    let row = row_entry(&store, 0x4002).expect("row must be untouched");
    assert_eq!(
        row.owner_opt(),
        Some(&dead_owner()),
        "cancel path must not run selected recovery"
    );
}
