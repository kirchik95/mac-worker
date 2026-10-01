//! Drain exercises the shared runner handoff used by RPC, reconciliation,
//! leader recovery, and a finishing runner's parked-turn replacement.

use std::{
    fs,
    os::{fd::AsRawFd, unix::fs::PermissionsExt},
    sync::atomic::{AtomicUsize, Ordering},
};

use mac_worker::test_support::{
    agents::agent::{AgentKind, PermissionPolicy},
    client_state::{ClientStateStore, scheduler::WorkerPreference},
    controller::{
        ControllerFault, ControllerLeader, ControllerStore, RequestPhase,
        drain::{is_drained, set_drained},
        parse_request,
    },
    core::{config::Config, error::WorkerError, paths::PathLayout, protocol::PROTOCOL_VERSION},
    host::{
        job::{CommandSummary, QueueEntry, QueueEntryKind},
        process::{ProcessRequest, ProcessResult, ProcessRunner},
        supervisor::SystemProcessInspector,
    },
    task::{
        client::TaskClient,
        model::{
            ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, RunnerIdentity, TaskId,
            TaskLimits, TaskMeta, TaskMetaInput, TaskSource, TaskState, TaskStatus, TurnId,
        },
        turn_runner::{RunnerExecutor, RunnerStart, start_runner_with_reservation},
    },
};

struct CountingExecutor(AtomicUsize);

impl CountingExecutor {
    fn new() -> Self {
        Self(AtomicUsize::new(0))
    }

    fn starts(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl RunnerExecutor for CountingExecutor {
    fn start(
        &self,
        _paths: &PathLayout,
        _task_id: TaskId,
        _turn_id: TurnId,
    ) -> Result<RunnerIdentity, WorkerError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(RunnerIdentity::new(
            SystemProcessInspector.identity_for_pid(std::process::id())?,
        ))
    }
}

struct Fixture {
    paths: PathLayout,
    state: ClientStateStore,
    task_id: TaskId,
    turn_id: TurnId,
    _temp: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let paths = PathLayout {
            config: root.join("config.toml"),
            state: root.join("state"),
            cache: root.join("cache"),
            data: root.join("data"),
        };
        let state = ClientStateStore::open(&paths.state).unwrap();
        let task_id = TaskId::generate();
        let turn_id = TurnId::generate();
        let meta = TaskMeta::new(TaskMetaInput {
            task_id,
            run_id: None,
            project_id: "a".repeat(64),
            worktree_id: "b".repeat(64),
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
            base_oid: "c".repeat(40).parse().unwrap(),
            limits: TaskLimits::default(),
            close_policy: ClosePolicy::Never,
            env_profile: None,
            git_identity: GitIdentity::new("Fixture", "fixture@example.test").unwrap(),
            title: None,
            prompt: "fixture task".into(),
            created_at_millis: 1,
        })
        .unwrap();
        let status = TaskStatus::new(
            TaskState::Queued,
            None,
            None,
            false,
            Some(meta.base_oid().clone()),
            None,
            vec![],
            vec![],
            None,
            vec![],
            1,
        )
        .unwrap();
        state
            .create_task(
                LocalTaskRecord::new(
                    meta,
                    status,
                    None,
                    None,
                    None,
                    "d".repeat(64),
                    None,
                    true,
                    None,
                )
                .unwrap(),
            )
            .unwrap();
        state
            .write_turn_prompt(task_id, turn_id, "fixture task")
            .unwrap();
        let owner = SystemProcessInspector
            .identity_for_pid(std::process::id())
            .unwrap();
        state
            .enqueue(
                QueueEntry::new(
                    turn_id,
                    state.client_id(),
                    "a".repeat(64),
                    "b".repeat(64),
                    CommandSummary::argv(2).unwrap(),
                    vec![],
                    WorkerPreference::Automatic,
                    QueueEntryKind::TaskTurn,
                    None,
                    owner,
                    1,
                )
                .unwrap(),
            )
            .unwrap();
        Self {
            paths,
            state,
            task_id,
            turn_id,
            _temp: temp,
        }
    }

    fn launch(
        &self,
        executor: &dyn RunnerExecutor,
        exclude_reserver: bool,
    ) -> Result<RunnerStart, WorkerError> {
        start_runner_with_reservation(
            &self.state,
            executor,
            &self.paths,
            self.task_id,
            self.turn_id,
            4,
            exclude_reserver,
        )
    }

    fn plant_drained(&self) {
        set_drained(&self.paths.controller_state_root(), true).unwrap();
    }
}

#[test]
fn drained_controller_blocks_all_shared_runner_handoffs_before_reservation() {
    // false is RPC/reconcile/leader; true is a completing runner replacing
    // its occupied slot with another parked task.
    for exclude_reserver in [false, true] {
        let fixture = Fixture::new();
        fixture.plant_drained();
        let before = fixture.state.queue_entry(fixture.turn_id).unwrap();
        let executor = CountingExecutor::new();

        assert_eq!(
            fixture.launch(&executor, exclude_reserver).unwrap(),
            RunnerStart::Drained,
            "drain must defer every path through the shared handoff"
        );
        assert_eq!(executor.starts(), 0);
        assert_eq!(fixture.state.queue_entry(fixture.turn_id).unwrap(), before);
        assert!(
            fixture
                .state
                .load_task(fixture.task_id)
                .unwrap()
                .runner()
                .is_none()
        );
    }
}

#[test]
fn reopening_store_does_not_clear_missing_drain_state() {
    let fixture = Fixture::new();
    let root = fixture.paths.controller_state_root();
    set_drained(&root, true).unwrap();
    fs::remove_file(root.join("drain.json")).unwrap();

    assert!(
        ControllerStore::open(&root).is_err(),
        "a missing initialized flag is unknown, not undrained"
    );
    let executor = CountingExecutor::new();
    assert!(fixture.launch(&executor, false).is_err());
    assert_eq!(executor.starts(), 0);
}

#[test]
fn first_controller_open_recovers_an_unpublished_empty_gate() {
    let fixture = Fixture::new();
    let root = fixture.paths.controller_state_root();
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(root.join("drain.lock"), b"").unwrap();
    fs::set_permissions(root.join("drain.lock"), fs::Permissions::from_mode(0o600)).unwrap();

    // A creator can be descheduled or exit after O_EXCL and before taking
    // the first flock. Another controller opener must finish initialization.
    ControllerStore::open(&root).expect("recover a gate never initialized before");
    assert!(!is_drained(&root).unwrap());
    let executor = CountingExecutor::new();
    assert!(matches!(
        fixture.launch(&executor, false).unwrap(),
        RunnerStart::Started(_)
    ));
}

struct NoProcesses;

impl ProcessRunner for NoProcesses {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        panic!("no external command belongs in queued drain recovery: {request:?}");
    }
}

#[test]
fn drained_requests_persist_across_leader_restart_and_recovery_resumes_after_off() {
    let fixture = Fixture::new();
    let root = fixture.paths.controller_state_root();
    let leader = ControllerLeader::acquire(&root).unwrap();
    let store = ControllerStore::open(&root).unwrap();
    set_drained(&root, true).unwrap();
    let request = parse_request(
        &serde_json::to_vec(&serde_json::json!({
            "protocol_version": PROTOCOL_VERSION,
            "request_id": "018f0f4a6b5c7d8e9f00112233445560",
            "command": "checkpoint.submit",
            "body": {"prompt": "accepted during drain"},
        }))
        .unwrap(),
    )
    .unwrap();
    let ack = store.handle(&request, ControllerFault::None).unwrap();
    assert_eq!(ack.status(), "acked");
    drop(store);
    drop(leader);

    let _restarted_leader = ControllerLeader::acquire(&root).unwrap();
    let reopened = ControllerStore::open(&root).unwrap();
    assert!(is_drained(&root).unwrap());
    assert_eq!(
        reopened
            .load(request.request_id())
            .unwrap()
            .unwrap()
            .phase(),
        RequestPhase::Acked
    );
    let config = Config::parse(
        "version = 1\n[[workers]]\nname = \"fixture\"\nssh = \"never-connect\"\nslots = 1\n",
    )
    .unwrap();
    let executor = CountingExecutor::new();
    let client = TaskClient::new(
        &NoProcesses,
        &config,
        &fixture.paths,
        &fixture.state,
        &executor,
    );
    assert_eq!(client.operator_reconcile().unwrap().started_runners(), 0);
    assert_eq!(
        client
            .reconcile_selected(&[fixture.task_id])
            .unwrap()
            .started_runners(),
        0
    );
    assert_eq!(
        client.tick_selected_recovery().unwrap().started_runners(),
        0
    );
    assert_eq!(executor.starts(), 0);
    assert!(
        fixture
            .state
            .queue_entry(fixture.turn_id)
            .unwrap()
            .is_some()
    );

    set_drained(&root, false).unwrap();
    assert!(!is_drained(&root).unwrap());
    assert_eq!(
        client.tick_selected_recovery().unwrap().started_runners(),
        1
    );
    assert_eq!(executor.starts(), 1);
}

#[test]
fn launch_holds_drain_lock_through_executor_but_running_turn_does_not() {
    struct LockCheckingExecutor;

    impl RunnerExecutor for LockCheckingExecutor {
        fn start(
            &self,
            paths: &PathLayout,
            _task_id: TaskId,
            _turn_id: TurnId,
        ) -> Result<RunnerIdentity, WorkerError> {
            let lock = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(paths.controller_state_root().join("drain.lock"))
                .unwrap();
            assert_eq!(
                unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
                -1,
                "a concurrent drain must not pass an in-flight handoff"
            );
            assert_eq!(
                std::io::Error::last_os_error().kind(),
                std::io::ErrorKind::WouldBlock
            );
            Ok(RunnerIdentity::new(
                SystemProcessInspector.identity_for_pid(std::process::id())?,
            ))
        }
    }

    let fixture = Fixture::new();
    let root = fixture.paths.controller_state_root();
    ControllerStore::open(&root).unwrap();
    assert!(matches!(
        fixture.launch(&LockCheckingExecutor, true).unwrap(),
        RunnerStart::Started(_)
    ));
    let running = fixture.state.load_task(fixture.task_id).unwrap();
    let queue = fixture.state.queue_entry(fixture.turn_id).unwrap();
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.join("drain.lock"))
        .unwrap();
    assert_eq!(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0,
        "a running turn must not keep the handoff permit"
    );
    drop(lock);
    set_drained(&root, true).unwrap();
    assert_eq!(fixture.state.load_task(fixture.task_id).unwrap(), running);
    assert_eq!(fixture.state.queue_entry(fixture.turn_id).unwrap(), queue);
}

#[test]
fn local_runner_and_status_without_controller_state_do_not_create_it() {
    let fixture = Fixture::new();
    let root = fixture.paths.controller_state_root();
    assert!(!root.exists());
    assert!(!is_drained(&root).unwrap());
    let executor = CountingExecutor::new();
    assert!(matches!(
        fixture.launch(&executor, false).unwrap(),
        RunnerStart::Started(_)
    ));
    assert_eq!(executor.starts(), 1);
    assert!(!root.exists());
}

#[test]
fn corrupt_or_unsupported_drain_state_fails_closed() {
    for bytes in [
        b"private planted text".as_slice(),
        br#"{"version":1}"#.as_slice(),
        br#"{"version":2,"drained":false}"#.as_slice(),
    ] {
        let fixture = Fixture::new();
        fixture.plant_drained();
        let root = fixture.paths.controller_state_root();
        fs::write(root.join("drain.json"), bytes).unwrap();
        let before = fixture.state.queue_entry(fixture.turn_id).unwrap();
        let executor = CountingExecutor::new();

        assert!(fixture.launch(&executor, false).is_err());
        assert!(is_drained(&root).is_err());
        assert!(set_drained(&root, false).is_err());
        assert_eq!(executor.starts(), 0);
        assert_eq!(fixture.state.queue_entry(fixture.turn_id).unwrap(), before);
        assert_eq!(fs::read(root.join("drain.json")).unwrap(), bytes);
    }
}
