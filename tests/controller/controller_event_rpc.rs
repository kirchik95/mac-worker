use mac_worker::{
    controller::events::{EventSelector, ReadQuery, TaskAddressQuery, TaskRepairQuery},
    task::TaskId,
};

#[test]
fn all_reads_use_only_the_safe_task_list_selector() {
    let id = TaskId::new(uuid::Uuid::from_u128(1));
    for (selector, op) in [
        (EventSelector::Read(ReadQuery::default()), "read"),
        (
            EventSelector::Tasks(TaskAddressQuery::try_new(vec![id], false, None).unwrap()),
            "tasks",
        ),
        (EventSelector::Repair(TaskRepairQuery::default()), "repair"),
    ] {
        let body = selector.request_body().unwrap();
        assert_eq!(body.as_object().unwrap().len(), 1);
        assert_eq!(body["controller_events"]["op"], op);
        assert_eq!(EventSelector::from_request_body(&body).unwrap(), selector);
    }
}

use mac_worker::{
    client_state::ClientStateStore,
    config::Config,
    controller::{
        ControllerFault, ControllerStore, FakeControllerExecutor, decode_frame, decode_request,
        encode_json_frame,
        events::{
            EventCursor, EventReadResult, EventRuntime, EventSource, EventSupport, JournalProvider,
            SafeOutcome, Seq, TaskFacts, TaskProjectionProvider,
            client::ControllerEventClient,
            rpc::{is_event_selector, serve_selector_with},
            testing::{
                FakeJournalProvider, FakeTaskProjectionProvider, ManualEventRuntime, MemoryJournal,
                MemoryTaskReader,
            },
        },
    },
    error::WorkerError,
    job::HostControlError,
    paths::PathLayout,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::PROTOCOL_VERSION,
    task::TurnId,
    task_client::TaskClient,
    turn_runner::DetachedRunnerExecutor,
};
use serde_json::{Value, json};
use std::{
    os::unix::process::ExitStatusExt,
    process::ExitStatus,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

fn deadline() -> Duration {
    Duration::from_secs(60)
}
fn task_id(n: u128) -> TaskId {
    TaskId::new(uuid::Uuid::from_u128(n))
}
fn terminal(n: u128, outcome: SafeOutcome, quiescent: bool) -> TaskFacts {
    TaskFacts::test_terminal(
        task_id(n),
        TurnId::new(uuid::Uuid::from_u128(n + 100)),
        outcome,
        quiescent,
    )
}
fn request(selector: &EventSelector) -> mac_worker::controller::ControllerRequest {
    rpc_request("task.list", selector.request_body().unwrap())
}
fn rpc_request(command: &str, body: Value) -> mac_worker::controller::ControllerRequest {
    mac_worker::controller::parse_request(
        &serde_json::to_vec(&json!({
            "protocol_version": PROTOCOL_VERSION,
            "command": command, "request_id": uuid::Uuid::new_v4().simple().to_string(),
            "body": body
        }))
        .unwrap(),
    )
    .unwrap()
}
fn routed_rpc(
    paths: &PathLayout,
    request: &mac_worker::controller::ControllerRequest,
) -> Result<Vec<u8>, WorkerError> {
    let config = Config::parse("version=1\n[controller]\nenabled=true\nssh='controller'\n")?;
    let input = encode_json_frame(&json!({
        "protocol_version": request.protocol_version(),
        "command": request.command(),
        "request_id": request.request_id(),
        "body": request.body(),
    }))?;
    let mut output = Vec::new();
    mac_worker::controller::serve_rpc_with_runtime(
        paths,
        &config,
        &NoRemote,
        &mut std::io::Cursor::new(input),
        &mut output,
        ControllerFault::None,
    )?;
    Ok(output)
}
fn envelope(request: &mac_worker::controller::ControllerRequest, result: Value) -> Value {
    json!({"protocol_version": PROTOCOL_VERSION, "command":request.command(),
        "request_id":request.request_id(),"payload_sha256":request.payload_sha256(),"result":result})
}
struct NoRemote;
impl ProcessRunner for NoRemote {
    fn run(&self, _: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        panic!("read attempted remote work")
    }
}
struct Dispatch {
    paths: PathLayout,
    config: Config,
    old: AtomicBool,
    journal: Arc<dyn JournalProvider>,
    tasks: Arc<dyn TaskProjectionProvider>,
}
impl ProcessRunner for Dispatch {
    fn run(&self, process: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let request = decode_request(process.stdin.as_ref().unwrap())?;
        assert_eq!(request.command(), "task.list");
        let reply = if !self.old.load(Ordering::SeqCst)
            && request.body() == &json!({"controller_health":true})
        {
            let mut status =
                serde_json::to_value(mac_worker::controller::health_read::assess_health(
                    None,
                    mac_worker::supervisor::ProcessObservation::Absent,
                    100,
                ))
                .unwrap();
            status["features"] = json!(["controller.events"]);
            encode_json_frame(&envelope(&request, status))
        } else if !self.old.load(Ordering::SeqCst) && is_event_selector(&request) {
            serve_selector_with(
                &request,
                self.journal.as_ref(),
                self.tasks.as_ref(),
                deadline(),
            )
        } else {
            // Actual N-1 branch order, including its durable fallback. Unknown
            // commands would leave a receipt; task.list rejects the selector first.
            let state = ClientStateStore::open(&self.paths.state)?;
            let client = TaskClient::new(
                &NoRemote,
                &self.config,
                &self.paths,
                &state,
                &DetachedRunnerExecutor,
            );
            if mac_worker::controller::is_read_command(request.command()) {
                mac_worker::controller::read::serve_read_command(&request, &client)
            } else {
                ControllerStore::open(&self.paths.controller_state_root())?
                    .handle_with(&request, &FakeControllerExecutor, ControllerFault::None)
                    .and_then(|ack| encode_json_frame(&ack))
            }
        };
        let (status, stdout) = match reply {
            Ok(frame) => (0, frame),
            Err(error) => {
                // Same versioned protocol error detail as the host CLI.
                let detail = match &error {
                    WorkerError::Protocol(message) => message
                        .split_once(": ")
                        .map(|(_, detail)| detail.to_owned())
                        .unwrap_or_else(|| error.public_message()),
                    _ => error.public_message(),
                };
                (
                    1 << 8,
                    encode_json_frame(
                        &HostControlError::new(error.public_code(), detail).unwrap(),
                    )?,
                )
            }
        };
        Ok(ProcessResult {
            status: ExitStatus::from_raw(status),
            stdout,
            stderr: Vec::new(),
        })
    }
}
struct RpcHarness {
    _root: tempfile::TempDir,
    dispatch: Arc<Dispatch>,
    journal: Arc<FakeJournalProvider>,
    tasks: Arc<FakeTaskProjectionProvider>,
    client: ControllerEventClient,
}
impl RpcHarness {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().canonicalize().unwrap();
        let paths = PathLayout {
            config: path.join("config.toml"),
            state: path.join("state"),
            cache: path.join("cache"),
            data: path.join("data"),
        };
        let config =
            Config::parse("version=1\n[controller]\nenabled=true\nssh='controller'\n").unwrap();
        ControllerStore::open(&paths.controller_state_root()).unwrap();
        let journal = Arc::new(FakeJournalProvider::present(Arc::new(MemoryJournal::new())));
        let tasks = Arc::new(FakeTaskProjectionProvider::new(Arc::new(
            MemoryTaskReader::new(),
        )));
        let dispatch = Arc::new(Dispatch {
            paths,
            config: config.clone(),
            old: AtomicBool::new(false),
            journal: journal.clone(),
            tasks: tasks.clone(),
        });
        let client = ControllerEventClient::new(
            dispatch.clone(),
            config.controller,
            Arc::new(ManualEventRuntime::new()),
        );
        Self {
            _root: root,
            dispatch,
            journal,
            tasks,
            client,
        }
    }
    fn active_receipts(&self) -> u64 {
        ControllerStore::open(&self.dispatch.paths.controller_state_root())
            .unwrap()
            .pending_health(1000)
            .unwrap()
            .active_count
    }
    fn request_rows(&self) -> usize {
        let path = self.dispatch.paths.controller_state_root();
        std::fs::read_dir(path)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.file_name().to_string_lossy().starts_with("req-")
                    && entry.path().extension().is_some_and(|ext| ext == "json")
            })
            .count()
    }
}

#[test]
fn discovery_then_old_dispatch_leaves_no_durable_request_artifacts() {
    for selector in [
        EventSelector::Read(ReadQuery::default()),
        EventSelector::Tasks(TaskAddressQuery::try_new(vec![task_id(1)], false, None).unwrap()),
        EventSelector::Repair(TaskRepairQuery::default()),
    ] {
        let h = RpcHarness::new();
        assert_eq!(
            h.client.discover(deadline()).unwrap(),
            EventSupport::Supported
        );
        h.dispatch.old.store(true, Ordering::SeqCst);
        let error = match selector {
            EventSelector::Read(query) => h.client.read(query, deadline()).unwrap_err(),
            EventSelector::Tasks(query) => h.client.tasks(query, deadline()).unwrap_err(),
            EventSelector::Repair(query) => h.client.repair(query, deadline()).unwrap_err(),
        };
        assert_eq!(error.public_code(), "CONTROLLER_EVENTS_UNSUPPORTED");
        assert_eq!(h.active_receipts(), 0);
        assert_eq!(h.request_rows(), 0);
    }
}

#[test]
fn selector_dispatch_is_lazy_and_preserves_read_envelope() {
    let h = RpcHarness::new();
    let selectors = [
        EventSelector::Read(ReadQuery::default()),
        EventSelector::Tasks(TaskAddressQuery::try_new(vec![task_id(1)], false, None).unwrap()),
        EventSelector::Repair(TaskRepairQuery::default()),
    ];
    for selector in selectors {
        let request = request(&selector);
        let frame = serve_selector_with(&request, h.journal.as_ref(), h.tasks.as_ref(), deadline())
            .unwrap();
        let reply: mac_worker::controller::read::ControllerReadReply<Value> =
            serde_json::from_slice(decode_frame(&frame).unwrap()).unwrap();
        reply.verify_envelope(&request).unwrap();
        assert!(frame.len() < mac_worker::controller::MAX_FRAME_BYTES);
    }
    assert_eq!(h.journal.open_count(), 1);
    assert_eq!(h.tasks.open_count(), 2);
}

#[test]
fn invalid_selectors_are_rejected_before_opening_any_provider() {
    let h = RpcHarness::new();
    for body in [
        json!({"controller_events":{"op":"read"},"full":true}),
        json!({"controller_events":{"op":"tasks","task_ids":[]}}),
        json!({"controller_events":{"op":"read","path":"secret"}}),
    ] {
        let req = mac_worker::controller::parse_request(
            &serde_json::to_vec(&json!({
            "protocol_version":PROTOCOL_VERSION,"command":"task.list",
            "request_id":uuid::Uuid::new_v4().simple().to_string(),"body":body}))
            .unwrap(),
        )
        .unwrap();
        assert!(is_event_selector(&req));
        let error = serve_selector_with(&req, h.journal.as_ref(), h.tasks.as_ref(), deadline())
            .unwrap_err();
        assert_eq!(error.public_code(), "CONTROLLER_EVENTS_INVALID");
    }
    let raw = format!(
        "{{\"protocol_version\":7,\"command\":\"task.list\",\"request_id\":\"{}\",\"body\":{{\"controller_events\":{{\"op\":\"read\",\"op\":\"tasks\"}}}}}}",
        uuid::Uuid::new_v4().simple()
    );
    assert!(mac_worker::controller::parse_request(raw.as_bytes()).is_err());
    assert_eq!(h.journal.open_count(), 0);
    assert_eq!(h.tasks.open_count(), 0);
}

mod state_reads {
    use super::{
        ControllerStore, EventSelector, ReadQuery, TaskAddressQuery, TaskRepairQuery, WorkerError,
        decode_frame, is_event_selector,
    };
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        os::unix::process::ExitStatusExt,
        sync::atomic::{AtomicBool, AtomicU64, Ordering},
    };

    use mac_worker::{
        agent::{AgentKind, PermissionPolicy},
        client_state::ClientStateStore,
        job::ProcessIdentity,
        task::{
            ClosePolicy, GitIdentity, PublishMode, RunnerIdentity, TaskLimits, TaskMeta,
            TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnId, TurnSummary,
            TurnTerminal,
        },
    };

    use mac_worker::{
        controller::events::{OpaqueCursor, rpc::task_reads::*},
        paths::PathLayout,
        task::{LocalTaskRecord, TaskId},
    };
    use std::{sync::Arc, time::Duration};

    #[derive(Default)]
    struct ManualRuntime {
        inner: mac_worker::controller::events::testing::ManualEventRuntime,
        step: AtomicU64,
        cancelled: AtomicBool,
    }
    impl EventRuntime for ManualRuntime {
        fn now(&self) -> Duration {
            let now = self.inner.now();
            self.inner
                .advance(Duration::from_millis(self.step.load(Ordering::SeqCst)));
            now
        }
        fn sleep(&self, duration: Duration) {
            self.inner.sleep(duration);
        }
        fn cancelled(&self) -> bool {
            self.cancelled.load(Ordering::SeqCst) || self.inner.cancelled()
        }
    }

    fn fixture() -> (tempfile::TempDir, PathLayout, Arc<ManualRuntime>) {
        let root = tempfile::tempdir().unwrap();
        let physical = root.path().canonicalize().unwrap();
        let paths = PathLayout {
            config: physical.join("config.toml"),
            state: physical.join("state"),
            cache: physical.join("cache"),
            data: physical.join("data"),
        };
        ClientStateStore::open(&paths.state).unwrap();
        (root, paths, Arc::new(ManualRuntime::default()))
    }

    fn record(number: u128, state: TaskState, outcome: TaskOutcome) -> LocalTaskRecord {
        let meta = TaskMeta::new(TaskMetaInput {
            task_id: id(number),
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
            base_oid: "a".repeat(40).parse().unwrap(),
            limits: TaskLimits::default(),
            close_policy: ClosePolicy::Never,
            env_profile: None,
            git_identity: GitIdentity::new("Fixture", "fixture@example.test").unwrap(),
            title: Some("fixture title".into()),
            prompt: "private fixture prompt".into(),
            created_at_millis: 1,
        })
        .unwrap();
        let status = TaskStatus::new(
            state,
            Some(outcome.clone()),
            Some("mini-1".into()),
            false,
            Some(meta.base_oid().clone()),
            Some("private fixture summary".into()),
            vec![],
            vec![],
            None,
            vec![TurnSummary::new(
                1,
                TurnId::new(uuid::Uuid::from_u128(number + 1_000_000)),
                Some(TurnTerminal::Succeeded),
                Some(outcome),
                Some(true),
                false,
                Some(1),
                Some(2),
            )],
            2,
        )
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

    fn write_record(paths: &PathLayout, record: &LocalTaskRecord) {
        let file = paths
            .state
            .join("tasks")
            .join(format!("{}.json", record.meta().task_id()));
        fs::write(&file, record.canonical_bytes().unwrap()).unwrap();
        fs::set_permissions(file, fs::Permissions::from_mode(0o600)).unwrap();
    }

    fn id(number: u128) -> TaskId {
        TaskId::new(uuid::Uuid::from_u128(number))
    }

    fn legacy_reply<T: serde::de::DeserializeOwned>(
        paths: &PathLayout,
        command: &str,
        body: serde_json::Value,
    ) -> super::legacy_v7::ControllerReadReply<T> {
        let request = super::rpc_request(command, body);
        let frame = super::routed_rpc(paths, &request).unwrap();
        let reply: super::legacy_v7::ControllerReadReply<T> =
            serde_json::from_slice(decode_frame(&frame).unwrap()).unwrap();
        assert_eq!(reply.protocol_version, 7);
        assert_eq!(reply.command, command);
        assert_eq!(reply.request_id, request.request_id());
        assert_eq!(reply.payload_sha256, request.payload_sha256());
        reply
    }

    #[test]
    fn legacy_laptop_decodes_status_list_log_wait_and_drain_from_new_rpc() {
        use super::legacy_v7;
        use base64::Engine;
        use std::io::Write;

        let (_root, paths, _) = fixture();
        let task = record(1, TaskState::Open, TaskOutcome::Done);
        let turn_id = task.status().turns()[0].turn_id();
        write_record(&paths, &task);
        let state = ClientStateStore::open(&paths.state).unwrap();
        let mut log = state.open_runner_log(id(1), turn_id).unwrap();
        log.write_all(b"legacy log\n").unwrap();
        drop(log);
        let checkpoint = paths
            .state
            .join("runners")
            .join(id(1).to_string())
            .join(format!("{turn_id}.checkpoint.json"));
        fs::write(
            &checkpoint,
            serde_json::to_vec(&serde_json::json!({
                "version":1,"task_id":id(1),"turn_id":turn_id,
                "committed":{"offsets":[0,0],"len":11,"accepted":true,
                    "completion":{"outcome":{"kind":"done"},"drained":true}},
                "pending":null
            }))
            .unwrap(),
        )
        .unwrap();
        fs::set_permissions(checkpoint, fs::Permissions::from_mode(0o600)).unwrap();

        let status = legacy_reply::<legacy_v7::ControllerTaskStatusResult>(
            &paths,
            "task.status",
            serde_json::json!({"task_id":id(1)}),
        )
        .result;
        assert_eq!(status.task_id, id(1));
        assert_eq!(status.run_id, None);
        assert_eq!(status.status.state(), TaskState::Open);
        assert_eq!(status.status.last_outcome(), Some(&TaskOutcome::Done));
        assert_eq!(status.runner, None);
        assert_eq!(status.exit_code, None);

        let list = legacy_reply::<legacy_v7::TaskListProjection>(
            &paths,
            "task.list",
            serde_json::json!({"full":true}),
        )
        .result;
        assert_eq!(list.tasks.len(), 1);
        assert_eq!(list.tasks[0].task_id, id(1));
        assert_eq!(list.tasks[0].state, TaskState::Open);
        assert_eq!(list.tasks[0].last_outcome, Some(TaskOutcome::Done));
        assert_eq!(list.progress.total, 1);
        assert_eq!(list.progress.open, 1);
        assert!(list.runs.is_empty());
        assert!(list.dag_nodes.is_empty());

        let log = legacy_reply::<legacy_v7::ControllerTaskLogsResult>(
            &paths,
            "task.logs",
            serde_json::json!({"task_id":id(1),"raw":true,"offset":0,"limit":64}),
        )
        .result;
        assert_eq!(log.task_id, id(1));
        assert_eq!(log.turn_id, turn_id);
        assert_eq!(log.turn_number, 1);
        assert_eq!(log.agent, "codex");
        assert_eq!(log.offset, 0);
        assert_eq!(log.next_offset, 11);
        assert!(log.raw && log.exhausted && log.complete);
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(log.bytes_base64)
                .unwrap(),
            b"legacy log\n"
        );
        assert_eq!(log.failure, None);

        let wait = legacy_reply::<legacy_v7::ControllerWaitPollResult>(
            &paths,
            "task.wait.poll",
            serde_json::json!({"task_id":id(1)}),
        )
        .result;
        assert_eq!(wait.task_ids, vec![id(1)]);
        assert!(wait.quiescent);
        assert_eq!(wait.exit_code, 0);
        for (body, expected) in [
            (serde_json::json!({}), false),
            (serde_json::json!({"drained":true}), true),
            (serde_json::json!({}), true),
            (serde_json::json!({"drained":false}), false),
        ] {
            assert_eq!(
                legacy_reply::<legacy_v7::DrainResult>(&paths, "controller.drain", body)
                    .result
                    .drained,
                expected
            );
        }
        assert_eq!(
            ControllerStore::open(&paths.controller_state_root())
                .unwrap()
                .pending_health(1000)
                .unwrap()
                .active_count,
            0
        );
        assert!(
            fs::read_dir(paths.controller_state_root())
                .unwrap()
                .all(|entry| {
                    !entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with("req-")
                })
        );
        assert!(!paths.controller_state_root().join("events").exists());
    }

    const SHORT_WAIT_DEADLINE: Duration = Duration::from_secs(2);

    struct WaitRpc<'a> {
        paths: &'a PathLayout,
        requests: std::sync::Mutex<Vec<mac_worker::controller::ControllerRequest>>,
    }

    impl<'a> WaitRpc<'a> {
        fn new(paths: &'a PathLayout) -> Self {
            Self {
                paths,
                requests: Default::default(),
            }
        }

        fn only_poll(&self, expected_body: serde_json::Value) {
            let requests = self.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].command(), "task.wait.poll");
            assert_eq!(requests[0].body(), &expected_body);
            assert!(!self.paths.controller_state_root().join("events").exists());
        }
    }

    impl super::ProcessRunner for WaitRpc<'_> {
        fn run(
            &self,
            process: &super::ProcessRequest,
        ) -> Result<super::ProcessResult, WorkerError> {
            let request = mac_worker::controller::decode_request(process.stdin.as_ref().unwrap())?;
            assert_eq!(
                request.command(),
                "task.wait.poll",
                "wait used discovery or events"
            );
            assert!(process.policy.deadline <= SHORT_WAIT_DEADLINE);
            self.requests.lock().unwrap().push(request.clone());
            let (status, stdout) = match super::routed_rpc(self.paths, &request) {
                Ok(frame) => (0, frame),
                Err(error) => (
                    1 << 8,
                    mac_worker::controller::encode_json_frame(
                        &mac_worker::job::HostControlError::new(
                            error.public_code(),
                            error.public_message(),
                        )
                        .unwrap(),
                    )?,
                ),
            };
            Ok(super::ProcessResult {
                status: super::ExitStatus::from_raw(status),
                stdout,
                stderr: Vec::new(),
            })
        }
    }

    fn wait_config() -> mac_worker::config::ControllerConfig {
        mac_worker::config::ControllerConfig {
            enabled: true,
            ssh: "controller".into(),
            ..Default::default()
        }
    }

    #[test]
    fn wait_wiring_only_polls_with_short_deadline_and_eventless_quiescence() {
        use mac_worker::controller::{ControllerWaitSelector, wait_via_controller};

        for (outcome, expected_exit) in [
            (TaskOutcome::Done, 0),
            (TaskOutcome::failed("agent exited 3"), 1),
        ] {
            let (_root, paths, _) = fixture();
            write_record(&paths, &record(1, TaskState::Open, outcome));
            let runner = WaitRpc::new(&paths);
            let report = wait_via_controller(
                &runner,
                &wait_config(),
                ControllerWaitSelector::Task(id(1)),
                Some(SHORT_WAIT_DEADLINE),
            )
            .unwrap();
            assert_eq!(report.task_ids(), &[id(1)]);
            assert_eq!(report.exit_code(), expected_exit);
            runner.only_poll(serde_json::json!({"task_id":id(1)}));
        }
        let (_root, paths, _) = fixture();
        let runner = WaitRpc::new(&paths);
        let error = wait_via_controller(
            &runner,
            &wait_config(),
            ControllerWaitSelector::Task(id(1)),
            Some(Duration::ZERO),
        )
        .unwrap_err();
        assert_eq!(error.public_code(), "WAIT_TIMEOUT");
        assert!(runner.requests.lock().unwrap().is_empty());
    }

    #[test]
    fn wait_wiring_advances_eventless_dag_and_preserves_ids_and_aggregate_exit() {
        use mac_worker::{
            controller::{ControllerWaitSelector, wait_via_controller},
            dag::{
                DAG_PARENT_FAILED, DagBase, DagFrozenSpec, DagNode, DagNodeState, DagRecord,
                dag_pin_ref,
            },
            task::{RunId, RunRecord},
        };
        use std::collections::BTreeMap;

        let (_root, paths, _) = fixture();
        let run_id = RunId::new(uuid::Uuid::from_u128(100));
        let mut value = serde_json::to_value(record(
            1,
            TaskState::Closed,
            TaskOutcome::failed("agent exited 3"),
        ))
        .unwrap();
        value["meta"]["run_id"] = serde_json::json!(run_id);
        let task: LocalTaskRecord = serde_json::from_value(value).unwrap();
        write_record(&paths, &task);
        let frozen: DagFrozenSpec = serde_json::from_value(serde_json::json!({
            "prompt":"frozen work","agent":"codex","source":"local","publish":["fetch"],
            "close_on":"never","wip":false,"project_path":paths.data,
            "project_id":"a".repeat(64),"worktree_id":"b".repeat(64),
            "timeout_millis":1000,"max_followups":3,"permissions":"workspace",
            "requires":[],"include_untracked":[],"include_empty_dirs":[],"allow_sensitive":[],"cli_includes":[]
        })).unwrap();
        let parent = DagNode {
            batch_id: "root".into(),
            task_id: id(1),
            turn_id: task.status().turns()[0].turn_id(),
            depends_on: vec![],
            base: DagBase::Frozen {
                oid: task.meta().base_oid().clone(),
                pin_ref: dag_pin_ref(run_id, "root"),
                wip: false,
            },
            frozen,
            state: DagNodeState::Submitted,
            bound_oid: None,
            bound_turn_id: None,
            pin_ref: None,
            blocked_by: None,
            claimed_by: None,
            claimed_at_millis: None,
        };
        let child = DagNode {
            batch_id: "child".into(),
            task_id: id(2),
            turn_id: TurnId::new(uuid::Uuid::from_u128(1_000_002)),
            depends_on: vec!["root".into()],
            base: DagBase::From {
                parent: "root".into(),
            },
            state: DagNodeState::Waiting,
            ..parent.clone()
        };
        let dag = DagRecord::new(
            run_id,
            BTreeMap::from([("root".into(), parent), ("child".into(), child)]),
            1,
            None,
            1,
        )
        .unwrap();
        let state = ClientStateStore::open(&paths.state).unwrap();
        state
            .create_run_with_dag(
                RunRecord::new(run_id, None, vec![id(1)], 1, 1).unwrap(),
                dag,
            )
            .unwrap();
        let runner = WaitRpc::new(&paths);
        let report = wait_via_controller(
            &runner,
            &wait_config(),
            ControllerWaitSelector::Run(run_id.to_string()),
            Some(SHORT_WAIT_DEADLINE),
        )
        .unwrap();
        assert_eq!(report.task_ids(), &[id(1)]);
        assert_eq!(report.exit_code(), 1);
        let dag = state.load_run_dag(run_id).unwrap().unwrap();
        assert_eq!(dag.nodes["child"].state, DagNodeState::Blocked);
        assert_eq!(
            dag.nodes["child"].blocked_by.as_deref(),
            Some(DAG_PARENT_FAILED)
        );
        assert!(state.load_task_optional(id(2)).unwrap().is_none());
        assert!(state.list_pending_run_ids().unwrap().is_empty());
        runner.only_poll(serde_json::json!({"run":run_id.to_string()}));
    }

    #[test]
    fn wait_wiring_preserves_wait_blocked_without_events_or_discovery() {
        use mac_worker::{
            controller::{ControllerWaitSelector, wait_via_controller},
            job::{
                CommandSpec, QueueEntry, QueueEntryKind, REPLACEMENT_FAILURE_PARK_AFTER,
                ReplacementFailureBudget,
            },
            scheduler::WorkerPreference,
        };

        let (_root, paths, _) = fixture();
        let task = record(1, TaskState::Open, TaskOutcome::Done);
        let turn_id = task.status().turns()[0].turn_id();
        write_record(&paths, &task);
        let state = ClientStateStore::open(&paths.state).unwrap();
        let turn_path = paths
            .state
            .join("turns")
            .join(id(1).to_string())
            .join(turn_id.to_string());
        fs::create_dir_all(&turn_path).unwrap();
        fs::set_permissions(
            turn_path.parent().unwrap(),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        fs::set_permissions(&turn_path, fs::Permissions::from_mode(0o700)).unwrap();
        let owner = ProcessIdentity::new(crate::support::fixture_pid(80_003), 1).unwrap();
        state
            .enqueue(
                QueueEntry::new(
                    turn_id,
                    state.client_id(),
                    "a".repeat(64),
                    "b".repeat(64),
                    CommandSpec::argv(vec!["__worker_task__".into()])
                        .unwrap()
                        .summary()
                        .unwrap(),
                    vec![],
                    WorkerPreference::Pinned {
                        worker: "mini-1".into(),
                    },
                    QueueEntryKind::TaskTurn,
                    None,
                    owner,
                    1,
                )
                .unwrap(),
            )
            .unwrap();
        state.park_row(turn_id).unwrap();
        state
            .set_replacement_failure(
                turn_id,
                Some(
                    ReplacementFailureBudget::new(
                        1,
                        "HOST_IO".into(),
                        REPLACEMENT_FAILURE_PARK_AFTER,
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
        let local_error = super::routed_rpc(
            &paths,
            &super::rpc_request("task.wait.poll", serde_json::json!({"task_id":id(1)})),
        )
        .unwrap_err();
        assert_eq!(local_error.public_code(), "WAIT_BLOCKED");
        assert_eq!(local_error.public_message(), "RUNNER_REPEATED_FAILURE");
        let runner = WaitRpc::new(&paths);
        let error = wait_via_controller(
            &runner,
            &wait_config(),
            ControllerWaitSelector::Task(id(1)),
            Some(SHORT_WAIT_DEADLINE),
        )
        .unwrap_err();
        assert_eq!(error.public_code(), "WAIT_BLOCKED");
        assert_eq!(error.exit_code(), 70);
        assert_eq!(error.public_message(), "task error");
        runner.only_poll(serde_json::json!({"task_id":id(1)}));
    }

    #[test]
    fn rpc_routing_serves_all_selectors_with_the_existing_read_envelope() {
        use mac_worker::controller::{
            ControllerLeader,
            events::{
                EventBatch, JournalReader, NewEvent,
                journal::{ControllerJournal, JournalOptions},
            },
            read::ControllerReadReply,
        };

        let (_root, paths, runtime) = fixture();
        write_record(&paths, &record(1, TaskState::Open, TaskOutcome::Done));
        let leader = ControllerLeader::acquire(&paths.controller_state_root()).unwrap();
        let journal =
            ControllerJournal::initialize_for_leader(&paths, &leader, JournalOptions { runtime })
                .unwrap();
        let after = journal.window(super::deadline()).unwrap().cursor();
        journal
            .append(
                EventBatch::try_new(vec![NewEvent::ControllerDrainChanged { drained: true }])
                    .unwrap(),
                super::deadline(),
            )
            .unwrap();
        for selector in [
            EventSelector::Read(ReadQuery {
                after: Some(after),
                limit: 1,
                wait_ms: 0,
            }),
            EventSelector::Tasks(TaskAddressQuery::try_new(vec![id(1)], false, None).unwrap()),
            EventSelector::Repair(TaskRepairQuery::default()),
        ] {
            let request = super::request(&selector);
            assert_eq!(request.command(), "task.list");
            let frame = super::routed_rpc(&paths, &request).unwrap();
            let reply: ControllerReadReply<serde_json::Value> =
                serde_json::from_slice(decode_frame(&frame).unwrap()).unwrap();
            reply.verify_envelope(&request).unwrap();
            let value = reply.result();
            match selector {
                EventSelector::Read(_) => {
                    assert_eq!(value["type"], "batch");
                    assert_eq!(value["events"][0]["kind"], "controller.drained");
                    assert_eq!(value["next_after"]["seq"], "1");
                }
                EventSelector::Tasks(_) | EventSelector::Repair(_) => {
                    assert_eq!(value["rows"][0]["task_id"], id(1).to_string());
                    assert_eq!(value["rows"][0]["quiescent"], true);
                    assert_eq!(value["baseline_after"], serde_json::Value::Null);
                }
            }
        }
        assert_eq!(
            ControllerStore::open(&paths.controller_state_root())
                .unwrap()
                .pending_health(1000)
                .unwrap()
                .active_count,
            0
        );
        assert!(
            fs::read_dir(paths.controller_state_root())
                .unwrap()
                .all(|entry| {
                    !entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with("req-")
                })
        );
    }

    #[test]
    fn rpc_routing_rejects_selector_conflicts_before_health_or_state_open() {
        let (_root, mut paths, _) = fixture();
        paths.state = paths.state.with_file_name("uninitialized");
        for body in [
            serde_json::json!({"controller_events":{"op":"read"},"controller_health":true}),
            serde_json::json!({"controller_events":{"op":"read"},"full":true}),
            serde_json::json!({"controller_events":{"op":"read"},"run":"run"}),
            serde_json::json!({"controller_events":{"op":"read"},"state":"open"}),
            serde_json::json!({"controller_events":{"op":"read"},"outcome":"done"}),
            serde_json::json!({"controller_events":null}),
            serde_json::json!({"controller_events":{"op":"unknown"}}),
            serde_json::json!({"controller_events":{"op":"read","path":"private"}}),
            serde_json::json!({"controller_events":{"op":"tasks","task_ids":[]}}),
        ] {
            let request = super::rpc_request("task.list", body);
            let error = super::routed_rpc(&paths, &request).unwrap_err();
            assert_eq!(error.public_code(), "CONTROLLER_EVENTS_INVALID");
            assert!(
                !paths.state.exists(),
                "malformed selector bootstrapped state"
            );
        }
    }

    #[test]
    fn rpc_routing_state_selectors_use_minimal_reads_without_journal_access() {
        let (_root, paths, _) = fixture();
        write_record(&paths, &record(1, TaskState::Open, TaskOutcome::NeedsInput));
        fs::write(paths.state.join("runs").join("invalid.json"), b"broken run").unwrap();
        fs::remove_dir_all(paths.state.join("active-tasks")).unwrap();
        let events = paths.controller_state_root().join("events");
        fs::create_dir_all(&events).unwrap();
        for damaged_lock in [false, true] {
            if damaged_lock {
                std::os::unix::fs::symlink("missing", events.join("journal.lock")).unwrap();
            } else {
                fs::remove_dir(&events).unwrap();
                fs::write(&events, b"not a directory").unwrap();
            }
            for selector in [
                EventSelector::Tasks(TaskAddressQuery::try_new(vec![id(1)], false, None).unwrap()),
                EventSelector::Repair(TaskRepairQuery::default()),
            ] {
                let frame = super::routed_rpc(&paths, &super::request(&selector)).unwrap();
                let reply: serde_json::Value =
                    serde_json::from_slice(decode_frame(&frame).unwrap()).unwrap();
                assert_eq!(reply["result"]["rows"][0]["quiescent"], true);
                assert_eq!(reply["result"]["baseline_after"], serde_json::Value::Null);
                assert!(!paths.state.join("active-tasks").exists());
            }
            if !damaged_lock {
                fs::remove_file(&events).unwrap();
                fs::create_dir(&events).unwrap();
            }
        }
    }

    #[test]
    fn rpc_routing_missing_journal_is_unavailable_without_initializing_state() {
        let (_root, mut paths, _) = fixture();
        paths.state = paths.state.with_file_name("uninitialized");
        let error = super::routed_rpc(
            &paths,
            &super::request(&EventSelector::Read(ReadQuery::default())),
        )
        .unwrap_err();
        assert_eq!(error.public_code(), "CONTROLLER_EVENTS_UNAVAILABLE");
        assert!(!paths.state.exists());
    }

    #[test]
    fn rpc_routing_unknown_commands_never_enter_event_reads() {
        let (_root, paths, _) = fixture();
        for command in ["events.read", "events.tasks", "events.repair"] {
            let request = super::rpc_request(
                command,
                EventSelector::Repair(TaskRepairQuery::default())
                    .request_body()
                    .unwrap(),
            );
            assert!(!is_event_selector(&request));
            let error = super::routed_rpc(&paths, &request).unwrap_err();
            assert_eq!(error.public_code(), "CONTROLLER_TRANSPORT");
            assert!(
                matches!(error, WorkerError::Protocol(message) if message.contains("unsupported controller command"))
            );
        }
        assert!(!paths.controller_state_root().join("events").exists());
    }

    #[test]
    fn minimal_existing_open_never_bootstraps_and_avoids_other_domains() {
        let (_root, paths, runtime) = fixture();
        let task = record(1, TaskState::Open, TaskOutcome::Done);
        write_record(&paths, &task);
        fs::write(
            paths.state.join("runs").join("invalid.json"),
            b"broken large run",
        )
        .unwrap();
        fs::remove_dir_all(paths.state.join("active-tasks")).unwrap();
        let reader = TaskEventReadStore::open_existing(&paths, runtime.clone()).unwrap();
        let result = reader
            .addressed_measured(&[id(1), id(2)], false, None, Duration::from_secs(30))
            .unwrap();
        assert_eq!(result.value.rows.len(), 1);
        assert_eq!(result.value.missing, vec![id(2)]);
        assert_eq!(result.stats.queue_reads, 1);
        assert_eq!(result.stats.names_calls, 0);
        assert!(!paths.state.join("active-tasks").exists());
        let mut absent = paths;
        absent.state = absent.state.with_file_name("absent");
        assert!(TaskEventReadStore::open_existing(&absent, runtime).is_err());
        assert!(!absent.state.exists());
    }
    #[test]
    fn event_directory_or_lock_damage_still_returns_state() {
        let (_root, paths, runtime) = fixture();
        write_record(&paths, &record(1, TaskState::Open, TaskOutcome::NeedsInput));
        let events = paths.controller_state_root().join("events");
        fs::create_dir_all(&events).unwrap();
        std::os::unix::fs::symlink("missing", events.join("journal.lock")).unwrap();
        let reader = TaskEventReadStore::open_existing(&paths, runtime.clone()).unwrap();
        assert_eq!(
            reader
                .addressed_measured(&[id(1)], false, None, Duration::from_secs(30))
                .unwrap()
                .value
                .rows[0]
                .quiescent,
            Some(true)
        );
        fs::remove_dir_all(&events).unwrap();
        fs::write(events, b"not a directory").unwrap();
        let reader = TaskEventReadStore::open_existing(&paths, runtime.clone()).unwrap();
        let provider = mac_worker::controller::events::rpc::ExistingTaskProjectionProvider::new(
            paths.clone(),
            runtime,
        );
        let journal = mac_worker::controller::events::testing::FakeJournalProvider::error(
            "unsafe event namespace",
        );
        for selector in [
            mac_worker::controller::events::EventSelector::Tasks(
                mac_worker::controller::events::TaskAddressQuery::try_new(vec![id(1)], false, None)
                    .unwrap(),
            ),
            mac_worker::controller::events::EventSelector::Repair(
                mac_worker::controller::events::TaskRepairQuery::default(),
            ),
        ] {
            let frame = mac_worker::controller::events::rpc::serve_selector_with(
                &super::request(&selector),
                &journal,
                &provider,
                Duration::from_secs(30),
            )
            .unwrap();
            let value: serde_json::Value =
                serde_json::from_slice(mac_worker::controller::decode_frame(&frame).unwrap())
                    .unwrap();
            assert_eq!(value["result"]["rows"][0]["quiescent"], true);
        }
        assert_eq!(journal.open_count(), 0);
        assert_eq!(
            reader
                .addressed_measured(&[id(1)], false, None, Duration::from_secs(30))
                .unwrap()
                .value
                .rows
                .len(),
            1
        );
    }
    #[test]
    fn recorded_dead_runner_is_still_busy_without_liveness_inspection() {
        let task = record(1, TaskState::Open, TaskOutcome::Done)
            .with_runner(Some(RunnerIdentity::new(
                ProcessIdentity::new(2_000_000_001, 1).unwrap(),
            )))
            .unwrap();
        let facts = record_facts(&task, Some(false), false).unwrap();
        assert!(facts.runner_present);
        assert_eq!(facts.busy, Some(true));
        assert_eq!(facts.quiescent, Some(false));
    }
    #[test]
    fn unsafe_root_and_changed_task_binding_fail_closed() {
        let (_root, paths, runtime) = fixture();
        fs::set_permissions(&paths.state, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(TaskEventReadStore::open_existing(&paths, runtime.clone()).is_err());
        fs::set_permissions(&paths.state, fs::Permissions::from_mode(0o700)).unwrap();
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        fs::rename(paths.state.join("tasks"), paths.state.join("old-tasks")).unwrap();
        fs::create_dir(paths.state.join("tasks")).unwrap();
        fs::set_permissions(paths.state.join("tasks"), fs::Permissions::from_mode(0o700)).unwrap();
        assert!(
            reader
                .addressed_measured(&[id(1)], false, None, Duration::from_secs(30))
                .is_err()
        );
    }
    #[test]
    fn large_frozen_registry_key_pages_complete_under_injected_work_budget() {
        let (_root, paths, runtime) = fixture();
        for number in 1..=65 {
            write_record(&paths, &record(number, TaskState::Open, TaskOutcome::Done));
        }
        let reader = TaskEventReadStore::open_existing(&paths, runtime.clone()).unwrap();
        runtime.step.store(5, Ordering::SeqCst);
        let mut cursor = None;
        let mut seen = Vec::new();
        let mut pages = 0;
        loop {
            let page = reader
                .repair_read(
                    cursor.as_ref().map(OpaqueCursor::as_str),
                    128,
                    Duration::from_secs(1000),
                )
                .unwrap();
            pages += 1;
            assert_eq!(page.stats.names_calls, 1);
            assert!(page.stats.record_reads > 0 && page.stats.record_reads < 65);
            assert!(page.stats.association_checks <= 32);
            assert!(page.stats.input_bytes <= 8 * 1024 * 1024);
            assert!(page.value.rows.iter().all(|row| row.title.is_none()));
            seen.extend(page.value.rows.iter().map(|row| row.task_id));
            if page.value.complete {
                assert!(page.value.next.is_none());
                break;
            }
            let next = page.value.next.unwrap();
            assert!(next.as_str().len() <= 2048);
            assert_ne!(cursor.as_ref(), Some(&next));
            use base64::Engine as _;
            let token: serde_json::Value = serde_json::from_slice(
                &base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(next.as_str())
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(
                token["after_task_id"].as_str().unwrap(),
                seen.last().unwrap().to_string()
            );
            cursor = Some(next);
            assert!(pages <= 65, "a frozen admitted registry must make progress");
        }
        assert!(pages > 1);
        assert_eq!(seen, (1..=65).map(id).collect::<Vec<_>>());
    }
    #[test]
    fn removed_cursor_and_insertions_above_and_below_it_converge_by_keys() {
        let (_root, paths, runtime) = fixture();
        for number in [10, 20, 30] {
            write_record(&paths, &record(number, TaskState::Open, TaskOutcome::Done));
        }
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        let first = reader
            .repair_read(None, 2, Duration::from_secs(30))
            .unwrap()
            .value;
        assert_eq!(
            first.rows.iter().map(|row| row.task_id).collect::<Vec<_>>(),
            vec![id(10), id(20)]
        );
        fs::remove_file(paths.state.join("tasks").join(format!("{}.json", id(20)))).unwrap();
        write_record(&paths, &record(5, TaskState::Open, TaskOutcome::Done));
        write_record(&paths, &record(25, TaskState::Open, TaskOutcome::Done));
        let last = reader
            .repair_read(
                first.next.as_ref().map(OpaqueCursor::as_str),
                2,
                Duration::from_secs(30),
            )
            .unwrap()
            .value;
        assert_eq!(
            last.rows.iter().map(|row| row.task_id).collect::<Vec<_>>(),
            vec![id(25), id(30)]
        );
        assert!(last.complete);
        let next_sweep = reader
            .repair_read(None, 64, Duration::from_secs(30))
            .unwrap()
            .value;
        assert_eq!(
            next_sweep
                .rows
                .iter()
                .map(|row| row.task_id)
                .collect::<Vec<_>>(),
            vec![id(5), id(10), id(25), id(30)]
        );
        assert!(next_sweep.complete);
    }
    #[test]
    fn residue_excluded_without_record_or_queue_reads() {
        let (_root, paths, runtime) = fixture();
        let residue = paths
            .state
            .join("tasks")
            .join("replace-00000000-0000-0000-0000-000000000001");
        fs::write(&residue, b"never decode this residue").unwrap();
        fs::write(
            paths.state.join("queue/state.json"),
            b"broken queue is irrelevant to an empty page",
        )
        .unwrap();
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        let page = reader
            .repair_read(None, 64, Duration::from_secs(30))
            .unwrap();
        assert!(page.value.complete);
        assert!(page.value.next.is_none());
        assert!(page.value.rows.is_empty());
        assert_eq!(page.stats.record_reads, 0);
        assert_eq!(page.stats.queue_reads, 0);
        assert_eq!(fs::read(residue).unwrap(), b"never decode this residue");
    }
    #[test]
    fn bad_key_cursor_and_foreign_root_are_rejected() {
        let (_root, paths, runtime) = fixture();
        for number in 1..=2 {
            write_record(&paths, &record(number, TaskState::Open, TaskOutcome::Done));
        }
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        let first = reader
            .repair_read(None, 1, Duration::from_secs(30))
            .unwrap()
            .value;
        for invalid in ["not a cursor".to_owned(), "a".repeat(2049)] {
            assert!(
                reader
                    .repair_read(Some(&invalid), 1, Duration::from_secs(30))
                    .is_err()
            );
        }
        let (_other, other_paths, other_runtime) = fixture();
        let other = TaskEventReadStore::open_existing(&other_paths, other_runtime).unwrap();
        assert!(
            other
                .repair_read(
                    first.next.as_ref().map(OpaqueCursor::as_str),
                    1,
                    Duration::from_secs(30)
                )
                .is_err()
        );
        // Reopening the same physical root is independent of process-owned iterator state.
        let reopened =
            TaskEventReadStore::open_existing(&paths, Arc::new(ManualRuntime::default())).unwrap();
        assert_eq!(
            reopened
                .repair_read(
                    first.next.as_ref().map(OpaqueCursor::as_str),
                    1,
                    Duration::from_secs(30)
                )
                .unwrap()
                .value
                .rows[0]
                .task_id,
            id(2)
        );
    }
    #[test]
    fn cursor_reused_in_new_process() {
        let (_root, paths, runtime) = fixture();
        for number in 1..=2 {
            write_record(&paths, &record(number, TaskState::Open, TaskOutcome::Done));
        }
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        let cursor = reader
            .repair_read(None, 1, Duration::from_secs(30))
            .unwrap()
            .value
            .next
            .unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &crate::support::libtest_name(module_path!(), "resume_cursor_child"),
                "--ignored",
                "--test-threads=1",
            ])
            .env("EV_T4_FIXTURE_STATE", &paths.state)
            .env("EV_T4_FIXTURE_CURSOR", cursor.as_str())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
    }
    #[test]
    #[ignore = "fixture: explicitly re-executed by cursor_reused_in_new_process"]
    fn resume_cursor_child() {
        let state = std::path::PathBuf::from(std::env::var_os("EV_T4_FIXTURE_STATE").unwrap());
        let cursor = std::env::var("EV_T4_FIXTURE_CURSOR").unwrap();
        let paths = PathLayout {
            state,
            config: "unused".into(),
            cache: "unused".into(),
            data: "unused".into(),
        };
        let reader =
            TaskEventReadStore::open_existing(&paths, Arc::new(ManualRuntime::default())).unwrap();
        let page = reader
            .repair_read(Some(cursor.as_str()), 1, Duration::from_secs(30))
            .unwrap()
            .value;
        assert_eq!(page.rows[0].task_id, id(2));
        assert!(page.complete);
    }
    #[test]
    fn maximal_task_record_yields_once_and_next_page_progresses() {
        let (_root, paths, runtime) = fixture();
        write_record(&paths, &near_max_record(1));
        write_record(&paths, &record(2, TaskState::Open, TaskOutcome::Done));
        let reader = TaskEventReadStore::open_existing(&paths, runtime.clone()).unwrap();
        runtime.step.store(30, Ordering::SeqCst);
        let first = reader
            .repair_read(None, 64, Duration::from_secs(1000))
            .unwrap();
        assert_eq!(first.value.rows.len(), 1);
        assert_eq!(first.value.rows[0].task_id, id(1));
        assert_eq!(first.stats.record_reads, 1);
        assert!(!first.value.complete);
        let second = reader
            .repair_read(
                first.value.next.as_ref().map(OpaqueCursor::as_str),
                64,
                Duration::from_secs(1000),
            )
            .unwrap();
        assert_eq!(second.value.rows[0].task_id, id(2));
        assert!(second.value.complete);
    }
    #[test]
    fn repair_input_bytes_and_encoded_frame_are_bounded() {
        let (_root, paths, runtime) = fixture();
        let large = near_max_record(1);
        for number in 1..=12 {
            let mut wire = serde_json::to_value(&large).unwrap();
            wire["meta"]["task_id"] = serde_json::json!(id(number));
            write_record(&paths, &serde_json::from_value(wire).unwrap());
        }
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        let page = reader
            .repair_read(None, 128, Duration::from_secs(30))
            .unwrap();
        assert!(page.stats.record_reads > 0 && page.stats.record_reads < 12);
        assert!(page.stats.input_bytes <= 8 * 1024 * 1024);
        assert!(!page.value.complete);
        let request = mac_worker::controller::parse_request(
            &serde_json::to_vec(&serde_json::json!({
                "protocol_version": 7, "request_id": "00000000000000000000000000000001",
                "command": "task.list", "body": {"controller_events": {"op": "repair"}},
            }))
            .unwrap(),
        )
        .unwrap();
        let frame = mac_worker::controller::encode_json_frame(&super::envelope(
            &request,
            serde_json::to_value(page.value).unwrap(),
        ))
        .unwrap();
        assert!(frame.len() < 1024 * 1024);
    }
    #[test]
    fn real_names_cost_cap_and_independent_addressed_reads() {
        use std::os::unix::fs::OpenOptionsExt;

        let (_root, paths, runtime) = fixture();
        write_record(&paths, &record(1, TaskState::Open, TaskOutcome::Done));
        let tasks = paths.state.join("tasks");
        fs::create_dir(tasks.join(".mac-worker-rooted-fs")).unwrap();
        fs::set_permissions(
            tasks.join(".mac-worker-rooted-fs"),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        let mut entries = 2;
        for requested in [1_000, 10_000, 100_000, 100_001] {
            while entries < requested {
                let name = if entries % 10 == 0 {
                    format!("replace-{}", uuid::Uuid::from_u128(entries as u128))
                } else {
                    format!("{}.json", id(entries as u128))
                };
                fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .mode(0o600)
                    .open(tasks.join(name))
                    .unwrap();
                entries += 1;
            }
            let (page, stats) = reader.repair_measured(None, 1, Duration::from_secs(30));
            assert_eq!(stats.names_calls, 1);
            assert_eq!(stats.directory_entries, requested);
            assert_eq!(
                stats.name_bytes,
                37 * (requested - 2) + 7 * ((requested - 1) / 10) + 58
            );
            if requested <= 100_000 {
                assert_eq!(page.unwrap().rows[0].task_id, id(1));
                assert_eq!(stats.record_reads, 1);
                assert_eq!(stats.queue_reads, 1);
            } else {
                assert!(
                    page.unwrap_err()
                        .to_string()
                        .contains("CONTROLLER_EVENTS_REPAIR_REGISTRY_TOO_LARGE")
                );
                assert_eq!(stats.record_reads, 0);
                assert_eq!(stats.queue_reads, 0);
                assert_eq!(stats.input_bytes, 0);
                assert_eq!(stats.association_checks, 0);
                assert_eq!(
                    reader
                        .addressed_measured(&[id(1)], false, None, Duration::from_secs(30))
                        .unwrap()
                        .value
                        .rows[0]
                        .quiescent,
                    Some(true)
                );
            }
            println!(
                "EV_T4_NAMES entries={} name_bytes={} names_us={} work_us={} records={} task_bytes={} queue_reads={} associations={}",
                requested,
                stats.name_bytes,
                stats.names_elapsed.as_micros(),
                stats.work_elapsed.as_micros(),
                stats.record_reads,
                stats.input_bytes,
                stats.queue_reads,
                stats.association_checks
            );
        }
    }
    #[test]
    fn cancellation_and_expired_deadline_precede_state_binding_io() {
        let (_root, paths, runtime) = fixture();
        let reader = TaskEventReadStore::open_existing(&paths, runtime.clone()).unwrap();
        let (result, stats) = reader.repair_measured(None, 1, Duration::ZERO);
        assert!(result.unwrap_err().to_string().contains("deadline expired"));
        assert_eq!(stats.names_calls, 0);
        fs::rename(paths.state.join("tasks"), paths.state.join("retired-tasks")).unwrap();
        runtime.cancelled.store(true, Ordering::SeqCst);
        let error = reader
            .addressed_measured(&[id(1)], false, None, Duration::from_secs(30))
            .err()
            .unwrap();
        assert_eq!(error.public_code(), "CONTROLLER_EVENTS_CANCELLED");
    }
    fn near_max_record(number: u128) -> LocalTaskRecord {
        let mut wire =
            serde_json::to_value(record(number, TaskState::Open, TaskOutcome::Done)).unwrap();
        let template = wire["status"]["turns"][0].clone();
        let history = (1..=7000)
            .map(|index| {
                let mut turn = template.clone();
                turn["turn_number"] = serde_json::json!(index);
                turn["turn_id"] = serde_json::json!(TurnId::new(uuid::Uuid::from_u128(
                    number + 1_000_000 + index
                )));
                turn
            })
            .collect::<Vec<_>>();
        let mut low = 1;
        let mut high = history.len();
        while low < high {
            let middle = (low + high).div_ceil(2);
            wire["status"]["turns"] = serde_json::json!(&history[..middle]);
            let candidate: LocalTaskRecord = serde_json::from_value(wire.clone()).unwrap();
            if candidate.canonical_bytes().unwrap().len() <= MAX_TASK_RECORD_BYTES {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        wire["status"]["turns"] = serde_json::json!(&history[..low]);
        let candidate: LocalTaskRecord = serde_json::from_value(wire).unwrap();
        assert!(candidate.canonical_bytes().unwrap().len() > MAX_TASK_RECORD_BYTES - 1024);
        candidate
    }
}

mod reconciliation {
    use super::*;
    use mac_worker::controller::events::{
        ChangeCause, EventBatch, EventReconciler, JournalReader, JournalWriter, NewEvent,
        PreviousProjection, ReconcileInput, Reconciliation, RepairProgress, TaskFactsBatch,
        TaskHint, TaskProjectionReader, TaskRepairPage, TurnHint, WireEvent,
        client::TaskReconciler,
        notify::{NotifyOptions, NotifyState, plan_notifications},
        testing::ScriptedEventSource,
    };
    use std::{collections::BTreeMap, sync::Mutex};

    struct Source {
        journal: MemoryJournal,
        tasks: MemoryTaskReader,
        operations: Mutex<Vec<&'static str>>,
        read_hook: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }
    impl Source {
        fn new(runtime: Arc<dyn EventRuntime>) -> Self {
            Self {
                journal: MemoryJournal::with_runtime(runtime.clone()),
                tasks: MemoryTaskReader::with_runtime(runtime),
                operations: Mutex::new(Vec::new()),
                read_hook: Mutex::new(None),
            }
        }
        fn hint(&self, id: u128, terminal: bool) {
            let event = if terminal {
                NewEvent::TurnFinished(TurnHint {
                    task_id: task_id(id),
                    turn_id: TurnId::new(uuid::Uuid::from_u128(id + 100)),
                    run_id: None,
                    outcome: SafeOutcome::Done,
                    code: None,
                })
            } else {
                NewEvent::TaskChanged(TaskHint {
                    task_id: task_id(id),
                    run_id: None,
                    turn_id: None,
                    state: "open".into(),
                    code: None,
                })
            };
            self.journal
                .append(EventBatch::try_new(vec![event]).unwrap(), deadline())
                .unwrap();
        }
    }
    impl EventSource for Source {
        fn discover(&self, _: Duration) -> Result<EventSupport, WorkerError> {
            Ok(EventSupport::Supported)
        }
        fn read(
            &self,
            query: ReadQuery,
            deadline: Duration,
        ) -> Result<EventReadResult, WorkerError> {
            self.operations.lock().unwrap().push("read");
            let result = self.journal.read(query, deadline);
            if let Some(hook) = self.read_hook.lock().unwrap().take() {
                hook();
            }
            result
        }
        fn tasks(
            &self,
            query: TaskAddressQuery,
            deadline: Duration,
        ) -> Result<TaskFactsBatch, WorkerError> {
            self.operations.lock().unwrap().push("tasks");
            self.tasks.addressed(query, deadline)
        }
        fn repair(
            &self,
            query: TaskRepairQuery,
            deadline: Duration,
        ) -> Result<TaskRepairPage, WorkerError> {
            self.operations.lock().unwrap().push("repair");
            self.tasks.repair(query, deadline)
        }
    }
    struct Harness {
        runtime: Arc<ManualEventRuntime>,
        source: Arc<Source>,
        reconciler: TaskReconciler,
    }
    impl Harness {
        fn new(
            previous: PreviousProjection,
            cursor: Option<EventCursor>,
            pending: Vec<TaskId>,
        ) -> Self {
            let runtime = Arc::new(ManualEventRuntime::new());
            let source = Arc::new(Source::new(runtime.clone()));
            let reconciler = TaskReconciler::new(previous, cursor, pending, runtime.clone());
            Self {
                runtime,
                source,
                reconciler,
            }
        }
        fn tick(&mut self, read: Option<EventReadResult>, due: bool) -> Reconciliation {
            let result = self
                .reconciler
                .reconcile(
                    self.source.as_ref(),
                    ReconcileInput {
                        read,
                        repair_due: due,
                        include_titles: true,
                    },
                    deadline(),
                )
                .unwrap();
            result.validate().unwrap();
            result
        }
        fn sweep(&mut self) -> Vec<mac_worker::controller::events::DerivedTaskChange> {
            let mut changes = Vec::new();
            for index in 0..200 {
                let result = self.tick(None, index == 0);
                changes.extend(result.changes);
                if result.repair == RepairProgress::Complete {
                    for _ in 0..16 {
                        changes.extend(self.tick(None, false).changes);
                    }
                    return changes;
                }
            }
            panic!("bounded frozen sweep never completed")
        }
        fn feed(&self, after: EventCursor) -> EventReadResult {
            self.source
                .journal
                .read(
                    ReadQuery {
                        after: Some(after),
                        limit: 256,
                        wait_ms: 0,
                    },
                    deadline(),
                )
                .unwrap()
        }
    }
    fn warm(facts: TaskFacts) -> PreviousProjection {
        PreviousProjection::Present(BTreeMap::from([(facts.task_id, facts)]))
    }

    #[test]
    fn review_cold_attention_reaches_notifier_as_startup_summary() {
        for count in 1..=256 {
            for (saved_cursor, delayed_proof) in [(false, false), (true, false), (true, true)] {
                let mut h = Harness::new(PreviousProjection::Absent, None, vec![]);
                let cursor =
                    saved_cursor.then(|| h.source.journal.window(deadline()).unwrap().cursor());
                h.reconciler = TaskReconciler::new(
                    PreviousProjection::Absent,
                    cursor,
                    vec![],
                    h.runtime.clone(),
                );
                for id in 1..=count {
                    h.source
                        .tasks
                        .insert(terminal(id, SafeOutcome::NeedsInput, true))
                        .unwrap();
                }
                if delayed_proof {
                    h.source.tasks.set_proof_work(task_id(1), 96).unwrap();
                }
                let mut saved = NotifyState::empty();
                saved.consumed_after = cursor;
                let mut notices = Vec::new();
                let mut completed = false;
                for step in 0..40 {
                    // Refresh the installed projection while the initial
                    // addressed proof is still unknown.
                    if delayed_proof && step == 1 {
                        h.runtime.advance(Duration::from_secs(15));
                    }
                    let result = h.tick(None, step == 0);
                    assert!(
                        result.changes.is_empty(),
                        "cold history cannot become a completion"
                    );
                    if let Some(attention) = &result.attention {
                        assert_eq!(attention.count, count as usize);
                        completed = true;
                    }
                    let plan =
                        plan_notifications(&saved, &result, &NotifyOptions::default(), 16_000);
                    saved = plan.next;
                    notices.extend(plan.notices);
                    if completed {
                        break;
                    }
                }
                assert!(
                    completed,
                    "startup proof did not complete for {count} tasks"
                );
                assert_eq!(
                    notices.len(),
                    1,
                    "count={count}, cursor={saved_cursor}, delayed={delayed_proof}"
                );
                assert_eq!(notices[0].title, "Tasks need attention");
                assert_eq!(
                    notices[0].body,
                    if count == 1 {
                        "1 task".into()
                    } else {
                        format!("{count} tasks")
                    }
                );
                // Persisting the summary consumes it across later sweeps.
                for step in 0..24 {
                    let result = h.tick(None, step == 0);
                    let plan =
                        plan_notifications(&saved, &result, &NotifyOptions::default(), 16_001);
                    saved = plan.next;
                    assert!(plan.notices.is_empty());
                }
            }
        }
    }

    #[test]
    fn cold_attention_summary_preserves_simultaneous_warm_completion() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![task_id(2)]);
        h.source
            .tasks
            .insert(terminal(1, SafeOutcome::NeedsInput, true))
            .unwrap();
        h.source
            .tasks
            .insert(terminal(2, SafeOutcome::Done, false))
            .unwrap();
        h.source
            .tasks
            .insert(terminal(3, SafeOutcome::Done, true))
            .unwrap();
        let mut saved = NotifyState::empty();
        let first = h.tick(None, true);
        let plan = plan_notifications(&saved, &first, &NotifyOptions::default(), 1_000);
        assert!(plan.notices.is_empty());
        saved = plan.next;

        h.source
            .tasks
            .insert(terminal(2, SafeOutcome::Done, true))
            .unwrap();
        let warm = h.tick(None, false);
        assert_eq!(
            warm.baseline,
            mac_worker::controller::events::BaselineKind::Warm
        );
        assert_eq!(warm.changes.len(), 1);
        assert_eq!(warm.changes[0].task_id, task_id(2));
        assert_eq!(warm.changes[0].cause, ChangeCause::RepairDifference);
        assert!(
            warm.attention.is_none(),
            "the cold summary needs its own chunk"
        );
        let plan = plan_notifications(&saved, &warm, &NotifyOptions::default(), 1_001);
        assert_eq!(plan.notices.len(), 1);
        assert_eq!(plan.notices[0].title, "Done");
        saved = plan.next;

        let cold = h.tick(None, false);
        assert_eq!(
            cold.baseline,
            mac_worker::controller::events::BaselineKind::Cold
        );
        assert!(cold.changes.is_empty());
        assert_eq!(cold.attention.as_ref().unwrap().count, 1);
        let plan = plan_notifications(&saved, &cold, &NotifyOptions::default(), 1_002);
        assert_eq!(plan.notices.len(), 1);
        assert_eq!(plan.notices[0].title, "Tasks need attention");
    }

    fn expired_cursor_recovery(
        paged: bool,
        delayed: bool,
    ) -> (Harness, NotifyState, EventReadResult) {
        let old = (1..=3)
            .map(|id| (task_id(id), terminal(id, SafeOutcome::Done, false)))
            .collect();
        let mut h = Harness::new(PreviousProjection::Present(old), None, vec![]);
        let cursor = h.source.journal.window(deadline()).unwrap().cursor();
        h.reconciler = TaskReconciler::new(
            h.reconciler.previous_projection().clone(),
            Some(cursor),
            vec![task_id(1), task_id(2), task_id(3)],
            h.runtime.clone(),
        );
        for id in 1..=2 {
            h.source
                .tasks
                .insert(terminal(id, SafeOutcome::Done, true))
                .unwrap();
            if delayed {
                h.source.tasks.set_proof_work(task_id(id), 96).unwrap();
            }
        }
        // A confirmed busy candidate can remain pending after recovery ends.
        h.source
            .tasks
            .insert(terminal(3, SafeOutcome::Done, false))
            .unwrap();
        if paged {
            h.source.tasks.set_page_budget(1);
        }
        for _ in 0..3 {
            h.source.hint(999, false);
        }
        h.source.journal.trim_to(Seq::new(3)).unwrap();
        let read = h.feed(cursor);
        let EventReadResult::SnapshotRequired(control) = &read else {
            panic!("the real journal must require cursor repair")
        };
        assert_eq!(control.reason, "cursor_expired");
        assert_eq!(control.window.journal_id, cursor.journal_id);
        let mut saved = NotifyState::empty();
        saved.consumed_after = Some(cursor);
        saved.last_complete_repair_millis = Some(0);
        (h, saved, read)
    }

    #[test]
    fn review_same_epoch_reset_coalesces_real_reconciler_decisions() {
        for paged in [false, true] {
            let (mut h, mut saved, read) = expired_cursor_recovery(paged, false);
            let mut read = Some(read);
            let mut notices = Vec::new();
            let mut changed = Vec::new();
            let mut complete = false;
            for _ in 0..8 {
                let result = h.tick(read.take(), false);
                changed.extend(result.changes.iter().map(|change| change.task_id));
                let plan = plan_notifications(&saved, &result, &NotifyOptions::default(), 1_000);
                saved = plan.next;
                notices.extend(plan.notices);
                if result.repair == RepairProgress::Complete && !result.repair_needed {
                    assert_eq!(result.pending_ids, vec![task_id(3)]);
                    complete = true;
                    break;
                }
            }
            assert!(complete, "cursor recovery did not settle");
            assert_eq!(changed, vec![task_id(1), task_id(2)]);
            assert_eq!(
                h.source.tasks.repair_requests().len(),
                if paged { 3 } else { 1 }
            );
            assert_eq!(notices.len(), 1, "paged={paged}");
            assert_eq!(notices[0].title, "Tasks finished");
            assert_eq!(notices[0].body, "2 tasks");

            // Once recovery settles, an ordinary new completion is individual.
            let after = h.reconciler.cursor().unwrap();
            h.source
                .tasks
                .insert(terminal(3, SafeOutcome::Done, true))
                .unwrap();
            h.source.hint(3, true);
            let result = h.tick(Some(h.feed(after)), false);
            assert!(!result.repair_needed);
            let plan = plan_notifications(&saved, &result, &NotifyOptions::default(), 1_001);
            assert_eq!(plan.notices.len(), 1);
            assert_eq!(plan.notices[0].title, "Done");
        }
    }

    #[test]
    fn cursor_recovery_coalesces_decisions_after_delayed_proof() {
        let (mut h, mut saved, read) = expired_cursor_recovery(false, true);
        let mut read = Some(read);
        let mut changed = Vec::new();
        let mut summaries = Vec::new();
        let mut complete = false;
        for _ in 0..10 {
            let result = h.tick(read.take(), false);
            let plan = plan_notifications(&saved, &result, &NotifyOptions::default(), 1_000);
            saved = plan.next;
            changed.extend(result.changes.iter().map(|change| change.task_id));
            for notice in plan.notices {
                assert_eq!(
                    notice.title, "Tasks finished",
                    "delayed recovery decision lost its provenance"
                );
                summaries.push(notice);
            }
            if result.repair == RepairProgress::Complete && !result.repair_needed {
                assert_eq!(result.pending_ids, vec![task_id(3)]);
                complete = true;
                assert_eq!(
                    changed.len(),
                    2,
                    "recovery completed before its unknown proof settled"
                );
                break;
            }
        }
        assert!(complete);
        assert_eq!(changed, vec![task_id(1), task_id(2)]);
        assert_eq!(
            summaries.len(),
            2,
            "each bounded decision chunk must coalesce"
        );
    }

    #[test]
    fn cursor_recovery_unknown_completion_keeps_periodic_repair_running() {
        let (mut h, _, read) = expired_cursor_recovery(false, true);
        let first = h.tick(Some(read), false);
        assert!(first.repair_needed);
        assert_eq!(h.source.tasks.repair_requests().len(), 1);

        h.runtime.advance(Duration::from_secs(15));
        let second = h.tick(None, false);
        assert!(second.repair_needed);
        assert_eq!(
            h.source.tasks.repair_requests().len(),
            2,
            "unknown non-attention proof cannot starve periodic repair"
        );
        let mut changed = Vec::new();
        for _ in 0..10 {
            let result = h.tick(None, false);
            changed.extend(result.changes.iter().map(|change| change.task_id));
            if result.repair == RepairProgress::Complete && !result.repair_needed {
                assert_eq!(changed, vec![task_id(1), task_id(2)]);
                return;
            }
        }
        panic!("refreshed recovery did not settle")
    }

    #[test]
    fn lost_hint_warm_busy_to_quiescent_is_derived_change() {
        let mut h = Harness::new(warm(terminal(1, SafeOutcome::Done, false)), None, vec![]);
        h.source
            .tasks
            .insert(terminal(1, SafeOutcome::Done, true))
            .unwrap();
        let changes = h.sweep();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].cause, ChangeCause::RepairDifference);
        assert_eq!(changes[0].previous.as_ref().unwrap().quiescent, Some(false));
        assert_eq!(changes[0].current.as_ref().unwrap().quiescent, Some(true));
        assert!(!format!("{changes:?}").contains("seq:"));
    }
    #[test]
    fn absent_baseline_even_with_valid_cursor_baselines_lost_completion() {
        let mut h = Harness::new(
            PreviousProjection::Absent,
            Some(EventCursor {
                journal_id: uuid::Uuid::from_u128(1),
                seq: Seq::ZERO,
            }),
            vec![],
        );
        h.source
            .tasks
            .insert(terminal(1, SafeOutcome::Done, true))
            .unwrap();
        assert!(h.sweep().is_empty());
        let PreviousProjection::Present(rows) = h.reconciler.previous_projection() else {
            panic!("cold sweep must establish an actual baseline")
        };
        assert_eq!(rows.len(), 1);
    }
    #[test]
    fn present_empty_detects_new_row() {
        let mut h = Harness::new(PreviousProjection::Present(BTreeMap::new()), None, vec![]);
        h.source
            .tasks
            .insert(terminal(1, SafeOutcome::Done, true))
            .unwrap();
        let changes = h.sweep();
        assert_eq!(changes.len(), 1);
        assert!(changes[0].previous.is_none());
    }
    #[test]
    fn unchanged_history_after_ring_eviction_has_no_change() {
        let facts = terminal(1, SafeOutcome::Done, true);
        let mut h = Harness::new(warm(facts.clone()), None, vec![]);
        h.source.tasks.insert(facts).unwrap();
        h.source
            .journal
            .replace_epoch(uuid::Uuid::from_u128(2))
            .unwrap();
        assert!(h.sweep().is_empty());
        assert!(
            matches!(h.reconciler.previous_projection(),PreviousProjection::Present(rows) if rows.len()==1)
        );
    }
    #[test]
    fn replay_hint_after_saved_cursor_is_separate_and_matches_current_turn() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![]);
        let after = h.source.journal.window(deadline()).unwrap().cursor();
        h.reconciler = TaskReconciler::new(
            PreviousProjection::Absent,
            Some(after),
            vec![],
            h.runtime.clone(),
        );
        h.source
            .tasks
            .insert(terminal(1, SafeOutcome::Done, true))
            .unwrap();
        h.source.hint(1, true);
        let read = h.feed(after);
        let result = h.tick(Some(read), false);
        assert_eq!(result.changes.len(), 1);
        assert!(matches!(
            result.changes[0].cause,
            ChangeCause::ReplayTerminal {
                outcome: SafeOutcome::Done,
                ..
            }
        ));
        h.source
            .tasks
            .insert(terminal(2, SafeOutcome::NeedsInput, true))
            .unwrap();
        h.source.hint(2, true);
        let read = h.feed(h.reconciler.cursor().unwrap());
        let result = h.tick(Some(read), false);
        assert_eq!(result.changes.len(), 1);
        assert_eq!(
            result.changes[0].cause,
            ChangeCause::RepairDifference,
            "a stale Done hint must remain separate from a warm confirmed NeedsInput difference"
        );
    }
    #[test]
    fn generic_hint_on_cold_history_is_not_completion() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![]);
        let after = h.source.journal.window(deadline()).unwrap().cursor();
        h.source
            .tasks
            .insert(terminal(1, SafeOutcome::Done, true))
            .unwrap();
        h.source.hint(1, false);
        let read = h.feed(after);
        let result = h.tick(Some(read), false);
        assert!(result.changes.is_empty());
        assert_eq!(result.confirmed.len(), 1);
    }
    #[test]
    fn closing_quiescent_done_is_not_new_completion_and_closed_attention_is_false() {
        let done = terminal(1, SafeOutcome::Done, true);
        let mut closed = done.clone();
        closed.state = "closed".into();
        let mut h = Harness::new(warm(done), None, vec![]);
        h.source.tasks.insert(closed).unwrap();
        assert!(h.sweep().is_empty());
        let mut input = terminal(2, SafeOutcome::NeedsInput, true);
        input.state = "closed".into();
        assert!(!input.eligibility_signature().current_attention);
    }
    #[test]
    fn pending_confirmation_has_priority_titles_and_bounded_proof_groups() {
        let mut h = Harness::new(
            PreviousProjection::Absent,
            None,
            (1..=20).map(task_id).collect(),
        );
        for id in 1..=20 {
            h.source
                .tasks
                .insert(terminal(id, SafeOutcome::Done, true))
                .unwrap();
        }
        h.source.tasks.set_proof_work(task_id(1), 40).unwrap();
        h.source.tasks.set_proof_budget(16);
        let first = h.tick(None, true);
        assert!(!first.pending_ids.is_empty());
        assert_eq!(h.source.operations.lock().unwrap()[0], "tasks");
        for _ in 0..8 {
            h.tick(None, false);
        }
        let queries = h.source.tasks.addressed_requests();
        assert!(queries.len() >= 3);
        assert!(
            queries
                .iter()
                .all(|q| q.task_ids.len() <= 16 && q.include_titles)
        );
        assert!(queries.iter().any(|q| q.proof_after.is_some()));
        assert!(
            h.source
                .tasks
                .repair_requests()
                .iter()
                .all(|q| q.limit <= 128)
        );
    }
    #[test]
    fn partial_sweep_keeps_previous_rows_and_over_cap_keeps_addressed_reads() {
        let mut h = Harness::new(warm(terminal(9, SafeOutcome::Done, true)), None, vec![]);
        h.source
            .tasks
            .insert(terminal(1, SafeOutcome::Done, true))
            .unwrap();
        h.source
            .tasks
            .insert(terminal(2, SafeOutcome::Done, true))
            .unwrap();
        h.source.tasks.set_page_budget(1);
        let first = h.tick(None, true);
        assert_ne!(first.repair, RepairProgress::Complete);
        assert!(
            matches!(h.reconciler.previous_projection(),PreviousProjection::Present(rows) if rows.contains_key(&task_id(9)))
        );
        h.source.tasks.set_directory_entries(100001);
        let error = h
            .reconciler
            .reconcile(
                h.source.as_ref(),
                ReconcileInput {
                    read: None,
                    repair_due: false,
                    include_titles: false,
                },
                deadline(),
            )
            .unwrap_err();
        assert_eq!(
            error.public_code(),
            "CONTROLLER_EVENTS_REPAIR_REGISTRY_TOO_LARGE"
        );
        assert!(
            matches!(h.reconciler.previous_projection(),PreviousProjection::Present(rows) if rows.contains_key(&task_id(9)))
        );
        h.source
            .tasks
            .addressed(
                TaskAddressQuery::try_new(vec![task_id(1)], false, None).unwrap(),
                deadline(),
            )
            .unwrap();
        h.source.tasks.set_directory_entries(2);
        let changes = h.sweep();
        assert!(
            changes
                .iter()
                .any(|change| change.task_id == task_id(9) && change.current.is_none())
        );
    }
    #[test]
    fn continuous_events_do_not_starve_nonoverlapping_repair() {
        let mut h = Harness::new(warm(terminal(1, SafeOutcome::Done, true)), None, vec![]);
        h.source
            .tasks
            .insert(terminal(1, SafeOutcome::Done, true))
            .unwrap();
        h.sweep();
        let old = h.source.tasks.repair_requests().len();
        h.runtime.advance(Duration::from_secs(15));
        let after = h.source.journal.window(deadline()).unwrap().cursor();
        h.source.hint(1, false);
        let read = h.feed(after);
        h.tick(Some(read), false);
        assert!(h.source.tasks.repair_requests().len() > old);
    }
    #[test]
    fn unknown_kind_requests_global_repair_without_trusting_raw_task_id() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![]);
        let id = h.source.journal.window(deadline()).unwrap().journal_id;
        let read = EventReadResult::Batch(mac_worker::controller::events::ReadBatch {
            schema_version: 1,
            journal_id: id,
            oldest_seq: Seq::new(1),
            head_seq: Seq::new(1),
            next_after: EventCursor {
                journal_id: id,
                seq: Seq::new(1),
            },
            events: vec![WireEvent {
                schema_version: 1,
                journal_id: id,
                seq: Seq::new(1),
                time_millis: 0,
                kind: "future.changed".into(),
                data: json!({"task_id":task_id(8)}),
            }],
            has_more: false,
        });
        let result = h.tick(Some(read), false);
        assert!(!h.source.tasks.repair_requests().is_empty());
        assert!(result.changes.is_empty());
        assert!(h.source.tasks.addressed_requests().is_empty());
    }
    #[test]
    fn unknown_outcome_is_non_notifying() {
        let wire = json!({"task_id":task_id(1),"run_id":null,"state":"open","latest_turn_id":TurnId::new(uuid::Uuid::from_u128(101)),"outcome":"future_outcome","code":null,"runner_present":false,"close_intent":false,"auto_continue_intent":false,"queue_dispatching":false,"result_imported":true,"busy":false,"quiescent":true,"fact_digest":"a".repeat(64),"title":null});
        let facts: TaskFacts = serde_json::from_value(wire).unwrap();
        assert_eq!(facts.quiescent, None);
        let mut h = Harness::new(warm(terminal(1, SafeOutcome::Done, false)), None, vec![]);
        h.source.tasks.insert(facts).unwrap();
        assert!(h.sweep().iter().all(|change| {
            change
                .current
                .as_ref()
                .is_none_or(|facts| facts.quiescent != Some(true))
        }));
    }
    #[test]
    fn repeated_restart_preserves_last_complete_view() {
        let runtime = Arc::new(ManualEventRuntime::new());
        let source = ScriptedEventSource::new();
        let baseline = EventCursor {
            journal_id: uuid::Uuid::from_u128(1),
            seq: Seq::ZERO,
        };
        for _ in 0..3 {
            source
                .queue_read(Ok(EventReadResult::SnapshotRequired(
                    mac_worker::controller::events::SnapshotRequired {
                        reason: "bootstrap".into(),
                        window: mac_worker::controller::events::JournalWindow {
                            journal_id: baseline.journal_id,
                            oldest_seq: Seq::new(1),
                            head_seq: Seq::ZERO,
                        },
                    },
                )))
                .unwrap();
            source
                .queue_repair(Ok(TaskRepairPage {
                    rows: vec![],
                    next: None,
                    complete: false,
                    restart: true,
                    baseline_after: Some(baseline),
                }))
                .unwrap();
        }
        let mut reconciler = TaskReconciler::new(
            warm(terminal(9, SafeOutcome::Done, true)),
            None,
            vec![],
            runtime,
        );
        for _ in 0..3 {
            let result = reconciler
                .reconcile(
                    &source,
                    ReconcileInput {
                        read: None,
                        repair_due: true,
                        include_titles: false,
                    },
                    deadline(),
                )
                .unwrap();
            assert_eq!(result.repair, RepairProgress::Restarted);
            assert!(
                matches!(reconciler.previous_projection(),PreviousProjection::Present(rows) if rows.contains_key(&task_id(9)))
            );
        }
    }
    #[test]
    fn repair_never_jumps_to_post_snapshot_head() {
        let mut h = Harness::new(warm(terminal(1, SafeOutcome::Done, false)), None, vec![]);
        h.source
            .tasks
            .insert(terminal(1, SafeOutcome::Done, false))
            .unwrap();
        h.source
            .tasks
            .insert(terminal(2, SafeOutcome::Done, true))
            .unwrap();
        h.source.tasks.set_page_budget(1);
        assert_eq!(h.tick(None, true).repair, RepairProgress::InProgress);
        let baseline = h.source.tasks.repair_requests()[0].baseline_after.unwrap();
        assert_eq!(baseline.seq, Seq::ZERO);
        h.source
            .tasks
            .insert(terminal(1, SafeOutcome::Done, true))
            .unwrap();
        h.source.hint(1, true);
        let source = h.source.clone();
        *h.source.read_hook.lock().unwrap() = Some(Box::new(move || source.hint(2, false)));
        assert_eq!(h.tick(None, false).repair, RepairProgress::InProgress);
        let result = h.tick(None, false);
        assert_eq!(result.repair, RepairProgress::Complete);
        assert_eq!(result.consumed_after.unwrap().seq, Seq::new(1));
        assert_eq!(
            h.source.journal.window(deadline()).unwrap().head_seq,
            Seq::new(2)
        );
        assert!(
            h.source
                .tasks
                .repair_requests()
                .iter()
                .all(|q| q.baseline_after == Some(baseline))
        );
        assert!(
            matches!(h.reconciler.previous_projection(),PreviousProjection::Present(rows) if rows[&task_id(1)].quiescent==Some(true))
        );
    }
    #[test]
    fn replay_expiry_restarts_without_replacing_previous_projection() {
        let mut h = Harness::new(warm(terminal(9, SafeOutcome::Done, true)), None, vec![]);
        for id in 1..=2 {
            h.source
                .tasks
                .insert(terminal(id, SafeOutcome::Done, true))
                .unwrap();
        }
        h.source.tasks.set_page_budget(1);
        h.tick(None, true);
        h.source
            .journal
            .replace_epoch(uuid::Uuid::from_u128(2))
            .unwrap();
        let result = h.tick(None, false);
        assert_eq!(result.repair, RepairProgress::Restarted);
        assert!(
            matches!(h.reconciler.previous_projection(),PreviousProjection::Present(rows) if rows.contains_key(&task_id(9)))
        );
    }
    #[test]
    fn cold_busy_baseline_can_later_derive_a_warm_quiescent_transition() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![task_id(1)]);
        h.source
            .tasks
            .insert(terminal(1, SafeOutcome::Done, false))
            .unwrap();
        let first = h.tick(None, true);
        assert!(first.changes.is_empty());
        assert_eq!(first.repair, RepairProgress::Complete);
        h.source
            .tasks
            .insert(terminal(1, SafeOutcome::Done, true))
            .unwrap();
        let result = h.tick(None, false);
        assert_eq!(result.changes.len(), 1);
        assert_eq!(result.changes[0].cause, ChangeCause::RepairDifference);
        assert_eq!(
            result.changes[0].previous.as_ref().unwrap().busy,
            Some(true)
        );
    }
    #[test]
    fn unaccounted_candidates_do_not_advance_cursor_and_replayed_ids_eventually_fit() {
        let mut h = Harness::new(
            PreviousProjection::Present(BTreeMap::new()),
            None,
            (1..=256).map(task_id).collect(),
        );
        let after = h.source.journal.window(deadline()).unwrap().cursor();
        h.reconciler = TaskReconciler::new(
            PreviousProjection::Present(BTreeMap::new()),
            Some(after),
            (1..=256).map(task_id).collect(),
            h.runtime.clone(),
        );
        for id in 1..=2 {
            h.source
                .tasks
                .insert(terminal(id, SafeOutcome::Done, true))
                .unwrap();
        }
        // Keep the first sweep partial: no independent replay can account for
        // the rejected hint until a later tick has freed candidate capacity.
        h.source.tasks.set_page_budget(1);
        h.source.hint(1000, false);
        let read = h.feed(after);
        let first = h.tick(Some(read.clone()), false);
        assert_eq!(first.consumed_after, Some(after));
        assert!(first.pending_ids.len() <= 256);
        let second = h.tick(Some(read), false);
        assert_eq!(second.consumed_after.unwrap().seq, Seq::new(1));
    }
    #[test]
    fn a_gap_after_consumed_cursor_is_rejected_without_advancing_it() {
        let mut h = Harness::new(PreviousProjection::Present(BTreeMap::new()), None, vec![]);
        let after = h.source.journal.window(deadline()).unwrap().cursor();
        h.reconciler = TaskReconciler::new(
            PreviousProjection::Present(BTreeMap::new()),
            Some(after),
            vec![],
            h.runtime.clone(),
        );
        h.source.hint(1, false);
        h.source.hint(2, false);
        let EventReadResult::Batch(mut batch) = h.feed(after) else {
            panic!("batch expected")
        };
        batch.events.remove(0);
        batch.validate().unwrap();
        let error = h
            .reconciler
            .reconcile(
                h.source.as_ref(),
                ReconcileInput {
                    read: Some(EventReadResult::Batch(batch)),
                    repair_due: false,
                    include_titles: false,
                },
                deadline(),
            )
            .unwrap_err();
        assert_eq!(error.public_code(), "CONTROLLER_EVENTS_UNAVAILABLE");
        assert_eq!(h.reconciler.cursor(), Some(after));
    }
    #[test]
    fn many_current_attention_rows_are_chunked_with_stable_complete_fingerprint() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![]);
        for id in 1..=300 {
            h.source
                .tasks
                .insert(terminal(id, SafeOutcome::NeedsInput, true))
                .unwrap();
        }
        let mut summary = None;
        for i in 0..200 {
            let result = h.tick(None, i == 0);
            assert!(
                result.confirmed.len() <= 256
                    && result.changes.len() <= 256
                    && result.pending_ids.len() <= 256
            );
            if result.attention.is_some() {
                summary = result.attention;
                break;
            }
        }
        let summary = summary.expect("complete attention summary");
        assert_eq!(summary.count, 300);
        h.runtime.advance(Duration::from_secs(15));
        let mut next = None;
        for _ in 0..200 {
            let result = h.tick(None, false);
            if result.attention.is_some() {
                next = result.attention;
                break;
            }
        }
        assert_eq!(next, Some(summary));
    }
    #[test]
    fn bootstrap_batch_never_uses_undelivered_head_as_baseline() {
        let source = ScriptedEventSource::new();
        let id = uuid::Uuid::from_u128(1);
        let cursor = EventCursor {
            journal_id: id,
            seq: Seq::new(10),
        };
        source
            .queue_read(Ok(EventReadResult::Batch(
                mac_worker::controller::events::ReadBatch {
                    schema_version: 1,
                    journal_id: id,
                    oldest_seq: Seq::new(10),
                    head_seq: Seq::new(10),
                    next_after: cursor,
                    events: vec![
                        NewEvent::ControllerDrainChanged { drained: true }
                            .to_wire(id, Seq::new(10), 0)
                            .unwrap(),
                    ],
                    has_more: false,
                },
            )))
            .unwrap();
        source
            .queue_repair(Ok(TaskRepairPage {
                rows: vec![],
                next: None,
                complete: true,
                restart: false,
                baseline_after: Some(cursor),
            }))
            .unwrap();
        let mut reconciler = TaskReconciler::new(
            PreviousProjection::Absent,
            None,
            vec![],
            Arc::new(ManualEventRuntime::new()),
        );
        assert!(
            reconciler
                .reconcile(
                    &source,
                    ReconcileInput {
                        read: None,
                        repair_due: true,
                        include_titles: false
                    },
                    deadline()
                )
                .is_err()
        );
        assert_eq!(
            source
                .requests()
                .iter()
                .filter(|request| request.body["controller_events"]["op"] == "repair")
                .count(),
            0
        );
        assert_eq!(reconciler.cursor(), None);
    }
    #[test]
    fn review_valid_saved_cursor_accounts_retained_terminal_backlog_before_h() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![]);
        let after = h.source.journal.window(deadline()).unwrap().cursor();
        h.reconciler = TaskReconciler::new(
            PreviousProjection::Absent,
            Some(after),
            vec![],
            h.runtime.clone(),
        );
        for _ in 0..4 {
            h.source
                .journal
                .append(
                    EventBatch::try_new(vec![
                        NewEvent::ControllerDrainChanged { drained: true };
                        32
                    ])
                    .unwrap(),
                    deadline(),
                )
                .unwrap();
        }
        h.source
            .tasks
            .insert(terminal(1, SafeOutcome::Done, true))
            .unwrap();
        h.source.hint(1, true);
        let first = h
            .source
            .journal
            .read(
                ReadQuery {
                    after: Some(after),
                    limit: 128,
                    wait_ms: 0,
                },
                deadline(),
            )
            .unwrap();
        assert!(matches!(&first,EventReadResult::Batch(batch) if batch.has_more));
        let mut changes = h.tick(Some(first), false).changes;
        changes.extend(h.sweep());
        assert_eq!(changes.len(), 1);
        assert!(matches!(
            changes[0].cause,
            ChangeCause::ReplayTerminal {
                outcome: SafeOutcome::Done,
                ..
            }
        ));
        assert_eq!(h.reconciler.cursor().unwrap().seq, Seq::new(129));
    }
    #[test]
    fn review_stale_terminal_hint_preserves_confirmed_warm_difference() {
        let mut h = Harness::new(warm(terminal(1, SafeOutcome::Done, false)), None, vec![]);
        let mut current = terminal(1, SafeOutcome::NeedsInput, true);
        current.latest_turn_id = Some(TurnId::new(uuid::Uuid::from_u128(201)));
        h.source.tasks.insert(current).unwrap();
        let after = h.source.journal.window(deadline()).unwrap().cursor();
        h.source.hint(1, true);
        let read = h.feed(after);
        let result = h.tick(Some(read), false);
        assert_eq!(result.changes.len(), 1);
        assert_eq!(result.changes[0].cause, ChangeCause::RepairDifference);
        assert_eq!(
            result.changes[0].current.as_ref().unwrap().outcome,
            Some(SafeOutcome::NeedsInput)
        );
    }
    #[test]
    fn review_more_than_candidate_cap_busy_rows_finish_and_allow_next_sweep() {
        let mut h = Harness::new(PreviousProjection::Present(BTreeMap::new()), None, vec![]);
        for id in 1..=300 {
            h.source
                .tasks
                .insert(terminal(id, SafeOutcome::Done, false))
                .unwrap();
        }
        let mut complete = false;
        for i in 0..100 {
            let result = h.tick(None, i == 0);
            assert!(result.pending_ids.len() <= 256);
            if result.repair == RepairProgress::Complete {
                complete = true;
                break;
            }
        }
        assert!(
            complete,
            "settled busy registry must finish even with more than 256 rows"
        );
        let first = h.source.tasks.repair_requests().len();
        h.runtime.advance(Duration::from_secs(15));
        h.tick(None, false);
        assert!(h.source.tasks.repair_requests().len() > first);
    }
    #[test]
    fn review_resolving_cold_unknown_proof_baselines_history_including_overflow() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![]);
        for id in 1..=300 {
            let mut facts = terminal(id, SafeOutcome::Done, true);
            facts.queue_dispatching = None;
            facts.busy = None;
            facts.quiescent = None;
            h.source.tasks.insert(facts).unwrap();
        }
        assert!(h.sweep().is_empty());
        for id in 1..=300 {
            h.source
                .tasks
                .insert(terminal(id, SafeOutcome::Done, true))
                .unwrap();
        }
        h.runtime.advance(Duration::from_secs(15));
        assert!(
            h.sweep().is_empty(),
            "finishing old proof must not alert historical completion"
        );
    }
    #[test]
    fn review_attention_summary_waits_for_addressed_confirmation() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![]);
        h.source
            .tasks
            .insert(terminal(1, SafeOutcome::NeedsInput, true))
            .unwrap();
        let first = h.tick(None, true);
        assert!(first.attention.is_none());
        assert!(first.confirmed.is_empty());
        let mut closed = terminal(1, SafeOutcome::NeedsInput, true);
        closed.state = "closed".into();
        h.source.tasks.insert(closed).unwrap();
        let result = h.tick(None, false);
        assert_eq!(result.attention.unwrap().count, 0);
        assert_eq!(h.source.tasks.addressed_requests().len(), 1);
        assert!(result.changes.is_empty());
    }

    #[test]
    fn unresolved_cold_attention_does_not_hide_a_later_new_turn() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![]);
        let mut unknown = terminal(1, SafeOutcome::NeedsInput, true);
        unknown.queue_dispatching = None;
        unknown.busy = None;
        unknown.quiescent = None;
        h.source.tasks.insert(unknown).unwrap();
        for i in 0..4 {
            assert!(h.tick(None, i == 0).changes.is_empty());
        }
        let mut current = terminal(1, SafeOutcome::Blocked, true);
        current.latest_turn_id = Some(TurnId::new(uuid::Uuid::from_u128(901)));
        h.source.tasks.insert(current).unwrap();
        let result = h.tick(None, false);
        assert_eq!(result.changes.len(), 1);
        assert_eq!(result.changes[0].cause, ChangeCause::RepairDifference);
    }

    #[test]
    fn busy_pending_capacity_does_not_block_attention_overflow() {
        let old: BTreeMap<_, _> = (1..=256)
            .map(|id| (task_id(id), terminal(id, SafeOutcome::Done, false)))
            .collect();
        let mut h = Harness::new(
            PreviousProjection::Present(old.clone()),
            None,
            (1..=256).map(task_id).collect(),
        );
        for row in old.into_values() {
            h.source.tasks.insert(row).unwrap();
        }
        h.source
            .tasks
            .insert(terminal(1000, SafeOutcome::NeedsInput, true))
            .unwrap();
        let mut summary = None;
        let mut changes = Vec::new();
        for i in 0..100 {
            let result = h.tick(None, i == 0);
            assert!(result.pending_ids.len() <= 256);
            changes.extend(result.changes);
            if result.attention.is_some() {
                summary = result.attention;
                break;
            }
        }
        assert_eq!(
            summary
                .expect("fresh overflow attention must be confirmed")
                .count,
            1
        );
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].task_id, task_id(1000));
    }

    #[test]
    fn warm_eligible_overflow_is_rediscovered_without_losing_or_repeating_changes() {
        let mut h = Harness::new(PreviousProjection::Present(BTreeMap::new()), None, vec![]);
        for id in 1..=300 {
            h.source
                .tasks
                .insert(terminal(id, SafeOutcome::Done, true))
                .unwrap();
        }
        let mut changes = Vec::new();
        for i in 0..100 {
            let result = h.tick(None, i == 0);
            assert!(result.pending_ids.len() <= 256 && result.changes.len() <= 256);
            changes.extend(result.changes);
        }
        assert_eq!(changes.len(), 300);
        let ids: std::collections::BTreeSet<_> = changes.iter().map(|row| row.task_id).collect();
        assert_eq!(ids.len(), 300);
    }

    #[test]
    fn unresolved_attention_allows_timer_repair_and_waits_for_real_proof() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![]);
        let mut unknown = terminal(1, SafeOutcome::NeedsInput, true);
        unknown.queue_dispatching = None;
        unknown.busy = None;
        unknown.quiescent = None;
        h.source.tasks.insert(unknown).unwrap();
        for i in 0..4 {
            let result = h.tick(None, i == 0);
            assert!(result.attention.is_none() && result.changes.is_empty());
        }
        let before = h.source.tasks.repair_requests().len();
        h.runtime.advance(Duration::from_secs(15));
        assert!(h.tick(None, false).attention.is_none());
        assert!(h.source.tasks.repair_requests().len() > before);
        h.source
            .tasks
            .insert(terminal(1, SafeOutcome::NeedsInput, true))
            .unwrap();
        let result = h.tick(None, false);
        assert_eq!(result.attention.unwrap().count, 1);
        assert!(result.changes.is_empty());
    }

    #[test]
    fn changed_turn_of_unresolved_cold_member_survives_a_completed_new_sweep() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![]);
        for id in 1..=300 {
            let mut row = terminal(id, SafeOutcome::NeedsInput, true);
            row.queue_dispatching = None;
            row.busy = None;
            row.quiescent = None;
            h.source.tasks.insert(row).unwrap();
        }
        for i in 0..10 {
            assert!(h.tick(None, i == 0).changes.is_empty());
        }
        let mut current = terminal(250, SafeOutcome::Blocked, true);
        current.latest_turn_id = Some(TurnId::new(uuid::Uuid::from_u128(950)));
        h.source.tasks.insert(current).unwrap();
        h.runtime.advance(Duration::from_secs(15));
        let mut changes = Vec::new();
        for _ in 0..80 {
            changes.extend(h.tick(None, false).changes);
        }
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].task_id, task_id(250));
        assert_eq!(changes[0].cause, ChangeCause::RepairDifference);
    }

    #[test]
    fn attention_proof_that_becomes_unknown_cannot_complete_a_stale_summary() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![]);
        for id in 1..=300 {
            h.source
                .tasks
                .insert(terminal(id, SafeOutcome::NeedsInput, true))
                .unwrap();
        }
        for i in 0..200 {
            let result = h.tick(None, i == 0);
            assert!(result.attention.is_none());
            let requests = h.source.tasks.addressed_requests();
            if requests
                .last()
                .is_some_and(|query| query.task_ids.contains(&task_id(300)))
            {
                break;
            }
        }
        let mut unknown = terminal(1, SafeOutcome::NeedsInput, true);
        unknown.queue_dispatching = None;
        unknown.busy = None;
        unknown.quiescent = None;
        h.source.tasks.insert(unknown).unwrap();
        let after = h.reconciler.cursor().unwrap();
        h.source.hint(1, false);
        let read = h.feed(after);
        for i in 0..4 {
            assert!(
                h.tick((i == 0).then(|| read.clone()), false)
                    .attention
                    .is_none()
            );
        }
        h.source
            .tasks
            .insert(terminal(1, SafeOutcome::NeedsInput, true))
            .unwrap();
        let mut summary = None;
        for _ in 0..4 {
            let result = h.tick(None, false);
            if result.attention.is_some() {
                summary = result.attention;
                break;
            }
        }
        assert_eq!(summary.unwrap().count, 300);
    }

    #[test]
    fn timer_sweeps_preserve_slow_attention_confirmation_progress() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![]);
        for id in 1..=300 {
            h.source
                .tasks
                .insert(terminal(id, SafeOutcome::NeedsInput, true))
                .unwrap();
        }
        let mut summary = None;
        for i in 0..100 {
            let result = h
                .reconciler
                .reconcile(
                    h.source.as_ref(),
                    ReconcileInput {
                        read: None,
                        repair_due: i == 0,
                        include_titles: false,
                    },
                    h.runtime.now() + Duration::from_secs(60),
                )
                .unwrap();
            if result.attention.is_some() {
                summary = result.attention;
                break;
            }
            h.runtime.advance(Duration::from_secs(1));
        }
        assert_eq!(
            summary
                .expect("timer repairs must preserve confirmation progress")
                .count,
            300
        );
        assert!(h.source.tasks.repair_requests().len() > 5);
    }

    #[test]
    fn hinted_attention_member_waits_behind_unrelated_unknown_candidates() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![]);
        for id in 1..=300 {
            h.source
                .tasks
                .insert(terminal(id, SafeOutcome::NeedsInput, true))
                .unwrap();
        }
        for i in 0..100 {
            let result = h.tick(None, i == 0);
            assert!(result.attention.is_none());
            if h.source
                .tasks
                .addressed_requests()
                .last()
                .is_some_and(|query| query.task_ids.contains(&task_id(300)))
            {
                break;
            }
        }
        let after = h.reconciler.cursor().unwrap();
        for id in 1001..=1032 {
            let mut row = terminal(id, SafeOutcome::Done, true);
            row.queue_dispatching = None;
            row.busy = None;
            row.quiescent = None;
            h.source.tasks.insert(row).unwrap();
            h.source.hint(id, false);
        }
        let mut closed = terminal(1, SafeOutcome::NeedsInput, true);
        closed.state = "closed".into();
        h.source.tasks.insert(closed).unwrap();
        h.source.hint(1, false);
        let read = h.feed(after);
        assert!(h.tick(Some(read), false).attention.is_none());
        assert!(h.tick(None, false).attention.is_none());
        let mut summary = None;
        for _ in 0..6 {
            if let Some(attention) = h.tick(None, false).attention {
                summary = Some(attention);
                break;
            }
        }
        assert_eq!(summary.unwrap().count, 299);
    }

    #[test]
    fn global_hint_during_hash_waits_for_a_new_complete_repair() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![]);
        for id in 1..=300 {
            h.source
                .tasks
                .insert(terminal(id, SafeOutcome::NeedsInput, true))
                .unwrap();
        }
        for i in 0..100 {
            let result = h.tick(None, i == 0);
            assert!(result.attention.is_none());
            if h.source
                .tasks
                .addressed_requests()
                .last()
                .is_some_and(|query| query.task_ids.contains(&task_id(300)))
            {
                break;
            }
        }
        let mut closed = terminal(1, SafeOutcome::NeedsInput, true);
        closed.state = "closed".into();
        h.source.tasks.insert(closed).unwrap();
        let after = h.reconciler.cursor().unwrap();
        h.source
            .journal
            .append(
                EventBatch::try_new(vec![NewEvent::ControllerDrainChanged { drained: true }])
                    .unwrap(),
                deadline(),
            )
            .unwrap();
        let read = h.feed(after);
        assert!(h.tick(Some(read), false).attention.is_none());
        assert!(h.tick(None, false).attention.is_none());
        let mut summary = None;
        for _ in 0..100 {
            if let Some(attention) = h.tick(None, false).attention {
                summary = Some(attention);
                break;
            }
        }
        assert_eq!(summary.unwrap().count, 299);
    }

    #[test]
    fn cold_live_and_repair_replay_overlap_emits_terminal_evidence_once() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![]);
        let after = h.source.journal.window(deadline()).unwrap().cursor();
        h.reconciler = TaskReconciler::new(
            PreviousProjection::Absent,
            Some(after),
            vec![],
            h.runtime.clone(),
        );
        for id in 1..=2 {
            h.source
                .tasks
                .insert(terminal(id, SafeOutcome::Done, true))
                .unwrap();
        }
        h.source.tasks.set_page_budget(1);
        assert!(h.tick(None, true).changes.is_empty());
        h.source.hint(1, true);
        let read = h.feed(after);
        let mut changes = h.tick(Some(read), false).changes;
        changes.extend(h.sweep());
        assert_eq!(changes.len(), 1);
        assert!(matches!(
            changes[0].cause,
            ChangeCause::ReplayTerminal { .. }
        ));
    }

    #[test]
    fn ahead_saved_cursor_is_replaced_only_by_the_completed_captured_baseline() {
        let mut h = Harness::new(warm(terminal(1, SafeOutcome::Done, true)), None, vec![]);
        h.source
            .tasks
            .insert(terminal(1, SafeOutcome::Done, true))
            .unwrap();
        let window = h.source.journal.window(deadline()).unwrap();
        let ahead = EventCursor {
            journal_id: window.journal_id,
            seq: Seq::new(900),
        };
        h.reconciler = TaskReconciler::new(
            warm(terminal(1, SafeOutcome::Done, true)),
            Some(ahead),
            vec![],
            h.runtime.clone(),
        );
        let control = h
            .source
            .journal
            .read(
                ReadQuery {
                    after: Some(ahead),
                    limit: 128,
                    wait_ms: 0,
                },
                deadline(),
            )
            .unwrap();
        let result = h.tick(Some(control), false);
        assert_eq!(result.repair, RepairProgress::Complete);
        assert_eq!(result.consumed_after, Some(window.cursor()));
        assert!(result.changes.is_empty());
    }

    #[test]
    fn repeated_compatible_repair_control_preserves_active_key_sweep() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![]);
        let old = h.source.journal.window(deadline()).unwrap().cursor();
        h.reconciler = TaskReconciler::new(
            PreviousProjection::Absent,
            Some(old),
            vec![],
            h.runtime.clone(),
        );
        h.source
            .journal
            .replace_epoch(uuid::Uuid::from_u128(2))
            .unwrap();
        for id in 1..=2 {
            h.source
                .tasks
                .insert(terminal(id, SafeOutcome::Done, true))
                .unwrap();
        }
        h.source.tasks.set_page_budget(1);
        let control = h
            .source
            .journal
            .read(
                ReadQuery {
                    after: Some(old),
                    limit: 128,
                    wait_ms: 0,
                },
                deadline(),
            )
            .unwrap();
        assert_eq!(
            h.tick(Some(control.clone()), false).repair,
            RepairProgress::InProgress
        );
        let result = h.tick(Some(control), false);
        assert_eq!(result.repair, RepairProgress::Complete);
        let requests = h.source.tasks.repair_requests();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].after.is_some());
    }

    fn queued(id: u128) -> TaskFacts {
        let mut row = terminal(id, SafeOutcome::Done, false);
        row.state = "queued".into();
        row.latest_turn_id = None;
        row.outcome = None;
        row.runner_present = false;
        row.result_imported = false;
        row.busy = Some(false);
        row.quiescent = Some(false);
        row.queue_dispatching = Some(false);
        row.validate().unwrap();
        assert_eq!(row.eligibility_signature().busy, Some(false));
        assert_eq!(row.eligibility_signature().quiescent, Some(false));
        row
    }

    #[test]
    fn queued_pending_capacity_does_not_block_a_fresh_attention_member() {
        let old: BTreeMap<_, _> = (1..=256).map(|id| (task_id(id), queued(id))).collect();
        let mut h = Harness::new(
            PreviousProjection::Present(old.clone()),
            None,
            (1..=256).map(task_id).collect(),
        );
        for row in old.into_values() {
            h.source.tasks.insert(row).unwrap();
        }
        h.source
            .tasks
            .insert(terminal(1000, SafeOutcome::NeedsInput, true))
            .unwrap();
        let mut summary = None;
        let mut changes = Vec::new();
        for i in 0..100 {
            let result = h.tick(None, i == 0);
            assert!(result.pending_ids.len() <= 256);
            changes.extend(result.changes);
            if result.attention.is_some() {
                summary = result.attention;
                break;
            }
        }
        assert_eq!(
            summary
                .expect("queued candidates must yield their bounded slots")
                .count,
            1
        );
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].task_id, task_id(1000));
    }

    #[test]
    fn cold_queued_baseline_detects_first_terminal_turn_without_a_hint() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![task_id(1)]);
        h.source.tasks.insert(queued(1)).unwrap();
        let baseline = h.tick(None, true);
        assert_eq!(baseline.repair, RepairProgress::Complete);
        assert!(baseline.changes.is_empty());
        h.source
            .tasks
            .insert(terminal(1, SafeOutcome::Done, true))
            .unwrap();
        let result = h.tick(None, false);
        assert_eq!(result.changes.len(), 1);
        assert_eq!(result.changes[0].cause, ChangeCause::RepairDifference);
        assert_eq!(result.changes[0].previous.as_ref().unwrap().state, "queued");
    }

    #[test]
    fn cold_queued_overflow_admits_retained_terminal_before_baseline_commit() {
        let mut h = Harness::new(PreviousProjection::Absent, None, vec![]);
        let after = h.source.journal.window(deadline()).unwrap().cursor();
        h.reconciler = TaskReconciler::new(
            PreviousProjection::Absent,
            Some(after),
            (1..=256).map(task_id).collect(),
            h.runtime.clone(),
        );
        for id in 1..=256 {
            h.source.tasks.insert(queued(id)).unwrap();
        }
        h.source
            .tasks
            .insert(terminal(1000, SafeOutcome::Done, true))
            .unwrap();
        h.source.hint(1000, true);
        let changes = h.sweep();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].task_id, task_id(1000));
        assert!(matches!(
            changes[0].cause,
            ChangeCause::ReplayTerminal { .. }
        ));
        assert_eq!(h.reconciler.cursor().unwrap().seq, Seq::new(1));
        assert!(
            matches!(h.reconciler.previous_projection(), PreviousProjection::Present(rows) if rows.len() == 257)
        );
    }
}

mod transport_bounds {
    use super::*;
    use mac_worker::controller::events::{
        EventBatch, JournalReader, JournalWriter, NewEvent, TaskProjectionReader,
        rpc::TaskEventReadStore, testing::ManualEventRuntime,
    };
    use std::sync::Mutex;
    type ReplyFactory = dyn Fn(&mac_worker::controller::ControllerRequest) -> Value + Send + Sync;
    struct ReplyRunner {
        reply: Box<ReplyFactory>,
        calls: Mutex<Vec<ProcessRequest>>,
    }
    impl ProcessRunner for ReplyRunner {
        fn run(&self, process: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            self.calls.lock().unwrap().push(process.clone());
            let request = decode_request(process.stdin.as_ref().unwrap())?;
            let value = (self.reply)(&request);
            Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout: encode_json_frame(&value)?,
                stderr: b"PRIVATE REMOTE DETAIL".to_vec(),
            })
        }
    }
    fn client(
        reply: impl Fn(&mac_worker::controller::ControllerRequest) -> Value + Send + Sync + 'static,
        runtime: Arc<ManualEventRuntime>,
    ) -> (ControllerEventClient, Arc<ReplyRunner>) {
        let runner = Arc::new(ReplyRunner {
            reply: Box::new(reply),
            calls: Mutex::new(vec![]),
        });
        let config =
            Config::parse("version=1\n[controller]\nenabled=true\nssh='controller'\n").unwrap();
        (
            ControllerEventClient::new(runner.clone(), config.controller, runtime),
            runner,
        )
    }
    #[test]
    fn only_exact_legacy_selector_rejection_means_unsupported() {
        for (code, message, want) in [
            (
                "INVALID_REQUEST",
                "task.list body contained unexpected key controller_events",
                "CONTROLLER_EVENTS_UNSUPPORTED",
            ),
            (
                "INVALID_REQUEST",
                "another malformed request",
                "CONTROLLER_EVENTS_UNAVAILABLE",
            ),
            (
                "CONTROLLER_EVENTS_UNAVAILABLE",
                "task.list body contained unexpected key controller_events",
                "CONTROLLER_EVENTS_UNAVAILABLE",
            ),
            (
                "INVALID_REQUEST",
                "protocol error",
                "CONTROLLER_EVENTS_UNAVAILABLE",
            ),
        ] {
            let (client, _) = client(
                move |_| {
                    serde_json::to_value(HostControlError::new(code, message).unwrap()).unwrap()
                },
                Arc::new(ManualEventRuntime::new()),
            );
            let error = client.read(ReadQuery::default(), deadline()).unwrap_err();
            assert_eq!(error.public_code(), want);
            assert!(!error.to_string().contains("PRIVATE"));
        }
    }
    #[test]
    fn every_reply_identity_field_is_verified() {
        for field in [
            "protocol_version",
            "command",
            "request_id",
            "payload_sha256",
        ] {
            let (client, _) = client(
                move |request| {
                    let mut value = envelope(
                        request,
                        json!({"rows":[],"missing":[task_id(1)],"proof_after":null,"baseline_after":null}),
                    );
                    value[field] = if field == "protocol_version" {
                        json!(6)
                    } else {
                        json!("wrong")
                    };
                    value
                },
                Arc::new(ManualEventRuntime::new()),
            );
            let error = client
                .tasks(
                    TaskAddressQuery::try_new(vec![task_id(1)], false, None).unwrap(),
                    deadline(),
                )
                .unwrap_err();
            assert_eq!(error.public_code(), "CONTROLLER_EVENTS_UNAVAILABLE");
        }
    }
    #[test]
    fn addressed_reply_covers_exact_requested_ids_and_title_preference() {
        for bad in [
            json!({"rows":[],"missing":[task_id(2)],"proof_after":null,"baseline_after":null}),
            {
                let mut facts = terminal(1, SafeOutcome::Done, true);
                facts.title = Some("display".into());
                json!({"rows":[facts],"missing":[],"proof_after":null,"baseline_after":null})
            },
        ] {
            let (client, _) = client(
                move |request| envelope(request, bad.clone()),
                Arc::new(ManualEventRuntime::new()),
            );
            assert!(
                client
                    .tasks(
                        TaskAddressQuery::try_new(vec![task_id(1)], false, None).unwrap(),
                        deadline()
                    )
                    .is_err()
            );
        }
    }
    #[test]
    fn limit_wait_overflow_clamps_and_process_policy_keeps_original_budget() {
        let journal = MemoryJournal::new();
        let after = journal.window(deadline()).unwrap().cursor();
        journal
            .append(
                EventBatch::try_new(vec![NewEvent::ControllerDrainChanged { drained: true }; 32])
                    .unwrap(),
                deadline(),
            )
            .unwrap();
        let (client, runner) = client(
            move |request| {
                let EventSelector::Read(query) =
                    EventSelector::from_request_body(request.body()).unwrap()
                else {
                    panic!("read")
                };
                assert_eq!(query.limit, 256);
                assert_eq!(query.wait_ms, 20000);
                envelope(
                    request,
                    serde_json::to_value(journal.read(query, deadline()).unwrap()).unwrap(),
                )
            },
            Arc::new(ManualEventRuntime::new()),
        );
        client
            .read(
                ReadQuery {
                    after: Some(after),
                    limit: usize::MAX,
                    wait_ms: u64::MAX,
                },
                Duration::from_secs(7),
            )
            .unwrap();
        assert_eq!(
            runner.calls.lock().unwrap()[0].policy.deadline,
            Duration::from_secs(7)
        );
    }
    #[test]
    fn read_deadline_and_cancellation_use_injected_runtime() {
        let runtime = Arc::new(ManualEventRuntime::new());
        let advance = runtime.clone();
        let (client, runner) = client(
            move |request| {
                advance.advance(Duration::from_secs(30));
                envelope(request, json!({}))
            },
            runtime.clone(),
        );
        assert_eq!(
            client
                .read(ReadQuery::default(), deadline())
                .unwrap_err()
                .public_code(),
            "CONTROLLER_EVENTS_UNAVAILABLE"
        );
        assert_eq!(
            runner.calls.lock().unwrap()[0].policy.deadline,
            Duration::from_secs(30)
        );
        runtime.cancel();
        assert_eq!(
            client
                .read(ReadQuery::default(), deadline())
                .unwrap_err()
                .public_code(),
            "CONTROLLER_EVENTS_CANCELLED"
        );
        assert_eq!(runner.calls.lock().unwrap().len(), 1);
    }
    #[test]
    fn lazy_real_reader_reports_event_cancellation_code() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().canonicalize().unwrap();
        let paths = PathLayout {
            config: path.join("config"),
            state: path.join("state"),
            cache: path.join("cache"),
            data: path.join("data"),
        };
        ClientStateStore::open(&paths.state).unwrap();
        let runtime = Arc::new(ManualEventRuntime::new());
        let reader = TaskEventReadStore::open_existing(&paths, runtime.clone()).unwrap();
        runtime.cancel();
        let error = reader
            .addressed(
                TaskAddressQuery::try_new(vec![task_id(1)], false, None).unwrap(),
                deadline(),
            )
            .unwrap_err();
        assert_eq!(error.public_code(), "CONTROLLER_EVENTS_CANCELLED");
    }
    #[test]
    fn damaged_or_absent_journal_does_not_disable_supported_state_selectors() {
        for journal in [
            FakeJournalProvider::error("damaged event root/lock"),
            FakeJournalProvider::absent(),
        ] {
            let tasks = FakeTaskProjectionProvider::new(Arc::new(MemoryTaskReader::new()));
            for selector in [
                EventSelector::Tasks(
                    TaskAddressQuery::try_new(vec![task_id(1)], false, None).unwrap(),
                ),
                EventSelector::Repair(TaskRepairQuery::default()),
            ] {
                serve_selector_with(&request(&selector), &journal, &tasks, deadline()).unwrap();
            }
            assert_eq!(journal.open_count(), 0);
            assert_eq!(tasks.open_count(), 2);
            assert!(
                serve_selector_with(
                    &request(&EventSelector::Read(ReadQuery::default())),
                    &journal,
                    &tasks,
                    deadline()
                )
                .is_err()
            );
        }
    }
}

// Frozen DTO definitions from 37915a9, before the event wave. Existing nested
// task schema types are reused; no production DTO or module root is changed.
#[allow(dead_code)]
mod legacy_v7 {
    use mac_worker::{
        dag::DagNodeProjection,
        task::{
            BranchName, ClosePolicy, OriginDelivery, RunId, RunnerState, TaskId, TaskOutcome,
            TaskState, TaskStatus, TurnId,
        },
        task_view::{ReviewState, TaskFreshness},
    };
    use serde_json::Value;
    #[derive(Debug, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct ControllerReadReply<T> {
        pub(super) protocol_version: u32,
        pub(super) command: String,
        pub(super) request_id: String,
        pub(super) payload_sha256: String,
        pub(super) result: T,
    }

    #[derive(Debug, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct ControllerTaskStatusResult {
        pub(super) task_id: TaskId,
        pub(super) run_id: Option<RunId>,
        pub(super) status: TaskStatus,
        #[serde(default)]
        pub(super) warnings: Vec<String>,
        #[serde(default)]
        pub(super) events: Vec<Value>,
        pub(super) runner: Option<RunnerState>,
        pub(super) exit_code: Option<u8>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(super) delivery: Option<OriginDelivery>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        pub(super) deliveries: Vec<OriginDelivery>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(super) stage: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(super) residual: Option<Vec<String>>,
    }

    #[derive(Debug, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct ControllerTaskLogsResult {
        pub(super) task_id: TaskId,
        pub(super) turn_id: TurnId,
        pub(super) turn_number: u32,
        pub(super) agent: String,
        pub(super) offset: u64,
        pub(super) next_offset: u64,
        pub(super) exhausted: bool,
        pub(super) complete: bool,
        pub(super) raw: bool,
        pub(super) bytes_base64: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(super) failure: Option<String>,
    }

    #[derive(Debug, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct ControllerWaitPollResult {
        pub(super) task_ids: Vec<TaskId>,
        pub(super) quiescent: bool,
        pub(super) exit_code: u8,
    }

    #[derive(Debug, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct DrainResult {
        pub(super) drained: bool,
    }

    #[derive(Debug, serde::Deserialize)]
    pub(super) struct TaskListProjection {
        pub tasks: Vec<TaskListRow>,
        pub runs: Vec<TaskRunProjection>,
        pub progress: RunProgress,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        pub dag_nodes: Vec<DagNodeProjection>,
    }

    #[derive(Debug, serde::Deserialize)]
    pub(super) struct TaskListRow {
        pub task_id: TaskId,
        pub run_id: Option<RunId>,
        pub run_position: Option<u32>,
        pub title: String,
        pub agent: String,
        pub model: Option<String>,
        pub effort: Option<String>,
        pub permissions: Option<String>,
        pub env_profile: Option<String>,
        pub state: TaskState,
        pub blocking_code: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub stage: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub residual: Option<Vec<String>>,
        pub last_outcome: Option<TaskOutcome>,
        pub worker: Option<String>,
        pub branch: BranchName,
        pub turn_count: u32,
        pub runner: Option<RunnerState>,
        pub freshness: TaskFreshness,
        pub created_at_millis: u64,
        pub updated_at_millis: u64,
        pub active_turn_id: Option<TurnId>,
        pub close_policy: ClosePolicy,
        pub review_state: ReviewState,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub delivery: Option<OriginDelivery>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        pub deliveries: Vec<OriginDelivery>,
        /// True when the task asked for an origin push. Text `task list` uses
        /// this so fetch-only rows never grow a `push:` suffix.
        #[serde(skip)]
        pub publish_push: bool,
    }

    #[derive(Debug, serde::Deserialize)]
    pub(super) struct TaskRunProjection {
        pub run_id: RunId,
        pub name: Option<String>,
        pub max_parallel: u32,
        pub created_at_millis: u64,
        pub progress: RunProgress,
    }

    #[derive(Debug, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct RunProgress {
        pub total: usize,
        pub queued: usize,
        pub active: usize,
        pub open: usize,
        pub closed: usize,
        pub failed_like: usize,
    }
}
