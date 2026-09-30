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
    mac_worker::controller::parse_request(
        &serde_json::to_vec(&json!({
            "protocol_version": PROTOCOL_VERSION,
            "command": "task.list", "request_id": uuid::Uuid::new_v4().simple().to_string(),
            "body": selector.request_body().unwrap()
        }))
        .unwrap(),
    )
    .unwrap()
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
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
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
        error::WorkerError,
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
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
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
        assert!(matches!(
            error,
            WorkerError::Process(mac_worker::error::ProcessError::Cancelled)
        ));
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
    fn private_directory(path: &std::path::Path) {
        fs::create_dir(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
}
