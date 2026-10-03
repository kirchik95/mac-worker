use mac_worker::test_support::events::{JournalProvider, testing::FakeJournalProvider};
use std::time::Duration;

use clap::Parser;
use mac_worker::test_support::{
    agents::agent::{AgentKind, PermissionPolicy},
    cli::Cli,
    client_state::{ClientStateStore, scheduler::WorkerPreference},
    controller::encode_json_frame,
    core::{error::WorkerError, paths::PathLayout},
    events::{
        EventCursor, EventReadResult, JournalReader, ReadQuery, Seq, WireEvent,
        journal::{ControllerJournal, JournalOptions},
        testing::ManualEventRuntime,
    },
    host::{
        job::{CommandSummary, QueueEntry, QueueEntryKind},
        process::{ProcessRequest, ProcessResult, ProcessRunner},
        supervisor::SystemProcessInspector,
    },
    runtime::{RuntimeContext, run_with_stdio_in_context},
    task::model::{
        ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskId, TaskLimits, TaskMeta,
        TaskMetaInput, TaskSource, TaskState, TaskStatus, TurnId, TurnSummary,
    },
};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    io::{BufRead, BufReader, Cursor},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, mpsc},
};

struct CommitAwareWriter {
    bytes: Vec<u8>,
    wait_for: Option<(PathLayout, &'static str)>,
}
impl std::io::Write for CommitAwareWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if let Some((paths, kind)) = self.wait_for.take() {
            use std::os::unix::fs::PermissionsExt;
            let event_root = paths.controller_state_root().join("events");
            if event_root.join("initialization.json").is_file()
                && fs::symlink_metadata(&event_root)
                    .is_ok_and(|entry| entry.is_dir() && entry.permissions().mode() & 0o077 == 0)
            {
                let (sent, received) = mpsc::channel();
                std::thread::spawn(move || {
                    loop {
                        let deadline = Duration::MAX;
                        if let Ok(Some(journal)) = ControllerJournal::open_existing(
                            &paths,
                            JournalOptions {
                                runtime: Arc::new(ManualEventRuntime::new()),
                            },
                        ) && let Ok(window) = journal.window(deadline)
                            && let Ok(EventReadResult::Batch(batch)) = journal.read(
                                ReadQuery {
                                    after: Some(EventCursor {
                                        journal_id: window.journal_id,
                                        seq: Seq::ZERO,
                                    }),
                                    limit: 256,
                                    wait_ms: 0,
                                },
                                deadline,
                            )
                            && batch.events.iter().any(|event| event.kind == kind)
                        {
                            let _ = sent.send(());
                            break;
                        }
                        std::thread::yield_now();
                    }
                });
                // Deterministic fixture handshake while the real process
                // still owns its publisher, before optional exit grace.
                received
                    .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
                    .unwrap();
            }
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct NoRemoteProcesses;
impl ProcessRunner for NoRemoteProcesses {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == std::ffi::OsStr::new("/usr/bin/git") {
            use std::os::unix::process::ExitStatusExt;
            // Cancellation may try to release a base in a local project.
            // This fixture intentionally has no project or transfer repo.
            return Ok(ProcessResult {
                status: std::process::ExitStatus::from_raw(256),
                stdout: Vec::new(),
                stderr: Vec::new(),
            });
        }
        panic!(
            "fixture must not invoke a remote process: {:?}",
            request.program
        );
    }
}

fn isolated(root: &Path) -> (RuntimeContext, PathLayout, BTreeMap<OsString, OsString>) {
    let home = root.join("home");
    let environment = BTreeMap::from([
        (OsString::from("HOME"), home.as_os_str().to_owned()),
        (
            OsString::from("XDG_STATE_HOME"),
            root.join("state").into_os_string(),
        ),
        (
            OsString::from("XDG_CONFIG_HOME"),
            root.join("config").into_os_string(),
        ),
        (
            OsString::from("XDG_CACHE_HOME"),
            root.join("cache").into_os_string(),
        ),
        (
            OsString::from("XDG_DATA_HOME"),
            root.join("data").into_os_string(),
        ),
    ]);
    for value in environment.values() {
        crate::support::create_directory(PathBuf::from(value));
    }
    let paths = PathLayout::discover(None, &environment, &home).unwrap();
    (
        RuntimeContext::isolated(environment.clone(), home, root.to_owned()),
        paths,
        environment,
    )
}

/// Private process homes and a no-network runner. Re-exec enters the real CLI
/// dispatcher; the parent creates fixtures using a deliberately bare store.
struct ProcessWiringHarness {
    _temporary: tempfile::TempDir,
    root: PathBuf,
    paths: PathLayout,
    environment: BTreeMap<OsString, OsString>,
}

impl ProcessWiringHarness {
    fn controller() -> Self {
        Self::laptop_local()
    }

    fn laptop_local() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let (_, paths, environment) = isolated(&root);
        crate::support::create_directory(paths.config.parent().unwrap());
        fs::write(&paths.config, "version = 1\n[controller]\nenabled = false\n[notifications]\nherdr = false\n[[workers]]\nname = \"fixture\"\nssh = \"never-connect\"\nslots = 1\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        let fake_ssh = root.join("fake-ssh");
        fs::write(
            &fake_ssh,
            "#!/bin/sh\n# Private controller fixture: never reach a host.\nexit 1\n",
        )
        .unwrap();
        fs::set_permissions(fake_ssh, fs::Permissions::from_mode(0o700)).unwrap();
        Self {
            _temporary: temporary,
            root,
            paths,
            environment,
        }
    }

    fn start_controller_leader(&mut self) {
        let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
        command
            .envs(&self.environment)
            .current_dir(&self.root)
            .env("MAC_WORKER_TEST_SSH", self.root.join("fake-ssh"))
            .args(["controller", "run"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = crate::controller_process::OwnedChild::spawn(&mut command);
        let stdout = child.take_stdout();
        let (sent, received) = mpsc::channel();
        std::thread::spawn(move || {
            let mut line = String::new();
            BufReader::new(stdout).read_line(&mut line).unwrap();
            sent.send(line).unwrap();
        });
        assert_eq!(
            received
                .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
                .unwrap()
                .trim(),
            "controller leader acquired"
        );
        unsafe {
            libc::kill(child.id() as i32, libc::SIGTERM);
        }
        assert!(
            child
                .wait_timeout(crate::support::HANDSHAKE_TIMEOUT)
                .unwrap()
                .success()
        );
    }

    fn spawn_detached_runner_fixture(&mut self) -> (TaskId, TurnId) {
        let store = ClientStateStore::open(&self.paths.state).unwrap();
        let record = task_record();
        let task = record.meta().task_id();
        let turn = record.status().turns()[0].turn_id();
        store.create_task(record).unwrap();
        store
            .write_turn_prompt(task, turn, "private fixture prompt")
            .unwrap();
        let owner = SystemProcessInspector
            .identity_for_pid(std::process::id())
            .unwrap();
        let entry = QueueEntry::new(
            turn,
            store.client_id(),
            "a".repeat(64),
            "b".repeat(64),
            CommandSummary::argv(1).unwrap(),
            vec![],
            WorkerPreference::Automatic,
            QueueEntryKind::TaskTurn,
            None,
            owner,
            100,
        )
        .unwrap();
        store.enqueue(entry).unwrap();
        // Seed a cancelled waiting row, as left by controller cancellation.
        // No leader/runner is alive while this fixture is prepared.
        let queue_path = self.paths.state.join("queue/state.json");
        let mut wire: serde_json::Value =
            serde_json::from_slice(&fs::read(&queue_path).unwrap()).unwrap();
        wire["entries"][0]["cancel_requested_at_millis"] = serde_json::json!(101);
        let snapshot: mac_worker::test_support::host::job::QueueSnapshot =
            serde_json::from_value(wire).unwrap();
        let mut encoded = serde_json::to_vec(&snapshot).unwrap();
        encoded.push(b'\n');
        fs::write(queue_path, encoded).unwrap();
        let _ = self.reexec("runner", Some((task, turn)));
        let saved = store.load_task(task).unwrap();
        assert_eq!(saved.status().state(), TaskState::Open);
        assert_eq!(
            saved.status().turns()[0].outcome(),
            Some(&mac_worker::test_support::task::model::TaskOutcome::Cancelled)
        );
        (task, turn)
    }

    fn run_laptop_local_fixture(&mut self) {
        self.spawn_detached_runner_fixture();
    }

    fn reexec(&self, mode: &str, ids: Option<(TaskId, TurnId)>) -> std::process::Output {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                &crate::support::libtest_name(module_path!(), "process_entry_fixture"),
                "--nocapture",
            ])
            .env("EV_T8A_PROCESS_ROOT", &self.root)
            .env("EV_T8A_PROCESS_MODE", mode);
        if let Some((task, turn)) = ids {
            command
                .env("EV_T8A_TASK", task.to_string())
                .env("EV_T8A_TURN", turn.to_string());
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "fixture failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if mode != "fsync-gate" {
            assert!(
                !String::from_utf8_lossy(&output.stderr).contains("CONTROLLER_EVENT_HINTS_DROPPED")
            );
        }
        output
    }

    fn journal_exists(&self) -> bool {
        self.paths.controller_state_root().join("events").exists()
    }

    fn rpc<T: serde::de::DeserializeOwned>(&self, body: serde_json::Value) -> T {
        use mac_worker::test_support::controller::{
            decode_frame, parse_request, read::ControllerReadReply,
        };
        use std::io::Write;

        let wire = serde_json::json!({
            "protocol_version": 7,
            "command": "task.list",
            "request_id": uuid::Uuid::new_v4().simple().to_string(),
            "body": body,
        });
        let request = parse_request(&serde_json::to_vec(&wire).unwrap()).unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_worker"))
            .envs(&self.environment)
            .current_dir(&self.root)
            .env("MAC_WORKER_TEST_SSH", self.root.join("fake-ssh"))
            .args(["host", "controller-rpc"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(&encode_json_frame(&wire).unwrap())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "RPC failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.stderr.is_empty(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let reply: ControllerReadReply<T> =
            serde_json::from_slice(decode_frame(&output.stdout).unwrap()).unwrap();
        reply.verify_envelope(&request).unwrap();
        reply.into_result()
    }

    fn journal(&self) -> Arc<ControllerJournal> {
        ControllerJournal::open_existing(
            &self.paths,
            JournalOptions {
                runtime: Arc::new(ManualEventRuntime::new()),
            },
        )
        .unwrap()
        .expect("leader must initialize its host journal")
    }

    fn tail_committed_events(&self) -> Vec<WireEvent> {
        let journal = self.journal();
        let window = journal.window(Duration::from_secs(60)).unwrap();
        match journal
            .read(
                ReadQuery {
                    after: Some(EventCursor {
                        journal_id: window.journal_id,
                        seq: Seq::ZERO,
                    }),
                    limit: 256,
                    wait_ms: 0,
                },
                Duration::from_secs(60),
            )
            .unwrap()
        {
            EventReadResult::Batch(batch) => batch.events,
            other => panic!("expected committed events: {other:?}"),
        }
    }
}

fn task_record() -> LocalTaskRecord {
    let meta = TaskMeta::new(TaskMetaInput {
        session_import: None,
        task_id: TaskId::generate(),
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
        title: Some("private fixture title".into()),
        prompt: "private fixture prompt".into(),
        created_at_millis: 100,
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
            1,
            TurnId::generate(),
            None,
            None,
            None,
            false,
            Some(100),
            None,
        )],
        100,
    )
    .unwrap();
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
    .unwrap()
}

#[test]
fn process_entry_fixture() {
    let Some(root) = std::env::var_os("EV_T8A_PROCESS_ROOT") else {
        return;
    };
    let (runtime, paths, _) = isolated(Path::new(&root));
    let mode = std::env::var("EV_T8A_PROCESS_MODE").unwrap();
    if mode == "fsync-gate" {
        prove_fsync_gate(paths);
        return;
    }
    let mut input = Vec::new();
    let args = match mode.as_str() {
        "runner" => vec![
            "worker".into(),
            "runner".into(),
            std::env::var("EV_T8A_TASK").unwrap(),
            std::env::var("EV_T8A_TURN").unwrap(),
        ],
        "drain" => vec!["worker".into(), "controller".into(), "drain".into()],
        "rpc-drain" => {
            input = encode_json_frame(&serde_json::json!({"protocol_version":7,"request_id":"00000000000000000000000000000001","command":"controller.drain","body":{"drained":true}})).unwrap();
            vec!["worker".into(), "host".into(), "controller-rpc".into()]
        }
        _ => panic!("unknown fixture mode"),
    };
    let mut stdout = CommitAwareWriter {
        bytes: Vec::new(),
        wait_for: (mode != "runner").then(|| (paths.clone(), "controller.drained")),
    };
    let mut stderr = CommitAwareWriter {
        bytes: Vec::new(),
        wait_for: (mode == "runner").then_some((paths, "turn.finished")),
    };
    let code = run_with_stdio_in_context(
        Cli::parse_from(args),
        &NoRemoteProcesses,
        &runtime,
        &mut Cursor::new(input),
        &mut stdout,
        &mut stderr,
    );
    if mode == "runner" {
        assert_eq!(
            code,
            64,
            "{} {}",
            String::from_utf8_lossy(&stdout.bytes),
            String::from_utf8_lossy(&stderr.bytes)
        );
        assert!(String::from_utf8_lossy(&stderr.bytes).contains("TASK_CANCELLED"));
    } else {
        assert_eq!(
            code,
            0,
            "{} {}",
            String::from_utf8_lossy(&stdout.bytes),
            String::from_utf8_lossy(&stderr.bytes)
        );
    }
}

#[test]
fn child_attachment_uses_initialized_host_journal() {
    let mut controller = ProcessWiringHarness::controller();
    controller.start_controller_leader();
    controller.spawn_detached_runner_fixture();
    assert!(
        controller
            .tail_committed_events()
            .iter()
            .any(|event| event.kind == "turn.finished")
    );
    let mut laptop = ProcessWiringHarness::laptop_local();
    laptop.run_laptop_local_fixture();
    assert!(!laptop.journal_exists());
}

#[test]
fn laptop_local_never_initializes_or_emits() {
    let mut laptop = ProcessWiringHarness::laptop_local();
    laptop.run_laptop_local_fixture();
    assert!(!laptop.journal_exists());
    assert!(
        ControllerJournal::open_existing(
            &laptop.paths,
            JournalOptions {
                runtime: Arc::new(ManualEventRuntime::new())
            }
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn leader_restart_preserves_journal_id_and_epoch() {
    let mut controller = ProcessWiringHarness::controller();
    controller.start_controller_leader();
    let id = controller
        .journal()
        .window(Duration::from_secs(60))
        .unwrap()
        .journal_id;
    let epoch_path = controller
        .paths
        .controller_state_root()
        .join("events/initialization.json");
    let epoch = fs::read(&epoch_path).unwrap();
    controller.start_controller_leader();
    assert_eq!(
        controller
            .journal()
            .window(Duration::from_secs(60))
            .unwrap()
            .journal_id,
        id
    );
    assert_eq!(fs::read(epoch_path).unwrap(), epoch);
}

#[test]
fn direct_drain_attaches_to_initialized_host_journal() {
    let mut controller = ProcessWiringHarness::controller();
    controller.start_controller_leader();
    let output = controller.reexec("drain", None);
    let events = controller.tail_committed_events();
    assert!(
        events
            .iter()
            .any(|event| event.kind == "controller.drained"),
        "{events:?} {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn rpc_drain_attaches_to_initialized_host_journal() {
    let mut controller = ProcessWiringHarness::controller();
    controller.start_controller_leader();
    let output = controller.reexec("rpc-drain", None);
    let events = controller.tail_committed_events();
    assert!(
        events
            .iter()
            .any(|event| event.kind == "controller.drained"),
        "{events:?} {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn process_end_to_end_reads_detached_producer_and_rpc_drain_on_one_root() {
    use mac_worker::test_support::{
        controller::ControllerStore,
        events::{
            EventSelector, SafeOutcome, TaskAddressQuery, TaskFactsBatch, TaskRepairPage,
            TaskRepairQuery,
        },
    };

    let mut controller = ProcessWiringHarness::controller();
    controller.start_controller_leader();
    let bootstrap: EventReadResult = controller.rpc(
        EventSelector::Read(ReadQuery::default())
            .request_body()
            .unwrap(),
    );
    let EventReadResult::SnapshotRequired(control) = bootstrap else {
        panic!("a missing cursor must bootstrap from the real host journal");
    };
    assert_eq!(control.reason, "bootstrap");
    let baseline = control.window.cursor();

    let (task, turn) = controller.spawn_detached_runner_fixture();
    controller.reexec("rpc-drain", None);
    let read: EventReadResult = controller.rpc(
        EventSelector::Read(ReadQuery {
            after: Some(baseline),
            limit: 256,
            wait_ms: 0,
        })
        .request_body()
        .unwrap(),
    );
    read.validate().unwrap();
    let EventReadResult::Batch(batch) = read else {
        panic!("RPC must return the committed producer prefix");
    };
    assert_eq!(batch.journal_id, baseline.journal_id);
    let finished = batch
        .events
        .iter()
        .find(|event| event.kind == "turn.finished")
        .unwrap();
    assert_eq!(finished.data["task_id"], task.to_string());
    assert_eq!(finished.data["turn_id"], turn.to_string());
    assert_eq!(finished.data["outcome"], "cancelled");
    let drained = batch
        .events
        .iter()
        .find(|event| event.kind == "controller.drained")
        .unwrap();
    assert!(finished.seq < drained.seq);
    assert_eq!(batch.next_after.seq, batch.events.last().unwrap().seq);
    assert!(!batch.has_more);

    let facts: TaskFactsBatch = controller.rpc(
        EventSelector::Tasks(TaskAddressQuery::try_new(vec![task], false, None).unwrap())
            .request_body()
            .unwrap(),
    );
    facts.validate().unwrap();
    assert_eq!(facts.rows.len(), 1);
    assert!(facts.missing.is_empty());
    assert_eq!(facts.rows[0].task_id, task);
    assert_eq!(facts.rows[0].latest_turn_id, Some(turn));
    assert_eq!(facts.rows[0].outcome, Some(SafeOutcome::Cancelled));
    assert_eq!(facts.rows[0].quiescent, Some(true));
    assert_eq!(facts.rows[0].title, None);

    let repair: TaskRepairPage = controller.rpc(
        EventSelector::Repair(TaskRepairQuery {
            baseline_after: Some(baseline),
            ..TaskRepairQuery::default()
        })
        .request_body()
        .unwrap(),
    );
    repair.validate().unwrap();
    assert!(repair.complete);
    assert_eq!(repair.baseline_after, Some(baseline));
    assert_eq!(repair.rows.len(), 1);
    assert_eq!(repair.rows[0].task_id, task);
    assert_eq!(repair.rows[0].quiescent, Some(true));

    let saved = ClientStateStore::open(&controller.paths.state)
        .unwrap()
        .load_task(task)
        .unwrap();
    assert_eq!(saved.status().state(), TaskState::Open);
    assert_eq!(
        saved.status().turns()[0].outcome(),
        Some(&mac_worker::test_support::task::model::TaskOutcome::Cancelled)
    );
    assert!(
        mac_worker::test_support::controller::drain::is_drained(
            &controller.paths.controller_state_root()
        )
        .unwrap()
    );
    assert_eq!(
        ControllerStore::open(&controller.paths.controller_state_root())
            .unwrap()
            .pending_health(1000)
            .unwrap()
            .active_count,
        0
    );
    assert!(
        fs::read_dir(controller.paths.controller_state_root())
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
fn unsafe_journal_disables_only_the_sink() {
    use std::os::unix::fs::PermissionsExt;
    for symlink in [false, true] {
        let mut controller = ProcessWiringHarness::controller();
        controller.start_controller_leader();
        let root = controller.paths.controller_state_root().join("events");
        let saved = root.with_file_name("events-saved");
        if symlink {
            fs::rename(&root, &saved).unwrap();
            std::os::unix::fs::symlink(&saved, &root).unwrap();
        } else {
            fs::set_permissions(&root, fs::Permissions::from_mode(0o777)).unwrap();
        }
        assert!(
            ControllerJournal::open_existing(
                &controller.paths,
                JournalOptions {
                    runtime: Arc::new(ManualEventRuntime::new())
                }
            )
            .is_err()
        );
        controller.spawn_detached_runner_fixture();
        if symlink {
            fs::remove_file(&root).unwrap();
            fs::rename(saved, &root).unwrap();
        } else {
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        }
        assert!(controller.tail_committed_events().is_empty());
    }
}

fn assert_fences_released(paths: &[PathBuf]) {
    use std::os::fd::AsRawFd;
    for path in paths {
        let file = fs::File::open(path).unwrap();
        assert_eq!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0,
            "append/sleep under fence {}",
            path.display()
        );
    }
}

struct FsyncRelease(Option<mpsc::Sender<()>>);
impl Drop for FsyncRelease {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}

fn prove_fsync_gate(paths: PathLayout) {
    use mac_worker::test_support::{
        controller::{decode_frame, drain::set_drained_with_event_sink, parse_request},
        core::config::Config,
        events::{
            EventBatch, EventRuntime, NewEvent, PublishAttempt,
            journal::{ExistingJournalProvider, JournalFaultPoint},
            rpc::{ExistingTaskProjectionProvider, serve_selector_with},
        },
        runtime::{ControllerEventPublisher, ControllerEventRuntime},
        task::{
            client::TaskClient,
            turn_runner::{InlineRunnerExecutor, TurnRunner},
        },
    };
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };

    let plain = ClientStateStore::open(&paths.state).unwrap();
    let record = task_record();
    let task = record.meta().task_id();
    let turn = record.status().turns()[0].turn_id();
    plain.create_task(record).unwrap();
    plain
        .write_turn_prompt(task, turn, "private fixture prompt")
        .unwrap();
    let owner = SystemProcessInspector
        .identity_for_pid(std::process::id())
        .unwrap();
    plain
        .enqueue(
            QueueEntry::new(
                turn,
                plain.client_id(),
                "a".repeat(64),
                "b".repeat(64),
                CommandSummary::argv(1).unwrap(),
                vec![],
                WorkerPreference::Automatic,
                QueueEntryKind::TaskTurn,
                None,
                owner,
                100,
            )
            .unwrap(),
        )
        .unwrap();
    let queue_path = paths.state.join("queue/state.json");
    let mut wire: serde_json::Value =
        serde_json::from_slice(&fs::read(&queue_path).unwrap()).unwrap();
    wire["entries"][0]["cancel_requested_at_millis"] = serde_json::json!(101);
    let snapshot: mac_worker::test_support::host::job::QueueSnapshot =
        serde_json::from_value(wire).unwrap();
    let mut encoded = serde_json::to_vec(&snapshot).unwrap();
    encoded.push(b'\n');
    fs::write(queue_path, encoded).unwrap();
    let fences = vec![
        paths.state.join("jobs.lock"),
        paths.state.join("queue/lock"),
        paths.controller_state_root().join("drain.lock"),
        paths.state.join(format!("runners/{task}/{turn}.log")),
    ];
    let clock = ManualEventRuntime::new();
    let sleep_fences = fences.clone();
    clock.on_sleep(move |_| assert_fences_released(&sleep_fences));
    let runtime = Arc::new(ControllerEventRuntime::new(Arc::new(clock.clone())));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let first = AtomicBool::new(true);
    let journal = ControllerJournal::open_existing_with_hook(
        &paths,
        JournalOptions {
            runtime: runtime.clone(),
        },
        Arc::new(move |point| {
            if point == JournalFaultPoint::AppendPrepared {
                assert_eq!(std::thread::current().name(), Some("controller-events"));
                assert_fences_released(&fences);
            }
            if point == JournalFaultPoint::SegmentSynced && first.swap(false, Ordering::SeqCst) {
                entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }
            Ok(())
        }),
    )
    .unwrap()
    .unwrap();
    let release = FsyncRelease(Some(release_tx));
    let (store, publisher) = ControllerEventPublisher::attach(plain, journal, runtime.clone());
    let config = Config::load(&paths.config).unwrap();
    let runner = TurnRunner::new(
        &NoRemoteProcesses,
        &config,
        &paths,
        &store,
        &InlineRunnerExecutor,
    );
    assert_eq!(
        runner.run_detached(task, turn).unwrap_err().public_code(),
        "TASK_CANCELLED"
    );
    entered_rx
        .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
        .unwrap();
    // The real runner's outcome is durable before journal visibility/fsync.
    assert_eq!(
        store.load_task(task).unwrap().status().state(),
        TaskState::Open
    );

    let (changed_tx, changed_rx) = mpsc::channel();
    let mutation_store = store
        .reopen_until(Some(std::time::Instant::now() + Duration::from_secs(60)))
        .unwrap();
    std::thread::spawn(move || {
        let record = task_record();
        mutation_store.create_task(record.clone()).unwrap();
        mutation_store
            .enqueue(
                QueueEntry::new(
                    record.status().turns()[0].turn_id(),
                    mutation_store.client_id(),
                    "a".repeat(64),
                    "b".repeat(64),
                    CommandSummary::argv(1).unwrap(),
                    vec![],
                    WorkerPreference::Automatic,
                    QueueEntryKind::TaskTurn,
                    None,
                    owner,
                    100,
                )
                .unwrap(),
            )
            .unwrap();
        changed_tx.send(record.meta().task_id()).unwrap();
    });
    let unrelated = changed_rx
        .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
        .unwrap();
    assert!(store.load_task(unrelated).is_ok());

    let (drain_tx, drain_rx) = mpsc::channel();
    let drain_root = paths.controller_state_root();
    let drain_sink = publisher.sink();
    std::thread::spawn(move || {
        set_drained_with_event_sink(&drain_root, true, Some(drain_sink.clone())).unwrap();
        set_drained_with_event_sink(&drain_root, false, Some(drain_sink)).unwrap();
        drain_tx.send(()).unwrap();
    });
    drain_rx
        .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
        .unwrap();
    assert!(
        !mac_worker::test_support::controller::drain::is_drained(&paths.controller_state_root())
            .unwrap()
    );

    let (read_tx, read_rx) = mpsc::channel();
    let read_store = store.clone();
    let read_paths = paths.clone();
    std::thread::spawn(move || {
        assert_eq!(
            read_store.load_task(task).unwrap().status().state(),
            TaskState::Open
        );
        assert_eq!(read_store.list_tasks().unwrap().len(), 2);
        let read_clock = Arc::new(ManualEventRuntime::new());
        let journal = ExistingJournalProvider::new(read_paths.clone(), read_clock.clone());
        let tasks = ExistingTaskProjectionProvider::new(read_paths.clone(), read_clock);
        for selector in [
            serde_json::json!({"op":"tasks","task_ids":[task],"include_titles":false}),
            serde_json::json!({"op":"repair","limit":64}),
        ] {
            let request = parse_request(&serde_json::to_vec(&serde_json::json!({"protocol_version":7,"request_id":uuid::Uuid::new_v4().simple().to_string(),"command":"task.list","body":{"controller_events":selector}})).unwrap()).unwrap();
            let frame =
                serve_selector_with(&request, &journal, &tasks, Duration::from_secs(60)).unwrap();
            let reply: serde_json::Value =
                serde_json::from_slice(decode_frame(&frame).unwrap()).unwrap();
            assert!(!reply["result"]["rows"].as_array().unwrap().is_empty());
        }
        // Existing strict reads remain usable without the held event lock.
        let client = TaskClient::new(
            &NoRemoteProcesses,
            &config,
            &read_paths,
            &read_store,
            &InlineRunnerExecutor,
        );
        let request = parse_request(&serde_json::to_vec(&serde_json::json!({"protocol_version":7,"request_id":uuid::Uuid::new_v4().simple().to_string(),"command":"task.status","body":{"task_id":task}})).unwrap()).unwrap();
        let frame =
            mac_worker::test_support::controller::read::serve_read_command(&request, &client)
                .unwrap();
        let reply: serde_json::Value =
            serde_json::from_slice(decode_frame(&frame).unwrap()).unwrap();
        assert_eq!(reply["result"]["task_id"], task.to_string());
        read_tx.send(()).unwrap();
    });
    read_rx
        .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
        .unwrap();

    // One in flight + mutation/queue + two drain batches = five. All 128
    // reservations include the in-flight fsync; the next durable write drops.
    for _ in 0..123 {
        store.create_task(task_record()).unwrap();
    }
    assert!(publisher.diagnostics().is_empty());
    assert_eq!(
        mac_worker::test_support::client_state::events::dropped_hint_count(),
        0
    );
    let overflow = task_record();
    store.create_task(overflow.clone()).unwrap();
    assert!(store.load_task(overflow.meta().task_id()).is_ok());
    assert!(
        publisher
            .diagnostics()
            .iter()
            .any(|item| item.code == "CONTROLLER_EVENTS_DROPPED_FULL" && item.count == 1)
    );
    assert_eq!(
        mac_worker::test_support::client_state::events::dropped_hint_count(),
        1
    );
    let sink = publisher.sink();
    let (exit_tx, exit_rx) = mpsc::channel();
    std::thread::spawn(move || {
        drop(publisher);
        exit_tx.send(()).unwrap();
    });
    exit_rx
        .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
        .unwrap();
    assert!(runtime.cancelled());
    assert!(clock.sleeps().iter().copied().sum::<Duration>() <= Duration::from_secs(3));
    assert_eq!(
        sink.try_publish(
            EventBatch::try_new(vec![NewEvent::ControllerDrainChanged { drained: false }]).unwrap()
        ),
        PublishAttempt::Dropped
    );
    // Exit completed while the fsync gate was still held: never join disk I/O.
    drop(release);
}

#[test]
fn publisher_fsync_gate_does_not_block_authoritative_work_or_exit() {
    let mut controller = ProcessWiringHarness::controller();
    controller.start_controller_leader();
    let output = controller.reexec("fsync-gate", None);
    let stderr = String::from_utf8(output.stderr).unwrap();
    let diagnostics = stderr
        .lines()
        .filter(|line| line.starts_with("CONTROLLER_EVENT_HINTS_DROPPED "))
        .collect::<Vec<_>>();
    assert_eq!(diagnostics.len(), 1, "{stderr}");
    assert!(diagnostics[0].contains("count=1"), "{stderr}");
    assert!(
        diagnostics[0].contains("CONTROLLER_EVENTS_DROPPED_FULL=1"),
        "{stderr}"
    );
}

#[test]
fn absent_host_journal_is_optional_and_never_initialized() {
    let provider = FakeJournalProvider::absent();
    assert!(
        provider
            .open_existing(Duration::from_secs(60))
            .unwrap()
            .is_none()
    );
    assert_eq!(provider.open_count(), 1);
}
