use mac_worker::controller::events::{JournalProvider, testing::FakeJournalProvider};
use std::time::Duration;

use clap::Parser;
use mac_worker::{
    RuntimeContext,
    agent::{AgentKind, PermissionPolicy},
    cli::Cli,
    client_state::ClientStateStore,
    controller::{
        encode_json_frame,
        events::{
            EventCursor, EventReadResult, JournalReader, ReadQuery, Seq, WireEvent,
            journal::{ControllerJournal, JournalOptions},
            testing::ManualEventRuntime,
        },
    },
    error::WorkerError,
    job::{CommandSummary, QueueEntry, QueueEntryKind},
    paths::PathLayout,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    run_with_stdio_in_context,
    scheduler::WorkerPreference,
    supervisor::SystemProcessInspector,
    task::{
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
            if paths.controller_state_root().join("events/initialization.json").is_file() {
                let (sent, received) = mpsc::channel();
                std::thread::spawn(move || {
                    loop {
                        let deadline = Duration::MAX;
                        if let Ok(Some(journal)) = ControllerJournal::open_existing(&paths, JournalOptions { runtime: Arc::new(ManualEventRuntime::new()) })
                            && let Ok(window) = journal.window(deadline)
                            && let Ok(EventReadResult::Batch(batch)) = journal.read(ReadQuery { after: Some(EventCursor { journal_id: window.journal_id, seq: Seq::ZERO }), limit: 256, wait_ms: 0 }, deadline)
                            && batch.events.iter().any(|event| event.kind == kind) {
                                let _ = sent.send(());
                                break;
                            }
                        std::thread::yield_now();
                    }
                });
                // Deterministic fixture handshake while the real process
                // still owns its publisher, before optional exit grace.
                received.recv_timeout(crate::support::HANDSHAKE_TIMEOUT).unwrap();
            }
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

struct NoRemoteProcesses;
impl ProcessRunner for NoRemoteProcesses {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == std::ffi::OsStr::new("/usr/bin/git") {
            use std::os::unix::process::ExitStatusExt;
            // Cancellation may try to release a base in a local project.
            // This fixture intentionally has no project or transfer repo.
            return Ok(ProcessResult { status: std::process::ExitStatus::from_raw(256), stdout: Vec::new(), stderr: Vec::new() });
        }
        panic!("fixture must not invoke a remote process: {:?}", request.program);
    }
}

fn isolated(root: &Path) -> (RuntimeContext, PathLayout, BTreeMap<OsString, OsString>) {
    let home = root.join("home");
    let environment = BTreeMap::from([
        (OsString::from("HOME"), home.as_os_str().to_owned()),
        (OsString::from("XDG_STATE_HOME"), root.join("state").into_os_string()),
        (OsString::from("XDG_CONFIG_HOME"), root.join("config").into_os_string()),
        (OsString::from("XDG_CACHE_HOME"), root.join("cache").into_os_string()),
        (OsString::from("XDG_DATA_HOME"), root.join("data").into_os_string()),
    ]);
    for value in environment.values() {
        crate::support::create_directory(PathBuf::from(value));
    }
    let paths = PathLayout::discover(None, &environment, &home).unwrap();
    (RuntimeContext::isolated(environment.clone(), home, root.to_owned()), paths, environment)
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
        Self { _temporary: temporary, root, paths, environment }
    }

    fn start_controller_leader(&mut self) {
        let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
        command.envs(&self.environment).current_dir(&self.root)
            .args(["controller", "run"]).stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = crate::controller_process::OwnedChild::spawn(&mut command);
        let stdout = child.take_stdout();
        let (sent, received) = mpsc::channel();
        std::thread::spawn(move || {
            let mut line = String::new();
            BufReader::new(stdout).read_line(&mut line).unwrap();
            sent.send(line).unwrap();
        });
        assert_eq!(received.recv_timeout(crate::support::HANDSHAKE_TIMEOUT).unwrap().trim(), "controller leader acquired");
        unsafe { libc::kill(child.id() as i32, libc::SIGTERM); }
        assert!(child.wait_timeout(crate::support::HANDSHAKE_TIMEOUT).unwrap().success());
    }

    fn spawn_detached_runner_fixture(&mut self) {
        let store = ClientStateStore::open(&self.paths.state).unwrap();
        let record = task_record();
        let task = record.meta().task_id();
        let turn = record.status().turns()[0].turn_id();
        store.create_task(record).unwrap();
        store.write_turn_prompt(task, turn, "private fixture prompt").unwrap();
        let owner = SystemProcessInspector.identity_for_pid(std::process::id()).unwrap();
        let entry = QueueEntry::new(turn, store.client_id(), "a".repeat(64), "b".repeat(64),
            CommandSummary::argv(1).unwrap(), vec![], WorkerPreference::Automatic,
            QueueEntryKind::TaskTurn, None, owner, 100).unwrap();
        store.enqueue(entry).unwrap();
        // Seed a cancelled waiting row, as left by controller cancellation.
        // No leader/runner is alive while this fixture is prepared.
        let queue_path = self.paths.state.join("queue/state.json");
        let mut wire: serde_json::Value = serde_json::from_slice(&fs::read(&queue_path).unwrap()).unwrap();
        wire["entries"][0]["cancel_requested_at_millis"] = serde_json::json!(101);
        let snapshot: mac_worker::job::QueueSnapshot = serde_json::from_value(wire).unwrap();
        let mut encoded = serde_json::to_vec(&snapshot).unwrap();
        encoded.push(b'\n');
        fs::write(queue_path, encoded).unwrap();
        let _ = self.reexec("runner", Some((task, turn)));
        let saved = store.load_task(task).unwrap();
        assert_eq!(saved.status().state(), TaskState::Open);
        assert_eq!(saved.status().turns()[0].outcome(), Some(&mac_worker::task::TaskOutcome::Cancelled));
    }

    fn run_laptop_local_fixture(&mut self) {
        self.spawn_detached_runner_fixture();
    }

    fn reexec(&self, mode: &str, ids: Option<(TaskId, TurnId)>) -> std::process::Output {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.args(["--exact", &crate::support::libtest_name(module_path!(), "process_entry_fixture"), "--nocapture"])
            .env("EV_T8A_PROCESS_ROOT", &self.root).env("EV_T8A_PROCESS_MODE", mode);
        if let Some((task, turn)) = ids {
            command.env("EV_T8A_TASK", task.to_string()).env("EV_T8A_TURN", turn.to_string());
        }
        let output = command.output().unwrap();
        assert!(output.status.success(), "fixture failed: {} {}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        output
    }

    fn journal_exists(&self) -> bool {
        self.paths.controller_state_root().join("events").exists()
    }

    fn journal(&self) -> Arc<ControllerJournal> {
        ControllerJournal::open_existing(&self.paths, JournalOptions { runtime: Arc::new(ManualEventRuntime::new()) })
            .unwrap().expect("leader must initialize its host journal")
    }

    fn tail_committed_events(&self) -> Vec<WireEvent> {
        let journal = self.journal();
        let window = journal.window(Duration::from_secs(60)).unwrap();
        match journal.read(ReadQuery { after: Some(EventCursor { journal_id: window.journal_id, seq: Seq::ZERO }), limit: 256, wait_ms: 0 }, Duration::from_secs(60)).unwrap() {
            EventReadResult::Batch(batch) => batch.events,
            other => panic!("expected committed events: {other:?}"),
        }
    }
}

fn task_record() -> LocalTaskRecord {
    let meta = TaskMeta::new(TaskMetaInput {
        task_id: TaskId::generate(), run_id: None, project_id: "a".repeat(64), worktree_id: "b".repeat(64),
        agent: AgentKind::Codex, model: None, effort: None, policy: PermissionPolicy::Workspace,
        source: TaskSource::Local { wip: false, push_target: None }, publish: vec![PublishMode::Fetch],
        publish_branch: None, base_oid: "c".repeat(40).parse().unwrap(), limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never, env_profile: None,
        git_identity: GitIdentity::new("Fixture", "fixture@example.test").unwrap(),
        title: Some("private fixture title".into()), prompt: "private fixture prompt".into(), created_at_millis: 100,
    }).unwrap();
    let status = TaskStatus::new(TaskState::Active, None, None, false, Some(meta.base_oid().clone()), None,
        vec![], vec![], None, vec![TurnSummary::new(1, TurnId::generate(), None, None, None, false, Some(100), None)], 100).unwrap();
    LocalTaskRecord::new(meta, status, None, None, None, "d".repeat(64), None, true, None).unwrap()
}

#[test]
fn process_entry_fixture() {
    let Some(root) = std::env::var_os("EV_T8A_PROCESS_ROOT") else { return; };
    let (runtime, paths, _) = isolated(Path::new(&root));
    let mode = std::env::var("EV_T8A_PROCESS_MODE").unwrap();
    let mut input = Vec::new();
    let args = match mode.as_str() {
        "runner" => vec!["worker".into(), "runner".into(), std::env::var("EV_T8A_TASK").unwrap(), std::env::var("EV_T8A_TURN").unwrap()],
        "drain" => vec!["worker".into(), "controller".into(), "drain".into()],
        "rpc-drain" => {
            input = encode_json_frame(&serde_json::json!({"protocol_version":7,"request_id":"00000000000000000000000000000001","command":"controller.drain","body":{"drained":true}})).unwrap();
            vec!["worker".into(), "host".into(), "controller-rpc".into()]
        }
        _ => panic!("unknown fixture mode"),
    };
    let mut stdout = CommitAwareWriter { bytes: Vec::new(), wait_for: (mode != "runner").then(|| (paths.clone(), "controller.drained")) };
    let mut stderr = CommitAwareWriter { bytes: Vec::new(), wait_for: (mode == "runner").then(|| (paths, "turn.finished")) };
    let code = run_with_stdio_in_context(Cli::parse_from(args), &NoRemoteProcesses, &runtime, &mut Cursor::new(input), &mut stdout, &mut stderr);
    if mode == "runner" {
        assert_eq!(code, 64, "{} {}", String::from_utf8_lossy(&stdout.bytes), String::from_utf8_lossy(&stderr.bytes));
        assert!(String::from_utf8_lossy(&stderr.bytes).contains("TASK_CANCELLED"));
    } else {
        assert_eq!(code, 0, "{} {}", String::from_utf8_lossy(&stdout.bytes), String::from_utf8_lossy(&stderr.bytes));
    }
}

#[test]
fn child_attachment_uses_initialized_host_journal() {
    let mut controller = ProcessWiringHarness::controller();
    controller.start_controller_leader();
    controller.spawn_detached_runner_fixture();
    assert!(controller.tail_committed_events().iter().any(|event| event.kind == "turn.finished"));
    let mut laptop = ProcessWiringHarness::laptop_local();
    laptop.run_laptop_local_fixture();
    assert!(!laptop.journal_exists());
}

#[test]
fn laptop_local_never_initializes_or_emits() {
    let mut laptop = ProcessWiringHarness::laptop_local();
    laptop.run_laptop_local_fixture();
    assert!(!laptop.journal_exists());
    assert!(ControllerJournal::open_existing(&laptop.paths, JournalOptions { runtime: Arc::new(ManualEventRuntime::new()) }).unwrap().is_none());
}

#[test]
fn leader_restart_preserves_journal_id_and_epoch() {
    let mut controller = ProcessWiringHarness::controller();
    controller.start_controller_leader();
    let id = controller.journal().window(Duration::from_secs(60)).unwrap().journal_id;
    let epoch_path = controller.paths.controller_state_root().join("events/initialization.json");
    let epoch = fs::read(&epoch_path).unwrap();
    controller.start_controller_leader();
    assert_eq!(controller.journal().window(Duration::from_secs(60)).unwrap().journal_id, id);
    assert_eq!(fs::read(epoch_path).unwrap(), epoch);
}

#[test]
fn direct_drain_attaches_to_initialized_host_journal() {
    let mut controller = ProcessWiringHarness::controller();
    controller.start_controller_leader();
    let output = controller.reexec("drain", None);
    let events = controller.tail_committed_events();
    assert!(events.iter().any(|event| event.kind == "controller.drained"), "{events:?} {}", String::from_utf8_lossy(&output.stderr));
}

#[test]
fn rpc_drain_attaches_to_initialized_host_journal() {
    let mut controller = ProcessWiringHarness::controller();
    controller.start_controller_leader();
    let output = controller.reexec("rpc-drain", None);
    let events = controller.tail_committed_events();
    assert!(events.iter().any(|event| event.kind == "controller.drained"), "{events:?} {}", String::from_utf8_lossy(&output.stderr));
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
