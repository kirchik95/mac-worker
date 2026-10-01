//! Seeded existing-task evidence for enabled controller read routes.
//!
//! These tests do not exercise a real submit lifecycle. They plant one
//! legitimate `LocalTaskRecord` (and optional runner log) in an isolated
//! controller store, then query it through `host controller-rpc`.

use mac_worker::test_support::cli::from_parts;
use std::{
    collections::BTreeMap,
    ffi::OsString,
    io::{Cursor, Read, Write},
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    path::PathBuf,
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use clap::Parser;
use mac_worker::test_support::{
    agents::agent::{AgentKind, PermissionPolicy},
    cli::{Cli, Command as WorkerCommand, HostCommand},
    client_state::ClientStateStore,
    controller::{
        ControllerFault, ControllerReadReply, ControllerStore, ControllerTaskLogsResult,
        ControllerTaskStatusResult, canonical_request_sha256, decode_frame, encode_frame,
        encode_json_frame, protocol::MAX_FRAME_BYTES, serve_rpc_with_runtime,
    },
    core::{config::Config, error::WorkerError, paths::PathLayout, protocol::PROTOCOL_VERSION},
    host::{
        job::{JobId, MAX_LOG_CHUNK_BYTES},
        process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    },
    runtime::{RuntimeContext, run_with_stdio_in_context},
    task::{
        model::{
            ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskId, TaskLimits, TaskMeta,
            TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnId, TurnSummary,
            TurnTerminal,
        },
        store::{TaskDiffRequest, TaskDiffResponse},
        turn_log,
    },
    transfer::HostOperation,
};
use serde_json::{Value, json};
use uuid::Uuid;

const SEEDED_TASK: u128 = 0x018f_0f4a_6b5c_7d8e_9f00_1122_3344_5566;
const SEEDED_TURN: u128 = 0x018f_0f4a_6b5c_7d8e_9f00_1122_3344_5577;
const STATUS_REQUEST: &str = "018f0f4a6b5c7d8e9f00112233445588";

struct OwnedChild {
    child: Option<Child>,
}

impl OwnedChild {
    fn spawn(command: &mut Command) -> Self {
        Self {
            child: Some(command.spawn().unwrap()),
        }
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child
            .as_mut()
            .expect("controller child already reaped")
    }

    fn take_stdin(&mut self) -> std::process::ChildStdin {
        self.child_mut().stdin.take().expect("stdin pipe")
    }

    fn wait_timeout(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            let status = {
                let child = self.child.as_mut()?;
                child.try_wait().unwrap()
            };
            if let Some(status) = status {
                self.child = None;
                return Some(status);
            }
            if Instant::now() > deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn kill_and_reap(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        self.kill_and_reap();
    }
}

struct IsolatedHome {
    _temp: tempfile::TempDir,
    home: PathBuf,
    environment: BTreeMap<OsString, OsString>,
    runtime: RuntimeContext,
    paths: PathLayout,
}

impl IsolatedHome {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let home = root.join("home");
        let state_home = root.join("state");
        let cache_home = root.join("cache");
        let config_home = root.join("config");
        let data_home = root.join("data");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::create_dir_all(&state_home).unwrap();
        std::fs::create_dir_all(&cache_home).unwrap();
        std::fs::create_dir_all(&config_home).unwrap();
        std::fs::create_dir_all(&data_home).unwrap();
        let environment = BTreeMap::from([
            (OsString::from("HOME"), home.as_os_str().to_os_string()),
            (OsString::from("XDG_STATE_HOME"), state_home.into()),
            (OsString::from("XDG_CACHE_HOME"), cache_home.into()),
            (OsString::from("XDG_CONFIG_HOME"), config_home.into()),
            (OsString::from("XDG_DATA_HOME"), data_home.into()),
        ]);
        let paths = PathLayout::discover(None, &environment, &home).unwrap();
        Self {
            runtime: RuntimeContext::isolated(environment.clone(), home.clone(), root.clone()),
            paths,
            home,
            environment,
            _temp: temp,
        }
    }
}

struct SshToControllerRpc {
    worker_bin: PathBuf,
    environment: BTreeMap<OsString, OsString>,
}

impl ProcessRunner for SshToControllerRpc {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(
            request.program,
            OsString::from("/usr/bin/ssh"),
            "stand-in is worker host controller-rpc; /usr/bin/ssh is hardcoded"
        );
        let remote = request
            .args
            .last()
            .and_then(|arg| arg.to_str())
            .unwrap_or_default();
        assert!(
            remote.ends_with("host controller-rpc"),
            "unexpected remote command {remote}"
        );
        let mut command = Command::new(&self.worker_bin);
        command
            .args(["host", "controller-rpc"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env_clear();
        for (key, value) in &self.environment {
            command.env(key, value);
        }
        let mut child = command.spawn().map_err(WorkerError::Io)?;
        if let Some(stdin) = request.stdin.as_ref() {
            child
                .stdin
                .take()
                .unwrap()
                .write_all(stdin)
                .map_err(WorkerError::Io)?;
        }
        drop(child.stdin.take());
        let output = child.wait_with_output().map_err(WorkerError::Io)?;
        Ok(ProcessResult {
            status: output.status,
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

struct FailingSsh;

impl ProcessRunner for FailingSsh {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(request.program, OsString::from("/usr/bin/ssh"));
        Err(WorkerError::Unavailable(
            "CONTROLLER_UNAVAILABLE: injected ssh outage".into(),
        ))
    }
}

fn seeded_ids() -> (TaskId, TurnId) {
    (
        TaskId::new(Uuid::from_u128(SEEDED_TASK)),
        TurnId::new(Uuid::from_u128(SEEDED_TURN)),
    )
}

fn seeded_record(diff_stat: Option<&str>) -> LocalTaskRecord {
    seeded_record_with(
        diff_stat,
        TaskState::Open,
        Some(TaskOutcome::Done),
        Some(TurnTerminal::Succeeded),
        Some(TaskOutcome::Done),
    )
}

fn seeded_record_with(
    diff_stat: Option<&str>,
    state: TaskState,
    last_outcome: Option<TaskOutcome>,
    terminal: Option<TurnTerminal>,
    turn_outcome: Option<TaskOutcome>,
) -> LocalTaskRecord {
    let (task_id, turn_id) = seeded_ids();
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
        base_oid: "0123456789abcdef0123456789abcdef01234567".parse().unwrap(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Fixture", "fixture@example.test").unwrap(),
        title: Some("seeded status".into()),
        prompt: "seeded existing-task evidence".into(),
        created_at_millis: 1_700_000_000_000,
    })
    .unwrap();
    let status = TaskStatus::new(
        state,
        last_outcome,
        Some("mini-1".into()),
        false,
        Some(meta.base_oid().clone()),
        Some("seeded summary".into()),
        Vec::new(),
        vec!["README".into()],
        diff_stat.map(str::to_owned),
        vec![TurnSummary::new(
            1,
            JobId::new(Uuid::from_u128(SEEDED_TURN)),
            terminal,
            turn_outcome,
            Some(true),
            false,
            Some(1),
            Some(2),
        )],
        2,
    )
    .unwrap();
    let _ = turn_id;
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

fn seed_controller_task(paths: &PathLayout, diff_stat: Option<&str>) -> LocalTaskRecord {
    let record = seeded_record(diff_stat);
    ClientStateStore::open(&paths.state)
        .unwrap()
        .create_task(record.clone())
        .unwrap();
    record
}

fn seed_controller_task_with(
    paths: &PathLayout,
    state: TaskState,
    last_outcome: Option<TaskOutcome>,
    terminal: Option<TurnTerminal>,
    turn_outcome: Option<TaskOutcome>,
) -> LocalTaskRecord {
    let record = seeded_record_with(None, state, last_outcome, terminal, turn_outcome);
    ClientStateStore::open(&paths.state)
        .unwrap()
        .create_task(record.clone())
        .unwrap();
    record
}

fn seed_runner_log(paths: &PathLayout, bytes: &[u8]) {
    seed_runner_log_checkpoint(paths, bytes, Some(TaskOutcome::Done));
}

fn seed_active_runner_log(paths: &PathLayout, bytes: &[u8]) {
    seed_runner_log_checkpoint(paths, bytes, None);
}

struct LogWaitClock<'a> {
    elapsed: std::sync::Mutex<Duration>,
    sleeps: std::sync::Mutex<Vec<Duration>>,
    on_sleep: Box<dyn Fn(Duration) + Send + Sync + 'a>,
}

impl<'a> LogWaitClock<'a> {
    fn new(on_sleep: impl Fn(Duration) + Send + Sync + 'a) -> Self {
        Self {
            elapsed: Default::default(),
            sleeps: Default::default(),
            on_sleep: Box::new(on_sleep),
        }
    }

    fn elapsed(&self) -> Duration {
        *self.elapsed.lock().unwrap()
    }
}

impl mac_worker::test_support::transfer::ResolutionRuntime for LogWaitClock<'_> {
    fn monotonic_now(&self) -> Duration {
        self.elapsed()
    }

    fn sleep(&self, duration: Duration) {
        let elapsed = {
            let mut elapsed = self.elapsed.lock().unwrap();
            *elapsed += duration;
            *elapsed
        };
        self.sleeps.lock().unwrap().push(duration);
        (self.on_sleep)(elapsed);
    }
}

fn log_wait_fixture() -> IsolatedHome {
    let fixture = IsolatedHome::new();
    seed_controller_task_with(&fixture.paths, TaskState::Active, None, None, None);
    seed_active_runner_log(&fixture.paths, b"");
    fixture
}

fn log_wait_reply(
    fixture: &IsolatedHome,
    extra: Value,
    clock: &LogWaitClock<'_>,
) -> Result<ControllerTaskLogsResult, WorkerError> {
    let state = ClientStateStore::open(&fixture.paths.state).unwrap();
    let config =
        Config::parse("version=1\n[[workers]]\nname='mini-1'\nssh='test'\nslots=1\n").unwrap();
    struct NoProcess;
    impl ProcessRunner for NoProcess {
        fn run(&self, _: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            panic!("log long-poll must use only local read-only state");
        }
    }
    let client = mac_worker::test_support::task::client::TaskClient::new(
        &NoProcess,
        &config,
        &fixture.paths,
        &state,
        &mac_worker::test_support::task::turn_runner::DetachedRunnerExecutor,
    );
    let mut body =
        json!({"task_id": seeded_ids().0, "offset": 0, "follow": true, "raw": true, "limit": 64});
    body.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    let request = mac_worker::test_support::controller::parse_request(&serde_json::to_vec(&json!({
        "protocol_version": PROTOCOL_VERSION, "request_id": STATUS_REQUEST, "command": "task.logs", "body": body,
    })).unwrap()).unwrap();
    let reply = mac_worker::test_support::controller::read::logs_reply_with_runtime(
        &request, &client, clock,
    )?;
    reply.verify_envelope(&request).unwrap();
    mac_worker::test_support::controller::ControllerReadIdentity::verify_payload(
        reply.result(),
        &request,
    )
    .unwrap();
    Ok(reply.into_result())
}

#[test]
fn logs_wait_returns_when_bytes_arrive_without_holding_state_locks() {
    let fixture = log_wait_fixture();
    let clock = LogWaitClock::new(|elapsed| {
        if elapsed == Duration::from_millis(400) {
            // Taking the state lock while sleeping would deadlock this callback.
            let state = ClientStateStore::open(&fixture.paths.state).unwrap();
            let record = state.load_task(seeded_ids().0).unwrap();
            assert!(
                state
                    .update_task_if_current(&record, record.clone())
                    .unwrap()
            );
            seed_active_runner_log(&fixture.paths, b"arrived\n");
        }
    });
    let reply = log_wait_reply(&fixture, json!({"wait_ms": 20_000}), &clock).unwrap();
    assert_eq!(clock.elapsed(), Duration::from_millis(400));
    assert_eq!(reply.decode_bytes().unwrap(), b"arrived\n");
    assert_eq!(reply.turn_id(), seeded_ids().1);
    assert_eq!(request_row_count(&fixture.paths), 0);
}

#[test]
fn logs_wait_caps_requested_time_and_shortens_the_last_tick() {
    for (requested, expected) in [(450_u64, 450), (20_000, 20_000), (u64::MAX, 20_000)] {
        let fixture = log_wait_fixture();
        let clock = LogWaitClock::new(|_| {});
        let reply = log_wait_reply(&fixture, json!({"wait_ms": requested}), &clock).unwrap();
        assert_eq!(clock.elapsed(), Duration::from_millis(expected));
        assert!(
            clock
                .sleeps
                .lock()
                .unwrap()
                .iter()
                .all(|sleep| *sleep <= Duration::from_millis(200))
        );
        assert!(reply.exhausted());
        assert!(!reply.complete());
        assert!(reply.decode_bytes().unwrap().is_empty());
    }
}

#[test]
fn logs_wait_returns_on_log_completion_even_when_terminal_status_arrives_first() {
    for terminal_only in [false, true] {
        let fixture = log_wait_fixture();
        let clock = LogWaitClock::new(|elapsed| {
            if elapsed == Duration::from_millis(200) {
                if terminal_only {
                    let state = ClientStateStore::open(&fixture.paths.state).unwrap();
                    let old = state.load_task(seeded_ids().0).unwrap();
                    let terminal = seeded_record_with(
                        None,
                        TaskState::Lost,
                        Some(TaskOutcome::Lost),
                        Some(TurnTerminal::Lost),
                        Some(TaskOutcome::Lost),
                    );
                    assert!(state.update_task_if_current(&old, terminal).unwrap());
                } else {
                    seed_runner_log(&fixture.paths, b"");
                }
            }
            if terminal_only && elapsed == Duration::from_millis(600) {
                seed_runner_log_checkpoint(
                    &fixture.paths,
                    b"last bytes\n",
                    Some(TaskOutcome::Lost),
                );
            }
        });
        let reply = log_wait_reply(&fixture, json!({"wait_ms": 20_000}), &clock).unwrap();
        assert_eq!(
            clock.elapsed(),
            Duration::from_millis(if terminal_only { 600 } else { 200 })
        );
        assert!(
            reply.complete(),
            "only the durable checkpoint proves log completion"
        );
        if terminal_only {
            assert_eq!(reply.decode_bytes().unwrap(), b"last bytes\n");
        }
    }
}

#[test]
fn logs_wait_terminal_follow_remains_bounded_until_checkpoint_completion() {
    let controller = IsolatedHome::new();
    seed_controller_task_with(
        &controller.paths,
        TaskState::Lost,
        Some(TaskOutcome::Lost),
        Some(TurnTerminal::Lost),
        Some(TaskOutcome::Lost),
    );
    seed_active_runner_log(&controller.paths, b"");
    let clock = LogWaitClock::new(|_| {});
    struct ClockedRpc<'a> {
        controller: &'a IsolatedHome,
        clock: &'a LogWaitClock<'a>,
        calls: std::sync::Mutex<usize>,
    }
    impl ProcessRunner for ClockedRpc<'_> {
        fn run(&self, process: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            let request = mac_worker::test_support::controller::decode_request(
                process.stdin.as_ref().unwrap(),
            )?;
            let stdout =
                if mac_worker::test_support::controller::health_read::is_health_read(&request) {
                    mac_worker::test_support::controller::health_read::serve_health_read(
                        &request,
                        &self.controller.paths.controller_state_root(),
                    )?
                } else {
                    let call = {
                        let mut count = self.calls.lock().unwrap();
                        *count += 1;
                        *count
                    };
                    assert!(call <= 3, "terminal logs must eventually finish");
                    assert_eq!(request.body()["wait_ms"], 15_000);
                    if call == 3 {
                        seed_runner_log_checkpoint(
                            &self.controller.paths,
                            b"drained tail\n",
                            Some(TaskOutcome::Lost),
                        );
                    }
                    let state = ClientStateStore::open(&self.controller.paths.state)?;
                    let config =
                        Config::parse("version=1\n[controller]\nenabled=true\nssh='controller'\n")?;
                    let client = mac_worker::test_support::task::client::TaskClient::new(
                        &FailingSsh,
                        &config,
                        &self.controller.paths,
                        &state,
                        &mac_worker::test_support::task::turn_runner::DetachedRunnerExecutor,
                    );
                    let reply =
                        mac_worker::test_support::controller::read::logs_reply_with_runtime(
                            &request, &client, self.clock,
                        )?;
                    assert_eq!(
                        self.clock.elapsed(),
                        Duration::from_secs((call.min(2) * 15) as u64),
                        "a terminal incomplete checkpoint must not cause immediate SSH repolling"
                    );
                    assert_eq!(reply.result().complete(), call == 3);
                    encode_json_frame(&reply)?
                };
            Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout,
                stderr: Vec::new(),
            })
        }
    }
    let rpc = ClockedRpc {
        controller: &controller,
        clock: &clock,
        calls: std::sync::Mutex::new(0),
    };
    let laptop = IsolatedHome::new();
    write_enabled_config(&laptop.paths);
    let (exit, stdout, stderr) = run_laptop_task_logs(
        &laptop,
        &rpc,
        &[&seeded_ids().0.to_string(), "--follow", "--raw"],
    );
    assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&stderr));
    assert_eq!(stdout, b"drained tail\n");
    assert_eq!(*rpc.calls.lock().unwrap(), 3);
    assert!(!laptop_task_store_exists(&laptop.paths));
}

#[test]
fn logs_wait_keeps_immediate_reads_and_returns_available_bytes_immediately() {
    for extra in [
        json!({}),
        json!({"wait_ms": 0}),
        json!({"wait_ms": 20_000, "follow": false}),
    ] {
        let fixture = log_wait_fixture();
        let clock = LogWaitClock::new(|_| panic!("this request must not sleep"));
        let reply = log_wait_reply(&fixture, extra, &clock).unwrap();
        assert!(reply.decode_bytes().unwrap().is_empty());
    }
    for bytes in [b"short\n".as_slice(), &[b'a'; 100]] {
        let fixture = log_wait_fixture();
        seed_active_runner_log(&fixture.paths, bytes);
        let clock = LogWaitClock::new(|_| panic!("available bytes must return immediately"));
        let reply = log_wait_reply(&fixture, json!({"wait_ms": 20_000}), &clock).unwrap();
        assert_eq!(reply.decode_bytes().unwrap(), bytes[..bytes.len().min(64)]);
    }
}

#[test]
fn logs_wait_rejects_unknown_keys_and_invalid_wait_values() {
    let fixture = log_wait_fixture();
    for extra in [
        json!({"future_wait": 1}),
        json!({"wait_ms": -1}),
        json!({"wait_ms": "100"}),
        json!({"wait_ms": 1.5}),
    ] {
        let clock = LogWaitClock::new(|_| panic!("invalid request must not sleep"));
        let expected = if extra.get("future_wait").is_some() {
            "INVALID_REQUEST"
        } else {
            "CONTROLLER_TRANSPORT"
        };
        assert_eq!(
            log_wait_reply(&fixture, extra, &clock)
                .unwrap_err()
                .public_code(),
            expected
        );
    }
}

fn seed_runner_log_checkpoint(paths: &PathLayout, bytes: &[u8], completion: Option<TaskOutcome>) {
    let (task_id, turn_id) = seeded_ids();
    let store = ClientStateStore::open(&paths.state).unwrap();
    let mut log = store.open_runner_log(task_id, turn_id).unwrap();
    log.write_all(bytes).unwrap();
    drop(log);
    let dir = paths.state.join("runners").join(task_id.to_string());
    let len = std::fs::metadata(dir.join(format!("{turn_id}.log")))
        .unwrap()
        .len();
    let path = dir.join(format!("{turn_id}.checkpoint.json"));
    let mut committed = json!({
        "offsets": [0, 0],
        "len": len,
        "accepted": true
    });
    if let Some(outcome) = completion {
        committed["completion"] = json!({"outcome": outcome, "drained": true});
    }
    std::fs::write(
        &path,
        serde_json::to_vec(&json!({
            "version": 1,
            "task_id": task_id,
            "turn_id": turn_id,
            "committed": committed,
            "pending": null
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

fn write_enabled_config(paths: &PathLayout) {
    std::fs::create_dir_all(paths.config.parent().unwrap()).unwrap();
    std::fs::write(
        &paths.config,
        r#"
version = 1
[controller]
enabled = true
ssh = "user@always-on-host"
"#,
    )
    .unwrap();
}

fn write_enabled_config_with_worker(paths: &PathLayout) {
    std::fs::create_dir_all(paths.config.parent().unwrap()).unwrap();
    std::fs::write(
        &paths.config,
        r#"
version = 1
[[workers]]
name = "mini-1"
ssh = "mini-1.example"
slots = 1
[controller]
enabled = true
ssh = "user@always-on-host"
"#,
    )
    .unwrap();
}

const WORKER_PATCH: &str = "\
diff --git a/README b/README
--- a/README
+++ b/README
@@ -1 +1,2 @@
 hello
+world
";
const WORKER_STAT: &str = " README | 1 +\n 1 file changed, 1 insertion(+)\n";

fn frame_json(value: &Value) -> Vec<u8> {
    encode_frame(&serde_json::to_vec(value).unwrap()).unwrap()
}

fn status_request(task_id: TaskId) -> Value {
    json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": STATUS_REQUEST,
        "command": "task.status",
        "body": { "task_id": task_id.to_string() }
    })
}

fn host_controller_rpc_cli() -> Cli {
    from_parts(
        None,
        false,
        WorkerCommand::Host {
            command: HostCommand::ControllerRpc,
        },
    )
}

fn request_row_count(paths: &PathLayout) -> usize {
    let root = paths.controller_state_root();
    if !root.is_dir() {
        return 0;
    }
    std::fs::read_dir(root)
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .ok()
                .and_then(|entry| entry.file_name().to_str().map(str::to_owned))
                .is_some_and(|name| name.starts_with("req-") && name.ends_with(".json"))
        })
        .count()
}

fn laptop_task_store_exists(paths: &PathLayout) -> bool {
    paths.state.exists()
}

fn decode_read<T: serde::de::DeserializeOwned>(stdout: &[u8]) -> ControllerReadReply<T> {
    serde_json::from_slice(decode_frame(stdout).unwrap()).unwrap()
}

/// Separate `worker host controller-rpc` process. Seeded status, not submit.
#[test]
fn seeded_status_round_trips_through_a_host_controller_rpc_process() {
    let controller = IsolatedHome::new();
    let record = seed_controller_task(&controller.paths, Some(" README | 1 +"));
    write_enabled_config(&controller.paths);

    let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
    command
        .args(["host", "controller-rpc"])
        .env("HOME", &controller.home)
        .env(
            "XDG_STATE_HOME",
            controller
                .environment
                .get(&OsString::from("XDG_STATE_HOME"))
                .unwrap(),
        )
        .env(
            "XDG_CACHE_HOME",
            controller
                .environment
                .get(&OsString::from("XDG_CACHE_HOME"))
                .unwrap(),
        )
        .env(
            "XDG_CONFIG_HOME",
            controller
                .environment
                .get(&OsString::from("XDG_CONFIG_HOME"))
                .unwrap(),
        )
        .env(
            "XDG_DATA_HOME",
            controller
                .environment
                .get(&OsString::from("XDG_DATA_HOME"))
                .unwrap(),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = OwnedChild::spawn(&mut command);
    let mut stdin = child.take_stdin();
    stdin
        .write_all(&frame_json(&status_request(record.meta().task_id())))
        .unwrap();
    drop(stdin);
    let mut stdout_pipe = child.child_mut().stdout.take().expect("stdout");
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stdout_pipe.read_to_end(&mut buf);
        let _ = tx.send(buf);
    });
    let status = child
        .wait_timeout(Duration::from_secs(15))
        .unwrap_or_else(|| {
            child.kill_and_reap();
            panic!("host controller-rpc did not exit");
        });
    let stdout = rx.recv_timeout(crate::support::HANDSHAKE_TIMEOUT).unwrap();
    assert!(status.success(), "stderr process failed: {status:?}");
    let reply: ControllerReadReply<ControllerTaskStatusResult> = decode_read(&stdout);
    assert_eq!(reply.command(), "task.status");
    assert_eq!(reply.request_id(), STATUS_REQUEST);
    assert_eq!(reply.protocol_version(), PROTOCOL_VERSION);
    let envelope: Value = serde_json::from_slice(decode_frame(&stdout).unwrap()).unwrap();
    assert!(envelope.get("turn_id").is_none());
    assert_ne!(
        envelope.get("status").and_then(Value::as_str),
        Some("acked")
    );
    let report = reply.into_result().into_report();
    assert_eq!(report.task_id(), record.meta().task_id());
    assert_eq!(report.status().state(), TaskState::Open);
    assert_eq!(report.status().summary(), Some("seeded summary"));
    assert_eq!(report.status().worker(), Some("mini-1"));
    assert_eq!(request_row_count(&controller.paths), 0);
    let reloaded = ClientStateStore::open(&controller.paths.state)
        .unwrap()
        .load_task(record.meta().task_id())
        .unwrap();
    assert_eq!(reloaded, record);
}

#[test]
fn enabled_laptop_status_uses_ssh_stub_and_does_not_open_laptop_store() {
    let controller = IsolatedHome::new();
    let laptop = IsolatedHome::new();
    let record = seed_controller_task(&controller.paths, None);
    write_enabled_config(&controller.paths);
    write_enabled_config(&laptop.paths);
    let runner = SshToControllerRpc {
        worker_bin: PathBuf::from(env!("CARGO_BIN_EXE_worker")),
        environment: controller.environment.clone(),
    };
    let cli = Cli::try_parse_from([
        "worker",
        "task",
        "status",
        &record.meta().task_id().to_string(),
    ])
    .unwrap();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        cli,
        &runner,
        &laptop.runtime,
        &mut Cursor::new(Vec::new()),
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0, "stderr={}", String::from_utf8_lossy(&stderr));
    // The last seeded turn finished `done`; status prints its outcome and,
    // for this legacy record without a stored policy, `ask`.
    let expected = format!(
        "task {}: open (mini-1)\noutcome: done\nquestions policy: ask\n",
        record.meta().task_id()
    );
    assert_eq!(String::from_utf8(stdout).unwrap(), expected);
    assert!(!laptop_task_store_exists(&laptop.paths));
    assert_eq!(request_row_count(&controller.paths), 0);
    assert_eq!(request_row_count(&laptop.paths), 0);
}

#[test]
fn enabled_status_outage_does_not_create_laptop_task_files() {
    let laptop = IsolatedHome::new();
    write_enabled_config(&laptop.paths);
    let (task_id, _) = seeded_ids();
    let cli = Cli::try_parse_from(["worker", "task", "status", &task_id.to_string()]).unwrap();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        cli,
        &FailingSsh,
        &laptop.runtime,
        &mut Cursor::new(Vec::new()),
        &mut stdout,
        &mut stderr,
    );
    assert_ne!(exit, 0);
    let stderr = String::from_utf8_lossy(&stderr);
    assert!(stderr.contains("CONTROLLER_UNAVAILABLE"), "stderr={stderr}");
    assert!(!laptop_task_store_exists(&laptop.paths));
}

#[test]
fn incompatible_version_and_oversize_fail_without_a_task_store() {
    let controller = IsolatedHome::new();
    write_enabled_config(&controller.paths);
    let cli = host_controller_rpc_cli();
    let mut v6 = status_request(seeded_ids().0);
    v6["protocol_version"] = json!(6);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        cli,
        &SystemProcessRunner,
        &controller.runtime,
        &mut Cursor::new(frame_json(&v6)),
        &mut stdout,
        &mut stderr,
    );
    assert_ne!(exit, 0);
    let payload = decode_frame(&stdout).unwrap();
    let error: Value = serde_json::from_slice(payload).unwrap();
    assert_eq!(error["error"]["code"], "INCOMPATIBLE_PROTOCOL");
    assert!(!controller.paths.state.exists());
    assert_eq!(request_row_count(&controller.paths), 0);

    let mut huge = (MAX_FRAME_BYTES as u32 + 1).to_be_bytes().to_vec();
    huge.extend_from_slice(&[b'x'; 8]);
    let fresh = IsolatedHome::new();
    write_enabled_config(&fresh.paths);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        host_controller_rpc_cli(),
        &SystemProcessRunner,
        &fresh.runtime,
        &mut Cursor::new(huge),
        &mut stdout,
        &mut stderr,
    );
    assert_ne!(exit, 0);
    assert!(!fresh.paths.state.exists());
}

#[test]
fn seeded_list_logs_diff_and_result_use_typed_task_client_payloads() {
    let controller = IsolatedHome::new();
    let laptop = IsolatedHome::new();
    let record = seed_controller_task(&controller.paths, Some(" README | 1 +"));
    seed_runner_log(&controller.paths, b"first chunk line\nsecond chunk line\n");
    write_enabled_config(&controller.paths);
    write_enabled_config(&laptop.paths);
    let runner = SshToControllerRpc {
        worker_bin: PathBuf::from(env!("CARGO_BIN_EXE_worker")),
        environment: controller.environment.clone(),
    };
    let task_id = record.meta().task_id().to_string();

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        Cli::try_parse_from(["worker", "task", "list"]).unwrap(),
        &runner,
        &laptop.runtime,
        &mut Cursor::new(Vec::new()),
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0, "list stderr={}", String::from_utf8_lossy(&stderr));
    assert_eq!(
        String::from_utf8(stdout).unwrap(),
        format!("{task_id}: open (mini-1)\n")
    );

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        Cli::try_parse_from(["worker", "task", "result", &task_id]).unwrap(),
        &runner,
        &laptop.runtime,
        &mut Cursor::new(Vec::new()),
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(
        exit,
        0,
        "result stderr={}",
        String::from_utf8_lossy(&stderr)
    );
    let text = String::from_utf8(stdout).unwrap();
    assert!(text.contains(&format!("task {task_id}: open")));
    assert!(text.contains("summary: seeded summary"));
    assert!(text.contains(&format!("branch: task/{task_id}")));

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        Cli::try_parse_from(["worker", "task", "logs", "--raw", &task_id]).unwrap(),
        &runner,
        &laptop.runtime,
        &mut Cursor::new(Vec::new()),
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0, "logs stderr={}", String::from_utf8_lossy(&stderr));
    assert_eq!(
        String::from_utf8(stdout).unwrap(),
        "first chunk line\nsecond chunk line\n"
    );

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let first = json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": "018f0f4a6b5c7d8e9f00112233445599",
        "command": "task.logs",
        "body": {
            "task_id": task_id,
            "offset": 0,
            "limit": 12,
            "raw": true
        }
    });
    let exit = run_with_stdio_in_context(
        host_controller_rpc_cli(),
        &SystemProcessRunner,
        &controller.runtime,
        &mut Cursor::new(frame_json(&first)),
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0, "chunk stderr={}", String::from_utf8_lossy(&stderr));
    let chunk: ControllerReadReply<ControllerTaskLogsResult> = decode_read(&stdout);
    assert!(!chunk.result().exhausted());
    assert!(chunk.result().complete());
    assert_eq!(
        String::from_utf8(chunk.result().decode_bytes().unwrap()).unwrap(),
        "first chunk "
    );
    let next = chunk.result().next_offset();
    let mut stdout = Vec::new();
    let second = json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": "018f0f4a6b5c7d8e9f001122334455aa",
        "command": "task.logs",
        "body": {
            "task_id": task_id,
            "offset": next,
            "limit": 64,
            "raw": true
        }
    });
    let exit = run_with_stdio_in_context(
        host_controller_rpc_cli(),
        &SystemProcessRunner,
        &controller.runtime,
        &mut Cursor::new(frame_json(&second)),
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0);
    let chunk: ControllerReadReply<ControllerTaskLogsResult> = decode_read(&stdout);
    assert!(chunk.result().exhausted());
    assert_eq!(
        String::from_utf8(chunk.result().decode_bytes().unwrap()).unwrap(),
        "line\nsecond chunk line\n"
    );
    assert_eq!(request_row_count(&controller.paths), 0);
    assert!(!laptop_task_store_exists(&laptop.paths));
}

#[test]
fn ordinary_local_status_still_reads_the_laptop_store_when_controller_is_disabled() {
    let local = IsolatedHome::new();
    let record = seed_controller_task(&local.paths, None);
    std::fs::create_dir_all(local.paths.config.parent().unwrap()).unwrap();
    std::fs::write(
        &local.paths.config,
        r#"
version = 1
[[workers]]
name = "local-1"
ssh = "unused-host"
slots = 1
"#,
    )
    .unwrap();
    let cli = Cli::try_parse_from([
        "worker",
        "task",
        "status",
        &record.meta().task_id().to_string(),
    ])
    .unwrap();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        cli,
        &SystemProcessRunner,
        &local.runtime,
        &mut Cursor::new(Vec::new()),
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0, "stderr={}", String::from_utf8_lossy(&stderr));
    assert_eq!(
        String::from_utf8(stdout).unwrap(),
        format!(
            "task {}: open (mini-1)\noutcome: done\nquestions policy: ask\n",
            record.meta().task_id()
        )
    );
}

#[test]
fn seeded_status_does_not_allocate_a_durable_request_row() {
    let controller = IsolatedHome::new();
    let record = seed_controller_task(&controller.paths, None);
    write_enabled_config(&controller.paths);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        host_controller_rpc_cli(),
        &SystemProcessRunner,
        &controller.runtime,
        &mut Cursor::new(frame_json(&status_request(record.meta().task_id()))),
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0, "stderr={}", String::from_utf8_lossy(&stderr));
    assert_eq!(request_row_count(&controller.paths), 0);
    assert!(
        ControllerStore::open(&controller.paths.controller_state_root())
            .unwrap()
            .load(STATUS_REQUEST)
            .unwrap()
            .is_none()
    );
}

struct WorkerTaskDiffStub;

impl ProcessRunner for WorkerTaskDiffStub {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(
            request.program,
            OsString::from("/usr/bin/ssh"),
            "worker stand-in; /usr/bin/ssh is hardcoded"
        );
        let remote = request
            .args
            .last()
            .and_then(|arg| arg.to_str())
            .unwrap_or_default();
        assert_eq!(
            remote,
            HostOperation::TaskDiff.command(),
            "unexpected worker command {remote}; no real named host"
        );
        let req: TaskDiffRequest =
            serde_json::from_slice(request.stdin.as_ref().expect("task-diff stdin")).unwrap();
        let text = if req.stat() {
            WORKER_STAT.to_owned()
        } else {
            WORKER_PATCH.to_owned()
        };
        let stdout = serde_json::to_vec(&TaskDiffResponse::new(text, false)).unwrap();
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout,
            stderr: Vec::new(),
        })
    }
}

struct IsolatedControllerTransport {
    paths: PathLayout,
    config: Config,
}

impl ProcessRunner for IsolatedControllerTransport {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(
            request.program,
            OsString::from("/usr/bin/ssh"),
            "stand-in is in-process controller-rpc; /usr/bin/ssh is hardcoded"
        );
        let remote = request
            .args
            .last()
            .and_then(|arg| arg.to_str())
            .unwrap_or_default();
        assert!(
            remote.ends_with("host controller-rpc"),
            "unexpected remote command {remote}"
        );
        let mut stdout = Vec::new();
        let mut stdin = Cursor::new(request.stdin.clone().unwrap_or_default());
        serve_rpc_with_runtime(
            &self.paths,
            &self.config,
            &WorkerTaskDiffStub,
            &mut stdin,
            &mut stdout,
            ControllerFault::None,
        )
        .unwrap();
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout,
            stderr: Vec::new(),
        })
    }
}

struct MutatingSsh {
    mutate: fn(&mut serde_json::Value),
}

impl ProcessRunner for MutatingSsh {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(request.program, OsString::from("/usr/bin/ssh"));
        let payload = decode_frame(request.stdin.as_ref().unwrap()).unwrap();
        let parsed: Value = serde_json::from_slice(payload).unwrap();
        let command = parsed["command"].as_str().unwrap();
        let request_id = parsed["request_id"].as_str().unwrap();
        let body = parsed["body"].clone();
        let digest = canonical_request_sha256(PROTOCOL_VERSION, command, &body).unwrap();
        let mut reply = json!({
            "protocol_version": PROTOCOL_VERSION,
            "command": command,
            "request_id": request_id,
            "payload_sha256": digest,
            "result": {
                "task_id": body.get("task_id").cloned().unwrap_or(json!("018f0f4a6b5c7d8e9f00112233445566")),
                "stat": body.get("stat").cloned().unwrap_or(json!(false)),
                "text": "patch\n"
            }
        });
        (self.mutate)(&mut reply);
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: encode_json_frame(&reply).unwrap(),
            stderr: Vec::new(),
        })
    }
}

#[test]
fn seeded_diff_returns_actual_patch_and_stat_from_worker_task_diff() {
    let controller = IsolatedHome::new();
    let laptop = IsolatedHome::new();
    let record = seed_controller_task(&controller.paths, Some(" README | 1 +"));
    write_enabled_config_with_worker(&controller.paths);
    write_enabled_config(&laptop.paths);
    let runner = IsolatedControllerTransport {
        paths: PathLayout {
            config: controller.paths.config.clone(),
            state: controller.paths.state.clone(),
            cache: controller.paths.cache.clone(),
            data: controller.paths.data.clone(),
        },
        config: Config::load(&controller.paths.config).unwrap(),
    };
    let task_id = record.meta().task_id().to_string();

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        Cli::try_parse_from(["worker", "task", "diff", &task_id]).unwrap(),
        &runner,
        &laptop.runtime,
        &mut Cursor::new(Vec::new()),
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0, "patch stderr={}", String::from_utf8_lossy(&stderr));
    assert_eq!(String::from_utf8(stdout).unwrap(), WORKER_PATCH);
    assert!(
        !WORKER_PATCH.contains("file changed"),
        "patch evidence must not be a stat placeholder"
    );

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        Cli::try_parse_from(["worker", "task", "diff", "--stat", &task_id]).unwrap(),
        &runner,
        &laptop.runtime,
        &mut Cursor::new(Vec::new()),
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0, "stat stderr={}", String::from_utf8_lossy(&stderr));
    assert_eq!(String::from_utf8(stdout).unwrap(), WORKER_STAT);
    assert!(!laptop_task_store_exists(&laptop.paths));
}

#[test]
fn seeded_long_line_and_split_records_make_bounded_offset_progress() {
    let controller = IsolatedHome::new();
    seed_controller_task(&controller.paths, None);
    let mut long = vec![b'x'; MAX_LOG_CHUNK_BYTES + 16];
    long.extend_from_slice(b"tail\n");
    seed_runner_log(&controller.paths, &long);
    write_enabled_config(&controller.paths);
    let task_id = seeded_ids().0;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let first = json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": "018f0f4a6b5c7d8e9f001122334455b1",
        "command": "task.logs",
        "body": {
            "task_id": task_id.to_string(),
            "offset": 0,
            "limit": MAX_LOG_CHUNK_BYTES,
            "raw": false,
            "follow": true
        }
    });
    let exit = run_with_stdio_in_context(
        host_controller_rpc_cli(),
        &SystemProcessRunner,
        &controller.runtime,
        &mut Cursor::new(frame_json(&first)),
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(
        exit,
        0,
        "long-line stderr={}",
        String::from_utf8_lossy(&stderr)
    );
    let chunk: ControllerReadReply<ControllerTaskLogsResult> = decode_read(&stdout);
    let next = chunk.result().next_offset();
    assert!(
        next > 0,
        "newline-free line longer than a chunk must still advance: next={next}"
    );
    assert!(!chunk.result().exhausted());
    assert!(chunk.result().complete());
    assert!(!chunk.result().decode_bytes().unwrap().is_empty());

    let mut stdout = Vec::new();
    let second = json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": "018f0f4a6b5c7d8e9f001122334455b2",
        "command": "task.logs",
        "body": {
            "task_id": task_id.to_string(),
            "offset": next,
            "limit": MAX_LOG_CHUNK_BYTES,
            "raw": false,
            "follow": true
        }
    });
    let exit = run_with_stdio_in_context(
        host_controller_rpc_cli(),
        &SystemProcessRunner,
        &controller.runtime,
        &mut Cursor::new(frame_json(&second)),
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0);
    let chunk: ControllerReadReply<ControllerTaskLogsResult> = decode_read(&stdout);
    assert!(chunk.result().exhausted());
    let rest = String::from_utf8(chunk.result().decode_bytes().unwrap()).unwrap();
    assert!(
        rest.ends_with("tail\n"),
        "complete partial tail must flush, got {rest:?}"
    );

    let split = IsolatedHome::new();
    seed_controller_task(&split.paths, None);
    seed_runner_log(&split.paths, b"ok\n\xe4\xb8\x96\n{\"type\":\"item\"}\n");
    write_enabled_config(&split.paths);
    let mut stdout = Vec::new();
    let utf8 = json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": "018f0f4a6b5c7d8e9f001122334455b3",
        "command": "task.logs",
        "body": {
            "task_id": task_id.to_string(),
            "offset": 0,
            "limit": 4,
            "raw": true
        }
    });
    let exit = run_with_stdio_in_context(
        host_controller_rpc_cli(),
        &SystemProcessRunner,
        &split.runtime,
        &mut Cursor::new(frame_json(&utf8)),
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0);
    let chunk: ControllerReadReply<ControllerTaskLogsResult> = decode_read(&stdout);
    assert_eq!(chunk.result().decode_bytes().unwrap(), b"ok\n\xe4");
    assert_eq!(chunk.result().next_offset(), 4);

    let mut stdout = Vec::new();
    let rest = json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": "018f0f4a6b5c7d8e9f001122334455b4",
        "command": "task.logs",
        "body": {
            "task_id": task_id.to_string(),
            "offset": 4,
            "limit": 32,
            "raw": true
        }
    });
    let exit = run_with_stdio_in_context(
        host_controller_rpc_cli(),
        &SystemProcessRunner,
        &split.runtime,
        &mut Cursor::new(frame_json(&rest)),
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0);
    let chunk: ControllerReadReply<ControllerTaskLogsResult> = decode_read(&stdout);
    assert_eq!(
        chunk.result().decode_bytes().unwrap(),
        b"\xb8\x96\n{\"type\":\"item\"}\n"
    );
    assert!(chunk.result().exhausted());
}

#[test]
fn mismatched_controller_replies_fail_without_a_laptop_task_store() {
    let laptop = IsolatedHome::new();
    write_enabled_config(&laptop.paths);
    let task_id = seeded_ids().0.to_string();
    let cases: [fn(&mut Value); 5] = [
        |reply| {
            reply["protocol_version"] = json!(6);
        },
        |reply| {
            reply["request_id"] = json!("018f0f4a6b5c7d8e9f00112233445577");
        },
        |reply| {
            reply["command"] = json!("task.status");
        },
        |reply| {
            reply["result"]["task_id"] = json!("018f0f4a6b5c7d8e9f00112233445577");
        },
        |reply| {
            reply["payload_sha256"] = json!("deadbeef");
        },
    ];
    for mutate in cases {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let exit = run_with_stdio_in_context(
            Cli::try_parse_from(["worker", "task", "diff", &task_id]).unwrap(),
            &MutatingSsh { mutate },
            &laptop.runtime,
            &mut Cursor::new(Vec::new()),
            &mut stdout,
            &mut stderr,
        );
        assert_ne!(
            exit,
            0,
            "mismatch should fail: {}",
            String::from_utf8_lossy(&stderr)
        );
        assert!(!laptop_task_store_exists(&laptop.paths));
    }

    struct NonprogressLogs;
    impl ProcessRunner for NonprogressLogs {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            let payload = decode_frame(request.stdin.as_ref().unwrap()).unwrap();
            let parsed: Value = serde_json::from_slice(payload).unwrap();
            let command = parsed["command"].as_str().unwrap();
            let request_id = parsed["request_id"].as_str().unwrap();
            let body = parsed["body"].clone();
            let digest = canonical_request_sha256(PROTOCOL_VERSION, command, &body).unwrap();
            let reply = json!({
                "protocol_version": PROTOCOL_VERSION,
                "command": command,
                "request_id": request_id,
                "payload_sha256": digest,
                "result": {
                    "task_id": body["task_id"],
                    "turn_id": "018f0f4a6b5c7d8e9f00112233445577",
                    "turn_number": 1,
                    "agent": "codex",
                    "offset": body["offset"],
                    "next_offset": body["offset"],
                    "exhausted": false,
                    "complete": true,
                    "raw": true,
                    "bytes_base64": ""
                }
            });
            Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout: encode_json_frame(&reply).unwrap(),
                stderr: Vec::new(),
            })
        }
    }
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        Cli::try_parse_from(["worker", "task", "logs", "--raw", &task_id]).unwrap(),
        &NonprogressLogs,
        &laptop.runtime,
        &mut Cursor::new(Vec::new()),
        &mut stdout,
        &mut stderr,
    );
    assert_ne!(exit, 0, "non-progress logs must fail");
    assert!(!laptop_task_store_exists(&laptop.paths));
}

fn ordinary_rendered_log(bytes: &[u8]) -> Vec<u8> {
    let mut rendered = Vec::new();
    turn_log::render_agent_log(bytes, AgentKind::Codex, &mut rendered).unwrap();
    rendered
}

fn run_laptop_task(
    laptop: &IsolatedHome,
    runner: &dyn ProcessRunner,
    args: &[&str],
) -> (u8, Vec<u8>, Vec<u8>) {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut argv = vec!["worker", "task"];
    argv.extend_from_slice(args);
    let exit = run_with_stdio_in_context(
        Cli::try_parse_from(argv).unwrap(),
        runner,
        &laptop.runtime,
        &mut Cursor::new(Vec::new()),
        &mut stdout,
        &mut stderr,
    );
    (exit, stdout, stderr)
}

fn run_laptop_task_logs(
    laptop: &IsolatedHome,
    runner: &dyn ProcessRunner,
    args: &[&str],
) -> (u8, Vec<u8>, Vec<u8>) {
    let mut argv = vec!["logs"];
    argv.extend_from_slice(args);
    run_laptop_task(laptop, runner, &argv)
}

fn assert_missing_log_not_missing_task(stderr: &[u8]) {
    let stderr = String::from_utf8_lossy(stderr);
    assert!(
        !stderr.contains("TASK_NOT_FOUND"),
        "existing task without a runner log must not look absent: {stderr}"
    );
    assert!(
        !stderr.contains("not present in controller state"),
        "missing log must keep the IO error: {stderr}"
    );
    assert!(
        stderr.contains("HOST_IO"),
        "missing runner log should stay a host IO error: {stderr}"
    );
}

fn ssh_to_controller(controller: &IsolatedHome) -> SshToControllerRpc {
    SshToControllerRpc {
        worker_bin: PathBuf::from(env!("CARGO_BIN_EXE_worker")),
        environment: controller.environment.clone(),
    }
}

#[test]
fn seeded_completed_one_newline_plus_tail_is_emitted_once() {
    let controller = IsolatedHome::new();
    let laptop = IsolatedHome::new();
    let record = seed_controller_task(&controller.paths, None);
    seed_runner_log(&controller.paths, b"one\ntail");
    write_enabled_config(&controller.paths);
    write_enabled_config(&laptop.paths);
    let runner = ssh_to_controller(&controller);
    let task_id = record.meta().task_id().to_string();
    let expected = ordinary_rendered_log(b"one\ntail");
    assert_eq!(expected, b"one\ntail\n");

    for follow in [false, true] {
        let args = if follow {
            vec!["--follow", task_id.as_str()]
        } else {
            vec![task_id.as_str()]
        };
        let (exit, stdout, stderr) = run_laptop_task_logs(&laptop, &runner, &args);
        assert_eq!(exit, 0, "stderr={}", String::from_utf8_lossy(&stderr));
        assert_eq!(stdout, expected, "follow={follow}");
        assert_eq!(
            stdout
                .windows(expected.len())
                .filter(|w| *w == expected)
                .count(),
            1
        );
        assert!(!laptop_task_store_exists(&laptop.paths));
    }
}

#[test]
fn seeded_active_nonfollow_one_newline_plus_tail_matches_ordinary_rendering() {
    let controller = IsolatedHome::new();
    let laptop = IsolatedHome::new();
    let record = seed_controller_task(&controller.paths, None);
    seed_active_runner_log(&controller.paths, b"one\ntail");
    write_enabled_config(&controller.paths);
    write_enabled_config(&laptop.paths);
    let runner = ssh_to_controller(&controller);
    let task_id = record.meta().task_id().to_string();
    let expected = ordinary_rendered_log(b"one\ntail");
    let (exit, stdout, stderr) = run_laptop_task_logs(&laptop, &runner, &[&task_id]);
    assert_eq!(exit, 0, "stderr={}", String::from_utf8_lossy(&stderr));
    assert_eq!(stdout, expected);
    assert!(!laptop_task_store_exists(&laptop.paths));
}

#[test]
fn seeded_missing_nonfollow_log_does_not_succeed_empty() {
    let controller = IsolatedHome::new();
    let laptop = IsolatedHome::new();
    let record = seed_controller_task(&controller.paths, None);
    write_enabled_config(&controller.paths);
    write_enabled_config(&laptop.paths);
    let runner = ssh_to_controller(&controller);
    let task_id = record.meta().task_id().to_string();
    let (exit, stdout, stderr) = run_laptop_task_logs(&laptop, &runner, &[&task_id]);
    assert_ne!(exit, 0, "stderr={}", String::from_utf8_lossy(&stderr));
    assert!(
        stdout.is_empty(),
        "stdout={}",
        String::from_utf8_lossy(&stdout)
    );
    assert_missing_log_not_missing_task(&stderr);
    assert!(!laptop_task_store_exists(&laptop.paths));
}

#[test]
fn seeded_missing_task_reads_are_task_not_found() {
    // `task logs` and `task wait` on an unknown task stay TASK_NOT_FOUND: no
    // laptop task store appears, the error never looks like a missing log
    // file, and no durable request row is allocated.
    for command in ["logs", "wait"] {
        let controller = IsolatedHome::new();
        let laptop = IsolatedHome::new();
        write_enabled_config(&controller.paths);
        write_enabled_config(&laptop.paths);
        let runner = ssh_to_controller(&controller);
        let missing =
            TaskId::new(Uuid::from_u128(0x018f_0f4a_6b5c_7d8e_9f00_dead_beef_0001)).to_string();
        let (exit, stdout, stderr) = match command {
            "logs" => run_laptop_task_logs(&laptop, &runner, &[&missing]),
            _ => run_laptop_task(&laptop, &runner, &["wait", "--task-id", missing.as_str()]),
        };
        assert_ne!(
            exit,
            0,
            "{command}: stderr={}",
            String::from_utf8_lossy(&stderr)
        );
        assert!(
            stdout.is_empty(),
            "{command}: stdout={}",
            String::from_utf8_lossy(&stdout)
        );
        let stderr = String::from_utf8_lossy(&stderr);
        assert!(
            stderr.contains("TASK_NOT_FOUND"),
            "missing {command} must stay TASK_NOT_FOUND: {stderr}"
        );
        assert!(
            !stderr.contains("HOST_IO"),
            "missing task must not look like a missing log: {stderr}"
        );
        assert!(!laptop_task_store_exists(&laptop.paths), "{command}");
        assert_eq!(request_row_count(&controller.paths), 0, "{command}");
    }
}

#[test]
fn seeded_queued_or_active_missing_log_is_not_task_not_found() {
    for state in [TaskState::Queued, TaskState::Active] {
        let controller = IsolatedHome::new();
        let laptop = IsolatedHome::new();
        let record = seed_controller_task_with(&controller.paths, state, None, None, None);
        write_enabled_config(&controller.paths);
        write_enabled_config(&laptop.paths);
        let runner = ssh_to_controller(&controller);
        let task_id = record.meta().task_id().to_string();
        let (exit, stdout, stderr) = run_laptop_task_logs(&laptop, &runner, &[&task_id]);
        assert_ne!(
            exit,
            0,
            "state={state:?} stderr={}",
            String::from_utf8_lossy(&stderr)
        );
        assert!(
            stdout.is_empty(),
            "state={state:?} stdout={}",
            String::from_utf8_lossy(&stdout)
        );
        assert_missing_log_not_missing_task(&stderr);
        assert!(!laptop_task_store_exists(&laptop.paths));
    }
}

#[test]
fn seeded_terminal_nolog_overlays_match_ordinary_and_raw_has_none() {
    for (outcome, overlay) in [
        (TaskOutcome::Cancelled, "turn 1 cancelled\n"),
        (
            TaskOutcome::failed("agent exited 1"),
            "turn 1 failed: agent exited 1\n",
        ),
        (TaskOutcome::TimedOut, "turn 1 timed_out\n"),
        (TaskOutcome::Lost, "turn 1 lost\n"),
    ] {
        let terminal = match &outcome {
            TaskOutcome::Failed { .. } => TurnTerminal::Failed,
            TaskOutcome::Cancelled => TurnTerminal::Cancelled,
            TaskOutcome::TimedOut => TurnTerminal::TimedOut,
            TaskOutcome::Lost => TurnTerminal::Lost,
            _ => unreachable!(),
        };
        let controller = IsolatedHome::new();
        let laptop = IsolatedHome::new();
        let record = seed_controller_task_with(
            &controller.paths,
            TaskState::Open,
            Some(outcome.clone()),
            Some(terminal),
            Some(outcome.clone()),
        );
        write_enabled_config(&controller.paths);
        write_enabled_config(&laptop.paths);
        let runner = ssh_to_controller(&controller);
        let task_id = record.meta().task_id().to_string();

        let (exit, stdout, stderr) = run_laptop_task_logs(&laptop, &runner, &[&task_id]);
        assert_eq!(
            exit,
            0,
            "overlay stderr={} outcome={outcome:?}",
            String::from_utf8_lossy(&stderr)
        );
        assert_eq!(stdout, overlay.as_bytes(), "outcome={outcome:?}");
        assert!(!laptop_task_store_exists(&laptop.paths));

        let (exit, stdout, stderr) =
            run_laptop_task_logs(&laptop, &runner, &["--raw", task_id.as_str()]);
        assert_ne!(
            exit,
            0,
            "raw missing log must fail: {}",
            String::from_utf8_lossy(&stderr)
        );
        assert_missing_log_not_missing_task(&stderr);
        assert!(
            !String::from_utf8_lossy(&stdout).contains(overlay.trim()),
            "raw must not print overlay, stdout={}",
            String::from_utf8_lossy(&stdout)
        );
        assert!(!laptop_task_store_exists(&laptop.paths));

        let (exit, stdout, stderr) =
            run_laptop_task_logs(&laptop, &runner, &["--follow", task_id.as_str()]);
        assert_ne!(
            exit,
            0,
            "follow of terminal no-log must not wait: {}",
            String::from_utf8_lossy(&stderr)
        );
        assert!(
            String::from_utf8_lossy(&stderr).contains("LOG_COMPLETION_UNKNOWN"),
            "follow stderr={}",
            String::from_utf8_lossy(&stderr)
        );
        assert!(
            !String::from_utf8_lossy(&stdout).contains(overlay.trim()),
            "failed follow must not print overlay then hang, stdout={}",
            String::from_utf8_lossy(&stdout)
        );
        assert!(!laptop_task_store_exists(&laptop.paths));
    }
}

#[test]
fn seeded_failed_complete_log_overlay_matches_ordinary_and_raw_omits_it() {
    let controller = IsolatedHome::new();
    let laptop = IsolatedHome::new();
    let outcome = TaskOutcome::failed("PUBLISH_FAILED");
    let record = seed_controller_task_with(
        &controller.paths,
        TaskState::Open,
        Some(outcome.clone()),
        Some(TurnTerminal::Failed),
        Some(outcome.clone()),
    );
    seed_runner_log_checkpoint(&controller.paths, b"one\ntail", Some(outcome));
    write_enabled_config(&controller.paths);
    write_enabled_config(&laptop.paths);
    let runner = ssh_to_controller(&controller);
    let task_id = record.meta().task_id().to_string();
    let mut expected = ordinary_rendered_log(b"one\ntail");
    expected.extend_from_slice(b"turn 1 failed: PUBLISH_FAILED\n");

    for args in [vec![task_id.as_str()], vec!["--follow", task_id.as_str()]] {
        let (exit, stdout, stderr) = run_laptop_task_logs(&laptop, &runner, &args);
        assert_eq!(exit, 0, "stderr={}", String::from_utf8_lossy(&stderr));
        assert_eq!(stdout, expected, "args={args:?}");
        assert_eq!(
            stdout
                .windows(b"turn 1 failed: PUBLISH_FAILED\n".len())
                .filter(|window| *window == b"turn 1 failed: PUBLISH_FAILED\n")
                .count(),
            1
        );
        assert!(!laptop_task_store_exists(&laptop.paths));
    }

    let (exit, stdout, stderr) =
        run_laptop_task_logs(&laptop, &runner, &["--raw", task_id.as_str()]);
    assert_eq!(exit, 0, "raw stderr={}", String::from_utf8_lossy(&stderr));
    assert_eq!(stdout, b"one\ntail");
    assert!(
        !String::from_utf8_lossy(&stdout).contains("turn 1 failed"),
        "raw must not grow overlay into byte offsets"
    );
}

#[test]
fn seeded_queued_missing_follow_chunk_waits_without_complete() {
    let controller = IsolatedHome::new();
    let record = seed_controller_task_with(&controller.paths, TaskState::Queued, None, None, None);
    write_enabled_config(&controller.paths);
    let task_id = record.meta().task_id().to_string();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let request = json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": "018f0f4a6b5c7d8e9f001122334455bb",
        "command": "task.logs",
        "body": {
            "task_id": task_id,
            "offset": 0,
            "limit": 64,
            "raw": false,
            "follow": true
        }
    });
    let exit = run_with_stdio_in_context(
        host_controller_rpc_cli(),
        &SystemProcessRunner,
        &controller.runtime,
        &mut Cursor::new(frame_json(&request)),
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(
        exit,
        0,
        "wait chunk stderr={}",
        String::from_utf8_lossy(&stderr)
    );
    let chunk: ControllerReadReply<ControllerTaskLogsResult> = decode_read(&stdout);
    assert!(chunk.result().exhausted());
    assert!(!chunk.result().complete());
    assert!(chunk.result().decode_bytes().unwrap().is_empty());
    assert_eq!(chunk.result().failure(), None);
    assert_eq!(chunk.result().next_offset(), chunk.result().offset());
}

#[test]
fn seeded_nonraw_chunked_json_and_utf8_match_ordinary_rendering() {
    let controller = IsolatedHome::new();
    let laptop = IsolatedHome::new();
    write_enabled_config(&controller.paths);
    write_enabled_config(&laptop.paths);
    let runner = ssh_to_controller(&controller);
    let task_id = seed_controller_task(&controller.paths, None)
        .meta()
        .task_id()
        .to_string();

    let secret = "SECRET_PAYLOAD_MUST_NOT_LEAK_AS_JSON";
    let json = format!(
        "{{\"type\":\"item.completed\",\"item\":{{\"type\":\"agent_message\",\"text\":\"{secret}{}\"}}}}\n",
        "y".repeat(MAX_LOG_CHUNK_BYTES)
    );
    seed_runner_log(&controller.paths, json.as_bytes());
    let expected = ordinary_rendered_log(json.as_bytes());
    let (exit, stdout, stderr) = run_laptop_task_logs(&laptop, &runner, &[&task_id]);
    assert_eq!(exit, 0, "json stderr={}", String::from_utf8_lossy(&stderr));
    assert_eq!(stdout, expected);
    assert!(
        !String::from_utf8_lossy(&stdout).contains("item.completed"),
        "partial JSON must not be printed"
    );
    assert!(String::from_utf8_lossy(&stdout).starts_with(secret));
    assert!(!laptop_task_store_exists(&laptop.paths));

    let mut split = vec![b'x'; MAX_LOG_CHUNK_BYTES - 1];
    split.extend_from_slice("世\n".as_bytes());
    let utf_controller = IsolatedHome::new();
    let utf_laptop = IsolatedHome::new();
    seed_controller_task(&utf_controller.paths, None);
    seed_runner_log(&utf_controller.paths, &split);
    write_enabled_config(&utf_controller.paths);
    write_enabled_config(&utf_laptop.paths);
    let utf_runner = ssh_to_controller(&utf_controller);
    let expected = ordinary_rendered_log(&split);
    let (exit, stdout, stderr) = run_laptop_task_logs(&utf_laptop, &utf_runner, &[&task_id]);
    assert_eq!(exit, 0, "utf8 stderr={}", String::from_utf8_lossy(&stderr));
    assert_eq!(stdout, expected);
    assert!(
        !String::from_utf8_lossy(&stdout).contains('\u{FFFD}'),
        "split UTF-8 must not be replaced"
    );
    assert!(!laptop_task_store_exists(&utf_laptop.paths));
}

struct ScriptedLogs {
    mutate: fn(&Value, &mut Value),
}

impl ProcessRunner for ScriptedLogs {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(request.program, OsString::from("/usr/bin/ssh"));
        let payload = decode_frame(request.stdin.as_ref().unwrap()).unwrap();
        let parsed: Value = serde_json::from_slice(payload).unwrap();
        let command = parsed["command"].as_str().unwrap();
        let request_id = parsed["request_id"].as_str().unwrap();
        let body = parsed["body"].clone();
        let digest = canonical_request_sha256(PROTOCOL_VERSION, command, &body).unwrap();
        let offset = body["offset"].as_u64().unwrap_or(0);
        let raw = body["raw"].as_bool().unwrap_or(false);
        let mut reply = json!({
            "protocol_version": PROTOCOL_VERSION,
            "command": command,
            "request_id": request_id,
            "payload_sha256": digest,
            "result": {
                "task_id": body["task_id"],
                "turn_id": "018f0f4a6b5c7d8e9f00112233445577",
                "turn_number": 1,
                "agent": "codex",
                "offset": offset,
                "next_offset": offset,
                "exhausted": true,
                "complete": true,
                "raw": raw,
                "bytes_base64": ""
            }
        });
        (self.mutate)(&body, &mut reply);
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: encode_json_frame(&reply).unwrap(),
            stderr: Vec::new(),
        })
    }
}

#[test]
fn mismatched_logs_turn_and_range_fail_without_a_laptop_task_store() {
    let laptop = IsolatedHome::new();
    write_enabled_config(&laptop.paths);
    let task_id = seeded_ids().0.to_string();
    let fail = |mutate: fn(&Value, &mut Value), args: &[&str]| {
        let (exit, _, stderr) = run_laptop_task_logs(&laptop, &ScriptedLogs { mutate }, args);
        assert_ne!(
            exit,
            0,
            "invalid logs reply must fail: {}",
            String::from_utf8_lossy(&stderr)
        );
        assert!(
            String::from_utf8_lossy(&stderr).contains("CONTROLLER_UNAVAILABLE"),
            "host-control invalid ACK path, stderr={}",
            String::from_utf8_lossy(&stderr)
        );
        assert!(!laptop_task_store_exists(&laptop.paths));
    };

    fail(
        |_, reply| {
            reply["result"]["turn_id"] = json!("018f0f4a6b5c7d8e9f00112233445500");
            reply["result"]["turn_number"] = json!(2);
        },
        &["--turn", "1", task_id.as_str()],
    );
    fail(
        |body, reply| {
            if body.get("turn_id").is_some() {
                reply["result"]["turn_id"] = json!("018f0f4a6b5c7d8e9f00112233445500");
            } else {
                reply["result"]["next_offset"] = json!(body["offset"].as_u64().unwrap_or(0) + 4);
                reply["result"]["exhausted"] = json!(false);
                reply["result"]["bytes_base64"] = json!("YWJjZA==");
            }
        },
        &[task_id.as_str()],
    );
    fail(
        |body, reply| {
            reply["result"]["next_offset"] = json!(body["offset"].as_u64().unwrap_or(0) + 99);
        },
        &[task_id.as_str()],
    );
    fail(
        |body, reply| {
            reply["result"]["next_offset"] = json!(body["offset"].as_u64().unwrap_or(0) + 4);
        },
        &[task_id.as_str()],
    );
}

struct OverlayTwiceLogs;

impl ProcessRunner for OverlayTwiceLogs {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(request.program, OsString::from("/usr/bin/ssh"));
        let payload = decode_frame(request.stdin.as_ref().unwrap()).unwrap();
        let parsed: Value = serde_json::from_slice(payload).unwrap();
        let command = parsed["command"].as_str().unwrap();
        let request_id = parsed["request_id"].as_str().unwrap();
        let body = parsed["body"].clone();
        let digest = canonical_request_sha256(PROTOCOL_VERSION, command, &body).unwrap();
        let offset = body["offset"].as_u64().unwrap_or(0);
        let raw = body["raw"].as_bool().unwrap_or(false);
        let (next_offset, exhausted, complete, bytes_base64) = if offset == 0 {
            (5_u64, false, false, "bGluZQo=")
        } else {
            (offset, true, true, "")
        };
        let reply = json!({
            "protocol_version": PROTOCOL_VERSION,
            "command": command,
            "request_id": request_id,
            "payload_sha256": digest,
            "result": {
                "task_id": body["task_id"],
                "turn_id": "018f0f4a6b5c7d8e9f00112233445577",
                "turn_number": 1,
                "agent": "codex",
                "offset": offset,
                "next_offset": next_offset,
                "exhausted": exhausted,
                "complete": complete,
                "raw": raw,
                "bytes_base64": bytes_base64,
                "failure": "turn 1 failed: PUBLISH_FAILED"
            }
        });
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: encode_json_frame(&reply).unwrap(),
            stderr: Vec::new(),
        })
    }
}

#[test]
fn logs_failure_overlay_is_printed_once_and_ignored_for_raw() {
    let laptop = IsolatedHome::new();
    write_enabled_config(&laptop.paths);
    let task_id = seeded_ids().0.to_string();
    let mut expected = ordinary_rendered_log(b"line\n");
    expected.extend_from_slice(b"turn 1 failed: PUBLISH_FAILED\n");
    let (exit, stdout, stderr) =
        run_laptop_task_logs(&laptop, &OverlayTwiceLogs, &[task_id.as_str()]);
    assert_eq!(exit, 0, "stderr={}", String::from_utf8_lossy(&stderr));
    assert_eq!(stdout, expected);
    assert_eq!(
        stdout
            .windows(b"turn 1 failed: PUBLISH_FAILED\n".len())
            .filter(|window| *window == b"turn 1 failed: PUBLISH_FAILED\n")
            .count(),
        1
    );
    let (exit, stdout, stderr) =
        run_laptop_task_logs(&laptop, &OverlayTwiceLogs, &["--raw", task_id.as_str()]);
    assert_eq!(exit, 0, "raw stderr={}", String::from_utf8_lossy(&stderr));
    assert_eq!(stdout, b"line\n");
    assert!(!laptop_task_store_exists(&laptop.paths));
}

#[test]
fn seeded_open_wait_returns_without_laptop_store() {
    let controller = IsolatedHome::new();
    let laptop = IsolatedHome::new();
    let record = seed_controller_task(&controller.paths, None);
    write_enabled_config(&controller.paths);
    write_enabled_config(&laptop.paths);
    let runner = ssh_to_controller(&controller);
    let task_id = record.meta().task_id().to_string();
    let (exit, stdout, stderr) =
        run_laptop_task(&laptop, &runner, &["wait", "--task-id", task_id.as_str()]);
    assert_eq!(exit, 0, "stderr={}", String::from_utf8_lossy(&stderr));
    assert_eq!(stdout, b"wait complete (exit 0)\n");
    assert!(!laptop_task_store_exists(&laptop.paths));
    assert_eq!(request_row_count(&controller.paths), 0);
    assert_eq!(request_row_count(&laptop.paths), 0);
}

#[test]
fn laptop_wait_timeout_when_poll_stays_busy() {
    let laptop = IsolatedHome::new();
    write_enabled_config(&laptop.paths);
    let task_id = seeded_ids().0.to_string();
    let (exit, stdout, stderr) = run_laptop_task(
        &laptop,
        &BusyWaitPoll,
        &["wait", "--task-id", task_id.as_str(), "--timeout", "1ms"],
    );
    assert_eq!(exit, 70, "stderr={}", String::from_utf8_lossy(&stderr));
    assert!(
        stdout.is_empty(),
        "stdout={}",
        String::from_utf8_lossy(&stdout)
    );
    let stderr = String::from_utf8_lossy(&stderr);
    assert!(
        stderr.contains("WAIT_TIMEOUT"),
        "busy wait must not return success: {stderr}"
    );
    assert!(!laptop_task_store_exists(&laptop.paths));
}

struct DeadlineCheckedWaitPoll;

impl ProcessRunner for DeadlineCheckedWaitPoll {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert!(
            request.policy.deadline <= Duration::from_secs(2),
            "wait poll exceeds the remaining timeout: {:?}",
            request.policy.deadline
        );
        let result = BusyWaitPoll.run(request)?;
        let mut reply: Value =
            serde_json::from_slice(decode_frame(&result.stdout).unwrap()).unwrap();
        reply["result"]["quiescent"] = json!(true);
        Ok(ProcessResult {
            stdout: encode_json_frame(&reply).unwrap(),
            ..result
        })
    }
}

#[test]
fn laptop_wait_passes_remaining_timeout_into_controller_poll() {
    let laptop = IsolatedHome::new();
    write_enabled_config(&laptop.paths);
    let task_id = seeded_ids().0.to_string();
    let (exit, _, stderr) = run_laptop_task(
        &laptop,
        &DeadlineCheckedWaitPoll,
        &["wait", "--task-id", task_id.as_str(), "--timeout", "2s"],
    );
    assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&stderr));
}

#[test]
fn seeded_reconcile_does_not_create_laptop_store() {
    let controller = IsolatedHome::new();
    let laptop = IsolatedHome::new();
    write_enabled_config(&controller.paths);
    write_enabled_config(&laptop.paths);
    let runner = ssh_to_controller(&controller);
    let (exit, stdout, stderr) = run_laptop_task(&laptop, &runner, &["reconcile"]);
    assert_eq!(exit, 0, "stderr={}", String::from_utf8_lossy(&stderr));
    assert_eq!(
        stdout,
        b"runners: 0 replaced, 0 started; task rows: 0 repaired, 0 unverifiable\n"
    );
    assert!(!laptop_task_store_exists(&laptop.paths));
    assert_eq!(request_row_count(&controller.paths), 0);
}

#[test]
fn wait_poll_extra_keys_do_not_persist_durable_rows() {
    let controller = IsolatedHome::new();
    write_enabled_config(&controller.paths);
    let (task_id, _) = seeded_ids();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let request = json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": "018f0f4a6b5c7d8e9f001122334455cc",
        "command": "task.wait.poll",
        "body": {
            "task_id": task_id.to_string(),
            "extra": true
        }
    });
    let exit = run_with_stdio_in_context(
        host_controller_rpc_cli(),
        &SystemProcessRunner,
        &controller.runtime,
        &mut Cursor::new(frame_json(&request)),
        &mut stdout,
        &mut stderr,
    );
    assert_ne!(exit, 0, "stderr={}", String::from_utf8_lossy(&stderr));
    let payload = decode_frame(&stdout).unwrap();
    let error: Value = serde_json::from_slice(payload).unwrap();
    assert_eq!(error["error"]["code"], "INVALID_REQUEST");
    assert_eq!(request_row_count(&controller.paths), 0);
}

#[test]
fn enabled_say_wait_reports_controller_outage() {
    let laptop = IsolatedHome::new();
    write_enabled_config(&laptop.paths);
    let task_id = seeded_ids().0.to_string();
    let (exit, stdout, stderr) = run_laptop_task(
        &laptop,
        &FailingSsh,
        &["say", task_id.as_str(), "--message", "hi", "--wait"],
    );
    assert_ne!(exit, 0, "stderr={}", String::from_utf8_lossy(&stderr));
    assert!(
        stdout.is_empty(),
        "stdout={}",
        String::from_utf8_lossy(&stdout)
    );
    let stderr = String::from_utf8_lossy(&stderr);
    assert!(
        stderr.contains("CONTROLLER_UNAVAILABLE"),
        "say --wait must surface the controller outage: {stderr}"
    );
    assert!(!laptop_task_store_exists(&laptop.paths));
}

struct BusyWaitPoll;

impl ProcessRunner for BusyWaitPoll {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(request.program, OsString::from("/usr/bin/ssh"));
        let payload = decode_frame(request.stdin.as_ref().unwrap()).unwrap();
        let parsed: Value = serde_json::from_slice(payload).unwrap();
        let command = parsed["command"].as_str().unwrap();
        assert_eq!(command, "task.wait.poll");
        let request_id = parsed["request_id"].as_str().unwrap();
        let body = parsed["body"].clone();
        let digest = canonical_request_sha256(PROTOCOL_VERSION, command, &body).unwrap();
        let reply = json!({
            "protocol_version": PROTOCOL_VERSION,
            "command": command,
            "request_id": request_id,
            "payload_sha256": digest,
            "result": {
                "task_ids": [body["task_id"]],
                "quiescent": false,
                "exit_code": 0
            }
        });
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: encode_json_frame(&reply).unwrap(),
            stderr: Vec::new(),
        })
    }
}
