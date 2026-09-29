use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    io::Cursor,
    os::unix::process::ExitStatusExt,
    process::ExitStatus,
    sync::Mutex,
};

use clap::Parser;
use mac_worker::{
    RuntimeContext,
    cli::Cli,
    controller::{canonical_request_sha256, decode_frame, encode_json_frame},
    error::WorkerError,
    paths::PathLayout,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::PROTOCOL_VERSION,
    run_with_stdio_in_context,
    task::{BaseOid, TaskOutcome, TaskState, TaskStatus, TurnId, TurnSummary, TurnTerminal},
};
use serde_json::{Value, json};
use tempfile::TempDir;

const TASK_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";
const TURN_ID: &str = "018f0f4a6b5c7d8e9f00112233445577";

struct IsolatedHome {
    _temp: TempDir,
    runtime: RuntimeContext,
    paths: PathLayout,
}

struct Rpc {
    active: bool,
    commands: Mutex<Vec<String>>,
}

#[test]
fn controller_interrupt_of_an_active_task_cancels_then_says() {
    let laptop = IsolatedHome::new();
    write_enabled_config(&laptop.paths);
    let rpc = Rpc::active();
    let (exit, stdout, stderr) = run_say(&laptop, &rpc, false);
    assert_eq!(exit, 0, "stderr={stderr}\nstdout={stdout}");
    assert!(
        stdout.starts_with("interrupted turn 1 (cancelled)\n"),
        "{stdout}"
    );
    // A terminal cancel ack is not enough: the controller's runner may still be
    // retiring the turn, so the laptop waits for quiescence before the say.
    assert_eq!(
        rpc.commands(),
        [
            "task.status",
            "task.cancel",
            "task.wait.poll",
            "task.status",
            "task.say"
        ]
    );
}

#[test]
fn controller_interrupt_of_an_active_task_json_names_the_turn() {
    let laptop = IsolatedHome::new();
    write_enabled_config(&laptop.paths);
    let rpc = Rpc::active();
    let (exit, stdout, stderr) = run_say(&laptop, &rpc, true);
    assert_eq!(exit, 0, "stderr={stderr}\nstdout={stdout}");
    let value: Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(
        value["interrupted"],
        json!({
            "turn_id": TURN_ID,
            "outcome": "cancelled",
        })
    );
    // A terminal cancel ack is not enough: the controller's runner may still be
    // retiring the turn, so the laptop waits for quiescence before the say.
    assert_eq!(
        rpc.commands(),
        [
            "task.status",
            "task.cancel",
            "task.wait.poll",
            "task.status",
            "task.say"
        ]
    );
}

#[test]
fn controller_interrupt_of_an_idle_task_only_says() {
    let laptop = IsolatedHome::new();
    write_enabled_config(&laptop.paths);
    let rpc = Rpc::idle();
    let (exit, stdout, stderr) = run_say(&laptop, &rpc, false);
    assert_eq!(exit, 0, "stderr={stderr}\nstdout={stdout}");
    assert!(!stdout.contains("interrupted"), "{stdout}");
    assert_eq!(rpc.commands(), ["task.status", "task.say"]);
}

#[test]
fn controller_interrupt_of_an_idle_task_json_omits_interrupted() {
    let laptop = IsolatedHome::new();
    write_enabled_config(&laptop.paths);
    let rpc = Rpc::idle();
    let (exit, stdout, stderr) = run_say(&laptop, &rpc, true);
    assert_eq!(exit, 0, "stderr={stderr}\nstdout={stdout}");
    let value: Value = serde_json::from_str(stdout.trim()).unwrap();
    assert!(value.get("interrupted").is_none(), "{value}");
    assert_eq!(rpc.commands(), ["task.status", "task.say"]);
}

fn run_say(laptop: &IsolatedHome, rpc: &Rpc, json: bool) -> (u8, String, String) {
    let mut args = vec![
        "worker",
        "task",
        "say",
        TASK_ID,
        "--interrupt",
        "--message",
        "steer",
    ];
    if json {
        args.insert(1, "--json");
    }
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        Cli::try_parse_from(args).unwrap(),
        rpc,
        &laptop.runtime,
        &mut Cursor::new(Vec::new()),
        &mut stdout,
        &mut stderr,
    );
    (
        exit,
        String::from_utf8(stdout).unwrap(),
        String::from_utf8(stderr).unwrap(),
    )
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
        for directory in [&home, &state_home, &cache_home, &config_home, &data_home] {
            std::fs::create_dir_all(directory).unwrap();
        }
        let environment = BTreeMap::from([
            (OsString::from("HOME"), home.as_os_str().to_os_string()),
            (OsString::from("XDG_STATE_HOME"), state_home.into()),
            (OsString::from("XDG_CACHE_HOME"), cache_home.into()),
            (OsString::from("XDG_CONFIG_HOME"), config_home.into()),
            (OsString::from("XDG_DATA_HOME"), data_home.into()),
        ]);
        let paths = PathLayout::discover(None, &environment, &home).unwrap();
        Self {
            runtime: RuntimeContext::isolated(environment, home.clone(), root),
            paths,
            _temp: temp,
        }
    }
}

fn write_enabled_config(paths: &PathLayout) {
    std::fs::create_dir_all(paths.config.parent().unwrap()).unwrap();
    std::fs::write(
        &paths.config,
        "version = 1\n[controller]\nenabled = true\nssh = \"user@always-on-host\"\n",
    )
    .unwrap();
}

impl Rpc {
    fn active() -> Self {
        Self {
            active: true,
            commands: Mutex::new(Vec::new()),
        }
    }

    fn idle() -> Self {
        Self {
            active: false,
            commands: Mutex::new(Vec::new()),
        }
    }

    fn commands(&self) -> Vec<String> {
        self.commands.lock().unwrap().clone()
    }
}

impl ProcessRunner for Rpc {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(request.program, OsStr::new("/usr/bin/ssh"));
        let payload = decode_frame(request.stdin.as_ref().unwrap()).unwrap();
        let parsed: Value = serde_json::from_slice(payload).unwrap();
        let command = parsed["command"].as_str().unwrap().to_owned();
        assert!(
            parsed["body"].get("interrupt").is_none(),
            "interrupt must stay off the wire: {parsed}"
        );
        if command == "task.say" {
            assert_eq!(parsed["body"]["message"], "steer");
        }
        self.commands.lock().unwrap().push(command.clone());
        let request_id = parsed["request_id"].as_str().unwrap();
        let body = parsed["body"].clone();
        let digest = canonical_request_sha256(PROTOCOL_VERSION, &command, &body).unwrap();
        let task_id = body["task_id"].as_str().unwrap();
        assert_eq!(task_id, TASK_ID);
        let reply = match command.as_str() {
            "task.wait.poll" => json!({
                "protocol_version": PROTOCOL_VERSION,
                "command": command,
                "request_id": request_id,
                "payload_sha256": digest,
                "result": {"task_ids": [TASK_ID], "quiescent": true, "exit_code": 0},
            }),
            "task.status" => read_reply(
                &command,
                request_id,
                &digest,
                if self.active && !self.commands().iter().any(|seen| seen == "task.cancel") {
                    active_status()
                } else if self.active {
                    cancelled_status()
                } else {
                    idle_status()
                },
            ),
            "task.cancel" | "task.say" => ack(
                request_id,
                &digest,
                task_id,
                if command == "task.cancel" {
                    cancelled_status()
                } else {
                    follow_up_status()
                },
            ),
            other => panic!("unexpected controller command {other}"),
        };
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: encode_json_frame(&reply).unwrap(),
            stderr: Vec::new(),
        })
    }
}

fn read_reply(command: &str, request_id: &str, digest: &str, status: TaskStatus) -> Value {
    json!({
        "protocol_version": PROTOCOL_VERSION,
        "command": command,
        "request_id": request_id,
        "payload_sha256": digest,
        "result": status_result(status),
    })
}

fn ack(request_id: &str, digest: &str, task_id: &str, status: TaskStatus) -> Value {
    json!({
        "protocol_version": PROTOCOL_VERSION,
        "status": "acked",
        "request_id": request_id,
        "payload_sha256": digest,
        "task_id": task_id,
        "created_at_millis": 1,
        "result": status_result(status),
    })
}

fn status_result(status: TaskStatus) -> Value {
    json!({
        "task_id": TASK_ID,
        "run_id": null,
        "status": status,
        "warnings": [],
        "events": [],
        "runner": null,
        "exit_code": null,
    })
}

fn active_status() -> TaskStatus {
    turn_status(TaskState::Active, None, None, 2)
}

fn cancelled_status() -> TaskStatus {
    turn_status(
        TaskState::Open,
        Some(TurnTerminal::Cancelled),
        Some(TaskOutcome::Cancelled),
        3,
    )
}

fn idle_status() -> TaskStatus {
    turn_status(
        TaskState::Open,
        Some(TurnTerminal::Succeeded),
        Some(TaskOutcome::Done),
        4,
    )
}

fn follow_up_status() -> TaskStatus {
    cancelled_status()
}

fn turn_status(
    state: TaskState,
    terminal: Option<TurnTerminal>,
    outcome: Option<TaskOutcome>,
    updated_at_millis: u64,
) -> TaskStatus {
    let turn_id: TurnId = TURN_ID.parse().unwrap();
    let base: BaseOid = "a".repeat(40).parse().unwrap();
    let turn = TurnSummary::new(
        1,
        turn_id,
        terminal,
        outcome.clone(),
        terminal.map(|_| true),
        false,
        Some(1),
        terminal.map(|_| 2),
    );
    TaskStatus::new(
        state,
        outcome,
        Some("mini-1".into()),
        true,
        Some(base),
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![turn],
        updated_at_millis,
    )
    .unwrap()
}
