//! Enabled `task say --wait` must return the wait poll exit, not status.exit_code.

use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    os::unix::process::ExitStatusExt,
    process::ExitStatus,
    sync::Mutex,
};

use clap::Parser;
use mac_worker::{
    RuntimeContext,
    cli::Cli,
    controller::{decode_frame, encode_json_frame, parse_request},
    error::WorkerError,
    paths::PathLayout,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::PROTOCOL_VERSION,
    run_with_io_in_context,
    task::{BaseOid, TaskId, TaskOutcome, TaskState, TaskStatus, TurnSummary, TurnTerminal},
};
use serde_json::{Value, json};
use uuid::Uuid;

const TASK: u128 = 0x018f_0f4a_6b5c_7d8e_9f00_1122_3344_5566;
const TURN: u128 = 0x018f_0f4a_6b5c_7d8e_9f00_1122_3344_5577;

struct IsolatedHome {
    _temp: tempfile::TempDir,
    runtime: RuntimeContext,
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
        for dir in [&home, &state_home, &cache_home, &config_home, &data_home] {
            std::fs::create_dir_all(dir).unwrap();
        }
        let environment = BTreeMap::from([
            (OsString::from("HOME"), home.as_os_str().to_os_string()),
            (OsString::from("XDG_STATE_HOME"), state_home.into()),
            (OsString::from("XDG_CACHE_HOME"), cache_home.into()),
            (OsString::from("XDG_CONFIG_HOME"), config_home.into()),
            (OsString::from("XDG_DATA_HOME"), data_home.into()),
        ]);
        let paths = PathLayout::discover(None, &environment, &home).unwrap();
        if let Some(parent) = paths.config.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(
            &paths.config,
            "version = 1\n\n[controller]\nenabled = true\nssh = \"controller-host\"\n",
        )
        .unwrap();
        Self {
            runtime: RuntimeContext::isolated(environment, home, root),
            _temp: temp,
        }
    }
}

struct SayWaitController {
    wait_exit: u8,
    outcome: TaskOutcome,
    commands: Mutex<Vec<String>>,
}

impl SayWaitController {
    fn new(wait_exit: u8, outcome: TaskOutcome) -> Self {
        Self {
            wait_exit,
            outcome,
            commands: Mutex::new(Vec::new()),
        }
    }
}

impl ProcessRunner for SayWaitController {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program != OsStr::new("/usr/bin/ssh") {
            return Err(WorkerError::Protocol("unexpected fixture process".into()));
        }
        let remote = request
            .args
            .last()
            .and_then(|argument| argument.to_str())
            .unwrap_or_default();
        if !remote.ends_with("host controller-rpc") {
            return Err(WorkerError::Protocol(format!(
                "unexpected fixture worker operation: {remote}"
            )));
        }
        let stdin = request
            .stdin
            .as_deref()
            .ok_or_else(|| WorkerError::Protocol("controller RPC request had no stdin".into()))?;
        let payload = decode_frame(stdin)
            .map_err(|error| WorkerError::Protocol(format!("controller RPC frame: {error}")))?;
        let parsed = parse_request(payload)?;
        self.commands
            .lock()
            .unwrap()
            .push(parsed.command().to_owned());
        let reply = match parsed.command() {
            "task.say" => json!({
                "protocol_version": PROTOCOL_VERSION,
                "status": "published",
                "request_id": parsed.request_id(),
                "payload_sha256": parsed.payload_sha256(),
                "task_id": parsed.body().get("task_id"),
                "turn_id": format!("{:x}", Uuid::from_u128(TURN).simple()),
                "created_at_millis": 1,
            }),
            "task.wait.poll" => json!({
                "protocol_version": PROTOCOL_VERSION,
                "command": "task.wait.poll",
                "request_id": parsed.request_id(),
                "payload_sha256": parsed.payload_sha256(),
                "result": {
                    "task_ids": [parsed.body().get("task_id")],
                    "quiescent": true,
                    "exit_code": self.wait_exit,
                },
            }),
            "task.status" => {
                let task_id: TaskId = parsed
                    .body()
                    .get("task_id")
                    .and_then(Value::as_str)
                    .unwrap()
                    .parse()
                    .unwrap();
                let turn_id = format!("{:x}", Uuid::from_u128(TURN).simple())
                    .parse()
                    .unwrap();
                let status = failed_or_done_status(&self.outcome, turn_id);
                json!({
                    "protocol_version": PROTOCOL_VERSION,
                    "command": "task.status",
                    "request_id": parsed.request_id(),
                    "payload_sha256": parsed.payload_sha256(),
                    "result": {
                        "task_id": task_id.to_string(),
                        "run_id": Value::Null,
                        "status": status,
                        "warnings": [],
                        "events": [],
                        "runner": Value::Null,
                        "exit_code": Value::Null,
                    },
                })
            }
            other => {
                return Err(WorkerError::Protocol(format!(
                    "unexpected controller command {other}"
                )));
            }
        };
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: encode_json_frame(&reply).unwrap(),
            stderr: Vec::new(),
        })
    }
}

fn failed_or_done_status(outcome: &TaskOutcome, turn_id: mac_worker::task::TurnId) -> TaskStatus {
    let turn = TurnSummary::new(
        1,
        turn_id,
        Some(TurnTerminal::Succeeded),
        Some(outcome.clone()),
        Some(true),
        false,
        Some(1),
        Some(2),
    );
    TaskStatus::new(
        TaskState::Open,
        Some(outcome.clone()),
        Some("mini-1".into()),
        true,
        Some("b".repeat(40).parse::<BaseOid>().unwrap()),
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![turn],
        2,
    )
    .unwrap()
}

fn run_say_wait(controller: &SayWaitController) -> (u8, Value, Vec<String>) {
    let home = IsolatedHome::new();
    let task = format!("{:x}", Uuid::from_u128(TASK).simple());
    let cli = Cli::try_parse_from([
        "worker",
        "--json",
        "task",
        "say",
        &task,
        "--message",
        "continue",
        "--wait",
    ])
    .unwrap();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let code = run_with_io_in_context(cli, controller, &home.runtime, &mut stdout, &mut stderr);
    assert!(
        stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&stderr)
    );
    let report: Value = serde_json::from_slice(&stdout).unwrap();
    let commands = controller.commands.lock().unwrap().clone();
    (code, report, commands)
}

#[test]
fn enabled_say_wait_returns_wait_exit_1_when_status_exit_is_null() {
    let controller = SayWaitController::new(1, TaskOutcome::failed("PUBLISH_FAILED"));
    let (code, report, commands) = run_say_wait(&controller);
    assert_eq!(commands, ["task.say", "task.wait.poll", "task.status"]);
    assert_eq!(report["exit_code"], Value::Null);
    assert_eq!(
        report["status"]["last_outcome"],
        json!({"kind": "failed", "reason": "PUBLISH_FAILED"})
    );
    assert_eq!(code, 1);
}

#[test]
fn enabled_say_wait_returns_0_when_wait_succeeds() {
    let controller = SayWaitController::new(0, TaskOutcome::Done);
    let (code, report, commands) = run_say_wait(&controller);
    assert_eq!(commands, ["task.say", "task.wait.poll", "task.status"]);
    assert_eq!(report["exit_code"], Value::Null);
    assert_eq!(report["status"]["last_outcome"], json!({"kind": "done"}));
    assert_eq!(code, 0);
}
