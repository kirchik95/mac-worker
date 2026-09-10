//! Enabled-controller follow-up lifecycle through real processes.
//!
//! Grammar tests run on immutable d545. Runtime tests stay un-ignored and
//! fail until FLOW routes `task say --wait` (and the rest of the lifecycle)
//! over the existing ENV process fixture. Shared `tests/support/controller_process*`
//! is referenced, not copied.

#[path = "support/controller_process.rs"]
mod controller_process;
mod support;

use std::path::Path;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use clap::Parser;
use controller_process::ProcessFixture;
use mac_worker::{
    cli::{Cli, Command as WorkerCommand, TaskCommand},
    controller::{OperationEnvelope, decode_frame, encode_json_frame, load_operation_envelope},
    protocol::PROTOCOL_VERSION,
    transfer_repo::TransferRepo,
};
use serde_json::{Value, json};

const SAMPLE_TASK: &str = "018f0f4a6b5c7d8e9f00112233445566";
const WAIT_BUDGET: Duration = Duration::from_secs(30);
const REPLAY_QUIESCE: Duration = Duration::from_secs(2);

const GIT_ENVIRONMENT_REMOVALS: &[&str] = &[
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_COMMON_DIR",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_CEILING_DIRECTORIES",
    "GIT_DISCOVERY_ACROSS_FILESYSTEM",
    "GIT_CONFIG_COUNT",
    "GIT_CONFIG_PARAMETERS",
];

#[test]
fn grammar_followup_cli_is_public() {
    let submit = Cli::try_parse_from([
        "worker",
        "task",
        "submit",
        "--prompt",
        "first turn stays open",
        "--wip",
        "--no-wait",
        "--close-on",
        "never",
    ])
    .unwrap();
    match submit.command {
        WorkerCommand::Task {
            command:
                TaskCommand::Submit {
                    wip: true,
                    no_wait: true,
                    close_on: Some(ref policy),
                    ..
                },
        } if policy == "never" => {}
        other => panic!("expected submit --wip --no-wait --close-on never, got {other:?}"),
    }

    let say = Cli::try_parse_from([
        "worker",
        "task",
        "say",
        SAMPLE_TASK,
        "--message",
        "second turn",
        "--wait",
    ])
    .unwrap();
    match say.command {
        WorkerCommand::Task {
            command:
                TaskCommand::Say {
                    wait: true,
                    message: Some(ref text),
                    ..
                },
        } if text == "second turn" => {}
        other => panic!("expected say --message --wait, got {other:?}"),
    }

    for (args, describe) in [
        (
            vec![
                "worker",
                "task",
                "wait",
                "--task-id",
                SAMPLE_TASK,
                "--timeout",
                "30s",
            ],
            "task wait --task-id --timeout",
        ),
        (vec!["worker", "task", "status", SAMPLE_TASK], "task status"),
        (vec!["worker", "task", "logs", SAMPLE_TASK], "task logs"),
        (
            vec!["worker", "task", "diff", SAMPLE_TASK, "--stat"],
            "task diff --stat",
        ),
        (vec!["worker", "task", "result", SAMPLE_TASK], "task result"),
        (vec!["worker", "task", "cancel", SAMPLE_TASK], "task cancel"),
        (vec!["worker", "task", "close", SAMPLE_TASK], "task close"),
        (vec!["worker", "task", "fetch", SAMPLE_TASK], "task fetch"),
    ] {
        Cli::try_parse_from(args).unwrap_or_else(|error| panic!("{describe}: {error}"));
    }
}

#[test]
fn grammar_process_fixture_leader_ready_and_reaps() {
    let fixture = ProcessFixture::new();
    let mut leader = fixture.spawn_controller_run();
    let ready = fixture.wait_until_leader_ready(&mut leader);
    assert!(
        ready.contains("controller leader acquired"),
        "controller run must print the existing fixture ready line; got {ready:?}"
    );
    leader.terminate_and_reap();
}

/// Successful SAY/--wait, terminal cancel no-op, required identical say-envelope
/// replay, close, then explicit fetch. Red on d545: enabled CLI still opens the
/// laptop store and does not route `task say`.
#[test]
fn runtime_enabled_controller_say_wait_close_and_fetch() {
    let fixture = ProcessFixture::new();
    let repo = support::GitRepo::init();
    repo.write("README", b"followup-process\n");
    repo.commit_all("init");

    let mut leader = fixture.spawn_controller_run();
    fixture.wait_until_leader_ready(&mut leader);

    let (submit_status, submit_stdout, submit_stderr) = fixture.run_laptop(
        &[
            "--json",
            "task",
            "submit",
            "--prompt",
            "first controller turn",
            "--wip",
            "--no-wait",
            "--close-on",
            "never",
        ],
        Some(repo.root()),
    );
    let submit_text = String::from_utf8_lossy(&submit_stdout);
    let submit_err = String::from_utf8_lossy(&submit_stderr);
    assert!(
        submit_status.success(),
        "blocked FLOW seam: enabled submit --wip --close-on never must ACK through controller-rpc; stdout={submit_text} stderr={submit_err}"
    );
    assert_no_laptop_authority(&fixture);
    let submit = parse_json(&submit_stdout);
    let task_id = json_str(&submit, "task_id")
        .unwrap_or_else(|| panic!("submit ACK needs task_id; stdout={submit_text}"))
        .to_owned();
    let submit_turn = json_str(&submit, "turn_id").map(str::to_owned);
    let frozen_body = submit_envelope(&fixture);
    let frozen = json_str(frozen_body.body(), "base_oid")
        .unwrap_or_else(|| {
            panic!(
                "task.submit FrozenSubmitBody must carry top-level base_oid; body={:?}",
                frozen_body.body()
            )
        })
        .to_owned();
    let project_id = json_str(frozen_body.body(), "project_id")
        .unwrap_or_else(|| panic!("FrozenSubmitBody needs project_id"))
        .to_owned();
    let worktree_id = json_str(frozen_body.body(), "worktree_id")
        .unwrap_or_else(|| panic!("FrozenSubmitBody needs worktree_id"))
        .to_owned();
    let transfer_git = controller_transfer_git(&fixture, &project_id, &worktree_id);

    let (wait_status, wait_stdout, wait_stderr) = fixture.run_laptop(
        &[
            "--json",
            "task",
            "wait",
            "--task-id",
            &task_id,
            "--timeout",
            "30s",
        ],
        Some(repo.root()),
    );
    assert!(
        wait_status.success(),
        "blocked FLOW seam: task wait must observe turn 1 without a laptop store; stdout={} stderr={}",
        String::from_utf8_lossy(&wait_stdout),
        String::from_utf8_lossy(&wait_stderr)
    );

    let first = wait_until_open_done(&fixture, repo.root(), &task_id, 1, submit_turn.as_deref());
    let turn1_id = last_turn_id(&first)
        .expect("turn 1 id from status.turns")
        .to_owned();
    let head1 = head_oid(&first).expect("turn 1 head").to_owned();
    assert_ne!(head1, frozen, "turn 1 result must not be the frozen base");
    assert!(
        !laptop_has_object(&repo, &head1),
        "laptop Git must not hold turn 1 result {head1} before explicit fetch"
    );
    assert_remote_descendant(&fixture, &transfer_git, &frozen, &head1, "turn 1");
    assert_eq!(
        fixture.journal_opcode_count("host task-turn"),
        1,
        "first turn must be exactly one accepted host task-turn; journal={}",
        fixture.exec_journal()
    );

    let say_envelopes_before = say_envelopes(&fixture);
    let (say_status, say_stdout, say_stderr) = fixture.run_laptop(
        &[
            "--json",
            "task",
            "say",
            &task_id,
            "--message",
            "second controller turn",
            "--wait",
        ],
        Some(repo.root()),
    );
    let say_text = String::from_utf8_lossy(&say_stdout);
    let say_err = String::from_utf8_lossy(&say_stderr);
    assert!(
        say_status.success(),
        "blocked FLOW seam: enabled task say --wait must complete through controller RPC; stdout={say_text} stderr={say_err}"
    );
    let say = parse_json(&say_stdout);
    assert_eq!(json_str(&say, "task_id"), Some(task_id.as_str()));
    assert!(
        is_open_done(&say, 2, None),
        "task say --wait must itself return Open+Done with two succeeded turns; stdout={say_text} stderr={say_err} journal={}",
        fixture.exec_journal()
    );
    let after_say = say;
    assert_eq!(json_str(&after_say, "task_id"), Some(task_id.as_str()));
    assert_eq!(json_str(status_object(&after_say), "state"), Some("open"));
    let turn2 = last_turn_id(&after_say)
        .expect("turn 2 id from status.turns")
        .to_owned();
    assert_ne!(turn2, turn1_id, "say must mint a new turn id");
    let head2 = head_oid(&after_say).expect("turn 2 head").to_owned();
    assert_ne!(head2, frozen, "turn 2 result must not be the frozen base");
    assert_ne!(
        head2, head1,
        "say must mint a distinct result commit; Git is-ancestor allows head1==head2"
    );
    assert!(
        !laptop_has_object(&repo, &head2),
        "laptop Git must not hold turn 2 result {head2} before explicit fetch"
    );
    assert_remote_descendant(&fixture, &transfer_git, &frozen, &head2, "turn 2");
    assert!(
        git_dir_is_ancestor(&transfer_git, &head1, &head2),
        "turn 2 {head2} must be a descendant of turn 1 {head1} in the controller transfer cache"
    );
    assert!(
        git_dir_is_ancestor(&fixture.exec_git, &head1, &head2),
        "fakeexec result {head2} must sit on the previous imported head {head1}"
    );
    assert_eq!(turns(&after_say).len(), 2, "status={after_say}");
    assert_second_host_turn_consumed_head1(&fixture, &task_id, &frozen, &head1);
    assert_no_laptop_authority(&fixture);

    let status = read_status(&fixture, repo.root(), &task_id);
    assert_eq!(json_str(&status, "task_id"), Some(task_id.as_str()));
    assert_eq!(head_oid(&status), Some(head2.as_str()));
    assert_eq!(last_turn_id(&status), Some(turn2.as_str()));
    if let Some(files) = status_object(&status)
        .get("files_changed")
        .and_then(Value::as_array)
    {
        assert!(
            files
                .iter()
                .any(|value| value.as_str() == Some("RESULT.txt")),
            "status files_changed must include the fakeexec RESULT.txt; status={status}"
        );
    }

    let (logs_status, logs_stdout, logs_stderr) =
        fixture.run_laptop(&["--json", "task", "logs", &task_id], Some(repo.root()));
    assert!(
        logs_status.success(),
        "enabled task logs must succeed over controller RPC; stdout={} stderr={}",
        String::from_utf8_lossy(&logs_stdout),
        String::from_utf8_lossy(&logs_stderr)
    );
    let logs_text = String::from_utf8_lossy(&logs_stdout);
    assert!(
        !looks_like_error_event(&logs_stdout),
        "task logs writes runner bytes, not an error event; stdout={logs_text}"
    );
    assert!(
        logs_text.trim().is_empty(),
        "existing fakeexec status-logs chunks are empty; logs must not invent identity text; stdout={logs_text}"
    );
    assert_no_laptop_authority(&fixture);

    let expected_stat = git_dir_diff_stat(&transfer_git, &frozen, &head2);
    let fakeexec_stat = git_dir_diff_stat(&fixture.exec_git, &frozen, &head2);
    assert_eq!(
        stat_without_trailing_newline(&fakeexec_stat),
        stat_without_trailing_newline(&expected_stat),
        "controller transfer and fakeexec git diff --stat {frozen}..{head2} must match"
    );
    assert!(
        stat_without_trailing_newline(&expected_stat).contains("RESULT.txt"),
        "controller transfer git diff --stat {frozen}..{head2} must show fakeexec RESULT.txt; got {expected_stat:?}"
    );
    let (diff_status, diff_stdout, diff_stderr) = fixture.run_laptop(
        &["--json", "task", "diff", &task_id, "--stat"],
        Some(repo.root()),
    );
    assert!(
        diff_status.success(),
        "enabled task diff --stat must succeed over controller RPC; stdout={} stderr={}",
        String::from_utf8_lossy(&diff_stdout),
        String::from_utf8_lossy(&diff_stderr)
    );
    let diff_text = String::from_utf8_lossy(&diff_stdout);
    assert_eq!(
        stat_without_trailing_newline(&diff_text),
        stat_without_trailing_newline(&expected_stat),
        "task diff --stat must be the real Git --stat text (option forwarded), not a full patch that happens to mention RESULT.txt; stdout={diff_text}"
    );
    assert_no_laptop_authority(&fixture);

    let (result_status, result_stdout, result_stderr) =
        fixture.run_laptop(&["--json", "task", "result", &task_id], Some(repo.root()));
    assert!(
        result_status.success(),
        "enabled task result must succeed; stdout={} stderr={}",
        String::from_utf8_lossy(&result_stdout),
        String::from_utf8_lossy(&result_stderr)
    );
    let result = parse_json(&result_stdout);
    assert_eq!(json_str(&result, "task_id"), Some(task_id.as_str()));
    assert_eq!(head_oid(&result), Some(head2.as_str()));
    let expected_branch = format!("task/{task_id}");
    assert_eq!(json_str(&result, "branch"), Some(expected_branch.as_str()));
    assert_eq!(
        fixture.journal_opcode_count("host task-turn"),
        2,
        "status/logs/diff/result must not start another host execution; journal={}",
        fixture.exec_journal()
    );

    let (cancel_status, cancel_stdout, cancel_stderr) =
        fixture.run_laptop(&["--json", "task", "cancel", &task_id], Some(repo.root()));
    assert!(
        cancel_status.success(),
        "quiescent cancel must be a documented terminal no-op; stdout={} stderr={}",
        String::from_utf8_lossy(&cancel_stdout),
        String::from_utf8_lossy(&cancel_stderr)
    );
    let after_cancel = read_status(&fixture, repo.root(), &task_id);
    assert_eq!(turns(&after_cancel).len(), 2, "cancel must not add a turn");
    assert_eq!(head_oid(&after_cancel), Some(head2.as_str()));
    assert_eq!(
        json_str(status_object(&after_cancel), "state"),
        Some("open")
    );
    assert_eq!(
        fixture.journal_opcode_count("host task-turn"),
        2,
        "cancel must not start another host execution; journal={}",
        fixture.exec_journal()
    );

    let say_envelope = say_envelopes(&fixture)
        .into_iter()
        .find(|envelope| {
            !say_envelopes_before
                .iter()
                .any(|prior| prior.request_id() == envelope.request_id())
        })
        .unwrap_or_else(|| {
            panic!(
                "blocked FLOW seam: enabled task say must persist a laptop OperationEnvelope for identical replay; cache={:?}",
                fixture.envelope_paths()
            )
        });
    assert_eq!(say_envelope.command(), "task.say");
    leader.terminate_and_reap();
    let mut leader = fixture.spawn_controller_run();
    fixture.wait_until_leader_ready(&mut leader);
    let frame = encode_json_frame(&json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": say_envelope.request_id(),
        "command": say_envelope.command(),
        "body": say_envelope.body(),
    }))
    .expect("say envelope frame");
    let (rpc_status, rpc_stdout, rpc_stderr) = fixture.run_controller_rpc(&frame);
    assert!(
        rpc_status.success(),
        "identical say envelope replay must ACK the original request; stdout={} stderr={}",
        String::from_utf8_lossy(&rpc_stdout),
        String::from_utf8_lossy(&rpc_stderr)
    );
    let ack = decode_rpc_json(&rpc_stdout, &rpc_stderr);
    assert_eq!(
        json_str(&ack, "request_id"),
        Some(say_envelope.request_id())
    );
    assert_eq!(json_str(&ack, "task_id"), Some(task_id.as_str()));
    if let Some(turn) = json_str(&ack, "turn_id") {
        assert_eq!(turn, turn2);
    }
    let after_replay = wait_until_open_done(&fixture, repo.root(), &task_id, 2, Some(&turn2));
    assert_eq!(head_oid(&after_replay), Some(head2.as_str()));
    assert_eq!(turns(&after_replay).len(), 2);
    assert_eq!(
        json_str(status_object(&after_replay), "state"),
        Some("open")
    );
    wait_replay_quiescent(&fixture, repo.root(), &task_id, &head2, &turn2);

    let (close_status, close_stdout, close_stderr) =
        fixture.run_laptop(&["--json", "task", "close", &task_id], Some(repo.root()));
    assert!(
        close_status.success(),
        "close must succeed; stdout={} stderr={}",
        String::from_utf8_lossy(&close_stdout),
        String::from_utf8_lossy(&close_stderr)
    );
    let closed = read_status(&fixture, repo.root(), &task_id);
    assert_eq!(json_str(status_object(&closed), "state"), Some("closed"));
    assert_eq!(
        status_object(&closed)
            .get("last_outcome")
            .and_then(|value| json_str(value, "kind")),
        Some("done")
    );
    assert_eq!(turns(&closed).len(), 2);
    assert_eq!(head_oid(&closed), Some(head2.as_str()));
    assert!(
        !laptop_has_object(&repo, &head2),
        "explicit fetch, not close, materializes {head2} in the laptop repo"
    );

    let (fetch_status, fetch_stdout, fetch_stderr) =
        fixture.run_laptop(&["--json", "task", "fetch", &task_id], Some(repo.root()));
    assert!(
        fetch_status.success(),
        "explicit fetch must materialize the recorded head; stdout={} stderr={}",
        String::from_utf8_lossy(&fetch_stdout),
        String::from_utf8_lossy(&fetch_stderr)
    );
    let fetch = parse_json(&fetch_stdout);
    assert_eq!(json_str(&fetch, "head_oid"), Some(head2.as_str()));
    assert!(laptop_has_object(&repo, &head2));
    assert_eq!(git_object_type(&repo, &head2), "commit");
    assert!(
        git_is_ancestor(&repo, &frozen, &head2),
        "after fetch, laptop {head2} must be a descendant of frozen {frozen}"
    );
    assert!(
        git_is_ancestor(&repo, &head1, &head2),
        "after fetch, laptop {head2} must remain a descendant of turn 1 {head1}"
    );
    assert_no_laptop_authority(&fixture);
    assert_eq!(fixture.journal_opcode_count("host task-turn"), 2);
    drop(leader);
}

fn assert_no_laptop_authority(fixture: &ProcessFixture) {
    let state = fixture.laptop_state();
    if !state.exists() {
        return;
    }
    for name in ["tasks", "queue", "runners", "turns", "runs", "dags"] {
        let path = state.join(name);
        assert!(
            !directory_has_regular_files(&path),
            "laptop must not hold authoritative {name} data at {}",
            path.display()
        );
    }
}

fn directory_has_regular_files(root: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(root) else {
        return false;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if directory_has_regular_files(&path) {
                return true;
            }
        } else if path.is_file() {
            return true;
        }
    }
    false
}

fn parse_json(stdout: &[u8]) -> Value {
    serde_json::from_slice(stdout).unwrap_or_else(|_| {
        panic!(
            "expected JSON stdout, got {}",
            String::from_utf8_lossy(stdout)
        )
    })
}

fn looks_like_error_event(stdout: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(stdout) else {
        return false;
    };
    json_str(&value, "event") == Some("error")
}

fn json_str<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn status_object(report: &Value) -> &Value {
    report.get("status").unwrap_or(report)
}

fn turns(report: &Value) -> &[Value] {
    status_object(report)
        .get("turns")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn last_turn_id(report: &Value) -> Option<&str> {
    json_str(turns(report).last()?, "turn_id")
}

fn head_oid(report: &Value) -> Option<&str> {
    json_str(status_object(report), "head_oid")
}

fn read_status(fixture: &ProcessFixture, cwd: &Path, task_id: &str) -> Value {
    let (status, stdout, stderr) =
        fixture.run_laptop(&["--json", "task", "status", task_id], Some(cwd));
    assert!(
        status.success(),
        "status must succeed; stdout={} stderr={}",
        String::from_utf8_lossy(&stdout),
        String::from_utf8_lossy(&stderr)
    );
    parse_json(&stdout)
}

fn wait_until_open_done(
    fixture: &ProcessFixture,
    cwd: &Path,
    task_id: &str,
    expected_turns: usize,
    expected_last_turn: Option<&str>,
) -> Value {
    let deadline = Instant::now() + WAIT_BUDGET;
    loop {
        let (status, stdout, stderr) =
            fixture.run_laptop(&["--json", "task", "status", task_id], Some(cwd));
        let text = String::from_utf8_lossy(&stdout);
        let err = String::from_utf8_lossy(&stderr);
        assert!(
            status.success(),
            "status polling must not open a laptop store; stdout={text} stderr={err}"
        );
        let report = parse_json(&stdout);
        if is_open_done(&report, expected_turns, expected_last_turn) {
            return report;
        }
        if Instant::now() > deadline {
            panic!(
                "blocked FLOW seam: did not reach Open+Done with {expected_turns} turn(s); status={text} stderr={err} journal={}",
                fixture.exec_journal()
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_replay_quiescent(
    fixture: &ProcessFixture,
    cwd: &Path,
    task_id: &str,
    head: &str,
    turn2: &str,
) {
    let deadline = Instant::now() + REPLAY_QUIESCE;
    loop {
        let report = read_status(fixture, cwd, task_id);
        assert_eq!(turns(&report).len(), 2, "replay must not mint a third turn");
        assert_eq!(last_turn_id(&report), Some(turn2));
        assert_eq!(head_oid(&report), Some(head));
        assert_eq!(
            fixture.journal_opcode_count("host task-turn"),
            2,
            "replay must not mint a third host task-turn; journal={}",
            fixture.exec_journal()
        );
        if Instant::now() > deadline {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn is_open_done(report: &Value, expected_turns: usize, expected_last_turn: Option<&str>) -> bool {
    let status = status_object(report);
    if json_str(status, "state") != Some("open") {
        return false;
    }
    if status
        .get("last_outcome")
        .and_then(|value| json_str(value, "kind"))
        != Some("done")
    {
        return false;
    }
    let Some(turns) = status.get("turns").and_then(Value::as_array) else {
        return false;
    };
    if turns.len() != expected_turns {
        return false;
    }
    let Some(last) = turns.last() else {
        return false;
    };
    if json_str(last, "terminal") != Some("succeeded") {
        return false;
    }
    if last
        .get("outcome")
        .and_then(|value| json_str(value, "kind"))
        != Some("done")
    {
        return false;
    }
    if expected_last_turn.is_some_and(|expected| json_str(last, "turn_id") != Some(expected)) {
        return false;
    }
    json_str(status, "head_oid").is_some()
}

fn envelope_named(fixture: &ProcessFixture, path: &Path) -> Option<OperationEnvelope> {
    let name = path.file_name()?.to_string_lossy();
    let request_id = name
        .strip_prefix("op-")
        .and_then(|name| name.strip_suffix(".json"))?;
    load_operation_envelope(&fixture.laptop_controller_cache(), request_id)
        .ok()
        .flatten()
}

fn submit_envelope(fixture: &ProcessFixture) -> OperationEnvelope {
    let mut found: Vec<_> = fixture
        .envelope_paths()
        .into_iter()
        .filter_map(|path| envelope_named(fixture, &path))
        .filter(|envelope| envelope.command() == "task.submit")
        .collect();
    assert_eq!(
        found.len(),
        1,
        "enabled submit must persist one task.submit envelope; cache={:?}",
        fixture.envelope_paths()
    );
    found.remove(0)
}

fn say_envelopes(fixture: &ProcessFixture) -> Vec<OperationEnvelope> {
    fixture
        .envelope_paths()
        .into_iter()
        .filter_map(|path| envelope_named(fixture, &path))
        .filter(|envelope| envelope.command() == "task.say")
        .collect()
}

fn assert_second_host_turn_consumed_head1(
    fixture: &ProcessFixture,
    task_id: &str,
    frozen: &str,
    head1: &str,
) {
    let turns = fixture.journal_task_turns();
    assert_eq!(
        turns.len(),
        2,
        "exactly two accepted host task-turn executions; journal={}",
        fixture.exec_journal()
    );
    let first = &turns[0];
    let second = &turns[1];
    assert_eq!(journal_task_id(first), Some(task_id));
    assert_eq!(journal_task_id(second), Some(task_id));
    assert_eq!(
        journal_base_oid(first),
        Some(frozen),
        "first TaskTurnRequest turn.base_oid must be the frozen submit base; journal={}",
        fixture.exec_journal()
    );
    assert_eq!(
        journal_base_oid(second),
        Some(head1),
        "second TaskTurnRequest turn.base_oid must be the imported turn1 head; journal={}",
        fixture.exec_journal()
    );
    let job1 = journal_job_id(first).unwrap_or_else(|| {
        panic!(
            "first host task-turn must journal submit.material.job_id; journal={}",
            fixture.exec_journal()
        )
    });
    let job2 = journal_job_id(second).unwrap_or_else(|| {
        panic!(
            "second host task-turn must journal submit.material.job_id; journal={}",
            fixture.exec_journal()
        )
    });
    assert_ne!(
        job1,
        job2,
        "second execution must be a distinct job_id; journal={}",
        fixture.exec_journal()
    );
    match (journal_turn_number(first), journal_turn_number(second)) {
        (Some(first_n), Some(second_n)) => assert_ne!(
            first_n,
            second_n,
            "second execution must be a distinct turn_number; journal={}",
            fixture.exec_journal()
        ),
        _ => panic!(
            "host task-turn journal must carry TurnMaterial.turn_number; journal={}",
            fixture.exec_journal()
        ),
    }
}

fn journal_task_id(row: &Value) -> Option<&str> {
    row.get("turn")
        .and_then(|turn| json_str(turn, "task_id"))
        .or_else(|| json_str(row, "task_id"))
}

fn journal_base_oid(row: &Value) -> Option<&str> {
    row.get("turn")
        .and_then(|turn| json_str(turn, "base_oid"))
        .or_else(|| json_str(row, "base_oid"))
}

fn journal_job_id(row: &Value) -> Option<&str> {
    row.get("submit")
        .and_then(|submit| submit.get("material"))
        .and_then(|material| json_str(material, "job_id"))
        .or_else(|| json_str(row, "job_id"))
}

fn journal_turn_number(row: &Value) -> Option<u64> {
    row.get("turn")
        .and_then(|turn| turn.get("turn_number"))
        .and_then(Value::as_u64)
        .or_else(|| row.get("turn_number").and_then(Value::as_u64))
}

fn controller_transfer_git(
    fixture: &ProcessFixture,
    project_id: &str,
    worktree_id: &str,
) -> std::path::PathBuf {
    let cache_root = fixture.controller_xdg_cache.join("mac-worker");
    TransferRepo::controller_transfer_git_path(&cache_root, project_id, worktree_id)
        .expect("controller transfer git path")
}

fn assert_remote_descendant(
    fixture: &ProcessFixture,
    transfer_git: &Path,
    frozen: &str,
    head: &str,
    label: &str,
) {
    assert!(
        git_dir_object_exists(transfer_git, head),
        "{label} {head} must exist in the controller TransferRepo at {}",
        transfer_git.display()
    );
    assert!(
        git_dir_is_ancestor(transfer_git, frozen, head),
        "{label} {head} must be a descendant of frozen {frozen} in the controller TransferRepo"
    );
    assert!(
        git_dir_object_exists(&fixture.exec_git, head),
        "{label} {head} must exist in fakeexec bare Git"
    );
    assert!(
        git_dir_is_ancestor(&fixture.exec_git, frozen, head),
        "{label} {head} must be a descendant of frozen {frozen} in fakeexec Git"
    );
    assert!(
        fixture
            .controller_imported_oids()
            .iter()
            .any(|oid| oid == head),
        "{label} {head} must be recorded as a controller imported result; imported={:?}",
        fixture.controller_imported_oids()
    );
}

fn decode_rpc_json(stdout: &[u8], stderr: &[u8]) -> Value {
    let payload = decode_frame(stdout).unwrap_or_else(|error| {
        panic!(
            "rpc stdout was not a controller frame ({error}); stdout={} stderr={}",
            String::from_utf8_lossy(stdout),
            String::from_utf8_lossy(stderr)
        )
    });
    serde_json::from_slice(payload).unwrap_or_else(|_| {
        panic!(
            "rpc payload was not JSON; stdout={} stderr={}",
            String::from_utf8_lossy(stdout),
            String::from_utf8_lossy(stderr)
        )
    })
}

fn git_dir_command(git_dir: &Path, args: &[&str]) -> Output {
    let home = git_dir.parent().unwrap_or(git_dir).join("git-home");
    let _ = std::fs::create_dir_all(&home);
    let mut command = Command::new("/usr/bin/git");
    command
        .arg("--git-dir")
        .arg(git_dir)
        .env("HOME", &home)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1");
    for name in GIT_ENVIRONMENT_REMOVALS {
        command.env_remove(name);
    }
    command
        .args(args)
        .output()
        .expect("run git in isolated git-dir")
}

fn git_dir_object_exists(git_dir: &Path, oid: &str) -> bool {
    git_dir_command(git_dir, &["cat-file", "-t", oid])
        .status
        .success()
}

fn git_dir_is_ancestor(git_dir: &Path, ancestor: &str, oid: &str) -> bool {
    git_dir_command(git_dir, &["merge-base", "--is-ancestor", ancestor, oid])
        .status
        .success()
}

fn git_dir_diff_stat(git_dir: &Path, from: &str, to: &str) -> String {
    String::from_utf8_lossy(&git_dir_command(git_dir, &["diff", "--stat", from, to]).stdout)
        .into_owned()
}

fn stat_without_trailing_newline(text: &str) -> &str {
    text.trim_end_matches(['\n', '\r'])
}

fn laptop_has_object(repo: &support::GitRepo, oid: &str) -> bool {
    repo.git(&["cat-file", "-t", oid]).status.success()
}

fn git_object_type(repo: &support::GitRepo, oid: &str) -> String {
    String::from_utf8_lossy(&repo.git(&["cat-file", "-t", oid]).stdout)
        .trim()
        .to_owned()
}

fn git_is_ancestor(repo: &support::GitRepo, ancestor: &str, oid: &str) -> bool {
    repo.git(&["merge-base", "--is-ancestor", ancestor, oid])
        .status
        .success()
}
