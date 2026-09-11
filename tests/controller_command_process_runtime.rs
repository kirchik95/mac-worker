//! Process-level `task list`, `task reconcile` and run-aware `task wait` proof.
//!
//! Harness (`harness_*`) compiles and runs on the b3 base: it only checks real
//! public CLI grammar. Runtime tests (`runtime_*`) assert FLOW-owned enabled
//! routing through the actual worker binary, private child homes, the existing
//! fake SSH hop / fake agent-host and real Git. They stay present without
//! `#[ignore]` and fail with `blocked FLOW seam` messages until FLOW's
//! combined runtime lands; they are NOT acceptance on this base.
//!
//! No production/shared-support/other-test edits. No seeded completion, no
//! canned ACKs, no weakened equality. Reuses the private `ProcessFixture`
//! lifecycle and reap guards (`OwnedChild` drops kill and reap), plus one
//! uniquely owned test-private transport tap for controller wait RPC (the
//! shared fake SSH journal only covers fakeexec, never controller frames).

#[path = "support/controller_process.rs"]
mod controller_process;
mod support;

use std::io::{BufReader, Read};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use controller_process::{OwnedChild, ProcessFixture, TEST_SSH_ENV};
use mac_worker::{
    cli::{Cli, Command as WorkerCommand, TaskCommand},
    controller::{
        BatchKind, FrozenBatchBody, OperationEnvelope, decode_frame, encode_json_frame,
        load_operation_envelope,
    },
    dag::DagBase,
    protocol::PROTOCOL_VERSION,
};
use serde_json::{Value, json};

const TASK_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";
const RUN_ID: &str = "0193f0f4a6b5c7d8e9f00112233445566";

/// Real public grammar for the commands this leaf owns. Runs green on b3.
#[test]
fn harness_command_grammar_list_reconcile_wait_run() {
    let list = Cli::try_parse_from(["worker", "task", "list"]).unwrap();
    assert!(matches!(
        list.command,
        WorkerCommand::Task {
            command: TaskCommand::List {
                run: None,
                state: None,
                outcome: None,
                full: false,
            }
        }
    ));

    let filtered = Cli::try_parse_from([
        "worker",
        "task",
        "list",
        "--run",
        RUN_ID,
        "--state",
        "open",
        "--outcome",
        "done",
        "--full",
    ])
    .unwrap();
    assert!(matches!(
        filtered.command,
        WorkerCommand::Task {
            command: TaskCommand::List {
                run: Some(_),
                state: Some(_),
                outcome: Some(_),
                full: true,
            }
        }
    ));

    let reconcile = Cli::try_parse_from(["worker", "task", "reconcile"]).unwrap();
    assert!(matches!(
        reconcile.command,
        WorkerCommand::Task {
            command: TaskCommand::Reconcile,
        }
    ));

    let wait_run = Cli::try_parse_from(["worker", "task", "wait", "--run", RUN_ID]).unwrap();
    assert!(matches!(
        wait_run.command,
        WorkerCommand::Task {
            command: TaskCommand::Wait {
                task_id: None,
                run: Some(_),
                timeout: None,
            }
        }
    ));

    let wait_task = Cli::try_parse_from([
        "worker",
        "task",
        "wait",
        "--task-id",
        TASK_ID,
        "--timeout",
        "45s",
    ])
    .unwrap();
    assert!(matches!(
        wait_task.command,
        WorkerCommand::Task {
            command: TaskCommand::Wait {
                task_id: Some(_),
                run: None,
                timeout: Some(_),
            }
        }
    ));

    Cli::try_parse_from(["worker", "task", "logs", TASK_ID, "--turn", "1", "--raw"])
        .unwrap_or_else(|error| panic!("task logs --turn/--raw: {error}"));
    Cli::try_parse_from(["worker", "task", "logs", TASK_ID, "--follow"])
        .unwrap_or_else(|error| panic!("task logs --follow: {error}"));

    let submit_never = Cli::try_parse_from([
        "worker",
        "task",
        "submit",
        "--prompt",
        "stays open",
        "--wip",
        "--no-wait",
        "--close-on",
        "never",
    ])
    .unwrap();
    assert!(matches!(
        submit_never.command,
        WorkerCommand::Task {
            command: TaskCommand::Submit {
                wip: true,
                no_wait: true,
                close_on: Some(_),
                ..
            }
        }
    ));
}

/// Helper validation for the owned wait tap's marker selection (runs green
/// on b3, no worker needed): the actual final response comes from the atomic
/// `last-completed` marker, never lexical PID order. The script's streaming
/// passthrough shape is source-reviewed here and awaits the combined runtime
/// for behavioral proof.
#[test]
fn harness_wait_tap_marker_ignores_pid_order() {
    let fixture = ProcessFixture::new();
    let tap = install_wait_poll_tap(&fixture);
    assert!(tap.script.is_absolute());
    assert_ne!(tap.script, fixture.fake_ssh);
    let mode = std::fs::metadata(&tap.script).unwrap().permissions().mode();
    assert_ne!(mode & 0o111, 0, "tap script must be executable");

    // Lexically larger stem holds the older non-quiescent response (PID
    // boundary/wrap); the marker must still select the true final one.
    let scratch = tempfile::TempDir::new().unwrap();
    let unit = WaitPollTap {
        dir: scratch.path().to_owned(),
        script: PathBuf::new(),
    };
    write_tap_exchange(&unit, "hop.9999.1700000000", "req-old", false, &[TASK_ID]);
    write_tap_exchange(
        &unit,
        "hop.100.1700000001",
        "req-final",
        true,
        &[TASK_ID, "028f0f4a6b5c7d8e9f00112233445566"],
    );
    std::fs::write(unit.dir.join("last-completed"), "hop.100.1700000001").unwrap();
    let all = read_poll_exchanges(&unit);
    assert_eq!(all.len(), 2, "both complete pairs must be visible");
    let final_exchange = final_poll_exchange(&unit).expect("marker-selected final exchange");
    assert_eq!(final_exchange.request_id, "req-final");
    assert_eq!(
        final_exchange.response_id.as_deref(),
        Some("req-final"),
        "response must echo its exact request ID"
    );
    assert_eq!(final_exchange.quiescent, Some(true));
    assert_eq!(final_exchange.task_ids.len(), 2);
}

fn write_tap_exchange(
    tap: &WaitPollTap,
    stem: &str,
    request_id: &str,
    quiescent: bool,
    task_ids: &[&str],
) {
    let request = encode_json_frame(&json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": request_id,
        "command": "task.wait.poll",
        "body": { "run": RUN_ID },
    }))
    .unwrap();
    let response = encode_json_frame(&json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": request_id,
        "command": "task.wait.poll",
        "payload_sha256": "00",
        "result": { "task_ids": task_ids, "quiescent": quiescent, "exit_code": 0 },
    }))
    .unwrap();
    std::fs::write(tap.dir.join(format!("{stem}.req.bin")), request).unwrap();
    std::fs::write(tap.dir.join(format!("{stem}.resp.bin")), response).unwrap();
}

/// Enabled submit, then `task list` must return the actual task identity and
/// status and `task reconcile` must succeed without another host execution.
#[test]
fn runtime_command_list_and_reconcile_after_enabled_submit() {
    let fixture = ProcessFixture::new();
    let repo = support_git_repo();
    let mut leader = fixture.spawn_controller_run();
    fixture.wait_until_leader_ready(&mut leader);

    let (status, stdout, stderr) = fixture.run_laptop(
        &[
            "--json",
            "task",
            "submit",
            "--prompt",
            "command leaf: list and reconcile after enabled submit",
            "--wip",
            "--close-on",
            "never",
            "--no-wait",
        ],
        Some(repo.root()),
    );
    let stdout_text = String::from_utf8_lossy(&stdout);
    let stderr_text = String::from_utf8_lossy(&stderr);
    assert!(
        status.success(),
        "blocked FLOW seam: enabled task submit must ACK through host controller-rpc; stdout={stdout_text} stderr={stderr_text}"
    );
    assert!(
        !fixture.laptop_state().exists(),
        "blocked FLOW seam: laptop must not open ClientStateStore; path={:?}",
        fixture.laptop_state()
    );
    let submit = parse_json_value(&stdout);
    let task_id = json_string(&submit, "task_id")
        .unwrap_or_else(|| panic!("submit ACK JSON must include task_id; stdout={stdout_text}"))
        .to_owned();
    let expected_turn = json_string(&submit, "turn_id").map(str::to_owned);
    let envelope = envelope_from_laptop(&fixture);
    assert_eq!(
        envelope.command(),
        "task.submit",
        "ordinary CLI submit must persist a real task.submit envelope"
    );
    // Exact frozen submit contract: base_oid only (no oid/head_oid fallbacks),
    // with the stable task/turn IDs from the typed body matching the ACK.
    let frozen = envelope
        .body()
        .get("base_oid")
        .and_then(Value::as_str)
        .unwrap_or_else(|| {
            panic!(
                "task.submit envelope must carry FrozenSubmitBody.base_oid; body={:?}",
                envelope.body()
            )
        })
        .to_owned();
    assert_eq!(
        envelope.body().get("close_on").and_then(Value::as_str),
        Some("never"),
        "task.submit envelope must freeze the requested open result policy; body={:?}",
        envelope.body()
    );
    assert_eq!(
        envelope.body().get("task_id").and_then(Value::as_str),
        Some(task_id.as_str()),
        "frozen submit task_id must be stable with the ACK; body={:?}",
        envelope.body()
    );
    if let Some(expected_turn) = expected_turn.as_deref() {
        assert_eq!(
            envelope.body().get("turn_id").and_then(Value::as_str),
            Some(expected_turn),
            "frozen submit turn_id must be stable with the ACK; body={:?}",
            envelope.body()
        );
    }

    let completed = wait_for_terminal_turn(&fixture, &repo, &task_id, expected_turn.as_deref(), 30);
    let status = status_object(&completed);
    assert_eq!(
        json_string(status, "state"),
        Some("open"),
        "close_on=never submit must stay Open after Done; status={completed}"
    );
    let result_oid = json_string(status, "head_oid")
        .expect("terminal status must include head_oid")
        .to_owned();
    assert_ne!(
        result_oid, frozen,
        "result head must be a fakeexec descendant of the transferred frozen base"
    );
    let status_state = json_string(status, "state").unwrap_or("").to_owned();
    assert!(
        turn_completed_with_oid(&completed, &result_oid, expected_turn.as_deref()),
        "terminal status must match the imported result; status={completed}"
    );

    // Establish quiescence through the public task wait (not just terminal
    // status) before list/reconcile.
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
    let wait_text = String::from_utf8_lossy(&wait_stdout);
    let wait_err = String::from_utf8_lossy(&wait_stderr);
    assert!(
        wait_status.success(),
        "blocked FLOW seam: public task wait must confirm quiescence; stdout={wait_text} stderr={wait_err}"
    );
    let waited = parse_json_value(&wait_stdout);
    assert_eq!(
        waited
            .get("task_ids")
            .and_then(Value::as_array)
            .map(|ids| ids.iter().filter_map(Value::as_str).collect::<Vec<_>>()),
        Some(vec![task_id.as_str()]),
        "public wait must return the actual task; stdout={wait_text}"
    );
    assert_eq!(
        waited.get("exit_code").and_then(Value::as_u64),
        Some(0),
        "quiescent done task must exit 0; stdout={wait_text}"
    );

    // Required scope: `task list` through the enabled public CLI returns the
    // actual task identity and status with the real TaskListJson schema.
    let (list_status, list_stdout, list_stderr) =
        fixture.run_laptop(&["--json", "task", "list"], Some(repo.root()));
    let list_text = String::from_utf8_lossy(&list_stdout);
    let list_err = String::from_utf8_lossy(&list_stderr);
    assert!(
        list_status.success(),
        "blocked FLOW seam: enabled task list must succeed; stdout={list_text} stderr={list_err}"
    );
    let list = parse_json_value(&list_stdout);
    assert_eq!(
        list.get("protocol_version").and_then(Value::as_u64),
        Some(u64::from(PROTOCOL_VERSION)),
        "task list JSON must carry the real protocol_version; stdout={list_text}"
    );
    for key in ["tasks", "runs", "progress"] {
        assert!(
            list.get(key).is_some(),
            "task list JSON must carry `{key}` (TaskListJson schema); stdout={list_text}"
        );
    }
    let tasks = list
        .get("tasks")
        .and_then(Value::as_array)
        .expect("task list JSON tasks must be an array");
    let row = tasks
        .iter()
        .find(|task| task.get("task_id").and_then(Value::as_str) == Some(task_id.as_str()))
        .unwrap_or_else(|| {
            panic!("task list must return the actual submitted task {task_id}; stdout={list_text}")
        });
    assert_eq!(
        row.get("state").and_then(Value::as_str),
        Some(status_state.as_str()),
        "task list row state must equal the actual task status; row={row} status={completed}"
    );
    assert_eq!(
        row.get("last_outcome")
            .and_then(|outcome| outcome.get("kind"))
            .and_then(Value::as_str),
        Some("done"),
        "task list row must report the actual terminal outcome; row={row}"
    );

    // Required scope: actual `task reconcile` succeeds with the real
    // ReconcileReport schema and starts no further host execution.
    let turns_before = fixture.journal_task_turns().len();
    assert_eq!(
        turns_before,
        1,
        "one execution before reconcile; journal={}",
        fixture.exec_journal()
    );
    let (rc_status, rc_stdout, rc_stderr) =
        fixture.run_laptop(&["--json", "task", "reconcile"], Some(repo.root()));
    let rc_text = String::from_utf8_lossy(&rc_stdout);
    let rc_err = String::from_utf8_lossy(&rc_stderr);
    assert!(
        rc_status.success(),
        "blocked FLOW seam: enabled task reconcile must succeed; stdout={rc_text} stderr={rc_err}"
    );
    let reconcile = parse_json_value(&rc_stdout);
    assert_eq!(
        reconcile.get("protocol_version").and_then(Value::as_u64),
        Some(u64::from(PROTOCOL_VERSION)),
        "reconcile JSON must carry the real protocol_version; stdout={rc_text}"
    );
    for key in ["replaced_runners", "repaired_rows"] {
        assert!(
            reconcile.get(key).and_then(Value::as_u64).is_some(),
            "reconcile JSON must carry numeric `{key}` (ReconcileReport schema); stdout={rc_text}"
        );
    }
    assert_eq!(
        reconcile.get("started_runners").and_then(Value::as_u64),
        Some(0),
        "reconcile on the quiescent task must start no runners; stdout={rc_text}"
    );
    assert_eq!(
        fixture.journal_task_turns().len(),
        turns_before,
        "reconcile must not create another host task-turn for the completed task; journal={}",
        fixture.exec_journal()
    );
    let again = wait_for_terminal_turn(&fixture, &repo, &task_id, expected_turn.as_deref(), 30);
    assert!(
        turn_completed_with_oid(&again, &result_oid, expected_turn.as_deref()),
        "task must stay terminal at the same result after reconcile; status={again}"
    );

    // Concise post-quiescence coverage of the documented log options.
    let (raw_status, _, raw_err) = fixture.run_laptop(
        &["task", "logs", &task_id, "--turn", "1", "--raw"],
        Some(repo.root()),
    );
    assert!(
        raw_status.success(),
        "blocked FLOW seam: task logs --turn/--raw must read the quiescent turn; stderr={}",
        String::from_utf8_lossy(&raw_err)
    );
    let (follow_status, _, follow_err) =
        fixture.run_laptop(&["task", "logs", &task_id, "--follow"], Some(repo.root()));
    assert!(
        follow_status.success(),
        "blocked FLOW seam: task logs --follow must terminate after quiescence; stderr={}",
        String::from_utf8_lossy(&follow_err)
    );
    assert_eq!(
        fixture.journal_task_turns().len(),
        1,
        "host execution count must remain 1 after quiescence and subsequent reads; journal={}",
        fixture.exec_journal()
    );

    assert!(
        !fixture.laptop_state().exists(),
        "laptop state must never acquire authoritative task/queue/turn records; path={:?}",
        fixture.laptop_state()
    );
    drop(leader);
}

/// Dynamic run-aware wait: start `task wait --run` while the DAG child is
/// still unsubmitted, prove it is polling via the private tap's actual
/// non-quiescent reply (no timing-only sleeps), close the parent via the
/// actual CLI, and require wait to admit and await the child before returning
/// the complete task set. Parent terminal status is not quiescence; public
/// `task wait --task-id` on the parent is the precondition before `--run`.
#[test]
fn runtime_command_wait_run_admits_child_after_parent_close() {
    let fixture = ProcessFixture::new();
    let repo = support_git_repo();
    let batch = fixture.laptop_home.join("from-parent.toml");
    write_from_parent_batch(&batch);

    let mut leader = fixture.spawn_controller_run();
    fixture.wait_until_leader_ready(&mut leader);

    let (status, stdout, stderr) = fixture.run_laptop(
        &["--json", "task", "batch", batch.to_str().unwrap()],
        Some(repo.root()),
    );
    let stdout_text = String::from_utf8_lossy(&stdout);
    let stderr_text = String::from_utf8_lossy(&stderr);
    assert!(
        status.success(),
        "blocked FLOW seam: enabled DAG batch must ACK; stdout={stdout_text} stderr={stderr_text}"
    );
    let submit = parse_json_value(&stdout);
    let envelope = envelope_from_laptop(&fixture);
    assert_eq!(envelope.command(), "task.batch");
    let body: FrozenBatchBody =
        serde_json::from_value(envelope.body().clone()).expect("envelope body is FrozenBatchBody");
    assert_eq!(body.kind, BatchKind::Dag);
    let parent_node = body.nodes.get("parent").expect("parent node");
    let child_node = body.nodes.get("child").expect("child node");
    assert!(matches!(child_node.base, DagBase::From { .. }));
    let parent_task = parent_node.task_id.to_string();
    let child_task = child_node.task_id.to_string();
    let parent_turn = parent_node.turn_id.to_string();
    let child_turn = child_node.turn_id.to_string();
    let run_id = body.run_id.to_string();
    assert_eq!(
        json_string(&submit, "run_id"),
        Some(run_id.as_str()),
        "batch ACK run_id must match the frozen DAG run"
    );

    let parent_done = wait_for_terminal_turn(&fixture, &repo, &parent_task, Some(&parent_turn), 45);
    let parent_status = status_object(&parent_done);
    assert_eq!(json_string(parent_status, "state"), Some("open"));
    let parent_result = terminal_head_oid(&parent_done, Some(&parent_turn))
        .expect("parent Done head")
        .to_owned();
    assert_eq!(
        fixture.journal_task_turns().len(),
        1,
        "child must not execute while close_on=never parent is Open+Done; journal={}",
        fixture.exec_journal()
    );

    // Terminal Open+Done is not operator-idle: close is TASK_BUSY while
    // runner() is still recorded. Public wait on the parent task (not --run)
    // is the fixture precondition; it must return before dynamic run wait.
    let (parent_wait_status, parent_wait_stdout, parent_wait_stderr) = fixture.run_laptop(
        &[
            "--json",
            "task",
            "wait",
            "--task-id",
            &parent_task,
            "--timeout",
            "45s",
        ],
        Some(repo.root()),
    );
    let parent_wait_text = String::from_utf8_lossy(&parent_wait_stdout);
    let parent_wait_err = String::from_utf8_lossy(&parent_wait_stderr);
    assert!(
        parent_wait_status.success(),
        "blocked FLOW seam: public task wait --task-id must confirm parent quiescence before wait --run; stdout={parent_wait_text} stderr={parent_wait_err} journal={} controller={}",
        fixture.exec_journal(),
        dump_controller_authority(&fixture, &parent_task)
    );
    let parent_waited = parse_json_value(&parent_wait_stdout);
    assert_eq!(
        parent_waited
            .get("task_ids")
            .and_then(Value::as_array)
            .map(|ids| ids.iter().filter_map(Value::as_str).collect::<Vec<_>>()),
        Some(vec![parent_task.as_str()]),
        "parent wait must return the parent task; stdout={parent_wait_text}"
    );
    assert_eq!(
        parent_waited.get("exit_code").and_then(Value::as_u64),
        Some(0),
        "quiescent done parent must exit 0; stdout={parent_wait_text}"
    );
    assert_eq!(
        fixture.journal_task_turns().len(),
        1,
        "parent wait --task-id must not start the child; journal={}",
        fixture.exec_journal()
    );

    // Start the actual `task wait --run` while the child is still unsubmitted.
    // The private tap overrides MAC_WORKER_TEST_SSH on this child only and
    // reaps with the child via OwnedChild Drop on panic/timeout.
    let tap = install_wait_poll_tap(&fixture);
    let (mut wait, wait_out, wait_err) =
        spawn_laptop_wait_run(&fixture, &tap, &run_id, repo.root());
    assert_wait_polling_while_child_pending(&fixture, &mut wait, &tap, &run_id);

    // Admit the child through the actual CLI.
    let pre_close_authority = dump_controller_authority(&fixture, &parent_task);
    let (close_status, close_stdout, close_stderr) = fixture.run_laptop(
        &["--json", "task", "close", &parent_task],
        Some(repo.root()),
    );
    let close_text = String::from_utf8_lossy(&close_stdout);
    let close_err = String::from_utf8_lossy(&close_stderr);
    assert!(
        close_status.success(),
        "blocked FLOW seam: parent accept is actual enabled task close; stdout={close_text} stderr={close_err} journal={} controller={}",
        fixture.exec_journal(),
        pre_close_authority
    );
    let close = parse_json_value(&close_stdout);
    assert_eq!(
        json_string(&close, "task_id"),
        Some(parent_task.as_str()),
        "close must accept the actual parent task"
    );
    assert_eq!(
        status_object(&close)
            .get("head_oid")
            .and_then(Value::as_str),
        Some(parent_result.as_str()),
        "exact accepted parent OID must be the admitted child base; close={close}"
    );

    // Wait must include/await the newly admitted child and finish only after
    // the run is quiescent, returning the actual complete task set. The
    // returned real poll response (captured in the tap) must itself be
    // quiescent with both IDs: no post-wait polling that could mask an early
    // return.
    let (wait_stdout, wait_stderr) = finish_laptop_wait(
        &fixture,
        &mut wait,
        wait_out,
        wait_err,
        Duration::from_secs(45),
    );
    // The actual final completed response is the marker-selected exchange,
    // never lexical `.last()`: PID boundaries/wrap can order an older
    // non-quiescent response after the true final one.
    let final_poll = final_poll_exchange(&tap)
        .expect("tap must publish a last-completed marker for the final poll exchange");
    assert_eq!(
        final_poll.command, "task.wait.poll",
        "final captured exchange must be a wait poll"
    );
    assert_eq!(
        final_poll.run.as_deref(),
        Some(run_id.as_str()),
        "final captured exchange must poll this run"
    );
    assert_eq!(
        final_poll.response_id.as_deref(),
        Some(final_poll.request_id.as_str()),
        "final poll response must echo its exact request ID"
    );
    assert_eq!(
        final_poll.quiescent,
        Some(true),
        "the actual final poll response must be quiescent"
    );
    assert_eq!(
        sorted_strings(&final_poll.task_ids),
        sorted_strings(&[parent_task.clone(), child_task.clone()]),
        "the actual final poll response must return both task IDs"
    );
    assert_eq!(
        final_poll.exit_code,
        Some(0),
        "the actual final poll response must exit 0"
    );
    let wait_text = String::from_utf8_lossy(&wait_stdout);
    let wait_report = parse_json_value(&wait_stdout);
    assert_eq!(
        wait_report.get("protocol_version").and_then(Value::as_u64),
        Some(u64::from(PROTOCOL_VERSION)),
        "wait JSON must carry the real protocol_version; stdout={wait_text} stderr={}",
        String::from_utf8_lossy(&wait_stderr)
    );
    let waited = wait_report
        .get("task_ids")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("wait JSON must carry task_ids; stdout={wait_text}"))
        .iter()
        .map(|item| {
            item.as_str()
                .unwrap_or_else(|| panic!("wait task_ids must be strings; stdout={wait_text}"))
                .to_owned()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        sorted_strings(&waited),
        sorted_strings(&[parent_task.clone(), child_task.clone()]),
        "wait --run must return the actual complete task set including the admitted child; stdout={wait_text}"
    );
    assert_eq!(
        wait_report.get("exit_code").and_then(Value::as_u64),
        Some(0),
        "quiescent done run must exit 0; stdout={wait_text}"
    );

    // Immediate inspection after the wait returns: single child status read
    // (no polling until completion, no further wait/reconcile that could let
    // a prematurely returned wait pass after the leader finishes the child).
    let (child_status_rc, child_stdout, child_stderr) = fixture.run_laptop(
        &["--json", "task", "status", &child_task],
        Some(repo.root()),
    );
    let child_text = String::from_utf8_lossy(&child_stdout);
    let child_err = String::from_utf8_lossy(&child_stderr);
    assert!(
        child_status_rc.success(),
        "child status must be readable immediately after wait returns; stdout={child_text} stderr={child_err}"
    );
    let child_done = parse_json_value(&child_stdout);
    let child_result = terminal_head_oid(&child_done, Some(&child_turn))
        .expect("child must already be terminal when wait returns")
        .to_owned();
    assert_ne!(child_result, parent_result);

    let turns = fixture.journal_task_turns();
    assert_eq!(
        turns.len(),
        2,
        "two executions total (parent + admitted child); journal={}",
        fixture.exec_journal()
    );
    let child_row = turns
        .iter()
        .find(|row| row.get("task_id").and_then(Value::as_str) == Some(child_task.as_str()))
        .expect("child task-turn after parent close");
    assert_eq!(
        child_row.get("base_oid").and_then(Value::as_str),
        Some(parent_result.as_str()),
        "child base from:parent must be the exact accepted/imported parent OID"
    );
    let imported = fixture.controller_imported_oids();
    assert_eq!(
        sorted_strings(&imported),
        sorted_strings(&[parent_result.clone(), child_result.clone()]),
        "controller imported heads must be the exact parent and child result commits"
    );

    for (task, expected) in [(&parent_task, &parent_result), (&child_task, &child_result)] {
        let (fetch_status, fetch_out, fetch_err) =
            fixture.run_laptop(&["--json", "task", "fetch", task], Some(repo.root()));
        assert!(
            fetch_status.success(),
            "fetch {task} must bind this checkout/task; stdout={} stderr={}",
            String::from_utf8_lossy(&fetch_out),
            String::from_utf8_lossy(&fetch_err)
        );
        assert_eq!(
            json_string(&parse_json_value(&fetch_out), "head_oid"),
            Some(expected.as_str()),
            "fetched OID must equal the actual result for {task}"
        );
        assert!(git_object_exists(&repo, expected));
    }
    assert!(git_is_ancestor(&repo, &parent_result, &child_result));
    assert!(
        !fixture.laptop_state().exists(),
        "laptop state must never acquire authoritative task/queue/turn records; path={:?}",
        fixture.laptop_state()
    );
    drop(leader);
}

/// Uniquely owned, test-private transport tap for controller wait RPC.
///
/// Root-authorized: the shared `ProcessFixture` journal only covers fakeexec,
/// never controller frames, so this tap captures the actual `task.wait.poll`
/// request AND the real response. It overrides `MAC_WORKER_TEST_SSH` only on
/// the spawned wait child (never global env, never the shared fixture).
/// Only controller-rpc control exchanges are captured; every other hop
/// execs the original `fixture.fake_ssh` directly with full streaming, and
/// both delegation paths restore `MAC_WORKER_TEST_SSH` to the original seam
/// so nested descendants never re-enter the tap (no Git deadlock). Each
/// captured hop writes one request file and one response file atomically
/// (temp + rename), then atomically publishes a `last-completed` marker with
/// the finished stem: the single wait child polls sequentially, so the
/// marker — never lexical PID order — identifies the actual final completed
/// response. Rust parses files with the existing `decode_frame` +
/// `serde_json` framing, never invented framing or fabricated replies.
struct WaitPollTap {
    dir: PathBuf,
    script: PathBuf,
}

fn install_wait_poll_tap(fixture: &ProcessFixture) -> WaitPollTap {
    let dir = fixture.laptop_home.join("wait-poll-tap");
    std::fs::create_dir_all(&dir).unwrap();
    let script = dir.join("tap-ssh");
    std::fs::write(
        &script,
        r#"#!/bin/sh
# Test-private transparent tap: capture only controller-rpc control
# exchanges. Every other hop execs the original seam directly with full
# streaming (no stdin buffering: a Git pack hop can wait for a server
# greeting while buffered stdin waits for EOF -> deadlock). The tap override
# reaches nested children through the hop environment, so BOTH delegation
# paths restore MAC_WORKER_TEST_SSH to the original seam: controller/runner
# descendants always use the original fixture hop, never this tap.
if [ -z "$MAC_WORKER_TAP_REAL_SSH" ] || [ -z "$MAC_WORKER_TAP_DIR" ]; then
  echo "wait-poll tap misconfigured" >&2
  exit 70
fi
capture=0
for arg in "$@"; do
  case "$arg" in
    *controller-rpc*) capture=1 ;;
  esac
done
if [ "$capture" -eq 0 ]; then
  MAC_WORKER_TEST_SSH="$MAC_WORKER_TAP_REAL_SSH" exec "$MAC_WORKER_TAP_REAL_SSH" "$@"
fi
name="hop.$$.$(date +%s)"
in_tmp="$MAC_WORKER_TAP_DIR/$name.req.tmp"
out_tmp="$MAC_WORKER_TAP_DIR/$name.resp.tmp"
err_tmp="$MAC_WORKER_TAP_DIR/$name.err.tmp"
marker_tmp="$MAC_WORKER_TAP_DIR/$name.marker.tmp"
cat > "$in_tmp"
MAC_WORKER_TEST_SSH="$MAC_WORKER_TAP_REAL_SSH" "$MAC_WORKER_TAP_REAL_SSH" "$@" < "$in_tmp" > "$out_tmp" 2> "$err_tmp"
code=$?
cat "$err_tmp" >&2
cat "$out_tmp"
mv "$in_tmp" "$MAC_WORKER_TAP_DIR/$name.req.bin"
mv "$out_tmp" "$MAC_WORKER_TAP_DIR/$name.resp.bin"
printf '%s' "$name" > "$marker_tmp"
mv "$marker_tmp" "$MAC_WORKER_TAP_DIR/last-completed"
rm -f "$err_tmp"
exit $code
"#,
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&script).unwrap().permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&script, permissions).unwrap();
    WaitPollTap { dir, script }
}

/// One captured poll exchange with exact request/response identity.
struct PollExchange {
    request_id: String,
    command: String,
    run: Option<String>,
    response_id: Option<String>,
    quiescent: Option<bool>,
    task_ids: Vec<String>,
    exit_code: Option<u64>,
}

fn read_poll_exchanges(tap: &WaitPollTap) -> Vec<PollExchange> {
    let entries = std::fs::read_dir(&tap.dir).unwrap();
    let mut stems: Vec<String> = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(stem) = name.strip_suffix(".req.bin") {
            stems.push(stem.to_owned());
        }
    }
    // Iteration order is arbitrary directory order: callers must not treat
    // it as capture order. The actual final response comes only from the
    // atomically published `last-completed` marker (see below).
    stems
        .into_iter()
        .filter_map(|stem| read_exchange(tap, &stem))
        .collect()
}

/// Read one complete request/response pair. Returns `None` while the hop is
/// still in flight (response file not yet committed) or when either side
/// does not parse as exactly one controller frame.
fn read_exchange(tap: &WaitPollTap, stem: &str) -> Option<PollExchange> {
    if stem.is_empty() || stem.contains('/') || stem.contains('\\') || stem.contains('\0') {
        return None;
    }
    let req_bytes = std::fs::read(tap.dir.join(format!("{stem}.req.bin"))).ok()?;
    let resp_bytes = std::fs::read(tap.dir.join(format!("{stem}.resp.bin"))).ok()?;
    let req_payload = decode_frame(&req_bytes).ok()?;
    let resp_payload = decode_frame(&resp_bytes).ok()?;
    let req: Value = serde_json::from_slice(req_payload).ok()?;
    let resp: Value = serde_json::from_slice(resp_payload).ok()?;
    let request_id = req.get("request_id").and_then(Value::as_str)?;
    let command = req.get("command").and_then(Value::as_str)?;
    let body = req.get("body");
    let result = resp.get("result");
    Some(PollExchange {
        request_id: request_id.to_owned(),
        command: command.to_owned(),
        run: body
            .and_then(|body| body.get("run"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        response_id: resp
            .get("request_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        quiescent: result
            .and_then(|result| result.get("quiescent"))
            .and_then(Value::as_bool),
        task_ids: result
            .and_then(|result| result.get("task_ids"))
            .and_then(Value::as_array)
            .map(|ids| {
                ids.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default(),
        exit_code: result
            .and_then(|result| result.get("exit_code"))
            .and_then(Value::as_u64),
    })
}

/// The actual final completed exchange: the stem named by the atomically
/// published `last-completed` marker. Lexical filename order is NOT capture
/// order (PID boundaries/wrap), so `.last()` over sorted names must never be
/// used to select the final response.
fn final_poll_exchange(tap: &WaitPollTap) -> Option<PollExchange> {
    let stem = std::fs::read_to_string(tap.dir.join("last-completed")).ok()?;
    read_exchange(tap, stem.trim())
}

fn spawn_laptop_wait_run(
    fixture: &ProcessFixture,
    tap: &WaitPollTap,
    run_id: &str,
    repo_root: &Path,
) -> (OwnedChild, mpsc::Receiver<Vec<u8>>, mpsc::Receiver<Vec<u8>>) {
    let mut command = fixture.laptop_worker();
    command.args(["--json", "task", "wait", "--run", run_id]);
    command.current_dir(repo_root);
    // Tap override applies ONLY to this spawned wait child.
    command.env(TEST_SSH_ENV, &tap.script);
    command.env("MAC_WORKER_TAP_REAL_SSH", &fixture.fake_ssh);
    command.env("MAC_WORKER_TAP_DIR", &tap.dir);
    let mut child = OwnedChild::spawn(&mut command);
    let stdout_pipe = child.take_stdout();
    let stderr_pipe = child.take_stderr();
    let (out_tx, out_rx) = mpsc::channel();
    let (err_tx, err_rx) = mpsc::channel();
    thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = BufReader::new(stdout_pipe).read_to_end(&mut buf);
        let _ = out_tx.send(buf);
    });
    thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = BufReader::new(stderr_pipe).read_to_end(&mut buf);
        let _ = err_tx.send(buf);
    });
    (child, out_rx, err_rx)
}

/// Prove the spawned `task wait --run` has begun polling while the DAG child
/// is still unsubmitted. The barrier observes this run's actual
/// non-quiescent `task.wait.poll` reply in the private tap (exact
/// command/body.run/request identity between request and response), not
/// merely bytes written before forwarding. The wait child must stay alive
/// and unexited, and the child execution must not have started (exactly one
/// `host task-turn`: the parent). Bounded 15s loop; no timing-only sleeps.
fn assert_wait_polling_while_child_pending(
    fixture: &ProcessFixture,
    wait: &mut OwnedChild,
    tap: &WaitPollTap,
    run_id: &str,
) {
    let pid = wait.id();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            process_alive(pid),
            "spawned task wait --run must stay alive while the child is unsubmitted"
        );
        assert!(
            wait.wait_timeout(Duration::from_millis(0)).is_none(),
            "spawned task wait --run must not exit before the run is quiescent"
        );
        assert_eq!(
            fixture.journal_task_turns().len(),
            1,
            "wait polling must not start the child execution; journal={}",
            fixture.exec_journal()
        );
        if poll_exchanges(tap).iter().any(|exchange| {
            exchange.command == "task.wait.poll"
                && exchange.run.as_deref() == Some(run_id)
                && exchange.response_id.as_deref() == Some(exchange.request_id.as_str())
                && exchange.quiescent == Some(false)
        }) {
            return;
        }
        if Instant::now() > deadline {
            panic!(
                "blocked FLOW seam: task wait --run never observed a non-quiescent task.wait.poll reply for run {run_id} while the child was unsubmitted; journal={}",
                fixture.exec_journal()
            );
        }
        thread::sleep(Duration::from_millis(100));
    }
}

/// Captured `task.wait.poll` exchanges for this run, in capture order, with
/// exact request/response identity (response echoes the request ID).
fn poll_exchanges(tap: &WaitPollTap) -> Vec<PollExchange> {
    read_poll_exchanges(tap)
        .into_iter()
        .filter(|exchange| {
            exchange.command == "task.wait.poll"
                && exchange.response_id.as_deref() == Some(exchange.request_id.as_str())
        })
        .collect()
}

fn finish_laptop_wait(
    fixture: &ProcessFixture,
    wait: &mut OwnedChild,
    out_rx: mpsc::Receiver<Vec<u8>>,
    err_rx: mpsc::Receiver<Vec<u8>>,
    timeout: Duration,
) -> (Vec<u8>, Vec<u8>) {
    match wait.wait_timeout(timeout) {
        Some(status) => assert!(
            status.success(),
            "task wait --run must exit 0 after quiescence; status={status} journal={}",
            fixture.exec_journal()
        ),
        None => panic!(
            "task wait --run did not finish within {timeout:?} after parent close; journal={}",
            fixture.exec_journal()
        ),
    }
    let stdout = out_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap_or_default();
    let stderr = err_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap_or_default();
    (stdout, stderr)
}

fn process_alive(pid: u32) -> bool {
    let status = unsafe { libc::kill(pid as i32, 0) };
    status == 0
}

fn parse_json_value(stdout: &[u8]) -> Value {
    serde_json::from_slice(stdout).unwrap_or_else(|_| {
        let text = String::from_utf8_lossy(stdout);
        panic!("expected JSON stdout, got: {text}");
    })
}

fn json_string<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn sorted_strings(values: &[String]) -> Vec<String> {
    let mut values = values.to_vec();
    values.sort();
    values
}

fn status_object(report: &Value) -> &Value {
    report.get("status").unwrap_or(report)
}

fn terminal_head_oid<'a>(report: &'a Value, expected_turn: Option<&str>) -> Option<&'a str> {
    let status = status_object(report);
    let state = json_string(status, "state").unwrap_or("");
    if !matches!(state, "open" | "closed") {
        return None;
    }
    let outcome = status
        .get("last_outcome")
        .and_then(|value| json_string(value, "kind"))
        .unwrap_or("");
    if outcome != "done" {
        return None;
    }
    let turns = status.get("turns").and_then(Value::as_array)?;
    if turns.len() != 1 {
        return None;
    }
    let turn = &turns[0];
    if json_string(turn, "terminal") != Some("succeeded") {
        return None;
    }
    if let Some(expected_turn) = expected_turn
        && json_string(turn, "turn_id") != Some(expected_turn)
    {
        return None;
    }
    json_string(status, "head_oid")
}

fn turn_completed_with_oid(
    report: &Value,
    expected_oid: &str,
    expected_turn: Option<&str>,
) -> bool {
    terminal_head_oid(report, expected_turn) == Some(expected_oid)
}

fn envelope_from_laptop(fixture: &ProcessFixture) -> OperationEnvelope {
    let paths = fixture.envelope_paths();
    assert_eq!(
        paths.len(),
        1,
        "enabled submit must persist one laptop OperationEnvelope; found {paths:?}"
    );
    let name = paths[0].file_name().unwrap().to_string_lossy().to_string();
    let request_id = name
        .strip_prefix("op-")
        .and_then(|name| name.strip_suffix(".json"))
        .unwrap_or_else(|| panic!("unexpected envelope name {name}"));
    load_operation_envelope(&fixture.laptop_controller_cache(), request_id)
        .expect("load envelope")
        .unwrap_or_else(|| panic!("missing envelope {request_id}"))
}

fn git_object_exists(repo: &support::GitRepo, oid: &str) -> bool {
    repo.git(&["cat-file", "-t", oid]).status.success()
}

fn git_is_ancestor(repo: &support::GitRepo, ancestor: &str, oid: &str) -> bool {
    repo.git(&["merge-base", "--is-ancestor", ancestor, oid])
        .status
        .success()
}

fn dump_controller_authority(fixture: &ProcessFixture, task_id: &str) -> String {
    let root = fixture.controller_state();
    let mut files = Vec::new();
    for name in ["tasks", "queue", "runners"] {
        collect_regular_files(&root.join(name), &root, &mut files);
    }
    files.sort();
    let mut lines = Vec::new();
    for rel in files {
        let path = root.join(&rel);
        let body = std::fs::read_to_string(&path).unwrap_or_default();
        let summary = authority_file_summary(task_id, &body);
        lines.push(format!("{rel}: {summary}"));
    }
    if lines.is_empty() {
        format!("<empty {}>", root.display())
    } else {
        lines.join(" | ")
    }
}

fn collect_regular_files(root: &Path, base: &Path, found: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_regular_files(&path, base, found);
        } else if path.is_file() {
            found.push(
                path.strip_prefix(base)
                    .unwrap_or(&path)
                    .display()
                    .to_string(),
            );
        }
    }
}

fn authority_file_summary(task_id: &str, body: &str) -> String {
    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return format!("bytes={}", body.len());
    };
    let state = value
        .pointer("/status/state")
        .or_else(|| value.get("state"))
        .and_then(Value::as_str)
        .unwrap_or("-");
    let runner = value.get("runner").is_some_and(|runner| !runner.is_null());
    let close_intent = value
        .get("close_intent")
        .is_some_and(|intent| !intent.is_null());
    let queue_states = value
        .get("entries")
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| {
                    let state = entry.get("state")?;
                    if let Some(name) = state.as_str() {
                        Some(name.to_owned())
                    } else {
                        state
                            .as_object()
                            .and_then(|object| object.keys().next().cloned())
                    }
                })
                .collect::<Vec<_>>()
                .join(",")
        })
        .filter(|states| !states.is_empty());
    let mentions_task = body.contains(task_id);
    match queue_states {
        Some(states) => format!(
            "state={state} runner={runner} close_intent={close_intent} queue={states} task={mentions_task} bytes={}",
            body.len()
        ),
        None => format!(
            "state={state} runner={runner} close_intent={close_intent} task={mentions_task} bytes={}",
            body.len()
        ),
    }
}

fn wait_for_terminal_turn(
    fixture: &ProcessFixture,
    repo: &support::GitRepo,
    task_id: &str,
    expected_turn: Option<&str>,
    timeout_secs: u64,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        let (status, stdout, stderr) =
            fixture.run_laptop(&["--json", "task", "status", task_id], Some(repo.root()));
        let last_status = String::from_utf8_lossy(&stdout);
        let last_stderr = String::from_utf8_lossy(&stderr);
        assert!(
            status.success(),
            "reconnect status must not open a laptop store; stdout={last_status} stderr={last_stderr}"
        );
        let report = parse_json_value(&stdout);
        if terminal_head_oid(&report, expected_turn).is_some() {
            return report;
        }
        if Instant::now() > deadline {
            panic!(
                "blocked FLOW seam: controller/runner did not reach a terminal succeeded turn; status={last_status} stderr={last_stderr} journal={}",
                fixture.exec_journal()
            );
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn write_from_parent_batch(path: &Path) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    std::fs::write(
        path,
        "version = 1\n\n[[tasks]]\nid = \"parent\"\nprompt = \"parent root stays open after Done\"\nclose_on = \"never\"\n\n[[tasks]]\nid = \"child\"\ndepends_on = [\"parent\"]\nbase = \"from:parent\"\nprompt = \"child from accepted parent\"\n",
    )
    .unwrap();
}

fn support_git_repo() -> support::GitRepo {
    let repo = support::GitRepo::init();
    repo.write("src.txt", b"fixture source\n");
    repo.commit_all("fixture");
    repo
}
