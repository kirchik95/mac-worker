#[path = "support/fake_herdr.rs"]
mod fake_herdr;

use std::time::{Duration, Instant};

use fake_herdr::{FakeHerdr, Reply};
use mac_worker::{
    agent::{AgentKind, Question},
    herdr::{HerdrClient, HerdrSocket},
    herdr_reporter::{
        DISPLAY_AGENT, FOLLOW_TURN_COMMAND, HerdrReporter, START_BUDGET, TurnIdentity,
        WORKSPACE_LABEL, short_task_id, task_label_prefix, terminal_report, title_line, turn_label,
    },
    task::{HerdrTurnState, TaskId, TaskOutcome},
};
use serde_json::{Value, json};
use uuid::Uuid;

const PROJECT: &str = "a134bc73e96b76775646bf7abc86624e8c3d7fc21dc9cd6b2f189a14d3692a8c";
const WORKTREE: &str = "f19e100877c97466aa04b88d3f58e9037fce789c20c578a6994a428dab9fde99";
const JOB: &str = "c5f0317574864561bc1c334d20a0c2d8";

fn task_id() -> TaskId {
    TaskId::new(Uuid::from_u128(0x3714_ccef_b795_46ae_9a13_6bc2_3ccd_f6e3))
}

fn turn(number: u32) -> TurnIdentity {
    TurnIdentity {
        project_id: PROJECT.into(),
        worktree_id: WORKTREE.into(),
        job_id: JOB.into(),
        task_id: task_id(),
        turn_number: number,
        agent: AgentKind::Codex,
        title: "lock check T1".into(),
    }
}

fn reporter(server: &FakeHerdr) -> HerdrReporter {
    HerdrReporter::with_client(HerdrClient::new(HerdrSocket::at(server.path())))
}

fn workspace_list(with_mac_worker: bool) -> Reply {
    let mut workspaces = vec![json!({ "workspace_id": "w2", "label": "lms" })];
    if with_mac_worker {
        workspaces.push(json!({ "workspace_id": "w3", "label": WORKSPACE_LABEL }));
    }
    Reply::Result(json!({ "type": "workspace_list", "workspaces": workspaces }))
}

fn tab_list(tabs: &[(&str, &str)]) -> Reply {
    Reply::Result(json!({
        "type": "tab_list",
        "tabs": tabs.iter().map(|(id, label)| json!({ "tab_id": id, "label": label, "workspace_id": "w3" })).collect::<Vec<_>>()
    }))
}

fn tab_created(tab_id: &str, pane_id: &str) -> Reply {
    Reply::Result(json!({
        "type": "tab_created",
        "tab": { "tab_id": tab_id, "label": "x", "workspace_id": "w3" },
        "root_pane": { "pane_id": pane_id, "tab_id": tab_id, "workspace_id": "w3" }
    }))
}

fn idle_shell() -> Reply {
    Reply::Result(json!({
        "type": "pane_process_info",
        "process_info": { "shell_pid": 500, "foreground_processes": [{ "name": "zsh", "pid": 500 }] }
    }))
}

fn busy_shell() -> Reply {
    Reply::Result(json!({
        "type": "pane_process_info",
        "process_info": { "shell_pid": 500, "foreground_processes": [{ "name": "zsh", "pid": 500 }, { "name": "cargo", "pid": 501 }] }
    }))
}

fn methods(server: &FakeHerdr) -> Vec<String> {
    server
        .requests()
        .iter()
        .map(|request| request["method"].as_str().unwrap_or("").to_owned())
        .collect()
}

fn params(server: &FakeHerdr, method: &str, index: usize) -> Value {
    server.requests_for(method)[index]["params"].clone()
}

#[test]
fn labels_and_titles_use_the_short_task_id() {
    let id = task_id();
    assert_eq!(short_task_id(id), "3714ccefb795");
    assert_eq!(task_label_prefix(id), "task 3714ccefb795");
    assert_eq!(turn_label(id, 2), "task 3714ccefb795 · turn 2");
    assert_eq!(
        title_line(id, "lock check T1"),
        "task 3714ccefb795 · lock check T1"
    );
}

#[test]
fn start_reuses_the_workspace_sweeps_the_task_arms_the_pane_and_reports_working() {
    let home = tempfile::tempdir().unwrap();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply("workspace.list", workspace_list(true));
    server.reply(
        "tab.list",
        tab_list(&[
            ("w3:t1", "1"),
            ("w3:t2", "task 3714ccefb795 · turn 1"),
            ("w3:t3", "task 0000deadbeef · turn 4"),
        ]),
    );
    server.reply("tab.create", tab_created("w3:t4", "w3:p4"));
    server.reply("pane.process_info", busy_shell());
    server.reply("pane.process_info", idle_shell());

    let reported = reporter(&server).start(&turn(2));

    assert_eq!(reported.report.state, HerdrTurnState::Attached);
    assert_eq!(reported.report.pane_id.as_deref(), Some("w3:p4"));
    assert_eq!(reported.diagnostic, None);
    assert_eq!(
        methods(&server),
        vec![
            "workspace.list",
            "tab.list",
            "tab.close",
            "tab.create",
            "pane.process_info",
            "pane.process_info",
            "pane.send_input",
            "pane.report_agent",
            "pane.report_metadata",
        ]
    );
    assert_eq!(params(&server, "tab.close", 0)["tab_id"], "w3:t2");
    let created = params(&server, "tab.create", 0);
    assert_eq!(created["workspace_id"], "w3");
    assert_eq!(created["label"], "task 3714ccefb795 · turn 2");
    assert_eq!(created["focus"], false);
    assert!(created.get("cwd").is_none(), "no path crosses the socket");
    let input = params(&server, "pane.send_input", 0);
    assert_eq!(input["pane_id"], "w3:p4");
    assert_eq!(
        input["text"],
        format!("{FOLLOW_TURN_COMMAND} {PROJECT} {WORKTREE} {JOB}")
    );
    assert_eq!(input["keys"], json!(["enter"]));
    let agent = params(&server, "pane.report_agent", 0);
    assert_eq!(agent["pane_id"], "w3:p4");
    assert_eq!(agent["source"], "mac-worker");
    assert_eq!(agent["agent"], "codex");
    assert_eq!(agent["state"], "working");
    assert_eq!(agent["message"], "lock check T1");
    let metadata = params(&server, "pane.report_metadata", 0);
    assert_eq!(metadata["title"], "task 3714ccefb795 · lock check T1");
    assert_eq!(metadata["display_agent"], DISPLAY_AGENT);
    assert_eq!(metadata["state_labels"]["working"], "turn 2");
    assert_eq!(metadata["tokens"]["task"], "3714ccefb795");
    assert_eq!(metadata["tokens"]["turn"], "2");
    assert_eq!(
        metadata["tokens"]["mw_title"],
        "task 3714ccefb795 · lock check T1"
    );
    assert_eq!(metadata["tokens"]["mw_agent"], "codex");
    assert_eq!(metadata["tokens"]["mw_outcome"], "running");
}

#[test]
fn start_creates_the_workspace_when_it_is_missing() {
    let home = tempfile::tempdir().unwrap();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply("workspace.list", workspace_list(false));
    server.reply(
        "workspace.create",
        Reply::Result(json!({
            "type": "workspace_created",
            "workspace": { "workspace_id": "w9", "label": WORKSPACE_LABEL },
            "tab": { "tab_id": "w9:t1", "workspace_id": "w9" },
            "root_pane": { "pane_id": "w9:p1" }
        })),
    );
    server.reply("tab.list", tab_list(&[("w9:t1", "1")]));
    server.reply("tab.create", tab_created("w9:t2", "w9:p2"));
    server.reply("pane.process_info", idle_shell());

    let reported = reporter(&server).start(&turn(1));

    assert_eq!(reported.report.pane_id.as_deref(), Some("w9:p2"));
    let created = params(&server, "workspace.create", 0);
    assert_eq!(created["label"], WORKSPACE_LABEL);
    assert_eq!(created["focus"], false);
    assert!(created.get("cwd").is_none());
    assert_eq!(params(&server, "tab.create", 0)["workspace_id"], "w9");
    assert!(!methods(&server).contains(&"tab.close".to_owned()));
}

#[test]
fn a_shell_that_never_settles_gets_no_command_but_the_state_is_still_reported() {
    let home = tempfile::tempdir().unwrap();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply("workspace.list", workspace_list(true));
    server.reply("tab.list", tab_list(&[]));
    server.reply("tab.create", tab_created("w3:t4", "w3:p4"));
    for _ in 0..64 {
        server.reply("pane.process_info", busy_shell());
    }

    let started = Instant::now();
    let reported = reporter(&server).start(&turn(1));

    assert_eq!(reported.report.state, HerdrTurnState::Attached);
    assert!(started.elapsed() < START_BUDGET, "{:?}", started.elapsed());
    assert!(
        started.elapsed() >= Duration::from_secs(4),
        "{:?}",
        started.elapsed()
    );
    let methods = methods(&server);
    assert!(!methods.contains(&"pane.send_input".to_owned()));
    assert_eq!(
        methods.last().map(String::as_str),
        Some("pane.report_metadata")
    );
}

#[test]
fn an_absent_herdr_makes_the_turn_unavailable_at_once() {
    let home = tempfile::tempdir().unwrap();
    let reporter = HerdrReporter::for_home(home.path());

    let started = Instant::now();
    let reported = reporter.start(&turn(1));

    assert_eq!(reported.report.state, HerdrTurnState::Unavailable);
    assert_eq!(reported.report.pane_id, None);
    assert_eq!(
        reported.diagnostic.as_deref(),
        Some("herdr reporter: unavailable (absent)")
    );
    assert!(started.elapsed() < Duration::from_millis(500));
}

#[test]
fn a_silent_herdr_makes_the_turn_unavailable_within_the_deadline() {
    let home = tempfile::tempdir().unwrap();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply("workspace.list", Reply::Silence);

    let started = Instant::now();
    let reported = reporter(&server).start(&turn(1));

    assert_eq!(reported.report.state, HerdrTurnState::Unavailable);
    assert_eq!(
        reported.diagnostic.as_deref(),
        Some("herdr reporter: unavailable (timeout)")
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
}

#[test]
fn terminal_reports_every_outcome_with_the_documented_state_and_message() {
    let question = Question::open("which database?");
    let cases: Vec<(TaskOutcome, &str, &str, &str)> = vec![
        (TaskOutcome::Done, "idle", "done", "slept 120 seconds"),
        (
            TaskOutcome::NeedsInput,
            "blocked",
            "needs_input",
            "which database?",
        ),
        (
            TaskOutcome::Blocked,
            "blocked",
            "blocked",
            "slept 120 seconds",
        ),
        (
            TaskOutcome::Unknown,
            "unknown",
            "unknown",
            "agent reported no structured result",
        ),
        (
            TaskOutcome::Failed {
                reason: "AGENT_EXITED".into(),
            },
            "unknown",
            "failed",
            "AGENT_EXITED",
        ),
        (TaskOutcome::Cancelled, "unknown", "cancelled", "cancelled"),
        (TaskOutcome::TimedOut, "unknown", "timed_out", "timed out"),
        (TaskOutcome::Lost, "unknown", "lost", "lost"),
    ];
    for (outcome, state, kind, message) in &cases {
        let (got_state, got_kind, got_message) = terminal_report(
            outcome,
            Some("slept 120 seconds"),
            std::slice::from_ref(&question),
        );
        assert_eq!(got_state.as_str(), *state, "{outcome:?}");
        assert_eq!(got_kind, *kind, "{outcome:?}");
        assert_eq!(got_message, *message, "{outcome:?}");
    }

    let home = tempfile::tempdir().unwrap();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply("pane.process_info", idle_shell());
    let reported = reporter(&server).terminal(
        &turn(1),
        Some("w3:p4"),
        &TaskOutcome::NeedsInput,
        Some("slept"),
        std::slice::from_ref(&question),
    );
    assert_eq!(reported.report.pane_id.as_deref(), Some("w3:p4"));
    assert_eq!(
        methods(&server),
        vec![
            "pane.process_info",
            "pane.report_agent",
            "pane.report_metadata"
        ]
    );
    let agent = params(&server, "pane.report_agent", 0);
    assert_eq!(agent["state"], "blocked");
    assert_eq!(agent["message"], "which database?");
    let metadata = params(&server, "pane.report_metadata", 0);
    assert_eq!(metadata["tokens"]["mw_outcome"], "needs_input");
    assert_eq!(metadata["state_labels"]["unknown"], "needs_input");
}

#[test]
fn terminal_reopens_the_tab_when_the_recorded_pane_is_gone() {
    let home = tempfile::tempdir().unwrap();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply(
        "pane.process_info",
        Reply::Error {
            code: "pane_not_found".into(),
            message: "gone".into(),
        },
    );
    server.reply("workspace.list", workspace_list(true));
    server.reply("tab.list", tab_list(&[]));
    server.reply("tab.create", tab_created("w3:t8", "w3:p8"));
    server.reply("pane.process_info", idle_shell());

    let reported = reporter(&server).terminal(
        &turn(1),
        Some("w3:p4"),
        &TaskOutcome::Done,
        Some("done"),
        &[],
    );

    assert_eq!(reported.report.state, HerdrTurnState::Attached);
    assert_eq!(reported.report.pane_id.as_deref(), Some("w3:p8"));
    assert_eq!(params(&server, "pane.report_agent", 0)["pane_id"], "w3:p8");
    assert_eq!(params(&server, "pane.report_agent", 0)["state"], "idle");
}

#[test]
fn close_removes_only_the_tabs_of_the_task() {
    let home = tempfile::tempdir().unwrap();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply("workspace.list", workspace_list(true));
    server.reply(
        "tab.list",
        tab_list(&[
            ("w3:t1", "1"),
            ("w3:t2", "task 3714ccefb795 · turn 1"),
            ("w3:t3", "task 3714ccefb795 · turn 2"),
            ("w3:t4", "task 0000deadbeef · turn 1"),
        ]),
    );

    // Another task still lives in the workspace afterwards.
    server.reply(
        "tab.list",
        tab_list(&[("w3:t1", "1"), ("w3:t4", "task 0000deadbeef · turn 1")]),
    );

    reporter(&server).close(task_id()).unwrap();

    let closed: Vec<Value> = server
        .requests_for("tab.close")
        .iter()
        .map(|request| request["params"]["tab_id"].clone())
        .collect();
    assert_eq!(closed, vec![json!("w3:t2"), json!("w3:t3")]);
    assert!(server.requests_for("workspace.close").is_empty());
}

#[test]
fn close_removes_the_workspace_when_only_its_first_tab_remains() {
    let home = tempfile::tempdir().unwrap();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply("workspace.list", workspace_list(true));
    server.reply(
        "tab.list",
        tab_list(&[("w3:t1", "1"), ("w3:t2", "task 3714ccefb795 · turn 1")]),
    );
    server.reply("tab.list", tab_list(&[("w3:t1", "1")]));

    reporter(&server).close(task_id()).unwrap();

    assert_eq!(
        methods(&server),
        vec![
            "workspace.list",
            "tab.list",
            "tab.close",
            "tab.list",
            "workspace.close"
        ]
    );
    assert_eq!(params(&server, "workspace.close", 0)["workspace_id"], "w3");
}

#[test]
fn close_keeps_the_workspace_while_the_operator_has_a_tab_in_it() {
    let home = tempfile::tempdir().unwrap();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply("workspace.list", workspace_list(true));
    server.reply(
        "tab.list",
        tab_list(&[
            ("w3:t1", "1"),
            ("w3:t2", "task 3714ccefb795 · turn 1"),
            ("w3:t9", "2"),
        ]),
    );
    server.reply("tab.list", tab_list(&[("w3:t1", "1"), ("w3:t9", "2")]));

    reporter(&server).close(task_id()).unwrap();

    assert_eq!(params(&server, "tab.close", 0)["tab_id"], "w3:t2");
    assert!(server.requests_for("workspace.close").is_empty());
}

#[test]
fn close_without_a_workspace_touches_nothing() {
    let home = tempfile::tempdir().unwrap();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply("workspace.list", workspace_list(false));

    reporter(&server).close(task_id()).unwrap();

    assert_eq!(methods(&server), vec!["workspace.list"]);
}

#[test]
fn sweep_orphans_closes_tabs_of_tasks_that_are_no_longer_live() {
    let home = tempfile::tempdir().unwrap();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply("workspace.list", workspace_list(true));
    server.reply(
        "tab.list",
        tab_list(&[
            ("w3:t1", "1"),
            ("w3:t2", "task 3714ccefb795 · turn 1"),
            ("w3:t3", "task 0000deadbeef · turn 3"),
            ("w3:t4", "task not-a-task-id · turn 1"),
        ]),
    );

    server.reply(
        "tab.list",
        tab_list(&[
            ("w3:t1", "1"),
            ("w3:t2", "task 3714ccefb795 · turn 1"),
            ("w3:t4", "task not-a-task-id · turn 1"),
        ]),
    );

    let closed = reporter(&server)
        .sweep_orphans(&|short| short == "3714ccefb795")
        .unwrap();

    assert_eq!(closed, 1);
    assert_eq!(params(&server, "tab.close", 0)["tab_id"], "w3:t3");
    assert!(server.requests_for("workspace.close").is_empty());
}

#[test]
fn sweep_orphans_removes_the_workspace_it_emptied() {
    let home = tempfile::tempdir().unwrap();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply("workspace.list", workspace_list(true));
    server.reply(
        "tab.list",
        tab_list(&[("w3:t1", "1"), ("w3:t3", "task 0000deadbeef · turn 3")]),
    );
    server.reply("tab.list", tab_list(&[("w3:t1", "1")]));

    let closed = reporter(&server).sweep_orphans(&|_| false).unwrap();

    assert_eq!(closed, 1);
    assert_eq!(params(&server, "workspace.close", 0)["workspace_id"], "w3");
}

#[test]
fn nothing_that_crosses_the_socket_is_a_path_a_prompt_or_a_secret() {
    let home = tempfile::tempdir().unwrap();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply("workspace.list", workspace_list(false));
    server.reply(
        "workspace.create",
        Reply::Result(json!({
            "type": "workspace_created",
            "workspace": { "workspace_id": "w9" },
            "tab": { "tab_id": "w9:t1" },
            "root_pane": { "pane_id": "w9:p1" }
        })),
    );
    server.reply("tab.list", tab_list(&[]));
    server.reply("tab.create", tab_created("w9:t2", "w9:p2"));
    server.reply("pane.process_info", idle_shell());
    let reporter = reporter(&server);
    let mut turn = turn(1);
    turn.title = "PLANTED_TITLE".into();
    reporter.start(&turn);
    server.reply("pane.process_info", idle_shell());
    reporter.terminal(
        &turn,
        Some("w9:p2"),
        &TaskOutcome::Failed {
            reason: "AGENT_EXITED".into(),
        },
        Some("summary line"),
        &[],
    );

    for request in server.requests() {
        let text = request.to_string();
        for forbidden in [
            "/Users/",
            "/home/",
            "/private/",
            "/var/",
            "/tmp/",
            "PLANTED_PROMPT",
        ] {
            assert!(!text.contains(forbidden), "{forbidden} in {text}");
        }
        let tildes = text.matches('~').count();
        let helper = text.matches(FOLLOW_TURN_COMMAND).count();
        assert_eq!(
            tildes, helper,
            "the only tilde is the helper command: {text}"
        );
    }
}
