#[allow(dead_code)]
mod support;

use std::io::{self, Write};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy},
    client_state::ClientStateStore,
    config::Config,
    error::WorkerError,
    paths::PathLayout,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    task::{
        ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskId, TaskLimits, TaskMeta,
        TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnId, TurnSummary,
        TurnTerminal,
    },
    task_client::TaskClient,
    turn_runner::InlineRunnerExecutor,
};

struct NoProcesses;

impl ProcessRunner for NoProcesses {
    fn run(&self, _: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        panic!("reading a local task log must not launch a process");
    }
}

struct Fixture {
    _root: tempfile::TempDir,
    paths: PathLayout,
    store: ClientStateStore,
    config: Config,
    task_id: TaskId,
}

impl Fixture {
    fn new(agent: AgentKind, state: TaskState, turns: Vec<TurnSummary>) -> Self {
        let root = tempfile::tempdir().unwrap();
        let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
        let store = ClientStateStore::open(&paths.state).unwrap();
        let task_id = TaskId::generate();
        let meta = TaskMeta::new(TaskMetaInput {
            task_id,
            run_id: None,
            project_id: "a".repeat(64),
            worktree_id: "b".repeat(64),
            agent,
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
            title: None,
            prompt: "private prompt body".into(),
            created_at_millis: 1,
        })
        .unwrap();
        let status = TaskStatus::new(
            state,
            turns.last().and_then(TurnSummary::outcome).cloned(),
            None,
            false,
            Some(meta.base_oid().clone()),
            None,
            Vec::new(),
            Vec::new(),
            None,
            turns,
            2,
        )
        .unwrap();
        store
            .create_task(
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
                .unwrap(),
            )
            .unwrap();
        Self {
            _root: root,
            paths,
            store,
            config: Config::parse("version = 1\nworkers = []\n").unwrap(),
            task_id,
        }
    }

    fn append(&self, turn_id: TurnId, bytes: &[u8]) {
        self.store
            .open_runner_log(self.task_id, turn_id)
            .unwrap()
            .write_all(bytes)
            .unwrap();
        checkpoint(self, turn_id, None);
    }

    fn logs(&self, turn: Option<u32>, raw: bool) -> Result<Vec<u8>, WorkerError> {
        let mut stdout = Vec::new();
        self.client()
            .logs(self.task_id, turn, false, raw, &mut stdout, &mut Vec::new())?;
        Ok(stdout)
    }

    fn client(&self) -> TaskClient<'_> {
        TaskClient::new(
            &NoProcesses,
            &self.config,
            &self.paths,
            &self.store,
            &InlineRunnerExecutor,
        )
    }

    fn finish(&self, turn_id: TurnId) {
        self.finish_as(
            turn_id,
            TaskState::Closed,
            TaskOutcome::failed("agent exited 1"),
        );
    }

    fn finish_as(&self, turn_id: TurnId, state: TaskState, outcome: TaskOutcome) {
        let record = self.store.load_task(self.task_id).unwrap();
        let turn = TurnSummary::new(
            1,
            turn_id,
            Some(TurnTerminal::Failed),
            Some(outcome.clone()),
            Some(false),
            false,
            Some(1),
            Some(2),
        );
        let status = TaskStatus::new(
            state,
            Some(outcome.clone()),
            None,
            false,
            Some(record.meta().base_oid().clone()),
            None,
            Vec::new(),
            Vec::new(),
            None,
            vec![turn],
            2,
        )
        .unwrap();
        self.store
            .update_task(record.with_status(status).unwrap())
            .unwrap();
        checkpoint(self, turn_id, Some(outcome));
    }
}

// Append real log bytes at a poll boundary, without timing-dependent sleeps
// or a background thread. A bounded flush count prevents a broken follower
// from hanging the test suite.
struct PollWriter<F> {
    bytes: Vec<u8>,
    polls: usize,
    on_flush: F,
}

impl<F: FnMut(usize, &[u8])> Write for PollWriter<F> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.polls += 1;
        if self.polls > 5 {
            return Err(io::Error::other("task log follow did not finish"));
        }
        (self.on_flush)(self.polls, &self.bytes);
        Ok(())
    }
}

fn active_turn() -> TurnSummary {
    TurnSummary::new(
        1,
        TurnId::generate(),
        None,
        None,
        None,
        false,
        Some(1),
        None,
    )
}

fn finished_turn(number: u32, outcome: TaskOutcome) -> TurnSummary {
    let terminal = match outcome {
        TaskOutcome::Failed { .. } => TurnTerminal::Failed,
        TaskOutcome::Cancelled => TurnTerminal::Cancelled,
        TaskOutcome::TimedOut => TurnTerminal::TimedOut,
        TaskOutcome::Lost => TurnTerminal::Lost,
        _ => TurnTerminal::Succeeded,
    };
    TurnSummary::new(
        number,
        TurnId::generate(),
        Some(terminal),
        Some(outcome),
        Some(false),
        false,
        Some(1),
        Some(2),
    )
}

#[test]
fn normal_logs_show_plain_agent_diagnostics_for_every_adapter() {
    // Regression: adapter-only rendering silently drops stderr on auth/launch failure.
    for agent in [
        AgentKind::Codex,
        AgentKind::Claude,
        AgentKind::Cursor,
        AgentKind::Opencode,
    ] {
        let turn = finished_turn(1, TaskOutcome::failed("agent exited 1"));
        let fixture = Fixture::new(agent, TaskState::Open, vec![turn.clone()]);
        fixture.append(
            turn.turn_id(),
            b"Authentication required. Please sign in.\nCheck the selected env profile.\n",
        );
        let output = String::from_utf8(fixture.logs(None, false).unwrap()).unwrap();
        assert!(
            output.contains("Authentication required. Please sign in.\n"),
            "{agent:?}: {output:?}"
        );
        assert!(output.contains("Check the selected env profile.\n"));
    }
}

#[test]
fn normal_logs_show_failure_even_when_the_agent_wrote_nothing() {
    // Regression: a terminal task with an empty log looks like a silent success.
    let turn = finished_turn(1, TaskOutcome::failed("agent exited 17"));
    let fixture = Fixture::new(AgentKind::Codex, TaskState::Open, vec![turn.clone()]);
    fixture.append(turn.turn_id(), b"");
    assert_eq!(
        fixture.logs(None, false).unwrap(),
        b"turn 1 failed: agent exited 17\n"
    );
    assert_eq!(fixture.logs(None, true).unwrap(), b"");
}

#[test]
fn normal_logs_identify_cancelled_timed_out_and_lost_turns() {
    for (outcome, expected) in [
        (TaskOutcome::Cancelled, "turn 1 cancelled\n"),
        (TaskOutcome::TimedOut, "turn 1 timed_out\n"),
        (TaskOutcome::Lost, "turn 1 lost\n"),
    ] {
        let turn = finished_turn(1, outcome);
        let fixture = Fixture::new(AgentKind::Codex, TaskState::Open, vec![turn.clone()]);
        fixture.append(turn.turn_id(), b"");
        assert_eq!(fixture.logs(None, false).unwrap(), expected.as_bytes());
    }
}

#[test]
fn cancellation_before_runner_start_is_reported_without_a_log_file() {
    let turn = finished_turn(1, TaskOutcome::Cancelled);
    let fixture = Fixture::new(AgentKind::Codex, TaskState::Open, vec![turn]);
    assert_eq!(fixture.logs(None, false).unwrap(), b"turn 1 cancelled\n");
    assert!(matches!(
        fixture.logs(None, true),
        Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound
    ));
}

#[test]
fn missing_logs_without_a_recorded_failure_still_return_an_error() {
    let fixture = Fixture::new(AgentKind::Codex, TaskState::Active, vec![active_turn()]);
    assert!(matches!(
        fixture.logs(None, false),
        Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound
    ));
}

#[test]
fn recorded_failure_does_not_hide_an_unsafe_log_file() {
    use std::os::unix::fs::PermissionsExt;

    let turn = finished_turn(1, TaskOutcome::failed("agent exited 1"));
    let fixture = Fixture::new(AgentKind::Codex, TaskState::Open, vec![turn.clone()]);
    let log = fixture
        .store
        .open_runner_log(fixture.task_id, turn.turn_id())
        .unwrap();
    log.set_permissions(std::fs::Permissions::from_mode(0o666))
        .unwrap();
    assert!(matches!(fixture.logs(None, false), Err(WorkerError::Io(_))));
}

#[test]
fn historical_logs_report_the_selected_turn_outcome() {
    let failed = finished_turn(1, TaskOutcome::failed("PUBLISH_FAILED"));
    let done = finished_turn(2, TaskOutcome::Done);
    let fixture = Fixture::new(
        AgentKind::Codex,
        TaskState::Open,
        vec![failed.clone(), done.clone()],
    );
    fixture.append(failed.turn_id(), b"");
    fixture.append(done.turn_id(), b"");
    assert_eq!(
        fixture.logs(Some(1), false).unwrap(),
        b"turn 1 failed: PUBLISH_FAILED\n"
    );
    assert_eq!(fixture.logs(None, false).unwrap(), b"");
    assert_eq!(
        fixture.logs(Some(3), false).unwrap_err().public_code(),
        "TASK_LOG_NOT_FOUND"
    );
}

#[test]
fn raw_logs_preserve_mixed_bytes_and_normal_logs_keep_event_formatting() {
    let turn = finished_turn(1, TaskOutcome::Done);
    let fixture = Fixture::new(AgentKind::Codex, TaskState::Open, vec![turn.clone()]);
    let mut bytes = br#"{"type":"item.completed","item":{"type":"agent_message","text":"working"}}
{"type":"turn_accepted","private_metadata":"do not render"}
warning: invalid byte "#
        .to_vec();
    bytes.extend_from_slice(b"\xff\n");
    fixture.append(turn.turn_id(), &bytes);
    assert_eq!(fixture.logs(None, true).unwrap(), bytes);
    let rendered = fixture.logs(None, false).unwrap();
    assert_eq!(
        rendered,
        "working\naccepted\nwarning: invalid byte \u{fffd}\n".as_bytes()
    );
    assert!(
        !String::from_utf8_lossy(&rendered).contains("do not render"),
        "unrecognised structured events must not leak payload: {}",
        String::from_utf8_lossy(&rendered)
    );
}

#[test]
fn first_turn_diagnostics_are_readable_before_host_acceptance() {
    // The first TurnSummary is published by the host only after acceptance.
    let fixture = Fixture::new(AgentKind::Codex, TaskState::Queued, Vec::new());
    let turn_id = TurnId::generate();
    fixture
        .store
        .write_turn_prompt(fixture.task_id, turn_id, "private prompt body")
        .unwrap();
    let diagnostic = b"exited: CAPACITY_BUSY workers=mini-1\n";
    fixture.append(turn_id, diagnostic);
    assert_eq!(fixture.logs(None, false).unwrap(), diagnostic);
    assert_eq!(fixture.logs(Some(1), false).unwrap(), diagnostic);
    assert_eq!(fixture.logs(None, true).unwrap(), diagnostic);
    assert_eq!(
        fixture.logs(Some(2), false).unwrap_err().public_code(),
        "TASK_LOG_NOT_FOUND"
    );
}

#[test]
fn logs_do_not_guess_an_unaccepted_turn_from_multiple_directories() {
    let fixture = Fixture::new(AgentKind::Codex, TaskState::Queued, Vec::new());
    for _ in 0..2 {
        let turn_id = TurnId::generate();
        fixture
            .store
            .write_turn_prompt(fixture.task_id, turn_id, "private prompt body")
            .unwrap();
        fixture.append(turn_id, b"exited: CAPACITY_BUSY\n");
    }
    assert_eq!(
        fixture.logs(None, false).unwrap_err().public_code(),
        "TASK_LOG_NOT_FOUND"
    );
}

#[test]
fn follow_keeps_json_and_utf8_lines_whole_across_polls() {
    let turn = active_turn();
    let fixture = Fixture::new(AgentKind::Codex, TaskState::Active, vec![turn.clone()]);
    let event = "{\"type\":\"error\",\"message\":\"Ошибка авторизации\"}\n";
    let split = event.find('О').unwrap() + 1;
    fixture.append(turn.turn_id(), &event.as_bytes()[..split]);
    let mut output = PollWriter {
        bytes: Vec::new(),
        polls: 0,
        on_flush: |poll, bytes: &[u8]| match poll {
            1 => {
                assert!(
                    bytes.is_empty(),
                    "a partial event must not become raw output"
                );
                fixture.append(turn.turn_id(), &event.as_bytes()[split..]);
            }
            2 => fixture.finish(turn.turn_id()),
            _ => {}
        },
    };
    fixture
        .client()
        .logs(
            fixture.task_id,
            None,
            true,
            false,
            &mut output,
            &mut Vec::new(),
        )
        .unwrap();
    assert_eq!(
        output.bytes,
        "Ошибка авторизации\nturn 1 failed: agent exited 1\n".as_bytes()
    );
}

#[test]
fn follow_reads_bytes_written_before_the_terminal_status_update() {
    // Regression: loading terminal status after reading the file skips the
    // diagnostics appended between the two reads, including a final fragment.
    let turn = active_turn();
    let fixture = Fixture::new(AgentKind::Codex, TaskState::Active, vec![turn.clone()]);
    fixture.append(turn.turn_id(), b"");
    let mut output = PollWriter {
        bytes: Vec::new(),
        polls: 0,
        on_flush: |poll, _: &[u8]| {
            if poll == 1 {
                fixture.append(turn.turn_id(), b"Authentication required");
                fixture.finish(turn.turn_id());
            }
        },
    };
    fixture
        .client()
        .logs(
            fixture.task_id,
            None,
            true,
            false,
            &mut output,
            &mut Vec::new(),
        )
        .unwrap();
    assert_eq!(
        output.bytes,
        b"Authentication required\nturn 1 failed: agent exited 1\n"
    );
}

#[test]
fn raw_follow_streams_partial_bytes_without_repeating_them() {
    let turn = active_turn();
    let fixture = Fixture::new(AgentKind::Codex, TaskState::Active, vec![turn.clone()]);
    fixture.append(turn.turn_id(), b"\xffpartial");
    let mut output = PollWriter {
        bytes: Vec::new(),
        polls: 0,
        on_flush: |poll, bytes: &[u8]| {
            if poll == 1 {
                assert_eq!(bytes, b"\xffpartial");
                fixture.append(turn.turn_id(), b" line\n");
            } else if poll == 2 {
                fixture.finish(turn.turn_id());
            }
        },
    };
    fixture
        .client()
        .logs(
            fixture.task_id,
            None,
            true,
            true,
            &mut output,
            &mut Vec::new(),
        )
        .unwrap();
    assert_eq!(output.bytes, b"\xffpartial line\n");
}

#[test]
fn follow_reports_an_open_turn_failure_once_before_the_task_is_closed() {
    let turn = finished_turn(1, TaskOutcome::failed("agent exited 1"));
    let fixture = Fixture::new(AgentKind::Codex, TaskState::Open, vec![turn.clone()]);
    fixture.append(turn.turn_id(), b"");
    let mut output = PollWriter {
        bytes: Vec::new(),
        polls: 0,
        on_flush: |poll, bytes: &[u8]| {
            assert_eq!(bytes, b"turn 1 failed: agent exited 1\n");
            if poll == 2 {
                fixture.finish(turn.turn_id());
            }
        },
    };
    fixture
        .client()
        .logs(
            fixture.task_id,
            None,
            true,
            false,
            &mut output,
            &mut Vec::new(),
        )
        .unwrap();
    assert_eq!(output.bytes, b"turn 1 failed: agent exited 1\n");
}

#[test]
fn follow_reports_a_later_publication_failure_while_the_task_is_open() {
    let turn = finished_turn(1, TaskOutcome::failed("agent exited 1"));
    let fixture = Fixture::new(AgentKind::Codex, TaskState::Open, vec![turn.clone()]);
    fixture.append(turn.turn_id(), b"");
    let mut output = PollWriter {
        bytes: Vec::new(),
        polls: 0,
        on_flush: |poll, bytes: &[u8]| match poll {
            1 => {
                assert_eq!(bytes, b"turn 1 failed: agent exited 1\n");
                fixture.finish_as(
                    turn.turn_id(),
                    TaskState::Open,
                    TaskOutcome::failed("PUBLISH_FAILED"),
                );
            }
            2 => {
                assert_eq!(
                    bytes,
                    b"turn 1 failed: agent exited 1\nturn 1 failed: PUBLISH_FAILED\n"
                );
                fixture.finish_as(
                    turn.turn_id(),
                    TaskState::Closed,
                    TaskOutcome::failed("PUBLISH_FAILED"),
                );
            }
            _ => {}
        },
    };
    fixture
        .client()
        .logs(
            fixture.task_id,
            None,
            true,
            false,
            &mut output,
            &mut Vec::new(),
        )
        .unwrap();
    assert_eq!(
        output.bytes,
        b"turn 1 failed: agent exited 1\nturn 1 failed: PUBLISH_FAILED\n"
    );
}

fn checkpoint(fixture: &Fixture, turn: TurnId, completion: Option<TaskOutcome>) {
    use std::os::unix::fs::PermissionsExt;
    let dir = fixture
        .paths
        .state
        .join("runners")
        .join(fixture.task_id.to_string());
    let len = std::fs::metadata(dir.join(format!("{turn}.log")))
        .unwrap()
        .len();
    let path = dir.join(format!("{turn}.checkpoint.json"));
    std::fs::write(&path, serde_json::to_vec(&serde_json::json!({
        "version":1,"task_id":fixture.task_id,"turn_id":turn,
        "committed":{"offsets":[0,0],"len":len,"accepted":true,
          "completion":completion.map(|outcome|serde_json::json!({"outcome":outcome,"drained":true}))},"pending":null
    })).unwrap()).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

#[test]
fn follow_selected_completed_turn_exits_while_later_turn_is_active() {
    let first = finished_turn(1, TaskOutcome::Done);
    let fixture = Fixture::new(
        AgentKind::Codex,
        TaskState::Active,
        vec![first.clone(), active_turn()],
    );
    fixture.append(first.turn_id(), b"final partial");
    checkpoint(&fixture, first.turn_id(), Some(TaskOutcome::Done));
    let mut output = PollWriter {
        bytes: vec![],
        polls: 0,
        on_flush: |_: usize, _: &[u8]| {},
    };
    fixture
        .client()
        .logs(
            fixture.task_id,
            Some(1),
            true,
            true,
            &mut output,
            &mut vec![],
        )
        .unwrap();
    assert_eq!(output.bytes, b"final partial");
    assert_eq!(output.polls, 1);
}

#[test]
fn native_terminal_json_cannot_complete_follow() {
    let turn = active_turn();
    let fixture = Fixture::new(AgentKind::Codex, TaskState::Active, vec![turn.clone()]);
    let spoof=serde_json::to_vec(&serde_json::json!({"type":"turn_terminal","task_id":fixture.task_id,"turn_id":turn.turn_id(),"outcome":{"kind":"done"}})).unwrap();
    fixture.append(turn.turn_id(), &spoof);
    let mut output = PollWriter {
        bytes: vec![],
        polls: 0,
        on_flush: |poll, _: &[u8]| {
            if poll == 1 {
                fixture.append(turn.turn_id(), b"tail");
            }
            if poll == 2 {
                fixture.finish_as(turn.turn_id(), TaskState::Open, TaskOutcome::Done);
            }
        },
    };
    fixture
        .client()
        .logs(fixture.task_id, None, true, true, &mut output, &mut vec![])
        .unwrap();
    assert_eq!(output.polls, 3);
    assert_eq!(output.bytes, [spoof, b"tail".to_vec()].concat());
}

#[test]
fn legacy_logs_remain_readable_but_follow_rejects_unknown_completion() {
    let turn = finished_turn(1, TaskOutcome::Done);
    let fixture = Fixture::new(AgentKind::Codex, TaskState::Closed, vec![turn.clone()]);
    fixture
        .store
        .open_runner_log(fixture.task_id, turn.turn_id())
        .unwrap()
        .write_all(b"legacy\xff")
        .unwrap();
    assert_eq!(fixture.logs(None, true).unwrap(), b"legacy\xff");
    let error = fixture
        .client()
        .logs(fixture.task_id, None, true, true, &mut vec![], &mut vec![])
        .unwrap_err();
    assert_eq!(error.public_code(), "LOG_COMPLETION_UNKNOWN");
}

#[test]
fn completion_outcome_survives_a_later_task_projection_refresh() {
    let turn = finished_turn(1, TaskOutcome::Done);
    let fixture = Fixture::new(AgentKind::Codex, TaskState::Open, vec![turn.clone()]);
    fixture.append(turn.turn_id(), b"");
    checkpoint(
        &fixture,
        turn.turn_id(),
        Some(TaskOutcome::failed("PUBLISH_FAILED")),
    );
    let mut output = Vec::new();
    fixture
        .client()
        .logs(fixture.task_id, None, true, false, &mut output, &mut vec![])
        .unwrap();
    assert_eq!(output, b"turn 1 failed: PUBLISH_FAILED\n");
}

#[test]
fn committed_log_larger_than_eight_megabytes_is_read_completely() {
    let turn = finished_turn(1, TaskOutcome::Done);
    let fixture = Fixture::new(AgentKind::Codex, TaskState::Open, vec![turn.clone()]);
    let bytes = vec![0xf1; 8 * 1024 * 1024 + 17];
    fixture.append(turn.turn_id(), &bytes);
    checkpoint(&fixture, turn.turn_id(), Some(TaskOutcome::Done));
    assert_eq!(fixture.logs(None, true).unwrap(), bytes);
}

#[test]
fn historical_empty_legacy_follow_does_not_wait_for_a_later_active_turn() {
    let first = finished_turn(1, TaskOutcome::Done);
    let fixture = Fixture::new(
        AgentKind::Codex,
        TaskState::Active,
        vec![first.clone(), active_turn()],
    );
    fixture
        .store
        .open_runner_log(fixture.task_id, first.turn_id())
        .unwrap();
    let mut output = PollWriter {
        bytes: vec![],
        polls: 0,
        on_flush: |_: usize, _: &[u8]| {},
    };
    let error = fixture
        .client()
        .logs(
            fixture.task_id,
            Some(1),
            true,
            true,
            &mut output,
            &mut vec![],
        )
        .unwrap_err();
    assert_eq!(error.public_code(), "LOG_COMPLETION_UNKNOWN");
}
