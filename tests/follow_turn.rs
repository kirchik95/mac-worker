//! `worker host follow-turn` runs inside the worker's herdr pane. It must
//! show a turn exactly as `worker task logs -f` shows it on the laptop, own
//! the pane's title, end with one outcome line, wait for herdr to close the
//! tab, and never write anything under the job directory. The fixtures here
//! build a host root by hand, so a test controls every byte the pane reads.

use std::{
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime},
};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy, Question, TurnLimits},
    error::WorkerError,
    follow_turn::{follow_turn, follow_turn_with_poll_interval},
    host_store::HostStore,
    job::{ClientId, CommandSpec, JobId, JobStatus, LeaseToken, RequestFingerprintMaterial},
    task::{
        BaseOid, ClosePolicy, GitIdentity, PublishMode, TaskId, TaskLimits, TaskMeta,
        TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnSummary, TurnTerminal,
    },
    turn::{TurnMaterial, TurnSection},
};
use tempfile::{TempDir, tempdir};
use uuid::Uuid;

const PROJECT_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const WORKTREE_ID: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const WORKER: &str = "mini-1";
const TITLE: &str = "Repair the flaky login spec";
/// Mirrors `job_service::EXECUTION_PAYLOAD_VERSION`; the fixture writes the
/// payload byte for byte as the host does, so a bump shows up here.
const EXECUTION_PAYLOAD_VERSION: u32 = 2;
const FIXTURE_ROOT: &str = "tests/fixtures";
const REFERENCE_ROOT: &str = "tests/fixtures/turn_log";
const FAST_POLL: Duration = Duration::from_millis(20);
const WAIT_LIMIT: Duration = Duration::from_secs(10);

fn task_id() -> TaskId {
    TaskId::new(Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef))
}

fn job_id() -> JobId {
    JobId::new(Uuid::from_u128(3))
}

fn base_oid() -> BaseOid {
    "a".repeat(40).parse().unwrap()
}

fn git_identity() -> GitIdentity {
    GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap()
}

fn expected_title() -> String {
    let id = task_id().to_string();
    format!("\x1b]0;task {} · {TITLE}\x07", &id[..12])
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// Creates `path` and every directory below `host_root` on the way to it as
/// owner-only directories, the only kind the host store's readers accept.
fn create_private_dir(host_root: &Path, path: &Path) {
    fs::create_dir_all(path).unwrap();
    let mut current = path;
    while current != host_root {
        fs::set_permissions(current, fs::Permissions::from_mode(0o700)).unwrap();
        current = current.parent().unwrap();
    }
}

fn write_private(path: &Path, bytes: &[u8]) {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    file.write_all(bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

/// Replaces a record the way the host does: a complete new file renamed over
/// the old one, never a truncate a reader could observe half-written.
fn replace_private(path: &Path, bytes: &[u8]) {
    let temporary = path.with_extension("replacement");
    write_private(&temporary, bytes);
    fs::rename(&temporary, path).unwrap();
}

fn append(path: &Path, bytes: &[u8]) {
    let mut file = OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(bytes).unwrap();
}

fn turn_material(agent: AgentKind) -> TurnMaterial {
    TurnMaterial::from_prompt(
        task_id(),
        1,
        agent,
        None,
        None,
        PermissionPolicy::Workspace,
        TurnLimits::new(30_000, None, None).unwrap(),
        base_oid(),
        "turn prompt",
        None,
        Uuid::from_u128(2),
        false,
    )
    .unwrap()
}

/// The canonical `execution.json` the host writes at submit: a turn section
/// for a task turn, `null` for a plain batch job.
fn execution_payload(agent: AgentKind, turn: bool) -> Vec<u8> {
    let material = turn_material(agent);
    let manifest_digest = if turn {
        material.digest()
    } else {
        "d".repeat(64)
    };
    let command = CommandSpec::shell("exec 'codex' '-'".into()).unwrap();
    let fingerprint = RequestFingerprintMaterial::new(
        job_id(),
        ClientId::new(Uuid::from_u128(4)),
        LeaseToken::new(Uuid::from_u128(5)),
        100,
        WORKER.into(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        manifest_digest,
        String::new(),
        30_000,
        "heavy".into(),
        command.clone(),
    )
    .unwrap()
    .fingerprint();
    let section = turn
        .then(|| TurnSection::new(material, PROJECT_ID, git_identity()).unwrap())
        .map(|section| serde_json::to_string(&section).unwrap())
        .unwrap_or_else(|| "null".into());
    format!(
        r#"{{"version":{EXECUTION_PAYLOAD_VERSION},"job_id":{},"client_id":{},"request_fingerprint":{},"lease_token":{},"command":{},"turn":{section}}}"#,
        serde_json::to_string(&job_id()).unwrap(),
        serde_json::to_string(&ClientId::new(Uuid::from_u128(4))).unwrap(),
        serde_json::to_string(&fingerprint).unwrap(),
        serde_json::to_string(&LeaseToken::new(Uuid::from_u128(5))).unwrap(),
        serde_json::to_string(&command).unwrap(),
    )
    .into_bytes()
}

fn task_meta(agent: AgentKind) -> TaskMeta {
    TaskMeta::new(TaskMetaInput {
        task_id: task_id(),
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
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
        base_oid: base_oid(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: git_identity(),
        title: Some(TITLE.into()),
        prompt: "turn prompt".into(),
        created_at_millis: 100,
    })
    .unwrap()
}

fn pending_status() -> TaskStatus {
    TaskStatus::new(
        TaskState::Active,
        None,
        Some(WORKER.into()),
        false,
        None,
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![TurnSummary::new(
            1,
            job_id(),
            None,
            None,
            None,
            false,
            Some(100),
            None,
        )],
        100,
    )
    .unwrap()
}

fn terminal_status(
    terminal: TurnTerminal,
    outcome: TaskOutcome,
    summary: Option<&str>,
    questions: Vec<Question>,
) -> TaskStatus {
    TaskStatus::new(
        TaskState::Open,
        Some(outcome.clone()),
        Some(WORKER.into()),
        true,
        None,
        summary.map(str::to_owned),
        questions,
        Vec::new(),
        None,
        vec![TurnSummary::new(
            1,
            job_id(),
            Some(terminal),
            Some(outcome),
            Some(false),
            false,
            Some(100),
            Some(200),
        )],
        200,
    )
    .unwrap()
}

struct Fixture {
    _temp: TempDir,
    host_root: PathBuf,
    task_dir: PathBuf,
    job_dir: PathBuf,
}

impl Fixture {
    fn new(agent: AgentKind) -> Self {
        let temp = tempdir().unwrap();
        let host_root = temp.path().join("host");
        Self::build(temp, host_root, agent)
    }

    /// A host root at the exact place the binary discovers it from `XDG_DATA_HOME`.
    fn under_xdg_data(temp: TempDir, data_home: &Path, agent: AgentKind) -> Self {
        let host_root = data_home.join("mac-worker").join("host");
        Self::build(temp, host_root, agent)
    }

    fn build(temp: TempDir, host_root: PathBuf, agent: AgentKind) -> Self {
        let store = HostStore::open(&host_root).unwrap();

        let task_dir = store.task_dir(PROJECT_ID, task_id()).unwrap();
        create_private_dir(&host_root, &task_dir);
        write_private(
            &task_dir.join("meta.json"),
            &serde_json::to_vec(&task_meta(agent)).unwrap(),
        );
        write_private(
            &task_dir.join("status.json"),
            &serde_json::to_vec(&pending_status()).unwrap(),
        );

        let job_dir = store.job(PROJECT_ID, WORKTREE_ID, job_id()).unwrap();
        create_private_dir(&host_root, &job_dir);
        create_private_dir(&host_root, &job_dir.join("tmp"));
        for log in ["stdout.log", "stderr.log", "tail.log"] {
            write_private(&job_dir.join(log), b"");
        }
        write_private(&job_dir.join("prompt.md"), b"turn prompt\n");
        write_private(
            &job_dir.join("status.json"),
            &serde_json::to_vec(&JobStatus::accepted(100).unwrap()).unwrap(),
        );
        write_private(
            &job_dir.join("execution.json"),
            &execution_payload(agent, true),
        );
        Self {
            _temp: temp,
            host_root,
            task_dir,
            job_dir,
        }
    }

    fn stdout_log(&self) -> PathBuf {
        self.job_dir.join("stdout.log")
    }

    fn stderr_log(&self) -> PathBuf {
        self.job_dir.join("stderr.log")
    }

    fn finish(
        &self,
        terminal: TurnTerminal,
        outcome: TaskOutcome,
        summary: Option<&str>,
        questions: Vec<Question>,
    ) {
        replace_private(
            &self.task_dir.join("status.json"),
            &serde_json::to_vec(&terminal_status(terminal, outcome, summary, questions)).unwrap(),
        );
    }

    fn finish_done(&self, summary: &str) {
        self.finish(
            TurnTerminal::Succeeded,
            TaskOutcome::Done,
            Some(summary),
            Vec::new(),
        );
    }

    /// The host removes the execution payload once a turn is published.
    fn remove_execution_payload(&self) {
        fs::remove_file(self.job_dir.join("execution.json")).unwrap();
    }

    /// Runs the follower to completion against the current files, stopping
    /// right after the outcome line.
    fn follow_once(&self) -> Result<Vec<u8>, WorkerError> {
        let mut out = Vec::new();
        follow_turn(
            &self.host_root,
            PROJECT_ID,
            WORKTREE_ID,
            &job_id().to_string(),
            &mut out,
            &|| true,
        )?;
        Ok(out)
    }
}

struct SharedOut(Arc<Mutex<Vec<u8>>>);

impl Write for SharedOut {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A follower running on its own thread with a fast poll, so a test can feed
/// the logs and the status while it watches.
struct Follower {
    output: Arc<Mutex<Vec<u8>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<Result<(), WorkerError>>>,
}

impl Follower {
    fn spawn(fixture: &Fixture) -> Self {
        let host_root = fixture.host_root.clone();
        let output = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let handle = thread::spawn({
            let output = output.clone();
            let stop = stop.clone();
            move || {
                let mut out = SharedOut(output);
                follow_turn_with_poll_interval(
                    &host_root,
                    PROJECT_ID,
                    WORKTREE_ID,
                    &job_id().to_string(),
                    FAST_POLL,
                    &mut out,
                    &|| stop.load(Ordering::SeqCst),
                )
            }
        });
        Self {
            output,
            stop,
            handle: Some(handle),
        }
    }

    fn output(&self) -> Vec<u8> {
        self.output.lock().unwrap().clone()
    }

    fn is_running(&self) -> bool {
        !self.handle.as_ref().unwrap().is_finished()
    }

    /// Waits until the follower has written `needle`; a follower that ends
    /// first fails the test with its result.
    fn wait_for(&mut self, needle: &[u8]) -> Vec<u8> {
        let deadline = Instant::now() + WAIT_LIMIT;
        loop {
            let output = self.output();
            if contains(&output, needle) {
                return output;
            }
            if !self.is_running() {
                let result = self.handle.take().unwrap().join().unwrap();
                panic!(
                    "follower ended with {result:?} before writing {:?}; output so far:\n{}",
                    text(needle),
                    text(&output)
                );
            }
            assert!(
                Instant::now() < deadline,
                "follower never wrote {:?}; output so far:\n{}",
                text(needle),
                text(&output)
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    /// Lets several polls pass and asserts the output did not change.
    fn assert_quiet(&self, expected: &[u8]) {
        thread::sleep(FAST_POLL * 8);
        assert!(self.is_running(), "follower ended unexpectedly");
        assert_eq!(text(&self.output()), text(expected));
    }

    fn stop_and_join(mut self) -> (Result<(), WorkerError>, Vec<u8>) {
        self.stop.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + WAIT_LIMIT;
        while self.is_running() {
            assert!(
                Instant::now() < deadline,
                "follower did not stop after the stop request"
            );
            thread::sleep(Duration::from_millis(5));
        }
        let result = self.handle.take().unwrap().join().unwrap();
        (result, self.output())
    }
}

impl Drop for Follower {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

fn agent_from_stem(stem: &str) -> AgentKind {
    match stem.split('-').next().unwrap_or_default() {
        "codex" => AgentKind::Codex,
        "claude" => AgentKind::Claude,
        "cursor" => AgentKind::Cursor,
        "opencode" => AgentKind::Opencode,
        other => panic!("fixture {stem} names an unknown agent {other}"),
    }
}

/// Every recorded transcript with its agent and reference rendering, the
/// same set `tests/turn_log.rs` holds `task logs` to.
fn recorded_fixtures() -> Vec<(PathBuf, AgentKind, PathBuf)> {
    let dirs: [(&str, Option<AgentKind>); 3] = [
        ("agents", None),
        ("cursor", Some(AgentKind::Cursor)),
        ("opencode", Some(AgentKind::Opencode)),
    ];
    let mut fixtures = Vec::new();
    for (dir, fixed_agent) in dirs {
        let mut paths: Vec<PathBuf> = fs::read_dir(Path::new(FIXTURE_ROOT).join(dir))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "jsonl"))
            .collect();
        paths.sort();
        for path in paths {
            let stem = path.file_stem().unwrap().to_str().unwrap();
            let agent = fixed_agent.unwrap_or_else(|| agent_from_stem(stem));
            let reference = Path::new(REFERENCE_ROOT)
                .join(dir)
                .join(format!("{stem}.txt"));
            fixtures.push((path, agent, reference));
        }
    }
    fixtures
}

fn codex_success_transcript() -> Vec<u8> {
    fs::read(Path::new(FIXTURE_ROOT).join("agents/codex-success.jsonl")).unwrap()
}

fn codex_success_reference() -> Vec<u8> {
    fs::read(Path::new(REFERENCE_ROOT).join("agents/codex-success.txt")).unwrap()
}

#[test]
fn every_recorded_transcript_renders_like_task_logs_then_the_outcome_line() {
    let fixtures = recorded_fixtures();
    assert!(!fixtures.is_empty(), "no recorded transcripts were found");
    for (transcript, agent, reference) in fixtures {
        let fixture = Fixture::new(agent);
        write_private(&fixture.stdout_log(), &fs::read(&transcript).unwrap());
        fixture.finish_done("ok");

        let output = fixture.follow_once().unwrap();

        let mut expected = expected_title().into_bytes();
        expected.extend_from_slice(&fs::read(&reference).unwrap());
        expected.extend_from_slice(b"turn 1: done \xe2\x80\x94 ok\n");
        assert!(
            output == expected,
            "{} rendered differently in the pane\n--- pane ---\n{}\n--- expected ---\n{}",
            transcript.display(),
            text(&output),
            text(&expected)
        );
    }
}

#[test]
fn the_osc_title_is_written_first_and_names_the_task() {
    let fixture = Fixture::new(AgentKind::Codex);
    fixture.finish_done("ok");

    let output = fixture.follow_once().unwrap();

    assert!(
        output.starts_with(expected_title().as_bytes()),
        "pane output must begin with the OSC 0 title: {}",
        text(&output)
    );
    assert!(!contains(&output, b"/"), "the title must not carry a path");
}

#[test]
fn stderr_lines_pass_through_verbatim_between_rendered_events() {
    let fixture = Fixture::new(AgentKind::Codex);
    let mut follower = Follower::spawn(&fixture);
    follower.wait_for(expected_title().as_bytes());

    append(
        &fixture.stderr_log(),
        b"codex: warning: slow network\n{\"type\":\"not_an_event\"}\n",
    );
    let output = follower.wait_for(b"{\"type\":\"not_an_event\"}\n");
    assert_eq!(
        text(&output),
        format!(
            "{}codex: warning: slow network\n{{\"type\":\"not_an_event\"}}\n",
            expected_title()
        ),
        "stderr is never parsed as agent events"
    );

    append(
        &fixture.stdout_log(),
        b"{\"type\":\"thread.started\",\"thread_id\":\"0d3c\"}\n",
    );
    follower.wait_for(b"session 0d3c\n");
    fixture.finish_done("ok");
    follower.wait_for(b"turn 1: done \xe2\x80\x94 ok\n");

    let (result, _) = follower.stop_and_join();
    result.unwrap();
}

#[test]
fn a_partial_trailing_line_waits_for_its_newline() {
    let fixture = Fixture::new(AgentKind::Codex);
    let mut follower = Follower::spawn(&fixture);
    follower.wait_for(expected_title().as_bytes());

    append(
        &fixture.stdout_log(),
        b"{\"type\":\"thread.started\",\"thread_id\":\"0d3c\"",
    );
    follower.assert_quiet(expected_title().as_bytes());

    append(&fixture.stdout_log(), b"}\n");
    let output = follower.wait_for(b"session 0d3c\n");
    assert_eq!(text(&output), format!("{}session 0d3c\n", expected_title()));

    // A stop before the turn ends leaves quietly: no outcome line is invented.
    let (result, output) = follower.stop_and_join();
    result.unwrap();
    assert_eq!(text(&output), format!("{}session 0d3c\n", expected_title()));
}

#[test]
fn an_unterminated_last_line_is_shown_once_the_turn_has_ended() {
    let fixture = Fixture::new(AgentKind::Codex);
    write_private(&fixture.stdout_log(), b"codex: exited without a newline");
    fixture.finish_done("ok");

    let output = fixture.follow_once().unwrap();

    assert_eq!(
        text(&output),
        format!(
            "{}codex: exited without a newline\nturn 1: done \u{2014} ok\n",
            expected_title()
        )
    );
}

/// A terminal turn as the publisher records it and the pane line it earns.
type OutcomeCase<'a> = (
    TurnTerminal,
    TaskOutcome,
    Option<&'a str>,
    Vec<Question>,
    &'a str,
);

#[test]
fn outcome_lines_carry_the_summary_the_question_or_the_reason() {
    let cases: [OutcomeCase<'_>; 6] = [
        (
            TurnTerminal::Succeeded,
            TaskOutcome::Done,
            Some("finished the rename"),
            Vec::new(),
            "turn 1: done \u{2014} finished the rename",
        ),
        (
            TurnTerminal::Succeeded,
            TaskOutcome::Done,
            None,
            Vec::new(),
            "turn 1: done",
        ),
        (
            TurnTerminal::Succeeded,
            TaskOutcome::NeedsInput,
            Some("Need the target crate name"),
            vec![
                Question::open("Which crate should be renamed?"),
                Question::open("Keep the old name as an alias?"),
            ],
            "turn 1: needs_input \u{2014} Which crate should be renamed?",
        ),
        (
            TurnTerminal::Succeeded,
            TaskOutcome::Blocked,
            Some("tests need a database"),
            Vec::new(),
            "turn 1: blocked \u{2014} tests need a database",
        ),
        (
            TurnTerminal::Failed,
            TaskOutcome::Failed {
                reason: "agent exited 2".into(),
            },
            None,
            Vec::new(),
            "turn 1: failed \u{2014} agent exited 2",
        ),
        (
            TurnTerminal::Cancelled,
            TaskOutcome::Cancelled,
            None,
            Vec::new(),
            "turn 1: cancelled",
        ),
    ];
    for (terminal, outcome, summary, questions, expected) in cases {
        let fixture = Fixture::new(AgentKind::Codex);
        fixture.finish(terminal, outcome.clone(), summary, questions);

        let output = fixture.follow_once().unwrap();

        assert_eq!(
            text(&output),
            format!("{}{expected}\n", expected_title()),
            "outcome {outcome:?} with summary {summary:?}"
        );
    }
}

#[test]
fn a_summary_with_line_breaks_stays_on_one_outcome_line() {
    let fixture = Fixture::new(AgentKind::Codex);
    fixture.finish_done("first line\nsecond line");

    let output = fixture.follow_once().unwrap();

    let after_title = text(&output[expected_title().len()..]);
    assert!(
        after_title.starts_with("turn 1: done \u{2014} first line"),
        "{after_title:?}"
    );
    assert!(after_title.contains("second line"), "{after_title:?}");
    assert_eq!(
        after_title.matches('\n').count(),
        1,
        "the pane gets exactly one outcome line: {after_title:?}"
    );
    assert!(after_title.ends_with('\n'));
}

#[test]
fn a_published_turn_is_still_followed_after_its_payload_was_removed() {
    // The terminal reporter path can re-arm a pane after the supervisor has
    // published the turn and removed execution.json; the finished turn must
    // still render, found through the task that lists its job id.
    let fixture = Fixture::new(AgentKind::Codex);
    write_private(&fixture.stdout_log(), &codex_success_transcript());
    fixture.finish_done("ok");
    fixture.remove_execution_payload();

    let output = fixture.follow_once().unwrap();

    let mut expected = expected_title().into_bytes();
    expected.extend_from_slice(&codex_success_reference());
    expected.extend_from_slice(b"turn 1: done \xe2\x80\x94 ok\n");
    assert_eq!(text(&output), text(&expected));
}

#[test]
fn malformed_identifiers_are_a_usage_error() {
    let fixture = Fixture::new(AgentKind::Codex);
    let job = job_id().to_string();
    let uppercase = PROJECT_ID.to_uppercase();
    let cases = [
        ("abc", WORKTREE_ID, job.as_str()),
        (uppercase.as_str(), WORKTREE_ID, job.as_str()),
        (PROJECT_ID, "not-a-digest", job.as_str()),
        (
            PROJECT_ID,
            WORKTREE_ID,
            "00000000-0000-0000-0000-000000000003",
        ),
        (PROJECT_ID, WORKTREE_ID, ""),
        (PROJECT_ID, WORKTREE_ID, "../../escape"),
    ];
    for (project, worktree, job) in cases {
        let mut out = Vec::new();
        let error = follow_turn(
            &fixture.host_root,
            project,
            worktree,
            job,
            &mut out,
            &|| true,
        )
        .unwrap_err();
        assert_eq!(error.exit_code(), 64, "{project} {worktree} {job}: {error}");
        assert_eq!(error.public_code(), "INVALID_COMPONENT");
        assert!(
            out.is_empty(),
            "nothing is written before validation passes"
        );
        assert!(!error.to_string().contains('/'), "{error}");
    }
}

#[test]
fn a_missing_job_a_batch_job_and_an_uninstalled_host_are_refused() {
    let fixture = Fixture::new(AgentKind::Codex);
    let other_job = JobId::new(Uuid::from_u128(9)).to_string();
    let mut out = Vec::new();
    let missing = follow_turn(
        &fixture.host_root,
        PROJECT_ID,
        WORKTREE_ID,
        &other_job,
        &mut out,
        &|| true,
    )
    .unwrap_err();
    assert_eq!(missing.exit_code(), 70, "{missing}");
    assert_eq!(missing.public_code(), "JOB_NOT_FOUND");
    assert!(out.is_empty());

    write_private(
        &fixture.job_dir.join("execution.json"),
        &execution_payload(AgentKind::Codex, false),
    );
    let batch = fixture.follow_once().unwrap_err();
    assert_eq!(batch.exit_code(), 70, "{batch}");
    assert_eq!(batch.public_code(), "NOT_A_TURN");

    // A finished batch job has no payload either and no task lists it.
    fixture.remove_execution_payload();
    replace_private(
        &fixture.task_dir.join("status.json"),
        &serde_json::to_vec(
            &TaskStatus::new(
                TaskState::Active,
                None,
                Some(WORKER.into()),
                false,
                None,
                None,
                Vec::new(),
                Vec::new(),
                None,
                vec![TurnSummary::new(
                    1,
                    JobId::new(Uuid::from_u128(9)),
                    None,
                    None,
                    None,
                    false,
                    Some(100),
                    None,
                )],
                100,
            )
            .unwrap(),
        )
        .unwrap(),
    );
    let unlisted = fixture.follow_once().unwrap_err();
    assert_eq!(unlisted.exit_code(), 70, "{unlisted}");
    assert_eq!(unlisted.public_code(), "NOT_A_TURN");

    let empty = tempdir().unwrap();
    let mut out = Vec::new();
    let uninstalled = follow_turn(
        &empty.path().join("host"),
        PROJECT_ID,
        WORKTREE_ID,
        &job_id().to_string(),
        &mut out,
        &|| true,
    )
    .unwrap_err();
    assert_eq!(uninstalled.exit_code(), 70, "{uninstalled}");
    assert!(
        !empty.path().join("host").exists(),
        "a read-only follower never initializes a host root"
    );
}

#[test]
fn the_follower_waits_after_the_outcome_line_until_it_is_stopped() {
    let fixture = Fixture::new(AgentKind::Codex);
    let mut follower = Follower::spawn(&fixture);
    follower.wait_for(expected_title().as_bytes());

    fixture.finish_done("ok");
    let output = follower.wait_for(b"turn 1: done \xe2\x80\x94 ok\n");
    follower.assert_quiet(&output);

    let (result, final_output) = follower.stop_and_join();
    result.unwrap();
    assert_eq!(final_output, output, "nothing follows the outcome line");
}

#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    name: String,
    len: u64,
    modified: SystemTime,
    content: Option<Vec<u8>>,
}

fn snapshot(directory: &Path) -> Vec<Snapshot> {
    let mut entries: Vec<Snapshot> = fs::read_dir(directory)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            let content = metadata
                .is_file()
                .then(|| fs::read(entry.path()).ok())
                .flatten();
            Snapshot {
                name: entry.file_name().to_string_lossy().into_owned(),
                len: metadata.len(),
                modified: metadata.modified().unwrap(),
                content,
            }
        })
        .collect();
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    entries
}

#[test]
fn the_follower_opens_nothing_for_writing_and_never_reads_the_prompt() {
    let fixture = Fixture::new(AgentKind::Codex);
    write_private(&fixture.stdout_log(), &codex_success_transcript());
    append(&fixture.stderr_log(), b"codex: note\n");
    fixture.finish_done("ok");
    let prompt = fixture.job_dir.join("prompt.md");
    fs::set_permissions(&prompt, fs::Permissions::from_mode(0o000)).unwrap();
    let job_before = snapshot(&fixture.job_dir);
    let task_before = snapshot(&fixture.task_dir);
    let job_dir_modified = fs::metadata(&fixture.job_dir).unwrap().modified().unwrap();

    let result = fixture.follow_once();
    let job_after = snapshot(&fixture.job_dir);
    let task_after = snapshot(&fixture.task_dir);
    let job_dir_modified_after = fs::metadata(&fixture.job_dir).unwrap().modified().unwrap();
    fs::set_permissions(&prompt, fs::Permissions::from_mode(0o600)).unwrap();
    let output = result.expect("an unreadable prompt is never opened");

    assert!(contains(&output, b"turn 1: done \xe2\x80\x94 ok\n"));
    assert_eq!(job_after, job_before);
    assert_eq!(task_after, task_before);
    assert_eq!(
        job_dir_modified_after, job_dir_modified,
        "no entry was created or removed under the job directory"
    );
    assert!(
        !fixture.job_dir.join(".mac-worker-rooted-fs").exists(),
        "no private write namespace was created"
    );
}

#[test]
fn the_binary_follows_a_turn_and_exits_zero_on_sigterm() {
    let temp = tempdir().unwrap();
    let root = temp.path().to_path_buf();
    let data_home = root.join("data");
    let fixture = Fixture::under_xdg_data(temp, &data_home, AgentKind::Codex);
    write_private(&fixture.stdout_log(), &codex_success_transcript());
    fixture.finish_done("ok");

    let mut child = Command::new(assert_cmd::cargo::cargo_bin("worker"))
        .env("HOME", &root)
        .env("XDG_DATA_HOME", &data_home)
        .env("XDG_CONFIG_HOME", root.join("config"))
        .env("XDG_STATE_HOME", root.join("state"))
        .env("XDG_CACHE_HOME", root.join("cache"))
        .args([
            "host",
            "follow-turn",
            PROJECT_ID,
            WORKTREE_ID,
            &job_id().to_string(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let collected = Arc::new(Mutex::new(Vec::new()));
    let reader = thread::spawn({
        let collected = collected.clone();
        let mut stdout = child.stdout.take().unwrap();
        move || {
            let mut buffer = [0u8; 4096];
            loop {
                match stdout.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(read) => collected.lock().unwrap().extend_from_slice(&buffer[..read]),
                }
            }
        }
    });

    let outcome = b"turn 1: done \xe2\x80\x94 ok\n";
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        if contains(&collected.lock().unwrap(), outcome) {
            break;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "follow-turn exited with {status} before the outcome line; output:\n{}",
                text(&collected.lock().unwrap())
            );
        }
        assert!(
            Instant::now() < deadline,
            "follow-turn never printed the outcome line; output:\n{}",
            text(&collected.lock().unwrap())
        );
        thread::sleep(Duration::from_millis(10));
    }
    // The process stays for herdr to close the tab; nothing else is printed.
    thread::sleep(Duration::from_millis(300));
    assert!(
        child.try_wait().unwrap().is_none(),
        "the pane process left early"
    );

    let pid = i32::try_from(child.id()).unwrap();
    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
    let status = child.wait().unwrap();
    reader.join().unwrap();

    assert!(
        status.success(),
        "SIGTERM must end the pane process with exit 0: {status}"
    );
    let output = collected.lock().unwrap().clone();
    let mut expected = expected_title().into_bytes();
    expected.extend_from_slice(&codex_success_reference());
    expected.extend_from_slice(outcome);
    assert_eq!(text(&output), text(&expected));
}
