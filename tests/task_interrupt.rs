#[allow(dead_code)]
mod support;

use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    io::Cursor,
    os::unix::process::ExitStatusExt,
    path::PathBuf,
    process::ExitStatus,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use clap::Parser;
use mac_worker::{
    RuntimeContext,
    agent::{AgentKind, PermissionPolicy},
    cli::Cli,
    client_state::ClientStateStore,
    error::WorkerError,
    job::{
        AdmissionObservation, CommandSummary, HostControlError, ProcessIdentity, QueueEntry,
        QueueEntryKind, QueueState,
    },
    paths::PathLayout,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    project_state::ProjectState,
    run_with_stdio_in_context,
    scheduler::{CandidateSlot, WorkerPreference},
    supervisor::SystemProcessInspector,
    task::{
        BaseOid, ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskId, TaskLimits,
        TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnId,
        TurnSummary, TurnTerminal,
    },
    task_store::{TaskCancelResponse, TaskStatusResponse},
    transfer::HostOperation,
};
use support::GitRepo;
use tempfile::TempDir;

const TASK_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";
const TURN_ID: &str = "018f0f4a6b5c7d8e9f00112233445577";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Script {
    Running,
    Idle,
    CancelFails,
    CloseAfter,
    /// The turn finishes on its own before the cancel lands.
    FinishedFirst,
}

#[derive(Clone, Copy)]
enum Phase {
    Before,
    After,
}

struct Home {
    _temp: TempDir,
    _repo: GitRepo,
    runtime: RuntimeContext,
    paths: PathLayout,
}

struct Remote {
    script: Script,
    phase: Mutex<Phase>,
    operations: Mutex<Vec<String>>,
    cancels: AtomicUsize,
    state_root: PathBuf,
    owner: ProcessIdentity,
    turn_id: TurnId,
    active: TaskStatus,
    open_cancelled: TaskStatus,
    closed: TaskStatus,
    idle: TaskStatus,
}

struct Fixture {
    home: Home,
    remote: Remote,
    task_id: TaskId,
    turn_id: TurnId,
}

#[test]
fn interrupt_of_a_running_turn_cancels_once_and_continues_the_session() {
    let fixture = plant(Script::Running);
    let (exit, stdout, stderr) = run_say(&fixture, false);
    assert_eq!(exit, 0, "stderr={stderr}\nstdout={stdout}");
    assert!(
        stdout.starts_with("interrupted turn 1 (cancelled)\n"),
        "{stdout}"
    );
    assert_eq!(
        fixture.remote.cancel_count(),
        1,
        "{:?}",
        fixture.remote.ops()
    );
    assert!(
        !fixture
            .remote
            .ops()
            .iter()
            .any(|operation| operation.contains("task-prebind") || operation.contains("task-turn")),
        "follow-up must not start a runner: {:?}",
        fixture.remote.ops()
    );
    let store = ClientStateStore::open(&fixture.home.paths.state).unwrap();
    let record = store.load_task(fixture.task_id).unwrap();
    assert_eq!(record.status().turns().len(), 2);
    assert_eq!(
        record.status().turns()[0].outcome(),
        Some(&TaskOutcome::Cancelled)
    );
    assert_eq!(record.status().turns()[0].turn_id(), fixture.turn_id);
    assert!(record.status().session_present());
    let prompt = store
        .read_turn_prompt(fixture.task_id, record.status().turns()[1].turn_id())
        .unwrap();
    assert!(prompt.contains("bound agent session"), "{prompt}");
    assert!(
        prompt.contains(&format!("Task ID: {}", fixture.task_id)),
        "{prompt}"
    );
    assert!(prompt.contains("Turn: 2"), "{prompt}");
    assert!(prompt.contains("steer"), "{prompt}");
}

#[test]
fn interrupt_of_a_running_turn_json_names_the_interrupted_turn() {
    let fixture = plant(Script::Running);
    let (exit, stdout, stderr) = run_say(&fixture, true);
    assert_eq!(exit, 0, "stderr={stderr}\nstdout={stdout}");
    let value: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(
        value["interrupted"],
        serde_json::json!({
            "turn_id": fixture.turn_id.to_string(),
            "outcome": "cancelled",
        })
    );
    assert_eq!(
        fixture.remote.cancel_count(),
        1,
        "{:?}",
        fixture.remote.ops()
    );
}

#[test]
fn interrupt_without_an_active_turn_is_a_plain_say() {
    let fixture = plant(Script::Idle);
    let (exit, stdout, stderr) = run_say(&fixture, false);
    assert_eq!(exit, 0, "stderr={stderr}\nstdout={stdout}");
    assert!(!stdout.contains("interrupted"), "{stdout}");
    assert_eq!(
        fixture.remote.cancel_count(),
        0,
        "{:?}",
        fixture.remote.ops()
    );
    let store = ClientStateStore::open(&fixture.home.paths.state).unwrap();
    let record = store.load_task(fixture.task_id).unwrap();
    assert_eq!(record.status().turns().len(), 2);
    assert_eq!(
        record.status().turns()[0].outcome(),
        Some(&TaskOutcome::Done)
    );
}

#[test]
fn interrupt_without_an_active_turn_json_omits_interrupted() {
    let fixture = plant(Script::Idle);
    let (exit, stdout, stderr) = run_say(&fixture, true);
    assert_eq!(exit, 0, "stderr={stderr}\nstdout={stdout}");
    let value: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert!(value.get("interrupted").is_none(), "{value}");
    assert_eq!(
        fixture.remote.cancel_count(),
        0,
        "{:?}",
        fixture.remote.ops()
    );
}

#[test]
fn a_failed_cancel_or_a_closing_task_sends_no_follow_up() {
    // A cancel that fails at the host and a task that closes between the
    // cancel and the say both stop before the follow-up: one cancel request,
    // no "interrupted" line, no follow-up turn.
    for (script, code, state) in [
        (Script::CancelFails, "HOST_IO", TaskState::Active),
        (Script::CloseAfter, "TASK_CLOSED", TaskState::Closed),
    ] {
        let fixture = plant(script);
        let (exit, stdout, stderr) = run_say(&fixture, false);
        assert_ne!(exit, 0, "{code}: stdout={stdout}");
        assert!(stderr.contains(code), "{stderr}");
        assert!(!stdout.contains("interrupted"), "{stdout}");
        assert_eq!(
            fixture.remote.cancel_count(),
            1,
            "{code}: {:?}",
            fixture.remote.ops()
        );
        let store = ClientStateStore::open(&fixture.home.paths.state).unwrap();
        let record = store.load_task(fixture.task_id).unwrap();
        assert_eq!(record.status().state(), state, "{code}");
        assert_eq!(record.status().turns().len(), 1, "{code}");
        if state == TaskState::Active {
            assert!(
                store
                    .read_turn_prompt(fixture.task_id, fixture.turn_id)
                    .is_err(),
                "{code}"
            );
        }
    }
}

#[test]
fn a_turn_that_finished_before_the_cancel_gets_no_follow_up() {
    let fixture = plant(Script::FinishedFirst);
    let (exit, stdout, stderr) = run_say(&fixture, false);
    assert_ne!(exit, 0, "stdout={stdout}");
    assert!(stderr.contains("TASK_REVISION_CONFLICT"), "{stderr}");
    assert!(
        stderr.contains("finished before it could be interrupted"),
        "{stderr}"
    );
    assert!(!stdout.contains("interrupted turn"), "{stdout}");
    let store = ClientStateStore::open(&fixture.home.paths.state).unwrap();
    let record = store.load_task(fixture.task_id).unwrap();
    assert_eq!(record.status().turns().len(), 1, "no follow-up turn");
    assert_eq!(
        record.status().turns()[0].outcome(),
        Some(&TaskOutcome::Done)
    );
}

fn run_say(fixture: &Fixture, json: bool) -> (u8, String, String) {
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
        &fixture.remote,
        &fixture.home.runtime,
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

fn plant(script: Script) -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let home_dir = root.join("home");
    let state_home = root.join("state");
    let cache_home = root.join("cache");
    let config_home = root.join("config");
    let data_home = root.join("data");
    for directory in [
        &home_dir,
        &state_home,
        &cache_home,
        &config_home,
        &data_home,
    ] {
        std::fs::create_dir_all(directory).unwrap();
    }
    let environment = BTreeMap::from([
        (OsString::from("HOME"), home_dir.as_os_str().to_os_string()),
        (OsString::from("XDG_STATE_HOME"), state_home.into()),
        (OsString::from("XDG_CACHE_HOME"), cache_home.into()),
        (OsString::from("XDG_CONFIG_HOME"), config_home.into()),
        (OsString::from("XDG_DATA_HOME"), data_home.into()),
    ]);
    let paths = PathLayout::discover(None, &environment, &home_dir).unwrap();
    std::fs::create_dir_all(paths.config.parent().unwrap()).unwrap();
    std::fs::write(
        &paths.config,
        "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
    )
    .unwrap();

    let repo = GitRepo::init();
    repo.write("README", b"fixture\n");
    repo.commit_all("fixture");
    let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let task_id: TaskId = TASK_ID.parse().unwrap();
    let turn_id: TurnId = TURN_ID.parse().unwrap();
    let base: BaseOid = "a".repeat(40).parse().unwrap();
    let active = status(TaskState::Active, &base, turn_id, None, None, true, 2);
    let open_cancelled = status(
        TaskState::Open,
        &base,
        turn_id,
        Some(TurnTerminal::Cancelled),
        Some(TaskOutcome::Cancelled),
        true,
        3,
    );
    let closed = status(
        TaskState::Closed,
        &base,
        turn_id,
        Some(TurnTerminal::Cancelled),
        Some(TaskOutcome::Cancelled),
        true,
        4,
    );
    let idle = status(
        TaskState::Open,
        &base,
        turn_id,
        Some(TurnTerminal::Succeeded),
        Some(TaskOutcome::Done),
        true,
        5,
    );
    let planted = if script == Script::Idle {
        idle.clone()
    } else {
        active.clone()
    };
    let record = task_record(
        task_id,
        &project.context.project_id,
        &project.context.worktree_id,
        planted,
    );
    let store = ClientStateStore::open(&paths.state).unwrap();
    store.create_task(record.clone()).unwrap();
    store
        .write_task_project_path(&record, &project.context.root)
        .unwrap();
    let owner = if script == Script::Idle {
        ProcessIdentity::new(support::fixture_pid(1), 1).unwrap()
    } else {
        dispatching_turn(&store, turn_id, &project)
    };
    mac_worker::controller::drain::set_drained(&paths.controller_state_root(), true).unwrap();
    let runtime = RuntimeContext::isolated(environment, home_dir, project.context.root.clone());
    Fixture {
        home: Home {
            _temp: temp,
            _repo: repo,
            runtime,
            paths: paths.clone(),
        },
        remote: Remote {
            script,
            phase: Mutex::new(Phase::Before),
            operations: Mutex::new(Vec::new()),
            cancels: AtomicUsize::new(0),
            state_root: paths.state,
            owner,
            turn_id,
            active,
            open_cancelled,
            closed,
            idle,
        },
        task_id,
        turn_id,
    }
}

fn dispatching_turn(
    store: &ClientStateStore,
    turn_id: TurnId,
    project: &ProjectState,
) -> ProcessIdentity {
    let owner = SystemProcessInspector
        .identity_for_pid(std::process::id())
        .unwrap();
    let now = 1_700_000_000_000;
    cache_idle(store, now);
    store
        .enqueue(
            QueueEntry::new(
                turn_id,
                store.client_id(),
                project.context.project_id.clone(),
                project.context.worktree_id.clone(),
                CommandSummary::argv(2).unwrap(),
                Vec::new(),
                WorkerPreference::Automatic,
                QueueEntryKind::TaskTurn,
                None,
                owner,
                now,
            )
            .unwrap(),
        )
        .unwrap();
    let claim = store
        .claim_next(owner, &["mini-1".to_owned()], now)
        .unwrap()
        .expect("running turn must be claimable");
    match claim.entry().state() {
        QueueState::Dispatching { dispatch_owner, .. } => *dispatch_owner,
        other => panic!("running turn must be dispatching, got {other:?}"),
    }
}

fn cache_idle(store: &ClientStateStore, now: u64) {
    let observation = AdmissionObservation::new(
        "mini-1".to_owned(),
        true,
        CandidateSlot::Idle,
        Vec::new(),
        Some(8 * 1024 * 1024 * 1024),
        64 * 1024 * 1024 * 1024,
        now,
    )
    .unwrap();
    store
        .admission_observation("mini-1", now, || Ok(observation))
        .unwrap();
}

fn task_record(
    task_id: TaskId,
    project_id: &str,
    worktree_id: &str,
    status: TaskStatus,
) -> LocalTaskRecord {
    let base_oid: BaseOid = "a".repeat(40).parse().unwrap();
    let meta = TaskMeta::new(TaskMetaInput {
        task_id,
        run_id: None,
        project_id: project_id.to_owned(),
        worktree_id: worktree_id.to_owned(),
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
        base_oid,
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("mac-worker", "mac-worker@example.test").unwrap(),
        title: None,
        prompt: "fixture task".into(),
        created_at_millis: 1,
    })
    .unwrap();
    LocalTaskRecord::new(
        meta,
        status,
        None,
        None,
        None,
        project_id.to_owned(),
        None,
        true,
        None,
    )
    .unwrap()
}

fn status(
    state: TaskState,
    base: &BaseOid,
    turn_id: TurnId,
    terminal: Option<TurnTerminal>,
    outcome: Option<TaskOutcome>,
    session_present: bool,
    updated_at_millis: u64,
) -> TaskStatus {
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
        session_present,
        Some(base.clone()),
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![turn],
        updated_at_millis,
    )
    .unwrap()
}

impl Remote {
    fn cancel_count(&self) -> usize {
        self.cancels.load(Ordering::SeqCst)
    }

    fn ops(&self) -> Vec<String> {
        self.operations.lock().unwrap().clone()
    }

    fn status_now(&self) -> TaskStatus {
        if self.script == Script::Idle {
            return self.idle.clone();
        }
        if matches!(*self.phase.lock().unwrap(), Phase::After) {
            if self.script == Script::CloseAfter {
                return self.closed.clone();
            }
            if self.script == Script::FinishedFirst {
                return self.idle.clone();
            }
            return self.open_cancelled.clone();
        }
        self.active.clone()
    }

    fn retire_running_turn(&self) {
        let store = ClientStateStore::open(&self.state_root).unwrap();
        store
            .remove_task_turn_after_terminal(self.turn_id, self.owner)
            .unwrap()
            .expect("dispatching turn row");
    }
}

impl ProcessRunner for Remote {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/git") {
            return SystemProcessRunner.run(request);
        }
        if request.program != OsStr::new("/usr/bin/ssh") {
            panic!(
                "unexpected fixture process {}",
                request.program.to_string_lossy()
            );
        }
        let operation = request
            .args
            .last()
            .and_then(|argument| argument.to_str())
            .unwrap_or_default()
            .to_owned();
        self.operations.lock().unwrap().push(operation.clone());
        if operation == HostOperation::TaskStatus.command() {
            return canonical(&TaskStatusResponse::new(self.status_now()));
        }
        if operation == HostOperation::TaskCancel.command() {
            self.cancels.fetch_add(1, Ordering::SeqCst);
            if self.script == Script::CancelFails {
                return host_failure();
            }
            if self.script == Script::Idle {
                panic!("idle say --interrupt must not cancel");
            }
            self.retire_running_turn();
            *self.phase.lock().unwrap() = Phase::After;
            if self.script == Script::FinishedFirst {
                return canonical(&TaskCancelResponse::new(self.idle.clone()));
            }
            return canonical(&TaskCancelResponse::new(self.open_cancelled.clone()));
        }
        panic!("unexpected fixture worker operation: {operation}");
    }
}

fn canonical<T: serde::Serialize>(value: &T) -> Result<ProcessResult, WorkerError> {
    let mut stdout = serde_json::to_vec(value).expect("fixture response");
    stdout.push(b'\n');
    Ok(ProcessResult {
        status: ExitStatus::from_raw(0),
        stdout,
        stderr: Vec::new(),
    })
}

fn host_failure() -> Result<ProcessResult, WorkerError> {
    let error = HostControlError::new("HOST_IO", "host failed").unwrap();
    let mut stdout = serde_json::to_vec(&error).unwrap();
    stdout.push(b'\n');
    Ok(ProcessResult {
        status: ExitStatus::from_raw(1 << 8),
        stdout,
        stderr: Vec::new(),
    })
}
