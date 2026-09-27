#[path = "support/agent_launch_fixture.rs"]
mod agent_launch_fixture;
#[path = "support/fake_herdr.rs"]
mod fake_herdr;

use std::{
    fs,
    os::unix::{fs::PermissionsExt, io::AsRawFd},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use fake_herdr::{FakeHerdr, Hold, Reply};
use mac_worker::{
    agent::{AgentKind, PermissionPolicy, TurnLimits},
    gc::HostGc,
    herdr_reporter::{WORKSPACE_LABEL, task_label_prefix},
    host_store::HostStore,
    job::JobId,
    process::SystemProcessRunner,
    task::{
        ClosePolicy, GitIdentity, HerdrTurnReport, HerdrTurnState, PublishMode, TaskId, TaskLimits,
        TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnSummary,
        TurnTerminal,
    },
    task_store::{TaskCloseRequest, TaskStore},
};
use serde_json::json;
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const BASE_OID: &str = "0123456789abcdef0123456789abcdef01234567";
const UPDATED_AT: u64 = 1_700_000_000_000;

fn task_id() -> TaskId {
    TaskId::new(Uuid::from_u128(1))
}

fn write_open_task(store: &HostStore, with_meta: bool) {
    let task = task_id();
    let task_dir = store.task_dir(PROJECT_ID, task).unwrap();
    private_dir(task_dir.parent().unwrap());
    private_dir(&task_dir);
    let turn = TurnSummary::new(
        1,
        JobId::new(Uuid::from_u128(2)),
        Some(TurnTerminal::Succeeded),
        Some(TaskOutcome::Done),
        Some(false),
        false,
        Some(UPDATED_AT),
        Some(UPDATED_AT),
    )
    .with_herdr(Some(HerdrTurnReport {
        state: HerdrTurnState::Attached,
        pane_id: Some("w9:p2".into()),
    }));
    let status = TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Done),
        None,
        false,
        None,
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![turn],
        UPDATED_AT,
    )
    .unwrap();
    write_private(
        &task_dir,
        "status.json",
        &serde_json::to_vec(&status).unwrap(),
    );
    if with_meta {
        let meta = TaskMeta::new(TaskMetaInput {
            task_id: task,
            run_id: None,
            project_id: PROJECT_ID.into(),
            worktree_id: WORKTREE_ID.into(),
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
            base_oid: BASE_OID.parse().unwrap(),
            limits: TaskLimits::new(TurnLimits::new(30 * 60 * 1000, None, None).unwrap(), 1)
                .unwrap(),
            close_policy: ClosePolicy::Never,
            env_profile: None,
            git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
            title: Some("hygiene".into()),
            prompt: "record a task without touching a real pool".into(),
            created_at_millis: UPDATED_AT,
        })
        .unwrap();
        write_private(&task_dir, "meta.json", &serde_json::to_vec(&meta).unwrap());
    }
}

fn private_dir(path: &Path) {
    fs::create_dir_all(path).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}

fn write_private(dir: &Path, name: &str, bytes: &[u8]) {
    let path = dir.join(name);
    fs::write(&path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

fn installation_lock(host: &Path) -> PathBuf {
    fs::read_dir(host.parent().unwrap())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with(".mac-worker-installation-") && name.ends_with(".lock")
                })
        })
        .expect("installation lock")
}

fn lock_is_free(path: &Path) -> bool {
    let Ok(file) = fs::OpenOptions::new().read(true).write(true).open(path) else {
        return false;
    };
    let free = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) == 0 };
    if free {
        unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
    }
    free
}

struct Release(std::sync::Arc<Hold>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn queue_held_workspace(server: &FakeHerdr, hold: &std::sync::Arc<Hold>) {
    let label = format!("{} · turn 1", task_label_prefix(task_id()));
    server.reply(
        "workspace.list",
        Reply::Hold(
            std::sync::Arc::clone(hold),
            json!({
                "type": "workspace_list",
                "workspaces": [{ "workspace_id": "w9", "label": WORKSPACE_LABEL }]
            }),
        ),
    );
    server.reply(
        "tab.list",
        Reply::Result(json!({
            "type": "tab_list",
            "tabs": [
                { "tab_id": "w9:t1", "label": "1", "workspace_id": "w9" },
                { "tab_id": "w9:t2", "label": label, "workspace_id": "w9" }
            ]
        })),
    );
    server.reply(
        "tab.list",
        Reply::Result(json!({
            "type": "tab_list",
            "tabs": [{ "tab_id": "w9:t1", "label": "1", "workspace_id": "w9" }]
        })),
    );
}

#[test]
fn hung_herdr_close_releases_host_locks_wrapper() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let home = home.to_str().unwrap();
    agent_launch_fixture::assert_subprocess_success(
        "hung_herdr_close_releases_host_locks",
        &[("HOME", home), ("MAC_WORKER_ACCOUNT_HOME", home)],
        false,
    );
}

#[test]
fn hung_herdr_close_releases_host_locks() {
    if agent_launch_fixture::skip_unless_subtest() {
        return;
    }
    let home = PathBuf::from(std::env::var("HOME").unwrap());
    let server = FakeHerdr::start_in_home(&home);
    let hold = Hold::new();
    queue_held_workspace(&server, &hold);

    let temp = tempfile::tempdir().unwrap();
    let host = temp.path().join("host");
    let store = HostStore::open(&host).unwrap();
    write_open_task(&store, false);
    let closer = store.clone();
    let join = thread::spawn(move || {
        TaskStore::new(&closer, &SystemProcessRunner).close(&TaskCloseRequest::new(
            PROJECT_ID,
            task_id(),
            false,
        ))
    });
    assert!(
        hold.wait_entered(Duration::from_secs(2)),
        "close did not reach herdr"
    );
    let _release = Release(std::sync::Arc::clone(&hold));
    assert!(
        lock_is_free(&installation_lock(&host)),
        "installation lock held while herdr close is in progress"
    );
    assert!(
        lock_is_free(&host.join("locks/session.lock")),
        "session lock held while herdr close is in progress"
    );
    assert_eq!(
        store.task_status(PROJECT_ID, task_id()).unwrap().state(),
        TaskState::Closed
    );
    drop(_release);
    join.join().unwrap().unwrap();
    assert!(
        server
            .requests_for("tab.close")
            .iter()
            .any(|request| request["params"]["tab_id"] == "w9:t2"),
        "{:?}",
        server.requests()
    );
    assert_eq!(
        store.task_status(PROJECT_ID, task_id()).unwrap().state(),
        TaskState::Closed
    );
}

#[test]
fn herdr_close_error_does_not_change_the_task_wrapper() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let home = home.to_str().unwrap();
    agent_launch_fixture::assert_subprocess_success(
        "herdr_close_error_does_not_change_the_task",
        &[("HOME", home), ("MAC_WORKER_ACCOUNT_HOME", home)],
        false,
    );
}

#[test]
fn herdr_close_error_does_not_change_the_task() {
    if agent_launch_fixture::skip_unless_subtest() {
        return;
    }
    let home = PathBuf::from(std::env::var("HOME").unwrap());
    let server = FakeHerdr::start_in_home(&home);
    server.reply(
        "workspace.list",
        Reply::Error {
            code: "busy".into(),
            message: "sidebar unavailable".into(),
        },
    );
    let temp = tempfile::tempdir().unwrap();
    let host = temp.path().join("host");
    let store = HostStore::open(&host).unwrap();
    write_open_task(&store, false);
    TaskStore::new(&store, &SystemProcessRunner)
        .close(&TaskCloseRequest::new(PROJECT_ID, task_id(), false))
        .unwrap();
    assert_eq!(
        store.task_status(PROJECT_ID, task_id()).unwrap().state(),
        TaskState::Closed
    );
    assert_eq!(server.requests_for("workspace.list").len(), 1);
}

#[test]
fn hung_herdr_sweep_releases_the_installation_lock_wrapper() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let home = home.to_str().unwrap();
    agent_launch_fixture::assert_subprocess_success(
        "hung_herdr_sweep_releases_the_installation_lock",
        &[("HOME", home), ("MAC_WORKER_ACCOUNT_HOME", home)],
        false,
    );
}

#[test]
fn hung_herdr_sweep_releases_the_installation_lock() {
    if agent_launch_fixture::skip_unless_subtest() {
        return;
    }
    let home = PathBuf::from(std::env::var("HOME").unwrap());
    let server = FakeHerdr::start_in_home(&home);
    let hold = Hold::new();
    queue_held_workspace(&server, &hold);

    let temp = tempfile::tempdir().unwrap();
    let host = temp.path().join("host");
    let store = HostStore::open(&host).unwrap();
    write_open_task(&store, true);
    let sweeper = store.clone();
    let join =
        thread::spawn(move || HostGc::new(&sweeper, &SystemProcessRunner).apply_at(UPDATED_AT));
    assert!(
        hold.wait_entered(Duration::from_secs(2)),
        "gc did not reach herdr"
    );
    let _release = Release(std::sync::Arc::clone(&hold));
    assert!(
        lock_is_free(&installation_lock(&host)),
        "installation lock held while herdr sweep is in progress"
    );
    assert_eq!(
        store.task_status(PROJECT_ID, task_id()).unwrap().state(),
        TaskState::Open
    );
    drop(_release);
    let report = join.join().unwrap().unwrap();
    assert!(report.applied().is_empty(), "{report:?}");
    assert_eq!(
        store.task_status(PROJECT_ID, task_id()).unwrap().state(),
        TaskState::Open
    );
}
