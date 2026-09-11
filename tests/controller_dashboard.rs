use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use clap::Parser;
use mac_worker::{
    cli::{Cli, Command as WorkerCommand},
    client_state::ClientStateStore,
    config::ControllerConfig,
    controller::{ControllerLeader, controller_dashboard_ssh_request},
    job::JobId,
    paths::PathLayout,
    supervisor::SystemProcessInspector,
    task::{
        BaseOid, ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskId, TaskLimits,
        TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnSummary,
        TurnTerminal,
    },
};
use uuid::Uuid;

const FAKE_SSH: &str = r#"#!/usr/bin/env python3
"""Labelled fake SSH for controller dashboard tests.

Ignores -L local-forward arguments because a one-box -L N plus remote --port N
would collide. Execs the remote command and passes stdin through so parent
SIGKILL still delivers EOF to the viewer.
"""
import json
import os
import sys

if os.environ.get("MAC_WORKER_FAKE_SSH_FAIL") == "1":
    sys.stderr.write("labelled fake ssh: forced failure\n")
    sys.exit(255)

worker = os.environ.get("MAC_WORKER_TEST_BIN")
if not worker:
    sys.stderr.write("MAC_WORKER_TEST_BIN is required\n")
    sys.exit(255)

argv_log = os.environ.get("MAC_WORKER_FAKE_SSH_ARGV_LOG")
if argv_log:
    with open(argv_log, "w", encoding="utf-8") as fh:
        json.dump(sys.argv, fh)

pid_file = os.environ.get("MAC_WORKER_FAKE_SSH_PID_FILE")
if pid_file:
    with open(pid_file, "w", encoding="utf-8") as fh:
        fh.write(str(os.getpid()))

if os.environ.get("MAC_WORKER_FAKE_SSH_HANG_STDOUT") == "1":
    import signal
    import time
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    while True:
        time.sleep(3600)

if os.environ.get("MAC_WORKER_FAKE_SSH_DRIP_HTTP") == "1":
    import signal
    import socket
    import time
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    port = None
    for i, arg in enumerate(sys.argv):
        if arg == "-L" and i + 1 < len(sys.argv):
            spec = sys.argv[i + 1].split(":")
            if len(spec) >= 2:
                port = int(spec[1])
            break
    if port is None:
        sys.stderr.write("labelled fake ssh: missing forward port\n")
        sys.exit(255)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(1)
    sys.stdout.write("http://127.0.0.1:%d/\n" % port)
    sys.stdout.flush()
    conn, _ = srv.accept()
    req = b""
    while b"\r\n\r\n" not in req:
        chunk = conn.recv(1)
        if not chunk:
            break
        req += chunk
    payload = (
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: application/json\r\n"
        "Connection: close\r\n"
        "\r\n"
        '{"api_version":1}'
    ).encode()
    for index in range(len(payload)):
        conn.send(payload[index:index + 1])
        time.sleep(0.5)
    while True:
        time.sleep(3600)

remote = sys.argv[-1] if len(sys.argv) > 1 else ""
prefix = "~/.local/bin/worker "
if not remote.startswith(prefix):
    sys.stderr.write("labelled fake ssh: unexpected remote command\n")
    sys.exit(255)

if home := os.environ.get("MAC_WORKER_FAKE_SSH_REMOTE_HOME"):
    os.environ["HOME"] = home
if config := os.environ.get("MAC_WORKER_FAKE_SSH_REMOTE_CONFIG"):
    os.environ["XDG_CONFIG_HOME"] = config
if state := os.environ.get("MAC_WORKER_FAKE_SSH_REMOTE_STATE"):
    os.environ["XDG_STATE_HOME"] = state
if cache := os.environ.get("MAC_WORKER_FAKE_SSH_REMOTE_CACHE"):
    os.environ["XDG_CACHE_HOME"] = cache
if data := os.environ.get("MAC_WORKER_FAKE_SSH_REMOTE_DATA"):
    os.environ["XDG_DATA_HOME"] = data

parts = remote.split()
if os.environ.get("MAC_WORKER_FAKE_SSH_IGNORE_TERM") == "1":
    import signal
    import subprocess
    import time
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    subprocess.Popen(
        [worker, *parts[1:]],
        stdin=sys.stdin,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )
    while True:
        time.sleep(3600)

os.execv(worker, [worker, *parts[1:]])
"#;

struct Homes {
    _root: tempfile::TempDir,
    laptop: PathBuf,
    controller: PathBuf,
    fake_ssh: PathBuf,
    argv_log: PathBuf,
    pid_file: PathBuf,
}

impl Homes {
    fn new(controller_enabled: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        let laptop = canonical_home(root.path().join("laptop"), controller_enabled);
        let controller = canonical_home(root.path().join("controller"), controller_enabled);
        let fake_ssh = root.path().join("fake-ssh");
        fs::write(&fake_ssh, FAKE_SSH).unwrap();
        let mut permissions = fs::metadata(&fake_ssh).unwrap().permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            permissions.set_mode(0o755);
            fs::set_permissions(&fake_ssh, permissions).unwrap();
        }
        Self {
            argv_log: root.path().join("fake-ssh-argv.json"),
            pid_file: root.path().join("fake-ssh.pid"),
            _root: root,
            laptop,
            controller,
            fake_ssh,
        }
    }
}

fn canonical_home(home: PathBuf, controller_enabled: bool) -> PathBuf {
    fs::create_dir_all(&home).unwrap();
    prepare_home(&home, controller_enabled);
    fs::canonicalize(home).unwrap()
}

fn prepare_home(home: &Path, controller_enabled: bool) {
    for directory in [
        home.join(".config/mac-worker"),
        home.join(".local/state"),
        home.join(".cache"),
        home.join(".local/share"),
    ] {
        fs::create_dir_all(directory).unwrap();
    }
    // Enabled controller homes omit workers (allowed) so the viewer never
    // probes a named Mac. Local-only homes keep one unroutable worker so
    // inspect cannot resolve or SSH to a live machine.
    let config = if controller_enabled {
        "version = 1\n[controller]\nenabled = true\nssh = \"controller.local\"\n"
    } else {
        "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac-worker-test.invalid\"\nslots = 1\n"
    };
    fs::write(home.join(".config/mac-worker/config.toml"), config).unwrap();
}

fn paths_for(home: &Path) -> PathLayout {
    PathLayout::discover(
        None,
        &BTreeMap::from([
            (
                OsString::from("XDG_CONFIG_HOME"),
                home.join(".config").into_os_string(),
            ),
            (
                OsString::from("XDG_STATE_HOME"),
                home.join(".local/state").into_os_string(),
            ),
            (
                OsString::from("XDG_CACHE_HOME"),
                home.join(".cache").into_os_string(),
            ),
            (
                OsString::from("XDG_DATA_HOME"),
                home.join(".local/share").into_os_string(),
            ),
        ]),
        home,
    )
    .unwrap()
}

fn apply_home(command: &mut Command, home: &Path) {
    command
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home.join(".config"))
        .env("XDG_STATE_HOME", home.join(".local/state"))
        .env("XDG_CACHE_HOME", home.join(".cache"))
        .env("XDG_DATA_HOME", home.join(".local/share"));
}

fn apply_fake_ssh(command: &mut Command, homes: &Homes) {
    command
        .env("MAC_WORKER_FAKE_SSH", &homes.fake_ssh)
        .env("MAC_WORKER_TEST_BIN", env!("CARGO_BIN_EXE_worker"))
        .env("MAC_WORKER_FAKE_SSH_ARGV_LOG", &homes.argv_log)
        .env("MAC_WORKER_FAKE_SSH_PID_FILE", &homes.pid_file)
        .env("MAC_WORKER_FAKE_SSH_REMOTE_HOME", &homes.controller)
        .env(
            "MAC_WORKER_FAKE_SSH_REMOTE_CONFIG",
            homes.controller.join(".config"),
        )
        .env(
            "MAC_WORKER_FAKE_SSH_REMOTE_STATE",
            homes.controller.join(".local/state"),
        )
        .env(
            "MAC_WORKER_FAKE_SSH_REMOTE_CACHE",
            homes.controller.join(".cache"),
        )
        .env(
            "MAC_WORKER_FAKE_SSH_REMOTE_DATA",
            homes.controller.join(".local/share"),
        );
}

fn spawn_dashboard(homes: &Homes, extra: &[&str], stdin: Stdio) -> FixtureChild {
    let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
    apply_home(&mut command, &homes.laptop);
    apply_fake_ssh(&mut command, homes);
    command
        .args(["dashboard", "--no-open", "--no-facts-refresh"])
        .args(extra)
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    FixtureChild::new(command.spawn().unwrap())
}

struct FixtureChild {
    child: Child,
}

impl FixtureChild {
    fn new(child: Child) -> Self {
        Self { child }
    }
}

impl Deref for FixtureChild {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        &self.child
    }
}

impl DerefMut for FixtureChild {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.child
    }
}

impl Drop for FixtureChild {
    fn drop(&mut self) {
        let pid = self.child.id();
        let descendants = child_pids(pid);
        let _ = self.child.kill();
        let _ = self.child.wait();
        for descendant in descendants {
            let _ = Command::new("/bin/kill")
                .args(["-KILL", &descendant.to_string()])
                .status();
        }
    }
}

fn wait_for_url(child: &mut Child) -> String {
    let stdout = child.stdout.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut line = String::new();
        let _ = BufReader::new(stdout).read_line(&mut line);
        let _ = sender.send(line);
    });
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        match receiver.recv_timeout(Duration::from_millis(50)) {
            Ok(line) if !line.trim().is_empty() => return line.trim().to_owned(),
            Ok(line) => {
                let status = child.try_wait();
                panic!(
                    "dashboard printed an empty URL line: {line:?} status={status:?} stderr={}",
                    drain_stderr_if_exited(child)
                );
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if let Ok(Some(status)) = child.try_wait() {
                    panic!(
                        "dashboard exited before URL: {status:?} stderr={}",
                        drain_stderr_if_exited(child)
                    );
                }
                if Instant::now() >= deadline {
                    panic!("dashboard URL was not printed before timeout");
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("dashboard stdout closed before a URL line");
            }
        }
    }
}

fn host_from_url(url: &str) -> String {
    url.trim()
        .strip_prefix("http://")
        .unwrap()
        .trim_end_matches('/')
        .to_owned()
}

fn http_get(address: &str, path: &str, host: &str) -> (u16, Vec<u8>) {
    let mut stream = std::net::TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    parse_http(raw)
}

fn http_post(address: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> u16 {
    let mut stream = std::net::TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut request = format!("POST {path} HTTP/1.1\r\nConnection: close\r\n");
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    let mut wire = request.into_bytes();
    wire.extend_from_slice(body);
    stream.write_all(&wire).unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    parse_http(raw).0
}

fn parse_http(raw: Vec<u8>) -> (u16, Vec<u8>) {
    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap();
    let (head, body) = raw.split_at(split + 4);
    let status = std::str::from_utf8(head)
        .unwrap()
        .split("\r\n")
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    (status, body.to_vec())
}

fn laptop_task_files(home: &Path) -> bool {
    let state = paths_for(home).state;
    if !state.exists() {
        return false;
    }
    walkdir_has_tasks(&state)
}

fn walkdir_has_tasks(root: &Path) -> bool {
    fn walk(path: &Path) -> bool {
        if path.file_name() == Some(OsStr::new("tasks")) {
            return true;
        }
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                if walk(&entry.path()) {
                    return true;
                }
            }
        }
        false
    }
    walk(root)
}

fn drain_stderr_if_exited(child: &mut Child) -> String {
    if !matches!(child.try_wait(), Ok(Some(_))) {
        return String::new();
    }
    let Some(mut err) = child.stderr.take() else {
        return String::new();
    };
    let mut stderr = String::new();
    let _ = err.read_to_string(&mut stderr);
    stderr
}

fn kill_and_reap(child: &mut Child) {
    let pid = child.id();
    let _ = child.kill();
    wait_until_dead(pid);
}

fn child_pids(pid: u32) -> Vec<u32> {
    let output = Command::new("pgrep")
        .args(["-P", &pid.to_string()])
        .output()
        .unwrap();
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.parse().ok())
        .collect()
}

fn process_live(pid: u32) -> bool {
    SystemProcessInspector.identity_for_pid(pid).is_ok()
}

fn wait_for_pid_file(path: &Path) -> u32 {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Ok(contents) = fs::read_to_string(path)
            && let Ok(pid) = contents.trim().parse()
        {
            return pid;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("pid file {} was not written", path.display());
}

fn process_group_live(pgid: u32) -> bool {
    unsafe { libc::killpg(pgid as i32, 0) == 0 }
}

fn wait_until_dead(pid: u32) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if !process_live(pid) {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let sample = Command::new("ps")
        .args([
            "-p",
            &pid.to_string(),
            "-o",
            "pid,ppid,pgid,stat,etime,command",
        ])
        .output()
        .unwrap();
    panic!(
        "pid {pid} was still live: {}",
        String::from_utf8_lossy(&sample.stdout)
    );
}

fn wait_child_exit(child: &mut Child, timeout: Duration) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return status;
        }
        if Instant::now() >= deadline {
            panic!(
                "child {} still live after {:?}: stderr={}",
                child.id(),
                timeout,
                drain_stderr_if_exited(child)
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn send_signal(pid: u32, signal: &str) {
    let status = Command::new("/bin/kill")
        .args([format!("-{signal}"), pid.to_string()])
        .status()
        .unwrap();
    assert!(status.success(), "kill -{signal} {pid} failed: {status:?}");
}

fn spawn_leader(homes: &Homes) -> FixtureChild {
    let mut leader = Command::new(env!("CARGO_BIN_EXE_worker"));
    apply_home(&mut leader, &homes.controller);
    FixtureChild::new(
        leader
            .args(["controller", "run"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    )
}

fn wait_for_leader_lock(homes: &Homes) -> PathBuf {
    let leader_root = paths_for(&homes.controller).controller_state_root();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if leader_root.join("leader.json").exists() {
            assert!(
                ControllerLeader::acquire(&leader_root)
                    .err()
                    .is_some_and(|error| error.to_string().contains("CONTROLLER_LOCK_HELD"))
            );
            return leader_root;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("controller leader lock was not acquired");
}

fn seed_task(home: &Path, state: TaskState) -> LocalTaskRecord {
    let paths = paths_for(home);
    let store = ClientStateStore::open(&paths.state).unwrap();
    let record = task_record(0x51, state);
    store.create_task(record.clone()).unwrap();
    record
}

fn task_record(id: u128, state: TaskState) -> LocalTaskRecord {
    let task_id = TaskId::new(Uuid::from_u128(id));
    let turn_id = JobId::new(Uuid::from_u128(id + 1));
    let base_oid: BaseOid = "a".repeat(40).parse().unwrap();
    let meta = TaskMeta::new(TaskMetaInput {
        task_id,
        run_id: None,
        project_id: "b".repeat(64),
        worktree_id: "c".repeat(64),
        agent: mac_worker::agent::AgentKind::Codex,
        model: None,
        effort: None,
        policy: mac_worker::agent::PermissionPolicy::Workspace,
        source: TaskSource::Local {
            wip: false,
            push_target: None,
        },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid: base_oid.clone(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("mac-worker", "mac-worker@example.test").unwrap(),
        title: None,
        prompt: "fixture task".into(),
        created_at_millis: 1,
    })
    .unwrap();
    let turn = TurnSummary::new(
        1,
        turn_id,
        Some(TurnTerminal::Succeeded),
        Some(TaskOutcome::Done),
        Some(true),
        false,
        Some(1),
        Some(2),
    );
    let status = TaskStatus::new(
        state,
        Some(TaskOutcome::Done),
        Some("mini-1".into()),
        true,
        Some(base_oid),
        Some("ready for review".into()),
        Vec::new(),
        Vec::new(),
        None,
        vec![turn],
        2,
    )
    .unwrap();
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

fn mutation_body(record: &LocalTaskRecord, turn_count: u32, state: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "expected_task_id": record.meta().task_id(),
        "expected_turn_id": record.status().turns().last().map(|turn| turn.turn_id()),
        "expected_turn_count": turn_count,
        "expected_head_oid": record.status().head_oid(),
        "expected_updated_at_millis": record.status().updated_at_millis(),
        "expected_state": state
    }))
    .unwrap()
}

#[test]
fn dashboard_ssh_request_uses_same_port_forward_and_hidden_viewer_flag() {
    let controller = ControllerConfig {
        enabled: true,
        ssh: "controller.local".into(),
        remote_binary: "~/.local/bin/worker".into(),
    };
    let request = controller_dashboard_ssh_request(&controller, 9173, true).unwrap();
    assert_eq!(request.program, OsStr::new("/usr/bin/ssh"));
    let args: Vec<String> = request
        .args
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    assert!(args.contains(&"ExitOnForwardFailure=yes".to_owned()));
    assert!(args.contains(&"-L".to_owned()));
    assert!(args.contains(&"127.0.0.1:9173:127.0.0.1:9173".to_owned()));
    assert!(!args.iter().any(|arg| arg.contains("ClearAllForwardings")));
    assert!(!args.contains(&"-t".to_owned()));
    let remote = args.last().expect("remote command");
    assert_eq!(
        remote,
        "~/.local/bin/worker dashboard --port 9173 --no-open --no-facts-refresh --controller-viewer"
    );
}

#[test]
fn enabled_false_serves_a_local_dashboard_without_fake_ssh() {
    let homes = Homes::new(false);
    let mut child = spawn_dashboard(&homes, &[], Stdio::null());
    let url = wait_for_url(&mut child);
    let host = host_from_url(&url);
    let (status, _) = http_get(&host, "/api/v1/snapshot", &host);
    assert!(status == 200 || status == 503);
    assert!(!homes.argv_log.exists());
    kill_and_reap(&mut child);
}

#[test]
fn enabled_true_ssh_failure_is_unavailable_without_laptop_task_files() {
    let homes = Homes::new(true);
    let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
    apply_home(&mut command, &homes.laptop);
    apply_fake_ssh(&mut command, &homes);
    command.env("MAC_WORKER_FAKE_SSH_FAIL", "1");
    let output = command.args(["dashboard", "--no-open"]).output().unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("CONTROLLER_UNAVAILABLE"), "stderr={stderr}");
    assert!(!laptop_task_files(&homes.laptop));
}

#[test]
fn fake_ssh_argv_includes_viewer_flag_and_same_port_forward() {
    let homes = Homes::new(true);
    let mut child = spawn_dashboard(&homes, &[], Stdio::piped());
    let url = wait_for_url(&mut child);
    let host = host_from_url(&url);
    let args: Vec<String> = serde_json::from_slice(&fs::read(&homes.argv_log).unwrap()).unwrap();
    assert!(args.contains(&"ExitOnForwardFailure=yes".to_owned()));
    assert!(args.iter().any(|arg| arg.starts_with("127.0.0.1:")
        && arg.ends_with(&format!(":127.0.0.1:{}", host.split(':').nth(1).unwrap()))));
    assert!(!args.iter().any(|arg| arg.contains("ClearAllForwardings")));
    assert!(args.last().unwrap().contains("dashboard --port"));
    assert!(args.last().unwrap().contains("--controller-viewer"));
    kill_and_reap(&mut child);
}

#[test]
fn tunneled_dashboard_enforces_host_origin_and_cas_against_controller_store() {
    let homes = Homes::new(true);
    let open = seed_task(&homes.controller, TaskState::Open);
    let closed = {
        let paths = paths_for(&homes.controller);
        let store = ClientStateStore::open(&paths.state).unwrap();
        let closed = task_record(0x61, TaskState::Closed);
        store.create_task(closed.clone()).unwrap();
        closed
    };
    let mut child = spawn_dashboard(&homes, &[], Stdio::piped());
    let url = wait_for_url(&mut child);
    let host = host_from_url(&url);
    let (wrong_host, _) = http_get(&host, "/api/v1/snapshot", "127.0.0.1:1");
    assert_eq!(wrong_host, 400);

    let path = format!("/api/v1/tasks/{}/accept", open.meta().task_id());
    let missing_origin = http_post(
        &host,
        &path,
        &[
            ("Host", host.as_str()),
            ("Content-Type", "application/json"),
            ("X-Mac-Worker-Task", "1"),
        ],
        &mutation_body(&open, 1, "open"),
    );
    assert_eq!(missing_origin, 400);

    let origin = format!("http://{host}");
    let stale = http_post(
        &host,
        &path,
        &[
            ("Host", host.as_str()),
            ("Origin", origin.as_str()),
            ("Content-Type", "application/json"),
            ("X-Mac-Worker-Task", "1"),
        ],
        &mutation_body(&open, 99, "open"),
    );
    assert_eq!(stale, 409);

    let accepted = http_post(
        &host,
        &format!("/api/v1/tasks/{}/accept", closed.meta().task_id()),
        &[
            ("Host", host.as_str()),
            ("Origin", origin.as_str()),
            ("Content-Type", "application/json"),
            ("X-Mac-Worker-Task", "1"),
        ],
        &mutation_body(&closed, 1, "closed"),
    );
    assert_eq!(accepted, 200);
    assert!(!laptop_task_files(&homes.laptop));
    kill_and_reap(&mut child);
}

#[test]
fn parent_sigkill_reaps_fake_ssh_and_viewer_without_dropping_the_leader() {
    let homes = Homes::new(true);
    let leader = spawn_leader(&homes);
    let leader_root = wait_for_leader_lock(&homes);

    let mut dashboard = spawn_dashboard(&homes, &[], Stdio::piped());
    let _url = wait_for_url(&mut dashboard);
    let dashboard_pid = dashboard.id();
    let mut descendants = child_pids(dashboard_pid);
    if descendants.is_empty() {
        let recorded = fs::read_to_string(&homes.pid_file).unwrap();
        descendants.push(recorded.trim().parse().unwrap());
    }
    dashboard.kill().unwrap();
    wait_until_dead(dashboard_pid);
    for pid in descendants {
        wait_until_dead(pid);
    }
    assert!(
        ControllerLeader::acquire(&leader_root)
            .err()
            .is_some_and(|error| error.to_string().contains("CONTROLLER_LOCK_HELD"))
    );
    assert!(process_live(leader.id()));
}

#[test]
fn occupied_port_fails_without_laptop_task_files() {
    let homes = Homes::new(true);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let output = {
        let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
        apply_home(&mut command, &homes.laptop);
        apply_fake_ssh(&mut command, &homes);
        command
            .args(["dashboard", "--no-open", "--port", &port.to_string()])
            .output()
            .unwrap()
    };
    drop(listener);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("DASHBOARD_BIND_FAILED") || stderr.contains("CONTROLLER_UNAVAILABLE"),
        "stderr={stderr}"
    );
    assert!(!laptop_task_files(&homes.laptop));
}

#[test]
fn enabled_true_redirected_stdin_eof_does_not_stop_the_tunnel() {
    let homes = Homes::new(true);
    let mut child = spawn_dashboard(&homes, &[], Stdio::piped());
    let url = wait_for_url(&mut child);
    let host = host_from_url(&url);
    drop(child.stdin.take());
    std::thread::sleep(Duration::from_millis(400));
    assert!(process_live(child.id()));
    let (status, _) = http_get(&host, "/api/v1/snapshot", &host);
    assert!(status == 200 || status == 503);
    assert!(!laptop_task_files(&homes.laptop));
    kill_and_reap(&mut child);
}

#[test]
fn controller_viewer_with_enabled_config_does_not_spawn_ssh() {
    let homes = Homes::new(true);
    let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
    apply_home(&mut command, &homes.laptop);
    apply_fake_ssh(&mut command, &homes);
    command.env("MAC_WORKER_FAKE_SSH_FAIL", "1");
    let mut child = FixtureChild::new(
        command
            .args([
                "dashboard",
                "--no-open",
                "--no-facts-refresh",
                "--controller-viewer",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let url = wait_for_url(&mut child);
    let host = host_from_url(&url);
    let (status, _) = http_get(&host, "/api/v1/snapshot", &host);
    assert!(status == 200 || status == 503);
    assert!(!homes.argv_log.exists());
    drop(child.stdin.take());
    let status = wait_child_exit(&mut child, Duration::from_secs(3));
    assert!(status.success() || status.code() == Some(0));
}

#[test]
fn hidden_viewer_flag_is_presence_only() {
    assert!(Cli::try_parse_from(["worker", "dashboard", "--controller-viewer=1"]).is_err());
    assert!(Cli::try_parse_from(["worker", "dashboard", "--controller-viewer", "PATH"]).is_err());
    let parsed = Cli::try_parse_from(["worker", "dashboard", "--controller-viewer"]).unwrap();
    assert!(matches!(
        parsed.command,
        WorkerCommand::Dashboard {
            controller_viewer: true,
            ..
        }
    ));
}

#[test]
fn viewer_exits_on_int_hup_term_while_stdin_pipe_stays_open() {
    let homes = Homes::new(true);
    for signal in ["INT", "HUP", "TERM"] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
        apply_home(&mut command, &homes.laptop);
        apply_fake_ssh(&mut command, &homes);
        command.env("MAC_WORKER_FAKE_SSH_FAIL", "1");
        let mut child = FixtureChild::new(
            command
                .args([
                    "dashboard",
                    "--no-open",
                    "--no-facts-refresh",
                    "--controller-viewer",
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let url = wait_for_url(&mut child);
        let host = host_from_url(&url);
        let (status, _) = http_get(&host, "/api/v1/snapshot", &host);
        assert!(status == 200 || status == 503);
        assert!(child.stdin.as_ref().is_some(), "stdin pipe must stay open");
        let pid = child.id();
        send_signal(pid, signal);
        wait_until_dead(pid);
        assert!(!homes.argv_log.exists(), "viewer must not spawn ssh");
    }
}

#[test]
fn tunnel_ctrl_c_reaps_term_ignoring_fake_ssh_and_keeps_leader() {
    let homes = Homes::new(true);
    let leader = spawn_leader(&homes);
    let leader_root = wait_for_leader_lock(&homes);
    let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
    apply_home(&mut command, &homes.laptop);
    apply_fake_ssh(&mut command, &homes);
    command.env("MAC_WORKER_FAKE_SSH_IGNORE_TERM", "1");
    let mut dashboard = FixtureChild::new(
        command
            .args(["dashboard", "--no-open"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let _url = wait_for_url(&mut dashboard);
    let stub_pid = fs::read_to_string(&homes.pid_file)
        .unwrap()
        .trim()
        .parse::<u32>()
        .unwrap();
    let dashboard_pid = dashboard.id();
    send_signal(dashboard_pid, "INT");
    wait_until_dead(dashboard_pid);
    wait_until_dead(stub_pid);
    assert!(
        ControllerLeader::acquire(&leader_root)
            .err()
            .is_some_and(|error| error.to_string().contains("CONTROLLER_LOCK_HELD"))
    );
    assert!(process_live(leader.id()));
}

#[test]
fn readiness_timeout_reaps_a_silent_term_ignoring_stub() {
    let homes = Homes::new(true);
    let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
    apply_home(&mut command, &homes.laptop);
    apply_fake_ssh(&mut command, &homes);
    command.env("MAC_WORKER_FAKE_SSH_HANG_STDOUT", "1");
    let started = Instant::now();
    let output = command.args(["dashboard", "--no-open"]).output().unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(14),
        "readiness hang took {:?}",
        started.elapsed()
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("CONTROLLER_UNAVAILABLE"), "stderr={stderr}");
    assert!(!laptop_task_files(&homes.laptop));
    if homes.pid_file.exists() {
        let stub_pid = fs::read_to_string(&homes.pid_file)
            .unwrap()
            .trim()
            .parse::<u32>()
            .unwrap();
        wait_until_dead(stub_pid);
    }
}

#[test]
fn readiness_timeout_rejects_slow_drip_http_and_reaps_child() {
    let homes = Homes::new(true);
    let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
    apply_home(&mut command, &homes.laptop);
    apply_fake_ssh(&mut command, &homes);
    command.env("MAC_WORKER_FAKE_SSH_DRIP_HTTP", "1");
    let started = Instant::now();
    let output = command.args(["dashboard", "--no-open"]).output().unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_secs(7),
        "slow drip finished before the absolute deadline: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(12),
        "slow drip exceeded the absolute readiness deadline: {elapsed:?}"
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("CONTROLLER_UNAVAILABLE"), "stderr={stderr}");
    assert!(!laptop_task_files(&homes.laptop));
    let stub_pid = wait_for_pid_file(&homes.pid_file);
    wait_until_dead(stub_pid);
    assert!(!process_group_live(stub_pid));
}

#[test]
fn startup_int_and_term_reap_silent_child_group_before_url() {
    let homes = Homes::new(true);
    for signal in ["INT", "TERM"] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
        apply_home(&mut command, &homes.laptop);
        apply_fake_ssh(&mut command, &homes);
        command.env("MAC_WORKER_FAKE_SSH_HANG_STDOUT", "1");
        let mut dashboard = FixtureChild::new(
            command
                .args(["dashboard", "--no-open"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let stub_pid = wait_for_pid_file(&homes.pid_file);
        let dashboard_pid = dashboard.id();
        send_signal(dashboard_pid, signal);
        wait_until_dead(dashboard_pid);
        wait_until_dead(stub_pid);
        assert!(
            !process_group_live(stub_pid),
            "{signal} left ssh group {stub_pid} live"
        );
        let _ = fs::remove_file(&homes.pid_file);
        let _ = dashboard.try_wait();
    }
}

#[test]
fn startup_int_reaps_slow_drip_http_child_group() {
    let homes = Homes::new(true);
    let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
    apply_home(&mut command, &homes.laptop);
    apply_fake_ssh(&mut command, &homes);
    command.env("MAC_WORKER_FAKE_SSH_DRIP_HTTP", "1");
    let mut dashboard = FixtureChild::new(
        command
            .args(["dashboard", "--no-open"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stub_pid = wait_for_pid_file(&homes.pid_file);
    std::thread::sleep(Duration::from_millis(300));
    let dashboard_pid = dashboard.id();
    send_signal(dashboard_pid, "INT");
    wait_until_dead(dashboard_pid);
    wait_until_dead(stub_pid);
    assert!(!process_group_live(stub_pid));
    let _ = dashboard.try_wait();
}
