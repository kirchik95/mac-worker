//! Controller-viewer heartbeat watchdog and laptop tunnel reconnect.

use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    path::PathBuf,
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

#[test]
fn viewer_heartbeat_loss_exits_tempfail() {
    let homes = Homes::new();
    let mut child = spawn_viewer(&homes, 200);
    let _url = wait_for_url(&child.stdout, &mut child.child);
    child.stdin.as_mut().unwrap().write_all(b"\n").unwrap();
    child.stdin.as_mut().unwrap().flush().unwrap();
    let status = wait_child_exit(&mut child.child, Duration::from_secs(3));
    let stderr = child.stderr_text();
    assert_eq!(status.code(), Some(75), "stderr={stderr}");
    assert!(
        stderr
            .lines()
            .any(|line| line == "DASHBOARD_VIEWER_HEARTBEAT_LOST"),
        "stderr={stderr}"
    );
    assert!(
        !stderr.contains("DASHBOARD_VIEWER_HEARTBEAT_LOST:"),
        "stderr={stderr}"
    );
}

struct Homes {
    _root: tempfile::TempDir,
    laptop: PathBuf,
    controller: PathBuf,
    fake_ssh: PathBuf,
    argv_log: PathBuf,
    pid_file: PathBuf,
    generation_file: PathBuf,
    port_file: PathBuf,
    stdin_log: PathBuf,
}

impl Homes {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let laptop = prepare_home(root.path().join("laptop"));
        let controller = prepare_home(root.path().join("controller"));
        let fake_ssh = root.path().join("fake-ssh");
        fs::write(&fake_ssh, FAKE_SSH).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&fake_ssh, fs::Permissions::from_mode(0o755)).unwrap();
        }
        Self {
            laptop,
            controller,
            fake_ssh,
            argv_log: root.path().join("argv.json"),
            pid_file: root.path().join("ssh.pid"),
            generation_file: root.path().join("generation"),
            port_file: root.path().join("port"),
            stdin_log: root.path().join("stdin.log"),
            _root: root,
        }
    }
}

fn prepare_home(home: PathBuf) -> PathBuf {
    fs::create_dir_all(home.join(".config/mac-worker")).unwrap();
    fs::write(
        home.join(".config/mac-worker/config.toml"),
        "version = 1\n[controller]\nenabled = true\nssh = \"controller.local\"\n",
    )
    .unwrap();
    for directory in [".local/state", ".cache", ".local/share"] {
        fs::create_dir_all(home.join(directory)).unwrap();
    }
    fs::canonicalize(home).unwrap()
}

struct CapturedChild {
    child: Child,
    stdin: Option<std::process::ChildStdin>,
    stdout: Arc<Mutex<String>>,
    stderr: Arc<Mutex<String>>,
    stdout_thread: Option<thread::JoinHandle<()>>,
    stderr_thread: Option<thread::JoinHandle<()>>,
}

impl CapturedChild {
    fn stderr_text(&mut self) -> String {
        if let Some(handle) = self.stderr_thread.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.stdout_thread.take() {
            let _ = handle.join();
        }
        self.stderr.lock().unwrap().clone()
    }
}

impl Drop for CapturedChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(handle) = self.stdout_thread.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.stderr_thread.take() {
            let _ = handle.join();
        }
    }
}

fn spawn_viewer(homes: &Homes, heartbeat_ms: u64) -> CapturedChild {
    let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
    command
        .env("HOME", &homes.laptop)
        .env("XDG_CONFIG_HOME", homes.laptop.join(".config"))
        .env("XDG_STATE_HOME", homes.laptop.join(".local/state"))
        .env("XDG_CACHE_HOME", homes.laptop.join(".cache"))
        .env("XDG_DATA_HOME", homes.laptop.join(".local/share"))
        .env(
            "MAC_WORKER_TEST_VIEWER_HEARTBEAT_MS",
            heartbeat_ms.to_string(),
        )
        .args([
            "dashboard",
            "--no-open",
            "--no-facts-refresh",
            "--controller-viewer",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().unwrap();
    let stdin = child.stdin.take();
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let stdout_buf = Arc::new(Mutex::new(String::new()));
    let stderr_buf = Arc::new(Mutex::new(String::new()));
    CapturedChild {
        child,
        stdin,
        stdout_thread: Some(spawn_reader(stdout, Arc::clone(&stdout_buf))),
        stderr_thread: Some(spawn_reader(stderr, Arc::clone(&stderr_buf))),
        stdout: stdout_buf,
        stderr: stderr_buf,
    }
}

fn spawn_reader<R>(reader: R, slot: Arc<Mutex<String>>) -> thread::JoinHandle<()>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut line = String::new();
        let mut reader = BufReader::new(reader);
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => slot.lock().unwrap().push_str(&line),
            }
        }
    })
}

fn wait_for_url(stdout: &Arc<Mutex<String>>, child: &mut Child) -> String {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let text = stdout.lock().unwrap().clone();
        if let Some(url) = text.lines().find(|line| line.starts_with("http://")) {
            return url.to_owned();
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!("viewer exited before URL: {status:?} stdout={text}");
        }
        if Instant::now() >= deadline {
            panic!("viewer URL was not printed before timeout: stdout={text}");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_child_exit(child: &mut Child, timeout: Duration) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return status;
        }
        if Instant::now() >= deadline {
            panic!("child {} still live after {timeout:?}", child.id());
        }
        thread::sleep(Duration::from_millis(20));
    }
}

const LOST: &str = "dashboard: controller tunnel lost; reconnecting…";
const RESTORED: &str = "dashboard: controller tunnel restored";

struct TunnelTimings {
    heartbeat_ms: u64,
    backoff_initial_ms: u64,
    backoff_cap_ms: u64,
    readiness_ms: u64,
    rotation_ms: u64,
}

#[test]
fn laptop_heartbeat_reaches_the_ssh_stdin() {
    let homes = Homes::new();
    let mut child = spawn_tunnel(
        &homes,
        "record-stdin",
        &TunnelTimings {
            heartbeat_ms: 60,
            backoff_initial_ms: 1_000,
            backoff_cap_ms: 1_000,
            readiness_ms: 8_000,
            rotation_ms: 60_000,
        },
    );
    let _url = wait_for_url(&child.stdout, &mut child.child);
    let args: Vec<String> = serde_json::from_slice(&fs::read(&homes.argv_log).unwrap()).unwrap();
    for option in [
        "-T",
        "ControlMaster=no",
        "ControlPath=none",
        "ServerAliveInterval=15",
        "ServerAliveCountMax=3",
        "ExitOnForwardFailure=yes",
        "BatchMode=yes",
    ] {
        assert!(
            args.iter().any(|arg| arg == option),
            "missing {option} in {args:?}"
        );
    }
    assert!(!args.iter().any(|arg| {
        arg == "ControlMaster=auto" || arg == "ControlPersist=60" || arg == "ServerAliveInterval=10"
    }));
    let first = wait_for_stdin_samples(&homes, 3, Duration::from_secs(4));
    thread::sleep(Duration::from_millis(250));
    let later = read_stdin_samples(&homes);
    assert!(
        later.len() > first.len(),
        "heartbeat stopped after {} bytes: {later:?}",
        first.len()
    );
    assert!(later.iter().all(|(_, byte)| *byte == 0x0a), "{later:?}");
    send_signal(child.child.id(), "TERM");
    let status = wait_child_exit(&mut child.child, Duration::from_secs(3));
    assert_eq!(status.code(), Some(0), "stderr={}", child.stderr_text());
}

#[test]
fn tunnel_reconnects_on_the_same_port_until_signalled() {
    let homes = Homes::new();
    let mut child = spawn_tunnel(
        &homes,
        "drop-then-stay",
        &TunnelTimings {
            heartbeat_ms: 40,
            backoff_initial_ms: 80,
            backoff_cap_ms: 160,
            readiness_ms: 8_000,
            rotation_ms: 60_000,
        },
    );
    let url = wait_for_url(&child.stdout, &mut child.child);
    wait_for_stderr(&child.stderr, LOST, Duration::from_secs(4));
    wait_for_stderr(&child.stderr, RESTORED, Duration::from_secs(8));
    thread::sleep(Duration::from_millis(200));
    assert!(process_live(child.child.id()));
    let stdout = child.stdout.lock().unwrap().clone();
    let urls = url_lines(&stdout);
    assert_eq!(urls, vec![url], "stdout={stdout}");
    let stderr = child.stderr.lock().unwrap().clone();
    assert_eq!(count_line(&stderr, LOST), 1, "{stderr}");
    assert_eq!(count_line(&stderr, RESTORED), 1, "{stderr}");
    send_signal(child.child.id(), "TERM");
    let status = wait_child_exit(&mut child.child, Duration::from_secs(4));
    assert_eq!(status.code(), Some(0), "stderr={}", child.stderr_text());
}

#[test]
fn ssh_connection_failures_keep_the_published_port() {
    let homes = Homes::new();
    let mut child = spawn_tunnel(
        &homes,
        "connect-down",
        &TunnelTimings {
            heartbeat_ms: 40,
            backoff_initial_ms: 25,
            backoff_cap_ms: 50,
            readiness_ms: 2_000,
            rotation_ms: 250,
        },
    );
    let url = wait_for_url(&child.stdout, &mut child.child);
    wait_for_stderr(&child.stderr, LOST, Duration::from_secs(4));
    thread::sleep(Duration::from_millis(1_500));
    assert!(
        process_live(child.child.id()),
        "dashboard exited during reconnect"
    );
    let stdout = child.stdout.lock().unwrap().clone();
    assert_eq!(url_lines(&stdout), vec![url], "stdout={stdout}");
    let stderr = child.stderr.lock().unwrap().clone();
    assert_eq!(count_line(&stderr, LOST), 1, "{stderr}");
    assert_eq!(count_line(&stderr, RESTORED), 0, "{stderr}");
    send_signal(child.child.id(), "TERM");
    let status = wait_child_exit(&mut child.child, Duration::from_secs(4));
    assert_eq!(status.code(), Some(0), "stderr={}", child.stderr_text());
}

#[test]
fn local_port_in_use_rotates_even_when_ssh_would_exit_255() {
    let homes = Homes::new();
    let mut child = spawn_tunnel(
        &homes,
        "connect-down",
        &TunnelTimings {
            heartbeat_ms: 40,
            backoff_initial_ms: 25,
            backoff_cap_ms: 50,
            readiness_ms: 2_000,
            rotation_ms: 300,
        },
    );
    let url = wait_for_url(&child.stdout, &mut child.child);
    wait_for_stderr(&child.stderr, LOST, Duration::from_secs(4));
    let port: u16 = url.rsplit(':').next().unwrap().parse().unwrap();
    let _held = hold_loopback_port(port);
    let urls = wait_for_urls(&child.stdout, &mut child.child, 2, Duration::from_secs(8));
    assert_ne!(urls[0], urls[1], "{urls:?}");
    wait_for_stderr(&child.stderr, RESTORED, Duration::from_secs(2));
    let stderr = child.stderr.lock().unwrap().clone();
    assert_eq!(count_line(&stderr, LOST), 1, "{stderr}");
    assert_eq!(count_line(&stderr, RESTORED), 1, "{stderr}");
    send_signal(child.child.id(), "TERM");
    let status = wait_child_exit(&mut child.child, Duration::from_secs(4));
    assert_eq!(status.code(), Some(0), "stderr={}", child.stderr_text());
}

#[test]
fn stdout_eof_before_a_remote_exit_still_rotates() {
    let homes = Homes::new();
    let mut child = spawn_tunnel(
        &homes,
        "stdout-then-exit",
        &TunnelTimings {
            heartbeat_ms: 40,
            backoff_initial_ms: 25,
            backoff_cap_ms: 50,
            readiness_ms: 2_000,
            rotation_ms: 400,
        },
    );
    wait_for_url(&child.stdout, &mut child.child);
    let urls = wait_for_urls(&child.stdout, &mut child.child, 2, Duration::from_secs(8));
    assert_ne!(urls[0], urls[1], "{urls:?}");
    wait_for_stderr(&child.stderr, RESTORED, Duration::from_secs(2));
    let stderr = child.stderr.lock().unwrap().clone();
    assert_eq!(count_line(&stderr, LOST), 1, "{stderr}");
    assert_eq!(count_line(&stderr, RESTORED), 1, "{stderr}");
    send_signal(child.child.id(), "TERM");
    let status = wait_child_exit(&mut child.child, Duration::from_secs(4));
    assert_eq!(status.code(), Some(0), "stderr={}", child.stderr_text());
}

#[test]
fn remote_viewer_failure_prints_a_new_url() {
    let homes = Homes::new();
    let mut child = spawn_tunnel(
        &homes,
        "sticky-port",
        &TunnelTimings {
            heartbeat_ms: 40,
            backoff_initial_ms: 25,
            backoff_cap_ms: 50,
            readiness_ms: 4_000,
            rotation_ms: 300,
        },
    );
    wait_for_url(&child.stdout, &mut child.child);
    let urls = wait_for_urls(&child.stdout, &mut child.child, 2, Duration::from_secs(8));
    assert_ne!(urls[0], urls[1], "{urls:?}");
    wait_for_stderr(&child.stderr, RESTORED, Duration::from_secs(2));
    let stderr = child.stderr.lock().unwrap().clone();
    assert_eq!(count_line(&stderr, LOST), 1, "{stderr}");
    assert_eq!(count_line(&stderr, RESTORED), 1, "{stderr}");
    let stdout = child.stdout.lock().unwrap().clone();
    assert_eq!(url_lines(&stdout).len(), 2, "stdout={stdout}");
    send_signal(child.child.id(), "TERM");
    let status = wait_child_exit(&mut child.child, Duration::from_secs(4));
    assert_eq!(status.code(), Some(0), "stderr={}", child.stderr_text());
}

#[test]
fn signal_during_backoff_exits_cleanly() {
    let homes = Homes::new();
    let mut child = spawn_tunnel(
        &homes,
        "drop-then-hang",
        &TunnelTimings {
            heartbeat_ms: 40,
            backoff_initial_ms: 5_000,
            backoff_cap_ms: 5_000,
            readiness_ms: 5_000,
            rotation_ms: 60_000,
        },
    );
    wait_for_url(&child.stdout, &mut child.child);
    wait_for_stderr(&child.stderr, LOST, Duration::from_secs(4));
    send_signal(child.child.id(), "TERM");
    let started = Instant::now();
    let status = wait_child_exit(&mut child.child, Duration::from_secs(2));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "signal during backoff took {:?}",
        started.elapsed()
    );
    let stderr = child.stderr_text();
    assert_eq!(status.code(), Some(0), "stderr={stderr}");
    assert_eq!(count_line(&stderr, RESTORED), 0, "{stderr}");
}

#[test]
fn signal_during_readiness_reaps_the_child() {
    let homes = Homes::new();
    let mut child = spawn_tunnel(
        &homes,
        "drop-then-hang",
        &TunnelTimings {
            heartbeat_ms: 40,
            backoff_initial_ms: 40,
            backoff_cap_ms: 80,
            readiness_ms: 5_000,
            rotation_ms: 60_000,
        },
    );
    wait_for_url(&child.stdout, &mut child.child);
    wait_for_stderr(&child.stderr, LOST, Duration::from_secs(4));
    let first_pid = read_pid(&homes.pid_file);
    let hung = wait_for_new_pid(&homes.pid_file, first_pid, Duration::from_secs(3));
    send_signal(child.child.id(), "INT");
    let status = wait_child_exit(&mut child.child, Duration::from_secs(6));
    let stderr = child.stderr_text();
    assert_eq!(status.code(), Some(0), "stderr={stderr}");
    assert!(!process_live(hung), "ssh child {hung} was not reaped");
}

#[test]
fn first_start_that_never_becomes_ready_returns_the_error() {
    let homes = Homes::new();
    let mut child = spawn_tunnel(
        &homes,
        "fail",
        &TunnelTimings {
            heartbeat_ms: 5_000,
            backoff_initial_ms: 1_000,
            backoff_cap_ms: 1_000,
            readiness_ms: 2_000,
            rotation_ms: 60_000,
        },
    );
    let status = wait_child_exit(&mut child.child, Duration::from_secs(4));
    let stderr = child.stderr_text();
    assert!(!status.success(), "stderr={stderr}");
    assert!(stderr.contains("CONTROLLER_UNAVAILABLE"), "{stderr}");
    assert!(!stderr.contains(LOST), "{stderr}");
    assert!(!stderr.contains(RESTORED), "{stderr}");
}

fn spawn_tunnel(homes: &Homes, mode: &str, timings: &TunnelTimings) -> CapturedChild {
    let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
    command
        .env("HOME", &homes.laptop)
        .env("XDG_CONFIG_HOME", homes.laptop.join(".config"))
        .env("XDG_STATE_HOME", homes.laptop.join(".local/state"))
        .env("XDG_CACHE_HOME", homes.laptop.join(".cache"))
        .env("XDG_DATA_HOME", homes.laptop.join(".local/share"))
        .env("MAC_WORKER_FAKE_SSH", &homes.fake_ssh)
        .env("MAC_WORKER_TEST_BIN", env!("CARGO_BIN_EXE_worker"))
        .env("MAC_WORKER_FAKE_SSH_ARGV_LOG", &homes.argv_log)
        .env("MAC_WORKER_FAKE_SSH_PID_FILE", &homes.pid_file)
        .env(
            "MAC_WORKER_FAKE_SSH_GENERATION_FILE",
            &homes.generation_file,
        )
        .env("MAC_WORKER_FAKE_SSH_PORT_FILE", &homes.port_file)
        .env("MAC_WORKER_FAKE_SSH_STDIN_LOG", &homes.stdin_log)
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
        )
        .env("MAC_WORKER_TUNNEL_FAKE_MODE", mode)
        .env(
            "MAC_WORKER_TEST_TUNNEL_HEARTBEAT_MS",
            timings.heartbeat_ms.to_string(),
        )
        .env(
            "MAC_WORKER_TEST_TUNNEL_BACKOFF_INITIAL_MS",
            timings.backoff_initial_ms.to_string(),
        )
        .env(
            "MAC_WORKER_TEST_TUNNEL_BACKOFF_CAP_MS",
            timings.backoff_cap_ms.to_string(),
        )
        .env(
            "MAC_WORKER_TEST_TUNNEL_READINESS_MS",
            timings.readiness_ms.to_string(),
        )
        .env(
            "MAC_WORKER_TEST_TUNNEL_PORT_ROTATION_MS",
            timings.rotation_ms.to_string(),
        )
        .args(["dashboard", "--no-open", "--no-facts-refresh"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().unwrap();
    let stdin = child.stdin.take();
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let stdout_buf = Arc::new(Mutex::new(String::new()));
    let stderr_buf = Arc::new(Mutex::new(String::new()));
    CapturedChild {
        child,
        stdin,
        stdout_thread: Some(spawn_reader(stdout, Arc::clone(&stdout_buf))),
        stderr_thread: Some(spawn_reader(stderr, Arc::clone(&stderr_buf))),
        stdout: stdout_buf,
        stderr: stderr_buf,
    }
}

fn wait_for_stderr(stderr: &Arc<Mutex<String>>, needle: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let text = stderr.lock().unwrap().clone();
        if text.contains(needle) {
            return;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for {needle:?} in {text:?}");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_urls(
    stdout: &Arc<Mutex<String>>,
    child: &mut Child,
    count: usize,
    timeout: Duration,
) -> Vec<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let text = stdout.lock().unwrap().clone();
        let urls = url_lines(&text);
        if urls.len() >= count {
            return urls;
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!("dashboard exited before {count} URLs: {status:?} stdout={text}");
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for {count} URLs, stdout={text}");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn hold_loopback_port(port: u16) -> std::net::TcpListener {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match std::net::TcpListener::bind(("127.0.0.1", port)) {
            Ok(listener) => return listener,
            Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
            Err(error) => panic!("could not hold 127.0.0.1:{port}: {error}"),
        }
    }
}

fn url_lines(text: &str) -> Vec<String> {
    text.lines()
        .filter(|line| line.starts_with("http://127.0.0.1:"))
        .map(str::to_owned)
        .collect()
}

fn count_line(text: &str, line: &str) -> usize {
    text.lines().filter(|item| *item == line).count()
}

fn read_stdin_samples(homes: &Homes) -> Vec<(String, u8)> {
    let Ok(text) = fs::read_to_string(&homes.stdin_log) else {
        return Vec::new();
    };
    text.lines()
        .filter_map(|line| {
            let (stamp, byte) = line.split_once(' ')?;
            Some((stamp.to_owned(), u8::from_str_radix(byte, 16).ok()?))
        })
        .collect()
}

fn wait_for_stdin_samples(homes: &Homes, count: usize, timeout: Duration) -> Vec<(String, u8)> {
    let deadline = Instant::now() + timeout;
    loop {
        let samples = read_stdin_samples(homes);
        if samples.len() >= count {
            return samples;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for {count} heartbeat bytes, saw {samples:?}");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn read_pid(path: &std::path::Path) -> u32 {
    fs::read_to_string(path)
        .ok()
        .and_then(|text| text.trim().parse().ok())
        .unwrap_or(0)
}

fn wait_for_new_pid(path: &std::path::Path, previous: u32, timeout: Duration) -> u32 {
    let deadline = Instant::now() + timeout;
    loop {
        let pid = read_pid(path);
        if pid != 0 && pid != previous && process_live(pid) {
            return pid;
        }
        if Instant::now() >= deadline {
            panic!("replacement ssh pid was not written (previous {previous})");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn process_live(pid: u32) -> bool {
    Command::new("/bin/kill")
        .args(["-0", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn send_signal(pid: u32, signal: &str) {
    let status = Command::new("/bin/kill")
        .args([format!("-{signal}"), pid.to_string()])
        .status()
        .unwrap();
    assert!(status.success(), "kill -{signal} {pid} failed");
}

const FAKE_SSH: &str = r#"#!/usr/bin/env python3
import fcntl
import os
import signal
import subprocess
import sys
import time

def fail(message):
    sys.stderr.write(message + "\n")
    sys.exit(255)

def port_from_argv():
    argv = sys.argv[1:]
    for index, arg in enumerate(argv):
        if arg == "-L" and index + 1 < len(argv):
            spec = argv[index + 1].split(":")
            if len(spec) >= 2 and spec[1].isdigit():
                return int(spec[1])
    remote = argv[-1] if argv else ""
    parts = remote.split()
    if "--port" in parts:
        index = parts.index("--port")
        if index + 1 < len(parts) and parts[index + 1].isdigit():
            return int(parts[index + 1])
    return None

def write_pid():
    path = os.environ.get("MAC_WORKER_FAKE_SSH_PID_FILE")
    if path:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(str(os.getpid()))

def next_generation():
    path = os.environ.get("MAC_WORKER_FAKE_SSH_GENERATION_FILE")
    if not path:
        return 1
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        os.lseek(fd, 0, os.SEEK_SET)
        raw = os.read(fd, 64).decode().strip()
        generation = int(raw) + 1 if raw else 1
        os.lseek(fd, 0, os.SEEK_SET)
        os.ftruncate(fd, 0)
        os.write(fd, str(generation).encode())
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)
    return generation

def apply_remote_env():
    mapping = {
        "MAC_WORKER_FAKE_SSH_REMOTE_HOME": "HOME",
        "MAC_WORKER_FAKE_SSH_REMOTE_CONFIG": "XDG_CONFIG_HOME",
        "MAC_WORKER_FAKE_SSH_REMOTE_STATE": "XDG_STATE_HOME",
        "MAC_WORKER_FAKE_SSH_REMOTE_CACHE": "XDG_CACHE_HOME",
        "MAC_WORKER_FAKE_SSH_REMOTE_DATA": "XDG_DATA_HOME",
    }
    for source, dest in mapping.items():
        if value := os.environ.get(source):
            os.environ[dest] = value

def viewer_argv():
    remote = sys.argv[-1] if len(sys.argv) > 1 else ""
    prefix = "~/.local/bin/worker "
    if not remote.startswith(prefix):
        fail("labelled fake ssh: unexpected remote command")
    worker = os.environ.get("MAC_WORKER_TEST_BIN")
    if not worker:
        fail("MAC_WORKER_TEST_BIN is required")
    return [worker, *remote.split()[1:]]

def exec_viewer():
    apply_remote_env()
    argv = viewer_argv()
    os.execv(argv[0], argv)

def spawn_viewer():
    apply_remote_env()
    read_fd, write_fd = os.pipe()
    proc = subprocess.Popen(viewer_argv(), stdin=read_fd, stdout=sys.stdout, stderr=sys.stderr)
    os.close(read_fd)
    return proc, write_fd

def stop_viewer(proc, write_fd):
    os.close(write_fd)
    proc.terminate()
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()

def read_stdin_byte():
    while True:
        try:
            return os.read(sys.stdin.fileno(), 1)
        except InterruptedError:
            continue

def drop_after_heartbeat():
    proc, write_fd = spawn_viewer()
    read_stdin_byte()
    stop_viewer(proc, write_fd)
    sys.exit(255)

def record_stdin():
    path = os.environ.get("MAC_WORKER_FAKE_SSH_STDIN_LOG")
    if not path:
        fail("stdin log path is required")
    proc, write_fd = spawn_viewer()
    try:
        with open(path, "w", encoding="utf-8") as log:
            while True:
                data = read_stdin_byte()
                if not data:
                    break
                log.write("%f %02x\n" % (time.time(), data[0]))
                log.flush()
    finally:
        try:
            stop_viewer(proc, write_fd)
        except Exception:
            pass
    sys.exit(0)

def hang():
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    while True:
        time.sleep(3600)

def stdout_eof_then_status(generation, port, fail_status):
    port_file = os.environ.get("MAC_WORKER_FAKE_SSH_PORT_FILE")
    if not port_file or port is None:
        fail("stdout-then-exit requires a forward port")
    if generation == 1:
        with open(port_file, "w", encoding="utf-8") as handle:
            handle.write(str(port))
        drop_after_heartbeat()
    saved = open(port_file, encoding="utf-8").read().strip()
    if str(port) == saved:
        sys.stdout.flush()
        os.close(1)
        time.sleep(0.2)
        os._exit(fail_status)
    exec_viewer()

def sticky_port(generation, port, fail_status):
    port_file = os.environ.get("MAC_WORKER_FAKE_SSH_PORT_FILE")
    if not port_file or port is None:
        fail("sticky port requires a forward port")
    if generation == 1:
        with open(port_file, "w", encoding="utf-8") as handle:
            handle.write(str(port))
        drop_after_heartbeat()
    saved = open(port_file, encoding="utf-8").read().strip()
    if str(port) == saved:
        sys.exit(fail_status)
    exec_viewer()

def log_argv():
    path = os.environ.get("MAC_WORKER_FAKE_SSH_ARGV_LOG")
    if not path:
        return
    import json
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(sys.argv, handle)

def main():
    mode = os.environ.get("MAC_WORKER_TUNNEL_FAKE_MODE", "")
    log_argv()
    write_pid()
    if mode == "fail":
        sys.exit(255)
    if mode == "hang":
        hang()
    generation = next_generation()
    port = port_from_argv()
    if mode == "record-stdin":
        record_stdin()
    if mode == "drop-then-stay":
        if generation == 1:
            drop_after_heartbeat()
        exec_viewer()
    if mode == "drop-then-hang":
        if generation == 1:
            drop_after_heartbeat()
        hang()
    if mode == "sticky-port":
        sticky_port(generation, port, 1)
    if mode == "connect-down":
        sticky_port(generation, port, 255)
    if mode == "stdout-then-exit":
        stdout_eof_then_status(generation, port, 1)
    fail("unknown fake ssh mode")

if __name__ == "__main__":
    main()
"#;
