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
}

impl Homes {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let laptop = root.path().join("laptop");
        fs::create_dir_all(laptop.join(".config/mac-worker")).unwrap();
        fs::write(
            laptop.join(".config/mac-worker/config.toml"),
            "version = 1\n[controller]\nenabled = true\nssh = \"controller.local\"\n",
        )
        .unwrap();
        for directory in [".local/state", ".cache", ".local/share"] {
            fs::create_dir_all(laptop.join(directory)).unwrap();
        }
        Self {
            laptop: fs::canonicalize(laptop).unwrap(),
            _root: root,
        }
    }
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
