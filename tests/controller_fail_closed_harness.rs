use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fs,
    io::{self, Write},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use mac_worker::{
    controller::{MAX_FRAME_BYTES, decode_frame, encode_frame},
    job::HostControlError,
    paths::PathLayout,
    protocol::PROTOCOL_VERSION,
};
use serde_json::{Value, json};

pub const TASK_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";
pub const TURN_ID: &str = "018f0f4a6b5c7d8e9f00112233445577";
pub const REQUEST_ID: &str = "018f0f4a6b5c7d8e9f00112233445588";
pub const REQUEST_ID_OTHER: &str = "018f0f4a6b5c7d8e9f00112233445599";
const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const BASE_OID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CHILD_WAIT: Duration = Duration::from_secs(15);
const TERM_WAIT: Duration = Duration::from_secs(2);

pub struct IsolatedHomes {
    _root: tempfile::TempDir,
    pub laptop: PathBuf,
    pub fake_ssh: PathBuf,
    pub ssh_log: PathBuf,
}

impl IsolatedHomes {
    pub fn enabled_controller() -> Self {
        Self::new(true)
    }

    pub fn default_disabled() -> Self {
        Self::new(false)
    }

    fn new(controller_enabled: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        let root_path = root.path().canonicalize().unwrap();
        let laptop = prepare_home(&root_path.join("laptop"), controller_enabled);
        let ssh_dir = root_path.join("ssh bin");
        fs::create_dir_all(&ssh_dir).unwrap();
        let fake_ssh = ssh_dir.join("fake'ssh hop");
        let source =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/controller_fail_closed_fake_ssh.py");
        fs::copy(&source, &fake_ssh).unwrap();
        let mut permissions = fs::metadata(&fake_ssh).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&fake_ssh, permissions).unwrap();
        Self {
            ssh_log: root_path.join("fake-ssh.ndjson"),
            _root: root,
            laptop,
            fake_ssh,
        }
    }

    pub fn paths(&self) -> PathLayout {
        PathLayout::discover(None, &xdg_env(&self.laptop), &self.laptop).unwrap()
    }
}

fn prepare_home(home: &Path, controller_enabled: bool) -> PathBuf {
    fs::create_dir_all(home).unwrap();
    for directory in [
        home.join(".config/mac-worker"),
        home.join(".local/state"),
        home.join(".cache"),
        home.join(".local/share"),
        home.join(".local/bin"),
    ] {
        fs::create_dir_all(directory).unwrap();
    }
    let config = if controller_enabled {
        "version = 1\n[controller]\nenabled = true\nssh = \"fakecontroller\"\n"
    } else {
        "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac-worker-test.invalid\"\nslots = 1\n"
    };
    fs::write(home.join(".config/mac-worker/config.toml"), config).unwrap();
    home.canonicalize().unwrap()
}

fn xdg_env(home: &Path) -> BTreeMap<OsString, OsString> {
    BTreeMap::from([
        (OsString::from("HOME"), home.as_os_str().to_os_string()),
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
    ])
}

pub fn apply_private_env(command: &mut Command, homes: &IsolatedHomes, attach_fake_ssh: bool) {
    command
        .env("HOME", &homes.laptop)
        .env("XDG_CONFIG_HOME", homes.laptop.join(".config"))
        .env("XDG_STATE_HOME", homes.laptop.join(".local/state"))
        .env("XDG_CACHE_HOME", homes.laptop.join(".cache"))
        .env("XDG_DATA_HOME", homes.laptop.join(".local/share"))
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .env_remove("GIT_ALTERNATE_OBJECT_DIRECTORIES")
        .env_remove("GIT_SSH_COMMAND")
        .env_remove("SSH_AUTH_SOCK");
    if attach_fake_ssh {
        command
            .env("MAC_WORKER_TEST_SSH", &homes.fake_ssh)
            .env("MAC_WORKER_FAIL_CLOSED_SSH_LOG", &homes.ssh_log);
    } else {
        command.env_remove("MAC_WORKER_TEST_SSH");
        command.env_remove("MAC_WORKER_FAIL_CLOSED_SSH_LOG");
    }
}

pub struct OwnedChild {
    child: Option<Child>,
}

impl OwnedChild {
    pub fn spawn(command: &mut Command) -> Self {
        Self {
            child: Some(command.spawn().unwrap()),
        }
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("child already reaped")
    }

    pub fn take_stdin(&mut self) -> std::process::ChildStdin {
        self.child_mut().stdin.take().expect("stdin pipe")
    }

    pub fn wait_timeout(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            let status = {
                let child = self.child.as_mut()?;
                child.try_wait().unwrap()
            };
            if let Some(status) = status {
                self.child = None;
                return Some(status);
            }
            if Instant::now() > deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn signal(&self, signal: i32) {
        if let Some(child) = &self.child {
            let pid = i32::try_from(child.id()).expect("child pid");
            unsafe {
                libc::kill(pid, signal);
            }
        }
    }

    pub fn terminate_then_kill(&mut self) {
        if self.child.is_none() {
            return;
        }
        self.signal(libc::SIGTERM);
        if self.wait_timeout(TERM_WAIT).is_some() {
            return;
        }
        self.signal(libc::SIGKILL);
        if let Some(mut child) = self.child.take() {
            let _ = child.wait();
        }
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        self.terminate_then_kill();
    }
}

pub struct ChildOutput {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl ChildOutput {
    pub fn stdout_lossy(&self) -> String {
        String::from_utf8_lossy(&self.stdout).into_owned()
    }

    pub fn stderr_lossy(&self) -> String {
        String::from_utf8_lossy(&self.stderr).into_owned()
    }

    pub fn json_code(&self) -> Option<String> {
        let text = self.stdout_lossy();
        let line = text.lines().next()?;
        let value: Value = serde_json::from_str(line).ok()?;
        value
            .get("code")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    }
}

fn take_pipe_bytes(mut pipe: impl io::Read + Send + 'static) -> thread::JoinHandle<Vec<u8>> {
    thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = io::Read::read_to_end(&mut pipe, &mut bytes);
        bytes
    })
}

pub fn run_worker(
    homes: &IsolatedHomes,
    args: &[&str],
    attach_fake_ssh: bool,
    cwd: Option<&Path>,
    stdin: Option<&[u8]>,
) -> ChildOutput {
    let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
    apply_private_env(&mut command, homes, attach_fake_ssh);
    command.args(args);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = OwnedChild::spawn(&mut command);
    {
        let mut child_stdin = child.take_stdin();
        if let Some(bytes) = stdin {
            child_stdin.write_all(bytes).unwrap();
        }
    }
    let stdout = take_pipe_bytes(child.child_mut().stdout.take().expect("stdout"));
    let stderr = take_pipe_bytes(child.child_mut().stderr.take().expect("stderr"));
    let status = match child.wait_timeout(CHILD_WAIT) {
        Some(status) => status,
        None => {
            child.terminate_then_kill();
            let stderr = stderr.join().unwrap_or_default();
            panic!(
                "worker {:?} exceeded {:?}: stderr={}",
                args,
                CHILD_WAIT,
                String::from_utf8_lossy(&stderr)
            );
        }
    };
    ChildOutput {
        status,
        stdout: stdout.join().unwrap(),
        stderr: stderr.join().unwrap(),
    }
}

pub fn host_controller_rpc(homes: &IsolatedHomes, stdin: &[u8]) -> ChildOutput {
    run_worker(homes, &["host", "controller-rpc"], false, None, Some(stdin))
}

pub fn frame_json(value: &Value) -> Vec<u8> {
    encode_frame(&serde_json::to_vec(value).unwrap()).unwrap()
}

pub fn frozen_submit_body(prompt: &str) -> Value {
    json!({
        "task_id": TASK_ID,
        "turn_id": TURN_ID,
        "created_at_millis": 1_700_000_000_000_u64,
        "prompt": prompt,
        "agent": "codex",
        "source": "local",
        "publish": ["fetch"],
        "close_on": "never",
        "wip": true,
        "project_id": PROJECT_ID,
        "worktree_id": WORKTREE_ID,
        "base_oid": BASE_OID,
        "timeout_millis": 2_700_000_u64,
        "max_followups": 10,
        "permissions": "workspace",
        "requires": [],
        "include_untracked": [],
        "include_empty_dirs": [],
        "allow_sensitive": [],
        "cli_includes": []
    })
}

pub fn frozen_submit_request(request_id: &str, prompt: &str) -> Value {
    json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": request_id,
        "command": "task.submit",
        "body": frozen_submit_body(prompt)
    })
}

pub fn framed_host_error(stdout: &[u8]) -> HostControlError {
    let payload = decode_frame(stdout).unwrap_or_else(|error| {
        panic!("expected one framed HostControlError, decode failed: {error}; stdout={stdout:?}")
    });
    serde_json::from_slice(payload).unwrap_or_else(|error| {
        panic!(
            "expected HostControlError JSON, got {} ({error})",
            String::from_utf8_lossy(payload)
        )
    })
}

pub fn laptop_task_authority(paths: &PathLayout) -> bool {
    let state = &paths.state;
    if !state.exists() {
        return false;
    }
    for name in ["tasks", "queue", "runners", "turns", "runs", "dags"] {
        let path = state.join(name);
        if directory_has_regular_files(&path) {
            return true;
        }
    }
    false
}

fn directory_has_regular_files(root: &Path) -> bool {
    let Ok(entries) = fs::read_dir(root) else {
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

pub fn controller_request_files(paths: &PathLayout) -> Vec<PathBuf> {
    let root = paths.controller_state_root();
    let Ok(entries) = fs::read_dir(&root) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .is_some_and(|name| name.starts_with("req-") && name.ends_with(".json"))
        })
        .collect()
}

pub fn ssh_log_entries(homes: &IsolatedHomes) -> Vec<Value> {
    let Ok(text) = fs::read_to_string(&homes.ssh_log) else {
        return Vec::new();
    };
    text.lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).expect("fake ssh log line"))
        .collect()
}

pub fn ssh_logged_commands(homes: &IsolatedHomes) -> Vec<String> {
    ssh_log_entries(homes)
        .into_iter()
        .filter_map(|entry| {
            entry
                .get("command")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .filter(|command| !command.is_empty())
        .collect()
}

pub fn ssh_saw_controller_rpc(homes: &IsolatedHomes) -> bool {
    ssh_log_entries(homes).iter().any(|entry| {
        entry
            .get("remote")
            .and_then(Value::as_str)
            .is_some_and(|remote| remote.contains("host controller-rpc"))
            || entry
                .get("argv")
                .and_then(Value::as_array)
                .is_some_and(|argv| {
                    argv.iter().any(|arg| {
                        arg.as_str()
                            .is_some_and(|arg| arg.contains("host controller-rpc"))
                    })
                })
    })
}

pub fn init_git_project() -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    git(directory.path(), &["init", "--initial-branch=main"]);
    git(directory.path(), &["config", "user.name", "fail-closed"]);
    git(
        directory.path(),
        &["config", "user.email", "fail-closed@example.test"],
    );
    fs::write(directory.path().join("README"), b"fail-closed\n").unwrap();
    git(directory.path(), &["add", "--all"]);
    git(directory.path(), &["commit", "-m", "init"]);
    directory
}

fn git(cwd: &Path, args: &[&str]) {
    let home = cwd.join("home");
    let _ = fs::create_dir_all(&home);
    let output = Command::new("/usr/bin/git")
        .args(args)
        .current_dir(cwd)
        .env("HOME", home)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

pub fn oversize_length_prefix() -> Vec<u8> {
    ((MAX_FRAME_BYTES as u32) + 1).to_be_bytes().to_vec()
}
