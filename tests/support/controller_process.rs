//! Isolated laptop/controller process fixture.
//!
//! Loaded from `tests/controller_process_runtime.rs` and
//! `tests/controller_batch_process_runtime.rs` via `#[path]`. Not registered in
//! `tests/support/mod.rs`. Fake SSH is labeled and never a live network host.
//! Child env/cwd are per-Command; the test process HOME/cwd are not mutated.

#![allow(dead_code)]

use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Output, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use mac_worker::transfer_repo::TransferRepo;

pub const FAKE_CONTROLLER_DEST: &str = "fakecontroller";
pub const FAKE_EXEC_DEST: &str = "fakeexec";
pub const REMOTE_BINARY: &str = "~/.local/bin/worker";
/// Debug/test-gated child env. Not a public config setting. FLOW reads
/// `std::env` in debug builds and POSIX-quotes this absolute path for Git/rsync.
pub const TEST_SSH_ENV: &str = "MAC_WORKER_TEST_SSH";
pub const LEADER_READY: &str = "controller leader acquired";
pub const LEADER_READY_TIMEOUT: Duration = Duration::from_secs(15);
pub const CHILD_EXIT_TIMEOUT: Duration = Duration::from_secs(15);
pub const TERM_WAIT: Duration = Duration::from_secs(2);

const GIT_ENVIRONMENT_REMOVALS: &[&str] = &[
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_COMMON_DIR",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
];

pub struct OwnedChild {
    child: Option<Child>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl OwnedChild {
    pub fn spawn(command: &mut Command) -> Self {
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        Self {
            child: Some(command.spawn().expect("spawn owned child")),
            stdout: Vec::new(),
            stderr: Vec::new(),
        }
    }

    pub fn id(&self) -> u32 {
        self.child.as_ref().expect("child already reaped").id()
    }

    pub fn take_stdout(&mut self) -> std::process::ChildStdout {
        self.child
            .as_mut()
            .expect("child already reaped")
            .stdout
            .take()
            .expect("stdout pipe")
    }

    pub fn take_stderr(&mut self) -> std::process::ChildStderr {
        self.child
            .as_mut()
            .expect("child already reaped")
            .stderr
            .take()
            .expect("stderr pipe")
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

    pub fn kill_and_reap(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Capture stderr and the child exit after a failed ready-line wait.
    /// Does not extend `LEADER_READY_TIMEOUT`; the short waits only drain
    /// pipes after stdout already EOF'd or the ready wait elapsed.
    fn fail_leader_startup(&mut self, why: &str) -> ! {
        let mut stderr_pipe = self.take_stderr();
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = stderr_pipe.read_to_end(&mut buf);
            let _ = tx.send(buf);
        });
        let exit = self.wait_timeout(Duration::from_millis(20));
        let stderr = rx
            .recv_timeout(Duration::from_millis(50))
            .unwrap_or_default();
        let stderr = String::from_utf8_lossy(&stderr);
        panic!("{why}; exit={exit:?}; stderr={stderr}");
    }

    pub fn terminate_and_reap(&mut self) {
        if let Some(mut child) = self.child.take() {
            let pid = child.id() as i32;
            unsafe {
                libc::kill(pid, libc::SIGTERM);
            }
            let deadline = Instant::now() + TERM_WAIT;
            loop {
                if child.try_wait().unwrap().is_some() {
                    return;
                }
                if Instant::now() > deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return;
                }
                thread::sleep(Duration::from_millis(20));
            }
        }
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        self.kill_and_reap();
    }
}

pub struct ProcessFixture {
    _temp: tempfile::TempDir,
    pub laptop_home: PathBuf,
    pub controller_home: PathBuf,
    pub laptop_xdg_config: PathBuf,
    pub laptop_xdg_state: PathBuf,
    pub laptop_xdg_cache: PathBuf,
    pub laptop_xdg_data: PathBuf,
    pub controller_xdg_config: PathBuf,
    pub controller_xdg_state: PathBuf,
    pub controller_xdg_cache: PathBuf,
    pub controller_xdg_data: PathBuf,
    pub fake_ssh: PathBuf,
    pub exec_journal: PathBuf,
    pub exec_state: PathBuf,
    pub exec_git: PathBuf,
    pub fake_exec_py: PathBuf,
    pub worker_bin: PathBuf,
}

impl ProcessFixture {
    pub fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let laptop_home = root.join("laptop/home");
        let controller_home = root.join("controller/home");
        let laptop_xdg_config = root.join("laptop/xdg-config");
        let laptop_xdg_state = root.join("laptop/xdg-state");
        let laptop_xdg_cache = root.join("laptop/xdg-cache");
        let laptop_xdg_data = root.join("laptop/xdg-data");
        let controller_xdg_config = root.join("controller/xdg-config");
        let controller_xdg_state = root.join("controller/xdg-state");
        let controller_xdg_cache = root.join("controller/xdg-cache");
        let controller_xdg_data = root.join("controller/xdg-data");
        for dir in [
            &laptop_home,
            &controller_home,
            &laptop_xdg_config,
            &laptop_xdg_state,
            &laptop_xdg_cache,
            &laptop_xdg_data,
            &controller_xdg_config,
            &controller_xdg_state,
            &controller_xdg_cache,
            &controller_xdg_data,
        ] {
            fs::create_dir_all(dir).unwrap();
        }

        let worker_bin = PathBuf::from(env!("CARGO_BIN_EXE_worker"));
        for home in [&laptop_home, &controller_home] {
            fs::create_dir_all(home.join(".local/bin")).unwrap();
            let link = home.join(".local/bin/worker");
            let _ = fs::remove_file(&link);
            std::os::unix::fs::symlink(&worker_bin, &link).unwrap();
        }

        let exec_journal = root.join("fake-exec-journal.jsonl");
        fs::write(&exec_journal, "").unwrap();
        let exec_state = root.join("fake-exec-state.json");
        let exec_git = root.join("fake-exec.git");
        init_bare_git(&exec_git);

        let fake_exec_py = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/support/controller_process_fake_exec.py");
        assert!(
            fake_exec_py.is_file(),
            "tests-only fake exec must exist at {}",
            fake_exec_py.display()
        );

        // Spaces + quote so FLOW's POSIX-quoted GIT_SSH_COMMAND/rsync -e is exercised.
        let ssh_dir = crate::support::create_directory(root.join("ssh bin"));
        let fake_ssh = ssh_dir.join("fake'ssh hop");
        write_fake_ssh(
            &fake_ssh,
            &controller_home,
            &controller_xdg_config,
            &controller_xdg_state,
            &controller_xdg_cache,
            &controller_xdg_data,
            &worker_bin,
            &exec_journal,
            &exec_state,
            &exec_git,
            &fake_exec_py,
        );

        let fixture = Self {
            _temp: temp,
            laptop_home,
            controller_home,
            laptop_xdg_config,
            laptop_xdg_state,
            laptop_xdg_cache,
            laptop_xdg_data,
            controller_xdg_config,
            controller_xdg_state,
            controller_xdg_cache,
            controller_xdg_data,
            fake_ssh,
            exec_journal,
            exec_state,
            exec_git,
            fake_exec_py,
            worker_bin,
        };
        fixture.write_laptop_config();
        fixture.write_controller_config();
        fixture
    }

    fn write_laptop_config(&self) {
        let dir = self.laptop_xdg_config.join("mac-worker");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("config.toml"),
            format!(
                "version = 1\n[controller]\nenabled = true\nssh = {dest:?}\nremote_binary = {bin:?}\n",
                dest = FAKE_CONTROLLER_DEST,
                bin = REMOTE_BINARY,
            ),
        )
        .unwrap();
    }

    fn write_controller_config(&self) {
        self.set_controller_slots(1);
    }

    /// Controller inventory slots used when the laptop omits `--max-parallel`.
    /// Laptop `workers[]` stays empty; do not resolve omitted parallelism there.
    pub fn set_controller_slots(&self, slots: u8) {
        assert!(
            slots >= 1,
            "controller inventory must resolve max_parallel >= 1"
        );
        let dir = self.controller_xdg_config.join("mac-worker");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("config.toml"),
            format!(
                "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = {dest:?}\nslots = {slots}\nremote_binary = {bin:?}\n",
                dest = FAKE_EXEC_DEST,
                bin = REMOTE_BINARY,
            ),
        )
        .unwrap();
    }

    pub fn laptop_state(&self) -> PathBuf {
        self.laptop_xdg_state.join("mac-worker")
    }

    pub fn controller_state(&self) -> PathBuf {
        self.controller_xdg_state.join("mac-worker")
    }

    pub fn controller_request_root(&self) -> PathBuf {
        self.controller_xdg_state.join("mac-worker-controller")
    }

    pub fn laptop_controller_cache(&self) -> PathBuf {
        self.laptop_xdg_cache.join("mac-worker/controller")
    }

    pub fn fake_ssh_is_labeled(&self) -> bool {
        fs::read_to_string(&self.fake_ssh)
            .unwrap()
            .contains("# fake SSH hop:")
    }

    pub fn apply_laptop_env(&self, command: &mut Command) {
        command
            .env("HOME", &self.laptop_home)
            .env("XDG_CONFIG_HOME", &self.laptop_xdg_config)
            .env("XDG_STATE_HOME", &self.laptop_xdg_state)
            .env("XDG_CACHE_HOME", &self.laptop_xdg_cache)
            .env("XDG_DATA_HOME", &self.laptop_xdg_data)
            .env(TEST_SSH_ENV, &self.fake_ssh)
            .env_remove("MAC_WORKER_SSH")
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_COMMON_DIR");
    }

    pub fn apply_controller_env(&self, command: &mut Command) {
        command
            .env("HOME", &self.controller_home)
            .env("XDG_CONFIG_HOME", &self.controller_xdg_config)
            .env("XDG_STATE_HOME", &self.controller_xdg_state)
            .env("XDG_CACHE_HOME", &self.controller_xdg_cache)
            .env("XDG_DATA_HOME", &self.controller_xdg_data)
            .env(TEST_SSH_ENV, &self.fake_ssh)
            .env_remove("MAC_WORKER_SSH")
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_COMMON_DIR");
    }

    pub fn laptop_worker(&self) -> Command {
        let mut command = Command::new(&self.worker_bin);
        self.apply_laptop_env(&mut command);
        command
    }

    pub fn spawn_controller_run(&self) -> OwnedChild {
        let mut command = Command::new(&self.worker_bin);
        self.apply_controller_env(&mut command);
        command.arg("controller").arg("run");
        OwnedChild::spawn(&mut command)
    }

    pub fn wait_until_leader_ready(&self, child: &mut OwnedChild) -> String {
        let stdout = child.take_stdout();
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let mut line = String::new();
            let _ = BufReader::new(stdout).read_line(&mut line);
            let _ = tx.send(line);
        });
        match rx.recv_timeout(LEADER_READY_TIMEOUT) {
            Ok(line) if line.contains(LEADER_READY) => line,
            Ok(line) => child.fail_leader_startup(&format!(
                "unexpected controller stdout: {line:?}"
            )),
            Err(_) => child.fail_leader_startup("controller run should print a readiness line"),
        }
    }

    pub fn exec_journal(&self) -> String {
        fs::read_to_string(&self.exec_journal).unwrap()
    }

    pub fn journal_opcode_count(&self, opcode: &str) -> usize {
        self.exec_journal()
            .lines()
            .filter(|line| line.contains(opcode))
            .count()
    }

    pub fn journal_objects(&self) -> Vec<serde_json::Value> {
        self.exec_journal()
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str(line).unwrap_or_else(|error| {
                    panic!("fakeexec journal must be JSONL ({error}): {line}")
                })
            })
            .collect()
    }

    pub fn journal_task_turns(&self) -> Vec<serde_json::Value> {
        self.journal_objects()
            .into_iter()
            .filter(|row| {
                row.get("opcode").and_then(|value| value.as_str()) == Some("host task-turn")
            })
            .collect()
    }

    pub fn run_laptop(&self, args: &[&str], cwd: Option<&Path>) -> (ExitStatus, Vec<u8>, Vec<u8>) {
        self.run_laptop_with_test_ssh(&self.fake_ssh, args, cwd)
    }

    /// Public `task wait --task-id`. Status Done is not quiescence: the
    /// runner still fetches/imports after persist_status(terminal).
    pub fn wait_for_task_quiescence(
        &self,
        cwd: Option<&Path>,
        task_id: &str,
    ) -> (ExitStatus, Vec<u8>, Vec<u8>) {
        self.run_laptop(
            &[
                "--json",
                "task",
                "wait",
                "--task-id",
                task_id,
                "--timeout",
                "30s",
            ],
            cwd,
        )
    }

    /// Child-private TEST_SSH override. Used to inject a refused
    /// fakecontroller hop without changing production leader/RPC guards.
    pub fn run_laptop_with_test_ssh(
        &self,
        test_ssh: &Path,
        args: &[&str],
        cwd: Option<&Path>,
    ) -> (ExitStatus, Vec<u8>, Vec<u8>) {
        let mut command = self.laptop_worker();
        command.env(TEST_SSH_ENV, test_ssh);
        command.args(args);
        if let Some(cwd) = cwd {
            command.current_dir(cwd);
        }
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let output = command.output().expect("laptop worker");
        (output.status, output.stdout, output.stderr)
    }

    /// Per-child fake SSH that refuses dest `fakecontroller`. Not a live
    /// network hop. Omitting `controller run` is not a transport outage.
    pub fn controller_outage_ssh(&self) -> PathBuf {
        let path = self
            .fake_ssh
            .parent()
            .expect("fake ssh directory")
            .join("refuse'controller hop");
        if !path.is_file() {
            write_controller_outage_ssh(&path, &self.exec_journal);
        }
        path
    }

    /// Real child RPC hop: fake SSH dest `fakecontroller` execs the worker
    /// `host controller-rpc` with controller XDG. One frame, then stdin EOF.
    pub fn run_controller_rpc(&self, frame: &[u8]) -> (ExitStatus, Vec<u8>, Vec<u8>) {
        let mut command = Command::new(&self.fake_ssh);
        self.apply_laptop_env(&mut command);
        command
            .args([
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=5",
                "-o",
                "ForwardAgent=no",
                "-o",
                "ClearAllForwardings=yes",
                "--",
                FAKE_CONTROLLER_DEST,
                "~/.local/bin/worker host controller-rpc",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().expect("spawn controller-rpc via fake SSH");
        {
            let mut stdin = child.stdin.take().expect("rpc stdin");
            stdin.write_all(frame).expect("write rpc frame");
        }
        let output = child.wait_with_output().expect("rpc child");
        (output.status, output.stdout, output.stderr)
    }

    /// Direct fakeexec JSON hop. Stdin is one protocol request object.
    /// Git upload-pack is not this path (stdin is the pack protocol).
    pub fn run_fakeexec(&self, opcode: &str, stdin: &[u8]) -> (ExitStatus, Vec<u8>, Vec<u8>) {
        let mut command = Command::new(&self.fake_ssh);
        command
            .args([
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=5",
                "--",
                FAKE_EXEC_DEST,
                &format!("{REMOTE_BINARY} {opcode}"),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().expect("spawn fakeexec via fake SSH");
        {
            let mut stdin_pipe = child.stdin.take().expect("fakeexec stdin");
            stdin_pipe.write_all(stdin).expect("write fakeexec stdin");
        }
        let output = child.wait_with_output().expect("fakeexec child");
        (output.status, output.stdout, output.stderr)
    }

    pub fn git_fetch_result(
        &self,
        dest: &Path,
        task_id: &str,
        client_id: &str,
        project_id: &str,
    ) -> std::process::Output {
        let home = dest.join("home");
        fs::create_dir_all(&home).ok();
        let mut command = Command::new("/usr/bin/git");
        command
            .current_dir(dest)
            .env("HOME", &home)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_SSH_COMMAND", posix_quote(&self.fake_ssh))
            .env_remove("GIT_SSH");
        for name in GIT_ENVIRONMENT_REMOVALS {
            command.env_remove(name);
        }
        let upload =
            format!("--upload-pack={REMOTE_BINARY} host upload-pack {task_id} {client_id}");
        let remote = format!("{FAKE_EXEC_DEST}:{project_id}");
        let refspec = format!("+refs/heads/task/{task_id}:refs/mac-worker/results/{task_id}");
        let output = command
            .args(["fetch", "--no-write-fetch-head", &upload, &remote, &refspec])
            .output()
            .expect("git fetch result via fakeexec");
        assert!(
            output.status.success(),
            "git fetch result failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    pub fn envelope_paths(&self) -> Vec<PathBuf> {
        let cache = self.laptop_controller_cache();
        let mut paths = Vec::new();
        let Ok(entries) = fs::read_dir(&cache) else {
            return paths;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("op-") && name.ends_with(".json") {
                paths.push(entry.path());
            }
        }
        paths.sort();
        paths
    }

    /// Durable controller import: `LocalTaskRecord.fetched_head` plus the
    /// checkout remotes ref written by `TransferRepo::import_result`.
    /// `res-*.json` is a laptop export token from `result.prepare`, not import.
    pub fn controller_imported_oids(&self) -> Vec<String> {
        let tasks_dir = self.controller_state().join("tasks");
        let mut oids = Vec::new();
        let Ok(entries) = fs::read_dir(&tasks_dir) else {
            return oids;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let Ok(bytes) = fs::read(&path) else {
                continue;
            };
            let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
                continue;
            };
            let Some(oid) = value.get("fetched_head").and_then(|head| head.as_str()) else {
                continue;
            };
            if oid.len() != 40 {
                continue;
            }
            if self.controller_git_owns_imported_oid(&value, oid) {
                oids.push(oid.to_owned());
            }
        }
        oids.sort();
        oids.dedup();
        oids
    }

    fn controller_git_owns_imported_oid(&self, record: &serde_json::Value, oid: &str) -> bool {
        let Some(meta) = record.get("meta") else {
            return false;
        };
        let Some(task_id) = meta.get("task_id").and_then(|value| value.as_str()) else {
            return false;
        };
        let Some(project_id) = meta.get("project_id").and_then(|value| value.as_str()) else {
            return false;
        };
        let Some(worktree_id) = meta.get("worktree_id").and_then(|value| value.as_str()) else {
            return false;
        };
        let worker = record
            .get("status")
            .and_then(|status| status.get("worker"))
            .and_then(|value| value.as_str())
            .or_else(|| {
                record
                    .get("pinned_worker")
                    .and_then(|value| value.as_str())
            });
        let Some(worker) = worker else {
            return false;
        };
        if let Some(git_dir) = self.controller_checkout_git_dir(project_id, worktree_id) {
            let local_ref = format!("refs/remotes/mac-worker/{worker}/task/{task_id}");
            if git_dir_ref_oid(&git_dir, &local_ref).as_deref() == Some(oid)
                && git_dir_has_commit(&git_dir, oid)
            {
                return true;
            }
        }
        let Ok(transfer) = TransferRepo::controller_transfer_git_path(
            &self.controller_xdg_cache.join("mac-worker"),
            project_id,
            worktree_id,
        ) else {
            return false;
        };
        let result_ref = format!("refs/mac-worker/results/{task_id}");
        git_dir_ref_oid(&transfer, &result_ref).as_deref() == Some(oid)
            && git_dir_has_commit(&transfer, oid)
    }

    fn controller_checkout_git_dir(&self, project_id: &str, worktree_id: &str) -> Option<PathBuf> {
        let map_name = format!("map-{project_id}-{worktree_id}.json");
        let map_path = self.controller_request_root().join(map_name);
        let checkout = if let Ok(bytes) = fs::read(&map_path) {
            let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
            PathBuf::from(value.get("checkout")?.as_str()?)
        } else {
            self.controller_xdg_data
                .join("mac-worker/controller-projects")
                .join(project_id)
                .join(worktree_id)
        };
        git_dir_of(&checkout)
    }
}

fn posix_quote(path: &Path) -> String {
    let text = path.to_str().expect("utf-8 path");
    format!("'{}'", text.replace('\'', r"'\''"))
}

fn git_dir_of(checkout: &Path) -> Option<PathBuf> {
    let git = checkout.join(".git");
    if git.is_dir() {
        return Some(git);
    }
    if !git.is_file() {
        return None;
    }
    let text = fs::read_to_string(&git).ok()?;
    let line = text.lines().find(|line| line.starts_with("gitdir: "))?;
    let pointed = PathBuf::from(line.trim_start_matches("gitdir: ").trim());
    let resolved = if pointed.is_absolute() {
        pointed
    } else {
        checkout.join(pointed)
    };
    resolved.is_dir().then_some(resolved)
}

fn isolated_git(git_dir: &Path, args: &[&str]) -> Output {
    let home = git_dir.parent().unwrap_or(git_dir).join("git-home");
    let _ = fs::create_dir_all(&home);
    let mut command = Command::new("/usr/bin/git");
    command
        .arg("--git-dir")
        .arg(git_dir)
        .env("HOME", &home)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1");
    for name in GIT_ENVIRONMENT_REMOVALS {
        command.env_remove(name);
    }
    command.args(args).output().expect("isolated git")
}

fn git_dir_has_commit(git_dir: &Path, oid: &str) -> bool {
    let output = isolated_git(git_dir, &["cat-file", "-t", oid]);
    output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "commit"
}

fn git_dir_ref_oid(git_dir: &Path, reference: &str) -> Option<String> {
    let output = isolated_git(git_dir, &["rev-parse", "--verify", reference]);
    if !output.status.success() {
        return None;
    }
    let oid = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (oid.len() == 40).then_some(oid)
}

fn init_bare_git(repo: &Path) {
    if let Some(parent) = repo.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    let home = repo.parent().unwrap_or(repo).join("fake-exec-git-home");
    fs::create_dir_all(&home).unwrap();
    let mut command = Command::new("/usr/bin/git");
    command
        .env("HOME", &home)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1");
    for name in GIT_ENVIRONMENT_REMOVALS {
        command.env_remove(name);
    }
    let output = command
        .args(["init", "--bare", repo.to_str().expect("utf-8 git path")])
        .output()
        .expect("git init --bare");
    assert!(
        output.status.success(),
        "git init --bare failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[allow(clippy::too_many_arguments)]
fn write_fake_ssh(
    path: &Path,
    controller_home: &Path,
    config: &Path,
    state: &Path,
    cache: &Path,
    data: &Path,
    worker_bin: &Path,
    journal: &Path,
    exec_state: &Path,
    exec_git: &Path,
    fake_exec_py: &Path,
) {
    let script = format!(
        r#"#!/bin/sh
# fake SSH hop: destination is not a live network host.
# hops: fakecontroller (real worker + controller XDG) and fakeexec (fake agent).
# Accepts either `dest command` or OpenSSH-style `-o … -- dest command`.
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) shift 2 ;;
    --) shift; break ;;
    -*) shift ;;
    *) break ;;
  esac
done
dest="$1"
shift
export HOME={controller_home:?}
export XDG_CONFIG_HOME={config:?}
export XDG_STATE_HOME={state:?}
export XDG_CACHE_HOME={cache:?}
export XDG_DATA_HOME={data:?}
export FAKE_EXEC_JOURNAL={journal:?}
export FAKE_EXEC_STATE={exec_state:?}
export FAKE_EXEC_GIT={exec_git:?}
cmd="$*"
if [ "$dest" = {exec:?} ]; then
  case "$cmd" in
    *"host upload-pack"*)
      printf '%s\n' "{{\"dest\":\"$dest\",\"cmd\":\"$cmd\",\"opcode\":\"host upload-pack\"}}" >> {journal:?}
      exec /usr/bin/git-upload-pack {exec_git:?}
      ;;
    *"host receive-pack"*)
      printf '%s\n' "{{\"dest\":\"$dest\",\"cmd\":\"$cmd\",\"opcode\":\"host receive-pack\"}}" >> {journal:?}
      exec /usr/bin/git-receive-pack {exec_git:?}
      ;;
    *"host "*)
      exec /usr/bin/python3 {python:?} --dest "$dest" --cmd "$cmd"
      ;;
    *)
      echo "fake execution worker: unlabeled host opcode: $cmd" >&2
      exit 2
      ;;
  esac
fi
printf '%s\n' "{{\"dest\":\"$dest\",\"cmd\":\"$cmd\",\"opcode\":\"controller-ssh\"}}" >> {journal:?}
# Isolated transfer fixture: a single remote argv is a shell command. Git
# quote-wraps the project path; `set -- $1` would keep those quotes and fail
# reject_path_arg. Multi-arg form maps the remote binary onto this worker.
if [ "$#" -eq 1 ]; then
  exec /bin/sh -c "$1"
fi
if [ "$1" = "~/.local/bin/worker" ]; then
  shift
  exec {worker:?} "$@"
fi
exec {worker:?} "$@"
"#,
        controller_home = controller_home,
        config = config,
        state = state,
        cache = cache,
        data = data,
        journal = journal,
        exec_state = exec_state,
        exec_git = exec_git,
        python = fake_exec_py,
        exec = FAKE_EXEC_DEST,
        worker = worker_bin,
    );
    fs::write(path, script).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(path, permissions).unwrap();
}

fn write_controller_outage_ssh(path: &Path, journal: &Path) {
    let script = format!(
        r#"#!/bin/sh
# labelled process-fixture SSH outage: refuse dest fakecontroller.
# Not a live network host. Private to the outage runtime child.
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) shift 2 ;;
    --) shift; break ;;
    -*) shift ;;
    *) break ;;
  esac
done
dest="$1"
shift
cmd="$*"
printf '%s\n' "{{\"dest\":\"$dest\",\"cmd\":\"$cmd\",\"opcode\":\"controller-ssh-outage\"}}" >> {journal:?}
echo "CONTROLLER_UNAVAILABLE: labelled fixture refused the hop" >&2
exit 255
"#,
        journal = journal,
    );
    fs::write(path, script).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(path, permissions).unwrap();
}

pub fn capture_stderr(child: &mut OwnedChild, timeout: Duration) -> String {
    let mut pipe = child.take_stderr();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = pipe.read_to_end(&mut buf);
        let _ = tx.send(buf);
    });
    let bytes = rx.recv_timeout(timeout).unwrap_or_default();
    String::from_utf8_lossy(&bytes).into_owned()
}
