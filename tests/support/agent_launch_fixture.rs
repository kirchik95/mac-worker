#![allow(dead_code)]

use std::{
    cell::Cell,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::{Mutex, MutexGuard, OnceLock},
    time::Instant,
};

use mac_worker::{
    agent::prebind_login_request,
    agent_facts::{AgentAuth, AgentFacts, AgentProbe},
    error::WorkerError,
    host_store::HostStore,
    probe::ProbeCollector,
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    supervisor::LaunchPlan,
};

pub const PROFILE_GOOD: &str = "profile-good";
pub const LOGIN_BAD: &str = "login-bad";
pub const LOGIN_GOOD: &str = "login-good";
pub const PARENT_ONLY: &str = "parent-only";
pub const PROFILE_NAME: &str = "fixture";

const PROBE_POLICY: ProcessPolicy = ProcessPolicy {
    stdout_limit: 4 * 1024,
    stderr_limit: 4 * 1024,
    deadline: std::time::Duration::from_secs(2),
};

/// Restrict login-shell PATH to synthetic fixture directories only.
pub fn fixture_only_path(dir: &Path) -> String {
    dir.display().to_string()
}

pub fn system_only_path() -> &'static str {
    "/usr/bin:/bin"
}

pub fn empty_base_path() -> &'static str {
    system_only_path()
}

/// Documented synthetic Git identity for the production `collect_git_identity` path.
/// Production still uses ambient HOME for those probes; the real-shell fixture must not
/// source the developer account's login files when refresh_facts runs through it.
const FIXTURE_GIT_IDENTITY_NAME: &str = "Fixture User";
const FIXTURE_GIT_IDENTITY_EMAIL: &str = "fixture@example.test";

#[derive(Debug, Clone)]
pub struct FixtureLayout {
    pub root: PathBuf,
    pub home: PathBuf,
    pub login_bin: PathBuf,
    pub profile_bin: PathBuf,
}

impl FixtureLayout {
    pub fn create(root: &Path) -> Self {
        let home = root.join("home");
        let login_bin = root.join("login-bin");
        let profile_bin = root.join("profile-bin");
        fs::create_dir_all(home.join("bin")).unwrap();
        fs::create_dir_all(&login_bin).unwrap();
        fs::create_dir_all(&profile_bin).unwrap();
        Self {
            root: root.to_path_buf(),
            home,
            login_bin,
            profile_bin,
        }
    }

    pub fn write_zprofile(&self, body: &str) {
        fs::write(self.home.join(".zprofile"), body).unwrap();
    }

    /// Hermetic login startup: when profile supplies `profile_bin` on PATH, keep it ahead of
    /// `login_bin`; otherwise expose only the synthetic login-bin directory. Appending the
    /// zsh default PATH would expose installed provider CLIs, so this replaces rather than
    /// augments system PATH.
    pub fn write_hermetic_login_zprofile(&self) {
        self.write_zprofile(&format!(
            "PROFILE_BIN=\"{}\"\n\
             LOGIN_BIN=\"{}\"\n\
             case \":$PATH:\" in\n\
               *\":$PROFILE_BIN:\"*)\n\
                 export PATH=\"$PROFILE_BIN:$LOGIN_BIN\"\n\
                 ;;\n\
               *)\n\
                 export PATH=\"$LOGIN_BIN\"\n\
                 ;;\n\
             esac\n",
            self.profile_bin.display(),
            self.login_bin.display(),
        ));
    }

    pub fn write_profile_env(&self, name: &str, body: &str) {
        let dir = self.home.join(".config/mac-worker/env");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{name}.env"));
        fs::write(&path, body).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    }

    pub fn write_cursor_agent(&self, path: &Path, marker: &str) {
        let script = format!(
            "#!/bin/sh\n\
             case \"$1\" in\n\
               --version) printf 'cursor-agent 9.9.9\\n' ;;\n\
               status)\n\
                 if [ \"$CURSOR_API_KEY\" = \"{marker}\" ]; then\n\
                   printf 'Authenticated\\n'\n\
                 else\n\
                   printf 'Not authenticated\\n'\n\
                 fi ;;\n\
               launch-check)\n\
                 command -v cursor-agent >/dev/null && printf 'ok\\n' ;;\n\
               *) exit 2 ;;\n\
             esac\n"
        );
        fs::write(path, script).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    pub fn install_login_cursor(&self, marker: &str) {
        self.write_cursor_agent(&self.login_bin.join("cursor-agent"), marker);
    }

    pub fn install_home_cursor(&self, marker: &str) {
        self.write_cursor_agent(&self.home.join("bin/cursor-agent"), marker);
    }

    pub fn install_profile_cursor(&self, marker: &str) {
        self.write_cursor_agent(&self.profile_bin.join("cursor-agent"), marker);
    }
}

pub struct DiagnosticProcessRunner;

fn is_git_identity_probe(request: &ProcessRequest) -> bool {
    if request.isolate_parent_environment {
        return false;
    }
    let program = request.program.to_string_lossy();
    if program != "zsh" && program != "/bin/zsh" {
        return false;
    }
    let Some(shell) = request.args.last().and_then(|arg| arg.to_str()) else {
        return false;
    };
    request.args.first().map(|arg| arg.as_os_str()) == Some("-lc".as_ref())
        && shell.starts_with("git config --get user.")
}

fn shell_command(request: &ProcessRequest) -> Option<&str> {
    request.args.last().and_then(|arg| arg.to_str())
}

fn is_exec_probe(request: &ProcessRequest) -> bool {
    request.program.to_string_lossy() == "/bin/zsh"
        && shell_command(request).is_some_and(|shell| shell.starts_with("exec "))
}

fn synthetic_git_identity_result(shell: &str) -> ProcessResult {
    let stdout = if shell.ends_with("user.name") {
        format!("{FIXTURE_GIT_IDENTITY_NAME}\n")
    } else if shell.ends_with("user.email") {
        format!("{FIXTURE_GIT_IDENTITY_EMAIL}\n")
    } else {
        panic!("unexpected git identity probe shell command: {shell}");
    };
    ProcessResult {
        status: exit_status(0),
        stdout: stdout.into_bytes(),
        stderr: Vec::new(),
    }
}

fn assert_exec_probe_succeeded(request: &ProcessRequest, result: &ProcessResult, started: Instant) {
    if result.status.success() {
        return;
    }
    panic!(
        "exec probe failed after {:?}: exit={:?} stdout={:?} stderr={:?}\nprogram={:?} args={:?}",
        started.elapsed(),
        result.status.code(),
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr),
        request.program,
        request.args
    );
}

impl ProcessRunner for DiagnosticProcessRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if is_git_identity_probe(request) {
            return Ok(synthetic_git_identity_result(
                request.args.last().unwrap().to_str().unwrap(),
            ));
        }
        let started = Instant::now();
        match SystemProcessRunner.run(request) {
            Ok(result) => {
                if is_exec_probe(request) {
                    assert_exec_probe_succeeded(request, &result, started);
                }
                Ok(result)
            }
            Err(error) => panic!(
                "shell probe failed after {:?}: {error:?}\nprogram={:?} args={:?}",
                started.elapsed(),
                request.program,
                request.args
            ),
        }
    }
}

pub fn is_agent_launch_subtest() -> bool {
    std::env::var("AGENT_LAUNCH_SUBTEST").ok().as_deref() == Some("1")
}

pub fn skip_unless_subtest() -> bool {
    !is_agent_launch_subtest()
}

pub fn fixture_home_from_env() -> PathBuf {
    PathBuf::from(std::env::var("FIXTURE_HOME").expect("FIXTURE_HOME must be set in subtest"))
}

pub fn refresh_cursor_facts(home: &Path) -> AgentFacts {
    let _guard = shell_fixture_lock();
    let host_root = home.join("host-state");
    HostStore::open(&host_root).unwrap();
    ProbeCollector::refresh_facts_at(&host_root, home, &DiagnosticProcessRunner).unwrap()
}

pub fn cursor_probe(facts: &AgentFacts) -> Option<&AgentProbe> {
    facts.agents.iter().find(|agent| agent.name == "cursor")
}

pub fn cursor_profile_auth(facts: &AgentFacts, profile_name: &str) -> AgentAuth {
    cursor_probe(facts)
        .and_then(|agent| {
            agent
                .auth_by_profile
                .iter()
                .find(|(name, _)| name == profile_name)
                .map(|(_, auth)| *auth)
        })
        .unwrap_or(AgentAuth::Unknown)
}

pub fn cursor_auth_for_profile(home: &Path, profile_name: &str) -> AgentAuth {
    cursor_profile_auth(&refresh_cursor_facts(home), profile_name)
}

pub fn prebind_status_auth(
    home: &Path,
    profile_entries: &[(std::ffi::OsString, std::ffi::OsString)],
) -> AgentAuth {
    let _guard = shell_fixture_lock();
    let request = prebind_login_request(
        &["cursor-agent".into(), "status".into()],
        home,
        profile_entries,
    )
    .unwrap();
    let result = DiagnosticProcessRunner.run(&request).unwrap();
    classify_cursor_process_result(&result)
}

pub fn run_launch_plan_cursor_auth(plan: &LaunchPlan) -> AgentAuth {
    let _guard = shell_fixture_lock();
    let mut args = plan.args().to_vec();
    if args.first().map(String::as_str) == Some(plan.program()) {
        args.remove(0);
    }
    let request = ProcessRequest {
        program: plan.program().into(),
        args: args.into_iter().map(Into::into).collect(),
        environment: plan.env().to_vec(),
        environment_remove: Vec::new(),
        stdin: None,
        policy: PROBE_POLICY,
        isolate_parent_environment: true,
    };
    let result = DiagnosticProcessRunner.run(&request).unwrap();
    classify_cursor_process_result(&result)
}

pub fn classify_cursor_process_result(result: &ProcessResult) -> AgentAuth {
    use mac_worker::agent::{AgentKind, AuthProbeResult, adapter_for};
    let probe = adapter_for(AgentKind::Cursor).auth_probe();
    if !result.status.success() {
        panic!(
            "cursor status exited with {:?}; stdout={:?} stderr={:?}",
            result.status.code(),
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
    }
    match probe.classify(result) {
        AuthProbeResult::Authenticated => AgentAuth::Authenticated,
        AuthProbeResult::Unauthenticated => AgentAuth::Unauthenticated,
        AuthProbeResult::Unknown | AuthProbeResult::UnknownWithReason(_) => AgentAuth::Unknown,
    }
}

pub fn run_subprocess_test(
    test_name: &str,
    env: &[(&str, &str)],
    env_clear: bool,
) -> (ExitStatus, String, String) {
    let exe = std::env::current_exe().unwrap();
    let mut command = Command::new(exe);
    command
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if env_clear {
        command.env_clear();
    }
    for (key, value) in env {
        command.env(key, value);
    }
    command.env("AGENT_LAUNCH_SUBTEST", "1");
    let output = command.output().unwrap();
    (
        output.status,
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

pub fn assert_subprocess_success(test_name: &str, env: &[(&str, &str)], env_clear: bool) {
    let _guard = shell_fixture_lock();
    let (status, stdout, stderr) = run_subprocess_test(test_name, env, env_clear);
    assert!(
        status.success(),
        "subprocess `{test_name}` failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

pub fn parse_profile_entries(body: &str) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    body.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (name, value) = line.split_once('=')?;
            Some((name.into(), value.into()))
        })
        .collect()
}

pub fn write_profile_entries(body: &str) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    parse_profile_entries(body)
}

fn exit_status(code: i32) -> ExitStatus {
    use std::os::unix::process::ExitStatusExt;
    ExitStatus::from_raw(code << 8)
}

struct ShellFixtureGuard {
    _inner: Option<MutexGuard<'static, ()>>,
}

impl Drop for ShellFixtureGuard {
    fn drop(&mut self) {
        if self._inner.is_some() {
            FIXTURE_LOCK_HELD.with(|held| held.set(false));
        }
    }
}

thread_local! {
    static FIXTURE_LOCK_HELD: Cell<bool> = const { Cell::new(false) };
}

fn shell_fixture_lock() -> ShellFixtureGuard {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    if FIXTURE_LOCK_HELD.with(|held| held.get()) {
        return ShellFixtureGuard { _inner: None };
    }
    let guard = LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    FIXTURE_LOCK_HELD.with(|held| held.set(true));
    ShellFixtureGuard {
        _inner: Some(guard),
    }
}
