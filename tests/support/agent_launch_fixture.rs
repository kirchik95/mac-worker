#![allow(dead_code)]

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
};

use mac_worker::{
    agent::{prebind_login_request, render_prebind_shell},
    agent_facts::{AgentAuth, AgentFacts, AgentProbe},
    host_store::HostStore,
    probe::ProbeCollector,
    process::{ProcessRunner, SystemProcessRunner},
};

pub const PROFILE_GOOD: &str = "profile-good";
pub const LOGIN_BAD: &str = "login-bad";
pub const LOGIN_GOOD: &str = "login-good";
pub const PARENT_ONLY: &str = "parent-only";
pub const PROFILE_NAME: &str = "fixture";

/// Restrict login-shell PATH to synthetic fixture directories only.
pub fn fixture_only_path(dir: &Path) -> String {
    dir.display().to_string()
}

pub fn empty_base_path() -> &'static str {
    "/usr/bin:/bin"
}

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

    /// Hermetic login startup: keep profile-supplied PATH ahead of login-bin, otherwise
    /// expose only the synthetic login-bin directory so installed providers stay ineligible.
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

pub fn is_agent_launch_subtest() -> bool {
    std::env::var("AGENT_LAUNCH_SUBTEST").ok().as_deref() == Some("1")
}

pub fn skip_unless_subtest() -> bool {
    !is_agent_launch_subtest()
}

pub fn fixture_home_from_env() -> PathBuf {
    PathBuf::from(std::env::var("FIXTURE_HOME").expect("FIXTURE_HOME must be set in subtest"))
}

pub fn refresh_cursor_facts(runner: &dyn ProcessRunner, home: &Path) -> AgentFacts {
    let _guard = shell_fixture_lock();
    let host_root = home.join("host-state");
    HostStore::open(&host_root).unwrap();
    ProbeCollector::refresh_facts_at(&host_root, home, runner).unwrap()
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

pub fn cursor_auth_for_profile(
    runner: &dyn ProcessRunner,
    home: &Path,
    profile_name: &str,
) -> AgentAuth {
    cursor_probe(&refresh_cursor_facts(runner, home))
        .and_then(|agent| {
            agent
                .auth_by_profile
                .iter()
                .find(|(name, _)| name == profile_name)
                .map(|(_, auth)| *auth)
        })
        .unwrap_or(AgentAuth::Unknown)
}

pub fn prebind_status_auth(
    home: &Path,
    profile_entries: &[(std::ffi::OsString, std::ffi::OsString)],
) -> AgentAuth {
    let _guard = shell_fixture_lock();
    let shell = render_prebind_shell(&["cursor-agent".into(), "status".into()]).unwrap();
    let request = prebind_login_request(
        &["cursor-agent".into(), "status".into()],
        home,
        profile_entries,
    )
    .unwrap();
    assert_eq!(request.args.last().unwrap().to_str().unwrap(), shell);
    let result = SystemProcessRunner.run(&request).unwrap();
    classify_cursor_stdout(&result.stdout)
}

pub fn classify_cursor_stdout(stdout: &[u8]) -> AgentAuth {
    use mac_worker::agent::{AgentKind, AuthProbeResult, adapter_for};
    let probe = adapter_for(AgentKind::Cursor).auth_probe();
    match probe.classify(&mac_worker::process::ProcessResult {
        status: exit_status(0),
        stdout: stdout.to_vec(),
        stderr: Vec::new(),
    }) {
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
    let (status, stdout, stderr) = run_subprocess_test(test_name, env, env_clear);
    assert!(
        status.success(),
        "subprocess `{test_name}` failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

pub fn write_profile_entries(body: &str) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
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

fn exit_status(code: i32) -> ExitStatus {
    use std::os::unix::process::ExitStatusExt;
    ExitStatus::from_raw(code << 8)
}

fn shell_fixture_lock() -> std::sync::MutexGuard<'static, ()> {
    use std::sync::{Mutex, OnceLock};
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
