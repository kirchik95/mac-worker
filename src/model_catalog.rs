//! Account-level Codex and OpenCode catalogs, discovered without a project.

use std::{
    fs,
    os::unix::fs::DirBuilderExt,
    path::{Path, PathBuf},
    time::Duration,
};

use serde_json::Value;

use crate::process::{ProcessPolicy, ProcessRunner};

fn policy(stdout_limit: usize) -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit,
        stderr_limit: 64 * 1024,
        deadline: Duration::from_secs(15),
    }
}

pub(crate) fn discover_codex(home: &Path, runner: &dyn ProcessRunner) -> Option<Value> {
    let output = discover(
        home,
        &["codex", "debug", "models"],
        runner,
        policy(4 * 1024 * 1024),
    )?;
    serde_json::from_slice(&output).ok()
}

pub(crate) fn discover_opencode(home: &Path, runner: &dyn ProcessRunner) -> Option<String> {
    let output = discover(home, &["opencode", "models"], runner, policy(256 * 1024))?;
    String::from_utf8(output).ok()
}

fn discover(
    home: &Path,
    argv: &[&str],
    runner: &dyn ProcessRunner,
    policy: ProcessPolicy,
) -> Option<Vec<u8>> {
    let scratch = DiscoveryDirectory::create()?;
    let argv = argv
        .iter()
        .map(|argument| (*argument).to_owned())
        .collect::<Vec<_>>();
    // These adapters authenticate from their account files, without env profiles.
    let mut request = crate::agent::prebind_login_request(&argv, home, &[]).ok()?;
    request
        .environment
        .retain(|(key, _)| !crate::keychain::is_reserved_env_name(&key.to_string_lossy()));
    // Startup scripts can set reserved values and change directories. Apply both
    // protections after the login shell has finished its account setup.
    let cwd = scratch.0.to_str()?.replace('\'', "'\\''");
    request.args[1] = format!(
        "unset MAC_WORKER_KEYCHAIN_PASSWORD MAC_WORKER_KEYCHAIN_PATH; cd -- '{cwd}' || exit 1; {}",
        request.args.get(1)?.to_str()?
    )
    .into();
    request.policy = policy;
    let result = runner.run(&request).ok()?;
    if !result.status.success()
        || result.stdout.is_empty()
        || result.stdout.len() > policy.stdout_limit
        || result.stderr.len() > policy.stderr_limit
    {
        return None;
    }
    Some(result.stdout)
}

struct DiscoveryDirectory(PathBuf);

impl DiscoveryDirectory {
    fn create() -> Option<Self> {
        let path =
            std::env::temp_dir().join(format!("mac-worker-model-catalog-{}", uuid::Uuid::new_v4()));
        fs::DirBuilder::new().mode(0o700).create(&path).ok()?;
        Some(Self(path))
    }
}

impl Drop for DiscoveryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{agent_settings::NativeAgentSettingsStore, process::SystemProcessRunner};
    use std::sync::Mutex;
    use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf, time::Instant};
    use tempfile::{TempDir, tempdir};

    static DISCOVERY_LOCK: Mutex<()> = Mutex::new(());

    const CODEX: &[&str] = &["codex", "debug", "models"];
    const OPENCODE: &[&str] = &["opencode", "models"];
    const CODEX_OUTPUT: &str = r#"{"models":[{"slug":"live-model","visibility":"list"}]}"#;
    const OPENCODE_OUTPUT: &str = "provider/live-model";

    fn fixture(argv: &[&str], script: &str) -> TempDir {
        let home = tempdir().unwrap();
        fs::write(home.path().join(".zshenv"), "unsetopt GLOBAL_RCS\n").unwrap();
        let bin = home.path().join("bin");
        fs::create_dir(&bin).unwrap();
        // Every agent name is owned by the fixture, even if a test accidentally
        // asks for a different catalog. Login startup cannot find a real CLI.
        for binary in ["codex", "opencode", "cursor-agent"] {
            let path = bin.join(binary);
            fs::write(&path, "#!/bin/sh\nexit 99\n").unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        fs::write(
            home.path().join(".zprofile"),
            concat!(
                "export PATH=\"$HOME/bin:/usr/bin:/bin\"\n",
                "export MAC_WORKER_KEYCHAIN_PASSWORD=fixture-secret\n",
                "export MAC_WORKER_KEYCHAIN_PATH=fixture-keychain\n",
                "export LOGIN_FIXTURE=present\n",
                // Even a login script that moves into a project must be overridden.
                "cd \"$HOME/project\"\n",
            ),
        )
        .unwrap();
        fs::create_dir_all(home.path().join("project/.codex")).unwrap();
        fs::write(home.path().join("project/opencode.json"), "project-only").unwrap();
        let binary = bin.join(argv[0]);
        fs::write(binary, format!("#!/bin/sh\n{script}\n")).unwrap();
        home
    }

    fn short_policy() -> ProcessPolicy {
        ProcessPolicy {
            stdout_limit: 2048,
            stderr_limit: 1024,
            deadline: Duration::from_secs(2),
        }
    }

    fn assert_reaped(home: &TempDir) {
        let pid: i32 = fs::read_to_string(home.path().join("leader.pid"))
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            -1,
            "catalog leader {pid} survived"
        );
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
        let mut status = 0;
        assert_eq!(
            unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) },
            -1
        );
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ECHILD)
        );
    }

    #[test]
    fn discovery_uses_login_path_account_home_empty_cwd_and_no_keychain_variables() {
        let _serial = DISCOVERY_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for (argv, output) in [(CODEX, CODEX_OUTPUT), (OPENCODE, OPENCODE_OUTPUT)] {
            let home = fixture(
                argv,
                &format!(
                    r#"
[ "$LOGIN_FIXTURE" = present ] || exit 10
[ -z "${{MAC_WORKER_KEYCHAIN_PASSWORD-}}" ] || exit 11
[ -z "${{MAC_WORKER_KEYCHAIN_PATH-}}" ] || exit 12
[ ! -e opencode.json ] && [ ! -e .codex ] || exit 13
[ -z "$(ls -A)" ] || exit 14
printf '%s' "$PWD" > "$HOME/scratch-path"
printf '%s' "$*" > "$HOME/arguments"
printf '%s' $$ > "$HOME/leader.pid"
printf '%s\n' '{output}'
"#
                ),
            );
            let result = discover(home.path(), argv, &SystemProcessRunner, short_policy()).unwrap();
            assert_eq!(String::from_utf8(result).unwrap().trim_end(), output);
            assert_eq!(
                fs::read_to_string(home.path().join("arguments")).unwrap(),
                argv[1..].join(" ")
            );
            let scratch =
                PathBuf::from(fs::read_to_string(home.path().join("scratch-path")).unwrap());
            assert!(!scratch.exists(), "scratch cwd survived cleanup");
            assert_eq!(
                fs::read_to_string(home.path().join("project/opencode.json")).unwrap(),
                "project-only"
            );
            assert_reaped(&home);
        }
    }

    #[test]
    fn discovery_discards_stdout_on_nonzero_exit() {
        let _serial = DISCOVERY_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for argv in [CODEX, OPENCODE] {
            let home = fixture(
                argv,
                "printf '%s' $$ > \"$HOME/leader.pid\"\nprintf '%s\\n' 'provider/ignored'\nexit 17",
            );
            assert!(discover(home.path(), argv, &SystemProcessRunner, short_policy()).is_none());
            assert_reaped(&home);
        }
    }

    #[test]
    fn discovery_bounds_both_output_streams_and_reaps_the_process() {
        let _serial = DISCOVERY_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for argv in [CODEX, OPENCODE] {
            for redirect in ["", " >&2"] {
                let home = fixture(
                    argv,
                    &format!(
                        "printf '%s' $$ > \"$HOME/leader.pid\"\nhead -c 8192 /dev/zero{redirect}\nexec sleep 60"
                    ),
                );
                let started = Instant::now();
                // A long deadline, so returning early proves the overflow killed the group.
                // `exec` keeps the fixture from forking while the group is killed; a
                // child forked during killpg could miss the signal and hold the pipes.
                assert!(
                    discover(
                        home.path(),
                        argv,
                        &SystemProcessRunner,
                        ProcessPolicy {
                            deadline: Duration::from_secs(30),
                            ..short_policy()
                        }
                    )
                    .is_none()
                );
                assert!(started.elapsed() < Duration::from_secs(20));
                assert_reaped(&home);
            }
        }
    }

    #[test]
    fn deadline_returns_promptly_and_kills_descendants_holding_pipes() {
        let _serial = DISCOVERY_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for argv in [CODEX, OPENCODE] {
            let home = fixture(
                argv,
                "printf '%s' $$ > \"$HOME/leader.pid\"\n(sleep 2; printf leaked > \"$HOME/descendant-ran\") &\nwait",
            );
            let started = Instant::now();
            assert!(
                discover(
                    home.path(),
                    argv,
                    &SystemProcessRunner,
                    ProcessPolicy {
                        deadline: Duration::from_secs(1),
                        ..short_policy()
                    }
                )
                .is_none()
            );
            // Upper bounds only rule out waiting for the script; the descendant
            // check below proves the group died at the deadline.
            assert!(started.elapsed() < Duration::from_secs(10));
            assert_reaped(&home);
            std::thread::sleep(Duration::from_millis(2200));
            assert!(
                !home.path().join("descendant-ran").exists(),
                "process-group descendant survived"
            );
        }
    }

    #[test]
    fn public_discoveries_parse_fixture_output_and_empty_or_malformed_results_fall_back() {
        let _serial = DISCOVERY_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let home = fixture(CODEX, &format!("printf '%s\\n' '{CODEX_OUTPUT}'"));
        assert_eq!(
            discover_codex(home.path(), &SystemProcessRunner).unwrap()["models"][0]["slug"],
            "live-model"
        );
        let home = fixture(OPENCODE, &format!("printf '%s\\n' '{OPENCODE_OUTPUT}'"));
        assert_eq!(
            discover_opencode(home.path(), &SystemProcessRunner)
                .unwrap()
                .trim_end(),
            OPENCODE_OUTPUT
        );
        for script in ["printf not-json", "printf ''", "printf '\\377'"] {
            let home = fixture(CODEX, script);
            assert!(discover_codex(home.path(), &SystemProcessRunner).is_none());
            let home = fixture(OPENCODE, script);
            let settings = NativeAgentSettingsStore::new(home.path())
                .with_opencode_catalog(discover_opencode(home.path(), &SystemProcessRunner))
                .read("opencode")
                .unwrap();
            assert_eq!(settings.model_catalog_source.as_deref(), Some("remembered"));
        }
    }
}
