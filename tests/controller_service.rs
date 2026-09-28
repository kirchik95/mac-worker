use std::{
    collections::VecDeque,
    fs,
    os::unix::{
        fs::{MetadataExt, PermissionsExt, symlink},
        process::ExitStatusExt,
    },
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::Mutex,
};

use mac_worker::{
    controller::service::{
        ServiceAction, ServiceStatus, launchdaemon_commands, manage, truncate_log,
    },
    error::WorkerError,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
};

const TARGET: &str = "gui/501/com.mac-worker.controller";
const PLIST: &str = "Library/LaunchAgents/com.mac-worker.controller.plist";
const LOG: &str = "Library/Logs/mac-worker/controller.log";

#[derive(Default)]
struct Launchctl {
    loaded: Mutex<bool>,
    calls: Mutex<Vec<Vec<String>>>,
    failures: Mutex<VecDeque<(&'static str, i32)>>,
}

impl Launchctl {
    fn loaded() -> Self {
        Self {
            loaded: Mutex::new(true),
            ..Self::default()
        }
    }

    fn fail(&self, action: &'static str, code: i32) {
        self.failures.lock().unwrap().push_back((action, code));
    }

    fn calls(&self) -> Vec<Vec<String>> {
        std::mem::take(&mut self.calls.lock().unwrap())
    }
}

impl ProcessRunner for Launchctl {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(request.program, "/bin/launchctl");
        assert!(request.stdin.is_none());
        assert!(request.isolate_parent_environment);
        assert!(request.policy.stdout_limit <= 1024 * 1024);
        assert!(request.policy.stderr_limit <= 64 * 1024);
        assert!(request.policy.deadline.as_secs() <= 30);
        let args: Vec<_> = request
            .args
            .iter()
            .map(|arg| arg.to_str().unwrap().to_owned())
            .collect();
        self.calls.lock().unwrap().push(args.clone());
        let mut failures = self.failures.lock().unwrap();
        if failures
            .front()
            .is_some_and(|(action, _)| *action == args[0])
        {
            let (_, code) = failures.pop_front().unwrap();
            return Ok(ProcessResult {
                status: ExitStatus::from_raw(code << 8),
                stdout: b"token=private-from-launchctl".to_vec(),
                stderr: b"secret=/private/credential".to_vec(),
            });
        }
        let mut loaded = self.loaded.lock().unwrap();
        let code = match args[0].as_str() {
            "print" => {
                assert_eq!(args, ["print", TARGET]);
                if *loaded { 0 } else { 113 }
            }
            "bootstrap" => {
                assert_eq!(args[1], "gui/501");
                assert!(Path::new(&args[2]).is_file());
                *loaded = true;
                0
            }
            "kickstart" => {
                assert_eq!(args, ["kickstart", "-k", TARGET]);
                assert!(*loaded);
                0
            }
            "bootout" => {
                assert_eq!(args, ["bootout", TARGET]);
                *loaded = false;
                0
            }
            other => panic!("unexpected launchctl action {other}"),
        };
        Ok(ProcessResult {
            status: ExitStatus::from_raw(code << 8),
            stdout: b"deliberately opaque launchctl output".to_vec(),
            stderr: Vec::new(),
        })
    }
}

fn home() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("controller & owner");
    fs::create_dir(&home).unwrap();
    (temp, home)
}

fn expected_calls(commands: &[&[&str]]) -> Vec<Vec<String>> {
    commands
        .iter()
        .map(|command| command.iter().map(|part| (*part).to_owned()).collect())
        .collect()
}

#[test]
fn install_bootstraps_supervised_fixed_home_plist_and_is_idempotent() {
    // Losing the supervisor arguments or reloading an unchanged live job breaks this contract.
    let (_temp, home) = home();
    let runner = Launchctl::default();
    let result = manage(&home, 501, &runner, ServiceAction::Install).unwrap();
    assert!(result.installed && result.loaded);
    assert_eq!(result.label, "com.mac-worker.controller");
    assert_eq!(result.domain, "gui/501");
    let path = home.join(PLIST);
    let plist = fs::read_to_string(&path).unwrap();
    let escaped_home = home.to_str().unwrap().replace('&', "&amp;");
    for required in [
        format!("<string>{escaped_home}/.local/bin/worker</string>"),
        "<string>controller</string>".into(),
        "<string>run</string>".into(),
        "<string>--supervised</string>".into(),
        "<key>RunAtLoad</key>\n  <true/>".into(),
        "<key>KeepAlive</key>\n  <true/>".into(),
        "<key>ThrottleInterval</key>\n  <integer>30</integer>".into(),
        format!("<string>{escaped_home}/Library/Logs/mac-worker/controller.log</string>"),
        format!("<key>HOME</key>\n    <string>{escaped_home}</string>"),
    ] {
        assert!(plist.contains(&required), "missing plist value: {required}");
    }
    assert_eq!(
        plist
            .matches("/Library/Logs/mac-worker/controller.log</string>")
            .count(),
        2
    );
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(home.join(LOG)).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let inode = fs::metadata(&path).unwrap().ino();
    assert_eq!(
        runner.calls(),
        expected_calls(&[
            &["print", TARGET],
            &["bootstrap", "gui/501", path.to_str().unwrap()],
            &["kickstart", "-k", TARGET],
            &["print", TARGET],
        ])
    );

    assert_eq!(
        manage(&home, 501, &runner, ServiceAction::Install).unwrap(),
        result
    );
    assert_eq!(fs::metadata(&path).unwrap().ino(), inode);
    assert_eq!(runner.calls(), expected_calls(&[&["print", TARGET]]));
}

#[test]
fn restart_kickstarts_live_service_and_bootstraps_unloaded_service() {
    // Omitting bootstrap strands an installed but unloaded job after a reboot or bootout.
    let (_temp, home) = home();
    let runner = Launchctl::default();
    manage(&home, 501, &runner, ServiceAction::Install).unwrap();
    runner.calls();
    manage(&home, 501, &runner, ServiceAction::Restart).unwrap();
    assert_eq!(
        runner.calls(),
        expected_calls(&[
            &["print", TARGET],
            &["kickstart", "-k", TARGET],
            &["print", TARGET],
        ])
    );
    *runner.loaded.lock().unwrap() = false;
    manage(&home, 501, &runner, ServiceAction::Restart).unwrap();
    assert_eq!(
        runner.calls(),
        expected_calls(&[
            &["print", TARGET],
            &["bootstrap", "gui/501", home.join(PLIST).to_str().unwrap()],
            &["kickstart", "-k", TARGET],
            &["print", TARGET],
        ])
    );
}

#[test]
fn uninstall_unloads_removes_plist_and_preserves_state_logs_and_keys() {
    // Removing only the process leaves the login-triggered plist active; deleting state loses recovery.
    let (_temp, home) = home();
    let runner = Launchctl::default();
    manage(&home, 501, &runner, ServiceAction::Install).unwrap();
    fs::write(home.join("state-and-keys"), b"preserve").unwrap();
    fs::write(home.join(LOG), b"retained diagnostics").unwrap();
    runner.calls();
    let result = manage(&home, 501, &runner, ServiceAction::Uninstall).unwrap();
    assert!(!result.installed && !result.loaded);
    assert!(!home.join(PLIST).exists());
    assert_eq!(fs::read(home.join("state-and-keys")).unwrap(), b"preserve");
    assert_eq!(fs::read(home.join(LOG)).unwrap(), b"retained diagnostics");
    assert_eq!(
        runner.calls(),
        expected_calls(&[&["print", TARGET], &["bootout", TARGET], &["print", TARGET],])
    );
    assert_eq!(
        manage(&home, 501, &runner, ServiceAction::Uninstall).unwrap(),
        result
    );
    assert_eq!(runner.calls(), expected_calls(&[&["print", TARGET]]));
}

#[test]
fn service_preserves_existing_user_library_directory_permissions() {
    // macOS may already have these shared user directories as 0755.
    let (_temp, home) = home();
    for path in ["Library", "Library/LaunchAgents", "Library/Logs"] {
        let path = home.join(path);
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    }
    let runner = Launchctl::default();
    manage(&home, 501, &runner, ServiceAction::Install).unwrap();
    manage(&home, 501, &runner, ServiceAction::Uninstall).unwrap();
    for path in ["Library", "Library/LaunchAgents", "Library/Logs"] {
        assert_eq!(
            fs::metadata(home.join(path)).unwrap().permissions().mode() & 0o777,
            0o755
        );
    }
}

#[test]
fn uninstall_preserves_a_plist_replaced_while_launchctl_is_running() {
    // The managed inode was observed before bootout; a replacement is not ours to unlink.
    struct ReplacingLaunchctl {
        inner: Launchctl,
        home: PathBuf,
    }
    impl ProcessRunner for ReplacingLaunchctl {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            let result = self.inner.run(request)?;
            if request.args[0] == "bootout" {
                fs::rename(self.home.join(PLIST), self.home.join("retained.plist")).unwrap();
                fs::write(self.home.join(PLIST), b"unrelated replacement").unwrap();
                fs::set_permissions(self.home.join(PLIST), fs::Permissions::from_mode(0o600))
                    .unwrap();
            }
            Ok(result)
        }
    }
    let (_temp, home) = home();
    manage(&home, 501, &Launchctl::default(), ServiceAction::Install).unwrap();
    let runner = ReplacingLaunchctl {
        inner: Launchctl::loaded(),
        home: home.clone(),
    };
    assert!(manage(&home, 501, &runner, ServiceAction::Uninstall).is_err());
    assert_eq!(
        fs::read(home.join(PLIST)).unwrap(),
        b"unrelated replacement"
    );
    assert!(home.join("retained.plist").is_file());
}

#[test]
fn status_does_not_create_directories_and_never_parses_launchctl_stdout() {
    let (_temp, home) = home();
    let status = manage(&home, 501, &Launchctl::default(), ServiceAction::Status).unwrap();
    assert!(!status.installed && !status.loaded);
    assert_eq!(fs::read_dir(&home).unwrap().count(), 0);
    let status = manage(&home, 501, &Launchctl::loaded(), ServiceAction::Status).unwrap();
    assert!(!status.installed && status.loaded);
    assert_eq!(
        serde_json::from_slice::<ServiceStatus>(&serde_json::to_vec(&status).unwrap()).unwrap(),
        status
    );
}

#[test]
fn unknown_launchctl_errors_fail_closed_without_echoing_output_or_removing_plist() {
    let (_temp, home) = home();
    let runner = Launchctl::default();
    manage(&home, 501, &runner, ServiceAction::Install).unwrap();
    runner.calls();
    runner.fail("print", 5);
    let error = manage(&home, 501, &runner, ServiceAction::Uninstall).unwrap_err();
    assert_eq!(error.public_code(), "CONTROLLER_SERVICE");
    let message = error.to_string();
    assert!(!message.contains("private-from-launchctl"));
    assert!(!message.contains("credential"));
    assert!(home.join(PLIST).exists());
    assert_eq!(runner.calls(), expected_calls(&[&["print", TARGET]]));
    runner.fail("bootout", 5);
    assert!(manage(&home, 501, &runner, ServiceAction::Uninstall).is_err());
    assert!(home.join(PLIST).exists());
}

#[test]
fn installation_and_log_truncation_refuse_symlink_targets() {
    let (temp, home) = home();
    let external = temp.path().join("external");
    fs::create_dir(&external).unwrap();
    symlink(&external, home.join("Library")).unwrap();
    assert!(manage(&home, 501, &Launchctl::default(), ServiceAction::Install).is_err());
    assert_eq!(fs::read_dir(&external).unwrap().count(), 0);
    fs::remove_file(home.join("Library")).unwrap();
    manage(&home, 501, &Launchctl::default(), ServiceAction::Install).unwrap();
    let outside = external.join("secret");
    fs::write(&outside, b"preserve").unwrap();
    fs::remove_file(home.join(LOG)).unwrap();
    symlink(&outside, home.join(LOG)).unwrap();
    assert!(truncate_log(&home).is_err());
    assert_eq!(fs::read(&outside).unwrap(), b"preserve");
}

#[test]
fn supervisor_truncates_log_in_place_without_replacing_launchd_descriptor() {
    // Replacing the inode leaves launchd appending to the old unbounded file.
    let (_temp, home) = home();
    manage(&home, 501, &Launchctl::default(), ServiceAction::Install).unwrap();
    fs::write(home.join(LOG), vec![b'x'; 1024 * 1024]).unwrap();
    let metadata = fs::metadata(home.join(LOG)).unwrap();
    truncate_log(&home).unwrap();
    let after = fs::metadata(home.join(LOG)).unwrap();
    assert_eq!(metadata.ino(), after.ino());
    assert_eq!(after.len(), 0);
    assert_eq!(after.permissions().mode() & 0o777, 0o600);
}

#[test]
fn supervisor_does_not_truncate_through_an_unsafe_account_home() {
    let (_temp, home) = home();
    manage(&home, 501, &Launchctl::default(), ServiceAction::Install).unwrap();
    fs::write(home.join(LOG), b"preserve diagnostics").unwrap();
    fs::set_permissions(&home, fs::Permissions::from_mode(0o777)).unwrap();
    assert!(truncate_log(&home).is_err());
    assert_eq!(fs::read(home.join(LOG)).unwrap(), b"preserve diagnostics");
}

#[test]
fn launchdaemon_instructions_use_supplied_account_and_safe_fixed_paths() {
    // A hardcoded user or a shell-expanded home would install the wrong execution identity.
    let commands =
        launchdaemon_commands(Path::new("/Users/some ' owner"), "different-user", 777).unwrap();
    assert!(commands.contains("<key>UserName</key>\n  <string>different-user</string>"));
    assert!(commands.contains("<string>/Users/some &apos; owner/.local/bin/worker</string>"));
    assert!(commands.contains("sudo /bin/launchctl bootout 'gui/777/com.mac-worker.controller'"));
    assert!(commands.contains("sudo /bin/launchctl bootstrap system '/Library/LaunchDaemons/com.mac-worker.controller.plist'"));
    assert!(
        commands.contains("sudo /bin/launchctl kickstart -k 'system/com.mac-worker.controller'")
    );
    assert!(commands.contains(
        "sudo /usr/sbin/chown root:wheel '/Library/LaunchDaemons/com.mac-worker.controller.plist'"
    ));
    assert!(
        commands.contains(
            "sudo /bin/chmod 0644 '/Library/LaunchDaemons/com.mac-worker.controller.plist'"
        )
    );
    assert!(commands.contains("sudo /usr/bin/tee '/Library/LaunchDaemons/com.mac-worker.controller.plist' >/dev/null <<'MAC_WORKER_CONTROLLER_PLIST'"));
    assert!(launchdaemon_commands(Path::new("relative/home"), "user", 501).is_err());
    assert!(launchdaemon_commands(Path::new("/Users/user"), "bad\nuser", 501).is_err());
}
