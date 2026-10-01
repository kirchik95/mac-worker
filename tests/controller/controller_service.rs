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

use mac_worker::test_support::{
    controller::service::{
        ServiceAction, ServicePaths, ServiceStatus, launchdaemon_commands as daemon_with_paths,
        manage as manage_with_paths, truncate_log,
    },
    core::error::WorkerError,
    host::process::{ProcessRequest, ProcessResult, ProcessRunner},
};

fn manage(
    home: &Path,
    uid: u32,
    runner: &dyn ProcessRunner,
    action: ServiceAction,
) -> Result<ServiceStatus, WorkerError> {
    let paths = mac_worker::test_support::core::paths::PathLayout::discover(
        None,
        &Default::default(),
        home,
    )?;
    manage_with_paths(home, &paths, &home.join(".config"), uid, runner, action)
}
fn launchdaemon_commands(home: &Path, username: &str, uid: u32) -> Result<String, WorkerError> {
    let paths = mac_worker::test_support::core::paths::PathLayout::discover(
        None,
        &Default::default(),
        home,
    )?;
    daemon_with_paths(home, &ServicePaths::from_layout(&paths)?, username, uid)
}

const TARGET: &str = "gui/501/com.mac-worker.controller";
const PLIST: &str = "Library/LaunchAgents/com.mac-worker.controller.plist";
const LOG: &str = "Library/Logs/mac-worker/controller.log";

#[derive(Default)]
struct Launchctl {
    loaded: Mutex<bool>,
    calls: Mutex<Vec<Vec<String>>>,
    failures: Mutex<VecDeque<(&'static str, i32)>>,
    print_output: Mutex<Option<String>>,
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
        assert!(request.policy.deadline.as_secs() <= 75);
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
            stdout: self
                .print_output
                .lock()
                .unwrap()
                .as_deref()
                .unwrap_or("deliberately opaque launchctl output")
                .as_bytes()
                .to_vec(),
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
    // RunAtLoad started the job on bootstrap; the install reports that start.
    assert!(result.restart_started_at_millis.is_some());
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
            // A kickstart here would kill the instance RunAtLoad just started
            // and wait out launchd's ThrottleInterval before the respawn.
            &["bootstrap", "gui/501", path.to_str().unwrap()],
            &["print", TARGET],
        ])
    );

    let rerun = manage(&home, 501, &runner, ServiceAction::Install).unwrap();
    assert_eq!(rerun.restart_started_at_millis, None);
    assert_eq!(
        ServiceStatus {
            restart_started_at_millis: None,
            ..result
        },
        rerun
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
    assert!(commands.contains("<string>--config</string>\n    <string>/Users/some &apos; owner/.config/mac-worker/config.toml</string>"));
    assert!(commands.contains(
        "<key>XDG_STATE_HOME</key>\n    <string>/Users/some &apos; owner/.local/state</string>"
    ));
    assert!(commands.contains(
        "<key>XDG_CONFIG_HOME</key>\n    <string>/Users/some &apos; owner/.config</string>"
    ));
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

#[test]
fn changed_service_plist_reloads_in_an_existing_public_launchagents_directory() {
    let (_temp, home) = home();
    let runner = Launchctl::default();
    manage(&home, 501, &runner, ServiceAction::Install).unwrap();
    let agents = home.join("Library/LaunchAgents");
    fs::set_permissions(&agents, fs::Permissions::from_mode(0o755)).unwrap();
    let original = fs::read_to_string(home.join(PLIST)).unwrap();
    fs::write(
        home.join(PLIST),
        original.replace("<integer>30</integer>", "<integer>1</integer>"),
    )
    .unwrap();
    runner.calls();
    let status = manage(&home, 501, &runner, ServiceAction::Install).unwrap();
    assert!(status.installed && status.loaded);
    assert_eq!(fs::read_to_string(home.join(PLIST)).unwrap(), original);
    assert_eq!(
        fs::metadata(agents).unwrap().permissions().mode() & 0o777,
        0o755
    );
    assert_eq!(
        runner
            .calls()
            .iter()
            .map(|a| a[0].as_str())
            .collect::<Vec<_>>(),
        ["print", "bootout", "bootstrap", "print"]
    );
}

fn host_service_install(home: &Path, variable: &str, root: &Path) -> String {
    host_service_install_with_config(home, variable, root, None)
}

fn host_service_install_with_config(
    home: &Path,
    variable: &str,
    root: &Path,
    config: Option<&Path>,
) -> String {
    use clap::Parser;
    use mac_worker::test_support::{
        cli::Cli,
        runtime::{RuntimeContext, run_with_stdio_in_context},
    };
    use std::{collections::BTreeMap, io::Cursor};
    struct HostLaunchctl(Mutex<bool>);
    impl ProcessRunner for HostLaunchctl {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            assert_eq!(request.program, "/bin/launchctl");
            let mut loaded = self.0.lock().unwrap();
            let code = match request.args[0].to_str().unwrap() {
                "print" => {
                    if *loaded {
                        0
                    } else {
                        113
                    }
                }
                "bootstrap" | "kickstart" => {
                    *loaded = true;
                    0
                }
                other => panic!("unexpected action {other}"),
            };
            Ok(ProcessResult {
                status: ExitStatus::from_raw(code << 8),
                stdout: vec![],
                stderr: vec![],
            })
        }
    }
    let runtime = RuntimeContext::isolated(
        BTreeMap::from([
            ("HOME".into(), home.as_os_str().into()),
            (variable.into(), root.as_os_str().into()),
        ]),
        home.into(),
        home.into(),
    );
    let mut out = vec![];
    let mut err = vec![];
    let mut args = vec!["worker"];
    if let Some(config) = config {
        args.extend(["--config", config.to_str().unwrap()]);
    }
    args.extend(["host", "controller-service"]);
    let exit = run_with_stdio_in_context(
        Cli::try_parse_from(args).unwrap(),
        &HostLaunchctl(Mutex::new(false)),
        &runtime,
        &mut Cursor::new(br#"{"action":"install"}"#),
        &mut out,
        &mut err,
    );
    assert_eq!(
        exit,
        0,
        "{} {}",
        String::from_utf8_lossy(&out),
        String::from_utf8_lossy(&err)
    );
    fs::read_to_string(home.join(PLIST)).unwrap()
}

#[test]
fn service_paths_pin_helper_config_home_independently_of_explicit_config() {
    let (_temp, home) = home();
    let root = home.join("custom-config-home");
    let config = home.join("custom-inventory.toml");
    let plist = host_service_install_with_config(&home, "XDG_CONFIG_HOME", &root, Some(&config));
    let escaped_root = root.to_str().unwrap().replace('&', "&amp;");
    assert!(
        plist.contains(&format!(
            "<key>XDG_CONFIG_HOME</key>\n    <string>{escaped_root}</string>"
        )),
        "{plist}"
    );
    let escaped_config = config.to_str().unwrap().replace('&', "&amp;");
    assert!(
        plist.contains(&format!(
            "<string>--config</string>\n    <string>{escaped_config}</string>"
        )),
        "{plist}"
    );
}

#[test]
fn service_paths_pin_helper_xdg_config_in_supervised_argv() {
    let (_temp, home) = home();
    let root = home.join("custom-config");
    let plist = host_service_install(&home, "XDG_CONFIG_HOME", &root);
    let config = root
        .join("mac-worker/config.toml")
        .to_str()
        .unwrap()
        .replace('&', "&amp;");
    assert!(
        plist.contains(&format!(
            "<string>--config</string>\n    <string>{config}</string>"
        )),
        "{plist}"
    );
    let escaped_root = root.to_str().unwrap().replace('&', "&amp;");
    let config_environment =
        format!("<key>XDG_CONFIG_HOME</key>\n    <string>{escaped_root}</string>");
    assert!(plist.contains(&config_environment), "{plist}");
    let paths = mac_worker::test_support::core::paths::PathLayout::discover(
        None,
        &std::collections::BTreeMap::from([("XDG_CONFIG_HOME".into(), root.into())]),
        &home,
    )
    .unwrap();
    let commands = daemon_with_paths(
        &home,
        &ServicePaths::from_layout(&paths).unwrap(),
        "owner",
        501,
    )
    .unwrap();
    assert!(commands.contains(&config_environment), "{commands}");
}

#[test]
fn service_paths_pin_helper_xdg_state_and_other_child_roots() {
    let (_temp, home) = home();
    let root = home.join("custom-state");
    let plist = host_service_install(&home, "XDG_STATE_HOME", &root);
    for (key, path) in [
        ("XDG_CONFIG_HOME", home.join(".config")),
        ("XDG_STATE_HOME", root),
        ("XDG_CACHE_HOME", home.join(".cache")),
        ("XDG_DATA_HOME", home.join(".local/share")),
    ] {
        let escaped = path.to_str().unwrap().replace('&', "&amp;");
        assert!(
            plist.contains(&format!("<key>{key}</key>\n    <string>{escaped}</string>")),
            "{plist}"
        );
    }
}

#[test]
fn service_paths_missing_config_fails_before_leader_acquisition() {
    use clap::Parser;
    use mac_worker::test_support::{
        cli::Cli,
        controller::ControllerLeader,
        core::paths::PathLayout,
        runtime::{RuntimeContext, run_with_stdio_in_context},
    };
    use std::{collections::BTreeMap, io::Cursor};
    for explicit in [false, true] {
        let (_temp, home) = home();
        let runtime = RuntimeContext::isolated(BTreeMap::new(), home.clone(), home.clone());
        let paths = PathLayout::discover(None, &BTreeMap::new(), &home).unwrap();
        let _leader = ControllerLeader::acquire(&paths.controller_state_root()).unwrap();
        let mut args = vec!["worker"];
        if explicit {
            args.extend(["--config", paths.config.to_str().unwrap()]);
        }
        args.extend(["controller", "run"]);
        if !explicit {
            args.push("--supervised");
        }
        let mut out = vec![];
        let mut err = vec![];
        let exit = run_with_stdio_in_context(
            Cli::try_parse_from(args).unwrap(),
            &Launchctl::default(),
            &runtime,
            &mut Cursor::new([]),
            &mut out,
            &mut err,
        );
        let error = String::from_utf8_lossy(&err);
        assert_ne!(exit, 0);
        assert!(
            !error.contains("CONTROLLER_LOCK_HELD"),
            "missing config silently fell back: {error}"
        );
        assert!(error.contains("config"), "{error}");
    }
}

#[test]
fn service_paths_runner_child_uses_plist_config_and_inherited_state() {
    // DetachedRunnerExecutor passes --config and inherits the leader environment.
    // Exercise a real runner child in exactly that launch context, with no task
    // present, so it must stop locally without reaching any worker transport.
    let (_temp, home) = home();
    let home = home.canonicalize().unwrap();
    let state_root = home.join("custom-state");
    let plist = host_service_install(&home, "XDG_STATE_HOME", &state_root);
    let config = home.join(".config/mac-worker/config.toml");
    fs::create_dir_all(config.parent().unwrap()).unwrap();
    fs::write(
        &config,
        "version=1\n[[workers]]\nname='fixture'\nssh='never-contact'\nslots=1\n",
    )
    .unwrap();
    mac_worker::test_support::core::config::Config::load(&config).unwrap();
    fn value_after(plist: &str, marker: &str) -> String {
        plist
            .split_once(marker)
            .expect("pinned launch value")
            .1
            .split_once("<string>")
            .unwrap()
            .1
            .split_once("</string>")
            .unwrap()
            .0
            .replace("&amp;", "&")
            .replace("&apos;", "'")
    }
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_worker"));
    child.env_clear().current_dir(&home);
    for name in ["HOME", "XDG_STATE_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME"] {
        child.env(name, value_after(&plist, &format!("<key>{name}</key>")));
    }
    let output = child
        .args([
            "--config",
            &value_after(&plist, "<string>--config</string>"),
            "runner",
            "018f0f4a6b5c7d8e9f00112233445566",
            "118f0f4a6b5c7d8e9f00112233445566",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("IO:"),
        "runner must stop at its absent task files: {error}"
    );
    assert!(state_root.join("mac-worker").is_dir());
    assert!(!home.join(".local/state/mac-worker").exists());
}

#[test]
fn service_observation_parses_running_pid_and_last_exit_status() {
    let (_temp, home) = home();
    let runner = Launchctl::loaded();
    *runner.print_output.lock().unwrap() = Some(format!(
        "{TARGET} = {{\n\tstate = running\n\tpid = 42\n\tlast exit code = 70\n\tenvironment = {{\n\t\tpid = 99\n\t}}\n}}\n"
    ));
    let status =
        serde_json::to_value(manage(&home, 501, &runner, ServiceAction::Status).unwrap()).unwrap();
    assert_eq!(status["pid"], 42);
    assert_eq!(status["running"], true);
    assert_eq!(status["last_exit_status"], 70);
}

#[test]
fn service_observation_parses_a_real_macos_launchctl_print_dump() {
    // Captured from `launchctl print gui/501/<label>` for a running Homebrew
    // LaunchAgent on macOS 26 (Darwin 25.2), relabelled to the controller and
    // with account-specific environment values removed. Nested blocks repeat
    // `state` and `active count`; only the top-level keys may be read.
    let (_temp, home) = home();
    let runner = Launchctl::loaded();
    *runner.print_output.lock().unwrap() = Some(format!(
        "{TARGET} = {{
\tactive count = 1
\tpath = /Users/owner/Library/LaunchAgents/com.mac-worker.controller.plist
\ttype = LaunchAgent
\tstate = running

\tprogram = /Users/owner/.local/bin/worker
\targuments = {{
\t\t/Users/owner/.local/bin/worker
\t\t--config
\t\t/Users/owner/.config/mac-worker/config.toml
\t\tcontroller
\t\trun
\t\t--supervised
\t}}

\tworking directory = /Users/owner

\tstdout path = /Users/owner/Library/Logs/mac-worker/controller.log
\tstderr path = /Users/owner/Library/Logs/mac-worker/controller.log
\tinherited environment = {{
\t\tSSH_AUTH_SOCK => /private/tmp/com.apple.launchd.example/Listeners
\t}}

\tdefault environment = {{
\t\tPATH => /usr/bin:/bin:/usr/sbin:/sbin
\t}}

\tenvironment = {{
\t\tHOME => /Users/owner
\t\tXPC_SERVICE_NAME => com.mac-worker.controller
\t}}

\tdomain = gui/501 [100023]
\tasid = 100023
\tminimum runtime = 10
\texit timeout = 5
\truns = 1
\tpid = 806
\timmediate reason = speculative
\tforks = 12
\texecs = 1
\tinitialized = 1
\ttrampolined = 1
\tstarted suspended = 0
\tproxy started suspended = 0
\tchecked allocations = 0 (queried = 1)
\tchecked allocations reason = no host
\tchecked allocations flags = 0x0
\tlast exit code = (never exited)

\tresource coalition = {{
\t\tID = 977
\t\ttype = resource
\t\tstate = active
\t\tactive count = 1
\t\tname = com.mac-worker.controller
\t}}

\tjetsam coalition = {{
\t\tID = 978
\t\ttype = jetsam
\t\tstate = active
\t\tactive count = 1
\t\tname = com.mac-worker.controller
\t}}

\tspawn type = daemon (3)
\tjetsam priority = 40
\tjetsam memory limit (active) = (unlimited)
\tjetsam memory limit (inactive) = (unlimited)
\tjetsamproperties category = daemon
\tjetsam thread limit = 32
\tcpumon = default

\tproperties = keepalive | runatload | inferred program | managed LWCR | has LWCR
}}
"
    ));
    let status =
        serde_json::to_value(manage(&home, 501, &runner, ServiceAction::Status).unwrap()).unwrap();
    assert_eq!(status["running"], true, "{status}");
    assert_eq!(status["pid"], 806, "{status}");
    assert_eq!(
        status["last_exit_status"],
        serde_json::Value::Null,
        "{status}"
    );
}

#[test]
fn service_observation_exited_or_unknown_never_reports_a_live_pid() {
    let (_temp, home) = home();
    let runner = Launchctl::loaded();
    for (output, running) in [
        (
            format!("{TARGET} = {{\nstate = not running\npid = 42\nlast exit code = 70\n}}\n"),
            serde_json::json!(false),
        ),
        (
            format!("{TARGET} = {{\nstate = future-format\npid = 42\n}}\n"),
            serde_json::json!(null),
        ),
        (
            format!("{TARGET} = {{\nstate = running\npid = 42\npid = 43\n}}\n"),
            serde_json::json!(true),
        ),
        (
            "unrecognized format pid = 42".into(),
            serde_json::json!(null),
        ),
    ] {
        *runner.print_output.lock().unwrap() = Some(output);
        let status =
            serde_json::to_value(manage(&home, 501, &runner, ServiceAction::Status).unwrap())
                .unwrap();
        assert_eq!(status["pid"], serde_json::Value::Null, "{status}");
        assert_eq!(status["running"], running, "{status}");
    }
}
