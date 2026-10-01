//! T1 contract gate; operator CLI wiring and help are implemented by T7b.
use mac_worker::test_support::channel::{pin::Pin, testing::identity_fixture};

#[test]
fn gate_repin_input_has_a_strict_schema() {
    let mut value = serde_json::to_value(Pin::from_identity(&identity_fixture())).unwrap();
    value["force"] = serde_json::json!(true);
    assert!(serde_json::from_value::<Pin>(value).is_err());
}

fn isolated_operator_fixture(name: &str) -> bool {
    if std::env::var_os("MAC_WORKER_T7B_OPERATOR_FIXTURE").is_some() {
        return false;
    }
    let root = tempfile::Builder::new()
        .prefix("p3b")
        .tempdir_in("/private/tmp")
        .unwrap();
    let request = mac_worker::test_support::host::process::ProcessRequest {
        program: std::env::current_exe().unwrap().into_os_string(),
        args: vec![
            "--exact".into(),
            format!("controller_channel::{name}").into(),
            "--nocapture".into(),
        ],
        environment: vec![
            ("MAC_WORKER_T7B_OPERATOR_FIXTURE".into(), "1".into()),
            ("HOME".into(), root.path().as_os_str().to_owned()),
            (
                "XDG_CONFIG_HOME".into(),
                root.path().join("config").into_os_string(),
            ),
            (
                "XDG_CACHE_HOME".into(),
                root.path().join("cache").into_os_string(),
            ),
            (
                "XDG_STATE_HOME".into(),
                root.path().join("state").into_os_string(),
            ),
            (
                "XDG_DATA_HOME".into(),
                root.path().join("data").into_os_string(),
            ),
        ],
        environment_remove: vec!["MAC_WORKER_TEST_SSH".into()],
        stdin: None,
        policy: mac_worker::test_support::host::process::ProcessPolicy {
            stdout_limit: 4 * 1024 * 1024,
            stderr_limit: 4 * 1024 * 1024,
            deadline: std::time::Duration::from_secs(60),
        },
        isolate_parent_environment: false,
    };
    use mac_worker::test_support::host::process::ProcessRunner;
    let result = mac_worker::test_support::host::process::SystemProcessRunner
        .run(&request)
        .unwrap();
    assert!(
        result.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    true
}

mod fixtures {
    use clap::Parser;
    use mac_worker::test_support::{
        channel::{
            ConfiguredRoute, SocketIdentity,
            testing::{identity_fixture, result_fixture},
        },
        cli::Cli,
        controller::decode_request,
        core::{config::Config, error::WorkerError, paths::PathLayout},
        host::process::{ProcessRequest, ProcessResult, ProcessRunner},
        runtime::RuntimeContext,
    };
    use std::{
        collections::{BTreeMap, VecDeque},
        ffi::OsString,
        os::unix::fs::PermissionsExt,
        sync::Mutex,
    };
    pub const FIRST: &str = "0123456789ab4def8123456789abcdef";
    pub const SECOND: &str = "fedcba98765443218123456789abcdef";
    pub struct Fixture {
        pub paths: PathLayout,
        pub runtime: RuntimeContext,
        pub config: Config,
        _temp: tempfile::TempDir,
    }
    impl Fixture {
        pub fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().canonicalize().unwrap();
            let home = root.join("home");
            let environment = BTreeMap::from([
                (OsString::from("HOME"), home.clone().into_os_string()),
                (
                    OsString::from("XDG_CONFIG_HOME"),
                    root.join("config").into_os_string(),
                ),
                (
                    OsString::from("XDG_CACHE_HOME"),
                    root.join("cache").into_os_string(),
                ),
                (
                    OsString::from("XDG_STATE_HOME"),
                    root.join("state").into_os_string(),
                ),
                (
                    OsString::from("XDG_DATA_HOME"),
                    root.join("data").into_os_string(),
                ),
            ]);
            for directory in environment.values() {
                std::fs::create_dir_all(std::path::PathBuf::from(directory)).unwrap();
            }
            let paths = PathLayout::discover(None, &environment, &home).unwrap();
            std::fs::create_dir_all(paths.config.parent().unwrap()).unwrap();
            let text = "version=1\n[controller]\nenabled=true\nssh='never-connect'\n[ssh]\nmultiplex=true\n";
            std::fs::write(&paths.config, text).unwrap();
            Self {
                paths,
                config: Config::parse(text).unwrap(),
                runtime: RuntimeContext::isolated(environment, home, root),
                _temp: temp,
            }
        }
        pub fn identity(&self, client_id: &str) -> SocketIdentity {
            let mut identity = identity_fixture();
            identity.service.controller_client_id = client_id.parse().unwrap();
            identity.route_sha256 = ConfiguredRoute::new(&self.config.controller, &self.config.ssh)
                .unwrap()
                .digest()
                .unwrap();
            identity
        }
        pub fn pin_path(&self) -> std::path::PathBuf {
            self.paths
                .controller_cache_root()
                .join("channel/pins")
                .join(format!("{}.json", self.identity(FIRST).route_sha256))
        }
        pub fn plant_pin(&self, bytes: &[u8]) {
            let path = self.pin_path();
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let mut directory = path.parent();
            while let Some(path) = directory {
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
                if path == self.paths.controller_cache_root() {
                    break;
                }
                directory = path.parent();
            }
            std::fs::write(&path, bytes).unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        pub fn run(&self, args: &[&str], runner: &dyn ProcessRunner) -> (u8, String, String) {
            let cli = Cli::try_parse_from(args).unwrap();
            let mut output = vec![];
            let mut errors = vec![];
            let exit = mac_worker::test_support::runtime::run_with_io_in_context(
                cli,
                runner,
                &self.runtime,
                &mut output,
                &mut errors,
            );
            (
                exit,
                String::from_utf8(output).unwrap(),
                String::from_utf8(errors).unwrap(),
            )
        }
    }
    pub struct IdentityRunner {
        identities: Mutex<VecDeque<SocketIdentity>>,
        pub calls: Mutex<Vec<ProcessRequest>>,
    }
    impl IdentityRunner {
        pub fn new(identities: Vec<SocketIdentity>) -> Self {
            Self {
                identities: Mutex::new(identities.into()),
                calls: Mutex::new(vec![]),
            }
        }
    }
    impl ProcessRunner for IdentityRunner {
        fn run(&self, process: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            assert_eq!(process.program, "/usr/bin/ssh");
            assert!(
                process
                    .args
                    .last()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .ends_with("host controller-rpc")
            );
            assert!(
                !process
                    .args
                    .iter()
                    .any(|arg| arg == "-O" || arg == "-S" || arg == "-G")
            );
            let request = decode_request(process.stdin.as_ref().unwrap()).unwrap();
            assert_eq!(request.command(), "task.list");
            assert_eq!(request.body().as_object().unwrap().len(), 1);
            assert_eq!(request.body()["controller_socket"]["op"], "identity");
            let identity = self
                .identities
                .lock()
                .unwrap()
                .pop_front()
                .expect("fresh raw identity");
            assert_eq!(
                request.body()["controller_socket"]["route_sha256"],
                identity.route_sha256.as_str()
            );
            self.calls.lock().unwrap().push(process.clone());
            Ok(result_fixture(
                &request,
                serde_json::json!({"available":identity}),
                0,
            ))
        }
    }
}

#[test]
fn identity_json_reads_raw_stdio_without_pinning() {
    if isolated_operator_fixture("identity_json_reads_raw_stdio_without_pinning") {
        return;
    }
    use fixtures::*;
    let fixture = Fixture::new();
    let runner = IdentityRunner::new(vec![fixture.identity(FIRST)]);
    let (exit, output, errors) = fixture.run(
        &["worker", "controller", "channel", "identity", "--json"],
        &runner,
    );
    assert_eq!(exit, 0, "{errors}");
    let actual: mac_worker::test_support::channel::SocketIdentity =
        serde_json::from_str(&output).unwrap();
    assert_eq!(actual, fixture.identity(FIRST));
    assert_eq!(runner.calls.lock().unwrap().len(), 1);
    assert!(!fixture.pin_path().exists());
    assert!(!fixture.paths.state.join("client-id").exists());
}

#[test]
fn repin_creates_missing_pin_with_multiplex_disabled() {
    if isolated_operator_fixture("repin_creates_missing_pin_with_multiplex_disabled") {
        return;
    }
    use fixtures::*;
    let fixture = Fixture::new();
    std::fs::write(
        &fixture.paths.config,
        "version=1\n[controller]\nenabled=true\nssh='never-connect'\n[ssh]\nmultiplex=false\n",
    )
    .unwrap();
    let runner = IdentityRunner::new(vec![fixture.identity(FIRST)]);
    let (exit, output, errors) = fixture.run(
        &[
            "worker",
            "controller",
            "channel",
            "repin",
            "--expect-client-id",
            FIRST,
            "--json",
        ],
        &runner,
    );
    assert_eq!(exit, 0, "{errors}");
    let expected = Pin::from_identity(&fixture.identity(FIRST));
    assert_eq!(serde_json::from_str::<Pin>(&output).unwrap(), expected);
    assert_eq!(
        serde_json::from_slice::<Pin>(&std::fs::read(fixture.pin_path()).unwrap()).unwrap(),
        expected
    );
    assert_eq!(runner.calls.lock().unwrap().len(), 1);
}

#[test]
fn repin_refreshes_raw_identity_before_exact_replacement() {
    if isolated_operator_fixture("repin_refreshes_raw_identity_before_exact_replacement") {
        return;
    }
    use fixtures::*;
    let fixture = Fixture::new();
    let initial = serde_json::to_vec(&Pin::from_identity(&fixture.identity(FIRST))).unwrap();
    fixture.plant_pin(&initial);
    let notify = fixture
        .paths
        .controller_cache_root()
        .join("notify-sentinel");
    std::fs::write(&notify, b"independent notification cache").unwrap();
    let runner = IdentityRunner::new(vec![fixture.identity(FIRST), fixture.identity(SECOND)]);
    assert_eq!(
        fixture
            .run(
                &["worker", "controller", "channel", "identity", "--json"],
                &runner
            )
            .0,
        0
    );
    let (exit, _, errors) = fixture.run(
        &[
            "worker",
            "controller",
            "channel",
            "repin",
            "--expect-client-id",
            SECOND,
        ],
        &runner,
    );
    assert_eq!(exit, 0, "{errors}");
    assert_eq!(runner.calls.lock().unwrap().len(), 2);
    let saved: Pin = serde_json::from_slice(&std::fs::read(fixture.pin_path()).unwrap()).unwrap();
    assert_eq!(saved, Pin::from_identity(&fixture.identity(SECOND)));
    assert_eq!(
        std::fs::read(notify).unwrap(),
        b"independent notification cache"
    );
}

#[test]
fn repin_expected_mismatch_and_unsafe_pin_preserve_existing_state() {
    if isolated_operator_fixture("repin_expected_mismatch_and_unsafe_pin_preserve_existing_state") {
        return;
    }
    use fixtures::*;
    let fixture = Fixture::new();
    let original = serde_json::to_vec(&Pin::from_identity(&fixture.identity(FIRST))).unwrap();
    fixture.plant_pin(&original);
    let runner = IdentityRunner::new(vec![fixture.identity(FIRST)]);
    assert_ne!(
        fixture
            .run(
                &[
                    "worker",
                    "controller",
                    "channel",
                    "repin",
                    "--expect-client-id",
                    SECOND
                ],
                &runner
            )
            .0,
        0
    );
    assert_eq!(runner.calls.lock().unwrap().len(), 1);
    assert_eq!(std::fs::read(fixture.pin_path()).unwrap(), original);
    std::fs::remove_file(fixture.pin_path()).unwrap();
    let target = fixture.paths.controller_cache_root().join("operator-owned");
    std::fs::write(&target, b"preserve unsafe pin target").unwrap();
    std::os::unix::fs::symlink(&target, fixture.pin_path()).unwrap();
    let runner = IdentityRunner::new(vec![fixture.identity(SECOND)]);
    assert_ne!(
        fixture
            .run(
                &[
                    "worker",
                    "controller",
                    "channel",
                    "repin",
                    "--expect-client-id",
                    SECOND
                ],
                &runner
            )
            .0,
        0
    );
    assert_eq!(runner.calls.lock().unwrap().len(), 1);
    assert!(
        std::fs::symlink_metadata(fixture.pin_path())
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        std::fs::read(target).unwrap(),
        b"preserve unsafe pin target"
    );
}

#[test]
fn repin_requires_canonical_expected_client_id_without_force_or_prompt() {
    use clap::Parser;
    use mac_worker::test_support::cli::Cli;
    assert!(
        Cli::try_parse_from([
            "worker",
            "controller",
            "channel",
            "repin",
            "--expect-client-id",
            fixtures::FIRST
        ])
        .is_ok()
    );
    for args in [
        vec!["worker", "controller", "channel", "repin"],
        vec![
            "worker",
            "controller",
            "channel",
            "repin",
            "--expect-client-id",
            "01234567-89ab-4def-8123-456789abcdef",
        ],
        vec![
            "worker",
            "controller",
            "channel",
            "repin",
            "--expect-client-id",
            "0123456789AB4DEF8123456789ABCDEF",
        ],
        vec![
            "worker",
            "controller",
            "channel",
            "repin",
            "--expect-client-id",
            "not-an-id",
        ],
        vec![
            "worker",
            "controller",
            "channel",
            "repin",
            "--expect-client-id",
            fixtures::FIRST,
            "--force",
        ],
        vec![
            "worker",
            "controller",
            "channel",
            "repin",
            "--expect-client-id",
            fixtures::FIRST,
            "--yes",
        ],
    ] {
        assert!(
            Cli::try_parse_from(args.clone()).is_err(),
            "accepted {args:?}"
        );
    }
}
