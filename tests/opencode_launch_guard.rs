//! The worker-side guard for OpenCode launches (`src/prepare_turn.rs`).
//!
//! A turn's argv is built on the runner from the worker's recorded facts,
//! which can be stale. The helper that execs the agent observes `--version`
//! on the resolved executable and refuses an OpenCode argv that does not fit
//! the installed generation: v2 without `--standalone` would start a
//! background service the turn's process group cannot clean up.
//!
//! No real OpenCode runs here. Every `opencode` is a fixture script that
//! answers `--version` and otherwise records its argv.

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

use mac_worker::agent::{
    AgentKind, PermissionPolicy, TurnLimits, TurnParams, adapter_for_launch, render_shell,
};

/// What the two generations print for `--version`.
const V1_VERSION: &str = "printf '1.18.32\\n'";
const V2_VERSION: &str = "printf 'opencode v2.0.18\\n'";

const RECORDED_V1: Option<&str> = Some("1.18.32");
const RECORDED_V2: Option<&str> = Some("2.0.18");

/// One fixture `opencode` per test. Its `--version` behaviour is a sourced
/// file, so a test covers several installed versions with a single script:
/// macOS assesses every new executable on its first exec, and a burst of new
/// scripts slows the bounded `--version` probes of every test running then.
struct Fixture {
    root: tempfile::TempDir,
    program: PathBuf,
}

impl Fixture {
    /// An `opencode` whose `--version` runs `version_script`. Any other
    /// invocation is the agent starting: it writes its argv to `ran`.
    fn new(version_script: &str) -> Self {
        Self::named("opencode", version_script)
    }

    fn named(name: &str, version_script: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let program = root.path().join("bin").join(name);
        let fixture = Self { root, program };
        for dir in [fixture.bin(), fixture.home(), fixture.turn()] {
            fs::create_dir(dir).unwrap();
        }
        fs::write(
            &fixture.program,
            "#!/bin/sh\n[ \"$1\" = --warm ] && exit 0\nif [ \"$1\" = --version ]; then\n. \"$0.version\"\nexit 0\nfi\nprintf '%s\\n' \"$@\" > ran\n",
        )
        .unwrap();
        fs::set_permissions(&fixture.program, fs::Permissions::from_mode(0o755)).unwrap();
        // Pay the first-exec assessment here, outside the helper's bounded
        // probe, so the probe measures only what the version script does.
        assert!(
            Command::new(&fixture.program)
                .arg("--warm")
                .status()
                .unwrap()
                .success()
        );
        // Login shells rebuild PATH, so the fixture directory is put first
        // by the temporary HOME's own startup file.
        fs::write(
            fixture.home().join(".zprofile"),
            "export PATH=\"$LAUNCH_BIN:/bin:/usr/bin\"\n",
        )
        .unwrap();
        fixture.install(version_script);
        fixture
    }

    /// Replaces the installed version and forgets the previous launch.
    fn install(&self, version_script: &str) {
        let mut version = self.program.clone().into_os_string();
        version.push(".version");
        fs::write(version, format!("{version_script}\n")).unwrap();
        let _ = fs::remove_file(self.path().join("ran"));
        let _ = fs::remove_dir_all(self.turn().join("tmp"));
        fs::create_dir(self.turn().join("tmp")).unwrap();
    }

    fn path(&self) -> &Path {
        self.root.path()
    }

    fn bin(&self) -> PathBuf {
        self.path().join("bin")
    }

    fn home(&self) -> PathBuf {
        self.path().join("home")
    }

    fn turn(&self) -> PathBuf {
        self.path().join("turn")
    }

    /// Execs the helper at the point the final login shell reaches it, with
    /// the fixture as the resolved agent. `lease_millis` is the time the
    /// turn's lease has left.
    fn record_agent(&self, launch_args: &[String], lease_millis: Option<u64>) -> Output {
        let mut command = Command::new(assert_cmd::cargo::cargo_bin!("worker"));
        command
            .args(["__mac_worker_record_agent", "--"])
            .arg(&self.program)
            .args(launch_args)
            .env_clear()
            .env("HOME", self.home())
            .env("PATH", "/bin:/usr/bin")
            .env("MAC_WORKER_TURN_DIR", self.turn())
            .current_dir(self.path());
        if let Some(lease_millis) = lease_millis {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            command.env(
                "MAC_WORKER_LEASE_DEADLINE_MILLIS",
                (now + u128::from(lease_millis)).to_string(),
            );
        }
        command.output().unwrap()
    }

    /// The whole worker-side path: the prepare-turn helper, the account
    /// login shell, then the recording helper and the agent.
    fn prepare_turn(&self, shell: &str) -> Output {
        Command::new(assert_cmd::cargo::cargo_bin!("worker"))
            .args(["__mac_worker_prepare_turn", "--", "/bin/zsh", "-lc", shell])
            .env_clear()
            .env("HOME", self.home())
            .env("ZDOTDIR", self.home())
            .env("PATH", "/bin:/usr/bin")
            .env("LAUNCH_BIN", self.bin())
            .env("MAC_WORKER_TURN_DIR", self.turn())
            .current_dir(self.path())
            .output()
            .unwrap()
    }

    /// The argv the agent was started with, or `None` when it never ran.
    fn ran(&self) -> Option<Vec<String>> {
        let text = fs::read_to_string(self.path().join("ran")).ok()?;
        Some(text.lines().map(str::to_owned).collect())
    }

    fn staged(&self, name: &str) -> Option<serde_json::Value> {
        let bytes = fs::read(self.turn().join("tmp").join(name)).ok()?;
        Some(serde_json::from_slice(&bytes).unwrap())
    }

    fn identity(&self) -> serde_json::Value {
        self.staged("mac-worker-agent-identity.json")
            .expect("the helper stages the identity before it decides")
    }

    fn refusal(&self) -> Option<String> {
        self.staged("mac-worker-setup-result.json")
            .map(|failure| failure["code"].as_str().unwrap().to_owned())
    }

    #[track_caller]
    fn assert_refused(&self, output: &Output, code: &str) {
        assert_eq!(output.status.code(), Some(78), "{output:?}");
        assert_eq!(self.refusal().as_deref(), Some(code));
        assert_eq!(self.ran(), None, "a refused launch must not exec the agent");
        // The helper can be killed by a cancel, so it writes only under tmp.
        let names = fs::read_dir(self.turn())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(names, ["tmp"], "the helper wrote outside tmp");
    }

    #[track_caller]
    fn assert_ran(&self, output: &Output, launch_args: &[String]) {
        assert!(output.status.success(), "{output:?}");
        assert_eq!(self.refusal(), None);
        assert_eq!(self.ran().as_deref(), Some(launch_args));
    }
}

fn params() -> TurnParams {
    TurnParams {
        kind: AgentKind::Opencode,
        model: None,
        effort: None,
        policy: PermissionPolicy::Unattended,
        limits: TurnLimits::new(45 * 60 * 1000, None, None).unwrap(),
        session_seed: uuid::Uuid::from_u128(1),
        allow_permission_fallback: false,
    }
}

/// The argv the runner builds for a worker whose facts record `version`.
fn launch_args(recorded_version: Option<&str>) -> Vec<String> {
    adapter_for_launch(AgentKind::Opencode, recorded_version)
        .first_turn(&params())
        .unwrap()
        .args()
        .to_vec()
}

fn resume_args(recorded_version: Option<&str>) -> Vec<String> {
    adapter_for_launch(AgentKind::Opencode, recorded_version)
        .resume_turn(&params(), "ses_f0ea2018effeLWl2cU5BkMIijI")
        .unwrap()
        .args()
        .to_vec()
}

#[test]
fn a_launch_built_for_the_installed_generation_runs() {
    let fixture = Fixture::new(V1_VERSION);
    for (version_script, recorded, observed) in [
        (V1_VERSION, RECORDED_V1, "1.18.32"),
        (V2_VERSION, RECORDED_V2, "2.0.18"),
    ] {
        for args in [launch_args(recorded), resume_args(recorded)] {
            fixture.install(version_script);
            let output = fixture.record_agent(&args, None);
            fixture.assert_ran(&output, &args);
            assert_eq!(fixture.identity()["version"], observed);
        }
    }
    assert!(!launch_args(RECORDED_V1).contains(&"--standalone".to_owned()));
    assert_eq!(launch_args(RECORDED_V2)[..2], ["run", "--standalone"]);
}

#[test]
fn stale_v1_facts_never_start_opencode_v2_without_standalone() {
    let fixture = Fixture::new(V2_VERSION);
    // Facts said v1, or were missing, and the worker runs v2.
    for recorded in [RECORDED_V1, None, Some("not-a-version")] {
        for args in [launch_args(recorded), resume_args(recorded)] {
            fixture.install(V2_VERSION);
            let output = fixture.record_agent(&args, None);
            fixture.assert_refused(&output, "OPENCODE_DIALECT_MISMATCH");
            // The observation stays for diagnosis.
            assert_eq!(fixture.identity()["version"], "2.0.18");
            assert_eq!(fixture.identity()["version_observation"], "observed");
        }
    }
}

#[test]
fn stale_v2_facts_are_refused_on_opencode_v1_with_a_clear_code() {
    let fixture = Fixture::new(V1_VERSION);
    for args in [launch_args(RECORDED_V2), resume_args(RECORDED_V2)] {
        fixture.install(V1_VERSION);
        let output = fixture.record_agent(&args, None);
        fixture.assert_refused(&output, "OPENCODE_DIALECT_MISMATCH");
        assert_eq!(fixture.identity()["version"], "1.18.32");
    }
}

#[test]
fn an_unobservable_version_refuses_only_a_launch_without_standalone() {
    let fixture = Fixture::new(V1_VERSION);
    for (version_script, observation) in [
        ("printf 'development build\\n'", "unavailable"),
        ("exit 3", "unavailable"),
        (
            "i=0; while [ $i -lt 6000 ]; do printf 'x'; i=$((i+1)); done",
            "output_limit",
        ),
    ] {
        // Without the flag the unknown generation may be v2: do not start.
        fixture.install(version_script);
        let output = fixture.record_agent(&launch_args(RECORDED_V1), None);
        fixture.assert_refused(&output, "OPENCODE_VERSION_UNVERIFIED");
        assert!(fixture.identity()["version"].is_null());
        assert_eq!(fixture.identity()["version_observation"], observation);

        // With the flag neither generation can start the service.
        fixture.install(version_script);
        let args = launch_args(RECORDED_V2);
        let output = fixture.record_agent(&args, None);
        fixture.assert_ran(&output, &args);
        assert_eq!(fixture.identity()["version_observation"], observation);
    }
}

#[test]
fn a_version_probe_that_never_answers_does_not_start_a_launch_without_standalone() {
    // The probe would sleep for ten minutes; the turn's remaining lease
    // bounds it, and the launch is refused rather than started blind.
    let fixture = Fixture::new("exec /bin/sleep 600");
    let output = fixture.record_agent(&launch_args(RECORDED_V1), Some(3_000));
    fixture.assert_refused(&output, "OPENCODE_VERSION_UNVERIFIED");
    assert_eq!(fixture.identity()["version_observation"], "timed_out");

    // With `--standalone` neither generation can start the service, so the
    // launch goes ahead once the same bound has passed.
    fixture.install("exec /bin/sleep 600");
    let args = launch_args(RECORDED_V2);
    let output = fixture.record_agent(&args, Some(3_000));
    fixture.assert_ran(&output, &args);
    assert_eq!(fixture.identity()["version_observation"], "timed_out");
}

#[test]
fn an_opencode_launch_waits_past_the_diagnostic_bound_for_its_version() {
    // Three seconds is past the two-second diagnostic bound and far inside
    // the ten seconds a launch that is checked against the answer may wait.
    let slow_v1 = format!("/bin/sleep 3\n{V1_VERSION}");
    let fixture = Fixture::new(&slow_v1);
    let args = launch_args(RECORDED_V1);
    let output = fixture.record_agent(&args, None);
    fixture.assert_ran(&output, &args);
    assert_eq!(fixture.identity()["version"], "1.18.32");
    assert_eq!(fixture.identity()["version_observation"], "observed");

    // The same wait lets a slow v1 refuse an argv built for v2.
    fixture.install(&slow_v1);
    let output = fixture.record_agent(&launch_args(RECORDED_V2), None);
    fixture.assert_refused(&output, "OPENCODE_DIALECT_MISMATCH");
}

#[test]
fn the_guard_runs_behind_the_login_shell_on_the_rendered_launch() {
    let render = |recorded: Option<&str>| {
        render_shell(
            &adapter_for_launch(AgentKind::Opencode, recorded)
                .first_turn(&params())
                .unwrap(),
        )
        .unwrap()
    };
    let fixture = Fixture::new(V2_VERSION);
    let pointer = format!(
        "Read the task from {}/prompt.md and follow it.",
        fixture.turn().display()
    );

    // v2 facts on a v2 worker: the agent starts with `--standalone`.
    let output = fixture.prepare_turn(&render(RECORDED_V2));
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        fixture.ran().unwrap(),
        [
            "run",
            "--standalone",
            "--format",
            "json",
            "--auto",
            pointer.as_str()
        ]
    );
    // The login shell resolved the fixture, not an installed OpenCode.
    assert_eq!(
        fixture.identity()["executable"],
        fs::canonicalize(&fixture.program)
            .unwrap()
            .to_str()
            .unwrap()
    );

    // v1 facts on a v1 worker: today's launch, unchanged.
    fixture.install(V1_VERSION);
    let output = fixture.prepare_turn(&render(RECORDED_V1));
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        fixture.ran().unwrap(),
        ["run", "--format", "json", "--auto", pointer.as_str()]
    );

    // Stale v1 facts on a v2 worker: refused before exec.
    fixture.install(V2_VERSION);
    let output = fixture.prepare_turn(&render(RECORDED_V1));
    fixture.assert_refused(&output, "OPENCODE_DIALECT_MISMATCH");

    // Stale v2 facts on a v1 worker: refused with the same code.
    fixture.install(V1_VERSION);
    let output = fixture.prepare_turn(&render(RECORDED_V2));
    fixture.assert_refused(&output, "OPENCODE_DIALECT_MISMATCH");
}

#[test]
fn another_agent_is_not_held_to_the_opencode_guard() {
    // The same argv under another program name is not an OpenCode launch:
    // its version stays diagnostic and never blocks the exec.
    let fixture = Fixture::named("fixture-agent", V2_VERSION);
    let args = launch_args(RECORDED_V1);
    let output = fixture.record_agent(&args, None);
    fixture.assert_ran(&output, &args);
    assert_eq!(fixture.identity()["version"], "2.0.18");
}
