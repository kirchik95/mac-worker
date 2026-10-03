use crate::support;
use clap::Parser;
use mac_worker::test_support::{
    controller::{canonical_request_sha256, decode_frame, encode_json_frame},
    core::{error::WorkerError, paths::PathLayout, protocol::PROTOCOL_VERSION},
    host::process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    runtime::{RuntimeContext, run_with_stdio_in_context},
    session::{FakeAgentHome, SessionAgent},
};
use mac_worker::{
    Cli,
    test_support::{
        cli::{Command, TaskCommand, into_command},
        task::{client::BatchFile, project_config::ProjectSettings},
    },
};
use std::{
    collections::BTreeMap, ffi::OsString, io::Cursor, os::unix::process::ExitStatusExt, sync::Mutex,
};

struct CliFixture {
    _temp: tempfile::TempDir,
    repo: support::GitRepo,
    home: FakeAgentHome,
    paths: PathLayout,
    runtime: RuntimeContext,
}
impl CliFixture {
    fn new(controller: bool) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let repo = support::GitRepo::init();
        repo.write("README", b"fixture\n");
        repo.commit_all("fixture");
        let home = FakeAgentHome::new();
        let environment = BTreeMap::from([
            (OsString::from("HOME"), home.home().into()),
            (
                OsString::from("XDG_CONFIG_HOME"),
                root.join("config").into(),
            ),
            (OsString::from("XDG_STATE_HOME"), root.join("state").into()),
            (OsString::from("XDG_CACHE_HOME"), root.join("cache").into()),
            (OsString::from("XDG_DATA_HOME"), root.join("data").into()),
        ]);
        let paths = PathLayout::discover(None, &environment, home.home()).unwrap();
        std::fs::create_dir_all(paths.config.parent().unwrap()).unwrap();
        std::fs::write(
            &paths.config,
            if controller {
                "version = 1\n[controller]\nenabled = true\nssh = 'fakecontroller'\n"
            } else {
                "version = 1\n[[workers]]\nname = 'fixture'\nssh = 'never-connect'\nslots = 1\n"
            },
        )
        .unwrap();
        let runtime = RuntimeContext::isolated(
            environment,
            home.home().to_path_buf(),
            repo.root().to_path_buf(),
        );
        Self {
            _temp: temp,
            repo,
            home,
            paths,
            runtime,
        }
    }
    fn run(&self, runner: &dyn ProcessRunner, args: &[&str]) -> (u8, String, String) {
        let cli =
            Cli::try_parse_from(std::iter::once("worker").chain(args.iter().copied())).unwrap();
        let (mut stdout, mut stderr) = (vec![], vec![]);
        let exit = run_with_stdio_in_context(
            cli,
            runner,
            &self.runtime,
            &mut Cursor::new(vec![]),
            &mut stdout,
            &mut stderr,
        );
        (
            exit,
            String::from_utf8(stdout).unwrap(),
            String::from_utf8(stderr).unwrap(),
        )
    }
}

struct ControllerMock {
    features: bool,
    fail_at: Option<&'static str>,
    bodies: Mutex<Vec<serde_json::Value>>,
}
impl ControllerMock {
    fn new(features: bool) -> Self {
        Self {
            features,
            fail_at: None,
            bodies: Mutex::new(vec![]),
        }
    }
}
impl ProcessRunner for ControllerMock {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let ok = |stdout| ProcessResult {
            status: std::process::ExitStatus::from_raw(0),
            stdout,
            stderr: vec![],
        };
        if request.program == "/usr/bin/git" {
            if request.args.iter().any(|arg| arg == "push") {
                if self.fail_at == Some("push") {
                    return Err(WorkerError::task(
                        "CONTROLLER_TRANSPORT",
                        "injected source push failure",
                    ));
                }
                assert!(request.args.iter().any(|arg| arg == "--atomic"));
                return Ok(ok(vec![]));
            }
            return SystemProcessRunner.run(request);
        }
        let parsed: serde_json::Value =
            serde_json::from_slice(decode_frame(request.stdin.as_ref().unwrap()).unwrap()).unwrap();
        let command = parsed["command"].as_str().unwrap();
        if self.fail_at == Some(command) {
            return Err(WorkerError::task(
                "CONTROLLER_TRANSPORT",
                "injected source stream failure",
            ));
        }
        let body = &parsed["body"];
        let digest = canonical_request_sha256(PROTOCOL_VERSION, command, body).unwrap();
        let result = match command {
            "task.list" if body.get("controller_health").is_some() => {
                serde_json::json!({ "features": if self.features { vec!["controller.session-import"] } else { vec![] }, "state":"healthy", "reason":"tick_succeeded" })
            }
            "controller.transfer.source.prepare" => serde_json::json!({
                "token":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "request_id":body["request_id"], "fingerprint":body["fingerprint"], "project_id":body["project_id"], "worktree_id":body["worktree_id"], "expected_oid":body["expected_oid"], "session_oid":body["session_oid"],
            }),
            "controller.transfer.source.finish" => serde_json::json!({
                "token":body["token"], "request_id":body["request_id"], "oid":body["expected_oid"], "request_ref":format!("refs/mac-worker/requests/{}", body["request_id"].as_str().unwrap()), "session_oid":body["session_oid"],
            }),
            "task.submit" => {
                self.bodies.lock().unwrap().push(body.clone());
                return Ok(ok(encode_json_frame(&serde_json::json!({
                    "protocol_version":PROTOCOL_VERSION, "status":"accepted", "request_id":parsed["request_id"], "payload_sha256":digest, "task_id":body["task_id"], "turn_id":body["turn_id"], "created_at_millis":body["created_at_millis"],
                })).unwrap()));
            }
            other => panic!("unexpected fixture RPC {other}"),
        };
        Ok(ok(encode_json_frame(&serde_json::json!({ "protocol_version":PROTOCOL_VERSION, "command":command, "request_id":parsed["request_id"], "payload_sha256":digest, "result":result })).unwrap()))
    }
}

#[test]
fn refusals_precede_capture_in_direct_and_controller_modes() {
    for controller in [false, true] {
        for (extra, code) in [
            (vec!["--agent", "claude"], "SESSION_AGENT_MISMATCH"),
            (vec!["--source", "origin"], "SESSION_REQUIRES_SNAPSHOT"),
            (vec![], "SESSION_NEEDS_WIP"),
        ] {
            let fixture = CliFixture::new(controller);
            fixture.repo.write("README", b"dirty\n");
            let mut args = vec![
                "--json",
                "task",
                "submit",
                "--prompt",
                "continue",
                "--from-session",
                "codex",
            ];
            args.extend(extra);
            let (exit, stdout, stderr) = fixture.run(&ControllerMock::new(false), &args);
            assert_ne!(exit, 0);
            assert!(stdout.contains(code), "{stdout} {stderr}");
            assert!(!fixture.paths.cache.join("transfer").exists());
        }
    }
}

#[test]
fn project_origin_default_is_refused_before_discovery() {
    let fixture = CliFixture::new(false);
    fixture
        .repo
        .write(".worker.toml", b"[task]\nsource = 'origin'\n");
    fixture.repo.commit_all("origin default");
    let (_, stdout, stderr) = fixture.run(
        &ControllerMock::new(false),
        &[
            "--json",
            "task",
            "submit",
            "--prompt",
            "continue",
            "--from-session",
            "claude",
        ],
    );
    assert!(
        stdout.contains("SESSION_REQUIRES_SNAPSHOT"),
        "{stdout} {stderr}"
    );
}

#[test]
fn controller_json_summary_freezes_selector_agent_and_requirements() {
    let fixture = CliFixture::new(true);
    // Configured default is deliberately not the selector's agent.
    fixture
        .repo
        .write(".worker.toml", b"[task]\ndefault_agent = 'claude'\n");
    fixture.repo.commit_all("default agent");
    let id = "018f0f4a-6b5c-7d8e-9f00-112233445566";
    fixture
        .home
        .codex(id, fixture.repo.root().to_str().unwrap(), "0.160.0", 1);
    let remote = ControllerMock::new(true);
    let (exit, stdout, stderr) = fixture.run(
        &remote,
        &[
            "--json",
            "task",
            "submit",
            "--prompt",
            "continue",
            "--from-session",
            "codex",
        ],
    );
    assert_eq!(exit, 0, "{stdout} {stderr}");
    let output: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let summary = &output["session_import"];
    assert_eq!(summary["agent"], "codex");
    assert_eq!(summary["source_id"], id);
    assert_eq!(summary["scrubbed"], 0);
    assert!(summary["size"].as_u64().unwrap() > 0);
    assert_eq!(summary["package_oid"].as_str().unwrap().len(), 40);
    assert!(!stdout.contains(fixture.home.home().to_str().unwrap()));
    let bodies = remote.bodies.lock().unwrap();
    let body = &bodies[0];
    assert_eq!(body["agent"], SessionAgent::Codex.as_str());
    let requires = body["requires"].as_array().unwrap();
    for required in [
        "agent:codex",
        "feature:task.session-import",
        "agent-min:codex@0.160.0",
    ] {
        assert!(requires.contains(&serde_json::json!(required)), "{body}");
    }
}

#[test]
fn controller_retry_refuses_each_incomplete_stream_without_recapture() {
    for phase in [
        "controller.transfer.source.prepare",
        "push",
        "controller.transfer.source.finish",
    ] {
        let fixture = CliFixture::new(true);
        let source = fixture.home.codex(
            "018f0f4a-6b5c-7d8e-9f00-112233445566",
            fixture.repo.root().to_str().unwrap(),
            "0.160.0",
            1,
        );
        let mut remote = ControllerMock::new(true);
        remote.fail_at = Some(phase);
        let (exit, _, _) = fixture.run(
            &remote,
            &[
                "--json",
                "task",
                "submit",
                "--prompt",
                "continue",
                "--from-session",
                "codex",
                "--wip",
            ],
        );
        assert_ne!(exit, 0, "{phase}");
        assert!(
            remote.bodies.lock().unwrap().is_empty(),
            "mutation must not precede source finish"
        );
        let envelope_path = std::fs::read_dir(fixture.paths.controller_cache_root())
            .unwrap()
            .flatten()
            .find(|entry| entry.file_name().to_string_lossy().starts_with("op-"))
            .unwrap()
            .path();
        let original = std::fs::read(&envelope_path).unwrap();
        let envelope: serde_json::Value = serde_json::from_slice(&original).unwrap();
        let context =
            mac_worker::test_support::task::project::ProjectInspector::new(&SystemProcessRunner)
                .inspect(fixture.repo.root())
                .unwrap();
        let transfer = mac_worker::test_support::transfer::repo::TransferRepo::open_or_create(
            &fixture.paths.cache,
            &context.common_dir,
        )
        .unwrap();
        let task = envelope["body"]["task_id"].as_str().unwrap();
        for prefix in ["refs/mac-worker/bases/", "refs/mac-worker/sessions/"] {
            assert!(transfer.has_ref(&format!("{prefix}{task}")), "{phase}");
        }
        drop(transfer);
        std::fs::write(source, b"changed invalid live transcript\n").unwrap();
        let request_id = envelope["request_id"].as_str().unwrap();
        let (exit, stdout, stderr) = fixture.run(
            &ControllerMock::new(true),
            &["controller", "retry", request_id],
        );
        assert_ne!(exit, 0);
        assert!(
            stderr.contains("envelope-only retry is unsafe"),
            "{stdout} {stderr}"
        );
        assert_eq!(std::fs::read(envelope_path).unwrap(), original);
    }
}

#[test]
fn controller_finished_stream_retry_reuses_frozen_package() {
    let fixture = CliFixture::new(true);
    let source = fixture.home.codex(
        "018f0f4a-6b5c-7d8e-9f00-112233445566",
        fixture.repo.root().to_str().unwrap(),
        "0.160.0",
        1,
    );
    let remote = ControllerMock::new(true);
    let (exit, stdout, stderr) = fixture.run(
        &remote,
        &[
            "--json",
            "task",
            "submit",
            "--prompt",
            "continue",
            "--from-session",
            "codex",
        ],
    );
    assert_eq!(exit, 0, "{stdout} {stderr}");
    let output: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    std::fs::write(source, b"changed invalid live transcript\n").unwrap();
    let (exit, stdout, stderr) = fixture.run(
        &remote,
        &[
            "--json",
            "controller",
            "retry",
            output["request_id"].as_str().unwrap(),
        ],
    );
    assert_eq!(exit, 0, "{stdout} {stderr}");
    let bodies = remote.bodies.lock().unwrap();
    assert_eq!(bodies.len(), 2);
    assert_eq!(
        serde_json::to_vec(&bodies[0]).unwrap(),
        serde_json::to_vec(&bodies[1]).unwrap()
    );
}

#[test]
fn human_summary_does_not_print_transcript_path() {
    let fixture = CliFixture::new(true);
    let source = fixture.home.codex(
        "018f0f4a-6b5c-7d8e-9f00-112233445566",
        fixture.repo.root().to_str().unwrap(),
        "0.160.0",
        1,
    );
    let (exit, stdout, stderr) = fixture.run(
        &ControllerMock::new(true),
        &[
            "task",
            "submit",
            "--prompt",
            "continue",
            "--from-session",
            "codex",
        ],
    );
    assert_eq!(exit, 0, "{stdout} {stderr}");
    assert!(
        stdout.starts_with("continuing codex session 018f0f4a ("),
        "{stdout}"
    );
    assert!(stdout.contains("0 secrets scrubbed): \"Synthetic item 0\""));
    assert!(stdout.contains("note: the worker's default model applies"));
    assert!(stdout.contains(
        "note: the session changed in the last 10 s; the pool gets a snapshot as of now"
    ));
    assert!(!stdout.contains(source.to_str().unwrap()));
}

#[test]
fn controller_feature_refusal_has_upgrade_and_restart_hint() {
    let fixture = CliFixture::new(true);
    let (exit, stdout, stderr) = fixture.run(
        &ControllerMock::new(false),
        &[
            "task",
            "submit",
            "--prompt",
            "continue",
            "--from-session",
            "claude",
        ],
    );
    assert_ne!(exit, 0);
    assert!(stderr.contains("CAPABILITY_MISSING"), "{stdout} {stderr}");
    assert!(
        stderr.contains("upgrade and restart the controller"),
        "{stderr}"
    );
    assert!(!fixture.paths.cache.join("transfer").exists());
}

#[test]
fn session_selector_parsing_and_submit_only_scope() {
    for selector in ["claude", "codex:018f0f4a-6b5c-7d8e-9f00-112233445566"] {
        let cli = Cli::try_parse_from([
            "worker",
            "task",
            "submit",
            "--prompt",
            "continue",
            "--from-session",
            selector,
        ])
        .unwrap();
        assert!(matches!(
            into_command(cli),
            Command::Task {
                command: TaskCommand::Submit { .. }
            }
        ));
    }
    for selector in ["cursor", "claude:bad", "codex:"] {
        assert!(
            Cli::try_parse_from([
                "worker",
                "task",
                "submit",
                "--prompt",
                "continue",
                "--from-session",
                selector
            ])
            .is_err()
        );
    }
    assert!(
        Cli::try_parse_from([
            "worker",
            "task",
            "batch",
            "tasks.toml",
            "--from-session",
            "claude"
        ])
        .is_err()
    );
}

#[test]
fn session_submit_help_explains_selector() {
    let help = Cli::try_parse_from(["worker", "task", "submit", "--help"])
        .unwrap_err()
        .to_string();
    assert!(help.contains("--from-session <SELECTOR>"), "{help}");
    assert!(
        help.contains("Continue a laptop Claude Code or Codex session"),
        "{help}"
    );
}

#[test]
fn batch_and_project_defaults_reject_session_keys() {
    for key in ["from_session", "session_import"] {
        for (prefix, suffix) in [
            (
                "version = 1\n",
                "[[tasks]]\nid = 'one'\nprompt = 'continue'\n",
            ),
            (
                "version = 1\n[defaults]\n",
                "[[tasks]]\nid = 'one'\nprompt = 'continue'\n",
            ),
            (
                "version = 1\n[[tasks]]\nid = 'one'\nprompt = 'continue'\n",
                "",
            ),
        ] {
            let control = format!("{prefix}{suffix}");
            toml::from_str::<BatchFile>(&control).expect("sessionless control must be valid");
            let text = format!("{prefix}{key} = 'claude'\n{suffix}");
            let error = toml::from_str::<BatchFile>(&text).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains(&format!("unknown field `{key}`")),
                "{error}"
            );
        }
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(".worker.toml");
        std::fs::write(&path, "[task]\n").unwrap();
        ProjectSettings::load(temp.path(), &[]).expect("sessionless project control must be valid");
        std::fs::write(&path, format!("[task]\n{key} = 'claude'\n")).unwrap();
        let error = ProjectSettings::load(temp.path(), &[]).unwrap_err();
        assert_eq!(error.public_code(), "TASK_CONFIG_INVALID");
    }
}
