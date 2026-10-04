use assert_cmd::Command;
use clap::{CommandFactory, Parser};
use mac_worker::test_support::cli::{
    Cli, Command as WorkerCommand, ControllerCommand, HostCommand,
};
use mac_worker::test_support::cli::{into_command, json};
use mac_worker::test_support::core::protocol::PROTOCOL_VERSION;
use predicates::prelude::*;
use std::path::PathBuf;

#[test]
fn controller_channel_help_exposes_identity_and_expected_repin() {
    let mut channel = Command::cargo_bin("worker").unwrap();
    channel.args(["controller", "channel", "--help"]);
    channel
        .assert()
        .success()
        .stdout(predicate::str::contains("identity"))
        .stdout(predicate::str::contains("repin"));
    let mut repin = Command::cargo_bin("worker").unwrap();
    repin.args(["controller", "channel", "repin", "--help"]);
    repin
        .assert()
        .success()
        .stdout(predicate::str::contains("--expect-client-id"))
        .stdout(predicate::str::contains("--force").not());
}

#[test]
fn events_requires_follow_and_rejects_historical_or_notification_options() {
    for arguments in [
        vec!["worker", "events", "-f"],
        vec!["worker", "events", "-f", "--json"],
        vec!["worker", "--json", "events", "-f"],
    ] {
        assert!(Cli::try_parse_from(arguments).is_ok());
    }
    for arguments in [
        vec!["worker", "events"],
        vec!["worker", "events", "--json"],
        vec!["worker", "events", "--follow"],
        vec!["worker", "events", "-f", "--since", "1"],
        vec!["worker", "events", "-f", "--quiet"],
        vec!["worker", "events", "-f", "--channel", "macos"],
        vec!["worker", "events", "-f", "--no-titles"],
        vec!["worker", "events", "-f", "task-id"],
    ] {
        assert!(
            Cli::try_parse_from(arguments.clone()).is_err(),
            "accepted {arguments:?}"
        );
    }
}

#[test]
fn notify_parses_every_option_and_channel() {
    for arguments in [
        vec!["worker", "notify"],
        vec!["worker", "notify", "--follow"],
        vec!["worker", "notify", "--quiet"],
        vec!["worker", "notify", "--no-titles"],
        vec!["worker", "notify", "--json"],
        vec![
            "worker",
            "notify",
            "--follow",
            "--quiet",
            "--no-titles",
            "--channel",
            "both",
        ],
    ] {
        assert!(
            Cli::try_parse_from(arguments.clone()).is_ok(),
            "rejected {arguments:?}"
        );
    }
    for channel in ["auto", "macos", "herdr", "both"] {
        assert!(Cli::try_parse_from(["worker", "notify", "--channel", channel]).is_ok());
    }
    for arguments in [
        vec!["worker", "notify", "-f"],
        vec!["worker", "notify", "--channel"],
        vec!["worker", "notify", "--channel", "remote"],
        vec!["worker", "notify", "--channel", ""],
        vec!["worker", "notify", "--since", "1"],
        vec!["worker", "notify", "--follow=false"],
        vec!["worker", "notify", "--quiet=false"],
        vec!["worker", "notify", "--no-titles=false"],
        vec!["worker", "notify", "task-id"],
    ] {
        assert!(
            Cli::try_parse_from(arguments.clone()).is_err(),
            "accepted {arguments:?}"
        );
    }
}

#[test]
fn events_and_notify_help_describes_every_control() {
    let mut root = Command::cargo_bin("worker").unwrap();
    root.arg("--help");
    root.assert()
        .success()
        .stdout(predicate::str::contains("events"))
        .stdout(predicate::str::contains("notify"));
    let mut events = Command::cargo_bin("worker").unwrap();
    events.args(["events", "--help"]);
    events
        .assert()
        .success()
        .stdout(predicate::str::contains("-f"))
        .stdout(predicate::str::contains("--json"))
        .stdout(predicate::str::contains("--since").not());
    let mut notify = Command::cargo_bin("worker").unwrap();
    notify.args(["notify", "--help"]);
    notify
        .assert()
        .success()
        .stdout(predicate::str::contains("--follow"))
        .stdout(predicate::str::contains("--quiet"))
        .stdout(predicate::str::contains("--channel"))
        .stdout(predicate::str::contains("--no-titles"))
        .stdout(predicate::str::contains("auto"))
        .stdout(predicate::str::contains("macos"))
        .stdout(predicate::str::contains("herdr"))
        .stdout(predicate::str::contains("both"));
}

#[test]
fn help_exposes_surviving_public_commands_and_keeps_host_hidden() {
    let mut command = Command::cargo_bin("worker").unwrap();
    command.arg("--help");

    command
        .assert()
        .success()
        .stdout(predicate::str::contains("setup"))
        .stdout(predicate::str::contains("doctor"))
        .stdout(predicate::str::contains("workers"))
        .stdout(predicate::str::contains("dashboard"))
        .stdout(predicate::str::contains("gc"))
        .stdout(predicate::str::contains("skills"))
        .stdout(predicate::str::contains("controller"))
        .stdout(predicate::str::contains("host").not())
        .stdout(predicate::str::contains("controller-rpc").not());

    for public_command in ["dashboard", "task", "gc", "skills", "controller"] {
        let mut command = Command::cargo_bin("worker").unwrap();
        command.args([public_command, "--help"]);
        let mut assertion = command
            .assert()
            .success()
            .stdout(predicate::str::contains(public_command))
            .stdout(predicate::str::contains("Usage:"));
        if public_command == "dashboard" {
            assertion = assertion
                .stdout(predicate::str::contains("--port"))
                .stdout(predicate::str::contains("--no-open"))
                .stdout(predicate::str::contains("--no-facts-refresh"))
                .stdout(predicate::str::contains(
                    "Settings and task replies still work",
                ))
                .stdout(predicate::str::contains("read-only").not())
                .stdout(predicate::str::contains("controller-viewer").not());
        }
    }
}

#[test]
fn retired_batch_commands_are_rejected_before_loading_config_or_creating_state() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path().canonicalize().unwrap();
    let config = root.join("missing-config.toml");
    let job_id = "018f0f4a6b5c7d8e9f00112233445566";
    let forms: [&[&str]; 4] = [
        &["run", "--", "/usr/bin/true"],
        &["status"],
        &["logs", job_id],
        &["cancel", job_id],
    ];

    for form in forms {
        for json in [false, true] {
            let mut args = vec!["worker", "--config", config.to_str().unwrap()];
            if json {
                args.push("--json");
            }
            args.extend(form.iter().copied());
            let error = Cli::try_parse_from(args.iter().copied())
                .expect_err("retired batch commands must not parse");
            assert_eq!(error.kind(), clap::error::ErrorKind::InvalidSubcommand);

            let mut command = Command::cargo_bin("worker").unwrap();
            command
                .current_dir(&root)
                .env("XDG_CONFIG_HOME", root.join("config"))
                .env("XDG_STATE_HOME", root.join("state"))
                .env("XDG_CACHE_HOME", root.join("cache"))
                .env("XDG_DATA_HOME", root.join("data"))
                .args(args.iter().skip(1));
            command
                .assert()
                .code(64)
                .stdout(predicate::str::is_empty())
                .stderr(predicate::str::contains("unrecognized subcommand"))
                .stderr(predicate::str::contains("configuration error").not());
        }
    }
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
}

#[test]
fn every_public_argument_has_help() {
    let mut missing = Vec::new();
    collect_missing_help(&Cli::command(), "worker", &mut missing);
    assert!(
        missing.is_empty(),
        "public arguments without help:\n{}",
        missing.join("\n")
    );
}

fn collect_missing_help(command: &clap::Command, path: &str, missing: &mut Vec<String>) {
    if command.is_hide_set() {
        return;
    }
    for arg in command.get_arguments() {
        if arg.is_hide_set() {
            continue;
        }
        let id = arg.get_id().as_str();
        if id == "help" || id == "version" {
            continue;
        }
        let help = arg
            .get_help()
            .or_else(|| arg.get_long_help())
            .map(|text| text.to_string())
            .unwrap_or_default();
        if help.trim().is_empty() {
            missing.push(format!("{path} {id}"));
        }
    }
    for subcommand in command.get_subcommands() {
        let child = format!("{path} {}", subcommand.get_name());
        collect_missing_help(subcommand, &child, missing);
    }
}

#[test]
fn gc_supports_preview_by_default_and_explicit_apply() {
    let preview = Cli::try_parse_from(["worker", "gc"]).unwrap();
    assert!(matches!(
        into_command(preview),
        WorkerCommand::Gc { apply: false }
    ));

    let apply = Cli::try_parse_from(["worker", "gc", "--apply"]).unwrap();
    assert!(matches!(
        into_command(apply),
        WorkerCommand::Gc { apply: true }
    ));

    let host = Cli::try_parse_from(["worker", "host", "gc"]).unwrap();
    assert!(matches!(
        into_command(host),
        WorkerCommand::Host {
            command: HostCommand::Gc
        }
    ));
}

#[test]
fn host_follow_turn_parses_three_identifiers_and_stays_hidden() {
    // The herdr pane types `worker host follow-turn <project> <worktree> <job>`;
    // the grammar must accept exactly that and nothing shorter, while the
    // command stays out of every help page like the other host commands.
    let project_id = "b".repeat(64);
    let worktree_id = "c".repeat(64);
    let job_id = "018f0f4a6b5c7d8e9f00112233445566";
    let cli = Cli::try_parse_from([
        "worker",
        "host",
        "follow-turn",
        project_id.as_str(),
        worktree_id.as_str(),
        job_id,
    ])
    .unwrap();
    assert!(matches!(
        into_command(cli),
        WorkerCommand::Host {
            command: HostCommand::FollowTurn { .. }
        }
    ));
    assert!(
        Cli::try_parse_from([
            "worker",
            "host",
            "follow-turn",
            project_id.as_str(),
            worktree_id.as_str()
        ])
        .is_err()
    );

    let mut help = Command::cargo_bin("worker").unwrap();
    help.arg("--help");
    help.assert()
        .success()
        .stdout(predicate::str::contains("follow-turn").not());

    let mut host_help = Command::cargo_bin("worker").unwrap();
    host_help.args(["host", "--help"]);
    host_help
        .assert()
        .success()
        .stdout(predicate::str::contains("task-close"))
        .stdout(predicate::str::contains("follow-turn").not())
        .stdout(predicate::str::contains("outbox-retry").not())
        .stdout(predicate::str::contains("controller-rpc").not());
}

#[test]
fn host_outbox_retry_parses_a_task_id() {
    let task_id = "018f0f4a6b5c7d8e9f00112233445566";
    let cli = Cli::try_parse_from(["worker", "host", "outbox-retry", task_id]).unwrap();
    assert!(matches!(
        into_command(cli),
        WorkerCommand::Host {
            command: HostCommand::OutboxRetry { .. }
        }
    ));
}

#[test]
fn controller_run_is_public_and_controller_rpc_stays_hidden() {
    let run = Cli::try_parse_from(["worker", "controller", "run"]).unwrap();
    assert!(matches!(
        into_command(run),
        WorkerCommand::Controller {
            command: ControllerCommand::Run { supervised: false }
        }
    ));
    let rpc = Cli::try_parse_from(["worker", "host", "controller-rpc"]).unwrap();
    assert!(matches!(
        into_command(rpc),
        WorkerCommand::Host {
            command: HostCommand::ControllerRpc
        }
    ));

    let mut host_help = Command::cargo_bin("worker").unwrap();
    host_help.args(["host", "--help"]);
    host_help
        .assert()
        .success()
        .stdout(predicate::str::contains("controller-rpc").not())
        .stdout(predicate::str::contains("controller-receive-pack").not())
        .stdout(predicate::str::contains("controller-upload-pack").not());
}

#[test]
fn task_help_exposes_lifecycle_commands_and_keeps_runner_hidden() {
    let mut command = Command::cargo_bin("worker").unwrap();
    command.args(["task", "--help"]);
    command
        .assert()
        .success()
        .stdout(predicate::str::contains("submit"))
        .stdout(predicate::str::contains("batch"))
        .stdout(predicate::str::contains("say"))
        .stdout(predicate::str::contains("reconcile"))
        .stdout(predicate::str::contains("runner").not());
}

#[test]
fn skills_help_exposes_list_and_get() {
    let mut root = Command::cargo_bin("worker").unwrap();
    root.arg("--help");
    root.assert()
        .success()
        .stdout(predicate::str::contains("skills"));

    let mut skills = Command::cargo_bin("worker").unwrap();
    skills.args(["skills", "--help"]);
    skills
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage: worker skills"))
        .stdout(predicate::str::contains("list"))
        .stdout(predicate::str::contains("get"));

    let mut get = Command::cargo_bin("worker").unwrap();
    get.args(["skills", "get", "--help"]);
    get.assert()
        .success()
        .stdout(predicate::str::contains("Usage: worker skills get"))
        .stdout(predicate::str::contains("--grammar-only"))
        .stdout(predicate::str::contains("pool-dispatch"))
        .stdout(predicate::str::contains("pool-task-authoring"));
}

#[test]
fn task_help_keeps_the_released_option_names_discoverable() {
    let mut submit = Command::cargo_bin("worker").unwrap();
    submit.args(["task", "submit", "--help"]);
    submit
        .assert()
        .success()
        .stdout(predicate::str::contains("--max-budget <MAX_BUDGET>"))
        .stdout(predicate::str::contains("--max-budget-usd").not());

    let mut batch = Command::cargo_bin("worker").unwrap();
    batch.args(["task", "batch", "--help"]);
    batch
        .assert()
        .success()
        .stdout(predicate::str::contains("--name <NAME>"))
        .stdout(predicate::str::contains("--preview"))
        .stdout(predicate::str::contains("--run-name").not());

    let mut wait = Command::cargo_bin("worker").unwrap();
    wait.args(["task", "wait", "--help"]);
    wait.assert()
        .success()
        .stdout(predicate::str::contains("--task-id <TASK_ID>"))
        .stdout(predicate::str::contains("--run <ID|NAME>"));

    let mut list = Command::cargo_bin("worker").unwrap();
    list.args(["task", "list", "--help"]);
    list.assert()
        .success()
        .stdout(predicate::str::contains("--run <ID|NAME>"));

    let mut task = Command::cargo_bin("worker").unwrap();
    task.args(["task", "--help"]);
    task.assert()
        .success()
        .stdout(predicate::str::contains("publish-retry"))
        .stdout(predicate::str::contains("outbox-retry").not());
}

#[test]
fn task_list_and_wait_parse_a_run_name_or_a_run_id() {
    let name = "polish-2026-09-10";
    let list = Cli::try_parse_from(["worker", "task", "list", "--run", name]).unwrap();
    let WorkerCommand::Task {
        command: mac_worker::test_support::cli::TaskCommand::List { run, .. },
    } = into_command(list)
    else {
        panic!("expected a task list command");
    };
    assert_eq!(run.as_deref(), Some(name));

    let wait = Cli::try_parse_from(["worker", "task", "wait", "--run", name]).unwrap();
    let WorkerCommand::Task {
        command: mac_worker::test_support::cli::TaskCommand::Wait { run, .. },
    } = into_command(wait)
    else {
        panic!("expected a task wait command");
    };
    assert_eq!(run.as_deref(), Some(name));

    let id = "018f0f4a6b5c7d8e9f00112233445566";
    Cli::try_parse_from(["worker", "task", "list", "--run", id]).expect("run id must still parse");
    Cli::try_parse_from(["worker", "task", "wait", "--run", id]).expect("run id must still parse");
}

#[test]
fn task_submit_help_exposes_origin_publication_and_profile_options() {
    let mut submit = Command::cargo_bin("worker").unwrap();
    submit.args(["task", "submit", "--help"]);
    submit
        .assert()
        .success()
        .stdout(predicate::str::contains("--agent <AGENT>"))
        .stdout(predicate::str::contains("--source <SOURCE>"))
        .stdout(predicate::str::contains("--publish <PUBLISH>"))
        .stdout(predicate::str::contains(
            "--publish-branch <PUBLISH_BRANCH>",
        ))
        .stdout(predicate::str::contains("--env-profile <ENV_PROFILE>"))
        .stdout(predicate::str::contains("--model <MODEL>"))
        .stdout(predicate::str::contains("--effort <EFFORT>"));
}

#[test]
fn task_submit_parses_model_and_effort() {
    let cli = Cli::try_parse_from([
        "worker",
        "task",
        "submit",
        "--agent",
        "codex",
        "--model",
        "gpt-5.6-luna",
        "--effort",
        "max",
        "--prompt",
        "fix the flaky login spec",
    ])
    .unwrap();
    let WorkerCommand::Task {
        command: mac_worker::test_support::cli::TaskCommand::Submit { model, effort, .. },
    } = into_command(cli)
    else {
        panic!("expected a task submit command");
    };
    assert_eq!(model.as_deref(), Some("gpt-5.6-luna"));
    assert_eq!(effort.as_deref(), Some("max"));
}

#[test]
fn task_list_parses_the_outcome_filter() {
    let cli = Cli::try_parse_from(["worker", "task", "list", "--outcome", "needs-input"]).unwrap();
    let WorkerCommand::Task {
        command: mac_worker::test_support::cli::TaskCommand::List { outcome, .. },
    } = into_command(cli)
    else {
        panic!("expected a task list command");
    };
    assert_eq!(outcome.as_deref(), Some("needs-input"));
}

#[test]
fn workers_help_exposes_refresh() {
    let mut command = Command::cargo_bin("worker").unwrap();
    command.args(["workers", "--help"]);
    command
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage: worker workers"))
        .stdout(predicate::str::contains("--refresh"))
        .stdout(predicate::str::contains("--clear-auth-incidents"));
}

#[test]
fn gc_help_exposes_preview_usage_and_apply() {
    let mut command = Command::cargo_bin("worker").unwrap();
    command.args(["gc", "--help"]);
    command
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage: worker gc [OPTIONS]"))
        .stdout(predicate::str::contains("--apply"));
}

#[test]
fn host_cancel_parses_without_a_positional_job_id() {
    let job_id = "018f0f4a6b5c7d8e9f00112233445566";
    let host = Cli::try_parse_from(["worker", "host", "cancel"]).unwrap();
    assert!(matches!(
        into_command(host),
        WorkerCommand::Host {
            command: HostCommand::Cancel
        }
    ));
    assert!(Cli::try_parse_from(["worker", "host", "cancel", job_id]).is_err());
}

#[test]
fn public_help_exposes_gc_but_excludes_unimplemented_later_phase_commands() {
    // Break caught: a future-phase control plane becomes discoverable before
    // its contract, lifecycle, and privacy boundaries are implemented.
    let mut command = Command::cargo_bin("worker").unwrap();
    command.arg("--help");

    let forbidden = predicate::str::contains("fetch")
        .or(predicate::str::contains("artifacts"))
        .or(predicate::str::contains("cache"))
        .or(predicate::str::contains("Docker"));

    command.assert().success().stdout(forbidden.not());
}

#[test]
fn doctor_parses_the_public_command_forms_without_resolving_the_project() {
    let cases = [
        (vec!["worker", "doctor"], false, None, Vec::<String>::new()),
        (
            vec!["worker", "doctor", "--project", "/path/to/worktree"],
            false,
            Some(PathBuf::from("/path/to/worktree")),
            Vec::new(),
        ),
        (
            vec![
                "worker",
                "doctor",
                "--include",
                "fixtures/generated/**",
                "--include",
                "tmp/contract.json",
            ],
            false,
            None,
            vec!["fixtures/generated/**".into(), "tmp/contract.json".into()],
        ),
        (
            vec![
                "worker",
                "--json",
                "doctor",
                "--project",
                "/path/to/worktree",
            ],
            true,
            Some(PathBuf::from("/path/to/worktree")),
            Vec::new(),
        ),
    ];

    for (arguments, expected_json, expected_project, expected_includes) in cases {
        let cli = Cli::try_parse_from(arguments).expect("public doctor form must parse");
        assert_eq!(json(&cli), expected_json);
        let WorkerCommand::Doctor { project, includes } = into_command(cli) else {
            panic!("doctor arguments must select the doctor command");
        };
        assert_eq!(project, expected_project);
        assert_eq!(includes, expected_includes);
    }
}

#[test]
fn doctor_rejects_empty_includes_and_unexpected_positionals_as_usage() {
    for arguments in [
        vec!["doctor", "--include", ""],
        vec!["doctor", "unexpected"],
    ] {
        let mut command = Command::cargo_bin("worker").unwrap();
        command.args(arguments);

        command
            .assert()
            .code(64)
            .stdout(predicate::str::is_empty())
            .stderr(predicate::str::is_empty().not());
    }
}

#[test]
fn json_is_a_global_output_mode() {
    let mut command = Command::cargo_bin("worker").unwrap();
    command.args(["--json", "workers", "--help"]);
    command.assert().success();
}

#[test]
fn configuration_errors_use_reserved_exit_code_and_only_stderr() {
    let mut command = Command::cargo_bin("worker").unwrap();
    command.args([
        "--config",
        "/definitely/missing/mac-worker.toml",
        "--json",
        "workers",
    ]);

    command
        .assert()
        .code(64)
        .stdout(predicate::str::is_empty())
        .stderr(predicate::str::contains("configuration error"));
}

#[test]
fn hidden_host_probe_outputs_raw_compact_json_without_loading_config() {
    for json in [false, true] {
        let mut command = Command::cargo_bin("worker").unwrap();
        command.args(["--config", "/definitely/missing/mac-worker.toml"]);
        if json {
            command.arg("--json");
        }
        command.args(["host", "probe"]);

        let output = command.output().unwrap();

        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        let stdout = std::str::from_utf8(&output.stdout).unwrap();
        assert_eq!(stdout.lines().count(), 1);
        let value: serde_json::Value = serde_json::from_str(stdout).unwrap();
        assert_eq!(value["protocol_version"], PROTOCOL_VERSION);
        assert_eq!(
            value["features"],
            serde_json::json!(["task.integration", "task.session-import"])
        );
        assert!(value.get("kind").is_none());
    }
}

#[test]
fn invalid_cli_usage_uses_the_reserved_usage_exit_code() {
    let mut command = Command::cargo_bin("worker").unwrap();
    command.arg("not-a-command");

    command
        .assert()
        .code(64)
        .stdout(predicate::str::is_empty())
        .stderr(predicate::str::contains("unrecognized subcommand"));
}

struct EventCliFixture {
    _temp: tempfile::TempDir,
    root: PathBuf,
    config: PathBuf,
    ssh: PathBuf,
}
impl EventCliFixture {
    fn new() -> Self {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let config = root.join("config.toml");
        std::fs::write(
            &config,
            "version = 1\n[controller]\nenabled = true\nssh = 'fixture-only'\n",
        )
        .unwrap();
        let ssh = root.join("fixture-ssh");
        std::fs::write(&ssh, r#"#!/usr/bin/python3
# Isolated controller fixture: ignores SSH argv and never contacts a host.
import glob, hashlib, json, os, struct, sys
wire = sys.stdin.buffer.read()
request = json.loads(wire[4:])
with open(os.environ['EVENT_FIXTURE_LOG'], 'a') as log:
    log.write(json.dumps(request) + '\n')
body = request['body']
window = {'journal_id':'00000000-0000-0000-0000-000000000001','oldest_seq':'1','head_seq':'5'}
if body.get('controller_health'):
    result = {'features':['controller.events'], 'state':'stale', 'reason':'missing'}
else:
    selector = body['controller_events']
    if selector['op'] == 'repair':
        result = {'rows':[], 'next':None, 'complete':True, 'restart':False, 'baseline_after':selector.get('baseline_after')}
    elif selector['op'] == 'read':
        if selector.get('after') and os.environ.get('EVENT_FIXTURE_READY_PIPE'):
            cached = glob.glob(os.environ['EVENT_FIXTURE_CACHE'] + '/*/notify.json')
            if cached and json.load(open(cached[0])).get('consumed_after'):
                with open(os.environ['EVENT_FIXTURE_READY_PIPE'], 'w') as ready:
                    ready.write('following\n')
        if os.environ.get('EVENT_FIXTURE_UNAVAILABLE') == '1':
            result = None
        elif selector.get('after') is None:
            result = {'type':'snapshot_required', 'reason':'bootstrap', 'window':window}
        else:
            result = dict(window, type='batch', schema_version=1, next_after=selector['after'], events=[], has_more=False)
    else:
        raise AssertionError('unexpected fixture selector')
identity = {'protocol_version':request['protocol_version'],'command':request['command'],'body':body}
digest = hashlib.sha256(json.dumps(identity,sort_keys=True,separators=(',',':')).encode()).hexdigest()
reply = {'protocol_version':request['protocol_version'],'command':request['command'],'request_id':request['request_id'],'payload_sha256':digest,'result':result}
payload = json.dumps(reply,separators=(',',':')).encode()
sys.stdout.buffer.write(struct.pack('>I',len(payload))+payload)
"#).unwrap();
        std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self {
            _temp: temp,
            root,
            config,
            ssh,
        }
    }
    fn command(&self) -> Command {
        Command::from_std(self.process_command())
    }
    fn process_command(&self) -> std::process::Command {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_worker"));
        command
            .env("HOME", self.root.join("home"))
            .env("XDG_CONFIG_HOME", self.root.join("config"))
            .env("XDG_STATE_HOME", self.root.join("state"))
            .env("XDG_CACHE_HOME", self.root.join("cache"))
            .env("XDG_DATA_HOME", self.root.join("data"))
            .env("MAC_WORKER_TEST_SSH", &self.ssh)
            .env("EVENT_FIXTURE_LOG", self.root.join("requests.jsonl"))
            .env_remove("HERDR_SOCKET")
            .args(["--config", self.config.to_str().unwrap()]);
        command
    }
    fn requests(&self) -> Vec<serde_json::Value> {
        std::fs::read_to_string(self.root.join("requests.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}

#[test]
fn production_notify_quiet_uses_rpc_baselines_without_local_task_state_or_channels() {
    for unavailable in [false, true] {
        let fixture = EventCliFixture::new();
        let mut command = fixture.command();
        if unavailable {
            command.env("EVENT_FIXTURE_UNAVAILABLE", "1");
        }
        command.args(["notify", "--quiet", "--no-titles", "--channel", "both"]);
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stdout.is_empty());
        let requests = fixture.requests();
        assert!(
            requests
                .iter()
                .all(|request| request["command"] == "task.list")
        );
        assert!(
            requests
                .iter()
                .any(|request| request["body"]["controller_events"]["op"] == "repair")
        );
        assert!(
            !requests
                .iter()
                .any(|request| request["command"] == "task.wait.poll")
        );
        assert!(!fixture.root.join("state/mac-worker/tasks").exists());
        let events = fixture.root.join("cache/mac-worker/controller/events");
        let state_file = std::fs::read_dir(events)
            .unwrap()
            .map(Result::unwrap)
            .find(|entry| {
                entry.file_type().unwrap().is_dir()
                    && entry.file_name().to_string_lossy().len() == 64
            })
            .unwrap()
            .path()
            .join("notify.json");
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(state_file).unwrap()).unwrap();
        if unavailable {
            assert!(saved["consumed_after"].is_null());
            assert!(String::from_utf8_lossy(&output.stderr).contains("state-only confirmation"));
        } else {
            assert_eq!(saved["consumed_after"]["seq"], "5");
        }
    }
}

#[test]
fn production_event_tail_emits_ready_and_ctrl_c_exits_the_foreground_loop() {
    use std::{
        io::{BufRead, BufReader},
        process::Stdio,
    };
    let fixture = EventCliFixture::new();
    let mut command = fixture.process_command();
    command
        .args(["events", "-f", "--json"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut ready = String::new();
    assert!(stdout.read_line(&mut ready).unwrap() > 0);
    let value: serde_json::Value = serde_json::from_str(&ready).unwrap();
    assert_eq!(value["event"], "ready");
    assert_eq!(value["data"]["head_seq"], "5");
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGINT) }, 0);
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(fixture.requests().iter().all(|request| {
        request["command"] == "task.list"
            && request["body"]
                .get("controller_events")
                .is_none_or(|selector| selector["op"] == "read")
    }));
}

#[test]
fn event_commands_require_controller_mode_before_any_rpc() {
    for arguments in [vec!["events", "-f"], vec!["notify", "--quiet"]] {
        let fixture = EventCliFixture::new();
        std::fs::write(
            &fixture.config,
            "version = 1\n[[workers]]\nname = 'fixture-worker'\nssh = 'fixture-only'\nslots = 1\n",
        )
        .unwrap();
        let valid = mac_worker::test_support::core::config::Config::parse(
            &std::fs::read_to_string(&fixture.config).unwrap(),
        )
        .unwrap();
        assert!(!valid.controller.enabled);
        let mut command = fixture.command();
        command.args(arguments);
        command
            .assert()
            .code(64)
            .stdout(predicate::str::is_empty())
            .stderr(predicate::str::contains("configuration error"));
        assert!(!fixture.root.join("requests.jsonl").exists());
    }
}

struct EventChild(Option<std::process::Child>);
impl Drop for EventChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn production_notify_follow_ctrl_c_stops_after_the_durable_baseline() {
    use std::{
        io::{BufRead, BufReader},
        os::unix::ffi::OsStrExt,
        process::Stdio,
    };
    let fixture = EventCliFixture::new();
    let pipe = fixture.root.join("ready.fifo");
    let pipe_c = std::ffi::CString::new(pipe.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(pipe_c.as_ptr(), 0o600) }, 0);
    let mut command = fixture.process_command();
    command
        .env("EVENT_FIXTURE_READY_PIPE", &pipe)
        .env(
            "EVENT_FIXTURE_CACHE",
            fixture.root.join("cache/mac-worker/controller/events"),
        )
        .args(["notify", "--follow", "--quiet", "--no-titles"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut owned = EventChild(Some(command.spawn().unwrap()));
    let mut handshake = BufReader::new(std::fs::File::open(pipe).unwrap());
    let mut line = String::new();
    assert!(handshake.read_line(&mut line).unwrap() > 0);
    assert_eq!(line, "following\n");
    assert_eq!(
        unsafe { libc::kill(owned.0.as_ref().unwrap().id() as i32, libc::SIGINT) },
        0
    );
    let output = owned.0.take().unwrap().wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
    assert!(
        fixture
            .requests()
            .iter()
            .all(|request| request["command"] == "task.list")
    );
    assert!(!fixture.root.join("state/mac-worker/tasks").exists());
}
