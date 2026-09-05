use assert_cmd::Command;
use clap::Parser;
use mac_worker::cli::{Cli, Command as WorkerCommand, HostCommand};
use mac_worker::protocol::PROTOCOL_VERSION;
use predicates::prelude::*;
use std::path::PathBuf;

#[test]
fn help_exposes_dashboard_run_status_logs_and_keeps_host_hidden() {
    let mut command = Command::cargo_bin("worker").unwrap();
    command.arg("--help");

    command
        .assert()
        .success()
        .stdout(predicate::str::contains("setup"))
        .stdout(predicate::str::contains("doctor"))
        .stdout(predicate::str::contains("workers"))
        .stdout(predicate::str::contains("dashboard"))
        .stdout(predicate::str::contains("run"))
        .stdout(predicate::str::contains("status"))
        .stdout(predicate::str::contains("logs"))
        .stdout(predicate::str::contains("cancel"))
        .stdout(predicate::str::contains("gc"))
        .stdout(predicate::str::contains("host").not());

    for public_command in ["dashboard", "run", "status", "logs", "cancel", "task"] {
        let mut command = Command::cargo_bin("worker").unwrap();
        command.args([public_command, "--help"]);
        command
            .assert()
            .success()
            .stdout(predicate::str::contains(public_command))
            .stdout(predicate::str::contains("Usage:"));
    }
}

#[test]
fn gc_supports_preview_by_default_and_explicit_apply() {
    let preview = Cli::try_parse_from(["worker", "gc"]).unwrap();
    assert!(matches!(
        preview.command,
        WorkerCommand::Gc { apply: false }
    ));

    let apply = Cli::try_parse_from(["worker", "gc", "--apply"]).unwrap();
    assert!(matches!(apply.command, WorkerCommand::Gc { apply: true }));

    let host = Cli::try_parse_from(["worker", "host", "gc"]).unwrap();
    assert!(matches!(
        host.command,
        WorkerCommand::Host {
            command: HostCommand::Gc
        }
    ));
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
        .stdout(predicate::str::contains("--run-name").not());

    let mut wait = Command::cargo_bin("worker").unwrap();
    wait.args(["task", "wait", "--help"]);
    wait.assert()
        .success()
        .stdout(predicate::str::contains("--task-id <TASK_ID>"))
        .stdout(predicate::str::contains("--run <RUN>"));
}

#[test]
fn task_submit_help_exposes_origin_publication_and_profile_options() {
    // Additive Task 1–3 grammar. Task 4 adds `worker gc` to the same help
    // surface; do not assert that command here.
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
        .stdout(predicate::str::contains("--env-profile <ENV_PROFILE>"));
}

#[test]
fn workers_help_exposes_refresh() {
    let mut command = Command::cargo_bin("worker").unwrap();
    command.args(["workers", "--help"]);
    command
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage: worker workers"))
        .stdout(predicate::str::contains("--refresh"));
}

#[test]
fn cancel_parses_one_job_id_and_exposes_no_hidden_arguments() {
    let job_id = "018f0f4a6b5c7d8e9f00112233445566";
    let cli = Cli::try_parse_from(["worker", "cancel", job_id]).unwrap();
    let WorkerCommand::Cancel { job_id: parsed } = cli.command else {
        panic!("cancel arguments must select the cancel command");
    };
    assert_eq!(parsed.to_string(), job_id);

    let mut command = Command::cargo_bin("worker").unwrap();
    command.args(["cancel", "--help"]);
    command
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage: worker cancel"))
        .stdout(predicate::str::contains("<JOB_ID>"))
        .stdout(predicate::str::contains("host").not());

    let host = Cli::try_parse_from(["worker", "host", "cancel"]).unwrap();
    assert!(matches!(
        host.command,
        WorkerCommand::Host {
            command: HostCommand::Cancel
        }
    ));
    assert!(Cli::try_parse_from(["worker", "host", "cancel", job_id]).is_err());
}

#[test]
fn run_help_exposes_optional_pin_and_no_wait_scheduler_controls() {
    // Break caught: the public grammar regresses to a mandatory worker, hides
    // automatic selection, or omits immediate-capacity mode from discoverable
    // help.
    let mut command = Command::cargo_bin("worker").unwrap();
    command.args(["run", "--help"]);

    command
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "worker run [--worker NAME] [--no-wait] -- COMMAND",
        ))
        .stdout(predicate::str::contains("--worker <WORKER>"))
        .stdout(predicate::str::contains("--no-wait"))
        .stdout(predicate::str::contains("automatically"));

    for arguments in [
        vec!["worker", "run", "--", "npm", "test"],
        vec!["worker", "run", "--worker", "mini-2", "--", "npm", "test"],
        vec!["worker", "run", "--no-wait", "--", "npm", "test"],
        vec![
            "worker",
            "run",
            "--worker",
            "mini-2",
            "--no-wait",
            "--",
            "npm",
            "test",
        ],
    ] {
        Cli::try_parse_from(arguments).expect("documented scheduler run form must parse");
    }
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
fn run_rejects_an_unconfigured_worker_pin_before_remote_work() {
    // Break caught: an arbitrary raw hostname is treated as a worker target
    // rather than being rejected against the configured inventory.
    let root = tempfile::tempdir().unwrap();
    let root_path = std::fs::canonicalize(root.path()).unwrap();
    let config = root_path.join("config.toml");
    for directory in ["config", "state", "cache", "data"] {
        std::fs::create_dir_all(root_path.join(directory)).unwrap();
    }
    std::fs::write(
        &config,
        "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
    )
    .unwrap();

    let mut command = Command::cargo_bin("worker").unwrap();
    command
        .env("XDG_CONFIG_HOME", root_path.join("config"))
        .env("XDG_STATE_HOME", root_path.join("state"))
        .env("XDG_CACHE_HOME", root_path.join("cache"))
        .env("XDG_DATA_HOME", root_path.join("data"))
        .args([
            "--config",
            config.to_str().unwrap(),
            "run",
            "--worker",
            "raw-hostname",
            "--",
            "/usr/bin/true",
        ]);

    command
        .assert()
        .code(64)
        .stdout(predicate::str::is_empty())
        .stderr(predicate::str::contains("WORKER_NOT_FOUND"));
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
        assert_eq!(cli.json, expected_json);
        let WorkerCommand::Doctor { project, includes } = cli.command else {
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
