use assert_cmd::Command;
use clap::Parser;
use mac_worker::cli::{Cli, Command as WorkerCommand};
use mac_worker::protocol::PROTOCOL_VERSION;
use predicates::prelude::*;
use std::path::PathBuf;

#[test]
fn help_exposes_run_status_logs_and_keeps_host_hidden() {
    let mut command = Command::cargo_bin("worker").unwrap();
    command.arg("--help");

    command
        .assert()
        .success()
        .stdout(predicate::str::contains("setup"))
        .stdout(predicate::str::contains("doctor"))
        .stdout(predicate::str::contains("workers"))
        .stdout(predicate::str::contains("run"))
        .stdout(predicate::str::contains("status"))
        .stdout(predicate::str::contains("logs"))
        .stdout(predicate::str::contains("host").not());

    for public_command in ["run", "status", "logs"] {
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
