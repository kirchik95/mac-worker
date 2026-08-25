use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn help_exposes_only_the_phase_one_public_commands() {
    let mut command = Command::cargo_bin("worker").unwrap();
    command.arg("--help");

    command
        .assert()
        .success()
        .stdout(predicate::str::contains("setup"))
        .stdout(predicate::str::contains("workers"))
        .stdout(predicate::str::contains("host").not());
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
        assert_eq!(value["protocol_version"], 1);
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
