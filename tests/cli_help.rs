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
