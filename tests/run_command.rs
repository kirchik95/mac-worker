use std::{collections::BTreeSet, path::PathBuf, time::Duration};

use assert_cmd::Command;
use clap::Parser;
use mac_worker::{
    cli::{Cli, Command as WorkerCommand},
    error::WorkerError,
    job::{
        CommandSpec, JobMeta, JobStatus, LocalJobRecord, RemoteUncertainty,
        RequestFingerprintMaterial,
    },
    protocol::PROTOCOL_VERSION,
    run::{RunCompletion, RunReport, RunRequest, StatusReport, StatusRow},
};
use predicates::prelude::*;
use serde_json::Value;

const JOB_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";
const CLIENT_ID: &str = "102f0f4a6b5c7d8e9f00112233445566";
const LEASE_TOKEN: &str = "202f0f4a6b5c7d8e9f00112233445566";
const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const MANIFEST_DIGEST: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

fn assert_usage(arguments: &[&str]) {
    let mut command = Command::cargo_bin("worker").unwrap();
    command.args(arguments);
    command
        .assert()
        .code(64)
        .stdout(predicates::str::is_empty())
        .stderr(predicates::str::is_empty().not());
}

fn local_record(relative_working_dir: &str) -> LocalJobRecord {
    let material = RequestFingerprintMaterial::new(
        JOB_ID.parse().unwrap(),
        CLIENT_ID.parse().unwrap(),
        LEASE_TOKEN.parse().unwrap(),
        "mini-1".into(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        MANIFEST_DIGEST.into(),
        relative_working_dir.into(),
        30_000,
        "heavy".into(),
        CommandSpec::shell("printf TASK9_COMMAND_SECRET".into()).unwrap(),
    )
    .unwrap();
    let meta = JobMeta::new(&material, material.fingerprint(), 100).unwrap();
    LocalJobRecord::new(
        meta,
        LEASE_TOKEN.parse().unwrap(),
        Some(JobStatus::accepted(101).unwrap()),
        RemoteUncertainty::None,
    )
    .unwrap()
}

#[test]
fn run_parses_literal_argv_and_explicit_shell_forms() {
    // Catches parsing that consumes command flags, loses include order, or
    // interprets the literal argv rather than retaining it as UTF-8 strings.
    let cli = Cli::try_parse_from([
        "worker",
        "run",
        "--worker",
        "mini-1",
        "--",
        "npm",
        "test",
        "--",
        "--literal",
    ])
    .unwrap();
    let WorkerCommand::Run {
        worker,
        project,
        includes,
        timeout,
        shell,
        argv,
    } = cli.command
    else {
        panic!("run form must select run command");
    };
    assert_eq!(worker, "mini-1");
    assert_eq!(project, None);
    assert!(includes.is_empty());
    assert_eq!(timeout, None);
    assert_eq!(shell, None);
    assert_eq!(argv, ["npm", "test", "--", "--literal"]);

    let cli = Cli::try_parse_from([
        "worker",
        "run",
        "--worker",
        "mini-1",
        "--project",
        "/repo",
        "--include",
        "fixtures/generated/**",
        "--timeout",
        "45m",
        "--",
        "npm",
        "test",
    ])
    .unwrap();
    let WorkerCommand::Run {
        worker,
        project,
        includes,
        timeout,
        shell,
        argv,
    } = cli.command
    else {
        panic!("run form must select run command");
    };
    assert_eq!(worker, "mini-1");
    assert_eq!(project, Some(PathBuf::from("/repo")));
    assert_eq!(includes, ["fixtures/generated/**"]);
    assert_eq!(timeout, Some(Duration::from_secs(45 * 60)));
    assert_eq!(shell, None);
    assert_eq!(argv, ["npm", "test"]);

    let cli = Cli::try_parse_from([
        "worker",
        "run",
        "--worker",
        "mini-1",
        "--shell",
        "npm run build && npm test",
    ])
    .unwrap();
    let WorkerCommand::Run {
        worker,
        project,
        includes,
        timeout,
        shell,
        argv,
    } = cli.command
    else {
        panic!("run form must select run command");
    };
    assert_eq!(worker, "mini-1");
    assert_eq!(project, None);
    assert!(includes.is_empty());
    assert_eq!(timeout, None);
    assert_eq!(shell.as_deref(), Some("npm run build && npm test"));
    assert!(argv.is_empty());

    let request = RunRequest {
        worker,
        project: PathBuf::from("/path/to/worktree"),
        cli_includes: includes,
        timeout,
        command: CommandSpec::shell(shell.unwrap()).unwrap(),
    };
    assert_eq!(request.worker, "mini-1");

    let cli = Cli::try_parse_from([
        "worker",
        "run",
        "--worker",
        "mini-1",
        "--include",
        "fixtures/generated/**",
        "--include",
        "tmp/contract.json",
        "--",
        "true",
    ])
    .unwrap();
    let WorkerCommand::Run { includes, .. } = cli.command else {
        panic!("run form must select run command");
    };
    assert_eq!(includes, ["fixtures/generated/**", "tmp/contract.json"]);
}

#[test]
fn run_rejects_missing_worker_empty_command_and_mutually_exclusive_modes() {
    // Catches accidental implicit worker selection or ambiguous command mode.
    for arguments in [
        vec!["run", "--", "true"],
        vec!["run", "--worker", "", "--", "true"],
        vec!["run", "--worker", "mini-1"],
        vec!["run", "--worker", "mini-1", "--shell", ""],
        vec!["run", "--worker", "mini-1", "--shell", "true", "--", "echo"],
    ] {
        assert_usage(&arguments);
    }
}

#[test]
fn run_rejects_invalid_timeout_and_future_phase_flags() {
    // Catches timeout boundaries or future-phase options becoming accepted.
    for arguments in [
        vec![
            "run",
            "--worker",
            "mini-1",
            "--timeout",
            "invalid",
            "--",
            "true",
        ],
        vec!["run", "--worker", "mini-1", "--timeout", "0s", "--", "true"],
        vec![
            "run",
            "--worker",
            "mini-1",
            "--timeout",
            "24h1s",
            "--",
            "true",
        ],
        vec![
            "run",
            "--worker",
            "mini-1",
            "--artifact",
            "out",
            "--",
            "true",
        ],
        vec![
            "run",
            "--worker",
            "mini-1",
            "--env",
            "TOKEN=value",
            "--",
            "true",
        ],
        vec![
            "run", "--worker", "mini-1", "--cache", "target", "--", "true",
        ],
    ] {
        assert_usage(&arguments);
    }
}

#[test]
fn status_is_a_list_without_id_and_exact_lookup_with_id() {
    // Catches making status require an ID or accepting a non-canonical ID.
    let cli = Cli::try_parse_from(["worker", "status"]).unwrap();
    assert!(matches!(
        cli.command,
        WorkerCommand::Status { job_id: None }
    ));

    let cli = Cli::try_parse_from(["worker", "status", JOB_ID]).unwrap();
    assert!(matches!(
        cli.command,
        WorkerCommand::Status { job_id: Some(id) } if id.to_string() == JOB_ID
    ));

    assert_usage(&["status", "not-a-job-id"]);
    assert_usage(&["status", JOB_ID, JOB_ID]);
}

#[test]
fn logs_requires_exact_id_and_accepts_short_follow_flag() {
    // Catches optional or non-canonical log identity and loss of `-f`.
    let cli = Cli::try_parse_from(["worker", "logs", JOB_ID]).unwrap();
    assert!(matches!(
        cli.command,
        WorkerCommand::Logs {
            follow: false,
            job_id
        } if job_id.to_string() == JOB_ID
    ));

    let cli = Cli::try_parse_from(["worker", "logs", "-f", JOB_ID]).unwrap();
    assert!(matches!(
        cli.command,
        WorkerCommand::Logs {
            follow: true,
            job_id
        } if job_id.to_string() == JOB_ID
    ));

    assert_usage(&["logs"]);
    assert_usage(&["logs", "not-a-job-id"]);
    assert_usage(&["logs", JOB_ID, JOB_ID]);
}

#[test]
fn status_row_serialization_omits_private_identity_and_payload() {
    // Catches exposing any record field beyond the explicitly sanitized row.
    let command_secret = "printf TASK9_COMMAND_SECRET";
    let material = RequestFingerprintMaterial::new(
        JOB_ID.parse().unwrap(),
        CLIENT_ID.parse().unwrap(),
        LEASE_TOKEN.parse().unwrap(),
        "mini-1".into(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        MANIFEST_DIGEST.into(),
        "packages/app".into(),
        30_000,
        "heavy".into(),
        CommandSpec::shell(command_secret.into()).unwrap(),
    )
    .unwrap();
    let meta = JobMeta::new(&material, material.fingerprint(), 100).unwrap();
    let status = JobStatus::accepted(101).unwrap();
    let record = LocalJobRecord::new(
        meta,
        LEASE_TOKEN.parse().unwrap(),
        Some(status.clone()),
        RemoteUncertainty::unknown_remote("STATUS_UNCERTAIN").unwrap(),
    )
    .unwrap();

    let row = StatusRow::try_from_record(&record).unwrap();
    let value = serde_json::to_value(&row).unwrap();
    let keys = value
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        keys,
        BTreeSet::from([
            "command_summary",
            "created_at_millis",
            "job_id",
            "manifest_digest",
            "project_id",
            "relative_working_dir",
            "remote_uncertainty",
            "status",
            "worker",
            "worktree_id",
        ])
    );
    let encoded = serde_json::to_string(&row).unwrap();
    for secret in [CLIENT_ID, LEASE_TOKEN, command_secret] {
        assert!(!encoded.contains(secret));
    }
    assert_eq!(value["job_id"], JOB_ID);
    assert_eq!(value["worker"], "mini-1");
    assert_eq!(value["command_summary"]["mode"], "shell");

    let run_report = RunReport::new(JOB_ID.parse().unwrap(), "mini-1".into(), status).unwrap();
    assert_eq!(run_report.protocol_version, PROTOCOL_VERSION);
    let completion = RunCompletion {
        report: run_report,
        exit_code: 0,
    };
    assert_eq!(completion.exit_code, 0);

    let report = StatusReport::new(vec![row], 3);
    assert_eq!(report.protocol_version, PROTOCOL_VERSION);
    assert_eq!(report.omitted, 3);
    let report_value: Value = serde_json::to_value(report).unwrap();
    assert_eq!(report_value["protocol_version"], PROTOCOL_VERSION);
}

#[test]
fn status_row_rejects_unsafe_relative_working_directories_without_echoing_them() {
    // Catches a valid persistent record exposing a full or traversal path at
    // the narrower public-report boundary.
    let unsafe_paths = [
        "/Users/alice/secret-project",
        "../../secret",
        "packages/./secret",
        "packages//secret",
        "packages\\secret",
        ".git/config",
    ];
    let mut leaked = Vec::new();

    for planted_path in unsafe_paths {
        match StatusRow::try_from_record(&local_record(planted_path)) {
            Err(WorkerError::Protocol(message)) => {
                assert!(message.contains("INVALID_LOCAL_RECORD"));
                assert!(!message.contains(planted_path));
            }
            Err(error) => panic!("unsafe local path returned the wrong error: {error}"),
            Ok(row) => leaked.push(row.relative_working_dir),
        }
    }

    assert!(
        leaked.is_empty(),
        "unsafe paths reached the public status row: {leaked:?}"
    );
}

#[test]
fn status_row_allows_an_empty_root_relative_working_directory() {
    // Catches treating the canonical project root as an unsafe empty path.
    let row = StatusRow::try_from_record(&local_record("")).unwrap();
    assert_eq!(row.relative_working_dir, "");
}
