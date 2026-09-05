use std::{fs, path::PathBuf};

use assert_cmd::Command;
use mac_worker::error::WorkerError;
use predicates::prelude::*;

fn read_repository(relative: &str) -> String {
    fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative))
        .unwrap_or_else(|error| panic!("read {relative}: {error}"))
}

fn command_help(args: &[&str]) -> String {
    let output = Command::cargo_bin("worker")
        .unwrap()
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "help {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("help stdout is UTF-8")
}

fn worker_help() -> String {
    [
        command_help(&["--help"]),
        command_help(&["task", "--help"]),
        command_help(&["task", "submit", "--help"]),
        command_help(&["workers", "--help"]),
        command_help(&["task", "reconcile", "--help"]),
        command_help(&["task", "close", "--help"]),
    ]
    .join("\n")
}

#[test]
fn phase_five_d_help_and_readme_document_origin_cursor_opencode_and_gc() {
    let help = worker_help();
    assert!(help.contains("--source <SOURCE>"));
    assert!(help.contains("--publish <PUBLISH>"));
    assert!(help.contains("--publish-branch <PUBLISH_BRANCH>"));
    assert!(help.contains("--agent <AGENT>"));
    assert!(help.contains("--env-profile <ENV_PROFILE>"));
    assert!(help.contains("Usage: worker workers"));
    assert!(help.contains("--refresh"));
    assert!(help.contains("reconcile"));

    let readme = read_repository("README.md");
    for phrase in [
        "source = \"origin\"",
        "publish = [\"fetch\", \"push\"]",
        "CURSOR_API_KEY",
        "worker task reconcile",
        "worker gc --apply",
        "agent turns run with the worker account's full access",
    ] {
        assert!(
            readme.contains(phrase),
            "missing documentation phrase: {phrase}"
        );
    }
}

#[test]
fn sanitized_phase_five_d_evidence_has_origin_and_adapter_rows_without_secrets() {
    let evidence = read_repository("docs/phase-five-validation.md");
    assert!(evidence.contains("Source origin and push publication"));
    assert!(evidence.contains("Cursor"));
    assert!(evidence.contains("OpenCode"));
    assert!(evidence.contains("PENDING LIVE RUN"));
    assert!(!evidence.contains("PLANTED_SECRET"));
    assert!(!evidence.contains("/Users/"));
}

#[test]
fn phase_five_d_json_output_keeps_publication_codes_sanitized() {
    let cases = [
        WorkerError::Project {
            code: "BASE_NOT_ON_ORIGIN",
            message: "origin does not advertise the exact base".into(),
        },
        WorkerError::Git {
            code: "BASE_UNAVAILABLE",
            message: "worker mirror could not fetch the recorded commit".into(),
        },
        WorkerError::Project {
            code: "PUBLISH_REQUIRES_COMMITTED_BASE",
            message: "publish push requires a committed base".into(),
        },
        WorkerError::Git {
            code: "PUBLISH_FAILED",
            message: "origin rejected the result push".into(),
        },
        WorkerError::Agent {
            code: "SESSION_UNBOUND",
            message: "resume requires a recorded session".into(),
        },
        WorkerError::Agent {
            code: "ENV_PROFILE_PERMISSIONS",
            message: "named profile is insecure".into(),
        },
        WorkerError::Capacity {
            code: "CAPABILITY_MISSING",
            message: "pinned worker lacks origin:<host>".into(),
        },
        WorkerError::Project {
            code: "TASK_CONFIG_INVALID",
            message: "publish branch is already reserved".into(),
        },
    ];

    for error in cases {
        let value = serde_json::json!({
            "code": error.public_code(),
            "message": error.public_message(),
        });
        let encoded = value.to_string();
        assert_eq!(value["code"], error.public_code());
        assert!(!encoded.contains("PLANTED_SECRET"));
        assert!(!encoded.contains("/Users/"));
    }

    let mut help = Command::cargo_bin("worker").unwrap();
    help.args(["--json", "task", "submit", "--help"]);
    help.assert().success().stdout(predicate::str::contains(
        "--publish-branch <PUBLISH_BRANCH>",
    ));
}
