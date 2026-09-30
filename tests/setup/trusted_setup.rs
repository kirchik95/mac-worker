use serde_json::json;
use sha2::{Digest, Sha256};
use std::{fs, process::Command};

fn frozen() -> String {
    let recipe = json!({
        "requires": [], "timeout_millis": 10000,
        "commands": ["printf approved >> setup-runs"],
        "check": "test -f setup-runs",
        "lockfiles": ["lock"], "inputs": ["package.json"],
        "input_digests": {
            "lock": format!("{:x}", Sha256::digest(b"original")),
            "package.json": format!("{:x}", Sha256::digest(b"{}"))
        }
    });
    // Canonical material is a JSON value (sorted object keys).
    let digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&recipe).unwrap()));
    json!({"recipe": recipe, "digest": digest}).to_string()
}

fn run(root: &std::path::Path, frozen: Option<&str>) -> std::process::Output {
    let mut command = Command::new(assert_cmd::cargo::cargo_bin!("worker"));
    command
        .args([
            "__mac_worker_prepare_turn",
            "--",
            "/bin/sh",
            "-c",
            "printf agent > agent-ran",
        ])
        .current_dir(root.join("workspace"))
        .env_clear()
        .env("HOME", root.join("home"))
        .env("ZDOTDIR", root.join("home"))
        .env("PATH", "/usr/bin:/bin")
        .env("MAC_WORKER_TURN_DIR", root.join("turn"))
        .env("MAC_WORKER_TASK_ID", "a".repeat(32))
        .env("MAC_WORKER_PROJECT_ID", "b".repeat(64));
    if let Some(frozen) = frozen {
        command.env("MAC_WORKER_FROZEN_SETUP", frozen);
    }
    command.output().unwrap()
}

fn fixture() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    for dir in ["workspace", "home", "turn", "turn/tmp"] {
        fs::create_dir(root.path().join(dir)).unwrap();
    }
    fs::write(root.path().join("workspace/lock"), "original").unwrap();
    fs::write(root.path().join("workspace/package.json"), "{}").unwrap();
    fs::write(
        root.path().join("workspace/.worker.toml"),
        "[setup]\ncommands = ['printf unapproved >> setup-runs']\n",
    )
    .unwrap();
    root
}

#[test]
fn workspace_recipe_cannot_override_frozen_recipe_and_unchanged_inputs_reuse_receipt() {
    let root = fixture();
    let result = run(root.path(), Some(&frozen()));
    assert!(result.status.success(), "{result:?}");
    assert_eq!(
        fs::read_to_string(root.path().join("workspace/setup-runs")).unwrap(),
        "approved"
    );
    fs::write(
        root.path().join("workspace/.worker.toml"),
        "not even valid toml!",
    )
    .unwrap();
    assert!(run(root.path(), Some(&frozen())).status.success());
    assert_eq!(
        fs::read_to_string(root.path().join("workspace/setup-runs")).unwrap(),
        "approved"
    );
}

#[test]
fn followup_refuses_changed_lockfile_and_lifecycle_input_before_check_or_agent() {
    for input in ["lock", "package.json"] {
        let root = fixture();
        assert!(run(root.path(), Some(&frozen())).status.success());
        fs::remove_file(root.path().join("workspace/agent-ran")).unwrap();
        fs::write(
            root.path().join("workspace").join(input),
            "changed by agent",
        )
        .unwrap();
        let result = run(root.path(), Some(&frozen()));
        assert_eq!(result.status.code(), Some(78));
        let failure: serde_json::Value = serde_json::from_slice(
            &fs::read(root.path().join("turn/tmp/mac-worker-setup-result.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(failure["code"], "SETUP_INPUTS_CHANGED");
        assert!(!root.path().join("workspace/agent-ran").exists());
        assert_eq!(
            fs::read_to_string(root.path().join("workspace/setup-runs")).unwrap(),
            "approved"
        );
    }
}

#[test]
fn legacy_turn_without_frozen_recipe_never_executes_workspace_setup() {
    let root = fixture();
    assert!(run(root.path(), None).status.success());
    assert!(!root.path().join("workspace/setup-runs").exists());
}

#[test]
fn frozen_setup_reads_original_commit_even_after_workspace_and_head_change() {
    use mac_worker::{process::SystemProcessRunner, project_readiness::FrozenSetup};
    let repo = support::GitRepo::init();
    repo.write(
        ".worker.toml",
        b"[setup]\ncommands = ['printf approved']\nlockfiles = ['lock']\n",
    );
    repo.write("lock", b"original");
    repo.commit_all("operator snapshot");
    let base = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    repo.write(
        ".worker.toml",
        b"[setup]\ncommands = ['printf unapproved']\n",
    );
    repo.write("lock", b"agent change");
    repo.commit_all("agent changes");
    let frozen = FrozenSetup::from_snapshot(&SystemProcessRunner, repo.root(), &base)
        .unwrap()
        .unwrap();
    assert_eq!(frozen.settings().commands, ["printf approved"]);
    assert_eq!(
        frozen.verify_inputs(repo.root()).unwrap_err().public_code(),
        "SETUP_INPUTS_CHANGED"
    );
    let encoded = serde_json::to_value(&frozen).unwrap();
    assert_eq!(
        encoded["recipe"]["input_digests"]["lock"],
        format!("{:x}", Sha256::digest(b"original"))
    );
    assert_eq!(encoded["digest"].as_str().unwrap().len(), 64);
}

use crate::support;
