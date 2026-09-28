use std::{fs, os::unix::fs::PermissionsExt, process::Command};

fn executable(path: &std::path::Path, text: &str) {
    fs::write(path, text).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn records_the_executable_and_version_after_final_login_environment_resolution() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let old = root.path().join("cached-bin");
    let selected = root.path().join("profile-bin");
    let turn = root.path().join("turn");
    for dir in [&home, &old, &selected, &turn] {
        fs::create_dir(dir).unwrap();
    }
    executable(
        &old.join("fixture-agent"),
        "#!/bin/sh\nprintf 'agent 1.0.0\\n'\n",
    );
    executable(
        &selected.join("fixture-agent"),
        "#!/bin/sh\nif [ \"$1\" = --version ]; then printf 'agent %s\\n' \"$PROFILE_VERSION\"; else printf '%s' \"$PROFILE_VERSION\" > ran; fi\n",
    );
    fs::write(
        home.join(".zprofile"),
        "export PATH=\"$LAUNCH_BIN:/bin:/usr/bin\"\nexport PROFILE_VERSION=2.3.4\n",
    )
    .unwrap();
    let output = Command::new(assert_cmd::cargo::cargo_bin!("worker"))
        .args([
            "__mac_worker_prepare_turn",
            "--",
            "/bin/zsh",
            "-lc",
            "exec 'fixture-agent'",
        ])
        .env_clear()
        .env("HOME", &home)
        .env("ZDOTDIR", &home)
        .env("PATH", &old)
        .env("LAUNCH_BIN", &selected)
        .env("PROFILE_VERSION", "1.0.0")
        .env("MAC_WORKER_TURN_DIR", &turn)
        .current_dir(root.path())
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        fs::read_to_string(root.path().join("ran")).unwrap(),
        "2.3.4"
    );
    let identity: serde_json::Value = serde_json::from_slice(
        &fs::read(turn.join("agent-identity.json")).expect("identity captured at launch"),
    )
    .unwrap();
    assert_eq!(
        identity["executable"],
        fs::canonicalize(selected.join("fixture-agent"))
            .unwrap()
            .to_str()
            .unwrap()
    );
    assert_eq!(identity["version"], "2.3.4");
    assert_eq!(identity["version_observation"], "observed");
}

#[test]
fn version_output_is_bounded_and_never_copied_to_public_diagnostics() {
    let root = tempfile::tempdir().unwrap();
    let turn = root.path().join("turn");
    fs::create_dir(&turn).unwrap();
    let program = root.path().join("fixture-agent");
    executable(
        &program,
        "#!/bin/sh\nif [ \"$1\" = --version ]; then i=0; while [ $i -lt 6000 ]; do printf 'secret-output'; i=$((i+1)); done; else printf ran > ran; fi\n",
    );
    let output = Command::new(assert_cmd::cargo::cargo_bin!("worker"))
        .args(["__mac_worker_record_agent", "--"])
        .arg(&program)
        .env_clear()
        .env("HOME", root.path())
        .env("PATH", "/bin:/usr/bin")
        .env("MAC_WORKER_TURN_DIR", &turn)
        .current_dir(root.path())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(78));
    assert!(!root.path().join("ran").exists());
    let bytes = fs::read(turn.join("agent-identity.json")).unwrap();
    let identity: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(identity["version_observation"], "output_limit");
    assert!(identity["version"].is_null());
    assert!(!String::from_utf8(bytes).unwrap().contains("secret-output"));
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
}
