use std::{
    fs,
    os::unix::{fs::PermissionsExt, process::CommandExt},
    process::Command,
};

fn executable(path: &std::path::Path, text: &str) {
    fs::write(path, text).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

/// The launch wrapper can be killed by a cancel at any point, so it may only
/// write inside the turn's disposable `tmp` scope. Returns the staged record.
fn staged_identity(turn: &std::path::Path) -> Vec<u8> {
    let names = fs::read_dir(turn)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, ["tmp"], "the wrapper wrote outside tmp");
    let names = fs::read_dir(turn.join("tmp"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, ["mac-worker-agent-identity.json"]);
    let path = turn.join("tmp/mac-worker-agent-identity.json");
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    fs::read(path).expect("identity staged at launch")
}

#[test]
fn records_the_executable_and_version_after_final_login_environment_resolution() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let old = root.path().join("cached-bin");
    let selected = root.path().join("profile-bin");
    let turn = root.path().join("turn");
    for dir in [&home, &old, &selected, &turn, &turn.join("tmp")] {
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
    let identity: serde_json::Value = serde_json::from_slice(&staged_identity(&turn)).unwrap();
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
    assert_unavailable_probe_still_launches(
        "i=0; while [ $i -lt 6000 ]; do printf 'secret-output'; i=$((i+1)); done",
        "output_limit",
    );
}

#[test]
fn a_slow_version_probe_times_out_without_blocking_the_agent() {
    assert_unavailable_probe_still_launches("exec /bin/sleep 5", "timed_out");
}

fn assert_unavailable_probe_still_launches(probe: &str, observation: &str) {
    let root = tempfile::tempdir().unwrap();
    let turn = root.path().join("turn");
    fs::create_dir(&turn).unwrap();
    fs::create_dir(turn.join("tmp")).unwrap();
    let program = root.path().join("fixture-agent");
    executable(
        &program,
        &format!(
            "#!/bin/sh\nif [ \"$1\" = --version ]; then /bin/ps -o pgid= -p $$ > probe.pgid; {probe}; else /bin/ps -o pgid= -p $$ > agent.pgid; printf '%s' $$ > agent.pid; printf ran > ran; fi\n"
        ),
    );
    let output = Command::new(assert_cmd::cargo::cargo_bin!("worker"))
        .args(["__mac_worker_record_agent", "--"])
        .arg(&program)
        .env_clear()
        .env("HOME", root.path())
        .env("PATH", "/bin:/usr/bin")
        .env("MAC_WORKER_TURN_DIR", &turn)
        .current_dir(root.path())
        .process_group(0)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert_eq!(fs::read_to_string(root.path().join("ran")).unwrap(), "ran");
    let group = fs::read_to_string(root.path().join("agent.pgid")).unwrap();
    assert_eq!(
        group.trim(),
        fs::read_to_string(root.path().join("probe.pgid"))
            .unwrap()
            .trim()
    );
    assert_eq!(
        group.trim(),
        fs::read_to_string(root.path().join("agent.pid")).unwrap()
    );
    let bytes = staged_identity(&turn);
    let identity: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        identity["executable"],
        fs::canonicalize(&program).unwrap().to_str().unwrap()
    );
    assert_eq!(identity["version_observation"], observation);
    assert!(identity["version"].is_null());
    assert!(!String::from_utf8(bytes).unwrap().contains("secret-output"));
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
}

#[test]
fn an_unavailable_identity_record_does_not_block_agent_exec() {
    let root = tempfile::tempdir().unwrap();
    let turn = root.path().join("not-a-directory");
    fs::write(&turn, b"unavailable diagnostic storage").unwrap();
    let program = root.path().join("fixture-agent");
    executable(
        &program,
        "#!/bin/sh\nif [ \"$1\" = --version ]; then printf 'agent 1.0.0\\n'; else printf ran > ran; fi\n",
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
    assert!(output.status.success(), "{output:?}");
    assert_eq!(fs::read_to_string(root.path().join("ran")).unwrap(), "ran");
    assert_eq!(fs::read(&turn).unwrap(), b"unavailable diagnostic storage");
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
}

#[test]
fn unresolvable_or_unexecutable_agents_still_fail_launch() {
    for invalid_executable in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let turn = root.path().join("turn");
        fs::create_dir(&turn).unwrap();
        fs::create_dir(turn.join("tmp")).unwrap();
        let program = root.path().join("fixture-agent");
        if invalid_executable {
            executable(&program, "#!/nonexistent/fixture-interpreter\n");
        }
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
        // A failing helper can still be killed by a cancel, so its failure
        // record is staged in tmp like the identity, never in the turn dir.
        let names = fs::read_dir(&turn)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(names, ["tmp"], "the helper wrote outside tmp");
        let failure: serde_json::Value = serde_json::from_slice(
            &fs::read(turn.join("tmp/mac-worker-setup-result.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            failure["code"],
            if invalid_executable {
                "SETUP_FAILED"
            } else {
                "AGENT_EXECUTABLE_NOT_FOUND"
            }
        );
    }
}
