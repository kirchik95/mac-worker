use std::{
    ffi::OsString, os::unix::process::ExitStatusExt, path::Path, sync::Mutex, time::Duration,
};

use mac_worker::{
    error::WorkerError,
    keychain::{KeychainUnlockConfig, unlock_keychain},
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    redaction::RedactionBoundary,
};

struct RecordingRunner {
    result: ProcessResult,
    request: Mutex<Option<ProcessRequest>>,
    new_session: Mutex<bool>,
}

impl RecordingRunner {
    fn new(result: ProcessResult) -> Self {
        Self {
            result,
            request: Mutex::new(None),
            new_session: Mutex::new(false),
        }
    }
}

impl ProcessRunner for RecordingRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        *self.request.lock().unwrap() = Some(request.clone());
        Ok(ProcessResult {
            status: self.result.status,
            stdout: self.result.stdout.clone(),
            stderr: self.result.stderr.clone(),
        })
    }

    fn run_in_new_session(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        *self.new_session.lock().unwrap() = true;
        self.run(request)
    }
}

fn config() -> KeychainUnlockConfig {
    KeychainUnlockConfig::from_entries(
        &[
            (
                OsString::from("MAC_WORKER_KEYCHAIN_PASSWORD"),
                OsString::from("profile-password"),
            ),
            (
                OsString::from("MAC_WORKER_KEYCHAIN_PATH"),
                OsString::from("/tmp/test.keychain-db"),
            ),
        ],
        Path::new("/Users/worker"),
    )
    .unwrap()
}

#[test]
fn password_only_config_uses_the_login_keychain_default_path() {
    let config = KeychainUnlockConfig::from_entries(
        &[(
            OsString::from("MAC_WORKER_KEYCHAIN_PASSWORD"),
            OsString::from("profile-password"),
        )],
        Path::new("/Users/worker"),
    )
    .unwrap();
    assert_eq!(
        config.path(),
        Path::new("/Users/worker/Library/Keychains/login.keychain-db")
    );
    assert!(!format!("{config:?}").contains("profile-password"));
    assert!(!format!("{config:?}").contains("login.keychain-db"));
}

fn exit_status(code: i32) -> std::process::ExitStatus {
    ExitStatusExt::from_raw(code << 8)
}

#[test]
fn unlock_helper_uses_stdin_new_session_and_a_bounded_timeout() {
    let runner = RecordingRunner::new(ProcessResult {
        status: exit_status(0),
        stdout: Vec::new(),
        stderr: Vec::new(),
    });
    unlock_keychain(
        &runner,
        &config(),
        &RedactionBoundary::new("/Users/worker")
            .with_secrets(["profile-password", "/tmp/test.keychain-db"]),
    )
    .unwrap();

    assert!(*runner.new_session.lock().unwrap());
    let request = runner.request.lock().unwrap().clone().unwrap();
    assert_eq!(request.program, "/usr/bin/security");
    assert_eq!(
        request.args,
        vec![
            OsString::from("unlock-keychain"),
            OsString::from("/tmp/test.keychain-db"),
        ]
    );
    assert_eq!(request.stdin, Some(b"profile-password\n".to_vec()));
    assert_eq!(
        request.policy,
        ProcessPolicy {
            stdout_limit: 4 * 1024,
            stderr_limit: 4 * 1024,
            deadline: Duration::from_secs(10),
        }
    );
    assert!(
        request
            .args
            .iter()
            .all(|argument| argument != "profile-password")
    );
}

#[test]
fn unlock_helper_failure_is_bounded_and_redacts_profile_values() {
    let runner = RecordingRunner::new(ProcessResult {
        status: exit_status(1),
        stdout: Vec::new(),
        stderr: b"security: profile-password /tmp/test.keychain-db\nunsafe-second-line\n".to_vec(),
    });
    let error =
        unlock_keychain(&runner, &config(), &RedactionBoundary::new("/Users/worker")).unwrap_err();

    assert_eq!(error.public_code(), "KEYCHAIN_UNLOCK_FAILED");
    let text = error.to_string();
    assert!(text.contains("security:"));
    assert!(!text.contains("profile-password"));
    assert!(!text.contains("/tmp/test.keychain-db"));
    assert!(!text.contains("unsafe-second-line"));
}

#[cfg(target_os = "macos")]
#[test]
fn unlocks_and_then_rejects_a_temporary_keychain_without_a_tty() {
    use std::process::Command;

    if !Path::new("/usr/bin/security").is_file() {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("test.keychain-db");
    let path_string = path.to_str().unwrap();
    let password = format!("test-{}", uuid::Uuid::new_v4());
    let created = Command::new("/usr/bin/security")
        .args(["create-keychain", "-p", &password, path_string])
        .output()
        .unwrap();
    if !created.status.success() {
        let diagnostic = String::from_utf8_lossy(&created.stderr);
        if diagnostic.contains("One or more parameters passed to a function were not valid") {
            eprintln!("skipping keychain integration: keychain service is unavailable");
            return;
        }
        panic!("create-keychain failed: {diagnostic}");
    }
    let add = Command::new("/usr/bin/security")
        .args([
            "add-generic-password",
            "-a",
            "mac-worker-test",
            "-s",
            "mac-worker-test",
            "-w",
            "value",
            path_string,
        ])
        .output()
        .unwrap();
    assert!(
        add.status.success(),
        "add-generic-password failed: {}",
        String::from_utf8_lossy(&add.stderr)
    );
    let locked = Command::new("/usr/bin/security")
        .args(["lock-keychain", path_string])
        .output()
        .unwrap();
    assert!(locked.status.success());

    let config = KeychainUnlockConfig::from_entries(
        &[
            (
                "MAC_WORKER_KEYCHAIN_PASSWORD".into(),
                password.clone().into(),
            ),
            ("MAC_WORKER_KEYCHAIN_PATH".into(), path_string.into()),
        ],
        directory.path(),
    )
    .unwrap();
    unlock_keychain(
        &SystemProcessRunner,
        &config,
        &RedactionBoundary::new(directory.path()).with_secrets([&password, path_string]),
    )
    .unwrap();
    let found = Command::new("/usr/bin/security")
        .args([
            "find-generic-password",
            "-s",
            "mac-worker-test",
            "-w",
            path_string,
        ])
        .output()
        .unwrap();
    assert!(found.status.success());
    assert_eq!(String::from_utf8_lossy(&found.stdout).trim(), "value");

    Command::new("/usr/bin/security")
        .args(["lock-keychain", path_string])
        .output()
        .unwrap();
    let wrong = KeychainUnlockConfig::from_entries(
        &[
            (
                "MAC_WORKER_KEYCHAIN_PASSWORD".into(),
                "wrong-password".into(),
            ),
            ("MAC_WORKER_KEYCHAIN_PATH".into(), path_string.into()),
        ],
        directory.path(),
    )
    .unwrap();
    let error = unlock_keychain(
        &SystemProcessRunner,
        &wrong,
        &RedactionBoundary::new(directory.path()).with_secrets(["wrong-password", path_string]),
    )
    .unwrap_err();
    assert_eq!(error.public_code(), "KEYCHAIN_UNLOCK_FAILED");
}
