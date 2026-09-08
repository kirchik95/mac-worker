use std::{
    ffi::{OsStr, OsString},
    fmt,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    time::Duration,
};

use crate::{
    error::WorkerError,
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
    redaction::RedactionBoundary,
};

pub const PASSWORD_ENV_NAME: &str = "MAC_WORKER_KEYCHAIN_PASSWORD";
pub const PATH_ENV_NAME: &str = "MAC_WORKER_KEYCHAIN_PATH";
pub const DEFAULT_KEYCHAIN_RELATIVE_PATH: &str = "Library/Keychains/login.keychain-db";
pub const UNLOCK_FAILED_REASON: &str = "keychain unlock failed";
pub const KEYCHAIN_LOCKED_REASON: &str = "keychain locked";
const SECURITY_PROGRAM: &str = "/usr/bin/security";
const UNLOCK_TIMEOUT: Duration = Duration::from_secs(10);
const UNLOCK_OUTPUT_LIMIT: usize = 4 * 1024;

#[derive(Clone, PartialEq, Eq)]
pub struct KeychainUnlockConfig {
    password: OsString,
    path: PathBuf,
}

impl KeychainUnlockConfig {
    pub fn from_entries(entries: &[(OsString, OsString)], home: &Path) -> Option<Self> {
        let password = entries
            .iter()
            .find_map(|(name, value)| (name == PASSWORD_ENV_NAME).then(|| value.clone()))?;
        let path = entries
            .iter()
            .find_map(|(name, value)| (name == PATH_ENV_NAME).then(|| PathBuf::from(value)))
            .unwrap_or_else(|| home.join(DEFAULT_KEYCHAIN_RELATIVE_PATH));
        Some(Self { password, path })
    }

    pub fn password(&self) -> &OsStr {
        &self.password
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl fmt::Debug for KeychainUnlockConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KeychainUnlockConfig")
            .field("password", &"[redacted]")
            .field("path", &"[redacted]")
            .finish()
    }
}

pub fn is_reserved_env_name(name: &str) -> bool {
    matches!(name, PASSWORD_ENV_NAME | PATH_ENV_NAME)
}

pub fn unlock_keychain(
    runner: &dyn ProcessRunner,
    config: &KeychainUnlockConfig,
    boundary: &RedactionBoundary,
) -> Result<(), WorkerError> {
    let boundary = boundary.clone().with_secrets([
        config.password().to_string_lossy().into_owned(),
        config.path().to_string_lossy().into_owned(),
    ]);
    let request = ProcessRequest {
        program: SECURITY_PROGRAM.into(),
        args: vec![
            "unlock-keychain".into(),
            config.path.clone().into_os_string(),
        ],
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: Some([config.password.as_os_str().as_bytes(), b"\n"].concat()),
        policy: ProcessPolicy {
            stdout_limit: UNLOCK_OUTPUT_LIMIT,
            stderr_limit: UNLOCK_OUTPUT_LIMIT,
            deadline: UNLOCK_TIMEOUT,
        },
        isolate_parent_environment: false,
    };
    let result = match runner.run_in_new_session(&request) {
        Ok(result) if result.status.success() => return Ok(()),
        Ok(result) => Some(result),
        Err(_) => None,
    };
    let first_line = result
        .as_ref()
        .and_then(first_output_line)
        .map(|line| boundary.failure_reason(&line));
    let message = match first_line.filter(|line| !line.is_empty()) {
        Some(line) => format!("{UNLOCK_FAILED_REASON}: {line}"),
        None => UNLOCK_FAILED_REASON.to_owned(),
    };
    Err(WorkerError::Protocol(format!(
        "KEYCHAIN_UNLOCK_FAILED: {message}"
    )))
}

pub fn unlock_keychain_if_supported(
    runner: &dyn ProcessRunner,
    config: &KeychainUnlockConfig,
    boundary: &RedactionBoundary,
) -> Result<(), WorkerError> {
    #[cfg(target_os = "macos")]
    {
        unlock_keychain(runner, config, boundary)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (runner, config, boundary);
        Ok(())
    }
}

fn first_output_line(result: &crate::process::ProcessResult) -> Option<String> {
    for bytes in [&result.stderr, &result.stdout] {
        if let Ok(text) = std::str::from_utf8(bytes)
            && let Some(line) = text.lines().next().filter(|line| !line.is_empty())
        {
            return Some(line.to_owned());
        }
    }
    None
}
