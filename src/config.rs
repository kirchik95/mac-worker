use std::{collections::HashSet, fs, path::Path};

use serde::Deserialize;

use crate::error::WorkerError;

const REMOTE_BINARY: &str = "~/.local/bin/worker";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    #[serde(default)]
    pub notifications: NotificationsConfig,
    pub workers: Vec<WorkerEntry>,
}

/// Laptop-side notifications about finished turns.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationsConfig {
    /// Tell the MacBook's herdr when a turn ends.  On by default because it
    /// is a no-op when no herdr socket is reachable.
    #[serde(default = "default_true")]
    pub herdr: bool,
}

impl Default for NotificationsConfig {
    fn default() -> Self {
        Self { herdr: true }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerEntry {
    pub name: String,
    pub ssh: String,
    pub slots: u8,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default = "default_remote_binary")]
    pub remote_binary: String,
    /// Report this worker's turns to the herdr server running on it.  Off by
    /// default: it creates tabs on the worker and needs herdr there.
    #[serde(default)]
    pub herdr: bool,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, WorkerError> {
        let contents = fs::read_to_string(path).map_err(|error| {
            let hint = if error.kind() == std::io::ErrorKind::NotFound {
                "; connect your first Mac with `worker init user@mini.local` (keep --config if you use a custom path)"
            } else { "" };
            WorkerError::Config(format!("failed to read {}: {error}{hint}", path.display()))
        })?;
        let config = Self::parse(&contents)?;
        config.validate()?;
        Ok(config)
    }

    pub fn parse(contents: &str) -> Result<Self, WorkerError> {
        toml::from_str(contents)
            .map_err(|error| WorkerError::Config(format!("invalid TOML configuration: {error}")))
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.version != 1 {
            return Err(WorkerError::Config(format!(
                "unsupported configuration version {}",
                self.version
            )));
        }
        if self.workers.is_empty() {
            return Err(WorkerError::Config(
                "at least one worker is required".into(),
            ));
        }

        let mut names = HashSet::new();
        let mut destinations = HashSet::new();
        for worker in &self.workers {
            if !valid_identifier(&worker.name) {
                return Err(WorkerError::Config(format!(
                    "invalid worker name {:?}",
                    worker.name
                )));
            }
            if !names.insert(&worker.name) {
                return Err(WorkerError::Config(format!(
                    "duplicate worker name {:?}",
                    worker.name
                )));
            }
            if !valid_ssh_destination(&worker.ssh) {
                return Err(WorkerError::Config(format!(
                    "invalid SSH destination {:?}",
                    worker.ssh
                )));
            }
            if !destinations.insert(&worker.ssh) {
                return Err(WorkerError::Config(format!(
                    "duplicate SSH destination {:?}",
                    worker.ssh
                )));
            }
            if worker.slots != 1 {
                return Err(WorkerError::Config(format!(
                    "worker {:?} must declare exactly one slot",
                    worker.name
                )));
            }
            if worker.remote_binary != REMOTE_BINARY {
                return Err(WorkerError::Config(format!(
                    "worker {:?} must use remote_binary {REMOTE_BINARY:?}",
                    worker.name
                )));
            }

            let mut capabilities = HashSet::new();
            for capability in &worker.capabilities {
                if !capabilities.insert(capability) {
                    return Err(WorkerError::Config(format!(
                        "worker {:?} declares duplicate capability {:?}",
                        worker.name, capability
                    )));
                }
            }
        }

        Ok(())
    }

    pub fn worker(&self, name: &str) -> Option<&WorkerEntry> {
        self.workers.iter().find(|worker| worker.name == name)
    }
}

fn default_remote_binary() -> String {
    REMOTE_BINARY.into()
}

pub(crate) fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'@'))
}

pub(crate) fn valid_ssh_destination(value: &str) -> bool {
    value
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphanumeric)
        && valid_identifier(value)
}
