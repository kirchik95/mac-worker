use std::{
    collections::{BTreeMap, HashSet},
    fs,
    path::Path,
    sync::Mutex,
};

use serde::{Deserialize, Serialize};

use crate::error::WorkerError;

const REMOTE_BINARY: &str = "~/.local/bin/worker";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    #[serde(default)]
    pub notifications: NotificationsConfig,
    #[serde(default)]
    pub workers: Vec<WorkerEntry>,
    #[serde(default)]
    pub controller: ControllerConfig,
    /// SSH client options for every mac-worker connection. Absent tables keep
    /// direct connections, so existing files stay valid.
    #[serde(default)]
    pub ssh: SshConfig,
}

/// Laptop and controller SSH client behavior.
///
/// `multiplex` is off unless the file sets it. A ControlMaster changes how
/// connection failures show up, and a stuck master must not become the only
/// path to the workers, so the default stays a direct `ssh` for each call.
/// Turn it on for a host that opens many short sessions, especially the
/// controller. See the SSH section of `docs/usage.md`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct SshConfig {
    #[serde(default)]
    pub multiplex: bool,
}

static INSTALLED_SSH: Mutex<SshConfig> = Mutex::new(SshConfig { multiplex: false });

pub(crate) fn installed_ssh() -> SshConfig {
    INSTALLED_SSH
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

fn install_ssh(ssh: &SshConfig) {
    *INSTALLED_SSH
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = ssh.clone();
}

/// Laptop opt-in for a remote persistent controller reached over authenticated SSH.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub ssh: String,
    #[serde(default = "default_remote_binary")]
    pub remote_binary: String,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ssh: String::new(),
            remote_binary: default_remote_binary(),
        }
    }
}

/// Laptop-side notifications about finished turns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
            // The path stays in Display for logs. The public code selects a
            // static catalog hint and is the only text the CLI prints.
            let code = if error.kind() == std::io::ErrorKind::NotFound {
                "CONFIG_MISSING"
            } else {
                "CONFIG"
            };
            WorkerError::Config(format!(
                "{code}: failed to read {}: {error}",
                path.display()
            ))
        })?;
        let config = Self::parse(&contents)?;
        config.validate()?;
        install_ssh(&config.ssh);
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

        self.controller.validate()?;
        if self.workers.is_empty() {
            if self.controller.enabled {
                return Ok(());
            }
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
            if worker.slots == 0 || worker.slots > crate::lease::MAX_HOST_SLOTS {
                return Err(WorkerError::Config(format!(
                    "worker {:?} slots must be between 1 and {}",
                    worker.name,
                    crate::lease::MAX_HOST_SLOTS
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

    /// Local `run`, `setup`, and `workers` still need the laptop inventory.
    /// Controller-only configs omit `[[workers]]`; the controller host owns
    /// the dispatch list.
    pub fn require_local_inventory(&self) -> Result<(), WorkerError> {
        if self.workers.is_empty() {
            return Err(WorkerError::Config(
                "at least one worker is required".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn with_workers(&self, workers: Vec<WorkerEntry>) -> Self {
        Self {
            version: self.version,
            notifications: self.notifications.clone(),
            workers,
            controller: self.controller.clone(),
            ssh: self.ssh.clone(),
        }
    }

    pub fn configured_runner_slots(&self) -> usize {
        self.workers
            .iter()
            .map(|worker| usize::from(worker.slots))
            .sum()
    }

    pub fn worker_slot_ceilings(&self) -> BTreeMap<String, u8> {
        self.workers
            .iter()
            .map(|worker| (worker.name.clone(), worker.slots))
            .collect()
    }
}

impl ControllerConfig {
    fn validate(&self) -> Result<(), WorkerError> {
        if self.remote_binary != REMOTE_BINARY {
            return Err(WorkerError::Config(format!(
                "controller must use remote_binary {REMOTE_BINARY:?}"
            )));
        }
        if !self.enabled && self.ssh.is_empty() {
            return Ok(());
        }
        if !valid_ssh_destination(&self.ssh) {
            return Err(WorkerError::Config(format!(
                "invalid controller SSH destination {:?}",
                self.ssh
            )));
        }
        Ok(())
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

/// Atomically switch only controller settings while preserving the laptop inventory.
/// The expected text fences concurrent edits; the stable per-config lock serializes
/// cooperating writers. Descriptor-relative operations reject symlink substitution.
pub fn write_controller_mode(
    path: &Path,
    expected: &str,
    controller: &ControllerConfig,
) -> Result<(), WorkerError> {
    use std::{
        ffi::CString,
        io::Write,
        os::fd::{AsRawFd, FromRawFd},
    };
    let absolute = std::path::absolute(path)?;
    let parent = absolute
        .parent()
        .ok_or_else(|| WorkerError::Config("config parent missing".into()))?;
    let name = absolute
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| WorkerError::Config("invalid config filename".into()))?;
    let root = crate::rooted_fs::RootedDir::open_anchored_absolute(parent)?;
    let _lock =
        crate::controller::provision::exclusive_lock(&root, &format!("{name}.controller.lock"))?;
    let current = crate::controller::provision::read_optional(&root, name)?
        .ok_or_else(|| WorkerError::Config("config disappeared during controller setup".into()))?;
    if current != expected.as_bytes() {
        return Err(WorkerError::Config(
            "config changed during controller setup; rerun the command".into(),
        ));
    }
    let mut document: toml::Value =
        toml::from_str(expected).map_err(|_| WorkerError::Config("invalid config".into()))?;
    document
        .as_table_mut()
        .ok_or_else(|| WorkerError::Config("invalid config root".into()))?
        .insert(
            "controller".into(),
            toml::Value::try_from(controller)
                .map_err(|_| WorkerError::Config("invalid controller config".into()))?,
        );
    let text = toml::to_string_pretty(&document)
        .map_err(|_| WorkerError::Config("invalid config".into()))?;
    Config::parse(&text)?.validate()?;
    if current == text.as_bytes() {
        return Ok(());
    }
    let temporary = format!(".controller-config-{}", uuid::Uuid::new_v4().simple());
    let tmp = CString::new(temporary).unwrap();
    let target =
        CString::new(name).map_err(|_| WorkerError::Config("invalid config filename".into()))?;
    let fd = unsafe {
        libc::openat(
            root.raw_directory_fd(),
            tmp.as_ptr(),
            libc::O_CREAT | libc::O_EXCL | libc::O_WRONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let mut file = unsafe { fs::File::from_raw_fd(fd) };
    let write = (|| -> Result<(), WorkerError> {
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        root.verify_bound()?;
        if root.read_private_regular(name, 1024 * 1024)? != current {
            return Err(WorkerError::Config(
                "config changed during controller setup".into(),
            ));
        }
        if unsafe {
            libc::renameat(
                root.raw_directory_fd(),
                tmp.as_ptr(),
                root.raw_directory_fd(),
                target.as_ptr(),
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        if unsafe { libc::fsync(root.raw_directory_fd()) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    })();
    if write.is_err() {
        unsafe {
            libc::unlinkat(root.raw_directory_fd(), tmp.as_ptr(), 0);
        }
    }
    // Keep the retained writer descriptor alive through directory fsync.
    let _ = file.as_raw_fd();
    write
}

#[cfg(test)]
mod tests {
    use super::Config;

    fn controller_only(extra: &str) -> String {
        format!("version = 1\n[controller]\nenabled = true\nssh = \"mac1\"\n{extra}")
    }

    #[test]
    fn ssh_section_defaults_off_and_parses_multiplex() {
        let absent = Config::parse(&controller_only("")).unwrap();
        assert!(!absent.ssh.multiplex);
        let enabled = Config::parse(&controller_only("[ssh]\nmultiplex = true\n")).unwrap();
        assert!(enabled.ssh.multiplex);
        let disabled = Config::parse(&controller_only("[ssh]\nmultiplex = false\n")).unwrap();
        assert!(!disabled.ssh.multiplex);
        assert!(Config::parse(&controller_only("[ssh]\nunknown = true\n")).is_err());
    }
}
