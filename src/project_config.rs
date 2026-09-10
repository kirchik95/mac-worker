use std::{
    collections::{BTreeMap, HashSet},
    fs::{self, OpenOptions},
    io::{self, Read},
    os::unix::fs::OpenOptionsExt,
    path::{Component, Path},
    time::Duration,
};

use globset::Glob;
use serde::Deserialize;

use crate::error::WorkerError;

pub(crate) const PROJECT_CONFIG: &str = ".worker.toml";
const DEFAULT_TIMEOUT: &str = "30m";
const DEFAULT_SETUP_TIMEOUT: &str = "10m";
const MAX_SETUP_COMMANDS: usize = 16;
const MAX_SETUP_COMMAND_BYTES: usize = 4096;
const MAX_SETUP_LOCKFILES: usize = 32;
const MAX_SETUP_INPUTS: usize = 32;
const PROJECT_CONFIG_OPEN_FLAGS: libc::c_int =
    libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectSettings {
    pub requires: Vec<String>,
    pub resource_class: ResourceClass,
    pub timeout: Duration,
    pub snapshot: SnapshotSettings,
    pub artifacts: ArtifactSettings,
    pub task: TaskSettings,
    pub setup: Option<SetupSettings>,
}

/// The part of `.worker.toml` that controls the phase-five task client.
/// Values stay textual at this boundary so project configuration remains
/// independent from the wire/task model; the client maps them to the typed
/// task enums after all project policy has been validated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSettings {
    pub source: String,
    pub publish: Vec<String>,
    pub env_profile: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub default_agent: String,
    pub timeout: Duration,
    pub max_followups: u32,
    pub permissions: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotSettings {
    pub include_untracked: Vec<String>,
    pub include_empty_dirs: Vec<String>,
    pub allow_sensitive: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceClass {
    Heavy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactSettings {
    pub include: Vec<String>,
    pub max_total_bytes: Option<u64>,
}

/// Optional project setup recipe. Absent means no commands and no cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupSettings {
    pub timeout: Duration,
    pub commands: Vec<String>,
    pub check: Option<String>,
    pub lockfiles: Vec<String>,
    pub inputs: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawProjectSettings {
    #[serde(default = "default_version")]
    version: u32,
    #[serde(default)]
    requires: Vec<String>,
    #[serde(default = "default_resource_class")]
    resource_class: String,
    #[serde(default = "default_timeout")]
    timeout: String,
    #[serde(default)]
    snapshot: RawSnapshotSettings,
    #[serde(default)]
    artifacts: RawArtifactSettings,
    #[serde(default)]
    task: RawTaskSettings,
    #[serde(default)]
    setup: Option<RawSetupSettings>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSnapshotSettings {
    #[serde(default)]
    include_untracked: Vec<String>,
    #[serde(default)]
    include_empty_dirs: Vec<String>,
    #[serde(default)]
    allow_sensitive: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawArtifactSettings {
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    max_total_bytes: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSetupSettings {
    #[serde(default = "default_setup_timeout")]
    timeout: String,
    #[serde(default)]
    commands: Vec<String>,
    #[serde(default)]
    check: Option<String>,
    #[serde(default)]
    lockfiles: Vec<String>,
    #[serde(default)]
    inputs: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTaskSettings {
    #[serde(default = "default_task_source")]
    source: String,
    #[serde(default = "default_task_publish")]
    publish: Vec<String>,
    #[serde(default)]
    env_profile: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    effort: Option<String>,
    #[serde(default = "default_task_agent")]
    default_agent: String,
    #[serde(default = "default_task_timeout")]
    timeout: String,
    #[serde(default = "default_task_max_followups")]
    max_followups: u32,
    #[serde(default = "default_task_permissions")]
    permissions: BTreeMap<String, String>,
}

impl Default for RawTaskSettings {
    fn default() -> Self {
        Self {
            source: default_task_source(),
            publish: default_task_publish(),
            env_profile: None,
            model: None,
            effort: None,
            default_agent: default_task_agent(),
            timeout: default_task_timeout(),
            max_followups: default_task_max_followups(),
            permissions: default_task_permissions(),
        }
    }
}

impl ProjectSettings {
    pub fn load(root: &Path, cli_includes: &[String]) -> Result<Self, WorkerError> {
        let raw = match read_project_config(root)? {
            Some(contents) => toml::from_str(&contents).map_err(|_| {
                if contents.contains("[task]") {
                    task_config("invalid [task] configuration")
                } else {
                    WorkerError::Config("invalid project configuration".into())
                }
            })?,
            None => default_project_settings(),
        };

        Self::validate(root, raw, cli_includes)
    }

    fn validate(
        root: &Path,
        raw: RawProjectSettings,
        cli_includes: &[String],
    ) -> Result<Self, WorkerError> {
        if raw.version != 1 {
            return Err(WorkerError::Config(
                "unsupported project configuration version".into(),
            ));
        }
        validate_requirements(&raw.requires)?;

        let resource_class = match raw.resource_class.as_str() {
            "heavy" => ResourceClass::Heavy,
            _ => {
                return Err(WorkerError::Config("unsupported resource class".into()));
            }
        };
        let timeout = humantime::parse_duration(&raw.timeout)
            .map_err(|_| WorkerError::Config("invalid project timeout".into()))?;

        validate_patterns(
            &raw.snapshot.include_untracked,
            "snapshot include_untracked",
        )?;
        validate_exact_paths(
            &raw.snapshot.include_empty_dirs,
            "snapshot include_empty_dirs",
        )?;
        validate_exact_paths(&raw.snapshot.allow_sensitive, "snapshot allow_sensitive")?;
        validate_patterns(&raw.artifacts.include, "artifact include")?;
        validate_patterns(cli_includes, "CLI include")?;

        let task = validate_task_settings(raw.task)?;
        let setup = raw.setup.map(validate_setup_settings).transpose()?;
        if let Some(setup) = &setup {
            verify_setup_lockfiles_exist(root, setup)?;
        }

        Ok(Self {
            requires: raw.requires,
            resource_class,
            timeout,
            snapshot: SnapshotSettings {
                include_untracked: merge_preserving_first(
                    raw.snapshot.include_untracked,
                    cli_includes,
                ),
                include_empty_dirs: raw.snapshot.include_empty_dirs,
                allow_sensitive: raw.snapshot.allow_sensitive,
            },
            artifacts: ArtifactSettings {
                include: raw.artifacts.include,
                max_total_bytes: raw.artifacts.max_total_bytes,
            },
            task,
            setup,
        })
    }
}

fn read_project_config(root: &Path) -> Result<Option<String>, WorkerError> {
    let path = root.join(PROJECT_CONFIG);
    let mut file = match OpenOptions::new()
        .read(true)
        .custom_flags(PROJECT_CONFIG_OPEN_FLAGS)
        .open(&path)
    {
        Ok(file) => file,
        Err(error) => return classify_project_config_open_error(&path, error),
    };
    let metadata = file.metadata().map_err(WorkerError::Io)?;
    if !metadata.file_type().is_file() {
        return Err(unsafe_project_config());
    }

    let mut contents = String::new();
    match file.read_to_string(&mut contents) {
        Ok(_) => Ok(Some(contents)),
        Err(error) if error.kind() == io::ErrorKind::InvalidData => {
            Err(WorkerError::Config("invalid project configuration".into()))
        }
        Err(error) => Err(WorkerError::Io(error)),
    }
}

fn classify_project_config_open_error(
    path: &Path,
    error: io::Error,
) -> Result<Option<String>, WorkerError> {
    if error.raw_os_error() == Some(libc::ELOOP) {
        return Err(unsafe_project_config());
    }

    match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.file_type().is_file() => Err(unsafe_project_config()),
        Ok(_) => Err(WorkerError::Io(error)),
        Err(metadata_error)
            if error.kind() == io::ErrorKind::NotFound
                && metadata_error.kind() == io::ErrorKind::NotFound =>
        {
            Ok(None)
        }
        Err(_) => Err(WorkerError::Io(error)),
    }
}

fn unsafe_project_config() -> WorkerError {
    WorkerError::Config("project configuration must be a regular file".into())
}

fn default_project_settings() -> RawProjectSettings {
    RawProjectSettings {
        version: default_version(),
        resource_class: default_resource_class(),
        timeout: default_timeout(),
        ..RawProjectSettings::default()
    }
}

fn default_version() -> u32 {
    1
}

fn default_resource_class() -> String {
    "heavy".to_owned()
}

fn default_timeout() -> String {
    DEFAULT_TIMEOUT.to_owned()
}

fn default_setup_timeout() -> String {
    DEFAULT_SETUP_TIMEOUT.to_owned()
}

fn default_task_source() -> String {
    "local".to_owned()
}

fn default_task_publish() -> Vec<String> {
    vec!["fetch".to_owned()]
}

fn default_task_agent() -> String {
    "codex".to_owned()
}

fn default_task_timeout() -> String {
    "45m".to_owned()
}

fn default_task_max_followups() -> u32 {
    10
}

fn default_task_permissions() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("codex".to_owned(), "workspace".to_owned()),
        ("claude".to_owned(), "unattended".to_owned()),
        ("cursor".to_owned(), "unattended".to_owned()),
        ("opencode".to_owned(), "unattended".to_owned()),
    ])
}

fn validate_setup_settings(raw: RawSetupSettings) -> Result<SetupSettings, WorkerError> {
    let timeout = humantime::parse_duration(&raw.timeout)
        .map_err(|_| WorkerError::Config("invalid setup timeout".into()))?;
    if timeout.is_zero() || timeout > Duration::from_secs(24 * 60 * 60) {
        return Err(WorkerError::Config(
            "setup timeout must be greater than zero and at most 24h".into(),
        ));
    }
    if raw.commands.len() > MAX_SETUP_COMMANDS {
        return Err(WorkerError::Config("too many setup commands".into()));
    }
    if raw.lockfiles.len() > MAX_SETUP_LOCKFILES {
        return Err(WorkerError::Config("too many setup lockfiles".into()));
    }
    if raw.inputs.len() > MAX_SETUP_INPUTS {
        return Err(WorkerError::Config("too many setup inputs".into()));
    }
    for command in &raw.commands {
        validate_setup_command(command, "setup command")?;
    }
    if let Some(check) = &raw.check {
        validate_setup_command(check, "setup check")?;
    }
    let lockfiles = unique_setup_paths(raw.lockfiles, "setup lockfile")?;
    let inputs = unique_setup_paths(raw.inputs, "setup input")?;
    Ok(SetupSettings {
        timeout,
        commands: raw.commands,
        check: raw.check,
        lockfiles,
        inputs,
    })
}

fn validate_setup_command(command: &str, field: &str) -> Result<(), WorkerError> {
    if command.is_empty()
        || command.len() > MAX_SETUP_COMMAND_BYTES
        || command.chars().any(char::is_control)
    {
        return Err(WorkerError::Config(format!(
            "{field} is empty, too long, or contains a control character"
        )));
    }
    Ok(())
}

fn unique_setup_paths(paths: Vec<String>, field: &str) -> Result<Vec<String>, WorkerError> {
    let mut seen = HashSet::new();
    for path in &paths {
        validate_relative_path(path, field)?;
        if contains_glob_metacharacter(path) {
            return Err(WorkerError::Config(format!(
                "{field} path must not contain glob metacharacters"
            )));
        }
        if !seen.insert(path) {
            return Err(WorkerError::Config(format!("duplicate {field}")));
        }
    }
    Ok(paths)
}

fn verify_setup_lockfiles_exist(root: &Path, setup: &SetupSettings) -> Result<(), WorkerError> {
    for path in setup.lockfiles.iter().chain(setup.inputs.iter()) {
        let metadata = fs::symlink_metadata(root.join(path)).map_err(|_| {
            WorkerError::Config("setup lockfile or input is missing or not a regular file".into())
        })?;
        if !metadata.file_type().is_file() {
            return Err(WorkerError::Config(
                "setup lockfile or input is missing or not a regular file".into(),
            ));
        }
    }
    Ok(())
}

fn validate_task_settings(raw: RawTaskSettings) -> Result<TaskSettings, WorkerError> {
    if !matches!(raw.source.as_str(), "local" | "origin") {
        return Err(task_config(
            "task source must be local or origin (TASK_CONFIG_INVALID)",
        ));
    }
    let mut publish = Vec::new();
    for mode in raw.publish {
        if !matches!(mode.as_str(), "fetch" | "push") {
            return Err(task_config(
                "task publish mode must be fetch or push (TASK_CONFIG_INVALID)",
            ));
        }
        if !publish.iter().any(|existing| existing == &mode) {
            publish.push(mode);
        }
    }
    if !publish.iter().any(|mode| mode == "fetch") {
        return Err(task_config(
            "publish fetch is required for every task (TASK_CONFIG_INVALID)",
        ));
    }
    validate_task_text(&raw.default_agent, "task default agent")?;
    if let Some(profile) = &raw.env_profile {
        validate_task_text(profile, "task environment profile")?;
    }
    if let Some(model) = &raw.model {
        validate_task_text(model, "task model")?;
    }
    if let Some(effort) = &raw.effort {
        crate::agent::validate_effort(effort)
            .map_err(|error| task_config(format!("task effort {error} (TASK_CONFIG_INVALID)")))?;
    }
    let timeout = humantime::parse_duration(&raw.timeout)
        .map_err(|_| task_config("task timeout must be a valid duration (TASK_CONFIG_INVALID)"))?;
    if timeout.is_zero() || timeout > Duration::from_secs(24 * 60 * 60) {
        return Err(task_config(
            "task timeout must be greater than zero and at most 24h (TASK_CONFIG_INVALID)",
        ));
    }
    if raw.max_followups > 100 {
        return Err(task_config(
            "task max_followups must be at most 100 (TASK_CONFIG_INVALID)",
        ));
    }
    for (agent, policy) in &raw.permissions {
        validate_task_text(agent, "task permission agent")?;
        if !matches!(policy.as_str(), "workspace" | "unattended") {
            return Err(task_config(
                "task permission must be workspace or unattended (TASK_CONFIG_INVALID)",
            ));
        }
    }
    Ok(TaskSettings {
        source: raw.source,
        publish,
        env_profile: raw.env_profile,
        model: raw.model,
        effort: raw.effort,
        default_agent: raw.default_agent,
        timeout,
        max_followups: raw.max_followups,
        permissions: raw.permissions,
    })
}

fn validate_task_text(value: &str, field: &str) -> Result<(), WorkerError> {
    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(task_config(format!(
            "{field} is empty, too long, or contains a control character (TASK_CONFIG_INVALID)"
        )));
    }
    Ok(())
}

fn task_config(message: impl Into<std::borrow::Cow<'static, str>>) -> WorkerError {
    WorkerError::task("TASK_CONFIG_INVALID", message)
}

fn validate_requirements(requires: &[String]) -> Result<(), WorkerError> {
    let mut seen = HashSet::new();
    for requirement in requires {
        if !valid_capability(requirement) {
            return Err(WorkerError::Config("invalid required capability".into()));
        }
        if !seen.insert(requirement) {
            return Err(WorkerError::Config("duplicate required capability".into()));
        }
    }
    Ok(())
}

fn valid_capability(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

fn validate_patterns(patterns: &[String], field: &str) -> Result<(), WorkerError> {
    for pattern in patterns {
        validate_relative_path(pattern, field)?;
        if first_component(pattern).is_some_and(contains_glob_metacharacter) {
            return Err(WorkerError::Config(format!(
                "{field} pattern must begin with a literal path component"
            )));
        }
        Glob::new(pattern).map_err(|_| WorkerError::Config(format!("invalid {field} pattern")))?;
    }
    Ok(())
}

fn validate_exact_paths(paths: &[String], field: &str) -> Result<(), WorkerError> {
    for path in paths {
        validate_relative_path(path, field)?;
        if contains_glob_metacharacter(path) {
            return Err(WorkerError::Config(format!(
                "{field} path must not contain glob metacharacters"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_preview_path(value: &str) -> Result<(), WorkerError> {
    validate_relative_path(value, "batch files")
}

fn validate_relative_path(value: &str, field: &str) -> Result<(), WorkerError> {
    if value.is_empty()
        || value.starts_with('/')
        || value.ends_with('/')
        || value
            .split('/')
            .any(|part| part == "." || part == ".." || part.is_empty())
    {
        return Err(WorkerError::Config(format!(
            "{field} path must be a precise relative path"
        )));
    }

    for component in Path::new(value).components() {
        if !matches!(component, Component::Normal(_)) {
            return Err(WorkerError::Config(format!(
                "{field} path contains a forbidden path component"
            )));
        }
    }
    Ok(())
}

fn first_component(value: &str) -> Option<&str> {
    value.split('/').next()
}

fn contains_glob_metacharacter(value: &str) -> bool {
    value
        .bytes()
        .any(|byte| matches!(byte, b'*' | b'?' | b'[' | b']' | b'{' | b'}' | b'!' | b'\\'))
}

fn merge_preserving_first(configured: Vec<String>, cli: &[String]) -> Vec<String> {
    let mut merged = Vec::new();
    let mut seen = HashSet::new();
    for include in configured.iter().chain(cli) {
        if seen.insert(include) {
            merged.push(include.clone());
        }
    }
    merged
}
