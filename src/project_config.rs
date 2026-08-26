use std::{
    collections::HashSet,
    fs,
    path::{Component, Path},
    time::Duration,
};

use globset::Glob;
use serde::Deserialize;

use crate::error::WorkerError;

const PROJECT_CONFIG: &str = ".worker.toml";
const DEFAULT_TIMEOUT: &str = "30m";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectSettings {
    pub requires: Vec<String>,
    pub resource_class: ResourceClass,
    pub timeout: Duration,
    pub snapshot: SnapshotSettings,
    pub artifacts: ArtifactSettings,
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

impl ProjectSettings {
    pub fn load(root: &Path, cli_includes: &[String]) -> Result<Self, WorkerError> {
        let config_path = root.join(PROJECT_CONFIG);
        let raw = match fs::read_to_string(&config_path) {
            Ok(contents) => toml::from_str(&contents).map_err(|error| {
                WorkerError::Config(format!(
                    "invalid project configuration {}: {error}",
                    config_path.display()
                ))
            })?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => RawProjectSettings {
                version: default_version(),
                resource_class: default_resource_class(),
                timeout: default_timeout(),
                ..RawProjectSettings::default()
            },
            Err(error) => {
                return Err(WorkerError::Config(format!(
                    "failed to read {}: {error}",
                    config_path.display()
                )));
            }
        };

        Self::validate(raw, cli_includes)
    }

    fn validate(raw: RawProjectSettings, cli_includes: &[String]) -> Result<Self, WorkerError> {
        if raw.version != 1 {
            return Err(WorkerError::Config(format!(
                "unsupported project configuration version {}",
                raw.version
            )));
        }
        validate_requirements(&raw.requires)?;

        let resource_class = match raw.resource_class.as_str() {
            "heavy" => ResourceClass::Heavy,
            _ => {
                return Err(WorkerError::Config(format!(
                    "unsupported resource class {:?}",
                    raw.resource_class
                )));
            }
        };
        let timeout = humantime::parse_duration(&raw.timeout).map_err(|error| {
            WorkerError::Config(format!(
                "invalid project timeout {:?}: {error}",
                raw.timeout
            ))
        })?;

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
        })
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

fn validate_requirements(requires: &[String]) -> Result<(), WorkerError> {
    let mut seen = HashSet::new();
    for requirement in requires {
        if !valid_capability(requirement) {
            return Err(WorkerError::Config(format!(
                "invalid required capability {requirement:?}"
            )));
        }
        if !seen.insert(requirement) {
            return Err(WorkerError::Config(format!(
                "duplicate required capability {requirement:?}"
            )));
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
                "{field} pattern {pattern:?} must begin with a literal path component"
            )));
        }
        Glob::new(pattern).map_err(|error| {
            WorkerError::Config(format!("invalid {field} pattern {pattern:?}: {error}"))
        })?;
    }
    Ok(())
}

fn validate_exact_paths(paths: &[String], field: &str) -> Result<(), WorkerError> {
    for path in paths {
        validate_relative_path(path, field)?;
        if contains_glob_metacharacter(path) {
            return Err(WorkerError::Config(format!(
                "{field} path {path:?} must not contain glob metacharacters"
            )));
        }
    }
    Ok(())
}

fn validate_relative_path(value: &str, field: &str) -> Result<(), WorkerError> {
    if value.is_empty()
        || value.starts_with('/')
        || value.split('/').any(|part| part == "." || part == "..")
    {
        return Err(WorkerError::Config(format!(
            "{field} path {value:?} must be a precise relative path"
        )));
    }

    for component in Path::new(value).components() {
        if !matches!(component, Component::Normal(_)) {
            return Err(WorkerError::Config(format!(
                "{field} path {value:?} contains a forbidden path component"
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
