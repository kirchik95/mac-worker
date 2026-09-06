//! Native per-agent model and effort defaults.
//!
//! This module deliberately exposes only the small allowlisted projection used by
//! the dashboard.  Native configuration documents are read with bounded sizes,
//! edited textually where possible, and replaced atomically so unrelated settings
//! and comments survive a save.

use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[cfg(not(unix))]
use std::fs::OpenOptions;

use crate::rooted_fs::RootedDir;

pub const SETTINGS_AGENT_IDS: [&str; 4] = ["codex", "cursor", "opencode", "claude"];
pub const MAX_SETTINGS_SOURCE_BYTES: usize = 1024 * 1024;
pub const MAX_SETTINGS_MODEL_BYTES: usize = 256;
pub const MAX_SETTINGS_EFFORT_BYTES: usize = 32;
pub const MAX_SETTINGS_REVISION_BYTES: usize = 128;

// Cursor exposes effort as a model-specific parameter but the installed CLI
// does not publish a stable global enum.  Keep the current value readable and
// leave the field uneditable until a connected model advertises its choices.
const CURSOR_EFFORT_OPTIONS: &[&str] = &[];
const CLAUDE_EFFORT_OPTIONS: &[&str] = &["low", "medium", "high", "xhigh", "max"];
const MISSING_REVISION_MARKER: &[u8] = b"mac-worker-agent-settings-missing-v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentDefaultSettings {
    pub agent: String,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub effort_options: Vec<String>,
    pub source: String,
    pub revision: Option<String>,
    pub writable: bool,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentSettingsList {
    pub agents: Vec<AgentDefaultSettings>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AgentSettingsSaveRequest {
    pub agent: String,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub revision: String,
}

impl<'de> Deserialize<'de> for AgentSettingsSaveRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            agent: String,
            model: Option<Option<String>>,
            effort: Option<Option<String>>,
            revision: String,
        }

        let wire = Wire::deserialize(deserializer)?;
        let model = wire
            .model
            .ok_or_else(|| serde::de::Error::missing_field("model"))?;
        let effort = wire
            .effort
            .ok_or_else(|| serde::de::Error::missing_field("effort"))?;
        Ok(Self {
            agent: wire.agent,
            model,
            effort,
            revision: wire.revision,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct AgentSettingsGetRequest {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentKind {
    Codex,
    Cursor,
    Opencode,
    Claude,
}

impl AgentKind {
    fn parse(value: &str) -> Result<Self, AgentSettingsError> {
        match value {
            "codex" => Ok(Self::Codex),
            "cursor" => Ok(Self::Cursor),
            "opencode" => Ok(Self::Opencode),
            "claude" => Ok(Self::Claude),
            _ => Err(AgentSettingsError::unknown_agent()),
        }
    }

    fn id(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Cursor => "cursor",
            Self::Opencode => "opencode",
            Self::Claude => "claude",
        }
    }

    fn source(self) -> &'static str {
        match self {
            Self::Codex => "native-codex",
            Self::Cursor => "native-cursor",
            Self::Opencode => "native-opencode",
            Self::Claude => "native-claude",
        }
    }

    fn static_effort_options(self) -> Vec<String> {
        let values = match self {
            // Codex reasoning levels are model-dependent. The native store
            // reads the worker's bounded models_cache.json instead of using
            // a process-local list.
            Self::Codex => &[],
            Self::Cursor => CURSOR_EFFORT_OPTIONS,
            Self::Opencode => &[],
            Self::Claude => CLAUDE_EFFORT_OPTIONS,
        };
        values.iter().map(|value| (*value).to_owned()).collect()
    }
}

#[derive(Debug, Clone)]
pub struct NativeAgentSettingsStore {
    home: PathBuf,
    environment: BTreeMap<OsString, OsString>,
}

impl NativeAgentSettingsStore {
    pub fn new(home: impl Into<PathBuf>) -> Self {
        let home = home.into();
        Self {
            // The worker home is the one trusted bootstrap path. Resolve it
            // once, while all native descendants remain no-follow checked.
            home: fs::canonicalize(&home).unwrap_or(home),
            environment: BTreeMap::new(),
        }
    }

    pub fn with_environment(mut self, environment: BTreeMap<OsString, OsString>) -> Self {
        self.environment = environment;
        self
    }

    pub fn read_all(&self) -> AgentSettingsList {
        AgentSettingsList {
            agents: SETTINGS_AGENT_IDS
                .iter()
                .map(|agent| match self.read(agent) {
                    Ok(entry) => entry,
                    Err(error) => self.unavailable_entry(agent, &error),
                })
                .collect(),
        }
    }

    pub fn read(&self, agent: &str) -> Result<AgentDefaultSettings, AgentSettingsError> {
        let kind = AgentKind::parse(agent)?;
        if kind == AgentKind::Opencode {
            return self.read_opencode();
        }
        let path = self.path(kind);
        let source = read_source(&path)?;
        let revision = Some(source_revision(kind.id(), source.as_deref()));
        let writable = source_writable(&path, source.as_deref());
        let (model, effort, mut message) = match source.as_deref() {
            None => (
                None,
                None,
                Some("native configuration file is not present".to_owned()),
            ),
            Some(bytes) => {
                let values = self.parse_values(kind, bytes)?;
                (values.model, values.effort, values.message)
            }
        };
        let effort_options = self.effort_options(kind, model.as_deref());
        if message.is_none()
            && kind == AgentKind::Codex
            && model.is_some()
            && effort_options.is_empty()
        {
            message = Some("verified effort choices are unavailable for this model".to_owned());
        } else if message.is_none()
            && kind == AgentKind::Cursor
            && effort.is_some()
            && effort_options.is_empty()
        {
            message = Some("effort choices are not published by this native CLI".to_owned());
        }
        Ok(AgentDefaultSettings {
            agent: kind.id().to_owned(),
            model,
            effort,
            effort_options,
            source: kind.source().to_owned(),
            revision,
            writable,
            message,
        })
    }

    pub fn save(
        &self,
        request: &AgentSettingsSaveRequest,
    ) -> Result<AgentDefaultSettings, AgentSettingsError> {
        validate_save_request(request)?;
        let kind = AgentKind::parse(&request.agent)?;
        if kind == AgentKind::Opencode {
            return self.save_opencode(request);
        }

        let path = self.path(kind);
        // A missing document plus an explicit removal is a true no-op. Do it
        // before creating a parent directory or lock file.
        let initial = read_source(&path)?;
        let initial_revision = source_revision(kind.id(), initial.as_deref());
        if request.revision != initial_revision {
            return Err(AgentSettingsError::conflict());
        }
        if initial.is_none() && request.model.is_none() && request.effort.is_none() {
            return self.read(kind.id());
        }
        ensure_parent_chain(&path)?;
        let _lock = SettingsLock::acquire(&path)?;
        let current = read_source(&path)?;
        let current_revision = source_revision(kind.id(), current.as_deref());
        if request.revision != current_revision {
            return Err(AgentSettingsError::conflict());
        }
        if current.is_some() && !source_writable(&path, current.as_deref()) {
            return Err(AgentSettingsError::unavailable(
                "native settings are not writable",
            ));
        }

        let parsed = current
            .as_deref()
            .map(|bytes| self.parse_values(kind, bytes))
            .transpose()?;
        let target_model = request
            .model
            .as_deref()
            .or_else(|| parsed.as_ref().and_then(|values| values.model.as_deref()));
        let effort_options = self.effort_options(kind, target_model);
        let unchanged_cursor_effort = kind == AgentKind::Cursor
            && request.model.as_deref()
                == parsed.as_ref().and_then(|values| values.model.as_deref())
            && request.effort.as_deref()
                == parsed.as_ref().and_then(|values| values.effort.as_deref());
        if request.effort.as_deref().is_some_and(|effort| {
            !effort_options.iter().any(|value| value == effort) && !unchanged_cursor_effort
        }) {
            return Err(AgentSettingsError::invalid(
                "requested effort is not supported by this native agent",
            ));
        }

        let replacement = match current.as_deref() {
            Some(bytes) => self.edit_source(kind, bytes, request)?,
            None => self.create_source(kind, request)?,
        };
        if let Some(bytes) = replacement {
            write_atomic(&path, &bytes, current.as_deref())?;
        }
        self.read(kind.id())
    }

    fn effort_options(&self, kind: AgentKind, model: Option<&str>) -> Vec<String> {
        match kind {
            AgentKind::Codex => self.codex_effort_options(model),
            _ => kind.static_effort_options(),
        }
    }

    fn codex_effort_options(&self, model: Option<&str>) -> Vec<String> {
        let Some(model) = model else {
            return Vec::new();
        };
        let path = self.home.join(".codex/models_cache.json");
        let Ok(Some(bytes)) = read_source(&path) else {
            return Vec::new();
        };
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
            return Vec::new();
        };
        let Some(models) = value.get("models").and_then(serde_json::Value::as_array) else {
            return Vec::new();
        };
        let Some(model_entry) = models
            .iter()
            .find(|entry| entry.get("slug").and_then(serde_json::Value::as_str) == Some(model))
        else {
            return Vec::new();
        };
        let Some(levels) = model_entry
            .get("supported_reasoning_levels")
            .and_then(serde_json::Value::as_array)
        else {
            return Vec::new();
        };
        let mut options = Vec::new();
        for level in levels {
            let value = level
                .as_str()
                .or_else(|| level.get("effort").and_then(serde_json::Value::as_str));
            let Some(value) = value else { continue };
            if value.is_empty()
                || value.len() > MAX_SETTINGS_EFFORT_BYTES
                || value
                    .chars()
                    .any(|character| character.is_control() || character.is_whitespace())
                || options.iter().any(|candidate| candidate == value)
            {
                continue;
            }
            options.push(value.to_owned());
        }
        options
    }

    fn unavailable_entry(&self, agent: &str, error: &AgentSettingsError) -> AgentDefaultSettings {
        let kind = AgentKind::parse(agent).expect("allowlisted agent");
        let path = self.path(kind);
        let revision = read_source(&path)
            .ok()
            .flatten()
            .map(|bytes| source_revision(kind.id(), Some(&bytes)));
        AgentDefaultSettings {
            agent: agent.to_owned(),
            model: None,
            effort: None,
            effort_options: kind.static_effort_options(),
            source: kind.source().to_owned(),
            revision,
            writable: false,
            message: Some(error.safe_message().to_owned()),
        }
    }

    fn path(&self, kind: AgentKind) -> PathBuf {
        match kind {
            AgentKind::Codex => self.home.join(".codex/config.toml"),
            AgentKind::Cursor => self.home.join(".cursor/cli-config.json"),
            AgentKind::Claude => self.home.join(".claude/settings.json"),
            AgentKind::Opencode => self.opencode_paths().1,
        }
    }

    fn opencode_paths(&self) -> (PathBuf, PathBuf) {
        let config_home = self
            .environment
            .get(OsStr::new("XDG_CONFIG_HOME"))
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .unwrap_or_else(|| self.home.join(".config"));
        (
            config_home.join("opencode/opencode.json"),
            config_home.join("opencode/opencode.jsonc"),
        )
    }

    fn read_opencode(&self) -> Result<AgentDefaultSettings, AgentSettingsError> {
        let (json, jsonc) = self.opencode_paths();
        let plain = read_source(&json)?;
        let commented = read_source(&jsonc)?;
        let jsonc_has_model = commented
            .as_deref()
            .map(|bytes| json_root_has_key(bytes, "model"))
            .transpose()?
            .unwrap_or(false);
        let plain_has_model = plain
            .as_deref()
            .map(|bytes| json_root_has_key(bytes, "model"))
            .transpose()?
            .unwrap_or(false);
        let model = effective_json_string(plain.as_deref(), commented.as_deref(), "model")?;
        validate_native_optional_text(model.as_deref(), MAX_SETTINGS_MODEL_BYTES, "model")?;
        let revision = Some(source_revision_multi(
            "opencode",
            &[("json", plain.as_deref()), ("jsonc", commented.as_deref())],
        ));
        let target = opencode_target(&json, &jsonc, plain.as_deref(), commented.as_deref())?;
        let target_source = if target == json {
            plain.as_deref()
        } else {
            commented.as_deref()
        };
        let writable = source_writable(target, target_source);
        let message = if plain.is_none() && commented.is_none() {
            Some("native configuration file is not present".to_owned())
        } else if commented.is_some() && plain_has_model && !jsonc_has_model {
            Some("model is inherited from the lower-precedence JSON configuration".to_owned())
        } else {
            None
        };
        Ok(AgentDefaultSettings {
            agent: "opencode".to_owned(),
            model,
            effort: None,
            effort_options: Vec::new(),
            source: AgentKind::Opencode.source().to_owned(),
            revision,
            writable,
            message,
        })
    }

    fn save_opencode(
        &self,
        request: &AgentSettingsSaveRequest,
    ) -> Result<AgentDefaultSettings, AgentSettingsError> {
        let (json, jsonc) = self.opencode_paths();
        let plain = read_source(&json)?;
        let commented = read_source(&jsonc)?;
        let current_revision = source_revision_multi(
            "opencode",
            &[("json", plain.as_deref()), ("jsonc", commented.as_deref())],
        );
        if request.revision != current_revision {
            return Err(AgentSettingsError::conflict());
        }
        if plain.is_none()
            && commented.is_none()
            && request.model.is_none()
            && request.effort.is_none()
        {
            return self.read_opencode();
        }
        ensure_parent_chain(&json)?;
        let _lock = SettingsLock::acquire(&json)?;
        let plain = read_source(&json)?;
        let commented = read_source(&jsonc)?;
        let current_revision = source_revision_multi(
            "opencode",
            &[("json", plain.as_deref()), ("jsonc", commented.as_deref())],
        );
        if request.revision != current_revision {
            return Err(AgentSettingsError::conflict());
        }
        let target = opencode_target(&json, &jsonc, plain.as_deref(), commented.as_deref())?;
        let current_target = read_source(target)?;
        if current_target.is_some() && !source_writable(target, current_target.as_deref()) {
            return Err(AgentSettingsError::unavailable(
                "native settings are not writable",
            ));
        }
        let replacement = match current_target.as_deref() {
            Some(bytes) => edit_json_root(bytes, request, false)?,
            None => edit_json_root(b"{}\n", request, false)?,
        };
        if replacement != current_target.as_deref().unwrap_or_default() {
            write_atomic(target, &replacement, current_target.as_deref())?;
        }
        self.read_opencode()
    }

    fn parse_values(
        &self,
        kind: AgentKind,
        bytes: &[u8],
    ) -> Result<ParsedValues, AgentSettingsError> {
        let values = match kind {
            AgentKind::Codex => parse_codex(bytes),
            AgentKind::Cursor => parse_cursor(bytes),
            AgentKind::Opencode => parse_json_agent(bytes, false),
            AgentKind::Claude => parse_json_agent(bytes, true),
        }?;
        validate_parsed_values(&values)?;
        Ok(values)
    }

    fn edit_source(
        &self,
        kind: AgentKind,
        bytes: &[u8],
        request: &AgentSettingsSaveRequest,
    ) -> Result<Option<Vec<u8>>, AgentSettingsError> {
        let replacement = match kind {
            AgentKind::Codex => edit_codex(bytes, request)?,
            AgentKind::Cursor => edit_cursor(bytes, request)?,
            AgentKind::Opencode => edit_json_root(bytes, request, false)?,
            AgentKind::Claude => edit_json_root(bytes, request, true)?,
        };
        Ok((replacement != bytes).then_some(replacement))
    }

    fn create_source(
        &self,
        kind: AgentKind,
        request: &AgentSettingsSaveRequest,
    ) -> Result<Option<Vec<u8>>, AgentSettingsError> {
        if request.model.is_none() && request.effort.is_none() {
            return Ok(None);
        }
        let empty = match kind {
            AgentKind::Codex => Vec::new(),
            _ => b"{}\n".to_vec(),
        };
        self.edit_source(kind, &empty, request)
    }
}

#[derive(Debug, Clone)]
struct ParsedValues {
    model: Option<String>,
    effort: Option<String>,
    message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentSettingsError {
    UnknownAgent,
    InvalidRequest(&'static str),
    InvalidConfig(&'static str),
    Unavailable(&'static str),
    Conflict,
}

impl AgentSettingsError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::UnknownAgent | Self::InvalidRequest(_) => "SETTINGS_INVALID",
            Self::InvalidConfig(_) => "SETTINGS_INVALID_CONFIG",
            Self::Unavailable(_) => "SETTINGS_UNAVAILABLE",
            Self::Conflict => "SETTINGS_CONFLICT",
        }
    }

    pub fn safe_message(&self) -> &'static str {
        match self {
            Self::UnknownAgent => "agent is not supported",
            Self::InvalidRequest(message) => message,
            Self::InvalidConfig(message) => message,
            Self::Unavailable(message) => message,
            Self::Conflict => "native settings changed; refresh and retry",
        }
    }

    fn unknown_agent() -> Self {
        Self::UnknownAgent
    }

    fn invalid(message: &'static str) -> Self {
        Self::InvalidRequest(message)
    }

    fn config(message: &'static str) -> Self {
        Self::InvalidConfig(message)
    }

    fn unavailable(message: &'static str) -> Self {
        Self::Unavailable(message)
    }

    fn conflict() -> Self {
        Self::Conflict
    }
}

impl std::fmt::Display for AgentSettingsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code(), self.safe_message())
    }
}

impl std::error::Error for AgentSettingsError {}

pub fn validate_save_request(request: &AgentSettingsSaveRequest) -> Result<(), AgentSettingsError> {
    let kind = AgentKind::parse(&request.agent)?;
    if request.revision.is_empty() || request.revision.len() > MAX_SETTINGS_REVISION_BYTES {
        return Err(AgentSettingsError::invalid("settings revision is invalid"));
    }
    if !request
        .revision
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(AgentSettingsError::invalid("settings revision is invalid"));
    }
    validate_optional_text(request.model.as_deref(), MAX_SETTINGS_MODEL_BYTES, "model")?;
    validate_optional_text(
        request.effort.as_deref(),
        MAX_SETTINGS_EFFORT_BYTES,
        "effort",
    )?;
    let static_options = kind.static_effort_options();
    if request.effort.as_deref().is_some_and(|effort| {
        !static_options.is_empty() && !static_options.iter().any(|value| value == effort)
    }) {
        return Err(AgentSettingsError::invalid(
            "requested effort is not supported by this native agent",
        ));
    }
    if request.effort.is_some() && kind == AgentKind::Opencode {
        return Err(AgentSettingsError::invalid(
            "requested effort is not supported by this native agent",
        ));
    }
    Ok(())
}

fn validate_optional_text(
    value: Option<&str>,
    max_bytes: usize,
    field: &'static str,
) -> Result<(), AgentSettingsError> {
    let Some(value) = value else { return Ok(()) };
    if value.is_empty()
        || value.len() > max_bytes
        || value
            .chars()
            .any(|character| character.is_control() || character == '\n' || character == '\r')
        || value.trim().is_empty()
    {
        return Err(AgentSettingsError::invalid(match field {
            "model" => "model must be a bounded single-line value",
            _ => "effort must be a bounded single-line value",
        }));
    }
    Ok(())
}

fn validate_native_optional_text(
    value: Option<&str>,
    max_bytes: usize,
    field: &'static str,
) -> Result<(), AgentSettingsError> {
    let Some(value) = value else { return Ok(()) };
    if value.is_empty()
        || value.len() > max_bytes
        || value
            .chars()
            .any(|character| character.is_control() || character == '\n' || character == '\r')
        || value.trim().is_empty()
    {
        return Err(AgentSettingsError::config(match field {
            "model" => "native model value is invalid",
            _ => "native effort value is invalid",
        }));
    }
    Ok(())
}

fn validate_parsed_values(values: &ParsedValues) -> Result<(), AgentSettingsError> {
    validate_native_optional_text(values.model.as_deref(), MAX_SETTINGS_MODEL_BYTES, "model")?;
    validate_native_optional_text(
        values.effort.as_deref(),
        MAX_SETTINGS_EFFORT_BYTES,
        "effort",
    )
}

fn read_source(path: &Path) -> Result<Option<Vec<u8>>, AgentSettingsError> {
    if !parent_chain_is_safe(path) {
        return Err(AgentSettingsError::unavailable(
            "native configuration path has an unsafe parent",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;

        let Some(mut file) = open_native_source(path)? else {
            return Ok(None);
        };
        let before = native_fd_stat(file.as_raw_fd())?;
        if !native_fd_stat_is_safe(&before) {
            return Err(AgentSettingsError::unavailable(
                "native configuration has unsafe ownership or permissions",
            ));
        }
        if before.size > MAX_SETTINGS_SOURCE_BYTES as u64 {
            return Err(AgentSettingsError::config(
                "native configuration exceeds the supported size",
            ));
        }
        let mut bytes = Vec::new();
        (&mut file)
            .take((MAX_SETTINGS_SOURCE_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| AgentSettingsError::unavailable("native configuration is unavailable"))?;
        if bytes.len() > MAX_SETTINGS_SOURCE_BYTES {
            return Err(AgentSettingsError::config(
                "native configuration exceeds the supported size",
            ));
        }
        let after = native_fd_stat(file.as_raw_fd())?;
        if !native_fd_stat_is_safe(&after) || !native_fd_stat_matches(&before, &after) {
            return Err(AgentSettingsError::conflict());
        }
        Ok(Some(bytes))
    }
    #[cfg(not(unix))]
    {
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => {
                return Err(AgentSettingsError::unavailable(
                    "native configuration is unavailable",
                ));
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(AgentSettingsError::unavailable(
                "native configuration is not a regular file",
            ));
        }
        if !native_file_metadata_is_safe(&metadata) {
            return Err(AgentSettingsError::unavailable(
                "native configuration has unsafe ownership or permissions",
            ));
        }
        let mut file = File::open(path)
            .map_err(|_| AgentSettingsError::unavailable("native configuration is unavailable"))?;
        let mut bytes = Vec::new();
        (&mut file)
            .take((MAX_SETTINGS_SOURCE_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| AgentSettingsError::unavailable("native configuration is unavailable"))?;
        if bytes.len() > MAX_SETTINGS_SOURCE_BYTES {
            return Err(AgentSettingsError::config(
                "native configuration exceeds the supported size",
            ));
        }
        Ok(Some(bytes))
    }
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy)]
struct NativeFdStat {
    device: libc::dev_t,
    inode: libc::ino_t,
    mode: libc::mode_t,
    uid: libc::uid_t,
    nlink: libc::nlink_t,
    size: u64,
}

#[cfg(unix)]
fn open_native_source(path: &Path) -> Result<Option<File>, AgentSettingsError> {
    use std::ffi::CString;
    use std::os::fd::FromRawFd;
    use std::os::unix::ffi::OsStrExt;

    let parent = path.parent().ok_or_else(|| {
        AgentSettingsError::unavailable("native configuration path is unavailable")
    })?;
    let directory = match RootedDir::open_anchored_absolute(parent) {
        Ok(directory) => directory,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(AgentSettingsError::unavailable(
                "native configuration path has an unsafe parent",
            ));
        }
    };
    directory
        .verify_bound_with_policy(native_directory_stat_is_safe)
        .map_err(|_| {
            AgentSettingsError::unavailable("native configuration path has an unsafe parent")
        })?;
    let name = CString::new(
        path.file_name()
            .ok_or_else(|| {
                AgentSettingsError::unavailable("native configuration path is unavailable")
            })?
            .as_bytes(),
    )
    .map_err(|_| AgentSettingsError::unavailable("native configuration path is unavailable"))?;
    let flags = libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;
    let descriptor = unsafe { libc::openat(directory.raw_directory_fd(), name.as_ptr(), flags) };
    if descriptor < 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::NotFound {
            return Ok(None);
        }
        return Err(AgentSettingsError::unavailable(
            "native configuration is unavailable",
        ));
    }
    Ok(Some(unsafe { File::from_raw_fd(descriptor) }))
}

#[cfg(unix)]
fn anchored_parent(path: &Path) -> Result<RootedDir, AgentSettingsError> {
    let parent = path.parent().ok_or_else(|| {
        AgentSettingsError::unavailable("native settings directory is unavailable")
    })?;
    let directory = RootedDir::open_anchored_absolute(parent)
        .map_err(|_| AgentSettingsError::unavailable("native settings directory is unavailable"))?;
    directory
        .verify_bound_with_policy(native_directory_stat_is_safe)
        .map_err(|_| {
            AgentSettingsError::unavailable("native settings directory has an unsafe parent")
        })?;
    Ok(directory)
}

#[cfg(unix)]
fn native_stat_at(
    directory: std::os::fd::RawFd,
    name: &std::ffi::CStr,
) -> Result<Option<NativeFdStat>, AgentSettingsError> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe {
        libc::fstatat(
            directory,
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::NotFound {
            return Ok(None);
        }
        return Err(AgentSettingsError::unavailable(
            "native configuration is unavailable",
        ));
    }
    let stat = unsafe { stat.assume_init() };
    Ok(Some(native_stat_from_raw(&stat)))
}

#[cfg(unix)]
fn native_stat_from_raw(stat: &libc::stat) -> NativeFdStat {
    NativeFdStat {
        device: stat.st_dev,
        inode: stat.st_ino,
        mode: stat.st_mode,
        uid: stat.st_uid,
        nlink: stat.st_nlink,
        size: stat.st_size.max(0) as u64,
    }
}

#[cfg(unix)]
fn native_name(path: &Path) -> Result<std::ffi::CString, AgentSettingsError> {
    use std::os::unix::ffi::OsStrExt;
    std::ffi::CString::new(
        path.file_name()
            .ok_or_else(|| {
                AgentSettingsError::unavailable("native configuration path is unavailable")
            })?
            .as_bytes(),
    )
    .map_err(|_| AgentSettingsError::unavailable("native configuration path is unavailable"))
}

#[cfg(unix)]
fn native_fd_stat(descriptor: std::os::fd::RawFd) -> Result<NativeFdStat, AgentSettingsError> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(descriptor, stat.as_mut_ptr()) } != 0 {
        return Err(AgentSettingsError::unavailable(
            "native configuration is unavailable",
        ));
    }
    let stat = unsafe { stat.assume_init() };
    Ok(native_stat_from_raw(&stat))
}

#[cfg(unix)]
fn native_fd_stat_is_safe(stat: &NativeFdStat) -> bool {
    (stat.mode & libc::S_IFMT) == libc::S_IFREG
        && stat.uid == unsafe { libc::geteuid() }
        && stat.nlink == 1
        && stat.mode & 0o022 == 0
}

#[cfg(unix)]
fn native_fd_stat_matches(left: &NativeFdStat, right: &NativeFdStat) -> bool {
    left.device == right.device
        && left.inode == right.inode
        && left.mode == right.mode
        && left.uid == right.uid
        && left.nlink == right.nlink
        && left.size == right.size
}

fn native_file_metadata_is_safe(metadata: &fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        metadata.uid() == unsafe { libc::geteuid() }
            && metadata.nlink() == 1
            && metadata.mode() & 0o022 == 0
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        true
    }
}

fn native_directory_metadata_is_safe(metadata: &fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let owner = metadata.uid();
        let effective_owner = unsafe { libc::geteuid() };
        if owner != effective_owner && owner != 0 {
            return false;
        }
        let mode = metadata.permissions().mode();
        mode & 0o022 == 0 || (mode & 0o1000 != 0 && mode & 0o002 != 0)
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        true
    }
}

#[cfg(unix)]
fn native_directory_stat_is_safe(stat: &libc::stat) -> bool {
    let owner = stat.st_uid;
    let effective_owner = unsafe { libc::geteuid() };
    if owner != effective_owner && owner != 0 {
        return false;
    }
    let mode = stat.st_mode;
    (mode & libc::S_IFMT) == libc::S_IFDIR
        && (mode & 0o022 == 0 || (mode & 0o1000 != 0 && mode & 0o002 != 0))
}

fn source_writable(path: &Path, source: Option<&[u8]>) -> bool {
    if !parent_chain_is_safe(path) {
        return false;
    }
    let Some(_source) = source else {
        return true;
    };
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return false;
    };
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || !native_file_metadata_is_safe(&metadata)
    {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o200 != 0
    }
    #[cfg(not(unix))]
    {
        !metadata.permissions().readonly()
    }
}

fn parent_chain_is_safe(path: &Path) -> bool {
    #[cfg(target_vendor = "apple")]
    let normalized;
    #[cfg(target_vendor = "apple")]
    let path = if let Ok(suffix) = path.strip_prefix("/var") {
        normalized = PathBuf::from("/private/var").join(suffix);
        normalized.as_path()
    } else if let Ok(suffix) = path.strip_prefix("/tmp") {
        normalized = PathBuf::from("/private/tmp").join(suffix);
        normalized.as_path()
    } else {
        path
    };
    let Some(parent) = path.parent() else {
        return false;
    };
    let mut current = parent;
    loop {
        match fs::symlink_metadata(current) {
            Ok(metadata)
                if metadata.is_dir()
                    && !metadata.file_type().is_symlink()
                    && native_directory_metadata_is_safe(&metadata) => {}
            Ok(_) => return false,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // A missing directory may be created by save; continue to the
                // first existing ancestor and validate every component there.
            }
            Err(_) => return false,
        }
        current = match current.parent() {
            Some(parent) => parent,
            None => return true,
        };
    }
}

fn ensure_parent_chain(path: &Path) -> Result<(), AgentSettingsError> {
    let Some(parent) = path.parent() else {
        return Err(AgentSettingsError::unavailable(
            "native settings directory is unavailable",
        ));
    };
    if !parent_chain_is_safe(path) {
        return Err(AgentSettingsError::unavailable(
            "native settings directory has an unsafe parent",
        ));
    }
    #[cfg(unix)]
    {
        let directory = RootedDir::open_or_create_anchored_absolute(parent).map_err(|_| {
            AgentSettingsError::unavailable("native settings directory could not be created")
        })?;
        directory
            .verify_bound_with_policy(native_directory_stat_is_safe)
            .map_err(|_| {
                AgentSettingsError::unavailable("native settings directory has an unsafe parent")
            })?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let mut missing = Vec::new();
        let mut current = parent;
        loop {
            match fs::symlink_metadata(current) {
                Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => break,
                Ok(_) => {
                    return Err(AgentSettingsError::unavailable(
                        "native settings directory is not a directory",
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    missing.push(current.to_owned());
                    current = current.parent().ok_or_else(|| {
                        AgentSettingsError::unavailable("native settings directory is unavailable")
                    })?;
                }
                Err(_) => {
                    return Err(AgentSettingsError::unavailable(
                        "native settings directory is unavailable",
                    ));
                }
            }
        }
        for directory in missing.into_iter().rev() {
            fs::create_dir(&directory).map_err(|_| {
                AgentSettingsError::unavailable("native settings directory could not be created")
            })?;
        }
        Ok(())
    }
}

struct SettingsLock {
    #[cfg(unix)]
    directory: RootedDir,
    #[cfg(unix)]
    name: std::ffi::CString,
    #[cfg(not(unix))]
    path: PathBuf,
}

impl SettingsLock {
    fn acquire(config_path: &Path) -> Result<Self, AgentSettingsError> {
        #[cfg(unix)]
        {
            use std::io::ErrorKind;
            use std::os::fd::FromRawFd;

            let directory = anchored_parent(config_path)?;
            let name = config_path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| {
                    AgentSettingsError::unavailable("native settings file is unavailable")
                })?;
            let name = std::ffi::CString::new(format!(".{name}.mac-worker-settings.lock"))
                .map_err(|_| {
                    AgentSettingsError::unavailable("native settings lock is unavailable")
                })?;
            let flags = libc::O_WRONLY
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK;
            let descriptor =
                unsafe { libc::openat(directory.raw_directory_fd(), name.as_ptr(), flags, 0o600) };
            if descriptor < 0 {
                let error = std::io::Error::last_os_error();
                return Err(if error.kind() == ErrorKind::AlreadyExists {
                    AgentSettingsError::unavailable("native settings are busy")
                } else {
                    AgentSettingsError::unavailable("native settings lock is unavailable")
                });
            }
            let mut file = unsafe { File::from_raw_fd(descriptor) };
            let _ = file.write_all(std::process::id().to_string().as_bytes());
            let _ = file.sync_all();
            Ok(Self { directory, name })
        }
        #[cfg(not(unix))]
        {
            let parent = config_path.parent().ok_or_else(|| {
                AgentSettingsError::unavailable("native settings directory is unavailable")
            })?;
            let name = config_path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| {
                    AgentSettingsError::unavailable("native settings file is unavailable")
                })?;
            let path = parent.join(format!(".{name}.mac-worker-settings.lock"));
            let mut file = OpenOptions::new();
            file.write(true).create_new(true);
            let mut file = file.open(&path).map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    AgentSettingsError::unavailable("native settings are busy")
                } else {
                    AgentSettingsError::unavailable("native settings lock is unavailable")
                }
            })?;
            let _ = file.write_all(std::process::id().to_string().as_bytes());
            let _ = file.sync_all();
            Ok(Self { path })
        }
    }
}

impl Drop for SettingsLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            unsafe {
                libc::unlinkat(self.directory.raw_directory_fd(), self.name.as_ptr(), 0);
            }
        }
        #[cfg(not(unix))]
        let _ = fs::remove_file(&self.path);
    }
}

fn write_atomic(
    path: &Path,
    bytes: &[u8],
    previous: Option<&[u8]>,
) -> Result<(), AgentSettingsError> {
    if bytes.len() > MAX_SETTINGS_SOURCE_BYTES {
        return Err(AgentSettingsError::config(
            "native configuration exceeds the supported size",
        ));
    }
    #[cfg(not(unix))]
    let parent = path.parent().ok_or_else(|| {
        AgentSettingsError::unavailable("native settings directory is unavailable")
    })?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| AgentSettingsError::unavailable("native settings file is unavailable"))?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();

    #[cfg(unix)]
    {
        use std::os::fd::FromRawFd;
        use std::os::unix::fs::PermissionsExt;

        let directory = anchored_parent(path)?;
        let name = native_name(path)?;
        let existing = native_stat_at(directory.raw_directory_fd(), &name)?;
        let existing_mode = existing.map(|stat| stat.mode & 0o7777);
        match previous {
            Some(previous) => {
                let stat = existing.ok_or_else(AgentSettingsError::conflict)?;
                if !native_fd_stat_is_safe(&stat) {
                    return Err(AgentSettingsError::unavailable(
                        "native configuration has unsafe ownership or permissions",
                    ));
                }
                let current = read_source(path)
                    .map_err(|_| AgentSettingsError::conflict())?
                    .ok_or_else(AgentSettingsError::conflict)?;
                if current != previous {
                    return Err(AgentSettingsError::conflict());
                }
            }
            None if existing.is_some() => return Err(AgentSettingsError::conflict()),
            None => {}
        }
        let temporary_name =
            std::ffi::CString::new(format!(".{file_name}.mac-worker-settings-{nonce:x}.tmp"))
                .map_err(|_| {
                    AgentSettingsError::unavailable("native settings temporary file is unavailable")
                })?;
        let flags = libc::O_WRONLY
            | libc::O_CREAT
            | libc::O_EXCL
            | libc::O_CLOEXEC
            | libc::O_NOFOLLOW
            | libc::O_NONBLOCK;
        let descriptor = unsafe {
            libc::openat(
                directory.raw_directory_fd(),
                temporary_name.as_ptr(),
                flags,
                0o600,
            )
        };
        if descriptor < 0 {
            return Err(AgentSettingsError::unavailable(
                "native settings temporary file is unavailable",
            ));
        }
        let mut file = unsafe { File::from_raw_fd(descriptor) };
        let mode = u32::from(existing_mode.unwrap_or(0o600));
        let _ = file.set_permissions(fs::Permissions::from_mode(mode));
        if let Err(error) = file.write_all(bytes).and_then(|()| file.sync_all()) {
            unsafe {
                libc::unlinkat(directory.raw_directory_fd(), temporary_name.as_ptr(), 0);
            }
            return Err(AgentSettingsError::unavailable(
                if error.kind() == std::io::ErrorKind::PermissionDenied {
                    "native settings are not writable"
                } else {
                    "native settings could not be written"
                },
            ));
        }
        drop(file);
        if directory
            .verify_bound_with_policy(native_directory_stat_is_safe)
            .is_err()
        {
            unsafe {
                libc::unlinkat(directory.raw_directory_fd(), temporary_name.as_ptr(), 0);
            }
            return Err(AgentSettingsError::unavailable(
                "native settings directory was replaced",
            ));
        }
        if unsafe {
            libc::renameat(
                directory.raw_directory_fd(),
                temporary_name.as_ptr(),
                directory.raw_directory_fd(),
                name.as_ptr(),
            )
        } != 0
        {
            unsafe {
                libc::unlinkat(directory.raw_directory_fd(), temporary_name.as_ptr(), 0);
            }
            return Err(AgentSettingsError::unavailable(
                "native settings could not be replaced",
            ));
        }
        let _ = unsafe { libc::fsync(directory.raw_directory_fd()) };
        Ok(())
    }

    #[cfg(not(unix))]
    {
        match previous {
            Some(previous) => {
                let metadata =
                    fs::symlink_metadata(path).map_err(|_| AgentSettingsError::conflict())?;
                if metadata.file_type().is_symlink() || !metadata.is_file() {
                    return Err(AgentSettingsError::unavailable(
                        "native configuration is not a regular file",
                    ));
                }
                let current = read_source(path)
                    .map_err(|_| AgentSettingsError::conflict())?
                    .ok_or_else(AgentSettingsError::conflict)?;
                if current != previous {
                    return Err(AgentSettingsError::conflict());
                }
            }
            None => match fs::symlink_metadata(path) {
                Ok(_) => return Err(AgentSettingsError::conflict()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => {
                    return Err(AgentSettingsError::unavailable(
                        "native configuration is unavailable",
                    ));
                }
            },
        }
        let temporary = parent.join(format!(".{file_name}.mac-worker-settings-{nonce:x}.tmp"));
        let mut file = OpenOptions::new();
        file.write(true).create_new(true).truncate(true);
        let mut file = file.open(&temporary).map_err(|_| {
            AgentSettingsError::unavailable("native settings temporary file is unavailable")
        })?;
        if let Err(error) = file.write_all(bytes).and_then(|()| file.sync_all()) {
            let _ = fs::remove_file(&temporary);
            return Err(AgentSettingsError::unavailable(
                if error.kind() == std::io::ErrorKind::PermissionDenied {
                    "native settings are not writable"
                } else {
                    "native settings could not be written"
                },
            ));
        }
        drop(file);
        fs::rename(&temporary, path).map_err(|_| {
            let _ = fs::remove_file(&temporary);
            AgentSettingsError::unavailable("native settings could not be replaced")
        })?;
        if let Ok(directory) = File::open(parent) {
            let _ = directory.sync_all();
        }
        Ok(())
    }
}

fn source_revision(agent: &str, bytes: Option<&[u8]>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"mac-worker-agent-settings-v1\0");
    hasher.update(agent.as_bytes());
    match bytes {
        Some(bytes) => {
            hasher.update(b"\0present\0");
            hasher.update(bytes);
        }
        None => {
            hasher.update(b"\0missing\0");
            hasher.update(MISSING_REVISION_MARKER);
        }
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn source_revision_multi(agent: &str, sources: &[(&str, Option<&[u8]>)]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"mac-worker-agent-settings-v1\0");
    hasher.update(agent.as_bytes());
    for (name, bytes) in sources {
        hasher.update(b"\0");
        hasher.update(name.as_bytes());
        match bytes {
            Some(bytes) => {
                hasher.update(b"\0present\0");
                hasher.update(bytes);
            }
            None => {
                hasher.update(b"\0missing\0");
                hasher.update(MISSING_REVISION_MARKER);
            }
        }
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn opencode_target<'a>(
    json: &'a Path,
    jsonc: &'a Path,
    plain: Option<&[u8]>,
    commented: Option<&[u8]>,
) -> Result<&'a Path, AgentSettingsError> {
    let jsonc_has_model = commented
        .map(|bytes| json_root_has_key(bytes, "model"))
        .transpose()?
        .unwrap_or(false);
    let plain_has_model = plain
        .map(|bytes| json_root_has_key(bytes, "model"))
        .transpose()?
        .unwrap_or(false);
    if jsonc_has_model || (commented.is_some() && !plain_has_model) {
        Ok(jsonc)
    } else {
        Ok(json)
    }
}

fn json_root_has_key(bytes: &[u8], key: &str) -> Result<bool, AgentSettingsError> {
    let value = parse_unique_json(bytes)?;
    let object = value
        .0
        .as_object()
        .ok_or_else(|| AgentSettingsError::config("native configuration is malformed"))?;
    Ok(object.contains_key(key))
}

fn effective_json_string(
    plain: Option<&[u8]>,
    commented: Option<&[u8]>,
    key: &str,
) -> Result<Option<String>, AgentSettingsError> {
    let lower = plain
        .map(parse_unique_json)
        .transpose()?
        .and_then(|value| value.0.as_object().cloned());
    let upper = commented
        .map(parse_unique_json)
        .transpose()?
        .and_then(|value| value.0.as_object().cloned());
    let selected = upper
        .as_ref()
        .and_then(|object| object.get(key))
        .or_else(|| lower.as_ref().and_then(|object| object.get(key)));
    optional_json_string(selected)
}

fn parse_unique_json(bytes: &[u8]) -> Result<UniqueJsonValue, AgentSettingsError> {
    let text = jsonc_to_json(bytes)?;
    serde_json::from_slice(&text)
        .map_err(|_| AgentSettingsError::config("native configuration is malformed"))
}

fn parse_codex(bytes: &[u8]) -> Result<ParsedValues, AgentSettingsError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| AgentSettingsError::config("native configuration is not valid UTF-8"))?;
    let value: toml::Value = toml::from_str(text)
        .map_err(|_| AgentSettingsError::config("native configuration is malformed"))?;
    let table = value
        .as_table()
        .ok_or_else(|| AgentSettingsError::config("native configuration is malformed"))?;
    let model = optional_toml_string(table.get("model"))?;
    let effort = optional_toml_string(table.get("model_reasoning_effort"))?;
    Ok(ParsedValues {
        model,
        effort,
        message: None,
    })
}

fn optional_toml_string(value: Option<&toml::Value>) -> Result<Option<String>, AgentSettingsError> {
    value
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| AgentSettingsError::config("native setting has an unsupported type"))
        })
        .transpose()
}

fn parse_json_agent(bytes: &[u8], _claude: bool) -> Result<ParsedValues, AgentSettingsError> {
    let text = jsonc_to_json(bytes)?;
    let value: UniqueJsonValue = serde_json::from_slice(&text)
        .map_err(|_| AgentSettingsError::config("native configuration is malformed"))?;
    let object = value
        .0
        .as_object()
        .ok_or_else(|| AgentSettingsError::config("native configuration is malformed"))?;
    let model = optional_json_string(object.get("model"))?;
    let effort = if _claude {
        optional_json_string(object.get("effortLevel"))?
    } else {
        None
    };
    Ok(ParsedValues {
        model,
        effort,
        message: None,
    })
}

fn parse_cursor(bytes: &[u8]) -> Result<ParsedValues, AgentSettingsError> {
    let text = jsonc_to_json(bytes)?;
    let value: UniqueJsonValue = serde_json::from_slice(&text)
        .map_err(|_| AgentSettingsError::config("native configuration is malformed"))?;
    let object = value
        .0
        .as_object()
        .ok_or_else(|| AgentSettingsError::config("native configuration is malformed"))?;
    for key in ["model", "selectedModel"] {
        if object.get(key).is_some_and(|value| !value.is_object()) {
            return Err(AgentSettingsError::config(
                "native model selection has an unsupported type",
            ));
        }
    }
    if object
        .get("modelParameters")
        .is_some_and(|value| !value.is_object())
    {
        return Err(AgentSettingsError::config(
            "native model parameters have an unsupported type",
        ));
    }
    let nested_model = object
        .get("model")
        .and_then(|value| value.as_object())
        .and_then(|value| value.get("modelId"));
    let selected_model = object
        .get("selectedModel")
        .and_then(|value| value.as_object())
        .and_then(|value| value.get("modelId"));
    let model = match (nested_model, selected_model) {
        (Some(left), Some(right)) if left != right => {
            return Err(AgentSettingsError::config(
                "native model selection is ambiguous",
            ));
        }
        (Some(value), _) | (_, Some(value)) => optional_json_string(Some(value))?,
        _ => None,
    };
    let selected_effort = selected_parameter(object.get("selectedModel"))?;
    let model_effort = model
        .as_deref()
        .map(|model| {
            let values = object
                .get("modelParameters")
                .and_then(|value| value.as_object())
                .and_then(|values| values.get(model));
            cached_parameter(values)
        })
        .transpose()?
        .flatten();
    let effort = match (selected_effort, model_effort) {
        (Some(left), Some(right)) if left != right => {
            return Err(AgentSettingsError::config(
                "native effort selection is ambiguous",
            ));
        }
        (Some(value), _) | (_, Some(value)) => Some(value),
        _ => None,
    };
    Ok(ParsedValues {
        model,
        effort,
        message: None,
    })
}

fn optional_json_string(
    value: Option<&serde_json::Value>,
) -> Result<Option<String>, AgentSettingsError> {
    value
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| AgentSettingsError::config("native setting has an unsupported type"))
        })
        .transpose()
}

fn selected_parameter(
    value: Option<&serde_json::Value>,
) -> Result<Option<String>, AgentSettingsError> {
    let Some(value) = value else { return Ok(None) };
    let Some(object) = value.as_object() else {
        return Err(AgentSettingsError::config(
            "native model selection has an unsupported type",
        ));
    };
    parameter_effort(object.get("parameters"))
}

fn cached_parameter(
    value: Option<&serde_json::Value>,
) -> Result<Option<String>, AgentSettingsError> {
    parameter_effort(value)
}

fn parameter_effort(
    value: Option<&serde_json::Value>,
) -> Result<Option<String>, AgentSettingsError> {
    let Some(value) = value else { return Ok(None) };
    let Some(parameters) = value.as_array() else {
        return Err(AgentSettingsError::config(
            "native model parameters have an unsupported type",
        ));
    };
    let mut effort = None;
    for parameter in parameters {
        let Some(parameter) = parameter.as_object() else {
            return Err(AgentSettingsError::config(
                "native model parameters have an unsupported type",
            ));
        };
        if parameter.get("id").and_then(serde_json::Value::as_str) != Some("effort") {
            continue;
        }
        if effort.is_some() {
            return Err(AgentSettingsError::config(
                "native effort selection is ambiguous",
            ));
        }
        effort = Some(
            optional_json_string(parameter.get("value"))?.ok_or_else(|| {
                AgentSettingsError::config("native model parameters have an unsupported type")
            })?,
        );
    }
    Ok(effort)
}

fn edit_codex(
    bytes: &[u8],
    request: &AgentSettingsSaveRequest,
) -> Result<Vec<u8>, AgentSettingsError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| AgentSettingsError::config("native configuration is not valid UTF-8"))?;
    let _: toml::Value = toml::from_str(text)
        .map_err(|_| AgentSettingsError::config("native configuration is malformed"))?;
    let text = edit_toml_key(text, "model", request.model.as_deref())?;
    let text = edit_toml_key(&text, "model_reasoning_effort", request.effort.as_deref())?;
    Ok(text.into_bytes())
}

fn edit_toml_key(text: &str, key: &str, value: Option<&str>) -> Result<String, AgentSettingsError> {
    let mut section = false;
    let mut multiline = None;
    let mut matches = Vec::new();
    let mut offset = 0usize;
    for line in text.split_inclusive('\n') {
        let without_newline = line.strip_suffix('\n').unwrap_or(line);
        let trimmed = without_newline.trim_start();
        let in_multiline = multiline.is_some();
        if !in_multiline && trimmed.starts_with('[') {
            section = true;
        }
        if !in_multiline
            && !section
            && !trimmed.is_empty()
            && !trimmed.starts_with('#')
            && let Some((raw_key, _equal)) = trimmed.split_once('=')
            && toml_key_matches(raw_key.trim(), key)
        {
            let key_start = offset + (without_newline.len() - trimmed.len());
            let equal_index = trimmed.find('=').unwrap_or(raw_key.len());
            let mut value_start = key_start + equal_index + 1;
            while value_start < offset + without_newline.len()
                && text.as_bytes()[value_start].is_ascii_whitespace()
            {
                value_start += 1;
            }
            let comment = find_toml_comment(without_newline, value_start.saturating_sub(offset))
                .map(|comment| offset + comment);
            matches.push((offset, offset + without_newline.len(), value_start, comment));
        }
        toml_update_multiline_state(&mut multiline, without_newline);
        offset += line.len();
    }
    if matches.len() > 1 {
        return Err(AgentSettingsError::config("native setting is ambiguous"));
    }
    let encoded = value.map(toml_quote);
    if let Some((line_start, line_end, value_start, comment)) = matches.first().copied() {
        let mut output = String::with_capacity(text.len());
        output.push_str(&text[..line_start]);
        if encoded.is_none() {
            let next = if line_end < text.len() && text.as_bytes()[line_end] == b'\n' {
                line_end + 1
            } else {
                line_end
            };
            if let Some(comment) = comment {
                output.push_str(&text[comment..line_end]);
                if next > line_end {
                    output.push('\n');
                }
            }
            output.push_str(&text[next..]);
        } else {
            let value_end = comment
                .map(|comment| {
                    let mut start = comment;
                    while start > value_start && text.as_bytes()[start - 1].is_ascii_whitespace() {
                        start -= 1;
                    }
                    start
                })
                .unwrap_or(line_end);
            output.push_str(&text[line_start..value_start]);
            output.push_str(encoded.as_deref().unwrap_or_default());
            output.push_str(&text[value_end..]);
        }
        return Ok(output);
    }
    let Some(encoded) = encoded else {
        return Ok(text.to_owned());
    };
    let insertion = format!("{key} = {encoded}\n");
    if let Some(section_start) = first_toml_table_offset(text) {
        let prefix = &text[..section_start];
        let suffix = &text[section_start..];
        return Ok(format!("{prefix}{insertion}{suffix}"));
    }
    let mut output = text.to_owned();
    if !output.is_empty() && !output.ends_with('\n') {
        output.push('\n');
    }
    output.push_str(&insertion);
    Ok(output)
}

fn find_toml_comment(line: &str, start: usize) -> Option<usize> {
    let bytes = line.as_bytes();
    let mut quote = None;
    let mut escaped = false;
    for (index, byte) in bytes.iter().enumerate().skip(start) {
        match *byte {
            b'"' if !escaped && quote.is_none() => quote = Some(b'"'),
            b'"' if !escaped && quote == Some(b'"') => quote = None,
            b'\'' if quote.is_none() => quote = Some(b'\''),
            b'\'' if quote == Some(b'\'') => quote = None,
            b'#' if quote.is_none() => return Some(index),
            b'\\' if quote == Some(b'"') => escaped = !escaped,
            _ => escaped = false,
        }
    }
    None
}

fn toml_key_matches(raw_key: &str, key: &str) -> bool {
    raw_key == key || raw_key == format!("\"{key}\"") || raw_key == format!("'{key}'")
}

fn first_toml_table_offset(text: &str) -> Option<usize> {
    let mut multiline = None;
    let mut offset = 0usize;
    for line in text.split_inclusive('\n') {
        let without_newline = line.strip_suffix('\n').unwrap_or(line);
        if multiline.is_none() && without_newline.trim_start().starts_with('[') {
            return Some(offset);
        }
        toml_update_multiline_state(&mut multiline, without_newline);
        offset += line.len();
    }
    None
}

#[derive(Clone, Copy)]
enum TomlMultiline {
    Basic,
    Literal,
}

fn toml_update_multiline_state(state: &mut Option<TomlMultiline>, line: &str) {
    let bytes = line.as_bytes();
    let mut cursor = 0usize;
    loop {
        if let Some(kind) = *state {
            let delimiter = match kind {
                TomlMultiline::Basic => b"\"\"\"",
                TomlMultiline::Literal => b"'''",
            };
            let Some(relative) = find_toml_delimiter(bytes, cursor, delimiter, kind) else {
                return;
            };
            *state = None;
            cursor = relative + delimiter.len();
            continue;
        }
        while cursor < bytes.len() {
            if bytes[cursor] == b'#' {
                return;
            }
            if bytes.get(cursor..cursor + 3) == Some(b"\"\"\"") {
                *state = Some(TomlMultiline::Basic);
                cursor += 3;
                break;
            }
            if bytes.get(cursor..cursor + 3) == Some(b"'''") {
                *state = Some(TomlMultiline::Literal);
                cursor += 3;
                break;
            }
            match bytes[cursor] {
                b'"' => {
                    cursor = skip_toml_string(bytes, cursor + 1, b'"', true);
                }
                b'\'' => {
                    cursor = skip_toml_string(bytes, cursor + 1, b'\'', false);
                }
                _ => cursor += 1,
            }
        }
        if cursor >= bytes.len() {
            return;
        }
    }
}

fn find_toml_delimiter(
    bytes: &[u8],
    mut cursor: usize,
    delimiter: &[u8],
    kind: TomlMultiline,
) -> Option<usize> {
    while cursor + delimiter.len() <= bytes.len() {
        if bytes.get(cursor..cursor + delimiter.len()) == Some(delimiter)
            && (matches!(kind, TomlMultiline::Literal) || !toml_byte_is_escaped(bytes, cursor))
        {
            return Some(cursor);
        }
        cursor += 1;
    }
    None
}

fn toml_byte_is_escaped(bytes: &[u8], offset: usize) -> bool {
    let mut backslashes = 0usize;
    let mut cursor = offset;
    while cursor > 0 && bytes[cursor - 1] == b'\\' {
        backslashes += 1;
        cursor -= 1;
    }
    backslashes % 2 == 1
}

fn skip_toml_string(bytes: &[u8], mut cursor: usize, quote: u8, escaped: bool) -> usize {
    while cursor < bytes.len() {
        if bytes[cursor] == quote && (!escaped || !toml_byte_is_escaped(bytes, cursor)) {
            return cursor + 1;
        }
        cursor += 1;
    }
    bytes.len()
}

fn toml_quote(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 2);
    output.push('"');
    for character in value.chars() {
        match character {
            '\\' => output.push_str("\\\\"),
            '"' => output.push_str("\\\""),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character => output.push(character),
        }
    }
    output.push('"');
    output
}

fn edit_json_root(
    bytes: &[u8],
    request: &AgentSettingsSaveRequest,
    claude: bool,
) -> Result<Vec<u8>, AgentSettingsError> {
    let _ = parse_json_agent(bytes, claude)?;
    let mut text = String::from_utf8(bytes.to_vec())
        .map_err(|_| AgentSettingsError::config("native configuration is not valid UTF-8"))?;
    text = json_set_path(&text, &["model"], request.model.as_deref())?;
    if claude {
        text = json_set_path(&text, &["effortLevel"], request.effort.as_deref())?;
    }
    Ok(text.into_bytes())
}

fn edit_cursor(
    bytes: &[u8],
    request: &AgentSettingsSaveRequest,
) -> Result<Vec<u8>, AgentSettingsError> {
    let parsed = parse_cursor(bytes)?;
    let mut text = String::from_utf8(bytes.to_vec())
        .map_err(|_| AgentSettingsError::config("native configuration is not valid UTF-8"))?;
    if request.model.is_none() {
        text = json_set_path(&text, &["model"], None)?;
        text = json_set_path(&text, &["selectedModel"], None)?;
        text = json_set_path(&text, &["hasChangedDefaultModel"], None)?;
        return Ok(text.into_bytes());
    }
    let old_model = parsed.model.clone();
    let model_changed = old_model.as_deref() != request.model.as_deref();
    let target_parameters = if model_changed {
        cursor_model_parameters_raw(&text, request.model.as_deref().unwrap_or_default())?
            .unwrap_or_else(|| "[]".to_owned())
    } else {
        String::new()
    };
    text = json_set_path(&text, &["model", "modelId"], request.model.as_deref())?;
    text = json_set_path(
        &text,
        &["selectedModel", "modelId"],
        request.model.as_deref(),
    )?;
    if model_changed {
        text = json_set_path_raw(
            &text,
            &["selectedModel", "parameters"],
            Some(&target_parameters),
        )?;
        for field in [
            "displayName",
            "displayNameShort",
            "displayModelId",
            "aliases",
            "name",
        ] {
            text = json_set_path(&text, &["model", field], None)?;
            text = json_set_path(&text, &["selectedModel", field], None)?;
        }
    }
    text = json_set_parameter(
        &text,
        &["selectedModel", "parameters"],
        request.effort.as_deref(),
    )?;
    if let Some(model) = request.model.as_deref() {
        text = json_set_parameter(
            &text,
            &["modelParameters", model],
            request.effort.as_deref(),
        )?;
    }
    Ok(text.into_bytes())
}

fn cursor_model_parameters_raw(
    text: &str,
    model: &str,
) -> Result<Option<String>, AgentSettingsError> {
    let Some(parameters) = find_object(text, &["modelParameters"])? else {
        return Ok(None);
    };
    let Some(field) = parameters.fields.iter().find(|field| field.key == model) else {
        return Ok(None);
    };
    let Some(array) = parse_array_at(text, field.value)? else {
        return Err(AgentSettingsError::config(
            "native model parameters have an unsupported type",
        ));
    };
    let raw = text[array.span.start..array.span.end].to_owned();
    let parsed = parse_unique_json(raw.as_bytes())?;
    let _ = parameter_effort(Some(&parsed.0))?;
    Ok(Some(raw))
}

#[derive(Debug, Clone)]
struct UniqueJsonValue(serde_json::Value);

impl<'de> Deserialize<'de> for UniqueJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = UniqueJsonValue;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON value with unique object keys")
            }
            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UniqueJsonValue(serde_json::Value::Bool(value)))
            }
            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UniqueJsonValue(value.into()))
            }
            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UniqueJsonValue(value.into()))
            }
            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                serde_json::Number::from_f64(value)
                    .map(|value| UniqueJsonValue(value.into()))
                    .ok_or_else(|| E::custom("JSON number is not finite"))
            }
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UniqueJsonValue(value.to_owned().into()))
            }
            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UniqueJsonValue(serde_json::Value::Null))
            }
            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(UniqueJsonValue(serde_json::Value::Null))
            }
            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(value) = seq.next_element::<UniqueJsonValue>()? {
                    values.push(value.0);
                }
                Ok(UniqueJsonValue(values.into()))
            }
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut values = serde_json::Map::new();
                while let Some((key, value)) = map.next_entry::<String, UniqueJsonValue>()? {
                    if values.insert(key, value.0).is_some() {
                        return Err(serde::de::Error::custom("duplicate JSON object key"));
                    }
                }
                Ok(UniqueJsonValue(values.into()))
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

fn jsonc_to_json(bytes: &[u8]) -> Result<Vec<u8>, AgentSettingsError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| AgentSettingsError::config("native configuration is not valid UTF-8"))?;
    let mut output = String::with_capacity(text.len());
    let mut chars = text.char_indices().peekable();
    let mut string = false;
    let mut escaped = false;
    while let Some((index, character)) = chars.next() {
        if string {
            output.push(character);
            if character == '"' && !escaped {
                string = false;
            }
            escaped = character == '\\' && !escaped;
            if character != '\\' {
                escaped = false;
            }
            continue;
        }
        match character {
            '"' => {
                string = true;
                output.push(character);
            }
            '/' if chars.peek().is_some_and(|(_, next)| *next == '/') => {
                let _ = chars.next();
                output.push(' ');
                for (_, next) in chars.by_ref() {
                    if next == '\n' {
                        output.push('\n');
                        break;
                    }
                }
            }
            '/' if chars.peek().is_some_and(|(_, next)| *next == '*') => {
                let _ = chars.next();
                output.push(' ');
                let mut previous = '\0';
                let mut closed = false;
                for (_, next) in chars.by_ref() {
                    if next == '\n' {
                        output.push('\n');
                    }
                    if previous == '*' && next == '/' {
                        closed = true;
                        break;
                    }
                    previous = next;
                }
                if !closed {
                    return Err(AgentSettingsError::config(
                        "native configuration is malformed",
                    ));
                }
            }
            _ => output.push(character),
        }
        let _ = index;
    }
    remove_json_trailing_commas(&output)
}

fn remove_json_trailing_commas(text: &str) -> Result<Vec<u8>, AgentSettingsError> {
    let mut output = String::with_capacity(text.len());
    let mut chars = text.char_indices().peekable();
    let mut string = false;
    let mut escaped = false;
    while let Some((index, character)) = chars.next() {
        if string {
            output.push(character);
            if character == '"' && !escaped {
                string = false;
            }
            escaped = character == '\\' && !escaped;
            if character != '\\' {
                escaped = false;
            }
            continue;
        }
        if character == '"' {
            string = true;
            output.push(character);
            continue;
        }
        if character == ',' {
            let mut look = chars.clone();
            while let Some((_, next)) = look.peek().copied() {
                if next.is_whitespace() {
                    let _ = look.next();
                } else {
                    break;
                }
            }
            if look
                .peek()
                .is_some_and(|(_, next)| matches!(*next, '}' | ']'))
            {
                continue;
            }
        }
        output.push(character);
        let _ = index;
    }
    if string {
        return Err(AgentSettingsError::config(
            "native configuration is malformed",
        ));
    }
    Ok(output.into_bytes())
}

#[derive(Debug, Clone, Copy)]
struct JsonSpan {
    start: usize,
    end: usize,
}

#[derive(Debug, Clone)]
struct JsonField {
    key: String,
    key_start: usize,
    value: JsonSpan,
}

#[derive(Debug, Clone)]
struct JsonObject {
    span: JsonSpan,
    fields: Vec<JsonField>,
    trailing_comma: bool,
}

#[derive(Debug, Clone)]
struct JsonArray {
    span: JsonSpan,
    elements: Vec<JsonSpan>,
    trailing_comma: bool,
}

fn json_set_path(
    text: &str,
    path: &[&str],
    value: Option<&str>,
) -> Result<String, AgentSettingsError> {
    let encoded = value.map(json_string);
    json_set_path_raw(text, path, encoded.as_deref())
}

fn json_set_path_raw(
    text: &str,
    path: &[&str],
    value: Option<&str>,
) -> Result<String, AgentSettingsError> {
    if path.is_empty() {
        return Err(AgentSettingsError::config(
            "native configuration path is invalid",
        ));
    }
    let Some(root) = parse_root_object(text)? else {
        return Err(AgentSettingsError::config(
            "native configuration is malformed",
        ));
    };
    let parent_path = &path[..path.len() - 1];
    let key = path[path.len() - 1];
    let parent = if parent_path.is_empty() {
        Some(root.clone())
    } else {
        find_object(text, parent_path)?
    };
    let Some(parent) = parent else {
        if value.is_none() {
            return Ok(text.to_owned());
        }
        if parent_path.len() == 1 {
            let nested = format!("{{{}}}", json_member(key, value.unwrap_or_default()));
            return json_insert_field(text, &root, parent_path[0], &nested);
        }
        return Err(AgentSettingsError::config(
            "native configuration path is unavailable",
        ));
    };
    if let Some(field) = parent.fields.iter().find(|field| field.key == key) {
        return match value {
            Some(value) => Ok(json_replace(text, field.value, value)),
            None => json_remove_field(text, &parent, field),
        };
    }
    let Some(value) = value else {
        return Ok(text.to_owned());
    };
    json_insert_field(text, &parent, key, value)
}

fn json_set_parameter(
    text: &str,
    path: &[&str],
    value: Option<&str>,
) -> Result<String, AgentSettingsError> {
    let Some(array) = find_array(text, path)? else {
        return Ok(text.to_owned());
    };
    let mut parameter = None;
    for (index, element) in array.elements.iter().enumerate() {
        let Some(object) = parse_object_at(text, *element)? else {
            continue;
        };
        if object
            .fields
            .iter()
            .find(|field| field.key == "id")
            .and_then(|field| json_string_value(text, field.value).ok().flatten())
            .as_deref()
            == Some("effort")
        {
            parameter = Some((index, object));
            break;
        }
    }
    if let Some((index, object)) = parameter {
        if let Some(field) = object.fields.iter().find(|field| field.key == "value") {
            return match value {
                Some(value) => Ok(json_replace(text, field.value, &json_string(value))),
                None => json_remove_array_element(text, &array, index),
            };
        }
        if let Some(value) = value {
            return json_insert_field(text, &object, "value", &json_string(value));
        }
        return json_remove_array_element(text, &array, index);
    }
    let Some(value) = value else {
        return Ok(text.to_owned());
    };
    json_insert_array_element(
        text,
        &array,
        &format!("{{\"id\":\"effort\",\"value\":{}}}", json_string(value)),
    )
}

fn parse_root_object(text: &str) -> Result<Option<JsonObject>, AgentSettingsError> {
    let start = json_skip(text, 0)?;
    if start >= text.len() {
        return Ok(None);
    }
    parse_object_at(
        text,
        JsonSpan {
            start,
            end: json_value_end(text, start)?,
        },
    )
}

fn find_object(text: &str, path: &[&str]) -> Result<Option<JsonObject>, AgentSettingsError> {
    let mut current = parse_root_object(text)?;
    for key in path {
        let Some(object) = current else {
            return Ok(None);
        };
        let Some(field) = object.fields.iter().find(|field| field.key == *key) else {
            return Ok(None);
        };
        current = parse_object_at(text, field.value)?;
    }
    Ok(current)
}

fn find_array(text: &str, path: &[&str]) -> Result<Option<JsonArray>, AgentSettingsError> {
    let mut current = parse_root_object(text)?;
    for (index, key) in path.iter().enumerate() {
        let Some(object) = current else {
            return Ok(None);
        };
        let Some(field) = object.fields.iter().find(|field| field.key == *key) else {
            return Ok(None);
        };
        if index + 1 == path.len() {
            return parse_array_at(text, field.value);
        }
        current = parse_object_at(text, field.value)?;
    }
    Ok(None)
}

fn parse_object_at(text: &str, span: JsonSpan) -> Result<Option<JsonObject>, AgentSettingsError> {
    let start = json_skip(text, span.start)?;
    if text.as_bytes().get(start) != Some(&b'{') {
        return Ok(None);
    }
    let end = json_value_end(text, start)?;
    if end > span.end {
        return Err(AgentSettingsError::config(
            "native configuration is malformed",
        ));
    }
    let mut fields = Vec::new();
    let mut trailing_comma = false;
    let mut cursor = json_skip(text, start + 1)?;
    if text.as_bytes().get(cursor) == Some(&b'}') {
        return Ok(Some(JsonObject {
            span: JsonSpan { start, end },
            fields,
            trailing_comma,
        }));
    }
    loop {
        let key_start = cursor;
        let key_end = json_string_end(text, key_start)?;
        let key = json_decode_string(&text[key_start..key_end])?;
        cursor = json_skip(text, key_end)?;
        if text.as_bytes().get(cursor) != Some(&b':') {
            return Err(AgentSettingsError::config(
                "native configuration is malformed",
            ));
        }
        let value_start = json_skip(text, cursor + 1)?;
        let value_end = json_value_end(text, value_start)?;
        fields.push(JsonField {
            key,
            key_start,
            value: JsonSpan {
                start: value_start,
                end: value_end,
            },
        });
        cursor = json_skip(text, value_end)?;
        match text.as_bytes().get(cursor) {
            Some(b',') => {
                cursor = json_skip(text, cursor + 1)?;
                if text.as_bytes().get(cursor) == Some(&b'}') {
                    trailing_comma = true;
                    break;
                }
            }
            Some(b'}') => break,
            _ => {
                return Err(AgentSettingsError::config(
                    "native configuration is malformed",
                ));
            }
        }
    }
    Ok(Some(JsonObject {
        span: JsonSpan { start, end },
        fields,
        trailing_comma,
    }))
}

fn parse_array_at(text: &str, span: JsonSpan) -> Result<Option<JsonArray>, AgentSettingsError> {
    let start = json_skip(text, span.start)?;
    if text.as_bytes().get(start) != Some(&b'[') {
        return Ok(None);
    }
    let end = json_value_end(text, start)?;
    if end > span.end {
        return Err(AgentSettingsError::config(
            "native configuration is malformed",
        ));
    }
    let mut elements = Vec::new();
    let mut trailing_comma = false;
    let mut cursor = json_skip(text, start + 1)?;
    if text.as_bytes().get(cursor) == Some(&b']') {
        return Ok(Some(JsonArray {
            span: JsonSpan { start, end },
            elements,
            trailing_comma,
        }));
    }
    loop {
        let element_start = cursor;
        let element_end = json_value_end(text, element_start)?;
        elements.push(JsonSpan {
            start: element_start,
            end: element_end,
        });
        cursor = json_skip(text, element_end)?;
        match text.as_bytes().get(cursor) {
            Some(b',') => {
                cursor = json_skip(text, cursor + 1)?;
                if text.as_bytes().get(cursor) == Some(&b']') {
                    trailing_comma = true;
                    break;
                }
            }
            Some(b']') => break,
            _ => {
                return Err(AgentSettingsError::config(
                    "native configuration is malformed",
                ));
            }
        }
    }
    Ok(Some(JsonArray {
        span: JsonSpan { start, end },
        elements,
        trailing_comma,
    }))
}

fn json_skip(text: &str, mut cursor: usize) -> Result<usize, AgentSettingsError> {
    let bytes = text.as_bytes();
    while cursor < bytes.len() {
        if bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
            continue;
        }
        if bytes[cursor] == b'/' && bytes.get(cursor + 1) == Some(&b'/') {
            cursor += 2;
            while cursor < bytes.len() && bytes[cursor] != b'\n' {
                cursor += 1;
            }
            continue;
        }
        if bytes[cursor] == b'/' && bytes.get(cursor + 1) == Some(&b'*') {
            cursor += 2;
            while cursor + 1 < bytes.len() && !(bytes[cursor] == b'*' && bytes[cursor + 1] == b'/')
            {
                cursor += 1;
            }
            if cursor + 1 >= bytes.len() {
                return Err(AgentSettingsError::config(
                    "native configuration is malformed",
                ));
            }
            cursor += 2;
            continue;
        }
        break;
    }
    Ok(cursor)
}

fn json_string_end(text: &str, start: usize) -> Result<usize, AgentSettingsError> {
    if text.as_bytes().get(start) != Some(&b'"') {
        return Err(AgentSettingsError::config(
            "native configuration is malformed",
        ));
    }
    let mut escaped = false;
    for (offset, character) in text[start + 1..].char_indices() {
        if character == '"' && !escaped {
            return Ok(start + 1 + offset + character.len_utf8());
        }
        escaped = character == '\\' && !escaped;
        if character != '\\' {
            escaped = false;
        }
    }
    Err(AgentSettingsError::config(
        "native configuration is malformed",
    ))
}

fn json_value_end(text: &str, start: usize) -> Result<usize, AgentSettingsError> {
    let start = json_skip(text, start)?;
    let Some(character) = text[start..].chars().next() else {
        return Err(AgentSettingsError::config(
            "native configuration is malformed",
        ));
    };
    match character {
        '"' => json_string_end(text, start),
        '{' => {
            let mut cursor = json_skip(text, start + 1)?;
            if text.as_bytes().get(cursor) == Some(&b'}') {
                return Ok(cursor + 1);
            }
            loop {
                cursor = json_string_end(text, cursor)?;
                cursor = json_skip(text, cursor)?;
                if text.as_bytes().get(cursor) != Some(&b':') {
                    return Err(AgentSettingsError::config(
                        "native configuration is malformed",
                    ));
                }
                cursor = json_value_end(text, cursor + 1)?;
                cursor = json_skip(text, cursor)?;
                match text.as_bytes().get(cursor) {
                    Some(b',') => {
                        cursor = json_skip(text, cursor + 1)?;
                        if text.as_bytes().get(cursor) == Some(&b'}') {
                            return Ok(cursor + 1);
                        }
                    }
                    Some(b'}') => return Ok(cursor + 1),
                    _ => {
                        return Err(AgentSettingsError::config(
                            "native configuration is malformed",
                        ));
                    }
                }
            }
        }
        '[' => {
            let mut cursor = json_skip(text, start + 1)?;
            if text.as_bytes().get(cursor) == Some(&b']') {
                return Ok(cursor + 1);
            }
            loop {
                cursor = json_value_end(text, cursor)?;
                cursor = json_skip(text, cursor)?;
                match text.as_bytes().get(cursor) {
                    Some(b',') => {
                        cursor = json_skip(text, cursor + 1)?;
                        if text.as_bytes().get(cursor) == Some(&b']') {
                            return Ok(cursor + 1);
                        }
                    }
                    Some(b']') => return Ok(cursor + 1),
                    _ => {
                        return Err(AgentSettingsError::config(
                            "native configuration is malformed",
                        ));
                    }
                }
            }
        }
        _ => {
            let mut cursor = start;
            while cursor < text.len() {
                match text.as_bytes()[cursor] {
                    b',' | b'}' | b']' if cursor > start => break,
                    byte if byte.is_ascii_whitespace() => break,
                    _ => cursor += 1,
                }
            }
            if cursor == start {
                Err(AgentSettingsError::config(
                    "native configuration is malformed",
                ))
            } else {
                Ok(cursor)
            }
        }
    }
}

fn json_decode_string(value: &str) -> Result<String, AgentSettingsError> {
    serde_json::from_str(value)
        .map_err(|_| AgentSettingsError::config("native configuration is malformed"))
}

fn json_string_value(text: &str, span: JsonSpan) -> Result<Option<String>, AgentSettingsError> {
    if text.as_bytes().get(span.start) != Some(&b'"') {
        return Ok(None);
    }
    Ok(Some(json_decode_string(&text[span.start..span.end])?))
}

fn json_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_owned())
}

fn json_member(key: &str, value: &str) -> String {
    format!("{}:{}", json_string(key), value)
}

fn json_replace(text: &str, span: JsonSpan, replacement: &str) -> String {
    let mut output = String::with_capacity(text.len() + replacement.len());
    output.push_str(&text[..span.start]);
    output.push_str(replacement);
    output.push_str(&text[span.end..]);
    output
}

fn json_remove_field(
    text: &str,
    object: &JsonObject,
    field: &JsonField,
) -> Result<String, AgentSettingsError> {
    let index = object
        .fields
        .iter()
        .position(|candidate| candidate.key_start == field.key_start)
        .ok_or_else(|| AgentSettingsError::config("native configuration is malformed"))?;
    let mut start = field.key_start;
    let mut end = field.value.end;
    let after = json_skip(text, end)?;
    if text.as_bytes().get(after) == Some(&b',') {
        end = after + 1;
    } else if index > 0 {
        let previous = &object.fields[index - 1];
        let previous_after = json_skip(text, previous.value.end)?;
        if text.as_bytes().get(previous_after) == Some(&b',') {
            start = previous_after;
        }
    }
    Ok(json_delete(text, start, end))
}

fn json_remove_array_element(
    text: &str,
    array: &JsonArray,
    index: usize,
) -> Result<String, AgentSettingsError> {
    let element = array
        .elements
        .get(index)
        .copied()
        .ok_or_else(|| AgentSettingsError::config("native configuration is malformed"))?;
    let mut start = element.start;
    let mut end = element.end;
    let after = json_skip(text, end)?;
    if text.as_bytes().get(after) == Some(&b',') {
        end = after + 1;
    } else if index > 0 {
        let previous = array.elements[index - 1];
        let previous_after = json_skip(text, previous.end)?;
        if text.as_bytes().get(previous_after) == Some(&b',') {
            start = previous_after;
        }
    }
    Ok(json_delete(text, start, end))
}

fn json_delete(text: &str, start: usize, end: usize) -> String {
    let mut output = String::with_capacity(text.len().saturating_sub(end - start));
    output.push_str(&text[..start]);
    output.push_str(&text[end..]);
    output
}

fn json_insert_field(
    text: &str,
    object: &JsonObject,
    key: &str,
    value: &str,
) -> Result<String, AgentSettingsError> {
    let close = object.span.end.saturating_sub(1);
    let has_newline = text[object.span.start..close].contains('\n');
    if object.trailing_comma {
        let last = object
            .fields
            .last()
            .ok_or_else(|| AgentSettingsError::config("native configuration is malformed"))?;
        let insertion_at = json_trailing_comma_offset(text, last.value)?;
        let insertion = if has_newline {
            let indent = json_child_indent(text, object);
            format!(",\n{indent}{}: {value}", json_string(key))
        } else {
            format!(",{}:{}", json_string(key), value)
        };
        return Ok(json_insert_at(text, insertion_at, &insertion));
    }
    let insertion = if object.fields.is_empty() {
        if has_newline {
            let indent = json_child_indent(text, object);
            format!(
                "\n{indent}{}: {value}\n{}",
                json_string(key),
                json_close_indent(text, object)
            )
        } else {
            format!("{}:{}", json_string(key), value)
        }
    } else if has_newline {
        let indent = json_child_indent(text, object);
        format!(",\n{indent}{}: {value}", json_string(key))
    } else {
        format!(",{}:{}", json_string(key), value)
    };
    let mut output = String::with_capacity(text.len() + insertion.len());
    output.push_str(&text[..close]);
    output.push_str(&insertion);
    output.push_str(&text[close..]);
    Ok(output)
}

fn json_insert_array_element(
    text: &str,
    array: &JsonArray,
    value: &str,
) -> Result<String, AgentSettingsError> {
    let close = array.span.end.saturating_sub(1);
    let has_newline = text[array.span.start..close].contains('\n');
    if array.trailing_comma {
        let last = array
            .elements
            .last()
            .ok_or_else(|| AgentSettingsError::config("native configuration is malformed"))?;
        let insertion_at = json_trailing_comma_offset(text, *last)?;
        let insertion = if has_newline {
            let indent = json_array_indent(text, array);
            format!(",\n{indent}{value}")
        } else {
            format!(",{value}")
        };
        return Ok(json_insert_at(text, insertion_at, &insertion));
    }
    let insertion = if array.elements.is_empty() {
        if has_newline {
            let indent = json_array_indent(text, array);
            format!(
                "\n{indent}{value}\n{}",
                json_array_close_indent(text, array)
            )
        } else {
            value.to_owned()
        }
    } else if has_newline {
        let indent = json_array_indent(text, array);
        format!(",\n{indent}{value}")
    } else {
        format!(",{value}")
    };
    Ok(json_insert_at(text, close, &insertion))
}

fn json_trailing_comma_offset(text: &str, last: JsonSpan) -> Result<usize, AgentSettingsError> {
    let comma = json_skip(text, last.end)?;
    if text.as_bytes().get(comma) == Some(&b',') {
        Ok(comma)
    } else {
        Err(AgentSettingsError::config(
            "native configuration is malformed",
        ))
    }
}

fn json_insert_at(text: &str, offset: usize, insertion: &str) -> String {
    let mut output = String::with_capacity(text.len() + insertion.len());
    output.push_str(&text[..offset]);
    output.push_str(insertion);
    output.push_str(&text[offset..]);
    output
}

fn json_child_indent(text: &str, object: &JsonObject) -> String {
    let close_indent = json_close_indent(text, object);
    format!("{close_indent}  ")
}

fn json_close_indent(text: &str, object: &JsonObject) -> String {
    let close = object.span.end.saturating_sub(1);
    let start = text[..close].rfind('\n').map_or(0, |index| index + 1);
    text[start..close]
        .chars()
        .take_while(|character| character.is_whitespace())
        .collect()
}

fn json_array_indent(text: &str, array: &JsonArray) -> String {
    let close_indent = json_array_close_indent(text, array);
    format!("{close_indent}  ")
}

fn json_array_close_indent(text: &str, array: &JsonArray) -> String {
    let close = array.span.end.saturating_sub(1);
    let start = text[..close].rfind('\n').map_or(0, |index| index + 1);
    text[start..close]
        .chars()
        .take_while(|character| character.is_whitespace())
        .collect()
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::tempdir;

    #[test]
    fn opened_parent_chain_applies_native_directory_policy() {
        let home = tempdir().unwrap();
        let directory = home.path().join(".codex");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o775)).unwrap();

        let anchored = RootedDir::open_anchored_absolute(&directory).unwrap();
        assert!(
            anchored
                .verify_bound_with_policy(native_directory_stat_is_safe)
                .is_err()
        );
    }
}
