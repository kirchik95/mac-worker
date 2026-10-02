use super::{place::fs::StoreWriter, scrub::Scrubber};
use crate::{agent::AgentKind, error::WorkerError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    borrow::Cow,
    collections::BTreeSet,
    path::{Path, PathBuf},
    str::FromStr,
    time::SystemTime,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionAgent {
    Claude,
    Codex,
}
impl SessionAgent {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
        }
    }
    pub fn agent_kind(self) -> AgentKind {
        match self {
            Self::Claude => AgentKind::Claude,
            Self::Codex => AgentKind::Codex,
        }
    }
    pub fn from_agent_kind(kind: AgentKind) -> Option<Self> {
        match kind {
            AgentKind::Claude => Some(Self::Claude),
            AgentKind::Codex => Some(Self::Codex),
            _ => None,
        }
    }
    pub fn format(self) -> SessionFormat {
        match self {
            Self::Claude => SessionFormat::ClaudeJsonlV1,
            Self::Codex => SessionFormat::CodexRolloutV1,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SessionFormat {
    ClaudeJsonlV1,
    CodexRolloutV1,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSelector {
    agent: SessionAgent,
    id: Option<String>,
}
impl FromStr for SessionSelector {
    type Err = WorkerError;
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (name, id) = text
            .split_once(':')
            .map_or((text, None), |(a, b)| (a, Some(b)));
        let agent = match name {
            "claude" => SessionAgent::Claude,
            "codex" => SessionAgent::Codex,
            _ => {
                return Err(session_error(
                    "TASK_CONFIG_INVALID",
                    "invalid session agent",
                ));
            }
        };
        if let Some(id) = id
            && !valid_uuid(id)
        {
            return Err(session_error("TASK_CONFIG_INVALID", "invalid session id"));
        }
        Ok(Self {
            agent,
            id: id.map(str::to_ascii_lowercase),
        })
    }
}
impl SessionSelector {
    pub fn agent(&self) -> SessionAgent {
        self.agent
    }
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }
}
fn valid_uuid(id: &str) -> bool {
    id.len() == 36
        && id.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
}

pub const MANIFEST_SCHEMA: u32 = 1;
pub const MAX_PACKAGE_BYTES: u64 = 64 << 20;
pub const MAX_FILE_BYTES: u64 = 64 << 20;
pub const MAX_PACKAGE_FILES: usize = 2_000;
pub const PACKAGE_MANIFEST_PATH: &str = "manifest.json";
pub const PACKAGE_SESSION_DIR: &str = "session";
pub const CLAUDE_MAIN_FILE: &str = "main.jsonl";
pub const CLAUDE_SIDECAR_DIR: &str = "sidecar";
pub const CODEX_ROLLOUT_FILE: &str = "rollout.jsonl";
pub const SESSION_REF_PREFIX: &str = "refs/mac-worker/sessions/";
pub const REQUEST_SESSION_REF_PREFIX: &str = "refs/mac-worker/request-sessions/";
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestFile {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionManifest {
    pub schema: u32,
    pub agent: SessionAgent,
    pub format: SessionFormat,
    pub source_session_id: String,
    pub source_agent_version: String,
    pub source_cwd_relative: String,
    pub files: Vec<ManifestFile>,
    pub scrubbed: u32,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageFile {
    pub path: String,
    pub bytes: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionPackage {
    manifest: SessionManifest,
    files: Vec<PackageFile>,
}
pub struct PackageSource {
    pub agent: SessionAgent,
    pub source_session_id: String,
    pub source_agent_version: String,
    pub source_cwd_relative: String,
    pub scrubbed: u32,
}
fn safe_relative(path: &str) -> bool {
    !path.is_empty()
        && !path.contains(['\\', '\0', ':'])
        && path
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}
fn validate_files(files: &[PackageFile]) -> Result<(), WorkerError> {
    if files.len() > MAX_PACKAGE_FILES {
        return Err(session_error("SESSION_TOO_LARGE", "too many session files"));
    }
    let mut total = 0u64;
    let mut paths = BTreeSet::new();
    for file in files {
        if !safe_relative(&file.path) || !paths.insert(&file.path) {
            return Err(session_error(
                "SESSION_UNREADABLE",
                "invalid session file path",
            ));
        }
        let size = file.bytes.len() as u64;
        total = total
            .checked_add(size)
            .ok_or_else(|| session_error("SESSION_TOO_LARGE", "session exceeds size cap"))?;
        if size > MAX_FILE_BYTES || total > MAX_PACKAGE_BYTES {
            return Err(session_error(
                "SESSION_TOO_LARGE",
                "session exceeds size cap",
            ));
        }
    }
    // A file cannot also be an ancestor directory of another file.
    for path in &paths {
        for (index, _) in path.match_indices('/') {
            if paths.contains(&path[..index].to_string()) {
                return Err(session_error(
                    "SESSION_UNREADABLE",
                    "conflicting session paths",
                ));
            }
        }
    }
    Ok(())
}
fn entries(files: &[PackageFile]) -> Vec<ManifestFile> {
    files
        .iter()
        .map(|file| ManifestFile {
            path: file.path.clone(),
            bytes: file.bytes.len() as u64,
            sha256: format!("{:x}", Sha256::digest(&file.bytes)),
        })
        .collect()
}
impl SessionPackage {
    pub fn build(source: PackageSource, mut files: Vec<PackageFile>) -> Result<Self, WorkerError> {
        validate_files(&files)?;
        files.sort_by(|a, b| a.path.cmp(&b.path));
        let manifest = SessionManifest {
            schema: MANIFEST_SCHEMA,
            agent: source.agent,
            format: source.agent.format(),
            source_session_id: source.source_session_id,
            source_agent_version: source.source_agent_version,
            source_cwd_relative: source.source_cwd_relative,
            files: entries(&files),
            scrubbed: source.scrubbed,
        };
        Ok(Self { manifest, files })
    }
    pub fn from_parts(
        manifest_json: &[u8],
        mut files: Vec<PackageFile>,
    ) -> Result<Self, WorkerError> {
        if manifest_json.len() as u64 > MAX_FILE_BYTES {
            return Err(session_error(
                "SESSION_TOO_LARGE",
                "manifest exceeds size cap",
            ));
        }
        let mut manifest: SessionManifest = serde_json::from_slice(manifest_json)
            .map_err(|_| session_error("SESSION_UNREADABLE", "invalid session manifest"))?;
        validate_files(&files)?;
        if manifest.schema != MANIFEST_SCHEMA || manifest.format != manifest.agent.format() {
            return Err(session_error(
                "SESSION_UNREADABLE",
                "unsupported session manifest",
            ));
        }
        files.sort_by(|a, b| a.path.cmp(&b.path));
        manifest.files.sort_by(|a, b| a.path.cmp(&b.path));
        if manifest.files != entries(&files) {
            return Err(session_error(
                "SESSION_UNREADABLE",
                "session manifest does not match files",
            ));
        }
        Ok(Self { manifest, files })
    }
    pub fn manifest(&self) -> &SessionManifest {
        &self.manifest
    }
    pub fn files(&self) -> &[PackageFile] {
        &self.files
    }
    pub fn manifest_json(&self) -> Vec<u8> {
        serde_json::to_vec(&self.manifest).expect("session manifest serializes")
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "SessionImportMetaWire")]
pub struct SessionImportMeta {
    agent: SessionAgent,
    format: SessionFormat,
    package_oid: String,
    source_agent_version: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionImportMetaWire {
    agent: SessionAgent,
    format: SessionFormat,
    package_oid: String,
    source_agent_version: String,
}
impl TryFrom<SessionImportMetaWire> for SessionImportMeta {
    type Error = WorkerError;
    fn try_from(wire: SessionImportMetaWire) -> Result<Self, Self::Error> {
        let meta = Self {
            agent: wire.agent,
            format: wire.format,
            package_oid: wire.package_oid,
            source_agent_version: wire.source_agent_version,
        };
        meta.validate()?;
        Ok(meta)
    }
}
impl SessionImportMeta {
    pub fn new(
        agent: SessionAgent,
        package_oid: impl Into<String>,
        source_agent_version: impl Into<String>,
    ) -> Result<Self, WorkerError> {
        let meta = Self {
            agent,
            format: agent.format(),
            package_oid: package_oid.into(),
            source_agent_version: source_agent_version.into(),
        };
        meta.validate()?;
        Ok(meta)
    }
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.format != self.agent.format()
            || ![40, 64].contains(&self.package_oid.len())
            || !self
                .package_oid
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || self.source_agent_version.is_empty()
            || self.source_agent_version.len() > 64
            || !self
                .source_agent_version
                .bytes()
                .all(|b| (33..=126).contains(&b))
        {
            return Err(session_error(
                "TASK_CONFIG_INVALID",
                "invalid session import metadata",
            ));
        }
        Ok(())
    }
    pub fn agent(&self) -> SessionAgent {
        self.agent
    }
    pub fn format(&self) -> SessionFormat {
        self.format
    }
    pub fn package_oid(&self) -> &str {
        &self.package_oid
    }
    pub fn source_agent_version(&self) -> &str {
        &self.source_agent_version
    }
}
pub const AGENT_MIN_REQUIREMENT_PREFIX: &str = "agent-min:";
pub fn agent_min_requirement(agent: SessionAgent, version: &str) -> String {
    format!("{AGENT_MIN_REQUIREMENT_PREFIX}{}@{version}", agent.as_str())
}
pub fn parse_agent_min_requirement(requirement: &str) -> Option<(SessionAgent, String)> {
    let (agent, version) = requirement
        .strip_prefix(AGENT_MIN_REQUIREMENT_PREFIX)?
        .split_once('@')?;
    let agent = match agent {
        "claude" => SessionAgent::Claude,
        "codex" => SessionAgent::Codex,
        _ => return None,
    };
    if version.is_empty()
        || version.len() > 64
        || !version
            .bytes()
            .all(|b| (33..=126).contains(&b) && b != b'@')
    {
        return None;
    }
    Some((agent, version.to_owned()))
}
pub fn session_error(code: &'static str, message: impl Into<Cow<'static, str>>) -> WorkerError {
    WorkerError::Task {
        code,
        message: message.into(),
    }
}
pub struct CaptureContext<'a> {
    pub project_root: &'a Path,
    pub home: &'a Path,
    pub scrubber: &'a Scrubber,
    pub now: SystemTime,
}
pub struct CapturedSession {
    pub package: SessionPackage,
    pub source_path: PathBuf,
    pub recently_modified: bool,
    pub first_prompt_preview: Option<String>,
}
pub trait SessionCapture {
    fn agent(&self) -> SessionAgent;
    fn discover(
        &self,
        selector: &SessionSelector,
        cx: &CaptureContext<'_>,
    ) -> Result<PathBuf, WorkerError>;
    fn capture(
        &self,
        source: &Path,
        cx: &CaptureContext<'_>,
    ) -> Result<CapturedSession, WorkerError>;
}
pub struct PlaceContext<'a> {
    pub workspace: &'a Path,
    pub store: &'a StoreWriter,
    pub session_id: &'a str,
    pub placed_at_millis: u64,
}
pub struct PlacedSession {
    pub primary_relative: String,
    pub files: Vec<String>,
}
pub trait SessionPlace {
    fn agent(&self) -> SessionAgent;
    fn place(
        &self,
        package: &SessionPackage,
        cx: &PlaceContext<'_>,
    ) -> Result<PlacedSession, WorkerError>;
}
