//! Synthetic transcripts and read-only capture/placement doubles. No native agents run.
use super::{claude_dir::claude_project_dir, contracts::*};
use crate::error::WorkerError;
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
};

fn jsonl(lines: Vec<serde_json::Value>) -> Vec<u8> {
    let mut bytes = Vec::new();
    for line in lines {
        bytes.extend(serde_json::to_vec(&line).expect("fixture JSON"));
        bytes.push(b'\n');
    }
    bytes
}
pub fn claude_fixture(session_id: &str, cwd: &str, version: &str, turns: usize) -> Vec<u8> {
    let mut lines = Vec::new();
    for turn in 0..turns {
        let user_id = uuid::Uuid::new_v4().to_string();
        lines.push(json!({"type":"user", "uuid":user_id, "parentUuid":null, "sessionId":session_id, "cwd":cwd, "version":version, "timestamp":"2026-01-01T00:00:00.000Z", "isSidechain":false, "message":{"role":"user", "content":format!("Synthetic prompt {turn}")}}));
        lines.push(json!({"type":"assistant", "uuid":uuid::Uuid::new_v4().to_string(), "parentUuid":user_id, "sessionId":session_id, "cwd":cwd, "version":version, "timestamp":"2026-01-01T00:00:01.000Z", "isSidechain":false, "message":{"role":"assistant", "content":[{"type":"text", "text":format!("Synthetic response {turn}")}]}}));
    }
    jsonl(lines)
}
pub fn codex_fixture(thread_id: &str, cwd: &str, cli_version: &str, items: usize) -> Vec<u8> {
    let mut lines = vec![
        json!({"type":"session_meta", "timestamp":"2026-01-01T00:00:00.000Z", "payload":{"id":thread_id, "session_id":thread_id, "cwd":cwd, "cli_version":cli_version, "originator":"codex-tui", "source":"cli", "timestamp":"2026-01-01T00:00:00.000Z"}}),
    ];
    for item in 0..items {
        lines.push(json!({"type":"response_item", "timestamp":"2026-01-01T00:00:01.000Z", "payload":{"type":"message", "role":"user", "content":[{"type":"input_text", "text":format!("Synthetic item {item}")}]}}));
    }
    jsonl(lines)
}

pub struct FakeAgentHome {
    home: PathBuf,
}
impl FakeAgentHome {
    pub fn new() -> Self {
        let home = std::env::temp_dir().join(format!(
            "mac-worker-session-fixture-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(home.join(".claude/projects")).expect("fixture home");
        fs::create_dir_all(home.join(".codex/sessions/2026/01/01")).expect("fixture home");
        Self { home }
    }
    pub fn home(&self) -> &Path {
        &self.home
    }
    pub fn claude(&self, session_id: &str, cwd: &str, version: &str, turns: usize) -> PathBuf {
        assert!(session_id.parse::<uuid::Uuid>().is_ok());
        let dir = self
            .home
            .join(".claude/projects")
            .join(claude_project_dir(cwd));
        fs::create_dir_all(&dir).expect("fixture directory");
        let path = dir.join(format!("{session_id}.jsonl"));
        fs::write(&path, claude_fixture(session_id, cwd, version, turns))
            .expect("fixture transcript");
        path
    }
    pub fn codex(&self, thread_id: &str, cwd: &str, cli_version: &str, items: usize) -> PathBuf {
        assert!(thread_id.parse::<uuid::Uuid>().is_ok());
        let path = self
            .home
            .join(".codex/sessions/2026/01/01")
            .join(format!("rollout-2026-01-01T00-00-00-{thread_id}.jsonl"));
        fs::write(&path, codex_fixture(thread_id, cwd, cli_version, items))
            .expect("fixture transcript");
        path
    }
}
impl Default for FakeAgentHome {
    fn default() -> Self {
        Self::new()
    }
}
impl Drop for FakeAgentHome {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.home);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaptureCall {
    Discover(SessionSelector),
    Capture(PathBuf),
}
pub struct FakeCapture {
    pub agent: SessionAgent,
    pub source: PathBuf,
    pub package: SessionPackage,
    pub calls: Mutex<Vec<CaptureCall>>,
}
impl SessionCapture for FakeCapture {
    fn agent(&self) -> SessionAgent {
        self.agent
    }
    fn discover(
        &self,
        selector: &SessionSelector,
        _cx: &CaptureContext<'_>,
    ) -> Result<PathBuf, WorkerError> {
        self.calls
            .lock()
            .expect("fixture lock")
            .push(CaptureCall::Discover(selector.clone()));
        Ok(self.source.clone())
    }
    fn capture(
        &self,
        source: &Path,
        _cx: &CaptureContext<'_>,
    ) -> Result<CapturedSession, WorkerError> {
        self.calls
            .lock()
            .expect("fixture lock")
            .push(CaptureCall::Capture(source.to_owned()));
        Ok(CapturedSession {
            package: self.package.clone(),
            source_path: source.to_owned(),
            recently_modified: false,
            first_prompt_preview: None,
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaceCall {
    pub package: SessionPackage,
    pub workspace: PathBuf,
    pub session_id: String,
}
pub struct FakePlace {
    pub agent: SessionAgent,
    pub primary_relative: String,
    pub files: Vec<String>,
    pub calls: Mutex<Vec<PlaceCall>>,
}
impl SessionPlace for FakePlace {
    fn agent(&self) -> SessionAgent {
        self.agent
    }
    fn place(
        &self,
        package: &SessionPackage,
        cx: &PlaceContext<'_>,
    ) -> Result<PlacedSession, WorkerError> {
        self.calls.lock().expect("fixture lock").push(PlaceCall {
            package: package.clone(),
            workspace: cx.workspace.to_owned(),
            session_id: cx.session_id.to_owned(),
        });
        Ok(PlacedSession {
            primary_relative: self.primary_relative.clone(),
            files: self.files.clone(),
        })
    }
}
