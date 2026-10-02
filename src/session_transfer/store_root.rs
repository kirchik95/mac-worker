use super::SessionAgent;
use std::path::{Path, PathBuf};
pub fn store_root(agent: SessionAgent, home: &Path, profile_env: &[(String, String)]) -> PathBuf {
    let (key, default) = match agent {
        SessionAgent::Claude => ("CLAUDE_CONFIG_DIR", ".claude"),
        SessionAgent::Codex => ("CODEX_HOME", ".codex"),
    };
    profile_env
        .iter()
        .find(|(name, _)| name == key)
        .map_or_else(|| home.join(default), |(_, value)| PathBuf::from(value))
}
