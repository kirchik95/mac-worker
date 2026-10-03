use super::{SessionAgent, session_error};
use crate::error::WorkerError;
use std::path::{Path, PathBuf};
pub fn store_root(
    agent: SessionAgent,
    home: &Path,
    profile_env: &[(String, String)],
) -> Result<PathBuf, WorkerError> {
    let (key, default) = match agent {
        SessionAgent::Claude => ("CLAUDE_CONFIG_DIR", ".claude"),
        SessionAgent::Codex => ("CODEX_HOME", ".codex"),
    };
    match profile_env.iter().rev().find(|(name, _)| name == key) {
        Some((_, value)) if value.is_empty() || !Path::new(value).is_absolute() => {
            Err(session_error(
                "SESSION_PLACEMENT_FAILED",
                "agent store override must be absolute",
            ))
        }
        Some((_, value)) => Ok(PathBuf::from(value)),
        None => Ok(home.join(default)),
    }
}
