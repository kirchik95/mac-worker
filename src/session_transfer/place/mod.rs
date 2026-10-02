pub mod claude;
pub mod codex;
pub mod fs;
use super::{SessionAgent, SessionPlace};
pub fn place_for(agent: SessionAgent) -> Box<dyn SessionPlace> {
    match agent {
        SessionAgent::Claude => Box::new(claude::ClaudePlace),
        SessionAgent::Codex => Box::new(codex::CodexPlace),
    }
}
