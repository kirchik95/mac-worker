use super::super::contracts::*;
use crate::error::WorkerError;
pub struct CodexPlace;
impl SessionPlace for CodexPlace {
    fn agent(&self) -> SessionAgent {
        SessionAgent::Codex
    }
    fn place(
        &self,
        _package: &SessionPackage,
        _cx: &PlaceContext<'_>,
    ) -> Result<PlacedSession, WorkerError> {
        Err(session_error(
            "SESSION_IMPORT_UNSUPPORTED",
            "not implemented yet",
        ))
    }
}
