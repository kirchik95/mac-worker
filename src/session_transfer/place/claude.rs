use super::super::contracts::*;
use crate::error::WorkerError;
pub struct ClaudePlace;
impl SessionPlace for ClaudePlace {
    fn agent(&self) -> SessionAgent {
        SessionAgent::Claude
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
