use super::super::contracts::*;
use crate::error::WorkerError;
use std::path::{Path, PathBuf};
pub struct CodexCapture;
impl SessionCapture for CodexCapture {
    fn agent(&self) -> SessionAgent {
        SessionAgent::Codex
    }
    fn discover(
        &self,
        _selector: &SessionSelector,
        _cx: &CaptureContext<'_>,
    ) -> Result<PathBuf, WorkerError> {
        Err(session_error(
            "SESSION_IMPORT_UNSUPPORTED",
            "not implemented yet",
        ))
    }
    fn capture(
        &self,
        _source: &Path,
        _cx: &CaptureContext<'_>,
    ) -> Result<CapturedSession, WorkerError> {
        Err(session_error(
            "SESSION_IMPORT_UNSUPPORTED",
            "not implemented yet",
        ))
    }
}
