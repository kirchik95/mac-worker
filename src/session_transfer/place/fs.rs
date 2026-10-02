use super::super::session_error;
use crate::error::WorkerError;
use std::path::{Path, PathBuf};
pub struct StoreWriter {
    root: PathBuf,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    Created,
    Unchanged,
}
impl StoreWriter {
    pub fn open(_root: &Path) -> Result<Self, WorkerError> {
        Err(session_error(
            "SESSION_PLACEMENT_FAILED",
            "not implemented yet",
        ))
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn write_file(&self, _relative: &str, _bytes: &[u8]) -> Result<WriteOutcome, WorkerError> {
        Err(session_error(
            "SESSION_PLACEMENT_FAILED",
            "not implemented yet",
        ))
    }
    pub fn read_file(
        &self,
        _relative: &str,
        _max_bytes: u64,
    ) -> Result<Option<Vec<u8>>, WorkerError> {
        Err(session_error(
            "SESSION_PLACEMENT_FAILED",
            "not implemented yet",
        ))
    }
}
