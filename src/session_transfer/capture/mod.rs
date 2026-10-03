pub mod claude;
pub mod codex;
use super::{SessionAgent, SessionCapture, session_error};
use crate::error::WorkerError;
use std::{
    fs::{File, OpenOptions},
    io::Read,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};
pub fn capture_for(agent: SessionAgent) -> Box<dyn SessionCapture> {
    match agent {
        SessionAgent::Claude => Box::new(claude::ClaudeCapture),
        SessionAgent::Codex => Box::new(codex::CodexCapture),
    }
}
pub fn read_complete_lines(path: &Path, max_bytes: u64) -> Result<Vec<Vec<u8>>, WorkerError> {
    let bytes = read_session_bytes(open_session_file(path)?, max_bytes)?;
    complete_lines(&bytes)
}

pub(super) fn open_session_file(path: &Path) -> Result<File, WorkerError> {
    let file = OpenOptions::new()
        .read(true)
        // NONBLOCK prevents a substituted FIFO from blocking before fstat.
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| session_error("SESSION_UNREADABLE", "cannot open session"))?;
    require_regular(&file)?;
    Ok(file)
}

fn require_regular(file: &File) -> Result<std::fs::Metadata, WorkerError> {
    // File::metadata uses fstat: inspect the opened object, never its path.
    let metadata = file
        .metadata()
        .map_err(|_| session_error("SESSION_UNREADABLE", "cannot inspect session"))?;
    if !metadata.is_file() {
        return Err(session_error(
            "SESSION_UNREADABLE",
            "session is not a regular file",
        ));
    }
    Ok(metadata)
}

pub(super) fn read_session_bytes(file: File, max_bytes: u64) -> Result<Vec<u8>, WorkerError> {
    if require_regular(&file)?.len() > max_bytes {
        return Err(session_error(
            "SESSION_TOO_LARGE",
            "session exceeds size cap",
        ));
    }
    let mut bytes = Vec::new();
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| session_error("SESSION_UNREADABLE", "cannot read session"))?;
    if bytes.len() as u64 > max_bytes {
        return Err(session_error(
            "SESSION_TOO_LARGE",
            "session exceeds size cap",
        ));
    }
    Ok(bytes)
}

pub(super) fn complete_lines(bytes: &[u8]) -> Result<Vec<Vec<u8>>, WorkerError> {
    let complete = bytes.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
    let mut lines = Vec::new();
    for line in bytes[..complete].split_inclusive(|&b| b == b'\n') {
        let line = &line[..line.len() - 1];
        serde_json::from_slice::<serde_json::Value>(line)
            .map_err(|_| session_error("SESSION_UNREADABLE", "invalid session JSONL"))?;
        lines.push(line.to_vec());
    }
    Ok(lines)
}
pub fn relative_inside(root: &Path, cwd: &Path) -> Result<String, WorkerError> {
    let root = root
        .canonicalize()
        .map_err(|_| session_error("SESSION_OUTSIDE_PROJECT", "cannot resolve project root"))?;
    let cwd: PathBuf = cwd.canonicalize().map_err(|_| {
        session_error(
            "SESSION_OUTSIDE_PROJECT",
            "cannot resolve session directory",
        )
    })?;
    let relative = cwd
        .strip_prefix(root)
        .map_err(|_| session_error("SESSION_OUTSIDE_PROJECT", "session is outside the project"))?;
    relative
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| session_error("SESSION_UNREADABLE", "session directory is not UTF-8"))
}
