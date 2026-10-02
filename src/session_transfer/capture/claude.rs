use super::super::{claude_dir::claude_project_dir, contracts::*};
use crate::error::WorkerError;
use std::{
    fs::{self, DirEntry},
    io,
    path::{Path, PathBuf},
    time::SystemTime,
};
pub struct ClaudeCapture;
impl SessionCapture for ClaudeCapture {
    fn agent(&self) -> SessionAgent {
        SessionAgent::Claude
    }
    fn discover(
        &self,
        selector: &SessionSelector,
        cx: &CaptureContext<'_>,
    ) -> Result<PathBuf, WorkerError> {
        if selector.agent() != self.agent() {
            return Err(session_error(
                "SESSION_AGENT_MISMATCH",
                "wrong session agent",
            ));
        }
        // Laptop CLAUDE_CONFIG_DIR overrides are intentionally unsupported in v1.
        let projects = cx.home.join(".claude/projects");
        let preferred = projects.join(claude_project_dir(path_text(cx.project_root)?));
        let mut candidates = Vec::new();
        if let Some(id) = selector.id() {
            let name = format!("{id}.jsonl");
            for project in directory_entries(&projects)? {
                if !project.file_type().map_err(|_| unreadable())?.is_dir() {
                    continue;
                }
                for entry in directory_entries(&project.path())? {
                    if entry.file_name() == name.as_str() {
                        add_candidate(&mut candidates, entry, project.path() == preferred)?;
                    }
                }
            }
        } else {
            let canonical = cx.project_root.canonicalize().map_err(|_| unreadable())?;
            let canonical_dir = projects.join(claude_project_dir(path_text(&canonical)?));
            let mut directories = vec![preferred];
            if !directories.contains(&canonical_dir) {
                directories.push(canonical_dir);
            }
            for directory in directories {
                for entry in directory_entries(&directory)? {
                    let path = entry.path();
                    if path.extension().and_then(|s| s.to_str()) == Some("jsonl")
                        && path
                            .file_stem()
                            .and_then(|s| s.to_str())
                            .is_some_and(lowercase_uuid)
                    {
                        add_candidate(&mut candidates, entry, false)?;
                    }
                }
            }
        }
        // Prefer the requested project for explicit ids, then newest mtime.
        // Path ordering makes equal-mtime choices deterministic.
        candidates.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)).then(a.2.cmp(&b.2)));
        candidates
            .into_iter()
            .next()
            .map(|(_, _, path)| path)
            .ok_or_else(|| session_error("SESSION_NOT_FOUND", "no matching Claude session"))
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

fn unreadable() -> WorkerError {
    session_error("SESSION_UNREADABLE", "cannot read Claude session")
}

fn path_text(path: &Path) -> Result<&str, WorkerError> {
    path.to_str().ok_or_else(unreadable)
}

fn lowercase_uuid(id: &str) -> bool {
    uuid::Uuid::parse_str(id).is_ok_and(|uuid| uuid.hyphenated().to_string() == id)
}

fn directory_entries(path: &Path) -> Result<Vec<DirEntry>, WorkerError> {
    match fs::read_dir(path) {
        Ok(entries) => entries.collect::<Result<_, _>>().map_err(|_| unreadable()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(_) => Err(unreadable()),
    }
}

fn add_candidate(
    candidates: &mut Vec<(bool, SystemTime, PathBuf)>,
    entry: DirEntry,
    preferred: bool,
) -> Result<(), WorkerError> {
    if entry.file_type().map_err(|_| unreadable())?.is_file() {
        let modified = entry
            .metadata()
            .and_then(|meta| meta.modified())
            .map_err(|_| unreadable())?;
        candidates.push((preferred, modified, entry.path()));
    }
    Ok(())
}
