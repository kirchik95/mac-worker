use super::{RootedSessionFile, complete_lines, relative_inside, unsupported_version};
use crate::{error::WorkerError, session_transfer::tokens};
use serde_json::Value;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    time::Duration,
};

use super::super::contracts::*;

const MAX_DISCOVERY_FILES: usize = 200;
const MAX_META_LINE_BYTES: u64 = 1 << 20;
const PREVIEW_CHARS: usize = 120;

pub struct CodexCapture;

impl SessionCapture for CodexCapture {
    fn agent(&self) -> SessionAgent {
        SessionAgent::Codex
    }

    fn discover(
        &self,
        selector: &SessionSelector,
        cx: &CaptureContext<'_>,
    ) -> Result<PathBuf, WorkerError> {
        if selector.agent() != self.agent() {
            return Err(session_error(
                "SESSION_AGENT_MISMATCH",
                "session selector is not Codex",
            ));
        }
        // Laptop CODEX_HOME overrides are deliberately unsupported in v1.
        let sessions = cx.home.join(".codex/sessions");
        let mut found = None;
        let mut compressed_match = false;
        let mut considered = 0;
        visit_rollouts(&sessions, &mut |path| {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            if let Some(id) = selector.id() {
                if name.ends_with(&format!("-{id}.jsonl")) {
                    found = Some(path.to_owned());
                    return true;
                }
                compressed_match |= name.ends_with(&format!("-{id}.jsonl.zst"));
            } else if name.ends_with(".jsonl") {
                considered += 1;
                if first_line_cwd(&sessions, path)
                    .is_some_and(|cwd| relative_inside(cx.project_root, Path::new(&cwd)).is_ok())
                {
                    found = Some(path.to_owned());
                    return true;
                }
                return considered >= MAX_DISCOVERY_FILES;
            }
            false
        })?;
        if let Some(path) = found {
            Ok(path)
        } else if compressed_match {
            Err(compressed_error())
        } else {
            Err(session_error(
                "SESSION_NOT_FOUND",
                "Codex session not found",
            ))
        }
    }

    fn capture(
        &self,
        source: &Path,
        cx: &CaptureContext<'_>,
    ) -> Result<CapturedSession, WorkerError> {
        if source
            .extension()
            .is_some_and(|extension| extension == "zst")
        {
            return Err(compressed_error());
        }
        let main_file = RootedSessionFile::open(&cx.home.join(".codex/sessions"), source)?;
        let metadata = main_file.metadata()?;
        let lines = complete_lines(&main_file.read_bytes(MAX_FILE_BYTES)?)?;
        let id = filename_id(source).ok_or_else(unreadable_meta)?;
        let (cwd, version) = session_meta(lines.first().ok_or_else(unreadable_meta)?, id)?;
        if !supported_agent_version(&version, cx.scrubber) {
            return Err(unsupported_version());
        }
        let source_cwd_relative = relative_inside(cx.project_root, Path::new(&cwd))?;
        let canonical_root = cx
            .project_root
            .canonicalize()
            .map_err(|_| unreadable_meta())?;
        let given = cx.project_root.to_str().ok_or_else(unreadable_meta)?;
        let canonical = canonical_root.to_str().ok_or_else(unreadable_meta)?;
        let mut roots = vec![given, canonical];
        roots.dedup();

        let mut scrubbed = 0;
        let mut text = Vec::new();
        for line in lines {
            let line = cx.scrubber.scrub_line(&line)?;
            scrubbed += line.replacements;
            text.extend_from_slice(&line.bytes);
            text.push(b'\n');
        }
        // Keep native paths and IDs in display text, but never expose unscrubbed input.
        let first_prompt_preview = first_prompt_preview(&text)?;
        let text = tokens::normalize(&text, &roots, id)?;
        let package = SessionPackage::build(
            PackageSource {
                agent: SessionAgent::Codex,
                source_session_id: id.to_owned(),
                source_agent_version: version,
                source_cwd_relative,
                scrubbed,
            },
            vec![PackageFile {
                path: CODEX_ROLLOUT_FILE.to_owned(),
                bytes: text,
            }],
        )?;
        let modified = metadata.modified().map_err(|_| {
            session_error(
                "SESSION_UNREADABLE",
                "cannot read session modification time",
            )
        })?;
        // A future mtime (clock skew) is conservatively considered live too.
        let recently_modified = cx
            .now
            .duration_since(modified)
            .map_or(true, |age| age < Duration::from_secs(10));
        Ok(CapturedSession {
            package,
            source_path: source.to_owned(),
            recently_modified,
            first_prompt_preview,
        })
    }
}

/// Native YYYY/MM/DD components sort lexically in chronological order. Visit
/// newest directories first, then descending rollout timestamps within a day.
/// Stop as soon as the visitor requests it; never follow directory/file symlinks.
fn visit_rollouts(
    directory: &Path,
    visit: &mut impl FnMut(&Path) -> bool,
) -> Result<bool, WorkerError> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_) => {
            return Err(session_error(
                "SESSION_UNREADABLE",
                "cannot list Codex sessions",
            ));
        }
    };
    let mut entries = entries
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| session_error("SESSION_UNREADABLE", "cannot list Codex sessions"))?;
    entries.sort_by_key(|entry| std::cmp::Reverse(entry.file_name()));
    for entry in entries {
        let kind = entry
            .file_type()
            .map_err(|_| session_error("SESSION_UNREADABLE", "cannot inspect Codex session"))?;
        if kind.is_dir() {
            if visit_rollouts(&entry.path(), visit)? {
                return Ok(true);
            }
        } else if kind.is_file()
            && entry.file_name().to_str().is_some_and(|name| {
                name.starts_with("rollout-")
                    && (name.ends_with(".jsonl") || name.ends_with(".jsonl.zst"))
            })
            && visit(&entry.path())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn first_line_cwd(store_root: &Path, path: &Path) -> Option<String> {
    RootedSessionFile::open(store_root, path)
        .ok()?
        .with_file(|file| Ok(first_line_cwd_in(file)))
        .ok()
        .flatten()
}

fn first_line_cwd_in(file: &File) -> Option<String> {
    let mut reader = BufReader::new(file.take(MAX_META_LINE_BYTES + 1));
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line).ok()?;
    if line.len() as u64 > MAX_META_LINE_BYTES || !line.ends_with(b"\n") {
        return None;
    }
    let meta: Value = serde_json::from_slice(&line).ok()?;
    if meta["type"] != "session_meta" {
        return None;
    }
    meta["payload"]["cwd"].as_str().map(str::to_owned)
}

fn filename_id(source: &Path) -> Option<&str> {
    let name = source.file_name()?.to_str()?;
    let stem = name.strip_prefix("rollout-")?.strip_suffix(".jsonl")?;
    let start = stem.len().checked_sub(36)?;
    if !stem.get(..start)?.ends_with('-') {
        return None;
    }
    let id = stem.get(start..)?;
    let uuid = uuid::Uuid::parse_str(id).ok()?;
    (uuid.hyphenated().to_string() == id).then_some(id)
}

fn session_meta(line: &[u8], id: &str) -> Result<(String, String), WorkerError> {
    let meta: Value = serde_json::from_slice(line).map_err(|_| unreadable_meta())?;
    if meta["type"] != "session_meta" || meta["payload"]["id"].as_str() != Some(id) {
        return Err(unreadable_meta());
    }
    let cwd = meta["payload"]["cwd"]
        .as_str()
        .ok_or_else(unreadable_meta)?;
    let version = meta["payload"]["cli_version"]
        .as_str()
        .ok_or_else(unsupported_version)?;
    if !Path::new(cwd).is_absolute() {
        return Err(unreadable_meta());
    }
    Ok((cwd.to_owned(), version.to_owned()))
}

fn first_prompt_preview(text: &[u8]) -> Result<Option<String>, WorkerError> {
    let mut fallback = None;
    for line in text
        .split(|&byte| byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let line: Value = serde_json::from_slice(line).map_err(|_| unreadable_meta())?;
        let payload = &line["payload"];
        if line["type"] == "event_msg"
            && payload["type"] == "user_message"
            && let Some(message) = payload["message"].as_str().and_then(preview)
        {
            return Ok(Some(message));
        }
        if fallback.is_none()
            && line["type"] == "response_item"
            && payload["type"] == "message"
            && payload["role"] == "user"
            && let Some(content) = payload["content"].as_array()
            && let Some(input) = content.iter().find(|item| item["type"] == "input_text")
            && let Some(text) = input["text"].as_str()
            && !text.trim_start().starts_with('<')
        {
            fallback = preview(text);
        }
    }
    Ok(fallback)
}

fn preview(text: &str) -> Option<String> {
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.is_empty() {
        return None;
    }
    let mut chars = text.chars();
    let mut output: String = chars.by_ref().take(PREVIEW_CHARS).collect();
    if chars.next().is_some() {
        output.push('…');
    }
    Some(output)
}

fn unreadable_meta() -> WorkerError {
    session_error("SESSION_UNREADABLE", "invalid Codex session metadata")
}

fn compressed_error() -> WorkerError {
    session_error(
        "SESSION_UNREADABLE",
        "compressed Codex sessions are not supported",
    )
}

#[cfg(test)]
mod security_tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn sec_fix_first_line_cwd_refuses_substituted_parent() {
        let temp = tempfile::tempdir().unwrap();
        let store = temp.path().join("sessions");
        let day = store.join("2026/01/01");
        fs::create_dir_all(&day).unwrap();
        let source = day.join("rollout.jsonl");
        fs::write(
            &source,
            b"{\"type\":\"session_meta\",\"payload\":{\"cwd\":\"/synthetic/project\"}}\n",
        )
        .unwrap();
        assert_eq!(
            first_line_cwd(&store, &source).as_deref(),
            Some("/synthetic/project")
        );
        fs::rename(&day, temp.path().join("original")).unwrap();
        symlink(temp.path().join("original"), &day).unwrap();
        assert!(first_line_cwd(&store, &source).is_none());
    }
}
