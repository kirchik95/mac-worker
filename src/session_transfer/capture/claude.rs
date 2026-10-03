use super::super::{claude_dir::claude_project_dir, contracts::*, scrub::Scrubber, tokens};
use super::{read_complete_lines, relative_inside};
use crate::error::WorkerError;
use serde_json::Value;
use std::{
    cmp::Ordering,
    fs::{self, DirEntry, File},
    io::{self, Read},
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
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
        source: &Path,
        cx: &CaptureContext<'_>,
    ) -> Result<CapturedSession, WorkerError> {
        let id = source
            .file_stem()
            .and_then(|s| s.to_str())
            .filter(|id| lowercase_uuid(id))
            .ok_or_else(unreadable)?;
        let metadata = fs::symlink_metadata(source).map_err(|_| unreadable())?;
        if !metadata.is_file() {
            return Err(unreadable());
        }
        let mut caps = RawCaps::default();
        caps.add_file(metadata.len())?;
        let sidecar_root = source.parent().ok_or_else(unreadable)?.join(id);
        let sidecars = collect_sidecars(&sidecar_root, &mut caps)?;
        let lines = read_complete_lines(source, MAX_FILE_BYTES)?;
        let main_raw_bytes = lines.iter().map(|line| line.len() as u64 + 1).sum();
        caps.observe(source, metadata.len(), main_raw_bytes)?;
        let mut cwd = None;
        let mut version: Option<String> = None;
        for line in &lines {
            let value: Value = serde_json::from_slice(line).map_err(|_| unreadable())?;
            if cwd.is_none() {
                cwd = value.get("cwd").and_then(Value::as_str).map(PathBuf::from);
            }
            if let Some(candidate) = value.get("version").and_then(Value::as_str)
                && version
                    .as_deref()
                    .is_none_or(|current| compare_versions(candidate, current).is_gt())
            {
                version = Some(candidate.to_owned());
            }
        }
        let relative = relative_inside(cx.project_root, &cwd.ok_or_else(unreadable)?)?;
        let version = version.ok_or_else(unreadable)?;
        let canonical = cx.project_root.canonicalize().map_err(|_| unreadable())?;
        let mut roots = vec![path_text(cx.project_root)?, path_text(&canonical)?];
        roots.sort_unstable();
        roots.dedup();
        let mut preview = None;
        let (main, mut scrubbed) = scrub_jsonl(lines, cx.scrubber, Some(&mut preview))?;
        let mut files = vec![PackageFile {
            path: CLAUDE_MAIN_FILE.to_owned(),
            bytes: tokens::normalize(&main, &roots, id)?,
        }];
        for sidecar in sidecars {
            let extension = sidecar.source.extension().and_then(|s| s.to_str());
            let (bytes, replacements, raw_bytes) = match extension {
                Some("jsonl") => {
                    let lines = read_complete_lines(&sidecar.source, MAX_FILE_BYTES)?;
                    let raw_bytes = lines.iter().map(|line| line.len() as u64 + 1).sum();
                    let (bytes, replacements) = scrub_jsonl(lines, cx.scrubber, None)?;
                    (bytes, replacements, raw_bytes)
                }
                Some("json") => {
                    let raw = read_bounded(&sidecar.source)?;
                    let raw_bytes = raw.len() as u64;
                    reject_reserved_tokens(&raw)?;
                    let result = cx.scrubber.scrub_line(&raw)?;
                    (result.bytes, result.replacements, raw_bytes)
                }
                _ => {
                    let bytes = read_bounded(&sidecar.source)?;
                    let raw_bytes = bytes.len() as u64;
                    (bytes, 0, raw_bytes)
                }
            };
            caps.observe(&sidecar.source, sidecar.raw_bytes, raw_bytes)?;
            scrubbed = scrubbed.checked_add(replacements).ok_or_else(too_large)?;
            files.push(PackageFile {
                path: sidecar.relative,
                bytes: tokens::normalize(&bytes, &roots, id)?,
            });
        }
        let package = SessionPackage::build(
            PackageSource {
                agent: self.agent(),
                source_session_id: id.to_owned(),
                source_agent_version: version,
                source_cwd_relative: relative,
                scrubbed,
            },
            files,
        )?;
        let modified = metadata.modified().map_err(|_| unreadable())?;
        Ok(CapturedSession {
            package,
            source_path: source.to_owned(),
            first_prompt_preview: preview,
            // A future mtime also deserves the live-session warning (clock skew).
            recently_modified: cx
                .now
                .duration_since(modified)
                .map_or(true, |age| age < Duration::from_secs(10)),
        })
    }
}

fn too_large() -> WorkerError {
    session_error("SESSION_TOO_LARGE", "Claude session exceeds package limits")
}

#[derive(Default)]
struct RawCaps {
    bytes: u64,
    files: usize,
}

impl RawCaps {
    fn add_file(&mut self, bytes: u64) -> Result<(), WorkerError> {
        self.files += 1;
        if self.files > MAX_PACKAGE_FILES || bytes > MAX_FILE_BYTES {
            return Err(too_large());
        }
        self.add_bytes(bytes)
    }

    fn add_bytes(&mut self, bytes: u64) -> Result<(), WorkerError> {
        self.bytes = self.bytes.checked_add(bytes).ok_or_else(too_large)?;
        if self.bytes > MAX_PACKAGE_BYTES {
            return Err(too_large());
        }
        Ok(())
    }

    fn observe(&mut self, path: &Path, original: u64, read_bytes: u64) -> Result<(), WorkerError> {
        // Recheck live files so growth after inventory cannot evade the raw cap.
        // Never reclaim shrinking files: the inventory is a conservative bound.
        let bytes = fs::symlink_metadata(path)
            .map_err(|_| unreadable())?
            .len()
            .max(read_bytes);
        if bytes > MAX_FILE_BYTES {
            return Err(too_large());
        }
        self.add_bytes(bytes.saturating_sub(original))
    }
}

struct SidecarFile {
    source: PathBuf,
    relative: String,
    raw_bytes: u64,
}

fn collect_sidecars(root: &Path, caps: &mut RawCaps) -> Result<Vec<SidecarFile>, WorkerError> {
    match fs::symlink_metadata(root) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => return Ok(Vec::new()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(_) => return Err(unreadable()),
    }
    let mut directories = vec![root.to_owned()];
    let mut files = Vec::new();
    while let Some(directory) = directories.pop() {
        for entry in directory_entries(&directory)? {
            let kind = entry.file_type().map_err(|_| unreadable())?;
            if kind.is_dir() {
                directories.push(entry.path());
            } else if kind.is_file() {
                let raw_bytes = entry.metadata().map_err(|_| unreadable())?.len();
                caps.add_file(raw_bytes)?;
                let source = entry.path();
                let relative = format!(
                    "{CLAUDE_SIDECAR_DIR}/{}",
                    path_text(source.strip_prefix(root).map_err(|_| unreadable())?)?
                );
                files.push(SidecarFile {
                    source,
                    relative,
                    raw_bytes,
                });
            }
            // file_type does not follow symlinks; all non-regular entries are skipped.
        }
    }
    files.sort_by(|a, b| a.relative.cmp(&b.relative));
    Ok(files)
}

fn read_bounded(path: &Path) -> Result<Vec<u8>, WorkerError> {
    let file = File::open(path).map_err(|_| unreadable())?;
    let mut bytes = Vec::new();
    file.take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| unreadable())?;
    if bytes.len() as u64 > MAX_FILE_BYTES {
        return Err(too_large());
    }
    Ok(bytes)
}

fn scrub_jsonl(
    lines: Vec<Vec<u8>>,
    scrubber: &Scrubber,
    mut preview: Option<&mut Option<String>>,
) -> Result<(Vec<u8>, u32), WorkerError> {
    let mut bytes = Vec::new();
    let mut replacements = 0u32;
    for line in lines {
        reject_reserved_tokens(&line)?;
        let result = scrubber.scrub_line(&line)?;
        replacements = replacements
            .checked_add(result.replacements)
            .ok_or_else(too_large)?;
        if let Some(slot) = preview.as_deref_mut()
            && slot.is_none()
        {
            let value: Value = serde_json::from_slice(&result.bytes).map_err(|_| unreadable())?;
            *slot = prompt_preview(&value);
        }
        bytes.extend(result.bytes);
        bytes.push(b'\n');
    }
    Ok((bytes, replacements))
}

fn reject_reserved_tokens(bytes: &[u8]) -> Result<(), WorkerError> {
    // Check the source, not just scrubbed output: a PEM/exact-secret match may
    // remove a token, but input with reserved tokens must still be refused.
    if [tokens::WORKSPACE_TOKEN, tokens::SESSION_TOKEN]
        .iter()
        .any(|token| {
            bytes
                .windows(token.len())
                .any(|window| window == token.as_bytes())
        })
    {
        return Err(session_error(
            "SESSION_UNREADABLE",
            "session contains a reserved token",
        ));
    }
    Ok(())
}

fn prompt_preview(value: &Value) -> Option<String> {
    if value.get("type")?.as_str()? != "user" {
        return None;
    }
    let content = value.get("message")?.get("content")?;
    let text = content.as_str().or_else(|| {
        content
            .as_array()?
            .iter()
            .find(|block| block.get("type").and_then(Value::as_str) == Some("text"))?
            .get("text")?
            .as_str()
    })?;
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() || collapsed.starts_with('<') {
        return None;
    }
    let mut chars = collapsed.chars();
    let mut preview: String = chars.by_ref().take(120).collect();
    if chars.next().is_some() {
        preview.push('…');
    }
    Some(preview)
}

fn compare_versions(a: &str, b: &str) -> Ordering {
    // Stop at the suffix and compare digit strings, avoiding integer overflow.
    fn components(version: &str) -> Vec<&str> {
        version
            .split(|c: char| !c.is_ascii_digit() && c != '.')
            .next()
            .unwrap_or("")
            .split('.')
            .map(|part| part.trim_start_matches('0'))
            .collect()
    }
    let a = components(a);
    let b = components(b);
    for index in 0..a.len().max(b.len()) {
        let a = a.get(index).copied().unwrap_or("");
        let b = b.get(index).copied().unwrap_or("");
        let ordering = a.len().cmp(&b.len()).then(a.cmp(b));
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
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
