use super::super::{claude_dir::claude_project_dir, contracts::*, scrub::Scrubber, tokens};
use super::{complete_lines, open_session_file, read_session_bytes, relative_inside};
use crate::{error::WorkerError, rooted_fs::RootedDir};
use serde_json::Value;
use std::{
    cmp::Ordering,
    ffi::{CStr, CString},
    fs::{self, DirEntry, File},
    io::{self, BufRead, BufReader, Read},
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
        unix::fs::MetadataExt,
    },
    path::{Path, PathBuf},
    sync::Arc,
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
                        && first_cwd(&path)
                            .is_some_and(|cwd| relative_inside(cx.project_root, &cwd).is_ok())
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
        let main_file = open_session_file(source)?;
        let metadata = main_file.metadata().map_err(|_| unreadable())?;
        let mut caps = RawCaps::default();
        caps.add_file(metadata.len())?;
        let sidecar_root = source.parent().ok_or_else(unreadable)?.join(id);
        let sidecars = collect_sidecars(&sidecar_root, &mut caps)?;
        let raw = read_session_bytes(main_file, MAX_FILE_BYTES)?;
        caps.observe(metadata.len(), raw.len() as u64)?;
        let lines = complete_lines(&raw)?;
        drop(raw);
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
            let raw = read_bounded(&sidecar)?;
            // Include partial JSONL tails in the raw cap, even though they are
            // dropped from the package. Never stat the path again for size.
            caps.observe(sidecar.raw_bytes, raw.len() as u64)?;
            let extension = Path::new(&sidecar.relative)
                .extension()
                .and_then(|s| s.to_str());
            let (bytes, replacements) = match extension {
                Some("jsonl") => scrub_jsonl(complete_lines(&raw)?, cx.scrubber, None)?,
                Some("json") => {
                    reject_reserved_tokens(&raw)?;
                    let result = cx.scrubber.scrub_line(&raw)?;
                    (result.bytes, result.replacements)
                }
                _ => (raw, 0),
            };
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

// Native project-directory encoding is only a lookup hint: distinct checkout
// names can collide. Inspect complete first lines before ranking by mtime.
fn first_cwd(path: &Path) -> Option<PathBuf> {
    const MAX_DISCOVERY_BYTES: u64 = 1 << 20;
    const MAX_DISCOVERY_LINES: usize = 64;
    let file = open_session_file(path).ok()?;
    let mut reader = BufReader::new(file.take(MAX_DISCOVERY_BYTES + 1));
    let mut line = Vec::new();
    let mut bytes = 0;
    for _ in 0..MAX_DISCOVERY_LINES {
        line.clear();
        bytes += reader.read_until(b'\n', &mut line).ok()? as u64;
        if bytes > MAX_DISCOVERY_BYTES || !line.ends_with(b"\n") {
            return None;
        }
        let value: Value = serde_json::from_slice(&line).ok()?;
        if let Some(cwd) = value.get("cwd").and_then(Value::as_str) {
            return Some(PathBuf::from(cwd));
        }
    }
    None
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

    fn observe(&mut self, original: u64, read_bytes: u64) -> Result<(), WorkerError> {
        // Actual reads (including partial tails) enforce growth caps. Inventory
        // sizes remain a conservative bound if a live file shrinks.
        if read_bytes > MAX_FILE_BYTES {
            return Err(too_large());
        }
        self.add_bytes(read_bytes.saturating_sub(original))
    }
}

struct SidecarFile {
    root: Arc<RootedDir>,
    components: Vec<CString>,
    relative: String,
    raw_bytes: u64,
    device: u64,
    inode: u64,
}

fn collect_sidecars(root: &Path, caps: &mut RawCaps) -> Result<Vec<SidecarFile>, WorkerError> {
    match fs::symlink_metadata(root) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => return Ok(Vec::new()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(_) => return Err(unreadable()),
    }
    // RootedDir opens the root and its parents with O_NOFOLLOW and lets reads
    // detect a renamed/replaced root, rather than reopening the display path.
    let root = Arc::new(RootedDir::open(root).map_err(|_| unreadable())?);
    let mut directories = vec![(
        open_at(root.raw_directory_fd(), c".", libc::O_DIRECTORY)?,
        Vec::<CString>::new(),
    )];
    let mut files = Vec::new();
    while let Some((directory, components)) = directories.pop() {
        for name in directory_names(directory.as_raw_fd())? {
            let metadata = stat_at(directory.as_raw_fd(), &name)?;
            let mut child = components.clone();
            child.push(name.clone());
            match metadata.st_mode & libc::S_IFMT {
                libc::S_IFDIR => {
                    directories.push((
                        open_at(directory.as_raw_fd(), &name, libc::O_DIRECTORY)?,
                        child,
                    ));
                }
                libc::S_IFREG => {
                    let file = open_regular_at(directory.as_raw_fd(), &name)?;
                    let opened = file.metadata().map_err(|_| unreadable())?;
                    caps.add_file(opened.len())?;
                    let relative = child
                        .iter()
                        .map(|part| part.to_str().map_err(|_| unreadable()))
                        .collect::<Result<Vec<_>, _>>()?
                        .join("/");
                    files.push(SidecarFile {
                        root: Arc::clone(&root),
                        components: child,
                        relative: format!("{CLAUDE_SIDECAR_DIR}/{relative}"),
                        raw_bytes: opened.len(),
                        device: opened.dev(),
                        inode: opened.ino(),
                    });
                }
                // Inventory never follows symlinks, FIFOs, or device entries.
                _ => {}
            }
        }
    }
    root.verify_bound().map_err(|_| unreadable())?;
    files.sort_by(|a, b| a.relative.cmp(&b.relative));
    Ok(files)
}

fn read_bounded(sidecar: &SidecarFile) -> Result<Vec<u8>, WorkerError> {
    sidecar.root.verify_bound().map_err(|_| unreadable())?;
    let mut parent = open_at(sidecar.root.raw_directory_fd(), c".", libc::O_DIRECTORY)?;
    let (name, directories) = sidecar.components.split_last().ok_or_else(unreadable)?;
    for directory in directories {
        parent = open_at(parent.as_raw_fd(), directory, libc::O_DIRECTORY)?;
    }
    let file = open_regular_at(parent.as_raw_fd(), name)?;
    let metadata = file.metadata().map_err(|_| unreadable())?;
    if (metadata.dev(), metadata.ino()) != (sidecar.device, sidecar.inode) {
        return Err(unreadable());
    }
    let bytes = read_session_bytes(file, MAX_FILE_BYTES)?;
    sidecar.root.verify_bound().map_err(|_| unreadable())?;
    Ok(bytes)
}

fn open_at(parent: RawFd, name: &CStr, flags: libc::c_int) -> Result<OwnedFd, WorkerError> {
    // SAFETY: parent is retained by the caller and name is NUL-terminated.
    let descriptor = unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK | flags,
        )
    };
    if descriptor < 0 {
        return Err(unreadable());
    }
    // SAFETY: successful openat transfers ownership of a fresh descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(descriptor) })
}

fn open_regular_at(parent: RawFd, name: &CStr) -> Result<File, WorkerError> {
    let file = File::from(open_at(parent, name, 0)?);
    // File::metadata is fstat, so a raced FIFO/device cannot pass this check.
    if !file.metadata().map_err(|_| unreadable())?.is_file() {
        return Err(unreadable());
    }
    Ok(file)
}

fn stat_at(parent: RawFd, name: &CStr) -> Result<libc::stat, WorkerError> {
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: parent is live, name is NUL-terminated, metadata is writable.
    if unsafe {
        libc::fstatat(
            parent,
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } < 0
    {
        return Err(unreadable());
    }
    // SAFETY: successful fstatat initialized metadata.
    Ok(unsafe { metadata.assume_init() })
}

fn directory_names(directory: RawFd) -> Result<Vec<CString>, WorkerError> {
    // Use a fresh directory description/offset and transfer its ownership to
    // fdopendir. Enumeration is fd-relative, not a reopened filesystem path.
    let raw = open_at(directory, c".", libc::O_DIRECTORY)?.into_raw_fd();
    // SAFETY: raw is a fresh owned directory fd; fdopendir consumes it on success.
    let stream = unsafe { libc::fdopendir(raw) };
    if stream.is_null() {
        // SAFETY: fdopendir failed and did not consume the descriptor.
        drop(unsafe { OwnedFd::from_raw_fd(raw) });
        return Err(unreadable());
    }
    let stream = DirectoryStream(stream);
    let mut names = Vec::new();
    loop {
        // SAFETY: errno is thread-local; stream is owned and used sequentially.
        unsafe { *errno_pointer() = 0 };
        let entry = unsafe { libc::readdir(stream.0) };
        if entry.is_null() {
            // SAFETY: errno_pointer returns this thread's live errno storage.
            if unsafe { *errno_pointer() } != 0 {
                return Err(unreadable());
            }
            break;
        }
        // SAFETY: readdir's name is NUL-terminated and valid until the next call.
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if name.to_bytes() != b"." && name.to_bytes() != b".." {
            names.push(name.to_owned());
        }
    }
    Ok(names)
}

struct DirectoryStream(*mut libc::DIR);
impl Drop for DirectoryStream {
    fn drop(&mut self) {
        // SAFETY: this wrapper uniquely owns the DIR and its descriptor.
        unsafe { libc::closedir(self.0) };
    }
}

fn errno_pointer() -> *mut libc::c_int {
    // SAFETY: libc returns the calling thread's errno storage.
    #[cfg(target_vendor = "apple")]
    unsafe {
        libc::__error()
    }
    #[cfg(not(target_vendor = "apple"))]
    unsafe {
        libc::__errno_location()
    }
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

#[cfg(test)]
mod review_fix_tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn review_fix_sidecar_replacement_with_symlink_is_refused() {
        let temp = tempfile::tempdir().unwrap();
        let sidecar = temp.path().join("sidecar");
        fs::create_dir(&sidecar).unwrap();
        let source = sidecar.join("tool-result.txt");
        fs::write(&source, b"public").unwrap();
        let private = temp.path().join("unrelated-private.txt");
        fs::write(&private, b"synthetic private bytes").unwrap();
        let mut caps = RawCaps::default();
        let files = collect_sidecars(&sidecar, &mut caps).unwrap();
        assert_eq!(files.len(), 1);

        // Interleave a rename/symlink between inventory and the capture read.
        fs::remove_file(&source).unwrap();
        symlink(&private, &source).unwrap();
        let result = read_bounded(&files[0]).and_then(|bytes| {
            caps.observe(files[0].raw_bytes, bytes.len() as u64)?;
            Ok(bytes)
        });
        assert!(
            result.is_err(),
            "capture followed the replacement: {result:?}"
        );
    }

    #[test]
    fn review_fix_sidecar_parent_replacement_with_symlink_is_refused() {
        let temp = tempfile::tempdir().unwrap();
        let sidecar = temp.path().join("sidecar");
        fs::create_dir(&sidecar).unwrap();
        fs::write(sidecar.join("tool-result.txt"), b"public").unwrap();
        let private = temp.path().join("unrelated-private");
        fs::create_dir(&private).unwrap();
        fs::write(private.join("tool-result.txt"), b"synthetic private bytes").unwrap();
        let mut caps = RawCaps::default();
        let files = collect_sidecars(&sidecar, &mut caps).unwrap();
        assert_eq!(files.len(), 1);

        // A final-component O_NOFOLLOW alone would not stop this interleaving.
        fs::rename(&sidecar, temp.path().join("old-sidecar")).unwrap();
        symlink(&private, &sidecar).unwrap();
        let result = read_bounded(&files[0]).and_then(|bytes| {
            caps.observe(files[0].raw_bytes, bytes.len() as u64)?;
            Ok(bytes)
        });
        assert!(
            result.is_err(),
            "capture followed the replaced parent: {result:?}"
        );
    }

    #[test]
    fn review_fix_nested_sidecar_directory_replacement_with_symlink_is_refused() {
        let temp = tempfile::tempdir().unwrap();
        let sidecar = temp.path().join("sidecar");
        let nested = sidecar.join("subagents");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join("tool-result.txt"), b"public").unwrap();
        let private = temp.path().join("unrelated-private");
        fs::create_dir(&private).unwrap();
        fs::write(private.join("tool-result.txt"), b"synthetic private bytes").unwrap();
        let files = collect_sidecars(&sidecar, &mut RawCaps::default()).unwrap();
        fs::rename(&nested, sidecar.join("old-subagents")).unwrap();
        symlink(&private, &nested).unwrap();
        assert_eq!(
            read_bounded(&files[0]).unwrap_err().public_code(),
            "SESSION_UNREADABLE"
        );
    }

    #[test]
    fn review_fix_sidecar_regular_file_substitution_is_refused() {
        let temp = tempfile::tempdir().unwrap();
        let sidecar = temp.path().join("sidecar");
        fs::create_dir(&sidecar).unwrap();
        let source = sidecar.join("tool-result.txt");
        fs::write(&source, b"public").unwrap();
        let files = collect_sidecars(&sidecar, &mut RawCaps::default()).unwrap();
        // Retain the inventoried inode to avoid filesystem inode reuse.
        fs::rename(&source, sidecar.join("old-tool-result.txt")).unwrap();
        fs::write(&source, b"substituted bytes").unwrap();
        assert_eq!(
            read_bounded(&files[0]).unwrap_err().public_code(),
            "SESSION_UNREADABLE"
        );
    }

    #[test]
    fn review_fix_raw_caps_include_growth_in_a_partial_jsonl_tail() {
        let temp = tempfile::tempdir().unwrap();
        let sidecar = temp.path().join("sidecar");
        fs::create_dir(&sidecar).unwrap();
        let source = sidecar.join("agent.jsonl");
        fs::write(&source, b"{}\n").unwrap();
        let mut caps = RawCaps {
            bytes: MAX_PACKAGE_BYTES - 3,
            files: 1,
        };
        let files = collect_sidecars(&sidecar, &mut caps).unwrap();
        fs::write(&source, b"{}\n{").unwrap();
        let raw = read_bounded(&files[0]).unwrap();
        assert_eq!(complete_lines(&raw).unwrap(), vec![b"{}".to_vec()]);
        assert_eq!(
            caps.observe(files[0].raw_bytes, raw.len() as u64)
                .unwrap_err()
                .public_code(),
            "SESSION_TOO_LARGE"
        );
    }

    #[test]
    fn review_fix_opened_transcript_reads_original_fd_after_symlink_substitution() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("main.jsonl");
        fs::write(&source, b"{}\n").unwrap();
        let file = open_session_file(&source).unwrap();
        let private = temp.path().join("unrelated-private");
        fs::write(&private, b"synthetic private bytes").unwrap();
        fs::remove_file(&source).unwrap();
        symlink(&private, &source).unwrap();
        assert_eq!(read_session_bytes(file, MAX_FILE_BYTES).unwrap(), b"{}\n");
    }
}
