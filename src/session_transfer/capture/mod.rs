pub mod claude;
pub mod codex;
use super::{SessionAgent, SessionCapture, session_error};
use crate::{error::WorkerError, rooted_fs::RootedDir};
use std::{
    ffi::{CStr, CString},
    fs::{File, OpenOptions},
    io::Read,
    os::{
        fd::{AsRawFd, FromRawFd, RawFd},
        unix::{
            ffi::OsStrExt,
            fs::{MetadataExt, OpenOptionsExt},
        },
    },
    path::{Component, Path, PathBuf},
};
pub fn capture_for(agent: SessionAgent) -> Box<dyn SessionCapture> {
    match agent {
        SessionAgent::Claude => Box::new(claude::ClaudeCapture),
        SessionAgent::Codex => Box::new(codex::CodexCapture),
    }
}
pub fn read_complete_lines(path: &Path, max_bytes: u64) -> Result<Vec<Vec<u8>>, WorkerError> {
    let bytes = read_session_bytes(&open_session_file(path)?, max_bytes)?;
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

/// A read-only main transcript, anchored at its native store. Retain every
/// directory descriptor so a rename or symlink swap cannot redirect a read.
pub(super) struct RootedSessionFile {
    root: RootedDir,
    directories: Vec<SessionDirectory>,
    file: File,
}

struct SessionDirectory {
    name: CString,
    file: File,
    device: u64,
    inode: u64,
}

impl RootedSessionFile {
    pub(super) fn open(store_root: &Path, source: &Path) -> Result<Self, WorkerError> {
        // Do not canonicalize a discovered path: that would follow the very
        // substitutions this reader is intended to refuse.
        let relative = source.strip_prefix(store_root).map_err(|_| unreadable())?;
        let components = relative
            .components()
            .map(|component| match component {
                Component::Normal(name) => CString::new(name.as_bytes()).map_err(|_| unreadable()),
                _ => Err(unreadable()),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (name, parents) = components.split_last().ok_or_else(unreadable)?;
        let root = RootedDir::open_anchored_absolute(store_root).map_err(|_| unreadable())?;
        let mut directories = Vec::new();
        let mut parent = root.raw_directory_fd();
        for name in parents {
            let file = open_descriptor_at(parent, name, libc::O_DIRECTORY)?;
            let metadata = file.metadata().map_err(|_| unreadable())?;
            parent = file.as_raw_fd();
            directories.push(SessionDirectory {
                name: name.clone(),
                file,
                device: metadata.dev(),
                inode: metadata.ino(),
            });
        }
        let file = open_descriptor_at(parent, name, 0)?;
        require_regular(&file)?;
        let opened = Self {
            root,
            directories,
            file,
        };
        opened.verify_bound()?;
        Ok(opened)
    }

    pub(super) fn metadata(&self) -> Result<std::fs::Metadata, WorkerError> {
        require_regular(&self.file)
    }

    pub(super) fn read_bytes(&self, max_bytes: u64) -> Result<Vec<u8>, WorkerError> {
        self.with_file(|file| read_session_bytes(file, max_bytes))
    }

    /// Discovery supplies its own bounded first-line reader; it uses exactly
    /// the same descriptor chain and post-read binding checks as capture.
    pub(super) fn with_file<T>(
        &self,
        read: impl FnOnce(&File) -> Result<T, WorkerError>,
    ) -> Result<T, WorkerError> {
        self.verify_bound()?;
        let result = read(&self.file)?;
        self.verify_bound()?;
        Ok(result)
    }

    fn verify_bound(&self) -> Result<(), WorkerError> {
        self.root.verify_bound().map_err(|_| unreadable())?;
        let mut parent = self.root.raw_directory_fd();
        for directory in &self.directories {
            let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
            // SAFETY: parent is retained, the name is NUL-terminated, and the
            // output buffer is valid. Never follow a substituted binding.
            if unsafe {
                libc::fstatat(
                    parent,
                    directory.name.as_ptr(),
                    metadata.as_mut_ptr(),
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            } < 0
            {
                return Err(unreadable());
            }
            // SAFETY: successful fstatat initialized the buffer.
            let metadata = unsafe { metadata.assume_init() };
            let held = directory.file.metadata().map_err(|_| unreadable())?;
            if metadata.st_mode & libc::S_IFMT != libc::S_IFDIR
                || (metadata.st_dev as u64, metadata.st_ino) != (directory.device, directory.inode)
                || (held.dev(), held.ino()) != (directory.device, directory.inode)
            {
                return Err(unreadable());
            }
            parent = directory.file.as_raw_fd();
        }
        Ok(())
    }
}

fn open_descriptor_at(parent: RawFd, name: &CStr, flags: libc::c_int) -> Result<File, WorkerError> {
    // SAFETY: the caller retains parent and the name is NUL-terminated.
    let descriptor = unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC | flags,
        )
    };
    if descriptor < 0 {
        return Err(unreadable());
    }
    // SAFETY: successful openat transfers a fresh descriptor to File.
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

fn unreadable() -> WorkerError {
    session_error("SESSION_UNREADABLE", "cannot read native session safely")
}

pub(super) fn unsupported_version() -> WorkerError {
    session_error("SESSION_UNREADABLE", "unsupported agent version text")
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

pub(super) fn read_session_bytes(file: &File, max_bytes: u64) -> Result<Vec<u8>, WorkerError> {
    if require_regular(file)?.len() > max_bytes {
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

#[cfg(test)]
mod security_tests {
    use super::*;
    use std::{fs, os::unix::fs::symlink};

    #[test]
    fn sec_fix_rooted_read_rechecks_every_directory_binding_after_reading() {
        for depth in 0..4 {
            for use_symlink in [false, true] {
                let temp = tempfile::tempdir().unwrap();
                let ancestor = temp.path().join("agent");
                let store = ancestor.join("sessions");
                let parent = store.join("year/day");
                fs::create_dir_all(&parent).unwrap();
                let source = parent.join("main.jsonl");
                fs::write(&source, b"{}\n").unwrap();
                let opened = RootedSessionFile::open(&store, &source).unwrap();
                let replaced = [&ancestor, &store, &store.join("year"), &parent][depth].to_owned();
                let outside = temp.path().join("outside");
                fs::create_dir(&outside).unwrap();
                fs::write(outside.join("main.jsonl"), b"SYNTHETIC_OUTSIDE_MAIN_BYTES").unwrap();
                let result = opened.with_file(|file| {
                    let bytes = read_session_bytes(file, 100)?;
                    assert_eq!(bytes, b"{}\n");
                    // Deterministically interleave substitution before the
                    // post-read check, without sleeps or synthetic fs results.
                    fs::rename(&replaced, temp.path().join("original")).unwrap();
                    if use_symlink {
                        symlink(&outside, &replaced).unwrap();
                    } else {
                        fs::create_dir(&replaced).unwrap();
                    }
                    Ok(bytes)
                });
                assert_eq!(result.unwrap_err().public_code(), "SESSION_UNREADABLE");
            }
        }
    }

    #[test]
    fn sec_fix_rooted_read_keeps_final_file_descriptor_after_name_substitution() {
        let temp = tempfile::tempdir().unwrap();
        let store = temp.path().join("sessions");
        fs::create_dir(&store).unwrap();
        let source = store.join("main.jsonl");
        fs::write(&source, b"{}\n").unwrap();
        let opened = RootedSessionFile::open(&store, &source).unwrap();
        let outside = temp.path().join("outside");
        fs::write(&outside, b"SYNTHETIC_OUTSIDE_MAIN_BYTES").unwrap();
        fs::remove_file(&source).unwrap();
        symlink(&outside, &source).unwrap();
        assert_eq!(opened.read_bytes(100).unwrap(), b"{}\n");
        assert!(RootedSessionFile::open(&store, &source).is_err());
    }
}
