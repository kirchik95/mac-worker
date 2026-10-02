use std::{
    ffi::{CStr, CString},
    fs::File,
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::fs::PermissionsExt,
    },
    path::{Path, PathBuf},
};

use crate::{error::WorkerError, rooted_fs::RootedDir};

pub struct StoreWriter {
    root: PathBuf,
    directory: StoreRoot,
}

// rooted_fs binds directory entries; the filesystem root has no parent entry.
enum StoreRoot {
    Entry(RootedDir),
    Filesystem(OwnedFd),
}

impl StoreRoot {
    fn verify_bound(&self) -> io::Result<()> {
        match self {
            Self::Entry(directory) => directory.verify_bound(),
            Self::Filesystem(_) => Ok(()),
        }
    }

    fn raw_directory_fd(&self) -> RawFd {
        match self {
            Self::Entry(directory) => directory.raw_directory_fd(),
            Self::Filesystem(descriptor) => descriptor.as_raw_fd(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    Created,
    Unchanged,
}

impl StoreWriter {
    pub fn open(root: &Path) -> Result<Self, WorkerError> {
        // Only the store root may be resolved through symlinks. All subsequent
        // traversal is relative to the retained rooted_fs descriptor.
        let physical = root.canonicalize().map_err(placement_error)?;
        let directory = if physical == Path::new("/") {
            StoreRoot::Filesystem(open_directory_at(libc::AT_FDCWD, c"/").map_err(placement_error)?)
        } else {
            StoreRoot::Entry(RootedDir::open(&physical).map_err(placement_error)?)
        };
        Ok(Self {
            root: root.to_path_buf(),
            directory,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn write_file(&self, relative: &str, bytes: &[u8]) -> Result<WriteOutcome, WorkerError> {
        let result = (|| {
            let components = components(relative)?;
            self.directory.verify_bound()?;
            let parent = self.parent(&components, true)?;
            let name = components.last().expect("validated nonempty path");
            if let Some(existing) = read_at(parent.as_raw_fd(), name, bytes.len() as u64)? {
                return compare(&existing, bytes);
            }
            let temporary =
                CString::new(format!(".session-transfer-{}", uuid::Uuid::new_v4())).unwrap();
            // SAFETY: parent is live and the unique sibling name is NUL-terminated.
            let descriptor = unsafe {
                libc::openat(
                    parent.as_raw_fd(),
                    temporary.as_ptr(),
                    libc::O_WRONLY
                        | libc::O_CREAT
                        | libc::O_EXCL
                        | libc::O_NOFOLLOW
                        | libc::O_CLOEXEC,
                    0o600,
                )
            };
            let mut file = File::from(owned_fd(descriptor)?);
            // Arm cleanup only after exclusive creation succeeds.
            let _staged = StagedFile {
                parent: parent.as_raw_fd(),
                name: &temporary,
            };
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            file.write_all(bytes)?;
            file.sync_all()?;
            self.directory.verify_bound()?;
            match rename_no_replace(parent.as_raw_fd(), &temporary, name) {
                Ok(()) => {
                    sync_fd(parent.as_raw_fd())?;
                    self.directory.verify_bound()?;
                    Ok(WriteOutcome::Created)
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    let existing = read_at(parent.as_raw_fd(), name, bytes.len() as u64)?
                        .ok_or_else(|| io::Error::from_raw_os_error(libc::ESTALE))?;
                    compare(&existing, bytes)
                }
                Err(error) => Err(error),
            }
        })();
        result.map_err(placement_error)
    }

    pub fn read_file(
        &self,
        relative: &str,
        max_bytes: u64,
    ) -> Result<Option<Vec<u8>>, WorkerError> {
        let result = (|| {
            let components = components(relative)?;
            self.directory.verify_bound()?;
            let parent = match self.parent(&components, false) {
                Ok(parent) => parent,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error),
            };
            let bytes = read_at(parent.as_raw_fd(), components.last().unwrap(), max_bytes)?;
            self.directory.verify_bound()?;
            Ok(bytes)
        })();
        result.map_err(placement_error)
    }

    // rooted_fs's child and read helpers require private modes on *existing*
    // directories/files. Native agent stores need not have those modes, and
    // this contract preserves them. Keep that less restrictive traversal local.
    fn parent(&self, components: &[CString], create: bool) -> io::Result<OwnedFd> {
        let mut current = open_directory_at(self.directory.raw_directory_fd(), c".")?;
        for component in &components[..components.len() - 1] {
            current = match open_directory_at(current.as_raw_fd(), component) {
                Ok(child) => child,
                Err(error) if create && error.kind() == io::ErrorKind::NotFound => {
                    // SAFETY: the directory fd is live and component is a plain name.
                    let created =
                        unsafe { libc::mkdirat(current.as_raw_fd(), component.as_ptr(), 0o700) };
                    if created < 0 {
                        let error = io::Error::last_os_error();
                        if error.kind() != io::ErrorKind::AlreadyExists {
                            return Err(error);
                        }
                    }
                    let child = open_directory_at(current.as_raw_fd(), component)?;
                    if created == 0 {
                        // SAFETY: child is the newly opened directory descriptor.
                        cvt(unsafe { libc::fchmod(child.as_raw_fd(), 0o700) })?;
                        sync_fd(child.as_raw_fd())?;
                        sync_fd(current.as_raw_fd())?;
                    }
                    child
                }
                Err(error) => return Err(error),
            };
        }
        Ok(current)
    }
}

fn placement_error(error: io::Error) -> WorkerError {
    // Never attach the caller's absolute store path.
    WorkerError::task(
        "SESSION_PLACEMENT_FAILED",
        format!("session store operation failed: {error}"),
    )
}

fn components(relative: &str) -> io::Result<Vec<CString>> {
    relative
        .split('/')
        .map(|component| {
            if component.is_empty() || component == "." || component == ".." {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid session store relative path",
                ));
            }
            CString::new(component).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid session store relative path",
                )
            })
        })
        .collect()
}

fn compare(existing: &[u8], bytes: &[u8]) -> io::Result<WriteOutcome> {
    if existing == bytes {
        Ok(WriteOutcome::Unchanged)
    } else {
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "session store file has different content",
        ))
    }
}

fn cvt(result: libc::c_int) -> io::Result<()> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn owned_fd(descriptor: RawFd) -> io::Result<OwnedFd> {
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful openat transfers a fresh descriptor to this owner.
    Ok(unsafe { OwnedFd::from_raw_fd(descriptor) })
}

fn open_directory_at(parent: RawFd, name: &CStr) -> io::Result<OwnedFd> {
    // SAFETY: parent is retained by the caller, name is NUL-terminated.
    owned_fd(unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    })
}

fn sync_fd(descriptor: RawFd) -> io::Result<()> {
    // SAFETY: caller retains descriptor throughout fsync.
    cvt(unsafe { libc::fsync(descriptor) })
}

fn read_at(parent: RawFd, name: &CStr, maximum: u64) -> io::Result<Option<Vec<u8>>> {
    // lstat first distinguishes dangling symlinks from absent files.
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: metadata is writable, parent is live, name is NUL-terminated.
    let result = unsafe {
        libc::fstatat(
            parent,
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result < 0 {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::NotFound {
            Ok(None)
        } else {
            Err(error)
        };
    }
    // SAFETY: fstatat succeeded and initialized metadata.
    let metadata = unsafe { metadata.assume_init() };
    if metadata.st_mode & libc::S_IFMT != libc::S_IFREG {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "session store target is not a regular file",
        ));
    }
    // SAFETY: parent is live, name is NUL-terminated. NONBLOCK avoids FIFO races.
    let descriptor = owned_fd(unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
        )
    })?;
    let mut file = File::from(descriptor);
    let opened = file.metadata()?;
    if !opened.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "session store target is not a regular file",
        ));
    }
    if opened.len() > maximum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "session store file exceeds limit",
        ));
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "session store file exceeds limit",
        ));
    }
    Ok(Some(bytes))
}

struct StagedFile<'a> {
    parent: RawFd,
    name: &'a CStr,
}
impl Drop for StagedFile<'_> {
    fn drop(&mut self) {
        // SAFETY: the guard is dropped before its parent descriptor and name.
        unsafe {
            libc::unlinkat(self.parent, self.name.as_ptr(), 0);
        }
    }
}

fn rename_no_replace(parent: RawFd, source: &CStr, target: &CStr) -> io::Result<()> {
    // SAFETY: both names are NUL-terminated siblings under a retained fd.
    #[cfg(target_vendor = "apple")]
    return cvt(unsafe {
        libc::renameatx_np(
            parent,
            source.as_ptr(),
            parent,
            target.as_ptr(),
            libc::RENAME_EXCL,
        )
    });
    #[cfg(any(target_os = "linux", target_os = "android"))]
    return cvt(unsafe {
        libc::renameat2(
            parent,
            source.as_ptr(),
            parent,
            target.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    });
    #[cfg(not(any(target_vendor = "apple", target_os = "linux", target_os = "android")))]
    {
        let _ = (parent, source, target);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "atomic no-replace rename unavailable",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        os::unix::fs::{PermissionsExt, symlink},
        sync::{
            Arc, Barrier,
            atomic::{AtomicBool, Ordering},
        },
    };
    use tempfile::tempdir;

    fn placement_error<T>(result: Result<T, WorkerError>) {
        assert!(matches!(
            result,
            Err(WorkerError::Task {
                code: "SESSION_PLACEMENT_FAILED",
                ..
            })
        ));
    }

    #[test]
    fn symlinked_parent_is_refused() {
        let temp = tempdir().unwrap();
        let outside = tempdir().unwrap();
        symlink(outside.path(), temp.path().join("parent")).unwrap();
        let store = StoreWriter::open(temp.path()).unwrap();
        placement_error(store.write_file("parent/session", b"data"));
        placement_error(store.read_file("parent/session", 100));
        assert!(!outside.path().join("session").exists());
    }

    #[test]
    fn symlinked_target_is_refused_even_if_identical_or_dangling() {
        let temp = tempdir().unwrap();
        let outside = tempdir().unwrap();
        fs::write(outside.path().join("data"), b"data").unwrap();
        symlink(outside.path().join("data"), temp.path().join("session")).unwrap();
        symlink(outside.path().join("missing"), temp.path().join("dangling")).unwrap();
        let store = StoreWriter::open(temp.path()).unwrap();
        for name in ["session", "dangling"] {
            placement_error(store.write_file(name, b"data"));
            placement_error(store.read_file(name, 100));
        }
        assert_eq!(fs::read(outside.path().join("data")).unwrap(), b"data");
    }

    #[test]
    fn unsafe_paths_are_refused() {
        let temp = tempdir().unwrap();
        let store = StoreWriter::open(temp.path()).unwrap();
        for path in [
            "../escape",
            "a/../escape",
            "/absolute",
            "",
            "a//b",
            "a/",
            "./a",
            "a/./b",
            "a\0b",
        ] {
            placement_error(store.write_file(path, b"data"));
            placement_error(store.read_file(path, 100));
        }
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 0);
    }

    #[test]
    fn existing_content_is_idempotent_but_never_overwritten() {
        let temp = tempdir().unwrap();
        let store = StoreWriter::open(temp.path()).unwrap();
        assert_eq!(
            store.write_file("a/b/session", b"data").unwrap(),
            WriteOutcome::Created
        );
        assert_eq!(
            store.write_file("a/b/session", b"data").unwrap(),
            WriteOutcome::Unchanged
        );
        placement_error(store.write_file("a/b/session", b"other"));
        assert_eq!(
            store.read_file("a/b/session", 4).unwrap(),
            Some(b"data".to_vec())
        );
        placement_error(store.write_file("a/b", b"data"));
        placement_error(store.read_file("a/b", 100));
    }

    #[test]
    fn new_entries_are_private_and_existing_parent_modes_are_preserved() {
        let temp = tempdir().unwrap();
        fs::create_dir(temp.path().join("existing")).unwrap();
        fs::set_permissions(
            temp.path().join("existing"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        let store = StoreWriter::open(temp.path()).unwrap();
        store.write_file("existing/new/session", b"data").unwrap();
        for (path, mode) in [
            ("existing", 0o755),
            ("existing/new", 0o700),
            ("existing/new/session", 0o600),
        ] {
            assert_eq!(
                fs::metadata(temp.path().join(path))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                mode
            );
        }
        fs::write(temp.path().join("existing/public"), b"data").unwrap();
        fs::set_permissions(
            temp.path().join("existing/public"),
            fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert_eq!(
            store.write_file("existing/public", b"data").unwrap(),
            WriteOutcome::Unchanged
        );
    }

    #[test]
    fn root_symlink_is_allowed_but_missing_or_file_root_is_not() {
        let temp = tempdir().unwrap();
        let actual = temp.path().join("actual");
        fs::create_dir(&actual).unwrap();
        let link = temp.path().join("link");
        symlink(&actual, &link).unwrap();
        let store = StoreWriter::open(&link).unwrap();
        assert_eq!(store.root(), link);
        store.write_file("session", b"data").unwrap();
        assert_eq!(fs::read(actual.join("session")).unwrap(), b"data");
        placement_error(StoreWriter::open(&temp.path().join("missing")));
        placement_error(StoreWriter::open(&actual.join("session")));
    }

    #[test]
    fn reads_are_bounded_and_missing_files_do_not_create_parents() {
        let temp = tempdir().unwrap();
        let store = StoreWriter::open(temp.path()).unwrap();
        assert_eq!(store.read_file("absent/nested/file", 100).unwrap(), None);
        assert!(!temp.path().join("absent").exists());
        store.write_file("session", b"data").unwrap();
        placement_error(store.read_file("session", 3));
        placement_error(store.read_file("session", 0));
        store.write_file("empty", b"").unwrap();
        assert_eq!(store.read_file("empty", 0).unwrap(), Some(vec![]));
    }

    #[test]
    fn concurrent_identical_writers_publish_only_complete_files() {
        let temp = tempdir().unwrap();
        let barrier = Arc::new(Barrier::new(4));
        let done = AtomicBool::new(false);
        let bytes = vec![b'x'; 1024 * 1024];
        std::thread::scope(|scope| {
            let observer_barrier = barrier.clone();
            let observed_bytes = &bytes;
            let done = &done;
            let root = temp.path();
            let observer = scope.spawn(move || {
                let store = StoreWriter::open(root).unwrap();
                observer_barrier.wait();
                loop {
                    if let Some(actual) = store
                        .read_file("nested/session", observed_bytes.len() as u64)
                        .unwrap()
                    {
                        assert_eq!(actual, *observed_bytes);
                    }
                    if done.load(Ordering::Acquire) {
                        break;
                    }
                    std::thread::yield_now();
                }
            });
            let mut threads = Vec::new();
            for _ in 0..2 {
                let barrier = barrier.clone();
                let bytes = &bytes;
                let root = temp.path();
                threads.push(scope.spawn(move || {
                    let store = StoreWriter::open(root).unwrap();
                    barrier.wait();
                    store.write_file("nested/session", bytes).unwrap()
                }));
            }
            barrier.wait();
            for thread in threads {
                assert!(matches!(
                    thread.join().unwrap(),
                    WriteOutcome::Created | WriteOutcome::Unchanged
                ));
            }
            done.store(true, Ordering::Release);
            observer.join().unwrap();
        });
        assert_eq!(fs::read(temp.path().join("nested/session")).unwrap(), bytes);
        assert_eq!(fs::read_dir(temp.path().join("nested")).unwrap().count(), 1);
    }
}
