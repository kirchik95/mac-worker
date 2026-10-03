use std::{
    ffi::{CStr, CString},
    fs::File,
    io::{self, Read, Seek, Write},
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
            if let Some(mut existing) = read_at(parent.as_raw_fd(), name, bytes.len() as u64)? {
                compare(&existing.bytes, bytes)?;
                #[cfg(test)]
                tests::before_unchanged();
                return self.finish_write(
                    &parent,
                    &components,
                    &mut existing.file,
                    bytes,
                    WriteOutcome::Unchanged,
                );
            }
            let temporary =
                CString::new(format!(".session-transfer-{}", uuid::Uuid::new_v4())).unwrap();
            // SAFETY: parent is live and the unique sibling name is NUL-terminated.
            let descriptor = unsafe {
                libc::openat(
                    parent.as_raw_fd(),
                    temporary.as_ptr(),
                    libc::O_RDWR
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
                descriptor: file.as_raw_fd(),
            };
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            file.write_all(bytes)?;
            file.sync_all()?;
            #[cfg(test)]
            tests::before_rename();
            // A pathname rename must not publish a replacement staging inode.
            verify_file_at(parent.as_raw_fd(), &temporary, file.as_raw_fd())?;
            parent.verify_bound(&self.directory, &components)?;
            match rename_no_replace(parent.as_raw_fd(), &temporary, name) {
                Ok(()) => {
                    #[cfg(test)]
                    tests::after_rename();
                    parent.verify_bound(&self.directory, &components)?;
                    verify_file_at(parent.as_raw_fd(), name, file.as_raw_fd())?;
                    self.finish_write(
                        &parent,
                        &components,
                        &mut file,
                        bytes,
                        WriteOutcome::Created,
                    )
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    let mut existing = read_at(parent.as_raw_fd(), name, bytes.len() as u64)?
                        .ok_or_else(|| io::Error::from_raw_os_error(libc::ESTALE))?;
                    compare(&existing.bytes, bytes)?;
                    #[cfg(test)]
                    tests::before_unchanged();
                    self.finish_write(
                        &parent,
                        &components,
                        &mut existing.file,
                        bytes,
                        WriteOutcome::Unchanged,
                    )
                }
                Err(error) => Err(error),
            }
        })();
        result.map_err(placement_error)
    }

    // Both outcomes are durability barriers, including retries of a rename or
    // directory creation whose fsync failed in an earlier call.
    fn finish_write(
        &self,
        parent: &ParentChain,
        components: &[CString],
        file: &mut File,
        bytes: &[u8],
        outcome: WriteOutcome,
    ) -> io::Result<WriteOutcome> {
        let name = components.last().unwrap();
        parent.verify_bound(&self.directory, components)?;
        verify_file_at(parent.as_raw_fd(), name, file.as_raw_fd())?;
        compare(&read_bytes(file, bytes.len() as u64)?, bytes)?;
        sync_fd(file.as_raw_fd())?;
        parent.sync_all()?;
        // Same-account writers may also mutate the inode in place. Recheck
        // content and all bindings after syncing, not just the earlier read.
        compare(&read_bytes(file, bytes.len() as u64)?, bytes)?;
        verify_file_at(parent.as_raw_fd(), name, file.as_raw_fd())?;
        parent.verify_bound(&self.directory, components)?;
        Ok(outcome)
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
            let name = components.last().unwrap();
            let existing = read_at(parent.as_raw_fd(), name, max_bytes)?;
            parent.verify_bound(&self.directory, &components)?;
            if let Some(existing) = existing {
                verify_file_at(parent.as_raw_fd(), name, existing.file.as_raw_fd())?;
                Ok(Some(existing.bytes))
            } else {
                Ok(None)
            }
        })();
        result.map_err(placement_error)
    }

    // rooted_fs's child and read helpers require private modes on *existing*
    // directories/files. Native agent stores need not have those modes, and
    // this contract preserves them. Keep that less restrictive traversal local.
    fn parent(&self, components: &[CString], create: bool) -> io::Result<ParentChain> {
        let mut chain = ParentChain {
            directories: vec![open_directory_at(self.directory.raw_directory_fd(), c".")?],
        };
        for component in &components[..components.len() - 1] {
            let current = chain.as_raw_fd();
            let child = match open_directory_at(current, component) {
                Ok(child) => child,
                Err(error) if create && error.kind() == io::ErrorKind::NotFound => {
                    // SAFETY: the directory fd is live and component is a plain name.
                    let created = unsafe { libc::mkdirat(current, component.as_ptr(), 0o700) };
                    if created < 0 {
                        let error = io::Error::last_os_error();
                        if error.kind() != io::ErrorKind::AlreadyExists {
                            return Err(error);
                        }
                    }
                    let child = open_directory_at(current, component)?;
                    if created == 0 {
                        // SAFETY: child is the newly opened directory descriptor.
                        cvt(unsafe { libc::fchmod(child.as_raw_fd(), 0o700) })?;
                        sync_fd(child.as_raw_fd())?;
                        sync_fd(current)?;
                    }
                    child
                }
                Err(error) => return Err(error),
            };
            chain.directories.push(child);
        }
        Ok(chain)
    }
}

struct ParentChain {
    // Retain every walked inode, including the root, until the call finishes.
    directories: Vec<OwnedFd>,
}

impl AsRawFd for ParentChain {
    fn as_raw_fd(&self) -> RawFd {
        self.directories.last().unwrap().as_raw_fd()
    }
}

impl ParentChain {
    fn verify_bound(&self, root: &StoreRoot, components: &[CString]) -> io::Result<()> {
        root.verify_bound()?;
        let mut current = open_directory_at(root.raw_directory_fd(), c".")?;
        verify_directory_identity(current.as_raw_fd(), self.directories[0].as_raw_fd())?;
        for (component, retained) in components[..components.len() - 1]
            .iter()
            .zip(&self.directories[1..])
        {
            // Rewalk by name from the root, never from a possibly displaced fd.
            current = open_directory_at(current.as_raw_fd(), component)?;
            verify_directory_identity(current.as_raw_fd(), retained.as_raw_fd())?;
        }
        root.verify_bound()
    }

    fn sync_all(&self) -> io::Result<()> {
        // Bottom-up: each entry is durable before syncing its parent's entry.
        for directory in self.directories.iter().rev() {
            sync_fd(directory.as_raw_fd())?;
        }
        Ok(())
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
    #[cfg(test)]
    {
        let is_directory = tests::record_sync(descriptor);
        if is_directory && tests::FAIL_NEXT_DIRECTORY_SYNC.with(|fail| fail.replace(false)) {
            return Err(io::Error::from_raw_os_error(libc::EIO));
        }
    }
    // SAFETY: caller retains descriptor throughout fsync.
    cvt(unsafe { libc::fsync(descriptor) })
}

fn stat_fd(descriptor: RawFd) -> io::Result<libc::stat> {
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: descriptor is live and metadata is writable.
    cvt(unsafe { libc::fstat(descriptor, metadata.as_mut_ptr()) })?;
    // SAFETY: fstat succeeded and initialized metadata.
    Ok(unsafe { metadata.assume_init() })
}

fn stat_at(parent: RawFd, name: &CStr) -> io::Result<libc::stat> {
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: metadata is writable, parent is live, name is NUL-terminated.
    cvt(unsafe {
        libc::fstatat(
            parent,
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    })?;
    // SAFETY: fstatat succeeded and initialized metadata.
    Ok(unsafe { metadata.assume_init() })
}

fn same_inode(left: &libc::stat, right: &libc::stat) -> bool {
    left.st_dev == right.st_dev && left.st_ino == right.st_ino
}

fn verify_directory_identity(current: RawFd, retained: RawFd) -> io::Result<()> {
    if same_inode(&stat_fd(current)?, &stat_fd(retained)?) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "session store parent directory inode changed",
        ))
    }
}

fn verify_file_at(parent: RawFd, name: &CStr, descriptor: RawFd) -> io::Result<()> {
    let target = stat_at(parent, name)?;
    let written = stat_fd(descriptor)?;
    if target.st_mode & libc::S_IFMT == libc::S_IFREG
        && written.st_mode & libc::S_IFMT == libc::S_IFREG
        && same_inode(&target, &written)
    {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "session store publication inode or type changed",
        ))
    }
}

struct ReadFile {
    file: File,
    bytes: Vec<u8>,
}

fn read_at(parent: RawFd, name: &CStr, maximum: u64) -> io::Result<Option<ReadFile>> {
    // lstat first distinguishes dangling symlinks from absent files.
    let metadata = match stat_at(parent, name) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
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
    let bytes = read_bytes(&mut file, maximum)?;
    verify_file_at(parent, name, file.as_raw_fd())?;
    Ok(Some(ReadFile { file, bytes }))
}

fn read_bytes(file: &mut File, maximum: u64) -> io::Result<Vec<u8>> {
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
    file.rewind()?;
    let mut bytes = Vec::new();
    Read::by_ref(file)
        .take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "session store file exceeds limit",
        ));
    }
    Ok(bytes)
}

struct StagedFile<'a> {
    parent: RawFd,
    name: &'a CStr,
    descriptor: RawFd,
}
impl Drop for StagedFile<'_> {
    fn drop(&mut self) {
        // Never deliberately remove an unknown replacement staging inode.
        if verify_file_at(self.parent, self.name, self.descriptor).is_ok() {
            // SAFETY: the guard is dropped before its descriptors and name.
            unsafe {
                libc::unlinkat(self.parent, self.name.as_ptr(), 0);
            }
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

    thread_local! {
        pub(super) static BEFORE_RENAME: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
            const { std::cell::RefCell::new(None) };
        pub(super) static AFTER_RENAME: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
            const { std::cell::RefCell::new(None) };
        pub(super) static BEFORE_UNCHANGED: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
            const { std::cell::RefCell::new(None) };
        pub(super) static SYNC_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        static SYNC_LOG: std::cell::RefCell<Vec<(u64, u64)>> = const { std::cell::RefCell::new(Vec::new()) };
        pub(super) static FAIL_NEXT_DIRECTORY_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }

    pub(super) fn before_rename() {
        if let Some(hook) = BEFORE_RENAME.with(|hook| hook.borrow_mut().take()) {
            hook();
        }
    }

    pub(super) fn after_rename() {
        if let Some(hook) = AFTER_RENAME.with(|hook| hook.borrow_mut().take()) {
            hook();
        }
    }

    pub(super) fn before_unchanged() {
        if let Some(hook) = BEFORE_UNCHANGED.with(|hook| hook.borrow_mut().take()) {
            hook();
        }
    }

    pub(super) fn record_sync(descriptor: RawFd) -> bool {
        use std::os::unix::fs::MetadataExt;
        SYNC_COUNT.with(|count| count.set(count.get() + 1));
        // SAFETY: the sync caller holds descriptor live; dup returns a fresh fd.
        let file = File::from(owned_fd(unsafe { libc::dup(descriptor) }).unwrap());
        let metadata = file.metadata().unwrap();
        SYNC_LOG.with(|log| log.borrow_mut().push((metadata.dev(), metadata.ino())));
        metadata.is_dir()
    }

    fn assert_synced(paths: &[PathBuf]) {
        use std::os::unix::fs::MetadataExt;
        for path in paths {
            let metadata = fs::metadata(path).unwrap();
            assert!(
                SYNC_LOG.with(|log| log.borrow().contains(&(metadata.dev(), metadata.ino()))),
                "publication did not sync {path:?}"
            );
        }
    }

    #[test]
    fn review_b_rejects_parent_moved_outside_store_before_rename() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("store");
        let outside = temp.path().join("outside");
        fs::create_dir_all(root.join("projects/project")).unwrap();
        fs::create_dir(&outside).unwrap();
        let store = StoreWriter::open(&root).unwrap();
        let original = root.join("projects/project");
        let moved = outside.join("moved-project");
        let moved_for_hook = moved.clone();
        BEFORE_RENAME.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                fs::rename(&original, &moved_for_hook).unwrap();
                fs::create_dir(&original).unwrap();
            }));
        });
        let result = store.write_file("projects/project/session.jsonl", b"{}\n");
        assert!(
            !moved.join("session.jsonl").exists(),
            "session bytes escaped the store; write returned {result:?}"
        );
        assert!(result.is_err());
    }

    #[test]
    fn review_b_rejects_replaced_staging_file_before_rename() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("store");
        let outside = temp.path().join("outside");
        fs::create_dir(&root).unwrap();
        fs::write(&outside, b"outside").unwrap();
        let store = StoreWriter::open(&root).unwrap();
        let hook_root = root.clone();
        BEFORE_RENAME.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                let staged = fs::read_dir(&hook_root)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .find(|path| {
                        path.file_name()
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .starts_with(".session-transfer-")
                    })
                    .unwrap();
                fs::remove_file(&staged).unwrap();
                symlink(&outside, &staged).unwrap();
            }));
        });
        let result = store.write_file("session.jsonl", b"{}\n");
        assert!(
            !root
                .join("session.jsonl")
                .symlink_metadata()
                .is_ok_and(|metadata| metadata.file_type().is_symlink()),
            "published an attacker-replaced symlink; write returned {result:?}"
        );
        assert!(result.is_err());
    }

    #[test]
    fn review_b_retry_syncs_a_previously_unsynced_publication() {
        let temp = tempdir().unwrap();
        let store = StoreWriter::open(temp.path()).unwrap();
        BEFORE_RENAME.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(|| {
                FAIL_NEXT_DIRECTORY_SYNC.with(|fail| fail.set(true));
            }));
        });
        assert!(store.write_file("session.jsonl", b"{}\n").is_err());
        assert_eq!(
            fs::read(temp.path().join("session.jsonl")).unwrap(),
            b"{}\n"
        );
        SYNC_COUNT.with(|count| count.set(0));
        assert_eq!(
            store.write_file("session.jsonl", b"{}\n").unwrap(),
            WriteOutcome::Unchanged
        );
        assert!(
            SYNC_COUNT.with(|count| count.get()) > 0,
            "successful retry did not sync the failed publication's directory"
        );
    }

    #[test]
    fn replaced_staging_regular_file_is_refused() {
        let temp = tempdir().unwrap();
        let store = StoreWriter::open(temp.path()).unwrap();
        let root = temp.path().to_path_buf();
        BEFORE_RENAME.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                let staged = fs::read_dir(&root)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .find(|path| {
                        path.file_name()
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .starts_with(".session-transfer-")
                    })
                    .unwrap();
                fs::remove_file(&staged).unwrap();
                fs::write(&staged, b"replacement").unwrap();
            }));
        });
        placement_error(store.write_file("session.jsonl", b"{}\n"));
    }

    #[test]
    fn unchanged_syncs_file_and_every_existing_directory() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        fs::create_dir_all(root.join("a/b")).unwrap();
        fs::write(root.join("a/b/session"), b"data").unwrap();
        let store = StoreWriter::open(root).unwrap();
        assert_eq!(
            store.write_file("a/b/session", b"data").unwrap(),
            WriteOutcome::Unchanged
        );
        assert_synced(&[
            root.to_path_buf(),
            root.join("a"),
            root.join("a/b"),
            root.join("a/b/session"),
        ]);
    }

    #[test]
    fn concurrent_unchanged_syncs_file_and_every_directory() {
        let temp = tempdir().unwrap();
        let root = temp.path().to_path_buf();
        fs::create_dir_all(root.join("a/b")).unwrap();
        let target = root.join("a/b/session");
        let store = StoreWriter::open(&root).unwrap();
        BEFORE_RENAME.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || fs::write(target, b"data").unwrap()));
        });
        assert_eq!(
            store.write_file("a/b/session", b"data").unwrap(),
            WriteOutcome::Unchanged
        );
        assert_synced(&[
            root.clone(),
            root.join("a"),
            root.join("a/b"),
            root.join("a/b/session"),
        ]);
    }

    #[test]
    fn concurrent_unchanged_refuses_sync_failure() {
        let temp = tempdir().unwrap();
        let target = temp.path().join("session");
        let store = StoreWriter::open(temp.path()).unwrap();
        BEFORE_RENAME.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                fs::write(target, b"data").unwrap();
                FAIL_NEXT_DIRECTORY_SYNC.with(|fail| fail.set(true));
            }));
        });
        placement_error(store.write_file("session", b"data"));
    }

    #[test]
    fn unchanged_refuses_rebound_parent_in_both_equality_branches() {
        for concurrent in [false, true] {
            let temp = tempdir().unwrap();
            let root = temp.path().join("store");
            let original = root.join("a/b");
            let moved = temp.path().join("moved");
            fs::create_dir_all(&original).unwrap();
            let target = original.join("session");
            let store = StoreWriter::open(&root).unwrap();
            if concurrent {
                BEFORE_RENAME.with(|hook| {
                    *hook.borrow_mut() =
                        Some(Box::new(move || fs::write(target, b"data").unwrap()));
                });
            } else {
                fs::write(target, b"data").unwrap();
            }
            BEFORE_UNCHANGED.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    fs::rename(&original, &moved).unwrap();
                    fs::create_dir(&original).unwrap();
                }));
            });
            placement_error(store.write_file("a/b/session", b"data"));
        }
    }

    #[test]
    fn created_refuses_ancestor_rebound_after_rename() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("store");
        let original = root.join("a");
        let moved = temp.path().join("moved");
        fs::create_dir_all(original.join("b")).unwrap();
        let store = StoreWriter::open(&root).unwrap();
        AFTER_RENAME.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                fs::rename(&original, &moved).unwrap();
                fs::create_dir_all(original.join("b")).unwrap();
            }));
        });
        placement_error(store.write_file("a/b/session", b"data"));
        assert!(!root.join("a/b/session").exists());
    }

    #[test]
    fn created_refuses_replaced_target_without_deleting_it() {
        let temp = tempdir().unwrap();
        let target = temp.path().join("session");
        let hook_target = target.clone();
        let original = temp.path().join("original");
        let store = StoreWriter::open(temp.path()).unwrap();
        AFTER_RENAME.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                fs::rename(&hook_target, original).unwrap();
                fs::write(hook_target, b"replacement").unwrap();
            }));
        });
        let result = store.write_file("session", b"data");
        let Err(WorkerError::Task { code, message }) = result else {
            panic!("replaced target was not refused: {result:?}");
        };
        assert_eq!(code, "SESSION_PLACEMENT_FAILED");
        assert!(message.contains("inode"));
        assert!(!message.contains(temp.path().to_str().unwrap()));
        assert_eq!(fs::read(target).unwrap(), b"replacement");
    }

    #[test]
    fn unchanged_refuses_replaced_target_in_both_equality_branches() {
        for concurrent in [false, true] {
            let temp = tempdir().unwrap();
            let target = temp.path().join("session");
            let hook_target = target.clone();
            let original = temp.path().join("original");
            let store = StoreWriter::open(temp.path()).unwrap();
            if concurrent {
                let target = target.clone();
                BEFORE_RENAME.with(|hook| {
                    *hook.borrow_mut() =
                        Some(Box::new(move || fs::write(target, b"data").unwrap()));
                });
            } else {
                fs::write(&target, b"data").unwrap();
            }
            BEFORE_UNCHANGED.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    fs::rename(&hook_target, original).unwrap();
                    fs::write(hook_target, b"replacement").unwrap();
                }));
            });
            placement_error(store.write_file("session", b"data"));
            assert_eq!(fs::read(target).unwrap(), b"replacement");
        }
    }

    #[test]
    fn created_refuses_in_place_staging_mutation() {
        let temp = tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let store = StoreWriter::open(&root).unwrap();
        BEFORE_RENAME.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                let staged = fs::read_dir(&root).unwrap().next().unwrap().unwrap().path();
                fs::write(staged, b"other").unwrap();
            }));
        });
        placement_error(store.write_file("session", b"data"));
    }

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
