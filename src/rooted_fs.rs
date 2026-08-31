use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::{CStr, CString},
    fmt,
    fs::File,
    io::{self, Read, Write},
    mem::MaybeUninit,
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
        unix::{ffi::OsStrExt, fs::FileExt},
    },
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{inputs::RelativePath, job::MAX_LOG_CHUNK_BYTES};

const DIRECTORY_OPEN_FLAGS: libc::c_int =
    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW;
const REGULAR_OPEN_FLAGS: libc::c_int =
    libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;
const MAX_SYMLINK_TARGET: usize = 64 * 1024;
const PRIVATE_NAMESPACE_NAME: &CStr = c".mac-worker-rooted-fs";
const CLEANUP_RECORD_MAX_BYTES: usize = 4096;
const CLEANUP_INTENT_PREFIX: &str = "cleanup-intent-v1-";
const CLEANUP_DECISION_PREFIX: &str = "cleanup-decision-v1-";
const CLEANUP_INTENT_STAGE_PREFIX: &str = "cleanup-intent-stage-v1-";
const CLEANUP_DECISION_STAGE_PREFIX: &str = "cleanup-decision-stage-v1-";
const CLEANUP_OPERATION_PREFIX: &str = "cleanup-op-v1-";
const CLEANUP_PLACEHOLDER_NAME: &CStr = c"cleanup-placeholder-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    RegularFile,
    Symlink,
}

#[cfg(test)]
mod task7_status_file_tests {
    use super::*;
    use std::{
        fs,
        os::unix::fs::{PermissionsExt, symlink},
    };
    use tempfile::tempdir;

    #[test]
    fn conditional_private_replacement_requires_exact_old_binding_and_bytes() {
        let temp = tempdir().unwrap();
        let root_path = temp.path().join("root");
        let root = RootedDir::create(&root_path).unwrap();
        root.write_private_atomic_no_replace("status.json", b"old")
            .unwrap();

        root.replace_private_regular_exact("status.json", b"old", b"new")
            .unwrap();
        assert_eq!(fs::read(root_path.join("status.json")).unwrap(), b"new");
        assert!(
            root.replace_private_regular_exact("status.json", b"old", b"bad")
                .is_err()
        );
        assert_eq!(fs::read(root_path.join("status.json")).unwrap(), b"new");
    }

    #[test]
    fn append_handle_is_owner_only_single_link_and_name_bound() {
        let temp = tempdir().unwrap();
        let root_path = temp.path().join("root");
        let root = RootedDir::create(&root_path).unwrap();
        root.write_private_atomic_no_replace("stdout.log", b"first")
            .unwrap();
        let mut append = root.open_private_append("stdout.log").unwrap();
        append.write_all(b"second").unwrap();
        append.sync_all().unwrap();
        root.validate_private_append_binding("stdout.log", &append)
            .unwrap();
        assert_eq!(
            fs::read(root_path.join("stdout.log")).unwrap(),
            b"firstsecond"
        );

        symlink(root_path.join("stdout.log"), root_path.join("link.log")).unwrap();
        assert!(root.open_private_append("link.log").is_err());
        fs::hard_link(root_path.join("stdout.log"), root_path.join("hard.log")).unwrap();
        assert!(root.open_private_append("hard.log").is_err());
    }

    #[test]
    fn conditional_status_replacement_rejects_symlink_hardlink_and_root_substitution() {
        let symlink_temp = tempdir().unwrap();
        let symlink_root_path = symlink_temp.path().join("root");
        let symlink_root = RootedDir::create(&symlink_root_path).unwrap();
        symlink_root
            .write_private_atomic_no_replace("status.json", b"old")
            .unwrap();
        fs::rename(
            symlink_root_path.join("status.json"),
            symlink_root_path.join("retained.json"),
        )
        .unwrap();
        symlink(
            symlink_root_path.join("retained.json"),
            symlink_root_path.join("status.json"),
        )
        .unwrap();
        assert!(
            symlink_root
                .replace_private_regular_exact("status.json", b"old", b"new")
                .is_err()
        );
        assert_eq!(
            fs::read(symlink_root_path.join("retained.json")).unwrap(),
            b"old"
        );

        let hardlink_temp = tempdir().unwrap();
        let hardlink_root_path = hardlink_temp.path().join("root");
        let hardlink_root = RootedDir::create(&hardlink_root_path).unwrap();
        hardlink_root
            .write_private_atomic_no_replace("status.json", b"old")
            .unwrap();
        fs::hard_link(
            hardlink_root_path.join("status.json"),
            hardlink_root_path.join("alias.json"),
        )
        .unwrap();
        assert!(
            hardlink_root
                .replace_private_regular_exact("status.json", b"old", b"new")
                .is_err()
        );
        assert_eq!(
            fs::read(hardlink_root_path.join("status.json")).unwrap(),
            b"old"
        );

        let replaced_temp = tempdir().unwrap();
        let replaced_root_path = replaced_temp.path().join("root");
        let replaced_root = RootedDir::create(&replaced_root_path).unwrap();
        replaced_root
            .write_private_atomic_no_replace("status.json", b"old")
            .unwrap();
        let detached = replaced_temp.path().join("detached");
        fs::rename(&replaced_root_path, &detached).unwrap();
        fs::create_dir(&replaced_root_path).unwrap();
        fs::set_permissions(&replaced_root_path, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(replaced_root_path.join("status.json"), b"replacement").unwrap();
        fs::set_permissions(
            replaced_root_path.join("status.json"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        assert!(
            replaced_root
                .replace_private_regular_exact("status.json", b"old", b"new")
                .is_err()
        );
        assert_eq!(
            fs::read(replaced_root_path.join("status.json")).unwrap(),
            b"replacement"
        );
        assert_eq!(fs::read(detached.join("status.json")).unwrap(), b"old");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotProjection {
    TransportOrOwner,
    OwnerOnly,
    Workspace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotFsKind {
    RegularFile,
    Directory,
    Symlink,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotFileRead {
    pub(crate) bytes: Vec<u8>,
    pub(crate) mode: u32,
    pub(crate) device: u64,
    pub(crate) inode: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotTreeEntry {
    pub(crate) path: RelativePath,
    pub(crate) kind: SnapshotFsKind,
    pub(crate) mode: u32,
    pub(crate) size: u64,
    pub(crate) device: u64,
    pub(crate) inode: u64,
    pub(crate) sha256: Option<String>,
    pub(crate) symlink_target: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotTreeInspection {
    pub(crate) root_mode: u32,
    pub(crate) root_device: u64,
    pub(crate) root_inode: u64,
    pub(crate) entries: Vec<SnapshotTreeEntry>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AtomicRenameCapability {
    NoReplace,
    Unsupported,
}

#[cfg(test)]
impl AtomicRenameCapability {
    fn current() -> Self {
        #[cfg(test)]
        if let Some(capability) = TEST_ATOMIC_RENAME_CAPABILITY.with(std::cell::Cell::get) {
            return capability;
        }
        if cfg!(any(
            target_vendor = "apple",
            target_os = "linux",
            target_os = "android"
        )) {
            Self::NoReplace
        } else {
            Self::Unsupported
        }
    }
}

#[cfg(test)]
thread_local! {
    static TEST_RENAME_NO_REPLACE_ERROR: std::cell::Cell<Option<RenameFault>> = const {
        std::cell::Cell::new(None)
    };
    static TEST_COPY_PRIVATE_CLEANUP_FAILURE: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static TEST_CLEANUP_FAULT: std::cell::Cell<Option<CleanupFault>> = const {
        std::cell::Cell::new(None)
    };
}

#[cfg(test)]
thread_local! {
    static TEST_ATOMIC_RENAME_CAPABILITY: std::cell::Cell<Option<AtomicRenameCapability>> = const {
        std::cell::Cell::new(None)
    };
}

#[cfg(test)]
struct AtomicRenameCapabilityOverride(Option<AtomicRenameCapability>);

#[cfg(test)]
impl AtomicRenameCapabilityOverride {
    fn set(capability: AtomicRenameCapability) -> Self {
        let previous = TEST_ATOMIC_RENAME_CAPABILITY.replace(Some(capability));
        Self(previous)
    }
}

#[cfg(test)]
impl Drop for AtomicRenameCapabilityOverride {
    fn drop(&mut self) {
        TEST_ATOMIC_RENAME_CAPABILITY.set(self.0);
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RenameFault {
    Always(libc::c_int),
    CrossDirectory(libc::c_int),
    Directory(libc::c_int),
}

#[cfg(test)]
struct RenameNoReplaceOverride(Option<RenameFault>);

#[cfg(test)]
impl RenameNoReplaceOverride {
    fn fail_with(errno: libc::c_int) -> Self {
        Self(TEST_RENAME_NO_REPLACE_ERROR.replace(Some(RenameFault::Always(errno))))
    }

    fn fail_cross_directory_with(errno: libc::c_int) -> Self {
        Self(TEST_RENAME_NO_REPLACE_ERROR.replace(Some(RenameFault::CrossDirectory(errno))))
    }

    fn fail_directory_with(errno: libc::c_int) -> Self {
        Self(TEST_RENAME_NO_REPLACE_ERROR.replace(Some(RenameFault::Directory(errno))))
    }
}

#[cfg(test)]
impl Drop for RenameNoReplaceOverride {
    fn drop(&mut self) {
        TEST_RENAME_NO_REPLACE_ERROR.set(self.0);
    }
}

#[cfg(test)]
struct CopyPrivateCleanupFailureOverride(bool);

#[cfg(test)]
impl CopyPrivateCleanupFailureOverride {
    fn set() -> Self {
        Self(TEST_COPY_PRIVATE_CLEANUP_FAILURE.replace(true))
    }
}

#[cfg(test)]
impl Drop for CopyPrivateCleanupFailureOverride {
    fn drop(&mut self) {
        TEST_COPY_PRIVATE_CLEANUP_FAILURE.set(self.0);
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanupFault {
    AfterAcquisitionValidation(libc::c_int),
    AfterCleanupBootstrap(libc::c_int),
    AfterCleanupPlaceholder(libc::c_int),
    DuringCleanupIntentWrite(libc::c_int),
    AfterCleanupIntentSync(libc::c_int),
    AfterTargetChmod(libc::c_int),
    AfterFirstRemoval(libc::c_int),
    BeforeFinalRootRemoval(libc::c_int),
}

#[cfg(test)]
struct CleanupFaultOverride(Option<CleanupFault>);

#[cfg(test)]
impl CleanupFaultOverride {
    fn set(fault: CleanupFault) -> Self {
        Self(TEST_CLEANUP_FAULT.replace(Some(fault)))
    }
}

#[cfg(test)]
impl Drop for CleanupFaultOverride {
    fn drop(&mut self) {
        TEST_CLEANUP_FAULT.set(self.0);
    }
}

#[cfg(test)]
pub(crate) fn fail_next_cleanup_before_final_root_removal(errno: libc::c_int) -> impl Drop {
    CleanupFaultOverride::set(CleanupFault::BeforeFinalRootRemoval(errno))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryMetadata {
    pub kind: EntryKind,
    pub mode: u32,
    pub size: u64,
    pub device: u64,
    pub inode: u64,
    pub modified_seconds: i64,
    pub modified_nanoseconds: i64,
}

#[derive(Debug)]
pub struct EntryInspection {
    pub kind: EntryKind,
    pub mode: u32,
    pub size: u64,
    pub device: u64,
    pub inode: u64,
    pub modified_seconds: i64,
    pub modified_nanoseconds: i64,
    file: Option<File>,
}

impl EntryInspection {
    pub fn metadata(&self) -> EntryMetadata {
        EntryMetadata {
            kind: self.kind,
            mode: self.mode,
            size: self.size,
            device: self.device,
            inode: self.inode,
            modified_seconds: self.modified_seconds,
            modified_nanoseconds: self.modified_nanoseconds,
        }
    }

    pub fn restat(&self) -> io::Result<EntryMetadata> {
        let file = self.file.as_ref().ok_or_else(invalid_type_error)?;
        metadata_from_stat(stat_fd(file.as_raw_fd())?)
    }
}

impl Read for EntryInspection {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.file
            .as_mut()
            .ok_or_else(invalid_type_error)?
            .read(buffer)
    }
}

pub struct RootedDir {
    root: OwnedFd,
    parent: OwnedFd,
    root_name: CString,
    root_identity: FileIdentity,
    // These immutable capability bindings are safe to share; each RootedDir
    // still owns fresh descriptors for its current root and direct parent.
    lineage: Vec<Arc<DirectoryBinding>>,
    security_device: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PrivateEntryIdentity {
    pub(crate) device: u64,
    pub(crate) inode: u64,
    pub(crate) kind: u32,
    pub(crate) owner: u32,
    pub(crate) mode: u32,
}

struct DirectoryBinding {
    directory: OwnedFd,
    parent: OwnedFd,
    name: CString,
    identity: FileIdentity,
    owner: u32,
    mode: u32,
    security_device: Option<u64>,
}

#[derive(Debug)]
struct LogOffsetBeyondEof;

impl fmt::Display for LogOffsetBeyondEof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("log offset is beyond EOF")
    }
}

impl std::error::Error for LogOffsetBeyondEof {}

pub(crate) fn is_log_offset_beyond_eof(error: &io::Error) -> bool {
    error
        .get_ref()
        .and_then(|source| source.downcast_ref::<LogOffsetBeyondEof>())
        .is_some()
}

impl PrivateEntryIdentity {
    fn from_stat(metadata: &libc::stat) -> Self {
        Self {
            device: metadata.st_dev as u64,
            inode: metadata.st_ino,
            kind: file_type(metadata.st_mode) as u32,
            owner: metadata.st_uid,
            mode: (metadata.st_mode & 0o777) as u32,
        }
    }
}

impl RootedDir {
    pub fn open(path: &Path) -> io::Result<Self> {
        let (parent_path, root_name) = split_root_path(path)?;
        let parent = open_directory_path(parent_path)?;
        let root = open_directory_at(parent.as_raw_fd(), &root_name)?;
        let root_identity = FileIdentity::from_stat(&stat_fd(root.as_raw_fd())?);
        Ok(Self {
            root,
            parent,
            root_name,
            root_identity,
            lineage: Vec::new(),
            security_device: None,
        })
    }

    pub fn create(path: &Path) -> io::Result<Self> {
        let (parent_path, root_name) = split_root_path(path)?;
        let parent = open_or_create_directory_path(parent_path)?;
        mkdir_at(parent.as_raw_fd(), &root_name, 0o700)?;
        let root = match open_directory_at(parent.as_raw_fd(), &root_name) {
            Ok(root) => root,
            Err(error) => {
                let _ = unlink_at(parent.as_raw_fd(), &root_name, libc::AT_REMOVEDIR);
                return Err(error);
            }
        };
        let root_identity = FileIdentity::from_stat(&stat_fd(root.as_raw_fd())?);
        cvt(unsafe { libc::fsync(parent.as_raw_fd()) })?;
        Ok(Self {
            root,
            parent,
            root_name,
            root_identity,
            lineage: Vec::new(),
            security_device: None,
        })
    }

    pub(crate) fn open_anchored_absolute(path: &Path) -> io::Result<Self> {
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "anchored directory path must be absolute",
            ));
        }
        #[cfg(target_vendor = "apple")]
        let normalized;
        #[cfg(target_vendor = "apple")]
        let path = if let Ok(suffix) = path.strip_prefix("/var") {
            normalized = PathBuf::from("/private/var").join(suffix);
            normalized.as_path()
        } else if let Ok(suffix) = path.strip_prefix("/tmp") {
            normalized = PathBuf::from("/private/tmp").join(suffix);
            normalized.as_path()
        } else {
            path
        };
        let components = path
            .components()
            .filter_map(|component| match component {
                Component::RootDir | Component::CurDir => None,
                Component::Normal(component) => {
                    Some(CString::new(component.as_bytes()).map_err(interior_nul_error))
                }
                Component::ParentDir | Component::Prefix(_) => Some(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "physical root path contains unsupported traversal",
                ))),
            })
            .collect::<io::Result<Vec<_>>>()?;
        let root_name = components.last().cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "anchored path must identify a directory entry",
            )
        })?;
        let mut current = owned_fd(unsafe { libc::open(c"/".as_ptr(), DIRECTORY_OPEN_FLAGS) })?;
        let mut lineage = Vec::with_capacity(components.len().saturating_sub(1));
        for (index, component) in components.iter().enumerate() {
            let child = open_directory_at(current.as_raw_fd(), component)?;
            let opened = stat_fd(child.as_raw_fd())?;
            if index + 1 == components.len() {
                return Ok(Self {
                    root: child,
                    parent: current,
                    root_name,
                    root_identity: FileIdentity::from_stat(&opened),
                    lineage,
                    security_device: None,
                });
            }
            lineage.push(Arc::new(DirectoryBinding {
                directory: reopen_directory(child.as_raw_fd())?,
                parent: reopen_directory(current.as_raw_fd())?,
                name: component.clone(),
                identity: FileIdentity::from_stat(&opened),
                owner: opened.st_uid,
                mode: (opened.st_mode & 0o777) as u32,
                security_device: None,
            }));
            current = child;
        }
        unreachable!("a non-empty component sequence returns its final directory")
    }

    pub(crate) fn open_or_create_anchored_absolute(path: &Path) -> io::Result<Self> {
        match Self::open_anchored_absolute(path) {
            Ok(directory) => Ok(directory),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                drop(open_or_create_directory_path(path)?);
                Self::open_anchored_absolute(path)
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn bind_host_device(&mut self, expected_device: u64) -> io::Result<()> {
        self.verify_root_name()?;
        let metadata = stat_fd(self.root.as_raw_fd())?;
        require_private_directory_on_device(&metadata, expected_device)?;
        self.security_device = Some(expected_device);
        Ok(())
    }

    pub(crate) fn reopen(&self) -> io::Result<Self> {
        self.verify_root_name()?;
        Ok(Self {
            root: reopen_directory(self.root.as_raw_fd())?,
            parent: reopen_directory(self.parent.as_raw_fd())?,
            root_name: self.root_name.clone(),
            root_identity: self.root_identity,
            lineage: clone_lineage(&self.lineage),
            security_device: self.security_device,
        })
    }

    pub(crate) fn verify_bound(&self) -> io::Result<()> {
        self.verify_root_name()
    }

    pub(crate) fn verify_descriptors_cloexec(&self) -> io::Result<()> {
        self.verify_root_name()?;
        require_fd_cloexec(self.root.as_raw_fd())?;
        require_fd_cloexec(self.parent.as_raw_fd())?;
        for binding in &self.lineage {
            require_fd_cloexec(binding.directory.as_raw_fd())?;
            require_fd_cloexec(binding.parent.as_raw_fd())?;
        }
        Ok(())
    }

    pub(crate) fn raw_directory_fd(&self) -> RawFd {
        self.root.as_raw_fd()
    }

    pub(crate) fn root_metadata(&self) -> io::Result<libc::stat> {
        self.verify_root_name()?;
        stat_fd(self.root.as_raw_fd())
    }

    pub(crate) fn identity(&self) -> io::Result<PrivateEntryIdentity> {
        self.verify_root_name()?;
        Ok(PrivateEntryIdentity::from_stat(&stat_fd(
            self.root.as_raw_fd(),
        )?))
    }

    pub(crate) fn private_entry_identity(&self, name: &str) -> io::Result<PrivateEntryIdentity> {
        self.verify_root_name()?;
        let name = CString::new(name).map_err(interior_nul_error)?;
        let metadata = stat_at(self.root.as_raw_fd(), &name)?;
        match file_type(metadata.st_mode) {
            libc::S_IFDIR => {
                if let Some(device) = self.security_device {
                    require_private_directory_on_device(&metadata, device)?;
                } else {
                    require_private_directory(&metadata)?;
                }
            }
            libc::S_IFREG => {
                require_private_regular(&metadata)?;
                if self
                    .security_device
                    .is_some_and(|device| metadata.st_dev as u64 != device)
                {
                    return Err(os_error(libc::EXDEV));
                }
            }
            _ => return Err(invalid_type_error()),
        }
        Ok(PrivateEntryIdentity::from_stat(&metadata))
    }

    pub(crate) fn validate_private_regular_binding(
        &self,
        name: &str,
        file: &File,
        expected: PrivateEntryIdentity,
    ) -> io::Result<()> {
        self.verify_root_name()?;
        let name = CString::new(name).map_err(interior_nul_error)?;
        let current = stat_at(self.root.as_raw_fd(), &name)?;
        let opened = stat_fd(file.as_raw_fd())?;
        require_private_regular(&current)?;
        require_private_regular(&opened)?;
        if PrivateEntryIdentity::from_stat(&current) != expected
            || PrivateEntryIdentity::from_stat(&opened) != expected
            || !same_file(&current, &opened)
        {
            return Err(os_error(libc::ESTALE));
        }
        if self
            .security_device
            .is_some_and(|device| current.st_dev as u64 != device)
        {
            return Err(os_error(libc::EXDEV));
        }
        Ok(())
    }

    pub(crate) fn open_child_directory(
        &self,
        path: &RelativePath,
        create: bool,
    ) -> io::Result<Self> {
        self.open_child_directory_inner(path, create, self.security_device)
    }

    pub(crate) fn open_child_directory_on_device(
        &self,
        path: &RelativePath,
        create: bool,
        expected_device: u64,
    ) -> io::Result<Self> {
        self.open_child_directory_inner(path, create, Some(expected_device))
    }

    fn open_child_directory_inner(
        &self,
        path: &RelativePath,
        create: bool,
        security_device: Option<u64>,
    ) -> io::Result<Self> {
        self.verify_root_name()?;
        let root_metadata = stat_fd(self.root.as_raw_fd())?;
        if let Some(device) = security_device {
            require_private_directory_on_device(&root_metadata, device)?;
        } else {
            require_private_directory(&root_metadata)?;
        }
        let (parent, name, lineage) = self.open_private_parent(path, create, security_device)?;
        if create {
            verify_lineage(&lineage)?;
            match mkdir_at(parent.as_raw_fd(), &name, 0o700) {
                Ok(()) => cvt(unsafe { libc::fsync(parent.as_raw_fd()) })?,
                Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {}
                Err(error) => return Err(error),
            }
        }
        let path_stat = stat_at(parent.as_raw_fd(), &name)?;
        let root = open_directory_at(parent.as_raw_fd(), &name)?;
        let opened = stat_fd(root.as_raw_fd())?;
        if let Some(device) = security_device {
            require_private_directory_on_device(&path_stat, device)?;
            require_private_directory_on_device(&opened, device)?;
        } else {
            require_private_directory(&path_stat)?;
            require_private_directory(&opened)?;
        }
        if !same_file(&path_stat, &opened) {
            return Err(os_error(libc::ESTALE));
        }
        Ok(Self {
            root,
            parent,
            root_name: name,
            root_identity: FileIdentity::from_stat(&opened),
            lineage,
            security_device,
        })
    }

    pub(crate) fn create_new_child_directory(&self, name: &str) -> io::Result<Self> {
        self.verify_root_name()?;
        let name = CString::new(name).map_err(interior_nul_error)?;
        mkdir_at(self.root.as_raw_fd(), &name, 0o700)?;
        let root = open_directory_at(self.root.as_raw_fd(), &name)?;
        let opened = stat_fd(root.as_raw_fd())?;
        if let Some(device) = self.security_device {
            require_private_directory_on_device(&opened, device)?;
        } else {
            require_private_directory(&opened)?;
        }
        let lineage = self.child_lineage()?;
        cvt(unsafe { libc::fsync(self.root.as_raw_fd()) })?;
        Ok(Self {
            root,
            parent: reopen_directory(self.root.as_raw_fd())?,
            root_name: name,
            root_identity: FileIdentity::from_stat(&opened),
            lineage,
            security_device: self.security_device,
        })
    }

    pub(crate) fn open_private_direct_child_on_device(
        &self,
        name: &str,
        expected_device: u64,
    ) -> io::Result<Self> {
        self.verify_root_name()?;
        let name = CString::new(name).map_err(interior_nul_error)?;
        let path_stat = stat_at(self.root.as_raw_fd(), &name)?;
        require_private_directory_on_device(&path_stat, expected_device)?;
        let root = open_directory_at(self.root.as_raw_fd(), &name)?;
        let opened = stat_fd(root.as_raw_fd())?;
        require_private_directory_on_device(&opened, expected_device)?;
        if !same_file(&path_stat, &opened) {
            return Err(os_error(libc::ESTALE));
        }
        Ok(Self {
            root,
            parent: reopen_directory(self.root.as_raw_fd())?,
            root_name: name,
            root_identity: FileIdentity::from_stat(&opened),
            lineage: self.child_lineage()?,
            security_device: Some(expected_device),
        })
    }

    pub(crate) fn list_names(&self) -> io::Result<Vec<Vec<u8>>> {
        self.verify_root_name()?;
        directory_entries(self.root.as_raw_fd()).map(|entries| {
            entries
                .into_iter()
                .map(|entry| entry.into_bytes())
                .collect()
        })
    }

    pub(crate) fn has_private_cleanup_residue(&self) -> io::Result<bool> {
        self.verify_root_name()?;
        let root = stat_fd(self.root.as_raw_fd())?;
        let parent = stat_fd(self.parent.as_raw_fd())?;
        if parent.st_dev != root.st_dev {
            return Err(os_error(libc::EXDEV));
        }
        let nested_namespace_has_entries =
            private_namespace_has_entries_at(self.root.as_raw_fd(), root.st_dev as u64)?;
        let adjacent_namespace_has_entries =
            private_namespace_has_entries_at(self.parent.as_raw_fd(), root.st_dev as u64)?;
        let direct_remove_residue = directory_entries(self.root.as_raw_fd())?
            .iter()
            .any(|name| is_random_private_name(name, "remove"));
        self.verify_root_name()?;
        Ok(nested_namespace_has_entries || adjacent_namespace_has_entries || direct_remove_residue)
    }

    pub(crate) fn validate_private_entry(&self, name: &str) -> io::Result<()> {
        self.verify_root_name()?;
        let name = CString::new(name).map_err(interior_nul_error)?;
        let metadata = stat_at(self.root.as_raw_fd(), &name)?;
        if metadata.st_uid != unsafe { libc::geteuid() } || metadata.st_mode & 0o077 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "host entry is not owner-only",
            ));
        }
        match file_type(metadata.st_mode) {
            libc::S_IFDIR | libc::S_IFREG => Ok(()),
            _ => Err(invalid_type_error()),
        }
    }

    pub(crate) fn entry_exists(&self, name: &str) -> io::Result<bool> {
        self.verify_root_name()?;
        let name = CString::new(name).map_err(interior_nul_error)?;
        match stat_at(self.root.as_raw_fd(), &name) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn open_private_lock(&self, name: &str) -> io::Result<File> {
        self.open_private_lock_with_created(name)
            .map(|(file, _)| file)
    }

    pub(crate) fn open_private_lock_with_created(&self, name: &str) -> io::Result<(File, bool)> {
        self.verify_root_name()?;
        let name = CString::new(name).map_err(interior_nul_error)?;
        let initial = match stat_at(self.root.as_raw_fd(), &name) {
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let (descriptor, created) = if initial.is_some() {
            (open_regular_rw_at(self.root.as_raw_fd(), &name)?, false)
        } else {
            let flags = libc::O_RDWR
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK;
            let raw = unsafe { libc::openat(self.root.as_raw_fd(), name.as_ptr(), flags, 0o600) };
            match owned_fd(raw) {
                Ok(descriptor) => (descriptor, true),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    let file = self.open_existing_private_lock(name.to_str().map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "lock name is not UTF-8")
                    })?)?;
                    return Ok((file, false));
                }
                Err(error) => return Err(error),
            }
        };
        let metadata = stat_fd(descriptor.as_raw_fd())?;
        require_private_regular(&metadata)?;
        if initial.is_some_and(|initial| !same_file(&initial, &metadata)) {
            return Err(os_error(libc::ESTALE));
        }
        Ok((File::from(descriptor), created))
    }

    pub(crate) fn open_existing_private_lock(&self, name: &str) -> io::Result<File> {
        self.verify_root_name()?;
        let name = CString::new(name).map_err(interior_nul_error)?;
        let initial = stat_at(self.root.as_raw_fd(), &name)?;
        require_private_regular(&initial)?;
        let descriptor = open_regular_rw_at(self.root.as_raw_fd(), &name)?;
        let opened = stat_fd(descriptor.as_raw_fd())?;
        require_private_regular(&opened)?;
        if !same_file(&initial, &opened) {
            return Err(os_error(libc::ESTALE));
        }
        Ok(File::from(descriptor))
    }

    pub(crate) fn read_private_regular(&self, name: &str, maximum: u64) -> io::Result<Vec<u8>> {
        self.read_private_regular_with_hook(name, maximum, || {})
    }

    pub(crate) fn read_private_regular_chunk(
        &self,
        name: &str,
        offset: u64,
        limit: usize,
    ) -> io::Result<Vec<u8>> {
        self.read_private_regular_chunk_with_hooks(name, offset, limit, || {}, || {}, || {})
    }

    fn read_private_regular_chunk_with_hooks(
        &self,
        name: &str,
        offset: u64,
        limit: usize,
        after_path_stat: impl FnOnce(),
        after_initial_validation: impl FnOnce(),
        after_read: impl FnOnce(),
    ) -> io::Result<Vec<u8>> {
        self.verify_root_name()?;
        let expected_device = self.security_device.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "log directory is not bound to a host device",
            )
        })?;
        let name = private_leaf_name(name)?;
        let path_stat = stat_at(self.root.as_raw_fd(), &name)?;
        require_private_regular_on_device(&path_stat, expected_device)?;
        after_path_stat();

        let descriptor = open_regular_at(self.root.as_raw_fd(), &name)?;
        let opened = stat_fd(descriptor.as_raw_fd())?;
        if let Err(error) = require_private_regular_opened_on_device(&opened, expected_device) {
            if error.raw_os_error() == Some(libc::ESTALE) {
                let rebound = stat_at(self.root.as_raw_fd(), &name)?;
                require_private_regular_on_device(&rebound, expected_device)?;
            }
            return Err(error);
        }
        if !same_file(&path_stat, &opened) || !private_regular_policy_stable(&path_stat, &opened) {
            let rebound = stat_at(self.root.as_raw_fd(), &name)?;
            require_private_regular_on_device(&rebound, expected_device)?;
            return Err(os_error(libc::ESTALE));
        }
        if opened.st_size < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "log length is invalid",
            ));
        }
        let initial_length = opened.st_size as u64;
        if offset > initial_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                LogOffsetBeyondEof,
            ));
        }
        after_initial_validation();

        let requested = limit.min(MAX_LOG_CHUNK_BYTES);
        let mut bytes = vec![0; requested];
        let file = File::from(descriptor);
        let read = file.read_at(&mut bytes, offset)?;
        bytes.truncate(read);
        let next_offset = offset
            .checked_add(u64::try_from(read).expect("usize fits in u64"))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "log offset overflow"))?;
        after_read();

        let after = stat_fd(file.as_raw_fd())?;
        let rebound = stat_at(self.root.as_raw_fd(), &name)?;
        require_private_regular_on_device(&rebound, expected_device)?;
        require_private_regular_opened_on_device(&after, expected_device)?;
        if !private_regular_policy_stable(&opened, &after)
            || !private_regular_policy_stable(&opened, &rebound)
            || !same_file(&opened, &after)
            || !same_file(&opened, &rebound)
        {
            return Err(os_error(libc::ESTALE));
        }
        if after.st_size < 0 || rebound.st_size < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "log length is invalid",
            ));
        }
        let after_length = after.st_size as u64;
        let rebound_length = rebound.st_size as u64;
        if after_length < initial_length
            || rebound_length < initial_length
            || after_length < next_offset
            || rebound_length < next_offset
        {
            return Err(os_error(libc::ESTALE));
        }
        self.verify_root_name()?;
        Ok(bytes)
    }

    fn read_private_regular_with_hook(
        &self,
        name: &str,
        maximum: u64,
        after_open: impl FnOnce(),
    ) -> io::Result<Vec<u8>> {
        self.read_private_regular_with_hooks(name, maximum, || {}, after_open)
    }

    fn read_private_regular_with_hooks(
        &self,
        name: &str,
        maximum: u64,
        after_descriptor_open: impl FnOnce(),
        after_opened_validation: impl FnOnce(),
    ) -> io::Result<Vec<u8>> {
        self.verify_root_name()?;
        let name = CString::new(name).map_err(interior_nul_error)?;
        let path_stat = stat_at(self.root.as_raw_fd(), &name)?;
        require_private_regular(&path_stat)?;
        let descriptor = open_regular_at(self.root.as_raw_fd(), &name)?;
        after_descriptor_open();
        let opened = stat_fd(descriptor.as_raw_fd())?;
        if let Err(error) = require_private_regular_opened(&opened) {
            if error.raw_os_error() == Some(libc::ESTALE) {
                let rebound = stat_at(self.root.as_raw_fd(), &name)?;
                require_private_regular(&rebound)?;
            }
            return Err(error);
        }
        if !same_file(&path_stat, &opened) {
            let rebound = stat_at(self.root.as_raw_fd(), &name)?;
            require_private_regular(&rebound)?;
            return Err(os_error(libc::ESTALE));
        }
        if opened.st_size < 0 || opened.st_size as u64 > maximum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "host file exceeds limit",
            ));
        }
        after_opened_validation();
        let mut bytes = Vec::new();
        let mut file = File::from(descriptor);
        Read::by_ref(&mut file)
            .take(maximum.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > maximum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "host file exceeds limit",
            ));
        }
        let after = stat_fd(file.as_raw_fd())?;
        let rebound = stat_at(self.root.as_raw_fd(), &name)?;
        require_private_regular(&rebound)?;
        require_private_regular_opened(&after)?;
        if bytes.len() as u64 != path_stat.st_size as u64
            || !snapshot_metadata_stable(&path_stat, &after)
            || !snapshot_metadata_stable(&path_stat, &rebound)
        {
            return Err(os_error(libc::ESTALE));
        }
        self.verify_root_name()?;
        Ok(bytes)
    }

    pub(crate) fn validate_snapshot_root(&self, expected_mode: u32) -> io::Result<()> {
        self.verify_root_name()?;
        let metadata = stat_fd(self.root.as_raw_fd())?;
        require_snapshot_entry(&metadata, metadata.st_dev as u64)?;
        if file_type(metadata.st_mode) != libc::S_IFDIR
            || (metadata.st_mode & 0o777) as u32 != expected_mode
        {
            return Err(snapshot_policy_error());
        }
        Ok(())
    }

    pub(crate) fn read_snapshot_regular(
        &self,
        name: &str,
        maximum: u64,
        projection: SnapshotProjection,
    ) -> io::Result<SnapshotFileRead> {
        self.verify_root_name()?;
        let root = stat_fd(self.root.as_raw_fd())?;
        let expected_device = root.st_dev as u64;
        let name = CString::new(name).map_err(interior_nul_error)?;
        let before = stat_at(self.root.as_raw_fd(), &name)?;
        require_snapshot_regular(&before, expected_device, projection)?;
        let descriptor = open_regular_at(self.root.as_raw_fd(), &name)?;
        let opened = stat_fd(descriptor.as_raw_fd())?;
        require_snapshot_regular(&opened, expected_device, projection)?;
        if !snapshot_metadata_stable(&before, &opened) {
            return Err(os_error(libc::ESTALE));
        }
        if opened.st_size < 0 || opened.st_size as u64 > maximum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot regular file exceeds its byte limit",
            ));
        }
        let mut bytes = Vec::new();
        let mut file = File::from(descriptor);
        Read::by_ref(&mut file)
            .take(maximum.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > maximum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot regular file exceeds its byte limit",
            ));
        }
        let after = stat_fd(file.as_raw_fd())?;
        let rebound = stat_at(self.root.as_raw_fd(), &name)?;
        if bytes.len() as u64 != before.st_size as u64
            || !snapshot_metadata_stable(&before, &after)
            || !snapshot_metadata_stable(&before, &rebound)
        {
            return Err(os_error(libc::ESTALE));
        }
        self.verify_root_name()?;
        Ok(SnapshotFileRead {
            bytes,
            mode: (before.st_mode & 0o7777) as u32,
            device: before.st_dev as u64,
            inode: before.st_ino,
        })
    }

    pub(crate) fn inspect_snapshot_tree(
        &self,
        name: &str,
        projection: SnapshotProjection,
    ) -> io::Result<SnapshotTreeInspection> {
        self.verify_root_name()?;
        let bundle = stat_fd(self.root.as_raw_fd())?;
        let expected_device = bundle.st_dev as u64;
        let name = CString::new(name).map_err(interior_nul_error)?;
        let before = stat_at(self.root.as_raw_fd(), &name)?;
        require_snapshot_directory(&before, expected_device, projection)?;
        let directory = open_directory_at(self.root.as_raw_fd(), &name)?;
        let opened = stat_fd(directory.as_raw_fd())?;
        require_snapshot_directory(&opened, expected_device, projection)?;
        if !snapshot_metadata_stable(&before, &opened) {
            return Err(os_error(libc::ESTALE));
        }
        let mut identities = BTreeSet::from([(before.st_dev as u64, before.st_ino)]);
        let mut entries = Vec::new();
        inspect_snapshot_directory(
            directory.as_raw_fd(),
            "",
            expected_device,
            projection,
            &mut identities,
            &mut entries,
        )?;
        let after = stat_fd(directory.as_raw_fd())?;
        let rebound = stat_at(self.root.as_raw_fd(), &name)?;
        if !snapshot_metadata_stable(&before, &after)
            || !snapshot_metadata_stable(&before, &rebound)
        {
            return Err(os_error(libc::ESTALE));
        }
        self.verify_root_name()?;
        entries.sort_by(|left, right| {
            left.path
                .as_str()
                .as_bytes()
                .cmp(right.path.as_str().as_bytes())
        });
        Ok(SnapshotTreeInspection {
            root_mode: (before.st_mode & 0o777) as u32,
            root_device: before.st_dev as u64,
            root_inode: before.st_ino,
            entries,
        })
    }

    pub(crate) fn prepare_snapshot_for_publication_with_hook(
        &self,
        mut after_first_conversion: impl FnMut() -> io::Result<()>,
    ) -> io::Result<()> {
        self.verify_root_name()?;
        let metadata = stat_fd(self.root.as_raw_fd())?;
        require_snapshot_entry(&metadata, metadata.st_dev as u64)?;
        if file_type(metadata.st_mode) != libc::S_IFDIR
            || !matches!((metadata.st_mode & 0o777) as u32, 0o700 | 0o500)
        {
            return Err(snapshot_policy_error());
        }
        let mut first_conversion = true;
        make_snapshot_directory_owner_only(
            self.root.as_raw_fd(),
            metadata.st_dev as u64,
            &mut || {
                if first_conversion {
                    first_conversion = false;
                    after_first_conversion()?;
                }
                Ok(())
            },
        )?;
        // Darwin requires the moved directory itself to remain owner-writable
        // for renamex_np. Every child is already immutable here; the retained
        // descriptor seals this root immediately after the no-replace rename.
        chmod_fd(self.root.as_raw_fd(), 0o700)?;
        cvt(unsafe { libc::fsync(self.root.as_raw_fd()) })?;
        self.verify_root_name()
    }

    pub(crate) fn seal_snapshot_root(&self) -> io::Result<()> {
        self.verify_root_name()?;
        let metadata = stat_fd(self.root.as_raw_fd())?;
        require_snapshot_entry(&metadata, metadata.st_dev as u64)?;
        if file_type(metadata.st_mode) != libc::S_IFDIR
            || !matches!((metadata.st_mode & 0o7777) as u32, 0o700 | 0o500)
        {
            return Err(snapshot_policy_error());
        }
        chmod_fd(self.root.as_raw_fd(), 0o500)?;
        cvt(unsafe { libc::fsync(self.root.as_raw_fd()) })?;
        self.verify_root_name()
    }

    pub(crate) fn open_private_regular_handle(&self, name: &str) -> io::Result<File> {
        self.verify_root_name()?;
        let name = CString::new(name).map_err(interior_nul_error)?;
        let path_stat = stat_at(self.root.as_raw_fd(), &name)?;
        require_private_regular(&path_stat)?;
        let descriptor = open_regular_at(self.root.as_raw_fd(), &name)?;
        let opened = stat_fd(descriptor.as_raw_fd())?;
        require_private_regular(&opened)?;
        if !same_file(&path_stat, &opened) {
            return Err(os_error(libc::ESTALE));
        }
        Ok(File::from(descriptor))
    }

    pub(crate) fn open_private_append(&self, name: &str) -> io::Result<File> {
        self.verify_root_name()?;
        let name = CString::new(name).map_err(interior_nul_error)?;
        let before = stat_at(self.root.as_raw_fd(), &name)?;
        require_private_regular(&before)?;
        self.require_bound_regular_device(&before)?;
        let flags =
            libc::O_WRONLY | libc::O_APPEND | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;
        let descriptor =
            owned_fd(unsafe { libc::openat(self.root.as_raw_fd(), name.as_ptr(), flags) })?;
        let opened = stat_fd(descriptor.as_raw_fd())?;
        require_private_regular(&opened)?;
        self.require_bound_regular_device(&opened)?;
        let rebound = stat_at(self.root.as_raw_fd(), &name)?;
        require_private_regular(&rebound)?;
        self.require_bound_regular_device(&rebound)?;
        if !same_file(&before, &opened) || !same_file(&before, &rebound) {
            return Err(os_error(libc::ESTALE));
        }
        self.verify_root_name()?;
        Ok(File::from(descriptor))
    }

    pub(crate) fn validate_private_append_binding(
        &self,
        name: &str,
        file: &File,
    ) -> io::Result<u64> {
        self.verify_root_name()?;
        let name = CString::new(name).map_err(interior_nul_error)?;
        let opened = stat_fd(file.as_raw_fd())?;
        let rebound = stat_at(self.root.as_raw_fd(), &name)?;
        require_private_regular(&opened)?;
        require_private_regular(&rebound)?;
        self.require_bound_regular_device(&opened)?;
        self.require_bound_regular_device(&rebound)?;
        if !same_file(&opened, &rebound) || opened.st_size < 0 {
            return Err(os_error(libc::ESTALE));
        }
        self.verify_root_name()?;
        Ok(opened.st_size as u64)
    }

    fn require_bound_regular_device(&self, metadata: &libc::stat) -> io::Result<()> {
        if self
            .security_device
            .is_some_and(|device| metadata.st_dev as u64 != device)
        {
            return Err(os_error(libc::EXDEV));
        }
        Ok(())
    }

    pub(crate) fn replace_private_regular_exact(
        &self,
        name: &str,
        expected: &[u8],
        replacement: &[u8],
    ) -> io::Result<()> {
        self.verify_root_name()?;
        let target = CString::new(name).map_err(interior_nul_error)?;
        let before = stat_at(self.root.as_raw_fd(), &target)?;
        require_private_regular(&before)?;
        if before.st_size < 0 || before.st_size as usize != expected.len() {
            return Err(os_error(libc::ESTALE));
        }
        let opened = open_regular_at(self.root.as_raw_fd(), &target)?;
        let opened_stat = stat_fd(opened.as_raw_fd())?;
        require_private_regular(&opened_stat)?;
        if !same_file(&before, &opened_stat) {
            return Err(os_error(libc::ESTALE));
        }
        let mut old_bytes = Vec::with_capacity(expected.len());
        File::from(duplicate_fd(opened.as_raw_fd())?)
            .take(expected.len() as u64 + 1)
            .read_to_end(&mut old_bytes)?;
        let rebound = stat_at(self.root.as_raw_fd(), &target)?;
        let after_read = stat_fd(opened.as_raw_fd())?;
        if old_bytes != expected
            || !snapshot_metadata_stable(&before, &after_read)
            || !snapshot_metadata_stable(&before, &rebound)
        {
            return Err(os_error(libc::ESTALE));
        }

        let temporary = random_private_name("replace");
        let replacement_fd = create_regular_at(self.root.as_raw_fd(), &temporary)?;
        let mut replacement_file = File::from(replacement_fd);
        if let Err(error) = replacement_file
            .write_all(replacement)
            .and_then(|()| replacement_file.sync_all())
        {
            let _ = unlink_at(self.root.as_raw_fd(), &temporary, 0);
            return Err(error);
        }
        let replacement_identity = FileIdentity::from_stat(&stat_fd(replacement_file.as_raw_fd())?);
        self.verify_root_name()?;
        let rebound = stat_at(self.root.as_raw_fd(), &target)?;
        if !snapshot_metadata_stable(&before, &rebound) {
            let _ = unlink_at(self.root.as_raw_fd(), &temporary, 0);
            return Err(os_error(libc::ESTALE));
        }
        if let Err(error) = exchange_entries(
            self.root.as_raw_fd(),
            &temporary,
            self.root.as_raw_fd(),
            &target,
        ) {
            let _ = unlink_at(self.root.as_raw_fd(), &temporary, 0);
            return Err(error);
        }
        let published = stat_at(self.root.as_raw_fd(), &target)?;
        let displaced = stat_at(self.root.as_raw_fd(), &temporary)?;
        let replacement_opened = stat_fd(replacement_file.as_raw_fd())?;
        if FileIdentity::from_stat(&published) != replacement_identity
            || FileIdentity::from_stat(&replacement_opened) != replacement_identity
            || !same_file(&published, &replacement_opened)
            || !same_file(&displaced, &before)
        {
            return Err(os_error(libc::ESTALE));
        }
        self.verify_root_name()?;
        cvt(unsafe { libc::fsync(self.root.as_raw_fd()) })?;
        self.remove_owned_regular(temporary.to_str().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "replacement name is not UTF-8")
        })?)?;
        let final_binding = stat_at(self.root.as_raw_fd(), &target)?;
        if !same_file(&final_binding, &replacement_opened) {
            return Err(os_error(libc::ESTALE));
        }
        cvt(unsafe { libc::fsync(self.root.as_raw_fd()) })
    }

    pub(crate) fn write_private_atomic_no_replace(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> io::Result<()> {
        self.write_private_atomic_no_replace_with_hook(name, bytes, || {})
    }

    pub(crate) fn write_private_atomic_no_replace_with_commit_hooks(
        &self,
        name: &str,
        staging_name: &str,
        bytes: &[u8],
        after_file_sync: impl FnOnce() -> io::Result<()>,
        after_publish: impl FnOnce() -> io::Result<()>,
        after_parent_sync: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<()> {
        self.write_private_atomic_no_replace_with_all_hooks(
            (name, Some(staging_name)),
            bytes,
            after_file_sync,
            after_publish,
            after_parent_sync,
            (|| {}, || {}),
        )
    }

    pub(crate) fn write_private_atomic_no_replace_with_identity(
        &self,
        name: &str,
        build: impl FnOnce(PrivateEntryIdentity) -> io::Result<Vec<u8>>,
    ) -> io::Result<PrivateEntryIdentity> {
        self.verify_root_name()?;
        let namespace = PrivateNamespace::select(
            self.parent.as_raw_fd(),
            self.root_identity.device as libc::dev_t,
            &[DirectoryIdentity {
                descriptor: self.root.as_raw_fd(),
                identity: self.root_identity,
            }],
        )?;
        let target = CString::new(name).map_err(interior_nul_error)?;
        let temporary = random_private_name("write");
        let descriptor = create_regular_at(self.root.as_raw_fd(), &temporary)?;
        let identity = PrivateEntryIdentity::from_stat(&stat_fd(descriptor.as_raw_fd())?);
        let bytes = match build(identity) {
            Ok(bytes) => bytes,
            Err(error) => {
                let _ = unlink_at(self.root.as_raw_fd(), &temporary, 0);
                return Err(error);
            }
        };
        let mut file = File::from(descriptor);
        file.write_all(&bytes)?;
        file.sync_all()?;
        self.verify_root_name()?;
        if let Err(error) = rename_no_replace(
            self.root.as_raw_fd(),
            &temporary,
            self.root.as_raw_fd(),
            &target,
        ) {
            let _ = unlink_at(self.root.as_raw_fd(), &temporary, 0);
            return Err(error);
        }
        if let Err(validation_error) = self.verify_root_name() {
            let recovery = self.recover_published_regular(
                &target,
                &temporary,
                file.as_raw_fd(),
                FileIdentity {
                    device: identity.device,
                    inode: identity.inode,
                },
                || {},
                &namespace,
            );
            return match recovery {
                Ok(()) => Err(validation_error),
                Err(recovery_error) => Err(recovery_error),
            };
        }
        cvt(unsafe { libc::fsync(self.root.as_raw_fd()) })?;
        Ok(identity)
    }

    fn write_private_atomic_no_replace_with_hook(
        &self,
        name: &str,
        bytes: &[u8],
        after_final_validation: impl FnOnce(),
    ) -> io::Result<()> {
        self.write_private_atomic_no_replace_with_hooks(name, bytes, after_final_validation, || {})
    }

    fn write_private_atomic_no_replace_with_hooks(
        &self,
        name: &str,
        bytes: &[u8],
        after_final_validation: impl FnOnce(),
        before_recovery: impl FnOnce(),
    ) -> io::Result<()> {
        self.write_private_atomic_no_replace_with_all_hooks(
            (name, None),
            bytes,
            || Ok(()),
            || Ok(()),
            || Ok(()),
            (after_final_validation, before_recovery),
        )
    }

    fn write_private_atomic_no_replace_with_all_hooks(
        &self,
        names: (&str, Option<&str>),
        bytes: &[u8],
        after_file_sync: impl FnOnce() -> io::Result<()>,
        after_publish: impl FnOnce() -> io::Result<()>,
        after_parent_sync: impl FnOnce() -> io::Result<()>,
        race_hooks: (impl FnOnce(), impl FnOnce()),
    ) -> io::Result<()> {
        let (after_final_validation, before_recovery) = race_hooks;
        let (name, staging_name) = names;
        self.verify_root_name()?;
        let namespace = PrivateNamespace::select(
            self.parent.as_raw_fd(),
            self.root_identity.device as libc::dev_t,
            &[DirectoryIdentity {
                descriptor: self.root.as_raw_fd(),
                identity: self.root_identity,
            }],
        )?;
        let target = CString::new(name).map_err(interior_nul_error)?;
        let retain_staging_on_hook_error = staging_name.is_some();
        let temporary = match staging_name {
            Some(staging_name) => CString::new(staging_name).map_err(interior_nul_error)?,
            None => random_private_name("write"),
        };
        let descriptor = create_regular_at(self.root.as_raw_fd(), &temporary)?;
        let mut file = File::from(descriptor);
        file.write_all(bytes)?;
        file.sync_all()?;
        if let Err(error) = after_file_sync() {
            if !retain_staging_on_hook_error {
                let _ = unlink_at(self.root.as_raw_fd(), &temporary, 0);
            }
            return Err(error);
        }
        let file_identity = FileIdentity::from_stat(&stat_fd(file.as_raw_fd())?);
        self.verify_root_name()?;
        after_final_validation();
        if let Err(error) = rename_no_replace(
            self.root.as_raw_fd(),
            &temporary,
            self.root.as_raw_fd(),
            &target,
        ) {
            let _ = unlink_at(self.root.as_raw_fd(), &temporary, 0);
            return Err(error);
        }
        if let Err(validation_error) = self.verify_root_name() {
            let recovery = self.recover_published_regular(
                &target,
                &temporary,
                file.as_raw_fd(),
                file_identity,
                before_recovery,
                &namespace,
            );
            return match recovery {
                Ok(()) => Err(validation_error),
                Err(recovery_error) => Err(recovery_error),
            };
        }
        after_publish()?;
        cvt(unsafe { libc::fsync(self.root.as_raw_fd()) })?;
        after_parent_sync()
    }

    pub(crate) fn write_new_private_file(&self, name: &str, bytes: &[u8]) -> io::Result<File> {
        self.verify_root_name()?;
        let name = CString::new(name).map_err(interior_nul_error)?;
        let descriptor = create_regular_at(self.root.as_raw_fd(), &name)?;
        let mut file = File::from(descriptor);
        file.write_all(bytes)?;
        Ok(file)
    }

    pub(crate) fn remove_owned_child(&self, name: &str) -> io::Result<()> {
        let component = private_leaf_name(name)?;
        self.verify_root_name()?;
        let parent_metadata = stat_fd(self.root.as_raw_fd())?;
        require_private_directory(&parent_metadata)?;
        let parent = FileIdentity::from_stat(&parent_metadata);
        let namespace =
            PrivateNamespace::select(self.root.as_raw_fd(), parent_metadata.st_dev, &[])?;
        validate_cleanup_namespace_evidence(&namespace, self.root.as_raw_fd(), parent)?;
        let verify_parent = || {
            self.verify_root_name()?;
            let current = stat_fd(self.root.as_raw_fd())?;
            if FileIdentity::from_stat(&current) != parent {
                return Err(os_error(libc::ESTALE));
            }
            Ok(())
        };
        if let Some(loaded) = find_cleanup_intent(&namespace, parent, component.to_bytes())? {
            let target = FileIdentity::from(loaded.intent.target);
            let original_mode = loaded.intent.original_mode;
            let result = resume_tree_cleanup(
                &namespace,
                self.root.as_raw_fd(),
                parent,
                component.to_bytes(),
                &loaded,
                &verify_parent,
                true,
            );
            return match result {
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                    ) =>
                {
                    drop(loaded);
                    resolve_bound_tree_cleanup(
                        &namespace,
                        self.root.as_raw_fd(),
                        parent,
                        component.to_bytes(),
                        target,
                        original_mode,
                        &verify_parent,
                        true,
                    )
                }
                result => result,
            };
        }
        let before = match stat_at(self.root.as_raw_fd(), &component) {
            Ok(before) => before,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        if file_type(before.st_mode) != libc::S_IFDIR
            || before.st_uid != effective_user_id()
            || before.st_dev != parent_metadata.st_dev
        {
            return Err(os_error(libc::ESTALE));
        }
        let target_identity = FileIdentity::from_stat(&before);
        let original_mode = before.st_mode as u32 & 0o7777;
        let relative = RelativePath::parse(component.to_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid child component"))?;
        let target = match self.open_child_directory(&relative, false) {
            Ok(target) => target,
            Err(error)
                if matches!(
                    error.raw_os_error(),
                    Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                ) =>
            {
                return resolve_bound_tree_cleanup(
                    &namespace,
                    self.root.as_raw_fd(),
                    parent,
                    component.to_bytes(),
                    target_identity,
                    original_mode,
                    &verify_parent,
                    true,
                );
            }
            Err(error) => return Err(error),
        };
        let opened = stat_fd(target.root.as_raw_fd())?;
        if !same_file(&before, &opened)
            || opened.st_uid != effective_user_id()
            || opened.st_dev != parent_metadata.st_dev
            || opened.st_mode as u32 & 0o7777 != original_mode
        {
            return resolve_bound_tree_cleanup(
                &namespace,
                self.root.as_raw_fd(),
                parent,
                component.to_bytes(),
                target_identity,
                original_mode,
                &verify_parent,
                true,
            );
        }
        match publish_tree_cleanup_intent(
            &namespace,
            self.root.as_raw_fd(),
            parent,
            &component,
            target_identity,
            original_mode,
        ) {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.raw_os_error(),
                    Some(libc::EEXIST)
                        | Some(libc::EAGAIN)
                        | Some(libc::ENOENT)
                        | Some(libc::ESTALE)
                ) => {}
            Err(error) => return Err(error),
        }
        resolve_bound_tree_cleanup(
            &namespace,
            self.root.as_raw_fd(),
            parent,
            component.to_bytes(),
            target_identity,
            original_mode,
            &verify_parent,
            true,
        )
    }

    pub(crate) fn resume_pending_owned_child_cleanup(&self, name: &str) -> io::Result<bool> {
        let component = private_leaf_name(name)?;
        self.verify_root_name()?;
        let parent_metadata = stat_fd(self.root.as_raw_fd())?;
        require_private_directory(&parent_metadata)?;
        let parent = FileIdentity::from_stat(&parent_metadata);
        let namespace =
            PrivateNamespace::select(self.root.as_raw_fd(), parent_metadata.st_dev, &[])?;
        validate_cleanup_namespace_evidence(&namespace, self.root.as_raw_fd(), parent)?;
        let verify_parent = || {
            self.verify_root_name()?;
            let current = stat_fd(self.root.as_raw_fd())?;
            if FileIdentity::from_stat(&current) != parent {
                return Err(os_error(libc::ESTALE));
            }
            Ok(())
        };
        if let Some(loaded) = find_cleanup_intent(&namespace, parent, component.to_bytes())? {
            let target = FileIdentity::from(loaded.intent.target);
            let original_mode = loaded.intent.original_mode;
            match resume_tree_cleanup(
                &namespace,
                self.root.as_raw_fd(),
                parent,
                component.to_bytes(),
                &loaded,
                &verify_parent,
                false,
            ) {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                    ) =>
                {
                    drop(loaded);
                    resolve_bound_tree_cleanup(
                        &namespace,
                        self.root.as_raw_fd(),
                        parent,
                        component.to_bytes(),
                        target,
                        original_mode,
                        &verify_parent,
                        false,
                    )?
                }
                Err(error) => return Err(error),
            }
            return Ok(true);
        }
        let Some(candidate) = find_unpublished_cleanup_bootstrap(
            &namespace,
            self.root.as_raw_fd(),
            parent,
            component.to_bytes(),
        )?
        else {
            return Ok(false);
        };
        resolve_bound_tree_cleanup(
            &namespace,
            self.root.as_raw_fd(),
            parent,
            component.to_bytes(),
            candidate.target,
            candidate.original_mode,
            &verify_parent,
            false,
        )?;
        Ok(true)
    }

    pub(crate) fn retry_pending_owned_children_matching(
        &self,
        mut predicate: impl FnMut(&[u8], PrivateEntryIdentity) -> bool,
    ) -> io::Result<usize> {
        self.verify_root_name()?;
        let parent_metadata = stat_fd(self.root.as_raw_fd())?;
        require_private_directory(&parent_metadata)?;
        let parent = FileIdentity::from_stat(&parent_metadata);
        let namespace =
            PrivateNamespace::select(self.root.as_raw_fd(), parent_metadata.st_dev, &[])?;
        validate_cleanup_namespace_evidence(&namespace, self.root.as_raw_fd(), parent)?;
        let candidates =
            collect_pending_tree_cleanup_candidates(&namespace, self.root.as_raw_fd(), parent)?;
        let verify_parent = || {
            self.verify_root_name()?;
            let current = stat_fd(self.root.as_raw_fd())?;
            if FileIdentity::from_stat(&current) != parent {
                return Err(os_error(libc::ESTALE));
            }
            Ok(())
        };
        let mut resumed = 0;
        for candidate in candidates {
            let target = PrivateEntryIdentity {
                device: candidate.target.device,
                inode: candidate.target.inode,
                kind: libc::S_IFDIR as u32,
                owner: effective_user_id(),
                mode: candidate.original_mode & 0o777,
            };
            if !predicate(&candidate.component, target) {
                continue;
            }
            resolve_bound_tree_cleanup(
                &namespace,
                self.root.as_raw_fd(),
                parent,
                &candidate.component,
                candidate.target,
                candidate.original_mode,
                &verify_parent,
                false,
            )?;
            resumed += 1;
        }
        Ok(resumed)
    }

    pub(crate) fn remove_owned_regular(&self, name: &str) -> io::Result<()> {
        self.remove_owned_regular_with_hooks(name, |_| {}, |_| {}, |_| {})
    }

    fn remove_owned_regular_with_hooks(
        &self,
        name: &str,
        after_private_rename: impl FnOnce(&CStr),
        before_recovery: impl FnOnce(&CStr),
        before_final_delete: impl FnOnce(&CStr),
    ) -> io::Result<()> {
        self.verify_root_name()?;
        let name = CString::new(name).map_err(interior_nul_error)?;
        let before = stat_at(self.root.as_raw_fd(), &name)?;
        require_private_regular(&before)?;
        let file = open_regular_at(self.root.as_raw_fd(), &name)?;
        let opened = stat_fd(file.as_raw_fd())?;
        if !same_file(&before, &opened) {
            return Err(os_error(libc::ESTALE));
        }
        let namespace = PrivateNamespace::select(
            self.parent.as_raw_fd(),
            self.root_identity.device as libc::dev_t,
            &[DirectoryIdentity {
                descriptor: self.root.as_raw_fd(),
                identity: self.root_identity,
            }],
        )?;
        let private = random_private_name("remove");
        self.verify_root_name()?;
        rename_no_replace(
            self.root.as_raw_fd(),
            &name,
            self.root.as_raw_fd(),
            &private,
        )?;
        after_private_rename(&private);
        let moved = stat_at(self.root.as_raw_fd(), &private)?;
        if !same_file(&opened, &moved) {
            return Err(os_error(libc::ESTALE));
        }
        if let Err(validation_error) = self.verify_root_name() {
            before_recovery(&private);
            let recovery = (|| {
                let mut operation = PrivateOperation::create(&namespace)?;
                let (placeholder_name, placeholder_identity) =
                    create_exchange_placeholder(&operation, false)?;
                let expected = FileIdentity::from_stat(&opened);
                capture_expected_entry(
                    &mut operation,
                    self.root.as_raw_fd(),
                    &private,
                    &placeholder_name,
                    placeholder_identity,
                    expected,
                    false,
                )?;
                if let Err(error) = rename_no_replace(
                    operation.directory.as_raw_fd(),
                    &placeholder_name,
                    self.root.as_raw_fd(),
                    &name,
                ) {
                    operation.cleaned = true;
                    return Err(error);
                }
                remove_installed_placeholder(
                    &mut operation,
                    self.root.as_raw_fd(),
                    &private,
                    &placeholder_name,
                    placeholder_identity,
                    false,
                )
            })();
            return match recovery {
                Ok(()) => Err(validation_error),
                Err(recovery_error) => Err(recovery_error),
            };
        }
        before_final_delete(&private);
        let mut operation = PrivateOperation::create(&namespace)?;
        let (placeholder_name, placeholder_identity) =
            create_exchange_placeholder(&operation, false)?;
        capture_expected_entry(
            &mut operation,
            self.root.as_raw_fd(),
            &private,
            &placeholder_name,
            placeholder_identity,
            FileIdentity::from_stat(&opened),
            false,
        )?;
        unlink_at(operation.directory.as_raw_fd(), &placeholder_name, 0)?;
        remove_installed_placeholder(
            &mut operation,
            self.root.as_raw_fd(),
            &private,
            &placeholder_name,
            placeholder_identity,
            false,
        )
    }

    pub(crate) fn publish_owned_into(
        &mut self,
        destination_parent: &RootedDir,
        destination_name: &str,
    ) -> io::Result<()> {
        self.publish_owned_into_with_hook(destination_parent, destination_name, || {})
    }

    pub(crate) fn publish_owned_into_with_post_rename(
        &mut self,
        destination_parent: &RootedDir,
        destination_name: &str,
        after_rename: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<()> {
        self.publish_owned_into_with_hooks(
            destination_parent,
            destination_name,
            || {},
            after_rename,
            || Ok(()),
            || {},
        )
    }

    pub(crate) fn publish_owned_into_with_commit_hooks(
        &mut self,
        destination_parent: &RootedDir,
        destination_name: &str,
        after_rename: impl FnOnce() -> io::Result<()>,
        after_parent_sync: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<()> {
        self.publish_owned_into_with_hooks(
            destination_parent,
            destination_name,
            || {},
            after_rename,
            after_parent_sync,
            || {},
        )
    }

    fn publish_owned_into_with_hook(
        &mut self,
        destination_parent: &RootedDir,
        destination_name: &str,
        after_final_validation: impl FnOnce(),
    ) -> io::Result<()> {
        self.publish_owned_into_with_hooks(
            destination_parent,
            destination_name,
            after_final_validation,
            || Ok(()),
            || Ok(()),
            || {},
        )
    }

    fn publish_owned_into_with_hooks(
        &mut self,
        destination_parent: &RootedDir,
        destination_name: &str,
        after_final_validation: impl FnOnce(),
        after_rename: impl FnOnce() -> io::Result<()>,
        after_parent_sync: impl FnOnce() -> io::Result<()>,
        before_recovery: impl FnOnce(),
    ) -> io::Result<()> {
        self.verify_root_name()?;
        destination_parent.verify_root_name()?;
        let destination_name = CString::new(destination_name).map_err(interior_nul_error)?;
        let destination_metadata = stat_fd(destination_parent.root.as_raw_fd())?;
        if destination_metadata.st_dev != self.root_identity.device as libc::dev_t {
            return Err(os_error(libc::EXDEV));
        }
        if let Some(device) = self.security_device {
            if destination_parent.security_device != Some(device) {
                return Err(os_error(libc::EXDEV));
            }
            require_private_directory_on_device(&destination_metadata, device)?;
        }
        let namespace = PrivateNamespace::select(
            self.parent.as_raw_fd(),
            self.root_identity.device as libc::dev_t,
            &[
                DirectoryIdentity {
                    descriptor: self.root.as_raw_fd(),
                    identity: self.root_identity,
                },
                DirectoryIdentity {
                    descriptor: destination_parent.root.as_raw_fd(),
                    identity: destination_parent.root_identity,
                },
            ],
        )?;
        let rebound_parent = reopen_directory(destination_parent.root.as_raw_fd())?;
        let rebound_lineage = destination_parent.child_lineage()?;
        self.verify_root_name()?;
        destination_parent.verify_root_name()?;
        after_final_validation();
        rename_no_replace(
            self.parent.as_raw_fd(),
            &self.root_name,
            destination_parent.root.as_raw_fd(),
            &destination_name,
        )?;
        after_rename()?;
        let published = stat_at(destination_parent.root.as_raw_fd(), &destination_name);
        let validation = published.and_then(|published| {
            let opened = stat_fd(self.root.as_raw_fd())?;
            if file_type(published.st_mode) != libc::S_IFDIR
                || FileIdentity::from_stat(&published) != self.root_identity
                || !same_file(&published, &opened)
            {
                return Err(os_error(libc::ESTALE));
            }
            verify_lineage(&self.lineage)?;
            destination_parent.verify_root_name()
        });
        if let Err(validation_error) = validation {
            before_recovery();
            let recovery = (|| {
                let mut operation = PrivateOperation::create(&namespace)?;
                let (placeholder_name, placeholder_identity) =
                    create_exchange_placeholder(&operation, true)?;
                capture_expected_entry(
                    &mut operation,
                    destination_parent.root.as_raw_fd(),
                    &destination_name,
                    &placeholder_name,
                    placeholder_identity,
                    self.root_identity,
                    true,
                )?;
                if let Err(error) = rename_no_replace(
                    operation.directory.as_raw_fd(),
                    &placeholder_name,
                    self.parent.as_raw_fd(),
                    &self.root_name,
                ) {
                    operation.cleaned = true;
                    return Err(error);
                }
                remove_installed_placeholder(
                    &mut operation,
                    destination_parent.root.as_raw_fd(),
                    &destination_name,
                    &placeholder_name,
                    placeholder_identity,
                    true,
                )?;
                cvt(unsafe { libc::fsync(self.parent.as_raw_fd()) })
            })();
            return match recovery {
                Ok(()) => Err(validation_error),
                Err(recovery_error) => Err(recovery_error),
            };
        }
        cvt(unsafe { libc::fsync(self.parent.as_raw_fd()) })?;
        cvt(unsafe { libc::fsync(destination_parent.root.as_raw_fd()) })?;
        after_parent_sync()?;
        self.parent = rebound_parent;
        self.root_name = destination_name;
        self.lineage = rebound_lineage;
        self.security_device = destination_parent.security_device;
        Ok(())
    }

    fn recover_published_regular(
        &self,
        target: &CStr,
        temporary: &CStr,
        file: RawFd,
        expected: FileIdentity,
        before_recovery: impl FnOnce(),
        namespace: &PrivateNamespace,
    ) -> io::Result<()> {
        let published = stat_at(self.root.as_raw_fd(), target)?;
        let opened = stat_fd(file)?;
        if file_type(published.st_mode) != libc::S_IFREG
            || FileIdentity::from_stat(&published) != expected
            || FileIdentity::from_stat(&opened) != expected
            || !same_file(&published, &opened)
        {
            return Err(os_error(libc::ESTALE));
        }
        before_recovery();
        let mut operation = PrivateOperation::create(namespace)?;
        let (placeholder_name, placeholder_identity) =
            create_exchange_placeholder(&operation, false)?;
        capture_expected_entry(
            &mut operation,
            self.root.as_raw_fd(),
            target,
            &placeholder_name,
            placeholder_identity,
            expected,
            false,
        )?;
        let recovered = stat_at(operation.directory.as_raw_fd(), &placeholder_name)?;
        if FileIdentity::from_stat(&recovered) != expected || !same_file(&recovered, &opened) {
            operation.cleaned = true;
            return Err(os_error(libc::ESTALE));
        }
        unlink_at(operation.directory.as_raw_fd(), &placeholder_name, 0)?;
        remove_installed_placeholder(
            &mut operation,
            self.root.as_raw_fd(),
            target,
            &placeholder_name,
            placeholder_identity,
            false,
        )?;
        let _ = temporary;
        Ok(())
    }

    #[allow(dead_code)] // Existing staged-publication integrity boundary.
    pub(crate) fn validate_and_sync_declared_tree(
        &self,
        declared: &BTreeSet<RelativePath>,
    ) -> io::Result<()> {
        if declared.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty staged tree",
            ));
        }
        self.verify_root_name()?;
        let mut actual = BTreeSet::new();
        collect_and_sync_tree(self.root.as_raw_fd(), "", &mut actual)?;
        if &actual != declared {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "staged tree does not match its declaration",
            ));
        }
        cvt(unsafe { libc::fsync(self.root.as_raw_fd()) })
    }

    pub(crate) fn validate_and_sync_snapshot_workspace(
        &self,
        declared: &BTreeSet<RelativePath>,
    ) -> io::Result<()> {
        self.verify_root_name()?;
        let mut actual = BTreeSet::new();
        collect_and_sync_tree(self.root.as_raw_fd(), "", &mut actual)?;
        if &actual != declared {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "workspace tree does not match its verified snapshot",
            ));
        }
        cvt(unsafe { libc::fsync(self.root.as_raw_fd()) })
    }

    pub fn inspect(&self, path: &RelativePath) -> io::Result<EntryInspection> {
        let (parent, name) = self.open_parent(path, false)?;
        let path_stat = stat_at(parent.as_raw_fd(), &name)?;
        match file_type(path_stat.st_mode) {
            libc::S_IFREG => {
                let descriptor = open_regular_at(parent.as_raw_fd(), &name)?;
                let descriptor_stat = stat_fd(descriptor.as_raw_fd())?;
                if file_type(descriptor_stat.st_mode) != libc::S_IFREG {
                    return Err(invalid_type_error());
                }
                if !same_file(&path_stat, &descriptor_stat) {
                    return Err(os_error(libc::ESTALE));
                }
                let metadata = metadata_from_stat(descriptor_stat)?;
                Ok(EntryInspection {
                    kind: metadata.kind,
                    mode: metadata.mode,
                    size: metadata.size,
                    device: metadata.device,
                    inode: metadata.inode,
                    modified_seconds: metadata.modified_seconds,
                    modified_nanoseconds: metadata.modified_nanoseconds,
                    file: Some(File::from(descriptor)),
                })
            }
            libc::S_IFLNK => {
                let metadata = metadata_from_stat(path_stat)?;
                Ok(EntryInspection {
                    kind: metadata.kind,
                    mode: metadata.mode,
                    size: metadata.size,
                    device: metadata.device,
                    inode: metadata.inode,
                    modified_seconds: metadata.modified_seconds,
                    modified_nanoseconds: metadata.modified_nanoseconds,
                    file: None,
                })
            }
            _ => Err(invalid_type_error()),
        }
    }

    pub fn copy_regular_to(&self, path: &RelativePath, destination: &RootedDir) -> io::Result<()> {
        let (source_parent, source_name) = self.open_parent(path, false)?;
        let source_path_stat = stat_at(source_parent.as_raw_fd(), &source_name)?;
        match file_type(source_path_stat.st_mode) {
            libc::S_IFREG => {}
            libc::S_IFLNK => return Err(os_error(libc::ELOOP)),
            _ => return Err(invalid_type_error()),
        }
        let source = open_regular_at(source_parent.as_raw_fd(), &source_name)?;
        let source_stat = stat_fd(source.as_raw_fd())?;
        if file_type(source_stat.st_mode) != libc::S_IFREG {
            return Err(invalid_type_error());
        }
        if !same_file(&source_path_stat, &source_stat) {
            return Err(os_error(libc::ESTALE));
        }
        let destination_mode = if source_stat.st_mode & 0o111 != 0 {
            0o555
        } else {
            0o444
        };
        let namespace = PrivateNamespace::select(
            destination.parent.as_raw_fd(),
            destination.root_identity.device as libc::dev_t,
            &[
                DirectoryIdentity {
                    descriptor: self.root.as_raw_fd(),
                    identity: self.root_identity,
                },
                DirectoryIdentity {
                    descriptor: destination.root.as_raw_fd(),
                    identity: destination.root_identity,
                },
            ],
        )?;
        let (destination_parent, destination_name) = destination.open_parent(path, true)?;
        if stat_fd(destination_parent.as_raw_fd())?.st_dev
            != destination.root_identity.device as libc::dev_t
        {
            return Err(os_error(libc::EXDEV));
        }

        copy_regular_in_namespace(
            source,
            &namespace,
            &destination_parent,
            &destination_name,
            destination_mode,
        )
    }

    pub(crate) fn copy_snapshot_regular_to_writable(
        &self,
        path: &RelativePath,
        destination: &RootedDir,
        executable: bool,
    ) -> io::Result<()> {
        self.verify_root_name()?;
        destination.verify_root_name()?;
        let source_root = stat_fd(self.root.as_raw_fd())?;
        let source_device = source_root.st_dev as u64;
        require_snapshot_directory(&source_root, source_device, SnapshotProjection::OwnerOnly)?;
        let (source_parent, source_name, source_lineage) =
            self.open_private_parent(path, false, self.security_device)?;
        let before = stat_at(source_parent.as_raw_fd(), &source_name)?;
        require_snapshot_regular(&before, source_device, SnapshotProjection::OwnerOnly)?;
        let source = open_regular_at(source_parent.as_raw_fd(), &source_name)?;
        let opened = stat_fd(source.as_raw_fd())?;
        require_snapshot_regular(&opened, source_device, SnapshotProjection::OwnerOnly)?;
        if !snapshot_metadata_stable(&before, &opened) {
            return Err(os_error(libc::ESTALE));
        }
        let namespace = PrivateNamespace::select(
            destination.parent.as_raw_fd(),
            destination.root_identity.device as libc::dev_t,
            &[
                DirectoryIdentity {
                    descriptor: self.root.as_raw_fd(),
                    identity: self.root_identity,
                },
                DirectoryIdentity {
                    descriptor: destination.root.as_raw_fd(),
                    identity: destination.root_identity,
                },
            ],
        )?;
        let (destination_parent, destination_name) = destination.open_parent(path, true)?;
        let destination_device = stat_fd(destination_parent.as_raw_fd())?.st_dev as u64;
        if destination_device != destination.root_identity.device {
            return Err(os_error(libc::EXDEV));
        }
        copy_regular_in_namespace(
            duplicate_fd(source.as_raw_fd())?,
            &namespace,
            &destination_parent,
            &destination_name,
            if executable { 0o700 } else { 0o600 },
        )?;

        let after = stat_fd(source.as_raw_fd())?;
        let rebound = stat_at(source_parent.as_raw_fd(), &source_name)?;
        if !snapshot_metadata_stable(&before, &after)
            || !snapshot_metadata_stable(&before, &rebound)
        {
            return Err(os_error(libc::ESTALE));
        }
        verify_lineage(&source_lineage)?;
        self.verify_root_name()?;

        let destination_path_stat = stat_at(destination_parent.as_raw_fd(), &destination_name)?;
        let destination_file = open_regular_at(destination_parent.as_raw_fd(), &destination_name)?;
        let destination_opened = stat_fd(destination_file.as_raw_fd())?;
        let expected_mode = if executable { 0o700 } else { 0o600 };
        if file_type(destination_path_stat.st_mode) != libc::S_IFREG
            || file_type(destination_opened.st_mode) != libc::S_IFREG
            || destination_path_stat.st_uid != unsafe { libc::geteuid() }
            || destination_opened.st_uid != unsafe { libc::geteuid() }
            || destination_path_stat.st_dev as u64 != destination_device
            || destination_opened.st_dev as u64 != destination_device
            || destination_path_stat.st_nlink != 1
            || destination_opened.st_nlink != 1
            || (destination_path_stat.st_mode & 0o7777) as u32 != expected_mode
            || (destination_opened.st_mode & 0o7777) as u32 != expected_mode
            || !same_file(&destination_path_stat, &destination_opened)
            || same_file(&opened, &destination_opened)
        {
            return Err(snapshot_policy_error());
        }
        destination.verify_root_name()
    }

    pub fn create_empty_directory(&self, path: &RelativePath) -> io::Result<()> {
        let (parent, name) = self.open_parent(path, true)?;
        match mkdir_at(parent.as_raw_fd(), &name, 0o700) {
            Ok(()) => {}
            Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {
                let existing = open_directory_at(parent.as_raw_fd(), &name)?;
                drop(existing);
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }

    pub fn read_symlink(&self, path: &RelativePath) -> io::Result<String> {
        let (parent, name) = self.open_parent(path, false)?;
        let metadata = stat_at(parent.as_raw_fd(), &name)?;
        if file_type(metadata.st_mode) != libc::S_IFLNK {
            return Err(invalid_type_error());
        }
        let bytes = read_link_at(parent.as_raw_fd(), &name, metadata.st_size)?;
        String::from_utf8(bytes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "UNSUPPORTED_PATH_ENCODING: symlink target is not UTF-8",
            )
        })
    }

    pub fn create_symlink(&self, path: &RelativePath, target: &str) -> io::Result<()> {
        let (parent, name) = self.open_parent(path, true)?;
        let target = CString::new(target).map_err(interior_nul_error)?;
        symlink_at(&target, parent.as_raw_fd(), &name)
    }

    pub fn make_read_only(&self) -> io::Result<()> {
        make_directory_read_only(self.root.as_raw_fd())
    }

    pub(crate) fn publish_owned_to(&mut self, destination: &Path) -> io::Result<()> {
        let (destination_parent_path, destination_name) = split_root_path(destination)?;
        let destination_parent = open_directory_path(destination_parent_path)?;
        self.verify_root_name()?;
        if stat_fd(destination_parent.as_raw_fd())?.st_dev
            != self.root_identity.device as libc::dev_t
        {
            return Err(os_error(libc::EXDEV));
        }
        rename_no_replace(
            self.parent.as_raw_fd(),
            &self.root_name,
            destination_parent.as_raw_fd(),
            &destination_name,
        )?;

        // All fallible work happens before the rename. Once the kernel moves
        // the exact opened root, rebinding its cleanup parent/name is
        // infallible and preserves ownership across the publication boundary.
        self.parent = destination_parent;
        self.root_name = destination_name;
        Ok(())
    }

    pub(crate) fn sync_parent(&self) -> io::Result<()> {
        // SAFETY: the retained parent descriptor is live for this call and
        // fsync does not retain it.
        cvt(unsafe { libc::fsync(self.parent.as_raw_fd()) })
    }

    pub(crate) fn sync_root(&self) -> io::Result<()> {
        // SAFETY: the retained root descriptor is live for this call and
        // fsync does not retain it.
        cvt(unsafe { libc::fsync(self.root.as_raw_fd()) })
    }

    pub fn remove_owned_tree(&self) -> io::Result<()> {
        let parent_metadata = stat_fd(self.parent.as_raw_fd())?;
        let parent = FileIdentity::from_stat(&parent_metadata);
        if self.root_identity.device != parent.device {
            return Err(os_error(libc::EXDEV));
        }
        let namespace = PrivateNamespace::select(
            self.parent.as_raw_fd(),
            parent_metadata.st_dev,
            &[DirectoryIdentity {
                descriptor: self.root.as_raw_fd(),
                identity: self.root_identity,
            }],
        )?;
        validate_cleanup_namespace_evidence(&namespace, self.parent.as_raw_fd(), parent)?;
        let verify_parent = || {
            let current = stat_fd(self.parent.as_raw_fd())?;
            if FileIdentity::from_stat(&current) != parent {
                return Err(os_error(libc::ESTALE));
            }
            verify_lineage(&self.lineage)
        };
        if let Some(loaded) = find_cleanup_intent(&namespace, parent, self.root_name.to_bytes())? {
            let target = FileIdentity::from(loaded.intent.target);
            let original_mode = loaded.intent.original_mode;
            let result = resume_tree_cleanup(
                &namespace,
                self.parent.as_raw_fd(),
                parent,
                self.root_name.to_bytes(),
                &loaded,
                &verify_parent,
                true,
            );
            return match result {
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                    ) =>
                {
                    drop(loaded);
                    resolve_bound_tree_cleanup(
                        &namespace,
                        self.parent.as_raw_fd(),
                        parent,
                        self.root_name.to_bytes(),
                        target,
                        original_mode,
                        &verify_parent,
                        true,
                    )
                }
                result => result,
            };
        }
        let current = match stat_at(self.parent.as_raw_fd(), &self.root_name) {
            Ok(current) => current,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        let opened = stat_fd(self.root.as_raw_fd())?;
        if file_type(current.st_mode) != libc::S_IFDIR
            || current.st_uid != effective_user_id()
            || opened.st_uid != effective_user_id()
            || !same_file(&current, &opened)
            || FileIdentity::from_stat(&opened) != self.root_identity
        {
            return Err(os_error(libc::ESTALE));
        }
        let original_mode = opened.st_mode as u32 & 0o7777;
        match publish_tree_cleanup_intent(
            &namespace,
            self.parent.as_raw_fd(),
            parent,
            &self.root_name,
            self.root_identity,
            original_mode,
        ) {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.raw_os_error(),
                    Some(libc::EEXIST)
                        | Some(libc::EAGAIN)
                        | Some(libc::ENOENT)
                        | Some(libc::ESTALE)
                ) => {}
            Err(error) => return Err(error),
        }
        resolve_bound_tree_cleanup(
            &namespace,
            self.parent.as_raw_fd(),
            parent,
            self.root_name.to_bytes(),
            self.root_identity,
            original_mode,
            &verify_parent,
            true,
        )
    }

    fn remove_owned_tree_with_hook(
        &self,
        after_private_validation: impl FnOnce(),
    ) -> io::Result<()> {
        self.remove_owned_tree_with_hooks(|| {}, || {}, after_private_validation, || {}, || {})
    }

    fn remove_owned_tree_with_hooks(
        &self,
        before_private_rename: impl FnOnce(),
        after_private_rename: impl FnOnce(),
        after_private_validation: impl FnOnce(),
        before_recovery: impl FnOnce(),
        before_final_delete: impl FnOnce(),
    ) -> io::Result<()> {
        let namespace = PrivateNamespace::select(
            self.parent.as_raw_fd(),
            self.root_identity.device as libc::dev_t,
            &[DirectoryIdentity {
                descriptor: self.root.as_raw_fd(),
                identity: self.root_identity,
            }],
        )?;
        self.verify_root_name()?;
        let parent_device = stat_fd(self.parent.as_raw_fd())?.st_dev;
        if self.root_identity.device != parent_device as u64 {
            return Err(os_error(libc::EXDEV));
        }
        let original_mode = stat_fd(self.root.as_raw_fd())?.st_mode & 0o7777;
        let private_name = random_private_name("cleanup");
        // macOS requires write/search permission on a directory moved across
        // parents. This descriptor is identity-bound to the originally opened
        // root, so a caller-name replacement is never chmodded here.
        chmod_fd(self.root.as_raw_fd(), 0o700)?;
        before_private_rename();
        if let Err(error) = rename_no_replace(
            self.parent.as_raw_fd(),
            &self.root_name,
            namespace.directory.as_raw_fd(),
            &private_name,
        ) {
            return match chmod_fd(self.root.as_raw_fd(), original_mode) {
                Ok(()) => Err(error),
                Err(restoration_error) => Err(restoration_error),
            };
        }
        after_private_rename();
        cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })?;
        cvt(unsafe { libc::fsync(self.parent.as_raw_fd()) })?;
        let moved = stat_at(namespace.directory.as_raw_fd(), &private_name);
        let validation = moved.and_then(|moved| {
            let opened = stat_fd(self.root.as_raw_fd())?;
            if file_type(moved.st_mode) != libc::S_IFDIR
                || FileIdentity::from_stat(&moved) != self.root_identity
                || !same_file(&moved, &opened)
            {
                return Err(os_error(libc::ESTALE));
            }
            verify_lineage(&self.lineage)?;
            Ok(())
        });
        let validation = validation.and_then(|()| injected_cleanup_validation_result());
        if let Err(validation_error) = validation {
            before_recovery();
            let rename_restoration = (|| {
                let mut operation = PrivateOperation::create(&namespace)?;
                let (placeholder_name, placeholder_identity) =
                    create_exchange_placeholder(&operation, true)?;
                capture_expected_entry(
                    &mut operation,
                    namespace.directory.as_raw_fd(),
                    &private_name,
                    &placeholder_name,
                    placeholder_identity,
                    self.root_identity,
                    true,
                )?;
                if let Err(error) = rename_no_replace(
                    operation.directory.as_raw_fd(),
                    &placeholder_name,
                    self.parent.as_raw_fd(),
                    &self.root_name,
                ) {
                    operation.cleaned = true;
                    return Err(error);
                }
                remove_installed_placeholder(
                    &mut operation,
                    namespace.directory.as_raw_fd(),
                    &private_name,
                    &placeholder_name,
                    placeholder_identity,
                    true,
                )?;
                cvt(unsafe { libc::fsync(self.parent.as_raw_fd()) })
            })();
            let mode_restoration = chmod_fd(self.root.as_raw_fd(), original_mode);
            return match rename_restoration {
                Ok(()) => match mode_restoration {
                    Ok(()) => Err(validation_error),
                    Err(restoration_error) => Err(restoration_error),
                },
                // The private entry failed identity validation and could not
                // be restored. It remains untouched in the private namespace.
                Err(rename_error) => match mode_restoration {
                    Ok(()) => Err(rename_error),
                    Err(restoration_error) => Err(restoration_error),
                },
            };
        }
        after_private_validation();
        remove_acquired_directory_contents(self.root.as_raw_fd())?;
        injected_cleanup_final_remove_result()?;
        before_final_delete();
        let mut operation = PrivateOperation::create(&namespace)?;
        let (placeholder_name, placeholder_identity) =
            create_exchange_placeholder(&operation, true)?;
        capture_expected_entry(
            &mut operation,
            namespace.directory.as_raw_fd(),
            &private_name,
            &placeholder_name,
            placeholder_identity,
            self.root_identity,
            true,
        )?;
        unlink_at(
            operation.directory.as_raw_fd(),
            &placeholder_name,
            libc::AT_REMOVEDIR,
        )?;
        remove_installed_placeholder(
            &mut operation,
            namespace.directory.as_raw_fd(),
            &private_name,
            &placeholder_name,
            placeholder_identity,
            true,
        )
    }

    fn open_parent(
        &self,
        path: &RelativePath,
        create_missing: bool,
    ) -> io::Result<(OwnedFd, CString)> {
        let components = path
            .as_str()
            .split('/')
            .map(|component| CString::new(component).map_err(interior_nul_error))
            .collect::<io::Result<Vec<_>>>()?;
        let (name, parents) = components
            .split_last()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "empty relative path"))?;
        let mut parent = duplicate_fd(self.root.as_raw_fd())?;
        for component in parents {
            if create_missing {
                match mkdir_at(parent.as_raw_fd(), component, 0o700) {
                    Ok(()) => {}
                    Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {}
                    Err(error) => return Err(error),
                }
            }
            parent = open_directory_at(parent.as_raw_fd(), component)?;
        }
        Ok((parent, name.clone()))
    }

    fn open_private_parent(
        &self,
        path: &RelativePath,
        create_missing: bool,
        security_device: Option<u64>,
    ) -> io::Result<(OwnedFd, CString, Vec<Arc<DirectoryBinding>>)> {
        let components = path
            .as_str()
            .split('/')
            .map(|component| CString::new(component).map_err(interior_nul_error))
            .collect::<io::Result<Vec<_>>>()?;
        let (name, parents) = components
            .split_last()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "empty relative path"))?;
        let mut lineage = self.child_lineage()?;
        let mut parent = reopen_directory(self.root.as_raw_fd())?;
        for component in parents {
            if create_missing {
                verify_lineage(&lineage)?;
                match mkdir_at(parent.as_raw_fd(), component, 0o700) {
                    Ok(()) => cvt(unsafe { libc::fsync(parent.as_raw_fd()) })?,
                    Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {}
                    Err(error) => return Err(error),
                }
            }
            let path_stat = stat_at(parent.as_raw_fd(), component)?;
            if let Some(device) = security_device {
                require_private_directory_on_device(&path_stat, device)?;
            } else {
                require_private_directory(&path_stat)?;
            }
            let child = open_directory_at(parent.as_raw_fd(), component)?;
            let opened = stat_fd(child.as_raw_fd())?;
            if let Some(device) = security_device {
                require_private_directory_on_device(&opened, device)?;
            } else {
                require_private_directory(&opened)?;
            }
            if !same_file(&path_stat, &opened) {
                return Err(os_error(libc::ESTALE));
            }
            lineage.push(Arc::new(DirectoryBinding {
                directory: reopen_directory(child.as_raw_fd())?,
                parent: reopen_directory(parent.as_raw_fd())?,
                name: component.clone(),
                identity: FileIdentity::from_stat(&opened),
                owner: opened.st_uid,
                mode: (opened.st_mode & 0o777) as u32,
                security_device,
            }));
            parent = child;
        }
        Ok((parent, name.clone(), lineage))
    }

    fn verify_root_name(&self) -> io::Result<()> {
        for binding in &self.lineage {
            binding.verify()?;
        }
        self.verify_self()
    }

    fn verify_self(&self) -> io::Result<()> {
        let current = stat_at(self.parent.as_raw_fd(), &self.root_name)?;
        if file_type(current.st_mode) == libc::S_IFLNK {
            return Err(os_error(libc::ELOOP));
        }
        if file_type(current.st_mode) != libc::S_IFDIR {
            return Err(os_error(libc::ENOTDIR));
        }
        if FileIdentity::from_stat(&current) != self.root_identity {
            return Err(os_error(libc::ESTALE));
        }
        let opened = stat_fd(self.root.as_raw_fd())?;
        if !same_file(&current, &opened) {
            return Err(os_error(libc::ESTALE));
        }
        if let Some(device) = self.security_device {
            require_private_directory_on_device(&current, device)?;
            require_private_directory_on_device(&opened, device)?;
        }
        Ok(())
    }

    fn child_lineage(&self) -> io::Result<Vec<Arc<DirectoryBinding>>> {
        let mut lineage = clone_lineage(&self.lineage);
        let metadata = stat_fd(self.root.as_raw_fd())?;
        lineage.push(Arc::new(DirectoryBinding {
            directory: reopen_directory(self.root.as_raw_fd())?,
            parent: reopen_directory(self.parent.as_raw_fd())?,
            name: self.root_name.clone(),
            identity: self.root_identity,
            owner: metadata.st_uid,
            mode: (metadata.st_mode & 0o777) as u32,
            security_device: self.security_device,
        }));
        Ok(lineage)
    }
}

impl DirectoryBinding {
    fn verify(&self) -> io::Result<()> {
        let current = stat_at(self.parent.as_raw_fd(), &self.name)?;
        let opened = stat_fd(self.directory.as_raw_fd())?;
        if file_type(current.st_mode) != libc::S_IFDIR
            || FileIdentity::from_stat(&current) != self.identity
            || current.st_uid != self.owner
            || (current.st_mode & 0o777) as u32 != self.mode
            || opened.st_uid != self.owner
            || (opened.st_mode & 0o777) as u32 != self.mode
            || !same_file(&current, &opened)
        {
            return Err(os_error(libc::ESTALE));
        }
        if let Some(device) = self.security_device {
            require_private_directory_on_device(&current, device)?;
            require_private_directory_on_device(&opened, device)?;
        }
        Ok(())
    }
}

fn clone_lineage(lineage: &[Arc<DirectoryBinding>]) -> Vec<Arc<DirectoryBinding>> {
    lineage.to_vec()
}

fn verify_lineage(lineage: &[Arc<DirectoryBinding>]) -> io::Result<()> {
    for binding in lineage {
        binding.verify()?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

impl FileIdentity {
    fn from_stat(metadata: &libc::stat) -> Self {
        Self {
            device: metadata.st_dev as u64,
            inode: metadata.st_ino,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileIdentityRecord {
    device: u64,
    inode: u64,
}

impl From<FileIdentity> for FileIdentityRecord {
    fn from(identity: FileIdentity) -> Self {
        Self {
            device: identity.device,
            inode: identity.inode,
        }
    }
}

impl From<FileIdentityRecord> for FileIdentity {
    fn from(identity: FileIdentityRecord) -> Self {
        Self {
            device: identity.device,
            inode: identity.inode,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CleanupTargetKind {
    Tree,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CleanupIntentV1 {
    version: u8,
    kind: CleanupTargetKind,
    key_sha256: String,
    component_hex: String,
    parent: FileIdentityRecord,
    namespace: FileIdentityRecord,
    target: FileIdentityRecord,
    original_mode: u32,
    quarantine: String,
    operation: String,
    operation_identity: FileIdentityRecord,
    placeholder: String,
    placeholder_identity: FileIdentityRecord,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CleanupDecisionV1 {
    Delete,
    Restore,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CleanupDecisionRecordV1 {
    version: u8,
    key_sha256: String,
    intent: FileIdentityRecord,
    decision: CleanupDecisionV1,
}

struct LoadedCleanupIntent {
    intent: CleanupIntentV1,
    file: File,
    identity: FileIdentity,
}

struct LoadedCleanupDecision {
    record: CleanupDecisionRecordV1,
    file: File,
    identity: FileIdentity,
}

struct CleanupIntentBindings {
    component: CString,
    quarantine: CString,
    operation: CString,
    placeholder: CString,
    parent: FileIdentity,
    target: FileIdentity,
    operation_identity: FileIdentity,
    placeholder_identity: FileIdentity,
    original_mode: u32,
}

#[derive(Clone, PartialEq, Eq)]
struct CleanupBootstrapName {
    key: String,
    target: FileIdentity,
    original_mode: u32,
}

struct PendingTreeCleanupCandidate {
    component: Vec<u8>,
    target: FileIdentity,
    original_mode: u32,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CleanupSlot {
    Missing,
    Target,
    Placeholder,
    Other,
}

#[derive(Clone, Copy)]
struct CleanupSlots {
    public: CleanupSlot,
    quarantine: CleanupSlot,
    operation: CleanupSlot,
    operation_exists: bool,
}

fn split_root_path(path: &Path) -> io::Result<(&Path, CString)> {
    let name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "root path must identify a directory entry",
        )
    })?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let name = CString::new(name.as_bytes()).map_err(interior_nul_error)?;
    Ok((parent, name))
}

fn open_directory_path(path: &Path) -> io::Result<OwnedFd> {
    // macOS exposes these two immutable system aliases as symlinks. Resolve
    // only the fixed alias lexically; every caller-controlled descendant is
    // still walked one component at a time with O_NOFOLLOW.
    #[cfg(target_vendor = "apple")]
    let normalized;
    #[cfg(target_vendor = "apple")]
    let path = if let Ok(suffix) = path.strip_prefix("/var") {
        normalized = PathBuf::from("/private/var").join(suffix);
        normalized.as_path()
    } else if let Ok(suffix) = path.strip_prefix("/tmp") {
        normalized = PathBuf::from("/private/tmp").join(suffix);
        normalized.as_path()
    } else {
        path
    };
    let start = if path.is_absolute() { c"/" } else { c"." };
    // SAFETY: `start` is a static NUL-terminated path. This acquires only the
    // trusted filesystem root or current-directory base and retains no pointer.
    let descriptor = unsafe { libc::open(start.as_ptr(), DIRECTORY_OPEN_FLAGS) };
    let mut current = owned_fd(descriptor)?;
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(component) => {
                let component = CString::new(component.as_bytes()).map_err(interior_nul_error)?;
                current = open_directory_at(current.as_raw_fd(), &component)?;
            }
            Component::ParentDir | Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "physical root path contains unsupported traversal",
                ));
            }
        }
    }
    Ok(current)
}

fn open_or_create_directory_path(path: &Path) -> io::Result<OwnedFd> {
    #[cfg(target_vendor = "apple")]
    let normalized;
    #[cfg(target_vendor = "apple")]
    let path = if let Ok(suffix) = path.strip_prefix("/var") {
        normalized = PathBuf::from("/private/var").join(suffix);
        normalized.as_path()
    } else if let Ok(suffix) = path.strip_prefix("/tmp") {
        normalized = PathBuf::from("/private/tmp").join(suffix);
        normalized.as_path()
    } else {
        path
    };
    let start = if path.is_absolute() { c"/" } else { c"." };
    let descriptor = unsafe { libc::open(start.as_ptr(), DIRECTORY_OPEN_FLAGS) };
    let mut current = owned_fd(descriptor)?;
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(component) => {
                let component = CString::new(component.as_bytes()).map_err(interior_nul_error)?;
                match mkdir_at(current.as_raw_fd(), &component, 0o700) {
                    Ok(()) => cvt(unsafe { libc::fsync(current.as_raw_fd()) })?,
                    Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {}
                    Err(error) => return Err(error),
                }
                current = open_directory_at(current.as_raw_fd(), &component)?;
            }
            Component::ParentDir | Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "physical root path contains unsupported traversal",
                ));
            }
        }
    }
    Ok(current)
}

fn open_directory_at(parent: RawFd, name: &CStr) -> io::Result<OwnedFd> {
    // SAFETY: `name` is NUL-terminated for the duration of `openat`; `parent`
    // is an owned, live directory descriptor at every call site.
    let descriptor = unsafe { libc::openat(parent, name.as_ptr(), DIRECTORY_OPEN_FLAGS) };
    owned_fd(descriptor).or_else(|error| normalize_directory_symlink_error(error, parent, name))
}

fn reopen_directory(descriptor: RawFd) -> io::Result<OwnedFd> {
    open_directory_at(descriptor, c".")
}

fn normalize_directory_symlink_error(
    error: io::Error,
    parent: RawFd,
    name: &CStr,
) -> io::Result<OwnedFd> {
    if matches!(
        error.raw_os_error(),
        Some(libc::ENOTDIR) | Some(libc::ELOOP)
    ) && let Ok(metadata) = stat_at(parent, name)
        && file_type(metadata.st_mode) == libc::S_IFLNK
    {
        return Err(os_error(libc::ELOOP));
    }
    Err(error)
}

fn open_regular_at(parent: RawFd, name: &CStr) -> io::Result<OwnedFd> {
    // SAFETY: `name` and `parent` remain live for this non-retaining call.
    let descriptor = unsafe { libc::openat(parent, name.as_ptr(), REGULAR_OPEN_FLAGS) };
    owned_fd(descriptor)
}

fn open_regular_rw_at(parent: RawFd, name: &CStr) -> io::Result<OwnedFd> {
    let flags = libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;
    let descriptor = unsafe { libc::openat(parent, name.as_ptr(), flags) };
    owned_fd(descriptor)
}

fn create_regular_at(parent: RawFd, name: &CStr) -> io::Result<OwnedFd> {
    let flags = libc::O_WRONLY
        | libc::O_CREAT
        | libc::O_EXCL
        | libc::O_CLOEXEC
        | libc::O_NOFOLLOW
        | libc::O_NONBLOCK;
    // SAFETY: `name` and `parent` remain live for this non-retaining call;
    // the mode argument is required because O_CREAT is present.
    let descriptor = unsafe { libc::openat(parent, name.as_ptr(), flags, 0o600) };
    owned_fd(descriptor)
}

fn duplicate_fd(descriptor: RawFd) -> io::Result<OwnedFd> {
    // SAFETY: `descriptor` is live and fcntl returns a new independently
    // owned descriptor on success.
    let duplicate = unsafe { libc::fcntl(descriptor, libc::F_DUPFD_CLOEXEC, 0) };
    owned_fd(duplicate)
}

fn owned_fd(descriptor: libc::c_int) -> io::Result<OwnedFd> {
    if descriptor == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a nonnegative descriptor returned by open/openat/fcntl is newly
    // owned and is transferred exactly once into OwnedFd.
    Ok(unsafe { OwnedFd::from_raw_fd(descriptor) })
}

fn mkdir_at(parent: RawFd, name: &CStr, mode: libc::mode_t) -> io::Result<()> {
    // SAFETY: `parent` is a live directory descriptor and `name` is a live
    // NUL-terminated component for this call.
    cvt(unsafe { libc::mkdirat(parent, name.as_ptr(), mode) })
}

fn symlink_at(target: &CStr, parent: RawFd, name: &CStr) -> io::Result<()> {
    // SAFETY: both byte strings and the directory descriptor remain live for
    // this non-retaining call.
    cvt(unsafe { libc::symlinkat(target.as_ptr(), parent, name.as_ptr()) })
}

fn unlink_at(parent: RawFd, name: &CStr, flags: libc::c_int) -> io::Result<()> {
    // SAFETY: `parent` and `name` remain live for this non-retaining call.
    cvt(unsafe { libc::unlinkat(parent, name.as_ptr(), flags) })
}

#[cfg(any(
    test,
    not(any(target_vendor = "apple", target_os = "linux", target_os = "android"))
))]
fn link_at(
    source_parent: RawFd,
    source_name: &CStr,
    destination_parent: RawFd,
    destination_name: &CStr,
) -> io::Result<()> {
    // SAFETY: both live directory descriptors and NUL-terminated components
    // remain valid for this non-retaining, no-follow hard-link call.
    cvt(unsafe {
        libc::linkat(
            source_parent,
            source_name.as_ptr(),
            destination_parent,
            destination_name.as_ptr(),
            0,
        )
    })
}

fn stat_at(parent: RawFd, name: &CStr) -> io::Result<libc::stat> {
    let mut metadata = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: the output points to writable, correctly aligned storage;
    // `parent` and `name` are live, and fstatat initializes the output on 0.
    let result = unsafe {
        libc::fstatat(
            parent,
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: the successful fstatat call initialized every stat field.
        Ok(unsafe { metadata.assume_init() })
    }
}

fn stat_fd(descriptor: RawFd) -> io::Result<libc::stat> {
    let mut metadata = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: `descriptor` is live and the output is valid writable storage;
    // fstat initializes the output on success.
    let result = unsafe { libc::fstat(descriptor, metadata.as_mut_ptr()) };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: the successful fstat call initialized every stat field.
        Ok(unsafe { metadata.assume_init() })
    }
}

fn private_leaf_name(name: &str) -> io::Result<CString> {
    if name.is_empty() || name == "." || name == ".." || name.as_bytes().contains(&b'/') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "private file name is not a single component",
        ));
    }
    CString::new(name).map_err(interior_nul_error)
}

fn require_private_regular(metadata: &libc::stat) -> io::Result<()> {
    if file_type(metadata.st_mode) != libc::S_IFREG
        || metadata.st_uid != unsafe { libc::geteuid() }
        || metadata.st_nlink != 1
        || metadata.st_mode & 0o077 != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "host file is not an owner-only regular file",
        ));
    }
    Ok(())
}

fn require_private_regular_on_device(
    metadata: &libc::stat,
    expected_device: u64,
) -> io::Result<()> {
    require_private_regular(metadata)?;
    if metadata.st_dev as u64 != expected_device {
        return Err(os_error(libc::EXDEV));
    }
    Ok(())
}

fn require_private_regular_opened(metadata: &libc::stat) -> io::Result<()> {
    if metadata.st_nlink == 0
        && file_type(metadata.st_mode) == libc::S_IFREG
        && metadata.st_uid == unsafe { libc::geteuid() }
        && metadata.st_mode & 0o077 == 0
    {
        return Err(os_error(libc::ESTALE));
    }
    require_private_regular(metadata)
}

fn require_private_regular_opened_on_device(
    metadata: &libc::stat,
    expected_device: u64,
) -> io::Result<()> {
    require_private_regular_opened(metadata)?;
    if metadata.st_dev as u64 != expected_device {
        return Err(os_error(libc::EXDEV));
    }
    Ok(())
}

fn private_regular_policy_stable(left: &libc::stat, right: &libc::stat) -> bool {
    left.st_dev == right.st_dev
        && left.st_ino == right.st_ino
        && left.st_mode == right.st_mode
        && left.st_uid == right.st_uid
        && left.st_nlink == right.st_nlink
}

fn require_private_directory(metadata: &libc::stat) -> io::Result<()> {
    if file_type(metadata.st_mode) != libc::S_IFDIR
        || metadata.st_uid != unsafe { libc::geteuid() }
        || metadata.st_mode & 0o077 != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "host directory is not owner-only",
        ));
    }
    Ok(())
}

fn require_private_directory_on_device(
    metadata: &libc::stat,
    expected_device: u64,
) -> io::Result<()> {
    require_private_directory(metadata)?;
    if metadata.st_dev as u64 != expected_device {
        return Err(os_error(libc::EXDEV));
    }
    Ok(())
}

fn collect_and_sync_tree(
    directory: RawFd,
    prefix: &str,
    actual: &mut BTreeSet<RelativePath>,
) -> io::Result<()> {
    for name in directory_entries(directory)? {
        let name_text = std::str::from_utf8(name.to_bytes()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "staged entry name is not UTF-8")
        })?;
        let path = if prefix.is_empty() {
            name_text.to_owned()
        } else {
            format!("{prefix}/{name_text}")
        };
        let relative = RelativePath::parse(path.as_bytes())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        let metadata = stat_at(directory, &name)?;
        match file_type(metadata.st_mode) {
            libc::S_IFDIR => {
                let child = open_verified_child_directory(directory, &name, &metadata)?;
                actual.insert(relative);
                collect_and_sync_tree(child.as_raw_fd(), &path, actual)?;
                cvt(unsafe { libc::fsync(child.as_raw_fd()) })?;
            }
            libc::S_IFREG => {
                let file = open_regular_at(directory, &name)?;
                let opened = stat_fd(file.as_raw_fd())?;
                if !same_file(&metadata, &opened) {
                    return Err(os_error(libc::ESTALE));
                }
                cvt(unsafe { libc::fsync(file.as_raw_fd()) })?;
                actual.insert(relative);
            }
            libc::S_IFLNK => {
                actual.insert(relative);
            }
            _ => return Err(invalid_type_error()),
        }
    }
    Ok(())
}

fn inspect_snapshot_directory(
    directory: RawFd,
    prefix: &str,
    expected_device: u64,
    projection: SnapshotProjection,
    identities: &mut BTreeSet<(u64, u64)>,
    entries: &mut Vec<SnapshotTreeEntry>,
) -> io::Result<()> {
    for name in directory_entries(directory)? {
        let name_text = std::str::from_utf8(name.to_bytes()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot entry name is not valid UTF-8",
            )
        })?;
        let path = if prefix.is_empty() {
            name_text.to_owned()
        } else {
            format!("{prefix}/{name_text}")
        };
        let relative = RelativePath::parse(path.as_bytes()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "snapshot entry path is unsafe")
        })?;
        let before = stat_at(directory, &name)?;
        require_snapshot_entry(&before, expected_device)?;
        if !identities.insert((before.st_dev as u64, before.st_ino)) {
            return Err(snapshot_policy_error());
        }
        match file_type(before.st_mode) {
            libc::S_IFDIR => {
                require_snapshot_directory(&before, expected_device, projection)?;
                let child = open_directory_at(directory, &name)?;
                let opened = stat_fd(child.as_raw_fd())?;
                require_snapshot_directory(&opened, expected_device, projection)?;
                if !snapshot_metadata_stable(&before, &opened) {
                    return Err(os_error(libc::ESTALE));
                }
                entries.push(SnapshotTreeEntry {
                    path: relative,
                    kind: SnapshotFsKind::Directory,
                    mode: (before.st_mode & 0o7777) as u32,
                    size: 0,
                    device: before.st_dev as u64,
                    inode: before.st_ino,
                    sha256: None,
                    symlink_target: None,
                });
                inspect_snapshot_directory(
                    child.as_raw_fd(),
                    &path,
                    expected_device,
                    projection,
                    identities,
                    entries,
                )?;
                let after = stat_fd(child.as_raw_fd())?;
                let rebound = stat_at(directory, &name)?;
                if !snapshot_metadata_stable(&before, &after)
                    || !snapshot_metadata_stable(&before, &rebound)
                {
                    return Err(os_error(libc::ESTALE));
                }
            }
            libc::S_IFREG => {
                require_snapshot_regular(&before, expected_device, projection)?;
                let descriptor = open_regular_at(directory, &name)?;
                let opened = stat_fd(descriptor.as_raw_fd())?;
                require_snapshot_regular(&opened, expected_device, projection)?;
                if !snapshot_metadata_stable(&before, &opened) {
                    return Err(os_error(libc::ESTALE));
                }
                let mut file = File::from(descriptor);
                let mut hasher = Sha256::new();
                let mut buffer = [0_u8; 64 * 1024];
                let mut bytes_read = 0_u64;
                loop {
                    let count = file.read(&mut buffer)?;
                    if count == 0 {
                        break;
                    }
                    bytes_read = bytes_read
                        .checked_add(count as u64)
                        .ok_or_else(snapshot_policy_error)?;
                    hasher.update(&buffer[..count]);
                }
                let after = stat_fd(file.as_raw_fd())?;
                let rebound = stat_at(directory, &name)?;
                if before.st_size < 0
                    || bytes_read != before.st_size as u64
                    || !snapshot_metadata_stable(&before, &after)
                    || !snapshot_metadata_stable(&before, &rebound)
                {
                    return Err(os_error(libc::ESTALE));
                }
                entries.push(SnapshotTreeEntry {
                    path: relative,
                    kind: SnapshotFsKind::RegularFile,
                    mode: (before.st_mode & 0o7777) as u32,
                    size: bytes_read,
                    device: before.st_dev as u64,
                    inode: before.st_ino,
                    sha256: Some(format!("{:x}", hasher.finalize())),
                    symlink_target: None,
                });
            }
            libc::S_IFLNK => {
                require_snapshot_symlink(&before, expected_device)?;
                let target = read_link_at(directory, &name, before.st_size)?;
                let after = stat_at(directory, &name)?;
                if !snapshot_metadata_stable(&before, &after)
                    || before.st_size < 0
                    || target.len() as u64 != before.st_size as u64
                {
                    return Err(os_error(libc::ESTALE));
                }
                let target = String::from_utf8(target).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "snapshot symlink target is not valid UTF-8",
                    )
                })?;
                let mut hasher = Sha256::new();
                hasher.update(b"symlink\0");
                hasher.update(target.as_bytes());
                entries.push(SnapshotTreeEntry {
                    path: relative,
                    kind: SnapshotFsKind::Symlink,
                    mode: (before.st_mode & 0o7777) as u32,
                    size: target.len() as u64,
                    device: before.st_dev as u64,
                    inode: before.st_ino,
                    sha256: Some(format!("{:x}", hasher.finalize())),
                    symlink_target: Some(target),
                });
            }
            _ => return Err(invalid_type_error()),
        }
    }
    Ok(())
}

fn require_snapshot_entry(metadata: &libc::stat, expected_device: u64) -> io::Result<()> {
    if metadata.st_uid != unsafe { libc::geteuid() } || metadata.st_dev as u64 != expected_device {
        return Err(snapshot_policy_error());
    }
    Ok(())
}

fn require_snapshot_directory(
    metadata: &libc::stat,
    expected_device: u64,
    projection: SnapshotProjection,
) -> io::Result<()> {
    require_snapshot_entry(metadata, expected_device)?;
    let mode = (metadata.st_mode & 0o7777) as u32;
    let valid_mode = match projection {
        SnapshotProjection::TransportOrOwner => matches!(mode, 0o555 | 0o500),
        SnapshotProjection::OwnerOnly => mode == 0o500,
        SnapshotProjection::Workspace => mode == 0o700,
    };
    if file_type(metadata.st_mode) != libc::S_IFDIR || !valid_mode {
        return Err(snapshot_policy_error());
    }
    Ok(())
}

fn require_snapshot_regular(
    metadata: &libc::stat,
    expected_device: u64,
    projection: SnapshotProjection,
) -> io::Result<()> {
    require_snapshot_entry(metadata, expected_device)?;
    let mode = (metadata.st_mode & 0o7777) as u32;
    let valid_mode = match projection {
        SnapshotProjection::TransportOrOwner => {
            matches!(mode, 0o444 | 0o555 | 0o400 | 0o500)
        }
        SnapshotProjection::OwnerOnly => matches!(mode, 0o400 | 0o500),
        SnapshotProjection::Workspace => matches!(mode, 0o600 | 0o700),
    };
    if file_type(metadata.st_mode) != libc::S_IFREG
        || metadata.st_nlink != 1
        || metadata.st_size < 0
        || !valid_mode
    {
        return Err(snapshot_policy_error());
    }
    Ok(())
}

fn require_snapshot_symlink(metadata: &libc::stat, expected_device: u64) -> io::Result<()> {
    require_snapshot_entry(metadata, expected_device)?;
    if file_type(metadata.st_mode) != libc::S_IFLNK
        || metadata.st_nlink != 1
        || metadata.st_size < 0
        || !snapshot_symlink_mode_valid((metadata.st_mode & 0o7777) as u32)
    {
        return Err(snapshot_policy_error());
    }
    Ok(())
}

fn snapshot_symlink_mode_valid(mode: u32) -> bool {
    #[cfg(target_vendor = "apple")]
    {
        // Darwin applies the process umask when creating symlinks and offers
        // no descriptor-relative, no-follow chmod primitive. Both modes are
        // emitted by the local builder/rsync path and are metadata-only.
        matches!(mode, 0o755 | 0o777)
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        mode == 0o777
    }
}

fn snapshot_metadata_stable(left: &libc::stat, right: &libc::stat) -> bool {
    left.st_dev == right.st_dev
        && left.st_ino == right.st_ino
        && left.st_mode == right.st_mode
        && left.st_nlink == right.st_nlink
        && left.st_uid == right.st_uid
        && left.st_gid == right.st_gid
        && left.st_size == right.st_size
        && left.st_mtime == right.st_mtime
        && left.st_mtime_nsec == right.st_mtime_nsec
        && left.st_ctime == right.st_ctime
        && left.st_ctime_nsec == right.st_ctime_nsec
}

fn make_snapshot_directory_owner_only(
    directory: RawFd,
    expected_device: u64,
    after_conversion: &mut impl FnMut() -> io::Result<()>,
) -> io::Result<()> {
    for name in directory_entries(directory)? {
        let before = stat_at(directory, &name)?;
        require_snapshot_entry(&before, expected_device)?;
        match file_type(before.st_mode) {
            libc::S_IFDIR => {
                require_snapshot_directory(
                    &before,
                    expected_device,
                    SnapshotProjection::TransportOrOwner,
                )?;
                let child = open_directory_at(directory, &name)?;
                let opened = stat_fd(child.as_raw_fd())?;
                require_snapshot_directory(
                    &opened,
                    expected_device,
                    SnapshotProjection::TransportOrOwner,
                )?;
                if !snapshot_metadata_stable(&before, &opened) {
                    return Err(os_error(libc::ESTALE));
                }
                make_snapshot_directory_owner_only(
                    child.as_raw_fd(),
                    expected_device,
                    after_conversion,
                )?;
                chmod_fd(child.as_raw_fd(), 0o500)?;
                cvt(unsafe { libc::fsync(child.as_raw_fd()) })?;
                let after = stat_fd(child.as_raw_fd())?;
                let rebound = stat_at(directory, &name)?;
                require_snapshot_directory(&after, expected_device, SnapshotProjection::OwnerOnly)?;
                require_snapshot_directory(
                    &rebound,
                    expected_device,
                    SnapshotProjection::OwnerOnly,
                )?;
                if !same_file(&before, &after) || !same_file(&before, &rebound) {
                    return Err(os_error(libc::ESTALE));
                }
                after_conversion()?;
            }
            libc::S_IFREG => {
                require_snapshot_regular(
                    &before,
                    expected_device,
                    SnapshotProjection::TransportOrOwner,
                )?;
                let descriptor = open_regular_at(directory, &name)?;
                let opened = stat_fd(descriptor.as_raw_fd())?;
                require_snapshot_regular(
                    &opened,
                    expected_device,
                    SnapshotProjection::TransportOrOwner,
                )?;
                if !snapshot_metadata_stable(&before, &opened) {
                    return Err(os_error(libc::ESTALE));
                }
                let mode = if opened.st_mode & 0o111 != 0 {
                    0o500
                } else {
                    0o400
                };
                chmod_fd(descriptor.as_raw_fd(), mode)?;
                cvt(unsafe { libc::fsync(descriptor.as_raw_fd()) })?;
                let after = stat_fd(descriptor.as_raw_fd())?;
                let rebound = stat_at(directory, &name)?;
                require_snapshot_regular(&after, expected_device, SnapshotProjection::OwnerOnly)?;
                require_snapshot_regular(&rebound, expected_device, SnapshotProjection::OwnerOnly)?;
                if !same_file(&before, &after) || !same_file(&before, &rebound) {
                    return Err(os_error(libc::ESTALE));
                }
                after_conversion()?;
            }
            libc::S_IFLNK => require_snapshot_symlink(&before, expected_device)?,
            _ => return Err(invalid_type_error()),
        }
    }
    Ok(())
}

fn snapshot_policy_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "snapshot entry violates the read-only descriptor policy",
    )
}

fn read_link_at(parent: RawFd, name: &CStr, reported_size: libc::off_t) -> io::Result<Vec<u8>> {
    let mut capacity = usize::try_from(reported_size)
        .unwrap_or(0)
        .saturating_add(1)
        .clamp(256, MAX_SYMLINK_TARGET);
    loop {
        let mut target = vec![0_u8; capacity];
        // SAFETY: `target` exposes `capacity` writable bytes, while `parent`
        // and `name` remain live for this non-retaining call.
        let length = unsafe {
            libc::readlinkat(
                parent,
                name.as_ptr(),
                target.as_mut_ptr().cast(),
                target.len(),
            )
        };
        if length == -1 {
            return Err(io::Error::last_os_error());
        }
        let length = length as usize;
        if length < capacity {
            target.truncate(length);
            return Ok(target);
        }
        if capacity == MAX_SYMLINK_TARGET {
            return Err(os_error(libc::ENAMETOOLONG));
        }
        capacity = capacity.saturating_mul(2).min(MAX_SYMLINK_TARGET);
    }
}

#[cfg(all(test, target_os = "macos"))]
fn copy_regular(
    source: OwnedFd,
    destination_parent: &OwnedFd,
    destination_name: &CStr,
    mode: libc::mode_t,
) -> io::Result<()> {
    let namespace = namespace_for_destination_parent(destination_parent)?;
    copy_regular_in_namespace(
        source,
        &namespace,
        destination_parent,
        destination_name,
        mode,
    )
}

#[cfg(target_os = "macos")]
fn copy_regular_in_namespace(
    source: OwnedFd,
    namespace: &PrivateNamespace,
    destination_parent: &OwnedFd,
    destination_name: &CStr,
    mode: libc::mode_t,
) -> io::Result<()> {
    copy_regular_with_clone_and_publish_in_namespace(
        source,
        namespace,
        destination_parent,
        destination_name,
        mode,
        |source, temporary_parent, temporary_name| {
            // SAFETY: the verified source descriptor, owned temporary
            // directory, and component remain live for this non-retaining call.
            cvt(unsafe { libc::fclonefileat(source, temporary_parent, temporary_name.as_ptr(), 0) })
        },
        publish_regular_no_replace,
    )
}

#[cfg(test)]
fn copy_regular_with_clone(
    source: OwnedFd,
    destination_parent: &OwnedFd,
    destination_name: &CStr,
    mode: libc::mode_t,
    clone_attempt: impl FnOnce(RawFd, RawFd, &CStr) -> io::Result<()>,
) -> io::Result<()> {
    let namespace = namespace_for_destination_parent(destination_parent)?;
    copy_regular_with_clone_and_publish_in_namespace(
        source,
        &namespace,
        destination_parent,
        destination_name,
        mode,
        clone_attempt,
        publish_regular_no_replace,
    )
}

#[cfg(test)]
fn copy_regular_with_clone_and_publish(
    source: OwnedFd,
    destination_parent: &OwnedFd,
    destination_name: &CStr,
    mode: libc::mode_t,
    clone_attempt: impl FnOnce(RawFd, RawFd, &CStr) -> io::Result<()>,
    publish: impl FnOnce(RawFd, &CStr, RawFd, &CStr) -> io::Result<()>,
) -> io::Result<()> {
    let namespace = namespace_for_destination_parent(destination_parent)?;
    copy_regular_with_clone_and_publish_in_namespace(
        source,
        &namespace,
        destination_parent,
        destination_name,
        mode,
        clone_attempt,
        publish,
    )
}

fn copy_regular_with_clone_and_publish_in_namespace(
    source: OwnedFd,
    namespace: &PrivateNamespace,
    destination_parent: &OwnedFd,
    destination_name: &CStr,
    mode: libc::mode_t,
    clone_attempt: impl FnOnce(RawFd, RawFd, &CStr) -> io::Result<()>,
    publish: impl FnOnce(RawFd, &CStr, RawFd, &CStr) -> io::Result<()>,
) -> io::Result<()> {
    let mut operation = PrivateOperation::create(namespace)?;
    let temporary_name = random_private_name("data");
    let clone_result = clone_attempt(
        source.as_raw_fd(),
        operation.directory.as_raw_fd(),
        &temporary_name,
    );
    complete_clone_or_copy(
        clone_result,
        source,
        operation.directory.as_raw_fd(),
        &temporary_name,
        mode,
    )?;
    let path_stat = stat_at(operation.directory.as_raw_fd(), &temporary_name)?;
    let descriptor = open_regular_at(operation.directory.as_raw_fd(), &temporary_name)?;
    let opened = stat_fd(descriptor.as_raw_fd())?;
    if file_type(path_stat.st_mode) != libc::S_IFREG
        || file_type(opened.st_mode) != libc::S_IFREG
        || !same_file(&path_stat, &opened)
    {
        return Err(os_error(libc::ESTALE));
    }
    let commit_name = random_private_name("commit");
    rename_no_replace(
        operation.directory.as_raw_fd(),
        &temporary_name,
        namespace.directory.as_raw_fd(),
        &commit_name,
    )?;
    let mut commit = PrivateCommit::new(namespace, commit_name);
    inject_copy_private_cleanup_failure(operation.directory.as_raw_fd())?;
    operation.remove_empty_owned()?;
    operation.cleaned = true;
    publish(
        namespace.directory.as_raw_fd(),
        &commit.name,
        destination_parent.as_raw_fd(),
        destination_name,
    )?;
    commit.published = true;
    Ok(())
}

#[cfg(all(test, not(target_os = "macos")))]
fn copy_regular(
    source: OwnedFd,
    destination_parent: &OwnedFd,
    destination_name: &CStr,
    mode: libc::mode_t,
) -> io::Result<()> {
    let namespace = namespace_for_destination_parent(destination_parent)?;
    copy_regular_in_namespace(
        source,
        &namespace,
        destination_parent,
        destination_name,
        mode,
    )
}

#[cfg(not(target_os = "macos"))]
fn copy_regular_in_namespace(
    source: OwnedFd,
    namespace: &PrivateNamespace,
    destination_parent: &OwnedFd,
    destination_name: &CStr,
    mode: libc::mode_t,
) -> io::Result<()> {
    copy_regular_with_clone_and_publish_in_namespace(
        source,
        namespace,
        destination_parent,
        destination_name,
        mode,
        |_source, _temporary_parent, _temporary_name| Err(os_error(libc::ENOTSUP)),
        publish_regular_no_replace,
    )
}

#[cfg(test)]
fn namespace_for_destination_parent(destination_parent: &OwnedFd) -> io::Result<PrivateNamespace> {
    let device = stat_fd(destination_parent.as_raw_fd())?.st_dev;
    let sibling_parent = open_directory_at(destination_parent.as_raw_fd(), c"..")?;
    PrivateNamespace::select(sibling_parent.as_raw_fd(), device, &[])
}

fn complete_clone_or_copy(
    clone_result: io::Result<()>,
    source: OwnedFd,
    temporary_parent: RawFd,
    temporary_name: &CStr,
    mode: libc::mode_t,
) -> io::Result<()> {
    let error = match clone_result {
        Ok(()) => return finish_created_regular(temporary_parent, temporary_name, mode),
        Err(error) => error,
    };
    if !matches!(
        error.raw_os_error(),
        Some(libc::ENOTSUP) | Some(libc::EXDEV) | Some(libc::EINVAL)
    ) {
        return Err(error);
    }
    remove_failed_clone_destination(temporary_parent, temporary_name)?;
    copy_regular_bytes(source, temporary_parent, temporary_name, mode)
}

fn remove_failed_clone_destination(parent: RawFd, name: &CStr) -> io::Result<()> {
    let metadata = match stat_at(parent, name) {
        Ok(metadata) => metadata,
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => return Ok(()),
        Err(error) => return Err(error),
    };
    if file_type(metadata.st_mode) != libc::S_IFREG {
        return Err(os_error(libc::EEXIST));
    }
    let descriptor = open_regular_at(parent, name)?;
    let opened = stat_fd(descriptor.as_raw_fd())?;
    if file_type(opened.st_mode) != libc::S_IFREG || !same_file(&metadata, &opened) {
        return Err(os_error(libc::ESTALE));
    }
    unlink_at(parent, name, 0)
}

#[derive(Clone, Copy)]
struct DirectoryIdentity {
    descriptor: RawFd,
    identity: FileIdentity,
}

// Reusable infrastructure, not per-operation residue. The directory is
// selected beside (or above) the target root on that root's filesystem,
// validated as euid-owned mode 0700, and kept empty between successful calls.
// Every entry beneath it is cryptographically random and remains module-private.
struct PrivateNamespace {
    directory: OwnedFd,
    identity: FileIdentity,
    created: bool,
}

impl PrivateNamespace {
    fn select(
        initial_parent: RawFd,
        expected_device: libc::dev_t,
        disallowed_roots: &[DirectoryIdentity],
    ) -> io::Result<Self> {
        let mut parent = duplicate_fd(initial_parent)?;
        let mut last_error = os_error(libc::ENOTSUP);
        for _ in 0..256 {
            let parent_metadata = stat_fd(parent.as_raw_fd())?;
            if parent_metadata.st_dev != expected_device {
                return Err(os_error(libc::EXDEV));
            }
            let mut parent_is_inside_root = false;
            for root in disallowed_roots {
                if identity_is_in_ancestry(root.identity, parent.as_raw_fd())? {
                    parent_is_inside_root = true;
                    break;
                }
            }
            if !parent_is_inside_root {
                match Self::open_or_create_at(&parent, expected_device) {
                    Ok(namespace) => {
                        if namespace.is_disjoint_from(disallowed_roots)? {
                            namespace.probe_atomic_rename()?;
                            return Ok(namespace);
                        }
                        namespace.remove_if_new_and_empty(parent.as_raw_fd());
                    }
                    Err(error) => last_error = error,
                }
            }
            let parent_identity = FileIdentity::from_stat(&parent_metadata);
            let next = open_directory_at(parent.as_raw_fd(), c"..")?;
            let next_metadata = stat_fd(next.as_raw_fd())?;
            let next_identity = FileIdentity::from_stat(&next_metadata);
            if next_identity == parent_identity || next_metadata.st_dev != expected_device {
                break;
            }
            parent = next;
        }
        if last_error.raw_os_error() == Some(libc::EXDEV) {
            Err(last_error)
        } else {
            Err(os_error(libc::ENOTSUP))
        }
    }

    fn open_or_create_at(parent: &OwnedFd, expected_device: libc::dev_t) -> io::Result<Self> {
        let parent_metadata = stat_fd(parent.as_raw_fd())?;
        if parent_metadata.st_dev != expected_device {
            return Err(os_error(libc::EXDEV));
        }
        let sticky = parent_metadata.st_mode & libc::S_ISVTX as libc::mode_t != 0;
        let private_to_user =
            parent_metadata.st_uid == effective_user_id() && parent_metadata.st_mode & 0o022 == 0;
        if file_type(parent_metadata.st_mode) != libc::S_IFDIR || (!sticky && !private_to_user) {
            return Err(os_error(libc::ENOTSUP));
        }
        let created = match mkdir_at(parent.as_raw_fd(), PRIVATE_NAMESPACE_NAME, 0o700) {
            Ok(()) => true,
            Err(error) if error.raw_os_error() == Some(libc::EEXIST) => false,
            Err(error) => return Err(error),
        };
        let result = (|| {
            let initial = stat_at(parent.as_raw_fd(), PRIVATE_NAMESPACE_NAME)?;
            let directory = open_directory_at(parent.as_raw_fd(), PRIVATE_NAMESPACE_NAME)?;
            let opened = stat_fd(directory.as_raw_fd())?;
            if file_type(initial.st_mode) != libc::S_IFDIR
                || !same_file(&initial, &opened)
                || opened.st_dev != expected_device
                || opened.st_uid != effective_user_id()
                || opened.st_mode & 0o777 != 0o700
            {
                return Err(os_error(libc::ESTALE));
            }
            Ok(Self {
                directory,
                identity: FileIdentity::from_stat(&opened),
                created,
            })
        })();
        if result.is_err() && created {
            let _ = unlink_at(
                parent.as_raw_fd(),
                PRIVATE_NAMESPACE_NAME,
                libc::AT_REMOVEDIR,
            );
        }
        result
    }

    fn is_disjoint_from(&self, roots: &[DirectoryIdentity]) -> io::Result<bool> {
        for root in roots {
            if identity_is_in_ancestry(root.identity, self.directory.as_raw_fd())?
                || identity_is_in_ancestry(self.identity, root.descriptor)?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn remove_if_new_and_empty(&self, parent: RawFd) {
        if self.created && directory_entries(self.directory.as_raw_fd()).is_ok_and(|v| v.is_empty())
        {
            let _ = unlink_at(parent, PRIVATE_NAMESPACE_NAME, libc::AT_REMOVEDIR);
        }
    }

    fn probe_atomic_rename(&self) -> io::Result<()> {
        let mut operation = PrivateOperation::create(self)?;
        probe_cross_directory_no_replace(self, &operation, false)?;
        probe_cross_directory_no_replace(self, &operation, true)?;
        probe_cross_directory_exchange(self, &operation, false)?;
        probe_cross_directory_exchange(self, &operation, true)?;
        operation.remove_empty_owned()?;
        operation.cleaned = true;
        Ok(())
    }
}

fn probe_cross_directory_no_replace(
    namespace: &PrivateNamespace,
    operation: &PrivateOperation<'_>,
    directory: bool,
) -> io::Result<()> {
    let kind = if directory {
        "probe-directory"
    } else {
        "probe-file"
    };
    let source_name = random_private_name("probe-source");
    let destination_name = random_private_name("probe-destination");
    let flags = if directory { libc::AT_REMOVEDIR } else { 0 };
    let create = |parent: RawFd, name: &CStr| {
        if directory {
            mkdir_at(parent, name, 0o700)
        } else {
            drop(create_regular_at(parent, name)?);
            Ok(())
        }
    };
    create(operation.directory.as_raw_fd(), &source_name)?;
    if let Err(error) = create(namespace.directory.as_raw_fd(), &destination_name) {
        let _ = unlink_at(operation.directory.as_raw_fd(), &source_name, flags);
        return Err(error);
    }
    let no_replace_result = rename_no_replace(
        operation.directory.as_raw_fd(),
        &source_name,
        namespace.directory.as_raw_fd(),
        &destination_name,
    );
    match no_replace_result {
        Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {}
        Err(error) => {
            return cleanup_probe_entries(
                operation.directory.as_raw_fd(),
                &source_name,
                namespace.directory.as_raw_fd(),
                &destination_name,
                flags,
            )
            .and(Err(error));
        }
        Ok(()) => {
            let cleanup = cleanup_probe_entries(
                operation.directory.as_raw_fd(),
                &source_name,
                namespace.directory.as_raw_fd(),
                &destination_name,
                flags,
            );
            return cleanup.and(Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("atomic no-replace rename replaced an existing {kind}"),
            )));
        }
    }
    unlink_at(namespace.directory.as_raw_fd(), &destination_name, flags)?;
    if let Err(error) = rename_no_replace(
        operation.directory.as_raw_fd(),
        &source_name,
        namespace.directory.as_raw_fd(),
        &destination_name,
    ) {
        return cleanup_probe_entries(
            operation.directory.as_raw_fd(),
            &source_name,
            namespace.directory.as_raw_fd(),
            &destination_name,
            flags,
        )
        .and(Err(error));
    }
    unlink_at(namespace.directory.as_raw_fd(), &destination_name, flags)
}

fn cleanup_probe_entries(
    source_parent: RawFd,
    source_name: &CStr,
    destination_parent: RawFd,
    destination_name: &CStr,
    flags: libc::c_int,
) -> io::Result<()> {
    let source = unlink_if_exists(source_parent, source_name, flags);
    let destination = unlink_if_exists(destination_parent, destination_name, flags);
    source.and(destination)
}

fn probe_cross_directory_exchange(
    namespace: &PrivateNamespace,
    operation: &PrivateOperation<'_>,
    directory: bool,
) -> io::Result<()> {
    let left = random_private_name("probe-exchange-left");
    let right = random_private_name("probe-exchange-right");
    let flags = if directory { libc::AT_REMOVEDIR } else { 0 };
    let create = |parent: RawFd, name: &CStr| {
        if directory {
            mkdir_at(parent, name, 0o700)
        } else {
            drop(create_regular_at(parent, name)?);
            Ok(())
        }
    };
    create(operation.directory.as_raw_fd(), &left)?;
    if let Err(error) = create(namespace.directory.as_raw_fd(), &right) {
        let _ = unlink_at(operation.directory.as_raw_fd(), &left, flags);
        return Err(error);
    }
    if let Err(error) = exchange_entries(
        operation.directory.as_raw_fd(),
        &left,
        namespace.directory.as_raw_fd(),
        &right,
    ) {
        return cleanup_probe_entries(
            operation.directory.as_raw_fd(),
            &left,
            namespace.directory.as_raw_fd(),
            &right,
            flags,
        )
        .and(Err(error));
    }
    exchange_entries(
        operation.directory.as_raw_fd(),
        &left,
        namespace.directory.as_raw_fd(),
        &right,
    )?;
    cleanup_probe_entries(
        operation.directory.as_raw_fd(),
        &left,
        namespace.directory.as_raw_fd(),
        &right,
        flags,
    )
}

fn unlink_if_exists(parent: RawFd, name: &CStr, flags: libc::c_int) -> io::Result<()> {
    match unlink_at(parent, name, flags) {
        Ok(()) => Ok(()),
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => Ok(()),
        Err(error) => Err(error),
    }
}

struct PrivateOperation<'a> {
    namespace: &'a PrivateNamespace,
    name: CString,
    directory: OwnedFd,
    identity: FileIdentity,
    cleaned: bool,
}

impl<'a> PrivateOperation<'a> {
    fn create(namespace: &'a PrivateNamespace) -> io::Result<Self> {
        let expected_device = stat_fd(namespace.directory.as_raw_fd())?.st_dev;
        for _ in 0..16 {
            let name = random_private_name("operation");
            match mkdir_at(namespace.directory.as_raw_fd(), &name, 0o700) {
                Ok(()) => {}
                Err(error) if error.raw_os_error() == Some(libc::EEXIST) => continue,
                Err(error) => return Err(error),
            }
            let initial = match stat_at(namespace.directory.as_raw_fd(), &name) {
                Ok(initial) => initial,
                Err(error) => {
                    let _ = unlink_at(namespace.directory.as_raw_fd(), &name, libc::AT_REMOVEDIR);
                    return Err(error);
                }
            };
            let directory = match open_directory_at(namespace.directory.as_raw_fd(), &name) {
                Ok(directory) => directory,
                Err(error) => {
                    let _ = unlink_at(namespace.directory.as_raw_fd(), &name, libc::AT_REMOVEDIR);
                    return Err(error);
                }
            };
            let opened = stat_fd(directory.as_raw_fd())?;
            if file_type(initial.st_mode) != libc::S_IFDIR
                || !same_file(&initial, &opened)
                || opened.st_dev != expected_device
                || opened.st_uid != effective_user_id()
                || opened.st_mode & 0o077 != 0
            {
                return Err(os_error(libc::ESTALE));
            }
            // The live lock lets cleanup-evidence validation distinguish an
            // in-flight module-private operation from abandoned legacy
            // residue without ever adopting or deleting the latter.
            cvt(unsafe { libc::flock(directory.as_raw_fd(), libc::LOCK_EX) })?;
            return Ok(Self {
                namespace,
                name,
                directory,
                identity: FileIdentity::from_stat(&opened),
                cleaned: false,
            });
        }
        Err(os_error(libc::EEXIST))
    }

    fn open_bound(
        namespace: &'a PrivateNamespace,
        name: &CStr,
        expected: FileIdentity,
    ) -> io::Result<Option<Self>> {
        if parse_cleanup_bootstrap_name(name)?.is_none() {
            return Err(cleanup_record_error());
        }
        let initial = match stat_at(namespace.directory.as_raw_fd(), name) {
            Ok(initial) => initial,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        require_private_directory_on_device(&initial, namespace.identity.device)?;
        if initial.st_mode & 0o777 != 0o700 {
            return Err(os_error(libc::ESTALE));
        }
        let directory = open_directory_at(namespace.directory.as_raw_fd(), name)?;
        let opened = stat_fd(directory.as_raw_fd())?;
        require_private_directory_on_device(&opened, namespace.identity.device)?;
        if opened.st_mode & 0o777 != 0o700
            || !same_file(&initial, &opened)
            || FileIdentity::from_stat(&opened) != expected
        {
            return Err(os_error(libc::ESTALE));
        }
        Ok(Some(Self {
            namespace,
            name: name.to_owned(),
            directory,
            identity: expected,
            // A retry never recursively cleans an operation in Drop. Every
            // entry is classified and consumed by an identity-bound step.
            cleaned: true,
        }))
    }

    fn cleanup(&self) -> io::Result<()> {
        remove_private_directory_contents(self.directory.as_raw_fd())?;
        self.remove_empty_owned()
    }

    fn remove_empty_owned(&self) -> io::Result<()> {
        if !directory_entries(self.directory.as_raw_fd())?.is_empty() {
            return Err(os_error(libc::ENOTEMPTY));
        }
        let opened = stat_fd(self.directory.as_raw_fd())?;
        let current = stat_at(self.namespace.directory.as_raw_fd(), &self.name)?;
        if file_type(opened.st_mode) != libc::S_IFDIR
            || FileIdentity::from_stat(&opened) != self.identity
            || !same_file(&opened, &current)
        {
            return Err(os_error(libc::ESTALE));
        }
        unlink_at(
            self.namespace.directory.as_raw_fd(),
            &self.name,
            libc::AT_REMOVEDIR,
        )
    }
}

impl Drop for PrivateOperation<'_> {
    fn drop(&mut self) {
        if !self.cleaned {
            let _ = self.cleanup();
        }
    }
}

fn create_exchange_placeholder(
    operation: &PrivateOperation<'_>,
    directory: bool,
) -> io::Result<(CString, FileIdentity)> {
    let name = random_private_name("exchange-placeholder");
    let descriptor = if directory {
        mkdir_at(operation.directory.as_raw_fd(), &name, 0o700)?;
        open_directory_at(operation.directory.as_raw_fd(), &name)?
    } else {
        create_regular_at(operation.directory.as_raw_fd(), &name)?
    };
    let metadata = stat_fd(descriptor.as_raw_fd())?;
    Ok((name, FileIdentity::from_stat(&metadata)))
}

fn capture_expected_entry(
    operation: &mut PrivateOperation<'_>,
    source_parent: RawFd,
    source_name: &CStr,
    private_name: &CStr,
    placeholder: FileIdentity,
    expected: FileIdentity,
    expected_directory: bool,
) -> io::Result<()> {
    exchange_entries(
        source_parent,
        source_name,
        operation.directory.as_raw_fd(),
        private_name,
    )?;
    let displaced = stat_at(operation.directory.as_raw_fd(), private_name);
    let installed = stat_at(source_parent, source_name);
    let valid = displaced.as_ref().is_ok_and(|metadata| {
        file_type(metadata.st_mode)
            == if expected_directory {
                libc::S_IFDIR
            } else {
                libc::S_IFREG
            }
            && FileIdentity::from_stat(metadata) == expected
    }) && installed
        .as_ref()
        .is_ok_and(|metadata| FileIdentity::from_stat(metadata) == placeholder);
    if valid {
        if let Err(error) = cvt(unsafe { libc::fsync(source_parent) })
            .and_then(|()| cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) }))
        {
            // The exact owned entry has already crossed into the private
            // operation. Preserve it there when durability is uncertain.
            operation.cleaned = true;
            return Err(error);
        }
        return Ok(());
    }
    if let Err(error) = exchange_entries(
        source_parent,
        source_name,
        operation.directory.as_raw_fd(),
        private_name,
    ) {
        // The operation may now hold an unrelated displaced entry. Preserve
        // it as durable recovery evidence rather than running Drop cleanup.
        operation.cleaned = true;
        return Err(error);
    }
    cvt(unsafe { libc::fsync(source_parent) })?;
    cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
    Err(os_error(libc::ESTALE))
}

fn remove_installed_placeholder(
    operation: &mut PrivateOperation<'_>,
    source_parent: RawFd,
    source_name: &CStr,
    private_name: &CStr,
    expected: FileIdentity,
    directory: bool,
) -> io::Result<()> {
    let flags = if directory { libc::AT_REMOVEDIR } else { 0 };
    if let Err(error) = rename_no_replace(
        source_parent,
        source_name,
        operation.directory.as_raw_fd(),
        private_name,
    ) {
        operation.cleaned = true;
        return Err(error);
    }
    let moved = match stat_at(operation.directory.as_raw_fd(), private_name) {
        Ok(moved) => moved,
        Err(error) => {
            // The moved entry cannot be classified, so Drop must not delete
            // it from the private recovery capability.
            operation.cleaned = true;
            return Err(error);
        }
    };
    if FileIdentity::from_stat(&moved) != expected
        || file_type(moved.st_mode)
            != if directory {
                libc::S_IFDIR
            } else {
                libc::S_IFREG
            }
    {
        if let Err(error) = rename_no_replace(
            operation.directory.as_raw_fd(),
            private_name,
            source_parent,
            source_name,
        ) {
            operation.cleaned = true;
            return Err(error);
        }
        cvt(unsafe { libc::fsync(source_parent) })?;
        cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
        return Err(os_error(libc::ESTALE));
    }
    unlink_at(operation.directory.as_raw_fd(), private_name, flags)?;
    cvt(unsafe { libc::fsync(source_parent) })?;
    cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
    operation.remove_empty_owned()?;
    operation.cleaned = true;
    Ok(())
}

struct PrivateCommit<'a> {
    namespace: &'a PrivateNamespace,
    name: CString,
    published: bool,
}

impl<'a> PrivateCommit<'a> {
    fn new(namespace: &'a PrivateNamespace, name: CString) -> Self {
        Self {
            namespace,
            name,
            published: false,
        }
    }
}

impl Drop for PrivateCommit<'_> {
    fn drop(&mut self) {
        if !self.published {
            let _ = unlink_at(self.namespace.directory.as_raw_fd(), &self.name, 0);
        }
    }
}

fn cleanup_key(parent: FileIdentity, component: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"mac-worker.cleanup-intent.tree.v1\0");
    hasher.update(parent.device.to_be_bytes());
    hasher.update(parent.inode.to_be_bytes());
    hasher.update((component.len() as u64).to_be_bytes());
    hasher.update(component);
    format!("{:x}", hasher.finalize())
}

fn cleanup_hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn cleanup_hex_decode(encoded: &str) -> io::Result<Vec<u8>> {
    if encoded.len() % 2 != 0 || encoded.is_empty() {
        return Err(cleanup_record_error());
    }
    let mut decoded = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().chunks_exact(2) {
        let nibble = |value: u8| match value {
            b'0'..=b'9' => Some(value - b'0'),
            b'a'..=b'f' => Some(value - b'a' + 10),
            _ => None,
        };
        let high = nibble(pair[0]).ok_or_else(cleanup_record_error)?;
        let low = nibble(pair[1]).ok_or_else(cleanup_record_error)?;
        decoded.push((high << 4) | low);
    }
    if decoded.is_empty()
        || decoded == b"."
        || decoded == b".."
        || decoded.contains(&b'/')
        || decoded.contains(&0)
    {
        return Err(cleanup_record_error());
    }
    Ok(decoded)
}

fn cleanup_record_name(prefix: &str, key: &str) -> io::Result<CString> {
    if key.len() != 64
        || !key
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(cleanup_record_error());
    }
    CString::new(format!("{prefix}{key}")).map_err(|_| cleanup_record_error())
}

fn cleanup_intent_name(key: &str) -> io::Result<CString> {
    cleanup_record_name(CLEANUP_INTENT_PREFIX, key)
}

fn cleanup_decision_name(key: &str) -> io::Result<CString> {
    cleanup_record_name(CLEANUP_DECISION_PREFIX, key)
}

fn cleanup_intent_stage_name(key: &str) -> io::Result<CString> {
    cleanup_record_name(CLEANUP_INTENT_STAGE_PREFIX, key)
}

fn cleanup_decision_stage_name(key: &str) -> io::Result<CString> {
    cleanup_record_name(CLEANUP_DECISION_STAGE_PREFIX, key)
}

fn cleanup_bootstrap_name(
    key: &str,
    target: FileIdentity,
    original_mode: u32,
) -> io::Result<CString> {
    cleanup_record_name("", key)?;
    if original_mode > 0o7777 {
        return Err(cleanup_record_error());
    }
    CString::new(format!(
        "{CLEANUP_OPERATION_PREFIX}{key}-d{:016x}-i{:016x}-k01-m{original_mode:04x}",
        target.device, target.inode
    ))
    .map_err(|_| cleanup_record_error())
}

fn parse_cleanup_bootstrap_name(name: &CStr) -> io::Result<Option<CleanupBootstrapName>> {
    let Some(encoded) = name
        .to_bytes()
        .strip_prefix(CLEANUP_OPERATION_PREFIX.as_bytes())
    else {
        return Ok(None);
    };
    let encoded = std::str::from_utf8(encoded).map_err(|_| cleanup_record_error())?;
    let mut fields = encoded.split('-');
    let key = fields.next().ok_or_else(cleanup_record_error)?;
    let device = fields.next().ok_or_else(cleanup_record_error)?;
    let inode = fields.next().ok_or_else(cleanup_record_error)?;
    let kind = fields.next().ok_or_else(cleanup_record_error)?;
    let mode = fields.next().ok_or_else(cleanup_record_error)?;
    if fields.next().is_some()
        || key.len() != 64
        || device.len() != 17
        || inode.len() != 17
        || kind != "k01"
        || mode.len() != 5
    {
        return Err(cleanup_record_error());
    }
    cleanup_record_name("", key)?;
    let target = FileIdentity {
        device: u64::from_str_radix(
            device.strip_prefix('d').ok_or_else(cleanup_record_error)?,
            16,
        )
        .map_err(|_| cleanup_record_error())?,
        inode: u64::from_str_radix(
            inode.strip_prefix('i').ok_or_else(cleanup_record_error)?,
            16,
        )
        .map_err(|_| cleanup_record_error())?,
    };
    let original_mode =
        u32::from_str_radix(mode.strip_prefix('m').ok_or_else(cleanup_record_error)?, 16)
            .map_err(|_| cleanup_record_error())?;
    let parsed = CleanupBootstrapName {
        key: key.to_owned(),
        target,
        original_mode,
    };
    if cleanup_bootstrap_name(&parsed.key, parsed.target, parsed.original_mode)?.as_bytes()
        != name.to_bytes()
    {
        return Err(cleanup_record_error());
    }
    Ok(Some(parsed))
}

fn cleanup_canonical_json<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|_| cleanup_record_error())?;
    if bytes.len() > CLEANUP_RECORD_MAX_BYTES {
        return Err(os_error(libc::EFBIG));
    }
    Ok(bytes)
}

fn cleanup_parse_canonical_json<T>(bytes: &[u8]) -> io::Result<T>
where
    T: Serialize + serde::de::DeserializeOwned,
{
    if bytes.len() > CLEANUP_RECORD_MAX_BYTES {
        return Err(os_error(libc::EFBIG));
    }
    let value = serde_json::from_slice(bytes).map_err(|_| cleanup_record_error())?;
    if cleanup_canonical_json(&value)? != bytes {
        return Err(cleanup_record_error());
    }
    Ok(value)
}

fn open_cleanup_record(
    parent: RawFd,
    name: &CStr,
    expected_device: u64,
    lock: bool,
) -> io::Result<(File, Vec<u8>, FileIdentity)> {
    let before = stat_at(parent, name)?;
    require_cleanup_record_metadata(&before, expected_device)?;
    let descriptor = open_regular_rw_at(parent, name)?;
    if lock {
        // SAFETY: the descriptor is live and flock retains neither pointer nor
        // ownership. The intent lock serializes cooperating retry processes.
        cvt(unsafe { libc::flock(descriptor.as_raw_fd(), libc::LOCK_EX) })?;
    }
    let opened = stat_fd(descriptor.as_raw_fd())?;
    let current = stat_at(parent, name)?;
    require_cleanup_record_metadata(&opened, expected_device)?;
    require_cleanup_record_metadata(&current, expected_device)?;
    if !same_file(&before, &opened) || !same_file(&opened, &current) {
        return Err(os_error(libc::ESTALE));
    }
    let mut file = File::from(descriptor);
    let mut bytes = Vec::new();
    (&mut file)
        .take((CLEANUP_RECORD_MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > CLEANUP_RECORD_MAX_BYTES {
        return Err(os_error(libc::EFBIG));
    }
    Ok((file, bytes, FileIdentity::from_stat(&opened)))
}

fn require_cleanup_record_metadata(metadata: &libc::stat, expected_device: u64) -> io::Result<()> {
    require_private_regular_on_device(metadata, expected_device)?;
    if metadata.st_mode & 0o777 != 0o600 || metadata.st_size < 0 {
        return Err(os_error(libc::ESTALE));
    }
    if metadata.st_size as usize > CLEANUP_RECORD_MAX_BYTES {
        return Err(os_error(libc::EFBIG));
    }
    Ok(())
}

fn create_cleanup_record(
    parent: RawFd,
    name: &CStr,
    value: &impl Serialize,
) -> io::Result<FileIdentity> {
    let bytes = cleanup_canonical_json(value)?;
    let descriptor = create_regular_at(parent, name)?;
    let mut file = File::from(descriptor);
    if let Err(error) = file.write_all(&bytes).and_then(|()| file.sync_all()) {
        drop(file);
        let _ = unlink_at(parent, name, 0);
        return Err(error);
    }
    let metadata = stat_fd(file.as_raw_fd())?;
    require_cleanup_record_metadata(&metadata, metadata.st_dev as u64)?;
    Ok(FileIdentity::from_stat(&metadata))
}

fn cleanup_record_error() -> io::Error {
    os_error(libc::EINVAL)
}

fn cleanup_optional_stat(parent: RawFd, name: &CStr) -> io::Result<Option<libc::stat>> {
    match stat_at(parent, name) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn validate_cleanup_intent(
    intent: &CleanupIntentV1,
    parent: FileIdentity,
    component: &[u8],
    namespace: &PrivateNamespace,
) -> io::Result<CleanupIntentBindings> {
    let namespace_metadata = stat_fd(namespace.directory.as_raw_fd())?;
    require_private_directory_on_device(&namespace_metadata, parent.device)?;
    if namespace_metadata.st_mode & 0o777 != 0o700
        || FileIdentity::from_stat(&namespace_metadata) != namespace.identity
        || intent.version != 1
        || intent.kind != CleanupTargetKind::Tree
        || FileIdentity::from(intent.parent) != parent
        || FileIdentity::from(intent.namespace) != namespace.identity
        || intent.key_sha256 != cleanup_key(parent, component)
        || cleanup_hex_decode(&intent.component_hex)? != component
        || intent.original_mode > 0o7777
    {
        return Err(os_error(libc::ESTALE));
    }
    let target = FileIdentity::from(intent.target);
    let operation_identity = FileIdentity::from(intent.operation_identity);
    let placeholder_identity = FileIdentity::from(intent.placeholder_identity);
    if target.device != parent.device
        || operation_identity.device != parent.device
        || placeholder_identity.device != parent.device
        || target == placeholder_identity
    {
        return Err(os_error(libc::ESTALE));
    }
    let component = CString::new(component).map_err(|_| cleanup_record_error())?;
    let quarantine =
        CString::new(intent.quarantine.as_bytes()).map_err(|_| cleanup_record_error())?;
    let operation =
        CString::new(intent.operation.as_bytes()).map_err(|_| cleanup_record_error())?;
    let placeholder =
        CString::new(intent.placeholder.as_bytes()).map_err(|_| cleanup_record_error())?;
    let bootstrap = parse_cleanup_bootstrap_name(&operation)?.ok_or_else(cleanup_record_error)?;
    if !is_random_private_name(&quarantine, "cleanup-tree-v1")
        || placeholder.as_bytes() != CLEANUP_PLACEHOLDER_NAME.to_bytes()
        || bootstrap.key != intent.key_sha256
        || bootstrap.target != target
        || bootstrap.original_mode != intent.original_mode
    {
        return Err(cleanup_record_error());
    }
    Ok(CleanupIntentBindings {
        component,
        quarantine,
        operation,
        placeholder,
        parent,
        target,
        operation_identity,
        placeholder_identity,
        original_mode: intent.original_mode,
    })
}

fn load_cleanup_intent_at(
    parent_fd: RawFd,
    name: &CStr,
    parent: FileIdentity,
    component: &[u8],
    namespace: &PrivateNamespace,
    lock: bool,
) -> io::Result<LoadedCleanupIntent> {
    let (file, bytes, identity) = open_cleanup_record(parent_fd, name, parent.device, lock)?;
    let intent: CleanupIntentV1 = cleanup_parse_canonical_json(&bytes)?;
    validate_cleanup_intent(&intent, parent, component, namespace)?;
    Ok(LoadedCleanupIntent {
        intent,
        file,
        identity,
    })
}

fn find_cleanup_intent(
    namespace: &PrivateNamespace,
    parent: FileIdentity,
    component: &[u8],
) -> io::Result<Option<LoadedCleanupIntent>> {
    let key = cleanup_key(parent, component);
    let intent_name = cleanup_intent_name(&key)?;
    for _ in 0..32 {
        if cleanup_optional_stat(namespace.directory.as_raw_fd(), &intent_name)?.is_none() {
            return Ok(None);
        }
        match load_cleanup_intent_at(
            namespace.directory.as_raw_fd(),
            &intent_name,
            parent,
            component,
            namespace,
            true,
        ) {
            Ok(loaded) => return Ok(Some(loaded)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        }
    }
    Err(os_error(libc::ESTALE))
}

fn collect_pending_tree_cleanup_candidates(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
) -> io::Result<Vec<PendingTreeCleanupCandidate>> {
    let mut candidates = BTreeMap::new();
    for name in directory_entries(namespace.directory.as_raw_fd())? {
        let Some(suffix) = name
            .to_bytes()
            .strip_prefix(CLEANUP_INTENT_PREFIX.as_bytes())
        else {
            continue;
        };
        let key = std::str::from_utf8(suffix).map_err(|_| cleanup_record_error())?;
        if cleanup_intent_name(key)?.as_bytes() != name.as_bytes() {
            return Err(cleanup_record_error());
        }
        let (file, bytes, _identity) =
            open_cleanup_record(namespace.directory.as_raw_fd(), &name, parent.device, false)?;
        let intent: CleanupIntentV1 = cleanup_parse_canonical_json(&bytes)?;
        drop(file);
        if FileIdentity::from(intent.parent) != parent {
            continue;
        }
        let component = cleanup_hex_decode(&intent.component_hex)?;
        let bindings = validate_cleanup_intent(&intent, parent, &component, namespace)?;
        if intent.key_sha256 != key
            || candidates
                .insert(
                    component,
                    PendingTreeCleanupCandidate {
                        component: cleanup_hex_decode(&intent.component_hex)?,
                        target: bindings.target,
                        original_mode: bindings.original_mode,
                    },
                )
                .is_some()
        {
            return Err(os_error(libc::ESTALE));
        }
    }
    for operation_name in directory_entries(namespace.directory.as_raw_fd())? {
        let Some(parsed) = parse_cleanup_bootstrap_name(&operation_name)? else {
            continue;
        };
        if candidates
            .values()
            .any(|candidate| cleanup_key(parent, &candidate.component) == parsed.key)
        {
            continue;
        }
        let candidate = validate_unpublished_cleanup_bootstrap(
            namespace,
            public_parent,
            parent,
            &operation_name,
            &parsed,
        )?;
        if candidates
            .insert(candidate.component.clone(), candidate)
            .is_some()
        {
            return Err(os_error(libc::ESTALE));
        }
    }
    Ok(candidates.into_values().collect())
}

fn wait_for_live_private_operation(namespace: &PrivateNamespace, name: &CStr) -> io::Result<bool> {
    let before = stat_at(namespace.directory.as_raw_fd(), name)?;
    require_private_directory_on_device(&before, namespace.identity.device)?;
    if before.st_mode & 0o777 != 0o700 {
        return Err(os_error(libc::ESTALE));
    }
    let operation = open_directory_at(namespace.directory.as_raw_fd(), name)?;
    let opened = stat_fd(operation.as_raw_fd())?;
    let rebound = stat_at(namespace.directory.as_raw_fd(), name)?;
    if !same_file(&before, &opened) || !same_file(&opened, &rebound) {
        return Err(os_error(libc::ESTALE));
    }
    // An immediately acquirable legacy operation is unbound residue and must
    // remain fatal. Only a lock already held by this module identifies live,
    // transient work; wait for its owner and then restart classification.
    match cvt(unsafe { libc::flock(operation.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) }) {
        Ok(_) => return Ok(false),
        Err(error)
            if error.raw_os_error() == Some(libc::EWOULDBLOCK)
                || error.raw_os_error() == Some(libc::EAGAIN) => {}
        Err(error) => return Err(error),
    }
    cvt(unsafe { libc::flock(operation.as_raw_fd(), libc::LOCK_EX) })?;
    match stat_at(namespace.directory.as_raw_fd(), name) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(true),
        Ok(current) if same_file(&opened, &current) => Ok(false),
        Ok(_) => Err(os_error(libc::ESTALE)),
        Err(error) => Err(error),
    }
}

fn validate_cleanup_namespace_evidence(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
) -> io::Result<()> {
    for _ in 0..32 {
        match validate_cleanup_namespace_evidence_once(namespace, public_parent, parent) {
            Err(error)
                if error.kind() == io::ErrorKind::NotFound
                    || error.raw_os_error() == Some(libc::EAGAIN) =>
            {
                continue;
            }
            result => return result,
        }
    }
    Err(os_error(libc::ESTALE))
}

fn validate_cleanup_namespace_evidence_once(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    expected_parent: FileIdentity,
) -> io::Result<()> {
    struct EvidenceIntent {
        intent: CleanupIntentV1,
        identity: FileIdentity,
        bindings: CleanupIntentBindings,
    }

    let device = namespace.identity.device;
    if expected_parent.device != device {
        return Err(os_error(libc::ESTALE));
    }
    let top_entries = directory_entries(namespace.directory.as_raw_fd())?;
    for name in &top_entries {
        if is_random_private_name(name, "operation")
            && wait_for_live_private_operation(namespace, name)?
        {
            return Err(os_error(libc::EAGAIN));
        }
    }
    let mut intents = BTreeMap::<String, EvidenceIntent>::new();
    for name in &top_entries {
        let Some(suffix) = name
            .to_bytes()
            .strip_prefix(CLEANUP_INTENT_PREFIX.as_bytes())
        else {
            continue;
        };
        let key = std::str::from_utf8(suffix).map_err(|_| cleanup_record_error())?;
        if cleanup_intent_name(key)?.as_bytes() != name.as_bytes() {
            return Err(cleanup_record_error());
        }
        let (_file, bytes, identity) =
            open_cleanup_record(namespace.directory.as_raw_fd(), name, device, true)?;
        let intent: CleanupIntentV1 = cleanup_parse_canonical_json(&bytes)?;
        let component = cleanup_hex_decode(&intent.component_hex)?;
        let parent = FileIdentity::from(intent.parent);
        if parent != expected_parent {
            return Err(os_error(libc::ESTALE));
        }
        let bindings = validate_cleanup_intent(&intent, parent, &component, namespace)?;
        if intent.key_sha256 != key
            || intents
                .insert(
                    key.to_owned(),
                    EvidenceIntent {
                        intent,
                        identity,
                        bindings,
                    },
                )
                .is_some()
        {
            return Err(os_error(libc::ESTALE));
        }
    }

    let mut bootstrap_names = BTreeMap::<String, (CString, CleanupBootstrapName)>::new();
    for operation_name in &top_entries {
        let Some(parsed) = parse_cleanup_bootstrap_name(operation_name)? else {
            continue;
        };
        if bootstrap_names
            .insert(parsed.key.clone(), (operation_name.clone(), parsed))
            .is_some()
        {
            return Err(os_error(libc::ESTALE));
        }
    }
    for (key, (operation_name, parsed)) in &bootstrap_names {
        if let Some(evidence) = intents.get(key) {
            let (operation, identity) =
                open_cleanup_bootstrap_directory(namespace, operation_name, parsed)?;
            drop(operation);
            if evidence.bindings.operation.as_bytes() != operation_name.as_bytes()
                || evidence.bindings.operation_identity != identity
                || evidence.bindings.target != parsed.target
                || evidence.bindings.original_mode != parsed.original_mode
            {
                return Err(os_error(libc::ESTALE));
            }
        } else {
            let intent_name = cleanup_intent_name(key)?;
            let (operation, operation_identity) =
                open_cleanup_bootstrap_directory(namespace, operation_name, parsed)?;
            // No namespace lock is held while waiting for the keyed bootstrap
            // owner. A completed publisher makes the canonical intent visible
            // before releasing this lock, so the validator can restart from a
            // coherent grammar instead of misclassifying the handoff.
            cvt(unsafe { libc::flock(operation.as_raw_fd(), libc::LOCK_EX) })?;
            let rebound = match stat_at(namespace.directory.as_raw_fd(), operation_name) {
                Ok(rebound) => rebound,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    return Err(os_error(libc::EAGAIN));
                }
                Err(error) => return Err(error),
            };
            if FileIdentity::from_stat(&rebound) != operation_identity {
                return Err(os_error(libc::ESTALE));
            }
            if cleanup_optional_stat(namespace.directory.as_raw_fd(), &intent_name)?.is_some() {
                return Err(os_error(libc::EAGAIN));
            }
            if let Err(error) = validate_unpublished_cleanup_bootstrap_opened(
                namespace,
                public_parent,
                expected_parent,
                operation_name,
                parsed,
                &operation,
                operation_identity,
            ) {
                if cleanup_optional_stat(namespace.directory.as_raw_fd(), &intent_name)?.is_some()
                    || cleanup_optional_stat(namespace.directory.as_raw_fd(), operation_name)?
                        .is_none()
                {
                    return Err(os_error(libc::EAGAIN));
                }
                return Err(error);
            }
            if cleanup_optional_stat(namespace.directory.as_raw_fd(), &intent_name)?.is_some() {
                return Err(os_error(libc::EAGAIN));
            }
        }
    }

    let mut quarantines = BTreeMap::new();
    let mut operations = BTreeMap::new();
    for evidence in intents.values() {
        if quarantines
            .insert(
                evidence.bindings.quarantine.as_bytes().to_vec(),
                (
                    evidence.bindings.target,
                    evidence.bindings.placeholder_identity,
                ),
            )
            .is_some()
            || operations
                .insert(
                    evidence.bindings.operation.as_bytes().to_vec(),
                    (
                        evidence.bindings.operation_identity,
                        evidence.bindings.placeholder.as_bytes().to_vec(),
                        evidence.bindings.target,
                        evidence.bindings.placeholder_identity,
                        evidence.intent.key_sha256.clone(),
                    ),
                )
                .is_some()
        {
            return Err(os_error(libc::ESTALE));
        }
    }

    for name in &top_entries {
        if name
            .to_bytes()
            .starts_with(CLEANUP_INTENT_PREFIX.as_bytes())
        {
            continue;
        }
        if let Some((target, placeholder)) = quarantines.get(name.to_bytes()) {
            let metadata = stat_at(namespace.directory.as_raw_fd(), name)?;
            let identity = FileIdentity::from_stat(&metadata);
            if file_type(metadata.st_mode) != libc::S_IFDIR
                || metadata.st_uid != effective_user_id()
                || metadata.st_dev as u64 != device
                || metadata.st_mode & 0o777 != 0o700
                || (identity != *target && identity != *placeholder)
            {
                return Err(os_error(libc::ESTALE));
            }
            continue;
        }
        if let Some((expected, _, _, _, _)) = operations.get(name.to_bytes()) {
            let metadata = stat_at(namespace.directory.as_raw_fd(), name)?;
            if file_type(metadata.st_mode) != libc::S_IFDIR
                || metadata.st_uid != effective_user_id()
                || metadata.st_dev as u64 != device
                || metadata.st_mode & 0o777 != 0o700
                || FileIdentity::from_stat(&metadata) != *expected
            {
                return Err(os_error(libc::ESTALE));
            }
            continue;
        }
        if name
            .to_bytes()
            .starts_with(CLEANUP_OPERATION_PREFIX.as_bytes())
        {
            if parse_cleanup_bootstrap_name(name)?.is_none() {
                return Err(cleanup_record_error());
            }
            continue;
        }
        if let Some(suffix) = name
            .to_bytes()
            .strip_prefix(CLEANUP_DECISION_PREFIX.as_bytes())
        {
            let key = std::str::from_utf8(suffix).map_err(|_| cleanup_record_error())?;
            if cleanup_decision_name(key)?.as_bytes() != name.as_bytes() {
                return Err(cleanup_record_error());
            }
            let evidence = intents.get(key).ok_or_else(|| os_error(libc::ESTALE))?;
            let (_file, bytes, _identity) =
                open_cleanup_record(namespace.directory.as_raw_fd(), name, device, false)?;
            let record: CleanupDecisionRecordV1 = cleanup_parse_canonical_json(&bytes)?;
            if record.version != 1
                || record.key_sha256 != key
                || FileIdentity::from(record.intent) != evidence.identity
            {
                return Err(os_error(libc::ESTALE));
            }
            continue;
        }
        return Err(os_error(libc::ESTALE));
    }

    for (operation_name, (expected, placeholder_name, target, placeholder, key)) in operations {
        let operation_name = CString::new(operation_name).map_err(|_| cleanup_record_error())?;
        let Some(operation) = PrivateOperation::open_bound(namespace, &operation_name, expected)?
        else {
            continue;
        };
        let placeholder_name =
            CString::new(placeholder_name).map_err(|_| cleanup_record_error())?;
        for name in directory_entries(operation.directory.as_raw_fd())? {
            if name.as_bytes() == placeholder_name.as_bytes() {
                let metadata = stat_at(operation.directory.as_raw_fd(), &name)?;
                let identity = FileIdentity::from_stat(&metadata);
                if file_type(metadata.st_mode) != libc::S_IFDIR
                    || metadata.st_uid != effective_user_id()
                    || metadata.st_dev as u64 != device
                    || metadata.st_mode & 0o777 != 0o700
                    || (identity != target && identity != placeholder)
                {
                    return Err(os_error(libc::ESTALE));
                }
                continue;
            }
            if name.as_bytes() == cleanup_decision_stage_name(&key)?.as_bytes() {
                let evidence = intents.get(&key).ok_or_else(|| os_error(libc::ESTALE))?;
                let (_file, bytes, _identity) =
                    open_cleanup_record(operation.directory.as_raw_fd(), &name, device, false)?;
                let record: CleanupDecisionRecordV1 = cleanup_parse_canonical_json(&bytes)?;
                if record.version != 1
                    || record.key_sha256 != key
                    || FileIdentity::from(record.intent) != evidence.identity
                {
                    return Err(os_error(libc::ESTALE));
                }
                continue;
            }
            return Err(os_error(libc::ESTALE));
        }
    }
    Ok(())
}

fn load_cleanup_decision(
    namespace: &PrivateNamespace,
    loaded: &LoadedCleanupIntent,
    operation: Option<&PrivateOperation<'_>>,
) -> io::Result<Option<LoadedCleanupDecision>> {
    let key = &loaded.intent.key_sha256;
    let decision_name = cleanup_decision_name(key)?;
    let stage_name = cleanup_decision_stage_name(key)?;
    let canonical_exists =
        cleanup_optional_stat(namespace.directory.as_raw_fd(), &decision_name)?.is_some();
    let stage_exists = match operation {
        Some(operation) => {
            cleanup_optional_stat(operation.directory.as_raw_fd(), &stage_name)?.is_some()
        }
        None => false,
    };
    if canonical_exists && stage_exists {
        return Err(os_error(libc::ESTALE));
    }
    if !canonical_exists && stage_exists {
        let operation = operation.ok_or_else(|| os_error(libc::ESTALE))?;
        let staged = load_cleanup_decision_at(
            operation.directory.as_raw_fd(),
            &stage_name,
            loaded,
            namespace,
        )?;
        rename_no_replace(
            operation.directory.as_raw_fd(),
            &stage_name,
            namespace.directory.as_raw_fd(),
            &decision_name,
        )?;
        cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
        cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })?;
        let current = stat_at(namespace.directory.as_raw_fd(), &decision_name)?;
        if FileIdentity::from_stat(&current) != staged.identity {
            return Err(os_error(libc::ESTALE));
        }
        return Ok(Some(staged));
    }
    if canonical_exists {
        load_cleanup_decision_at(
            namespace.directory.as_raw_fd(),
            &decision_name,
            loaded,
            namespace,
        )
        .map(Some)
    } else {
        Ok(None)
    }
}

fn load_cleanup_decision_at(
    parent: RawFd,
    name: &CStr,
    loaded: &LoadedCleanupIntent,
    namespace: &PrivateNamespace,
) -> io::Result<LoadedCleanupDecision> {
    let (file, bytes, identity) =
        open_cleanup_record(parent, name, namespace.identity.device, false)?;
    let record: CleanupDecisionRecordV1 = cleanup_parse_canonical_json(&bytes)?;
    if record.version != 1
        || record.key_sha256 != loaded.intent.key_sha256
        || FileIdentity::from(record.intent) != loaded.identity
    {
        return Err(os_error(libc::ESTALE));
    }
    Ok(LoadedCleanupDecision {
        record,
        file,
        identity,
    })
}

fn publish_cleanup_decision(
    namespace: &PrivateNamespace,
    loaded: &LoadedCleanupIntent,
    operation: &PrivateOperation<'_>,
    decision: CleanupDecisionV1,
) -> io::Result<LoadedCleanupDecision> {
    if let Some(existing) = load_cleanup_decision(namespace, loaded, Some(operation))? {
        if existing.record.decision != decision {
            return Err(os_error(libc::ESTALE));
        }
        return Ok(existing);
    }
    let record = CleanupDecisionRecordV1 {
        version: 1,
        key_sha256: loaded.intent.key_sha256.clone(),
        intent: loaded.identity.into(),
        decision,
    };
    let stage_name = cleanup_decision_stage_name(&record.key_sha256)?;
    let decision_name = cleanup_decision_name(&record.key_sha256)?;
    create_cleanup_record(operation.directory.as_raw_fd(), &stage_name, &record)?;
    cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
    cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })?;
    rename_no_replace(
        operation.directory.as_raw_fd(),
        &stage_name,
        namespace.directory.as_raw_fd(),
        &decision_name,
    )?;
    cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
    cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })?;
    load_cleanup_decision_at(
        namespace.directory.as_raw_fd(),
        &decision_name,
        loaded,
        namespace,
    )
}

fn cleanup_bootstrap_slots(
    namespace: &PrivateNamespace,
    key: &str,
) -> io::Result<Vec<(CString, CleanupBootstrapName)>> {
    let mut slots = Vec::new();
    for name in directory_entries(namespace.directory.as_raw_fd())? {
        if name
            .to_bytes()
            .starts_with(CLEANUP_OPERATION_PREFIX.as_bytes())
        {
            let parsed = parse_cleanup_bootstrap_name(&name)?.ok_or_else(cleanup_record_error)?;
            if parsed.key == key {
                slots.push((name, parsed));
            }
        }
    }
    Ok(slots)
}

fn open_cleanup_bootstrap_directory(
    namespace: &PrivateNamespace,
    name: &CStr,
    parsed: &CleanupBootstrapName,
) -> io::Result<(OwnedFd, FileIdentity)> {
    if parsed.target.device != namespace.identity.device || parsed.original_mode > 0o7777 {
        return Err(os_error(libc::ESTALE));
    }
    let before = stat_at(namespace.directory.as_raw_fd(), name)?;
    require_private_directory_on_device(&before, namespace.identity.device)?;
    if before.st_mode & 0o777 != 0o700 {
        return Err(os_error(libc::ESTALE));
    }
    let directory = open_directory_at(namespace.directory.as_raw_fd(), name)?;
    let opened = stat_fd(directory.as_raw_fd())?;
    let rebound = stat_at(namespace.directory.as_raw_fd(), name)?;
    require_private_directory_on_device(&opened, namespace.identity.device)?;
    require_private_directory_on_device(&rebound, namespace.identity.device)?;
    if opened.st_mode & 0o777 != 0o700
        || rebound.st_mode & 0o777 != 0o700
        || !same_file(&before, &opened)
        || !same_file(&opened, &rebound)
    {
        return Err(os_error(libc::ESTALE));
    }
    Ok((directory, FileIdentity::from_stat(&opened)))
}

fn find_cleanup_bootstrap_component(
    public_parent: RawFd,
    parent: FileIdentity,
    parsed: &CleanupBootstrapName,
) -> io::Result<Vec<u8>> {
    let mut component = None;
    for name in directory_entries(public_parent)? {
        if cleanup_key(parent, name.to_bytes()) != parsed.key {
            continue;
        }
        if component.is_some() {
            return Err(os_error(libc::ESTALE));
        }
        drop(open_bound_cleanup_directory(
            public_parent,
            &name,
            parsed.target,
            parent.device,
            Some(parsed.original_mode),
        )?);
        component = Some(name.to_bytes().to_vec());
    }
    component.ok_or_else(|| os_error(libc::ESTALE))
}

fn inspect_cleanup_bootstrap_placeholder(
    operation: RawFd,
    expected_device: u64,
) -> io::Result<Option<FileIdentity>> {
    let before = match stat_at(operation, CLEANUP_PLACEHOLDER_NAME) {
        Ok(before) => before,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    require_private_directory_on_device(&before, expected_device)?;
    if before.st_mode & 0o777 != 0o700 {
        return Err(os_error(libc::ESTALE));
    }
    let placeholder = open_directory_at(operation, CLEANUP_PLACEHOLDER_NAME)?;
    let opened = stat_fd(placeholder.as_raw_fd())?;
    let rebound = stat_at(operation, CLEANUP_PLACEHOLDER_NAME)?;
    require_private_directory_on_device(&opened, expected_device)?;
    require_private_directory_on_device(&rebound, expected_device)?;
    if opened.st_mode & 0o777 != 0o700
        || rebound.st_mode & 0o777 != 0o700
        || !same_file(&before, &opened)
        || !same_file(&opened, &rebound)
        || !directory_entries(placeholder.as_raw_fd())?.is_empty()
    {
        return Err(os_error(libc::ESTALE));
    }
    Ok(Some(FileIdentity::from_stat(&opened)))
}

fn validate_unpublished_cleanup_bootstrap(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
    operation_name: &CStr,
    parsed: &CleanupBootstrapName,
) -> io::Result<PendingTreeCleanupCandidate> {
    let (operation, operation_identity) =
        open_cleanup_bootstrap_directory(namespace, operation_name, parsed)?;
    validate_unpublished_cleanup_bootstrap_opened(
        namespace,
        public_parent,
        parent,
        operation_name,
        parsed,
        &operation,
        operation_identity,
    )
}

fn validate_unpublished_cleanup_bootstrap_opened(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
    operation_name: &CStr,
    parsed: &CleanupBootstrapName,
    operation: &OwnedFd,
    operation_identity: FileIdentity,
) -> io::Result<PendingTreeCleanupCandidate> {
    if parsed.key.len() != 64 || parsed.target.device != parent.device {
        return Err(os_error(libc::ESTALE));
    }
    let component = find_cleanup_bootstrap_component(public_parent, parent, parsed)?;
    let stage_name = cleanup_intent_stage_name(&parsed.key)?;
    let entries = directory_entries(operation.as_raw_fd())?;
    if entries.iter().any(|entry| {
        entry.as_bytes() != CLEANUP_PLACEHOLDER_NAME.to_bytes()
            && entry.as_bytes() != stage_name.to_bytes()
    }) || entries.len() > 2
    {
        return Err(os_error(libc::ESTALE));
    }
    let placeholder =
        inspect_cleanup_bootstrap_placeholder(operation.as_raw_fd(), namespace.identity.device)?;
    if cleanup_optional_stat(operation.as_raw_fd(), &stage_name)?.is_some() {
        let (_file, bytes, _stage_identity) = open_cleanup_record(
            operation.as_raw_fd(),
            &stage_name,
            namespace.identity.device,
            false,
        )?;
        if let Ok(intent) = cleanup_parse_canonical_json::<CleanupIntentV1>(&bytes) {
            let bindings = validate_cleanup_intent(&intent, parent, &component, namespace)?;
            if intent.key_sha256 != parsed.key
                || bindings.operation.as_bytes() != operation_name.to_bytes()
                || bindings.operation_identity != operation_identity
                || bindings.target != parsed.target
                || bindings.original_mode != parsed.original_mode
                || placeholder != Some(bindings.placeholder_identity)
                || cleanup_optional_stat(namespace.directory.as_raw_fd(), &bindings.quarantine)?
                    .is_some()
            {
                return Err(os_error(libc::ESTALE));
            }
        }
    }
    Ok(PendingTreeCleanupCandidate {
        component,
        target: parsed.target,
        original_mode: parsed.original_mode,
    })
}

fn find_unpublished_cleanup_bootstrap(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
    component: &[u8],
) -> io::Result<Option<PendingTreeCleanupCandidate>> {
    let key = cleanup_key(parent, component);
    let slots = cleanup_bootstrap_slots(namespace, &key)?;
    if slots.len() > 1 {
        return Err(os_error(libc::ESTALE));
    }
    let Some((name, parsed)) = slots.first() else {
        return Ok(None);
    };
    let candidate =
        validate_unpublished_cleanup_bootstrap(namespace, public_parent, parent, name, parsed)?;
    if candidate.component != component {
        return Err(os_error(libc::ESTALE));
    }
    Ok(Some(candidate))
}

fn resolve_bound_tree_cleanup(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
    component: &[u8],
    target: FileIdentity,
    original_mode: u32,
    verify_parent: &impl Fn() -> io::Result<()>,
    allow_injected_validation: bool,
) -> io::Result<()> {
    let component_name = CString::new(component).map_err(|_| cleanup_record_error())?;
    let key = cleanup_key(parent, component);
    let mut last_race = os_error(libc::ESTALE);
    for _ in 0..32 {
        validate_cleanup_namespace_evidence(namespace, public_parent, parent)?;
        if let Some(loaded) = find_cleanup_intent(namespace, parent, component)? {
            let result = resume_tree_cleanup(
                namespace,
                public_parent,
                parent,
                component,
                &loaded,
                verify_parent,
                allow_injected_validation,
            );
            match result {
                Ok(()) => return Ok(()),
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                    ) =>
                {
                    last_race = error;
                    drop(loaded);
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        if let Some(candidate) =
            find_unpublished_cleanup_bootstrap(namespace, public_parent, parent, component)?
        {
            if candidate.target != target || candidate.original_mode != original_mode {
                return Err(os_error(libc::ESTALE));
            }
            match publish_tree_cleanup_intent(
                namespace,
                public_parent,
                parent,
                &component_name,
                target,
                original_mode,
            ) {
                Ok(()) => continue,
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::EEXIST)
                            | Some(libc::EAGAIN)
                            | Some(libc::ENOENT)
                            | Some(libc::ESTALE)
                    ) =>
                {
                    last_race = error;
                    continue;
                }
                Err(error) => return Err(error),
            }
        }

        // Revalidate the terminal postcondition after both keyed lookup paths.
        // A bound call never starts over from a public name: only the same key
        // may resume, while a replacement remains untouched and fails closed.
        validate_cleanup_namespace_evidence(namespace, public_parent, parent)?;
        if cleanup_optional_stat(namespace.directory.as_raw_fd(), &cleanup_intent_name(&key)?)?
            .is_some()
            || !cleanup_bootstrap_slots(namespace, &key)?.is_empty()
        {
            continue;
        }
        return match stat_at(public_parent, &component_name) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Ok(_) => Err(os_error(libc::ESTALE)),
            Err(error) => Err(error),
        };
    }
    Err(last_race)
}

fn open_or_create_cleanup_bootstrap<'a>(
    namespace: &'a PrivateNamespace,
    public_parent: RawFd,
    component: &CStr,
    key: &str,
    target: FileIdentity,
    original_mode: u32,
) -> io::Result<Option<PrivateOperation<'a>>> {
    let expected_name = cleanup_bootstrap_name(key, target, original_mode)?;
    let intent_name = cleanup_intent_name(key)?;
    for _ in 0..32 {
        match open_bound_cleanup_directory(
            public_parent,
            component,
            target,
            target.device,
            Some(original_mode),
        ) {
            Ok(target) => drop(target),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let canonical =
                    cleanup_optional_stat(namespace.directory.as_raw_fd(), &intent_name)?.is_some();
                let slots = cleanup_bootstrap_slots(namespace, key)?;
                if canonical || slots.is_empty() {
                    return Ok(None);
                }
                return Err(os_error(libc::ESTALE));
            }
            Err(error) => return Err(error),
        }
        // SAFETY: the namespace descriptor is live. This lock serializes the
        // same-key scan/create step between cooperating retriers. It is
        // released before waiting for the operation lock.
        cvt(unsafe { libc::flock(namespace.directory.as_raw_fd(), libc::LOCK_EX) })?;
        let initial = (|| {
            if cleanup_optional_stat(namespace.directory.as_raw_fd(), &intent_name)?.is_some() {
                return Ok(None);
            }
            let slots = cleanup_bootstrap_slots(namespace, key)?;
            if slots.len() > 1 {
                return Err(os_error(libc::ESTALE));
            }
            let created = if let Some((name, parsed)) = slots.first() {
                if name.as_bytes() != expected_name.as_bytes()
                    || parsed.target != target
                    || parsed.original_mode != original_mode
                {
                    return Err(os_error(libc::ESTALE));
                }
                false
            } else {
                drop(open_bound_cleanup_directory(
                    public_parent,
                    component,
                    target,
                    target.device,
                    Some(original_mode),
                )?);
                match mkdir_at(namespace.directory.as_raw_fd(), &expected_name, 0o700) {
                    Ok(()) => {}
                    Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {
                        return Err(os_error(libc::EAGAIN));
                    }
                    Err(error) => return Err(error),
                }
                cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })?;
                true
            };
            let before = stat_at(namespace.directory.as_raw_fd(), &expected_name)?;
            require_private_directory_on_device(&before, target.device)?;
            if before.st_mode & 0o777 != 0o700 {
                return Err(os_error(libc::ESTALE));
            }
            let directory = open_directory_at(namespace.directory.as_raw_fd(), &expected_name)?;
            let opened = stat_fd(directory.as_raw_fd())?;
            let rebound = stat_at(namespace.directory.as_raw_fd(), &expected_name)?;
            if !same_file(&before, &opened)
                || !same_file(&opened, &rebound)
                || opened.st_uid != effective_user_id()
                || opened.st_dev as u64 != target.device
                || opened.st_mode & 0o777 != 0o700
            {
                return Err(os_error(libc::ESTALE));
            }
            Ok(Some((directory, FileIdentity::from_stat(&opened), created)))
        })();
        let unlock = cvt(unsafe { libc::flock(namespace.directory.as_raw_fd(), libc::LOCK_UN) });
        let Some((directory, identity, created)) = (match (initial, unlock) {
            (Ok(initial), Ok(())) => initial,
            (Err(error), Ok(())) if error.raw_os_error() == Some(libc::EAGAIN) => continue,
            (Err(error), Ok(())) => return Err(error),
            (_, Err(error)) => return Err(error),
        }) else {
            return Ok(None);
        };
        if created {
            injected_cleanup_bootstrap_result()?;
        }
        // SAFETY: no namespace lock is held while waiting for the operation.
        cvt(unsafe { libc::flock(directory.as_raw_fd(), libc::LOCK_EX) })?;
        cvt(unsafe { libc::flock(namespace.directory.as_raw_fd(), libc::LOCK_EX) })?;
        let handoff = (|| {
            if cleanup_optional_stat(namespace.directory.as_raw_fd(), &intent_name)?.is_some() {
                return Ok(None);
            }
            let slots = cleanup_bootstrap_slots(namespace, key)?;
            if slots.len() != 1 || slots[0].0.as_bytes() != expected_name.as_bytes() {
                if slots.is_empty() {
                    return Err(os_error(libc::EAGAIN));
                }
                return Err(os_error(libc::ESTALE));
            }
            drop(open_bound_cleanup_directory(
                public_parent,
                component,
                target,
                target.device,
                Some(original_mode),
            )?);
            let opened = stat_fd(directory.as_raw_fd())?;
            let rebound = stat_at(namespace.directory.as_raw_fd(), &expected_name)?;
            if FileIdentity::from_stat(&opened) != identity
                || FileIdentity::from_stat(&rebound) != identity
                || !same_file(&opened, &rebound)
                || opened.st_uid != effective_user_id()
                || opened.st_dev as u64 != target.device
                || opened.st_mode & 0o777 != 0o700
            {
                return Err(os_error(libc::ESTALE));
            }
            Ok(Some(PrivateOperation {
                namespace,
                name: expected_name.clone(),
                directory,
                identity,
                // The keyed slot is durable recovery evidence immediately
                // after its parent fsync and is never Drop-cleaned.
                cleaned: true,
            }))
        })();
        let unlock = cvt(unsafe { libc::flock(namespace.directory.as_raw_fd(), libc::LOCK_UN) });
        match (handoff, unlock) {
            (Ok(Some(operation)), Ok(())) => return Ok(Some(operation)),
            (Ok(None), Ok(())) => return Ok(None),
            (Err(error), Ok(())) if error.raw_os_error() == Some(libc::EAGAIN) => continue,
            (Err(error), Ok(())) => return Err(error),
            (_, Err(error)) => return Err(error),
        }
    }
    Err(os_error(libc::ESTALE))
}

fn prepare_cleanup_bootstrap_placeholder(
    operation: &PrivateOperation<'_>,
    expected_device: u64,
    stage_name: &CStr,
    create_if_missing: bool,
) -> io::Result<FileIdentity> {
    for entry in directory_entries(operation.directory.as_raw_fd())? {
        if entry.as_bytes() != CLEANUP_PLACEHOLDER_NAME.to_bytes()
            && entry.as_bytes() != stage_name.to_bytes()
        {
            return Err(os_error(libc::ESTALE));
        }
    }
    let created =
        match cleanup_optional_stat(operation.directory.as_raw_fd(), CLEANUP_PLACEHOLDER_NAME)? {
            Some(_) => false,
            None if create_if_missing => {
                match mkdir_at(
                    operation.directory.as_raw_fd(),
                    CLEANUP_PLACEHOLDER_NAME,
                    0o700,
                ) {
                    Ok(()) => true,
                    Err(error) if error.raw_os_error() == Some(libc::EEXIST) => false,
                    Err(error) => return Err(error),
                }
            }
            None => return Err(os_error(libc::ESTALE)),
        };
    let before = stat_at(operation.directory.as_raw_fd(), CLEANUP_PLACEHOLDER_NAME)?;
    require_private_directory_on_device(&before, expected_device)?;
    if before.st_mode & 0o777 != 0o700 {
        return Err(os_error(libc::ESTALE));
    }
    let placeholder = open_directory_at(operation.directory.as_raw_fd(), CLEANUP_PLACEHOLDER_NAME)?;
    let opened = stat_fd(placeholder.as_raw_fd())?;
    let rebound = stat_at(operation.directory.as_raw_fd(), CLEANUP_PLACEHOLDER_NAME)?;
    if !same_file(&before, &opened)
        || !same_file(&opened, &rebound)
        || opened.st_mode & 0o777 != 0o700
        || rebound.st_mode & 0o777 != 0o700
        || !directory_entries(placeholder.as_raw_fd())?.is_empty()
    {
        return Err(os_error(libc::ESTALE));
    }
    if created {
        cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
        injected_cleanup_placeholder_result()?;
    }
    Ok(FileIdentity::from_stat(&opened))
}

fn prepare_cleanup_intent_stage(
    namespace: &PrivateNamespace,
    operation: &PrivateOperation<'_>,
    public_parent: RawFd,
    parent: FileIdentity,
    component: &CStr,
    target: FileIdentity,
    original_mode: u32,
    placeholder_identity: FileIdentity,
    stage_name: &CStr,
) -> io::Result<CleanupIntentV1> {
    let key = cleanup_key(parent, component.to_bytes());
    if cleanup_optional_stat(operation.directory.as_raw_fd(), stage_name)?.is_some() {
        let (file, bytes, _identity) = open_cleanup_record(
            operation.directory.as_raw_fd(),
            stage_name,
            namespace.identity.device,
            false,
        )?;
        if let Ok(parsed) = cleanup_parse_canonical_json::<CleanupIntentV1>(&bytes) {
            let bindings =
                validate_cleanup_intent(&parsed, parent, component.to_bytes(), namespace)?;
            let entries = directory_entries(operation.directory.as_raw_fd())?;
            if parsed.key_sha256 != key
                || bindings.operation.as_bytes() != operation.name.as_bytes()
                || bindings.operation_identity != operation.identity
                || bindings.target != target
                || bindings.original_mode != original_mode
                || bindings.placeholder_identity != placeholder_identity
                || entries.len() != 2
                || !entries
                    .iter()
                    .any(|entry| entry.as_bytes() == CLEANUP_PLACEHOLDER_NAME.to_bytes())
                || !entries
                    .iter()
                    .any(|entry| entry.as_bytes() == stage_name.to_bytes())
            {
                return Err(os_error(libc::ESTALE));
            }
            drop(open_bound_cleanup_directory(
                public_parent,
                component,
                target,
                namespace.identity.device,
                Some(original_mode),
            )?);
            if cleanup_optional_stat(namespace.directory.as_raw_fd(), &bindings.quarantine)?
                .is_some()
            {
                return Err(os_error(libc::ESTALE));
            }
            drop(file);
            return Ok(parsed);
        }
        // A partial deterministic stage is rebuildable only while the exact
        // public target is still in its recorded original mode and no
        // quarantine/public mutation has occurred.
        drop(open_bound_cleanup_directory(
            public_parent,
            component,
            target,
            namespace.identity.device,
            Some(original_mode),
        )?);
        let entries = directory_entries(operation.directory.as_raw_fd())?;
        if entries.len() != 2
            || !entries
                .iter()
                .any(|entry| entry.as_bytes() == CLEANUP_PLACEHOLDER_NAME.to_bytes())
            || !entries
                .iter()
                .any(|entry| entry.as_bytes() == stage_name.to_bytes())
        {
            return Err(os_error(libc::ESTALE));
        }
        drop(file);
        unlink_at(operation.directory.as_raw_fd(), stage_name, 0)?;
        cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
    }
    let quarantine = random_private_name("cleanup-tree-v1");
    let intent = CleanupIntentV1 {
        version: 1,
        kind: CleanupTargetKind::Tree,
        key_sha256: key,
        component_hex: cleanup_hex_encode(component.to_bytes()),
        parent: parent.into(),
        namespace: namespace.identity.into(),
        target: target.into(),
        original_mode,
        quarantine: quarantine
            .to_str()
            .map_err(|_| cleanup_record_error())?
            .to_owned(),
        operation: operation
            .name
            .to_str()
            .map_err(|_| cleanup_record_error())?
            .to_owned(),
        operation_identity: operation.identity.into(),
        placeholder: CLEANUP_PLACEHOLDER_NAME
            .to_str()
            .map_err(|_| cleanup_record_error())?
            .to_owned(),
        placeholder_identity: placeholder_identity.into(),
    };
    validate_cleanup_intent(&intent, parent, component.to_bytes(), namespace)?;
    let bytes = cleanup_canonical_json(&intent)?;
    let descriptor = create_regular_at(operation.directory.as_raw_fd(), stage_name)?;
    let mut file = File::from(descriptor);
    let midpoint = bytes.len() / 2;
    file.write_all(&bytes[..midpoint])?;
    file.sync_all()?;
    cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
    injected_cleanup_intent_write_result()?;
    file.write_all(&bytes[midpoint..])?;
    file.sync_all()?;
    let metadata = stat_fd(file.as_raw_fd())?;
    require_cleanup_record_metadata(&metadata, namespace.identity.device)?;
    Ok(intent)
}

fn publish_tree_cleanup_intent(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
    component: &CStr,
    target: FileIdentity,
    original_mode: u32,
) -> io::Result<()> {
    let key = cleanup_key(parent, component.to_bytes());
    let Some(mut operation) = open_or_create_cleanup_bootstrap(
        namespace,
        public_parent,
        component,
        &key,
        target,
        original_mode,
    )?
    else {
        return Ok(());
    };
    let stage_name = cleanup_intent_stage_name(&key)?;
    let stage_exists =
        cleanup_optional_stat(operation.directory.as_raw_fd(), &stage_name)?.is_some();
    let placeholder_identity = prepare_cleanup_bootstrap_placeholder(
        &operation,
        parent.device,
        &stage_name,
        !stage_exists,
    )?;
    let intent_name = cleanup_intent_name(&key)?;
    let intent = prepare_cleanup_intent_stage(
        namespace,
        &operation,
        public_parent,
        parent,
        component,
        target,
        original_mode,
        placeholder_identity,
        &stage_name,
    )?;
    validate_cleanup_intent(&intent, parent, component.to_bytes(), namespace)?;
    cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
    cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })?;
    injected_cleanup_intent_sync_result()?;
    match rename_no_replace(
        operation.directory.as_raw_fd(),
        &stage_name,
        namespace.directory.as_raw_fd(),
        &intent_name,
    ) {
        Ok(()) => {}
        Err(error) if error.raw_os_error() == Some(libc::EEXIST) => return Err(error),
        Err(error) => return Err(error),
    }
    cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
    cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })?;
    operation.cleaned = true;
    Ok(())
}

fn classify_cleanup_slot(
    parent: RawFd,
    name: &CStr,
    target: FileIdentity,
    placeholder: FileIdentity,
) -> io::Result<CleanupSlot> {
    let Some(metadata) = cleanup_optional_stat(parent, name)? else {
        return Ok(CleanupSlot::Missing);
    };
    let identity = FileIdentity::from_stat(&metadata);
    if identity == target {
        if file_type(metadata.st_mode) != libc::S_IFDIR {
            return Err(os_error(libc::ESTALE));
        }
        Ok(CleanupSlot::Target)
    } else if identity == placeholder {
        if file_type(metadata.st_mode) != libc::S_IFDIR {
            return Err(os_error(libc::ESTALE));
        }
        Ok(CleanupSlot::Placeholder)
    } else {
        Ok(CleanupSlot::Other)
    }
}

fn classify_cleanup_slots(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    bindings: &CleanupIntentBindings,
    operation: Option<&PrivateOperation<'_>>,
) -> io::Result<CleanupSlots> {
    let public = classify_cleanup_slot(
        public_parent,
        &bindings.component,
        bindings.target,
        bindings.placeholder_identity,
    )?;
    let quarantine = classify_cleanup_slot(
        namespace.directory.as_raw_fd(),
        &bindings.quarantine,
        bindings.target,
        bindings.placeholder_identity,
    )?;
    let (operation_slot, operation_exists) = match operation {
        Some(operation) => (
            classify_cleanup_slot(
                operation.directory.as_raw_fd(),
                &bindings.placeholder,
                bindings.target,
                bindings.placeholder_identity,
            )?,
            true,
        ),
        None => (CleanupSlot::Missing, false),
    };
    let slots = CleanupSlots {
        public,
        quarantine,
        operation: operation_slot,
        operation_exists,
    };
    let target_count = [slots.public, slots.quarantine, slots.operation]
        .into_iter()
        .filter(|slot| *slot == CleanupSlot::Target)
        .count();
    let placeholder_count = [slots.public, slots.quarantine, slots.operation]
        .into_iter()
        .filter(|slot| *slot == CleanupSlot::Placeholder)
        .count();
    if target_count > 1
        || placeholder_count > 1
        || slots.quarantine == CleanupSlot::Other
        || slots.operation == CleanupSlot::Other
    {
        return Err(os_error(libc::ESTALE));
    }
    Ok(slots)
}

fn open_bound_cleanup_directory(
    parent: RawFd,
    name: &CStr,
    expected: FileIdentity,
    expected_device: u64,
    expected_mode: Option<u32>,
) -> io::Result<OwnedFd> {
    match expected_mode {
        Some(mode) => {
            open_bound_cleanup_directory_modes(parent, name, expected, expected_device, &[mode])
        }
        None => open_bound_cleanup_directory_modes(parent, name, expected, expected_device, &[]),
    }
}

fn open_bound_cleanup_directory_modes(
    parent: RawFd,
    name: &CStr,
    expected: FileIdentity,
    expected_device: u64,
    expected_modes: &[u32],
) -> io::Result<OwnedFd> {
    let before = stat_at(parent, name)?;
    if file_type(before.st_mode) != libc::S_IFDIR
        || before.st_uid != effective_user_id()
        || before.st_dev as u64 != expected_device
        || FileIdentity::from_stat(&before) != expected
        || (!expected_modes.is_empty()
            && !expected_modes.contains(&(before.st_mode as u32 & 0o7777)))
    {
        return Err(os_error(libc::ESTALE));
    }
    let directory = open_directory_at(parent, name)?;
    let opened = stat_fd(directory.as_raw_fd())?;
    let rebound = stat_at(parent, name)?;
    if file_type(opened.st_mode) != libc::S_IFDIR
        || opened.st_uid != effective_user_id()
        || opened.st_dev as u64 != expected_device
        || FileIdentity::from_stat(&opened) != expected
        || file_type(rebound.st_mode) != libc::S_IFDIR
        || rebound.st_uid != effective_user_id()
        || rebound.st_dev as u64 != expected_device
        || FileIdentity::from_stat(&rebound) != expected
        || (!expected_modes.is_empty()
            && !expected_modes.contains(&(opened.st_mode as u32 & 0o7777)))
        || (!expected_modes.is_empty()
            && !expected_modes.contains(&(rebound.st_mode as u32 & 0o7777)))
        || !same_file(&before, &opened)
        || !same_file(&opened, &rebound)
    {
        return Err(os_error(libc::ESTALE));
    }
    Ok(directory)
}

fn remove_empty_cleanup_operation(
    namespace: &PrivateNamespace,
    operation: &mut PrivateOperation<'_>,
) -> io::Result<()> {
    operation.remove_empty_owned()?;
    operation.cleaned = true;
    cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })
}

fn remove_bound_cleanup_record(
    namespace: &PrivateNamespace,
    name: &CStr,
    file: &File,
    expected: FileIdentity,
) -> io::Result<()> {
    let opened = stat_fd(file.as_raw_fd())?;
    let current = stat_at(namespace.directory.as_raw_fd(), name)?;
    require_cleanup_record_metadata(&opened, namespace.identity.device)?;
    require_cleanup_record_metadata(&current, namespace.identity.device)?;
    if FileIdentity::from_stat(&opened) != expected
        || FileIdentity::from_stat(&current) != expected
        || !same_file(&opened, &current)
    {
        return Err(os_error(libc::ESTALE));
    }
    unlink_at(namespace.directory.as_raw_fd(), name, 0)?;
    cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })
}

fn finish_cleanup_records(
    namespace: &PrivateNamespace,
    loaded: &LoadedCleanupIntent,
    decision: Option<&LoadedCleanupDecision>,
) -> io::Result<()> {
    if let Some(decision) = decision {
        let decision_name = cleanup_decision_name(&loaded.intent.key_sha256)?;
        remove_bound_cleanup_record(namespace, &decision_name, &decision.file, decision.identity)?;
    }
    let intent_name = cleanup_intent_name(&loaded.intent.key_sha256)?;
    remove_bound_cleanup_record(namespace, &intent_name, &loaded.file, loaded.identity)
}

fn cleanup_placeholder_only(
    namespace: &PrivateNamespace,
    operation: &mut PrivateOperation<'_>,
    bindings: &CleanupIntentBindings,
) -> io::Result<()> {
    let placeholder = open_bound_cleanup_directory(
        operation.directory.as_raw_fd(),
        &bindings.placeholder,
        bindings.placeholder_identity,
        bindings.parent.device,
        Some(0o700),
    )?;
    if !directory_entries(placeholder.as_raw_fd())?.is_empty() {
        return Err(os_error(libc::ESTALE));
    }
    unlink_at(
        operation.directory.as_raw_fd(),
        &bindings.placeholder,
        libc::AT_REMOVEDIR,
    )?;
    cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
    remove_empty_cleanup_operation(namespace, operation)
}

fn resume_tree_cleanup(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
    component: &[u8],
    loaded: &LoadedCleanupIntent,
    verify_parent: &impl Fn() -> io::Result<()>,
    allow_injected_validation: bool,
) -> io::Result<()> {
    let bindings = validate_cleanup_intent(&loaded.intent, parent, component, namespace)?;
    let mut return_after_restore = None;
    loop {
        let mut operation = PrivateOperation::open_bound(
            namespace,
            &bindings.operation,
            bindings.operation_identity,
        )?;
        let decision = load_cleanup_decision(namespace, loaded, operation.as_ref())?;
        let slots =
            classify_cleanup_slots(namespace, public_parent, &bindings, operation.as_ref())?;
        match decision.as_ref().map(|decision| decision.record.decision) {
            None => {
                if slots.public == CleanupSlot::Missing
                    && slots.quarantine == CleanupSlot::Missing
                    && slots.operation == CleanupSlot::Missing
                    && !slots.operation_exists
                {
                    finish_cleanup_records(namespace, loaded, None)?;
                    return Ok(());
                }
                let operation = operation.as_ref().ok_or_else(|| os_error(libc::ESTALE))?;
                match (slots.public, slots.quarantine, slots.operation) {
                    (CleanupSlot::Target, CleanupSlot::Missing, CleanupSlot::Placeholder) => {
                        let target = open_bound_cleanup_directory_modes(
                            public_parent,
                            &bindings.component,
                            bindings.target,
                            bindings.parent.device,
                            &[bindings.original_mode, 0o700],
                        )?;
                        let current_mode = stat_fd(target.as_raw_fd())?.st_mode as u32 & 0o7777;
                        if current_mode != 0o700 {
                            chmod_fd(target.as_raw_fd(), 0o700)?;
                            cvt(unsafe { libc::fsync(target.as_raw_fd()) })?;
                            injected_cleanup_after_target_chmod_result()?;
                        }
                        if let Err(error) = verify_parent() {
                            publish_cleanup_decision(
                                namespace,
                                loaded,
                                operation,
                                CleanupDecisionV1::Restore,
                            )?;
                            return_after_restore = Some(error);
                            continue;
                        }
                        if let Err(error) = rename_no_replace(
                            public_parent,
                            &bindings.component,
                            namespace.directory.as_raw_fd(),
                            &bindings.quarantine,
                        ) {
                            let restoration = chmod_fd(
                                target.as_raw_fd(),
                                bindings.original_mode as libc::mode_t,
                            );
                            return restoration.and(Err(error));
                        }
                        cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })?;
                        cvt(unsafe { libc::fsync(public_parent) })?;
                    }
                    (CleanupSlot::Missing, CleanupSlot::Target, CleanupSlot::Placeholder) => {
                        drop(open_bound_cleanup_directory(
                            namespace.directory.as_raw_fd(),
                            &bindings.quarantine,
                            bindings.target,
                            bindings.parent.device,
                            Some(0o700),
                        )?);
                        let validation = verify_parent().and_then(|()| {
                            if allow_injected_validation {
                                injected_cleanup_validation_result()
                            } else {
                                Ok(())
                            }
                        });
                        match validation {
                            Ok(()) => {
                                publish_cleanup_decision(
                                    namespace,
                                    loaded,
                                    operation,
                                    CleanupDecisionV1::Delete,
                                )?;
                            }
                            Err(error) => {
                                publish_cleanup_decision(
                                    namespace,
                                    loaded,
                                    operation,
                                    CleanupDecisionV1::Restore,
                                )?;
                                return_after_restore = Some(error);
                            }
                        }
                    }
                    _ => return Err(os_error(libc::ESTALE)),
                }
            }
            Some(CleanupDecisionV1::Delete) => {
                if slots.public != CleanupSlot::Missing {
                    return Err(os_error(libc::ESTALE));
                }
                match (slots.quarantine, slots.operation, slots.operation_exists) {
                    (CleanupSlot::Target, CleanupSlot::Placeholder, true) => {
                        let target = open_bound_cleanup_directory(
                            namespace.directory.as_raw_fd(),
                            &bindings.quarantine,
                            bindings.target,
                            bindings.parent.device,
                            Some(0o700),
                        )?;
                        remove_acquired_directory_contents(target.as_raw_fd())?;
                        injected_cleanup_final_remove_result()?;
                        let operation = operation.as_mut().ok_or_else(|| os_error(libc::ESTALE))?;
                        capture_expected_entry(
                            operation,
                            namespace.directory.as_raw_fd(),
                            &bindings.quarantine,
                            &bindings.placeholder,
                            bindings.placeholder_identity,
                            bindings.target,
                            true,
                        )?;
                    }
                    (CleanupSlot::Placeholder, CleanupSlot::Target, true) => {
                        let operation = operation.as_mut().ok_or_else(|| os_error(libc::ESTALE))?;
                        let target = open_bound_cleanup_directory(
                            operation.directory.as_raw_fd(),
                            &bindings.placeholder,
                            bindings.target,
                            bindings.parent.device,
                            Some(0o700),
                        )?;
                        if !directory_entries(target.as_raw_fd())?.is_empty() {
                            return Err(os_error(libc::ESTALE));
                        }
                        unlink_at(
                            operation.directory.as_raw_fd(),
                            &bindings.placeholder,
                            libc::AT_REMOVEDIR,
                        )?;
                        cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
                    }
                    (CleanupSlot::Placeholder, CleanupSlot::Missing, true) => {
                        let operation = operation.as_mut().ok_or_else(|| os_error(libc::ESTALE))?;
                        remove_installed_placeholder(
                            operation,
                            namespace.directory.as_raw_fd(),
                            &bindings.quarantine,
                            &bindings.placeholder,
                            bindings.placeholder_identity,
                            true,
                        )?;
                        cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })?;
                    }
                    (CleanupSlot::Missing, CleanupSlot::Placeholder, true) => {
                        let operation = operation.as_mut().ok_or_else(|| os_error(libc::ESTALE))?;
                        cleanup_placeholder_only(namespace, operation, &bindings)?;
                    }
                    (CleanupSlot::Missing, CleanupSlot::Missing, true) => {
                        let operation = operation.as_mut().ok_or_else(|| os_error(libc::ESTALE))?;
                        remove_empty_cleanup_operation(namespace, operation)?;
                    }
                    (CleanupSlot::Missing, CleanupSlot::Missing, false) => {
                        finish_cleanup_records(namespace, loaded, decision.as_ref())?;
                        return Ok(());
                    }
                    _ => return Err(os_error(libc::ESTALE)),
                }
            }
            Some(CleanupDecisionV1::Restore) => {
                if slots.public == CleanupSlot::Other {
                    return Err(os_error(libc::ESTALE));
                }
                match (
                    slots.public,
                    slots.quarantine,
                    slots.operation,
                    slots.operation_exists,
                ) {
                    (CleanupSlot::Missing, CleanupSlot::Target, CleanupSlot::Placeholder, true) => {
                        let operation = operation.as_mut().ok_or_else(|| os_error(libc::ESTALE))?;
                        capture_expected_entry(
                            operation,
                            namespace.directory.as_raw_fd(),
                            &bindings.quarantine,
                            &bindings.placeholder,
                            bindings.placeholder_identity,
                            bindings.target,
                            true,
                        )?;
                    }
                    (CleanupSlot::Missing, CleanupSlot::Placeholder, CleanupSlot::Target, true) => {
                        let operation = operation.as_mut().ok_or_else(|| os_error(libc::ESTALE))?;
                        rename_no_replace(
                            operation.directory.as_raw_fd(),
                            &bindings.placeholder,
                            public_parent,
                            &bindings.component,
                        )?;
                        drop(open_bound_cleanup_directory(
                            public_parent,
                            &bindings.component,
                            bindings.target,
                            bindings.parent.device,
                            Some(0o700),
                        )?);
                        cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
                        cvt(unsafe { libc::fsync(public_parent) })?;
                    }
                    (CleanupSlot::Target, CleanupSlot::Placeholder, CleanupSlot::Missing, true) => {
                        let operation = operation.as_mut().ok_or_else(|| os_error(libc::ESTALE))?;
                        remove_installed_placeholder(
                            operation,
                            namespace.directory.as_raw_fd(),
                            &bindings.quarantine,
                            &bindings.placeholder,
                            bindings.placeholder_identity,
                            true,
                        )?;
                        cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })?;
                    }
                    (CleanupSlot::Target, CleanupSlot::Missing, CleanupSlot::Placeholder, true) => {
                        let operation = operation.as_mut().ok_or_else(|| os_error(libc::ESTALE))?;
                        cleanup_placeholder_only(namespace, operation, &bindings)?;
                    }
                    (CleanupSlot::Target, CleanupSlot::Missing, CleanupSlot::Missing, true) => {
                        let operation = operation.as_mut().ok_or_else(|| os_error(libc::ESTALE))?;
                        remove_empty_cleanup_operation(namespace, operation)?;
                    }
                    (CleanupSlot::Target, CleanupSlot::Missing, CleanupSlot::Missing, false) => {
                        let target = open_bound_cleanup_directory(
                            public_parent,
                            &bindings.component,
                            bindings.target,
                            bindings.parent.device,
                            Some(0o700),
                        )?;
                        chmod_fd(target.as_raw_fd(), bindings.original_mode as libc::mode_t)?;
                        cvt(unsafe { libc::fsync(target.as_raw_fd()) })?;
                        cvt(unsafe { libc::fsync(public_parent) })?;
                        finish_cleanup_records(namespace, loaded, decision.as_ref())?;
                        if let Some(error) = return_after_restore.take() {
                            return Err(error);
                        }
                        return Ok(());
                    }
                    _ => return Err(os_error(libc::ESTALE)),
                }
            }
        }
    }
}

fn inject_copy_private_cleanup_failure(_directory: RawFd) -> io::Result<()> {
    #[cfg(test)]
    if TEST_COPY_PRIVATE_CLEANUP_FAILURE.with(std::cell::Cell::get) {
        drop(create_regular_at(_directory, c"injected-cleanup-blocker")?);
    }
    Ok(())
}

fn identity_is_in_ancestry(identity: FileIdentity, directory: RawFd) -> io::Result<bool> {
    let mut current = duplicate_fd(directory)?;
    for _ in 0..256 {
        let current_identity = FileIdentity::from_stat(&stat_fd(current.as_raw_fd())?);
        if current_identity == identity {
            return Ok(true);
        }
        let parent = open_directory_at(current.as_raw_fd(), c"..")?;
        let parent_identity = FileIdentity::from_stat(&stat_fd(parent.as_raw_fd())?);
        if parent_identity == current_identity {
            return Ok(false);
        }
        current = parent;
    }
    Err(os_error(libc::ELOOP))
}

fn random_private_name(kind: &str) -> CString {
    CString::new(format!("{kind}-{}", uuid::Uuid::new_v4())).expect("UUID private name has no NUL")
}

fn is_random_private_name(name: &CStr, kind: &str) -> bool {
    let Ok(name) = name.to_str() else {
        return false;
    };
    let Some(value) = name
        .strip_prefix(kind)
        .and_then(|name| name.strip_prefix('-'))
    else {
        return false;
    };
    uuid::Uuid::parse_str(value).is_ok_and(|uuid| uuid.hyphenated().to_string() == value)
}

fn private_namespace_has_entries_at(parent: RawFd, expected_device: u64) -> io::Result<bool> {
    let initial = match stat_at(parent, PRIVATE_NAMESPACE_NAME) {
        Ok(initial) => initial,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    require_private_directory_on_device(&initial, expected_device)?;
    if initial.st_mode & 0o777 != 0o700 {
        return Err(os_error(libc::ESTALE));
    }
    let namespace = open_directory_at(parent, PRIVATE_NAMESPACE_NAME)?;
    let opened = stat_fd(namespace.as_raw_fd())?;
    require_private_directory_on_device(&opened, expected_device)?;
    if opened.st_mode & 0o777 != 0o700 || !same_file(&initial, &opened) {
        return Err(os_error(libc::ESTALE));
    }
    let has_entries = !directory_entries(namespace.as_raw_fd())?.is_empty();
    let current = stat_at(parent, PRIVATE_NAMESPACE_NAME)?;
    if !same_file(&opened, &current) {
        return Err(os_error(libc::ESTALE));
    }
    Ok(has_entries)
}

fn effective_user_id() -> libc::uid_t {
    // SAFETY: geteuid takes no arguments and has no memory-safety contract.
    unsafe { libc::geteuid() }
}

#[cfg(target_vendor = "apple")]
fn rename_no_replace(
    source_parent: RawFd,
    source_name: &CStr,
    destination_parent: RawFd,
    destination_name: &CStr,
) -> io::Result<()> {
    if let Some(error) =
        injected_rename_no_replace_error(source_parent, source_name, destination_parent)
    {
        return Err(error);
    }
    // SAFETY: both live directory descriptors and NUL-terminated components
    // remain valid for this non-retaining atomic rename.
    cvt(unsafe {
        libc::renameatx_np(
            source_parent,
            source_name.as_ptr(),
            destination_parent,
            destination_name.as_ptr(),
            libc::RENAME_EXCL,
        )
    })
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn rename_no_replace(
    source_parent: RawFd,
    source_name: &CStr,
    destination_parent: RawFd,
    destination_name: &CStr,
) -> io::Result<()> {
    if let Some(error) =
        injected_rename_no_replace_error(source_parent, source_name, destination_parent)
    {
        return Err(error);
    }
    // SAFETY: both live directory descriptors and NUL-terminated components
    // remain valid for this non-retaining atomic rename.
    cvt(unsafe {
        libc::renameat2(
            source_parent,
            source_name.as_ptr(),
            destination_parent,
            destination_name.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    })
}

#[cfg(not(any(target_vendor = "apple", target_os = "linux", target_os = "android")))]
fn rename_no_replace(
    source_parent: RawFd,
    source_name: &CStr,
    destination_parent: RawFd,
    destination_name: &CStr,
) -> io::Result<()> {
    if let Some(error) =
        injected_rename_no_replace_error(source_parent, source_name, destination_parent)
    {
        return Err(error);
    }
    unsupported_rename_no_replace(
        source_parent,
        source_name,
        destination_parent,
        destination_name,
    )
}

#[cfg(target_vendor = "apple")]
fn exchange_entries(
    left_parent: RawFd,
    left_name: &CStr,
    right_parent: RawFd,
    right_name: &CStr,
) -> io::Result<()> {
    #[cfg(test)]
    if AtomicRenameCapability::current() == AtomicRenameCapability::Unsupported {
        return Err(os_error(libc::ENOTSUP));
    }
    // SAFETY: both live directory descriptors and NUL-terminated components
    // remain valid for this non-retaining atomic exchange.
    cvt(unsafe {
        libc::renameatx_np(
            left_parent,
            left_name.as_ptr(),
            right_parent,
            right_name.as_ptr(),
            libc::RENAME_SWAP,
        )
    })
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn exchange_entries(
    left_parent: RawFd,
    left_name: &CStr,
    right_parent: RawFd,
    right_name: &CStr,
) -> io::Result<()> {
    #[cfg(test)]
    if AtomicRenameCapability::current() == AtomicRenameCapability::Unsupported {
        return Err(os_error(libc::ENOTSUP));
    }
    // SAFETY: both live directory descriptors and NUL-terminated components
    // remain valid for this non-retaining atomic exchange.
    cvt(unsafe {
        libc::renameat2(
            left_parent,
            left_name.as_ptr(),
            right_parent,
            right_name.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    })
}

#[cfg(not(any(target_vendor = "apple", target_os = "linux", target_os = "android")))]
fn exchange_entries(
    _left_parent: RawFd,
    _left_name: &CStr,
    _right_parent: RawFd,
    _right_name: &CStr,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic entry exchange is unavailable",
    ))
}

fn injected_rename_no_replace_error(
    _source_parent: RawFd,
    _source_name: &CStr,
    _destination_parent: RawFd,
) -> Option<io::Error> {
    #[cfg(test)]
    {
        if let Some(fault) = TEST_RENAME_NO_REPLACE_ERROR.with(std::cell::Cell::get) {
            let errno = match fault {
                RenameFault::Always(errno) => Some(errno),
                RenameFault::CrossDirectory(errno) if _source_parent != _destination_parent => {
                    Some(errno)
                }
                RenameFault::Directory(errno)
                    if stat_at(_source_parent, _source_name)
                        .is_ok_and(|metadata| file_type(metadata.st_mode) == libc::S_IFDIR) =>
                {
                    Some(errno)
                }
                RenameFault::CrossDirectory(_) | RenameFault::Directory(_) => None,
            };
            if let Some(errno) = errno {
                return Some(os_error(errno));
            }
        }
        if AtomicRenameCapability::current() == AtomicRenameCapability::Unsupported {
            return Some(os_error(libc::ENOTSUP));
        }
    }
    None
}

#[cfg(any(
    test,
    not(any(target_vendor = "apple", target_os = "linux", target_os = "android"))
))]
fn unsupported_rename_no_replace(
    _source_parent: RawFd,
    _source_name: &CStr,
    _destination_parent: RawFd,
    _destination_name: &CStr,
) -> io::Result<()> {
    Err(os_error(libc::ENOTSUP))
}

#[cfg(any(target_vendor = "apple", target_os = "linux", target_os = "android"))]
fn publish_regular_no_replace(
    source_parent: RawFd,
    source_name: &CStr,
    destination_parent: RawFd,
    destination_name: &CStr,
) -> io::Result<()> {
    rename_no_replace(
        source_parent,
        source_name,
        destination_parent,
        destination_name,
    )
}

#[cfg(not(any(target_vendor = "apple", target_os = "linux", target_os = "android")))]
fn publish_regular_no_replace(
    source_parent: RawFd,
    source_name: &CStr,
    destination_parent: RawFd,
    destination_name: &CStr,
) -> io::Result<()> {
    publish_regular_with_link_ops(
        source_parent,
        source_name,
        destination_parent,
        destination_name,
        |source_parent, source_name, destination_parent, destination_name| {
            link_at(
                source_parent,
                source_name,
                destination_parent,
                destination_name,
            )
        },
        |source_parent, source_name| unlink_at(source_parent, source_name, 0),
    )
}

#[cfg(any(
    test,
    not(any(target_vendor = "apple", target_os = "linux", target_os = "android"))
))]
fn publish_regular_with_link_ops(
    _source_parent: RawFd,
    _source_name: &CStr,
    _destination_parent: RawFd,
    _destination_name: &CStr,
    link: impl FnOnce(RawFd, &CStr, RawFd, &CStr) -> io::Result<()>,
    unlink: impl FnOnce(RawFd, &CStr) -> io::Result<()>,
) -> io::Result<()> {
    drop(link);
    drop(unlink);
    Err(os_error(libc::ENOTSUP))
}

fn copy_regular_bytes(
    source: OwnedFd,
    destination_parent: RawFd,
    destination_name: &CStr,
    mode: libc::mode_t,
) -> io::Result<()> {
    let destination = create_regular_at(destination_parent, destination_name)?;
    let mut source = File::from(source);
    let mut destination = File::from(destination);
    if let Err(error) = io::copy(&mut source, &mut destination) {
        drop(destination);
        let _ = unlink_at(destination_parent, destination_name, 0);
        return Err(error);
    }
    if let Err(error) = chmod_fd(destination.as_raw_fd(), mode) {
        drop(destination);
        let _ = unlink_at(destination_parent, destination_name, 0);
        return Err(error);
    }
    Ok(())
}

fn finish_created_regular(parent: RawFd, name: &CStr, mode: libc::mode_t) -> io::Result<()> {
    let descriptor = match open_regular_at(parent, name) {
        Ok(descriptor) => descriptor,
        Err(error) => {
            let _ = unlink_at(parent, name, 0);
            return Err(error);
        }
    };
    let metadata = stat_fd(descriptor.as_raw_fd())?;
    if file_type(metadata.st_mode) != libc::S_IFREG {
        drop(descriptor);
        let _ = unlink_at(parent, name, 0);
        return Err(invalid_type_error());
    }
    if let Err(error) = chmod_fd(descriptor.as_raw_fd(), mode) {
        drop(descriptor);
        let _ = unlink_at(parent, name, 0);
        return Err(error);
    }
    Ok(())
}

fn make_directory_read_only(directory: RawFd) -> io::Result<()> {
    for name in directory_entries(directory)? {
        let metadata = stat_at(directory, &name)?;
        match file_type(metadata.st_mode) {
            libc::S_IFDIR => {
                let child = open_verified_child_directory(directory, &name, &metadata)?;
                make_directory_read_only(child.as_raw_fd())?;
            }
            libc::S_IFREG => make_regular_read_only_with_hook(directory, &name, &metadata, || {})?,
            libc::S_IFLNK => {}
            _ => return Err(invalid_type_error()),
        }
    }
    chmod_fd(directory, 0o555)
}

fn make_regular_read_only_with_hook(
    parent: RawFd,
    name: &CStr,
    initial: &libc::stat,
    before_open: impl FnOnce(),
) -> io::Result<()> {
    if file_type(initial.st_mode) != libc::S_IFREG {
        return Err(invalid_type_error());
    }
    let parent_device = stat_fd(parent)?.st_dev;
    if initial.st_dev != parent_device {
        return Err(os_error(libc::EXDEV));
    }
    before_open();
    let file = open_regular_at(parent, name)?;
    let opened = stat_fd(file.as_raw_fd())?;
    if opened.st_dev != parent_device {
        return Err(os_error(libc::EXDEV));
    }
    if file_type(opened.st_mode) != libc::S_IFREG || !same_file(initial, &opened) {
        return Err(os_error(libc::ESTALE));
    }
    let mode = if opened.st_mode & 0o111 != 0 {
        0o555
    } else {
        0o444
    };
    chmod_fd(file.as_raw_fd(), mode)
}

// `directory` must already be inside a validated OperationDirectory. Caller
// pathnames are never passed to this destructive recursion.
fn remove_private_directory_contents(directory: RawFd) -> io::Result<()> {
    chmod_fd(directory, 0o700)?;
    for name in directory_entries(directory)? {
        let metadata = stat_at(directory, &name)?;
        let kind = file_type(metadata.st_mode);
        match kind {
            libc::S_IFDIR => {
                let child = open_verified_child_directory(directory, &name, &metadata)?;
                remove_private_directory_contents(child.as_raw_fd())?;
                unlink_at(directory, &name, libc::AT_REMOVEDIR)?;
            }
            libc::S_IFREG | libc::S_IFLNK => unlink_at(directory, &name, 0)?,
            _ => return Err(invalid_type_error()),
        }
    }
    Ok(())
}

// Once this function is entered, cleanup is intentionally non-transactional:
// any returned error describes a partially removed tree that remains acquired
// in the private namespace. Callers must not restore it or resume in Drop.
fn remove_acquired_directory_contents(directory: RawFd) -> io::Result<()> {
    chmod_fd(directory, 0o700)?;
    for name in directory_entries(directory)? {
        let metadata = stat_at(directory, &name)?;
        match file_type(metadata.st_mode) {
            libc::S_IFDIR => {
                let child = open_verified_child_directory(directory, &name, &metadata)?;
                remove_acquired_directory_contents(child.as_raw_fd())?;
                unlink_at(directory, &name, libc::AT_REMOVEDIR)?;
                injected_cleanup_after_removal_result()?;
            }
            libc::S_IFREG | libc::S_IFLNK => {
                unlink_at(directory, &name, 0)?;
                injected_cleanup_after_removal_result()?;
            }
            _ => return Err(invalid_type_error()),
        }
    }
    Ok(())
}

fn injected_cleanup_validation_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::AfterAcquisitionValidation(errno)) =
        TEST_CLEANUP_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_bootstrap_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::AfterCleanupBootstrap(errno)) =
        TEST_CLEANUP_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_placeholder_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::AfterCleanupPlaceholder(errno)) =
        TEST_CLEANUP_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_intent_write_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::DuringCleanupIntentWrite(errno)) =
        TEST_CLEANUP_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_intent_sync_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::AfterCleanupIntentSync(errno)) =
        TEST_CLEANUP_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_after_target_chmod_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::AfterTargetChmod(errno)) =
        TEST_CLEANUP_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_after_removal_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::AfterFirstRemoval(errno)) =
        TEST_CLEANUP_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_final_remove_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::BeforeFinalRootRemoval(errno)) =
        TEST_CLEANUP_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn open_verified_child_directory(
    parent: RawFd,
    name: &CStr,
    initial: &libc::stat,
) -> io::Result<OwnedFd> {
    if file_type(initial.st_mode) != libc::S_IFDIR {
        return Err(os_error(libc::ENOTDIR));
    }
    let parent_device = stat_fd(parent)?.st_dev;
    if initial.st_dev != parent_device {
        return Err(os_error(libc::EXDEV));
    }
    let child = open_directory_at(parent, name)?;
    let opened = stat_fd(child.as_raw_fd())?;
    if opened.st_dev != parent_device {
        return Err(os_error(libc::EXDEV));
    }
    if file_type(opened.st_mode) != libc::S_IFDIR || !same_file(initial, &opened) {
        return Err(os_error(libc::ESTALE));
    }
    Ok(child)
}

fn directory_entries(directory: RawFd) -> io::Result<Vec<CString>> {
    let current = c".";
    let iterator = open_directory_at(directory, current)?;
    let raw = iterator.into_raw_fd();
    // SAFETY: `raw` is a newly opened directory description with its own
    // offset. On success ownership transfers to DIR; on failure it is
    // reconstructed below.
    let stream = unsafe { libc::fdopendir(raw) };
    if stream.is_null() {
        let error = io::Error::last_os_error();
        // SAFETY: fdopendir failed and therefore did not consume `raw`.
        drop(unsafe { OwnedFd::from_raw_fd(raw) });
        return Err(error);
    }
    let stream = DirectoryStream(stream);
    let mut entries = Vec::new();
    loop {
        clear_errno();
        // SAFETY: `stream` owns a valid DIR and no concurrent call mutates it.
        let entry = unsafe { libc::readdir(stream.0) };
        if entry.is_null() {
            let error = current_errno();
            if error == 0 {
                break;
            }
            return Err(os_error(error));
        }
        // SAFETY: readdir returned a live dirent whose d_name is NUL-terminated
        // and remains valid until the next call; it is copied immediately.
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if name.to_bytes() != b"." && name.to_bytes() != b".." {
            entries.push(name.to_owned());
        }
    }
    Ok(entries)
}

struct DirectoryStream(*mut libc::DIR);

impl Drop for DirectoryStream {
    fn drop(&mut self) {
        // SAFETY: this wrapper uniquely owns the DIR returned by fdopendir.
        unsafe {
            libc::closedir(self.0);
        }
    }
}

fn chmod_fd(descriptor: RawFd, mode: libc::mode_t) -> io::Result<()> {
    // SAFETY: `descriptor` is live for this non-retaining call.
    cvt(unsafe { libc::fchmod(descriptor, mode) })
}

fn metadata_from_stat(metadata: libc::stat) -> io::Result<EntryMetadata> {
    let kind = match file_type(metadata.st_mode) {
        libc::S_IFREG => EntryKind::RegularFile,
        libc::S_IFLNK => EntryKind::Symlink,
        _ => return Err(invalid_type_error()),
    };
    let (modified_seconds, modified_nanoseconds) = modified_time(&metadata);
    Ok(EntryMetadata {
        kind,
        mode: metadata.st_mode as u32 & 0o7777,
        size: metadata.st_size as u64,
        device: metadata.st_dev as u64,
        inode: metadata.st_ino,
        modified_seconds,
        modified_nanoseconds,
    })
}

fn modified_time(metadata: &libc::stat) -> (i64, i64) {
    (metadata.st_mtime, metadata.st_mtime_nsec)
}

fn same_file(left: &libc::stat, right: &libc::stat) -> bool {
    left.st_dev == right.st_dev && left.st_ino == right.st_ino
}

fn file_type(mode: libc::mode_t) -> libc::mode_t {
    mode & libc::S_IFMT
}

fn cvt(result: libc::c_int) -> io::Result<()> {
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn require_fd_cloexec(descriptor: RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
    if flags == -1 {
        return Err(io::Error::last_os_error());
    }
    if flags & libc::FD_CLOEXEC == 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "internal descriptor is inheritable",
        ));
    }
    Ok(())
}

fn invalid_type_error() -> io::Error {
    os_error(libc::EINVAL)
}

fn os_error(errno: libc::c_int) -> io::Error {
    io::Error::from_raw_os_error(errno)
}

fn interior_nul_error(_: std::ffi::NulError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "path component contains NUL")
}

#[cfg(target_vendor = "apple")]
fn clear_errno() {
    // SAFETY: __error returns the calling thread's errno storage.
    unsafe {
        *libc::__error() = 0;
    }
}

#[cfg(target_vendor = "apple")]
fn current_errno() -> libc::c_int {
    // SAFETY: __error returns the calling thread's errno storage.
    unsafe { *libc::__error() }
}

#[cfg(not(target_vendor = "apple"))]
fn clear_errno() {
    // SAFETY: __errno_location returns the calling thread's errno storage on
    // the supported non-Apple Unix test targets.
    unsafe {
        *libc::__errno_location() = 0;
    }
}

#[cfg(not(target_vendor = "apple"))]
fn current_errno() -> libc::c_int {
    // SAFETY: __errno_location returns the calling thread's errno storage on
    // the supported non-Apple Unix test targets.
    unsafe { *libc::__errno_location() }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use std::{
        fs,
        io::Write,
        os::{
            fd::AsRawFd,
            unix::ffi::OsStrExt,
            unix::fs::{MetadataExt, PermissionsExt, symlink},
        },
        path::Path,
    };

    use super::{
        AtomicRenameCapability, AtomicRenameCapabilityOverride, CleanupFault, CleanupFaultOverride,
        CleanupIntentV1, CopyPrivateCleanupFailureOverride, PrivateNamespace,
        RenameNoReplaceOverride, RootedDir, copy_regular, copy_regular_with_clone,
        copy_regular_with_clone_and_publish, create_regular_at, link_at,
        make_regular_read_only_with_hook, open_directory_path, open_regular_at,
        open_verified_child_directory, publish_regular_with_link_ops, stat_at,
        unsupported_rename_no_replace,
    };
    use crate::inputs::RelativePath;

    #[derive(Clone, Copy, Debug)]
    enum UnsafePrivateReadReplacement {
        PermissiveRegular,
        HardlinkedRegular,
        Directory,
    }

    fn install_unsafe_private_read_replacement(
        root_path: &Path,
        replacement: UnsafePrivateReadReplacement,
    ) {
        let target = root_path.join("receipt.json");
        let staged = root_path.join("replacement.json");
        match replacement {
            UnsafePrivateReadReplacement::PermissiveRegular => {
                fs::write(&staged, b"replacement").unwrap();
                fs::set_permissions(&staged, fs::Permissions::from_mode(0o644)).unwrap();
                fs::rename(&staged, target).unwrap();
            }
            UnsafePrivateReadReplacement::HardlinkedRegular => {
                fs::write(&staged, b"replacement").unwrap();
                fs::set_permissions(&staged, fs::Permissions::from_mode(0o600)).unwrap();
                fs::hard_link(&staged, root_path.join("replacement-alias.json")).unwrap();
                fs::rename(&staged, target).unwrap();
            }
            UnsafePrivateReadReplacement::Directory => {
                fs::create_dir(&staged).unwrap();
                fs::set_permissions(&staged, fs::Permissions::from_mode(0o700)).unwrap();
                fs::write(staged.join("sentinel"), b"replacement").unwrap();
                fs::remove_file(&target).unwrap();
                fs::rename(&staged, target).unwrap();
            }
        }
    }

    fn assert_unsafe_private_read_replacement_preserved(
        root_path: &Path,
        replacement: UnsafePrivateReadReplacement,
    ) {
        let target = root_path.join("receipt.json");
        match replacement {
            UnsafePrivateReadReplacement::PermissiveRegular => {
                assert_eq!(fs::read(&target).unwrap(), b"replacement");
                assert_eq!(
                    fs::metadata(&target).unwrap().permissions().mode() & 0o777,
                    0o644
                );
            }
            UnsafePrivateReadReplacement::HardlinkedRegular => {
                let alias = root_path.join("replacement-alias.json");
                assert_eq!(fs::read(&target).unwrap(), b"replacement");
                assert_eq!(fs::read(&alias).unwrap(), b"replacement");
                assert_eq!(fs::metadata(&target).unwrap().nlink(), 2);
            }
            UnsafePrivateReadReplacement::Directory => {
                assert!(target.is_dir());
                assert_eq!(fs::read(target.join("sentinel")).unwrap(), b"replacement");
            }
        }
    }

    #[test]
    fn clone_fallback_replaces_only_its_owned_partial_before_byte_copy() {
        // Catches byte fallback publishing a partial clone or leaving its
        // operation-owned temporary directory behind.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let source_path = physical.join("source");
        let destination_path = physical.join("destination");
        fs::create_dir(&source_path).unwrap();
        fs::write(source_path.join("value"), b"source bytes\n").unwrap();
        fs::set_permissions(source_path.join("value"), fs::Permissions::from_mode(0o755)).unwrap();
        let source_root = RootedDir::open(&source_path).unwrap();
        let destination_root = RootedDir::create(&destination_path).unwrap();
        let selected = RelativePath::parse(b"value").unwrap();
        let (source_parent, source_name) = source_root.open_parent(&selected, false).unwrap();
        let source = open_regular_at(source_parent.as_raw_fd(), &source_name).unwrap();
        let (destination_parent, destination_name) =
            destination_root.open_parent(&selected, true).unwrap();

        copy_regular_with_clone(
            source,
            &destination_parent,
            &destination_name,
            0o555,
            |_source, temporary_parent, temporary_name| {
                let mut partial = std::fs::File::from(
                    create_regular_at(temporary_parent, temporary_name).unwrap(),
                );
                partial.write_all(b"partial clone").unwrap();
                drop(partial);
                Err(std::io::Error::from_raw_os_error(libc::ENOTSUP))
            },
        )
        .unwrap();

        assert_eq!(
            fs::read(destination_path.join("value")).unwrap(),
            b"source bytes\n"
        );
        assert_eq!(
            fs::metadata(destination_path.join("value"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o555
        );
        assert_eq!(fs::read_dir(&destination_path).unwrap().count(), 1);
    }

    #[test]
    fn clone_fallback_never_removes_a_preexisting_final_destination() {
        // Catches treating an unrelated final regular file as a partial clone
        // after a fallback errno and unlinking it before byte copy.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let source_path = physical.join("source");
        let destination_path = physical.join("destination");
        fs::create_dir(&source_path).unwrap();
        fs::write(source_path.join("value"), b"source bytes\n").unwrap();
        let source_root = RootedDir::open(&source_path).unwrap();
        let destination_root = RootedDir::create(&destination_path).unwrap();
        fs::write(destination_path.join("value"), b"keep existing\n").unwrap();
        let selected = RelativePath::parse(b"value").unwrap();
        let (source_parent, source_name) = source_root.open_parent(&selected, false).unwrap();
        let source = open_regular_at(source_parent.as_raw_fd(), &source_name).unwrap();
        let (destination_parent, destination_name) =
            destination_root.open_parent(&selected, true).unwrap();

        let error = copy_regular_with_clone(
            source,
            &destination_parent,
            &destination_name,
            0o444,
            |_source, temporary_parent, temporary_name| {
                let mut partial = std::fs::File::from(
                    create_regular_at(temporary_parent, temporary_name).unwrap(),
                );
                partial.write_all(b"partial clone").unwrap();
                drop(partial);
                Err(std::io::Error::from_raw_os_error(libc::EINVAL))
            },
        )
        .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::EEXIST));
        assert_eq!(
            fs::read(destination_path.join("value")).unwrap(),
            b"keep existing\n"
        );
        assert_eq!(fs::read_dir(&destination_path).unwrap().count(), 1);
    }

    #[test]
    fn clone_reads_the_opened_source_after_its_name_becomes_an_outside_symlink() {
        // Catches clonefileat resolving source_name again after the regular
        // source descriptor was opened and identity-verified.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let source_path = physical.join("source");
        let destination_path = physical.join("destination");
        let outside_path = physical.join("outside.txt");
        fs::create_dir(&source_path).unwrap();
        fs::write(source_path.join("value"), b"opened source\n").unwrap();
        fs::write(&outside_path, b"outside bytes\n").unwrap();
        let source_root = RootedDir::open(&source_path).unwrap();
        let destination_root = RootedDir::create(&destination_path).unwrap();
        let selected = RelativePath::parse(b"value").unwrap();
        let (source_parent, source_name) = source_root.open_parent(&selected, false).unwrap();
        let source = open_regular_at(source_parent.as_raw_fd(), &source_name).unwrap();
        let (destination_parent, destination_name) =
            destination_root.open_parent(&selected, true).unwrap();
        fs::rename(source_path.join("value"), source_path.join("moved-value")).unwrap();
        std::os::unix::fs::symlink(&outside_path, source_path.join("value")).unwrap();

        copy_regular(source, &destination_parent, &destination_name, 0o444).unwrap();

        assert_eq!(
            fs::read(destination_path.join("value")).unwrap(),
            b"opened source\n"
        );
        assert_eq!(fs::read(&outside_path).unwrap(), b"outside bytes\n");
    }

    #[test]
    fn publication_rejects_a_child_directory_swapped_after_initial_stat() {
        // Catches recursively chmodding a replacement directory opened after
        // fstatat observed the originally owned child.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let root_path = physical.join("root");
        let outside_path = physical.join("outside");
        fs::create_dir(&root_path).unwrap();
        fs::create_dir(&outside_path).unwrap();
        fs::create_dir(root_path.join("child")).unwrap();
        fs::write(outside_path.join("sentinel.txt"), b"outside\n").unwrap();
        let root = RootedDir::open(&root_path).unwrap();
        let child = std::ffi::CString::new("child").unwrap();
        let initial = stat_at(root.root.as_raw_fd(), &child).unwrap();
        fs::rename(root_path.join("child"), root_path.join("moved-child")).unwrap();
        fs::rename(&outside_path, root_path.join("child")).unwrap();

        let error =
            open_verified_child_directory(root.root.as_raw_fd(), &child, &initial).unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(
            fs::read(root_path.join("child/sentinel.txt")).unwrap(),
            b"outside\n"
        );
        assert_eq!(
            fs::metadata(root_path.join("child"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
        assert_eq!(
            fs::metadata(root_path.join("moved-child"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
    }

    #[test]
    fn publication_regular_chmod_rejects_a_swap_after_stat_before_open() {
        // Catches chmodding a replacement regular file opened after fstatat
        // observed the originally owned file.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let root_path = physical.join("root");
        let outside_path = physical.join("outside.txt");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned\n").unwrap();
        fs::write(&outside_path, b"outside\n").unwrap();
        fs::set_permissions(root_path.join("value"), fs::Permissions::from_mode(0o644)).unwrap();
        fs::set_permissions(&outside_path, fs::Permissions::from_mode(0o600)).unwrap();
        let root = RootedDir::open(&root_path).unwrap();
        let value = std::ffi::CString::new("value").unwrap();
        let initial = stat_at(root.root.as_raw_fd(), &value).unwrap();

        let error =
            make_regular_read_only_with_hook(root.root.as_raw_fd(), &value, &initial, || {
                fs::rename(root_path.join("value"), root_path.join("moved-value")).unwrap();
                fs::rename(&outside_path, root_path.join("value")).unwrap();
            })
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(
            fs::metadata(root_path.join("value"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(root_path.join("moved-value"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o644
        );
    }

    #[test]
    fn private_deletion_boundary_is_not_replaceable_from_the_caller_namespace() {
        // Catches exposing a final deletion pathname below the caller's root.
        // Once the opened root has been atomically moved and validated in the
        // private namespace, a caller replacement at the old name is inert.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned\n").unwrap();
        let root = RootedDir::open(&root_path).unwrap();

        root.remove_owned_tree_with_hook(|| {
            assert!(!root_path.exists());
            assert!(!fs::read_dir(&physical).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".mac-worker-delete-")
            }));
            fs::create_dir(&root_path).unwrap();
            fs::write(root_path.join("replacement"), b"replacement\n").unwrap();
        })
        .unwrap();

        assert_eq!(
            fs::read(root_path.join("replacement")).unwrap(),
            b"replacement\n"
        );
    }

    #[test]
    fn failed_private_acquisition_never_drops_an_unvalidated_replacement() {
        // Catches best-effort private-directory cleanup deleting a replacement
        // that was moved at the acquisition boundary but failed identity
        // validation and could not be restored to an reoccupied caller name.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let root_path = physical.join("root");
        let replacement_path = physical.join("replacement");
        let moved_owned = physical.join("moved-owned");
        fs::create_dir(&root_path).unwrap();
        fs::create_dir(&replacement_path).unwrap();
        fs::write(root_path.join("owned"), b"owned\n").unwrap();
        fs::write(
            replacement_path.join("sentinel"),
            b"unvalidated replacement\n",
        )
        .unwrap();
        let root = RootedDir::open(&root_path).unwrap();
        let private_boundary = std::cell::RefCell::new(None);

        let error = root
            .remove_owned_tree_with_hooks(
                || {
                    fs::rename(&root_path, &moved_owned).unwrap();
                    fs::rename(&replacement_path, &root_path).unwrap();
                },
                || {
                    fs::create_dir(&root_path).unwrap();
                    fs::write(root_path.join("blocker"), b"block restore\n").unwrap();
                    let boundary = fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                        .unwrap()
                        .map(|entry| entry.unwrap().path())
                        .find(|path| {
                            fs::read(path.join("sentinel"))
                                .is_ok_and(|bytes| bytes == b"unvalidated replacement\n")
                        })
                        .unwrap();
                    private_boundary.replace(Some(boundary));
                },
                || {},
                || {},
                || {},
            )
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        let private_boundary = private_boundary.into_inner().unwrap();
        assert_eq!(
            fs::read(private_boundary.join("sentinel")).unwrap(),
            b"unvalidated replacement\n"
        );
        assert_eq!(
            fs::read(root_path.join("blocker")).unwrap(),
            b"block restore\n"
        );
        assert_eq!(fs::read(moved_owned.join("owned")).unwrap(), b"owned\n");

        fs::rename(&private_boundary, physical.join("recovered-replacement")).unwrap();
        fs::remove_dir(physical.join(".mac-worker-rooted-fs")).unwrap();
    }

    #[test]
    fn unsupported_public_cleanup_preserves_root_mode_and_contents() {
        // Catches the public destructive path chmodding its root before the
        // generic-Unix capability decision returns ENOTSUP.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"preserve\n").unwrap();
        fs::set_permissions(root_path.join("value"), fs::Permissions::from_mode(0o444)).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o555)).unwrap();
        let root = RootedDir::open(&root_path).unwrap();
        let _capability = AtomicRenameCapabilityOverride::set(AtomicRenameCapability::Unsupported);

        let error = root.remove_owned_tree().unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ENOTSUP));
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"preserve\n");
        assert_eq!(
            fs::metadata(root_path.join("value"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o444
        );
        assert_eq!(
            fs::metadata(&root_path).unwrap().permissions().mode() & 0o777,
            0o555
        );
    }

    #[test]
    fn unsupported_public_copy_fails_before_destination_mutation() {
        // Catches generic Unix entering materialization/link publication even
        // though it cannot atomically consume an identity-bound staging name.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let source_path = physical.join("source");
        let destination_path = physical.join("destination");
        fs::create_dir(&source_path).unwrap();
        fs::create_dir(&destination_path).unwrap();
        fs::write(source_path.join("value"), b"source bytes\n").unwrap();
        fs::write(destination_path.join("sentinel"), b"preserve\n").unwrap();
        let source = RootedDir::open(&source_path).unwrap();
        let destination = RootedDir::open(&destination_path).unwrap();
        let _capability = AtomicRenameCapabilityOverride::set(AtomicRenameCapability::Unsupported);

        let error = source
            .copy_regular_to(&RelativePath::parse(b"value").unwrap(), &destination)
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ENOTSUP));
        assert_eq!(
            fs::read(destination_path.join("sentinel")).unwrap(),
            b"preserve\n"
        );
        assert_eq!(fs::read_dir(&destination_path).unwrap().count(), 1);
    }

    #[test]
    fn copy_rejects_an_operation_parent_inside_the_caller_root_before_mutation() {
        // Catches treating /private/tmp as private when it is itself the
        // caller-supplied rooted namespace.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let source_path = physical.join("source");
        fs::create_dir(&source_path).unwrap();
        let name = format!("mac-worker-boundary-test-{}", uuid::Uuid::new_v4());
        fs::write(source_path.join(&name), b"source bytes\n").unwrap();
        let source = RootedDir::open(&source_path).unwrap();
        let destination = RootedDir::open(Path::new("/private/tmp")).unwrap();
        let selected = RelativePath::parse(name.as_bytes()).unwrap();

        let result = source.copy_regular_to(&selected, &destination);
        let final_path = Path::new("/private/tmp").join(&name);
        if final_path.exists() {
            fs::remove_file(&final_path).unwrap();
        }
        let error = result.unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ENOTSUP));
        assert!(!final_path.exists());
    }

    #[test]
    fn copy_derives_a_namespace_outside_the_source_root() {
        // Catches retaining the old /private/tmp binding when the source root
        // contains that directory. The destination-derived namespace is
        // physically disjoint, so the cross-root byte copy remains valid.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let destination_path = physical.join("destination");
        fs::create_dir(&destination_path).unwrap();
        let name = format!("mac-worker-source-boundary-test-{}", uuid::Uuid::new_v4());
        let source_path = Path::new("/private/tmp").join(&name);
        fs::write(&source_path, b"source bytes\n").unwrap();
        let source = RootedDir::open(Path::new("/private/tmp")).unwrap();
        let destination = RootedDir::open(&destination_path).unwrap();
        let selected = RelativePath::parse(name.as_bytes()).unwrap();

        source.copy_regular_to(&selected, &destination).unwrap();
        let final_path = destination_path.join(&name);
        assert_eq!(fs::read(&final_path).unwrap(), b"source bytes\n");
        assert!(physical.join(".mac-worker-rooted-fs").is_dir());
        fs::remove_file(&source_path).unwrap();
    }

    #[test]
    fn source_on_another_device_does_not_disqualify_the_destination_namespace() {
        // Catches conflating the destination workspace's same-device
        // requirement with the source root, which byte fallback may read
        // across devices.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let destination_parent = open_directory_path(&physical).unwrap();
        let destination_device = super::stat_fd(destination_parent.as_raw_fd())
            .unwrap()
            .st_dev;
        let source = open_directory_path(Path::new("/dev")).unwrap();
        let source_stat = super::stat_fd(source.as_raw_fd()).unwrap();
        assert_ne!(source_stat.st_dev, destination_device);

        let namespace = PrivateNamespace::select(
            destination_parent.as_raw_fd(),
            destination_device,
            &[super::DirectoryIdentity {
                descriptor: source.as_raw_fd(),
                identity: super::FileIdentity::from_stat(&source_stat),
            }],
        )
        .unwrap();

        assert_eq!(
            super::stat_fd(namespace.directory.as_raw_fd())
                .unwrap()
                .st_dev,
            destination_device
        );
    }

    #[test]
    fn public_copy_derives_a_private_namespace_beside_the_xdg_destination() {
        // Catches hard-binding copy materialization to /private/tmp instead
        // of deriving a same-filesystem private namespace from the XDG cache
        // layout that owns the destination root.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let source_path = physical.join("source");
        let snapshots_path = physical.join("xdg-cache/mac-worker/snapshots");
        let destination_path = snapshots_path.join("build");
        fs::create_dir(&source_path).unwrap();
        fs::create_dir_all(&snapshots_path).unwrap();
        fs::write(source_path.join("value"), b"source bytes\n").unwrap();
        let source = RootedDir::open(&source_path).unwrap();
        let destination = RootedDir::create(&destination_path).unwrap();

        source
            .copy_regular_to(&RelativePath::parse(b"value").unwrap(), &destination)
            .unwrap();

        let namespace_path = snapshots_path.join(".mac-worker-rooted-fs");
        let metadata = fs::metadata(&namespace_path).unwrap();
        assert!(metadata.is_dir());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
        assert_eq!(fs::read_dir(namespace_path).unwrap().count(), 0);
        assert_eq!(
            fs::read(destination_path.join("value")).unwrap(),
            b"source bytes\n"
        );
    }

    #[test]
    fn private_namespace_rejects_a_candidate_on_another_device() {
        // Deterministic alternate-device coverage: the namespace constructor
        // must reject a candidate filesystem that differs from the target.
        // The paired public-copy test above proves normal selection starts
        // from the XDG destination rather than from this global candidate.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let target_parent = open_directory_path(&physical).unwrap();
        let target_device = super::stat_fd(target_parent.as_raw_fd()).unwrap().st_dev;
        let alternate = open_directory_path(Path::new("/dev")).unwrap();
        assert_ne!(
            super::stat_fd(alternate.as_raw_fd()).unwrap().st_dev,
            target_device
        );

        let error = match PrivateNamespace::open_or_create_at(&alternate, target_device) {
            Ok(_) => panic!("alternate-device namespace unexpectedly succeeded"),
            Err(error) => error,
        };

        assert_eq!(error.raw_os_error(), Some(libc::EXDEV));
    }

    #[test]
    fn runtime_rename_probe_failure_precedes_public_copy_mutation() {
        // Catches compile-target capability detection allowing destination
        // parents to be materialized before the target filesystem rejects the
        // first real atomic no-replace rename.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let source_path = physical.join("source");
        let destination_path = physical.join("destination");
        fs::create_dir(&source_path).unwrap();
        fs::create_dir(&destination_path).unwrap();
        fs::create_dir(source_path.join("nested")).unwrap();
        fs::write(source_path.join("nested/value"), b"source bytes\n").unwrap();
        fs::write(destination_path.join("sentinel"), b"preserve\n").unwrap();
        let source = RootedDir::open(&source_path).unwrap();
        let destination = RootedDir::open(&destination_path).unwrap();
        let _rename = RenameNoReplaceOverride::fail_with(libc::ENOTSUP);

        let error = source
            .copy_regular_to(&RelativePath::parse(b"nested/value").unwrap(), &destination)
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ENOTSUP));
        assert_eq!(
            fs::read(destination_path.join("sentinel")).unwrap(),
            b"preserve\n"
        );
        assert!(!destination_path.join("nested").exists());
        let namespace_path = physical.join(".mac-worker-rooted-fs");
        assert_eq!(fs::read_dir(namespace_path).unwrap().count(), 0);
    }

    #[test]
    fn runtime_rename_probe_failure_precedes_public_cleanup_mutation() {
        // Catches chmodding or acquiring the cleanup root before the exact
        // target filesystem has accepted an atomic no-replace rename probe.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"preserve\n").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o555)).unwrap();
        let root = RootedDir::open(&root_path).unwrap();
        let _rename = RenameNoReplaceOverride::fail_with(libc::ENOTSUP);

        let error = root.remove_owned_tree().unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ENOTSUP));
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"preserve\n");
        assert_eq!(
            fs::metadata(&root_path).unwrap().permissions().mode() & 0o777,
            0o555
        );
        let namespace_path = physical.join(".mac-worker-rooted-fs");
        assert_eq!(fs::read_dir(namespace_path).unwrap().count(), 0);
    }

    #[test]
    fn cross_directory_rename_probe_failure_precedes_public_copy_mutation() {
        // Catches probing only a same-directory rename even though commit
        // crosses from a private operation directory into another directory.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let source_path = physical.join("source");
        let destination_path = physical.join("destination");
        fs::create_dir_all(source_path.join("nested")).unwrap();
        fs::create_dir(&destination_path).unwrap();
        fs::write(source_path.join("nested/value"), b"source bytes\n").unwrap();
        let source = RootedDir::open(&source_path).unwrap();
        let destination = RootedDir::open(&destination_path).unwrap();
        let _rename = RenameNoReplaceOverride::fail_cross_directory_with(libc::ENOTSUP);

        let error = source
            .copy_regular_to(&RelativePath::parse(b"nested/value").unwrap(), &destination)
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ENOTSUP));
        assert!(!destination_path.join("nested").exists());
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn directory_rename_probe_failure_precedes_public_cleanup_mutation() {
        // Catches probing only regular files even though cleanup acquires a
        // directory. A chmod-and-restore cycle is caller-visible through ctime.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"preserve\n").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o555)).unwrap();
        let root = RootedDir::open(&root_path).unwrap();
        let before = super::stat_fd(root.root.as_raw_fd()).unwrap();
        let _rename = RenameNoReplaceOverride::fail_directory_with(libc::ENOTSUP);

        let error = root.remove_owned_tree().unwrap_err();

        let after = super::stat_fd(root.root.as_raw_fd()).unwrap();
        assert_eq!(error.raw_os_error(), Some(libc::ENOTSUP));
        assert_eq!(
            (after.st_ctime, after.st_ctime_nsec),
            (before.st_ctime, before.st_ctime_nsec)
        );
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"preserve\n");
        assert_eq!(
            fs::metadata(root_path).unwrap().permissions().mode() & 0o777,
            0o555
        );
    }

    #[test]
    fn successful_copy_leaves_no_caller_visible_operation_entry() {
        // Catches successful publication leaving copy or staging residue in
        // the caller's destination namespace.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let source_path = physical.join("source");
        let destination_path = physical.join("destination");
        fs::create_dir(&source_path).unwrap();
        fs::create_dir(&destination_path).unwrap();
        fs::write(source_path.join("value"), b"portable bytes\n").unwrap();
        let source_parent = open_directory_path(&source_path).unwrap();
        let source = open_regular_at(source_parent.as_raw_fd(), c"value").unwrap();
        let destination_parent = open_directory_path(&destination_path).unwrap();

        copy_regular_with_clone_and_publish(
            source,
            &destination_parent,
            c"value",
            0o444,
            |_source, _temporary_parent, _temporary_name| {
                Err(std::io::Error::from_raw_os_error(libc::ENOTSUP))
            },
            |source_parent, source_name, destination_parent, destination_name| {
                super::publish_regular_no_replace(
                    source_parent,
                    source_name,
                    destination_parent,
                    destination_name,
                )
            },
        )
        .unwrap();

        assert_eq!(
            fs::read(destination_path.join("value")).unwrap(),
            b"portable bytes\n"
        );
        assert_eq!(
            fs::metadata(destination_path.join("value"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o444
        );
        assert_eq!(
            fs::read_dir(&destination_path)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<Vec<_>>(),
            vec![std::ffi::OsString::from("value")]
        );
    }

    #[test]
    fn private_cleanup_failure_happens_before_final_publication() {
        // Catches publishing the final destination and only then discovering
        // that the private materialization directory cannot be removed. An
        // error must not leave the caller with an unexpectedly committed file.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let source_path = physical.join("source");
        let destination_path = physical.join("destination");
        fs::create_dir(&source_path).unwrap();
        fs::create_dir(&destination_path).unwrap();
        fs::write(source_path.join("value"), b"source bytes\n").unwrap();
        let source_parent = open_directory_path(&source_path).unwrap();
        let source = open_regular_at(source_parent.as_raw_fd(), c"value").unwrap();
        let destination_parent = open_directory_path(&destination_path).unwrap();

        let error = copy_regular_with_clone_and_publish(
            source,
            &destination_parent,
            c"value",
            0o444,
            |_source, temporary_parent, _temporary_name| {
                drop(create_regular_at(temporary_parent, c"cleanup-blocker").unwrap());
                Err(std::io::Error::from_raw_os_error(libc::ENOTSUP))
            },
            super::publish_regular_no_replace,
        )
        .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ENOTEMPTY));
        assert!(!destination_path.join("value").exists());
        assert_eq!(fs::read_dir(destination_path).unwrap().count(), 0);
    }

    #[test]
    fn public_copy_cleanup_failure_does_not_cross_the_commit_point() {
        // Public-path counterpart to the boundary test above. The injected
        // entry makes the real private-directory removal fail; publication
        // must not happen, and pre-commit cleanup must remove operation data.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let source_path = physical.join("source");
        let destination_path = physical.join("destination");
        fs::create_dir(&source_path).unwrap();
        fs::create_dir(&destination_path).unwrap();
        fs::write(source_path.join("value"), b"source bytes\n").unwrap();
        let source = RootedDir::open(&source_path).unwrap();
        let destination = RootedDir::open(&destination_path).unwrap();
        let _failure = CopyPrivateCleanupFailureOverride::set();

        let error = source
            .copy_regular_to(&RelativePath::parse(b"value").unwrap(), &destination)
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ENOTEMPTY));
        assert!(!destination_path.join("value").exists());
        let namespace_path = physical.join(".mac-worker-rooted-fs");
        assert_eq!(fs::read_dir(namespace_path).unwrap().count(), 0);
    }

    #[test]
    fn public_cleanup_restores_the_root_after_acquisition_validation_fails() {
        // Catches treating a just-acquired but not yet validated root as
        // disposable operation data. Before recursion, the original name,
        // bytes, and mode must be restored.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned bytes\n").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o555)).unwrap();
        let root = RootedDir::open(&root_path).unwrap();
        let _fault =
            CleanupFaultOverride::set(CleanupFault::AfterAcquisitionValidation(libc::ESTALE));

        let error = root.remove_owned_tree().unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"owned bytes\n");
        assert_eq!(
            fs::metadata(&root_path).unwrap().permissions().mode() & 0o777,
            0o555
        );
        let namespace_path = physical.join(".mac-worker-rooted-fs");
        assert_eq!(fs::read_dir(namespace_path).unwrap().count(), 0);
    }

    #[test]
    fn public_cleanup_reports_partial_deletion_without_resuming_in_drop() {
        // Catches Drop continuing destructive recursion after the public call
        // has already encountered an error. Once recursion starts, the root
        // stays privately acquired and its remaining entries stay untouched.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let root_path = physical.join("root");
        let outside_path = physical.join("outside");
        fs::create_dir(&root_path).unwrap();
        fs::create_dir(&outside_path).unwrap();
        fs::write(root_path.join("first"), b"first\n").unwrap();
        fs::write(root_path.join("second"), b"second\n").unwrap();
        fs::write(outside_path.join("sentinel"), b"outside\n").unwrap();
        std::os::unix::fs::symlink(&outside_path, root_path.join("outside-link")).unwrap();
        let root = RootedDir::open(&root_path).unwrap();
        let _fault = CleanupFaultOverride::set(CleanupFault::AfterFirstRemoval(libc::EIO));

        let error = root.remove_owned_tree().unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::EIO), "{error:?}");
        assert!(!root_path.exists());
        assert_eq!(
            fs::read(outside_path.join("sentinel")).unwrap(),
            b"outside\n"
        );
        let namespace_path = physical.join(".mac-worker-rooted-fs");
        let acquired = fs::read_dir(&namespace_path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(acquired.len(), 4);
        assert!(acquired.iter().any(|path| {
            path.file_name()
                .unwrap()
                .as_bytes()
                .starts_with(b"cleanup-intent-v1-")
        }));
        assert!(acquired.iter().any(|path| {
            path.file_name()
                .unwrap()
                .as_bytes()
                .starts_with(b"cleanup-decision-v1-")
        }));
        assert!(acquired.iter().any(|path| {
            path.file_name()
                .unwrap()
                .as_bytes()
                .starts_with(b"cleanup-op-v1-")
        }));
        let quarantine = acquired
            .iter()
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .as_bytes()
                    .starts_with(b"cleanup-tree-v1-")
            })
            .unwrap();
        assert!(fs::read_dir(quarantine).unwrap().count() >= 1);
        fs::remove_dir_all(&namespace_path).unwrap();
    }

    #[test]
    fn cleanup_intent_tree_retry_after_first_removal() {
        // Catches retry resolving only the now-absent public component instead
        // of the durable identity-bound cleanup intent.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(root_path.join("first"), b"first\n").unwrap();
        fs::write(root_path.join("second"), b"second\n").unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let fault = CleanupFaultOverride::set(CleanupFault::AfterFirstRemoval(libc::EIO));

        let error = parent.remove_owned_child("root").unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EIO), "{error:?}");
        drop(fault);
        drop(parent);

        let namespace_path = physical.join(".mac-worker-rooted-fs");
        assert!(fs::read_dir(&namespace_path).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .as_bytes()
                .starts_with(b"cleanup-intent-v1-")
        }));
        let parent = RootedDir::open(&physical).unwrap();
        parent.remove_owned_child("root").unwrap();

        assert!(!root_path.exists());
        assert_eq!(fs::read_dir(namespace_path).unwrap().count(), 0);
    }

    #[test]
    fn cleanup_residue_probe_covers_nested_adjacent_and_direct_quarantines() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        let root = RootedDir::open(&root_path).unwrap();

        assert!(!root.has_private_cleanup_residue().unwrap());

        let nested = root_path.join(".mac-worker-rooted-fs");
        fs::create_dir(&nested).unwrap();
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(!root.has_private_cleanup_residue().unwrap());
        let nested_cleanup = nested.join("cleanup-303f0f4a-6b5c-4d8e-9f00-112233445566");
        fs::create_dir(&nested_cleanup).unwrap();
        assert!(root.has_private_cleanup_residue().unwrap());
        fs::remove_dir(&nested_cleanup).unwrap();
        fs::remove_dir(&nested).unwrap();

        let direct = root_path.join("remove-303f0f4a-6b5c-4d8e-9f00-112233445566");
        fs::write(&direct, b"quarantined").unwrap();
        assert!(root.has_private_cleanup_residue().unwrap());
        fs::remove_file(&direct).unwrap();
        fs::write(root_path.join("remove-not-an-operation"), b"ordinary").unwrap();
        assert!(!root.has_private_cleanup_residue().unwrap());

        let adjacent = physical.join(".mac-worker-rooted-fs");
        fs::create_dir(&adjacent).unwrap();
        fs::set_permissions(&adjacent, fs::Permissions::from_mode(0o700)).unwrap();
        fs::create_dir(adjacent.join("operation-303f0f4a-6b5c-4d8e-9f00-112233445566")).unwrap();
        assert!(root.has_private_cleanup_residue().unwrap());
    }

    #[test]
    fn public_cleanup_final_remove_failure_preserves_an_empty_private_boundary() {
        // Makes the post-recursion contract explicit: failure to remove the
        // now-empty acquired root returns an error and leaves that empty
        // private boundary as evidence. It is not restored or Drop-deleted.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(root_path.join("value"), b"owned bytes\n").unwrap();
        let root = RootedDir::open(&root_path).unwrap();
        let _fault = CleanupFaultOverride::set(CleanupFault::BeforeFinalRootRemoval(libc::EIO));

        let error = root.remove_owned_tree().unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::EIO));
        assert!(!root_path.exists());
        let namespace_path = physical.join(".mac-worker-rooted-fs");
        let acquired = fs::read_dir(&namespace_path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(acquired.len(), 4);
        let quarantine = acquired
            .iter()
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .as_bytes()
                    .starts_with(b"cleanup-tree-v1-")
            })
            .unwrap();
        assert_eq!(fs::read_dir(quarantine).unwrap().count(), 0);
        fs::remove_dir_all(&namespace_path).unwrap();
    }

    #[test]
    fn cleanup_intent_tree_retry_before_final_root_removal() {
        // Catches treating an absent public component as success while the
        // exact empty acquired root and its intent still remain private.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(root_path.join("value"), b"owned bytes\n").unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let fault = CleanupFaultOverride::set(CleanupFault::BeforeFinalRootRemoval(libc::EIO));

        let error = parent.remove_owned_child("root").unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EIO));
        drop(fault);
        drop(parent);

        let parent = RootedDir::open(&physical).unwrap();
        parent.remove_owned_child("root").unwrap();
        assert!(!root_path.exists());
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn cleanup_intent_tree_retry_after_target_chmod_restores_original_mode() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let fault = CleanupFaultOverride::set(CleanupFault::AfterTargetChmod(libc::EIO));

        let error = parent.remove_owned_child("root").unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EIO));
        assert_eq!(
            fs::metadata(&root_path).unwrap().permissions().mode() & 0o777,
            0o700
        );
        drop(fault);
        drop(parent);

        let parent = RootedDir::open(&physical).unwrap();
        let validation_fault =
            CleanupFaultOverride::set(CleanupFault::AfterAcquisitionValidation(libc::ESTALE));
        let error = parent.remove_owned_child("root").unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        drop(validation_fault);
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"owned");
        assert_eq!(
            fs::metadata(&root_path).unwrap().permissions().mode() & 0o777,
            0o500
        );
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
    }

    fn assert_cleanup_intent_tree_bootstrap_retry(fault: CleanupFault) {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let fault = CleanupFaultOverride::set(fault);

        let error = parent.remove_owned_child("root").unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EIO));
        drop(fault);
        drop(parent);
        assert!(root_path.exists());
        let namespace = physical.join(".mac-worker-rooted-fs");
        assert!(fs::read_dir(&namespace).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .as_bytes()
                .starts_with(b"cleanup-op-v1-")
        }));

        let parent = RootedDir::open(&physical).unwrap();
        parent.remove_owned_child("root").unwrap();

        assert!(!root_path.exists());
        assert_eq!(fs::read_dir(namespace).unwrap().count(), 0);
    }

    #[test]
    fn cleanup_intent_tree_bootstrap_retry_after_slot_fsync() {
        assert_cleanup_intent_tree_bootstrap_retry(CleanupFault::AfterCleanupBootstrap(libc::EIO));
    }

    #[test]
    fn cleanup_intent_tree_bootstrap_retry_after_placeholder_fsync() {
        assert_cleanup_intent_tree_bootstrap_retry(CleanupFault::AfterCleanupPlaceholder(
            libc::EIO,
        ));
    }

    #[test]
    fn cleanup_intent_tree_bootstrap_retry_after_partial_stage_sync() {
        assert_cleanup_intent_tree_bootstrap_retry(CleanupFault::DuringCleanupIntentWrite(
            libc::EIO,
        ));
    }

    #[test]
    fn cleanup_intent_tree_bootstrap_adopts_complete_stage_quarantine() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let fault = CleanupFaultOverride::set(CleanupFault::AfterCleanupIntentSync(libc::EIO));
        assert_eq!(
            parent
                .remove_owned_child("root")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(fault);
        drop(parent);
        let namespace = physical.join(".mac-worker-rooted-fs");
        let operation = fs::read_dir(&namespace)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .as_bytes()
                    .starts_with(b"cleanup-op-v1-")
            })
            .unwrap();
        let stage = fs::read_dir(&operation)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .as_bytes()
                    .starts_with(b"cleanup-intent-stage-v1-")
            })
            .unwrap();
        let staged: CleanupIntentV1 = serde_json::from_slice(&fs::read(stage).unwrap()).unwrap();

        let parent = RootedDir::open(&physical).unwrap();
        let chmod_fault = CleanupFaultOverride::set(CleanupFault::AfterTargetChmod(libc::EIO));
        assert_eq!(
            parent
                .remove_owned_child("root")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(chmod_fault);
        let canonical = fs::read_dir(&namespace)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .as_bytes()
                    .starts_with(b"cleanup-intent-v1-")
            })
            .unwrap();
        let published: CleanupIntentV1 =
            serde_json::from_slice(&fs::read(canonical).unwrap()).unwrap();
        assert_eq!(published.quarantine, staged.quarantine);
        parent.remove_owned_child("root").unwrap();
        assert_eq!(fs::read_dir(namespace).unwrap().count(), 0);
    }

    #[test]
    fn cleanup_intent_tree_bootstrap_two_retriers_converge() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        for index in 0..32 {
            fs::write(root_path.join(format!("value-{index}")), b"owned").unwrap();
        }
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let mut retriers = Vec::new();
        for _ in 0..2 {
            let physical = physical.clone();
            let barrier = barrier.clone();
            retriers.push(std::thread::spawn(move || {
                let parent = RootedDir::open(&physical).unwrap();
                barrier.wait();
                parent.remove_owned_child("root")
            }));
        }
        barrier.wait();
        for retrier in retriers {
            retrier.join().unwrap().unwrap();
        }
        assert!(!root_path.exists());
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
    }

    fn install_same_key_cleanup_bootstrap(namespace: &Path) -> std::path::PathBuf {
        let operation = fs::read_dir(namespace)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .as_bytes()
                    .starts_with(b"cleanup-op-v1-")
            })
            .unwrap();
        let operation_name =
            std::ffi::CString::new(operation.file_name().unwrap().as_bytes()).unwrap();
        let parsed = super::parse_cleanup_bootstrap_name(&operation_name)
            .unwrap()
            .unwrap();
        let duplicate_name = super::cleanup_bootstrap_name(
            &parsed.key,
            super::FileIdentity {
                device: parsed.target.device,
                inode: parsed.target.inode.wrapping_add(1),
            },
            parsed.original_mode,
        )
        .unwrap();
        let duplicate = namespace.join(std::ffi::OsStr::from_bytes(duplicate_name.to_bytes()));
        fs::create_dir(&duplicate).unwrap();
        fs::set_permissions(&duplicate, fs::Permissions::from_mode(0o700)).unwrap();
        duplicate
    }

    #[test]
    fn cleanup_intent_tree_bootstrap_rejects_same_key_different_target() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let fault = CleanupFaultOverride::set(CleanupFault::AfterCleanupBootstrap(libc::EIO));
        assert_eq!(
            parent
                .remove_owned_child("root")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(fault);
        let namespace = physical.join(".mac-worker-rooted-fs");
        let duplicate = install_same_key_cleanup_bootstrap(&namespace);
        let duplicate_inode = fs::metadata(&duplicate).unwrap().ino();

        let error = parent.remove_owned_child("root").unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"owned");
        assert_eq!(fs::metadata(duplicate).unwrap().ino(), duplicate_inode);
    }

    #[test]
    fn cleanup_intent_tree_bootstrap_rejects_canonical_same_key_conflict() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let fault = CleanupFaultOverride::set(CleanupFault::AfterTargetChmod(libc::EIO));
        assert_eq!(
            parent
                .remove_owned_child("root")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(fault);
        let namespace = physical.join(".mac-worker-rooted-fs");
        let duplicate = install_same_key_cleanup_bootstrap(&namespace);
        let duplicate_inode = fs::metadata(&duplicate).unwrap().ino();

        let error = parent.remove_owned_child("root").unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"owned");
        assert_eq!(fs::metadata(duplicate).unwrap().ino(), duplicate_inode);
    }

    #[test]
    fn cleanup_intent_tree_bootstrap_predicate_resumes_partial_slot() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let root_inode = fs::metadata(&root_path).unwrap().ino();
        let parent = RootedDir::open(&physical).unwrap();
        let fault = CleanupFaultOverride::set(CleanupFault::AfterCleanupBootstrap(libc::EIO));
        parent.remove_owned_child("root").unwrap_err();
        drop(fault);

        let resumed = parent
            .retry_pending_owned_children_matching(|component, identity| {
                component == b"root" && identity.inode == root_inode
            })
            .unwrap();

        assert_eq!(resumed, 1);
        assert!(!root_path.exists());
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn cleanup_intent_tree_bootstrap_preserves_unlocked_legacy_operation() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let namespace = physical.join(".mac-worker-rooted-fs");
        fs::create_dir(&namespace).unwrap();
        fs::set_permissions(&namespace, fs::Permissions::from_mode(0o700)).unwrap();
        let legacy = namespace.join("operation-303f0f4a-6b5c-4d8e-9f00-112233445566");
        fs::create_dir(&legacy).unwrap();
        fs::set_permissions(&legacy, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(legacy.join("sentinel"), b"preserve").unwrap();
        let legacy_inode = fs::metadata(&legacy).unwrap().ino();
        let parent = RootedDir::open(&physical).unwrap();

        let error = parent.remove_owned_child("absent").unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(fs::read(legacy.join("sentinel")).unwrap(), b"preserve");
        assert_eq!(fs::metadata(legacy).unwrap().ino(), legacy_inode);
    }

    #[test]
    fn cleanup_intent_tree_retry_preserves_public_replacement() {
        // Catches retry treating a new public child as the original target.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(root_path.join("first"), b"first\n").unwrap();
        fs::write(root_path.join("second"), b"second\n").unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let fault = CleanupFaultOverride::set(CleanupFault::AfterFirstRemoval(libc::EIO));
        parent.remove_owned_child("root").unwrap_err();
        drop(fault);
        drop(parent);

        let namespace = physical.join(".mac-worker-rooted-fs");
        let quarantine = fs::read_dir(&namespace)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .as_bytes()
                    .starts_with(b"cleanup-tree-v1-")
            })
            .unwrap();
        let quarantine_inode = fs::metadata(&quarantine).unwrap().ino();
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(root_path.join("sentinel"), b"unrelated").unwrap();

        let parent = RootedDir::open(&physical).unwrap();
        let error = parent.remove_owned_child("root").unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(fs::read(root_path.join("sentinel")).unwrap(), b"unrelated");
        assert_eq!(fs::metadata(quarantine).unwrap().ino(), quarantine_inode);
    }

    #[test]
    fn cleanup_intent_tree_retry_predicate_is_identity_scoped() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        for name in ["first-root", "second-root"] {
            let path = physical.join(name);
            fs::create_dir(&path).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
            fs::write(path.join("first"), b"first").unwrap();
            fs::write(path.join("second"), b"second").unwrap();
        }
        let first_identity = fs::metadata(physical.join("first-root")).unwrap().ino();
        let parent = RootedDir::open(&physical).unwrap();
        for name in ["first-root", "second-root"] {
            let fault = CleanupFaultOverride::set(CleanupFault::AfterFirstRemoval(libc::EIO));
            assert_eq!(
                parent.remove_owned_child(name).unwrap_err().raw_os_error(),
                Some(libc::EIO)
            );
            drop(fault);
        }
        drop(parent);

        let parent = RootedDir::open(&physical).unwrap();
        let resumed = parent
            .retry_pending_owned_children_matching(|component, identity| {
                assert!(component == b"first-root" || component == b"second-root");
                identity.inode == first_identity
            })
            .unwrap();

        assert_eq!(resumed, 1);
        assert!(!physical.join("first-root").exists());
        assert!(!physical.join("second-root").exists());
        assert!(
            !parent
                .resume_pending_owned_child_cleanup("first-root")
                .unwrap()
        );
        assert!(
            parent
                .resume_pending_owned_child_cleanup("second-root")
                .unwrap()
        );
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn cleanup_intent_tree_retry_preserves_legacy_unbound_evidence() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let namespace = physical.join(".mac-worker-rooted-fs");
        fs::create_dir(&namespace).unwrap();
        fs::set_permissions(&namespace, fs::Permissions::from_mode(0o700)).unwrap();
        let legacy = namespace.join("cleanup-303f0f4a-6b5c-4d8e-9f00-112233445566");
        fs::create_dir(&legacy).unwrap();
        fs::write(legacy.join("sentinel"), b"preserve").unwrap();
        let inode = fs::metadata(&legacy).unwrap().ino();
        let parent = RootedDir::open(&physical).unwrap();

        let error = parent.remove_owned_child("absent").unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(fs::read(legacy.join("sentinel")).unwrap(), b"preserve");
        assert_eq!(fs::metadata(legacy).unwrap().ino(), inode);
    }

    #[test]
    fn cleanup_intent_tree_retry_preserves_substituted_quarantine() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(root_path.join("first"), b"first").unwrap();
        fs::write(root_path.join("second"), b"second").unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let fault = CleanupFaultOverride::set(CleanupFault::AfterFirstRemoval(libc::EIO));
        parent.remove_owned_child("root").unwrap_err();
        drop(fault);
        drop(parent);
        let namespace = physical.join(".mac-worker-rooted-fs");
        let quarantine = fs::read_dir(&namespace)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .as_bytes()
                    .starts_with(b"cleanup-tree-v1-")
            })
            .unwrap();
        let evidence = physical.join("original-quarantine");
        fs::rename(&quarantine, &evidence).unwrap();
        fs::create_dir(&quarantine).unwrap();
        fs::set_permissions(&quarantine, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(quarantine.join("sentinel"), b"unrelated").unwrap();
        let replacement_inode = fs::metadata(&quarantine).unwrap().ino();

        let parent = RootedDir::open(&physical).unwrap();
        let error = parent.remove_owned_child("root").unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(fs::read(quarantine.join("sentinel")).unwrap(), b"unrelated");
        assert_eq!(fs::metadata(&quarantine).unwrap().ino(), replacement_inode);
        assert!(evidence.is_dir());
    }

    #[test]
    fn caller_visible_stage_replacement_is_never_published() {
        // Catches validating a caller-visible staging link and resolving that
        // pathname again after an observer replaces it before publication.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let source_path = physical.join("source");
        let destination_path = physical.join("destination");
        fs::create_dir(&source_path).unwrap();
        fs::create_dir(&destination_path).unwrap();
        fs::write(source_path.join("value"), b"owned bytes\n").unwrap();
        let source_parent = open_directory_path(&source_path).unwrap();
        let source = open_regular_at(source_parent.as_raw_fd(), c"value").unwrap();
        let destination_parent = open_directory_path(&destination_path).unwrap();
        let moved_stage = destination_path.join("moved-stage");

        copy_regular_with_clone_and_publish(
            source,
            &destination_parent,
            c"value",
            0o444,
            |_source, _temporary_parent, _temporary_name| {
                Err(std::io::Error::from_raw_os_error(libc::ENOTSUP))
            },
            |source_parent, source_name, destination_parent, destination_name| {
                let exposed_stage = fs::read_dir(&destination_path)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .find(|path| {
                        path.file_name()
                            .unwrap()
                            .to_string_lossy()
                            .starts_with(".mac-worker-publish-")
                    });
                if let Some(exposed_stage) = exposed_stage {
                    fs::rename(&exposed_stage, &moved_stage).unwrap();
                    fs::write(&exposed_stage, b"replacement bytes\n").unwrap();
                }
                super::publish_regular_no_replace(
                    source_parent,
                    source_name,
                    destination_parent,
                    destination_name,
                )
            },
        )
        .unwrap();

        assert_eq!(
            fs::read(destination_path.join("value")).unwrap(),
            b"owned bytes\n"
        );
        assert!(!moved_stage.exists());
    }

    #[test]
    fn failed_publication_drop_never_unlinks_a_stage_replacement() {
        // Catches failure cleanup resolving a caller-visible staging pathname
        // after an observer replaces it and publication fails.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let source_path = physical.join("source");
        let destination_path = physical.join("destination");
        fs::create_dir(&source_path).unwrap();
        fs::create_dir(&destination_path).unwrap();
        fs::write(source_path.join("value"), b"owned bytes\n").unwrap();
        let source_parent = open_directory_path(&source_path).unwrap();
        let source = open_regular_at(source_parent.as_raw_fd(), c"value").unwrap();
        let destination_parent = open_directory_path(&destination_path).unwrap();
        let moved_stage = destination_path.join("moved-stage");
        let attacked = std::cell::RefCell::new(None);

        let error = copy_regular_with_clone_and_publish(
            source,
            &destination_parent,
            c"value",
            0o444,
            |_source, _temporary_parent, _temporary_name| {
                Err(std::io::Error::from_raw_os_error(libc::ENOTSUP))
            },
            |_source_parent, _source_name, _destination_parent, _destination_name| {
                let exposed_stage = fs::read_dir(&destination_path)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .find(|path| {
                        path.file_name()
                            .unwrap()
                            .to_string_lossy()
                            .starts_with(".mac-worker-publish-")
                    });
                if let Some(exposed_stage) = exposed_stage {
                    fs::rename(&exposed_stage, &moved_stage).unwrap();
                    fs::write(&exposed_stage, b"replacement bytes\n").unwrap();
                    attacked.replace(Some(exposed_stage));
                }
                Err(std::io::Error::from_raw_os_error(libc::EIO))
            },
        )
        .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::EIO));
        if let Some(attacked) = attacked.into_inner() {
            assert_eq!(fs::read(attacked).unwrap(), b"replacement bytes\n");
            assert_eq!(fs::read(&moved_stage).unwrap(), b"owned bytes\n");
        }
        assert!(!destination_path.join("value").exists());
    }

    #[test]
    fn portable_publication_fails_before_a_staged_unlink_can_be_discarded() {
        // Catches link publication reporting success after its staged-link
        // unlink failed, which strands a .mac-worker-publish-* entry.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let source = physical.join("source");
        let destination = physical.join("destination");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&destination).unwrap();
        fs::write(source.join("data"), b"bytes\n").unwrap();
        let source_fd = open_directory_path(&source).unwrap();
        let destination_fd = open_directory_path(&destination).unwrap();
        let linked = std::cell::Cell::new(false);
        let unlink_attempted = std::cell::Cell::new(false);

        let error = publish_regular_with_link_ops(
            source_fd.as_raw_fd(),
            c"data",
            destination_fd.as_raw_fd(),
            c"final",
            |source_parent, source_name, destination_parent, destination_name| {
                linked.set(true);
                link_at(
                    source_parent,
                    source_name,
                    destination_parent,
                    destination_name,
                )
            },
            |_source_parent, _source_name| {
                unlink_attempted.set(true);
                Err(std::io::Error::from_raw_os_error(libc::EIO))
            },
        )
        .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ENOTSUP));
        assert!(!linked.get());
        assert!(!unlink_attempted.get());
        assert_eq!(fs::read(source.join("data")).unwrap(), b"bytes\n");
        assert!(!destination.join("final").exists());
    }

    #[test]
    fn portable_atomic_rename_fails_before_mutation_without_support() {
        // Catches the generic fallback partially moving an entry without an
        // atomic no-replace rename primitive.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let parent = physical.join("parent");
        fs::create_dir(&parent).unwrap();
        fs::create_dir(parent.join("child")).unwrap();
        let parent_fd = open_directory_path(&parent).unwrap();

        let error = unsupported_rename_no_replace(
            parent_fd.as_raw_fd(),
            c"child",
            parent_fd.as_raw_fd(),
            c"quarantine",
        )
        .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ENOTSUP));
        assert!(parent.join("child").is_dir());
        assert!(!parent.join("quarantine").exists());
    }

    #[test]
    fn recursive_directory_open_rejects_cross_device_mounts() {
        // Catches publication or cleanup descending from the filesystem root
        // into the separately mounted device filesystem.
        let root = open_directory_path(std::path::Path::new("/")).unwrap();
        let dev = std::ffi::CString::new("dev").unwrap();
        let metadata = stat_at(root.as_raw_fd(), &dev).unwrap();

        let error = open_verified_child_directory(root.as_raw_fd(), &dev, &metadata).unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::EXDEV));
    }

    #[test]
    fn host_child_creation_rejects_a_wrong_anchored_device_before_mutation() {
        // Catches host-owned namespaces being created on a device other than
        // the filesystem whose admission facts were measured.
        let fixture = tempfile::tempdir().unwrap();
        let root_path = fixture.path().join("host");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root = RootedDir::open(&root_path).unwrap();
        let wrong_device = root.root_metadata().unwrap().st_dev as u64 + 1;
        let child = RelativePath::parse(b"leases").unwrap();

        let error = match root.open_child_directory_on_device(&child, true, wrong_device) {
            Ok(_) => panic!("wrong-device host child was created"),
            Err(error) => error,
        };

        assert_eq!(error.raw_os_error(), Some(libc::EXDEV));
        assert!(!root_path.join("leases").exists());
    }

    #[test]
    fn anchored_lineage_rejects_an_ancestor_mode_change() {
        let fixture = tempfile::tempdir().unwrap();
        let ancestor = fixture.path().join("ancestor");
        let parent = ancestor.join("parent");
        fs::create_dir_all(&parent).unwrap();
        fs::set_permissions(&ancestor, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
        let anchored = RootedDir::open_anchored_absolute(&parent).unwrap();

        fs::set_permissions(&ancestor, fs::Permissions::from_mode(0o750)).unwrap();

        assert_eq!(
            anchored.verify_bound().unwrap_err().raw_os_error(),
            Some(libc::ESTALE)
        );
    }

    #[test]
    fn atomic_json_publication_rolls_back_when_a_grandparent_moves_at_commit() {
        let fixture = tempfile::tempdir().unwrap();
        let host_path = fixture.path().join("host");
        fs::create_dir(&host_path).unwrap();
        fs::set_permissions(&host_path, fs::Permissions::from_mode(0o700)).unwrap();
        let mut host = RootedDir::open(&host_path).unwrap();
        let device = host.root_metadata().unwrap().st_dev as u64;
        host.bind_host_device(device).unwrap();
        let index = host
            .open_child_directory_on_device(
                &RelativePath::parse(b"outer/job-index").unwrap(),
                true,
                device,
            )
            .unwrap();
        let detached = fixture.path().join("detached-files");
        let replacement = host_path.join("outer/job-index");

        let error = index
            .write_private_atomic_no_replace_with_hook("job.json", b"{}", || {
                fs::rename(host_path.join("outer"), &detached).unwrap();
                fs::create_dir_all(&replacement).unwrap();
                fs::set_permissions(host_path.join("outer"), fs::Permissions::from_mode(0o700))
                    .unwrap();
                fs::set_permissions(&replacement, fs::Permissions::from_mode(0o700)).unwrap();
                fs::write(replacement.join("sentinel"), b"keep").unwrap();
            })
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(fs::read(replacement.join("sentinel")).unwrap(), b"keep");
        assert!(!replacement.join("job.json").exists());
        assert_eq!(fs::read_dir(detached.join("job-index")).unwrap().count(), 0);
    }

    #[test]
    fn directory_publication_rolls_back_when_a_grandparent_moves_at_commit() {
        let fixture = tempfile::tempdir().unwrap();
        let host_path = fixture.path().join("host");
        fs::create_dir(&host_path).unwrap();
        fs::set_permissions(&host_path, fs::Permissions::from_mode(0o700)).unwrap();
        let mut host = RootedDir::open(&host_path).unwrap();
        let device = host.root_metadata().unwrap().st_dev as u64;
        host.bind_host_device(device).unwrap();
        let incoming = host
            .open_child_directory_on_device(
                &RelativePath::parse(b"incoming").unwrap(),
                true,
                device,
            )
            .unwrap();
        let mut stage = incoming.create_new_child_directory("stage").unwrap();
        stage.write_new_private_file("payload", b"owned").unwrap();
        let destination = host
            .open_child_directory_on_device(
                &RelativePath::parse(b"jobs/project/worktree").unwrap(),
                true,
                device,
            )
            .unwrap();
        let detached = fixture.path().join("detached-project");
        let replacement = host_path.join("jobs/project/worktree");

        let error = stage
            .publish_owned_into_with_hook(&destination, "job", || {
                fs::rename(host_path.join("jobs/project"), &detached).unwrap();
                fs::create_dir_all(&replacement).unwrap();
                fs::set_permissions(
                    host_path.join("jobs/project"),
                    fs::Permissions::from_mode(0o700),
                )
                .unwrap();
                fs::set_permissions(&replacement, fs::Permissions::from_mode(0o700)).unwrap();
                fs::write(replacement.join("sentinel"), b"keep").unwrap();
            })
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(fs::read(replacement.join("sentinel")).unwrap(), b"keep");
        assert!(!replacement.join("job").exists());
        assert!(!detached.join("worktree/job").exists());
        assert_eq!(
            fs::read(host_path.join("incoming/stage/payload")).unwrap(),
            b"owned"
        );
    }

    #[test]
    fn destructive_cleanup_rolls_back_when_a_grandparent_moves_at_commit() {
        let fixture = tempfile::tempdir().unwrap();
        let host_path = fixture.path().join("host");
        fs::create_dir(&host_path).unwrap();
        fs::set_permissions(&host_path, fs::Permissions::from_mode(0o700)).unwrap();
        let mut host = RootedDir::open(&host_path).unwrap();
        let device = host.root_metadata().unwrap().st_dev as u64;
        host.bind_host_device(device).unwrap();
        let job = host
            .open_child_directory_on_device(
                &RelativePath::parse(b"jobs/project/worktree/job").unwrap(),
                true,
                device,
            )
            .unwrap();
        job.write_new_private_file("payload", b"owned").unwrap();
        let detached = fixture.path().join("detached-project");
        let replacement = host_path.join("jobs/project/worktree");

        let error = job
            .remove_owned_tree_with_hooks(
                || {
                    fs::rename(host_path.join("jobs/project"), &detached).unwrap();
                    fs::create_dir_all(&replacement).unwrap();
                    fs::set_permissions(
                        host_path.join("jobs/project"),
                        fs::Permissions::from_mode(0o700),
                    )
                    .unwrap();
                    fs::set_permissions(&replacement, fs::Permissions::from_mode(0o700)).unwrap();
                    fs::write(replacement.join("sentinel"), b"keep").unwrap();
                },
                || {},
                || {},
                || {},
                || {},
            )
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(fs::read(replacement.join("sentinel")).unwrap(), b"keep");
        assert_eq!(
            fs::read(detached.join("worktree/job/payload")).unwrap(),
            b"owned"
        );
    }

    #[test]
    fn atomic_json_recovery_preserves_a_substitution_at_the_exact_rollback_boundary() {
        let fixture = tempfile::tempdir().unwrap();
        let host_path = fixture.path().join("host");
        fs::create_dir(&host_path).unwrap();
        fs::set_permissions(&host_path, fs::Permissions::from_mode(0o700)).unwrap();
        let mut host = RootedDir::open(&host_path).unwrap();
        let device = host.root_metadata().unwrap().st_dev as u64;
        host.bind_host_device(device).unwrap();
        let index = host
            .open_child_directory_on_device(
                &RelativePath::parse(b"outer/job-index").unwrap(),
                true,
                device,
            )
            .unwrap();
        let detached = fixture.path().join("detached-outer");
        let target = detached.join("job-index/job.json");
        let evidence = fixture.path().join("owned-json-evidence");
        let replacement_inode = std::cell::Cell::new(0);

        let error = index
            .write_private_atomic_no_replace_with_hooks(
                "job.json",
                b"owned",
                || {
                    fs::rename(host_path.join("outer"), &detached).unwrap();
                    fs::create_dir_all(host_path.join("outer/job-index")).unwrap();
                },
                || {
                    fs::rename(&target, &evidence).unwrap();
                    fs::write(&target, b"unrelated").unwrap();
                    replacement_inode.set(fs::metadata(&target).unwrap().ino());
                },
            )
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(fs::read(&target).unwrap(), b"unrelated");
        assert_eq!(
            fs::metadata(&target).unwrap().ino(),
            replacement_inode.get()
        );
        assert_eq!(fs::read(&evidence).unwrap(), b"owned");
    }

    #[test]
    fn bounded_private_read_rejects_a_final_name_swap_after_open() {
        let fixture = tempfile::tempdir().unwrap();
        let root_path = fixture.path().join("private");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root = RootedDir::open(&root_path).unwrap();
        root.write_private_atomic_no_replace("receipt.json", b"owned")
            .unwrap();

        let error = root
            .read_private_regular_with_hook("receipt.json", 1024, || {
                fs::rename(
                    root_path.join("receipt.json"),
                    root_path.join("detached.json"),
                )
                .unwrap();
                fs::write(root_path.join("receipt.json"), b"planted").unwrap();
                fs::set_permissions(
                    root_path.join("receipt.json"),
                    fs::Permissions::from_mode(0o600),
                )
                .unwrap();
            })
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(fs::read(root_path.join("detached.json")).unwrap(), b"owned");
        assert_eq!(
            fs::read(root_path.join("receipt.json")).unwrap(),
            b"planted"
        );
    }

    #[test]
    fn bounded_private_read_reports_stale_when_atomic_replacement_unlinks_opened_inode() {
        let fixture = tempfile::tempdir().unwrap();
        let root_path = fixture.path().join("private");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root = RootedDir::open(&root_path).unwrap();
        root.write_private_atomic_no_replace("receipt.json", b"old")
            .unwrap();

        let error = root
            .read_private_regular_with_hook("receipt.json", 1024, || {
                let replacement = root_path.join("replacement.json");
                fs::write(&replacement, b"replacement").unwrap();
                fs::set_permissions(&replacement, fs::Permissions::from_mode(0o600)).unwrap();
                fs::rename(&replacement, root_path.join("receipt.json")).unwrap();
            })
            .unwrap_err();

        assert_eq!(
            error.raw_os_error(),
            Some(libc::ESTALE),
            "unexpected error kind: {:?}",
            error.kind()
        );
        assert_eq!(
            fs::read(root_path.join("receipt.json")).unwrap(),
            b"replacement"
        );
        assert!(!root_path.join("replacement.json").exists());
    }

    #[test]
    fn bounded_private_read_checks_unsafe_rebound_before_post_read_staleness() {
        for replacement in [
            UnsafePrivateReadReplacement::PermissiveRegular,
            UnsafePrivateReadReplacement::HardlinkedRegular,
            UnsafePrivateReadReplacement::Directory,
        ] {
            let fixture = tempfile::tempdir().unwrap();
            let root_path = fixture.path().join("private");
            fs::create_dir(&root_path).unwrap();
            fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
            let root = RootedDir::open(&root_path).unwrap();
            root.write_private_atomic_no_replace("receipt.json", b"old")
                .unwrap();

            let error = root
                .read_private_regular_with_hook("receipt.json", 1024, || {
                    install_unsafe_private_read_replacement(&root_path, replacement);
                })
                .unwrap_err();

            assert_eq!(
                error.kind(),
                std::io::ErrorKind::PermissionDenied,
                "{replacement:?}"
            );
            assert_ne!(error.raw_os_error(), Some(libc::ESTALE), "{replacement:?}");
            assert_unsafe_private_read_replacement_preserved(&root_path, replacement);
        }
    }

    #[test]
    fn bounded_private_read_first_opened_stat_reports_safe_atomic_replacement_as_stale() {
        let fixture = tempfile::tempdir().unwrap();
        let root_path = fixture.path().join("private");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root = RootedDir::open(&root_path).unwrap();
        root.write_private_atomic_no_replace("receipt.json", b"old")
            .unwrap();

        let error = root
            .read_private_regular_with_hooks(
                "receipt.json",
                1024,
                || {
                    let replacement = root_path.join("replacement.json");
                    fs::write(&replacement, b"replacement").unwrap();
                    fs::set_permissions(&replacement, fs::Permissions::from_mode(0o600)).unwrap();
                    fs::rename(&replacement, root_path.join("receipt.json")).unwrap();
                },
                || {},
            )
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(
            fs::read(root_path.join("receipt.json")).unwrap(),
            b"replacement"
        );
        assert!(!root_path.join("replacement.json").exists());
    }

    #[test]
    fn bounded_private_read_checks_unsafe_rebound_before_first_opened_stat_staleness() {
        for replacement in [
            UnsafePrivateReadReplacement::PermissiveRegular,
            UnsafePrivateReadReplacement::HardlinkedRegular,
            UnsafePrivateReadReplacement::Directory,
        ] {
            let fixture = tempfile::tempdir().unwrap();
            let root_path = fixture.path().join("private");
            fs::create_dir(&root_path).unwrap();
            fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
            let root = RootedDir::open(&root_path).unwrap();
            root.write_private_atomic_no_replace("receipt.json", b"old")
                .unwrap();

            let error = root
                .read_private_regular_with_hooks(
                    "receipt.json",
                    1024,
                    || install_unsafe_private_read_replacement(&root_path, replacement),
                    || {},
                )
                .unwrap_err();

            assert_eq!(
                error.kind(),
                std::io::ErrorKind::PermissionDenied,
                "{replacement:?}"
            );
            assert_ne!(error.raw_os_error(), Some(libc::ESTALE), "{replacement:?}");
            assert_unsafe_private_read_replacement_preserved(&root_path, replacement);
        }
    }

    #[test]
    fn bounded_private_read_keeps_unsafe_named_entries_permission_denied() {
        let fixture = tempfile::tempdir().unwrap();
        let root_path = fixture.path().join("private");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root = RootedDir::open(&root_path).unwrap();

        fs::write(root_path.join("permissive.json"), b"permissive").unwrap();
        fs::set_permissions(
            root_path.join("permissive.json"),
            fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        fs::write(root_path.join("hardlinked.json"), b"hardlinked").unwrap();
        fs::set_permissions(
            root_path.join("hardlinked.json"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        fs::hard_link(
            root_path.join("hardlinked.json"),
            root_path.join("hardlink-alias.json"),
        )
        .unwrap();
        fs::create_dir(root_path.join("directory.json")).unwrap();
        fs::set_permissions(
            root_path.join("directory.json"),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();

        for name in ["permissive.json", "hardlinked.json", "directory.json"] {
            let error = root.read_private_regular(name, 1024).unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied, "{name}");
        }
    }

    #[test]
    fn log_chunk_read_clamps_limits_preserves_bytes_and_allows_exact_growth() {
        // Catches allocating or reading the raw request limit, using shared
        // seek state, rejecting exact EOF, losing binary bytes, or freezing
        // the readable range at the initial descriptor length.
        let fixture = tempfile::tempdir().unwrap();
        let root_path = fixture.path().join("private");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let mut root = RootedDir::open(&root_path).unwrap();
        let device = root.root_metadata().unwrap().st_dev as u64;
        root.bind_host_device(device).unwrap();
        let bytes = [
            b"utf8:\xe4\xb8\x96\xe7\x95\x8c\0\xff\x80".as_slice(),
            &vec![b'x'; 65_537],
        ]
        .concat();
        root.write_private_atomic_no_replace("stdout.log", &bytes)
            .unwrap();

        assert_eq!(
            root.read_private_regular_chunk("stdout.log", 0, 0).unwrap(),
            b""
        );
        for (limit, expected) in [
            (65_535usize, 65_535usize),
            (65_536usize, 65_536usize),
            (65_537usize, 65_536usize),
            (usize::MAX, 65_536usize),
        ] {
            assert_eq!(
                root.read_private_regular_chunk("stdout.log", 0, limit)
                    .unwrap(),
                bytes[..expected],
                "limit {limit}"
            );
        }
        assert_eq!(
            root.read_private_regular_chunk("stdout.log", bytes.len() as u64, 1)
                .unwrap(),
            b""
        );
        for offset in [bytes.len() as u64 + 1, u64::MAX] {
            let error = root
                .read_private_regular_chunk("stdout.log", offset, usize::MAX)
                .unwrap_err();
            assert!(super::is_log_offset_beyond_eof(&error), "{error:?}");
        }

        root.write_private_atomic_no_replace("stderr.log", b"abc")
            .unwrap();
        let grown = root
            .read_private_regular_chunk_with_hooks(
                "stderr.log",
                3,
                65_537,
                || {},
                || {
                    let mut file = fs::OpenOptions::new()
                        .append(true)
                        .open(root_path.join("stderr.log"))
                        .unwrap();
                    file.write_all(b"\0\xffgrown").unwrap();
                    file.sync_all().unwrap();
                },
                || {},
            )
            .unwrap();
        assert_eq!(grown, b"\0\xffgrown");
    }

    #[test]
    fn log_chunk_read_fails_closed_on_shrink_replacement_or_metadata_drift() {
        // Catches validating only before pread, accepting a net-shorter file,
        // or continuing after the trusted name stops naming the opened inode.
        fn bound_root(root_path: &Path, bytes: &[u8]) -> RootedDir {
            fs::create_dir(root_path).unwrap();
            fs::set_permissions(root_path, fs::Permissions::from_mode(0o700)).unwrap();
            let mut root = RootedDir::open(root_path).unwrap();
            let device = root.root_metadata().unwrap().st_dev as u64;
            root.bind_host_device(device).unwrap();
            root.write_private_atomic_no_replace("stdout.log", bytes)
                .unwrap();
            root
        }

        for boundary in ["before_read", "after_read"] {
            let fixture = tempfile::tempdir().unwrap();
            let root_path = fixture.path().join(boundary);
            let root = bound_root(&root_path, b"0123456789");
            let truncate = || {
                let file = fs::OpenOptions::new()
                    .write(true)
                    .open(root_path.join("stdout.log"))
                    .unwrap();
                file.set_len(2).unwrap();
                file.sync_all().unwrap();
            };
            let error = if boundary == "before_read" {
                root.read_private_regular_chunk_with_hooks(
                    "stdout.log",
                    4,
                    4,
                    || {},
                    truncate,
                    || {},
                )
            } else {
                root.read_private_regular_chunk_with_hooks(
                    "stdout.log",
                    4,
                    4,
                    || {},
                    || {},
                    truncate,
                )
            }
            .unwrap_err();
            assert_eq!(error.raw_os_error(), Some(libc::ESTALE), "{boundary}");
        }

        let fixture = tempfile::tempdir().unwrap();
        let root_path = fixture.path().join("open-race");
        let root = bound_root(&root_path, b"owned");
        let error = root
            .read_private_regular_chunk_with_hooks(
                "stdout.log",
                0,
                5,
                || {
                    fs::rename(root_path.join("stdout.log"), root_path.join("detached.log"))
                        .unwrap();
                    fs::write(root_path.join("stdout.log"), b"other").unwrap();
                    fs::set_permissions(
                        root_path.join("stdout.log"),
                        fs::Permissions::from_mode(0o600),
                    )
                    .unwrap();
                },
                || {},
                || {},
            )
            .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));

        let fixture = tempfile::tempdir().unwrap();
        let root_path = fixture.path().join("final-replacement");
        let root = bound_root(&root_path, b"owned");
        let error = root
            .read_private_regular_chunk_with_hooks(
                "stdout.log",
                0,
                5,
                || {},
                || {},
                || {
                    fs::rename(root_path.join("stdout.log"), root_path.join("detached.log"))
                        .unwrap();
                    fs::write(root_path.join("stdout.log"), b"other").unwrap();
                    fs::set_permissions(
                        root_path.join("stdout.log"),
                        fs::Permissions::from_mode(0o600),
                    )
                    .unwrap();
                },
            )
            .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));

        for drift in ["mode", "hardlink"] {
            let fixture = tempfile::tempdir().unwrap();
            let root_path = fixture.path().join(drift);
            let root = bound_root(&root_path, b"owned");
            let error = root
                .read_private_regular_chunk_with_hooks(
                    "stdout.log",
                    0,
                    5,
                    || {},
                    || {},
                    || {
                        if drift == "mode" {
                            fs::set_permissions(
                                root_path.join("stdout.log"),
                                fs::Permissions::from_mode(0o644),
                            )
                            .unwrap();
                        } else {
                            fs::hard_link(
                                root_path.join("stdout.log"),
                                root_path.join("alias.log"),
                            )
                            .unwrap();
                        }
                    },
                )
                .unwrap_err();
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::PermissionDenied,
                "{drift}"
            );
        }

        let fixture = tempfile::tempdir().unwrap();
        let root_path = fixture.path().join("wrong-device");
        let mut root = bound_root(&root_path, b"owned");
        root.security_device = Some(root.root_metadata().unwrap().st_dev as u64 + 1);
        assert_eq!(
            root.read_private_regular_chunk("stdout.log", 0, 5)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EXDEV)
        );

        let fixture = tempfile::tempdir().unwrap();
        let root_path = fixture.path().join("wrong-types");
        let mut root = bound_root(&root_path, b"owned");
        let device = root.root_metadata().unwrap().st_dev as u64;
        fs::rename(root_path.join("stdout.log"), root_path.join("target.log")).unwrap();
        symlink(root_path.join("target.log"), root_path.join("stdout.log")).unwrap();
        root.security_device = Some(device);
        assert!(root.read_private_regular_chunk("stdout.log", 0, 5).is_err());
    }

    #[test]
    fn directory_publication_recovery_preserves_a_substitution_at_rollback() {
        let fixture = tempfile::tempdir().unwrap();
        let host_path = fixture.path().join("host");
        fs::create_dir(&host_path).unwrap();
        fs::set_permissions(&host_path, fs::Permissions::from_mode(0o700)).unwrap();
        let mut host = RootedDir::open(&host_path).unwrap();
        let device = host.root_metadata().unwrap().st_dev as u64;
        host.bind_host_device(device).unwrap();
        let incoming = host
            .open_child_directory_on_device(
                &RelativePath::parse(b"incoming").unwrap(),
                true,
                device,
            )
            .unwrap();
        let mut stage = incoming.create_new_child_directory("stage").unwrap();
        stage.write_new_private_file("payload", b"owned").unwrap();
        let destination = host
            .open_child_directory_on_device(
                &RelativePath::parse(b"jobs/project/worktree").unwrap(),
                true,
                device,
            )
            .unwrap();
        let detached = fixture.path().join("detached-project");
        let target = detached.join("worktree/job");
        let evidence = fixture.path().join("owned-directory-evidence");
        let replacement_inode = std::cell::Cell::new(0);

        let error = stage
            .publish_owned_into_with_hooks(
                &destination,
                "job",
                || {
                    fs::rename(host_path.join("jobs/project"), &detached).unwrap();
                    fs::create_dir_all(host_path.join("jobs/project/worktree")).unwrap();
                },
                || Ok(()),
                || Ok(()),
                || {
                    fs::rename(&target, &evidence).unwrap();
                    fs::create_dir(&target).unwrap();
                    fs::write(target.join("sentinel"), b"unrelated").unwrap();
                    replacement_inode.set(fs::metadata(&target).unwrap().ino());
                },
            )
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(fs::read(target.join("sentinel")).unwrap(), b"unrelated");
        assert_eq!(
            fs::metadata(&target).unwrap().ino(),
            replacement_inode.get()
        );
        assert_eq!(fs::read(evidence.join("payload")).unwrap(), b"owned");
    }

    #[test]
    fn regular_cleanup_recovery_preserves_a_substituted_quarantine_entry() {
        let fixture = tempfile::tempdir().unwrap();
        let host_path = fixture.path().join("host");
        fs::create_dir_all(host_path.join("outer/files")).unwrap();
        fs::set_permissions(&host_path, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(host_path.join("outer"), fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(
            host_path.join("outer/files"),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        fs::write(host_path.join("outer/files/value"), b"owned").unwrap();
        fs::set_permissions(
            host_path.join("outer/files/value"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let files = RootedDir::open(&host_path.join("outer/files")).unwrap();
        let detached = fixture.path().join("detached-outer");
        let private_path = std::cell::RefCell::new(None);
        let replacement_inode = std::cell::Cell::new(0);
        let evidence = fixture.path().join("owned-regular-evidence");

        let error = files
            .remove_owned_regular_with_hooks(
                "value",
                |_| {
                    fs::rename(host_path.join("outer/files"), &detached).unwrap();
                    fs::create_dir(host_path.join("outer/files")).unwrap();
                },
                |private| {
                    let private = detached.join(std::ffi::OsStr::from_bytes(private.to_bytes()));
                    fs::rename(&private, &evidence).unwrap();
                    fs::write(&private, b"unrelated").unwrap();
                    replacement_inode.set(fs::metadata(&private).unwrap().ino());
                    private_path.replace(Some(private));
                },
                |_| {},
            )
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        let private = private_path.into_inner().unwrap();
        assert_eq!(fs::read(&private).unwrap(), b"unrelated");
        assert_eq!(
            fs::metadata(&private).unwrap().ino(),
            replacement_inode.get()
        );
        assert_eq!(fs::read(&evidence).unwrap(), b"owned");
    }

    #[test]
    fn regular_cleanup_final_delete_preserves_a_substituted_entry() {
        let fixture = tempfile::tempdir().unwrap();
        let root_path = fixture.path().join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(root_path.join("value"), fs::Permissions::from_mode(0o600)).unwrap();
        let root = RootedDir::open(&root_path).unwrap();
        let replacement_path = std::cell::RefCell::new(None);
        let replacement_inode = std::cell::Cell::new(0);
        let evidence = fixture.path().join("owned-final-regular");

        let error = root.remove_owned_regular_with_hooks(
            "value",
            |_| {},
            |_| {},
            |private| {
                let private = root_path.join(std::ffi::OsStr::from_bytes(private.to_bytes()));
                fs::rename(&private, &evidence).unwrap();
                fs::write(&private, b"unrelated").unwrap();
                replacement_inode.set(fs::metadata(&private).unwrap().ino());
                replacement_path.replace(Some(private));
            },
        );

        assert!(error.is_err());
        let replacement = replacement_path.into_inner().unwrap();
        assert_eq!(fs::read(&replacement).unwrap(), b"unrelated");
        assert_eq!(
            fs::metadata(&replacement).unwrap().ino(),
            replacement_inode.get()
        );
        assert_eq!(fs::read(&evidence).unwrap(), b"owned");
    }

    #[test]
    fn tree_cleanup_recovery_preserves_a_substituted_quarantine_entry() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        let root = RootedDir::open(&root_path).unwrap();
        let moved_root = physical.join("moved-root");
        let private_path = std::cell::RefCell::new(None);
        let replacement_inode = std::cell::Cell::new(0);
        let evidence = physical.join("owned-tree-evidence");

        let error = root
            .remove_owned_tree_with_hooks(
                || {
                    fs::rename(&root_path, &moved_root).unwrap();
                    fs::create_dir(&root_path).unwrap();
                },
                || {},
                || {},
                || {
                    let namespace = physical.join(".mac-worker-rooted-fs");
                    let private = fs::read_dir(&namespace)
                        .unwrap()
                        .map(|entry| entry.unwrap().path())
                        .find(|path| path.is_dir())
                        .unwrap();
                    fs::rename(&private, &evidence).unwrap();
                    fs::create_dir(&private).unwrap();
                    fs::write(private.join("sentinel"), b"unrelated").unwrap();
                    replacement_inode.set(fs::metadata(&private).unwrap().ino());
                    private_path.replace(Some(private));
                },
                || {},
            )
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        let private = private_path.into_inner().unwrap();
        assert_eq!(fs::read(private.join("sentinel")).unwrap(), b"unrelated");
        assert_eq!(
            fs::metadata(&private).unwrap().ino(),
            replacement_inode.get()
        );
        assert!(evidence.is_dir());
        assert_eq!(fs::read(moved_root.join("value")).unwrap(), b"owned");
    }

    #[test]
    fn tree_cleanup_final_delete_preserves_a_substituted_quarantine_entry() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        let root = RootedDir::open(&root_path).unwrap();
        let private_path = std::cell::RefCell::new(None);
        let replacement_inode = std::cell::Cell::new(0);
        let evidence = physical.join("owned-final-tree");

        let error = root.remove_owned_tree_with_hooks(
            || {},
            || {},
            || {},
            || {},
            || {
                let namespace = physical.join(".mac-worker-rooted-fs");
                let private = fs::read_dir(&namespace)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .find(|path| path.is_dir())
                    .unwrap();
                fs::rename(&private, &evidence).unwrap();
                fs::create_dir(&private).unwrap();
                replacement_inode.set(fs::metadata(&private).unwrap().ino());
                private_path.replace(Some(private));
            },
        );

        assert!(error.is_err());
        let private = private_path.into_inner().unwrap();
        assert!(private.is_dir());
        assert_eq!(
            fs::metadata(&private).unwrap().ino(),
            replacement_inode.get()
        );
        assert!(evidence.is_dir());
    }
}
