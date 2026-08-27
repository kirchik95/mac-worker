use std::{
    ffi::{CStr, CString},
    fs::File,
    io::{self, Read},
    mem::MaybeUninit,
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
        unix::ffi::OsStrExt,
    },
    path::{Component, Path},
};

use crate::inputs::RelativePath;

const DIRECTORY_OPEN_FLAGS: libc::c_int =
    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW;
const REGULAR_OPEN_FLAGS: libc::c_int =
    libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;
const MAX_SYMLINK_TARGET: usize = 64 * 1024;
const PRIVATE_NAMESPACE_NAME: &CStr = c".mac-worker-rooted-fs";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    RegularFile,
    Symlink,
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
        })
    }

    pub fn create(path: &Path) -> io::Result<Self> {
        let (parent_path, root_name) = split_root_path(path)?;
        let parent = open_directory_path(parent_path)?;
        mkdir_at(parent.as_raw_fd(), &root_name, 0o700)?;
        let root = match open_directory_at(parent.as_raw_fd(), &root_name) {
            Ok(root) => root,
            Err(error) => {
                let _ = unlink_at(parent.as_raw_fd(), &root_name, libc::AT_REMOVEDIR);
                return Err(error);
            }
        };
        let root_identity = FileIdentity::from_stat(&stat_fd(root.as_raw_fd())?);
        Ok(Self {
            root,
            parent,
            root_name,
            root_identity,
        })
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

    pub fn remove_owned_tree(&self) -> io::Result<()> {
        self.remove_owned_tree_with_hook(|| {})
    }

    fn remove_owned_tree_with_hook(
        &self,
        after_private_validation: impl FnOnce(),
    ) -> io::Result<()> {
        self.remove_owned_tree_with_hooks(|| {}, || {}, after_private_validation)
    }

    fn remove_owned_tree_with_hooks(
        &self,
        before_private_rename: impl FnOnce(),
        after_private_rename: impl FnOnce(),
        after_private_validation: impl FnOnce(),
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
        let moved = stat_at(namespace.directory.as_raw_fd(), &private_name);
        let validation = moved.and_then(|moved| {
            let opened = stat_fd(self.root.as_raw_fd())?;
            if file_type(moved.st_mode) != libc::S_IFDIR
                || FileIdentity::from_stat(&moved) != self.root_identity
                || !same_file(&moved, &opened)
            {
                return Err(os_error(libc::ESTALE));
            }
            Ok(())
        });
        let validation = validation.and_then(|()| injected_cleanup_validation_result());
        if let Err(validation_error) = validation {
            let rename_restoration = rename_no_replace(
                namespace.directory.as_raw_fd(),
                &private_name,
                self.parent.as_raw_fd(),
                &self.root_name,
            );
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
        unlink_at(
            namespace.directory.as_raw_fd(),
            &private_name,
            libc::AT_REMOVEDIR,
        )?;
        Ok(())
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

    fn verify_root_name(&self) -> io::Result<()> {
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
        Ok(())
    }
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

fn open_directory_at(parent: RawFd, name: &CStr) -> io::Result<OwnedFd> {
    // SAFETY: `name` is NUL-terminated for the duration of `openat`; `parent`
    // is an owned, live directory descriptor at every call site.
    let descriptor = unsafe { libc::openat(parent, name.as_ptr(), DIRECTORY_OPEN_FLAGS) };
    owned_fd(descriptor).or_else(|error| normalize_directory_symlink_error(error, parent, name))
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
        os::{fd::AsRawFd, unix::fs::PermissionsExt},
        path::Path,
    };

    use super::{
        AtomicRenameCapability, AtomicRenameCapabilityOverride, CleanupFault, CleanupFaultOverride,
        CopyPrivateCleanupFailureOverride, PrivateNamespace, RenameNoReplaceOverride, RootedDir,
        copy_regular, copy_regular_with_clone, copy_regular_with_clone_and_publish,
        create_regular_at, link_at, make_regular_read_only_with_hook, open_directory_path,
        open_regular_at, open_verified_child_directory, publish_regular_with_link_ops, stat_at,
        unsupported_rename_no_replace,
    };
    use crate::inputs::RelativePath;

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
            )
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::EEXIST));
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

        assert_eq!(error.raw_os_error(), Some(libc::EIO));
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
        assert_eq!(acquired.len(), 1);
        assert!(acquired[0].is_dir());
        assert!(fs::read_dir(&acquired[0]).unwrap().count() >= 1);
        fs::remove_dir_all(&namespace_path).unwrap();
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
        assert_eq!(acquired.len(), 1);
        assert!(acquired[0].is_dir());
        assert_eq!(fs::read_dir(&acquired[0]).unwrap().count(), 0);
        fs::remove_dir_all(&namespace_path).unwrap();
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
}
