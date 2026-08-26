use std::{
    ffi::{CStr, CString},
    fs::File,
    io::{self, Read},
    mem::MaybeUninit,
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
        unix::ffi::OsStrExt,
    },
    path::Path,
};

use crate::inputs::RelativePath;

const DIRECTORY_OPEN_FLAGS: libc::c_int =
    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW;
const REGULAR_OPEN_FLAGS: libc::c_int =
    libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;
const MAX_SYMLINK_TARGET: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    RegularFile,
    Symlink,
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
        let (destination_parent, destination_name) = destination.open_parent(path, true)?;

        copy_regular(
            &source_parent,
            &source_name,
            source,
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

    pub fn remove_owned_tree(&self) -> io::Result<()> {
        self.verify_root_name()?;
        remove_directory_contents(self.root.as_raw_fd())?;
        self.verify_root_name()?;
        unlink_at(self.parent.as_raw_fd(), &self.root_name, libc::AT_REMOVEDIR)
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

#[derive(Clone, Copy, PartialEq, Eq)]
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
    let path = CString::new(path.as_os_str().as_bytes()).map_err(interior_nul_error)?;
    // SAFETY: `path` is a live NUL-terminated byte string, and `open` does
    // not retain its pointer after the call.
    let descriptor = unsafe { libc::open(path.as_ptr(), DIRECTORY_OPEN_FLAGS) };
    owned_fd(descriptor)
        .or_else(|error| normalize_directory_symlink_error(error, libc::AT_FDCWD, &path))
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

#[cfg(target_os = "macos")]
fn copy_regular(
    source_parent: &OwnedFd,
    source_name: &CStr,
    source: OwnedFd,
    destination_parent: &OwnedFd,
    destination_name: &CStr,
    mode: libc::mode_t,
) -> io::Result<()> {
    // SAFETY: both directory descriptors and component strings remain live;
    // clonefileat retains none of them after returning.
    let result = unsafe {
        libc::clonefileat(
            source_parent.as_raw_fd(),
            source_name.as_ptr(),
            destination_parent.as_raw_fd(),
            destination_name.as_ptr(),
            0,
        )
    };
    let result = if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    };
    complete_clone_or_copy(
        result,
        source,
        destination_parent.as_raw_fd(),
        destination_name,
        mode,
    )
}

#[cfg(target_os = "macos")]
fn complete_clone_or_copy(
    clone_result: io::Result<()>,
    source: OwnedFd,
    destination_parent: RawFd,
    destination_name: &CStr,
    mode: libc::mode_t,
) -> io::Result<()> {
    let error = match clone_result {
        Ok(()) => {
            return finish_created_regular(destination_parent, destination_name, mode);
        }
        Err(error) => error,
    };
    if !matches!(
        error.raw_os_error(),
        Some(libc::ENOTSUP) | Some(libc::EXDEV) | Some(libc::EINVAL)
    ) {
        return Err(error);
    }
    remove_failed_clone_destination(destination_parent, destination_name)?;
    copy_regular_bytes(source, destination_parent, destination_name, mode)
}

#[cfg(not(target_os = "macos"))]
fn copy_regular(
    _source_parent: &OwnedFd,
    _source_name: &CStr,
    source: OwnedFd,
    destination_parent: &OwnedFd,
    destination_name: &CStr,
    mode: libc::mode_t,
) -> io::Result<()> {
    copy_regular_bytes(
        source,
        destination_parent.as_raw_fd(),
        destination_name,
        mode,
    )
}

#[cfg(target_os = "macos")]
fn remove_failed_clone_destination(parent: RawFd, name: &CStr) -> io::Result<()> {
    let metadata = match stat_at(parent, name) {
        Ok(metadata) => metadata,
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => return Ok(()),
        Err(error) => return Err(error),
    };
    if file_type(metadata.st_mode) != libc::S_IFREG {
        return Err(os_error(libc::EEXIST));
    }
    let opened = open_regular_at(parent, name)?;
    let opened_metadata = stat_fd(opened.as_raw_fd())?;
    if !same_file(&metadata, &opened_metadata) {
        return Err(os_error(libc::ESTALE));
    }
    drop(opened);
    unlink_at(parent, name, 0)
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
                let child = open_directory_at(directory, &name)?;
                make_directory_read_only(child.as_raw_fd())?;
            }
            libc::S_IFREG => {
                let file = open_regular_at(directory, &name)?;
                let mode = if metadata.st_mode & 0o111 != 0 {
                    0o555
                } else {
                    0o444
                };
                chmod_fd(file.as_raw_fd(), mode)?;
            }
            libc::S_IFLNK => {}
            _ => return Err(invalid_type_error()),
        }
    }
    chmod_fd(directory, 0o555)
}

fn remove_directory_contents(directory: RawFd) -> io::Result<()> {
    chmod_fd(directory, 0o700)?;
    for name in directory_entries(directory)? {
        let metadata = stat_at(directory, &name)?;
        match file_type(metadata.st_mode) {
            libc::S_IFDIR => {
                let child = open_directory_at(directory, &name)?;
                let identity = FileIdentity::from_stat(&stat_fd(child.as_raw_fd())?);
                remove_directory_contents(child.as_raw_fd())?;
                let current = stat_at(directory, &name)?;
                if file_type(current.st_mode) != libc::S_IFDIR
                    || FileIdentity::from_stat(&current) != identity
                {
                    return Err(os_error(libc::ESTALE));
                }
                unlink_at(directory, &name, libc::AT_REMOVEDIR)?;
            }
            libc::S_IFREG | libc::S_IFLNK => unlink_at(directory, &name, 0)?,
            _ => return Err(invalid_type_error()),
        }
    }
    Ok(())
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
    };

    use super::{RootedDir, complete_clone_or_copy, create_regular_at, open_regular_at};
    use crate::inputs::RelativePath;

    #[test]
    fn clone_fallback_removes_only_a_failed_regular_clone_before_byte_copy() {
        // Catches byte fallback opening a partial clone without O_EXCL, or
        // leaving its wrong bytes/mode in the destination.
        let fixture = tempfile::tempdir().unwrap();
        let source_path = fixture.path().join("source");
        let destination_path = fixture.path().join("destination");
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
        let mut partial = std::fs::File::from(
            create_regular_at(destination_parent.as_raw_fd(), &destination_name).unwrap(),
        );
        partial.write_all(b"partial clone").unwrap();
        drop(partial);

        complete_clone_or_copy(
            Err(std::io::Error::from_raw_os_error(libc::ENOTSUP)),
            source,
            destination_parent.as_raw_fd(),
            &destination_name,
            0o555,
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
    }
}
