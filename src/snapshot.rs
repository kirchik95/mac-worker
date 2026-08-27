use std::{
    ffi::{CStr, CString},
    fs::{self, File, OpenOptions},
    io::{self, Write},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::{
            ffi::OsStrExt,
            fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
        },
    },
    path::{Component, Path, PathBuf},
};

use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    error::WorkerError,
    inputs::{
        InputOrigin, InputSelection, InputSelector, RelativePath, SelectedInput, SelectedInputKind,
    },
    manifest::{ManifestEntry, ManifestEntryKind, SnapshotManifest},
    process::ProcessRunner,
    project::ProjectContext,
    project_config::SnapshotSettings,
    rooted_fs::{EntryKind, RootedDir},
};

const DIRECTORY_OPEN_FLAGS: libc::c_int =
    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW;
const ROOTED_FS_NAMESPACE: &str = ".mac-worker-rooted-fs";

#[doc(hidden)]
pub trait SnapshotHook: Send + Sync {
    fn after_materialization(&self, tree: &Path) -> io::Result<()>;

    #[doc(hidden)]
    fn after_publication(&self, _capture: &Path) -> io::Result<()> {
        Ok(())
    }
}

struct NoopSnapshotHook;

impl SnapshotHook for NoopSnapshotHook {
    fn after_materialization(&self, _tree: &Path) -> io::Result<()> {
        Ok(())
    }
}

static NOOP_SNAPSHOT_HOOK: NoopSnapshotHook = NoopSnapshotHook;

pub struct SnapshotBuilder<'a> {
    selector: InputSelector<'a>,
    cache_root: &'a Path,
    hook: &'a dyn SnapshotHook,
}

pub struct Snapshot {
    pub capture_id: Uuid,
    pub root: PathBuf,
    pub manifest_path: PathBuf,
    pub manifest: SnapshotManifest,
    pub digest: String,
    pub file_count: usize,
    pub total_bytes: u64,
    included_untracked_count: usize,
    warning_count: usize,
    publication_root: PathBuf,
    owned_capture: RootedDir,
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Snapshot")
            .field("capture_id", &self.capture_id)
            .field("root", &self.root)
            .field("manifest_path", &self.manifest_path)
            .field("manifest", &self.manifest)
            .field("digest", &self.digest)
            .field("file_count", &self.file_count)
            .field("total_bytes", &self.total_bytes)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SnapshotSummary {
    pub digest: String,
    pub file_count: usize,
    pub total_bytes: u64,
    pub tracked_deletion_count: usize,
    pub included_untracked_count: usize,
    pub warning_count: usize,
}

impl Snapshot {
    pub fn publication_root(&self) -> &Path {
        &self.publication_root
    }

    pub fn summary(&self) -> SnapshotSummary {
        SnapshotSummary {
            digest: self.digest.clone(),
            file_count: self.file_count,
            total_bytes: self.total_bytes,
            tracked_deletion_count: self.manifest.tracked_deletions.len(),
            included_untracked_count: self.included_untracked_count,
            warning_count: self.warning_count,
        }
    }

    pub fn cleanup(&self) -> Result<(), WorkerError> {
        self.owned_capture
            .remove_owned_tree()
            .map_err(WorkerError::Io)
    }
}

impl<'a> SnapshotBuilder<'a> {
    pub fn new(runner: &'a dyn ProcessRunner, cache_root: &'a Path) -> Self {
        Self::with_hook(runner, cache_root, &NOOP_SNAPSHOT_HOOK)
    }

    #[doc(hidden)]
    pub fn with_hook(
        runner: &'a dyn ProcessRunner,
        cache_root: &'a Path,
        hook: &'a dyn SnapshotHook,
    ) -> Self {
        Self {
            selector: InputSelector::new(runner),
            cache_root,
            hook,
        }
    }

    pub fn capture(
        &self,
        context: &ProjectContext,
        settings: &SnapshotSettings,
        initial: InputSelection,
    ) -> Result<Snapshot, WorkerError> {
        let cache_root = resolve_cache_root(self.cache_root)?;
        let snapshots = cache_root.join("snapshots");
        let staging = snapshots.join("staging");
        let ready = snapshots.join("ready");
        ensure_directory_chain(&staging)?;
        ensure_directory_chain(&ready)?;

        let (capture_id, partial_path, mut partial, partial_identity) =
            create_unique_partial(&staging)?;
        let tree_path = partial_path.join("tree");
        let tree = match RootedDir::create(&tree_path) {
            Ok(tree) => tree,
            Err(error) => return fail_with_owned_cleanup(&partial, WorkerError::Io(error)),
        };

        let prepared = match self.prepare_capture(context, settings, &initial, &tree_path, &tree) {
            Ok(prepared) => prepared,
            Err(error) => return fail_with_owned_cleanup(&partial, error),
        };

        let manifest_path = partial_path.join("manifest.json");
        if let Err(error) = write_manifest(&manifest_path, &prepared.canonical_bytes) {
            return fail_with_owned_cleanup(&partial, WorkerError::Io(error));
        }
        if let Err(error) = sync_directory(&partial_path) {
            return fail_with_owned_cleanup(&partial, WorkerError::Io(error));
        }
        if let Err(error) = tree.make_read_only() {
            return fail_with_owned_cleanup(&partial, WorkerError::Io(error));
        }
        if let Err(error) = sync_directory(&tree_path).and_then(|()| sync_directory(&partial_path))
        {
            return fail_with_owned_cleanup(&partial, WorkerError::Io(error));
        }
        match path_identity(&partial_path) {
            Ok(identity) if identity == partial_identity => {}
            Ok(_) => {
                return fail_with_owned_cleanup(
                    &partial,
                    snapshot_error(
                        "SNAPSHOT_CHANGED",
                        "the owned staging directory changed before publication",
                    ),
                );
            }
            Err(error) => return fail_with_owned_cleanup(&partial, WorkerError::Io(error)),
        }

        let ready_capture = ready.join(capture_id.to_string());
        if let Err(error) = partial.publish_owned_to(&ready_capture) {
            let primary = if matches!(error.raw_os_error(), Some(libc::EEXIST | libc::ENOTEMPTY)) {
                snapshot_error(
                    "SNAPSHOT_PUBLICATION_CONFLICT",
                    "the unique snapshot publication path already exists",
                )
            } else {
                WorkerError::Io(error)
            };
            return fail_with_owned_cleanup(&partial, primary);
        }
        if let Err(error) = self.hook.after_publication(&ready_capture) {
            return fail_with_owned_cleanup(&partial, WorkerError::Io(error));
        }
        if let Err(error) = partial.sync_parent() {
            return fail_with_owned_cleanup(&partial, WorkerError::Io(error));
        }

        Ok(Snapshot {
            capture_id,
            root: ready_capture.join("tree"),
            manifest_path: ready_capture.join("manifest.json"),
            manifest: prepared.manifest,
            digest: prepared.digest,
            file_count: prepared.file_count,
            total_bytes: prepared.total_bytes,
            included_untracked_count: initial
                .entries
                .iter()
                .filter(|entry| entry.origin != InputOrigin::Tracked)
                .count(),
            warning_count: initial.warnings.len(),
            publication_root: ready_capture,
            owned_capture: partial,
        })
    }

    fn prepare_capture(
        &self,
        context: &ProjectContext,
        settings: &SnapshotSettings,
        initial: &InputSelection,
        tree_path: &Path,
        tree: &RootedDir,
    ) -> Result<PreparedCapture, WorkerError> {
        let source = RootedDir::open(&context.root).map_err(map_source_error)?;
        let working_directory = validated_working_directory(context)?;
        let mut entries =
            Vec::with_capacity(initial.entries.len() + usize::from(working_directory.is_some()));
        for selected in &initial.entries {
            let fingerprint = materialize_entry(&source, tree, selected)?;
            entries.push(fingerprint);
        }
        if let Some(working_directory) = &working_directory
            && !initial
                .entries
                .iter()
                .any(|selected| selected.path == working_directory.path)
        {
            let fingerprint = materialize_entry(&source, tree, working_directory)?;
            entries.push(fingerprint);
        }
        remove_empty_rooted_fs_namespace(
            tree_path
                .parent()
                .expect("a staging tree always has its partial parent"),
        )?;

        self.hook
            .after_materialization(tree_path)
            .map_err(WorkerError::Io)?;

        let second = self.selector.select(context, settings).map_err(|error| {
            snapshot_error(
                "SNAPSHOT_CHANGED",
                format!("input selection changed during capture: {}", error.code),
            )
        })?;
        if &second != initial {
            return Err(snapshot_error(
                "SNAPSHOT_CHANGED",
                "input paths, origins, deletions, or warnings changed during capture",
            ));
        }

        for (selected, first) in initial.entries.iter().zip(&entries) {
            let second =
                fingerprint_selected(&source, selected).map_err(map_verification_source_error)?;
            if &second != first {
                return Err(snapshot_error(
                    "SNAPSHOT_CHANGED",
                    format!("selected input {} changed during capture", selected.path),
                ));
            }
        }

        entries.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
        let mut tracked_deletions = initial
            .tracked_deletions
            .iter()
            .map(|path| path.as_str().to_owned())
            .collect::<Vec<_>>();
        tracked_deletions.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        let relative_working_dir = context
            .relative_cwd
            .to_str()
            .ok_or_else(|| {
                snapshot_error(
                    "UNSUPPORTED_PATH_ENCODING",
                    "the relative working directory is not valid UTF-8",
                )
            })?
            .to_owned();
        let manifest = SnapshotManifest {
            version: 1,
            project_id: context.project_id.clone(),
            worktree_id: context.worktree_id.clone(),
            head: context.head.clone(),
            branch: context.branch.clone(),
            dirty: context.dirty,
            relative_working_dir,
            entries,
            tracked_deletions,
        };
        let canonical_bytes = manifest.canonical_bytes().map_err(manifest_error)?;
        let digest = manifest.digest().map_err(manifest_error)?;
        let file_count = manifest
            .entries
            .iter()
            .filter(|entry| entry.kind != ManifestEntryKind::Directory)
            .count();
        let total_bytes = manifest.entries.iter().try_fold(0_u64, |total, entry| {
            total.checked_add(entry.size).ok_or_else(|| {
                snapshot_error("SNAPSHOT_TOO_LARGE", "snapshot byte count overflowed")
            })
        })?;

        Ok(PreparedCapture {
            manifest,
            canonical_bytes,
            digest,
            file_count,
            total_bytes,
        })
    }
}

fn validated_working_directory(
    context: &ProjectContext,
) -> Result<Option<SelectedInput>, WorkerError> {
    if context.relative_cwd.as_os_str().is_empty() {
        return Ok(None);
    }
    let path = RelativePath::parse(context.relative_cwd.as_os_str().as_bytes()).map_err(|_| {
        snapshot_error(
            "UNSUPPORTED_PATH_ENCODING",
            "the relative working directory is not a safe UTF-8 path",
        )
    })?;
    open_existing_directory_chain(&context.root.join(path.as_path())).map_err(|error| {
        if matches!(
            error.raw_os_error(),
            Some(libc::ENOENT | libc::ENOTDIR | libc::ELOOP | libc::ESTALE)
        ) {
            snapshot_error(
                "SNAPSHOT_CHANGED",
                "the relative working directory changed before capture",
            )
        } else {
            WorkerError::Io(error)
        }
    })?;
    Ok(Some(SelectedInput {
        path,
        origin: InputOrigin::IncludedUntracked,
        kind: SelectedInputKind::EmptyDirectory,
    }))
}

fn open_existing_directory_chain(path: &Path) -> io::Result<()> {
    let start = if path.is_absolute() { c"/" } else { c"." };
    let mut current = open_directory_at(libc::AT_FDCWD, start)?;
    let mut saw_component = false;
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(component) => {
                saw_component = true;
                let name = CString::new(component.as_bytes()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "working directory contains a NUL byte",
                    )
                })?;
                current = open_directory_at(current.as_raw_fd(), &name)?;
            }
            Component::ParentDir | Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "working directory contains unsupported traversal",
                ));
            }
        }
    }
    if !saw_component {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "working directory must identify a directory",
        ));
    }
    Ok(())
}

struct PreparedCapture {
    manifest: SnapshotManifest,
    canonical_bytes: Vec<u8>,
    digest: String,
    file_count: usize,
    total_bytes: u64,
}

fn materialize_entry(
    source: &RootedDir,
    tree: &RootedDir,
    selected: &SelectedInput,
) -> Result<ManifestEntry, WorkerError> {
    let fingerprint = fingerprint_selected(source, selected).map_err(map_source_error)?;
    match fingerprint.kind {
        ManifestEntryKind::File => source
            .copy_regular_to(&selected.path, tree)
            .map_err(map_source_error)?,
        ManifestEntryKind::Symlink => tree
            .create_symlink(
                &selected.path,
                fingerprint
                    .symlink_target
                    .as_deref()
                    .expect("symlink fingerprints always contain their target"),
            )
            .map_err(WorkerError::Io)?,
        ManifestEntryKind::Directory => tree
            .create_empty_directory(&selected.path)
            .map_err(WorkerError::Io)?,
    }
    let staged = fingerprint_selected(tree, selected).map_err(WorkerError::Io)?;
    if staged != fingerprint {
        return Err(snapshot_error(
            "SNAPSHOT_CHANGED",
            format!(
                "materialized input {} does not match its source fingerprint",
                selected.path
            ),
        ));
    }
    Ok(fingerprint)
}

fn fingerprint_selected(root: &RootedDir, selected: &SelectedInput) -> io::Result<ManifestEntry> {
    if selected.kind == SelectedInputKind::EmptyDirectory {
        return Ok(ManifestEntry {
            path: selected.path.as_str().to_owned(),
            kind: ManifestEntryKind::Directory,
            mode: 0o755,
            size: 0,
            sha256: format!("{:x}", Sha256::digest(b"directory\0")),
            symlink_target: None,
        });
    }

    let mut inspection = root.inspect(&selected.path)?;
    let before = inspection.metadata();
    match before.kind {
        EntryKind::RegularFile => {
            let mut hasher = Sha256::new();
            let bytes_read = io::copy(&mut inspection, &mut HashWriter(&mut hasher))?;
            let after = inspection.restat()?;
            if before != after || bytes_read != before.size {
                return Err(io::Error::from_raw_os_error(libc::ESTALE));
            }
            Ok(ManifestEntry {
                path: selected.path.as_str().to_owned(),
                kind: ManifestEntryKind::File,
                mode: if before.mode & 0o111 != 0 {
                    0o755
                } else {
                    0o644
                },
                size: bytes_read,
                sha256: format!("{:x}", hasher.finalize()),
                symlink_target: None,
            })
        }
        EntryKind::Symlink => {
            drop(inspection);
            let target = root.read_symlink(&selected.path)?;
            let after = root.inspect(&selected.path)?.metadata();
            if before != after || before.size != target.len() as u64 {
                return Err(io::Error::from_raw_os_error(libc::ESTALE));
            }
            let mut hasher = Sha256::new();
            hasher.update(b"symlink\0");
            hasher.update(target.as_bytes());
            Ok(ManifestEntry {
                path: selected.path.as_str().to_owned(),
                kind: ManifestEntryKind::Symlink,
                mode: 0o777,
                size: target.len() as u64,
                sha256: format!("{:x}", hasher.finalize()),
                symlink_target: Some(target),
            })
        }
    }
}

struct HashWriter<'a>(&'a mut Sha256);

impl Write for HashWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn write_manifest(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    file.set_permissions(fs::Permissions::from_mode(0o444))?;
    file.sync_all()
}

fn remove_empty_rooted_fs_namespace(partial_path: &Path) -> Result<(), WorkerError> {
    let namespace = partial_path.join(ROOTED_FS_NAMESPACE);
    let metadata = match fs::symlink_metadata(&namespace) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(WorkerError::Io(error)),
    };
    if !metadata.file_type().is_dir()
        || metadata.uid() != effective_user_id()
        || metadata.mode() & 0o777 != 0o700
    {
        return Err(snapshot_error(
            "SNAPSHOT_CHANGED",
            "the rooted filesystem staging namespace changed unexpectedly",
        ));
    }
    fs::remove_dir(namespace).map_err(WorkerError::Io)
}

fn create_unique_partial(
    staging: &Path,
) -> Result<(Uuid, PathBuf, RootedDir, PathIdentity), WorkerError> {
    for _ in 0..16 {
        let capture_id = Uuid::new_v4();
        let path = staging.join(format!(".partial-{capture_id}"));
        match RootedDir::create(&path) {
            Ok(root) => {
                let metadata = match fs::symlink_metadata(&path) {
                    Ok(metadata) => metadata,
                    Err(error) => {
                        return fail_with_owned_cleanup(&root, WorkerError::Io(error));
                    }
                };
                if !metadata.file_type().is_dir()
                    || metadata.uid() != effective_user_id()
                    || metadata.mode() & 0o077 != 0
                {
                    return fail_with_owned_cleanup(
                        &root,
                        WorkerError::Io(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "snapshot staging root is not an owner-only directory",
                        )),
                    );
                }
                let identity = PathIdentity {
                    device: metadata.dev(),
                    inode: metadata.ino(),
                };
                return Ok((capture_id, path, root, identity));
            }
            Err(error) if error.raw_os_error() == Some(libc::EEXIST) => continue,
            Err(error) => return Err(WorkerError::Io(error)),
        }
    }
    Err(snapshot_error(
        "SNAPSHOT_ID_EXHAUSTED",
        "could not allocate a unique snapshot staging path",
    ))
}

fn fail_with_owned_cleanup<T>(root: &RootedDir, primary: WorkerError) -> Result<T, WorkerError> {
    match root.remove_owned_tree() {
        Ok(()) => Err(primary),
        Err(cleanup_error) => Err(WorkerError::Io(cleanup_error)),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PathIdentity {
    device: u64,
    inode: u64,
}

fn path_identity(path: &Path) -> io::Result<PathIdentity> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Err(io::Error::from_raw_os_error(
            if metadata.file_type().is_symlink() {
                libc::ELOOP
            } else {
                libc::ENOTDIR
            },
        ));
    }
    Ok(PathIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

fn ensure_directory_chain(path: &Path) -> io::Result<()> {
    let start = if path.is_absolute() { c"/" } else { c"." };
    let mut current = open_directory_at(libc::AT_FDCWD, start)?;
    let mut saw_component = false;
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(component) => {
                saw_component = true;
                let name = CString::new(component.as_bytes()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "cache path contains a NUL byte",
                    )
                })?;
                current = match open_directory_at(current.as_raw_fd(), &name) {
                    Ok(directory) => directory,
                    Err(error) if error.raw_os_error() == Some(libc::ENOENT) => {
                        mkdir_at(current.as_raw_fd(), &name, 0o700)?;
                        open_directory_at(current.as_raw_fd(), &name)?
                    }
                    Err(error) => return Err(error),
                };
            }
            Component::ParentDir | Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cache path contains unsupported traversal",
                ));
            }
        }
    }
    if !saw_component {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cache path must identify a directory",
        ));
    }
    Ok(())
}

fn resolve_cache_root(path: &Path) -> io::Result<PathBuf> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                return Err(io::Error::from_raw_os_error(libc::ELOOP));
            }
            if !metadata.file_type().is_dir() {
                return Err(io::Error::from_raw_os_error(libc::ENOTDIR));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(path)?;
            let metadata = fs::symlink_metadata(path)?;
            if metadata.file_type().is_symlink() {
                return Err(io::Error::from_raw_os_error(libc::ELOOP));
            }
            if !metadata.file_type().is_dir() {
                return Err(io::Error::from_raw_os_error(libc::ENOTDIR));
            }
        }
        Err(error) => return Err(error),
    }
    fs::canonicalize(path)
}

fn open_directory_at(parent: RawFd, name: &CStr) -> io::Result<OwnedFd> {
    // SAFETY: `parent` is live and `name` is NUL-terminated for this call.
    let descriptor = unsafe { libc::openat(parent, name.as_ptr(), DIRECTORY_OPEN_FLAGS) };
    if descriptor >= 0 {
        // SAFETY: a successful openat returns one newly owned descriptor.
        return Ok(unsafe { OwnedFd::from_raw_fd(descriptor) });
    }
    let error = io::Error::last_os_error();
    if matches!(error.raw_os_error(), Some(libc::ENOTDIR | libc::ELOOP))
        && is_symlink_at(parent, name).unwrap_or(false)
    {
        return Err(io::Error::from_raw_os_error(libc::ELOOP));
    }
    Err(error)
}

fn is_symlink_at(parent: RawFd, name: &CStr) -> io::Result<bool> {
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: the output pointer is valid and initialized on success; the
    // descriptor and component remain live for this non-retaining call.
    let result = unsafe {
        libc::fstatat(
            parent,
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fstatat succeeded and initialized the entire stat value.
    let metadata = unsafe { metadata.assume_init() };
    Ok(metadata.st_mode & libc::S_IFMT == libc::S_IFLNK)
}

fn mkdir_at(parent: RawFd, name: &CStr, mode: libc::mode_t) -> io::Result<()> {
    // SAFETY: the descriptor and NUL-terminated component remain live for this
    // call and mkdirat does not retain either.
    let result = unsafe { libc::mkdirat(parent, name.as_ptr(), mode) };
    if result == 0 {
        Ok(())
    } else {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EEXIST) {
            Ok(())
        } else {
            Err(error)
        }
    }
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn effective_user_id() -> u32 {
    // SAFETY: geteuid has no arguments and no memory-safety contract.
    unsafe { libc::geteuid() }
}

fn map_source_error(error: io::Error) -> WorkerError {
    if error.kind() == io::ErrorKind::InvalidData {
        return snapshot_error(
            "UNSUPPORTED_PATH_ENCODING",
            "a selected symlink target is not valid UTF-8",
        );
    }
    if matches!(
        error.raw_os_error(),
        Some(libc::ENOENT | libc::ENOTDIR | libc::ELOOP | libc::ESTALE | libc::EINVAL)
    ) {
        return snapshot_error(
            "SNAPSHOT_CHANGED",
            "a selected source entry changed during capture",
        );
    }
    WorkerError::Io(error)
}

fn map_verification_source_error(error: io::Error) -> WorkerError {
    if error.kind() == io::ErrorKind::InvalidData {
        return snapshot_error(
            "SNAPSHOT_CHANGED",
            "a selected symlink target changed to an unsupported encoding during capture",
        );
    }
    map_source_error(error)
}

fn manifest_error(error: serde_json::Error) -> WorkerError {
    snapshot_error(
        "SNAPSHOT_MANIFEST_FAILED",
        format!("could not serialize the canonical snapshot manifest: {error}"),
    )
}

fn snapshot_error(code: &'static str, message: impl Into<String>) -> WorkerError {
    WorkerError::Snapshot {
        code,
        message: message.into(),
    }
}
