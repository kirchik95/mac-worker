use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::{CStr, CString},
    fmt,
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
    mem::MaybeUninit,
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
        unix::{ffi::OsStrExt, fs::FileExt},
    },
    path::{Component, Path, PathBuf},
    sync::{Arc, Condvar, LazyLock, Mutex},
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
const CLEANUP_CAPABILITY_PROBE_NAME: &CStr = c"cleanup-capability-probe-v1";
const CLEANUP_CAPABILITY_PROBE_LEFT: &CStr = c"left-v1";
const CLEANUP_CAPABILITY_PROBE_RIGHT: &CStr = c"right-v1";
const CLEANUP_PROBE_REGULAR_SOURCE: &CStr = c"regular-no-replace-source-v1";
const CLEANUP_PROBE_REGULAR_DESTINATION: &CStr = c"regular-no-replace-destination-v1";
const CLEANUP_PROBE_DIRECTORY_SOURCE: &CStr = c"directory-no-replace-source-v1";
const CLEANUP_PROBE_DIRECTORY_DESTINATION: &CStr = c"directory-no-replace-destination-v1";
const CLEANUP_PROBE_EXCHANGE_LEFT: &CStr = c"directory-exchange-left-v1";
const CLEANUP_PROBE_EXCHANGE_RIGHT: &CStr = c"directory-exchange-right-v1";
const CLEANUP_PROBE_REGULAR_EXCHANGE_LEFT: &CStr = c"regular-exchange-left-v1";
const CLEANUP_PROBE_REGULAR_EXCHANGE_RIGHT: &CStr = c"regular-exchange-right-v1";

struct CleanupProcessKeyRegistry {
    active: Mutex<BTreeSet<String>>,
    released: Condvar,
}

static CLEANUP_PROCESS_KEYS: LazyLock<CleanupProcessKeyRegistry> =
    LazyLock::new(|| CleanupProcessKeyRegistry {
        active: Mutex::new(BTreeSet::new()),
        released: Condvar::new(),
    });

struct CleanupProcessKeyGuard {
    key: String,
}

impl Drop for CleanupProcessKeyGuard {
    fn drop(&mut self) {
        let mut active = match CLEANUP_PROCESS_KEYS.active.lock() {
            Ok(active) => active,
            Err(poisoned) => poisoned.into_inner(),
        };
        active.remove(&self.key);
        CLEANUP_PROCESS_KEYS.released.notify_all();
    }
}

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
    NoExchange,
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
    static TEST_CLEANUP_DECISION_FAULT: std::cell::Cell<Option<CleanupDecisionFault>> = const {
        std::cell::Cell::new(None)
    };
    static TEST_CLEANUP_PROBE_INTERRUPTED: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static TEST_CLEANUP_PROBE_TRANSITION: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
    static TEST_CLEANUP_INTENT_HANDOFF: std::cell::RefCell<Option<CleanupIntentHandoff>> = const {
        std::cell::RefCell::new(None)
    };
    static TEST_CLEANUP_BOOTSTRAP_HANDOFF: std::cell::RefCell<Option<CleanupBootstrapHandoff>> = const {
        std::cell::RefCell::new(None)
    };
    static TEST_CLEANUP_TERMINAL_HANDOFF: std::cell::RefCell<Option<CleanupTerminalHandoff>> = const {
        std::cell::RefCell::new(None)
    };
    static TEST_CLEANUP_DECISION_REWRITE_HANDOFF: std::cell::RefCell<Option<CleanupDecisionRewriteHandoff>> = const {
        std::cell::RefCell::new(None)
    };
    static TEST_CLEANUP_INTENT_RETIRE_HANDOFF: std::cell::RefCell<Option<CleanupIntentRetireHandoff>> = const {
        std::cell::RefCell::new(None)
    };
    static TEST_CLEANUP_PREDICATE_COMPLETION_HANDOFF: std::cell::RefCell<Option<CleanupPredicateCompletionHandoff>> = const {
        std::cell::RefCell::new(None)
    };
    static TEST_CLEANUP_RESTORE_SYNC_TRACE: std::cell::RefCell<Option<Vec<CleanupRestoreSyncBoundary>>> = const {
        std::cell::RefCell::new(None)
    };
    static TEST_CLEANUP_SAME_CALL_TRANSITION: std::cell::Cell<Option<fn()>> = const {
        std::cell::Cell::new(None)
    };
    static TEST_REGULAR_PARENT_ADMISSION_HOOK: std::cell::Cell<Option<fn()>> = const {
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
    DuringCleanupCapabilityProbe(usize, libc::c_int),
    AfterCleanupCapabilityProbeSync(libc::c_int),
    AfterCleanupPlaceholderObjectSync(libc::c_int),
    AfterCleanupPlaceholder(libc::c_int),
    DuringCleanupIntentWrite(libc::c_int),
    AfterCleanupIntentWriteBeforeSync(libc::c_int),
    AfterCleanupIntentAdoptionSync(libc::c_int),
    AfterCleanupIntentSync(libc::c_int),
    AfterCleanupIntentPublish(libc::c_int),
    BeforeBoundCompletion(libc::c_int),
    AfterTargetChmod(libc::c_int),
    AfterFirstRemoval(libc::c_int),
    BeforeFinalRootRemoval(libc::c_int),
    AfterCleanupTargetExchange(libc::c_int),
    AfterCleanupQuarantineNamespaceSync(libc::c_int),
    AfterCleanupQuarantineParentSync(libc::c_int),
    AfterCleanupQuarantineRename(libc::c_int),
    AfterCleanupPlaceholderRemoval(libc::c_int),
    AfterCleanupOperationRemoval(libc::c_int),
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanupDecisionFault {
    AfterStageSync(libc::c_int),
    DuringWrite(libc::c_int),
    AfterFullWriteBeforeSync(libc::c_int),
    AfterCompleteAdoptionSync(libc::c_int),
    BeforeRename(libc::c_int),
    AfterRename(libc::c_int),
    AfterDestinationSync(libc::c_int),
    AfterSourceSync(libc::c_int),
    AfterRestoreDestinationSync(libc::c_int),
    AfterRestoreSourceSync(libc::c_int),
    DuringRestoreRollback(libc::c_int),
    AfterRestoreSync(libc::c_int),
    AfterDecisionRetire(libc::c_int),
    AfterIntentRetire(libc::c_int),
}

#[cfg(test)]
struct CleanupFaultOverride(Option<CleanupFault>);

#[cfg(test)]
impl CleanupFaultOverride {
    fn set(fault: CleanupFault) -> Self {
        TEST_CLEANUP_PROBE_TRANSITION.set(0);
        Self(TEST_CLEANUP_FAULT.replace(Some(fault)))
    }
}

#[cfg(test)]
impl Drop for CleanupFaultOverride {
    fn drop(&mut self) {
        TEST_CLEANUP_FAULT.set(self.0);
        TEST_CLEANUP_PROBE_TRANSITION.set(0);
    }
}

#[cfg(test)]
struct CleanupSameCallTransitionOverride(Option<fn()>);

#[cfg(test)]
impl CleanupSameCallTransitionOverride {
    fn set(hook: fn()) -> Self {
        Self(TEST_CLEANUP_SAME_CALL_TRANSITION.replace(Some(hook)))
    }
}

#[cfg(test)]
impl Drop for CleanupSameCallTransitionOverride {
    fn drop(&mut self) {
        TEST_CLEANUP_SAME_CALL_TRANSITION.set(self.0);
    }
}

#[cfg(test)]
struct RegularParentAdmissionHookOverride(Option<fn()>);

#[cfg(test)]
impl RegularParentAdmissionHookOverride {
    fn set(hook: fn()) -> Self {
        Self(TEST_REGULAR_PARENT_ADMISSION_HOOK.replace(Some(hook)))
    }
}

#[cfg(test)]
impl Drop for RegularParentAdmissionHookOverride {
    fn drop(&mut self) {
        TEST_REGULAR_PARENT_ADMISSION_HOOK.set(self.0);
    }
}

#[cfg(test)]
struct CleanupIntentHandoff {
    renamed: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
struct CleanupIntentHandoffOverride(Option<CleanupIntentHandoff>);

#[cfg(test)]
impl CleanupIntentHandoffOverride {
    fn set(renamed: std::sync::mpsc::Sender<()>, release: std::sync::mpsc::Receiver<()>) -> Self {
        Self(TEST_CLEANUP_INTENT_HANDOFF.replace(Some(CleanupIntentHandoff { renamed, release })))
    }
}

#[cfg(test)]
impl Drop for CleanupIntentHandoffOverride {
    fn drop(&mut self) {
        TEST_CLEANUP_INTENT_HANDOFF.replace(self.0.take());
    }
}

#[cfg(test)]
struct CleanupBootstrapHandoff {
    stream: std::os::unix::net::UnixStream,
}

#[cfg(test)]
struct CleanupBootstrapHandoffOverride(Option<CleanupBootstrapHandoff>);

#[cfg(test)]
impl CleanupBootstrapHandoffOverride {
    fn set(stream: std::os::unix::net::UnixStream) -> Self {
        Self(TEST_CLEANUP_BOOTSTRAP_HANDOFF.replace(Some(CleanupBootstrapHandoff { stream })))
    }
}

#[cfg(test)]
impl Drop for CleanupBootstrapHandoffOverride {
    fn drop(&mut self) {
        TEST_CLEANUP_BOOTSTRAP_HANDOFF.replace(self.0.take());
    }
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum CleanupTerminalPhase {
    Initial,
    Final,
}

#[cfg(test)]
struct CleanupTerminalHandoff {
    phase: CleanupTerminalPhase,
    reached: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
struct CleanupTerminalHandoffOverride(Option<CleanupTerminalHandoff>);

#[cfg(test)]
impl CleanupTerminalHandoffOverride {
    fn set(
        phase: CleanupTerminalPhase,
        reached: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> Self {
        Self(
            TEST_CLEANUP_TERMINAL_HANDOFF.replace(Some(CleanupTerminalHandoff {
                phase,
                reached,
                release,
            })),
        )
    }
}

#[cfg(test)]
impl Drop for CleanupTerminalHandoffOverride {
    fn drop(&mut self) {
        TEST_CLEANUP_TERMINAL_HANDOFF.replace(self.0.take());
    }
}

#[cfg(test)]
struct CleanupDecisionRewriteHandoff {
    reached: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
struct CleanupDecisionRewriteHandoffOverride(Option<CleanupDecisionRewriteHandoff>);

#[cfg(test)]
impl CleanupDecisionRewriteHandoffOverride {
    fn set(reached: std::sync::mpsc::Sender<()>, release: std::sync::mpsc::Receiver<()>) -> Self {
        Self(
            TEST_CLEANUP_DECISION_REWRITE_HANDOFF
                .replace(Some(CleanupDecisionRewriteHandoff { reached, release })),
        )
    }
}

#[cfg(test)]
impl Drop for CleanupDecisionRewriteHandoffOverride {
    fn drop(&mut self) {
        TEST_CLEANUP_DECISION_REWRITE_HANDOFF.replace(self.0.take());
    }
}

#[cfg(test)]
struct CleanupIntentRetireHandoff {
    reached: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
struct CleanupIntentRetireHandoffOverride(Option<CleanupIntentRetireHandoff>);

#[cfg(test)]
impl CleanupIntentRetireHandoffOverride {
    fn set(reached: std::sync::mpsc::Sender<()>, release: std::sync::mpsc::Receiver<()>) -> Self {
        Self(
            TEST_CLEANUP_INTENT_RETIRE_HANDOFF
                .replace(Some(CleanupIntentRetireHandoff { reached, release })),
        )
    }
}

#[cfg(test)]
impl Drop for CleanupIntentRetireHandoffOverride {
    fn drop(&mut self) {
        TEST_CLEANUP_INTENT_RETIRE_HANDOFF.replace(self.0.take());
    }
}

#[cfg(test)]
struct CleanupPredicateCompletionHandoff {
    reached: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
struct CleanupPredicateCompletionHandoffOverride(Option<CleanupPredicateCompletionHandoff>);

#[cfg(test)]
impl CleanupPredicateCompletionHandoffOverride {
    fn set(reached: std::sync::mpsc::Sender<()>, release: std::sync::mpsc::Receiver<()>) -> Self {
        Self(
            TEST_CLEANUP_PREDICATE_COMPLETION_HANDOFF
                .replace(Some(CleanupPredicateCompletionHandoff { reached, release })),
        )
    }
}

#[cfg(test)]
impl Drop for CleanupPredicateCompletionHandoffOverride {
    fn drop(&mut self) {
        TEST_CLEANUP_PREDICATE_COMPLETION_HANDOFF.replace(self.0.take());
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CleanupRestoreSyncBoundary {
    CaptureDestination,
    CaptureSource,
    Destination,
    Source,
}

#[cfg(test)]
struct CleanupRestoreSyncTraceOverride(Option<Vec<CleanupRestoreSyncBoundary>>);

#[cfg(test)]
impl CleanupRestoreSyncTraceOverride {
    fn set() -> Self {
        Self(TEST_CLEANUP_RESTORE_SYNC_TRACE.replace(Some(Vec::new())))
    }

    fn snapshot(&self) -> Vec<CleanupRestoreSyncBoundary> {
        TEST_CLEANUP_RESTORE_SYNC_TRACE.with(|trace| trace.borrow().clone().unwrap_or_default())
    }
}

#[cfg(test)]
impl Drop for CleanupRestoreSyncTraceOverride {
    fn drop(&mut self) {
        TEST_CLEANUP_RESTORE_SYNC_TRACE.replace(self.0.take());
    }
}

#[cfg(test)]
struct CleanupDecisionFaultOverride(Option<CleanupDecisionFault>);

#[cfg(test)]
impl CleanupDecisionFaultOverride {
    fn set(fault: CleanupDecisionFault) -> Self {
        Self(TEST_CLEANUP_DECISION_FAULT.replace(Some(fault)))
    }
}

#[cfg(test)]
impl Drop for CleanupDecisionFaultOverride {
    fn drop(&mut self) {
        TEST_CLEANUP_DECISION_FAULT.set(self.0);
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
    display_path: PathBuf,
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
            mode: (metadata.st_mode & 0o7777) as u32,
        }
    }
}

impl RootedDir {
    /// Returns the best-effort display path captured when this rooted handle
    /// was opened. Security-sensitive operations continue to use the retained
    /// descriptors; this is provided for child processes such as Git whose
    /// interface accepts a filesystem path.
    pub fn path(&self) -> &Path {
        &self.display_path
    }

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
            display_path: path.to_path_buf(),
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
            display_path: path.to_path_buf(),
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
                    display_path: path.to_path_buf(),
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
            display_path: self.display_path.clone(),
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
            display_path: self.display_path.join(path.as_path()),
            lineage,
            security_device,
        })
    }

    pub(crate) fn create_new_child_directory(&self, name: &str) -> io::Result<Self> {
        self.verify_root_name()?;
        let name = CString::new(name).map_err(interior_nul_error)?;
        let display_path = self.display_path.join(name.to_string_lossy().as_ref());
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
            display_path,
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
        let display_path = self.display_path.join(name.to_string_lossy().as_ref());
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
            display_path,
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

    pub fn entry_exists(&self, name: &str) -> io::Result<bool> {
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
        if (metadata.st_mode & 0o7777) == 0o700 {
            chmod_fd(self.root.as_raw_fd(), 0o500)?;
        }
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

    /// Restores the owner-only mode of a regular file that an external
    /// operation may have replaced in place. The name and inode are checked
    /// through the rooted descriptor before and after the chmod.
    pub(crate) fn set_private_regular_mode(&self, name: &str, mode: u32) -> io::Result<()> {
        self.verify_root_name()?;
        let target = CString::new(name).map_err(interior_nul_error)?;
        let before = stat_at(self.root.as_raw_fd(), &target)?;
        if file_type(before.st_mode) != libc::S_IFREG
            || before.st_uid != unsafe { libc::geteuid() }
            || before.st_nlink != 1
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "host file is not a single owner regular file",
            ));
        }
        let file = File::from(open_regular_at(self.root.as_raw_fd(), &target)?);
        let opened = stat_fd(file.as_raw_fd())?;
        if !same_file(&before, &opened) {
            return Err(os_error(libc::ESTALE));
        }
        cvt(unsafe { libc::fchmod(file.as_raw_fd(), mode as libc::mode_t) })?;
        file.sync_all()?;
        let after = stat_fd(file.as_raw_fd())?;
        let rebound = stat_at(self.root.as_raw_fd(), &target)?;
        if !same_file(&before, &after)
            || !same_file(&before, &rebound)
            || after.st_uid != unsafe { libc::geteuid() }
            || rebound.st_uid != unsafe { libc::geteuid() }
            || after.st_nlink != 1
            || rebound.st_nlink != 1
            || after.st_mode as u32 & 0o7777 != mode
            || rebound.st_mode as u32 & 0o7777 != mode
        {
            return Err(os_error(libc::ESTALE));
        }
        cvt(unsafe { libc::fsync(self.root.as_raw_fd()) })
    }

    /// Rewrites an existing owner-only regular file without changing its
    /// inode. This is used by the host-layout migration so the installation
    /// identity can continue to bind the layout record to the same entry.
    /// The caller supplies the exact bytes observed before the rewrite.
    pub(crate) fn rewrite_private_regular_exact(
        &self,
        name: &str,
        expected: &[u8],
        replacement: &[u8],
    ) -> io::Result<()> {
        self.verify_root_name()?;
        let target = CString::new(name).map_err(interior_nul_error)?;
        let before = stat_at(self.root.as_raw_fd(), &target)?;
        require_private_regular(&before)?;
        let descriptor = open_regular_rw_at(self.root.as_raw_fd(), &target)?;
        let mut file = File::from(descriptor);
        let opened = stat_fd(file.as_raw_fd())?;
        require_private_regular(&opened)?;
        if !same_file(&before, &opened) {
            return Err(os_error(libc::ESTALE));
        }
        let mut old_bytes = Vec::new();
        file.read_to_end(&mut old_bytes)?;
        let rebound = stat_at(self.root.as_raw_fd(), &target)?;
        if old_bytes != expected
            || !snapshot_metadata_stable(&before, &opened)
            || !snapshot_metadata_stable(&before, &rebound)
        {
            return Err(os_error(libc::ESTALE));
        }
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(replacement)?;
        file.sync_all()?;
        let after = stat_fd(file.as_raw_fd())?;
        let final_path = stat_at(self.root.as_raw_fd(), &target)?;
        require_private_regular(&after)?;
        require_private_regular(&final_path)?;
        if !same_file(&before, &after) || !same_file(&before, &final_path) {
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
        self.remove_owned_child_with_cleanup_hook(name, &|| Ok(()))
    }

    pub(crate) fn remove_owned_child_with_cleanup_hook(
        &self,
        name: &str,
        after_durable_delete: &dyn Fn() -> io::Result<()>,
    ) -> io::Result<()> {
        let component = private_leaf_name(name)?;
        self.verify_root_name()?;
        let parent_metadata = stat_fd(self.root.as_raw_fd())?;
        require_private_directory(&parent_metadata)?;
        let parent = FileIdentity::from_stat(&parent_metadata);
        let _process_key = lock_cleanup_process_key(parent, component.to_bytes())?;
        self.remove_owned_child_inner_with_hook(name, after_durable_delete)
    }

    #[cfg(test)]
    fn remove_owned_child_inner(&self, name: &str) -> io::Result<()> {
        self.remove_owned_child_inner_with_hook(name, &|| Ok(()))
    }

    fn remove_owned_child_inner_with_hook(
        &self,
        name: &str,
        after_durable_delete: &dyn Fn() -> io::Result<()>,
    ) -> io::Result<()> {
        let component = private_leaf_name(name)?;
        self.verify_root_name()?;
        let parent_metadata = stat_fd(self.root.as_raw_fd())?;
        require_private_directory(&parent_metadata)?;
        let parent = FileIdentity::from_stat(&parent_metadata);
        let namespace = PrivateNamespace::select_for_cleanup(
            self.root.as_raw_fd(),
            parent_metadata.st_dev,
            &[],
        )?;
        let verify_parent = || {
            self.verify_root_name()?;
            let current = stat_fd(self.root.as_raw_fd())?;
            if FileIdentity::from_stat(&current) != parent {
                return Err(os_error(libc::ESTALE));
            }
            Ok(())
        };
        let mut bound = None;
        let mut last_race = os_error(libc::ESTALE);
        for _ in 0..32 {
            match validate_cleanup_namespace_evidence(&namespace, self.root.as_raw_fd(), parent) {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                    ) =>
                {
                    last_race = error;
                    continue;
                }
                Err(error) => return Err(error),
            }
            match cleanup_exact_initial_snapshot(
                &namespace,
                self.root.as_raw_fd(),
                parent,
                &component,
                None,
                None,
                CleanupTargetKind::Tree,
            ) {
                Ok(CleanupInitialSnapshot::Absent) => return Ok(()),
                Ok(CleanupInitialSnapshot::Bound {
                    metadata,
                    generation,
                }) => {
                    injected_cleanup_bootstrap_result()?;
                    bound = Some((metadata, generation));
                    break;
                }
                Ok(CleanupInitialSnapshot::Evidence) => {
                    if let Some(loaded) =
                        find_cleanup_intent(&namespace, parent, component.to_bytes())?
                    {
                        let target = FileIdentity::from(loaded.intent.target);
                        let original_mode = loaded.intent.original_mode;
                        let generation = cleanup_generation_from_loaded(
                            &namespace,
                            parent,
                            component.to_bytes(),
                            &loaded,
                        )?;
                        drop(loaded);
                        return complete_bound_cleanup(
                            &namespace,
                            self.root.as_raw_fd(),
                            parent,
                            component.to_bytes(),
                            target,
                            original_mode,
                            Some(generation),
                            &verify_parent,
                            true,
                            CleanupTargetKind::Tree,
                            after_durable_delete,
                        )
                        .map(|_| ());
                    }
                    if let Some(candidate) = find_unpublished_cleanup_bootstrap(
                        &namespace,
                        self.root.as_raw_fd(),
                        parent,
                        component.to_bytes(),
                    )? {
                        return complete_bound_cleanup(
                            &namespace,
                            self.root.as_raw_fd(),
                            parent,
                            component.to_bytes(),
                            candidate.target,
                            candidate.original_mode,
                            Some(candidate.generation),
                            &verify_parent,
                            true,
                            CleanupTargetKind::Tree,
                            after_durable_delete,
                        )
                        .map(|_| ());
                    }
                }
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                    ) =>
                {
                    last_race = error;
                }
                Err(error) => return Err(error),
            }
        }
        let Some((before, initial_generation)) = bound else {
            return Err(last_race);
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
                return complete_bound_cleanup(
                    &namespace,
                    self.root.as_raw_fd(),
                    parent,
                    component.to_bytes(),
                    target_identity,
                    original_mode,
                    Some(initial_generation.clone()),
                    &verify_parent,
                    true,
                    CleanupTargetKind::Tree,
                    after_durable_delete,
                )
                .map(|_| ());
            }
            Err(error) => return Err(error),
        };
        let opened = stat_fd(target.root.as_raw_fd())?;
        if !same_file(&before, &opened)
            || opened.st_uid != effective_user_id()
            || opened.st_dev != parent_metadata.st_dev
            || opened.st_mode as u32 & 0o7777 != original_mode
        {
            return complete_bound_cleanup(
                &namespace,
                self.root.as_raw_fd(),
                parent,
                component.to_bytes(),
                target_identity,
                original_mode,
                Some(initial_generation.clone()),
                &verify_parent,
                true,
                CleanupTargetKind::Tree,
                after_durable_delete,
            )
            .map(|_| ());
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
        complete_bound_cleanup(
            &namespace,
            self.root.as_raw_fd(),
            parent,
            component.to_bytes(),
            target_identity,
            original_mode,
            Some(initial_generation),
            &verify_parent,
            true,
            CleanupTargetKind::Tree,
            after_durable_delete,
        )
        .map(|_| ())
    }

    pub(crate) fn resume_pending_owned_child_cleanup(&self, name: &str) -> io::Result<bool> {
        self.resume_pending_owned_cleanup(name, CleanupTargetKind::Tree)
    }

    pub(crate) fn resume_pending_owned_regular_cleanup(&self, name: &str) -> io::Result<bool> {
        self.resume_pending_owned_cleanup(name, CleanupTargetKind::Regular)
    }

    fn resume_pending_owned_cleanup(
        &self,
        name: &str,
        kind: CleanupTargetKind,
    ) -> io::Result<bool> {
        let component = private_leaf_name(name)?;
        self.verify_root_name()?;
        let parent_metadata = stat_fd(self.root.as_raw_fd())?;
        require_private_directory(&parent_metadata)?;
        let parent = FileIdentity::from_stat(&parent_metadata);
        let _process_key = lock_cleanup_process_key(parent, component.to_bytes())?;
        let namespace = match kind {
            CleanupTargetKind::Tree => PrivateNamespace::select_for_cleanup(
                self.root.as_raw_fd(),
                parent_metadata.st_dev,
                &[],
            )?,
            CleanupTargetKind::Regular => PrivateNamespace::select_for_cleanup_at(
                self.root.as_raw_fd(),
                parent_metadata.st_dev,
                &[],
            )?,
        };
        let verify_parent = || {
            self.verify_root_name()?;
            let current = stat_fd(self.root.as_raw_fd())?;
            if FileIdentity::from_stat(&current) != parent {
                return Err(os_error(libc::ESTALE));
            }
            Ok(())
        };
        let mut last_race = os_error(libc::ESTALE);
        for _ in 0..32 {
            match validate_cleanup_namespace_evidence(&namespace, self.root.as_raw_fd(), parent) {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                    ) =>
                {
                    last_race = error;
                    continue;
                }
                Err(error) => return Err(error),
            }
            match cleanup_pending_snapshot(&namespace, parent, &component) {
                Ok(false) => return Ok(false),
                Ok(true) => {}
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                    ) =>
                {
                    last_race = error;
                    continue;
                }
                Err(error) => return Err(error),
            }
            if let Some(loaded) = find_cleanup_intent(&namespace, parent, component.to_bytes())? {
                if loaded.intent.kind != kind {
                    return Err(os_error(libc::ESTALE));
                }
                let target = FileIdentity::from(loaded.intent.target);
                let original_mode = loaded.intent.original_mode;
                let generation = cleanup_generation_from_loaded(
                    &namespace,
                    parent,
                    component.to_bytes(),
                    &loaded,
                )?;
                drop(loaded);
                let outcome = resolve_bound_cleanup(
                    &namespace,
                    self.root.as_raw_fd(),
                    parent,
                    component.to_bytes(),
                    target,
                    original_mode,
                    Some(&generation),
                    &verify_parent,
                    false,
                    kind,
                    &|| Ok(()),
                )?;
                return match outcome {
                    TreeCleanupOutcome::Deleted | TreeCleanupOutcome::Restored(None) => Ok(true),
                    TreeCleanupOutcome::AlreadyRetired => Ok(false),
                    TreeCleanupOutcome::Restored(Some(error)) => Err(error),
                };
            }
            if let Some(candidate) = find_unpublished_cleanup_bootstrap(
                &namespace,
                self.root.as_raw_fd(),
                parent,
                component.to_bytes(),
            )? {
                if candidate.kind != kind {
                    return Err(os_error(libc::ESTALE));
                }
                let outcome = resolve_bound_cleanup(
                    &namespace,
                    self.root.as_raw_fd(),
                    parent,
                    component.to_bytes(),
                    candidate.target,
                    candidate.original_mode,
                    Some(&candidate.generation),
                    &verify_parent,
                    false,
                    kind,
                    &|| Ok(()),
                )?;
                return match outcome {
                    TreeCleanupOutcome::Deleted | TreeCleanupOutcome::Restored(None) => Ok(true),
                    TreeCleanupOutcome::AlreadyRetired => Ok(false),
                    TreeCleanupOutcome::Restored(Some(error)) => Err(error),
                };
            }
        }
        Err(last_race)
    }

    pub(crate) fn retry_pending_owned_children_matching(
        &self,
        predicate: impl FnMut(&[u8], PrivateEntryIdentity) -> bool,
    ) -> io::Result<usize> {
        self.retry_pending_owned_matching(CleanupTargetKind::Tree, predicate)
    }

    pub(crate) fn retry_pending_owned_regulars_matching(
        &self,
        predicate: impl FnMut(&[u8], PrivateEntryIdentity) -> bool,
    ) -> io::Result<usize> {
        self.retry_pending_owned_matching(CleanupTargetKind::Regular, predicate)
    }

    fn retry_pending_owned_matching(
        &self,
        kind: CleanupTargetKind,
        mut predicate: impl FnMut(&[u8], PrivateEntryIdentity) -> bool,
    ) -> io::Result<usize> {
        self.verify_root_name()?;
        let parent_metadata = stat_fd(self.root.as_raw_fd())?;
        require_private_directory(&parent_metadata)?;
        let parent = FileIdentity::from_stat(&parent_metadata);
        let namespace = match kind {
            CleanupTargetKind::Tree => PrivateNamespace::select_for_cleanup(
                self.root.as_raw_fd(),
                parent_metadata.st_dev,
                &[],
            )?,
            CleanupTargetKind::Regular => PrivateNamespace::select_for_cleanup_at(
                self.root.as_raw_fd(),
                parent_metadata.st_dev,
                &[],
            )?,
        };
        let verify_parent = || {
            self.verify_root_name()?;
            let current = stat_fd(self.root.as_raw_fd())?;
            if FileIdentity::from_stat(&current) != parent {
                return Err(os_error(libc::ESTALE));
            }
            Ok(())
        };
        let mut resumed = 0;
        let mut decisions = Vec::<(Vec<u8>, CleanupGenerationFence, bool)>::new();
        let mut last_race = os_error(libc::ESTALE);
        for _ in 0..64 {
            match validate_cleanup_namespace_evidence(&namespace, self.root.as_raw_fd(), parent) {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                    ) =>
                {
                    last_race = error;
                    continue;
                }
                Err(error) => return Err(error),
            }
            let candidates = match collect_pending_cleanup_candidates(
                &namespace,
                self.root.as_raw_fd(),
                parent,
                kind,
            ) {
                Ok(candidates) => candidates,
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                    ) =>
                {
                    last_race = error;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if candidates.is_empty() {
                return Ok(resumed);
            }
            let mut invoked_predicate = false;
            let mut retry = false;
            for candidate in candidates {
                let approval = decisions
                    .iter()
                    .find(|(component, generation, _)| {
                        component.as_slice() == candidate.component.as_slice()
                            && cleanup_generation_matches(generation, &candidate.generation)
                    })
                    .map(|(_, _, approval)| *approval);
                let approved = match approval {
                    Some(approved) => approved,
                    None => {
                        invoked_predicate = true;
                        let target = PrivateEntryIdentity {
                            device: candidate.target.device,
                            inode: candidate.target.inode,
                            kind: cleanup_target_file_type(candidate.kind) as u32,
                            owner: effective_user_id(),
                            mode: candidate.original_mode & 0o7777,
                        };
                        let approved = predicate(&candidate.component, target);
                        decisions.push((
                            candidate.component.clone(),
                            candidate.generation.clone(),
                            approved,
                        ));
                        approved
                    }
                };
                if !approved {
                    continue;
                }
                let _process_key = lock_cleanup_process_key(parent, &candidate.component)?;
                let current = match collect_pending_cleanup_candidates(
                    &namespace,
                    self.root.as_raw_fd(),
                    parent,
                    kind,
                ) {
                    Ok(current) => current,
                    Err(error)
                        if matches!(
                            error.raw_os_error(),
                            Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                        ) =>
                    {
                        last_race = error;
                        retry = true;
                        break;
                    }
                    Err(error) => return Err(error),
                };
                if !current.iter().any(|current| {
                    current.component == candidate.component
                        && cleanup_generation_matches(&candidate.generation, &current.generation)
                }) {
                    last_race = os_error(libc::EAGAIN);
                    retry = true;
                    break;
                }
                injected_cleanup_predicate_completion_handoff();
                let completion = match candidate.kind {
                    CleanupTargetKind::Tree => complete_bound_tree_cleanup_outcome(
                        &namespace,
                        self.root.as_raw_fd(),
                        parent,
                        &candidate.component,
                        candidate.target,
                        candidate.original_mode,
                        Some(candidate.generation.clone()),
                        &verify_parent,
                        false,
                    ),
                    CleanupTargetKind::Regular => complete_bound_cleanup(
                        &namespace,
                        self.root.as_raw_fd(),
                        parent,
                        &candidate.component,
                        candidate.target,
                        candidate.original_mode,
                        Some(candidate.generation.clone()),
                        &verify_parent,
                        false,
                        candidate.kind,
                        &|| Ok(()),
                    ),
                };
                match completion {
                    Ok(TreeCleanupOutcome::Deleted) => {
                        resumed += 1;
                        retry = true;
                        break;
                    }
                    Ok(TreeCleanupOutcome::AlreadyRetired) => {
                        last_race = os_error(libc::EAGAIN);
                        retry = true;
                        break;
                    }
                    Ok(TreeCleanupOutcome::Restored(_)) => {
                        last_race = os_error(libc::EAGAIN);
                        retry = true;
                        break;
                    }
                    Err(error)
                        if matches!(
                            error.raw_os_error(),
                            Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                        ) =>
                    {
                        if error.raw_os_error() == Some(libc::ESTALE) {
                            match collect_pending_cleanup_candidates(
                                &namespace,
                                self.root.as_raw_fd(),
                                parent,
                                kind,
                            ) {
                                Ok(current)
                                    if current.iter().any(|current| {
                                        current.component == candidate.component
                                            && cleanup_generation_matches(
                                                &candidate.generation,
                                                &current.generation,
                                            )
                                    }) =>
                                {
                                    return Err(error);
                                }
                                Ok(_) => {}
                                Err(snapshot_error)
                                    if matches!(
                                        snapshot_error.raw_os_error(),
                                        Some(libc::EAGAIN)
                                            | Some(libc::ENOENT)
                                            | Some(libc::ESTALE)
                                    ) =>
                                {
                                    last_race = snapshot_error;
                                    retry = true;
                                    break;
                                }
                                Err(snapshot_error) => return Err(snapshot_error),
                            }
                        }
                        last_race = error;
                        retry = true;
                        break;
                    }
                    Err(error) => return Err(error),
                }
            }
            if retry || invoked_predicate {
                continue;
            }
            return Ok(resumed);
        }
        Err(last_race)
    }

    pub(crate) fn remove_owned_regular(&self, name: &str) -> io::Result<()> {
        self.remove_owned_regular_with_cleanup_hook(name, &|| Ok(()))
    }

    pub(crate) fn remove_owned_regular_with_cleanup_hook(
        &self,
        name: &str,
        after_durable_delete: &dyn Fn() -> io::Result<()>,
    ) -> io::Result<()> {
        let component = private_leaf_name(name)?;
        self.verify_root_name()?;
        let parent_metadata = stat_fd(self.root.as_raw_fd())?;
        require_private_directory(&parent_metadata)?;
        let parent = FileIdentity::from_stat(&parent_metadata);
        let _process_key = lock_cleanup_process_key(parent, component.to_bytes())?;
        injected_regular_parent_admission_hook();
        let namespace = PrivateNamespace::select_for_cleanup_at(
            self.root.as_raw_fd(),
            parent_metadata.st_dev,
            &[],
        )?;
        let verify_parent = || {
            self.verify_root_name()?;
            let current = stat_fd(self.root.as_raw_fd())?;
            if FileIdentity::from_stat(&current) != parent {
                return Err(os_error(libc::ESTALE));
            }
            Ok(())
        };
        let mut bound = None;
        let mut last_race = os_error(libc::ESTALE);
        for _ in 0..32 {
            match validate_cleanup_namespace_evidence(&namespace, self.root.as_raw_fd(), parent) {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                    ) =>
                {
                    last_race = error;
                    continue;
                }
                Err(error) => return Err(error),
            }
            match cleanup_exact_initial_snapshot(
                &namespace,
                self.root.as_raw_fd(),
                parent,
                &component,
                None,
                None,
                CleanupTargetKind::Regular,
            ) {
                Ok(CleanupInitialSnapshot::Absent) => return Ok(()),
                Ok(CleanupInitialSnapshot::Bound {
                    metadata,
                    generation,
                }) => {
                    injected_cleanup_bootstrap_result()?;
                    bound = Some((metadata, generation));
                    break;
                }
                Ok(CleanupInitialSnapshot::Evidence) => {
                    if let Some(loaded) =
                        find_cleanup_intent(&namespace, parent, component.to_bytes())?
                    {
                        if loaded.intent.kind != CleanupTargetKind::Regular {
                            return Err(os_error(libc::ESTALE));
                        }
                        let target = FileIdentity::from(loaded.intent.target);
                        let original_mode = loaded.intent.original_mode;
                        let generation = cleanup_generation_from_loaded(
                            &namespace,
                            parent,
                            component.to_bytes(),
                            &loaded,
                        )?;
                        drop(loaded);
                        return complete_bound_cleanup(
                            &namespace,
                            self.root.as_raw_fd(),
                            parent,
                            component.to_bytes(),
                            target,
                            original_mode,
                            Some(generation),
                            &verify_parent,
                            true,
                            CleanupTargetKind::Regular,
                            after_durable_delete,
                        )
                        .map(|_| ());
                    }
                    if let Some(candidate) = find_unpublished_cleanup_bootstrap(
                        &namespace,
                        self.root.as_raw_fd(),
                        parent,
                        component.to_bytes(),
                    )? {
                        if candidate.kind != CleanupTargetKind::Regular {
                            return Err(os_error(libc::ESTALE));
                        }
                        return complete_bound_cleanup(
                            &namespace,
                            self.root.as_raw_fd(),
                            parent,
                            component.to_bytes(),
                            candidate.target,
                            candidate.original_mode,
                            Some(candidate.generation),
                            &verify_parent,
                            true,
                            CleanupTargetKind::Regular,
                            after_durable_delete,
                        )
                        .map(|_| ());
                    }
                }
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                    ) =>
                {
                    last_race = error;
                }
                Err(error) => return Err(error),
            }
        }
        let Some((before, initial_generation)) = bound else {
            return Err(last_race);
        };
        if !cleanup_public_target_matches(
            &before,
            parent_metadata.st_dev as u64,
            CleanupTargetKind::Regular,
        ) {
            return Err(os_error(libc::ESTALE));
        }
        let target_identity = FileIdentity::from_stat(&before);
        let original_mode = before.st_mode as u32 & 0o7777;
        let file = match open_bound_cleanup_regular(
            self.root.as_raw_fd(),
            &component,
            target_identity,
            parent.device,
            original_mode,
        ) {
            Ok(file) => file,
            Err(error)
                if matches!(
                    error.raw_os_error(),
                    Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                ) =>
            {
                return complete_bound_cleanup(
                    &namespace,
                    self.root.as_raw_fd(),
                    parent,
                    component.to_bytes(),
                    target_identity,
                    original_mode,
                    Some(initial_generation.clone()),
                    &verify_parent,
                    true,
                    CleanupTargetKind::Regular,
                    after_durable_delete,
                )
                .map(|_| ());
            }
            Err(error) => return Err(error),
        };
        let opened = stat_fd(file.as_raw_fd())?;
        if !same_file(&before, &opened)
            || opened.st_uid != effective_user_id()
            || opened.st_dev != parent_metadata.st_dev
            || opened.st_nlink != 1
            || opened.st_mode as u32 & 0o7777 != original_mode
        {
            return complete_bound_cleanup(
                &namespace,
                self.root.as_raw_fd(),
                parent,
                component.to_bytes(),
                target_identity,
                original_mode,
                Some(initial_generation.clone()),
                &verify_parent,
                true,
                CleanupTargetKind::Regular,
                after_durable_delete,
            )
            .map(|_| ());
        }
        drop(file);
        match publish_cleanup_intent(
            &namespace,
            self.root.as_raw_fd(),
            parent,
            &component,
            target_identity,
            original_mode,
            CleanupTargetKind::Regular,
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
        complete_bound_cleanup(
            &namespace,
            self.root.as_raw_fd(),
            parent,
            component.to_bytes(),
            target_identity,
            original_mode,
            Some(initial_generation),
            &verify_parent,
            true,
            CleanupTargetKind::Regular,
            after_durable_delete,
        )
        .map(|_| ())
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
        self.remove_owned_tree_with_cleanup_hook(&|| Ok(()))
    }

    pub(crate) fn remove_owned_tree_with_cleanup_hook(
        &self,
        after_durable_delete: &dyn Fn() -> io::Result<()>,
    ) -> io::Result<()> {
        let parent_metadata = stat_fd(self.parent.as_raw_fd())?;
        let parent = FileIdentity::from_stat(&parent_metadata);
        if self.root_identity.device != parent.device {
            return Err(os_error(libc::EXDEV));
        }
        let _process_key = lock_cleanup_process_key(parent, self.root_name.to_bytes())?;
        let namespace = PrivateNamespace::select_for_cleanup(
            self.parent.as_raw_fd(),
            parent_metadata.st_dev,
            &[DirectoryIdentity {
                descriptor: self.root.as_raw_fd(),
                identity: self.root_identity,
            }],
        )?;
        let verify_parent = || {
            let current = stat_fd(self.parent.as_raw_fd())?;
            if FileIdentity::from_stat(&current) != parent {
                return Err(os_error(libc::ESTALE));
            }
            verify_lineage(&self.lineage)
        };
        let mut bound = None;
        let mut last_race = os_error(libc::ESTALE);
        for _ in 0..32 {
            match validate_cleanup_namespace_evidence(&namespace, self.parent.as_raw_fd(), parent) {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                    ) =>
                {
                    last_race = error;
                    continue;
                }
                Err(error) => return Err(error),
            }
            match cleanup_exact_initial_snapshot(
                &namespace,
                self.parent.as_raw_fd(),
                parent,
                &self.root_name,
                Some(self.root_identity),
                None,
                CleanupTargetKind::Tree,
            ) {
                Ok(CleanupInitialSnapshot::Absent) => return Ok(()),
                Ok(CleanupInitialSnapshot::Bound {
                    metadata,
                    generation,
                }) => {
                    injected_cleanup_bootstrap_result()?;
                    bound = Some((metadata, generation));
                    break;
                }
                Ok(CleanupInitialSnapshot::Evidence) => {
                    if let Some(loaded) =
                        find_cleanup_intent(&namespace, parent, self.root_name.to_bytes())?
                    {
                        let target = FileIdentity::from(loaded.intent.target);
                        let original_mode = loaded.intent.original_mode;
                        let generation = cleanup_generation_from_loaded(
                            &namespace,
                            parent,
                            self.root_name.to_bytes(),
                            &loaded,
                        )?;
                        drop(loaded);
                        return complete_bound_cleanup(
                            &namespace,
                            self.parent.as_raw_fd(),
                            parent,
                            self.root_name.to_bytes(),
                            target,
                            original_mode,
                            Some(generation),
                            &verify_parent,
                            true,
                            CleanupTargetKind::Tree,
                            after_durable_delete,
                        )
                        .map(|_| ());
                    }
                    if let Some(candidate) = find_unpublished_cleanup_bootstrap(
                        &namespace,
                        self.parent.as_raw_fd(),
                        parent,
                        self.root_name.to_bytes(),
                    )? {
                        return complete_bound_cleanup(
                            &namespace,
                            self.parent.as_raw_fd(),
                            parent,
                            self.root_name.to_bytes(),
                            candidate.target,
                            candidate.original_mode,
                            Some(candidate.generation),
                            &verify_parent,
                            true,
                            CleanupTargetKind::Tree,
                            after_durable_delete,
                        )
                        .map(|_| ());
                    }
                }
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                    ) =>
                {
                    last_race = error;
                }
                Err(error) => return Err(error),
            }
        }
        let Some((current, initial_generation)) = bound else {
            return Err(last_race);
        };
        let opened = stat_fd(self.root.as_raw_fd())?;
        let original_mode = current.st_mode as u32 & 0o7777;
        if file_type(current.st_mode) != libc::S_IFDIR
            || current.st_uid != effective_user_id()
            || opened.st_uid != effective_user_id()
            || !same_file(&current, &opened)
            || FileIdentity::from_stat(&opened) != self.root_identity
            || opened.st_mode as u32 & 0o7777 != original_mode
        {
            return Err(os_error(libc::ESTALE));
        }
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
        complete_bound_cleanup(
            &namespace,
            self.parent.as_raw_fd(),
            parent,
            self.root_name.to_bytes(),
            self.root_identity,
            original_mode,
            Some(initial_generation),
            &verify_parent,
            true,
            CleanupTargetKind::Tree,
            after_durable_delete,
        )
        .map(|_| ())
    }

    #[cfg(test)]
    fn remove_owned_tree_with_hook(
        &self,
        after_private_validation: impl FnOnce(),
    ) -> io::Result<()> {
        self.remove_owned_tree_with_hooks(|| {}, || {}, after_private_validation, || {}, || {})
    }

    #[cfg(test)]
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
    Regular,
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

#[derive(Debug)]
enum TreeCleanupOutcome {
    Deleted,
    AlreadyRetired,
    Restored(Option<io::Error>),
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
    kind: CleanupTargetKind,
}

#[derive(Clone, PartialEq, Eq)]
struct CleanupBootstrapName {
    key: String,
    target: FileIdentity,
    original_mode: u32,
    kind: CleanupTargetKind,
}

#[derive(Clone, PartialEq, Eq)]
struct CleanupDecisionStageName {
    key: String,
    intent: FileIdentity,
    decision: CleanupDecisionV1,
}

struct PendingTreeCleanupCandidate {
    component: Vec<u8>,
    target: FileIdentity,
    original_mode: u32,
    kind: CleanupTargetKind,
    generation: CleanupGenerationFence,
}

#[derive(Clone, PartialEq, Eq)]
struct CleanupGenerationFence {
    intent: Option<FileIdentity>,
    operation: CString,
    operation_identity: FileIdentity,
}

#[derive(Clone, PartialEq, Eq)]
struct PendingCleanupEvidenceSnapshot {
    key: String,
    intent: Option<(CString, FileIdentity)>,
    bootstrap: Option<(CString, CleanupBootstrapName, FileIdentity)>,
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
        Self::select_with_probe(
            initial_parent,
            expected_device,
            disallowed_roots,
            true,
            true,
        )
    }

    fn select_for_cleanup(
        initial_parent: RawFd,
        expected_device: libc::dev_t,
        disallowed_roots: &[DirectoryIdentity],
    ) -> io::Result<Self> {
        Self::select_with_probe(
            initial_parent,
            expected_device,
            disallowed_roots,
            false,
            true,
        )
    }

    fn select_for_cleanup_at(
        initial_parent: RawFd,
        expected_device: libc::dev_t,
        disallowed_roots: &[DirectoryIdentity],
    ) -> io::Result<Self> {
        Self::select_with_probe(
            initial_parent,
            expected_device,
            disallowed_roots,
            false,
            false,
        )
    }

    fn select_with_probe(
        initial_parent: RawFd,
        expected_device: libc::dev_t,
        disallowed_roots: &[DirectoryIdentity],
        probe: bool,
        ancestor_fallback: bool,
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
                let observed_cleanup = if probe {
                    None
                } else {
                    Some(match stat_at(parent.as_raw_fd(), PRIVATE_NAMESPACE_NAME) {
                        Ok(metadata) => Some(FileIdentity::from_stat(&metadata)),
                        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                        Err(error) => return Err(error),
                    })
                };
                match Self::open_or_create_at(&parent, expected_device) {
                    Ok(namespace) => {
                        if let Some(Some(expected)) = observed_cleanup
                            && (namespace.created || namespace.identity != expected)
                        {
                            namespace.remove_if_new_and_empty(parent.as_raw_fd());
                            return Err(os_error(libc::ESTALE));
                        }
                        if namespace.is_disjoint_from(disallowed_roots)? {
                            if probe {
                                namespace.probe_atomic_rename()?;
                            } else {
                                cvt(unsafe { libc::fsync(parent.as_raw_fd()) })?;
                                let opened = stat_fd(namespace.directory.as_raw_fd())?;
                                let rebound = stat_at(parent.as_raw_fd(), PRIVATE_NAMESPACE_NAME)?;
                                require_private_directory_on_device(
                                    &opened,
                                    expected_device as u64,
                                )?;
                                require_private_directory_on_device(
                                    &rebound,
                                    expected_device as u64,
                                )?;
                                if opened.st_mode & 0o777 != 0o700
                                    || rebound.st_mode & 0o777 != 0o700
                                    || FileIdentity::from_stat(&opened) != namespace.identity
                                    || FileIdentity::from_stat(&rebound) != namespace.identity
                                    || !same_file(&opened, &rebound)
                                {
                                    return Err(os_error(libc::ESTALE));
                                }
                            }
                            return Ok(namespace);
                        }
                        namespace.remove_if_new_and_empty(parent.as_raw_fd());
                    }
                    Err(error)
                        if !probe
                            && (error.raw_os_error() == Some(libc::ESTALE)
                                || matches!(observed_cleanup, Some(Some(_)))) =>
                    {
                        return Err(error);
                    }
                    Err(error) if matches!(observed_cleanup, Some(None)) => {
                        match stat_at(parent.as_raw_fd(), PRIVATE_NAMESPACE_NAME) {
                            Ok(_) => return Err(os_error(libc::ESTALE)),
                            Err(recheck) if recheck.kind() == io::ErrorKind::NotFound => {
                                last_error = error;
                            }
                            Err(recheck) => return Err(recheck),
                        }
                    }
                    Err(error) => last_error = error,
                }
            }
            if !ancestor_fallback {
                break;
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
        let synced = (|| {
            cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
            record_cleanup_capture_destination_sync();
            cvt(unsafe { libc::fsync(source_parent) })?;
            record_cleanup_capture_source_sync();
            Ok(())
        })();
        if let Err(error) = synced {
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
    injected_cleanup_after_placeholder_removal_result()?;
    let namespace = operation.namespace;
    remove_empty_cleanup_operation(namespace, operation)
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

fn cleanup_kind_code(kind: CleanupTargetKind) -> &'static str {
    match kind {
        CleanupTargetKind::Tree => "k01",
        CleanupTargetKind::Regular => "k02",
    }
}

fn parse_cleanup_kind_code(code: &str) -> io::Result<CleanupTargetKind> {
    match code {
        "k01" => Ok(CleanupTargetKind::Tree),
        "k02" => Ok(CleanupTargetKind::Regular),
        _ => Err(cleanup_record_error()),
    }
}

fn cleanup_quarantine_kind(kind: CleanupTargetKind) -> &'static str {
    match kind {
        CleanupTargetKind::Tree => "cleanup-tree-v1",
        CleanupTargetKind::Regular => "cleanup-regular-v1",
    }
}

fn cleanup_target_file_type(kind: CleanupTargetKind) -> libc::mode_t {
    match kind {
        CleanupTargetKind::Tree => libc::S_IFDIR,
        CleanupTargetKind::Regular => libc::S_IFREG,
    }
}

fn cleanup_public_target_matches(
    metadata: &libc::stat,
    expected_device: u64,
    kind: CleanupTargetKind,
) -> bool {
    if metadata.st_uid != effective_user_id() || metadata.st_dev as u64 != expected_device {
        return false;
    }
    match kind {
        CleanupTargetKind::Tree => file_type(metadata.st_mode) == libc::S_IFDIR,
        CleanupTargetKind::Regular => {
            file_type(metadata.st_mode) == libc::S_IFREG
                && metadata.st_nlink == 1
                && metadata.st_mode & 0o077 == 0
        }
    }
}

fn cleanup_namespace_slot_matches(
    metadata: &libc::stat,
    identity: FileIdentity,
    target: FileIdentity,
    placeholder: FileIdentity,
    kind: CleanupTargetKind,
    original_mode: u32,
    device: u64,
) -> bool {
    if metadata.st_uid != effective_user_id() || metadata.st_dev as u64 != device {
        return false;
    }
    if identity == placeholder {
        return file_type(metadata.st_mode) == libc::S_IFDIR && metadata.st_mode & 0o777 == 0o700;
    }
    if identity != target {
        return false;
    }
    match kind {
        CleanupTargetKind::Tree => {
            file_type(metadata.st_mode) == libc::S_IFDIR && metadata.st_mode & 0o777 == 0o700
        }
        CleanupTargetKind::Regular => {
            file_type(metadata.st_mode) == libc::S_IFREG
                && metadata.st_nlink == 1
                && metadata.st_mode as u32 & 0o7777 == original_mode
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
    if !encoded.len().is_multiple_of(2) || encoded.is_empty() {
        return Err(cleanup_record_error());
    }
    let mut decoded = Vec::with_capacity(encoded.len() / 2);
    for &[high_byte, low_byte] in encoded.as_bytes().as_chunks::<2>().0 {
        let nibble = |value: u8| match value {
            b'0'..=b'9' => Some(value - b'0'),
            b'a'..=b'f' => Some(value - b'a' + 10),
            _ => None,
        };
        let high = nibble(high_byte).ok_or_else(cleanup_record_error)?;
        let low = nibble(low_byte).ok_or_else(cleanup_record_error)?;
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

fn cleanup_decision_stage_name(
    key: &str,
    intent: FileIdentity,
    decision: CleanupDecisionV1,
) -> io::Result<CString> {
    cleanup_record_name("", key)?;
    let decision = match decision {
        CleanupDecisionV1::Delete => 1,
        CleanupDecisionV1::Restore => 2,
    };
    CString::new(format!(
        "{CLEANUP_DECISION_STAGE_PREFIX}{key}-d{:016x}-i{:016x}-c{decision:02x}",
        intent.device, intent.inode
    ))
    .map_err(|_| cleanup_record_error())
}

fn parse_cleanup_decision_stage_name(name: &CStr) -> io::Result<Option<CleanupDecisionStageName>> {
    let Some(encoded) = name
        .to_bytes()
        .strip_prefix(CLEANUP_DECISION_STAGE_PREFIX.as_bytes())
    else {
        return Ok(None);
    };
    let encoded = std::str::from_utf8(encoded).map_err(|_| cleanup_record_error())?;
    let mut fields = encoded.split('-');
    let key = fields.next().ok_or_else(cleanup_record_error)?;
    let device = fields.next().ok_or_else(cleanup_record_error)?;
    let inode = fields.next().ok_or_else(cleanup_record_error)?;
    let decision = fields.next().ok_or_else(cleanup_record_error)?;
    if fields.next().is_some()
        || key.len() != 64
        || device.len() != 17
        || inode.len() != 17
        || decision.len() != 3
    {
        return Err(cleanup_record_error());
    }
    cleanup_record_name("", key)?;
    let intent = FileIdentity {
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
    let decision = match decision {
        "c01" => CleanupDecisionV1::Delete,
        "c02" => CleanupDecisionV1::Restore,
        _ => return Err(cleanup_record_error()),
    };
    let parsed = CleanupDecisionStageName {
        key: key.to_owned(),
        intent,
        decision,
    };
    if cleanup_decision_stage_name(&parsed.key, parsed.intent, parsed.decision)?.as_bytes()
        != name.to_bytes()
    {
        return Err(cleanup_record_error());
    }
    Ok(Some(parsed))
}

fn cleanup_bootstrap_name(
    key: &str,
    target: FileIdentity,
    original_mode: u32,
    kind: CleanupTargetKind,
) -> io::Result<CString> {
    cleanup_record_name("", key)?;
    if original_mode > 0o7777 {
        return Err(cleanup_record_error());
    }
    CString::new(format!(
        "{CLEANUP_OPERATION_PREFIX}{key}-d{:016x}-i{:016x}-{}-m{original_mode:04x}",
        target.device,
        target.inode,
        cleanup_kind_code(kind)
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
        || mode.len() != 5
    {
        return Err(cleanup_record_error());
    }
    let kind = parse_cleanup_kind_code(kind)?;
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
        kind,
    };
    if cleanup_bootstrap_name(
        &parsed.key,
        parsed.target,
        parsed.original_mode,
        parsed.kind,
    )?
    .as_bytes()
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

enum CleanupStageJson<T> {
    Complete(T),
    Truncated,
}

fn cleanup_parse_stage_json<T>(bytes: &[u8]) -> io::Result<CleanupStageJson<T>>
where
    T: Serialize + serde::de::DeserializeOwned,
{
    if bytes.len() > CLEANUP_RECORD_MAX_BYTES {
        return Err(os_error(libc::EFBIG));
    }
    match serde_json::from_slice(bytes) {
        Ok(value) => {
            if cleanup_canonical_json(&value)? != bytes {
                return Err(cleanup_record_error());
            }
            Ok(CleanupStageJson::Complete(value))
        }
        Err(error) if error.is_eof() => Ok(CleanupStageJson::Truncated),
        Err(_) => Err(cleanup_record_error()),
    }
}

fn cleanup_uuid_name_prefix(bytes: &[u8], kind: &str) -> bool {
    let mut prefix = kind.as_bytes().to_vec();
    prefix.push(b'-');
    if bytes.len() <= prefix.len() {
        return prefix.starts_with(bytes);
    }
    if !bytes.starts_with(&prefix) {
        return false;
    }
    let uuid = &bytes[prefix.len()..];
    if uuid.len() > 36 {
        return false;
    }
    uuid.iter().enumerate().all(|(index, byte)| match index {
        8 | 13 | 18 | 23 => *byte == b'-',
        14 => *byte == b'4',
        19 => matches!(*byte, b'8' | b'9' | b'a' | b'b'),
        _ => byte.is_ascii_digit() || matches!(*byte, b'a'..=b'f'),
    })
}

#[allow(clippy::too_many_arguments)]
fn cleanup_intent_record(
    key: &str,
    component: &CStr,
    parent: FileIdentity,
    namespace: &PrivateNamespace,
    target: FileIdentity,
    original_mode: u32,
    quarantine: String,
    operation_name: &CStr,
    operation_identity: FileIdentity,
    placeholder_identity: FileIdentity,
    kind: CleanupTargetKind,
) -> io::Result<CleanupIntentV1> {
    Ok(CleanupIntentV1 {
        version: 1,
        kind,
        key_sha256: key.to_owned(),
        component_hex: cleanup_hex_encode(component.to_bytes()),
        parent: parent.into(),
        namespace: namespace.identity.into(),
        target: target.into(),
        original_mode,
        quarantine,
        operation: operation_name
            .to_str()
            .map_err(|_| cleanup_record_error())?
            .to_owned(),
        operation_identity: operation_identity.into(),
        placeholder: CLEANUP_PLACEHOLDER_NAME
            .to_str()
            .map_err(|_| cleanup_record_error())?
            .to_owned(),
        placeholder_identity: placeholder_identity.into(),
    })
}

#[allow(clippy::too_many_arguments)]
fn cleanup_truncated_intent_is_attributable(
    bytes: &[u8],
    key: &str,
    component: &CStr,
    parent: FileIdentity,
    namespace: &PrivateNamespace,
    target: FileIdentity,
    original_mode: u32,
    operation_name: &CStr,
    operation_identity: FileIdentity,
    placeholder_identity: FileIdentity,
    kind: CleanupTargetKind,
) -> io::Result<bool> {
    const QUARANTINE_FIELD: &[u8] = b"\"quarantine\":\"";
    let template = cleanup_canonical_json(&cleanup_intent_record(
        key,
        component,
        parent,
        namespace,
        target,
        original_mode,
        String::new(),
        operation_name,
        operation_identity,
        placeholder_identity,
        kind,
    )?)?;
    let field = template
        .windows(QUARANTINE_FIELD.len())
        .position(|window| window == QUARANTINE_FIELD)
        .ok_or_else(cleanup_record_error)?;
    let value_start = field + QUARANTINE_FIELD.len();
    let prefix = &template[..value_start];
    if bytes.len() <= prefix.len() {
        return Ok(prefix.starts_with(bytes));
    }
    if !bytes.starts_with(prefix) {
        return Ok(false);
    }
    let remainder = &bytes[value_start..];
    let Some(value_end) = remainder.iter().position(|byte| *byte == b'"') else {
        return Ok(cleanup_uuid_name_prefix(
            remainder,
            cleanup_quarantine_kind(kind),
        ));
    };
    let quarantine = &remainder[..value_end];
    let quarantine_name = CString::new(quarantine).map_err(|_| cleanup_record_error())?;
    if !is_random_private_name(&quarantine_name, cleanup_quarantine_kind(kind)) {
        return Ok(false);
    }
    let quarantine = std::str::from_utf8(quarantine_name.to_bytes())
        .map_err(|_| cleanup_record_error())?
        .to_owned();
    let expected = cleanup_canonical_json(&cleanup_intent_record(
        key,
        component,
        parent,
        namespace,
        target,
        original_mode,
        quarantine,
        operation_name,
        operation_identity,
        placeholder_identity,
        kind,
    )?)?;
    Ok(expected.starts_with(bytes))
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

fn revalidate_cleanup_record_binding(
    parent: RawFd,
    name: &CStr,
    file: &File,
    identity: FileIdentity,
    expected_device: u64,
) -> io::Result<libc::stat> {
    let opened = stat_fd(file.as_raw_fd())?;
    let rebound = stat_at(parent, name)?;
    require_cleanup_record_metadata(&opened, expected_device)?;
    require_cleanup_record_metadata(&rebound, expected_device)?;
    if FileIdentity::from_stat(&opened) != identity
        || FileIdentity::from_stat(&rebound) != identity
        || !same_file(&opened, &rebound)
    {
        return Err(os_error(libc::ESTALE));
    }
    Ok(opened)
}

fn revalidate_cleanup_record_bytes(
    parent: RawFd,
    name: &CStr,
    file: &File,
    identity: FileIdentity,
    expected_device: u64,
    expected: &[u8],
) -> io::Result<()> {
    let before = revalidate_cleanup_record_binding(parent, name, file, identity, expected_device)?;
    if before.st_size as usize != expected.len() {
        return Err(cleanup_record_error());
    }
    let mut readback = vec![0; expected.len()];
    let mut offset = 0;
    while offset < readback.len() {
        let read = file.read_at(&mut readback[offset..], offset as u64)?;
        if read == 0 {
            return Err(cleanup_record_error());
        }
        offset += read;
    }
    let after = revalidate_cleanup_record_binding(parent, name, file, identity, expected_device)?;
    if readback != expected || !snapshot_metadata_stable(&before, &after) {
        return Err(os_error(libc::ESTALE));
    }
    Ok(())
}

fn revalidate_cleanup_record_bytes_or_retry(
    parent: RawFd,
    name: &CStr,
    file: &File,
    identity: FileIdentity,
    expected_device: u64,
    expected: &[u8],
) -> io::Result<()> {
    revalidate_cleanup_record_bytes(parent, name, file, identity, expected_device, expected)
        .map_err(|error| match error.raw_os_error() {
            Some(libc::ENOENT) | Some(libc::EAGAIN) | Some(libc::ESTALE) => os_error(libc::EAGAIN),
            _ if error.kind() == io::ErrorKind::NotFound => os_error(libc::EAGAIN),
            _ => error,
        })
}

fn reopen_and_revalidate_cleanup_record_bytes(
    parent: RawFd,
    name: &CStr,
    identity: FileIdentity,
    expected_device: u64,
    expected: &[u8],
) -> io::Result<()> {
    let verifier = File::from(open_regular_rw_at(parent, name)?);
    revalidate_cleanup_record_bytes(parent, name, &verifier, identity, expected_device, expected)
}

fn cleanup_record_error() -> io::Error {
    os_error(libc::EINVAL)
}

fn lock_cleanup_process_key(
    parent: FileIdentity,
    component: &[u8],
) -> io::Result<CleanupProcessKeyGuard> {
    lock_cleanup_process_serialization_key(cleanup_process_key_id(parent, component))
}

fn cleanup_process_key_id(parent: FileIdentity, component: &[u8]) -> String {
    format!("cleanup:{}", cleanup_key(parent, component))
}

#[cfg(test)]
fn try_lock_cleanup_process_key(
    parent: FileIdentity,
    component: &[u8],
) -> io::Result<Option<CleanupProcessKeyGuard>> {
    let key = cleanup_process_key_id(parent, component);
    let mut active = match CLEANUP_PROCESS_KEYS.active.lock() {
        Ok(active) => active,
        Err(poisoned) => poisoned.into_inner(),
    };
    if active.contains(&key) {
        return Ok(None);
    }
    active.insert(key.clone());
    Ok(Some(CleanupProcessKeyGuard { key }))
}

fn lock_cleanup_process_serialization_key(key: String) -> io::Result<CleanupProcessKeyGuard> {
    let mut active = match CLEANUP_PROCESS_KEYS.active.lock() {
        Ok(active) => active,
        Err(poisoned) => poisoned.into_inner(),
    };
    while active.contains(&key) {
        active = match CLEANUP_PROCESS_KEYS.released.wait(active) {
            Ok(active) => active,
            Err(poisoned) => poisoned.into_inner(),
        };
    }
    active.insert(key.clone());
    Ok(CleanupProcessKeyGuard { key })
}

fn with_cleanup_namespace_lock<T>(
    namespace: &PrivateNamespace,
    action: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    // This lock protects only short, descriptor-relative namespace snapshots
    // and same-key bootstrap publication. Callers must not wait for an intent
    // or operation lock, recurse, or invoke user code from `action`.
    // `flock` ownership differs across Unix implementations. Pair it with a
    // same-process mutex so independent OFDs cannot split one logical
    // snapshot or accidentally unlock each other in this process.
    let _process_namespace = lock_cleanup_process_serialization_key(format!(
        "namespace:{:016x}:{:016x}",
        namespace.identity.device, namespace.identity.inode
    ))?;
    cvt(unsafe { libc::flock(namespace.directory.as_raw_fd(), libc::LOCK_EX) })?;
    let result = action();
    let unlock = cvt(unsafe { libc::flock(namespace.directory.as_raw_fd(), libc::LOCK_UN) });
    match (result, unlock) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (_, Err(error)) => Err(error),
    }
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
        || !matches!(
            intent.kind,
            CleanupTargetKind::Tree | CleanupTargetKind::Regular
        )
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
    if !is_random_private_name(&quarantine, cleanup_quarantine_kind(intent.kind))
        || placeholder.as_bytes() != CLEANUP_PLACEHOLDER_NAME.to_bytes()
        || bootstrap.key != intent.key_sha256
        || bootstrap.target != target
        || bootstrap.original_mode != intent.original_mode
        || bootstrap.kind != intent.kind
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
        kind: intent.kind,
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

fn cleanup_generation_from_loaded(
    namespace: &PrivateNamespace,
    parent: FileIdentity,
    component: &[u8],
    loaded: &LoadedCleanupIntent,
) -> io::Result<CleanupGenerationFence> {
    let bindings = validate_cleanup_intent(&loaded.intent, parent, component, namespace)?;
    Ok(CleanupGenerationFence {
        intent: Some(loaded.identity),
        operation: bindings.operation,
        operation_identity: bindings.operation_identity,
    })
}

fn cleanup_generation_matches(
    expected: &CleanupGenerationFence,
    current: &CleanupGenerationFence,
) -> bool {
    expected.operation == current.operation
        && expected.operation_identity == current.operation_identity
        && expected
            .intent
            .is_none_or(|identity| current.intent == Some(identity))
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

fn pending_cleanup_evidence_snapshot(
    namespace: &PrivateNamespace,
) -> io::Result<Vec<PendingCleanupEvidenceSnapshot>> {
    with_cleanup_namespace_lock(namespace, || {
        let mut evidence = BTreeMap::<String, PendingCleanupEvidenceSnapshot>::new();
        for name in directory_entries(namespace.directory.as_raw_fd())? {
            if let Some(suffix) = name
                .to_bytes()
                .strip_prefix(CLEANUP_INTENT_PREFIX.as_bytes())
            {
                let key = std::str::from_utf8(suffix).map_err(|_| cleanup_record_error())?;
                if cleanup_intent_name(key)?.as_bytes() != name.as_bytes() {
                    return Err(cleanup_record_error());
                }
                let metadata = stat_at(namespace.directory.as_raw_fd(), &name)?;
                require_cleanup_record_metadata(&metadata, namespace.identity.device)?;
                let slot = evidence.entry(key.to_owned()).or_insert_with(|| {
                    PendingCleanupEvidenceSnapshot {
                        key: key.to_owned(),
                        intent: None,
                        bootstrap: None,
                    }
                });
                if slot
                    .intent
                    .replace((name, FileIdentity::from_stat(&metadata)))
                    .is_some()
                {
                    return Err(os_error(libc::ESTALE));
                }
                continue;
            }
            let Some(parsed) = parse_cleanup_bootstrap_name(&name)? else {
                continue;
            };
            let metadata = stat_at(namespace.directory.as_raw_fd(), &name)?;
            require_private_directory_on_device(&metadata, namespace.identity.device)?;
            if metadata.st_mode & 0o777 != 0o700 {
                return Err(os_error(libc::ESTALE));
            }
            let slot = evidence.entry(parsed.key.clone()).or_insert_with(|| {
                PendingCleanupEvidenceSnapshot {
                    key: parsed.key.clone(),
                    intent: None,
                    bootstrap: None,
                }
            });
            if slot
                .bootstrap
                .replace((name, parsed, FileIdentity::from_stat(&metadata)))
                .is_some()
            {
                return Err(os_error(libc::ESTALE));
            }
        }
        Ok(evidence.into_values().collect())
    })
}

#[cfg(test)]
fn collect_pending_tree_cleanup_candidates(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
) -> io::Result<Vec<PendingTreeCleanupCandidate>> {
    collect_pending_cleanup_candidates(namespace, public_parent, parent, CleanupTargetKind::Tree)
}

fn collect_pending_cleanup_candidates(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
    kind: CleanupTargetKind,
) -> io::Result<Vec<PendingTreeCleanupCandidate>> {
    let mut last_race = os_error(libc::ESTALE);
    for _ in 0..32 {
        let snapshot = pending_cleanup_evidence_snapshot(namespace)?;
        let mut candidates = BTreeMap::new();
        let mut drifted = false;
        for evidence in &snapshot {
            if let Some((intent_name, intent_identity)) = evidence.intent.as_ref() {
                let (intent_file, bytes, opened_identity) = match open_cleanup_record(
                    namespace.directory.as_raw_fd(),
                    intent_name,
                    parent.device,
                    false,
                ) {
                    Ok(opened) => opened,
                    Err(error)
                        if matches!(
                            error.raw_os_error(),
                            Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                        ) =>
                    {
                        last_race = error;
                        drifted = true;
                        break;
                    }
                    Err(error) => return Err(error),
                };
                if opened_identity != *intent_identity {
                    last_race = os_error(libc::ESTALE);
                    drifted = true;
                    break;
                }
                let intent: CleanupIntentV1 = cleanup_parse_canonical_json(&bytes)?;
                if FileIdentity::from(intent.parent) != parent {
                    continue;
                }
                let component = cleanup_hex_decode(&intent.component_hex)?;
                let bindings = validate_cleanup_intent(&intent, parent, &component, namespace)?;
                if intent.key_sha256 != evidence.key {
                    return Err(os_error(libc::ESTALE));
                }
                if let Some((operation_name, parsed, operation_identity)) =
                    evidence.bootstrap.as_ref()
                    && (bindings.operation.as_bytes() != operation_name.as_bytes()
                        || bindings.operation_identity != *operation_identity
                        || bindings.target != parsed.target
                        || bindings.original_mode != parsed.original_mode
                        || bindings.kind != parsed.kind)
                {
                    return Err(os_error(libc::ESTALE));
                }
                if let Err(error) = revalidate_cleanup_record_bytes_or_retry(
                    namespace.directory.as_raw_fd(),
                    intent_name,
                    &intent_file,
                    opened_identity,
                    parent.device,
                    &bytes,
                ) {
                    if error.raw_os_error() == Some(libc::EAGAIN) {
                        last_race = error;
                        drifted = true;
                        break;
                    }
                    return Err(error);
                }
                if intent.kind != kind {
                    drop(intent_file);
                    continue;
                }
                let candidate = PendingTreeCleanupCandidate {
                    component: component.clone(),
                    target: bindings.target,
                    original_mode: bindings.original_mode,
                    kind: bindings.kind,
                    generation: CleanupGenerationFence {
                        intent: Some(opened_identity),
                        operation: bindings.operation,
                        operation_identity: bindings.operation_identity,
                    },
                };
                if candidates.insert(component, candidate).is_some() {
                    return Err(os_error(libc::ESTALE));
                }
                drop(intent_file);
                continue;
            }
            let Some((operation_name, parsed, operation_identity)) = evidence.bootstrap.as_ref()
            else {
                continue;
            };
            let (operation, opened_identity) =
                match open_cleanup_bootstrap_directory(namespace, operation_name, parsed) {
                    Ok(opened) => opened,
                    Err(error)
                        if matches!(
                            error.raw_os_error(),
                            Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                        ) =>
                    {
                        last_race = error;
                        drifted = true;
                        break;
                    }
                    Err(error) => return Err(error),
                };
            if opened_identity != *operation_identity {
                last_race = os_error(libc::ESTALE);
                drifted = true;
                break;
            }
            // A live bootstrap for another key is valid transient grammar and
            // must not serialize unrelated cleanups. Same-key callers will
            // wait when they open/adopt that exact operation later.
            match cvt(unsafe { libc::flock(operation.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) })
            {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(error) => return Err(error),
            }
            let rebound = match stat_at(namespace.directory.as_raw_fd(), operation_name) {
                Ok(rebound) => rebound,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    last_race = error;
                    drifted = true;
                    break;
                }
                Err(error) => return Err(error),
            };
            if FileIdentity::from_stat(&rebound) != *operation_identity {
                last_race = os_error(libc::ESTALE);
                drifted = true;
                break;
            }
            let candidate = validate_unpublished_cleanup_bootstrap_opened(
                namespace,
                public_parent,
                parent,
                operation_name,
                parsed,
                &operation,
                *operation_identity,
            )?;
            if candidate.kind != kind {
                continue;
            }
            if candidates
                .insert(candidate.component.clone(), candidate)
                .is_some()
            {
                return Err(os_error(libc::ESTALE));
            }
        }
        if drifted {
            continue;
        }
        if pending_cleanup_evidence_snapshot(namespace)? != snapshot {
            last_race = os_error(libc::EAGAIN);
            continue;
        }
        return Ok(candidates.into_values().collect());
    }
    Err(last_race)
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
    let mut decision_keys = BTreeSet::new();
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
        let (file, bytes, identity) =
            open_cleanup_record(namespace.directory.as_raw_fd(), name, device, false)?;
        let intent: CleanupIntentV1 = cleanup_parse_canonical_json(&bytes)?;
        let component = cleanup_hex_decode(&intent.component_hex)?;
        let parent = FileIdentity::from(intent.parent);
        if parent != expected_parent {
            return Err(os_error(libc::ESTALE));
        }
        let bindings = validate_cleanup_intent(&intent, parent, &component, namespace)?;
        if intent.key_sha256 != key {
            return Err(os_error(libc::ESTALE));
        }
        revalidate_cleanup_record_bytes_or_retry(
            namespace.directory.as_raw_fd(),
            name,
            &file,
            identity,
            device,
            &bytes,
        )?;
        if intents
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
                || evidence.bindings.kind != parsed.kind
            {
                return Err(os_error(libc::ESTALE));
            }
        } else {
            let intent_name = cleanup_intent_name(key)?;
            let (operation, operation_identity) =
                open_cleanup_bootstrap_directory(namespace, operation_name, parsed)?;
            // A live bootstrap for another key is valid transient grammar and
            // must not serialize unrelated cleanups. Same-key callers will
            // wait when they open/adopt that exact operation later.
            match cvt(unsafe { libc::flock(operation.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) })
            {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(error) => return Err(error),
            }
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
                    evidence.bindings.kind,
                    evidence.bindings.original_mode,
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
                        evidence.bindings.kind,
                        evidence.bindings.original_mode,
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
        if let Some((target, placeholder, kind, original_mode)) = quarantines.get(name.to_bytes()) {
            let metadata = stat_at(namespace.directory.as_raw_fd(), name)?;
            let identity = FileIdentity::from_stat(&metadata);
            if !cleanup_namespace_slot_matches(
                &metadata,
                identity,
                *target,
                *placeholder,
                *kind,
                *original_mode,
                device,
            ) {
                return Err(os_error(libc::ESTALE));
            }
            continue;
        }
        if let Some((expected, _, _, _, _, _, _)) = operations.get(name.to_bytes()) {
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
                || !decision_keys.insert(key.to_owned())
            {
                return Err(os_error(libc::ESTALE));
            }
            continue;
        }
        return Err(os_error(libc::ESTALE));
    }

    for (
        operation_name,
        (expected, placeholder_name, target, placeholder, key, kind, original_mode),
    ) in operations
    {
        let operation_name = CString::new(operation_name).map_err(|_| cleanup_record_error())?;
        let Some(operation) = PrivateOperation::open_bound(namespace, &operation_name, expected)?
        else {
            continue;
        };
        let placeholder_name =
            CString::new(placeholder_name).map_err(|_| cleanup_record_error())?;
        let mut decision_stage_seen = false;
        for name in directory_entries(operation.directory.as_raw_fd())? {
            if name.as_bytes() == placeholder_name.as_bytes() {
                let metadata = stat_at(operation.directory.as_raw_fd(), &name)?;
                let identity = FileIdentity::from_stat(&metadata);
                if !cleanup_namespace_slot_matches(
                    &metadata,
                    identity,
                    target,
                    placeholder,
                    kind,
                    original_mode,
                    device,
                ) {
                    return Err(os_error(libc::ESTALE));
                }
                continue;
            }
            if name
                .to_bytes()
                .starts_with(CLEANUP_DECISION_STAGE_PREFIX.as_bytes())
            {
                let evidence = intents.get(&key).ok_or_else(|| os_error(libc::ESTALE))?;
                let parsed =
                    parse_cleanup_decision_stage_name(&name)?.ok_or_else(cleanup_record_error)?;
                if decision_stage_seen
                    || decision_keys.contains(&key)
                    || parsed.key != key
                    || parsed.intent != evidence.identity
                {
                    return Err(os_error(libc::ESTALE));
                }
                decision_stage_seen = true;
                let (_file, bytes, _identity) =
                    open_cleanup_record(operation.directory.as_raw_fd(), &name, device, false)?;
                let expected = CleanupDecisionRecordV1 {
                    version: 1,
                    key_sha256: key.clone(),
                    intent: evidence.identity.into(),
                    decision: parsed.decision,
                };
                match cleanup_parse_stage_json::<CleanupDecisionRecordV1>(&bytes)? {
                    CleanupStageJson::Complete(record) => {
                        if record != expected {
                            return Err(os_error(libc::ESTALE));
                        }
                    }
                    CleanupStageJson::Truncated => {
                        if !cleanup_canonical_json(&expected)?.starts_with(&bytes) {
                            return Err(cleanup_record_error());
                        }
                    }
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
    let canonical_exists =
        cleanup_optional_stat(namespace.directory.as_raw_fd(), &decision_name)?.is_some();
    let stage = match operation {
        Some(operation) => find_cleanup_decision_stage(operation, loaded)?,
        None => None,
    };
    if canonical_exists && stage.is_some() {
        return Err(os_error(libc::ESTALE));
    }
    if !canonical_exists && let Some((stage_name, parsed)) = stage {
        let operation = operation.ok_or_else(|| os_error(libc::ESTALE))?;
        let staged =
            load_cleanup_decision_stage_at(namespace, loaded, operation, &stage_name, &parsed)?;
        rename_no_replace(
            operation.directory.as_raw_fd(),
            &stage_name,
            namespace.directory.as_raw_fd(),
            &decision_name,
        )?;
        cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })?;
        cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
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

fn cleanup_decision_record(
    loaded: &LoadedCleanupIntent,
    decision: CleanupDecisionV1,
) -> CleanupDecisionRecordV1 {
    CleanupDecisionRecordV1 {
        version: 1,
        key_sha256: loaded.intent.key_sha256.clone(),
        intent: loaded.identity.into(),
        decision,
    }
}

fn find_cleanup_decision_stage(
    operation: &PrivateOperation<'_>,
    loaded: &LoadedCleanupIntent,
) -> io::Result<Option<(CString, CleanupDecisionStageName)>> {
    let mut found = None;
    for name in directory_entries(operation.directory.as_raw_fd())? {
        if !name
            .to_bytes()
            .starts_with(CLEANUP_DECISION_STAGE_PREFIX.as_bytes())
        {
            continue;
        }
        let parsed = parse_cleanup_decision_stage_name(&name)?.ok_or_else(cleanup_record_error)?;
        if parsed.key != loaded.intent.key_sha256 || parsed.intent != loaded.identity {
            return Err(os_error(libc::ESTALE));
        }
        if found.replace((name, parsed)).is_some() {
            return Err(os_error(libc::ESTALE));
        }
    }
    Ok(found)
}

fn load_cleanup_decision_stage_at(
    namespace: &PrivateNamespace,
    loaded: &LoadedCleanupIntent,
    operation: &PrivateOperation<'_>,
    name: &CStr,
    parsed: &CleanupDecisionStageName,
) -> io::Result<LoadedCleanupDecision> {
    if parsed.key != loaded.intent.key_sha256 || parsed.intent != loaded.identity {
        return Err(os_error(libc::ESTALE));
    }
    let expected = cleanup_decision_record(loaded, parsed.decision);
    let expected_bytes = cleanup_canonical_json(&expected)?;
    let (file, bytes, identity) = open_cleanup_record(
        operation.directory.as_raw_fd(),
        name,
        namespace.identity.device,
        false,
    )?;
    let record = match cleanup_parse_stage_json::<CleanupDecisionRecordV1>(&bytes)? {
        CleanupStageJson::Complete(record) => {
            if record != expected {
                return Err(os_error(libc::ESTALE));
            }
            file.sync_all()?;
            injected_cleanup_decision_adoption_sync_result()?;
            revalidate_cleanup_record_bytes(
                operation.directory.as_raw_fd(),
                name,
                &file,
                identity,
                namespace.identity.device,
                &expected_bytes,
            )?;
            record
        }
        CleanupStageJson::Truncated => {
            if !expected_bytes.starts_with(&bytes) {
                return Err(cleanup_record_error());
            }
            rewrite_cleanup_decision_stage(
                operation.directory.as_raw_fd(),
                name,
                &file,
                identity,
                namespace.identity.device,
                &expected_bytes,
            )?;
            expected
        }
    };
    Ok(LoadedCleanupDecision {
        record,
        file,
        identity,
    })
}

fn rewrite_cleanup_decision_stage(
    parent: RawFd,
    name: &CStr,
    file: &File,
    identity: FileIdentity,
    expected_device: u64,
    bytes: &[u8],
) -> io::Result<()> {
    injected_cleanup_decision_rewrite_handoff();
    revalidate_cleanup_record_binding(parent, name, file, identity, expected_device)?;
    cvt(unsafe { libc::ftruncate(file.as_raw_fd(), 0) })?;
    let midpoint = bytes.len() / 2;
    file.write_all_at(&bytes[..midpoint], 0)?;
    file.sync_all()?;
    injected_cleanup_decision_write_result()?;
    file.write_all_at(&bytes[midpoint..], midpoint as u64)?;
    injected_cleanup_decision_write_before_sync_result()?;
    file.sync_all()?;
    reopen_and_revalidate_cleanup_record_bytes(parent, name, identity, expected_device, bytes)
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
    let record = cleanup_decision_record(loaded, decision);
    let stage_name = cleanup_decision_stage_name(&record.key_sha256, loaded.identity, decision)?;
    let decision_name = cleanup_decision_name(&record.key_sha256)?;
    let descriptor = create_regular_at(operation.directory.as_raw_fd(), &stage_name)?;
    let file = File::from(descriptor);
    let opened = stat_fd(file.as_raw_fd())?;
    let rebound = stat_at(operation.directory.as_raw_fd(), &stage_name)?;
    require_cleanup_record_metadata(&opened, namespace.identity.device)?;
    require_cleanup_record_metadata(&rebound, namespace.identity.device)?;
    if !same_file(&opened, &rebound) {
        return Err(os_error(libc::ESTALE));
    }
    let identity = FileIdentity::from_stat(&opened);
    file.sync_all()?;
    cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
    injected_cleanup_decision_stage_sync_result()?;
    let bytes = cleanup_canonical_json(&record)?;
    rewrite_cleanup_decision_stage(
        operation.directory.as_raw_fd(),
        &stage_name,
        &file,
        identity,
        namespace.identity.device,
        &bytes,
    )?;
    injected_cleanup_decision_before_rename_result()?;
    rename_no_replace(
        operation.directory.as_raw_fd(),
        &stage_name,
        namespace.directory.as_raw_fd(),
        &decision_name,
    )?;
    injected_cleanup_decision_after_rename_result()?;
    cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })?;
    injected_cleanup_decision_destination_sync_result()?;
    cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
    injected_cleanup_decision_source_sync_result()?;
    let loaded = load_cleanup_decision_at(
        namespace.directory.as_raw_fd(),
        &decision_name,
        loaded,
        namespace,
    )?;
    if loaded.identity != identity {
        return Err(os_error(libc::ESTALE));
    }
    Ok(loaded)
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

enum CleanupInitialSnapshot {
    Evidence,
    Absent,
    Bound {
        metadata: libc::stat,
        generation: CleanupGenerationFence,
    },
}

enum CleanupTerminalSnapshot {
    Evidence,
    Absent,
    Public(libc::stat),
}

fn cleanup_same_key_evidence(namespace: &PrivateNamespace, key: &str) -> io::Result<bool> {
    let intent_name = cleanup_intent_name(key)?;
    if cleanup_optional_stat(namespace.directory.as_raw_fd(), &intent_name)?.is_some() {
        return Ok(true);
    }
    let slots = cleanup_bootstrap_slots(namespace, key)?;
    if slots.len() > 1 {
        return Err(os_error(libc::ESTALE));
    }
    Ok(!slots.is_empty())
}

fn cleanup_exact_initial_snapshot(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
    component: &CStr,
    expected_target: Option<FileIdentity>,
    expected_mode: Option<u32>,
    kind: CleanupTargetKind,
) -> io::Result<CleanupInitialSnapshot> {
    let key = cleanup_key(parent, component.to_bytes());
    with_cleanup_namespace_lock(namespace, || {
        if cleanup_same_key_evidence(namespace, &key)? {
            return Ok(CleanupInitialSnapshot::Evidence);
        }
        let metadata = match stat_at(public_parent, component) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                injected_cleanup_initial_terminal_handoff();
                return Ok(CleanupInitialSnapshot::Absent);
            }
            Err(error) => return Err(error),
        };
        if !cleanup_public_target_matches(&metadata, parent.device, kind) {
            return Err(os_error(libc::ESTALE));
        }
        let target = FileIdentity::from_stat(&metadata);
        let original_mode = metadata.st_mode as u32 & 0o7777;
        if expected_target.is_some_and(|expected| expected != target)
            || expected_mode.is_some_and(|expected| expected != original_mode)
        {
            return Err(os_error(libc::ESTALE));
        }
        let operation_name = cleanup_bootstrap_name(&key, target, original_mode, kind)?;
        match mkdir_at(namespace.directory.as_raw_fd(), &operation_name, 0o700) {
            Ok(()) => {}
            Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {
                return Err(os_error(libc::EAGAIN));
            }
            Err(error) => return Err(error),
        }
        cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })?;
        let before = stat_at(namespace.directory.as_raw_fd(), &operation_name)?;
        require_private_directory_on_device(&before, namespace.identity.device)?;
        let operation = open_directory_at(namespace.directory.as_raw_fd(), &operation_name)?;
        let opened = stat_fd(operation.as_raw_fd())?;
        let rebound = stat_at(namespace.directory.as_raw_fd(), &operation_name)?;
        require_private_directory_on_device(&opened, namespace.identity.device)?;
        require_private_directory_on_device(&rebound, namespace.identity.device)?;
        if before.st_mode & 0o777 != 0o700
            || opened.st_mode & 0o777 != 0o700
            || rebound.st_mode & 0o777 != 0o700
            || !same_file(&before, &opened)
            || !same_file(&opened, &rebound)
            || !directory_entries(operation.as_raw_fd())?.is_empty()
        {
            return Err(os_error(libc::ESTALE));
        }
        Ok(CleanupInitialSnapshot::Bound {
            metadata,
            generation: CleanupGenerationFence {
                intent: None,
                operation: operation_name,
                operation_identity: FileIdentity::from_stat(&opened),
            },
        })
    })
}

fn cleanup_terminal_snapshot(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
    component: &CStr,
) -> io::Result<CleanupTerminalSnapshot> {
    let key = cleanup_key(parent, component.to_bytes());
    with_cleanup_namespace_lock(namespace, || {
        if cleanup_same_key_evidence(namespace, &key)? {
            return Ok(CleanupTerminalSnapshot::Evidence);
        }
        match stat_at(public_parent, component) {
            Ok(metadata) => Ok(CleanupTerminalSnapshot::Public(metadata)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                injected_cleanup_final_terminal_handoff();
                Ok(CleanupTerminalSnapshot::Absent)
            }
            Err(error) => Err(error),
        }
    })
}

fn cleanup_pending_snapshot(
    namespace: &PrivateNamespace,
    parent: FileIdentity,
    component: &CStr,
) -> io::Result<bool> {
    let key = cleanup_key(parent, component.to_bytes());
    with_cleanup_namespace_lock(namespace, || {
        let pending = cleanup_same_key_evidence(namespace, &key)?;
        if !pending {
            injected_cleanup_initial_terminal_handoff();
        }
        Ok(pending)
    })
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
        match parsed.kind {
            CleanupTargetKind::Tree => {
                drop(open_bound_cleanup_directory(
                    public_parent,
                    &name,
                    parsed.target,
                    parent.device,
                    Some(parsed.original_mode),
                )?);
            }
            CleanupTargetKind::Regular => {
                drop(open_bound_cleanup_regular(
                    public_parent,
                    &name,
                    parsed.target,
                    parent.device,
                    parsed.original_mode,
                )?);
            }
        }
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
    require_private_directory_on_device(&before, expected_device)
        .map_err(|_| os_error(libc::ESTALE))?;
    if before.st_mode & 0o777 != 0o700 {
        return Err(os_error(libc::ESTALE));
    }
    let placeholder = open_directory_at(operation, CLEANUP_PLACEHOLDER_NAME)?;
    let opened = stat_fd(placeholder.as_raw_fd())?;
    let rebound = stat_at(operation, CLEANUP_PLACEHOLDER_NAME)?;
    require_private_directory_on_device(&opened, expected_device)
        .map_err(|_| os_error(libc::ESTALE))?;
    require_private_directory_on_device(&rebound, expected_device)
        .map_err(|_| os_error(libc::ESTALE))?;
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
    if entries.len() == 1 && entries[0].as_bytes() == CLEANUP_CAPABILITY_PROBE_NAME.to_bytes() {
        drop(validate_cleanup_capability_probe(
            operation.as_raw_fd(),
            namespace.identity.device,
        )?);
        return Ok(PendingTreeCleanupCandidate {
            component,
            target: parsed.target,
            original_mode: parsed.original_mode,
            kind: parsed.kind,
            generation: CleanupGenerationFence {
                intent: None,
                operation: operation_name.to_owned(),
                operation_identity,
            },
        });
    }
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
        match cleanup_parse_stage_json::<CleanupIntentV1>(&bytes)? {
            CleanupStageJson::Complete(intent) => {
                let bindings = validate_cleanup_intent(&intent, parent, &component, namespace)?;
                if intent.key_sha256 != parsed.key
                    || bindings.operation.as_bytes() != operation_name.to_bytes()
                    || bindings.operation_identity != operation_identity
                    || bindings.target != parsed.target
                    || bindings.original_mode != parsed.original_mode
                    || bindings.kind != parsed.kind
                    || placeholder != Some(bindings.placeholder_identity)
                    || cleanup_optional_stat(namespace.directory.as_raw_fd(), &bindings.quarantine)?
                        .is_some()
                {
                    return Err(os_error(libc::ESTALE));
                }
            }
            CleanupStageJson::Truncated => {
                let placeholder = placeholder.ok_or_else(|| os_error(libc::ESTALE))?;
                let component =
                    CString::new(component.as_slice()).map_err(|_| cleanup_record_error())?;
                if !cleanup_truncated_intent_is_attributable(
                    &bytes,
                    &parsed.key,
                    &component,
                    parent,
                    namespace,
                    parsed.target,
                    parsed.original_mode,
                    operation_name,
                    operation_identity,
                    placeholder,
                    parsed.kind,
                )? {
                    return Err(cleanup_record_error());
                }
            }
        }
    }
    Ok(PendingTreeCleanupCandidate {
        component,
        target: parsed.target,
        original_mode: parsed.original_mode,
        kind: parsed.kind,
        generation: CleanupGenerationFence {
            intent: None,
            operation: operation_name.to_owned(),
            operation_identity,
        },
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

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn resolve_bound_tree_cleanup(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
    component: &[u8],
    target: FileIdentity,
    original_mode: u32,
    generation: Option<&CleanupGenerationFence>,
    verify_parent: &impl Fn() -> io::Result<()>,
    allow_injected_validation: bool,
) -> io::Result<TreeCleanupOutcome> {
    resolve_bound_cleanup(
        namespace,
        public_parent,
        parent,
        component,
        target,
        original_mode,
        generation,
        verify_parent,
        allow_injected_validation,
        CleanupTargetKind::Tree,
        &|| Ok(()),
    )
}

#[allow(clippy::too_many_arguments)]
fn resolve_bound_cleanup(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
    component: &[u8],
    target: FileIdentity,
    original_mode: u32,
    generation: Option<&CleanupGenerationFence>,
    verify_parent: &impl Fn() -> io::Result<()>,
    allow_injected_validation: bool,
    kind: CleanupTargetKind,
    after_durable_delete: &dyn Fn() -> io::Result<()>,
) -> io::Result<TreeCleanupOutcome> {
    let component_name = CString::new(component).map_err(|_| cleanup_record_error())?;
    let mut last_race = os_error(libc::ESTALE);
    let mut pending_restore_error = None;
    let mut restore_expected = false;
    let mut locally_completed_delete = false;
    for _ in 0..32 {
        validate_cleanup_namespace_evidence(namespace, public_parent, parent)?;
        if let Some(loaded) = find_cleanup_intent(namespace, parent, component)? {
            let current_generation =
                cleanup_generation_from_loaded(namespace, parent, component, &loaded)?;
            if loaded.intent.kind != kind
                || FileIdentity::from(loaded.intent.target) != target
                || loaded.intent.original_mode != original_mode
                || generation.is_some_and(|expected| {
                    !cleanup_generation_matches(expected, &current_generation)
                })
            {
                return Err(os_error(libc::ESTALE));
            }
            let result = resume_cleanup(
                namespace,
                public_parent,
                parent,
                component,
                &loaded,
                verify_parent,
                allow_injected_validation,
                &mut pending_restore_error,
                &mut restore_expected,
                after_durable_delete,
            );
            match result {
                Ok(TreeCleanupOutcome::Deleted) => {
                    locally_completed_delete = true;
                    continue;
                }
                Ok(TreeCleanupOutcome::AlreadyRetired) => {
                    return Ok(TreeCleanupOutcome::AlreadyRetired);
                }
                Ok(outcome @ TreeCleanupOutcome::Restored(_)) => return Ok(outcome),
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
            if candidate.kind != kind
                || candidate.target != target
                || candidate.original_mode != original_mode
            {
                return Err(os_error(libc::ESTALE));
            }
            if generation.is_some_and(|expected| {
                !cleanup_generation_matches(expected, &candidate.generation)
            }) {
                return Err(os_error(libc::ESTALE));
            }
            match publish_cleanup_intent(
                namespace,
                public_parent,
                parent,
                &component_name,
                target,
                original_mode,
                kind,
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
        match cleanup_terminal_snapshot(namespace, public_parent, parent, &component_name)? {
            CleanupTerminalSnapshot::Evidence => continue,
            CleanupTerminalSnapshot::Absent if restore_expected => {
                return Err(os_error(libc::ESTALE));
            }
            CleanupTerminalSnapshot::Absent if locally_completed_delete || generation.is_none() => {
                return Ok(TreeCleanupOutcome::Deleted);
            }
            CleanupTerminalSnapshot::Absent => {
                return Ok(TreeCleanupOutcome::AlreadyRetired);
            }
            CleanupTerminalSnapshot::Public(metadata) if restore_expected => {
                if !cleanup_public_target_matches(&metadata, parent.device, kind)
                    || FileIdentity::from_stat(&metadata) != target
                    || metadata.st_mode as u32 & 0o7777 != original_mode
                    || (kind == CleanupTargetKind::Regular && metadata.st_nlink != 1)
                {
                    return Err(os_error(libc::ESTALE));
                }
                return match open_bound_cleanup_target(
                    public_parent,
                    &component_name,
                    target,
                    parent.device,
                    original_mode,
                    kind,
                ) {
                    Ok(_) => Ok(TreeCleanupOutcome::Restored(pending_restore_error.take())),
                    Err(error)
                        if matches!(
                            error.raw_os_error(),
                            Some(libc::EAGAIN) | Some(libc::ENOENT) | Some(libc::ESTALE)
                        ) =>
                    {
                        Err(os_error(libc::ESTALE))
                    }
                    Err(error) => Err(error),
                };
            }
            CleanupTerminalSnapshot::Public(_) => return Err(os_error(libc::ESTALE)),
        }
    }
    Err(last_race)
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn complete_bound_tree_cleanup(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
    component: &[u8],
    target: FileIdentity,
    original_mode: u32,
    generation: Option<CleanupGenerationFence>,
    verify_parent: &impl Fn() -> io::Result<()>,
    allow_injected_validation: bool,
) -> io::Result<()> {
    complete_bound_cleanup(
        namespace,
        public_parent,
        parent,
        component,
        target,
        original_mode,
        generation,
        verify_parent,
        allow_injected_validation,
        CleanupTargetKind::Tree,
        &|| Ok(()),
    )
    .map(|_| ())
}

#[allow(clippy::too_many_arguments)]
fn complete_bound_tree_cleanup_outcome(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
    component: &[u8],
    target: FileIdentity,
    original_mode: u32,
    generation: Option<CleanupGenerationFence>,
    verify_parent: &impl Fn() -> io::Result<()>,
    allow_injected_validation: bool,
) -> io::Result<TreeCleanupOutcome> {
    complete_bound_cleanup(
        namespace,
        public_parent,
        parent,
        component,
        target,
        original_mode,
        generation,
        verify_parent,
        allow_injected_validation,
        CleanupTargetKind::Tree,
        &|| Ok(()),
    )
}

#[allow(clippy::too_many_arguments)]
fn complete_bound_cleanup(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
    component: &[u8],
    target: FileIdentity,
    original_mode: u32,
    mut generation: Option<CleanupGenerationFence>,
    verify_parent: &impl Fn() -> io::Result<()>,
    allow_injected_validation: bool,
    kind: CleanupTargetKind,
    after_durable_delete: &dyn Fn() -> io::Result<()>,
) -> io::Result<TreeCleanupOutcome> {
    injected_cleanup_before_bound_completion_result()?;
    let component_name = CString::new(component).map_err(|_| cleanup_record_error())?;
    for continuation in 0..2 {
        match resolve_bound_cleanup(
            namespace,
            public_parent,
            parent,
            component,
            target,
            original_mode,
            generation.as_ref(),
            verify_parent,
            allow_injected_validation,
            kind,
            after_durable_delete,
        )? {
            TreeCleanupOutcome::Deleted => return Ok(TreeCleanupOutcome::Deleted),
            TreeCleanupOutcome::AlreadyRetired => {
                return Ok(TreeCleanupOutcome::AlreadyRetired);
            }
            TreeCleanupOutcome::Restored(Some(error)) => return Err(error),
            TreeCleanupOutcome::Restored(None) => {
                if continuation != 0 {
                    return Err(os_error(libc::ESTALE));
                }
                verify_parent()?;
                validate_cleanup_namespace_evidence(namespace, public_parent, parent)?;
                let next_generation = match cleanup_exact_initial_snapshot(
                    namespace,
                    public_parent,
                    parent,
                    &component_name,
                    Some(target),
                    Some(original_mode),
                    kind,
                )? {
                    CleanupInitialSnapshot::Bound { generation, .. } => generation,
                    CleanupInitialSnapshot::Absent | CleanupInitialSnapshot::Evidence => {
                        return Err(os_error(libc::ESTALE));
                    }
                };
                injected_cleanup_bootstrap_result()?;
                drop(open_bound_cleanup_target(
                    public_parent,
                    &component_name,
                    target,
                    parent.device,
                    original_mode,
                    kind,
                )?);
                generation = Some(next_generation);
                match publish_cleanup_intent(
                    namespace,
                    public_parent,
                    parent,
                    &component_name,
                    target,
                    original_mode,
                    kind,
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
            }
        }
    }
    Err(os_error(libc::ESTALE))
}

fn open_or_create_cleanup_bootstrap<'a>(
    namespace: &'a PrivateNamespace,
    public_parent: RawFd,
    component: &CStr,
    key: &str,
    target: FileIdentity,
    original_mode: u32,
    kind: CleanupTargetKind,
) -> io::Result<Option<PrivateOperation<'a>>> {
    let expected_name = cleanup_bootstrap_name(key, target, original_mode, kind)?;
    let intent_name = cleanup_intent_name(key)?;
    for _ in 0..32 {
        match open_bound_cleanup_target(
            public_parent,
            component,
            target,
            target.device,
            original_mode,
            kind,
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
                    || parsed.kind != kind
                {
                    return Err(os_error(libc::ESTALE));
                }
                false
            } else {
                drop(open_bound_cleanup_target(
                    public_parent,
                    component,
                    target,
                    target.device,
                    original_mode,
                    kind,
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
            drop(open_bound_cleanup_target(
                public_parent,
                component,
                target,
                target.device,
                original_mode,
                kind,
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

struct ValidatedCleanupProbeEntry {
    name: CString,
    identity: FileIdentity,
    flags: libc::c_int,
}

struct ValidatedCleanupProbeSide {
    name: &'static CStr,
    directory: OwnedFd,
    identity: FileIdentity,
    entries: Vec<ValidatedCleanupProbeEntry>,
}

struct ValidatedCleanupProbe {
    directory: OwnedFd,
    identity: FileIdentity,
    left: Option<ValidatedCleanupProbeSide>,
    right: Option<ValidatedCleanupProbeSide>,
}

fn open_cleanup_probe_directory(
    parent: RawFd,
    name: &CStr,
    expected_device: u64,
) -> io::Result<(OwnedFd, FileIdentity)> {
    let before = stat_at(parent, name)?;
    require_private_directory_on_device(&before, expected_device)
        .map_err(|_| os_error(libc::ESTALE))?;
    if before.st_mode & 0o777 != 0o700 {
        return Err(os_error(libc::ESTALE));
    }
    let directory = open_directory_at(parent, name)?;
    let opened = stat_fd(directory.as_raw_fd())?;
    let rebound = stat_at(parent, name)?;
    require_private_directory_on_device(&opened, expected_device)
        .map_err(|_| os_error(libc::ESTALE))?;
    require_private_directory_on_device(&rebound, expected_device)
        .map_err(|_| os_error(libc::ESTALE))?;
    if opened.st_mode & 0o777 != 0o700
        || rebound.st_mode & 0o777 != 0o700
        || !same_file(&before, &opened)
        || !same_file(&opened, &rebound)
    {
        return Err(os_error(libc::ESTALE));
    }
    Ok((directory, FileIdentity::from_stat(&opened)))
}

fn validate_cleanup_probe_side(
    probe: RawFd,
    name: &'static CStr,
    expected_device: u64,
    left: bool,
) -> io::Result<ValidatedCleanupProbeSide> {
    let (directory, identity) = open_cleanup_probe_directory(probe, name, expected_device)?;
    let mut entries = Vec::new();
    for entry in directory_entries(directory.as_raw_fd())? {
        let regular_exchange = entry.as_bytes() == CLEANUP_PROBE_REGULAR_EXCHANGE_LEFT.to_bytes()
            || entry.as_bytes() == CLEANUP_PROBE_REGULAR_EXCHANGE_RIGHT.to_bytes();
        let directory_entry = if left {
            entry.as_bytes() == CLEANUP_PROBE_DIRECTORY_SOURCE.to_bytes()
                || entry.as_bytes() == CLEANUP_PROBE_EXCHANGE_LEFT.to_bytes()
        } else {
            entry.as_bytes() == CLEANUP_PROBE_DIRECTORY_DESTINATION.to_bytes()
                || entry.as_bytes() == CLEANUP_PROBE_EXCHANGE_RIGHT.to_bytes()
        };
        let regular_entry = if left {
            entry.as_bytes() == CLEANUP_PROBE_REGULAR_SOURCE.to_bytes()
        } else {
            entry.as_bytes() == CLEANUP_PROBE_REGULAR_DESTINATION.to_bytes()
        };
        if !directory_entry && !regular_entry && !regular_exchange {
            return Err(os_error(libc::ESTALE));
        }
        let before = stat_at(directory.as_raw_fd(), &entry)?;
        let entry_identity = FileIdentity::from_stat(&before);
        let directory_entry =
            directory_entry || (regular_exchange && file_type(before.st_mode) == libc::S_IFDIR);
        if directory_entry {
            let (opened, identity) =
                open_cleanup_probe_directory(directory.as_raw_fd(), &entry, expected_device)?;
            if identity != entry_identity || !directory_entries(opened.as_raw_fd())?.is_empty() {
                return Err(os_error(libc::ESTALE));
            }
            entries.push(ValidatedCleanupProbeEntry {
                name: entry,
                identity,
                flags: libc::AT_REMOVEDIR,
            });
        } else {
            require_cleanup_record_metadata(&before, expected_device)
                .map_err(|_| os_error(libc::ESTALE))?;
            if before.st_size != 0 {
                return Err(os_error(libc::ESTALE));
            }
            let file = File::from(open_regular_rw_at(directory.as_raw_fd(), &entry)?);
            let opened = stat_fd(file.as_raw_fd())?;
            let rebound = stat_at(directory.as_raw_fd(), &entry)?;
            require_cleanup_record_metadata(&opened, expected_device)
                .map_err(|_| os_error(libc::ESTALE))?;
            require_cleanup_record_metadata(&rebound, expected_device)
                .map_err(|_| os_error(libc::ESTALE))?;
            if opened.st_size != 0
                || rebound.st_size != 0
                || !same_file(&before, &opened)
                || !same_file(&opened, &rebound)
            {
                return Err(os_error(libc::ESTALE));
            }
            entries.push(ValidatedCleanupProbeEntry {
                name: entry,
                identity: entry_identity,
                flags: 0,
            });
        }
    }
    Ok(ValidatedCleanupProbeSide {
        name,
        directory,
        identity,
        entries,
    })
}

fn validate_cleanup_capability_probe(
    operation: RawFd,
    expected_device: u64,
) -> io::Result<ValidatedCleanupProbe> {
    let (directory, identity) =
        open_cleanup_probe_directory(operation, CLEANUP_CAPABILITY_PROBE_NAME, expected_device)?;
    let entries = directory_entries(directory.as_raw_fd())?;
    if entries.iter().any(|entry| {
        entry.as_bytes() != CLEANUP_CAPABILITY_PROBE_LEFT.to_bytes()
            && entry.as_bytes() != CLEANUP_CAPABILITY_PROBE_RIGHT.to_bytes()
    }) {
        return Err(os_error(libc::ESTALE));
    }
    let has_left = entries
        .iter()
        .any(|entry| entry.as_bytes() == CLEANUP_CAPABILITY_PROBE_LEFT.to_bytes());
    let has_right = entries
        .iter()
        .any(|entry| entry.as_bytes() == CLEANUP_CAPABILITY_PROBE_RIGHT.to_bytes());
    if has_right && !has_left {
        return Err(os_error(libc::ESTALE));
    }
    let left = has_left
        .then(|| {
            validate_cleanup_probe_side(
                directory.as_raw_fd(),
                CLEANUP_CAPABILITY_PROBE_LEFT,
                expected_device,
                true,
            )
        })
        .transpose()?;
    let right = has_right
        .then(|| {
            validate_cleanup_probe_side(
                directory.as_raw_fd(),
                CLEANUP_CAPABILITY_PROBE_RIGHT,
                expected_device,
                false,
            )
        })
        .transpose()?;
    Ok(ValidatedCleanupProbe {
        directory,
        identity,
        left,
        right,
    })
}

fn remove_validated_cleanup_probe_entry(
    parent: RawFd,
    entry: &ValidatedCleanupProbeEntry,
    expected_device: u64,
) -> io::Result<()> {
    let current = stat_at(parent, &entry.name)?;
    if FileIdentity::from_stat(&current) != entry.identity {
        return Err(os_error(libc::ESTALE));
    }
    if entry.flags == libc::AT_REMOVEDIR {
        let (directory, identity) =
            open_cleanup_probe_directory(parent, &entry.name, expected_device)?;
        if identity != entry.identity || !directory_entries(directory.as_raw_fd())?.is_empty() {
            return Err(os_error(libc::ESTALE));
        }
    } else {
        require_cleanup_record_metadata(&current, expected_device)?;
        if current.st_size != 0 {
            return Err(os_error(libc::ESTALE));
        }
        let file = File::from(open_regular_rw_at(parent, &entry.name)?);
        let opened = stat_fd(file.as_raw_fd())?;
        if FileIdentity::from_stat(&opened) != entry.identity || !same_file(&current, &opened) {
            return Err(os_error(libc::ESTALE));
        }
    }
    unlink_at(parent, &entry.name, entry.flags)?;
    cvt(unsafe { libc::fsync(parent) })
}

fn remove_validated_cleanup_probe_side(
    probe: RawFd,
    side: ValidatedCleanupProbeSide,
    expected_device: u64,
) -> io::Result<()> {
    for entry in &side.entries {
        remove_validated_cleanup_probe_entry(side.directory.as_raw_fd(), entry, expected_device)?;
    }
    if !directory_entries(side.directory.as_raw_fd())?.is_empty() {
        return Err(os_error(libc::ESTALE));
    }
    let opened = stat_fd(side.directory.as_raw_fd())?;
    let rebound = stat_at(probe, side.name)?;
    if FileIdentity::from_stat(&opened) != side.identity
        || FileIdentity::from_stat(&rebound) != side.identity
        || !same_file(&opened, &rebound)
    {
        return Err(os_error(libc::ESTALE));
    }
    unlink_at(probe, side.name, libc::AT_REMOVEDIR)?;
    cvt(unsafe { libc::fsync(probe) })
}

fn reset_cleanup_capability_probe(
    operation: &PrivateOperation<'_>,
    expected_device: u64,
    inject_transitions: bool,
) -> io::Result<()> {
    let validated =
        validate_cleanup_capability_probe(operation.directory.as_raw_fd(), expected_device)?;
    if let Some(right) = validated.right {
        remove_validated_cleanup_probe_side(
            validated.directory.as_raw_fd(),
            right,
            expected_device,
        )?;
        if inject_transitions {
            injected_cleanup_probe_transition_result()?;
        }
    }
    if let Some(left) = validated.left {
        remove_validated_cleanup_probe_side(
            validated.directory.as_raw_fd(),
            left,
            expected_device,
        )?;
        if inject_transitions {
            injected_cleanup_probe_transition_result()?;
        }
    }
    if !directory_entries(validated.directory.as_raw_fd())?.is_empty() {
        return Err(os_error(libc::ESTALE));
    }
    let opened = stat_fd(validated.directory.as_raw_fd())?;
    let rebound = stat_at(
        operation.directory.as_raw_fd(),
        CLEANUP_CAPABILITY_PROBE_NAME,
    )?;
    if FileIdentity::from_stat(&opened) != validated.identity
        || FileIdentity::from_stat(&rebound) != validated.identity
        || !same_file(&opened, &rebound)
    {
        return Err(os_error(libc::ESTALE));
    }
    unlink_at(
        operation.directory.as_raw_fd(),
        CLEANUP_CAPABILITY_PROBE_NAME,
        libc::AT_REMOVEDIR,
    )?;
    cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
    if inject_transitions {
        injected_cleanup_probe_transition_result()?;
    }
    Ok(())
}

fn create_cleanup_probe_entry(
    parent: RawFd,
    name: &CStr,
    directory: bool,
    expected_device: u64,
) -> io::Result<FileIdentity> {
    if directory {
        mkdir_at(parent, name, 0o700)?;
        let (opened, identity) = open_cleanup_probe_directory(parent, name, expected_device)?;
        cvt(unsafe { libc::fsync(opened.as_raw_fd()) })?;
        cvt(unsafe { libc::fsync(parent) })?;
        Ok(identity)
    } else {
        let file = File::from(create_regular_at(parent, name)?);
        let opened = stat_fd(file.as_raw_fd())?;
        let rebound = stat_at(parent, name)?;
        require_cleanup_record_metadata(&opened, expected_device)?;
        require_cleanup_record_metadata(&rebound, expected_device)?;
        if opened.st_size != 0 || rebound.st_size != 0 || !same_file(&opened, &rebound) {
            return Err(os_error(libc::ESTALE));
        }
        file.sync_all()?;
        cvt(unsafe { libc::fsync(parent) })?;
        Ok(FileIdentity::from_stat(&opened))
    }
}

fn remove_cleanup_probe_entry(
    parent: RawFd,
    name: &CStr,
    identity: FileIdentity,
    directory: bool,
    expected_device: u64,
) -> io::Result<()> {
    remove_validated_cleanup_probe_entry(
        parent,
        &ValidatedCleanupProbeEntry {
            name: name.to_owned(),
            identity,
            flags: if directory { libc::AT_REMOVEDIR } else { 0 },
        },
        expected_device,
    )
}

fn probe_cleanup_no_replace(
    left: RawFd,
    right: RawFd,
    source: &CStr,
    destination: &CStr,
    directory: bool,
    expected_device: u64,
) -> io::Result<()> {
    let source_identity = create_cleanup_probe_entry(left, source, directory, expected_device)?;
    injected_cleanup_probe_transition_result()?;
    let destination_identity =
        create_cleanup_probe_entry(right, destination, directory, expected_device)?;
    injected_cleanup_probe_transition_result()?;
    match rename_no_replace(left, source, right, destination) {
        Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {}
        Err(error) => return Err(error),
        Ok(()) => return Err(os_error(libc::ENOTSUP)),
    }
    injected_cleanup_probe_transition_result()?;
    let current_source = stat_at(left, source)?;
    let current_destination = stat_at(right, destination)?;
    if FileIdentity::from_stat(&current_source) != source_identity
        || FileIdentity::from_stat(&current_destination) != destination_identity
    {
        return Err(os_error(libc::ESTALE));
    }
    remove_cleanup_probe_entry(
        right,
        destination,
        destination_identity,
        directory,
        expected_device,
    )?;
    injected_cleanup_probe_transition_result()?;
    rename_no_replace(left, source, right, destination)?;
    cvt(unsafe { libc::fsync(left) })?;
    cvt(unsafe { libc::fsync(right) })?;
    injected_cleanup_probe_transition_result()?;
    if FileIdentity::from_stat(&stat_at(right, destination)?) != source_identity {
        return Err(os_error(libc::ESTALE));
    }
    remove_cleanup_probe_entry(
        right,
        destination,
        source_identity,
        directory,
        expected_device,
    )?;
    injected_cleanup_probe_transition_result()
}

fn probe_cleanup_directory_exchange(
    left: RawFd,
    right: RawFd,
    expected_device: u64,
) -> io::Result<()> {
    let left_identity =
        create_cleanup_probe_entry(left, CLEANUP_PROBE_EXCHANGE_LEFT, true, expected_device)?;
    injected_cleanup_probe_transition_result()?;
    let right_identity =
        create_cleanup_probe_entry(right, CLEANUP_PROBE_EXCHANGE_RIGHT, true, expected_device)?;
    injected_cleanup_probe_transition_result()?;
    exchange_entries(
        left,
        CLEANUP_PROBE_EXCHANGE_LEFT,
        right,
        CLEANUP_PROBE_EXCHANGE_RIGHT,
    )?;
    cvt(unsafe { libc::fsync(left) })?;
    cvt(unsafe { libc::fsync(right) })?;
    injected_cleanup_probe_transition_result()?;
    if FileIdentity::from_stat(&stat_at(left, CLEANUP_PROBE_EXCHANGE_LEFT)?) != right_identity
        || FileIdentity::from_stat(&stat_at(right, CLEANUP_PROBE_EXCHANGE_RIGHT)?) != left_identity
    {
        return Err(os_error(libc::ENOTSUP));
    }
    exchange_entries(
        left,
        CLEANUP_PROBE_EXCHANGE_LEFT,
        right,
        CLEANUP_PROBE_EXCHANGE_RIGHT,
    )?;
    cvt(unsafe { libc::fsync(left) })?;
    cvt(unsafe { libc::fsync(right) })?;
    injected_cleanup_probe_transition_result()?;
    if FileIdentity::from_stat(&stat_at(left, CLEANUP_PROBE_EXCHANGE_LEFT)?) != left_identity
        || FileIdentity::from_stat(&stat_at(right, CLEANUP_PROBE_EXCHANGE_RIGHT)?) != right_identity
    {
        return Err(os_error(libc::ENOTSUP));
    }
    remove_cleanup_probe_entry(
        left,
        CLEANUP_PROBE_EXCHANGE_LEFT,
        left_identity,
        true,
        expected_device,
    )?;
    injected_cleanup_probe_transition_result()?;
    remove_cleanup_probe_entry(
        right,
        CLEANUP_PROBE_EXCHANGE_RIGHT,
        right_identity,
        true,
        expected_device,
    )?;
    injected_cleanup_probe_transition_result()
}

fn run_cleanup_capability_probe(
    operation: &PrivateOperation<'_>,
    expected_device: u64,
) -> io::Result<()> {
    mkdir_at(
        operation.directory.as_raw_fd(),
        CLEANUP_CAPABILITY_PROBE_NAME,
        0o700,
    )?;
    cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
    injected_cleanup_probe_transition_result()?;
    let (probe, _) = open_cleanup_probe_directory(
        operation.directory.as_raw_fd(),
        CLEANUP_CAPABILITY_PROBE_NAME,
        expected_device,
    )?;
    mkdir_at(probe.as_raw_fd(), CLEANUP_CAPABILITY_PROBE_LEFT, 0o700)?;
    cvt(unsafe { libc::fsync(probe.as_raw_fd()) })?;
    injected_cleanup_probe_transition_result()?;
    mkdir_at(probe.as_raw_fd(), CLEANUP_CAPABILITY_PROBE_RIGHT, 0o700)?;
    cvt(unsafe { libc::fsync(probe.as_raw_fd()) })?;
    injected_cleanup_probe_transition_result()?;
    let (left, _) = open_cleanup_probe_directory(
        probe.as_raw_fd(),
        CLEANUP_CAPABILITY_PROBE_LEFT,
        expected_device,
    )?;
    let (right, _) = open_cleanup_probe_directory(
        probe.as_raw_fd(),
        CLEANUP_CAPABILITY_PROBE_RIGHT,
        expected_device,
    )?;
    probe_cleanup_no_replace(
        left.as_raw_fd(),
        right.as_raw_fd(),
        CLEANUP_PROBE_REGULAR_SOURCE,
        CLEANUP_PROBE_REGULAR_DESTINATION,
        false,
        expected_device,
    )?;
    probe_cleanup_no_replace(
        left.as_raw_fd(),
        right.as_raw_fd(),
        CLEANUP_PROBE_DIRECTORY_SOURCE,
        CLEANUP_PROBE_DIRECTORY_DESTINATION,
        true,
        expected_device,
    )?;
    probe_cleanup_directory_exchange(left.as_raw_fd(), right.as_raw_fd(), expected_device)?;
    reset_cleanup_capability_probe(operation, expected_device, true)
}

fn ensure_cleanup_tree_capabilities(
    namespace: &PrivateNamespace,
    operation: &mut PrivateOperation<'_>,
    public_parent: RawFd,
    component: &CStr,
    target: FileIdentity,
    original_mode: u32,
    stage_name: &CStr,
) -> io::Result<()> {
    drop(open_bound_cleanup_directory(
        public_parent,
        component,
        target,
        namespace.identity.device,
        Some(original_mode),
    )?);
    let entries = directory_entries(operation.directory.as_raw_fd())?;
    let has_probe = entries
        .iter()
        .any(|entry| entry.as_bytes() == CLEANUP_CAPABILITY_PROBE_NAME.to_bytes());
    let has_placeholder = entries
        .iter()
        .any(|entry| entry.as_bytes() == CLEANUP_PLACEHOLDER_NAME.to_bytes());
    if has_placeholder {
        if has_probe
            || entries.len() > 2
            || entries.iter().any(|entry| {
                entry.as_bytes() != CLEANUP_PLACEHOLDER_NAME.to_bytes()
                    && entry.as_bytes() != stage_name.to_bytes()
            })
        {
            return Err(os_error(libc::ESTALE));
        }
        inspect_cleanup_bootstrap_placeholder(
            operation.directory.as_raw_fd(),
            namespace.identity.device,
        )?
        .ok_or_else(|| os_error(libc::ESTALE))?;
        return Ok(());
    }
    if has_probe {
        if entries.len() != 1 {
            return Err(os_error(libc::ESTALE));
        }
        reset_cleanup_capability_probe(operation, namespace.identity.device, false)?;
    } else if !entries.is_empty() {
        return Err(os_error(libc::ESTALE));
    }
    drop(open_bound_cleanup_directory(
        public_parent,
        component,
        target,
        namespace.identity.device,
        Some(original_mode),
    )?);
    match run_cleanup_capability_probe(operation, namespace.identity.device) {
        Ok(()) => {}
        Err(error) if cleanup_probe_was_interrupted() => return Err(error),
        Err(error) => {
            reset_cleanup_capability_probe(operation, namespace.identity.device, false)?;
            remove_empty_cleanup_operation(namespace, operation)?;
            return Err(error);
        }
    }
    injected_cleanup_probe_final_sync_result()
}

fn probe_cleanup_regular_exchange(
    left: RawFd,
    right: RawFd,
    expected_device: u64,
) -> io::Result<()> {
    let file_identity = create_cleanup_probe_entry(
        left,
        CLEANUP_PROBE_REGULAR_EXCHANGE_LEFT,
        false,
        expected_device,
    )?;
    injected_cleanup_probe_transition_result()?;
    let directory_identity = create_cleanup_probe_entry(
        right,
        CLEANUP_PROBE_REGULAR_EXCHANGE_RIGHT,
        true,
        expected_device,
    )?;
    injected_cleanup_probe_transition_result()?;
    exchange_entries(
        left,
        CLEANUP_PROBE_REGULAR_EXCHANGE_LEFT,
        right,
        CLEANUP_PROBE_REGULAR_EXCHANGE_RIGHT,
    )?;
    cvt(unsafe { libc::fsync(left) })?;
    cvt(unsafe { libc::fsync(right) })?;
    injected_cleanup_probe_transition_result()?;
    if FileIdentity::from_stat(&stat_at(left, CLEANUP_PROBE_REGULAR_EXCHANGE_LEFT)?)
        != directory_identity
        || FileIdentity::from_stat(&stat_at(right, CLEANUP_PROBE_REGULAR_EXCHANGE_RIGHT)?)
            != file_identity
    {
        return Err(os_error(libc::ENOTSUP));
    }
    exchange_entries(
        left,
        CLEANUP_PROBE_REGULAR_EXCHANGE_LEFT,
        right,
        CLEANUP_PROBE_REGULAR_EXCHANGE_RIGHT,
    )?;
    cvt(unsafe { libc::fsync(left) })?;
    cvt(unsafe { libc::fsync(right) })?;
    injected_cleanup_probe_transition_result()?;
    if FileIdentity::from_stat(&stat_at(left, CLEANUP_PROBE_REGULAR_EXCHANGE_LEFT)?)
        != file_identity
        || FileIdentity::from_stat(&stat_at(right, CLEANUP_PROBE_REGULAR_EXCHANGE_RIGHT)?)
            != directory_identity
    {
        return Err(os_error(libc::ENOTSUP));
    }
    remove_cleanup_probe_entry(
        left,
        CLEANUP_PROBE_REGULAR_EXCHANGE_LEFT,
        file_identity,
        false,
        expected_device,
    )?;
    injected_cleanup_probe_transition_result()?;
    remove_cleanup_probe_entry(
        right,
        CLEANUP_PROBE_REGULAR_EXCHANGE_RIGHT,
        directory_identity,
        true,
        expected_device,
    )?;
    injected_cleanup_probe_transition_result()
}

fn run_cleanup_regular_capability_probe(
    operation: &PrivateOperation<'_>,
    expected_device: u64,
) -> io::Result<()> {
    mkdir_at(
        operation.directory.as_raw_fd(),
        CLEANUP_CAPABILITY_PROBE_NAME,
        0o700,
    )?;
    cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
    injected_cleanup_probe_transition_result()?;
    let (probe, _) = open_cleanup_probe_directory(
        operation.directory.as_raw_fd(),
        CLEANUP_CAPABILITY_PROBE_NAME,
        expected_device,
    )?;
    mkdir_at(probe.as_raw_fd(), CLEANUP_CAPABILITY_PROBE_LEFT, 0o700)?;
    cvt(unsafe { libc::fsync(probe.as_raw_fd()) })?;
    injected_cleanup_probe_transition_result()?;
    mkdir_at(probe.as_raw_fd(), CLEANUP_CAPABILITY_PROBE_RIGHT, 0o700)?;
    cvt(unsafe { libc::fsync(probe.as_raw_fd()) })?;
    injected_cleanup_probe_transition_result()?;
    let (left, _) = open_cleanup_probe_directory(
        probe.as_raw_fd(),
        CLEANUP_CAPABILITY_PROBE_LEFT,
        expected_device,
    )?;
    let (right, _) = open_cleanup_probe_directory(
        probe.as_raw_fd(),
        CLEANUP_CAPABILITY_PROBE_RIGHT,
        expected_device,
    )?;
    probe_cleanup_no_replace(
        left.as_raw_fd(),
        right.as_raw_fd(),
        CLEANUP_PROBE_REGULAR_SOURCE,
        CLEANUP_PROBE_REGULAR_DESTINATION,
        false,
        expected_device,
    )?;
    probe_cleanup_regular_exchange(left.as_raw_fd(), right.as_raw_fd(), expected_device)?;
    reset_cleanup_capability_probe(operation, expected_device, true)
}

fn ensure_cleanup_regular_capabilities(
    namespace: &PrivateNamespace,
    operation: &mut PrivateOperation<'_>,
    public_parent: RawFd,
    component: &CStr,
    target: FileIdentity,
    original_mode: u32,
    stage_name: &CStr,
) -> io::Result<()> {
    drop(open_bound_cleanup_regular(
        public_parent,
        component,
        target,
        namespace.identity.device,
        original_mode,
    )?);
    let entries = directory_entries(operation.directory.as_raw_fd())?;
    let has_probe = entries
        .iter()
        .any(|entry| entry.as_bytes() == CLEANUP_CAPABILITY_PROBE_NAME.to_bytes());
    let has_placeholder = entries
        .iter()
        .any(|entry| entry.as_bytes() == CLEANUP_PLACEHOLDER_NAME.to_bytes());
    if has_placeholder {
        if has_probe
            || entries.len() > 2
            || entries.iter().any(|entry| {
                entry.as_bytes() != CLEANUP_PLACEHOLDER_NAME.to_bytes()
                    && entry.as_bytes() != stage_name.to_bytes()
            })
        {
            return Err(os_error(libc::ESTALE));
        }
        inspect_cleanup_bootstrap_placeholder(
            operation.directory.as_raw_fd(),
            namespace.identity.device,
        )?
        .ok_or_else(|| os_error(libc::ESTALE))?;
        return Ok(());
    }
    if has_probe {
        if entries.len() != 1 {
            return Err(os_error(libc::ESTALE));
        }
        reset_cleanup_capability_probe(operation, namespace.identity.device, false)?;
    } else if !entries.is_empty() {
        return Err(os_error(libc::ESTALE));
    }
    drop(open_bound_cleanup_regular(
        public_parent,
        component,
        target,
        namespace.identity.device,
        original_mode,
    )?);
    match run_cleanup_regular_capability_probe(operation, namespace.identity.device) {
        Ok(()) => {}
        Err(error) if cleanup_probe_was_interrupted() => return Err(error),
        Err(error) => {
            reset_cleanup_capability_probe(operation, namespace.identity.device, false)?;
            remove_empty_cleanup_operation(namespace, operation)?;
            return Err(error);
        }
    }
    injected_cleanup_probe_final_sync_result()
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
        cvt(unsafe { libc::fsync(placeholder.as_raw_fd()) })?;
        injected_cleanup_placeholder_object_sync_result()?;
        cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
        injected_cleanup_placeholder_result()?;
    }
    Ok(FileIdentity::from_stat(&opened))
}

#[allow(clippy::too_many_arguments)]
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
    kind: CleanupTargetKind,
) -> io::Result<CleanupIntentV1> {
    let key = cleanup_key(parent, component.to_bytes());
    if cleanup_optional_stat(operation.directory.as_raw_fd(), stage_name)?.is_some() {
        let (file, bytes, identity) = open_cleanup_record(
            operation.directory.as_raw_fd(),
            stage_name,
            namespace.identity.device,
            false,
        )?;
        match cleanup_parse_stage_json::<CleanupIntentV1>(&bytes)? {
            CleanupStageJson::Complete(parsed) => {
                let bindings =
                    validate_cleanup_intent(&parsed, parent, component.to_bytes(), namespace)?;
                let entries = directory_entries(operation.directory.as_raw_fd())?;
                if parsed.key_sha256 != key
                    || bindings.operation.as_bytes() != operation.name.as_bytes()
                    || bindings.operation_identity != operation.identity
                    || bindings.target != target
                    || bindings.original_mode != original_mode
                    || bindings.kind != kind
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
                drop(open_bound_cleanup_target(
                    public_parent,
                    component,
                    target,
                    namespace.identity.device,
                    original_mode,
                    kind,
                )?);
                if cleanup_optional_stat(namespace.directory.as_raw_fd(), &bindings.quarantine)?
                    .is_some()
                {
                    return Err(os_error(libc::ESTALE));
                }
                file.sync_all()?;
                injected_cleanup_intent_adoption_sync_result()?;
                revalidate_cleanup_record_bytes(
                    operation.directory.as_raw_fd(),
                    stage_name,
                    &file,
                    identity,
                    namespace.identity.device,
                    &bytes,
                )?;
                drop(file);
                return Ok(parsed);
            }
            CleanupStageJson::Truncated => {
                if !cleanup_truncated_intent_is_attributable(
                    &bytes,
                    &key,
                    component,
                    parent,
                    namespace,
                    target,
                    original_mode,
                    &operation.name,
                    operation.identity,
                    placeholder_identity,
                    kind,
                )? {
                    return Err(cleanup_record_error());
                }
            }
        }
        // A partial deterministic stage is rebuildable only while the exact
        // public target is still in its recorded original mode and no
        // quarantine/public mutation has occurred.
        drop(open_bound_cleanup_target(
            public_parent,
            component,
            target,
            namespace.identity.device,
            original_mode,
            kind,
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
    let quarantine = random_private_name(cleanup_quarantine_kind(kind));
    let intent = cleanup_intent_record(
        &key,
        component,
        parent,
        namespace,
        target,
        original_mode,
        quarantine
            .to_str()
            .map_err(|_| cleanup_record_error())?
            .to_owned(),
        &operation.name,
        operation.identity,
        placeholder_identity,
        kind,
    )?;
    validate_cleanup_intent(&intent, parent, component.to_bytes(), namespace)?;
    let bytes = cleanup_canonical_json(&intent)?;
    let descriptor = create_regular_at(operation.directory.as_raw_fd(), stage_name)?;
    let mut file = File::from(descriptor);
    let opened = stat_fd(file.as_raw_fd())?;
    let rebound = stat_at(operation.directory.as_raw_fd(), stage_name)?;
    require_cleanup_record_metadata(&opened, namespace.identity.device)?;
    require_cleanup_record_metadata(&rebound, namespace.identity.device)?;
    if !same_file(&opened, &rebound) {
        return Err(os_error(libc::ESTALE));
    }
    let identity = FileIdentity::from_stat(&opened);
    let midpoint = bytes.len() / 2;
    file.write_all(&bytes[..midpoint])?;
    file.sync_all()?;
    cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
    injected_cleanup_intent_write_result()?;
    file.write_all(&bytes[midpoint..])?;
    injected_cleanup_intent_write_before_sync_result()?;
    file.sync_all()?;
    reopen_and_revalidate_cleanup_record_bytes(
        operation.directory.as_raw_fd(),
        stage_name,
        identity,
        namespace.identity.device,
        &bytes,
    )?;
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
    publish_cleanup_intent(
        namespace,
        public_parent,
        parent,
        component,
        target,
        original_mode,
        CleanupTargetKind::Tree,
    )
}

fn publish_cleanup_intent(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
    component: &CStr,
    target: FileIdentity,
    original_mode: u32,
    kind: CleanupTargetKind,
) -> io::Result<()> {
    let key = cleanup_key(parent, component.to_bytes());
    let Some(mut operation) = open_or_create_cleanup_bootstrap(
        namespace,
        public_parent,
        component,
        &key,
        target,
        original_mode,
        kind,
    )?
    else {
        return Ok(());
    };
    let stage_name = cleanup_intent_stage_name(&key)?;
    match kind {
        CleanupTargetKind::Tree => ensure_cleanup_tree_capabilities(
            namespace,
            &mut operation,
            public_parent,
            component,
            target,
            original_mode,
            &stage_name,
        )?,
        CleanupTargetKind::Regular => ensure_cleanup_regular_capabilities(
            namespace,
            &mut operation,
            public_parent,
            component,
            target,
            original_mode,
            &stage_name,
        )?,
    }
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
        kind,
    )?;
    validate_cleanup_intent(&intent, parent, component.to_bytes(), namespace)?;
    let (staged_file, staged_bytes, staged_identity) = open_cleanup_record(
        operation.directory.as_raw_fd(),
        &stage_name,
        namespace.identity.device,
        true,
    )?;
    let staged_intent: CleanupIntentV1 = cleanup_parse_canonical_json(&staged_bytes)?;
    if staged_intent != intent {
        return Err(os_error(libc::ESTALE));
    }
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
    injected_cleanup_intent_after_rename_handoff();
    cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })?;
    cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
    revalidate_cleanup_record_bytes(
        namespace.directory.as_raw_fd(),
        &intent_name,
        &staged_file,
        staged_identity,
        namespace.identity.device,
        &staged_bytes,
    )?;
    operation.cleaned = true;
    injected_cleanup_intent_publish_result()?;
    Ok(())
}

fn classify_cleanup_slot(
    parent: RawFd,
    name: &CStr,
    target: FileIdentity,
    placeholder: FileIdentity,
    kind: CleanupTargetKind,
) -> io::Result<CleanupSlot> {
    let Some(metadata) = cleanup_optional_stat(parent, name)? else {
        return Ok(CleanupSlot::Missing);
    };
    let identity = FileIdentity::from_stat(&metadata);
    if identity == target {
        if file_type(metadata.st_mode) != cleanup_target_file_type(kind) {
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
        bindings.kind,
    )?;
    let quarantine = classify_cleanup_slot(
        namespace.directory.as_raw_fd(),
        &bindings.quarantine,
        bindings.target,
        bindings.placeholder_identity,
        bindings.kind,
    )?;
    let (operation_slot, operation_exists) = match operation {
        Some(operation) => (
            classify_cleanup_slot(
                operation.directory.as_raw_fd(),
                &bindings.placeholder,
                bindings.target,
                bindings.placeholder_identity,
                bindings.kind,
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

fn open_bound_cleanup_regular(
    parent: RawFd,
    name: &CStr,
    expected: FileIdentity,
    expected_device: u64,
    expected_mode: u32,
) -> io::Result<OwnedFd> {
    let before = stat_at(parent, name)?;
    if file_type(before.st_mode) != libc::S_IFREG
        || before.st_uid != effective_user_id()
        || before.st_dev as u64 != expected_device
        || FileIdentity::from_stat(&before) != expected
        || before.st_nlink != 1
        || before.st_mode as u32 & 0o7777 != expected_mode
    {
        return Err(os_error(libc::ESTALE));
    }
    let file = open_regular_at(parent, name)?;
    let opened = stat_fd(file.as_raw_fd())?;
    let rebound = stat_at(parent, name)?;
    if file_type(opened.st_mode) != libc::S_IFREG
        || opened.st_uid != effective_user_id()
        || opened.st_dev as u64 != expected_device
        || FileIdentity::from_stat(&opened) != expected
        || opened.st_nlink != 1
        || opened.st_mode as u32 & 0o7777 != expected_mode
        || file_type(rebound.st_mode) != libc::S_IFREG
        || rebound.st_uid != effective_user_id()
        || rebound.st_dev as u64 != expected_device
        || FileIdentity::from_stat(&rebound) != expected
        || rebound.st_nlink != 1
        || rebound.st_mode as u32 & 0o7777 != expected_mode
        || !same_file(&before, &opened)
        || !same_file(&opened, &rebound)
    {
        return Err(os_error(libc::ESTALE));
    }
    Ok(file)
}

fn open_bound_cleanup_target(
    parent: RawFd,
    name: &CStr,
    expected: FileIdentity,
    expected_device: u64,
    expected_mode: u32,
    kind: CleanupTargetKind,
) -> io::Result<OwnedFd> {
    match kind {
        CleanupTargetKind::Tree => open_bound_cleanup_directory(
            parent,
            name,
            expected,
            expected_device,
            Some(expected_mode),
        ),
        CleanupTargetKind::Regular => {
            open_bound_cleanup_regular(parent, name, expected, expected_device, expected_mode)
        }
    }
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
    cvt(unsafe { libc::flock(namespace.directory.as_raw_fd(), libc::LOCK_EX) })?;
    let removal = operation
        .remove_empty_owned()
        .and_then(|()| cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) }));
    let unlock = cvt(unsafe { libc::flock(namespace.directory.as_raw_fd(), libc::LOCK_UN) });
    match (removal, unlock) {
        (Ok(()), Ok(())) => {
            operation.cleaned = true;
            injected_cleanup_after_operation_removal_result()?;
            Ok(())
        }
        (Err(error), Ok(())) => Err(error),
        (_, Err(error)) => Err(error),
    }
}

fn remove_bound_cleanup_record(
    namespace: &PrivateNamespace,
    name: &CStr,
    file: &File,
    expected: FileIdentity,
    before_sync: impl FnOnce(),
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
    before_sync();
    cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })
}

fn finish_cleanup_records(
    namespace: &PrivateNamespace,
    loaded: &LoadedCleanupIntent,
    decision: Option<&LoadedCleanupDecision>,
) -> io::Result<()> {
    with_cleanup_namespace_lock(namespace, || {
        if let Some(decision) = decision {
            let decision_name = cleanup_decision_name(&loaded.intent.key_sha256)?;
            remove_bound_cleanup_record(
                namespace,
                &decision_name,
                &decision.file,
                decision.identity,
                || {},
            )?;
            injected_cleanup_after_decision_retire_result()?;
        }
        let intent_name = cleanup_intent_name(&loaded.intent.key_sha256)?;
        remove_bound_cleanup_record(
            namespace,
            &intent_name,
            &loaded.file,
            loaded.identity,
            injected_cleanup_intent_retire_before_sync_handoff,
        )?;
        injected_cleanup_after_intent_retire_result()
    })
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
    injected_cleanup_after_placeholder_removal_result()?;
    remove_empty_cleanup_operation(namespace, operation)
}

#[allow(clippy::too_many_arguments)]
fn resume_cleanup(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
    component: &[u8],
    loaded: &LoadedCleanupIntent,
    verify_parent: &impl Fn() -> io::Result<()>,
    allow_injected_validation: bool,
    pending_restore_error: &mut Option<io::Error>,
    restore_expected: &mut bool,
    after_durable_delete: &dyn Fn() -> io::Result<()>,
) -> io::Result<TreeCleanupOutcome> {
    let bindings = validate_cleanup_intent(&loaded.intent, parent, component, namespace)?;
    match bindings.kind {
        CleanupTargetKind::Tree => resume_tree_cleanup(
            namespace,
            public_parent,
            parent,
            component,
            loaded,
            verify_parent,
            allow_injected_validation,
            pending_restore_error,
            restore_expected,
            after_durable_delete,
        ),
        CleanupTargetKind::Regular => resume_regular_cleanup(
            namespace,
            public_parent,
            parent,
            component,
            loaded,
            verify_parent,
            allow_injected_validation,
            pending_restore_error,
            restore_expected,
            after_durable_delete,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn resume_tree_cleanup(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
    component: &[u8],
    loaded: &LoadedCleanupIntent,
    verify_parent: &impl Fn() -> io::Result<()>,
    allow_injected_validation: bool,
    pending_restore_error: &mut Option<io::Error>,
    restore_expected: &mut bool,
    after_durable_delete: &dyn Fn() -> io::Result<()>,
) -> io::Result<TreeCleanupOutcome> {
    let bindings = validate_cleanup_intent(&loaded.intent, parent, component, namespace)?;
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
                    return Ok(TreeCleanupOutcome::Deleted);
                }
                if slots.public == CleanupSlot::Target
                    && slots.quarantine == CleanupSlot::Missing
                    && slots.operation == CleanupSlot::Missing
                    && !slots.operation_exists
                {
                    *restore_expected = true;
                    let target = open_bound_cleanup_directory(
                        public_parent,
                        &bindings.component,
                        bindings.target,
                        bindings.parent.device,
                        Some(bindings.original_mode),
                    )?;
                    cvt(unsafe { libc::fsync(target.as_raw_fd()) })?;
                    cvt(unsafe { libc::fsync(public_parent) })?;
                    injected_cleanup_after_restore_sync_result()?;
                    finish_cleanup_records(namespace, loaded, None)?;
                    return Ok(TreeCleanupOutcome::Restored(pending_restore_error.take()));
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
                            *restore_expected = true;
                            if pending_restore_error.is_none() {
                                *pending_restore_error = Some(error);
                            }
                            publish_cleanup_decision(
                                namespace,
                                loaded,
                                operation,
                                CleanupDecisionV1::Restore,
                            )?;
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
                        injected_cleanup_after_quarantine_rename_result()?;
                        cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })?;
                        injected_cleanup_after_quarantine_namespace_sync_result()?;
                        cvt(unsafe { libc::fsync(public_parent) })?;
                        injected_cleanup_after_quarantine_parent_sync_result()?;
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
                                after_durable_delete()?;
                            }
                            Err(error) => {
                                *restore_expected = true;
                                if pending_restore_error.is_none() {
                                    *pending_restore_error = Some(error);
                                }
                                publish_cleanup_decision(
                                    namespace,
                                    loaded,
                                    operation,
                                    CleanupDecisionV1::Restore,
                                )?;
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
                        cvt(unsafe { libc::fsync(target.as_raw_fd()) })?;
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
                        injected_cleanup_after_target_exchange_result()?;
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
                        remove_acquired_directory_contents(target.as_raw_fd())?;
                        cvt(unsafe { libc::fsync(target.as_raw_fd()) })?;
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
                        return Ok(TreeCleanupOutcome::Deleted);
                    }
                    _ => return Err(os_error(libc::ESTALE)),
                }
            }
            Some(CleanupDecisionV1::Restore) => {
                *restore_expected = true;
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
                        injected_cleanup_restore_rollback_result()?;
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
                        cvt(unsafe { libc::fsync(public_parent) })?;
                        injected_cleanup_restore_destination_sync_result()?;
                        cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
                        injected_cleanup_restore_source_sync_result()?;
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
                        let target = open_bound_cleanup_directory_modes(
                            public_parent,
                            &bindings.component,
                            bindings.target,
                            bindings.parent.device,
                            &[bindings.original_mode, 0o700],
                        )?;
                        let current_mode = stat_fd(target.as_raw_fd())?.st_mode as u32 & 0o7777;
                        if current_mode != bindings.original_mode {
                            chmod_fd(target.as_raw_fd(), bindings.original_mode as libc::mode_t)?;
                        }
                        cvt(unsafe { libc::fsync(target.as_raw_fd()) })?;
                        cvt(unsafe { libc::fsync(public_parent) })?;
                        injected_cleanup_after_restore_sync_result()?;
                        finish_cleanup_records(namespace, loaded, decision.as_ref())?;
                        return Ok(TreeCleanupOutcome::Restored(pending_restore_error.take()));
                    }
                    _ => return Err(os_error(libc::ESTALE)),
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn resume_regular_cleanup(
    namespace: &PrivateNamespace,
    public_parent: RawFd,
    parent: FileIdentity,
    component: &[u8],
    loaded: &LoadedCleanupIntent,
    verify_parent: &impl Fn() -> io::Result<()>,
    allow_injected_validation: bool,
    pending_restore_error: &mut Option<io::Error>,
    restore_expected: &mut bool,
    after_durable_delete: &dyn Fn() -> io::Result<()>,
) -> io::Result<TreeCleanupOutcome> {
    let bindings = validate_cleanup_intent(&loaded.intent, parent, component, namespace)?;
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
                    return Ok(TreeCleanupOutcome::Deleted);
                }
                if slots.public == CleanupSlot::Target
                    && slots.quarantine == CleanupSlot::Missing
                    && slots.operation == CleanupSlot::Missing
                    && !slots.operation_exists
                {
                    *restore_expected = true;
                    let target = open_bound_cleanup_regular(
                        public_parent,
                        &bindings.component,
                        bindings.target,
                        bindings.parent.device,
                        bindings.original_mode,
                    )?;
                    cvt(unsafe { libc::fsync(target.as_raw_fd()) })?;
                    cvt(unsafe { libc::fsync(public_parent) })?;
                    injected_cleanup_after_restore_sync_result()?;
                    finish_cleanup_records(namespace, loaded, None)?;
                    return Ok(TreeCleanupOutcome::Restored(pending_restore_error.take()));
                }
                let operation = operation.as_ref().ok_or_else(|| os_error(libc::ESTALE))?;
                match (slots.public, slots.quarantine, slots.operation) {
                    (CleanupSlot::Target, CleanupSlot::Missing, CleanupSlot::Placeholder) => {
                        drop(open_bound_cleanup_regular(
                            public_parent,
                            &bindings.component,
                            bindings.target,
                            bindings.parent.device,
                            bindings.original_mode,
                        )?);
                        if let Err(error) = verify_parent() {
                            *restore_expected = true;
                            if pending_restore_error.is_none() {
                                *pending_restore_error = Some(error);
                            }
                            publish_cleanup_decision(
                                namespace,
                                loaded,
                                operation,
                                CleanupDecisionV1::Restore,
                            )?;
                            continue;
                        }
                        let public_target = open_bound_cleanup_regular(
                            public_parent,
                            &bindings.component,
                            bindings.target,
                            bindings.parent.device,
                            bindings.original_mode,
                        )?;
                        rename_no_replace(
                            public_parent,
                            &bindings.component,
                            namespace.directory.as_raw_fd(),
                            &bindings.quarantine,
                        )?;
                        injected_cleanup_after_quarantine_rename_result()?;
                        drop(public_target);
                        cvt(unsafe { libc::fsync(namespace.directory.as_raw_fd()) })?;
                        injected_cleanup_after_quarantine_namespace_sync_result()?;
                        cvt(unsafe { libc::fsync(public_parent) })?;
                        injected_cleanup_after_quarantine_parent_sync_result()?;
                    }
                    (CleanupSlot::Missing, CleanupSlot::Target, CleanupSlot::Placeholder) => {
                        drop(open_bound_cleanup_regular(
                            namespace.directory.as_raw_fd(),
                            &bindings.quarantine,
                            bindings.target,
                            bindings.parent.device,
                            bindings.original_mode,
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
                                after_durable_delete()?;
                            }
                            Err(error) => {
                                *restore_expected = true;
                                if pending_restore_error.is_none() {
                                    *pending_restore_error = Some(error);
                                }
                                publish_cleanup_decision(
                                    namespace,
                                    loaded,
                                    operation,
                                    CleanupDecisionV1::Restore,
                                )?;
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
                        drop(open_bound_cleanup_regular(
                            namespace.directory.as_raw_fd(),
                            &bindings.quarantine,
                            bindings.target,
                            bindings.parent.device,
                            bindings.original_mode,
                        )?);
                        let operation = operation.as_mut().ok_or_else(|| os_error(libc::ESTALE))?;
                        capture_expected_entry(
                            operation,
                            namespace.directory.as_raw_fd(),
                            &bindings.quarantine,
                            &bindings.placeholder,
                            bindings.placeholder_identity,
                            bindings.target,
                            false,
                        )?;
                        injected_cleanup_after_target_exchange_result()?;
                    }
                    (CleanupSlot::Placeholder, CleanupSlot::Target, true) => {
                        let operation = operation.as_mut().ok_or_else(|| os_error(libc::ESTALE))?;
                        injected_cleanup_same_call_transition();
                        injected_cleanup_final_remove_result()?;
                        let target = open_bound_cleanup_regular(
                            operation.directory.as_raw_fd(),
                            &bindings.placeholder,
                            bindings.target,
                            bindings.parent.device,
                            bindings.original_mode,
                        )?;
                        let opened = stat_fd(target.as_raw_fd())?;
                        if file_type(opened.st_mode) != libc::S_IFREG
                            || opened.st_uid != effective_user_id()
                            || opened.st_dev as u64 != bindings.parent.device
                            || FileIdentity::from_stat(&opened) != bindings.target
                            || opened.st_mode as u32 & 0o7777 != bindings.original_mode
                            || opened.st_nlink != 1
                        {
                            return Err(os_error(libc::ESTALE));
                        }
                        unlink_at(operation.directory.as_raw_fd(), &bindings.placeholder, 0)?;
                        drop(target);
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
                        return Ok(TreeCleanupOutcome::Deleted);
                    }
                    _ => return Err(os_error(libc::ESTALE)),
                }
            }
            Some(CleanupDecisionV1::Restore) => {
                *restore_expected = true;
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
                        drop(open_bound_cleanup_regular(
                            namespace.directory.as_raw_fd(),
                            &bindings.quarantine,
                            bindings.target,
                            bindings.parent.device,
                            bindings.original_mode,
                        )?);
                        let operation = operation.as_mut().ok_or_else(|| os_error(libc::ESTALE))?;
                        capture_expected_entry(
                            operation,
                            namespace.directory.as_raw_fd(),
                            &bindings.quarantine,
                            &bindings.placeholder,
                            bindings.placeholder_identity,
                            bindings.target,
                            false,
                        )?;
                        injected_cleanup_restore_rollback_result()?;
                    }
                    (CleanupSlot::Missing, CleanupSlot::Placeholder, CleanupSlot::Target, true) => {
                        let operation = operation.as_mut().ok_or_else(|| os_error(libc::ESTALE))?;
                        let restore_target = open_bound_cleanup_regular(
                            operation.directory.as_raw_fd(),
                            &bindings.placeholder,
                            bindings.target,
                            bindings.parent.device,
                            bindings.original_mode,
                        )?;
                        rename_no_replace(
                            operation.directory.as_raw_fd(),
                            &bindings.placeholder,
                            public_parent,
                            &bindings.component,
                        )?;
                        drop(restore_target);
                        drop(open_bound_cleanup_regular(
                            public_parent,
                            &bindings.component,
                            bindings.target,
                            bindings.parent.device,
                            bindings.original_mode,
                        )?);
                        cvt(unsafe { libc::fsync(public_parent) })?;
                        injected_cleanup_restore_destination_sync_result()?;
                        cvt(unsafe { libc::fsync(operation.directory.as_raw_fd()) })?;
                        injected_cleanup_restore_source_sync_result()?;
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
                        let target = open_bound_cleanup_regular(
                            public_parent,
                            &bindings.component,
                            bindings.target,
                            bindings.parent.device,
                            bindings.original_mode,
                        )?;
                        cvt(unsafe { libc::fsync(target.as_raw_fd()) })?;
                        cvt(unsafe { libc::fsync(public_parent) })?;
                        injected_cleanup_after_restore_sync_result()?;
                        finish_cleanup_records(namespace, loaded, decision.as_ref())?;
                        return Ok(TreeCleanupOutcome::Restored(pending_restore_error.take()));
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
    if AtomicRenameCapability::current() != AtomicRenameCapability::NoReplace {
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
    if AtomicRenameCapability::current() != AtomicRenameCapability::NoReplace {
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
    #[cfg(test)]
    TEST_CLEANUP_BOOTSTRAP_HANDOFF.with(|handoff| -> io::Result<()> {
        let Some(mut handoff) = handoff.borrow_mut().take() else {
            return Ok(());
        };
        handoff.stream.write_all(b"H")?;
        let mut release = [0];
        handoff.stream.read_exact(&mut release)?;
        if release != *b"R" {
            return Err(os_error(libc::EIO));
        }
        Ok(())
    })?;
    Ok(())
}

fn injected_cleanup_probe_transition_result() -> io::Result<()> {
    #[cfg(test)]
    {
        let transition = TEST_CLEANUP_PROBE_TRANSITION.get().saturating_add(1);
        TEST_CLEANUP_PROBE_TRANSITION.set(transition);
        if let Some(CleanupFault::DuringCleanupCapabilityProbe(expected, errno)) =
            TEST_CLEANUP_FAULT.with(std::cell::Cell::get)
            && transition == expected
        {
            TEST_CLEANUP_FAULT.set(None);
            TEST_CLEANUP_PROBE_INTERRUPTED.set(true);
            return Err(os_error(errno));
        }
    }
    Ok(())
}

fn injected_cleanup_probe_final_sync_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::AfterCleanupCapabilityProbeSync(errno)) =
        TEST_CLEANUP_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn cleanup_probe_was_interrupted() -> bool {
    #[cfg(test)]
    {
        TEST_CLEANUP_PROBE_INTERRUPTED.replace(false)
    }
    #[cfg(not(test))]
    {
        false
    }
}

fn injected_cleanup_placeholder_object_sync_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::AfterCleanupPlaceholderObjectSync(errno)) =
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

fn injected_cleanup_intent_write_before_sync_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::AfterCleanupIntentWriteBeforeSync(errno)) =
        TEST_CLEANUP_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_intent_adoption_sync_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::AfterCleanupIntentAdoptionSync(errno)) =
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

fn injected_cleanup_intent_publish_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::AfterCleanupIntentPublish(errno)) =
        TEST_CLEANUP_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_before_bound_completion_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::BeforeBoundCompletion(errno)) =
        TEST_CLEANUP_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_intent_after_rename_handoff() {
    #[cfg(test)]
    TEST_CLEANUP_INTENT_HANDOFF.with(|handoff| {
        let handoff = handoff.borrow();
        if let Some(handoff) = handoff.as_ref() {
            let _ = handoff.renamed.send(());
            let _ = handoff.release.recv();
        }
    });
}

fn injected_cleanup_initial_terminal_handoff() {
    #[cfg(test)]
    injected_cleanup_terminal_handoff(CleanupTerminalPhase::Initial);
}

fn injected_cleanup_final_terminal_handoff() {
    #[cfg(test)]
    injected_cleanup_terminal_handoff(CleanupTerminalPhase::Final);
}

#[cfg(test)]
fn injected_cleanup_terminal_handoff(phase: CleanupTerminalPhase) {
    TEST_CLEANUP_TERMINAL_HANDOFF.with(|handoff| {
        let handoff = handoff.borrow();
        if let Some(handoff) = handoff.as_ref()
            && handoff.phase == phase
        {
            let _ = handoff.reached.send(());
            let _ = handoff.release.recv();
        }
    });
}

fn injected_cleanup_decision_stage_sync_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupDecisionFault::AfterStageSync(errno)) =
        TEST_CLEANUP_DECISION_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_DECISION_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_decision_write_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupDecisionFault::DuringWrite(errno)) =
        TEST_CLEANUP_DECISION_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_DECISION_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_decision_rewrite_handoff() {
    #[cfg(test)]
    TEST_CLEANUP_DECISION_REWRITE_HANDOFF.with(|handoff| {
        let handoff = handoff.borrow();
        if let Some(handoff) = handoff.as_ref() {
            let _ = handoff.reached.send(());
            let _ = handoff.release.recv();
        }
    });
}

fn injected_cleanup_intent_retire_before_sync_handoff() {
    #[cfg(test)]
    TEST_CLEANUP_INTENT_RETIRE_HANDOFF.with(|handoff| {
        let handoff = handoff.borrow();
        if let Some(handoff) = handoff.as_ref() {
            let _ = handoff.reached.send(());
            let _ = handoff.release.recv();
        }
    });
}

fn injected_cleanup_predicate_completion_handoff() {
    #[cfg(test)]
    TEST_CLEANUP_PREDICATE_COMPLETION_HANDOFF.with(|handoff| {
        let handoff = handoff.borrow();
        if let Some(handoff) = handoff.as_ref() {
            let _ = handoff.reached.send(());
            let _ = handoff.release.recv();
        }
    });
}

fn injected_cleanup_decision_write_before_sync_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupDecisionFault::AfterFullWriteBeforeSync(errno)) =
        TEST_CLEANUP_DECISION_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_DECISION_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_decision_adoption_sync_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupDecisionFault::AfterCompleteAdoptionSync(errno)) =
        TEST_CLEANUP_DECISION_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_DECISION_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_decision_before_rename_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupDecisionFault::BeforeRename(errno)) =
        TEST_CLEANUP_DECISION_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_DECISION_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_decision_after_rename_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupDecisionFault::AfterRename(errno)) =
        TEST_CLEANUP_DECISION_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_DECISION_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_decision_destination_sync_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupDecisionFault::AfterDestinationSync(errno)) =
        TEST_CLEANUP_DECISION_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_DECISION_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_decision_source_sync_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupDecisionFault::AfterSourceSync(errno)) =
        TEST_CLEANUP_DECISION_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_DECISION_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn record_cleanup_capture_destination_sync() {
    #[cfg(test)]
    TEST_CLEANUP_RESTORE_SYNC_TRACE.with(|trace| {
        if let Some(trace) = trace.borrow_mut().as_mut() {
            trace.push(CleanupRestoreSyncBoundary::CaptureDestination);
        }
    });
}

fn record_cleanup_capture_source_sync() {
    #[cfg(test)]
    TEST_CLEANUP_RESTORE_SYNC_TRACE.with(|trace| {
        if let Some(trace) = trace.borrow_mut().as_mut() {
            trace.push(CleanupRestoreSyncBoundary::CaptureSource);
        }
    });
}

fn injected_cleanup_restore_destination_sync_result() -> io::Result<()> {
    #[cfg(test)]
    {
        TEST_CLEANUP_RESTORE_SYNC_TRACE.with(|trace| {
            if let Some(trace) = trace.borrow_mut().as_mut() {
                trace.push(CleanupRestoreSyncBoundary::Destination);
            }
        });
        if let Some(CleanupDecisionFault::AfterRestoreDestinationSync(errno)) =
            TEST_CLEANUP_DECISION_FAULT.with(std::cell::Cell::get)
        {
            TEST_CLEANUP_DECISION_FAULT.set(None);
            return Err(os_error(errno));
        }
    }
    Ok(())
}

fn injected_cleanup_restore_source_sync_result() -> io::Result<()> {
    #[cfg(test)]
    {
        TEST_CLEANUP_RESTORE_SYNC_TRACE.with(|trace| {
            if let Some(trace) = trace.borrow_mut().as_mut() {
                trace.push(CleanupRestoreSyncBoundary::Source);
            }
        });
        if let Some(CleanupDecisionFault::AfterRestoreSourceSync(errno)) =
            TEST_CLEANUP_DECISION_FAULT.with(std::cell::Cell::get)
        {
            TEST_CLEANUP_DECISION_FAULT.set(None);
            return Err(os_error(errno));
        }
    }
    Ok(())
}

fn injected_cleanup_restore_rollback_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupDecisionFault::DuringRestoreRollback(errno)) =
        TEST_CLEANUP_DECISION_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_DECISION_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_after_restore_sync_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupDecisionFault::AfterRestoreSync(errno)) =
        TEST_CLEANUP_DECISION_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_DECISION_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_after_decision_retire_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupDecisionFault::AfterDecisionRetire(errno)) =
        TEST_CLEANUP_DECISION_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_DECISION_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_after_intent_retire_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupDecisionFault::AfterIntentRetire(errno)) =
        TEST_CLEANUP_DECISION_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_DECISION_FAULT.set(None);
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

fn injected_cleanup_same_call_transition() {
    #[cfg(test)]
    if let Some(hook) = TEST_CLEANUP_SAME_CALL_TRANSITION.replace(None) {
        hook();
    }
}

fn injected_regular_parent_admission_hook() {
    #[cfg(test)]
    if let Some(hook) = TEST_REGULAR_PARENT_ADMISSION_HOOK.replace(None) {
        hook();
    }
}

fn injected_cleanup_after_target_exchange_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::AfterCleanupTargetExchange(errno)) =
        TEST_CLEANUP_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_after_quarantine_rename_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::AfterCleanupQuarantineRename(errno)) =
        TEST_CLEANUP_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_after_quarantine_namespace_sync_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::AfterCleanupQuarantineNamespaceSync(errno)) =
        TEST_CLEANUP_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_after_quarantine_parent_sync_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::AfterCleanupQuarantineParentSync(errno)) =
        TEST_CLEANUP_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_after_placeholder_removal_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::AfterCleanupPlaceholderRemoval(errno)) =
        TEST_CLEANUP_FAULT.with(std::cell::Cell::get)
    {
        TEST_CLEANUP_FAULT.set(None);
        return Err(os_error(errno));
    }
    Ok(())
}

fn injected_cleanup_after_operation_removal_result() -> io::Result<()> {
    #[cfg(test)]
    if let Some(CleanupFault::AfterCleanupOperationRemoval(errno)) =
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
        io::{Read, Write},
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::ffi::OsStrExt,
            unix::fs::{MetadataExt, PermissionsExt, symlink},
        },
        path::{Path, PathBuf},
    };

    use super::{
        AtomicRenameCapability, AtomicRenameCapabilityOverride, CleanupDecisionFault,
        CleanupDecisionFaultOverride, CleanupDecisionRecordV1, CleanupDecisionV1, CleanupFault,
        CleanupFaultOverride, CleanupIntentV1, CleanupTerminalPhase,
        CopyPrivateCleanupFailureOverride, PrivateNamespace, RenameNoReplaceOverride, RootedDir,
        cleanup_canonical_json, cleanup_parse_canonical_json, copy_regular,
        copy_regular_with_clone, copy_regular_with_clone_and_publish, create_regular_at, link_at,
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

    const REGULAR_RETRY_ENTRY_BYTES: &[u8] = b"regular-owned\n";
    const REGULAR_RETRY_SIBLING_BYTES: &[u8] = b"unrelated-sibling\n";
    const REGULAR_RETRY_SUBSTITUTE_BYTES: &[u8] = b"unrelated-substitute\n";

    struct RegularCleanupRetryFixture {
        _temp: tempfile::TempDir,
        root: PathBuf,
        entry_device: u64,
        entry_inode: u64,
    }

    impl RegularCleanupRetryFixture {
        fn create() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let physical = temp.path().canonicalize().unwrap();
            fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
            let root = physical.join("root");
            fs::create_dir(&root).unwrap();
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            fs::write(root.join("entry"), REGULAR_RETRY_ENTRY_BYTES).unwrap();
            fs::set_permissions(root.join("entry"), fs::Permissions::from_mode(0o600)).unwrap();
            fs::write(root.join("sibling"), REGULAR_RETRY_SIBLING_BYTES).unwrap();
            fs::set_permissions(root.join("sibling"), fs::Permissions::from_mode(0o600)).unwrap();
            let entry = fs::symlink_metadata(root.join("entry")).unwrap();
            Self {
                _temp: temp,
                root,
                entry_device: entry.dev(),
                entry_inode: entry.ino(),
            }
        }

        fn interrupt_after_public_entry_vanishes(
            &self,
            fault: CleanupFault,
        ) -> CleanupFaultOverride {
            let parent = RootedDir::open(&self.root).unwrap();
            let fault = CleanupFaultOverride::set(fault);
            let error = parent.remove_owned_regular("entry").unwrap_err();
            assert_eq!(error.raw_os_error(), Some(libc::EIO));
            drop(parent);
            assert!(!self.root.join("entry").exists());
            assert_eq!(
                fs::read(self.root.join("sibling")).unwrap(),
                REGULAR_RETRY_SIBLING_BYTES
            );
            fault
        }

        fn namespace_roles(&self) -> Vec<(Vec<u8>, fs::FileType)> {
            fs::read_dir(self.root.join(".mac-worker-rooted-fs"))
                .unwrap()
                .map(|entry| {
                    let entry = entry.unwrap();
                    let file_type = fs::symlink_metadata(entry.path()).unwrap().file_type();
                    (entry.file_name().as_bytes().to_vec(), file_type)
                })
                .collect()
        }

        fn unique_namespace_role(&self, prefix: &[u8]) -> PathBuf {
            let matches = fs::read_dir(self.root.join(".mac-worker-rooted-fs"))
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| {
                    path.file_name()
                        .is_some_and(|name| name.as_bytes().starts_with(prefix))
                })
                .collect::<Vec<_>>();
            assert_eq!(matches.len(), 1);
            matches.into_iter().next().unwrap()
        }

        fn retry_original_name_and_require_absent(self, fault: CleanupFaultOverride) {
            drop(fault);
            let parent = RootedDir::open(&self.root).unwrap();
            parent.remove_owned_regular("entry").unwrap();
            drop(parent);
            assert!(!self.root.join("entry").exists());
            assert_eq!(
                fs::read(self.root.join("sibling")).unwrap(),
                REGULAR_RETRY_SIBLING_BYTES
            );
            let residue = fs::read_dir(self.root.join(".mac-worker-rooted-fs"))
                .unwrap()
                .map(|entry| entry.unwrap().file_name().as_bytes().to_vec())
                .collect::<Vec<_>>();
            assert!(
                !residue
                    .iter()
                    .any(|name| name.starts_with(b"cleanup-intent-v1-"))
            );
            assert!(
                !residue
                    .iter()
                    .any(|name| name.starts_with(b"cleanup-decision-v1-"))
            );
            assert!(
                !residue
                    .iter()
                    .any(|name| name.starts_with(b"cleanup-op-v1-"))
            );
            assert!(
                !residue
                    .iter()
                    .any(|name| name.starts_with(b"cleanup-regular-v1-"))
            );
            assert!(!residue.iter().any(|name| name == b"cleanup-placeholder-v1"));
            assert_eq!(residue.len(), 0);
        }

        fn assert_namespace_empty_or_absent(&self) {
            let namespace = self.root.join(".mac-worker-rooted-fs");
            if namespace.exists() {
                assert_eq!(fs::read_dir(namespace).unwrap().count(), 0);
            }
        }

        fn assert_sibling_preserved(&self) {
            assert_eq!(
                fs::read(self.root.join("sibling")).unwrap(),
                REGULAR_RETRY_SIBLING_BYTES
            );
        }

        fn assert_regular_retry_intent_bootstrap_binding(&self) {
            let intent_path = self.unique_namespace_role(b"cleanup-intent-v1-");
            let intent: CleanupIntentV1 =
                cleanup_parse_canonical_json(&fs::read(&intent_path).unwrap()).unwrap();
            assert!(matches!(intent.kind, super::CleanupTargetKind::Regular));
            let operation_name = std::ffi::CString::new(intent.operation.as_bytes()).unwrap();
            let bootstrap = super::parse_cleanup_bootstrap_name(&operation_name)
                .unwrap()
                .expect("bound regular retry intent names a cleanup bootstrap");
            assert!(matches!(bootstrap.kind, super::CleanupTargetKind::Regular));
            assert_eq!(bootstrap.key, intent.key_sha256);
            assert_eq!(bootstrap.target, super::FileIdentity::from(intent.target));
            assert_eq!(bootstrap.original_mode, intent.original_mode);
            assert_eq!(
                self.unique_namespace_role(b"cleanup-op-v1-")
                    .file_name()
                    .unwrap()
                    .as_bytes(),
                intent.operation.as_bytes()
            );
            assert!(!fs::read_dir(&self.root).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .as_bytes()
                    .starts_with(b"remove-")
            }));
        }

        fn retry_remove_owned_regular(&self) -> std::io::Error {
            RootedDir::open(&self.root)
                .unwrap()
                .remove_owned_regular("entry")
                .unwrap_err()
        }

        fn snapshot_unique_role(&self, prefix: &[u8]) -> (PathBuf, u64) {
            let path = self.unique_namespace_role(prefix);
            let inode = fs::symlink_metadata(&path).unwrap().ino();
            (path, inode)
        }

        fn assert_public_entry_unchanged(&self) {
            assert_no_follow_regular(
                &self.root.join("entry"),
                self.entry_device,
                self.entry_inode,
                0o600,
                REGULAR_RETRY_ENTRY_BYTES,
            );
            self.assert_sibling_preserved();
        }

        fn assert_public_entry_absent(&self) {
            assert!(!self.root.join("entry").exists());
            self.assert_sibling_preserved();
            assert!(!fs::read_dir(&self.root).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .as_bytes()
                    .starts_with(b"remove-")
            }));
        }
    }

    fn assert_no_follow_regular(path: &Path, device: u64, inode: u64, mode: u32, bytes: &[u8]) {
        let meta = fs::symlink_metadata(path).unwrap();
        assert!(meta.file_type().is_file());
        assert_eq!(meta.dev(), device);
        assert_eq!(meta.ino(), inode);
        assert_eq!(meta.permissions().mode() & 0o777, mode);
        assert_eq!(meta.nlink(), 1);
        assert_eq!(fs::read(path).unwrap(), bytes);
    }

    fn plant_regular_substitute(path: &Path) -> u64 {
        fs::write(path, REGULAR_RETRY_SUBSTITUTE_BYTES).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::symlink_metadata(path).unwrap().ino()
    }

    fn after_delete_hook_eio() -> std::io::Result<()> {
        Err(std::io::Error::from_raw_os_error(libc::EIO))
    }

    fn unique_namespace_role(namespace: &Path, prefix: &[u8]) -> PathBuf {
        let matches = fs::read_dir(namespace)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .is_some_and(|name| name.as_bytes().starts_with(prefix))
            })
            .collect::<Vec<_>>();
        assert_eq!(matches.len(), 1, "{prefix:?} roles in {namespace:?}");
        matches.into_iter().next().unwrap()
    }

    fn assert_canonical_delete_residue(namespace: &Path, quarantine_prefix: &[u8]) {
        let intent = unique_namespace_role(namespace, b"cleanup-intent-v1-");
        assert!(fs::symlink_metadata(&intent).unwrap().file_type().is_file());
        let decision = unique_namespace_role(namespace, b"cleanup-decision-v1-");
        let record: CleanupDecisionRecordV1 =
            serde_json::from_slice(&fs::read(&decision).unwrap()).unwrap();
        assert!(matches!(record.decision, CleanupDecisionV1::Delete));
        assert!(unique_namespace_role(namespace, b"cleanup-op-v1-").is_dir());
        assert!(unique_namespace_role(namespace, quarantine_prefix).exists());
    }

    #[test]
    fn cleanup_predicate_identity_preserves_full_mode_mask() {
        // Catches PrivateEntryIdentity::from_stat dropping setuid/setgid/sticky
        // bits by masking with 0o777 instead of the full 0o7777 mode. A
        // synthetic stat is required: APFS may strip setuid/setgid on chmod.
        let mut metadata: libc::stat = unsafe { std::mem::zeroed() };
        metadata.st_dev = 7;
        metadata.st_ino = 11;
        metadata.st_uid = 42;
        metadata.st_mode = libc::S_IFREG | 0o7600;
        let identity = super::PrivateEntryIdentity::from_stat(&metadata);

        assert_eq!(identity.kind, libc::S_IFREG as u32);
        assert_eq!(identity.owner, 42);
        assert_eq!(identity.mode, 0o7600);
        assert_eq!(identity.device, 7);
        assert_eq!(identity.inode, 11);
    }

    #[test]
    fn cleanup_intent_regular_retry_resume_only_leaves_ordinary_public_untouched() {
        let fixture = RegularCleanupRetryFixture::create();
        let parent = RootedDir::open(&fixture.root).unwrap();

        assert!(
            !parent
                .resume_pending_owned_regular_cleanup("entry")
                .unwrap()
        );

        fixture.assert_public_entry_unchanged();
        fixture.assert_namespace_empty_or_absent();
    }

    #[test]
    fn cleanup_intent_regular_retry_resume_only_resumes_canonical_pending() {
        let fixture = RegularCleanupRetryFixture::create();
        let fault = fixture.interrupt_after_public_entry_vanishes(
            CleanupFault::AfterCleanupQuarantineRename(libc::EIO),
        );
        drop(fault);
        fixture.assert_regular_retry_intent_bootstrap_binding();

        let parent = RootedDir::open(&fixture.root).unwrap();
        assert!(
            parent
                .resume_pending_owned_regular_cleanup("entry")
                .unwrap()
        );
        drop(parent);

        fixture.assert_public_entry_absent();
        fixture.assert_namespace_empty_or_absent();
    }

    #[test]
    fn cleanup_predicate_regular_accepts_matching_generated_name() {
        const MATCHED: &str = ".replace-11111111-1111-4111-8111-111111111111";
        const REJECTED: &str = ".replace-22222222-2222-4222-8222-222222222222";
        const SPECIAL_MODE: u32 = 0o1600;
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root = physical.join("root");
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let mut identities = Vec::new();
        for (name, bytes) in [
            (MATCHED, b"matched-generated\n".as_slice()),
            (REJECTED, b"rejected-generated\n".as_slice()),
        ] {
            let path = root.join(name);
            fs::write(&path, bytes).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(SPECIAL_MODE)).unwrap();
            let metadata = fs::symlink_metadata(&path).unwrap();
            assert_eq!(metadata.permissions().mode() & 0o7777, SPECIAL_MODE);
            identities.push((metadata.dev(), metadata.ino(), bytes));
        }
        let parent = RootedDir::open(&root).unwrap();
        for name in [MATCHED, REJECTED] {
            let fault =
                CleanupFaultOverride::set(CleanupFault::AfterCleanupQuarantineRename(libc::EIO));
            assert_eq!(
                parent
                    .remove_owned_regular(name)
                    .unwrap_err()
                    .raw_os_error(),
                Some(libc::EIO)
            );
            drop(fault);
            assert!(!root.join(name).exists());
        }

        let mut observed = Vec::new();
        let resumed = parent
            .retry_pending_owned_regulars_matching(|component, identity| {
                observed.push((component.to_vec(), identity));
                component == MATCHED.as_bytes()
            })
            .unwrap();

        assert_eq!(resumed, 1);
        assert_eq!(observed.len(), 2);
        for (component, identity) in &observed {
            let (expected_device, expected_inode, _) = if component.as_slice() == MATCHED.as_bytes()
            {
                identities[0]
            } else {
                assert_eq!(component.as_slice(), REJECTED.as_bytes());
                identities[1]
            };
            assert_eq!(identity.device, expected_device);
            assert_eq!(identity.inode, expected_inode);
            assert_eq!(identity.kind, libc::S_IFREG as u32);
            assert_eq!(identity.owner, unsafe { libc::geteuid() });
            assert_eq!(identity.mode, SPECIAL_MODE);
        }
        assert!(!root.join(MATCHED).exists());
        assert!(!root.join(REJECTED).exists());
        assert!(
            !parent
                .resume_pending_owned_regular_cleanup(MATCHED)
                .unwrap()
        );
        let rejected_quarantine =
            unique_namespace_role(&root.join(".mac-worker-rooted-fs"), b"cleanup-regular-v1-");
        let rejected_meta = fs::symlink_metadata(&rejected_quarantine).unwrap();
        assert_eq!(rejected_meta.dev(), identities[1].0);
        assert_eq!(rejected_meta.ino(), identities[1].1);
        assert_eq!(rejected_meta.permissions().mode() & 0o7777, SPECIAL_MODE);
        assert_eq!(fs::read(&rejected_quarantine).unwrap(), identities[1].2);
        assert!(
            parent
                .resume_pending_owned_regular_cleanup(REJECTED)
                .unwrap()
        );
        assert_eq!(
            fs::read_dir(root.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn cleanup_intent_tree_retry_after_delete_hook_leaves_canonical_residue() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(root_path.join("first"), b"first\n").unwrap();
        fs::write(root_path.join("second"), b"second\n").unwrap();
        let parent = RootedDir::open(&physical).unwrap();

        let error = parent
            .remove_owned_child_with_cleanup_hook("root", &after_delete_hook_eio)
            .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EIO), "{error:?}");
        drop(parent);

        assert!(!root_path.exists());
        let namespace = physical.join(".mac-worker-rooted-fs");
        assert_canonical_delete_residue(&namespace, b"cleanup-tree-v1-");
        let quarantine = unique_namespace_role(&namespace, b"cleanup-tree-v1-");
        assert!(fs::read_dir(&quarantine).unwrap().count() >= 1);

        let parent = RootedDir::open(&physical).unwrap();
        parent.remove_owned_child("root").unwrap();
        assert!(!root_path.exists());
        assert_eq!(fs::read_dir(namespace).unwrap().count(), 0);
    }

    #[test]
    fn cleanup_intent_regular_retry_after_delete_hook_leaves_canonical_residue() {
        let fixture = RegularCleanupRetryFixture::create();
        let parent = RootedDir::open(&fixture.root).unwrap();

        let error = parent
            .remove_owned_regular_with_cleanup_hook("entry", &after_delete_hook_eio)
            .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EIO), "{error:?}");
        drop(parent);

        fixture.assert_public_entry_absent();
        let namespace = fixture.root.join(".mac-worker-rooted-fs");
        assert_canonical_delete_residue(&namespace, b"cleanup-regular-v1-");
        let quarantine = unique_namespace_role(&namespace, b"cleanup-regular-v1-");
        let quarantined = fs::symlink_metadata(&quarantine).unwrap();
        assert!(quarantined.file_type().is_file());
        assert_eq!(quarantined.dev(), fixture.entry_device);
        assert_eq!(quarantined.ino(), fixture.entry_inode);
        assert_eq!(fs::read(&quarantine).unwrap(), REGULAR_RETRY_ENTRY_BYTES);

        let parent = RootedDir::open(&fixture.root).unwrap();
        parent.remove_owned_regular("entry").unwrap();
        drop(parent);
        fixture.assert_public_entry_absent();
        fixture.assert_namespace_empty_or_absent();
    }

    #[test]
    fn cleanup_intent_tree_retry_root_after_delete_hook_leaves_canonical_residue() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(root_path.join("value"), b"owned bytes\n").unwrap();
        let root = RootedDir::open(&root_path).unwrap();

        let error = root
            .remove_owned_tree_with_cleanup_hook(&after_delete_hook_eio)
            .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EIO), "{error:?}");
        drop(root);

        assert!(!root_path.exists());
        let namespace = physical.join(".mac-worker-rooted-fs");
        assert_canonical_delete_residue(&namespace, b"cleanup-tree-v1-");
        let quarantine = unique_namespace_role(&namespace, b"cleanup-tree-v1-");
        assert!(fs::read_dir(&quarantine).unwrap().count() >= 1);

        let root = RootedDir::open(&physical).unwrap();
        root.remove_owned_child("root").unwrap();
        assert!(!root_path.exists());
        assert_eq!(fs::read_dir(namespace).unwrap().count(), 0);
    }

    #[test]
    fn cleanup_intent_regular_retry_after_quarantine_rename() {
        // Catches treating a vanished public regular as success while the
        // bound quarantine and unpublished Delete still remain journaled.
        let fixture = RegularCleanupRetryFixture::create();
        let fault = fixture.interrupt_after_public_entry_vanishes(
            CleanupFault::AfterCleanupQuarantineRename(libc::EIO),
        );

        let roles = fixture.namespace_roles();
        assert!(
            roles
                .iter()
                .any(|(name, kind)| { name.starts_with(b"cleanup-intent-v1-") && kind.is_file() })
        );
        assert!(
            !roles
                .iter()
                .any(|(name, _)| name.starts_with(b"cleanup-decision-v1-"))
        );
        assert!(roles.iter().any(|(name, kind)| {
            name.starts_with(b"cleanup-op-v1-")
                && kind.is_dir()
                && name.windows(4).any(|window| window == b"-k02")
        }));

        let intent = fixture.unique_namespace_role(b"cleanup-intent-v1-");
        let intent_meta = fs::symlink_metadata(&intent).unwrap();
        assert!(intent_meta.file_type().is_file());
        assert_eq!(intent_meta.permissions().mode() & 0o777, 0o600);
        fixture.assert_regular_retry_intent_bootstrap_binding();

        let quarantine = fixture.unique_namespace_role(b"cleanup-regular-v1-");
        let quarantined = fs::symlink_metadata(&quarantine).unwrap();
        assert!(quarantined.file_type().is_file());
        assert_eq!(quarantined.dev(), fixture.entry_device);
        assert_eq!(quarantined.ino(), fixture.entry_inode);
        assert_eq!(quarantined.permissions().mode() & 0o777, 0o600);
        assert_eq!(quarantined.nlink(), 1);
        assert_eq!(fs::read(&quarantine).unwrap(), REGULAR_RETRY_ENTRY_BYTES);

        let placeholder = fs::symlink_metadata(
            fixture
                .unique_namespace_role(b"cleanup-op-v1-")
                .join("cleanup-placeholder-v1"),
        )
        .unwrap();
        assert!(placeholder.file_type().is_dir());
        assert_eq!(placeholder.permissions().mode() & 0o777, 0o700);

        fixture.retry_original_name_and_require_absent(fault);
    }

    thread_local! {
        static REGULAR_PARENT_MODE_RACE_ROOT: std::cell::RefCell<Option<PathBuf>> = const {
            std::cell::RefCell::new(None)
        };
    }

    fn chmod_regular_parent_to_group_writable() {
        let root = REGULAR_PARENT_MODE_RACE_ROOT
            .with(|cell| cell.borrow().clone())
            .expect("regular parent mode-race root");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o720)).unwrap();
        assert_eq!(
            fs::symlink_metadata(&root).unwrap().permissions().mode() & 0o777,
            0o720
        );
    }

    fn namespace_roles_at(dir: &Path) -> Vec<(Vec<u8>, fs::FileType)> {
        let namespace = dir.join(".mac-worker-rooted-fs");
        if !namespace.exists() {
            return Vec::new();
        }
        fs::read_dir(namespace)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let file_type = fs::symlink_metadata(entry.path()).unwrap().file_type();
                (entry.file_name().as_bytes().to_vec(), file_type)
            })
            .collect()
    }

    fn namespace_has_regular_k02_journal(dir: &Path) -> bool {
        let roles = namespace_roles_at(dir);
        roles
            .iter()
            .any(|(name, kind)| name.starts_with(b"cleanup-intent-v1-") && kind.is_file())
            && roles.iter().any(|(name, kind)| {
                name.starts_with(b"cleanup-op-v1-")
                    && kind.is_dir()
                    && name.windows(4).any(|window| window == b"-k02")
            })
    }

    fn assert_no_cleanup_residue(dir: &Path) {
        let namespace = dir.join(".mac-worker-rooted-fs");
        if namespace.exists() {
            assert_eq!(fs::read_dir(&namespace).unwrap().count(), 0);
        }
    }

    #[test]
    fn cleanup_intent_regular_retry_regular_cleanup_parent_mode_race_does_not_journal_at_ancestor()
    {
        // Catches walking to an ancestor after the exact regular parent was
        // already admitted as owner-only. Current code can publish a k02
        // intent/quarantine there; after crash and 0700 restoration, retry
        // creates an empty local namespace and absence-ok returns Ok.
        let fixture = RegularCleanupRetryFixture::create();
        let ancestor = fixture.root.parent().unwrap().to_path_buf();
        assert_eq!(
            fs::symlink_metadata(&ancestor)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::symlink_metadata(&fixture.root)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        REGULAR_PARENT_MODE_RACE_ROOT.with(|cell| {
            *cell.borrow_mut() = Some(fixture.root.clone());
        });
        let _hook =
            super::RegularParentAdmissionHookOverride::set(chmod_regular_parent_to_group_writable);
        let fault =
            CleanupFaultOverride::set(CleanupFault::AfterCleanupQuarantineRename(libc::EIO));

        let first = RootedDir::open(&fixture.root)
            .unwrap()
            .remove_owned_regular("entry");

        assert_eq!(first.unwrap_err().raw_os_error(), Some(libc::ENOTSUP));
        assert!(
            fixture.root.join("entry").exists(),
            "public regular must stay put before a local owner-only namespace exists"
        );
        assert_eq!(
            fs::read(fixture.root.join("entry")).unwrap(),
            REGULAR_RETRY_ENTRY_BYTES
        );
        fixture.assert_sibling_preserved();
        assert!(
            !namespace_has_regular_k02_journal(&ancestor),
            "regular cleanup must not journal k02 evidence at an ancestor"
        );
        assert!(
            !namespace_has_regular_k02_journal(&fixture.root),
            "regular cleanup must not publish after the exact parent became group-writable"
        );
        assert_no_cleanup_residue(&ancestor);
        assert_no_cleanup_residue(&fixture.root);

        drop(fault);
        drop(_hook);
        fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o700)).unwrap();
        RootedDir::open(&fixture.root)
            .unwrap()
            .remove_owned_regular("entry")
            .unwrap();
        assert!(!fixture.root.join("entry").exists());
        fixture.assert_sibling_preserved();
        assert_no_cleanup_residue(&ancestor);
        assert_no_cleanup_residue(&fixture.root);
        REGULAR_PARENT_MODE_RACE_ROOT.with(|cell| {
            *cell.borrow_mut() = None;
        });
    }

    #[test]
    fn private_namespace_select_and_tree_cleanup_still_walk_to_ancestor() {
        // Ordinary probe=true and cleanup-with-fallback must still create the
        // private namespace at an owner-only ancestor when the start parent
        // is group-writable.
        let fixture = tempfile::tempdir().unwrap();
        let ancestor = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&ancestor, fs::Permissions::from_mode(0o700)).unwrap();
        let parent = ancestor.join("parent");
        fs::create_dir(&parent).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o720)).unwrap();
        let tree = parent.join("tree");
        fs::create_dir(&tree).unwrap();
        fs::set_permissions(&tree, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(tree.join("value"), b"owned").unwrap();

        let parent_fd = open_directory_path(&parent).unwrap();
        let parent_metadata = super::stat_fd(parent_fd.as_raw_fd()).unwrap();
        let probed =
            PrivateNamespace::select(parent_fd.as_raw_fd(), parent_metadata.st_dev, &[]).unwrap();
        drop(probed);
        assert!(ancestor.join(".mac-worker-rooted-fs").is_dir());
        assert!(!parent.join(".mac-worker-rooted-fs").exists());
        fs::remove_dir(ancestor.join(".mac-worker-rooted-fs")).unwrap();

        let cleanup = PrivateNamespace::select_for_cleanup(
            parent_fd.as_raw_fd(),
            parent_metadata.st_dev,
            &[],
        )
        .unwrap();
        drop(cleanup);
        assert!(ancestor.join(".mac-worker-rooted-fs").is_dir());
        assert!(!parent.join(".mac-worker-rooted-fs").exists());
        fs::remove_dir(ancestor.join(".mac-worker-rooted-fs")).unwrap();

        RootedDir::open(&tree).unwrap().remove_owned_tree().unwrap();
        assert!(!tree.exists());
        assert!(ancestor.join(".mac-worker-rooted-fs").is_dir());
        assert!(!parent.join(".mac-worker-rooted-fs").exists());
        assert_eq!(
            fs::read_dir(ancestor.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn cleanup_intent_regular_retry_after_capture() {
        // Catches retry resolving only the absent public name after the
        // identity-bound regular has already been captured into the operation.
        let fixture = RegularCleanupRetryFixture::create();
        let fault = fixture.interrupt_after_public_entry_vanishes(
            CleanupFault::AfterCleanupTargetExchange(libc::EIO),
        );

        let roles = fixture.namespace_roles();
        assert!(
            roles
                .iter()
                .any(|(name, kind)| { name.starts_with(b"cleanup-intent-v1-") && kind.is_file() })
        );
        assert!(
            roles.iter().any(|(name, kind)| {
                name.starts_with(b"cleanup-decision-v1-") && kind.is_file()
            })
        );
        assert!(roles.iter().any(|(name, kind)| {
            name.starts_with(b"cleanup-op-v1-")
                && kind.is_dir()
                && name.windows(4).any(|window| window == b"-k02")
        }));

        let intent = fixture.unique_namespace_role(b"cleanup-intent-v1-");
        assert_eq!(
            fs::symlink_metadata(&intent).unwrap().permissions().mode() & 0o777,
            0o600
        );
        fixture.assert_regular_retry_intent_bootstrap_binding();

        let decision = fixture.unique_namespace_role(b"cleanup-decision-v1-");
        let record: CleanupDecisionRecordV1 =
            serde_json::from_slice(&fs::read(&decision).unwrap()).unwrap();
        assert!(matches!(record.decision, CleanupDecisionV1::Delete));
        assert_eq!(
            fs::symlink_metadata(&decision)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );

        let quarantine = fixture.unique_namespace_role(b"cleanup-regular-v1-");
        let exchanged = fs::symlink_metadata(&quarantine).unwrap();
        assert!(exchanged.file_type().is_dir());
        assert_eq!(exchanged.permissions().mode() & 0o777, 0o700);

        let captured = fixture
            .unique_namespace_role(b"cleanup-op-v1-")
            .join("cleanup-placeholder-v1");
        let captured_meta = fs::symlink_metadata(&captured).unwrap();
        assert!(captured_meta.file_type().is_file());
        assert_eq!(captured_meta.dev(), fixture.entry_device);
        assert_eq!(captured_meta.ino(), fixture.entry_inode);
        assert_eq!(captured_meta.permissions().mode() & 0o777, 0o600);
        assert_eq!(captured_meta.nlink(), 1);
        assert_eq!(fs::read(&captured).unwrap(), REGULAR_RETRY_ENTRY_BYTES);

        fixture.retry_original_name_and_require_absent(fault);
    }

    #[test]
    fn cleanup_intent_regular_retry_before_final_unlink() {
        // Catches treating public absence as completion while the captured
        // regular and its Delete decision are still waiting for unlink.
        let fixture = RegularCleanupRetryFixture::create();
        let fault = fixture
            .interrupt_after_public_entry_vanishes(CleanupFault::BeforeFinalRootRemoval(libc::EIO));

        let roles = fixture.namespace_roles();
        assert!(
            roles
                .iter()
                .any(|(name, kind)| { name.starts_with(b"cleanup-intent-v1-") && kind.is_file() })
        );
        assert!(
            roles.iter().any(|(name, kind)| {
                name.starts_with(b"cleanup-decision-v1-") && kind.is_file()
            })
        );
        assert!(roles.iter().any(|(name, kind)| {
            name.starts_with(b"cleanup-op-v1-")
                && kind.is_dir()
                && name.windows(4).any(|window| window == b"-k02")
        }));

        fixture.assert_regular_retry_intent_bootstrap_binding();

        let decision = fixture.unique_namespace_role(b"cleanup-decision-v1-");
        let record: CleanupDecisionRecordV1 =
            serde_json::from_slice(&fs::read(&decision).unwrap()).unwrap();
        assert!(matches!(record.decision, CleanupDecisionV1::Delete));

        let captured = fixture
            .unique_namespace_role(b"cleanup-op-v1-")
            .join("cleanup-placeholder-v1");
        let captured_meta = fs::symlink_metadata(&captured).unwrap();
        assert!(captured_meta.file_type().is_file());
        assert_eq!(captured_meta.dev(), fixture.entry_device);
        assert_eq!(captured_meta.ino(), fixture.entry_inode);
        assert_eq!(captured_meta.permissions().mode() & 0o777, 0o600);
        assert_eq!(captured_meta.nlink(), 1);
        assert_eq!(fs::read(&captured).unwrap(), REGULAR_RETRY_ENTRY_BYTES);

        fixture.retry_original_name_and_require_absent(fault);
    }

    thread_local! {
        static SAME_CALL_FINAL_UNLINK_ROOT: std::cell::RefCell<Option<PathBuf>> = const {
            std::cell::RefCell::new(None)
        };
        static SAME_CALL_FINAL_UNLINK_REPLACEMENT_INODE: std::cell::Cell<u64> = const {
            std::cell::Cell::new(0)
        };
    }

    const SAME_CALL_FINAL_UNLINK_SUBSTITUTE_BYTES: &[u8] = b"unrelated-substitute\n";

    fn substitute_same_call_final_unlink_regular() {
        let root = SAME_CALL_FINAL_UNLINK_ROOT
            .with(|cell| cell.borrow().clone())
            .expect("same-call final unlink root");
        let captured = fs::read_dir(root.join(".mac-worker-rooted-fs"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .is_some_and(|name| name.as_bytes().starts_with(b"cleanup-op-v1-"))
            })
            .unwrap()
            .join("cleanup-placeholder-v1");
        let evidence = root.parent().unwrap().join("owned-final-unlink-evidence");
        fs::rename(&captured, &evidence).unwrap();
        fs::write(&captured, SAME_CALL_FINAL_UNLINK_SUBSTITUTE_BYTES).unwrap();
        fs::set_permissions(&captured, fs::Permissions::from_mode(0o600)).unwrap();
        SAME_CALL_FINAL_UNLINK_REPLACEMENT_INODE
            .set(fs::symlink_metadata(&captured).unwrap().ino());
    }

    #[test]
    fn cleanup_intent_regular_retry_same_call_final_unlink_substitution_fails_closed() {
        // Catches unlinking the public/operation name after the first bind:
        // a same-call substitute at that seam must fail closed, not be deleted.
        let fixture = RegularCleanupRetryFixture::create();
        SAME_CALL_FINAL_UNLINK_ROOT.with(|cell| {
            *cell.borrow_mut() = Some(fixture.root.clone());
        });
        SAME_CALL_FINAL_UNLINK_REPLACEMENT_INODE.set(0);
        let _hook = super::CleanupSameCallTransitionOverride::set(
            substitute_same_call_final_unlink_regular,
        );
        let parent = RootedDir::open(&fixture.root).unwrap();

        let error = parent.remove_owned_regular("entry").unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        let captured = fixture
            .unique_namespace_role(b"cleanup-op-v1-")
            .join("cleanup-placeholder-v1");
        let captured_meta = fs::symlink_metadata(&captured).unwrap();
        assert_eq!(
            fs::read(&captured).unwrap(),
            SAME_CALL_FINAL_UNLINK_SUBSTITUTE_BYTES
        );
        assert_eq!(
            captured_meta.ino(),
            SAME_CALL_FINAL_UNLINK_REPLACEMENT_INODE.get()
        );
        let evidence = fixture
            .root
            .parent()
            .unwrap()
            .join("owned-final-unlink-evidence");
        let evidence_meta = fs::symlink_metadata(&evidence).unwrap();
        assert_eq!(fs::read(&evidence).unwrap(), REGULAR_RETRY_ENTRY_BYTES);
        assert_eq!(evidence_meta.ino(), fixture.entry_inode);
        assert_eq!(evidence_meta.dev(), fixture.entry_device);
        let roles = fixture.namespace_roles();
        assert!(
            roles
                .iter()
                .any(|(name, kind)| name.starts_with(b"cleanup-intent-v1-") && kind.is_file())
        );
        assert!(
            roles
                .iter()
                .any(|(name, kind)| name.starts_with(b"cleanup-decision-v1-") && kind.is_file())
        );
        assert!(
            roles
                .iter()
                .any(|(name, kind)| name.starts_with(b"cleanup-op-v1-") && kind.is_dir())
        );
        assert_eq!(
            fs::read(fixture.root.join("sibling")).unwrap(),
            REGULAR_RETRY_SIBLING_BYTES
        );
    }

    #[test]
    fn cleanup_intent_regular_retry_preserves_public_replacement() {
        // Catches retry treating a new public regular as the original target.
        const REPLACEMENT_BYTES: &[u8] = b"unrelated-public-replacement\n";
        let fixture = RegularCleanupRetryFixture::create();
        let fault = fixture.interrupt_after_public_entry_vanishes(
            CleanupFault::AfterCleanupQuarantineRename(libc::EIO),
        );
        fs::write(fixture.root.join("entry"), REPLACEMENT_BYTES).unwrap();
        fs::set_permissions(
            fixture.root.join("entry"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let replacement = fs::symlink_metadata(fixture.root.join("entry")).unwrap();
        let quarantine = fixture.unique_namespace_role(b"cleanup-regular-v1-");
        drop(fault);

        let error = RootedDir::open(&fixture.root)
            .unwrap()
            .remove_owned_regular("entry")
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(
            fs::read(fixture.root.join("entry")).unwrap(),
            REPLACEMENT_BYTES
        );
        assert_eq!(
            fs::symlink_metadata(fixture.root.join("entry"))
                .unwrap()
                .ino(),
            replacement.ino()
        );
        let quarantined = fs::symlink_metadata(&quarantine).unwrap();
        assert_eq!(fs::read(&quarantine).unwrap(), REGULAR_RETRY_ENTRY_BYTES);
        assert_eq!(quarantined.ino(), fixture.entry_inode);
        assert_eq!(quarantined.dev(), fixture.entry_device);
        let roles = fixture.namespace_roles();
        assert!(
            roles
                .iter()
                .any(|(name, kind)| name.starts_with(b"cleanup-intent-v1-") && kind.is_file())
        );
        assert!(
            !roles
                .iter()
                .any(|(name, _)| name.starts_with(b"cleanup-decision-v1-"))
        );
        assert!(roles.iter().any(|(name, kind)| {
            name.starts_with(b"cleanup-op-v1-")
                && kind.is_dir()
                && name.windows(4).any(|window| window == b"-k02")
        }));
        assert!(
            roles
                .iter()
                .any(|(name, kind)| name.starts_with(b"cleanup-regular-v1-") && kind.is_file())
        );
        fixture.assert_sibling_preserved();
    }

    #[test]
    fn cleanup_intent_regular_retry_rejects_initial_symlink() {
        // Catches starting a regular journal when the public name is already a symlink.
        let fixture = RegularCleanupRetryFixture::create();
        fs::remove_file(fixture.root.join("entry")).unwrap();
        symlink("sibling", fixture.root.join("entry")).unwrap();
        let planted = fs::symlink_metadata(fixture.root.join("entry")).unwrap();

        let error = RootedDir::open(&fixture.root)
            .unwrap()
            .remove_owned_regular("entry")
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert!(
            fs::symlink_metadata(fixture.root.join("entry"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            fs::symlink_metadata(fixture.root.join("entry"))
                .unwrap()
                .ino(),
            planted.ino()
        );
        assert_eq!(
            fs::read_link(fixture.root.join("entry")).unwrap(),
            Path::new("sibling")
        );
        fixture.assert_sibling_preserved();
        fixture.assert_namespace_empty_or_absent();
    }

    #[test]
    fn cleanup_intent_regular_retry_rejects_initial_hardlink() {
        // Catches starting a regular journal when the public file already has nlink=2.
        let fixture = RegularCleanupRetryFixture::create();
        let alias = fixture.root.join("alias");
        fs::hard_link(fixture.root.join("entry"), &alias).unwrap();

        let error = RootedDir::open(&fixture.root)
            .unwrap()
            .remove_owned_regular("entry")
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        let entry = fs::symlink_metadata(fixture.root.join("entry")).unwrap();
        let alias_meta = fs::symlink_metadata(&alias).unwrap();
        assert_eq!(
            fs::read(fixture.root.join("entry")).unwrap(),
            REGULAR_RETRY_ENTRY_BYTES
        );
        assert_eq!(fs::read(&alias).unwrap(), REGULAR_RETRY_ENTRY_BYTES);
        assert_eq!(entry.ino(), fixture.entry_inode);
        assert_eq!(alias_meta.ino(), fixture.entry_inode);
        assert_eq!(entry.nlink(), 2);
        assert_eq!(alias_meta.nlink(), 2);
        fixture.assert_sibling_preserved();
        fixture.assert_namespace_empty_or_absent();
    }

    #[test]
    fn cleanup_intent_regular_retry_preserves_quarantine_or_captured_hardlink() {
        // Catches unlinking a bound regular after an extra hard link appears on that slot.
        for fault in [
            CleanupFault::AfterCleanupQuarantineRename(libc::EIO),
            CleanupFault::AfterCleanupTargetExchange(libc::EIO),
        ] {
            let fixture = RegularCleanupRetryFixture::create();
            let interrupt = fixture.interrupt_after_public_entry_vanishes(fault);
            let bound = match fault {
                CleanupFault::AfterCleanupQuarantineRename(_) => {
                    fixture.unique_namespace_role(b"cleanup-regular-v1-")
                }
                CleanupFault::AfterCleanupTargetExchange(_) => fixture
                    .unique_namespace_role(b"cleanup-op-v1-")
                    .join("cleanup-placeholder-v1"),
                _ => unreachable!(),
            };
            let bound_meta = fs::symlink_metadata(&bound).unwrap();
            let alias = fixture.root.parent().unwrap().join("bound-hardlink-alias");
            fs::hard_link(&bound, &alias).unwrap();
            let mut roles = fixture.namespace_roles();
            roles.sort_by(|left, right| left.0.cmp(&right.0));
            drop(interrupt);

            let error = RootedDir::open(&fixture.root)
                .unwrap()
                .remove_owned_regular("entry")
                .unwrap_err();

            assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
            let after = fs::symlink_metadata(&bound).unwrap();
            assert_eq!(fs::read(&bound).unwrap(), REGULAR_RETRY_ENTRY_BYTES);
            assert_eq!(after.ino(), bound_meta.ino());
            assert_eq!(after.dev(), fixture.entry_device);
            assert_eq!(after.nlink(), 2);
            assert_eq!(fs::read(&alias).unwrap(), REGULAR_RETRY_ENTRY_BYTES);
            assert_eq!(fs::symlink_metadata(&alias).unwrap().nlink(), 2);
            let mut after_roles = fixture.namespace_roles();
            after_roles.sort_by(|left, right| left.0.cmp(&right.0));
            assert_eq!(after_roles, roles);
            fixture.assert_sibling_preserved();
        }
    }

    #[test]
    fn cleanup_intent_regular_retry_rejects_same_key_cross_kind_bootstrap() {
        // Catches adopting a Regular k02 bootstrap when a same-key Tree k01 slot also exists.
        let fixture = RegularCleanupRetryFixture::create();
        let parent = RootedDir::open(&fixture.root).unwrap();
        let fault = CleanupFaultOverride::set(CleanupFault::AfterCleanupBootstrap(libc::EIO));
        let error = parent.remove_owned_regular("entry").unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EIO));
        drop(fault);
        drop(parent);

        let namespace = fixture.root.join(".mac-worker-rooted-fs");
        let regular_bootstrap = fixture.unique_namespace_role(b"cleanup-op-v1-");
        assert!(
            regular_bootstrap
                .file_name()
                .unwrap()
                .as_bytes()
                .windows(4)
                .any(|window| window == b"-k02")
        );
        let regular_inode = fs::symlink_metadata(&regular_bootstrap).unwrap().ino();
        let parsed = super::parse_cleanup_bootstrap_name(
            &std::ffi::CString::new(regular_bootstrap.file_name().unwrap().as_bytes()).unwrap(),
        )
        .unwrap()
        .unwrap();
        let tree_name = super::cleanup_bootstrap_name(
            &parsed.key,
            parsed.target,
            parsed.original_mode,
            super::CleanupTargetKind::Tree,
        )
        .unwrap();
        let tree_bootstrap = namespace.join(std::ffi::OsStr::from_bytes(tree_name.to_bytes()));
        fs::create_dir(&tree_bootstrap).unwrap();
        fs::set_permissions(&tree_bootstrap, fs::Permissions::from_mode(0o700)).unwrap();
        let tree_inode = fs::symlink_metadata(&tree_bootstrap).unwrap().ino();
        assert!(
            tree_name
                .as_bytes()
                .windows(4)
                .any(|window| window == b"-k01")
        );

        let error = RootedDir::open(&fixture.root)
            .unwrap()
            .remove_owned_regular("entry")
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(
            fs::read(fixture.root.join("entry")).unwrap(),
            REGULAR_RETRY_ENTRY_BYTES
        );
        assert_eq!(
            fs::symlink_metadata(fixture.root.join("entry"))
                .unwrap()
                .ino(),
            fixture.entry_inode
        );
        assert_eq!(
            fs::symlink_metadata(&regular_bootstrap).unwrap().ino(),
            regular_inode
        );
        assert_eq!(
            fs::symlink_metadata(&tree_bootstrap).unwrap().ino(),
            tree_inode
        );
        assert!(regular_bootstrap.is_dir());
        assert!(tree_bootstrap.is_dir());
        fixture.assert_sibling_preserved();
    }

    #[test]
    fn cleanup_intent_regular_retry_preserves_legacy_public_remove_uuid() {
        // Catches adopting an unbound public remove-UUID sibling as exact cleanup evidence.
        const LEGACY_BYTES: &[u8] = b"legacy-public-remove\n";
        let fixture = RegularCleanupRetryFixture::create();
        fs::remove_file(fixture.root.join("entry")).unwrap();
        let legacy = fixture
            .root
            .join("remove-303f0f4a-6b5c-4d8e-9f00-112233445566");
        fs::write(&legacy, LEGACY_BYTES).unwrap();
        let planted = fs::symlink_metadata(&legacy).unwrap();

        RootedDir::open(&fixture.root)
            .unwrap()
            .remove_owned_regular("entry")
            .unwrap();

        assert_eq!(fs::read(&legacy).unwrap(), LEGACY_BYTES);
        assert_eq!(fs::symlink_metadata(&legacy).unwrap().ino(), planted.ino());
        fixture.assert_sibling_preserved();
        fixture.assert_namespace_empty_or_absent();
    }

    #[test]
    fn cleanup_intent_regular_retry_rejects_kind_operation_or_quarantine_mismatch() {
        // Catches adopting a Regular retry after kind, bootstrap, or quarantine
        // evidence is rewritten to the Tree grammar.
        enum Mismatch {
            Intent,
            Operation,
            Quarantine,
        }
        for arm in [Mismatch::Intent, Mismatch::Operation, Mismatch::Quarantine] {
            let fixture = RegularCleanupRetryFixture::create();
            let interrupt = fixture.interrupt_after_public_entry_vanishes(
                CleanupFault::AfterCleanupQuarantineRename(libc::EIO),
            );
            let intent = fixture.unique_namespace_role(b"cleanup-intent-v1-");
            let operation = fixture.unique_namespace_role(b"cleanup-op-v1-");
            let quarantine = fixture.unique_namespace_role(b"cleanup-regular-v1-");
            let intent_inode = fs::symlink_metadata(&intent).unwrap().ino();
            let operation_inode = fs::symlink_metadata(&operation).unwrap().ino();
            let quarantine_inode = fs::symlink_metadata(&quarantine).unwrap().ino();
            let intent_bytes = fs::read(&intent).unwrap();
            let quarantine_bytes = fs::read(&quarantine).unwrap();
            let namespace = fixture.root.join(".mac-worker-rooted-fs");
            let expected = match arm {
                Mismatch::Intent => {
                    let mut record: CleanupIntentV1 =
                        cleanup_parse_canonical_json(&intent_bytes).unwrap();
                    record.kind = super::CleanupTargetKind::Tree;
                    let rewritten = super::cleanup_canonical_json(&record).unwrap();
                    fs::write(&intent, &rewritten).unwrap();
                    fs::set_permissions(&intent, fs::Permissions::from_mode(0o600)).unwrap();
                    Some(libc::EINVAL)
                }
                Mismatch::Operation => {
                    let parsed = super::parse_cleanup_bootstrap_name(
                        &std::ffi::CString::new(operation.file_name().unwrap().as_bytes()).unwrap(),
                    )
                    .unwrap()
                    .unwrap();
                    let tree_name = super::cleanup_bootstrap_name(
                        &parsed.key,
                        parsed.target,
                        parsed.original_mode,
                        super::CleanupTargetKind::Tree,
                    )
                    .unwrap();
                    fs::rename(
                        &operation,
                        namespace.join(std::ffi::OsStr::from_bytes(tree_name.to_bytes())),
                    )
                    .unwrap();
                    Some(libc::ESTALE)
                }
                Mismatch::Quarantine => {
                    fs::rename(
                        &quarantine,
                        namespace.join(format!(
                            "cleanup-tree-v1-{}",
                            uuid::Uuid::new_v4().hyphenated()
                        )),
                    )
                    .unwrap();
                    Some(libc::ESTALE)
                }
            };
            let intent_after = match arm {
                Mismatch::Intent => fs::read(&intent).unwrap(),
                _ => intent_bytes,
            };
            let operation_after = match arm {
                Mismatch::Operation => fixture.unique_namespace_role(b"cleanup-op-v1-"),
                _ => operation.clone(),
            };
            let quarantine_after = match arm {
                Mismatch::Quarantine => fs::read_dir(&namespace)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .find(|path| {
                        path.file_name()
                            .is_some_and(|name| name.as_bytes().starts_with(b"cleanup-tree-v1-"))
                    })
                    .unwrap(),
                _ => quarantine.clone(),
            };
            drop(interrupt);

            let error = fixture.retry_remove_owned_regular();

            assert_eq!(error.raw_os_error(), expected);
            assert!(!fixture.root.join("entry").exists());
            assert_eq!(fs::read(&intent).unwrap(), intent_after);
            assert_eq!(fs::symlink_metadata(&intent).unwrap().ino(), intent_inode);
            assert_eq!(
                fs::symlink_metadata(&intent).unwrap().permissions().mode() & 0o777,
                0o600
            );
            assert_eq!(
                fs::symlink_metadata(&operation_after).unwrap().ino(),
                operation_inode
            );
            assert!(operation_after.is_dir());
            assert_eq!(fs::read(&quarantine_after).unwrap(), quarantine_bytes);
            assert_eq!(
                fs::symlink_metadata(&quarantine_after).unwrap().ino(),
                quarantine_inode
            );
            assert!(!fs::read_dir(&namespace).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .as_bytes()
                    .starts_with(b"cleanup-decision-v1-")
            }));
            fixture.assert_sibling_preserved();
        }
    }

    #[test]
    fn cleanup_intent_regular_retry_preserves_bad_published_intent() {
        // Catches adopting or rewriting a published Regular intent that is no
        // longer a canonical owner-only record.
        enum BadIntent {
            Truncated,
            TrailingSpace,
            Oversized,
            Mode0644,
        }
        for arm in [
            BadIntent::Truncated,
            BadIntent::TrailingSpace,
            BadIntent::Oversized,
            BadIntent::Mode0644,
        ] {
            let fixture = RegularCleanupRetryFixture::create();
            let interrupt = fixture.interrupt_after_public_entry_vanishes(
                CleanupFault::AfterCleanupQuarantineRename(libc::EIO),
            );
            let intent = fixture.unique_namespace_role(b"cleanup-intent-v1-");
            let operation = fixture.unique_namespace_role(b"cleanup-op-v1-");
            let quarantine = fixture.unique_namespace_role(b"cleanup-regular-v1-");
            let intent_inode = fs::symlink_metadata(&intent).unwrap().ino();
            let operation_inode = fs::symlink_metadata(&operation).unwrap().ino();
            let quarantine_inode = fs::symlink_metadata(&quarantine).unwrap().ino();
            let quarantine_bytes = fs::read(&quarantine).unwrap();
            let canonical = fs::read(&intent).unwrap();
            let (planted, expected_raw, expected_kind, expected_mode) = match arm {
                BadIntent::Truncated => {
                    let planted = canonical[..canonical.len() / 2].to_vec();
                    fs::write(&intent, &planted).unwrap();
                    (planted, Some(libc::EINVAL), None, 0o600)
                }
                BadIntent::TrailingSpace => {
                    let mut planted = canonical;
                    planted.push(b' ');
                    fs::write(&intent, &planted).unwrap();
                    (planted, Some(libc::EINVAL), None, 0o600)
                }
                BadIntent::Oversized => {
                    let planted = vec![b'x'; 4097];
                    fs::write(&intent, &planted).unwrap();
                    (planted, Some(libc::EFBIG), None, 0o600)
                }
                BadIntent::Mode0644 => {
                    fs::set_permissions(&intent, fs::Permissions::from_mode(0o644)).unwrap();
                    (
                        canonical,
                        None,
                        Some(std::io::ErrorKind::PermissionDenied),
                        0o644,
                    )
                }
            };
            drop(interrupt);

            let error = fixture.retry_remove_owned_regular();

            assert_eq!(error.raw_os_error(), expected_raw);
            if let Some(kind) = expected_kind {
                assert_eq!(error.kind(), kind);
            }
            assert_eq!(fs::read(&intent).unwrap(), planted);
            assert_eq!(fs::symlink_metadata(&intent).unwrap().ino(), intent_inode);
            assert_eq!(
                fs::symlink_metadata(&intent).unwrap().permissions().mode() & 0o777,
                expected_mode
            );
            assert_eq!(fs::read(&quarantine).unwrap(), quarantine_bytes);
            assert_eq!(
                fs::symlink_metadata(&quarantine).unwrap().ino(),
                quarantine_inode
            );
            assert_eq!(
                fs::symlink_metadata(&operation).unwrap().ino(),
                operation_inode
            );
            assert!(operation.is_dir());
            fixture.assert_sibling_preserved();
        }
    }

    #[test]
    fn cleanup_intent_regular_retry_rejects_duplicate_intent_evidence() {
        // Catches adopting a published Regular intent when the same canonical
        // bytes also remain in the bound operation stage slot.
        let fixture = RegularCleanupRetryFixture::create();
        let interrupt = fixture.interrupt_after_public_entry_vanishes(
            CleanupFault::AfterCleanupQuarantineRename(libc::EIO),
        );
        let intent = fixture.unique_namespace_role(b"cleanup-intent-v1-");
        let operation = fixture.unique_namespace_role(b"cleanup-op-v1-");
        let quarantine = fixture.unique_namespace_role(b"cleanup-regular-v1-");
        let record: CleanupIntentV1 =
            cleanup_parse_canonical_json(&fs::read(&intent).unwrap()).unwrap();
        let stage_name = super::cleanup_intent_stage_name(&record.key_sha256).unwrap();
        let stage = operation.join(std::ffi::OsStr::from_bytes(stage_name.to_bytes()));
        let canonical = fs::read(&intent).unwrap();
        fs::write(&stage, &canonical).unwrap();
        fs::set_permissions(&stage, fs::Permissions::from_mode(0o600)).unwrap();
        let intent_inode = fs::symlink_metadata(&intent).unwrap().ino();
        let stage_inode = fs::symlink_metadata(&stage).unwrap().ino();
        let operation_inode = fs::symlink_metadata(&operation).unwrap().ino();
        let quarantine_inode = fs::symlink_metadata(&quarantine).unwrap().ino();
        let quarantine_bytes = fs::read(&quarantine).unwrap();
        drop(interrupt);

        let error = fixture.retry_remove_owned_regular();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(fs::read(&intent).unwrap(), canonical);
        assert_eq!(fs::symlink_metadata(&intent).unwrap().ino(), intent_inode);
        assert_eq!(fs::read(&stage).unwrap(), canonical);
        assert_eq!(fs::symlink_metadata(&stage).unwrap().ino(), stage_inode);
        assert_eq!(
            fs::symlink_metadata(&stage).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::symlink_metadata(&operation).unwrap().ino(),
            operation_inode
        );
        assert_eq!(fs::read(&quarantine).unwrap(), quarantine_bytes);
        assert_eq!(
            fs::symlink_metadata(&quarantine).unwrap().ino(),
            quarantine_inode
        );
        fixture.assert_sibling_preserved();
    }

    #[test]
    fn cleanup_intent_regular_retry_rejects_wrong_mode_operation_or_namespace() {
        // Catches opening a Regular retry through a group-readable operation or
        // private namespace directory.
        enum WrongMode {
            Operation,
            Namespace,
        }
        for arm in [WrongMode::Operation, WrongMode::Namespace] {
            let fixture = RegularCleanupRetryFixture::create();
            let interrupt = fixture.interrupt_after_public_entry_vanishes(
                CleanupFault::AfterCleanupQuarantineRename(libc::EIO),
            );
            let intent = fixture.unique_namespace_role(b"cleanup-intent-v1-");
            let operation = fixture.unique_namespace_role(b"cleanup-op-v1-");
            let quarantine = fixture.unique_namespace_role(b"cleanup-regular-v1-");
            let namespace = fixture.root.join(".mac-worker-rooted-fs");
            let intent_bytes = fs::read(&intent).unwrap();
            let intent_inode = fs::symlink_metadata(&intent).unwrap().ino();
            let quarantine_bytes = fs::read(&quarantine).unwrap();
            let quarantine_inode = fs::symlink_metadata(&quarantine).unwrap().ino();
            let operation_inode = fs::symlink_metadata(&operation).unwrap().ino();
            let namespace_inode = fs::symlink_metadata(&namespace).unwrap().ino();
            let mutated = match arm {
                WrongMode::Operation => operation.clone(),
                WrongMode::Namespace => namespace.clone(),
            };
            fs::set_permissions(&mutated, fs::Permissions::from_mode(0o755)).unwrap();
            assert_eq!(
                fs::symlink_metadata(&mutated).unwrap().permissions().mode() & 0o777,
                0o755
            );
            let mutated_inode = fs::symlink_metadata(&mutated).unwrap().ino();
            drop(interrupt);

            let result = RootedDir::open(&fixture.root)
                .unwrap()
                .remove_owned_regular("entry");
            assert_eq!(
                fs::symlink_metadata(&mutated).unwrap().permissions().mode() & 0o777,
                0o755
            );
            assert_eq!(fs::symlink_metadata(&mutated).unwrap().ino(), mutated_inode);
            assert_eq!(fs::read(&intent).unwrap(), intent_bytes);
            assert_eq!(fs::symlink_metadata(&intent).unwrap().ino(), intent_inode);
            assert_eq!(fs::read(&quarantine).unwrap(), quarantine_bytes);
            assert_eq!(
                fs::symlink_metadata(&quarantine).unwrap().ino(),
                quarantine_inode
            );
            assert_eq!(
                fs::symlink_metadata(&operation).unwrap().ino(),
                operation_inode
            );
            assert_eq!(
                fs::symlink_metadata(&namespace).unwrap().ino(),
                namespace_inode
            );
            fixture.assert_sibling_preserved();
            if matches!(arm, WrongMode::Namespace) {
                fs::set_permissions(&namespace, fs::Permissions::from_mode(0o700)).unwrap();
            }
            let error = result.expect_err("0755 private-dir retry must fail closed");
            match arm {
                WrongMode::Operation => {
                    // require_private_directory sees group/other bits before the
                    // bootstrap 0700 ESTALE check.
                    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
                    assert_eq!(error.raw_os_error(), None);
                }
                WrongMode::Namespace => {
                    // PrivateNamespace::open_or_create_at rejects != 0700 as ESTALE
                    // before the owner-only PermissionDenied path.
                    assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
                }
            }
        }
    }

    #[test]
    fn cleanup_intent_regular_retry_regular_cleanup_capability_probe_recovers_regular_exchange() {
        let fixture = RegularCleanupRetryFixture::create();
        let parent = RootedDir::open(&fixture.root).unwrap();
        let fault =
            CleanupFaultOverride::set(CleanupFault::DuringCleanupCapabilityProbe(12, libc::EIO));
        assert_eq!(
            parent
                .remove_owned_regular("entry")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(fault);
        drop(parent);
        fixture.assert_public_entry_unchanged();

        let operation = fixture.unique_namespace_role(b"cleanup-op-v1-");
        let left = operation.join("cleanup-capability-probe-v1/left-v1/regular-exchange-left-v1");
        let right =
            operation.join("cleanup-capability-probe-v1/right-v1/regular-exchange-right-v1");
        assert!(fs::symlink_metadata(&left).unwrap().file_type().is_dir());
        assert!(fs::symlink_metadata(&right).unwrap().file_type().is_file());
        assert!(
            !operation
                .join("cleanup-capability-probe-v1/left-v1/directory-exchange-left-v1")
                .exists()
        );
        assert!(
            !operation
                .join("cleanup-capability-probe-v1/left-v1/directory-no-replace-source-v1")
                .exists()
        );
        assert!(
            !operation
                .join("cleanup-capability-probe-v1/right-v1/directory-exchange-right-v1")
                .exists()
        );
        assert!(
            !operation
                .join("cleanup-capability-probe-v1/right-v1/directory-no-replace-destination-v1")
                .exists()
        );

        RootedDir::open(&fixture.root)
            .unwrap()
            .remove_owned_regular("entry")
            .unwrap();
        fixture.assert_public_entry_absent();
        fixture.assert_namespace_empty_or_absent();
    }

    #[test]
    fn cleanup_intent_regular_retry_regular_cleanup_capability_probe_regular_exchange_unsupported()
    {
        let fixture = RegularCleanupRetryFixture::create();
        let _capability = AtomicRenameCapabilityOverride::set(AtomicRenameCapability::NoExchange);
        let parent = RootedDir::open(&fixture.root).unwrap();
        assert_eq!(
            parent
                .remove_owned_regular("entry")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ENOTSUP)
        );
        fixture.assert_public_entry_unchanged();
        fixture.assert_namespace_empty_or_absent();
    }

    #[test]
    fn cleanup_intent_regular_retry_regular_cleanup_restore_returns_live_eperm() {
        let fixture = RegularCleanupRetryFixture::create();
        let parent = RootedDir::open(&fixture.root).unwrap();
        let _fault =
            CleanupFaultOverride::set(CleanupFault::AfterAcquisitionValidation(libc::EPERM));
        assert_eq!(
            parent
                .remove_owned_regular("entry")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EPERM)
        );
        fixture.assert_public_entry_unchanged();
        fixture.assert_namespace_empty_or_absent();
        assert!(!fs::read_dir(&fixture.root).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .as_bytes()
                .starts_with(b"remove-")
        }));
    }

    #[test]
    fn cleanup_intent_regular_retry_regular_cleanup_restore_recovers_after_destination_sync() {
        let fixture = RegularCleanupRetryFixture::create();
        let parent = RootedDir::open(&fixture.root).unwrap();
        let validation =
            CleanupFaultOverride::set(CleanupFault::AfterAcquisitionValidation(libc::ESTALE));
        let sync = CleanupDecisionFaultOverride::set(
            CleanupDecisionFault::AfterRestoreDestinationSync(libc::EIO),
        );
        assert_eq!(
            parent
                .remove_owned_regular("entry")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(sync);
        drop(validation);
        drop(parent);

        fixture.assert_public_entry_unchanged();
        let decision = fixture.unique_namespace_role(b"cleanup-decision-v1-");
        let record: CleanupDecisionRecordV1 =
            cleanup_parse_canonical_json(&fs::read(&decision).unwrap()).unwrap();
        assert!(matches!(record.decision, CleanupDecisionV1::Restore));
        assert_eq!(
            fs::symlink_metadata(&decision)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        fixture.assert_regular_retry_intent_bootstrap_binding();
        let quarantine = fixture.unique_namespace_role(b"cleanup-regular-v1-");
        let quarantine_meta = fs::symlink_metadata(&quarantine).unwrap();
        assert!(quarantine_meta.file_type().is_dir());
        assert_eq!(quarantine_meta.permissions().mode() & 0o777, 0o700);

        RootedDir::open(&fixture.root)
            .unwrap()
            .remove_owned_regular("entry")
            .unwrap();
        fixture.assert_public_entry_absent();
        fixture.assert_namespace_empty_or_absent();
    }

    #[test]
    fn cleanup_delete_replays_entries_in_exact_operation_target_after_exchange() {
        // Catches assuming that an exchanged target must stay empty after a
        // crash. Directory-entry deletion can replay unless the target was
        // synced, and recovery must remain descriptor/identity-bound.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let fault = CleanupFaultOverride::set(CleanupFault::AfterCleanupTargetExchange(libc::EIO));

        let error = parent.remove_owned_child("root").unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::EIO));
        drop(fault);
        let namespace = physical.join(".mac-worker-rooted-fs");
        let intent_path = fs::read_dir(&namespace)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .as_bytes()
                    .starts_with(b"cleanup-intent-v1-")
            })
            .unwrap();
        let intent: CleanupIntentV1 =
            serde_json::from_slice(&fs::read(intent_path).unwrap()).unwrap();
        let operation_target = namespace.join(&intent.operation).join(&intent.placeholder);
        let target_before = fs::metadata(&operation_target).unwrap();
        assert_eq!(target_before.dev(), intent.target.device);
        assert_eq!(target_before.ino(), intent.target.inode);
        fs::write(operation_target.join("replayed-value"), b"owned").unwrap();

        assert!(parent.resume_pending_owned_child_cleanup("root").unwrap());

        assert!(!root_path.exists());
        assert_eq!(fs::read_dir(namespace).unwrap().count(), 0);
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

    fn assert_cleanup_decision_publication_retry(
        decision: CleanupDecisionV1,
        decision_fault: CleanupDecisionFault,
    ) {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let target = fs::metadata(&root_path).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let validation_fault = (decision == CleanupDecisionV1::Restore).then(|| {
            CleanupFaultOverride::set(CleanupFault::AfterAcquisitionValidation(libc::ESTALE))
        });
        let publication_fault = CleanupDecisionFaultOverride::set(decision_fault);

        let error = parent.remove_owned_child("root").unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::EIO), "{error:?}");
        drop(publication_fault);
        drop(validation_fault);
        drop(parent);
        assert!(!root_path.exists());
        let namespace = physical.join(".mac-worker-rooted-fs");
        let mut canonical = Vec::new();
        let mut staged = Vec::new();
        for entry in fs::read_dir(&namespace).unwrap() {
            let path = entry.unwrap().path();
            if path
                .file_name()
                .unwrap()
                .as_bytes()
                .starts_with(b"cleanup-decision-v1-")
            {
                canonical.push(path);
            } else if path.is_dir()
                && path
                    .file_name()
                    .unwrap()
                    .as_bytes()
                    .starts_with(b"cleanup-op-v1-")
            {
                staged.extend(
                    fs::read_dir(path)
                        .unwrap()
                        .map(|entry| entry.unwrap().path())
                        .filter(|path| {
                            path.file_name()
                                .unwrap()
                                .as_bytes()
                                .starts_with(b"cleanup-decision-stage-v1-")
                        }),
                );
            }
        }
        if matches!(
            decision_fault,
            CleanupDecisionFault::AfterRename(libc::EIO)
                | CleanupDecisionFault::AfterDestinationSync(libc::EIO)
                | CleanupDecisionFault::AfterSourceSync(libc::EIO)
        ) {
            assert_eq!(canonical.len(), 1);
            assert!(staged.is_empty());
        } else {
            assert!(canonical.is_empty());
            assert_eq!(staged.len(), 1);
            let suffix = match decision {
                CleanupDecisionV1::Delete => b"-c01".as_slice(),
                CleanupDecisionV1::Restore => b"-c02".as_slice(),
            };
            assert!(staged[0].file_name().unwrap().as_bytes().ends_with(suffix));
            if decision_fault == CleanupDecisionFault::AfterStageSync(libc::EIO) {
                assert!(fs::read(&staged[0]).unwrap().is_empty());
            }
            if decision_fault == CleanupDecisionFault::DuringWrite(libc::EIO) {
                let bytes = fs::read(&staged[0]).unwrap();
                assert!(!bytes.is_empty());
                assert!(serde_json::from_slice::<CleanupDecisionRecordV1>(&bytes).is_err());
            }
        }

        let parent = RootedDir::open(&physical).unwrap();
        match decision {
            CleanupDecisionV1::Delete => {
                parent.remove_owned_child("root").unwrap();
                assert!(!root_path.exists());
            }
            CleanupDecisionV1::Restore => {
                assert!(parent.resume_pending_owned_child_cleanup("root").unwrap());
                let restored = fs::metadata(&root_path).unwrap();
                assert_eq!(restored.dev(), target.dev());
                assert_eq!(restored.ino(), target.ino());
                assert_eq!(restored.permissions().mode() & 0o777, 0o500);
                assert_eq!(fs::read(root_path.join("value")).unwrap(), b"owned");
                assert!(!parent.resume_pending_owned_child_cleanup("root").unwrap());
            }
        }
        assert_eq!(fs::read_dir(namespace).unwrap().count(), 0);
    }

    #[test]
    fn cleanup_decision_publication_delete_recovers_after_stage_sync() {
        assert_cleanup_decision_publication_retry(
            CleanupDecisionV1::Delete,
            CleanupDecisionFault::AfterStageSync(libc::EIO),
        );
    }

    #[test]
    fn cleanup_decision_publication_delete_recovers_during_write() {
        assert_cleanup_decision_publication_retry(
            CleanupDecisionV1::Delete,
            CleanupDecisionFault::DuringWrite(libc::EIO),
        );
    }

    #[test]
    fn cleanup_decision_publication_delete_recovers_before_rename() {
        assert_cleanup_decision_publication_retry(
            CleanupDecisionV1::Delete,
            CleanupDecisionFault::BeforeRename(libc::EIO),
        );
    }

    #[test]
    fn cleanup_decision_publication_delete_recovers_after_rename() {
        assert_cleanup_decision_publication_retry(
            CleanupDecisionV1::Delete,
            CleanupDecisionFault::AfterRename(libc::EIO),
        );
    }

    #[test]
    fn cleanup_decision_publication_delete_recovers_after_destination_sync() {
        assert_cleanup_decision_publication_retry(
            CleanupDecisionV1::Delete,
            CleanupDecisionFault::AfterDestinationSync(libc::EIO),
        );
    }

    #[test]
    fn cleanup_decision_publication_delete_recovers_after_source_sync() {
        assert_cleanup_decision_publication_retry(
            CleanupDecisionV1::Delete,
            CleanupDecisionFault::AfterSourceSync(libc::EIO),
        );
    }

    #[test]
    fn cleanup_decision_publication_restore_recovers_after_stage_sync() {
        assert_cleanup_decision_publication_retry(
            CleanupDecisionV1::Restore,
            CleanupDecisionFault::AfterStageSync(libc::EIO),
        );
    }

    #[test]
    fn cleanup_decision_publication_restore_recovers_during_write() {
        assert_cleanup_decision_publication_retry(
            CleanupDecisionV1::Restore,
            CleanupDecisionFault::DuringWrite(libc::EIO),
        );
    }

    #[test]
    fn cleanup_decision_stage_repeated_eof_repair_keeps_inode_and_converges() {
        // Catches replacing a bound decision stage, or refusing to repair the
        // same canonical prefix after more than one interrupted rewrite.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();

        let mut stage_path = None;
        let mut first_prefix = None;
        let mut first_inode = None;
        for attempt in 0..2 {
            let fault =
                CleanupDecisionFaultOverride::set(CleanupDecisionFault::DuringWrite(libc::EIO));
            let error = parent.remove_owned_child("root").unwrap_err();
            assert_eq!(error.raw_os_error(), Some(libc::EIO));
            drop(fault);

            let current_stage = fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .find(|path| {
                    path.is_dir()
                        && path
                            .file_name()
                            .unwrap()
                            .as_bytes()
                            .starts_with(b"cleanup-op-v1-")
                })
                .and_then(|operation| {
                    fs::read_dir(operation)
                        .unwrap()
                        .map(|entry| entry.unwrap().path())
                        .find(|path| {
                            path.file_name()
                                .unwrap()
                                .as_bytes()
                                .starts_with(b"cleanup-decision-stage-v1-")
                        })
                })
                .unwrap();
            let prefix = fs::read(&current_stage).unwrap();
            assert!(!prefix.is_empty());
            assert!(serde_json::from_slice::<CleanupDecisionRecordV1>(&prefix).is_err());
            let inode = fs::metadata(&current_stage).unwrap().ino();
            if attempt == 0 {
                stage_path = Some(current_stage);
                first_prefix = Some(prefix);
                first_inode = Some(inode);
            } else {
                assert_eq!(&current_stage, stage_path.as_ref().unwrap());
                assert_eq!(&prefix, first_prefix.as_ref().unwrap());
                assert_eq!(inode, first_inode.unwrap());
            }
        }

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
    fn cleanup_decision_publication_restore_recovers_before_rename() {
        assert_cleanup_decision_publication_retry(
            CleanupDecisionV1::Restore,
            CleanupDecisionFault::BeforeRename(libc::EIO),
        );
    }

    #[test]
    fn cleanup_decision_publication_restore_recovers_after_rename() {
        assert_cleanup_decision_publication_retry(
            CleanupDecisionV1::Restore,
            CleanupDecisionFault::AfterRename(libc::EIO),
        );
    }

    #[test]
    fn cleanup_decision_publication_restore_recovers_after_destination_sync() {
        assert_cleanup_decision_publication_retry(
            CleanupDecisionV1::Restore,
            CleanupDecisionFault::AfterDestinationSync(libc::EIO),
        );
    }

    #[test]
    fn cleanup_decision_publication_restore_recovers_after_source_sync() {
        assert_cleanup_decision_publication_retry(
            CleanupDecisionV1::Restore,
            CleanupDecisionFault::AfterSourceSync(libc::EIO),
        );
    }

    fn assert_cleanup_decision_complete_stage_is_synced_before_adoption(
        decision: CleanupDecisionV1,
    ) {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let target = fs::metadata(&root_path).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let validation_fault = (decision == CleanupDecisionV1::Restore).then(|| {
            CleanupFaultOverride::set(CleanupFault::AfterAcquisitionValidation(libc::ESTALE))
        });
        let write_fault = CleanupDecisionFaultOverride::set(
            CleanupDecisionFault::AfterFullWriteBeforeSync(libc::EIO),
        );
        assert_eq!(
            parent
                .remove_owned_child("root")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(write_fault);
        drop(validation_fault);
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
                    .starts_with(b"cleanup-decision-stage-v1-")
            })
            .unwrap();
        let bytes = fs::read(&stage).unwrap();
        assert!(
            cleanup_parse_canonical_json::<CleanupDecisionRecordV1>(&bytes)
                .unwrap()
                .decision
                == decision
        );
        let inode = fs::metadata(&stage).unwrap().ino();

        let parent = RootedDir::open(&physical).unwrap();
        let adoption_fault = CleanupDecisionFaultOverride::set(
            CleanupDecisionFault::AfterCompleteAdoptionSync(libc::EIO),
        );
        assert_eq!(
            parent
                .remove_owned_child("root")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        assert_eq!(fs::read(&stage).unwrap(), bytes);
        assert_eq!(fs::metadata(&stage).unwrap().ino(), inode);
        drop(adoption_fault);
        drop(parent);

        let parent = RootedDir::open(&physical).unwrap();
        match decision {
            CleanupDecisionV1::Delete => {
                parent.remove_owned_child("root").unwrap();
                assert!(!root_path.exists());
            }
            CleanupDecisionV1::Restore => {
                assert!(parent.resume_pending_owned_child_cleanup("root").unwrap());
                let restored = fs::metadata(&root_path).unwrap();
                assert_eq!(restored.dev(), target.dev());
                assert_eq!(restored.ino(), target.ino());
                assert_eq!(restored.permissions().mode() & 0o777, 0o500);
                assert_eq!(fs::read(root_path.join("value")).unwrap(), b"owned");
                assert!(!parent.resume_pending_owned_child_cleanup("root").unwrap());
            }
        }
        assert_eq!(fs::read_dir(namespace).unwrap().count(), 0);
    }

    #[test]
    fn cleanup_decision_delete_complete_stage_is_synced_before_adoption() {
        assert_cleanup_decision_complete_stage_is_synced_before_adoption(CleanupDecisionV1::Delete);
    }

    #[test]
    fn cleanup_decision_restore_complete_stage_is_synced_before_adoption() {
        assert_cleanup_decision_complete_stage_is_synced_before_adoption(
            CleanupDecisionV1::Restore,
        );
    }

    fn cleanup_decision_stage_fixture() -> (tempfile::TempDir, PathBuf, PathBuf, PathBuf) {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let fault =
            CleanupDecisionFaultOverride::set(CleanupDecisionFault::BeforeRename(libc::EIO));
        assert_eq!(
            parent
                .remove_owned_child("root")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(fault);
        drop(parent);
        assert!(!root_path.exists());
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
                    .starts_with(b"cleanup-decision-stage-v1-")
            })
            .unwrap();
        (fixture, physical, operation, stage)
    }

    fn assert_cleanup_decision_evidence_preserved(
        physical: &Path,
        evidence: &[(PathBuf, Vec<u8>, u64)],
    ) {
        let error = RootedDir::open(physical)
            .unwrap()
            .remove_owned_child("root")
            .unwrap_err();
        assert!(matches!(
            error.raw_os_error(),
            Some(libc::EINVAL) | Some(libc::ESTALE)
        ));
        assert!(!physical.join("root").exists());
        for (path, bytes, inode) in evidence {
            assert_eq!(&fs::read(path).unwrap(), bytes);
            assert_eq!(fs::metadata(path).unwrap().ino(), *inode);
        }
    }

    #[test]
    fn cleanup_decision_stage_preserves_malformed_and_noncanonical_bytes() {
        for replacement in [
            b"{}".to_vec(),
            br#"{"unknown":1}"#.to_vec(),
            b"[".to_vec(),
            br#"{"wrong":"#.to_vec(),
        ] {
            let (_fixture, physical, _operation, stage) = cleanup_decision_stage_fixture();
            fs::write(&stage, &replacement).unwrap();
            let inode = fs::metadata(&stage).unwrap().ino();
            assert_cleanup_decision_evidence_preserved(&physical, &[(stage, replacement, inode)]);
        }

        let (_fixture, physical, _operation, stage) = cleanup_decision_stage_fixture();
        let mut noncanonical = fs::read(&stage).unwrap();
        noncanonical.push(b' ');
        fs::write(&stage, &noncanonical).unwrap();
        let inode = fs::metadata(&stage).unwrap().ino();
        assert_cleanup_decision_evidence_preserved(&physical, &[(stage, noncanonical, inode)]);
    }

    #[test]
    fn cleanup_decision_stage_preserves_opposite_choice_payload() {
        let (_fixture, physical, _operation, stage) = cleanup_decision_stage_fixture();
        let mut record: CleanupDecisionRecordV1 =
            cleanup_parse_canonical_json(&fs::read(&stage).unwrap()).unwrap();
        record.decision = CleanupDecisionV1::Restore;
        let opposite = serde_json::to_vec(&record).unwrap();
        fs::write(&stage, &opposite).unwrap();
        let inode = fs::metadata(&stage).unwrap().ino();

        assert_cleanup_decision_evidence_preserved(&physical, &[(stage, opposite, inode)]);
    }

    #[test]
    fn cleanup_decision_stage_rejects_duplicate_choices() {
        let (_fixture, physical, operation, stage) = cleanup_decision_stage_fixture();
        let stage_name = std::ffi::CString::new(stage.file_name().unwrap().as_bytes()).unwrap();
        let parsed = super::parse_cleanup_decision_stage_name(&stage_name)
            .unwrap()
            .unwrap();
        let mut record: CleanupDecisionRecordV1 =
            cleanup_parse_canonical_json(&fs::read(&stage).unwrap()).unwrap();
        record.decision = CleanupDecisionV1::Restore;
        let opposite = serde_json::to_vec(&record).unwrap();
        let duplicate_name = super::cleanup_decision_stage_name(
            &parsed.key,
            parsed.intent,
            CleanupDecisionV1::Restore,
        )
        .unwrap();
        let duplicate = operation.join(std::ffi::OsStr::from_bytes(duplicate_name.to_bytes()));
        fs::write(&duplicate, &opposite).unwrap();
        fs::set_permissions(&duplicate, fs::Permissions::from_mode(0o600)).unwrap();
        let original = fs::read(&stage).unwrap();
        let evidence = vec![
            (stage.clone(), original, fs::metadata(&stage).unwrap().ino()),
            (
                duplicate.clone(),
                opposite,
                fs::metadata(&duplicate).unwrap().ino(),
            ),
        ];

        assert_cleanup_decision_evidence_preserved(&physical, &evidence);
    }

    #[test]
    fn cleanup_decision_stage_rejects_canonical_conflict_and_stale_binding() {
        let (_fixture, physical, _operation, stage) = cleanup_decision_stage_fixture();
        let stage_name = std::ffi::CString::new(stage.file_name().unwrap().as_bytes()).unwrap();
        let parsed = super::parse_cleanup_decision_stage_name(&stage_name)
            .unwrap()
            .unwrap();
        let namespace = physical.join(".mac-worker-rooted-fs");
        let canonical_name = super::cleanup_decision_name(&parsed.key).unwrap();
        let canonical = namespace.join(std::ffi::OsStr::from_bytes(canonical_name.to_bytes()));
        let bytes = fs::read(&stage).unwrap();
        fs::write(&canonical, &bytes).unwrap();
        fs::set_permissions(&canonical, fs::Permissions::from_mode(0o600)).unwrap();
        let evidence = vec![
            (
                stage.clone(),
                bytes.clone(),
                fs::metadata(&stage).unwrap().ino(),
            ),
            (
                canonical.clone(),
                bytes,
                fs::metadata(&canonical).unwrap().ino(),
            ),
        ];
        assert_cleanup_decision_evidence_preserved(&physical, &evidence);

        let (_fixture, physical, operation, stage) = cleanup_decision_stage_fixture();
        let stage_name = std::ffi::CString::new(stage.file_name().unwrap().as_bytes()).unwrap();
        let parsed = super::parse_cleanup_decision_stage_name(&stage_name)
            .unwrap()
            .unwrap();
        let stale_name = super::cleanup_decision_stage_name(
            &parsed.key,
            super::FileIdentity {
                device: parsed.intent.device,
                inode: parsed.intent.inode.wrapping_add(1),
            },
            parsed.decision,
        )
        .unwrap();
        let stale = operation.join(std::ffi::OsStr::from_bytes(stale_name.to_bytes()));
        fs::rename(&stage, &stale).unwrap();
        let bytes = fs::read(&stale).unwrap();
        let inode = fs::metadata(&stale).unwrap().ino();
        assert_cleanup_decision_evidence_preserved(&physical, &[(stale, bytes, inode)]);
    }

    #[test]
    fn cleanup_decision_stage_rewrite_revalidates_binding_before_truncate() {
        // Catches truncating the detached original descriptor after the
        // deterministic stage name has been rebound to an unrelated inode.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let fault = CleanupDecisionFaultOverride::set(CleanupDecisionFault::DuringWrite(libc::EIO));
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
                path.is_dir()
                    && path
                        .file_name()
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
                    .starts_with(b"cleanup-decision-stage-v1-")
            })
            .unwrap();
        let original_bytes = fs::read(&stage).unwrap();
        let original_inode = fs::metadata(&stage).unwrap().ino();
        let detached = operation.join("detached-decision-stage-v1");
        let replacement_bytes = b"replacement-evidence".to_vec();
        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker_physical = physical.clone();
        let worker = std::thread::spawn(move || {
            let _handoff =
                super::CleanupDecisionRewriteHandoffOverride::set(reached_tx, release_rx);
            RootedDir::open(&worker_physical)
                .unwrap()
                .remove_owned_child("root")
        });
        reached_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();

        fs::rename(&stage, &detached).unwrap();
        fs::write(&stage, &replacement_bytes).unwrap();
        fs::set_permissions(&stage, fs::Permissions::from_mode(0o600)).unwrap();
        let replacement_inode = fs::metadata(&stage).unwrap().ino();
        assert_ne!(original_inode, replacement_inode);
        release_tx.send(()).unwrap();

        let error = worker.join().unwrap().unwrap_err();
        // The first binding mismatch is retryable; the next fail-closed scan
        // may report either stale binding or malformed ambiguous evidence,
        // depending on directory iteration order. Preservation is the
        // invariant that proves no pre-revalidation truncate occurred.
        assert!(matches!(
            error.raw_os_error(),
            Some(libc::EINVAL) | Some(libc::ESTALE)
        ));
        assert_eq!(fs::read(&detached).unwrap(), original_bytes);
        assert_eq!(fs::metadata(&detached).unwrap().ino(), original_inode);
        assert_eq!(fs::read(&stage).unwrap(), replacement_bytes);
        assert_eq!(fs::metadata(&stage).unwrap().ino(), replacement_inode);
        assert!(!root_path.exists());
    }

    fn assert_cleanup_restore_retirement_retry(fault: CleanupDecisionFault) {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let target = fs::metadata(&root_path).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let validation_fault =
            CleanupFaultOverride::set(CleanupFault::AfterAcquisitionValidation(libc::ESTALE));
        let retirement_fault = CleanupDecisionFaultOverride::set(fault);

        let error = parent.remove_owned_child("root").unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::EIO), "{error:?}");
        drop(retirement_fault);
        drop(validation_fault);
        drop(parent);
        let restored = fs::metadata(&root_path).unwrap();
        assert_eq!(restored.dev(), target.dev());
        assert_eq!(restored.ino(), target.ino());
        assert_eq!(restored.permissions().mode() & 0o777, 0o500);
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"owned");
        let namespace = physical.join(".mac-worker-rooted-fs");
        let names = fs::read_dir(&namespace)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        match fault {
            CleanupDecisionFault::AfterRestoreSync(_) => {
                assert_eq!(names.len(), 2);
                assert!(
                    names
                        .iter()
                        .any(|name| { name.as_bytes().starts_with(b"cleanup-intent-v1-") })
                );
                assert!(
                    names
                        .iter()
                        .any(|name| { name.as_bytes().starts_with(b"cleanup-decision-v1-") })
                );
            }
            CleanupDecisionFault::AfterDecisionRetire(_) => {
                assert_eq!(names.len(), 1);
                assert!(names[0].as_bytes().starts_with(b"cleanup-intent-v1-"));
            }
            CleanupDecisionFault::AfterIntentRetire(_) => assert!(names.is_empty()),
            _ => unreachable!(),
        }

        let parent = RootedDir::open(&physical).unwrap();
        match fault {
            CleanupDecisionFault::AfterRestoreSync(_)
            | CleanupDecisionFault::AfterDecisionRetire(_) => {
                assert!(parent.resume_pending_owned_child_cleanup("root").unwrap());
                let restored = fs::metadata(&root_path).unwrap();
                assert_eq!(restored.dev(), target.dev());
                assert_eq!(restored.ino(), target.ino());
                assert_eq!(restored.permissions().mode() & 0o777, 0o500);
                assert_eq!(fs::read(root_path.join("value")).unwrap(), b"owned");
                assert_eq!(fs::read_dir(&namespace).unwrap().count(), 0);
                assert!(!parent.resume_pending_owned_child_cleanup("root").unwrap());
            }
            CleanupDecisionFault::AfterIntentRetire(_) => {
                assert!(!parent.resume_pending_owned_child_cleanup("root").unwrap());
                let restored = fs::metadata(&root_path).unwrap();
                assert_eq!(restored.dev(), target.dev());
                assert_eq!(restored.ino(), target.ino());
                assert_eq!(restored.permissions().mode() & 0o777, 0o500);
                assert_eq!(fs::read(root_path.join("value")).unwrap(), b"owned");
                assert_eq!(fs::read_dir(&namespace).unwrap().count(), 0);
                parent.remove_owned_child("root").unwrap();
                assert!(!root_path.exists());
            }
            _ => unreachable!(),
        }
    }

    fn cleanup_restore_sync_trace() -> Vec<super::CleanupRestoreSyncBoundary> {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let validation =
            CleanupFaultOverride::set(CleanupFault::AfterAcquisitionValidation(libc::ESTALE));
        let trace = super::CleanupRestoreSyncTraceOverride::set();

        let error = parent.remove_owned_child("root").unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        let events = trace.snapshot();
        drop(trace);
        drop(validation);
        let restored = fs::metadata(&root_path).unwrap();
        assert_eq!(restored.permissions().mode() & 0o777, 0o500);
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"owned");
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
        events
    }

    #[test]
    fn cleanup_restore_syncs_destination_before_source() {
        // The destination directory is the authoritative crash-recovery
        // boundary for operation -> public. Its rename must be durable before
        // the source directory is synced.
        let events = cleanup_restore_sync_trace();
        assert_eq!(
            &events[events.len().saturating_sub(2)..],
            [
                super::CleanupRestoreSyncBoundary::Destination,
                super::CleanupRestoreSyncBoundary::Source,
            ]
        );
    }

    #[test]
    fn cleanup_restore_capture_syncs_destination_before_source() {
        // After a valid capture exchange the payload lives in
        // operation.directory. That destination must be durable before the
        // source parent records removal of the old binding.
        let events = cleanup_restore_sync_trace();
        assert_eq!(
            &events[..2],
            [
                super::CleanupRestoreSyncBoundary::CaptureDestination,
                super::CleanupRestoreSyncBoundary::CaptureSource,
            ]
        );
    }

    fn assert_cleanup_restore_move_sync_fault_recovers(fault: CleanupDecisionFault) {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"exact-owned-bytes").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let target = fs::metadata(&root_path).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let validation =
            CleanupFaultOverride::set(CleanupFault::AfterAcquisitionValidation(libc::ESTALE));
        let sync_fault = CleanupDecisionFaultOverride::set(fault);

        let error = parent.remove_owned_child("root").unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::EIO), "{error:?}");
        drop(sync_fault);
        drop(validation);
        assert!(parent.resume_pending_owned_child_cleanup("root").unwrap());
        let restored = fs::metadata(&root_path).unwrap();
        assert_eq!(restored.dev(), target.dev());
        assert_eq!(restored.ino(), target.ino());
        assert_eq!(restored.permissions().mode() & 0o777, 0o500);
        assert_eq!(
            fs::read(root_path.join("value")).unwrap(),
            b"exact-owned-bytes"
        );
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
        assert!(!parent.resume_pending_owned_child_cleanup("root").unwrap());
    }

    #[test]
    fn cleanup_restore_recovers_after_destination_directory_sync() {
        assert_cleanup_restore_move_sync_fault_recovers(
            CleanupDecisionFault::AfterRestoreDestinationSync(libc::EIO),
        );
    }

    #[test]
    fn cleanup_restore_recovers_after_source_directory_sync() {
        assert_cleanup_restore_move_sync_fault_recovers(
            CleanupDecisionFault::AfterRestoreSourceSync(libc::EIO),
        );
    }

    #[test]
    fn cleanup_restore_retries_after_target_mode_sync() {
        assert_cleanup_restore_retirement_retry(CleanupDecisionFault::AfterRestoreSync(libc::EIO));
    }

    #[test]
    fn cleanup_restore_retries_after_decision_retirement() {
        assert_cleanup_restore_retirement_retry(CleanupDecisionFault::AfterDecisionRetire(
            libc::EIO,
        ));
    }

    #[test]
    fn cleanup_restore_retries_after_intent_retirement() {
        assert_cleanup_restore_retirement_retry(CleanupDecisionFault::AfterIntentRetire(libc::EIO));
    }

    #[test]
    fn cleanup_live_restore_error_survives_retryable_rollback() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let target = fs::metadata(&root_path).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let validation_fault =
            CleanupFaultOverride::set(CleanupFault::AfterAcquisitionValidation(libc::EPERM));
        let rollback_fault = CleanupDecisionFaultOverride::set(
            CleanupDecisionFault::DuringRestoreRollback(libc::EAGAIN),
        );

        let error = parent.remove_owned_child("root").unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::EPERM));
        assert!(
            super::TEST_CLEANUP_DECISION_FAULT
                .with(std::cell::Cell::get)
                .is_none()
        );
        let restored = fs::metadata(&root_path).unwrap();
        assert_eq!(restored.dev(), target.dev());
        assert_eq!(restored.ino(), target.ino());
        assert_eq!(restored.permissions().mode() & 0o777, 0o500);
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"owned");
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
        drop(rollback_fault);
        drop(validation_fault);
    }

    #[test]
    fn cleanup_live_restore_error_survives_retryable_decision_publication() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let target = fs::metadata(&root_path).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let validation_fault =
            CleanupFaultOverride::set(CleanupFault::AfterAcquisitionValidation(libc::EPERM));
        let publication_fault =
            CleanupDecisionFaultOverride::set(CleanupDecisionFault::AfterRename(libc::EAGAIN));

        let error = parent.remove_owned_child("root").unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::EPERM));
        assert!(
            super::TEST_CLEANUP_DECISION_FAULT
                .with(std::cell::Cell::get)
                .is_none()
        );
        let restored = fs::metadata(&root_path).unwrap();
        assert_eq!(restored.dev(), target.dev());
        assert_eq!(restored.ino(), target.ino());
        assert_eq!(restored.permissions().mode() & 0o777, 0o500);
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"owned");
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
        drop(publication_fault);
        drop(validation_fault);
    }

    #[test]
    fn cleanup_live_restore_error_survives_retryable_intent_retirement() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let target = fs::metadata(&root_path).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let validation_fault =
            CleanupFaultOverride::set(CleanupFault::AfterAcquisitionValidation(libc::EPERM));
        let retirement_fault = CleanupDecisionFaultOverride::set(
            CleanupDecisionFault::AfterIntentRetire(libc::EAGAIN),
        );

        let error = parent.remove_owned_child("root").unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::EPERM));
        let restored = fs::metadata(&root_path).unwrap();
        assert_eq!(restored.dev(), target.dev());
        assert_eq!(restored.ino(), target.ino());
        assert_eq!(restored.permissions().mode() & 0o777, 0o500);
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"owned");
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
        drop(retirement_fault);
        drop(validation_fault);
    }

    #[test]
    fn cleanup_recovered_restore_survives_retryable_intent_retirement() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let target = fs::metadata(&root_path).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let validation_fault =
            CleanupFaultOverride::set(CleanupFault::AfterAcquisitionValidation(libc::ESTALE));
        let first_retirement =
            CleanupDecisionFaultOverride::set(CleanupDecisionFault::AfterDecisionRetire(libc::EIO));
        assert_eq!(
            parent
                .remove_owned_child("root")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(first_retirement);
        drop(validation_fault);
        drop(parent);

        let parent = RootedDir::open(&physical).unwrap();
        let second_retirement = CleanupDecisionFaultOverride::set(
            CleanupDecisionFault::AfterIntentRetire(libc::EAGAIN),
        );
        assert!(parent.resume_pending_owned_child_cleanup("root").unwrap());

        let restored = fs::metadata(&root_path).unwrap();
        assert_eq!(restored.dev(), target.dev());
        assert_eq!(restored.ino(), target.ino());
        assert_eq!(restored.permissions().mode() & 0o777, 0o500);
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"owned");
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
        assert!(!parent.resume_pending_owned_child_cleanup("root").unwrap());
        drop(second_retirement);
    }

    #[test]
    fn cleanup_resume_only_consumes_restore_without_deleting_target() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let inode = fs::metadata(&root_path).unwrap().ino();
        let parent = RootedDir::open(&physical).unwrap();
        let validation_fault =
            CleanupFaultOverride::set(CleanupFault::AfterAcquisitionValidation(libc::ESTALE));
        let restore_fault =
            CleanupDecisionFaultOverride::set(CleanupDecisionFault::AfterRestoreSync(libc::EIO));
        assert_eq!(
            parent
                .remove_owned_child("root")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(restore_fault);
        drop(validation_fault);
        drop(parent);

        let parent = RootedDir::open(&physical).unwrap();
        assert!(parent.resume_pending_owned_child_cleanup("root").unwrap());
        let restored = fs::metadata(&root_path).unwrap();
        assert_eq!(restored.ino(), inode);
        assert_eq!(restored.permissions().mode() & 0o777, 0o500);
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
        assert!(!parent.resume_pending_owned_child_cleanup("root").unwrap());
    }

    #[test]
    fn cleanup_no_decision_restore_tail_rejects_journal_owned_mode() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let validation_fault =
            CleanupFaultOverride::set(CleanupFault::AfterAcquisitionValidation(libc::ESTALE));
        let retire_fault =
            CleanupDecisionFaultOverride::set(CleanupDecisionFault::AfterDecisionRetire(libc::EIO));
        assert_eq!(
            parent
                .remove_owned_child("root")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(retire_fault);
        drop(validation_fault);
        drop(parent);
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let namespace = physical.join(".mac-worker-rooted-fs");
        let before = fs::read_dir(&namespace)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(before.len(), 1);

        let error = RootedDir::open(&physical)
            .unwrap()
            .remove_owned_child("root")
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert!(root_path.exists());
        assert_eq!(
            fs::metadata(&root_path).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::read_dir(namespace)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect::<Vec<_>>(),
            before
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

    fn assert_cleanup_capability_probe_retry(fault: CleanupFault, probe_present: bool) {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let before = fs::metadata(&root_path).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let fault = CleanupFaultOverride::set(fault);

        let error = parent.remove_owned_child("root").unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::EIO));
        drop(fault);
        drop(parent);
        let after = fs::metadata(&root_path).unwrap();
        assert_eq!(after.dev(), before.dev());
        assert_eq!(after.ino(), before.ino());
        assert_eq!(after.ctime(), before.ctime());
        assert_eq!(after.ctime_nsec(), before.ctime_nsec());
        assert_eq!(after.permissions().mode() & 0o777, 0o500);
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"owned");
        let namespace = physical.join(".mac-worker-rooted-fs");
        let top = fs::read_dir(&namespace)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(top.len(), 1);
        assert!(
            top[0]
                .file_name()
                .unwrap()
                .as_bytes()
                .starts_with(b"cleanup-op-v1-")
        );
        assert_eq!(
            top[0].join("cleanup-capability-probe-v1").exists(),
            probe_present
        );

        RootedDir::open(&physical)
            .unwrap()
            .remove_owned_child("root")
            .unwrap();
        assert!(!root_path.exists());
        assert_eq!(fs::read_dir(namespace).unwrap().count(), 0);
    }

    #[test]
    fn cleanup_capability_probe_recovers_mid_probe() {
        assert_cleanup_capability_probe_retry(
            CleanupFault::DuringCleanupCapabilityProbe(5, libc::EIO),
            true,
        );
    }

    #[test]
    fn cleanup_capability_probe_all_durable_transitions_converge() {
        for transition in 1..=24 {
            assert_cleanup_capability_probe_retry(
                CleanupFault::DuringCleanupCapabilityProbe(transition, libc::EIO),
                transition < 24,
            );
        }
    }

    #[test]
    fn cleanup_capability_probe_recovers_after_final_sync() {
        assert_cleanup_capability_probe_retry(
            CleanupFault::AfterCleanupCapabilityProbeSync(libc::EIO),
            false,
        );
    }

    fn assert_cleanup_capability_unsupported_fails_before_public_mutation<T>(_guard: T) {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let before = fs::metadata(&root_path).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let error = parent.remove_owned_child("root").unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ENOTSUP));
        let after = fs::metadata(&root_path).unwrap();
        assert_eq!(after.dev(), before.dev());
        assert_eq!(after.ino(), before.ino());
        assert_eq!(after.ctime(), before.ctime());
        assert_eq!(after.ctime_nsec(), before.ctime_nsec());
        assert_eq!(after.permissions().mode() & 0o777, 0o500);
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"owned");
        let namespace = physical.join(".mac-worker-rooted-fs");
        assert_eq!(fs::read_dir(namespace).unwrap().count(), 0);
    }

    #[test]
    fn cleanup_capability_probe_regular_no_replace_unsupported() {
        assert_cleanup_capability_unsupported_fails_before_public_mutation(
            RenameNoReplaceOverride::fail_cross_directory_with(libc::ENOTSUP),
        );
    }

    #[test]
    fn cleanup_capability_probe_directory_no_replace_unsupported() {
        assert_cleanup_capability_unsupported_fails_before_public_mutation(
            RenameNoReplaceOverride::fail_directory_with(libc::ENOTSUP),
        );
    }

    #[test]
    fn cleanup_capability_probe_directory_exchange_unsupported() {
        assert_cleanup_capability_unsupported_fails_before_public_mutation(
            AtomicRenameCapabilityOverride::set(AtomicRenameCapability::NoExchange),
        );
    }

    fn cleanup_capability_probe_fixture(
        transition: usize,
    ) -> (tempfile::TempDir, PathBuf, PathBuf) {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let fault = CleanupFaultOverride::set(CleanupFault::DuringCleanupCapabilityProbe(
            transition,
            libc::EIO,
        ));
        assert_eq!(
            parent
                .remove_owned_child("root")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(fault);
        drop(parent);
        let operation = fs::read_dir(physical.join(".mac-worker-rooted-fs"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        (fixture, physical, operation)
    }

    fn assert_cleanup_capability_probe_malformed(physical: &Path) {
        let error = RootedDir::open(physical)
            .unwrap()
            .remove_owned_child("root")
            .unwrap_err();
        assert!(matches!(
            error.raw_os_error(),
            Some(libc::EINVAL) | Some(libc::ESTALE)
        ));
        assert_eq!(fs::read(physical.join("root/value")).unwrap(), b"owned");
        assert_eq!(
            fs::metadata(physical.join("root"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o500
        );
    }

    #[test]
    fn cleanup_capability_probe_preserves_substituted_known_roles() {
        let (_fixture, physical, operation) = cleanup_capability_probe_fixture(5);
        let source = operation
            .join("cleanup-capability-probe-v1/left-v1")
            .join("regular-no-replace-source-v1");
        fs::set_permissions(&source, fs::Permissions::from_mode(0o644)).unwrap();
        let inode = fs::metadata(&source).unwrap().ino();
        assert_cleanup_capability_probe_malformed(&physical);
        assert_eq!(fs::metadata(&source).unwrap().ino(), inode);
        assert_eq!(
            fs::metadata(&source).unwrap().permissions().mode() & 0o777,
            0o644
        );

        let (_fixture, physical, operation) = cleanup_capability_probe_fixture(5);
        let source = operation
            .join("cleanup-capability-probe-v1/left-v1")
            .join("regular-no-replace-source-v1");
        fs::remove_file(&source).unwrap();
        fs::create_dir(&source).unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o700)).unwrap();
        let inode = fs::metadata(&source).unwrap().ino();
        assert_cleanup_capability_probe_malformed(&physical);
        assert!(source.is_dir());
        assert_eq!(fs::metadata(&source).unwrap().ino(), inode);

        let (_fixture, physical, operation) = cleanup_capability_probe_fixture(5);
        let left = operation.join("cleanup-capability-probe-v1/left-v1");
        let right = operation.join("cleanup-capability-probe-v1/right-v1");
        let source = left.join("regular-no-replace-source-v1");
        let destination = right.join("regular-no-replace-destination-v1");
        fs::remove_file(&destination).unwrap();
        fs::hard_link(&source, &destination).unwrap();
        let inode = fs::metadata(&source).unwrap().ino();
        assert_cleanup_capability_probe_malformed(&physical);
        assert_eq!(fs::metadata(&source).unwrap().ino(), inode);
        assert_eq!(fs::metadata(&destination).unwrap().ino(), inode);
        assert_eq!(fs::metadata(&source).unwrap().nlink(), 2);

        let (_fixture, physical, operation) = cleanup_capability_probe_fixture(11);
        let directory = operation
            .join("cleanup-capability-probe-v1/left-v1")
            .join("directory-no-replace-source-v1");
        let payload = directory.join("payload-v1");
        fs::write(&payload, b"preserve").unwrap();
        fs::set_permissions(&payload, fs::Permissions::from_mode(0o600)).unwrap();
        let inode = fs::metadata(&payload).unwrap().ino();
        assert_cleanup_capability_probe_malformed(&physical);
        assert_eq!(fs::read(&payload).unwrap(), b"preserve");
        assert_eq!(fs::metadata(&payload).unwrap().ino(), inode);
    }

    #[test]
    fn cleanup_capability_probe_preserves_malformed_subtree() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let fault =
            CleanupFaultOverride::set(CleanupFault::DuringCleanupCapabilityProbe(5, libc::EIO));
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
            .next()
            .unwrap()
            .unwrap()
            .path();
        let unexpected = operation
            .join("cleanup-capability-probe-v1")
            .join("left-v1")
            .join("unexpected-v1");
        fs::write(&unexpected, b"preserve").unwrap();
        fs::set_permissions(&unexpected, fs::Permissions::from_mode(0o600)).unwrap();
        let inode = fs::metadata(&unexpected).unwrap().ino();

        let error = RootedDir::open(&physical)
            .unwrap()
            .remove_owned_child("root")
            .unwrap_err();

        assert!(matches!(
            error.raw_os_error(),
            Some(libc::EINVAL) | Some(libc::ESTALE)
        ));
        assert_eq!(fs::read(&unexpected).unwrap(), b"preserve");
        assert_eq!(fs::metadata(&unexpected).unwrap().ino(), inode);
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"owned");
    }

    #[test]
    fn cleanup_intent_tree_bootstrap_retry_after_placeholder_fsync() {
        assert_cleanup_intent_tree_bootstrap_retry(CleanupFault::AfterCleanupPlaceholder(
            libc::EIO,
        ));
    }

    #[test]
    fn cleanup_intent_tree_bootstrap_retry_after_placeholder_object_fsync() {
        assert_cleanup_intent_tree_bootstrap_retry(
            CleanupFault::AfterCleanupPlaceholderObjectSync(libc::EIO),
        );
    }

    #[test]
    fn cleanup_intent_tree_bootstrap_retry_after_partial_stage_sync() {
        assert_cleanup_intent_tree_bootstrap_retry(CleanupFault::DuringCleanupIntentWrite(
            libc::EIO,
        ));
    }

    #[test]
    fn cleanup_intent_complete_stage_is_synced_before_adoption() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let write_fault =
            CleanupFaultOverride::set(CleanupFault::AfterCleanupIntentWriteBeforeSync(libc::EIO));
        assert_eq!(
            parent
                .remove_owned_child("root")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(write_fault);
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
        let bytes = fs::read(&stage).unwrap();
        cleanup_parse_canonical_json::<CleanupIntentV1>(&bytes).unwrap();
        let inode = fs::metadata(&stage).unwrap().ino();

        let parent = RootedDir::open(&physical).unwrap();
        let adoption_fault =
            CleanupFaultOverride::set(CleanupFault::AfterCleanupIntentAdoptionSync(libc::EIO));
        assert_eq!(
            parent
                .remove_owned_child("root")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        assert_eq!(fs::read(&stage).unwrap(), bytes);
        assert_eq!(fs::metadata(&stage).unwrap().ino(), inode);
        drop(adoption_fault);
        drop(parent);

        RootedDir::open(&physical)
            .unwrap()
            .remove_owned_child("root")
            .unwrap();
        assert!(!root_path.exists());
        assert_eq!(fs::read_dir(namespace).unwrap().count(), 0);
    }

    fn cleanup_intent_stage_fixture(complete: bool) -> (tempfile::TempDir, PathBuf, PathBuf) {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let fault = CleanupFaultOverride::set(if complete {
            CleanupFault::AfterCleanupIntentSync(libc::EIO)
        } else {
            CleanupFault::DuringCleanupIntentWrite(libc::EIO)
        });
        assert_eq!(
            parent
                .remove_owned_child("root")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(fault);
        drop(parent);
        let operation = fs::read_dir(physical.join(".mac-worker-rooted-fs"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .as_bytes()
                    .starts_with(b"cleanup-op-v1-")
            })
            .unwrap();
        let stage = fs::read_dir(operation)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .as_bytes()
                    .starts_with(b"cleanup-intent-stage-v1-")
            })
            .unwrap();
        (fixture, physical, stage)
    }

    #[test]
    fn cleanup_intent_stage_repair_preserves_complete_schema_invalid_bytes() {
        // Catches treating complete JSON with missing, unknown, or wrongly
        // typed fields as a crash-truncated record and replacing it.
        for bytes in [
            b"{}".as_slice(),
            br#"{"unknown":1}"#.as_slice(),
            br#"{"version":"wrong"}"#.as_slice(),
        ] {
            let (_fixture, physical, stage) = cleanup_intent_stage_fixture(false);
            fs::write(&stage, bytes).unwrap();
            let inode = fs::metadata(&stage).unwrap().ino();
            let parent = RootedDir::open(&physical).unwrap();

            let error = parent.remove_owned_child("root").unwrap_err();

            assert!(matches!(
                error.raw_os_error(),
                Some(libc::EINVAL) | Some(libc::ESTALE)
            ));
            assert_eq!(fs::read(&stage).unwrap(), bytes);
            assert_eq!(fs::metadata(&stage).unwrap().ino(), inode);
            assert_eq!(fs::read(physical.join("root/value")).unwrap(), b"owned");
        }
    }

    #[test]
    fn cleanup_intent_stage_repair_preserves_non_prefix_eof_bytes() {
        // `[` is syntactically incomplete JSON, but cannot be a sequential
        // prefix of the object record this exact bound slot publishes.
        let (_fixture, physical, stage) = cleanup_intent_stage_fixture(false);
        fs::write(&stage, b"[").unwrap();
        let inode = fs::metadata(&stage).unwrap().ino();
        let parent = RootedDir::open(&physical).unwrap();

        let error = parent.remove_owned_child("root").unwrap_err();

        assert!(matches!(
            error.raw_os_error(),
            Some(libc::EINVAL) | Some(libc::ESTALE)
        ));
        assert_eq!(fs::read(&stage).unwrap(), b"[");
        assert_eq!(fs::metadata(&stage).unwrap().ino(), inode);
        assert_eq!(fs::read(physical.join("root/value")).unwrap(), b"owned");
    }

    #[test]
    fn cleanup_intent_stage_repair_preserves_valid_noncanonical_bytes() {
        // A complete record with trailing whitespace is valid JSON but not
        // the protocol's canonical byte representation and must stay intact.
        let (_fixture, physical, stage) = cleanup_intent_stage_fixture(true);
        let mut bytes = fs::read(&stage).unwrap();
        bytes.push(b' ');
        fs::write(&stage, &bytes).unwrap();
        let inode = fs::metadata(&stage).unwrap().ino();
        let parent = RootedDir::open(&physical).unwrap();

        let error = parent.remove_owned_child("root").unwrap_err();

        assert!(matches!(
            error.raw_os_error(),
            Some(libc::EINVAL) | Some(libc::ESTALE)
        ));
        assert_eq!(fs::read(&stage).unwrap(), bytes);
        assert_eq!(fs::metadata(&stage).unwrap().ino(), inode);
        assert_eq!(fs::read(physical.join("root/value")).unwrap(), b"owned");
    }

    #[test]
    fn cleanup_intent_stage_empty_and_bound_canonical_prefixes_converge() {
        // Catches rejecting crash states before serde has enough bytes to
        // identify any field, or at the fixed-fields/quarantine boundary.
        for prefix_ordinal in 0..5 {
            let (_fixture, physical, stage) = cleanup_intent_stage_fixture(true);
            let canonical = fs::read(&stage).unwrap();
            assert_eq!(canonical.first(), Some(&b'{'));
            let quarantine_field = b"\"quarantine\":\"";
            let field_start = canonical
                .windows(quarantine_field.len())
                .position(|window| window == quarantine_field)
                .unwrap();
            let value_start = field_start + quarantine_field.len();
            let prefix_len = [0, 1, field_start, value_start - 1, value_start][prefix_ordinal];
            fs::write(&stage, &canonical[..prefix_len]).unwrap();

            RootedDir::open(&physical)
                .unwrap()
                .remove_owned_child("root")
                .unwrap();

            assert!(!physical.join("root").exists());
            assert_eq!(
                fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                    .unwrap()
                    .count(),
                0
            );
        }
    }

    fn cleanup_intent_quarantine_offset(bytes: &[u8]) -> (usize, CleanupIntentV1) {
        let intent: CleanupIntentV1 = cleanup_parse_canonical_json(bytes).unwrap();
        let quarantine = intent.quarantine.as_bytes();
        let offset = bytes
            .windows(quarantine.len())
            .position(|window| window == quarantine)
            .unwrap();
        (offset, intent)
    }

    #[test]
    fn cleanup_intent_stage_valid_uuid_truncation_converges() {
        // Catches requiring a complete random quarantine name before an
        // otherwise attributable sequential stage prefix may be rebuilt.
        let (_fixture, physical, stage) = cleanup_intent_stage_fixture(true);
        let canonical = fs::read(&stage).unwrap();
        let (quarantine_offset, intent) = cleanup_intent_quarantine_offset(&canonical);
        let uuid_prefix_len = "cleanup-tree-v1-".len() + 20;
        assert!(uuid_prefix_len < intent.quarantine.len());
        let truncated = &canonical[..quarantine_offset + uuid_prefix_len];
        assert!(matches!(
            super::cleanup_parse_stage_json::<CleanupIntentV1>(truncated).unwrap(),
            super::CleanupStageJson::Truncated
        ));
        fs::write(&stage, truncated).unwrap();

        RootedDir::open(&physical)
            .unwrap()
            .remove_owned_child("root")
            .unwrap();

        assert!(!physical.join("root").exists());
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn cleanup_intent_stage_preserves_bad_uuid_version_and_hex_prefixes() {
        // Catches broad EOF repair that would erase non-v4 or non-hex
        // quarantine evidence merely because the JSON string is unfinished.
        for (uuid_index, replacement) in [(14, b'5'), (6, b'g')] {
            let (_fixture, physical, stage) = cleanup_intent_stage_fixture(true);
            let canonical = fs::read(&stage).unwrap();
            let (quarantine_offset, intent) = cleanup_intent_quarantine_offset(&canonical);
            let uuid_offset = quarantine_offset + "cleanup-tree-v1-".len();
            let mut malformed = canonical[..uuid_offset + usize::max(uuid_index + 1, 20)].to_vec();
            assert_eq!(
                intent.quarantine.as_bytes()["cleanup-tree-v1-".len() + 14],
                b'4'
            );
            malformed[uuid_offset + uuid_index] = replacement;
            assert!(matches!(
                super::cleanup_parse_stage_json::<CleanupIntentV1>(&malformed).unwrap(),
                super::CleanupStageJson::Truncated
            ));
            fs::write(&stage, &malformed).unwrap();
            let inode = fs::metadata(&stage).unwrap().ino();

            let error = RootedDir::open(&physical)
                .unwrap()
                .remove_owned_child("root")
                .unwrap_err();

            assert!(matches!(
                error.raw_os_error(),
                Some(libc::EINVAL) | Some(libc::ESTALE)
            ));
            assert_eq!(fs::read(&stage).unwrap(), malformed);
            assert_eq!(fs::metadata(&stage).unwrap().ino(), inode);
            assert_eq!(fs::read(physical.join("root/value")).unwrap(), b"owned");
        }
    }

    #[test]
    fn cleanup_partial_intent_preserves_evidence_when_public_target_is_absent() {
        // Catches rebuilding a partial intent after its required public A has
        // disappeared, which would invent a new quarantine generation.
        let (_fixture, physical, stage) = cleanup_intent_stage_fixture(false);
        let bytes = fs::read(&stage).unwrap();
        let inode = fs::metadata(&stage).unwrap().ino();
        let root_path = physical.join("root");
        let detached_a = physical.join("detached-a");
        let a = fs::metadata(&root_path).unwrap();
        // macOS requires write permission on a directory being renamed. Put
        // the detached fixture back in the intent's recorded mode before the
        // retry observes it.
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        fs::rename(&root_path, &detached_a).unwrap();
        fs::set_permissions(&detached_a, fs::Permissions::from_mode(0o500)).unwrap();

        let error = RootedDir::open(&physical)
            .unwrap()
            .remove_owned_child("root")
            .unwrap_err();

        assert!(matches!(
            error.raw_os_error(),
            Some(libc::EINVAL) | Some(libc::ENOENT) | Some(libc::ESTALE)
        ));
        assert_eq!(fs::read(&stage).unwrap(), bytes);
        assert_eq!(fs::metadata(&stage).unwrap().ino(), inode);
        assert!(!root_path.exists());
        let detached = fs::metadata(&detached_a).unwrap();
        assert_eq!(detached.dev(), a.dev());
        assert_eq!(detached.ino(), a.ino());
        assert_eq!(detached.permissions().mode() & 0o777, 0o500);
        assert_eq!(fs::read(detached_a.join("value")).unwrap(), b"owned");
    }

    #[test]
    fn cleanup_partial_intent_preserves_public_replacement() {
        // Catches authorizing B from a partial stage that was bound to A.
        let (_fixture, physical, stage) = cleanup_intent_stage_fixture(false);
        let bytes = fs::read(&stage).unwrap();
        let inode = fs::metadata(&stage).unwrap().ino();
        let root_path = physical.join("root");
        let detached_a = physical.join("detached-a");
        let a = fs::metadata(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        fs::rename(&root_path, &detached_a).unwrap();
        fs::set_permissions(&detached_a, fs::Permissions::from_mode(0o500)).unwrap();
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("sentinel-b"), b"replacement").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let b_inode = fs::metadata(&root_path).unwrap().ino();
        assert_ne!(a.ino(), b_inode);

        let error = RootedDir::open(&physical)
            .unwrap()
            .remove_owned_child("root")
            .unwrap_err();

        assert!(matches!(
            error.raw_os_error(),
            Some(libc::EINVAL) | Some(libc::ESTALE)
        ));
        assert_eq!(fs::read(&stage).unwrap(), bytes);
        assert_eq!(fs::metadata(&stage).unwrap().ino(), inode);
        assert_eq!(fs::metadata(&root_path).unwrap().ino(), b_inode);
        assert_eq!(
            fs::read(root_path.join("sentinel-b")).unwrap(),
            b"replacement"
        );
        let detached = fs::metadata(&detached_a).unwrap();
        assert_eq!(detached.dev(), a.dev());
        assert_eq!(detached.ino(), a.ino());
        assert_eq!(detached.permissions().mode() & 0o777, 0o500);
        assert_eq!(fs::read(detached_a.join("value")).unwrap(), b"owned");
    }

    #[test]
    fn cleanup_partial_intent_preserves_unexpected_operation_child() {
        // Catches deleting an unknown operation child while trying to repair
        // the known deterministic intent-stage role.
        let (_fixture, physical, stage) = cleanup_intent_stage_fixture(false);
        let stage_bytes = fs::read(&stage).unwrap();
        let stage_inode = fs::metadata(&stage).unwrap().ino();
        let unexpected = stage.parent().unwrap().join("unexpected-v1");
        fs::write(&unexpected, b"preserve").unwrap();
        fs::set_permissions(&unexpected, fs::Permissions::from_mode(0o600)).unwrap();
        let unexpected_inode = fs::metadata(&unexpected).unwrap().ino();

        let error = RootedDir::open(&physical)
            .unwrap()
            .remove_owned_child("root")
            .unwrap_err();

        assert!(matches!(
            error.raw_os_error(),
            Some(libc::EINVAL) | Some(libc::ESTALE)
        ));
        assert_eq!(fs::read(&stage).unwrap(), stage_bytes);
        assert_eq!(fs::metadata(&stage).unwrap().ino(), stage_inode);
        assert_eq!(fs::read(&unexpected).unwrap(), b"preserve");
        assert_eq!(fs::metadata(&unexpected).unwrap().ino(), unexpected_inode);
        assert_eq!(fs::read(physical.join("root/value")).unwrap(), b"owned");
    }

    #[test]
    fn cleanup_intent_stage_repeated_repairs_converge() {
        // Catches a repair path that handles one interrupted replacement but
        // cannot attribute and rebuild the deterministic slot again.
        let (_fixture, physical, initial_stage) = cleanup_intent_stage_fixture(false);
        let parent = RootedDir::open(&physical).unwrap();
        let root_path = physical.join("root");
        let target = fs::metadata(&root_path).unwrap();
        for _ in 0..2 {
            let fault =
                CleanupFaultOverride::set(CleanupFault::DuringCleanupIntentWrite(libc::EIO));
            let error = parent.remove_owned_child("root").unwrap_err();
            assert_eq!(error.raw_os_error(), Some(libc::EIO));
            drop(fault);
            let bytes = fs::read(&initial_stage).unwrap();
            assert!(!bytes.is_empty());
            assert!(matches!(
                super::cleanup_parse_stage_json::<CleanupIntentV1>(&bytes).unwrap(),
                super::CleanupStageJson::Truncated
            ));
            let current = fs::metadata(&root_path).unwrap();
            assert_eq!(current.dev(), target.dev());
            assert_eq!(current.ino(), target.ino());
            assert_eq!(current.permissions().mode() & 0o777, 0o500);
            assert_eq!(fs::read(root_path.join("value")).unwrap(), b"owned");
        }

        parent.remove_owned_child("root").unwrap();
        assert!(!physical.join("root").exists());
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
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
    fn cleanup_intent_publication_holds_inode_lock_through_directory_syncs() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();

        let (renamed_tx, renamed_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let publisher_physical = physical.clone();
        let publisher = std::thread::spawn(move || {
            let _handoff = super::CleanupIntentHandoffOverride::set(renamed_tx, release_rx);
            let parent = RootedDir::open(&publisher_physical).unwrap();
            let parent_metadata = super::stat_fd(parent.root.as_raw_fd()).unwrap();
            let parent_identity = super::FileIdentity::from_stat(&parent_metadata);
            let namespace = PrivateNamespace::select_for_cleanup(
                parent.root.as_raw_fd(),
                parent_metadata.st_dev,
                &[],
            )
            .unwrap();
            let component = std::ffi::CString::new("root").unwrap();
            let target = super::FileIdentity::from_stat(
                &stat_at(parent.root.as_raw_fd(), &component).unwrap(),
            );
            super::publish_tree_cleanup_intent(
                &namespace,
                parent.root.as_raw_fd(),
                parent_identity,
                &component,
                target,
                0o500,
            )
        });
        renamed_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();

        let observer = RootedDir::open(&physical).unwrap();
        let parent_metadata = super::stat_fd(observer.root.as_raw_fd()).unwrap();
        let parent_identity = super::FileIdentity::from_stat(&parent_metadata);
        let namespace = PrivateNamespace::select_for_cleanup(
            observer.root.as_raw_fd(),
            parent_metadata.st_dev,
            &[],
        )
        .unwrap();
        let key = super::cleanup_key(parent_identity, b"root");
        let intent_name = super::cleanup_intent_name(&key).unwrap();
        let (probe, _bytes, canonical_identity) = super::open_cleanup_record(
            namespace.directory.as_raw_fd(),
            &intent_name,
            parent_identity.device,
            false,
        )
        .unwrap();
        super::clear_errno();
        let probe_result = unsafe { libc::flock(probe.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        let probe_errno = super::current_errno();
        if probe_result == 0 {
            assert_eq!(unsafe { libc::flock(probe.as_raw_fd(), libc::LOCK_UN) }, 0);
        }

        let waiter_physical = physical.clone();
        let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
        let (loaded_tx, loaded_rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            let parent = RootedDir::open(&waiter_physical).unwrap();
            let parent_metadata = super::stat_fd(parent.root.as_raw_fd()).unwrap();
            let parent_identity = super::FileIdentity::from_stat(&parent_metadata);
            let namespace = PrivateNamespace::select_for_cleanup(
                parent.root.as_raw_fd(),
                parent_metadata.st_dev,
                &[],
            )
            .unwrap();
            waiting_tx.send(()).unwrap();
            let loaded = super::find_cleanup_intent(&namespace, parent_identity, b"root")
                .unwrap()
                .unwrap();
            loaded_tx.send(loaded.identity).unwrap();
        });
        waiting_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();

        release_tx.send(()).unwrap();
        publisher.join().unwrap().unwrap();
        assert_eq!(
            loaded_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap(),
            canonical_identity
        );
        waiter.join().unwrap();
        assert_eq!(probe_result, -1, "publisher did not retain the intent lock");
        assert!(probe_errno == libc::EAGAIN || probe_errno == libc::EWOULDBLOCK);

        drop(probe);
        drop(namespace);
        drop(observer);
        RootedDir::open(&physical)
            .unwrap()
            .remove_owned_child("root")
            .unwrap();
        assert!(!root_path.exists());
    }

    fn assert_cleanup_terminal_snapshot_holds_namespace_lock(phase: CleanupTerminalPhase) {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        if phase == CleanupTerminalPhase::Final {
            fs::create_dir(&root_path).unwrap();
            fs::write(root_path.join("value"), b"owned").unwrap();
            fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        }
        let observer = RootedDir::open(&physical).unwrap();
        let parent_metadata = super::stat_fd(observer.root.as_raw_fd()).unwrap();
        let namespace = PrivateNamespace::select_for_cleanup(
            observer.root.as_raw_fd(),
            parent_metadata.st_dev,
            &[],
        )
        .unwrap();
        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker_physical = physical.clone();
        let worker = std::thread::spawn(move || {
            let _handoff =
                super::CleanupTerminalHandoffOverride::set(phase, reached_tx, release_rx);
            RootedDir::open(&worker_physical)
                .unwrap()
                .remove_owned_child("root")
        });
        reached_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();

        super::clear_errno();
        let probe_result = unsafe {
            libc::flock(
                namespace.directory.as_raw_fd(),
                libc::LOCK_EX | libc::LOCK_NB,
            )
        };
        let probe_errno = super::current_errno();
        if probe_result == 0 {
            assert_eq!(
                unsafe { libc::flock(namespace.directory.as_raw_fd(), libc::LOCK_UN) },
                0
            );
        }
        release_tx.send(()).unwrap();
        worker.join().unwrap().unwrap();

        assert_eq!(probe_result, -1, "terminal snapshot was not linearized");
        assert!(probe_errno == libc::EAGAIN || probe_errno == libc::EWOULDBLOCK);
        assert!(!root_path.exists());
    }

    #[test]
    fn cleanup_initial_absence_snapshot_holds_namespace_lock() {
        assert_cleanup_terminal_snapshot_holds_namespace_lock(CleanupTerminalPhase::Initial);
    }

    #[test]
    fn cleanup_final_absence_snapshot_holds_namespace_lock() {
        assert_cleanup_terminal_snapshot_holds_namespace_lock(CleanupTerminalPhase::Final);
    }

    #[test]
    fn cleanup_intent_retirement_is_atomic_through_final_directory_sync_cross_process() {
        const ROLE_ENV: &str = "MAC_WORKER_CLEANUP_RETIRE_OBSERVER_ROLE";
        const ROOT_ENV: &str = "MAC_WORKER_CLEANUP_RETIRE_OBSERVER_ROOT";
        const STREAM_FD_ENV: &str = "MAC_WORKER_CLEANUP_RETIRE_OBSERVER_STREAM_FD";
        const TEST_NAME: &str = "rooted_fs::tests::cleanup_intent_retirement_is_atomic_through_final_directory_sync_cross_process";

        if std::env::var_os(ROLE_ENV).is_some() {
            let physical = std::path::PathBuf::from(std::env::var_os(ROOT_ENV).unwrap());
            let descriptor = std::env::var(STREAM_FD_ENV)
                .unwrap()
                .parse::<libc::c_int>()
                .unwrap();
            let mut stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(descriptor) };
            stream.write_all(b"W").unwrap();
            let mut go = [0];
            stream.read_exact(&mut go).unwrap();
            assert_eq!(go, [b'G']);
            let observed = RootedDir::open(&physical)
                .unwrap()
                .resume_pending_owned_child_cleanup("root")
                .unwrap();
            stream.write_all(&[b'D', u8::from(observed)]).unwrap();
            return;
        }

        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let prepare = CleanupFaultOverride::set(CleanupFault::BeforeFinalRootRemoval(libc::EIO));
        assert_eq!(
            parent
                .remove_owned_child("root")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(prepare);

        let (mut observer_control, observer_stream) =
            std::os::unix::net::UnixStream::pair().unwrap();
        let descriptor = observer_stream.as_raw_fd();
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        assert_ne!(flags, -1);
        assert_eq!(
            unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags & !libc::FD_CLOEXEC) },
            0
        );
        let observer = std::process::Command::new(std::env::current_exe().unwrap())
            .env(ROLE_ENV, "observer")
            .env(ROOT_ENV, &physical)
            .env(STREAM_FD_ENV, descriptor.to_string())
            .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        drop(observer_stream);
        let mut ready = [0];
        observer_control.read_exact(&mut ready).unwrap();
        assert_eq!(ready, [b'W']);

        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker_physical = physical.clone();
        let worker = std::thread::spawn(move || {
            let _handoff = super::CleanupIntentRetireHandoffOverride::set(reached_tx, release_rx);
            RootedDir::open(&worker_physical)
                .unwrap()
                .resume_pending_owned_child_cleanup("root")
        });
        reached_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        observer_control.write_all(b"G").unwrap();
        let mut early_probe = observer_control.try_clone().unwrap();
        early_probe
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        let mut observed = [0; 2];
        let early = match early_probe.read_exact(&mut observed) {
            Ok(()) => true,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                false
            }
            Err(error) => panic!("observer handoff failed: {error:?}"),
        };
        drop(early_probe);
        release_tx.send(()).unwrap();
        worker.join().unwrap().unwrap();
        if !early {
            observer_control.read_exact(&mut observed).unwrap();
        }
        let output = observer.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "observer stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        assert!(
            !early,
            "observer returned before final intent unlink was synced"
        );
        assert_eq!(observed, [b'D', 0]);
        assert!(!root_path.exists());
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn cleanup_owned_root_replacement_never_creates_replacement_bootstrap() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root = RootedDir::open(&root_path).unwrap();
        fs::rename(&root_path, physical.join("detached-owned")).unwrap();
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"replacement").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let replacement = fs::metadata(&root_path).unwrap();

        let error = root.remove_owned_tree().unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        let after = fs::metadata(&root_path).unwrap();
        assert_eq!(after.dev(), replacement.dev());
        assert_eq!(after.ino(), replacement.ino());
        assert_eq!(after.permissions().mode() & 0o777, 0o500);
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"replacement");
        let namespace = physical.join(".mac-worker-rooted-fs");
        assert_eq!(fs::read_dir(&namespace).unwrap().count(), 0);
        assert!(
            !RootedDir::open(&physical)
                .unwrap()
                .resume_pending_owned_child_cleanup("root")
                .unwrap()
        );
        assert_eq!(fs::read_dir(namespace).unwrap().count(), 0);
    }

    #[test]
    fn cleanup_intent_tree_bootstrap_two_retriers_converge() {
        let iterations = std::env::var("MAC_WORKER_CLEANUP_RETRIER_STRESS_ITERS")
            .ok()
            .map(|value| value.parse::<usize>().unwrap())
            .unwrap_or(128);
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let mut retriers = Vec::new();
        for _ in 0..2 {
            let physical = physical.clone();
            let barrier = barrier.clone();
            retriers.push(std::thread::spawn(move || {
                let parent = RootedDir::open(&physical).unwrap();
                let mut results = Vec::with_capacity(iterations);
                for _ in 0..iterations {
                    barrier.wait();
                    results.push(parent.remove_owned_child_inner("root"));
                    barrier.wait();
                }
                results
            }));
        }

        for iteration in 0..iterations {
            let root_path = physical.join("root");
            fs::create_dir(&root_path).unwrap();
            fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
            for index in 0..2 {
                fs::write(root_path.join(format!("value-{index}")), b"owned").unwrap();
            }
            barrier.wait();
            barrier.wait();
            assert!(!root_path.exists(), "iteration {iteration}");
            assert_eq!(
                fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                    .unwrap()
                    .count(),
                0,
                "iteration {iteration}"
            );
        }
        for retrier in retriers {
            for (iteration, result) in retrier.join().unwrap().into_iter().enumerate() {
                result.unwrap_or_else(|error| panic!("iteration {iteration}: {error:?}"));
            }
        }
    }

    #[test]
    fn cleanup_cross_process_public_retrier_adopts_paused_bootstrap() {
        const ROLE_ENV: &str = "MAC_WORKER_CLEANUP_CROSS_PROCESS_ROLE";
        const ROOT_ENV: &str = "MAC_WORKER_CLEANUP_CROSS_PROCESS_ROOT";
        const STREAM_FD_ENV: &str = "MAC_WORKER_CLEANUP_CROSS_PROCESS_STREAM_FD";
        const TEST_NAME: &str =
            "rooted_fs::tests::cleanup_cross_process_public_retrier_adopts_paused_bootstrap";

        if let Some(role) = std::env::var_os(ROLE_ENV) {
            let physical = std::path::PathBuf::from(std::env::var_os(ROOT_ENV).unwrap());
            let descriptor = std::env::var(STREAM_FD_ENV)
                .unwrap()
                .parse::<libc::c_int>()
                .unwrap();
            let mut stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(descriptor) };
            let parent = RootedDir::open(&physical).unwrap();
            match role.to_str().unwrap() {
                "publisher" => {
                    let _handoff = super::CleanupBootstrapHandoffOverride::set(stream);
                    parent.remove_owned_child("root").unwrap();
                }
                "retrier" => {
                    stream.write_all(b"W").unwrap();
                    let mut go = [0];
                    stream.read_exact(&mut go).unwrap();
                    assert_eq!(go, [b'G']);
                    parent.remove_owned_child("root").unwrap();
                }
                role => panic!("unexpected cleanup child role {role}"),
            }
            return;
        }

        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();

        let spawn_worker = |role: &str, stream: std::os::unix::net::UnixStream| {
            let descriptor = stream.as_raw_fd();
            let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
            assert_ne!(flags, -1);
            assert_eq!(
                unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags & !libc::FD_CLOEXEC) },
                0
            );
            let child = std::process::Command::new(std::env::current_exe().unwrap())
                .env(ROLE_ENV, role)
                .env(ROOT_ENV, &physical)
                .env(STREAM_FD_ENV, descriptor.to_string())
                .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            drop(stream);
            child
        };
        let assert_child_success = |role: &str, output: std::process::Output| {
            assert!(
                output.status.success(),
                "{role} stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        };

        let (mut retrier_control, retrier_stream) = std::os::unix::net::UnixStream::pair().unwrap();
        let retrier = spawn_worker("retrier", retrier_stream);
        let mut ready = [0];
        retrier_control.read_exact(&mut ready).unwrap();
        assert_eq!(ready, [b'W']);

        let (mut publisher_control, publisher_stream) =
            std::os::unix::net::UnixStream::pair().unwrap();
        let publisher = spawn_worker("publisher", publisher_stream);
        let mut handoff = [0];
        publisher_control.read_exact(&mut handoff).unwrap();
        assert_eq!(handoff, [b'H']);
        assert!(root_path.exists());
        let namespace = physical.join(".mac-worker-rooted-fs");
        let evidence = fs::read_dir(&namespace)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(evidence.len(), 1);
        assert!(evidence[0].as_bytes().starts_with(b"cleanup-op-v1-"));

        retrier_control.write_all(b"G").unwrap();
        assert_child_success("retrier", retrier.wait_with_output().unwrap());
        assert!(!root_path.exists());
        assert_eq!(fs::read_dir(&namespace).unwrap().count(), 0);

        publisher_control.write_all(b"R").unwrap();
        assert_child_success("publisher", publisher.wait_with_output().unwrap());
        assert!(!root_path.exists());
        assert_eq!(fs::read_dir(namespace).unwrap().count(), 0);
    }

    #[test]
    fn cleanup_process_guard_allows_different_keys_and_roots_while_one_key_is_paused() {
        // Catches a process-wide cleanup guard: a paused journal for one
        // (parent, component) key must not stall unrelated keys or roots.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        for name in ["paused", "peer"] {
            let path = physical.join(name);
            fs::create_dir(&path).unwrap();
            fs::write(path.join("value"), name.as_bytes()).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o500)).unwrap();
        }

        let other_fixture = tempfile::tempdir().unwrap();
        let other_physical = other_fixture.path().canonicalize().unwrap();
        fs::set_permissions(&other_physical, fs::Permissions::from_mode(0o700)).unwrap();
        let other_root = other_physical.join("paused");
        fs::create_dir(&other_root).unwrap();
        fs::write(other_root.join("value"), b"other").unwrap();
        fs::set_permissions(&other_root, fs::Permissions::from_mode(0o500)).unwrap();

        let (paused_tx, paused_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let paused_physical = physical.clone();
        let paused = std::thread::spawn(move || {
            let (hook, mut worker) = std::os::unix::net::UnixStream::pair().unwrap();
            let relay = std::thread::spawn(move || {
                let mut reached = [0];
                worker.read_exact(&mut reached).unwrap();
                assert_eq!(reached, [b'H']);
                paused_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                worker.write_all(b"R").unwrap();
            });
            let _handoff = super::CleanupBootstrapHandoffOverride::set(hook);
            let result = RootedDir::open(&paused_physical)
                .unwrap()
                .remove_owned_child("paused");
            relay.join().unwrap();
            result
        });
        paused_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();

        let (same_parent_tx, same_parent_rx) = std::sync::mpsc::channel();
        let peer_physical = physical.clone();
        let same_parent = std::thread::spawn(move || {
            same_parent_tx
                .send(
                    RootedDir::open(&peer_physical)
                        .unwrap()
                        .remove_owned_child("peer"),
                )
                .unwrap();
        });
        let (other_root_tx, other_root_rx) = std::sync::mpsc::channel();
        let other_root_worker = std::thread::spawn(move || {
            other_root_tx
                .send(
                    RootedDir::open(&other_physical)
                        .unwrap()
                        .remove_owned_child("paused"),
                )
                .unwrap();
        });

        let same_parent_while_paused =
            same_parent_rx.recv_timeout(std::time::Duration::from_secs(1));
        let other_root_while_paused = other_root_rx.recv_timeout(std::time::Duration::from_secs(1));
        release_tx.send(()).unwrap();
        paused.join().unwrap().unwrap();
        same_parent.join().unwrap();
        other_root_worker.join().unwrap();

        same_parent_while_paused.unwrap().unwrap();
        other_root_while_paused.unwrap().unwrap();
        assert!(!physical.join("paused").exists());
        assert!(!physical.join("peer").exists());
        assert!(!other_root.exists());
    }

    #[test]
    fn cleanup_namespace_validation_does_not_wait_on_unrelated_canonical_intent() {
        // Global namespace validation must read an unrelated published
        // canonical intent without taking its inode flock. Otherwise cleanup
        // B waits behind publisher A after A's canonical rename.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        for name in ["paused", "peer"] {
            let path = physical.join(name);
            fs::create_dir(&path).unwrap();
            fs::write(path.join("value"), name.as_bytes()).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o500)).unwrap();
        }

        let (renamed_tx, renamed_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let paused_physical = physical.clone();
        let paused = std::thread::spawn(move || {
            let _handoff = super::CleanupIntentHandoffOverride::set(renamed_tx, release_rx);
            RootedDir::open(&paused_physical)
                .unwrap()
                .remove_owned_child("paused")
        });
        renamed_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();

        let observer = RootedDir::open(&physical).unwrap();
        let parent_metadata = super::stat_fd(observer.root.as_raw_fd()).unwrap();
        let parent_identity = super::FileIdentity::from_stat(&parent_metadata);
        let namespace = PrivateNamespace::select_for_cleanup(
            observer.root.as_raw_fd(),
            parent_metadata.st_dev,
            &[],
        )
        .unwrap();
        let key = super::cleanup_key(parent_identity, b"paused");
        let intent_name = super::cleanup_intent_name(&key).unwrap();
        let (probe, _bytes, _canonical_identity) = super::open_cleanup_record(
            namespace.directory.as_raw_fd(),
            &intent_name,
            parent_identity.device,
            false,
        )
        .unwrap();
        super::clear_errno();
        let probe_result = unsafe { libc::flock(probe.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        let probe_errno = super::current_errno();
        if probe_result == 0 {
            assert_eq!(unsafe { libc::flock(probe.as_raw_fd(), libc::LOCK_UN) }, 0);
        }

        let (peer_tx, peer_rx) = std::sync::mpsc::channel();
        let peer_physical = physical.clone();
        let peer = std::thread::spawn(move || {
            peer_tx
                .send(
                    RootedDir::open(&peer_physical)
                        .unwrap()
                        .remove_owned_child("peer"),
                )
                .unwrap();
        });
        let peer_while_paused = peer_rx.recv_timeout(std::time::Duration::from_secs(2));
        release_tx.send(()).unwrap();
        paused.join().unwrap().unwrap();
        peer.join().unwrap();

        assert_eq!(probe_result, -1, "publisher did not retain the intent lock");
        assert!(probe_errno == libc::EAGAIN || probe_errno == libc::EWOULDBLOCK);
        peer_while_paused.unwrap().unwrap();
        assert!(!physical.join("paused").exists());
        assert!(!physical.join("peer").exists());
        drop(probe);
        drop(namespace);
        drop(observer);
    }

    #[test]
    fn cleanup_predicate_does_not_wait_on_unrelated_canonical_intent() {
        // Predicate collection must read an unrelated published canonical
        // intent without taking its inode flock. Otherwise sibling B waits
        // behind publisher A after A's canonical rename even though global
        // validation already skips that lock.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        for name in ["paused", "peer"] {
            let path = physical.join(name);
            fs::create_dir(&path).unwrap();
            fs::write(path.join("value"), name.as_bytes()).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o500)).unwrap();
        }
        let parent = RootedDir::open(&physical).unwrap();
        let prepare = CleanupFaultOverride::set(CleanupFault::AfterTargetChmod(libc::EIO));
        assert_eq!(
            parent
                .remove_owned_child("peer")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(prepare);
        drop(parent);

        let (renamed_tx, renamed_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let paused_physical = physical.clone();
        let paused = std::thread::spawn(move || {
            let _handoff = super::CleanupIntentHandoffOverride::set(renamed_tx, release_rx);
            RootedDir::open(&paused_physical)
                .unwrap()
                .remove_owned_child("paused")
        });
        renamed_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();

        let observer = RootedDir::open(&physical).unwrap();
        let parent_metadata = super::stat_fd(observer.root.as_raw_fd()).unwrap();
        let parent_identity = super::FileIdentity::from_stat(&parent_metadata);
        let namespace = PrivateNamespace::select_for_cleanup(
            observer.root.as_raw_fd(),
            parent_metadata.st_dev,
            &[],
        )
        .unwrap();
        let key = super::cleanup_key(parent_identity, b"paused");
        let intent_name = super::cleanup_intent_name(&key).unwrap();
        let (probe, _bytes, _canonical_identity) = super::open_cleanup_record(
            namespace.directory.as_raw_fd(),
            &intent_name,
            parent_identity.device,
            false,
        )
        .unwrap();
        super::clear_errno();
        let probe_result = unsafe { libc::flock(probe.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        let probe_errno = super::current_errno();
        if probe_result == 0 {
            assert_eq!(unsafe { libc::flock(probe.as_raw_fd(), libc::LOCK_UN) }, 0);
        }
        super::validate_cleanup_namespace_evidence(
            &namespace,
            observer.root.as_raw_fd(),
            parent_identity,
        )
        .unwrap();

        let (peer_tx, peer_rx) = std::sync::mpsc::channel();
        let peer_physical = physical.clone();
        let peer = std::thread::spawn(move || {
            peer_tx
                .send(
                    RootedDir::open(&peer_physical)
                        .unwrap()
                        .retry_pending_owned_children_matching(|component, _| component == b"peer"),
                )
                .unwrap();
        });
        let peer_while_paused = peer_rx.recv_timeout(std::time::Duration::from_secs(2));
        release_tx.send(()).unwrap();
        paused.join().unwrap().unwrap();
        peer.join().unwrap();

        assert_eq!(probe_result, -1, "publisher did not retain the intent lock");
        assert!(probe_errno == libc::EAGAIN || probe_errno == libc::EWOULDBLOCK);
        assert_eq!(peer_while_paused.unwrap().unwrap(), 1);
        assert!(!physical.join("paused").exists());
        assert!(!physical.join("peer").exists());
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
        drop(probe);
        drop(namespace);
        drop(observer);
    }

    #[test]
    fn cleanup_predicate_does_not_wait_on_unrelated_live_bootstrap() {
        // Predicate collection must LOCK_NB-skip a live unpublished bootstrap.
        // A blocking operation flock reintroduces cross-key HOL even though
        // global validation already treats WouldBlock as in-flight work.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        for name in ["paused", "peer"] {
            let path = physical.join(name);
            fs::create_dir(&path).unwrap();
            fs::write(path.join("value"), name.as_bytes()).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o500)).unwrap();
        }
        let parent = RootedDir::open(&physical).unwrap();
        let prepare = CleanupFaultOverride::set(CleanupFault::AfterTargetChmod(libc::EIO));
        assert_eq!(
            parent
                .remove_owned_child("peer")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(prepare);
        drop(parent);

        let observer = RootedDir::open(&physical).unwrap();
        let parent_metadata = super::stat_fd(observer.root.as_raw_fd()).unwrap();
        let parent_identity = super::FileIdentity::from_stat(&parent_metadata);
        let namespace = PrivateNamespace::select_for_cleanup(
            observer.root.as_raw_fd(),
            parent_metadata.st_dev,
            &[],
        )
        .unwrap();
        let component = std::ffi::CString::new("paused").unwrap();
        let target = super::FileIdentity::from_stat(
            &stat_at(observer.root.as_raw_fd(), &component).unwrap(),
        );
        let held = super::open_or_create_cleanup_bootstrap(
            &namespace,
            observer.root.as_raw_fd(),
            &component,
            &super::cleanup_key(parent_identity, b"paused"),
            target,
            0o500,
            super::CleanupTargetKind::Tree,
        )
        .unwrap()
        .unwrap();
        assert!(
            super::find_cleanup_intent(&namespace, parent_identity, b"paused")
                .unwrap()
                .is_none()
        );
        let parsed = super::parse_cleanup_bootstrap_name(&held.name)
            .unwrap()
            .unwrap();
        let (probe, _probe_identity) =
            super::open_cleanup_bootstrap_directory(&namespace, &held.name, &parsed).unwrap();
        super::clear_errno();
        let probe_result = unsafe { libc::flock(probe.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        let probe_errno = super::current_errno();
        if probe_result == 0 {
            assert_eq!(unsafe { libc::flock(probe.as_raw_fd(), libc::LOCK_UN) }, 0);
        }
        super::validate_cleanup_namespace_evidence(
            &namespace,
            observer.root.as_raw_fd(),
            parent_identity,
        )
        .unwrap();

        let (peer_tx, peer_rx) = std::sync::mpsc::channel();
        let peer_physical = physical.clone();
        let peer = std::thread::spawn(move || {
            peer_tx
                .send(
                    RootedDir::open(&peer_physical)
                        .unwrap()
                        .retry_pending_owned_children_matching(|component, _| component == b"peer"),
                )
                .unwrap();
        });
        let peer_while_held = peer_rx.recv_timeout(std::time::Duration::from_secs(2));
        drop(held);
        peer.join().unwrap();

        assert_eq!(
            probe_result, -1,
            "bootstrap helper did not retain the operation lock"
        );
        assert!(probe_errno == libc::EAGAIN || probe_errno == libc::EWOULDBLOCK);
        assert_eq!(peer_while_held.unwrap().unwrap(), 1);
        assert!(physical.join("paused").exists());
        assert!(!physical.join("peer").exists());
        drop(probe);
        drop(namespace);
        observer.remove_owned_child("paused").unwrap();
        assert!(!physical.join("paused").exists());
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn cleanup_predicate_callback_can_reenter_same_key_without_deadlock() {
        const ROLE_ENV: &str = "MAC_WORKER_CLEANUP_CALLBACK_REENTRY_ROLE";
        const ROOT_ENV: &str = "MAC_WORKER_CLEANUP_CALLBACK_REENTRY_ROOT";
        const STREAM_FD_ENV: &str = "MAC_WORKER_CLEANUP_CALLBACK_REENTRY_STREAM_FD";
        const TEST_NAME: &str =
            "rooted_fs::tests::cleanup_predicate_callback_can_reenter_same_key_without_deadlock";

        if std::env::var_os(ROLE_ENV).is_some() {
            let physical = std::path::PathBuf::from(std::env::var_os(ROOT_ENV).unwrap());
            let descriptor = std::env::var(STREAM_FD_ENV)
                .unwrap()
                .parse::<libc::c_int>()
                .unwrap();
            let mut stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(descriptor) };
            let parent = RootedDir::open(&physical).unwrap();
            stream.write_all(b"W").unwrap();
            let resumed = parent
                .retry_pending_owned_children_matching(|component, _| {
                    assert_eq!(component, b"root");
                    stream.write_all(b"C").unwrap();
                    assert!(
                        RootedDir::open(&physical)
                            .unwrap()
                            .resume_pending_owned_child_cleanup("root")
                            .unwrap()
                    );
                    stream.write_all(b"R").unwrap();
                    true
                })
                .unwrap();
            stream.write_all(&[b'D', resumed as u8]).unwrap();
            return;
        }

        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"owned").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let prepare = CleanupFaultOverride::set(CleanupFault::AfterTargetChmod(libc::EIO));
        parent.remove_owned_child("root").unwrap_err();
        drop(prepare);
        drop(parent);

        let (mut control, worker_stream) = std::os::unix::net::UnixStream::pair().unwrap();
        let descriptor = worker_stream.as_raw_fd();
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        assert_ne!(flags, -1);
        assert_eq!(
            unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags & !libc::FD_CLOEXEC) },
            0
        );
        let mut worker = std::process::Command::new(std::env::current_exe().unwrap())
            .env(ROLE_ENV, "worker")
            .env(ROOT_ENV, &physical)
            .env(STREAM_FD_ENV, descriptor.to_string())
            .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        drop(worker_stream);
        let mut progress = [0; 2];
        control.read_exact(&mut progress).unwrap();
        assert_eq!(progress, [b'W', b'C']);
        control
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        let mut completion = [0; 3];
        let reentered = match control.read_exact(&mut completion) {
            Ok(()) => true,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                false
            }
            Err(error) => panic!("callback re-entry handoff failed: {error:?}"),
        };
        if !reentered {
            worker.kill().unwrap();
        }
        let output = worker.wait_with_output().unwrap();

        assert!(reentered, "same-key callback re-entry deadlocked");
        assert!(
            output.status.success(),
            "worker stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(completion, [b'R', b'D', 0]);
        assert!(!root_path.exists());
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn cleanup_process_entry_guard_serializes_same_process_journals() {
        let parent = super::FileIdentity {
            device: u64::MAX - 1,
            inode: u64::MAX - 2,
        };
        let first = super::lock_cleanup_process_key(parent, b"same-key").unwrap();
        let (blocked, blocked_receive) = std::sync::mpsc::channel();
        let (entered, receive) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            assert!(
                super::try_lock_cleanup_process_key(parent, b"same-key")
                    .unwrap()
                    .is_none(),
                "cleanup process gate admitted a concurrent same-key journal"
            );
            blocked.send(()).unwrap();
            let _second = super::lock_cleanup_process_key(parent, b"same-key").unwrap();
            entered.send(()).unwrap();
        });

        blocked_receive
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        drop(first);
        receive
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        waiter.join().unwrap();
    }

    #[test]
    fn cleanup_process_entry_guard_recovers_from_poison() {
        let poisoned = std::thread::spawn(|| {
            let _guard = super::CLEANUP_PROCESS_KEYS.active.lock().unwrap();
            panic!("poison cleanup test gate");
        });
        assert!(poisoned.join().is_err());
        drop(
            super::lock_cleanup_process_key(
                super::FileIdentity {
                    device: u64::MAX - 3,
                    inode: u64::MAX - 4,
                },
                b"poison-recovery",
            )
            .unwrap(),
        );
        super::CLEANUP_PROCESS_KEYS.active.clear_poison();
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
            parsed.kind,
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
    fn cleanup_bound_exact_generation_never_resumes_later_same_key_intent() {
        // A key binds parent+component, not an inode generation. A stale
        // resolver bound to retired A must not adopt canonical intent B.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value-a"), b"generation-a").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let held_a = RootedDir::open(&root_path).unwrap();
        let a_metadata = fs::metadata(&root_path).unwrap();
        let a = super::FileIdentity {
            device: a_metadata.dev(),
            inode: a_metadata.ino(),
        };
        let parent = RootedDir::open(&physical).unwrap();
        parent.remove_owned_child("root").unwrap();

        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("sentinel-b"), b"generation-b").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let b_inode = fs::metadata(&root_path).unwrap().ino();
        assert_ne!(b_inode, a.inode);
        let fault = CleanupFaultOverride::set(CleanupFault::AfterTargetChmod(libc::EIO));
        assert_eq!(
            parent
                .remove_owned_child("root")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(fault);
        let parent_metadata = super::stat_fd(parent.root.as_raw_fd()).unwrap();
        let parent_identity = super::FileIdentity::from_stat(&parent_metadata);
        let namespace =
            PrivateNamespace::select(parent.root.as_raw_fd(), parent_metadata.st_dev, &[]).unwrap();

        let error = super::resolve_bound_tree_cleanup(
            &namespace,
            parent.root.as_raw_fd(),
            parent_identity,
            b"root",
            a,
            0o500,
            None,
            &|| Ok(()),
            false,
        )
        .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(
            fs::read(root_path.join("sentinel-b")).unwrap(),
            b"generation-b"
        );
        assert_eq!(fs::metadata(&root_path).unwrap().ino(), b_inode);
        drop(held_a);
    }

    #[test]
    fn cleanup_bound_canonical_intent_rejects_wrong_expected_original_mode() {
        // Catches treating target+intent/operation identity as the complete
        // generation fence while silently accepting a mismatched mode.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"generation-a").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let target_metadata = fs::metadata(&root_path).unwrap();
        let target = super::FileIdentity {
            device: target_metadata.dev(),
            inode: target_metadata.ino(),
        };
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
        let parent_metadata = super::stat_fd(parent.root.as_raw_fd()).unwrap();
        let parent_identity = super::FileIdentity::from_stat(&parent_metadata);
        let namespace = PrivateNamespace::select_for_cleanup(
            parent.root.as_raw_fd(),
            parent_metadata.st_dev,
            &[],
        )
        .unwrap();
        let loaded = super::find_cleanup_intent(&namespace, parent_identity, b"root")
            .unwrap()
            .unwrap();
        assert_eq!(super::FileIdentity::from(loaded.intent.target), target);
        assert_eq!(loaded.intent.original_mode, 0o500);
        let generation =
            super::cleanup_generation_from_loaded(&namespace, parent_identity, b"root", &loaded)
                .unwrap();
        let intent_name = super::cleanup_intent_name(&loaded.intent.key_sha256).unwrap();
        let intent_path = physical
            .join(".mac-worker-rooted-fs")
            .join(std::ffi::OsStr::from_bytes(intent_name.to_bytes()));
        let intent_bytes = fs::read(&intent_path).unwrap();
        let intent_inode = fs::metadata(&intent_path).unwrap().ino();
        drop(loaded);

        let error = super::resolve_bound_tree_cleanup(
            &namespace,
            parent.root.as_raw_fd(),
            parent_identity,
            b"root",
            target,
            0o700,
            Some(&generation),
            &|| Ok(()),
            false,
        )
        .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(fs::read(&intent_path).unwrap(), intent_bytes);
        assert_eq!(fs::metadata(&intent_path).unwrap().ino(), intent_inode);
        let current = fs::metadata(&root_path).unwrap();
        assert_eq!(current.dev(), target.device);
        assert_eq!(current.ino(), target.inode);
        assert_eq!(current.permissions().mode() & 0o777, 0o700);
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"generation-a");
    }

    #[test]
    fn cleanup_bound_predicate_candidate_never_resumes_later_same_key_intent() {
        // Models a cross-process retirement between predicate snapshot and
        // resume: cached approval for A cannot authorize canonical B.
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value-a"), b"generation-a").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let held_a = RootedDir::open(&root_path).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let fault = CleanupFaultOverride::set(CleanupFault::AfterTargetChmod(libc::EIO));
        parent.remove_owned_child("root").unwrap_err();
        drop(fault);
        let parent_metadata = super::stat_fd(parent.root.as_raw_fd()).unwrap();
        let parent_identity = super::FileIdentity::from_stat(&parent_metadata);
        let namespace =
            PrivateNamespace::select(parent.root.as_raw_fd(), parent_metadata.st_dev, &[]).unwrap();
        let candidate = super::collect_pending_tree_cleanup_candidates(
            &namespace,
            parent.root.as_raw_fd(),
            parent_identity,
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
        drop(namespace);
        parent.remove_owned_child("root").unwrap();

        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("sentinel-b"), b"generation-b").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let b_inode = fs::metadata(&root_path).unwrap().ino();
        assert_ne!(b_inode, candidate.target.inode);
        let fault = CleanupFaultOverride::set(CleanupFault::AfterTargetChmod(libc::EIO));
        parent.remove_owned_child("root").unwrap_err();
        drop(fault);
        let namespace =
            PrivateNamespace::select(parent.root.as_raw_fd(), parent_metadata.st_dev, &[]).unwrap();

        let error = super::resolve_bound_tree_cleanup(
            &namespace,
            parent.root.as_raw_fd(),
            parent_identity,
            &candidate.component,
            candidate.target,
            candidate.original_mode,
            Some(&candidate.generation),
            &|| Ok(()),
            false,
        )
        .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(
            fs::read(root_path.join("sentinel-b")).unwrap(),
            b"generation-b"
        );
        assert_eq!(fs::metadata(&root_path).unwrap().ino(), b_inode);
        drop(held_a);
    }

    #[test]
    fn cleanup_predicate_resnapshots_later_generation_with_fresh_callback() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value-a"), b"generation-a").unwrap();
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

        let peer = RootedDir::open(&physical).unwrap();
        let parent_metadata = super::stat_fd(peer.root.as_raw_fd()).unwrap();
        let parent_identity = super::FileIdentity::from_stat(&parent_metadata);
        let namespace = PrivateNamespace::select_for_cleanup(
            peer.root.as_raw_fd(),
            parent_metadata.st_dev,
            &[],
        )
        .unwrap();
        let candidate = super::collect_pending_tree_cleanup_candidates(
            &namespace,
            peer.root.as_raw_fd(),
            parent_identity,
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
        let a_inode = candidate.target.inode;
        let mut calls = Vec::new();
        let mut replaced = false;

        let resumed = parent
            .retry_pending_owned_children_matching(|component, identity| {
                assert_eq!(component, b"root");
                super::clear_errno();
                assert_eq!(
                    unsafe {
                        libc::flock(
                            namespace.directory.as_raw_fd(),
                            libc::LOCK_EX | libc::LOCK_NB,
                        )
                    },
                    0,
                    "predicate ran under the namespace lock: {}",
                    super::current_errno()
                );
                assert_eq!(
                    unsafe { libc::flock(namespace.directory.as_raw_fd(), libc::LOCK_UN) },
                    0
                );
                calls.push(identity.inode);
                if !replaced {
                    super::complete_bound_tree_cleanup(
                        &namespace,
                        peer.root.as_raw_fd(),
                        parent_identity,
                        &candidate.component,
                        candidate.target,
                        candidate.original_mode,
                        Some(candidate.generation.clone()),
                        &|| Ok(()),
                        false,
                    )
                    .unwrap();
                    assert!(!root_path.exists());
                    fs::create_dir(&root_path).unwrap();
                    fs::write(root_path.join("value-b"), b"generation-b").unwrap();
                    fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
                    let component = std::ffi::CString::new("root").unwrap();
                    let b = super::FileIdentity::from_stat(
                        &stat_at(peer.root.as_raw_fd(), &component).unwrap(),
                    );
                    super::publish_tree_cleanup_intent(
                        &namespace,
                        peer.root.as_raw_fd(),
                        parent_identity,
                        &component,
                        b,
                        0o500,
                    )
                    .unwrap();
                    replaced = true;
                    true
                } else {
                    false
                }
            })
            .unwrap();

        assert_eq!(resumed, 0);
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], a_inode);
        assert_ne!(calls[1], a_inode);
        assert_eq!(
            fs::read(root_path.join("value-b")).unwrap(),
            b"generation-b"
        );
        assert!(parent.resume_pending_owned_child_cleanup("root").unwrap());
        assert!(!root_path.exists());
    }

    #[test]
    fn cleanup_predicate_does_not_count_generation_retired_during_callback() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value-a"), b"generation-a").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let fault = CleanupFaultOverride::set(CleanupFault::AfterTargetChmod(libc::EIO));
        parent.remove_owned_child("root").unwrap_err();
        drop(fault);

        let peer = RootedDir::open(&physical).unwrap();
        let parent_metadata = super::stat_fd(peer.root.as_raw_fd()).unwrap();
        let parent_identity = super::FileIdentity::from_stat(&parent_metadata);
        let namespace = PrivateNamespace::select_for_cleanup(
            peer.root.as_raw_fd(),
            parent_metadata.st_dev,
            &[],
        )
        .unwrap();
        let candidate = super::collect_pending_tree_cleanup_candidates(
            &namespace,
            peer.root.as_raw_fd(),
            parent_identity,
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
        let mut calls = 0;

        let resumed = parent
            .retry_pending_owned_children_matching(|component, identity| {
                assert_eq!(component, b"root");
                assert_eq!(identity.inode, candidate.target.inode);
                calls += 1;
                super::complete_bound_tree_cleanup(
                    &namespace,
                    peer.root.as_raw_fd(),
                    parent_identity,
                    &candidate.component,
                    candidate.target,
                    candidate.original_mode,
                    Some(candidate.generation.clone()),
                    &|| Ok(()),
                    false,
                )
                .unwrap();
                true
            })
            .unwrap();

        assert_eq!(calls, 1);
        assert_eq!(resumed, 0);
        assert!(!root_path.exists());
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn cleanup_predicate_does_not_count_cross_process_retirement_after_resnapshot() {
        const ROLE_ENV: &str = "MAC_WORKER_CLEANUP_PREDICATE_LATE_RETIRE_ROLE";
        const ROOT_ENV: &str = "MAC_WORKER_CLEANUP_PREDICATE_LATE_RETIRE_ROOT";
        const STREAM_FD_ENV: &str = "MAC_WORKER_CLEANUP_PREDICATE_LATE_RETIRE_STREAM_FD";
        const TEST_NAME: &str = "rooted_fs::tests::cleanup_predicate_does_not_count_cross_process_retirement_after_resnapshot";

        if std::env::var_os(ROLE_ENV).is_some() {
            let physical = std::path::PathBuf::from(std::env::var_os(ROOT_ENV).unwrap());
            let descriptor = std::env::var(STREAM_FD_ENV)
                .unwrap()
                .parse::<libc::c_int>()
                .unwrap();
            let mut stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(descriptor) };
            stream.write_all(b"W").unwrap();
            let mut go = [0];
            stream.read_exact(&mut go).unwrap();
            assert_eq!(go, [b'G']);
            let retired = RootedDir::open(&physical)
                .unwrap()
                .resume_pending_owned_child_cleanup("root")
                .unwrap();
            stream.write_all(&[b'D', u8::from(retired)]).unwrap();
            return;
        }

        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"generation-a").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let prepare = CleanupFaultOverride::set(CleanupFault::AfterTargetChmod(libc::EIO));
        assert_eq!(
            parent
                .remove_owned_child("root")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(prepare);

        let (mut peer_control, peer_stream) = std::os::unix::net::UnixStream::pair().unwrap();
        let descriptor = peer_stream.as_raw_fd();
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        assert_ne!(flags, -1);
        assert_eq!(
            unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags & !libc::FD_CLOEXEC) },
            0
        );
        let peer = std::process::Command::new(std::env::current_exe().unwrap())
            .env(ROLE_ENV, "peer")
            .env(ROOT_ENV, &physical)
            .env(STREAM_FD_ENV, descriptor.to_string())
            .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        drop(peer_stream);
        let mut ready = [0];
        peer_control.read_exact(&mut ready).unwrap();
        assert_eq!(ready, [b'W']);

        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker_physical = physical.clone();
        let worker = std::thread::spawn(move || {
            let _handoff =
                super::CleanupPredicateCompletionHandoffOverride::set(reached_tx, release_rx);
            RootedDir::open(&worker_physical)
                .unwrap()
                .retry_pending_owned_children_matching(|component, identity| {
                    assert_eq!(component, b"root");
                    assert_eq!(identity.kind, libc::S_IFDIR as u32);
                    true
                })
        });
        reached_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        peer_control.write_all(b"G").unwrap();
        let mut retired = [0; 2];
        peer_control.read_exact(&mut retired).unwrap();
        assert_eq!(retired, [b'D', 1]);
        let output = peer.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "peer stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        release_tx.send(()).unwrap();
        let resumed = worker.join().unwrap().unwrap();

        assert_eq!(resumed, 0);
        assert!(!root_path.exists());
        assert_eq!(
            fs::read_dir(physical.join(".mac-worker-rooted-fs"))
                .unwrap()
                .count(),
            0
        );
    }

    #[test]
    fn cleanup_predicate_preserves_exact_generation_on_completion_estale() {
        let fixture = tempfile::tempdir().unwrap();
        let physical = fixture.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = physical.join("root");
        fs::create_dir(&root_path).unwrap();
        fs::write(root_path.join("value"), b"generation-a").unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o500)).unwrap();
        let parent = RootedDir::open(&physical).unwrap();
        let interrupted = CleanupFaultOverride::set(CleanupFault::AfterTargetChmod(libc::EIO));
        assert_eq!(
            parent
                .remove_owned_child("root")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EIO)
        );
        drop(interrupted);

        let completion =
            CleanupFaultOverride::set(CleanupFault::BeforeBoundCompletion(libc::ESTALE));
        let error = parent
            .retry_pending_owned_children_matching(|component, _| component == b"root")
            .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(fs::read(root_path.join("value")).unwrap(), b"generation-a");
        drop(completion);

        assert!(parent.resume_pending_owned_child_cleanup("root").unwrap());
        assert!(!root_path.exists());
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
    fn cleanup_intent_regular_retry_regular_cleanup_recovery_preserves_a_substituted_quarantine_entry()
     {
        let fixture = RegularCleanupRetryFixture::create();
        let fault = fixture.interrupt_after_public_entry_vanishes(
            CleanupFault::AfterCleanupQuarantineRename(libc::EIO),
        );
        drop(fault);

        let (intent, intent_inode) = fixture.snapshot_unique_role(b"cleanup-intent-v1-");
        let (operation, operation_inode) = fixture.snapshot_unique_role(b"cleanup-op-v1-");
        assert!(
            operation
                .file_name()
                .unwrap()
                .as_bytes()
                .windows(4)
                .any(|window| window == b"-k02")
        );
        fixture.assert_regular_retry_intent_bootstrap_binding();
        assert!(
            !fixture
                .namespace_roles()
                .iter()
                .any(|(name, _)| name.starts_with(b"cleanup-decision-v1-"))
        );
        let (quarantine, _) = fixture.snapshot_unique_role(b"cleanup-regular-v1-");
        assert_no_follow_regular(
            &quarantine,
            fixture.entry_device,
            fixture.entry_inode,
            0o600,
            REGULAR_RETRY_ENTRY_BYTES,
        );
        let placeholder = fs::symlink_metadata(operation.join("cleanup-placeholder-v1")).unwrap();
        assert!(placeholder.file_type().is_dir());
        assert_eq!(placeholder.permissions().mode() & 0o777, 0o700);

        let evidence = fixture
            .root
            .parent()
            .unwrap()
            .join("owned-regular-quarantine-evidence");
        fs::rename(&quarantine, &evidence).unwrap();
        let substitute_inode = plant_regular_substitute(&quarantine);

        assert_eq!(
            fixture.retry_remove_owned_regular().raw_os_error(),
            Some(libc::ESTALE)
        );
        assert_no_follow_regular(
            &quarantine,
            fixture.entry_device,
            substitute_inode,
            0o600,
            REGULAR_RETRY_SUBSTITUTE_BYTES,
        );
        assert_no_follow_regular(
            &evidence,
            fixture.entry_device,
            fixture.entry_inode,
            0o600,
            REGULAR_RETRY_ENTRY_BYTES,
        );
        assert_eq!(fs::symlink_metadata(&intent).unwrap().ino(), intent_inode);
        assert_eq!(
            fs::symlink_metadata(&operation).unwrap().ino(),
            operation_inode
        );
        assert!(
            !fixture
                .namespace_roles()
                .iter()
                .any(|(name, _)| name.starts_with(b"cleanup-decision-v1-"))
        );
        assert_eq!(
            fixture.unique_namespace_role(b"cleanup-regular-v1-"),
            quarantine
        );
        fixture.assert_public_entry_absent();
        fixture.assert_regular_retry_intent_bootstrap_binding();
    }

    #[test]
    fn cleanup_intent_regular_retry_regular_cleanup_final_delete_preserves_a_substituted_entry() {
        let fixture = RegularCleanupRetryFixture::create();
        let fault = fixture
            .interrupt_after_public_entry_vanishes(CleanupFault::BeforeFinalRootRemoval(libc::EIO));
        drop(fault);

        let (intent, intent_inode) = fixture.snapshot_unique_role(b"cleanup-intent-v1-");
        let (decision, decision_inode) = fixture.snapshot_unique_role(b"cleanup-decision-v1-");
        let record: CleanupDecisionRecordV1 =
            cleanup_parse_canonical_json(&fs::read(&decision).unwrap()).unwrap();
        assert!(matches!(record.decision, CleanupDecisionV1::Delete));
        assert_eq!(
            fs::symlink_metadata(&decision)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let (operation, operation_inode) = fixture.snapshot_unique_role(b"cleanup-op-v1-");
        assert!(
            operation
                .file_name()
                .unwrap()
                .as_bytes()
                .windows(4)
                .any(|window| window == b"-k02")
        );
        let (quarantine, quarantine_inode) = fixture.snapshot_unique_role(b"cleanup-regular-v1-");
        let quarantine_meta = fs::symlink_metadata(&quarantine).unwrap();
        assert!(quarantine_meta.file_type().is_dir());
        assert_eq!(quarantine_meta.permissions().mode() & 0o777, 0o700);
        fixture.assert_regular_retry_intent_bootstrap_binding();

        let captured = operation.join("cleanup-placeholder-v1");
        assert_no_follow_regular(
            &captured,
            fixture.entry_device,
            fixture.entry_inode,
            0o600,
            REGULAR_RETRY_ENTRY_BYTES,
        );
        let evidence = fixture
            .root
            .parent()
            .unwrap()
            .join("owned-final-regular-evidence");
        fs::rename(&captured, &evidence).unwrap();
        let substitute_inode = plant_regular_substitute(&captured);

        assert_eq!(
            fixture.retry_remove_owned_regular().raw_os_error(),
            Some(libc::ESTALE)
        );
        assert_no_follow_regular(
            &captured,
            fixture.entry_device,
            substitute_inode,
            0o600,
            REGULAR_RETRY_SUBSTITUTE_BYTES,
        );
        assert_no_follow_regular(
            &evidence,
            fixture.entry_device,
            fixture.entry_inode,
            0o600,
            REGULAR_RETRY_ENTRY_BYTES,
        );
        assert_eq!(fs::symlink_metadata(&intent).unwrap().ino(), intent_inode);
        assert_eq!(
            fs::symlink_metadata(&decision).unwrap().ino(),
            decision_inode
        );
        let leftover: CleanupDecisionRecordV1 =
            cleanup_parse_canonical_json(&fs::read(&decision).unwrap()).unwrap();
        assert!(matches!(leftover.decision, CleanupDecisionV1::Delete));
        assert_eq!(
            fs::symlink_metadata(&operation).unwrap().ino(),
            operation_inode
        );
        assert_eq!(
            fs::symlink_metadata(&quarantine).unwrap().ino(),
            quarantine_inode
        );
        fixture.assert_public_entry_absent();
        fixture.assert_regular_retry_intent_bootstrap_binding();
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

    #[derive(Clone, Copy)]
    enum CrashMatrixKind {
        Tree,
        Regular,
    }

    enum CrashMatrixFault {
        Cleanup(CleanupFault),
        Decision(CleanupDecisionFault),
        RestoreThen(CleanupDecisionFault),
    }

    struct CrashMatrixCell {
        name: &'static str,
        kind: CrashMatrixKind,
        fault: CrashMatrixFault,
        post_rename: bool,
        expect_restore: bool,
    }

    fn assert_content_free_cleanup_error(
        error: &std::io::Error,
        expected_raw: Option<i32>,
        expected_kind: Option<std::io::ErrorKind>,
        forbidden: &[&[u8]],
    ) {
        if let Some(errno) = expected_raw {
            assert_eq!(error.raw_os_error(), Some(errno), "{error:?}");
        }
        if let Some(kind) = expected_kind {
            assert_eq!(error.kind(), kind, "{error:?}");
        }
        let display = error.to_string();
        let debug = format!("{error:?}");
        for needle in forbidden {
            let text = String::from_utf8_lossy(needle);
            if text.is_empty() {
                continue;
            }
            assert!(
                !display.contains(text.as_ref()),
                "Display leaked {text:?}: {display}"
            );
            assert!(
                !debug.contains(text.as_ref()),
                "Debug leaked {text:?}: {debug}"
            );
        }
    }

    fn assert_cleanup_namespace_roles_retired(namespace: &Path) {
        if !namespace.exists() {
            return;
        }
        let leftover = fs::read_dir(namespace)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().as_bytes().to_vec())
            .collect::<Vec<_>>();
        assert!(
            leftover.iter().all(|name| {
                !name.starts_with(b"cleanup-intent-v1-")
                    && !name.starts_with(b"cleanup-decision-v1-")
                    && !name.starts_with(b"cleanup-op-v1-")
                    && !name.starts_with(b"cleanup-tree-v1-")
                    && !name.starts_with(b"cleanup-regular-v1-")
                    && name.as_slice() != b"cleanup-placeholder-v1"
            }),
            "journal roles remain: {leftover:?}"
        );
        assert_eq!(leftover.len(), 0, "namespace not empty: {leftover:?}");
    }

    fn crash_matrix_forbidden(root: &Path, public: &str, component_hex: &[u8]) -> Vec<Vec<u8>> {
        let mut forbidden = vec![
            root.as_os_str().as_bytes().to_vec(),
            public.as_bytes().to_vec(),
            component_hex.to_vec(),
            b"replacement-bytes".to_vec(),
            b"owned-leaf".to_vec(),
            b"owned-regular".to_vec(),
            b"sibling-bytes".to_vec(),
        ];
        let namespace = root.join(".mac-worker-rooted-fs");
        if let Ok(entries) = fs::read_dir(&namespace) {
            for entry in entries {
                let name = entry.unwrap().file_name();
                let bytes = name.as_bytes();
                if bytes.starts_with(b"cleanup-intent-v1-")
                    || bytes.starts_with(b"cleanup-decision-v1-")
                    || bytes.starts_with(b"cleanup-op-v1-")
                    || bytes.starts_with(b"cleanup-tree-v1-")
                    || bytes.starts_with(b"cleanup-regular-v1-")
                    || bytes == b"cleanup-placeholder-v1"
                {
                    forbidden.push(bytes.to_vec());
                }
            }
        }
        forbidden
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum OriginalEvidenceExpectation {
        Present,
        Missing,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum TreeLeafExpectation {
        Intact,
        Removed,
        NotATree,
    }

    #[derive(Debug, PartialEq, Eq)]
    enum PinnedTreeLeaf {
        Intact {
            dev: u64,
            ino: u64,
            mode: u32,
            bytes: Vec<u8>,
        },
        Removed,
    }

    #[derive(Debug, PartialEq, Eq)]
    enum PinnedOriginalPayload {
        RegularFile {
            bytes: Vec<u8>,
        },
        Tree {
            entries: Vec<Vec<u8>>,
            leaf: PinnedTreeLeaf,
        },
    }

    #[derive(Debug, PartialEq, Eq)]
    struct OriginalEvidencePin {
        path: PathBuf,
        dev: u64,
        ino: u64,
        mode: u32,
        payload: PinnedOriginalPayload,
    }

    #[derive(Debug, PartialEq, Eq)]
    enum OriginalEvidenceState {
        Present(OriginalEvidencePin),
        Missing { dev: u64, ino: u64 },
    }

    fn expected_original_after_crash(cell: &CrashMatrixCell) -> OriginalEvidenceExpectation {
        match &cell.fault {
            CrashMatrixFault::Cleanup(CleanupFault::AfterCleanupPlaceholderRemoval(_))
            | CrashMatrixFault::Cleanup(CleanupFault::AfterCleanupOperationRemoval(_))
            | CrashMatrixFault::Decision(CleanupDecisionFault::AfterDecisionRetire(_)) => {
                OriginalEvidenceExpectation::Missing
            }
            _ => OriginalEvidenceExpectation::Present,
        }
    }

    fn expected_tree_leaf_after_crash(cell: &CrashMatrixCell) -> TreeLeafExpectation {
        match cell.kind {
            CrashMatrixKind::Regular => TreeLeafExpectation::NotATree,
            CrashMatrixKind::Tree => match &cell.fault {
                CrashMatrixFault::Cleanup(CleanupFault::AfterFirstRemoval(_))
                | CrashMatrixFault::Cleanup(CleanupFault::BeforeFinalRootRemoval(_)) => {
                    TreeLeafExpectation::Removed
                }
                _ => match expected_original_after_crash(cell) {
                    OriginalEvidenceExpectation::Present => TreeLeafExpectation::Intact,
                    OriginalEvidenceExpectation::Missing => TreeLeafExpectation::NotATree,
                },
            },
        }
    }

    fn find_inode_path(root: &Path, dev: u64, ino: u64) -> Option<PathBuf> {
        let meta = fs::symlink_metadata(root).ok()?;
        if meta.dev() == dev && meta.ino() == ino {
            return Some(root.to_path_buf());
        }
        if !meta.file_type().is_dir() {
            return None;
        }
        for entry in fs::read_dir(root).ok()? {
            if let Some(found) = find_inode_path(&entry.ok()?.path(), dev, ino) {
                return Some(found);
            }
        }
        None
    }

    fn capture_original_evidence_state(
        namespace: &Path,
        original: &fs::Metadata,
    ) -> OriginalEvidenceState {
        match find_inode_path(namespace, original.dev(), original.ino()) {
            Some(path) => OriginalEvidenceState::Present(pin_present_original(&path, original)),
            None => OriginalEvidenceState::Missing {
                dev: original.dev(),
                ino: original.ino(),
            },
        }
    }

    fn pin_present_original(path: &Path, original: &fs::Metadata) -> OriginalEvidencePin {
        let meta = fs::symlink_metadata(path).unwrap();
        assert_eq!(meta.dev(), original.dev());
        assert_eq!(meta.ino(), original.ino());
        let payload = if meta.file_type().is_file() {
            PinnedOriginalPayload::RegularFile {
                bytes: fs::read(path).unwrap(),
            }
        } else if meta.file_type().is_dir() {
            let mut entries = fs::read_dir(path)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().as_bytes().to_vec())
                .collect::<Vec<_>>();
            entries.sort();
            let leaf_path = path.join("leaf");
            let leaf = match fs::symlink_metadata(&leaf_path) {
                Ok(leaf_meta) => PinnedTreeLeaf::Intact {
                    dev: leaf_meta.dev(),
                    ino: leaf_meta.ino(),
                    mode: leaf_meta.permissions().mode() & 0o7777,
                    bytes: fs::read(&leaf_path).unwrap(),
                },
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    PinnedTreeLeaf::Removed
                }
                Err(error) => panic!("tree leaf metadata: {error:?}"),
            };
            PinnedOriginalPayload::Tree { entries, leaf }
        } else {
            panic!("original evidence is neither file nor directory: {path:?}");
        };
        OriginalEvidencePin {
            path: path.to_path_buf(),
            dev: meta.dev(),
            ino: meta.ino(),
            mode: meta.permissions().mode() & 0o7777,
            payload,
        }
    }

    fn require_original_evidence_for_phase(
        cell: &CrashMatrixCell,
        observed: &OriginalEvidenceState,
    ) {
        let expected = expected_original_after_crash(cell);
        match (expected, observed) {
            (OriginalEvidenceExpectation::Present, OriginalEvidenceState::Present(pin)) => {
                match (expected_tree_leaf_after_crash(cell), &pin.payload) {
                    (
                        TreeLeafExpectation::NotATree,
                        PinnedOriginalPayload::RegularFile { bytes },
                    ) => {
                        assert_eq!(bytes, b"owned-regular", "{}", cell.name);
                    }
                    (
                        TreeLeafExpectation::Intact,
                        PinnedOriginalPayload::Tree {
                            leaf: PinnedTreeLeaf::Intact { bytes, .. },
                            entries,
                        },
                    ) => {
                        assert_eq!(bytes, b"owned-leaf", "{}", cell.name);
                        assert!(
                            entries.iter().any(|name| name == b"leaf"),
                            "{}: intact tree missing leaf entry {entries:?}",
                            cell.name
                        );
                    }
                    (
                        TreeLeafExpectation::Removed,
                        PinnedOriginalPayload::Tree {
                            leaf: PinnedTreeLeaf::Removed,
                            entries,
                        },
                    ) => {
                        assert!(
                            !entries.iter().any(|name| name == b"leaf"),
                            "{}: removed tree still lists leaf {entries:?}",
                            cell.name
                        );
                    }
                    (leaf, payload) => panic!(
                        "{}: original payload {payload:?} does not match leaf expectation {leaf:?}",
                        cell.name
                    ),
                }
            }
            (OriginalEvidenceExpectation::Missing, OriginalEvidenceState::Missing { .. }) => {}
            (expected, observed) => panic!(
                "{}: expected original evidence {expected:?}, observed {observed:?}",
                cell.name
            ),
        }
    }

    fn assert_original_evidence_state_unchanged(
        before: &OriginalEvidenceState,
        namespace: &Path,
        original: &fs::Metadata,
        cell_name: &str,
    ) {
        let after = capture_original_evidence_state(namespace, original);
        match (before, &after) {
            (
                OriginalEvidenceState::Present(before_pin),
                OriginalEvidenceState::Present(after_pin),
            ) => {
                assert_eq!(after_pin, before_pin, "{cell_name}");
            }
            (
                OriginalEvidenceState::Missing { dev, ino },
                OriginalEvidenceState::Missing {
                    dev: after_dev,
                    ino: after_ino,
                },
            ) => {
                assert_eq!(after_dev, dev, "{cell_name}");
                assert_eq!(after_ino, ino, "{cell_name}");
                assert!(
                    find_inode_path(namespace, *dev, *ino).is_none(),
                    "{cell_name}: deleted original {dev}/{ino} reappeared"
                );
            }
            (before, after) => {
                panic!("{cell_name}: original evidence state changed: {before:?} -> {after:?}")
            }
        }
    }

    fn run_crash_matrix_cell(cell: CrashMatrixCell) {
        let temp = tempfile::tempdir().unwrap();
        let physical = temp.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root = physical.join("parent");
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let public = match cell.kind {
            CrashMatrixKind::Tree => "child",
            CrashMatrixKind::Regular => "entry",
        };
        let public_path = root.join(public);
        let sibling = root.join("sibling");
        fs::write(&sibling, b"sibling-bytes").unwrap();
        fs::set_permissions(&sibling, fs::Permissions::from_mode(0o600)).unwrap();
        match cell.kind {
            CrashMatrixKind::Tree => {
                fs::create_dir(&public_path).unwrap();
                fs::write(public_path.join("leaf"), b"owned-leaf").unwrap();
                fs::set_permissions(&public_path, fs::Permissions::from_mode(0o500)).unwrap();
            }
            CrashMatrixKind::Regular => {
                fs::write(&public_path, b"owned-regular").unwrap();
                fs::set_permissions(&public_path, fs::Permissions::from_mode(0o600)).unwrap();
            }
        }
        let original = fs::symlink_metadata(&public_path).unwrap();
        let parent = RootedDir::open(&root).unwrap();
        let cleanup_fault = match &cell.fault {
            CrashMatrixFault::Cleanup(fault) => Some(CleanupFaultOverride::set(*fault)),
            CrashMatrixFault::RestoreThen(_) => Some(CleanupFaultOverride::set(
                CleanupFault::AfterAcquisitionValidation(libc::ESTALE),
            )),
            CrashMatrixFault::Decision(_) => None,
        };
        let decision_fault = match &cell.fault {
            CrashMatrixFault::Decision(fault) | CrashMatrixFault::RestoreThen(fault) => {
                Some(CleanupDecisionFaultOverride::set(*fault))
            }
            CrashMatrixFault::Cleanup(_) => None,
        };
        let first = match cell.kind {
            CrashMatrixKind::Tree => parent.remove_owned_child(public),
            CrashMatrixKind::Regular => parent.remove_owned_regular(public),
        };
        let first = match first {
            Ok(()) => panic!("{}: first call succeeded", cell.name),
            Err(error) => error,
        };
        assert_eq!(
            first.raw_os_error(),
            Some(libc::EIO),
            "{}: {first:?}",
            cell.name
        );
        drop(decision_fault);
        drop(cleanup_fault);
        drop(parent);
        assert_eq!(
            fs::read(&sibling).unwrap(),
            b"sibling-bytes",
            "{}",
            cell.name
        );

        if cell.post_rename {
            assert!(!public_path.exists(), "{}: public still present", cell.name);
            match cell.kind {
                CrashMatrixKind::Tree => {
                    fs::create_dir(&public_path).unwrap();
                    fs::set_permissions(&public_path, fs::Permissions::from_mode(0o700)).unwrap();
                    fs::write(public_path.join("replacement"), b"replacement-bytes").unwrap();
                }
                CrashMatrixKind::Regular => {
                    fs::write(&public_path, b"replacement-bytes").unwrap();
                    fs::set_permissions(&public_path, fs::Permissions::from_mode(0o600)).unwrap();
                }
            }
            let planted = fs::symlink_metadata(&public_path).unwrap();
            let namespace = root.join(".mac-worker-rooted-fs");
            let original_state = capture_original_evidence_state(&namespace, &original);
            require_original_evidence_for_phase(&cell, &original_state);
            let intent_path = unique_namespace_role(&namespace, b"cleanup-intent-v1-");
            let intent_record: CleanupIntentV1 =
                cleanup_parse_canonical_json(&fs::read(&intent_path).unwrap()).unwrap();
            let forbidden =
                crash_matrix_forbidden(&root, public, intent_record.component_hex.as_bytes());
            let retry = match cell.kind {
                CrashMatrixKind::Tree => RootedDir::open(&root).unwrap().remove_owned_child(public),
                CrashMatrixKind::Regular => {
                    RootedDir::open(&root).unwrap().remove_owned_regular(public)
                }
            };
            let error = match retry {
                Ok(()) => panic!("{}: post-rename retry succeeded", cell.name),
                Err(error) => error,
            };
            assert_eq!(error.raw_os_error(), Some(libc::ESTALE), "{}", cell.name);
            let needles = forbidden
                .iter()
                .map(|bytes| bytes.as_slice())
                .collect::<Vec<_>>();
            assert_content_free_cleanup_error(&error, Some(libc::ESTALE), None, &needles);
            assert_original_evidence_state_unchanged(
                &original_state,
                &namespace,
                &original,
                cell.name,
            );
            let after = fs::symlink_metadata(&public_path).unwrap_or_else(|error| {
                panic!("{}: replacement missing after retry: {error:?}", cell.name)
            });
            assert_eq!(after.dev(), planted.dev(), "{}", cell.name);
            assert_eq!(after.ino(), planted.ino(), "{}", cell.name);
            match cell.kind {
                CrashMatrixKind::Tree => {
                    assert_eq!(
                        fs::read(public_path.join("replacement")).unwrap(),
                        b"replacement-bytes",
                        "{}",
                        cell.name
                    );
                }
                CrashMatrixKind::Regular => {
                    assert_eq!(
                        fs::read(&public_path).unwrap(),
                        b"replacement-bytes",
                        "{}",
                        cell.name
                    );
                }
            }
            assert_eq!(
                fs::read(&sibling).unwrap(),
                b"sibling-bytes",
                "{}",
                cell.name
            );
            return;
        }

        let reopened = RootedDir::open(&root).unwrap();
        if cell.expect_restore {
            let resumed = match cell.kind {
                CrashMatrixKind::Tree => reopened.resume_pending_owned_child_cleanup(public),
                CrashMatrixKind::Regular => reopened.resume_pending_owned_regular_cleanup(public),
            };
            resumed.unwrap_or_else(|error| panic!("{}: {error:?}", cell.name));
        } else {
            let retry = match cell.kind {
                CrashMatrixKind::Tree => reopened.remove_owned_child(public),
                CrashMatrixKind::Regular => reopened.remove_owned_regular(public),
            };
            retry.unwrap_or_else(|error| panic!("{}: {error:?}", cell.name));
        }
        assert_eq!(
            fs::read(&sibling).unwrap(),
            b"sibling-bytes",
            "{}",
            cell.name
        );
        let namespace = root.join(".mac-worker-rooted-fs");
        assert_cleanup_namespace_roles_retired(&namespace);
        if cell.expect_restore {
            let after = fs::symlink_metadata(&public_path).unwrap();
            assert_eq!(after.dev(), original.dev(), "{}", cell.name);
            assert_eq!(after.ino(), original.ino(), "{}", cell.name);
            match cell.kind {
                CrashMatrixKind::Tree => {
                    assert_eq!(after.permissions().mode() & 0o777, 0o500, "{}", cell.name);
                    assert_eq!(
                        fs::read(public_path.join("leaf")).unwrap(),
                        b"owned-leaf",
                        "{}",
                        cell.name
                    );
                }
                CrashMatrixKind::Regular => {
                    assert_eq!(after.permissions().mode() & 0o777, 0o600, "{}", cell.name);
                    assert_eq!(
                        fs::read(&public_path).unwrap(),
                        b"owned-regular",
                        "{}",
                        cell.name
                    );
                }
            }
        } else {
            assert!(!public_path.exists(), "{}: public survived", cell.name);
        }
    }

    fn crash_matrix_cells() -> Vec<CrashMatrixCell> {
        let mut cells = Vec::new();
        for kind in [CrashMatrixKind::Tree, CrashMatrixKind::Regular] {
            cells.extend([
                CrashMatrixCell {
                    name: "intent file sync",
                    kind,
                    fault: CrashMatrixFault::Cleanup(
                        CleanupFault::AfterCleanupIntentWriteBeforeSync(libc::EIO),
                    ),
                    post_rename: false,
                    expect_restore: false,
                },
                CrashMatrixCell {
                    name: "intent publish",
                    kind,
                    fault: CrashMatrixFault::Cleanup(CleanupFault::AfterCleanupIntentPublish(
                        libc::EIO,
                    )),
                    post_rename: false,
                    expect_restore: false,
                },
                CrashMatrixCell {
                    name: "target quarantine rename",
                    kind,
                    fault: CrashMatrixFault::Cleanup(CleanupFault::AfterCleanupQuarantineRename(
                        libc::EIO,
                    )),
                    post_rename: true,
                    expect_restore: false,
                },
                CrashMatrixCell {
                    name: "namespace sync",
                    kind,
                    fault: CrashMatrixFault::Cleanup(
                        CleanupFault::AfterCleanupQuarantineNamespaceSync(libc::EIO),
                    ),
                    post_rename: true,
                    expect_restore: false,
                },
                CrashMatrixCell {
                    name: "public-parent sync",
                    kind,
                    fault: CrashMatrixFault::Cleanup(
                        CleanupFault::AfterCleanupQuarantineParentSync(libc::EIO),
                    ),
                    post_rename: true,
                    expect_restore: false,
                },
                CrashMatrixCell {
                    name: "decision publish",
                    kind,
                    fault: CrashMatrixFault::Decision(CleanupDecisionFault::AfterSourceSync(
                        libc::EIO,
                    )),
                    post_rename: true,
                    expect_restore: false,
                },
                CrashMatrixCell {
                    name: "final target unlink",
                    kind,
                    fault: CrashMatrixFault::Cleanup(CleanupFault::BeforeFinalRootRemoval(
                        libc::EIO,
                    )),
                    post_rename: true,
                    expect_restore: false,
                },
                CrashMatrixCell {
                    name: "placeholder removal",
                    kind,
                    fault: CrashMatrixFault::Cleanup(CleanupFault::AfterCleanupPlaceholderRemoval(
                        libc::EIO,
                    )),
                    post_rename: true,
                    expect_restore: false,
                },
                CrashMatrixCell {
                    name: "operation removal",
                    kind,
                    fault: CrashMatrixFault::Cleanup(CleanupFault::AfterCleanupOperationRemoval(
                        libc::EIO,
                    )),
                    post_rename: true,
                    expect_restore: false,
                },
                CrashMatrixCell {
                    name: "decision removal",
                    kind,
                    fault: CrashMatrixFault::Decision(CleanupDecisionFault::AfterDecisionRetire(
                        libc::EIO,
                    )),
                    post_rename: true,
                    expect_restore: false,
                },
                CrashMatrixCell {
                    name: "intent removal",
                    kind,
                    fault: CrashMatrixFault::Decision(CleanupDecisionFault::AfterIntentRetire(
                        libc::EIO,
                    )),
                    post_rename: false,
                    expect_restore: false,
                },
                CrashMatrixCell {
                    name: "restore after destination sync",
                    kind,
                    fault: CrashMatrixFault::RestoreThen(
                        CleanupDecisionFault::AfterRestoreDestinationSync(libc::EIO),
                    ),
                    post_rename: false,
                    expect_restore: true,
                },
            ]);
        }
        cells.push(CrashMatrixCell {
            name: "first recursive removal",
            kind: CrashMatrixKind::Tree,
            fault: CrashMatrixFault::Cleanup(CleanupFault::AfterFirstRemoval(libc::EIO)),
            post_rename: true,
            expect_restore: false,
        });
        cells
    }

    #[test]
    fn cleanup_crash_matrix_tree_and_regular_one_safe_result() {
        for cell in crash_matrix_cells() {
            run_crash_matrix_cell(cell);
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum EvidenceKind {
        Tree,
        Regular,
    }

    #[derive(Clone, Copy, Debug)]
    enum EvidencePlant {
        EmptyLegacyCleanupUuid,
        NonemptyLegacyCleanupUuid,
        PublicRemoveUuid,
        UnknownIntentField,
        ConcatenatedJson,
        OversizedJson,
        NoncanonicalHex,
        WrongParentIdentity,
        WrongNamespaceIdentity,
        WrongTargetIdentity,
        SymlinkIntent,
        HardlinkIntent,
        IntentMode0644,
        IntentOwner,
    }

    fn evidence_expected(plant: &EvidencePlant) -> (Option<i32>, Option<std::io::ErrorKind>, bool) {
        match plant {
            EvidencePlant::EmptyLegacyCleanupUuid | EvidencePlant::NonemptyLegacyCleanupUuid => {
                (Some(libc::ESTALE), None, false)
            }
            EvidencePlant::PublicRemoveUuid => (None, None, true),
            EvidencePlant::UnknownIntentField
            | EvidencePlant::ConcatenatedJson
            | EvidencePlant::NoncanonicalHex => (Some(libc::EINVAL), None, false),
            EvidencePlant::OversizedJson => (Some(libc::EFBIG), None, false),
            EvidencePlant::WrongParentIdentity | EvidencePlant::WrongNamespaceIdentity => {
                (Some(libc::ESTALE), None, false)
            }
            EvidencePlant::WrongTargetIdentity => (Some(libc::EINVAL), None, false),
            EvidencePlant::SymlinkIntent
            | EvidencePlant::HardlinkIntent
            | EvidencePlant::IntentMode0644
            | EvidencePlant::IntentOwner => {
                (None, Some(std::io::ErrorKind::PermissionDenied), false)
            }
        }
    }

    const OVERSIZED_UNIQUE_MUTATION: &[u8] = b"oversized-unique-mutation-marker";

    fn run_published_evidence_case(kind: EvidenceKind, plant: EvidencePlant) {
        if matches!(plant, EvidencePlant::IntentOwner) && unsafe { libc::geteuid() } != 0 {
            // Owner-violation coverage needs chown(2) to a foreign uid.
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let physical = temp.path().canonicalize().unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let root = physical.join("parent");
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let public = match kind {
            EvidenceKind::Tree => "child",
            EvidenceKind::Regular => "entry",
        };
        let public_path = root.join(public);
        let sibling = root.join("sibling");
        fs::write(&sibling, b"sibling-bytes").unwrap();
        fs::set_permissions(&sibling, fs::Permissions::from_mode(0o600)).unwrap();
        let namespace = root.join(".mac-worker-rooted-fs");
        let (success, raw, kind_expected) = {
            let (raw, kind_expected, success) = evidence_expected(&plant);
            (success, raw, kind_expected)
        };

        match plant {
            EvidencePlant::EmptyLegacyCleanupUuid | EvidencePlant::NonemptyLegacyCleanupUuid => {
                fs::create_dir(&namespace).unwrap();
                fs::set_permissions(&namespace, fs::Permissions::from_mode(0o700)).unwrap();
                let legacy = namespace.join("cleanup-303f0f4a-6b5c-4d8e-9f00-112233445566");
                fs::create_dir(&legacy).unwrap();
                if matches!(plant, EvidencePlant::NonemptyLegacyCleanupUuid) {
                    fs::write(legacy.join("sentinel"), b"preserve-legacy").unwrap();
                }
                let inode = fs::symlink_metadata(&legacy).unwrap().ino();
                let error = match kind {
                    EvidenceKind::Tree => RootedDir::open(&root)
                        .unwrap()
                        .remove_owned_child("absent")
                        .unwrap_err(),
                    EvidenceKind::Regular => RootedDir::open(&root)
                        .unwrap()
                        .remove_owned_regular("absent")
                        .unwrap_err(),
                };
                let forbidden = [
                    root.as_os_str().as_bytes(),
                    b"preserve-legacy".as_slice(),
                    b"303f0f4a-6b5c-4d8e-9f00-112233445566".as_slice(),
                    public.as_bytes(),
                    b"absent".as_slice(),
                ];
                assert_eq!(error.raw_os_error(), raw, "{kind:?} {plant:?}: {error:?}");
                assert_content_free_cleanup_error(&error, raw, kind_expected, &forbidden);
                assert_eq!(fs::symlink_metadata(&legacy).unwrap().ino(), inode);
                if matches!(plant, EvidencePlant::NonemptyLegacyCleanupUuid) {
                    assert_eq!(
                        fs::read(legacy.join("sentinel")).unwrap(),
                        b"preserve-legacy"
                    );
                } else {
                    assert_eq!(fs::read_dir(&legacy).unwrap().count(), 0);
                }
                assert_eq!(fs::read(&sibling).unwrap(), b"sibling-bytes");
                return;
            }
            EvidencePlant::PublicRemoveUuid => {
                let legacy = root.join("remove-303f0f4a-6b5c-4d8e-9f00-112233445566");
                fs::write(&legacy, b"legacy-public-remove").unwrap();
                let inode = fs::symlink_metadata(&legacy).unwrap().ino();
                match kind {
                    EvidenceKind::Tree => RootedDir::open(&root)
                        .unwrap()
                        .remove_owned_child("absent")
                        .unwrap(),
                    EvidenceKind::Regular => RootedDir::open(&root)
                        .unwrap()
                        .remove_owned_regular("absent")
                        .unwrap(),
                }
                assert_eq!(fs::read(&legacy).unwrap(), b"legacy-public-remove");
                assert_eq!(fs::symlink_metadata(&legacy).unwrap().ino(), inode);
                assert_eq!(fs::read(&sibling).unwrap(), b"sibling-bytes");
                assert_cleanup_namespace_roles_retired(&namespace);
                return;
            }
            _ => {}
        }

        match kind {
            EvidenceKind::Tree => {
                fs::create_dir(&public_path).unwrap();
                fs::set_permissions(&public_path, fs::Permissions::from_mode(0o700)).unwrap();
                fs::write(public_path.join("leaf"), b"owned-leaf").unwrap();
            }
            EvidenceKind::Regular => {
                fs::write(&public_path, b"owned-regular").unwrap();
                fs::set_permissions(&public_path, fs::Permissions::from_mode(0o600)).unwrap();
            }
        }
        let parent = RootedDir::open(&root).unwrap();
        let interrupt =
            CleanupFaultOverride::set(CleanupFault::AfterCleanupQuarantineRename(libc::EIO));
        let first = match kind {
            EvidenceKind::Tree => parent.remove_owned_child(public).unwrap_err(),
            EvidenceKind::Regular => parent.remove_owned_regular(public).unwrap_err(),
        };
        assert_eq!(first.raw_os_error(), Some(libc::EIO));
        drop(interrupt);
        drop(parent);

        let intent_path = unique_namespace_role(&namespace, b"cleanup-intent-v1-");
        let operation = unique_namespace_role(&namespace, b"cleanup-op-v1-");
        let quarantine_prefix = match kind {
            EvidenceKind::Tree => b"cleanup-tree-v1-".as_slice(),
            EvidenceKind::Regular => b"cleanup-regular-v1-".as_slice(),
        };
        let quarantine = unique_namespace_role(&namespace, quarantine_prefix);
        let canonical = fs::read(&intent_path).unwrap();
        let intent_inode = fs::symlink_metadata(&intent_path).unwrap().ino();
        let operation_inode = fs::symlink_metadata(&operation).unwrap().ino();
        let quarantine_inode = fs::symlink_metadata(&quarantine).unwrap().ino();
        let intent_record: CleanupIntentV1 = cleanup_parse_canonical_json(&canonical).unwrap();
        let component_hex = intent_record.component_hex.clone();
        let mut planted = canonical.clone();
        let mut auxiliary = None;
        match plant {
            EvidencePlant::UnknownIntentField => {
                assert_eq!(planted.pop(), Some(b'}'));
                planted.extend_from_slice(br#","extra":1}"#);
                fs::write(&intent_path, &planted).unwrap();
            }
            EvidencePlant::ConcatenatedJson => {
                planted.extend_from_slice(&canonical);
                fs::write(&intent_path, &planted).unwrap();
            }
            EvidencePlant::OversizedJson => {
                planted = vec![b'x'; 4097];
                let start = 128;
                let end = start + OVERSIZED_UNIQUE_MUTATION.len();
                planted[start..end].copy_from_slice(OVERSIZED_UNIQUE_MUTATION);
                fs::write(&intent_path, &planted).unwrap();
            }
            EvidencePlant::NoncanonicalHex => {
                let mut rewritten = intent_record;
                rewritten.component_hex = rewritten.component_hex.to_ascii_uppercase();
                planted = cleanup_canonical_json(&rewritten).unwrap();
                fs::write(&intent_path, &planted).unwrap();
            }
            EvidencePlant::WrongParentIdentity => {
                let mut rewritten = intent_record;
                rewritten.parent.inode = rewritten.parent.inode.wrapping_add(1);
                planted = cleanup_canonical_json(&rewritten).unwrap();
                fs::write(&intent_path, &planted).unwrap();
            }
            EvidencePlant::WrongNamespaceIdentity => {
                let mut rewritten = intent_record;
                rewritten.namespace.inode = rewritten.namespace.inode.wrapping_add(1);
                planted = cleanup_canonical_json(&rewritten).unwrap();
                fs::write(&intent_path, &planted).unwrap();
            }
            EvidencePlant::WrongTargetIdentity => {
                let mut rewritten = intent_record;
                rewritten.target.inode = rewritten.target.inode.wrapping_add(1);
                planted = cleanup_canonical_json(&rewritten).unwrap();
                fs::write(&intent_path, &planted).unwrap();
            }
            EvidencePlant::SymlinkIntent => {
                let backup = root.parent().unwrap().join("intent-symlink-target");
                fs::write(&backup, &canonical).unwrap();
                fs::set_permissions(&backup, fs::Permissions::from_mode(0o600)).unwrap();
                fs::remove_file(&intent_path).unwrap();
                std::os::unix::fs::symlink(&backup, &intent_path).unwrap();
                planted = canonical.clone();
                let meta = fs::symlink_metadata(&backup).unwrap();
                auxiliary = Some((backup, meta, canonical.clone()));
            }
            EvidencePlant::HardlinkIntent => {
                let extra = root.parent().unwrap().join("intent-hardlink-alias");
                fs::hard_link(&intent_path, &extra).unwrap();
                let meta = fs::symlink_metadata(&extra).unwrap();
                auxiliary = Some((extra, meta, canonical.clone()));
            }
            EvidencePlant::IntentMode0644 => {
                fs::set_permissions(&intent_path, fs::Permissions::from_mode(0o644)).unwrap();
            }
            EvidencePlant::IntentOwner => {
                let foreign_uid = 1;
                assert_ne!(foreign_uid, unsafe { libc::geteuid() });
                let path = std::ffi::CString::new(intent_path.as_os_str().as_bytes()).unwrap();
                assert_eq!(
                    unsafe { libc::chown(path.as_ptr(), foreign_uid, u32::MAX) },
                    0,
                    "chown intent to foreign uid"
                );
            }
            EvidencePlant::EmptyLegacyCleanupUuid
            | EvidencePlant::NonemptyLegacyCleanupUuid
            | EvidencePlant::PublicRemoveUuid => unreachable!(),
        }
        let forbidden = {
            let mut needles = vec![
                root.as_os_str().as_bytes().to_vec(),
                public.as_bytes().to_vec(),
                component_hex.as_bytes().to_vec(),
                intent_path.file_name().unwrap().as_bytes().to_vec(),
                operation.file_name().unwrap().as_bytes().to_vec(),
                quarantine.file_name().unwrap().as_bytes().to_vec(),
                b"owned-leaf".to_vec(),
                b"owned-regular".to_vec(),
                b"sibling-bytes".to_vec(),
                b"replacement-bytes".to_vec(),
            ];
            if let Some((aux_path, _, aux_bytes)) = auxiliary.as_ref() {
                needles.push(aux_path.as_os_str().as_bytes().to_vec());
                needles.push(aux_path.file_name().unwrap().as_bytes().to_vec());
                needles.push(aux_bytes.clone());
            }
            needles.push(planted.clone());
            if matches!(plant, EvidencePlant::OversizedJson) {
                needles.push(OVERSIZED_UNIQUE_MUTATION.to_vec());
            }
            needles
        };

        let error = match kind {
            EvidenceKind::Tree => RootedDir::open(&root)
                .unwrap()
                .remove_owned_child(public)
                .unwrap_err(),
            EvidenceKind::Regular => RootedDir::open(&root)
                .unwrap()
                .remove_owned_regular(public)
                .unwrap_err(),
        };
        assert!(!success);
        let needles = forbidden
            .iter()
            .map(|bytes| bytes.as_slice())
            .collect::<Vec<_>>();
        assert_content_free_cleanup_error(&error, raw, kind_expected, &needles);
        match plant {
            EvidencePlant::SymlinkIntent => {
                assert!(
                    fs::symlink_metadata(&intent_path)
                        .unwrap()
                        .file_type()
                        .is_symlink()
                );
            }
            EvidencePlant::HardlinkIntent => {
                assert_eq!(fs::symlink_metadata(&intent_path).unwrap().nlink(), 2);
                assert_eq!(fs::read(&intent_path).unwrap(), canonical);
            }
            EvidencePlant::IntentMode0644 => {
                assert_eq!(
                    fs::symlink_metadata(&intent_path)
                        .unwrap()
                        .permissions()
                        .mode()
                        & 0o777,
                    0o644
                );
                assert_eq!(fs::read(&intent_path).unwrap(), canonical);
            }
            EvidencePlant::IntentOwner => {
                let meta = fs::symlink_metadata(&intent_path).unwrap();
                assert_eq!(meta.ino(), intent_inode);
                assert_eq!(meta.permissions().mode() & 0o777, 0o600);
                assert_eq!(meta.uid(), 1);
                assert_eq!(fs::read(&intent_path).unwrap(), canonical);
            }
            _ => {
                assert_eq!(fs::read(&intent_path).unwrap(), planted);
            }
        }
        if let Some((aux_path, aux_meta, aux_bytes)) = auxiliary.as_ref() {
            let after = fs::symlink_metadata(aux_path).unwrap();
            assert_eq!(after.dev(), aux_meta.dev());
            assert_eq!(after.ino(), aux_meta.ino());
            assert_eq!(fs::read(aux_path).unwrap(), aux_bytes.as_slice());
        }
        if !matches!(plant, EvidencePlant::SymlinkIntent) {
            assert_eq!(
                fs::symlink_metadata(&intent_path).unwrap().ino(),
                intent_inode
            );
        }
        assert_eq!(
            fs::symlink_metadata(&operation).unwrap().ino(),
            operation_inode
        );
        assert_eq!(
            fs::symlink_metadata(&quarantine).unwrap().ino(),
            quarantine_inode
        );
        assert_eq!(fs::read(&sibling).unwrap(), b"sibling-bytes");
        assert!(!public_path.exists());
    }

    #[test]
    fn cleanup_evidence_legacy_and_malformed_published_intent_matrix() {
        let cases = [
            (EvidenceKind::Tree, EvidencePlant::EmptyLegacyCleanupUuid),
            (EvidenceKind::Regular, EvidencePlant::EmptyLegacyCleanupUuid),
            (
                EvidenceKind::Regular,
                EvidencePlant::NonemptyLegacyCleanupUuid,
            ),
            (EvidenceKind::Tree, EvidencePlant::PublicRemoveUuid),
            (EvidenceKind::Tree, EvidencePlant::UnknownIntentField),
            (EvidenceKind::Regular, EvidencePlant::UnknownIntentField),
            (EvidenceKind::Tree, EvidencePlant::ConcatenatedJson),
            (EvidenceKind::Regular, EvidencePlant::ConcatenatedJson),
            (EvidenceKind::Tree, EvidencePlant::OversizedJson),
            (EvidenceKind::Tree, EvidencePlant::NoncanonicalHex),
            (EvidenceKind::Regular, EvidencePlant::NoncanonicalHex),
            (EvidenceKind::Tree, EvidencePlant::WrongParentIdentity),
            (EvidenceKind::Regular, EvidencePlant::WrongParentIdentity),
            (EvidenceKind::Tree, EvidencePlant::WrongNamespaceIdentity),
            (EvidenceKind::Regular, EvidencePlant::WrongNamespaceIdentity),
            (EvidenceKind::Tree, EvidencePlant::WrongTargetIdentity),
            (EvidenceKind::Regular, EvidencePlant::WrongTargetIdentity),
            (EvidenceKind::Tree, EvidencePlant::SymlinkIntent),
            (EvidenceKind::Regular, EvidencePlant::SymlinkIntent),
            (EvidenceKind::Tree, EvidencePlant::HardlinkIntent),
            (EvidenceKind::Regular, EvidencePlant::HardlinkIntent),
            (EvidenceKind::Tree, EvidencePlant::IntentMode0644),
            (EvidenceKind::Tree, EvidencePlant::IntentOwner),
            (EvidenceKind::Regular, EvidencePlant::IntentOwner),
        ];
        for (kind, plant) in cases {
            run_published_evidence_case(kind, plant);
        }
    }
}
