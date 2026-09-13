//! Started-versus-installed identity for a long-running `worker` process.
//!
//! Compared against the file currently at that path (inode, size, mtime).
//! A replaced install changes at least one of those without needing a
//! build-id in the binary. Host outbox watchers and the laptop dashboard
//! share this check.

use std::{
    fs,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

use serde::{Deserialize, Serialize};

/// Identity of the CLI file a long-running process started from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BinaryIdentity {
    pub path: PathBuf,
    pub inode: u64,
    pub size: u64,
    pub mtime_millis: u64,
}

impl BinaryIdentity {
    pub fn from_path(path: &Path) -> Option<Self> {
        let metadata = fs::metadata(path).ok()?;
        let mtime_millis = metadata
            .modified()
            .ok()?
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_millis()
            .try_into()
            .ok()?;
        Some(Self {
            path: path.to_path_buf(),
            inode: metadata.ino(),
            size: metadata.len(),
            mtime_millis,
        })
    }

    pub fn from_current_exe() -> Option<Self> {
        std::env::current_exe()
            .ok()
            .and_then(|path| Self::from_path(&path))
    }
}

/// Started-versus-installed identity for a long-running process.
pub trait BinaryIdentitySource: Send + Sync + 'static {
    fn started(&self) -> Option<BinaryIdentity>;
    fn installed(&self) -> Option<BinaryIdentity>;
}

/// Records the executable at construction and restats that same path later.
pub struct SystemBinaryIdentitySource {
    started: Option<BinaryIdentity>,
}

impl SystemBinaryIdentitySource {
    pub fn capture() -> Self {
        Self {
            started: BinaryIdentity::from_current_exe(),
        }
    }
}

impl BinaryIdentitySource for SystemBinaryIdentitySource {
    fn started(&self) -> Option<BinaryIdentity> {
        self.started.clone()
    }

    fn installed(&self) -> Option<BinaryIdentity> {
        self.started
            .as_ref()
            .and_then(|started| BinaryIdentity::from_path(&started.path))
    }
}

/// Test double that returns fixed identities.
#[derive(Debug, Clone)]
pub struct FixedBinaryIdentitySource {
    pub started: Option<BinaryIdentity>,
    pub installed: Option<BinaryIdentity>,
}

impl BinaryIdentitySource for FixedBinaryIdentitySource {
    fn started(&self) -> Option<BinaryIdentity> {
        self.started.clone()
    }

    fn installed(&self) -> Option<BinaryIdentity> {
        self.installed.clone()
    }
}

/// True when both identities are known and they differ.
pub fn binary_is_outdated(source: &dyn BinaryIdentitySource) -> bool {
    match (source.started(), source.installed()) {
        (Some(started), Some(installed)) => started != installed,
        _ => false,
    }
}
