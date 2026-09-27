//! Started-versus-installed identity for a long-running `worker` process.
//!
//! Compared against the file currently at that path (inode, size, mtime).
//! A replaced install changes at least one of those. The running executable's
//! SHA-256 is computed once per process and cached for probes and skew checks.
//! Host outbox watchers and the laptop dashboard share the inode check.

use std::{
    fs,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::OnceLock,
    time::UNIX_EPOCH,
};

use sha2::{Digest, Sha256};

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

/// SHA-256 of the running executable, computed once per process.
///
/// `None` when the executable path cannot be read. Callers treat that as
/// "identity unknown", not as a mismatch.
pub fn current_binary_sha256() -> Option<String> {
    static CACHE: OnceLock<Option<String>> = OnceLock::new();
    CACHE
        .get_or_init(|| {
            let path = std::env::current_exe().ok()?;
            let bytes = fs::read(path).ok()?;
            Some(sha256_hex(&bytes))
        })
        .clone()
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

/// True when both identities are known and they differ.
pub fn binary_is_outdated(source: &dyn BinaryIdentitySource) -> bool {
    match (source.started(), source.installed()) {
        (Some(started), Some(installed)) => started != installed,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{current_binary_sha256, sha256_hex};

    #[test]
    fn current_binary_sha256_matches_the_executable_and_stays_cached() {
        let path = std::env::current_exe().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let expected = sha256_hex(&bytes);
        assert_eq!(current_binary_sha256().as_deref(), Some(expected.as_str()));
        assert_eq!(current_binary_sha256().as_deref(), Some(expected.as_str()));
        assert_eq!(expected.len(), 64);
    }
}
