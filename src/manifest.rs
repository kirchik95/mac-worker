use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotManifest {
    pub version: u32,
    pub project_id: String,
    pub worktree_id: String,
    pub head: Option<String>,
    pub branch: Option<String>,
    pub dirty: bool,
    pub relative_working_dir: String,
    pub entries: Vec<ManifestEntry>,
    pub tracked_deletions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestEntry {
    pub path: String,
    pub kind: ManifestEntryKind,
    pub mode: u32,
    pub size: u64,
    pub sha256: String,
    pub symlink_target: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ManifestEntryKind {
    File,
    Symlink,
    Directory,
}

impl SnapshotManifest {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut canonical = self.clone();
        canonical
            .entries
            .sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
        canonical
            .tracked_deletions
            .sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        serde_json::to_vec(&canonical)
    }

    pub fn digest(&self) -> Result<String, serde_json::Error> {
        let bytes = self.canonical_bytes()?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}
