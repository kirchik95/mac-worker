//! Journal storage primitives. Public event contracts are adapted by the facade.
use std::io;

use serde::{Deserialize, Serialize};

use crate::rooted_fs::{PrivateEntryIdentity, RootedDir};

pub(super) const SEGMENT_BYTES: usize = 256 * 1024;
pub(super) const RETAINED_SEGMENTS: usize = 64;
pub(super) const EVENT_BYTES: usize = 1024;
pub(super) const BATCH_EVENTS: usize = 32;
pub(super) const BATCH_BYTES: usize = 32 * 1024;
pub(super) const METADATA_BYTES: usize = 64 * 1024;
pub(super) const EVIDENCE_BYTES: usize = 512 * 1024;
pub(super) const EVIDENCE_FILES: usize = 32;
pub(super) const TOTAL_BYTES: usize = 18 * 1024 * 1024;
pub(super) const TOTAL_FILES: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Binding {
    device: u64,
    inode: u64,
    kind: u32,
    owner: u32,
    mode: u32,
}

impl From<PrivateEntryIdentity> for Binding {
    fn from(value: PrivateEntryIdentity) -> Self {
        Self {
            device: value.device,
            inode: value.inode,
            kind: value.kind,
            owner: value.owner,
            mode: value.mode,
        }
    }
}

impl From<Binding> for PrivateEntryIdentity {
    fn from(value: Binding) -> Self {
        Self {
            device: value.device,
            inode: value.inode,
            kind: value.kind,
            owner: value.owner,
            mode: value.mode,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Segment {
    name: String,
    first: u64,
    /// None denotes an empty active segment, never an invented sequence.
    last: Option<u64>,
    committed_len: usize,
    binding: Binding,
    sealed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Manifest {
    schema_version: u32,
    journal_id: String,
    head: u64,
    oldest: u64,
    segments: Vec<Segment>,
}

fn unavailable() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "CONTROLLER_EVENTS_UNAVAILABLE")
}

fn canonical_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|id| id.to_string() == value)
}

fn canonical_seq(value: &serde_json::Value) -> io::Result<u64> {
    let text = value.as_str().ok_or_else(unavailable)?;
    let number: u64 = text.parse().map_err(|_| unavailable())?;
    if number.to_string() != text {
        return Err(unavailable());
    }
    Ok(number)
}

fn validate_manifest(manifest: &Manifest) -> io::Result<()> {
    if manifest.schema_version != 1
        || !canonical_uuid(&manifest.journal_id)
        || manifest.segments.is_empty()
        || manifest.segments.len() > RETAINED_SEGMENTS
        || serde_json::to_vec(manifest)
            .map_err(|_| unavailable())?
            .len()
            > METADATA_BYTES
    {
        return Err(unavailable());
    }
    let mut next = manifest.oldest;
    let mut last = None;
    for (index, segment) in manifest.segments.iter().enumerate() {
        if segment.first == 0
            || segment.first != next
            || segment.name != format!("segment-{}.jsonl", segment.first)
            || segment.committed_len > SEGMENT_BYTES
            || segment.binding.kind != libc::S_IFREG as u32
            || segment.binding.owner != unsafe { libc::geteuid() }
            || segment.binding.mode != 0o600
            || segment.sealed != (index + 1 < manifest.segments.len())
        {
            return Err(unavailable());
        }
        match segment.last {
            Some(end) if end >= segment.first && segment.committed_len > 0 => {
                last = Some(end);
                // u64::MAX may be a valid final committed sequence.
                next = end.checked_add(1).unwrap_or(0);
            }
            None if !segment.sealed && segment.committed_len == 0 => {}
            _ => return Err(unavailable()),
        }
    }
    if last.unwrap_or(0) != manifest.head {
        return Err(unavailable());
    }
    Ok(())
}

fn committed_records(
    root: &RootedDir,
    segment: &Segment,
    journal_id: &str,
    allow_tail: bool,
) -> io::Result<Vec<serde_json::Value>> {
    if Binding::from(root.private_entry_identity(&segment.name)?) != segment.binding {
        return Err(unavailable());
    }
    let bytes = root.read_private_regular(&segment.name, SEGMENT_BYTES as u64)?;
    if Binding::from(root.private_entry_identity(&segment.name)?) != segment.binding
        || segment.committed_len > bytes.len()
        || ((!allow_tail || segment.sealed) && bytes.len() != segment.committed_len)
    {
        return Err(unavailable());
    }
    decode_records(
        &bytes[..segment.committed_len],
        journal_id,
        segment.first,
        segment.last,
    )
}

fn decode_records(
    bytes: &[u8],
    journal_id: &str,
    first: u64,
    last: Option<u64>,
) -> io::Result<Vec<serde_json::Value>> {
    if bytes.is_empty() {
        return if last.is_none() {
            Ok(Vec::new())
        } else {
            Err(unavailable())
        };
    }
    if bytes.last() != Some(&b'\n') || last.is_none() {
        return Err(unavailable());
    }
    let mut records = Vec::new();
    let mut seq = first;
    for line in bytes[..bytes.len() - 1].split(|byte| *byte == b'\n') {
        if line.len() + 1 > EVENT_BYTES || line.is_empty() {
            return Err(unavailable());
        }
        let event: serde_json::Value = serde_json::from_slice(line).map_err(|_| unavailable())?;
        if canonical_seq(&event["seq"])? != seq
            || event["journal_id"].as_str() != Some(journal_id)
            || event["schema_version"].as_u64() != Some(1)
            || event["time_millis"].as_u64().is_none()
            || event["kind"].as_str().is_none_or(str::is_empty)
            || !event["data"].is_object()
        {
            return Err(unavailable());
        }
        records.push(event);
        seq = seq.checked_add(1).unwrap_or(0);
    }
    if records
        .last()
        .map(|record| canonical_seq(&record["seq"]))
        .transpose()?
        != last
    {
        return Err(unavailable());
    }
    Ok(records)
}

fn completion_suffix<'a>(pending: &'a [u8], tail: &[u8]) -> io::Result<&'a [u8]> {
    if !pending.starts_with(tail) {
        return Err(unavailable());
    }
    Ok(&pending[tail.len()..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    const EPOCH: &str = "614dc3be-668f-4922-bd31-b1d7a0056790";

    fn record(seq: u64) -> Vec<u8> {
        format!(
            "{{\"schema_version\":1,\"journal_id\":\"{EPOCH}\",\"seq\":\"{seq}\",\"time_millis\":0,\"kind\":\"controller.drained\",\"data\":{{\"drained\":true}}}}\n"
        ).into_bytes()
    }

    fn fixture(bytes: &[u8], sealed: bool) -> (TempDir, RootedDir, Manifest) {
        let temp = tempfile::tempdir().unwrap();
        let root = RootedDir::create(&temp.path().join("events")).unwrap();
        let file = root
            .write_new_private_file("segment-1.jsonl", bytes)
            .unwrap();
        file.sync_all().unwrap();
        root.sync_root().unwrap();
        let manifest = Manifest {
            schema_version: 1,
            journal_id: EPOCH.into(),
            head: 1,
            oldest: 1,
            segments: vec![Segment {
                name: "segment-1.jsonl".into(),
                first: 1,
                last: Some(1),
                committed_len: bytes.len(),
                binding: root
                    .private_entry_identity("segment-1.jsonl")
                    .unwrap()
                    .into(),
                sealed,
            }],
        };
        (temp, root, manifest)
    }

    #[test]
    fn sealed_binding_is_pinned_across_owned_copy_replacement() {
        let (_temp, root, manifest) = fixture(&record(1), true);
        let original = root.path().join("segment-1.jsonl");
        let copy = root.path().join("replacement");
        fs::write(&copy, record(1)).unwrap();
        fs::set_permissions(&copy, fs::Permissions::from_mode(0o600)).unwrap();
        fs::rename(copy, original).unwrap();
        assert!(committed_records(&root, &manifest.segments[0], EPOCH, false).is_err());
    }

    #[test]
    fn sealed_extra_bytes_and_committed_sequence_gaps_fail_closed() {
        let (_temp, root, mut manifest) = fixture(&record(2), true);
        assert!(committed_records(&root, &manifest.segments[0], EPOCH, false).is_err());
        manifest.segments[0].first = 2;
        manifest.segments[0].last = Some(2);
        manifest.segments[0].committed_len -= 1;
        assert!(committed_records(&root, &manifest.segments[0], EPOCH, true).is_err());
    }

    #[test]
    fn pending_tail_is_completed_only_when_exact_prefix_matches() {
        let pending = b"first\nsecond\n";
        assert_eq!(completion_suffix(pending, b"first\nsec").unwrap(), b"ond\n");
        assert_eq!(completion_suffix(pending, pending).unwrap(), b"");
        assert!(completion_suffix(pending, b"first\nforeign").is_err());
        assert!(completion_suffix(pending, b"first\nsecond\nextra").is_err());
    }

    #[test]
    fn manifest_requires_bounded_contiguous_retained_ranges() {
        let (_temp, _root, mut manifest) = fixture(&record(1), false);
        validate_manifest(&manifest).unwrap();
        manifest.segments[0].name = "segment-01.jsonl".into();
        assert!(validate_manifest(&manifest).is_err());
        manifest.segments[0].name = "segment-1.jsonl".into();
        manifest.segments[0].committed_len = SEGMENT_BYTES + 1;
        assert!(validate_manifest(&manifest).is_err());
        manifest.segments[0].committed_len = record(1).len();
        manifest.segments.push(manifest.segments[0].clone());
        assert!(validate_manifest(&manifest).is_err());
    }
}
