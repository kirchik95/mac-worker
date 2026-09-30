//! Journal storage primitives. Public event contracts are adapted by the facade.
use std::io;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::rooted_fs::PrivateRolePoint;
use crate::rooted_fs::{PrivateEntryIdentity, PrivateRegularRole, PrivateRoleHooks, RootedDir};

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum RoleKind {
    Initialization,
    Manifest,
    Pending,
    Segment,
    Retirement,
}

impl RoleKind {
    const ALL: [Self; 5] = [
        Self::Initialization,
        Self::Manifest,
        Self::Pending,
        Self::Segment,
        Self::Retirement,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Initialization => "initialization",
            Self::Manifest => "manifest",
            Self::Pending => "pending",
            Self::Segment => "segment",
            Self::Retirement => "retirement",
        }
    }

    fn stage(self) -> String {
        format!("{}.stage", self.name())
    }
    fn evidence(self) -> String {
        format!("{}.role", self.name())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FaultPoint {
    Role(RoleKind, PrivateRolePoint),
    PendingDurable,
    PartialAppend,
    SegmentSynced,
    ManifestCommitted,
    PendingRemoved,
    Retired,
}

pub(super) trait FaultHooks: Send + Sync {
    fn at(&self, point: FaultPoint) -> io::Result<()>;
}

pub(super) struct NoFaults;
impl FaultHooks for NoFaults {
    fn at(&self, _: FaultPoint) -> io::Result<()> {
        Ok(())
    }
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn bounded_json<T: Serialize>(value: &T, limit: usize) -> io::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|_| unavailable())?;
    if bytes.len() > limit {
        return Err(unavailable());
    }
    Ok(bytes)
}

fn read_json<T: Serialize + serde::de::DeserializeOwned>(
    root: &RootedDir,
    name: &str,
    limit: usize,
) -> io::Result<T> {
    let bytes = root.read_private_regular(name, limit as u64)?;
    let value: T = serde_json::from_slice(&bytes).map_err(|_| unavailable())?;
    if bounded_json(&value, limit)? != bytes {
        return Err(unavailable());
    }
    Ok(value)
}

#[derive(Default, Debug)]
pub(super) struct DiskUsage {
    pub(super) bytes: usize,
    pub(super) files: usize,
    pub(super) evidence_bytes: usize,
    pub(super) evidence_files: usize,
    entries: usize,
}

impl DiskUsage {
    fn admit(&self, bytes: usize, files: usize) -> io::Result<()> {
        if self
            .bytes
            .checked_add(bytes)
            .is_none_or(|sum| sum > TOTAL_BYTES)
            || self
                .files
                .checked_add(files)
                .is_none_or(|sum| sum > TOTAL_FILES)
            || self.evidence_bytes > EVIDENCE_BYTES
            || self.evidence_files > EVIDENCE_FILES
        {
            return Err(unavailable());
        }
        Ok(())
    }
}

fn valid_segment_name(name: &str) -> bool {
    name.strip_prefix("segment-")
        .and_then(|s| s.strip_suffix(".jsonl"))
        .is_some_and(|s| s.parse::<u64>().is_ok_and(|n| n > 0 && n.to_string() == s))
}

fn valid_role_target(kind: RoleKind, target: &str) -> bool {
    if kind == RoleKind::Segment {
        valid_segment_name(target)
    } else {
        target == format!("{}.json", kind.name())
    }
}

fn audit(root: &RootedDir) -> io::Result<DiskUsage> {
    fn visit(root: &RootedDir, device: u64, depth: usize, usage: &mut DiskUsage) -> io::Result<()> {
        if depth > 4 {
            return Err(unavailable());
        }
        let names = root.list_names()?;
        usage.entries = usage
            .entries
            .checked_add(names.len())
            .ok_or_else(unavailable)?;
        if usage.entries > TOTAL_FILES * 2 {
            return Err(unavailable());
        }
        for raw in names {
            let name = std::str::from_utf8(&raw).map_err(|_| unavailable())?;
            let binding = root.private_entry_identity(name)?;
            if binding.device != device {
                return Err(unavailable());
            }
            if binding.kind == libc::S_IFDIR as u32 {
                if depth == 0 && name != ".mac-worker-rooted-fs" {
                    return Err(unavailable());
                }
                let child = root.open_private_direct_child_on_device(name, device)?;
                visit(&child, device, depth + 1, usage)?;
            } else {
                if depth == 0 && binding.mode != 0o600 {
                    return Err(unavailable());
                }
                let size =
                    usize::try_from(root.open_private_regular_handle(name)?.metadata()?.len())
                        .map_err(|_| unavailable())?;
                usage.bytes = usage.bytes.checked_add(size).ok_or_else(unavailable)?;
                usage.files += 1;
                if depth > 0 || name.ends_with(".role") {
                    usage.evidence_bytes = usage
                        .evidence_bytes
                        .checked_add(size)
                        .ok_or_else(unavailable)?;
                    usage.evidence_files += 1;
                }
                usage.admit(0, 0)?;
            }
        }
        root.verify_bound()?;
        Ok(())
    }
    let root_binding = root.identity()?;
    if root_binding.mode != 0o700 {
        return Err(unavailable());
    }
    let mut usage = DiskUsage::default();
    visit(root, root_binding.device, 0, &mut usage)?;
    Ok(usage)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoleEvidence {
    schema_version: u32,
    journal_id: String,
    transaction_id: String,
    root: Binding,
    kind: RoleKind,
    target: String,
    old_binding: Option<Binding>,
    old_len: usize,
    old_digest: Option<String>,
    stage_binding: Binding,
    new_len: usize,
    new_digest: String,
}

struct CreationRecorder<'a> {
    root: &'a RootedDir,
    epoch: &'a str,
    kind: RoleKind,
    target: &'a str,
    old: Option<&'a [u8]>,
    old_binding: Option<Binding>,
    replacement: &'a [u8],
    faults: &'a dyn FaultHooks,
}

impl PrivateRoleHooks for CreationRecorder<'_> {
    fn record_creation(&self, binding: PrivateEntryIdentity) -> io::Result<()> {
        let evidence = RoleEvidence {
            schema_version: 1,
            journal_id: self.epoch.into(),
            transaction_id: uuid::Uuid::new_v4().to_string(),
            root: self.root.identity()?.into(),
            kind: self.kind,
            target: self.target.into(),
            old_binding: self.old_binding,
            old_len: self.old.map_or(0, <[u8]>::len),
            old_digest: self.old.map(digest),
            stage_binding: binding.into(),
            new_len: self.replacement.len(),
            new_digest: digest(self.replacement),
        };
        let bytes = bounded_json(&evidence, 4096)?;
        self.root
            .write_new_private_file(&self.kind.evidence(), &bytes)?
            .sync_all()?;
        self.root.sync_root()
    }

    fn at(&self, point: PrivateRolePoint) -> io::Result<()> {
        self.faults.at(FaultPoint::Role(self.kind, point))
    }
}

fn remove_bound(root: &RootedDir, name: &str, binding: Binding) -> io::Result<()> {
    root.retry_pending_owned_regulars_matching(|target, identity| {
        target == name.as_bytes() && Binding::from(identity) == binding
    })?;
    if root.entry_exists(name)? {
        if Binding::from(root.private_entry_identity(name)?) != binding {
            return Err(unavailable());
        }
        root.remove_owned_regular(name)?;
    }
    root.sync_root()
}

fn replace_role(
    root: &RootedDir,
    epoch: &str,
    kind: RoleKind,
    target: &str,
    replacement: &[u8],
    faults: &dyn FaultHooks,
) -> io::Result<()> {
    recover_roles(root, epoch)?;
    let limit = if kind == RoleKind::Segment {
        SEGMENT_BYTES
    } else {
        METADATA_BYTES
    };
    if !canonical_uuid(epoch) || !valid_role_target(kind, target) || replacement.len() > limit {
        return Err(unavailable());
    }
    audit(root)?.admit(
        replacement.len() + 4096 + EVIDENCE_BYTES,
        2 + EVIDENCE_FILES,
    )?;
    let old = if root.entry_exists(target)? {
        Some(root.read_private_regular(target, limit as u64)?)
    } else {
        None
    };
    let old_binding = old
        .as_ref()
        .map(|_| root.private_entry_identity(target).map(Binding::from))
        .transpose()?;
    let hooks = CreationRecorder {
        root,
        epoch,
        kind,
        target,
        old: old.as_deref(),
        old_binding,
        replacement,
        faults,
    };
    root.replace_private_regular_exact_in_role(
        PrivateRegularRole {
            target,
            stage: &kind.stage(),
            expected_target: old_binding.map(Into::into),
        },
        old.as_deref(),
        replacement,
        &hooks,
    )?;
    let evidence_binding = root.private_entry_identity(&kind.evidence())?.into();
    remove_bound(root, &kind.evidence(), evidence_binding)?;
    audit(root)?.admit(0, 0)
}

fn matches_file(
    root: &RootedDir,
    name: &str,
    binding: Binding,
    length: usize,
    expected_digest: &str,
) -> io::Result<bool> {
    if !root.entry_exists(name)? {
        return Ok(false);
    }
    if Binding::from(root.private_entry_identity(name)?) != binding {
        return Ok(false);
    }
    let bytes = root.read_private_regular(name, length as u64)?;
    Ok(bytes.len() == length && digest(&bytes) == expected_digest)
}

fn recover_roles(root: &RootedDir, epoch: &str) -> io::Result<()> {
    audit(root)?;
    for kind in RoleKind::ALL {
        root.resume_pending_owned_regular_cleanup(&kind.evidence())?;
        if !root.entry_exists(&kind.evidence())? {
            if root.entry_exists(&kind.stage())? {
                return Err(unavailable());
            }
            continue;
        }
        let evidence: RoleEvidence = read_json(root, &kind.evidence(), 4096)?;
        if evidence.schema_version != 1
            || evidence.journal_id != epoch
            || !canonical_uuid(&evidence.transaction_id)
            || evidence.root != Binding::from(root.identity()?)
            || evidence.kind != kind
            || !valid_role_target(kind, &evidence.target)
            || evidence.old_binding.is_some() != evidence.old_digest.is_some()
            || evidence.new_len
                > if kind == RoleKind::Segment {
                    SEGMENT_BYTES
                } else {
                    METADATA_BYTES
                }
        {
            return Err(unavailable());
        }
        let new_published = matches_file(
            root,
            &evidence.target,
            evidence.stage_binding,
            evidence.new_len,
            &evidence.new_digest,
        )?;
        if new_published {
            // Either side of the first directory fsync may be durable. Validate
            // both bindings before making the new generation a recovery fact.
            if let (Some(old), Some(old_digest)) =
                (evidence.old_binding, evidence.old_digest.as_deref())
            {
                root.retry_pending_owned_regulars_matching(|name, id| {
                    name == kind.stage().as_bytes() && Binding::from(id) == old
                })?;
                if root.entry_exists(&kind.stage())? {
                    if !matches_file(root, &kind.stage(), old, evidence.old_len, old_digest)? {
                        return Err(unavailable());
                    }
                    root.sync_root()?;
                    remove_bound(root, &kind.stage(), old)?;
                }
            } else if root.entry_exists(&kind.stage())? {
                return Err(unavailable());
            }
            root.sync_root()?;
        } else {
            let old_matches = match (evidence.old_binding, evidence.old_digest.as_deref()) {
                (Some(old), Some(expected)) => {
                    matches_file(root, &evidence.target, old, evidence.old_len, expected)?
                }
                (None, None) => !root.entry_exists(&evidence.target)?,
                _ => false,
            };
            if !old_matches {
                return Err(unavailable());
            }
            root.retry_pending_owned_regulars_matching(|name, id| {
                name == kind.stage().as_bytes() && Binding::from(id) == evidence.stage_binding
            })?;
            if root.entry_exists(&kind.stage())? {
                if Binding::from(root.private_entry_identity(&kind.stage())?)
                    != evidence.stage_binding
                {
                    return Err(unavailable());
                }
                let bytes = root.read_private_regular(&kind.stage(), evidence.new_len as u64)?;
                if bytes.len() == evidence.new_len && digest(&bytes) != evidence.new_digest {
                    return Err(unavailable());
                }
                remove_bound(root, &kind.stage(), evidence.stage_binding)?;
            }
        }
        let binding = root.private_entry_identity(&kind.evidence())?.into();
        remove_bound(root, &kind.evidence(), binding)?;
    }
    audit(root)?.admit(0, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    const EPOCH: &str = "614dc3be-668f-4922-bd31-b1d7a0056790";

    struct FailOnce(std::sync::Mutex<Option<FaultPoint>>);
    impl FaultHooks for FailOnce {
        fn at(&self, point: FaultPoint) -> io::Result<()> {
            let mut selected = self.0.lock().unwrap();
            if *selected == Some(point) {
                *selected = None;
                Err(io::Error::from_raw_os_error(libc::EIO))
            } else {
                Ok(())
            }
        }
    }

    fn role_fault_matrix(iterations: usize) {
        let points = [
            PrivateRolePoint::CreationRecorded,
            PrivateRolePoint::PartialStage,
            PrivateRolePoint::StageSynced,
            PrivateRolePoint::Published,
            PrivateRolePoint::DirectorySynced,
            PrivateRolePoint::DisplacedRemoved,
            PrivateRolePoint::FinalSynced,
        ];
        for _ in 0..iterations {
            for role in RoleKind::ALL {
                for point in points {
                    let temp = tempfile::tempdir().unwrap();
                    let root = RootedDir::create(&temp.path().join("events")).unwrap();
                    let target = if role == RoleKind::Segment {
                        "segment-1.jsonl".into()
                    } else {
                        format!("{}.json", role.name())
                    };
                    root.write_new_private_file(&target, b"old generation")
                        .unwrap()
                        .sync_all()
                        .unwrap();
                    let fault =
                        FailOnce(std::sync::Mutex::new(Some(FaultPoint::Role(role, point))));
                    assert!(
                        replace_role(&root, EPOCH, role, &target, b"new generation", &fault)
                            .is_err(),
                        "{role:?} {point:?}"
                    );
                    recover_roles(&root, EPOCH).unwrap();
                    let expected: &[u8] = if matches!(
                        point,
                        PrivateRolePoint::CreationRecorded
                            | PrivateRolePoint::PartialStage
                            | PrivateRolePoint::StageSynced
                    ) {
                        b"old generation"
                    } else {
                        b"new generation"
                    };
                    assert_eq!(
                        root.read_private_regular(&target, 64).unwrap(),
                        expected,
                        "{role:?} {point:?}"
                    );
                    assert!(!root.entry_exists(&role.stage()).unwrap());
                    assert!(!root.entry_exists(&role.evidence()).unwrap());
                    replace_role(&root, EPOCH, role, &target, b"next generation", &NoFaults)
                        .unwrap();
                    assert_eq!(
                        root.read_private_regular(&target, 64).unwrap(),
                        b"next generation"
                    );
                }
            }
        }
    }

    #[test]
    fn every_named_role_recovers_each_exchange_and_sync_boundary() {
        role_fault_matrix(1);
    }

    #[test]
    #[ignore = "stress: repeat every journal role fault at least 200 times"]
    fn journal_role_fault_matrix_stress() {
        role_fault_matrix(200);
    }

    #[test]
    fn stage_without_creation_evidence_is_preserved_and_unavailable() {
        let temp = tempfile::tempdir().unwrap();
        let root = RootedDir::create(&temp.path().join("events")).unwrap();
        root.write_new_private_file("manifest.stage", b"unexplained")
            .unwrap();
        assert!(recover_roles(&root, EPOCH).is_err());
        assert_eq!(
            root.read_private_regular("manifest.stage", 64).unwrap(),
            b"unexplained"
        );
    }

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
