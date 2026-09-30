//! Journal storage primitives. Public event contracts are adapted by the facade.
use std::io;

use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write;

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestDelta {
    appended: Segment,
    rotate: bool,
    retired: Option<Segment>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pending {
    schema_version: u32,
    journal_id: String,
    transaction_id: String,
    previous_digest: String,
    previous_head: u64,
    first: u64,
    last: u64,
    bytes_base64: String,
    batch_digest: String,
    delta: ManifestDelta,
    next_digest: String,
}

fn prepare_append(
    manifest: &Manifest,
    bytes: &[u8],
    new_segment: Option<Segment>,
) -> io::Result<Pending> {
    validate_manifest(manifest)?;
    let count = bytes.iter().filter(|byte| **byte == b'\n').count();
    if bytes.len() > BATCH_BYTES || count == 0 || count > BATCH_EVENTS {
        return Err(unavailable());
    }
    let first = manifest.head.checked_add(1).ok_or_else(unavailable)?;
    let last = manifest
        .head
        .checked_add(count as u64)
        .ok_or_else(unavailable)?;
    decode_records(bytes, &manifest.journal_id, first, Some(last))?;
    let active = manifest.segments.last().ok_or_else(unavailable)?;
    let rotate = active
        .committed_len
        .checked_add(bytes.len())
        .is_none_or(|len| len > SEGMENT_BYTES);
    let mut appended = if rotate {
        let segment = new_segment.ok_or_else(unavailable)?;
        if segment.first != first
            || segment.name != format!("segment-{first}.jsonl")
            || segment.last.is_some()
            || segment.committed_len != 0
            || segment.sealed
            || segment.binding.device != active.binding.device
        {
            return Err(unavailable());
        }
        segment
    } else {
        if new_segment.is_some() {
            return Err(unavailable());
        }
        active.clone()
    };
    appended.committed_len += bytes.len();
    appended.last = Some(last);
    let retired = if rotate && manifest.segments.len() == RETAINED_SEGMENTS {
        Some(manifest.segments[0].clone())
    } else {
        None
    };
    let mut pending = Pending {
        schema_version: 1,
        journal_id: manifest.journal_id.clone(),
        transaction_id: uuid::Uuid::new_v4().to_string(),
        previous_digest: digest(&bounded_json(manifest, METADATA_BYTES)?),
        previous_head: manifest.head,
        first,
        last,
        bytes_base64: STANDARD.encode(bytes),
        batch_digest: digest(bytes),
        delta: ManifestDelta {
            appended,
            rotate,
            retired,
        },
        next_digest: String::new(),
    };
    bounded_json(&pending.delta, 4096)?;
    let next = apply_delta(manifest, &pending)?;
    pending.next_digest = digest(&bounded_json(&next, METADATA_BYTES)?);
    bounded_json(&pending, METADATA_BYTES)?;
    Ok(pending)
}

fn apply_delta(manifest: &Manifest, pending: &Pending) -> io::Result<Manifest> {
    let mut next = manifest.clone();
    if pending.delta.rotate {
        next.segments.last_mut().ok_or_else(unavailable)?.sealed = true;
        next.segments.push(pending.delta.appended.clone());
        if next.segments.len() > RETAINED_SEGMENTS {
            let retired = next.segments.remove(0);
            if pending.delta.retired.as_ref() != Some(&retired) {
                return Err(unavailable());
            }
        } else if pending.delta.retired.is_some() {
            return Err(unavailable());
        }
    } else {
        *next.segments.last_mut().ok_or_else(unavailable)? = pending.delta.appended.clone();
        if pending.delta.retired.is_some() {
            return Err(unavailable());
        }
    }
    next.head = pending.last;
    next.oldest = next.segments[0].first;
    validate_manifest(&next)?;
    Ok(next)
}

fn pending_bytes(pending: &Pending) -> io::Result<Vec<u8>> {
    bounded_json(pending, METADATA_BYTES)?;
    if pending.schema_version != 1
        || !canonical_uuid(&pending.journal_id)
        || !canonical_uuid(&pending.transaction_id)
        || pending.bytes_base64.len() > BATCH_BYTES.div_ceil(3) * 4
    {
        return Err(unavailable());
    }
    let bytes = STANDARD
        .decode(&pending.bytes_base64)
        .map_err(|_| unavailable())?;
    if bytes.len() > BATCH_BYTES
        || digest(&bytes) != pending.batch_digest
        || STANDARD.encode(&bytes) != pending.bytes_base64
    {
        return Err(unavailable());
    }
    Ok(bytes)
}

fn verify_pending(manifest: &Manifest, pending: &Pending, bytes: &[u8]) -> io::Result<Manifest> {
    let new_segment = if pending.delta.rotate {
        let mut segment = pending.delta.appended.clone();
        segment.last = None;
        segment.committed_len = 0;
        Some(segment)
    } else {
        None
    };
    let expected = prepare_append(manifest, bytes, new_segment)?;
    if expected.journal_id != pending.journal_id
        || expected.previous_digest != pending.previous_digest
        || expected.previous_head != pending.previous_head
        || expected.first != pending.first
        || expected.last != pending.last
        || expected.delta != pending.delta
        || expected.next_digest != pending.next_digest
    {
        return Err(unavailable());
    }
    apply_delta(manifest, pending)
}

fn predecessor(next: &Manifest, pending: &Pending, bytes: &[u8]) -> io::Result<Manifest> {
    let mut old = next.clone();
    if pending.delta.rotate {
        if old.segments.pop().as_ref() != Some(&pending.delta.appended) {
            return Err(unavailable());
        }
        old.segments.last_mut().ok_or_else(unavailable)?.sealed = false;
        if let Some(retired) = &pending.delta.retired {
            old.segments.insert(0, retired.clone());
        }
    } else {
        let active = old.segments.last_mut().ok_or_else(unavailable)?;
        active.committed_len = active
            .committed_len
            .checked_sub(bytes.len())
            .ok_or_else(unavailable)?;
        active.last = if active.committed_len == 0 {
            None
        } else {
            Some(pending.previous_head)
        };
    }
    old.head = pending.previous_head;
    old.oldest = old.segments.first().ok_or_else(unavailable)?.first;
    validate_manifest(&old)?;
    verify_pending(&old, pending, bytes)?;
    Ok(old)
}

fn verify_retained(
    root: &RootedDir,
    manifest: &Manifest,
    pending: Option<&Pending>,
) -> io::Result<()> {
    validate_manifest(manifest)?;
    let device = root.identity()?.device;
    for segment in &manifest.segments {
        if segment.binding.device != device {
            return Err(unavailable());
        }
        let allow_tail = pending.is_some_and(|p| p.delta.appended.name == segment.name);
        committed_records(root, segment, &manifest.journal_id, allow_tail)?;
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Retirement {
    schema_version: u32,
    journal_id: String,
    manifest_digest: String,
    segment: Segment,
}

fn recover_retirement(
    root: &RootedDir,
    manifest: &Manifest,
    faults: &dyn FaultHooks,
) -> io::Result<()> {
    root.resume_pending_owned_regular_cleanup("retirement.json")?;
    if !root.entry_exists("retirement.json")? {
        return Ok(());
    }
    let retirement: Retirement = read_json(root, "retirement.json", METADATA_BYTES)?;
    if retirement.schema_version != 1
        || retirement.journal_id != manifest.journal_id
        || retirement.manifest_digest != digest(&bounded_json(manifest, METADATA_BYTES)?)
        || manifest
            .segments
            .iter()
            .any(|s| s.name == retirement.segment.name || s.binding == retirement.segment.binding)
        || !retirement.segment.sealed
        || !valid_segment_name(&retirement.segment.name)
    {
        return Err(unavailable());
    }
    root.retry_pending_owned_regulars_matching(|name, id| {
        name == retirement.segment.name.as_bytes()
            && Binding::from(id) == retirement.segment.binding
    })?;
    if root.entry_exists(&retirement.segment.name)? {
        committed_records(root, &retirement.segment, &manifest.journal_id, false)?;
        remove_bound(root, &retirement.segment.name, retirement.segment.binding)?;
    }
    faults.at(FaultPoint::Retired)?;
    let binding = root.private_entry_identity("retirement.json")?.into();
    remove_bound(root, "retirement.json", binding)
}

fn finish_pending(
    root: &RootedDir,
    manifest: &Manifest,
    pending: &Pending,
    faults: &dyn FaultHooks,
) -> io::Result<()> {
    if let Some(segment) = &pending.delta.retired {
        let retirement = Retirement {
            schema_version: 1,
            journal_id: manifest.journal_id.clone(),
            manifest_digest: pending.next_digest.clone(),
            segment: segment.clone(),
        };
        let bytes = bounded_json(&retirement, METADATA_BYTES)?;
        if root.entry_exists("retirement.json")? {
            if root.read_private_regular("retirement.json", METADATA_BYTES as u64)? != bytes {
                return Err(unavailable());
            }
        } else {
            replace_role(
                root,
                &manifest.journal_id,
                RoleKind::Retirement,
                "retirement.json",
                &bytes,
                faults,
            )?;
        }
    }
    let binding = root.private_entry_identity("pending.json")?.into();
    remove_bound(root, "pending.json", binding)?;
    faults.at(FaultPoint::PendingRemoved)?;
    recover_retirement(root, manifest, faults)
}

fn publish_append(
    root: &RootedDir,
    pending: &Pending,
    faults: &dyn FaultHooks,
) -> io::Result<Manifest> {
    let manifest = recover_append(root, faults)?;
    let bytes = pending_bytes(pending)?;
    verify_pending(&manifest, pending, &bytes)?;
    let target = &pending.delta.appended;
    if Binding::from(root.private_entry_identity(&target.name)?) != target.binding {
        return Err(unavailable());
    }
    let encoded = bounded_json(pending, METADATA_BYTES)?;
    replace_role(
        root,
        &manifest.journal_id,
        RoleKind::Pending,
        "pending.json",
        &encoded,
        faults,
    )?;
    faults.at(FaultPoint::PendingDurable)?;
    recover_append(root, faults)
}

fn recover_append(root: &RootedDir, faults: &dyn FaultHooks) -> io::Result<Manifest> {
    let initial: Manifest = read_json(root, "manifest.json", METADATA_BYTES)?;
    recover_roles(root, &initial.journal_id)?;
    root.resume_pending_owned_regular_cleanup("pending.json")?;
    let manifest: Manifest = read_json(root, "manifest.json", METADATA_BYTES)?;
    if !root.entry_exists("pending.json")? {
        verify_retained(root, &manifest, None)?;
        recover_retirement(root, &manifest, faults)?;
        return Ok(manifest);
    }
    let pending: Pending = read_json(root, "pending.json", METADATA_BYTES)?;
    let bytes = pending_bytes(&pending)?;
    let current_digest = digest(&bounded_json(&manifest, METADATA_BYTES)?);
    if current_digest == pending.next_digest {
        predecessor(&manifest, &pending, &bytes)?;
        verify_retained(root, &manifest, None)?;
        finish_pending(root, &manifest, &pending, faults)?;
        return Ok(manifest);
    }
    if current_digest != pending.previous_digest {
        return Err(unavailable());
    }
    let next = verify_pending(&manifest, &pending, &bytes)?;
    verify_retained(root, &manifest, Some(&pending))?;
    let target = &pending.delta.appended;
    if Binding::from(root.private_entry_identity(&target.name)?) != target.binding {
        return Err(unavailable());
    }
    let existing = root.read_private_regular(&target.name, SEGMENT_BYTES as u64)?;
    let offset = target
        .committed_len
        .checked_sub(bytes.len())
        .ok_or_else(unavailable)?;
    let tail = existing.get(offset..).ok_or_else(unavailable)?;
    let suffix = completion_suffix(&bytes, tail)?;
    let mut file = root.open_private_append(&target.name)?;
    root.validate_private_regular_binding(&target.name, &file, target.binding.into())?;
    if !suffix.is_empty() {
        let split = suffix.len().div_ceil(2);
        file.write_all(&suffix[..split])?;
        faults.at(FaultPoint::PartialAppend)?;
        file.write_all(&suffix[split..])?;
    }
    file.sync_all()?;
    root.validate_private_regular_binding(&target.name, &file, target.binding.into())?;
    faults.at(FaultPoint::SegmentSynced)?;
    // The committed prefix in this manifest is the only visibility boundary.
    replace_role(
        root,
        &manifest.journal_id,
        RoleKind::Manifest,
        "manifest.json",
        &bounded_json(&next, METADATA_BYTES)?,
        faults,
    )?;
    faults.at(FaultPoint::ManifestCommitted)?;
    verify_retained(root, &next, None)?;
    finish_pending(root, &next, &pending, faults)?;
    audit(root)?.admit(0, 0)?;
    Ok(next)
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

    fn append_fixture() -> (TempDir, RootedDir, Pending) {
        let (temp, root, manifest) = fixture(&record(1), false);
        root.write_new_private_file("manifest.json", &serde_json::to_vec(&manifest).unwrap())
            .unwrap()
            .sync_all()
            .unwrap();
        let bytes = [record(2), record(3)].concat();
        let pending = prepare_append(&manifest, &bytes, None).unwrap();
        (temp, root, pending)
    }

    fn padded_record(seq: u64) -> Vec<u8> {
        let mut value: serde_json::Value = serde_json::from_slice(&record(seq)).unwrap();
        value["data"]["padding"] = "".into();
        let length = serde_json::to_vec(&value).unwrap().len() + 1;
        value["data"]["padding"] = "x".repeat(1024 - length).into();
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        bytes
    }

    #[test]
    fn pending_encodes_maximum_batch_with_delta_instead_of_manifest_copy() {
        let (_temp, _root, manifest) = fixture(&record(1), false);
        let bytes = (2..=33).map(padded_record).collect::<Vec<_>>().concat();
        assert_eq!(bytes.len(), 32 * 1024);
        let pending = prepare_append(&manifest, &bytes, None).unwrap();
        let encoded = serde_json::to_vec(&pending).unwrap();
        assert!(encoded.len() <= 64 * 1024);
        let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert!(value.get("segments").is_none());
        assert!(serde_json::to_vec(&value["delta"]).unwrap().len() <= 4096);
    }

    #[test]
    fn sixty_four_segment_rotation_retires_only_manifest_excluded_binding() {
        let temp = tempfile::tempdir().unwrap();
        let root = RootedDir::create(&temp.path().join("events")).unwrap();
        let mut segments = Vec::new();
        for index in 0..64u64 {
            let first = index * 256 + 1;
            let name = format!("segment-{first}.jsonl");
            let bytes = (first..first + 256)
                .map(padded_record)
                .collect::<Vec<_>>()
                .concat();
            root.write_new_private_file(&name, &bytes)
                .unwrap()
                .sync_all()
                .unwrap();
            segments.push(Segment {
                name: name.clone(),
                first,
                last: Some(first + 255),
                committed_len: 256 * 1024,
                binding: root.private_entry_identity(&name).unwrap().into(),
                sealed: index < 63,
            });
        }
        let manifest = Manifest {
            schema_version: 1,
            journal_id: EPOCH.into(),
            head: 16_384,
            oldest: 1,
            segments,
        };
        root.write_new_private_file("manifest.json", &serde_json::to_vec(&manifest).unwrap())
            .unwrap()
            .sync_all()
            .unwrap();
        root.write_new_private_file("segment-16385.jsonl", b"")
            .unwrap()
            .sync_all()
            .unwrap();
        let new_segment = Segment {
            name: "segment-16385.jsonl".into(),
            first: 16_385,
            last: None,
            committed_len: 0,
            binding: root
                .private_entry_identity("segment-16385.jsonl")
                .unwrap()
                .into(),
            sealed: false,
        };
        let pending = prepare_append(&manifest, &record(16_385), Some(new_segment)).unwrap();
        let fault = FailOnce(std::sync::Mutex::new(Some(FaultPoint::Retired)));
        assert!(publish_append(&root, &pending, &fault).is_err());
        let next = recover_append(&root, &NoFaults).unwrap();
        assert_eq!(next.head, 16_385);
        assert_eq!(next.oldest, 257);
        assert_eq!(next.segments.len(), 64);
        assert!(!root.entry_exists("segment-1.jsonl").unwrap());
        assert!(root.entry_exists("segment-257.jsonl").unwrap());
        let usage = audit(&root).unwrap();
        assert!(usage.bytes <= 18 * 1024 * 1024 && usage.files <= 128);
    }

    #[test]
    fn pending_recovery_commits_each_append_boundary_once() {
        for point in [
            FaultPoint::PendingDurable,
            FaultPoint::PartialAppend,
            FaultPoint::SegmentSynced,
            FaultPoint::ManifestCommitted,
            FaultPoint::PendingRemoved,
        ] {
            let (_temp, root, pending) = append_fixture();
            let fault = FailOnce(std::sync::Mutex::new(Some(point)));
            assert!(
                publish_append(&root, &pending, &fault).is_err(),
                "{point:?}"
            );
            let reopened = RootedDir::open(root.path()).unwrap();
            let manifest = recover_append(&reopened, &NoFaults).unwrap();
            assert_eq!(manifest.head, 3, "{point:?}");
            let records =
                committed_records(&reopened, &manifest.segments[0], EPOCH, false).unwrap();
            let seqs: Vec<&str> = records.iter().map(|r| r["seq"].as_str().unwrap()).collect();
            assert_eq!(seqs, ["1", "2", "3"]);
            // The next caller starts at four; recovery never allocates another range.
            let next = prepare_append(&manifest, &record(4), None).unwrap();
            assert_eq!(publish_append(&reopened, &next, &NoFaults).unwrap().head, 4);
            assert_eq!(recover_append(&reopened, &NoFaults).unwrap().head, 4);
            let usage = audit(&reopened).unwrap();
            assert!(usage.bytes <= 18 * 1024 * 1024 && usage.files <= 128);
            assert!(usage.evidence_bytes <= 512 * 1024 && usage.evidence_files <= 32);
        }
    }

    #[test]
    fn unexplained_append_tail_is_preserved_without_committing() {
        use std::io::Write;
        let (_temp, root, pending) = append_fixture();
        let fault = FailOnce(std::sync::Mutex::new(Some(FaultPoint::PendingDurable)));
        assert!(publish_append(&root, &pending, &fault).is_err());
        let mut file = root.open_private_append("segment-1.jsonl").unwrap();
        file.write_all(b"foreign bytes").unwrap();
        file.sync_all().unwrap();
        assert!(recover_append(&root, &NoFaults).is_err());
        let saved: Manifest = read_json(&root, "manifest.json", METADATA_BYTES).unwrap();
        assert_eq!(saved.head, 1);
        assert!(
            root.read_private_regular("segment-1.jsonl", SEGMENT_BYTES as u64)
                .unwrap()
                .ends_with(b"foreign bytes")
        );
    }

    #[test]
    fn repeated_partial_append_crashes_keep_one_sequence_range_and_bounded_residue() {
        let (_temp, root, pending) = append_fixture();
        let initial = FailOnce(std::sync::Mutex::new(Some(FaultPoint::PendingDurable)));
        assert!(publish_append(&root, &pending, &initial).is_err());
        for _ in 0..20 {
            let fault = FailOnce(std::sync::Mutex::new(Some(FaultPoint::PartialAppend)));
            if recover_append(&root, &fault).is_ok() {
                break;
            }
            let usage = audit(&root).unwrap();
            assert!(usage.files <= 4 && usage.bytes < 64 * 1024);
        }
        let manifest = recover_append(&root, &NoFaults).unwrap();
        assert_eq!(manifest.head, 3);
        assert_eq!(
            committed_records(&root, &manifest.segments[0], EPOCH, false)
                .unwrap()
                .len(),
            3
        );
    }

    #[test]
    fn prepare_rejects_batch_count_size_and_sequence_overflow_before_writing() {
        let (_temp, _root, mut manifest) = fixture(&record(1), false);
        let too_many = (2..=34).map(record).collect::<Vec<_>>().concat();
        assert!(prepare_append(&manifest, &too_many, None).is_err());
        assert!(prepare_append(&manifest, &vec![b'x'; 32 * 1024 + 1], None).is_err());
        assert!(prepare_append(&manifest, &record(3), None).is_err());
        manifest.head = u64::MAX;
        manifest.oldest = u64::MAX;
        manifest.segments[0].name = format!("segment-{}.jsonl", u64::MAX);
        manifest.segments[0].first = u64::MAX;
        manifest.segments[0].last = Some(u64::MAX);
        assert!(prepare_append(&manifest, &record(0), None).is_err());
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
