use std::{fs::File, path::Path};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    controller::{
        leader::{
            lock_exclusive, now_millis, open_controller_root, open_existing_controller_root,
            store_io,
        },
        protocol::{ControllerRequest, canonical_request_sha256, validate_request_id},
    },
    error::WorkerError,
    protocol::PROTOCOL_VERSION,
};

const OPERATIONS_LOCK: &str = "operations.lock";
const ADOPTION_MARKER: &str = "operations-adopted-v1.json";
const MAX_ENVELOPE_BYTES: u64 = crate::controller::protocol::MAX_STORED_REQUEST_BYTES as u64;
pub const ENVELOPE_WINDOW_MILLIS: u64 = 7 * 24 * 60 * 60 * 1000;
const PRUNE_SCAN_LIMIT: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperationOutcome {
    Acknowledged,
    Rejected { code: String },
}

/// Laptop transport-cache retry handle. This is not a second task/queue store.
/// Retrying an existing envelope must not re-freeze a changed body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationEnvelope {
    request_id: String,
    payload_sha256: String,
    command: String,
    body: Value,
    created_at_millis: u64,
    // Explicit null fields distinguish new pending envelopes from legacy files.
    #[serde(default)]
    settled_at_millis: Option<u64>,
    #[serde(default)]
    outcome: Option<OperationOutcome>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AdoptionMarker {
    created_at_millis: u64,
}

pub(crate) fn adopt_operation_envelopes(cache_root: &Path) -> Result<(), WorkerError> {
    let root = open_controller_root(cache_root)?;
    let _lock = lock_operations(&root)?;
    adoption_time(&root, now_millis()?)?;
    Ok(())
}

// Caller holds operations.lock; publish once and never advance the cutoff.
fn adoption_time(root: &crate::rooted_fs::RootedDir, now: u64) -> Result<u64, WorkerError> {
    if !root.entry_exists(ADOPTION_MARKER).map_err(store_io)? {
        let bytes = serde_json::to_vec(&AdoptionMarker {
            created_at_millis: now,
        })
        .map_err(std::io::Error::other)?;
        match root.write_private_atomic_no_replace(ADOPTION_MARKER, &bytes) {
            Ok(()) => return Ok(now),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(store_io(error)),
        }
    }
    let bytes = root
        .read_private_regular(ADOPTION_MARKER, 256)
        .map_err(store_io)?;
    let marker: AdoptionMarker = serde_json::from_slice(&bytes).map_err(|_| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: envelope adoption marker is invalid".into())
    })?;
    Ok(marker.created_at_millis)
}

impl OperationEnvelope {
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn payload_sha256(&self) -> &str {
        &self.payload_sha256
    }

    pub fn command(&self) -> &str {
        &self.command
    }

    pub fn body(&self) -> &Value {
        &self.body
    }

    pub fn created_at_millis(&self) -> u64 {
        self.created_at_millis
    }

    pub fn settled_at_millis(&self) -> Option<u64> {
        self.settled_at_millis
    }

    pub fn outcome(&self) -> Option<&OperationOutcome> {
        self.outcome.as_ref()
    }

    pub fn to_request(&self) -> Result<ControllerRequest, WorkerError> {
        validate_envelope(&envelope_file_name(&self.request_id)?, self)?;
        let bytes = serde_json::to_vec(&serde_json::json!({
            "protocol_version": PROTOCOL_VERSION,
            "request_id": self.request_id,
            "command": self.command,
            "body": self.body,
        }))
        .map_err(std::io::Error::other)?;
        crate::controller::protocol::parse_request(&bytes)
    }
}

/// Create a new envelope, or return the original when the same ID is retried
/// with the same command and body. A changed payload conflicts.
pub fn persist_operation_envelope(
    cache_root: &Path,
    request: &ControllerRequest,
) -> Result<OperationEnvelope, WorkerError> {
    let name = envelope_file_name(request.request_id())?;
    let root = open_controller_root(cache_root)?;
    let _lock = lock_operations(&root)?;
    let now = now_millis()?;
    // Adoption only filters legacy listings; it must never block a mutation.
    let _ = adoption_time(&root, now);
    if root.entry_exists(&name).map_err(store_io)? {
        let existing = load_envelope(&root, &name)?;
        return reuse_or_conflict(&existing, request);
    }
    let envelope = OperationEnvelope {
        request_id: request.request_id().to_owned(),
        payload_sha256: request.payload_sha256().to_owned(),
        command: request.command().to_owned(),
        body: request.body().clone(),
        created_at_millis: now,
        settled_at_millis: None,
        outcome: None,
    };
    // Best effort, under the same lock as settlement. Never let stale or
    // unsafe cache entries prevent persisting the next recovery handle.
    let _ = prune_settled(&root, envelope.created_at_millis);
    let bytes = serde_json::to_vec(&envelope).map_err(|_| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: operation envelope is invalid".into())
    })?;
    match root.write_private_atomic_no_replace(&name, &bytes) {
        Ok(()) => Ok(envelope),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            reuse_or_conflict(&load_envelope(&root, &name)?, request)
        }
        Err(error) => Err(store_io(error)),
    }
}

/// Publish settlement without changing the frozen payload or creation time.
pub fn settle_operation_envelope(
    cache_root: &Path,
    request: &ControllerRequest,
    outcome: OperationOutcome,
) -> Result<(), WorkerError> {
    let name = envelope_file_name(request.request_id())?;
    let root = open_controller_root(cache_root)?;
    let _lock = lock_operations(&root)?;
    let bytes = root
        .read_private_regular(&name, MAX_ENVELOPE_BYTES)
        .map_err(store_io)?;
    let mut envelope = decode_envelope(&name, &bytes)?;
    reuse_or_conflict(&envelope, request)?;
    // Preserve the first definitive observation when concurrent retries settle.
    if envelope.settled_at_millis.is_some() {
        return Ok(());
    }
    envelope.settled_at_millis = Some(now_millis()?);
    envelope.outcome = Some(outcome);
    let next = serde_json::to_vec(&envelope).map_err(std::io::Error::other)?;
    root.replace_private_regular_exact(&name, &bytes, &next)
        .map_err(store_io)
}

#[derive(Debug, Default)]
pub struct PendingEnvelopes {
    pub pending: Vec<OperationEnvelope>,
    pub unreadable: Vec<UnreadableEnvelope>,
    pub adoption_marker_unreadable: bool,
}

#[derive(Debug, Serialize)]
pub struct UnreadableEnvelope {
    pub request_id: Option<String>,
    pub code: String,
}

pub fn list_pending_envelopes(
    cache_root: &Path,
    all: bool,
) -> Result<PendingEnvelopes, WorkerError> {
    list_pending_envelopes_at(cache_root, all, now_millis()?)
}

fn list_pending_envelopes_at(
    cache_root: &Path,
    all: bool,
    now: u64,
) -> Result<PendingEnvelopes, WorkerError> {
    list_pending_envelopes_with_hook(cache_root, all, now, || {})
}

fn list_pending_envelopes_with_hook(
    cache_root: &Path,
    all: bool,
    now: u64,
    mut after_open: impl FnMut(),
) -> Result<PendingEnvelopes, WorkerError> {
    let Some(root) = open_existing_controller_root(cache_root)? else {
        return Ok(PendingEnvelopes::default());
    };
    let mut report = PendingEnvelopes::default();
    let adopted_at = {
        let _lock = lock_operations(&root)?;
        adoption_time(&root, now).unwrap_or_else(|_| {
            report.adoption_marker_unreadable = true;
            0
        })
    };
    for name in root.list_names().map_err(store_io)? {
        if !name.starts_with(b"op-") || !name.ends_with(b".json") {
            continue;
        }
        let name = String::from_utf8(name).ok();
        let Some(name) = name.filter(|name| is_envelope_name(name)) else {
            report.unreadable.push(UnreadableEnvelope {
                request_id: None,
                code: "CONTROLLER_TRANSPORT".into(),
            });
            continue;
        };
        // Settlement and pruning use this same lock. Take it per addressed
        // entry so a long listing does not block unrelated cache mutations.
        let _lock = lock_operations(&root)?;
        let (envelope, legacy) = match load_pending_envelope(&root, &name, &mut after_open) {
            Ok(envelope) => envelope,
            // A concurrent prune may remove a settled entry after enumeration.
            Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                report.unreadable.push(UnreadableEnvelope {
                    // The validated filename is trustworthy even if its body is not.
                    request_id: Some(name[3..name.len() - 5].to_owned()),
                    code: error.public_code(),
                });
                continue;
            }
        };
        if envelope.settled_at_millis.is_none()
            && (all
                || (now.saturating_sub(envelope.created_at_millis) <= ENVELOPE_WINDOW_MILLIS
                    && (!legacy || envelope.created_at_millis >= adopted_at)))
        {
            report.pending.push(envelope);
        }
    }
    report.pending.sort_by(|a, b| {
        b.created_at_millis
            .cmp(&a.created_at_millis)
            .then_with(|| a.request_id.cmp(&b.request_id))
    });
    report
        .unreadable
        .sort_by(|a, b| a.request_id.cmp(&b.request_id));
    Ok(report)
}

fn is_envelope_name(name: &str) -> bool {
    name.strip_prefix("op-")
        .and_then(|id| id.strip_suffix(".json"))
        .is_some_and(|id| validate_request_id(id).is_ok())
}

/// Bound envelope reads while giving every entry a chance to be pruned,
/// even when a large pending prefix is retained indefinitely.
fn prune_settled(root: &crate::rooted_fs::RootedDir, now: u64) -> Result<(), WorkerError> {
    prune_settled_from(root, now, uuid::Uuid::new_v4().as_u128() as usize)
}

fn prune_settled_from(
    root: &crate::rooted_fs::RootedDir,
    now: u64,
    start: usize,
) -> Result<(), WorkerError> {
    let mut names = root
        .list_names()
        .map_err(store_io)?
        .into_iter()
        .filter_map(|name| String::from_utf8(name).ok())
        .filter(|name| is_envelope_name(name))
        .collect::<Vec<_>>();
    names.sort_unstable();
    if names.is_empty() {
        return Ok(());
    }
    let start = start % names.len();
    for name in names[start..]
        .iter()
        .chain(&names[..start])
        .take(PRUNE_SCAN_LIMIT)
    {
        if let Ok(bytes) = root.read_private_regular(name, MAX_ENVELOPE_BYTES)
            && let Ok(envelope) = decode_envelope_metadata(name, &bytes)
            && envelope
                .settled_at_millis
                .is_some_and(|settled| now.saturating_sub(settled) > ENVELOPE_WINDOW_MILLIS)
        {
            let _ = root.remove_owned_regular(name);
        }
    }
    Ok(())
}

/// Reads the cached envelope under the operations lock. Does not create a
/// missing cache directory; the lock file may be created.
pub fn load_operation_envelope(
    cache_root: &Path,
    request_id: &str,
) -> Result<Option<OperationEnvelope>, WorkerError> {
    load_operation_envelope_with_hook(cache_root, request_id, || {})
}

fn load_operation_envelope_with_hook(
    cache_root: &Path,
    request_id: &str,
    after_open: impl FnOnce(),
) -> Result<Option<OperationEnvelope>, WorkerError> {
    let name = envelope_file_name(request_id)?;
    let Some(root) = open_existing_controller_root(cache_root)? else {
        return Ok(None);
    };
    let _lock = lock_operations(&root)?;
    if !root.entry_exists(&name).map_err(store_io)? {
        return Ok(None);
    }
    Ok(Some(load_envelope_with_hook(&root, &name, after_open)?))
}

fn lock_operations(root: &crate::rooted_fs::RootedDir) -> Result<File, WorkerError> {
    let file = root.open_private_lock(OPERATIONS_LOCK).map_err(store_io)?;
    lock_exclusive(&file)?;
    Ok(file)
}

fn load_envelope(
    root: &crate::rooted_fs::RootedDir,
    name: &str,
) -> Result<OperationEnvelope, WorkerError> {
    load_envelope_with_hook(root, name, || {})
}

fn load_envelope_with_hook(
    root: &crate::rooted_fs::RootedDir,
    name: &str,
    after_open: impl FnOnce(),
) -> Result<OperationEnvelope, WorkerError> {
    let bytes = root
        .read_private_regular_with_hook(name, MAX_ENVELOPE_BYTES, after_open)
        .map_err(store_io)?;
    decode_envelope(name, &bytes)
}

fn load_pending_envelope(
    root: &crate::rooted_fs::RootedDir,
    name: &str,
    after_open: impl FnOnce(),
) -> Result<(OperationEnvelope, bool), WorkerError> {
    let bytes = root
        .read_private_regular_with_hook(name, MAX_ENVELOPE_BYTES, after_open)
        .map_err(store_io)?;
    // Validate the original bytes: Value would erase duplicate identity or
    // settlement fields before the typed decoder can reject them.
    let envelope = decode_envelope(name, &bytes)?;
    let invalid =
        |_| WorkerError::Protocol("CONTROLLER_TRANSPORT: operation envelope is invalid".into());
    let value: Value = serde_json::from_slice(&bytes).map_err(invalid)?;
    let legacy = value.get("settled_at_millis").is_none() && value.get("outcome").is_none();
    Ok((envelope, legacy))
}

fn decode_envelope(name: &str, bytes: &[u8]) -> Result<OperationEnvelope, WorkerError> {
    let envelope = decode_envelope_metadata(name, bytes)?;
    validate_envelope(name, &envelope)?;
    Ok(envelope)
}

// Retention depends on settlement metadata, not the protocol used to hash the
// frozen payload. Retry and pending listing still require the current digest.
fn decode_envelope_metadata(name: &str, bytes: &[u8]) -> Result<OperationEnvelope, WorkerError> {
    let envelope: OperationEnvelope = serde_json::from_slice(bytes).map_err(|_| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: operation envelope is invalid".into())
    })?;
    validate_envelope_metadata(name, &envelope)?;
    Ok(envelope)
}

fn validate_envelope(name: &str, envelope: &OperationEnvelope) -> Result<(), WorkerError> {
    validate_envelope_metadata(name, envelope)?;
    let digest = canonical_request_sha256(PROTOCOL_VERSION, &envelope.command, &envelope.body)?;
    if digest != envelope.payload_sha256 {
        return Err(WorkerError::task(
            "CONTROLLER_ENVELOPE_INCOMPATIBLE",
            "saved request does not match this protocol version or its frozen payload; retry refused",
        ));
    }
    Ok(())
}

fn validate_envelope_metadata(name: &str, envelope: &OperationEnvelope) -> Result<(), WorkerError> {
    if envelope_file_name(&envelope.request_id)? != name {
        return Err(WorkerError::Protocol(
            "CONTROLLER_TRANSPORT: operation envelope id does not match its filename".into(),
        ));
    }
    if envelope.settled_at_millis.is_some() != envelope.outcome.is_some() {
        return Err(WorkerError::Protocol(
            "CONTROLLER_TRANSPORT: operation envelope settlement is invalid".into(),
        ));
    }
    Ok(())
}

fn reuse_or_conflict(
    existing: &OperationEnvelope,
    request: &ControllerRequest,
) -> Result<OperationEnvelope, WorkerError> {
    if existing.payload_sha256 == request.payload_sha256() && existing.command == request.command()
    {
        return Ok(existing.clone());
    }
    Err(WorkerError::Protocol(
        "CONTROLLER_REQUEST_CONFLICT: request_id is bound to a different payload".into(),
    ))
}

fn envelope_file_name(request_id: &str) -> Result<String, WorkerError> {
    validate_request_id(request_id)?;
    Ok(format!("op-{request_id}.json"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::protocol::parse_request;
    use serde_json::json;

    fn request(id: u128) -> ControllerRequest {
        parse_request(
            &serde_json::to_vec(&json!({
                "protocol_version": PROTOCOL_VERSION,
                "request_id": format!("{id:032x}"),
                "command": "task.cancel",
                "body": {"task_id": "018f0f4a6b5c7d8e9f00112233445566"}
            }))
            .unwrap(),
        )
        .unwrap()
    }

    fn rewrite(cache: &Path, id: u128, update: impl FnOnce(&mut Value)) {
        let path = cache.join(format!("op-{id:032x}.json"));
        let mut value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        update(&mut value);
        std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
    }

    fn seed_envelope(root: &crate::rooted_fs::RootedDir, id: u128, settled: Option<u64>) {
        let request = request(id);
        let envelope = OperationEnvelope {
            request_id: request.request_id().to_owned(),
            payload_sha256: request.payload_sha256().to_owned(),
            command: request.command().to_owned(),
            body: request.body().clone(),
            created_at_millis: 1,
            settled_at_millis: settled,
            outcome: settled.map(|_| OperationOutcome::Acknowledged),
        };
        root.write_private_atomic_no_replace(
            &envelope_file_name(request.request_id()).unwrap(),
            &serde_json::to_vec(&envelope).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn envelope_reads_exclude_settlement_while_the_record_is_open() {
        use std::os::fd::AsRawFd;

        for operation in ["lookup", "pending"] {
            let temp = tempfile::tempdir().unwrap();
            let cache = temp.path().join("cache");
            let request = request(1);
            let original = persist_operation_envelope(&cache, &request).unwrap();
            let root = open_existing_controller_root(&cache).unwrap().unwrap();
            let name = envelope_file_name(request.request_id()).unwrap();
            let previous = serde_json::to_vec(&original).unwrap();
            let mut settled = original.clone();
            settled.settled_at_millis = Some(original.created_at_millis);
            settled.outcome = Some(OperationOutcome::Acknowledged);
            let next = serde_json::to_vec(&settled).unwrap();
            let mut peer_acquired = false;
            let mut publish = || {
                let lock = root.open_existing_private_lock(OPERATIONS_LOCK).unwrap();
                if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                    peer_acquired = true;
                    root.replace_private_regular_exact(&name, &previous, &next)
                        .unwrap();
                } else {
                    assert_eq!(
                        std::io::Error::last_os_error().kind(),
                        std::io::ErrorKind::WouldBlock
                    );
                }
            };

            match operation {
                "lookup" => assert_eq!(
                    load_operation_envelope_with_hook(&cache, request.request_id(), &mut publish)
                        .unwrap()
                        .unwrap(),
                    original
                ),
                "pending" => {
                    let report = list_pending_envelopes_with_hook(
                        &cache,
                        true,
                        original.created_at_millis,
                        &mut publish,
                    )
                    .unwrap();
                    assert!(report.unreadable.is_empty());
                    assert_eq!(report.pending, vec![original]);
                }
                _ => unreachable!(),
            }
            assert!(!peer_acquired, "{operation} read must exclude settlement");
            settle_operation_envelope(&cache, &request, OperationOutcome::Acknowledged).unwrap();
            assert!(
                load_operation_envelope(&cache, request.request_id())
                    .unwrap()
                    .unwrap()
                    .settled_at_millis()
                    .is_some()
            );
            assert!(
                list_pending_envelopes(&cache, true)
                    .unwrap()
                    .pending
                    .is_empty()
            );
        }
    }

    #[test]
    fn envelope_pending_read_keeps_pruning_outside_the_open_record() {
        use std::os::fd::AsRawFd;

        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        let root = open_controller_root(&cache).unwrap();
        seed_envelope(&root, 1, Some(1));
        let name = envelope_file_name(request(1).request_id()).unwrap();
        let mut peer_acquired = false;
        let report =
            list_pending_envelopes_with_hook(&cache, true, ENVELOPE_WINDOW_MILLIS + 2, || {
                let lock = root.open_existing_private_lock(OPERATIONS_LOCK).unwrap();
                if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                    peer_acquired = true;
                    root.remove_owned_regular(&name).unwrap();
                } else {
                    assert_eq!(
                        std::io::Error::last_os_error().kind(),
                        std::io::ErrorKind::WouldBlock
                    );
                }
            })
            .unwrap();

        assert!(
            !peer_acquired,
            "pruning must wait for the bound envelope read"
        );
        assert!(report.pending.is_empty());
        assert!(report.unreadable.is_empty());
        let _lock = lock_operations(&root).unwrap();
        prune_settled_from(&root, ENVELOPE_WINDOW_MILLIS + 2, 0).unwrap();
        assert!(!root.entry_exists(&name).unwrap());
    }

    #[test]
    fn envelope_reads_do_not_follow_a_replaced_cache_directory() {
        use std::{fs, os::unix::fs::PermissionsExt};

        for operation in ["lookup", "pending"] {
            let temp = tempfile::tempdir().unwrap();
            let cache = temp.path().join("cache");
            let request = request(1);
            let original = persist_operation_envelope(&cache, &request).unwrap();
            let name = envelope_file_name(request.request_id()).unwrap();
            let bytes = fs::read(cache.join(&name)).unwrap();
            let mut replace = || {
                fs::rename(&cache, temp.path().join("detached")).unwrap();
                fs::create_dir(&cache).unwrap();
                fs::set_permissions(&cache, fs::Permissions::from_mode(0o700)).unwrap();
                let file = cache.join(&name);
                fs::write(&file, &bytes).unwrap();
                fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
            };

            match operation {
                "lookup" => assert!(matches!(
                    load_operation_envelope_with_hook(&cache, request.request_id(), replace),
                    Err(WorkerError::Io(error)) if error.raw_os_error() == Some(libc::ESTALE)
                )),
                "pending" => {
                    let report = list_pending_envelopes_with_hook(
                        &cache,
                        true,
                        original.created_at_millis,
                        &mut replace,
                    )
                    .unwrap();
                    assert!(report.pending.is_empty());
                    assert_eq!(report.unreadable.len(), 1);
                    assert_eq!(report.unreadable[0].code, "IO");
                }
                _ => unreachable!(),
            }
            assert_eq!(fs::read(cache.join(&name)).unwrap(), bytes);
            assert_eq!(
                fs::read(temp.path().join("detached").join(&name)).unwrap(),
                bytes
            );
        }
    }

    #[test]
    fn legacy_envelopes_and_settlement_preserve_frozen_request() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        let request = request(1);
        let original = persist_operation_envelope(&cache, &request).unwrap();
        assert!(original.settled_at_millis().is_none());
        settle_operation_envelope(&cache, &request, OperationOutcome::Acknowledged).unwrap();
        let settled = load_operation_envelope(&cache, request.request_id())
            .unwrap()
            .unwrap();
        assert!(settled.settled_at_millis().is_some());
        assert_eq!(settled.outcome(), Some(&OperationOutcome::Acknowledged));
        assert_eq!(settled.to_request().unwrap(), request);
        assert_eq!(
            persist_operation_envelope(&cache, &request).unwrap(),
            settled
        );
        rewrite(&cache, 1, |value| {
            value.as_object_mut().unwrap().remove("settled_at_millis");
            value.as_object_mut().unwrap().remove("outcome");
        });
        let legacy = load_operation_envelope(&cache, request.request_id())
            .unwrap()
            .unwrap();
        assert!(legacy.settled_at_millis().is_none());
        assert_eq!(legacy.to_request().unwrap(), request);
    }

    #[test]
    fn mutation_persistence_ignores_missing_invalid_and_unsafe_adoption_markers() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        for kind in ["missing", "invalid", "permissions", "directory", "symlink"] {
            let temp = tempfile::tempdir().unwrap();
            let cache = temp.path().join("cache");
            let root = open_controller_root(&cache).unwrap();
            let marker = cache.join(ADOPTION_MARKER);
            match kind {
                "missing" => {}
                "invalid" => root
                    .write_private_atomic_no_replace(ADOPTION_MARKER, b"PRIVATE invalid JSON")
                    .unwrap(),
                "permissions" => {
                    root.write_private_atomic_no_replace(ADOPTION_MARKER, b"{}")
                        .unwrap();
                    std::fs::set_permissions(&marker, std::fs::Permissions::from_mode(0o644))
                        .unwrap();
                }
                "directory" => std::fs::create_dir(&marker).unwrap(),
                "symlink" => symlink(temp.path().join("absent"), &marker).unwrap(),
                _ => unreachable!(),
            }
            let request = request(1);
            let saved = persist_operation_envelope(&cache, &request)
                .unwrap_or_else(|error| panic!("{kind}: {error}"));
            assert_eq!(saved.to_request().unwrap(), request);
            assert_eq!(persist_operation_envelope(&cache, &request).unwrap(), saved);
            settle_operation_envelope(&cache, &request, OperationOutcome::Acknowledged).unwrap();
            assert_eq!(
                load_operation_envelope(&cache, request.request_id())
                    .unwrap()
                    .unwrap()
                    .outcome(),
                Some(&OperationOutcome::Acknowledged)
            );
        }
    }

    #[test]
    fn pending_window_is_newest_first_and_all_includes_legacy_history() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        let now = 20 * ENVELOPE_WINDOW_MILLIS;
        for (id, age) in [(1, 1), (2, ENVELOPE_WINDOW_MILLIS + 1), (3, 0), (4, 2)] {
            persist_operation_envelope(&cache, &request(id)).unwrap();
            rewrite(&cache, id, |value| {
                value["created_at_millis"] = json!(now - age)
            });
        }
        settle_operation_envelope(
            &cache,
            &request(3),
            OperationOutcome::Rejected {
                code: "TASK_CLOSED".into(),
            },
        )
        .unwrap();
        let ids = |all| {
            list_pending_envelopes_at(&cache, all, now)
                .unwrap()
                .pending
                .iter()
                .map(|e| e.request_id().to_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(false), [format!("{:032x}", 1), format!("{:032x}", 4)]);
        assert_eq!(
            ids(true),
            [
                format!("{:032x}", 1),
                format!("{:032x}", 4),
                format!("{:032x}", 2)
            ]
        );
        let missing = temp.path().join("missing");
        assert!(
            list_pending_envelopes_at(&missing, false, now)
                .unwrap()
                .pending
                .is_empty()
        );
        assert!(!missing.exists());
    }

    #[test]
    fn adoption_hides_only_legacy_envelopes_created_before_the_marker() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        let root = open_controller_root(&cache).unwrap();
        let adopted_at = 20 * ENVELOPE_WINDOW_MILLIS;
        root.write_private_atomic_no_replace(
            "operations-adopted-v1.json",
            &serde_json::to_vec(&json!({"created_at_millis": adopted_at})).unwrap(),
        )
        .unwrap();
        for (id, created, legacy) in [
            (1, adopted_at - 1, true),
            (2, adopted_at - 1, false),
            (3, adopted_at + 1, true),
            (4, adopted_at, true),
        ] {
            seed_envelope(&root, id, None);
            rewrite(&cache, id, |value| {
                value["created_at_millis"] = json!(created);
                if legacy {
                    value.as_object_mut().unwrap().remove("settled_at_millis");
                    value.as_object_mut().unwrap().remove("outcome");
                } else {
                    value["settled_at_millis"] = Value::Null;
                    value["outcome"] = Value::Null;
                }
            });
        }
        let ids = |all| {
            list_pending_envelopes_at(&cache, all, adopted_at + 2)
                .unwrap()
                .pending
                .into_iter()
                .map(|envelope| envelope.request_id)
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(false), [3, 4, 2].map(|id| format!("{id:032x}")));
        assert_eq!(ids(true), [3, 4, 1, 2].map(|id| format!("{id:032x}")));
        assert!(
            load_operation_envelope(&cache, request(1).request_id())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn new_envelope_prunes_only_expired_settlements() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        for id in 1..=3 {
            persist_operation_envelope(&cache, &request(id)).unwrap();
        }
        // Age records after persistence, so setup itself cannot prune them.
        rewrite(&cache, 1, |value| {
            value["created_at_millis"] = json!(1);
            value["settled_at_millis"] = json!(1);
            value["outcome"] = json!({"kind": "acknowledged"});
        });
        rewrite(&cache, 2, |value| value["created_at_millis"] = json!(1));
        settle_operation_envelope(&cache, &request(3), OperationOutcome::Acknowledged).unwrap();
        persist_operation_envelope(&cache, &request(4)).unwrap();
        assert!(
            load_operation_envelope(&cache, request(1).request_id())
                .unwrap()
                .is_none()
        );
        assert!(
            load_operation_envelope(&cache, request(2).request_id())
                .unwrap()
                .is_some()
        );
        assert!(
            load_operation_envelope(&cache, request(3).request_id())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn pruning_reaches_expired_settlement_behind_more_than_256_pending_envelopes() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        let root = open_controller_root(&cache).unwrap();
        for id in (1..=300).rev() {
            seed_envelope(&root, id, None);
        }
        seed_envelope(&root, 301, Some(1));
        let expired = cache.join(format!("op-{:032x}.json", 301));
        let now = ENVELOPE_WINDOW_MILLIS + 2;

        prune_settled_from(&root, now, 0).unwrap();
        assert!(expired.exists(), "the first 256 entries are pending");
        prune_settled_from(&root, now, 256).unwrap();
        assert!(!expired.exists(), "a later start must reach the settlement");
        assert!((1..=300).all(|id| cache.join(format!("op-{id:032x}.json")).is_file()));
    }

    #[test]
    fn pruning_wraps_and_limits_each_call_to_256_envelopes() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        let root = open_controller_root(&cache).unwrap();
        for id in (1..=300).rev() {
            seed_envelope(&root, id, Some(1));
        }
        // Start with the final two entries, then wrap to the first 254.
        prune_settled_from(&root, ENVELOPE_WINDOW_MILLIS + 2, 298).unwrap();
        for id in 1..=300 {
            assert_eq!(
                cache.join(format!("op-{id:032x}.json")).exists(),
                (255..=298).contains(&id),
                "unexpected pruning result for envelope {id}",
            );
        }
    }

    #[test]
    fn pruning_preserves_pending_recent_corrupt_and_unsafe_entries() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        let root = open_controller_root(&cache).unwrap();
        seed_envelope(&root, 1, Some(1));
        seed_envelope(&root, 2, None);
        seed_envelope(&root, 3, Some(2)); // Exactly seven days old: retain it.
        for id in 4..=8 {
            seed_envelope(&root, id, Some(1));
        }
        let path = |id| cache.join(format!("op-{id:032x}.json"));
        std::fs::write(path(4), b"invalid json").unwrap();
        rewrite(&cache, 5, |value| {
            value["request_id"] = json!(request(6).request_id())
        });
        rewrite(&cache, 6, |value| value["outcome"] = Value::Null);
        let outside = temp.path().join("outside.json");
        std::fs::rename(path(7), &outside).unwrap();
        let outside_bytes = std::fs::read(&outside).unwrap();
        symlink(&outside, path(7)).unwrap();
        std::fs::set_permissions(path(8), std::fs::Permissions::from_mode(0o644)).unwrap();
        std::fs::create_dir(path(9)).unwrap();
        root.write_private_atomic_no_replace("op-not-an-id.json", b"invalid filename")
            .unwrap();

        prune_settled_from(&root, ENVELOPE_WINDOW_MILLIS + 2, 0).unwrap();
        assert!(!path(1).exists());
        for id in 2..=9 {
            assert!(
                path(id).symlink_metadata().is_ok(),
                "entry {id} must be retained"
            );
        }
        assert_eq!(std::fs::read_link(path(7)).unwrap(), outside);
        assert_eq!(std::fs::read(outside).unwrap(), outside_bytes);
        assert!(cache.join("op-not-an-id.json").exists());
    }

    #[test]
    fn review_pruning_does_not_create_or_change_legacy_cursor_files() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        let root = open_controller_root(&cache).unwrap();
        prune_settled(&root, ENVELOPE_WINDOW_MILLIS + 2).unwrap();
        let cursor = cache.join("operations-prune-offset.json");
        assert!(!cursor.exists(), "pruning must not create a cursor file");
        root.write_private_atomic_no_replace(
            "operations-prune-offset.json",
            b"legacy cursor is opaque",
        )
        .unwrap();
        prune_settled(&root, ENVELOPE_WINDOW_MILLIS + 2).unwrap();
        assert_eq!(std::fs::read(cursor).unwrap(), b"legacy cursor is opaque");
    }

    #[test]
    fn pruning_expires_foreign_protocol_settlements_but_keeps_pending_recent_and_unsafe() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        let root = open_controller_root(&cache).unwrap();
        for (id, settled) in [(1, Some(1)), (2, None), (3, Some(2)), (4, Some(1))] {
            seed_envelope(&root, id, settled);
            rewrite(&cache, id, |value| {
                value["payload_sha256"] = json!(
                    canonical_request_sha256(
                        PROTOCOL_VERSION - 1,
                        request(id).command(),
                        request(id).body()
                    )
                    .unwrap()
                )
            });
        }
        let path = |id| cache.join(format!("op-{id:032x}.json"));
        std::fs::set_permissions(path(4), std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(
            load_operation_envelope(&cache, request(1).request_id())
                .unwrap_err()
                .public_code(),
            "CONTROLLER_ENVELOPE_INCOMPATIBLE"
        );
        prune_settled_from(&root, ENVELOPE_WINDOW_MILLIS + 2, 0).unwrap();
        assert!(
            !path(1).exists(),
            "expired settlement does not depend on today's protocol digest"
        );
        for id in 2..=4 {
            assert!(path(id).exists());
        }
    }

    #[test]
    fn incompatible_protocol_digest_is_refused_with_a_public_reason() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        let request = request(1);
        persist_operation_envelope(&cache, &request).unwrap();
        rewrite(&cache, 1, |value| {
            value["payload_sha256"] = json!(
                canonical_request_sha256(PROTOCOL_VERSION - 1, request.command(), request.body())
                    .unwrap()
            )
        });
        let error = load_operation_envelope(&cache, request.request_id()).unwrap_err();
        assert_eq!(error.public_code(), "CONTROLLER_ENVELOPE_INCOMPATIBLE");
        assert!(error.public_message().contains("protocol"));
    }
}
