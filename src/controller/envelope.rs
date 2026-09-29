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
const MAX_ENVELOPE_BYTES: u64 = crate::controller::protocol::MAX_STORED_REQUEST_BYTES as u64;
pub const ENVELOPE_WINDOW_MILLIS: u64 = 7 * 24 * 60 * 60 * 1000;
const PRUNE_SCAN_LIMIT: usize = 64;
const PRUNE_PAGE_BYTES: usize = 4096;
const PRUNE_CURSOR: &str = "operations-prune-offset.json";

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    settled_at_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outcome: Option<OperationOutcome>,
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
    if root.entry_exists(&name).map_err(store_io)? {
        let existing = load_envelope(&root, &name)?;
        return reuse_or_conflict(&existing, request);
    }
    let envelope = OperationEnvelope {
        request_id: request.request_id().to_owned(),
        payload_sha256: request.payload_sha256().to_owned(),
        command: request.command().to_owned(),
        body: request.body().clone(),
        created_at_millis: now_millis()?,
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

pub fn list_pending_envelopes(
    cache_root: &Path,
    all: bool,
) -> Result<Vec<OperationEnvelope>, WorkerError> {
    list_pending_envelopes_at(cache_root, all, now_millis()?)
}

fn list_pending_envelopes_at(
    cache_root: &Path,
    all: bool,
    now: u64,
) -> Result<Vec<OperationEnvelope>, WorkerError> {
    let Some(root) = open_existing_controller_root(cache_root)? else {
        return Ok(Vec::new());
    };
    let mut pending = Vec::new();
    for name in root.list_names().map_err(store_io)? {
        let Ok(name) = String::from_utf8(name) else {
            continue;
        };
        if !is_envelope_name(&name) {
            continue;
        }
        let envelope = match load_envelope(&root, &name) {
            Ok(envelope) => envelope,
            // A concurrent prune may remove a settled entry after enumeration.
            Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if envelope.settled_at_millis.is_none()
            && (all || now.saturating_sub(envelope.created_at_millis) <= ENVELOPE_WINDOW_MILLIS)
        {
            pending.push(envelope);
        }
    }
    pending.sort_by(|a, b| {
        b.created_at_millis
            .cmp(&a.created_at_millis)
            .then_with(|| a.request_id.cmp(&b.request_id))
    });
    Ok(pending)
}

fn is_envelope_name(name: &str) -> bool {
    name.strip_prefix("op-")
        .and_then(|id| id.strip_suffix(".json"))
        .is_some_and(|id| validate_request_id(id).is_ok())
}

/// Read one fixed-size directory page per call. Unlike Darwin's per-DIR
/// telldir cookies, kernel lseek offsets survive reopening the directory.
/// Directory changes may repeat/skip entries; EOF starts another sweep. The
/// 40-byte op filenames occupy at least 64 bytes per dirent, bounding reads
/// to 64 envelopes per 4 KiB page, even with a large retained pending prefix.
fn prune_settled(root: &crate::rooted_fs::RootedDir, now: u64) -> Result<(), WorkerError> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    root.verify_bound().map_err(store_io)?;
    let previous = root.read_private_regular(PRUNE_CURSOR, 64).ok();
    let offset: libc::off_t = previous
        .as_deref()
        .and_then(|bytes| serde_json::from_slice(bytes).ok())
        .unwrap_or(0);
    // SAFETY: root owns the directory fd. Opening "." yields an independent
    // directory offset and never resolves a caller-controlled path.
    let fd = unsafe {
        libc::openat(
            root.raw_directory_fd(),
            c".".as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(store_io(std::io::Error::last_os_error()));
    }
    // SAFETY: openat returned a newly owned descriptor.
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    // The cursor is advisory. An invalidated offset restarts at zero.
    // SAFETY: fd is live; lseek does not retain it or access memory.
    if offset > 0 && unsafe { libc::lseek(fd.as_raw_fd(), offset, libc::SEEK_SET) } < 0 {
        unsafe {
            libc::lseek(fd.as_raw_fd(), 0, libc::SEEK_SET);
        }
    }
    let mut page = [0_u8; PRUNE_PAGE_BYTES];
    let count = read_directory_page(fd.as_raw_fd(), &mut page).map_err(store_io)?;
    let next = if count == 0 {
        0
    } else {
        // SAFETY: fd is the live directory description used for the page.
        let position = unsafe { libc::lseek(fd.as_raw_fd(), 0, libc::SEEK_CUR) };
        if position < 0 {
            return Err(store_io(std::io::Error::last_os_error()));
        }
        position
    };
    let mut names = Vec::new();
    let mut remaining = &page[..count];
    let name_offset = std::mem::offset_of!(libc::dirent, d_name);
    let length_offset = std::mem::offset_of!(libc::dirent, d_reclen);
    while !remaining.is_empty() {
        let invalid = || {
            store_io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid directory page",
            ))
        };
        let length_bytes: [u8; 2] = remaining
            .get(length_offset..length_offset + 2)
            .ok_or_else(invalid)?
            .try_into()
            .map_err(|_| invalid())?;
        let length = u16::from_ne_bytes(length_bytes) as usize;
        if length <= name_offset || length > remaining.len() {
            return Err(invalid());
        }
        let name = &remaining[name_offset..length];
        let end = name
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(invalid)?;
        if let Ok(name) = std::str::from_utf8(&name[..end])
            && is_envelope_name(name)
        {
            names.push(name.to_owned());
        }
        remaining = &remaining[length..];
    }
    if names.len() > PRUNE_SCAN_LIMIT {
        return Err(store_io(std::io::Error::other(
            "directory page exceeds envelope scan bound",
        )));
    }
    root.verify_bound().map_err(store_io)?;
    for name in names {
        if let Ok(envelope) = load_envelope(root, &name)
            && envelope
                .settled_at_millis
                .is_some_and(|settled| now.saturating_sub(settled) > ENVELOPE_WINDOW_MILLIS)
        {
            let _ = root.remove_owned_regular(&name);
        }
    }
    let bytes = serde_json::to_vec(&next).map_err(std::io::Error::other)?;
    match previous {
        Some(previous) => root.replace_private_regular_exact(PRUNE_CURSOR, &previous, &bytes),
        None => root.write_private_atomic_no_replace(PRUNE_CURSOR, &bytes),
    }
    .map_err(store_io)
}

/// Read-only lookup. Does not create a missing cache directory.
pub fn load_operation_envelope(
    cache_root: &Path,
    request_id: &str,
) -> Result<Option<OperationEnvelope>, WorkerError> {
    let name = envelope_file_name(request_id)?;
    let Some(root) = open_existing_controller_root(cache_root)? else {
        return Ok(None);
    };
    if !root.entry_exists(&name).map_err(store_io)? {
        return Ok(None);
    }
    Ok(Some(load_envelope(&root, &name)?))
}

/// Use the native 64-bit dirent ABI; std/libc readdir buffers a whole page
/// and exposes stream-local cookies on macOS, which cannot be persisted.
fn read_directory_page(fd: libc::c_int, page: &mut [u8]) -> std::io::Result<usize> {
    #[cfg(target_os = "macos")]
    let count = {
        unsafe extern "C" {
            #[link_name = "__getdirentries64"]
            fn getdirentries64(
                fd: libc::c_int,
                buf: *mut libc::c_char,
                size: libc::size_t,
                base: *mut libc::off_t,
            ) -> libc::ssize_t;
        }
        let mut base = 0;
        // SAFETY: fd is a live directory; page and base are writable for the
        // full call. The returned page is parsed with checked slice bounds.
        unsafe { getdirentries64(fd, page.as_mut_ptr().cast(), page.len(), &mut base) }
    };
    #[cfg(target_os = "linux")]
    // SAFETY: fd is a live directory and page is writable for its stated size.
    let count = unsafe { libc::syscall(libc::SYS_getdents64, fd, page.as_mut_ptr(), page.len()) };
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    let count = {
        let _ = (fd, page);
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "bounded directory scan is unavailable",
        ));
    };
    if count < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(count as usize)
    }
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
    let bytes = root
        .read_private_regular(name, MAX_ENVELOPE_BYTES)
        .map_err(store_io)?;
    decode_envelope(name, &bytes)
}

fn decode_envelope(name: &str, bytes: &[u8]) -> Result<OperationEnvelope, WorkerError> {
    let envelope: OperationEnvelope = serde_json::from_slice(bytes).map_err(|_| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: operation envelope is invalid".into())
    })?;
    validate_envelope(name, &envelope)?;
    Ok(envelope)
}

fn validate_envelope(name: &str, envelope: &OperationEnvelope) -> Result<(), WorkerError> {
    if envelope_file_name(&envelope.request_id)? != name {
        return Err(WorkerError::Protocol(
            "CONTROLLER_TRANSPORT: operation envelope id does not match its filename".into(),
        ));
    }
    let digest = canonical_request_sha256(PROTOCOL_VERSION, &envelope.command, &envelope.body)?;
    if digest != envelope.payload_sha256 {
        return Err(WorkerError::task(
            "CONTROLLER_ENVELOPE_INCOMPATIBLE",
            "saved request does not match this protocol version or its frozen payload; retry refused",
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
                .is_empty()
        );
        assert!(!missing.exists());
    }

    #[test]
    fn new_envelope_prunes_only_expired_settlements_with_bounded_progress() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        for id in 1..=150 {
            persist_operation_envelope(&cache, &request(id)).unwrap();
        }
        // Age records after persistence, so setup itself cannot prune them.
        for id in 1..=148 {
            rewrite(&cache, id, |value| {
                value["created_at_millis"] = json!(1);
                value["settled_at_millis"] = json!(1);
                value["outcome"] = json!({"kind": "acknowledged"});
            });
        }
        rewrite(&cache, 149, |value| value["created_at_millis"] = json!(1));
        settle_operation_envelope(&cache, &request(150), OperationOutcome::Acknowledged).unwrap();
        let old_count = || {
            (1..=148)
                .filter(|id| cache.join(format!("op-{id:032x}.json")).exists())
                .count()
        };
        persist_operation_envelope(&cache, &request(151)).unwrap();
        assert!(old_count() >= 148 - PRUNE_SCAN_LIMIT);
        for id in 152..=180 {
            persist_operation_envelope(&cache, &request(id)).unwrap();
        }
        assert_eq!(old_count(), 0);
        assert!(
            load_operation_envelope(&cache, request(149).request_id())
                .unwrap()
                .is_some()
        );
        assert!(
            load_operation_envelope(&cache, request(150).request_id())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn pruning_reaches_expired_envelopes_behind_a_retained_directory_prefix() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        for id in 1..=(PRUNE_SCAN_LIMIT as u128 + 20) {
            persist_operation_envelope(&cache, &request(id)).unwrap();
        }
        let root = open_controller_root(&cache).unwrap();
        let names = root
            .list_names()
            .unwrap()
            .into_iter()
            .map(|name| String::from_utf8(name).unwrap())
            .filter(|name| is_envelope_name(name))
            .collect::<Vec<_>>();
        let expired = &names[PRUNE_SCAN_LIMIT..];
        assert!(!expired.is_empty());
        for name in expired {
            let id = u128::from_str_radix(
                name.strip_prefix("op-")
                    .unwrap()
                    .strip_suffix(".json")
                    .unwrap(),
                16,
            )
            .unwrap();
            rewrite(&cache, id, |value| {
                value["settled_at_millis"] = json!(1);
                value["outcome"] = json!({"kind": "acknowledged"});
            });
        }
        for id in 1000..1010 {
            persist_operation_envelope(&cache, &request(id)).unwrap();
        }
        assert!(
            expired.iter().all(|name| !cache.join(name).exists()),
            "an undeletable prefix must not starve expired envelopes"
        );
        assert!(
            names[..PRUNE_SCAN_LIMIT]
                .iter()
                .all(|name| cache.join(name).exists())
        );
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
