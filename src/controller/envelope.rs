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
    };
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
    let envelope: OperationEnvelope = serde_json::from_slice(&bytes).map_err(|_| {
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
        return Err(WorkerError::Protocol(
            "CONTROLLER_TRANSPORT: operation envelope digest does not match command and body"
                .into(),
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
