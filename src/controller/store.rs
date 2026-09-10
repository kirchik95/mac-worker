use std::{fs::File, io, path::Path};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    controller::{
        leader::{lock_exclusive, now_millis, open_controller_root, store_io},
        protocol::{
            ControllerRequest, MAX_STORED_REQUEST_BYTES, canonical_request_sha256,
            validate_request_id,
        },
    },
    error::WorkerError,
    protocol::PROTOCOL_VERSION,
    rooted_fs::RootedDir,
    task::{TaskId, TurnId},
};

const REQUESTS_LOCK: &str = "requests.lock";

/// Test-only publication boundary. Production RPC never stops after publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerFault {
    None,
    /// Durable published row exists; fake ACK has not started.
    StopAfterPublish,
    /// Replacement staging is complete; the live name still holds the full
    /// published record. Process loss must resume from that record.
    CrashBeforeAckExchange,
    /// Live name already holds the ACK record; directory sync has not finished.
    CrashAfterAckExchangeBeforeSync,
}

/// Checkpoint-1 fake executor. It does not call `TaskClient`, `create_task`,
/// or enqueue work. Resume reuses this durable row's IDs, frozen envelope,
/// and timestamp instead of allocating a second task.
pub struct FakeControllerExecutor;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestPhase {
    Published,
    Acked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableRequest {
    protocol_version: u32,
    request_id: String,
    command: String,
    body: Value,
    payload_sha256: String,
    created_at_millis: u64,
    task_id: String,
    turn_id: String,
    phase: RequestPhase,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerAck {
    protocol_version: u32,
    status: String,
    request_id: String,
    payload_sha256: String,
    task_id: String,
    turn_id: String,
    created_at_millis: u64,
}

pub struct ControllerStore {
    root: RootedDir,
}

impl DurableRequest {
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

    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    pub fn turn_id(&self) -> &str {
        &self.turn_id
    }

    pub fn created_at_millis(&self) -> u64 {
        self.created_at_millis
    }

    pub fn phase(&self) -> RequestPhase {
        self.phase
    }
}

impl ControllerAck {
    pub fn status(&self) -> &str {
        &self.status
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn payload_sha256(&self) -> &str {
        &self.payload_sha256
    }

    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    pub fn turn_id(&self) -> &str {
        &self.turn_id
    }

    pub fn created_at_millis(&self) -> u64 {
        self.created_at_millis
    }
}

impl ControllerStore {
    pub fn open(state_root: &Path) -> Result<Self, WorkerError> {
        let root = open_controller_root(state_root)?;
        let _requests = root.open_private_lock(REQUESTS_LOCK).map_err(store_io)?;
        Ok(Self { root })
    }

    pub fn request_count(&self) -> Result<usize, WorkerError> {
        Ok(self.request_names()?.len())
    }

    pub fn load(&self, request_id: &str) -> Result<Option<DurableRequest>, WorkerError> {
        let name = request_file_name(request_id)?;
        if !self.root.entry_exists(&name).map_err(store_io)? {
            return Ok(None);
        }
        Ok(Some(self.read_record(&name)?))
    }

    /// Persist or resume under `requests.lock`. RPC must not take the leader lock.
    pub fn handle(
        &self,
        request: &ControllerRequest,
        fault: ControllerFault,
    ) -> Result<ControllerAck, WorkerError> {
        let _guard = self.lock_requests()?;
        match self.load(request.request_id())? {
            None => {
                let record = self.publish(request)?;
                if fault == ControllerFault::StopAfterPublish {
                    return Ok(ack_from(&record));
                }
                Ok(ack_from(&self.fake_ack(record, fault)?))
            }
            Some(existing) if existing.payload_sha256 == request.payload_sha256() => {
                if existing.command != request.command() {
                    return Err(conflict());
                }
                match existing.phase {
                    RequestPhase::Published => Ok(ack_from(&self.fake_ack(existing, fault)?)),
                    RequestPhase::Acked => Ok(ack_from(&existing)),
                }
            }
            Some(_) => Err(conflict()),
        }
    }

    /// Resume published rows without reallocating IDs. Fake executor only.
    pub fn resume_incomplete(&self) -> Result<Vec<ControllerAck>, WorkerError> {
        let _guard = self.lock_requests()?;
        let mut acks = Vec::new();
        for name in self.request_names()? {
            let record = self.read_record(&name)?;
            if record.phase == RequestPhase::Published {
                acks.push(ack_from(&self.fake_ack(record, ControllerFault::None)?));
            }
        }
        Ok(acks)
    }

    fn lock_requests(&self) -> Result<File, WorkerError> {
        let file = self
            .root
            .open_private_lock(REQUESTS_LOCK)
            .map_err(store_io)?;
        lock_exclusive(&file)?;
        Ok(file)
    }

    fn publish(&self, request: &ControllerRequest) -> Result<DurableRequest, WorkerError> {
        let record = DurableRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: request.request_id().to_owned(),
            command: request.command().to_owned(),
            body: request.body().clone(),
            payload_sha256: request.payload_sha256().to_owned(),
            created_at_millis: now_millis()?,
            task_id: TaskId::generate().to_string(),
            turn_id: TurnId::generate().to_string(),
            phase: RequestPhase::Published,
        };
        let name = request_file_name(record.request_id())?;
        let bytes = encode_record(&record)?;
        ensure_stored_size(&bytes)?;
        self.root
            .write_private_atomic_no_replace(&name, &bytes)
            .map_err(store_io)?;
        Ok(record)
    }

    fn fake_ack(
        &self,
        record: DurableRequest,
        fault: ControllerFault,
    ) -> Result<DurableRequest, WorkerError> {
        let _ = FakeControllerExecutor;
        if record.phase == RequestPhase::Acked {
            return Ok(record);
        }
        let previous = encode_record(&record)?;
        let acked = DurableRequest {
            phase: RequestPhase::Acked,
            ..record
        };
        let name = request_file_name(acked.request_id())?;
        let next = encode_record(&acked)?;
        ensure_stored_size(&next)?;
        self.root
            .replace_private_regular_exact_with_sync_hooks(
                &name,
                &previous,
                &next,
                || fault_before_exchange(fault),
                || fault_after_exchange(fault),
                || Ok(()),
            )
            .map_err(store_io)?;
        Ok(acked)
    }

    fn read_record(&self, name: &str) -> Result<DurableRequest, WorkerError> {
        let bytes = self
            .root
            .read_private_regular(name, MAX_STORED_REQUEST_BYTES as u64)
            .map_err(store_io)?;
        ensure_stored_size(&bytes)?;
        let record: DurableRequest = serde_json::from_slice(&bytes).map_err(|_| {
            WorkerError::Protocol("CONTROLLER_TRANSPORT: durable request is invalid".into())
        })?;
        validate_record(name, &record)?;
        Ok(record)
    }

    fn request_names(&self) -> Result<Vec<String>, WorkerError> {
        let mut names = Vec::new();
        for raw in self.root.list_names().map_err(store_io)? {
            let Ok(name) = String::from_utf8(raw) else {
                continue;
            };
            if name.starts_with("req-") && name.ends_with(".json") {
                names.push(name);
            }
        }
        names.sort();
        Ok(names)
    }
}

pub(crate) fn ensure_stored_size(bytes: &[u8]) -> Result<(), WorkerError> {
    if bytes.len() > MAX_STORED_REQUEST_BYTES {
        return Err(WorkerError::Protocol(
            "CONTROLLER_TRANSPORT: durable controller request exceeds the stored size limit".into(),
        ));
    }
    Ok(())
}

fn request_file_name(request_id: &str) -> Result<String, WorkerError> {
    validate_request_id(request_id)?;
    Ok(format!("req-{request_id}.json"))
}

fn encode_record(record: &DurableRequest) -> Result<Vec<u8>, WorkerError> {
    serde_json::to_vec(record).map_err(|_| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: durable request could not be encoded".into())
    })
}

fn validate_record(name: &str, record: &DurableRequest) -> Result<(), WorkerError> {
    if record.protocol_version != PROTOCOL_VERSION {
        return Err(WorkerError::Protocol(
            "INCOMPATIBLE_PROTOCOL: durable controller request requires protocol 7".into(),
        ));
    }
    if request_file_name(&record.request_id)? != name {
        return Err(WorkerError::Protocol(
            "CONTROLLER_TRANSPORT: durable request_id does not match its filename".into(),
        ));
    }
    record
        .task_id
        .parse::<TaskId>()
        .map_err(|_| invalid_stored("durable task_id is invalid"))?;
    record
        .turn_id
        .parse::<TurnId>()
        .map_err(|_| invalid_stored("durable turn_id is invalid"))?;
    if !record.body.is_object() {
        return Err(invalid_stored("durable request body must be a JSON object"));
    }
    let digest = canonical_request_sha256(record.protocol_version, &record.command, &record.body)?;
    if digest != record.payload_sha256 {
        return Err(invalid_stored(
            "durable request digest does not match command and body",
        ));
    }
    Ok(())
}

fn ack_from(record: &DurableRequest) -> ControllerAck {
    ControllerAck {
        protocol_version: record.protocol_version,
        status: match record.phase {
            RequestPhase::Published => "published".into(),
            RequestPhase::Acked => "acked".into(),
        },
        request_id: record.request_id.clone(),
        payload_sha256: record.payload_sha256.clone(),
        task_id: record.task_id.clone(),
        turn_id: record.turn_id.clone(),
        created_at_millis: record.created_at_millis,
    }
}

fn conflict() -> WorkerError {
    WorkerError::Protocol(
        "CONTROLLER_REQUEST_CONFLICT: request_id is bound to a different payload".into(),
    )
}

fn invalid_stored(message: &str) -> WorkerError {
    WorkerError::Protocol(format!("CONTROLLER_TRANSPORT: {message}"))
}

fn fault_before_exchange(fault: ControllerFault) -> io::Result<()> {
    if fault == ControllerFault::CrashBeforeAckExchange {
        Err(injected_fault())
    } else {
        Ok(())
    }
}

fn fault_after_exchange(fault: ControllerFault) -> io::Result<()> {
    if fault == ControllerFault::CrashAfterAckExchangeBeforeSync {
        Err(injected_fault())
    } else {
        Ok(())
    }
}

fn injected_fault() -> io::Error {
    io::Error::other("injected controller replacement fault")
}

pub fn serve_rpc(
    state_root: &Path,
    stdin: &mut dyn io::Read,
    stdout: &mut dyn io::Write,
    fault: ControllerFault,
) -> Result<ControllerAck, WorkerError> {
    let payload = crate::controller::protocol::read_frame(stdin)?;
    let request = crate::controller::protocol::parse_request(&payload)?;
    let store = ControllerStore::open(state_root)?;
    let ack = store.handle(&request, fault)?;
    let frame = crate::controller::protocol::encode_json_frame(&ack)?;
    stdout.write_all(&frame).map_err(WorkerError::Io)?;
    stdout.flush().map_err(WorkerError::Io)?;
    Ok(ack)
}

#[cfg(test)]
mod tests {
    use super::ensure_stored_size;
    use crate::controller::protocol::MAX_STORED_REQUEST_BYTES;

    #[test]
    fn oversized_encoded_records_are_rejected_before_publication() {
        let too_big = vec![0u8; MAX_STORED_REQUEST_BYTES + 1];
        assert!(ensure_stored_size(&too_big).is_err());
        assert!(ensure_stored_size(&vec![0u8; MAX_STORED_REQUEST_BYTES]).is_ok());
    }
}
