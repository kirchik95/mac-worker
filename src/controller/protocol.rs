use std::io::{self, Read, Write};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::{error::WorkerError, protocol::PROTOCOL_VERSION, task::deserialize_unique_json};

/// Maximum JSON payload size for one controller RPC frame (the 4-byte length
/// prefix is not counted).
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
/// Durable rows wrap the body with IDs, timestamp, digest, and phase. This is
/// larger than the wire frame so a max-size request can be stored and reread.
pub const MAX_STORED_REQUEST_BYTES: usize = MAX_FRAME_BYTES + 8192;

const REQUEST_ID_BYTES: usize = 32;

/// Validated controller RPC request. The payload digest is always computed
/// server-side from protocol version, command, and the canonicalized `body`.
/// A client-supplied `payload_sha256` is ignored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerRequest {
    request_id: String,
    command: String,
    body: Value,
    payload_sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRequest {
    protocol_version: u32,
    request_id: String,
    #[serde(default, rename = "payload_sha256")]
    _ignored_digest: Option<String>,
    command: String,
    body: Value,
}

impl ControllerRequest {
    pub fn protocol_version(&self) -> u32 {
        PROTOCOL_VERSION
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn command(&self) -> &str {
        &self.command
    }

    pub fn body(&self) -> &Value {
        &self.body
    }

    pub fn payload_sha256(&self) -> &str {
        &self.payload_sha256
    }
}

/// Length-prefix `payload` as a 4-byte big-endian frame.
pub fn encode_frame(payload: &[u8]) -> Result<Vec<u8>, WorkerError> {
    let length = u32::try_from(payload.len())
        .map_err(|_| transport("controller RPC frame exceeds 4 GiB"))?;
    if payload.len() > MAX_FRAME_BYTES {
        return Err(transport("controller RPC frame exceeds 1 MiB"));
    }
    if payload.is_empty() {
        return Err(transport("controller RPC frame is empty"));
    }
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

pub fn write_frame(writer: &mut dyn Write, payload: &[u8]) -> Result<(), WorkerError> {
    let frame = encode_frame(payload)?;
    writer.write_all(&frame).map_err(WorkerError::Io)?;
    writer.flush().map_err(WorkerError::Io)
}

pub fn encode_json_frame<T: Serialize>(value: &T) -> Result<Vec<u8>, WorkerError> {
    let payload = serde_json::to_vec(value)
        .map_err(|_| invalid("controller RPC response could not be encoded"))?;
    encode_frame(&payload)
}

/// Split one length-prefixed frame from `bytes`. Requires the slice to contain
/// exactly one frame (strict EOF): no trailing bytes.
///
/// The SSH client for `host controller-rpc` must write one frame and close
/// stdin before waiting for the response. The server does not read a second
/// request from the same stream.
pub fn decode_frame(bytes: &[u8]) -> Result<&[u8], WorkerError> {
    let payload = decode_frame_prefix(bytes)?;
    let framed = 4 + payload.len();
    if bytes.len() > framed {
        return Err(transport("controller RPC frame contained trailing data"));
    }
    Ok(payload)
}

/// Read exactly one frame from `reader` and require EOF afterwards.
///
/// This is the stdio contract for `host controller-rpc`: one request frame,
/// then stdin EOF, then one response frame. A client that leaves stdin open
/// while waiting will stall the server on this trailing-byte check.
pub fn read_frame(reader: &mut dyn Read) -> Result<Vec<u8>, WorkerError> {
    let mut length_bytes = [0u8; 4];
    read_exact(reader, &mut length_bytes)?;
    let length = parse_frame_length(u32::from_be_bytes(length_bytes))?;
    let mut payload = vec![0u8; length];
    read_exact(reader, &mut payload)?;
    let mut trailing = [0u8; 1];
    match reader.read(&mut trailing) {
        Ok(0) => Ok(payload),
        Ok(_) => Err(transport("controller RPC frame contained trailing data")),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(payload),
        Err(error) => Err(WorkerError::Io(error)),
    }
}

/// Parse a JSON controller request payload. Framing must already have been
/// accepted. This function does not write state.
pub fn parse_request(payload: &[u8]) -> Result<ControllerRequest, WorkerError> {
    let value = decode_json_value(payload)?;
    let Some(version) = value.get("protocol_version").and_then(Value::as_u64) else {
        return Err(incompatible());
    };
    if version != u64::from(PROTOCOL_VERSION) {
        return Err(incompatible());
    }

    let WireRequest {
        protocol_version,
        request_id,
        _ignored_digest: _,
        command,
        body,
    } = serde_json::from_value(value).map_err(|_| invalid("controller RPC request is invalid"))?;
    if protocol_version != PROTOCOL_VERSION {
        return Err(incompatible());
    }
    validate_request_id(&request_id)?;
    validate_command(&command)?;
    if !body.is_object() {
        return Err(invalid("controller RPC request body must be a JSON object"));
    }
    let payload_sha256 = canonical_request_sha256(protocol_version, &command, &body)?;
    Ok(ControllerRequest {
        request_id,
        command,
        body,
        payload_sha256,
    })
}

/// Decode one framed request from an exact byte slice. Does not write state.
pub fn decode_request(bytes: &[u8]) -> Result<ControllerRequest, WorkerError> {
    parse_request(decode_frame(bytes)?)
}

/// SHA-256 of the durable request identity: wire protocol version, command,
/// and canonical JSON body. Same `request_id` with a different command or
/// body must produce a different digest so the request store can conflict.
pub fn canonical_request_sha256(
    protocol_version: u32,
    command: &str,
    body: &Value,
) -> Result<String, WorkerError> {
    let identity = Value::Object({
        let mut map = Map::new();
        map.insert("body".into(), body.clone());
        map.insert("command".into(), Value::String(command.to_owned()));
        map.insert("protocol_version".into(), Value::from(protocol_version));
        map
    });
    let bytes = serde_json::to_vec(&canonical_value(&identity))
        .map_err(|_| invalid("controller RPC request identity could not be canonicalized"))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn decode_frame_prefix(bytes: &[u8]) -> Result<&[u8], WorkerError> {
    if bytes.len() < 4 {
        return Err(transport("controller RPC frame was truncated"));
    }
    let mut prefix = [0u8; 4];
    prefix.copy_from_slice(&bytes[..4]);
    let length = parse_frame_length(u32::from_be_bytes(prefix))?;
    let end = 4 + length;
    if bytes.len() < end {
        return Err(transport("controller RPC frame was truncated"));
    }
    Ok(&bytes[4..end])
}

fn parse_frame_length(length: u32) -> Result<usize, WorkerError> {
    let length =
        usize::try_from(length).map_err(|_| transport("controller RPC frame exceeds 1 MiB"))?;
    if length == 0 {
        return Err(transport("controller RPC frame is empty"));
    }
    if length > MAX_FRAME_BYTES {
        return Err(transport("controller RPC frame exceeds 1 MiB"));
    }
    Ok(length)
}

fn decode_json_value(payload: &[u8]) -> Result<Value, WorkerError> {
    // `serde_json::Value` keeps the last duplicate key. Reject duplicates
    // before WireRequest / deny_unknown_fields / canonical identity.
    let mut deserializer = serde_json::Deserializer::from_slice(payload);
    let value = deserialize_unique_json(&mut deserializer).map_err(json_decode_error)?;
    deserializer
        .end()
        .map_err(|_| transport("controller RPC frame contained trailing data"))?;
    Ok(value)
}

fn json_decode_error(error: serde_json::Error) -> WorkerError {
    if error.to_string().contains("duplicate field") {
        invalid("controller RPC request contained duplicate keys")
    } else {
        transport("controller RPC frame was not JSON")
    }
}

fn canonical_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut ordered = Map::new();
            for key in keys {
                ordered.insert(key.clone(), canonical_value(&map[key]));
            }
            Value::Object(ordered)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonical_value).collect()),
        other => other.clone(),
    }
}

pub(crate) fn validate_request_id(value: &str) -> Result<(), WorkerError> {
    if value.len() != REQUEST_ID_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || uuid::Uuid::try_parse(value).is_err()
    {
        return Err(invalid(
            "controller RPC request_id must be a lowercase simple UUID",
        ));
    }
    Ok(())
}

fn validate_command(value: &str) -> Result<(), WorkerError> {
    if value.is_empty()
        || value.len() > 128
        || value.as_bytes().contains(&0)
        || value.chars().any(char::is_control)
    {
        return Err(invalid("controller RPC command is invalid"));
    }
    Ok(())
}

fn read_exact(reader: &mut dyn Read, buffer: &mut [u8]) -> Result<(), WorkerError> {
    match reader.read_exact(buffer) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
            Err(transport("controller RPC frame was truncated"))
        }
        Err(error) => Err(WorkerError::Io(error)),
    }
}

fn transport(message: &str) -> WorkerError {
    WorkerError::Protocol(format!("CONTROLLER_TRANSPORT: {message}"))
}

fn incompatible() -> WorkerError {
    WorkerError::Protocol("INCOMPATIBLE_PROTOCOL: controller RPC requires protocol 7".into())
}

fn invalid(message: &str) -> WorkerError {
    WorkerError::Protocol(format!("INVALID_REQUEST: {message}"))
}
