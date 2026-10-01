//! Bounded framing for the sequential controller read channel.
//!
//! These codecs consume one socket frame without requiring EOF. The existing
//! stdio codec and its strict EOF contract remain unchanged.

use std::os::unix::process::ExitStatusExt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub mod io;
pub use io::FramedSocketConnector;

use super::contracts::{
    CHANNEL_VERSION, ChannelFailure, ChannelReason, IDENTITY_BYTES, RouteDigest, ServiceIdentity,
    SocketIdentity, verify_expected_service,
};
pub use super::contracts::{
    ChannelCodec, DecodeProgress, FrameDecoder, SocketConnector, SocketSession,
};
use crate::{
    controller::{
        protocol::{self, ControllerRequest, MAX_FRAME_BYTES},
        read::ControllerReadReply,
    },
    job::HostControlError,
    process::ProcessResult,
    protocol::PROTOCOL_VERSION,
    task::deserialize_unique_json,
};

/// At most one validated frame; coalesced trailing bytes remain with the caller.
#[derive(Default)]
pub struct BoundedFrameDecoder {
    prefix: [u8; 4],
    prefix_bytes: usize,
    length: Option<usize>,
    payload: Vec<u8>,
    invalid: bool,
}

impl FrameDecoder for BoundedFrameDecoder {
    fn feed(&mut self, input: &[u8]) -> Result<DecodeProgress, ChannelFailure> {
        if self.invalid {
            return Err(invalid_frame());
        }
        let mut consumed = 0;
        if self.prefix_bytes < 4 {
            let count = (4 - self.prefix_bytes).min(input.len());
            self.prefix[self.prefix_bytes..self.prefix_bytes + count]
                .copy_from_slice(&input[..count]);
            self.prefix_bytes += count;
            consumed += count;
            if self.prefix_bytes < 4 {
                return Ok(DecodeProgress {
                    consumed,
                    payload: None,
                });
            }
            let length = u32::from_be_bytes(self.prefix) as usize;
            if length == 0 || length > MAX_FRAME_BYTES {
                self.invalid = true;
                return Err(invalid_frame());
            }
            self.length = Some(length);
            // No allocation occurs until the entire length prefix is validated.
            self.payload = Vec::with_capacity(length);
        }
        let length = self.length.ok_or_else(invalid_frame)?;
        let count = (length - self.payload.len()).min(input.len() - consumed);
        self.payload
            .extend_from_slice(&input[consumed..consumed + count]);
        consumed += count;
        let payload = if self.payload.len() == length {
            self.prefix_bytes = 0;
            self.length = None;
            Some(std::mem::take(&mut self.payload))
        } else {
            None
        };
        Ok(DecodeProgress { consumed, payload })
    }

    fn retained_bytes(&self) -> usize {
        self.prefix_bytes + self.payload.len()
    }
}

#[derive(Default)]
pub struct SessionCodec;

impl SessionCodec {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Hello {
    kind: String,
    channel_version: u32,
    route_sha256: RouteDigest,
    expected_service: ServiceIdentity,
}

#[derive(Serialize, Deserialize)]
struct Ready {
    kind: String,
    route_sha256: RouteDigest,
    service: ServiceIdentity,
}

#[derive(Serialize, Deserialize)]
struct Reply {
    kind: String,
    request_id: String,
    payload_sha256: String,
    exit_code: u8,
    payload: Value,
}

impl ChannelCodec for SessionCodec {
    fn decoder(&self) -> Box<dyn FrameDecoder> {
        Box::new(BoundedFrameDecoder::default())
    }

    fn encode_hello(&self, expected: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure> {
        expected.validate()?;
        encode_bounded(
            &Hello {
                kind: "hello".into(),
                channel_version: CHANNEL_VERSION,
                route_sha256: expected.route_sha256.clone(),
                expected_service: expected.service.clone(),
            },
            IDENTITY_BYTES,
        )
    }

    fn decode_hello(&self, payload: &[u8]) -> Result<SocketIdentity, ChannelFailure> {
        let value = unique_json(payload, IDENTITY_BYTES)?;
        // The hello is a strict snapshot, including its embedded service/account.
        strict_fields(
            &value["expected_service"],
            &[
                "protocol_version",
                "channel_version",
                "controller_client_id",
                "account",
                "leader",
                "service_generation",
                "socket_path",
                "features",
                "journal_id",
            ],
        )?;
        strict_fields(
            &value["expected_service"]["account"],
            &["uid", "username", "home"],
        )?;
        let hello: Hello = serde_json::from_value(value).map_err(|_| invalid_frame())?;
        if hello.kind != "hello" || hello.channel_version != CHANNEL_VERSION {
            return Err(invalid_frame());
        }
        let identity = SocketIdentity {
            route_sha256: hello.route_sha256,
            service: hello.expected_service,
        };
        identity.validate()?;
        Ok(identity)
    }

    fn encode_ready(&self, identity: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure> {
        identity.validate()?;
        encode_bounded(
            &Ready {
                kind: "ready".into(),
                route_sha256: identity.route_sha256.clone(),
                service: identity.service.clone(),
            },
            IDENTITY_BYTES,
        )
    }

    fn decode_ready(
        &self,
        payload: &[u8],
        expected: &SocketIdentity,
    ) -> Result<(), ChannelFailure> {
        let ready: Ready = serde_json::from_value(unique_json(payload, IDENTITY_BYTES)?)
            .map_err(|_| invalid_frame())?;
        if ready.kind != "ready" {
            return Err(invalid_frame());
        }
        let actual = SocketIdentity {
            route_sha256: ready.route_sha256,
            service: ready.service,
        };
        // Authentication uses the frozen helper and excludes journal hints.
        // A failed handshake precedes application transmission, so it remains
        // a setup failure eligible for the caller's normal stdio path.
        verify_expected_service(expected, &actual).map_err(|failure| match failure {
            ChannelFailure::UnverifiedReply => {
                ChannelFailure::Unavailable(ChannelReason::ServiceUnavailable)
            }
            other => other,
        })
    }

    fn encode_reply(
        &self,
        request: &ControllerRequest,
        result: &ProcessResult,
    ) -> Result<Vec<u8>, ChannelFailure> {
        let exit_code = result
            .status
            .code()
            .and_then(|code| u8::try_from(code).ok())
            .ok_or_else(invalid_frame)?;
        let payload = unique_json(
            protocol::decode_frame(&result.stdout).map_err(|_| invalid_frame())?,
            MAX_FRAME_BYTES,
        )?;
        verify_inner(&payload, request)?;
        encode_bounded(
            &Reply {
                kind: "reply".into(),
                request_id: request.request_id().into(),
                payload_sha256: request.payload_sha256().into(),
                exit_code,
                payload,
            },
            MAX_FRAME_BYTES,
        )
    }

    fn decode_reply(
        &self,
        payload: &[u8],
        request: &ControllerRequest,
    ) -> Result<ProcessResult, ChannelFailure> {
        let reply: Reply = serde_json::from_value(unique_json(payload, MAX_FRAME_BYTES)?)
            .map_err(|_| invalid_frame())?;
        if reply.kind != "reply" {
            return Err(invalid_frame());
        }
        if reply.request_id != request.request_id()
            || reply.payload_sha256 != request.payload_sha256()
        {
            return Err(ChannelFailure::UnverifiedReply);
        }
        verify_inner(&reply.payload, request)?;
        Ok(ProcessResult {
            status: std::process::ExitStatus::from_raw(i32::from(reply.exit_code) << 8),
            stdout: protocol::encode_json_frame(&reply.payload).map_err(|_| invalid_frame())?,
            stderr: Vec::new(),
        })
    }
}

fn verify_inner(payload: &Value, request: &ControllerRequest) -> Result<(), ChannelFailure> {
    if let Some(version) = payload.get("protocol_version").and_then(Value::as_u64)
        && version != u64::from(PROTOCOL_VERSION)
    {
        return Err(ChannelFailure::UnverifiedReply);
    }
    if payload.get("error").is_some() {
        let error: HostControlError =
            serde_json::from_value(payload.clone()).map_err(|_| invalid_frame())?;
        error.validate().map_err(|_| invalid_frame())
    } else {
        let reply: ControllerReadReply<Value> =
            serde_json::from_value(payload.clone()).map_err(|_| invalid_frame())?;
        reply
            .verify_envelope(request)
            .map_err(|_| ChannelFailure::UnverifiedReply)
    }
}

fn unique_json(payload: &[u8], limit: usize) -> Result<Value, ChannelFailure> {
    if payload.is_empty() || payload.len() > limit {
        return Err(invalid_frame());
    }
    let mut decoder = serde_json::Deserializer::from_slice(payload);
    let value = deserialize_unique_json(&mut decoder).map_err(|_| invalid_frame())?;
    decoder.end().map_err(|_| invalid_frame())?;
    Ok(value)
}

fn encode_bounded(value: &impl Serialize, limit: usize) -> Result<Vec<u8>, ChannelFailure> {
    let payload = serde_json::to_vec(value).map_err(|_| invalid_frame())?;
    if payload.len() > limit {
        return Err(invalid_frame());
    }
    protocol::encode_frame(&payload).map_err(|_| invalid_frame())
}

fn strict_fields(value: &Value, allowed: &[&str]) -> Result<(), ChannelFailure> {
    let object = value.as_object().ok_or_else(invalid_frame)?;
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(invalid_frame());
    }
    Ok(())
}

fn invalid_frame() -> ChannelFailure {
    ChannelFailure::Unavailable(ChannelReason::InvalidFrame)
}
