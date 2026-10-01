//! Bounded framing for the sequential controller read channel.
//!
//! These codecs consume one socket frame without requiring EOF. The existing
//! stdio codec and its strict EOF contract remain unchanged.

use std::os::unix::process::ExitStatusExt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub mod io;
pub use io::FramedSocketConnector;

use super::contracts::*;
pub use super::contracts::{ChannelCodec, DecodeProgress, FrameDecoder, SocketConnector, SocketSession};
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
        validate_identity(expected)?;
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
        validate_identity(&identity)?;
        Ok(identity)
    }

    fn encode_ready(&self, identity: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure> {
        validate_identity(identity)?;
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
        validate_identity(expected)?;
        validate_identity(&actual)?;
        let a = &actual.service;
        let e = &expected.service;
        // Journal identity is an optional hint. Events validate their own cursor.
        if actual.route_sha256 != expected.route_sha256
            || a.protocol_version != e.protocol_version
            || a.channel_version != e.channel_version
            || a.controller_client_id != e.controller_client_id
            || a.account != e.account
            || a.leader != e.leader
            || a.service_generation != e.service_generation
            || a.socket_path != e.socket_path
            || a.features != e.features
        {
            return Err(ChannelFailure::Unavailable(
                ChannelReason::ServiceUnavailable,
            ));
        }
        Ok(())
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

fn validate_identity(identity: &SocketIdentity) -> Result<(), ChannelFailure> {
    // Phase A DTO shells lack the T1 validators. Keep validation at the wire
    // boundary; Phase B will consume the frozen validated identity types.
    let value = serde_json::to_value(identity).map_err(|_| invalid_frame())?;
    let route = value["route_sha256"].as_str().ok_or_else(invalid_frame)?;
    if route.len() != 64
        || !route
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid_frame());
    }
    let service = &identity.service;
    if service.protocol_version != PROTOCOL_VERSION
        || service.channel_version != CHANNEL_VERSION
        || service.leader.pid() == 0
        || service.leader.start_time_micros() == 0
    {
        return Err(invalid_frame());
    }
    validate_uuid(&value["service"]["service_generation"])?;
    if !value["service"]["journal_id"].is_null() {
        validate_uuid(&value["service"]["journal_id"])?;
        if value["service"]["journal_id"] == value["service"]["service_generation"] {
            return Err(invalid_frame());
        }
    }
    if service.features.len() > 64
        || service
            .features
            .iter()
            .any(|s| s.is_empty() || s.len() > 64 || s.chars().any(char::is_control))
        || service.features.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(invalid_frame());
    }
    if service.account.username.is_empty()
        || service.account.username.len() > 256
        || service.account.username.chars().any(char::is_control)
    {
        return Err(invalid_frame());
    }
    let home = service.account.home.to_str().ok_or_else(invalid_frame)?;
    if !service.account.home.is_absolute()
        || home.len() > 4096
        || home.chars().any(char::is_control)
    {
        return Err(invalid_frame());
    }
    let socket = service.socket_path.to_str().ok_or_else(invalid_frame)?;
    if !service.socket_path.is_absolute()
        || socket.len() >= 104
        || socket
            .chars()
            .any(|c| c.is_control() || matches!(c, ':' | '%' | '$'))
    {
        return Err(invalid_frame());
    }
    Ok(())
}

fn validate_uuid(value: &Value) -> Result<(), ChannelFailure> {
    let text = value.as_str().ok_or_else(invalid_frame)?;
    let uuid = uuid::Uuid::try_parse(text).map_err(|_| invalid_frame())?;
    if uuid.is_nil()
        || uuid.get_version_num() != 4
        || uuid.get_variant() != uuid::Variant::RFC4122
        || uuid.hyphenated().to_string() != text
    {
        return Err(invalid_frame());
    }
    Ok(())
}

fn invalid_frame() -> ChannelFailure {
    ChannelFailure::Unavailable(ChannelReason::InvalidFrame)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::protocol::{MAX_FRAME_BYTES, decode_frame, encode_frame};
    use serde_json::{Value, json};

    fn identity_json() -> Value {
        json!({
            "route_sha256": "a".repeat(64),
            "service": {
                "protocol_version": 7, "channel_version": 1,
                "controller_client_id": "11111111111141118111111111111111",
                "account": {"uid": 501, "username": "controller", "home": "/Users/controller"},
                "leader": {"pid": 123, "start_time_micros": 456},
                "service_generation": "22222222-2222-4222-8222-222222222222",
                "socket_path": "/Users/controller/rpc/s",
                "features": ["controller.socket"], "journal_id": null
            }
        })
    }
    pub(super) fn identity() -> SocketIdentity {
        serde_json::from_value(identity_json()).unwrap()
    }
    fn hello() -> Value {
        let id = identity_json();
        json!({"kind": "hello", "channel_version": 1, "route_sha256": id["route_sha256"], "expected_service": id["service"]})
    }
    fn ready() -> Value {
        let id = identity_json();
        json!({"kind": "ready", "route_sha256": id["route_sha256"], "service": id["service"]})
    }
    pub(super) fn bytes(value: &Value) -> Vec<u8> {
        serde_json::to_vec(value).unwrap()
    }

    #[test]
    fn decoder_accepts_every_prefix_and_payload_split() {
        let payload = br#"{"value":17}"#;
        let frame = encode_frame(payload).unwrap();
        for split in 0..frame.len() {
            let mut decoder = SessionCodec::new().decoder();
            let first = decoder.feed(&frame[..split]).unwrap();
            assert_eq!(first.consumed, split);
            assert!(first.payload.is_none());
            assert_eq!(decoder.retained_bytes(), split);
            let second = decoder.feed(&frame[split..]).unwrap();
            assert_eq!(second.consumed, frame.len() - split);
            assert_eq!(second.payload.as_deref(), Some(payload.as_slice()));
            assert_eq!(decoder.retained_bytes(), 0);
        }
    }

    #[test]
    fn decoder_consumes_one_coalesced_frame_without_retaining_its_tail() {
        let mut input = encode_frame(b"first").unwrap();
        input.extend(encode_frame(b"second").unwrap());
        let mut decoder = SessionCodec::new().decoder();
        let first = decoder.feed(&input).unwrap();
        assert_eq!(first.consumed, 9);
        assert_eq!(first.payload.as_deref(), Some(b"first".as_slice()));
        assert_eq!(decoder.retained_bytes(), 0);
        let second = decoder.feed(&input[first.consumed..]).unwrap();
        assert_eq!(second.consumed, 10);
        assert_eq!(second.payload.as_deref(), Some(b"second".as_slice()));
        assert_eq!(decoder.retained_bytes(), 0);
        // The old stdio decoder continues requiring exactly one frame.
        assert!(decode_frame(&input).is_err());
    }

    #[test]
    fn decoder_rejects_invalid_lengths_before_retaining_payload() {
        for length in [0, (MAX_FRAME_BYTES + 1) as u32, u32::MAX] {
            let mut decoder = SessionCodec::new().decoder();
            assert!(
                decoder
                    .feed(&length.to_be_bytes()[..2])
                    .unwrap()
                    .payload
                    .is_none()
            );
            assert_eq!(decoder.retained_bytes(), 2);
            assert!(decoder.feed(&length.to_be_bytes()[2..]).is_err());
            assert!(decoder.retained_bytes() <= 4);
        }
    }

    #[test]
    fn decoder_bounds_a_maximum_payload_and_reuses_the_frame_slot() {
        let frame = encode_frame(&vec![b'x'; MAX_FRAME_BYTES]).unwrap();
        let mut decoder = SessionCodec::new().decoder();
        for chunk in frame[..frame.len() - 1].chunks(READ_SCRATCH_BYTES) {
            assert!(decoder.feed(chunk).unwrap().payload.is_none());
            assert!(decoder.retained_bytes() <= MAX_FRAME_BYTES + 4);
        }
        assert_eq!(decoder.retained_bytes(), MAX_FRAME_BYTES + 3);
        assert_eq!(
            decoder
                .feed(&frame[frame.len() - 1..])
                .unwrap()
                .payload
                .unwrap()
                .len(),
            MAX_FRAME_BYTES
        );
        assert_eq!(decoder.retained_bytes(), 0);
        assert_eq!(
            decoder.feed(&[0, 0, 0, 1, b'y']).unwrap().payload.unwrap(),
            b"y"
        );
    }

    #[test]
    fn hello_is_strict_and_round_trips_validated_identity() {
        let codec = SessionCodec::new();
        let frame = codec.encode_hello(&identity()).unwrap();
        let decoded = codec.decode_hello(decode_frame(&frame).unwrap()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), identity_json());
        for pointer in [
            "/extra",
            "/expected_service/extra",
            "/expected_service/account/extra",
        ] {
            let mut value = hello();
            let (parent, key) = pointer.rsplit_once('/').unwrap();
            value
                .pointer_mut(parent)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert(key.to_owned(), json!(true));
            assert!(codec.decode_hello(&bytes(&value)).is_err(), "{pointer}");
        }
    }

    #[test]
    fn ready_accepts_additions_and_optional_journal_hints() {
        let codec = SessionCodec::new();
        codec
            .decode_ready(
                decode_frame(&codec.encode_ready(&identity()).unwrap()).unwrap(),
                &identity(),
            )
            .unwrap();
        for journal in [
            None,
            Some(Value::Null),
            Some(json!("33333333-3333-4333-8333-333333333333")),
        ] {
            let mut value = ready();
            value["extra"] = json!({"future": true});
            value["service"]
                .as_object_mut()
                .unwrap()
                .remove("journal_id");
            if let Some(journal) = journal {
                value["service"]["journal_id"] = journal;
            }
            codec.decode_ready(&bytes(&value), &identity()).unwrap();
            let mut value = hello();
            value["expected_service"]
                .as_object_mut()
                .unwrap()
                .remove("journal_id");
            codec.decode_hello(&bytes(&value)).unwrap();
        }
    }

    #[test]
    fn hello_and_ready_reject_nested_duplicate_keys_and_trailing_json() {
        let codec = SessionCodec::new();
        let valid = String::from_utf8(bytes(&hello())).unwrap();
        for duplicate in [
            valid.replace("\"uid\":501", "\"uid\":501,\"uid\":501"),
            valid.replace(
                "\"kind\":\"hello\"",
                "\"kind\":\"hello\",\"kind\":\"hello\"",
            ),
            format!("{valid} {{}}"),
        ] {
            assert!(codec.decode_hello(duplicate.as_bytes()).is_err());
        }
        let duplicate = String::from_utf8(bytes(&ready()))
            .unwrap()
            .replace("\"uid\":501", "\"uid\":501,\"uid\":501");
        assert!(
            codec
                .decode_ready(duplicate.as_bytes(), &identity())
                .is_err()
        );
    }

    #[test]
    fn handshake_rejects_invalid_uuid_strings_and_equal_journal_generation() {
        let codec = SessionCodec::new();
        for invalid in [
            "00000000-0000-0000-0000-000000000000",
            "22222222-2222-1222-8222-222222222222",
            "22222222222242228222222222222222",
            "ABCDEFAB-2222-4222-8222-222222222222",
            "invalid",
        ] {
            for field in ["service_generation", "journal_id"] {
                let mut value = hello();
                value["expected_service"][field] = json!(invalid);
                assert!(
                    codec.decode_hello(&bytes(&value)).is_err(),
                    "{field} {invalid}"
                );
            }
        }
        let mut value = hello();
        value["expected_service"]["journal_id"] =
            value["expected_service"]["service_generation"].clone();
        assert!(codec.decode_hello(&bytes(&value)).is_err());
    }

    #[test]
    fn handshake_rejects_wrong_versions_kinds_feature_caps_and_sizes() {
        let codec = SessionCodec::new();
        for (pointer, invalid) in [
            ("/kind", json!("ready")),
            ("/channel_version", json!(2)),
            ("/expected_service/protocol_version", json!(6)),
            ("/expected_service/channel_version", json!(2)),
            ("/route_sha256", json!("bad")),
            (
                "/expected_service/features",
                json!(["controller.socket", "controller.socket"]),
            ),
            (
                "/expected_service/features",
                json!(["z", "controller.socket"]),
            ),
            ("/expected_service/features", json!(["x".repeat(65)])),
            (
                "/expected_service/features",
                json!(
                    (0..65)
                        .map(|n| format!("feature{n:02}"))
                        .collect::<Vec<_>>()
                ),
            ),
        ] {
            let mut value = hello();
            *value.pointer_mut(pointer).unwrap() = invalid;
            assert!(codec.decode_hello(&bytes(&value)).is_err(), "{pointer}");
        }
        let mut value = ready();
        value["large"] = json!("x".repeat(IDENTITY_BYTES));
        assert!(codec.decode_ready(&bytes(&value), &identity()).is_err());
        let mut id = identity();
        id.service.features = vec!["x".repeat(IDENTITY_BYTES)];
        assert!(codec.encode_hello(&id).is_err());
    }

    #[test]
    fn ready_checks_every_required_service_field_and_route() {
        let codec = SessionCodec::new();
        for (pointer, changed) in [
            ("/kind", json!("hello")),
            ("/route_sha256", json!("b".repeat(64))),
            (
                "/service/controller_client_id",
                json!("99999999999949918999999999999999"),
            ),
            ("/service/account/uid", json!(502)),
            ("/service/account/username", json!("other")),
            ("/service/account/home", json!("/Users/other")),
            ("/service/leader/pid", json!(124)),
            ("/service/leader/start_time_micros", json!(457)),
            (
                "/service/service_generation",
                json!("44444444-4444-4444-8444-444444444444"),
            ),
            ("/service/socket_path", json!("/Users/controller/other/s")),
            ("/service/features", json!(["controller.socket", "other"])),
        ] {
            let mut value = ready();
            *value.pointer_mut(pointer).unwrap() = changed;
            assert!(
                codec.decode_ready(&bytes(&value), &identity()).is_err(),
                "{pointer}"
            );
        }
    }

    pub(super) fn request() -> ControllerRequest {
        crate::controller::protocol::parse_request(&bytes(&json!({
            "protocol_version": 7, "request_id": "55555555555545558555555555555555",
            "command": "task.wait.poll", "body": {"task_id": "66666666666646668666666666666666"}
        })))
        .unwrap()
    }
    pub(super) fn read_payload(request: &ControllerRequest) -> Value {
        json!({"protocol_version": 7, "request_id": request.request_id(),
            "command": request.command(), "payload_sha256": request.payload_sha256(),
            "result": {"message": "unchanged", "nested": [1, null, true]}})
    }
    pub(super) fn process(payload: &Value, code: i32) -> ProcessResult {
        use std::os::unix::process::ExitStatusExt;
        ProcessResult {
            status: std::process::ExitStatus::from_raw(code << 8),
            stdout: encode_frame(&bytes(payload)).unwrap(),
            stderr: b"private diagnostic".to_vec(),
        }
    }
    fn wrapper(payload: Value, request: &ControllerRequest, code: i32) -> Value {
        json!({"kind": "reply", "request_id": request.request_id(),
            "payload_sha256": request.payload_sha256(), "exit_code": code, "payload": payload})
    }

    #[test]
    fn reply_round_trips_inner_read_and_application_error_statuses() {
        let codec = SessionCodec::new();
        let request = request();
        for (payload, code) in [
            (read_payload(&request), 0),
            (
                json!({"protocol_version":7,"error":{"code":"CONTROLLER_UNAVAILABLE","message":"unavailable"}}),
                69,
            ),
            (
                json!({"protocol_version":7,"error":{"code":"CAPACITY_BUSY","message":"busy"}}),
                75,
            ),
            (
                json!({"protocol_version":7,"error":{"code":"CURSOR_INVALID","message":"invalid cursor"}}),
                64,
            ),
        ] {
            let original = process(&payload, code);
            let encoded = codec.encode_reply(&request, &original).unwrap();
            let wire = decode_frame(&encoded).unwrap();
            let value: Value = serde_json::from_slice(wire).unwrap();
            assert_eq!(value["kind"], "reply");
            assert_eq!(value["exit_code"], code);
            assert_eq!(value["request_id"], request.request_id());
            assert_eq!(value["payload_sha256"], request.payload_sha256());
            assert_eq!(value["payload"], payload);
            let decoded = codec.decode_reply(wire, &request).unwrap();
            assert_eq!(decoded.status.code(), Some(code));
            assert_eq!(decoded.stdout, original.stdout);
            assert!(decoded.stderr.is_empty());
            assert!(!wire.windows(18).any(|part| part == b"private diagnostic"));
        }
    }

    #[test]
    fn reply_allows_outer_additions_and_preserves_inner_schema() {
        let codec = SessionCodec::new();
        let request = request();
        let payload = read_payload(&request);
        let mut value = wrapper(payload.clone(), &request, 0);
        value["future"] = json!({"ok":true});
        let result = codec.decode_reply(&bytes(&value), &request).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(decode_frame(&result.stdout).unwrap()).unwrap(),
            payload
        );
        value["payload"]["future"] = json!(true);
        assert!(codec.decode_reply(&bytes(&value), &request).is_err());
    }

    #[test]
    fn reply_rejects_wrong_complete_outer_identity_without_replay() {
        let codec = SessionCodec::new();
        let request = request();
        for (field, wrong) in [
            ("request_id", "77777777777747778777777777777777".to_owned()),
            ("payload_sha256", "b".repeat(64)),
        ] {
            let mut value = wrapper(read_payload(&request), &request, 0);
            value[field] = json!(wrong);
            assert!(matches!(
                codec.decode_reply(&bytes(&value), &request),
                Err(ChannelFailure::UnverifiedReply)
            ));
        }
    }

    #[test]
    fn reply_verifies_all_common_inner_envelope_fields() {
        let codec = SessionCodec::new();
        let request = request();
        for (field, wrong) in [
            ("protocol_version", json!(6)),
            ("command", json!("task.list")),
            ("request_id", json!("77777777777747778777777777777777")),
            ("payload_sha256", json!("b".repeat(64))),
        ] {
            let mut payload = read_payload(&request);
            payload[field] = wrong;
            assert!(
                matches!(
                    codec.decode_reply(&bytes(&wrapper(payload.clone(), &request, 0)), &request),
                    Err(ChannelFailure::UnverifiedReply)
                ),
                "{field}"
            );
            assert!(codec.encode_reply(&request, &process(&payload, 0)).is_err());
        }
        let payload =
            json!({"protocol_version":6,"error":{"code":"CAPACITY_BUSY","message":"busy"}});
        assert!(
            codec
                .decode_reply(&bytes(&wrapper(payload, &request, 75)), &request)
                .is_err()
        );
    }

    #[test]
    fn reply_rejects_signals_missing_malformed_and_duplicate_output() {
        use std::os::unix::process::ExitStatusExt;
        let codec = SessionCodec::new();
        let request = request();
        let mut result = process(&read_payload(&request), 0);
        result.status = std::process::ExitStatus::from_raw(libc::SIGTERM);
        assert!(codec.encode_reply(&request, &result).is_err());
        result.status = std::process::ExitStatus::from_raw(0);
        for output in [vec![], encode_frame(b"not JSON").unwrap(), vec![0,0,0,3,b'{'],
            encode_frame(br#"{"protocol_version":7,"error":{"code":"CAPACITY_BUSY","code":"CAPACITY_BUSY","message":"busy"}}"#).unwrap()] {
            result.stdout = output; assert!(codec.encode_reply(&request, &result).is_err());
        }
        for code in [json!(-1), json!(256), json!(1.5), Value::Null] {
            let mut value = wrapper(read_payload(&request), &request, 0);
            value["exit_code"] = code;
            assert!(codec.decode_reply(&bytes(&value), &request).is_err());
        }
        let duplicate = String::from_utf8(bytes(&wrapper(read_payload(&request), &request, 0)))
            .unwrap()
            .replace(
                "\"message\":\"unchanged\"",
                "\"message\":\"unchanged\",\"message\":\"unchanged\"",
            );
        assert!(codec.decode_reply(duplicate.as_bytes(), &request).is_err());
    }

    #[test]
    fn reply_wrap_cap_leaves_a_maximum_inner_reply_intact_for_stdio() {
        let codec = SessionCodec::new();
        let request = request();
        let mut payload = read_payload(&request);
        payload["result"] = json!({"data":""});
        let fixed = bytes(&payload).len();
        payload["result"]["data"] = json!("x".repeat(MAX_FRAME_BYTES - fixed));
        let original = process(&payload, 0);
        assert_eq!(original.stdout.len(), MAX_FRAME_BYTES + 4);
        assert!(codec.encode_reply(&request, &original).is_err());
        assert_eq!(
            serde_json::from_slice::<Value>(decode_frame(&original.stdout).unwrap()).unwrap(),
            payload
        );
        let oversized = wrapper(payload, &request, 0);
        assert!(bytes(&oversized).len() > MAX_FRAME_BYTES);
        assert!(codec.decode_reply(&bytes(&oversized), &request).is_err());
    }

    #[test]
    fn reply_wrap_accepts_the_exact_whole_frame_cap() {
        let codec = SessionCodec::new();
        let request = request();
        let mut payload = read_payload(&request);
        payload["result"] = json!({"data":""});
        let fixed = bytes(&wrapper(payload.clone(), &request, 0)).len();
        payload["result"]["data"] = json!("x".repeat(MAX_FRAME_BYTES - fixed));
        let original = process(&payload, 0);
        let encoded = codec.encode_reply(&request, &original).unwrap();
        assert_eq!(encoded.len(), MAX_FRAME_BYTES + 4);
        assert_eq!(
            codec
                .decode_reply(decode_frame(&encoded).unwrap(), &request)
                .unwrap()
                .stdout,
            original.stdout
        );
    }
}
