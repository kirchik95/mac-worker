//! Real T2 wire and Unix session acceptance on the frozen T1 contracts.

use mac_worker::test_support::channel::{
    codec::{FramedSocketConnector, SessionCodec},
    contracts::*,
    testing::{ManualRuntime, identity_fixture, request_fixture, result_fixture},
};
use mac_worker::test_support::controller::protocol::{MAX_FRAME_BYTES, decode_frame, encode_frame};
use mac_worker::test_support::{
    controller::protocol::ControllerRequest, host::process::ProcessResult,
};
use serde_json::{Value, json};

fn identity_json() -> Value {
    serde_json::to_value(identity()).unwrap()
}
fn identity() -> SocketIdentity {
    identity_fixture()
}
fn hello() -> Value {
    let id = identity_json();
    json!({"kind": "hello", "channel_version": 1, "route_sha256": id["route_sha256"], "expected_service": id["service"]})
}
fn ready() -> Value {
    let id = identity_json();
    json!({"kind": "ready", "route_sha256": id["route_sha256"], "service": id["service"]})
}
fn bytes(value: &Value) -> Vec<u8> {
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

fn request() -> ControllerRequest {
    request_fixture(
        "task.wait.poll",
        json!({"task_id":"66666666666646668666666666666666"}),
    )
}
fn read_payload(request: &ControllerRequest) -> Value {
    json!({"protocol_version": 7, "request_id": request.request_id(),
        "command": request.command(), "payload_sha256": request.payload_sha256(),
        "result": {"message": "unchanged", "nested": [1, null, true]}})
}
fn process(payload: &Value, code: i32) -> ProcessResult {
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
    let payload = json!({"protocol_version":6,"error":{"code":"CAPACITY_BUSY","message":"busy"}});
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

mod io {
    use super::*;
    use mac_worker::test_support::channel::contracts::{
        ChannelRuntime, DecodeProgress, FrameDecoder,
    };
    use mac_worker::test_support::controller::protocol::{
        decode_frame, encode_frame, encode_json_frame, parse_request,
    };
    use serde_json::{Value, json};
    use std::{
        cell::Cell,
        io::{Read, Write},
        net::Shutdown,
        os::fd::AsRawFd,
        os::unix::net::{UnixListener, UnixStream},
        path::Path,
        rc::Rc,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        thread,
        time::Duration,
    };

    const HANG_GUARD: Duration = Duration::from_secs(30);
    type Runtime = ManualRuntime;
    fn context<'a>(runtime: &'a Runtime, stop: &'a dyn Fn() -> bool) -> ClientContext<'a> {
        ClientContext {
            runtime,
            deadline: runtime.now() + HANG_GUARD,
            should_stop: stop,
        }
    }
    fn fixture<T: Send + 'static>(
        serve: impl FnOnce(UnixStream) -> T + Send + 'static,
    ) -> (tempfile::TempDir, std::path::PathBuf, thread::JoinHandle<T>) {
        let dir = tempfile::Builder::new()
            .prefix("p3t2-")
            .tempdir_in("/private/tmp")
            .unwrap();
        let path = dir.path().join("s");
        let listener = UnixListener::bind(&path).unwrap();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream.set_read_timeout(Some(HANG_GUARD)).unwrap();
            stream.set_write_timeout(Some(HANG_GUARD)).unwrap();
            serve(stream)
        });
        (dir, path, handle)
    }
    fn read_one(stream: &mut UnixStream) -> Vec<u8> {
        let mut prefix = [0; 4];
        stream.read_exact(&mut prefix).unwrap();
        let len = u32::from_be_bytes(prefix) as usize;
        assert!(
            (1..=mac_worker::test_support::controller::protocol::MAX_FRAME_BYTES).contains(&len)
        );
        let mut payload = vec![0; len];
        stream.read_exact(&mut payload).unwrap();
        payload
    }
    fn accept_hello(stream: &mut UnixStream) {
        let hello: Value = serde_json::from_slice(&read_one(stream)).unwrap();
        assert_eq!(hello["kind"], "hello");
        assert_eq!(hello["channel_version"], 1);
        assert_eq!(
            hello["expected_service"],
            serde_json::to_value(identity()).unwrap()["service"]
        );
        assert_eq!(hello["route_sha256"], "a".repeat(64));
    }
    fn connector() -> FramedSocketConnector {
        FramedSocketConnector::new(Arc::new(SessionCodec::new()))
    }
    fn request_frame(request: &ControllerRequest) -> Vec<u8> {
        encode_json_frame(&json!({"protocol_version":7,"request_id":request.request_id(),"command":request.command(),"body":request.body()})).unwrap()
    }
    fn assert_reason<T>(result: Result<T, ChannelFailure>, reason: ChannelReason) {
        assert!(matches!(result, Err(ChannelFailure::Unavailable(actual)) if actual == reason));
    }

    #[test]
    fn session_sends_hello_before_application_and_reuses_a_sequential_connection() {
        let (_dir, path, server) = fixture(|mut stream| {
            accept_hello(&mut stream);
            let ready = SessionCodec::new().encode_ready(&identity()).unwrap();
            for chunk in ready.chunks(3) {
                stream.write_all(chunk).unwrap();
            }
            for _ in 0..2 {
                let payload = read_one(&mut stream);
                let request = parse_request(&payload).unwrap();
                assert_eq!(request.command(), "task.wait.poll");
                let reply = SessionCodec::new()
                    .encode_reply(&request, &process(&read_payload(&request), 0))
                    .unwrap();
                for chunk in reply.chunks(7) {
                    stream.write_all(chunk).unwrap();
                }
            }
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).unwrap(), 0);
        });
        let runtime = Runtime::default();
        let ctx = context(&runtime, &|| false);
        let mut session = connector().connect(&path, &identity(), &ctx).unwrap();
        let request = request();
        let frame = request_frame(&request);
        for _ in 0..2 {
            let result = session.exchange(&frame, &request, &ctx).unwrap();
            assert_eq!(result.status.code(), Some(0));
            assert_eq!(
                serde_json::from_slice::<Value>(decode_frame(&result.stdout).unwrap()).unwrap(),
                read_payload(&request)
            );
        }
        session.close();
        session.close();
        server.join().unwrap();
        assert_reason(
            session.exchange(&frame, &request, &ctx),
            ChannelReason::ForwardLost,
        );
    }

    #[test]
    fn failed_ready_closes_without_sending_application_bytes() {
        for (pointer, changed) in [
            ("/route_sha256", json!("b".repeat(64))),
            (
                "/service/controller_client_id",
                json!("99999999999949918999999999999999"),
            ),
            ("/service/account/uid", json!(502)),
            ("/service/leader/pid", json!(124)),
            (
                "/service/service_generation",
                json!("44444444-4444-4444-8444-444444444444"),
            ),
        ] {
            let mut ready = ready();
            *ready.pointer_mut(pointer).unwrap() = changed;
            let frame = encode_frame(&bytes(&ready)).unwrap();
            let (_dir, path, server) = fixture(move |mut stream| {
                accept_hello(&mut stream);
                stream.write_all(&frame).unwrap();
                let mut app = Vec::new();
                stream.read_to_end(&mut app).unwrap();
                assert!(app.is_empty());
            });
            let runtime = Runtime::default();
            assert_reason(
                connector().connect(&path, &identity(), &context(&runtime, &|| false)),
                ChannelReason::ServiceUnavailable,
            );
            server.join().unwrap();
        }
    }

    #[test]
    fn live_ready_allows_optional_journal_change() {
        let (_dir, path, server) = fixture(|mut stream| {
            accept_hello(&mut stream);
            let mut value = serde_json::to_value(identity()).unwrap();
            value["service"]["journal_id"] = json!("33333333-3333-4333-8333-333333333333");
            let id: SocketIdentity = serde_json::from_value(value).unwrap();
            stream
                .write_all(&SessionCodec::new().encode_ready(&id).unwrap())
                .unwrap();
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).unwrap(), 0);
        });
        let runtime = Runtime::default();
        let mut session = connector()
            .connect(&path, &identity(), &context(&runtime, &|| false))
            .unwrap();
        session.close();
        server.join().unwrap();
    }

    #[test]
    fn handshake_eof_at_partial_prefix_or_payload_never_sends_rpc() {
        for truncated in [vec![], vec![0, 0], vec![0, 0, 0, 8, b'{', b'"']] {
            let (_dir, path, server) = fixture(move |mut stream| {
                accept_hello(&mut stream);
                stream.write_all(&truncated).unwrap();
                stream.shutdown(Shutdown::Write).unwrap();
                let mut app = Vec::new();
                stream.read_to_end(&mut app).unwrap();
                assert!(app.is_empty());
            });
            let runtime = Runtime::default();
            assert!(
                connector()
                    .connect(&path, &identity(), &context(&runtime, &|| false))
                    .is_err()
            );
            server.join().unwrap();
        }
    }

    #[test]
    fn borrowed_non_send_predicate_cancels_an_entered_handshake_without_runtime_cancel() {
        let stopped = Arc::new(AtomicBool::new(false));
        let entered = stopped.clone();
        let (_dir, path, server) = fixture(move |mut stream| {
            accept_hello(&mut stream);
            entered.store(true, Ordering::SeqCst);
            let mut app = Vec::new();
            stream.read_to_end(&mut app).unwrap();
            assert!(app.is_empty());
        });
        let borrowed = Rc::new(Cell::new(0));
        let stop = || {
            borrowed.set(borrowed.get() + 1);
            stopped.load(Ordering::SeqCst)
        };
        let runtime = Runtime::default();
        assert_reason(
            connector().connect(&path, &identity(), &context(&runtime, &stop)),
            ChannelReason::Cancelled,
        );
        assert!(!runtime.cancelled());
        assert!(borrowed.get() > 1);
        server.join().unwrap();
    }

    #[test]
    fn borrowed_predicate_cancels_an_entered_application_read_without_runtime_cancel() {
        let stopped = Arc::new(AtomicBool::new(false));
        let entered = stopped.clone();
        let (_dir, path, server) = fixture(move |mut stream| {
            accept_hello(&mut stream);
            stream
                .write_all(&SessionCodec::new().encode_ready(&identity()).unwrap())
                .unwrap();
            let request = parse_request(&read_one(&mut stream)).unwrap();
            assert_eq!(request.command(), "task.wait.poll");
            entered.store(true, Ordering::SeqCst);
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).unwrap(), 0);
        });
        let borrowed = Rc::new(Cell::new(0));
        let stop = || {
            borrowed.set(borrowed.get() + 1);
            stopped.load(Ordering::SeqCst)
        };
        let runtime = Runtime::default();
        let ctx = context(&runtime, &stop);
        let mut session = connector().connect(&path, &identity(), &ctx).unwrap();
        let request = request();
        assert_reason(
            session.exchange(&request_frame(&request), &request, &ctx),
            ChannelReason::Cancelled,
        );
        assert!(!runtime.cancelled());
        server.join().unwrap();
    }

    fn small_receive_buffer(stream: &UnixStream) {
        let size: libc::c_int = 2048;
        // SAFETY: a live socket and a correctly sized read-only integer option.
        assert_eq!(
            unsafe {
                libc::setsockopt(
                    stream.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_RCVBUF,
                    (&size as *const libc::c_int).cast(),
                    std::mem::size_of_val(&size) as libc::socklen_t,
                )
            },
            0
        );
    }
    fn large_request() -> ControllerRequest {
        request_fixture("task.wait.poll", json!({"data":"x".repeat(256 * 1024)}))
    }
    #[test]
    fn borrowed_predicate_cancels_a_partial_application_write() {
        let stopped = Arc::new(AtomicBool::new(false));
        let entered = stopped.clone();
        let (release, released) = mpsc::channel();
        let (_dir, path, server) = fixture(move |mut peer| {
            small_receive_buffer(&peer);
            accept_hello(&mut peer);
            peer.write_all(&SessionCodec::new().encode_ready(&identity()).unwrap())
                .unwrap();
            let mut byte = [0];
            peer.read_exact(&mut byte).unwrap();
            entered.store(true, Ordering::SeqCst);
            released.recv_timeout(HANG_GUARD).unwrap();
            let mut rest = Vec::new();
            peer.read_to_end(&mut rest).unwrap();
            rest.len() + 1
        });
        let runtime = Runtime::default();
        let captured = Rc::new(Cell::new(0));
        let stop = || {
            captured.set(captured.get() + 1);
            stopped.load(Ordering::SeqCst)
        };
        let ctx = context(&runtime, &stop);
        let mut session = connector().connect(&path, &identity(), &ctx).unwrap();
        let request = large_request();
        let frame = request_frame(&request);
        let result = session.exchange(&frame, &request, &ctx);
        session.close();
        release.send(()).unwrap();
        let transmitted = server.join().unwrap();
        assert_reason(result, ChannelReason::Cancelled);
        assert!(!runtime.cancelled());
        assert!((1..frame.len()).contains(&transmitted));
    }

    #[test]
    fn session_completes_partial_writes_without_truncating_the_request() {
        let (_dir, path, server) = fixture(|mut peer| {
            small_receive_buffer(&peer);
            accept_hello(&mut peer);
            peer.write_all(&SessionCodec::new().encode_ready(&identity()).unwrap())
                .unwrap();
            let payload = read_one(&mut peer);
            let request = parse_request(&payload).unwrap();
            assert_eq!(request.body()["data"].as_str().unwrap().len(), 256 * 1024);
            peer.write_all(
                &SessionCodec::new()
                    .encode_reply(
                        &request,
                        &result_fixture(
                            &request,
                            json!({"message":"unchanged","nested":[1,null,true]}),
                            0,
                        ),
                    )
                    .unwrap(),
            )
            .unwrap();
            payload
        });
        let request = large_request();
        let frame = request_frame(&request);
        let runtime = Runtime::default();
        let ctx = context(&runtime, &|| false);
        let mut session = connector().connect(&path, &identity(), &ctx).unwrap();
        assert_eq!(
            session
                .exchange(&frame, &request, &ctx)
                .unwrap()
                .status
                .code(),
            Some(0)
        );
        session.close();
        assert_eq!(server.join().unwrap(), decode_frame(&frame).unwrap());
    }

    #[test]
    fn handshake_deadline_uses_the_injected_clock() {
        let runtime = Arc::new(Runtime::default());
        let advance = runtime.clone();
        let (_dir, path, server) = fixture(move |mut stream| {
            accept_hello(&mut stream);
            advance.advance(Duration::from_secs(6));
            let mut app = Vec::new();
            stream.read_to_end(&mut app).unwrap();
            assert!(app.is_empty());
        });
        assert_reason(
            connector().connect(&path, &identity(), &context(&runtime, &|| false)),
            ChannelReason::Timeout,
        );
        server.join().unwrap();
    }

    #[test]
    fn application_deadline_does_not_reset_the_original_call_budget() {
        let runtime = Arc::new(Runtime::default());
        let advance = runtime.clone();
        let (_dir, path, server) = fixture(move |mut stream| {
            accept_hello(&mut stream);
            stream
                .write_all(&SessionCodec::new().encode_ready(&identity()).unwrap())
                .unwrap();
            read_one(&mut stream);
            advance.advance(Duration::from_secs(4));
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).unwrap(), 0);
        });
        let ctx = ClientContext {
            runtime: runtime.as_ref(),
            deadline: Duration::from_secs(3),
            should_stop: &|| false,
        };
        let mut session = connector().connect(&path, &identity(), &ctx).unwrap();
        let request = request();
        assert_reason(
            session.exchange(&request_frame(&request), &request, &ctx),
            ChannelReason::Timeout,
        );
        server.join().unwrap();
    }

    #[test]
    fn session_eof_at_partial_reply_closes_the_connection() {
        let (_dir, path, server) = fixture(|mut stream| {
            accept_hello(&mut stream);
            stream
                .write_all(&SessionCodec::new().encode_ready(&identity()).unwrap())
                .unwrap();
            read_one(&mut stream);
            stream.write_all(&[0, 0, 0, 20, b'{']).unwrap();
            stream.shutdown(Shutdown::Write).unwrap();
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).unwrap(), 0);
        });
        let runtime = Runtime::default();
        let ctx = context(&runtime, &|| false);
        let mut session = connector().connect(&path, &identity(), &ctx).unwrap();
        let request = request();
        assert_reason(
            session.exchange(&request_frame(&request), &request, &ctx),
            ChannelReason::ForwardLost,
        );
        assert_reason(
            session.exchange(&request_frame(&request), &request, &ctx),
            ChannelReason::ForwardLost,
        );
        server.join().unwrap();
    }

    #[test]
    fn already_cancelled_borrowed_context_never_opens_a_socket() {
        let runtime = Runtime::default();
        let capture = Rc::new(Cell::new(true));
        let stop = || capture.get();
        assert_reason(
            connector().connect(
                Path::new("/private/tmp/p3t2-missing-socket"),
                &identity(),
                &context(&runtime, &stop),
            ),
            ChannelReason::Cancelled,
        );
        assert!(!runtime.cancelled());
    }

    #[test]
    fn connected_socket_cannot_leak_through_exec() {
        let (closed, observed) = mpsc::channel();
        let (_dir, path, server) = fixture(move |mut peer| {
            accept_hello(&mut peer);
            peer.write_all(&SessionCodec::new().encode_ready(&identity()).unwrap())
                .unwrap();
            let mut byte = [0];
            assert_eq!(peer.read(&mut byte).unwrap(), 0);
            closed.send(()).unwrap();
        });
        struct ChildGuard(std::process::Child);
        impl Drop for ChildGuard {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let runtime = Runtime::default();
        let ctx = context(&runtime, &|| false);
        let mut session = connector().connect(&path, &identity(), &ctx).unwrap();
        let mut child = ChildGuard(
            std::process::Command::new("/bin/sh")
                .args(["-c", "printf 'ready\n'; read -r release"])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap(),
        );
        let mut ready = [0; 6];
        child
            .0
            .stdout
            .take()
            .unwrap()
            .read_exact(&mut ready)
            .unwrap();
        assert_eq!(&ready, b"ready\n");
        session.close();
        // The peer must see EOF while the gated child is still alive.
        observed.recv_timeout(HANG_GUARD).unwrap();
        child
            .0
            .stdin
            .take()
            .unwrap()
            .write_all(b"release\n")
            .unwrap();
        assert!(child.0.wait().unwrap().success());
        server.join().unwrap();
    }

    struct AdvancingCodec {
        runtime: Arc<Runtime>,
        armed: Arc<AtomicBool>,
    }
    struct AdvancingDecoder {
        inner: Box<dyn FrameDecoder>,
        runtime: Arc<Runtime>,
        armed: Arc<AtomicBool>,
    }
    impl FrameDecoder for AdvancingDecoder {
        fn feed(&mut self, input: &[u8]) -> Result<DecodeProgress, ChannelFailure> {
            let progress = self.inner.feed(input)?;
            if self.armed.load(Ordering::SeqCst)
                && progress.payload.is_none()
                && progress.consumed > 0
            {
                self.runtime.advance(Duration::from_secs(6));
            }
            Ok(progress)
        }
        fn retained_bytes(&self) -> usize {
            self.inner.retained_bytes()
        }
    }
    impl ChannelCodec for AdvancingCodec {
        fn decoder(&self) -> Box<dyn FrameDecoder> {
            Box::new(AdvancingDecoder {
                inner: SessionCodec::new().decoder(),
                runtime: self.runtime.clone(),
                armed: self.armed.clone(),
            })
        }
        fn encode_hello(&self, id: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure> {
            SessionCodec::new().encode_hello(id)
        }
        fn decode_hello(&self, payload: &[u8]) -> Result<SocketIdentity, ChannelFailure> {
            SessionCodec::new().decode_hello(payload)
        }
        fn encode_ready(&self, id: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure> {
            SessionCodec::new().encode_ready(id)
        }
        fn decode_ready(&self, payload: &[u8], id: &SocketIdentity) -> Result<(), ChannelFailure> {
            SessionCodec::new().decode_ready(payload, id)
        }
        fn encode_reply(
            &self,
            request: &ControllerRequest,
            result: &ProcessResult,
        ) -> Result<Vec<u8>, ChannelFailure> {
            SessionCodec::new().encode_reply(request, result)
        }
        fn decode_reply(
            &self,
            payload: &[u8],
            request: &ControllerRequest,
        ) -> Result<ProcessResult, ChannelFailure> {
            SessionCodec::new().decode_reply(payload, request)
        }
    }

    #[test]
    fn partial_reply_guard_uses_injected_time_before_the_application_deadline() {
        let (_dir, path, server) = fixture(|mut peer| {
            accept_hello(&mut peer);
            peer.write_all(&SessionCodec::new().encode_ready(&identity()).unwrap())
                .unwrap();
            read_one(&mut peer);
            peer.write_all(&[0, 0]).unwrap();
            let mut byte = [0];
            assert_eq!(peer.read(&mut byte).unwrap(), 0);
        });
        let runtime = Arc::new(Runtime::default());
        let armed = Arc::new(AtomicBool::new(false));
        let connector = FramedSocketConnector::new(Arc::new(AdvancingCodec {
            runtime: runtime.clone(),
            armed: armed.clone(),
        }));
        let ctx = context(&runtime, &|| false);
        let mut session = connector.connect(&path, &identity(), &ctx).unwrap();
        armed.store(true, Ordering::SeqCst);
        let request = request();
        assert_reason(
            session.exchange(&request_frame(&request), &request, &ctx),
            ChannelReason::Timeout,
        );
        assert_eq!(runtime.now(), Duration::from_secs(6));
        server.join().unwrap();
    }

    #[test]
    fn session_rejects_mismatched_request_frames_before_sending_bytes() {
        let (_dir, path, server) = fixture(|mut peer| {
            accept_hello(&mut peer);
            peer.write_all(&SessionCodec::new().encode_ready(&identity()).unwrap())
                .unwrap();
            let mut byte = [0];
            assert_eq!(peer.read(&mut byte).unwrap(), 0);
        });
        let runtime = Runtime::default();
        let ctx = context(&runtime, &|| false);
        let mut session = connector().connect(&path, &identity(), &ctx).unwrap();
        let request = request();
        assert_reason(
            session.exchange(&encode_frame(b"{}").unwrap(), &request, &ctx),
            ChannelReason::InvalidFrame,
        );
        server.join().unwrap();
    }

    #[test]
    fn connect_rejects_parent_components_before_attempting_io() {
        let runtime = Runtime::default();
        let calls = Rc::new(Cell::new(0));
        // A guard stops the old implementation if it accepts this unsafe path.
        let stop = || {
            calls.set(calls.get() + 1);
            calls.get() >= 3
        };
        let ctx = context(&runtime, &stop);
        assert_reason(
            connector().connect(
                std::path::Path::new("/private/tmp/../p3t2-parent/s"),
                &identity(),
                &ctx,
            ),
            ChannelReason::UnsafePath,
        );
    }
}
