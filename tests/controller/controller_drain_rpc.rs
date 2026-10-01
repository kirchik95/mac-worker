//! The drain RPC owns only the admission flag; it neither opens the task
//! store nor writes durable request receipts.

use std::{io::Cursor, os::unix::process::ExitStatusExt, process::ExitStatus, sync::Mutex};

use mac_worker::test_support::{
    controller::{
        ControllerFault, ControllerReadReply, control::drain_via_controller, decode_frame,
        drain::is_drained, encode_json_frame, parse_request, serve_rpc_with_runtime,
    },
    core::{
        config::{Config, ControllerConfig},
        error::WorkerError,
        paths::PathLayout,
        protocol::PROTOCOL_VERSION,
    },
    host::process::{ProcessRequest, ProcessResult, ProcessRunner},
};
use serde_json::{Value, json};

const REQUEST_ID: &str = "018f0f4a6b5c7d8e9f00112233445560";

struct NoProcesses;

impl ProcessRunner for NoProcesses {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        panic!("drain RPC must not launch an external command: {request:?}");
    }
}

struct Fixture {
    paths: PathLayout,
    config: Config,
    _temp: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        Self {
            paths: PathLayout {
                config: root.join("config.toml"),
                state: root.join("state"),
                cache: root.join("cache"),
                data: root.join("data"),
            },
            config: Config::parse("version = 1\n").unwrap(),
            _temp: temp,
        }
    }

    fn frame(body: Value) -> Vec<u8> {
        encode_json_frame(&json!({
            "protocol_version": PROTOCOL_VERSION,
            "request_id": REQUEST_ID,
            "command": "controller.drain",
            "body": body,
        }))
        .unwrap()
    }

    fn rpc(&self, body: Value) -> Result<ControllerReadReply<Value>, WorkerError> {
        let frame = Self::frame(body);
        let request = parse_request(decode_frame(&frame)?)?;
        let mut stdout = Vec::new();
        serve_rpc_with_runtime(
            &self.paths,
            &self.config,
            &NoProcesses,
            &mut Cursor::new(frame),
            &mut stdout,
            ControllerFault::None,
        )?;
        let reply: ControllerReadReply<Value> =
            serde_json::from_slice(decode_frame(&stdout)?).unwrap();
        reply.verify_envelope(&request).unwrap();
        Ok(reply)
    }
}

#[test]
fn drain_read_rpc_observes_false_without_creating_any_state() {
    let fixture = Fixture::new();
    let reply = fixture.rpc(json!({})).expect("drain is a direct RPC");
    assert_eq!(reply.result(), &json!({"drained": false}));
    assert!(!fixture.paths.state.exists());
    assert!(!fixture.paths.controller_state_root().exists());
}

#[test]
fn drain_write_rpc_persists_flag_and_repeated_ids_use_direct_set_semantics() {
    let fixture = Fixture::new();
    for drained in [true, true, false, false, true] {
        let reply = fixture.rpc(json!({"drained": drained})).unwrap();
        assert_eq!(reply.result(), &json!({"drained": drained}));
        assert_eq!(
            is_drained(&fixture.paths.controller_state_root()).unwrap(),
            drained
        );
        assert_eq!(
            fixture.rpc(json!({})).unwrap().result(),
            &json!({"drained": drained})
        );
    }
    assert!(!fixture.paths.state.exists());
    let root = fixture.paths.controller_state_root();
    assert!(!root.join("requests.lock").exists());
    assert!(!root.join("active").exists());
    assert!(!root.join(format!("req-{REQUEST_ID}.json")).exists());
}

#[test]
fn malformed_drain_rpc_body_has_no_side_effects() {
    for body in [
        json!({"drained": null}),
        json!({"drained": "true"}),
        json!({"drained": 1}),
        json!({"drained": []}),
        json!({"drained": {}}),
        json!({"drained": true, "extra": false}),
        json!({"extra": false}),
    ] {
        let fixture = Fixture::new();
        let error = fixture.rpc(body.clone()).unwrap_err();
        assert_eq!(error.public_code(), "INVALID_REQUEST", "body={body}");
        assert!(!fixture.paths.state.exists(), "body={body}");
        assert!(
            !fixture.paths.controller_state_root().exists(),
            "body={body}"
        );
    }
}

fn controller_config() -> ControllerConfig {
    Config::parse("version = 1\n[controller]\nenabled = true\nssh = \"fixture-controller\"\n")
        .unwrap()
        .controller
}

struct Loopback {
    fixture: Fixture,
    bodies: Mutex<Vec<Value>>,
}

impl ProcessRunner for Loopback {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(request.program, "/usr/bin/ssh");
        assert_eq!(
            request.args.last().unwrap(),
            "~/.local/bin/worker host controller-rpc"
        );
        let frame = request.stdin.as_ref().unwrap();
        let parsed = parse_request(decode_frame(frame)?)?;
        assert_eq!(parsed.command(), "controller.drain");
        self.bodies.lock().unwrap().push(parsed.body().clone());
        let mut stdout = Vec::new();
        serve_rpc_with_runtime(
            &self.fixture.paths,
            &self.fixture.config,
            &NoProcesses,
            &mut Cursor::new(frame),
            &mut stdout,
            ControllerFault::None,
        )?;
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout,
            stderr: Vec::new(),
        })
    }
}

#[test]
fn drain_client_reads_and_writes_via_the_real_rpc_dispatch() {
    let loopback = Loopback {
        fixture: Fixture::new(),
        bodies: Mutex::new(Vec::new()),
    };
    let controller = controller_config();
    assert!(!drain_via_controller(&loopback, &controller, None).unwrap());
    assert!(drain_via_controller(&loopback, &controller, Some(true)).unwrap());
    assert!(drain_via_controller(&loopback, &controller, None).unwrap());
    assert!(!drain_via_controller(&loopback, &controller, Some(false)).unwrap());
    assert_eq!(
        *loopback.bodies.lock().unwrap(),
        [
            json!({}),
            json!({"drained": true}),
            json!({}),
            json!({"drained": false})
        ]
    );
    assert!(!loopback.fixture.paths.state.exists());
}

struct ReplyFixture {
    requested: Option<bool>,
    result: Value,
    envelope_override: Option<(&'static str, Value)>,
}

impl ProcessRunner for ReplyFixture {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let request = parse_request(decode_frame(request.stdin.as_ref().unwrap())?)?;
        assert_eq!(request.command(), "controller.drain");
        assert_eq!(
            request.body(),
            &match self.requested {
                Some(drained) => json!({"drained": drained}),
                None => json!({}),
            }
        );
        let mut reply = json!({
            "protocol_version": PROTOCOL_VERSION,
            "command": request.command(),
            "request_id": request.request_id(),
            "payload_sha256": request.payload_sha256(),
            "result": self.result,
        });
        if let Some((field, value)) = &self.envelope_override {
            reply[*field] = value.clone();
        }
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: encode_json_frame(&reply)?,
            stderr: Vec::new(),
        })
    }
}

#[test]
fn drain_client_accepts_either_boolean_for_reads_and_requires_write_value() {
    for drained in [false, true] {
        for requested in [None, Some(drained)] {
            let runner = ReplyFixture {
                requested,
                result: json!({"drained": drained}),
                envelope_override: None,
            };
            assert_eq!(
                drain_via_controller(&runner, &controller_config(), requested).unwrap(),
                drained
            );
        }
        let runner = ReplyFixture {
            requested: Some(drained),
            result: json!({"drained": !drained}),
            envelope_override: None,
        };
        assert_eq!(
            drain_via_controller(&runner, &controller_config(), Some(drained))
                .unwrap_err()
                .public_code(),
            "CONTROLLER_UNAVAILABLE"
        );
    }
}

#[test]
fn drain_client_rejects_malformed_results_and_mismatched_envelopes() {
    for result in [
        json!({}),
        json!({"drained": null}),
        json!({"drained": "true"}),
        json!({"drained": 1}),
        json!({"drained": true, "extra": false}),
    ] {
        let runner = ReplyFixture {
            requested: None,
            result,
            envelope_override: None,
        };
        assert_eq!(
            drain_via_controller(&runner, &controller_config(), None)
                .unwrap_err()
                .public_code(),
            "CONTROLLER_UNAVAILABLE"
        );
    }
    for field in [
        "protocol_version",
        "command",
        "request_id",
        "payload_sha256",
    ] {
        let value = if field == "protocol_version" {
            json!(PROTOCOL_VERSION + 1)
        } else {
            json!("private planted reply")
        };
        let runner = ReplyFixture {
            requested: Some(true),
            result: json!({"drained": true}),
            envelope_override: Some((field, value)),
        };
        let error = drain_via_controller(&runner, &controller_config(), Some(true)).unwrap_err();
        assert_eq!(error.public_code(), "CONTROLLER_UNAVAILABLE", "{field}");
        assert!(!error.to_string().contains("private planted reply"));
    }
}
