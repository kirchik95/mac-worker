use std::{
    collections::BTreeMap,
    ffi::OsString,
    io::{BufRead, BufReader, Cursor, Read},
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use mac_worker::{
    RuntimeContext,
    cli::{Cli, Command as WorkerCommand, HostCommand},
    client_state::ClientStateStore,
    config::Config,
    controller::{
        ControllerFault, ControllerLeader, ControllerStore, RequestPhase,
        controller_rpc_ssh_request, load_operation_envelope, persist_operation_envelope,
        protocol::{
            MAX_FRAME_BYTES, MAX_STORED_REQUEST_BYTES, canonical_request_sha256, decode_frame,
            decode_request, encode_frame, parse_request, read_frame,
        },
        serve_rpc,
    },
    error::WorkerError,
    job::HostControlError,
    paths::PathLayout,
    process::SystemProcessRunner,
    protocol::PROTOCOL_VERSION,
    run_with_stdio_in_context,
};
use serde_json::{Value, json};

const REQUEST_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";

fn frame_json(value: &Value) -> Vec<u8> {
    encode_frame(&serde_json::to_vec(value).unwrap()).unwrap()
}

fn valid_request_value() -> Value {
    json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": REQUEST_ID,
        "command": "checkpoint.submit",
        "body": {
            "prompt": "freeze this snapshot",
            "z": 1,
            "a": {"b": 2, "a": 1}
        }
    })
}

fn public_code(error: WorkerError) -> String {
    error.public_code()
}

#[test]
fn a_well_formed_frame_round_trips_and_hashes_the_canonical_identity() {
    let request = decode_request(&frame_json(&valid_request_value())).unwrap();

    assert_eq!(request.protocol_version(), PROTOCOL_VERSION);
    assert_eq!(request.request_id(), REQUEST_ID);
    assert_eq!(request.command(), "checkpoint.submit");
    assert_eq!(
        request.payload_sha256(),
        canonical_request_sha256(
            request.protocol_version(),
            request.command(),
            request.body()
        )
        .unwrap()
    );
    assert!(request.body().is_object());
}

#[test]
fn payload_identity_is_the_server_hash_of_version_command_and_sorted_body_keys() {
    let mut first = valid_request_value();
    first["body"] = json!({"z": 1, "a": 2, "nested": {"b": 1, "a": 2}});
    let mut second = first.clone();
    second["body"] = json!({"nested": {"a": 2, "b": 1}, "a": 2, "z": 1});

    let left = decode_request(&frame_json(&first)).unwrap();
    let right = decode_request(&frame_json(&second)).unwrap();

    assert_eq!(left.payload_sha256(), right.payload_sha256());
    assert_eq!(left.payload_sha256().len(), 64);
}

#[test]
fn same_request_id_and_empty_body_with_a_different_command_has_a_different_identity() {
    // Durable rows key by request_id; identity must still distinguish
    // task.cancel from task.close when both freeze body={}.
    let mut cancel = valid_request_value();
    cancel["command"] = json!("task.cancel");
    cancel["body"] = json!({});
    let mut close = cancel.clone();
    close["command"] = json!("task.close");

    let cancel = decode_request(&frame_json(&cancel)).unwrap();
    let close = decode_request(&frame_json(&close)).unwrap();

    assert_eq!(cancel.request_id(), close.request_id());
    assert_eq!(cancel.body(), close.body());
    assert_ne!(cancel.command(), close.command());
    assert_ne!(cancel.payload_sha256(), close.payload_sha256());
    assert_eq!(
        cancel.payload_sha256(),
        canonical_request_sha256(PROTOCOL_VERSION, "task.cancel", &json!({})).unwrap()
    );
    assert_eq!(
        close.payload_sha256(),
        canonical_request_sha256(PROTOCOL_VERSION, "task.close", &json!({})).unwrap()
    );
}

#[test]
fn a_client_supplied_digest_is_ignored() {
    let mut value = valid_request_value();
    let expected = decode_request(&frame_json(&value))
        .unwrap()
        .payload_sha256()
        .to_owned();
    value["payload_sha256"] = json!("0".repeat(64));

    let request = decode_request(&frame_json(&value)).unwrap();

    assert_eq!(request.payload_sha256(), expected);
    assert_ne!(request.payload_sha256(), "0".repeat(64));
}

#[test]
fn a_missing_or_non_object_body_is_rejected_before_any_store_exists() {
    for body in [Value::Null, json!("prompt"), json!([]), json!(1)] {
        let mut value = valid_request_value();
        value["body"] = body;
        let error = decode_request(&frame_json(&value)).unwrap_err();
        assert_eq!(public_code(error), "INVALID_REQUEST");
    }

    let mut missing = valid_request_value();
    missing.as_object_mut().unwrap().remove("body");
    let error = decode_request(&frame_json(&missing)).unwrap_err();
    assert_eq!(public_code(error), "INVALID_REQUEST");
}

#[test]
fn protocol_v6_and_unknown_versions_fail_closed_as_incompatible() {
    for version in [6_u64, 8, 0] {
        let mut value = valid_request_value();
        value["protocol_version"] = json!(version);
        let error = decode_request(&frame_json(&value)).unwrap_err();
        assert_eq!(public_code(error), "INCOMPATIBLE_PROTOCOL");
        assert_rpc_does_not_write(&frame_json(&value), "INCOMPATIBLE_PROTOCOL");
    }

    let mut missing = valid_request_value();
    missing.as_object_mut().unwrap().remove("protocol_version");
    let error = decode_request(&frame_json(&missing)).unwrap_err();
    assert_eq!(public_code(error), "INCOMPATIBLE_PROTOCOL");
}

#[test]
fn malformed_json_and_unknown_fields_do_not_parse() {
    let malformed = encode_frame(b"{not-json").unwrap();
    assert_eq!(
        public_code(decode_request(&malformed).unwrap_err()),
        "CONTROLLER_TRANSPORT"
    );
    assert_rpc_does_not_write(&malformed, "CONTROLLER_TRANSPORT");

    let mut extra = valid_request_value();
    extra["unexpected"] = json!(true);
    assert_eq!(
        public_code(decode_request(&frame_json(&extra)).unwrap_err()),
        "INVALID_REQUEST"
    );
    assert_rpc_does_not_write(&frame_json(&extra), "INVALID_REQUEST");

    let trailing_json = encode_frame(br#"{"protocol_version":7}{}"#).unwrap();
    assert_eq!(
        public_code(decode_request(&trailing_json).unwrap_err()),
        "CONTROLLER_TRANSPORT"
    );
    assert_eq!(
        public_code(parse_request(br#"{"protocol_version":7}{}"#).unwrap_err()),
        "CONTROLLER_TRANSPORT"
    );
    assert_rpc_does_not_write(&trailing_json, "CONTROLLER_TRANSPORT");
}

fn frame_raw(json: &str) -> Vec<u8> {
    encode_frame(json.as_bytes()).unwrap()
}

fn duplicate_request_payloads() -> Vec<String> {
    let version = PROTOCOL_VERSION;
    vec![
        format!(
            r#"{{"protocol_version":6,"protocol_version":{version},"request_id":"{REQUEST_ID}","command":"checkpoint.submit","body":{{}}}}"#
        ),
        format!(
            r#"{{"protocol_version":{version},"request_id":"{REQUEST_ID}","request_id":"018f0f4a6b5c7d8e9f00112233445577","command":"checkpoint.submit","body":{{}}}}"#
        ),
        format!(
            r#"{{"protocol_version":{version},"request_id":"{REQUEST_ID}","command":"task.cancel","command":"task.close","body":{{}}}}"#
        ),
        format!(
            r#"{{"protocol_version":{version},"request_id":"{REQUEST_ID}","command":"checkpoint.submit","body":{{"a":1}},"body":{{"a":2}}}}"#
        ),
        format!(
            r#"{{"protocol_version":{version},"request_id":"{REQUEST_ID}","command":"checkpoint.submit","body":{{"a":1,"a":2}}}}"#
        ),
    ]
}

#[test]
fn duplicate_request_keys_are_rejected_before_canonicalization_and_do_not_write() {
    // serde_json::Value would keep the last duplicate. These frames must fail
    // closed instead of hashing protocol 7, a second ID, or the last command/body.
    for payload in duplicate_request_payloads() {
        let framed = decode_request(&frame_raw(&payload)).unwrap_err();
        let unframed = parse_request(payload.as_bytes()).unwrap_err();
        assert_eq!(public_code(framed), "INVALID_REQUEST");
        assert_eq!(public_code(unframed), "INVALID_REQUEST");
        assert_rpc_does_not_write(&frame_raw(&payload), "INVALID_REQUEST");
    }
}

fn assert_rpc_does_not_write(frame: &[u8], expected_code: &str) {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("mac-worker-controller");
    let mut stdout = Vec::new();
    let error = serve_rpc(
        &state,
        &mut Cursor::new(frame),
        &mut stdout,
        ControllerFault::None,
    )
    .unwrap_err();
    assert_eq!(public_code(error), expected_code);
    assert!(stdout.is_empty());
    assert!(!state.exists());
}

#[test]
fn oversize_and_empty_length_prefixes_are_rejected_without_trusting_the_claimed_size() {
    let oversize = (MAX_FRAME_BYTES as u32 + 1).to_be_bytes();
    assert_eq!(
        public_code(decode_request(&oversize).unwrap_err()),
        "CONTROLLER_TRANSPORT"
    );
    assert_eq!(
        public_code(read_frame(&mut Cursor::new(oversize)).unwrap_err()),
        "CONTROLLER_TRANSPORT"
    );

    let empty = 0u32.to_be_bytes();
    assert_eq!(
        public_code(decode_request(&empty).unwrap_err()),
        "CONTROLLER_TRANSPORT"
    );
    assert_rpc_does_not_write(&oversize, "CONTROLLER_TRANSPORT");
    assert_rpc_does_not_write(&empty, "CONTROLLER_TRANSPORT");

    let too_large = vec![0u8; MAX_FRAME_BYTES + 1];
    assert_eq!(
        public_code(encode_frame(&too_large).unwrap_err()),
        "CONTROLLER_TRANSPORT"
    );
}

#[test]
fn truncated_frames_and_trailing_bytes_are_transport_errors() {
    let frame = frame_json(&valid_request_value());
    assert_eq!(
        public_code(decode_request(&frame[..3]).unwrap_err()),
        "CONTROLLER_TRANSPORT"
    );
    assert_eq!(
        public_code(decode_request(&frame[..frame.len() - 1]).unwrap_err()),
        "CONTROLLER_TRANSPORT"
    );
    assert_eq!(
        public_code(read_frame(&mut Cursor::new(&frame[..frame.len() - 1])).unwrap_err()),
        "CONTROLLER_TRANSPORT"
    );

    let mut trailing = frame.clone();
    trailing.push(0);
    assert_eq!(
        public_code(decode_request(&trailing).unwrap_err()),
        "CONTROLLER_TRANSPORT"
    );
    assert_eq!(
        public_code(read_frame(&mut Cursor::new(trailing)).unwrap_err()),
        "CONTROLLER_TRANSPORT"
    );
    assert_eq!(
        public_code(read_frame(&mut Cursor::new(Vec::<u8>::new())).unwrap_err()),
        "CONTROLLER_TRANSPORT"
    );
}

#[test]
fn ssh_stdio_accepts_one_frame_then_requires_stdin_eof() {
    // Transport must close stdin after the request frame and before waiting
    // for the ACK. The server reads exactly one frame and then EOF.
    let payload = vec![b'a'; MAX_FRAME_BYTES];
    let frame = encode_frame(&payload).unwrap();
    assert_eq!(frame.len(), 4 + MAX_FRAME_BYTES);
    assert_eq!(read_frame(&mut Cursor::new(&frame)).unwrap(), payload);

    let mut still_open = frame.clone();
    still_open.push(0);
    assert_eq!(
        public_code(read_frame(&mut Cursor::new(still_open)).unwrap_err()),
        "CONTROLLER_TRANSPORT"
    );
}

#[test]
fn invalid_request_ids_and_commands_are_rejected() {
    for request_id in [
        "",
        "ZZ",
        &"g".repeat(32),
        "018F0F4A6B5C7D8E9F00112233445566",
    ] {
        let mut value = valid_request_value();
        value["request_id"] = json!(request_id);
        assert_eq!(
            public_code(decode_request(&frame_json(&value)).unwrap_err()),
            "INVALID_REQUEST"
        );
    }

    let mut empty_command = valid_request_value();
    empty_command["command"] = json!("");
    assert_eq!(
        public_code(decode_request(&frame_json(&empty_command)).unwrap_err()),
        "INVALID_REQUEST"
    );
}

const WORKER_INVENTORY: &str = r#"
version = 1
[[workers]]
name = "mini-1"
ssh = "mac1"
slots = 1
"#;

#[test]
fn configs_without_a_controller_table_keep_local_task_client_defaults() {
    let config = Config::parse(WORKER_INVENTORY).unwrap();
    config.validate().unwrap();
    assert!(!config.controller.enabled);
    assert!(config.controller.ssh.is_empty());
    assert_eq!(config.controller.remote_binary, "~/.local/bin/worker");

    for contents in ["version = 1", "version = 1\nworkers = []\n"] {
        let config = Config::parse(contents).unwrap();
        assert!(matches!(config.validate(), Err(WorkerError::Config(_))));
        assert!(matches!(
            config.require_local_inventory(),
            Err(WorkerError::Config(_))
        ));
    }
}

#[test]
fn enabled_controller_config_requires_a_valid_ssh_destination() {
    let missing_ssh = format!("{WORKER_INVENTORY}\n[controller]\nenabled = true\n");
    let config = Config::parse(&missing_ssh).unwrap();
    assert!(matches!(config.validate(), Err(WorkerError::Config(_))));

    let invalid_ssh = format!("{WORKER_INVENTORY}\n[controller]\nenabled = true\nssh = \"-V\"\n");
    let config = Config::parse(&invalid_ssh).unwrap();
    assert!(matches!(config.validate(), Err(WorkerError::Config(_))));

    let wrong_binary = format!(
        "{WORKER_INVENTORY}\n[controller]\nenabled = true\nssh = \"user@mini.local\"\nremote_binary = \"/tmp/worker\"\n"
    );
    let config = Config::parse(&wrong_binary).unwrap();
    assert!(matches!(config.validate(), Err(WorkerError::Config(_))));
}

#[test]
fn enabled_controller_config_accepts_validated_ssh_and_fixed_remote_binary() {
    let contents = format!(
        "{WORKER_INVENTORY}\n[controller]\nenabled = true\nssh = \"user@always-on-host\"\n"
    );
    let config = Config::parse(&contents).unwrap();
    config.validate().unwrap();
    assert!(config.controller.enabled);
    assert_eq!(config.controller.ssh, "user@always-on-host");
    assert_eq!(config.controller.remote_binary, "~/.local/bin/worker");
}

#[test]
fn a_controller_only_laptop_config_omits_local_workers() {
    let contents = r#"
version = 1
[controller]
enabled = true
ssh = "user@always-on-host"
"#;
    let config = Config::parse(contents).unwrap();
    config.validate().unwrap();
    assert!(config.controller.enabled);
    assert!(config.workers.is_empty());
    assert!(matches!(
        config.require_local_inventory(),
        Err(WorkerError::Config(_))
    ));
}

#[test]
fn enabled_controller_without_ssh_is_rejected_without_a_dummy_worker() {
    let contents = "version = 1\n[controller]\nenabled = true\n";
    let config = Config::parse(contents).unwrap();
    assert!(matches!(config.validate(), Err(WorkerError::Config(_))));
}

fn host_controller_rpc_cli() -> Cli {
    Cli {
        config: None,
        json: false,
        command: WorkerCommand::Host {
            command: HostCommand::ControllerRpc,
        },
    }
}

fn parsed_request() -> mac_worker::controller::ControllerRequest {
    parse_request(&serde_json::to_vec(&valid_request_value()).unwrap()).unwrap()
}

fn open_store(temp: &tempfile::TempDir) -> (std::path::PathBuf, ControllerStore) {
    let state = temp.path().join("mac-worker-controller");
    let store = ControllerStore::open(&state).unwrap();
    (state, store)
}

fn isolated_paths(temp: &tempfile::TempDir) -> (RuntimeContext, PathLayout) {
    let home = temp.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let state_home = temp.path().join("state");
    let cache_home = temp.path().join("cache");
    std::fs::create_dir_all(&state_home).unwrap();
    std::fs::create_dir_all(&cache_home).unwrap();
    let environment = BTreeMap::from([
        (OsString::from("HOME"), home.as_os_str().to_os_string()),
        (
            OsString::from("XDG_STATE_HOME"),
            state_home.as_os_str().to_os_string(),
        ),
        (
            OsString::from("XDG_CACHE_HOME"),
            cache_home.as_os_str().to_os_string(),
        ),
        (
            OsString::from("XDG_CONFIG_HOME"),
            temp.path().join("config").into(),
        ),
        (
            OsString::from("XDG_DATA_HOME"),
            temp.path().join("data").into(),
        ),
    ]);
    let paths = PathLayout::discover(None, &environment, &home).unwrap();
    (
        RuntimeContext::isolated(environment, home, temp.path().to_path_buf()),
        paths,
    )
}

#[test]
fn concurrent_duplicate_payloads_reuse_the_same_ids() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("mac-worker-controller");
    ControllerStore::open(&state).unwrap();
    let request = parsed_request();

    thread::scope(|scope| {
        let left = scope.spawn(|| {
            ControllerStore::open(&state)
                .unwrap()
                .handle(&request, ControllerFault::None)
        });
        let right = scope.spawn(|| {
            ControllerStore::open(&state)
                .unwrap()
                .handle(&request, ControllerFault::None)
        });
        let left = left.join().unwrap().unwrap();
        let right = right.join().unwrap().unwrap();
        assert_eq!(left.request_id(), right.request_id());
        assert_eq!(left.payload_sha256(), right.payload_sha256());
        assert_eq!(left.task_id(), right.task_id());
        assert_eq!(left.turn_id(), right.turn_id());
        assert_eq!(left.created_at_millis(), right.created_at_millis());
        assert_eq!(left.status(), "acked");
        assert_eq!(right.status(), "acked");
    });
    assert_eq!(
        ControllerStore::open(&state)
            .unwrap()
            .request_count()
            .unwrap(),
        1
    );
}

#[test]
fn same_request_id_with_a_changed_payload_or_command_conflicts() {
    let temp = tempfile::tempdir().unwrap();
    let (_, store) = open_store(&temp);
    let first = store
        .handle(&parsed_request(), ControllerFault::None)
        .unwrap();

    let mut body = valid_request_value();
    body["body"] = json!({"prompt": "different frozen snapshot"});
    let changed_body = parse_request(&serde_json::to_vec(&body).unwrap()).unwrap();
    assert_eq!(
        public_code(
            store
                .handle(&changed_body, ControllerFault::None)
                .unwrap_err()
        ),
        "CONTROLLER_REQUEST_CONFLICT"
    );

    let mut command = valid_request_value();
    command["command"] = json!("task.close");
    command["body"] = json!({});
    let changed_command = parse_request(&serde_json::to_vec(&command).unwrap()).unwrap();
    // Same ID as the first request, different command identity.
    assert_eq!(changed_command.request_id(), first.request_id());
    assert_eq!(
        public_code(
            store
                .handle(&changed_command, ControllerFault::None)
                .unwrap_err()
        ),
        "CONTROLLER_REQUEST_CONFLICT"
    );

    let loaded = store.load(first.request_id()).unwrap().unwrap();
    assert_eq!(loaded.task_id(), first.task_id());
    assert_eq!(loaded.phase(), RequestPhase::Acked);
    assert_eq!(store.request_count().unwrap(), 1);
}

#[test]
fn restart_after_durable_publish_resumes_the_same_ids_without_submit_with_ids() {
    let temp = tempfile::tempdir().unwrap();
    let (state, store) = open_store(&temp);
    let request = parsed_request();
    let published = store
        .handle(&request, ControllerFault::StopAfterPublish)
        .unwrap();
    assert_eq!(published.status(), "published");
    let loaded = store.load(request.request_id()).unwrap().unwrap();
    assert_eq!(loaded.phase(), RequestPhase::Published);
    assert_eq!(loaded.task_id(), published.task_id());

    drop(store);
    let store = ControllerStore::open(&state).unwrap();
    let resumed = store.handle(&request, ControllerFault::None).unwrap();
    assert_eq!(resumed.status(), "acked");
    assert_eq!(resumed.task_id(), published.task_id());
    assert_eq!(resumed.turn_id(), published.turn_id());
    assert_eq!(resumed.created_at_millis(), published.created_at_millis());
    assert_eq!(resumed.payload_sha256(), published.payload_sha256());
}

#[test]
fn restart_after_ack_returns_the_same_ack() {
    let temp = tempfile::tempdir().unwrap();
    let (state, store) = open_store(&temp);
    let request = parsed_request();
    let first = store.handle(&request, ControllerFault::None).unwrap();
    drop(store);
    let store = ControllerStore::open(&state).unwrap();
    let second = store.handle(&request, ControllerFault::None).unwrap();
    assert_eq!(first.request_id(), second.request_id());
    assert_eq!(first.task_id(), second.task_id());
    assert_eq!(first.turn_id(), second.turn_id());
    assert_eq!(first.payload_sha256(), second.payload_sha256());
    assert_eq!(first.created_at_millis(), second.created_at_millis());
    assert_eq!(first.status(), "acked");
    assert_eq!(second.status(), "acked");
}

#[test]
fn crash_before_ack_exchange_keeps_the_published_record_for_resume() {
    let temp = tempfile::tempdir().unwrap();
    let (state, store) = open_store(&temp);
    let request = parsed_request();
    assert!(
        store
            .handle(&request, ControllerFault::CrashBeforeAckExchange)
            .is_err()
    );
    let loaded = store.load(request.request_id()).unwrap().unwrap();
    assert_eq!(loaded.phase(), RequestPhase::Published);

    drop(store);
    let store = ControllerStore::open(&state).unwrap();
    let resumed = store.resume_incomplete().unwrap();
    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0].status(), "acked");
    assert_eq!(resumed[0].task_id(), loaded.task_id());
    assert_eq!(resumed[0].turn_id(), loaded.turn_id());
    assert_eq!(
        store.load(request.request_id()).unwrap().unwrap().phase(),
        RequestPhase::Acked
    );
}

#[test]
fn near_limit_body_is_stored_and_reread_and_oversize_stored_does_not_write() {
    let prefix = format!(
        r#"{{"protocol_version":{version},"request_id":"{REQUEST_ID}","command":"checkpoint.submit","body":{{"blob":""#,
        version = PROTOCOL_VERSION
    );
    let suffix = r#""}}"#;
    let pad = MAX_FRAME_BYTES - prefix.len() - suffix.len();
    let near = format!("{prefix}{}{suffix}", "x".repeat(pad));
    assert_eq!(near.len(), MAX_FRAME_BYTES);
    let request = parse_request(near.as_bytes()).unwrap();
    let frame = encode_frame(near.as_bytes()).unwrap();

    let temp = tempfile::tempdir().unwrap();
    let (state, store) = open_store(&temp);
    let ack = store.handle(&request, ControllerFault::None).unwrap();
    let loaded = store.load(request.request_id()).unwrap().unwrap();
    assert_eq!(loaded.payload_sha256(), ack.payload_sha256());
    assert_eq!(loaded.body()["blob"].as_str().unwrap().len(), pad);

    drop(store);
    let store = ControllerStore::open(&state).unwrap();
    let retry = store.handle(&request, ControllerFault::None).unwrap();
    assert_eq!(retry.task_id(), ack.task_id());

    let mut stdout = Vec::new();
    let served = serve_rpc(
        &state,
        &mut Cursor::new(frame),
        &mut stdout,
        ControllerFault::None,
    )
    .unwrap();
    assert_eq!(served.task_id(), ack.task_id());

    let huge = format!(
        r#"{{"protocol_version":{version},"request_id":"018f0f4a6b5c7d8e9f00112233445577","command":"checkpoint.submit","body":{{"blob":"{blob}"}}}}"#,
        version = PROTOCOL_VERSION,
        blob = "y".repeat(MAX_STORED_REQUEST_BYTES)
    );
    let oversized = parse_request(huge.as_bytes()).unwrap();
    let before = store.request_count().unwrap();
    assert!(store.handle(&oversized, ControllerFault::None).is_err());
    assert_eq!(store.request_count().unwrap(), before);
    assert!(
        store
            .load("018f0f4a6b5c7d8e9f00112233445577")
            .unwrap()
            .is_none()
    );
}

/// Kill and wait any still-live child. `std::process::Child` drop does not
/// reap, so a panic between spawn and wait would leak a controller daemon
/// into the shared Cargo queue.
struct OwnedChild {
    child: Option<Child>,
}

impl OwnedChild {
    fn spawn(command: &mut Command) -> Self {
        Self {
            child: Some(command.spawn().unwrap()),
        }
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child
            .as_mut()
            .expect("controller child already reaped")
    }

    fn take_stdout(&mut self) -> std::process::ChildStdout {
        self.child_mut().stdout.take().expect("stdout pipe")
    }

    fn take_stderr(&mut self) -> std::process::ChildStderr {
        self.child_mut().stderr.take().expect("stderr pipe")
    }

    fn wait_timeout(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            let status = {
                let child = self.child.as_mut()?;
                child.try_wait().unwrap()
            };
            if let Some(status) = status {
                self.child = None;
                return Some(status);
            }
            if Instant::now() > deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn kill_and_reap(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        self.kill_and_reap();
    }
}

fn spawn_controller_run(home: &Path, state_home: &Path, process_home: &Path) -> OwnedChild {
    let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
    command
        .args(["controller", "run"])
        .env("HOME", home)
        .env("XDG_STATE_HOME", state_home)
        .env("XDG_CONFIG_HOME", process_home.join("config"))
        .env("XDG_CACHE_HOME", process_home.join("cache"))
        .env("XDG_DATA_HOME", process_home.join("data"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    OwnedChild::spawn(&mut command)
}

#[test]
fn two_leaders_are_excluded_in_library_and_process() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let state = root.join("mac-worker-controller");
    let first = ControllerLeader::acquire(&state).unwrap();
    assert_eq!(
        public_code(
            ControllerLeader::acquire(&state)
                .err()
                .expect("second in-process leader must be rejected")
        ),
        "CONTROLLER_LOCK_HELD"
    );
    ClientStateStore::open(&root.join("mac-worker")).unwrap();
    drop(first);

    let process_home = tempfile::tempdir().unwrap();
    let process_root = process_home.path().canonicalize().unwrap();
    let home = process_root.join("home");
    let state_home = process_root.join("state");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&state_home).unwrap();

    let mut leader = spawn_controller_run(&home, &state_home, &process_root);
    let stdout = leader.take_stdout();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut line = String::new();
        let _ = BufReader::new(stdout).read_line(&mut line);
        let _ = tx.send(line);
    });
    let line = rx
        .recv_timeout(Duration::from_secs(15))
        .expect("controller run should print leader acquired");
    assert!(
        line.contains("controller leader acquired"),
        "unexpected leader stdout: {line:?}"
    );

    let mut rejected = spawn_controller_run(&home, &state_home, &process_root);
    let mut stderr_pipe = rejected.take_stderr();
    let (err_tx, err_rx) = mpsc::channel();
    thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stderr_pipe.read_to_end(&mut buf);
        let _ = err_tx.send(buf);
    });
    let status = rejected
        .wait_timeout(Duration::from_secs(15))
        .unwrap_or_else(|| {
            rejected.kill_and_reap();
            panic!("second controller run did not exit");
        });
    let stderr = err_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap_or_else(|_| {
            rejected.kill_and_reap();
            panic!("second controller run stderr closed without a line");
        });
    let stderr = String::from_utf8_lossy(&stderr);
    assert!(!status.success());
    assert!(
        stderr.contains("CONTROLLER_LOCK_HELD"),
        "second leader stderr: {stderr}"
    );
}

#[test]
fn operation_envelope_validates_id_conflicts_and_stays_read_only_on_load() {
    let temp = tempfile::tempdir().unwrap();
    let missing = temp.path().join("missing-cache");
    assert_eq!(
        public_code(load_operation_envelope(&missing, "not-a-uuid").unwrap_err()),
        "INVALID_REQUEST"
    );
    assert!(!missing.exists());
    assert!(
        load_operation_envelope(&missing, REQUEST_ID)
            .unwrap()
            .is_none()
    );
    assert!(!missing.exists());

    let cache = temp.path().join("controller-cache");
    let request = parsed_request();
    let first = persist_operation_envelope(&cache, &request).unwrap();

    let mut changed = valid_request_value();
    changed["body"] = json!({"prompt": "changed"});
    let changed = parse_request(&serde_json::to_vec(&changed).unwrap()).unwrap();
    assert_eq!(
        public_code(persist_operation_envelope(&cache, &changed).unwrap_err()),
        "CONTROLLER_REQUEST_CONFLICT"
    );
    let loaded = load_operation_envelope(&cache, REQUEST_ID)
        .unwrap()
        .unwrap();
    assert_eq!(loaded.payload_sha256(), first.payload_sha256());
    assert_eq!(loaded.command(), first.command());

    thread::scope(|scope| {
        let left = scope.spawn(|| persist_operation_envelope(&cache, &request));
        let right = scope.spawn(|| persist_operation_envelope(&cache, &request));
        let left = left.join().unwrap().unwrap();
        let right = right.join().unwrap().unwrap();
        assert_eq!(left.request_id(), right.request_id());
        assert_eq!(left.payload_sha256(), right.payload_sha256());
        assert_eq!(left.request_id(), first.request_id());
    });
}

#[test]
fn ssh_controller_rpc_command_is_the_fixed_remote_binary_helper() {
    let contents = r#"
version = 1
[controller]
enabled = true
ssh = "user@always-on-host"
"#;
    let config = Config::parse(contents).unwrap();
    let request = controller_rpc_ssh_request(&config.controller).unwrap();
    assert_eq!(
        request.args.last().unwrap(),
        "~/.local/bin/worker host controller-rpc"
    );
}

#[test]
fn host_controller_rpc_stdio_acks_and_duplicate_keys_write_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let (runtime, paths) = isolated_paths(&temp);
    let frame = frame_json(&valid_request_value());
    let cli = host_controller_rpc_cli();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        cli,
        &SystemProcessRunner,
        &runtime,
        &mut Cursor::new(frame),
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0, "stderr={}", String::from_utf8_lossy(&stderr));
    let payload = decode_frame(&stdout).unwrap();
    let ack: Value = serde_json::from_slice(payload).unwrap();
    assert_eq!(ack["status"], "acked");
    assert_eq!(ack["request_id"], REQUEST_ID);
    assert!(
        paths
            .controller_state_root()
            .join(format!("req-{REQUEST_ID}.json"))
            .exists()
    );

    let duplicate = frame_raw(&duplicate_request_payloads()[0]);
    let fresh = tempfile::tempdir().unwrap();
    let (runtime, paths) = isolated_paths(&fresh);
    let cli = host_controller_rpc_cli();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        cli,
        &SystemProcessRunner,
        &runtime,
        &mut Cursor::new(duplicate),
        &mut stdout,
        &mut stderr,
    );
    assert_ne!(exit, 0);
    let payload = decode_frame(&stdout).unwrap();
    let error: HostControlError = serde_json::from_slice(payload).unwrap();
    assert_eq!(error.error().code(), "INVALID_REQUEST");
    assert!(!paths.controller_state_root().exists());
}
