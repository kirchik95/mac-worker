#[path = "support/fake_herdr.rs"]
mod fake_herdr;

use std::{
    ffi::OsString,
    os::unix::net::UnixListener,
    path::Path,
    time::{Duration, Instant},
};

use fake_herdr::{FakeHerdr, Reply};
use mac_worker::herdr::{
    AgentState, DEFAULT_SOCKET_RELATIVE, HerdrClient, HerdrError, HerdrSocket, NotificationSound,
    PaneMetadata, SOCKET_ENV_NAME, SOURCE,
};
use serde_json::{Value, json};

fn client_for(server: &FakeHerdr) -> HerdrClient {
    HerdrClient::new(HerdrSocket::at(server.path()))
}

fn temp_home() -> tempfile::TempDir {
    tempfile::tempdir().expect("temp home")
}

#[test]
fn a_request_is_one_json_line_and_its_result_comes_back() {
    let home = temp_home();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply("ping", Reply::Result(json!({ "type": "pong" })));
    let client = client_for(&server);

    let result = client.request("ping", json!({})).unwrap();

    assert_eq!(result, json!({ "type": "pong" }));
    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request["method"], "ping");
    assert_eq!(request["params"], json!({}));
    let id = request["id"].as_str().expect("request id is a string");
    assert!(id.starts_with("mac-worker:"), "{id}");
    assert_eq!(id.split(':').count(), 3, "{id}");
}

#[test]
fn server_errors_are_typed() {
    let home = temp_home();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply(
        "pane.report_agent",
        Reply::Error {
            code: "pane_not_found".into(),
            message: "pane w9:p9 not found".into(),
        },
    );
    let client = client_for(&server);

    let error = client
        .pane_report_agent("w9:p9", "codex", AgentState::Working, None)
        .unwrap_err();

    assert_eq!(error.kind(), "server");
    assert_eq!(
        error.to_string(),
        "herdr error pane_not_found: pane w9:p9 not found"
    );
    match error {
        HerdrError::Server { code, message } => {
            assert_eq!(code, "pane_not_found");
            assert_eq!(message, "pane w9:p9 not found");
        }
        other => panic!("expected a server error, got {other:?}"),
    }
}

#[test]
fn an_absent_socket_is_classified_at_once() {
    let home = temp_home();
    let client = HerdrClient::new(HerdrSocket::default_for_home(home.path()));

    let started = Instant::now();
    let error = client.ping().unwrap_err();

    assert!(matches!(error, HerdrError::Absent), "{error:?}");
    assert_eq!(error.kind(), "absent");
    assert!(started.elapsed() < Duration::from_millis(500));
}

#[test]
fn a_socket_nobody_accepts_on_is_refused() {
    let home = temp_home();
    let path = home.path().join("stale.sock");
    drop(UnixListener::bind(&path).expect("bind then abandon"));
    let client = HerdrClient::new(HerdrSocket::at(&path));

    let started = Instant::now();
    let error = client.ping().unwrap_err();

    assert!(matches!(error, HerdrError::Refused(_)), "{error:?}");
    assert!(started.elapsed() < Duration::from_millis(500));
}

#[test]
fn a_silent_server_times_out_within_the_response_deadline() {
    let home = temp_home();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply("ping", Reply::Silence);
    let client = HerdrClient::with_deadlines(
        HerdrSocket::at(server.path()),
        Duration::from_millis(500),
        Duration::from_millis(300),
    );

    let started = Instant::now();
    let error = client.ping().unwrap_err();

    assert!(matches!(error, HerdrError::Timeout), "{error:?}");
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(250), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
}

#[test]
fn a_slow_answer_inside_the_deadline_is_accepted() {
    let home = temp_home();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply(
        "ping",
        Reply::Stall(Duration::from_millis(100), json!({ "type": "pong" })),
    );
    let client = client_for(&server);

    assert!(client.ping().is_ok());
}

#[test]
fn an_answer_to_another_request_is_a_protocol_error() {
    let home = temp_home();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply(
        "ping",
        Reply::Raw(json!({ "id": "somebody-else", "result": { "type": "pong" } })),
    );
    let client = client_for(&server);

    let error = client.ping().unwrap_err();

    assert!(matches!(error, HerdrError::Protocol(_)), "{error:?}");
    assert_eq!(error.kind(), "protocol");
}

#[test]
fn every_call_opens_and_closes_its_own_connection() {
    let home = temp_home();
    let server = FakeHerdr::start_in_home(home.path());
    let client = client_for(&server);

    client.ping().unwrap();
    client.tab_close("w1:t1").unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    while server.connections_closed_after_reply().len() < 2 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    let closed = server.connections_closed_after_reply();
    assert_eq!(closed, vec![true, true], "{closed:?}");
    assert_eq!(server.requests().len(), 2);
}

#[test]
fn the_socket_path_honours_the_environment_then_the_home() {
    let home = Path::new("/Users/someone");
    let from_env = HerdrSocket::from_env_or_home(
        |key| (key == SOCKET_ENV_NAME).then(|| OsString::from("/tmp/session/herdr.sock")),
        home,
    );
    assert_eq!(from_env.path(), Path::new("/tmp/session/herdr.sock"));

    let empty_env =
        HerdrSocket::from_env_or_home(|key| (key == SOCKET_ENV_NAME).then(OsString::new), home);
    assert_eq!(empty_env.path(), home.join(DEFAULT_SOCKET_RELATIVE));

    let no_env = HerdrSocket::from_env_or_home(|_| None, home);
    assert_eq!(no_env, HerdrSocket::default_for_home(home));
    assert_eq!(
        no_env.path(),
        Path::new("/Users/someone/.config/herdr/herdr.sock")
    );
}

#[test]
fn typed_results_are_parsed_from_the_shapes_herdr_answers_with() {
    let home = temp_home();
    let server = FakeHerdr::start_in_home(home.path());
    server.reply(
        "workspace.list",
        Reply::Result(json!({
            "type": "workspace_list",
            "workspaces": [
                { "workspace_id": "w2", "label": "lms", "focused": true, "agent_status": "unknown" },
                { "workspace_id": "w3", "label": "mac-worker", "focused": false, "agent_status": "working" }
            ]
        })),
    );
    server.reply(
        "workspace.create",
        Reply::Result(json!({
            "type": "workspace_created",
            "workspace": { "workspace_id": "w4", "label": "mac-worker", "number": 3 },
            "tab": { "tab_id": "w4:t1", "label": "1", "workspace_id": "w4" },
            "root_pane": { "pane_id": "w4:p1", "tab_id": "w4:t1", "workspace_id": "w4", "agent_status": "unknown" }
        })),
    );
    server.reply(
        "tab.create",
        Reply::Result(json!({
            "type": "tab_created",
            "tab": { "tab_id": "w4:t2", "label": "task abc123def456 · turn 1", "workspace_id": "w4" },
            "root_pane": { "pane_id": "w4:p2", "tab_id": "w4:t2", "workspace_id": "w4" }
        })),
    );
    server.reply(
        "tab.list",
        Reply::Result(json!({
            "type": "tab_list",
            "tabs": [
                { "tab_id": "w4:t1", "label": "1", "workspace_id": "w4" },
                { "tab_id": "w4:t2", "label": "task abc123def456 · turn 1", "workspace_id": "w4" }
            ]
        })),
    );
    server.reply(
        "pane.process_info",
        Reply::Result(json!({
            "type": "pane_process_info",
            "process_info": {
                "pane_id": "w4:p2",
                "shell_pid": 12717,
                "foreground_process_group_id": 12717,
                "foreground_processes": [
                    { "argv": ["-zsh"], "argv0": "zsh", "cmdline": "-zsh", "cwd": "/Users/kirchik", "name": "zsh", "pid": 12717 }
                ]
            }
        })),
    );
    server.reply(
        "pane.process_info",
        Reply::Result(json!({
            "type": "pane_process_info",
            "process_info": {
                "pane_id": "w4:p2",
                "shell_pid": 12717,
                "foreground_processes": [
                    { "name": "zsh", "pid": 12717 },
                    { "name": "worker", "pid": 12800 }
                ]
            }
        })),
    );
    let client = client_for(&server);

    let workspaces = client.workspace_list().unwrap();
    assert_eq!(workspaces.len(), 2);
    assert_eq!(workspaces[1].workspace_id, "w3");
    assert_eq!(workspaces[1].label.as_deref(), Some("mac-worker"));

    let created = client.workspace_create("mac-worker", None).unwrap();
    assert_eq!(
        (
            created.workspace_id.as_str(),
            created.tab_id.as_str(),
            created.pane_id.as_str()
        ),
        ("w4", "w4:t1", "w4:p1")
    );

    let tab = client
        .tab_create("w4", "task abc123def456 · turn 1", None)
        .unwrap();
    assert_eq!(
        (
            tab.workspace_id.as_str(),
            tab.tab_id.as_str(),
            tab.pane_id.as_str()
        ),
        ("w4", "w4:t2", "w4:p2")
    );

    let tabs = client.tab_list("w4").unwrap();
    assert_eq!(tabs.len(), 2);
    assert_eq!(tabs[1].label.as_deref(), Some("task abc123def456 · turn 1"));

    let idle = client.pane_process_info("w4:p2").unwrap();
    assert!(idle.is_idle_shell());
    let busy = client.pane_process_info("w4:p2").unwrap();
    assert!(!busy.is_idle_shell());
    assert_eq!(busy.foreground[1].name, "worker");
}

#[test]
fn typed_calls_match_the_captured_herdr_schema() {
    let home = temp_home();
    let server = FakeHerdr::start_in_home(home.path());
    let client = client_for(&server);
    let cwd = Path::new("/Users/someone");
    let mut metadata = PaneMetadata {
        agent: Some("codex".into()),
        title: Some("task abc123def456 · fix the lock".into()),
        display_agent: Some("mac-worker".into()),
        ttl_ms: Some(3_600_000),
        ..PaneMetadata::default()
    };
    metadata
        .state_labels
        .insert("working".into(), "turn 1".into());
    metadata.tokens.insert("task".into(), "abc123def456".into());
    metadata.tokens.insert("turn".into(), "1".into());

    client.ping().unwrap();
    client.workspace_list().unwrap_or_default();
    let _ = client.workspace_create("mac-worker", Some(cwd));
    let _ = client.tab_list("w1");
    let _ = client.tab_create("w1", "task abc123def456 · turn 1", None);
    client.tab_close("w1:t2").unwrap();
    let _ = client.pane_process_info("w1:p2");
    client
        .pane_send_input("w1:p2", "exec worker host follow-turn a b c", &["enter"])
        .unwrap();
    client
        .pane_report_agent(
            "w1:p2",
            "codex",
            AgentState::Blocked,
            Some("which database?"),
        )
        .unwrap();
    client.pane_report_metadata("w1:p2", &metadata).unwrap();
    client.pane_release_agent("w1:p2", "codex").unwrap();
    client
        .notification_show(
            "task abc123def456: done",
            Some("slept"),
            NotificationSound::Done,
        )
        .unwrap();

    let schema: Value = serde_json::from_str(include_str!("fixtures/herdr/schema-subset.json"))
        .expect("schema fixture");
    let methods = schema["methods"].as_object().expect("methods");
    let requests = server.requests();
    assert_eq!(requests.len(), 12, "every typed call reached the socket");
    for request in &requests {
        let method = request["method"].as_str().expect("method");
        let params = methods
            .get(method)
            .unwrap_or_else(|| panic!("{method} is not in the captured schema"));
        validate(method, &request["params"], params);
    }

    let report = &server.requests_for("pane.report_agent")[0]["params"];
    assert_eq!(report["source"], SOURCE);
    assert_eq!(report["state"], "blocked");
    assert_eq!(report["message"], "which database?");
    assert!(report["seq"].as_u64().unwrap() > 0);
    let metadata_params = &server.requests_for("pane.report_metadata")[0]["params"];
    assert_eq!(metadata_params["tokens"]["task"], "abc123def456");
    assert_eq!(metadata_params["state_labels"]["working"], "turn 1");
    let input = &server.requests_for("pane.send_input")[0]["params"];
    assert_eq!(input["keys"], json!(["enter"]));
    let created = &server.requests_for("tab.create")[0]["params"];
    assert_eq!(created["focus"], false);
}

/// The smallest check that keeps the client honest against herdr's own
/// schema: required keys present, no key herdr does not know, scalar
/// types and enumerations respected.
fn validate(method: &str, params: &Value, schema: &Value) {
    let object = params
        .as_object()
        .unwrap_or_else(|| panic!("{method}: params must be an object"));
    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        for key in required {
            let key = key.as_str().unwrap();
            assert!(
                object.contains_key(key),
                "{method}: missing required `{key}`"
            );
        }
    }
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        assert!(object.is_empty(), "{method}: schema declares no parameters");
        return;
    };
    for (key, value) in object {
        let property = properties
            .get(key)
            .unwrap_or_else(|| panic!("{method}: herdr does not know `{key}`"));
        check_type(method, key, value, property);
    }
}

fn check_type(method: &str, key: &str, value: &Value, property: &Value) {
    if let Some(options) = property.get("enum").and_then(Value::as_array) {
        assert!(
            options.contains(value),
            "{method}.{key}: {value} not in {options:?}"
        );
        return;
    }
    let Some(kind) = property.get("type") else {
        return;
    };
    let kinds: Vec<&str> = match kind {
        Value::String(one) => vec![one.as_str()],
        Value::Array(many) => many.iter().filter_map(Value::as_str).collect(),
        _ => return,
    };
    let ok = kinds.iter().any(|kind| match *kind {
        "string" => value.is_string(),
        "boolean" => value.is_boolean(),
        "integer" => value.is_u64() || value.is_i64(),
        "number" => value.is_number(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        "null" => value.is_null(),
        _ => true,
    });
    assert!(ok, "{method}.{key}: {value} is not one of {kinds:?}");
    if let (Some(items), Some(elements)) = (property.get("items"), value.as_array()) {
        for element in elements {
            check_type(method, key, element, items);
        }
    }
}
