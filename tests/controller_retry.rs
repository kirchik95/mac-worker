use clap::Parser;
use mac_worker::{
    RuntimeContext,
    cli::Cli,
    config::Config,
    controller::{
        ControllerRequest, canonical_request_sha256, decode_frame, encode_json_frame,
        load_operation_envelope, parse_request, persist_operation_envelope,
    },
    error::WorkerError,
    job::HostControlError,
    paths::PathLayout,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::PROTOCOL_VERSION,
    run_with_io_in_context,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    os::unix::process::ExitStatusExt,
    process::ExitStatus,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

const ID: &str = "018f0f4a6b5c7d8e9f00112233445566";
const TASK: &str = "018f0f4a6b5c7d8e9f00112233445577";
const TURN: &str = "018f0f4a6b5c7d8e9f00112233445588";
const RUN: &str = "018f0f4a6b5c7d8e9f00112233445599";

struct Fixture {
    _temp: tempfile::TempDir,
    paths: PathLayout,
    runtime: RuntimeContext,
}
impl Fixture {
    fn new(enabled: bool) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let home = root.join("home");
        fs::create_dir(&home).unwrap();
        let env: BTreeMap<OsString, OsString> = BTreeMap::from([
            ("HOME".into(), home.clone().into_os_string()),
            (
                "XDG_CONFIG_HOME".into(),
                root.join("config").into_os_string(),
            ),
            ("XDG_STATE_HOME".into(), root.join("state").into_os_string()),
            ("XDG_CACHE_HOME".into(), root.join("cache").into_os_string()),
            ("XDG_DATA_HOME".into(), root.join("data").into_os_string()),
        ]);
        let paths = PathLayout::discover(None, &env, &home).unwrap();
        fs::create_dir_all(paths.config.parent().unwrap()).unwrap();
        let contents = format!(
            "version = 1\n[controller]\nenabled = {enabled}\nssh = 'fakecontroller'\n[[workers]]\nname = 'fixture'\nssh = 'fakeworker'\nslots = 1\n"
        );
        Config::parse(&contents).unwrap();
        fs::write(&paths.config, contents).unwrap();
        let runtime = RuntimeContext::isolated(env, home, root);
        Self {
            _temp: temp,
            paths,
            runtime,
        }
    }
    fn run(&self, args: &[&str], runner: &dyn ProcessRunner) -> (u8, String, String) {
        let cli = Cli::try_parse_from(std::iter::once("worker").chain(args.iter().copied()))
            .expect("controller recovery grammar");
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let exit = run_with_io_in_context(cli, runner, &self.runtime, &mut stdout, &mut stderr);
        (
            exit,
            String::from_utf8(stdout).unwrap(),
            String::from_utf8(stderr).unwrap(),
        )
    }
    fn rewrite(&self, update: impl FnOnce(&mut Value)) {
        let path = self
            .paths
            .controller_cache_root()
            .join(format!("op-{ID}.json"));
        let mut value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        update(&mut value);
        fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    }
}
fn request() -> ControllerRequest {
    parse_request(&serde_json::to_vec(&json!({"protocol_version": PROTOCOL_VERSION, "request_id": ID, "command": "task.batch", "body": {"run_id": RUN, "tasks": [{"task_id": TASK, "turn_id": TURN, "prompt": "PRIVATE PROMPT"}]}})).unwrap()).unwrap()
}
struct Reply {
    reject: bool,
    frames: Mutex<Vec<Vec<u8>>>,
}
impl Reply {
    fn new(reject: bool) -> Self {
        Self {
            reject,
            frames: Mutex::new(Vec::new()),
        }
    }
}
impl ProcessRunner for Reply {
    fn run(&self, process: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let frame = process.stdin.as_ref().unwrap();
        self.frames.lock().unwrap().push(frame.clone());
        let parsed = parse_request(decode_frame(frame).unwrap()).unwrap();
        let value = if self.reject {
            serde_json::to_value(
                HostControlError::with_category(
                    "CAPACITY_BUSY",
                    "PRIVATE REMOTE DETAIL",
                    "capacity",
                )
                .unwrap(),
            )
            .unwrap()
        } else {
            json!({"protocol_version": PROTOCOL_VERSION, "status": "acked", "request_id": parsed.request_id(), "payload_sha256": parsed.payload_sha256(), "created_at_millis": 1, "result": {"run_id": RUN, "task_ids": [TASK], "turn_id": TURN}})
        };
        Ok(ProcessResult {
            status: ExitStatus::from_raw(if self.reject { 75 << 8 } else { 0 }),
            stdout: encode_json_frame(&value).unwrap(),
            stderr: Vec::new(),
        })
    }
}
struct NoTransport;
impl ProcessRunner for NoTransport {
    fn run(&self, _: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        panic!("command must remain local")
    }
}

#[test]
fn pending_lists_safe_identifiers_without_prompts_and_honors_all() {
    let fixture = Fixture::new(true);
    persist_operation_envelope(&fixture.paths.controller_cache_root(), &request()).unwrap();
    let (exit, stdout, stderr) = fixture.run(&["controller", "pending", "--json"], &NoTransport);
    assert_eq!(exit, 0, "{stderr}");
    let pending: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(pending["pending"][0]["request_id"], ID);
    assert_eq!(pending["pending"][0]["command"], "task.batch");
    assert_eq!(pending["pending"][0]["task_ids"], json!([TASK]));
    assert_eq!(pending["pending"][0]["turn_ids"], json!([TURN]));
    assert_eq!(pending["pending"][0]["run_ids"], json!([RUN]));
    assert!(pending["pending"][0]["age_millis"].is_u64());
    assert!(!stdout.contains("PRIVATE"));
    let (_, text, _) = fixture.run(&["controller", "pending"], &NoTransport);
    for value in [ID, "task.batch", TASK, TURN, RUN] {
        assert!(text.contains(value), "{text}");
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    fixture.rewrite(|value| value["created_at_millis"] = json!(now - 8 * 24 * 60 * 60 * 1000));
    let (_, stdout, _) = fixture.run(&["controller", "pending", "--json"], &NoTransport);
    assert_eq!(
        serde_json::from_str::<Value>(&stdout).unwrap(),
        json!({"pending": [], "unreadable": []})
    );
    let (_, stdout, _) = fixture.run(&["controller", "pending", "--all", "--json"], &NoTransport);
    assert_eq!(
        serde_json::from_str::<Value>(&stdout).unwrap()["pending"][0]["request_id"],
        ID
    );
}

#[test]
fn first_controller_mode_run_writes_a_private_adoption_marker_once() {
    use std::os::unix::fs::MetadataExt;

    let fixture = Fixture::new(true);
    let cache = fixture.paths.controller_cache_root();
    persist_operation_envelope(&cache, &request()).unwrap();
    let marker = cache.join("operations-adopted-v1.json");
    // Model an upgrade: keep a recent legacy envelope, with no adoption marker.
    if marker.exists() {
        fs::remove_file(&marker).unwrap();
    }
    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    fixture.rewrite(|value| {
        value["created_at_millis"] = json!(before - 24 * 60 * 60 * 1000);
        value.as_object_mut().unwrap().remove("settled_at_millis");
        value.as_object_mut().unwrap().remove("outcome");
    });
    let (exit, _, stderr) = fixture.run(
        &["controller", "retry", "00000000000000000000000000000000"],
        &NoTransport,
    );
    assert_eq!(exit, 64, "{stderr}");
    assert!(
        marker.is_file(),
        "the first run must adopt before a recovery command returns"
    );
    let marker_bytes = fs::read(&marker).unwrap();
    let metadata = fs::metadata(&marker).unwrap();
    assert_eq!(metadata.mode() & 0o777, 0o600);
    let adopted: Value = serde_json::from_slice(&marker_bytes).unwrap();
    assert!(adopted["created_at_millis"].as_u64().unwrap() >= before);
    let (exit, stdout, stderr) = fixture.run(&["controller", "pending", "--json"], &NoTransport);
    assert_eq!(exit, 0, "{stderr}");
    assert_eq!(
        serde_json::from_str::<Value>(&stdout).unwrap()["pending"],
        json!([])
    );
    let (exit, stdout, stderr) =
        fixture.run(&["controller", "pending", "--all", "--json"], &NoTransport);
    assert_eq!(exit, 0, "{stderr}");
    assert_eq!(
        serde_json::from_str::<Value>(&stdout).unwrap()["pending"][0]["request_id"],
        ID
    );
    let new_request = parse_request(
        &serde_json::to_vec(&json!({
            "protocol_version": PROTOCOL_VERSION,
            "request_id": "00000000000000000000000000000001",
            "command": request().command(), "body": request().body(),
        }))
        .unwrap(),
    )
    .unwrap();
    persist_operation_envelope(&cache, &new_request).unwrap();
    let (exit, stdout, stderr) = fixture.run(&["controller", "pending", "--json"], &NoTransport);
    assert_eq!(exit, 0, "{stderr}");
    let pending: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(pending["pending"].as_array().unwrap().len(), 1);
    assert_eq!(
        pending["pending"][0]["request_id"],
        new_request.request_id()
    );
    assert_eq!(fs::read(&marker).unwrap(), marker_bytes);
    assert_eq!(fs::metadata(marker).unwrap().ino(), metadata.ino());
}

#[test]
fn pending_lists_legacy_requests_and_warns_once_when_adoption_marker_is_invalid() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    for kind in ["invalid", "permissions", "directory", "symlink"] {
        let fixture = Fixture::new(true);
        let cache = fixture.paths.controller_cache_root();
        persist_operation_envelope(&cache, &request()).unwrap();
        fixture.rewrite(|value| {
            value["created_at_millis"] = json!(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
                    - 86_400_000
            );
            value.as_object_mut().unwrap().remove("settled_at_millis");
            value.as_object_mut().unwrap().remove("outcome");
        });
        let marker = cache.join("operations-adopted-v1.json");
        match kind {
            "invalid" => fs::write(&marker, b"PRIVATE invalid marker").unwrap(),
            "permissions" => {
                fs::set_permissions(&marker, fs::Permissions::from_mode(0o644)).unwrap()
            }
            "directory" => {
                fs::remove_file(&marker).unwrap();
                fs::create_dir(&marker).unwrap();
            }
            "symlink" => {
                fs::remove_file(&marker).unwrap();
                symlink(fixture._temp.path().join("absent"), &marker).unwrap();
            }
            _ => unreachable!(),
        }
        for args in [
            vec!["controller", "pending", "--json"],
            vec!["controller", "pending", "--all", "--json"],
        ] {
            let (exit, stdout, stderr) = fixture.run(&args, &NoTransport);
            assert_eq!(exit, 0, "{kind}: {stderr}");
            assert_eq!(
                stderr,
                "controller request adoption marker could not be read; including legacy requests\n"
            );
            let report: Value = serde_json::from_str(&stdout).unwrap();
            assert_eq!(report["pending"].as_array().unwrap().len(), 1);
            assert_eq!(report["pending"][0]["request_id"], ID);
            assert_eq!(report["unreadable"], json!([]));
            assert!(!stdout.contains("PRIVATE"));
        }
        if kind == "invalid" {
            assert_eq!(fs::read(marker).unwrap(), b"PRIVATE invalid marker");
        }
    }
}

#[test]
fn retry_settles_normally_with_an_invalid_adoption_marker() {
    let fixture = Fixture::new(true);
    let cache = fixture.paths.controller_cache_root();
    persist_operation_envelope(&cache, &request()).unwrap();
    fs::write(
        cache.join("operations-adopted-v1.json"),
        b"PRIVATE invalid marker",
    )
    .unwrap();
    let runner = Reply::new(false);
    let (exit, stdout, stderr) = fixture.run(&["controller", "retry", ID, "--json"], &runner);
    assert_eq!(exit, 0, "{stderr}");
    assert!(stderr.is_empty());
    assert_eq!(
        serde_json::from_str::<Value>(&stdout).unwrap()["run_id"],
        RUN
    );
    assert_eq!(
        load_operation_envelope(&cache, ID)
            .unwrap()
            .unwrap()
            .outcome(),
        Some(&mac_worker::controller::OperationOutcome::Acknowledged)
    );
}

#[test]
fn pending_skips_unreadable_envelopes_and_reports_them_without_private_details() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let fixture = Fixture::new(true);
    let cache = fixture.paths.controller_cache_root();
    let envelope = persist_operation_envelope(&cache, &request()).unwrap();
    let valid = serde_json::to_value(&envelope).unwrap();
    let path = |id| cache.join(format!("op-{id:032x}.json"));
    for id in 1..=5 {
        let mut value = valid.clone();
        value["request_id"] = json!(format!("{id:032x}"));
        if id == 2 {
            value["payload_sha256"] = json!(
                canonical_request_sha256(
                    PROTOCOL_VERSION - 1,
                    request().command(),
                    request().body()
                )
                .unwrap()
            );
        }
        fs::write(path(id), serde_json::to_vec(&value).unwrap()).unwrap();
        fs::set_permissions(path(id), fs::Permissions::from_mode(0o600)).unwrap();
    }
    fs::write(path(1), b"PRIVATE corrupt envelope").unwrap();
    let outside = fixture._temp.path().join("outside.json");
    fs::rename(path(3), &outside).unwrap();
    let outside_bytes = fs::read(&outside).unwrap();
    symlink(&outside, path(3)).unwrap();
    fs::set_permissions(path(4), fs::Permissions::from_mode(0o644)).unwrap();
    fs::remove_file(path(5)).unwrap();
    fs::create_dir(path(5)).unwrap();
    fs::write(cache.join("op-invalid.json"), b"PRIVATE invalid name").unwrap();

    for args in [
        vec!["controller", "pending", "--json"],
        vec!["controller", "pending", "--all", "--json"],
    ] {
        let (exit, stdout, stderr) = fixture.run(&args, &NoTransport);
        assert_eq!(exit, 0, "{stderr}");
        assert_eq!(stderr, "6 saved controller requests could not be read\n");
        let report: Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(report["pending"].as_array().unwrap().len(), 1);
        assert_eq!(report["pending"][0]["request_id"], ID);
        let unreadable = report["unreadable"].as_array().unwrap();
        assert_eq!(unreadable.len(), 6);
        for id in 1..=5 {
            assert!(
                unreadable
                    .iter()
                    .any(|entry| entry["request_id"] == format!("{id:032x}"))
            );
        }
        assert!(unreadable.iter().any(|entry| entry["request_id"].is_null()));
        assert!(!stdout.contains("PRIVATE"));
        assert!(!stdout.contains("outside.json"));
    }
    let (exit, stdout, stderr) = fixture.run(&["controller", "pending"], &NoTransport);
    assert_eq!(exit, 0);
    assert!(stdout.contains(ID));
    assert_eq!(stderr, "6 saved controller requests could not be read\n");
    for (id, code) in [
        (1, "CONTROLLER_TRANSPORT"),
        (2, "CONTROLLER_ENVELOPE_INCOMPATIBLE"),
        (3, "IO"),
        (4, "IO"),
        (5, "IO"),
    ] {
        let (exit, _, stderr) = fixture.run(
            &["controller", "retry", &format!("{id:032x}")],
            &NoTransport,
        );
        assert_ne!(exit, 0);
        assert!(stderr.contains(code), "{stderr}");
    }
    assert_eq!(fs::read(outside).unwrap(), outside_bytes);
    assert_eq!(fs::read(path(1)).unwrap(), b"PRIVATE corrupt envelope");
}

#[test]
fn pending_rejects_duplicate_envelope_fields_like_explicit_retry() {
    let fixture = Fixture::new(true);
    let cache = fixture.paths.controller_cache_root();
    persist_operation_envelope(&cache, &request()).unwrap();
    let path = cache.join(format!("op-{ID}.json"));
    let original = fs::read_to_string(&path).unwrap();
    for (field, value) in [
        ("request_id", json!(ID)),
        ("settled_at_millis", Value::Null),
        ("outcome", Value::Null),
    ] {
        let duplicate = format!("{},\"{field}\":{value}}}", &original[..original.len() - 1]);
        fs::write(&path, &duplicate).unwrap();
        let (exit, stdout, stderr) =
            fixture.run(&["controller", "pending", "--json"], &NoTransport);
        assert_eq!(exit, 0, "{stderr}");
        assert_eq!(stderr, "1 saved controller requests could not be read\n");
        let report: Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(report["pending"], json!([]));
        assert_eq!(
            report["unreadable"],
            json!([{"request_id": ID, "code": "CONTROLLER_TRANSPORT"}])
        );
        let (exit, _, stderr) = fixture.run(&["controller", "retry", ID], &NoTransport);
        assert_ne!(exit, 0);
        assert!(stderr.contains("CONTROLLER_TRANSPORT"));
        assert_eq!(fs::read_to_string(&path).unwrap(), duplicate);
    }
}

#[test]
fn retry_reconstructs_frozen_frame_settles_and_prints_ack_result() {
    let fixture = Fixture::new(true);
    persist_operation_envelope(&fixture.paths.controller_cache_root(), &request()).unwrap();
    let runner = Reply::new(false);
    let (exit, stdout, stderr) = fixture.run(&["controller", "retry", ID, "--json"], &runner);
    assert_eq!(exit, 0, "{stderr}");
    assert_eq!(
        serde_json::from_str::<Value>(&stdout).unwrap(),
        json!({"run_id": RUN, "task_ids": [TASK], "turn_id": TURN})
    );
    let sent = runner.frames.lock().unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(
        parse_request(decode_frame(&sent[0]).unwrap()).unwrap(),
        request()
    );
    drop(sent);
    let envelope = load_operation_envelope(&fixture.paths.controller_cache_root(), ID)
        .unwrap()
        .unwrap();
    assert!(serde_json::to_value(envelope).unwrap()["settled_at_millis"].is_u64());
    let (_, pending, _) = fixture.run(&["controller", "pending", "--json"], &NoTransport);
    assert_eq!(
        serde_json::from_str::<Value>(&pending).unwrap(),
        json!({"pending": [], "unreadable": []})
    );
    let (exit, text, stderr) = fixture.run(&["controller", "retry", ID], &runner);
    assert_eq!(exit, 0, "{stderr}");
    assert!(text.contains(&format!("request {ID} (task.batch): acknowledged")));
    for id in [TASK, TURN, RUN] {
        assert!(text.contains(id), "{text}");
    }
}

#[test]
fn retry_rejection_preserves_exit_category_and_settles() {
    let fixture = Fixture::new(true);
    persist_operation_envelope(&fixture.paths.controller_cache_root(), &request()).unwrap();
    let runner = Reply::new(true);
    let (exit, _, stderr) = fixture.run(&["controller", "retry", ID, "--json"], &runner);
    assert_eq!(exit, 75);
    assert!(stderr.contains("CAPACITY_BUSY"));
    assert!(!stderr.contains("PRIVATE"));
    assert_eq!(runner.frames.lock().unwrap().len(), 1);
    let envelope = load_operation_envelope(&fixture.paths.controller_cache_root(), ID)
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(envelope).unwrap()["outcome"],
        json!({"kind": "rejected", "code": "CAPACITY_BUSY"})
    );
}

#[test]
fn recovery_requires_controller_mode_and_refuses_missing_or_incompatible_envelopes() {
    let disabled = Fixture::new(false);
    for args in [
        vec!["controller", "pending"],
        vec!["controller", "retry", ID],
    ] {
        let (exit, _, stderr) = disabled.run(&args, &NoTransport);
        assert_eq!(exit, 64, "{stderr}");
        assert!(stderr.contains("CONTROLLER_MODE_REQUIRED"));
    }
    let enabled = Fixture::new(true);
    let (exit, _, stderr) = enabled.run(&["controller", "retry", ID], &NoTransport);
    assert_eq!(exit, 64, "{stderr}");
    assert!(stderr.contains("CONTROLLER_ENVELOPE_NOT_FOUND"));
    persist_operation_envelope(&enabled.paths.controller_cache_root(), &request()).unwrap();
    enabled.rewrite(|value| {
        value["payload_sha256"] = json!(
            canonical_request_sha256(PROTOCOL_VERSION - 1, request().command(), request().body())
                .unwrap()
        )
    });
    let (exit, _, stderr) = enabled.run(&["controller", "retry", ID], &NoTransport);
    assert_eq!(exit, 64, "{stderr}");
    assert!(stderr.contains("CONTROLLER_ENVELOPE_INCOMPATIBLE"));
    assert!(stderr.contains("protocol"));
}

#[test]
fn ordinary_mutations_settle_typed_rejections_through_shared_sender() {
    for args in [
        vec!["task", "cancel", TASK],
        vec!["task", "close", TASK],
        vec!["task", "say", TASK, "--message", "follow up"],
    ] {
        let fixture = Fixture::new(true);
        let runner = Reply::new(true);
        let (exit, _, stderr) = fixture.run(&args, &runner);
        assert_eq!(exit, 75, "{args:?}: {stderr}");
        let frames = runner.frames.lock().unwrap();
        assert_eq!(frames.len(), 1);
        let sent = parse_request(decode_frame(&frames[0]).unwrap()).unwrap();
        assert_eq!(sent.command(), format!("task.{}", args[1]));
        let envelope =
            load_operation_envelope(&fixture.paths.controller_cache_root(), sent.request_id())
                .unwrap()
                .unwrap();
        assert!(serde_json::to_value(envelope).unwrap()["settled_at_millis"].is_u64());
    }
}
