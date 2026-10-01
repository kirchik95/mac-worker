use std::{cell::Cell, path::PathBuf, rc::Rc, sync::Arc, time::Duration};

use mac_worker::test_support::{
    channel::{contracts::*, testing::*},
    core::config::{ControllerConfig, SshConfig},
    host::{
        job::ProcessIdentity,
        process::{ProcessPolicy, ProcessRequest, ProcessRunner},
    },
};
use serde_json::json;

const TASK: &str = "0123456789ab4def8123456789abcdef";

#[test]
fn route_digest_binds_each_configured_field_and_has_a_canonical_fixture() {
    let controller = ControllerConfig {
        enabled: true,
        ssh: "mini-1".into(),
        remote_binary: "~/.local/bin/worker".into(),
    };
    let ssh = SshConfig {
        multiplex: true,
        config_file: None,
    };
    let route = ConfiguredRoute::new(&controller, &ssh).unwrap();
    let digest = route.digest().unwrap();
    // SHA-256 of the sorted compact JSON identity, independently computed.
    assert_eq!(
        digest.as_str(),
        "6866e42a19302e3334777639170d70b00455057cff47746d6f80b5f894c1f32f"
    );
    for other in [
        ConfiguredRoute {
            ssh: "mini-2".into(),
            ..route.clone()
        },
        ConfiguredRoute {
            remote_binary: "/opt/worker".into(),
            ..route.clone()
        },
        ConfiguredRoute {
            ssh_config_file: Some("/private/ssh/config".into()),
            ..route.clone()
        },
    ] {
        assert_ne!(digest, other.digest().unwrap());
    }
    let mut invalid = route.clone();
    invalid.ssh_config_file = Some("relative/config".into());
    assert!(invalid.digest().is_err());
    for text in ["", "a", &"A".repeat(64), &"g".repeat(64)] {
        assert!(RouteDigest::parse(text).is_err());
    }
}

#[test]
fn scope_allowlist_separates_all_four_read_loops_from_raw_families() {
    let cases = [
        (
            ReadLoopScope::Wait,
            "task.wait.poll",
            json!({"task_id": TASK}),
            true,
        ),
        (
            ReadLoopScope::Wait,
            "task.wait.poll",
            json!({"run": "my-run"}),
            true,
        ),
        (
            ReadLoopScope::LogsFollow,
            "task.logs",
            json!({"task_id": TASK, "follow": true, "wait_ms": 15_000}),
            true,
        ),
        (
            ReadLoopScope::LogsFollow,
            "task.logs",
            json!({"task_id": TASK}),
            true,
        ),
        (
            ReadLoopScope::Wait,
            "task.logs",
            json!({"task_id": TASK}),
            false,
        ),
        (
            ReadLoopScope::EventsFollow,
            "task.list",
            json!({"controller_events": {"op": "read"}}),
            true,
        ),
        (
            ReadLoopScope::Notify,
            "task.list",
            json!({"controller_events": {"op": "tasks", "task_ids": [TASK]}}),
            true,
        ),
        (
            ReadLoopScope::Notify,
            "task.list",
            json!({"controller_events": {"op": "repair"}}),
            true,
        ),
        (
            ReadLoopScope::LogsFollow,
            "task.list",
            json!({"controller_health": true}),
            true,
        ),
        (
            ReadLoopScope::EventsFollow,
            "task.list",
            json!({"controller_health": true}),
            true,
        ),
        (
            ReadLoopScope::Notify,
            "task.list",
            json!({"controller_health": true}),
            true,
        ),
        (
            ReadLoopScope::Wait,
            "task.list",
            json!({"controller_health": true}),
            false,
        ),
    ];
    for (scope, command, body, want) in cases {
        let request = request_fixture(command, body);
        assert_eq!(eligible_read(scope, &request), want, "{scope:?} {command}");
        if want {
            assert!(server_eligible_read(&request));
        }
    }
    for command in [
        "task.submit",
        "task.say",
        "task.cancel",
        "task.close",
        "task.batch",
        "checkpoint.submit",
        "controller.retry",
        "controller.drain",
        "task.reconcile",
        "task.publish-retry",
        "controller.transfer.source.prepare",
        "controller.transfer.source.finish",
        "controller.transfer.result.prepare",
        "task.status",
        "task.list",
        "task.diff",
        "task.result",
        "controller.health",
        "doctor",
        "controller.channel.identity",
        "controller.channel.repin",
        "host.controller-service",
    ] {
        let request = request_fixture(command, json!({}));
        assert!(!server_eligible_read(&request), "{command}");
        for scope in [
            ReadLoopScope::Wait,
            ReadLoopScope::LogsFollow,
            ReadLoopScope::EventsFollow,
            ReadLoopScope::Notify,
        ] {
            assert!(!eligible_read(scope, &request), "{scope:?} {command}");
        }
    }
    for body in [
        json!({}),
        json!({"drained": true}),
        json!({"drained": false}),
    ] {
        assert!(!server_eligible_read(&request_fixture(
            "controller.drain",
            body
        )));
    }
}

#[test]
fn request_grammar_rejects_mixed_selectors_unknown_keys_and_bad_types() {
    let cases = [
        ("task.wait.poll", json!({})),
        ("task.wait.poll", json!({"task_id": TASK, "run": "my-run"})),
        ("task.wait.poll", json!({"task_id": "bad"})),
        ("task.wait.poll", json!({"run": ""})),
        ("task.wait.poll", json!({"run": "a\nb"})),
        ("task.wait.poll", json!({"task_id": TASK, "full": true})),
        ("task.logs", json!({"task_id": TASK, "unknown": 1})),
        ("task.logs", json!({"task_id": "bad"})),
        ("task.logs", json!({"task_id": TASK, "turn_id": "bad"})),
        ("task.logs", json!({"task_id": TASK, "offset": -1})),
        ("task.logs", json!({"task_id": TASK, "wait_ms": "15000"})),
        ("task.logs", json!({"task_id": TASK, "follow": "true"})),
        (
            "task.logs",
            json!({"task_id": TASK, "turn": 4294967296_u64}),
        ),
        (
            "task.list",
            json!({"controller_events": {"op": "read"}, "controller_health": true}),
        ),
        (
            "task.list",
            json!({"controller_events": {"op": "read"}, "full": true}),
        ),
        (
            "task.list",
            json!({"controller_events": {"op": "read", "unexpected": true}}),
        ),
        (
            "task.list",
            json!({"controller_events": {"op": "tasks", "task_ids": [TASK, TASK]}}),
        ),
        (
            "task.list",
            json!({"controller_events": {"op": "tasks", "task_ids": []}}),
        ),
        ("task.list", json!({"controller_events": {"op": "unknown"}})),
        ("task.list", json!({"controller_health": false})),
        (
            "task.list",
            json!({"controller_health": true, "full": true}),
        ),
        (
            "task.list",
            json!({"controller_socket": {"op": "identity"}}),
        ),
    ];
    for (command, body) in cases {
        assert!(
            !server_eligible_read(&request_fixture(command, body.clone())),
            "{command} {body}"
        );
    }
    // Existing nullable/default/clamped logs arguments remain valid grammar.
    assert!(server_eligible_read(&request_fixture(
        "task.logs",
        json!({
            "task_id": TASK, "turn": null, "turn_id": null, "raw": null, "follow": null,
            "limit": 0, "wait_ms": u64::MAX,
        })
    )));
}

#[test]
fn request_grammar_accepts_existing_nullable_wait_selectors() {
    for body in [
        json!({"task_id": TASK, "run": null}),
        json!({"task_id": null, "run": "fixture"}),
    ] {
        assert!(server_eligible_read(&request_fixture(
            "task.wait.poll",
            body
        )));
    }
    assert!(!server_eligible_read(&request_fixture(
        "task.wait.poll",
        json!({"task_id": null, "run": null})
    )));
}

#[test]
fn frozen_pin_bytes_and_service_record_schema_roundtrip() {
    let identity = identity_fixture();
    let pin = Pin::from_identity(&identity);
    assert_eq!(
        serde_json::to_string(&pin).unwrap(),
        concat!(
            "{\"schema_version\":1,\"route_sha256\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",",
            "\"controller_client_id\":\"0123456789ab4def8123456789abcdef\",",
            "\"account\":{\"uid\":501,\"username\":\"controller\",\"home\":\"/Users/controller\"}}"
        )
    );
    let record = ServiceRecord {
        schema_version: 1,
        service: identity.service,
        binding: SocketBinding {
            parent: entry_fixture(libc::S_IFDIR.into(), 0o700),
            socket: entry_fixture(libc::S_IFSOCK.into(), 0o600),
        },
        executable: PinnedExecutable {
            path: "/private/controller/rpc/e0123456789ab4def8123456789abcdef".into(),
            binding: entry_fixture(libc::S_IFREG.into(), 0o755),
        },
    };
    assert_eq!(
        serde_json::from_slice::<ServiceRecord>(&serde_json::to_vec(&record).unwrap()).unwrap(),
        record
    );
    let mut invalid = serde_json::to_value(record).unwrap();
    invalid["schema_version"] = json!(2);
    assert!(serde_json::from_value::<ServiceRecord>(invalid).is_err());
    let mut oversized = identity_fixture();
    oversized.service.account.home = format!("/{}", "h".repeat(4000)).into();
    oversized
        .service
        .features
        .extend((0..62).map(|i| format!("f{i:02}{}", "f".repeat(61))));
    assert!(serde_json::to_vec(&oversized).is_err());
}

#[test]
fn required_service_identity_is_verified_while_journal_hints_are_optional() {
    let expected = identity_fixture();
    let mut changed = expected.clone();
    changed.service.journal_id = None;
    verify_expected_service(&expected, &changed).unwrap();
    changed.service.journal_id = Some(UuidString::new_v4());
    verify_expected_service(&expected, &changed).unwrap();
    type IdentityMutation = Box<dyn Fn(&mut SocketIdentity)>;
    let changes: Vec<IdentityMutation> = vec![
        Box::new(|i| i.route_sha256 = RouteDigest::parse(&"b".repeat(64)).unwrap()),
        Box::new(|i| i.service.protocol_version += 1),
        Box::new(|i| i.service.channel_version += 1),
        Box::new(|i| {
            i.service.controller_client_id = "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa".parse().unwrap()
        }),
        Box::new(|i| i.service.account.uid += 1),
        Box::new(|i| i.service.account.username = "another".into()),
        Box::new(|i| i.service.account.home = "/Users/another".into()),
        Box::new(|i| i.service.leader = ProcessIdentity::new(99, 999).unwrap()),
        Box::new(|i| i.service.service_generation = UuidString::new_v4()),
        Box::new(|i| i.service.socket_path = "/private/channel/other".into()),
        Box::new(|i| i.service.features = vec!["controller.socket".into()]),
    ];
    for change in changes {
        let mut actual = expected.clone();
        change(&mut actual);
        assert!(verify_expected_service(&expected, &actual).is_err());
    }
}

#[test]
fn uuid_strings_reject_nil_other_versions_and_noncanonical_forms() {
    let text = "01234567-89ab-4def-8123-456789abcdef";
    let value = UuidString::parse(text).unwrap();
    assert_eq!(value.as_str(), text);
    assert_eq!(
        serde_json::to_string(&value).unwrap(),
        format!("\"{text}\"")
    );
    assert_eq!(
        serde_json::from_str::<UuidString>(&format!("\"{text}\"")).unwrap(),
        value
    );
    for text in [
        "00000000-0000-0000-0000-000000000000",
        "01234567-89ab-1def-8123-456789abcdef",
        "01234567-89ab-4def-0123-456789abcdef",
        "0123456789ab4def8123456789abcdef",
        "01234567-89AB-4DEF-8123-456789ABCDEF",
        "{01234567-89ab-4def-8123-456789abcdef}",
        "urn:uuid:01234567-89ab-4def-8123-456789abcdef",
        "",
        "bad",
    ] {
        assert!(UuidString::parse(text).is_err(), "{text}");
        assert!(serde_json::from_value::<UuidString>(json!(text)).is_err());
    }
    assert!(serde_json::from_value::<UuidString>(json!([1, 2, 3])).is_err());
    UuidString::parse(UuidString::new_v4().as_str()).unwrap();
}

#[test]
fn identity_and_pin_serialization_reject_invalid_schema_bounds_and_duplicates() {
    let identity = identity_fixture();
    let value = serde_json::to_value(&identity).unwrap();
    assert!(value["service"]["service_generation"].is_string());
    assert!(value["service"]["journal_id"].is_string());
    assert_eq!(
        serde_json::from_value::<SocketIdentity>(value.clone()).unwrap(),
        identity
    );
    let mut missing_hint = value.clone();
    missing_hint["service"]
        .as_object_mut()
        .unwrap()
        .remove("journal_id");
    assert!(
        serde_json::from_value::<SocketIdentity>(missing_hint)
            .unwrap()
            .service
            .journal_id
            .is_none()
    );
    for (key, bad) in [
        ("protocol_version", json!(6)),
        ("channel_version", json!(2)),
        ("journal_id", value["service"]["service_generation"].clone()),
        ("service_generation", json!("bad")),
        ("socket_path", json!("relative/s")),
        ("socket_path", json!("/private/a:%C")),
        ("socket_path", json!(format!("/{}", "s".repeat(103)))),
        (
            "features",
            json!(["controller.socket", "controller.socket"]),
        ),
        ("features", json!(["z", "controller.socket"])),
        ("features", json!(["x".repeat(65)])),
        (
            "features",
            json!((0..65).map(|i| format!("f{i:02}")).collect::<Vec<_>>()),
        ),
    ] {
        let mut invalid = value.clone();
        invalid["service"][key] = bad;
        assert!(
            serde_json::from_value::<SocketIdentity>(invalid).is_err(),
            "{key}"
        );
    }
    for (key, bad) in [
        ("username", json!("bad\nuser")),
        ("username", json!("x".repeat(257))),
        ("home", json!("relative")),
        ("home", json!("/x\n")),
        ("home", json!(format!("/{}", "x".repeat(4096)))),
    ] {
        let mut invalid = value.clone();
        invalid["service"]["account"][key] = bad;
        assert!(
            serde_json::from_value::<SocketIdentity>(invalid).is_err(),
            "{key}"
        );
    }
    let pin = Pin::from_identity(&identity);
    let pin_value = serde_json::to_value(&pin).unwrap();
    assert_eq!(pin_value.as_object().unwrap().len(), 4);
    let mut restarted = identity.clone();
    restarted.service.service_generation = UuidString::new_v4();
    restarted.service.journal_id = None;
    assert_eq!(pin, Pin::from_identity(&restarted));
    for key in ["schema_version", "unexpected"] {
        let mut invalid = pin_value.clone();
        invalid[key] = json!(2);
        assert!(serde_json::from_value::<Pin>(invalid).is_err());
    }
    let duplicate = serde_json::to_string(&pin)
        .unwrap()
        .replacen("{", "{\"schema_version\":1,", 1);
    assert!(serde_json::from_str::<Pin>(&duplicate).is_err());
    let duplicate = serde_json::to_string(&identity).unwrap().replacen(
        "{",
        "{\"route_sha256\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",",
        1,
    );
    assert!(serde_json::from_str::<SocketIdentity>(&duplicate).is_err());
    let mut oversized = pin;
    oversized.account.home = PathBuf::from(format!("/{}", "h".repeat(4000)));
    assert!(serde_json::to_vec(&oversized).is_err());
}

#[test]
fn borrowed_should_stop_is_live_without_send_sync_or_command_cancellation() {
    let runtime = Arc::new(ManualRuntime::default());
    let flag = Rc::new(Cell::new(false));
    let polls = Cell::new(0);
    let stop = || {
        polls.set(polls.get() + 1);
        flag.get()
    };
    let ctx = ClientContext {
        runtime: runtime.as_ref(),
        deadline: Duration::from_secs(30),
        should_stop: &stop,
    };
    ctx.check().unwrap();
    flag.set(true);
    assert_eq!(
        ctx.check(),
        Err(ChannelFailure::Unavailable(ChannelReason::Cancelled))
    );
    assert!(!runtime.cancelled());
    assert_eq!(polls.get(), 2);
    let cleanup = CleanupContext::new(runtime.clone());
    assert_eq!(cleanup.remaining(), Duration::from_secs(5));
    runtime.cancel();
    cleanup.check().unwrap();
}

#[test]
fn contexts_preserve_deadline_and_owned_server_cancellation() {
    let runtime = Arc::new(ManualRuntime::default());
    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ctx = ServerContext {
        runtime: runtime.clone(),
        deadline: Duration::from_secs(30),
        cancelled: cancelled.clone(),
    };
    assert_eq!(ctx.remaining(), Duration::from_secs(30));
    runtime.advance(Duration::from_secs(29));
    assert_eq!(ctx.remaining(), Duration::from_secs(1));
    cancelled.store(true, std::sync::atomic::Ordering::Release);
    assert_eq!(
        ctx.check(),
        Err(ChannelFailure::Unavailable(ChannelReason::Cancelled))
    );
    cancelled.store(false, std::sync::atomic::Ordering::Release);
    runtime.advance(Duration::from_secs(1));
    assert_eq!(
        ctx.check(),
        Err(ChannelFailure::Unavailable(ChannelReason::Timeout))
    );
    assert_eq!(ctx.remaining(), Duration::ZERO);
}

#[test]
fn child_launch_contract_keeps_installed_runner_path_distinct_from_rpc_link() {
    let image = RunningImage {
        path: "/Users/controller/.local/bin/worker".into(),
        device: 1,
        inode: 2,
    };
    let spec = ChildRpcSpec {
        executable: PinnedExecutable {
            path: "/private/controller/rpc/e0123456789ab4def8123456789abcdef".into(),
            binding: entry_fixture(libc::S_IFREG.into(), 0o755),
        },
        detached_runner_executable: image.path.clone(),
        config: "/Users/controller/.config/mac-worker/config.toml".into(),
        environment: vec![(
            DETACHED_RUNNER_EXECUTABLE_ENV.into(),
            image.path.clone().into_os_string(),
        )],
    };
    assert_ne!(spec.executable.path, spec.detached_runner_executable);
    assert_eq!(
        spec.environment[0].0,
        "MAC_WORKER_DETACHED_RUNNER_EXECUTABLE"
    );
    assert_eq!(spec.environment[0].1, "/Users/controller/.local/bin/worker");
}

#[test]
fn forward_failure_retains_uncertain_open_and_cleanup_ignores_foreground_cancel() {
    let runtime = Arc::new(ManualRuntime::default());
    runtime.cancel();
    let cleanup = CleanupContext::new(runtime.clone());
    cleanup.check().unwrap();
    runtime.advance(Duration::from_secs(5));
    assert_eq!(
        cleanup.check(),
        Err(ChannelFailure::Unavailable(ChannelReason::Timeout))
    );
    let failure =
        ForwardOpenFailure::retained(ChannelFailure::Unavailable(ChannelReason::Cancelled));
    assert_eq!(failure.disposition, ForwardDisposition::Retained);
    let raw = RecordingRunner::new(vec![]);
    let forwards = FakeForwardControl::new("/private/forward/s".into());
    forwards.fail_next(failure);
    let live = ManualRuntime::default();
    let ctx = ClientContext {
        runtime: &live,
        deadline: Duration::from_secs(30),
        should_stop: &|| false,
    };
    let route = ConfiguredRoute::new(
        &ControllerConfig {
            enabled: true,
            ssh: "mini-1".into(),
            ..ControllerConfig::default()
        },
        &SshConfig::default(),
    )
    .unwrap();
    let master = forwards.resolve(&raw, &route, &ctx).unwrap();
    let failed = forwards
        .open(&raw, &master, &identity_fixture(), &ctx)
        .err()
        .unwrap();
    assert_eq!(failed.disposition, ForwardDisposition::Retained);
    assert!(raw.calls().is_empty());
}

fn entry_fixture(kind: u32, mode: u32) -> EntryIdentity {
    EntryIdentity {
        device: 1,
        inode: 2,
        owner: 501,
        kind,
        mode,
    }
}

// Compiles all three legacy runner methods through trait-object Arc delegation.
#[test]
fn recording_runner_arc_keeps_interruptible_predicate_live() {
    let raw = Arc::new(RecordingRunner::new(vec![]));
    let runner: Arc<dyn ProcessRunner> = raw.clone();
    let request = ProcessRequest {
        program: "fake".into(),
        args: vec![],
        environment: vec![],
        environment_remove: vec![],
        stdin: None,
        policy: ProcessPolicy {
            stdout_limit: 1,
            stderr_limit: 1,
            deadline: Duration::from_secs(30),
        },
        isolate_parent_environment: false,
    };
    assert!(runner.run_interruptible(&request, &|| true).is_err());
    assert_eq!(raw.calls().len(), 0);
}
