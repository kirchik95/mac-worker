use mac_worker::test_support::integration::*;
use mac_worker::test_support::{
    controller::{
        acknowledge_controller_submit_pins, parse_request, reconcile_controller_submit_pins,
        record_controller_submit_pins, record_session_source_finished,
        require_session_source_finished,
    },
    core::paths::PathLayout,
    host::process::SystemProcessRunner,
    session::{SessionAgent, SessionImportMeta},
    transfer::repo::TransferRepo,
};
use serde_json::json;

struct NoProcesses;

struct CompanionPeer {
    paths: PathLayout,
    config: mac_worker::test_support::core::config::Config,
    discovery: bool,
    execution: bool,
    calls: std::sync::Mutex<Vec<serde_json::Value>>,
}
impl mac_worker::test_support::host::process::ProcessRunner for CompanionPeer {
    fn run(
        &self,
        process: &mac_worker::test_support::host::process::ProcessRequest,
    ) -> Result<
        mac_worker::test_support::host::process::ProcessResult,
        mac_worker::test_support::core::error::WorkerError,
    > {
        use mac_worker::test_support::controller::{
            decode_frame, encode_json_frame, serve_rpc_with_integration_features,
        };
        use std::os::unix::process::ExitStatusExt;
        let bytes = process.stdin.as_ref().unwrap();
        let request: serde_json::Value = serde_json::from_slice(decode_frame(bytes)?).unwrap();
        self.calls.lock().unwrap().push(request["body"].clone());
        let features = if self.execution {
            vec![CONTROLLER_FEATURE_INTEGRATION.into()]
        } else {
            vec![]
        };
        let mut out = Vec::new();
        let result = serve_rpc_with_integration_features(
            &self.paths,
            &self.config,
            &NoProcesses,
            &mut std::io::Cursor::new(bytes),
            &mut out,
            &features,
        );
        let status = match result {
            Ok(()) => {
                if request["body"].get("controller_health").is_some() {
                    let mut reply: serde_json::Value =
                        serde_json::from_slice(decode_frame(&out)?).unwrap();
                    reply["result"]["features"] = if self.discovery {
                        json!(["controller.events", CONTROLLER_FEATURE_INTEGRATION])
                    } else {
                        json!(["controller.events"])
                    };
                    out = encode_json_frame(&reply)?;
                }
                0
            }
            Err(error) => {
                out = encode_json_frame(
                    &mac_worker::test_support::host::job::HostControlError::new(
                        error.public_code(),
                        error.public_message(),
                    )
                    .unwrap(),
                )?;
                1 << 8
            }
        };
        Ok(mac_worker::test_support::host::process::ProcessResult {
            status: std::process::ExitStatus::from_raw(status),
            stdout: out,
            stderr: vec![],
        })
    }
}

#[test]
fn concrete_event_client_companion_matrix_refuses_old_and_rolled_back_execution_without_mutation() {
    use mac_worker::test_support::events::{
        EventSource, client::ControllerEventClient, testing::ManualEventRuntime,
    };
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };
    for (discovery, execution) in [(false, false), (true, false), (true, true)] {
        let temp = tempfile::tempdir().unwrap();
        let paths = isolated_paths(&temp.path().canonicalize().unwrap());
        let config = fixture_config(&paths);
        let peer = Arc::new(CompanionPeer {
            paths: paths.clone(),
            config: config.clone(),
            discovery,
            execution,
            calls: Mutex::new(vec![]),
        });
        let mut controller = config.controller;
        controller.ssh = "fixture.invalid".into();
        controller.enabled = true;
        let client = ControllerEventClient::new(
            peer.clone(),
            controller,
            Arc::new(ManualEventRuntime::new()),
        );
        let result = client.integrations(&[fixture_task()], Duration::from_secs(3));
        if discovery && execution {
            assert_eq!(
                result.unwrap().integrations.get(&fixture_task()),
                Some(&None)
            );
        } else {
            assert_eq!(result.unwrap_err().public_code(), "INTEGRATION_UNAVAILABLE");
        }
        let calls = peer.calls.lock().unwrap();
        assert_eq!(calls[0], json!({"controller_health":true}));
        assert_eq!(calls.len(), if discovery { 2 } else { 1 });
        if discovery {
            assert_eq!(
                calls[1],
                json!({"integration":{"task_ids":[fixture_task()]}})
            );
        }
        assert!(!paths.controller_state_root().join("requests").exists());
        assert!(!paths.state.join("integrations").exists());
    }
}

#[test]
fn native_notifier_repairs_dropped_hints_confirms_revisions_and_deduplicates_across_restart() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        controller::ControllerLeader,
        events::{
            EventSource, NoticeChannel, NotifyOptions, PreviousProjection, TaskAddressQuery,
            client::{ControllerEventClient, TaskReconciler},
            journal::{ControllerJournal, JournalOptions},
            notify::{
                NotifyCache,
                follow::{NotifyExit, NotifyLoop},
            },
            testing::{ManualEventRuntime, RecordingNoticeChannel},
        },
    };
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };
    let temp = tempfile::tempdir().unwrap();
    let paths = isolated_paths(&temp.path().canonicalize().unwrap());
    let config = fixture_config(&paths);
    ClientStateStore::open(&paths.state)
        .unwrap()
        .create_task(sample_ordinary(fixture_task(), fixture_source()))
        .unwrap();
    let state = RootedIntegrationState::open(&paths, Arc::new(ManualIntegrationRuntime::default()))
        .unwrap();
    let mut record = sample_record(fixture_task(), fixture_source(), "main");
    state
        .publish_policy(record.task_id, &record.policy)
        .unwrap();
    state
        .replace(record.task_id, IntegrationRevision(0), &record)
        .unwrap();
    let runtime = Arc::new(ManualEventRuntime::new());
    let leader = ControllerLeader::acquire(&paths.controller_state_root()).unwrap();
    let _journal = ControllerJournal::initialize_for_leader(
        &paths,
        &leader,
        JournalOptions {
            runtime: runtime.clone(),
        },
    )
    .unwrap();
    let peer = Arc::new(CompanionPeer {
        paths: paths.clone(),
        config: config.clone(),
        discovery: true,
        execution: true,
        calls: Mutex::new(vec![]),
    });
    let mut controller = config.controller;
    controller.enabled = true;
    controller.ssh = "fixture.invalid".into();
    let client = ControllerEventClient::new(peer.clone(), controller.clone(), runtime.clone());
    let channel = Arc::new(RecordingNoticeChannel::new());
    let channels: Vec<Arc<dyn NoticeChannel>> = vec![channel.clone()];
    for epoch in [0, 1, 1, 2, 2] {
        if epoch > record.snapshot.epoch {
            let revision = record.snapshot.revision;
            record.snapshot.revision = revision.next().unwrap();
            record.snapshot.epoch = epoch;
            record.snapshot.state = IntegrationStatus::Blocked;
            record.snapshot.blocked_code = Some(IntegrationCode::IntegrationChecksFailed);
            state.replace(record.task_id, revision, &record).unwrap();
        }
        // Reopen both durable cache and actual reconciler on each iteration.
        // No IntegrationChanged hint is published: only repair sees the change.
        let cache = NotifyCache::open(&paths, &controller).unwrap();
        let saved = cache.load().unwrap();
        let mut reconciler = TaskReconciler::new(
            PreviousProjection::Absent,
            saved.consumed_after,
            saved
                .pending
                .iter()
                .map(|candidate| candidate.task_id)
                .collect(),
            runtime.clone(),
        );
        let mut diagnostics = vec![];
        assert_eq!(
            NotifyLoop {
                source: &client,
                reconciler: &mut reconciler,
                cache: &cache,
                channels: &channels,
                options: &NotifyOptions::default(),
                runtime: runtime.clone(),
                stop_at: None
            }
            .run(&mut diagnostics)
            .unwrap(),
            NotifyExit::Complete,
            "{}",
            String::from_utf8_lossy(&diagnostics)
        );
        assert_eq!(channel.records().len(), epoch as usize);
        let facts = client
            .tasks(
                TaskAddressQuery::try_new(vec![record.task_id], false, None).unwrap(),
                Duration::from_secs(30),
            )
            .unwrap();
        let companion = client
            .integrations(&[record.task_id], Duration::from_secs(30))
            .unwrap();
        assert!(
            facts.rows[0]
                .integration
                .as_ref()
                .unwrap()
                .confirms(companion.integrations[&record.task_id].as_ref().unwrap())
        );
        assert!(!paths.controller_state_root().join("requests").exists());
    }
    assert!(
        peer.calls
            .lock()
            .unwrap()
            .iter()
            .any(|body| body.get("integration").is_some())
    );
}

#[test]
fn capable_companion_and_event_rpc_read_saved_state_without_request_rows_or_processes() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        controller::{ControllerFault, decode_frame, encode_json_frame, serve_rpc_with_runtime},
    };
    let temp = tempfile::tempdir().unwrap();
    let paths = isolated_paths(&temp.path().canonicalize().unwrap());
    let config = fixture_config(&paths);
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    ClientStateStore::open(&paths.state)
        .unwrap()
        .create_task(ordinary)
        .unwrap();
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let mut record = sample_record(fixture_task(), fixture_source(), "main");
    record.snapshot.state = IntegrationStatus::Blocked;
    record.snapshot.blocked_code = Some(IntegrationCode::IntegrationChecksFailed);
    state
        .publish_policy(record.task_id, &record.policy)
        .unwrap();
    state
        .replace(record.task_id, IntegrationRevision(0), &record)
        .unwrap();
    for body in [
        json!({"integration": {"task_ids": [record.task_id]}}),
        json!({"controller_events": {"op":"tasks", "task_ids":[record.task_id], "include_titles":false}}),
    ] {
        let payload = json!({"protocol_version":7,"request_id":"00000000000000000000000000000043",
            "command":"task.list", "body":body});
        let mut out = Vec::new();
        serve_rpc_with_runtime(
            &paths,
            &config,
            &NoProcesses,
            &mut std::io::Cursor::new(encode_json_frame(&payload).unwrap()),
            &mut out,
            ControllerFault::None,
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(decode_frame(&out).unwrap()).unwrap();
        if body.get("integration").is_some() {
            assert_eq!(
                value["result"]["integrations"][record.task_id.to_string()],
                json!(record.snapshot)
            );
        } else {
            assert_eq!(
                value["result"]["rows"][0]["integration"],
                json!(record.snapshot.annotation().unwrap())
            );
            assert_eq!(value["result"]["rows"][0]["outcome"], "blocked");
        }
        assert!(!paths.controller_state_root().exists());
        assert_eq!(state.load(record.task_id).unwrap().unwrap(), record);
    }
}

#[test]
fn capable_rpc_redrive_uses_the_native_imported_receipt_idempotently() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        controller::{ControllerFault, decode_frame, encode_json_frame, serve_rpc_with_runtime},
    };
    let temp = tempfile::tempdir().unwrap();
    let paths = isolated_paths(&temp.path().canonicalize().unwrap());
    let config = fixture_config(&paths);
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    ClientStateStore::open(&paths.state)
        .unwrap()
        .create_task(ordinary.clone())
        .unwrap();
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let mut record = sample_record(fixture_task(), fixture_source(), "main");
    record.snapshot.state = IntegrationStatus::Integrated;
    record.snapshot.disposition = Some(IntegrationDisposition::AlreadyIntegrated);
    record.snapshot.observed_target_oid = Some(fixture_head());
    record.receipt = Some(IntegrationReceipt {
        integration_id: record.snapshot.integration_id,
        epoch: record.snapshot.epoch,
        source_turn_id: fixture_source(),
        source_head: fixture_head(),
        target_head: fixture_head(),
        merge_oid: None,
        disposition: IntegrationDisposition::AlreadyIntegrated,
        recorded_at_millis: 1001,
        imported: true,
    });
    state
        .publish_policy(record.task_id, &record.policy)
        .unwrap();
    state
        .replace(record.task_id, IntegrationRevision(0), &record)
        .unwrap();
    let request_id = "00000000000000000000000000000054";
    let request = json!({"protocol_version":7,"request_id":request_id,"command":"task.integrate",
        "body":{"task_id":record.task_id,"expected":record.snapshot.revision,"request_id":request_id}});
    let mut output = vec![];
    serve_rpc_with_runtime(
        &paths,
        &config,
        &NoProcesses,
        &mut std::io::Cursor::new(encode_json_frame(&request).unwrap()),
        &mut output,
        ControllerFault::None,
    )
    .unwrap();
    let reply: serde_json::Value = serde_json::from_slice(decode_frame(&output).unwrap()).unwrap();
    assert!(reply.get("error").is_none(), "{reply}");
    assert_eq!(reply["result"], json!(record.snapshot));
    assert_eq!(state.load(record.task_id).unwrap().unwrap(), record);
    assert_eq!(
        ClientStateStore::open(&paths.state)
            .unwrap()
            .load_task(record.task_id)
            .unwrap(),
        ordinary
    );
}
impl mac_worker::test_support::host::process::ProcessRunner for NoProcesses {
    fn run(
        &self,
        _: &mac_worker::test_support::host::process::ProcessRequest,
    ) -> Result<
        mac_worker::test_support::host::process::ProcessResult,
        mac_worker::test_support::core::error::WorkerError,
    > {
        panic!("frozen cycle retry must not execute a process")
    }
}

fn isolated_paths(root: &std::path::Path) -> PathLayout {
    PathLayout {
        config: root.join("config.toml"),
        state: root.join("state/mac-worker"),
        cache: root.join("cache/mac-worker"),
        data: root.join("data/mac-worker"),
    }
}

fn wrapped_request(
    base: &str,
    package: &str,
) -> mac_worker::test_support::controller::ControllerRequest {
    let import =
        SessionImportMeta::new(SessionAgent::Codex, package.to_owned(), "0.160.0").unwrap();
    let submit: FrozenSubmitBody = serde_json::from_value(json!({
        "task_id": fixture_task(), "turn_id": fixture_source(), "created_at_millis": 1000,
        "prompt": "continue frozen session", "agent": "codex", "source": "local",
        "origin_url": "https://example.test/repo.git", "publish": ["fetch"],
        "close_on": "never", "wip": false, "project_id": "a".repeat(64),
        "worktree_id": "b".repeat(64), "base_oid": base, "timeout_millis": 2700000,
        "max_followups": 10, "permissions": "workspace", "requires": [],
        "include_untracked": [], "include_empty_dirs": [], "allow_sensitive": [],
        "cli_includes": [], "session_import": import
    }))
    .unwrap();
    parse_request(
        &serde_json::to_vec(&json!({
            "protocol_version": 7, "request_id": "00000000000000000000000000000041",
            "command": "task.submit-integrating", "body": {
                "submit": submit,
                // Cleanup must still see paired pins when admission rejects policy.
                "integration": {"malformed": true}
            }
        }))
        .unwrap(),
    )
    .unwrap()
}

fn valid_wrapper() -> mac_worker::test_support::controller::ControllerRequest {
    let invalid = wrapped_request(&"b".repeat(40), &"d".repeat(40));
    let submit: FrozenSubmitBody =
        serde_json::from_value(invalid.body()["submit"].clone()).unwrap();
    let wrapper = prepare_integrating_submit(submit, sample_policy("main")).unwrap();
    parse_request(&serde_json::to_vec(&json!({ "protocol_version": 7,
        "request_id": invalid.request_id(), "command": "task.submit-integrating", "body": wrapper })).unwrap()).unwrap()
}

fn fixture_config(paths: &PathLayout) -> mac_worker::test_support::core::config::Config {
    std::fs::write(
        &paths.config,
        "version = 1\n[[workers]]\nname = 'fixture'\nssh = 'fixture.invalid'\nslots = 1\n",
    )
    .unwrap();
    mac_worker::test_support::core::config::Config::load(&paths.config).unwrap()
}

#[test]
fn advertised_features_keep_disabled_submit_status_and_result_compatible_with_old_peers() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        controller::{
            ControllerFault, ControllerStore, TaskSubmitHandler, decode_frame, encode_json_frame,
            serve_rpc_with_integration_features, serve_rpc_with_runtime,
        },
    };
    // Catches accidentally requiring integration on an ordinary request or
    // adding integration fields to its saved submit or read replies.
    let temp = tempfile::tempdir().unwrap();
    let paths = isolated_paths(&temp.path().canonicalize().unwrap());
    let config = fixture_config(&paths);
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    tasks.create_task(ordinary.clone()).unwrap();
    let record_path = paths.state.join(format!("tasks/{}.json", fixture_task()));
    let original = std::fs::read(&record_path).unwrap();
    let mut body = wrapped_request(&"b".repeat(40), &"d".repeat(40)).body()["submit"].clone();
    body.as_object_mut().unwrap().remove("session_import");
    assert_eq!(body["requires"], json!([]));
    let request = parse_request(
        &serde_json::to_vec(&json!({
            "protocol_version": 7, "request_id": "00000000000000000000000000000044",
            "command": "task.submit", "body": body,
        }))
        .unwrap(),
    )
    .unwrap();
    let handler = TaskSubmitHandler::new(&NoProcesses, &config, &paths, &tasks);
    let old = TaskSubmitHandler::new(&NoProcesses, &config, &paths, &tasks)
        .with_integration_features(vec![]);
    let store = ControllerStore::open(&paths.controller_state_root()).unwrap();
    let native_ack = store
        .handle_with(&request, &handler, ControllerFault::StopAfterPublish)
        .unwrap();
    let old_store = ControllerStore::open(&paths.controller_state_root().join("old-peer")).unwrap();
    let old_ack = old_store
        .handle_with(&request, &old, ControllerFault::StopAfterPublish)
        .unwrap();
    assert_eq!(
        serde_json::to_vec(&native_ack).unwrap(),
        serde_json::to_vec(&old_ack).unwrap()
    );
    let saved = store.load(request.request_id()).unwrap().unwrap();
    assert_eq!(
        serde_json::to_vec(saved.body()).unwrap(),
        serde_json::to_vec(&body).unwrap()
    );
    assert!(saved.prepared().is_null());
    for command in ["task.status", "task.result"] {
        let payload = json!({"protocol_version": 7,
            "request_id": "00000000000000000000000000000045",
            "command": command, "body": {"task_id": fixture_task()}});
        let frame = encode_json_frame(&payload).unwrap();
        let mut native = Vec::new();
        serve_rpc_with_runtime(
            &paths,
            &config,
            &NoProcesses,
            &mut std::io::Cursor::new(&frame),
            &mut native,
            ControllerFault::None,
        )
        .unwrap();
        let mut old = Vec::new();
        serve_rpc_with_integration_features(
            &paths,
            &config,
            &NoProcesses,
            &mut std::io::Cursor::new(&frame),
            &mut old,
            &[],
        )
        .unwrap();
        assert_eq!(native, old, "{command}");
        let reply: serde_json::Value =
            serde_json::from_slice(decode_frame(&native).unwrap()).unwrap();
        assert_eq!(
            reply["result"]["status"],
            json!(ordinary.status()),
            "{command}"
        );
        assert!(reply["result"].get("integration").is_none(), "{command}");
        assert!(reply["result"].get("workflow_state").is_none(), "{command}");
    }
    assert_eq!(std::fs::read(record_path).unwrap(), original);
    assert!(!paths.state.join("integrations").exists());
    assert!(tasks.queue_snapshot().unwrap().entries().is_empty());
}

#[test]
fn native_controller_shutdown_preserves_undrained_dispatch_across_restart() {
    let stop = |child: &mut crate::controller_process::OwnedChild| {
        assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
        assert!(
            child
                .wait_timeout(crate::controller_process::CHILD_EXIT_TIMEOUT)
                .expect("controller shutdown did not finish")
                .success()
        );
    };
    let fixture = crate::controller_process::ProcessFixture::new();
    let root = fixture.controller_request_root();
    let mut controller = fixture.spawn_controller_run();
    fixture.wait_until_leader_ready(&mut controller);
    stop(&mut controller);
    assert!(!mac_worker::test_support::controller::drain::is_drained(&root).unwrap());
    assert!(!root.join("integration-gate.json").exists());
    let mut restored = fixture.spawn_controller_run();
    fixture.wait_until_leader_ready(&mut restored);
    assert!(!mac_worker::test_support::controller::drain::is_drained(&root).unwrap());
    let repo = crate::support::GitRepo::init();
    repo.write("src.txt", b"ordinary source\n");
    repo.commit_all("fixture");
    let (status, stdout, stderr) = fixture.run_laptop(
        &[
            "--json",
            "task",
            "submit",
            "--prompt",
            "ordinary work after restart",
            "--wip",
            "--no-wait",
        ],
        Some(repo.root()),
    );
    assert!(
        status.success(),
        "submit failed: {}",
        String::from_utf8_lossy(&stderr)
    );
    let submitted: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    let task_id = submitted["task_id"].as_str().unwrap();
    let (status, stdout, stderr) = fixture.wait_for_task_quiescence(Some(repo.root()), task_id);
    assert!(
        status.success(),
        "ordinary dispatch needed undrain: stdout={} stderr={}",
        String::from_utf8_lossy(&stdout),
        String::from_utf8_lossy(&stderr)
    );
    assert_eq!(fixture.journal_task_turns().len(), 1);
    stop(&mut restored);
    assert!(!mac_worker::test_support::controller::drain::is_drained(&root).unwrap());
    assert!(!root.join("integration-gate.json").exists());
    let health: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("health.json")).unwrap()).unwrap();
    assert!(health["stopped_at_millis"].is_u64());
}

#[test]
fn durable_cancel_retry_accepts_its_own_revoked_stop_progress() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        controller::{ControllerFault, ControllerStore, RequestPhase, TaskSubmitHandler},
        task::model::*,
    };
    let temp = tempfile::tempdir().unwrap();
    let paths = isolated_paths(&temp.path().canonicalize().unwrap());
    let config = fixture_config(&paths);
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    tasks.create_task(ordinary.clone()).unwrap();
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let mut integration = sample_record(fixture_task(), fixture_source(), "main");
    state
        .publish_policy(fixture_task(), &integration.policy)
        .unwrap();
    state
        .replace(fixture_task(), IntegrationRevision(0), &integration)
        .unwrap();
    let handler = TaskSubmitHandler::new(&NoProcesses, &config, &paths, &tasks);
    let store = ControllerStore::open(&paths.controller_state_root()).unwrap();
    let request = parse_request(&serde_json::to_vec(&json!({"protocol_version": 7, "request_id": "00000000000000000000000000000061", "command": "task.cancel", "body": {"task_id": fixture_task()}})).unwrap()).unwrap();
    store
        .handle_with(&request, &handler, ControllerFault::StopAfterPublish)
        .unwrap();
    assert_eq!(
        store.load(request.request_id()).unwrap().unwrap().phase(),
        RequestPhase::Published
    );
    // The initial stop persists its tombstone; retirement then changes the
    // same turn's ordinary record before the controller retries its saved body.
    let status = TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Cancelled),
        Some("fixture-worker".into()),
        true,
        ordinary.status().head_oid().cloned(),
        None,
        vec![],
        vec![],
        None,
        vec![TurnSummary::new(
            1,
            fixture_source(),
            Some(TurnTerminal::Cancelled),
            Some(TaskOutcome::Cancelled),
            Some(true),
            false,
            Some(1000),
            Some(1002),
        )],
        1002,
    )
    .unwrap();
    assert!(
        tasks
            .update_task_if_current(&ordinary, ordinary.with_status(status).unwrap())
            .unwrap()
    );
    let expected = integration.snapshot.revision;
    integration.snapshot.revision = IntegrationRevision(2);
    integration.snapshot.state = IntegrationStatus::Revoked;
    integration.tombstone = Some(IntegrationTombstone {
        epoch: 0,
        revision: IntegrationRevision(2),
        requested_at_millis: 1002,
        acknowledged: true,
    });
    state
        .replace(fixture_task(), expected, &integration)
        .unwrap();
    store
        .handle_with(&request, &handler, ControllerFault::None)
        .unwrap();
    assert_eq!(
        store.load(request.request_id()).unwrap().unwrap().phase(),
        RequestPhase::Acked
    );
    assert_eq!(
        tasks
            .load_task(fixture_task())
            .unwrap()
            .status()
            .last_outcome(),
        Some(&TaskOutcome::Cancelled)
    );
    assert!(tasks.queue_snapshot().unwrap().entries().is_empty());
}

#[test]
fn durable_cancel_of_an_imported_commit_settles_without_relabeling_success() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        controller::{ControllerFault, ControllerStore, RequestPhase, TaskSubmitHandler},
        task::model::TaskOutcome,
    };
    let temp = tempfile::tempdir().unwrap();
    let paths = isolated_paths(&temp.path().canonicalize().unwrap());
    let config = fixture_config(&paths);
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    tasks.create_task(ordinary.clone()).unwrap();
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let mut integration = sample_record(fixture_task(), fixture_source(), "main");
    integration.snapshot.state = IntegrationStatus::Integrated;
    integration.snapshot.disposition = Some(IntegrationDisposition::AlreadyIntegrated);
    integration.snapshot.observed_target_oid = Some(fixture_head());
    integration.receipt = Some(IntegrationReceipt {
        integration_id: integration.snapshot.integration_id,
        epoch: 0,
        source_turn_id: fixture_source(),
        source_head: fixture_head(),
        target_head: fixture_head(),
        merge_oid: None,
        disposition: IntegrationDisposition::AlreadyIntegrated,
        imported: true,
        recorded_at_millis: 1001,
    });
    state
        .publish_policy(fixture_task(), &integration.policy)
        .unwrap();
    state
        .replace(fixture_task(), IntegrationRevision(0), &integration)
        .unwrap();
    let handler = TaskSubmitHandler::new(&NoProcesses, &config, &paths, &tasks);
    let store = ControllerStore::open(&paths.controller_state_root()).unwrap();
    let request = parse_request(
        &serde_json::to_vec(&json!({"protocol_version": 7,
        "request_id": "00000000000000000000000000000065", "command": "task.cancel",
        "body": {"task_id": fixture_task()}}))
        .unwrap(),
    )
    .unwrap();
    for _ in 0..2 {
        assert_eq!(
            store
                .handle_with(&request, &handler, ControllerFault::None)
                .unwrap_err()
                .public_code(),
            "INTEGRATION_ALREADY_COMMITTED"
        );
        assert_eq!(
            store.load(request.request_id()).unwrap().unwrap().phase(),
            RequestPhase::Acked
        );
        assert_eq!(
            tasks
                .load_task(fixture_task())
                .unwrap()
                .status()
                .last_outcome(),
            Some(&TaskOutcome::Done)
        );
        assert_eq!(state.load(fixture_task()).unwrap().unwrap(), integration);
        assert!(tasks.queue_snapshot().unwrap().entries().is_empty());
    }
}

#[test]
fn durable_cancel_retry_cannot_retarget_a_newer_integration_epoch() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        controller::{ControllerFault, ControllerStore, TaskSubmitHandler},
    };
    let temp = tempfile::tempdir().unwrap();
    let paths = isolated_paths(&temp.path().canonicalize().unwrap());
    let config = fixture_config(&paths);
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    tasks.create_task(ordinary.clone()).unwrap();
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let mut integration = sample_record(fixture_task(), fixture_source(), "main");
    state
        .publish_policy(fixture_task(), &integration.policy)
        .unwrap();
    state
        .replace(fixture_task(), IntegrationRevision(0), &integration)
        .unwrap();
    let handler = TaskSubmitHandler::new(&NoProcesses, &config, &paths, &tasks);
    let store = ControllerStore::open(&paths.controller_state_root()).unwrap();
    let request = parse_request(&serde_json::to_vec(&json!({"protocol_version": 7, "request_id": "00000000000000000000000000000062", "command": "task.cancel", "body": {"task_id": fixture_task()}})).unwrap()).unwrap();
    store
        .handle_with(&request, &handler, ControllerFault::StopAfterPublish)
        .unwrap();
    let expected = integration.snapshot.revision;
    integration.snapshot.revision = IntegrationRevision(2);
    integration.snapshot.epoch = 1;
    state
        .replace(fixture_task(), expected, &integration)
        .unwrap();
    let error = store
        .handle_with(&request, &handler, ControllerFault::None)
        .unwrap_err();
    assert_eq!(error.public_code(), "TASK_REVISION_CONFLICT");
    assert_eq!(tasks.load_task(fixture_task()).unwrap(), ordinary);
    assert_eq!(state.load(fixture_task()).unwrap().unwrap(), integration);
    assert!(tasks.queue_snapshot().unwrap().entries().is_empty());
}

#[test]
fn controller_redrive_freezes_the_request_without_running_an_owner_phase() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        controller::{ControllerFault, ControllerStore, TaskSubmitHandler},
    };
    let temp = tempfile::tempdir().unwrap();
    let paths = isolated_paths(&temp.path().canonicalize().unwrap());
    let config = fixture_config(&paths);
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    tasks
        .create_task(sample_ordinary(fixture_task(), fixture_source()))
        .unwrap();
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let mut integration = sample_record(fixture_task(), fixture_source(), "main");
    integration.snapshot.state = IntegrationStatus::Blocked;
    integration.snapshot.blocked_code = Some(IntegrationCode::IntegrationWorkerOffline);
    state
        .publish_policy(fixture_task(), &integration.policy)
        .unwrap();
    state
        .replace(fixture_task(), IntegrationRevision(0), &integration)
        .unwrap();
    let handler = TaskSubmitHandler::new(&NoProcesses, &config, &paths, &tasks);
    let store = ControllerStore::open(&paths.controller_state_root()).unwrap();
    let request_id = "00000000000000000000000000000063";
    let request = parse_request(&serde_json::to_vec(&json!({"protocol_version": 7, "request_id": request_id, "command": "task.integrate", "body": {"task_id": fixture_task(), "expected": integration.snapshot.revision, "request_id": request_id}})).unwrap()).unwrap();
    store
        .handle_with(&request, &handler, ControllerFault::StopAfterPublish)
        .unwrap();
    let saved = store.load(request_id).unwrap().unwrap();
    assert_eq!(saved.prepared(), request.body());
    assert_eq!(saved.body(), request.body());
    assert_eq!(state.load(fixture_task()).unwrap().unwrap(), integration);
    assert!(tasks.queue_snapshot().unwrap().entries().is_empty());
    let old = TaskSubmitHandler::new(&NoProcesses, &config, &paths, &tasks)
        .with_integration_features(vec![]);
    let unavailable = store
        .handle_with(&request, &old, ControllerFault::None)
        .unwrap_err();
    assert_eq!(unavailable.public_code(), "INTEGRATION_UNAVAILABLE");
    assert_eq!(state.load(fixture_task()).unwrap().unwrap(), integration);
}

#[test]
fn controller_redrive_binds_integrated_success_without_an_epoch_or_process() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        controller::{ControllerFault, ControllerStore, TaskSubmitHandler},
    };
    let temp = tempfile::tempdir().unwrap();
    let paths = isolated_paths(&temp.path().canonicalize().unwrap());
    let config = fixture_config(&paths);
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    tasks.create_task(ordinary.clone()).unwrap();
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let mut record = sample_record(fixture_task(), fixture_source(), "main");
    record.snapshot.state = IntegrationStatus::Integrated;
    record.snapshot.observed_target_oid = Some(fixture_head());
    record.snapshot.disposition = Some(IntegrationDisposition::AlreadyIntegrated);
    record.receipt = Some(IntegrationReceipt {
        integration_id: record.snapshot.integration_id,
        epoch: 0,
        source_turn_id: fixture_source(),
        source_head: fixture_head(),
        target_head: fixture_head(),
        merge_oid: None,
        disposition: IntegrationDisposition::AlreadyIntegrated,
        imported: true,
        recorded_at_millis: 1002,
    });
    state
        .publish_policy(record.task_id, &record.policy)
        .unwrap();
    state
        .replace(record.task_id, IntegrationRevision(0), &record)
        .unwrap();
    let handler = TaskSubmitHandler::new(&NoProcesses, &config, &paths, &tasks);
    let store = ControllerStore::open(&paths.controller_state_root()).unwrap();
    for ordinal in 0..2 {
        let request_id = format!("{:032x}", 100 + ordinal);
        let request = parse_request(&serde_json::to_vec(&json!({
            "protocol_version": 7, "request_id": request_id, "command": "task.integrate",
            "body": {"task_id": record.task_id, "expected": record.snapshot.revision, "request_id": request_id}
        })).unwrap()).unwrap();
        for _ in 0..2 {
            let ack = store
                .handle_with(&request, &handler, ControllerFault::None)
                .unwrap();
            assert_eq!(
                ack.result(),
                Some(&serde_json::to_value(&record.snapshot).unwrap())
            );
            assert_eq!(state.load(record.task_id).unwrap().unwrap(), record);
            assert_eq!(tasks.load_task(record.task_id).unwrap(), ordinary);
            assert!(tasks.queue_snapshot().unwrap().entries().is_empty());
            let saved: serde_json::Value = serde_json::from_slice(
                &std::fs::read(paths.state.join(format!(
                    "integrations/tasks/{}/redrive-{request_id}.json",
                    record.task_id
                )))
                .unwrap(),
            )
            .unwrap();
            assert_eq!(
                saved["result"],
                serde_json::to_value(&record.snapshot).unwrap()
            );
        }
    }
}

#[test]
fn capable_controller_publishes_the_policy_before_ordinary_preparation() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        controller::{ControllerCommandHandler, TaskSubmitHandler},
    };
    let temp = tempfile::tempdir().unwrap();
    let paths = isolated_paths(&temp.path().canonicalize().unwrap());
    let config = fixture_config(&paths);
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    let handler = TaskSubmitHandler::new(&SystemProcessRunner, &config, &paths, &tasks);
    let request = valid_wrapper();
    let prepared = handler.prepare(&request).unwrap();
    assert_eq!(prepared.task_id, Some(fixture_task().to_string()));
    let bytes = std::fs::read(
        paths
            .state
            .join(format!("integrations/tasks/{}/policy.json", fixture_task())),
    )
    .unwrap();
    let policy: FrozenIntegrationPolicy = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(policy, sample_policy("main"));
    assert!(tasks.list_tasks().unwrap().is_empty());
    assert!(tasks.queue_snapshot().unwrap().entries().is_empty());
    use mac_worker::test_support::controller::{ControllerFault, ControllerStore};
    let store = ControllerStore::open(&paths.controller_state_root()).unwrap();
    store
        .handle_with(&request, &handler, ControllerFault::StopAfterPublish)
        .unwrap();
    let durable = store.load(request.request_id()).unwrap().unwrap();
    assert_eq!(durable.body(), request.body());
    assert_eq!(durable.payload_sha256(), request.payload_sha256());
    assert_eq!(durable.command(), "task.submit-integrating");
}

#[test]
fn old_controller_rejects_wrappers_and_companion_selectors_without_mutation_rows() {
    use mac_worker::test_support::controller::{
        encode_json_frame, serve_rpc_with_integration_features,
    };
    let temp = tempfile::tempdir().unwrap();
    let paths = isolated_paths(&temp.path().canonicalize().unwrap());
    let config = fixture_config(&paths);
    let submit = valid_wrapper();
    let selector = parse_request(
        &serde_json::to_vec(&json!({ "protocol_version": 7,
        "request_id": "00000000000000000000000000000042", "command": "task.list",
        "body": {"integration": {"task_ids": [fixture_task()]}} }))
        .unwrap(),
    )
    .unwrap();
    for request in [submit, selector] {
        let mut out = Vec::new();
        let payload = json!({"protocol_version": 7, "request_id": request.request_id(),
            "command": request.command(), "body": request.body()});
        let error = serve_rpc_with_integration_features(
            &paths,
            &config,
            &SystemProcessRunner,
            &mut std::io::Cursor::new(encode_json_frame(&payload).unwrap()),
            &mut out,
            &[],
        )
        .unwrap_err();
        assert_eq!(error.public_code(), "INTEGRATION_UNAVAILABLE");
        assert!(!paths.controller_state_root().exists());
    }
}

#[test]
fn verified_nested_source_keeps_request_pins_and_retires_the_unadopted_task_pin() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        controller::{ControllerFault, ControllerStore, TaskSubmitHandler},
        core::{config::Config, error::WorkerError},
        host::process::{ProcessRequest, ProcessResult, ProcessRunner},
    };
    struct GitOnly;
    impl ProcessRunner for GitOnly {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            if request.program == std::ffi::OsStr::new("/usr/bin/git") {
                SystemProcessRunner.run(request)
            } else {
                assert_eq!(request.program, std::ffi::OsStr::new("/usr/bin/ssh"));
                Err(WorkerError::Unavailable(
                    "isolated helper is unavailable".into(),
                ))
            }
        }
    }
    let mut f = super::controller_session_transfer::Fixture::new(true);
    f.body.wip = false;
    let mut policy = sample_policy("main");
    policy.base_oid = Some(f.base.clone());
    f.body.origin_url = Some(policy.origin.clone());
    let wrapper = prepare_integrating_submit(f.body.clone(), policy.clone()).unwrap();
    f.request = parse_request(
        &serde_json::to_vec(&json!({ "protocol_version": 7,
        "request_id": f.request.request_id(), "command": "task.submit-integrating",
        "body": wrapper }))
        .unwrap(),
    )
    .unwrap();
    let identity = f.prepare(Some(f.package.as_str()));
    assert!(
        f.push(&identity, &f.specs(Some(&f.package)))
            .status
            .success()
    );
    f.finish(&identity, Some(f.package.as_str()));
    let tasks = ClientStateStore::open(&f.paths.state).unwrap();
    let config = Config::parse(
        "version = 1\n[[workers]]\nname = 'fixture'\nssh = 'fixture.invalid'\nslots = 1\n",
    )
    .unwrap();
    let handler = TaskSubmitHandler::new(&GitOnly, &config, &f.paths, &tasks);
    let store = ControllerStore::open(&f.paths.controller_state_root()).unwrap();
    let error = store
        .handle_with(&f.request, &handler, ControllerFault::None)
        .unwrap_err();
    assert_eq!(error.public_code(), "INTEGRATION_UNAVAILABLE");
    assert!(tasks.list_tasks().unwrap().is_empty());
    assert!(tasks.queue_snapshot().unwrap().entries().is_empty());
    let saved: FrozenIntegrationPolicy = serde_json::from_slice(
        &std::fs::read(
            f.paths
                .state
                .join(format!("integrations/tasks/{}/policy.json", f.body.task_id)),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(saved, policy);
    let cache = f.cache();
    let read_ref = |name: &str| {
        std::process::Command::new("/usr/bin/git")
            .arg("--git-dir")
            .arg(cache.path())
            .args(["rev-parse", "--verify", name])
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env_remove("GIT_DIR")
            .env_remove("GIT_COMMON_DIR")
            .env_remove("GIT_WORK_TREE")
            .output()
            .unwrap()
    };
    for (reference, oid) in [
        (
            format!("refs/mac-worker/requests/{}", f.request.request_id()),
            &f.base,
        ),
        (
            format!(
                "refs/mac-worker/request-sessions/{}",
                f.request.request_id()
            ),
            &f.package,
        ),
    ] {
        let output = read_ref(&reference);
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stdout).unwrap().trim(),
            oid.as_str()
        );
    }
    assert!(
        !read_ref(&format!("refs/mac-worker/sessions/{}", f.body.task_id))
            .status
            .success()
    );
    let durable = store.load(f.request.request_id()).unwrap().unwrap();
    assert_eq!(durable.body(), f.request.body());
    assert_eq!(durable.payload_sha256(), f.request.payload_sha256());
}

#[test]
fn nested_import_retry_requires_the_original_wrapper_source_finish() {
    let temp = tempfile::tempdir().unwrap();
    let paths = isolated_paths(&temp.path().canonicalize().unwrap());
    let request = wrapped_request(&"b".repeat(40), &"d".repeat(40));
    assert_eq!(
        require_session_source_finished(&paths, &request)
            .unwrap_err()
            .public_code(),
        "CONTROLLER_SOURCE_CONFLICT"
    );
    record_session_source_finished(&paths, &request).unwrap();
    require_session_source_finished(&paths, &request).unwrap();
    let mut wire = json!({ "protocol_version": 7, "request_id": request.request_id(),
        "command": request.command(), "body": request.body() });
    wire["body"]["submit"]["prompt"] = json!("rebound source");
    let changed = parse_request(&serde_json::to_vec(&wire).unwrap()).unwrap();
    assert_eq!(
        require_session_source_finished(&paths, &changed)
            .unwrap_err()
            .public_code(),
        "CONTROLLER_SOURCE_CONFLICT"
    );
}

#[test]
fn acknowledged_nested_wrapper_releases_both_pins_and_replays_without_recapture() {
    let temp = tempfile::tempdir().unwrap();
    let paths = isolated_paths(&temp.path().canonicalize().unwrap());
    let repo = crate::support::GitRepo::init();
    repo.write("README", b"base\n");
    repo.commit_all("base");
    let base = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    repo.write("README", b"package stand-in\n");
    repo.commit_all("package");
    let package = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    let transfer = TransferRepo::open_or_create(&paths.cache, &repo.root().join(".git")).unwrap();
    let base_ref = format!("refs/mac-worker/bases/{}", fixture_task());
    let session_ref = format!("refs/mac-worker/sessions/{}", fixture_task());
    repo.git(&[
        "--git-dir",
        transfer.path().to_str().unwrap(),
        "update-ref",
        &base_ref,
        &base,
    ]);
    transfer
        .pin_object(
            &SystemProcessRunner,
            &session_ref,
            &package.parse().unwrap(),
        )
        .unwrap();
    let request = wrapped_request(&base, &package);
    record_controller_submit_pins(&paths, &request, &transfer).unwrap();
    record_session_source_finished(&paths, &request).unwrap();
    acknowledge_controller_submit_pins(&paths.controller_cache_root(), &request).unwrap();
    reconcile_controller_submit_pins(&paths, &SystemProcessRunner, &mut std::io::sink());
    reconcile_controller_submit_pins(&paths, &SystemProcessRunner, &mut std::io::sink());
    for reference in [&base_ref, &session_ref] {
        let output = std::process::Command::new("/usr/bin/git")
            .arg("--git-dir")
            .arg(transfer.path())
            .args(["show-ref", "--verify", reference])
            .output()
            .unwrap();
        assert!(!output.status.success(), "leaked paired pin {reference}");
    }
    assert!(
        !paths
            .controller_cache_root()
            .join("submit-pin-retirements")
            .join(format!("{}.json", request.request_id()))
            .exists()
    );
}

#[test]
fn fetch_only_integrating_batch_freezes_own_origin_before_any_task_effect() {
    use mac_worker::test_support::{
        controller::freeze_laptop_batch, core::config::Config, task::model::ClosePolicy,
    };
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let paths = isolated_paths(&root);
    let repo = crate::support::GitRepo::init();
    repo.write("README", b"base\n");
    repo.write(".worker.toml", b"[task]\nintegrate = 'main'\n");
    repo.commit_all("base");
    let origin = root.join("origin.git");
    repo.git(&["clone", "--bare", ".", origin.to_str().unwrap()]);
    let origin_url = format!("file://{}", origin.display());
    repo.git(&["remote", "add", "origin", &origin_url]);
    let batch = root.join("batch.toml");
    std::fs::write(&batch, "[[tasks]]\nid = 'enabled'\nprompt = 'work'\n[[tasks]]\nid = 'disabled'\nprompt = 'ordinary'\nintegrate = false\n").unwrap();
    let before = repo
        .git(&["for-each-ref", "--format=%(refname) %(objectname)"])
        .stdout;
    let frozen = freeze_laptop_batch(
        &SystemProcessRunner,
        &Config::parse("version = 1\nworkers = []\n").unwrap(),
        &paths,
        repo.root(),
        &batch,
        None,
        Some(1),
    )
    .unwrap();
    let enabled = &frozen.body().nodes["enabled"].frozen;
    assert_eq!(enabled.origin_url.as_deref(), Some(origin_url.as_str()));
    assert_eq!(enabled.close_on, ClosePolicy::Never);
    assert!(
        enabled
            .requires
            .contains(&"feature:task.integration".to_owned())
    );
    assert!(enabled.requires.contains(&"origin:file".to_owned()));
    assert_eq!(
        frozen.body().nodes["disabled"].frozen.close_on,
        ClosePolicy::Done
    );
    assert_eq!(
        repo.git(&["for-each-ref", "--format=%(refname) %(objectname)"])
            .stdout,
        before
    );
    assert!(
        !paths.state.exists(),
        "freeze created an ordinary task store"
    );
    assert!(frozen.body().nodes.values().all(|node| {
        serde_json::to_value(&node.frozen)
            .unwrap()
            .get("session_import")
            .is_none()
    }));
}

#[test]
fn host_command_arms_the_prepared_task_without_running_a_merge() {
    use mac_worker::test_support::{
        cli::{Command, HostCommand, from_parts},
        runtime::{RuntimeContext, run_with_stdio_in_context},
    };
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let mut f = GitIntegrationFixture::at(root.join("data/mac-worker"));
    f.commit_base();
    f.commit_task();
    let origin_before = f.origin_tip();
    let runtime = RuntimeContext::isolated(
        [("XDG_DATA_HOME".into(), root.join("data").into_os_string())].into(),
        root.join("home"),
        root.clone(),
    );
    let request = HostIntegrationRequest {
        protocol_version: 7,
        task_id: f.record.task_id,
        integration_id: None,
        epoch: 0,
        revision: IntegrationRevision(0),
        action: HostIntegrationAction::Arm {
            policy: f.record.policy.clone(),
        },
    };
    let mut out = Vec::new();
    let mut err = Vec::new();
    let code = run_with_stdio_in_context(
        from_parts(
            None,
            true,
            Command::Host {
                command: HostCommand::TaskIntegration,
            },
        ),
        &SystemProcessRunner,
        &runtime,
        &mut std::io::Cursor::new(encode_host_request(&request).unwrap()),
        &mut out,
        &mut err,
    );
    assert_eq!(
        code,
        0,
        "out={} err={}",
        String::from_utf8_lossy(&out),
        String::from_utf8_lossy(&err)
    );
    let response: HostIntegrationResponse = serde_json::from_slice(&out).unwrap();
    response.validate_for(&request).unwrap();
    assert!(matches!(
        response,
        HostIntegrationResponse::Progress { snapshot: None, .. }
    ));
    let path = f
        .store
        .task_dir(&f.record.policy.project_id, f.record.task_id)
        .unwrap()
        .join("integration/policy.json");
    let policy: FrozenIntegrationPolicy =
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(policy, f.record.policy);
    assert_eq!(f.origin_tip(), origin_before);
    assert!(
        f.store
            .task_status(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .state()
            == mac_worker::test_support::task::model::TaskState::Open
    );
}

#[test]
fn controller_companion_read_contract_is_separate_strict_and_bounded() {
    let result = IntegrationReadResult {
        schema_version: 1,
        integrations: std::collections::BTreeMap::from([(fixture_task(), None)]),
    };
    result.validate().unwrap();
    let bytes = encode_bounded(&result, MAX_INTEGRATION_RPC_BYTES).unwrap();
    assert_eq!(
        decode_bounded::<IntegrationReadResult>(&bytes, MAX_INTEGRATION_RPC_BYTES).unwrap(),
        result
    );
    let mut invalid = serde_json::to_value(&result).unwrap();
    invalid["ordinary_status"] = serde_json::json!({});
    assert!(serde_json::from_value::<IntegrationReadResult>(invalid).is_err());
    let mut overflow = result;
    for seed in 3..=18 {
        overflow.integrations.insert(
            mac_worker::test_support::task::model::TaskId::new(uuid::Uuid::from_u128(seed)),
            None,
        );
    }
    assert!(overflow.validate().is_err());
    assert_eq!(HOST_FEATURE_INTEGRATION, "task.integration");
    assert_eq!(CONTROLLER_FEATURE_INTEGRATION, "controller.integration");
}
